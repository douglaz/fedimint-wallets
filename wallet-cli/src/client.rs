//! Client mode (THE DEFAULT, spec §6a.7): every operational verb becomes an HTTP call against
//! the `wallet-api` wire types, talking to a running `walletd` (§6a.6). The daemon URL + bearer
//! token come from `~/.config/walletd/client.toml` (written by `walletd init`), overridable with
//! `--url`/`--token-path` (the devimint gates).
//!
//! This module owns ONLY the transport: a small reqwest client + typed helpers over the
//! `wallet-api` DTOs, the §6a.6 error taxonomy mapped to distinct exit codes ([`CliExit`]), and
//! per-verb request + response rendering (via [`crate::render`], the frozen contract shared with
//! `--standalone`). No money/decision logic — that all lives behind the daemon's actor.

use crate::exit::CliExit;
use crate::render::{self, AwaitVerb};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::Instant;
use wallet_api::{
    ApiError, ApiErrorKind, ApproveRequest, BalanceResponse, CandidateView, DirectInflowRequest,
    FederationView, HealthView, HistoryResponse, JoinRequest, MoveRequest, OperationAccepted,
    OperationStatusDto, OperationView, PayRequest, Policy, ReceiveAccepted, ReceiveRequest,
};
use wallet_core::{FederationId, Msat};

/// A single per-request transport timeout. It must exceed the server's 60 s await long-poll and
/// the 30 s invoice-mint deadline (spec §6a.6), so a legitimate long-poll is never mistaken for a
/// dead daemon; a genuine unreachable daemon fails fast on connect, well before this.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(90);

/// The clear two-options error demanded by spec §6a.7 whenever the daemon can't be reached or the
/// client pointer is missing — NEVER a silent fallback to standalone.
fn not_running(detail: impl std::fmt::Display) -> CliExit {
    CliExit::Transport(format!(
        "walletd is not running (or not initialized): {detail}\n\
         start walletd, or rerun with --standalone"
    ))
}

/// The `~/.config/walletd/client.toml` pointer written by `walletd init` (§6a.6): the daemon URL
/// and the path to its 0600 bearer token file.
#[derive(Debug, serde::Deserialize)]
struct ClientPointer {
    url: String,
    token_path: String,
}

/// A resolved client target: base URL + the bearer token read from disk.
pub struct WalletdClient {
    base_url: String,
    token: String,
    http: reqwest::Client,
}

impl WalletdClient {
    /// Resolve the daemon target from the pointer file + flag overrides, then read the token.
    /// `--url` overrides the pointer URL; `--token-path` overrides the token path. A missing
    /// pointer (with no `--url`) or a missing/empty token is the §6a.7 not-running error, never a
    /// silent fallback.
    pub fn resolve(
        url_override: Option<&str>,
        token_path_override: Option<&Path>,
    ) -> Result<Self, CliExit> {
        // Fully explicit client configuration must not depend on HOME/XDG or on a stale pointer
        // that supplies neither selected value.
        let pointer = if url_override.is_some() && token_path_override.is_some() {
            None
        } else {
            load_pointer()?
        };
        let base_url = match (url_override, &pointer) {
            (Some(url), _) => url.to_owned(),
            (None, Some(pointer)) => pointer.url.clone(),
            (None, None) => {
                return Err(not_running(format!(
                    "no client pointer at {}",
                    pointer_path()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|_| "~/.config/walletd/client.toml".to_owned())
                )))
            }
        };
        let token_path = resolve_token_path(token_path_override, pointer.as_ref())?;
        let token = read_token(&token_path)?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|error| CliExit::Usage(anyhow::anyhow!("building HTTP client: {error}")))?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            token,
            http,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// A bearer-authed GET, mapping the response into `T` or the §6a.6 taxonomy.
    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, CliExit> {
        let response = self
            .http
            .get(self.url(path))
            .bearer_auth(&self.token)
            .query(query)
            .send()
            .await
            .map_err(transport_error)?;
        parse_response(response).await
    }

    async fn send_json<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<T, CliExit> {
        let response = self
            .http
            .request(method, self.url(path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(transport_error)?;
        parse_response(response).await
    }

    async fn post<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<T, CliExit> {
        self.send_json(reqwest::Method::POST, path, body).await
    }

    async fn put<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<T, CliExit> {
        self.send_json(reqwest::Method::PUT, path, body).await
    }

    // ---- reads --------------------------------------------------------------------------------

    pub async fn balance(&self) -> Result<(), CliExit> {
        let response: BalanceResponse = self.get("/v1/balance", &[]).await?;
        let open = response
            .federations
            .iter()
            .filter(|f| f.balance.is_some())
            .count();
        for fed in &response.federations {
            match fed.balance {
                Some(balance) => println!("{}: {} msat", fed.id.to_hex(), balance.0),
                None => println!("{}: unavailable (failed to open)", fed.id.to_hex()),
            }
        }
        println!(
            "total ({}/{} federations): {} msat",
            open,
            response.federations.len(),
            response.total.0
        );
        if open != response.federations.len() {
            return Err(CliExit::Usage(anyhow::anyhow!(
                "{} joined federation(s) failed to open; the total above covers only the open set",
                response.federations.len() - open
            )));
        }
        Ok(())
    }

    pub async fn list_feds(&self) -> Result<(), CliExit> {
        let federations: Vec<FederationView> = self.get("/v1/federations", &[]).await?;
        for fed in federations {
            println!(
                "{} invite={} joined_at={}",
                fed.id.to_hex(),
                fed.invite,
                fed.joined_at_secs
            );
        }
        Ok(())
    }

    pub async fn history(
        &self,
        limit: usize,
        actor: Option<crate::ActorFilter>,
        status: Option<crate::StatusFilter>,
        json: bool,
    ) -> Result<(), CliExit> {
        // `--actor` / `--status` are applied client-side over the wire rows (the daemon's
        // `/v1/history` endpoint has no filter params, §6a.6). `--fed` is rejected up-front by the
        // dispatcher (the wire row omits the federation). Page until `limit` MATCHING rows have
        // been collected so filters retain the standalone contract: filters apply before limit.
        let mut operations = Vec::new();
        let mut before_seq: Option<u64> = None;
        loop {
            let mut query = vec![("limit", limit.to_string())];
            if let Some(seq) = before_seq {
                query.push(("before_seq", seq.to_string()));
            }
            let response: HistoryResponse = self.get("/v1/history", &query).await?;
            let remaining = limit.saturating_sub(operations.len());
            operations.extend(
                response
                    .operations
                    .into_iter()
                    .filter(|op| actor.is_none_or(|a| a.matches_actor_tag(&op.actor)))
                    .filter(|op| status.is_none_or(|s| s.matches_status_dto(op.status)))
                    .take(remaining),
            );
            if operations.len() == limit || response.next_before_seq.is_none() {
                break;
            }
            before_seq = response.next_before_seq;
        }
        for op in &operations {
            if json {
                println!("{}", serde_json::to_string(op).map_err(usage)?);
            } else {
                println!("{}", operation_view_tsv(op));
            }
        }
        Ok(())
    }

    pub async fn show(&self, key: &str, json: bool) -> Result<(), CliExit> {
        let view: OperationView = self.get(&operation_path(key), &[]).await?;
        if json {
            println!("{}", serde_json::to_string(&view).map_err(usage)?);
        } else {
            print_operation_view(&view);
        }
        Ok(())
    }

    pub async fn status(&self) -> Result<(), CliExit> {
        let response: StatusResponse = self.get("/v1/status", &[]).await?;
        // §15.8 fail-loud parity with `--standalone status` (and the `balance` verb): the wire
        // `/v1/status` scores only the OPEN set, so cross-check the joined registry for any fed
        // that failed to open. A diagnostic must never present an incomplete scored universe as
        // authoritative — print the unopened rows and exit non-zero. (The scored ROW shape stays
        // the thin public projection the frozen wire carries: `id gated_eligible`; balance / rank /
        // designation are not on the wire, so client `status` can't render `--standalone`'s richer
        // rows — the exit-code contract is what we hold across modes.)
        let federations: Vec<FederationView> = self.get("/v1/federations", &[]).await?;
        let unopened: Vec<&FederationView> =
            federations.iter().filter(|f| f.balance.is_none()).collect();
        println!(
            "spending_fed: {}",
            response.spending_fed.as_deref().unwrap_or("none")
        );
        println!(
            "standby_fed: {}",
            response.standby_fed.as_deref().unwrap_or("none")
        );
        for scored in &response.scored {
            println!("{} gated_eligible={}", scored.id, scored.gated_eligible);
        }
        for fed in &unopened {
            println!("{}: unavailable (failed to open)", fed.id.to_hex());
        }
        for decision in &response.decisions {
            println!(
                "decision: {} reason={} action={}",
                decision.operation_key, decision.reason, decision.action
            );
        }
        if !unopened.is_empty() {
            return Err(CliExit::Usage(anyhow::anyhow!(
                "{} joined federation(s) failed to open; the scored view above covers only the open set",
                unopened.len()
            )));
        }
        Ok(())
    }

    pub async fn health(&self) -> Result<(), CliExit> {
        let health: HealthView = self.get("/v1/health", &[]).await?;
        println!(
            "actor_queue_depth={} inflight_drivers={} scheduler_alive={}",
            health.actor_queue_depth, health.inflight_drivers, health.scheduler_alive
        );
        Ok(())
    }

    pub async fn candidates(
        &self,
        state: Option<crate::CandidateStateArg>,
        json: bool,
    ) -> Result<(), CliExit> {
        let mut candidates: Vec<CandidateView> = self.get("/v1/candidates", &[]).await?;
        // `--state` is applied client-side (the daemon's `/v1/candidates` returns all).
        if let Some(state) = state {
            candidates.retain(|c| c.state == state.tag());
        }
        // The daemon returns the registry in raw DB-key order; the CLI contract (and `--standalone`)
        // promise newest-first, so apply the same descending `(updated_at_ms, id)` sort here.
        candidates.sort_by_key(|c| std::cmp::Reverse((c.updated_at_ms, c.id)));
        if json {
            println!("{}", serde_json::to_string(&candidates).map_err(usage)?);
        } else {
            for candidate in &candidates {
                println!("{}", candidate_view_tsv(candidate));
            }
        }
        Ok(())
    }

    // ---- writes: pay / move / join (202 + operation key, two-phase phase 1) ---------------------

    pub async fn pay(
        &self,
        invoice: String,
        amount: Option<u64>,
        fee_cap: Option<u64>,
        fed: Option<FederationId>,
    ) -> Result<(), CliExit> {
        let request = PayRequest {
            invoice,
            amount: amount.map(Msat),
            fee_cap: fee_cap.map(Msat),
            fed,
        };
        let accepted: OperationAccepted = self.post("/v1/pay", &request).await?;
        // The daemon's 202 carries ONLY the operation key (its `submit_operation` discards the
        // decide's dedup/status), so client mode always reports the phase-1 `started` line;
        // `--standalone`, which sees the whole `DecidedOp`, additionally renders
        // already-in-flight / already-paid.
        render::print_phase1("started", &accepted.operation_key);
        Ok(())
    }

    pub async fn move_op(
        &self,
        from: FederationId,
        to: FederationId,
        amount: u64,
        fee_cap: Option<u64>,
        occurrence: u64,
    ) -> Result<(), CliExit> {
        let request = MoveRequest {
            from,
            to,
            amount: Msat(amount),
            fee_cap: fee_cap.map(Msat),
            occurrence,
        };
        let accepted: OperationAccepted = self.post("/v1/move", &request).await?;
        render::print_phase1("started", &accepted.operation_key);
        Ok(())
    }

    pub async fn join(&self, invite: String) -> Result<(), CliExit> {
        let request = JoinRequest { invite };
        let accepted: OperationAccepted = self.post("/v1/join", &request).await?;
        // Join is async now (full Intent lifecycle, §6a.6): the phase-1 result is the operation
        // key; settlement is awaited via `await-move <key>` like every other admitted op.
        render::print_phase1("started", &accepted.operation_key);
        Ok(())
    }

    // ---- writes: receive / direct-inflow (block for the invoice under the mint deadline) -------

    pub async fn receive(
        &self,
        amount: u64,
        fee_cap: Option<u64>,
        nonce: String,
        to: Option<FederationId>,
    ) -> Result<(), CliExit> {
        let request = ReceiveRequest {
            to,
            amount: Msat(amount),
            fee_cap: fee_cap.map(Msat),
            nonce,
        };
        let accepted: ReceiveAccepted = self.post("/v1/receive", &request).await?;
        render::print_value_with_key(&accepted.invoice, &accepted.operation_key);
        Ok(())
    }

    pub async fn direct_inflow(
        &self,
        amount: u64,
        fee_cap: Option<u64>,
        nonce: String,
        to: Option<FederationId>,
    ) -> Result<(), CliExit> {
        let request = DirectInflowRequest {
            to,
            amount: Msat(amount),
            fee_cap: fee_cap.map(Msat),
            nonce,
        };
        let accepted: ReceiveAccepted = self.post("/v1/direct-inflow", &request).await?;
        render::print_value_with_key(&accepted.invoice, &accepted.operation_key);
        Ok(())
    }

    // ---- await-* : re-poll GET /v1/operations/{key}?wait=true until terminal or --timeout ------

    pub async fn await_op(
        &self,
        verb: AwaitVerb,
        key: &str,
        timeout: Duration,
    ) -> Result<(), CliExit> {
        let path = operation_path(key);
        let overall_deadline = Instant::now() + timeout;
        loop {
            let Some(remaining) = overall_deadline.checked_duration_since(Instant::now()) else {
                return Err(await_timeout(timeout, key));
            };
            let polled = tokio::time::timeout(remaining, self.poll_operation(&path))
                .await
                .map_err(|_| await_timeout(timeout, key))??;
            match polled {
                // The operation terminalized: render its terminal state (and exit code).
                Some(view) => {
                    return render::await_terminal(
                        verb,
                        &view.kind,
                        view.status,
                        view.error.as_deref(),
                        key,
                    )
                }
                // The server long-poll elapsed (a 504, or the rare non-terminal read) without a
                // terminal — re-poll unless the caller's --timeout passed. A genuine transport
                // failure (dead daemon) already returned via `?` above, so it never spins here.
                None if Instant::now() < overall_deadline => {
                    // A small backoff avoids busy-spinning if the server ever resolves fast.
                    tokio::time::sleep_until(
                        (Instant::now() + Duration::from_millis(200)).min(overall_deadline),
                    )
                    .await;
                }
                None => return Err(await_timeout(timeout, key)),
            }
        }
    }

    /// One `GET /v1/operations/{key}?wait=true` poll. `Some(view)` = terminal; `None` = keep polling
    /// (a 504 long-poll elapse, or a non-terminal read); `Err` = a genuine transport/HTTP error that
    /// must NOT be retried (a dead daemon fails fast, never a busy-loop of connect attempts).
    async fn poll_operation(&self, path: &str) -> Result<Option<OperationView>, CliExit> {
        let response = self
            .http
            .get(self.url(path))
            .bearer_auth(&self.token)
            .query(&[("wait", "true")])
            .send()
            .await
            .map_err(transport_error)?;
        let status = response.status();
        if status.is_success() {
            let view: OperationView = response.json().await.map_err(|error| {
                CliExit::Transport(format!("decoding walletd response: {error}"))
            })?;
            return Ok(is_terminal(view.status).then_some(view));
        }
        // The server's own long-poll deadline (504) is a re-poll signal, not a failure.
        if status == reqwest::StatusCode::GATEWAY_TIMEOUT {
            return Ok(None);
        }
        let raw = response
            .text()
            .await
            .map_err(|error| CliExit::Transport(format!("reading walletd error body: {error}")))?;
        match serde_json::from_str::<ApiError>(&raw) {
            Ok(api) => Err(api_error_to_exit(status, api)),
            // Same status-class mapping as `parse_response`: a non-JSON 5xx stays transport (exit 4),
            // never a usage error, so an await against a transiently-faulting daemon is retryable.
            Err(_) => Err(non_api_error_to_exit(status, &raw)),
        }
    }

    // ---- approve ------------------------------------------------------------------------------

    pub async fn approve(&self, fed: FederationId) -> Result<(), CliExit> {
        let request = ApproveRequest { fed };
        let accepted: OperationAccepted = self.post("/v1/approve", &request).await?;
        render::print_value_with_key(&fed.to_hex(), &accepted.operation_key);
        Ok(())
    }

    // ---- reconcile / policy -------------------------------------------------------------------

    pub async fn reconcile(&self) -> Result<(), CliExit> {
        let response: ReconcileResponse = self.post("/v1/reconcile", &()).await?;
        println!(
            "redriven={} awaiters_rehydrated={} executing_normalized={}",
            response.redriven, response.awaiters_rehydrated, response.executing_normalized
        );
        Ok(())
    }

    pub async fn get_policy(&self) -> Result<Policy, CliExit> {
        self.get("/v1/policy", &[]).await
    }

    pub async fn put_policy(&self, policy: &Policy) -> Result<Policy, CliExit> {
        self.put("/v1/policy", policy).await
    }
}

/// The daemon's `/v1/status` body (a daemon-private shape, not a `wallet-api` DTO): mirror it here
/// for deserialization only.
#[derive(Debug, serde::Deserialize)]
struct StatusResponse {
    spending_fed: Option<String>,
    standby_fed: Option<String>,
    decisions: Vec<StatusDecision>,
    scored: Vec<StatusScored>,
}

#[derive(Debug, serde::Deserialize)]
struct StatusDecision {
    operation_key: String,
    reason: String,
    action: String,
}

#[derive(Debug, serde::Deserialize)]
struct StatusScored {
    id: String,
    gated_eligible: bool,
}

/// The daemon's `/v1/reconcile` body (daemon-private shape).
#[derive(Debug, serde::Deserialize)]
struct ReconcileResponse {
    redriven: usize,
    awaiters_rehydrated: usize,
    executing_normalized: usize,
}

/// The frozen 10-column history TSV, rendered from the wire `OperationView` so a client-mode
/// `history` row matches `--standalone` byte-for-byte:
/// `seq · updated_at · kind · status · amount · recv_fee · send_fee_quoted · actor · reason · key`.
fn operation_view_tsv(view: &OperationView) -> String {
    [
        view.seq.to_string(),
        crate::rfc3339_from_millis(view.updated_at_ms),
        view.kind.clone(),
        status_dto_tag(view.status).to_owned(),
        opt_msat(view.amount),
        opt_msat(view.receive_fee),
        opt_msat(view.send_fee_quoted),
        view.actor.clone(),
        view.reason.clone(),
        view.operation_key.clone(),
    ]
    .join("\t")
}

/// The client-mode multi-line `show` view. The wire `OperationView` carries only the ledger's
/// PUBLIC columns (§6a.6) — the richer per-kind op ids/gateway of `--standalone`'s offline `show`
/// are not on the wire, so this view is the thinner public projection.
fn print_operation_view(view: &OperationView) {
    println!("seq: {}", view.seq);
    println!("key: {}", view.operation_key);
    println!("kind: {}", view.kind);
    println!("status: {}", status_dto_tag(view.status));
    println!("actor: {}", view.actor);
    println!("reason: {}", view.reason);
    println!(
        "updated_at: {}",
        crate::rfc3339_from_millis(view.updated_at_ms)
    );
    println!("amount_msat: {}", opt_msat(view.amount));
    println!("receive_fee_msat: {}", opt_msat(view.receive_fee));
    println!("send_fee_quoted_msat: {}", opt_msat(view.send_fee_quoted));
    println!("error: {}", view.error.as_deref().unwrap_or("-"));
}

/// The frozen candidate TSV (matches `--standalone`'s `candidate_tsv`):
/// `id · state · source · discovered_at_ms · structural · checked_at_ms · updated_at_ms · invite`.
fn candidate_view_tsv(view: &CandidateView) -> String {
    [
        view.id.to_hex(),
        view.state.clone(),
        view.source.clone(),
        view.discovered_at_ms.to_string(),
        view.structural.clone(),
        view.structural_checked_at_ms.to_string(),
        view.updated_at_ms.to_string(),
        view.invite.clone(),
    ]
    .join("\t")
}

fn status_dto_tag(status: OperationStatusDto) -> &'static str {
    match status {
        OperationStatusDto::Started => "started",
        OperationStatusDto::Awaiting => "awaiting",
        OperationStatusDto::Succeeded => "succeeded",
        OperationStatusDto::Failed => "failed",
    }
}

fn opt_msat(amount: Option<Msat>) -> String {
    amount.map_or_else(|| "-".to_owned(), |m| m.0.to_string())
}

fn is_terminal(status: OperationStatusDto) -> bool {
    matches!(
        status,
        OperationStatusDto::Succeeded | OperationStatusDto::Failed
    )
}

/// The `/v1/operations/{key}` path. The daemon derives receive/direct-inflow keys from an
/// unreserved-char nonce and pay/move keys from hex + numbers, so a key is always a single valid
/// path segment; we pass it through so it stays byte-for-byte round-trippable.
fn operation_path(key: &str) -> String {
    format!("/v1/operations/{key}")
}

/// A reqwest transport failure → the §6a.7 not-running error (exit 4). A connect failure IS the
/// daemon-not-running case; a timeout/other is still transport, never a silent fallback.
fn transport_error(error: reqwest::Error) -> CliExit {
    if error.is_connect() {
        not_running("connection refused")
    } else if error.is_timeout() {
        CliExit::Transport(format!("request to walletd timed out: {error}"))
    } else {
        CliExit::Transport(format!("transport error talking to walletd: {error}"))
    }
}

/// Map a daemon HTTP response onto `T` (2xx) or the §6a.6 taxonomy. A non-2xx body is a
/// `wallet_api::ApiError`; its `kind` (+ status) selects the exit-code layer.
async fn parse_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, CliExit> {
    let status = response.status();
    if status.is_success() {
        return response
            .json::<T>()
            .await
            .map_err(|error| CliExit::Transport(format!("decoding walletd response: {error}")));
    }
    let raw = response
        .text()
        .await
        .map_err(|error| CliExit::Transport(format!("reading walletd error body: {error}")))?;
    match serde_json::from_str::<ApiError>(&raw) {
        Ok(api) => Err(api_error_to_exit(status, api)),
        // A non-ApiError error body (should not happen against walletd, but be robust) maps by
        // status class so the exit code stays meaningful — notably a 5xx stays transport (exit 4).
        Err(_) => Err(non_api_error_to_exit(status, &raw)),
    }
}

/// The §6a.6 error taxonomy → distinct exit codes.
fn api_error_to_exit(status: reqwest::StatusCode, api: ApiError) -> CliExit {
    match api.kind {
        ApiErrorKind::Unauthorized => CliExit::Auth(api.message),
        // A decide-time refusal — nothing journaled, safe to retry after fixing the cause.
        ApiErrorKind::Refused => CliExit::Refused(refuse_message(&api)),
        // Exit 3 is reserved for a JOURNALED operation's durable terminal failure — always the
        // 409 (Conflict) + operation-key shape. A 5xx `Failed` (503 actor stopped / 500 storage
        // fault, spec §6a.6) is a server-side transient with NOTHING journaled, so it belongs to
        // the transport layer (retry later), never exit 3.
        ApiErrorKind::Failed if status.is_server_error() => CliExit::Transport(api.message),
        // A journaled operation's terminal failure — carry the operation key so the caller can
        // inspect `show <key>` / history.
        ApiErrorKind::Failed => CliExit::Failed(failed_message(&api)),
        // A bounded-wait deadline (invoice mint / await long-poll) — transport-ish/timeout layer.
        ApiErrorKind::Timeout => CliExit::Transport(failed_message(&api)),
        // An unknown operation key (`show`/await on a missing key): a usage/other error.
        ApiErrorKind::NotFound => CliExit::Usage(anyhow::anyhow!("{}", api.message)),
    }
}

/// Map a non-`ApiError` error body by status class so the exit code stays meaningful. walletd's own
/// errors are ALL a JSON `ApiError` (`error.rs`/`server.rs`), so this fires only for an unexpected
/// body — an intermediary's HTML/plain-text 5xx page, say. A 5xx is a server-side transient (retry
/// later), so it joins the transport layer (exit 4), CONSISTENT with `api_error_to_exit`, which
/// already maps a JSON 5xx `Failed` to Transport; classifying the SAME 500/503 as a usage error
/// (exit 1) purely because its body is not JSON would tell a supervising script that a transient
/// outage is invalid input. A 401 is auth (exit 5); anything else is a usage/other error (exit 1).
fn non_api_error_to_exit(status: reqwest::StatusCode, raw: &str) -> CliExit {
    if status.as_u16() == 401 {
        CliExit::Auth(format!("unauthorized: {raw}"))
    } else if status.is_server_error() {
        CliExit::Transport(format!("walletd returned {status}: {raw}"))
    } else {
        CliExit::Usage(anyhow::anyhow!("walletd returned {status}: {raw}"))
    }
}

fn await_timeout(timeout: Duration, key: &str) -> CliExit {
    CliExit::Transport(format!(
        "await timed out after {}s waiting for operation {key} to terminalize",
        timeout.as_secs()
    ))
}

fn refuse_message(api: &ApiError) -> String {
    match &api.refuse_reason {
        Some(reason) => format!("{}: {reason:?}", api.message),
        None => api.message.clone(),
    }
}

fn failed_message(api: &ApiError) -> String {
    match &api.operation_key {
        Some(key) => format!("{} (operation key: {key})", api.message),
        None => api.message.clone(),
    }
}

/// `~/.config/walletd/client.toml`, honoring `$XDG_CONFIG_HOME` (spec §6a.7). Matches the daemon's
/// own `config_home` resolution so the CLI reads exactly what `walletd init` wrote.
fn pointer_path() -> Result<PathBuf, CliExit> {
    Ok(config_home()?.join("client.toml"))
}

fn config_home() -> Result<PathBuf, CliExit> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        return Ok(xdg.join("walletd"));
    }
    Ok(home_dir()?.join(".config").join("walletd"))
}

fn home_dir() -> Result<PathBuf, CliExit> {
    match std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        Some(home) => Ok(PathBuf::from(home)),
        None => Err(CliExit::Usage(anyhow::anyhow!(
            "HOME is not set; pass --url and --token-path explicitly"
        ))),
    }
}

/// Load the client pointer if present. A parse error on an existing file is a hard usage error (a
/// corrupt pointer must not be mistaken for "not running"); a missing file returns `None`.
fn load_pointer() -> Result<Option<ClientPointer>, CliExit> {
    let path = pointer_path()?;
    match std::fs::read_to_string(&path) {
        Ok(text) => toml::from_str::<ClientPointer>(&text)
            .map(Some)
            .map_err(|error| {
                CliExit::Usage(anyhow::anyhow!(
                    "parsing client pointer {}: {error}",
                    path.display()
                ))
            }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CliExit::Usage(anyhow::anyhow!(
            "reading client pointer {}: {error}",
            path.display()
        ))),
    }
}

/// Token path precedence (spec §6a.7): `--token-path` flag > the pointer's `token_path`.
fn resolve_token_path(
    override_path: Option<&Path>,
    pointer: Option<&ClientPointer>,
) -> Result<PathBuf, CliExit> {
    if let Some(path) = override_path {
        return Ok(path.to_path_buf());
    }
    match pointer {
        Some(pointer) => Ok(PathBuf::from(&pointer.token_path)),
        None => Err(not_running(
            "no bearer token path (no client pointer and no --token-path)",
        )),
    }
}

fn read_token(path: &Path) -> Result<String, CliExit> {
    let token = std::fs::read_to_string(path).map_err(|error| {
        not_running(format!("reading bearer token {}: {error}", path.display()))
    })?;
    let token = token.trim().to_owned();
    if token.is_empty() {
        return Err(not_running(format!(
            "bearer token file {} is empty",
            path.display()
        )));
    }
    Ok(token)
}

fn usage(error: impl std::fmt::Display) -> CliExit {
    CliExit::Usage(anyhow::anyhow!("{error}"))
}
