//! One async fn per endpoint (spec §6a.6 table). Every handler is a pure translation:
//! parse the request → build an `OpRequest` / call a `WalletClient` (or detached journal /
//! `MultiClient`) method → map the result to a `wallet_api` DTO. Admission, reservations,
//! holds, the scheduler, and policy activation all live in the actor and are NOT re-done here.

use crate::error::HttpError;
use crate::server::AppState;
use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::Instant;
use wallet_api::{
    ApproveRequest, AwaitTarget, BalanceResponse, CandidateView, DirectInflowRequest,
    FederationView, HealthView, HistoryResponse, JoinRequest, MoveRequest, OperationAccepted,
    OperationStatusDto, OperationView, PayRequest, Policy, ReceiveAccepted, ReceiveRequest,
    RefuseReason, WatchStatusView,
};
use wallet_core::{
    Action, Actor, AllocatorDecision, FederationId, IdempotencyKey, Msat, Occurrence,
    OperationKind, OperationRecord, OperationStatus, ReasonCode,
};
use wallet_fedimint::{
    direct_inflow_nonce_key, join_intent_key, move_key, parse_invoice, raw_pay_key,
    raw_receive_key, AwaitOutcome, Invoice, OpRequest, OperationRef, Snapshot, SnapshotScope,
    TickPolicy,
};

/// Wall-clock unix millis for the actor's decide-time clock. Display/ordering material only —
/// `seq` remains the ledger's ordering authority.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---- balances / federations -----------------------------------------------------------------

pub async fn balance(State(state): State<AppState>) -> Result<impl IntoResponse, HttpError> {
    let federations = federation_views(&state).await?;
    let total = federations
        .iter()
        .filter_map(|view| view.balance)
        .fold(0u64, |acc, msat| acc.saturating_add(msat.0));
    Ok(Json(BalanceResponse {
        total: Msat(total),
        federations,
    }))
}

pub async fn federations(State(state): State<AppState>) -> Result<impl IntoResponse, HttpError> {
    Ok(Json(federation_views(&state).await?))
}

/// The joined-federation registry joined with live balances. A fed that is not open (no client,
/// or a balance read that faulted) reports `balance: None` (spec §15.8 semantics) rather than
/// dropping out — the total simply omits it.
async fn federation_views(state: &AppState) -> Result<Vec<FederationView>, HttpError> {
    let joined = state.journal.list_federations().await.map_err(storage)?;
    let open = state
        .mc
        .as_ref()
        .map(|mc| mc.federations())
        .unwrap_or_default();
    let mut views = Vec::with_capacity(joined.len());
    for (id, info) in joined {
        let balance = if open.contains(&id) {
            match state.mc.as_ref() {
                Some(mc) => mc.balance(&id).await.ok(),
                None => None,
            }
        } else {
            None
        };
        views.push(FederationView {
            id,
            balance,
            invite: info.invite,
            joined_at_secs: info.joined_at,
        });
    }
    Ok(views)
}

// ---- history / show -------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
pub struct HistoryQuery {
    limit: Option<usize>,
    before_seq: Option<u64>,
}

pub async fn history(
    State(state): State<AppState>,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Query(query) = query?;
    let limit = query.limit.unwrap_or(50);
    let rows = state
        .journal
        .history(limit, query.before_seq)
        .await
        .map_err(storage)?;
    // A full page means more rows may remain: hand back the oldest seq as the next cursor.
    let next_before_seq = (rows.len() == limit && limit > 0)
        .then(|| rows.last().map(|row| row.seq))
        .flatten();
    let operations = rows.iter().map(operation_view).collect();
    Ok(Json(HistoryResponse {
        operations,
        next_before_seq,
    }))
}

#[derive(Debug, Default, Deserialize)]
pub struct ShowQuery {
    #[serde(default)]
    wait: bool,
}

pub async fn show_operation(
    State(state): State<AppState>,
    key: Result<Path<String>, PathRejection>,
    query: Result<Query<ShowQuery>, QueryRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Path(key) = key?;
    let Query(query) = query?;
    let key = IdempotencyKey(key);
    if query.wait {
        // Pending-map long-poll: park on the terminal, then read the ledger row back. The
        // request deadline is the waiter's mandatory deadline, so shutdown-drain and timeout
        // stay uniform (spec §6a.6). The actor resolves it when the driver terminalizes.
        let deadline = Instant::now() + state.await_deadline;
        match state
            .client
            .resolve_await(key.clone(), AwaitTarget::Terminal, deadline)
            .await
        {
            Ok(_) => {}
            Err(wallet_fedimint::ServiceError::Timeout) => {
                return Err(HttpError::timeout(
                    "operation wait deadline elapsed",
                    Some(key.0),
                ))
            }
            Err(error) => return Err(error.into()),
        }
    }
    match state
        .journal
        .operation(&OperationRef::Key(key.clone()))
        .await
        .map_err(storage)?
    {
        Some(record) => Ok(Json(operation_view(&record))),
        None => Err(HttpError::not_found(format!(
            "no operation found for key {}",
            key.0
        ))),
    }
}

// ---- status (dry-run) -----------------------------------------------------------------------

#[derive(Serialize)]
struct StatusResponse {
    spending_fed: Option<String>,
    standby_fed: Option<String>,
    decisions: Vec<StatusDecision>,
    scored: Vec<StatusScored>,
}

#[derive(Serialize)]
struct StatusDecision {
    operation_key: String,
    reason: String,
    action: String,
}

#[derive(Serialize)]
struct StatusScored {
    id: String,
    gated_eligible: bool,
}

pub async fn status(State(state): State<AppState>) -> Result<impl IntoResponse, HttpError> {
    let Some(runtime) = state.runtime.as_ref() else {
        return Err(HttpError::unavailable(
            "status dry-run requires a live runtime (covered by the daemon gate)",
        ));
    };
    let policy = state.client.get_policy().await?;
    let mut tick_policy = TickPolicy::from(&policy);
    // The dry-run describes what the NEXT scheduler tick would do. That tick advances the
    // persisted watch occurrence before planning (so its keys are stored+1) and scores probe
    // verdicts against the live clock — `From<&Policy>` leaves both at 0, which would emit
    // occurrence-0 keys (possibly already terminal) and mis-score every TTL-gated probe.
    let watch = state.journal.get_watch_state().await.map_err(storage)?;
    tick_policy.occurrence = Occurrence(watch.occurrence.saturating_add(1));
    tick_policy.now = now_ms();
    let report = runtime
        .status(&tick_policy)
        .await
        .map_err(|error| HttpError::unavailable(format!("status probe failed: {error}")))?;
    Ok(Json(StatusResponse {
        spending_fed: report.spending_fed.map(|id| id.to_hex()),
        standby_fed: report.standby_fed.map(|id| id.to_hex()),
        decisions: report
            .decisions
            .iter()
            .map(|decision| StatusDecision {
                operation_key: decision.idempotency_key.0.clone(),
                reason: reason_tag(decision.reason).to_owned(),
                action: format!("{:?}", decision.action),
            })
            .collect(),
        scored: report
            .scored
            .iter()
            .map(|scored| StatusScored {
                id: scored.id.to_hex(),
                gated_eligible: scored.gated_eligible,
            })
            .collect(),
    }))
}

// ---- watch observability / health -----------------------------------------------------------

pub async fn watch_status(State(state): State<AppState>) -> Result<impl IntoResponse, HttpError> {
    let watch = state.journal.get_watch_state().await.map_err(storage)?;
    Ok(Json(WatchStatusView {
        occurrence: watch.occurrence,
        last_discover_ms: watch.last_discover_ms,
        discover_cursor: watch.discover_cursor,
        discover_backlog: watch.discover_backlog,
    }))
}

pub async fn health(State(state): State<AppState>) -> Result<impl IntoResponse, HttpError> {
    // Registry size via the actor's Registry snapshot (bounded, ms-scale). Best-effort: a
    // health probe never fails the whole endpoint on a transient snapshot error.
    let inflight_drivers = match state.client.snapshot(SnapshotScope::Registry).await {
        Ok(Snapshot::Registry { drivers }) => drivers,
        _ => 0,
    };
    Ok(Json(HealthView {
        actor_queue_depth: state.client.queue_depth(),
        inflight_drivers,
        scheduler_alive: state
            .scheduler_alive
            .load(std::sync::atomic::Ordering::Relaxed),
    }))
}

// ---- pay / move (202 + operation key) -------------------------------------------------------

pub async fn pay(
    State(state): State<AppState>,
    request: Result<Json<PayRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = request?;
    // Request DEFAULTS (fee cap, spending pin) resolve from the policy visible at acceptance
    // time, here and in move/receive/direct-inflow. A PUT /v1/policy racing the request can
    // leave a just-superseded default on this one operation — accepted deliberately: it is the
    // caller's own request-vs-PUT race (either order is a valid linearization), unlike the
    // scheduler's minutes-long validation window, which IS generation-guarded (CommitTick).
    // Admission itself — caps, reservations, holds — always reads the CURRENT policy inside
    // the actor; a default here never bypasses a tightened cap.
    let policy = state.client.get_policy().await?;
    let details = parse_invoice(&Invoice(request.invoice.clone()))
        .map_err(|error| HttpError::invalid_request(format!("invalid BOLT11 invoice: {error}")))?;
    let amount = match (details.amount, request.amount) {
        (Some(invoice_amount), Some(stated)) if invoice_amount != stated => {
            return Err(HttpError::refused(
                RefuseReason::SizingConflict {
                    field: "amount".to_owned(),
                },
                "stated amount does not match the invoice amount",
            ))
        }
        (Some(invoice_amount), _) => invoice_amount,
        (None, Some(stated)) => stated,
        (None, None) => {
            return Err(HttpError::refused(
                RefuseReason::AmountRequired,
                "an amountless BOLT11 invoice requires an explicit amount",
            ))
        }
    };
    let fee_cap = request.fee_cap.unwrap_or(policy.max_fee);
    let from = resolve_fed(request.fed, policy.spending_fed, &state).await?;
    let key = raw_pay_key(details.payment_hash);
    let action = Action::Pay {
        from,
        invoice: Invoice(request.invoice),
        amount,
        fee_cap,
        payment_hash: details.payment_hash,
        gateway: None,
    };
    let balances = sample_balances(&state, &[from]).await?;
    submit_operation(&state, action, key, balances).await
}

pub async fn move_op(
    State(state): State<AppState>,
    request: Result<Json<MoveRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = request?;
    if request.from == request.to {
        return Err(HttpError::invalid_request(
            "move from and to must be different federations (from == to is a no-op)",
        ));
    }
    // Reject an unjoined source or destination synchronously (422), exactly as `resolve_fed`
    // does for pay/receive/direct-inflow. Without this, a move to an unjoined `to` is admitted
    // (202) and only fails async in the driver — an inconsistency with the sibling verbs.
    ensure_joined(request.from, &state).await?;
    ensure_joined(request.to, &state).await?;
    let policy = state.client.get_policy().await?;
    let fee_cap = request.fee_cap.unwrap_or(policy.max_fee);
    let key = move_key(
        &request.from,
        &request.to,
        request.amount,
        fee_cap,
        Occurrence(request.occurrence),
    );
    let action = Action::Move {
        from: request.from,
        to: request.to,
        amount: request.amount,
        fee_cap,
    };
    let balances = sample_balances(&state, &[request.from, request.to]).await?;
    submit_operation_at(
        &state,
        action,
        key,
        balances,
        Occurrence(request.occurrence),
    )
    .await
}

// ---- receive / direct-inflow (block for the invoice under the mint deadline) ----------------

pub async fn receive(
    State(state): State<AppState>,
    request: Result<Json<ReceiveRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = request?;
    validate_nonce(&request.nonce)?;
    let policy = state.client.get_policy().await?;
    let to = resolve_fed(request.to, policy.spending_fed, &state).await?;
    let fee_cap = request.fee_cap.unwrap_or(policy.max_fee);
    let key = raw_receive_key(to, request.amount, &request.nonce);
    let action = Action::Receive {
        to,
        amount: request.amount,
        fee_cap,
        nonce: request.nonce,
        gateway: None,
    };
    let balances = sample_balances(&state, &[to]).await?;
    block_for_invoice(&state, action, key, balances).await
}

pub async fn direct_inflow(
    State(state): State<AppState>,
    request: Result<Json<DirectInflowRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = request?;
    validate_nonce(&request.nonce)?;
    let policy = state.client.get_policy().await?;
    let to = resolve_fed(request.to, policy.spending_fed, &state).await?;
    let fee_cap = request.fee_cap.unwrap_or(policy.max_fee);
    let key = direct_inflow_nonce_key(to, request.amount, &request.nonce);
    let action = Action::DirectInflow {
        to,
        amount: request.amount,
        fee_cap,
    };
    let balances = sample_balances(&state, &[to]).await?;
    block_for_invoice(&state, action, key, balances).await
}

// ---- join / approve / candidates ------------------------------------------------------------

pub async fn join(
    State(state): State<AppState>,
    request: Result<Json<JoinRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = request?;
    use fedimint_core::invite_code::InviteCode;
    use std::str::FromStr as _;
    let parsed = InviteCode::from_str(&request.invite)
        .map_err(|error| HttpError::invalid_request(format!("invalid invite code: {error}")))?;
    let federation = {
        use fedimint_core::BitcoinHash as _;
        FederationId(parsed.federation_id().0.to_byte_array())
    };
    // Canonicalize the invite so the derived key is stable regardless of input formatting,
    // exactly as the standalone `Runtime::join` does.
    let invite = parsed.to_string();
    let key = join_intent_key(federation, &invite);
    let membership_preexisting = state
        .journal
        .get_federation(&federation)
        .await
        .map_err(storage)?
        .is_some();
    let action = Action::Join {
        federation,
        invite,
        membership_preexisting,
    };
    submit_operation(&state, action, key, BTreeMap::new()).await
}

pub async fn approve(
    State(state): State<AppState>,
    request: Result<Json<ApproveRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = request?;
    match state
        .journal
        .get_candidate(&request.fed)
        .await
        .map_err(storage)?
    {
        None => {
            return Err(HttpError::not_found(format!(
                "candidate {} was not found",
                request.fed.to_hex()
            )))
        }
        Some(candidate) if candidate.state != wallet_fedimint::CandidateState::AutoJoined => {
            return Err(HttpError::refused(
                RefuseReason::Conflict,
                format!(
                    "candidate {} is {:?}, not AutoJoined",
                    request.fed.to_hex(),
                    candidate.state
                ),
            ))
        }
        Some(_) => {}
    }
    let key = IdempotencyKey(format!("approve:{}:{}", request.fed.to_hex(), nonce()));
    if let Err(error) = state
        .journal
        .approve_auto_joined_candidate(request.fed, &key, now_ms())
        .await
    {
        return match error {
            // Another concurrent approval can win after the state check above. That remains a
            // request-state conflict, not a server/storage fault.
            wallet_core::ExecError::Permanent(message) => {
                Err(HttpError::refused(RefuseReason::Conflict, message))
            }
            error => Err(storage(error)),
        };
    }
    Ok((
        StatusCode::OK,
        Json(OperationAccepted {
            operation_key: key.0,
        }),
    ))
}

pub async fn candidates(State(state): State<AppState>) -> Result<impl IntoResponse, HttpError> {
    let report = state
        .journal
        .list_candidates_report()
        .await
        .map_err(storage)?;
    let views = report
        .candidates
        .into_iter()
        .map(|(id, record)| CandidateView {
            id,
            invite: record.invite.to_string(),
            source: discovery_source_tag(record.source).to_owned(),
            discovered_at_ms: record.discovered_at_ms,
            structural: structural_tag(&record.structural),
            structural_checked_at_ms: record.structural_checked_at_ms,
            state: candidate_state_tag(record.state).to_owned(),
            updated_at_ms: record.updated_at_ms,
        })
        .collect::<Vec<_>>();
    Ok(Json(views))
}

// ---- reconcile ------------------------------------------------------------------------------

#[derive(Serialize)]
struct ReconcileResponse {
    redriven: usize,
    awaiters_rehydrated: usize,
    executing_normalized: usize,
}

pub async fn reconcile(State(state): State<AppState>) -> Result<impl IntoResponse, HttpError> {
    // Actor-side intent re-drive first (idempotent; overlapping calls coalesce — the actor
    // registers the re-drive drivers itself). Then the off-actor O(ledger) ledger repair
    // (TL-4): it must NOT run inside the actor's critical section, and its CAS hardening makes
    // it a no-op against any row the actor already terminalized. Best-effort — a repair I/O
    // fault is logged, never fails the button (the intent re-drive already committed).
    let report = state.client.reconcile().await?;
    if let Some(mc) = state.mc.as_ref() {
        if let Err(error) = state.journal.repair_ledger(mc.as_ref()).await {
            tracing::warn!(
                ?error,
                "reconcile: off-actor ledger repair faulted; continuing"
            );
        }
    }
    Ok(Json(ReconcileResponse {
        redriven: report.redriven,
        awaiters_rehydrated: report.awaiters_rehydrated,
        executing_normalized: report.executing_normalized,
    }))
}

// ---- policy ---------------------------------------------------------------------------------

pub async fn get_policy(State(state): State<AppState>) -> Result<impl IntoResponse, HttpError> {
    Ok(Json(state.client.get_policy().await?))
}

pub async fn put_policy(
    State(state): State<AppState>,
    policy: Result<Json<Policy>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(policy) = policy?;
    // Validation + journal + scheduler wake all happen in the actor; an invalid policy comes
    // back as a refused ApiError naming the offending field (§6a.6).
    Ok(Json(state.client.put_policy(policy).await?))
}

// ---- shared translation helpers -------------------------------------------------------------

/// Build the `OpRequest`, submit it, and return `202` + the operation key. Used by pay/join
/// (occurrence 0) — the 202 key IS the ledger correlation key.
async fn submit_operation(
    state: &AppState,
    action: Action,
    key: IdempotencyKey,
    balances: BTreeMap<FederationId, Msat>,
) -> Result<(StatusCode, Json<OperationAccepted>), HttpError> {
    submit_operation_at(state, action, key, balances, Occurrence(0)).await
}

async fn submit_operation_at(
    state: &AppState,
    action: Action,
    key: IdempotencyKey,
    balances: BTreeMap<FederationId, Msat>,
    occurrence: Occurrence,
) -> Result<(StatusCode, Json<OperationAccepted>), HttpError> {
    state
        .client
        .decide_op(OpRequest {
            decision: AllocatorDecision {
                action,
                reason: ReasonCode::UserInitiated,
                occurrence,
                idempotency_key: key.clone(),
            },
            actor: Actor::User,
            now_ms: now_ms(),
            balances,
            probe_session_nonce: None,
        })
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAccepted {
            operation_key: key.0,
        }),
    ))
}

/// Admit a receive/direct-inflow, then BLOCK for its minted invoice under the hard deadline
/// (spec §6a.6): the BOLT11 is the response; settlement stays async. A bounded timeout returns
/// a `Timeout` ApiError carrying the (already-admitted) operation key, never a hang.
async fn block_for_invoice(
    state: &AppState,
    action: Action,
    key: IdempotencyKey,
    balances: BTreeMap<FederationId, Msat>,
) -> Result<axum::response::Response, HttpError> {
    state
        .client
        .decide_op(OpRequest {
            decision: AllocatorDecision {
                action,
                reason: ReasonCode::UserInitiated,
                occurrence: Occurrence(0),
                idempotency_key: key.clone(),
            },
            actor: Actor::User,
            now_ms: now_ms(),
            balances,
            probe_session_nonce: None,
        })
        .await?;
    let deadline = Instant::now() + state.invoice_deadline;
    match state
        .client
        .resolve_await(key.clone(), AwaitTarget::InvoiceArtifact, deadline)
        .await
    {
        Ok(AwaitOutcome::Invoice(invoice)) => Ok(Json(ReceiveAccepted {
            operation_key: key.0,
            invoice: invoice.0,
        })
        .into_response()),
        // Terminal without an invoice artifact = the mint failed before producing a BOLT11.
        // A journaled terminal is the "failed" layer: surface it with the op key (not a 5xx)
        // so the client inspects /v1/operations/{key}.
        Ok(AwaitOutcome::Terminal(_)) => Err(HttpError::failed(
            key.0,
            "the operation terminalized without a payable invoice",
        )),
        Err(wallet_fedimint::ServiceError::Timeout) => Err(HttpError::timeout(
            "invoice mint deadline elapsed; settlement continues asynchronously",
            Some(key.0),
        )),
        Err(error) => Err(error.into()),
    }
}

/// Sample live spendable balances for the involved federations (detached, before entering the
/// actor). A fed that is not currently open is omitted — admission treats a missing fed as zero
/// spendable, which refuses a spend cleanly rather than admitting an unfunded one. A balance
/// read that FAULTS on an open client fails closed (503): never size an admission against a
/// silently-zeroed balance.
async fn sample_balances(
    state: &AppState,
    feds: &[FederationId],
) -> Result<BTreeMap<FederationId, Msat>, HttpError> {
    let mut balances = BTreeMap::new();
    let Some(mc) = state.mc.as_ref() else {
        return Ok(balances);
    };
    let open = mc.federations();
    for fed in feds {
        if !open.contains(fed) {
            continue;
        }
        match mc.balance(fed).await {
            Ok(msat) => {
                balances.insert(*fed, msat);
            }
            Err(error) => {
                return Err(HttpError::unavailable(format!(
                    "reading balance for federation {} failed: {error}",
                    fed.to_hex()
                )))
            }
        }
    }
    Ok(balances)
}

/// Resolve the federation for a verb: the explicit request field, else the policy pin, else the
/// sole joined federation. Ambiguous (many joined, no choice) and empty are clear refusals.
async fn resolve_fed(
    explicit: Option<FederationId>,
    pin: Option<FederationId>,
    state: &AppState,
) -> Result<FederationId, HttpError> {
    if let Some(id) = explicit.or(pin) {
        ensure_joined(id, state).await?;
        return Ok(id);
    }
    let joined = state.journal.list_federations().await.map_err(storage)?;
    match joined.as_slice() {
        [(only, _)] => Ok(*only),
        [] => Err(HttpError::invalid_request(
            "no federations joined; join one first",
        )),
        _ => Err(HttpError::invalid_request(
            "multiple federations joined; name the federation explicitly",
        )),
    }
}

/// Refuse a money verb naming a federation the wallet has not joined — the same synchronous 422
/// [`resolve_fed`] returns for an explicit/pinned fed, factored out so `move`'s two explicit
/// endpoints reject an unjoined fed up front like every sibling verb, instead of admitting the
/// operation and only failing asynchronously in the driver.
async fn ensure_joined(id: FederationId, state: &AppState) -> Result<(), HttpError> {
    match state.journal.get_federation(&id).await.map_err(storage)? {
        Some(_) => Ok(()),
        None => Err(HttpError::invalid_request(format!(
            "federation {} is not joined",
            id.to_hex()
        ))),
    }
}

/// The client nonce is echoed verbatim into the receive/direct-inflow operation key, which is the
/// `{key}` path segment of `GET /v1/operations/{key}`. A nonce carrying `/` (or another
/// URL-structural byte) would yield a key the client can create but then never fetch back as a
/// single path segment. Require RFC 3986 "unreserved" bytes only, so the derived key is always a
/// round-trippable segment. (The pay/move keys derive from hex hashes + numbers and are safe.)
fn validate_nonce(nonce: &str) -> Result<(), HttpError> {
    if nonce.is_empty() {
        return Err(HttpError::invalid_request("nonce must not be empty"));
    }
    if !nonce
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~'))
    {
        return Err(HttpError::invalid_request(
            "nonce must contain only unreserved URL characters (A-Z a-z 0-9 - . _ ~)",
        ));
    }
    Ok(())
}

fn storage(error: wallet_core::ExecError) -> HttpError {
    HttpError::from(wallet_fedimint::ServiceError::Storage(format!("{error:?}")))
}

fn nonce() -> String {
    use rand::RngCore as _;
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    let mut out = String::with_capacity(32);
    use std::fmt::Write as _;
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

// ---- OperationRecord -> OperationView mapping (the ledger's public columns, §11) ------------

fn operation_view(record: &OperationRecord) -> OperationView {
    let (kind, amount) = kind_and_amount(&record.kind);
    OperationView {
        seq: record.seq,
        updated_at_ms: record.updated_at_ms,
        kind: kind.to_owned(),
        status: operation_status_dto(record.status),
        amount,
        receive_fee: record.fees.receive_fee,
        send_fee_quoted: record.fees.send_fee_quoted,
        actor: actor_tag(record.actor),
        reason: reason_tag(record.reason).to_owned(),
        operation_key: record.correlation_key.0.clone(),
        error: record.error.clone(),
    }
}

fn operation_status_dto(status: OperationStatus) -> OperationStatusDto {
    match status {
        OperationStatus::Started => OperationStatusDto::Started,
        OperationStatus::Awaiting => OperationStatusDto::Awaiting,
        OperationStatus::Succeeded => OperationStatusDto::Succeeded,
        OperationStatus::Failed => OperationStatusDto::Failed,
    }
}

fn kind_and_amount(kind: &OperationKind) -> (&'static str, Option<Msat>) {
    match kind {
        OperationKind::Join { .. } => ("join", None),
        OperationKind::Receive {
            amount_invoiced, ..
        } => ("receive", Some(*amount_invoiced)),
        OperationKind::Pay { invoice_amount, .. } => ("pay", *invoice_amount),
        OperationKind::DirectInflow { amount, .. } => ("direct-inflow", Some(*amount)),
        OperationKind::Move {
            amount, evacuation, ..
        } => (
            if *evacuation { "evacuation" } else { "move" },
            Some(*amount),
        ),
        OperationKind::Refusal { .. } => ("refusal", None),
        OperationKind::Probe { amount_msat, .. } => ("probe", Some(*amount_msat)),
        OperationKind::Tick { .. } => ("tick", None),
        OperationKind::Discover { .. } => ("discover", None),
        OperationKind::AutoJoin { .. } => ("autojoin", None),
        OperationKind::Approve { .. } => ("approve", None),
    }
}

fn actor_tag(actor: Actor) -> String {
    match actor {
        Actor::User => "user".to_owned(),
        Actor::Agent { occurrence } => format!("agent:{}", occurrence.0),
    }
}

fn reason_tag(reason: ReasonCode) -> &'static str {
    match reason {
        ReasonCode::SpendingBelowTarget => "spending_below_target",
        ReasonCode::StandbyBelowTarget => "standby_below_target",
        ReasonCode::ShutdownNotice => "shutdown_notice",
        ReasonCode::Unhealthy => "unhealthy",
        ReasonCode::OverCap => "over_cap",
        ReasonCode::NotProbed => "not_probed",
        ReasonCode::LowReputation => "low_reputation",
        ReasonCode::UserInitiated => "user_initiated",
        ReasonCode::StandingInstruction => "standing_instruction",
        ReasonCode::ActiveProbe => "active_probe",
    }
}

fn discovery_source_tag(source: wallet_core::DiscoverySource) -> &'static str {
    match source {
        wallet_core::DiscoverySource::Observer => "observer",
        wallet_core::DiscoverySource::Nostr => "nostr",
        wallet_core::DiscoverySource::Manual => "manual",
    }
}

fn candidate_state_tag(state: wallet_fedimint::CandidateState) -> &'static str {
    match state {
        wallet_fedimint::CandidateState::Discovered => "discovered",
        wallet_fedimint::CandidateState::AutoJoined => "autojoined",
        wallet_fedimint::CandidateState::UserApproved => "userapproved",
        wallet_fedimint::CandidateState::Rejected => "rejected",
    }
}

fn structural_tag(structural: &wallet_fedimint::StructuralOutcome) -> String {
    match structural {
        wallet_fedimint::StructuralOutcome::Passed => "passed".to_owned(),
        wallet_fedimint::StructuralOutcome::Rejected(reason) => format!("rejected:{reason}"),
    }
}

/// Deadline defaults (spec §6a.6, constants not policy). Carried in [`AppState`] so tests can
/// shorten them; production uses these.
pub const INVOICE_MINT_DEADLINE: Duration = Duration::from_secs(30);
pub const AWAIT_LONGPOLL_DEADLINE: Duration = Duration::from_secs(60);
