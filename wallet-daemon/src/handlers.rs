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
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use wallet_api::{
    ApproveRequest, AwaitTarget, BalanceResponse, CandidateView, DirectInflowRequest,
    FederationView, HealthView, HistoryResponse, JoinRequest, MoveRequest, OperationAccepted,
    OperationStatusDto, OperationView, PayRequest, Policy, ReceiveAccepted, ReceiveRequest,
    RecoverRequest, RefuseReason, WatchStatusView,
};
use wallet_core::{
    Action, Actor, AllocatorDecision, FederationId, IdempotencyKey, IntentStatus, Msat, Occurrence,
    OperationKind, OperationRecord, OperationStatus, ReasonCode,
};
use wallet_fedimint::{
    direct_inflow_nonce_key, join_intent_key, move_key, parse_invoice, raw_pay_key,
    raw_receive_key, recover_intent_key, repair_ledger_with_actor, AwaitOutcome, Invoice,
    MultiClient, OpRequest, OperationRef, Snapshot, SnapshotScope, TickPolicy,
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

/// Bounded so a public history request cannot turn one audit projection into an unbounded
/// snapshot/read allocation.  Link sidecars for the whole page are read in one journal snapshot.
pub(crate) const HISTORY_PAGE_LIMIT_MAX: usize = 500;

fn capped_history_limit(requested: Option<usize>) -> usize {
    requested.unwrap_or(50).min(HISTORY_PAGE_LIMIT_MAX)
}

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
    let limit = capped_history_limit(query.limit);
    let rows = state
        .journal
        .history(limit, query.before_seq)
        .await
        .map_err(storage)?;
    // A full page means more rows may remain: hand back the oldest seq as the next cursor.
    let next_before_seq = (rows.len() == limit && limit > 0)
        .then(|| rows.last().map(|row| row.seq))
        .flatten();
    let links = state
        .journal
        .evacuation_supersession_neighbors_for_display_keys(
            &rows
                .iter()
                .map(|row| row.correlation_key.clone())
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(storage)?;
    let mut operations = Vec::with_capacity(rows.len());
    for row in &rows {
        operations.push(operation_view_with_supersession_neighbors(
            operation_view_masked(row, &state.mc),
            display_supersession_neighbors_for_key(&links, &row.correlation_key),
        ));
    }
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
        Some(record) => {
            // Unlike history, `show` has one requested key, so fetch the exact intent to surface
            // a live structural evacuation marker without an N+1 history read.  A malformed
            // intent row degrades to absent (with a `warn!`) like a malformed sidecar, so one
            // corrupt marker cannot 500 the ledger row an operator is reading mid-incident.
            let intent = state
                .journal
                .intent_for_display(&key)
                .await
                .map_err(storage)?;
            let links = state
                .journal
                .evacuation_supersession_neighbors_for_display_keys(std::slice::from_ref(
                    &record.correlation_key,
                ))
                .await
                .map_err(storage)?;
            Ok(Json(operation_view_with_supersession_neighbors(
                operation_view_with_evacuation_refusal(
                    operation_view_masked(&record, &state.mc),
                    intent.as_ref(),
                ),
                display_supersession_neighbors_for_key(&links, &record.correlation_key),
            )))
        }
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
    /// Funding goals the tick wants but withholds because the shortfall is below the move floor.
    /// A tick emits no decision and no ledger row for these, so this is the only place they are
    /// visible; an empty array means nothing is being withheld for dust (br-0vg).
    deferred: Vec<StatusDeferred>,
}

/// A withheld funding goal: what it wanted, what blocked it, and which floor that was.
#[derive(Serialize)]
struct StatusDeferred {
    dest: String,
    source: Option<String>,
    reason: String,
    want_msat: u64,
    floor_msat: u64,
    /// `protocol_min_move` (lnv2's minimum incoming contract) or `route_min_viable` (the pair's
    /// economics under `max_fee_bps_of_move`).
    floor_source: &'static str,
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
    let Some(mc) = state.mc.as_ref() else {
        return Err(HttpError::unavailable(
            "status dry-run requires the live federation membership view; retry after walletd opens every joined federation",
        ));
    };
    let joined_report = state
        .journal
        .list_federations_report()
        .await
        .map_err(storage)?;
    if joined_report.skipped_rows > 0 {
        return Err(HttpError::unavailable(format!(
            "status dry-run refuses an incomplete federation registry: {} corrupt row(s) were skipped. \
             Stop walletd, preserve the wallet data directory, and repair the corrupt federation \
             registry row(s) before retrying; no scheduler planning was performed",
            joined_report.skipped_rows
        )));
    }
    let joined = joined_report.federations;
    let open = mc.federations();
    let unopened: Vec<_> = joined
        .iter()
        .filter(|(id, _)| !open.contains(id))
        .map(|(id, _)| id.to_hex())
        .collect();
    if !unopened.is_empty() {
        return Err(HttpError::unavailable(format!(
            "status dry-run requires every joined federation to be open; unopened: {}. \
             Retry after walletd opens them",
            unopened.join(", ")
        )));
    }
    let policy = state.client.get_policy().await?;
    let mut tick_policy = TickPolicy::from(&policy);
    // The dry-run describes what the NEXT scheduler tick would do. That tick advances the
    // persisted watch occurrence before planning (so its keys are stored+1) and scores probe
    // verdicts against the live clock — `From<&Policy>` leaves both at 0, which would emit
    // occurrence-0 keys (possibly already terminal) and mis-score every TTL-gated probe.
    let watch = state.journal.get_watch_state().await.map_err(storage)?;
    tick_policy.occurrence = Occurrence(watch.occurrence.checked_add(1).ok_or_else(|| {
        HttpError::unavailable(
            "watch scheduler occurrence exhausted at u64::MAX; restore a checkpoint below u64::MAX \
             before scheduling another cycle",
        )
    })?);
    tick_policy.now = now_ms();
    let report = runtime
        .status_for_daemon_scheduler(&tick_policy)
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
        deferred: report
            .deferred
            .iter()
            .map(|goal| StatusDeferred {
                dest: goal.dest.to_hex(),
                source: goal.source.map(|id| id.to_hex()),
                reason: reason_tag(goal.reason).to_owned(),
                want_msat: goal.want.0,
                floor_msat: goal.floor.0,
                floor_source: match goal.floor_source {
                    wallet_core::DeferralFloor::ProtocolMinMove => "protocol_min_move",
                    wallet_core::DeferralFloor::RouteMinViable => "route_min_viable",
                },
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
        // The pinned lnv2 send API takes no amount parameter (`MultiClient::pay` →
        // `LightningClientModule::send(bolt11, gateway, meta)`), so an amountless invoice is
        // UNPAYABLE by the engine — refuse at admission rather than 202 an operation whose
        // driver can only fail.
        (None, _) => {
            return Err(HttpError::refused(
                RefuseReason::AmountRequired,
                "amountless BOLT11 invoices are not payable (the lnv2 send API cannot supply \
                 an amount); request an amount-carrying invoice",
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
        gateway: None,
    };
    let balances = sample_balances(&state, &[request.from, request.to]).await?;
    // Dest-side fail-fast: a FRESH move to a joined-but-unopened `to` returns 503 at admission
    // rather than parking an unfunded Pending row. Source openness is intentionally NOT gated.
    let dest_unavailable = unopened_destination(&state, request.to);
    submit_operation_at(
        &state,
        action,
        key,
        balances,
        Occurrence(request.occurrence),
        dest_unavailable,
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
    // Dest-side fail-fast: a FRESH receive to a joined-but-unopened `to` returns 503 at admission
    // rather than admitting and stalling ~the invoice-mint deadline before a 504.
    let dest_unavailable = unopened_destination(&state, to);
    block_for_invoice(&state, action, key, balances, dest_unavailable).await
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
    // Dest-side fail-fast: a FRESH direct-inflow to a joined-but-unopened `to` returns 503 at
    // admission rather than admitting and stalling ~the invoice-mint deadline before a 504.
    let dest_unavailable = unopened_destination(&state, to);
    block_for_invoice(&state, action, key, balances, dest_unavailable).await
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

/// `POST /v1/recover`: rebuild a federation's balance from the seed (`docs/archive/wallet-recovery-spec.md`,
/// D1). Mirrors [`join`] — admit and return the operation key; the long recovery drives in a
/// detached task (D5), so the operator polls `GET /v1/operations/{key}` for the terminal state.
/// Recovering an already-registered federation (open OR registered-but-unopened), or a failed
/// module recovery, terminalizes that operation `Failed` (the refusal lives in
/// [`wallet_fedimint::MultiClient::recover`], not here).
pub async fn recover(
    State(state): State<AppState>,
    request: Result<Json<RecoverRequest>, JsonRejection>,
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
    // Canonicalize the invite so the derived key is stable regardless of input formatting.
    let invite = parsed.to_string();
    let key = recover_intent_key(federation, &invite);
    let action = Action::Recover { federation, invite };
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
    // registers the re-drive drivers itself). Then run the off-actor O(ledger) repair scan;
    // its raw Pay/Receive terminal intent status writes re-enter through the actor. Best-effort
    // — a repair I/O fault is logged, never fails the button (the re-drive already committed).
    let report = state.client.reconcile_durable().await?;
    if let Some(mc) = state.mc.as_ref() {
        if let Err(error) =
            repair_ledger_with_actor(state.journal.as_ref(), mc.as_ref(), &state.client).await
        {
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

/// The known `Policy` field names, derived from the type itself so the wire contract cannot drift
/// from the struct (br-c3j). `Policy` carries no `rename`/`flatten`/`skip_serializing_if`, so
/// serializing the default yields exactly its field set.
fn known_policy_fields() -> std::collections::BTreeSet<String> {
    let serialized =
        serde_json::to_value(Policy::default()).expect("Policy always serializes to JSON");
    serialized
        .as_object()
        .expect("Policy serializes to a JSON object")
        .keys()
        .cloned()
        .collect()
}

pub async fn put_policy(
    State(state): State<AppState>,
    body: Result<Json<serde_json::Value>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(body) = body?;
    // The STORED row is permissive so a rollback can still read a policy written by a newer build
    // (br-c3j); strictness belongs here, on the request, where a typo'd field would otherwise
    // silently leave the old value in place while the operator believes they changed it.
    let object = body
        .as_object()
        .ok_or_else(|| HttpError::invalid_request("policy must be a JSON object"))?;
    let known = known_policy_fields();
    let unknown: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|field| !known.contains(*field))
        .collect();
    if !unknown.is_empty() {
        return Err(HttpError::invalid_request(format!(
            "unknown policy field(s): {}",
            unknown.join(", ")
        )));
    }
    let policy: Policy = serde_json::from_value(body)
        .map_err(|e| HttpError::invalid_request(format!("policy is not well-formed: {e}")))?;
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
    // pay/join carry no destination-openness gate — `dest_unavailable` is `None`.
    submit_operation_at(state, action, key, balances, Occurrence(0), None).await
}

async fn submit_operation_at(
    state: &AppState,
    action: Action,
    key: IdempotencyKey,
    balances: BTreeMap<FederationId, Msat>,
    occurrence: Occurrence,
    dest_unavailable: Option<FederationId>,
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
            dest_unavailable,
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
    dest_unavailable: Option<FederationId>,
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
            dest_unavailable,
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

/// The FRESH-admission destination-openness signal for a dest-side verb (receive / direct-inflow
/// / move). The destination `to` is already ensured JOINED by `resolve_fed` / `ensure_joined`, so
/// this asks only whether it is currently OPEN — present in the live open set `mc.federations()`,
/// the SAME detached read [`sample_balances`] performs before entering the actor. Returns
/// `Some(to)` when `to` is joined-but-not-open (the actor fails a FRESH admission fast with 503
/// instead of journaling a Pending row that can only stall), and `None` when `to` is open OR when
/// openness is unknown because no runtime is attached (in-process/standalone: admission proceeds
/// exactly as before). The read is detached, so it can be stale: a `Some` that races `to` opening
/// merely costs the caller a retry — it never loses an attach, because the actor re-decides
/// fresh-vs-existing under its single ownership and an EXISTING key takes the attach path first.
fn unopened_destination(state: &AppState, to: FederationId) -> Option<FederationId> {
    let mc = state.mc.as_ref()?;
    (!mc.federations().contains(&to)).then_some(to)
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
        superseded_by: None,
        supersedes: None,
        // Only a refusal that actually recorded diagnostics carries a `refusal` object; the
        // default/figure-less refusal (plain over-cap, no-destination evacuation, ordinary
        // tick-drop) maps to `None` so the wire omits the field entirely rather than emitting
        // a collection of null/default diagnostic fields.
        // Conflict-suppressed tick drops carry observational diagnostics and remain populated.
        refusal: match &record.kind {
            OperationKind::Refusal { diagnostics, .. } if diagnostics.is_populated() => {
                Some(*diagnostics)
            }
            _ => None,
        },
        evacuation_refusal: None,
        evacuation_refusal_active: None,
    }
}

/// `operation_view`, then mask the narrow recovery commit→insert window. `MultiClient::recover`
/// terminalizes the recovery intent (→ `Succeeded`) before it synchronously inserts the reopened
/// client, so for a few microseconds an operation-status read sees `Succeeded` while
/// `/v1/balance` still omits the fed (absent from `federations()` until the insert). The recovery
/// reservation is held across that window, so while `recovery_handle_missing(fed)` holds, keep
/// reporting the same in-progress `Started` the op showed throughout the replay — status and
/// balance then agree. A commit-then-error or restart can extend the gap until the scheduler reopens
/// the registered partition; the mask stops as soon as that handle is installed.
fn operation_view_masked(record: &OperationRecord, mc: &Option<Arc<MultiClient>>) -> OperationView {
    let mut view = operation_view(record);
    if view.status == OperationStatusDto::Succeeded {
        if let OperationKind::Recover { fed } = &record.kind {
            if mc
                .as_ref()
                .is_some_and(|mc| mc.recovery_handle_missing(fed))
            {
                view.status = OperationStatusDto::Started;
            }
        }
    }
    view
}

/// `show` is intentionally the only operation projection that reads the matching intent. This
/// exposes an exact tri-state structural-marker projection without turning bounded history into an
/// N+1 intent scan.
fn operation_view_with_evacuation_refusal(
    mut view: OperationView,
    intent: Option<&wallet_core::Intent>,
) -> OperationView {
    view.evacuation_refusal = intent.and_then(|intent| intent.evacuation_refusal.clone());
    view.evacuation_refusal_active = intent.map(|intent| {
        intent.evacuation_refusal.is_some()
            && intent.status == IntentStatus::Pending
            && matches!(intent.actor, Actor::Agent { .. })
            && matches!(intent.action, Action::Evacuate { .. })
    });
    view
}

/// Apply already-validated sidecar neighbors to the wire view.  Kept separate from the journal
/// bulk read so the daemon mapping can be tested without an HTTP server or a storage fixture.
fn operation_view_with_supersession_neighbors(
    mut view: OperationView,
    links: wallet_fedimint::EvacuationSupersessionNeighbors,
) -> OperationView {
    view.supersedes = links.predecessor.map(|link| link.old_key.0);
    view.superseded_by = links.successor.map(|link| link.new_key.0);
    view
}

/// Presentation sidecars must never hide a ledger row. The journal normally inserts an empty
/// neighbor entry for every requested key, but a missing entry is equivalent to no display links.
/// Strict replacement and confirmation paths deliberately use their strict journal readers instead.
fn display_supersession_neighbors_for_key(
    links: &BTreeMap<IdempotencyKey, wallet_fedimint::EvacuationSupersessionNeighbors>,
    key: &IdempotencyKey,
) -> wallet_fedimint::EvacuationSupersessionNeighbors {
    links.get(key).cloned().unwrap_or_default()
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
        OperationKind::Recover { .. } => ("recover", None),
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
        ReasonCode::UneconomicRoute => "uneconomic_route",
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

#[cfg(test)]
mod tests {
    use super::*;
    use wallet_core::{
        EvacFeeCap, EvacuationQuoteSample, EvacuationRefusalEvidence, FeeBreakdown,
        RefusalDiagnostics,
    };
    use wallet_fedimint::{EvacuationSupersessionNeighbors, EvacuationSupersessionRecord};

    fn refusal_record(diagnostics: RefusalDiagnostics) -> OperationRecord {
        OperationRecord {
            seq: 1,
            correlation_key: IdempotencyKey("refuse:spending_below_target:0101:0".into()),
            kind: OperationKind::Refusal {
                fed: FederationId([1; 32]),
                diagnostics,
            },
            actor: Actor::Agent {
                occurrence: Occurrence(0),
            },
            reason: ReasonCode::SpendingBelowTarget,
            status: OperationStatus::Succeeded,
            created_at_ms: 0,
            updated_at_ms: 0,
            fees: FeeBreakdown::default(),
            error: None,
            repaired: false,
        }
    }

    fn supersession(old: &str, new: &str) -> EvacuationSupersessionRecord {
        let cap = EvacFeeCap {
            base_msat: Msat(10),
            bps: 100,
        };
        EvacuationSupersessionRecord {
            old_key: IdempotencyKey(old.into()),
            old_attempt: 0,
            new_key: IdempotencyKey(new.into()),
            new_attempt: 0,
            old_occurrence: Occurrence(1),
            occurrence: Occurrence(2),
            source: FederationId([1; 32]),
            old_cap_components: Some(cap),
            new_cap_components: Some(cap),
            refusal: EvacuationRefusalEvidence {
                cap_components: cap,
                requested_net: Msat(100),
                source_spendable: Msat(100),
                low: EvacuationQuoteSample {
                    delivered_net: Msat(10),
                    total_fee: Msat(20),
                    fee_cap: cap.at(Msat(10)),
                },
                high: EvacuationQuoteSample {
                    delivered_net: Msat(100),
                    total_fee: Msat(30),
                    fee_cap: cap.at(Msat(100)),
                },
                diagnostic: "test relation".into(),
                measured_at_ms: 1,
            },
            superseded_at_ms: 1,
        }
    }

    fn marked_evacuation_intent(status: IntentStatus) -> wallet_core::Intent {
        let evidence = supersession("evac:old", "evac:new").refusal;
        let cap = evidence.cap_components;
        wallet_core::Intent {
            idempotency_key: IdempotencyKey("evac:marked".into()),
            attempt: 0,
            action: Action::Evacuate {
                from: FederationId([1; 32]),
                to: FederationId([2; 32]),
                amount: Msat(100),
                fee_cap: cap.at(Msat(100)),
                gateway: None,
                fee_cap_components: Some(cap),
            },
            max_fee: Some(cap.at(Msat(100))),
            status,
            reason: ReasonCode::ShutdownNotice,
            actor: Actor::Agent {
                occurrence: Occurrence(1),
            },
            created_at_ms: 1,
            operation_id: None,
            invoice: None,
            evacuation_refusal: Some(evidence),
        }
    }

    #[test]
    fn operation_view_carries_populated_refusal_and_omits_figure_less() {
        // A default/figure-less refusal maps to no wire object (so `skip_serializing_if` omits
        // it); a populated one is carried. Guards the `is_populated` gate against a silent revert —
        // the always-equal `PartialEq` means a DTO round-trip alone would not catch it.
        let empty = operation_view(&refusal_record(RefusalDiagnostics::default()));
        assert!(empty.refusal.is_none());
        assert!(
            empty.evacuation_refusal_active.is_none(),
            "history/base mapping has no intent N+1"
        );
        assert!(
            serde_json::to_value(&empty)
                .expect("serialize history/base view")
                .get("evacuation_refusal_active")
                .is_none(),
            "history omits live marker authority because it performs no intent lookup"
        );

        let populated = operation_view(&refusal_record(RefusalDiagnostics {
            want: Some(Msat(50_000)),
            ..Default::default()
        }));
        assert_eq!(
            populated.refusal.expect("populated refusal carried").want,
            Some(Msat(50_000))
        );

        let conflict_only = operation_view(&refusal_record(RefusalDiagnostics {
            conflict_suppressed: true,
            ..Default::default()
        }));
        assert!(
            conflict_only
                .refusal
                .expect("conflict suppression is observational data")
                .conflict_suppressed
        );
    }

    #[test]
    fn show_marker_projection_distinguishes_active_historical_and_absent_evidence() {
        let record = refusal_record(RefusalDiagnostics::default());
        let active = marked_evacuation_intent(IntentStatus::Pending);
        let active_view =
            operation_view_with_evacuation_refusal(operation_view(&record), Some(&active));
        assert_eq!(active_view.evacuation_refusal_active, Some(true));
        assert_eq!(
            serde_json::to_value(&active_view).expect("serialize active daemon view")
                ["evacuation_refusal_active"],
            true
        );
        assert!(
            active_view.evacuation_refusal.is_some(),
            "active marker includes its exact evidence"
        );

        let failed = marked_evacuation_intent(IntentStatus::Failed);
        let failed_view =
            operation_view_with_evacuation_refusal(operation_view(&record), Some(&failed));
        assert_eq!(
            failed_view.evacuation_refusal_active,
            Some(false),
            "a superseded Failed parent retains evidence without becoming live"
        );
        assert!(
            failed_view.evacuation_refusal.is_some(),
            "historical evidence remains visible"
        );
        assert_eq!(
            serde_json::to_value(&failed_view).expect("serialize failed daemon view")
                ["evacuation_refusal_active"],
            false,
            "a readable exact historical intent explicitly projects inactive"
        );

        let absent_view = operation_view_with_evacuation_refusal(operation_view(&record), None);
        assert_eq!(absent_view.evacuation_refusal_active, None);
        assert!(absent_view.evacuation_refusal.is_none());
        assert!(serde_json::to_value(&absent_view)
            .expect("serialize absent daemon view")
            .get("evacuation_refusal_active")
            .is_none());
    }

    #[test]
    fn operation_view_keeps_predecessor_and_successor_for_chain_middle() {
        let middle = OperationRecord {
            correlation_key: IdempotencyKey("evac:b".into()),
            ..refusal_record(RefusalDiagnostics::default())
        };
        let view = operation_view_with_supersession_neighbors(
            operation_view(&middle),
            EvacuationSupersessionNeighbors {
                predecessor: Some(supersession("evac:a", "evac:b")),
                successor: Some(supersession("evac:b", "evac:c")),
            },
        );
        assert_eq!(view.supersedes.as_deref(), Some("evac:a"));
        assert_eq!(view.superseded_by.as_deref(), Some("evac:c"));
    }

    #[test]
    fn missing_bulk_supersession_key_degrades_to_empty_display_links() {
        let record = refusal_record(RefusalDiagnostics::default());
        let links = BTreeMap::new();

        let view = operation_view_with_supersession_neighbors(
            operation_view(&record),
            display_supersession_neighbors_for_key(&links, &record.correlation_key),
        );

        assert_eq!(view.supersedes, None);
        assert_eq!(view.superseded_by, None);
    }

    #[test]
    fn history_page_limit_is_capped_before_the_journal_read() {
        assert_eq!(capped_history_limit(None), 50);
        assert_eq!(capped_history_limit(Some(12)), 12);
        assert_eq!(capped_history_limit(Some(HISTORY_PAGE_LIMIT_MAX + 1)), 500);
    }
}
