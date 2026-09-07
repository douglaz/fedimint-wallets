use super::{
    PolicyExt, ProbeFacts, ProbePolicySnapshot, ReconcileReport, ServiceError, WalletClient,
};
use crate::discovery::{CandidateSource, ObserverSource};
use crate::runtime::{ledger_nonce, now_ms, Runtime};
use fedimint_core::runtime as fedimint_runtime;
use lightning_invoice::Bolt11Invoice;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use wallet_core::{adaptive_sleep_ms, IdempotencyKey, Msat, Occurrence, OperationStatus};

const DEFAULT_OBSERVER_URL: &str = "https://observer.fedimint.org/api";

/// How many receives must be stuck Awaiting past the stall deadline before we conclude the
/// fedimint client's shared receive task has died (rather than a single slow/unpaid invoice).
const SETTLEMENT_STALL_THRESHOLD: usize = 3;

/// Preserve the typed journal failure across `run_cycle`'s anyhow boundary. We use this only for
/// the immediate valid-watch-floor drain retry; every other cycle error follows normal backoff.
#[derive(Debug)]
struct WatchAdvanceCycleError(wallet_core::ExecError);

impl fmt::Display for WatchAdvanceCycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", self.0)
    }
}

impl std::error::Error for WatchAdvanceCycleError {}

/// The settlement-stall deadline (host-operational, env-overridable for the devimint gates).
fn settlement_stall_deadline() -> Duration {
    std::env::var("WALLETD_SETTLEMENT_STALL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(300))
}

/// Settlement-stall watchdog (root-caused 2026-07 by the 24h soak). fedimint's lnv2 client
/// spawned a shared `receive_lnurl_task` that held a DB transaction open across a long-poll and
/// committed with a NON-retrying `commit_tx()`; under the sustained concurrent load only a 24/7
/// daemon produces, a `WriteConflict` panicked that task and killed it — after which NO receive
/// ever claimed and the awaiter drivers pinned the registry at its cap. That bug is fixed at our
/// pinned fedimint rev (upstream PR #8816), but the watchdog stays as cause-agnostic insurance:
/// ANY future failure mode that silently kills settlement gets the same self-heal by restarting:
/// a fresh process rebuilds the client (fresh receive task) and reconcile re-drives the Awaiting
/// operations to their TRUE terminal — claiming a funded contract, expiring an unpaid one — so
/// no payment is ever marked failed while its contract is still claimable.
///
/// Two layers keep this from restart-looping on legitimately-unpaid invoices. FIRST, a receive is
/// only counted once it has outlived its own invoice validity (`is_settlement_stalled`): within its
/// expiry an unpaid invoice is normal, so a quiet pilot holding a few open invoices never counts —
/// this is what a low-traffic federation needs, since the second layer alone could not tell an
/// unpaid invoice from a dead task without other traffic. SECOND, even among expired-and-stuck
/// receives the signal is SELF-CLEARING: it fires only when zero receives have CLAIMED within the
/// deadline window. A live client keeps claiming other receives (nonzero recent successes ⇒ no
/// restart); a dead task claims nothing (zero successes ⇒ restart), and the fresh task's successes
/// clear it after the restart.
/// Returning `Some` makes [`run`] exit; its `CriticalTaskGuard` fires, walletd exits non-zero,
/// and the supervisor (systemd `Restart=on-failure`, shipped) brings it back.
async fn detect_settlement_stall(journal: &crate::journal::FedimintJournal) -> Option<String> {
    let deadline_ms = settlement_stall_deadline().as_millis() as u64;
    let now = now_ms();

    // Every-cycle scan: how many receives are stuck Awaiting past the settlement deadline AND past
    // their invoice's own validity? A receive still within its invoice expiry is legitimately
    // awaiting an external payer (an unpaid, UNEXPIRED invoice), NOT a settlement stall — see
    // `is_settlement_stalled`. Excluding those stops a quiet pilot that merely holds a few open
    // invoices from restart-looping (the false-fire this watchdog previously had).
    let awaiting = journal.awaiting().await.ok()?;
    let mut stalled = 0usize;
    for intent in &awaiting {
        // Cheap pre-checks before any per-intent journal read: only a receive-shaped op past the
        // settlement deadline can be a stall.
        let is_receive = matches!(
            intent.action,
            wallet_core::Action::Receive { .. } | wallet_core::Action::DirectInflow { .. }
        );
        if !is_receive || now.saturating_sub(intent.created_at_ms) <= deadline_ms {
            continue;
        }
        // Resolve the invoice to read its real expiry: a raw `Receive` carries it on the intent; a
        // `DirectInflow` runs the move path and persists it on the derived `MoveRecord` instead
        // (its `intent.invoice` is always `None`), so it must be fetched from there. A move-record
        // READ error is NOT a missing invoice: treating it as one would take the created-at fallback
        // and could restart on a fresh-but-old-intent inflow during a transient storage hiccup, so
        // abort the whole watchdog cycle instead — a decision this consequential must not run on an
        // unreliable journal read. The `min_move` fallback is reserved for a genuine `Ok(None)`.
        let invoice = match &intent.action {
            wallet_core::Action::DirectInflow { .. } => {
                match journal.get_move(&intent.idempotency_key).await {
                    Ok(record) => record.and_then(|rec| rec.invoice),
                    Err(_) => return None,
                }
            }
            _ => intent.invoice.clone(),
        };
        if is_settlement_stalled(
            &intent.action,
            invoice.as_ref(),
            intent.created_at_ms,
            now,
            deadline_ms,
        ) {
            stalled += 1;
        }
    }
    if stalled < SETTLEMENT_STALL_THRESHOLD {
        return None;
    }

    // Gate (only reached when already stalled): a receive claimed within the window means the
    // receive path is ALIVE — those stalled ones are just unpaid, so do NOT restart.
    let recent = journal.history(4096, None).await.ok()?;
    let claimed_recently = recent.iter().any(|row| {
        matches!(row.kind, wallet_core::OperationKind::Receive { .. })
            && row.status == OperationStatus::Succeeded
            && now.saturating_sub(row.updated_at_ms) <= deadline_ms
    });

    settlement_stall_verdict(stalled, claimed_recently, deadline_ms / 1000)
}

/// Whether one Awaiting intent counts toward the settlement-stall signal: a receive-shaped op
/// (`Receive`/`DirectInflow`, both paid by an EXTERNAL party) that is past the settlement deadline
/// AND whose invoice has actually EXPIRED. Within its validity an unpaid invoice is legitimately
/// awaiting a payer, not a stall — this is the fix for the quiet-pilot false-fire. Expiry is read
/// from the persisted BOLT11 itself (its own mint timestamp + expiry), so a receive that minted its
/// invoice long AFTER the intent was created — e.g. after a long run of failed `perform` attempts —
/// is judged by the invoice's REAL validity, not `created_at_ms` (which predates the mint). Only a
/// receive still Awaiting past its invoice's expiry (it should have terminalized: expired-unpaid →
/// Failed, funded → Succeeded) is evidence the shared receive task has died. A missing/unparseable
/// invoice on an Awaiting receive is anomalous; fall back to the conservative created-at anchor.
/// Pure, so the decision is unit-tested without a journal fixture.
fn is_settlement_stalled(
    action: &wallet_core::Action,
    invoice: Option<&wallet_core::Invoice>,
    created_at_ms: u64,
    now: u64,
    deadline_ms: u64,
) -> bool {
    let is_receive = matches!(
        action,
        wallet_core::Action::Receive { .. } | wallet_core::Action::DirectInflow { .. }
    );
    // Cheap pre-filter: nothing younger than the settlement deadline is a stall, and it avoids
    // parsing an invoice for the common recent-op case.
    if !is_receive || now.saturating_sub(created_at_ms) <= deadline_ms {
        return false;
    }
    // Post-expiry GRACE: count only once the invoice has been expired for MORE than the settlement
    // deadline, evaluated at `now - deadline_ms`. An invoice's expiry -> terminal transition is
    // asynchronous: at the boundary a HEALTHY awaiter has not yet written the expired/Failed status,
    // so counting the instant an invoice expires would restart a normal wallet whenever several
    // invoices expire together with no recent claim. The grace gives that transition time to land.
    //
    // The BOLT11 timestamp is authored by the gateway/LN node, whereas lnv2's incoming contract
    // expiry is derived from a separate clock; this same grace also absorbs any gateway-vs-wallet
    // clock skew up to `deadline_ms` (300s), which covers every realistic (NTP-synced) deployment.
    // A skew beyond that is a broken node — the consequence here is only a money-safe self-heal
    // restart, and the truly authoritative source (the persisted receive-contract expiration) is
    // not reachable from the journal without a per-receive live-client query.
    let grace_point_ms = now.saturating_sub(deadline_ms);
    match invoice.and_then(|inv| Bolt11Invoice::from_str(&inv.0).ok()) {
        Some(bolt11) => bolt11.would_expire(Duration::from_millis(grace_point_ms)),
        None => {
            now.saturating_sub(created_at_ms)
                > (crate::multi_client::RECEIVE_EXPIRY_SECS as u64)
                    .saturating_mul(1000)
                    .saturating_add(deadline_ms)
        }
    }
}

/// The pure decision behind [`detect_settlement_stall`], split out so the self-clearing logic is
/// unit-tested without a journal fixture: restart only when the stuck count reaches the threshold
/// AND no receive claimed recently (a live client always claims *some* receive; a dead task claims
/// none). `stalled` is already filtered to receives past the deadline by the caller.
fn settlement_stall_verdict(
    stalled: usize,
    claimed_recently: bool,
    deadline_secs: u64,
) -> Option<String> {
    if stalled < SETTLEMENT_STALL_THRESHOLD || claimed_recently {
        return None;
    }
    Some(format!(
        "settlement stall: {stalled} receive operation(s) stuck Awaiting past {deadline_secs}s \
         with zero receives claimed in that window — the fedimint client's receive task has \
         likely died; exiting for a supervised restart (reconcile re-drives on a fresh client)"
    ))
}

#[cfg(test)]
mod stall_tests {
    use super::{is_settlement_stalled, settlement_stall_verdict, SETTLEMENT_STALL_THRESHOLD};
    use lightning_invoice::Bolt11Invoice;
    use std::str::FromStr;
    use wallet_core::{Action, FederationId, Invoice, Msat};

    const DEADLINE_MS: u64 = 300_000; // 300s
    const EXPIRY_MS: u64 = 3_600_000; // fixed 1h fallback anchor (RECEIVE_EXPIRY_SECS)
    const NOW: u64 = 100_000_000;

    // A valid BOLT11 (the spec example) — a fixed mint timestamp and the default 3600s expiry, so
    // its real validity window is derivable and independent of `created_at_ms`.
    const REAL_INVOICE: &str = "lnbc25m1pvjluezpp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqdq5vdhkven9v5sxyetpdeessp5zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygs9q5sqqqqqqqqqqqqqqqqsgq2a25dxl5hrntdtn6zvydt7d66hyzsyhqs4wdynavys42xgl6sgx9c4g7me86a27t07mdtfry458rtjr0v92cnmswpsjscgt2vcse3sgpz3uapa";

    fn receive() -> Action {
        Action::Receive {
            to: FederationId([0x11; 32]),
            amount: Msat(1_000),
            fee_cap: Msat(10),
            nonce: String::new(),
            gateway: None,
        }
    }

    fn invoice_expiry_ms() -> u64 {
        let b = Bolt11Invoice::from_str(REAL_INVOICE).expect("valid test invoice");
        (b.duration_since_epoch() + b.expiry_time()).as_millis() as u64
    }

    #[test]
    fn unexpired_invoice_is_not_stalled() {
        // Past the settlement deadline, but the invoice itself is still within its validity — the
        // legitimately-open unpaid invoice a quiet pilot holds. Must NOT count.
        let now = invoice_expiry_ms() - 1_000; // 1s before the invoice expires
        let inv = Invoice(REAL_INVOICE.to_string());
        assert!(!is_settlement_stalled(
            &receive(),
            Some(&inv),
            now - 600_000,
            now,
            DEADLINE_MS
        ));
    }

    #[test]
    fn expired_invoice_past_grace_is_stalled() {
        // Expired for MORE than the settlement deadline yet still Awaiting → the async expiry ->
        // terminal transition should have landed by now → genuine stall.
        let now = invoice_expiry_ms() + DEADLINE_MS + 1_000;
        let inv = Invoice(REAL_INVOICE.to_string());
        assert!(is_settlement_stalled(
            &receive(),
            Some(&inv),
            now - 5_000_000,
            now,
            DEADLINE_MS
        ));
    }

    #[test]
    fn just_expired_within_grace_is_not_stalled() {
        // Expired only moments ago — inside the post-expiry grace, a healthy awaiter's terminal
        // write may still be in flight, so this must NOT count (would otherwise restart a normal
        // wallet whenever several invoices expire together).
        let now = invoice_expiry_ms() + 1_000; // 1s past expiry, well within the 300s grace
        let inv = Invoice(REAL_INVOICE.to_string());
        assert!(!is_settlement_stalled(
            &receive(),
            Some(&inv),
            now - 5_000_000,
            now,
            DEADLINE_MS
        ));
    }

    #[test]
    fn delayed_mint_is_judged_by_invoice_not_created_at() {
        // The regression codex caught: the intent was created LONG before the invoice was minted
        // (a run of failed perform attempts). Judged by the invoice's own fresh validity it is NOT
        // a stall, even though `created_at_ms` is 2h old — the old created-at anchor would have
        // wrongly counted it and re-introduced the restart loop.
        let now = invoice_expiry_ms() - 1_000;
        let created = now - 7_200_000; // intent created 2h before now
        let inv = Invoice(REAL_INVOICE.to_string());
        assert!(!is_settlement_stalled(
            &receive(),
            Some(&inv),
            created,
            now,
            DEADLINE_MS
        ));
    }

    #[test]
    fn within_deadline_is_never_stalled() {
        // Younger than the settlement deadline → the cheap pre-filter excludes it even with an
        // already-expired invoice.
        let now = invoice_expiry_ms() + 10_000_000;
        let inv = Invoice(REAL_INVOICE.to_string());
        assert!(!is_settlement_stalled(
            &receive(),
            Some(&inv),
            now - 100_000, // 100s < 300s deadline
            now,
            DEADLINE_MS
        ));
    }

    #[test]
    fn no_invoice_falls_back_to_created_at() {
        // Anomalous: an Awaiting receive with no parseable invoice falls back to the created-at +
        // fixed-expiry anchor. Within the anchor → not a stall; past it → a stall.
        assert!(!is_settlement_stalled(
            &receive(),
            None,
            NOW - 600_000, // 10 min, < 1h
            NOW,
            DEADLINE_MS
        ));
        assert!(is_settlement_stalled(
            &receive(),
            None,
            NOW - (EXPIRY_MS + DEADLINE_MS + 60_000), // past the 1h anchor + the post-expiry grace
            NOW,
            DEADLINE_MS
        ));
    }

    #[test]
    fn direct_inflow_uses_the_same_rule() {
        // In production `detect_settlement_stall` resolves a DirectInflow's invoice from its
        // `MoveRecord` (its `intent.invoice` is always `None`) and passes it in exactly like this.
        let inflow = Action::DirectInflow {
            to: FederationId([0x22; 32]),
            amount: Msat(1_000),
            fee_cap: Msat(10),
        };
        let now = invoice_expiry_ms() - 1_000;
        let inv = Invoice(REAL_INVOICE.to_string());
        assert!(!is_settlement_stalled(
            &inflow,
            Some(&inv),
            now - 600_000,
            now,
            DEADLINE_MS
        ));
    }

    #[test]
    fn non_receive_is_never_stalled() {
        let mv = Action::Move {
            from: FederationId([0x01; 32]),
            to: FederationId([0x02; 32]),
            amount: Msat(1),
            fee_cap: Msat(1),
            gateway: None,
        };
        assert!(!is_settlement_stalled(
            &mv,
            None,
            NOW - (EXPIRY_MS + 60_000),
            NOW,
            DEADLINE_MS
        ));
    }

    #[test]
    fn below_threshold_never_restarts() {
        assert!(settlement_stall_verdict(SETTLEMENT_STALL_THRESHOLD - 1, false, 300).is_none());
        assert!(settlement_stall_verdict(0, false, 300).is_none());
    }

    #[test]
    fn a_recent_claim_exonerates_even_at_threshold() {
        // Many receives stuck, but the client claimed one within the window ⇒ merely unpaid, not
        // a dead task. Must NOT restart (this is what prevents a loop on legit-unpaid invoices).
        assert!(settlement_stall_verdict(SETTLEMENT_STALL_THRESHOLD + 5, true, 300).is_none());
    }

    #[test]
    fn threshold_stuck_and_no_recent_claim_restarts() {
        let verdict = settlement_stall_verdict(SETTLEMENT_STALL_THRESHOLD, false, 300)
            .expect("a stalled receive path with no claims must trigger a restart");
        assert!(verdict.contains("settlement stall"));
        assert!(verdict.contains("300s"));
    }
}

pub(super) fn default_sources() -> Vec<Box<dyn CandidateSource>> {
    vec![Box::new(ObserverSource::new(DEFAULT_OBSERVER_URL))]
}

async fn abortable<T>(
    future: impl std::future::Future<Output = T>,
    abort: &mut oneshot::Receiver<()>,
) -> Option<T> {
    tokio::select! {
        _ = abort => None,
        output = future => Some(output),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaitEvent {
    Abort,
    Policy,
    Timer,
}

#[derive(Default)]
struct ExpiryWakeTasks(Vec<fedimint_runtime::JoinHandle<()>>);

impl ExpiryWakeTasks {
    fn extend(&mut self, tasks: Vec<fedimint_runtime::JoinHandle<()>>) {
        self.0.extend(tasks);
    }
}

impl Drop for ExpiryWakeTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

async fn wait_for_policy_or_sleep(
    sleep_ms: u64,
    policy_wake: &mut watch::Receiver<u64>,
    abort: &mut oneshot::Receiver<()>,
) -> WaitEvent {
    tokio::select! {
        _ = abort => WaitEvent::Abort,
        _ = policy_wake.changed() => WaitEvent::Policy,
        _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => WaitEvent::Timer,
    }
}

/// Whether this cycle may open a tick row, pay for route quotes, and commit decisions.
///
/// The bar is a SUCCESSFUL reconcile, nothing more (br-p93). It used to be `redriven == 0`, a
/// global count: one permanently retryable intent is re-driven every cycle forever, so the daemon
/// never ticked again — no rebalancing, and no evacuation for a DIFFERENT dying federation. What
/// that gate was protecting (not re-issuing in-flight work under a fresh occurrence) is now the
/// report's conflict-scoped `blocked` set, applied per logical goal in planning and at commit.
/// A FAILED reconcile is still a global no: unknown durable state, so nothing may be admitted.
pub(super) fn tick_may_commit(reconcile: &Option<ReconcileReport>) -> bool {
    reconcile.is_some()
}

/// The facts one cycle plans against, derived from the reconcile that opened it.
///
/// Extracted from `run_cycle` so both lines that carry eligibility out of the report are covered
/// by a test: `price_routes` (whether this cycle may buy route quotes at all) and `blocked` (which
/// logical allocator goals it must suppress). Dropping the latter would not break a single
/// assertion in the daemon path otherwise — the cycle would silently fall back to refusing the
/// same work at commit instead of never planning it, after paying for its quotes.
///
/// `price_routes` derives from the same [`tick_may_commit`] the tick row and the commit branch
/// read, so the cycle cannot buy quotes it may not act on. A FAILED reconcile carries no
/// eligibility at all: an empty blocker set here is not "nothing is in flight" but "unknown", and
/// the `None` arm in `run_cycle` short-circuits ahead of planning rather than trusting it.
fn cycle_probe_facts(
    reconcile: &Option<ReconcileReport>,
    probes: Vec<(wallet_core::FederationId, crate::probe::ProbeResult)>,
    occurrence: Occurrence,
    now_ms: u64,
) -> ProbeFacts {
    ProbeFacts {
        probes,
        occurrence,
        now_ms,
        // Route economics costs live fee quotes + gateway round trips, so the allowance rides the
        // same eligibility the commit does. ONE budget for the whole cycle — the planning loop can
        // re-plan once per unroutable destination, and a shared deadline is what stops a stalled
        // federation from multiplying its timeout by the revision count.
        price_routes: tick_may_commit(reconcile),
        // The pairs whose funding is already in flight are dropped before any quote is spent on
        // them (br-p93); independent pairs still price and still commit.
        blocked: reconcile
            .as_ref()
            .map(|report| report.blocked.clone())
            .unwrap_or_default(),
        admission_snapshot: reconcile
            .as_ref()
            .map(|report| report.admission_snapshot.clone())
            .unwrap_or_default(),
    }
}

/// Read the policy and re-designate from a new balance sample before scheduling active probes.
///
/// A commit can admit independent work while a concurrent terminal mutation changes any
/// federation's balance, and `PutPolicy` can supersede the policy that sized the plan.  Probe
/// scheduling therefore never reuses a designation or baseline from a cycle that tried to commit.
/// Return the exact policy and balances used for designation so `service_due_probes` evaluates the
/// same policy against the same probe sample.
struct FreshProbeDesignation {
    snapshot: ProbePolicySnapshot,
    balances: BTreeMap<wallet_core::FederationId, Msat>,
    /// A designation failure is deliberately carried as data. The rest of a watch cycle remains
    /// useful (notably discovery and deadlines), but no fresh probe may infer a source from a pin.
    spending: Result<Option<wallet_core::FederationId>, String>,
}

async fn current_policy_and_spending(
    runtime: &Runtime,
    client: &WalletClient,
    occurrence: Occurrence,
) -> Result<FreshProbeDesignation, ServiceError> {
    let snapshot = client.probe_policy_snapshot().await?;
    let mut tick_policy = crate::tick::TickPolicy::from(snapshot.policy());
    tick_policy.occurrence = occurrence;
    tick_policy.now = now_ms();
    let probes = runtime.probe_all().await;
    let balances = probes
        .iter()
        .map(|(id, probe)| (*id, Msat(probe.spendable_msat)))
        .collect();
    let spending = runtime
        .designated_spending_from_probes(
            &tick_policy,
            &wallet_core::ScorerPolicy::default(),
            &probes,
        )
        .await
        .map_err(|error| format!("watch scheduler: fresh probe designation failed: {error:#}"));
    Ok(FreshProbeDesignation {
        snapshot,
        balances,
        spending,
    })
}

async fn record_tick_started(runtime: &Runtime, occurrence: Occurrence) -> IdempotencyKey {
    let key = IdempotencyKey(format!("tick:{}:{}", occurrence.0, ledger_nonce()));
    if let Err(error) = runtime
        .service_journal()
        .record_tick_started(&key, occurrence, now_ms())
        .await
    {
        tracing::warn!(
            ?error,
            "watch scheduler: recording the Started tick row failed"
        );
    }
    key
}

async fn record_tick_planning_failed(runtime: &Runtime, key: &IdempotencyKey, error: &str) {
    if let Err(record_error) = runtime
        .service_journal()
        .record_tick_terminal(key, None, OperationStatus::Failed, Some(error), now_ms())
        .await
    {
        tracing::warn!(
            ?record_error,
            "watch scheduler: recording the failed tick row failed"
        );
    }
}

pub(super) async fn run(
    runtime: Arc<Runtime>,
    client: WalletClient,
    sources: Vec<Box<dyn CandidateSource>>,
    mut policy_wake: watch::Receiver<u64>,
    mut abort: oneshot::Receiver<()>,
    automation_blocker: Arc<std::sync::Mutex<Option<wallet_api::AutomationBlocked>>>,
) {
    let (expiry_wake_tx, mut expiry_wake_rx) = mpsc::channel(32);
    let multi_client = runtime.service_multi_client();
    let mut expiry_wake_feds = BTreeSet::new();
    let mut expiry_wake_tasks = ExpiryWakeTasks::default();
    expiry_wake_tasks.extend(
        multi_client.spawn_expiry_wake_tasks(&mut expiry_wake_feds, expiry_wake_tx.clone()),
    );
    let mut last_subscription_noop_ms = None;
    let mut triggered_by_subscription = false;

    loop {
        let Some(cycle) = abortable(run_cycle(&runtime, &client, &sources), &mut abort).await
        else {
            return;
        };
        let cycle = match cycle {
            Ok(cycle) => cycle,
            Err(error) => {
                tracing::warn!(?error, "watch scheduler: cycle failed");
                CycleResult {
                    deadlines: wallet_core::AdaptiveSleepDeadlines::default(),
                    noop: false,
                    // A cycle that ERRORED did not reach planning either. Report it under its
                    // own tag rather than leaving `/v1/health` claiming automation is ready.
                    automation_blocked: Some(wallet_api::AutomationBlocked {
                        reason: "cycle_failed".to_owned(),
                        detail: format!("{error}"),
                    }),
                }
            }
        };
        // Publish before the wait: this is the value `/v1/health` reads between cycles, and a
        // cleared blocker is as important to publish as a set one.
        if let Ok(mut slot) = automation_blocker.lock() {
            slot.clone_from(&cycle.automation_blocked);
        }
        // Settlement-stall watchdog: exit for a supervised restart if the client's receive path
        // has died (see `detect_settlement_stall`). Runs off-actor each cycle; the history scan
        // is gated behind the cheap awaiting scan so it only fires when receives are stuck.
        if let Some(reason) = detect_settlement_stall(&runtime.service_journal()).await {
            tracing::error!("{reason}");
            return;
        }
        expiry_wake_tasks.extend(
            multi_client.spawn_expiry_wake_tasks(&mut expiry_wake_feds, expiry_wake_tx.clone()),
        );
        if triggered_by_subscription && cycle.noop {
            last_subscription_noop_ms = Some(now_ms());
        }
        triggered_by_subscription = false;
        let mut deadlines = cycle.deadlines;

        'wait_for_cycle: loop {
            let policy = match abortable(client.get_policy(), &mut abort).await {
                None => return,
                Some(result) => match result {
                    Ok(policy) => policy,
                    Err(ServiceError::ShuttingDown | ServiceError::ActorStopped) => return,
                    Err(error) => {
                        tracing::warn!(?error, "watch scheduler: policy read failed");
                        break 'wait_for_cycle;
                    }
                },
            };
            let watch_policy = policy.watch_policy();
            let sleep_ms = adaptive_sleep_ms(now_ms(), &watch_policy, &deadlines);
            tokio::select! {
                event = wait_for_policy_or_sleep(sleep_ms, &mut policy_wake, &mut abort) => {
                    match event {
                        WaitEvent::Abort => return,
                        WaitEvent::Policy | WaitEvent::Timer => break 'wait_for_cycle,
                    }
                }
                wake = expiry_wake_rx.recv() => {
                    let Some((_fed, hinted_expiry_ms)) = wake else {
                        continue;
                    };
                    let now = now_ms();
                    let refresh = runtime.watch_deadlines_reusing_probe_schedule(
                        now,
                        &deadlines,
                        hinted_expiry_ms,
                    );
                    match abortable(refresh, &mut abort).await {
                        None => return,
                        Some(Ok(updated)) => deadlines = updated,
                        Some(Err(error)) => {
                            tracing::warn!(?error, "watch scheduler: expiry deadline refresh failed");
                            continue;
                        }
                    }
                    let recomputed = adaptive_sleep_ms(now, &watch_policy, &deadlines);
                    let (mut delay, mut is_subscription) = super::coalesced_subscription_delay_ms(
                        now,
                        last_subscription_noop_ms,
                        watch_policy.min_interval_ms,
                        recomputed,
                    );
                    if delay == 0 {
                        triggered_by_subscription = is_subscription;
                        break 'wait_for_cycle;
                    }
                    loop {
                        tokio::select! {
                            _ = &mut abort => return,
                            _ = policy_wake.changed() => break 'wait_for_cycle,
                            _ = tokio::time::sleep(Duration::from_millis(delay)) => {
                                triggered_by_subscription = is_subscription;
                                break 'wait_for_cycle;
                            }
                            wake = expiry_wake_rx.recv() => {
                                let Some((_fed, hinted_expiry_ms)) = wake else {
                                    continue 'wait_for_cycle;
                                };
                                let now = now_ms();
                                let refresh = runtime.watch_deadlines_reusing_probe_schedule(
                                    now,
                                    &deadlines,
                                    hinted_expiry_ms,
                                );
                                match abortable(refresh, &mut abort).await {
                                    None => return,
                                    Some(Ok(updated)) => deadlines = updated,
                                    Some(Err(error)) => {
                                        tracing::warn!(?error, "watch scheduler: expiry deadline refresh failed");
                                        continue;
                                    }
                                }
                                let recomputed = adaptive_sleep_ms(now, &watch_policy, &deadlines);
                                (delay, is_subscription) = super::coalesced_subscription_delay_ms(
                                    now,
                                    last_subscription_noop_ms,
                                    watch_policy.min_interval_ms,
                                    recomputed,
                                );
                                if delay == 0 {
                                    triggered_by_subscription = is_subscription;
                                    break 'wait_for_cycle;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// A floor fault after ReconcileDecide has offered a parked marker leaves this cycle unable to
/// plan. Drop only that in-memory offer; the durable marker remains and the next healthy cycle can
/// recapture it rather than having its next reconciliation clear it as stale.
async fn abandon_parked_handoff_after_watch_fault(client: &WalletClient) {
    if let Err(error) = client.abandon_parked_evacuation_handoff().await {
        tracing::warn!(
            ?error,
            "watch scheduler: could not abandon parked marker handoff after watch-floor fault"
        );
    }
}

/// A scheduler cycle that cannot allocate a fresh occurrence must still re-own old durable work.
/// This is intentionally distinct from public durable reconciliation: it permits the actor to
/// claim a planner-owned structural marker, while preserving the marker if admission authority is
/// poisoned.
async fn redrive_without_planner(client: &WalletClient, context: &'static str) {
    if let Err(error) = client.reconcile_recovery_only_cycle().await {
        tracing::warn!(
            ?error,
            %context,
            "watch scheduler: recovery-only reconciliation failed"
        );
    }
}

async fn run_cycle(
    runtime: &Runtime,
    client: &WalletClient,
    sources: &[Box<dyn CandidateSource>],
) -> anyhow::Result<CycleResult> {
    let journal = runtime.service_journal();
    // Recovery always precedes membership opening, and preserves planner ownership until the whole
    // view reaches the CaptureForPlanner reconciliation below.
    if let Err(error) = client.reconcile_durable().await {
        tracing::warn!(
            ?error,
            "watch scheduler: durable reconciliation failed; continuing cycle"
        );
    }
    // The O(ledger) op-log scan remains off actor; its raw Pay/Receive terminal
    // intent synchronization is routed back through the actor.
    if let Err(error) = super::repair_ledger_with_actor(
        runtime.service_journal().as_ref(),
        runtime.service_multi_client().as_ref(),
        client,
    )
    .await
    {
        tracing::warn!(
            ?error,
            "watch scheduler: ledger repair failed; continuing cycle"
        );
    }
    // §15.8 (ported from the standalone watch): a tick must NOT drive money decisions from a
    // partial world-view — an unopened joined federation would silently vanish from balances,
    // probes, and every allocation the cycle plans. The 5.2 watch process refused to start and
    // relied on the supervisor restart to retry `open_all`; the daemon keeps serving user ops
    // but retries the MISSING opens itself each cycle (re-opening an already-open fed would
    // replace its live client under in-flight drivers, so only the missing set is retried) and
    // skips the whole automated cycle — tick, scheduled probes, discovery — until whole.
    // Crash-recovery (the reconcile + repair above) still runs: re-driving already-admitted
    // intents is not a fresh money decision over the world-view.
    let multi_client = runtime.service_multi_client();
    let joined_report = runtime
        .service_journal()
        .list_federations_report()
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    if joined_report.skipped_rows > 0 {
        // A poison registry row is not an absent federation. Its funds and membership may be part
        // of the world the allocator would otherwise score, so the healthy subset is not a safe
        // planning view. Do not retry opens from that subset; recovery-only is still allowed to
        // redrive work that was admitted under an earlier durable authority (including a parked
        // RecoveryOnly marker).
        tracing::warn!(
            skipped_rows = joined_report.skipped_rows,
            "watch scheduler: corrupt federation registry rows make the membership view unknown; \
             skipping fresh tick, probes, discovery, and federation opening until repaired"
        );
        redrive_without_planner(client, "corrupt federation registry recovery").await;
        return Ok(CycleResult {
            deadlines: wallet_core::AdaptiveSleepDeadlines::default(),
            noop: false,
        });
    }
    let joined = joined_report.federations;
    let open: BTreeSet<_> = multi_client.federations().into_iter().collect();
    let missing: Vec<_> = joined
        .iter()
        .filter(|(id, _)| !open.contains(id))
        .map(|(_, info)| info.clone())
        .collect();
    if !missing.is_empty() {
        // Retrying a registered-but-unopened federation changes the process-visible membership
        // set. The SDK open can wait on the network; `open_all_with_membership_lease` therefore
        // acquires actor authority only for each final map insertion, invalidating any tick token
        // issued before that publication.
        let _ = multi_client
            .open_all_with_membership_lease(&missing, client)
            .await;
        let open: BTreeSet<_> = multi_client.federations().into_iter().collect();
        let unopened: Vec<_> = joined
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| !open.contains(id))
            .collect();
        if !unopened.is_empty() {
            let named = unopened
                .iter()
                .map(|id| id.to_hex())
                .collect::<Vec<_>>()
                .join(", ");
            tracing::warn!(
                unopened = ?unopened.iter().map(|id| id.to_hex()).collect::<Vec<_>>(),
                "watch scheduler: partial federation view; skipping the automated cycle (§15.8)"
            );
            // The opening attempts are over and this cycle has committed not to plan. One
            // recovery-only pass claims old planner markers rather than leaving an evacuation
            // stranded behind an unavailable federation.
            redrive_without_planner(client, "partial federation view after membership open").await;
            return Ok(CycleResult {
                deadlines: wallet_core::AdaptiveSleepDeadlines::default(),
                noop: false,
                // Refusing is right; refusing invisibly is not. Every pass re-fences until the
                // opens succeed, so without this an operator sees only `scheduler_alive: true`.
                automation_blocked: Some(wallet_api::AutomationBlocked {
                    reason: "partial_federation_view".to_owned(),
                    detail: format!(
                        "{} joined federation(s) could not be opened: {named}",
                        unopened.len()
                    ),
                }),
            });
        }
    }
    // The world is whole. Mint the one scheduler handoff immediately before occurrence
    // allocation; a partial/recovery-only cycle never captures a parked marker.
    let reconcile = match client.reconcile().await {
        Ok(report) => Some(report),
        Err(error) => {
            tracing::warn!(
                ?error,
                "watch scheduler: reconcile before fresh allocation failed; continuing non-money cycle"
            );
            None
        }
    };
    after_reconcile_before_watch_advance_test_hook(runtime).await;
    let watch_state = match journal.advance_watch_occurrence().await {
        Ok(state) => state,
        Err(error) => {
            abandon_parked_handoff_after_watch_fault(client).await;
            return Err(anyhow::Error::new(WatchAdvanceCycleError(error)));
        }
    };
    let occurrence = Occurrence(watch_state.occurrence);
    // ONE eligibility RULE for the whole cycle: the tick row and the commit below read this, and
    // `cycle_probe_facts` derives the route-quote allowance from the same `tick_may_commit` on the
    // same report, so they cannot disagree about whether this cycle may act.
    let may_commit = tick_may_commit(&reconcile);
    // Match the synchronous tick audit lifecycle: if this cycle is allowed to tick, open
    // its row before sensing or planning so a crash or planning failure remains visible.
    let tick_key = if may_commit {
        Some(record_tick_started(runtime, occurrence).await)
    } else {
        None
    };
    let probes = runtime.probe_all().await;
    let sensed_at_ms = now_ms();
    let balances = probes
        .iter()
        .map(|(id, probe)| (*id, Msat(probe.spendable_msat)))
        .collect::<BTreeMap<_, _>>();
    let mut facts = cycle_probe_facts(&reconcile, probes.clone(), occurrence, sensed_at_ms);
    let mut decision_count = 0;
    let mut commit = super::CommitTickReport::default();
    let mut tick_failed = false;
    // ReconcileDecide may park exact marked-evacuation snapshots. Once this scheduler cycle has
    // chosen not to invoke CommitTick, those snapshots must be abandoned in memory so a healthy
    // next cycle recaptures durable state rather than clearing an old offer.
    let mut commit_tick_invoked = false;
    // The allocator result is never probe-admission authority. Every path below obtains one fresh
    // atomic policy snapshot and re-designates under that exact policy after planning/commit work.
    let _planned_spending = match &reconcile {
        // A failed reconcile has no actor-issued planning result. In particular, do not make a
        // configured pin look like a freshly verified active-probe source.
        None => None,
        Some(_) => {
            facts.now_ms = now_ms();
            let round = client
                .decide_tick_round(facts)
                .await
                .map_err(anyhow::Error::new);
            match round {
                Ok(round) => {
                    after_tick_plan_test_hook(runtime, occurrence).await;
                    let spending = round.spending_fed;
                    if may_commit {
                        decision_count = round.decisions.len();
                        // Route validation performs network IO. Re-sample immediately before
                        // admission so a user operation that settled during that window cannot
                        // disappear from reservations while leaving its old balance behind.
                        match client.issue_balance_facts_token().await {
                            Ok(balance_facts) => {
                                let commit_balances = runtime
                                    .probe_all()
                                    .await
                                    .into_iter()
                                    .map(|(id, probe)| (id, Msat(probe.spendable_msat)))
                                    .collect();
                                commit_tick_invoked = true;
                                match client
                                    .commit_tick_with_facts(
                                        round,
                                        commit_balances,
                                        balance_facts,
                                        tick_key.clone(),
                                    )
                                    .await
                                {
                                    Ok(report) => {
                                        commit = report;
                                    }
                                    Err(error) => {
                                        tick_failed = true;
                                        tracing::warn!(
                                            ?error,
                                            "watch scheduler: tick commit failed; continuing cycle"
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                if let Some(tick_key) = &tick_key {
                                    record_tick_planning_failed(
                                        runtime,
                                        tick_key,
                                        &error.to_string(),
                                    )
                                    .await;
                                }
                                tracing::warn!(
                                    ?error,
                                    "watch scheduler: balance-token issuance failed; terminalized tick"
                                );
                                // A failed tick still has cycle work: probes/discovery/deadlines
                                // must run so an independent recovery is not delayed until the
                                // next scheduler wakeup.
                                tick_failed = true;
                            }
                        }
                    }
                    spending
                }
                Err(error) => {
                    tick_failed = true;
                    if let Some(tick_key) = &tick_key {
                        record_tick_planning_failed(runtime, tick_key, &error.to_string()).await;
                    }
                    tracing::warn!(
                        ?error,
                        "watch scheduler: tick planning failed; continuing cycle"
                    );
                    None
                }
            }
        }
    };
    if !commit_tick_invoked {
        abandon_parked_handoff_after_watch_fault(client).await;
    }

    let (policy, balances, spending, fresh_snapshot) = match current_policy_and_spending(
        runtime, client, occurrence,
    )
    .await
    {
        Ok(fresh) => {
            let policy = fresh.snapshot.policy().clone();
            match fresh.spending {
                Ok(spending) if may_commit => {
                    (policy, fresh.balances, spending, Some(fresh.snapshot))
                }
                Ok(_) => {
                    // A failed reconcile means durable ownership is unknown. The atomic policy
                    // sample remains useful for retained scheduling, but it cannot authorize a
                    // new money-moving probe.
                    (policy, fresh.balances, None, None)
                }
                Err(error) => {
                    tick_failed = true;
                    tracing::warn!(
                        %error,
                        "watch scheduler: designation failed; continuing with retained probes only"
                    );
                    (policy, fresh.balances, None, None)
                }
            }
        }
        Err(error) => {
            tick_failed = true;
            tracing::warn!(
                    ?error,
                    "watch scheduler: probe-policy snapshot failed; continuing with retained probes only"
                );
            (
                client.get_policy().await.map_err(anyhow::Error::new)?,
                balances,
                None,
                None,
            )
        }
    };
    let probe_now = now_ms();
    let mut tick_policy = crate::tick::TickPolicy::from(&policy);
    tick_policy.occurrence = occurrence;
    tick_policy.now = probe_now;
    let watch_policy = policy.watch_policy();
    before_due_probes_test_hook(runtime, occurrence, spending, &balances).await;
    let (due_probes, defer_fresh_probes) = runtime
        .service_due_probes(
            spending,
            &tick_policy,
            &watch_policy,
            &balances,
            fresh_snapshot.as_ref(),
            probe_now,
            occurrence,
        )
        .await?;
    let attempted_probes = due_probes.len();
    let mut registry_owned_probes = BTreeSet::new();
    let mut retry_probes = BTreeSet::new();
    before_decide_probes_test_hook(runtime, occurrence, &due_probes).await;
    for candidate in due_probes {
        let federation = candidate.federation;
        match client.decide_probe(candidate).await {
            Ok(decision) if decision.deduplicated => {
                registry_owned_probes.insert(decision.candidate);
            }
            Ok(_) => {}
            Err(error) => {
                retry_probes.insert(federation);
                tracing::warn!(federation = %federation.to_hex(), ?error, "watch scheduler: probe refused");
            }
        }
    }
    let discovery_before = runtime
        .service_journal()
        .get_watch_state()
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let policy = client.get_policy().await.map_err(anyhow::Error::new)?;
    let discover_now = now_ms();
    let discovery_policy = policy.discovery_policy();
    let watch_policy = policy.watch_policy();
    // Discovery collection, preview, and SDK join can all wait on the network.  Pass authority
    // through to auto-join instead: it acquires the lease only for its final registry + map
    // publication, so a discovery pass does not live-fence unrelated tick authority.
    runtime
        .service_discover_cycle(
            sources,
            &discovery_policy,
            &policy.probe_policy(),
            &watch_policy,
            occurrence,
            discover_now,
            Some(client),
        )
        .await?;
    let discovery_after = runtime
        .service_journal()
        .get_watch_state()
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let policy = client.get_policy().await.map_err(anyhow::Error::new)?;
    let deadline_now = now_ms();
    let mut tick_policy = crate::tick::TickPolicy::from(&policy);
    tick_policy.occurrence = occurrence;
    tick_policy.now = deadline_now;
    let watch_policy = policy.watch_policy();
    let deadlines = runtime
        .service_watch_deadlines(
            &tick_policy,
            &watch_policy,
            deadline_now,
            &registry_owned_probes,
            &retry_probes,
            defer_fresh_probes,
        )
        .await?;
    Ok(CycleResult {
        deadlines,
        noop: !tick_failed
            && reconcile.as_ref().is_some_and(ReconcileReport::is_idle)
            && decision_count == 0
            && commit.accepted.is_empty()
            && commit.refused.is_empty()
            && attempted_probes == 0
            && discovery_before == discovery_after,
        // A cycle that reached planning is by definition not fenced.
        automation_blocked: None,
    })
}

// This test-only rendezvous is deliberately after actor planning and before balance-token
// issuance. It lets the regression test create an actor lease in the narrow production race
// without guessing at task scheduling or adding a production failure-injection knob. Hooks are
// keyed by the specific runtime allocation and cycle occurrence: concurrently running scheduler
// tests therefore cannot consume each other's rendezvous.
#[cfg(test)]
type AfterTickPlanTestHook = oneshot::Sender<oneshot::Sender<()>>;
#[cfg(test)]
type AfterTickPlanTestHookKey = (usize, Occurrence);
#[cfg(test)]
type AfterTickPlanTestHooks = BTreeMap<AfterTickPlanTestHookKey, AfterTickPlanTestHook>;
#[cfg(test)]
static AFTER_TICK_PLAN_TEST_HOOKS: OnceLock<Mutex<AfterTickPlanTestHooks>> = OnceLock::new();

#[cfg(test)]
type AfterReconcileBeforeWatchAdvanceTestHooks =
    BTreeMap<usize, oneshot::Sender<oneshot::Sender<()>>>;
#[cfg(test)]
static AFTER_RECONCILE_BEFORE_WATCH_ADVANCE_TEST_HOOKS: OnceLock<
    Mutex<AfterReconcileBeforeWatchAdvanceTestHooks>,
> = OnceLock::new();

#[cfg(test)]
fn install_after_reconcile_before_watch_advance_test_hook(
    runtime: &Runtime,
) -> oneshot::Receiver<oneshot::Sender<()>> {
    let (sent, receive) = oneshot::channel();
    let previous = AFTER_RECONCILE_BEFORE_WATCH_ADVANCE_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("post-reconcile hook lock poisoned")
        .insert(runtime as *const Runtime as usize, sent);
    assert!(
        previous.is_none(),
        "a test installed two post-reconcile hooks for the same runtime"
    );
    receive
}

#[cfg(test)]
async fn after_reconcile_before_watch_advance_test_hook(runtime: &Runtime) {
    let hook = AFTER_RECONCILE_BEFORE_WATCH_ADVANCE_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("post-reconcile hook lock poisoned")
        .remove(&(runtime as *const Runtime as usize));
    if let Some(hook) = hook {
        let (resume, wait_for_resume) = oneshot::channel();
        let _ = hook.send(resume);
        let _ = wait_for_resume.await;
    }
}

#[cfg(not(test))]
async fn after_reconcile_before_watch_advance_test_hook(_runtime: &Runtime) {}

#[cfg(test)]
type BeforeDueProbesTestHook = oneshot::Sender<(
    Option<wallet_core::FederationId>,
    BTreeMap<wallet_core::FederationId, Msat>,
)>;
#[cfg(test)]
type BeforeDueProbesTestHooks = BTreeMap<AfterTickPlanTestHookKey, BeforeDueProbesTestHook>;
#[cfg(test)]
static BEFORE_DUE_PROBES_TEST_HOOKS: OnceLock<Mutex<BeforeDueProbesTestHooks>> = OnceLock::new();

#[cfg(test)]
type BeforeDecideProbesTestPayload = (
    Vec<(
        wallet_core::FederationId,
        wallet_core::FederationId,
        Option<String>,
    )>,
    oneshot::Sender<()>,
);
#[cfg(test)]
type BeforeDecideProbesTestHook = oneshot::Sender<BeforeDecideProbesTestPayload>;
#[cfg(test)]
type BeforeDecideProbesTestHooks = BTreeMap<AfterTickPlanTestHookKey, BeforeDecideProbesTestHook>;
#[cfg(test)]
static BEFORE_DECIDE_PROBES_TEST_HOOKS: OnceLock<Mutex<BeforeDecideProbesTestHooks>> =
    OnceLock::new();

#[cfg(test)]
fn install_after_tick_plan_test_hook(
    runtime: &Runtime,
    occurrence: Occurrence,
) -> oneshot::Receiver<oneshot::Sender<()>> {
    let (planned, wait_for_planned) = oneshot::channel();
    let previous = AFTER_TICK_PLAN_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("after-plan hook lock poisoned")
        .insert((runtime as *const Runtime as usize, occurrence), planned);
    assert!(
        previous.is_none(),
        "a test installed two post-plan hooks for the same runtime cycle"
    );
    wait_for_planned
}

#[cfg(test)]
async fn after_tick_plan_test_hook(runtime: &Runtime, occurrence: Occurrence) {
    let hook = AFTER_TICK_PLAN_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("after-plan hook lock poisoned")
        .remove(&(runtime as *const Runtime as usize, occurrence));
    if let Some(ready) = hook {
        let (resume, wait_for_resume) = oneshot::channel();
        let _ = ready.send(resume);
        let _ = wait_for_resume.await;
    }
}

#[cfg(not(test))]
async fn after_tick_plan_test_hook(_runtime: &Runtime, _occurrence: Occurrence) {}

#[cfg(test)]
fn install_before_due_probes_test_hook(
    runtime: &Runtime,
    occurrence: Occurrence,
) -> oneshot::Receiver<(
    Option<wallet_core::FederationId>,
    BTreeMap<wallet_core::FederationId, Msat>,
)> {
    let (spending, receive) = oneshot::channel();
    let previous = BEFORE_DUE_PROBES_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("before-due-probes hook lock poisoned")
        .insert((runtime as *const Runtime as usize, occurrence), spending);
    assert!(
        previous.is_none(),
        "a test installed two pre-probe hooks for the same runtime cycle"
    );
    receive
}

#[cfg(test)]
async fn before_due_probes_test_hook(
    runtime: &Runtime,
    occurrence: Occurrence,
    spending: Option<wallet_core::FederationId>,
    balances: &BTreeMap<wallet_core::FederationId, Msat>,
) {
    let hook = BEFORE_DUE_PROBES_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("before-due-probes hook lock poisoned")
        .remove(&(runtime as *const Runtime as usize, occurrence));
    if let Some(hook) = hook {
        let _ = hook.send((spending, balances.clone()));
    }
}

#[cfg(not(test))]
async fn before_due_probes_test_hook(
    _runtime: &Runtime,
    _occurrence: Occurrence,
    _spending: Option<wallet_core::FederationId>,
    _balances: &BTreeMap<wallet_core::FederationId, Msat>,
) {
}

#[cfg(test)]
fn install_before_decide_probes_test_hook(
    runtime: &Runtime,
    occurrence: Occurrence,
) -> oneshot::Receiver<BeforeDecideProbesTestPayload> {
    let (candidates, receive) = oneshot::channel();
    let previous = BEFORE_DECIDE_PROBES_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("before-decide-probes hook lock poisoned")
        .insert((runtime as *const Runtime as usize, occurrence), candidates);
    assert!(
        previous.is_none(),
        "a test installed two pre-DecideProbe hooks for the same runtime cycle"
    );
    receive
}

#[cfg(test)]
async fn before_decide_probes_test_hook(
    runtime: &Runtime,
    occurrence: Occurrence,
    candidates: &[super::ProbeCandidate],
) {
    let hook = BEFORE_DECIDE_PROBES_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("before-decide-probes hook lock poisoned")
        .remove(&(runtime as *const Runtime as usize, occurrence));
    if let Some(ready) = hook {
        let (resume, wait_for_resume) = oneshot::channel();
        let candidates = candidates
            .iter()
            .map(|candidate| {
                let expected_nonce = match &candidate.admission {
                    super::ProbeAdmission::Fresh(_) => None,
                    super::ProbeAdmission::ResumeOnly { expected_nonce } => {
                        Some(expected_nonce.clone())
                    }
                };
                (candidate.federation, candidate.source, expected_nonce)
            })
            .collect();
        let _ = ready.send((candidates, resume));
        let _ = wait_for_resume.await;
    }
}

#[cfg(not(test))]
async fn before_decide_probes_test_hook(
    _runtime: &Runtime,
    _occurrence: Occurrence,
    _candidates: &[super::ProbeCandidate],
) {
}

struct CycleResult {
    deadlines: wallet_core::AdaptiveSleepDeadlines,
    noop: bool,
    /// Set when this cycle refused to plan money work at all. `run` publishes it so `/v1/health`
    /// can report `automation_ready: false` instead of a bare `scheduler_alive: true`.
    automation_blocked: Option<wallet_api::AutomationBlocked>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{FederationInfo, FedimintJournal, WatchState};
    use crate::multi_client::MultiClient;
    use crate::runtime::TickPlan;
    use fedimint_bip39::Mnemonic;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IDatabaseTransactionOpsCore as _;
    use fedimint_core::db::IRawDatabaseExt as _;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use wallet_api::Policy;
    use wallet_core::{
        Action, Actor, AllocatorDecision, AllocatorSnapshot, DiscoverySource, FedBalance,
        FederationStatus, GoalBlockers, Journal, ReasonCode, SourceStatus,
    };

    fn scheduler_replacement_plan(parent: wallet_core::Intent, occurrence: Occurrence) -> TickPlan {
        let Action::Evacuate {
            from,
            to,
            amount,
            fee_cap_components: Some(_),
            ..
        } = &parent.action
        else {
            panic!("scheduler replacement fixture parent must retain its fee-cap components");
        };
        let (from, to, amount) = (*from, *to, *amount);
        let old_key = parent.idempotency_key.clone();
        let fresh_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(20_000),
            bps: 0,
        };
        let fresh = AllocatorDecision {
            action: Action::Evacuate {
                from,
                to,
                amount,
                fee_cap: fresh_cap.at(amount),
                gateway: None,
                fee_cap_components: Some(fresh_cap),
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence,
            idempotency_key: IdempotencyKey(format!(
                "evac:{}:{}:{}",
                from.to_hex(),
                to.to_hex(),
                occurrence.0
            )),
        };
        let evidence = parent
            .evacuation_refusal
            .clone()
            .expect("scheduler replacement fixture parent carries evidence");
        let source = FederationStatus {
            id: from,
            balance: FedBalance {
                spendable: Msat(500_000),
                in_flight: Msat(0),
                claimable: Msat(0),
                reserved_fee: Msat(0),
            },
            probed_ok: true,
            reputation: 0,
            shutdown_notice: true,
            healthy: true,
            eligible_to_fund: true,
        };
        let destination = FederationStatus {
            id: to,
            balance: FedBalance {
                spendable: Msat(0),
                in_flight: Msat(0),
                claimable: Msat(0),
                reserved_fee: Msat(0),
            },
            probed_ok: true,
            reputation: 0,
            shutdown_notice: false,
            healthy: true,
            eligible_to_fund: true,
        };
        TickPlan {
            raw_probes: vec![],
            probes: vec![],
            active_probes: BTreeMap::new(),
            snapshot: AllocatorSnapshot {
                federations: vec![source, destination],
                spending_fed: Some(from),
                standby_fed: Some(to),
                per_fed_cap: Msat(1_000_000),
                target_spending_balance: Msat(0),
                standby_target: Msat(0),
                max_fee: Msat(1_000_000),
                max_fee_bps_of_move: 100,
                evac_fee_base_msat: fresh_cap.base_msat,
                evac_fee_bps: fresh_cap.bps,
                min_move: Msat(1),
                route_economics_by_pair: BTreeMap::new(),
                reservations: wallet_core::Reservations::default(),
                now: 1,
            },
            decisions: vec![],
            suppressed: vec![],
            replacement_deferred: vec![],
            deferred: vec![],
            blockers: GoalBlockers::default(),
            replacement: Some(crate::service::EvacuationReplacementPlan {
                parent,
                old_key,
                old_attempt: 0,
                evidence,
                fresh,
            }),
            marker_disposition: None,
        }
    }

    fn scheduler_marked_parent(
        label: &str,
        from: wallet_core::FederationId,
        to: wallet_core::FederationId,
        occurrence: Occurrence,
    ) -> wallet_core::Intent {
        let cap = wallet_core::EvacFeeCap {
            base_msat: Msat(10_000),
            bps: 0,
        };
        wallet_core::Intent {
            idempotency_key: IdempotencyKey(format!("evac:scheduler-queued-{label}")),
            attempt: 0,
            action: Action::Evacuate {
                from,
                to,
                amount: Msat(100_000),
                fee_cap: cap.at(Msat(100_000)),
                gateway: None,
                fee_cap_components: Some(cap),
            },
            max_fee: Some(cap.at(Msat(100_000))),
            status: wallet_core::IntentStatus::Pending,
            reason: ReasonCode::ShutdownNotice,
            actor: Actor::Agent { occurrence },
            created_at_ms: 1,
            operation_id: None,
            invoice: None,
            evacuation_refusal: Some(wallet_core::EvacuationRefusalEvidence {
                cap_components: cap,
                requested_net: Msat(100_000),
                source_spendable: Msat(500_000),
                low: wallet_core::EvacuationQuoteSample {
                    delivered_net: Msat(10_000),
                    total_fee: Msat(15_000),
                    fee_cap: cap.at(Msat(10_000)),
                },
                high: wallet_core::EvacuationQuoteSample {
                    delivered_net: Msat(100_000),
                    total_fee: Msat(25_000),
                    fee_cap: cap.at(Msat(100_000)),
                },
                diagnostic: format!("scheduler queued marker {label}"),
                measured_at_ms: 1,
            }),
        }
    }

    fn scheduler_replacement_probes(
        parent: &wallet_core::Intent,
    ) -> Vec<(wallet_core::FederationId, crate::probe::ProbeResult)> {
        let Action::Evacuate { from, to, .. } = &parent.action else {
            panic!("scheduler replacement fixture parent must evacuate");
        };
        let probe = |spendable_msat, shutdown_scheduled| crate::probe::ProbeResult {
            guardian_count: 4,
            threshold: 3,
            is_mainnet: true,
            module_kinds: vec!["mint".to_owned(), "wallet".to_owned(), "lnv2".to_owned()],
            has_lnv2: true,
            quorum_live: true,
            latency_ms: 10,
            gateway_available: true,
            wallet_module_present: true,
            expiry_timestamp_secs: None,
            config_expiry_secs: None,
            meta_module_expiry_secs: None,
            status_scheduled_shutdown: shutdown_scheduled,
            shutdown_scheduled,
            spendable_msat,
            in_flight_msat: 0,
            claimable_msat: 0,
        };
        vec![(*from, probe(500_000, true)), (*to, probe(0, false))]
    }

    #[tokio::test]
    async fn partial_joined_view_recovery_only_redrives_old_marker_without_a_tick() {
        let (runtime, service) = scheduler_runtime_fixture(0xF5).await;
        let client = service.client();
        let journal = runtime.service_journal();
        let mut policy = client.get_policy().await.expect("read policy");
        policy.evac_fee_base_msat = Msat(20_000);
        policy.evac_fee_bps = 0;
        client
            .put_policy(policy)
            .await
            .expect("install the qualifying replacement cap");
        journal
            .put_watch_state(&WatchState {
                occurrence: 8,
                ..WatchState::default()
            })
            .await
            .expect("seed scheduler occurrence");
        let parent_key = IdempotencyKey("evac:partial-view-marked-parent".to_owned());
        let cap = wallet_core::EvacFeeCap {
            base_msat: Msat(10_000),
            bps: 0,
        };
        let evidence = wallet_core::EvacuationRefusalEvidence {
            cap_components: cap,
            requested_net: Msat(100_000),
            source_spendable: Msat(500_000),
            low: wallet_core::EvacuationQuoteSample {
                delivered_net: Msat(10_000),
                total_fee: Msat(15_000),
                fee_cap: cap.at(Msat(10_000)),
            },
            high: wallet_core::EvacuationQuoteSample {
                delivered_net: Msat(100_000),
                total_fee: Msat(25_000),
                fee_cap: cap.at(Msat(100_000)),
            },
            diagnostic: "partial-view scheduler fixture".to_owned(),
            measured_at_ms: 1,
        };
        let parent = wallet_core::Intent {
            idempotency_key: parent_key.clone(),
            attempt: 0,
            action: Action::Evacuate {
                from: wallet_core::FederationId([0xA5; 32]),
                to: wallet_core::FederationId([0xB5; 32]),
                amount: Msat(100_000),
                fee_cap: cap.at(Msat(100_000)),
                gateway: None,
                fee_cap_components: Some(cap),
            },
            max_fee: Some(cap.at(Msat(100_000))),
            status: wallet_core::IntentStatus::Pending,
            reason: ReasonCode::ShutdownNotice,
            actor: Actor::Agent {
                occurrence: Occurrence(8),
            },
            created_at_ms: 1,
            operation_id: None,
            invoice: None,
            evacuation_refusal: Some(evidence),
        };
        journal.upsert(&parent).await.expect("seed marked parent");
        let unopened = wallet_core::FederationId([0xC5; 32]);
        let db_prefix = 0xC5;
        journal
            .put_federation(
                &unopened,
                &FederationInfo {
                    // Before the retry fixture is installed this intentionally cannot be opened,
                    // leaving the process with a partial joined/open world-view.
                    invite: "partial-view-retry-open-fixture".to_owned(),
                    db_prefix,
                    joined_at: 0,
                },
            )
            .await
            .expect("register unopened federation");

        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        run_cycle(runtime.as_ref(), &client, &sources)
            .await
            .expect("a partial world-view skips fresh scheduler work");
        for _ in 0..100 {
            let terminal = journal
                .get(&parent_key)
                .await
                .expect("read recovery-only parent")
                .is_some_and(|intent| intent.status == wallet_core::IntentStatus::Done);
            if terminal && service.inflight_drivers() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let redriven = journal
            .get(&parent_key)
            .await
            .expect("read recovery-only parent")
            .expect("recovery-only keeps an auditable old parent");
        assert_eq!(redriven.status, wallet_core::IntentStatus::Done);
        assert_eq!(
            redriven.evacuation_refusal, None,
            "the recovery claim, not a direct marker clear, consumes old evidence"
        );
        assert!(
            journal
                .evacuation_supersession(&parent_key)
                .await
                .expect("read replacement sidecar")
                .is_none(),
            "a partial world-view may re-drive old work but cannot create a replacement child"
        );
        assert!(
            !journal
                .history(usize::MAX, None)
                .await
                .expect("history during partial cycle")
                .iter()
                .any(|row| matches!(row.kind, wallet_core::OperationKind::Tick { .. })),
            "a partial world-view must not open a fresh Tick while it re-drives old work"
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn corrupt_federation_registry_redrives_old_marker_without_fresh_scheduler_work() {
        let executor = Arc::new(KeyRecordingExecutor::default());
        let (runtime, service, journal_db) =
            scheduler_runtime_fixture_with_executor_and_journal_db(0xF6, executor.clone()).await;
        let client = service.client();
        let journal = runtime.service_journal();
        let mut policy = client.get_policy().await.expect("read policy");
        policy.evac_fee_base_msat = Msat(20_000);
        policy.evac_fee_bps = 0;
        client
            .put_policy(policy)
            .await
            .expect("install the qualifying replacement cap");
        journal
            .put_watch_state(&WatchState {
                occurrence: 8,
                ..WatchState::default()
            })
            .await
            .expect("seed scheduler occurrence");
        let parent_key = IdempotencyKey("evac:corrupt-registry-marked-parent".to_owned());
        let cap = wallet_core::EvacFeeCap {
            base_msat: Msat(10_000),
            bps: 0,
        };
        let parent = wallet_core::Intent {
            idempotency_key: parent_key.clone(),
            attempt: 0,
            action: Action::Evacuate {
                from: wallet_core::FederationId([0xA6; 32]),
                to: wallet_core::FederationId([0xB6; 32]),
                amount: Msat(100_000),
                fee_cap: cap.at(Msat(100_000)),
                gateway: None,
                fee_cap_components: Some(cap),
            },
            max_fee: Some(cap.at(Msat(100_000))),
            status: wallet_core::IntentStatus::Pending,
            reason: ReasonCode::ShutdownNotice,
            actor: Actor::Agent {
                occurrence: Occurrence(8),
            },
            created_at_ms: 1,
            operation_id: None,
            invoice: None,
            evacuation_refusal: Some(wallet_core::EvacuationRefusalEvidence {
                cap_components: cap,
                requested_net: Msat(100_000),
                source_spendable: Msat(500_000),
                low: wallet_core::EvacuationQuoteSample {
                    delivered_net: Msat(10_000),
                    total_fee: Msat(15_000),
                    fee_cap: cap.at(Msat(10_000)),
                },
                high: wallet_core::EvacuationQuoteSample {
                    delivered_net: Msat(100_000),
                    total_fee: Msat(25_000),
                    fee_cap: cap.at(Msat(100_000)),
                },
                diagnostic: "corrupt registry scheduler fixture".to_owned(),
                measured_at_ms: 1,
            }),
        };
        journal.upsert(&parent).await.expect("seed marked parent");
        let healthy = wallet_core::FederationId([0xD6; 32]);
        let healthy_db_prefix = 0xD6;
        journal
            .put_federation(
                &healthy,
                &FederationInfo {
                    // This explicit open seam makes the healthy subset observable: removing or
                    // moving the poison gate would publish it before discovery/probes can run.
                    invite: "corrupt-registry-healthy-open-fixture".to_owned(),
                    db_prefix: healthy_db_prefix,
                    joined_at: 0,
                },
            )
            .await
            .expect("register healthy federation alongside poison row");
        runtime
            .service_multi_client()
            .install_retry_open_fixture(healthy_db_prefix, healthy);
        let app_db = journal_db.with_prefix(vec![0x00]);
        let mut dbtx = app_db.begin_transaction().await;
        let mut key = vec![0x03];
        key.extend_from_slice(&[0xC6; 32]);
        dbtx.raw_insert_bytes(&key, b"not valid json")
            .await
            .expect("insert corrupt federation registry row");
        dbtx.commit_tx_result()
            .await
            .expect("commit corrupt federation registry row");

        let discovery_calls = Arc::new(AtomicUsize::new(0));
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(CountingDiscoverySource {
            calls: Arc::clone(&discovery_calls),
        })];
        let cycle = run_cycle(runtime.as_ref(), &client, &sources)
            .await
            .expect("corrupt registry limits the cycle to recovery");

        assert_eq!(
            cycle.deadlines,
            wallet_core::AdaptiveSleepDeadlines::default(),
            "an unknown world must retain conservative default deadlines"
        );
        assert!(
            !cycle.noop,
            "the warning recovery cycle is not a scheduler no-op"
        );
        assert!(
            !runtime
                .service_multi_client()
                .federations()
                .contains(&healthy),
            "a corrupt registry must fence healthy-subset federation opening before probes"
        );
        assert_eq!(
            discovery_calls.load(Ordering::SeqCst),
            0,
            "a corrupt registry must not collect fresh discovery candidates from the healthy subset"
        );
        for _ in 0..100 {
            let terminal = journal
                .get(&parent_key)
                .await
                .expect("read recovery-only parent")
                .is_some_and(|intent| intent.status == wallet_core::IntentStatus::Done);
            if terminal && service.inflight_drivers() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let redriven = journal
            .get(&parent_key)
            .await
            .expect("read recovery-only parent")
            .expect("recovery-only keeps an auditable old parent");
        assert_eq!(redriven.status, wallet_core::IntentStatus::Done);
        assert_eq!(
            redriven.evacuation_refusal, None,
            "the RecoveryOnly claim consumes the durable marker instead of planning a replacement"
        );
        assert!(
            journal
                .evacuation_supersession(&parent_key)
                .await
                .expect("read replacement sidecar")
                .is_none(),
            "an unknown registry cannot produce a fresh replacement decision"
        );
        assert!(
            !journal
                .history(usize::MAX, None)
                .await
                .expect("history during corrupt-registry cycle")
                .iter()
                .any(|row| {
                    matches!(
                        row.kind,
                        wallet_core::OperationKind::Probe { .. }
                            | wallet_core::OperationKind::Tick { .. }
                    )
                }),
            "a corrupt registry must not allocate fresh probe or Tick work"
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn post_reconcile_watch_advance_fault_abandons_handoff_and_preserves_marker_for_retry() {
        let (runtime, service) = empty_scheduler_fixture(0xF4).await;
        let client = service.client();
        let journal = runtime.service_journal();
        let key = IdempotencyKey("evac:post-reconcile-floor-fault".to_owned());
        let cap = wallet_core::EvacFeeCap {
            base_msat: Msat(10_000),
            bps: 0,
        };
        let evidence = wallet_core::EvacuationRefusalEvidence {
            cap_components: cap,
            requested_net: Msat(100_000),
            source_spendable: Msat(500_000),
            low: wallet_core::EvacuationQuoteSample {
                delivered_net: Msat(10_000),
                total_fee: Msat(15_000),
                fee_cap: cap.at(Msat(10_000)),
            },
            high: wallet_core::EvacuationQuoteSample {
                delivered_net: Msat(100_000),
                total_fee: Msat(25_000),
                fee_cap: cap.at(Msat(100_000)),
            },
            diagnostic: "post-reconcile fault fixture".to_owned(),
            measured_at_ms: 1,
        };
        let parent = wallet_core::Intent {
            idempotency_key: key.clone(),
            attempt: 0,
            action: Action::Evacuate {
                from: wallet_core::FederationId([0xA4; 32]),
                to: wallet_core::FederationId([0xB4; 32]),
                amount: Msat(100_000),
                fee_cap: cap.at(Msat(100_000)),
                gateway: None,
                fee_cap_components: Some(cap),
            },
            max_fee: Some(cap.at(Msat(100_000))),
            status: wallet_core::IntentStatus::Pending,
            reason: ReasonCode::ShutdownNotice,
            actor: Actor::Agent {
                occurrence: Occurrence(8),
            },
            created_at_ms: 1,
            operation_id: None,
            invoice: None,
            evacuation_refusal: Some(evidence),
        };
        journal.upsert(&parent).await.expect("seed marked parent");
        let paused = install_after_reconcile_before_watch_advance_test_hook(runtime.as_ref());
        let cycle_runtime = Arc::clone(&runtime);
        let cycle_client = client.clone();
        let cycle = tokio::spawn(async move {
            let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
            run_cycle(cycle_runtime.as_ref(), &cycle_client, &sources).await
        });
        let resume = tokio::time::timeout(Duration::from_secs(5), paused)
            .await
            .expect("cycle reached ReconcileDecide before allocation")
            .expect("post-reconcile hook installed");
        journal
            .put_watch_state(&WatchState {
                occurrence: u64::MAX,
                ..WatchState::default()
            })
            .await
            .expect("race an exhausted occurrence floor in after the handoff");
        resume.send(()).expect("resume allocation");
        let error = match cycle.await.expect("cycle task") {
            Ok(_) => panic!("a post-reconcile allocation fault must fence the cycle"),
            Err(error) => error,
        };
        assert!(
            format!("{error:?}").contains("occurrence exhausted"),
            "{error:?}"
        );
        assert_eq!(
            journal.get(&key).await.expect("read parent after fault"),
            Some(parent.clone()),
            "abandoning the handoff must not clear its durable marker"
        );
        journal
            .put_watch_state(&WatchState {
                occurrence: 8,
                ..WatchState::default()
            })
            .await
            .expect("restore an allocatable checkpoint for the next cycle");
        let planned = install_after_reconcile_before_watch_advance_test_hook(runtime.as_ref());
        let retry_runtime = Arc::clone(&runtime);
        let retry_client = client.clone();
        let retry = tokio::spawn(async move {
            let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
            run_cycle(retry_runtime.as_ref(), &retry_client, &sources).await
        });
        let resume = tokio::time::timeout(Duration::from_secs(5), planned)
            .await
            .expect("repaired cycle recaptures marker into its planner handoff")
            .expect("repaired hook");
        assert_eq!(
            journal.get(&key).await.expect("read marker at recapture"),
            Some(parent),
            "the recaptured marker survives until the cycle gets its planner opportunity"
        );
        resume.send(()).expect("finish repaired cycle");
        retry
            .await
            .expect("retry task")
            .expect("repaired cycle completes");
        service.shutdown().await.expect("shutdown");
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct PausedDiscoverySource {
        started: Mutex<Option<oneshot::Sender<()>>>,
        resume: Mutex<Option<oneshot::Receiver<()>>>,
    }

    #[async_trait::async_trait]
    impl CandidateSource for PausedDiscoverySource {
        fn source(&self) -> DiscoverySource {
            DiscoverySource::Manual
        }

        async fn candidates(&self) -> crate::discovery::SourceResult {
            let started = self
                .started
                .lock()
                .expect("paused source started lock")
                .take();
            if let Some(started) = started {
                let _ = started.send(());
            }
            let resume = self
                .resume
                .lock()
                .expect("paused source resume lock")
                .take();
            if let Some(resume) = resume {
                let _ = resume.await;
            }
            crate::discovery::SourceResult {
                candidates: Vec::new(),
                status: SourceStatus::Ok,
            }
        }
    }

    async fn empty_scheduler_fixture(entropy: u8) -> (Arc<Runtime>, super::super::WalletService) {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic =
            Mnemonic::from_entropy(&[entropy; 16]).expect("valid scheduler fixture mnemonic");
        let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        let runtime = Arc::new(Runtime::new(
            multi_client,
            journal.clone(),
            None,
            None,
            None,
        ));
        let service = super::super::WalletService::start_parts(
            None,
            journal,
            Arc::new(runtime.service_executor(None)),
            Policy::default(),
            None,
        )
        .await
        .expect("start actor-only scheduler fixture");
        (runtime, service)
    }

    #[derive(Default)]
    struct KeyRecordingExecutor {
        performed: Mutex<Vec<IdempotencyKey>>,
    }

    struct CountingDiscoverySource {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl CandidateSource for CountingDiscoverySource {
        fn source(&self) -> DiscoverySource {
            DiscoverySource::Manual
        }

        async fn candidates(&self) -> crate::discovery::SourceResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            crate::discovery::SourceResult {
                candidates: Vec::new(),
                status: SourceStatus::Ok,
            }
        }
    }

    #[async_trait::async_trait]
    impl wallet_core::Executor for KeyRecordingExecutor {
        async fn perform(
            &self,
            intent: &wallet_core::Intent,
        ) -> Result<wallet_core::PerformOutcome, wallet_core::ExecError> {
            self.performed
                .lock()
                .expect("recording executor lock poisoned")
                .push(intent.idempotency_key.clone());
            Ok(wallet_core::PerformOutcome::Done)
        }
    }

    async fn scheduler_runtime_fixture_with_executor(
        entropy: u8,
        executor: Arc<dyn wallet_core::Executor>,
    ) -> (Arc<Runtime>, super::super::WalletService) {
        let (runtime, service, _) =
            scheduler_runtime_fixture_with_executor_and_journal_db(entropy, executor).await;
        (runtime, service)
    }

    async fn scheduler_runtime_fixture_with_executor_and_journal_db(
        entropy: u8,
        executor: Arc<dyn wallet_core::Executor>,
    ) -> (
        Arc<Runtime>,
        super::super::WalletService,
        fedimint_core::db::Database,
    ) {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic =
            Mnemonic::from_entropy(&[entropy; 16]).expect("valid scheduler fixture mnemonic");
        let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db.clone()));
        let runtime = Arc::new(Runtime::new(
            multi_client,
            journal.clone(),
            None,
            None,
            None,
        ));
        runtime.enable_scheduler_tick_fixture_for_test();
        let service = super::super::WalletService::start_parts_inner(
            Some(Arc::clone(&runtime)),
            None,
            journal,
            executor,
            Policy::default(),
            None,
        )
        .await
        .expect("start runtime-backed scheduler fixture");
        (runtime, service, journal_db)
    }

    async fn scheduler_runtime_fixture(entropy: u8) -> (Arc<Runtime>, super::super::WalletService) {
        scheduler_runtime_fixture_with_executor(entropy, Arc::new(wallet_core::MockExecutor::new()))
            .await
    }

    #[tokio::test]
    async fn run_cycle_executes_the_final_max_occurrence_once_then_fails_closed() {
        let (runtime, service) = empty_scheduler_fixture(0xD7).await;
        let journal = runtime.service_journal();
        journal
            .put_watch_state(&WatchState {
                occurrence: u64::MAX - 1,
                ..WatchState::default()
            })
            .await
            .expect("seed the final scheduler floor");
        let client = service.client();
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();

        run_cycle(runtime.as_ref(), &client, &sources)
            .await
            .expect("the scheduler must run its one final allocated occurrence");

        let ticks: Vec<_> = journal
            .history(usize::MAX, None)
            .await
            .expect("read tick history")
            .into_iter()
            .filter(|row| {
                matches!(
                    row.kind,
                    wallet_core::OperationKind::Tick {
                        occurrence: Occurrence(u64::MAX),
                        ..
                    }
                )
            })
            .collect();
        assert_eq!(ticks.len(), 1, "the final occurrence opens one tick row");
        assert_eq!(ticks[0].status, OperationStatus::Succeeded);
        assert!(matches!(
            ticks[0].kind,
            wallet_core::OperationKind::Tick {
                occurrence: Occurrence(u64::MAX),
                decisions: 0,
                performed: 0,
                failed: 0,
            }
        ));
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read consumed final floor")
                .occurrence,
            u64::MAX
        );

        let error = match run_cycle(runtime.as_ref(), &client, &sources).await {
            Ok(_) => panic!("the cycle after MAX must fail checked occurrence exhaustion"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("watch scheduler occurrence exhausted"),
            "{error:#}"
        );
        let final_ticks = journal
            .history(usize::MAX, None)
            .await
            .expect("read final tick history")
            .into_iter()
            .filter(|row| {
                matches!(
                    row.kind,
                    wallet_core::OperationKind::Tick {
                        occurrence: Occurrence(u64::MAX),
                        ..
                    }
                )
            })
            .count();
        assert_eq!(
            final_ticks, 1,
            "a rejected post-MAX cycle must not duplicate final scheduler work"
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn abort_arm_cancels_a_stuck_cycle_promptly() {
        let (abort, mut abort_rx) = oneshot::channel();
        let task =
            tokio::spawn(
                async move { abortable(std::future::pending::<()>(), &mut abort_rx).await },
            );
        abort.send(()).expect("scheduler is listening");
        tokio::task::yield_now().await;
        assert_eq!(task.await.expect("join"), None);
    }

    #[tokio::test]
    async fn dropping_scheduler_subscription_tasks_aborts_their_streams() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();
        let (started, started_rx) = oneshot::channel();
        let task = fedimint_runtime::spawn("test-expiry-wake", async move {
            let _drop_flag = DropFlag(dropped_in_task);
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("subscription task started");

        drop(ExpiryWakeTasks(vec![task]));
        for _ in 0..100 {
            if dropped.load(Ordering::SeqCst) {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("subscription future remained live after scheduler task ownership dropped");
    }

    #[tokio::test(start_paused = true)]
    async fn policy_wake_preempts_the_old_long_sleep() {
        let (wake, mut wake_rx) = watch::channel(0_u64);
        let (_abort, mut abort_rx) = oneshot::channel();
        let started = tokio::time::Instant::now();
        let task = tokio::spawn(async move {
            wait_for_policy_or_sleep(10 * 60 * 1_000, &mut wake_rx, &mut abort_rx).await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        wake.send_modify(|generation| *generation += 1);
        tokio::task::yield_now().await;
        assert_eq!(task.await.expect("join"), WaitEvent::Policy);
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_secs(1)
        );
    }

    #[test]
    fn tick_commit_stays_fail_closed_only_when_reconcile_failed() {
        // A FAILED reconcile leaves the durable state unknown: still a global no-commit.
        assert!(!tick_may_commit(&None));
        // A successful pass that re-drove work may still commit (br-p93): the conflict-scoped
        // `blocked` set — not the global count — decides which decisions are withheld.
        let redrove = ReconcileReport {
            redriven: 1,
            ..Default::default()
        };
        assert!(tick_may_commit(&Some(redrove.clone())));
        assert!(tick_may_commit(&Some(ReconcileReport::default())));
        // A re-drive is still not an idle cycle, so subscription coalescing is unaffected...
        assert!(!redrove.is_idle());
        assert!(ReconcileReport::default().is_idle());
        // ...while a standing blocker set alone (a registry-owned goal, nothing re-driven) must
        // NOT make a quiet cycle look busy.
        let held = wallet_core::Intent {
            idempotency_key: IdempotencyKey("evac:held".to_owned()),
            attempt: 0,
            action: wallet_core::Action::Evacuate {
                from: wallet_core::FederationId([0x11; 32]),
                to: wallet_core::FederationId([0x22; 32]),
                amount: Msat(10),
                fee_cap: Msat(1),
                gateway: None,
                fee_cap_components: None,
            },
            max_fee: Some(Msat(1)),
            status: wallet_core::IntentStatus::Executing,
            reason: wallet_core::ReasonCode::ShutdownNotice,
            actor: Actor::Agent {
                occurrence: Occurrence(1),
            },
            created_at_ms: 0,
            operation_id: None,
            invoice: None,
            evacuation_refusal: None,
        };
        let owned = ReconcileReport {
            blocked: wallet_core::GoalBlockers::from_intents(std::slice::from_ref(&held)),
            ..ReconcileReport::default()
        };
        assert!(!owned.blocked.is_empty());
        assert!(owned.is_idle());
    }

    /// br-p93: the two lines that carry the reconcile's eligibility into the cycle's planning
    /// facts. Dropping `blocked` here breaks no other assertion in the suite — the daemon would
    /// silently stop suppressing at plan time (paying for the blocked pair's route quotes) and
    /// degrade to refusing the same work at commit.
    #[test]
    fn a_cycle_plans_against_the_eligibility_its_reconcile_derived() {
        let held = wallet_core::Intent {
            idempotency_key: IdempotencyKey("evac:held".to_owned()),
            attempt: 0,
            action: wallet_core::Action::Evacuate {
                from: wallet_core::FederationId([0x11; 32]),
                to: wallet_core::FederationId([0x22; 32]),
                amount: Msat(10),
                fee_cap: Msat(1),
                gateway: None,
                fee_cap_components: None,
            },
            max_fee: Some(Msat(1)),
            status: wallet_core::IntentStatus::Pending,
            reason: wallet_core::ReasonCode::ShutdownNotice,
            actor: Actor::Agent {
                occurrence: Occurrence(1),
            },
            created_at_ms: 0,
            operation_id: None,
            invoice: None,
            evacuation_refusal: None,
        };
        // A successful pass that re-drove work: the cycle still prices and still commits, and it
        // plans against exactly the goals that pass left in flight.
        let report = ReconcileReport {
            redriven: 1,
            blocked: wallet_core::GoalBlockers::from_intents(std::slice::from_ref(&held)),
            ..ReconcileReport::default()
        };
        let facts = cycle_probe_facts(&Some(report.clone()), vec![], Occurrence(9), 1_234);
        assert_eq!(facts.occurrence, Occurrence(9));
        assert_eq!(facts.now_ms, 1_234);
        assert!(facts.price_routes, "a successful reconcile may buy quotes");
        assert_eq!(
            facts.blocked.goals(),
            report.blocked.goals(),
            "the report's blocker set must reach planning, not an empty default"
        );

        // A FAILED reconcile: no quotes, and the empty set below is never read — `run_cycle`
        // short-circuits ahead of planning rather than treating it as "nothing is in flight".
        let none = cycle_probe_facts(&None, vec![], Occurrence(9), 1_234);
        assert!(!none.price_routes);
        assert!(none.blocked.is_empty());
    }

    #[test]
    fn production_scheduler_has_an_observer_discovery_source() {
        let sources = default_sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source(), wallet_core::DiscoverySource::Observer);
    }

    #[tokio::test]
    async fn tick_planning_failure_still_reaches_due_discovery() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0_u8; 16]).expect("valid test mnemonic");
        let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        let runtime = Runtime::new(multi_client, journal.clone(), None, None, None);
        let service = super::super::WalletService::start_parts(
            None,
            journal.clone(),
            Arc::new(runtime.service_executor(None)),
            Policy::default(),
            None,
        )
        .await
        .expect("start actor-only service");
        let client = service.client();
        let mut policy = client.get_policy().await.expect("read policy");
        policy.spending_fed = Some(wallet_core::FederationId([0xAA; 32]));
        client
            .put_policy(policy)
            .await
            .expect("pin absent federation");

        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let cycle = run_cycle(&runtime, &client, &sources)
            .await
            .expect("tick failure must not abort the remaining cycle");
        let state = journal.get_watch_state().await.expect("watch state");
        assert_eq!(state.occurrence, 1);
        assert!(state.last_discover_ms > 0, "due discovery still ran");
        assert!(!cycle.noop, "a failed tick step is not a no-op cycle");
        let history = journal.history(usize::MAX, None).await.expect("history");
        assert!(
            history.iter().any(|row| {
                matches!(
                    row.kind,
                    wallet_core::OperationKind::Tick {
                        occurrence: Occurrence(1),
                        ..
                    }
                ) && row.status == OperationStatus::Failed
                    && row
                        .error
                        .as_deref()
                        .is_some_and(|error| error.contains("pinned federation"))
            }),
            "planning failure was not durably terminalized: {history:#?}"
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn designation_fault_keeps_cycle_work_but_never_uses_a_configured_pin_as_probe_source() {
        let (runtime, service) = empty_scheduler_fixture(0xD6).await;
        let client = service.client();
        let pinned = wallet_core::FederationId([0xD6; 32]);
        let mut policy = client.get_policy().await.expect("read policy");
        policy.spending_fed = Some(pinned);
        client.put_policy(policy).await.expect("set configured pin");
        // The pin makes actor planning fail (there is no open federation), so the cycle reaches
        // the same fresh-designation path used after a failed plan. Inject precisely that read
        // fault: the cycle must still discover and collect deadlines, but it has no fresh source.
        runtime.fail_next_scheduler_designations_for_test(1);
        let probe_source = install_before_due_probes_test_hook(runtime.as_ref(), Occurrence(1));
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let cycle = run_cycle(runtime.as_ref(), &client, &sources)
            .await
            .expect("designation fault must not abort discovery/deadline work");
        let (source, _balances) = probe_source
            .await
            .expect("cycle reaches due-probe scheduling despite designation fault");
        assert_eq!(
            source, None,
            "a fresh probe must not fall back to the configured pin after designation failed"
        );
        let state = runtime
            .service_journal()
            .get_watch_state()
            .await
            .expect("watch state");
        assert!(
            state.last_discover_ms > 0,
            "discovery continues after a designation read fault"
        );
        assert!(!cycle.noop, "a designation-fault cycle is not a no-op");
        service.shutdown().await.expect("shutdown");
    }

    /// A scheduler that is ALIVE but refusing to plan must say so.
    ///
    /// The §15.8 partial-view fence is correct — an unopened joined federation would silently
    /// vanish from balances, probes, and every allocation the cycle plans, so refusing is right.
    /// What is wrong is that it refuses in silence: `scheduler_alive` stays `true` because the
    /// loop is healthy, and a `warn!` line is the only trace. This wallet has already lost weeks
    /// to two other fail-closed refusals nothing could see (a shortfall below the move floor for
    /// 27 days; three undecodable ledger rows disabling probing). An operator must be able to
    /// poll the difference between "idle and healthy" and "alive and permanently fenced".
    #[tokio::test]
    async fn a_partial_federation_view_reports_why_automation_is_blocked() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0x51; 16]).expect("valid test mnemonic");
        let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        let runtime = Runtime::new(multi_client.clone(), journal.clone(), None, None, None);
        let service = super::super::WalletService::start_parts(
            None,
            journal.clone(),
            Arc::new(runtime.service_executor(None)),
            Policy::default(),
            None,
        )
        .await
        .expect("start actor-only service");
        let client = service.client();

        // Registered but not openable: the invite never parses, so the retry-open leaves it
        // missing and the cycle must fence. No retry-open fixture is installed here.
        let unopenable = wallet_core::FederationId([0x51; 32]);
        journal
            .put_federation(
                &unopenable,
                &FederationInfo {
                    invite: "not-a-parseable-invite".to_owned(),
                    db_prefix: 51,
                    joined_at: 0,
                },
            )
            .await
            .expect("register unopened federation");

        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let cycle = run_cycle(&runtime, &client, &sources)
            .await
            .expect("the fence skips the cycle; it does not abort the scheduler");

        assert!(
            !multi_client.federations().contains(&unopenable),
            "the fixture must actually leave the federation unopened"
        );
        let blocked = cycle
            .automation_blocked
            .expect("a fenced cycle must report WHY, not just skip silently");
        assert_eq!(blocked.reason, "partial_federation_view");
        assert!(
            blocked.detail.contains(&unopenable.to_hex()),
            "the detail must name the offending federation so an operator can act on the page              without reading logs: {}",
            blocked.detail
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn retry_open_reconciles_again_before_planning_a_tick() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0x49; 16]).expect("valid test mnemonic");
        let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        let runtime = Runtime::new(multi_client.clone(), journal.clone(), None, None, None);
        let service = super::super::WalletService::start_parts(
            None,
            journal.clone(),
            Arc::new(runtime.service_executor(None)),
            Policy::default(),
            None,
        )
        .await
        .expect("start actor-only service");
        let client = service.client();
        let reopened = wallet_core::FederationId([0x49; 32]);
        let db_prefix = 49;
        journal
            .put_federation(
                &reopened,
                &FederationInfo {
                    // The retry-open fixture intercepts this registry row before the SDK parses
                    // the invite, while still exercising run_cycle ->
                    // open_all_with_membership_lease -> actor membership publication.
                    invite: "scheduler-retry-open-fixture".to_owned(),
                    db_prefix,
                    joined_at: 0,
                },
            )
            .await
            .expect("register unopened federation");
        multi_client.install_retry_open_fixture(db_prefix, reopened);

        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let cycle = run_cycle(&runtime, &client, &sources)
            .await
            .expect("successful retry-open must not abort the cycle");

        assert!(
            multi_client.federations().contains(&reopened),
            "the actual retry-open seam must publish the missing federation"
        );
        assert!(
            !cycle.noop,
            "due discovery still makes this scheduler cycle observable after the healthy reopen"
        );
        let history = journal.history(usize::MAX, None).await.expect("history");
        let ticks = history
            .iter()
            .filter(|row| {
                matches!(
                    row.kind,
                    wallet_core::OperationKind::Tick {
                        occurrence: Occurrence(1),
                        ..
                    }
                )
            })
            .collect::<Vec<_>>();
        assert!(
            !ticks.is_empty(),
            "the fresh post-open reconcile token must let this cycle reach tick publication: {history:#?}"
        );
        assert!(
            ticks
                .iter()
                .all(|row| row.status != OperationStatus::Failed),
            "the token minted before retry-open was invalidated by membership publication; \
             a fresh reconcile must prevent a false failed tick: {ticks:#?}"
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn discovery_network_wait_does_not_live_fence_tick_authority() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[7_u8; 16]).expect("valid test mnemonic");
        let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        let runtime = Arc::new(Runtime::new(
            multi_client,
            journal.clone(),
            None,
            None,
            None,
        ));
        let service = super::super::WalletService::start_parts(
            None,
            journal,
            Arc::new(runtime.service_executor(None)),
            Policy::default(),
            None,
        )
        .await
        .expect("start actor-only service");
        let client = service.client();
        let (started_tx, started_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        let sources: Vec<Box<dyn CandidateSource>> = vec![Box::new(PausedDiscoverySource {
            started: Mutex::new(Some(started_tx)),
            resume: Mutex::new(Some(resume_rx)),
        })];
        let cycle_runtime = Arc::clone(&runtime);
        let cycle_client = client.clone();
        let cycle = tokio::spawn(async move {
            run_cycle(cycle_runtime.as_ref(), &cycle_client, &sources).await
        });
        started_rx
            .await
            .expect("scheduler reached discovery source network work");
        client
            .issue_tick_plan_token()
            .await
            .expect("discovery collection must not hold membership authority");
        resume_tx.send(()).expect("resume discovery source");
        cycle
            .await
            .expect("cycle task")
            .expect("cycle completes after discovery source resumes");
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn post_plan_balance_token_failure_still_runs_discovery_and_deadline_collection() {
        let (runtime, service) = scheduler_runtime_fixture(0xB9).await;
        let journal = runtime.service_journal();
        let client = service.client();
        let parent_key = IdempotencyKey("evac:post-plan-token-failure-parent".to_owned());
        let old_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(10_000),
            bps: 0,
        };
        let evidence = wallet_core::EvacuationRefusalEvidence {
            cap_components: old_cap,
            requested_net: Msat(100_000),
            source_spendable: Msat(500_000),
            low: wallet_core::EvacuationQuoteSample {
                delivered_net: Msat(10_000),
                total_fee: Msat(15_000),
                fee_cap: old_cap.at(Msat(10_000)),
            },
            high: wallet_core::EvacuationQuoteSample {
                delivered_net: Msat(100_000),
                total_fee: Msat(25_000),
                fee_cap: old_cap.at(Msat(100_000)),
            },
            diagnostic: "post-plan balance-token failure fixture".to_owned(),
            measured_at_ms: 1,
        };
        let parent = wallet_core::Intent {
            idempotency_key: parent_key.clone(),
            attempt: 0,
            action: Action::Evacuate {
                from: wallet_core::FederationId([0xB9; 32]),
                to: wallet_core::FederationId([0xBA; 32]),
                amount: Msat(100_000),
                fee_cap: old_cap.at(Msat(100_000)),
                gateway: None,
                fee_cap_components: Some(old_cap),
            },
            max_fee: Some(old_cap.at(Msat(100_000))),
            status: wallet_core::IntentStatus::Pending,
            reason: ReasonCode::ShutdownNotice,
            actor: Actor::Agent {
                occurrence: Occurrence(0),
            },
            created_at_ms: 1,
            operation_id: None,
            invoice: None,
            evacuation_refusal: Some(evidence.clone()),
        };
        journal
            .upsert(&parent)
            .await
            .expect("seed qualifying marker");
        let mut policy = client.get_policy().await.expect("read policy");
        policy.evac_fee_base_msat = Msat(20_000);
        policy.evac_fee_bps = 0;
        client
            .put_policy(policy)
            .await
            .expect("install qualifying replacement cap");
        runtime.set_scheduler_tick_test_fixture(scheduler_replacement_plan(
            parent.clone(),
            Occurrence(1),
        ));
        runtime.set_scheduler_probe_fixture(scheduler_replacement_probes(&parent));

        let planned_rx = install_after_tick_plan_test_hook(runtime.as_ref(), Occurrence(1));
        let cycle_runtime = runtime.clone();
        let cycle_client = client.clone();
        let cycle = tokio::spawn(async move {
            let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
            run_cycle(cycle_runtime.as_ref(), &cycle_client, &sources).await
        });
        let resume = planned_rx
            .await
            .expect("scheduler reached post-plan pre-balance-token seam");
        let lease = client
            .begin_external_terminal_mutation(Action::DirectInflow {
                to: wallet_core::FederationId([9; 32]),
                amount: Msat(1),
                fee_cap: Msat(0),
            })
            .await
            .expect("force only balance-token issuance to fail after planning");
        resume.send(()).expect("resume scheduler");
        let cycle = cycle
            .await
            .expect("cycle task")
            .expect("balance-token failure does not abort the cycle");
        client
            .end_external_terminal_mutation(lease)
            .await
            .expect("release injected failure");

        let state = journal.get_watch_state().await.expect("watch state");
        assert_eq!(state.occurrence, 1);
        assert!(state.last_discover_ms > 0, "due discovery still ran");
        assert_eq!(
            cycle.deadlines.last_discover_ms, state.last_discover_ms,
            "the cycle still collected its adaptive deadline snapshot"
        );
        assert!(!cycle.noop, "a post-plan failed tick is not a no-op cycle");
        let history = journal.history(usize::MAX, None).await.expect("history");
        assert!(
            history.iter().any(|row| {
                matches!(
                    row.kind,
                    wallet_core::OperationKind::Tick {
                        occurrence: Occurrence(1),
                        ..
                    }
                ) && row.status == OperationStatus::Failed
                    && row.error.as_deref().is_some_and(|error| {
                        error.contains("external terminal mutation lease is in flight")
                    })
            }),
            "post-plan balance-token failure was not terminalized: {history:#?}"
        );
        let healthy = client
            .reconcile()
            .await
            .expect("the next public scheduler reconciliation recaptures the marker");
        assert_eq!(
            healthy.redriven, 0,
            "an abandoned pre-CommitTick handoff must stay planner-owned: {healthy:#?}"
        );
        assert_eq!(
            journal
                .get(&parent_key)
                .await
                .expect("read parent after healthy reconciliation"),
            Some(parent),
            "balance-token failure must not clear the replacement marker"
        );
        service.shutdown().await.expect("shutdown");
    }

    /// Once CommitTick has been invoked, its actor-owned marker outcome is authoritative. The
    /// scheduler must not subsequently abandon the parked queue: an unselected parent remains
    /// available for the actor's bounded next-cycle handoff.
    #[tokio::test]
    async fn invoked_commit_preserves_unselected_parked_marker_for_the_next_plan() {
        let (runtime, service) = scheduler_runtime_fixture(0xBC).await;
        let journal = runtime.service_journal();
        let client = service.client();
        let first = scheduler_marked_parent(
            "first",
            wallet_core::FederationId([0xC1; 32]),
            wallet_core::FederationId([0xC2; 32]),
            Occurrence(0),
        );
        let second = scheduler_marked_parent(
            "second",
            wallet_core::FederationId([0xC3; 32]),
            wallet_core::FederationId([0xC4; 32]),
            Occurrence(0),
        );
        journal.upsert(&first).await.expect("seed first marker");
        journal.upsert(&second).await.expect("seed second marker");
        let mut policy = client.get_policy().await.expect("read policy");
        policy.evac_fee_base_msat = Msat(20_000);
        policy.evac_fee_bps = 0;
        client
            .put_policy(policy)
            .await
            .expect("install qualifying replacement cap");
        runtime.set_scheduler_tick_test_fixture(scheduler_replacement_plan(
            first.clone(),
            Occurrence(1),
        ));
        runtime.set_scheduler_probe_fixture(scheduler_replacement_probes(&first));
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        run_cycle(runtime.as_ref(), &client, &sources)
            .await
            .expect("the first replacement reaches CommitTick");
        assert_eq!(
            client
                .parked_evacuation_handoff_state_for_test()
                .await
                .expect("inspect queue after invoked commit"),
            (2, true),
            "CommitTick invocation must leave actor-owned parked outcomes intact"
        );
        let next = client
            .reconcile()
            .await
            .expect("next cycle releases only the completed first handoff");
        assert_eq!(
            client
                .parked_evacuation_handoff_state_for_test()
                .await
                .expect("inspect next offered marker"),
            (1, true),
            "the unselected parent remains the next bounded handoff"
        );
        runtime.set_scheduler_tick_test_fixture(scheduler_replacement_plan(
            second.clone(),
            Occurrence(2),
        ));
        let next_round = client
            .decide_tick_round(ProbeFacts {
                probes: scheduler_replacement_probes(&second),
                occurrence: Occurrence(2),
                now_ms: 2,
                price_routes: false,
                blocked: next.blocked,
                admission_snapshot: next.admission_snapshot,
            })
            .await
            .expect("next cycle plans the queued second marker");
        assert_eq!(
            next_round
                .replacement
                .as_ref()
                .map(|replacement| &replacement.parent),
            Some(&second),
            "the next plan must select the unselected parent, not abandon its queue"
        );
        service.shutdown().await.expect("shutdown");
    }

    /// CommitTick may return an actor error after it has accepted the scheduler's handoff.  That
    /// invocation still owns the parked queue: its failed replacement consumes only its exact
    /// parent, and the scheduler must not abandon the remaining parent before the next reconcile.
    #[tokio::test]
    async fn errored_invoked_commit_preserves_the_remaining_parked_marker_for_the_next_plan() {
        let (runtime, service) = scheduler_runtime_fixture(0xBD).await;
        let journal = runtime.service_journal();
        let client = service.client();
        let first = scheduler_marked_parent(
            "errored-first",
            wallet_core::FederationId([0xD1; 32]),
            wallet_core::FederationId([0xD2; 32]),
            Occurrence(0),
        );
        let second = scheduler_marked_parent(
            "errored-second",
            wallet_core::FederationId([0xD3; 32]),
            wallet_core::FederationId([0xD4; 32]),
            Occurrence(0),
        );
        journal.upsert(&first).await.expect("seed first marker");
        journal.upsert(&second).await.expect("seed second marker");
        let mut policy = client.get_policy().await.expect("read policy");
        policy.evac_fee_base_msat = Msat(20_000);
        policy.evac_fee_bps = 0;
        client
            .put_policy(policy)
            .await
            .expect("install qualifying replacement cap");
        runtime.set_scheduler_tick_test_fixture(scheduler_replacement_plan(
            first.clone(),
            Occurrence(1),
        ));
        runtime.set_scheduler_probe_fixture(scheduler_replacement_probes(&first));
        let planned = install_after_tick_plan_test_hook(runtime.as_ref(), Occurrence(1));
        let cycle_runtime = Arc::clone(&runtime);
        let cycle_client = client.clone();
        let cycle = tokio::spawn(async move {
            let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
            run_cycle(cycle_runtime.as_ref(), &cycle_client, &sources).await
        });
        let resume = planned
            .await
            .expect("scheduler reached the post-plan pre-CommitTick seam");
        // The next parent read is CommitTick's pre-exchange exact-parent revalidation. It makes
        // the invoked actor return an error without changing either durable marker.
        journal.fail_one_intent_read_after_successes_for_test(0);
        resume.send(()).expect("resume scheduler into CommitTick");
        cycle
            .await
            .expect("scheduler task")
            .expect("an invoked CommitTick error does not abort the scheduler cycle");
        assert_eq!(
            client
                .parked_evacuation_handoff_state_for_test()
                .await
                .expect("inspect queue after the invoked actor error"),
            (1, false),
            "the failed first commit consumes only its own parked offer"
        );
        assert_eq!(
            journal
                .get(&first.idempotency_key)
                .await
                .expect("read failed first parent"),
            Some(first.clone()),
            "the failing pre-exchange read must leave the first marker durable"
        );
        let next = client
            .reconcile()
            .await
            .expect("the next reconciliation preserves the remaining parked parent");
        runtime.set_scheduler_tick_test_fixture(scheduler_replacement_plan(
            second.clone(),
            Occurrence(2),
        ));
        let next_round = client
            .decide_tick_round(ProbeFacts {
                probes: scheduler_replacement_probes(&second),
                occurrence: Occurrence(2),
                now_ms: 2,
                price_routes: false,
                blocked: next.blocked,
                admission_snapshot: next.admission_snapshot,
            })
            .await
            .expect("the next cycle plans the remaining parked marker");
        assert_eq!(
            next_round
                .replacement
                .as_ref()
                .map(|replacement| &replacement.parent),
            Some(&second),
            "the scheduler must not abandon the actor queue after CommitTick was invoked"
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn completed_terminal_on_competing_fed_recomputes_probe_source_without_dropping_independent_work(
    ) {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0x55; 16]).expect("valid test mnemonic");
        let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        let a = wallet_core::FederationId([0xA1; 32]);
        let b = wallet_core::FederationId([0xB2; 32]);
        let c = wallet_core::FederationId([0xC3; 32]);
        let probe = |spendable_msat| crate::probe::ProbeResult {
            guardian_count: 4,
            threshold: 3,
            is_mainnet: true,
            module_kinds: vec!["mint".to_owned(), "wallet".to_owned(), "lnv2".to_owned()],
            has_lnv2: true,
            quorum_live: true,
            latency_ms: 10,
            gateway_available: true,
            wallet_module_present: true,
            expiry_timestamp_secs: None,
            config_expiry_secs: None,
            meta_module_expiry_secs: None,
            status_scheduled_shutdown: false,
            shutdown_scheduled: false,
            spendable_msat,
            in_flight_msat: 0,
            claimable_msat: 0,
        };
        let sampled_probes = vec![(a, probe(0)), (b, probe(100)), (c, probe(10))];
        let independent = AllocatorDecision {
            action: Action::Move {
                from: c,
                to: b,
                amount: Msat(1),
                fee_cap: Msat(0),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence: Occurrence(1),
            idempotency_key: IdempotencyKey("scheduler:independent-after-terminal".to_owned()),
        };
        let status = |id, balance| FederationStatus {
            id,
            balance: FedBalance {
                spendable: Msat(balance),
                in_flight: Msat(0),
                claimable: Msat(0),
                reserved_fee: Msat(0),
            },
            probed_ok: true,
            reputation: 0,
            shutdown_notice: false,
            healthy: true,
            eligible_to_fund: true,
        };
        let mut runtime = Runtime::new(multi_client, journal.clone(), None, None, None);
        // The actor plan selected A, but the fresh raw samples rank B first.  A completed terminal
        // mutation on B must not permit the scheduler to reuse A just because A itself was
        // unchanged: every commit attempt re-senses and re-designates.
        runtime.set_tick_test_fixture(
            Arc::new(wallet_core::MockExecutor::new()),
            TickPlan {
                deferred: vec![],
                raw_probes: sampled_probes.clone(),
                probes: sampled_probes.clone(),
                active_probes: BTreeMap::new(),
                snapshot: AllocatorSnapshot {
                    federations: vec![status(a, 0), status(b, 100), status(c, 10)],
                    spending_fed: Some(a),
                    standby_fed: Some(b),
                    per_fed_cap: Msat(1_000_000),
                    target_spending_balance: Msat(1_000),
                    standby_target: Msat(1_000),
                    max_fee: Msat(1_000),
                    max_fee_bps_of_move: 100,
                    evac_fee_base_msat: Msat(0),
                    evac_fee_bps: 100,
                    min_move: Msat(1),
                    route_economics_by_pair: BTreeMap::new(),
                    reservations: wallet_core::Reservations::default(),
                    now: 1,
                },
                decisions: vec![independent.clone()],
                suppressed: vec![],
                replacement_deferred: vec![],
                blockers: GoalBlockers::default(),
                replacement: None,
                marker_disposition: None,
            },
        );
        runtime.set_scheduler_probe_fixture(sampled_probes);
        let runtime = Arc::new(runtime);
        let service = super::super::WalletService::start_parts(
            Some(Arc::clone(&runtime)),
            journal.clone(),
            Arc::new(runtime.service_executor(None)),
            Policy::default(),
            None,
        )
        .await
        .expect("start scheduler fixture");
        let client = service.client();
        let planned = install_after_tick_plan_test_hook(runtime.as_ref(), Occurrence(1));
        let probe_source = install_before_due_probes_test_hook(runtime.as_ref(), Occurrence(1));
        let cycle_runtime = Arc::clone(&runtime);
        let cycle_client = client.clone();
        let cycle = tokio::spawn(async move {
            let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
            run_cycle(cycle_runtime.as_ref(), &cycle_client, &sources).await
        });

        let resume = tokio::time::timeout(Duration::from_secs(5), planned)
            .await
            .expect("timed out waiting for scheduler post-plan seam")
            .expect("scheduler reached its production post-plan seam");
        let lease = client
            .begin_external_terminal_mutation(Action::DirectInflow {
                to: c,
                amount: Msat(1),
                fee_cap: Msat(0),
            })
            .await
            .expect("completed competing terminal mutation after planning");
        client
            .end_external_terminal_mutation(lease)
            .await
            .expect("complete competing terminal mutation before fresh sampling");
        // The completed terminal mutation changed C's candidate baseline while the actor-owned
        // plan was off-thread.  Its fixture is the fresh probe response that the production
        // scheduler must carry all the way to `service_due_probes`.
        runtime.set_scheduler_probe_fixture(vec![(a, probe(0)), (b, probe(100)), (c, probe(11))]);
        resume
            .send(())
            .expect("resume scheduler after completed mutation");

        let (source, due_balances) = tokio::time::timeout(Duration::from_secs(5), probe_source)
            .await
            .expect("timed out waiting for due-probe source selection")
            .expect("scheduler reached due-probe source selection");
        assert_eq!(
            source,
            Some(b),
            "the source passed to service_due_probes must be re-designated from fresh probes"
        );
        assert_eq!(
            due_balances.get(&c),
            Some(&Msat(11)),
            "the candidate baseline passed to service_due_probes must come from the same fresh \
             sample as its re-designated source, not the pre-plan sample"
        );
        cycle
            .await
            .expect("cycle task")
            .expect("completed competing terminal mutation must not abort the cycle");
        let committed = journal
            .get(&independent.idempotency_key)
            .await
            .expect("read independent decision")
            .is_some();
        assert!(
            committed,
            "the unaffected decision must still commit while the planned source is stale; \
             history={:#?}",
            journal.history(usize::MAX, None).await.expect("history")
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn put_policy_during_plan_redesignates_due_probe_source_under_current_policy() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0x56; 16]).expect("valid test mnemonic");
        let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        let a = wallet_core::FederationId([0xA3; 32]);
        let b = wallet_core::FederationId([0xB4; 32]);
        let probe = |spendable_msat| crate::probe::ProbeResult {
            guardian_count: 4,
            threshold: 3,
            is_mainnet: true,
            module_kinds: vec!["mint".to_owned(), "wallet".to_owned(), "lnv2".to_owned()],
            has_lnv2: true,
            quorum_live: true,
            latency_ms: 10,
            gateway_available: true,
            wallet_module_present: true,
            expiry_timestamp_secs: None,
            config_expiry_secs: None,
            meta_module_expiry_secs: None,
            status_scheduled_shutdown: false,
            shutdown_scheduled: false,
            spendable_msat,
            in_flight_msat: 0,
            claimable_msat: 0,
        };
        let sampled_probes = vec![(a, probe(100)), (b, probe(10))];
        let status = |id, balance| FederationStatus {
            id,
            balance: FedBalance {
                spendable: Msat(balance),
                in_flight: Msat(0),
                claimable: Msat(0),
                reserved_fee: Msat(0),
            },
            probed_ok: true,
            reputation: 0,
            shutdown_notice: false,
            healthy: true,
            eligible_to_fund: true,
        };
        let mut runtime = Runtime::new(multi_client, journal.clone(), None, None, None);
        runtime.set_tick_test_fixture(
            Arc::new(wallet_core::MockExecutor::new()),
            TickPlan {
                deferred: vec![],
                raw_probes: sampled_probes.clone(),
                probes: sampled_probes.clone(),
                active_probes: BTreeMap::new(),
                snapshot: AllocatorSnapshot {
                    federations: vec![status(a, 100), status(b, 10)],
                    spending_fed: Some(a),
                    standby_fed: Some(b),
                    per_fed_cap: Msat(1_000_000),
                    target_spending_balance: Msat(1_000),
                    standby_target: Msat(1_000),
                    max_fee: Msat(1_000),
                    max_fee_bps_of_move: 100,
                    evac_fee_base_msat: Msat(0),
                    evac_fee_bps: 100,
                    min_move: Msat(1),
                    route_economics_by_pair: BTreeMap::new(),
                    reservations: wallet_core::Reservations::default(),
                    now: 1,
                },
                decisions: vec![],
                suppressed: vec![],
                replacement_deferred: vec![],
                blockers: GoalBlockers::default(),
                replacement: None,
                marker_disposition: None,
            },
        );
        runtime.set_scheduler_probe_fixture(sampled_probes);
        let runtime = Arc::new(runtime);
        let service = super::super::WalletService::start_parts(
            Some(Arc::clone(&runtime)),
            journal,
            Arc::new(runtime.service_executor(None)),
            Policy {
                spending_fed: Some(a),
                ..Policy::default()
            },
            None,
        )
        .await
        .expect("start scheduler fixture");
        let client = service.client();
        let planned = install_after_tick_plan_test_hook(runtime.as_ref(), Occurrence(1));
        let probe_source = install_before_due_probes_test_hook(runtime.as_ref(), Occurrence(1));
        let cycle_runtime = Arc::clone(&runtime);
        let cycle_client = client.clone();
        let cycle = tokio::spawn(async move {
            let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
            run_cycle(cycle_runtime.as_ref(), &cycle_client, &sources).await
        });

        let resume = tokio::time::timeout(Duration::from_secs(5), planned)
            .await
            .expect("timed out waiting for scheduler post-plan seam")
            .expect("scheduler reached its production post-plan seam");
        let mut policy = client.get_policy().await.expect("read planned policy");
        policy.spending_fed = Some(b);
        client
            .put_policy(policy)
            .await
            .expect("supersede policy while plan is off actor");
        resume
            .send(())
            .expect("resume scheduler after policy supersession");

        let (source, _due_balances) = tokio::time::timeout(Duration::from_secs(5), probe_source)
            .await
            .expect("timed out waiting for due-probe source selection")
            .expect("scheduler reached due-probe source selection");
        assert_eq!(
            source,
            Some(b),
            "due probes must use policy B, not the pre-plan designation A"
        );
        cycle
            .await
            .expect("cycle task")
            .expect("policy supersession must not abort the cycle");
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn put_policy_after_due_candidates_refuses_the_stale_fresh_probe() {
        let (runtime, service) = empty_scheduler_fixture(0x5A).await;
        let client = service.client();
        let source_a = wallet_core::FederationId([0xA5; 32]);
        let source_b = wallet_core::FederationId([0xB5; 32]);
        let candidate = wallet_core::FederationId([0xC5; 32]);
        let probe = |spendable_msat| crate::probe::ProbeResult {
            guardian_count: 4,
            threshold: 3,
            is_mainnet: true,
            module_kinds: vec!["mint".to_owned(), "wallet".to_owned(), "lnv2".to_owned()],
            has_lnv2: true,
            quorum_live: true,
            latency_ms: 10,
            gateway_available: true,
            wallet_module_present: true,
            expiry_timestamp_secs: None,
            config_expiry_secs: None,
            meta_module_expiry_secs: None,
            status_scheduled_shutdown: false,
            shutdown_scheduled: false,
            spendable_msat,
            in_flight_msat: 0,
            claimable_msat: 0,
        };
        runtime.set_scheduler_probe_fixture(vec![
            (source_a, probe(100)),
            (source_b, probe(50)),
            (candidate, probe(10)),
        ]);
        let mut policy_a = client.get_policy().await.expect("policy A");
        policy_a.spending_fed = Some(source_a);
        policy_a.probe_amount = Msat(111);
        client
            .put_policy(policy_a.clone())
            .await
            .expect("install policy A");

        let planned = install_after_tick_plan_test_hook(runtime.as_ref(), Occurrence(1));
        let before_decide = install_before_decide_probes_test_hook(runtime.as_ref(), Occurrence(1));
        let cycle_runtime = Arc::clone(&runtime);
        let cycle_client = client.clone();
        let cycle = tokio::spawn(async move {
            let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
            run_cycle(cycle_runtime.as_ref(), &cycle_client, &sources).await
        });

        let resume_plan = tokio::time::timeout(Duration::from_secs(5), planned)
            .await
            .expect("timed out waiting for scheduler plan")
            .expect("scheduler reached post-plan seam");
        runtime
            .service_journal()
            .put_federation(
                &candidate,
                &FederationInfo {
                    invite: "probe-policy-snapshot-fixture".to_owned(),
                    db_prefix: 0xC5,
                    joined_at: 0,
                },
            )
            .await
            .expect("publish the due candidate after the cycle's open pass");
        resume_plan
            .send(())
            .expect("resume scheduler into fresh policy snapshot");

        let (candidates, resume_decide) =
            tokio::time::timeout(Duration::from_secs(5), before_decide)
                .await
                .expect("timed out waiting for built due candidates")
                .expect("scheduler reached pre-DecideProbe seam");
        assert_eq!(
            candidates,
            vec![(candidate, source_a, None)],
            "the queued candidate was built under policy A"
        );

        let mut policy_b = policy_a;
        policy_b.spending_fed = Some(source_b);
        policy_b.probe_amount = Msat(222);
        client
            .put_policy(policy_b)
            .await
            .expect("supersede policy after candidates were built");
        resume_decide
            .send(())
            .expect("release stale policy-A candidate");

        cycle
            .await
            .expect("cycle task")
            .expect("a per-candidate policy refusal must not abort the cycle");
        assert!(
            runtime
                .service_journal()
                .probe_record(&candidate)
                .await
                .expect("probe record")
                .and_then(|record| record.in_flight)
                .is_none(),
            "policy-A work must not leave a session after policy B is accepted"
        );
        assert!(
            !runtime
                .service_journal()
                .history(usize::MAX, None)
                .await
                .expect("history")
                .iter()
                .any(|row| {
                    matches!(
                        row.kind,
                        wallet_core::OperationKind::Probe { fed, .. } if fed == candidate
                    ) && row.reason == ReasonCode::ActiveProbe
                }),
            "policy-A work must not write a probe invocation"
        );
        service.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn post_plan_hooks_are_isolated_between_concurrent_cycle_runtimes() {
        let (first_runtime, first_service) = empty_scheduler_fixture(2).await;
        let (second_runtime, second_service) = empty_scheduler_fixture(3).await;
        assert_ne!(
            Arc::as_ptr(&first_runtime),
            Arc::as_ptr(&second_runtime),
            "each concurrently driven fixture needs its own stable hook identity"
        );
        let first_client = first_service.client();
        let second_client = second_service.client();
        let first_planned =
            install_after_tick_plan_test_hook(first_runtime.as_ref(), Occurrence(1));
        let second_planned =
            install_after_tick_plan_test_hook(second_runtime.as_ref(), Occurrence(1));

        let first_cycle = {
            let runtime = Arc::clone(&first_runtime);
            tokio::spawn(async move {
                let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
                run_cycle(runtime.as_ref(), &first_client, &sources).await
            })
        };
        let second_cycle = {
            let runtime = Arc::clone(&second_runtime);
            tokio::spawn(async move {
                let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
                run_cycle(runtime.as_ref(), &second_client, &sources).await
            })
        };

        // A singleton hook lets either cycle take the most recently installed sender, leaving the
        // other receiver unanswered. Both receivers must be reached before either cycle resumes.
        let first_resume = tokio::time::timeout(Duration::from_secs(5), first_planned)
            .await
            .expect("first runtime's hook was not consumed by its own cycle")
            .expect("first cycle dropped its hook");
        let second_resume = tokio::time::timeout(Duration::from_secs(5), second_planned)
            .await
            .expect("second runtime's hook was not consumed by its own cycle")
            .expect("second cycle dropped its hook");
        first_resume.send(()).expect("resume first cycle");
        second_resume.send(()).expect("resume second cycle");
        tokio::time::timeout(Duration::from_secs(5), first_cycle)
            .await
            .expect("first cycle completed after its hook resumed")
            .expect("first cycle task")
            .expect("first cycle result");
        tokio::time::timeout(Duration::from_secs(5), second_cycle)
            .await
            .expect("second cycle completed after its hook resumed")
            .expect("second cycle task")
            .expect("second cycle result");
        first_service
            .shutdown()
            .await
            .expect("shutdown first service");
        second_service
            .shutdown()
            .await
            .expect("shutdown second service");
    }
}
