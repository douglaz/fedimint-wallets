use super::{PolicyExt, ProbeCandidate, ProbeFacts, ReconcileReport, ServiceError, WalletClient};
use crate::discovery::{CandidateSource, ObserverSource};
use crate::runtime::{ledger_nonce, now_ms, Runtime};
use fedimint_core::runtime as fedimint_runtime;
use lightning_invoice::Bolt11Invoice;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use wallet_core::{adaptive_sleep_ms, Actor, IdempotencyKey, Msat, Occurrence, OperationStatus};

const DEFAULT_OBSERVER_URL: &str = "https://observer.fedimint.org/api";

/// How many receives must be stuck Awaiting past the stall deadline before we conclude the
/// fedimint client's shared receive task has died (rather than a single slow/unpaid invoice).
const SETTLEMENT_STALL_THRESHOLD: usize = 3;

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

fn tick_may_commit(reconcile: &Option<ReconcileReport>) -> bool {
    reconcile
        .as_ref()
        .is_some_and(|report| report.redriven == 0)
}

async fn spending_after_failed_tick(
    runtime: &Runtime,
    tick_policy: &crate::tick::TickPolicy,
) -> Option<wallet_core::FederationId> {
    // This failure-only fallback needs designation for scheduling, not a second tick plan.
    // Calling `status()` here would create a fresh route budget and price every pair again,
    // including on a reconcile cycle whose plan was deliberately built with `price_routes=false`.
    let probes = runtime.probe_all().await;
    runtime
        .designated_spending_from_probes(
            tick_policy,
            &wallet_core::ScorerPolicy::default(),
            &probes,
        )
        .await
        .unwrap_or(tick_policy.spending_fed)
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
                }
            }
        };
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

async fn run_cycle(
    runtime: &Runtime,
    client: &WalletClient,
    sources: &[Box<dyn CandidateSource>],
) -> anyhow::Result<CycleResult> {
    let reconcile = match client.reconcile().await {
        Ok(report) => Some(report),
        Err(error) => {
            tracing::warn!(
                ?error,
                "watch scheduler: reconcile failed; continuing cycle"
            );
            None
        }
    };
    // Ledger repair is the deliberate TL-4 exception to "the actor owns every journal
    // write": it is an O(ledger) op-log reconciliation that must not run inside the actor's
    // critical section. It writes directly to the journal off-actor; the phase-4 CAS
    // hardening makes those writes safe against the actor's concurrent transitions. The
    // in-process actor reconcile above only re-drives intents — it does not repair stuck
    // raw-pay/receive/join/tick ledger rows, so the daemon does it here each cycle.
    if let Err(error) = runtime
        .service_journal()
        .repair_ledger(runtime.service_multi_client().as_ref())
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
    let joined = runtime
        .service_journal()
        .list_federations()
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let open: BTreeSet<_> = multi_client.federations().into_iter().collect();
    let missing: Vec<_> = joined
        .iter()
        .filter(|(id, _)| !open.contains(id))
        .map(|(_, info)| info.clone())
        .collect();
    if !missing.is_empty() {
        let _ = multi_client.open_all(&missing).await;
        let open: BTreeSet<_> = multi_client.federations().into_iter().collect();
        let unopened: Vec<_> = joined
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| !open.contains(id))
            .collect();
        if !unopened.is_empty() {
            tracing::warn!(
                unopened = ?unopened.iter().map(|id| id.to_hex()).collect::<Vec<_>>(),
                "watch scheduler: partial federation view; skipping the automated cycle (§15.8)"
            );
            return Ok(CycleResult {
                deadlines: wallet_core::AdaptiveSleepDeadlines::default(),
                noop: false,
            });
        }
    }
    let policy = client.get_policy().await.map_err(anyhow::Error::new)?;
    let watch_state = runtime
        .service_journal()
        .advance_watch_occurrence()
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let occurrence = Occurrence(watch_state.occurrence);
    let mut tick_policy = crate::tick::TickPolicy::from(&policy);
    tick_policy.occurrence = occurrence;
    // Match the synchronous tick audit lifecycle: if this cycle is allowed to tick, open
    // its row before sensing or planning so a crash or planning failure remains visible.
    let tick_key = if tick_may_commit(&reconcile) {
        Some(record_tick_started(runtime, occurrence).await)
    } else {
        None
    };
    let probes = runtime.probe_all().await;
    let sensed_at_ms = now_ms();
    tick_policy.now = sensed_at_ms;
    let balances = probes
        .iter()
        .map(|(id, probe)| (*id, Msat(probe.spendable_msat)))
        .collect::<BTreeMap<_, _>>();
    let mut facts = ProbeFacts {
        probes: probes.clone(),
        occurrence,
        now_ms: sensed_at_ms,
        // Route economics costs live fee quotes + gateway round trips, so only a cycle that can
        // actually COMMIT pays for them: when reconcile re-drove work the round below is planned
        // and thrown away. ONE budget for the whole cycle — the loop can re-plan once per
        // unroutable destination, and a shared deadline is what stops a stalled federation from
        // multiplying its timeout by the revision count.
        price_routes: tick_may_commit(&reconcile),
    };
    let mut decision_count = 0;
    let mut commit = super::CommitTickReport::default();
    let mut tick_failed = false;
    let spending = match &reconcile {
        None => policy.spending_fed,
        Some(_) => {
            facts.now_ms = now_ms();
            let round = client
                .decide_tick_round(facts)
                .await
                .map_err(anyhow::Error::new);
            match round {
                Ok(round) => {
                    let mut spending = round.spending_fed;
                    if tick_may_commit(&reconcile) {
                        decision_count = round.decisions.len();
                        // The generation the round was planned under. A PutPolicy landing
                        // during route validation bumps it; the actor then refuses the batch.
                        let planned_generation = round.planned_generation;
                        // Route validation performs network IO. Re-sample immediately before
                        // admission so a user operation that settled during that window cannot
                        // disappear from reservations while leaving its old balance behind.
                        let commit_balances = runtime
                            .probe_all()
                            .await
                            .into_iter()
                            .map(|(id, probe)| (id, Msat(probe.spendable_msat)))
                            .collect();
                        match client
                            .commit_tick_with_facts(
                                round.decisions,
                                Some(commit_balances),
                                tick_key.clone(),
                                planned_generation,
                            )
                            .await
                        {
                            Ok(report) => commit = report,
                            Err(error) => {
                                tick_failed = true;
                                tracing::warn!(
                                    ?error,
                                    "watch scheduler: tick commit failed; continuing cycle"
                                );
                                spending = spending_after_failed_tick(runtime, &tick_policy).await;
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
                    spending_after_failed_tick(runtime, &tick_policy).await
                }
            }
        }
    };

    let policy = client.get_policy().await.map_err(anyhow::Error::new)?;
    let probe_now = now_ms();
    tick_policy = crate::tick::TickPolicy::from(&policy);
    tick_policy.occurrence = occurrence;
    tick_policy.now = probe_now;
    let watch_policy = policy.watch_policy();
    let (due_probes, defer_fresh_probes) = runtime
        .service_due_probes(
            spending,
            &tick_policy,
            &watch_policy,
            &balances,
            probe_now,
            occurrence,
        )
        .await?;
    let attempted_probes = due_probes.len();
    let mut registry_owned_probes = BTreeSet::new();
    let mut retry_probes = BTreeSet::new();
    for (candidate, source, baseline) in due_probes {
        match client
            .decide_probe(ProbeCandidate {
                federation: candidate,
                source,
                baseline,
                actor: Actor::Agent { occurrence },
                now_ms: probe_now,
            })
            .await
        {
            Ok(decision) if decision.deduplicated => {
                registry_owned_probes.insert(decision.candidate);
            }
            Ok(_) => {}
            Err(error) => {
                retry_probes.insert(candidate);
                tracing::warn!(federation = %candidate.to_hex(), ?error, "watch scheduler: probe refused");
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
    runtime
        .service_discover_cycle(
            sources,
            &discovery_policy,
            &policy.probe_policy(),
            &watch_policy,
            occurrence,
            discover_now,
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
            && reconcile == Some(ReconcileReport::default())
            && decision_count == 0
            && commit.accepted.is_empty()
            && commit.refused.is_empty()
            && attempted_probes == 0
            && discovery_before == discovery_after,
    })
}

struct CycleResult {
    deadlines: wallet_core::AdaptiveSleepDeadlines,
    noop: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::FedimintJournal;
    use crate::multi_client::MultiClient;
    use fedimint_bip39::Mnemonic;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt as _;
    use std::sync::atomic::{AtomicBool, Ordering};
    use wallet_api::Policy;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
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
    fn tick_commit_stays_fail_closed_when_reconcile_failed_or_redrove_work() {
        assert!(!tick_may_commit(&None));
        assert!(!tick_may_commit(&Some(ReconcileReport {
            redriven: 1,
            ..Default::default()
        })));
        assert!(tick_may_commit(&Some(ReconcileReport::default())));
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
}
