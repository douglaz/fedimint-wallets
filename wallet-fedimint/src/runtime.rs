//! [`Runtime`] — the thin async façade the headless frontend drives (spec §9). It owns the
//! shared fedimint I/O (`MultiClient`) + durable journal (`FedimintJournal`) and exposes the
//! engine verbs `wallet-cli` needs on top of `wallet_core::{apply, reconcile}`:
//!
//! - [`Runtime::direct_inflow`] — journal + drive a `DirectInflow` intent (spec §7): the
//!   executor sizes + cap-checks the receive invoice (§6 fixed point), mints it, persists the
//!   `MoveRecord`, and returns `Awaiting`; we then surface the BOLT11 (the payer is external).
//! - [`Runtime::do_move`] — journal + drive a cross-federation `Move` (spec §7): B (`to`)
//!   receives, A (`from`) pays through the shared gateway's internal swap, both legs settle.
//!   Synchronous — `perform` runs the whole two-leg move to `Done` (never `Awaiting`).
//! - [`Runtime::await_move`] — finalize an `Awaiting` inflow: await its `recv_op`, and on the
//!   `Claimed` state mark the intent `Done` via the journal CAS (spec §9.5).
//! - [`Runtime::reconcile`] — the resume loop (spec §9): rebuild `MoveRecord`s from the op-log
//!   for pending + awaiting intents BEFORE re-driving, re-drive `pending()` only (so a `Move`
//!   left `Pending` by a transient fault is re-driven here), then report the still-`Awaiting`
//!   set (finalized out-of-band by `await-move` in a one-shot CLI).
//!
//! `Evacuate` now drives through the executor as a send-required move (Phase 3.A), so the tick
//! can flee a dying federation, not just top up a standby. The `Runtime` holds an optional pinned
//! gateway (⟦D4⟧; devimint's LDK gateway is not auto-registered, runbook §4) that a FRESH move
//! resolves through — a resumed move reuses the gateway already recorded in its `MoveRecord`.

use crate::discovery::{
    auto_join_kind, discover_kind, discovery_actor, run_discover_pass_bounded_with_rotation,
    run_discover_pass_bounded_with_rotation_and_probe_policy,
    run_discover_pass_bounded_with_rotation_and_probe_policy_with_membership_lease,
    AutoJoinAttempt, AutoJoinCounts, CandidateSource, DiscoverPassResume, DiscoverReport,
    DiscoveryBackend, PreviewedCandidate, DISCOVERY_REASON,
};
use crate::executor::FedimintExecutor;
use crate::journal::{
    CandidateListReport, CandidateState, FedimintJournal, OperationRef, ProbeRecord, ProbeSession,
    RawOperationRole, WatchState,
};
use crate::move_protocol::{MovePhase, MoveRecord};
use crate::multi_client::{
    parse_invoice, JoinDeadlineOutcome, MultiClient, ReceiveState, SendState,
};
use crate::probe::{assemble_facts, assemble_status, FedimintProbeRunner, ProbeResult};
use crate::route_econ::RouteQuoteBudget;
use crate::service::{ProbeAdmission, ProbeCandidate, ProbePolicySnapshot, WalletClient};
use crate::tick::{
    build_snapshot, decisions_to_apply, ScoredFed, StatusReport, TickPolicy, TickReport,
};
use crate::types::{GatewayUrl, Invoice};
use async_trait::async_trait;
use bitcoin::hashes::{sha256, Hash as _};
use fedimint_core::config::ClientConfig;
use fedimint_core::encoding::{Decodable, DynRawFallback};
use fedimint_core::invite_code::InviteCode;
use fedimint_core::module::registry::ModuleDecoderRegistry;
use fedimint_core::runtime;
use fedimint_core::NumPeers;
use std::str::FromStr as _;
use std::time::Duration;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use wallet_core::{
    probe_budget_ok, probe_budget_usage, probe_next_due_at, probe_pass_expiry_anchor_ms,
    probe_verdict, probe_wake_due_ms, score, Action, ActiveProbeVerdict, Actor,
    AdaptiveSleepDeadlines, AllocatorDecision, AllocatorSnapshot, DiscoveryPolicy, ExecError,
    ExecutionSummary, Executor, FederationFacts, FederationId, GoalBlockers, IdempotencyKey,
    Intent, IntentStatus, Journal, Module, Msat, Occurrence, OperationId, OperationKind,
    OperationRecord, OperationStatus, PerformOutcome, ProbeAttempt, ProbeBudgetUsage, ProbePolicy,
    ReasonCode, Reservations, ScorerPolicy, WatchPolicy,
};

/// Wall-clock in unix millis for the ledger's `created_at_ms` (§8/§9.4). `seq` is the
/// ordering authority; this is display material, so a pre-epoch clock degrades to `0`
/// rather than failing a money op. The durable §9.4 injected clock is a later run's concern.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) const PROBE_BUDGET_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// A fresh 128-bit nonce as 32 lowercase-hex chars for a per-attempt ledger key (§10.1 — a
/// 32-bit nonce risks birthday collisions over a wallet lifetime, aliasing two attempts onto
/// one `0x06` entry). The runtime owns randomness (the journal stays deterministic, §9.3);
/// this draws from fedimint's CSPRNG.
pub(crate) fn ledger_nonce() -> String {
    use std::fmt::Write as _;
    let bytes = fedimint_core::core::OperationId::new_random().0;
    let mut out = String::with_capacity(32);
    for byte in &bytes[..16] {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The result of a [`Runtime::direct_inflow`] call: the intent's key (the durable handle the
/// operator passes to `await-move`), the surfaced BOLT11 to pay (read from the persisted
/// `MoveRecord`, so a re-run returns the SAME invoice — no second mint), and the intent status.
#[derive(Clone, Debug)]
pub struct DirectInflowOutcome {
    pub key: IdempotencyKey,
    pub invoice: Option<Invoice>,
    pub status: Option<IntentStatus>,
}

/// The result of a [`Runtime::do_move`] call: the move intent's key (the durable handle), the
/// terminal intent status, and — when the move did not settle — the reason recorded on its
/// `MoveRecord`. A `Move` is synchronous (spec §7): `perform` drives both legs to `Done` (or
/// `Failed`), so unlike [`DirectInflowOutcome`] there is no invoice to surface and no external
/// payer to await. A `Pending` status means a transient fault left the move re-drivable via
/// `reconcile` (or a re-run of `move` with the same occurrence + `--gateway`).
#[derive(Clone, Debug)]
pub struct MoveOutcome {
    pub key: IdempotencyKey,
    pub status: Option<IntentStatus>,
    pub outcome: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RawPayOutcome {
    pub key: IdempotencyKey,
    pub operation_id: OperationId,
    pub status: IntentStatus,
    pub already_in_flight: bool,
}

#[derive(Clone, Debug)]
pub struct RawReceiveOutcome {
    pub key: IdempotencyKey,
    pub operation_id: OperationId,
    pub invoice: Invoice,
    pub status: IntentStatus,
}

/// The two callers have deliberately different dry-run authority contracts.  Keep this private:
/// frontend selection belongs at the Runtime boundary, not in a caller-controlled policy field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatusMode {
    /// The exclusive direct-DB CLI diagnoses a stale replacement, but must not advertise its child.
    StandaloneDiagnostic,
    /// The daemon scheduler owns occurrence allocation, so a stale replacement is an authority error.
    DaemonStrict,
}

fn stale_standalone_replacement_status_warning(error: &str) -> String {
    format!(
        "{error}; returning scored/designation diagnostics with no would-run decisions; \
         retry standalone tick/status with a strictly newer --occurrence"
    )
}

#[derive(Clone, Debug)]
pub struct JoinIntentOutcome {
    pub key: IdempotencyKey,
    pub status: IntentStatus,
}

/// The terminal result of [`Runtime::await_move`]: the inflow settled (`Done`) or did not
/// (`Failed`, carrying the reason). `await_move` blocks on the receive leg, so it never
/// returns while the intent is still merely `Awaiting`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalizeOutcome {
    Done,
    Failed(String),
}

/// Counts + keys from a [`Runtime::reconcile`] pass (spec §9). `performed`/`failed`/`skipped`
/// come from the `wallet_core::reconcile` re-drive of pending intents; `awaiting` is the set of
/// `DirectInflow` intents whose external payer has not settled — reported (not re-driven) so the
/// operator can `await-move` each. `retryable` is the §15.11 subset of `failed` that was left
/// `Pending` for a later retry (a transient timeout/transport fault), so a scheduler driving
/// `reconcile` in a loop can tell "will clear on a later pass" from a terminal `failed − retryable`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileSummary {
    pub performed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub retryable: usize,
    pub awaiting: usize,
    pub awaiting_keys: Vec<IdempotencyKey>,
    /// The logical allocator goals still owned by durable work when this pass finished (br-p93),
    /// projected from the FINAL `Pending`/`Executing` scan rather than from the counts above: a
    /// retryable failure and a live driver leave the same durable evidence, and only the durable
    /// state can say which goals a following tick must withhold.
    ///
    /// This value REPORTS the standalone path's eligibility; it does not carry it. `watch_once`
    /// reads it to log what the tick will suppress, and each seam that ACTS on the suppression
    /// re-derives the same projection from the same durable source with
    /// [`GoalBlockers::from_intents`]: `plan_tick` before route pricing, `tick` again before
    /// apply. Those later scans are strictly fresher and equally fail-closed — work admitted since
    /// this pass appears in them, and work that SETTLED since is a legitimate recurrence — so
    /// re-deriving never narrows the suppression below what is still in flight. The daemon,
    /// whose reconcile and tick are separate actor round-trips, does carry its equivalent
    /// ([`crate::service::ReconcileReport::blocked`]) onto the cycle's `TickPolicy`.
    pub blocked: GoalBlockers,
}

#[derive(Clone, Debug)]
pub struct WatchCycleReport {
    pub occurrence: Occurrence,
    pub reconcile: WatchReconcileOutcome,
    pub tick: WatchTickOutcome,
    pub probes: Vec<WatchProbeReport>,
    pub discover: WatchDiscoverOutcome,
    pub budget_usage: ProbeBudgetUsage,
    pub watch_state: WatchState,
    pub deadlines: AdaptiveSleepDeadlines,
}

impl WatchCycleReport {
    pub fn subscription_noop(&self) -> bool {
        let reconcile_noop = match &self.reconcile {
            WatchReconcileOutcome::Ran(summary) => {
                summary.performed == 0
                    && summary.failed == 0
                    && summary.skipped == 0
                    && summary.retryable == 0
                    && summary.awaiting == 0
            }
            WatchReconcileOutcome::Failed(_) => false,
        };
        let tick_noop = match &self.tick {
            WatchTickOutcome::Ran(report) => {
                report.decisions.is_empty()
                    && report.summary.performed == 0
                    && report.summary.failed == 0
                    && report.summary.terminal_failed_skipped == 0
                    && report.summary.retryable == 0
            }
            WatchTickOutcome::SkippedReconcileFailed | WatchTickOutcome::Failed(_) => false,
        };
        let probes_noop = self.probes.iter().all(|probe| {
            matches!(
                &probe.outcome,
                WatchProbeOutcome::NotDue
                    | WatchProbeOutcome::Passed
                    | WatchProbeOutcome::NoSource
                    | WatchProbeOutcome::BudgetBlocked
                    | WatchProbeOutcome::DeferredByInFlight
            )
        });
        let discover_noop = matches!(
            &self.discover,
            WatchDiscoverOutcome::Disabled | WatchDiscoverOutcome::NotDue { .. }
        );
        reconcile_noop && tick_noop && probes_noop && discover_noop
    }
}

#[derive(Clone, Debug)]
pub enum WatchReconcileOutcome {
    Ran(ReconcileSummary),
    Failed(String),
}

#[derive(Clone, Debug)]
pub enum WatchTickOutcome {
    Ran(TickReport),
    /// Reconcile itself faulted, so which goals are in flight is UNKNOWN. This is the only
    /// remaining global skip (br-p93): a successful reconcile always ticks, projected through the
    /// conflict-scoped blocker set it derived.
    SkippedReconcileFailed,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchProbeReport {
    pub fed: FederationId,
    pub verdict: ActiveProbeVerdict,
    pub due_ms: u64,
    pub outcome: WatchProbeOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchProbeOutcome {
    Passed,
    NotDue,
    NoSource,
    BudgetBlocked,
    DeferredByInFlight,
    Attempted,
    Failed(String),
}

#[derive(Clone, Debug)]
pub enum WatchDiscoverOutcome {
    Disabled,
    NotDue { next_due_ms: u64 },
    Ran(DiscoverReport),
    Failed(String),
}

#[derive(Clone, Debug)]
struct ProbeScheduleContext {
    last_invocations: BTreeMap<(FederationId, FederationId), u64>,
    budget_usage: ProbeBudgetUsage,
    budget_ok: bool,
    budget_reset_ms: Option<u64>,
    fresh_probe_defer_until_ms: Option<u64>,
}

struct ProbeScheduleInput {
    candidate: FederationId,
    source: Option<FederationId>,
    verdict: ActiveProbeVerdict,
    due_ms: u64,
    /// The exact durable identity observed while building this schedule input. Keeping the
    /// session here prevents a retained item from silently becoming fresh if the journal changes
    /// before the service actor sees it.
    session: Option<ProbeSession>,
    post_in_resume: bool,
}

impl ProbeScheduleContext {
    fn new(
        budget_usage: ProbeBudgetUsage,
        budget_reset_ms: Option<u64>,
        policy: &WatchPolicy,
    ) -> Self {
        let budget_ok = probe_budget_ok(
            budget_usage.attempts,
            budget_usage.spend_msat,
            &policy.probe_budget,
        );
        Self {
            last_invocations: BTreeMap::new(),
            budget_usage,
            budget_ok,
            budget_reset_ms,
            fresh_probe_defer_until_ms: None,
        }
    }

    fn record_invocation(
        &mut self,
        candidate: FederationId,
        spending: FederationId,
        invoked_at_ms: u64,
    ) {
        self.last_invocations
            .entry((candidate, spending))
            .and_modify(|last| *last = (*last).max(invoked_at_ms))
            .or_insert(invoked_at_ms);
    }

    fn record_budget_attempt(&mut self, cost_msat: u64, created_at_ms: u64, policy: &WatchPolicy) {
        self.budget_usage.attempts = self.budget_usage.attempts.saturating_add(1);
        self.budget_usage.spend_msat = self.budget_usage.spend_msat.saturating_add(cost_msat);
        let reset_ms = created_at_ms.saturating_add(PROBE_BUDGET_WINDOW_MS);
        self.budget_reset_ms = Some(
            self.budget_reset_ms
                .map_or(reset_ms, |old| old.min(reset_ms)),
        );
        self.budget_ok = probe_budget_ok(
            self.budget_usage.attempts,
            self.budget_usage.spend_msat,
            &policy.probe_budget,
        );
    }

    fn defer_fresh_probes_until(&mut self, ready_ms: u64) {
        self.fresh_probe_defer_until_ms = Some(
            self.fresh_probe_defer_until_ms
                .map_or(ready_ms, |existing| existing.max(ready_ms)),
        );
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TickPlan {
    pub(crate) raw_probes: Vec<(FederationId, ProbeResult)>,
    pub(crate) probes: Vec<(FederationId, ProbeResult)>,
    pub(crate) active_probes: BTreeMap<FederationId, ActiveProbeVerdict>,
    pub(crate) snapshot: AllocatorSnapshot,
    /// The decisions this tick may act on (conflict-suppressed work already removed).
    pub(crate) decisions: Vec<AllocatorDecision>,
    /// Work conflict projection withheld before route preflight. Only the pinned-input check reads
    /// it, separately from `decisions`, because it is not executable endpoint evidence.
    pub(crate) suppressed: Vec<AllocatorDecision>,
    /// Ordinary work deferred solely by replacement one-child exclusivity. Audit-only: it never
    /// becomes an apply candidate or a conflict-suppression voucher.
    pub(crate) replacement_deferred: Vec<AllocatorDecision>,
    /// Funding goals withheld by the move floor (br-0vg). Diagnostic only.
    pub(crate) deferred: Vec<wallet_core::DeferredFunding>,
    /// Durable rebalance endpoints observed while planning. `status` reports against this planning
    /// view; `tick` re-scans and uses a fresh value after its final conflict retention.
    pub(crate) blockers: GoalBlockers,
    /// A marker-bearing evacuation may be atomically exchanged for this fresh child.  It is kept
    /// outside `decisions` because the parent remains the durable reservation until the exchange;
    /// callers must not apply the child before that atomic hand-off.
    pub(crate) replacement: Option<crate::service::EvacuationReplacementPlan>,
    /// Exact marked parent to clear after a qualifying shadow yielded no child.  It is committed
    /// without starting a driver, leaving ordinary retry to the next normal tick.
    pub(crate) marker_disposition: Option<crate::service::EvacuationMarkerDisposition>,
}

/// Why a detached service awaiter stopped before terminalizing its durable intent.
///
/// A retryable subscription or persistence fault retains await ownership. A structurally invalid
/// durable intent must instead be terminalized through the actor, or it would remain Awaiting with
/// an endless succession of local awaiters.
#[derive(Debug)]
pub(crate) enum AwaitFailure {
    Retryable(String),
    Permanent(String),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum TestAwaitOutcome {
    Retryable,
    Permanent,
    Done,
}

/// Test-only terminal SDK states used to exercise the real raw Pay/Receive await continuation.
///
/// This is intentionally separate from [`TestAwaitOutcome`]: the latter stops before an SDK
/// observation, while these values enter the post-observation correlation/finalization path.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum TestTerminalAwaitState {
    SendSucceeded,
    ReceiveClaimed,
}

/// A local error injected only after the test terminal SDK observation above.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TestPostObservationFault {
    PreparePermanent,
    FinalizeStatusMismatch,
}

impl AwaitFailure {
    fn retryable_error(error: anyhow::Error) -> Self {
        Self::Retryable(format!("{error:#}"))
    }

    fn retryable_exec(error: ExecError) -> Self {
        Self::Retryable(format!("{error:?}"))
    }

    fn retryable_service_error(error: crate::service::ServiceError) -> Self {
        Self::Retryable(error.to_string())
    }

    /// An SDK typed-await validation fault happens before an operation terminal state was
    /// observed. Its explicit classification may therefore safely tell the actor whether the
    /// durable attempt is structurally invalid or should retain await ownership.
    fn from_await_operation(error: crate::multi_client::AwaitOperationError) -> Self {
        Self::from_exec(error.into_exec_error())
    }

    /// Once `await_send`/`await_receive` returned a terminal SDK state, an error in our
    /// correlation, preparation, or journal-finalization work is local uncertainty, not evidence
    /// that the operation failed. Retain this attempt's await ownership even when that local API
    /// reports `Permanent`/`Unsupported`: the externally observed operation may have succeeded.
    fn post_terminal_observation_exec(error: ExecError) -> Self {
        Self::Retryable(format!("post-terminal-observation local fault: {error:?}"))
    }

    fn permanent(message: String) -> Self {
        Self::Permanent(message)
    }

    fn from_exec(error: ExecError) -> Self {
        match error {
            ExecError::Retryable(message) => Self::Retryable(message),
            ExecError::StructuralEvacuationRefusal(evidence) => {
                Self::Retryable(evidence.diagnostic)
            }
            ExecError::Permanent(message) => Self::Permanent(message),
            ExecError::Unsupported => Self::Permanent("await operation is unsupported".to_owned()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MoveRouteProblem {
    pub(crate) from: FederationId,
    pub(crate) to: FederationId,
    /// The federation whose gateway is marked unavailable in the planning probe copy so the
    /// tick planner re-runs allocation onto a different route. This is ALWAYS the selected
    /// destination `to`: a destination that cannot receive is skipped directly, and a source
    /// leg that the destination-selected gateway cannot serve is retried against another
    /// eligible destination (an evacuation additionally captures a fallback plan first). There
    /// is no route problem that leaves the destination usable, so this is never absent.
    pub(crate) mark_unavailable: FederationId,
    pub(crate) gateway: Option<GatewayUrl>,
    pub(crate) error: String,
    pub(crate) evacuation_source_route: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SendRouteKind {
    Move,
    Evacuate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TerminalReplay {
    key: IdempotencyKey,
    status: IntentStatus,
}

/// Wraps an [`Executor`] so money-operation `perform` calls are bounded by a wall-clock deadline
/// (§15.9). A tick blocks on `await_send`/`await_receive` (the SDK long-polls up to 60 min/request),
/// so one stalled gateway would otherwise freeze probing and every other decision. On timeout the
/// perform future is DROPPED — the move engine is crash-safe (a later reconcile rebuilds the
/// record from the op-log and reattaches, never re-minting/re-paying) — and the intent is left
/// `Pending` via the `Retryable` path, so the tick moves on and the summary counts it. Joins retain
/// their pre-intent unbounded behavior because dropping the SDK join future can interrupt its
/// best-effort partition cleanup; discovery's separate join deadline remains cancellation-aware.
struct TimeoutExecutor<E> {
    inner: E,
    timeout: Option<Duration>,
}

impl<E> TimeoutExecutor<E> {
    fn new(inner: E, timeout: Option<Duration>) -> Self {
        Self { inner, timeout }
    }
}

#[async_trait]
impl<E: Executor> Executor for TimeoutExecutor<E> {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        // Join (slow DKG config download) and Recover (long epoch-history replay) are legitimately
        // long-running; the per-perform deadline never applies to them, or a healthy recovery
        // would be killed mid-replay.
        if matches!(intent.action, Action::Join { .. } | Action::Recover { .. }) {
            return self.inner.perform(intent).await;
        }
        match self.timeout {
            Some(deadline) => match runtime::timeout(deadline, self.inner.perform(intent)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(ExecError::Retryable(format!(
                    "perform exceeded the {}s deadline for intent {}; leaving it Pending for the \
                     next reconcile",
                    deadline.as_secs(),
                    intent.idempotency_key.0
                ))),
            },
            None => self.inner.perform(intent).await,
        }
    }
}

/// The engine façade over one wallet's shared fedimint clients + journal (spec §9).
pub struct Runtime {
    mc: Arc<MultiClient>,
    journal: Arc<FedimintJournal>,
    pinned_gateway: Option<GatewayUrl>,
    /// The hard per-fed balance cap enforced at perform time (§15.2), threaded into the executor.
    /// `None` disables it (the operator's `--allow-over-cap`). For a tick this is the policy's
    /// `per_fed_cap`; for an operator verb it is the ADR-0018 default unless overridden.
    hard_cap: Option<Msat>,
    /// Per-`perform` wall-clock deadline (§15.9). `None` disables the deadline.
    perform_timeout: Option<Duration>,
    /// Unit-test seam for exercising the standalone watch/tick commit path without live guardians.
    #[cfg(test)]
    test_executor: Option<Arc<wallet_core::MockExecutor>>,
    /// A preplanned round paired with `test_executor`; production always calls `plan_tick` normally.
    #[cfg(test)]
    test_tick_plan: std::sync::Mutex<Option<TickPlan>>,
    /// Select the supplied mock executor for a production-scheduler test fixture.  This is
    /// separate from the plan itself so a test can install the exact parent/child plan after it
    /// has seeded the durable marker but before it starts the scheduler cycle.
    #[cfg(test)]
    test_scheduler_fixture_enabled: std::sync::atomic::AtomicBool,
    /// Deterministic clock values for the standalone replacement's one atomic timestamp.  Kept
    /// narrower than `now_ms()` because only this exact identity boundary needs the seam.
    #[cfg(test)]
    test_replacement_exchange_times: std::sync::Mutex<std::collections::VecDeque<u64>>,
    /// Test-only sampled probe view for exercising the production scheduler's post-plan paths.
    /// It is separate from `test_tick_plan`: the scheduler samples both before planning and again
    /// when a stale designation must be re-derived.
    #[cfg(test)]
    test_probe_all: std::sync::Mutex<Option<Vec<(FederationId, ProbeResult)>>>,
    /// Narrow planner seam: raw-probe unit tests have no live gateway clients,
    /// so they can isolate allocation/replacement propagation from concrete
    /// route I/O without prebuilding a `TickPlan`.
    #[cfg(test)]
    test_skip_route_preflight: std::sync::atomic::AtomicBool,
    /// Deterministic detached-awaiter outcomes for service-driver classification tests.
    #[cfg(test)]
    test_await_outcomes: std::sync::Mutex<std::collections::VecDeque<TestAwaitOutcome>>,
    /// Typed pre-observation await failures for service classification tests.
    #[cfg(test)]
    test_await_operation_errors:
        std::sync::Mutex<std::collections::VecDeque<crate::multi_client::AwaitOperationError>>,
    /// Terminal states injected at the precise SDK-observation boundary for raw awaiter tests.
    #[cfg(test)]
    test_terminal_await_states:
        std::sync::Mutex<std::collections::VecDeque<TestTerminalAwaitState>>,
    /// A local post-observation fault; its error class must never terminalize the actor attempt.
    #[cfg(test)]
    test_post_observation_faults:
        std::sync::Mutex<std::collections::VecDeque<TestPostObservationFault>>,
    /// Test-only hold at the awaiter handoff, used to inspect retained ownership before a
    /// successor runs.
    #[cfg(test)]
    test_awaiter_retry_hold: std::sync::Mutex<Option<Arc<tokio::sync::Notify>>>,
    /// One-shot designation-read fault seam for the production scheduler's degraded-cycle test.
    #[cfg(test)]
    test_scheduler_designation_failures: std::sync::atomic::AtomicUsize,
    /// Hold one service probe after actor admission but before its exact durable-session read.
    #[cfg(test)]
    test_service_probe_start_hold: std::sync::Mutex<Option<Arc<tokio::sync::Notify>>>,
}

impl Runtime {
    pub fn new(
        mc: Arc<MultiClient>,
        journal: Arc<FedimintJournal>,
        pinned_gateway: Option<GatewayUrl>,
        hard_cap: Option<Msat>,
        perform_timeout: Option<Duration>,
    ) -> Self {
        Self {
            mc,
            journal,
            pinned_gateway,
            hard_cap,
            perform_timeout,
            #[cfg(test)]
            test_executor: None,
            #[cfg(test)]
            test_tick_plan: std::sync::Mutex::new(None),
            #[cfg(test)]
            test_scheduler_fixture_enabled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            test_replacement_exchange_times: std::sync::Mutex::new(
                std::collections::VecDeque::new(),
            ),
            #[cfg(test)]
            test_probe_all: std::sync::Mutex::new(None),
            #[cfg(test)]
            test_skip_route_preflight: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            test_await_outcomes: std::sync::Mutex::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            test_await_operation_errors: std::sync::Mutex::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            test_terminal_await_states: std::sync::Mutex::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            test_post_observation_faults: std::sync::Mutex::new(std::collections::VecDeque::new()),
            #[cfg(test)]
            test_awaiter_retry_hold: std::sync::Mutex::new(None),
            #[cfg(test)]
            test_scheduler_designation_failures: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            test_service_probe_start_hold: std::sync::Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_tick_test_fixture(
        &mut self,
        executor: Arc<wallet_core::MockExecutor>,
        plan: TickPlan,
    ) {
        self.test_executor = Some(executor);
        *self
            .test_tick_plan
            .lock()
            .expect("tick test-plan mutex poisoned") = Some(plan);
    }

    #[cfg(test)]
    fn set_tick_test_executor(&mut self, executor: Arc<wallet_core::MockExecutor>) {
        self.test_executor = Some(executor);
    }

    #[cfg(test)]
    pub(crate) fn set_replacement_exchange_times_for_test(
        &self,
        times: impl IntoIterator<Item = u64>,
    ) {
        *self
            .test_replacement_exchange_times
            .lock()
            .expect("replacement exchange clock mutex poisoned") = times.into_iter().collect();
    }

    fn replacement_exchange_now(&self) -> u64 {
        #[cfg(test)]
        if let Some(now) = self
            .test_replacement_exchange_times
            .lock()
            .expect("replacement exchange clock mutex poisoned")
            .pop_front()
        {
            return now;
        }
        now_ms()
    }

    #[cfg(test)]
    pub(crate) fn set_scheduler_probe_fixture(&self, probes: Vec<(FederationId, ProbeResult)>) {
        *self
            .test_probe_all
            .lock()
            .expect("scheduler probe fixture mutex poisoned") = Some(probes);
    }

    /// Install the actor-side scheduler planning fixture after the runtime is shared with the
    /// service. Unlike [`Self::set_tick_test_fixture`], scheduler planning does not use the
    /// standalone executor seam, so this only needs interior access to the prebuilt plan.
    #[cfg(test)]
    pub(crate) fn set_scheduler_tick_test_fixture(&self, plan: TickPlan) {
        *self
            .test_tick_plan
            .lock()
            .expect("tick test-plan mutex poisoned") = Some(plan);
    }

    #[cfg(test)]
    pub(crate) fn enable_scheduler_tick_fixture_for_test(&self) {
        self.test_scheduler_fixture_enabled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn scheduler_tick_fixture_enabled_for_test(&self) -> bool {
        self.test_scheduler_fixture_enabled
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn skip_route_preflight_for_test(&self) {
        self.test_skip_route_preflight
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn set_awaiter_test_outcomes(
        &self,
        outcomes: impl IntoIterator<Item = TestAwaitOutcome>,
    ) {
        *self
            .test_await_outcomes
            .lock()
            .expect("awaiter test-outcomes mutex poisoned") = outcomes.into_iter().collect();
    }

    #[cfg(test)]
    pub(crate) fn set_awaiter_test_operation_errors(
        &self,
        errors: impl IntoIterator<Item = crate::multi_client::AwaitOperationError>,
    ) {
        *self
            .test_await_operation_errors
            .lock()
            .expect("awaiter operation-error test mutex poisoned") = errors.into_iter().collect();
    }

    #[cfg(test)]
    pub(crate) fn set_post_observation_awaiter_test_fixture(
        &self,
        terminal_states: impl IntoIterator<Item = TestTerminalAwaitState>,
        faults: impl IntoIterator<Item = TestPostObservationFault>,
    ) {
        *self
            .test_terminal_await_states
            .lock()
            .expect("terminal await-state test mutex poisoned") =
            terminal_states.into_iter().collect();
        *self
            .test_post_observation_faults
            .lock()
            .expect("post-observation await-fault test mutex poisoned") =
            faults.into_iter().collect();
    }

    #[cfg(test)]
    pub(crate) fn hold_next_awaiter_retry_for_test(&self) -> Arc<tokio::sync::Notify> {
        let hold = Arc::new(tokio::sync::Notify::new());
        *self
            .test_awaiter_retry_hold
            .lock()
            .expect("awaiter retry-hold test mutex poisoned") = Some(hold.clone());
        hold
    }

    /// Keep awaiter retry pacing outside the actor. Tests can hold this exact handoff to inspect
    /// the retained Awaiting attempt before releasing the successor.
    pub(crate) async fn service_awaiter_retry_delay(&self) {
        #[cfg(test)]
        let test_hold = {
            self.test_awaiter_retry_hold
                .lock()
                .expect("awaiter retry-hold test mutex poisoned")
                .clone()
        };
        #[cfg(test)]
        if let Some(hold) = test_hold {
            hold.notified().await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_scheduler_designations_for_test(&self, count: usize) {
        self.test_scheduler_designation_failures
            .store(count, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn hold_next_service_probe_start_for_test(&self) -> Arc<tokio::sync::Notify> {
        let hold = Arc::new(tokio::sync::Notify::new());
        *self
            .test_service_probe_start_hold
            .lock()
            .expect("service-probe start-hold mutex poisoned") = Some(hold.clone());
        hold
    }

    #[cfg(test)]
    pub(crate) fn scheduler_tick_test_plan(&self) -> Option<TickPlan> {
        self.test_tick_plan
            .lock()
            .expect("tick test-plan mutex poisoned")
            .clone()
    }

    /// Service-layer journal handle used for actor decisions and lifecycle transitions.
    pub(crate) fn service_journal(&self) -> Arc<FedimintJournal> {
        self.journal.clone()
    }

    pub(crate) fn service_multi_client(&self) -> Arc<MultiClient> {
        self.mc.clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn service_due_probes(
        &self,
        spending: Option<FederationId>,
        tick_policy: &TickPolicy,
        watch_policy: &WatchPolicy,
        sampled_balances: &BTreeMap<FederationId, Msat>,
        fresh_policy: Option<&ProbePolicySnapshot>,
        now_ms: u64,
        occurrence: Occurrence,
    ) -> anyhow::Result<(Vec<ProbeCandidate>, bool)> {
        let context = self.probe_schedule_context(now_ms, watch_policy).await?;
        let inputs = self
            .probe_schedule_inputs(
                spending,
                &tick_policy.probe_gate_policy,
                watch_policy,
                now_ms,
                &context.last_invocations,
            )
            .await?;
        let mut resumed = Vec::new();
        let mut fresh = Vec::new();
        for input in inputs {
            let ProbeScheduleInput {
                candidate,
                source,
                verdict: _,
                due_ms,
                session,
                post_in_resume: _,
            } = input;
            let Some(source) = source else {
                continue;
            };
            let retained = session.is_some();
            if !retained && due_ms > now_ms {
                continue;
            }
            if !retained && !context.budget_ok {
                self.record_watch_probe_skip(
                    candidate,
                    source,
                    tick_policy.probe_gate_policy.amount_msat,
                    occurrence,
                    budget_skip_diagnostic_bucket_ms(now_ms, context.budget_reset_ms),
                    "watch probe skipped: weekly probe budget exhausted",
                )
                .await
                .map_err(exec_err)?;
                continue;
            }
            if let Some(session) = session {
                resumed.push(ProbeCandidate {
                    federation: candidate,
                    source,
                    baseline: Msat(session.c_spendable_before_in_msat),
                    actor: Actor::Agent { occurrence },
                    now_ms,
                    admission: ProbeAdmission::ResumeOnly {
                        expected_nonce: session.nonce,
                    },
                });
            } else if let Some(baseline) = fresh_probe_baseline(
                self.mc.has_client(&candidate),
                sampled_balances.get(&candidate).copied(),
            ) {
                if let Some(snapshot) = fresh_policy {
                    fresh.push(ProbeCandidate {
                        federation: candidate,
                        source,
                        baseline,
                        actor: Actor::Agent { occurrence },
                        now_ms,
                        admission: ProbeAdmission::Fresh(snapshot.clone()),
                    });
                }
            } else {
                tracing::warn!(
                    federation = %candidate.to_hex(),
                    "watch scheduler: skipping fresh probe because its open candidate baseline was not sampled"
                );
            }
        }
        // The synchronous 5.2 loop resumes retained probe money before starting anything
        // fresh, and defers fresh probes when that resume remains in flight. Service probes
        // return as soon as their driver is spawned, so a resumed session is necessarily still
        // live for this cycle; admitting only the resume group is the async equivalent.
        let resuming = !resumed.is_empty();
        Ok((if resuming { resumed } else { fresh }, resuming))
    }

    /// Fresh step-2 executor for a detached service driver.
    pub(crate) fn service_executor(&self, hard_cap: Option<Msat>) -> FedimintExecutor {
        FedimintExecutor::new(
            self.mc.clone(),
            self.journal.clone(),
            self.pinned_gateway.clone(),
            hard_cap,
        )
    }

    /// Build the production executor for an actor driver. Artifact and phase writes use one-shot
    /// actor commands; membership publication uses the same client for its short final fence.
    pub(crate) fn service_executor_with_client(
        &self,
        hard_cap: Option<Msat>,
        client: WalletClient,
    ) -> FedimintExecutor {
        self.service_executor(hard_cap).with_service_client(client)
    }

    pub(crate) fn service_perform_timeout(&self) -> Option<Duration> {
        self.perform_timeout
    }

    /// Reattach the subscription-owned side of a service intent. The issued operation
    /// artifact is authoritative; this path never mints or pays again.
    pub(crate) async fn service_await_intent(
        &self,
        intent: &Intent,
        client: &WalletClient,
    ) -> Result<(), AwaitFailure> {
        #[cfg(test)]
        let test_outcome = {
            // A queued terminal SDK state exercises the production continuation below. Do not
            // let the coarse whole-awaiter seam consume its successor outcome first.
            let terminal_state_pending = !self
                .test_terminal_await_states
                .lock()
                .expect("terminal await-state test mutex poisoned")
                .is_empty();
            (!terminal_state_pending)
                .then(|| {
                    self.test_await_outcomes
                        .lock()
                        .expect("awaiter test-outcomes mutex poisoned")
                        .pop_front()
                })
                .flatten()
        };
        #[cfg(test)]
        if let Some(outcome) = test_outcome {
            return match outcome {
                TestAwaitOutcome::Retryable => Err(AwaitFailure::Retryable(
                    "injected retryable await failure".to_owned(),
                )),
                TestAwaitOutcome::Permanent => Err(AwaitFailure::Permanent(
                    "injected permanent await failure".to_owned(),
                )),
                TestAwaitOutcome::Done => client
                    .journal_transition(
                        intent.idempotency_key.clone(),
                        crate::service::JournalTransition::SetStatus {
                            expected_attempt: intent.attempt,
                            status: IntentStatus::Done,
                            error: None,
                        },
                    )
                    .await
                    .map(|_| ())
                    .map_err(AwaitFailure::retryable_service_error),
            };
        }
        let key = &intent.idempotency_key;
        match &intent.action {
            Action::DirectInflow { .. } => {
                self.service_await_direct_inflow(key, client).await?;
            }
            Action::Pay { from, .. } => {
                let operation_id = intent.operation_id.ok_or_else(|| {
                    AwaitFailure::permanent(format!(
                        "awaiting raw pay {} has no send operation id",
                        key.0
                    ))
                })?;
                let (status, error) = match self.service_await_send(from, operation_id).await? {
                    SendState::Success(_) => (OperationStatus::Succeeded, None),
                    SendState::Refunded => {
                        (OperationStatus::Failed, Some("send refunded".to_owned()))
                    }
                    SendState::Failed(error) => (OperationStatus::Failed, Some(error)),
                };
                // Correlation proof and settlement observation may use the SDK.  Complete them
                // before acquiring the actor lease, which protects only the final journal write.
                let prepared = self
                    .journal
                    .prepare_raw_operation_terminal(
                        self.mc.as_ref(),
                        *from,
                        operation_id,
                        key,
                        intent.attempt,
                        RawOperationRole::Send,
                    )
                    .await;
                #[cfg(test)]
                let prepared = self.inject_post_observation_prepare_fault_for_test(prepared);
                #[cfg(test)]
                let prepared =
                    self.prepare_for_post_observation_finalize_fault_test(prepared, intent.attempt);
                let prepared = prepared.map_err(AwaitFailure::post_terminal_observation_exec)?;
                let lease = client
                    .begin_external_terminal_mutation(intent.action.clone())
                    .await
                    .map_err(AwaitFailure::retryable_service_error)?;
                let result = self
                    .journal
                    .finalize_raw_operation(key, status, error.as_deref(), prepared)
                    .await;
                #[cfg(test)]
                let result = self.inject_post_observation_finalize_fault_for_test(result);
                let result = result.map_err(AwaitFailure::post_terminal_observation_exec);
                let end = client
                    .end_external_terminal_mutation(lease)
                    .await
                    .map_err(AwaitFailure::retryable_service_error);
                result?;
                end?;
            }
            Action::Receive { to, .. } => {
                let operation_id = intent.operation_id.ok_or_else(|| {
                    AwaitFailure::permanent(format!(
                        "awaiting raw receive {} has no receive operation id",
                        key.0
                    ))
                })?;
                let (status, error) = match self.service_await_receive(to, operation_id).await? {
                    ReceiveState::Claimed => (OperationStatus::Succeeded, None),
                    ReceiveState::Expired => {
                        (OperationStatus::Failed, Some("receive expired".to_owned()))
                    }
                    ReceiveState::Failed(error) => (OperationStatus::Failed, Some(error)),
                };
                let prepared = self
                    .journal
                    .prepare_raw_operation_terminal(
                        self.mc.as_ref(),
                        *to,
                        operation_id,
                        key,
                        intent.attempt,
                        RawOperationRole::Receive,
                    )
                    .await;
                #[cfg(test)]
                let prepared = self.inject_post_observation_prepare_fault_for_test(prepared);
                #[cfg(test)]
                let prepared =
                    self.prepare_for_post_observation_finalize_fault_test(prepared, intent.attempt);
                let prepared = prepared.map_err(AwaitFailure::post_terminal_observation_exec)?;
                let lease = client
                    .begin_external_terminal_mutation(intent.action.clone())
                    .await
                    .map_err(AwaitFailure::retryable_service_error)?;
                let result = self
                    .journal
                    .finalize_raw_operation(key, status, error.as_deref(), prepared)
                    .await;
                #[cfg(test)]
                let result = self.inject_post_observation_finalize_fault_for_test(result);
                let result = result.map_err(AwaitFailure::post_terminal_observation_exec);
                let end = client
                    .end_external_terminal_mutation(lease)
                    .await
                    .map_err(AwaitFailure::retryable_service_error);
                result?;
                end?;
            }
            _ => {
                return Err(AwaitFailure::permanent(format!(
                    "intent {} has no subscription-owned await path",
                    key.0
                )));
            }
        }
        Ok(())
    }

    /// Await a raw send operation for a service awaiter. Test terminal states enter the same
    /// continuation as the production SDK result, rather than using the coarse whole-awaiter
    /// outcome seam.
    async fn service_await_send(
        &self,
        from: &FederationId,
        operation_id: OperationId,
    ) -> Result<SendState, AwaitFailure> {
        #[cfg(test)]
        if let Some(error) = self
            .test_await_operation_errors
            .lock()
            .expect("awaiter operation-error test mutex poisoned")
            .pop_front()
        {
            return Err(AwaitFailure::from_await_operation(error));
        }
        #[cfg(test)]
        if let Some(state) = self
            .test_terminal_await_states
            .lock()
            .expect("terminal await-state test mutex poisoned")
            .pop_front()
        {
            return match state {
                TestTerminalAwaitState::SendSucceeded => {
                    Ok(SendState::Success(wallet_core::Preimage([0; 32])))
                }
                TestTerminalAwaitState::ReceiveClaimed => {
                    panic!("receive terminal test state used for raw Pay awaiter")
                }
            };
        }
        self.mc
            .await_send(from, operation_id)
            .await
            .map_err(AwaitFailure::from_await_operation)
    }

    /// Await a raw receive operation for a service awaiter. See [`Self::service_await_send`] for
    /// why the test seam is at the terminal-state boundary.
    async fn service_await_receive(
        &self,
        to: &FederationId,
        operation_id: OperationId,
    ) -> Result<ReceiveState, AwaitFailure> {
        #[cfg(test)]
        if let Some(error) = self
            .test_await_operation_errors
            .lock()
            .expect("awaiter operation-error test mutex poisoned")
            .pop_front()
        {
            return Err(AwaitFailure::from_await_operation(error));
        }
        #[cfg(test)]
        if let Some(state) = self
            .test_terminal_await_states
            .lock()
            .expect("terminal await-state test mutex poisoned")
            .pop_front()
        {
            return match state {
                TestTerminalAwaitState::SendSucceeded => {
                    panic!("send terminal test state used for raw Receive awaiter")
                }
                TestTerminalAwaitState::ReceiveClaimed => Ok(ReceiveState::Claimed),
            };
        }
        self.mc
            .await_receive(to, operation_id)
            .await
            .map_err(AwaitFailure::from_await_operation)
    }

    /// Inject the preparation failure only after the real preparation result was obtained, so the
    /// test exercises the production post-observation classification at that call site.
    #[cfg(test)]
    fn inject_post_observation_prepare_fault_for_test<T>(
        &self,
        prepared: Result<T, ExecError>,
    ) -> Result<T, ExecError> {
        if self.take_post_observation_fault_for_test(TestPostObservationFault::PreparePermanent) {
            Err(ExecError::Permanent(
                "injected post-observation prepare fault".to_owned(),
            ))
        } else {
            prepared
        }
    }

    /// The in-memory runtime fixture intentionally has no SDK client, so its genuine preparation
    /// can fail while trying to read an op-log row. A finalizer-stage fault still needs to execute
    /// the real finalizer and lease-release path; use an unfenced preparation only for that narrow
    /// test fixture after the real preparation expression has run.
    #[cfg(test)]
    fn prepare_for_post_observation_finalize_fault_test(
        &self,
        prepared: Result<crate::journal::PreparedRawOperationTerminal, ExecError>,
        expected_attempt: u32,
    ) -> Result<crate::journal::PreparedRawOperationTerminal, ExecError> {
        if prepared.is_err()
            && self.has_post_observation_fault_for_test(
                TestPostObservationFault::FinalizeStatusMismatch,
            )
        {
            Ok(crate::journal::PreparedRawOperationTerminal::unfenced_for_test(expected_attempt))
        } else {
            prepared
        }
    }

    /// Inject a post-finalizer persistence fault after the actor lease has been acquired. The
    /// real finalizer still runs and the caller still ends that lease before classifying its
    /// result.
    #[cfg(test)]
    fn inject_post_observation_finalize_fault_for_test<T>(
        &self,
        finalized: Result<T, ExecError>,
    ) -> Result<T, ExecError> {
        if self
            .take_post_observation_fault_for_test(TestPostObservationFault::FinalizeStatusMismatch)
        {
            Err(ExecError::Permanent(
                "injected post-observation raw terminal status mismatch".to_owned(),
            ))
        } else {
            finalized
        }
    }

    #[cfg(test)]
    fn take_post_observation_fault_for_test(&self, expected: TestPostObservationFault) -> bool {
        let mut faults = self
            .test_post_observation_faults
            .lock()
            .expect("post-observation await-fault test mutex poisoned");
        if faults.front().is_some_and(|fault| *fault == expected) {
            faults.pop_front();
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    fn has_post_observation_fault_for_test(&self, expected: TestPostObservationFault) -> bool {
        self.test_post_observation_faults
            .lock()
            .expect("post-observation await-fault test mutex poisoned")
            .front()
            .is_some_and(|fault| *fault == expected)
    }

    /// Service-only DirectInflow awaiter.  All network waiting happens before
    /// acquiring the actor lease; only the terminal MoveRecord + intent writes are
    /// protected by it.
    async fn service_await_direct_inflow(
        &self,
        key: &IdempotencyKey,
        client: &WalletClient,
    ) -> Result<(), AwaitFailure> {
        let intent = self
            .journal
            .get(key)
            .await
            .map_err(AwaitFailure::retryable_exec)?
            .ok_or_else(|| AwaitFailure::permanent(format!("no intent found for key {}", key.0)))?;
        if matches!(intent.status, IntentStatus::Done | IntentStatus::Failed) {
            return Ok(());
        }
        if intent.status != IntentStatus::Awaiting {
            return Err(AwaitFailure::permanent(format!(
                "intent {} is not awaiting",
                key.0
            )));
        }
        // The MoveRecord is a derived cache (§9.2), so reconstruct it from the op-log before
        // treating a missing receive leg as durable corruption. Its pre-network cache write is
        // actor-routed; the later composite terminal writes remain under the existing external
        // lease and therefore must stay direct (never nest a second actor mutation gate).
        let record = self
            .service_executor_with_client(None, client.clone())
            .backfill_move_record(&intent)
            .await
            .map_err(AwaitFailure::from_exec)?
            .ok_or_else(|| {
                AwaitFailure::permanent(format!("intent {} is not an executable move", key.0))
            })?;
        let recv_op = record.recv_op.ok_or_else(|| {
            AwaitFailure::permanent(format!(
                "awaiting intent {} has no receive op to finalize",
                key.0
            ))
        })?;
        let state = self
            .mc
            .await_receive(&record.to, recv_op)
            .await
            .map_err(AwaitFailure::from_await_operation)?;
        let lease = client
            .begin_external_terminal_mutation(intent.action.clone())
            .await
            .map_err(AwaitFailure::retryable_service_error)?;
        let result: Result<(), AwaitFailure> = async {
            match state {
                ReceiveState::Claimed => {
                    self.settle_move(&record, intent.attempt, MovePhase::Settled, None)
                        .await
                        .map_err(AwaitFailure::retryable_error)?;
                    self.finalize(key, intent.attempt, IntentStatus::Done)
                        .await
                        .map_err(AwaitFailure::retryable_error)?;
                }
                ReceiveState::Expired => {
                    let message = "receive invoice expired before payment".to_owned();
                    self.settle_move(&record, intent.attempt, MovePhase::Failed, Some(message))
                        .await
                        .map_err(AwaitFailure::retryable_error)?;
                    self.finalize(key, intent.attempt, IntentStatus::Failed)
                        .await
                        .map_err(AwaitFailure::retryable_error)?;
                }
                ReceiveState::Failed(message) => {
                    self.settle_move(&record, intent.attempt, MovePhase::Failed, Some(message))
                        .await
                        .map_err(AwaitFailure::retryable_error)?;
                    self.finalize(key, intent.attempt, IntentStatus::Failed)
                        .await
                        .map_err(AwaitFailure::retryable_error)?;
                }
            }
            Ok(())
        }
        .await;
        let end = client
            .end_external_terminal_mutation(lease)
            .await
            .map_err(AwaitFailure::retryable_service_error);
        result?;
        end
    }

    /// A fresh executor sharing this runtime's clients + journal + pinned gateway + hard cap.
    /// Cheap (`Arc` clones); made per call so each standalone verb gets a `&self`-only executor.
    /// Standalone helper calls (`backfill_move_record` / `validate_direct_inflow_amount`) use it
    /// directly under the exclusive DB lock; service helpers instead use
    /// [`Self::service_executor_with_client`]. The `perform`-driving standalone paths wrap it via
    /// [`Self::driving_executor`] to apply the tick deadline.
    fn executor(&self) -> FedimintExecutor {
        FedimintExecutor::new(
            self.mc.clone(),
            self.journal.clone(),
            self.pinned_gateway.clone(),
            self.hard_cap,
        )
    }

    /// The executor `wallet_core::apply`/`reconcile` drive, wrapped with the §15.9 per-`perform`
    /// deadline so one stalled gateway can never freeze the whole tick.
    fn driving_executor(&self) -> TimeoutExecutor<FedimintExecutor> {
        TimeoutExecutor::new(self.executor(), self.perform_timeout)
    }

    async fn decide_and_drive(
        &self,
        decision: &AllocatorDecision,
        actor: Actor,
        balances: Option<&BTreeMap<FederationId, Msat>>,
        per_fed_cap: Option<Msat>,
    ) -> Result<(ExecutionSummary, Option<PerformOutcome>, Option<ExecError>), ExecError> {
        let mut summary = ExecutionSummary::default();
        let mut performed_outcome = None;
        let mut drive_error = None;
        match wallet_core::decide_and_journal(
            self.journal.as_ref(),
            decision,
            actor,
            now_ms(),
            balances,
            per_fed_cap,
        )
        .await?
        {
            wallet_core::DecideAndJournal::Drive(intent) => {
                let executor = self.driving_executor();
                match wallet_core::drive_intent_step(
                    self.journal.as_ref(),
                    &executor,
                    &intent,
                    &mut summary,
                )
                .await
                {
                    Ok(outcome) => performed_outcome = outcome,
                    Err(error) => drive_error = Some(error),
                }
            }
            wallet_core::DecideAndJournal::Skip => summary.skipped += 1,
            wallet_core::DecideAndJournal::TerminalFailed => {
                summary.skipped += 1;
                summary.terminal_failed_skipped += 1;
            }
        }
        Ok((summary, performed_outcome, drive_error))
    }

    async fn terminal_intent_error(&self, key: &IdempotencyKey) -> Result<ExecError, ExecError> {
        let reason = self
            .journal
            .operation(&OperationRef::Key(key.clone()))
            .await?
            .and_then(|row| row.error)
            .unwrap_or_else(|| format!("intent {} previously failed", key.0));
        Ok(ExecError::Permanent(reason))
    }

    /// The BOLT11 surfaced for an intent (spec §7's `invoice_for`): read the persisted
    /// `MoveRecord.invoice`. `None` before the invoice is minted (or for a non-move intent).
    pub async fn invoice_for(&self, key: &IdempotencyKey) -> Result<Option<Invoice>, ExecError> {
        Ok(self
            .journal
            .get_move(key)
            .await?
            .and_then(|rec| rec.invoice))
    }

    /// Route an inflow to `to` netting EXACTLY `amount` (spec §6/§7). Builds a `DirectInflow`
    /// decision under a deterministic key and drives it through `wallet_core::apply`: `perform`
    /// sizes + cap-checks + mints the receive invoice, persists the `MoveRecord`, and returns
    /// `Awaiting` (the payer is external). Idempotent on the key — a re-run of the same
    /// (`to`, `amount`, `fee_cap`, `occurrence`) finds the `Awaiting` intent and SKIPS the drive
    /// (no second invoice), while we still surface the already-minted invoice from the journal.
    pub async fn direct_inflow(
        &self,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        occurrence: Occurrence,
    ) -> anyhow::Result<DirectInflowOutcome> {
        let key = direct_inflow_key(&to, amount, fee_cap, occurrence);
        let attached = self.journal.get(&key).await.map_err(exec_err)?.is_some();
        if !attached {
            // The preflight exists to catch DETERMINISTIC rejections (lnv2 dust) before an
            // intent is journaled. A RETRYABLE failure here (e.g. the never-over quote loop
            // not settling this instant) must NOT hard-fail the command pre-journal — there
            // would be no pending intent for `reconcile`/a same-occurrence re-run to
            // re-drive. Proceed to journal + drive instead: `perform` re-quotes from
            // scratch, and if the quotes are still unstable it leaves the intent `Pending`
            // for the re-drive paths, which is the documented behavior.
            match self
                .executor()
                .validate_direct_inflow_amount(to, amount)
                .await
            {
                Ok(()) => {}
                Err(ExecError::Retryable(reason)) => tracing::warn!(
                    %reason,
                    "direct-inflow preflight retryable; journaling the intent and driving anyway"
                ),
                Err(e) => return Err(exec_err(e)),
            }
        }
        let decision = AllocatorDecision {
            action: Action::DirectInflow {
                to,
                amount,
                fee_cap,
            },
            // A plain operator verb (§8): the ledger records it as user-initiated.
            reason: ReasonCode::UserInitiated,
            occurrence,
            idempotency_key: key.clone(),
        };
        let balances = if attached || self.hard_cap.is_none() || !self.mc.has_client(&to) {
            None
        } else {
            Some(BTreeMap::from([(
                to,
                self.mc
                    .balance(&to)
                    .await
                    .map_err(|error| exec_err(ExecError::Retryable(error.to_string())))?,
            )]))
        };
        let _summary = self
            .decide_and_drive(&decision, Actor::User, balances.as_ref(), self.hard_cap)
            .await
            .map_err(exec_err)?;

        // Read the intent + its derived record together so we can complete a transition that a
        // crash in `await_move` interrupted (spec §9.5): if `settle_move` wrote a terminal record
        // phase but the process died before the intent CAS landed, the intent is stuck Awaiting
        // over already-final receive state. Finish that transition here before reporting status.
        let current_intent = self.journal.get(&key).await.map_err(exec_err)?;
        let mut status = current_intent.as_ref().map(|intent| intent.status);
        let record = self.journal.get_move(&key).await.map_err(exec_err)?;
        if status == Some(IntentStatus::Awaiting) {
            match record.as_ref().map(|rec| rec.phase) {
                Some(MovePhase::Settled) => {
                    self.finalize(
                        &key,
                        current_intent
                            .as_ref()
                            .expect("Awaiting intent was read")
                            .attempt,
                        IntentStatus::Done,
                    )
                    .await?;
                    status = Some(IntentStatus::Done);
                }
                Some(MovePhase::Failed) => {
                    self.finalize(
                        &key,
                        current_intent
                            .as_ref()
                            .expect("Awaiting intent was read")
                            .attempt,
                        IntentStatus::Failed,
                    )
                    .await?;
                    status = Some(IntentStatus::Failed);
                }
                _ => {}
            }
        }
        let invoice = record.and_then(|rec| rec.invoice);
        Ok(DirectInflowOutcome {
            key,
            invoice,
            status,
        })
    }

    /// Transfer `amount` net ecash from federation `from` to `to` through the shared gateway's
    /// internal swap (spec §7): B (`to`) receives, A (`from`) pays, both legs settle. Builds a
    /// `Move` decision under a deterministic key and drives it through `wallet_core::apply`;
    /// `perform` runs the WHOLE two-leg move to completion (it is synchronous — it returns
    /// `Done` when settled, never `Awaiting`), so this returns once the move is terminal.
    ///
    /// Idempotent on the key: a re-run of the same (`from`, `to`, `amount`, `fee_cap`,
    /// `occurrence`) reattaches to the in-flight/settled move (backfill + the lnv2 send dedup)
    /// and never re-mints or re-pays. A transient fault leaves the intent `Pending` (re-drivable
    /// by `reconcile` or a same-occurrence re-run with `--gateway`); a `Permanent` fault (fee
    /// over cap, refund/failed settlement) leaves it `Failed`, its reason on the `MoveRecord`.
    ///
    /// `reason`/`actor` are the ledger provenance (§8 / phase 5 §5.0.5): the CLI `move` verb
    /// passes `UserInitiated`/`User`; [`Self::active_probe`] threads `ActiveProbe` plus its
    /// caller's actor so both probe legs are explained in `history`.
    #[allow(clippy::too_many_arguments)]
    pub async fn do_move(
        &self,
        from: FederationId,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        occurrence: Occurrence,
        reason: ReasonCode,
        actor: Actor,
    ) -> anyhow::Result<MoveOutcome> {
        let key = move_key(&from, &to, amount, fee_cap, occurrence);
        let attached = self.journal.get(&key).await.map_err(exec_err)?.is_some();
        let decision = AllocatorDecision {
            action: Action::Move {
                from,
                to,
                amount,
                fee_cap,
                gateway: None,
            },
            reason,
            occurrence,
            idempotency_key: key.clone(),
        };
        let balances = if attached || !self.mc.has_client(&from) || !self.mc.has_client(&to) {
            None
        } else {
            Some(BTreeMap::from([
                (
                    from,
                    self.mc
                        .balance(&from)
                        .await
                        .map_err(|error| exec_err(ExecError::Retryable(error.to_string())))?,
                ),
                (
                    to,
                    self.mc
                        .balance(&to)
                        .await
                        .map_err(|error| exec_err(ExecError::Retryable(error.to_string())))?,
                ),
            ]))
        };
        let _summary = self
            .decide_and_drive(&decision, actor, balances.as_ref(), self.hard_cap)
            .await
            .map_err(exec_err)?;

        let status = self
            .journal
            .get(&key)
            .await
            .map_err(exec_err)?
            .map(|i| i.status);
        let outcome = self
            .journal
            .get_move(&key)
            .await
            .map_err(exec_err)?
            .and_then(|rec| rec.outcome);
        Ok(MoveOutcome {
            key,
            status,
            outcome,
        })
    }

    pub async fn pay(
        &self,
        from: FederationId,
        invoice: Invoice,
        amount: Msat,
        fee_cap: Msat,
        payment_hash: [u8; 32],
        gateway: Option<GatewayUrl>,
    ) -> anyhow::Result<RawPayOutcome> {
        let details = parse_invoice(&invoice)?;
        anyhow::ensure!(
            details.payment_hash == payment_hash,
            "payment hash does not match the invoice"
        );
        let invoice_amount = details.amount.ok_or_else(|| {
            anyhow::anyhow!(
                "amountless BOLT11 invoices are not supported by the pinned lnv2 client"
            )
        })?;
        anyhow::ensure!(
            invoice_amount == amount,
            "stated amount does not match the invoice amount"
        );
        let key = raw_pay_key(payment_hash);
        let attached = self.journal.get(&key).await.map_err(exec_err)?.is_some();
        let decision = AllocatorDecision {
            action: Action::Pay {
                from,
                invoice,
                amount,
                fee_cap,
                payment_hash,
                gateway,
            },
            reason: ReasonCode::UserInitiated,
            occurrence: Occurrence(0),
            idempotency_key: key.clone(),
        };
        let balances = if attached {
            None
        } else {
            Some(BTreeMap::from([(
                from,
                self.mc
                    .balance(&from)
                    .await
                    .map_err(|error| exec_err(ExecError::Retryable(error.to_string())))?,
            )]))
        };
        let (summary, performed_outcome, drive_error) = self
            .decide_and_drive(&decision, Actor::User, balances.as_ref(), None)
            .await
            .map_err(exec_err)?;
        if let Some(error) = drive_error {
            return Err(exec_err(error));
        }
        if summary.terminal_failed_skipped > 0 {
            return Err(exec_err(
                self.terminal_intent_error(&key).await.map_err(exec_err)?,
            ));
        }
        let intent = self
            .journal
            .get(&key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| anyhow::anyhow!("raw pay intent {} was not journaled", key.0))?;
        Ok(RawPayOutcome {
            key,
            operation_id: intent.operation_id.ok_or_else(|| {
                anyhow::anyhow!("raw pay intent has no durable send operation artifact")
            })?,
            status: intent.status,
            already_in_flight: matches!(
                performed_outcome,
                Some(PerformOutcome::AwaitingAlreadyInFlight)
            ) || (performed_outcome.is_none()
                && attached
                && intent.status == IntentStatus::Awaiting),
        })
    }

    pub async fn receive(
        &self,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        nonce: String,
        gateway: Option<GatewayUrl>,
    ) -> anyhow::Result<RawReceiveOutcome> {
        let key = raw_receive_key(to, amount, &nonce);
        let attached = self.journal.get(&key).await.map_err(exec_err)?.is_some();
        let decision = AllocatorDecision {
            action: Action::Receive {
                to,
                amount,
                fee_cap,
                nonce,
                gateway,
            },
            reason: ReasonCode::UserInitiated,
            occurrence: Occurrence(0),
            idempotency_key: key.clone(),
        };
        let balances = if attached {
            None
        } else {
            Some(BTreeMap::from([(
                to,
                self.mc
                    .balance(&to)
                    .await
                    .map_err(|error| exec_err(ExecError::Retryable(error.to_string())))?,
            )]))
        };
        let (summary, _, drive_error) = self
            .decide_and_drive(&decision, Actor::User, balances.as_ref(), self.hard_cap)
            .await
            .map_err(exec_err)?;
        if let Some(error) = drive_error {
            return Err(exec_err(error));
        }
        if summary.terminal_failed_skipped > 0 {
            return Err(exec_err(
                self.terminal_intent_error(&key).await.map_err(exec_err)?,
            ));
        }
        let intent = self
            .journal
            .get(&key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| anyhow::anyhow!("raw receive intent {} was not journaled", key.0))?;
        Ok(RawReceiveOutcome {
            key,
            operation_id: intent.operation_id.ok_or_else(|| {
                anyhow::anyhow!("raw receive intent has no durable operation artifact")
            })?,
            invoice: intent
                .invoice
                .ok_or_else(|| anyhow::anyhow!("raw receive intent has no durable invoice"))?,
            status: intent.status,
        })
    }

    pub async fn join(
        &self,
        federation: FederationId,
        invite: String,
    ) -> anyhow::Result<JoinIntentOutcome> {
        let parsed = InviteCode::from_str(&invite)?;
        let invite_federation = crate::multi_client::bridge_federation_id(parsed.federation_id());
        anyhow::ensure!(
            invite_federation == federation,
            "join federation does not match the invite"
        );
        let invite = parsed.to_string();
        let key = join_intent_key(federation, &invite);
        let membership_preexisting = match self.journal.get(&key).await.map_err(exec_err)? {
            Some(Intent {
                action:
                    Action::Join {
                        membership_preexisting,
                        ..
                    },
                ..
            }) => membership_preexisting,
            Some(_) => false,
            None => self
                .journal
                .get_federation(&federation)
                .await
                .map_err(exec_err)?
                .is_some(),
        };
        let decision = AllocatorDecision {
            action: Action::Join {
                federation,
                invite,
                membership_preexisting,
            },
            reason: ReasonCode::UserInitiated,
            occurrence: Occurrence(0),
            idempotency_key: key.clone(),
        };
        let (summary, _, drive_error) = self
            .decide_and_drive(&decision, Actor::User, None, None)
            .await
            .map_err(exec_err)?;
        if let Some(error) = drive_error {
            return Err(exec_err(error));
        }
        if summary.terminal_failed_skipped > 0 {
            return Err(exec_err(
                self.terminal_intent_error(&key).await.map_err(exec_err)?,
            ));
        }
        let status = self
            .journal
            .get(&key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| anyhow::anyhow!("join intent {} was not journaled", key.0))?
            .status;
        Ok(JoinIntentOutcome { key, status })
    }

    /// Run one discovery pass (§5.1.2): union source announcements, authenticate configs without
    /// joining, write candidate rows, and optionally auto-join within the configured caps.
    pub async fn discover(
        &self,
        sources: Vec<Box<dyn CandidateSource>>,
        policy: DiscoveryPolicy,
    ) -> anyhow::Result<DiscoverReport> {
        // A one-off `discover` (the CLI verb) is NOT the watch scheduler: it must NOT resume from
        // or write back the persisted `0x0a` watch cursor/backlog — that rotation state belongs to
        // `watch` (5.2b). Sharing it here would let an ad-hoc `discover --invite X` resume
        // mid-rotation and SKIP X, and its write-back would clobber the loop's backlog. So run ONE
        // fresh bounded pass (the per-preview timeout / whole-pass deadline / candidate cap still
        // apply WITHIN the pass); cross-pass cursor rotation is wired only by the scheduler.
        let nonce = ledger_nonce();
        let now = now_ms();
        let outcome = run_discover_pass_bounded_with_rotation(
            &sources,
            &policy,
            self,
            now,
            &nonce,
            &WatchPolicy::default(),
            DiscoverPassResume {
                cursor: None,
                rotation: &[],
                occurrence: Occurrence(0),
            },
        )
        .await?;
        Ok(outcome.report)
    }

    pub async fn watch_once(
        &self,
        tick_policy: &TickPolicy,
        watch_policy: &WatchPolicy,
        sources: &[Box<dyn CandidateSource>],
        discovery_policy: &DiscoveryPolicy,
        discover_enabled: bool,
    ) -> anyhow::Result<WatchCycleReport> {
        let reconcile = match self.reconcile().await {
            Ok(summary) => WatchReconcileOutcome::Ran(summary),
            Err(e) => {
                tracing::warn!(error = ?e, "watch: reconcile failed; continuing cycle");
                WatchReconcileOutcome::Failed(e.to_string())
            }
        };

        let advanced = self
            .journal
            .advance_watch_occurrence()
            .await
            .map_err(exec_err)?;
        let occurrence = Occurrence(advanced.occurrence);
        let mut cycle_tick_policy = tick_policy.clone();
        cycle_tick_policy.occurrence = occurrence;

        let tick = match &reconcile {
            // Reconcile faulted, so the pending-move state is unknown. Running the tick
            // now could re-issue a still-`Pending` prior-occurrence move under this
            // cycle's fresh occurrence (a distinct idempotency key). Fail safe: skip the
            // tick and let the next cycle re-drive once reconcile succeeds. This is the ONE
            // remaining global skip — unknown eligibility, not merely blocked eligibility.
            WatchReconcileOutcome::Failed(_) => {
                tracing::warn!("watch: reconcile failed; skipping tick to avoid duplicate intents");
                WatchTickOutcome::SkippedReconcileFailed
            }
            // Reconcile succeeded, so the durable state IS known: tick, projected through the
            // goals that pass left in flight (br-p93). Only work duplicating one of those goals
            // is withheld; every independent decision proceeds, including for another federation
            // whose evacuation is the whole reason the wallet must stay responsive.
            WatchReconcileOutcome::Ran(summary) => {
                // Reported, not carried: `tick` re-derives this projection from the same durable
                // source at plan time and again before apply, which is strictly fresher than a
                // set snapshotted here. See `ReconcileSummary::blocked`.
                if !summary.blocked.is_empty() {
                    tracing::info!(
                        blocked = ?summary.blocked.goals(),
                        "watch: ticking with the in-flight allocator goals suppressed"
                    );
                }
                match self.tick_for_daemon_scheduler(&cycle_tick_policy).await {
                    Ok(report) => WatchTickOutcome::Ran(report),
                    Err(e) => {
                        tracing::warn!(error = ?e, "watch: tick failed; continuing cycle");
                        WatchTickOutcome::Failed(e.to_string())
                    }
                }
            }
        };
        let spending = match &tick {
            WatchTickOutcome::Ran(report) => report.spending_fed,
            WatchTickOutcome::SkippedReconcileFailed | WatchTickOutcome::Failed(_) => self
                .status_for_daemon_scheduler(&cycle_tick_policy)
                .await
                .map(|status| status.spending_fed)
                .unwrap_or(cycle_tick_policy.spending_fed),
        };

        let probe_now = now_ms();
        let mut probe_context = self.probe_schedule_context(probe_now, watch_policy).await?;
        let (mut probes, budget_usage) = self
            .run_scheduled_probes(
                occurrence,
                spending,
                &cycle_tick_policy,
                watch_policy,
                probe_now,
                &mut probe_context,
            )
            .await?;

        let state_before_discover = self.journal.get_watch_state().await.map_err(exec_err)?;
        let now = now_ms();
        let discover = if !discover_enabled {
            WatchDiscoverOutcome::Disabled
        } else if !discovery_due(&state_before_discover, watch_policy, now) {
            WatchDiscoverOutcome::NotDue {
                next_due_ms: state_before_discover
                    .last_discover_ms
                    .saturating_add(watch_policy.discover_every_ms),
            }
        } else {
            let nonce = ledger_nonce();
            match run_discover_pass_bounded_with_rotation_and_probe_policy(
                sources,
                discovery_policy,
                &cycle_tick_policy.probe_gate_policy,
                self,
                now,
                &nonce,
                watch_policy,
                DiscoverPassResume {
                    cursor: state_before_discover.discover_cursor,
                    rotation: &state_before_discover.discover_rotation,
                    occurrence,
                },
            )
            .await
            {
                Ok(outcome) => {
                    let progress = outcome.report.progress;
                    self.journal
                        .put_watch_discovery_state(
                            progress.next_cursor,
                            progress.backlog,
                            Some(now),
                            outcome.next_rotation,
                        )
                        .await
                        .map_err(exec_err)?;
                    WatchDiscoverOutcome::Ran(outcome.report)
                }
                Err(e) => {
                    tracing::warn!(error = ?e, "watch: discover failed; backing off");
                    // Advance the discovery clock AND clear the backlog flag so a persistent
                    // discover fault backs off by `discover_every` instead of retrying every
                    // cycle — `discovery_due`/`adaptive_sleep_ms` both short-circuit on
                    // backlog, so leaving it set would defeat the backoff. Preserve the
                    // cursor/rotation (the pass did not complete, so resume where it left
                    // off); a still-overflowing rotation re-sets backlog on the next
                    // SUCCESSFUL pass, so no deferred work is lost.
                    self.journal
                        .put_watch_discovery_state(
                            state_before_discover.discover_cursor,
                            false,
                            Some(now),
                            state_before_discover.discover_rotation.clone(),
                        )
                        .await
                        .map_err(exec_err)?;
                    WatchDiscoverOutcome::Failed(e.to_string())
                }
            }
        };

        let final_state = self.journal.get_watch_state().await.map_err(exec_err)?;
        let deadlines = self
            .watch_deadlines_with_context(
                &cycle_tick_policy,
                watch_policy,
                now_ms(),
                Some(&probe_context),
                &BTreeSet::new(),
                &BTreeSet::new(),
                false,
            )
            .await?;
        probes.sort_by_key(|probe| probe.fed);
        Ok(WatchCycleReport {
            occurrence,
            reconcile,
            tick,
            probes,
            discover,
            budget_usage,
            watch_state: final_state,
            deadlines,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn service_discover_cycle(
        &self,
        sources: &[Box<dyn CandidateSource>],
        discovery_policy: &DiscoveryPolicy,
        probe_policy: &ProbePolicy,
        watch_policy: &WatchPolicy,
        occurrence: Occurrence,
        now: u64,
        membership_client: Option<&crate::service::WalletClient>,
    ) -> anyhow::Result<()> {
        let state = self.journal.get_watch_state().await.map_err(exec_err)?;
        if !discovery_due(&state, watch_policy, now) {
            return Ok(());
        }
        let nonce = ledger_nonce();
        match run_discover_pass_bounded_with_rotation_and_probe_policy_with_membership_lease(
            sources,
            discovery_policy,
            probe_policy,
            self,
            membership_client,
            now,
            &nonce,
            watch_policy,
            DiscoverPassResume {
                cursor: state.discover_cursor,
                rotation: &state.discover_rotation,
                occurrence,
            },
        )
        .await
        {
            Ok(outcome) => {
                let progress = outcome.report.progress;
                self.journal
                    .put_watch_discovery_state(
                        progress.next_cursor,
                        progress.backlog,
                        Some(now),
                        outcome.next_rotation,
                    )
                    .await
                    .map_err(exec_err)?;
            }
            Err(error) => {
                tracing::warn!(?error, "watch: discover failed; backing off");
                self.journal
                    .put_watch_discovery_state(
                        state.discover_cursor,
                        false,
                        Some(now),
                        state.discover_rotation,
                    )
                    .await
                    .map_err(exec_err)?;
            }
        }
        Ok(())
    }

    pub async fn watch_deadlines(
        &self,
        tick_policy: &TickPolicy,
        watch_policy: &WatchPolicy,
        now_ms: u64,
    ) -> anyhow::Result<AdaptiveSleepDeadlines> {
        self.watch_deadlines_with_context(
            tick_policy,
            watch_policy,
            now_ms,
            None,
            &BTreeSet::new(),
            &BTreeSet::new(),
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn service_watch_deadlines(
        &self,
        tick_policy: &TickPolicy,
        watch_policy: &WatchPolicy,
        now_ms: u64,
        registry_owned_probes: &BTreeSet<FederationId>,
        retry_probes: &BTreeSet<FederationId>,
        defer_fresh_probes: bool,
    ) -> anyhow::Result<AdaptiveSleepDeadlines> {
        self.watch_deadlines_with_context(
            tick_policy,
            watch_policy,
            now_ms,
            None,
            registry_owned_probes,
            retry_probes,
            defer_fresh_probes,
        )
        .await
    }

    pub async fn watch_deadlines_reusing_probe_schedule(
        &self,
        now_ms: u64,
        previous: &AdaptiveSleepDeadlines,
        hinted_expiry_ms: Option<u64>,
    ) -> anyhow::Result<AdaptiveSleepDeadlines> {
        let state = self.journal.get_watch_state().await.map_err(exec_err)?;
        let mut deadlines = AdaptiveSleepDeadlines {
            last_discover_ms: state.last_discover_ms,
            discover_backlog: state.discover_backlog,
            expiries_ms: previous
                .expiries_ms
                .iter()
                .copied()
                .filter(|expiry_ms| *expiry_ms > now_ms)
                .collect(),
            probe_due_ms: previous.probe_due_ms.clone(),
        };
        if let Some(expiry_ms) = hinted_expiry_ms {
            add_expiry_deadline(&mut deadlines, expiry_ms, now_ms);
        }
        Ok(deadlines)
    }

    #[allow(clippy::too_many_arguments)]
    async fn watch_deadlines_with_context(
        &self,
        tick_policy: &TickPolicy,
        watch_policy: &WatchPolicy,
        now_ms: u64,
        context: Option<&ProbeScheduleContext>,
        registry_owned_probes: &BTreeSet<FederationId>,
        retry_probes: &BTreeSet<FederationId>,
        defer_fresh_probes: bool,
    ) -> anyhow::Result<AdaptiveSleepDeadlines> {
        let state = self.journal.get_watch_state().await.map_err(exec_err)?;
        let mut deadlines = AdaptiveSleepDeadlines {
            last_discover_ms: state.last_discover_ms,
            discover_backlog: state.discover_backlog,
            ..AdaptiveSleepDeadlines::default()
        };

        let raw_probes = self.probe_all().await;
        add_expiry_deadlines(&mut deadlines, &raw_probes, now_ms);

        let spending = match self
            .designated_spending_from_probes(tick_policy, &ScorerPolicy::default(), &raw_probes)
            .await
        {
            Ok(spending) => spending,
            Err(e) => {
                tracing::warn!(error = ?e, "watch: designation failed while computing probe deadlines");
                // A pin is policy, not fresh designation evidence. Retained sessions still carry
                // their durable `session.from`, while fresh probes remain source-less and cannot be
                // admitted until a later successful designation.
                None
            }
        };
        let context_storage;
        let context = match context {
            Some(context) => context,
            None => {
                context_storage = self.probe_schedule_context(now_ms, watch_policy).await?;
                &context_storage
            }
        };
        for input in self
            .probe_schedule_inputs(
                spending,
                &tick_policy.probe_gate_policy,
                watch_policy,
                now_ms,
                &context.last_invocations,
            )
            .await?
        {
            let ProbeScheduleInput {
                candidate,
                source,
                verdict: _,
                due_ms,
                session,
                post_in_resume: _,
            } = input;
            if source.is_none() {
                continue;
            }
            // The standalone 5.2 loop drives a retained session to completion. The service
            // actor returns as soon as it has attached that session to a live driver, so the
            // scheduler must not immediately re-attach it while the actor still owns it.
            if registry_owned_probes.contains(&candidate) {
                continue;
            }
            let mut wake_ms = if session.is_some() {
                due_ms
            } else {
                let budget_due_ms = probe_wake_due_ms(
                    due_ms,
                    now_ms,
                    context.budget_ok,
                    context.budget_reset_ms,
                    watch_policy,
                );
                context
                    .fresh_probe_defer_until_ms
                    .map_or(budget_due_ms, |defer_until| budget_due_ms.max(defer_until))
            };
            if retry_probes.contains(&candidate) {
                // `run_scheduled_probes` records a failed invocation in its schedule
                // context, which applies this same retry backoff. Actor refusals journal no
                // invocation, so carry that one-cycle fact explicitly into this recompute.
                wake_ms = wake_ms.max(now_ms.saturating_add(watch_policy.probe_retry_backoff_ms));
            } else if defer_fresh_probes {
                // A retained session displaced the fresh group this cycle. This is the
                // async equivalent of 5.2's `fresh_probe_defer_until_ms` after a retained
                // probe remains in flight.
                wake_ms = wake_ms.max(now_ms.saturating_add(watch_policy.min_interval_ms));
            }
            deadlines.probe_due_ms.push(wake_ms);
        }
        Ok(deadlines)
    }

    async fn run_scheduled_probes(
        &self,
        occurrence: Occurrence,
        spending: Option<FederationId>,
        tick_policy: &TickPolicy,
        watch_policy: &WatchPolicy,
        now: u64,
        context: &mut ProbeScheduleContext,
    ) -> anyhow::Result<(Vec<WatchProbeReport>, ProbeBudgetUsage)> {
        let mut reports = Vec::new();
        let inputs = self
            .probe_schedule_inputs(
                spending,
                &tick_policy.probe_gate_policy,
                watch_policy,
                now,
                &context.last_invocations,
            )
            .await?;
        for input in inputs {
            let ProbeScheduleInput {
                candidate,
                source,
                verdict,
                due_ms,
                session,
                post_in_resume: _,
            } = input;
            let retained = session.is_some();
            let Some(source) = source else {
                reports.push(WatchProbeReport {
                    fed: candidate,
                    verdict,
                    due_ms,
                    outcome: WatchProbeOutcome::NoSource,
                });
                continue;
            };
            let outcome = if !retained && due_ms > now {
                if verdict == ActiveProbeVerdict::Passed {
                    WatchProbeOutcome::Passed
                } else {
                    WatchProbeOutcome::NotDue
                }
            } else if !retained
                && context
                    .fresh_probe_defer_until_ms
                    .is_some_and(|defer_until| defer_until > now)
            {
                WatchProbeOutcome::DeferredByInFlight
            } else if !retained && !context.budget_ok {
                let reason = "watch probe skipped: weekly probe budget exhausted";
                if let Err(e) = self
                    .record_watch_probe_skip(
                        candidate,
                        source,
                        tick_policy.probe_gate_policy.amount_msat,
                        occurrence,
                        budget_skip_diagnostic_bucket_ms(now, context.budget_reset_ms),
                        reason,
                    )
                    .await
                {
                    tracing::warn!(
                        federation = %candidate.to_hex(),
                        error = ?e,
                        "watch: recording budget-blocked probe skip failed"
                    );
                }
                WatchProbeOutcome::BudgetBlocked
            } else {
                match self
                    .active_probe(
                        candidate,
                        source,
                        &tick_policy.probe_gate_policy,
                        Actor::Agent { occurrence },
                    )
                    .await
                {
                    Ok(report) => {
                        context.record_invocation(candidate, report.source, now);
                        if let Some(cost) = report.cost_msat {
                            context.record_budget_attempt(cost.0, now, watch_policy);
                        }
                        WatchProbeOutcome::Attempted
                    }
                    Err(e) => {
                        let retained_source = self.active_probe_source(candidate).await?;
                        let actual_source = retained_source.unwrap_or(source);
                        context.record_invocation(candidate, actual_source, now);
                        if retained_source.is_some() {
                            context.defer_fresh_probes_until(
                                now.saturating_add(watch_policy.min_interval_ms),
                            );
                        }
                        tracing::warn!(
                            federation = %candidate.to_hex(),
                            error = ?e,
                            "watch: scheduled probe failed; continuing cycle"
                        );
                        WatchProbeOutcome::Failed(e.to_string())
                    }
                }
            };
            reports.push(WatchProbeReport {
                fed: candidate,
                verdict,
                due_ms,
                outcome,
            });
        }
        Ok((reports, context.budget_usage))
    }

    async fn probe_schedule_inputs(
        &self,
        spending: Option<FederationId>,
        gate_policy: &ProbePolicy,
        watch_policy: &WatchPolicy,
        now_ms: u64,
        last_invocations: &BTreeMap<(FederationId, FederationId), u64>,
    ) -> anyhow::Result<Vec<ProbeScheduleInput>> {
        let mut out = Vec::new();
        for candidate in self.auto_joined_candidates().await? {
            let record = self
                .journal
                .probe_record(&candidate)
                .await
                .map_err(exec_err)?
                .unwrap_or_default();
            let source = match record.in_flight.as_ref() {
                Some(session) => session.from,
                None => match spending {
                    Some(spending) if candidate != spending => spending,
                    Some(_) => continue,
                    None => {
                        out.push(ProbeScheduleInput {
                            candidate,
                            source: None,
                            verdict: ActiveProbeVerdict::NeverProbed,
                            due_ms: now_ms,
                            session: None,
                            post_in_resume: false,
                        });
                        continue;
                    }
                },
            };
            let post_in_resume = match record.in_flight.as_ref() {
                Some(session) => self.probe_session_has_leg_in(candidate, session).await?,
                None => false,
            };
            let verdict = probe_verdict(&record.attempts, source, now_ms, gate_policy);
            let last_invocation = last_invocations.get(&(candidate, source)).copied();
            let due_base = probe_due_base_ms(verdict, &record, source, now_ms, gate_policy);
            let mut due_ms = probe_next_due_at(
                verdict,
                due_base,
                last_invocation,
                now_ms,
                watch_policy,
                gate_policy,
            );
            if post_in_resume {
                due_ms = now_ms;
            }
            out.push(ProbeScheduleInput {
                candidate,
                source: Some(source),
                verdict,
                due_ms,
                session: record.in_flight,
                post_in_resume,
            });
        }
        // Resume retained in-flight probe sessions before starting fresh probes: a failed
        // resume defers fresh probes for the rest of the cycle, so in-flight money-moving
        // work is always driven to completion first. Stable sort keeps the deterministic
        // per-candidate order within each group.
        out.sort_by_key(|input| !input.post_in_resume);
        Ok(out)
    }

    async fn probe_schedule_context(
        &self,
        now_ms: u64,
        watch_policy: &WatchPolicy,
    ) -> anyhow::Result<ProbeScheduleContext> {
        let scan_horizon_ms = PROBE_BUDGET_WINDOW_MS.max(watch_policy.probe_retry_backoff_ms);
        let rows = self
            .journal
            .probe_schedule_ledger_rows(now_ms, scan_horizon_ms)
            .await
            .map_err(exec_err)?;
        let budget_effective_at_ms =
            |row: &OperationRecord| row.created_at_ms.max(row.updated_at_ms);
        let budget_rows = rows.iter().filter(|row| {
            now_ms.saturating_sub(budget_effective_at_ms(row)) < PROBE_BUDGET_WINDOW_MS
        });
        let budget_usage = probe_budget_usage(budget_rows);
        let budget_reset_ms = rows
            .iter()
            .filter(|row| {
                now_ms.saturating_sub(budget_effective_at_ms(row)) < PROBE_BUDGET_WINDOW_MS
            })
            .filter(|row| budget_counted_probe_cost_msat(row).is_some())
            .map(|row| budget_effective_at_ms(row).saturating_add(PROBE_BUDGET_WINDOW_MS))
            .min();
        let mut context = ProbeScheduleContext::new(budget_usage, budget_reset_ms, watch_policy);
        for row in rows {
            if matches!(row.actor, Actor::Agent { .. }) && row.reason == ReasonCode::ActiveProbe {
                if let OperationKind::Probe { fed, from, .. } = &row.kind {
                    let invoked_at = row.created_at_ms.max(row.updated_at_ms);
                    context.record_invocation(*fed, *from, invoked_at);
                }
            }
        }
        context.budget_ok = probe_budget_ok(
            context.budget_usage.attempts,
            context.budget_usage.spend_msat,
            &watch_policy.probe_budget,
        );
        Ok(context)
    }

    async fn probe_session_has_leg_in(
        &self,
        candidate: FederationId,
        session: &ProbeSession,
    ) -> anyhow::Result<bool> {
        if session.out_net_msat.is_some() {
            return Ok(true);
        }
        let occurrence = occurrence_from_nonce(&session.nonce)?;
        let in_key = move_key(
            &session.from,
            &candidate,
            Msat(session.amount_msat),
            Msat(session.leg_fee_cap_msat),
            occurrence,
        );
        Ok(self.journal.get(&in_key).await.map_err(exec_err)?.is_some())
    }

    async fn active_probe_source(
        &self,
        candidate: FederationId,
    ) -> anyhow::Result<Option<FederationId>> {
        Ok(self
            .journal
            .probe_record(&candidate)
            .await
            .map_err(exec_err)?
            .and_then(|record| record.in_flight.map(|session| session.from)))
    }

    async fn record_watch_probe_skip(
        &self,
        candidate: FederationId,
        spending: FederationId,
        amount_msat: u64,
        occurrence: Occurrence,
        diagnostic_bucket_ms: u64,
        reason: &str,
    ) -> Result<(), ExecError> {
        let key = IdempotencyKey(format!(
            "watch-probe-skip:{}:{}:{}:{}",
            candidate.to_hex(),
            spending.to_hex(),
            amount_msat,
            diagnostic_bucket_ms
        ));
        let now = now_ms();
        self.journal
            .record_started(
                &key,
                OperationKind::Probe {
                    fed: candidate,
                    from: spending,
                    amount_msat: Msat(amount_msat),
                    cost_msat: None,
                },
                Actor::Agent { occurrence },
                ReasonCode::StandingInstruction,
                now,
                None,
            )
            .await?;
        self.journal
            .record_terminal(&key, OperationStatus::Failed, now, Some(reason), None)
            .await
    }

    /// Run ONE active probe of `candidate` from spending federation `from` (phase 5
    /// §5.0.5): a two-leg, exact-net round trip on the real money path — leg IN mints
    /// `policy.amount_msat` on the candidate through the ordinary `Move` machinery, leg
    /// OUT redeems the affordably-sized delta back, and the finished attempt lands in the
    /// durable `0x08` history the pure [`probe_verdict`] evaluates.
    ///
    /// Ok = an ATTEMPT was recorded (a clean pass, or a demoting candidate-fault
    /// failure). Every other exit is an error: the no-attempt terminal exits (preflight/
    /// local/no-route/inconclusive — umbrella row `Failed`, session cleared, no demotion)
    /// and the transient still-pending legs (session RETAINED; a re-run of `probe`
    /// resumes it — step 0 below).
    pub async fn active_probe(
        &self,
        candidate: FederationId,
        from: FederationId,
        policy: &ProbePolicy,
        actor: Actor,
    ) -> anyhow::Result<ProbeReport> {
        self.active_probe_inner(candidate, from, policy, actor, self.hard_cap, None, None)
            .await
    }

    /// Service probe orchestration. Probe session/verdict mechanics stay here, while
    /// each money leg enters through the service actor's shared admission guard.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn service_active_probe(
        &self,
        candidate: FederationId,
        from: FederationId,
        expected_session_nonce: String,
        policy: &ProbePolicy,
        actor: Actor,
        per_fed_cap: Msat,
        client: crate::service::WalletClient,
    ) -> anyhow::Result<ProbeReport> {
        #[cfg(test)]
        let start_hold = self
            .test_service_probe_start_hold
            .lock()
            .expect("service-probe start-hold mutex poisoned")
            .take();
        #[cfg(test)]
        if let Some(hold) = start_hold {
            hold.notified().await;
        }
        self.active_probe_inner(
            candidate,
            from,
            policy,
            actor,
            Some(per_fed_cap),
            Some(client),
            Some(expected_session_nonce),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn active_probe_inner(
        &self,
        candidate: FederationId,
        from: FederationId,
        policy: &ProbePolicy,
        actor: Actor,
        hard_cap: Option<Msat>,
        service_client: Option<crate::service::WalletClient>,
        expected_session_nonce: Option<String>,
    ) -> anyhow::Result<ProbeReport> {
        let record = self
            .journal
            .probe_record(&candidate)
            .await
            .map_err(exec_err)?;
        let attempts_before = record
            .as_ref()
            .map(|r| r.attempts.clone())
            .unwrap_or_default();

        // §5.0.5 step 0: resume FIRST — an in-flight session owns this invocation (its
        // parameters, including `from`, are fixed). Service drivers additionally carry the exact
        // actor-approved nonce and can never enter the standalone fresh branch if that durable
        // session clears or is replaced before this detached task runs.
        let durable_session = record.and_then(|r| r.in_flight);
        let (mut session, resuming) = match (expected_session_nonce, durable_session) {
            (Some(expected), Some(session)) if session.nonce == expected => (session, true),
            (Some(expected), Some(session)) => {
                anyhow::bail!(
                    "service probe session changed after actor admission: expected {}, found {}",
                    expected,
                    session.nonce
                );
            }
            (Some(expected), None) => {
                anyhow::bail!(
                    "service probe session cleared after actor admission: expected {expected}"
                );
            }
            (None, Some(session)) => {
                if session.from != from {
                    tracing::warn!(
                        session_from = %session.from.to_hex(),
                        requested_from = %from.to_hex(),
                        "probe: resuming the in-flight session; its recorded source wins"
                    );
                }
                (session, true)
            }
            (None, None) => {
                // Fresh probe: sample the no-sweep BASELINE before anything else. An
                // unopened candidate reads 0 — safe, because the preflight below refuses
                // it before any money path (leg OUT, the only baseline consumer, is
                // unreachable); an OPEN candidate whose read fails bails here, pre-session
                // (nothing durable written yet), rather than record a too-low baseline
                // that would weaken the §5.0.4 guard.
                let baseline = if self.mc.federations().contains(&candidate) {
                    self.mc
                        .balance(&candidate)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("probe: sampling the candidate baseline failed: {e}")
                        })?
                        .0
                } else {
                    0
                };
                let session = ProbeSession {
                    nonce: ledger_nonce(),
                    from,
                    amount_msat: policy.amount_msat,
                    leg_fee_cap_msat: policy.leg_fee_cap_msat,
                    c_spendable_before_in_msat: baseline,
                    out_net_msat: None,
                    started_at_ms: now_ms(),
                };
                self.journal
                    .begin_probe_session(&candidate, &session)
                    .await
                    .map_err(exec_err)?;
                (session, false)
            }
        };
        let occurrence = occurrence_from_nonce(&session.nonce)?;
        let amount = Msat(session.amount_msat);
        let leg_fee_cap = Msat(session.leg_fee_cap_msat);
        // The MONEY params are the SESSION's, not the caller's flags: a resume runs the
        // legs with the stored amount/fee_cap, so the verdict must qualify the resulting
        // attempt against those same values — otherwise an operator changing `--amount` on
        // resume would judge the just-spent attempt against thresholds it was never run
        // with (flipping it qualifying/non-qualifying). On a FRESH probe the session was
        // built FROM these same flags, so this is a no-op there. The verdict-WINDOW fields
        // (min_successes/span/ttl) stay the caller's.
        let effective_policy = ProbePolicy {
            amount_msat: session.amount_msat,
            leg_fee_cap_msat: session.leg_fee_cap_msat,
            ..policy.clone()
        };
        let run = ProbeRun {
            candidate,
            source: session.from,
            actor,
            verdict_before: probe_verdict(
                &attempts_before,
                session.from,
                now_ms(),
                &effective_policy,
            ),
            nonce: session.nonce.clone(),
            umbrella_key: probe_umbrella_key(&candidate, &session.nonce),
            amount,
            leg_fee_cap,
            in_key: move_key(&session.from, &candidate, amount, leg_fee_cap, occurrence),
            effective_policy,
            started_at_ms: session.started_at_ms,
        };
        self.journal
            .record_probe_invocation(&run.umbrella_key, probe_kind(&run, None), actor, now_ms())
            .await
            .map_err(exec_err)?;

        // §5.0.5 step 1 — umbrella row then preflight, for a FRESH probe or a pre-leg-IN
        // resume ONLY (both re-enter here; §5.0.4's disambiguation): once leg IN is
        // journaled money may have moved, so fresh-probe balance/cap checks no longer hold
        // and would misclassify a recoverable probe as a new local error.
        let leg_in_journaled = self
            .journal
            .get(&run.in_key)
            .await
            .map_err(exec_err)?
            .is_some();
        if session.out_net_msat.is_none() && !leg_in_journaled {
            if resuming {
                tracing::info!(
                    candidate = %candidate.to_hex(),
                    "probe: resuming a pre-leg-IN session; re-running the preflight"
                );
            }
            if let Err(diagnostic) = self.probe_preflight(&session, candidate, hard_cap).await {
                return self.finish_probe_no_attempt(&run, &diagnostic, None).await;
            }
        }

        // Re-sample the no-sweep BASELINE immediately before leg IN — after the slow
        // preflight/route validation (gateway HTTP), during which a candidate-side receive
        // state machine could settle asynchronously and change the balance. Sampling here
        // (vs. pre-preflight) folds any such settlement into the pre-existing baseline, so
        // the exact-match resume guard isolates the probe delta precisely instead of
        // false-aborting a valid resume as "delta consumed" (a safe-direction failure, but
        // avoidable). ONLY before leg IN credits the candidate (`!leg_in_journaled`); a
        // post-IN resume keeps its recorded baseline. Best-effort: a read failure keeps the
        // early baseline (already durable), and a same-nonce write only fires on a change.
        if !leg_in_journaled && self.mc.federations().contains(&candidate) {
            if let Ok(fresh) = self.mc.balance(&candidate).await {
                if fresh.0 != session.c_spendable_before_in_msat {
                    session.c_spendable_before_in_msat = fresh.0;
                    self.journal
                        .begin_probe_session(&candidate, &session)
                        .await
                        .map_err(exec_err)?;
                }
            }
        }

        // §5.0.5 step 3 — leg IN (journals the intent; a resume reattaches idempotently).
        let in_outcome = self
            .drive_probe_leg(
                run.source,
                candidate,
                run.amount,
                run.leg_fee_cap,
                occurrence,
                actor,
                &session.nonce,
                service_client.as_ref(),
            )
            .await?;
        match in_outcome.status {
            Some(IntentStatus::Done) => {}
            Some(IntentStatus::Failed) => {
                return self
                    .finish_probe_failed_leg(&run, ProbeLeg::In, &run.in_key, None, None)
                    .await;
            }
            other => anyhow::bail!(
                "probe leg IN {} did not settle (status {}); transient — re-run `probe` to \
                 resume (session retained)",
                run.in_key.0,
                intent_status_label_opt(other)
            ),
        }
        let in_rec = self
            .journal
            .get_move(&run.in_key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| {
                anyhow::anyhow!("probe leg IN settled but its move record is missing")
            })?;
        // Leg IN's DELIVERED net (possibly a verified hair under the ask) — durable on the
        // move record, so the sizing budget survives a crash.
        let delivered_in = in_rec.amount;

        // §5.0.5 steps 4-5 — size leg OUT with budget = the delivered net, persist the
        // sized amount BEFORE journaling leg OUT (a resume never re-sizes).
        let out_net = match session.out_net_msat {
            Some(persisted) => Msat(persisted),
            None => {
                // Size leg OUT against a budget REDUCED by a fee-jitter margin. The final
                // fee cap (`probe_out_fee_cap`) is bounded by the FULL delivered_in for
                // no-sweep, so sizing out_net a margin smaller leaves that cap headroom
                // above the sizing-time fee ESTIMATE — absorbing the small upward re-quote
                // the Pay step can produce (observed live: an 8432-msat actual vs an
                // 8417-msat estimate deferred the whole probe). The margin becomes bounded
                // extra RESIDUE on the candidate (accepted, §5.0.9 decision 6); it stays
                // well under the leg fee cap, so the "residue < fee cap" invariant holds.
                let sizing_budget = Msat(delivered_in.0.saturating_sub(PROBE_FEE_MARGIN_MSAT));
                match self
                    .executor()
                    .size_probe_leg_out(candidate, run.source, sizing_budget, run.leg_fee_cap)
                    .await
                {
                    Ok(Some(sized)) => {
                        // First `out_net_msat` fill. Two callers racing this window (both
                        // re-sizing, both journaling a leg OUT against the same delta) is a
                        // CONCURRENCY hazard the wallet's SINGLE-WRITER architecture forecloses
                        // in v1: the RocksDB store is opened under an exclusive `db.lock` (a
                        // second process blocks at open) and the probe verb runs synchronously.
                        // The crash-then-resume case is sequential (a dead process holds no
                        // lock; the resume is the only live writer and journals ONE leg). This
                        // is the SAME concurrency precondition §5.0.1's no-sweep isolation rests
                        // on — Phase 6's long-running app must revisit the whole probe under a
                        // per-probe reservation, not a lone CAS here (which would be false
                        // safety while the balance sampling + no-sweep guard share the exposure).
                        session.out_net_msat = Some(sized.0);
                        self.journal
                            .begin_probe_session(&candidate, &session)
                            .await
                            .map_err(exec_err)?;
                        sized
                    }
                    Ok(None) => {
                        // The post-IN feasibility abort: a LOCAL parameter/fee-environment
                        // error, NOT a redeemability failure (§5.0.5 step 4).
                        let diagnostic = format!(
                            "probe leg OUT infeasible: the delivered {} msat cannot afford any \
                             redeem whose contract clears the lnv2 minimum within the {} msat \
                             leg fee cap (shortfall is parametric, not a redeemability failure)",
                            delivered_in.0, run.leg_fee_cap.0
                        );
                        return self
                            .finish_probe_no_attempt(
                                &run,
                                &diagnostic,
                                probe_cost(Some(&in_rec), None),
                            )
                            .await;
                    }
                    Err(e) => anyhow::bail!(
                        "probe leg OUT sizing failed transiently ({e:?}); re-run `probe` to \
                         resume (session retained)"
                    ),
                }
            }
        };
        let out_fee_cap = probe_out_fee_cap(delivered_in, out_net, run.leg_fee_cap);
        let out_key = move_key(&candidate, &run.source, out_net, out_fee_cap, occurrence);

        // §5.0.4 no-sweep guard on the not-yet-journaled window (trivially true on the
        // fresh path; load-bearing on a sized-but-unjournaled resume): leg OUT may start
        // only while the candidate still holds baseline + delta. Once the out intent is
        // journaled the money path owns it like any other move — no guard before DRIVING.
        if self
            .journal
            .get(&out_key)
            .await
            .map_err(exec_err)?
            .is_none()
        {
            let c_spendable = self.mc.balance(&candidate).await.map_err(|e| {
                anyhow::anyhow!(
                    "probe: reading the candidate balance for the no-sweep check failed \
                     transiently ({e}); re-run `probe` to resume (session retained)"
                )
            })?;
            if !no_sweep_ok(
                c_spendable,
                Msat(session.c_spendable_before_in_msat),
                delivered_in,
            ) {
                let diagnostic = "probe delta consumed before redemption; inconclusive";
                return self
                    .finish_probe_no_attempt(&run, diagnostic, probe_cost(Some(&in_rec), None))
                    .await;
            }
            // Re-check the SOURCE cap on resume too (the fresh preflight's check is stale
            // once a resume can span an inflow): if `from` drifted above the cap between the
            // legs, `do_move(candidate -> from)` would deterministically fail ADR-0018 after
            // leg IN already spent — the same guaranteed inconclusive spend the fresh
            // preflight prevents. Abort umbrella-only BEFORE the doomed return move.
            if let Some(cap) = hard_cap {
                let src_spendable = self.mc.balance(&run.source).await.map_err(|e| {
                    anyhow::anyhow!(
                        "probe: reading the source balance for the resume cap check failed                          transiently ({e}); re-run `probe` to resume (session retained)"
                    )
                })?;
                if src_spendable.0 > cap.0 {
                    let diagnostic =
                        "probe source rose above the per-fed cap between legs; inconclusive";
                    return self
                        .finish_probe_no_attempt(&run, diagnostic, probe_cost(Some(&in_rec), None))
                        .await;
                }
            }
        }

        // Leg OUT — sized exactly, same nonce-derived occurrence.
        let out_outcome = self
            .drive_probe_leg(
                candidate,
                run.source,
                out_net,
                out_fee_cap,
                occurrence,
                actor,
                &session.nonce,
                service_client.as_ref(),
            )
            .await?;
        match out_outcome.status {
            Some(IntentStatus::Done) => {}
            Some(IntentStatus::Failed) => {
                return self
                    .finish_probe_failed_leg(
                        &run,
                        ProbeLeg::Out,
                        &out_key,
                        Some(&in_rec),
                        Some(out_key.clone()),
                    )
                    .await;
            }
            other => anyhow::bail!(
                "probe leg OUT {} did not settle (status {}); transient — re-run `probe` to \
                 resume (session retained)",
                out_key.0,
                intent_status_label_opt(other)
            ),
        }

        // §5.0.5 step 6 — both legs settled: ONE atomic outcome write (attempt appended,
        // session cleared, umbrella row Succeeded with the S-net-outflow cost).
        // Fail closed on a missing out record (as leg IN does): `Done` proves leg OUT
        // settled, but a cache-loss recovery could leave `get_move` empty, and recording
        // `cost = full debit` (credit 0) would persist a successful probe in history as if
        // NONE of the funds came back. Deferring (session retained) lets a re-run rebuild
        // the record and record the true S-net-outflow cost.
        let out_rec = self
            .journal
            .get_move(&out_key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "probe leg OUT settled but its move record is missing; transient — re-run \
                 `probe` to resume (session retained)"
                )
            })?;
        let cost = probe_cost(Some(&in_rec), Some(&out_rec));
        let attempt = ProbeAttempt {
            at_ms: run.started_at_ms,
            ok: true,
            from: run.source,
            amount_msat: run.amount.0,
            leg_fee_cap_msat: run.leg_fee_cap.0,
            error: None,
        };
        let committed = self
            .journal
            .record_probe_outcome(
                &candidate,
                &run.nonce,
                Some(attempt.clone()),
                &run.umbrella_key,
                probe_kind(&run, cost),
                actor,
                OperationStatus::Succeeded,
                None,
            )
            .await
            .map_err(exec_err)?;
        Self::note_probe_commit(committed, &run.nonce);
        let after = self.probe_attempts(&candidate).await?;
        Ok(ProbeReport {
            source: run.source,
            verdict_before: run.verdict_before,
            outcome: ProbeOutcome::Attempt(attempt),
            verdict_after: probe_verdict(&after, run.source, now_ms(), &run.effective_policy),
            cost_msat: cost,
            in_key: run.in_key,
            out_key: Some(out_key),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn drive_probe_leg(
        &self,
        from: FederationId,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        occurrence: Occurrence,
        actor: Actor,
        session_nonce: &str,
        service_client: Option<&crate::service::WalletClient>,
    ) -> anyhow::Result<MoveOutcome> {
        let Some(client) = service_client else {
            return self
                .do_move(
                    from,
                    to,
                    amount,
                    fee_cap,
                    occurrence,
                    ReasonCode::ActiveProbe,
                    actor,
                )
                .await;
        };

        let key = move_key(&from, &to, amount, fee_cap, occurrence);
        let attached = self.journal.get(&key).await.map_err(exec_err)?.is_some();
        let balances = if attached || !self.mc.has_client(&from) || !self.mc.has_client(&to) {
            BTreeMap::new()
        } else {
            BTreeMap::from([
                (
                    from,
                    self.mc
                        .balance(&from)
                        .await
                        .map_err(|error| exec_err(ExecError::Retryable(error.to_string())))?,
                ),
                (
                    to,
                    self.mc
                        .balance(&to)
                        .await
                        .map_err(|error| exec_err(ExecError::Retryable(error.to_string())))?,
                ),
            ])
        };
        let decision = AllocatorDecision {
            action: Action::Move {
                from,
                to,
                amount,
                fee_cap,
                gateway: None,
            },
            reason: ReasonCode::ActiveProbe,
            occurrence,
            idempotency_key: key.clone(),
        };
        let decided = client
            .decide_op(crate::service::OpRequest {
                decision,
                actor,
                now_ms: now_ms(),
                balances,
                probe_session_nonce: Some(session_nonce.to_owned()),
                // Probe legs are internal and self-heal via reconcile; never fail-fast on
                // destination openness (the dest-side 503 gate is for FRESH user admissions).
                dest_unavailable: None,
            })
            .await
            .map_err(|error| anyhow::anyhow!("probe leg admission failed: {error}"))?;
        if !matches!(decided.status, IntentStatus::Done | IntentStatus::Failed) {
            let deadline = tokio::time::Instant::now()
                + self
                    .perform_timeout
                    .unwrap_or_else(|| Duration::from_secs(24 * 60 * 60));
            client
                .resolve_await(key.clone(), wallet_api::AwaitTarget::Terminal, deadline)
                .await
                .map_err(|error| anyhow::anyhow!("probe leg wait failed: {error}"))?;
        }
        let status = self
            .journal
            .get(&key)
            .await
            .map_err(exec_err)?
            .map(|intent| intent.status);
        let outcome = self
            .journal
            .get_move(&key)
            .await
            .map_err(exec_err)?
            .and_then(|record| record.outcome);
        Ok(MoveOutcome {
            key,
            status,
            outcome,
        })
    }

    /// The §5.0.5 step-1 preflight for a fresh (or pre-leg-IN resumed) probe. `Err`
    /// carries the LOCAL / no-shared-route diagnostic that terminalizes the umbrella row
    /// with NO attempt (neither demotes — §5.0.3's scoping rule).
    async fn probe_preflight(
        &self,
        session: &ProbeSession,
        candidate: FederationId,
        hard_cap: Option<Msat>,
    ) -> Result<(), String> {
        let open = self.mc.federations();
        if !open.contains(&candidate) {
            return Err(format!(
                "candidate federation {} is not joined/open",
                candidate.to_hex()
            ));
        }
        if !open.contains(&session.from) {
            return Err(format!(
                "source federation {} is not joined/open",
                session.from.to_hex()
            ));
        }
        let source_spendable = self
            .mc
            .balance(&session.from)
            .await
            .map_err(|e| format!("reading the source balance failed: {e}"))?;
        let candidate_spendable = self
            .mc
            .balance(&candidate)
            .await
            .map_err(|e| format!("reading the candidate balance failed: {e}"))?;
        probe_local_faults(
            candidate,
            session.from,
            source_spendable,
            candidate_spendable,
            Msat(session.amount_msat),
            Msat(session.leg_fee_cap_msat),
            hard_cap,
        )?;
        // The existing move-route preflight in BOTH directions (§15.6): leg IN proves
        // S -> C and leg OUT must be known routable before money lands on C. The
        // verbatim route error is the umbrella diagnostic — pair reachability, never
        // candidate honesty.
        self.validate_executor_move_route(SendRouteKind::Move, session.from, candidate)
            .await
            .map_err(|problem| problem.error)?;
        self.validate_executor_move_route(SendRouteKind::Move, candidate, session.from)
            .await
            .map_err(|problem| problem.error)
    }

    /// Terminalize a probe with NO attempt (§5.0.5's local/route/inconclusive exits):
    /// session cleared + umbrella row `Failed` in one dbtx, verdict history untouched.
    /// Note a probe finalizer that lost to a stale-nonce guard in `record_probe_outcome`
    /// (its `false` return). Under single-writer v1 (exclusive `db.lock` + the synchronous
    /// verb) two finalizers for one session cannot race, so `committed` is always true; the
    /// `debug_assert` pins that invariant for tests/dev, and the release warn flags the
    /// Phase-6 concurrency case (where the returned report could disagree with history)
    /// instead of silently discarding the signal.
    fn note_probe_commit(committed: bool, nonce: &str) {
        if !committed {
            tracing::warn!(
                nonce,
                "probe: stale finalizer — durable history holds a different outcome for this \
                 session; the returned report may not match it (a concurrency case \
                 single-writer v1 forecloses; Phase-6 revisit)"
            );
        }
        debug_assert!(
            committed,
            "stale probe finalizer for {nonce} (unreachable under single-writer v1)"
        );
    }

    async fn finish_probe_no_attempt(
        &self,
        run: &ProbeRun,
        diagnostic: &str,
        cost: Option<Msat>,
    ) -> anyhow::Result<ProbeReport> {
        let committed = self
            .journal
            .record_probe_outcome(
                &run.candidate,
                &run.nonce,
                None,
                &run.umbrella_key,
                probe_kind(run, cost),
                run.actor,
                OperationStatus::Failed,
                Some(diagnostic),
            )
            .await
            .map_err(exec_err)?;
        Self::note_probe_commit(committed, &run.nonce);
        // No attempt was recorded, so the trust verdict is unchanged from the run's start.
        Ok(ProbeReport {
            source: run.source,
            verdict_before: run.verdict_before,
            outcome: ProbeOutcome::NoAttempt(diagnostic.to_string()),
            verdict_after: run.verdict_before,
            cost_msat: cost,
            in_key: run.in_key.clone(),
            out_key: None,
        })
    }

    /// Terminalize a probe whose leg FAILED (§5.0.3's fault attribution): a
    /// candidate-attributable failure records a DEMOTING attempt and returns the report;
    /// source/gateway/ambiguous/local faults record an umbrella-only outcome (no attempt,
    /// no demotion) and surface as an error.
    async fn finish_probe_failed_leg(
        &self,
        run: &ProbeRun,
        leg: ProbeLeg,
        leg_key: &IdempotencyKey,
        in_rec: Option<&MoveRecord>,
        out_key: Option<IdempotencyKey>,
    ) -> anyhow::Result<ProbeReport> {
        let (leg_rec, diagnostic) = self.leg_failure_details(leg_key).await.map_err(|e| {
            anyhow::anyhow!(
                "probe leg {} {} failed, but reading its diagnostic failed ({e:?}); \
                 re-run `probe` to resume (session retained)",
                leg.label(),
                leg_key.0
            )
        })?;
        let error_text = format!("probe leg {} failed: {diagnostic}", leg.label());
        let cost = match leg {
            ProbeLeg::In => probe_cost(leg_rec.as_ref(), None),
            ProbeLeg::Out => probe_cost(in_rec, leg_rec.as_ref()),
        };
        match classify_leg_failure(leg, leg_rec.as_ref(), &diagnostic) {
            LegFault::Candidate => {
                let attempt = ProbeAttempt {
                    at_ms: run.started_at_ms,
                    ok: false,
                    from: run.source,
                    amount_msat: run.amount.0,
                    leg_fee_cap_msat: run.leg_fee_cap.0,
                    error: Some(error_text.clone()),
                };
                let committed = self
                    .journal
                    .record_probe_outcome(
                        &run.candidate,
                        &run.nonce,
                        Some(attempt.clone()),
                        &run.umbrella_key,
                        probe_kind(run, cost),
                        run.actor,
                        OperationStatus::Failed,
                        Some(&error_text),
                    )
                    .await
                    .map_err(exec_err)?;
                Self::note_probe_commit(committed, &run.nonce);
                let after = self.probe_attempts(&run.candidate).await?;
                Ok(ProbeReport {
                    source: run.source,
                    verdict_before: run.verdict_before,
                    outcome: ProbeOutcome::Attempt(attempt),
                    verdict_after: probe_verdict(
                        &after,
                        run.source,
                        now_ms(),
                        &run.effective_policy,
                    ),
                    cost_msat: cost,
                    in_key: run.in_key.clone(),
                    out_key,
                })
            }
            LegFault::UmbrellaOnly => {
                // Preserve the failed out leg's handle on the report (finish_probe_no_attempt
                // defaults it None for the pre-leg-OUT refusals): when leg OUT itself failed,
                // its move exists and `out_key` is the operator's direct handle to inspect it.
                let mut report = self.finish_probe_no_attempt(run, &error_text, cost).await?;
                report.out_key = out_key;
                Ok(report)
            }
        }
    }

    /// A failed leg's `(move record, diagnostic)`: the record's terminal `outcome` first,
    /// else the ledger row's `error` (the §8.3 threaded executor diagnostic — several
    /// permanent failures never reach a terminal `MoveRecord.outcome`).
    async fn leg_failure_details(
        &self,
        key: &IdempotencyKey,
    ) -> Result<(Option<MoveRecord>, String), ExecError> {
        let rec = self.journal.get_move(key).await?;
        if let Some(outcome) = rec.as_ref().and_then(|r| r.outcome.clone()) {
            return Ok((rec, outcome));
        }
        let ledger_error = self
            .journal
            .operation(&OperationRef::Key(key.clone()))
            .await?
            .and_then(|row| row.error);
        Ok((
            rec,
            ledger_error.unwrap_or_else(|| "move failed with no recorded diagnostic".to_string()),
        ))
    }

    /// The fed's retained probe attempts (empty when never probed).
    async fn probe_attempts(&self, fed: &FederationId) -> anyhow::Result<Vec<ProbeAttempt>> {
        Ok(self
            .journal
            .probe_record(fed)
            .await
            .map_err(exec_err)?
            .map(|r| r.attempts)
            .unwrap_or_default())
    }

    /// Finalize an `Awaiting` `DirectInflow` (spec §9.5): reattach to its `recv_op` (rebuilt
    /// from the op-log so a lost cache still finds it), await the receive leg, and on `Claimed`
    /// mark the intent `Done` via the journal CAS. Blocks until the receive reaches a final
    /// state. Idempotent: an already-`Done` intent returns `Done` without re-awaiting.
    ///
    /// `expected_fed`, when supplied, guards against finalizing the wrong federation's inflow;
    /// the destination is otherwise read authoritatively from the intent's `MoveRecord`.
    pub async fn await_move(
        &self,
        key: &IdempotencyKey,
        expected_fed: Option<FederationId>,
    ) -> anyhow::Result<FinalizeOutcome> {
        let intent = self
            .journal
            .get(key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| anyhow::anyhow!("no intent found for key {}", key.0))?;
        match intent.status {
            IntentStatus::Done => {
                if let Some(fed) = expected_fed {
                    let rec = self.move_record_for_guard(&intent).await?;
                    ensure_expected_fed(key, &rec, fed)?;
                }
                return Ok(FinalizeOutcome::Done);
            }
            IntentStatus::Failed => {
                let rec = if expected_fed.is_some() {
                    Some(self.move_record_for_guard(&intent).await?)
                } else {
                    self.journal.get_move(key).await.map_err(exec_err)?
                };
                if let (Some(fed), Some(rec)) = (expected_fed, rec.as_ref()) {
                    ensure_expected_fed(key, rec, fed)?;
                }
                return Ok(FinalizeOutcome::Failed(
                    rec.and_then(|rec| rec.outcome)
                        .unwrap_or_else(|| format!("intent {} already failed", key.0)),
                ));
            }
            IntentStatus::Awaiting => {}
            other @ (IntentStatus::Pending | IntentStatus::Executing) => anyhow::bail!(
                "intent {} is {other:?}, not awaiting — run `direct-inflow`/`reconcile` first",
                key.0
            ),
        }

        // Rebuild the record from the op-log so we reattach to the existing recv_op even if the
        // MoveRecord cache was lost (spec §9.2), then await the external payer's payment.
        let executor = self.executor();
        let rec = executor
            .backfill_move_record(&intent)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| anyhow::anyhow!("intent {} is not an executable move", key.0))?;
        if let Some(fed) = expected_fed {
            ensure_expected_fed(key, &rec, fed)?;
        }
        let recv_op = rec.recv_op.ok_or_else(|| {
            anyhow::anyhow!("awaiting intent {} has no receive op to finalize", key.0)
        })?;

        let outcome = match self.mc.await_receive(&rec.to, recv_op).await? {
            ReceiveState::Claimed => {
                self.settle_move(&rec, intent.attempt, MovePhase::Settled, None)
                    .await?;
                self.finalize(key, intent.attempt, IntentStatus::Done)
                    .await?;
                FinalizeOutcome::Done
            }
            ReceiveState::Expired => {
                let msg = "receive invoice expired before payment".to_string();
                self.settle_move(&rec, intent.attempt, MovePhase::Failed, Some(msg.clone()))
                    .await?;
                self.finalize(key, intent.attempt, IntentStatus::Failed)
                    .await?;
                FinalizeOutcome::Failed(msg)
            }
            ReceiveState::Failed(msg) => {
                self.settle_move(&rec, intent.attempt, MovePhase::Failed, Some(msg.clone()))
                    .await?;
                self.finalize(key, intent.attempt, IntentStatus::Failed)
                    .await?;
                FinalizeOutcome::Failed(msg)
            }
        };
        Ok(outcome)
    }

    /// The resume loop (spec §9): rebuild `MoveRecord`s from the op-log for pending + awaiting
    /// intents BEFORE re-driving (so a re-drive of an intent that crashed mid-receive reattaches
    /// to its op instead of minting a second invoice), re-drive `pending()` (Pending|Executing)
    /// ONLY via `wallet_core::reconcile`, then report the still-`Awaiting` set — subscription-
    /// owned, finalized out-of-band by `await-move` in this one-shot CLI.
    ///
    /// The clients are assumed already opened by the caller (the CLI runs `open_all` at startup,
    /// satisfying §9.1); `reconcile` operates on the open set.
    pub async fn reconcile(&self) -> anyhow::Result<ReconcileSummary> {
        let executor = self.executor();

        // §9.2: rebuild the derived records for every intent we might re-drive or finalize.
        let mut backfill_set = self.journal.pending().await.map_err(exec_err)?;
        backfill_set.extend(self.journal.awaiting().await.map_err(exec_err)?);
        for intent in &backfill_set {
            if let Err(e) = executor.backfill_move_record(intent).await {
                tracing::warn!(
                    key = %intent.idempotency_key.0,
                    error = ?e,
                    "reconcile: could not rebuild move record; leaving it for a later pass"
                );
            }
        }

        // §9.4: re-drive pending() only; Failed/Permanent stay terminal, Awaiting is skipped.
        // Wrap the drive with the §15.9 per-perform deadline (the backfill above uses the raw
        // executor, since it makes no `perform` call).
        #[cfg(test)]
        let exec = if let Some(executor) = &self.test_executor {
            wallet_core::reconcile(self.journal.as_ref(), executor.as_ref()).await
        } else {
            let driving = self.driving_executor();
            wallet_core::reconcile(self.journal.as_ref(), &driving).await
        };
        #[cfg(not(test))]
        let exec = {
            let driving = self.driving_executor();
            wallet_core::reconcile(self.journal.as_ref(), &driving).await
        };

        // §10.3: repair stuck non-terminal ledger rows (raw pay/recv, join, tick) from op-log +
        // registry evidence. Best-effort — a repair I/O fault must not fail the whole reconcile
        // (the intent re-drive above already committed its own money-path progress).
        match self.journal.repair_ledger(self.mc.as_ref()).await {
            Ok(summary) if summary.repaired > 0 => {
                tracing::info!(
                    repaired = summary.repaired,
                    "reconcile: repaired stuck ledger rows"
                )
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(
                error = ?e,
                "reconcile: ledger repair pass failed; leaving rows for a later pass"
            ),
        }

        // §9.3: surface the Awaiting set so the operator drives `await-move` for each.
        let awaiting = self.journal.awaiting().await.map_err(exec_err)?;
        // br-p93: project the logical goals still owned by durable work, from the FINAL scan —
        // after the re-drive above, so work that settled in this pass no longer blocks its own
        // recurrence. A scan fault propagates: an unknown durable state must fail the whole
        // reconcile (and with it the tick), never degrade to an empty, permissive blocker set.
        let blocked = GoalBlockers::from_intents(&self.journal.pending().await.map_err(exec_err)?);
        Ok(ReconcileSummary {
            performed: exec.performed,
            failed: exec.failed,
            skipped: exec.skipped,
            retryable: exec.retryable,
            awaiting: awaiting.len(),
            awaiting_keys: awaiting
                .into_iter()
                .map(|intent| intent.idempotency_key)
                .collect(),
            blocked,
        })
    }

    /// ONE orchestrator tick (Phase 2 step 2.2, `docs/archive/phase2-plan.md`): probe every open
    /// federation → build the `AllocatorSnapshot` (via `build_snapshot` — `score()` +
    /// designation) → [`wallet_core::decide_with_blockers`] →
    /// [`wallet_core::apply_with_allocator_admission`] the decisions through the
    /// [`FedimintExecutor`], which performs the resulting `Move`s AND `Evacuate`s (each a
    /// send-required move, synchronous to `Done`). Advisory `RefuseInflow` decisions are
    /// surfaced in the returned [`TickReport`] but never executed (`apply` skips them via
    /// `Action::is_executable`). As of Phase 3.A an `Evacuate` is executed like a `Move`
    /// (draining a dying fed into `safest_other`), no longer withheld from `apply`.
    /// Returns the FULL decision list + the [`ExecutionSummary`].
    ///
    /// The scorer runs at [`ScorerPolicy::default`] (the v1 structural floor); the money policy
    /// (caps/targets/fees + designation) comes from `policy`. A `Move` needs a routable shared
    /// gateway — supply it as this runtime's pinned gateway (devimint does not auto-register its
    /// LDK gateway; §4), exactly as `do_move` does. The probe route gate validates that same
    /// pinned gateway when present, so decisions match the route the executor will use.
    pub async fn tick(&self, policy: &TickPolicy) -> anyhow::Result<TickReport> {
        // A standalone MAX occurrence has no possible strictly newer daemon
        // successor. Refuse before touching the watch checkpoint, tick ledger, or
        // an intent so its CLI error is actionable and leaves no poisoned state.
        crate::journal::ensure_occurrence_has_successor(policy.occurrence.0).map_err(exec_err)?;
        self.tick_after_occurrence_authority(policy).await
    }

    /// Execute work for an occurrence allocated by the daemon scheduler.
    ///
    /// `advance_watch_occurrence` may allocate `u64::MAX` exactly once from a
    /// `u64::MAX - 1` checkpoint. That final scheduler occurrence is valid work;
    /// only a following scheduler cycle is exhausted. Standalone `tick` remains
    /// stricter because it persists an occurrence floor that needs a successor.
    async fn tick_for_daemon_scheduler(&self, policy: &TickPolicy) -> anyhow::Result<TickReport> {
        self.tick_after_occurrence_authority(policy).await
    }

    /// Execute after the caller has applied its occurrence-authority boundary.
    ///
    /// Both standalone and daemon-scheduler callers use the same planning,
    /// replacement, and execution behavior. Their sole distinction is whether
    /// this occurrence must itself have a successor.
    async fn tick_after_occurrence_authority(
        &self,
        policy: &TickPolicy,
    ) -> anyhow::Result<TickReport> {
        // A standalone `--occurrence` is also an allocator-generation boundary. Persist its floor
        // under the journal transaction before planning so a subsequently restarted daemon advances
        // beyond it rather than proposing a replacement child at or below its marked parent.
        //
        // The daemon has already atomically allocated and persisted this occurrence. Re-observing
        // its final `u64::MAX` value would incorrectly apply the standalone successor requirement.
        if policy.occurrence.0 != u64::MAX {
            self.journal
                .observe_watch_occurrence(policy.occurrence.0)
                .await
                .map_err(exec_err)?;
        }
        // §10.4: open a `Started` tick row BEFORE probing (a per-attempt `tick:` key, §10.1), so
        // a crash mid-tick leaves a durable row that reconcile repairs after 1h. Ledger recording
        // is auxiliary to the money op, so a storage fault here is logged, never fatal.
        let tick_key = IdempotencyKey(format!("tick:{}:{}", policy.occurrence.0, ledger_nonce()));
        if let Err(e) = self
            .journal
            .record_tick_started(&tick_key, policy.occurrence, now_ms())
            .await
        {
            tracing::warn!(error = ?e, "tick: recording the Started tick row failed");
        }

        // `plan_tick` scans the candidate registry (`auto_joined_candidates`) before a plan
        // exists, so a storage fault there can error out AFTER the `Started` row was written.
        // Terminalize the tick `Failed` on that path too, or `history/show` leaves it in-flight
        // until reconcile repairs it an hour later (§10.4), same as the bail paths below.
        let mut plan = match self.plan_tick(policy).await {
            Ok(plan) => plan,
            Err(e) => {
                // A corrupt strict reservation view makes fresh admission and replacement
                // unknowable.  Do not run a bespoke executor bypass: the exact marked parent
                // remains durable and parked until storage is repaired.
                self.record_tick_failed(&tick_key, &e.to_string()).await;
                return Err(e);
            }
        };
        // A valid planner yields exactly one authoritative marker outcome. This guard comes before
        // any marker clear, replacement exchange, child admission, deferred audit, or executor
        // action. The tick itself is already an auditable `Started` row, so terminalize that row
        // rather than leaving an impossible plan in-flight; both parents remain exact repair
        // evidence for a later valid round.
        if plan.replacement.is_some() && plan.marker_disposition.is_some() {
            let error =
                "tick: replacement and marker-clear disposition cannot share a standalone round";
            self.record_tick_failed(&tick_key, error).await;
            anyhow::bail!("{error}");
        }
        // The final conflict check before anything is journaled (br-p93). `plan_tick` derived the
        // same conflict model from durable state before route pricing, so this normally removes
        // nothing; re-scan here so work admitted while planning was doing network I/O still blocks
        // a duplicate. A scan fault makes eligibility unknown and therefore fails the whole tick
        // closed. Advisory decisions carry no goal and are never touched.
        let actor = Actor::Agent {
            occurrence: policy.occurrence,
        };
        let blocked = match self.allocator_goal_blockers().await {
            Ok(blocked) => blocked,
            Err(error) => {
                self.record_tick_failed(&tick_key, &error.to_string()).await;
                return Err(error);
            }
        };
        let mut newly_suppressed = Vec::new();
        plan.decisions.retain(|decision| {
            let conflicts = blocked.blocks_decision(decision, actor);
            // Planning already logged (and skipped the route I/O for) everything it withheld, so
            // anything caught HERE was admitted while this tick was doing network I/O. Say so:
            // the daemon's equivalent seam records a `Conflict` refusal, and a race that silently
            // shrinks a planned batch is the one an operator has no other trace of.
            if conflicts {
                newly_suppressed.push(decision.clone());
                tracing::warn!(
                    key = %decision.idempotency_key.0,
                    "tick: withholding a decision that began conflicting with allocator work while planning"
                );
            }
            !conflicts
        });
        // The final re-scan can race work admitted while planning was doing route I/O. Preserve
        // every resulting nonzero executable drop as the actor does at commit time: this is
        // auxiliary observability, so a journal fault is loud but must not turn the tick's
        // otherwise-safe conflict suppression into a failed money operation.
        for decision in &newly_suppressed {
            let message = format!(
                "decision {} conflicts with allocator work already in flight",
                decision.idempotency_key.0
            );
            if let Err(error) = self
                .journal
                .record_tick_dropped_refusal(decision, policy.occurrence, now_ms(), &message, true)
                .await
            {
                tracing::warn!(
                    key = %decision.idempotency_key.0,
                    ?error,
                    "tick: recording a conflict-suppressed decision failed"
                );
            }
        }
        plan.suppressed.extend(newly_suppressed);
        // A tick is a money op: an operator-pinned fed that could not be sensed or failed the
        // lnv2/probe gate this pass means the requested rebalance was NOT evaluated. Fail LOUDLY
        // (non-zero exit) rather than let `decide` degrade it to an advisory `RefuseInflow` that
        // `apply` skips, which would report a false success to a scheduler gating on the exit code.
        // Both bail paths land a `Failed` tick row WITH the diagnostic before returning (§10.4).
        // The check receives admitted and suppressed work separately. The fresh scan above moves a
        // race-lost decision into `suppressed` before this authoritative pin check.
        let pin_blockers = plan
            .replacement
            .as_ref()
            .map(|replacement| blocked.excluding_key(&replacement.old_key))
            .unwrap_or_else(|| blocked.clone());
        let problems = Self::pinned_input_problems(policy, &plan, &pin_blockers);
        if !problems.is_empty() {
            let error = format!("tick: {}", problems.join("; "));
            self.record_tick_failed(&tick_key, &error).await;
            anyhow::bail!("{error}");
        }
        let admitted_decisions = planned_tick_decisions(&plan);
        if let Err(e) = self
            .ensure_fresh_tick_decisions(&admitted_decisions, policy.occurrence)
            .await
        {
            self.record_tick_failed(&tick_key, &e.to_string()).await;
            return Err(e);
        }
        let balances = plan
            .snapshot
            .federations
            .iter()
            .map(|fed| (fed.id, fed.balance.spendable))
            .collect::<BTreeMap<_, _>>();
        for deferred in plan
            .replacement_deferred
            .iter()
            .filter(|decision| decision.action.is_executable())
        {
            if let Err(error) = self
                .journal
                .record_tick_dropped_refusal(
                    deferred,
                    policy.occurrence,
                    now_ms(),
                    "deferred: replacement-exclusive one-child round",
                    false,
                )
                .await
            {
                tracing::warn!(
                    key = %deferred.idempotency_key.0,
                    ?error,
                    "tick: recording replacement-exclusive deferred audit failed"
                );
            }
        }
        if let Err(error) = self
            .journal
            .record_refusals_with_note(
                &plan.replacement_deferred,
                policy.occurrence,
                now_ms(),
                Some("deferred: replacement-exclusive one-child round"),
            )
            .await
        {
            tracing::warn!(
                error = ?error,
                "tick: recording replacement-exclusive deferred advisory audit failed"
            );
        }
        let marker_clear_error = if let Some(disposition) = plan.marker_disposition.as_ref() {
            match self
                .journal
                .clear_marked_evacuation_if_pending(&disposition.parent)
                .await
            {
                Ok(true) => {
                    // No immediate driver/policy wake: this cycle merely returns the old
                    // evacuation to ordinary Pending retry for a subsequent normal tick.
                    None
                }
                Ok(false) => {
                    tracing::warn!(
                        key = %disposition.parent.idempotency_key.0,
                        "tick: marker-clear disposition no longer owned its exact Pending evacuation"
                    );
                    None
                }
                Err(error) => Some(error),
            }
        } else {
            None
        };
        if let Some(error) = marker_clear_error {
            if plan.decisions.is_empty() {
                self.record_tick_failed(&tick_key, &format!("{error:?}"))
                    .await;
                return Err(exec_err(error));
            }
            // This exact marker remains durable/planner-owned, but a clear fault must not suppress
            // independently replanned ordinary work. The actor follows the same split.
            tracing::warn!(
                ?error,
                "tick: retaining marker after clear fault while continuing independent decisions"
            );
        }
        let (to_apply, reservations) = match plan.replacement.as_ref() {
            Some(replacement) => match self
                .replace_marked_evacuation_standalone(replacement, policy, &balances, &blocked)
                .await
            {
                Ok(reservations) => (
                    decisions_to_apply(std::slice::from_ref(&replacement.fresh)),
                    reservations,
                ),
                Err(error) => {
                    self.record_tick_failed(&tick_key, &error.to_string()).await;
                    return Err(error);
                }
            },
            None => (
                decisions_to_apply(&plan.decisions),
                plan.snapshot.reservations.clone(),
            ),
        };
        #[cfg(test)]
        let summary = match &self.test_executor {
            Some(executor) => {
                self.apply_tick_decisions(
                    executor.as_ref(),
                    &to_apply,
                    actor,
                    &balances,
                    policy,
                    reservations.clone(),
                )
                .await
            }
            None => {
                self.apply_tick_decisions(
                    &self.driving_executor(),
                    &to_apply,
                    actor,
                    &balances,
                    policy,
                    reservations,
                )
                .await
            }
        };
        #[cfg(not(test))]
        let summary = self
            .apply_tick_decisions(
                &self.driving_executor(),
                &to_apply,
                actor,
                &balances,
                policy,
                reservations,
            )
            .await;

        // §10.4: one `Refusal` row per advisory decision, then terminalize the tick with its
        // decision/apply counts. Both are auxiliary recordings — log a fault, never fail the tick.
        if let Err(e) = self
            .journal
            .record_refusals(&plan.decisions, policy.occurrence, now_ms())
            .await
        {
            tracing::warn!(error = ?e, "tick: recording refusal rows failed");
        }
        let counts = Some((
            admitted_decisions.len() as u32,
            summary.performed as u32,
            summary.failed as u32,
        ));
        let (tick_status, tick_error) = tick_terminal(&summary);
        if let Err(e) = self
            .journal
            .record_tick_terminal(
                &tick_key,
                counts,
                tick_status,
                tick_error.as_deref(),
                now_ms(),
            )
            .await
        {
            tracing::warn!(error = ?e, "tick: recording the terminal tick row failed");
        }
        Ok(TickReport {
            decisions: admitted_decisions,
            summary,
            spending_fed: plan.snapshot.spending_fed,
            standby_fed: plan.snapshot.standby_fed,
        })
    }

    /// Terminalize a tick row `Failed` on a bail path (§10.4) with zero counts + its diagnostic.
    /// Best-effort: a recording fault must not mask the bail's own error.
    async fn record_tick_failed(&self, key: &IdempotencyKey, error: &str) {
        if let Err(e) = self
            .journal
            .record_tick_terminal(key, None, OperationStatus::Failed, Some(error), now_ms())
            .await
        {
            tracing::warn!(error = ?e, "tick: recording the failed tick row failed");
        }
    }

    /// A DRY-RUN tick (Phase 2 step 2.2): probe → `score()` → `build_snapshot` → `decide()`, but
    /// DO NOT apply. Returns the per-fed scored view (each fed's `FederationVerdict` +
    /// `FederationStatus`), the designation `build_snapshot` chose, and the decisions that WOULD
    /// run. No money moves — this is `wallet-cli status`.
    ///
    /// Unlike [`Runtime::tick`], `status` does NOT bail on an unsensed / unusable pin, nor on a
    /// terminal-replaying occurrence: its whole job is to SHOW the operator why a tick would
    /// fail, so hard-failing before assembling the scored view would blank out exactly the
    /// diagnostic they ran it for. It surfaces each such problem as a `warn!` (to stderr) and
    /// still returns the full scored view + would-run decisions. The route check reflects the
    /// pinned gateway when one was supplied, same as `tick`.
    ///
    /// The exhausted standalone occurrence is a deliberate hard error: it has no successor. A stale
    /// marked replacement is different. Standalone returns the scored/designation diagnostic, but
    /// omits every would-run decision rather than advertising an impossible child (or ordinary work
    /// deferred by that child's one-child round). The daemon's scheduler authority remains strict.
    /// Both paths stay read-only: status writes neither an exchange nor a child.
    pub async fn status(&self, policy: &TickPolicy) -> anyhow::Result<StatusReport> {
        // Status must not advertise an exhausted standalone generation as
        // would-run work: `tick` rejects it before any durable write because
        // no strictly newer successor can exist. This check is read-only.
        crate::journal::ensure_occurrence_has_successor(policy.occurrence.0).map_err(exec_err)?;
        self.status_after_occurrence_authority(policy, StatusMode::StandaloneDiagnostic)
            .await
    }

    /// Dry-run the next daemon-scheduler occurrence.
    ///
    /// The daemon can allocate `Occurrence(u64::MAX)` exactly once as the final
    /// child of a `u64::MAX - 1` watch floor. It must describe that valid final
    /// work with the same dry-run behavior as [`Runtime::status`], rather than
    /// applying the standalone successor requirement. Marked replacement
    /// children remain strictly newer than their Agent parent.
    pub async fn status_for_daemon_scheduler(
        &self,
        policy: &TickPolicy,
    ) -> anyhow::Result<StatusReport> {
        self.status_after_occurrence_authority(policy, StatusMode::DaemonStrict)
            .await
    }

    /// Build a dry-run after the caller has applied its occurrence-authority boundary.
    async fn status_after_occurrence_authority(
        &self,
        policy: &TickPolicy,
        mode: StatusMode,
    ) -> anyhow::Result<StatusReport> {
        let scorer_policy = ScorerPolicy::default();
        let plan = self.plan_tick(policy).await?;
        let mut status_decisions = planned_tick_decisions(&plan);
        // A stale structural replacement cannot be committed. The direct standalone command is an
        // operator diagnostic, so retain the full scored/designation report but show no would-run
        // work at all: its child is impossible and its ordinary siblings were deferred by the same
        // one-child round. Daemon status instead remains an allocation-authority fence.
        if let Some(replacement) = plan.replacement.as_ref() {
            if let Actor::Agent {
                occurrence: old_occurrence,
            } = replacement.parent.actor
            {
                if replacement.fresh.occurrence <= old_occurrence
                    || replacement.fresh.idempotency_key == replacement.old_key
                {
                    let error = crate::service::replacement_occurrence_error(
                        old_occurrence,
                        replacement.fresh.occurrence,
                    );
                    match mode {
                        StatusMode::StandaloneDiagnostic => {
                            let warning = stale_standalone_replacement_status_warning(&error);
                            tracing::warn!("status: {warning}");
                            status_decisions.clear();
                        }
                        StatusMode::DaemonStrict => anyhow::bail!(error),
                    }
                }
            }
        }
        // Surface (do NOT bail on) any pinned-input problem the equivalent `tick` would fail on, so
        // the operator sees BOTH the warning and the full scored view that explains it.
        for problem in Self::pinned_input_problems(policy, &plan, &plan.blockers) {
            tracing::warn!("status: {problem}");
        }
        match self
            .terminal_replayed_executable_decisions(&status_decisions)
            .await
        {
            Ok(replays) if !replays.is_empty() => tracing::warn!(
                "status: occurrence {} would replay already-terminal/subscription-owned decision(s) {}; \
                 tick will fail until --occurrence is advanced",
                policy.occurrence.0,
                describe_terminal_replays(&replays)
            ),
            Err(e) => tracing::warn!(
                "status: could not check whether this occurrence replays terminal decisions: {e}"
            ),
            _ => {}
        }
        // §5.0.6: the ACTIVE-probe verdict is SOURCE-RELATIVE — evaluated against the
        // snapshot's designated SPENDING fed (the fed that would fund the candidate),
        // always with the DEFAULT policy (the CLI's shrink-only overrides never reach the
        // production surface). Filled onto the facts (the field 5.1's gate reads) and the
        // scored row (the `status` display); the scorer itself ignores it in 5.0.
        let spending = plan.snapshot.spending_fed;
        let mut scored = Vec::with_capacity(plan.raw_probes.len());
        for (id, probe) in &plan.raw_probes {
            let active_probe = match spending {
                // The designated spending fed cannot probe ITSELF (a probe is a candidate
                // pair): leave its own row's verdict `None`/`-` rather than reporting a
                // bogus self-probe `never`/stale state on one of status's key rows.
                Some(source) if source == *id => None,
                Some(_) => plan.active_probes.get(id).copied(),
                None => None,
            };
            let mut facts = assemble_facts(probe, *id);
            facts.active_probe = active_probe;
            // The POST-GATE fundability the tick actually applies (§5.1.3), read from the exact
            // snapshot the planner decided on — NOT re-derived from `score()`, which ignores the
            // active probe. `build_snapshot` maps 1:1 over the probes, so the fed is always
            // present; the `is_some_and` default is a defensive fail-closed, not a real branch.
            let gated_eligible = plan
                .snapshot
                .federations
                .iter()
                .find(|f| f.id == *id)
                .is_some_and(|f| f.eligible_to_fund);
            scored.push(ScoredFed {
                id: *id,
                verdict: score(&facts, &scorer_policy),
                status: assemble_status(probe, *id),
                active_probe,
                gated_eligible,
            });
        }
        for goal in &plan.deferred {
            tracing::warn!(
                dest = %goal.dest.to_hex(),
                reason = ?goal.reason,
                want_msat = goal.want.0,
                floor_msat = goal.floor.0,
                floor_source = ?goal.floor_source,
                "status: a funding goal is deferred below the move floor; it will not be funded \
                 until the shortfall grows past the floor, the route gets cheaper, or the \
                 proportional cap is raised"
            );
        }
        Ok(StatusReport {
            scored,
            spending_fed: plan.snapshot.spending_fed,
            standby_fed: plan.snapshot.standby_fed,
            decisions: status_decisions,
            deferred: plan.deferred,
        })
    }

    /// Return the pin diagnostics for this planned round. The replacement child and ordinary work
    /// deferred solely by its one-child exclusivity were both route-planned, so both participate in
    /// validation. Conflict-suppressed work remains separate because it has only the narrower
    /// holder-associated voucher semantics.
    fn pinned_input_problems(
        policy: &TickPolicy,
        plan: &TickPlan,
        blockers: &GoalBlockers,
    ) -> Vec<String> {
        let mut validation_decisions = planned_tick_decisions(plan);
        validation_decisions.extend(plan.replacement_deferred.clone());
        crate::tick::pinned_input_problems(
            policy,
            &plan.snapshot,
            &plan.probes,
            &validation_decisions,
            &plan.suppressed,
            blockers,
        )
    }

    /// Probe, then use the actor-owned planner — the same implementation the daemon runs through
    /// `DecideTickRound`.
    ///
    /// Pinned-input problems are NOT raised here: `tick` bails on them and `status` reports them,
    /// so the decision belongs to the caller.
    async fn plan_tick(&self, policy: &TickPolicy) -> anyhow::Result<TickPlan> {
        #[cfg(test)]
        if let Some(plan) = self
            .test_tick_plan
            .lock()
            .expect("tick test-plan mutex poisoned")
            .clone()
        {
            return Ok(plan);
        }
        self.plan_tick_from_probes(policy, self.probe_all().await)
            .await
    }

    /// Plan standalone tick/status work from already collected probes. Unlike the daemon's
    /// reconcile-carrying policy, standalone callers have no preceding blocker report, so this
    /// helper derives the durable conflict projection before route pricing and allocation.
    async fn plan_tick_from_probes(
        &self,
        policy: &TickPolicy,
        raw_probes: Vec<(FederationId, ProbeResult)>,
    ) -> anyhow::Result<TickPlan> {
        // A direct standalone `tick`/`status` has no preceding reconcile report. Derive its
        // conflict projection here rather than trusting `TickPolicy::default()`'s empty set, so
        // every Runtime entry point drops blocked funding pairs before route quotes and blocked
        // decisions before concrete preflight. An unreadable scan fails closed.
        let mut policy = policy.clone();
        policy.blocked = self.allocator_goal_blockers().await?;
        let round = crate::service::plan_tick_round(
            self.journal.as_ref(),
            Some(self),
            raw_probes.clone(),
            &policy,
            now_ms(),
            Some(RouteQuoteBudget::starting_at(now_ms())),
        )
        .await
        .map_err(exec_err)?;
        Ok(TickPlan {
            raw_probes,
            suppressed: round.suppressed,
            replacement_deferred: round.replacement_deferred,
            deferred: round.deferred,
            probes: round.probes,
            active_probes: round.active_probes,
            snapshot: round.snapshot,
            decisions: round.decisions,
            blockers: round.blocked,
            replacement: round.replacement,
            marker_disposition: round.marker_disposition,
        })
    }

    /// The standalone tick's money seam. The admission arguments — the tick's own fresh balances
    /// and the per-fed cap — are written ONCE here, so the `#[cfg(test)]` executor swap in `tick`
    /// chooses only WHICH executor drives the batch and can never drift into admitting the batch
    /// on terms production does not use.
    async fn apply_tick_decisions<E: Executor>(
        &self,
        executor: &E,
        decisions: &[AllocatorDecision],
        actor: Actor,
        balances: &BTreeMap<FederationId, Msat>,
        policy: &TickPolicy,
        reservations: Reservations,
    ) -> ExecutionSummary {
        wallet_core::apply_with_allocator_admission(
            self.journal.as_ref(),
            executor,
            decisions,
            actor,
            now_ms(),
            Some(balances),
            Some(policy.per_fed_cap),
            reservations,
        )
        .await
    }

    /// The exclusive standalone counterpart of the actor's replacement commit.  A standalone
    /// `tick` owns the database for its whole documented invocation, so it has no actor-generation
    /// token to check; it still repeats every durable/fresh admission check before exchanging the
    /// parent and only then hands the already-created child to the ordinary executor path. Every
    /// replacement-path error retains the exact Pending marker: only the successful (or exactly
    /// confirmed committed) exchange may consume it. The planner's separate no-child disposition is
    /// the sole non-exchange marker-clear path.
    async fn replace_marked_evacuation_standalone(
        &self,
        replacement: &crate::service::EvacuationReplacementPlan,
        policy: &TickPolicy,
        balances: &BTreeMap<FederationId, Msat>,
        blockers: &GoalBlockers,
    ) -> anyhow::Result<Reservations> {
        let current_cap = wallet_core::EvacFeeCap {
            base_msat: policy.evac_fee_base_msat,
            bps: policy.evac_fee_bps,
        };
        let Action::Evacuate {
            from,
            to,
            amount,
            fee_cap,
            fee_cap_components: Some(components),
            ..
        } = &replacement.fresh.action
        else {
            anyhow::bail!("standalone replacement child is not a component-capped evacuation");
        };
        if *components != current_cap || current_cap.at(*amount) != *fee_cap {
            anyhow::bail!(
                "standalone replacement child no longer exactly matches the current evacuation fee cap"
            );
        }
        if !balances.contains_key(from) || !balances.contains_key(to) {
            anyhow::bail!(
                "standalone replacement requires fresh balances for both endpoints {} -> {}",
                from.to_hex(),
                to.to_hex()
            );
        }
        let old_read = self.journal.get(&replacement.old_key).await;
        let old = match old_read {
            Err(error) => {
                // No exchange was entered. Retain the exact structural marker as durable repair
                // evidence; clearing a marker on an unreadable parent would turn a storage fault
                // into permission to replan ordinary work.
                anyhow::bail!(
                    "standalone replacement could not read parent before exchange: {error:?}"
                )
            }
            Ok(Some(old)) => old,
            Ok(None) => anyhow::bail!("standalone replacement parent disappeared before exchange"),
        };
        if let Actor::Agent {
            occurrence: old_occurrence,
        } = old.actor
        {
            if replacement.fresh.occurrence <= old_occurrence
                || replacement.fresh.idempotency_key == replacement.old_key
            {
                // This is an operator-correctable stale occurrence, not a failed exchange.
                // Keep the typed marker so a rerun with a greater occurrence can replace the
                // same parent directly; clearing it would discard its structural evidence.
                anyhow::bail!(crate::service::replacement_occurrence_error(
                    old_occurrence,
                    replacement.fresh.occurrence,
                ));
            }
        }
        if old != replacement.parent
            || old.attempt != replacement.old_attempt
            || old.status != IntentStatus::Pending
            || old.evacuation_refusal.as_ref() != Some(&replacement.evidence)
            || !matches!(old.actor, Actor::Agent { .. })
            || !matches!(&old.action, Action::Evacuate { from: old_from, .. } if old_from == from)
        {
            anyhow::bail!("standalone replacement parent is no longer exclusively pending");
        }
        if !wallet_core::evacuation_cap_qualifies_replacement(&replacement.evidence, current_cap) {
            anyhow::bail!("standalone replacement fee-cap evidence no longer qualifies");
        }
        let fresh_blockers = match self.allocator_goal_blockers().await {
            Ok(blockers) => blockers.excluding_key(&replacement.old_key),
            Err(error) => {
                // A projection read fault is not an authoritative no-child disposition.
                anyhow::bail!(
                    "standalone replacement could not project blockers before exchange: {error}"
                )
            }
        };
        if fresh_blockers.blocks_decision(
            &replacement.fresh,
            Actor::Agent {
                occurrence: replacement.fresh.occurrence,
            },
        ) || blockers
            .excluding_key(&replacement.old_key)
            .blocks_decision(
                &replacement.fresh,
                Actor::Agent {
                    occurrence: replacement.fresh.occurrence,
                },
            )
        {
            anyhow::bail!(
                "standalone replacement child conflicts with allocator work already in flight"
            );
        }
        let reservations = match crate::service::project_allocator_reservations_excluding(
            self.journal.as_ref(),
            &replacement.old_key,
        )
        .await
        {
            Ok(reservations) => reservations,
            Err(error) => {
                // Keep the marker. We have not entered the exchange and cannot prove the
                // reservation projection that would make consuming it safe.
                anyhow::bail!(
                    "standalone replacement could not project reservations before exchange: {error:?}"
                )
            }
        };
        // The child identity and the atomic sidecar must have one timestamp.  In particular a
        // post-commit retryable error is confirmed against this exact child; sampling again for
        // the exchange would make a perfectly committed exchange look ambiguous at a clock tick.
        let exchange_now = self.replacement_exchange_now();
        let child = Intent::from_decision(
            &replacement.fresh,
            Actor::Agent {
                occurrence: replacement.fresh.occurrence,
            },
            exchange_now,
        );
        if let Err(error) = wallet_core::admit_intent(
            &child,
            Some(balances),
            Some(policy.per_fed_cap),
            &reservations,
        ) {
            return Err(exec_err(error));
        }
        let exchanged = self
            .journal
            .replace_marked_evacuation(
                &replacement.old_key,
                replacement.old_attempt,
                &replacement.evidence,
                &replacement.fresh,
                exchange_now,
                &replacement.parent,
            )
            .await;
        let mut refusal = None;
        let committed = match exchanged {
            Ok(true) => true,
            Ok(false) => {
                anyhow::bail!("standalone replacement parent was no longer exclusively pending");
            }
            Err(error) => {
                let confirmed = self
                    .confirm_standalone_replacement_exchange(
                        replacement,
                        &replacement.parent,
                        &child,
                    )
                    .await
                    .map_err(exec_err)
                    .map_err(|confirmation| {
                        anyhow::anyhow!(
                            "standalone replacement exchange outcome is ambiguous after error \
                              {error:?}; exact confirmation failed: {confirmation}"
                        )
                    })?;
                refusal = Some(error);
                confirmed
            }
        };
        if !committed {
            // The bail below reports only that nothing was written. The exchange's own error is
            // the money-path signal — `replace_marked_evacuation` rejects incoherent parent move
            // artifacts and a second live agent evacuation on the source through this same `Err`
            // channel, and both leave every reread row untouched, so they confirm as uncommitted
            // here. Its third corruption guard, a dirty child namespace, does NOT reach this
            // branch: `replacement_child_namespace` still reports `Contaminated`, so exact
            // confirmation fails and the caller reports an ambiguous outcome instead. Surface the
            // two that do land here (stderr at the CLI's default `warn`) rather than drop them.
            if let Some(error) = refusal {
                tracing::warn!(
                    ?error,
                    key = %replacement.old_key.0,
                    "standalone replacement exchange refused and confirmed uncommitted"
                );
            }
            anyhow::bail!("standalone replacement exchange was definitely uncommitted");
        }
        Ok(reservations)
    }

    /// Confirm a retryable exchange error against the exact sidecar, retired parent, and child
    /// state.  A mismatched or incomplete reread is deliberately an error: retrying could retire a
    /// parent without knowing whether the child was created.
    async fn confirm_standalone_replacement_exchange(
        &self,
        replacement: &crate::service::EvacuationReplacementPlan,
        old_before: &Intent,
        expected_child: &Intent,
    ) -> Result<bool, ExecError> {
        // A post-commit database error can be accompanied by one transient read fault. Retry only
        // that transport class, a small bounded number of times; structural/mixed snapshots remain
        // immediately ambiguous and therefore fail closed.
        for attempt in 0..3 {
            match self
                .confirm_standalone_replacement_exchange_once(
                    replacement,
                    old_before,
                    expected_child,
                )
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(ExecError::Retryable(error)) if attempt < 2 => {
                    tracing::warn!(
                        attempt,
                        %error,
                        "standalone replacement confirmation read retrying"
                    );
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded confirmation loop returns on its final attempt")
    }

    /// One exact confirmation snapshot attempt. Callers own the bounded Retryable retry policy.
    async fn confirm_standalone_replacement_exchange_once(
        &self,
        replacement: &crate::service::EvacuationReplacementPlan,
        old_before: &Intent,
        expected_child: &Intent,
    ) -> Result<bool, ExecError> {
        let relation = self
            .journal
            .evacuation_canonical_successor(&replacement.old_key)
            .await?;
        let old = self.journal.get(&replacement.old_key).await?;
        let child = self.journal.get(&replacement.fresh.idempotency_key).await?;
        let namespace = self
            .journal
            .replacement_child_namespace(&replacement.fresh.idempotency_key)
            .await?;
        match (relation, old, child, namespace) {
            (Some(relation), Some(old), Some(child), _)
                if relation.old_key == replacement.old_key
                    && relation.old_attempt == replacement.old_attempt
                    && relation.new_key == replacement.fresh.idempotency_key
                    && relation.new_attempt == 0
                    && relation.occurrence == replacement.fresh.occurrence
                    && relation.refusal == replacement.evidence
                    && old.status == IntentStatus::Failed
                    && old.attempt == replacement.old_attempt
                    && old.evacuation_refusal.as_ref() == Some(&replacement.evidence)
                    && child == *expected_child =>
            {
                Ok(true)
            }
            (
                None,
                Some(old),
                None,
                crate::journal::ReplacementChildNamespace::Pristine,
            ) if old == *old_before => Ok(false),
            _ => Err(ExecError::Permanent(
                "standalone replacement exchange reread was incomplete or did not exactly match either outcome"
                    .to_owned(),
            )),
        }
    }

    /// The standalone path's conflict projection, from the same durable `pending()` scan the
    /// daemon's reconcile uses. Fail-closed: an unreadable scan is an unknown eligibility, which
    /// every caller turns into a refusal to plan or apply rather than an empty, permissive set.
    async fn allocator_goal_blockers(&self) -> anyhow::Result<GoalBlockers> {
        self.journal
            .pending()
            .await
            .map(|pending| GoalBlockers::from_intents(&pending))
            .map_err(exec_err)
    }

    /// Price the funding pairs of `snapshot` that `priced` does not already cover, in place
    /// (`route_econ::price_missing_pairs`). `None` budget = no route I/O at all, which the
    /// allocator reads as the permissive `min_move` fallback. The pinned gateway is threaded
    /// through so an operator pin OVERRIDES route selection (§Q4). `blocked` drops the pairs
    /// that conflict with allocator work already in flight, so quotes are never spent on work
    /// this tick cannot emit anyway (br-p93). A held funding goal leaves its REVERSE pair and
    /// every other independent pair priceable; a live evacuation owns either direction touching
    /// its unreserved source balance.
    pub(crate) async fn price_missing_routes(
        &self,
        snapshot: &AllocatorSnapshot,
        budget: &mut RouteQuoteBudget,
        priced: &mut BTreeMap<(FederationId, FederationId), wallet_core::RouteEconomics>,
        blocked: &GoalBlockers,
    ) {
        crate::route_econ::price_missing_pairs(
            self.mc.as_ref(),
            self.pinned_gateway.as_ref(),
            snapshot,
            budget,
            priced,
            blocked,
        )
        .await
    }

    pub(crate) async fn designated_spending_from_probes(
        &self,
        policy: &TickPolicy,
        scorer_policy: &ScorerPolicy,
        probes: &[(FederationId, ProbeResult)],
    ) -> anyhow::Result<Option<FederationId>> {
        #[cfg(test)]
        if self
            .test_scheduler_designation_failures
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
        {
            anyhow::bail!("injected scheduler designation read fault");
        }
        let auto_joined = self.auto_joined_candidates().await?;
        let preliminary = build_snapshot(
            probes,
            policy,
            scorer_policy,
            &auto_joined,
            &BTreeMap::new(),
        );
        let active_probes = crate::service::active_probe_verdicts(
            self.journal.as_ref(),
            probes,
            preliminary.spending_fed,
            &policy.probe_gate_policy,
            now_ms(),
        )
        .await;
        Ok(
            build_snapshot(probes, policy, scorer_policy, &auto_joined, &active_probes)
                .spending_fed,
        )
    }

    /// The funding gate's PROBE-GATED set: every JOINED (`0x03`) federation that is NOT
    /// provably user-owned. Deriving it from joined MEMBERSHIP minus `UserApproved` candidates
    /// — rather than from `AutoJoined` candidate rows alone — fails CLOSED on two windows an
    /// AutoJoined-only set misses, both of which would otherwise let `tick` fund an
    /// agent-created member PRE-PROBE (defeating §5.1's "probes gate, discovery never
    /// promotes" invariant on the money path): (a) a crash between the Agent `join` and the
    /// `AutoJoined` candidate write leaves a member with a `Discovered`/absent `0x09` row;
    /// (b) a `0x03`-only restore leaves every agent-joined member with no `0x09` row.
    /// `discover`'s step-0 recovery repairs such rows, but `tick`/`build_snapshot` never run
    /// it, so the GATE itself must be conservative. Only an explicit `UserApproved` row (a
    /// user `join`/`approve`) exempts a member; a poison `0x09` row cannot PROVE user
    /// ownership, so it never exempts (the member stays gated by construction).
    async fn auto_joined_candidates(&self) -> anyhow::Result<BTreeSet<FederationId>> {
        let report = self
            .journal
            .list_candidates_report()
            .await
            .map_err(exec_err)?;
        let joined = self.journal.list_federations().await.map_err(exec_err)?;
        Ok(probe_gated_members(
            joined.into_iter().map(|(id, _)| id),
            report.candidates.iter().map(|(id, rec)| (*id, rec.state)),
        ))
    }

    async fn ensure_fresh_tick_decisions(
        &self,
        decisions: &[AllocatorDecision],
        occurrence: Occurrence,
    ) -> anyhow::Result<()> {
        let replays = self
            .terminal_replayed_executable_decisions(decisions)
            .await?;
        anyhow::ensure!(
            replays.is_empty(),
            "tick: occurrence {} would replay already-terminal/subscription-owned decision(s) {}; pass a fresh \
             --occurrence for a new rebalance, or use the same occurrence only to retry a \
             Pending/Executing tick",
            occurrence.0,
            describe_terminal_replays(&replays)
        );
        Ok(())
    }

    /// The same-occurrence decisions whose key already maps to an intent `apply` treats as
    /// TERMINAL, so re-driving them this tick is impossible without a fresh `--occurrence`. This
    /// MUST mirror `apply`'s terminal set (`wallet-core/src/executor.rs`): `Done` (idempotent
    /// replay of a settled intent), `Awaiting` (a `DirectInflow` owned by its subscription), and
    /// `Failed` (terminal until a manual reset — a recurring tick must not resurrect it). `apply`
    /// skips a `Failed` replay as `terminal_failed_skipped`, which `wallet-cli` turns into a
    /// non-zero tick exit; including it here lets `tick` fail early with the "advance --occurrence"
    /// remedy and lets the `status` dry run surface the SAME stale-occurrence signal.
    async fn terminal_replayed_executable_decisions(
        &self,
        decisions: &[AllocatorDecision],
    ) -> anyhow::Result<Vec<TerminalReplay>> {
        let mut replays = Vec::new();
        let mut seen = BTreeSet::new();
        for decision in decisions {
            if !tick_applies_decision(decision) || !seen.insert(decision.idempotency_key.clone()) {
                continue;
            }
            if let Some(intent) = self
                .journal
                .get(&decision.idempotency_key)
                .await
                .map_err(exec_err)?
            {
                if matches!(
                    intent.status,
                    IntentStatus::Done | IntentStatus::Awaiting | IntentStatus::Failed
                ) {
                    replays.push(TerminalReplay {
                        key: decision.idempotency_key.clone(),
                        status: intent.status,
                    });
                }
            }
        }
        Ok(replays)
    }

    /// The first route problem in this tick's fresh, apply-bound send-required decisions.
    /// Destination failures and send-gateway source failures both mark the selected
    /// destination unavailable, letting the tick planner rerun allocation and fall through to a
    /// later eligible federation when one can actually serve the route. If every destination
    /// fails an evacuation source-route preflight, the planner falls back to the last evacuation
    /// round and lets execution surface the real failure loudly.
    pub(crate) async fn first_move_route_problem(
        &self,
        decisions: &[AllocatorDecision],
    ) -> Option<MoveRouteProblem> {
        #[cfg(test)]
        if self
            .test_skip_route_preflight
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return None;
        }
        let decisions = decisions_to_apply(decisions);
        for decision in &decisions {
            let problem = match &decision.action {
                Action::Move { from, to, .. } => {
                    if self.has_existing_intent(decision).await {
                        continue;
                    }
                    self.validate_executor_move_route(SendRouteKind::Move, *from, *to)
                        .await
                        .err()
                }
                Action::Evacuate { from, to, .. } => {
                    if self.has_existing_intent(decision).await {
                        continue;
                    }
                    self.validate_executor_move_route(SendRouteKind::Evacuate, *from, *to)
                        .await
                        .err()
                }
                _ => None,
            };
            let Some(problem) = problem else {
                continue;
            };
            return Some(problem);
        }
        None
    }

    async fn has_existing_intent(&self, decision: &AllocatorDecision) -> bool {
        match self.journal.get(&decision.idempotency_key).await {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(
                    key = %decision.idempotency_key.0,
                    error = ?e,
                    "tick: could not read existing intent before route preflight; leaving route validation to apply"
                );
                true
            }
        }
    }

    /// Preflight the executor's concrete gateway route for a fresh send-required action.
    ///
    /// Destination failures mean this tick's chosen target cannot receive through the same
    /// gateway the executor will use, so the planner marks that destination unavailable and
    /// reruns allocation. Source-side failures are also tied to the destination-selected
    /// gateway: if that gateway cannot serve the source, another eligible destination may
    /// still work and should be tried before the executor commits any receive-side artifact.
    async fn validate_executor_move_route(
        &self,
        kind: SendRouteKind,
        from: FederationId,
        to: FederationId,
    ) -> Result<(), MoveRouteProblem> {
        // Mirror the executor's serving-both-ends predicate (§15.6): the route is usable iff SOME
        // registered gateway (or the pin) serves the destination and the source. The executor's
        // economic argmin need not be repeated here because this preflight is only a routability
        // verdict.
        let candidates = match self.route_gateway_candidates(&to).await {
            Ok(candidates) => candidates,
            Err(error) => {
                return Err(MoveRouteProblem {
                    from,
                    to,
                    mark_unavailable: to,
                    gateway: None,
                    error,
                    evacuation_source_route: false,
                });
            }
        };
        // Validate candidates in registration order, short-circuiting on the first that serves the
        // whole route; a gateway that fails the destination never has its source checked.
        let mut outcomes = Vec::with_capacity(candidates.len());
        for gateway in &candidates {
            let dest_ok = self.mc.validate_gateway(&to, gateway).await.is_ok();
            let source_ok = dest_ok && self.mc.validate_gateway(&from, gateway).await.is_ok();
            outcomes.push((dest_ok, source_ok));
            if source_ok {
                break;
            }
        }
        match scan_route(&outcomes) {
            RouteScan::Routable(_) => Ok(()),
            // A gateway served the destination but none of those also served the source → a
            // source-route problem (an evacuation may re-target another destination).
            RouteScan::SourceUnserved(i) => Err(source_route_problem(
                kind,
                from,
                to,
                candidates[i].clone(),
                "no gateway serving the destination also serves the source".into(),
            )),
            // No candidate served the destination at all → a destination problem.
            RouteScan::DestinationUnserved => Err(MoveRouteProblem {
                from,
                to,
                mark_unavailable: to,
                gateway: candidates.first().cloned(),
                error: "no registered gateway serves the destination".into(),
                evacuation_source_route: false,
            }),
        }
    }

    /// The gateway candidates the executor would SCAN for a move into `to` (§15.6): the single
    /// pinned gateway, or the destination's registered lnv2 set. `Err` (empty / unreadable) is a
    /// destination-route problem the caller reports against `to`.
    async fn route_gateway_candidates(&self, to: &FederationId) -> Result<Vec<GatewayUrl>, String> {
        if let Some(gateway) = &self.pinned_gateway {
            return Ok(vec![gateway.clone()]);
        }
        let gateways = self
            .mc
            .gateways(to)
            .await
            .map_err(|e| format!("listing destination gateways failed: {e}"))?;
        if gateways.is_empty() {
            return Err(format!(
                "no lnv2 gateway registered for destination federation {}",
                to.to_hex()
            ));
        }
        Ok(gateways)
    }

    /// Probe every OPEN federation into a `(FederationId, ProbeResult)` list, BEST-EFFORT: a fed
    /// whose probe errors (a local db/config read genuinely failed) is warn-logged and skipped,
    /// mirroring [`MultiClient::open_all`]'s poison-tolerance so one un-probeable fed cannot
    /// strand the whole tick. A skipped fed simply drops out of the snapshot — the allocator then
    /// cannot fund it or from it, which is the safe degradation (never a bad move).
    pub(crate) async fn probe_all(&self) -> Vec<(FederationId, ProbeResult)> {
        #[cfg(test)]
        if let Some(probes) = self
            .test_probe_all
            .lock()
            .expect("scheduler probe fixture mutex poisoned")
            .clone()
        {
            return probes;
        }
        let runner =
            FedimintProbeRunner::with_pinned_gateway(self.mc.clone(), self.pinned_gateway.clone());
        let mut probes = Vec::new();
        for id in self.mc.federations() {
            match runner.probe(&id).await {
                Ok(probe) => probes.push((id, probe)),
                Err(e) => tracing::warn!(
                    federation = %id.to_hex(),
                    error = ?e,
                    "tick: skipping federation that failed to probe"
                ),
            }
        }
        probes
    }

    /// Persist the settled/failed phase (+ optional outcome message) of a finalized move's
    /// `MoveRecord`, keeping the derived cache consistent with the intent's terminal status.
    async fn settle_move(
        &self,
        rec: &MoveRecord,
        expected_attempt: u32,
        phase: MovePhase,
        outcome: Option<String>,
    ) -> anyhow::Result<()> {
        let mut settled = rec.clone();
        settled.phase = phase;
        if outcome.is_some() {
            settled.outcome = outcome;
        }
        self.journal
            .put_move_if_attempt(&rec.key, expected_attempt, &settled)
            .await
            .map_err(exec_err)?
            .then_some(())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "move record no longer accepts cache finalization (attempt mismatch, terminal intent, or structural evacuation marker); leaving current state unchanged"
                )
            })
    }

    /// CAS the intent from `Awaiting` to a terminal status. `Ok(false)` means a concurrent
    /// finalize already moved it (idempotent) — not an error.
    async fn finalize(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        new: IntentStatus,
    ) -> anyhow::Result<()> {
        self.journal
            .set_status_if(key, expected_attempt, IntentStatus::Awaiting, new)
            .await
            .map_err(exec_err)?;
        Ok(())
    }

    async fn move_record_for_guard(
        &self,
        intent: &wallet_core::Intent,
    ) -> anyhow::Result<MoveRecord> {
        if let Some(rec) = self
            .journal
            .get_move(&intent.idempotency_key)
            .await
            .map_err(exec_err)?
        {
            return Ok(rec);
        }
        self.executor()
            .backfill_move_record(intent)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "intent {} is not an executable move",
                    intent.idempotency_key.0
                )
            })
    }
}

fn fresh_probe_baseline(candidate_is_open: bool, sampled: Option<Msat>) -> Option<Msat> {
    sampled.or_else(|| (!candidate_is_open).then_some(Msat(0)))
}

#[async_trait]
impl DiscoveryBackend for Runtime {
    async fn joined_federations(&self) -> anyhow::Result<BTreeSet<FederationId>> {
        Ok(self
            .journal
            .list_federations()
            .await
            .map_err(exec_err)?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }

    async fn joined_federation_invites(
        &self,
    ) -> anyhow::Result<Vec<(FederationId, fedimint_core::invite_code::InviteCode)>> {
        let mut invites = Vec::new();
        for (id, info) in self.journal.list_federations().await.map_err(exec_err)? {
            match info
                .invite
                .parse::<fedimint_core::invite_code::InviteCode>()
            {
                Ok(invite) => invites.push((id, invite)),
                Err(e) => tracing::warn!(
                    federation = %id.to_hex(),
                    error = ?e,
                    "discover: joined federation registry invite is invalid; cannot seed candidate row"
                ),
            }
        }
        Ok(invites)
    }

    async fn get_candidate(
        &self,
        id: FederationId,
    ) -> anyhow::Result<Option<crate::CandidateRecord>> {
        self.journal.get_candidate(&id).await.map_err(exec_err)
    }

    async fn put_candidate(&self, record: crate::CandidateRecord) -> anyhow::Result<()> {
        self.journal.put_candidate(&record).await.map_err(exec_err)
    }

    async fn list_candidates(&self) -> anyhow::Result<Vec<(FederationId, crate::CandidateRecord)>> {
        self.journal.list_candidates().await.map_err(exec_err)
    }

    async fn agent_created_federation(&self, id: FederationId) -> anyhow::Result<bool> {
        self.journal
            .agent_created_federation(&id)
            .await
            .map_err(exec_err)
    }

    async fn preview(
        &self,
        invite: &fedimint_core::invite_code::InviteCode,
    ) -> anyhow::Result<PreviewedCandidate> {
        let config = self.mc.preview_config(invite).await?;
        let id = crate::multi_client::bridge_federation_id(config.calculate_federation_id());
        Ok(PreviewedCandidate {
            id,
            facts: facts_from_client_config(id, &config),
        })
    }

    async fn auto_join_counts(
        &self,
        now_ms: u64,
        probe_policy: &ProbePolicy,
    ) -> anyhow::Result<AutoJoinCounts> {
        let passed = self.passed_probe_feds(now_ms, probe_policy).await;
        Ok(AutoJoinCounts {
            concurrent_unproven: self
                .journal
                .concurrent_unproven(&passed)
                .await
                .map_err(exec_err)?,
            weekly_auto_joins: self
                .journal
                .weekly_auto_joins(now_ms)
                .await
                .map_err(exec_err)?,
            lifetime_auto_joins: self.journal.lifetime_auto_joins().await.map_err(exec_err)?,
        })
    }

    async fn join_as_agent_with_membership_lease(
        &self,
        id: FederationId,
        invite: fedimint_core::invite_code::InviteCode,
        occurrence: Occurrence,
        now_ms: u64,
        join_timeout: Duration,
        membership_client: Option<&crate::service::WalletClient>,
    ) -> AutoJoinAttempt {
        let key = IdempotencyKey(format!("join:{}:{}", id.to_hex(), ledger_nonce()));
        if let Err(e) = self
            .journal
            .record_started(
                &key,
                OperationKind::Join { fed: id },
                Actor::Agent { occurrence },
                ReasonCode::StandingInstruction,
                now_ms,
                None,
            )
            .await
        {
            return AutoJoinAttempt::Failed(exec_err(e));
        }
        let join = match membership_client {
            Some(client) => {
                self.mc
                    .join_before_deadline_with_membership_lease(invite, join_timeout, client)
                    .await
            }
            None => self.mc.join_before_deadline(invite, join_timeout).await,
        };
        let outcome = match join {
            Ok(JoinDeadlineOutcome::Joined(outcome)) => outcome,
            Ok(JoinDeadlineOutcome::DeadlineElapsed) => {
                tracing::warn!(
                    key = %key.0,
                    timeout_ms = join_timeout.as_millis(),
                    "auto-join exceeded pass deadline; leaving join row repairable"
                );
                return AutoJoinAttempt::DeadlineElapsed;
            }
            Err(e) => {
                let _ = self
                    .journal
                    .record_terminal(
                        &key,
                        OperationStatus::Failed,
                        now_ms,
                        Some(&e.to_string()),
                        None,
                    )
                    .await;
                return AutoJoinAttempt::Failed(e);
            }
        };
        let note = (!outcome.newly_joined).then_some(crate::JOIN_NOOP_REOPEN_NOTE);
        match self
            .journal
            .record_terminal(&key, OperationStatus::Succeeded, now_ms, note, None)
            .await
        {
            Ok(()) => AutoJoinAttempt::Joined(outcome),
            Err(e) => AutoJoinAttempt::Failed(anyhow::anyhow!(
                "auto-join joined federation {} but failed to terminalize join row {}: {e:?}",
                outcome.id.to_hex(),
                key.0
            )),
        }
    }

    async fn record_discover(
        &self,
        key: IdempotencyKey,
        occurrence: Occurrence,
        report: &crate::DiscoverSourceReport,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        self.journal
            .record_terminal_operation(
                &key,
                discover_kind(report),
                discovery_actor(occurrence),
                DISCOVERY_REASON,
                now_ms,
            )
            .await
            .map_err(exec_err)
    }

    async fn record_auto_join(
        &self,
        key: IdempotencyKey,
        occurrence: Occurrence,
        report: &crate::AutoJoinReport,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        self.journal
            .record_terminal_operation(
                &key,
                auto_join_kind(report),
                discovery_actor(occurrence),
                DISCOVERY_REASON,
                now_ms,
            )
            .await
            .map_err(exec_err)
    }
}

/// The candidate ids whose live probe verdict can EXEMPT them from the concurrent-unproven cap
/// (§5.1.4): every `AutoJoined` row PLUS every poison-skipped id. `concurrent_unproven` counts
/// skipped ids fail-closed, so `passed_probe_feds` must be able to clear a skipped id that has
/// since Passed — otherwise a corrupt `AutoJoined` row whose probe passed would consume a
/// concurrent slot forever. Mirrors [`Runtime::auto_joined_candidates`]'s fail-closed set.
fn probe_gate_candidate_ids(report: &CandidateListReport) -> BTreeSet<FederationId> {
    let mut ids: BTreeSet<FederationId> = report
        .candidates
        .iter()
        .filter(|(_, rec)| rec.state == CandidateState::AutoJoined)
        .map(|(id, _)| *id)
        .collect();
    ids.extend(report.skipped_ids.iter().copied());
    ids
}

impl Runtime {
    async fn passed_probe_feds(
        &self,
        now_ms: u64,
        probe_policy: &ProbePolicy,
    ) -> BTreeSet<FederationId> {
        let report = match self.journal.list_candidates_report().await {
            Ok(report) => report,
            Err(e) => {
                tracing::warn!(error = ?e, "discover: candidate scan failed while computing passed probes");
                return BTreeSet::new();
            }
        };
        let mut passed = BTreeSet::new();
        for id in probe_gate_candidate_ids(&report) {
            let attempts = match self.journal.probe_record(&id).await {
                Ok(record) => record.map(|r| r.attempts).unwrap_or_default(),
                Err(e) => {
                    tracing::warn!(
                        federation = %id.to_hex(),
                        error = ?e,
                        "discover: probe record unreadable; treating candidate as unproven"
                    );
                    continue;
                }
            };
            let sources: BTreeSet<_> = attempts.iter().map(|attempt| attempt.from).collect();
            if sources.into_iter().any(|source| {
                probe_verdict(&attempts, source, now_ms, probe_policy) == ActiveProbeVerdict::Passed
            }) {
                passed.insert(id);
            }
        }
        passed
    }
}

fn facts_from_client_config(id: FederationId, config: &ClientConfig) -> FederationFacts {
    let num_endpoints = config.global.api_endpoints.len();
    let module_kinds: Vec<String> = config
        .modules
        .values()
        .map(|module| module.kind.as_str().to_owned())
        .collect();
    let has_lnv2 = module_kinds
        .iter()
        .any(|kind| kind == fedimint_lnv2_client::common::KIND.as_str());
    FederationFacts {
        id,
        guardian_count: num_endpoints as u32,
        threshold: threshold_for_endpoints(num_endpoints),
        is_mainnet: wallet_network(config) == Some(bitcoin::Network::Bitcoin),
        modules: module_kinds
            .iter()
            .map(|kind| module_from_kind(kind))
            .collect(),
        quorum_live: false,
        round_trip_ok: false,
        peg_out_quotable: false,
        latency_ms: 0,
        shutdown_scheduled: false,
        has_lnv2,
        observer: None,
        active_probe: None,
    }
}

fn discovery_due(state: &WatchState, policy: &WatchPolicy, now_ms: u64) -> bool {
    state.discover_backlog
        || now_ms
            >= state
                .last_discover_ms
                .saturating_add(policy.discover_every_ms)
}

fn add_expiry_deadlines(
    deadlines: &mut AdaptiveSleepDeadlines,
    raw_probes: &[(FederationId, ProbeResult)],
    now_ms: u64,
) {
    for (_, probe) in raw_probes {
        for expiry_ms in [
            probe
                .config_expiry_secs
                .map(|secs| secs.saturating_mul(1000)),
            probe
                .meta_module_expiry_secs
                .map(|secs| secs.saturating_mul(1000)),
        ]
        .into_iter()
        .flatten()
        {
            add_expiry_deadline(deadlines, expiry_ms, now_ms);
        }
    }
}

fn add_expiry_deadline(deadlines: &mut AdaptiveSleepDeadlines, expiry_ms: u64, now_ms: u64) {
    if expiry_ms > now_ms {
        deadlines.expiries_ms.push(expiry_ms);
    }
}

fn probe_due_base_ms(
    verdict: ActiveProbeVerdict,
    record: &ProbeRecord,
    source: FederationId,
    now_ms: u64,
    policy: &ProbePolicy,
) -> Option<u64> {
    match verdict {
        ActiveProbeVerdict::NeverProbed => None,
        ActiveProbeVerdict::Passed => {
            probe_pass_expiry_anchor_ms(&record.attempts, source, now_ms, policy)
        }
        ActiveProbeVerdict::Insufficient
        | ActiveProbeVerdict::Expired
        | ActiveProbeVerdict::Failed
        | ActiveProbeVerdict::FailedSinceLastPass => record
            .attempts
            .iter()
            .filter(|attempt| attempt.from == source)
            .map(|attempt| attempt.at_ms)
            .max(),
    }
}

fn threshold_for_endpoints(num_endpoints: usize) -> u32 {
    if num_endpoints == 0 {
        return 0;
    }
    NumPeers::from(num_endpoints).threshold() as u32
}

fn wallet_network(config: &ClientConfig) -> Option<bitcoin::Network> {
    config
        .modules
        .values()
        .find(|module| module.kind == fedimint_wallet_client::common::KIND)
        .and_then(|module| match &module.config {
            DynRawFallback::Decoded(config) => config
                .as_any()
                .downcast_ref::<fedimint_wallet_client::config::WalletClientConfig>()
                .map(|config| config.network.0),
            DynRawFallback::Raw { raw, .. } => {
                fedimint_wallet_client::config::WalletClientConfig::consensus_decode_whole(
                    raw,
                    &ModuleDecoderRegistry::default(),
                )
                .ok()
                .map(|config| config.network.0)
            }
        })
}

fn module_from_kind(kind: &str) -> Module {
    match kind {
        "mint" => Module::Mint,
        "ln" => Module::Ln,
        "lnv2" => Module::Lnv2,
        "wallet" => Module::Wallet,
        "meta" => Module::Meta,
        _ => Module::Other,
    }
}

fn ensure_expected_fed(
    key: &IdempotencyKey,
    rec: &MoveRecord,
    expected: FederationId,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        rec.to == expected,
        "intent {} receives into {}, not {}",
        key.0,
        rec.to.to_hex(),
        expected.to_hex()
    );
    Ok(())
}

/// The deterministic idempotency key for a CLI-driven `DirectInflow` (mirrors the allocator's
/// `move:`/`evac:` key scheme). Stable across re-runs of the same request so `apply` dedups it;
/// bumping `occurrence` produces a fresh key for a genuinely new inflow.
fn direct_inflow_key(
    to: &FederationId,
    amount: Msat,
    fee_cap: Msat,
    occurrence: Occurrence,
) -> IdempotencyKey {
    IdempotencyKey(format!(
        "direct-inflow:{}:{}:{}:{}",
        to.to_hex(),
        amount.0,
        fee_cap.0,
        occurrence.0
    ))
}

/// The deterministic idempotency key for a CLI-driven `Move` (mirrors the allocator's `move:`
/// scheme and [`direct_inflow_key`]'s all-params form). Stable across re-runs of the same
/// request so `apply` dedups it (no re-mint/re-pay); bumping `occurrence` produces a fresh key
/// for a genuinely new move. All params participate, so a same-`from`/`to`/`occurrence` request
/// with a DIFFERENT amount/cap is a distinct move rather than silently dedup'd to the old one.
pub fn move_key(
    from: &FederationId,
    to: &FederationId,
    amount: Msat,
    fee_cap: Msat,
    occurrence: Occurrence,
) -> IdempotencyKey {
    IdempotencyKey(format!(
        "move:{}:{}:{}:{}:{}",
        from.to_hex(),
        to.to_hex(),
        amount.0,
        fee_cap.0,
        occurrence.0
    ))
}

pub fn raw_pay_key(payment_hash: [u8; 32]) -> IdempotencyKey {
    IdempotencyKey(format!("pay:{}", bytes_hex(&payment_hash)))
}

pub fn raw_receive_key(to: FederationId, amount: Msat, nonce: &str) -> IdempotencyKey {
    IdempotencyKey(format!("recv:{}:{}:{nonce}", to.to_hex(), amount.0))
}

/// The nonce-anchored idempotency key for a `walletd` API `direct-inflow` (spec §6a.6): a
/// timed-out client retry with the SAME `(to, amount, nonce)` collides on this key and
/// dedups, while a deliberate repeat carries a fresh nonce. Distinct from the standalone
/// verb's occurrence-anchored [`direct_inflow_key`] — the two entry points key differently by
/// design (§6a.6), but both derive here so the daemon never forks its own scheme.
pub fn direct_inflow_nonce_key(to: FederationId, amount: Msat, nonce: &str) -> IdempotencyKey {
    IdempotencyKey(format!("dinflow:{}:{}:{nonce}", to.to_hex(), amount.0))
}

pub fn join_intent_key(federation: FederationId, invite: &str) -> IdempotencyKey {
    let invite_hash = sha256::Hash::hash(invite.as_bytes()).to_byte_array();
    IdempotencyKey(format!(
        "join:{}:{}",
        federation.to_hex(),
        bytes_hex(&invite_hash)
    ))
}

/// The idempotency key for a seed `recover` (`docs/archive/wallet-recovery-spec.md`). Distinct `recover:`
/// prefix from [`join_intent_key`] so recovery rows never classify as `KeyClass::Join` and so a
/// re-submitted recover of the same `(federation, invite)` dedups to the live/terminal intent
/// instead of forking a second recovery.
pub fn recover_intent_key(federation: FederationId, invite: &str) -> IdempotencyKey {
    let invite_hash = sha256::Hash::hash(invite.as_bytes()).to_byte_array();
    IdempotencyKey(format!(
        "recover:{}:{}",
        federation.to_hex(),
        bytes_hex(&invite_hash)
    ))
}

fn bytes_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The §5.0.5 probe report: the verdicts around one terminal invocation plus the leg keys.
/// `cost_msat` mirrors the terminal umbrella row's budget-counted cost, when money moved.
#[derive(Clone, Debug)]
pub struct ProbeReport {
    pub source: FederationId,
    pub verdict_before: ActiveProbeVerdict,
    pub outcome: ProbeOutcome,
    pub verdict_after: ActiveProbeVerdict,
    pub cost_msat: Option<Msat>,
    pub in_key: IdempotencyKey,
    /// `None` when the probe never reached leg OUT (a leg-IN failure).
    pub out_key: Option<IdempotencyKey>,
}

/// A probe invocation's terminal, operator-visible result. `active_probe` returns this
/// (Ok) for EVERY terminal outcome — success, a candidate-attributable leg failure, OR an
/// umbrella-only no-attempt refusal — so the CLI can honor its §5.0.7 scriptable stdout
/// contract in the failure cases too. `active_probe` reserves `Err` for genuinely
/// TRANSIENT defers (a balance read failed, session retained for a re-run).
#[derive(Clone, Debug)]
pub enum ProbeOutcome {
    /// A recorded attempt: a full round trip (`ok`) or a candidate-attributable leg
    /// failure (`!ok`) — both durably appended to the probe history and reflected in
    /// `verdict_after`.
    Attempt(ProbeAttempt),
    /// A terminal umbrella-only refusal that recorded NO attempt (a source/route/local
    /// fault, an inconclusive resume, or a parametric infeasibility): the trust verdict is
    /// unchanged. Carries the verbatim diagnostic.
    NoAttempt(String),
}

/// One probe invocation's fixed identity, threaded through the §5.0.5 exits.
struct ProbeRun {
    candidate: FederationId,
    source: FederationId,
    actor: Actor,
    verdict_before: ActiveProbeVerdict,
    nonce: String,
    umbrella_key: IdempotencyKey,
    amount: Msat,
    leg_fee_cap: Msat,
    in_key: IdempotencyKey,
    /// The policy the legs actually run under — money fields locked to the session, so a
    /// resumed attempt is judged against the parameters it was spent with (not the flags).
    effective_policy: ProbePolicy,
    /// The probe's START time, from the durable session (persisted before leg IN). The
    /// recorded attempt's `at_ms` uses THIS, not `now_ms()`: a crash-then-delayed-resume
    /// must stamp the evidence at when the probe happened, not at recovery time — the
    /// verdict is driven entirely by `at_ms`, so a recovery-time stamp could keep a stale
    /// probe inside the ttl window or synthesize the span a `Passed` needs.
    started_at_ms: u64,
}

/// Which probe leg a move drives: IN mints on the candidate (S → C), OUT redeems back
/// (C → S). Decides fault attribution — each step's HOST differs per leg.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeLeg {
    In,
    Out,
}

impl ProbeLeg {
    fn label(self) -> &'static str {
        match self {
            ProbeLeg::In => "IN",
            ProbeLeg::Out => "OUT",
        }
    }
}

/// §5.0.3's fault attribution verdict for a failed leg.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegFault {
    /// The candidate itself refused (mint on leg IN / pay on leg OUT): a DEMOTING attempt.
    Candidate,
    /// Source, gateway, ambiguous, or local-parametric: umbrella row only, no attempt.
    /// Safety holds without demotion because NO-ATTEMPT ≠ PASS — the candidate simply
    /// does not progress toward `Passed`.
    UmbrellaOnly,
}

/// Classify a failed probe leg from what the move machinery already exposes (§5.0.3):
/// the failing STEP (derived from which artifacts the move record holds), the terminal
/// settlement phase, and the executor's diagnostic. PURE, unit-tested. Demotes ONLY on a
/// candidate-hosted step's failure that is not a recognized local/gateway/corruption
/// signature — when attribution is genuinely unclear, the fault is AMBIGUOUS and does
/// not demote.
fn classify_leg_failure(leg: ProbeLeg, rec: Option<&MoveRecord>, error: &str) -> LegFault {
    match rec.map(|r| r.phase) {
        // A `Stranded` leg (send settled, receive never credited) attributes to nothing this
        // classifier can act on: the terminal collapses several destination-side outcomes and
        // establishes no fault, least of all the gateway's — a gateway alone cannot produce it
        // (see the `Stranded` note at the top of `move_protocol`). `Refunded` failed downstream
        // and made the payer whole. Neither demotes.
        Some(MovePhase::Stranded) | Some(MovePhase::Refunded) => return LegFault::UmbrellaOnly,
        // The send leg reached a terminal FAILED settlement: the PAYER owns it. Leg OUT's
        // payer is the candidate — the redeemability core.
        Some(MovePhase::Failed) => {
            return if leg == ProbeLeg::Out {
                LegFault::Candidate
            } else {
                LegFault::UmbrellaOnly
            };
        }
        _ => {}
    }
    // No terminal settlement phase: a Permanent executor error mid-step. Which step, from
    // the record's artifacts (`next_step`'s own derivation): no invoice and no send op =
    // `CreateInvoice` (runs on the move's DESTINATION); invoice without a send op = `Pay`
    // (runs on the move's SOURCE); both present = an await-step oddity (ambiguous).
    let has_invoice = rec.is_some_and(|r| r.invoice.is_some());
    let has_send_op = rec.is_some_and(|r| r.send_op.is_some());
    let candidate_hosted_step = if !has_invoice && !has_send_op {
        leg == ProbeLeg::In // mint hosted on C ⇔ the move's destination is C ⇔ leg IN
    } else if !has_send_op {
        leg == ProbeLeg::Out // pay hosted on C ⇔ the move's source is C ⇔ leg OUT
    } else {
        return LegFault::UmbrellaOnly;
    };
    if candidate_hosted_step && !is_known_non_candidate_error(error) {
        LegFault::Candidate
    } else {
        LegFault::UmbrellaOnly
    }
}

/// Error signatures OUR OWN machinery produces for local-parametric, fee-environment,
/// gateway-TOCTOU, expiry, and corruption faults — never candidate dishonesty, so they
/// must not demote even when they surface on a candidate-hosted step (§5.0.2/§5.0.3).
/// These are free-text couplings to diagnostics emitted in the executors; the test
/// `non_candidate_signatures_match_an_emit_site` pins each one to its emitting source
/// so a reworded diagnostic cannot silently start demoting candidates.
const NON_CANDIDATE_SIGNATURES: &[&str] = &[
    "fee over cap",                     // receive-side + pay-step cap refusals (local)
    "lnv2 requires at least",           // minimum-incoming-contract refusal (parametric)
    "no invoice can net the requested", // unsolvable gross-up (local/fee environment)
    "destination would exceed the per-fed cap", // ADR-0018 local cap refusal
    "gateway receive fee changed between quote and mint", // §15.7 TOCTOU (gateway-timed)
    "receive op is missing the quoted contract amount", // corruption
    "receive contract check failed",    // corruption
    "parsing move invoice",             // corruption
    "move invoice expired before the send leg", // §15.4 expiry belt (timing)
    "move invoice carries no amount",   // malformed/corrupt return invoice (source-side, not C)
    "reached with no",                  // internal invariant breaches
    "executor does not support this action",
];

fn is_known_non_candidate_error(error: &str) -> bool {
    if NON_CANDIDATE_SIGNATURES
        .iter()
        .any(|sig| error.contains(sig))
    {
        return true;
    }

    // Pinned SDK b108ec6 exposes these as deterministic send rejections
    // (`modules/fedimint-lnv2-client/src/lib.rs:1231-1249`). They are gateway
    // limits or a timing race around an already-minted invoice, not evidence that
    // the candidate federation refuses redemption.
    error.contains("lnv2 send deterministically rejected the invoice:")
        && (error.contains("Gateway fee exceeds the allowed limit")
            || error.contains("Gateway expiration time exceeds the allowed limit")
            || error.contains("Invoice has expired"))
}

/// §5.0.5: the wallet's NET OUTFLOW FROM the source — leg IN's total S debit (the
/// delivered net + both leg-IN fee quotes; the send settled iff the phase is `Settled`
/// or `Stranded`) minus leg OUT's S credit (its delivered net, iff `Settled`). `None`
/// when no money left the source (leg IN never settled its send, or refunded whole). On
/// a clean pass this is fees + the small residue; on a hostile candidate whose leg OUT
/// never redeems it is fees + the WHOLE delivered amount — the honest exposure number.
pub(crate) fn probe_cost(
    in_rec: Option<&MoveRecord>,
    out_rec: Option<&MoveRecord>,
) -> Option<Msat> {
    let debit = in_rec.and_then(|r| match r.phase {
        MovePhase::Settled | MovePhase::Stranded => Some(
            r.amount
                .0
                .saturating_add(r.receive_fee_quoted.map_or(0, |f| f.0))
                .saturating_add(r.send_fee_quoted.map_or(0, |f| f.0)),
        ),
        _ => None,
    })?;
    let credit = out_rec
        .and_then(|r| (r.phase == MovePhase::Settled).then_some(r.amount.0))
        .unwrap_or(0);
    Some(Msat(debit.saturating_sub(credit)))
}

/// §5.0.4's no-sweep precondition for the sized-but-unjournaled leg-OUT RESUME window:
/// leg OUT may start only when the candidate holds EXACTLY the pre-probe baseline plus
/// the delivered delta. Leg IN credits C exactly `delivered_in` (never-over; fees are
/// paid by the source), so an untouched C sits at exactly `baseline + delivered_in`. A
/// `>=` check is fooled by SPEND-THEN-REPLENISH: C held 100, delta 20 (→120); spend 15
/// (→105), then an unrelated inflow of 20 (→125) — `125 >= 120` passes though 15 sats of
/// the redemption would now come from other funds. Any deviation (below OR above) means
/// intervening activity touched C between the crash and this resume, so the delta's
/// provenance is no longer certain: abort INCONCLUSIVE (§5.0.4) rather than risk a sweep.
fn no_sweep_ok(c_spendable: Msat, baseline: Msat, delivered_in: Msat) -> bool {
    c_spendable.0 == baseline.0.saturating_add(delivered_in.0)
}

/// PURE core of [`Runtime::auto_joined_candidates`] (§5.1.3 funding gate): the probe-gated
/// set = JOINED members minus the `UserApproved` ones. Membership-minus-UserApproved (NOT
/// AutoJoined-rows-only) is what fails closed on the crash/restore windows where an
/// agent-created member's `0x09` row is still `Discovered`/`Rejected`/absent — those would
/// otherwise read as ungated on `tick` and fund pre-probe.
pub(crate) fn probe_gated_members(
    joined: impl IntoIterator<Item = FederationId>,
    candidate_states: impl IntoIterator<Item = (FederationId, CandidateState)>,
) -> BTreeSet<FederationId> {
    let user_approved: BTreeSet<FederationId> = candidate_states
        .into_iter()
        .filter_map(|(id, state)| (state == CandidateState::UserApproved).then_some(id))
        .collect();
    joined
        .into_iter()
        .filter(|id| !user_approved.contains(id))
        .collect()
}

/// Leg OUT's effective cap: the operator's per-leg cap still bounds fees, but the
/// return leg must also prove `out_net + actual drive-time fees <= delivered_in`.
/// The executor re-quotes send fees at `Pay`, so using the remaining delivered delta
/// as the move's fee cap keeps a fee spike from spending pre-probe candidate funds.
/// Fee-jitter margin reserved when sizing leg OUT (§5.0.2): the fee QUOTE at sizing time
/// can come in a few msat under the ACTUAL fee re-quoted at the Pay step, and the return
/// leg's cap is bounded tight by the no-sweep budget — with no margin, that jitter breaches
/// the cap and defers the probe. Sized out of the redeemed budget, it lands as bounded
/// extra candidate residue (accepted, §5.0.9 decision 6), always far below the leg fee cap.
const PROBE_FEE_MARGIN_MSAT: u64 = 1_000;

pub(crate) fn probe_out_fee_cap(delivered_in: Msat, out_net: Msat, leg_fee_cap: Msat) -> Msat {
    Msat(leg_fee_cap.0.min(delivered_in.0.saturating_sub(out_net.0)))
}

/// The probe's umbrella [`OperationKind`] (§5.0.5), with `cost_msat` = the terminal cost
/// (or `None` on `record_started` / no-money exits).
fn probe_kind(run: &ProbeRun, cost_msat: Option<Msat>) -> OperationKind {
    OperationKind::Probe {
        fed: run.candidate,
        from: run.source,
        amount_msat: run.amount,
        cost_msat,
    }
}

/// The umbrella ledger key `probe:<fed-hex>:<nonce>` (§5.0.5).
pub(crate) fn probe_umbrella_key(fed: &FederationId, nonce: &str) -> IdempotencyKey {
    IdempotencyKey(format!("probe:{}:{nonce}", fed.to_hex()))
}

/// The nonce-derived occurrence embedded in both probe legs' `move:` keys (§5.0.5): the
/// keys stay reconstructible from the session alone, and a 64-bit random head never
/// collides with user moves' small occurrence integers.
pub(crate) fn occurrence_from_nonce(nonce: &str) -> anyhow::Result<Occurrence> {
    let head = nonce
        .get(..16)
        .ok_or_else(|| anyhow::anyhow!("probe session nonce {nonce:?} is too short"))?;
    let value = u64::from_str_radix(head, 16)
        .map_err(|e| anyhow::anyhow!("probe session nonce {nonce:?} is not hex: {e}"))?;
    Ok(Occurrence(value))
}

/// The §5.0.5 LOCAL preflight faults, pure over sampled balances: self-probe, a source
/// too poor to fund `amount + leg fee cap`, a candidate without ADR-0018 cap room for
/// `amount`. The SOURCE needs no cap-room check: leg IN debits it by strictly more than
/// leg OUT returns, so the return always fits the room leg IN just created.
fn probe_local_faults(
    candidate: FederationId,
    source: FederationId,
    source_spendable: Msat,
    candidate_spendable: Msat,
    amount: Msat,
    leg_fee_cap: Msat,
    hard_cap: Option<Msat>,
) -> Result<(), String> {
    if candidate == source {
        return Err(format!(
            "cannot probe federation {} from itself",
            candidate.to_hex()
        ));
    }
    let needed = amount.0.saturating_add(leg_fee_cap.0);
    if source_spendable.0 < needed {
        return Err(format!(
            "insufficient source balance: {} holds {} msat, below amount + leg fee cap = {needed} msat",
            source.to_hex(),
            source_spendable.0
        ));
    }
    if let Some(cap) = hard_cap {
        if candidate_spendable.0.saturating_add(amount.0) > cap.0 {
            return Err(format!(
                "insufficient candidate cap room: {} holds {} msat and the {} msat probe amount \
                 would exceed the per-fed cap {} msat",
                candidate.to_hex(),
                candidate_spendable.0,
                amount.0,
                cap.0
            ));
        }
        // Leg OUT mints BACK into the source, which runs the same ADR-0018 perform-time cap
        // enforcement. Leg IN first debits the source by `amount + fees` and leg OUT credits
        // back strictly less, so an untouched source ENDING ≤ its start means a source that
        // starts AT-OR-BELOW the cap can never breach it on the return leg. But a source
        // already ABOVE the cap (a transient inbound) would spend leg IN and then fail leg
        // OUT umbrella-only with "destination would exceed the per-fed cap" — a GUARANTEED
        // inconclusive spend. Refuse it here as a LOCAL fault before any money moves.
        if source_spendable.0 > cap.0 {
            return Err(format!(
                "probe source {} holds {} msat, already above the per-fed cap {} msat: the \
                 return leg would breach it — reduce the source below the cap first",
                source.to_hex(),
                source_spendable.0,
                cap.0
            ));
        }
    }
    Ok(())
}

fn intent_status_label_opt(status: Option<IntentStatus>) -> &'static str {
    status.map_or("absent", intent_status_label)
}

pub(crate) fn mark_gateway_unavailable(
    probes: &mut [(FederationId, ProbeResult)],
    id: FederationId,
) -> bool {
    let Some((_, probe)) = probes.iter_mut().find(|(probe_id, _)| *probe_id == id) else {
        return false;
    };
    if !probe.gateway_available {
        return false;
    }
    probe.gateway_available = false;
    true
}

/// The verdict of scanning a destination's gateway set for a usable route (§15.6). Given, in
/// registration order, each candidate's `(serves_destination, serves_source)` validation outcomes
/// (`serves_source` is `true` for a receive-only route), decide whether SOME gateway serves BOTH
/// ends. PURE, so the "first gateway dead / second alive" and "serves only the destination" cases
/// are unit-tested without a live gateway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RouteScan {
    /// A fully-valid gateway (both ends served) was found at this index.
    Routable(usize),
    /// Some gateway served the destination (index given) but none of those served the source.
    SourceUnserved(usize),
    /// No gateway served the destination at all.
    DestinationUnserved,
}

fn scan_route(candidates: &[(bool, bool)]) -> RouteScan {
    let mut first_dest_ok: Option<usize> = None;
    for (i, &(dest_ok, source_ok)) in candidates.iter().enumerate() {
        if !dest_ok {
            continue;
        }
        if source_ok {
            return RouteScan::Routable(i);
        }
        first_dest_ok.get_or_insert(i);
    }
    match first_dest_ok {
        Some(i) => RouteScan::SourceUnserved(i),
        None => RouteScan::DestinationUnserved,
    }
}

fn source_route_problem(
    kind: SendRouteKind,
    from: FederationId,
    to: FederationId,
    gateway: GatewayUrl,
    error: String,
) -> MoveRouteProblem {
    MoveRouteProblem {
        from,
        to,
        mark_unavailable: to,
        gateway: Some(gateway),
        error: format!("source gateway validation failed: {error}"),
        evacuation_source_route: matches!(kind, SendRouteKind::Evacuate),
    }
}

/// Whether a decision is one the tick drives through `apply` — kept in lockstep with
/// [`decisions_to_apply`](crate::tick::decisions_to_apply), so the stale-occurrence guard in
/// [`Runtime::terminal_replayed_executable_decisions`] checks EXACTLY the set `apply` runs. As
/// of Phase 3.A that is every executable action (`Move`/`Evacuate`/`DirectInflow`); `Evacuate` is
/// no longer excluded, so a same-occurrence re-tick of a now-terminal evacuate fails loudly like a
/// Move instead of silently reporting success.
/// The one executable child of a replacement is deliberately absent from the ordinary round
/// decisions until its atomic parent exchange.  Planning/status/freshness all use this projection
/// so it is treated as the admitted work without ever letting ordinary `apply` create it early.
fn planned_tick_decisions(plan: &TickPlan) -> Vec<AllocatorDecision> {
    plan.replacement
        .as_ref()
        .map(|replacement| vec![replacement.fresh.clone()])
        .unwrap_or_else(|| plan.decisions.clone())
}

fn tick_applies_decision(decision: &AllocatorDecision) -> bool {
    decision.action.is_executable()
}

fn describe_terminal_replays(replays: &[TerminalReplay]) -> String {
    replays
        .iter()
        .map(|replay| format!("{} ({})", replay.key.0, intent_status_label(replay.status)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn intent_status_label(status: IntentStatus) -> &'static str {
    match status {
        IntentStatus::Pending => "pending",
        IntentStatus::Executing => "executing",
        IntentStatus::Done => "done",
        IntentStatus::Awaiting => "awaiting",
        IntentStatus::Failed => "failed",
    }
}

fn tick_terminal(summary: &ExecutionSummary) -> (OperationStatus, Option<String>) {
    if summary.failed == 0 && summary.terminal_failed_skipped == 0 {
        return (OperationStatus::Succeeded, None);
    }

    (
        OperationStatus::Failed,
        Some(format!(
            "tick: {} decision(s) did not apply (performed={} skipped={} failed={} \
             terminal_failed_skipped={} retryable={})",
            summary.failed + summary.terminal_failed_skipped,
            summary.performed,
            summary.skipped,
            summary.failed,
            summary.terminal_failed_skipped,
            summary.retryable
        )),
    )
}

/// Bridge an [`ExecError`] into an `anyhow::Error` for the CLI surface. `ExecError` carries its
/// diagnostic string in the variant, so `Debug` renders the useful context.
fn exec_err(e: ExecError) -> anyhow::Error {
    anyhow::anyhow!("{e:?}")
}

fn budget_counted_probe_cost_msat(row: &OperationRecord) -> Option<u64> {
    if !matches!(row.actor, Actor::Agent { .. }) {
        return None;
    }
    match &row.kind {
        OperationKind::Probe {
            cost_msat: Some(Msat(cost)),
            ..
        } => Some(*cost),
        _ => None,
    }
}

fn budget_skip_diagnostic_bucket_ms(now_ms: u64, budget_reset_ms: Option<u64>) -> u64 {
    budget_reset_ms.unwrap_or_else(|| {
        now_ms
            .saturating_div(PROBE_BUDGET_WINDOW_MS)
            .saturating_mul(PROBE_BUDGET_WINDOW_MS)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::FederationInfo;
    use fedimint_bip39::Mnemonic;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IDatabaseTransactionOpsCore as _;
    use fedimint_core::db::IRawDatabaseExt as _;
    use wallet_core::{FederationId, Intent, Journal, Msat, Occurrence, ProbeBudget};

    const FED_A: FederationId = FederationId([0xAA; 32]);
    const FED_B: FederationId = FederationId([0xBB; 32]);
    const FED_C: FederationId = FederationId([0xCC; 32]);
    const FED_D: FederationId = FederationId([0xDD; 32]);

    #[test]
    fn stale_standalone_status_warning_is_actionable() {
        let error = crate::service::replacement_occurrence_error(Occurrence(8), Occurrence(8));
        assert_eq!(
            stale_standalone_replacement_status_warning(&error),
            "structurally refused standalone evacuation requires --occurrence advanced beyond old \
             Agent occurrence (old=8, new=8); daemon scheduling advances occurrences automatically; \
             returning scored/designation diagnostics with no would-run decisions; retry standalone \
             tick/status with a strictly newer --occurrence"
        );
    }

    #[test]
    fn probe_gated_set_is_joined_members_minus_user_approved() {
        // The §5.1.3 funding gate must probe-gate every JOINED member that is not provably
        // user-owned — closing the crash/restore bypass where an agent-created member's 0x09
        // row is still Discovered/absent and an AutoJoined-rows-only set would read it ungated.
        let joined = [FED_A, FED_B, FED_C, FED_D];
        let states = [
            (FED_A, CandidateState::UserApproved), // user-owned -> UNGATED
            (FED_B, CandidateState::AutoJoined),   // agent-owned -> gated
            (FED_C, CandidateState::Discovered),   // crash: joined member, stale row -> gated
                                                   // FED_D: joined member with NO candidate row (0x03-only restore) -> gated
        ];
        let gated = probe_gated_members(joined, states);
        assert!(
            !gated.contains(&FED_A),
            "UserApproved member is not probe-gated"
        );
        assert!(gated.contains(&FED_B), "AutoJoined member is probe-gated");
        assert!(
            gated.contains(&FED_C),
            "a joined member with a stale Discovered row (crash window) is probe-gated"
        );
        assert!(
            gated.contains(&FED_D),
            "a joined member with no candidate row (restore) is probe-gated, not ungated"
        );
        // A Discovered candidate that is NOT joined never reaches the gate (not in `joined`).
        let not_joined = probe_gated_members([FED_A], [(FED_B, CandidateState::Discovered)]);
        assert_eq!(not_joined, BTreeSet::from([FED_A]));
    }

    async fn runtime_fixture() -> (Runtime, Arc<FedimintJournal>) {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        (Runtime::new(mc, journal.clone(), None, None, None), journal)
    }

    #[tokio::test]
    async fn join_rejects_a_federation_that_disagrees_with_the_invite() {
        use fedimint_core::config::FederationId as SdkFederationId;
        use fedimint_core::util::SafeUrl;
        use fedimint_core::PeerId;

        let (runtime, journal) = runtime_fixture().await;
        let sdk_id = SdkFederationId::from_str(&FED_A.to_hex()).expect("valid federation id");
        let invite = InviteCode::new(
            SafeUrl::parse("https://join-mismatch.example").expect("valid URL"),
            PeerId::from(0),
            sdk_id,
            None,
        );
        let invite = invite.to_string();
        let error = runtime
            .join(FED_B, invite.clone())
            .await
            .expect_err("mismatched join identity must be refused");
        assert!(error.to_string().contains("does not match"), "{error}");
        assert_eq!(
            journal
                .get(&join_intent_key(FED_B, &invite))
                .await
                .expect("read intent"),
            None
        );
    }

    #[test]
    fn join_operation_keys_are_invite_derived() {
        let invite_a = "invite-a";
        let invite_b = "invite-b";

        assert_eq!(
            join_intent_key(FED_A, invite_a),
            join_intent_key(FED_A, invite_a)
        );
        assert_ne!(
            join_intent_key(FED_A, invite_a),
            join_intent_key(FED_A, invite_b)
        );
    }

    #[tokio::test]
    async fn terminal_attach_returns_the_original_driver_error() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = AllocatorDecision {
            action: Action::Receive {
                to: FED_A,
                amount: Msat(50_000),
                fee_cap: Msat(1_000),
                nonce: "terminal-retry".into(),
                gateway: None,
            },
            reason: ReasonCode::UserInitiated,
            occurrence: Occurrence(0),
            idempotency_key: raw_receive_key(FED_A, Msat(50_000), "terminal-retry"),
        };
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("journal intent");
        journal
            .set_status(
                &decision.idempotency_key,
                0,
                IntentStatus::Failed,
                Some("original terminal reason"),
            )
            .await
            .expect("terminalize intent");

        let error = runtime
            .receive(
                FED_A,
                Msat(50_000),
                Msat(1_000),
                "terminal-retry".into(),
                None,
            )
            .await
            .expect_err("terminal attach returns its failure");
        assert!(
            error.to_string().contains("original terminal reason"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn join_terminal_attach_returns_the_original_driver_error() {
        use fedimint_core::config::FederationId as SdkFederationId;
        use fedimint_core::util::SafeUrl;
        use fedimint_core::PeerId;

        let (runtime, journal) = runtime_fixture().await;
        let sdk_id = SdkFederationId::from_str(&FED_A.to_hex()).expect("valid federation id");
        let invite = InviteCode::new(
            SafeUrl::parse("https://join-terminal.example").expect("valid URL"),
            PeerId::from(0),
            sdk_id,
            None,
        )
        .to_string();
        let key = join_intent_key(FED_A, &invite);
        let decision = AllocatorDecision {
            action: Action::Join {
                federation: FED_A,
                invite: invite.clone(),
                membership_preexisting: false,
            },
            reason: ReasonCode::UserInitiated,
            occurrence: Occurrence(0),
            idempotency_key: key.clone(),
        };
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("journal intent");
        journal
            .set_status(&key, 0, IntentStatus::Failed, Some("original join failure"))
            .await
            .expect("terminalize intent");

        let error = runtime
            .join(FED_A, invite)
            .await
            .expect_err("terminal join attach returns its failure");
        assert!(
            error.to_string().contains("original join failure"),
            "{error}"
        );
    }

    fn direct_inflow_intent(key: IdempotencyKey, to: FederationId, status: IntentStatus) -> Intent {
        Intent {
            idempotency_key: key,
            attempt: 0,
            action: Action::DirectInflow {
                to,
                amount: Msat(100_000),
                fee_cap: Msat(1_000),
            },
            max_fee: Some(Msat(1_000)),
            status,
            reason: ReasonCode::UserInitiated,
            actor: Actor::User,
            created_at_ms: 0,
            operation_id: None,
            invoice: None,
            evacuation_refusal: None,
        }
    }

    fn direct_inflow_record(
        key: IdempotencyKey,
        to: FederationId,
        phase: MovePhase,
        outcome: Option<&str>,
    ) -> MoveRecord {
        MoveRecord {
            key,
            from: None,
            to,
            amount: Msat(100_000),
            fee_cap: Msat(1_000),
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: false,
            invoice: Some(Invoice("lnbc1ptest".into())),
            recv_op: Some(crate::types::OperationId([0x07; 32])),
            send_op: None,
            phase,
            outcome: outcome.map(str::to_string),
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
        }
    }

    fn tick_move_decision(key: &str, from: FederationId, to: FederationId) -> AllocatorDecision {
        AllocatorDecision {
            action: Action::Move {
                from,
                to,
                amount: Msat(100_000),
                fee_cap: Msat(1_000),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey(key.to_string()),
        }
    }

    fn tick_evacuate_decision(
        key: &str,
        from: FederationId,
        to: FederationId,
    ) -> AllocatorDecision {
        AllocatorDecision {
            action: Action::Evacuate {
                from,
                to,
                amount: Msat(100_000),
                fee_cap: Msat(1_000),
                gateway: None,
                fee_cap_components: None,
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey(key.to_string()),
        }
    }

    fn standalone_replacement_plan(
        parent: Intent,
        evidence: wallet_core::EvacuationRefusalEvidence,
        fresh: AllocatorDecision,
    ) -> TickPlan {
        TickPlan {
            raw_probes: vec![],
            probes: vec![],
            active_probes: BTreeMap::new(),
            snapshot: AllocatorSnapshot {
                federations: vec![
                    wallet_core::FederationStatus {
                        id: FED_A,
                        balance: wallet_core::FedBalance {
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
                    },
                    wallet_core::FederationStatus {
                        id: FED_B,
                        balance: wallet_core::FedBalance {
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
                    },
                ],
                spending_fed: Some(FED_A),
                standby_fed: Some(FED_B),
                per_fed_cap: Msat(1_000_000),
                target_spending_balance: Msat(0),
                standby_target: Msat(0),
                max_fee: Msat(1_000),
                max_fee_bps_of_move: 100,
                evac_fee_base_msat: Msat(20_000),
                evac_fee_bps: 0,
                min_move: Msat(1),
                route_economics_by_pair: BTreeMap::new(),
                reservations: Reservations::default(),
                now: 1,
            },
            decisions: vec![],
            suppressed: vec![],
            replacement_deferred: vec![],
            deferred: vec![],
            blockers: GoalBlockers::default(),
            replacement: Some(crate::service::EvacuationReplacementPlan {
                old_key: parent.idempotency_key.clone(),
                old_attempt: parent.attempt,
                parent,
                evidence,
                fresh,
            }),
            marker_disposition: None,
        }
    }

    fn marked_evacuation_evidence() -> wallet_core::EvacuationRefusalEvidence {
        let old_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(10_000),
            bps: 0,
        };
        wallet_core::EvacuationRefusalEvidence {
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
            diagnostic: "measured, not proven".to_owned(),
            measured_at_ms: 1,
        }
    }

    async fn seed_standalone_replacement(
        journal: &FedimintJournal,
        parent_name: &str,
    ) -> (
        Intent,
        wallet_core::EvacuationRefusalEvidence,
        AllocatorDecision,
        TickPolicy,
    ) {
        let evidence = marked_evacuation_evidence();
        let new_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(20_000),
            bps: 0,
        };
        let mut fresh = tick_evacuate_decision(&format!("evac:{parent_name}-child"), FED_A, FED_B);
        fresh.occurrence = Occurrence(9);
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut fresh.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = new_cap.at(Msat(100_000));
        *fee_cap_components = Some(new_cap);
        let mut parent = Intent::from_decision(
            &tick_evacuate_decision(&format!("evac:{parent_name}-parent"), FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(8),
            },
            1,
        );
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut parent.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = evidence.cap_components.at(Msat(100_000));
        *fee_cap_components = Some(evidence.cap_components);
        parent.max_fee = Some(*fee_cap);
        parent.evacuation_refusal = Some(evidence.clone());
        journal
            .upsert(&parent)
            .await
            .expect("seed marked standalone parent");
        (
            parent,
            evidence,
            fresh,
            TickPolicy {
                per_fed_cap: Msat(1_000_000),
                evac_fee_base_msat: new_cap.base_msat,
                evac_fee_bps: new_cap.bps,
                ..TickPolicy::default()
            },
        )
    }

    #[tokio::test]
    async fn standalone_tick_rejects_dual_marker_outcomes_before_marker_or_child_mutation() {
        let (mut runtime, journal) = runtime_fixture().await;
        let (replacement_parent, evidence, replacement_child, policy) =
            seed_standalone_replacement(journal.as_ref(), "dual-shape-replacement").await;
        let disposition_evidence = marked_evacuation_evidence();
        let mut disposition_parent = Intent::from_decision(
            &tick_evacuate_decision("evac:dual-shape-disposition-parent", FED_C, FED_D),
            Actor::Agent {
                occurrence: Occurrence(7),
            },
            1,
        );
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut disposition_parent.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = disposition_evidence.cap_components.at(Msat(100_000));
        *fee_cap_components = Some(disposition_evidence.cap_components);
        disposition_parent.max_fee = Some(*fee_cap);
        disposition_parent.evacuation_refusal = Some(disposition_evidence);
        journal
            .upsert(&disposition_parent)
            .await
            .expect("seed independent no-child marker");
        let disposition_child =
            tick_evacuate_decision("evac:dual-shape-disposition-child", FED_C, FED_D);
        let mut plan = standalone_replacement_plan(
            replacement_parent.clone(),
            evidence,
            replacement_child.clone(),
        );
        plan.marker_disposition = Some(crate::service::EvacuationMarkerDisposition {
            parent: disposition_parent.clone(),
        });
        let executor = Arc::new(wallet_core::MockExecutor::new());
        runtime.set_tick_test_fixture(executor.clone(), plan);

        let error = runtime.tick(&policy).await.expect_err(
            "two independent marker outcomes must terminalize the audit before marker writes",
        );
        assert!(
            error
                .to_string()
                .contains("replacement and marker-clear disposition cannot share"),
            "{error:#}"
        );
        assert_eq!(
            journal
                .get(&replacement_parent.idempotency_key)
                .await
                .expect("read replacement parent"),
            Some(replacement_parent.clone()),
            "the replacement marker stays exact"
        );
        assert_eq!(
            journal
                .get(&disposition_parent.idempotency_key)
                .await
                .expect("read disposition parent"),
            Some(disposition_parent.clone()),
            "the no-child marker stays exact"
        );
        for child in [&replacement_child, &disposition_child] {
            assert!(
                journal
                    .get(&child.idempotency_key)
                    .await
                    .expect("read forged child")
                    .is_none(),
                "the forged dual plan must not create either child"
            );
        }
        for parent in [&replacement_parent, &disposition_parent] {
            assert!(
                journal
                    .evacuation_supersession(&parent.idempotency_key)
                    .await
                    .expect("read forged replacement sidecar")
                    .is_none(),
                "the forged dual plan must not create either sidecar"
            );
        }
        assert!(
            journal
                .history(usize::MAX, None)
                .await
                .expect("read forged dual-plan history")
                .iter()
                .any(|row| {
                    matches!(row.kind, OperationKind::Tick { .. })
                        && row.status == OperationStatus::Failed
                }),
            "the guard terminalizes its already-open tick audit row"
        );
        assert_eq!(
            executor.performed_keys(),
            Vec::<IdempotencyKey>::new(),
            "the guard runs before executor admission"
        );
    }

    async fn assert_retained_standalone_replacement_marker(
        journal: &FedimintJournal,
        parent: &Intent,
        fresh: &AllocatorDecision,
    ) {
        assert_eq!(
            journal
                .get(&parent.idempotency_key)
                .await
                .expect("read retained replacement parent"),
            Some(parent.clone()),
            "pre-exchange errors retain the exact Pending marker"
        );
        assert_eq!(
            journal
                .get(&fresh.idempotency_key)
                .await
                .expect("read absent replacement child"),
            None,
            "pre-exchange errors do not create a child"
        );
        assert!(
            journal
                .evacuation_supersession(&parent.idempotency_key)
                .await
                .expect("read absent replacement sidecar")
                .is_none(),
            "pre-exchange errors do not create a replacement sidecar"
        );
    }

    #[tokio::test]
    async fn standalone_replacement_admission_and_fresh_blocker_errors_retain_marker() {
        let (runtime, journal) = runtime_fixture().await;
        let (parent, evidence, fresh, mut policy) =
            seed_standalone_replacement(journal.as_ref(), "admission-retention").await;
        policy.per_fed_cap = Msat(1);
        let replacement = crate::service::EvacuationReplacementPlan {
            old_key: parent.idempotency_key.clone(),
            old_attempt: parent.attempt,
            parent: parent.clone(),
            evidence,
            fresh: fresh.clone(),
        };
        let error = runtime
            .replace_marked_evacuation_standalone(
                &replacement,
                &policy,
                &BTreeMap::from([(FED_A, Msat(1)), (FED_B, Msat(0))]),
                &GoalBlockers::default(),
            )
            .await
            .expect_err("admission failure must not release a structural marker");
        assert!(!error.to_string().is_empty(), "{error:#}");
        assert_retained_standalone_replacement_marker(&journal, &parent, &fresh).await;

        let (parent, evidence, fresh, policy) =
            seed_standalone_replacement(journal.as_ref(), "fresh-blocker-retention").await;
        let holder = Intent::from_decision(
            &tick_evacuate_decision("evac:fresh-blocker-holder", FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(7),
            },
            1,
        );
        journal
            .upsert(&holder)
            .await
            .expect("seed fresh durable blocker");
        let replacement = crate::service::EvacuationReplacementPlan {
            old_key: parent.idempotency_key.clone(),
            old_attempt: parent.attempt,
            parent: parent.clone(),
            evidence,
            fresh: fresh.clone(),
        };
        let error = runtime
            .replace_marked_evacuation_standalone(
                &replacement,
                &policy,
                &BTreeMap::from([(FED_A, Msat(500_000)), (FED_B, Msat(0))]),
                &GoalBlockers::default(),
            )
            .await
            .expect_err("a fresh durable blocker must not release a structural marker");
        assert!(
            error.to_string().contains("conflicts with allocator work"),
            "{error:#}"
        );
        assert_retained_standalone_replacement_marker(&journal, &parent, &fresh).await;
    }

    /// Standalone planning remains fail-closed when the complete reservation view is corrupt: it
    /// must not bypass the planner with a bespoke executor for a marked parent.
    #[tokio::test]
    async fn standalone_tick_corrupt_reservations_keeps_marked_parent_unchanged() {
        let client_db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(client_db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db.clone()));
        let mut runtime = Runtime::new(mc, journal.clone(), None, None, None);
        let executor = Arc::new(wallet_core::MockExecutor::new());
        runtime.set_tick_test_executor(executor.clone());

        let parent_key = IdempotencyKey("evac:standalone-corrupt-fallback".to_owned());
        let evidence = marked_evacuation_evidence();
        let mut parent = Intent::from_decision(
            &tick_evacuate_decision(&parent_key.0, FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(8),
            },
            1,
        );
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut parent.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = evidence.cap_components.at(Msat(100_000));
        *fee_cap_components = Some(evidence.cap_components);
        parent.max_fee = Some(*fee_cap);
        parent.evacuation_refusal = Some(evidence.clone());
        journal.upsert(&parent).await.expect("seed marked parent");

        let unmarked_key = IdempotencyKey("evac:standalone-corrupt-unmarked".to_owned());
        let unmarked = Intent::from_decision(
            &tick_evacuate_decision(&unmarked_key.0, FED_C, FED_B),
            Actor::Agent {
                occurrence: Occurrence(7),
            },
            1,
        );
        journal
            .upsert(&unmarked)
            .await
            .expect("seed ordinary unmarked evacuation");

        let poison_key = IdempotencyKey("move:standalone-corrupt-reservation".to_owned());
        let app_db = journal_db.with_prefix(vec![0x00]);
        let mut intent_key = vec![0x01];
        intent_key.extend_from_slice(poison_key.0.as_bytes());
        let mut pending_index_key = vec![0x04, 0x00];
        pending_index_key.extend_from_slice(poison_key.0.as_bytes());
        let mut dbtx = app_db.begin_transaction().await;
        dbtx.raw_insert_bytes(&intent_key, b"not valid json")
            .await
            .expect("insert corrupt pending intent");
        dbtx.raw_insert_bytes(&pending_index_key, &[])
            .await
            .expect("index corrupt pending intent");
        dbtx.commit_tx_result()
            .await
            .expect("commit corrupt pending intent");

        let error = runtime
            .tick(&TickPolicy {
                occurrence: Occurrence(9),
                evac_fee_base_msat: Msat(20_000),
                evac_fee_bps: 0,
                ..TickPolicy::default()
            })
            .await
            .expect_err("strict corrupt reservation view still aborts standalone planning");
        assert!(error.to_string().contains("intent"), "{error}");
        let tick_rows = journal
            .history(usize::MAX, None)
            .await
            .expect("read standalone corrupt-reservation history")
            .into_iter()
            .filter(|row| {
                matches!(
                    row.kind,
                    OperationKind::Tick {
                        occurrence: Occurrence(9),
                        ..
                    }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            tick_rows.len(),
            1,
            "real planning corruption must leave exactly one tick audit row: {tick_rows:#?}"
        );
        let tick = &tick_rows[0];
        assert_eq!(tick.status, OperationStatus::Failed);
        assert_eq!(tick.error.as_deref(), Some(error.to_string().as_str()));
        assert_eq!(
            tick.kind,
            OperationKind::Tick {
                occurrence: Occurrence(9),
                decisions: 0,
                performed: 0,
                failed: 0,
            },
            "planning failed before any decision could be applied"
        );
        assert_eq!(
            executor.performed_keys(),
            Vec::<IdempotencyKey>::new(),
            "corrupt strict reservations must not launch a marked-parent executor bypass"
        );
        let retained_parent = journal
            .get(&parent_key)
            .await
            .expect("read marked parent")
            .expect("marked parent remains durable");
        assert_eq!(retained_parent, parent);
        assert_eq!(
            journal.get(&unmarked_key).await.expect("read unmarked row"),
            Some(unmarked),
            "reservation corruption is not a generic bypass for ordinary pending work"
        );
        assert!(
            journal.reservation_intents().await.is_err(),
            "the recovery attempt does not weaken strict planning admission"
        );
    }

    #[tokio::test]
    async fn standalone_status_is_dry_and_tick_atomically_replaces_marked_evacuation() {
        let (mut runtime, journal) = runtime_fixture().await;
        let old_key = IdempotencyKey("evac:standalone-parent".to_owned());
        let evidence = marked_evacuation_evidence();
        let new_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(20_000),
            bps: 0,
        };
        let mut fresh = tick_evacuate_decision("evac:standalone-child", FED_A, FED_B);
        fresh.occurrence = Occurrence(9);
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut fresh.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = new_cap.at(Msat(100_000));
        *fee_cap_components = Some(new_cap);
        let mut parent = Intent::from_decision(
            &tick_evacuate_decision(&old_key.0, FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(8),
            },
            1,
        );
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut parent.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = evidence.cap_components.at(Msat(100_000));
        *fee_cap_components = Some(evidence.cap_components);
        parent.max_fee = Some(*fee_cap);
        parent.evacuation_refusal = Some(evidence.clone());
        journal.upsert(&parent).await.expect("seed marked parent");
        let executor = Arc::new(wallet_core::MockExecutor::new());
        let deferred_executable = tick_move_decision("move:standalone-deferred-c-b", FED_C, FED_B);
        let deferred_advisory = AllocatorDecision {
            action: Action::RefuseInflow {
                fed: FED_B,
                reason: ReasonCode::SpendingBelowTarget,
                diagnostics: wallet_core::RefusalDiagnostics {
                    source: Some(FED_C),
                    want: Some(Msat(101)),
                    available: Some(Msat(102)),
                    source_spendable: Some(Msat(103)),
                    max_fee: Some(Msat(104)),
                    max_fee_bps: Some(105),
                    cap_room: Some(Msat(106)),
                    amount: Some(Msat(107)),
                    conflict_suppressed: true,
                    min_move: Some(Msat(108)),
                },
            },
            reason: ReasonCode::SpendingBelowTarget,
            occurrence: Occurrence(9),
            idempotency_key: IdempotencyKey("refuse:standalone-deferred-c-b".to_owned()),
        };
        let replacement_plan = |child: AllocatorDecision| {
            let mut plan = standalone_replacement_plan(parent.clone(), evidence.clone(), child);
            plan.raw_probes = vec![
                (FED_A, raw_probe_with_expiry(true, None, None)),
                (FED_B, raw_probe_with_expiry(true, None, None)),
            ];
            plan.snapshot
                .federations
                .push(wallet_core::FederationStatus {
                    id: FED_C,
                    balance: wallet_core::FedBalance {
                        spendable: Msat(100_000),
                        in_flight: Msat(0),
                        claimable: Msat(0),
                        reserved_fee: Msat(0),
                    },
                    probed_ok: false,
                    reputation: 0,
                    shutdown_notice: false,
                    healthy: true,
                    eligible_to_fund: true,
                });
            let mut unavailable_c = raw_probe_with_expiry(false, None, None);
            unavailable_c.gateway_available = false;
            plan.probes = vec![(FED_C, unavailable_c)];
            plan.replacement_deferred =
                vec![deferred_executable.clone(), deferred_advisory.clone()];
            plan
        };
        let policy = TickPolicy {
            per_fed_cap: Msat(1_000_000),
            evac_fee_base_msat: new_cap.base_msat,
            evac_fee_bps: new_cap.bps,
            spending_fed: Some(FED_C),
            ..TickPolicy::default()
        };
        // The CLI default occurrence (zero) is not a successor. In particular, a hand-composed
        // standalone plan must not use the parent's identity to turn a structural marker into an
        // ordinary retry. This runs before the exchange, so the exact parent bytes and both
        // sidecars remain unchanged.
        let mut same_occurrence = fresh.clone();
        same_occurrence.idempotency_key =
            IdempotencyKey("evac:standalone-default-occurrence".into());
        same_occurrence.occurrence = Occurrence(0);
        runtime.set_tick_test_fixture(executor.clone(), replacement_plan(same_occurrence.clone()));
        let stale_status = runtime
            .status(&policy)
            .await
            .expect("standalone stale replacement status remains a scored diagnostic");
        assert!(
            stale_status.decisions.is_empty(),
            "standalone status must not advertise an impossible child or deferred ordinary work: \
             {stale_status:#?}"
        );
        assert_eq!(
            stale_status.scored.len(),
            2,
            "the populated status probes remain visible"
        );
        assert_eq!(stale_status.spending_fed, Some(FED_A));
        assert_eq!(stale_status.standby_fed, Some(FED_B));
        assert_eq!(
            journal.get(&old_key).await.expect("read dry stale parent"),
            Some(parent.clone()),
            "the diagnostic must not mutate the marker"
        );
        assert!(
            journal
                .get(&same_occurrence.idempotency_key)
                .await
                .expect("read dry stale child")
                .is_none(),
            "the diagnostic must not create a child"
        );
        assert!(
            journal
                .history(usize::MAX, None)
                .await
                .expect("read dry stale ledger")
                .iter()
                .all(|row| !matches!(row.kind, OperationKind::Tick { .. })),
            "the diagnostic must not open a tick row"
        );
        let error = runtime
            .tick(&policy)
            .await
            .expect_err("old=8/new=0 replacement must be refused before exchange");
        assert!(
            error
                .to_string()
                .contains("requires --occurrence advanced beyond old Agent occurrence"),
            "{error}"
        );
        assert_eq!(
            journal.get(&old_key).await.expect("read unchanged parent"),
            Some(parent.clone()),
            "old=8/new=0 is operator-correctable: retain the typed marker for a newer rerun"
        );
        assert!(
            journal
                .evacuation_supersession(&old_key)
                .await
                .expect("read absent forward sidecar")
                .is_none(),
            "old=8/new=0 must not write either replacement sidecar"
        );
        assert_eq!(executor.performed_keys(), Vec::<IdempotencyKey>::new());

        // The stale occurrence is operator-correctable, not a failed exchange: N+1 directly
        // reuses the unchanged typed marker and follows the production exchange/drive path.
        runtime.set_tick_test_fixture(executor.clone(), replacement_plan(fresh.clone()));
        // A post-commit Retryable forces exact confirmation.  This fixed timestamp is deliberately
        // unlike the wall clock: replacing either use of `exchange_now` with a second clock sample
        // makes the confirmation's expected child disagree with the atomic journal child.
        runtime.set_replacement_exchange_times_for_test([424_242]);

        let status = runtime.status(&policy).await.expect("dry status");
        assert_eq!(status.decisions, vec![fresh.clone()]);
        assert_eq!(
            journal.get(&old_key).await.expect("read parent"),
            Some(parent.clone()),
            "status must not retire the parent"
        );
        assert_eq!(
            journal
                .get(&fresh.idempotency_key)
                .await
                .expect("read prospective child"),
            None,
            "status must not create the successor"
        );

        journal.fail_after_next_evacuation_replacement_with_confirmation_read_for_test();
        // The first exact confirmation read faults after the exchange committed. The bounded
        // Retryable-only confirmation loop must reread and recover the child rather than poison
        // authority or reporting an ambiguous outcome.
        let report = runtime
            .tick(&policy)
            .await
            .expect("post-commit exchange exact-confirms after one Retryable read fault");
        assert_eq!(report.decisions, vec![fresh.clone()]);
        assert_eq!(
            (
                report.decisions.len(),
                report.summary.performed,
                report.summary.failed
            ),
            (1, 1, 0),
            "the confirmed child is driven and the tick records {{1,1,0}}"
        );
        assert_eq!(
            executor.performed_keys(),
            vec![fresh.idempotency_key.clone()]
        );
        for deferred in [&deferred_executable, &deferred_advisory] {
            assert_eq!(
                journal
                    .get(&deferred.idempotency_key)
                    .await
                    .expect("read deferred intent"),
                None,
                "a deferred decision must stay audit-only and never become a child intent"
            );
        }
        let executable_row = journal
            .operation(&crate::journal::OperationRef::Key(IdempotencyKey(format!(
                "tick-drop:{}:{}",
                policy.occurrence.0, deferred_executable.idempotency_key.0
            ))))
            .await
            .expect("read executable deferred audit")
            .expect("executable deferred audit row");
        assert!(
            executable_row
                .error
                .as_deref()
                .is_some_and(|error| error.contains("replacement-exclusive one-child round")),
            "{executable_row:#?}"
        );
        let advisory_row = journal
            .operation(&crate::journal::OperationRef::Key(
                deferred_advisory.idempotency_key.clone(),
            ))
            .await
            .expect("read advisory deferred audit")
            .expect("advisory deferred audit row");
        assert_eq!(
            advisory_row.error.as_deref(),
            Some("deferred: replacement-exclusive one-child round"),
            "{advisory_row:#?}"
        );
        assert_eq!(
            advisory_row.kind,
            wallet_core::OperationKind::Refusal {
                fed: FED_B,
                diagnostics: match deferred_advisory.action {
                    Action::RefuseInflow { diagnostics, .. } => diagnostics,
                    _ => unreachable!("advisory fixture"),
                },
            },
            "the standalone advisory's allocator diagnostics must be exact"
        );
        assert_eq!(
            journal
                .get(&old_key)
                .await
                .expect("read retired parent")
                .expect("parent remains auditable")
                .status,
            IntentStatus::Failed
        );
        let child = journal
            .get(&fresh.idempotency_key)
            .await
            .expect("read child")
            .expect("atomic child");
        assert_eq!(child.status, IntentStatus::Done);
        assert_eq!(child.created_at_ms, 424_242);
        assert_eq!(
            journal
                .evacuation_supersession(&old_key)
                .await
                .expect("read exact relation")
                .expect("replacement relation")
                .superseded_at_ms,
            child.created_at_ms,
            "child identity and atomic sidecar share the one exchange timestamp"
        );
        assert!(
            journal
                .history(usize::MAX, None)
                .await
                .expect("tick history")
                .into_iter()
                .any(|row| {
                    matches!(
                        row.kind,
                        OperationKind::Tick {
                            decisions: 1,
                            performed: 1,
                            failed: 0,
                            ..
                        }
                    )
                }),
            "the exchange is one tick decision, not an uncounted side effect"
        );
    }

    async fn assert_standalone_marker_disposition_clear_fault(post_commit: bool, no_work: bool) {
        let (mut runtime, journal) = runtime_fixture().await;
        let evidence = marked_evacuation_evidence();
        let mut parent = Intent::from_decision(
            &tick_evacuate_decision("evac:standalone-marker-clear", FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(8),
            },
            1,
        );
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut parent.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = evidence.cap_components.at(Msat(100_000));
        *fee_cap_components = Some(evidence.cap_components);
        parent.max_fee = Some(*fee_cap);
        parent.evacuation_refusal = Some(evidence.clone());
        journal.upsert(&parent).await.expect("seed marked parent");

        let fresh = tick_evacuate_decision("evac:unused-replacement", FED_A, FED_B);
        let mut plan = standalone_replacement_plan(parent.clone(), evidence.clone(), fresh);
        plan.replacement = None;
        plan.marker_disposition = Some(crate::service::EvacuationMarkerDisposition {
            parent: parent.clone(),
        });
        if !no_work {
            let mut source = plan.snapshot.federations[0].clone();
            source.id = FED_C;
            source.balance.spendable = Msat(100_000);
            source.shutdown_notice = false;
            let mut destination = plan.snapshot.federations[1].clone();
            destination.id = FED_D;
            plan.snapshot.federations.extend([source, destination]);
            plan.decisions = vec![tick_move_decision(
                "move:standalone-marker-clear-independent",
                FED_C,
                FED_D,
            )];
            plan.decisions[0].occurrence = Occurrence(9);
        }
        runtime.set_tick_test_fixture(Arc::new(wallet_core::MockExecutor::new()), plan);
        if post_commit {
            journal.fail_after_next_marker_clear_for_test();
        } else {
            journal.fail_before_next_marker_clear_for_test();
        }
        let result = runtime
            .tick(&TickPolicy {
                occurrence: Occurrence(9),
                ..TickPolicy::default()
            })
            .await;
        if no_work {
            assert!(
                result.is_err(),
                "a marker-only clear failure must remain loud"
            );
        } else {
            let report =
                result.expect("marker-local clear fault must not suppress independent work");
            assert!(
                report
                    .decisions
                    .iter()
                    .any(|decision| decision.idempotency_key.0
                        == "move:standalone-marker-clear-independent"),
                "{report:#?}"
            );
        }
        let parent_after = journal
            .get(&parent.idempotency_key)
            .await
            .expect("read marker parent")
            .expect("marker parent remains");
        assert_eq!(
            parent_after.evacuation_refusal,
            if post_commit { None } else { Some(evidence) },
            "pre-commit faults retain the exact marker; a post-commit ambiguity confirms its clear"
        );
    }

    #[tokio::test]
    async fn standalone_marker_disposition_clear_fault_continues_independent_work() {
        assert_standalone_marker_disposition_clear_fault(false, false).await;
        assert_standalone_marker_disposition_clear_fault(true, false).await;
    }

    #[tokio::test]
    async fn standalone_marker_disposition_clear_fault_without_work_terminalizes_tick() {
        assert_standalone_marker_disposition_clear_fault(false, true).await;
    }

    #[tokio::test]
    async fn standalone_replacement_confirmation_ignores_a_middle_parents_predecessor() {
        let (runtime, journal) = runtime_fixture().await;
        let cap_a = wallet_core::EvacFeeCap {
            base_msat: Msat(10_000),
            bps: 0,
        };
        let cap_b = wallet_core::EvacFeeCap {
            base_msat: Msat(20_000),
            bps: 0,
        };
        let cap_c = wallet_core::EvacFeeCap {
            base_msat: Msat(30_000),
            bps: 0,
        };
        let a_key = IdempotencyKey("evac:standalone-chain-a".to_owned());
        let mut a = Intent::from_decision(
            &tick_evacuate_decision(&a_key.0, FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(8),
            },
            1,
        );
        let mut a_evidence = marked_evacuation_evidence();
        a_evidence.cap_components = cap_a;
        a_evidence.low.fee_cap = cap_a.at(a_evidence.low.delivered_net);
        a_evidence.high.fee_cap = cap_a.at(a_evidence.high.delivered_net);
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut a.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = cap_a.at(Msat(100_000));
        *fee_cap_components = Some(cap_a);
        a.max_fee = Some(*fee_cap);
        a.evacuation_refusal = Some(a_evidence.clone());
        journal.upsert(&a).await.expect("seed marked A");

        let mut b = tick_evacuate_decision("evac:standalone-chain-b", FED_A, FED_B);
        b.occurrence = Occurrence(9);
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut b.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = cap_b.at(Msat(100_000));
        *fee_cap_components = Some(cap_b);
        journal
            .replace_marked_evacuation(&a_key, a.attempt, &a_evidence, &b, 2, &a)
            .await
            .expect("commit A -> B");

        let mut b_parent = journal
            .get(&b.idempotency_key)
            .await
            .expect("read B")
            .expect("B exists");
        let mut b_evidence = a_evidence.clone();
        b_evidence.cap_components = cap_b;
        b_evidence.low.fee_cap = cap_b.at(b_evidence.low.delivered_net);
        b_evidence.high.fee_cap = cap_b.at(b_evidence.high.delivered_net);
        b_evidence.low.total_fee = Msat(b_evidence.low.fee_cap.0 + 1);
        b_evidence.high.total_fee = Msat(b_evidence.high.fee_cap.0 + 2);
        b_parent.evacuation_refusal = Some(b_evidence.clone());
        journal.upsert(&b_parent).await.expect("mark B");

        let mut c = tick_evacuate_decision("evac:standalone-chain-c", FED_A, FED_B);
        c.occurrence = Occurrence(10);
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut c.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = cap_c.at(Msat(100_000));
        *fee_cap_components = Some(cap_c);
        let replacement = crate::service::EvacuationReplacementPlan {
            old_key: b.idempotency_key.clone(),
            old_attempt: b_parent.attempt,
            parent: b_parent.clone(),
            evidence: b_evidence.clone(),
            fresh: c.clone(),
        };
        let policy = TickPolicy {
            per_fed_cap: Msat(1_000_000),
            evac_fee_base_msat: cap_c.base_msat,
            evac_fee_bps: cap_c.bps,
            ..TickPolicy::default()
        };
        let balances = BTreeMap::from([(FED_A, Msat(500_000)), (FED_B, Msat(0))]);

        // B has a coherent A -> B predecessor, but the B -> C exchange faults before it writes.
        // Exact confirmation must therefore return Uncommitted while retaining B's exact marker and
        // leaving global durable facts without a C child. Replacing the strict lookup with the
        // dual-key reader makes this confirmation ambiguous instead.
        journal.fail_before_next_evacuation_replacement_for_test();
        let error = runtime
            .replace_marked_evacuation_standalone(
                &replacement,
                &policy,
                &balances,
                &GoalBlockers::default(),
            )
            .await
            .expect_err("pre-commit B -> C fault is confirmed uncommitted");
        assert!(
            error.to_string().contains("definitely uncommitted"),
            "{error:#}"
        );
        let b_after_fault = journal
            .get(&b.idempotency_key)
            .await
            .expect("read B after uncommitted confirmation")
            .expect("B remains");
        assert_eq!(b_after_fault.status, IntentStatus::Pending);
        assert_eq!(
            b_after_fault.evacuation_refusal,
            Some(b_evidence.clone()),
            "uncommitted confirmation retains exactly B's Pending parent marker"
        );
        assert!(
            journal
                .get(&c.idempotency_key)
                .await
                .expect("read C after uncommitted confirmation")
                .is_none(),
            "confirmation neither writes nor clears a nonexistent attempted child"
        );
        assert_eq!(
            journal
                .evacuation_supersession(&b.idempotency_key)
                .await
                .expect("read B predecessor through dual-key API")
                .expect("A -> B predecessor")
                .old_key,
            a_key,
            "the predecessor remains durable audit history, not an outcome for B -> C"
        );

        // No manual re-mark/reconcile is needed: a higher occurrence retries the exact retained
        // marker. The same strict reader must still confirm the actual B -> C successor.
        runtime.set_replacement_exchange_times_for_test([424_243]);
        journal.fail_after_next_evacuation_replacement_for_test();
        runtime
            .replace_marked_evacuation_standalone(
                &replacement,
                &policy,
                &balances,
                &GoalBlockers::default(),
            )
            .await
            .expect("post-commit B -> C fault is confirmed committed");
        assert_eq!(
            journal
                .evacuation_canonical_successor(&b.idempotency_key)
                .await
                .expect("read B canonical successor")
                .expect("B -> C successor")
                .new_key,
            c.idempotency_key
        );
        assert_eq!(
            journal
                .get(&c.idempotency_key)
                .await
                .expect("read committed C")
                .expect("C exists")
                .created_at_ms,
            424_243
        );
    }

    #[tokio::test]
    async fn standalone_replacement_validation_failure_preserves_the_marked_parent() {
        let (mut runtime, journal) = runtime_fixture().await;
        let old_key = IdempotencyKey("evac:standalone-corrupt-parent".to_owned());
        let evidence = marked_evacuation_evidence();
        let wrong_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(19_999),
            bps: 0,
        };
        let mut fresh = tick_evacuate_decision("evac:standalone-corrupt-child", FED_A, FED_B);
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut fresh.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = wrong_cap.at(Msat(100_000));
        *fee_cap_components = Some(wrong_cap);
        let mut parent = Intent::from_decision(
            &tick_evacuate_decision(&old_key.0, FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(8),
            },
            1,
        );
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut parent.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = evidence.cap_components.at(Msat(100_000));
        *fee_cap_components = Some(evidence.cap_components);
        parent.max_fee = Some(*fee_cap);
        parent.evacuation_refusal = Some(evidence.clone());
        journal.upsert(&parent).await.expect("seed marked parent");
        runtime.set_tick_test_fixture(
            Arc::new(wallet_core::MockExecutor::new()),
            standalone_replacement_plan(parent.clone(), evidence, fresh.clone()),
        );

        let error = runtime
            .tick(&TickPolicy {
                per_fed_cap: Msat(1_000_000),
                evac_fee_base_msat: Msat(20_000),
                evac_fee_bps: 0,
                ..TickPolicy::default()
            })
            .await
            .expect_err("mismatched child cap must fail before the exchange");
        assert!(
            error.to_string().contains("no longer exactly matches"),
            "{error}"
        );
        assert_eq!(
            journal.get(&old_key).await.expect("read parent"),
            Some(parent),
            "a failed validation retains the exact Pending parent marker"
        );
        assert_eq!(
            journal
                .get(&fresh.idempotency_key)
                .await
                .expect("read child"),
            None,
            "a failed validation must not create a child"
        );
    }

    /// The standalone path must discover a marked evacuation through the real
    /// raw-probe planner, not by accepting a test-built `TickPlan`.  This pins
    /// the `round.replacement -> TickPlan.replacement` propagation that tick
    /// later exchanges and drives.
    #[tokio::test]
    async fn standalone_probe_planner_carries_the_exact_structural_replacement() {
        let (runtime, journal) = runtime_fixture().await;
        runtime.skip_route_preflight_for_test();
        let old_occurrence = Occurrence(41);
        let child_occurrence = Occurrence(42);
        let old_key = IdempotencyKey(format!(
            "evac:{}:{}:{}",
            FED_A.to_hex(),
            FED_B.to_hex(),
            old_occurrence.0
        ));
        let old_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(10_000),
            bps: 0,
        };
        let new_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(30_000),
            bps: 0,
        };
        let evidence = marked_evacuation_evidence();
        let mut parent_decision = tick_evacuate_decision(&old_key.0, FED_A, FED_B);
        parent_decision.occurrence = old_occurrence;
        let Action::Evacuate {
            amount,
            fee_cap,
            fee_cap_components,
            ..
        } = &mut parent_decision.action
        else {
            unreachable!("evacuation fixture")
        };
        *amount = Msat(300_000);
        *fee_cap = old_cap.at(*amount);
        *fee_cap_components = Some(old_cap);
        let mut parent = Intent::from_decision(
            &parent_decision,
            Actor::Agent {
                occurrence: old_occurrence,
            },
            1,
        );
        parent.max_fee = Some(old_cap.at(Msat(300_000)));
        parent.evacuation_refusal = Some(evidence.clone());
        journal
            .upsert(&parent)
            .await
            .expect("seed structural marker");
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("the replacement destination is joined");
        let probe_gate_policy = ProbePolicy {
            min_successes: 1,
            min_span_ms: 0,
            ttl_ms: 60 * 60 * 1000,
            ..ProbePolicy::default()
        };
        seed_passed_probe(journal.as_ref(), FED_B, FED_A, &probe_gate_policy).await;

        let policy = TickPolicy {
            per_fed_cap: Msat(1_000_000),
            target_spending_balance: Msat(0),
            standby_target: Msat(0),
            evac_fee_base_msat: new_cap.base_msat,
            evac_fee_bps: new_cap.bps,
            spending_fed: Some(FED_A),
            standby_fed: Some(FED_B),
            occurrence: child_occurrence,
            probe_gate_policy,
            ..TickPolicy::default()
        };
        let mut dying_a = raw_probe_with_expiry(true, None, None);
        dying_a.spendable_msat = 300_000;
        let mut healthy_b = raw_probe_with_expiry(false, None, None);
        healthy_b.spendable_msat = 0;

        let plan = runtime
            .plan_tick_from_probes(&policy, vec![(FED_A, dying_a), (FED_B, healthy_b)])
            .await
            .expect("real standalone probe planner discovers a qualifying marker");
        assert!(plan.decisions.is_empty(), "{plan:#?}");
        let replacement = plan
            .replacement
            .expect("round replacement survives into the standalone TickPlan");
        assert_eq!(replacement.old_key, old_key);
        assert_eq!(replacement.old_attempt, 0);
        assert_eq!(replacement.evidence, evidence);
        assert_eq!(
            replacement.fresh.idempotency_key,
            IdempotencyKey(format!(
                "evac:{}:{}:{}",
                FED_A.to_hex(),
                FED_B.to_hex(),
                child_occurrence.0
            ))
        );
        assert_eq!(replacement.fresh.occurrence, child_occurrence);
        assert!(matches!(
            replacement.fresh.action,
            Action::Evacuate {
                from: FED_A,
                to: FED_B,
                amount: Msat(300_000),
                fee_cap,
                fee_cap_components: Some(cap),
                ..
            } if cap == new_cap && fee_cap == new_cap.at(Msat(300_000))
        ));
    }

    #[tokio::test]
    async fn absent_pinned_input_refuses_standalone_replacement_before_the_exchange() {
        let (mut runtime, journal) = runtime_fixture().await;
        let old_key = IdempotencyKey("evac:standalone-pinned-parent".to_owned());
        let evidence = marked_evacuation_evidence();
        let new_cap = wallet_core::EvacFeeCap {
            base_msat: Msat(20_000),
            bps: 0,
        };
        let mut fresh = tick_evacuate_decision("evac:standalone-pinned-child", FED_A, FED_B);
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut fresh.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = new_cap.at(Msat(100_000));
        *fee_cap_components = Some(new_cap);
        let mut parent = Intent::from_decision(
            &tick_evacuate_decision(&old_key.0, FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(8),
            },
            1,
        );
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = &mut parent.action
        else {
            unreachable!("evacuation fixture")
        };
        *fee_cap = evidence.cap_components.at(Msat(100_000));
        *fee_cap_components = Some(evidence.cap_components);
        parent.max_fee = Some(*fee_cap);
        parent.evacuation_refusal = Some(evidence.clone());
        journal.upsert(&parent).await.expect("seed marked parent");
        runtime.set_tick_test_fixture(
            Arc::new(wallet_core::MockExecutor::new()),
            standalone_replacement_plan(parent.clone(), evidence, fresh.clone()),
        );

        let error = runtime
            .tick(&TickPolicy {
                per_fed_cap: Msat(1_000_000),
                evac_fee_base_msat: new_cap.base_msat,
                evac_fee_bps: new_cap.bps,
                standby_fed: Some(FED_C),
                ..TickPolicy::default()
            })
            .await
            .expect_err("an absent configured pin must stop the exchange");
        assert!(
            error.to_string().contains("failed to probe"),
            "pin failure, not a replacement side effect: {error}"
        );
        assert_eq!(
            journal.get(&old_key).await.expect("read parent"),
            Some(parent),
            "pin validation happens before the atomic parent retirement"
        );
        assert_eq!(
            journal
                .get(&fresh.idempotency_key)
                .await
                .expect("read child"),
            None,
            "pin validation must not create the child"
        );
    }

    #[test]
    fn replacement_child_is_admitted_evidence_for_pinned_input_validation() {
        let evidence = marked_evacuation_evidence();
        let fresh = tick_evacuate_decision("evac:pin-evidence-child", FED_A, FED_B);
        let mut plan = standalone_replacement_plan(
            Intent::from_decision(
                &tick_evacuate_decision("evac:pin-evidence-parent", FED_A, FED_B),
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                0,
            ),
            evidence,
            fresh,
        );
        let mut unusable_source = raw_probe_with_expiry(true, None, None);
        unusable_source.gateway_available = false;
        plan.probes = vec![(FED_A, unusable_source)];
        let policy = TickPolicy {
            spending_fed: Some(FED_A),
            ..TickPolicy::default()
        };
        assert!(
            Runtime::pinned_input_problems(&policy, &plan, &plan.blockers).is_empty(),
            "the planned replacement evacuation is admitted endpoint evidence for its pinned source"
        );
    }

    #[test]
    fn replacement_deferred_are_validation_evidence_without_becoming_suppression_vouchers() {
        let evidence = marked_evacuation_evidence();
        let fresh = tick_evacuate_decision("evac:pin-evidence-child", FED_A, FED_B);
        let mut plan = standalone_replacement_plan(
            Intent::from_decision(
                &tick_evacuate_decision("evac:pin-evidence-parent", FED_A, FED_B),
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                0,
            ),
            evidence,
            fresh,
        );
        plan.snapshot
            .federations
            .push(wallet_core::FederationStatus {
                id: FED_C,
                balance: wallet_core::FedBalance {
                    spendable: Msat(100_000),
                    in_flight: Msat(0),
                    claimable: Msat(0),
                    reserved_fee: Msat(0),
                },
                probed_ok: false,
                reputation: 0,
                shutdown_notice: false,
                healthy: true,
                eligible_to_fund: true,
            });
        let mut unusable_c = raw_probe_with_expiry(false, None, None);
        unusable_c.gateway_available = false;
        plan.probes = vec![(FED_C, unusable_c)];
        let policy = TickPolicy {
            spending_fed: Some(FED_C),
            ..TickPolicy::default()
        };
        let executable = tick_move_decision("move:deferred-c-b", FED_C, FED_B);
        let advisory = AllocatorDecision {
            action: Action::RefuseInflow {
                fed: FED_B,
                reason: ReasonCode::SpendingBelowTarget,
                diagnostics: wallet_core::RefusalDiagnostics {
                    source: Some(FED_C),
                    ..Default::default()
                },
            },
            reason: ReasonCode::SpendingBelowTarget,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey("refuse:deferred-c-b".to_owned()),
        };
        plan.replacement_deferred = vec![executable, advisory.clone()];
        assert!(
            Runtime::pinned_input_problems(&policy, &plan, &plan.blockers).is_empty(),
            "the third configured pin's deferred executable was planned and route-preflighted; \
             replacement exclusivity must not turn that into a false pin refusal"
        );

        // Deferred work is validation evidence, not conflict suppression. An advisory only vouches
        // when its source still matches the durable holder; a re-sourced holder must stay loud.
        plan.replacement_deferred = vec![advisory];
        let re_sourced_holder = tick_move_decision("move:held-a-b", FED_A, FED_B);
        plan.blockers.hold_decision(
            &re_sourced_holder,
            Actor::Agent {
                occurrence: Occurrence(0),
            },
        );
        assert!(
            !Runtime::pinned_input_problems(&policy, &plan, &plan.blockers).is_empty(),
            "an unrelated deferred advisory must not produce a false pass for the third pin"
        );
    }

    fn federation_info() -> FederationInfo {
        FederationInfo {
            invite: "test invite not parsed by scheduler tests".to_owned(),
            db_prefix: 0,
            joined_at: 1,
        }
    }

    fn due_discovery_watch_policy() -> WatchPolicy {
        WatchPolicy {
            discover_every_ms: 0,
            max_candidates_per_pass: 2,
            ..WatchPolicy::default()
        }
    }

    fn raw_probe_with_expiry(
        shutdown_scheduled: bool,
        config_expiry_secs: Option<u64>,
        meta_module_expiry_secs: Option<u64>,
    ) -> ProbeResult {
        ProbeResult {
            guardian_count: 4,
            threshold: 3,
            is_mainnet: true,
            module_kinds: vec!["mint".to_owned(), "wallet".to_owned(), "lnv2".to_owned()],
            has_lnv2: true,
            quorum_live: true,
            latency_ms: 10,
            gateway_available: true,
            wallet_module_present: true,
            expiry_timestamp_secs: config_expiry_secs,
            config_expiry_secs,
            meta_module_expiry_secs,
            status_scheduled_shutdown: shutdown_scheduled,
            shutdown_scheduled,
            spendable_msat: 0,
            in_flight_msat: 0,
            claimable_msat: 0,
        }
    }

    async fn seed_passed_probe(
        journal: &FedimintJournal,
        candidate: FederationId,
        source: FederationId,
        policy: &ProbePolicy,
    ) {
        let started_at_ms = now_ms().saturating_sub(40 * 60 * 1000);
        let nonce = "00000000000000550000000000000000";
        let session = ProbeSession {
            nonce: nonce.to_owned(),
            from: source,
            amount_msat: policy.amount_msat,
            leg_fee_cap_msat: policy.leg_fee_cap_msat,
            c_spendable_before_in_msat: 0,
            out_net_msat: None,
            started_at_ms,
        };
        journal
            .begin_probe_session(&candidate, &session)
            .await
            .expect("begin probe session");
        let attempt = ProbeAttempt {
            at_ms: started_at_ms,
            ok: true,
            from: source,
            amount_msat: policy.amount_msat,
            leg_fee_cap_msat: policy.leg_fee_cap_msat,
            error: None,
        };
        journal
            .record_probe_outcome(
                &candidate,
                nonce,
                Some(attempt),
                &probe_umbrella_key(&candidate, nonce),
                OperationKind::Probe {
                    fed: candidate,
                    from: source,
                    amount_msat: Msat(policy.amount_msat),
                    cost_msat: Some(Msat(1)),
                },
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                OperationStatus::Succeeded,
                None,
            )
            .await
            .expect("record probe outcome");
    }

    async fn seed_pre_leg_probe_session(
        journal: &FedimintJournal,
        candidate: FederationId,
        source: FederationId,
        policy: &ProbePolicy,
    ) {
        let session = ProbeSession {
            nonce: "00000000000000770000000000000000".to_owned(),
            from: source,
            amount_msat: policy.amount_msat,
            leg_fee_cap_msat: policy.leg_fee_cap_msat,
            c_spendable_before_in_msat: 0,
            out_net_msat: None,
            started_at_ms: now_ms(),
        };
        journal
            .begin_probe_session(&candidate, &session)
            .await
            .expect("begin probe session");
    }

    async fn seed_post_in_probe_session(
        journal: &FedimintJournal,
        candidate: FederationId,
        source: FederationId,
        policy: &ProbePolicy,
    ) -> IdempotencyKey {
        let nonce = "00000000000000660000000000000000";
        let occurrence = occurrence_from_nonce(nonce).expect("valid nonce occurrence");
        let session = ProbeSession {
            nonce: nonce.to_owned(),
            from: source,
            amount_msat: policy.amount_msat,
            leg_fee_cap_msat: policy.leg_fee_cap_msat,
            c_spendable_before_in_msat: 0,
            out_net_msat: None,
            started_at_ms: now_ms(),
        };
        journal
            .begin_probe_session(&candidate, &session)
            .await
            .expect("begin probe session");
        let in_key = move_key(
            &source,
            &candidate,
            Msat(policy.amount_msat),
            Msat(policy.leg_fee_cap_msat),
            occurrence,
        );
        journal
            .upsert(&Intent {
                idempotency_key: in_key.clone(),
                attempt: 0,
                action: Action::Move {
                    from: source,
                    to: candidate,
                    amount: Msat(policy.amount_msat),
                    fee_cap: Msat(policy.leg_fee_cap_msat),
                    gateway: None,
                },
                max_fee: Some(Msat(policy.leg_fee_cap_msat)),
                status: IntentStatus::Done,
                reason: ReasonCode::ActiveProbe,
                actor: Actor::Agent {
                    occurrence: Occurrence(1),
                },
                created_at_ms: now_ms(),
                operation_id: None,
                invoice: None,
                evacuation_refusal: None,
            })
            .await
            .expect("seed leg-in intent");
        in_key
    }

    #[test]
    fn direct_inflow_key_is_deterministic_and_param_sensitive() {
        let to = FederationId([0xCD; 32]);
        let base = direct_inflow_key(&to, Msat(100_000), Msat(1_100_000), Occurrence(0));
        // Same inputs -> same key: a re-run of the same request dedups (no second invoice).
        assert_eq!(
            base,
            direct_inflow_key(&to, Msat(100_000), Msat(1_100_000), Occurrence(0))
        );
        // Each parameter participates, so a genuinely different inflow gets a distinct key.
        assert_ne!(
            base,
            direct_inflow_key(&to, Msat(100_001), Msat(1_100_000), Occurrence(0))
        );
        assert_ne!(
            base,
            direct_inflow_key(&to, Msat(100_000), Msat(1_100_001), Occurrence(0))
        );
        assert_ne!(
            base,
            direct_inflow_key(&to, Msat(100_000), Msat(1_100_000), Occurrence(1))
        );
        assert_ne!(
            base,
            direct_inflow_key(
                &FederationId([0xCE; 32]),
                Msat(100_000),
                Msat(1_100_000),
                Occurrence(0)
            )
        );
        // The key embeds the destination hex + the three numeric params, in order.
        assert_eq!(
            base.0,
            format!("direct-inflow:{}:100000:1100000:0", to.to_hex())
        );
    }

    #[test]
    fn move_key_is_deterministic_and_param_sensitive() {
        let base = move_key(&FED_A, &FED_B, Msat(50_000), Msat(2_000), Occurrence(0));
        // Same inputs -> same key: a re-run of the same move dedups (no re-mint / no re-pay).
        assert_eq!(
            base,
            move_key(&FED_A, &FED_B, Msat(50_000), Msat(2_000), Occurrence(0))
        );
        // Every parameter participates, so a genuinely different move gets a distinct key.
        assert_ne!(
            base,
            move_key(&FED_B, &FED_B, Msat(50_000), Msat(2_000), Occurrence(0)),
            "swapping the source federation must change the key"
        );
        assert_ne!(
            base,
            move_key(&FED_A, &FED_A, Msat(50_000), Msat(2_000), Occurrence(0)),
            "changing the destination must change the key"
        );
        assert_ne!(
            base,
            move_key(&FED_A, &FED_B, Msat(50_001), Msat(2_000), Occurrence(0)),
            "a different amount must not dedup to the old move"
        );
        assert_ne!(
            base,
            move_key(&FED_A, &FED_B, Msat(50_000), Msat(2_001), Occurrence(0))
        );
        assert_ne!(
            base,
            move_key(&FED_A, &FED_B, Msat(50_000), Msat(2_000), Occurrence(1))
        );
        // The key embeds both federation hexes + the three numeric params, in order.
        assert_eq!(
            base.0,
            format!("move:{}:{}:50000:2000:0", FED_A.to_hex(), FED_B.to_hex())
        );
    }

    #[tokio::test]
    async fn await_move_done_retry_honors_expected_fed() {
        let (runtime, journal) = runtime_fixture().await;
        let key = IdempotencyKey("done-direct-inflow".into());
        journal
            .upsert(&direct_inflow_intent(
                key.clone(),
                FED_A,
                // The move-record fence deliberately rejects creation under a
                // terminal intent.  Model the real ordering: persist the
                // settled record while the receive is Awaiting, then make the
                // matching terminal transition.
                IntentStatus::Awaiting,
            ))
            .await
            .expect("upsert intent");
        journal
            .put_move_if_attempt(
                &key,
                0,
                &direct_inflow_record(key.clone(), FED_A, MovePhase::Settled, None),
            )
            .await
            .expect("put move");
        journal
            .set_status(&key, 0, IntentStatus::Done, None)
            .await
            .expect("terminalize intent");

        let err = runtime
            .await_move(&key, Some(FED_B))
            .await
            .expect_err("wrong fed guard must fail");
        assert!(err.to_string().contains("receives into"));
        assert_eq!(
            runtime.await_move(&key, Some(FED_A)).await.expect("done"),
            FinalizeOutcome::Done
        );
    }

    #[tokio::test]
    async fn await_move_failed_retry_returns_failed_status() {
        let (runtime, journal) = runtime_fixture().await;
        let key = IdempotencyKey("failed-direct-inflow".into());
        journal
            .upsert(&direct_inflow_intent(
                key.clone(),
                FED_A,
                // As above, a real failure writes its record before the
                // expected-attempt terminalization.
                IntentStatus::Awaiting,
            ))
            .await
            .expect("upsert intent");
        journal
            .put_move_if_attempt(
                &key,
                0,
                &direct_inflow_record(
                    key.clone(),
                    FED_A,
                    MovePhase::Failed,
                    Some("receive invoice expired before payment"),
                ),
            )
            .await
            .expect("put move");
        journal
            .set_status(
                &key,
                0,
                IntentStatus::Failed,
                Some("receive invoice expired before payment"),
            )
            .await
            .expect("terminalize intent");

        assert_eq!(
            runtime.await_move(&key, None).await.expect("failed retry"),
            FinalizeOutcome::Failed("receive invoice expired before payment".into())
        );
    }

    #[tokio::test]
    async fn direct_inflow_repairs_awaiting_over_failed_record_and_hides_dead_invoice() {
        let (runtime, journal) = runtime_fixture().await;
        let to = FED_A;
        let amount = Msat(100_000);
        let fee_cap = Msat(1_000);
        let occurrence = Occurrence(0);
        let key = direct_inflow_key(&to, amount, fee_cap, occurrence);

        // Simulate a crash inside `await_move`: the record was written `Failed` (its invoice now
        // dead) but the intent CAS to `Failed` never landed, leaving the intent stuck `Awaiting`.
        journal
            .upsert(&direct_inflow_intent(
                key.clone(),
                to,
                IntentStatus::Awaiting,
            ))
            .await
            .expect("upsert intent");
        journal
            .put_move_if_attempt(
                &key,
                0,
                &direct_inflow_record(
                    key.clone(),
                    to,
                    MovePhase::Failed,
                    Some("receive invoice expired before payment"),
                ),
            )
            .await
            .expect("put move");

        let outcome = runtime
            .direct_inflow(to, amount, fee_cap, occurrence)
            .await
            .expect("direct_inflow");

        // The stuck `Awaiting` intent is repaired to `Failed`, so the CLI (which gates stdout on a
        // non-`Failed` status) never surfaces the dead invoice as payable.
        assert_eq!(outcome.status, Some(IntentStatus::Failed));
        assert_eq!(
            journal.get(&key).await.expect("get").map(|i| i.status),
            Some(IntentStatus::Failed)
        );
    }

    #[tokio::test]
    async fn tick_bails_when_a_pinned_fed_cannot_be_probed() {
        // The fixture has NO joined federations, so `probe_all` yields an empty batch and any
        // pinned fed is necessarily absent from the snapshot. A tick pinning a spending fed must
        // therefore fail LOUDLY (so a scheduler gating on the exit code never mistakes an
        // un-evaluated, explicitly-pinned rebalance for success) rather than report `decisions:
        // none` and exit 0. An UNPINNED (fully auto) tick over the same empty batch is a no-op, not
        // an error — auto designation degrades safely.
        let (runtime, _journal) = runtime_fixture().await;
        let pinned = TickPolicy {
            spending_fed: Some(FED_A),
            ..TickPolicy::default()
        };
        let err = runtime
            .tick(&pinned)
            .await
            .expect_err("a pinned fed that cannot be probed must fail the tick");
        assert!(err.to_string().contains("failed to probe"), "{err}");

        let report = runtime
            .tick(&TickPolicy::default())
            .await
            .expect("an all-auto tick over an empty fed set is a clean no-op");
        assert!(report.decisions.is_empty());
    }

    #[tokio::test]
    async fn watch_once_advances_occurrence_and_persists_discovery_checkpoint() {
        let (runtime, journal) = runtime_fixture().await;
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let tick_policy = TickPolicy::default();
        let watch_policy = due_discovery_watch_policy();
        let discovery_policy = DiscoveryPolicy::default();

        let first = runtime
            .watch_once(
                &tick_policy,
                &watch_policy,
                &sources,
                &discovery_policy,
                true,
            )
            .await
            .expect("first watch cycle");
        let second = runtime
            .watch_once(
                &tick_policy,
                &watch_policy,
                &sources,
                &discovery_policy,
                true,
            )
            .await
            .expect("second watch cycle");

        assert_eq!(first.occurrence, Occurrence(1));
        assert_eq!(second.occurrence, Occurrence(2));
        assert!(matches!(first.tick, WatchTickOutcome::Ran(_)));
        assert!(matches!(second.tick, WatchTickOutcome::Ran(_)));
        let WatchDiscoverOutcome::Ran(discover) = first.discover else {
            panic!("due discovery should run");
        };
        assert!(discover.progress.wrapped);
        assert!(!discover.progress.backlog);
        let state = journal.get_watch_state().await.expect("watch state");
        assert_eq!(state.occurrence, 2);
        assert!(state.last_discover_ms > 0);
        assert_eq!(state.discover_cursor, None);
        assert!(!state.discover_backlog);
        assert!(state.discover_rotation.is_empty());
    }

    #[tokio::test]
    async fn standalone_tick_occurrence_advances_the_next_daemon_watch_cycle() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_watch_state(&WatchState {
                occurrence: 5,
                ..WatchState::default()
            })
            .await
            .expect("seed older daemon checkpoint");
        runtime
            .tick(&TickPolicy {
                occurrence: Occurrence(41),
                ..TickPolicy::default()
            })
            .await
            .expect("standalone no-op tick");

        assert_eq!(
            journal
                .observe_watch_occurrence(4)
                .await
                .expect("a lower standalone occurrence cannot rewind the checkpoint")
                .occurrence,
            41
        );
        assert_eq!(
            journal
                .advance_watch_occurrence()
                .await
                .expect("daemon next occurrence")
                .occurrence,
            42,
            "a restarted daemon must never reuse a standalone occurrence below its marked parent"
        );
    }

    #[tokio::test]
    async fn standalone_tick_refuses_max_before_writing_watch_state_or_tick() {
        let (runtime, journal) = runtime_fixture().await;
        let checkpoint = WatchState {
            occurrence: 41,
            last_discover_ms: 42,
            ..WatchState::default()
        };
        journal
            .put_watch_state(&checkpoint)
            .await
            .expect("seed checkpoint");

        let error = runtime
            .tick(&TickPolicy {
                occurrence: Occurrence(u64::MAX),
                ..TickPolicy::default()
            })
            .await
            .expect_err("a standalone MAX tick has no possible successor");
        assert!(
            error
                .to_string()
                .contains("occurrence exhausted at u64::MAX"),
            "{error}"
        );
        assert_eq!(
            journal.get_watch_state().await.expect("read checkpoint"),
            checkpoint,
            "runtime must reject before observing the standalone occurrence"
        );
        assert!(
            journal
                .history(usize::MAX, None)
                .await
                .expect("read ledger")
                .is_empty(),
            "runtime must reject before opening a tick ledger row"
        );
    }

    #[tokio::test]
    async fn standalone_status_refuses_max_without_writing_watch_state_or_tick() {
        let (runtime, journal) = runtime_fixture().await;
        let checkpoint = WatchState {
            occurrence: 51,
            last_discover_ms: 52,
            ..WatchState::default()
        };
        journal
            .put_watch_state(&checkpoint)
            .await
            .expect("seed checkpoint");

        let error = runtime
            .status(&TickPolicy {
                occurrence: Occurrence(u64::MAX),
                ..TickPolicy::default()
            })
            .await
            .expect_err("status must reject the same exhausted occurrence as tick");
        assert!(
            error
                .to_string()
                .contains("occurrence exhausted at u64::MAX"),
            "{error}"
        );
        assert_eq!(
            journal.get_watch_state().await.expect("read checkpoint"),
            checkpoint,
            "status must reject before any watch-floor write"
        );
        assert!(
            journal
                .history(usize::MAX, None)
                .await
                .expect("read ledger")
                .is_empty(),
            "status must reject without opening a tick ledger row"
        );
    }

    #[tokio::test]
    async fn daemon_scheduler_status_and_final_tick_accept_max_once() {
        let (mut runtime, journal) = runtime_fixture().await;
        let checkpoint = WatchState {
            occurrence: u64::MAX - 1,
            ..WatchState::default()
        };
        journal
            .put_watch_state(&checkpoint)
            .await
            .expect("seed final scheduler floor");
        let policy = TickPolicy {
            occurrence: Occurrence(u64::MAX),
            ..TickPolicy::default()
        };
        let mut normal = tick_move_decision("move:final-scheduler-work", FED_A, FED_B);
        normal.occurrence = Occurrence(u64::MAX);
        let evidence = marked_evacuation_evidence();
        let parent = Intent::from_decision(
            &tick_evacuate_decision("evac:final-scheduler-parent", FED_A, FED_B),
            Actor::Agent {
                occurrence: Occurrence(u64::MAX - 1),
            },
            1,
        );
        let mut normal_plan =
            standalone_replacement_plan(parent.clone(), evidence.clone(), normal.clone());
        normal_plan.replacement = None;
        normal_plan.decisions = vec![normal.clone()];
        let executor = Arc::new(wallet_core::MockExecutor::new());
        runtime.set_tick_test_fixture(executor.clone(), normal_plan.clone());

        let status = runtime
            .status_for_daemon_scheduler(&policy)
            .await
            .expect("the final daemon-scheduler occurrence is valid dry-run work");
        assert_eq!(status.decisions, vec![normal.clone()]);
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read dry-run checkpoint"),
            checkpoint,
            "the daemon status dry-run must not allocate or write"
        );
        assert!(
            runtime.status(&policy).await.is_err(),
            "the standalone status contract still rejects MAX"
        );

        let mut marked_child =
            tick_evacuate_decision("evac:final-scheduler-replacement", FED_A, FED_B);
        marked_child.occurrence = Occurrence(u64::MAX);
        runtime.set_tick_test_fixture(
            executor.clone(),
            standalone_replacement_plan(parent.clone(), evidence.clone(), marked_child.clone()),
        );
        let marked_status = runtime
            .status_for_daemon_scheduler(&policy)
            .await
            .expect("a MAX child remains strictly newer than its MAX-1 marked parent");
        assert_eq!(marked_status.decisions, vec![marked_child.clone()]);

        let mut stale_child = marked_child.clone();
        stale_child.occurrence = Occurrence(u64::MAX - 1);
        runtime.set_tick_test_fixture(
            executor.clone(),
            standalone_replacement_plan(parent, evidence, stale_child),
        );
        let stale_error = runtime
            .status_for_daemon_scheduler(&policy)
            .await
            .expect_err("daemon status retains strict marked-replacement authority");
        assert!(
            stale_error
                .to_string()
                .contains("requires --occurrence advanced beyond old Agent occurrence"),
            "{stale_error}"
        );

        runtime.set_tick_test_fixture(executor.clone(), normal_plan);
        let report = runtime
            .watch_once(
                &TickPolicy::default(),
                &WatchPolicy::default(),
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("the scheduler executes its one final allocated occurrence");
        assert_eq!(report.occurrence, Occurrence(u64::MAX));
        let WatchTickOutcome::Ran(tick) = report.tick else {
            panic!("final scheduler work must run: {:?}", report.tick);
        };
        assert_eq!(tick.decisions, vec![normal.clone()]);
        assert_eq!(
            executor.performed_keys(),
            vec![normal.idempotency_key.clone()]
        );
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read final checkpoint")
                .occurrence,
            u64::MAX
        );
        let error = runtime
            .watch_once(
                &TickPolicy::default(),
                &WatchPolicy::default(),
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect_err("a scheduler cycle after its final occurrence is exhausted");
        assert!(
            error
                .to_string()
                .contains("watch scheduler occurrence exhausted"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn watch_once_records_tick_error_but_still_runs_due_discovery() {
        let (runtime, _journal) = runtime_fixture().await;
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            ..TickPolicy::default()
        };

        let report = runtime
            .watch_once(
                &tick_policy,
                &due_discovery_watch_policy(),
                &sources,
                &DiscoveryPolicy::default(),
                true,
            )
            .await
            .expect("watch cycle continues after tick failure");

        assert!(matches!(report.reconcile, WatchReconcileOutcome::Ran(_)));
        assert!(matches!(report.tick, WatchTickOutcome::Failed(_)));
        assert!(matches!(report.discover, WatchDiscoverOutcome::Ran(_)));
        assert_eq!(report.occurrence, Occurrence(1));
    }

    /// `status` reports the blocker view captured while planning, but `tick` must validate after
    /// its final durable re-scan. A stale planning holder must not hide a currently-unheld raw pin.
    #[tokio::test]
    async fn tick_uses_fresh_blockers_for_pin_validation_after_planning() {
        let (mut runtime, _) = runtime_fixture().await;
        let stale = tick_move_decision("move:stale-a-b", FED_A, FED_B);
        let mut stale_blockers = GoalBlockers::default();
        stale_blockers.hold_decision(
            &stale,
            Actor::Agent {
                occurrence: Occurrence(0),
            },
        );
        let mut raw_a = raw_probe_with_expiry(false, None, None);
        raw_a.gateway_available = false;
        runtime.set_tick_test_fixture(
            Arc::new(wallet_core::MockExecutor::new()),
            TickPlan {
                deferred: vec![],
                raw_probes: vec![],
                probes: vec![(FED_A, raw_a)],
                active_probes: BTreeMap::new(),
                snapshot: AllocatorSnapshot {
                    federations: vec![wallet_core::FederationStatus {
                        id: FED_A,
                        balance: wallet_core::FedBalance {
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
                    }],
                    spending_fed: Some(FED_A),
                    standby_fed: None,
                    per_fed_cap: Msat(1_000_000),
                    target_spending_balance: Msat(0),
                    standby_target: Msat(0),
                    max_fee: Msat(1_000),
                    max_fee_bps_of_move: 100,
                    evac_fee_base_msat: Msat(0),
                    evac_fee_bps: 100,
                    min_move: Msat(5_000),
                    route_economics_by_pair: BTreeMap::new(),
                    reservations: wallet_core::Reservations::default(),
                    now: 1,
                },
                decisions: vec![],
                suppressed: vec![],
                replacement_deferred: vec![],
                blockers: stale_blockers,
                replacement: None,
                marker_disposition: None,
            },
        );

        let error = runtime
            .tick(&TickPolicy {
                spending_fed: Some(FED_A),
                ..TickPolicy::default()
            })
            .await
            .expect_err("the fresh empty scan must not inherit the stale plan's pin relaxation");
        assert!(
            error.to_string().contains("failed the lnv2/probe gate")
                && error.to_string().contains(&FED_A.to_hex()),
            "{error}"
        );
    }

    /// br-p93, STANDALONE path: a permanently retryable agent funding move no longer suppresses
    /// the whole cycle. The tick RUNS, withholds a fresh key for that goal, and commits an
    /// independent evacuation through the real `tick` apply/journal lifecycle. The preplanned
    /// round and mock executor replace only guardian I/O, which a unit fixture cannot provide.
    /// Fails against the old `summary.retryable > 0` global skip.
    #[tokio::test]
    async fn watch_once_runs_the_tick_and_suppresses_only_the_conflicting_goal() {
        let (mut runtime, journal) = runtime_fixture().await;
        let stuck = IdempotencyKey(format!("move:{}:{}:0", FED_A.to_hex(), FED_B.to_hex()));
        let decision = tick_move_decision(&stuck.0, FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(
                &decision,
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                0,
            ))
            .await
            .expect("seed pending move");
        let occurrence = Occurrence(1);
        let reissued = IdempotencyKey(format!(
            "move:{}:{}:{}",
            FED_A.to_hex(),
            FED_B.to_hex(),
            occurrence.0
        ));
        let mut reissued_decision = tick_move_decision(&reissued.0, FED_A, FED_B);
        reissued_decision.occurrence = occurrence;
        let independent = IdempotencyKey(format!(
            "evac:{}:{}:{}",
            FED_C.to_hex(),
            FED_A.to_hex(),
            occurrence.0
        ));
        let mut independent_decision = tick_evacuate_decision(&independent.0, FED_C, FED_A);
        independent_decision.occurrence = occurrence;
        let snapshot = AllocatorSnapshot {
            federations: vec![
                wallet_core::FederationStatus {
                    id: FED_A,
                    balance: wallet_core::FedBalance {
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
                },
                wallet_core::FederationStatus {
                    id: FED_B,
                    balance: wallet_core::FedBalance {
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
                },
                wallet_core::FederationStatus {
                    id: FED_C,
                    balance: wallet_core::FedBalance {
                        spendable: Msat(100_000),
                        in_flight: Msat(0),
                        claimable: Msat(0),
                        reserved_fee: Msat(0),
                    },
                    probed_ok: true,
                    reputation: 0,
                    shutdown_notice: true,
                    healthy: false,
                    eligible_to_fund: false,
                },
            ],
            spending_fed: Some(FED_A),
            standby_fed: Some(FED_B),
            per_fed_cap: Msat(1_000_000),
            target_spending_balance: Msat(0),
            standby_target: Msat(0),
            max_fee: Msat(1_000),
            max_fee_bps_of_move: 100,
            evac_fee_base_msat: Msat(0),
            evac_fee_bps: 100,
            min_move: Msat(5_000),
            route_economics_by_pair: BTreeMap::new(),
            reservations: wallet_core::Reservations::default(),
            now: 1,
        };
        let executor = Arc::new(wallet_core::MockExecutor::new());
        executor.fail_retryable(&stuck.0);
        let mut unavailable_a_probe = raw_probe_with_expiry(false, None, None);
        unavailable_a_probe.gateway_available = false;
        runtime.set_tick_test_fixture(
            executor.clone(),
            TickPlan {
                deferred: vec![],
                raw_probes: vec![],
                // FED_A is pinned but fails its own coarse gateway probe. Its only rebalance is
                // moved into `suppressed` by `tick`'s fresh re-scan; pin checking receives that
                // non-preflighted work separately from the retained `decisions`.
                probes: vec![(FED_A, unavailable_a_probe)],
                active_probes: BTreeMap::new(),
                snapshot,
                decisions: vec![reissued_decision.clone(), independent_decision.clone()],
                suppressed: vec![],
                replacement_deferred: vec![],
                blockers: GoalBlockers::default(),
                replacement: None,
                marker_disposition: None,
            },
        );
        let sources: Vec<Box<dyn CandidateSource>> = Vec::new();

        let report = runtime
            .watch_once(
                &TickPolicy {
                    per_fed_cap: Msat(1_000_000),
                    spending_fed: Some(FED_A),
                    ..TickPolicy::default()
                },
                &due_discovery_watch_policy(),
                &sources,
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("watch cycle continues after retryable reconcile");

        let WatchReconcileOutcome::Ran(summary) = &report.reconcile else {
            panic!("reconcile ran: {:?}", report.reconcile);
        };
        assert_eq!(summary.retryable, 1, "the stuck goal is still retryable");
        // The eligibility the tick is projected through: this goal, and only this goal.
        assert_eq!(
            summary.blocked.goals(),
            BTreeSet::from([wallet_core::AllocatorGoal::FundInto(FED_B)]),
            "reconcile projects the one in-flight goal"
        );
        let WatchTickOutcome::Ran(tick) = &report.tick else {
            panic!(
                "retryable pending work must not skip the tick wallet-wide: {:?}",
                report.tick
            );
        };
        assert_eq!(tick.decisions, vec![independent_decision]);
        assert_eq!(tick.summary.performed, 1, "the independent goal commits");
        assert!(matches!(&report.discover, WatchDiscoverOutcome::Disabled));
        assert_eq!(report.occurrence, occurrence);

        let rows = journal.history(usize::MAX, None).await.expect("history");
        let ticks = rows
            .iter()
            .filter(|row| matches!(row.kind, OperationKind::Tick { .. }))
            .collect::<Vec<_>>();
        assert_eq!(
            ticks.len(),
            1,
            "the cycle starts exactly one fresh tick row: {rows:#?}"
        );
        assert!(
            ticks[0].status.is_terminal(),
            "the fresh tick row is terminalized: {:?}",
            ticks[0]
        );

        // The blocked goal is not re-issued: the only intent for it remains the stuck one.
        let pending = journal.pending().await.expect("pending");
        assert_eq!(
            pending
                .iter()
                .map(|intent| intent.idempotency_key.clone())
                .collect::<Vec<_>>(),
            vec![stuck.clone()],
            "no fresh-occurrence intent was created for the suppressed goal"
        );
        assert!(
            journal
                .get(&reissued)
                .await
                .expect("read the fresh blocked key")
                .is_none(),
            "the old logical goal is not re-issued under the fresh occurrence"
        );
        let tick_drop = IdempotencyKey(format!("tick-drop:{}:{}", occurrence.0, reissued.0));
        assert!(matches!(
            journal
                .operation(&crate::journal::OperationRef::Key(tick_drop))
                .await
                .expect("read the final-rescan tick-drop refusal")
                .expect("Runtime::tick persists the newly conflicting decision")
                .kind,
            OperationKind::Refusal { diagnostics, .. }
                if diagnostics.amount == Some(Msat(0)) && diagnostics.conflict_suppressed
        ));
        assert_eq!(
            journal
                .get(&independent)
                .await
                .expect("read the independent intent")
                .map(|intent| intent.status),
            Some(IntentStatus::Done),
            "the independent goal is durably committed and performed"
        );
        assert_eq!(
            executor.performed_keys(),
            vec![independent],
            "the mock records only the independent goal's successful perform"
        );
    }

    /// br-p93, the standalone derivation `plan_tick` and `tick` both re-run: it reads the durable
    /// `pending()` scan, so a live AGENT evacuation holds its goal while a user operation
    /// journaled beside it holds nothing — a user retry must never be suppressed because the
    /// agent has similar work in flight, and the daemon's reconcile projects from the same scan.
    #[tokio::test]
    async fn the_standalone_blocker_projection_reads_durable_agent_work_only() {
        let (runtime, journal) = runtime_fixture().await;
        assert!(
            runtime
                .allocator_goal_blockers()
                .await
                .expect("an empty journal projects an empty set")
                .is_empty(),
            "nothing in flight blocks nothing"
        );

        let evacuation = tick_evacuate_decision("evac:standalone", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(
                &evacuation,
                Actor::Agent {
                    occurrence: Occurrence(3),
                },
                0,
            ))
            .await
            .expect("seed the agent evacuation");
        let user_move = tick_move_decision("move:user", FED_B, FED_C);
        journal
            .upsert(&Intent::from_decision(&user_move, Actor::User, 0))
            .await
            .expect("seed the user move");

        let blocked = runtime
            .allocator_goal_blockers()
            .await
            .expect("project the durable scan");
        assert_eq!(
            blocked.goals(),
            BTreeSet::from([wallet_core::AllocatorGoal::Evacuate(FED_A)]),
            "only the agent's own allocator work holds a goal"
        );
        assert_eq!(
            blocked
                .holder(wallet_core::AllocatorGoal::Evacuate(FED_A))
                .map(|key| key.0.as_str()),
            Some("evac:standalone"),
            "the holding key is the durable intent's own"
        );
    }

    /// The standalone raw-probe seam retains planning blockers for pin validation. A held A -> B
    /// evacuation suppresses its current nonzero recurrence but co-emits a refusal; C -> B must
    /// still remain eligible.
    #[tokio::test]
    async fn standalone_planner_derives_blockers_before_reserving_evacuation_room() {
        let (mut runtime, journal) = runtime_fixture().await;
        let stuck = AllocatorDecision {
            action: Action::Evacuate {
                from: FED_A,
                to: FED_B,
                amount: Msat(400_000),
                fee_cap: Msat(0),
                gateway: None,
                fee_cap_components: None,
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence: Occurrence(10),
            idempotency_key: IdempotencyKey("evac:standalone-held-a".to_owned()),
        };
        journal
            .upsert(&Intent::from_decision(
                &stuck,
                Actor::Agent {
                    occurrence: Occurrence(10),
                },
                0,
            ))
            .await
            .expect("seed durable A evacuation");
        // This in-memory Runtime has no open federations/gateway to validate. A terminal record
        // under C's prospective key is not a blocker, but makes normal concrete preflight take
        // its existing-intent path so this test reaches the raw-probe planner's durable-blocker
        // projection rather than its unrelated no-gateway revision loop.
        let independent_key = IdempotencyKey(format!(
            "evac:{}:{}:{}",
            FED_C.to_hex(),
            FED_B.to_hex(),
            Occurrence(11).0
        ));
        let independent = AllocatorDecision {
            action: Action::Evacuate {
                from: FED_C,
                to: FED_B,
                amount: Msat(500_000),
                fee_cap: Msat(0),
                gateway: None,
                fee_cap_components: None,
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence: Occurrence(11),
            idempotency_key: independent_key.clone(),
        };
        journal
            .upsert(&Intent::from_decision(
                &independent,
                Actor::Agent {
                    occurrence: Occurrence(11),
                },
                0,
            ))
            .await
            .expect("seed terminal C key for route-preflight isolation");
        journal
            .set_status(&independent_key, 0, IntentStatus::Done, None)
            .await
            .expect("terminalize C key");
        let policy = TickPolicy {
            per_fed_cap: Msat(1_000_000),
            target_spending_balance: Msat(0),
            standby_target: Msat(0),
            spending_fed: Some(FED_A),
            standby_fed: Some(FED_B),
            occurrence: Occurrence(11),
            ..TickPolicy::default()
        };
        let mut dying_a = raw_probe_with_expiry(true, None, None);
        dying_a.spendable_msat = 0;
        // A is a source-only pin with no coarse gateway of its own. Its current evacuation is
        // conflict-suppressed, so only the stored planning blocker can relax this raw pin.
        dying_a.gateway_available = false;
        dying_a.spendable_msat = 500_000;
        let mut healthy_b = raw_probe_with_expiry(false, None, None);
        healthy_b.spendable_msat = 0;
        let mut dying_c = raw_probe_with_expiry(true, None, None);
        dying_c.spendable_msat = 500_000;

        let plan = runtime
            .plan_tick_from_probes(
                &policy,
                vec![(FED_A, dying_a), (FED_B, healthy_b), (FED_C, dying_c)],
            )
            .await
            .expect("standalone plan with one held evacuation and one independent evacuation");

        assert!(
            !plan.decisions.iter().any(
                |decision| matches!(decision.action, Action::Evacuate { from, .. } if from == FED_A)
            ),
            "held A must produce no fresh executable recurrence: {:#?}",
            plan.decisions
        );
        let refusal = plan
            .decisions
            .iter()
            .find(|decision| {
                matches!(
                    decision.action,
                    Action::RefuseInflow {
                        fed,
                        reason: ReasonCode::ShutdownNotice,
                        diagnostics,
                    } if fed == FED_A
                        && diagnostics.available == Some(Msat(100_000))
                        && diagnostics.amount == Some(Msat(0))
                        && diagnostics.conflict_suppressed
                )
            })
            .cloned()
            .expect("standalone decision list retains the withheld evacuation's refusal");
        let candidate = plan
            .suppressed
            .iter()
            .find(|decision| matches!(decision.action, Action::Evacuate { from, to, .. } if from == FED_A && to == FED_B))
            .expect("the exact withheld evacuation candidate");
        assert_eq!(
            refusal.idempotency_key.0,
            format!(
                "conflict-suppressed:{}:shutdown_notice",
                candidate.idempotency_key.0
            )
        );
        assert!(
            plan.decisions.iter().any(|decision| matches!(
                decision.action,
                Action::Evacuate {
                    from,
                    to,
                    amount: Msat(500_000),
                    ..
                } if from == FED_C && to == FED_B
            )),
            "the independent C evacuation must retain B's 500k room: {:#?}",
            plan.decisions
        );
        assert!(
            Runtime::pinned_input_problems(&policy, &plan, &plan.blockers).is_empty(),
            "the paired held A evacuation advisory vouches for its unusable source-only pin"
        );
        // Exercise Runtime::tick rather than the journal helper directly: the standalone tick
        // must carry the planner's advisory decision through its own history lifecycle.
        runtime.set_tick_test_fixture(
            Arc::new(wallet_core::MockExecutor::new()),
            TickPlan {
                decisions: vec![refusal.clone()],
                ..plan
            },
        );
        runtime
            .tick(&policy)
            .await
            .expect("standalone tick records its suppression refusal");
        assert!(matches!(
            journal
                .operation(&crate::journal::OperationRef::Key(refusal.idempotency_key))
                .await
                .expect("read standalone suppression refusal")
                .expect("Runtime::tick records the standalone decision list")
                .kind,
            OperationKind::Refusal { diagnostics, .. }
                if diagnostics.amount == Some(Msat(0)) && diagnostics.conflict_suppressed
        ));
    }

    #[tokio::test]
    async fn watch_once_records_budget_exhausted_probe_skip() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        journal
            .record_started(
                &IdempotencyKey("probe-budget-row".to_owned()),
                OperationKind::Probe {
                    fed: FED_C,
                    from: FED_A,
                    amount_msat: Msat(20_000),
                    cost_msat: Some(Msat(1)),
                },
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                ReasonCode::ActiveProbe,
                now_ms(),
                None,
            )
            .await
            .expect("seed budget row");
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            ..TickPolicy::default()
        };
        let watch_policy = WatchPolicy {
            probe_budget: ProbeBudget {
                max_probe_attempts_per_week: 1,
                max_probe_spend_per_week_msat: 1_000,
            },
            ..WatchPolicy::default()
        };

        let report = runtime
            .watch_once(
                &tick_policy,
                &watch_policy,
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("watch cycle");

        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].fed, FED_B);
        assert_eq!(report.probes[0].verdict, ActiveProbeVerdict::NeverProbed);
        assert_eq!(report.probes[0].outcome, WatchProbeOutcome::BudgetBlocked);
        assert_eq!(report.budget_usage.attempts, 1);
        runtime
            .watch_once(
                &tick_policy,
                &watch_policy,
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("second watch cycle");
        let rows = journal.history(usize::MAX, None).await.expect("history");
        let skip_rows = rows
            .iter()
            .filter(|row| {
                row.reason == ReasonCode::StandingInstruction
                    && row.status == OperationStatus::Failed
                    && matches!(row.kind, OperationKind::Probe { fed, cost_msat: None, .. } if fed == FED_B)
            })
            .count();
        assert_eq!(
            skip_rows, 1,
            "budget-blocked probe diagnostics are idempotent within the same budget bucket"
        );
        assert!(rows.iter().any(|row| {
            row.reason == ReasonCode::StandingInstruction
                && row.status == OperationStatus::Failed
                && matches!(row.kind, OperationKind::Probe { fed, cost_msat: None, .. } if fed == FED_B)
        }));
    }

    #[tokio::test]
    async fn watch_once_resumes_post_in_probe_when_budget_is_exhausted() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy::default();
        let in_key = seed_post_in_probe_session(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        journal
            .record_started(
                &IdempotencyKey("probe-budget-row-for-resume".to_owned()),
                OperationKind::Probe {
                    fed: FED_C,
                    from: FED_A,
                    amount_msat: Msat(20_000),
                    cost_msat: Some(Msat(1)),
                },
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                ReasonCode::ActiveProbe,
                now_ms(),
                None,
            )
            .await
            .expect("seed budget row");
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };
        let watch_policy = WatchPolicy {
            probe_budget: ProbeBudget {
                max_probe_attempts_per_week: 1,
                max_probe_spend_per_week_msat: 1_000,
            },
            ..WatchPolicy::default()
        };

        let report = runtime
            .watch_once(
                &tick_policy,
                &watch_policy,
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("watch cycle");

        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].fed, FED_B);
        assert!(matches!(
            report.probes[0].outcome,
            WatchProbeOutcome::Failed(_)
        ));
        assert_ne!(report.probes[0].outcome, WatchProbeOutcome::BudgetBlocked);
        assert!(
            report.probes[0].due_ms <= now_ms(),
            "post-IN sessions are due immediately for cleanup"
        );
        let rows = journal.history(usize::MAX, None).await.expect("history");
        assert!(rows.iter().any(|row| {
            row.reason == ReasonCode::ActiveProbe
                && matches!(row.kind, OperationKind::Probe { fed, from, cost_msat: None, .. } if fed == FED_B && from == FED_A)
        }));
        assert!(journal
            .get(&in_key)
            .await
            .expect("leg-in intent remains readable")
            .is_some());
    }

    #[tokio::test]
    async fn watch_once_defers_fresh_probes_after_retained_in_flight_probe() {
        let (runtime, journal) = runtime_fixture().await;
        for fed in [FED_B, FED_C] {
            journal
                .put_federation(&fed, &federation_info())
                .await
                .expect("put auto-joined fed");
        }
        let gate_policy = ProbePolicy::default();
        seed_post_in_probe_session(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };

        let report = runtime
            .watch_once(
                &tick_policy,
                &WatchPolicy::default(),
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("watch cycle");

        assert_eq!(report.probes.len(), 2);
        assert_eq!(report.probes[0].fed, FED_B);
        assert!(matches!(
            report.probes[0].outcome,
            WatchProbeOutcome::Failed(_)
        ));
        assert_eq!(report.probes[1].fed, FED_C);
        assert_eq!(
            report.probes[1].outcome,
            WatchProbeOutcome::DeferredByInFlight
        );
        let rows = journal.history(usize::MAX, None).await.expect("history");
        assert!(
            !rows.iter().any(|row| {
                row.reason == ReasonCode::ActiveProbe
                    && matches!(row.kind, OperationKind::Probe { fed, .. } if fed == FED_C)
            }),
            "the second due candidate must not launch a fresh scheduled probe in the same cycle"
        );
    }

    #[tokio::test]
    async fn service_scheduler_defers_fresh_probes_while_resuming_a_session() {
        let (runtime, journal) = runtime_fixture().await;
        for fed in [FED_B, FED_C] {
            journal
                .put_federation(&fed, &federation_info())
                .await
                .expect("put auto-joined fed");
        }
        let gate_policy = ProbePolicy::default();
        seed_post_in_probe_session(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };
        let now = now_ms();

        let (due, resuming) = runtime
            .service_due_probes(
                Some(FED_A),
                &tick_policy,
                &WatchPolicy::default(),
                &BTreeMap::new(),
                None,
                now,
                Occurrence(1),
            )
            .await
            .expect("service probe schedule");

        assert_eq!(due.len(), 1);
        assert_eq!(due[0].federation, FED_B);
        assert_eq!(due[0].source, FED_A);
        assert_eq!(due[0].baseline, Msat(0));
        assert!(matches!(
            &due[0].admission,
            ProbeAdmission::ResumeOnly { expected_nonce }
                if expected_nonce == "00000000000000660000000000000000"
        ));
        assert!(resuming);
    }

    #[tokio::test]
    async fn service_scheduler_resumes_a_pre_in_session_after_the_budget_is_lowered() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy::default();
        seed_pre_leg_probe_session(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };
        let watch_policy = WatchPolicy {
            probe_budget: ProbeBudget {
                max_probe_attempts_per_week: 0,
                max_probe_spend_per_week_msat: 0,
            },
            ..WatchPolicy::default()
        };
        let now = now_ms();

        let (due, resuming) = runtime
            .service_due_probes(
                None,
                &tick_policy,
                &watch_policy,
                &BTreeMap::new(),
                None,
                now,
                Occurrence(1),
            )
            .await
            .expect("service probe schedule");

        assert_eq!(due.len(), 1);
        assert_eq!(due[0].federation, FED_B);
        assert_eq!(due[0].source, FED_A);
        assert!(matches!(
            &due[0].admission,
            ProbeAdmission::ResumeOnly { expected_nonce }
                if expected_nonce == "00000000000000770000000000000000"
        ));
        assert!(
            resuming,
            "an already-admitted session bypasses the later fresh budget gate"
        );
        let deadlines = runtime
            .service_watch_deadlines(
                &tick_policy,
                &watch_policy,
                now,
                &BTreeSet::new(),
                &BTreeSet::new(),
                false,
            )
            .await
            .expect("retained-session deadlines");
        assert!(
            deadlines.probe_due_ms.iter().any(|due_ms| *due_ms <= now),
            "the lowered fresh budget must not defer retained probe money"
        );
    }

    #[tokio::test]
    async fn service_deadlines_do_not_reschedule_an_actor_owned_post_in_probe() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy::default();
        seed_post_in_probe_session(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };
        let watch_policy = WatchPolicy::default();
        let now = now_ms();

        let standalone = runtime
            .watch_deadlines(&tick_policy, &watch_policy, now)
            .await
            .expect("standalone deadlines");
        assert!(
            standalone.probe_due_ms.iter().any(|due_ms| *due_ms <= now),
            "the synchronous 5.2 loop still resumes the retained session immediately"
        );

        let service = runtime
            .service_watch_deadlines(
                &tick_policy,
                &watch_policy,
                now,
                &BTreeSet::from([FED_B]),
                &BTreeSet::new(),
                true,
            )
            .await
            .expect("service deadlines");
        assert!(
            service.probe_due_ms.is_empty(),
            "an actor-owned post-IN session must not pin the daemon to its one-second floor"
        );
    }

    #[tokio::test]
    async fn service_deadlines_defer_due_probes_not_started_by_the_actor() {
        let (runtime, journal) = runtime_fixture().await;
        for fed in [FED_B, FED_C] {
            journal
                .put_federation(&fed, &federation_info())
                .await
                .expect("put auto-joined fed");
        }
        let gate_policy = ProbePolicy::default();
        seed_post_in_probe_session(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };
        let watch_policy = WatchPolicy::default();
        let now = now_ms();

        let while_resuming = runtime
            .service_watch_deadlines(
                &tick_policy,
                &watch_policy,
                now,
                &BTreeSet::from([FED_B]),
                &BTreeSet::new(),
                true,
            )
            .await
            .expect("service deadlines while resuming");
        assert!(while_resuming
            .probe_due_ms
            .iter()
            .all(|due_ms| *due_ms >= now.saturating_add(watch_policy.min_interval_ms)));

        let after_refusal = runtime
            .service_watch_deadlines(
                &tick_policy,
                &watch_policy,
                now,
                &BTreeSet::new(),
                &BTreeSet::from([FED_C]),
                false,
            )
            .await
            .expect("service deadlines after refusal");
        let retry_at = now.saturating_add(watch_policy.probe_retry_backoff_ms);
        assert!(after_refusal
            .probe_due_ms
            .iter()
            .any(|due_ms| *due_ms >= retry_at));
    }

    #[test]
    fn fresh_service_probe_requires_a_sample_for_an_open_candidate() {
        assert_eq!(fresh_probe_baseline(true, None), None);
        assert_eq!(fresh_probe_baseline(true, Some(Msat(42))), Some(Msat(42)));
        assert_eq!(fresh_probe_baseline(false, None), Some(Msat(0)));
    }

    #[tokio::test]
    async fn service_due_probes_requires_current_spending_for_fresh_work_but_resumes_durable_source(
    ) {
        use fedimint_core::invite_code::InviteCode;
        use fedimint_core::util::SafeUrl;
        use fedimint_core::PeerId;
        use std::str::FromStr as _;

        fn auto_joined_candidate(id: FederationId) -> crate::CandidateRecord {
            let fed_id = fedimint_core::config::FederationId::from_str(&id.to_hex())
                .expect("valid federation id");
            crate::CandidateRecord {
                id,
                invite: InviteCode::new(
                    SafeUrl::parse("https://service-due-probes.example").expect("valid URL"),
                    PeerId::from(0),
                    fed_id,
                    None,
                ),
                source: wallet_core::DiscoverySource::Manual,
                discovered_at_ms: 0,
                structural: crate::StructuralOutcome::Passed,
                structural_checked_at_ms: 0,
                state: CandidateState::AutoJoined,
                updated_at_ms: 0,
            }
        }

        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("seed fresh auto-joined membership");
        journal
            .put_candidate(&auto_joined_candidate(FED_B))
            .await
            .expect("seed fresh auto-joined candidate");
        let gate_policy = ProbePolicy::default();
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy.clone(),
            ..TickPolicy::default()
        };
        let now = now_ms();
        let (fresh_due, fresh_resuming) = runtime
            .service_due_probes(
                None,
                &tick_policy,
                &WatchPolicy::default(),
                &BTreeMap::from([(FED_B, Msat(19))]),
                None,
                now,
                Occurrence(1),
            )
            .await
            .expect("service probe schedule");
        assert!(
            fresh_due.is_empty(),
            "a degraded cycle must not use tick_policy.spending_fed to launch fresh FED_B work"
        );
        assert!(!fresh_resuming);

        journal
            .put_federation(&FED_C, &federation_info())
            .await
            .expect("seed retained auto-joined membership");
        journal
            .put_candidate(&auto_joined_candidate(FED_C))
            .await
            .expect("seed retained auto-joined candidate");
        let retained = ProbeSession {
            nonce: "00000000000000DD0000000000000000".to_owned(),
            from: FED_D,
            amount_msat: gate_policy.amount_msat,
            leg_fee_cap_msat: gate_policy.leg_fee_cap_msat,
            c_spendable_before_in_msat: 73,
            out_net_msat: None,
            started_at_ms: now,
        };
        journal
            .begin_probe_session(&FED_C, &retained)
            .await
            .expect("seed retained probe session");
        let (due, resuming) = runtime
            .service_due_probes(
                None,
                &tick_policy,
                &WatchPolicy::default(),
                &BTreeMap::from([(FED_B, Msat(19))]),
                None,
                now,
                Occurrence(1),
            )
            .await
            .expect("service retained probe schedule");

        assert_eq!(due.len(), 1);
        assert_eq!(due[0].federation, FED_C);
        assert_eq!(due[0].source, FED_D);
        assert_eq!(due[0].baseline, Msat(73));
        assert!(matches!(
            &due[0].admission,
            ProbeAdmission::ResumeOnly { expected_nonce }
                if expected_nonce == &retained.nonce
        ));
        assert!(resuming, "the sole due probe is a retained session");
    }

    #[tokio::test]
    async fn watch_once_resumes_in_flight_probe_without_current_spending_fed() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy::default();
        seed_post_in_probe_session(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };

        let report = runtime
            .watch_once(
                &tick_policy,
                &WatchPolicy::default(),
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("watch cycle");

        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].fed, FED_B);
        assert!(matches!(
            report.probes[0].outcome,
            WatchProbeOutcome::Failed(_)
        ));
        let rows = journal.history(usize::MAX, None).await.expect("history");
        assert!(rows.iter().any(|row| {
            row.reason == ReasonCode::ActiveProbe
                && matches!(row.kind, OperationKind::Probe { fed, from, cost_msat: None, .. } if fed == FED_B && from == FED_A)
        }));
    }

    #[tokio::test]
    async fn watch_once_checks_in_flight_session_before_skipping_self_probe() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy::default();
        seed_post_in_probe_session(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_B),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };

        let report = runtime
            .watch_once(
                &tick_policy,
                &WatchPolicy::default(),
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("watch cycle");

        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].fed, FED_B);
        assert!(matches!(
            report.probes[0].outcome,
            WatchProbeOutcome::Failed(_)
        ));
    }

    #[tokio::test]
    async fn scheduled_probe_backoff_is_scoped_to_source_fed() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let now = now_ms();
        journal
            .record_probe_invocation(
                &IdempotencyKey("probe-other-source".to_owned()),
                OperationKind::Probe {
                    fed: FED_B,
                    from: FED_C,
                    amount_msat: Msat(20_000),
                    cost_msat: None,
                },
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                now,
            )
            .await
            .expect("seed other-source invocation");
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            ..TickPolicy::default()
        };

        let deadlines = runtime
            .watch_deadlines(&tick_policy, &WatchPolicy::default(), now)
            .await
            .expect("watch deadlines");

        assert_eq!(deadlines.probe_due_ms.len(), 1);
        assert!(
            deadlines.probe_due_ms[0] <= now,
            "a probe from FED_C must not back off FED_A's first scheduled probe"
        );
    }

    #[tokio::test]
    async fn watch_once_records_resumed_probe_backoff_under_session_source() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy::default();
        seed_pre_leg_probe_session(journal.as_ref(), FED_B, FED_C, &gate_policy).await;
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };

        let report = runtime
            .watch_once(
                &tick_policy,
                &WatchPolicy::default(),
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("watch cycle");

        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].fed, FED_B);
        assert!(matches!(
            report.probes[0].outcome,
            WatchProbeOutcome::Attempted
        ));
        assert_eq!(report.deadlines.probe_due_ms.len(), 1);
        assert!(
            report.deadlines.probe_due_ms[0] <= now_ms(),
            "resuming a FED_C session must not back off FED_A's first scheduled probe"
        );
        let rows = journal.history(usize::MAX, None).await.expect("history");
        assert!(rows.iter().any(|row| {
            row.reason == ReasonCode::ActiveProbe
                && matches!(row.kind, OperationKind::Probe { fed, from, cost_msat: None, .. } if fed == FED_B && from == FED_C)
        }));
    }

    #[tokio::test]
    async fn in_flight_probe_backoff_uses_session_source() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy::default();
        seed_pre_leg_probe_session(journal.as_ref(), FED_B, FED_C, &gate_policy).await;
        let now = now_ms();
        let watch_policy = WatchPolicy::default();
        journal
            .record_probe_invocation(
                &IdempotencyKey("probe-in-flight-source".to_owned()),
                OperationKind::Probe {
                    fed: FED_B,
                    from: FED_C,
                    amount_msat: Msat(20_000),
                    cost_msat: None,
                },
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                now,
            )
            .await
            .expect("seed session-source invocation");
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };

        let deadlines = runtime
            .watch_deadlines(&tick_policy, &watch_policy, now)
            .await
            .expect("watch deadlines");

        assert_eq!(deadlines.probe_due_ms.len(), 1);
        assert_eq!(
            deadlines.probe_due_ms[0],
            now.saturating_add(watch_policy.probe_retry_backoff_ms),
            "an in-flight FED_C session must not be scheduled as FED_A's first probe"
        );
    }

    #[tokio::test]
    async fn scheduled_probe_backoff_uses_touched_invocation_timestamp() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let watch_policy = WatchPolicy::default();
        let now = now_ms();
        let key = IdempotencyKey("probe-touched".to_owned());
        journal
            .record_started(
                &key,
                OperationKind::Probe {
                    fed: FED_B,
                    from: FED_A,
                    amount_msat: Msat(20_000),
                    cost_msat: None,
                },
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                ReasonCode::ActiveProbe,
                now.saturating_sub(watch_policy.probe_retry_backoff_ms * 2),
                None,
            )
            .await
            .expect("seed old invocation");
        journal
            .record_probe_invocation(
                &key,
                OperationKind::Probe {
                    fed: FED_B,
                    from: FED_A,
                    amount_msat: Msat(20_000),
                    cost_msat: None,
                },
                Actor::Agent {
                    occurrence: Occurrence(1),
                },
                now,
            )
            .await
            .expect("touch invocation");
        journal
            .record_started(
                &IdempotencyKey("probe-old-higher-seq".to_owned()),
                OperationKind::Probe {
                    fed: FED_C,
                    from: FED_A,
                    amount_msat: Msat(20_000),
                    cost_msat: None,
                },
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                ReasonCode::ActiveProbe,
                now.saturating_sub(watch_policy.probe_retry_backoff_ms * 2),
                None,
            )
            .await
            .expect("seed old higher-seq row");
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            ..TickPolicy::default()
        };

        let deadlines = runtime
            .watch_deadlines(&tick_policy, &watch_policy, now)
            .await
            .expect("watch deadlines");

        assert_eq!(deadlines.probe_due_ms.len(), 1);
        assert_eq!(
            deadlines.probe_due_ms[0],
            now.saturating_add(watch_policy.probe_retry_backoff_ms),
            "the scheduler should back off from the touched retry timestamp, not original creation"
        );
    }

    #[tokio::test]
    async fn probe_schedule_context_counts_only_windowed_budget_rows() {
        let (runtime, journal) = runtime_fixture().await;
        let now = now_ms();
        let old = now.saturating_sub(PROBE_BUDGET_WINDOW_MS + 1);
        for (key, created_at_ms) in [("old-probe-budget", old), ("new-probe-budget", now)] {
            journal
                .record_started(
                    &IdempotencyKey(key.to_owned()),
                    OperationKind::Probe {
                        fed: FED_B,
                        from: FED_A,
                        amount_msat: Msat(20_000),
                        cost_msat: Some(Msat(3)),
                    },
                    Actor::Agent {
                        occurrence: Occurrence(0),
                    },
                    ReasonCode::ActiveProbe,
                    created_at_ms,
                    None,
                )
                .await
                .expect("seed budget row");
        }

        let context = runtime
            .probe_schedule_context(now, &WatchPolicy::default())
            .await
            .expect("probe schedule context");

        assert_eq!(
            context.budget_usage,
            ProbeBudgetUsage {
                attempts: 1,
                spend_msat: 3
            }
        );
        assert_eq!(
            context.budget_reset_ms,
            Some(now.saturating_add(PROBE_BUDGET_WINDOW_MS))
        );
    }

    #[tokio::test]
    async fn probe_schedule_context_counts_resumed_probe_spend_at_updated_time() {
        let (runtime, journal) = runtime_fixture().await;
        let now = now_ms();
        let old = now.saturating_sub(PROBE_BUDGET_WINDOW_MS + 1);
        let key = IdempotencyKey("resumed-probe-budget".to_owned());
        journal
            .record_started(
                &key,
                OperationKind::Probe {
                    fed: FED_B,
                    from: FED_A,
                    amount_msat: Msat(20_000),
                    cost_msat: Some(Msat(7)),
                },
                Actor::Agent {
                    occurrence: Occurrence(0),
                },
                ReasonCode::ActiveProbe,
                old,
                None,
            )
            .await
            .expect("seed old probe row");
        journal
            .record_terminal(&key, OperationStatus::Succeeded, now, None, None)
            .await
            .expect("touch terminal probe row");

        let context = runtime
            .probe_schedule_context(now, &WatchPolicy::default())
            .await
            .expect("probe schedule context");

        assert_eq!(
            context.budget_usage,
            ProbeBudgetUsage {
                attempts: 1,
                spend_msat: 7
            }
        );
        assert_eq!(
            context.budget_reset_ms,
            Some(now.saturating_add(PROBE_BUDGET_WINDOW_MS))
        );
    }

    #[test]
    fn subscription_noop_treats_budget_blocked_probe_as_coalescible() {
        let report = WatchCycleReport {
            occurrence: Occurrence(1),
            reconcile: WatchReconcileOutcome::Ran(ReconcileSummary::default()),
            tick: WatchTickOutcome::Ran(TickReport {
                decisions: Vec::new(),
                summary: ExecutionSummary::default(),
                spending_fed: Some(FED_A),
                standby_fed: None,
            }),
            probes: vec![WatchProbeReport {
                fed: FED_B,
                verdict: ActiveProbeVerdict::NeverProbed,
                due_ms: 0,
                outcome: WatchProbeOutcome::BudgetBlocked,
            }],
            discover: WatchDiscoverOutcome::Disabled,
            budget_usage: ProbeBudgetUsage::default(),
            watch_state: WatchState::default(),
            deadlines: AdaptiveSleepDeadlines::default(),
        };

        assert!(report.subscription_noop());
    }

    #[test]
    fn shutdown_flag_without_expiry_does_not_add_busy_spin_deadline() {
        let now = 1_700_000_000_000;
        let mut no_expiry = AdaptiveSleepDeadlines::default();
        add_expiry_deadlines(
            &mut no_expiry,
            &[(FED_A, raw_probe_with_expiry(true, None, None))],
            now,
        );
        assert!(
            no_expiry.expiries_ms.is_empty(),
            "a shutdown boolean without a concrete expiry is not an adaptive deadline"
        );

        let mut with_expiry = AdaptiveSleepDeadlines::default();
        let expiry_secs = (now + 2 * 60 * 60 * 1000) / 1000;
        add_expiry_deadlines(
            &mut with_expiry,
            &[(FED_A, raw_probe_with_expiry(true, Some(expiry_secs), None))],
            now,
        );

        assert_eq!(with_expiry.expiries_ms, vec![expiry_secs * 1000]);
        assert_ne!(with_expiry.expiries_ms, vec![now]);

        let mut past_expiry = AdaptiveSleepDeadlines::default();
        add_expiry_deadlines(
            &mut past_expiry,
            &[(
                FED_A,
                raw_probe_with_expiry(true, Some((now - 1_000) / 1000), None),
            )],
            now,
        );
        assert!(
            past_expiry.expiries_ms.is_empty(),
            "past expiry timestamps must not pin the watch loop to the busy-spin floor"
        );
    }

    #[tokio::test]
    async fn wake_hint_deadline_reuse_keeps_probe_schedule_and_adds_hint_without_probe_scan() {
        let (runtime, _journal) = runtime_fixture().await;
        let now = now_ms();
        let previous = AdaptiveSleepDeadlines {
            last_discover_ms: now.saturating_sub(10_000),
            discover_backlog: false,
            expiries_ms: vec![now.saturating_sub(1), now.saturating_add(10_000)],
            probe_due_ms: vec![now.saturating_add(20_000)],
        };

        let deadlines = runtime
            .watch_deadlines_reusing_probe_schedule(now, &previous, Some(now.saturating_add(5_000)))
            .await
            .expect("reuse deadlines");

        assert_eq!(deadlines.probe_due_ms, previous.probe_due_ms);
        assert_eq!(
            deadlines.expiries_ms,
            vec![now.saturating_add(10_000), now.saturating_add(5_000)]
        );
    }

    #[tokio::test]
    async fn watch_deadlines_include_passed_probe_refresh() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy {
            min_successes: 1,
            min_span_ms: 0,
            ttl_ms: 60 * 60 * 1000,
            ..ProbePolicy::default()
        };
        seed_passed_probe(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };
        let watch_policy = WatchPolicy {
            probe_retry_backoff_ms: 0,
            ..WatchPolicy::default()
        };
        let now = now_ms();

        let deadlines = runtime
            .watch_deadlines(&tick_policy, &watch_policy, now)
            .await
            .expect("watch deadlines");

        assert_eq!(deadlines.probe_due_ms.len(), 1);
        assert!(
            deadlines.probe_due_ms[0] <= now,
            "passed refresh should be due immediately"
        );
    }

    #[tokio::test]
    async fn watch_once_attempts_due_passed_probe_refresh() {
        let (runtime, journal) = runtime_fixture().await;
        journal
            .put_federation(&FED_B, &federation_info())
            .await
            .expect("put auto-joined fed");
        let gate_policy = ProbePolicy {
            min_successes: 1,
            min_span_ms: 0,
            ttl_ms: 60 * 60 * 1000,
            ..ProbePolicy::default()
        };
        seed_passed_probe(journal.as_ref(), FED_B, FED_A, &gate_policy).await;
        let tick_policy = TickPolicy {
            spending_fed: Some(FED_A),
            probe_gate_policy: gate_policy,
            ..TickPolicy::default()
        };
        let watch_policy = WatchPolicy {
            probe_retry_backoff_ms: 0,
            ..WatchPolicy::default()
        };

        let report = runtime
            .watch_once(
                &tick_policy,
                &watch_policy,
                &[],
                &DiscoveryPolicy::default(),
                false,
            )
            .await
            .expect("watch cycle");

        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].fed, FED_B);
        assert_eq!(report.probes[0].verdict, ActiveProbeVerdict::Passed);
        assert!(
            matches!(
                report.probes[0].outcome,
                WatchProbeOutcome::Attempted | WatchProbeOutcome::Failed(_)
            ),
            "due Passed probe should enter the active probe path, not be skipped as Passed"
        );
    }

    #[tokio::test]
    async fn tick_route_preflight_skips_existing_move_intents() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-existing", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("upsert existing move intent");

        let problem = runtime
            .first_move_route_problem(std::slice::from_ref(&decision))
            .await;

        assert!(
            problem.is_none(),
            "same-key replay must be left to apply/executor so it can reuse the stored intent and cached gateway"
        );
    }

    #[tokio::test]
    async fn tick_route_preflight_checks_fresh_move_intents() {
        let (runtime, _journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-fresh", FED_A, FED_B);

        let problem = runtime
            .first_move_route_problem(std::slice::from_ref(&decision))
            .await
            .expect("fresh move should be preflighted against executor gateway selection");

        assert_eq!(problem.from, FED_A);
        assert_eq!(problem.to, FED_B);
        assert_eq!(problem.mark_unavailable, FED_B);
    }

    #[tokio::test]
    async fn tick_route_preflight_skips_existing_evacuate_intents() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_evacuate_decision("evac-existing", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("upsert existing evacuate intent");

        let problem = runtime
            .first_move_route_problem(std::slice::from_ref(&decision))
            .await;

        assert!(
            problem.is_none(),
            "same-key evacuate replay must be left to apply/executor so it can reuse the stored intent and cached gateway"
        );
    }

    #[tokio::test]
    async fn tick_route_preflight_checks_fresh_evacuate_intents() {
        let (runtime, _journal) = runtime_fixture().await;
        let decision = tick_evacuate_decision("evac-fresh", FED_A, FED_B);

        let problem = runtime
            .first_move_route_problem(std::slice::from_ref(&decision))
            .await
            .expect("fresh evacuate should be preflighted against executor gateway selection");

        assert_eq!(problem.from, FED_A);
        assert_eq!(problem.to, FED_B);
        assert_eq!(problem.mark_unavailable, FED_B);
    }

    #[test]
    fn evacuation_source_route_failure_revises_destination() {
        let problem = source_route_problem(
            SendRouteKind::Evacuate,
            FED_A,
            FED_B,
            GatewayUrl("https://gw.example".into()),
            "not connected".into(),
        );

        assert_eq!(problem.from, FED_A);
        assert_eq!(problem.to, FED_B);
        assert_eq!(problem.mark_unavailable, FED_B);
        assert!(problem.evacuation_source_route);
        assert!(
            problem.error.contains("source gateway validation failed"),
            "{}",
            problem.error
        );
    }

    #[test]
    fn move_source_route_failure_still_revises_destination() {
        let problem = source_route_problem(
            SendRouteKind::Move,
            FED_A,
            FED_B,
            GatewayUrl("https://gw.example".into()),
            "not connected".into(),
        );

        assert_eq!(problem.mark_unavailable, FED_B);
        assert!(!problem.evacuation_source_route);
    }

    #[test]
    fn scan_route_picks_the_first_gateway_serving_the_whole_route() {
        // §15.6. First gateway dead (serves neither), second serves both -> routable via #1.
        assert_eq!(
            scan_route(&[(false, false), (true, true)]),
            RouteScan::Routable(1)
        );
        // A gateway serving ONLY the destination is skipped when the source needs it; with no
        // other gateway serving the source the route is source-unserved (re-target the dest).
        assert_eq!(scan_route(&[(true, false)]), RouteScan::SourceUnserved(0));
        // Serves-only-dest, then a gateway serving both -> routable via the second.
        assert_eq!(
            scan_route(&[(true, false), (true, true)]),
            RouteScan::Routable(1)
        );
        // No gateway serves the destination at all, and an empty candidate set.
        assert_eq!(
            scan_route(&[(false, false)]),
            RouteScan::DestinationUnserved
        );
        assert_eq!(scan_route(&[]), RouteScan::DestinationUnserved);
        // A receive-only route (source always "served") is routable on the first dest-ok gateway.
        assert_eq!(scan_route(&[(true, true)]), RouteScan::Routable(0));
    }

    #[tokio::test]
    async fn perform_timeout_leaves_a_stalled_intent_pending() {
        use wallet_core::MemJournal;

        // §15.9. An executor whose `perform` never resolves (a stalled gateway long-poll).
        struct NeverResolves;
        #[async_trait]
        impl Executor for NeverResolves {
            async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
                std::future::pending::<()>().await;
                unreachable!("pending() never resolves")
            }
        }

        let journal = MemJournal::new();
        let decision = tick_move_decision("stall", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("upsert pending intent");

        // Wrap the never-resolving executor with a short deadline and drive it via reconcile.
        let executor = TimeoutExecutor::new(NeverResolves, Some(Duration::from_millis(50)));
        let summary = wallet_core::reconcile(&journal, &executor).await;

        // The perform timed out: counted as a (retryable) failure, NOT performed, and the intent
        // is left Pending for the next reconcile — never resurrected to a terminal status.
        assert_eq!(summary.performed, 0);
        assert_eq!(summary.failed, 1);
        assert_eq!(
            journal
                .get(&decision.idempotency_key)
                .await
                .expect("get")
                .map(|i| i.status),
            Some(IntentStatus::Pending)
        );
    }

    #[tokio::test]
    async fn perform_timeout_does_not_cancel_join_partition_cleanup() {
        struct SlowJoin;
        #[async_trait]
        impl Executor for SlowJoin {
            async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
                fedimint_core::runtime::sleep(Duration::from_millis(25)).await;
                Ok(PerformOutcome::Done)
            }
        }

        let decision = AllocatorDecision {
            action: Action::Join {
                federation: FED_A,
                invite: "test-invite".into(),
                membership_preexisting: false,
            },
            reason: ReasonCode::UserInitiated,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey("join:test".into()),
        };
        let intent = Intent::from_decision(&decision, Actor::User, 0);
        let executor = TimeoutExecutor::new(SlowJoin, Some(Duration::from_millis(1)));

        assert_eq!(executor.perform(&intent).await, Ok(PerformOutcome::Done));
    }

    #[tokio::test]
    async fn tick_rejects_already_terminal_same_occurrence_replays() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-stale", FED_A, FED_B);
        let mut done = Intent::from_decision(&decision, Actor::User, 0);
        done.status = IntentStatus::Done;
        journal.upsert(&done).await.expect("upsert done intent");

        let replays = runtime
            .terminal_replayed_executable_decisions(std::slice::from_ref(&decision))
            .await
            .expect("freshness scan");
        assert_eq!(
            replays,
            vec![TerminalReplay {
                key: decision.idempotency_key.clone(),
                status: IntentStatus::Done,
            }]
        );

        let err = runtime
            .ensure_fresh_tick_decisions(std::slice::from_ref(&decision), Occurrence(0))
            .await
            .expect_err("same-occurrence terminal replay must fail a tick");
        let msg = err.to_string();
        assert!(msg.contains("already-terminal"), "{msg}");
        assert!(msg.contains("fresh --occurrence"), "{msg}");
        assert!(msg.contains("move-stale"), "{msg}");
    }

    #[tokio::test]
    async fn tick_rejects_failed_same_occurrence_replays() {
        // A `Failed` intent is terminal in `apply` (skipped as `terminal_failed_skipped`, which the
        // CLI turns into a non-zero tick exit). The freshness scan must flag it too so `tick` fails
        // early with the "advance --occurrence" remedy and `status` surfaces the same signal.
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-failed", FED_A, FED_B);
        let mut failed = Intent::from_decision(&decision, Actor::User, 0);
        failed.status = IntentStatus::Failed;
        journal.upsert(&failed).await.expect("upsert failed intent");

        let replays = runtime
            .terminal_replayed_executable_decisions(std::slice::from_ref(&decision))
            .await
            .expect("freshness scan");
        assert_eq!(
            replays,
            vec![TerminalReplay {
                key: decision.idempotency_key.clone(),
                status: IntentStatus::Failed,
            }]
        );

        let err = runtime
            .ensure_fresh_tick_decisions(std::slice::from_ref(&decision), Occurrence(0))
            .await
            .expect_err("same-occurrence terminal Failed replay must fail a tick");
        let msg = err.to_string();
        assert!(msg.contains("already-terminal"), "{msg}");
        assert!(msg.contains("fresh --occurrence"), "{msg}");
        assert!(msg.contains("move-failed"), "{msg}");
    }

    #[tokio::test]
    async fn tick_freshness_allows_pending_same_occurrence_retries() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-pending", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("upsert pending intent");

        assert!(runtime
            .terminal_replayed_executable_decisions(std::slice::from_ref(&decision))
            .await
            .expect("freshness scan")
            .is_empty());
        runtime
            .ensure_fresh_tick_decisions(std::slice::from_ref(&decision), Occurrence(0))
            .await
            .expect("pending same-occurrence tick remains retryable");
    }

    #[test]
    fn tick_terminal_marks_apply_failures_as_failed() {
        let clean = ExecutionSummary {
            performed: 2,
            skipped: 1,
            failed: 0,
            terminal_failed_skipped: 0,
            retryable: 0,
        };
        assert_eq!(tick_terminal(&clean), (OperationStatus::Succeeded, None));

        let retryable = ExecutionSummary {
            performed: 1,
            skipped: 0,
            failed: 1,
            terminal_failed_skipped: 0,
            retryable: 1,
        };
        let (status, error) = tick_terminal(&retryable);
        assert_eq!(status, OperationStatus::Failed);
        let error = error.expect("failed tick carries diagnostic");
        assert!(error.contains("failed=1"), "{error}");
        assert!(error.contains("retryable=1"), "{error}");

        let terminal_skip = ExecutionSummary {
            performed: 0,
            skipped: 1,
            failed: 0,
            terminal_failed_skipped: 1,
            retryable: 0,
        };
        let (status, error) = tick_terminal(&terminal_skip);
        assert_eq!(status, OperationStatus::Failed);
        assert!(error
            .expect("terminal skip carries diagnostic")
            .contains("terminal_failed_skipped=1"));
    }

    /// §8: the operator verbs stamp `Actor::User` + `ReasonCode::UserInitiated` on the intent
    /// they journal (replacing the old hardcoded dummy reason). With no federation joined the
    /// two-leg drive fails retryably and leaves the intent `Pending`, but the journaled intent
    /// already carries the ledger identity.
    #[tokio::test]
    async fn user_move_intent_carries_user_actor_and_reason() {
        let (runtime, journal) = runtime_fixture().await;
        let outcome = runtime
            .do_move(
                FED_A,
                FED_B,
                Msat(10_000),
                Msat(500),
                Occurrence(0),
                ReasonCode::UserInitiated,
                Actor::User,
            )
            .await
            .expect("do_move returns even when the drive is retryable");
        let intent = journal
            .get(&outcome.key)
            .await
            .expect("get")
            .expect("the move intent is journaled");
        assert_eq!(intent.actor, Actor::User);
        assert_eq!(intent.reason, ReasonCode::UserInitiated);
    }

    // ---- phase 5 §5.0.8: the pure probe pieces --------------------------------------

    /// A leg move record with the given phase/artifacts, for the classification table.
    fn probe_leg_rec(phase: MovePhase, invoice: bool, send_op: bool) -> MoveRecord {
        MoveRecord {
            key: IdempotencyKey("move:leg".into()),
            from: Some(FED_A),
            to: FED_B,
            amount: Msat(20_000),
            fee_cap: Msat(10_000),
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: true,
            invoice: invoice.then(|| Invoice("lnbc1pexample".into())),
            recv_op: invoice.then_some(crate::types::OperationId([0x07; 32])),
            send_op: send_op.then_some(crate::types::OperationId([0x09; 32])),
            phase,
            outcome: None,
            preimage: None,
            receive_fee_quoted: Some(Msat(300)),
            send_fee_quoted: Some(Msat(200)),
        }
    }

    #[test]
    fn probe_out_fee_cap_never_allows_return_debit_above_delivered_delta() {
        assert_eq!(
            probe_out_fee_cap(Msat(19_500), Msat(15_000), Msat(10_000)),
            Msat(4_500),
            "leg OUT can spend at most delivered_in - out_net in fees"
        );
        assert_eq!(
            probe_out_fee_cap(Msat(30_000), Msat(15_000), Msat(10_000)),
            Msat(10_000),
            "the operator's leg fee cap still bounds the return leg"
        );
        assert_eq!(
            probe_out_fee_cap(Msat(15_000), Msat(16_000), Msat(10_000)),
            Msat(0),
            "a corrupt oversized out_net cannot mint extra fee budget"
        );
    }

    #[test]
    fn classification_table_demotes_only_candidate_refused_legs() {
        use ProbeLeg::{In, Out};
        let rejected = "lnv2 send deterministically rejected the invoice: FederationNotSupported";
        // Terminal settlement phases: Stranded/Refunded never demote; a terminal FAILED
        // send demotes only when the payer is the candidate (leg OUT).
        for leg in [In, Out] {
            let rec = probe_leg_rec(MovePhase::Stranded, true, true);
            assert_eq!(
                classify_leg_failure(leg, Some(&rec), "x"),
                LegFault::UmbrellaOnly
            );
            let rec = probe_leg_rec(MovePhase::Refunded, true, true);
            assert_eq!(
                classify_leg_failure(leg, Some(&rec), "x"),
                LegFault::UmbrellaOnly
            );
        }
        let failed = probe_leg_rec(MovePhase::Failed, true, true);
        assert_eq!(
            classify_leg_failure(Out, Some(&failed), rejected),
            LegFault::Candidate
        );
        assert_eq!(
            classify_leg_failure(In, Some(&failed), rejected),
            LegFault::UmbrellaOnly,
            "leg IN's payer is the SOURCE — its send failure never demotes the candidate"
        );

        // CreateInvoice step (no artifacts): hosted on the destination — the candidate
        // for leg IN only, and only for a non-local error.
        assert_eq!(
            classify_leg_failure(In, None, "the federation refused to mint"),
            LegFault::Candidate
        );
        assert_eq!(
            classify_leg_failure(In, None, "fee over cap (receive side exceeds fee_cap)"),
            LegFault::UmbrellaOnly,
            "§5.0.2: a parametric refusal must not demote"
        );
        assert_eq!(
            classify_leg_failure(
                In,
                None,
                "gateway receive fee changed between quote and mint; re-run"
            ),
            LegFault::UmbrellaOnly,
            "the §15.7 TOCTOU refusal is gateway-timed, not candidate dishonesty"
        );
        assert_eq!(
            classify_leg_failure(
                In,
                None,
                "destination would exceed the per-fed cap (999+20000 > 1000 msat) for federation x"
            ),
            LegFault::UmbrellaOnly,
            "the ADR-0018 cap refusal is local policy, not candidate dishonesty"
        );
        assert_eq!(
            classify_leg_failure(Out, None, "anything at all"),
            LegFault::UmbrellaOnly,
            "leg OUT's mint is hosted on the SOURCE"
        );

        // Pay step (invoice, no send op): hosted on the source of the move — the
        // candidate for leg OUT only, and only for a non-local error.
        let at_pay = probe_leg_rec(MovePhase::Invoiced, true, false);
        assert_eq!(
            classify_leg_failure(Out, Some(&at_pay), rejected),
            LegFault::Candidate
        );
        assert_eq!(
            classify_leg_failure(
                Out,
                Some(&at_pay),
                "fee over cap: the fixed receive quote 900 msat alone exceeds fee_cap 500 msat"
            ),
            LegFault::UmbrellaOnly
        );
        assert_eq!(
            classify_leg_failure(
                Out,
                Some(&at_pay),
                "move invoice expired before the send leg could pay it (move x); re-run"
            ),
            LegFault::UmbrellaOnly,
            "the §15.4 expiry belt is a timing artifact"
        );
        for sdk_rejection in [
            "lnv2 send deterministically rejected the invoice: Gateway fee exceeds the allowed limit",
            "lnv2 send deterministically rejected the invoice: Gateway expiration time exceeds the allowed limit",
            "lnv2 send deterministically rejected the invoice: Invoice has expired",
        ] {
            assert_eq!(
                classify_leg_failure(Out, Some(&at_pay), sdk_rejection),
                LegFault::UmbrellaOnly,
                "{sdk_rejection} is gateway-parametric/timing, not candidate dishonesty"
            );
        }
        assert_eq!(
            classify_leg_failure(In, Some(&at_pay), rejected),
            LegFault::UmbrellaOnly,
            "leg IN's pay is hosted on the SOURCE"
        );

        // Both artifacts present without a terminal phase: an await-step oddity —
        // genuinely unclear attribution never demotes.
        let odd = probe_leg_rec(MovePhase::Sending, true, true);
        assert_eq!(
            classify_leg_failure(Out, Some(&odd), "x"),
            LegFault::UmbrellaOnly
        );
    }

    #[test]
    fn non_candidate_signatures_match_an_emit_site() {
        // `is_known_non_candidate_error` matches free text emitted by the executors. If
        // an emit site rewords its diagnostic without updating the signature list, a
        // local/gateway fault on a candidate-hosted step silently turns into a wrongful
        // demotion — pin every signature to a source that still emits it.
        let emitting_sources = [
            include_str!("executor.rs"),
            include_str!("../../wallet-core/src/executor.rs"),
        ];
        for sig in NON_CANDIDATE_SIGNATURES {
            assert!(
                emitting_sources.iter().any(|src| src.contains(sig)),
                "signature {sig:?} no longer appears in any emitting source; update \
                 NON_CANDIDATE_SIGNATURES together with the reworded diagnostic"
            );
        }
    }

    #[test]
    fn probe_cost_is_the_source_net_outflow() {
        let settled_in = probe_leg_rec(MovePhase::Settled, true, true);
        let mut settled_out = probe_leg_rec(MovePhase::Settled, true, true);
        settled_out.amount = Msat(15_000);
        // Clean pass: (20_000 + 300 + 200) − 15_000 = fees + residue.
        assert_eq!(
            probe_cost(Some(&settled_in), Some(&settled_out)),
            Some(Msat(5_500))
        );
        // Leg OUT never redeemed: the WHOLE delivered amount + fees is the exposure.
        assert_eq!(probe_cost(Some(&settled_in), None), Some(Msat(20_500)));
        let failed_out = probe_leg_rec(MovePhase::Failed, true, true);
        assert_eq!(
            probe_cost(Some(&settled_in), Some(&failed_out)),
            Some(Msat(20_500))
        );
        // A STRANDED leg IN still debited the source in full.
        let stranded_in = probe_leg_rec(MovePhase::Stranded, true, true);
        assert_eq!(probe_cost(Some(&stranded_in), None), Some(Msat(20_500)));
        // No settled send on leg IN = no money left the source.
        let refunded_in = probe_leg_rec(MovePhase::Refunded, true, true);
        assert_eq!(probe_cost(Some(&refunded_in), None), None);
        assert_eq!(probe_cost(None, None), None);
    }

    #[test]
    fn no_sweep_guard_requires_baseline_plus_delta() {
        // Baseline 100, delta 20: an EXACTLY untouched candidate (120) passes…
        assert!(no_sweep_ok(Msat(120), Msat(100), Msat(20)));
        // …a plain 15-sat spend (105) fails — still exceeds the delta alone (a delta-only
        // check would be fooled) yet not baseline + delta…
        assert!(!no_sweep_ok(Msat(105), Msat(100), Msat(20)));
        assert!(!no_sweep_ok(Msat(119), Msat(100), Msat(20)));
        // …and SPEND-THEN-REPLENISH (spend 15, receive 20 unrelated -> 125) also fails:
        // `>=` would pass, but 15 sats of a redemption would now be pre-existing funds.
        assert!(!no_sweep_ok(Msat(125), Msat(100), Msat(20)));
    }

    #[test]
    fn probe_local_faults_reject_self_probe_poor_source_and_capped_candidate() {
        let ok = probe_local_faults(
            FED_B,
            FED_A,
            Msat(30_000),
            Msat(0),
            Msat(20_000),
            Msat(10_000),
            Some(Msat(1_000_000)),
        );
        assert_eq!(ok, Ok(()));
        // Self-probe.
        let err = probe_local_faults(
            FED_A,
            FED_A,
            Msat(30_000),
            Msat(0),
            Msat(20_000),
            Msat(10_000),
            None,
        )
        .expect_err("self-probe");
        assert!(err.contains("from itself"), "{err}");
        // Source short of amount + leg fee cap.
        let err = probe_local_faults(
            FED_B,
            FED_A,
            Msat(29_999),
            Msat(0),
            Msat(20_000),
            Msat(10_000),
            None,
        )
        .expect_err("poor source");
        assert!(err.contains("insufficient source balance"), "{err}");
        // Candidate without cap room for the probe amount.
        let err = probe_local_faults(
            FED_B,
            FED_A,
            Msat(30_000),
            Msat(990_000),
            Msat(20_000),
            Msat(10_000),
            Some(Msat(1_000_000)),
        )
        .expect_err("capped candidate");
        assert!(err.contains("insufficient candidate cap room"), "{err}");
        // Source ALREADY above the cap: leg OUT would breach it -> refuse before any spend
        // (a guaranteed inconclusive probe otherwise). Source has amount + fee headroom and
        // the candidate has room, so ONLY the over-cap source triggers this.
        let err = probe_local_faults(
            FED_B,
            FED_A,
            Msat(1_100_000),
            Msat(0),
            Msat(20_000),
            Msat(10_000),
            Some(Msat(1_000_000)),
        )
        .expect_err("over-cap source");
        assert!(err.contains("already above the per-fed cap"), "{err}");
        // No hard cap disables the room check.
        assert_eq!(
            probe_local_faults(
                FED_B,
                FED_A,
                Msat(30_000),
                Msat(990_000),
                Msat(20_000),
                Msat(10_000),
                None,
            ),
            Ok(())
        );
    }

    #[test]
    fn occurrence_and_umbrella_key_derive_from_the_session_nonce() {
        let occ = occurrence_from_nonce("000000000000002a0000000000000000").expect("valid nonce");
        assert_eq!(occ, Occurrence(42));
        occurrence_from_nonce("shorty").expect_err("too-short nonce");
        occurrence_from_nonce("zzzzzzzzzzzzzzzz0000000000000000").expect_err("non-hex nonce");
        assert_eq!(
            probe_umbrella_key(&FED_A, "0011").0,
            format!("probe:{}:0011", FED_A.to_hex())
        );
    }

    #[tokio::test]
    async fn direct_inflow_repairs_awaiting_over_settled_record_to_done() {
        let (runtime, journal) = runtime_fixture().await;
        let to = FED_A;
        let amount = Msat(100_000);
        let fee_cap = Msat(1_000);
        let occurrence = Occurrence(0);
        let key = direct_inflow_key(&to, amount, fee_cap, occurrence);

        // Simulate the symmetric crash inside `await_move`: the record was written `Settled`,
        // but the intent CAS to `Done` never landed.
        journal
            .upsert(&direct_inflow_intent(
                key.clone(),
                to,
                IntentStatus::Awaiting,
            ))
            .await
            .expect("upsert intent");
        journal
            .put_move_if_attempt(
                &key,
                0,
                &direct_inflow_record(key.clone(), to, MovePhase::Settled, None),
            )
            .await
            .expect("put move");

        let outcome = runtime
            .direct_inflow(to, amount, fee_cap, occurrence)
            .await
            .expect("direct_inflow");

        assert_eq!(outcome.status, Some(IntentStatus::Done));
        assert_eq!(
            journal.get(&key).await.expect("get").map(|i| i.status),
            Some(IntentStatus::Done)
        );
    }

    #[test]
    fn probe_gate_candidate_ids_covers_auto_joined_and_skipped_rows() {
        use fedimint_core::invite_code::InviteCode;
        use fedimint_core::util::SafeUrl;
        use fedimint_core::PeerId;
        use std::str::FromStr as _;

        fn invite(id: FederationId) -> InviteCode {
            let fed_id =
                fedimint_core::config::FederationId::from_str(&id.to_hex()).expect("valid fed id");
            InviteCode::new(
                SafeUrl::parse("https://probe-gate.example").expect("valid url"),
                PeerId::from(0),
                fed_id,
                None,
            )
        }
        fn row(id: FederationId, state: CandidateState) -> crate::CandidateRecord {
            crate::CandidateRecord {
                id,
                invite: invite(id),
                source: wallet_core::DiscoverySource::Manual,
                discovered_at_ms: 0,
                structural: crate::StructuralOutcome::Passed,
                structural_checked_at_ms: 0,
                state,
                updated_at_ms: 0,
            }
        }

        let auto = FederationId([0x01; 32]);
        let discovered = FederationId([0x02; 32]);
        let skipped = FederationId([0x03; 32]);
        let report = CandidateListReport {
            candidates: vec![
                (auto, row(auto, CandidateState::AutoJoined)),
                (discovered, row(discovered, CandidateState::Discovered)),
            ],
            skipped_ids: BTreeSet::from([skipped]),
            skipped_rows: 1,
            skipped_unidentified: 0,
        };

        // A poison-skipped id joins the probe-gate set so a later Passed probe can clear the
        // concurrent cap; a plain `Discovered` row (no partition) never counts.
        let ids = probe_gate_candidate_ids(&report);
        assert_eq!(ids, BTreeSet::from([auto, skipped]));
        assert!(!ids.contains(&discovered));
    }

    #[tokio::test]
    async fn discovery_probe_gate_uses_the_supplied_policy() {
        use fedimint_core::invite_code::InviteCode;
        use fedimint_core::util::SafeUrl;
        use fedimint_core::PeerId;
        use std::str::FromStr as _;

        let (runtime, journal) = runtime_fixture().await;
        let fed_id = fedimint_core::config::FederationId::from_str(&FED_B.to_hex())
            .expect("valid federation id");
        journal
            .put_candidate(&crate::CandidateRecord {
                id: FED_B,
                invite: InviteCode::new(
                    SafeUrl::parse("https://probe-policy.example").expect("valid url"),
                    PeerId::from(0),
                    fed_id,
                    None,
                ),
                source: wallet_core::DiscoverySource::Manual,
                discovered_at_ms: 0,
                structural: crate::StructuralOutcome::Passed,
                structural_checked_at_ms: 0,
                state: CandidateState::AutoJoined,
                updated_at_ms: 0,
            })
            .await
            .expect("put auto-joined candidate");
        let one_success = ProbePolicy {
            min_successes: 1,
            min_span_ms: 0,
            ttl_ms: 60 * 60 * 1000,
            ..ProbePolicy::default()
        };
        seed_passed_probe(journal.as_ref(), FED_B, FED_A, &one_success).await;
        let now = now_ms();
        assert_eq!(
            runtime.passed_probe_feds(now, &one_success).await,
            BTreeSet::from([FED_B])
        );

        let two_successes = ProbePolicy {
            min_successes: 2,
            ..one_success
        };
        assert!(runtime
            .passed_probe_feds(now, &two_successes)
            .await
            .is_empty());
    }

    #[test]
    fn threshold_for_endpoints_handles_zero_without_underflow() {
        assert_eq!(threshold_for_endpoints(0), 0);
        assert_eq!(threshold_for_endpoints(1), 1);
        assert_eq!(threshold_for_endpoints(4), 3);
    }
}
