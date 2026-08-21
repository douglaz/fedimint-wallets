//! Reusable in-process wallet service: a bookkeeping actor plus concurrent IO drivers.

mod actor;
mod driver;
mod scheduler;

pub(crate) use actor::{active_probe_verdicts, plan_tick_round};

use crate::journal::{
    FedimintJournal, LedgerRepairOracle, ProbeRecord, ProbeSession, RawIntentTerminalFence,
    RawIntentTerminalSink,
};
use crate::probe::ProbeResult;
use crate::runtime::Runtime;
use crate::tick::TickPolicy;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use wallet_api::{AwaitTarget, Policy, RefuseReason};
use wallet_core::DiscoveryPolicy;
use wallet_core::{
    Action, Actor, AllocatorDecision, AllocatorGoal, ExecError, Executor, FederationId,
    IdempotencyKey, Intent, IntentStatus, Invoice, MoveRecord, Msat, OperationId, ProbeBudget,
    ProbePolicy, Reservations, WatchPolicy,
};

pub trait PolicyExt {
    fn probe_policy(&self) -> ProbePolicy;
    fn watch_policy(&self) -> WatchPolicy;
    fn discovery_policy(&self) -> DiscoveryPolicy;
}

impl PolicyExt for Policy {
    fn probe_policy(&self) -> ProbePolicy {
        ProbePolicy {
            amount_msat: self.probe_amount.0,
            leg_fee_cap_msat: self.max_fee.0,
            min_successes: self.probe_min_successes,
            min_span_ms: self.probe_min_span_secs.saturating_mul(1000),
            ttl_ms: self.probe_ttl_secs.saturating_mul(1000),
        }
    }

    fn watch_policy(&self) -> WatchPolicy {
        WatchPolicy {
            base_interval_ms: self.base_interval_secs.saturating_mul(1000),
            min_interval_ms: self.min_interval_secs.saturating_mul(1000),
            evacuation_lead_ms: self.evacuation_lead_secs.saturating_mul(1000),
            discover_every_ms: self.discover_every_secs.saturating_mul(1000),
            discover_pass_deadline_ms: self.discover_pass_deadline_secs.saturating_mul(1000),
            per_preview_timeout_ms: self.per_preview_timeout_secs.saturating_mul(1000),
            max_candidates_per_pass: self.max_candidates_per_pass as usize,
            probe_refresh_lead_ms: self.probe_refresh_lead_secs.saturating_mul(1000),
            probe_retry_backoff_ms: self.probe_retry_backoff_secs.saturating_mul(1000),
            probe_budget: ProbeBudget {
                max_probe_attempts_per_week: self.max_probe_attempts_per_week,
                max_probe_spend_per_week_msat: self.max_probe_spend_per_week.0,
            },
        }
    }

    fn discovery_policy(&self) -> DiscoveryPolicy {
        DiscoveryPolicy {
            auto_join: self.auto_join,
            max_auto_joins_per_week: self.max_auto_joins_per_week,
            auto_join_lifetime_cap: self.auto_join_lifetime_cap,
            require_mainnet: self.require_mainnet,
            ..DiscoveryPolicy::default()
        }
    }
}

impl From<&Policy> for TickPolicy {
    fn from(policy: &Policy) -> Self {
        Self {
            per_fed_cap: policy.per_fed_cap,
            target_spending_balance: policy.spending_target,
            standby_target: policy.standby_target,
            max_fee: policy.max_fee,
            max_fee_bps_of_move: policy.max_fee_bps_of_move,
            evac_fee_base_msat: policy.evac_fee_base_msat,
            evac_fee_bps: policy.evac_fee_bps,
            spending_fed: policy.spending_fed,
            standby_fed: policy.standby_fed,
            probe_gate_policy: policy.probe_policy(),
            ..Self::default()
        }
    }
}

pub const ACTOR_MAILBOX_CAPACITY: usize = 64;
pub const EXTERNAL_DRIVER_CAP: usize = 32;

/// Actor-minted, goal-scoped admission state for one planned tick.
///
/// The authority is intentionally private: callers can carry a token from
/// `reconcile` (or obtain one from [`WalletClient::issue_tick_plan_token`]), but
/// cannot fabricate one.  Counters retain admissions that have already become
/// terminal, which is what closes the off-actor planning window without a
/// terminal-history journal scan.
#[derive(Clone, Debug)]
pub struct GoalAdmissionSnapshot {
    authority: Arc<()>,
    counters: BTreeMap<AllocatorGoal, u64>,
    blocked: wallet_core::GoalBlockers,
    /// Membership changes alter the world the allocator planned against, even where
    /// they do not conflict with one particular money-moving goal.
    world_generation: u64,
    membership_epoch: u64,
}

/// Actor-issued generation vector for a sequential fresh balance sample.
/// It is opaque so a caller cannot relabel stale facts as fresh.
#[derive(Clone, Debug)]
pub struct BalanceFactsToken {
    authority: Arc<()>,
    generations: BTreeMap<FederationId, u64>,
}

/// A single-use, actor-issued guard for a short direct terminal journal mutation.
/// It intentionally is not Clone: dropping it without `end_external_terminal_mutation`
/// leaves balance-fact and tick authority fail-closed until restart.
#[derive(Debug)]
pub struct ExternalTerminalMutationLease {
    authority: Arc<()>,
    epoch: u64,
    balance_federations: std::collections::BTreeSet<FederationId>,
}

/// Opaque authority for service discovery's rare direct membership mutation. It blocks only tick
/// authority, never short raw-terminal leases or user money operations; a dropped lease therefore
/// fails closed for scheduler work without globally disabling independently scoped balance facts.
#[derive(Debug)]
pub struct MembershipMutationLease {
    authority: Arc<()>,
    epoch: u64,
}

impl BalanceFactsToken {
    fn is_issued_by(&self, authority: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.authority, authority)
    }
}

/// Actor-issued authority for admitting fresh scheduled probe work under one exact policy.
///
/// Both identities are opaque. `authority` prevents a capability minted by another wallet actor
/// from being accepted, while `version` is replaced rather than incremented on every successful
/// `PutPolicy`, so a wrapped integer can never make an old policy current again.
#[derive(Clone, Debug)]
pub(crate) struct ProbePolicySnapshot {
    authority: Arc<()>,
    version: Arc<()>,
    policy: Arc<Policy>,
}

impl ProbePolicySnapshot {
    pub(crate) fn policy(&self) -> &Policy {
        self.policy.as_ref()
    }

    fn is_current_for(&self, authority: &Arc<()>, version: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.authority, authority) && Arc::ptr_eq(&self.version, version)
    }
}

impl PartialEq for GoalAdmissionSnapshot {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.authority, &other.authority)
            && self.counters == other.counters
            && self.world_generation == other.world_generation
            && self.membership_epoch == other.membership_epoch
    }
}

impl Eq for GoalAdmissionSnapshot {}

impl GoalAdmissionSnapshot {
    fn is_issued_by(&self, authority: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.authority, authority)
    }
}

/// A default value is deliberately foreign to every actor and therefore fails
/// closed if a caller tries to use it as an eligibility token.
impl Default for GoalAdmissionSnapshot {
    fn default() -> Self {
        Self {
            authority: Arc::new(()),
            counters: BTreeMap::new(),
            blocked: wallet_core::GoalBlockers::default(),
            world_generation: 0,
            membership_epoch: 0,
        }
    }
}

pub fn coalesced_subscription_delay_ms(
    now_ms: u64,
    last_subscription_noop_ms: Option<u64>,
    min_interval_ms: u64,
    recomputed_sleep_ms: u64,
) -> (u64, bool) {
    let Some(last_noop) = last_subscription_noop_ms else {
        return (0, true);
    };
    let cooldown_until = last_noop.saturating_add(min_interval_ms);
    if now_ms >= cooldown_until {
        (0, true)
    } else {
        let cooldown_remaining = cooldown_until - now_ms;
        if recomputed_sleep_ms < cooldown_remaining {
            (recomputed_sleep_ms, false)
        } else {
            (cooldown_remaining, true)
        }
    }
}

pub type ServiceResult<T> = Result<T, ServiceError>;

struct ActorRawIntentTerminalSink<'a> {
    client: &'a WalletClient,
}

#[async_trait]
impl RawIntentTerminalSink for ActorRawIntentTerminalSink<'_> {
    async fn set_raw_terminal(
        &self,
        key: &IdempotencyKey,
        fence: &RawIntentTerminalFence,
        status: IntentStatus,
        error: Option<String>,
    ) -> Result<bool, ExecError> {
        self.client
            .journal_transition(
                key.clone(),
                JournalTransition::SetRawTerminal {
                    fence: fence.clone(),
                    status,
                    error,
                },
            )
            .await
            .map(|result| matches!(result, TransitionResult::Compared(true)))
            .map_err(|error| ExecError::Retryable(format!("actor terminal transition: {error:?}")))
    }
}

/// Run expensive repair I/O off actor while routing only reservation-releasing
/// raw Pay/Receive terminal intent writes through the actor.
pub async fn repair_ledger_with_actor(
    journal: &FedimintJournal,
    oracle: &dyn LedgerRepairOracle,
    client: &WalletClient,
) -> Result<crate::journal::RepairSummary, ExecError> {
    let sink = ActorRawIntentTerminalSink { client };
    journal
        .repair_ledger_with_terminal_sink(oracle, &sink)
        .await
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceError {
    Refused {
        reason: RefuseReason,
        message: String,
    },
    Storage(String),
    NotFound(String),
    /// A FRESH dest-side admission (receive/direct-inflow/move) named a destination that is
    /// JOINED but not currently OPEN. Fail fast so the caller retries once the fed reconnects,
    /// instead of journaling a Pending row that can only stall (the receive/direct-inflow driver
    /// stalls on the invoice deadline). Money-safe: nothing is debited before the destination
    /// opens. The daemon maps this to a 503 (the status code is the "retry shortly" signal).
    DestinationUnavailable(String),
    Timeout,
    ShuttingDown,
    ActorStopped,
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { message, .. }
            | Self::Storage(message)
            | Self::NotFound(message)
            | Self::DestinationUnavailable(message) => formatter.write_str(message),
            Self::Timeout => formatter.write_str("operation wait deadline elapsed"),
            Self::ShuttingDown => formatter.write_str("wallet service is shutting down"),
            Self::ActorStopped => formatter.write_str("wallet service actor stopped"),
        }
    }
}

impl std::error::Error for ServiceError {}

#[derive(Clone, Debug)]
pub struct OpRequest {
    pub decision: AllocatorDecision,
    pub actor: Actor,
    pub now_ms: u64,
    /// Live, detached balance facts sampled before entering the actor.
    pub balances: BTreeMap<FederationId, Msat>,
    /// Present only for a leg owned by the named durable probe session.
    pub probe_session_nonce: Option<String>,
    /// Set by a dest-side handler (receive/direct-inflow/move) to the destination federation when
    /// it is JOINED but not currently open — computed from the same detached `mc.federations()`
    /// read `sample_balances` performs before entering the actor. On the FRESH admission branch
    /// the actor fails fast with [`ServiceError::DestinationUnavailable`] (503) rather than
    /// journaling a Pending row that can only stall. `None` for source-side verbs, for the
    /// scheduler/probe paths, and whenever the destination is open. EXISTING (attach/retrieve)
    /// requests never consult this — the actor takes the attach path before the gate.
    pub dest_unavailable: Option<FederationId>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProbeCandidate {
    pub(crate) federation: FederationId,
    pub(crate) source: FederationId,
    /// Candidate balance sampled by the scheduler before entering the actor. It becomes
    /// the durable no-sweep baseline, so a missing/stale implicit default is never used.
    pub(crate) baseline: Msat,
    pub(crate) actor: Actor,
    pub(crate) now_ms: u64,
    /// Fresh work requires actor-minted policy authority. Retained work can only attach to the
    /// exact durable session observed by the scheduler and can never fall through to fresh
    /// admission if that session finishes or is replaced before this command is handled.
    pub(crate) admission: ProbeAdmission,
}

#[derive(Clone, Debug)]
pub(crate) enum ProbeAdmission {
    Fresh(ProbePolicySnapshot),
    ResumeOnly { expected_nonce: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecidedOp {
    pub key: IdempotencyKey,
    pub status: IntentStatus,
    pub deduplicated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProbeDecision {
    pub(crate) candidate: FederationId,
    pub(crate) key: IdempotencyKey,
    pub(crate) session: ProbeSession,
    pub(crate) deduplicated: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub redriven: usize,
    pub awaiters_rehydrated: usize,
    pub executing_normalized: usize,
    /// The logical allocator goals durable work still owns (br-p93), projected from the pending
    /// scan BEFORE registry-ownership filtering — a goal a live driver owns is not re-driven
    /// (`redriven` stays 0) but is very much in flight. The scheduler carries this into the
    /// cycle's `ProbeFacts` so route pricing and planning suppress exactly these goals and
    /// nothing else.
    pub blocked: wallet_core::GoalBlockers,
    /// Eligibility captured on the actor before this scheduler cycle senses or
    /// plans.  A later durable Agent admission changes only the conflicting
    /// old executable decision at commit.
    pub admission_snapshot: GoalAdmissionSnapshot,
}

impl ReconcileReport {
    /// Whether this pass changed nothing, for the scheduler's subscription-coalescing no-op
    /// check. `blocked` is deliberately excluded: it is a standing projection of work journaled
    /// earlier, not work this pass performed, so a permanently stuck goal must not make every
    /// cycle look busy.
    pub fn is_idle(&self) -> bool {
        let Self {
            redriven,
            awaiters_rehydrated,
            executing_normalized,
            blocked: _,
            admission_snapshot: _,
        } = self;
        *redriven == 0 && *awaiters_rehydrated == 0 && *executing_normalized == 0
    }
}

#[derive(Clone, Debug)]
pub struct ProbeFacts {
    pub probes: Vec<(FederationId, ProbeResult)>,
    pub occurrence: wallet_core::Occurrence,
    pub now_ms: u64,
    /// Whether this cycle may do route-pricing and preflight I/O.
    ///
    /// Only the caller knows whether the cycle can commit. The actor mints one non-cloneable
    /// allowance when this is true and reuses it across every route-revision round.
    pub price_routes: bool,
    /// The in-flight allocator goals this cycle's reconcile projected (br-p93). The actor copies
    /// it onto the tick policy, so the same value suppresses route pricing for a blocked pair and
    /// the decision that pair would have carried.
    pub blocked: wallet_core::GoalBlockers,
    /// The actor-issued eligibility watermark carried from reconcile through
    /// off-actor planning. Missing or foreign tokens are refused by the actor.
    pub admission_snapshot: GoalAdmissionSnapshot,
}

#[derive(Debug)]
pub struct TickRound {
    pub(crate) decisions: Vec<AllocatorDecision>,
    pub(crate) occurrence: wallet_core::Occurrence,
    pub(crate) spending_fed: Option<FederationId>,
    /// The policy generation the actor held when it planned this round. A commit is
    /// refused if a PutPolicy has bumped the generation since — the decisions were
    /// sized against caps/targets the operator has since changed.
    pub(crate) planned_generation: u64,
    /// Actor world generation captured with the admission authority.  Join/Recover
    /// changes invalidate a complete off-actor plan, not merely decisions sharing a
    /// particular allocator goal.
    pub(crate) planned_world_generation: u64,
    /// The same actor-issued eligibility watermark that guarded planning.
    admission_snapshot: GoalAdmissionSnapshot,
}

#[cfg(test)]
impl TickRound {
    pub(crate) fn for_test(
        decisions: Vec<AllocatorDecision>,
        planned_generation: u64,
        admission_snapshot: GoalAdmissionSnapshot,
    ) -> Self {
        let occurrence = decisions
            .first()
            .map(|decision| decision.occurrence)
            .unwrap_or(wallet_core::Occurrence(0));
        Self {
            decisions,
            occurrence,
            spending_fed: None,
            planned_generation,
            planned_world_generation: admission_snapshot.world_generation,
            admission_snapshot,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickRefusal {
    pub key: IdempotencyKey,
    pub reason: RefuseReason,
    pub message: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitTickReport {
    pub accepted: Vec<IdempotencyKey>,
    pub refused: Vec<TickRefusal>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AwaitOutcome {
    Terminal(Box<Intent>),
    Invoice(Invoice),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotScope {
    Intent(IdempotencyKey),
    Reservations,
    Registry,
    Probe(FederationId),
}

#[derive(Clone, Debug, PartialEq)]
// `Intent` deliberately carries the complete persisted Action. Boxing this internal reply enum
// would add allocation and touch every snapshot consumer only to satisfy a layout heuristic.
#[allow(clippy::large_enum_variant)]
pub enum Snapshot {
    Intent(Option<Intent>),
    Reservations(Reservations),
    Registry { drivers: usize },
    Probe(Option<ProbeRecord>),
}

#[derive(Clone, Debug)]
pub(crate) enum JournalTransition {
    // Boxed: `Intent` is by far the largest variant (it carries a full `Action`), and boxing
    // keeps `JournalTransition` — which is cloned and moved through the actor loop — small.
    Upsert {
        expected_attempt: u32,
        intent: Box<Intent>,
    },
    CompareAndSet {
        expected_attempt: u32,
        expected: IntentStatus,
        new: IntentStatus,
    },
    SetStatus {
        expected_attempt: u32,
        status: IntentStatus,
        error: Option<String>,
    },
    SetRawTerminal {
        fence: RawIntentTerminalFence,
        status: IntentStatus,
        error: Option<String>,
    },
    /// The registered wrapper has returned from its step-2 drive, so its process-local
    /// perform guard is gone and the actor may safely hand ownership to a successor.
    DriverFinished {
        generation: u64,
        expected_attempt: u32,
        /// An awaiter lost a transient subscription/observation call.  It has already backed off
        /// outside the actor; after removing this generation the actor may reattach only if the
        /// same durable attempt is still Awaiting.
        retry_awaiter: bool,
    },
    /// Re-read durable state after an existing step-2 executor wrote a derived artifact.
    Refresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TransitionResult {
    Applied,
    Compared(bool),
}

pub(crate) enum Command {
    DecideOp {
        req: OpRequest,
        reply: oneshot::Sender<ServiceResult<DecidedOp>>,
    },
    DecideProbe {
        candidate: ProbeCandidate,
        reply: oneshot::Sender<ServiceResult<ProbeDecision>>,
    },
    JournalTransition {
        key: IdempotencyKey,
        transition: JournalTransition,
        reply: oneshot::Sender<ServiceResult<TransitionResult>>,
    },
    SetOperationArtifact {
        key: IdempotencyKey,
        expected_attempt: u32,
        operation_id: OperationId,
        invoice: Option<Invoice>,
        reply: oneshot::Sender<ServiceResult<bool>>,
    },
    PutMove {
        key: IdempotencyKey,
        expected_attempt: u32,
        record: Box<MoveRecord>,
        reply: oneshot::Sender<ServiceResult<bool>>,
    },
    Snapshot {
        scope: SnapshotScope,
        reply: oneshot::Sender<ServiceResult<Snapshot>>,
    },
    ResolveAwait {
        key: IdempotencyKey,
        target: AwaitTarget,
        deadline: Instant,
        waiter: oneshot::Sender<ServiceResult<AwaitOutcome>>,
    },
    ReconcileDecide {
        reply: oneshot::Sender<ServiceResult<ReconcileReport>>,
    },
    /// Rehydrate durable work without issuing scheduler tick authority.  Public recovery
    /// endpoints use this so a live Join/Recover can resume even while it fences ticks.
    ReconcileDurable {
        reply: oneshot::Sender<ServiceResult<ReconcileReport>>,
    },
    /// Internal, durable-only reconciliation used to recover registry ownership after
    /// a `DriverFinished` post-removal read fault.  Unlike scheduler reconciliation it
    /// intentionally does not mint a tick token, so a tick-ineligible wallet can still
    /// re-own its live work.
    RecoverDriverOwnership {
        reply: oneshot::Sender<ServiceResult<u64>>,
    },
    /// The detached ownership-recovery task has reconciled generation `generation`.
    /// The reply says whether no newer read fault arrived before this actor turn.
    FinishDriverOwnershipRecovery {
        generation: u64,
        reply: oneshot::Sender<ServiceResult<bool>>,
    },
    IssueTickPlanToken {
        reply: oneshot::Sender<ServiceResult<GoalAdmissionSnapshot>>,
    },
    IssueBalanceFactsToken {
        reply: oneshot::Sender<ServiceResult<BalanceFactsToken>>,
    },
    IssueProbePolicySnapshot {
        reply: oneshot::Sender<ServiceResult<ProbePolicySnapshot>>,
    },
    DecideTickRound {
        facts: ProbeFacts,
        reply: oneshot::Sender<ServiceResult<TickRound>>,
    },
    CommitTick {
        round: TickRound,
        balances: BTreeMap<FederationId, Msat>,
        balance_facts: BalanceFactsToken,
        tick_key: Option<IdempotencyKey>,
        reply: oneshot::Sender<ServiceResult<CommitTickReport>>,
    },
    #[cfg(test)]
    FailAfterFreshAdmissionForTest {
        key: IdempotencyKey,
        reply: oneshot::Sender<ServiceResult<()>>,
    },
    BeginExternalTerminalMutation {
        action: Action,
        reply: oneshot::Sender<ServiceResult<ExternalTerminalMutationLease>>,
    },
    EndExternalTerminalMutation {
        lease: ExternalTerminalMutationLease,
        reply: oneshot::Sender<ServiceResult<()>>,
    },
    BeginMembershipMutation {
        reply: oneshot::Sender<ServiceResult<MembershipMutationLease>>,
    },
    EndMembershipMutation {
        lease: MembershipMutationLease,
        reply: oneshot::Sender<ServiceResult<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<ServiceResult<ShutdownToken>>,
    },
    GetPolicy {
        reply: oneshot::Sender<ServiceResult<Policy>>,
    },
    PutPolicy {
        policy: Policy,
        reply: oneshot::Sender<ServiceResult<Policy>>,
    },
}

pub struct ShutdownToken {
    aborts: Vec<tokio::task::AbortHandle>,
    registry: driver::Registry,
    finish: Option<oneshot::Sender<()>>,
}

impl ShutdownToken {
    /// Abort every driver, WAIT until their Drop guards have emptied the registry (so no
    /// aborted driver can race a late `JournalTransition` past the actor's drain), then
    /// release the actor to drain + exit. The wait is bounded: a driver stuck at a
    /// non-await point cannot be force-killed, and the crash-recovery model already
    /// covers whatever it loses (step-3 review, round 8).
    async fn abort_then_drain(mut self) {
        for abort in &self.aborts {
            abort.abort();
        }
        for _ in 0..500u32 {
            if driver::len(&self.registry) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
    }
}

impl Drop for ShutdownToken {
    fn drop(&mut self) {
        for abort in &self.aborts {
            abort.abort();
        }
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
    }
}

#[derive(Clone)]
pub struct WalletClient {
    sender: mpsc::Sender<Command>,
    accepting: Arc<AtomicBool>,
}

impl WalletClient {
    async fn send(&self, command: Command) -> ServiceResult<()> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ServiceError::ShuttingDown);
        }
        self.sender
            .send(command)
            .await
            .map_err(|_| ServiceError::ActorStopped)
    }

    async fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<ServiceResult<T>>) -> Command,
    ) -> ServiceResult<T> {
        let (reply, receive) = oneshot::channel();
        self.send(build(reply)).await?;
        receive.await.map_err(|_| ServiceError::ActorStopped)?
    }

    pub async fn decide_op(&self, req: OpRequest) -> ServiceResult<DecidedOp> {
        self.request(|reply| Command::DecideOp { req, reply }).await
    }

    pub(crate) async fn decide_probe(
        &self,
        candidate: ProbeCandidate,
    ) -> ServiceResult<ProbeDecision> {
        self.request(|reply| Command::DecideProbe { candidate, reply })
            .await
    }

    pub(crate) async fn probe_policy_snapshot(&self) -> ServiceResult<ProbePolicySnapshot> {
        self.request(|reply| Command::IssueProbePolicySnapshot { reply })
            .await
    }

    pub(crate) async fn journal_transition(
        &self,
        key: IdempotencyKey,
        transition: JournalTransition,
    ) -> ServiceResult<TransitionResult> {
        self.request(|reply| Command::JournalTransition {
            key,
            transition,
            reply,
        })
        .await
    }

    /// Fence one raw operation-artifact write on the actor turn. Private to service executors:
    /// public admission has no way to release reservations through this seam.
    pub(crate) async fn set_operation_artifact_if_attempt(
        &self,
        key: IdempotencyKey,
        expected_attempt: u32,
        operation_id: OperationId,
        invoice: Option<&Invoice>,
    ) -> ServiceResult<bool> {
        self.request(|reply| Command::SetOperationArtifact {
            key,
            expected_attempt,
            operation_id,
            invoice: invoice.cloned(),
            reply,
        })
        .await
    }

    /// Fence one derived MoveRecord write on the actor turn. This is a one-shot DB command, not a
    /// lease: no driver can hold the actor gate across network I/O.
    pub(crate) async fn put_move_if_attempt(
        &self,
        key: IdempotencyKey,
        expected_attempt: u32,
        record: MoveRecord,
    ) -> ServiceResult<bool> {
        self.request(|reply| Command::PutMove {
            key,
            expected_attempt,
            record: Box::new(record),
            reply,
        })
        .await
    }

    pub async fn snapshot(&self, scope: SnapshotScope) -> ServiceResult<Snapshot> {
        self.request(|reply| Command::Snapshot { scope, reply })
            .await
    }

    pub async fn resolve_await(
        &self,
        key: IdempotencyKey,
        target: AwaitTarget,
        deadline: Instant,
    ) -> ServiceResult<AwaitOutcome> {
        self.request(|waiter| Command::ResolveAwait {
            key,
            target,
            deadline,
            waiter,
        })
        .await
    }

    pub async fn reconcile(&self) -> ServiceResult<ReconcileReport> {
        self.request(|reply| Command::ReconcileDecide { reply })
            .await
    }

    /// Rehydrate all durable work without issuing tick-planning authority.
    ///
    /// This is for user-triggered recovery such as public and standalone reconcile or
    /// standalone await.  Scheduler cycles must use [`Self::reconcile`], which mints the
    /// authority required to plan a tick and therefore refuses while membership work is live.
    pub async fn reconcile_durable(&self) -> ServiceResult<ReconcileReport> {
        self.request(|reply| Command::ReconcileDurable { reply })
            .await
    }

    async fn recover_driver_ownership(&self) -> ServiceResult<u64> {
        self.request(|reply| Command::RecoverDriverOwnership { reply })
            .await
    }

    async fn finish_driver_ownership_recovery(&self, generation: u64) -> ServiceResult<bool> {
        self.request(|reply| Command::FinishDriverOwnershipRecovery { generation, reply })
            .await
    }

    pub async fn decide_tick_round(&self, facts: ProbeFacts) -> ServiceResult<TickRound> {
        self.request(|reply| Command::DecideTickRound { facts, reply })
            .await
    }

    /// Mint a token for a direct caller that is not using scheduler reconcile.
    /// Scheduler cycles instead carry the token in [`ReconcileReport`], which
    /// is captured before they probe balances.
    pub async fn issue_tick_plan_token(&self) -> ServiceResult<GoalAdmissionSnapshot> {
        self.request(|reply| Command::IssueTickPlanToken { reply })
            .await
    }

    /// One-shot test seam for the generic post-journal-admission error path.
    #[cfg(test)]
    async fn fail_after_fresh_admission_for_test(&self, key: IdempotencyKey) {
        self.request(|reply| Command::FailAfterFreshAdmissionForTest { key, reply })
            .await
            .expect("configure fresh-admission failure");
    }

    /// Capture the actor generation immediately before sampling fresh balances.
    pub async fn issue_balance_facts_token(&self) -> ServiceResult<BalanceFactsToken> {
        self.request(|reply| Command::IssueBalanceFactsToken { reply })
            .await
    }

    pub(crate) async fn begin_external_terminal_mutation(
        &self,
        action: Action,
    ) -> ServiceResult<ExternalTerminalMutationLease> {
        self.request(|reply| Command::BeginExternalTerminalMutation { action, reply })
            .await
    }

    pub(crate) async fn end_external_terminal_mutation(
        &self,
        lease: ExternalTerminalMutationLease,
    ) -> ServiceResult<()> {
        self.request(|reply| Command::EndExternalTerminalMutation { lease, reply })
            .await
    }

    pub(crate) async fn begin_membership_mutation(&self) -> ServiceResult<MembershipMutationLease> {
        self.request(|reply| Command::BeginMembershipMutation { reply })
            .await
    }

    pub(crate) async fn end_membership_mutation(
        &self,
        lease: MembershipMutationLease,
    ) -> ServiceResult<()> {
        self.request(|reply| Command::EndMembershipMutation { lease, reply })
            .await
    }

    pub async fn commit_tick(
        &self,
        round: TickRound,
        balances: BTreeMap<FederationId, Msat>,
        balance_facts: BalanceFactsToken,
    ) -> ServiceResult<CommitTickReport> {
        self.commit_tick_with_facts(round, balances, balance_facts, None)
            .await
    }

    async fn commit_tick_with_facts(
        &self,
        round: TickRound,
        balances: BTreeMap<FederationId, Msat>,
        balance_facts: BalanceFactsToken,
        tick_key: Option<IdempotencyKey>,
    ) -> ServiceResult<CommitTickReport> {
        self.request(|reply| Command::CommitTick {
            round,
            balances,
            balance_facts,
            tick_key,
            reply,
        })
        .await
    }

    /// Test-only compatibility seam for fixtures that deliberately construct a
    /// malformed or stale round. Production callers cannot supply these parts
    /// separately.
    #[cfg(test)]
    async fn commit_tick_legacy(
        &self,
        decisions: Vec<AllocatorDecision>,
        planned_generation: u64,
        admission_snapshot: GoalAdmissionSnapshot,
    ) -> ServiceResult<CommitTickReport> {
        // Legacy malformed-round fixtures predate the production requirement
        // that callers supply a fresh balance sample.  Give those tests a
        // deliberately simple, internally generated sample; production callers
        // cannot reach this cfg(test)-only seam. Tests of balance rejection use
        // `commit_tick_with_facts_legacy` with explicit facts instead.
        let mut balances = BTreeMap::new();
        for decision in &decisions {
            match &decision.action {
                wallet_core::Action::Move { from, to, .. } => {
                    balances.entry(*to).or_insert(Msat(0));
                    // These compatibility fixtures are not balance-boundary
                    // tests. Give the source ample room even when unrelated
                    // pending operations already reserve value from it.
                    let source = balances.entry(*from).or_insert(Msat(0));
                    source.0 = u64::MAX;
                }
                wallet_core::Action::Evacuate {
                    from, to, amount, ..
                } => {
                    balances.entry(*to).or_insert(Msat(0));
                    balances.entry(*from).or_insert(*amount);
                }
                _ => {}
            }
        }
        self.commit_tick(
            TickRound::for_test(decisions, planned_generation, admission_snapshot),
            balances,
            self.issue_balance_facts_token().await?,
        )
        .await
    }

    #[cfg(test)]
    async fn commit_tick_with_facts_legacy(
        &self,
        decisions: Vec<AllocatorDecision>,
        balances: Option<BTreeMap<FederationId, Msat>>,
        tick_key: Option<IdempotencyKey>,
        planned_generation: u64,
        admission_snapshot: GoalAdmissionSnapshot,
    ) -> ServiceResult<CommitTickReport> {
        self.commit_tick_with_facts(
            TickRound::for_test(decisions, planned_generation, admission_snapshot),
            balances.unwrap_or_default(),
            self.issue_balance_facts_token().await?,
            tick_key,
        )
        .await
    }

    /// Approximate actor-mailbox occupancy for `/v1/health`: submitted commands not yet
    /// received. A snapshot of a moving value, never held as an invariant.
    pub fn queue_depth(&self) -> usize {
        self.sender
            .max_capacity()
            .saturating_sub(self.sender.capacity())
    }

    pub async fn get_policy(&self) -> ServiceResult<Policy> {
        self.request(|reply| Command::GetPolicy { reply }).await
    }

    pub async fn put_policy(&self, policy: Policy) -> ServiceResult<Policy> {
        self.request(|reply| Command::PutPolicy { policy, reply })
            .await
    }

    async fn shutdown(&self) -> ServiceResult<()> {
        self.request(|reply| Command::Shutdown { reply })
            .await?
            .abort_then_drain()
            .await;
        Ok(())
    }
}

pub struct WalletService {
    client: WalletClient,
    task: JoinHandle<()>,
    registry: driver::Registry,
    scheduler_abort: Option<oneshot::Sender<()>>,
    scheduler_task: Option<JoinHandle<()>>,
    /// Cleared when the scheduler task returns (graceful abort OR panic), so `/v1/health`
    /// reports scheduler liveness without owning the join handle. Seeded `false` when no
    /// runtime is present (a detached fixture service has no scheduler).
    scheduler_alive: Arc<AtomicBool>,
    critical_exit: mpsc::UnboundedReceiver<&'static str>,
    #[cfg(test)]
    policy_wake: tokio::sync::watch::Receiver<u64>,
}

impl WalletService {
    /// Live in-flight driver count — the `/v1/health` observability surface.
    pub fn inflight_drivers(&self) -> usize {
        driver::len(&self.registry)
    }

    /// A cloneable handle to the scheduler-liveness flag for `/v1/health`: `true` while the
    /// scheduler task runs, flipped `false` when it returns or panics.
    pub fn scheduler_liveness(&self) -> Arc<AtomicBool> {
        self.scheduler_alive.clone()
    }

    /// Wait for the actor or scheduler to stop unexpectedly. The daemon races this against
    /// SIGTERM/SIGINT and the HTTP server so systemd can restart a degraded process instead of
    /// leaving it alive with a dead critical task.
    pub async fn critical_task_exit(&mut self) -> Option<&'static str> {
        self.critical_exit.recv().await
    }
}

/// Reports task exit even when unwinding from a panic. The liveness flag is scheduler-only;
/// actor guards leave it absent.
struct CriticalTaskGuard {
    name: &'static str,
    exit: mpsc::UnboundedSender<&'static str>,
    liveness: Option<Arc<AtomicBool>>,
}

impl Drop for CriticalTaskGuard {
    fn drop(&mut self) {
        if let Some(liveness) = &self.liveness {
            liveness.store(false, Ordering::Release);
        }
        let _ = self.exit.send(self.name);
    }
}

impl WalletService {
    /// Production daemon constructor: actor + drivers + the watch scheduler.
    pub async fn start(runtime: Runtime) -> ServiceResult<Self> {
        Self::start_from_runtime(runtime, true).await
    }

    /// Standalone constructor (spec §6a.7): the CLI's one-shot `--standalone` mode spins up the
    /// SAME actor + drivers the daemon uses, MINUS the watch scheduler. A one-shot command must
    /// not fire the background rebalancer — running the scheduler with no HTTP surface is exactly
    /// the "daemon-without-an-API in standalone mode" the deleted `watch` verb was; the scheduler's
    /// only home is now the daemon. The actor still owns admission/holds/driving, so the money
    /// verbs run the one true `WalletClient` command path.
    pub async fn start_without_scheduler(runtime: Runtime) -> ServiceResult<Self> {
        Self::start_from_runtime(runtime, false).await
    }

    /// Shared bring-up from a live [`Runtime`]. `run_scheduler` gates the background watch task:
    /// the daemon runs it; the one-shot standalone CLI does not.
    async fn start_from_runtime(runtime: Runtime, run_scheduler: bool) -> ServiceResult<Self> {
        let policy = Policy::default();
        let runtime = Arc::new(runtime);
        let journal = runtime.service_journal();
        let executor: Arc<dyn Executor> =
            Arc::new(runtime.service_executor(Some(policy.per_fed_cap)));
        let perform_timeout = runtime.service_perform_timeout();
        let scheduler_runtime = run_scheduler.then(|| runtime.clone());
        Self::start_parts_inner(
            Some(runtime),
            scheduler_runtime,
            journal,
            executor,
            policy,
            perform_timeout,
        )
        .await
    }

    /// Fixture/test constructor: a detached service (no runtime → no scheduler, no network)
    /// over a caller-supplied journal + [`Executor`], seeding the policy insert-if-absent.
    /// Production uses [`Self::start`]; the daemon's axum tests use this to exercise the HTTP
    /// surface against an in-process actor without live guardians.
    pub async fn start_detached(
        journal: Arc<FedimintJournal>,
        executor: Arc<dyn Executor>,
        seed_policy: Policy,
    ) -> ServiceResult<Self> {
        Self::start_parts(None, journal, executor, seed_policy, None).await
    }

    /// Constructor where the scheduler is coupled to the runtime's presence (the `start_detached`
    /// fixtures and the in-crate tests): the scheduler runs iff a runtime is present.
    /// [`Self::start_parts_inner`] decouples the two so standalone can run the actor without it.
    async fn start_parts(
        runtime: Option<Arc<Runtime>>,
        journal: Arc<FedimintJournal>,
        executor: Arc<dyn Executor>,
        seed_policy: Policy,
        perform_timeout: Option<std::time::Duration>,
    ) -> ServiceResult<Self> {
        let scheduler_runtime = runtime.clone();
        Self::start_parts_inner(
            runtime,
            scheduler_runtime,
            journal,
            executor,
            seed_policy,
            perform_timeout,
        )
        .await
    }

    async fn start_parts_inner(
        runtime: Option<Arc<Runtime>>,
        scheduler_runtime: Option<Arc<Runtime>>,
        journal: Arc<FedimintJournal>,
        executor: Arc<dyn Executor>,
        seed_policy: Policy,
        perform_timeout: Option<std::time::Duration>,
    ) -> ServiceResult<Self> {
        let policy = journal
            .seed_policy(&seed_policy)
            .await
            .map_err(actor::storage)?;
        policy.validate().map_err(|error| ServiceError::Refused {
            reason: RefuseReason::PolicyInvalid,
            message: format!(
                "invalid stored policy field {}: {error}",
                error.offending_field()
            ),
        })?;
        let (sender, receiver) = mpsc::channel(ACTOR_MAILBOX_CAPACITY);
        let accepting = Arc::new(AtomicBool::new(true));
        let client = WalletClient {
            sender,
            accepting: accepting.clone(),
        };
        let registry: driver::Registry =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let (policy_wake, policy_wake_rx) = tokio::sync::watch::channel(0);
        #[cfg(test)]
        let test_policy_wake = policy_wake_rx.clone();
        let (critical_exit_tx, critical_exit) = mpsc::unbounded_channel();
        let actor_exit = critical_exit_tx.clone();
        let actor_sender = client.sender.downgrade();
        let actor_registry = registry.clone();
        let task = tokio::spawn(async move {
            let _guard = CriticalTaskGuard {
                name: "wallet actor",
                exit: actor_exit,
                liveness: None,
            };
            actor::run(
                receiver,
                actor_sender,
                accepting,
                runtime,
                journal,
                executor,
                policy,
                perform_timeout,
                actor_registry,
                policy_wake,
            )
            .await;
        });
        let scheduler_alive = Arc::new(AtomicBool::new(scheduler_runtime.is_some()));
        let (scheduler_abort, scheduler_task) = match scheduler_runtime {
            Some(runtime) => {
                let (abort, abort_rx) = oneshot::channel();
                let scheduler_client = client.clone();
                let liveness = scheduler_alive.clone();
                let scheduler_exit = critical_exit_tx.clone();
                let task = tokio::spawn(async move {
                    let _guard = CriticalTaskGuard {
                        name: "wallet scheduler",
                        exit: scheduler_exit,
                        liveness: Some(liveness),
                    };
                    scheduler::run(
                        runtime,
                        scheduler_client,
                        scheduler::default_sources(),
                        policy_wake_rx,
                        abort_rx,
                    )
                    .await;
                });
                (Some(abort), Some(task))
            }
            None => (None, None),
        };
        Ok(Self {
            client,
            task,
            registry,
            scheduler_abort,
            scheduler_task,
            scheduler_alive,
            critical_exit,
            #[cfg(test)]
            policy_wake: test_policy_wake,
        })
    }

    pub fn client(&self) -> WalletClient {
        self.client.clone()
    }

    pub async fn shutdown(mut self) -> ServiceResult<()> {
        if let Some(abort) = self.scheduler_abort.take() {
            let _ = abort.send(());
        }
        let scheduler_result = match self.scheduler_task.take() {
            Some(task) => task.await.map_err(|_| ServiceError::ActorStopped),
            None => Ok(()),
        };
        let shutdown_result = self.client.shutdown().await;
        let actor_result = self.task.await.map_err(|_| ServiceError::ActorStopped);
        scheduler_result?;
        shutdown_result?;
        actor_result
    }
}

fn registry_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
