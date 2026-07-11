//! Reusable in-process wallet service: a bookkeeping actor plus concurrent IO drivers.

mod actor;
mod driver;
mod scheduler;

use crate::journal::{FedimintJournal, ProbeRecord, ProbeSession};
use crate::probe::ProbeResult;
use crate::runtime::{MoveRouteProblem, Runtime};
use crate::tick::TickPolicy;
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
    Actor, AllocatorDecision, ExecError, Executor, FederationId, IdempotencyKey, Intent,
    IntentStatus, Invoice, Msat, OperationId, ProbeBudget, ProbePolicy, Reservations, WatchPolicy,
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
            spending_fed: policy.spending_fed,
            standby_fed: policy.standby_fed,
            probe_gate_policy: policy.probe_policy(),
            ..Self::default()
        }
    }
}

pub const ACTOR_MAILBOX_CAPACITY: usize = 64;
pub const EXTERNAL_DRIVER_CAP: usize = 32;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceError {
    Refused {
        reason: RefuseReason,
        message: String,
    },
    Storage(String),
    NotFound(String),
    Timeout,
    ShuttingDown,
    ActorStopped,
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { message, .. } | Self::Storage(message) | Self::NotFound(message) => {
                formatter.write_str(message)
            }
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
}

#[derive(Clone, Debug)]
pub struct ProbeCandidate {
    pub federation: FederationId,
    pub source: FederationId,
    /// Candidate balance sampled by the scheduler before entering the actor. It becomes
    /// the durable no-sweep baseline, so a missing/stale implicit default is never used.
    pub baseline: Msat,
    pub actor: Actor,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecidedOp {
    pub key: IdempotencyKey,
    pub status: IntentStatus,
    pub deduplicated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeDecision {
    pub candidate: FederationId,
    pub key: IdempotencyKey,
    pub session: ProbeSession,
    pub deduplicated: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub redriven: usize,
    pub awaiters_rehydrated: usize,
    pub executing_normalized: usize,
}

#[derive(Clone, Debug)]
pub struct ProbeFacts {
    pub probes: Vec<(FederationId, ProbeResult)>,
    pub occurrence: wallet_core::Occurrence,
    pub now_ms: u64,
}

#[derive(Clone, Debug)]
pub struct TickRound {
    pub decisions: Vec<AllocatorDecision>,
    pub spending_fed: Option<FederationId>,
    pub standby_fed: Option<FederationId>,
    /// The policy generation the actor held when it planned this round. A commit is
    /// refused if a PutPolicy has bumped the generation since — the decisions were
    /// sized against caps/targets the operator has since changed.
    pub planned_generation: u64,
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
pub enum Snapshot {
    Intent(Option<Intent>),
    Reservations(Reservations),
    Registry { drivers: usize },
    Probe(Option<ProbeRecord>),
}

#[derive(Clone, Debug)]
pub enum JournalTransition {
    Upsert(Intent),
    CompareAndSet {
        expected: IntentStatus,
        new: IntentStatus,
    },
    SetStatus {
        status: IntentStatus,
        error: Option<String>,
    },
    OperationArtifact {
        operation_id: OperationId,
        invoice: Option<Invoice>,
    },
    /// The registered wrapper has returned from its step-2 drive, so its process-local
    /// perform guard is gone and the actor may safely hand ownership to a successor.
    DriverFinished {
        generation: u64,
    },
    /// Re-read durable state after an existing step-2 executor wrote a derived artifact.
    Refresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransitionResult {
    Applied,
    Compared(bool),
}

pub enum Command {
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
    DecideTickRound {
        facts: ProbeFacts,
        route_failures: Vec<MoveRouteProblem>,
        reply: oneshot::Sender<ServiceResult<TickRound>>,
    },
    CommitTick {
        decisions: Vec<AllocatorDecision>,
        balances: Option<BTreeMap<FederationId, Msat>>,
        tick_key: Option<IdempotencyKey>,
        planned_generation: u64,
        reply: oneshot::Sender<ServiceResult<CommitTickReport>>,
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

    pub async fn decide_probe(&self, candidate: ProbeCandidate) -> ServiceResult<ProbeDecision> {
        self.request(|reply| Command::DecideProbe { candidate, reply })
            .await
    }

    pub async fn journal_transition(
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

    pub async fn decide_tick_round(
        &self,
        facts: ProbeFacts,
        route_failures: Vec<MoveRouteProblem>,
    ) -> ServiceResult<TickRound> {
        self.request(|reply| Command::DecideTickRound {
            facts,
            route_failures,
            reply,
        })
        .await
    }

    pub async fn commit_tick(
        &self,
        decisions: Vec<AllocatorDecision>,
        planned_generation: u64,
    ) -> ServiceResult<CommitTickReport> {
        self.commit_tick_with_facts(decisions, None, None, planned_generation)
            .await
    }

    async fn commit_tick_with_facts(
        &self,
        decisions: Vec<AllocatorDecision>,
        balances: Option<BTreeMap<FederationId, Msat>>,
        tick_key: Option<IdempotencyKey>,
        planned_generation: u64,
    ) -> ServiceResult<CommitTickReport> {
        self.request(|reply| Command::CommitTick {
            decisions,
            balances,
            tick_key,
            planned_generation,
            reply,
        })
        .await
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
    #[cfg(test)]
    policy_wake: tokio::sync::watch::Receiver<u64>,
}

impl WalletService {
    /// Live in-flight driver count — the `/v1/health` observability surface.
    pub fn inflight_drivers(&self) -> usize {
        driver::len(&self.registry)
    }
}

impl WalletService {
    pub async fn start(runtime: Runtime) -> ServiceResult<Self> {
        let policy = Policy::default();
        let runtime = Arc::new(runtime);
        let journal = runtime.service_journal();
        let executor: Arc<dyn Executor> =
            Arc::new(runtime.service_executor(Some(policy.per_fed_cap)));
        let perform_timeout = runtime.service_perform_timeout();
        Self::start_parts(Some(runtime), journal, executor, policy, perform_timeout).await
    }

    async fn start_parts(
        runtime: Option<Arc<Runtime>>,
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
        let scheduler_runtime = runtime.clone();
        let task = tokio::spawn(actor::run(
            receiver,
            client.sender.downgrade(),
            accepting,
            runtime,
            journal,
            executor,
            policy,
            perform_timeout,
            registry.clone(),
            policy_wake,
        ));
        let (scheduler_abort, scheduler_task) = match scheduler_runtime {
            Some(runtime) => {
                let (abort, abort_rx) = oneshot::channel();
                let scheduler_client = client.clone();
                let task = tokio::spawn(scheduler::run(
                    runtime,
                    scheduler_client,
                    scheduler::default_sources(),
                    policy_wake_rx,
                    abort_rx,
                ));
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
