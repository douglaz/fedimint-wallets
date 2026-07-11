//! Reusable in-process wallet service: a bookkeeping actor plus concurrent IO drivers.

mod actor;
mod driver;

use crate::journal::{FedimintJournal, ProbeRecord, ProbeSession};
use crate::runtime::Runtime;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use wallet_api::{AwaitTarget, Policy, RefuseReason};
use wallet_core::{
    Actor, AllocatorDecision, ExecError, Executor, FederationId, IdempotencyKey, Intent,
    IntentStatus, Invoice, Msat, OperationId, Reservations,
};

pub const ACTOR_MAILBOX_CAPACITY: usize = 64;
pub const EXTERNAL_DRIVER_CAP: usize = 32;

pub type ServiceResult<T> = Result<T, ServiceError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceError {
    Refused {
        reason: RefuseReason,
        message: String,
    },
    Storage(String),
    Policy(String),
    NotFound(String),
    Timeout,
    ShuttingDown,
    ActorStopped,
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { message, .. }
            | Self::Storage(message)
            | Self::Policy(message)
            | Self::NotFound(message) => formatter.write_str(message),
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
}

impl WalletService {
    /// Live in-flight driver count — the `/v1/health` observability surface.
    pub fn inflight_drivers(&self) -> usize {
        driver::len(&self.registry)
    }
}

impl WalletService {
    pub fn start(runtime: Runtime, policy: Policy) -> ServiceResult<Self> {
        policy
            .validate()
            .map_err(|error| ServiceError::Policy(error.to_string()))?;
        let runtime = Arc::new(runtime);
        let journal = runtime.service_journal();
        let executor: Arc<dyn Executor> =
            Arc::new(runtime.service_executor(Some(policy.per_fed_cap)));
        let perform_timeout = runtime.service_perform_timeout();
        Ok(Self::start_parts(
            Some(runtime),
            journal,
            executor,
            policy,
            perform_timeout,
        ))
    }

    fn start_parts(
        runtime: Option<Arc<Runtime>>,
        journal: Arc<FedimintJournal>,
        executor: Arc<dyn Executor>,
        policy: Policy,
        perform_timeout: Option<std::time::Duration>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(ACTOR_MAILBOX_CAPACITY);
        let accepting = Arc::new(AtomicBool::new(true));
        let client = WalletClient {
            sender,
            accepting: accepting.clone(),
        };
        let registry: driver::Registry =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
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
        ));
        Self {
            client,
            task,
            registry,
        }
    }

    pub fn client(&self) -> WalletClient {
        self.client.clone()
    }

    pub async fn shutdown(self) -> ServiceResult<()> {
        self.client.shutdown().await?;
        self.task.await.map_err(|_| ServiceError::ActorStopped)
    }
}

fn registry_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
