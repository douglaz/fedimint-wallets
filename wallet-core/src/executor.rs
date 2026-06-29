use crate::types::{Action, AllocatorDecision, IdempotencyKey, Msat};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentStatus {
    Pending,
    Executing,
    Done,
    /// A `DirectInflow` whose EXTERNAL payer has not yet paid. Owned by the `recv_op`
    /// subscription (§9.5); `reconcile` does NOT re-drive it through `perform`.
    Awaiting,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Intent {
    pub idempotency_key: IdempotencyKey,
    pub action: Action,
    pub max_fee: Msat,
    pub status: IntentStatus,
}

impl Intent {
    pub fn from_decision(decision: &AllocatorDecision) -> Self {
        Self {
            idempotency_key: decision.idempotency_key.clone(),
            action: decision.action.clone(),
            max_fee: decision.max_fee,
            status: IntentStatus::Pending,
        }
    }
}

/// Outcome counts from [`apply`]/[`reconcile`]. An `Awaiting` outcome counts as
/// `performed` (the side effect ran; only the external settlement is outstanding).
/// `failed` counts attempts that did not complete in this pass, including retryable
/// failures that leave the intent `Pending` for a later retry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutionSummary {
    pub performed: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// The result of a successful [`Executor::perform`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PerformOutcome {
    /// The intent's effect completed; mark it `Done`.
    Done,
    /// The effect was started but completion depends on an EXTERNAL event (a
    /// `DirectInflow` payer paying the surfaced invoice). Mark `Awaiting`; the
    /// `recv_op` subscription finalizes it (§2/§9.5), NOT a re-drive.
    Awaiting,
}

/// A typed execution failure (was information-free). The variant decides how
/// [`drive`] updates the intent's status.
///
/// The `Retryable`/`Permanent` payloads are diagnostic context for the real
/// `Executor` impl (wallet-fedimint, a later step) to log when it surfaces a failure.
/// `drive` dispatches on the VARIANT alone in this phase — dependency-light
/// `wallet-core` has no log seam yet and `ExecutionSummary` only carries counts — so
/// the strings are carried, not yet emitted. (Kept per spec §2, which mandates the
/// `String` payloads.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecError {
    /// Transient: leave the intent `Pending` so the next [`reconcile`] retries it.
    Retryable(String),
    /// Terminal: mark `Failed`. NOT auto-re-driven; only a manual retry resets it.
    Permanent(String),
    /// The action is not executable in this phase (e.g. `Evacuate`/advisory). → `Failed`.
    Unsupported,
}

#[async_trait]
pub trait Journal: Send + Sync {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError>;
    async fn get(&self, key: &IdempotencyKey) -> Option<Intent>;
    async fn set_status(&self, key: &IdempotencyKey, status: IntentStatus)
        -> Result<(), ExecError>;
    /// Intents to re-drive on the next pass: `Pending | Executing` ONLY (never
    /// `Awaiting`/`Failed`, which are terminal-or-subscription-owned).
    async fn pending(&self) -> Vec<Intent>;
    async fn failed(&self) -> Vec<Intent>;
}

#[derive(Debug, Default)]
pub struct MemJournal {
    intents: Mutex<BTreeMap<IdempotencyKey, Intent>>,
}

impl MemJournal {
    pub fn new() -> Self {
        Self::default()
    }

    fn with_status(&self, accept: impl Fn(IntentStatus) -> bool) -> Vec<Intent> {
        self.intents
            .lock()
            .expect("journal mutex poisoned")
            .values()
            .filter(|intent| accept(intent.status))
            .cloned()
            .collect()
    }
}

#[async_trait]
impl Journal for MemJournal {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError> {
        self.intents
            .lock()
            .expect("journal mutex poisoned")
            .insert(intent.idempotency_key.clone(), intent.clone());
        Ok(())
    }

    async fn get(&self, key: &IdempotencyKey) -> Option<Intent> {
        self.intents
            .lock()
            .expect("journal mutex poisoned")
            .get(key)
            .cloned()
    }

    async fn set_status(
        &self,
        key: &IdempotencyKey,
        status: IntentStatus,
    ) -> Result<(), ExecError> {
        self.intents
            .lock()
            .expect("journal mutex poisoned")
            .get_mut(key)
            .map(|intent| intent.status = status)
            .ok_or_else(|| ExecError::Permanent("journal: intent not found".into()))
    }

    async fn pending(&self) -> Vec<Intent> {
        self.with_status(|status| matches!(status, IntentStatus::Pending | IntentStatus::Executing))
    }

    async fn failed(&self) -> Vec<Intent> {
        self.with_status(|status| status == IntentStatus::Failed)
    }
}

/// The side-effecting step that turns a journaled [`Intent`] into a real-world effect
/// (later: a real cross-federation Lightning move).
///
/// # `&self` + interior mutability
///
/// `perform` takes `&self`: the real implementation holds shared `Arc`s (a
/// `MultiClient` + a `Database`-backed journal) and must be `Send + Sync`. Test
/// doubles ([`MockExecutor`]) use interior mutability for their recorded state.
///
/// # Idempotency contract (load-bearing)
///
/// `perform` MUST be idempotent keyed on `intent.idempotency_key`. After a crash,
/// [`reconcile`] re-drives any intent left `Pending`/`Executing` and calls `perform`
/// again on an intent that may already have moved money, so a real implementation
/// must dedupe on the key (e.g. reuse the in-flight payment registered under that
/// key) and move the underlying funds AT MOST ONCE per key. [`MockExecutor`] models
/// the contract by recording each key and returning `Ok` without re-performing a key
/// it has already seen.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError>;
}

#[derive(Debug, Default)]
pub struct MockExecutor {
    inner: Mutex<MockState>,
}

#[derive(Debug, Default)]
struct MockState {
    fail: BTreeMap<IdempotencyKey, ExecError>,
    awaiting: BTreeSet<IdempotencyKey>,
    performed_keys: Vec<IdempotencyKey>,
}

impl MockExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    #[doc(hidden)] // test-only failure injection; not part of the supported API surface
    pub fn fail_retryable(&self, key: &str) {
        self.inner.lock().expect("mock mutex poisoned").fail.insert(
            IdempotencyKey(key.to_string()),
            ExecError::Retryable("injected".into()),
        );
    }

    #[doc(hidden)] // test-only failure injection; not part of the supported API surface
    pub fn fail_permanent(&self, key: &str) {
        self.inner.lock().expect("mock mutex poisoned").fail.insert(
            IdempotencyKey(key.to_string()),
            ExecError::Permanent("injected".into()),
        );
    }

    #[doc(hidden)] // test-only failure injection; not part of the supported API surface
    pub fn succeed(&self, key: &str) {
        self.inner
            .lock()
            .expect("mock mutex poisoned")
            .fail
            .remove(&IdempotencyKey(key.to_string()));
    }

    #[doc(hidden)] // test-only: make this key return `Ok(Awaiting)` (a `DirectInflow`)
    pub fn set_awaiting(&self, key: &str) {
        self.inner
            .lock()
            .expect("mock mutex poisoned")
            .awaiting
            .insert(IdempotencyKey(key.to_string()));
    }

    pub fn performed_keys(&self) -> Vec<IdempotencyKey> {
        self.inner
            .lock()
            .expect("mock mutex poisoned")
            .performed_keys
            .clone()
    }
}

#[async_trait]
impl Executor for MockExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        let mut state = self.inner.lock().expect("mock mutex poisoned");
        if state.performed_keys.contains(&intent.idempotency_key) {
            // Idempotent replay: already acted under this key; do not repeat the effect.
            return if state.awaiting.contains(&intent.idempotency_key) {
                Ok(PerformOutcome::Awaiting)
            } else {
                Ok(PerformOutcome::Done)
            };
        }
        if let Some(err) = state.fail.get(&intent.idempotency_key) {
            return Err(err.clone());
        }
        state.performed_keys.push(intent.idempotency_key.clone());
        if state.awaiting.contains(&intent.idempotency_key) {
            Ok(PerformOutcome::Awaiting)
        } else {
            Ok(PerformOutcome::Done)
        }
    }
}

pub async fn apply<J: Journal, E: Executor>(
    journal: &J,
    executor: &E,
    decisions: &[AllocatorDecision],
) -> ExecutionSummary {
    let mut summary = ExecutionSummary::default();
    let mut seen = BTreeSet::new();

    for decision in decisions {
        if !seen.insert(decision.idempotency_key.clone()) {
            summary.skipped += 1;
            continue;
        }

        // Terminal/owned states are NOT refreshed by a fresh allocator tick:
        //  - `Done`: idempotent REPLAY of a completed intent (crash recovery). It also
        //    means an identical decision that legitimately RECURS later is permanently
        //    skipped, because the allocator key is per-logical-intent with no occurrence
        //    nonce; reviving recurring allocations needs an epoch in the key (TODOS).
        //  - `Failed`: terminal until a MANUAL retry resets it (§2). A recurring tick
        //    must NOT resurrect a fee-over-cap/unsupported failure back to `Pending`.
        //  - `Awaiting`: a `DirectInflow` owned by its `recv_op` subscription (§9.5);
        //    re-driving through `perform` would mint a second invoice.
        match journal.get(&decision.idempotency_key).await {
            Some(intent)
                if matches!(
                    intent.status,
                    IntentStatus::Done | IntentStatus::Failed | IntentStatus::Awaiting
                ) =>
            {
                summary.skipped += 1;
            }
            _ => {
                let intent = Intent::from_decision(decision);
                if journal.upsert(&intent).await.is_err() {
                    summary.failed += 1;
                    continue;
                }
                drive(journal, executor, &intent, &mut summary).await;
            }
        }
    }

    summary
}

pub async fn reconcile<J: Journal, E: Executor>(journal: &J, executor: &E) -> ExecutionSummary {
    let mut summary = ExecutionSummary::default();

    // Re-drive `pending()` (Pending|Executing) ONLY. `Failed`/`Permanent` stay terminal
    // and `Awaiting` is subscription-owned (§2/§9.4): neither is re-driven here.
    for intent in journal.pending().await {
        drive(journal, executor, &intent, &mut summary).await;
    }

    summary
}

async fn drive<J: Journal, E: Executor>(
    journal: &J,
    executor: &E,
    intent: &Intent,
    summary: &mut ExecutionSummary,
) {
    if journal
        .set_status(&intent.idempotency_key, IntentStatus::Executing)
        .await
        .is_err()
    {
        summary.failed += 1;
        return;
    }

    let mut executing = intent.clone();
    executing.status = IntentStatus::Executing;

    match executor.perform(&executing).await {
        Ok(outcome) => {
            let next = match outcome {
                PerformOutcome::Done => IntentStatus::Done,
                PerformOutcome::Awaiting => IntentStatus::Awaiting,
            };
            if journal
                .set_status(&intent.idempotency_key, next)
                .await
                .is_ok()
            {
                summary.performed += 1;
            } else {
                summary.failed += 1;
            }
        }
        Err(ExecError::Retryable(_)) => {
            // Leave the intent Pending so the next reconcile retries it (NOT Failed).
            let _ = journal
                .set_status(&intent.idempotency_key, IntentStatus::Pending)
                .await;
            summary.failed += 1;
        }
        Err(ExecError::Permanent(_)) | Err(ExecError::Unsupported) => {
            let _ = journal
                .set_status(&intent.idempotency_key, IntentStatus::Failed)
                .await;
            summary.failed += 1;
        }
    }
}
