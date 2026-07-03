use crate::types::{Action, AllocatorDecision, IdempotencyKey, Msat};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IntentStatus {
    Pending,
    Executing,
    Done,
    /// A `DirectInflow` whose EXTERNAL payer has not yet paid. Owned by the `recv_op`
    /// subscription (§9.5); `reconcile` does NOT re-drive it through `perform`.
    Awaiting,
    Failed,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Intent {
    pub idempotency_key: IdempotencyKey,
    pub action: Action,
    /// Derived from `action.fee_cap()`: `Some` for `Move`/`Evacuate` (total move
    /// cost) and `DirectInflow` (its receive-side gross-up cost, spec §6); `None`
    /// only for advisory actions, which are never executed.
    pub max_fee: Option<Msat>,
    pub status: IntentStatus,
}

impl Intent {
    pub fn from_decision(decision: &AllocatorDecision) -> Self {
        Self {
            idempotency_key: decision.idempotency_key.clone(),
            action: decision.action.clone(),
            max_fee: decision.action.fee_cap(),
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
    /// Existing intents that were already terminal `Failed` and therefore skipped instead of
    /// re-driven. This is a subset of `skipped`; callers that gate money operations on success
    /// can distinguish these from benign idempotent `Done` skips.
    pub terminal_failed_skipped: usize,
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
/// The `Retryable`/`Permanent` payloads are diagnostic context from the real
/// `Executor` impl. `drive` dispatches on the variant and logs the payload before
/// collapsing the result into [`ExecutionSummary`]'s counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecError {
    /// Transient: leave the intent `Pending` so the next [`reconcile`] retries it.
    Retryable(String),
    /// Terminal: mark `Failed`. NOT auto-re-driven; only a manual retry resets it.
    Permanent(String),
    /// The action is not one this executor implementation performs (an `Executor`
    /// need not map every `Action` shape to a side effect — the fedimint executor
    /// returns this only for a non-move action reaching `perform`). → `Failed`.
    Unsupported,
}

#[async_trait]
pub trait Journal: Send + Sync {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError>;
    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError>;
    async fn set_status(&self, key: &IdempotencyKey, status: IntentStatus)
        -> Result<(), ExecError>;
    /// The single-writer claim (spec §2): atomically, if the stored intent's status ==
    /// `expected`, set it to `new` (moving the pending index in the same transaction for
    /// durable impls) and return `Ok(true)`; otherwise make no change and return `Ok(false)`.
    /// `Ok(false)` also covers an absent key. `drive` uses this to claim a `Pending` intent
    /// before performing it, so two concurrent drivers of the same intent can never both win
    /// the claim.
    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError>;
    /// Intents to re-drive on the next pass: `Pending | Executing` ONLY (never
    /// `Awaiting`/`Failed`, which are terminal-or-subscription-owned).
    async fn pending(&self) -> Vec<Intent>;
    async fn failed(&self) -> Vec<Intent>;
    /// A stable identity for this journal's underlying durable store — used ONLY to scope
    /// `drive`'s process-local in-flight-performs guard (a belt-and-suspenders duplicate-perform
    /// guard alongside the CAS above; an `Executing -> Executing` CAS can't provide mutual
    /// exclusion by itself, since re-reading unchanged state can't tell "my own resume" apart
    /// from "a different concurrent claimant"). Two `Journal` VALUES over the SAME underlying
    /// store (e.g. two `FedimintJournal`s built from clones of the same `Database`, which is
    /// documented to share storage) MUST return the same id, or the guard silently fails to
    /// serialize them; two unrelated journals (e.g. independent test fixtures) must not
    /// collide. The default (this value's own address) is correct for any impl that is never
    /// constructed twice over the same shared storage — true of `MemJournal` and the trait's
    /// test doubles; `FedimintJournal` overrides it (its storage CAN be shared across
    /// independently-constructed handles).
    fn store_id(&self) -> usize {
        (self as *const Self).cast::<()>() as usize
    }
}

#[derive(Debug, Default)]
pub struct MemJournal {
    intents: Mutex<BTreeMap<IdempotencyKey, Intent>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InFlightPerformKey {
    store: usize,
    key: IdempotencyKey,
}

static IN_FLIGHT_PERFORMS: OnceLock<Mutex<BTreeSet<InFlightPerformKey>>> = OnceLock::new();

fn in_flight_performs() -> &'static Mutex<BTreeSet<InFlightPerformKey>> {
    IN_FLIGHT_PERFORMS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

struct InFlightPerform {
    key: InFlightPerformKey,
}

impl InFlightPerform {
    fn claim<J: Journal>(journal: &J, key: &IdempotencyKey) -> Option<Self> {
        let key = InFlightPerformKey {
            store: journal.store_id(),
            key: key.clone(),
        };
        let mut in_flight = in_flight_performs()
            .lock()
            .expect("in-flight perform mutex poisoned");
        if in_flight.insert(key.clone()) {
            Some(Self { key })
        } else {
            None
        }
    }
}

impl Drop for InFlightPerform {
    fn drop(&mut self) {
        in_flight_performs()
            .lock()
            .expect("in-flight perform mutex poisoned")
            .remove(&self.key);
    }
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

    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        Ok(self
            .intents
            .lock()
            .expect("journal mutex poisoned")
            .get(key)
            .cloned())
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

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        let mut intents = self.intents.lock().expect("journal mutex poisoned");
        match intents.get_mut(key) {
            Some(intent) if intent.status == expected => {
                intent.status = new;
                Ok(true)
            }
            _ => Ok(false),
        }
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
        // Advisory actions (`RefuseInflow`/`Cap`) are policy signals, not work: never
        // journal an Intent for them.
        if !decision.action.is_executable() {
            summary.skipped += 1;
            continue;
        }

        if !seen.insert(decision.idempotency_key.clone()) {
            summary.skipped += 1;
            continue;
        }

        // Terminal/owned states are NOT refreshed by a fresh allocator tick:
        //  - `Done`: idempotent REPLAY of a completed intent (crash recovery). A
        //    legitimately RECURRING decision stays live because the allocator stamps
        //    the current `Occurrence` (T10) into the key, so the next tick's key
        //    differs once this one has settled `Done`.
        //  - `Failed`: terminal until a MANUAL retry resets it (§2). A recurring tick
        //    must NOT resurrect a fee-over-cap/unsupported failure back to `Pending`.
        //  - `Awaiting`: a `DirectInflow` owned by its `recv_op` subscription (§9.5);
        //    re-driving through `perform` would mint a second invoice.
        let existing = match journal.get(&decision.idempotency_key).await {
            Ok(existing) => existing,
            Err(_) => {
                // Unknown durable state must not be treated as "missing"; doing so could
                // re-drive a terminal or subscription-owned intent.
                summary.failed += 1;
                continue;
            }
        };

        match existing {
            Some(intent) if intent.status == IntentStatus::Failed => {
                summary.skipped += 1;
                summary.terminal_failed_skipped += 1;
            }
            Some(intent)
                if matches!(intent.status, IntentStatus::Done | IntentStatus::Awaiting) =>
            {
                summary.skipped += 1;
            }
            Some(intent) => {
                // The key names an already-started operation. Drive the durable intent as
                // stored instead of refreshing action fields from a later allocator tick;
                // the real executor fixes invoices/amounts under the original key.
                drive(journal, executor, &intent, &mut summary).await;
            }
            None => {
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
    // `drive` only ever receives a `Pending` or `Executing` intent (see `apply`/`reconcile`).
    // A `Pending` intent must first win the durable CAS claim. `Executing` means either
    // crash-recovery resume or another in-process driver already won that claim; the
    // process-local guard below lets crash recovery proceed while skipping live duplicates.
    if intent.status == IntentStatus::Pending {
        match journal
            .set_status_if(
                &intent.idempotency_key,
                IntentStatus::Pending,
                IntentStatus::Executing,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                // Another driver already claimed this intent; not a failure, just lost the race.
                summary.skipped += 1;
                return;
            }
            Err(_) => {
                summary.failed += 1;
                return;
            }
        }
    }

    let Some(_in_flight) = InFlightPerform::claim(journal, &intent.idempotency_key) else {
        summary.skipped += 1;
        return;
    };

    let executing = match journal.get(&intent.idempotency_key).await {
        Ok(Some(intent)) if intent.status == IntentStatus::Executing => intent,
        Ok(_) => {
            // This can be a stale `Executing` snapshot from a concurrent scan after the
            // winning driver has already written a terminal status.
            summary.skipped += 1;
            return;
        }
        Err(_) => {
            summary.failed += 1;
            return;
        }
    };

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
        Err(ExecError::Retryable(reason)) => {
            tracing::warn!(
                key = %intent.idempotency_key.0,
                reason = %reason,
                "executor perform failed retryably"
            );
            // Leave the intent Pending so the next reconcile retries it (NOT Failed).
            let _ = journal
                .set_status(&intent.idempotency_key, IntentStatus::Pending)
                .await;
            summary.failed += 1;
        }
        Err(ExecError::Permanent(reason)) => {
            tracing::warn!(
                key = %intent.idempotency_key.0,
                reason = %reason,
                "executor perform failed permanently"
            );
            let _ = journal
                .set_status(&intent.idempotency_key, IntentStatus::Failed)
                .await;
            summary.failed += 1;
        }
        Err(ExecError::Unsupported) => {
            tracing::warn!(
                key = %intent.idempotency_key.0,
                "executor action unsupported"
            );
            let _ = journal
                .set_status(&intent.idempotency_key, IntentStatus::Failed)
                .await;
            summary.failed += 1;
        }
    }
}
