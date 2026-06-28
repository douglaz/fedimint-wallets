use crate::types::{Action, AllocatorDecision, Sats};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentStatus {
    Pending,
    Executing,
    Done,
    Failed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Intent {
    pub idempotency_key: String,
    pub action: Action,
    pub max_fee: Sats,
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

/// Outcome counts from [`apply`]/[`reconcile`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutionSummary {
    pub performed: usize,
    pub skipped: usize,
    pub failed: usize,
}

/// Opaque execution failure. Intentionally information-free for this pure seam; a richer
/// retryable-vs-terminal taxonomy (and the retry policy that would key off it) is out of
/// scope here (TODOS T2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecError;

pub trait Journal {
    fn upsert(&mut self, intent: &Intent) -> Result<(), ExecError>;
    fn get(&self, key: &str) -> Option<Intent>;
    fn set_status(&mut self, key: &str, status: IntentStatus) -> Result<(), ExecError>;
    fn pending(&self) -> Vec<Intent>;
    fn failed(&self) -> Vec<Intent>;
}

#[derive(Clone, Debug, Default)]
pub struct MemJournal {
    intents: BTreeMap<String, Intent>,
}

impl MemJournal {
    pub fn new() -> Self {
        Self::default()
    }

    fn with_status(&self, accept: impl Fn(IntentStatus) -> bool) -> Vec<Intent> {
        self.intents
            .values()
            .filter(|intent| accept(intent.status))
            .cloned()
            .collect()
    }
}

impl Journal for MemJournal {
    fn upsert(&mut self, intent: &Intent) -> Result<(), ExecError> {
        self.intents
            .insert(intent.idempotency_key.clone(), intent.clone());
        Ok(())
    }

    fn get(&self, key: &str) -> Option<Intent> {
        self.intents.get(key).cloned()
    }

    fn set_status(&mut self, key: &str, status: IntentStatus) -> Result<(), ExecError> {
        self.intents
            .get_mut(key)
            .map(|intent| intent.status = status)
            .ok_or(ExecError)
    }

    fn pending(&self) -> Vec<Intent> {
        self.with_status(|status| matches!(status, IntentStatus::Pending | IntentStatus::Executing))
    }

    fn failed(&self) -> Vec<Intent> {
        self.with_status(|status| status == IntentStatus::Failed)
    }
}

/// The side-effecting step that turns a journaled [`Intent`] into a real-world effect
/// (later: a real cross-federation Lightning move).
///
/// # Idempotency contract (load-bearing)
///
/// `perform` MUST be idempotent keyed on `intent.idempotency_key`. After a crash,
/// [`reconcile`] re-drives any intent left `Pending`/`Executing` and calls `perform` again
/// on an intent that may already have moved money, so a real implementation must dedupe on
/// the key (e.g. reuse the in-flight payment registered under that key) and move the
/// underlying funds AT MOST ONCE per key. A naive impl that performs unconditionally would
/// double-spend on replay; the crash-safety of the whole write-ahead-log design rests on
/// this guarantee. [`MockExecutor`] models the contract by recording each key and returning
/// `Ok` without re-performing a key it has already seen.
pub trait Executor {
    fn perform(&mut self, intent: &Intent) -> Result<(), ExecError>;
}

#[derive(Clone, Debug, Default)]
pub struct MockExecutor {
    fail_keys: BTreeSet<String>,
    performed_keys: Vec<String>,
}

impl MockExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    #[doc(hidden)] // test-only failure injection; not part of the supported API surface
    pub fn fail(&mut self, key: impl Into<String>) {
        self.fail_keys.insert(key.into());
    }

    #[doc(hidden)] // test-only failure injection; not part of the supported API surface
    pub fn succeed(&mut self, key: &str) {
        self.fail_keys.remove(key);
    }

    pub fn performed_keys(&self) -> &[String] {
        &self.performed_keys
    }
}

impl Executor for MockExecutor {
    fn perform(&mut self, intent: &Intent) -> Result<(), ExecError> {
        if self.performed_keys.contains(&intent.idempotency_key) {
            return Ok(());
        }
        if self.fail_keys.contains(&intent.idempotency_key) {
            return Err(ExecError);
        }

        self.performed_keys.push(intent.idempotency_key.clone());
        Ok(())
    }
}

pub fn apply<J: Journal, E: Executor>(
    journal: &mut J,
    executor: &mut E,
    decisions: &[AllocatorDecision],
) -> ExecutionSummary {
    let mut summary = ExecutionSummary::default();
    let mut seen = BTreeSet::new();

    for decision in decisions {
        if !seen.insert(decision.idempotency_key.clone()) {
            summary.skipped += 1;
            continue;
        }

        // Skipping a key already `Done` dedupes idempotent REPLAY of the same intent
        // (crash recovery). It also means an identical decision that legitimately RECURS
        // later is permanently skipped, because the allocator's key is per-logical-intent
        // with no occurrence/epoch nonce. Reviving recurring allocations needs an epoch in
        // the key (allocator/types) — tracked as a follow-up in TODOS.md.
        match journal.get(&decision.idempotency_key) {
            Some(intent) if intent.status == IntentStatus::Done => summary.skipped += 1,
            _ => {
                let intent = Intent::from_decision(decision);
                if journal.upsert(&intent).is_err() {
                    summary.failed += 1;
                    continue;
                }
                drive(journal, executor, &intent, &mut summary);
            }
        }
    }

    summary
}

pub fn reconcile<J: Journal, E: Executor>(journal: &mut J, executor: &mut E) -> ExecutionSummary {
    let mut summary = ExecutionSummary::default();
    let mut seen = BTreeSet::new();

    // `seen` is defensive: pending() (Pending|Executing) and failed() (Failed) are disjoint
    // status sets today, but a future journal could overlap; never drive a key twice per pass.
    for intent in journal.pending().into_iter().chain(journal.failed()) {
        if seen.insert(intent.idempotency_key.clone()) {
            drive(journal, executor, &intent, &mut summary);
        }
    }

    summary
}

fn drive<J: Journal, E: Executor>(
    journal: &mut J,
    executor: &mut E,
    intent: &Intent,
    summary: &mut ExecutionSummary,
) {
    if journal
        .set_status(&intent.idempotency_key, IntentStatus::Executing)
        .is_err()
    {
        summary.failed += 1;
        return;
    }

    let mut executing = intent.clone();
    executing.status = IntentStatus::Executing;

    match executor.perform(&executing) {
        Ok(()) => {
            if journal
                .set_status(&intent.idempotency_key, IntentStatus::Done)
                .is_ok()
            {
                summary.performed += 1;
            } else {
                summary.failed += 1;
            }
        }
        Err(ExecError) => {
            let _ = journal.set_status(&intent.idempotency_key, IntentStatus::Failed);
            summary.failed += 1;
        }
    }
}
