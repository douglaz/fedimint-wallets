use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tokio::sync::Barrier;
use wallet_core::*;

fn ikey(key: &str) -> IdempotencyKey {
    IdempotencyKey(key.to_string())
}

fn decision(key: &str, action: Action, reason: ReasonCode) -> AllocatorDecision {
    AllocatorDecision {
        action,
        reason,
        occurrence: Occurrence(1),
        idempotency_key: ikey(key),
    }
}

fn move_decision(key: &str, amount: u64) -> AllocatorDecision {
    decision(
        key,
        Action::Move {
            from: FederationId([1; 32]),
            to: FederationId([2; 32]),
            amount: Msat(amount),
            fee_cap: Msat(7),
        },
        ReasonCode::SpendingBelowTarget,
    )
}

fn refuse_decision(key: &str) -> AllocatorDecision {
    decision(
        key,
        Action::RefuseInflow {
            fed: FederationId([1; 32]),
            reason: ReasonCode::OverCap,
        },
        ReasonCode::OverCap,
    )
}

fn counts(performed: usize, skipped: usize, failed: usize) -> ExecutionSummary {
    ExecutionSummary {
        performed,
        skipped,
        failed,
        terminal_failed_skipped: 0,
        retryable: 0,
    }
}

/// Like [`counts`] but with the §15.11 `retryable` sub-count set (`retryable` is a subset of
/// `failed`, so a purely-retryable pass is `counts_with_retryable(p, s, failed, failed)`).
fn counts_with_retryable(
    performed: usize,
    skipped: usize,
    failed: usize,
    retryable: usize,
) -> ExecutionSummary {
    ExecutionSummary {
        performed,
        skipped,
        failed,
        terminal_failed_skipped: 0,
        retryable,
    }
}

fn counts_with_terminal_failed_skipped(
    performed: usize,
    skipped: usize,
    failed: usize,
    terminal_failed_skipped: usize,
) -> ExecutionSummary {
    ExecutionSummary {
        performed,
        skipped,
        failed,
        terminal_failed_skipped,
        retryable: 0,
    }
}

#[derive(Default)]
struct GetFailsJournal {
    upserts: Mutex<usize>,
}

#[async_trait]
impl Journal for GetFailsJournal {
    async fn upsert(&self, _intent: &Intent) -> Result<(), ExecError> {
        *self.upserts.lock().expect("mutex poisoned") += 1;
        Ok(())
    }

    async fn get(&self, _key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        Err(ExecError::Permanent("poison intent row".to_string()))
    }

    async fn set_status(
        &self,
        _key: &IdempotencyKey,
        _status: IntentStatus,
    ) -> Result<(), ExecError> {
        unreachable!("apply must not drive when get fails")
    }

    async fn set_status_if(
        &self,
        _key: &IdempotencyKey,
        _expected: IntentStatus,
        _new: IntentStatus,
    ) -> Result<bool, ExecError> {
        unreachable!("apply must not drive when get fails")
    }

    async fn pending(&self) -> Vec<Intent> {
        Vec::new()
    }

    async fn failed(&self) -> Vec<Intent> {
        Vec::new()
    }
}

/// A [`Journal`] wrapping a [`MemJournal`] whose `set_status_if` rendezvouses at a
/// [`Barrier`] before delegating. Used by `concurrent_drive_performs_once` to force two
/// concurrent `drive`s to both reach their CAS claim together, so the race it exercises is
/// real rather than accidentally serialized by the runtime running one `reconcile` to
/// completion before the other starts.
struct BarrierJournal {
    inner: MemJournal,
    barrier: Barrier,
}

#[async_trait]
impl Journal for BarrierJournal {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError> {
        self.inner.upsert(intent).await
    }

    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        self.inner.get(key).await
    }

    async fn set_status(
        &self,
        key: &IdempotencyKey,
        status: IntentStatus,
    ) -> Result<(), ExecError> {
        self.inner.set_status(key, status).await
    }

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        self.barrier.wait().await;
        self.inner.set_status_if(key, expected, new).await
    }

    async fn pending(&self) -> Vec<Intent> {
        self.inner.pending().await
    }

    async fn failed(&self) -> Vec<Intent> {
        self.inner.failed().await
    }
}

/// A [`Journal`] wrapper whose `pending()` captures a snapshot immediately, then waits
/// before returning it. This lets a test hold a stale `Executing` snapshot until after the
/// first driver has already completed the intent.
struct DelayedPendingJournal {
    inner: Arc<MemJournal>,
    snapshot_taken: Barrier,
    release_snapshot: Barrier,
}

#[async_trait]
impl Journal for DelayedPendingJournal {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError> {
        self.inner.upsert(intent).await
    }

    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        self.inner.get(key).await
    }

    async fn set_status(
        &self,
        key: &IdempotencyKey,
        status: IntentStatus,
    ) -> Result<(), ExecError> {
        self.inner.set_status(key, status).await
    }

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        self.inner.set_status_if(key, expected, new).await
    }

    async fn pending(&self) -> Vec<Intent> {
        let snapshot = self.inner.pending().await;
        self.snapshot_taken.wait().await;
        self.release_snapshot.wait().await;
        snapshot
    }

    async fn failed(&self) -> Vec<Intent> {
        self.inner.failed().await
    }

    fn store_id(&self) -> usize {
        Arc::as_ptr(&self.inner) as *const () as usize
    }
}

/// An executor that holds the first `perform` open. A second `perform` call records a
/// duplicate key and returns immediately, making the in-flight `Executing` race observable
/// without hanging the test.
struct BlockingExecutor {
    performed_keys: Mutex<Vec<IdempotencyKey>>,
    first_entered: Barrier,
    release_first: Barrier,
}

impl BlockingExecutor {
    fn new() -> Self {
        Self {
            performed_keys: Mutex::new(Vec::new()),
            first_entered: Barrier::new(2),
            release_first: Barrier::new(2),
        }
    }

    fn performed_keys(&self) -> Vec<IdempotencyKey> {
        self.performed_keys
            .lock()
            .expect("blocking executor mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl Executor for BlockingExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        let call = {
            let mut performed_keys = self
                .performed_keys
                .lock()
                .expect("blocking executor mutex poisoned");
            performed_keys.push(intent.idempotency_key.clone());
            performed_keys.len()
        };

        if call == 1 {
            self.first_entered.wait().await;
            self.release_first.wait().await;
        }

        Ok(PerformOutcome::Done)
    }
}

#[test]
fn direct_inflow_is_executable() {
    let action = Action::DirectInflow {
        to: FederationId([2; 32]),
        amount: Msat(50_000),
        fee_cap: Msat(500),
    };
    assert!(action.is_executable());
    // A `DirectInflow` now carries a receive-side fee budget (spec §6), so it
    // surfaces a `fee_cap` like `Move`/`Evacuate`.
    assert_eq!(action.fee_cap(), Some(Msat(500)));
}

#[test]
fn advisory_actions_are_not_executable() {
    let refuse = Action::RefuseInflow {
        fed: FederationId([1; 32]),
        reason: ReasonCode::OverCap,
    };
    assert!(!refuse.is_executable());
    assert_eq!(refuse.fee_cap(), None);
}

#[tokio::test]
async fn apply_fresh_decision_journals_and_performs_once() {
    let key = "move:1:2:42";
    let decisions = vec![move_decision(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(1, 0, 0)
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    let intent = journal
        .get(&ikey(key))
        .await
        .expect("get")
        .expect("intent is journaled");
    assert_eq!(intent.idempotency_key, ikey(key));
    assert_eq!(intent.action, decisions[0].action);
    assert_eq!(intent.max_fee, Some(Msat(7)));
    assert_eq!(intent.status, IntentStatus::Done);
}

#[tokio::test]
async fn apply_direct_inflow_journals_with_its_fee_cap() {
    // A construct-a-DirectInflow smoke test: it is executable, gets an Intent, and
    // journals with its receive-side `fee_cap` as `max_fee` (spec §6).
    let key = "inflow:2:1";
    let decisions = vec![decision(
        key,
        Action::DirectInflow {
            to: FederationId([2; 32]),
            amount: Msat(50_000),
            fee_cap: Msat(500),
        },
        ReasonCode::SpendingBelowTarget,
    )];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(1, 0, 0)
    );
    let intent = journal
        .get(&ikey(key))
        .await
        .expect("get")
        .expect("intent is journaled");
    assert_eq!(intent.max_fee, Some(Msat(500)));
    assert_eq!(intent.status, IntentStatus::Done);
}

#[tokio::test]
async fn apply_skips_advisory_decisions_with_no_intent() {
    // RefuseInflow is a policy signal, not work: apply() must not journal an Intent for
    // it, and the executor must never see it.
    let key = "refuse:1:1";
    let decisions = vec![refuse_decision(key)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(0, 1, 0)
    );
    assert_eq!(journal.get(&ikey(key)).await.expect("get"), None);
    assert!(executor.performed_keys().is_empty());
}

#[tokio::test]
async fn applying_same_decisions_twice_performs_only_once() {
    let key = "move:1:2:42";
    let decisions = vec![move_decision(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();

    assert_eq!(apply(&journal, &executor, &decisions).await.performed, 1);
    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(0, 1, 0)
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
}

#[tokio::test]
async fn apply_redrives_stored_pending_intent_without_refreshing_fields() {
    let key = "move:1:2:42";
    let old_decision = decision(
        key,
        Action::Move {
            from: FederationId([1; 32]),
            to: FederationId([2; 32]),
            amount: Msat(42),
            fee_cap: Msat(1),
        },
        ReasonCode::SpendingBelowTarget,
    );
    let stored_intent = Intent::from_decision(&old_decision);

    let new_decision = decision(
        key,
        Action::Move {
            from: FederationId([1; 32]),
            to: FederationId([2; 32]),
            amount: Msat(99),
            fee_cap: Msat(99),
        },
        ReasonCode::SpendingBelowTarget,
    );
    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    journal.upsert(&stored_intent).await.unwrap();

    assert_eq!(
        apply(&journal, &executor, &[new_decision]).await,
        counts(1, 0, 0)
    );
    let intent = journal
        .get(&ikey(key))
        .await
        .expect("get")
        .expect("intent still journaled");
    assert_eq!(intent.status, IntentStatus::Done);
    assert_eq!(intent.action, old_decision.action);
    assert_eq!(intent.max_fee, Some(Msat(1)));
}

#[tokio::test]
async fn apply_get_error_does_not_create_fresh_intent() {
    let key = "move:1:2:42";
    let decisions = vec![move_decision(key, 42)];
    let journal = GetFailsJournal::default();
    let executor = MockExecutor::new();

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(0, 0, 1)
    );
    assert_eq!(*journal.upserts.lock().expect("mutex poisoned"), 0);
    assert!(executor.performed_keys().is_empty());
}

#[tokio::test]
async fn duplicate_keys_in_one_apply_are_performed_once() {
    let key = "move:1:2:42";
    let decisions = vec![move_decision(key, 42), move_decision(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(1, 1, 0)
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn reconcile_redrives_executing_intent_after_crash() {
    let key = "move:1:2:42";
    let mut intent = Intent::from_decision(&move_decision(key, 42));
    intent.status = IntentStatus::Executing;

    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    journal.upsert(&intent).await.unwrap();

    assert_eq!(reconcile(&journal, &executor).await, counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn retryable_failure_stays_pending_and_reconcile_retries() {
    let key = "move:1:2:42";
    let decisions = vec![move_decision(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    executor.fail_retryable(key);

    // §15.11: a retryable failure counts in `failed` AND in the `retryable` sub-count, so a
    // scheduler can tell "left Pending, will retry" apart from a terminal money-op failure.
    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts_with_retryable(0, 0, 1, 1)
    );
    // A Retryable error leaves the intent Pending (NOT Failed), so reconcile retries it.
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Pending
    );
    assert!(executor.performed_keys().is_empty());

    executor.succeed(key);
    assert_eq!(reconcile(&journal, &executor).await, counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn permanent_failure_is_terminal_and_reconcile_does_not_redrive() {
    let key = "move:1:2:42";
    let decisions = vec![move_decision(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    executor.fail_permanent(key);

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(0, 0, 1)
    );
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Failed
    );

    // Even with the injection cleared, reconcile must NOT re-drive a Failed intent: it
    // is terminal until a manual retry resets it to Pending.
    executor.succeed(key);
    assert_eq!(reconcile(&journal, &executor).await, counts(0, 0, 0));
    assert!(executor.performed_keys().is_empty());
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Failed
    );
}

#[tokio::test]
async fn apply_skips_decision_when_key_is_already_done() {
    let key = "move:1:2:42";
    let mut intent = Intent::from_decision(&move_decision(key, 42));
    intent.status = IntentStatus::Done;

    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    journal.upsert(&intent).await.unwrap();

    assert_eq!(
        apply(&journal, &executor, &[move_decision(key, 42)]).await,
        counts(0, 1, 0)
    );
    assert!(executor.performed_keys().is_empty());
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn apply_does_not_resurrect_failed_intent() {
    let key = "move:1:2:42";
    let old_decision = decision(
        key,
        Action::Move {
            from: FederationId([1; 32]),
            to: FederationId([2; 32]),
            amount: Msat(42),
            fee_cap: Msat(1),
        },
        ReasonCode::SpendingBelowTarget,
    );
    let mut failed_intent = Intent::from_decision(&old_decision);
    failed_intent.status = IntentStatus::Failed;

    let new_decision = decision(
        key,
        Action::Move {
            from: FederationId([1; 32]),
            to: FederationId([2; 32]),
            amount: Msat(42),
            fee_cap: Msat(99),
        },
        ReasonCode::SpendingBelowTarget,
    );
    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    journal.upsert(&failed_intent).await.unwrap();

    // A fresh allocator tick with the same key must NOT reset Failed back to Pending,
    // nor refresh its fields nor re-perform it (§2: apply treats Failed as terminal).
    assert_eq!(
        apply(&journal, &executor, &[new_decision]).await,
        counts_with_terminal_failed_skipped(0, 1, 0, 1)
    );
    let intent = journal
        .get(&ikey(key))
        .await
        .expect("get")
        .expect("intent still journaled");
    assert_eq!(intent.status, IntentStatus::Failed);
    assert_eq!(intent.max_fee, Some(Msat(1)));
    assert!(executor.performed_keys().is_empty());
}

#[tokio::test]
async fn awaiting_outcome_leaves_intent_awaiting_and_is_not_redriven() {
    let key = "directinflow:to=2:42";
    let decisions = vec![move_decision(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    executor.set_awaiting(key);

    // Ok(Awaiting): the effect ran (invoice minted) but the external payer hasn't paid.
    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(1, 0, 0)
    );
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Awaiting
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);

    // Awaiting is subscription-owned: reconcile drives pending() only and must skip it,
    // so the effect is not performed a second time.
    assert_eq!(reconcile(&journal, &executor).await, counts(0, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Awaiting
    );
}

#[tokio::test]
async fn awaiting_replay_after_crash_preserves_awaiting_status() {
    let key = "directinflow:to=2:42";
    let mut intent = Intent::from_decision(&move_decision(key, 42));
    intent.status = IntentStatus::Executing;

    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    executor.set_awaiting(key);
    journal.upsert(&intent).await.unwrap();

    // Simulate the crash window after the external effect started but before the
    // journal persisted `Awaiting`: the key has been performed, journal is Executing.
    assert_eq!(
        executor.perform(&intent).await.unwrap(),
        PerformOutcome::Awaiting
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Executing
    );

    // Reconcile replays the same key. The mock must preserve the original Awaiting
    // outcome instead of reporting Done, and must not record another side effect.
    assert_eq!(reconcile(&journal, &executor).await, counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Awaiting
    );
}

/// The single-writer CAS claim: two concurrent `drive`s of the same `Pending` intent must
/// yield exactly one `perform`. `BarrierJournal` forces both `reconcile` calls to reach their
/// CAS claim together (both have already fetched the `Pending` intent via `pending()` before
/// either attempts the claim), so this exercises the real race, not two serialized passes.
#[tokio::test]
async fn concurrent_drive_performs_once() {
    let key = "move:1:2:42";
    let intent = Intent::from_decision(&move_decision(key, 42));
    let inner = MemJournal::new();
    inner.upsert(&intent).await.unwrap();
    let journal = Arc::new(BarrierJournal {
        inner,
        barrier: Barrier::new(2),
    });
    let executor = Arc::new(MockExecutor::new());

    let (j1, e1) = (Arc::clone(&journal), Arc::clone(&executor));
    let (j2, e2) = (Arc::clone(&journal), Arc::clone(&executor));
    let task1 = tokio::spawn(async move { reconcile(j1.as_ref(), e1.as_ref()).await });
    let task2 = tokio::spawn(async move { reconcile(j2.as_ref(), e2.as_ref()).await });
    let (a, b) = tokio::join!(task1, task2);
    let (a, b) = (a.expect("task1 join"), b.expect("task2 join"));

    let combined = ExecutionSummary {
        performed: a.performed + b.performed,
        skipped: a.skipped + b.skipped,
        failed: a.failed + b.failed,
        terminal_failed_skipped: a.terminal_failed_skipped + b.terminal_failed_skipped,
        retryable: a.retryable + b.retryable,
    };
    assert_eq!(
        combined,
        counts(1, 1, 0),
        "exactly one of the two concurrent drives performs; the other loses the CAS and skips"
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Done
    );
}

/// Once a first driver has claimed `Pending -> Executing`, ordinary `apply`/`reconcile`
/// can observe that `Executing` row before the first `perform` stores a terminal status.
/// That second driver must skip the live in-process operation, while a later crash-recovery
/// pass with no in-flight guard still resumes `Executing`.
#[tokio::test]
async fn concurrent_executing_drive_skips_in_flight_perform() {
    let key = "executing-race:1";
    let mut intent = Intent::from_decision(&move_decision(key, 42));
    intent.status = IntentStatus::Executing;

    let journal = Arc::new(MemJournal::new());
    journal.upsert(&intent).await.unwrap();
    let executor = Arc::new(BlockingExecutor::new());

    let (j1, e1) = (Arc::clone(&journal), Arc::clone(&executor));
    let task1 = tokio::spawn(async move { reconcile(j1.as_ref(), e1.as_ref()).await });

    executor.first_entered.wait().await;

    let second = reconcile(journal.as_ref(), executor.as_ref()).await;
    assert_eq!(
        second,
        counts(0, 1, 0),
        "a concurrent Executing driver must skip the in-flight perform"
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);

    executor.release_first.wait().await;
    let first = task1.await.expect("task1 join");
    assert_eq!(first, counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Done
    );
}

/// A stale `Executing` snapshot captured while another driver is in `perform` must not run
/// after that first driver completes and releases the process-local in-flight guard.
#[tokio::test]
async fn concurrent_executing_stale_snapshot_skips_after_completion() {
    let key = "executing-stale:1";
    let mut intent = Intent::from_decision(&move_decision(key, 42));
    intent.status = IntentStatus::Executing;

    let journal = Arc::new(MemJournal::new());
    journal.upsert(&intent).await.unwrap();
    let executor = Arc::new(BlockingExecutor::new());

    let (j1, e1) = (Arc::clone(&journal), Arc::clone(&executor));
    let task1 = tokio::spawn(async move { reconcile(j1.as_ref(), e1.as_ref()).await });

    executor.first_entered.wait().await;

    let delayed = Arc::new(DelayedPendingJournal {
        inner: Arc::clone(&journal),
        snapshot_taken: Barrier::new(2),
        release_snapshot: Barrier::new(2),
    });
    let (j2, e2) = (Arc::clone(&delayed), Arc::clone(&executor));
    let task2 = tokio::spawn(async move { reconcile(j2.as_ref(), e2.as_ref()).await });

    delayed.snapshot_taken.wait().await;

    executor.release_first.wait().await;
    let first = task1.await.expect("task1 join");
    assert_eq!(first, counts(1, 0, 0));
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Done
    );

    delayed.release_snapshot.wait().await;
    let second = task2.await.expect("task2 join");
    assert_eq!(
        second,
        counts(0, 1, 0),
        "a stale Executing snapshot must skip after the row is already terminal"
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
}

/// `set_status_if` itself: wins on a matching `expected`, moving the status (and, for a
/// durable journal, the pending index); loses (no change) on a mismatch, including on an
/// intent that has already moved past `expected`; and returns `Ok(false)` for an absent key.
#[tokio::test]
async fn set_status_if_cas() {
    let key = ikey("move:1:2:42");
    let intent = Intent::from_decision(&move_decision("move:1:2:42", 42));
    let journal = MemJournal::new();
    journal.upsert(&intent).await.unwrap();

    assert_eq!(
        journal
            .set_status_if(&key, IntentStatus::Pending, IntentStatus::Executing)
            .await,
        Ok(true)
    );
    assert_eq!(
        journal.get(&key).await.expect("get").unwrap().status,
        IntentStatus::Executing
    );

    // Already Executing: a second claim against the stale `expected` (Pending) must not win.
    assert_eq!(
        journal
            .set_status_if(&key, IntentStatus::Pending, IntentStatus::Executing)
            .await,
        Ok(false)
    );
    assert_eq!(
        journal.get(&key).await.expect("get").unwrap().status,
        IntentStatus::Executing
    );

    // An absent key never matches any `expected`.
    let missing = ikey("no-such-key");
    assert_eq!(
        journal
            .set_status_if(&missing, IntentStatus::Pending, IntentStatus::Executing)
            .await,
        Ok(false)
    );
}
