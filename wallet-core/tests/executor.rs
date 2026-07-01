use async_trait::async_trait;
use std::sync::Mutex;
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
        requires_auth: false,
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

    async fn pending(&self) -> Vec<Intent> {
        Vec::new()
    }

    async fn failed(&self) -> Vec<Intent> {
        Vec::new()
    }
}

#[test]
fn direct_inflow_is_executable() {
    let action = Action::DirectInflow {
        to: FederationId([2; 32]),
    };
    assert!(action.is_executable());
    assert_eq!(action.fee_cap(), None);
}

#[test]
fn advisory_actions_are_not_executable() {
    let refuse = Action::RefuseInflow {
        fed: FederationId([1; 32]),
        reason: ReasonCode::OverCap,
    };
    let cap = Action::Cap {
        fed: FederationId([1; 32]),
        reason: ReasonCode::OverCap,
    };
    assert!(!refuse.is_executable());
    assert!(!cap.is_executable());
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
async fn apply_direct_inflow_journals_with_no_fee_cap() {
    // A construct-a-DirectInflow smoke test: it is executable, gets an Intent, and
    // (having no fee_cap of its own) journals with `max_fee: None`.
    let key = "inflow:2:1";
    let decisions = vec![decision(
        key,
        Action::DirectInflow {
            to: FederationId([2; 32]),
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
    assert_eq!(intent.max_fee, None);
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

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(0, 0, 1)
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
        counts(0, 1, 0)
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
