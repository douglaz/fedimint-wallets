use wallet_core::*;

fn ikey(key: &str) -> IdempotencyKey {
    IdempotencyKey(key.to_string())
}

fn decision(key: &str, action: Action, reason: ReasonCode) -> AllocatorDecision {
    AllocatorDecision {
        action,
        reason,
        max_fee: Msat(7),
        idempotency_key: ikey(key),
        requires_auth: false,
    }
}

fn topup(key: &str, amount: u64) -> AllocatorDecision {
    decision(
        key,
        Action::TopUpSpending {
            from: FederationId([1; 32]),
            to: FederationId([2; 32]),
            amount: Msat(amount),
        },
        ReasonCode::SpendingBelowTarget,
    )
}

fn counts(performed: usize, skipped: usize, failed: usize) -> ExecutionSummary {
    ExecutionSummary {
        performed,
        skipped,
        failed,
    }
}

#[tokio::test]
async fn apply_fresh_decision_journals_and_performs_once() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(1, 0, 0)
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    let intent = journal.get(&ikey(key)).await.expect("intent is journaled");
    assert_eq!(intent.idempotency_key, ikey(key));
    assert_eq!(intent.action, decisions[0].action);
    assert_eq!(intent.max_fee, decisions[0].max_fee);
    assert_eq!(intent.status, IntentStatus::Done);
}

#[tokio::test]
async fn applying_same_decisions_twice_performs_only_once() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42)];
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
async fn duplicate_keys_in_one_apply_are_performed_once() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42), topup(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(1, 1, 0)
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn reconcile_redrives_executing_intent_after_crash() {
    let key = "topup:1:2:42";
    let mut intent = Intent::from_decision(&topup(key, 42));
    intent.status = IntentStatus::Executing;

    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    journal.upsert(&intent).await.unwrap();

    assert_eq!(reconcile(&journal, &executor).await, counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn retryable_failure_stays_pending_and_reconcile_retries() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    executor.fail_retryable(key);

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(0, 0, 1)
    );
    // A Retryable error leaves the intent Pending (NOT Failed), so reconcile retries it.
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Pending
    );
    assert!(executor.performed_keys().is_empty());

    executor.succeed(key);
    assert_eq!(reconcile(&journal, &executor).await, counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn permanent_failure_is_terminal_and_reconcile_does_not_redrive() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    executor.fail_permanent(key);

    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(0, 0, 1)
    );
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Failed
    );

    // Even with the injection cleared, reconcile must NOT re-drive a Failed intent: it
    // is terminal until a manual retry resets it to Pending.
    executor.succeed(key);
    assert_eq!(reconcile(&journal, &executor).await, counts(0, 0, 0));
    assert!(executor.performed_keys().is_empty());
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Failed
    );
}

#[tokio::test]
async fn apply_skips_decision_when_key_is_already_done() {
    let key = "topup:1:2:42";
    let mut intent = Intent::from_decision(&topup(key, 42));
    intent.status = IntentStatus::Done;

    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    journal.upsert(&intent).await.unwrap();

    assert_eq!(
        apply(&journal, &executor, &[topup(key, 42)]).await,
        counts(0, 1, 0)
    );
    assert!(executor.performed_keys().is_empty());
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn apply_does_not_resurrect_failed_intent() {
    let key = "topup:1:2:42";
    let mut old_decision = topup(key, 42);
    old_decision.max_fee = Msat(1);
    let mut failed_intent = Intent::from_decision(&old_decision);
    failed_intent.status = IntentStatus::Failed;

    let mut new_decision = topup(key, 42);
    new_decision.max_fee = Msat(99);
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
        .expect("intent still journaled");
    assert_eq!(intent.status, IntentStatus::Failed);
    assert_eq!(intent.max_fee, Msat(1));
    assert!(executor.performed_keys().is_empty());
}

#[tokio::test]
async fn awaiting_outcome_leaves_intent_awaiting_and_is_not_redriven() {
    let key = "directinflow:to=2:42";
    let decisions = vec![topup(key, 42)];
    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    executor.set_awaiting(key);

    // Ok(Awaiting): the effect ran (invoice minted) but the external payer hasn't paid.
    assert_eq!(
        apply(&journal, &executor, &decisions).await,
        counts(1, 0, 0)
    );
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Awaiting
    );
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);

    // Awaiting is subscription-owned: reconcile drives pending() only and must skip it,
    // so the effect is not performed a second time.
    assert_eq!(reconcile(&journal, &executor).await, counts(0, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Awaiting
    );
}

#[tokio::test]
async fn awaiting_replay_after_crash_preserves_awaiting_status() {
    let key = "directinflow:to=2:42";
    let mut intent = Intent::from_decision(&topup(key, 42));
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
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Executing
    );

    // Reconcile replays the same key. The mock must preserve the original Awaiting
    // outcome instead of reporting Done, and must not record another side effect.
    assert_eq!(reconcile(&journal, &executor).await, counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), vec![ikey(key)]);
    assert_eq!(
        journal.get(&ikey(key)).await.unwrap().status,
        IntentStatus::Awaiting
    );
}
