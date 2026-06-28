use wallet_core::*;

fn decision(key: &str, action: Action, reason: ReasonCode) -> AllocatorDecision {
    AllocatorDecision {
        action,
        reason,
        max_fee: Sats(7),
        idempotency_key: key.to_string(),
        requires_auth: false,
    }
}

fn topup(key: &str, amount: u64) -> AllocatorDecision {
    decision(
        key,
        Action::TopUpSpending {
            from: FederationId(1),
            to: FederationId(2),
            amount: Sats(amount),
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

#[test]
fn apply_fresh_decision_journals_and_performs_once() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42)];
    let (mut journal, mut executor) = (MemJournal::new(), MockExecutor::new());

    assert_eq!(
        apply(&mut journal, &mut executor, &decisions),
        counts(1, 0, 0)
    );
    assert_eq!(executor.performed_keys(), &[key.to_string()]);
    let intent = journal.get(key).expect("intent is journaled");
    assert_eq!(intent.idempotency_key, key);
    assert_eq!(intent.action, decisions[0].action);
    assert_eq!(intent.max_fee, decisions[0].max_fee);
    assert_eq!(intent.status, IntentStatus::Done);
}

#[test]
fn applying_same_decisions_twice_performs_only_once() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42)];
    let (mut journal, mut executor) = (MemJournal::new(), MockExecutor::new());

    assert_eq!(apply(&mut journal, &mut executor, &decisions).performed, 1);
    assert_eq!(
        apply(&mut journal, &mut executor, &decisions),
        counts(0, 1, 0)
    );
    assert_eq!(executor.performed_keys(), &[key.to_string()]);
}

#[test]
fn duplicate_keys_in_one_apply_are_performed_once() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42), topup(key, 42)];
    let (mut journal, mut executor) = (MemJournal::new(), MockExecutor::new());

    assert_eq!(
        apply(&mut journal, &mut executor, &decisions),
        counts(1, 1, 0)
    );
    assert_eq!(executor.performed_keys(), &[key.to_string()]);
    assert_eq!(journal.get(key).unwrap().status, IntentStatus::Done);
}

#[test]
fn reconcile_redrives_executing_intent_after_crash() {
    let key = "topup:1:2:42";
    let mut intent = Intent::from_decision(&topup(key, 42));
    intent.status = IntentStatus::Executing;

    let (mut journal, mut executor) = (MemJournal::new(), MockExecutor::new());
    journal.upsert(&intent).unwrap();

    assert_eq!(reconcile(&mut journal, &mut executor), counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), &[key.to_string()]);
    assert_eq!(journal.get(key).unwrap().status, IntentStatus::Done);
}

#[test]
fn perform_failure_is_marked_failed_and_reconcile_can_retry() {
    let key = "topup:1:2:42";
    let decisions = vec![topup(key, 42)];
    let (mut journal, mut executor) = (MemJournal::new(), MockExecutor::new());
    executor.fail(key);

    assert_eq!(
        apply(&mut journal, &mut executor, &decisions),
        counts(0, 0, 1)
    );
    assert_eq!(journal.get(key).unwrap().status, IntentStatus::Failed);
    assert!(executor.performed_keys().is_empty());
    executor.succeed(key);
    assert_eq!(reconcile(&mut journal, &mut executor), counts(1, 0, 0));
    assert_eq!(executor.performed_keys(), &[key.to_string()]);
    assert_eq!(journal.get(key).unwrap().status, IntentStatus::Done);
}

#[test]
fn apply_skips_decision_when_key_is_already_done() {
    let key = "topup:1:2:42";
    let mut intent = Intent::from_decision(&topup(key, 42));
    intent.status = IntentStatus::Done;

    let (mut journal, mut executor) = (MemJournal::new(), MockExecutor::new());
    journal.upsert(&intent).unwrap();

    assert_eq!(
        apply(&mut journal, &mut executor, &[topup(key, 42)]),
        counts(0, 1, 0)
    );
    assert!(executor.performed_keys().is_empty());
    assert_eq!(journal.get(key).unwrap().status, IntentStatus::Done);
}

#[test]
fn apply_refreshes_non_done_intent_before_redrive() {
    let key = "topup:1:2:42";
    let mut old_decision = topup(key, 42);
    old_decision.max_fee = Sats(1);
    let mut old_intent = Intent::from_decision(&old_decision);
    old_intent.status = IntentStatus::Failed;

    let mut new_decision = topup(key, 42);
    new_decision.max_fee = Sats(99);
    let (mut journal, mut executor) = (MemJournal::new(), MockExecutor::new());
    journal.upsert(&old_intent).unwrap();

    assert_eq!(
        apply(&mut journal, &mut executor, &[new_decision.clone()]),
        counts(1, 0, 0)
    );

    let intent = journal.get(key).expect("intent is still journaled");
    assert_eq!(intent.max_fee, new_decision.max_fee);
    assert_eq!(intent.action, new_decision.action);
    assert_eq!(intent.status, IntentStatus::Done);
    assert_eq!(executor.performed_keys(), &[key.to_string()]);
}
