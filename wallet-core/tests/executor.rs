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
            diagnostics: Default::default(),
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

fn intent_for(action: Action, status: IntentStatus) -> Intent {
    let decision = decision("reservation", action, ReasonCode::UserInitiated);
    let mut intent = Intent::from_decision(&decision, Actor::User, 0);
    intent.status = status;
    intent
}

fn move_record(intent: &Intent, phase: MovePhase) -> MoveRecord {
    let (from, to, amount, fee_cap, send_required) = match intent.action {
        Action::Move {
            from,
            to,
            amount,
            fee_cap,
        }
        | Action::Evacuate {
            from,
            to,
            amount,
            fee_cap,
        } => (Some(from), to, amount, fee_cap, true),
        Action::DirectInflow {
            to,
            amount,
            fee_cap,
        } => (None, to, amount, fee_cap, false),
        _ => panic!("test record requires a move-shaped action"),
    };
    MoveRecord {
        key: intent.idempotency_key.clone(),
        from,
        to,
        amount,
        fee_cap,
        gateway: GatewayUrl("https://gateway.invalid".into()),
        send_required,
        invoice: None,
        recv_op: None,
        send_op: None,
        phase,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    }
}

#[test]
fn reservation_projection_covers_every_phase6a_table_row() {
    let from = FederationId([1; 32]);
    let to = FederationId([2; 32]);
    let move_action = Action::Move {
        from,
        to,
        amount: Msat(100),
        fee_cap: Msat(7),
    };

    for status in [IntentStatus::Pending, IntentStatus::Executing] {
        for phase in [None, Some(MovePhase::Created), Some(MovePhase::Invoiced)] {
            let intent = intent_for(move_action.clone(), status);
            let record = phase.map(|phase| move_record(&intent, phase));
            let reservations =
                project_reservations(std::slice::from_ref(&intent), |_| record.clone());
            assert_eq!(reservations.outbound(from), Msat(107));
            assert_eq!(reservations.inbound(to), Msat(100));
        }
    }

    let sending = intent_for(move_action.clone(), IntentStatus::Executing);
    let sending_record = move_record(&sending, MovePhase::Sending);
    let reservations = project_reservations(std::slice::from_ref(&sending), |_| {
        Some(sending_record.clone())
    });
    assert_eq!(reservations.outbound(from), Msat(0));
    assert_eq!(reservations.inbound(to), Msat(100));

    for phase in [
        MovePhase::Settled,
        MovePhase::Refunded,
        MovePhase::Failed,
        MovePhase::Stranded,
    ] {
        let intent = intent_for(move_action.clone(), IntentStatus::Executing);
        let record = move_record(&intent, phase);
        let reservations =
            project_reservations(std::slice::from_ref(&intent), |_| Some(record.clone()));
        assert_eq!(reservations, Reservations::default());
    }

    for status in [IntentStatus::Done, IntentStatus::Failed] {
        let intent = intent_for(move_action.clone(), status);
        let reservations = project_reservations(std::slice::from_ref(&intent), |_| None);
        assert_eq!(reservations, Reservations::default());
    }

    let inflow = intent_for(
        Action::DirectInflow {
            to,
            amount: Msat(55),
            fee_cap: Msat(3),
        },
        IntentStatus::Awaiting,
    );
    let reservations = project_reservations(std::slice::from_ref(&inflow), |_| None);
    assert_eq!(reservations.outbound(from), Msat(0));
    assert_eq!(reservations.inbound(to), Msat(55));

    let evacuate = intent_for(
        Action::Evacuate {
            from,
            to,
            amount: Msat(40),
            fee_cap: Msat(9),
        },
        IntentStatus::Pending,
    );
    let reservations = project_reservations(std::slice::from_ref(&evacuate), |_| None);
    assert_eq!(reservations.outbound(from), Msat(0));
    assert_eq!(reservations.inbound(to), Msat(40));

    let mut downsized_evacuation = move_record(&evacuate, MovePhase::Created);
    downsized_evacuation.amount = Msat(25);
    let reservations = project_reservations(std::slice::from_ref(&evacuate), |_| {
        Some(downsized_evacuation.clone())
    });
    assert_eq!(reservations.outbound(from), Msat(0));
    assert_eq!(
        reservations.inbound(to),
        Msat(25),
        "the durable executable amount replaces the intent's original evacuation ask"
    );

    let receive = intent_for(
        Action::Receive {
            to,
            amount: Msat(70),
            fee_cap: Msat(4),
            nonce: "receive-nonce".into(),
            gateway: None,
        },
        IntentStatus::Awaiting,
    );
    let reservations = project_reservations(std::slice::from_ref(&receive), |_| None);
    assert_eq!(reservations.outbound(from), Msat(0));
    assert_eq!(reservations.inbound(to), Msat(70));

    let join = intent_for(
        Action::Join {
            federation: to,
            invite: "invite".into(),
            membership_preexisting: false,
        },
        IntentStatus::Executing,
    );
    assert_eq!(
        project_reservations(std::slice::from_ref(&join), |_| None),
        Reservations::default()
    );
}

#[test]
fn raw_pay_reservation_ends_at_the_durable_send_artifact() {
    let from = FederationId([1; 32]);
    let action = Action::Pay {
        from,
        invoice: Invoice("lnbc1fixture".into()),
        amount: Msat(100),
        fee_cap: Msat(7),
        payment_hash: [9; 32],
        gateway: None,
    };
    let pre_fund = intent_for(action.clone(), IntentStatus::Pending);
    assert_eq!(
        project_reservations(std::slice::from_ref(&pre_fund), |_| None).outbound(from),
        Msat(107)
    );

    let mut post_fund = pre_fund;
    post_fund.operation_id = Some(OperationId([3; 32]));
    assert_eq!(
        project_reservations(std::slice::from_ref(&post_fund), |_| None).outbound(from),
        Msat(0)
    );
}

#[derive(Default)]
struct GetFailsJournal {
    upserts: Mutex<usize>,
}

#[derive(Clone, Copy)]
enum ScanFailure {
    Pending,
    Awaiting,
    MoveRecord,
}

struct ScanFailsJournal {
    inner: MemJournal,
    failure: ScanFailure,
}

#[async_trait]
impl Journal for ScanFailsJournal {
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
        error: Option<&str>,
    ) -> Result<(), ExecError> {
        self.inner.set_status(key, status, error).await
    }

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        self.inner.set_status_if(key, expected, new).await
    }

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        if matches!(self.failure, ScanFailure::Pending) {
            return Err(ExecError::Retryable("pending scan unavailable".into()));
        }
        self.inner.pending().await
    }

    async fn awaiting(&self) -> Result<Vec<Intent>, ExecError> {
        if matches!(self.failure, ScanFailure::Awaiting) {
            return Err(ExecError::Retryable("awaiting scan unavailable".into()));
        }
        self.inner.awaiting().await
    }

    async fn failed(&self) -> Vec<Intent> {
        self.inner.failed().await
    }

    async fn move_record(&self, key: &IdempotencyKey) -> Result<Option<MoveRecord>, ExecError> {
        if matches!(self.failure, ScanFailure::MoveRecord) {
            return Err(ExecError::Retryable("move record unavailable".into()));
        }
        self.inner.move_record(key).await
    }
}

fn pay_decision(key: &str, from: FederationId, amount: u64, fee_cap: u64) -> AllocatorDecision {
    decision(
        key,
        Action::Pay {
            from,
            invoice: Invoice(format!("lnbc1{key}")),
            amount: Msat(amount),
            fee_cap: Msat(fee_cap),
            payment_hash: [key.len() as u8; 32],
            gateway: None,
        },
        ReasonCode::UserInitiated,
    )
}

#[tokio::test]
async fn admission_fails_closed_when_any_reservation_scan_read_errors() {
    for (failure, expected) in [
        (ScanFailure::Pending, "pending scan unavailable"),
        (ScanFailure::Awaiting, "awaiting scan unavailable"),
        (ScanFailure::MoveRecord, "move record unavailable"),
    ] {
        let journal = ScanFailsJournal {
            inner: MemJournal::new(),
            failure,
        };
        if matches!(failure, ScanFailure::MoveRecord) {
            let existing = move_decision("existing-move", 10);
            journal
                .inner
                .upsert(&Intent::from_decision(&existing, Actor::User, 0))
                .await
                .expect("seed pending move");
        }
        let decision = pay_decision("pay-a", FederationId([1; 32]), 10, 1);
        let balances = std::collections::BTreeMap::from([(FederationId([1; 32]), Msat(100))]);
        let error = decide_and_journal(&journal, &decision, Actor::User, 0, Some(&balances), None)
            .await
            .expect_err("scan failure must refuse admission");
        assert_eq!(error, ExecError::Retryable(expected.into()));
        assert_eq!(journal.inner.get(&ikey("pay-a")).await.expect("get"), None);
    }
}

#[tokio::test]
async fn cross_operation_reservations_gate_source_and_destination_admission() {
    let from = FederationId([1; 32]);
    let to = FederationId([2; 32]);
    let balances = std::collections::BTreeMap::from([(from, Msat(150)), (to, Msat(30))]);

    let journal = MemJournal::new();
    let first = pay_decision("pay-a", from, 100, 10);
    decide_and_journal(&journal, &first, Actor::User, 0, Some(&balances), None)
        .await
        .expect("first pay admitted");
    let second = pay_decision("pay-bb", from, 50, 1);
    assert!(matches!(
        decide_and_journal(&journal, &second, Actor::User, 0, Some(&balances), None).await,
        Err(ExecError::Permanent(reason)) if reason.contains("insufficient balance after reservations")
    ));

    journal
        .set_operation_artifact(&first.idempotency_key, OperationId([8; 32]), None)
        .await
        .expect("funding artifact");
    decide_and_journal(&journal, &second, Actor::User, 0, Some(&balances), None)
        .await
        .expect("post-fund pay no longer double counts the live balance debit");

    let inflow = decision(
        "inflow-a",
        Action::DirectInflow {
            to,
            amount: Msat(60),
            fee_cap: Msat(1),
        },
        ReasonCode::UserInitiated,
    );
    let mut inflow_intent = Intent::from_decision(&inflow, Actor::User, 0);
    inflow_intent.status = IntentStatus::Awaiting;
    journal.upsert(&inflow_intent).await.expect("seed inflow");
    let incoming_move = decision(
        "move-in",
        Action::Move {
            from,
            to,
            amount: Msat(20),
            fee_cap: Msat(1),
        },
        ReasonCode::UserInitiated,
    );
    assert!(matches!(
        decide_and_journal(
            &journal,
            &incoming_move,
            Actor::User,
            0,
            Some(&balances),
            Some(Msat(100)),
        )
        .await,
        Err(ExecError::Permanent(reason)) if reason.contains("per-fed cap after reservations")
    ));

    let evacuation_journal = MemJournal::new();
    let evacuation = decision(
        "evacuation",
        Action::Evacuate {
            from,
            to,
            amount: Msat(40),
            fee_cap: Msat(50),
        },
        ReasonCode::ShutdownNotice,
    );
    let tiny_source = std::collections::BTreeMap::from([(from, Msat(10)), (to, Msat(0))]);
    decide_and_journal(
        &evacuation_journal,
        &evacuation,
        Actor::User,
        0,
        Some(&tiny_source),
        Some(Msat(100)),
    )
    .await
    .expect("evacuation admission must not pre-reserve amount plus fee on its source");
}

#[tokio::test]
async fn new_intent_kinds_attach_only_when_all_sizing_fields_match() {
    let from = FederationId([1; 32]);
    let balances = std::collections::BTreeMap::from([(from, Msat(1_000))]);
    let journal = MemJournal::new();
    let original = pay_decision("same-hash", from, 100, 7);
    decide_and_journal(&journal, &original, Actor::User, 0, Some(&balances), None)
        .await
        .expect("fresh pay");
    assert!(matches!(
        decide_and_journal(&journal, &original, Actor::User, 1, Some(&balances), None).await,
        Ok(DecideAndJournal::Drive(_))
    ));

    let conflicting_action = |from, amount, fee_cap| {
        let Action::Pay {
            invoice,
            payment_hash,
            gateway,
            ..
        } = &original.action
        else {
            unreachable!("fixture is a pay")
        };
        Action::Pay {
            from,
            invoice: invoice.clone(),
            amount: Msat(amount),
            fee_cap: Msat(fee_cap),
            payment_hash: *payment_hash,
            gateway: gateway.clone(),
        }
    };
    for action in [
        conflicting_action(FederationId([9; 32]), 100, 7),
        conflicting_action(from, 101, 7),
        conflicting_action(from, 100, 8),
    ] {
        let conflict = AllocatorDecision {
            action,
            ..original.clone()
        };
        assert!(matches!(
            decide_and_journal(&journal, &conflict, Actor::User, 2, Some(&balances), None).await,
            Err(ExecError::Permanent(reason)) if reason.contains("conflicts")
        ));
    }

    let receive_journal = MemJournal::new();
    let receive = decision(
        "receive-key",
        Action::Receive {
            to: from,
            amount: Msat(50),
            fee_cap: Msat(3),
            nonce: "nonce".into(),
            gateway: None,
        },
        ReasonCode::UserInitiated,
    );
    decide_and_journal(&receive_journal, &receive, Actor::User, 0, None, None)
        .await
        .expect("fresh receive");
    assert!(
        decide_and_journal(&receive_journal, &receive, Actor::User, 1, None, None)
            .await
            .is_ok()
    );
    for conflicting_action in [
        Action::Receive {
            to: FederationId([9; 32]),
            amount: Msat(50),
            fee_cap: Msat(3),
            nonce: "nonce".into(),
            gateway: None,
        },
        Action::Receive {
            to: from,
            amount: Msat(51),
            fee_cap: Msat(3),
            nonce: "nonce".into(),
            gateway: None,
        },
        Action::Receive {
            to: from,
            amount: Msat(50),
            fee_cap: Msat(4),
            nonce: "nonce".into(),
            gateway: None,
        },
    ] {
        let conflict = AllocatorDecision {
            action: conflicting_action,
            ..receive.clone()
        };
        assert!(matches!(
            decide_and_journal(&receive_journal, &conflict, Actor::User, 1, None, None).await,
            Err(ExecError::Permanent(reason)) if reason.contains("conflicts")
        ));
    }

    let join_journal = MemJournal::new();
    let join = decision(
        "join-key",
        Action::Join {
            federation: from,
            invite: "invite-a".into(),
            membership_preexisting: false,
        },
        ReasonCode::UserInitiated,
    );
    decide_and_journal(&join_journal, &join, Actor::User, 0, None, None)
        .await
        .expect("fresh join");
    assert!(
        decide_and_journal(&join_journal, &join, Actor::User, 1, None, None)
            .await
            .is_ok()
    );
    for conflicting_action in [
        Action::Join {
            federation: FederationId([9; 32]),
            invite: "invite-a".into(),
            membership_preexisting: false,
        },
        Action::Join {
            federation: from,
            invite: "invite-b".into(),
            membership_preexisting: false,
        },
    ] {
        let conflict = AllocatorDecision {
            action: conflicting_action,
            ..join.clone()
        };
        assert!(matches!(
            decide_and_journal(&join_journal, &conflict, Actor::User, 1, None, None).await,
            Err(ExecError::Permanent(reason)) if reason.contains("conflicts")
        ));
    }
}

#[tokio::test]
async fn unfunded_raw_pay_attach_can_replace_its_gateway() {
    let from = FederationId([1; 32]);
    let balances = std::collections::BTreeMap::from([(from, Msat(1_000))]);
    let journal = MemJournal::new();
    let mut original = pay_decision("same-hash", from, 100, 7);
    let Action::Pay { gateway, .. } = &mut original.action else {
        unreachable!("fixture is a pay")
    };
    *gateway = Some(wallet_core::GatewayUrl("https://bad.example".into()));
    decide_and_journal(&journal, &original, Actor::User, 0, Some(&balances), None)
        .await
        .expect("fresh pay");

    let mut retry = original.clone();
    let Action::Pay { gateway, .. } = &mut retry.action else {
        unreachable!("fixture is a pay")
    };
    *gateway = Some(wallet_core::GatewayUrl("https://good.example".into()));

    let attached = decide_and_journal(&journal, &retry, Actor::User, 1, Some(&balances), None)
        .await
        .expect("same-sizing pre-fund retry can replace its route");
    let DecideAndJournal::Drive(attached) = attached else {
        panic!("pending raw pay should be re-driven")
    };
    assert!(matches!(
        &attached.action,
        Action::Pay { gateway: Some(gateway), .. } if gateway.0 == "https://good.example"
    ));
    assert_eq!(
        journal
            .get(&original.idempotency_key)
            .await
            .expect("get")
            .expect("intent")
            .action,
        attached.action,
        "the replacement route must be durable before the driver uses it"
    );
}

#[tokio::test]
async fn unfunded_raw_receive_attach_can_replace_its_gateway() {
    let to = FederationId([1; 32]);
    let journal = MemJournal::new();
    let original = decision(
        "receive-key",
        Action::Receive {
            to,
            amount: Msat(50),
            fee_cap: Msat(3),
            nonce: "nonce".into(),
            gateway: Some(wallet_core::GatewayUrl("https://bad.example".into())),
        },
        ReasonCode::UserInitiated,
    );
    decide_and_journal(&journal, &original, Actor::User, 0, None, None)
        .await
        .expect("fresh receive");

    let mut retry = original.clone();
    let Action::Receive { gateway, .. } = &mut retry.action else {
        unreachable!("fixture is a receive")
    };
    *gateway = Some(wallet_core::GatewayUrl("https://good.example".into()));

    let attached = decide_and_journal(&journal, &retry, Actor::User, 1, None, None)
        .await
        .expect("same-sizing pre-fund retry can replace its route");
    let DecideAndJournal::Drive(attached) = attached else {
        panic!("pending raw receive should be re-driven")
    };
    assert!(matches!(
        &attached.action,
        Action::Receive { gateway: Some(gateway), .. } if gateway.0 == "https://good.example"
    ));
    assert_eq!(
        journal
            .get(&original.idempotency_key)
            .await
            .expect("get")
            .expect("intent")
            .action,
        attached.action,
        "the replacement route must be durable before the driver uses it"
    );
}

#[tokio::test]
async fn recomposed_apply_matches_explicit_decide_then_drive() {
    for (action, awaiting) in [
        (
            Action::Move {
                from: FederationId([1; 32]),
                to: FederationId([2; 32]),
                amount: Msat(42),
                fee_cap: Msat(7),
            },
            false,
        ),
        (
            Action::DirectInflow {
                to: FederationId([2; 32]),
                amount: Msat(42),
                fee_cap: Msat(7),
            },
            true,
        ),
    ] {
        let decision = decision("equivalence", action, ReasonCode::UserInitiated);
        let composed_journal = MemJournal::new();
        let composed_executor = MockExecutor::new();
        if awaiting {
            composed_executor.set_awaiting("equivalence");
        }
        apply(
            &composed_journal,
            &composed_executor,
            std::slice::from_ref(&decision),
            Actor::User,
            99,
        )
        .await;

        let split_journal = MemJournal::new();
        let split_executor = MockExecutor::new();
        if awaiting {
            split_executor.set_awaiting("equivalence");
        }
        let DecideAndJournal::Drive(intent) =
            decide_and_journal(&split_journal, &decision, Actor::User, 99, None, None)
                .await
                .expect("decide")
        else {
            panic!("fresh intent must drive");
        };
        let mut summary = ExecutionSummary::default();
        drive_to_terminal(&split_journal, &split_executor, &intent, &mut summary).await;

        assert_eq!(
            composed_journal
                .get(&decision.idempotency_key)
                .await
                .expect("composed row"),
            split_journal
                .get(&decision.idempotency_key)
                .await
                .expect("split row")
        );
    }
}

struct AlreadyInFlightExecutor;

#[async_trait]
impl Executor for AlreadyInFlightExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        Ok(PerformOutcome::AwaitingAlreadyInFlight)
    }
}

#[tokio::test]
async fn drive_surfaces_already_in_flight_while_persisting_awaiting() {
    let journal = MemJournal::new();
    let decision = pay_decision("sdk-dedup", FederationId([1; 32]), 100, 7);
    let DecideAndJournal::Drive(intent) =
        decide_and_journal(&journal, &decision, Actor::User, 0, None, None)
            .await
            .expect("decide")
    else {
        panic!("fresh intent must drive");
    };
    let outcome = drive_intent_step(
        &journal,
        &AlreadyInFlightExecutor,
        &intent,
        &mut ExecutionSummary::default(),
    )
    .await
    .expect("drive succeeds");
    assert_eq!(outcome, Some(PerformOutcome::AwaitingAlreadyInFlight));
    assert_eq!(
        journal
            .get(&decision.idempotency_key)
            .await
            .expect("get")
            .expect("intent")
            .status,
        IntentStatus::Awaiting
    );
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
        _error: Option<&str>,
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

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        Ok(Vec::new())
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
        error: Option<&str>,
    ) -> Result<(), ExecError> {
        self.inner.set_status(key, status, error).await
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

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
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
        error: Option<&str>,
    ) -> Result<(), ExecError> {
        self.inner.set_status(key, status, error).await
    }

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        self.inner.set_status_if(key, expected, new).await
    }

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        let snapshot = self.inner.pending().await?;
        self.snapshot_taken.wait().await;
        self.release_snapshot.wait().await;
        Ok(snapshot)
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

#[tokio::test]
async fn decide_and_journal_rejects_advisory_actions() {
    // The "advisories are never journaled" guard used to live only in `apply`'s loop;
    // decide_and_journal now enforces it for every caller (step-3 review, round 8).
    let journal = MemJournal::new();
    let decision = AllocatorDecision {
        action: Action::RefuseInflow {
            fed: FederationId([9; 32]),
            reason: ReasonCode::OverCap,
            diagnostics: Default::default(),
        },
        reason: ReasonCode::OverCap,
        occurrence: Occurrence(0),
        idempotency_key: IdempotencyKey("advisory:never".into()),
    };
    assert!(matches!(
        decide_and_journal(&journal, &decision, Actor::User, 0, None, None).await,
        Err(ExecError::Permanent(reason)) if reason.contains("advisory")
    ));
    assert!(journal
        .get(&decision.idempotency_key)
        .await
        .expect("journal read")
        .is_none());
}

#[test]
fn advisory_actions_are_not_executable() {
    let refuse = Action::RefuseInflow {
        fed: FederationId([1; 32]),
        reason: ReasonCode::OverCap,
        diagnostics: Default::default(),
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
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
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
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
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
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
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

    assert_eq!(
        apply(&journal, &executor, &decisions, Actor::User, 0)
            .await
            .performed,
        1
    );
    assert_eq!(
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
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
    let stored_intent = Intent::from_decision(&old_decision, Actor::User, 0);

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
        apply(&journal, &executor, &[new_decision], Actor::User, 0).await,
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
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
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
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
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
    let mut intent = Intent::from_decision(&move_decision(key, 42), Actor::User, 0);
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
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
        counts_with_retryable(0, 0, 1, 1)
    );
    // A Retryable error leaves the intent Pending (NOT Failed), so reconcile retries it.
    assert_eq!(
        journal.get(&ikey(key)).await.expect("get").unwrap().status,
        IntentStatus::Pending
    );
    assert!(executor.performed_keys().is_empty());

    let pending = journal.get(&ikey(key)).await.expect("get").expect("intent");
    let error = drive_intent_step(
        &journal,
        &executor,
        &pending,
        &mut ExecutionSummary::default(),
    )
    .await
    .expect_err("the decomposed driver must surface the retry reason");
    assert_eq!(error, ExecError::Retryable("injected".into()));

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
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
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
    let mut intent = Intent::from_decision(&move_decision(key, 42), Actor::User, 0);
    intent.status = IntentStatus::Done;

    let journal = MemJournal::new();
    let executor = MockExecutor::new();
    journal.upsert(&intent).await.unwrap();

    assert_eq!(
        apply(
            &journal,
            &executor,
            &[move_decision(key, 42)],
            Actor::User,
            0
        )
        .await,
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
    let mut failed_intent = Intent::from_decision(&old_decision, Actor::User, 0);
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
        apply(&journal, &executor, &[new_decision], Actor::User, 0).await,
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
        apply(&journal, &executor, &decisions, Actor::User, 0).await,
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
    let mut intent = Intent::from_decision(&move_decision(key, 42), Actor::User, 0);
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
    let intent = Intent::from_decision(&move_decision(key, 42), Actor::User, 0);
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
    let mut intent = Intent::from_decision(&move_decision(key, 42), Actor::User, 0);
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
    let mut intent = Intent::from_decision(&move_decision(key, 42), Actor::User, 0);
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
    let intent = Intent::from_decision(&move_decision("move:1:2:42", 42), Actor::User, 0);
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

/// §8: an `apply`'d decision yields an `Intent` carrying the actor, the decision's reason, and
/// the creation clock — the ledger identity that used to be dropped.
#[tokio::test]
async fn apply_stamps_actor_reason_and_created_at() {
    let key = "move:1:2:42";
    let decision = decision(
        key,
        Action::Move {
            from: FederationId([1; 32]),
            to: FederationId([2; 32]),
            amount: Msat(42),
            fee_cap: Msat(7),
        },
        ReasonCode::StandbyBelowTarget,
    );
    let journal = MemJournal::new();
    let executor = MockExecutor::new();

    let actor = Actor::Agent {
        occurrence: Occurrence(9),
    };
    apply(&journal, &executor, &[decision], actor, 1_700_000_000_123).await;

    let intent = journal
        .get(&ikey(key))
        .await
        .expect("get")
        .expect("intent journaled");
    assert_eq!(intent.actor, actor);
    assert_eq!(intent.reason, ReasonCode::StandbyBelowTarget);
    assert_eq!(intent.created_at_ms, 1_700_000_000_123);
}

/// A `Journal` wrapper over [`MemJournal`] that records every `(status, error)` passed to
/// `set_status`, so a test can assert `drive` threads the executor's diagnostic (§8.3).
#[derive(Default)]
struct RecordingJournal {
    inner: MemJournal,
    set_status_calls: Mutex<Vec<(IntentStatus, Option<String>)>>,
}

#[async_trait]
impl Journal for RecordingJournal {
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
        error: Option<&str>,
    ) -> Result<(), ExecError> {
        self.set_status_calls
            .lock()
            .expect("mutex poisoned")
            .push((status, error.map(str::to_owned)));
        self.inner.set_status(key, status, error).await
    }

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        self.inner.set_status_if(key, expected, new).await
    }

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        self.inner.pending().await
    }

    async fn failed(&self) -> Vec<Intent> {
        self.inner.failed().await
    }
}

struct UnsupportedExecutor;

#[async_trait]
impl Executor for UnsupportedExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        Err(ExecError::Unsupported)
    }
}

struct TerminalWriteFailsJournal {
    inner: MemJournal,
}

#[async_trait]
impl Journal for TerminalWriteFailsJournal {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError> {
        self.inner.upsert(intent).await
    }

    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        self.inner.get(key).await
    }

    async fn set_status(
        &self,
        _key: &IdempotencyKey,
        _status: IntentStatus,
        _error: Option<&str>,
    ) -> Result<(), ExecError> {
        Err(ExecError::Permanent("terminal status write failed".into()))
    }

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        self.inner.set_status_if(key, expected, new).await
    }

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        self.inner.pending().await
    }

    async fn failed(&self) -> Vec<Intent> {
        self.inner.failed().await
    }
}

#[tokio::test]
async fn drive_propagates_terminal_status_write_failures() {
    for unsupported in [false, true] {
        let key = "move:terminal-write-failure";
        let journal = TerminalWriteFailsJournal {
            inner: MemJournal::new(),
        };
        let intent = Intent::from_decision(&move_decision(key, 42), Actor::User, 0);
        journal.inner.upsert(&intent).await.expect("seed intent");
        let mut summary = ExecutionSummary::default();

        let error = if unsupported {
            drive_intent_step(&journal, &UnsupportedExecutor, &intent, &mut summary)
                .await
                .expect_err("unsupported terminal write must fail")
        } else {
            let executor = MockExecutor::new();
            executor.fail_permanent(key);
            drive_intent_step(&journal, &executor, &intent, &mut summary)
                .await
                .expect_err("permanent terminal write must fail")
        };

        assert_eq!(
            error,
            ExecError::Permanent("terminal status write failed".into())
        );
        assert_eq!(summary.failed, 1);
        assert_eq!(
            journal
                .inner
                .get(&ikey(key))
                .await
                .expect("read intent")
                .expect("intent exists")
                .status,
            IntentStatus::Executing,
            "a failed terminal write must not be reported as durable"
        );
    }
}

/// §8.3: `drive` passes the `ExecError` diagnostic to `set_status` on the `Permanent` and
/// `Unsupported` paths and `None` on the retryable/success paths.
#[tokio::test]
async fn drive_threads_permanent_error_to_set_status() {
    let key = "move:1:2:42";
    let journal = RecordingJournal::default();
    let executor = MockExecutor::new();
    executor.fail_permanent(key);

    apply(
        &journal,
        &executor,
        &[move_decision(key, 42)],
        Actor::User,
        0,
    )
    .await;

    let calls = journal.set_status_calls.lock().expect("mutex poisoned");
    // The Pending→Executing claim goes through `set_status_if`, not `set_status`; the only
    // `set_status` here is the terminal Failed write, which must carry the diagnostic.
    assert_eq!(
        *calls,
        vec![(IntentStatus::Failed, Some("injected".to_string()))]
    );
}

#[tokio::test]
async fn drive_threads_unsupported_error_to_set_status() {
    let key = "move:1:2:42";
    let journal = RecordingJournal::default();
    let executor = UnsupportedExecutor;

    apply(
        &journal,
        &executor,
        &[move_decision(key, 42)],
        Actor::User,
        0,
    )
    .await;

    let calls = journal.set_status_calls.lock().expect("mutex poisoned");
    assert_eq!(
        *calls,
        vec![(
            IntentStatus::Failed,
            Some("executor does not support this action".to_string())
        )]
    );
}

/// The success path threads `None` — no failure to report.
#[tokio::test]
async fn drive_threads_no_error_on_success() {
    let key = "move:1:2:42";
    let journal = RecordingJournal::default();
    let executor = MockExecutor::new();

    apply(
        &journal,
        &executor,
        &[move_decision(key, 42)],
        Actor::User,
        0,
    )
    .await;

    let calls = journal.set_status_calls.lock().expect("mutex poisoned");
    assert_eq!(*calls, vec![(IntentStatus::Done, None)]);
}
