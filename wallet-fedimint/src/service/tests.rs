use super::*;
use async_trait::async_trait;
use fedimint_core::db::mem_impl::MemDatabase;
use fedimint_core::db::IDatabaseTransactionOpsCore;
use fedimint_core::db::IRawDatabaseExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
use wallet_core::{Action, Journal, Occurrence, PerformOutcome, ReasonCode};

#[derive(Default)]
struct SlowExecutor {
    calls: AtomicUsize,
    started: Notify,
}

struct AwaitingExecutor;

#[derive(Default)]
struct SlowJoinExecutor {
    calls: AtomicUsize,
}

#[async_trait]
impl Executor for AwaitingExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        Ok(PerformOutcome::Awaiting)
    }
}

#[async_trait]
impl Executor for SlowJoinExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        assert!(matches!(intent.action, Action::Join { .. }));
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        Ok(PerformOutcome::Done)
    }
}

#[derive(Default)]
struct FailThenSlowExecutor {
    calls: AtomicUsize,
    first_started: Notify,
    release_first: Notify,
}

#[derive(Default)]
struct RetryThenSlowExecutor {
    calls: AtomicUsize,
    first_started: Notify,
    release_first: Notify,
}

#[async_trait]
impl Executor for RetryThenSlowExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            self.first_started.notify_waiters();
            self.release_first.notified().await;
            Err(ExecError::Retryable("retry the attempt".to_owned()))
        } else {
            std::future::pending().await
        }
    }
}

#[async_trait]
impl Executor for FailThenSlowExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            self.first_started.notify_waiters();
            self.release_first.notified().await;
            Err(ExecError::Permanent("first attempt failed".to_owned()))
        } else {
            std::future::pending().await
        }
    }
}

#[async_trait]
impl Executor for SlowExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_waiters();
        std::future::pending().await
    }
}

fn fed(byte: u8) -> FederationId {
    FederationId([byte; 32])
}

fn pay(key: &str, from: FederationId, amount: u64, fee: u64, hash: u8) -> OpRequest {
    OpRequest {
        decision: AllocatorDecision {
            action: Action::Pay {
                from,
                invoice: Invoice(format!("invoice-{hash}")),
                amount: Msat(amount),
                fee_cap: Msat(fee),
                payment_hash: [hash; 32],
                gateway: None,
            },
            reason: ReasonCode::UserInitiated,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey(key.to_owned()),
        },
        actor: Actor::User,
        now_ms: 1,
        balances: BTreeMap::from([(from, Msat(100))]),
        probe_session_nonce: None,
    }
}

fn fixture(executor: Arc<dyn Executor>) -> (WalletService, Arc<FedimintJournal>) {
    fixture_with_timeout(executor, None)
}

fn fixture_with_timeout(
    executor: Arc<dyn Executor>,
    perform_timeout: Option<std::time::Duration>,
) -> (WalletService, Arc<FedimintJournal>) {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let service = WalletService::start_parts(
        None,
        journal.clone(),
        executor,
        Policy {
            per_fed_cap: Msat(1_000),
            spending_target: Msat(100),
            standby_target: Msat(100),
            ..Policy::default()
        },
        perform_timeout,
    );
    (service, journal)
}

fn move_request(
    key: &str,
    action: Action,
    balances: BTreeMap<FederationId, Msat>,
    probe_session_nonce: Option<String>,
) -> OpRequest {
    OpRequest {
        decision: AllocatorDecision {
            action,
            reason: ReasonCode::UserInitiated,
            occurrence: Occurrence(1),
            idempotency_key: IdempotencyKey(key.to_owned()),
        },
        actor: Actor::User,
        now_ms: 2,
        balances,
        probe_session_nonce,
    }
}

async fn registry_size(client: &WalletClient) -> usize {
    match client
        .snapshot(SnapshotScope::Registry)
        .await
        .expect("registry snapshot")
    {
        Snapshot::Registry { drivers } => drivers,
        other => panic!("wrong snapshot: {other:?}"),
    }
}

async fn wait_for_registry(client: &WalletClient, expected: usize) {
    for _ in 0..100 {
        if registry_size(client).await == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("registry did not reach {expected}");
}

async fn decide_probe_ready(
    client: &WalletClient,
    candidate: ProbeCandidate,
) -> ServiceResult<ProbeDecision> {
    loop {
        match client.decide_probe(candidate.clone()).await {
            Err(ServiceError::Storage(message)) if message.contains("still loading") => {
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
}

struct ExitExecutor(Exit);

enum Exit {
    Ok,
    Err,
    Panic,
}

#[async_trait]
impl Executor for ExitExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        match self.0 {
            Exit::Ok => Ok(PerformOutcome::Done),
            Exit::Err => Err(ExecError::Permanent("injected".to_owned())),
            Exit::Panic => panic!("injected driver panic"),
        }
    }
}

#[tokio::test]
async fn two_concurrent_pays_start_without_waiting_for_each_others_io() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, _) = fixture(executor.clone());
    let client = service.client();

    client
        .decide_op(pay("pay:one", fed(1), 40, 5, 1))
        .await
        .expect("first pay admitted");
    client
        .decide_op(pay("pay:two", fed(1), 40, 5, 2))
        .await
        .expect("second pay sizes against the first and is admitted");
    let third = client
        .decide_op(pay("pay:three", fed(1), 40, 5, 3))
        .await
        .expect_err("third pay sees the first two reservations");
    assert!(matches!(
        third,
        ServiceError::Refused {
            reason: RefuseReason::InsufficientAfterReservations,
            ..
        }
    ));

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while executor.calls.load(Ordering::SeqCst) != 2 {
            executor.started.notified().await;
        }
    })
    .await
    .expect("both drivers start promptly");

    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_waits_for_aborted_drivers_before_releasing_the_actor() {
    // Round-8 review: abort() alone races the drain — a not-yet-cancelled driver could
    // submit a transition after the actor exits. abort_then_drain must wait for the
    // Drop guards to empty the registry before the actor is released.
    let executor = Arc::new(SlowExecutor::default());
    let (service, _) = fixture(executor.clone());
    let client = service.client();
    client
        .decide_op(pay("pay:shutdown-race", fed(1), 40, 5, 1))
        .await
        .expect("pay admitted; driver parked in slow IO");
    let registry = service.registry.clone();
    assert_eq!(
        driver::len(&registry),
        1,
        "one driver in flight before shutdown"
    );
    service.shutdown().await.expect("shutdown");
    assert_eq!(
        driver::len(&registry),
        0,
        "shutdown returned while an aborted driver still occupied the registry"
    );
}

#[tokio::test]
async fn pay_is_held_probe_refused_own_leg_passes_and_evacuation_preempts_without_demotion() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor);
    let client = service.client();
    let candidate = fed(1);
    let source = fed(2);
    let probe = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: candidate,
            source,
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(7),
            },
            now_ms: 10,
        },
    )
    .await
    .expect("probe admitted with durable hold");
    assert_eq!(
        journal
            .probe_record(&candidate)
            .await
            .expect("probe row")
            .and_then(|record| record.in_flight)
            .map(|session| session.nonce),
        Some(probe.session.nonce.clone())
    );

    let refusal = client
        .decide_op(pay("pay:held", candidate, 10, 1, 3))
        .await
        .expect_err("ordinary spend from held candidate is refused");
    assert!(matches!(
        refusal,
        ServiceError::Refused {
            reason: RefuseReason::FedHeldByProbe,
            ..
        }
    ));

    client
        .decide_op(move_request(
            "move:probe-out",
            Action::Move {
                from: candidate,
                to: source,
                amount: Msat(10),
                fee_cap: Msat(1),
            },
            BTreeMap::from([(candidate, Msat(100)), (source, Msat(0))]),
            Some(probe.session.nonce.clone()),
        ))
        .await
        .expect("holding session's own OUT leg is exempt");

    let occurrence =
        crate::runtime::occurrence_from_nonce(&probe.session.nonce).expect("generated probe nonce");
    let in_key = crate::runtime::move_key(
        &source,
        &candidate,
        Msat(probe.session.amount_msat),
        Msat(probe.session.leg_fee_cap_msat),
        occurrence,
    );
    journal
        .put_move(&wallet_core::MoveRecord {
            key: in_key,
            from: Some(source),
            to: candidate,
            amount: Msat(probe.session.amount_msat),
            fee_cap: Msat(probe.session.leg_fee_cap_msat),
            gateway: crate::GatewayUrl("https://gw.example".to_owned()),
            send_required: true,
            invoice: Some(Invoice("lnbc1probe".to_owned())),
            recv_op: Some(OperationId([1; 32])),
            send_op: Some(OperationId([2; 32])),
            phase: wallet_core::MovePhase::Settled,
            outcome: None,
            preimage: None,
            receive_fee_quoted: Some(Msat(2)),
            send_fee_quoted: Some(Msat(3)),
        })
        .await
        .expect("seed settled probe leg IN");

    client
        .decide_op(move_request(
            "evacuate:held",
            Action::Evacuate {
                from: candidate,
                to: source,
                amount: Msat(20),
                fee_cap: Msat(1),
            },
            BTreeMap::from([(candidate, Msat(100)), (source, Msat(0))]),
            None,
        ))
        .await
        .expect("evacuation preempts the hold");
    wait_for_registry(&client, 1).await;
    let record = journal
        .probe_record(&candidate)
        .await
        .expect("probe row")
        .expect("probe record retained");
    assert_eq!(record.in_flight, None);
    assert!(record.attempts.is_empty(), "preemption must not demote");
    let umbrella = journal
        .operation(&crate::OperationRef::Key(probe.key))
        .await
        .expect("probe umbrella read")
        .expect("probe umbrella exists");
    assert!(matches!(
        umbrella.kind,
        wallet_core::OperationKind::Probe {
            cost_msat: Some(Msat(cost)),
            ..
        } if cost == probe.session.amount_msat + 5
    ));

    let mut stale_leg = move_request(
        "move:stale-probe-out",
        Action::Move {
            from: candidate,
            to: source,
            amount: Msat(1),
            fee_cap: Msat(0),
        },
        BTreeMap::from([(candidate, Msat(100)), (source, Msat(0))]),
        Some(probe.session.nonce),
    );
    stale_leg.decision.reason = ReasonCode::ActiveProbe;
    let stale = client
        .decide_op(stale_leg)
        .await
        .expect_err("a leg queued after preemption must not restart the resolved probe");
    assert!(stale
        .to_string()
        .contains("probe session is no longer active"));

    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn evacuation_preemption_keeps_real_probe_cost_in_the_live_budget() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let candidate = fed(1);
    let source = fed(2);
    let mut policy = client.get_policy().await.expect("policy");
    policy.max_probe_attempts_per_week = 1;
    policy.max_probe_spend_per_week = Msat(1_000_000);
    client.put_policy(policy).await.expect("tight probe budget");
    let probe = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: candidate,
            source,
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(70),
            },
            now_ms: 10,
        },
    )
    .await
    .expect("probe admitted");
    let occurrence =
        crate::runtime::occurrence_from_nonce(&probe.session.nonce).expect("probe nonce");
    journal
        .put_move(&wallet_core::MoveRecord {
            key: crate::runtime::move_key(
                &source,
                &candidate,
                Msat(probe.session.amount_msat),
                Msat(probe.session.leg_fee_cap_msat),
                occurrence,
            ),
            from: Some(source),
            to: candidate,
            amount: Msat(probe.session.amount_msat),
            fee_cap: Msat(probe.session.leg_fee_cap_msat),
            gateway: crate::GatewayUrl("https://gw.example".to_owned()),
            send_required: true,
            invoice: Some(Invoice("lnbc1probe".to_owned())),
            recv_op: Some(OperationId([1; 32])),
            send_op: Some(OperationId([2; 32])),
            phase: wallet_core::MovePhase::Settled,
            outcome: None,
            preimage: None,
            receive_fee_quoted: Some(Msat(2)),
            send_fee_quoted: Some(Msat(3)),
        })
        .await
        .expect("seed settled probe leg IN");
    client
        .decide_op(move_request(
            "evacuate:budgeted-probe",
            Action::Evacuate {
                from: candidate,
                to: source,
                amount: Msat(20),
                fee_cap: Msat(1),
            },
            BTreeMap::from([(candidate, Msat(100)), (source, Msat(0))]),
            None,
        ))
        .await
        .expect("evacuation preempts probe");

    let error = client
        .decide_probe(ProbeCandidate {
            federation: fed(3),
            source,
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(71),
            },
            now_ms: 11,
        })
        .await
        .expect_err("the preempted probe's actual spend still consumes the attempt budget");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::BudgetExhausted,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn evacuation_preemption_credits_a_settled_probe_return_leg() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let candidate = fed(1);
    let source = fed(2);
    let probe = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: candidate,
            source,
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(73),
            },
            now_ms: 10,
        },
    )
    .await
    .expect("probe admitted");
    let occurrence =
        crate::runtime::occurrence_from_nonce(&probe.session.nonce).expect("probe nonce");
    let in_key = crate::runtime::move_key(
        &source,
        &candidate,
        Msat(probe.session.amount_msat),
        Msat(probe.session.leg_fee_cap_msat),
        occurrence,
    );
    journal
        .put_move(&wallet_core::MoveRecord {
            key: in_key,
            from: Some(source),
            to: candidate,
            amount: Msat(20),
            fee_cap: Msat(probe.session.leg_fee_cap_msat),
            gateway: crate::GatewayUrl("https://gw.example".to_owned()),
            send_required: true,
            invoice: Some(Invoice("lnbc1probe-in".to_owned())),
            recv_op: Some(OperationId([1; 32])),
            send_op: Some(OperationId([2; 32])),
            phase: wallet_core::MovePhase::Settled,
            outcome: None,
            preimage: None,
            receive_fee_quoted: Some(Msat(2)),
            send_fee_quoted: Some(Msat(3)),
        })
        .await
        .expect("seed settled probe leg IN");
    let mut session = probe.session.clone();
    session.out_net_msat = Some(18);
    journal
        .begin_probe_session(&candidate, &session)
        .await
        .expect("persist sized return leg");
    let out_fee_cap =
        crate::runtime::probe_out_fee_cap(Msat(20), Msat(18), Msat(session.leg_fee_cap_msat));
    let out_key = crate::runtime::move_key(&candidate, &source, Msat(18), out_fee_cap, occurrence);
    journal
        .put_move(&wallet_core::MoveRecord {
            key: out_key,
            from: Some(candidate),
            to: source,
            amount: Msat(18),
            fee_cap: out_fee_cap,
            gateway: crate::GatewayUrl("https://gw.example".to_owned()),
            send_required: true,
            invoice: Some(Invoice("lnbc1probe-out".to_owned())),
            recv_op: Some(OperationId([3; 32])),
            send_op: Some(OperationId([4; 32])),
            phase: wallet_core::MovePhase::Settled,
            outcome: None,
            preimage: None,
            receive_fee_quoted: Some(Msat(1)),
            send_fee_quoted: Some(Msat(1)),
        })
        .await
        .expect("seed settled probe leg OUT");

    client
        .decide_op(move_request(
            "evacuate:settled-probe",
            Action::Evacuate {
                from: candidate,
                to: source,
                amount: Msat(20),
                fee_cap: Msat(1),
            },
            BTreeMap::from([(candidate, Msat(100)), (source, Msat(0))]),
            None,
        ))
        .await
        .expect("evacuation preempts probe");
    let umbrella = journal
        .operation(&crate::OperationRef::Key(probe.key))
        .await
        .expect("probe umbrella read")
        .expect("probe umbrella exists");
    assert!(matches!(
        umbrella.kind,
        wallet_core::OperationKind::Probe {
            cost_msat: Some(Msat(7)),
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn reconcile_preempts_a_crash_orphaned_evacuation_before_driving_any_probe_leg() {
    let candidate = fed(1);
    let source = fed(2);
    let (service, journal) = fixture(Arc::new(SlowExecutor::default()));
    let probe = decide_probe_ready(
        &service.client(),
        ProbeCandidate {
            federation: candidate,
            source,
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(72),
            },
            now_ms: 10,
        },
    )
    .await
    .expect("probe admitted");
    service.shutdown().await.expect("simulate process stop");

    let occurrence =
        crate::runtime::occurrence_from_nonce(&probe.session.nonce).expect("probe nonce");
    let leg_decision = AllocatorDecision {
        action: Action::Move {
            from: source,
            to: candidate,
            amount: Msat(probe.session.amount_msat),
            fee_cap: Msat(probe.session.leg_fee_cap_msat),
        },
        reason: ReasonCode::ActiveProbe,
        occurrence,
        idempotency_key: crate::runtime::move_key(
            &source,
            &candidate,
            Msat(probe.session.amount_msat),
            Msat(probe.session.leg_fee_cap_msat),
            occurrence,
        ),
    };
    let leg = Intent::from_decision(
        &leg_decision,
        Actor::Agent {
            occurrence: Occurrence(72),
        },
        11,
    );
    journal.upsert(&leg).await.expect("seed orphaned probe leg");

    let evacuation = move_request(
        "evacuate:crash-window",
        Action::Evacuate {
            from: candidate,
            to: source,
            amount: Msat(20),
            fee_cap: Msat(1),
        },
        BTreeMap::from([(candidate, Msat(100)), (source, Msat(0))]),
        None,
    );
    journal
        .upsert(&Intent::from_decision(
            &evacuation.decision,
            Actor::User,
            12,
        ))
        .await
        .expect("seed committed evacuation");

    let executor = Arc::new(SlowExecutor::default());
    let service = WalletService::start_parts(
        None,
        journal.clone(),
        executor.clone(),
        Policy {
            per_fed_cap: Msat(1_000),
            spending_target: Msat(100),
            standby_target: Msat(100),
            ..Policy::default()
        },
        None,
    );
    let client = service.client();
    let report = client.reconcile().await.expect("reconcile recovery state");
    assert_eq!(report.redriven, 1, "only the evacuation may be driven");
    while executor.calls.load(Ordering::SeqCst) != 1 {
        executor.started.notified().await;
    }
    assert_eq!(
        journal
            .probe_record(&candidate)
            .await
            .expect("probe row")
            .and_then(|record| record.in_flight),
        None,
        "recovery clears the hold before spawning the evacuation"
    );
    assert_eq!(
        journal
            .get(&leg.idempotency_key)
            .await
            .expect("probe leg")
            .expect("probe leg retained for audit")
            .status,
        IntentStatus::Failed,
        "the preempted leg must never be re-driven"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn decide_probe_defers_when_an_existing_intent_spends_candidate() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, _) = fixture(executor);
    let client = service.client();
    client
        .decide_op(pay("pay:existing", fed(1), 10, 1, 4))
        .await
        .expect("pay admitted");
    let error = client
        .decide_probe(ProbeCandidate {
            federation: fed(1),
            source: fed(2),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(8),
            },
            now_ms: 11,
        })
        .await
        .expect_err("retroactive hold cannot start over an existing spend");
    assert!(error.to_string().contains("already spends"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn concurrent_probe_budget_check_and_marker_prevent_double_admission() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let mut policy = client.get_policy().await.expect("policy");
    policy.max_probe_attempts_per_week = 1;
    policy.max_probe_spend_per_week = Msat(1_000_000);
    client.put_policy(policy).await.expect("tight probe budget");
    decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(1),
            source: fed(3),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(9),
            },
            now_ms: 12,
        },
    )
    .await
    .expect("first probe reserves budget and its hold");
    let error = client
        .decide_probe(ProbeCandidate {
            federation: fed(2),
            source: fed(3),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(9),
            },
            now_ms: 12,
        })
        .await
        .expect_err("second concurrent probe sees the first budget reservation");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::BudgetExhausted,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn concurrent_probe_budget_reserves_possible_principal_loss() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let mut policy = client.get_policy().await.expect("policy");
    policy.probe_amount = Msat(500);
    policy.max_fee = Msat(100);
    policy.max_probe_attempts_per_week = 2;
    policy.max_probe_spend_per_week = Msat(650);
    client.put_policy(policy).await.expect("tight probe budget");

    decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(1),
            source: fed(3),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(24),
            },
            now_ms: 12,
        },
    )
    .await
    .expect("one probe fits its worst-case principal-loss reservation");

    let error = client
        .decide_probe(ProbeCandidate {
            federation: fed(2),
            source: fed(3),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(24),
            },
            now_ms: 12,
        })
        .await
        .expect_err("a second probe would exceed the spend budget if both lose principal");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::BudgetExhausted,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn corrupt_ledger_row_fails_closed_during_probe_budget_loading() {
    let db = MemDatabase::new().into_database();
    let app_db = db.clone().with_prefix(vec![0x00]);
    let mut raw_key = vec![0x05];
    raw_key.extend_from_slice(&99_u64.to_be_bytes());
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&raw_key, b"not valid json")
        .await
        .expect("insert corrupt ledger row");
    dbtx.commit_tx_result().await.expect("commit corrupt row");

    let journal = Arc::new(FedimintJournal::new(db));
    let service = WalletService::start_parts(
        None,
        journal,
        Arc::new(SlowExecutor::default()),
        Policy {
            per_fed_cap: Msat(1_000),
            spending_target: Msat(100),
            standby_target: Msat(100),
            ..Policy::default()
        },
        None,
    );
    let client = service.client();
    let error = loop {
        let result = client
            .decide_probe(ProbeCandidate {
                federation: fed(1),
                source: fed(2),
                baseline: Msat(0),
                actor: Actor::Agent {
                    occurrence: Occurrence(23),
                },
                now_ms: 10,
            })
            .await;
        match result {
            Err(ServiceError::Storage(message)) if message.contains("still loading") => {
                tokio::task::yield_now().await;
            }
            Err(error) => break error,
            Ok(_) => panic!("a corrupt budget ledger must never admit an automated probe"),
        }
    };
    assert!(
        error
            .to_string()
            .contains("cannot reconstruct probe budget"),
        "unexpected error: {error}"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn policy_change_does_not_shrink_an_active_probe_budget_reservation() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let first = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(1),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(10),
            },
            now_ms: 13,
        },
    )
    .await
    .expect("first probe reserves its admitted worst-case cost");
    assert_eq!(first.session.leg_fee_cap_msat, 200_000);

    let mut policy = client.get_policy().await.expect("policy");
    policy.max_fee = Msat(50_000);
    client
        .put_policy(policy)
        .await
        .expect("lower probe fee cap");

    decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(2),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(10),
            },
            now_ms: 13,
        },
    )
    .await
    .expect("the remaining budget admits one probe at the new fee cap");
    let error = client
        .decide_probe(ProbeCandidate {
            federation: fed(3),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(10),
            },
            now_ms: 13,
        })
        .await
        .expect_err("the original reservation remains charged at its admitted fee cap");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::BudgetExhausted,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn restart_rehydrates_an_active_probes_original_budget_reservation() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let initial_policy = Policy {
        per_fed_cap: Msat(1_000),
        spending_target: Msat(100),
        standby_target: Msat(100),
        ..Policy::default()
    };
    let first_service = WalletService::start_parts(
        None,
        journal.clone(),
        Arc::new(SlowExecutor::default()),
        initial_policy.clone(),
        None,
    );
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("post-epoch clock")
        .as_millis() as u64;
    decide_probe_ready(
        &first_service.client(),
        ProbeCandidate {
            federation: fed(1),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(11),
            },
            now_ms,
        },
    )
    .await
    .expect("first probe admitted");
    first_service.shutdown().await.expect("first shutdown");

    let mut lowered_policy = initial_policy;
    lowered_policy.max_fee = Msat(50_000);
    let restarted = WalletService::start_parts(
        None,
        journal,
        Arc::new(SlowExecutor::default()),
        lowered_policy,
        None,
    );
    let client = restarted.client();
    decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(2),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(11),
            },
            now_ms,
        },
    )
    .await
    .expect("remaining budget admits one lower-fee probe after restart");
    let error = client
        .decide_probe(ProbeCandidate {
            federation: fed(3),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(11),
            },
            now_ms,
        })
        .await
        .expect_err("rehydrated reservation retains the original fee cap");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::BudgetExhausted,
            ..
        }
    ));
    restarted.shutdown().await.expect("second shutdown");
}

#[tokio::test]
async fn active_probe_budget_reservation_does_not_expire_before_terminalization() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let mut policy = client.get_policy().await.expect("policy");
    policy.max_probe_attempts_per_week = 1;
    policy.max_probe_spend_per_week = Msat(1_000_000);
    client.put_policy(policy).await.expect("tight probe budget");
    decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(1),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(14),
            },
            now_ms: 10,
        },
    )
    .await
    .expect("first probe admitted");

    let error = client
        .decide_probe(ProbeCandidate {
            federation: fed(2),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(15),
            },
            now_ms: crate::runtime::PROBE_BUDGET_WINDOW_MS + 11,
        })
        .await
        .expect_err("an unresolved probe keeps its reservation past the history window");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::BudgetExhausted,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn restart_rehydrates_an_active_probe_older_than_the_budget_window() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let mut policy = Policy {
        per_fed_cap: Msat(1_000),
        spending_target: Msat(100),
        standby_target: Msat(100),
        ..Policy::default()
    };
    policy.max_probe_attempts_per_week = 1;
    policy.max_probe_spend_per_week = Msat(1_000_000);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("post-epoch clock")
        .as_millis() as u64;
    let first_service = WalletService::start_parts(
        None,
        journal.clone(),
        Arc::new(SlowExecutor::default()),
        policy.clone(),
        None,
    );
    decide_probe_ready(
        &first_service.client(),
        ProbeCandidate {
            federation: fed(1),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(16),
            },
            now_ms: now_ms.saturating_sub(crate::runtime::PROBE_BUDGET_WINDOW_MS + 1),
        },
    )
    .await
    .expect("old probe admitted");
    first_service.shutdown().await.expect("first shutdown");

    let restarted = WalletService::start_parts(
        None,
        journal,
        Arc::new(SlowExecutor::default()),
        policy,
        None,
    );
    let error = decide_probe_ready(
        &restarted.client(),
        ProbeCandidate {
            federation: fed(2),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(17),
            },
            now_ms,
        },
    )
    .await
    .expect_err("restart retains an old unresolved probe's reservation");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::BudgetExhausted,
            ..
        }
    ));
    restarted.shutdown().await.expect("second shutdown");
}

#[tokio::test(start_paused = true)]
async fn timeout_deregisters_and_overlapping_reconcile_redrives_once_after_normalizing() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) =
        fixture_with_timeout(executor.clone(), Some(std::time::Duration::from_secs(10)));
    let client = service.client();
    let key = IdempotencyKey("pay:timeout".to_owned());
    client
        .decide_op(pay(&key.0, fed(1), 10, 1, 5))
        .await
        .expect("pay admitted");
    while executor.calls.load(Ordering::SeqCst) != 1 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(std::time::Duration::from_secs(11)).await;
    wait_for_registry(&client, 0).await;
    assert_eq!(
        journal.get(&key).await.expect("intent").unwrap().status,
        IntentStatus::Executing
    );

    let (left, right) = tokio::join!(client.reconcile(), client.reconcile());
    let left = left.expect("first reconcile");
    let right = right.expect("second reconcile");
    assert_eq!(left.redriven + right.redriven, 1);
    assert_eq!(left.executing_normalized + right.executing_normalized, 1);
    while executor.calls.load(Ordering::SeqCst) != 2 {
        tokio::task::yield_now().await;
    }
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn service_perform_timeout_does_not_cancel_join_cleanup() {
    let executor = Arc::new(SlowJoinExecutor::default());
    let (service, journal) =
        fixture_with_timeout(executor.clone(), Some(std::time::Duration::from_secs(10)));
    let client = service.client();
    let key = IdempotencyKey("join:slow".to_owned());
    client
        .decide_op(move_request(
            &key.0,
            Action::Join {
                federation: fed(1),
                invite: "slow-invite".to_owned(),
                membership_preexisting: false,
            },
            BTreeMap::new(),
            None,
        ))
        .await
        .expect("join admitted");
    while executor.calls.load(Ordering::SeqCst) != 1 {
        tokio::task::yield_now().await;
    }

    tokio::time::advance(std::time::Duration::from_secs(11)).await;
    assert_eq!(registry_size(&client).await, 1, "join remains registered");
    assert_eq!(
        journal.get(&key).await.expect("intent").unwrap().status,
        IntentStatus::Executing
    );

    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    wait_for_registry(&client, 0).await;
    assert_eq!(
        journal.get(&key).await.expect("intent").unwrap().status,
        IntentStatus::Done
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn drop_guard_deregisters_ok_driver() {
    assert_drop_guard(Exit::Ok).await;
}

#[tokio::test]
async fn drop_guard_deregisters_err_driver() {
    assert_drop_guard(Exit::Err).await;
}

#[tokio::test]
async fn drop_guard_deregisters_panicking_driver() {
    assert_drop_guard(Exit::Panic).await;
}

#[tokio::test]
async fn panicking_probe_driver_deregisters_without_releasing_its_durable_hold() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let candidate = fed(1);
    let source = fed(2);
    let session = ProbeSession {
        nonce: "panic-hold".to_owned(),
        from: source,
        amount_msat: 20,
        leg_fee_cap_msat: 2,
        c_spendable_before_in_msat: 0,
        out_net_msat: None,
        started_at_ms: 1,
    };
    journal
        .begin_probe_session(&candidate, &session)
        .await
        .expect("seed durable probe hold");

    let registry: driver::Registry = Arc::new(Mutex::new(std::collections::HashMap::new()));
    driver::spawn_registered(
        &registry,
        IdempotencyKey("probe:panic-hold".to_owned()),
        1,
        driver::DriverKind::Probe { candidate },
        async { panic!("injected probe-driver panic") },
    );
    while driver::len(&registry) != 0 {
        tokio::task::yield_now().await;
    }

    let service = WalletService::start_parts(
        None,
        journal,
        Arc::new(SlowExecutor::default()),
        Policy {
            per_fed_cap: Msat(1_000),
            spending_target: Msat(100),
            standby_target: Msat(100),
            ..Policy::default()
        },
        None,
    );
    let refusal = service
        .client()
        .decide_op(pay("pay:after-probe-panic", candidate, 10, 1, 18))
        .await
        .expect_err("driver cleanup must not clear the durable probe hold");
    assert!(matches!(
        refusal,
        ServiceError::Refused {
            reason: RefuseReason::FedHeldByProbe,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

async fn assert_drop_guard(exit: Exit) {
    let (service, _) = fixture(Arc::new(ExitExecutor(exit)));
    let client = service.client();
    client
        .decide_op(pay("pay:drop", fed(1), 10, 1, 6))
        .await
        .expect("pay admitted");
    wait_for_registry(&client, 0).await;
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn terminal_waiters_coalesce_and_already_terminal_resolves_immediately() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let key = IdempotencyKey("pay:await".to_owned());
    client
        .decide_op(pay(&key.0, fed(1), 10, 1, 7))
        .await
        .expect("pay admitted");
    let first = {
        let client = client.clone();
        let key = key.clone();
        tokio::spawn(async move {
            client
                .resolve_await(
                    key,
                    AwaitTarget::Terminal,
                    Instant::now() + std::time::Duration::from_secs(60),
                )
                .await
        })
    };
    let second = {
        let client = client.clone();
        let key = key.clone();
        tokio::spawn(async move {
            client
                .resolve_await(
                    key,
                    AwaitTarget::Terminal,
                    Instant::now() + std::time::Duration::from_secs(60),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    client
        .journal_transition(
            key.clone(),
            JournalTransition::SetStatus {
                status: IntentStatus::Done,
                error: None,
            },
        )
        .await
        .expect("terminal transition");
    for outcome in [
        first.await.unwrap().unwrap(),
        second.await.unwrap().unwrap(),
    ] {
        assert!(matches!(
            outcome,
            AwaitOutcome::Terminal(intent) if intent.status == IntentStatus::Done
        ));
    }
    assert!(matches!(
        client
            .resolve_await(key, AwaitTarget::Terminal, Instant::now())
            .await
            .expect("already terminal"),
        AwaitOutcome::Terminal(_)
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn resolve_await_deadline_returns_timeout() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let key = IdempotencyKey("pay:deadline".to_owned());
    client
        .decide_op(pay(&key.0, fed(1), 10, 1, 8))
        .await
        .expect("pay admitted");
    let waiter = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .resolve_await(
                    key,
                    AwaitTarget::Terminal,
                    Instant::now() + std::time::Duration::from_secs(5),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(6)).await;
    assert_eq!(waiter.await.unwrap(), Err(ServiceError::Timeout));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn invoice_artifact_wait_resolves_on_write_and_when_already_journaled() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let key = IdempotencyKey("pay:invoice".to_owned());
    client
        .decide_op(pay(&key.0, fed(1), 10, 1, 9))
        .await
        .expect("intent admitted");
    let waiter = {
        let client = client.clone();
        let key = key.clone();
        tokio::spawn(async move {
            client
                .resolve_await(
                    key,
                    AwaitTarget::InvoiceArtifact,
                    Instant::now() + std::time::Duration::from_secs(60),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    let invoice = Invoice("bolt11-fixture".to_owned());
    client
        .journal_transition(
            key.clone(),
            JournalTransition::OperationArtifact {
                operation_id: OperationId([9; 32]),
                invoice: Some(invoice.clone()),
            },
        )
        .await
        .expect("artifact transition");
    assert_eq!(
        waiter.await.unwrap().unwrap(),
        AwaitOutcome::Invoice(invoice.clone())
    );
    assert_eq!(
        client
            .resolve_await(key, AwaitTarget::InvoiceArtifact, Instant::now())
            .await
            .expect("already-journaled artifact"),
        AwaitOutcome::Invoice(invoice)
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_drains_parked_waiters_with_errors() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default()));
    let client = service.client();
    let key = IdempotencyKey("pay:shutdown".to_owned());
    client
        .decide_op(pay(&key.0, fed(1), 10, 1, 10))
        .await
        .expect("pay admitted");
    let waiter = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .resolve_await(
                    key,
                    AwaitTarget::Terminal,
                    Instant::now() + std::time::Duration::from_secs(60),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    service.shutdown().await.expect("shutdown");
    assert_eq!(waiter.await.unwrap(), Err(ServiceError::ShuttingDown));
}

#[tokio::test]
async fn shutdown_drain_deregisters_finished_drivers_without_spawning_handoffs() {
    let (service, journal) = fixture(Arc::new(AwaitingExecutor));
    let client = service.client();
    let req = move_request(
        "direct:shutdown-handoff",
        Action::DirectInflow {
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(1),
        },
        BTreeMap::from([(fed(2), Msat(0))]),
        None,
    );
    client
        .decide_op(req.clone())
        .await
        .expect("inflow admitted");
    loop {
        if journal
            .get(&req.decision.idempotency_key)
            .await
            .expect("awaiting intent")
            .is_some_and(|intent| intent.status == IntentStatus::Awaiting)
            && registry_size(&client).await == 1
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    client
        .decide_op(req.clone())
        .await
        .expect("attach requests an awaiter handoff");

    let WalletService {
        client: service_client,
        task,
        registry: _,
    } = service;
    let (shutdown_reply, shutdown_result) = oneshot::channel();
    service_client
        .sender
        .send(Command::Shutdown {
            reply: shutdown_reply,
        })
        .await
        .expect("queue shutdown");
    let (finished_reply, finished_result) = oneshot::channel();
    service_client
        .sender
        .send(Command::JournalTransition {
            key: req.decision.idempotency_key,
            transition: JournalTransition::DriverFinished { generation: 2 },
            reply: finished_reply,
        })
        .await
        .expect("queue late driver completion");
    let (snapshot_reply, snapshot_result) = oneshot::channel();
    service_client
        .sender
        .send(Command::Snapshot {
            scope: SnapshotScope::Registry,
            reply: snapshot_reply,
        })
        .await
        .expect("queue drain snapshot");

    drop(
        shutdown_result
            .await
            .expect("shutdown reply")
            .expect("shutdown token"),
    );
    finished_result
        .await
        .expect("finished reply")
        .expect("finished transition");
    assert_eq!(
        snapshot_result
            .await
            .expect("snapshot reply")
            .expect("snapshot"),
        Snapshot::Registry { drivers: 0 }
    );
    task.await.expect("actor exits after drain");
}

#[tokio::test]
async fn same_key_live_attach_ensures_orphan_is_driven_and_done_dedups() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone());
    let client = service.client();
    let live = pay("pay:live", fed(1), 10, 1, 11);
    client.decide_op(live.clone()).await.expect("first admit");
    let attached = client.decide_op(live).await.expect("live attach");
    assert!(attached.deduplicated);
    while executor.calls.load(Ordering::SeqCst) != 1 {
        tokio::task::yield_now().await;
    }

    let orphan_req = pay("pay:orphan", fed(1), 10, 1, 12);
    let orphan = Intent::from_decision(&orphan_req.decision, Actor::User, 1);
    journal.upsert(&orphan).await.expect("seed orphan");
    client
        .decide_op(orphan_req)
        .await
        .expect("orphan attach ensures driver");
    while executor.calls.load(Ordering::SeqCst) != 2 {
        tokio::task::yield_now().await;
    }

    let done_req = pay("pay:done", fed(1), 10, 1, 13);
    let mut done = Intent::from_decision(&done_req.decision, Actor::User, 1);
    done.status = IntentStatus::Done;
    journal.upsert(&done).await.expect("seed done");
    let dedup = client
        .decide_op(pay("pay:done", fed(1), 99, 7, 13))
        .await
        .expect("done dedup ignores stale sizing inputs");
    assert!(dedup.deduplicated);
    assert_eq!(dedup.status, IntentStatus::Done);
    let wrong_hash = client
        .decide_op(pay("pay:done", fed(1), 99, 7, 14))
        .await
        .expect_err("done dedup validates the payment-hash anchor");
    assert!(wrong_hash.to_string().contains("idempotency anchor"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn failed_manual_retry_refreshes_sizing_but_live_mismatch_conflicts() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor);
    let client = service.client();
    let old = pay("pay:manual", fed(1), 10, 1, 14);
    let mut failed = Intent::from_decision(&old.decision, Actor::User, 1);
    failed.status = IntentStatus::Failed;
    let failed_operation_key = failed.operation_correlation_key();
    journal.upsert(&failed).await.expect("seed failed");
    let retry = pay("pay:manual", fed(1), 20, 2, 14);
    let outcome = client.decide_op(retry).await.expect("manual retry");
    assert!(!outcome.deduplicated);
    let refreshed = journal
        .get(&IdempotencyKey("pay:manual".to_owned()))
        .await
        .expect("read refreshed")
        .unwrap();
    assert!(matches!(
        refreshed.action,
        Action::Pay {
            amount: Msat(20),
            fee_cap: Msat(2),
            ..
        }
    ));
    assert_eq!(refreshed.attempt, 1);
    assert_ne!(
        refreshed.operation_correlation_key(),
        failed_operation_key,
        "a manual retry must not rediscover the failed SDK attempt"
    );
    let retry_rows = journal.history(10, None).await.expect("retry history");
    let retry_rows: Vec<_> = retry_rows
        .into_iter()
        .filter(|row| row.correlation_key.0 == "pay:manual")
        .collect();
    assert_eq!(retry_rows.len(), 2, "failed attempt remains immutable");
    assert_eq!(retry_rows[1].status, wallet_core::OperationStatus::Failed);

    let live = pay("pay:conflict", fed(1), 10, 1, 15);
    client.decide_op(live).await.expect("live admitted");
    let conflict = client
        .decide_op(pay("pay:conflict", fed(1), 11, 1, 15))
        .await
        .expect_err("live sizing mismatch");
    assert!(matches!(
        conflict,
        ServiceError::Refused {
            reason: RefuseReason::SizingConflict { .. },
            ..
        }
    ));

    let inflow = move_request(
        "direct:manual",
        Action::DirectInflow {
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(1),
        },
        BTreeMap::from([(fed(2), Msat(0))]),
        None,
    );
    let mut failed = Intent::from_decision(&inflow.decision, Actor::User, 1);
    failed.status = IntentStatus::Failed;
    journal.upsert(&failed).await.expect("seed failed inflow");
    journal
        .put_move(&wallet_core::MoveRecord {
            key: failed.idempotency_key.clone(),
            from: None,
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(1),
            gateway: crate::GatewayUrl("https://stale.example".to_owned()),
            send_required: false,
            invoice: None,
            recv_op: None,
            send_op: None,
            phase: wallet_core::MovePhase::Failed,
            outcome: Some("old attempt failed".to_owned()),
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
        })
        .await
        .expect("seed failed attempt cache");
    let retried = client
        .decide_op(move_request(
            "direct:manual",
            Action::DirectInflow {
                to: fed(2),
                amount: Msat(10),
                fee_cap: Msat(2),
            },
            BTreeMap::from([(fed(2), Msat(0))]),
            None,
        ))
        .await
        .expect("direct inflow retry may refresh its fee cap");
    assert!(!retried.deduplicated);
    assert_eq!(
        journal
            .get_move(&IdempotencyKey("direct:manual".to_owned()))
            .await
            .expect("read retry cache"),
        None,
        "manual retry resets the failed attempt's derived cache"
    );
    assert!(matches!(
        journal
            .get(&IdempotencyKey("direct:manual".to_owned()))
            .await
            .expect("read retried inflow")
            .unwrap()
            .action,
        Action::DirectInflow {
            fee_cap: Msat(2),
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn terminal_waiter_can_retry_before_the_old_driver_wrapper_exits() {
    let executor = Arc::new(FailThenSlowExecutor::default());
    let (service, _) = fixture(executor.clone());
    let client = service.client();
    let req = pay("pay:retry-race", fed(1), 10, 1, 16);
    client
        .decide_op(req.clone())
        .await
        .expect("first attempt admitted");
    while executor.calls.load(Ordering::SeqCst) == 0 {
        executor.first_started.notified().await;
    }
    let waiter = {
        let client = client.clone();
        let key = req.decision.idempotency_key.clone();
        tokio::spawn(async move {
            client
                .resolve_await(
                    key,
                    AwaitTarget::Terminal,
                    Instant::now() + std::time::Duration::from_secs(60),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    executor.release_first.notify_one();
    assert!(matches!(
        waiter.await.unwrap().unwrap(),
        AwaitOutcome::Terminal(intent) if intent.status == IntentStatus::Failed
    ));

    client
        .decide_op(req)
        .await
        .expect("manual retry registers a replacement driver");
    while executor.calls.load(Ordering::SeqCst) != 2 {
        tokio::task::yield_now().await;
    }
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn pending_attach_can_redrive_before_the_old_driver_wrapper_exits() {
    let executor = Arc::new(RetryThenSlowExecutor::default());
    let (service, journal) = fixture(executor.clone());
    let client = service.client();
    let req = pay("pay:pending-race", fed(1), 10, 1, 17);
    client
        .decide_op(req.clone())
        .await
        .expect("first attempt admitted");
    while executor.calls.load(Ordering::SeqCst) == 0 {
        executor.first_started.notified().await;
    }
    executor.release_first.notify_one();
    loop {
        if journal
            .get(&req.decision.idempotency_key)
            .await
            .expect("read retryable intent")
            .is_some_and(|intent| intent.status == IntentStatus::Pending)
        {
            break;
        }
        tokio::task::yield_now().await;
    }

    client
        .decide_op(req)
        .await
        .expect("same-key attach registers a replacement driver");
    while executor.calls.load(Ordering::SeqCst) != 2 {
        tokio::task::yield_now().await;
    }
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn reconcile_redrives_a_retryable_probe_leg_while_its_umbrella_driver_waits() {
    let executor = Arc::new(RetryThenSlowExecutor::default());
    let (service, journal) = fixture(executor.clone());
    let client = service.client();
    let candidate = fed(1);
    let source = fed(2);
    let probe = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: candidate,
            source,
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(73),
            },
            now_ms: 10,
        },
    )
    .await
    .expect("probe admitted");
    let occurrence =
        crate::runtime::occurrence_from_nonce(&probe.session.nonce).expect("probe nonce");
    let decision = AllocatorDecision {
        action: Action::Move {
            from: source,
            to: candidate,
            amount: Msat(probe.session.amount_msat),
            fee_cap: Msat(probe.session.leg_fee_cap_msat),
        },
        reason: ReasonCode::ActiveProbe,
        occurrence,
        idempotency_key: crate::runtime::move_key(
            &source,
            &candidate,
            Msat(probe.session.amount_msat),
            Msat(probe.session.leg_fee_cap_msat),
            occurrence,
        ),
    };
    let intent = Intent::from_decision(
        &decision,
        Actor::Agent {
            occurrence: Occurrence(73),
        },
        11,
    );
    journal.upsert(&intent).await.expect("seed probe leg");

    assert_eq!(client.reconcile().await.unwrap().redriven, 1);
    while executor.calls.load(Ordering::SeqCst) == 0 {
        executor.first_started.notified().await;
    }
    executor.release_first.notify_one();
    wait_for_registry(&client, 1).await;
    assert_eq!(
        journal
            .get(&intent.idempotency_key)
            .await
            .expect("probe leg")
            .expect("probe leg exists")
            .status,
        IntentStatus::Pending
    );

    assert_eq!(client.reconcile().await.unwrap().redriven, 1);
    while executor.calls.load(Ordering::SeqCst) != 2 {
        tokio::task::yield_now().await;
    }

    client
        .decide_op(move_request(
            "evacuate:recovered-probe-leg",
            Action::Evacuate {
                from: candidate,
                to: source,
                amount: Msat(10),
                fee_cap: Msat(1),
            },
            BTreeMap::from([(candidate, Msat(100)), (source, Msat(0))]),
            None,
        ))
        .await
        .expect("evacuation preempts the recovered probe leg");
    wait_for_registry(&client, 1).await;
    assert!(
        journal
            .probe_record(&candidate)
            .await
            .expect("probe record")
            .and_then(|record| record.in_flight)
            .is_none(),
        "evacuation resolves the durable probe session"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn reconcile_rehydrates_awaiters_once() {
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok)));
    let client = service.client();
    let req = move_request(
        "direct:awaiting",
        Action::DirectInflow {
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(1),
        },
        BTreeMap::new(),
        None,
    );
    let mut intent = Intent::from_decision(&req.decision, Actor::User, 1);
    intent.status = IntentStatus::Awaiting;
    journal.upsert(&intent).await.expect("seed awaiting");
    assert_eq!(client.reconcile().await.unwrap().awaiters_rehydrated, 1);
    assert_eq!(client.reconcile().await.unwrap().awaiters_rehydrated, 0);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn newly_awaiting_intent_hands_off_to_an_awaiter_before_releasing_ownership() {
    let (service, journal) = fixture(Arc::new(AwaitingExecutor));
    let client = service.client();
    let req = move_request(
        "direct:new-awaiting",
        Action::DirectInflow {
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(1),
        },
        BTreeMap::from([(fed(2), Msat(0))]),
        None,
    );
    let key = req.decision.idempotency_key.clone();
    client.decide_op(req).await.expect("inflow admitted");
    loop {
        if journal
            .get(&key)
            .await
            .expect("awaiting intent")
            .is_some_and(|intent| intent.status == IntentStatus::Awaiting)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    wait_for_registry(&client, 1).await;
    assert_eq!(client.reconcile().await.unwrap().awaiters_rehydrated, 0);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn externally_admitted_awaiters_continue_to_consume_the_driver_cap() {
    let (service, _) = fixture(Arc::new(AwaitingExecutor));
    let client = service.client();
    for index in 0..EXTERNAL_DRIVER_CAP {
        client
            .decide_op(move_request(
                &format!("direct:cap-{index}"),
                Action::DirectInflow {
                    to: fed(2),
                    amount: Msat(1),
                    fee_cap: Msat(0),
                },
                BTreeMap::from([(fed(2), Msat(0))]),
                None,
            ))
            .await
            .expect("fill external cap with inflow subscriptions");
    }
    wait_for_registry(&client, EXTERNAL_DRIVER_CAP).await;
    let error = client
        .decide_op(move_request(
            "direct:over-cap",
            Action::DirectInflow {
                to: fed(2),
                amount: Msat(1),
                fee_cap: Msat(0),
            },
            BTreeMap::from([(fed(2), Msat(0))]),
            None,
        ))
        .await
        .expect_err("long-lived external awaiters retain their admission slots");
    assert!(error.to_string().contains("admission cap"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn evacuation_bypasses_a_full_external_driver_cap_for_fresh_and_retry_requests() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor);
    let client = service.client();
    let retry = move_request(
        "evacuate:retry-at-cap",
        Action::Evacuate {
            from: fed(2),
            to: fed(3),
            amount: Msat(1),
            fee_cap: Msat(0),
        },
        BTreeMap::from([(fed(2), Msat(10)), (fed(3), Msat(0))]),
        None,
    );
    let failed = Intent::from_decision(&retry.decision, Actor::User, 1);
    journal.upsert(&failed).await.expect("seed retry intent");
    journal
        .set_status(
            &retry.decision.idempotency_key,
            IntentStatus::Failed,
            Some("injected failure"),
        )
        .await
        .expect("fail retry intent");

    for index in 0..EXTERNAL_DRIVER_CAP {
        client
            .decide_op(pay(
                &format!("pay:evac-cap-{index}"),
                fed(1),
                1,
                0,
                index as u8,
            ))
            .await
            .expect("fill external driver cap");
    }
    wait_for_registry(&client, EXTERNAL_DRIVER_CAP).await;

    client
        .decide_op(move_request(
            "evacuate:fresh-at-cap",
            Action::Evacuate {
                from: fed(2),
                to: fed(3),
                amount: Msat(1),
                fee_cap: Msat(0),
            },
            BTreeMap::from([(fed(2), Msat(10)), (fed(3), Msat(0))]),
            None,
        ))
        .await
        .expect("fresh evacuation bypasses the external cap");
    client
        .decide_op(retry)
        .await
        .expect("manual evacuation retry bypasses the external cap");
    wait_for_registry(&client, EXTERNAL_DRIVER_CAP + 2).await;

    let error = client
        .decide_op(pay("pay:still-over-cap", fed(1), 1, 0, 250))
        .await
        .expect_err("evacuations do not consume external admission slots");
    assert!(error.to_string().contains("admission cap"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn full_cap_transition_pressure_keeps_decide_round_trip_prompt() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, _) = fixture(executor);
    let client = service.client();
    decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(9),
            source: fed(8),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(12),
            },
            now_ms: 20,
        },
    )
    .await
    .expect("internal probe driver does not consume external admission capacity");
    for index in 0..EXTERNAL_DRIVER_CAP {
        client
            .decide_op(pay(&format!("pay:cap-{index}"), fed(1), 1, 0, index as u8))
            .await
            .expect("fill admission cap");
    }
    for _ in 0..ACTOR_MAILBOX_CAPACITY {
        let client = client.clone();
        tokio::spawn(async move {
            let _ = client
                .journal_transition(
                    IdempotencyKey("pay:cap-0".to_owned()),
                    JournalTransition::Refresh,
                )
                .await;
        });
    }
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        client.decide_op(pay("pay:over-cap", fed(1), 1, 0, 250)),
    )
    .await
    .expect("DecideOp round-trip remains prompt under mailbox churn")
    .expect_err("external admission cap rejects the extra driver");
    assert!(result.to_string().contains("admission cap"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn policy_and_snapshot_commands_round_trip_and_validate() {
    let (service, _) = fixture(Arc::new(ExitExecutor(Exit::Ok)));
    let client = service.client();
    assert_eq!(client.get_policy().await.unwrap().per_fed_cap, Msat(1_000));
    let mut invalid = client.get_policy().await.unwrap();
    invalid.per_fed_cap = Msat(0);
    assert!(matches!(
        client.put_policy(invalid).await,
        Err(ServiceError::Policy(_))
    ));
    let mut updated = client.get_policy().await.unwrap();
    updated.per_fed_cap = Msat(2_000);
    assert_eq!(
        client.put_policy(updated).await.unwrap().per_fed_cap,
        Msat(2_000)
    );
    assert!(matches!(
        client.snapshot(SnapshotScope::Reservations).await.unwrap(),
        Snapshot::Reservations(_)
    ));
    assert_eq!(
        client.reconcile().await.unwrap(),
        ReconcileReport::default()
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn closing_every_sender_exits_actor_cleanly() {
    let (service, _) = fixture(Arc::new(ExitExecutor(Exit::Ok)));
    let WalletService {
        client,
        task,
        registry: _,
    } = service;
    drop(client);
    task.await.expect("actor exits on None from recv");
}

#[test]
fn journal_database_faults_keep_the_storage_refusal_taxonomy() {
    assert!(matches!(
        actor::refusal_from_exec(ExecError::Retryable(
            "journal db error: injected".to_owned()
        )),
        ServiceError::Refused {
            reason: RefuseReason::StorageError,
            ..
        }
    ));
}

#[tokio::test]
async fn targeted_intent_read_failure_is_a_decide_time_storage_refusal() {
    let db = MemDatabase::new().into_database();
    let key = IdempotencyKey("pay:corrupt-target".to_owned());
    let app_db = db.clone().with_prefix(vec![0x00]);
    let mut raw_key = vec![0x01];
    raw_key.extend_from_slice(key.0.as_bytes());
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&raw_key, b"not valid json")
        .await
        .expect("insert corrupt intent row");
    dbtx.commit_tx_result().await.expect("commit corrupt row");

    let journal = Arc::new(FedimintJournal::new(db));
    let service = WalletService::start_parts(
        None,
        journal,
        Arc::new(ExitExecutor(Exit::Ok)),
        Policy {
            per_fed_cap: Msat(1_000),
            spending_target: Msat(100),
            standby_target: Msat(100),
            ..Policy::default()
        },
        None,
    );
    let error = service
        .client()
        .decide_op(pay(&key.0, fed(1), 10, 1, 33))
        .await
        .expect_err("a corrupt targeted read must fail closed before admission");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::StorageError,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}
