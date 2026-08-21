use super::*;
use crate::journal::OperationRef;
use crate::multi_client::AwaitOperationError;
use crate::runtime::{TestAwaitOutcome, TestPostObservationFault, TestTerminalAwaitState};
use crate::{MultiClient, Runtime};
use crate::{RawOpObservation, RawTerminal};
use async_trait::async_trait;
use fedimint_bip39::Mnemonic;
use fedimint_core::db::mem_impl::MemDatabase;
use fedimint_core::db::IDatabaseTransactionOpsCore;
use fedimint_core::db::IRawDatabaseExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;
use wallet_core::{
    Action, Journal, Occurrence, OperationId, OperationKind, OperationStatus, PerformOutcome,
    ReasonCode, RefusalDiagnostics,
};

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

#[derive(Default)]
struct RetryableJoinExecutor {
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
        assert!(matches!(
            intent.action,
            Action::Join { .. } | Action::Recover { .. }
        ));
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        Ok(PerformOutcome::Done)
    }
}

#[async_trait]
impl Executor for RetryableJoinExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        if matches!(intent.action, Action::Join { .. } | Action::Recover { .. }) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ExecError::Retryable("membership network retry".to_owned()))
        } else {
            std::future::pending().await
        }
    }
}

#[derive(Default)]
struct FailThenSlowExecutor {
    calls: AtomicUsize,
    performed_attempts: Mutex<Vec<u32>>,
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
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.performed_attempts
            .lock()
            .expect("performed attempt lock")
            .push(intent.attempt);
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

fn healthy_probe(balance: u64) -> crate::probe::ProbeResult {
    crate::probe::ProbeResult {
        guardian_count: 4,
        threshold: 3,
        is_mainnet: true,
        module_kinds: vec!["mint".to_owned(), "wallet".to_owned(), "lnv2".to_owned()],
        has_lnv2: true,
        quorum_live: true,
        latency_ms: 10,
        gateway_available: true,
        wallet_module_present: true,
        expiry_timestamp_secs: None,
        config_expiry_secs: None,
        meta_module_expiry_secs: None,
        status_scheduled_shutdown: false,
        shutdown_scheduled: false,
        spendable_msat: balance,
        in_flight_msat: 0,
        claimable_msat: 0,
    }
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
        dest_unavailable: None,
    }
}

async fn fixture(executor: Arc<dyn Executor>) -> (WalletService, Arc<FedimintJournal>) {
    fixture_with_timeout(executor, None).await
}

async fn fixture_with_timeout(
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
    )
    .await
    .expect("start fixture service");
    (service, journal)
}

async fn awaiter_runtime_fixture(
    outcomes: impl IntoIterator<Item = TestAwaitOutcome>,
) -> (WalletService, Arc<FedimintJournal>) {
    let (service, journal, _hold) =
        awaiter_runtime_fixture_with_await_operation_errors(outcomes, [], [], [], false).await;
    (service, journal)
}

async fn awaiter_runtime_fixture_with_post_observation(
    outcomes: impl IntoIterator<Item = TestAwaitOutcome>,
    terminal_states: impl IntoIterator<Item = TestTerminalAwaitState>,
    post_observation_faults: impl IntoIterator<Item = TestPostObservationFault>,
) -> (WalletService, Arc<FedimintJournal>, Arc<Notify>) {
    let (service, journal, hold) = awaiter_runtime_fixture_with_await_operation_errors(
        outcomes,
        terminal_states,
        post_observation_faults,
        [],
        true,
    )
    .await;
    (
        service,
        journal,
        hold.expect("post-observation fixture holds retry handoff"),
    )
}

async fn awaiter_runtime_fixture_with_await_operation_errors(
    outcomes: impl IntoIterator<Item = TestAwaitOutcome>,
    terminal_states: impl IntoIterator<Item = TestTerminalAwaitState>,
    post_observation_faults: impl IntoIterator<Item = TestPostObservationFault>,
    await_operation_errors: impl IntoIterator<Item = AwaitOperationError>,
    hold_retry: bool,
) -> (WalletService, Arc<FedimintJournal>, Option<Arc<Notify>>) {
    let db = MemDatabase::new().into_database();
    let journal_db = MemDatabase::new().into_database();
    let mnemonic = Mnemonic::from_entropy(&[0xA7; 16]).expect("valid test mnemonic");
    let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
    let journal = Arc::new(FedimintJournal::new(journal_db));
    let runtime = Runtime::new(multi_client, journal.clone(), None, None, None);
    runtime.set_awaiter_test_outcomes(outcomes);
    runtime.set_post_observation_awaiter_test_fixture(terminal_states, post_observation_faults);
    runtime.set_awaiter_test_operation_errors(await_operation_errors);
    let hold = hold_retry.then(|| runtime.hold_next_awaiter_retry_for_test());
    let service = WalletService::start_without_scheduler(runtime)
        .await
        .expect("start runtime-backed service");
    (service, journal, hold)
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
        dest_unavailable: None,
    }
}

fn agent_request(
    key: &str,
    action: Action,
    reason: ReasonCode,
    occurrence: Occurrence,
    balances: BTreeMap<FederationId, Msat>,
) -> OpRequest {
    OpRequest {
        decision: AllocatorDecision {
            action,
            reason,
            occurrence,
            idempotency_key: IdempotencyKey(key.to_owned()),
        },
        actor: Actor::Agent { occurrence },
        now_ms: 2,
        balances,
        probe_session_nonce: None,
        dest_unavailable: None,
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

async fn fresh_probe_admission(client: &WalletClient) -> ProbeAdmission {
    ProbeAdmission::Fresh(
        client
            .probe_policy_snapshot()
            .await
            .expect("issue fresh-probe policy snapshot"),
    )
}

struct ExitExecutor(Exit);

#[derive(Default)]
struct CountingExitExecutor {
    calls: AtomicUsize,
}

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

#[async_trait]
impl Executor for CountingExitExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(PerformOutcome::Done)
    }
}

#[tokio::test]
async fn two_concurrent_pays_start_without_waiting_for_each_others_io() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, _) = fixture(executor.clone()).await;
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
    let (service, _) = fixture(executor.clone()).await;
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
    let (service, journal) = fixture(executor).await;
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
            admission: fresh_probe_admission(&client).await,
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
                gateway: None,
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
                gateway: None,
                fee_cap_components: None,
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
            gateway: None,
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
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
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
            admission: fresh_probe_admission(&client).await,
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
                gateway: None,
                fee_cap_components: None,
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
            admission: fresh_probe_admission(&client).await,
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
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
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
            admission: fresh_probe_admission(&client).await,
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
                gateway: None,
                fee_cap_components: None,
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
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
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
            admission: fresh_probe_admission(&service.client()).await,
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
            gateway: None,
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
            gateway: None,
            fee_cap_components: None,
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
    )
    .await
    .expect("start reconciliation service");
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
    let (service, _) = fixture(executor).await;
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
            admission: fresh_probe_admission(&client).await,
        })
        .await
        .expect_err("retroactive hold cannot start over an existing spend");
    assert!(error.to_string().contains("already spends"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fresh_probe_rejects_a_superseded_policy_snapshot_without_writing() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let candidate = fed(3);
    let source_a = fed(1);
    let source_b = fed(2);

    let mut policy_a = client.get_policy().await.expect("policy A");
    policy_a.spending_fed = Some(source_a);
    policy_a.probe_amount = Msat(111);
    policy_a.max_fee = Msat(7);
    client
        .put_policy(policy_a.clone())
        .await
        .expect("install policy A");
    let stale = client
        .probe_policy_snapshot()
        .await
        .expect("snapshot policy A");

    let mut policy_b = policy_a;
    policy_b.spending_fed = Some(source_b);
    policy_b.probe_amount = Msat(222);
    policy_b.max_fee = Msat(9);
    client.put_policy(policy_b).await.expect("install policy B");

    let error = client
        .decide_probe(ProbeCandidate {
            federation: candidate,
            source: source_a,
            baseline: Msat(17),
            actor: Actor::Agent {
                occurrence: Occurrence(80),
            },
            now_ms: 20,
            admission: ProbeAdmission::Fresh(stale),
        })
        .await
        .expect_err("policy-A authority cannot admit after policy B");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::PolicySuperseded,
            ..
        }
    ));
    assert!(
        journal
            .probe_record(&candidate)
            .await
            .expect("probe record")
            .is_none(),
        "a superseded policy must not create a durable probe session"
    );
    assert!(
        !journal
            .history(usize::MAX, None)
            .await
            .expect("probe history")
            .iter()
            .any(|row| matches!(row.kind, OperationKind::Probe { fed, .. } if fed == candidate)),
        "a superseded policy must not record a probe invocation"
    );
    assert_eq!(registry_size(&client).await, 0);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fresh_probe_rejects_a_foreign_actor_policy_snapshot() {
    let (issuing_service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let foreign = issuing_service
        .client()
        .probe_policy_snapshot()
        .await
        .expect("foreign policy snapshot");
    let (receiving_service, receiving_journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let receiving_client = receiving_service.client();
    let candidate = fed(3);

    let error = receiving_client
        .decide_probe(ProbeCandidate {
            federation: candidate,
            source: fed(1),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(81),
            },
            now_ms: 21,
            admission: ProbeAdmission::Fresh(foreign),
        })
        .await
        .expect_err("one actor cannot consume another actor's fresh authority");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::PolicySuperseded,
            ..
        }
    ));
    assert!(receiving_journal
        .probe_record(&candidate)
        .await
        .expect("probe record")
        .is_none());
    assert_eq!(registry_size(&receiving_client).await, 0);

    issuing_service.shutdown().await.expect("issuer shutdown");
    receiving_service
        .shutdown()
        .await
        .expect("receiver shutdown");
}

#[tokio::test]
async fn every_fresh_probe_candidate_revalidates_its_shared_policy_snapshot() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let snapshot = client
        .probe_policy_snapshot()
        .await
        .expect("shared policy snapshot");

    decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(1),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(82),
            },
            now_ms: 22,
            admission: ProbeAdmission::Fresh(snapshot.clone()),
        },
    )
    .await
    .expect("the first candidate is admitted under the current snapshot");

    let mut next_policy = client.get_policy().await.expect("current policy");
    next_policy.probe_amount = Msat(next_policy.probe_amount.0.saturating_add(1));
    client
        .put_policy(next_policy)
        .await
        .expect("supersede between candidates");

    let error = client
        .decide_probe(ProbeCandidate {
            federation: fed(2),
            source: fed(4),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(82),
            },
            now_ms: 22,
            admission: ProbeAdmission::Fresh(snapshot),
        })
        .await
        .expect_err("the second candidate must revalidate the shared snapshot");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::PolicySuperseded,
            ..
        }
    ));
    assert!(journal
        .probe_record(&fed(2))
        .await
        .expect("second probe record")
        .is_none());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn retained_probe_resumes_after_policy_change_only_for_its_durable_nonce() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let candidate = fed(1);
    let source = fed(2);
    let session = ProbeSession {
        nonce: "00000000000000830000000000000000".to_owned(),
        from: source,
        amount_msat: 111,
        leg_fee_cap_msat: 7,
        c_spendable_before_in_msat: 19,
        out_net_msat: None,
        started_at_ms: 23,
    };
    journal
        .begin_probe_session(&candidate, &session)
        .await
        .expect("seed retained policy-A session");
    let mut policy_b = client.get_policy().await.expect("policy");
    policy_b.spending_fed = Some(fed(4));
    policy_b.probe_amount = Msat(222);
    client.put_policy(policy_b).await.expect("install policy B");

    let resumed = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: candidate,
            source,
            baseline: Msat(session.c_spendable_before_in_msat),
            actor: Actor::Agent {
                occurrence: Occurrence(83),
            },
            now_ms: 24,
            admission: ProbeAdmission::ResumeOnly {
                expected_nonce: session.nonce.clone(),
            },
        },
    )
    .await
    .expect("the retained session resumes across the policy change");
    assert!(resumed.deduplicated);
    assert_eq!(resumed.session, session);
    assert_eq!(registry_size(&client).await, 1);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn resume_only_never_attaches_a_replacement_or_falls_through_to_fresh() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let candidate = fed(1);
    let old = ProbeSession {
        nonce: "00000000000000840000000000000000".to_owned(),
        from: fed(2),
        amount_msat: 111,
        leg_fee_cap_msat: 7,
        c_spendable_before_in_msat: 19,
        out_net_msat: None,
        started_at_ms: 25,
    };
    journal
        .begin_probe_session(&candidate, &old)
        .await
        .expect("seed observed session");
    journal
        .record_probe_outcome(
            &candidate,
            &old.nonce,
            None,
            &IdempotencyKey("probe:cleared-before-resume".to_owned()),
            OperationKind::Probe {
                fed: candidate,
                from: old.from,
                amount_msat: Msat(old.amount_msat),
                cost_msat: None,
            },
            Actor::Agent {
                occurrence: Occurrence(84),
            },
            OperationStatus::Failed,
            Some("clear observed session before actor admission"),
        )
        .await
        .expect("clear observed session");
    let replacement = ProbeSession {
        nonce: "00000000000000850000000000000000".to_owned(),
        from: fed(3),
        amount_msat: 222,
        leg_fee_cap_msat: 9,
        c_spendable_before_in_msat: 23,
        out_net_msat: None,
        started_at_ms: 26,
    };
    journal
        .begin_probe_session(&candidate, &replacement)
        .await
        .expect("seed replacement session");

    let stale_resume = ProbeCandidate {
        federation: candidate,
        source: old.from,
        baseline: Msat(old.c_spendable_before_in_msat),
        actor: Actor::Agent {
            occurrence: Occurrence(84),
        },
        now_ms: 27,
        admission: ProbeAdmission::ResumeOnly {
            expected_nonce: old.nonce.clone(),
        },
    };
    let replaced_error = client
        .decide_probe(stale_resume.clone())
        .await
        .expect_err("an old nonce cannot attach the replacement");
    assert!(matches!(
        replaced_error,
        ServiceError::Refused {
            reason: RefuseReason::Conflict,
            ..
        }
    ));
    assert_eq!(
        journal
            .probe_record(&candidate)
            .await
            .expect("replacement record")
            .and_then(|record| record.in_flight),
        Some(replacement.clone())
    );
    assert_eq!(registry_size(&client).await, 0);

    journal
        .record_probe_outcome(
            &candidate,
            &replacement.nonce,
            None,
            &IdempotencyKey("probe:replacement-cleared".to_owned()),
            OperationKind::Probe {
                fed: candidate,
                from: replacement.from,
                amount_msat: Msat(replacement.amount_msat),
                cost_msat: None,
            },
            Actor::Agent {
                occurrence: Occurrence(84),
            },
            OperationStatus::Failed,
            Some("clear replacement before retry"),
        )
        .await
        .expect("clear replacement");
    let missing_error = client
        .decide_probe(stale_resume)
        .await
        .expect_err("resume-only cannot create a fresh session after the durable row clears");
    assert!(matches!(
        missing_error,
        ServiceError::Refused {
            reason: RefuseReason::Conflict,
            ..
        }
    ));
    assert_eq!(
        journal
            .probe_record(&candidate)
            .await
            .expect("cleared record")
            .and_then(|record| record.in_flight),
        None
    );
    assert_eq!(registry_size(&client).await, 0);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn probe_driver_never_switches_actor_approved_session_after_admission() {
    let db = MemDatabase::new().into_database();
    let journal_db = MemDatabase::new().into_database();
    let mnemonic = Mnemonic::from_entropy(&[0x86; 16]).expect("valid test mnemonic");
    let multi_client = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
    let journal = Arc::new(FedimintJournal::new(journal_db));
    let runtime = Arc::new(Runtime::new(
        multi_client,
        journal.clone(),
        None,
        None,
        None,
    ));
    let policy = Policy {
        per_fed_cap: Msat(1_000),
        spending_target: Msat(100),
        standby_target: Msat(100),
        ..Policy::default()
    };
    let service = WalletService::start_parts_inner(
        Some(Arc::clone(&runtime)),
        None,
        journal.clone(),
        Arc::new(runtime.service_executor(Some(policy.per_fed_cap))),
        policy,
        None,
    )
    .await
    .expect("start runtime-backed probe fixture");
    let client = service.client();
    let hold = runtime.hold_next_service_probe_start_for_test();
    let candidate = fed(1);
    let source = fed(2);

    let admitted = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: candidate,
            source,
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(86),
            },
            now_ms: 30,
            admission: fresh_probe_admission(&client).await,
        },
    )
    .await
    .expect("actor admits and journals the original session");
    assert_eq!(registry_size(&client).await, 1);

    journal
        .record_probe_outcome(
            &candidate,
            &admitted.session.nonce,
            None,
            &admitted.key,
            OperationKind::Probe {
                fed: candidate,
                from: admitted.session.from,
                amount_msat: Msat(admitted.session.amount_msat),
                cost_msat: None,
            },
            Actor::Agent {
                occurrence: Occurrence(86),
            },
            OperationStatus::Failed,
            Some("replace actor-approved session before driver read"),
        )
        .await
        .expect("clear actor-approved session");
    let replacement = ProbeSession {
        nonce: "00000000000000870000000000000000".to_owned(),
        from: fed(3),
        amount_msat: 333,
        leg_fee_cap_msat: 11,
        c_spendable_before_in_msat: 29,
        out_net_msat: None,
        started_at_ms: 31,
    };
    journal
        .begin_probe_session(&candidate, &replacement)
        .await
        .expect("install replacement session");

    hold.notify_one();
    wait_for_registry(&client, 0).await;
    assert_eq!(
        journal
            .probe_record(&candidate)
            .await
            .expect("replacement record")
            .and_then(|record| record.in_flight),
        Some(replacement),
        "the old-key driver must not attach, drive, or clear the replacement nonce"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn concurrent_probe_budget_check_and_marker_prevent_double_admission() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
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
            admission: fresh_probe_admission(&client).await,
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
            admission: fresh_probe_admission(&client).await,
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
async fn probe_refresh_retries_an_operation_read_fault_before_releasing_its_budget_reservation() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("policy");
    policy.max_probe_attempts_per_week = 1;
    policy.max_probe_spend_per_week = Msat(1_000_000);
    client.put_policy(policy).await.expect("tight probe budget");
    let first = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(1),
            source: fed(3),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(90),
            },
            now_ms: 12,
            admission: fresh_probe_admission(&client).await,
        },
    )
    .await
    .expect("first probe reserves an active budget entry");
    journal
        .record_probe_outcome(
            &first.candidate,
            &first.session.nonce,
            None,
            &first.key,
            wallet_core::OperationKind::Probe {
                fed: first.candidate,
                from: first.session.from,
                amount_msat: Msat(first.session.amount_msat),
                cost_msat: None,
            },
            Actor::Agent {
                occurrence: Occurrence(90),
            },
            OperationStatus::Failed,
            Some("injected terminal probe outcome"),
        )
        .await
        .expect("terminal probe row");

    // `Refresh` must surface this actor-side operation read error so the sole
    // registered probe owner retries it outside the actor.  A successful retry
    // removes the active reservation because this terminal row has no cost.
    journal.fail_next_operation_reads_for_test(1);
    assert!(
        driver::refresh_probe_budget_until_success(&client, first.key.clone()).await,
        "retry stops only after the durable terminal row refreshes the budget"
    );
    let second = decide_probe_ready(
        &client,
        ProbeCandidate {
            federation: fed(2),
            source: fed(3),
            baseline: Msat(0),
            actor: Actor::Agent {
                occurrence: Occurrence(91),
            },
            now_ms: 13,
            admission: fresh_probe_admission(&client).await,
        },
    )
    .await;
    assert!(
        second.is_ok(),
        "the terminal refresh cleared the first active budget reservation"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn concurrent_probe_budget_reserves_possible_principal_loss() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
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
            admission: fresh_probe_admission(&client).await,
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
            admission: fresh_probe_admission(&client).await,
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
    )
    .await
    .expect("start corrupt-ledger service");
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
                admission: fresh_probe_admission(&client).await,
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
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
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
            admission: fresh_probe_admission(&client).await,
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
            admission: fresh_probe_admission(&client).await,
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
            admission: fresh_probe_admission(&client).await,
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
    )
    .await
    .expect("start first probe-budget service");
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
            admission: fresh_probe_admission(&first_service.client()).await,
        },
    )
    .await
    .expect("first probe admitted");
    let mut lowered_policy = initial_policy.clone();
    lowered_policy.max_fee = Msat(50_000);
    first_service
        .client()
        .put_policy(lowered_policy.clone())
        .await
        .expect("persist the edited policy before restart");
    first_service.shutdown().await.expect("first shutdown");

    let restarted = WalletService::start_parts(
        None,
        journal,
        Arc::new(SlowExecutor::default()),
        lowered_policy,
        None,
    )
    .await
    .expect("restart probe-budget service");
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
            admission: fresh_probe_admission(&client).await,
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
            admission: fresh_probe_admission(&client).await,
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
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
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
            admission: fresh_probe_admission(&client).await,
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
            admission: fresh_probe_admission(&client).await,
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
    )
    .await
    .expect("start old-probe service");
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
            admission: fresh_probe_admission(&first_service.client()).await,
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
    )
    .await
    .expect("restart old-probe service");
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
            admission: fresh_probe_admission(&restarted.client()).await,
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
        fixture_with_timeout(executor.clone(), Some(std::time::Duration::from_secs(10))).await;
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
        fixture_with_timeout(executor.clone(), Some(std::time::Duration::from_secs(10))).await;
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
    )
    .await
    .expect("start post-panic service");
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
    let (service, _) = fixture(Arc::new(ExitExecutor(exit))).await;
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
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
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
                expected_attempt: 0,
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
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
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
async fn shutdown_drains_parked_waiters_with_errors() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
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
    let (service, journal) = fixture(Arc::new(AwaitingExecutor)).await;
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
        scheduler_abort: _,
        scheduler_task: _,
        scheduler_alive: _,
        critical_exit: _,
        policy_wake: _,
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
            transition: JournalTransition::DriverFinished {
                generation: 2,
                expected_attempt: 0,
                retry_awaiter: false,
            },
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
    let (service, journal) = fixture(executor.clone()).await;
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
async fn failed_pay_with_committed_op_refuses_manual_retry() {
    // lnv2 allows one payment attempt per invoice: a Failed pay whose prior attempt
    // COMMITTED its send op (operation_id set) can never succeed on retry — the SDK
    // dedups any re-`pay` back to the dead op. The actor must refuse loudly with an
    // actionable message instead of refreshing an unwinnable intent.
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor).await;
    let client = service.client();
    let old = pay("pay:spent", fed(1), 10, 1, 21);
    let mut failed = Intent::from_decision(&old.decision, Actor::User, 1);
    failed.status = IntentStatus::Failed;
    failed.operation_id = Some(OperationId([7; 32]));
    journal.upsert(&failed).await.expect("seed failed with op");
    let err = client
        .decide_op(pay("pay:spent", fed(1), 20, 2, 21))
        .await
        .expect_err("committed-op retry must refuse");
    assert!(matches!(
        err,
        ServiceError::Refused {
            reason: RefuseReason::Conflict,
            ..
        }
    ));
    assert!(
        err.to_string().contains("single payment attempt"),
        "refusal must tell the user the invoice is spent: {err}"
    );
    // The failed intent is untouched: same attempt counter, still Failed.
    let untouched = journal
        .get(&IdempotencyKey("pay:spent".to_owned()))
        .await
        .expect("read back")
        .unwrap();
    assert_eq!(untouched.status, IntentStatus::Failed);
    assert_eq!(untouched.attempt, 0);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn failed_manual_retry_refreshes_sizing_but_live_mismatch_conflicts() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor).await;
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
        .put_move_if_attempt(
            &failed.idempotency_key,
            failed.attempt,
            &wallet_core::MoveRecord {
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
            },
        )
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
    let (service, _) = fixture(executor.clone()).await;
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
async fn manual_retry_hands_off_after_old_driver_finished_without_reconcile() {
    let executor = Arc::new(FailThenSlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let req = pay("pay:manual-retry-handoff", fed(1), 10, 1, 18);
    client
        .decide_op(req.clone())
        .await
        .expect("attempt N admitted");
    while executor.calls.load(Ordering::SeqCst) == 0 {
        executor.first_started.notified().await;
    }

    // Make N manually retryable while its wrapper is still blocked in perform.  `decide_op`
    // must admit N+1 but cannot spawn it yet: N is still the sole registry owner.
    client
        .journal_transition(
            req.decision.idempotency_key.clone(),
            JournalTransition::SetStatus {
                expected_attempt: 0,
                status: IntentStatus::Failed,
                error: Some("operator retry".to_owned()),
            },
        )
        .await
        .expect("terminalize attempt N");
    client
        .decide_op(req.clone())
        .await
        .expect("manual retry admits attempt N+1");
    let replacement = journal
        .get(&req.decision.idempotency_key)
        .await
        .expect("replacement read")
        .expect("replacement exists");
    assert_eq!(replacement.attempt, 1);
    assert_eq!(replacement.status, IntentStatus::Pending);
    assert_eq!(
        registry_size(&client).await,
        1,
        "the old driver must still own attempt N's registry slot at N+1 admission"
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);

    executor.release_first.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while executor.calls.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("DriverFinished must hand the registry slot to attempt N+1");
    assert_eq!(
        *executor
            .performed_attempts
            .lock()
            .expect("performed attempt lock"),
        vec![0, 1],
        "the replacement driver must perform attempt N+1, not stale attempt N"
    );
    assert_eq!(
        registry_size(&client).await,
        1,
        "DriverFinished handed the slot straight to N+1; no reconcile was needed"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn pending_attach_can_redrive_before_the_old_driver_wrapper_exits() {
    let executor = Arc::new(RetryThenSlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
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
    let (service, journal) = fixture(executor.clone()).await;
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
            admission: fresh_probe_admission(&client).await,
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
            gateway: None,
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
                gateway: None,
                fee_cap_components: None,
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
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
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
async fn awaiting_direct_inflow_missing_derived_cache_backfills_before_permanent_classification() {
    let (service, journal, retry_hold) =
        awaiter_runtime_fixture_with_await_operation_errors([], [], [], [], true).await;
    let retry_hold = retry_hold.expect("fixture holds retry handoff");
    let client = service.client();
    let request = move_request(
        "direct:missing-derived-cache",
        Action::DirectInflow {
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(1),
        },
        BTreeMap::new(),
        None,
    );
    let mut intent = Intent::from_decision(&request.decision, Actor::User, 1);
    intent.status = IntentStatus::Awaiting;
    let key = intent.idempotency_key.clone();
    journal
        .upsert(&intent)
        .await
        .expect("seed Awaiting direct inflow with no derived MoveRecord");

    assert_eq!(
        client
            .reconcile()
            .await
            .expect("attach direct-inflow awaiter")
            .awaiters_rehydrated,
        1
    );
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    let awaiting = journal
        .get(&key)
        .await
        .expect("read direct inflow after backfill attempt")
        .expect("durable direct inflow");
    assert_eq!(
        awaiting.status,
        IntentStatus::Awaiting,
        "a cache miss must reach op-log backfill and retain ownership on its retryable result"
    );
    assert_eq!(
        registry_size(&client).await,
        1,
        "the retryable op-log backfill result retains await ownership"
    );
    retry_hold.notify_one();
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn transient_awaiter_error_reacquires_subscription_then_terminalizes_without_reconcile() {
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    let req = move_request(
        "direct:awaiter-transient-error",
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
    let key = intent.idempotency_key.clone();
    journal.upsert(&intent).await.expect("seed awaiting intent");

    // Model an awaiter whose first subscription call failed after its out-of-actor backoff. The
    // driver result reaches the actor with retry_awaiter=true; no reconcile command participates.
    driver::spawn_registered(
        &service.registry,
        key.clone(),
        41,
        driver::DriverKind::Awaiter {
            external_admission: false,
        },
        std::future::pending(),
    );
    assert_eq!(
        driver::len(&service.registry),
        1,
        "first awaiter owns intent"
    );
    client
        .journal_transition(
            key.clone(),
            JournalTransition::DriverFinished {
                generation: 41,
                expected_attempt: 0,
                retry_awaiter: true,
            },
        )
        .await
        .expect("actor accepts the transient awaiter completion");
    assert_eq!(
        driver::len(&service.registry),
        1,
        "the actor replaces the failed awaiter with a new subscription owner"
    );

    // The replacement observes completion and terminalizes normally. Its false retry flag means
    // `DriverFinished` removes ownership instead of respawning after a terminal/stale result.
    client
        .journal_transition(
            key.clone(),
            JournalTransition::SetStatus {
                expected_attempt: 0,
                status: IntentStatus::Done,
                error: None,
            },
        )
        .await
        .expect("replacement awaiter terminalizes intent");
    client
        .journal_transition(
            key.clone(),
            JournalTransition::DriverFinished {
                generation: 1,
                expected_attempt: 0,
                retry_awaiter: false,
            },
        )
        .await
        .expect("normal awaiter completion is accepted");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read terminal intent")
            .expect("intent")
            .status,
        IntentStatus::Done
    );
    assert_eq!(
        driver::len(&service.registry),
        0,
        "terminal awaiter completion does not hot-loop into a successor"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn permanent_awaiter_failure_terminalizes_once_and_releases_ownership() {
    let (service, journal) = awaiter_runtime_fixture([TestAwaitOutcome::Permanent]).await;
    let client = service.client();
    let req = move_request(
        "direct:awaiter-permanent-error",
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
    let key = intent.idempotency_key.clone();
    journal.upsert(&intent).await.expect("seed awaiting intent");

    assert_eq!(
        client
            .reconcile()
            .await
            .expect("attach awaiter")
            .awaiters_rehydrated,
        1
    );
    for _ in 0..100 {
        let terminal = journal
            .get(&key)
            .await
            .expect("read intent")
            .is_some_and(|intent| intent.status == IntentStatus::Failed);
        if terminal && registry_size(&client).await == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let failed = journal
        .get(&key)
        .await
        .expect("read terminal intent")
        .expect("intent remains durable");
    assert_eq!(failed.status, IntentStatus::Failed);
    let row = journal
        .operation(&OperationRef::Key(key.clone()))
        .await
        .expect("read operation row")
        .expect("ledger row");
    assert!(
        row.error
            .as_deref()
            .is_some_and(|error| error.contains("injected permanent await failure")),
        "permanent await diagnostic must reach the terminal ledger row: {row:#?}"
    );
    assert_eq!(
        registry_size(&client).await,
        0,
        "a terminal permanent failure must not spawn a second awaiter"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn retryable_awaiter_failure_retries_once_then_terminalizes_without_reconcile() {
    let (service, journal) =
        awaiter_runtime_fixture([TestAwaitOutcome::Retryable, TestAwaitOutcome::Done]).await;
    let client = service.client();
    let req = move_request(
        "direct:awaiter-retryable-error",
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
    let key = intent.idempotency_key.clone();
    journal.upsert(&intent).await.expect("seed awaiting intent");

    assert_eq!(
        client
            .reconcile()
            .await
            .expect("attach awaiter")
            .awaiters_rehydrated,
        1
    );
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    for _ in 0..100 {
        let done = journal
            .get(&key)
            .await
            .expect("read intent")
            .is_some_and(|intent| intent.status == IntentStatus::Done);
        if done && registry_size(&client).await == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read terminal intent")
            .expect("intent")
            .status,
        IntentStatus::Done,
        "the second awaiter terminalized the original durable attempt"
    );
    assert_eq!(
        registry_size(&client).await,
        0,
        "the true terminal result does not require reconcile or leave retry ownership"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn permanent_awaiter_terminalization_write_fault_retries_ownership_then_completes_same_attempt(
) {
    let (service, journal) =
        awaiter_runtime_fixture([TestAwaitOutcome::Permanent, TestAwaitOutcome::Done]).await;
    let client = service.client();
    let req = move_request(
        "direct:awaiter-permanent-terminalization-write-fault",
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
    let key = intent.idempotency_key.clone();
    journal.upsert(&intent).await.expect("seed awaiting intent");

    // `reconcile` scans its pending index without calling `get`. Its spawned awaiter consumes this
    // fault in SetStatus(Failed)'s actor pre-read, so the first permanent terminalization fails
    // before it can mutate the durable attempt.
    journal.fail_next_intent_reads_for_test(1);
    assert_eq!(
        client
            .reconcile()
            .await
            .expect("attach awaiter")
            .awaiters_rehydrated,
        1
    );
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    let awaiting = journal
        .get(&key)
        .await
        .expect("read failed-terminalization attempt")
        .expect("durable attempt remains");
    assert_eq!(
        awaiting.status,
        IntentStatus::Awaiting,
        "the failed SetStatus(Failed) must retain the same Awaiting attempt"
    );
    assert_eq!(awaiting.attempt, 0);
    assert_eq!(
        registry_size(&client).await,
        1,
        "the permanent terminalization error retains awaiter ownership for its retry"
    );

    // No external reconcile is sent: only the driver's bounded retry handoff may launch the
    // successor, which consumes the queued Done result for the original durable attempt.
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    for _ in 0..100 {
        let done = journal
            .get(&key)
            .await
            .expect("read successor result")
            .is_some_and(|intent| intent.status == IntentStatus::Done);
        if done && registry_size(&client).await == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let done = journal
        .get(&key)
        .await
        .expect("read completed attempt")
        .expect("durable attempt remains");
    assert_eq!(done.status, IntentStatus::Done);
    assert_eq!(
        done.attempt, 0,
        "the retry reuses its original expected-attempt fence"
    );
    assert_eq!(
        registry_size(&client).await,
        0,
        "the successor releases ownership after its terminal Done result"
    );
    service.shutdown().await.expect("shutdown");
}

async fn post_observation_fault_retains_raw_awaiter_ownership(
    key_text: &str,
    action: Action,
    terminal_state: TestTerminalAwaitState,
    fault: TestPostObservationFault,
) {
    // `Done` is consumed only by the successor. The first awaiter instead gets a terminal SDK
    // state and a narrow post-observation fault, so this is not a whole-awaiter outcome test.
    let (service, journal, retry_hold) = awaiter_runtime_fixture_with_post_observation(
        [TestAwaitOutcome::Done],
        [terminal_state],
        [fault],
    )
    .await;
    let client = service.client();
    let request = move_request(key_text, action, BTreeMap::new(), None);
    let mut intent = Intent::from_decision(&request.decision, Actor::User, 1);
    intent.status = IntentStatus::Awaiting;
    intent.operation_id = Some(OperationId([0xCD; 32]));
    let key = intent.idempotency_key.clone();
    if matches!(fault, TestPostObservationFault::FinalizeStatusMismatch) {
        let _ = wallet_core::decide_and_journal(
            journal.as_ref(),
            &request.decision,
            Actor::User,
            request.now_ms,
            Some(&request.balances),
            None,
        )
        .await
        .expect("seed raw ledger row for post-observation finalization");
    }
    journal
        .upsert(&intent)
        .await
        .expect("seed raw awaiting intent");

    assert_eq!(
        client
            .reconcile()
            .await
            .expect("attach raw awaiter")
            .awaiters_rehydrated,
        1
    );
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    let awaiting = journal
        .get(&key)
        .await
        .expect("read held post-observation result")
        .expect("raw intent remains durable");
    assert_eq!(
        awaiting.status,
        IntentStatus::Awaiting,
        "a post-observation local fault must not actor-mark the same attempt Failed"
    );
    assert_eq!(awaiting.attempt, 0);
    assert_eq!(
        registry_size(&client).await,
        1,
        "the failed awaiter retains ownership until its successor is released"
    );
    client
        .issue_balance_facts_token()
        .await
        .expect("the post-observation finalizer released its terminal-mutation lease");

    retry_hold.notify_one();
    for _ in 0..100 {
        let done = journal
            .get(&key)
            .await
            .expect("read successor result")
            .is_some_and(|intent| intent.status == IntentStatus::Done);
        if done && registry_size(&client).await == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let final_intent = journal
        .get(&key)
        .await
        .expect("read final raw intent")
        .expect("raw intent");
    assert_eq!(
        final_intent.status,
        IntentStatus::Done,
        "the successor owns and completes the same durable attempt"
    );
    assert_eq!(
        final_intent.attempt, 0,
        "the local retry never creates a new attempt"
    );
    let history = journal.history(10, None).await.expect("read raw history");
    assert!(
        history
            .iter()
            .filter(|row| row.correlation_key == key)
            .all(|row| row.status != OperationStatus::Failed),
        "post-observation local faults must never write a Failed operation row: {history:#?}"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn raw_pay_post_observation_permanent_retries_without_failing_the_attempt() {
    post_observation_fault_retains_raw_awaiter_ownership(
        "pay:post-observation-permanent",
        Action::Pay {
            from: fed(1),
            invoice: Invoice("invoice-post-observation-pay".to_owned()),
            amount: Msat(10),
            fee_cap: Msat(1),
            payment_hash: [0xA1; 32],
            gateway: None,
        },
        TestTerminalAwaitState::SendSucceeded,
        TestPostObservationFault::PreparePermanent,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn raw_receive_post_observation_mismatch_retries_without_failing_the_attempt() {
    post_observation_fault_retains_raw_awaiter_ownership(
        "receive:post-observation-mismatch",
        Action::Receive {
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(1),
            nonce: "post-observation-receive".to_owned(),
            gateway: None,
        },
        TestTerminalAwaitState::ReceiveClaimed,
        TestPostObservationFault::FinalizeStatusMismatch,
    )
    .await;
}

#[tokio::test]
async fn raw_pay_wrong_leg_before_observation_fails_the_exact_attempt() {
    let (service, journal, _hold) = awaiter_runtime_fixture_with_await_operation_errors(
        [],
        [],
        [],
        [AwaitOperationError::WrongOperationKind {
            operation: OperationId([0xDC; 32]),
            actual: "receive",
            expected: "send",
        }],
        false,
    )
    .await;
    let client = service.client();
    let request = move_request(
        "pay:wrong-await-leg",
        Action::Pay {
            from: fed(1),
            invoice: Invoice("invoice-wrong-await-leg".to_owned()),
            amount: Msat(10),
            fee_cap: Msat(1),
            payment_hash: [0xD1; 32],
            gateway: None,
        },
        BTreeMap::new(),
        None,
    );
    let mut intent = Intent::from_decision(&request.decision, Actor::User, 1);
    intent.status = IntentStatus::Awaiting;
    intent.operation_id = Some(OperationId([0xDC; 32]));
    let key = intent.idempotency_key.clone();
    journal.upsert(&intent).await.expect("seed raw pay intent");

    client.reconcile().await.expect("attach raw pay awaiter");
    for _ in 0..100 {
        let failed = journal
            .get(&key)
            .await
            .expect("read wrong-leg result")
            .is_some_and(|intent| intent.status == IntentStatus::Failed);
        if failed && registry_size(&client).await == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let failed = journal
        .get(&key)
        .await
        .expect("read failed raw pay")
        .expect("raw pay intent");
    assert_eq!(failed.status, IntentStatus::Failed);
    assert_eq!(
        failed.attempt, 0,
        "the typed pre-observation mismatch fails only the observed durable attempt"
    );
    let row = journal
        .operation(&OperationRef::Key(key))
        .await
        .expect("read wrong-leg operation row")
        .expect("operation row");
    assert!(
        row.error.is_some(),
        "the actor records a permanent typed-await diagnostic for the failed attempt: {row:#?}"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn finished_driver_refresh_fault_recovers_awaiter_ownership_without_external_reconcile() {
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    let req = move_request(
        "direct:finished-refresh-fault",
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
    let key = intent.idempotency_key.clone();
    journal.upsert(&intent).await.expect("seed awaiting intent");

    driver::spawn_registered(
        &service.registry,
        key.clone(),
        41,
        driver::DriverKind::Awaiter {
            external_admission: false,
        },
        std::future::pending(),
    );
    journal.fail_next_intent_reads_for_test(1);
    client
        .journal_transition(
            key.clone(),
            JournalTransition::DriverFinished {
                generation: 41,
                expected_attempt: 0,
                retry_awaiter: false,
            },
        )
        .await
        .expect("finished generation is removed despite its refresh fault");

    // The detached bounded-backoff recovery invokes the actor's durable reconcile itself; this
    // test deliberately sends no external reconcile command. Its new generation proves it did not
    // merely retain the old registry entry after the injected post-removal read failure.
    let recovered_generation = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let generation = service
                .registry
                .lock()
                .expect("registry lock")
                .get(&key)
                .map(|entry| entry.generation);
            if let Some(generation) = generation {
                if generation != 41 {
                    return generation;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reconcile recovery rehydrates a successor awaiter");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read rehydrated intent")
            .expect("intent")
            .status,
        IntentStatus::Awaiting,
        "the recovered owner retains the same durable attempt"
    );

    // A completed subscription would make this terminal transition; route it manually in this
    // runtime-free fixture, then ensure the recovered generation releases ownership normally.
    client
        .journal_transition(
            key.clone(),
            JournalTransition::SetStatus {
                expected_attempt: 0,
                status: IntentStatus::Done,
                error: None,
            },
        )
        .await
        .expect("same awaiter attempt terminalizes");
    client
        .journal_transition(
            key.clone(),
            JournalTransition::DriverFinished {
                generation: recovered_generation,
                expected_attempt: 0,
                retry_awaiter: false,
            },
        )
        .await
        .expect("recovered awaiter completion");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read terminal intent")
            .expect("intent")
            .status,
        IntentStatus::Done
    );
    assert_eq!(registry_size(&client).await, 0);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn simultaneous_finished_read_faults_share_one_ownership_recovery_scan() {
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    let mut keys = Vec::new();
    for (suffix, destination, generation) in [("a", fed(2), 51), ("b", fed(3), 52)] {
        let req = move_request(
            &format!("direct:finished-refresh-coalesce:{suffix}"),
            Action::DirectInflow {
                to: destination,
                amount: Msat(10),
                fee_cap: Msat(1),
            },
            BTreeMap::new(),
            None,
        );
        let mut intent = Intent::from_decision(&req.decision, Actor::User, 1);
        intent.status = IntentStatus::Awaiting;
        let key = intent.idempotency_key.clone();
        journal.upsert(&intent).await.expect("seed awaiting intent");
        driver::spawn_registered(
            &service.registry,
            key.clone(),
            generation,
            driver::DriverKind::Awaiter {
                external_admission: false,
            },
            std::future::pending(),
        );
        keys.push((key, generation));
    }
    journal.reset_pending_reads_for_test();
    journal.fail_next_intent_reads_for_test(2);
    let left = client.journal_transition(
        keys[0].0.clone(),
        JournalTransition::DriverFinished {
            generation: keys[0].1,
            expected_attempt: 0,
            retry_awaiter: false,
        },
    );
    let right = client.journal_transition(
        keys[1].0.clone(),
        JournalTransition::DriverFinished {
            generation: keys[1].1,
            expected_attempt: 0,
            retry_awaiter: false,
        },
    );
    let (left, right) = tokio::join!(left, right);
    left.expect("first failed refresh removes its finished owner");
    right.expect("second failed refresh removes its finished owner");

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let reowned = {
                let registry = service.registry.lock().expect("registry lock");
                keys.iter().all(|(key, generation)| {
                    registry
                        .get(key)
                        .is_some_and(|entry| entry.generation != *generation)
                })
            };
            if reowned {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one recovery worker rehydrates every live awaiter");
    assert_eq!(
        journal.pending_reads_for_test(),
        1,
        "simultaneous read faults coalesce into one durable recovery scan"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn persistent_finished_read_faults_pace_stale_ownership_recovery_scans() {
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    let mut keys = Vec::new();
    for (suffix, destination, generation) in [("a", fed(2), 61), ("b", fed(3), 62)] {
        let req = move_request(
            &format!("direct:finished-refresh-persistent:{suffix}"),
            Action::DirectInflow {
                to: destination,
                amount: Msat(10),
                fee_cap: Msat(1),
            },
            BTreeMap::new(),
            None,
        );
        let mut intent = Intent::from_decision(&req.decision, Actor::User, 1);
        intent.status = IntentStatus::Awaiting;
        let key = intent.idempotency_key.clone();
        journal.upsert(&intent).await.expect("seed awaiting intent");
        driver::spawn_registered(
            &service.registry,
            key.clone(),
            generation,
            driver::DriverKind::Awaiter {
                external_admission: false,
            },
            std::future::pending(),
        );
        journal.persistently_fail_intent_read_for_test(key.clone());
        keys.push((key, generation));
    }
    journal.reset_pending_reads_for_test();
    let pause = journal.pause_next_pending_read_for_test();

    // The first fault starts the sole detached worker.  Pause its healthy durable scan, then
    // deliver the second *persistent per-key* post-DriverFinished get fault.  The second actor
    // turn advances the generation while the same worker owns the scan; it must therefore get
    // `Ok(false)` from its acknowledgement rather than spawn another worker.
    let first_client = client.clone();
    let first_key = keys[0].0.clone();
    let first = tokio::spawn(async move {
        first_client
            .journal_transition(
                first_key,
                JournalTransition::DriverFinished {
                    generation: 61,
                    expected_attempt: 0,
                    retry_awaiter: false,
                },
            )
            .await
    });
    pause.wait_until_started().await;
    let second_client = client.clone();
    let second_key = keys[1].0.clone();
    let second = tokio::spawn(async move {
        second_client
            .journal_transition(
                second_key,
                JournalTransition::DriverFinished {
                    generation: 62,
                    expected_attempt: 0,
                    retry_awaiter: false,
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    pause.release();
    first
        .await
        .expect("first driver-finish task does not panic")
        .expect("first failed refresh still releases ownership");
    second
        .await
        .expect("second driver-finish task does not panic")
        .expect("second failed refresh still releases ownership");

    // Before 25 ms elapses the generation-mismatch path must still be sleeping.  The old
    // immediate `Ok(false) => continue` loop reaches a second pending scan here, turning a
    // persistent get fault into a scan storm.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        journal.pending_reads_for_test(),
        1,
        "both post-finish faults share one initial durable scan"
    );
    tokio::time::advance(std::time::Duration::from_millis(24)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        journal.pending_reads_for_test(),
        1,
        "the stale scan acknowledgement is paced by the initial 25 ms backoff"
    );

    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    for _ in 0..100 {
        if journal.pending_reads_for_test() == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        journal.pending_reads_for_test(),
        2,
        "one coalesced worker performs exactly one paced rescan after the generation mismatch"
    );
    for (key, _) in &keys {
        journal.clear_persistent_intent_read_fault_for_test(key);
    }
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn newly_awaiting_intent_hands_off_to_an_awaiter_before_releasing_ownership() {
    let (service, journal) = fixture(Arc::new(AwaitingExecutor)).await;
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
    let (service, _) = fixture(Arc::new(AwaitingExecutor)).await;
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
    let (service, journal) = fixture(executor).await;
    let client = service.client();
    let retry = move_request(
        "evacuate:retry-at-cap",
        Action::Evacuate {
            from: fed(2),
            to: fed(3),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        BTreeMap::from([(fed(2), Msat(10)), (fed(3), Msat(0))]),
        None,
    );
    let failed = Intent::from_decision(&retry.decision, Actor::User, 1);
    journal.upsert(&failed).await.expect("seed retry intent");
    journal
        .set_status(
            &retry.decision.idempotency_key,
            failed.attempt,
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
                gateway: None,
                fee_cap_components: None,
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
    let (service, _) = fixture(executor).await;
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
            admission: fresh_probe_admission(&client).await,
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
async fn commit_tick_bypasses_and_does_not_consume_the_external_driver_cap() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, _) = fixture(executor).await;
    let client = service.client();
    for index in 0..EXTERNAL_DRIVER_CAP {
        client
            .decide_op(pay(
                &format!("pay:tick-cap-{index}"),
                fed(1),
                1,
                0,
                index as u8,
            ))
            .await
            .expect("fill external driver cap");
    }
    wait_for_registry(&client, EXTERNAL_DRIVER_CAP).await;

    let occurrence = Occurrence(36);
    client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), healthy_probe(100)), (fed(2), healthy_probe(0))],
            occurrence,
            now_ms: 104,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("seed tick facts");
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::SpendingBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:agent-at-external-cap".to_owned()),
    };
    let report = client
        .commit_tick_legacy(
            vec![decision],
            0,
            client.issue_tick_plan_token().await.expect("token"),
        )
        .await
        .expect("agent tick bypasses the external cap");
    assert_eq!(report.accepted.len(), 1);
    wait_for_registry(&client, EXTERNAL_DRIVER_CAP + 1).await;
    assert_eq!(driver::external_len(&service.registry), EXTERNAL_DRIVER_CAP);

    let error = client
        .decide_op(pay("pay:still-capped-after-tick", fed(1), 1, 0, 250))
        .await
        .expect_err("agent tick does not free or consume an external slot");
    assert!(error.to_string().contains("admission cap"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_drains_the_actor_even_when_the_scheduler_panics() {
    let (mut service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    client
        .decide_op(pay("pay:scheduler-panic", fed(1), 10, 1, 40))
        .await
        .expect("start driver");
    wait_for_registry(&client, 1).await;
    let registry = service.registry.clone();
    service.scheduler_task = Some(tokio::spawn(async {
        panic!("injected scheduler panic");
    }));
    tokio::task::yield_now().await;

    assert!(matches!(
        service.shutdown().await,
        Err(ServiceError::ActorStopped)
    ));
    assert_eq!(
        driver::len(&registry),
        0,
        "actor shutdown aborted the driver"
    );
    assert!(matches!(
        client.get_policy().await,
        Err(ServiceError::ShuttingDown)
    ));
}

#[tokio::test]
async fn critical_task_guard_reports_panics_and_clears_scheduler_liveness() {
    let (exit_tx, mut exit_rx) = mpsc::unbounded_channel();
    let alive = Arc::new(AtomicBool::new(true));
    let task_alive = alive.clone();
    let task = tokio::spawn(async move {
        let _guard = CriticalTaskGuard {
            name: "test scheduler",
            exit: exit_tx,
            liveness: Some(task_alive),
        };
        panic!("injected scheduler panic");
    });

    assert!(task.await.is_err(), "fixture task must panic");
    assert_eq!(exit_rx.recv().await, Some("test scheduler"));
    assert!(!alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn policy_and_snapshot_commands_round_trip_and_validate() {
    let (service, _) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    assert_eq!(client.get_policy().await.unwrap().per_fed_cap, Msat(1_000));
    let mut invalid = client.get_policy().await.unwrap();
    invalid.per_fed_cap = Msat(0);
    let error = client
        .put_policy(invalid)
        .await
        .expect_err("zero cap is invalid");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::PolicyInvalid,
            ..
        }
    ));
    assert!(error.to_string().contains("per_fed_cap"));
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
    let reconcile = client.reconcile().await.unwrap();
    assert_eq!(reconcile.redriven, 0);
    assert_eq!(reconcile.awaiters_rehydrated, 0);
    assert_eq!(reconcile.executing_normalized, 0);
    assert!(reconcile.blocked.is_empty());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn policy_seed_is_insert_if_absent_and_put_survives_restart() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    assert_eq!(journal.get_policy().await.unwrap(), None);
    let first_seed = Policy {
        per_fed_cap: Msat(1_000),
        spending_target: Msat(100),
        standby_target: Msat(100),
        ..Policy::default()
    };
    let first = WalletService::start_parts(
        None,
        journal.clone(),
        Arc::new(ExitExecutor(Exit::Ok)),
        first_seed,
        None,
    )
    .await
    .expect("seed policy service");
    let mut edited = first.client().get_policy().await.expect("seeded policy");
    edited.per_fed_cap = Msat(2_000);
    edited.spending_target = Msat(200);
    first
        .client()
        .put_policy(edited.clone())
        .await
        .expect("persist policy");
    first.shutdown().await.expect("first shutdown");

    let restarted = WalletService::start_parts(
        None,
        journal.clone(),
        Arc::new(ExitExecutor(Exit::Ok)),
        Policy::default(),
        None,
    )
    .await
    .expect("restart policy service");
    assert_eq!(restarted.client().get_policy().await.unwrap(), edited);
    assert_eq!(journal.get_policy().await.unwrap(), Some(edited));
    restarted.shutdown().await.expect("restart shutdown");
}

#[tokio::test]
async fn corrupt_persisted_policy_is_reported_before_service_start_returns() {
    let db = MemDatabase::new().into_database();
    let app_db = db.clone().with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&[0x0b], b"not valid json")
        .await
        .expect("insert corrupt policy row");
    dbtx.commit_tx_result()
        .await
        .expect("commit corrupt policy");

    let result = WalletService::start_parts(
        None,
        Arc::new(FedimintJournal::new(db)),
        Arc::new(ExitExecutor(Exit::Ok)),
        Policy::default(),
        None,
    )
    .await;
    let Err(error) = result else {
        panic!("corrupt persisted policy must prevent service startup");
    };
    assert!(matches!(error, ServiceError::Storage(_)));
    assert!(error.to_string().contains("policy"));
}

#[tokio::test]
async fn invalid_persisted_policy_is_reported_before_service_start_returns() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let invalid = Policy {
        per_fed_cap: Msat(0),
        ..Policy::default()
    };
    journal
        .put_policy(&invalid)
        .await
        .expect("store invalid policy fixture");

    let result = WalletService::start_parts(
        None,
        journal,
        Arc::new(ExitExecutor(Exit::Ok)),
        Policy::default(),
        None,
    )
    .await;
    let Err(error) = result else {
        panic!("invalid persisted policy must prevent service startup");
    };
    assert!(matches!(
        &error,
        ServiceError::Refused {
            reason: RefuseReason::PolicyInvalid,
            ..
        }
    ));
    assert!(error.to_string().contains("per_fed_cap"));
}

#[tokio::test]
async fn lowered_policy_cap_applies_to_the_next_decide() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("policy");
    policy.per_fed_cap = Msat(50);
    policy.spending_target = Msat(40);
    policy.standby_target = Msat(40);
    client.put_policy(policy).await.expect("lower cap");

    let error = client
        .decide_op(move_request(
            "move:over-new-cap",
            Action::Move {
                from: fed(1),
                to: fed(2),
                amount: Msat(20),
                fee_cap: Msat(0),
                gateway: None,
            },
            BTreeMap::from([(fed(1), Msat(100)), (fed(2), Msat(40))]),
            None,
        ))
        .await
        .expect_err("current policy cap must govern the next admission");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::OverCap,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn put_policy_notifies_the_scheduler_to_replace_its_old_sleep() {
    let (service, _) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    let mut wake = service.policy_wake.clone();
    let old_policy = client.get_policy().await.unwrap();
    let deadlines = wallet_core::AdaptiveSleepDeadlines {
        last_discover_ms: 1,
        ..Default::default()
    };
    assert_eq!(
        wallet_core::adaptive_sleep_ms(1, &old_policy.watch_policy(), &deadlines),
        10 * 60 * 1_000
    );

    let mut updated = old_policy;
    updated.base_interval_secs = 30;
    updated.min_interval_secs = 30;
    client.put_policy(updated.clone()).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), wake.changed())
        .await
        .expect("PutPolicy wakes the scheduler")
        .expect("wake sender remains live");
    assert_eq!(
        wallet_core::adaptive_sleep_ms(1, &updated.watch_policy(), &deadlines),
        30 * 1_000
    );
    service.shutdown().await.expect("shutdown");
}

#[test]
fn policy_projects_every_scheduler_and_probe_field() {
    let policy = Policy::default();
    let tick = TickPolicy::from(&policy);
    let watch = policy.watch_policy();
    let discovery = policy.discovery_policy();
    let probe = policy.probe_policy();
    assert_eq!(tick.per_fed_cap, policy.per_fed_cap);
    assert_eq!(tick.target_spending_balance, policy.spending_target);
    assert_eq!(tick.probe_gate_policy, probe);
    assert_eq!(watch.base_interval_ms, policy.base_interval_secs * 1_000);
    assert_eq!(
        watch.probe_budget.max_probe_spend_per_week_msat,
        policy.max_probe_spend_per_week.0
    );
    assert_eq!(discovery.auto_join, policy.auto_join);
    assert_eq!(
        discovery.max_auto_joins_per_week,
        policy.max_auto_joins_per_week
    );
}

#[tokio::test]
async fn decide_tick_round_matches_the_pure_allocator_fixture() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.unwrap();
    policy.per_fed_cap = Msat(1_000);
    policy.spending_target = Msat(100);
    policy.standby_target = Msat(100);
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    client.put_policy(policy.clone()).await.unwrap();
    let probes = vec![(fed(1), healthy_probe(250)), (fed(2), healthy_probe(0))];
    let occurrence = Occurrence(31);
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: probes.clone(),
            occurrence,
            now_ms: 99,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("decide tick round");

    let mut tick_policy = TickPolicy::from(&policy);
    tick_policy.occurrence = occurrence;
    tick_policy.now = 99;
    let expected_snapshot = crate::tick::build_snapshot(
        &probes,
        &tick_policy,
        &wallet_core::ScorerPolicy::default(),
        &std::collections::BTreeSet::new(),
        &BTreeMap::new(),
    );
    assert_eq!(
        round.decisions,
        wallet_core::decide(&expected_snapshot, occurrence)
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn actor_rejects_a_default_or_foreign_tick_plan_token() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let error = client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), healthy_probe(200)), (fed(2), healthy_probe(0))],
            occurrence: Occurrence(59),
            now_ms: 1,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: super::GoalAdmissionSnapshot::default(),
        })
        .await
        .expect_err("a default capability was not issued by this actor");
    assert!(error
        .to_string()
        .contains("foreign actor-issued admission token"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn actor_rejects_a_foreign_balance_facts_token() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let error = client
        .commit_tick(
            TickRound::for_test(
                vec![],
                0,
                client
                    .issue_tick_plan_token()
                    .await
                    .expect("actor plan token"),
            ),
            BTreeMap::new(),
            super::BalanceFactsToken {
                authority: std::sync::Arc::new(()),
                generations: BTreeMap::new(),
            },
        )
        .await
        .expect_err("a balance-facts capability must come from this actor");
    assert!(error
        .to_string()
        .contains("foreign actor-issued balance-facts token"));
    service.shutdown().await.expect("shutdown");
}

/// Direct users of the actor token API get the same pending-goal baseline as a
/// scheduler reconcile.  That baseline is intentionally stronger than the
/// caller-supplied advisory blockers: it still suppresses a goal after the
/// original live intent terminalizes during the planning window.
#[tokio::test]
async fn direct_tick_plan_token_retains_live_goal_baseline_through_terminalization() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("policy");
    policy.per_fed_cap = Msat(10_000_000);
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    policy.spending_target = Msat(1_000_000);
    policy.standby_target = Msat(1_000_000);
    client
        .put_policy(policy)
        .await
        .expect("designate funding pair");

    let live_key = IdempotencyKey("agent:live-fund-into-b-for-token-baseline".to_owned());
    client
        .decide_op(agent_request(
            &live_key.0,
            Action::Move {
                from: fed(1),
                to: fed(2),
                amount: Msat(1_000_000),
                fee_cap: Msat(0),
                gateway: None,
            },
            ReasonCode::StandbyBelowTarget,
            Occurrence(68),
            BTreeMap::from([(fed(1), Msat(2_000_000)), (fed(2), Msat(0))]),
        ))
        .await
        .expect("live Agent goal admitted");
    let token = client
        .issue_tick_plan_token()
        .await
        .expect("token captures the pending Agent goal");
    client
        .journal_transition(
            live_key,
            JournalTransition::SetStatus {
                expected_attempt: 0,
                status: IntentStatus::Done,
                error: None,
            },
        )
        .await
        .expect("live goal terminalizes before planning");

    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), healthy_probe(2_000_000)),
                (fed(2), healthy_probe(0)),
            ],
            occurrence: Occurrence(69),
            now_ms: 1,
            price_routes: false,
            // A direct caller cannot erase the token's actor-captured baseline.
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: token,
        })
        .await
        .expect("actor plans from the token baseline");
    assert!(
        !round.decisions.iter().any(|decision| {
            AllocatorGoal::of_decision(
                decision,
                Actor::Agent {
                    occurrence: Occurrence(69),
                },
            ) == Some(AllocatorGoal::FundInto(fed(2)))
        }),
        "the old B funding goal is absent despite an empty caller blocker set"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn absent_transition_upsert_cannot_bypass_actor_admission() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let intent = agent_funding_intent(fed(1), fed(2), Msat(100), Occurrence(58));
    let error = client
        .journal_transition(
            intent.idempotency_key.clone(),
            JournalTransition::Upsert {
                expected_attempt: intent.attempt,
                intent: Box::new(intent.clone()),
            },
        )
        .await
        .expect_err("a driver transition cannot create a fresh Agent intent");
    assert!(error
        .to_string()
        .contains("Upsert cannot create an absent intent"));
    assert!(journal
        .get(&intent.idempotency_key)
        .await
        .unwrap()
        .is_none());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn empty_tick_round_commits_normally() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let token = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts token");
    let report = client
        .commit_tick(
            TickRound::for_test(vec![], 0, token),
            BTreeMap::new(),
            facts,
        )
        .await
        .expect("empty scheduler round is a normal no-op");
    assert!(report.accepted.is_empty());
    assert!(report.refused.is_empty());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn missing_evacuation_destination_balance_is_a_scoped_refusal() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let token = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts token");
    let decision = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(1),
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence: Occurrence(57),
        idempotency_key: IdempotencyKey("missing-evac-destination".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![decision], 0, token),
            BTreeMap::from([(fed(1), Msat(10))]),
            facts,
        )
        .await
        .expect("missing destination is a per-decision refusal");
    assert_eq!(report.refused.len(), 1);
    assert!(report.refused[0].message.contains("no fresh balance"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shared_destination_batch_does_not_stale_its_later_sibling() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let token = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts token");
    let evacuation = |from: FederationId, key: &str| AllocatorDecision {
        action: Action::Evacuate {
            from,
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence: Occurrence(56),
        idempotency_key: IdempotencyKey(key.to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![
                    evacuation(fed(3), "evac-c-b"),
                    evacuation(fed(4), "evac-d-b"),
                ],
                0,
                token,
            ),
            BTreeMap::from([(fed(2), Msat(0)), (fed(3), Msat(10)), (fed(4), Msat(10))]),
            facts,
        )
        .await
        .expect("same-batch admissions compare one pre-loop generation snapshot");
    assert_eq!(report.accepted.len(), 2);
    service.shutdown().await.expect("shutdown");
}

/// An unpaid external DirectInflow consumes destination cap room but does not promise
/// wallet-delivered target value. A fresh Agent top-up may therefore spend the full gap.
#[tokio::test]
async fn commit_tick_target_gap_excludes_pending_user_direct_inflow() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(65);
    let snapshot = client.issue_tick_plan_token().await.expect("old token");
    client
        .decide_op(move_request(
            "user:pending-direct-inflow-b",
            Action::DirectInflow {
                to: fed(2),
                amount: Msat(25),
                fee_cap: Msat(0),
            },
            BTreeMap::from([(fed(2), Msat(0))]),
            None,
        ))
        .await
        .expect("user direct inflow is durable but still pending");
    for _ in 0..100 {
        if executor.calls.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);

    // Issue after the user admission: this is a fresh (but not yet settled)
    // sample, so the refusal below is specifically the inbound reservation,
    // not the balance-generation guard.
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("fresh facts token");
    let old = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(100),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("agent:old-full-gap-funding-b".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![old.clone()], 0, snapshot),
            BTreeMap::from([(fed(1), Msat(100)), (fed(2), Msat(0))]),
            facts,
        )
        .await
        .expect("unpaid external inflow does not reduce the target gap");

    assert_eq!(
        report.accepted,
        vec![old.idempotency_key.clone()],
        "{report:#?}"
    );
    assert!(report.refused.is_empty(), "{report:#?}");
    assert!(
        journal.get(&old.idempotency_key).await.unwrap().is_some(),
        "the full-gap Agent move becomes an intent"
    );
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        2,
        "both the external DirectInflow and full Agent move start their drivers"
    );
    service.shutdown().await.expect("shutdown");
}

/// Commit-order reservations are local to the batch.  An evacuation into B
/// must shrink a later funding move's B target gap, while two independent
/// evacuations into B remain legal (covered by the sibling test above).
#[tokio::test]
async fn commit_tick_evacuation_reserves_destination_before_later_funding() {
    let executor = Arc::new(CountingExitExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let token = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts token");
    let evacuation = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(3),
            to: fed(2),
            amount: Msat(20),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence: Occurrence(66),
        idempotency_key: IdempotencyKey("evac-c-before-fund-b".to_owned()),
    };
    let funding = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(100),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(66),
        idempotency_key: IdempotencyKey("fund-b-after-evac-c".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![evacuation.clone(), funding.clone()], 0, token),
            BTreeMap::from([(fed(1), Msat(100)), (fed(2), Msat(0)), (fed(3), Msat(20))]),
            facts,
        )
        .await
        .expect("local target-gap conflict is a scoped refusal");

    assert_eq!(report.accepted, vec![evacuation.idempotency_key.clone()]);
    assert_eq!(report.refused.len(), 1);
    assert_eq!(report.refused[0].key, funding.idempotency_key);
    assert!(report.refused[0].message.contains("fresh target shortfall"));
    assert!(journal
        .get(&evacuation.idempotency_key)
        .await
        .unwrap()
        .is_some());
    assert!(journal
        .get(&funding.idempotency_key)
        .await
        .unwrap()
        .is_none());
    wait_for_registry(&client, 0).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_tick_target_sized_attach_does_not_double_reserve_before_an_independent_evacuation()
{
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("policy");
    policy.standby_target = Msat(60);
    client
        .put_policy(policy)
        .await
        .expect("set exact attach target");
    let occurrence = Occurrence(976);
    let existing = agent_funding_intent(fed(1), fed(2), Msat(60), occurrence);
    let existing_decision = AllocatorDecision {
        action: existing.action.clone(),
        reason: existing.reason,
        occurrence,
        idempotency_key: existing.idempotency_key.clone(),
    };
    journal
        .upsert(&existing)
        .await
        .expect("seed same-key attach");
    let evacuation = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(4),
            to: fed(2),
            amount: Msat(940),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("evac:after-existing-attach".to_owned()),
    };
    let over_cap = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(5),
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("evac:after-cap-filled-attach".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![
                    existing_decision.clone(),
                    evacuation.clone(),
                    over_cap.clone(),
                ],
                1,
                client.issue_tick_plan_token().await.expect("plan token"),
            ),
            BTreeMap::from([
                (fed(1), Msat(60)),
                (fed(2), Msat(0)),
                (fed(4), Msat(940)),
                (fed(5), Msat(1)),
            ]),
            client
                .issue_balance_facts_token()
                .await
                .expect("balance facts"),
        )
        .await
        .expect("same-key attach and independent evacuation commit");
    assert_eq!(
        report.accepted,
        vec![
            existing_decision.idempotency_key,
            evacuation.idempotency_key
        ],
        "the initial projection already contains the attached intent's 60-msat inbound hold: {report:#?}"
    );
    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, over_cap.idempotency_key);
    assert_eq!(
        report.refused[0].reason,
        RefuseReason::OverCap,
        "the attached intent must still count exactly once against cap room: {report:#?}"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_tick_rechecks_reservations_and_records_a_dropped_decision() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let occurrence = Occurrence(32);
    client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), healthy_probe(100)), (fed(2), healthy_probe(0))],
            occurrence,
            now_ms: 100,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("seed tick facts");
    client
        .decide_op(pay("pay:consumed-surplus", fed(1), 80, 0, 81))
        .await
        .expect("user pay consumes the surplus during route validation");
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(30),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:dropped-mid-validation".to_owned()),
    };
    let report = client
        .commit_tick_with_facts_legacy(
            vec![decision.clone()],
            Some(BTreeMap::from([(fed(1), Msat(100)), (fed(2), Msat(0))])),
            None,
            0,
            client.issue_tick_plan_token().await.expect("token"),
        )
        .await
        .expect("commit records an admission refusal instead of forcing the move");
    assert!(report.accepted.is_empty());
    assert_eq!(report.refused.len(), 1);
    assert_eq!(
        report.refused[0].reason,
        RefuseReason::InsufficientAfterReservations
    );
    assert!(journal
        .operation(&crate::journal::OperationRef::Key(IdempotencyKey(format!(
            "tick-drop:{}:{}",
            occurrence.0, decision.idempotency_key.0
        ))))
        .await
        .unwrap()
        .is_some());
    service.shutdown().await.expect("shutdown");
}

/// ADR-0031: a poison-tolerant scheduler pre-filter is advisory; the actor's admission seam must
/// still fail closed when its complete reservation view is corrupt.
#[tokio::test]
async fn commit_tick_refuses_when_strict_admission_sees_corruption_prefilter_omitted() {
    let db = MemDatabase::new().into_database();
    let journal = Arc::new(FedimintJournal::new(db.clone()));
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
    )
    .await
    .expect("start isolated service");
    let client = service.client();
    let occurrence = Occurrence(44);
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:must-not-admit-after-corruption".to_owned()),
    };
    // Plan successfully first, then corrupt durable state in the plan→commit window. This lets the
    // test reach the production CommitTick admission seam instead of the planner's own strict
    // reservation projection.
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), healthy_probe(100)), (fed(2), healthy_probe(0))],
            occurrence,
            now_ms: 109,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("plan before injecting corruption");

    let poison_key = IdempotencyKey("move:corrupt-pending".to_owned());
    let app_db = db.with_prefix(vec![0x00]);
    let mut intent_key = vec![0x01];
    intent_key.extend_from_slice(poison_key.0.as_bytes());
    let mut pending_index_key = vec![0x04, 0x00];
    pending_index_key.extend_from_slice(poison_key.0.as_bytes());
    let mut dbtx = app_db.begin_transaction().await;
    // Do not use the typed DB API here: it decodes values eagerly and panics on this deliberately
    // malformed row. The journal's raw scan is the production poison-tolerance boundary.
    dbtx.raw_insert_bytes(&intent_key, b"not valid json")
        .await
        .expect("insert corrupt pending intent");
    dbtx.raw_insert_bytes(&pending_index_key, &[])
        .await
        .expect("index corrupt intent as pending");
    dbtx.commit_tx_result()
        .await
        .expect("commit corrupt pending intent");

    let pending = journal
        .pending()
        .await
        .expect("poison-tolerant blocker pre-filter");
    assert!(
        pending.is_empty(),
        "the advisory pre-filter must omit the undecodable row: {pending:#?}"
    );
    assert!(
        !wallet_core::GoalBlockers::from_intents(&pending)
            .blocks_decision(&decision, Actor::Agent { occurrence }),
        "the omitted corrupt row leaves the advisory blocker projection incomplete"
    );

    // CommitTick reaches decide_and_journal, whose reservation_intents() scan is strict. It must
    // refuse rather than admit from the incomplete pre-filter's view.
    let report = client
        .commit_tick_legacy(
            vec![decision.clone()],
            round.planned_generation,
            round.admission_snapshot,
        )
        .await
        .expect("corrupt admission view is a per-decision tick refusal");
    assert!(
        report.accepted.is_empty(),
        "no intent may be admitted: {report:#?}"
    );
    assert_eq!(
        report.refused.len(),
        1,
        "the tick must refuse its only decision"
    );
    assert_eq!(report.refused[0].key, decision.idempotency_key);
    assert_eq!(report.refused[0].reason, RefuseReason::StorageError);
    assert!(
        report.refused[0].message.contains("intent row"),
        "the strict reservation scan must name the corrupt intent: {report:#?}"
    );
    assert!(
        journal
            .get(&decision.idempotency_key)
            .await
            .expect("read candidate intent")
            .is_none(),
        "the refused decision is never journaled"
    );
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        0,
        "no money execution starts after strict admission rejects the tick"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_tick_refuses_a_batch_planned_under_a_superseded_policy() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let occurrence = Occurrence(41);
    // Plan the round under the current policy generation. The scheduler validates routes
    // over the network before committing; a PutPolicy can land in that window.
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), healthy_probe(100)), (fed(2), healthy_probe(0))],
            occurrence,
            now_ms: 108,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("seed tick facts");
    // The operator changes policy mid-validation — the round's sizing is now stale.
    let mut policy = client.get_policy().await.expect("policy");
    policy.max_fee = Msat(policy.max_fee.0 + 1);
    client.put_policy(policy).await.expect("policy supersede");
    let tick_key = IdempotencyKey("tick:stale-policy-open".to_owned());
    journal
        .record_tick_started(&tick_key, occurrence, 109)
        .await
        .expect("scheduler opened its audit row before route pricing completed");
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(30),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:stale-generation".to_owned()),
    };
    // Committing under the old generation refuses the whole batch — softly, not an error.
    let report = client
        .commit_tick_with_facts(
            TickRound::for_test(
                vec![decision.clone()],
                round.planned_generation,
                round.admission_snapshot,
            ),
            BTreeMap::from([(fed(1), Msat(100)), (fed(2), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("facts are immaterial to a superseded round"),
            Some(tick_key.clone()),
        )
        .await
        .expect("stale-generation commit fails softly, not with a transport error");
    assert!(report.accepted.is_empty());
    assert_eq!(report.refused.len(), 1);
    assert_eq!(report.refused[0].key, decision.idempotency_key);
    assert_eq!(report.refused[0].reason, RefuseReason::PolicySuperseded);
    // Nothing was admitted and no drop row is needed, but a scheduler-opened Started row must
    // never leak when policy supersedes the in-flight plan.
    assert_eq!(driver::external_len(&service.registry), 0);
    assert_eq!(
        journal
            .operation(&crate::journal::OperationRef::Key(tick_key))
            .await
            .expect("tick row")
            .expect("scheduler row exists")
            .status,
        OperationStatus::Failed
    );
    assert!(journal
        .operation(&crate::journal::OperationRef::Key(IdempotencyKey(format!(
            "tick-drop:{}:{}",
            occurrence.0, decision.idempotency_key.0
        ))))
        .await
        .unwrap()
        .is_none());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn membership_admission_invalidates_an_older_tick_world_without_global_blocking() {
    let executor = Arc::new(SlowJoinExecutor::default());
    let (service, _) = fixture(executor.clone()).await;
    let client = service.client();
    let token = client
        .issue_tick_plan_token()
        .await
        .expect("pre-join token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("pre-join balance facts");
    client
        .decide_op(move_request(
            "join:world-generation",
            Action::Join {
                federation: fed(9),
                invite: "world-invite".to_owned(),
                membership_preexisting: false,
            },
            BTreeMap::new(),
            None,
        ))
        .await
        .expect("membership admission");
    while executor.calls.load(Ordering::SeqCst) != 1 {
        tokio::task::yield_now().await;
    }
    client
        .issue_tick_plan_token()
        .await
        .expect("a slow membership network wait does not globally block fresh tick authority");
    // Exercise the production planning entry point, rather than manufacturing a TickRound:
    // the old capability must be rejected at DecideTickRound itself and cannot be relabelled
    // with the actor's current membership world generation.
    assert!(
        client
            .decide_tick_round(ProbeFacts {
                probes: Vec::new(),
                occurrence: Occurrence(91),
                now_ms: 1,
                price_routes: false,
                blocked: wallet_core::GoalBlockers::default(),
                admission_snapshot: token.clone(),
            })
            .await
            .is_err(),
        "a token minted before Join admission must not plan a fresh round"
    );
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(91),
        idempotency_key: IdempotencyKey("move:old-world".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![decision], 0, token),
            BTreeMap::from([(fed(1), Msat(100)), (fed(2), Msat(0))]),
            facts,
        )
        .await
        .expect("stale world is a soft whole-batch refusal");
    assert!(report.accepted.is_empty());
    assert_eq!(report.refused.len(), 1);
    assert_eq!(report.refused[0].reason, RefuseReason::Conflict);
    client
        .journal_transition(
            IdempotencyKey("join:world-generation".to_owned()),
            JournalTransition::CompareAndSet {
                expected_attempt: 0,
                expected: IntentStatus::Executing,
                new: IntentStatus::Done,
            },
        )
        .await
        .expect("terminal membership transition");
    client
        .issue_tick_plan_token()
        .await
        .expect("new authority after terminal membership transition");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ambiguous_terminal_join_and_recover_writes_bump_the_membership_world() {
    for (label, action) in [
        (
            "join",
            Action::Join {
                federation: fed(9),
                invite: "ambiguous-terminal-join".to_owned(),
                membership_preexisting: false,
            },
        ),
        (
            "recover",
            Action::Recover {
                federation: fed(10),
                invite: "ambiguous-terminal-recover".to_owned(),
            },
        ),
    ] {
        let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
        let client = service.client();
        let key = IdempotencyKey(format!("{label}:ambiguous-terminal-world"));
        journal
            .upsert(&Intent {
                idempotency_key: key.clone(),
                attempt: 0,
                action,
                max_fee: None,
                status: IntentStatus::Executing,
                reason: ReasonCode::UserInitiated,
                actor: Actor::User,
                created_at_ms: 1,
                operation_id: None,
                invoice: None,
            })
            .await
            .expect("seed membership intent");
        let stale_plan = client
            .issue_tick_plan_token()
            .await
            .expect("plan before ambiguous terminal membership write");
        let stale_facts = client
            .issue_balance_facts_token()
            .await
            .expect("facts before ambiguous terminal membership write");
        journal.fail_after_next_status_write_for_test();
        client
            .journal_transition(
                key,
                JournalTransition::SetStatus {
                    expected_attempt: 0,
                    status: IntentStatus::Done,
                    error: None,
                },
            )
            .await
            .expect_err("terminal membership writer reports its injected post-commit error");

        let decision = one_msat_funding(
            &format!("move:stale-{label}-membership-world"),
            fed(1),
            fed(2),
            Occurrence(986),
        );
        let report = client
            .commit_tick(
                TickRound::for_test(vec![decision], 0, stale_plan),
                BTreeMap::from([(fed(1), Msat(1)), (fed(2), Msat(0))]),
                stale_facts,
            )
            .await
            .expect("stale membership world is a soft refusal");
        assert_eq!(report.refused.len(), 1, "{label}: {report:#?}");
        assert_eq!(report.refused[0].reason, RefuseReason::Conflict);

        let fresh = one_msat_funding(
            &format!("move:fresh-{label}-membership-world"),
            fed(1),
            fed(2),
            Occurrence(987),
        );
        let fresh_report = client
            .commit_tick(
                TickRound::for_test(
                    vec![fresh.clone()],
                    0,
                    client
                        .issue_tick_plan_token()
                        .await
                        .expect("fresh plan token"),
                ),
                BTreeMap::from([(fed(1), Msat(1)), (fed(2), Msat(0))]),
                client
                    .issue_balance_facts_token()
                    .await
                    .expect("fresh balance facts"),
            )
            .await
            .expect("fresh membership world token remains usable");
        assert_eq!(fresh_report.accepted, vec![fresh.idempotency_key]);
        service.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn retryable_join_pending_allows_an_unrelated_evacuation_tick() {
    let executor = Arc::new(RetryableJoinExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    client
        .decide_op(move_request(
            "join:retryable-pending",
            Action::Join {
                federation: fed(9),
                invite: "retryable-pending-invite".to_owned(),
                membership_preexisting: false,
            },
            BTreeMap::new(),
            None,
        ))
        .await
        .expect("join admitted");
    while executor.calls.load(Ordering::SeqCst) != 1 || registry_size(&client).await != 0 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        journal
            .get(&IdempotencyKey("join:retryable-pending".to_owned()))
            .await
            .expect("join intent")
            .expect("join exists")
            .status,
        IntentStatus::Pending,
        "the join remains durable crash-recoverable work"
    );

    let token = client
        .issue_tick_plan_token()
        .await
        .expect("retryable Join does not fence an unrelated tick");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("fresh balance facts");
    let evacuation = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(1),
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence: Occurrence(93),
        idempotency_key: IdempotencyKey("agent:evacuate-during-retryable-join".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![evacuation.clone()], 0, token),
            BTreeMap::from([(fed(1), Msat(10)), (fed(2), Msat(0))]),
            facts,
        )
        .await
        .expect("unrelated evacuation tick commits");
    assert_eq!(report.accepted, vec![evacuation.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn durable_reconcile_rehydrates_membership_and_awaits_while_scheduler_authority_is_fenced() {
    let executor = Arc::new(SlowJoinExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let keys = [
        IdempotencyKey("join:durable-reconcile".to_owned()),
        IdempotencyKey("recover:durable-reconcile".to_owned()),
    ];
    for (key, action) in [
        (
            keys[0].clone(),
            Action::Join {
                federation: fed(9),
                invite: "durable-reconcile-join-invite".to_owned(),
                membership_preexisting: false,
            },
        ),
        (
            keys[1].clone(),
            Action::Recover {
                federation: fed(10),
                invite: "durable-reconcile-recover-invite".to_owned(),
            },
        ),
    ] {
        journal
            .upsert(&Intent {
                idempotency_key: key,
                attempt: 0,
                action,
                max_fee: None,
                status: IntentStatus::Pending,
                reason: ReasonCode::UserInitiated,
                actor: Actor::User,
                created_at_ms: 1,
                operation_id: None,
                invoice: None,
            })
            .await
            .expect("seed crash-orphaned membership work");
    }

    let report = client
        .reconcile_durable()
        .await
        .expect("public recovery rehydrates membership work");
    assert_eq!(report.redriven, 2);
    while executor.calls.load(Ordering::SeqCst) != 2 {
        tokio::task::yield_now().await;
    }
    client
        .reconcile()
        .await
        .expect("crash-recovered membership work does not globally fence scheduler authority");

    let awaiters = keys.map(|key| {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .resolve_await(
                    key,
                    AwaitTarget::Terminal,
                    Instant::now() + std::time::Duration::from_secs(21),
                )
                .await
        })
    });
    tokio::time::advance(std::time::Duration::from_secs(20)).await;
    for awaiter in awaiters {
        assert!(matches!(
            awaiter.await.expect("awaiter task"),
            Ok(AwaitOutcome::Terminal(intent)) if intent.status == IntentStatus::Done
        ));
    }
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn executing_recovery_intent_does_not_fence_tick_authority_before_publication() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let key = IdempotencyKey("recover:tick-world-visibility".to_owned());
    let intent = Intent {
        idempotency_key: key.clone(),
        attempt: 0,
        action: Action::Recover {
            federation: fed(9),
            invite: "recovery-world-invite".to_owned(),
        },
        max_fee: None,
        status: IntentStatus::Executing,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: 1,
        operation_id: None,
        invoice: None,
    };
    journal
        .upsert(&intent)
        .await
        .expect("seed executing recovery");

    client
        .issue_tick_plan_token()
        .await
        .expect("an executing recovery has not yet published a client");
    client
        .journal_transition(
            key,
            JournalTransition::CompareAndSet {
                expected_attempt: 0,
                expected: IntentStatus::Executing,
                new: IntentStatus::Done,
            },
        )
        .await
        .expect("terminal recovery transition");
    client
        .issue_tick_plan_token()
        .await
        .expect("the completed recovery restores usable tick authority");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn membership_publication_lease_refuses_authority_and_stales_prepublication_token() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let token = client
        .issue_tick_plan_token()
        .await
        .expect("token before membership publication");
    let lease = client
        .begin_membership_mutation()
        .await
        .expect("begin short publication lease");
    assert!(
        client.issue_tick_plan_token().await.is_err(),
        "an active Join/Recover publication lease refuses new tick authority"
    );
    client
        .end_membership_mutation(lease)
        .await
        .expect("publication end advances membership world generation");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts after publication");
    let stale = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(94),
        idempotency_key: IdempotencyKey("agent:prepublication-token".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![stale.clone()], 0, token),
            BTreeMap::from([(fed(1), Msat(1)), (fed(2), Msat(0))]),
            facts,
        )
        .await
        .expect("stale authority is a soft whole-batch refusal");
    assert_eq!(report.accepted, Vec::<IdempotencyKey>::new());
    assert_eq!(report.refused.len(), 1);
    assert_eq!(report.refused[0].key, stale.idempotency_key);
    assert_eq!(report.refused[0].reason, RefuseReason::Conflict);
    service.shutdown().await.expect("shutdown");
}

/// Membership-publication epochs are local counters, so a lease must also carry its issuing
/// actor's opaque authority.  A crossed same-epoch lease must not release or publish the other
/// actor's membership world.
#[tokio::test]
async fn membership_publication_lease_cannot_end_a_different_actors_live_lease() {
    let (first_service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let (second_service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let first = first_service.client();
    let second = second_service.client();

    let _first_lease = first
        .begin_membership_mutation()
        .await
        .expect("first actor membership lease");
    let second_lease = second
        .begin_membership_mutation()
        .await
        .expect("second actor same-epoch membership lease");

    let error = first
        .end_membership_mutation(second_lease)
        .await
        .expect_err("a same-epoch lease from another actor must not end this publication");
    assert!(
        error.to_string().contains("missing or stale"),
        "foreign authority must be rejected before local publication state changes: {error}"
    );
    assert!(
        first.issue_tick_plan_token().await.is_err(),
        "the foreign lease must not release the first actor's live publication"
    );
    assert!(
        second.issue_tick_plan_token().await.is_err(),
        "consuming the foreign lease must not release its issuing actor either"
    );

    first_service
        .shutdown()
        .await
        .expect("shutdown first service with fail-closed publication lease");
    second_service
        .shutdown()
        .await
        .expect("shutdown second service with its live publication lease");
}

#[tokio::test]
async fn external_terminal_lease_fails_closed_while_live_but_not_after_ending() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let token = client
        .issue_tick_plan_token()
        .await
        .expect("plan before terminal lease");
    let facts_before_lease = client
        .issue_balance_facts_token()
        .await
        .expect("facts before terminal lease");
    let lease = client
        .begin_external_terminal_mutation(pay("terminal-pay", fed(1), 1, 0, 1).decision.action)
        .await
        .expect("lease");
    assert!(client.issue_tick_plan_token().await.is_err());
    assert!(client.issue_balance_facts_token().await.is_err());
    let blocked = AllocatorDecision {
        action: Action::Move {
            from: fed(3),
            to: fed(4),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(95),
        idempotency_key: IdempotencyKey("agent:blocked-by-live-terminal-lease".to_owned()),
    };
    assert!(
        client
            .commit_tick(
                TickRound::for_test(vec![blocked.clone()], 0, token.clone()),
                BTreeMap::from([(fed(3), Msat(1)), (fed(4), Msat(0))]),
                facts_before_lease,
            )
            .await
            .is_err(),
        "a live terminal lease refuses commit rather than admitting a decision"
    );
    assert!(
        journal
            .get(&blocked.idempotency_key)
            .await
            .unwrap()
            .is_none(),
        "a live lease cannot journal the rejected tick decision"
    );
    client
        .end_external_terminal_mutation(lease)
        .await
        .expect("end bumps the affected balance generation");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("fresh facts after end");
    let independent = AllocatorDecision {
        action: Action::Move {
            from: fed(3),
            to: fed(4),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(95),
        idempotency_key: IdempotencyKey("agent:independent-after-terminal-pay".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![independent.clone()], 0, token),
            BTreeMap::from([(fed(3), Msat(1)), (fed(4), Msat(0))]),
            facts,
        )
        .await
        .expect("a completed Pay lease does not globally stale an unrelated old plan");
    assert_eq!(report.accepted, vec![independent.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn external_terminal_lease_cannot_end_a_different_actors_live_lease() {
    let (first_service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let (second_service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let first = first_service.client();
    let second = second_service.client();

    let first_plan = first
        .issue_tick_plan_token()
        .await
        .expect("plan before either terminal lease");
    let first_facts = first
        .issue_balance_facts_token()
        .await
        .expect("facts before either terminal lease");
    let first_lease = first
        .begin_external_terminal_mutation(
            pay("first-terminal-pay", fed(1), 1, 0, 1).decision.action,
        )
        .await
        .expect("first actor lease");
    let second_lease = second
        .begin_external_terminal_mutation(
            pay("second-terminal-pay", fed(2), 1, 0, 1).decision.action,
        )
        .await
        .expect("second actor lease at the same epoch");

    let error = first
        .end_external_terminal_mutation(second_lease)
        .await
        .expect_err("a same-epoch lease from another actor must not end this actor's lease");
    assert!(
        error.to_string().contains("missing or stale"),
        "foreign authority must be rejected before any local lease state changes: {error}"
    );
    assert!(
        first.issue_balance_facts_token().await.is_err(),
        "rejecting the foreign lease must leave the first actor's own lease live"
    );
    assert!(
        second.issue_balance_facts_token().await.is_err(),
        "the consumed foreign lease must not release its issuing actor either"
    );

    first
        .end_external_terminal_mutation(first_lease)
        .await
        .expect("the first actor still owns and can end its original lease");
    let independent = AllocatorDecision {
        action: Action::Move {
            from: fed(2),
            to: fed(3),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(96),
        idempotency_key: IdempotencyKey("agent:foreign-terminal-scope-not-bumped".to_owned()),
    };
    let report = first
        .commit_tick(
            TickRound::for_test(vec![independent.clone()], 0, first_plan),
            BTreeMap::from([(fed(2), Msat(1)), (fed(3), Msat(0))]),
            first_facts,
        )
        .await
        .expect("rejecting a foreign lease must not bump its balance scope on the first actor");
    assert_eq!(report.accepted, vec![independent.idempotency_key]);

    first_service
        .shutdown()
        .await
        .expect("shutdown first service");
    second_service
        .shutdown()
        .await
        .expect("shutdown second service with its fail-closed lease still live");
}

#[tokio::test]
async fn external_terminal_lease_rejects_actions_without_balance_scope() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let error = client
        .begin_external_terminal_mutation(Action::Join {
            federation: fed(1),
            invite: "invite".to_owned(),
            membership_preexisting: false,
        })
        .await
        .expect_err("membership-only action has no terminal balance scope");
    assert!(error
        .to_string()
        .contains("requires a balance-affecting action"));
    client
        .issue_tick_plan_token()
        .await
        .expect("an empty scope rejection never acquires the live mutation gate");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ended_terminal_lease_stales_only_its_balance_facts() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let stale_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts before terminal lease");
    let lease = client
        .begin_external_terminal_mutation(pay("terminal-pay", fed(1), 1, 0, 1).decision.action)
        .await
        .expect("Pay terminal lease");
    client
        .end_external_terminal_mutation(lease)
        .await
        .expect("end advances Pay source generation");
    let fresh_plan = client
        .issue_tick_plan_token()
        .await
        .expect("plan after terminal lease");
    let affected = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(96),
        idempotency_key: IdempotencyKey("agent:stale-pay-source".to_owned()),
    };
    let independent = AllocatorDecision {
        action: Action::Move {
            from: fed(3),
            to: fed(4),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(96),
        idempotency_key: IdempotencyKey("agent:fresh-independent-source".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![affected.clone(), independent.clone()], 0, fresh_plan),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
            ]),
            stale_facts,
        )
        .await
        .expect("stale facts are a per-decision conflict, not a global error");
    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, affected.idempotency_key);
    assert_eq!(report.refused[0].reason, RefuseReason::Conflict);
    assert_eq!(report.accepted, vec![independent.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

fn one_msat_funding(
    key: &str,
    from: FederationId,
    to: FederationId,
    occurrence: Occurrence,
) -> AllocatorDecision {
    AllocatorDecision {
        action: Action::Move {
            from,
            to,
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey(key.to_owned()),
    }
}

#[tokio::test]
async fn actor_routed_pay_artifact_stales_only_its_source_balance_facts() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, _journal) = fixture(executor.clone()).await;
    let client = service.client();
    let pay_key = IdempotencyKey("pay:artifact-generation".to_owned());
    client
        .decide_op(pay(&pay_key.0, fed(1), 80, 0, 71))
        .await
        .expect("seed strict raw-pay reservation");
    let stale_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts before send artifact");
    assert!(
        client
            .set_operation_artifact_if_attempt(pay_key, 0, OperationId([0x71; 32]), None)
            .await
            .expect("actor-routed raw artifact"),
        "the current attempt accepts its first operation artifact"
    );

    let occurrence = Occurrence(971);
    let touched = one_msat_funding("move:after-pay-artifact", fed(1), fed(2), occurrence);
    let independent = one_msat_funding("move:independent-pay-artifact", fed(3), fed(4), occurrence);
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![touched.clone(), independent.clone()],
                0,
                client.issue_tick_plan_token().await.expect("plan token"),
            ),
            // This sample represents the post-issuance spendable balance. The allocator projection
            // absorbs the Pay, so only the artifact command's generation bump can reject its stale
            // pre-artifact token.
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
            ]),
            stale_facts,
        )
        .await
        .expect("artifact staleness is scoped per decision");
    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, touched.idempotency_key);
    assert!(report.refused[0].message.contains("balance facts changed"));
    assert_eq!(report.accepted, vec![independent.idempotency_key]);

    let mut fresh_user_pay = pay("pay:user-remains-strict", fed(1), 1, 0, 75);
    fresh_user_pay.balances = BTreeMap::from([(fed(1), Msat(1))]);
    assert!(matches!(
        client.decide_op(fresh_user_pay).await,
        Err(ServiceError::Refused {
            reason: RefuseReason::InsufficientAfterReservations,
            ..
        })
    ));

    let fresh_occurrence = Occurrence(975);
    let post_artifact = one_msat_funding(
        "move:fresh-after-pay-artifact",
        fed(1),
        fed(5),
        fresh_occurrence,
    );
    let fresh_report = client
        .commit_tick(
            TickRound::for_test(
                vec![post_artifact.clone()],
                0,
                client
                    .issue_tick_plan_token()
                    .await
                    .expect("post-artifact plan token"),
            ),
            BTreeMap::from([(fed(1), Msat(1)), (fed(5), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("post-artifact balance facts"),
        )
        .await
        .expect("fresh facts may use the absorbed Pay debit");
    assert_eq!(fresh_report.accepted, vec![post_artifact.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn actor_routed_move_sending_stales_both_endpoint_balance_facts() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let key = IdempotencyKey("move:sending-generation".to_owned());
    let action = Action::Move {
        from: fed(1),
        to: fed(2),
        amount: Msat(80),
        fee_cap: Msat(0),
        gateway: None,
    };
    let intent = Intent {
        idempotency_key: key.clone(),
        attempt: 0,
        action,
        max_fee: Some(Msat(0)),
        status: IntentStatus::Executing,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: 1,
        operation_id: None,
        invoice: None,
    };
    journal
        .upsert(&intent)
        .await
        .expect("seed live move intent");
    let stale_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts before Sending phase");
    let record = wallet_core::MoveRecord {
        key: key.clone(),
        from: Some(fed(1)),
        to: fed(2),
        amount: Msat(80),
        fee_cap: Msat(0),
        gateway: crate::GatewayUrl("https://gateway.invalid".to_owned()),
        send_required: true,
        invoice: Some(Invoice("invoice-sending-generation".to_owned())),
        recv_op: Some(OperationId([0x72; 32])),
        send_op: Some(OperationId([0x73; 32])),
        phase: wallet_core::MovePhase::Sending,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    };
    assert!(!client
        .put_move_if_attempt(key.clone(), 1, record.clone())
        .await
        .expect("stale MoveRecord attempt is a clean false"));
    let after_false = client
        .issue_balance_facts_token()
        .await
        .expect("facts after false MoveRecord fence");
    assert_eq!(
        stale_facts.generations, after_false.generations,
        "a false attempt fence must not advance any balance generation"
    );
    assert!(client
        .put_move_if_attempt(key, 0, record)
        .await
        .expect("actor-routed Sending record"));

    let occurrence = Occurrence(972);
    let source_touched = one_msat_funding("move:after-sending", fed(1), fed(5), occurrence);
    let destination_touched = one_msat_funding("move:to-after-sending", fed(2), fed(5), occurrence);
    let independent = one_msat_funding("move:independent-sending", fed(3), fed(4), occurrence);
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![
                    source_touched.clone(),
                    destination_touched.clone(),
                    independent.clone(),
                ],
                0,
                client.issue_tick_plan_token().await.expect("plan token"),
            ),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
                (fed(5), Msat(0)),
            ]),
            stale_facts,
        )
        .await
        .expect("Sending staleness is scoped per decision");
    assert_eq!(report.refused.len(), 2, "{report:#?}");
    for decision in [&source_touched, &destination_touched] {
        assert!(
            report
                .refused
                .iter()
                .any(|refusal| refusal.key == decision.idempotency_key
                    && refusal.message.contains("balance facts changed")),
            "{} must be refused against the stale endpoint facts: {report:#?}",
            decision.idempotency_key.0
        );
    }
    assert_eq!(report.accepted, vec![independent.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_before_pay_artifact_keeps_the_strict_reservation() {
    let (service, _journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let pay_key = IdempotencyKey("pay:commit-before-artifact".to_owned());
    client
        .decide_op(pay(&pay_key.0, fed(1), 80, 0, 74))
        .await
        .expect("seed strict raw-pay reservation");
    let occurrence = Occurrence(973);
    let decision = one_msat_funding("move:commit-before-artifact", fed(1), fed(2), occurrence);
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![decision.clone()],
                0,
                client.issue_tick_plan_token().await.expect("plan token"),
            ),
            BTreeMap::from([(fed(1), Msat(1)), (fed(2), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("pre-artifact balance facts"),
        )
        .await
        .expect("strict pre-artifact view is a scoped refusal");
    assert!(report.accepted.is_empty());
    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(
        report.refused[0].reason,
        RefuseReason::InsufficientAfterReservations
    );
    assert!(client
        .set_operation_artifact_if_attempt(pay_key, 0, OperationId([0x74; 32]), None)
        .await
        .expect("artifact after commit"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ambiguous_terminal_transition_error_stales_only_the_known_action() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let key = IdempotencyKey("pay:ambiguous-terminal".to_owned());
    client
        .decide_op(pay(&key.0, fed(1), 10, 0, 76))
        .await
        .expect("seed live Pay");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while executor.calls.load(Ordering::SeqCst) != 1 {
            executor.started.notified().await;
        }
    })
    .await
    .expect("driver reached Executing");
    let stale_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts before ambiguous terminal write");
    let plan = client
        .issue_tick_plan_token()
        .await
        .expect("plan before ambiguous terminal write");
    journal.fail_after_next_status_write_for_test();
    client
        .journal_transition(
            key.clone(),
            JournalTransition::SetStatus {
                expected_attempt: 0,
                status: IntentStatus::Failed,
                error: Some("injected terminal".to_owned()),
            },
        )
        .await
        .expect_err("journal reports its injected post-commit error");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read committed terminal")
            .expect("Pay intent")
            .status,
        IntentStatus::Failed,
        "the error is durability-ambiguous because the terminal write committed"
    );
    let touched = one_msat_funding(
        "move:touched-after-ambiguous-terminal",
        fed(1),
        fed(2),
        Occurrence(977),
    );
    let independent = one_msat_funding(
        "move:independent-after-ambiguous-terminal",
        fed(3),
        fed(4),
        Occurrence(977),
    );
    let report = client
        .commit_tick(
            TickRound::for_test(vec![touched.clone(), independent.clone()], 0, plan),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
            ]),
            stale_facts,
        )
        .await
        .expect("known-action ambiguity is a scoped refusal");
    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, touched.idempotency_key);
    assert!(report.refused[0].message.contains("balance facts changed"));
    assert_eq!(report.accepted, vec![independent.idempotency_key]);

    let fresh = one_msat_funding(
        "move:fresh-after-ambiguous-terminal",
        fed(1),
        fed(5),
        Occurrence(980),
    );
    let fresh_report = client
        .commit_tick(
            TickRound::for_test(
                vec![fresh.clone()],
                0,
                client
                    .issue_tick_plan_token()
                    .await
                    .expect("fresh plan token"),
            ),
            BTreeMap::from([(fed(1), Msat(1)), (fed(5), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("fresh facts are issuable after scoped ambiguity"),
        )
        .await
        .expect("fresh facts remain usable after scoped ambiguity");
    assert_eq!(fresh_report.accepted, vec![fresh.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn orphaned_active_probe_reconcile_write_ambiguity_stales_only_its_action() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let key = IdempotencyKey("move:orphaned-active-probe-ambiguity".to_owned());
    let mut decision = one_msat_funding(&key.0, fed(1), fed(2), Occurrence(978));
    decision.reason = ReasonCode::ActiveProbe;
    journal
        .upsert(&Intent::from_decision(
            &decision,
            Actor::Agent {
                occurrence: Occurrence(978),
            },
            1,
        ))
        .await
        .expect("seed orphaned active-probe money intent");
    let stale_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts before ambiguous orphan terminalization");
    let plan = client
        .issue_tick_plan_token()
        .await
        .expect("plan before ambiguous orphan terminalization");
    journal.fail_after_next_status_write_for_test();
    client
        .reconcile_durable()
        .await
        .expect_err("post-commit orphan terminalization error reaches recovery caller");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read committed orphan terminal")
            .expect("orphaned active-probe intent")
            .status,
        IntentStatus::Failed,
        "the reported recovery error is durability-ambiguous because the terminal write committed"
    );
    let touched = one_msat_funding(
        "move:touched-after-ambiguous-orphan-terminalization",
        fed(1),
        fed(2),
        Occurrence(979),
    );
    let independent = one_msat_funding(
        "move:independent-after-ambiguous-orphan-terminalization",
        fed(3),
        fed(4),
        Occurrence(979),
    );
    let report = client
        .commit_tick(
            TickRound::for_test(vec![touched.clone(), independent.clone()], 0, plan),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
            ]),
            stale_facts,
        )
        .await
        .expect("known orphan action ambiguity is a scoped refusal");
    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, touched.idempotency_key);
    assert!(report.refused[0].message.contains("balance facts changed"));
    assert_eq!(report.accepted, vec![independent.idempotency_key]);

    let fresh = one_msat_funding(
        "move:fresh-after-ambiguous-orphan-terminalization",
        fed(1),
        fed(5),
        Occurrence(981),
    );
    let fresh_report = client
        .commit_tick(
            TickRound::for_test(
                vec![fresh.clone()],
                0,
                client
                    .issue_tick_plan_token()
                    .await
                    .expect("fresh plan token"),
            ),
            BTreeMap::from([(fed(1), Msat(1)), (fed(5), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("fresh facts are issuable after orphan ambiguity"),
        )
        .await
        .expect("fresh facts remain usable after orphan ambiguity");
    assert_eq!(fresh_report.accepted, vec![fresh.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ambiguous_artifact_write_error_stales_only_its_known_action() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let key = IdempotencyKey("pay:ambiguous-artifact".to_owned());
    client
        .decide_op(pay(&key.0, fed(1), 80, 0, 82))
        .await
        .expect("seed live Pay");
    let stale_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts before ambiguous artifact write");
    let plan = client
        .issue_tick_plan_token()
        .await
        .expect("plan before ambiguous artifact write");
    journal.fail_after_next_artifact_write_for_test();
    client
        .set_operation_artifact_if_attempt(key.clone(), 0, OperationId([0x82; 32]), None)
        .await
        .expect_err("artifact writer reports its injected post-commit error");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read committed artifact")
            .expect("Pay intent")
            .operation_id,
        Some(OperationId([0x82; 32]))
    );

    let touched = one_msat_funding(
        "move:touched-after-ambiguous-artifact",
        fed(1),
        fed(2),
        Occurrence(982),
    );
    let independent = one_msat_funding(
        "move:independent-after-ambiguous-artifact",
        fed(3),
        fed(4),
        Occurrence(982),
    );
    let report = client
        .commit_tick(
            TickRound::for_test(vec![touched.clone(), independent.clone()], 0, plan),
            BTreeMap::from([
                (fed(1), Msat(100)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
            ]),
            stale_facts,
        )
        .await
        .expect("known artifact ambiguity is a scoped refusal");
    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, touched.idempotency_key);
    assert!(report.refused[0].message.contains("balance facts changed"));
    assert_eq!(report.accepted, vec![independent.idempotency_key]);

    let fresh = one_msat_funding(
        "move:fresh-after-ambiguous-artifact",
        fed(1),
        fed(5),
        Occurrence(983),
    );
    let fresh_report = client
        .commit_tick(
            TickRound::for_test(
                vec![fresh.clone()],
                0,
                client
                    .issue_tick_plan_token()
                    .await
                    .expect("fresh plan token"),
            ),
            BTreeMap::from([(fed(1), Msat(100)), (fed(5), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("fresh facts are issuable after artifact ambiguity"),
        )
        .await
        .expect("fresh facts remain usable after artifact ambiguity");
    assert_eq!(fresh_report.accepted, vec![fresh.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ambiguous_move_record_write_error_stales_only_its_known_action() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let key = IdempotencyKey("move:ambiguous-record".to_owned());
    let intent = Intent {
        idempotency_key: key.clone(),
        attempt: 0,
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(80),
            fee_cap: Msat(0),
            gateway: None,
        },
        max_fee: Some(Msat(0)),
        status: IntentStatus::Executing,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: 1,
        operation_id: None,
        invoice: None,
    };
    journal.upsert(&intent).await.expect("seed live Move");
    let stale_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts before ambiguous MoveRecord write");
    let plan = client
        .issue_tick_plan_token()
        .await
        .expect("plan before ambiguous MoveRecord write");
    let record = wallet_core::MoveRecord {
        key: key.clone(),
        from: Some(fed(1)),
        to: fed(2),
        amount: Msat(80),
        fee_cap: Msat(0),
        gateway: crate::GatewayUrl("https://gateway.invalid".to_owned()),
        send_required: true,
        invoice: Some(Invoice("invoice-ambiguous-record".to_owned())),
        recv_op: Some(OperationId([0x83; 32])),
        send_op: Some(OperationId([0x84; 32])),
        phase: wallet_core::MovePhase::Sending,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    };
    journal.fail_after_next_move_write_for_test();
    client
        .put_move_if_attempt(key.clone(), 0, record)
        .await
        .expect_err("MoveRecord writer reports its injected post-commit error");
    assert!(
        journal
            .get_move(&key)
            .await
            .expect("read committed MoveRecord")
            .is_some(),
        "the reported writer error is durability-ambiguous because its record committed"
    );

    let source_touched = one_msat_funding(
        "move:source-touched-after-ambiguous-record",
        fed(1),
        fed(5),
        Occurrence(984),
    );
    let destination_touched = one_msat_funding(
        "move:destination-touched-after-ambiguous-record",
        fed(2),
        fed(5),
        Occurrence(984),
    );
    let independent = one_msat_funding(
        "move:independent-after-ambiguous-record",
        fed(3),
        fed(4),
        Occurrence(984),
    );
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![
                    source_touched.clone(),
                    destination_touched.clone(),
                    independent.clone(),
                ],
                0,
                plan,
            ),
            BTreeMap::from([
                (fed(1), Msat(100)),
                (fed(2), Msat(100)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
                (fed(5), Msat(0)),
            ]),
            stale_facts,
        )
        .await
        .expect("known MoveRecord ambiguity is a scoped refusal");
    assert_eq!(report.refused.len(), 2, "{report:#?}");
    for touched in [&source_touched, &destination_touched] {
        assert!(
            report
                .refused
                .iter()
                .any(|refusal| refusal.key == touched.idempotency_key
                    && refusal.message.contains("balance facts changed")),
            "{} must be refused against stale endpoint facts: {report:#?}",
            touched.idempotency_key.0
        );
    }
    assert_eq!(report.accepted, vec![independent.idempotency_key]);

    let fresh = one_msat_funding(
        "move:fresh-after-ambiguous-record",
        fed(1),
        fed(6),
        Occurrence(985),
    );
    let fresh_report = client
        .commit_tick(
            TickRound::for_test(
                vec![fresh.clone()],
                0,
                client
                    .issue_tick_plan_token()
                    .await
                    .expect("fresh plan token"),
            ),
            BTreeMap::from([(fed(1), Msat(100)), (fed(6), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("fresh facts are issuable after MoveRecord ambiguity"),
        )
        .await
        .expect("fresh facts remain usable after MoveRecord ambiguity");
    assert_eq!(fresh_report.accepted, vec![fresh.idempotency_key]);
    service.shutdown().await.expect("shutdown");
}

#[test]
fn balance_federation_scope_matches_each_action_direction() {
    fn expected(
        federations: impl IntoIterator<Item = FederationId>,
    ) -> std::collections::BTreeSet<FederationId> {
        federations.into_iter().collect()
    }
    let move_or_evac = |action| {
        assert_eq!(
            super::actor::balance_federations(&action),
            expected([fed(1), fed(2)])
        );
    };
    move_or_evac(Action::Move {
        from: fed(1),
        to: fed(2),
        amount: Msat(1),
        fee_cap: Msat(0),
        gateway: None,
    });
    move_or_evac(Action::Evacuate {
        from: fed(1),
        to: fed(2),
        amount: Msat(1),
        fee_cap: Msat(0),
        gateway: None,
        fee_cap_components: None,
    });
    assert_eq!(
        super::actor::balance_federations(&pay("scope-pay", fed(1), 1, 0, 1).decision.action),
        expected([fed(1)])
    );
    assert_eq!(
        super::actor::balance_federations(&Action::Receive {
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
            nonce: "scope-receive".to_owned(),
            gateway: None,
        }),
        expected([fed(2)])
    );
    assert_eq!(
        super::actor::balance_federations(&Action::DirectInflow {
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
        }),
        expected([fed(2)])
    );
    for action in [
        Action::Join {
            federation: fed(1),
            invite: "invite".to_owned(),
            membership_preexisting: false,
        },
        Action::RefuseInflow {
            fed: fed(1),
            reason: ReasonCode::Unhealthy,
            diagnostics: RefusalDiagnostics::default(),
        },
    ] {
        assert!(
            super::actor::balance_federations(&action).is_empty(),
            "{action:?} must not acquire a raw-terminal balance lease"
        );
    }
}

#[tokio::test]
async fn repair_terminal_sink_routes_raw_status_through_actor_and_stales_balance_facts() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    client
        .decide_op(pay("pay:repair-sink", fed(1), 10, 0, 12))
        .await
        .expect("raw pay admitted");
    let plan = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts before repair terminal status");
    let row = journal
        .operation(&crate::OperationRef::Key(IdempotencyKey(
            "pay:repair-sink".to_owned(),
        )))
        .await
        .expect("ledger row read")
        .expect("ledger row exists");
    let operation_id = OperationId([7; 32]);
    assert!(journal
        .record_raw_observation_if_attempt(
            &IdempotencyKey("pay:repair-sink".to_owned()),
            0,
            operation_id,
            &RawOpObservation {
                terminal: Some(RawTerminal {
                    succeeded: true,
                    error: None,
                }),
                gateway: None,
                fees: Default::default(),
                invoice_amount: None,
                payment_hash: None,
            },
        )
        .await
        .expect("seed fenced terminal ledger row"));
    let fence = crate::journal::RawIntentTerminalFence::new(
        row.seq,
        0,
        fed(1),
        Some(operation_id),
        crate::journal::RawOperationRole::Send,
        OperationStatus::Succeeded,
    );
    let sink = ActorRawIntentTerminalSink { client: &client };
    RawIntentTerminalSink::set_raw_terminal(
        &sink,
        &IdempotencyKey("pay:repair-sink".to_owned()),
        &fence,
        IntentStatus::Done,
        None,
    )
    .await
    .expect("repair terminal status is actor-routed");
    assert_eq!(
        journal
            .get(&IdempotencyKey("pay:repair-sink".to_owned()))
            .await
            .expect("intent")
            .expect("present")
            .status,
        IntentStatus::Done
    );
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(92),
        idempotency_key: IdempotencyKey("move:repair-sink-stale-facts".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![decision], 0, plan),
            BTreeMap::from([(fed(1), Msat(100)), (fed(2), Msat(0))]),
            facts,
        )
        .await
        .expect("stale fact is a scoped refusal");
    assert_eq!(report.refused.len(), 1);
    assert_eq!(report.refused[0].reason, RefuseReason::Conflict);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn live_driver_cannot_regress_a_repair_terminal_for_its_attempt() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let key = IdempotencyKey("pay:driver-repair-fence".to_owned());
    let started = executor.started.notified();
    client
        .decide_op(pay(&key.0, fed(1), 10, 0, 13))
        .await
        .expect("start live raw driver");
    started.await;
    let row = journal
        .operation(&crate::OperationRef::Key(key.clone()))
        .await
        .expect("ledger row read")
        .expect("ledger row exists");
    let operation_id = OperationId([7; 32]);
    assert!(journal
        .record_raw_observation_if_attempt(
            &key,
            0,
            operation_id,
            &RawOpObservation {
                terminal: Some(RawTerminal {
                    succeeded: true,
                    error: None,
                }),
                gateway: None,
                fees: Default::default(),
                invoice_amount: None,
                payment_hash: None,
            },
        )
        .await
        .expect("seed fenced terminal ledger row"));
    let fence = crate::journal::RawIntentTerminalFence::new(
        row.seq,
        0,
        fed(1),
        Some(operation_id),
        crate::journal::RawOperationRole::Send,
        OperationStatus::Succeeded,
    );

    let sink = ActorRawIntentTerminalSink { client: &client };
    assert!(
        RawIntentTerminalSink::set_raw_terminal(&sink, &key, &fence, IntentStatus::Done, None,)
            .await
            .expect("repair terminal transition"),
        "repair must terminalize the live attempt"
    );
    let stale = client
        .journal_transition(
            key.clone(),
            JournalTransition::SetStatus {
                expected_attempt: 0,
                status: IntentStatus::Awaiting,
                error: None,
            },
        )
        .await
        .expect("late driver transition benignly loses");
    assert_eq!(stale, TransitionResult::Compared(false));
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read intent")
            .expect("intent exists")
            .status,
        IntentStatus::Done,
        "a live driver's stale Awaiting write cannot revive repair's terminal state"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_tick_rechecks_fresh_balances_after_a_user_op_settles() {
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    let occurrence = Occurrence(37);
    client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), healthy_probe(100)), (fed(2), healthy_probe(0))],
            occurrence,
            now_ms: 105,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("seed balance facts before route validation");
    client
        .decide_op(pay("pay:settled-mid-validation", fed(1), 80, 0, 82))
        .await
        .expect("user pay starts during route validation");
    wait_for_registry(&client, 0).await;
    assert_eq!(
        journal
            .get(&IdempotencyKey("pay:settled-mid-validation".to_owned()))
            .await
            .expect("read settled pay")
            .expect("pay intent")
            .status,
        IntentStatus::Done
    );

    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(30),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:dropped-after-settlement".to_owned()),
    };
    let report = client
        .commit_tick_with_facts_legacy(
            vec![decision],
            Some(BTreeMap::from([(fed(1), Msat(20)), (fed(2), Msat(0))])),
            None,
            0,
            client.issue_tick_plan_token().await.expect("token"),
        )
        .await
        .expect("fresh balance refusal is a successful commit recheck");
    assert!(report.accepted.is_empty());
    assert_eq!(report.refused.len(), 1);
    assert_eq!(
        report.refused[0].reason,
        RefuseReason::InsufficientAfterReservations
    );
    service.shutdown().await.expect("shutdown");
}

/// A balance-facts token describes one point in actor history, not a caller's
/// claim about a map.  A terminal user transition after that point invalidates
/// only Agent decisions touching the changed federation.
#[tokio::test]
async fn commit_tick_refuses_only_decisions_touched_after_balance_facts_sample() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor).await;
    let client = service.client();
    let occurrence = Occurrence(67);
    let plan_token = client.issue_tick_plan_token().await.expect("plan token");
    let user_key = IdempotencyKey("user:terminal-inflow-b-after-sample".to_owned());
    client
        .decide_op(move_request(
            &user_key.0,
            Action::DirectInflow {
                to: fed(2),
                amount: Msat(1),
                fee_cap: Msat(0),
            },
            BTreeMap::from([(fed(2), Msat(10))]),
            None,
        ))
        .await
        .expect("user inflow admitted");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("sample immediately after user admission");
    client
        .journal_transition(
            user_key,
            JournalTransition::SetStatus {
                expected_attempt: 0,
                status: IntentStatus::Done,
                error: None,
            },
        )
        .await
        .expect("the user operation terminalizes after the sample");

    let touched = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(2),
            to: fed(3),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("agent:stale-evacuate-b".to_owned()),
    };
    let independent = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(4),
            to: fed(5),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("agent:fresh-evacuate-d".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![touched.clone(), independent.clone()], 0, plan_token),
            // Deliberately stale: this map predates the terminal user transition.
            BTreeMap::from([
                (fed(2), Msat(10)),
                (fed(3), Msat(0)),
                (fed(4), Msat(10)),
                (fed(5), Msat(0)),
            ]),
            facts,
        )
        .await
        .expect("a stale decision is scoped rather than aborting the batch");

    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, touched.idempotency_key);
    assert!(report.refused[0].message.contains("balance facts changed"));
    assert_eq!(report.accepted, vec![independent.idempotency_key.clone()]);
    assert!(journal
        .get(&touched.idempotency_key)
        .await
        .unwrap()
        .is_none());
    assert!(
        journal
            .operation(&crate::journal::OperationRef::Key(IdempotencyKey(format!(
                "tick-drop:{}:{}",
                occurrence.0, touched.idempotency_key.0
            ))))
            .await
            .expect("tick-drop lookup")
            .is_some(),
        "a per-decision stale-facts refusal must remain durable audit evidence"
    );
    assert!(journal
        .get(&independent.idempotency_key)
        .await
        .unwrap()
        .is_some());
    service.shutdown().await.expect("shutdown");
}

/// A user retry creates fresh strict reservations before its replacement driver starts.  Facts
/// sampled before that durable retry must therefore be stale only for decisions touching its
/// balance federation.
#[tokio::test]
async fn failed_user_retry_invalidates_only_touched_old_balance_facts() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(68);
    let retry = move_request(
        "user:retry-inflow-b-after-sample",
        Action::DirectInflow {
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
        },
        BTreeMap::from([(fed(2), Msat(10))]),
        None,
    );
    client
        .decide_op(retry.clone())
        .await
        .expect("start user inflow whose driver will remain blocked");
    while executor.calls.load(Ordering::SeqCst) != 1 {
        tokio::task::yield_now().await;
    }
    client
        .journal_transition(
            retry.decision.idempotency_key.clone(),
            JournalTransition::SetStatus {
                expected_attempt: 0,
                status: IntentStatus::Failed,
                error: Some("operator retry".to_owned()),
            },
        )
        .await
        .expect("seed failed user inflow");
    let plan_token = client
        .issue_tick_plan_token()
        .await
        .expect("old plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("old balance facts");

    client
        .decide_op(retry)
        .await
        .expect("durable retry admits replacement reservations");
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        1,
        "the old blocked wrapper leaves the replacement pending before it can mutate facts"
    );

    let touched = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(2),
            to: fed(3),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("agent:stale-after-user-retry-b".to_owned()),
    };
    let independent = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(4),
            to: fed(5),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("agent:fresh-after-user-retry-d".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![touched.clone(), independent.clone()], 0, plan_token),
            BTreeMap::from([
                (fed(2), Msat(10)),
                (fed(3), Msat(0)),
                (fed(4), Msat(10)),
                (fed(5), Msat(0)),
            ]),
            facts,
        )
        .await
        .expect("stale balance facts refuse only their touched decision");

    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, touched.idempotency_key);
    assert_eq!(report.refused[0].reason, RefuseReason::Conflict);
    assert!(report.refused[0].message.contains("balance facts changed"));
    assert_eq!(report.accepted, vec![independent.idempotency_key.clone()]);
    assert!(
        journal
            .get(&touched.idempotency_key)
            .await
            .expect("read touched decision")
            .is_none(),
        "the stale touched decision must not be admitted"
    );
    assert!(
        journal
            .get(&independent.idempotency_key)
            .await
            .expect("read independent decision")
            .is_some(),
        "an independent decision remains admissible"
    );
    service.shutdown().await.expect("shutdown");
}

/// An Agent admission remains a conflict for an older off-actor plan even
/// after its driver has reached Done and disappeared from the pending scan.
#[tokio::test]
async fn commit_tick_watermark_refuses_terminal_same_goal_without_a_second_driver() {
    let executor = Arc::new(CountingExitExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(60);
    let snapshot = client.issue_tick_plan_token().await.expect("old token");
    client
        .decide_op(agent_request(
            "agent:new-fund-into-b",
            Action::Move {
                from: fed(1),
                to: fed(2),
                amount: Msat(100),
                fee_cap: Msat(0),
                gateway: None,
            },
            ReasonCode::StandbyBelowTarget,
            Occurrence(61),
            BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))]),
        ))
        .await
        .expect("new Agent funding is durable");
    wait_for_registry(&client, 0).await;
    assert_eq!(
        journal
            .get(&IdempotencyKey("agent:new-fund-into-b".to_owned()))
            .await
            .expect("read new funding")
            .expect("durable funding")
            .status,
        IntentStatus::Done
    );
    let old = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(100),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("agent:old-fund-into-b".to_owned()),
    };
    let report = client
        .commit_tick_with_facts_legacy(
            vec![old.clone()],
            Some(BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))])),
            None,
            0,
            snapshot,
        )
        .await
        .expect("watermark refusal is a soft tick result");
    assert!(report.accepted.is_empty());
    assert_eq!(report.refused[0].reason, RefuseReason::Conflict);
    assert!(
        journal.get(&old.idempotency_key).await.unwrap().is_none(),
        "the old same-goal key is never admitted"
    );
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        1,
        "the stale plan must not start a second driver"
    );
    let fresh = client.issue_tick_plan_token().await.expect("newer token");
    let recurrence = AllocatorDecision {
        idempotency_key: IdempotencyKey("agent:post-terminal-fund-into-b".to_owned()),
        ..old
    };
    let permitted = client
        .commit_tick_with_facts_legacy(
            vec![recurrence],
            Some(BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))])),
            None,
            0,
            fresh,
        )
        .await
        .expect("a snapshot taken after terminal work permits a new recurrence");
    assert_eq!(permitted.accepted.len(), 1);
    wait_for_registry(&client, 0).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
    service.shutdown().await.expect("shutdown");
}

/// A direct fresh Agent request can receive a storage error after its intent committed.  The exact
/// reread must retain that admission through an attach, so a token minted before the error cannot
/// drive the same goal a second time.
#[tokio::test]
async fn direct_fresh_agent_upsert_error_advances_watermark_once_after_durable_reread() {
    let executor = Arc::new(CountingExitExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let old_plan = client.issue_tick_plan_token().await.expect("old token");
    let facts_before_error = client
        .issue_balance_facts_token()
        .await
        .expect("facts before ambiguous upsert");
    let action = Action::Move {
        from: fed(1),
        to: fed(2),
        amount: Msat(100),
        fee_cap: Msat(0),
        gateway: None,
    };
    let fresh = agent_request(
        "agent:ambiguous-direct-fresh",
        action.clone(),
        ReasonCode::StandbyBelowTarget,
        Occurrence(201),
        BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))]),
    );
    journal.fail_after_next_upsert_for_test();
    let error = client
        .decide_op(fresh.clone())
        .await
        .expect_err("post-upsert storage fault is returned");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::StorageError,
            ..
        }
    ));
    assert_eq!(
        journal
            .get(&fresh.decision.idempotency_key)
            .await
            .expect("read ambiguous intent")
            .expect("upsert committed before its error")
            .status,
        IntentStatus::Pending
    );
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        0,
        "the error path does not start a driver before an attach"
    );
    let facts_after_error = client
        .issue_balance_facts_token()
        .await
        .expect("facts after matching durable reread");
    assert_eq!(
        facts_before_error.generations,
        BTreeMap::new(),
        "the fixture has no prior balance mutations"
    );
    assert_eq!(
        facts_after_error.generations,
        BTreeMap::from([(fed(1), 1), (fed(2), 1)]),
        "the matching Move admission increments its source and destination exactly once"
    );
    let post_ambiguity_plan = client
        .issue_tick_plan_token()
        .await
        .expect("watermark remains issuable after a matching reread");
    assert_ne!(
        old_plan, post_ambiguity_plan,
        "the matching durable intent advances the admission counter"
    );

    journal
        .set_status(
            &fresh.decision.idempotency_key,
            0,
            IntentStatus::Done,
            Some("test terminal before attach"),
        )
        .await
        .expect("terminalize matching durable intent");
    let attached = client
        .decide_op(fresh)
        .await
        .expect("terminal durable intent attaches");
    assert!(attached.deduplicated);
    wait_for_registry(&client, 0).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        post_ambiguity_plan,
        client
            .issue_tick_plan_token()
            .await
            .expect("attach does not re-admit the intent"),
        "the ambiguous durable admission was counted exactly once"
    );
    assert_eq!(
        client
            .issue_balance_facts_token()
            .await
            .expect("terminal attach does not change balance facts")
            .generations,
        facts_after_error.generations,
        "attaching the already-durable intent does not add a second admission balance bump"
    );

    let old_same_goal = AllocatorDecision {
        action,
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(200),
        idempotency_key: IdempotencyKey("agent:old-direct-same-goal".to_owned()),
    };
    let fresh_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts after terminal attach");
    let report = client
        .commit_tick(
            TickRound::for_test(vec![old_same_goal.clone()], 0, old_plan),
            BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))]),
            fresh_facts,
        )
        .await
        .expect("watermark refusal is a scoped tick result");
    assert!(report.accepted.is_empty(), "{report:#?}");
    assert_eq!(report.refused[0].reason, RefuseReason::Conflict);
    assert!(journal
        .get(&old_same_goal.idempotency_key)
        .await
        .expect("read old same-goal key")
        .is_none());
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        0,
        "the terminal attach and old token must not start a driver"
    );
    service.shutdown().await.expect("shutdown");
}

/// CommitTick's allocator-reservation override shares the fresh core path.  Its post-upsert
/// storage refusal must advance the actor watermark too, not only the batch-local blocker.
#[tokio::test]
async fn commit_tick_fresh_agent_upsert_error_advances_watermark_after_attach() {
    let executor = Arc::new(CountingExitExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let old_plan = client.issue_tick_plan_token().await.expect("old token");
    let action = Action::Move {
        from: fed(1),
        to: fed(2),
        amount: Msat(100),
        fee_cap: Msat(0),
        gateway: None,
    };
    let first = AllocatorDecision {
        action: action.clone(),
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(211),
        idempotency_key: IdempotencyKey("agent:ambiguous-tick-fresh".to_owned()),
    };
    journal.fail_after_next_upsert_for_test();
    let report = client
        .commit_tick(
            TickRound::for_test(vec![first.clone()], 0, old_plan.clone()),
            BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("facts for ambiguous tick"),
        )
        .await
        .expect("storage refusal is scoped to the failed decision");
    assert!(
        report.refused.iter().any(|refusal| {
            refusal.key == first.idempotency_key && refusal.reason == RefuseReason::StorageError
        }),
        "{report:#?}"
    );
    assert_eq!(
        journal
            .get(&first.idempotency_key)
            .await
            .expect("read ambiguous tick intent")
            .expect("upsert committed before its error")
            .status,
        IntentStatus::Pending
    );
    let post_ambiguity_plan = client
        .issue_tick_plan_token()
        .await
        .expect("watermark remains issuable after a matching reread");
    assert_ne!(
        old_plan, post_ambiguity_plan,
        "the CommitTick core path advances the admission counter"
    );

    client
        .decide_op(agent_request(
            &first.idempotency_key.0,
            action.clone(),
            ReasonCode::StandbyBelowTarget,
            first.occurrence,
            BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))]),
        ))
        .await
        .expect("matching durable tick intent attaches");
    wait_for_registry(&client, 0).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        post_ambiguity_plan,
        client
            .issue_tick_plan_token()
            .await
            .expect("attach does not re-admit the tick intent"),
        "the ambiguous CommitTick admission was counted exactly once"
    );

    let old_same_goal = AllocatorDecision {
        action,
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(210),
        idempotency_key: IdempotencyKey("agent:old-tick-same-goal".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![old_same_goal.clone()], 0, old_plan),
            BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("facts after terminal attach"),
        )
        .await
        .expect("watermark refusal is a scoped tick result");
    assert!(report.accepted.is_empty(), "{report:#?}");
    assert_eq!(report.refused[0].reason, RefuseReason::Conflict);
    assert!(journal
        .get(&old_same_goal.idempotency_key)
        .await
        .expect("read old same-goal key")
        .is_none());
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.expect("shutdown");
}

/// The core helper can instead fail its pre-upsert reread.  The exact key remains absent, so this
/// must not fabricate a watermark that refuses an old otherwise-valid same-goal plan.
#[tokio::test]
async fn fresh_agent_pre_upsert_read_fault_with_absent_key_does_not_bump_watermark() {
    let executor = Arc::new(CountingExitExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let old_plan = client.issue_tick_plan_token().await.expect("old token");
    let facts_before_fault = client
        .issue_balance_facts_token()
        .await
        .expect("facts before definite pre-upsert fault");
    let action = Action::Move {
        from: fed(1),
        to: fed(2),
        amount: Msat(100),
        fee_cap: Msat(0),
        gateway: None,
    };
    let failed = agent_request(
        "agent:pre-upsert-read-fault",
        action.clone(),
        ReasonCode::StandbyBelowTarget,
        Occurrence(221),
        BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))]),
    );
    // The actor's fresh/existing dispatch consumes the first successful get.  Fault the core
    // helper's second get, before it can make or attempt an upsert; its ambiguity reread is absent.
    journal.fail_one_intent_read_after_successes_for_test(1);
    let error = client
        .decide_op(failed.clone())
        .await
        .expect_err("pre-upsert read fault is returned");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::StorageError,
            ..
        }
    ));
    assert!(journal
        .get(&failed.decision.idempotency_key)
        .await
        .expect("reread absent failed key")
        .is_none());
    let facts_after_fault = client
        .issue_balance_facts_token()
        .await
        .expect("facts after definite pre-upsert fault");
    assert_eq!(
        facts_after_fault.generations, facts_before_fault.generations,
        "an absent exact-key reread must not alter any balance generation"
    );

    let old_same_goal = AllocatorDecision {
        action,
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(220),
        idempotency_key: IdempotencyKey("agent:old-after-pre-upsert-read-fault".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(vec![old_same_goal.clone()], 0, old_plan),
            BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(0))]),
            facts_after_fault,
        )
        .await
        .expect("the old token can still admit after definite absence");
    assert_eq!(report.accepted, vec![old_same_goal.idempotency_key.clone()]);
    wait_for_registry(&client, 0).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.expect("shutdown");
}

/// A core read fault before upsert is a definite non-mutation even when CommitTick is using its
/// allocator-reservation override.  Same-goal and shared-source siblings must therefore remain
/// admissible rather than inheriting a phantom fold.
#[tokio::test]
async fn commit_tick_pre_upsert_read_fault_does_not_fold_goal_or_reservation() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(230);
    let first = one_msat_funding("move:pre-upsert-first", fed(1), fed(2), occurrence);
    let same_goal = one_msat_funding("move:pre-upsert-same-goal", fed(3), fed(2), occurrence);
    let shared_source =
        one_msat_funding("move:pre-upsert-shared-source", fed(1), fed(4), occurrence);
    // CommitTick's decision pre-read and shared helper fresh/existing read succeed first; fault
    // the core helper's own pre-upsert get.  Its exact-key ambiguity reread then observes None.
    journal.fail_one_intent_read_after_successes_for_test(2);
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![first.clone(), same_goal.clone(), shared_source.clone()],
                0,
                client.issue_tick_plan_token().await.expect("plan token"),
            ),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
            ]),
            client
                .issue_balance_facts_token()
                .await
                .expect("balance facts"),
        )
        .await
        .expect("definite storage refusal remains scoped");
    assert!(
        report.refused.iter().any(|refusal| {
            refusal.key == first.idempotency_key && refusal.reason == RefuseReason::StorageError
        }),
        "{report:#?}"
    );
    assert_eq!(
        report.accepted,
        vec![
            same_goal.idempotency_key.clone(),
            shared_source.idempotency_key.clone()
        ],
        "an absent pre-upsert key must not reserve the target or source"
    );
    assert!(journal
        .get(&first.idempotency_key)
        .await
        .expect("read failed key")
        .is_none());
    assert!(journal
        .get(&same_goal.idempotency_key)
        .await
        .expect("read same-goal key")
        .is_some());
    assert!(journal
        .get(&shared_source.idempotency_key)
        .await
        .expect("read shared-source key")
        .is_some());
    wait_for_registry(&client, 2).await;
    service.shutdown().await.expect("shutdown");
}

/// A readable row with the right key but a different Agent identity is not evidence for this
/// request.  The tick must fail immediately before it can consider a later decision which conflicts
/// only with that stored identity.
#[tokio::test]
async fn commit_tick_mismatched_ambiguous_upsert_row_fails_before_later_decisions() {
    let executor = Arc::new(CountingExitExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(231);
    let first = one_msat_funding("move:mismatched-upsert-first", fed(1), fed(2), occurrence);
    let stored = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(9),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: first.idempotency_key.clone(),
    };
    let later_conflicting_only_with_stored =
        one_msat_funding("move:mismatched-upsert-later", fed(3), fed(9), occurrence);
    journal.replace_after_next_upsert_for_test(Intent::from_decision(
        &stored,
        Actor::Agent { occurrence },
        0,
    ));
    journal.fail_after_next_upsert_for_test();
    let error = client
        .commit_tick(
            TickRound::for_test(
                vec![first.clone(), later_conflicting_only_with_stored.clone()],
                0,
                client.issue_tick_plan_token().await.expect("plan token"),
            ),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(9), Msat(0)),
            ]),
            client
                .issue_balance_facts_token()
                .await
                .expect("balance facts"),
        )
        .await
        .expect_err("mismatched durable identity fails the whole tick");
    assert!(matches!(
        error,
        ServiceError::Refused {
            reason: RefuseReason::StorageError,
            ..
        }
    ));
    assert!(
        client.issue_tick_plan_token().await.is_err(),
        "mismatched identity poisons future tick admission authority"
    );
    assert!(
        client.issue_balance_facts_token().await.is_err(),
        "mismatched identity poisons future balance-facts authority"
    );
    assert_eq!(
        journal
            .get(&first.idempotency_key)
            .await
            .expect("read mismatched durable row")
            .expect("replacement row")
            .action,
        stored.action
    );
    assert!(
        journal
            .get(&later_conflicting_only_with_stored.idempotency_key)
            .await
            .expect("read later key")
            .is_none(),
        "the tick stops before considering a later stored-goal conflict"
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(journal
        .history(usize::MAX, None)
        .await
        .expect("tick history")
        .iter()
        .any(|row| {
            matches!(
                row.kind,
                wallet_core::OperationKind::Tick {
                    occurrence: row_occurrence,
                    ..
                } if row_occurrence == occurrence
            ) && row.status == wallet_core::OperationStatus::Failed
        }));
    service.shutdown().await.expect("shutdown");
}

/// If the exact reread itself faults, the requested first Agent identity remains the conservative
/// holder: CommitTick folds its goal/source for siblings, advances its watermark, and invalidates
/// precisely the source/destination balance facts rather than unrelated ones.
#[tokio::test]
async fn commit_tick_ambiguous_reread_error_folds_known_goal_and_exact_balance_generations() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(232);
    let first = one_msat_funding("move:reread-error-first", fed(1), fed(2), occurrence);
    let same_goal = one_msat_funding("move:reread-error-same-goal", fed(3), fed(2), occurrence);
    let shared_source = one_msat_funding(
        "move:reread-error-shared-source",
        fed(1),
        fed(4),
        occurrence,
    );
    let independent = one_msat_funding("move:reread-error-independent", fed(5), fed(6), occurrence);
    let old_plan = client
        .issue_tick_plan_token()
        .await
        .expect("old plan token");
    let round_facts = client
        .issue_balance_facts_token()
        .await
        .expect("facts for failed round");
    let stale_facts = client
        .issue_balance_facts_token()
        .await
        .expect("same pre-admission facts retained for exactness check");
    assert_eq!(stale_facts.generations, BTreeMap::new());
    // Pre-read, fresh/existing dispatch, and core dedup get all succeed.  The post-upsert exact
    // reread is the fourth get and faults, leaving the requested identity conservatively known.
    journal.fail_one_intent_read_after_successes_for_test(3);
    journal.fail_after_next_upsert_for_test();
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![
                    first.clone(),
                    same_goal.clone(),
                    shared_source.clone(),
                    independent.clone(),
                ],
                0,
                old_plan.clone(),
            ),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(1)),
                (fed(4), Msat(0)),
                (fed(5), Msat(1)),
                (fed(6), Msat(0)),
                (fed(7), Msat(1)),
                (fed(8), Msat(0)),
            ]),
            round_facts,
        )
        .await
        .expect("known ambiguous mutation remains a scoped storage refusal");
    assert_eq!(report.accepted, vec![independent.idempotency_key.clone()]);
    assert!(report.refused.iter().any(|refusal| {
        refusal.key == first.idempotency_key && refusal.reason == RefuseReason::StorageError
    }));
    assert!(report
        .refused
        .iter()
        .any(|refusal| refusal.key == same_goal.idempotency_key));
    let post_ambiguity_plan = client
        .issue_tick_plan_token()
        .await
        .expect("known request advances a watermark");
    assert_ne!(old_plan, post_ambiguity_plan);
    assert_eq!(
        client
            .issue_balance_facts_token()
            .await
            .expect("known identities leave balance facts issuable")
            .generations,
        BTreeMap::from([(fed(1), 1), (fed(2), 1), (fed(5), 2), (fed(6), 2)]),
        "the ambiguous request changes exactly f1/f2; the independently accepted f5/f6 sibling has both admission and its started-driver transition"
    );

    let touches_source = one_msat_funding(
        "move:reread-error-touches-source",
        fed(1),
        fed(3),
        occurrence,
    );
    let touches_destination = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(2),
            to: fed(3),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("move:reread-error-touches-destination".to_owned()),
    };
    let unrelated = one_msat_funding("move:reread-error-unrelated", fed(7), fed(8), occurrence);
    let exactness = client
        .commit_tick(
            TickRound::for_test(
                vec![
                    touches_source.clone(),
                    touches_destination.clone(),
                    unrelated.clone(),
                ],
                0,
                post_ambiguity_plan,
            ),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(1)),
                (fed(3), Msat(0)),
                (fed(5), Msat(1)),
                (fed(6), Msat(0)),
                (fed(7), Msat(1)),
                (fed(8), Msat(0)),
            ]),
            stale_facts,
        )
        .await
        .expect("only actions touching the known request's balance generations are stale");
    assert_eq!(exactness.accepted, vec![unrelated.idempotency_key.clone()]);
    assert!(exactness
        .refused
        .iter()
        .any(|refusal| refusal.key == touches_source.idempotency_key));
    assert!(exactness
        .refused
        .iter()
        .any(|refusal| refusal.key == touches_destination.idempotency_key));
    wait_for_registry(&client, 2).await;
    service.shutdown().await.expect("shutdown");
}

/// The watermark is scoped by the exact asymmetric conflict relation: an
/// intervening Evacuate(A) blocks an old FundInto touching A, but leaves an
/// independent evacuation in the same old batch executable.
#[tokio::test]
async fn commit_tick_watermark_keeps_independent_evacuation_while_refusing_evacuation_edge() {
    let executor = Arc::new(ExitExecutor(Exit::Ok));
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(62);
    let snapshot = client.issue_tick_plan_token().await.expect("old token");
    client
        .decide_op(agent_request(
            "agent:new-evacuate-a",
            Action::Evacuate {
                from: fed(1),
                to: fed(2),
                amount: Msat(50),
                fee_cap: Msat(0),
                gateway: None,
                fee_cap_components: None,
            },
            ReasonCode::ShutdownNotice,
            Occurrence(63),
            BTreeMap::from([(fed(1), Msat(50)), (fed(2), Msat(0)), (fed(3), Msat(50))]),
        ))
        .await
        .expect("new Agent evacuation is durable");
    wait_for_registry(&client, 0).await;
    let stale_funding = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(100),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("agent:old-fund-touching-a".to_owned()),
    };
    let stale_same_evacuation = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(1),
            to: fed(2),
            amount: Msat(50),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("agent:old-evacuate-a".to_owned()),
    };
    let independent = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(3),
            to: fed(2),
            amount: Msat(50),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("agent:old-evacuate-c".to_owned()),
    };
    let report = client
        .commit_tick_with_facts_legacy(
            vec![
                stale_funding.clone(),
                stale_same_evacuation.clone(),
                independent.clone(),
            ],
            Some(BTreeMap::from([
                (fed(1), Msat(200)),
                (fed(2), Msat(0)),
                (fed(3), Msat(50)),
            ])),
            None,
            0,
            snapshot,
        )
        .await
        .expect("independent old work remains a soft commit");
    assert_eq!(report.refused.len(), 2);
    assert!(report
        .refused
        .iter()
        .any(|refusal| refusal.key == stale_funding.idempotency_key));
    assert!(
        report
            .refused
            .iter()
            .any(|refusal| refusal.key == stale_same_evacuation.idempotency_key),
        "a terminal intervening Evacuate(A) still invalidates its old recurrence"
    );
    assert_eq!(report.accepted, vec![independent.idempotency_key.clone()]);
    assert!(journal
        .get(&stale_funding.idempotency_key)
        .await
        .unwrap()
        .is_none());
    assert!(journal
        .get(&stale_same_evacuation.idempotency_key)
        .await
        .unwrap()
        .is_none());
    assert!(
        journal
            .get(&independent.idempotency_key)
            .await
            .unwrap()
            .is_some(),
        "the independent evacuation is admitted"
    );
    service.shutdown().await.expect("shutdown");
}

/// Fresh scheduler facts must not overfill a destination when a user inflow
/// settled during route validation while source affordability and cap room
/// still permit the old amount.
#[tokio::test]
async fn commit_tick_refuses_funding_that_exceeds_the_fresh_destination_shortfall() {
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    let occurrence = Occurrence(64);
    let snapshot = client.issue_tick_plan_token().await.expect("old token");
    client
        .decide_op(move_request(
            "user:direct-inflow-b",
            Action::DirectInflow {
                to: fed(2),
                amount: Msat(100),
                fee_cap: Msat(0),
            },
            BTreeMap::from([(fed(2), Msat(0))]),
            None,
        ))
        .await
        .expect("user direct inflow during planning");
    wait_for_registry(&client, 0).await;
    let old = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(100),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("agent:stale-target-funding".to_owned()),
    };
    let report = client
        .commit_tick_with_facts_legacy(
            vec![old.clone()],
            Some(BTreeMap::from([(fed(1), Msat(200)), (fed(2), Msat(100))])),
            None,
            0,
            snapshot,
        )
        .await
        .expect("fresh target check is a soft refusal");
    assert!(report.accepted.is_empty());
    assert_eq!(report.refused.len(), 1);
    assert!(
        report.refused[0].message.contains("fresh target shortfall"),
        "{report:#?}"
    );
    assert!(journal.get(&old.idempotency_key).await.unwrap().is_none());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn tick_row_waits_for_the_admitted_driver_outcome() {
    let executor = Arc::new(FailThenSlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(38);
    client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), healthy_probe(100)), (fed(2), healthy_probe(0))],
            occurrence,
            now_ms: 106,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("seed tick facts");
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:tick-outcome".to_owned()),
    };
    client
        .commit_tick_legacy(
            vec![decision],
            0,
            client.issue_tick_plan_token().await.expect("token"),
        )
        .await
        .expect("driver admitted");
    while executor.calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let started = journal
        .history(usize::MAX, None)
        .await
        .expect("history")
        .into_iter()
        .find(|row| {
            matches!(
                row.kind,
                wallet_core::OperationKind::Tick {
                    occurrence: row_occurrence,
                    ..
                } if row_occurrence == occurrence
            )
        })
        .expect("tick row opened");
    assert_eq!(started.status, wallet_core::OperationStatus::Started);
    assert!(matches!(
        started.kind,
        wallet_core::OperationKind::Tick {
            performed: 0,
            failed: 0,
            ..
        }
    ));

    executor.release_first.notify_waiters();
    wait_for_registry(&client, 0).await;
    let terminal = journal
        .history(usize::MAX, None)
        .await
        .expect("history")
        .into_iter()
        .find(|row| {
            matches!(
                row.kind,
                wallet_core::OperationKind::Tick {
                    occurrence: row_occurrence,
                    ..
                } if row_occurrence == occurrence
            )
        })
        .expect("tick row terminalized");
    assert_eq!(terminal.status, wallet_core::OperationStatus::Failed);
    assert!(matches!(
        terminal.kind,
        wallet_core::OperationKind::Tick {
            performed: 0,
            failed: 1,
            ..
        }
    ));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_tick_records_advisory_refusal_once_without_executable_admission() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let occurrence = Occurrence(35);
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), healthy_probe(1_200)), (fed(2), healthy_probe(100))],
            occurrence,
            now_ms: 103,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("plan over-cap advisory");
    let advisory = round
        .decisions
        .iter()
        .find(|decision| matches!(decision.action, Action::RefuseInflow { .. }))
        .cloned()
        .expect("allocator emits an over-cap refusal");

    let report = client
        .commit_tick_legacy(
            round.decisions,
            round.planned_generation,
            round.admission_snapshot.clone(),
        )
        .await
        .expect("advisory decision does not fail commit");
    assert!(report.accepted.is_empty());
    assert!(report.refused.is_empty());
    assert!(journal
        .operation(&crate::journal::OperationRef::Key(
            advisory.idempotency_key.clone()
        ))
        .await
        .expect("read advisory ledger row")
        .is_some());
    assert!(journal
        .operation(&crate::journal::OperationRef::Key(IdempotencyKey(format!(
            "tick-drop:{}:{}",
            occurrence.0, advisory.idempotency_key.0
        ))))
        .await
        .expect("read commit-drop ledger row")
        .is_none());
    assert!(journal
        .get(&advisory.idempotency_key)
        .await
        .expect("read advisory intent")
        .is_none());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_tick_spawns_every_fitting_decision_without_an_agent_lane() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, _) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(33);
    client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), healthy_probe(100)),
                (fed(2), healthy_probe(0)),
                (fed(3), healthy_probe(0)),
            ],
            occurrence,
            now_ms: 101,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("seed tick facts");
    let decisions = [fed(2), fed(3)]
        .into_iter()
        .enumerate()
        .map(|(index, to)| AllocatorDecision {
            action: Action::Move {
                from: fed(1),
                to,
                amount: Msat(10),
                fee_cap: Msat(0),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence,
            idempotency_key: IdempotencyKey(format!("move:concurrent-{index}")),
        })
        .collect::<Vec<_>>();
    let report = client
        .commit_tick_legacy(
            decisions,
            0,
            client.issue_tick_plan_token().await.expect("token"),
        )
        .await
        .expect("commit batch");
    assert_eq!(report.accepted.len(), 2);
    for _ in 0..100 {
        if executor.calls.load(Ordering::SeqCst) == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
    assert_eq!(registry_size(&client).await, 2);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_tick_continues_after_one_decision_hits_a_storage_fault() {
    let db = MemDatabase::new().into_database();
    let journal = Arc::new(FedimintJournal::new(db.clone()));
    let held = fed(1);
    let held_destination = fed(2);
    let session = ProbeSession {
        nonce: "00000000000000010000000000000000".to_owned(),
        from: held_destination,
        amount_msat: 20,
        leg_fee_cap_msat: 0,
        c_spendable_before_in_msat: 0,
        out_net_msat: None,
        started_at_ms: 1,
    };
    journal
        .begin_probe_session(&held, &session)
        .await
        .expect("seed held probe session");
    let in_key = crate::runtime::move_key(
        &held_destination,
        &held,
        Msat(session.amount_msat),
        Msat(session.leg_fee_cap_msat),
        crate::runtime::occurrence_from_nonce(&session.nonce).expect("valid nonce"),
    );
    let mut raw_key = vec![0x02];
    raw_key.extend_from_slice(in_key.0.as_bytes());
    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&raw_key, b"not valid json")
        .await
        .expect("insert corrupt probe move row");
    dbtx.commit_tx_result().await.expect("commit corrupt row");

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
    )
    .await
    .expect("start service");
    let client = service.client();
    let occurrence = Occurrence(36);
    client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (held, healthy_probe(100)),
                (held_destination, healthy_probe(0)),
                (fed(3), healthy_probe(100)),
                (fed(4), healthy_probe(0)),
            ],
            occurrence,
            now_ms: 104,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("seed tick facts");
    let first_key = IdempotencyKey("evacuate:faulted-preemption".to_owned());
    let same_goal_key = IdempotencyKey("evacuate:same-goal-after-fault".to_owned());
    let second_key = IdempotencyKey("evacuate:continues-after-fault".to_owned());
    let decisions = vec![
        AllocatorDecision {
            action: Action::Evacuate {
                from: held,
                to: held_destination,
                amount: Msat(10),
                fee_cap: Msat(0),
                gateway: None,
                fee_cap_components: None,
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence,
            idempotency_key: first_key.clone(),
        },
        // The first decision's storage error occurs after its intent upsert. This is a distinct
        // key for the same evacuation goal (the source identifies that goal), so it must be
        // withheld while the first decision's durability is unknown; the independent evacuation
        // after it must still be admitted.
        AllocatorDecision {
            action: Action::Evacuate {
                from: held,
                to: fed(4),
                amount: Msat(10),
                fee_cap: Msat(0),
                gateway: None,
                fee_cap_components: None,
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence,
            idempotency_key: same_goal_key.clone(),
        },
        AllocatorDecision {
            action: Action::Evacuate {
                from: fed(3),
                to: fed(4),
                amount: Msat(10),
                fee_cap: Msat(0),
                gateway: None,
                fee_cap_components: None,
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence,
            idempotency_key: second_key.clone(),
        },
    ];

    let error = client
        .commit_tick_legacy(
            decisions,
            0,
            client.issue_tick_plan_token().await.expect("token"),
        )
        .await
        .expect_err("the partial tick still reports its storage failure");
    assert!(error.to_string().contains("move record"));
    wait_for_registry(&client, 1).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        journal
            .get(&first_key)
            .await
            .expect("first intent")
            .expect("first intent was journaled")
            .status,
        IntentStatus::Pending
    );
    assert!(
        journal
            .get(&same_goal_key)
            .await
            .expect("same-goal intent")
            .is_none(),
        "the same logical goal is refused rather than driven under a second key"
    );
    assert!(journal
        .get(&second_key)
        .await
        .expect("second intent")
        .is_some());
    assert!(journal
        .history(usize::MAX, None)
        .await
        .expect("history")
        .iter()
        .any(|row| {
            matches!(
                row.kind,
                wallet_core::OperationKind::Tick {
                    occurrence: row_occurrence,
                    performed: 0,
                    failed: 0,
                    ..
                } if row_occurrence == occurrence
            ) && row.status == wallet_core::OperationStatus::Started
        }));
    service.shutdown().await.expect("shutdown");
}

/// A non-refusal error after fresh journaling leaves the mutation's durable outcome unknown.
/// `record_goal_admission` already suppresses a same-goal recurrence in this setup, so the
/// distinct-goal sibling below shares only the failed Move's source: the conservative strict
/// reservation fold is the sole admission check that can reject it. Independent work continues.
#[tokio::test]
async fn commit_tick_generic_post_admission_error_folds_strict_reservation() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(1_022);
    let first = one_msat_funding("move:generic-error-fresh", fed(1), fed(2), occurrence);
    let shared_source = one_msat_funding(
        "move:generic-error-shared-source",
        fed(1),
        fed(3),
        occurrence,
    );
    let independent =
        one_msat_funding("move:generic-error-independent", fed(4), fed(5), occurrence);
    let plan = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("balance facts");
    client
        .fail_after_fresh_admission_for_test(first.idempotency_key.clone())
        .await;

    let error = client
        .commit_tick(
            TickRound::for_test(
                vec![first.clone(), shared_source.clone(), independent.clone()],
                0,
                plan,
            ),
            BTreeMap::from([
                (fed(1), Msat(1)),
                (fed(2), Msat(0)),
                (fed(3), Msat(0)),
                (fed(4), Msat(1)),
                (fed(5), Msat(0)),
            ]),
            facts,
        )
        .await
        .expect_err("the injected generic error remains the tick result");
    assert!(
        error
            .to_string()
            .contains("injected post-fresh-admission failure"),
        "{error:#?}"
    );
    assert_eq!(
        journal
            .get(&first.idempotency_key)
            .await
            .expect("read uncertain first decision")
            .expect("the fresh upsert committed")
            .status,
        IntentStatus::Pending
    );
    assert!(
        journal
            .get(&shared_source.idempotency_key)
            .await
            .expect("read shared-source decision")
            .is_none(),
        "only the folded strict reservation can reject this distinct funding goal"
    );
    assert!(journal
        .get(&independent.idempotency_key)
        .await
        .expect("read independent decision")
        .is_some());
    wait_for_registry(&client, 1).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.expect("shutdown");
}

/// A journal `upsert` can commit before its caller observes a storage error.  CommitTick must
/// retain both views of that uncertain fresh admission: blockers prevent another source funding
/// the same target, and reservations prevent another decision spending its source.  Neither may
/// stop an unrelated decision in the same batch.
#[tokio::test]
async fn commit_tick_storage_refusal_after_fresh_upsert_folds_goal_and_reservation() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(1_023);
    let first = one_msat_funding("move:storage-refusal-fresh", fed(1), fed(2), occurrence);
    let same_goal = one_msat_funding("move:storage-refusal-same-goal", fed(3), fed(2), occurrence);
    let shared_source = one_msat_funding(
        "move:storage-refusal-shared-source",
        fed(1),
        fed(4),
        occurrence,
    );
    let independent = one_msat_funding(
        "move:storage-refusal-independent",
        fed(5),
        fed(6),
        occurrence,
    );
    let decisions = vec![
        first.clone(),
        same_goal.clone(),
        shared_source.clone(),
        independent.clone(),
    ];
    let balances = BTreeMap::from([
        (fed(1), Msat(1)),
        (fed(2), Msat(0)),
        (fed(3), Msat(1)),
        (fed(4), Msat(0)),
        (fed(5), Msat(1)),
        (fed(6), Msat(0)),
    ]);
    let plan = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("balance facts");
    journal.fail_after_next_upsert_for_test();

    let report = client
        .commit_tick(TickRound::for_test(decisions, 0, plan), balances, facts)
        .await
        .expect("storage refusal is scoped to the uncertain decision");

    assert_eq!(report.accepted, vec![independent.idempotency_key.clone()]);
    assert!(
        report.refused.iter().any(|refusal| {
            refusal.key == first.idempotency_key && refusal.reason == RefuseReason::StorageError
        }),
        "{report:#?}"
    );
    assert!(
        report
            .refused
            .iter()
            .any(|refusal| refusal.key == same_goal.idempotency_key),
        "the fresh upsert's allocator goal remains blocked"
    );
    assert!(
        report.refused.iter().any(|refusal| {
            refusal.key == shared_source.idempotency_key
                && refusal.reason == RefuseReason::InsufficientAfterReservations
        }),
        "the fresh upsert's source remains strictly reserved"
    );
    assert_eq!(
        journal
            .get(&first.idempotency_key)
            .await
            .expect("read first")
            .expect("first upsert committed")
            .status,
        IntentStatus::Pending
    );
    assert!(journal
        .get(&same_goal.idempotency_key)
        .await
        .expect("read same-goal")
        .is_none());
    assert!(journal
        .get(&shared_source.idempotency_key)
        .await
        .expect("read shared-source")
        .is_none());
    assert!(journal
        .get(&independent.idempotency_key)
        .await
        .expect("read independent")
        .is_some());
    wait_for_registry(&client, 1).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.expect("shutdown");
}

/// The same typed storage refusal is not evidence of a mutation when CommitTick's pre-read found
/// an existing key.  A live user-owned row has no allocator goal; the later agent decisions
/// expose phantom goal and strict-reservation folds of this replay as refusals.
#[tokio::test]
async fn commit_tick_existing_replay_storage_refusal_does_not_phantom_fold() {
    let executor = Arc::new(SlowExecutor::default());
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let service = WalletService::start_parts(
        None,
        journal.clone(),
        executor.clone(),
        Policy {
            // The existing user Move and the distinct evacuation each consume one inbound
            // msat. The real projection therefore fits exactly; a phantom replay fold exceeds it.
            per_fed_cap: Msat(2),
            spending_target: Msat(2),
            standby_target: Msat(2),
            ..Policy::default()
        },
        None,
    )
    .await
    .expect("start constrained-cap service");
    let client = service.client();
    let occurrence = Occurrence(1_024);
    let replay = one_msat_funding(
        "move:storage-refusal-existing-replay",
        fed(1),
        fed(2),
        occurrence,
    );
    // A zero-sized funding decision still owns FundInto(fed(2)), but its admission leaves the
    // destination headroom needed to isolate the preceding strict-reservation assertion.
    let same_goal = AllocatorDecision {
        action: Action::Move {
            from: fed(3),
            to: fed(2),
            amount: Msat(0),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:storage-refusal-after-existing-replay".to_owned()),
    };
    // This distinct evacuation goal uses the replay action's exact source/destination pair.
    // An existing user Move already reserves one inbound msat at fed(2), and the two-msat cap
    // below leaves exactly one more msat of room. A phantom fold of the replay would count that
    // inbound reservation twice and refuse this otherwise unrelated evacuation.
    let shared_action = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(1),
            to: fed(2),
            amount: Msat(1),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("move:storage-refusal-shared-existing-action".to_owned()),
    };
    let existing = Intent::from_decision(&replay, Actor::User, 1);
    journal.upsert(&existing).await.expect("seed replay row");

    let plan = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("balance facts");
    // CommitTick's `decision_existed` pre-read and `decide_op`'s existing-key dispatch each read
    // once.  Fault the core helper's third replay read, which maps its journal ExecError to a
    // StorageError refusal.
    journal.fail_one_intent_read_after_successes_for_test(2);
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![replay.clone(), shared_action.clone(), same_goal.clone()],
                0,
                plan,
            ),
            BTreeMap::from([(fed(1), Msat(1)), (fed(2), Msat(0)), (fed(3), Msat(0))]),
            facts,
        )
        .await
        .expect("the replay refusal does not stop later work");

    assert!(
        report.refused.iter().any(|refusal| {
            refusal.key == replay.idempotency_key && refusal.reason == RefuseReason::StorageError
        }),
        "{report:#?}"
    );
    assert_eq!(
        report.accepted,
        vec![
            shared_action.idempotency_key.clone(),
            same_goal.idempotency_key.clone()
        ],
        "an existing-key replay error must not manufacture a goal or strict-reservation holder"
    );
    assert_eq!(
        journal
            .get(&replay.idempotency_key)
            .await
            .expect("read replay")
            .expect("seeded replay remains")
            .status,
        IntentStatus::Pending
    );
    assert!(journal
        .get(&same_goal.idempotency_key)
        .await
        .expect("read same-goal")
        .is_some());
    assert!(journal
        .get(&shared_action.idempotency_key)
        .await
        .expect("read shared-action decision")
        .is_some());
    wait_for_registry(&client, 2).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
    service.shutdown().await.expect("shutdown");
}

/// A definite admission refusal happens before the fresh intent is journaled.  It must not turn a
/// rejected key into a conservative in-batch holder: the smaller compatible move below shares both
/// the first move's source and funding goal, so either a phantom goal or strict-reservation fold
/// would reject it.
#[tokio::test]
async fn commit_tick_definite_refusal_does_not_phantom_fold_goal_or_reservation() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(1_025);
    let refused = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(2),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:definite-refusal-no-fold".to_owned()),
    };
    let compatible = one_msat_funding(
        "move:definite-refusal-compatible",
        fed(1),
        fed(2),
        occurrence,
    );
    let plan = client.issue_tick_plan_token().await.expect("plan token");
    let facts = client
        .issue_balance_facts_token()
        .await
        .expect("balance facts");

    let report = client
        .commit_tick(
            TickRound::for_test(vec![refused.clone(), compatible.clone()], 0, plan),
            BTreeMap::from([(fed(1), Msat(1)), (fed(2), Msat(0))]),
            facts,
        )
        .await
        .expect("a definite refusal is scoped to its key");

    assert!(
        report.refused.iter().any(|refusal| {
            refusal.key == refused.idempotency_key
                && refusal.reason == RefuseReason::InsufficientAfterReservations
        }),
        "{report:#?}"
    );
    assert_eq!(
        report.accepted,
        vec![compatible.idempotency_key.clone()],
        "the rejected key must not manufacture a goal or reservation holder"
    );
    assert!(journal
        .get(&refused.idempotency_key)
        .await
        .expect("read definite-refusal key")
        .is_none());
    assert!(journal
        .get(&compatible.idempotency_key)
        .await
        .expect("read compatible key")
        .is_some());
    wait_for_registry(&client, 1).await;
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_tick_scopes_an_exact_terminal_replay_and_commits_an_independent_decision() {
    let (service, journal) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let client = service.client();
    let occurrence = Occurrence(34);
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence,
        idempotency_key: IdempotencyKey("move:stale-terminal".to_owned()),
    };
    client
        .commit_tick(
            TickRound::for_test(
                vec![decision.clone()],
                0,
                client.issue_tick_plan_token().await.expect("first token"),
            ),
            BTreeMap::from([(fed(1), Msat(100)), (fed(2), Msat(0))]),
            client
                .issue_balance_facts_token()
                .await
                .expect("first facts"),
        )
        .await
        .expect("first commit");
    wait_for_registry(&client, 0).await;
    let independent = AllocatorDecision {
        action: Action::Evacuate {
            from: fed(3),
            to: fed(4),
            amount: Msat(10),
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        reason: ReasonCode::ShutdownNotice,
        occurrence,
        idempotency_key: IdempotencyKey("evac:independent-after-terminal-replay".to_owned()),
    };
    let report = client
        .commit_tick(
            TickRound::for_test(
                vec![decision.clone(), independent.clone()],
                0,
                client.issue_tick_plan_token().await.expect("second token"),
            ),
            BTreeMap::from([
                (fed(1), Msat(100)),
                (fed(2), Msat(0)),
                (fed(3), Msat(10)),
                (fed(4), Msat(0)),
            ]),
            client
                .issue_balance_facts_token()
                .await
                .expect("second facts"),
        )
        .await
        .expect("terminal replay is a scoped refusal");
    assert_eq!(report.refused.len(), 1, "{report:#?}");
    assert_eq!(report.refused[0].key, decision.idempotency_key);
    assert!(report.refused[0]
        .message
        .contains("already has terminal durable work"));
    assert_eq!(report.accepted, vec![independent.idempotency_key.clone()]);
    assert!(journal
        .get(&independent.idempotency_key)
        .await
        .unwrap()
        .is_some());
    service.shutdown().await.expect("shutdown");
}

/// A funding `Move` the AGENT decided, journaled `Pending` under `occurrence`.
fn agent_funding_intent(
    from: FederationId,
    to: FederationId,
    amount: Msat,
    occurrence: Occurrence,
) -> Intent {
    Intent {
        idempotency_key: funding_key(from, to, occurrence),
        attempt: 0,
        action: Action::Move {
            from,
            to,
            amount,
            fee_cap: Msat(0),
            gateway: None,
        },
        max_fee: Some(Msat(0)),
        status: IntentStatus::Pending,
        reason: ReasonCode::StandbyBelowTarget,
        actor: Actor::Agent { occurrence },
        created_at_ms: 1,
        operation_id: None,
        invoice: None,
    }
}

/// An evacuation the AGENT decided, journaled `Pending` under `occurrence`.
fn agent_evacuation_intent(
    from: FederationId,
    to: FederationId,
    amount: Msat,
    occurrence: Occurrence,
) -> Intent {
    Intent {
        idempotency_key: evacuation_key(from, to, occurrence),
        attempt: 0,
        action: Action::Evacuate {
            from,
            to,
            amount,
            fee_cap: Msat(0),
            gateway: None,
            fee_cap_components: None,
        },
        max_fee: Some(Msat(0)),
        status: IntentStatus::Pending,
        reason: ReasonCode::ShutdownNotice,
        actor: Actor::Agent { occurrence },
        created_at_ms: 1,
        operation_id: None,
        invoice: None,
    }
}

/// `allocator::idem_move`'s key shape, rebuilt here so the test names the exact key the
/// allocator would emit for the same logical goal at a different occurrence.
fn funding_key(from: FederationId, to: FederationId, occurrence: Occurrence) -> IdempotencyKey {
    IdempotencyKey(format!(
        "move:{}:{}:{}",
        from.to_hex(),
        to.to_hex(),
        occurrence.0
    ))
}

/// `allocator::idem_evac`'s key shape (same reason as [`funding_key`]).
fn evacuation_key(from: FederationId, to: FederationId, occurrence: Occurrence) -> IdempotencyKey {
    IdempotencyKey(format!(
        "evac:{}:{}:{}",
        from.to_hex(),
        to.to_hex(),
        occurrence.0
    ))
}

fn dying_probe(balance: u64) -> crate::probe::ProbeResult {
    crate::probe::ProbeResult {
        shutdown_scheduled: true,
        status_scheduled_shutdown: true,
        ..healthy_probe(balance)
    }
}

#[tokio::test]
async fn shutdown_planner_absorbs_issued_pay_and_sending_move_source_debits() {
    let policy = TickPolicy {
        per_fed_cap: Msat(10_000_000),
        target_spending_balance: Msat(0),
        standby_target: Msat(0),
        spending_fed: Some(fed(2)),
        standby_fed: Some(fed(2)),
        occurrence: Occurrence(970),
        ..TickPolicy::default()
    };

    let pay_journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    pay_journal
        .upsert(&Intent {
            idempotency_key: IdempotencyKey("pay:issued-before-shutdown".to_owned()),
            attempt: 0,
            action: Action::Pay {
                from: fed(1),
                invoice: Invoice("invoice-issued-before-shutdown".to_owned()),
                amount: Msat(500_000),
                fee_cap: Msat(0),
                payment_hash: [0x91; 32],
                gateway: None,
            },
            max_fee: Some(Msat(0)),
            status: IntentStatus::Executing,
            reason: ReasonCode::UserInitiated,
            actor: Actor::User,
            created_at_ms: 1,
            operation_id: Some(OperationId([0x91; 32])),
            invoice: None,
        })
        .await
        .expect("seed issued Pay");
    let pay_round = plan_tick_round(
        &pay_journal,
        None,
        vec![(fed(1), dying_probe(500_000)), (fed(2), healthy_probe(0))],
        &policy,
        1,
        None,
    )
    .await
    .expect("plan shutdown after Pay issuance");
    assert!(
        pay_round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate {
                from,
                amount: Msat(amount),
                ..
            } if from == fed(1) && amount > 0
        )),
        "an issued Pay must not zero the already-reduced shutdown balance: {:#?}",
        pay_round.decisions
    );

    let move_journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let move_key = IdempotencyKey("move:sending-before-shutdown".to_owned());
    let move_intent = Intent {
        idempotency_key: move_key.clone(),
        attempt: 0,
        action: Action::Move {
            from: fed(1),
            to: fed(3),
            amount: Msat(500_000),
            fee_cap: Msat(0),
            gateway: None,
        },
        max_fee: Some(Msat(0)),
        status: IntentStatus::Executing,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: 1,
        operation_id: None,
        invoice: None,
    };
    move_journal
        .upsert(&move_intent)
        .await
        .expect("seed sending Move");
    assert!(move_journal
        .put_move_if_attempt(
            &move_key,
            0,
            &wallet_core::MoveRecord {
                key: move_key.clone(),
                from: Some(fed(1)),
                to: fed(3),
                amount: Msat(500_000),
                fee_cap: Msat(0),
                gateway: crate::GatewayUrl("https://gateway.invalid".to_owned()),
                send_required: true,
                invoice: Some(Invoice("invoice-sending-before-shutdown".to_owned())),
                recv_op: Some(OperationId([0x92; 32])),
                send_op: Some(OperationId([0x93; 32])),
                phase: wallet_core::MovePhase::Sending,
                outcome: None,
                preimage: None,
                receive_fee_quoted: None,
                send_fee_quoted: None,
            },
        )
        .await
        .expect("write Sending record"));
    let move_round = plan_tick_round(
        &move_journal,
        None,
        vec![
            (fed(1), dying_probe(500_000)),
            (fed(2), healthy_probe(0)),
            (fed(3), healthy_probe(0)),
        ],
        &policy,
        1,
        None,
    )
    .await
    .expect("plan shutdown after Move send");
    assert_eq!(
        move_round.snapshot.reservations.inbound(fed(3)),
        Msat(500_000),
        "Sending absorbs the source debit but retains the promised destination inflow"
    );
    assert!(
        move_round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate {
                from,
                amount: Msat(amount),
                ..
            } if from == fed(1) && amount > 0
        )),
        "a Sending Move must not zero the already-reduced shutdown balance: {:#?}",
        move_round.decisions
    );
}

#[tokio::test]
async fn corrupt_move_record_keeps_shutdown_planning_strict() {
    let db = MemDatabase::new().into_database();
    let journal = Arc::new(FedimintJournal::new(db.clone()));
    let key = IdempotencyKey("move:corrupt-shutdown-record".to_owned());
    journal
        .upsert(&Intent {
            idempotency_key: key.clone(),
            attempt: 0,
            action: Action::Move {
                from: fed(1),
                to: fed(3),
                amount: Msat(500_000),
                fee_cap: Msat(0),
                gateway: None,
            },
            max_fee: Some(Msat(0)),
            status: IntentStatus::Executing,
            reason: ReasonCode::UserInitiated,
            actor: Actor::User,
            created_at_ms: 1,
            operation_id: None,
            invoice: None,
        })
        .await
        .expect("seed live move");
    let app_db = db.with_prefix(vec![0x00]);
    let mut raw_key = vec![0x02];
    raw_key.extend_from_slice(key.0.as_bytes());
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&raw_key, b"not valid json")
        .await
        .expect("insert corrupt MoveRecord");
    dbtx.commit_tx_result()
        .await
        .expect("commit corrupt MoveRecord");

    let round = plan_tick_round(
        &journal,
        None,
        vec![
            (fed(1), dying_probe(500_000)),
            (fed(2), healthy_probe(0)),
            (fed(3), healthy_probe(0)),
        ],
        &TickPolicy {
            per_fed_cap: Msat(10_000_000),
            target_spending_balance: Msat(0),
            standby_target: Msat(0),
            spending_fed: Some(fed(2)),
            standby_fed: Some(fed(2)),
            occurrence: Occurrence(974),
            ..TickPolicy::default()
        },
        1,
        None,
    )
    .await
    .expect("corrupt derived cache falls back instead of aborting planning");
    assert_eq!(round.snapshot.reservations.outbound(fed(1)), Msat(500_000));
    assert!(
        !round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate {
                from,
                amount: Msat(amount),
                ..
            } if from == fed(1) && amount > 0
        )),
        "corrupt phase data must never release the strict source hold"
    );
}

/// br-p93: the planner BOTH paths share (`Runtime::plan_tick` in standalone, `DecideTickRound` in
/// the daemon) suppresses only the conflicting goal. The first arm proves the fixture really does
/// want both pieces of work, so the second arm's absence means suppression rather than a snapshot
/// that never planned it.
#[tokio::test]
async fn the_shared_planner_drops_only_the_conflicting_goal() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let occurrence = Occurrence(12);
    let mut policy = TickPolicy {
        per_fed_cap: Msat(10_000_000),
        target_spending_balance: Msat(1_000_000),
        standby_target: Msat(1_000_000),
        spending_fed: Some(fed(1)),
        standby_fed: Some(fed(2)),
        occurrence,
        ..TickPolicy::default()
    };
    // fed(1) is below its spending target (top it up from the standby) and fed(3) is dying
    // (evacuate it) — two independent goals in one round.
    let probes = vec![
        (fed(1), healthy_probe(100_000)),
        (fed(2), healthy_probe(5_000_000)),
        (fed(3), dying_probe(500_000)),
    ];
    let top_up = funding_key(fed(2), fed(1), occurrence);
    let evacuation = evacuation_key(fed(3), fed(2), occurrence);

    let unblocked = plan_tick_round(&journal, None, probes.clone(), &policy, 1, None)
        .await
        .expect("plan with nothing in flight");
    let planned = |round: &crate::service::actor::PlannedTickRound, key: &IdempotencyKey| {
        round
            .decisions
            .iter()
            .any(|decision| decision.idempotency_key == *key)
    };
    assert!(
        planned(&unblocked, &top_up) && planned(&unblocked, &evacuation),
        "the fixture wants both goals: {:#?}",
        unblocked.decisions
    );

    // The SAME goal is already in flight — from a different source, at a different size, under an
    // older occurrence. None of that is part of the goal's identity.
    policy.blocked = wallet_core::GoalBlockers::from_intents(&[agent_funding_intent(
        fed(4),
        fed(1),
        Msat(1),
        Occurrence(11),
    )]);
    let blocked = plan_tick_round(&journal, None, probes, &policy, 1, None)
        .await
        .expect("plan with the goal in flight");
    assert!(
        !planned(&blocked, &top_up),
        "the conflicting top-up must not be re-planned: {:#?}",
        blocked.decisions
    );
    assert!(
        planned(&blocked, &evacuation),
        "independent work in the same round still plans: {:#?}",
        blocked.decisions
    );
}

/// A blocked evacuation must be removed before the allocator charges its intra-tick destination
/// reservation. The durable old A -> B intent already reserves 400k inbound at B; if a fresh
/// Evacuate(A) is also allowed to consume the remaining 600k locally before being discarded, the
/// independent Evacuate(C) sees no room and is wrongly refused.
#[tokio::test]
async fn blocked_evacuation_does_not_consume_room_needed_by_an_independent_evacuation() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let stuck = agent_evacuation_intent(fed(1), fed(2), Msat(400_000), Occurrence(10));
    journal.upsert(&stuck).await.expect("seed stuck evacuation");

    let mut policy = TickPolicy {
        per_fed_cap: Msat(1_000_000),
        target_spending_balance: Msat(0),
        standby_target: Msat(0),
        spending_fed: Some(fed(2)),
        standby_fed: Some(fed(2)),
        occurrence: Occurrence(11),
        ..TickPolicy::default()
    };
    policy.blocked = wallet_core::GoalBlockers::from_intents(std::slice::from_ref(&stuck));

    let round = plan_tick_round(
        &journal,
        None,
        vec![
            (fed(1), dying_probe(600_000)),
            (fed(2), healthy_probe(0)),
            (fed(3), dying_probe(500_000)),
        ],
        &policy,
        1,
        None,
    )
    .await
    .expect("plan with one blocked and one independent evacuation");

    assert!(
        !round.decisions.iter().any(
            |decision| matches!(decision.action, Action::Evacuate { from, .. } if from == fed(1))
        ),
        "the old logical evacuation must stay suppressed: {:#?}",
        round.decisions
    );
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate {
                from,
                to,
                amount: Msat(500_000),
                ..
            } if from == fed(3) && to == fed(2)
        )),
        "the blocked A goal must not consume B's remaining room before independent C is planned: {:#?}",
        round.decisions
    );
}

/// A durable evacuation owns its source balance even if the next probe reports that source healthy
/// again. New allocator funding sourced from that federation must wait, while an evacuation of a
/// different source into the old evacuation's destination remains independent.
#[tokio::test]
async fn a_live_evacuation_blocks_recovered_source_funding_not_an_independent_evacuation() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    let stuck = agent_evacuation_intent(fed(1), fed(2), Msat(400_000), Occurrence(10));
    journal.upsert(&stuck).await.expect("seed stuck evacuation");

    let mut policy = TickPolicy {
        per_fed_cap: Msat(10_000_000),
        target_spending_balance: Msat(1_000_000),
        standby_target: Msat(1_000_000),
        spending_fed: Some(fed(1)),
        standby_fed: Some(fed(2)),
        occurrence: Occurrence(11),
        ..TickPolicy::default()
    };
    policy.blocked = wallet_core::GoalBlockers::from_intents(std::slice::from_ref(&stuck));

    let round = plan_tick_round(
        &journal,
        None,
        vec![
            (fed(1), healthy_probe(5_000_000)),
            (fed(2), healthy_probe(100_000)),
            (fed(3), dying_probe(500_000)),
        ],
        &policy,
        1,
        None,
    )
    .await
    .expect("plan after the evacuation source recovered");

    assert!(
        !round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Move { from, to, .. } if from == fed(1) || to == fed(1)
        )),
        "new allocator funding must not race the live evacuation of fed(1): {:#?}",
        round.decisions
    );
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate { from, to, .. } if from == fed(3) && to == fed(2)
        )),
        "the independent evacuation still plans: {:#?}",
        round.decisions
    );
}

/// br-p93: a live evacuation owns its source against later allocator MOVES, and no further. The
/// standby is `safest_other`'s FIRST choice of evacuation destination, so if a stuck
/// `Evacuate(standby)` also blocked evacuations INTO the standby, a different dying federation's
/// whole balance would sit there for as long as that stuck intent lives — reinstating, for the one
/// case that matters most, the wallet-wide strand this bead removes. Money into a recovered
/// federation is safe: `eligible_for_evacuation` requires the destination to be healthy, and the
/// worst case is that the stuck drain later carries it one hop further.
#[tokio::test]
async fn a_live_evacuation_does_not_strand_a_dying_federation_that_drains_into_it() {
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    // The STANDBY itself was evacuated while unhealthy, and that intent never terminalized.
    let stuck = agent_evacuation_intent(fed(2), fed(1), Msat(400_000), Occurrence(10));
    journal.upsert(&stuck).await.expect("seed stuck evacuation");

    let mut policy = TickPolicy {
        per_fed_cap: Msat(10_000_000),
        target_spending_balance: Msat(0),
        standby_target: Msat(0),
        spending_fed: Some(fed(1)),
        standby_fed: Some(fed(2)),
        occurrence: Occurrence(11),
        ..TickPolicy::default()
    };
    policy.blocked = wallet_core::GoalBlockers::from_intents(std::slice::from_ref(&stuck));

    let round = plan_tick_round(
        &journal,
        None,
        // fed(2) has recovered, so it is eligible again — and being the standby it is the
        // destination `safest_other` picks for the newly dying fed(3).
        vec![
            (fed(1), healthy_probe(1_000_000)),
            (fed(2), healthy_probe(100_000)),
            (fed(3), dying_probe(500_000)),
        ],
        &policy,
        1,
        None,
    )
    .await
    .expect("plan while the standby's own evacuation is stuck");

    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate { from, to, .. } if from == fed(3) && to == fed(2)
        )),
        "a dying federation must still drain into the recovered standby: {:#?}",
        round.decisions
    );
    assert!(
        !round.decisions.iter().any(
            |decision| matches!(decision.action, Action::Evacuate { from, .. } if from == fed(2))
        ),
        "the standby's own stuck evacuation goal stays suppressed: {:#?}",
        round.decisions
    );
}

/// br-p93 regression: a suppressed recurrence can relax its pinned source only when the held
/// conflict has that same source association. A re-sourced same-goal holder cannot vouch for it.
#[tokio::test]
async fn a_suppressed_goal_does_not_fail_a_pinned_source_round() {
    let (service, _journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("read policy");
    policy.per_fed_cap = Msat(10_000_000);
    policy.spending_target = Msat(1_000_000);
    policy.standby_target = Msat(1_000_000);
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    client
        .put_policy(policy)
        .await
        .expect("pin the designation");

    // The pinned standby funds the spending fed but registers no gateway of its own.
    let mut source_only = healthy_probe(5_000_000);
    source_only.gateway_available = false;
    let probes = vec![(fed(1), healthy_probe(100_000)), (fed(2), source_only)];
    let occurrence = Occurrence(21);
    let top_up = funding_key(fed(2), fed(1), occurrence);

    let unblocked = client
        .decide_tick_round(ProbeFacts {
            probes: probes.clone(),
            occurrence,
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("the move it sources relaxes the pin");
    assert!(
        unblocked
            .decisions
            .iter()
            .any(|decision| decision.idempotency_key == top_up),
        "the fixture's pin is only relaxed by this move: {:#?}",
        unblocked.decisions
    );

    let blocked = client
        .decide_tick_round(ProbeFacts {
            probes,
            occurrence,
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::from_intents(&[agent_funding_intent(
                fed(2),
                fed(1),
                Msat(1),
                Occurrence(20),
            )]),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("a same-source withheld decision vouches for its pinned source");
    assert!(
        !blocked
            .decisions
            .iter()
            .any(|decision| decision.idempotency_key == top_up),
        "the conflicting goal is still withheld: {:#?}",
        blocked.decisions
    );
    // Model a policy rotation: the current designation sources the suppressed recurrence from
    // fed(2), while the old held `FundInto(fed(1))` was sourced from fed(4).
    let re_sourced = client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), healthy_probe(100_000)),
                (fed(2), {
                    let mut source_only = healthy_probe(5_000_000);
                    source_only.gateway_available = false;
                    source_only
                }),
            ],
            occurrence,
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::from_intents(&[agent_funding_intent(
                fed(4),
                fed(1),
                Msat(1),
                Occurrence(20),
            )]),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await;
    assert!(
        re_sourced.is_err(),
        "a different held source must not relax the suppressed recurrence"
    );
    service.shutdown().await.expect("shutdown");
}

/// Policy rotation must not let an old funding source conceal the new, unusable funding
/// destination. The current `FundInto(A)` is refused at A's receive gate, so it is loud even
/// though old `A -> B` work remains held.
#[tokio::test]
async fn policy_rotation_old_a_to_b_does_not_exempt_new_unusable_funding_destination_a() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("read policy");
    policy.per_fed_cap = Msat(10_000_000);
    policy.spending_target = Msat(1_000_000);
    policy.standby_target = Msat(0);
    // The rotated policy now tops up A from B. A is an explicit pin and the funding destination.
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    client
        .put_policy(policy)
        .await
        .expect("persist rotated policy");

    let old_a_to_b = agent_funding_intent(fed(1), fed(2), Msat(1), Occurrence(20));
    let mut unusable_a = healthy_probe(100_000);
    unusable_a.gateway_available = false;
    let error = client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), unusable_a), (fed(2), healthy_probe(5_000_000))],
            occurrence: Occurrence(21),
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::from_intents(&[old_a_to_b]),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect_err("the fresh FundInto(A) receive-gate refusal must keep A loud");
    assert!(
        error.to_string().contains("failed the lnv2/probe gate")
            && error.to_string().contains(&fed(1).to_hex()),
        "{error:?}"
    );
    service.shutdown().await.expect("shutdown");
}

/// A held evacuation can outlive every fresh executable recurrence: once its source is empty, the
/// allocator records an advisory recurrence. Its paired held `Evacuate(A)` source, rather than a
/// merged preflighted decision list, relaxes A's raw pin gate so C's independent drain is not
/// globally rejected.
#[tokio::test]
async fn held_empty_evacuation_relaxes_its_raw_pin_while_independent_drain_survives() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("read policy");
    policy.per_fed_cap = Msat(1_000_000);
    policy.spending_target = Msat(0);
    policy.standby_target = Msat(0);
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    client
        .put_policy(policy)
        .await
        .expect("pin the designation");

    let held = agent_evacuation_intent(fed(1), fed(2), Msat(400_000), Occurrence(20));
    let mut empty_dying_a = dying_probe(0);
    empty_dying_a.gateway_available = false;
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), empty_dying_a),
                (fed(2), healthy_probe(0)),
                (fed(3), dying_probe(500_000)),
            ],
            occurrence: Occurrence(21),
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::from_intents(&[held]),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("the paired held A evacuation advisory must keep its raw pin from rejecting C");

    assert!(
        !round.decisions.iter().any(
            |decision| matches!(decision.action, Action::Evacuate { from, .. } if from == fed(1))
        ),
        "A has no fresh executable evacuation: {:#?}",
        round.decisions
    );
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::RefuseInflow { fed: refused, .. } if refused == fed(1)
        )),
        "the empty A evacuation remains an advisory refusal: {:#?}",
        round.decisions
    );
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate { from, to, amount: Msat(500_000), .. }
                if from == fed(3) && to == fed(2)
        )),
        "independent C evacuation must survive: {:#?}",
        round.decisions
    );
    service.shutdown().await.expect("shutdown");
}

/// `FundInto(B)` deliberately identifies only B, so this needs the original held move source A
/// paired with the current zero-sized funding advisory. With no source availability, fresh sizing
/// emits no A -> B move; that associated recurrence keeps A's raw pin from rejecting C's
/// independent evacuation.
#[tokio::test]
async fn held_funding_source_relaxes_its_raw_pin_without_a_fresh_recurrence() {
    let (service, _) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("read policy");
    policy.per_fed_cap = Msat(1_000_000);
    policy.spending_target = Msat(0);
    policy.standby_target = Msat(1_000_000);
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    client
        .put_policy(policy)
        .await
        .expect("pin the designation");

    let held = agent_funding_intent(fed(1), fed(2), Msat(400_000), Occurrence(20));
    let mut empty_a = healthy_probe(0);
    empty_a.gateway_available = false;
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), empty_a),
                (fed(2), healthy_probe(0)),
                (fed(3), dying_probe(500_000)),
            ],
            occurrence: Occurrence(21),
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::from_intents(&[held]),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("the paired held funding advisory must relax A's raw pin");

    assert!(
        !round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Move { from, to, .. } if from == fed(1) && to == fed(2)
        )),
        "A has no source availability for a fresh funding recurrence: {:#?}",
        round.decisions
    );
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate { from, to, amount: Msat(500_000), .. }
                if from == fed(3) && to == fed(2)
        )),
        "independent C evacuation must survive: {:#?}",
        round.decisions
    );
    service.shutdown().await.expect("shutdown");
}

/// A current admitted executable route is stronger evidence than a concurrent destination-side
/// `NotProbed` refusal. This production-shaped fixture has a pinned standby with no registered
/// gateway which is both the source of a top-up and below its own target; the independent
/// evacuation proves this does not freeze the whole actor round. This focused actor fixture skips
/// route I/O; production preflights when budget is available and execution revalidates either way.
#[tokio::test]
async fn admitted_source_route_beats_a_pinned_standby_receive_refusal() {
    let (service, _journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("read policy");
    policy.per_fed_cap = Msat(10_000_000);
    policy.spending_target = Msat(1_000_000);
    policy.standby_target = Msat(1_000_000);
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    client
        .put_policy(policy)
        .await
        .expect("pin the designation");

    let mut pinned_standby = healthy_probe(100_000);
    pinned_standby.gateway_available = false;
    let occurrence = Occurrence(22);
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), healthy_probe(100_000)),
                (fed(2), pinned_standby),
                (fed(3), dying_probe(500_000)),
            ],
            occurrence,
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("the admitted source route must relax the raw pin");

    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Move { from, to, amount, .. }
                if from == fed(2) && to == fed(1) && amount.0 > 0
        )),
        "the standby's admitted top-up is the endpoint voucher: {:#?}",
        round.decisions
    );
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::RefuseInflow {
                fed: refused_fed,
                reason: ReasonCode::NotProbed,
                ..
            } if refused_fed == fed(2)
        )),
        "the standby's current receive refusal remains visible: {:#?}",
        round.decisions
    );
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate { from, to, .. } if from == fed(3) && to == fed(1)
        )),
        "independent work survives the coexistence: {:#?}",
        round.decisions
    );
    service.shutdown().await.expect("shutdown");
}

/// A suppressed executable `B -> A` is current source evidence for its exact
/// held B→A goal.  It must outrank B's simultaneous coarse receive refusal,
/// so that independent evacuation of dying C still reaches commit planning.
#[tokio::test]
async fn suppressed_source_voucher_beats_pinned_standby_receive_refusal() {
    let (service, _journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("read policy");
    policy.per_fed_cap = Msat(10_000_000);
    policy.spending_target = Msat(1_000_000);
    policy.standby_target = Msat(1_000_000);
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    client.put_policy(policy).await.expect("pin designation");

    let mut pinned_b = healthy_probe(100_000);
    pinned_b.gateway_available = false;
    let held_b_to_a = agent_funding_intent(fed(2), fed(1), Msat(1), Occurrence(24));
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), healthy_probe(100_000)),
                (fed(2), pinned_b),
                (fed(3), dying_probe(500_000)),
            ],
            occurrence: Occurrence(25),
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::from_intents(std::slice::from_ref(&held_b_to_a)),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("exact suppressed source evidence must relax B's raw pin");
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::RefuseInflow {
                fed: refused,
                reason: ReasonCode::NotProbed,
                ..
            } if refused == fed(2)
        )),
        "the receive refusal remains visible"
    );
    assert!(
        round.decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Evacuate { from, .. } if from == fed(3)
        )),
        "independent dying C evacuation must survive: {:#?}",
        round.decisions
    );
    service.shutdown().await.expect("shutdown");
}

/// br-p93, PRODUCTION path: one permanently retryable agent funding move must not gate the whole
/// wallet. Reconcile re-drives it (and keeps re-driving it once a live driver owns it), yet the
/// scheduler's eligibility, `DecideTickRound` and `CommitTick` must all let an INDEPENDENT logical
/// goal — evacuating a dying federation — through, while never re-issuing the SAME logical goal
/// (funding fed(2)) under a fresh occurrence. Fails against the `redriven == 0` global gate.
#[tokio::test]
async fn daemon_tick_commits_independent_work_while_the_conflicting_goal_stays_suppressed() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let mut policy = client.get_policy().await.expect("read policy");
    policy.per_fed_cap = Msat(10_000_000);
    policy.spending_target = Msat(1_000_000);
    policy.standby_target = Msat(1_000_000);
    policy.spending_fed = Some(fed(1));
    policy.standby_fed = Some(fed(2));
    client
        .put_policy(policy)
        .await
        .expect("pin the designation");

    // The stuck goal: an OLD partial funding move into fed(2) that never terminalizes.
    // Its 400k durable inbound reservation plus the round's independent 500k evacuation
    // leave a real 100k target shortfall. That residual candidate must still be suppressed
    // by the held FundInto(fed(2)) goal and audited, rather than disappearing merely because
    // funding wants are now reservation-aware.
    let stuck = agent_funding_intent(fed(1), fed(2), Msat(400_000), Occurrence(0));
    journal.upsert(&stuck).await.expect("seed the stuck goal");

    let redriven = client.reconcile().await.expect("first reconcile");
    assert_eq!(redriven.redriven, 1, "the stuck goal is re-driven");
    wait_for_registry(&client, 1).await;
    assert!(
        scheduler::tick_may_commit(&Some(redriven.clone())),
        "a successful reconcile must not disqualify the whole cycle from committing"
    );

    // Registry-owned coverage: a second pass re-drives NOTHING (a live driver owns the intent),
    // so the eligibility value cannot be the re-drive count — the conflict must still be seen.
    let owned = client.reconcile().await.expect("second reconcile");
    assert_eq!(owned.redriven, 0, "the live driver still owns the intent");
    assert!(scheduler::tick_may_commit(&Some(owned.clone())));

    let occurrence = Occurrence(7);
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), healthy_probe(5_000_000)),
                (fed(2), healthy_probe(0)),
                (fed(3), dying_probe(500_000)),
            ],
            occurrence,
            now_ms: 1_000,
            // Exactly what the scheduler cycle builds from its reconcile report.
            price_routes: scheduler::tick_may_commit(&Some(owned.clone())),
            blocked: owned.blocked.clone(),
            admission_snapshot: owned.admission_snapshot.clone(),
        })
        .await
        .expect("plan the tick round");

    let evacuation = evacuation_key(fed(3), fed(2), occurrence);
    let reissued = funding_key(fed(1), fed(2), occurrence);
    assert!(
        round
            .decisions
            .iter()
            .any(|decision| decision.idempotency_key == evacuation),
        "the independent evacuation must be planned: {:#?}",
        round.decisions
    );
    assert!(
        !round
            .decisions
            .iter()
            .any(|decision| decision.idempotency_key == reissued),
        "the conflicting funding goal must not be re-planned under a fresh occurrence: {:#?}",
        round.decisions
    );
    let funding_suppression_refusal = round
        .decisions
        .iter()
        .find(|decision| {
            matches!(
                decision.action,
                Action::RefuseInflow {
                    fed: refused_fed,
                    reason: ReasonCode::StandbyBelowTarget,
                    diagnostics,
                } if refused_fed == fed(2)
                    && diagnostics.amount == Some(Msat(0))
                    && diagnostics.conflict_suppressed
                    && diagnostics.available.is_some_and(|available| available.0 >= 1_000_000)
            )
        })
        .cloned()
        .expect("the fully fundable withheld top-up has a durable advisory twin");
    assert_eq!(
        funding_suppression_refusal.idempotency_key.0,
        format!("conflict-suppressed:{}:standby_below_target", reissued.0),
        "the suppression refusal follows the exact withheld funding key"
    );
    // An ordinary refusal from an earlier retry has the historical generic key. It must not make
    // the correlated suppression fact disappear when this round is committed.
    let mut ordinary_refusal = funding_suppression_refusal.clone();
    ordinary_refusal.idempotency_key = IdempotencyKey(format!(
        "refuse:standby_below_target:{}:{}",
        fed(2).to_hex(),
        occurrence.0
    ));
    if let Action::RefuseInflow { diagnostics, .. } = &mut ordinary_refusal.action {
        diagnostics.amount = Some(Msat(1));
        diagnostics.conflict_suppressed = false;
    }
    journal
        .record_refusals(&[ordinary_refusal.clone()], occurrence, 999)
        .await
        .expect("seed the prior ordinary funding refusal");

    let commit = client
        .commit_tick_legacy(
            round.decisions,
            round.planned_generation,
            round.admission_snapshot.clone(),
        )
        .await
        .expect("commit the round");
    assert_eq!(
        commit.accepted,
        vec![evacuation],
        "exactly the independent goal commits"
    );
    assert!(
        journal
            .get(&reissued)
            .await
            .expect("read the re-issued key")
            .is_none(),
        "the stuck goal must not be journaled again under a fresh occurrence"
    );
    assert!(matches!(
        journal
            .operation(&crate::journal::OperationRef::Key(
                funding_suppression_refusal.idempotency_key
            ))
            .await
            .expect("read suppression refusal ledger row")
            .expect("record_refusals persists the actor decision list")
            .kind,
        wallet_core::OperationKind::Refusal { diagnostics, .. }
            if diagnostics.amount == Some(Msat(0)) && diagnostics.conflict_suppressed
    ));
    assert!(
        journal
            .operation(&crate::journal::OperationRef::Key(
                ordinary_refusal.idempotency_key
            ))
            .await
            .expect("read ordinary refusal ledger row")
            .is_some(),
        "the prior ordinary refusal remains distinct from the suppression row"
    );

    // Final fail-closed check: a caller that bypasses planning is still refused at commit.
    let bypass_occurrence = Occurrence(8);
    let bypass = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(1_000_000),
            fee_cap: Msat(0),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: bypass_occurrence,
        idempotency_key: funding_key(fed(1), fed(2), bypass_occurrence),
    };
    let refused = client
        .commit_tick_with_facts_legacy(
            vec![bypass.clone()],
            Some(BTreeMap::from([
                (fed(1), Msat(5_000_000)),
                (fed(2), Msat(0)),
            ])),
            None,
            round.planned_generation,
            round.admission_snapshot,
        )
        .await
        .expect("a suppressed decision is a refusal, not a transport error");
    assert!(refused.accepted.is_empty());
    assert_eq!(refused.refused.len(), 1);
    assert_eq!(refused.refused[0].reason, RefuseReason::Conflict);
    assert!(journal
        .get(&bypass.idempotency_key)
        .await
        .expect("read the bypassed key")
        .is_none());

    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn actor_commit_persists_a_fully_sized_suppressed_evacuation_refusal() {
    let (service, journal) = fixture(Arc::new(SlowExecutor::default())).await;
    let client = service.client();
    let occurrence = Occurrence(23);
    let held = agent_evacuation_intent(fed(1), fed(2), Msat(1), Occurrence(22));
    let round = client
        .decide_tick_round(ProbeFacts {
            probes: vec![(fed(1), dying_probe(900_000)), (fed(2), healthy_probe(0))],
            occurrence,
            now_ms: 1_000,
            price_routes: false,
            blocked: wallet_core::GoalBlockers::from_intents(std::slice::from_ref(&held)),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("plan the held evacuation");
    let refusal = round
        .decisions
        .iter()
        .find(|decision| {
            matches!(
                decision.action,
                Action::RefuseInflow {
                    fed: refused_fed,
                    reason: ReasonCode::ShutdownNotice,
                    diagnostics,
                } if refused_fed == fed(1)
                    && diagnostics.available == Some(Msat(900_000))
                    && diagnostics.amount == Some(Msat(0))
                    && diagnostics.conflict_suppressed
                    && diagnostics.max_fee.is_none()
                    && diagnostics.max_fee_bps.is_none()
            )
        })
        .cloned()
        .expect("the withheld evacuation has an honest advisory twin");
    let candidate = evacuation_key(fed(1), fed(2), occurrence);
    assert_eq!(
        refusal.idempotency_key.0,
        format!("conflict-suppressed:{}:shutdown_notice", candidate.0),
        "the suppression refusal follows the exact withheld evacuation key"
    );
    let mut ordinary_refusal = refusal.clone();
    ordinary_refusal.idempotency_key = IdempotencyKey(format!(
        "refuse:shutdown_notice:{}:{}",
        fed(1).to_hex(),
        occurrence.0
    ));
    if let Action::RefuseInflow { diagnostics, .. } = &mut ordinary_refusal.action {
        diagnostics.amount = None;
        diagnostics.conflict_suppressed = false;
    }
    journal
        .record_refusals(&[ordinary_refusal.clone()], occurrence, 999)
        .await
        .expect("seed the prior ordinary evacuation refusal");

    let report = client
        .commit_tick_legacy(
            round.decisions,
            round.planned_generation,
            round.admission_snapshot,
        )
        .await
        .expect("advisory-only suppressed evacuation commits");
    assert!(report.accepted.is_empty());
    assert!(matches!(
        journal
            .operation(&crate::journal::OperationRef::Key(refusal.idempotency_key))
            .await
            .expect("read suppression refusal ledger row")
            .expect("record_refusals persists the actor decision list")
            .kind,
        wallet_core::OperationKind::Refusal { diagnostics, .. }
            if diagnostics.available == Some(Msat(900_000))
                && diagnostics.amount == Some(Msat(0))
                && diagnostics.conflict_suppressed
                && diagnostics.max_fee.is_none()
                && diagnostics.max_fee_bps.is_none()
    ));
    assert!(
        journal
            .operation(&crate::journal::OperationRef::Key(
                ordinary_refusal.idempotency_key
            ))
            .await
            .expect("read ordinary refusal ledger row")
            .is_some(),
        "the prior ordinary refusal remains distinct from the suppression row"
    );
    service.shutdown().await.expect("shutdown");
}

/// br-p93: the commit-time guard also covers a batch that carries ONE logical goal twice. The
/// pre-loop durable scan cannot see what the batch itself journals, so without folding each
/// admission back in, a caller that bypassed planning would get two keys admitted for one goal —
/// two federations funding the same destination for the same shortfall, i.e. the same money moved
/// twice. `decide_with_blockers` cannot emit that pair (one funding goal per designated
/// destination), which is exactly why only this seam can catch it.
#[tokio::test]
async fn commit_tick_refuses_a_second_decision_for_a_goal_admitted_in_the_same_batch() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(41);
    client
        .decide_tick_round(ProbeFacts {
            probes: vec![
                (fed(1), healthy_probe(100)),
                (fed(2), healthy_probe(0)),
                (fed(3), healthy_probe(100)),
            ],
            occurrence,
            now_ms: 101,
            price_routes: false,
            // Nothing durable is in flight: the ONLY conflict here is inside the batch.
            blocked: wallet_core::GoalBlockers::default(),
            admission_snapshot: client.issue_tick_plan_token().await.expect("token"),
        })
        .await
        .expect("seed tick facts");

    // Two different SOURCES funding one destination. Different keys, one goal — and each is
    // independently admissible (both sources are funded, and fed(2) has cap room for both).
    let decisions = [fed(1), fed(3)]
        .into_iter()
        .map(|from| AllocatorDecision {
            action: Action::Move {
                from,
                to: fed(2),
                amount: Msat(10),
                fee_cap: Msat(0),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence,
            idempotency_key: funding_key(from, fed(2), occurrence),
        })
        .collect::<Vec<_>>();

    let report = client
        .commit_tick_legacy(
            decisions.clone(),
            0,
            client.issue_tick_plan_token().await.expect("token"),
        )
        .await
        .expect("commit the batch");
    assert_eq!(
        report.accepted,
        vec![decisions[0].idempotency_key.clone()],
        "the first decision for the goal is admitted: {report:#?}"
    );
    assert_eq!(
        report
            .refused
            .iter()
            .map(|refusal| (refusal.key.clone(), refusal.reason.clone()))
            .collect::<Vec<_>>(),
        vec![(decisions[1].idempotency_key.clone(), RefuseReason::Conflict)],
        "the second key for the SAME goal is refused as a conflict: {report:#?}"
    );
    assert!(
        journal
            .get(&decisions[1].idempotency_key)
            .await
            .expect("read the duplicate key")
            .is_none(),
        "the duplicate goal is never journaled"
    );
    service.shutdown().await.expect("shutdown");
}

/// br-p93: public `WalletClient::decide_op` is also an actor-admission seam. A caller can submit
/// an agent decision outside CommitTick, so it must not create a second durable key for a live
/// allocator goal; unrelated goals remain independently admissible.
#[tokio::test]
async fn public_decide_op_refuses_a_fresh_agent_key_for_a_live_allocator_goal() {
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor.clone()).await;
    let client = service.client();
    let occurrence = Occurrence(42);
    let request = |key: &str, from, to| OpRequest {
        decision: AllocatorDecision {
            action: Action::Move {
                from,
                to,
                amount: Msat(10),
                fee_cap: Msat(0),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence,
            idempotency_key: IdempotencyKey(key.to_owned()),
        },
        actor: Actor::Agent { occurrence },
        now_ms: 101,
        balances: BTreeMap::from([
            (fed(1), Msat(100)),
            (fed(2), Msat(0)),
            (fed(3), Msat(100)),
            (fed(4), Msat(0)),
        ]),
        probe_session_nonce: None,
        dest_unavailable: None,
    };
    let live = request("move:public-live", fed(1), fed(2));
    let duplicate = request("move:public-duplicate", fed(3), fed(2));
    let independent = request("move:public-independent", fed(1), fed(4));

    client
        .decide_op(live.clone())
        .await
        .expect("public client admits the first agent allocator goal");
    wait_for_registry(&client, 1).await;
    while executor.calls.load(Ordering::SeqCst) != 1 {
        tokio::task::yield_now().await;
    }

    let refusal = client
        .decide_op(duplicate.clone())
        .await
        .expect_err("a fresh public key cannot duplicate a live allocator goal");
    assert!(matches!(
        refusal,
        ServiceError::Refused {
            reason: RefuseReason::Conflict,
            ..
        }
    ));
    assert!(
        journal
            .get(&duplicate.decision.idempotency_key)
            .await
            .expect("read duplicate key")
            .is_none(),
        "the refused duplicate has no durable intent"
    );
    wait_for_registry(&client, 1).await;
    assert_eq!(
        executor.calls.load(Ordering::SeqCst),
        1,
        "the refused duplicate starts no second driver"
    );

    client
        .decide_op(independent.clone())
        .await
        .expect("an independent public agent allocator goal still admits");
    wait_for_registry(&client, 2).await;
    assert!(
        journal
            .get(&independent.decision.idempotency_key)
            .await
            .expect("read independent key")
            .is_some(),
        "the independent goal is durable and driven"
    );
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn closing_every_sender_exits_actor_cleanly() {
    let (service, _) = fixture(Arc::new(ExitExecutor(Exit::Ok))).await;
    let WalletService {
        client,
        task,
        registry: _,
        scheduler_abort: _,
        scheduler_task: _,
        scheduler_alive: _,
        critical_exit: _,
        policy_wake: _,
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
    )
    .await
    .expect("start corrupt-intent service");
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

#[tokio::test]
async fn fresh_admission_with_unopened_destination_fails_fast() {
    // br-u2o: on the FRESH branch (no existing intent), a dest-side handler's joined-but-not-open
    // signal (`dest_unavailable = Some`) makes the actor fail fast with DestinationUnavailable
    // BEFORE anything is journaled — the single-owner, race-free gate. Nothing is admitted.
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor).await;
    let client = service.client();
    let mut req = move_request(
        "move:unopened-dest",
        Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(10),
            fee_cap: Msat(1),
            gateway: None,
        },
        BTreeMap::from([(fed(1), Msat(100))]),
        None,
    );
    req.dest_unavailable = Some(fed(2));
    let error = client
        .decide_op(req)
        .await
        .expect_err("a fresh admission to a joined-but-unopened dest must fail fast");
    assert!(matches!(error, ServiceError::DestinationUnavailable(_)));
    assert!(error.to_string().contains("joined but not currently open"));
    assert!(journal
        .get(&IdempotencyKey("move:unopened-dest".to_owned()))
        .await
        .expect("intent lookup")
        .is_none());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn admitted_while_open_then_closed_still_attaches_on_replay() {
    // br-u2o MUST-PRESERVE: a request admitted while `to` was open must still ATTACH on an
    // idempotent replay after `to` closes — the openness gate lives on the FRESH branch only, and
    // an EXISTING key takes the attach path first. The replay carries `dest_unavailable = Some`
    // (dest now closed) yet dedups instead of 503-ing.
    let executor = Arc::new(SlowExecutor::default());
    let (service, journal) = fixture(executor).await;
    let client = service.client();
    let action = Action::Move {
        from: fed(1),
        to: fed(2),
        amount: Msat(10),
        fee_cap: Msat(1),
        gateway: None,
    };
    let balances = BTreeMap::from([(fed(1), Msat(100))]);
    let admitted = client
        .decide_op(move_request(
            "move:attach-after-close",
            action.clone(),
            balances.clone(),
            None,
        ))
        .await
        .expect("fresh admit while destination open");
    assert!(!admitted.deduplicated);

    let mut replay = move_request("move:attach-after-close", action, balances, None);
    replay.dest_unavailable = Some(fed(2));
    let attached = client
        .decide_op(replay)
        .await
        .expect("replay of an existing key must attach, never 503");
    assert!(attached.deduplicated);
    // The intent is still journaled and live — the replay attached to it rather than refusing.
    assert!(journal
        .get(&IdempotencyKey("move:attach-after-close".to_owned()))
        .await
        .expect("intent lookup")
        .is_some());
    service.shutdown().await.expect("shutdown");
}
