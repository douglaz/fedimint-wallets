//! Operation-ledger durability + reconcile-repair tests (spec §9–§10) over an in-memory
//! fedimint `Database` (`MemDatabase`, no devimint / money path). They pin: the same-dbtx
//! intent + ledger atomicity, seq monotonicity/ordering, one-row-per-key under replay, poison
//! tolerance of the ledger scans, the §9.2 fees/op-ids refresh from the move row, the standalone
//! `record_*` mechanics, and the §10.3 repair decision logic (join arbitration, tick staleness,
//! raw pay/recv custom-meta backfill + hash-dedup) via a mock op-log oracle — INCLUDING the
//! §9.4 skewed-clock cases (forward jump inside the 1h window; a join attempt stamped after
//! `joined_at`).

use async_trait::async_trait;
use fedimint_core::db::mem_impl::MemDatabase;
use fedimint_core::db::{IDatabaseTransactionOpsCore, IRawDatabaseExt};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use wallet_core::{
    drive_intent_step, Action, Actor, AllocatorDecision, DiscoverySource, EvacFeeCap, ExecError,
    ExecutionSummary, Executor, FederationId, FeeBreakdown, IdempotencyKey, Intent, IntentStatus,
    Journal, Msat, Occurrence, OperationKind, OperationRecord, OperationStatus, PerformOutcome,
    RawOpUpdate, ReasonCode, RefusalDiagnostics, SourceStatus,
};
use wallet_fedimint::journal::RawIntentTerminalSink;
use wallet_fedimint::{
    assemble_move_record, FederationInfo, FedimintJournal, GatewayUrl, Invoice, LedgerRepairOracle,
    Leg, MoveParams, MovePhase, MoveRecord, OpArtifact, OperationId, OperationRef,
    RawOpObservation, RawOperationRole, RawTerminal, JOIN_NOOP_REOPEN_NOTE,
};

const BASE: u64 = 1_700_000_000_000; // a base ms timestamp (divisible by 1000: joins the sec/ms math)
const HOUR: u64 = 60 * 60 * 1000;

// Fixed-value injected clocks (§9.4): `fn() -> u64` cannot capture, so a controllable clock is a
// distinct constant-returning fn. Rows are seeded with explicit `now_ms`, so relative age is set
// by picking the journal's clock.
fn clock_base() -> u64 {
    BASE
}
fn clock_base_plus_30m() -> u64 {
    BASE + 30 * 60 * 1000
}
fn clock_base_plus_2h() -> u64 {
    BASE + 2 * HOUR
}

fn mem_ledger() -> FedimintJournal {
    FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base)
}

fn fed(n: u8) -> FederationId {
    FederationId([n; 32])
}

fn key(s: &str) -> IdempotencyKey {
    IdempotencyKey(s.to_string())
}

fn op(n: u8) -> OperationId {
    OperationId([n; 32])
}

fn pay_kind(fed: FederationId) -> OperationKind {
    OperationKind::Pay {
        fed,
        invoice_amount: None,
        payment_hash: None,
        op_id: None,
        gateway: None,
    }
}

fn recv_kind(fed: FederationId, amount: Msat) -> OperationKind {
    OperationKind::Receive {
        fed,
        amount_invoiced: amount,
        op_id: None,
        gateway: None,
    }
}

fn fees_send(quote: u64) -> FeeBreakdown {
    FeeBreakdown {
        fee_cap: None,
        receive_fee: None,
        send_fee_quoted: Some(Msat(quote)),
    }
}

fn move_intent(k: &str, status: IntentStatus) -> Intent {
    Intent {
        idempotency_key: key(k),
        attempt: 0,
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(100_000),
            fee_cap: Msat(2_000),
            gateway: None,
        },
        max_fee: Some(Msat(2_000)),
        status,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: BASE,
        operation_id: None,
        invoice: None,
    }
}

fn move_record_for(k: &str) -> MoveRecord {
    MoveRecord {
        key: key(k),
        from: Some(fed(1)),
        to: fed(2),
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: GatewayUrl("https://gw.example".to_string()),
        send_required: true,
        invoice: Some(Invoice("lnbc1".to_string())),
        recv_op: Some(op(7)),
        send_op: Some(op(9)),
        phase: MovePhase::Sending,
        outcome: None,
        preimage: None,
        receive_fee_quoted: Some(Msat(150)),
        send_fee_quoted: Some(Msat(250)),
    }
}

/// The pilot's evacuation cap rule (`200_000 msat + 300 bps`) and a planned ask far above what
/// the tests then execute — so the cap at the plan and the cap at the executed net differ.
const EVAC_CAP: EvacFeeCap = EvacFeeCap {
    base_msat: Msat(200_000),
    bps: 300,
};
const EVAC_PLANNED: Msat = Msat(75_000_000);

fn evacuation_intent(k: &str, status: IntentStatus) -> Intent {
    Intent {
        idempotency_key: key(k),
        attempt: 0,
        action: Action::Evacuate {
            from: fed(1),
            to: fed(2),
            amount: EVAC_PLANNED,
            fee_cap: EVAC_CAP.at(EVAC_PLANNED),
            gateway: None,
            fee_cap_components: Some(EVAC_CAP),
        },
        max_fee: Some(EVAC_CAP.at(EVAC_PLANNED)),
        status,
        reason: ReasonCode::ShutdownNotice,
        actor: Actor::Agent {
            occurrence: Occurrence(1),
        },
        created_at_ms: BASE,
        operation_id: None,
        invoice: None,
    }
}

/// The params reassembly starts from: the PLANNED pair, exactly as the durable intent holds it.
fn evacuation_params(k: &str) -> MoveParams {
    MoveParams {
        key: key(k),
        operation_key: key(k),
        from: Some(fed(1)),
        to: fed(2),
        amount: EVAC_PLANNED,
        fee_cap: EVAC_CAP.at(EVAC_PLANNED),
        fee_cap_components: Some(EVAC_CAP),
        gateway: GatewayUrl("https://gw.example".to_string()),
        send_required: true,
    }
}

fn fed_info(joined_at: u64) -> FederationInfo {
    FederationInfo {
        invite: "fed1".to_string(),
        db_prefix: 1,
        joined_at,
    }
}

fn refuse_dec(target: FederationId, reason: ReasonCode, k: &str) -> AllocatorDecision {
    AllocatorDecision {
        action: Action::RefuseInflow {
            fed: target,
            reason,
            diagnostics: Default::default(),
        },
        reason,
        occurrence: Occurrence(0),
        idempotency_key: key(k),
    }
}

async fn op_of(j: &FedimintJournal, k: &IdempotencyKey) -> OperationRecord {
    j.operation(&OperationRef::Key(k.clone()))
        .await
        .expect("read")
        .expect("row exists")
}

async fn status_of(j: &FedimintJournal, k: &IdempotencyKey) -> OperationStatus {
    op_of(j, k).await.status
}

// --- a mock op-log oracle: canned evidence so the §10.3 repair logic is testable offline ---

#[derive(Default)]
struct MockOracle {
    by_key: BTreeMap<(FederationId, String), OperationId>,
    by_hash: BTreeMap<(FederationId, [u8; 32]), OperationId>,
    observations: BTreeMap<(FederationId, [u8; 32]), RawOpObservation>,
}

#[async_trait]
impl LedgerRepairOracle for MockOracle {
    async fn find_op_by_correlation_key(
        &self,
        fed: FederationId,
        k: &IdempotencyKey,
    ) -> Result<Option<OperationId>, ExecError> {
        Ok(self.by_key.get(&(fed, k.0.clone())).copied())
    }
    async fn find_send_op_by_payment_hash(
        &self,
        fed: FederationId,
        hash: [u8; 32],
    ) -> Result<Option<OperationId>, ExecError> {
        Ok(self.by_hash.get(&(fed, hash)).copied())
    }
    async fn observe_op(
        &self,
        fed: FederationId,
        operation: OperationId,
    ) -> Result<RawOpObservation, ExecError> {
        self.observations
            .get(&(fed, operation.0))
            .cloned()
            .ok_or_else(|| ExecError::Retryable("no observation".into()))
    }
}

fn empty_oracle() -> MockOracle {
    MockOracle::default()
}

struct RetryDuringSink {
    journal: FedimintJournal,
    replacement: Intent,
}

#[async_trait]
impl RawIntentTerminalSink for RetryDuringSink {
    async fn set_raw_terminal(
        &self,
        key: &IdempotencyKey,
        _fence: &wallet_fedimint::journal::RawIntentTerminalFence,
        _status: IntentStatus,
        _error: Option<String>,
    ) -> Result<bool, ExecError> {
        self.journal
            .set_status(key, 0, IntentStatus::Failed, Some("operator retries"))
            .await?;
        self.journal.retry_failed_intent(&self.replacement).await?;
        Ok(false)
    }
}

/// The repair fence is captured before this oracle receives the observation request.  It retires
/// attempt N while replying with N's terminal witness, exercising the scan/observation boundary:
/// the repair must use the captured sequence and attempt correlation, not whatever now occupies
/// the public key.
struct RetryAfterObservationOracle {
    journal: FedimintJournal,
    replacement: Intent,
    expected_correlation: IdempotencyKey,
    observation: RawOpObservation,
    correct_correlation_queries: Arc<AtomicUsize>,
}

#[async_trait]
impl LedgerRepairOracle for RetryAfterObservationOracle {
    async fn find_op_by_correlation_key(
        &self,
        federation: FederationId,
        correlation: &IdempotencyKey,
    ) -> Result<Option<OperationId>, ExecError> {
        if federation == fed(1) && *correlation == self.expected_correlation {
            self.correct_correlation_queries
                .fetch_add(1, Ordering::SeqCst);
            Ok(Some(op(7)))
        } else {
            Ok(None)
        }
    }

    async fn find_send_op_by_payment_hash(
        &self,
        _federation: FederationId,
        _hash: [u8; 32],
    ) -> Result<Option<OperationId>, ExecError> {
        Ok(None)
    }

    async fn observe_op(
        &self,
        federation: FederationId,
        operation: OperationId,
    ) -> Result<RawOpObservation, ExecError> {
        assert_eq!(federation, fed(1));
        assert_eq!(operation, op(7));
        self.journal
            .set_status(
                &self.replacement.idempotency_key,
                0,
                IntentStatus::Failed,
                Some("operator retries after observation"),
            )
            .await?;
        self.journal.retry_failed_intent(&self.replacement).await?;
        Ok(self.observation.clone())
    }
}

struct FailOnceSink {
    journal: FedimintJournal,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl RawIntentTerminalSink for FailOnceSink {
    async fn set_raw_terminal(
        &self,
        key: &IdempotencyKey,
        fence: &wallet_fedimint::journal::RawIntentTerminalFence,
        status: IntentStatus,
        error: Option<String>,
    ) -> Result<bool, ExecError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ExecError::Retryable("deliberate sink fault".to_owned()));
        }
        self.journal
            .set_raw_terminal_if_fenced(key, fence, status, error.as_deref())
            .await
    }
}

/// Exercises the narrow gap after repair's ledger write but before its intent-only sink CAS.
/// An authoritative same-attempt artifact may supersede a SOFT terminal in-place; the sink must
/// see that changed terminal status and leave the still-live reservation alone.
struct SameAttemptSupersedingSink {
    journal: FedimintJournal,
    result: Arc<std::sync::Mutex<Option<bool>>>,
}

#[async_trait]
impl RawIntentTerminalSink for SameAttemptSupersedingSink {
    async fn set_raw_terminal(
        &self,
        key: &IdempotencyKey,
        fence: &wallet_fedimint::journal::RawIntentTerminalFence,
        status: IntentStatus,
        error: Option<String>,
    ) -> Result<bool, ExecError> {
        let expected_attempt = self
            .journal
            .get(key)
            .await?
            .ok_or_else(|| ExecError::Permanent("test sink intent disappeared".to_owned()))?
            .attempt;
        assert!(
            self.journal
                .record_raw_observation_if_attempt(
                    key,
                    expected_attempt,
                    op(7),
                    &in_flight_send_obs(),
                )
                .await?
        );
        let result = self
            .journal
            .set_raw_terminal_if_fenced(key, fence, status, error.as_deref())
            .await?;
        *self.result.lock().expect("sink result lock") = Some(result);
        Ok(result)
    }
}

fn terminal_send_obs(succeeded: bool, send_fee: u64) -> RawOpObservation {
    RawOpObservation {
        terminal: Some(RawTerminal {
            succeeded,
            error: (!succeeded).then(|| "send failed".to_string()),
        }),
        gateway: Some(GatewayUrl("https://gw".to_string())),
        fees: fees_send(send_fee),
        invoice_amount: Some(Msat(50_000)),
        payment_hash: Some([0xab; 32]),
    }
}

fn repairable_pay_intent(key: IdempotencyKey, status: IntentStatus, attempt: u32) -> Intent {
    Intent {
        idempotency_key: key,
        attempt,
        action: Action::Pay {
            from: fed(1),
            invoice: Invoice("lnbc1repair".into()),
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            payment_hash: [0xab; 32],
            gateway: None,
        },
        max_fee: Some(Msat(1_000)),
        status,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: BASE,
        operation_id: Some(op(7)),
        invoice: None,
    }
}

fn in_flight_send_obs() -> RawOpObservation {
    RawOpObservation {
        terminal: None,
        gateway: Some(GatewayUrl("https://gw".to_string())),
        fees: FeeBreakdown::default(),
        invoice_amount: Some(Msat(50_000)),
        payment_hash: Some([0xab; 32]),
    }
}

/// Replays the two raw sinks a fresh executor reaches after an SDK result returns.  It lets this
/// durability test exercise the core driver's final status transition without a real federation.
struct ReplayTerminalRawExecutor {
    journal: FedimintJournal,
    operation_id: OperationId,
    observation: RawOpObservation,
}

#[async_trait]
impl Executor for ReplayTerminalRawExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        if !self
            .journal
            .set_operation_artifact_if_attempt(
                &intent.idempotency_key,
                intent.attempt,
                self.operation_id,
                None,
            )
            .await?
        {
            return Err(ExecError::Retryable(
                "artifact replay lost its attempt".to_owned(),
            ));
        }
        if !self
            .journal
            .record_raw_observation_if_attempt(
                &intent.idempotency_key,
                intent.attempt,
                self.operation_id,
                &self.observation,
            )
            .await?
        {
            return Err(ExecError::Retryable(
                "terminal observation replay lost its attempt".to_owned(),
            ));
        }
        Ok(PerformOutcome::Done)
    }
}

#[tokio::test]
async fn raw_terminal_crash_window_redrive_converges_pending_and_executing_intents() {
    for initial_status in [IntentStatus::Pending, IntentStatus::Executing] {
        let journal = mem_ledger();
        let key = key(match initial_status {
            IntentStatus::Pending => "pay:crash-terminal-pending",
            IntentStatus::Executing => "pay:crash-terminal-executing",
            _ => unreachable!("test only drives non-terminal raw intents"),
        });
        let intent = repairable_pay_intent(key.clone(), initial_status, 0);
        journal.upsert(&intent).await.expect("seed raw intent");
        // Simulate the crash after the authoritative ledger write but before core's final
        // `set_status(Done)`: the durable intent is still owned by this exact attempt/op.
        assert!(journal
            .record_raw_observation_if_attempt(&key, 0, op(7), &terminal_send_obs(true, 42))
            .await
            .expect("seed terminal ledger conclusion"));
        let executor = ReplayTerminalRawExecutor {
            journal: journal.clone(),
            operation_id: op(7),
            observation: terminal_send_obs(true, 42),
        };
        let mut summary = ExecutionSummary::default();
        drive_intent_step(&journal, &executor, &intent, &mut summary)
            .await
            .expect("same-attempt raw re-drive converges");

        assert_eq!(
            journal
                .get(&key)
                .await
                .expect("read converged intent")
                .expect("intent exists")
                .status,
            IntentStatus::Done
        );
        assert_eq!(
            status_of(&journal, &key).await,
            OperationStatus::Succeeded,
            "the immutable terminal ledger conclusion was not rewritten"
        );
    }
}

#[tokio::test]
async fn raw_finalizer_completes_intent_and_ledger_from_one_observation() {
    let journal = mem_ledger();
    let key = key("pay:finalize");
    let operation_id = op(7);
    let intent = Intent {
        idempotency_key: key.clone(),
        attempt: 0,
        action: Action::Pay {
            from: fed(1),
            invoice: Invoice("lnbc1fixture".into()),
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            payment_hash: [0xab; 32],
            gateway: None,
        },
        max_fee: Some(Msat(1_000)),
        status: IntentStatus::Awaiting,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: BASE,
        operation_id: Some(operation_id),
        invoice: None,
    };
    journal.upsert(&intent).await.expect("seed raw pay intent");
    let mut oracle = MockOracle::default();
    oracle
        .observations
        .insert((fed(1), operation_id.0), terminal_send_obs(true, 42));

    let prepared = journal
        .prepare_raw_operation_terminal(
            &oracle,
            fed(1),
            operation_id,
            &key,
            0,
            RawOperationRole::Send,
        )
        .await
        .expect("prepare terminal observation");
    assert!(journal
        .finalize_raw_operation(&key, OperationStatus::Succeeded, None, prepared,)
        .await
        .expect("finalize")
        .is_empty());
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read intent")
            .expect("intent exists")
            .status,
        IntentStatus::Done
    );
    let row = op_of(&journal, &key).await;
    assert_eq!(row.status, OperationStatus::Succeeded);
    assert_eq!(row.fees.send_fee_quoted, Some(Msat(42)));
}

#[tokio::test]
async fn raw_finalizer_retries_after_total_observation_failure() {
    let journal = mem_ledger();
    let key = key("pay:prepare-observation-retry");
    let operation_id = op(7);
    journal
        .upsert(&repairable_pay_intent(
            key.clone(),
            IntentStatus::Awaiting,
            0,
        ))
        .await
        .expect("seed awaiting raw pay");

    let error = match journal
        .prepare_raw_operation_terminal(
            &empty_oracle(),
            fed(1),
            operation_id,
            &key,
            0,
            RawOperationRole::Send,
        )
        .await
    {
        Ok(_) => panic!("a total observe_op failure must surface to the awaiter"),
        Err(error) => error,
    };
    assert!(
        matches!(error, ExecError::Retryable(_)),
        "the absent op-log observation is retriable: {error:?}"
    );
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read intent")
            .expect("intent")
            .status,
        IntentStatus::Awaiting,
        "prepare must not release the reservation without an observation"
    );
    assert_eq!(
        status_of(&journal, &key).await,
        OperationStatus::Awaiting,
        "prepare must not terminalize the ledger without an observation"
    );

    let mut recovered_oracle = MockOracle::default();
    recovered_oracle
        .observations
        .insert((fed(1), operation_id.0), terminal_send_obs(true, 42));
    let prepared = journal
        .prepare_raw_operation_terminal(
            &recovered_oracle,
            fed(1),
            operation_id,
            &key,
            0,
            RawOperationRole::Send,
        )
        .await
        .expect("second observation succeeds");
    journal
        .finalize_raw_operation(&key, OperationStatus::Succeeded, None, prepared)
        .await
        .expect("second observation terminalizes atomically");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read intent")
            .expect("intent")
            .status,
        IntentStatus::Done
    );
    let settled = op_of(&journal, &key).await;
    assert_eq!(settled.status, OperationStatus::Succeeded);
    assert_eq!(settled.fees.send_fee_quoted, Some(Msat(42)));
}

#[tokio::test]
async fn raw_finalizer_rejects_an_in_flight_observation_without_releasing_reservation() {
    let journal = mem_ledger();
    let key = key("pay:prepare-in-flight");
    journal
        .upsert(&repairable_pay_intent(
            key.clone(),
            IntentStatus::Awaiting,
            0,
        ))
        .await
        .expect("seed awaiting raw pay");
    let mut oracle = MockOracle::default();
    oracle
        .observations
        .insert((fed(1), op(7).0), in_flight_send_obs());

    let error = match journal
        .prepare_raw_operation_terminal(&oracle, fed(1), op(7), &key, 0, RawOperationRole::Send)
        .await
    {
        Ok(_) => panic!("an in-flight observation cannot prepare a terminal commit"),
        Err(error) => error,
    };
    assert!(matches!(error, ExecError::Retryable(_)), "{error:?}");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read intent")
            .expect("intent")
            .status,
        IntentStatus::Awaiting
    );
    assert_eq!(status_of(&journal, &key).await, OperationStatus::Awaiting);
}

#[tokio::test]
async fn raw_finalizer_retries_a_correlation_noop_while_the_same_attempt_awaits() {
    let journal = mem_ledger();
    let key = key("pay:prepare-correlation-noop");
    journal
        .upsert(&repairable_pay_intent(
            key.clone(),
            IntentStatus::Awaiting,
            0,
        ))
        .await
        .expect("seed awaiting raw pay");

    // The prepared operation belongs to neither the intent nor its ledger row. Preparation
    // deliberately returns a no-op rather than trusting a foreign terminal observation; finalize
    // must turn that into a retry while this exact reservation still needs an awaiter.
    let prepared = journal
        .prepare_raw_operation_terminal(
            &empty_oracle(),
            fed(1),
            op(8),
            &key,
            0,
            RawOperationRole::Send,
        )
        .await
        .expect("foreign operation is a safe preparation no-op");
    let error = journal
        .finalize_raw_operation(&key, OperationStatus::Succeeded, None, prepared)
        .await
        .expect_err("same-attempt Awaiting must retain retry ownership");
    assert!(
        matches!(error, ExecError::Retryable(message) if message.contains("preparation no-op"))
    );
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read intent")
            .expect("intent")
            .status,
        IntentStatus::Awaiting
    );
    assert_eq!(status_of(&journal, &key).await, OperationStatus::Awaiting);
}

#[tokio::test]
async fn raw_finalizer_rejects_a_status_contradicting_its_observation() {
    let journal = mem_ledger();
    let key = key("pay:prepare-status-mismatch");
    journal
        .upsert(&repairable_pay_intent(
            key.clone(),
            IntentStatus::Awaiting,
            0,
        ))
        .await
        .expect("seed awaiting raw pay");
    let mut oracle = MockOracle::default();
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));
    let prepared = journal
        .prepare_raw_operation_terminal(&oracle, fed(1), op(7), &key, 0, RawOperationRole::Send)
        .await
        .expect("prepare observed success");

    let error = journal
        .finalize_raw_operation(
            &key,
            OperationStatus::Failed,
            Some("caller must not override the observer"),
            prepared,
        )
        .await
        .expect_err("caller status must agree with the observed terminal outcome");
    assert!(matches!(error, ExecError::Permanent(_)), "{error:?}");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read intent")
            .expect("intent")
            .status,
        IntentStatus::Awaiting
    );
    assert_eq!(status_of(&journal, &key).await, OperationStatus::Awaiting);
}

#[tokio::test]
async fn raw_finalizer_second_attempt_converges_ordinary_terminal_ledger_and_awaiting_intent() {
    let journal = mem_ledger();
    let key = key("pay:finalize-fence-retry");
    journal
        .upsert(&repairable_pay_intent(
            key.clone(),
            IntentStatus::Awaiting,
            0,
        ))
        .await
        .expect("seed awaiting raw pay");
    // The first finalizer committed the ordinary ledger terminal write and crashed before it
    // released the Awaiting intent.  The second finalizer must treat that exact terminal row as
    // its idempotently satisfied ledger half, not strand the reservation waiting for repair.
    assert!(journal
        .record_raw_observation_if_attempt(&key, 0, op(7), &terminal_send_obs(true, 42))
        .await
        .expect("seed terminal conclusion"));
    let mut oracle = MockOracle::default();
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));
    let prepared = journal
        .prepare_raw_operation_terminal(&oracle, fed(1), op(7), &key, 0, RawOperationRole::Send)
        .await
        .expect("prepare matching observation");

    journal
        .finalize_raw_operation(&key, OperationStatus::Succeeded, None, prepared)
        .await
        .expect("second finalizer atomically releases the matching intent");
    assert_eq!(
        journal
            .get(&key)
            .await
            .expect("read intent")
            .expect("intent")
            .status,
        IntentStatus::Done
    );
    assert!(
        !op_of(&journal, &key).await.repaired,
        "the converged terminal was ordinary, not a repair"
    );
}

#[tokio::test]
async fn op_less_terminal_failed_finalizer_adopts_the_operation_before_failing_pay() {
    let journal = mem_ledger();
    let key = key("pay:finalizer-adopts-failed-op");
    let operation_id = op(7);
    let mut intent = repairable_pay_intent(key.clone(), IntentStatus::Awaiting, 0);
    intent.operation_id = None;
    journal.upsert(&intent).await.expect("seed op-less raw pay");

    // The ledger/intent are correlated by this attempt's key, but the SDK identity was not
    // persisted before the awaiter observed its terminal failure.
    let mut oracle = MockOracle::default();
    oracle
        .by_key
        .insert((fed(1), intent.operation_correlation_key().0), operation_id);
    oracle
        .observations
        .insert((fed(1), operation_id.0), terminal_send_obs(false, 42));
    let prepared = journal
        .prepare_raw_operation_terminal(
            &oracle,
            fed(1),
            operation_id,
            &key,
            0,
            RawOperationRole::Send,
        )
        .await
        .expect("prepare failed terminal observation");
    journal
        .finalize_raw_operation(
            &key,
            OperationStatus::Failed,
            Some("SDK reported failed"),
            prepared,
        )
        .await
        .expect("finalize failed terminal observation");

    let final_intent = journal
        .get(&key)
        .await
        .expect("read final intent")
        .expect("intent exists");
    assert_eq!(final_intent.status, IntentStatus::Failed);
    assert_eq!(
        final_intent.operation_id,
        Some(operation_id),
        "the actor's manual-retry guard must see the committed failed Pay operation"
    );
}

#[tokio::test]
async fn stale_prepared_raw_finalizer_cannot_terminalize_retry_attempt_n_plus_one() {
    let journal = mem_ledger();
    let key = key("pay:stale-prepared-finalizer");
    let operation_id = op(7);
    let first = repairable_pay_intent(key.clone(), IntentStatus::Awaiting, 0);
    journal.upsert(&first).await.expect("seed first attempt");
    let mut oracle = MockOracle::default();
    oracle
        .observations
        .insert((fed(1), operation_id.0), terminal_send_obs(true, 42));
    let prepared = journal
        .prepare_raw_operation_terminal(
            &oracle,
            fed(1),
            operation_id,
            &key,
            0,
            RawOperationRole::Send,
        )
        .await
        .expect("prepare first attempt");

    journal
        .set_status(&key, 0, IntentStatus::Failed, Some("operator retries"))
        .await
        .expect("terminalize first attempt");
    let mut retry = repairable_pay_intent(key.clone(), IntentStatus::Pending, 1);
    retry.operation_id = None;
    journal
        .retry_failed_intent(&retry)
        .await
        .expect("start retry before stale finalization");

    journal
        .finalize_raw_operation(&key, OperationStatus::Succeeded, None, prepared)
        .await
        .expect("stale prepared finalizer benignly no-ops");
    let current = journal
        .get(&key)
        .await
        .expect("read")
        .expect("retry exists");
    assert_eq!(current.attempt, 1);
    assert_eq!(current.status, IntentStatus::Pending);
    assert_eq!(current.operation_id, None);
    let row = op_of(&journal, &key).await;
    assert!(!row.status.is_terminal());
    assert!(matches!(row.kind, OperationKind::Pay { op_id: None, .. }));
}

fn terminal_recv_obs(recv_fee: u64) -> RawOpObservation {
    RawOpObservation {
        terminal: Some(RawTerminal {
            succeeded: true,
            error: None,
        }),
        gateway: Some(GatewayUrl("https://gw".to_string())),
        fees: FeeBreakdown {
            fee_cap: None,
            receive_fee: Some(Msat(recv_fee)),
            send_fee_quoted: None,
        },
        invoice_amount: Some(Msat(1_000)),
        payment_hash: None,
    }
}

// --- §9.3 standalone recording mechanics ---

#[tokio::test]
async fn seq_is_monotonic_and_history_is_newest_first() {
    let j = mem_ledger();
    for (i, k) in ["pay:aa:1", "pay:aa:2", "pay:aa:3"].iter().enumerate() {
        j.record_started(
            &key(k),
            pay_kind(fed(1)),
            Actor::User,
            ReasonCode::UserInitiated,
            BASE + i as u64,
            None,
        )
        .await
        .expect("record_started");
    }
    let hist = j.history(10, None).await.expect("history");
    assert_eq!(
        hist.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![2, 1, 0],
        "newest-first, seq monotonic from 0"
    );
    assert_eq!(
        hist.iter()
            .map(|r| r.correlation_key.0.as_str())
            .collect::<Vec<_>>(),
        vec!["pay:aa:3", "pay:aa:2", "pay:aa:1"]
    );

    // `before_seq` + `limit`: only the row before seq 2, limited to 1.
    let page = j.history(1, Some(2)).await.expect("page");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].seq, 1);
}

#[tokio::test]
async fn record_started_is_idempotent_per_key_under_replay() {
    let j = mem_ledger();
    let k = key("recv:aa:1");
    j.record_started(
        &k,
        recv_kind(fed(1), Msat(1_000)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("first");
    // A re-drive of the same key (even with different content) never appends or overwrites.
    j.record_started(
        &k,
        recv_kind(fed(1), Msat(9_999)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("replay");

    assert_eq!(j.history(10, None).await.expect("history").len(), 1);
    match op_of(&j, &k).await.kind {
        OperationKind::Receive {
            amount_invoiced, ..
        } => assert_eq!(amount_invoiced, Msat(1_000), "the first row stands"),
        other => panic!("kind changed: {other:?}"),
    }
}

#[tokio::test]
async fn record_update_advances_started_to_awaiting_then_terminal_is_immutable() {
    let j = mem_ledger();
    let k = key("pay:aa:1");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");

    // Post-parse amount + hash: same status.
    j.record_update(
        &k,
        RawOpUpdate {
            invoice_amount: Some(Msat(50_000)),
            payment_hash: Some([0xab; 32]),
            ..Default::default()
        },
    )
    .await
    .expect("parse update");
    assert_eq!(status_of(&j, &k).await, OperationStatus::Started);

    // Op id: advances Started -> Awaiting.
    j.record_update(
        &k,
        RawOpUpdate {
            op_id: Some(op(7)),
            ..Default::default()
        },
    )
    .await
    .expect("op-id update");
    assert_eq!(status_of(&j, &k).await, OperationStatus::Awaiting);

    // Terminal carries the final fee enrichment.
    j.record_terminal(
        &k,
        OperationStatus::Succeeded,
        BASE,
        None,
        Some(RawOpUpdate {
            fees: Some(fees_send(42)),
            ..Default::default()
        }),
    )
    .await
    .expect("terminal");
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Succeeded);
    assert_eq!(rec.fees.send_fee_quoted, Some(Msat(42)));

    // A later terminal write is a no-op (terminal-immutable).
    j.record_terminal(&k, OperationStatus::Failed, BASE, Some("late"), None)
        .await
        .expect("no-op terminal");
    assert_eq!(status_of(&j, &k).await, OperationStatus::Succeeded);
}

#[tokio::test]
async fn delayed_legacy_record_update_from_attempt_n_rejects_retry_n_plus_one_unchanged() {
    let j = mem_ledger();
    let k = key("pay:legacy-update-retry");
    let first = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    j.upsert(&first).await.expect("seed attempt N");
    j.set_status(&k, 0, IntentStatus::Failed, Some("attempt N failed"))
        .await
        .expect("terminalize attempt N");
    let mut retry = repairable_pay_intent(k.clone(), IntentStatus::Pending, 1);
    retry.operation_id = None;
    j.retry_failed_intent(&retry)
        .await
        .expect("start attempt N+1");

    let before_intent = j.get(&k).await.expect("read retry").expect("retry exists");
    let before_ledger = op_of(&j, &k).await;
    let error = j
        .record_update(
            &k,
            RawOpUpdate {
                op_id: Some(op(7)),
                ..Default::default()
            },
        )
        .await
        .expect_err("delayed legacy update must not mutate retry N+1");
    assert!(
        matches!(&error, ExecError::Permanent(message) if message.contains("record_update")
            && message.contains("attempt-fenced")),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        j.get(&k).await.expect("read retry").expect("retry exists"),
        before_intent,
        "legacy attempt-N update must leave the N+1 intent unchanged"
    );
    assert_eq!(
        op_of(&j, &k).await,
        before_ledger,
        "legacy attempt-N update must leave the N+1 ledger row unchanged"
    );
}

#[tokio::test]
async fn delayed_legacy_record_terminal_from_attempt_n_rejects_retry_n_plus_one_unchanged() {
    let j = mem_ledger();
    let k = key("pay:legacy-terminal-retry");
    let first = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    j.upsert(&first).await.expect("seed attempt N");
    j.set_status(&k, 0, IntentStatus::Failed, Some("attempt N failed"))
        .await
        .expect("terminalize attempt N");
    let mut retry = repairable_pay_intent(k.clone(), IntentStatus::Pending, 1);
    retry.operation_id = None;
    j.retry_failed_intent(&retry)
        .await
        .expect("start attempt N+1");

    let before_intent = j.get(&k).await.expect("read retry").expect("retry exists");
    let before_ledger = op_of(&j, &k).await;
    let error = j
        .record_terminal(
            &k,
            OperationStatus::Succeeded,
            BASE + 1,
            None,
            Some(RawOpUpdate {
                fees: Some(fees_send(42)),
                ..Default::default()
            }),
        )
        .await
        .expect_err("delayed legacy terminal must not mutate retry N+1");
    assert!(
        matches!(&error, ExecError::Permanent(message) if message.contains("record_terminal")
            && message.contains("attempt-fenced")),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        j.get(&k).await.expect("read retry").expect("retry exists"),
        before_intent,
        "legacy attempt-N terminal must leave the N+1 intent unchanged"
    );
    assert_eq!(
        op_of(&j, &k).await,
        before_ledger,
        "legacy attempt-N terminal must leave the N+1 ledger row unchanged"
    );
}

#[tokio::test]
async fn tick_row_started_then_terminal_carries_counts() {
    let j = mem_ledger();
    let k = key("tick:5:n");
    j.record_tick_started(&k, Occurrence(5), BASE)
        .await
        .expect("tick started");
    assert_eq!(status_of(&j, &k).await, OperationStatus::Started);

    j.record_tick_terminal(
        &k,
        Some((3, 2, 1)),
        OperationStatus::Succeeded,
        None,
        BASE + 1,
    )
    .await
    .expect("tick terminal");
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Succeeded);
    match rec.kind {
        OperationKind::Tick {
            occurrence,
            decisions,
            performed,
            failed,
        } => {
            assert_eq!(occurrence, Occurrence(5));
            assert_eq!((decisions, performed, failed), (3, 2, 1));
        }
        other => panic!("kind changed: {other:?}"),
    }
}

#[tokio::test]
async fn record_refusals_are_deduped_terminal_rows() {
    let j = mem_ledger();
    let decisions = vec![refuse_dec(
        fed(1),
        ReasonCode::OverCap,
        "refuse:over_cap:0101:0",
    )];
    j.record_refusals(&decisions, Occurrence(0), BASE)
        .await
        .expect("refusals");
    // Re-tick of the same occurrence reuses the same `refuse:` key -> one row (dedup via 0x06).
    j.record_refusals(&decisions, Occurrence(0), BASE)
        .await
        .expect("re-tick refusals");

    let hist = j.history(10, None).await.expect("history");
    assert_eq!(hist.len(), 1);
    let rec = &hist[0];
    assert_eq!(rec.status, OperationStatus::Succeeded);
    assert!(matches!(rec.kind, OperationKind::Refusal { .. }));
    assert_eq!(rec.reason, ReasonCode::OverCap);
    assert_eq!(
        rec.actor,
        Actor::Agent {
            occurrence: Occurrence(0)
        }
    );
}

#[tokio::test]
async fn record_refusals_persist_diagnostics_across_serialization() {
    // The point of the diagnostics: a refusal's figures reach the journal (serde_json over the
    // raw byte store — a real serialize/deserialize), so it is reconstructible after a restart
    // and not only via live tracing. This is the acceptance signal for the feature.
    let j = mem_ledger();
    let diagnostics = RefusalDiagnostics {
        source: Some(fed(2)),
        want: Some(Msat(50_000)),
        available: Some(Msat(9_500)),
        source_spendable: Some(Msat(10_000)),
        max_fee: Some(Msat(500)),
        max_fee_bps: Some(100),
        cap_room: Some(Msat(40_000)),
        amount: Some(Msat(0)),
        conflict_suppressed: true,
        min_move: Some(Msat(0)),
    };
    let decision = AllocatorDecision {
        action: Action::RefuseInflow {
            fed: fed(1),
            reason: ReasonCode::SpendingBelowTarget,
            diagnostics,
        },
        reason: ReasonCode::SpendingBelowTarget,
        occurrence: Occurrence(0),
        idempotency_key: IdempotencyKey("refuse:spending_below_target:0101:0".into()),
    };
    j.record_refusals(std::slice::from_ref(&decision), Occurrence(0), BASE)
        .await
        .expect("refusals");

    let hist = j.history(10, None).await.expect("history");
    assert_eq!(hist.len(), 1);
    match &hist[0].kind {
        OperationKind::Refusal {
            fed: f,
            diagnostics: read_back,
        } => {
            assert_eq!(*f, fed(1));
            // `RefusalDiagnostics` compares equal always, so assert every field explicitly.
            assert_eq!(read_back.source, Some(fed(2)));
            assert_eq!(read_back.want, Some(Msat(50_000)));
            assert_eq!(read_back.available, Some(Msat(9_500)));
            assert_eq!(read_back.source_spendable, Some(Msat(10_000)));
            assert_eq!(read_back.max_fee, Some(Msat(500)));
            assert_eq!(read_back.max_fee_bps, Some(100));
            assert_eq!(read_back.cap_room, Some(Msat(40_000)));
            assert_eq!(read_back.amount, Some(Msat(0)));
            assert!(read_back.conflict_suppressed);
            assert_eq!(read_back.min_move, Some(Msat(0)));
        }
        other => panic!("expected a refusal row, got {other:?}"),
    }
}

#[test]
fn refusal_diagnostics_missing_observational_fields_decode_to_defaults() {
    // Refusal rows are re-decoded on every `history` read. Rows persisted before
    // `max_fee_bps`/`conflict_suppressed` existed omit those keys, so preserve their distinct
    // legacy defaults directly.
    let mut json = serde_json::to_value(RefusalDiagnostics {
        source_spendable: Some(Msat(10_000)),
        max_fee_bps: Some(100),
        ..Default::default()
    })
    .expect("serialize");
    let object = json.as_object_mut().expect("object");
    object.remove("max_fee_bps");
    object.remove("conflict_suppressed");
    let decoded: RefusalDiagnostics =
        serde_json::from_value(json).expect("legacy refusal row (no max_fee_bps) still decodes");
    assert_eq!(decoded.max_fee_bps, None);
    assert!(!decoded.conflict_suppressed);
    assert_eq!(decoded.source_spendable, Some(Msat(10_000)));
}

#[tokio::test]
async fn conflict_dropped_tick_row_records_an_observable_emitted_zero() {
    let journal = mem_ledger();
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(50_000),
            fee_cap: Msat(500),
            gateway: None,
        },
        reason: ReasonCode::StandbyBelowTarget,
        occurrence: Occurrence(7),
        idempotency_key: key("move:held-conflict"),
    };
    journal
        .record_tick_dropped_refusal(
            &decision,
            Occurrence(7),
            BASE,
            "decision move:held-conflict conflicts with allocator work already in flight",
            true,
        )
        .await
        .expect("record conflict drop");

    let history = journal.history(10, None).await.expect("history");
    let OperationKind::Refusal { diagnostics, .. } = &history[0].kind else {
        panic!("expected refusal row: {:?}", history[0]);
    };
    assert_eq!(diagnostics.amount, Some(Msat(0)));
    assert!(diagnostics.conflict_suppressed);
}

// --- §9.2 journal-integrated writes (same dbtx as the intent) ---

#[tokio::test]
async fn upsert_writes_the_ledger_row_in_the_same_dbtx() {
    let j = mem_ledger();
    let intent = move_intent("move:0102:0", IntentStatus::Pending);
    j.upsert(&intent).await.expect("upsert");

    // The intent row AND its ledger row are both visible after the single commit.
    assert!(j.get(&intent.idempotency_key).await.expect("get").is_some());
    let rec = op_of(&j, &intent.idempotency_key).await;
    assert_eq!(rec.status, OperationStatus::Started);
    assert_eq!(rec.reason, ReasonCode::UserInitiated);
    assert_eq!(rec.actor, Actor::User);
    assert!(matches!(
        rec.kind,
        OperationKind::Move {
            evacuation: false,
            ..
        }
    ));
    assert_eq!(rec.fees.fee_cap, Some(Msat(2_000)));
}

#[tokio::test]
async fn set_status_failed_records_the_executor_error_on_the_ledger_row() {
    let j = mem_ledger();
    let intent = move_intent("move:0102:1", IntentStatus::Pending);
    j.upsert(&intent).await.expect("upsert");
    j.set_status(
        &intent.idempotency_key,
        0,
        IntentStatus::Failed,
        Some("cap exceeded"),
    )
    .await
    .expect("set_status");

    let rec = op_of(&j, &intent.idempotency_key).await;
    assert_eq!(rec.status, OperationStatus::Failed);
    assert_eq!(rec.error.as_deref(), Some("cap exceeded"));
}

#[tokio::test]
async fn set_status_failed_falls_back_to_move_record_outcome() {
    let j = mem_ledger();
    let intent = move_intent("move:0102:2", IntentStatus::Pending);
    j.upsert(&intent).await.expect("upsert");
    let mut mv = move_record_for("move:0102:2");
    mv.outcome = Some("stranded: debited, not credited".to_string());
    assert!(j
        .put_move_if_attempt(&intent.idempotency_key, intent.attempt, &mv)
        .await
        .expect("put_move"));

    // No executor string -> the ledger error falls back to the MoveRecord outcome (§9.2).
    j.set_status(&intent.idempotency_key, 0, IntentStatus::Failed, None)
        .await
        .expect("set_status");
    assert_eq!(
        op_of(&j, &intent.idempotency_key).await.error.as_deref(),
        Some("stranded: debited, not credited")
    );
}

#[tokio::test]
async fn ledger_refreshes_fees_and_op_ids_from_the_move_row_on_non_terminal_writes() {
    let j = mem_ledger();
    let intent = move_intent("move:0102:3", IntentStatus::Pending);
    j.upsert(&intent).await.expect("upsert");
    // The executor persists the move record (recv/send op, gateway, fee quotes) BEFORE the flip.
    assert!(j
        .put_move_if_attempt(
            &intent.idempotency_key,
            intent.attempt,
            &move_record_for("move:0102:3"),
        )
        .await
        .expect("put_move"));
    // A NON-terminal status write must reflect the in-flight metadata (§9.2).
    j.set_status(&intent.idempotency_key, 0, IntentStatus::Awaiting, None)
        .await
        .expect("set_status");

    let rec = op_of(&j, &intent.idempotency_key).await;
    assert_eq!(rec.status, OperationStatus::Awaiting);
    assert_eq!(rec.fees.receive_fee, Some(Msat(150)));
    assert_eq!(rec.fees.send_fee_quoted, Some(Msat(250)));
    match rec.kind {
        OperationKind::Move {
            send_op,
            recv_op,
            gateway,
            ..
        } => {
            assert_eq!(send_op, Some(op(9)));
            assert_eq!(recv_op, Some(op(7)));
            assert_eq!(gateway, Some(GatewayUrl("https://gw.example".to_string())));
        }
        other => panic!("kind changed: {other:?}"),
    }
}

/// ADR-0029 LEDGER AGREEMENT — an evacuation the sizing search clamped must leave the ledger
/// row reporting the pair it EXECUTED: the net it moved, and the cap recomputed at that net.
///
/// Both or neither. A row reading `amount = planned, fee_cap = enforced` is internally FALSE —
/// an auditor recomputing the cap from the displayed amount derives a different number — so
/// refreshing one alone makes the row worse than leaving both planned.
#[tokio::test]
async fn a_clamped_evacuation_row_reports_the_cap_and_the_amount_it_executed() {
    let j = mem_ledger();
    let k = "evac:0102:0";
    let intent = evacuation_intent(k, IntentStatus::Pending);
    j.upsert(&intent).await.expect("upsert");

    // What sizing did: clamp the net to what the dying source can fund, then recompute the cap
    // AT that net — `apply_evacuation_sizing`'s two writes, which always move together.
    let executed = Msat(1_000_000);
    let enforced = EVAC_CAP.at(executed);
    assert_ne!(
        enforced,
        EVAC_CAP.at(EVAC_PLANNED),
        "planned and enforced caps must differ, or neither assertion below distinguishes them"
    );
    let mut mv = move_record_for(k);
    mv.amount = executed;
    mv.fee_cap = enforced;
    assert!(j
        .put_move_if_attempt(&intent.idempotency_key, intent.attempt, &mv)
        .await
        .expect("put_move"));

    j.set_status(&intent.idempotency_key, 0, IntentStatus::Awaiting, None)
        .await
        .expect("set_status");

    let rec = op_of(&j, &intent.idempotency_key).await;
    assert_eq!(
        rec.fees.fee_cap,
        Some(enforced),
        "the row must report the cap the move was authorised under, not the planned {:?}",
        intent.max_fee
    );
    match rec.kind {
        OperationKind::Move { amount, .. } => assert_eq!(
            amount, executed,
            "the row must report the net that moved, not the planned ask"
        ),
        other => panic!("kind changed: {other:?}"),
    }
}

/// The same agreement after CACHE-LOSS RECONSTRUCTION. With the journal's cached move row gone,
/// `assemble_move_record` recovers the executed pair from the committed op metadata while its
/// params still carry the PLANNED pair — so this is the path where a ledger row that reads its
/// amount from the intent, and its cap from nowhere, disagrees with the move it describes.
#[tokio::test]
async fn a_reconstructed_evacuation_row_reports_the_committed_cap_and_amount() {
    let j = mem_ledger();
    let k = "evac:0102:1";
    let intent = evacuation_intent(k, IntentStatus::Pending);
    j.upsert(&intent).await.expect("upsert");

    let executed = Msat(1_000_000);
    let enforced = EVAC_CAP.at(executed);
    // The receive leg committed at the sized-down net. The cache is GONE (`None`), so reassembly
    // has only this artifact and the intent's planned params to work from.
    let mv = assemble_move_record(
        evacuation_params(k),
        &[OpArtifact {
            move_id: key(k),
            leg: Leg::Receive,
            op_id: op(7),
            amount: executed,
            fee_cap: Some(enforced),
            invoice: Some(Invoice("lnbc1".to_string())),
        }],
        None,
    );
    assert_eq!(
        mv.amount, executed,
        "reassembly must recover the executed net"
    );
    assert_eq!(
        mv.fee_cap, enforced,
        "reassembly must recover the enforced cap"
    );
    assert!(j
        .put_move_if_attempt(&intent.idempotency_key, intent.attempt, &mv)
        .await
        .expect("put_move"));

    j.set_status(&intent.idempotency_key, 0, IntentStatus::Awaiting, None)
        .await
        .expect("set_status");

    let rec = op_of(&j, &intent.idempotency_key).await;
    assert_eq!(
        rec.fees.fee_cap,
        Some(enforced),
        "a reconstructed row must report the recovered cap, not the intent's planned one"
    );
    match rec.kind {
        OperationKind::Move { amount, .. } => assert_eq!(
            amount, executed,
            "a reconstructed row must report the recovered net, not the intent's planned ask"
        ),
        other => panic!("kind changed: {other:?}"),
    }
}

/// A move row with NO committed leg is a pre-operation DRAFT, and the ledger must not restate
/// itself from one. `size_fresh_evacuation` re-sizes the draft from the intent on every
/// pre-receive pass, and the pre-mint cap re-check persists it and then returns `Retryable`
/// (`executor.rs`) — so a row stamped from it would report an amount and a cap that NO operation
/// ever ran under.
///
/// This drives the TERMINAL transition on purpose. A `Failed` write is where the harm is
/// permanent — terminal rows are immutable, so a pair stamped here can never be corrected — and
/// it is reachable over a persisted draft: `clamp_desired_to_cap_room` returns `Permanent` for an
/// evacuation into a destination already at its cap, on a tick after an earlier tick persisted a
/// sized draft. A same-status `Pending` rewrite would exercise the one case where the stamp costs
/// nothing and would also pass if no write happened at all.
#[tokio::test]
async fn a_draft_move_row_does_not_freeze_its_pair_onto_the_terminal_row() {
    let j = mem_ledger();
    let k = "evac:0102:2";
    let intent = evacuation_intent(k, IntentStatus::Pending);
    j.upsert(&intent).await.expect("upsert");

    // Sized, persisted, and then refused BEFORE minting: no invoice, no op ids on either leg.
    let mut mv = move_record_for(k);
    mv.amount = Msat(1_000_000);
    mv.fee_cap = EVAC_CAP.at(Msat(1_000_000));
    mv.invoice = None;
    mv.recv_op = None;
    mv.send_op = None;
    mv.phase = MovePhase::Created;
    assert!(j
        .put_move_if_attempt(&intent.idempotency_key, intent.attempt, &mv)
        .await
        .expect("put_move"));

    j.set_status(
        &intent.idempotency_key,
        0,
        IntentStatus::Failed,
        Some("destination is already at its cap"),
    )
    .await
    .expect("set_status");

    let rec = op_of(&j, &intent.idempotency_key).await;
    // The write really happened — otherwise "the row is unchanged" would be vacuously true.
    assert_eq!(rec.status, OperationStatus::Failed, "the row is terminal");
    assert_eq!(
        rec.fees.fee_cap,
        Some(EVAC_CAP.at(EVAC_PLANNED)),
        "a draft with no committed leg must not freeze its cap onto the terminal row"
    );
    match rec.kind {
        OperationKind::Move { amount, .. } => assert_eq!(
            amount, EVAC_PLANNED,
            "a draft with no committed leg must not freeze its amount onto the terminal row"
        ),
        other => panic!("kind changed: {other:?}"),
    }
}

// --- §9.3 scans: resolve by key AND seq; poison tolerance ---

#[tokio::test]
async fn operation_resolves_by_key_and_by_seq() {
    let j = mem_ledger();
    let k = key("pay:aa:1");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    let by_key = j
        .operation(&OperationRef::Key(k.clone()))
        .await
        .expect("by key")
        .expect("exists");
    let by_seq = j
        .operation(&OperationRef::Seq(by_key.seq))
        .await
        .expect("by seq")
        .expect("exists");
    assert_eq!(by_key, by_seq);
    assert!(j
        .operation(&OperationRef::Key(key("no-such-key")))
        .await
        .expect("miss")
        .is_none());
    assert!(j
        .operation(&OperationRef::Seq(999))
        .await
        .expect("miss")
        .is_none());
}

#[tokio::test]
async fn ledger_scans_skip_poison_rows() {
    let db = MemDatabase::new().into_database();
    let j = FedimintJournal::with_clock(db.clone(), clock_base);
    let k = key("pay:aa:1");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");

    // Inject a corrupt 0x05 ledger row directly.
    let app = db.with_prefix(vec![0x00]);
    let mut dbtx = app.begin_transaction().await;
    let mut poison = vec![0x05];
    poison.extend_from_slice(&999u64.to_be_bytes());
    dbtx.raw_insert_bytes(&poison, b"not valid json")
        .await
        .expect("insert poison");
    dbtx.commit_tx_result().await.expect("commit");

    // The scan skips it and returns the healthy row.
    let hist = j.history(10, None).await.expect("history skips poison");
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].correlation_key.0, "pay:aa:1");
}

// --- §10.1 window mechanics (journal level) ---

#[tokio::test]
async fn synchronous_failure_leaves_a_durable_failed_row() {
    let j = mem_ledger();
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    // The malformed-invoice / synchronous-error path terminalizes with the REAL error.
    j.record_terminal(
        &k,
        OperationStatus::Failed,
        BASE,
        Some("parsing invoice: invalid checksum"),
        None,
    )
    .await
    .expect("fail");

    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Failed);
    assert!(
        !rec.repaired,
        "an authoritative synchronous failure is not a soft repair"
    );
    assert_eq!(
        rec.error.as_deref(),
        Some("parsing invoice: invalid checksum")
    );
    assert_eq!(j.history(10, None).await.expect("history").len(), 1);
}

#[tokio::test]
async fn settled_dedup_terminal_carries_definitive_fees() {
    let j = mem_ledger();
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    // A dedup'd re-pay whose op already settled: the awaiter terminalizes Succeeded carrying
    // the definitive fees read from the op meta.
    j.record_terminal(
        &k,
        OperationStatus::Succeeded,
        BASE,
        None,
        Some(RawOpUpdate {
            op_id: Some(op(7)),
            invoice_amount: Some(Msat(50_000)),
            fees: Some(fees_send(88)),
            ..Default::default()
        }),
    )
    .await
    .expect("already-paid terminal");

    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Succeeded);
    assert_eq!(rec.fees.send_fee_quoted, Some(Msat(88)));
    match rec.kind {
        OperationKind::Pay {
            op_id,
            invoice_amount,
            ..
        } => {
            assert_eq!(op_id, Some(op(7)));
            assert_eq!(invoice_amount, Some(Msat(50_000)));
        }
        other => panic!("kind changed: {other:?}"),
    }
}

// --- §10.3 reconcile repair ---

#[tokio::test]
async fn repair_soft_fails_a_raw_row_with_no_op_after_1h() {
    // Row stamped at BASE; the journal clock is BASE + 2h -> age > 1h -> negative inference.
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");

    let summary = j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert_eq!(summary.repaired, 1);
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Failed);
    assert!(
        rec.repaired,
        "a negative inference is a defeasible (soft) repair"
    );
    assert_eq!(rec.error.as_deref(), Some("never reached the federation"));
}

#[tokio::test]
async fn raw_negative_repair_keeps_the_intent_retriable_and_the_ledger_defeasible() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("pay:0101:soft-intent");
    j.upsert(&Intent {
        idempotency_key: k.clone(),
        attempt: 0,
        action: Action::Pay {
            from: fed(1),
            invoice: Invoice("lnbc1repairfixture".into()),
            amount: Msat(10_000),
            fee_cap: Msat(100),
            payment_hash: [0xab; 32],
            gateway: None,
        },
        max_fee: Some(Msat(100)),
        status: IntentStatus::Pending,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: BASE,
        operation_id: None,
        invoice: None,
    })
    .await
    .expect("seed raw intent and ledger row");

    let first = j
        .repair_ledger(&empty_oracle())
        .await
        .expect("first repair");
    assert_eq!(first.repaired, 1);

    let row = op_of(&j, &k).await;
    assert_eq!(row.status, OperationStatus::Failed);
    assert!(row.repaired, "negative evidence must remain defeasible");
    assert_eq!(
        j.get(&k)
            .await
            .expect("read intent")
            .expect("intent exists")
            .status,
        IntentStatus::Pending,
        "pass 1 no-evidence soft repair must leave the operation eligible for authoritative retry"
    );
    let second = j
        .repair_ledger(&empty_oracle())
        .await
        .expect("second terminal-row retry pass");
    assert_eq!(
        second.repaired, 0,
        "the existing soft terminal is accounting history, not a second repair"
    );
    assert_eq!(
        j.get(&k)
            .await
            .expect("read intent")
            .expect("intent exists")
            .status,
        IntentStatus::Pending,
        "terminal-row retry must never sink the deliberate RAW_NEVER_REACHED soft repair"
    );
}

#[tokio::test]
async fn record_update_op_id_supersedes_soft_failed_raw_row_to_awaiting() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert!(op_of(&j, &k).await.repaired);

    j.record_update(
        &k,
        RawOpUpdate {
            op_id: Some(op(7)),
            ..Default::default()
        },
    )
    .await
    .expect("op-id update");

    let awaiting = op_of(&j, &k).await;
    assert_eq!(awaiting.status, OperationStatus::Awaiting);
    assert!(
        !awaiting.repaired,
        "authoritative op-id evidence clears the soft repair"
    );
    assert_eq!(
        awaiting.error, None,
        "the stale repair diagnostic is cleared"
    );
    match awaiting.kind {
        OperationKind::Pay { op_id, .. } => assert_eq!(op_id, Some(op(7))),
        other => panic!("kind changed: {other:?}"),
    }

    j.record_terminal(
        &k,
        OperationStatus::Succeeded,
        BASE + 3 * HOUR,
        None,
        Some(RawOpUpdate {
            fees: Some(fees_send(42)),
            ..Default::default()
        }),
    )
    .await
    .expect("terminal");
    let terminal = op_of(&j, &k).await;
    assert_eq!(terminal.status, OperationStatus::Succeeded);
    assert_eq!(terminal.fees.send_fee_quoted, Some(Msat(42)));
}

#[tokio::test]
async fn record_update_parse_enrichment_supersedes_soft_failed_raw_row_to_started() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert!(op_of(&j, &k).await.repaired);

    j.record_update(
        &k,
        RawOpUpdate {
            invoice_amount: Some(Msat(50_000)),
            payment_hash: Some([0xab; 32]),
            ..Default::default()
        },
    )
    .await
    .expect("parse update");

    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Started);
    assert!(
        !rec.repaired,
        "authoritative parse evidence clears the soft repair without freezing the row"
    );
    assert_eq!(rec.error, None);
    match rec.kind {
        OperationKind::Pay {
            invoice_amount,
            payment_hash,
            ..
        } => {
            assert_eq!(invoice_amount, Some(Msat(50_000)));
            assert_eq!(payment_hash, Some([0xab; 32]));
        }
        other => panic!("kind changed: {other:?}"),
    }
}

#[tokio::test]
async fn repair_defers_a_fresh_row_within_the_hour_forward_jump() {
    // SKEWED CLOCK: the clock jumped forward 30m, but the row is still < 1h old -> deferred.
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_30m);
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");

    let summary = j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert_eq!(summary.repaired, 0);
    assert_eq!(
        status_of(&j, &k).await,
        OperationStatus::Started,
        "a row still within the hour is deferred despite the forward jump"
    );
}

#[tokio::test]
async fn repair_backfills_op_id_from_the_correlation_key() {
    let j = mem_ledger();
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    let mut oracle = MockOracle::default();
    oracle.by_key.insert((fed(1), k.0.clone()), op(7));
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));

    let summary = j.repair_ledger(&oracle).await.expect("repair");
    assert_eq!(summary.repaired, 1);
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Succeeded);
    assert!(
        !rec.repaired,
        "found by its OWN key -> authoritative, not repaired"
    );
    assert_eq!(rec.fees.send_fee_quoted, Some(Msat(42)));
    match rec.kind {
        OperationKind::Pay { op_id, .. } => assert_eq!(op_id, Some(op(7))),
        other => panic!("kind changed: {other:?}"),
    }
}

#[tokio::test]
async fn repair_uses_captured_fence_across_observation_before_sink_cas() {
    let j = mem_ledger();
    let k = key("pay:repair-observation-fence");
    let mut first = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    let mut replacement = repairable_pay_intent(k.clone(), IntentStatus::Pending, 1);
    first.operation_id = None;
    replacement.operation_id = None;
    j.upsert(&first).await.expect("seed first attempt");
    let correct_correlation_queries = Arc::new(AtomicUsize::new(0));
    let oracle = RetryAfterObservationOracle {
        journal: j.clone(),
        replacement,
        expected_correlation: first.operation_correlation_key(),
        observation: terminal_send_obs(true, 42),
        correct_correlation_queries: Arc::clone(&correct_correlation_queries),
    };

    j.repair_ledger(&oracle)
        .await
        .expect("stale observation is a benign no-op");

    assert_eq!(
        correct_correlation_queries.load(Ordering::SeqCst),
        1,
        "the op-log lookup used attempt N's captured correlation key"
    );
    let current = j.get(&k).await.expect("read").expect("replacement exists");
    assert_eq!(current.attempt, 1);
    assert_eq!(current.status, IntentStatus::Pending);
    assert_eq!(current.operation_id, None);
    let current_row = op_of(&j, &k).await;
    assert!(
        !current_row.status.is_terminal(),
        "N's witness must not terminalize N+1 after the oracle replaced the ledger row"
    );
    assert!(matches!(
        current_row.kind,
        OperationKind::Pay { op_id: None, .. }
    ));
}

#[tokio::test]
async fn repair_sink_cas_cannot_terminalize_retry_started_inside_sink() {
    let j = mem_ledger();
    let k = key("pay:repair-attempt-cas");
    let first = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    j.upsert(&first).await.expect("seed first attempt");
    let mut replacement = repairable_pay_intent(k.clone(), IntentStatus::Pending, 1);
    replacement.operation_id = None;
    let mut oracle = MockOracle::default();
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));
    let sink = RetryDuringSink {
        journal: j.clone(),
        replacement,
    };

    j.repair_ledger_with_terminal_sink(&oracle, &sink)
        .await
        .expect("old repair is a benign no-op against retry");

    let current = j.get(&k).await.expect("read").expect("replacement exists");
    assert_eq!(current.attempt, 1);
    assert_eq!(current.status, IntentStatus::Pending);
    assert_eq!(current.operation_id, None);
    let current_row = op_of(&j, &k).await;
    assert!(
        !current_row.status.is_terminal(),
        "old N observation must not terminalize current N+1 ledger row"
    );
    assert!(matches!(
        current_row.kind,
        OperationKind::Pay { op_id: None, .. }
    ));
}

#[tokio::test]
async fn repair_sink_fence_rejects_same_attempt_soft_terminal_superseded_before_cas() {
    let j = mem_ledger();
    let k = key("pay:repair-same-attempt-supersession");
    let mut intent = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    intent.operation_id = None;
    j.upsert(&intent).await.expect("seed intent-backed raw row");

    // Hash attribution is deliberately SOFT, so an authoritative artifact is allowed to supersede
    // this repaired terminal on the same ledger sequence.
    let mut oracle = MockOracle::default();
    oracle.by_hash.insert((fed(1), [0xab; 32]), op(7));
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));
    let sink_result = Arc::new(std::sync::Mutex::new(None));
    let sink = SameAttemptSupersedingSink {
        journal: j.clone(),
        result: Arc::clone(&sink_result),
    };

    let summary = j
        .repair_ledger_with_terminal_sink(&oracle, &sink)
        .await
        .expect("the stale sink CAS is a benign false result");
    assert_eq!(
        summary.repaired, 1,
        "the original soft terminal was recorded"
    );
    assert_eq!(
        *sink_result.lock().expect("sink result lock"),
        Some(false),
        "the post-repair sink CAS must reject the same-sequence supersession"
    );
    assert_eq!(
        j.get(&k)
            .await
            .expect("read intent")
            .expect("intent exists")
            .status,
        IntentStatus::Pending,
        "the same-attempt authoritative supersession must leave its intent nonterminal"
    );
    let row = op_of(&j, &k).await;
    assert_eq!(row.status, OperationStatus::Awaiting);
    assert!(
        !row.repaired,
        "the authoritative artifact supersedes the soft terminal in place"
    );
}

#[tokio::test]
async fn repair_retries_terminal_intent_sink_after_first_sink_failure() {
    let j = mem_ledger();
    let k = key("pay:repair-sink-retry");
    j.upsert(&repairable_pay_intent(k.clone(), IntentStatus::Pending, 0))
        .await
        .expect("seed raw intent");
    let mut oracle = MockOracle::default();
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = FailOnceSink {
        journal: j.clone(),
        calls: Arc::clone(&calls),
    };

    let first = j
        .repair_ledger_with_terminal_sink(&oracle, &sink)
        .await
        .expect("ledger terminal survives sink fault");
    assert_eq!(
        first.repaired, 1,
        "the committed ledger terminal counts even while its intent sink is retried"
    );
    assert_eq!(status_of(&j, &k).await, OperationStatus::Succeeded);
    assert_eq!(
        j.get(&k).await.expect("intent").expect("exists").status,
        IntentStatus::Pending,
        "first sink failure leaves reservation live"
    );

    let second = j
        .repair_ledger_with_terminal_sink(&oracle, &sink)
        .await
        .expect("second pass retries terminal sink");
    assert_eq!(
        second.repaired, 0,
        "the second pass only synchronizes the already-counted terminal row"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        j.get(&k).await.expect("intent").expect("exists").status,
        IntentStatus::Done
    );
    let third = j
        .repair_ledger_with_terminal_sink(&oracle, &sink)
        .await
        .expect("terminal intent does not churn through the sink");
    assert_eq!(third.repaired, 0);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the terminal-row retry remains available after a failed sink, but stops once its intent is terminal"
    );
}

#[tokio::test]
async fn repair_hash_dedup_terminal_is_soft_with_note() {
    let j = mem_ledger();
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    j.record_update(
        &k,
        RawOpUpdate {
            payment_hash: Some([0xab; 32]),
            ..Default::default()
        },
    )
    .await
    .expect("hash update");

    // NOT found by key; found by the durably-written payment hash (a deduped retry).
    let mut oracle = MockOracle::default();
    oracle.by_hash.insert((fed(1), [0xab; 32]), op(7));
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));

    j.repair_ledger(&oracle).await.expect("repair");
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Succeeded);
    assert!(
        rec.repaired,
        "hash-dedup attribution is uncertain -> SOFT terminal"
    );
    assert!(
        rec.error.as_ref().expect("note").contains("payment hash"),
        "the ambiguity is recorded: {:?}",
        rec.error
    );
}

#[tokio::test]
async fn intent_backed_hash_dedup_repair_keeps_uncertainty_until_authoritative_observation() {
    let j = mem_ledger();
    let k = key("pay:hash-dedup-intent");
    let mut intent = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    intent.operation_id = None;
    j.upsert(&intent).await.expect("seed intent-backed raw row");

    let mut oracle = MockOracle::default();
    oracle.by_hash.insert((fed(1), [0xab; 32]), op(7));
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));
    let summary = j.repair_ledger(&oracle).await.expect("hash-dedup repair");
    assert_eq!(summary.repaired, 1);
    assert_eq!(
        j.get(&k)
            .await
            .expect("read intent")
            .expect("intent")
            .status,
        IntentStatus::Done,
        "the repair sink releases only the matching raw reservation"
    );
    assert_eq!(
        j.get(&k)
            .await
            .expect("read intent")
            .expect("intent")
            .operation_id,
        Some(op(7)),
        "the fenced repair adopts its recovered operation before terminalizing the intent"
    );

    let public = j
        .operation(&OperationRef::Key(k.clone()))
        .await
        .expect("public operation read")
        .expect("ledger row");
    assert_eq!(public.status, OperationStatus::Succeeded);
    assert!(public.repaired, "hash-dedup attribution remains defeasible");
    assert!(
        public
            .error
            .as_deref()
            .is_some_and(|error| error.starts_with("correlated by payment hash")),
        "public operation retains the hash-dedup uncertainty: {:?}",
        public.error
    );
    let history = j.history(10, None).await.expect("public history");
    let historical = history
        .iter()
        .find(|row| row.correlation_key == k)
        .expect("current row is in history");
    assert!(historical.repaired);
    assert!(
        historical
            .error
            .as_deref()
            .is_some_and(|error| error.starts_with("correlated by payment hash")),
        "history exposes the same uncertainty: {:?}",
        historical.error
    );

    // A later direct observation must not rewrite an already-terminal intent, even when the
    // earlier repair was soft. The finalizer shares the lifecycle transition predicate with every
    // other writer: terminal rows are immutable and a same-terminal observation is a no-op.
    let prepared = j
        .prepare_raw_operation_terminal(&oracle, fed(1), op(7), &k, 0, RawOperationRole::Send)
        .await
        .expect("authoritative observation prepares");
    j.finalize_raw_operation(&k, OperationStatus::Succeeded, None, prepared)
        .await
        .expect("terminal observation is a benign no-op");
    let authoritative = op_of(&j, &k).await;
    assert!(authoritative.repaired);
    assert!(authoritative
        .error
        .as_deref()
        .is_some_and(|error| error.starts_with("correlated by payment hash")));
}

#[tokio::test]
async fn intent_backed_failed_pay_repair_adopts_op_before_terminalizing_intent() {
    let j = mem_ledger();
    let k = key("pay:hash-dedup-failed-intent");
    let mut intent = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    intent.operation_id = None;
    j.upsert(&intent).await.expect("seed intent-backed raw row");

    let mut oracle = MockOracle::default();
    oracle.by_hash.insert((fed(1), [0xab; 32]), op(7));
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(false, 42));

    let summary = j.repair_ledger(&oracle).await.expect("failed hash repair");
    assert_eq!(summary.repaired, 1);
    let repaired = j
        .get(&k)
        .await
        .expect("read intent")
        .expect("intent exists");
    assert_eq!(repaired.status, IntentStatus::Failed);
    assert_eq!(
        repaired.operation_id,
        Some(op(7)),
        "a terminal-failed Pay retains the recovered committed operation, so the public actor \
         rejects its otherwise-unwinnable manual retry"
    );
}

#[tokio::test]
async fn raw_terminal_sink_rejects_intent_status_opposite_its_fenced_ledger_status() {
    let j = mem_ledger();
    let k = key("pay:fence-status-mismatch");
    j.upsert(&repairable_pay_intent(k.clone(), IntentStatus::Pending, 0))
        .await
        .expect("seed raw pay");
    assert!(j
        .record_raw_observation_if_attempt(&k, 0, op(7), &terminal_send_obs(true, 42))
        .await
        .expect("seed succeeded fenced ledger row"));
    let row = op_of(&j, &k).await;
    let fence = wallet_fedimint::journal::RawIntentTerminalFence::new(
        row.seq,
        0,
        fed(1),
        Some(op(7)),
        RawOperationRole::Send,
        OperationStatus::Succeeded,
    );

    assert!(
        !j.set_raw_terminal_if_fenced(&k, &fence, IntentStatus::Failed, None)
            .await
            .expect("opposite terminal transition is a rejected compare"),
        "a public fence constructor must not authorize a Failed intent for a Succeeded ledger row"
    );
    assert_eq!(
        j.get(&k)
            .await
            .expect("read intent")
            .expect("intent exists")
            .status,
        IntentStatus::Pending
    );
}

#[tokio::test]
async fn raw_terminal_sink_rejects_conflicting_intent_operation_identity() {
    let j = mem_ledger();
    let k = key("pay:fence-op-conflict");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("seed standalone raw pay");
    j.record_update(
        &k,
        RawOpUpdate {
            op_id: Some(op(7)),
            ..Default::default()
        },
    )
    .await
    .expect("seed standalone recovered ledger operation");
    j.record_terminal(&k, OperationStatus::Failed, BASE, Some("failed"), None)
        .await
        .expect("seed standalone failed ledger row");

    let mut intent = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    intent.operation_id = Some(op(8));
    j.upsert(&intent).await.expect("seed raw pay intent");
    // The raw ledger may be enriched by recovery independently of the intent. The fence proves
    // this recovered op, but the sink must fail closed rather than overwrite another intent op.
    let row = op_of(&j, &k).await;
    let fence = wallet_fedimint::journal::RawIntentTerminalFence::new(
        row.seq,
        0,
        fed(1),
        Some(op(7)),
        RawOperationRole::Send,
        OperationStatus::Failed,
    );

    assert!(!j
        .set_raw_terminal_if_fenced(&k, &fence, IntentStatus::Failed, None)
        .await
        .expect("conflicting operation is a rejected compare"));
    let unchanged = j
        .get(&k)
        .await
        .expect("read intent")
        .expect("intent exists");
    assert_eq!(unchanged.status, IntentStatus::Pending);
    assert_eq!(unchanged.operation_id, Some(op(8)));
}

#[tokio::test]
async fn hash_only_old_op_in_flight_then_failed_does_not_sink_or_adopt_live_retry_n_plus_one() {
    let j = mem_ledger();
    let k = key("pay:hash-failed-retry");
    let mut first = repairable_pay_intent(k.clone(), IntentStatus::Pending, 0);
    first.operation_id = None;
    j.upsert(&first).await.expect("seed attempt N");
    // This is the failed SDK operation to which the shared payment hash resolves.  The old
    // intent intentionally has no operation id, reproducing the pre-sink identity gap.
    assert!(j
        .record_raw_observation_if_attempt(&k, first.attempt, op(7), &in_flight_send_obs())
        .await
        .expect("record N operation"));
    j.set_status(&k, 0, IntentStatus::Failed, Some("N failed"))
        .await
        .expect("terminalize attempt N");
    let mut retry = repairable_pay_intent(k.clone(), IntentStatus::Pending, 1);
    retry.operation_id = None;
    j.retry_failed_intent(&retry)
        .await
        .expect("start live retry N+1");

    let mut oracle = MockOracle::default();
    oracle.by_hash.insert((fed(1), [0xab; 32]), op(7));
    oracle
        .observations
        .insert((fed(1), op(7).0), in_flight_send_obs());
    let summary = j
        .repair_ledger(&oracle)
        .await
        .expect("repair live retry while N remains in flight");
    assert_eq!(
        summary.repaired, 0,
        "a hash-only in-flight result for N is not evidence for retry N+1"
    );

    let current = j.get(&k).await.expect("read retry").expect("retry exists");
    assert_eq!(current.attempt, 1);
    assert_eq!(current.status, IntentStatus::Pending);
    assert_eq!(
        current.operation_id, None,
        "N+1 must not adopt N's in-flight operation"
    );
    let current_row = op_of(&j, &k).await;
    assert_eq!(current_row.status, OperationStatus::Started);
    assert!(matches!(
        current_row.kind,
        OperationKind::Pay { op_id: None, .. }
    ));

    // The same old operation subsequently fails. Neither hash-only observation identifies the
    // current retry, so it remains unadopted until its attempt correlation or an artifact does.
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(false, 42));
    let summary = j
        .repair_ledger(&oracle)
        .await
        .expect("repair live retry after N fails");
    assert_eq!(
        summary.repaired, 0,
        "a hash-only FAILED result for N is not evidence against retry N+1"
    );

    let current = j.get(&k).await.expect("read retry").expect("retry exists");
    assert_eq!(current.attempt, 1);
    assert_eq!(current.status, IntentStatus::Pending);
    assert_eq!(current.operation_id, None, "N+1 must not adopt N's op");
    let current_row = op_of(&j, &k).await;
    assert_eq!(current_row.status, OperationStatus::Started);
    assert!(matches!(
        current_row.kind,
        OperationKind::Pay { op_id: None, .. }
    ));
}

#[tokio::test]
async fn repair_hash_dedup_in_flight_adopts_awaiting() {
    let j = mem_ledger();
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    j.record_update(
        &k,
        RawOpUpdate {
            payment_hash: Some([0xab; 32]),
            ..Default::default()
        },
    )
    .await
    .expect("hash update");

    let mut oracle = MockOracle::default();
    oracle.by_hash.insert((fed(1), [0xab; 32]), op(7));
    oracle
        .observations
        .insert((fed(1), op(7).0), in_flight_send_obs());

    j.repair_ledger(&oracle).await.expect("repair");
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Awaiting);
    assert!(
        !rec.repaired,
        "a non-terminal adoption is not a repaired terminal"
    );
    match rec.kind {
        OperationKind::Pay { op_id, .. } => assert_eq!(op_id, Some(op(7))),
        other => panic!("kind changed: {other:?}"),
    }
    assert!(rec.error.as_ref().expect("note").contains("payment hash"));
}

#[tokio::test]
async fn repair_awaiting_with_op_id_terminalizes_from_the_op_log() {
    let j = mem_ledger();
    let k = key("recv:0101:n");
    j.record_started(
        &k,
        recv_kind(fed(1), Msat(1_000)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    j.record_update(
        &k,
        RawOpUpdate {
            op_id: Some(op(7)),
            ..Default::default()
        },
    )
    .await
    .expect("op-id update"); // -> Awaiting
    j.upsert(&Intent {
        idempotency_key: k.clone(),
        attempt: 0,
        action: Action::Receive {
            to: fed(1),
            amount: Msat(1_000),
            fee_cap: Msat(100),
            nonce: "repair-terminal".into(),
            gateway: None,
        },
        max_fee: Some(Msat(100)),
        status: IntentStatus::Awaiting,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: BASE,
        operation_id: Some(op(7)),
        invoice: Some(Invoice("lnbc1repairfixture".into())),
    })
    .await
    .expect("seed matching raw receive intent");
    assert_eq!(status_of(&j, &k).await, OperationStatus::Awaiting);

    let mut oracle = MockOracle::default();
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_recv_obs(150));

    j.repair_ledger(&oracle).await.expect("repair");
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Succeeded);
    assert!(
        !rec.repaired,
        "reading a real op-log outcome is authoritative"
    );
    assert_eq!(rec.fees.receive_fee, Some(Msat(150)));
    assert_eq!(
        j.get(&k)
            .await
            .expect("read intent")
            .expect("intent exists")
            .status,
        IntentStatus::Done,
        "repair must release the raw receive reservation with the ledger terminal"
    );
}

#[tokio::test]
async fn repair_hash_dedup_settlement_stays_soft_and_keeps_note() {
    // §10.3: a `pay:` row adopted by hash-dedup while its op was still in flight (pass 1 → the
    // uncertain-attribution note) must, when that op later settles, terminalize SOFT and RE-CARRY
    // the note. A clean authoritative `Succeeded` would let `advance` shed the note, so history
    // would silently claim an attempt-level certainty it never had.
    let j = mem_ledger();
    let k = key("pay:0101:n");
    j.record_started(
        &k,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    j.record_update(
        &k,
        RawOpUpdate {
            payment_hash: Some([0xab; 32]),
            ..Default::default()
        },
    )
    .await
    .expect("hash update");

    // Pass 1: matched by hash but still in flight → Awaiting, op id adopted, ambiguity noted.
    let mut oracle = MockOracle::default();
    oracle.by_hash.insert((fed(1), [0xab; 32]), op(7));
    oracle
        .observations
        .insert((fed(1), op(7).0), in_flight_send_obs());
    j.repair_ledger(&oracle).await.expect("repair pass 1");
    let after1 = op_of(&j, &k).await;
    assert_eq!(after1.status, OperationStatus::Awaiting);
    assert!(after1
        .error
        .as_ref()
        .expect("note")
        .contains("payment hash"));

    // Pass 2: the SAME op now carries a terminal outcome → SOFT Succeeded that KEEPS the note.
    oracle
        .observations
        .insert((fed(1), op(7).0), terminal_send_obs(true, 42));
    j.repair_ledger(&oracle).await.expect("repair pass 2");
    let after2 = op_of(&j, &k).await;
    assert_eq!(after2.status, OperationStatus::Succeeded);
    assert!(
        after2.repaired,
        "an uncertain hash-dedup settlement stays defeasible (soft)"
    );
    assert!(
        after2
            .error
            .as_ref()
            .expect("note preserved")
            .contains("payment hash"),
        "the ambiguity note survives settlement"
    );
}

#[tokio::test]
async fn raw_repair_oracle_error_does_not_block_later_rows() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let bad_raw = key("pay:0101:bad");
    j.record_started(
        &bad_raw,
        pay_kind(fed(1)),
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("raw start");
    j.record_update(
        &bad_raw,
        RawOpUpdate {
            op_id: Some(op(99)),
            ..Default::default()
        },
    )
    .await
    .expect("raw op id");
    let tick = key("tick:0:n");
    j.record_tick_started(&tick, Occurrence(0), BASE)
        .await
        .expect("tick started");

    let summary = j
        .repair_ledger(&empty_oracle())
        .await
        .expect("one raw oracle failure must not abort the pass");
    assert_eq!(
        summary.repaired, 1,
        "the later stale tick is still repaired"
    );
    assert_eq!(
        status_of(&j, &bad_raw).await,
        OperationStatus::Awaiting,
        "the bad raw row remains truthful and retries on a later pass"
    );
    assert_eq!(status_of(&j, &tick).await, OperationStatus::Failed);
}

#[tokio::test]
async fn repair_soft_fails_a_stale_tick_row_after_1h() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("tick:0:n");
    j.record_tick_started(&k, Occurrence(0), BASE)
        .await
        .expect("tick started");

    let summary = j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert_eq!(summary.repaired, 1);
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Failed);
    assert!(rec.repaired);
    assert_eq!(
        rec.error.as_deref(),
        Some("interrupted — no terminal report")
    );
}

#[tokio::test]
async fn repair_soft_fails_stale_discover_and_autojoin_rows_after_1h() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let discover = key("discover:manual:n");
    let autojoin = key("autojoin:n");
    let probe_skip = key("watch-probe-skip:0202:0101:20000:1700000000000");
    j.record_started(
        &discover,
        OperationKind::Discover {
            source: DiscoverySource::Manual,
            status: SourceStatus::Ok,
            found: 3,
            structurally_passed: 2,
            rejected: 1,
        },
        Actor::Agent {
            occurrence: Occurrence(7),
        },
        ReasonCode::StandingInstruction,
        BASE,
        None,
    )
    .await
    .expect("discover started");
    j.record_started(
        &autojoin,
        OperationKind::AutoJoin {
            considered: 4,
            joined: 1,
            blocked_concurrent: 1,
            blocked_weekly: 1,
            blocked_lifetime: 1,
        },
        Actor::Agent {
            occurrence: Occurrence(7),
        },
        ReasonCode::StandingInstruction,
        BASE,
        None,
    )
    .await
    .expect("autojoin started");
    j.record_started(
        &probe_skip,
        OperationKind::Probe {
            fed: fed(2),
            from: fed(1),
            amount_msat: Msat(20_000),
            cost_msat: None,
        },
        Actor::Agent {
            occurrence: Occurrence(7),
        },
        ReasonCode::StandingInstruction,
        BASE,
        None,
    )
    .await
    .expect("probe skip started");

    let summary = j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert_eq!(summary.repaired, 3);
    for k in [&discover, &autojoin, &probe_skip] {
        let rec = op_of(&j, k).await;
        assert_eq!(rec.status, OperationStatus::Failed);
        assert!(rec.repaired);
        assert_eq!(
            rec.error.as_deref(),
            Some("interrupted — no terminal report")
        );
    }
}

#[tokio::test]
async fn repair_join_terminal_retry_supersedes_older_started_attempt() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let stale = key("join:0101:stale");
    let completed = key("join:0101:completed");
    j.record_started(
        &stale,
        OperationKind::Join { fed: fed(1) },
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("stale attempt");
    j.record_started(
        &completed,
        OperationKind::Join { fed: fed(1) },
        Actor::User,
        ReasonCode::UserInitiated,
        BASE + 5_000,
        None,
    )
    .await
    .expect("completed attempt");
    j.record_terminal(
        &completed,
        OperationStatus::Succeeded,
        BASE + 6_000,
        None,
        None,
    )
    .await
    .expect("completed terminal");
    j.put_federation(&fed(1), &fed_info((BASE + 10_000) / 1000))
        .await
        .expect("put_federation");

    let summary = j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert_eq!(summary.repaired, 1);
    let stale = op_of(&j, &stale).await;
    let completed = op_of(&j, &completed).await;
    assert_eq!(stale.status, OperationStatus::Failed);
    assert!(stale.repaired);
    assert_eq!(
        stale.error.as_deref(),
        Some("superseded by a later join attempt")
    );
    assert_eq!(completed.status, OperationStatus::Succeeded);
    assert!(
        !completed.repaired,
        "the authoritative terminal row is untouched"
    );
}

#[tokio::test]
async fn late_join_outcome_supersedes_repaired_join_superseded_failure() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let federation = fed(1);
    let old_key = key("join:0101:authoritative-winner");
    let current_key = key("join:0101:late-outcome");
    let join_intent = |idempotency_key: IdempotencyKey, status| Intent {
        idempotency_key,
        attempt: 0,
        action: Action::Join {
            federation,
            invite: "invite".into(),
            membership_preexisting: false,
        },
        max_fee: None,
        status,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: BASE,
        operation_id: None,
        invoice: None,
    };
    let old = join_intent(old_key.clone(), IntentStatus::Executing);
    let current = join_intent(current_key.clone(), IntentStatus::Executing);
    j.upsert(&old).await.expect("seed old join");
    j.set_status(&old_key, old.attempt, IntentStatus::Done, None)
        .await
        .expect("terminalize authoritative old join");
    j.upsert(&current)
        .await
        .expect("seed current executing join");
    j.put_federation(&federation, &fed_info((BASE + 1_000) / 1000))
        .await
        .expect("registry proves membership");

    let summary = j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert_eq!(summary.repaired, 1);
    let repaired = op_of(&j, &current_key).await;
    assert_eq!(repaired.status, OperationStatus::Failed);
    assert!(repaired.repaired);
    assert_eq!(
        repaired.error.as_deref(),
        Some("superseded by a later join attempt")
    );

    assert!(
        j.record_join_outcome(&current_key, current.attempt, true)
            .await
            .expect("late join outcome"),
        "the real join outcome must supersede a defeasible repair"
    );
    let authoritative = op_of(&j, &current_key).await;
    assert_eq!(authoritative.status, OperationStatus::Succeeded);
    assert!(!authoritative.repaired);
    assert_eq!(authoritative.error, None);

    j.set_status(&current_key, current.attempt, IntentStatus::Done, None)
        .await
        .expect("intent can converge after its ledger outcome");
    assert_eq!(
        j.get(&current_key)
            .await
            .expect("read converged intent")
            .expect("current intent")
            .status,
        IntentStatus::Done
    );
}

#[tokio::test]
async fn noop_join_outcome_supersedes_repaired_join_success() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let federation = fed(1);
    let intent = Intent {
        idempotency_key: key("join:0101:repaired-noop"),
        attempt: 0,
        action: Action::Join {
            federation,
            invite: "invite".into(),
            membership_preexisting: false,
        },
        max_fee: None,
        status: IntentStatus::Executing,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: BASE,
        operation_id: None,
        invoice: None,
    };
    j.upsert(&intent).await.expect("seed executing join");
    // This real repair path concludes that the live attempt created the registry entry, but its
    // Succeeded conclusion remains defeasible until the join driver reports its actual outcome.
    j.put_federation(&federation, &fed_info(BASE / 1000))
        .await
        .expect("registry proves membership");
    let summary = j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert_eq!(summary.repaired, 1);
    let repaired = op_of(&j, &intent.idempotency_key).await;
    assert_eq!(repaired.status, OperationStatus::Succeeded);
    assert!(repaired.repaired);
    assert_eq!(repaired.error, None);

    assert!(
        j.record_join_outcome(&intent.idempotency_key, intent.attempt, false)
            .await
            .expect("authoritative no-op join outcome"),
        "the authoritative no-op outcome must apply over a defeasible repair"
    );
    let authoritative = op_of(&j, &intent.idempotency_key).await;
    assert_eq!(authoritative.status, OperationStatus::Succeeded);
    assert!(
        !authoritative.repaired,
        "the authoritative no-op outcome must clear the repaired marker"
    );
    assert_eq!(
        authoritative.error.as_deref(),
        Some(JOIN_NOOP_REOPEN_NOTE),
        "the authoritative no-op outcome must replace the repair conclusion with its exact note"
    );
}

#[tokio::test]
async fn repair_join_single_attempt_in_window_succeeds_without_note() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("join:0101:n");
    j.record_started(
        &k,
        OperationKind::Join { fed: fed(1) },
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    // `joined_at` (seconds) converts to BASE ms; the attempt at BASE ms is within the window.
    j.put_federation(&fed(1), &fed_info(BASE / 1000))
        .await
        .expect("put_federation");

    j.repair_ledger(&empty_oracle()).await.expect("repair");
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Succeeded);
    assert!(rec.repaired);
    assert_eq!(
        rec.error, None,
        "a single in-window candidate carries no ambiguity note"
    );
}

#[tokio::test]
async fn repair_join_absent_registry_soft_fails_after_1h() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("join:0101:n");
    j.record_started(
        &k,
        OperationKind::Join { fed: fed(1) },
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    // No registry row -> membership never completed.
    j.repair_ledger(&empty_oracle()).await.expect("repair");
    let rec = op_of(&j, &k).await;
    assert_eq!(rec.status, OperationStatus::Failed);
    assert!(rec.repaired);
    assert_eq!(
        rec.error.as_deref(),
        Some("join did not complete — federation not in the registry; re-run join")
    );
}

#[tokio::test]
async fn repair_join_failed_attempt_then_successful_retry_yields_two_truthful_rows() {
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let attempt1 = key("join:0101:a1");
    let attempt2 = key("join:0101:a2");
    // attempt1 crashed (older); attempt2 (newer) completed the join. Both predate `joined_at`.
    j.record_started(
        &attempt1,
        OperationKind::Join { fed: fed(1) },
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("attempt1");
    j.record_started(
        &attempt2,
        OperationKind::Join { fed: fed(1) },
        Actor::User,
        ReasonCode::UserInitiated,
        BASE + 5_000,
        None,
    )
    .await
    .expect("attempt2");
    j.put_federation(&fed(1), &fed_info((BASE + 10_000) / 1000))
        .await
        .expect("put_federation");

    j.repair_ledger(&empty_oracle()).await.expect("repair");
    let r1 = op_of(&j, &attempt1).await;
    let r2 = op_of(&j, &attempt2).await;
    // Two candidates -> newest soft-Succeeds WITH the ambiguity note; the older soft-Fails.
    assert_eq!(r2.status, OperationStatus::Succeeded);
    assert!(r2
        .error
        .as_ref()
        .expect("note")
        .contains("overlapping attempts"));
    assert_eq!(r1.status, OperationStatus::Failed);
    assert_eq!(
        r1.error.as_deref(),
        Some("superseded by a later join attempt")
    );
    assert!(
        r1.repaired && r2.repaired,
        "both writes are soft/defeasible"
    );
}

#[tokio::test]
async fn repair_join_attempt_stamped_after_joined_at_still_succeeds_with_note() {
    // SKEWED CLOCK: a backward jump stamped the attempt AFTER `joined_at`, so no attempt falls
    // inside the window — but membership is registry-proven, so the newest still soft-Succeeds.
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("join:0101:n");
    j.record_started(
        &k,
        OperationKind::Join { fed: fed(1) },
        Actor::User,
        ReasonCode::UserInitiated,
        BASE + HOUR, // stamped an hour after `joined_at` (backward-jumped device clock)
        None,
    )
    .await
    .expect("start");
    j.put_federation(&fed(1), &fed_info(BASE / 1000))
        .await
        .expect("put_federation");

    j.repair_ledger(&empty_oracle()).await.expect("repair");
    let rec = op_of(&j, &k).await;
    assert_eq!(
        rec.status,
        OperationStatus::Succeeded,
        "membership is registry-proven despite the clock skew"
    );
    assert!(rec.repaired);
    assert!(
        rec.error
            .as_ref()
            .expect("note")
            .contains("overlapping attempts"),
        "the arbitration is uncertain (no in-window attempt), so it is noted"
    );
}

#[tokio::test]
async fn an_authoritative_write_supersedes_a_soft_repair() {
    // The defeasible-repair self-healing property, end to end (§7/§10.3).
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let k = key("join:0101:n");
    j.record_started(
        &k,
        OperationKind::Join { fed: fed(1) },
        Actor::User,
        ReasonCode::UserInitiated,
        BASE,
        None,
    )
    .await
    .expect("start");
    // Registry absent + > 1h -> soft Failed.
    j.repair_ledger(&empty_oracle()).await.expect("repair");
    let soft = op_of(&j, &k).await;
    assert_eq!(soft.status, OperationStatus::Failed);
    assert!(soft.repaired);

    // The real join later reports success (authoritative): it supersedes the soft repair once.
    j.record_terminal(&k, OperationStatus::Succeeded, BASE + 3 * HOUR, None, None)
        .await
        .expect("authoritative supersession");
    let healed = op_of(&j, &k).await;
    assert_eq!(healed.status, OperationStatus::Succeeded);
    assert!(!healed.repaired, "the supersession clears the soft flag");
    assert_eq!(healed.error, None, "the stale repair diagnostic is cleared");
}

#[tokio::test]
async fn repair_never_touches_move_intent_rows() {
    // A move-shaped intent row is owned by the §9.2 move journal integration and is never
    // repaired here, even when non-terminal and old. Raw pay/receive intents have their own
    // op-log-backed repair path.
    let j = FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_base_plus_2h);
    let intent = move_intent("move:0102:0", IntentStatus::Pending);
    j.upsert(&intent).await.expect("upsert");

    let summary = j.repair_ledger(&empty_oracle()).await.expect("repair");
    assert_eq!(summary.repaired, 0);
    assert_eq!(
        status_of(&j, &intent.idempotency_key).await,
        OperationStatus::Started,
        "intent-keyed rows are left to the journal integration"
    );
}
