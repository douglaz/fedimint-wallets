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
use wallet_core::{
    Action, Actor, AllocatorDecision, DiscoverySource, ExecError, FederationId, FeeBreakdown,
    IdempotencyKey, Intent, IntentStatus, Journal, Msat, Occurrence, OperationKind,
    OperationRecord, OperationStatus, RawOpUpdate, ReasonCode, SourceStatus,
};
use wallet_fedimint::{
    FederationInfo, FedimintJournal, GatewayUrl, Invoice, LedgerRepairOracle, MovePhase,
    MoveRecord, OperationId, OperationRef, RawOpObservation, RawTerminal,
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
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(100_000),
            fee_cap: Msat(2_000),
        },
        max_fee: Some(Msat(2_000)),
        status,
        reason: ReasonCode::UserInitiated,
        actor: Actor::User,
        created_at_ms: BASE,
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

fn in_flight_send_obs() -> RawOpObservation {
    RawOpObservation {
        terminal: None,
        gateway: Some(GatewayUrl("https://gw".to_string())),
        fees: FeeBreakdown::default(),
        invoice_amount: Some(Msat(50_000)),
        payment_hash: Some([0xab; 32]),
    }
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
    j.put_move(&mv).await.expect("put_move");

    // No executor string -> the ledger error falls back to the MoveRecord outcome (§9.2).
    j.set_status(&intent.idempotency_key, IntentStatus::Failed, None)
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
    j.put_move(&move_record_for("move:0102:3"))
        .await
        .expect("put_move");
    // A NON-terminal status write must reflect the in-flight metadata (§9.2).
    j.set_status(&intent.idempotency_key, IntentStatus::Awaiting, None)
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
async fn already_paid_terminal_carries_definitive_fees() {
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
    // AlreadyPaid: terminalize Succeeded carrying the definitive fees read from the op meta.
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
async fn repair_never_touches_intent_keyed_rows() {
    // An intent-keyed row (owned by the §9.2 journal integration) is NEVER repaired here, even
    // when non-terminal and old.
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
