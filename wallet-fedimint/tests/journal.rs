//! Durability tests for [`FedimintJournal`] over an in-memory fedimint `Database`
//! (`MemDatabase` — no devimint, no money path). They pin the spec §8 contract: serde
//! round-trips, the atomic Intent + `PendingIndexKey` write, the index moving on a status
//! change, the `MoveRecord` cache, the federation registry, and cross-handle persistence.

use fedimint_core::db::mem_impl::MemDatabase;
use fedimint_core::db::IDatabaseTransactionOpsCore;
use fedimint_core::db::IRawDatabaseExt;
use futures::StreamExt;
use wallet_core::{
    Action, ExecError, FederationId, IdempotencyKey, Intent, IntentStatus, Journal, Msat,
};
use wallet_fedimint::{
    FederationInfo, FedimintJournal, GatewayUrl, Invoice, MovePhase, MoveRecord, OperationId,
};

fn mem_journal() -> FedimintJournal {
    FedimintJournal::new(MemDatabase::new().into_database())
}

fn fed(n: u8) -> FederationId {
    FederationId([n; 32])
}

fn intent(key: &str, status: IntentStatus) -> Intent {
    Intent {
        idempotency_key: IdempotencyKey(key.to_string()),
        action: Action::TopUpSpending {
            from: fed(1),
            to: fed(2),
            amount: Msat(100_000),
        },
        max_fee: Msat(2_000),
        status,
    }
}

fn move_record(key: &str) -> MoveRecord {
    MoveRecord {
        key: IdempotencyKey(key.to_string()),
        from: Some(fed(1)),
        to: fed(2),
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: GatewayUrl("https://gw.example".to_string()),
        send_required: true,
        invoice: Some(Invoice("lnbc1pexample".to_string())),
        recv_op: Some(OperationId([0x07; 32])),
        send_op: Some(OperationId([0x09; 32])),
        phase: MovePhase::Sending,
        outcome: None,
    }
}

fn has_key(intents: &[Intent], key: &str) -> bool {
    intents.iter().any(|i| i.idempotency_key.0 == key)
}

fn tagged_key(tag: u8, id_bytes: &[u8]) -> Vec<u8> {
    let mut raw_key = vec![tag];
    raw_key.extend_from_slice(id_bytes);
    raw_key
}

fn index_key(status_byte: u8, id_bytes: &[u8]) -> Vec<u8> {
    let mut raw_key = vec![0x04, status_byte];
    raw_key.extend_from_slice(id_bytes);
    raw_key
}

#[derive(serde::Serialize)]
struct TestStoredRowRef<'a, T> {
    version: u8,
    data: &'a T,
}

fn encoded_test_row<T: serde::Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(&TestStoredRowRef {
        version: 1,
        data: value,
    })
    .expect("encode test row")
}

/// Test 1: upsert an Intent → `get` returns the identical Intent (serde + DB round-trip).
#[tokio::test]
async fn upsert_then_get() {
    let journal = mem_journal();
    let i = intent("k1", IntentStatus::Pending);
    journal.upsert(&i).await.expect("upsert");

    assert_eq!(journal.get(&i.idempotency_key).await.expect("get"), Some(i));
}

/// Test 2: a Pending intent is in `pending()`; setting it Failed moves it to `failed()` and
/// out of `pending()` — the `PendingIndexKey` moved atomically with the status.
#[tokio::test]
async fn set_status_moves_between_indexes() {
    let journal = mem_journal();
    let i = intent("k2", IntentStatus::Pending);
    journal.upsert(&i).await.expect("upsert");

    assert!(has_key(&journal.pending().await, "k2"));
    assert!(!has_key(&journal.failed().await, "k2"));

    journal
        .set_status(&i.idempotency_key, IntentStatus::Failed)
        .await
        .expect("set_status");

    assert!(has_key(&journal.failed().await, "k2"));
    assert!(!has_key(&journal.pending().await, "k2"));
    // The intent itself reflects the new status.
    assert_eq!(
        journal
            .get(&i.idempotency_key)
            .await
            .expect("get")
            .map(|i| i.status),
        Some(IntentStatus::Failed)
    );
}

/// Test 3: `pending()` returns Pending|Executing only — Done and Failed are excluded.
#[tokio::test]
async fn pending_excludes_done_and_failed() {
    let journal = mem_journal();
    journal
        .upsert(&intent("p", IntentStatus::Pending))
        .await
        .expect("upsert pending");
    journal
        .upsert(&intent("x", IntentStatus::Executing))
        .await
        .expect("upsert executing");
    journal
        .upsert(&intent("d", IntentStatus::Done))
        .await
        .expect("upsert done");
    journal
        .upsert(&intent("f", IntentStatus::Failed))
        .await
        .expect("upsert failed");

    let pending = journal.pending().await;
    assert!(has_key(&pending, "p"));
    assert!(has_key(&pending, "x"));
    assert!(!has_key(&pending, "d"));
    assert!(!has_key(&pending, "f"));
    assert_eq!(pending.len(), 2);

    assert!(has_key(&journal.failed().await, "f"));
}

/// Test 4: `put_move` then `get_move` returns an equal `MoveRecord` (serde round-trip).
#[tokio::test]
async fn move_record_roundtrip() {
    let journal = mem_journal();
    let rec = move_record("m1");
    journal.put_move(&rec).await.expect("put_move");

    assert_eq!(
        journal.get_move(&rec.key).await.expect("get_move"),
        Some(rec)
    );
}

/// Test 5: `put_federation` then `list_federations`/`get_federation` round-trip the registry.
#[tokio::test]
async fn federation_registry_roundtrip() {
    let journal = mem_journal();
    let id_a = fed(0xAA);
    let id_b = fed(0xBB);
    let info_a = FederationInfo {
        invite: "fed1aaa".to_string(),
        db_prefix: 1,
        joined_at: 1_700_000_000,
    };
    let info_b = FederationInfo {
        invite: "fed1bbb".to_string(),
        db_prefix: 2,
        joined_at: 1_700_000_500,
    };
    journal.put_federation(&id_a, &info_a).await.expect("put a");
    journal.put_federation(&id_b, &info_b).await.expect("put b");

    assert_eq!(
        journal.get_federation(&id_a).await.expect("get a"),
        Some(info_a.clone())
    );
    assert_eq!(
        journal.get_federation(&id_b).await.expect("get b"),
        Some(info_b.clone())
    );

    let mut listed = journal.list_federations().await.expect("list");
    listed.sort_by_key(|(_, info)| info.db_prefix);
    assert_eq!(
        listed,
        vec![(id_a, info_a), (id_b, info_b)],
        "list_federations returns every registered federation with its id"
    );
}

/// Test 6: rows written through one `FedimintJournal` are visible to a second journal over
/// the SAME underlying `Database` (the cross-handle visibility/backfill property).
#[tokio::test]
async fn shared_database_handle_persists() {
    let db = MemDatabase::new().into_database();
    let writer = FedimintJournal::new(db.clone());

    let i = intent("persist", IntentStatus::Pending);
    writer.upsert(&i).await.expect("upsert");
    let rec = move_record("persist");
    writer.put_move(&rec).await.expect("put_move");

    // A fresh journal over the same Database sees everything the writer committed.
    let reader = FedimintJournal::new(db);
    assert_eq!(reader.get(&i.idempotency_key).await.expect("get"), Some(i));
    assert!(has_key(&reader.pending().await, "persist"));
    assert_eq!(
        reader.get_move(&rec.key).await.expect("get_move"),
        Some(rec)
    );
}

/// Test 7: a single `upsert` commits BOTH the IntentKey row and the PendingIndexKey row —
/// `get` sees the intent (IntentKey present) and `pending` sees it (PendingIndexKey present)
/// immediately, with no intermediate write.
#[tokio::test]
async fn atomic_intent_and_index() {
    let journal = mem_journal();
    let i = intent("atomic", IntentStatus::Pending);
    journal.upsert(&i).await.expect("upsert");

    // IntentKey row present.
    assert_eq!(journal.get(&i.idempotency_key).await.expect("get"), Some(i));
    // PendingIndexKey row present (the scan finds it).
    assert!(has_key(&journal.pending().await, "atomic"));
}

#[tokio::test]
async fn corrupt_intent_row_surfaces_error_not_missing() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    let key = IdempotencyKey("corrupt".to_string());

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    let raw_key = tagged_key(0x01, key.0.as_bytes());
    dbtx.raw_insert_bytes(&raw_key, b"not valid json")
        .await
        .expect("insert corrupt intent row");
    dbtx.commit_tx_result().await.expect("commit corrupt row");

    let err = journal
        .get(&key)
        .await
        .expect_err("a corrupt intent row must surface an error");
    assert!(matches!(err, ExecError::Permanent(_)));
}

/// App-specific reads also surface decode failures as `Result::Err(Permanent)` instead of
/// panicking — a momentary storage issue during resume must be retryable/recoverable, not a
/// process abort.
#[tokio::test]
async fn corrupt_move_row_surfaces_error_not_panic() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    let key = IdempotencyKey("corrupt-move".to_string());

    // Write garbage under the move tag (0x02) inside the app prefix (0x00).
    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    let raw_key = tagged_key(0x02, key.0.as_bytes());
    dbtx.raw_insert_bytes(&raw_key, b"not valid json")
        .await
        .expect("insert corrupt move row");
    dbtx.commit_tx_result().await.expect("commit corrupt row");

    let err = journal
        .get_move(&key)
        .await
        .expect_err("a corrupt move row must surface an error, not panic");
    assert!(matches!(err, ExecError::Permanent(_)));
}

/// The always-on scan path skips poison index rows instead of panic-looping reconcile.
#[tokio::test]
async fn index_scans_skip_poison_rows() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    journal
        .upsert(&intent("good-pending", IntentStatus::Pending))
        .await
        .expect("upsert good pending");
    journal
        .upsert(&intent("good-failed", IntentStatus::Failed))
        .await
        .expect("upsert good failed");

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    // Dangling index entries.
    dbtx.raw_insert_bytes(&index_key(0, b"missing-pending"), &[])
        .await
        .expect("insert missing pending index");
    dbtx.raw_insert_bytes(&index_key(4, b"missing-failed"), &[])
        .await
        .expect("insert missing failed index");
    // Malformed UTF-8 in an indexed key.
    dbtx.raw_insert_bytes(&index_key(0, &[0xff]), &[])
        .await
        .expect("insert malformed pending index");
    dbtx.raw_insert_bytes(&index_key(4, &[0xfe]), &[])
        .await
        .expect("insert malformed failed index");
    // Index entries pointing at corrupt Intent rows.
    dbtx.raw_insert_bytes(&tagged_key(0x01, b"corrupt-pending"), b"not valid json")
        .await
        .expect("insert corrupt pending intent");
    dbtx.raw_insert_bytes(&index_key(0, b"corrupt-pending"), &[])
        .await
        .expect("insert corrupt pending index");
    dbtx.raw_insert_bytes(&tagged_key(0x01, b"corrupt-failed"), b"not valid json")
        .await
        .expect("insert corrupt failed intent");
    dbtx.raw_insert_bytes(&index_key(4, b"corrupt-failed"), &[])
        .await
        .expect("insert corrupt failed index");
    dbtx.commit_tx_result().await.expect("commit poison rows");

    let pending = journal.pending().await;
    assert_eq!(pending.len(), 1);
    assert!(has_key(&pending, "good-pending"));

    let failed = journal.failed().await;
    assert_eq!(failed.len(), 1);
    assert!(has_key(&failed, "good-failed"));
}

#[tokio::test]
async fn index_scans_skip_intent_key_mismatch() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());

    let real_b = intent("embedded-b", IntentStatus::Done);
    journal.upsert(&real_b).await.expect("upsert real b");

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    let poisoned = intent("embedded-b", IntentStatus::Pending);
    dbtx.raw_insert_bytes(
        &tagged_key(0x01, b"indexed-a"),
        &encoded_test_row(&poisoned),
    )
    .await
    .expect("insert key-mismatched intent row");
    dbtx.raw_insert_bytes(&index_key(0, b"indexed-a"), &[])
        .await
        .expect("insert pending index");
    dbtx.commit_tx_result().await.expect("commit poison rows");

    let pending = journal.pending().await;
    assert!(
        !has_key(&pending, "embedded-b"),
        "the poisoned row must not drive the embedded key"
    );
    assert!(!has_key(&pending, "indexed-a"));
    assert_eq!(
        journal
            .get(&IdempotencyKey("embedded-b".to_string()))
            .await
            .expect("get real b")
            .map(|i| i.status),
        Some(IntentStatus::Done),
        "the real embedded-key row remains terminal"
    );
}

/// Durable value rows are JSON envelopes with an explicit schema version.
#[tokio::test]
async fn stored_rows_are_versioned_json_envelopes() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());

    let i = intent("versioned", IntentStatus::Pending);
    let rec = move_record("versioned");
    let fed_id = fed(0xCC);
    let fed_info = FederationInfo {
        invite: "fed1versioned".to_string(),
        db_prefix: 7,
        joined_at: 1_700_001_000,
    };
    journal.upsert(&i).await.expect("upsert intent");
    journal.put_move(&rec).await.expect("put move");
    journal
        .put_federation(&fed_id, &fed_info)
        .await
        .expect("put federation");

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction_nc().await;
    for raw_key in [
        tagged_key(0x01, i.idempotency_key.0.as_bytes()),
        tagged_key(0x02, rec.key.0.as_bytes()),
        tagged_key(0x03, &fed_id.0),
    ] {
        let bytes = dbtx
            .raw_get_bytes(&raw_key)
            .await
            .expect("raw get")
            .expect("row exists");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("json envelope");
        assert_eq!(value["version"], 1);
        assert!(value.get("data").is_some());
    }
}

#[tokio::test]
async fn unsupported_row_version_surfaces_error() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    let key = IdempotencyKey("newer-version".to_string());

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    let raw_key = tagged_key(0x02, key.0.as_bytes());
    dbtx.raw_insert_bytes(&raw_key, br#"{"version":2,"data":null}"#)
        .await
        .expect("insert unsupported version row");
    dbtx.commit_tx_result()
        .await
        .expect("commit unsupported version row");

    let err = journal
        .get_move(&key)
        .await
        .expect_err("unsupported row versions must surface as errors");
    match err {
        ExecError::Permanent(msg) => assert!(msg.contains("unsupported move record row version 2")),
        other => panic!("expected Permanent unsupported-version error, got {other:?}"),
    }
}

/// Only the terminal `Done` status leaves no `PendingIndexKey` row. The other four
/// (`Pending`/`Executing`/`Failed`/`Awaiting`) are all scanned — `Awaiting` by
/// `FedimintJournal::awaiting` for resume rehydration (spec §9.3) — so each is indexed.
#[tokio::test]
async fn only_done_status_leaves_no_index_row() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    for (key, status) in [
        ("done", IntentStatus::Done),
        ("awaiting", IntentStatus::Awaiting),
        ("pending", IntentStatus::Pending),
        ("executing", IntentStatus::Executing),
        ("failed", IntentStatus::Failed),
    ] {
        journal.upsert(&intent(key, status)).await.expect("upsert");
    }

    // Scan the raw index prefix (0x04) under the app prefix (0x00) and collect the status
    // byte (the second key byte) of every index row.
    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction_nc().await;
    let mut status_bytes: Vec<u8> = dbtx
        .raw_find_by_prefix(&[0x04])
        .await
        .expect("scan index prefix")
        .map(|(raw_key, _)| raw_key[1])
        .collect()
        .await;
    status_bytes.sort_unstable();
    // Pending(0), Executing(1), Awaiting(3), Failed(4) are indexed; Done(2) is not.
    assert_eq!(
        status_bytes,
        vec![0, 1, 3, 4],
        "every status except Done leaves an index row"
    );
}

/// An `Awaiting` intent (a `DirectInflow` whose external payer has not settled) is found by
/// `awaiting()` so the resume loop can re-subscribe its `recv_op` (spec §9.3), yet it is
/// NEVER returned by `pending()` (subscription-owned, not re-driven) or `failed()`.
#[tokio::test]
async fn awaiting_intents_are_scannable_for_resume() {
    let journal = mem_journal();
    let key = IdempotencyKey("inflow".to_string());

    // A DirectInflow that is still executing is in pending(), not awaiting().
    journal
        .upsert(&intent("inflow", IntentStatus::Executing))
        .await
        .expect("upsert executing");
    assert!(has_key(&journal.pending().await, "inflow"));
    assert!(!has_key(
        &journal.awaiting().await.expect("awaiting"),
        "inflow"
    ));

    // Once it returns Awaiting (invoice surfaced, payer external), it leaves pending() and
    // becomes discoverable by awaiting() for subscription rehydration (spec §9.3).
    journal
        .set_status(&key, IntentStatus::Awaiting)
        .await
        .expect("set awaiting");
    assert!(has_key(
        &journal.awaiting().await.expect("awaiting"),
        "inflow"
    ));
    assert!(!has_key(&journal.pending().await, "inflow"));
    assert!(!has_key(&journal.failed().await, "inflow"));

    // The recv_op subscription finally settles it (→ Done): it leaves every index.
    journal
        .set_status(&key, IntentStatus::Done)
        .await
        .expect("set done");
    assert!(!has_key(
        &journal.awaiting().await.expect("awaiting"),
        "inflow"
    ));
    assert!(!has_key(&journal.pending().await, "inflow"));
    assert_eq!(
        journal.get(&key).await.expect("get").map(|i| i.status),
        Some(IntentStatus::Done)
    );
}

/// `awaiting()` is the resume-time subscription-rehydration scan (spec §9.3), so it is
/// poison-tolerant like every other scan: one corrupt/dangling `Awaiting` row is SKIPPED,
/// not fatal, so it cannot strand the rehydration of every OTHER healthy inflow on resume.
#[tokio::test]
async fn awaiting_skips_poison_rows() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());

    // A healthy Awaiting inflow that must survive alongside the poison rows.
    journal
        .upsert(&intent("good-awaiting", IntentStatus::Awaiting))
        .await
        .expect("upsert good awaiting");

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    // A dangling Awaiting index entry (no intent row).
    dbtx.raw_insert_bytes(&index_key(3, b"missing-awaiting"), &[])
        .await
        .expect("insert missing awaiting index");
    // Malformed UTF-8 in an Awaiting index key.
    dbtx.raw_insert_bytes(&index_key(3, &[0xff]), &[])
        .await
        .expect("insert malformed awaiting index");
    // A corrupt Awaiting intent row + its index entry.
    dbtx.raw_insert_bytes(&tagged_key(0x01, b"corrupt-awaiting"), b"not valid json")
        .await
        .expect("insert corrupt awaiting intent");
    dbtx.raw_insert_bytes(&index_key(3, b"corrupt-awaiting"), &[])
        .await
        .expect("insert corrupt awaiting index");
    dbtx.commit_tx_result().await.expect("commit poison rows");

    let awaiting = journal
        .awaiting()
        .await
        .expect("awaiting skips poison rows instead of erroring");
    assert_eq!(awaiting.len(), 1);
    assert!(has_key(&awaiting, "good-awaiting"));
}

/// `list_federations` gates re-opening EVERY client on resume (§9.1), so it is
/// poison-tolerant: one corrupt value or malformed key is skipped, not fatal, so the
/// healthy federations (which may hold funds) still resume.
#[tokio::test]
async fn list_federations_skips_poison_rows() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    let good_id = fed(0x11);
    let good = FederationInfo {
        invite: "fed1good".to_string(),
        db_prefix: 1,
        joined_at: 1,
    };
    journal
        .put_federation(&good_id, &good)
        .await
        .expect("put good");

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    // A federation row (tag 0x03) whose value is not valid JSON.
    dbtx.raw_insert_bytes(&tagged_key(0x03, &[0x22; 32]), b"not valid json")
        .await
        .expect("insert corrupt federation row");
    // A federation row whose key is the wrong length (not a 32-byte id).
    dbtx.raw_insert_bytes(&tagged_key(0x03, &[0x33; 8]), b"{}")
        .await
        .expect("insert malformed-key federation row");
    dbtx.commit_tx_result().await.expect("commit poison rows");

    let report = journal
        .list_federations_report()
        .await
        .expect("list with report");
    assert_eq!(
        report.skipped_rows, 2,
        "both poison registry rows are surfaced as skipped"
    );
    assert_eq!(
        report.federations,
        vec![(good_id, good.clone())],
        "only the healthy registry row survives"
    );

    let listed = journal.list_federations().await.expect("list");
    assert_eq!(
        listed,
        vec![(good_id, good)],
        "only the healthy registry row survives"
    );
}
