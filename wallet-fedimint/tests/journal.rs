//! Durability tests for [`FedimintJournal`] over an in-memory fedimint `Database`
//! (`MemDatabase` — no devimint, no money path). They pin the spec §8 contract: serde
//! round-trips, the atomic Intent + `PendingIndexKey` write, the index moving on a status
//! change, the `MoveRecord` cache, the federation registry, and cross-handle persistence.

use async_trait::async_trait;
use fedimint_core::db::mem_impl::MemDatabase;
use fedimint_core::db::IDatabaseTransactionOpsCore;
use fedimint_core::db::IRawDatabaseExt;
use fedimint_core::invite_code::InviteCode;
use futures::StreamExt;
use std::sync::{Arc, Mutex};
use std::{collections::BTreeSet, str::FromStr};
use tokio::sync::Barrier;
use wallet_core::{
    reconcile, Action, Actor, DiscoverySource, ExecError, Executor, FederationId, IdempotencyKey,
    Intent, IntentStatus, Journal, MockExecutor, Msat, Occurrence, OperationKind, OperationStatus,
    PerformOutcome, ReasonCode,
};
use wallet_fedimint::{
    CandidateRecord, CandidateState, FederationInfo, FedimintJournal, GatewayUrl, Invoice,
    MovePhase, MoveRecord, OperationId, Preimage, StructuralOutcome, WatchState,
    JOIN_NOOP_REOPEN_NOTE,
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
        created_at_ms: 0,
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
        // Non-None so the serde round-trip (`move_record_roundtrip`) proves the §2.3/§3 fields
        // persist through the JSON row unchanged.
        preimage: Some(Preimage([0x0b; 32])),
        receive_fee_quoted: Some(Msat(150)),
        send_fee_quoted: Some(Msat(250)),
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

fn invite() -> InviteCode {
    InviteCode::from_str(
        "fed11qgqpu8rhwden5te0vejkg6tdd9h8gepwd4cxcumxv4jzuen0duhsqqfqh6nl7sgk72caxfx8khtfnn8y436q3nhyrkev3qp8ugdhdllnh86qmp42pm",
    )
    .expect("valid invite code")
}

fn candidate(id: FederationId, state: CandidateState) -> CandidateRecord {
    CandidateRecord {
        id,
        invite: invite(),
        source: DiscoverySource::Manual,
        discovered_at_ms: 1_700_000_000_000,
        structural: StructuralOutcome::Passed,
        structural_checked_at_ms: 1_700_000_000_100,
        state,
        updated_at_ms: 1_700_000_000_200,
    }
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
        .set_status(&i.idempotency_key, IntentStatus::Failed, None)
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

/// The single-writer CAS claim: `set_status_if` wins when the stored status matches
/// `expected`, moving BOTH the intent row and the `PendingIndexKey` in the same dbtx; it
/// loses (no write at all) on a status mismatch, including once the intent has already moved
/// past `expected`; and it returns `Ok(false)` for a key with no stored intent.
#[tokio::test]
async fn set_status_if_cas() {
    let journal = mem_journal();
    let i = intent("cas", IntentStatus::Pending);
    journal.upsert(&i).await.expect("upsert");

    assert!(journal
        .set_status_if(
            &i.idempotency_key,
            IntentStatus::Pending,
            IntentStatus::Executing,
        )
        .await
        .expect("set_status_if"));
    assert_eq!(
        journal
            .get(&i.idempotency_key)
            .await
            .expect("get")
            .map(|i| i.status),
        Some(IntentStatus::Executing)
    );
    assert!(has_key(&journal.pending().await, "cas"));

    // A second claim against the now-stale `expected` (Pending) must not win: no change to
    // the intent row or either index.
    assert!(!journal
        .set_status_if(
            &i.idempotency_key,
            IntentStatus::Pending,
            IntentStatus::Executing,
        )
        .await
        .expect("set_status_if"));
    assert_eq!(
        journal
            .get(&i.idempotency_key)
            .await
            .expect("get")
            .map(|i| i.status),
        Some(IntentStatus::Executing)
    );

    // Winning claim moves the index too: Executing -> Failed via CAS leaves `pending()` and
    // enters `failed()`.
    assert!(journal
        .set_status_if(
            &i.idempotency_key,
            IntentStatus::Executing,
            IntentStatus::Failed,
        )
        .await
        .expect("set_status_if"));
    assert!(!has_key(&journal.pending().await, "cas"));
    assert!(has_key(&journal.failed().await, "cas"));

    // An absent key never matches any `expected`.
    let missing = IdempotencyKey("no-such-key".to_string());
    assert!(!journal
        .set_status_if(&missing, IntentStatus::Pending, IntentStatus::Executing)
        .await
        .expect("set_status_if"));
}

#[tokio::test]
async fn watch_state_roundtrip_and_default_seed() {
    let journal = mem_journal();

    assert_eq!(
        journal
            .get_watch_state()
            .await
            .expect("default watch state"),
        WatchState::default()
    );

    let state = WatchState {
        occurrence: 7,
        last_discover_ms: 1_700_000_000_000,
        discover_cursor: Some(fed(0x42)),
        discover_backlog: true,
        discover_rotation: vec![fed(0x41), fed(0x42), fed(0x43)],
    };
    journal
        .put_watch_state(&state)
        .await
        .expect("put watch state");

    assert_eq!(journal.get_watch_state().await.expect("watch state"), state);
}

#[tokio::test]
async fn watch_state_occurrence_advance_is_monotonic_and_preserves_discovery_fields() {
    let journal = mem_journal();
    let first = journal
        .advance_watch_occurrence()
        .await
        .expect("advance seeded state");
    assert_eq!(first.occurrence, 1);

    let seeded = WatchState {
        occurrence: 41,
        last_discover_ms: 10_000,
        discover_cursor: Some(fed(0x99)),
        discover_backlog: true,
        discover_rotation: vec![fed(0x98), fed(0x99)],
    };
    journal
        .put_watch_state(&seeded)
        .await
        .expect("put watch state");
    let advanced = journal
        .advance_watch_occurrence()
        .await
        .expect("advance watch state");

    assert_eq!(
        advanced,
        WatchState {
            occurrence: 42,
            ..seeded
        }
    );
    assert_eq!(
        journal.get_watch_state().await.expect("persisted advance"),
        advanced
    );
}

#[tokio::test]
async fn watch_state_discovery_update_preserves_advanced_occurrence() {
    let journal = mem_journal();
    let seeded = WatchState {
        occurrence: 7,
        last_discover_ms: 10_000,
        discover_cursor: Some(fed(0x10)),
        discover_backlog: false,
        discover_rotation: vec![fed(0x10), fed(0x20)],
    };
    journal
        .put_watch_state(&seeded)
        .await
        .expect("put watch state");
    let stale_pre_pass_state = journal
        .get_watch_state()
        .await
        .expect("read watch state before pass");

    journal
        .advance_watch_occurrence()
        .await
        .expect("advance occurrence during pass");
    let updated = journal
        .put_watch_discovery_state(
            Some(fed(0x20)),
            true,
            Some(20_000),
            vec![fed(0x20), fed(0x30)],
        )
        .await
        .expect("put discovery state");

    assert_eq!(stale_pre_pass_state.occurrence, 7);
    assert_eq!(
        updated,
        WatchState {
            occurrence: 8,
            last_discover_ms: 20_000,
            discover_cursor: Some(fed(0x20)),
            discover_backlog: true,
            discover_rotation: vec![fed(0x20), fed(0x30)],
        }
    );
    assert_eq!(
        journal.get_watch_state().await.expect("persisted update"),
        updated
    );
}

#[tokio::test]
async fn watch_state_fails_closed_on_corrupt_row() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&[0x0a], b"not valid json")
        .await
        .expect("corrupt watch row");
    dbtx.commit_tx_result().await.expect("commit corrupt row");

    let err = journal
        .get_watch_state()
        .await
        .expect_err("corrupt watch state fails closed");
    assert!(
        matches!(err, ExecError::Permanent(_)),
        "expected permanent corruption error, got {err:?}"
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

#[tokio::test]
async fn candidate_registry_round_trips_every_state_and_upserts() {
    let journal = mem_journal();
    for (n, state) in [
        (0x10, CandidateState::Rejected),
        (0x11, CandidateState::Discovered),
        (0x12, CandidateState::AutoJoined),
        (0x13, CandidateState::UserApproved),
    ] {
        let mut rec = candidate(fed(n), state);
        if state == CandidateState::Rejected {
            rec.structural = StructuralOutcome::Rejected("missing module".to_owned());
        }
        journal.put_candidate(&rec).await.expect("put candidate");
        assert_eq!(
            journal.get_candidate(&rec.id).await.expect("get candidate"),
            Some(rec.clone())
        );
    }

    let mut listed = journal.list_candidates().await.expect("list candidates");
    listed.sort_by_key(|(id, _)| *id);
    assert_eq!(listed.len(), 4);
    assert_eq!(listed[0].1.state, CandidateState::Rejected);
    assert_eq!(listed[1].1.state, CandidateState::Discovered);
    assert_eq!(listed[2].1.state, CandidateState::AutoJoined);
    assert_eq!(listed[3].1.state, CandidateState::UserApproved);

    let mut replacement = candidate(fed(0x11), CandidateState::AutoJoined);
    replacement.updated_at_ms = 1_700_000_999_999;
    journal
        .put_candidate(&replacement)
        .await
        .expect("upsert replacement");
    assert_eq!(
        journal
            .get_candidate(&fed(0x11))
            .await
            .expect("get replacement"),
        Some(replacement),
        "put_candidate replaces the one row for that federation"
    );
}

#[tokio::test]
async fn list_candidates_skips_poison_rows() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    let good_id = fed(0x21);
    let good = candidate(good_id, CandidateState::Discovered);
    journal.put_candidate(&good).await.expect("put good");

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&tagged_key(0x09, &[0x22; 32]), b"not valid json")
        .await
        .expect("insert corrupt candidate row");
    dbtx.raw_insert_bytes(&tagged_key(0x09, &[0x33; 8]), b"{}")
        .await
        .expect("insert malformed-key candidate row");
    dbtx.commit_tx_result().await.expect("commit poison rows");

    let listed = journal.list_candidates().await.expect("list candidates");
    assert_eq!(listed, vec![(good_id, good)]);

    let report = journal
        .list_candidates_report()
        .await
        .expect("candidate report");
    assert_eq!(
        report.candidates,
        vec![(good_id, candidate(good_id, CandidateState::Discovered))]
    );
    assert_eq!(report.skipped_ids, BTreeSet::from([fed(0x22)]));
    assert_eq!(report.skipped_rows, 2);
    // The malformed-key row's value (`{}`) is not a decodable candidate, so its id is
    // unrecoverable: it counts against the concurrent cap but cannot be gate-attributed.
    assert_eq!(report.skipped_unidentified, 1);
}

#[tokio::test]
async fn list_candidates_recovers_id_from_malformed_key_row() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    // A malformed-key row (only 8 id bytes after the tag) whose VALUE still decodes to a valid
    // AutoJoined candidate for `embedded_id`. The id must be recovered from the value so the row
    // fails closed against BOTH the funding gate and the concurrent cap instead of vanishing.
    let embedded_id = fed(0x52);
    let auto = candidate(embedded_id, CandidateState::AutoJoined);

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&tagged_key(0x09, &[0x51; 8]), &encoded_test_row(&auto))
        .await
        .expect("insert malformed-key row with decodable value");
    dbtx.commit_tx_result()
        .await
        .expect("commit malformed-key row");

    let report = journal
        .list_candidates_report()
        .await
        .expect("candidate report");
    assert_eq!(
        report.candidates,
        Vec::new(),
        "the malformed-key row is not listed"
    );
    assert_eq!(
        report.skipped_ids,
        BTreeSet::from([embedded_id]),
        "the embedded id is recovered from the value"
    );
    assert_eq!(report.skipped_rows, 1);
    assert_eq!(
        report.skipped_unidentified, 0,
        "an id was recovered, so nothing is unidentified"
    );

    // It counts fail-closed against the concurrent cap (could be an unproven AutoJoined
    // partition), and drops out once its recovered id is known to have Passed.
    assert_eq!(
        journal
            .concurrent_unproven(&BTreeSet::new())
            .await
            .expect("concurrent count"),
        1,
        "the recovered malformed-key AutoJoined id counts against the concurrent cap"
    );
    assert_eq!(
        journal
            .concurrent_unproven(&BTreeSet::from([embedded_id]))
            .await
            .expect("concurrent count with passed"),
        0,
        "once the recovered id has Passed it no longer counts"
    );
}

#[tokio::test]
async fn candidate_registry_treats_embedded_id_mismatch_as_poison() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    let key_id = fed(0x25);
    let embedded_id = fed(0x26);
    let mismatched = candidate(embedded_id, CandidateState::AutoJoined);

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&tagged_key(0x09, &key_id.0), &encoded_test_row(&mismatched))
        .await
        .expect("insert mismatched candidate row");
    dbtx.commit_tx_result()
        .await
        .expect("commit mismatched row");

    let err = journal
        .get_candidate(&key_id)
        .await
        .expect_err("targeted read fails closed on mismatched embedded id");
    assert!(
        matches!(
            err,
            ExecError::Permanent(ref msg)
                if msg.contains("candidate row key id")
                    && msg.contains(&key_id.to_hex())
                    && msg.contains(&embedded_id.to_hex())
        ),
        "unexpected error: {err:?}"
    );

    assert_eq!(
        journal.list_candidates().await.expect("list candidates"),
        Vec::new(),
        "bulk scans skip the mismatched row instead of returning it under either id"
    );

    let report = journal
        .list_candidates_report()
        .await
        .expect("candidate report");
    assert_eq!(report.candidates, Vec::new());
    assert_eq!(report.skipped_ids, BTreeSet::from([key_id]));
    assert_eq!(report.skipped_rows, 1);
    // The key is well-formed, so the id came from the key, not an unrecoverable value.
    assert_eq!(report.skipped_unidentified, 0);
}

#[tokio::test]
async fn put_candidate_overwrites_a_corrupt_row_so_get_recovers() {
    // The CLI user-join path (mark_candidate_user_approved) recovers a poisoned 0x09 row by
    // OVERWRITING it with a fresh UserApproved record rather than bailing — otherwise an
    // EXPLICITLY user-joined fed stays fail-closed to AutoJoined behind the probe gate (a
    // corrupt/skipped id counts as AutoJoined in auto_joined_candidates). This pins the
    // load-bearing mechanism the fix depends on: put_candidate over a corrupt key makes the
    // targeted get_candidate read the fresh row.
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    let id = fed(0x27);

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&tagged_key(0x09, &id.0), b"not valid json")
        .await
        .expect("insert corrupt candidate row");
    dbtx.commit_tx_result().await.expect("commit corrupt row");

    journal
        .get_candidate(&id)
        .await
        .expect_err("a corrupt row fails closed on the targeted read");

    let recovered = candidate(id, CandidateState::UserApproved);
    journal
        .put_candidate(&recovered)
        .await
        .expect("overwrite the corrupt row");

    assert_eq!(
        journal
            .get_candidate(&id)
            .await
            .expect("get after overwrite"),
        Some(recovered),
        "put_candidate overwrites the poisoned key so the fed is recovered as UserApproved"
    );
}

#[tokio::test]
async fn concurrent_unproven_counts_only_auto_joined_rows_without_passed_probe() {
    let journal = mem_journal();
    journal
        .put_candidate(&candidate(fed(0x31), CandidateState::AutoJoined))
        .await
        .expect("put auto unproven");
    journal
        .put_candidate(&candidate(fed(0x32), CandidateState::AutoJoined))
        .await
        .expect("put auto passed");
    journal
        .put_candidate(&candidate(fed(0x33), CandidateState::UserApproved))
        .await
        .expect("put approved");
    journal
        .put_candidate(&candidate(fed(0x34), CandidateState::Discovered))
        .await
        .expect("put discovered");

    let passed = BTreeSet::from([fed(0x32)]);
    assert_eq!(
        journal
            .concurrent_unproven(&passed)
            .await
            .expect("concurrent count"),
        1,
        "only AutoJoined rows without Passed probe evidence count"
    );
}

#[tokio::test]
async fn concurrent_unproven_counts_undecodable_rows_fail_closed() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    // A live unproven auto-joined partition.
    journal
        .put_candidate(&candidate(fed(0x41), CandidateState::AutoJoined))
        .await
        .expect("put auto unproven");

    // A corrupt candidate row: its state is unknowable, so it counts against the concurrent cap
    // (it could be an unproven AutoJoined partition), never silently dropped — mirroring the
    // funding gate's fail-closed treatment of the same skipped ids.
    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&tagged_key(0x09, &[0x42; 32]), b"not valid json")
        .await
        .expect("insert corrupt candidate row");
    dbtx.commit_tx_result().await.expect("commit poison row");

    assert_eq!(
        journal
            .concurrent_unproven(&BTreeSet::new())
            .await
            .expect("concurrent count"),
        2,
        "a corrupt candidate row counts fail-closed alongside the live unproven auto-joined row"
    );

    // A skipped id that has since PASSED its probe is excluded (conservative, not double-fail).
    assert_eq!(
        journal
            .concurrent_unproven(&BTreeSet::from([fed(0x42)]))
            .await
            .expect("concurrent count with passed skip"),
        1,
        "a corrupt row whose id has since passed no longer counts against the concurrent cap"
    );
}

#[tokio::test]
async fn concurrent_unproven_counts_unidentified_rows_fail_closed() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    // A live unproven auto-joined partition.
    journal
        .put_candidate(&candidate(fed(0x61), CandidateState::AutoJoined))
        .await
        .expect("put auto unproven");

    // A doubly-corrupt row: BOTH the key (8 id bytes) AND the value are undecodable, so no id can
    // be recovered. It cannot be Passed-filtered, so it counts unconditionally against the cap —
    // the fully-conservative direction (it could be an unproven AutoJoined partition).
    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&tagged_key(0x09, &[0x62; 8]), b"not valid json")
        .await
        .expect("insert doubly-corrupt candidate row");
    dbtx.commit_tx_result().await.expect("commit poison row");

    let report = journal
        .list_candidates_report()
        .await
        .expect("candidate report");
    assert!(
        report.skipped_ids.is_empty(),
        "no id could be recovered from the doubly-corrupt row"
    );
    assert_eq!(report.skipped_unidentified, 1);

    // Even filtering by an arbitrary passed set cannot subtract an unidentified row, so it stays
    // counted alongside the live unproven partition.
    assert_eq!(
        journal
            .concurrent_unproven(&BTreeSet::from([fed(0x99)]))
            .await
            .expect("concurrent count"),
        2,
        "an unidentifiable corrupt candidate row counts fail-closed against the concurrent cap"
    );
}

#[tokio::test]
async fn auto_join_history_counts_agent_created_partitions() {
    const DAY_MS: u64 = 24 * 60 * 60 * 1000;
    let journal = mem_journal();
    let actor = Actor::Agent {
        occurrence: Occurrence(7),
    };

    async fn write_join(
        journal: &FedimintJournal,
        suffix: &str,
        fed_id: FederationId,
        actor: Actor,
        started_at_ms: u64,
        status: OperationStatus,
        error: Option<&str>,
    ) {
        let key = IdempotencyKey(format!("join:{}:{suffix}", fed_id.to_hex()));
        journal
            .record_started(
                &key,
                OperationKind::Join { fed: fed_id },
                actor,
                ReasonCode::StandingInstruction,
                started_at_ms,
                None,
            )
            .await
            .expect("record started");
        journal
            .record_terminal(&key, status, started_at_ms + 1, error, None)
            .await
            .expect("record terminal");
    }

    async fn write_started_join(
        journal: &FedimintJournal,
        suffix: &str,
        fed_id: FederationId,
        actor: Actor,
        started_at_ms: u64,
    ) {
        let key = IdempotencyKey(format!("join:{}:{suffix}", fed_id.to_hex()));
        journal
            .record_started(
                &key,
                OperationKind::Join { fed: fed_id },
                actor,
                ReasonCode::StandingInstruction,
                started_at_ms,
                None,
            )
            .await
            .expect("record started");
    }

    let now = 10 * DAY_MS;
    write_join(
        &journal,
        "recent-new",
        fed(0x41),
        actor,
        now - DAY_MS,
        OperationStatus::Succeeded,
        None,
    )
    .await;
    write_join(
        &journal,
        "old-new",
        fed(0x42),
        actor,
        now - 8 * DAY_MS,
        OperationStatus::Succeeded,
        None,
    )
    .await;
    write_join(
        &journal,
        "failed",
        fed(0x43),
        actor,
        now - DAY_MS,
        OperationStatus::Failed,
        Some("network error"),
    )
    .await;
    write_join(
        &journal,
        "noop",
        fed(0x44),
        actor,
        now - DAY_MS,
        OperationStatus::Succeeded,
        Some(JOIN_NOOP_REOPEN_NOTE),
    )
    .await;
    write_join(
        &journal,
        "user",
        fed(0x45),
        Actor::User,
        now - DAY_MS,
        OperationStatus::Succeeded,
        None,
    )
    .await;

    let crashed_after_registry = fed(0x47);
    let crashed_started_at = now - DAY_MS;
    write_started_join(
        &journal,
        "crashed-after-registry",
        crashed_after_registry,
        actor,
        crashed_started_at,
    )
    .await;
    journal
        .put_federation(
            &crashed_after_registry,
            &FederationInfo {
                invite: "fed1crashed".to_string(),
                db_prefix: 47,
                joined_at: crashed_started_at / 1000,
            },
        )
        .await
        .expect("put crashed registry evidence");

    let slow_crashed_after_registry = fed(0x4B);
    let slow_crashed_started_at = now - DAY_MS;
    write_started_join(
        &journal,
        "slow-crashed-after-registry",
        slow_crashed_after_registry,
        actor,
        slow_crashed_started_at,
    )
    .await;
    journal
        .put_federation(
            &slow_crashed_after_registry,
            &FederationInfo {
                invite: "fed1slowcrashed".to_string(),
                db_prefix: 51,
                joined_at: slow_crashed_started_at.saturating_add(2 * 60 * 1000) / 1000,
            },
        )
        .await
        .expect("put slow crashed registry evidence");

    let started_without_registry = fed(0x48);
    write_started_join(
        &journal,
        "started-without-registry",
        started_without_registry,
        actor,
        now - DAY_MS,
    )
    .await;

    let preexisting_registry = fed(0x49);
    let preexisting_started_at = now - DAY_MS;
    write_started_join(
        &journal,
        "preexisting-registry",
        preexisting_registry,
        actor,
        preexisting_started_at,
    )
    .await;
    journal
        .put_federation(
            &preexisting_registry,
            &FederationInfo {
                invite: "fed1preexisting".to_string(),
                db_prefix: 49,
                joined_at: preexisting_started_at.saturating_sub(2 * 60 * 1000) / 1000,
            },
        )
        .await
        .expect("put preexisting registry evidence");

    let stale_before_later_user_join = fed(0x4A);
    let stale_started_at = now - 2 * DAY_MS;
    write_started_join(
        &journal,
        "stale-before-later-user-join",
        stale_before_later_user_join,
        actor,
        stale_started_at,
    )
    .await;
    journal
        .put_federation(
            &stale_before_later_user_join,
            &FederationInfo {
                invite: "fed1lateruser".to_string(),
                db_prefix: 50,
                joined_at: now / 1000,
            },
        )
        .await
        .expect("put later user registry evidence");

    assert_eq!(
        journal.weekly_auto_joins(now).await.expect("weekly count"),
        4,
        "weekly counts recent terminal Agent joins plus registry-backed crashed Agent joins"
    );
    assert_eq!(
        journal.lifetime_auto_joins().await.expect("lifetime count"),
        5,
        "lifetime counts terminal Agent joins plus registry-backed crashed Agent joins"
    );
    assert!(
        journal
            .agent_created_federation(&fed(0x41))
            .await
            .expect("agent-created recent"),
        "a successful new-partition Agent join identifies the fed as agent-created"
    );
    assert!(
        journal
            .agent_created_federation(&fed(0x42))
            .await
            .expect("agent-created old"),
        "old successful Agent joins still count for ownership recovery"
    );
    assert!(
        journal
            .agent_created_federation(&crashed_after_registry)
            .await
            .expect("agent-created from registry-backed started join"),
        "a registry-backed non-terminal Agent join identifies the crash-after-partition case"
    );
    assert!(
        journal
            .agent_created_federation(&slow_crashed_after_registry)
            .await
            .expect("agent-created from slow registry-backed started join"),
        "a slow registry-backed non-terminal Agent join is recovered fail-closed"
    );
    assert!(
        journal
            .agent_created_federation(&stale_before_later_user_join)
            .await
            .expect("agent-created from stale registry-backed started join"),
        "an old non-terminal Agent join backed by a later registry row is ambiguous, so recovery counts it fail-closed"
    );
    for id in [
        fed(0x43),
        fed(0x44),
        fed(0x45),
        fed(0x46),
        started_without_registry,
        preexisting_registry,
    ] {
        assert!(
            !journal
                .agent_created_federation(&id)
                .await
                .expect("non agent-created"),
            "failed, no-op, user, absent, and post-registry started joins are not agent-created evidence"
        );
    }
}

#[tokio::test]
async fn auto_join_history_counts_corrupt_ledger_rows_fail_closed() {
    const DAY_MS: u64 = 24 * 60 * 60 * 1000;
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());

    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&tagged_key(0x05, &99_u64.to_be_bytes()), b"not valid json")
        .await
        .expect("insert corrupt ledger row");
    dbtx.commit_tx_result().await.expect("commit poison row");

    let now = 10 * DAY_MS;
    assert_eq!(
        journal.weekly_auto_joins(now).await.expect("weekly count"),
        1,
        "a corrupt ledger row may be a recent successful Agent join, so it counts fail-closed"
    );
    assert_eq!(
        journal.lifetime_auto_joins().await.expect("lifetime count"),
        1,
        "a corrupt ledger row may be a successful Agent join, so it counts fail-closed"
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

/// `Journal::store_id` (the identity `drive`'s process-local in-flight-performs guard uses
/// to recognize "same store") must match for two INDEPENDENTLY-CONSTRUCTED
/// `FedimintJournal`s over the SAME `Database` — the cross-handle sharing
/// `shared_database_handle_persists` already proves for reads/writes — and must NOT match
/// for two unrelated `Database`s, or the guard would wrongly conflate unrelated stores.
#[tokio::test]
async fn store_id_matches_only_for_the_same_database() {
    let db = MemDatabase::new().into_database();
    let a = FedimintJournal::new(db.clone());
    let b = FedimintJournal::new(db);
    assert_eq!(
        a.store_id(),
        b.store_id(),
        "two handles over the same Database must share one store_id"
    );

    let unrelated = FedimintJournal::new(MemDatabase::new().into_database());
    assert_ne!(
        a.store_id(),
        unrelated.store_id(),
        "handles over unrelated Databases must not collide"
    );
}

/// A `Journal` wrapper that forces its `set_status_if` (the CAS claim point) to rendezvous at
/// a `Barrier` before delegating — see `wallet-core`'s `concurrent_drive_performs_once`, which
/// uses the identical technique so the race under test is real rather than accidentally
/// serialized by the runtime finishing one `reconcile` before the other starts.
struct BarrierJournal {
    inner: FedimintJournal,
    barrier: Arc<Barrier>,
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

    async fn pending(&self) -> Vec<Intent> {
        self.inner.pending().await
    }

    async fn failed(&self) -> Vec<Intent> {
        self.inner.failed().await
    }

    fn store_id(&self) -> usize {
        self.inner.store_id()
    }
}

/// Without `store_id` correctly identifying shared storage, `drive`'s in-flight guard keys on
/// each `FedimintJournal` wrapper's OWN address, so two independently-constructed handles
/// over the SAME `Database` (a documented, supported pattern — see
/// `shared_database_handle_persists`) would each think they hold an unclaimed store and both
/// call `perform`. This exercises `reconcile` through exactly that two-handle setup and
/// asserts exactly one performs.
#[tokio::test]
async fn shared_database_handle_dedupes_concurrent_perform() {
    let db = MemDatabase::new().into_database();
    let barrier = Arc::new(Barrier::new(2));
    let a = Arc::new(BarrierJournal {
        inner: FedimintJournal::new(db.clone()),
        barrier: Arc::clone(&barrier),
    });
    let b = Arc::new(BarrierJournal {
        inner: FedimintJournal::new(db),
        barrier,
    });
    assert_eq!(a.store_id(), b.store_id(), "both wrap the same Database");

    let i = intent("shared-perform", IntentStatus::Pending);
    a.upsert(&i).await.expect("upsert");

    let executor = Arc::new(MockExecutor::new());
    let (ea, eb) = (Arc::clone(&executor), Arc::clone(&executor));
    let task_a = tokio::spawn(async move { reconcile(a.as_ref(), ea.as_ref()).await });
    let task_b = tokio::spawn(async move { reconcile(b.as_ref(), eb.as_ref()).await });
    let (ra, rb) = tokio::join!(task_a, task_b);
    let (ra, rb) = (ra.expect("task a join"), rb.expect("task b join"));

    assert_eq!(
        ra.performed + rb.performed,
        1,
        "exactly one of the two concurrent reconciles performs the shared intent"
    );
    assert_eq!(executor.performed_keys().len(), 1);
}

/// An executor that holds its first `perform` open until released, so a second concurrent
/// `drive` for the same (already `Executing`) intent is observable without hanging the test.
/// Mirrors `wallet-core`'s test double of the same name.
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

/// The CAS itself cannot cover an intent that is ALREADY `Executing` (past the CAS claim,
/// e.g. a resumed crash-recovery drive still mid-`perform`), so a second `reconcile` skips
/// `set_status_if` entirely (see `drive`) and its only defense against a duplicate `perform`
/// is the process-local in-flight guard keyed by `store_id`.
/// `shared_database_handle_dedupes_concurrent_perform` above races the CAS (a `Pending`
/// intent), which the durable dbtx already serializes on its own; this test instead starts
/// `Executing` and uses a blocking executor so the in-flight guard is the ONLY thing that can
/// prevent the second, independently-constructed handle from calling `perform` again.
#[tokio::test]
async fn shared_database_handle_dedupes_concurrent_executing_perform() {
    let db = MemDatabase::new().into_database();
    let a = FedimintJournal::new(db.clone());
    let b = FedimintJournal::new(db);
    assert_eq!(a.store_id(), b.store_id(), "both wrap the same Database");

    let i = intent("shared-executing", IntentStatus::Executing);
    a.upsert(&i).await.expect("upsert");

    let executor = Arc::new(BlockingExecutor::new());
    let (a, ea) = (Arc::new(a), Arc::clone(&executor));
    let task_a = tokio::spawn(async move { reconcile(a.as_ref(), ea.as_ref()).await });

    executor.first_entered.wait().await;

    let result_b = reconcile(&b, executor.as_ref()).await;
    assert_eq!(
        (result_b.performed, result_b.skipped),
        (0, 1),
        "the second handle must skip the in-flight perform, not repeat it"
    );
    assert_eq!(executor.performed_keys().len(), 1);

    executor.release_first.wait().await;
    let result_a = task_a.await.expect("task a join");
    assert_eq!(result_a.performed, 1);
    assert_eq!(executor.performed_keys().len(), 1);
    assert_eq!(
        b.get(&i.idempotency_key)
            .await
            .expect("get")
            .unwrap()
            .status,
        IntentStatus::Done
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
        .set_status(&key, IntentStatus::Awaiting, None)
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
        .set_status(&key, IntentStatus::Done, None)
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
