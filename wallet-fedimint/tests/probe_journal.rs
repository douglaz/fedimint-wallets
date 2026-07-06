//! §5.0.8 journal suites for the `0x08` probe state (`MemDatabase` — no devimint, no
//! money path): time-aware retention (+ the 256 backstop), the Expired-vs-NeverProbed
//! evidence, fail-closed targeted reads, and the one-dbtx session/attempt/umbrella
//! outcome write.

use fedimint_core::db::mem_impl::MemDatabase;
use fedimint_core::db::IDatabaseTransactionOpsCore;
use fedimint_core::db::IRawDatabaseExt;
use wallet_core::{
    probe_verdict, ActiveProbeVerdict, Actor, FederationId, IdempotencyKey, Msat, OperationKind,
    OperationStatus, ProbeAttempt, ProbePolicy, ReasonCode,
};
use wallet_fedimint::{
    prune_probe_attempts, FedimintJournal, OperationRef, ProbeSession, PROBE_HISTORY_CAP,
};

const NOW: u64 = 1_000_000_000_000;
const HOUR: u64 = 3_600_000;
const DAY: u64 = 24 * HOUR;

fn fed(n: u8) -> FederationId {
    FederationId([n; 32])
}

fn clock_now() -> u64 {
    NOW
}

fn mem_journal() -> FedimintJournal {
    FedimintJournal::with_clock(MemDatabase::new().into_database(), clock_now)
}

fn attempt(at_ms: u64, ok: bool) -> ProbeAttempt {
    ProbeAttempt {
        at_ms,
        ok,
        from: fed(1),
        amount_msat: 20_000,
        leg_fee_cap_msat: 10_000,
        error: (!ok).then(|| "probe leg OUT failed: rejected".to_string()),
    }
}

fn session(nonce: &str) -> ProbeSession {
    ProbeSession {
        nonce: nonce.to_string(),
        from: fed(1),
        amount_msat: 20_000,
        leg_fee_cap_msat: 10_000,
        c_spendable_before_in_msat: 0,
        out_net_msat: None,
        started_at_ms: NOW,
    }
}

fn probe_kind(cost: Option<Msat>) -> OperationKind {
    OperationKind::Probe {
        fed: fed(2),
        from: fed(1),
        amount_msat: Msat(20_000),
        cost_msat: cost,
    }
}

// ---- retention (pure `prune_probe_attempts`) --------------------------------------

#[test]
fn retention_keeps_window_plus_newest_success_and_newest_attempt() {
    // Stale failure + two stale successes + a fresh failure: the sub-ttl attempt and the
    // newest attempt survive by their own rules; of the stale rows only the NEWEST
    // success is retained (the Expired-vs-NeverProbed evidence).
    let attempts = vec![
        attempt(NOW - 30 * DAY, false),
        attempt(NOW - 20 * DAY, true),
        attempt(NOW - 15 * DAY, true),
        attempt(NOW - HOUR, false),
    ];
    let kept = prune_probe_attempts(attempts, NOW);
    assert_eq!(
        kept,
        vec![attempt(NOW - 15 * DAY, true), attempt(NOW - HOUR, false)]
    );
}

#[test]
fn retention_keeps_a_lone_stale_success_and_attempt_forever() {
    // A single stale success is BOTH the newest success and the newest attempt: retained
    // regardless of age, so a stale pass can never silently decay into NeverProbed.
    let attempts = vec![attempt(NOW - 90 * DAY, true)];
    assert_eq!(prune_probe_attempts(attempts.clone(), NOW), attempts);
}

#[test]
fn retention_keeps_the_newest_stale_success_per_source() {
    // `probe_verdict` qualifies a stale success by its SOURCE, so retention must keep the
    // newest stale success for EACH source — a newer success from a DIFFERENT source must
    // not evict the older source's Expired evidence. Two stale successes from distinct
    // sources: BOTH survive.
    let from_a = |ok: bool, at: u64| ProbeAttempt {
        at_ms: at,
        ok,
        from: fed(1),
        amount_msat: 20_000,
        leg_fee_cap_msat: 10_000,
        error: (!ok).then(|| "x".to_string()),
    };
    let from_b = |ok: bool, at: u64| ProbeAttempt {
        from: fed(9),
        ..from_a(ok, at)
    };
    let older_a = from_a(true, NOW - 20 * DAY);
    let newer_b = from_b(true, NOW - 10 * DAY);
    let kept = prune_probe_attempts(vec![older_a.clone(), newer_b.clone()], NOW);
    assert!(
        kept.contains(&older_a),
        "the older source-A stale success must survive a newer source-B success"
    );
    assert!(kept.contains(&newer_b));
    // Each still reads Expired for its own source (a qualifying pair round-trip aged out).
    assert_eq!(
        probe_verdict(&kept, fed(1), NOW, &ProbePolicy::default()),
        ActiveProbeVerdict::Expired
    );
    assert_eq!(
        probe_verdict(&kept, fed(9), NOW, &ProbePolicy::default()),
        ActiveProbeVerdict::Expired
    );
}

#[test]
fn retention_keeps_a_default_sized_pass_through_a_later_smoke_probe() {
    // From ONE source: an older DEFAULT-sized (qualifying) success, then a newer WEAKER
    // smoke probe (smaller amount). Retention must keep BOTH — the newest success (the
    // smoke) AND the newest default-qualifying success (the older pass) — so once both age
    // out, `status` under the default policy still reads Expired, not NeverProbed.
    let default_ok = |at: u64| ProbeAttempt {
        at_ms: at,
        ok: true,
        from: fed(1),
        amount_msat: 20_000,
        leg_fee_cap_msat: 10_000,
        error: None,
    };
    let smoke_ok = |at: u64| ProbeAttempt {
        amount_msat: 5_000, // below the default -> non-qualifying under the default policy
        ..default_ok(at)
    };
    let older_default = default_ok(NOW - 20 * DAY);
    let newer_smoke = smoke_ok(NOW - 10 * DAY);
    let kept = prune_probe_attempts(vec![older_default.clone(), newer_smoke.clone()], NOW);
    assert!(
        kept.contains(&older_default),
        "a weaker smoke probe must not evict the older DEFAULT-sized pass's Expired evidence"
    );
    assert_eq!(
        probe_verdict(&kept, fed(1), NOW, &ProbePolicy::default()),
        ActiveProbeVerdict::Expired
    );
}

#[test]
fn retention_enforces_the_hard_bound_of_256_newest() {
    // 300 fresh (in-window) attempts: the count backstop wins, keeping the newest 256.
    let attempts: Vec<ProbeAttempt> = (0..300u64).map(|i| attempt(NOW - 300 + i, true)).collect();
    let kept = prune_probe_attempts(attempts.clone(), NOW);
    assert_eq!(kept.len(), PROBE_HISTORY_CAP);
    assert_eq!(kept, attempts[300 - PROBE_HISTORY_CAP..].to_vec());
}

// ---- Expired vs NeverProbed over durable state -------------------------------------

#[tokio::test]
async fn stale_pass_then_silence_still_reads_expired() {
    let journal = mem_journal();
    let key = IdempotencyKey("probe:aa:nonce-expired".to_string());
    // A success recorded 30 days ago (the clock is pinned to NOW; the attempt's own
    // at_ms carries its age), then nothing. Retention keeps it as the newest success.
    journal
        .begin_probe_session(&fed(2), &session("nonce-expired"))
        .await
        .expect("begin session");
    journal
        .record_probe_outcome(
            &fed(2),
            "nonce-expired",
            Some(attempt(NOW - 30 * DAY, true)),
            &key,
            probe_kind(Some(Msat(5_000))),
            Actor::User,
            OperationStatus::Succeeded,
            None,
        )
        .await
        .expect("record outcome");
    let record = journal
        .probe_record(&fed(2))
        .await
        .expect("read")
        .expect("row exists");
    assert_eq!(record.attempts.len(), 1);
    assert_eq!(
        probe_verdict(&record.attempts, fed(1), NOW, &ProbePolicy::default()),
        ActiveProbeVerdict::Expired,
        "a stale pass must read Expired, never NeverProbed"
    );
}

// ---- fail-closed targeted read ------------------------------------------------------

#[tokio::test]
async fn probe_record_fails_closed_on_a_corrupt_row() {
    let db = MemDatabase::new().into_database();
    let journal = FedimintJournal::new(db.clone());
    // Corrupt the `0x08 ++ fed` row inside the `[0x00]` app partition.
    let app_db = db.with_prefix(vec![0x00]);
    let mut raw_key = vec![0x08u8];
    raw_key.extend_from_slice(&fed(2).0);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&raw_key, b"not valid json")
        .await
        .expect("insert corrupt probe row");
    dbtx.commit_tx_result().await.expect("commit corrupt row");

    journal
        .probe_record(&fed(2))
        .await
        .expect_err("a corrupt probe row must surface an error, never read as 'no session'");
}

// ---- session lifecycle + the ONE-dbtx outcome write ---------------------------------

#[tokio::test]
async fn begin_writes_in_flight_and_outcome_clears_appends_and_terminalizes_atomically() {
    let journal = mem_journal();
    let umbrella = IdempotencyKey("probe:bb:lifecycle-nonce".to_string());

    let s = ProbeSession {
        out_net_msat: Some(15_000),
        ..session("lifecycle-nonce")
    };
    journal
        .begin_probe_session(&fed(2), &s)
        .await
        .expect("begin session");
    assert_eq!(
        journal
            .probe_record(&fed(2))
            .await
            .expect("read")
            .expect("row")
            .in_flight,
        Some(s),
        "begin_probe_session must persist the in-flight session"
    );

    // The one terminal write: session cleared + attempt appended + umbrella row created
    // (the crash-between-session-and-record_started window: create-or-advance) — and the
    // final cost stamped onto the kind. All observable effects land together.
    let failed = attempt(NOW - HOUR, false);
    journal
        .record_probe_outcome(
            &fed(2),
            "lifecycle-nonce",
            Some(failed.clone()),
            &umbrella,
            probe_kind(Some(Msat(20_500))),
            Actor::User,
            OperationStatus::Failed,
            Some("probe leg OUT failed: rejected"),
        )
        .await
        .expect("record outcome");

    let record = journal
        .probe_record(&fed(2))
        .await
        .expect("read")
        .expect("row");
    assert_eq!(record.in_flight, None, "a terminal exit clears the session");
    assert_eq!(record.attempts, vec![failed]);

    let row = journal
        .operation(&OperationRef::Key(umbrella.clone()))
        .await
        .expect("read umbrella")
        .expect("umbrella row exists");
    assert_eq!(row.status, OperationStatus::Failed);
    assert_eq!(row.reason, ReasonCode::ActiveProbe);
    assert_eq!(row.error.as_deref(), Some("probe leg OUT failed: rejected"));
    assert_eq!(row.kind, probe_kind(Some(Msat(20_500))));
}

#[tokio::test]
async fn outcome_advances_an_existing_started_umbrella_row_and_stamps_cost() {
    let journal = mem_journal();
    let umbrella = IdempotencyKey("probe:cc:advance-nonce".to_string());
    journal
        .record_started(
            &umbrella,
            probe_kind(None),
            Actor::User,
            ReasonCode::ActiveProbe,
            NOW - HOUR,
            None,
        )
        .await
        .expect("record started");
    journal
        .begin_probe_session(&fed(2), &session("advance-nonce"))
        .await
        .expect("begin session");

    journal
        .record_probe_outcome(
            &fed(2),
            "advance-nonce",
            Some(attempt(NOW, true)),
            &umbrella,
            probe_kind(Some(Msat(700))),
            Actor::User,
            OperationStatus::Succeeded,
            None,
        )
        .await
        .expect("record outcome");

    let row = journal
        .operation(&OperationRef::Key(umbrella))
        .await
        .expect("read umbrella")
        .expect("umbrella row exists");
    assert_eq!(row.status, OperationStatus::Succeeded);
    assert_eq!(
        row.created_at_ms,
        NOW - HOUR,
        "advance preserves the Started row's identity"
    );
    assert_eq!(
        row.kind,
        probe_kind(Some(Msat(700))),
        "cost is filled at terminalization"
    );
    assert_eq!(row.error, None);
    // And a NO-ATTEMPT outcome on a fresh fed would leave history untouched — asserted
    // here by the attempt list holding exactly the one recorded attempt.
    let record = journal
        .probe_record(&fed(2))
        .await
        .expect("read")
        .expect("row");
    assert_eq!(record.attempts.len(), 1);
    assert_eq!(record.in_flight, None);
}

#[tokio::test]
async fn no_attempt_outcome_clears_the_session_without_touching_history() {
    let journal = mem_journal();
    let umbrella = IdempotencyKey("probe:dd:noattempt-nonce".to_string());
    journal
        .begin_probe_session(&fed(2), &session("noattempt-nonce"))
        .await
        .expect("begin session");

    journal
        .record_probe_outcome(
            &fed(2),
            "noattempt-nonce",
            None,
            &umbrella,
            probe_kind(None),
            Actor::User,
            OperationStatus::Failed,
            Some("no lnv2 gateway serves both federations"),
        )
        .await
        .expect("record outcome");

    let record = journal
        .probe_record(&fed(2))
        .await
        .expect("read")
        .expect("row");
    assert_eq!(record.in_flight, None, "the stale session must not survive");
    assert!(
        record.attempts.is_empty(),
        "a no-attempt exit writes NO verdict evidence (§5.0.3 scoping)"
    );
    let row = journal
        .operation(&OperationRef::Key(umbrella))
        .await
        .expect("read umbrella")
        .expect("umbrella row exists");
    assert_eq!(row.status, OperationStatus::Failed);
    assert_eq!(
        row.error.as_deref(),
        Some("no lnv2 gateway serves both federations")
    );
}

#[tokio::test]
async fn begin_refuses_to_clobber_a_live_session() {
    // §5.0.5: resume runs FIRST, so `begin_probe_session` must never overwrite a live
    // session — doing so would orphan the prior session's legs + umbrella row. A caller
    // that reaches begin with `in_flight` already set skipped resume; refuse loudly.
    let journal = mem_journal();
    let first = session("nonce-live");
    journal
        .begin_probe_session(&fed(2), &first)
        .await
        .expect("first session begins");

    let err = journal
        .begin_probe_session(&fed(2), &session("nonce-second"))
        .await
        .expect_err("a DIFFERENT-nonce begin over a live session must refuse");
    assert!(
        format!("{err:?}").contains("in-flight probe"),
        "the refusal names the live probe: {err:?}"
    );

    // The original session is untouched.
    let record = journal
        .probe_record(&fed(2))
        .await
        .expect("read")
        .expect("row");
    assert_eq!(record.in_flight, Some(first.clone()));

    // A SAME-nonce re-write is the legitimate in-place update (persisting `out_net_msat`
    // after sizing leg OUT, or a resume re-deriving its own session): it is allowed.
    let mut updated = first;
    updated.out_net_msat = Some(13_000);
    journal
        .begin_probe_session(&fed(2), &updated)
        .await
        .expect("same-nonce update is allowed");
    let record = journal
        .probe_record(&fed(2))
        .await
        .expect("read")
        .expect("row");
    assert_eq!(record.in_flight, Some(updated));
}

#[tokio::test]
async fn outcome_write_is_conditional_on_the_matching_in_flight_nonce() {
    let journal = mem_journal();
    let umbrella = IdempotencyKey("probe:ee:nonce-a".to_string());
    let ok = attempt(NOW, true);

    journal
        .begin_probe_session(&fed(2), &session("nonce-a"))
        .await
        .expect("begin session");
    assert!(
        journal
            .record_probe_outcome(
                &fed(2),
                "nonce-a",
                Some(ok.clone()),
                &umbrella,
                probe_kind(Some(Msat(500))),
                Actor::User,
                OperationStatus::Succeeded,
                None,
            )
            .await
            .expect("first outcome writes"),
        "the matching session should be finalized"
    );

    assert!(
        !journal
            .record_probe_outcome(
                &fed(2),
                "nonce-a",
                Some(ok.clone()),
                &umbrella,
                probe_kind(Some(Msat(500))),
                Actor::User,
                OperationStatus::Succeeded,
                None,
            )
            .await
            .expect("duplicate outcome is idempotent"),
        "an already-cleared session must not append duplicate evidence"
    );
    let record = journal
        .probe_record(&fed(2))
        .await
        .expect("read")
        .expect("row");
    assert_eq!(record.attempts, vec![ok.clone()]);
    assert_eq!(record.in_flight, None);

    let newer = session("nonce-b");
    journal
        .begin_probe_session(&fed(2), &newer)
        .await
        .expect("begin newer session");
    assert!(
        !journal
            .record_probe_outcome(
                &fed(2),
                "nonce-a",
                Some(attempt(NOW + 1, false)),
                &umbrella,
                probe_kind(Some(Msat(900))),
                Actor::User,
                OperationStatus::Failed,
                Some("stale replay"),
            )
            .await
            .expect("stale outcome is ignored"),
        "a stale finalizer must not clear a different active session"
    );
    let record = journal
        .probe_record(&fed(2))
        .await
        .expect("read")
        .expect("row");
    assert_eq!(record.attempts, vec![ok]);
    assert_eq!(record.in_flight, Some(newer));
}
