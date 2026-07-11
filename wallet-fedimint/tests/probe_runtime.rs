//! §5.0.8 runtime probe tests over `MemDatabase` (no devimint, no network): preflight
//! refusals land the umbrella row with NO attempt; fault attribution writes a demoting
//! attempt only for candidate-refused legs; resume drives (or truthfully defers) every
//! session state. What genuinely needs a live federation — the actual two-leg round trip
//! — is the deferred devimint smoke's job.

use fedimint_bip39::Mnemonic;
use fedimint_core::db::mem_impl::MemDatabase;
use fedimint_core::db::{Database, IDatabaseTransactionOpsCore, IRawDatabaseExt};
use std::sync::Arc;
use wallet_core::{
    Action, ActiveProbeVerdict, Actor, FederationId, IdempotencyKey, Intent, IntentStatus, Journal,
    Msat, OperationKind, OperationStatus, ProbePolicy, ReasonCode,
};
use wallet_fedimint::{
    FedimintJournal, GatewayUrl, Invoice, MovePhase, MoveRecord, MultiClient, OperationId,
    OperationRef, Preimage, ProbeOutcome, ProbeReport, ProbeSession, Runtime,
};

/// Unwrap a report's recorded attempt (the tests that call this drive a full round trip or a
/// candidate-attributable leg failure — both record an attempt).
fn attempt(report: &ProbeReport) -> &wallet_core::ProbeAttempt {
    match &report.outcome {
        ProbeOutcome::Attempt(a) => a,
        ProbeOutcome::NoAttempt(d) => panic!("expected a recorded attempt, got NoAttempt: {d}"),
    }
}

const CANDIDATE: FederationId = FederationId([0xCC; 32]);
const SOURCE: FederationId = FederationId([0x55; 32]);
/// First 16 hex chars decode to occurrence 42; the probe legs' keys embed it.
const NONCE: &str = "000000000000002a0000000000000000";
const AMOUNT: u64 = 20_000;
const LEG_FEE_CAP: u64 = 10_000;
const DELIVERED_IN: u64 = 19_500;
const OUT_NET: u64 = 15_000;

async fn fixture_with_db() -> (Runtime, Arc<FedimintJournal>, Database) {
    let db = MemDatabase::new().into_database();
    let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
    let mc = Arc::new(MultiClient::new(db.clone(), mnemonic).await);
    let journal = Arc::new(FedimintJournal::new(db.clone()));
    (
        Runtime::new(mc, journal.clone(), None, None, None),
        journal,
        db,
    )
}

async fn fixture() -> (Runtime, Arc<FedimintJournal>) {
    let (runtime, journal, _) = fixture_with_db().await;
    (runtime, journal)
}

fn in_key() -> IdempotencyKey {
    IdempotencyKey(format!(
        "move:{}:{}:{AMOUNT}:{LEG_FEE_CAP}:42",
        SOURCE.to_hex(),
        CANDIDATE.to_hex()
    ))
}

fn out_key(out_net: u64) -> IdempotencyKey {
    let fee_cap = out_fee_cap(DELIVERED_IN, out_net);
    IdempotencyKey(format!(
        "move:{}:{}:{out_net}:{fee_cap}:42",
        CANDIDATE.to_hex(),
        SOURCE.to_hex()
    ))
}

fn out_fee_cap(delivered_in: u64, out_net: u64) -> u64 {
    LEG_FEE_CAP.min(delivered_in.saturating_sub(out_net))
}

fn umbrella_key() -> IdempotencyKey {
    IdempotencyKey(format!("probe:{}:{NONCE}", CANDIDATE.to_hex()))
}

fn raw_ledger_key_index(key: &IdempotencyKey) -> Vec<u8> {
    let mut raw = vec![0x06];
    raw.extend_from_slice(key.0.as_bytes());
    raw
}

/// The runtime's `now_ms()` is the REAL wall clock (not injected), and a recorded probe
/// attempt now takes its `at_ms` from the session's `started_at_ms` (§P2-1: resume-independent
/// evidence). So a seeded session must stamp `started_at_ms` at ~now, or the recorded attempt
/// would be ancient relative to real-now and fall outside the verdict's ttl window.
fn real_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as u64
}

fn session(out_net: Option<u64>) -> ProbeSession {
    ProbeSession {
        nonce: NONCE.to_string(),
        from: SOURCE,
        amount_msat: AMOUNT,
        leg_fee_cap_msat: LEG_FEE_CAP,
        c_spendable_before_in_msat: 0,
        out_net_msat: out_net,
        started_at_ms: real_now_ms(),
    }
}

fn leg_intent(key: IdempotencyKey, from: FederationId, to: FederationId, amount: u64) -> Intent {
    leg_intent_with_fee_cap(key, from, to, amount, LEG_FEE_CAP)
}

fn leg_intent_with_fee_cap(
    key: IdempotencyKey,
    from: FederationId,
    to: FederationId,
    amount: u64,
    fee_cap: u64,
) -> Intent {
    Intent {
        idempotency_key: key,
        action: Action::Move {
            from,
            to,
            amount: Msat(amount),
            fee_cap: Msat(fee_cap),
        },
        max_fee: Some(Msat(fee_cap)),
        status: IntentStatus::Pending,
        reason: ReasonCode::ActiveProbe,
        actor: Actor::User,
        created_at_ms: 0,
        operation_id: None,
        invoice: None,
    }
}

fn out_leg_intent(out_net: u64) -> Intent {
    leg_intent_with_fee_cap(
        out_key(out_net),
        CANDIDATE,
        SOURCE,
        out_net,
        out_fee_cap(DELIVERED_IN, out_net),
    )
}

fn leg_record(
    key: IdempotencyKey,
    from: FederationId,
    to: FederationId,
    amount: u64,
    phase: MovePhase,
    outcome: Option<&str>,
) -> MoveRecord {
    leg_record_with_fee_cap(key, from, to, amount, LEG_FEE_CAP, phase, outcome)
}

fn leg_record_with_fee_cap(
    key: IdempotencyKey,
    from: FederationId,
    to: FederationId,
    amount: u64,
    fee_cap: u64,
    phase: MovePhase,
    outcome: Option<&str>,
) -> MoveRecord {
    MoveRecord {
        key,
        from: Some(from),
        to,
        amount: Msat(amount),
        fee_cap: Msat(fee_cap),
        gateway: GatewayUrl("https://gw.example".into()),
        send_required: true,
        invoice: Some(Invoice("lnbc1pexample".into())),
        recv_op: Some(OperationId([0x07; 32])),
        send_op: Some(OperationId([0x09; 32])),
        phase,
        outcome: outcome.map(str::to_owned),
        preimage: matches!(phase, MovePhase::Settled | MovePhase::Stranded)
            .then_some(Preimage([0x11; 32])),
        receive_fee_quoted: Some(Msat(300)),
        send_fee_quoted: Some(Msat(200)),
    }
}

fn out_leg_record(out_net: u64, phase: MovePhase, outcome: Option<&str>) -> MoveRecord {
    leg_record_with_fee_cap(
        out_key(out_net),
        CANDIDATE,
        SOURCE,
        out_net,
        out_fee_cap(DELIVERED_IN, out_net),
        phase,
        outcome,
    )
}

async fn seed_intent(journal: &FedimintJournal, intent: &Intent) {
    journal.upsert(intent).await.expect("seed intent");
}

async fn probe_state(journal: &FedimintJournal) -> wallet_fedimint::ProbeRecord {
    journal
        .probe_record(&CANDIDATE)
        .await
        .expect("read probe record")
        .expect("probe row exists")
}

async fn corrupt_ledger_key_index(db: &Database, key: &IdempotencyKey) {
    let app_db = db.with_prefix(vec![0x00]);
    let mut dbtx = app_db.begin_transaction().await;
    dbtx.raw_insert_bytes(&raw_ledger_key_index(key), b"short")
        .await
        .expect("insert corrupt ledger index");
    dbtx.commit_tx_result().await.expect("commit corruption");
}

// ---- preflight refusals -------------------------------------------------------------

#[tokio::test]
async fn not_joined_candidate_lands_a_failed_umbrella_row_with_no_attempt() {
    let (runtime, journal) = fixture().await;
    let report = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect("a not-joined candidate is a terminal NoAttempt outcome, not an Err");
    match &report.outcome {
        ProbeOutcome::NoAttempt(d) => assert!(d.contains("not joined"), "{d}"),
        other => panic!("expected NoAttempt, got {other:?}"),
    }
    assert_eq!(
        report.verdict_after, report.verdict_before,
        "a no-attempt refusal never changes the verdict"
    );

    // The refusal is in history (kind Probe, Failed, the diagnostic verbatim), the
    // session is cleared, and NO attempt was written (no demotion).
    let state = probe_state(&journal).await;
    assert_eq!(state.in_flight, None);
    assert!(state.attempts.is_empty());
    let rows = journal.history(10, None).await.expect("history");
    let row = rows
        .iter()
        .find(|r| matches!(r.kind, OperationKind::Probe { .. }))
        .expect("the probe umbrella row exists");
    assert_eq!(row.status, OperationStatus::Failed);
    assert_eq!(row.reason, ReasonCode::ActiveProbe);
    assert!(
        row.error.as_deref().unwrap_or("").contains("not joined"),
        "{row:?}"
    );
    assert!(row.correlation_key.0.starts_with("probe:"), "{row:?}");
}

#[tokio::test]
async fn session_only_pre_umbrella_resume_recreates_the_row_and_repreflights() {
    // The crash window between `begin_probe_session` and `record_started`: the next
    // invocation resumes the session (never mints a new one), recreates the Started row
    // idempotently, re-runs the preflight, and terminalizes the ORIGINAL umbrella key.
    let (runtime, journal) = fixture().await;
    journal
        .begin_probe_session(&CANDIDATE, &session(None))
        .await
        .expect("seed session");

    let report = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect("preflight refuses the not-joined candidate as a terminal NoAttempt");
    match &report.outcome {
        ProbeOutcome::NoAttempt(d) => assert!(d.contains("not joined"), "{d}"),
        other => panic!("expected NoAttempt, got {other:?}"),
    }

    let row = journal
        .operation(&OperationRef::Key(umbrella_key()))
        .await
        .expect("read umbrella")
        .expect("the resumed outcome landed on the session's own umbrella key");
    assert_eq!(row.status, OperationStatus::Failed);
    let state = probe_state(&journal).await;
    assert_eq!(
        state.in_flight, None,
        "the terminal exit cleared the session"
    );
    assert!(
        state.attempts.is_empty(),
        "a preflight refusal never demotes"
    );
}

// ---- resume across session states ---------------------------------------------------

#[tokio::test]
async fn mid_in_resume_drives_the_pending_leg_and_defers_on_transient_failure() {
    // Session + a journaled-but-Pending leg IN: resume skips the preflight (money may
    // already have moved), re-drives the leg, and — the drive failing transiently here
    // (no live federation) — defers WITHOUT clearing the session or writing an attempt.
    let (runtime, journal) = fixture().await;
    journal
        .begin_probe_session(&CANDIDATE, &session(None))
        .await
        .expect("seed session");
    seed_intent(&journal, &leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT)).await;

    let err = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect_err("a transiently-failing leg IN must defer, not terminalize");
    assert!(
        err.to_string().contains("re-run `probe` to resume"),
        "{err}"
    );
    let state = probe_state(&journal).await;
    assert!(
        state.in_flight.is_some(),
        "the session survives a transient fault"
    );
    assert!(state.attempts.is_empty());
}

#[tokio::test]
async fn sized_but_unjournaled_out_resume_runs_the_no_sweep_guard_first() {
    // `out_net` persisted but leg OUT never journaled: the resume must re-check the
    // no-sweep precondition BEFORE journaling leg OUT. Reading the candidate's balance
    // fails here (not joined) — a transient defer, session retained, nothing journaled.
    let (runtime, journal) = fixture().await;
    journal
        .begin_probe_session(&CANDIDATE, &session(Some(OUT_NET)))
        .await
        .expect("seed session");
    seed_intent(&journal, &{
        let mut done = leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT);
        done.status = IntentStatus::Done;
        done
    })
    .await;
    journal
        .put_move(&leg_record(
            in_key(),
            SOURCE,
            CANDIDATE,
            DELIVERED_IN,
            MovePhase::Settled,
            None,
        ))
        .await
        .expect("seed settled in record");

    let err = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect_err("an unreadable no-sweep balance must defer");
    assert!(err.to_string().contains("no-sweep"), "{err}");
    let state = probe_state(&journal).await;
    assert!(
        state.in_flight.is_some(),
        "the session survives the deferral"
    );
    assert!(
        journal.get(&out_key(OUT_NET)).await.expect("get").is_none(),
        "leg OUT must not be journaled before the guard passes"
    );
}

#[tokio::test]
async fn mid_out_resume_drives_the_journaled_leg_without_any_guard() {
    // Once the out intent exists the money path owns it like any other move: no guard,
    // straight to the drive (which defers transiently here — no live federation).
    let (runtime, journal) = fixture().await;
    journal
        .begin_probe_session(&CANDIDATE, &session(Some(OUT_NET)))
        .await
        .expect("seed session");
    seed_intent(&journal, &{
        let mut done = leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT);
        done.status = IntentStatus::Done;
        done
    })
    .await;
    journal
        .put_move(&leg_record(
            in_key(),
            SOURCE,
            CANDIDATE,
            DELIVERED_IN,
            MovePhase::Settled,
            None,
        ))
        .await
        .expect("seed settled in record");
    seed_intent(&journal, &out_leg_intent(OUT_NET)).await;

    let err = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect_err("a transiently-failing leg OUT must defer");
    assert!(
        err.to_string().contains("re-run `probe` to resume"),
        "{err}"
    );
    assert!(probe_state(&journal).await.in_flight.is_some());
}

#[tokio::test]
async fn recovered_attempt_is_stamped_at_probe_start_not_recovery_time() {
    // §P2-1: a crash-then-delayed-resume must stamp the durable attempt at when the probe
    // HAPPENED (the session's started_at_ms), not at recovery time — the verdict is driven
    // entirely by at_ms, so a recovery-time stamp could keep a stale probe inside ttl.
    let (runtime, journal) = fixture().await;
    let started = real_now_ms() - 3_600_000; // 1h ago: distinct from now, well within ttl
    journal
        .begin_probe_session(
            &CANDIDATE,
            &ProbeSession {
                started_at_ms: started,
                ..session(Some(OUT_NET))
            },
        )
        .await
        .expect("seed a session that started an hour ago");
    let mut done_in = leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT);
    done_in.status = IntentStatus::Done;
    seed_intent(&journal, &done_in).await;
    let mut done_out = out_leg_intent(OUT_NET);
    done_out.status = IntentStatus::Done;
    seed_intent(&journal, &done_out).await;
    journal
        .put_move(&leg_record(
            in_key(),
            SOURCE,
            CANDIDATE,
            DELIVERED_IN,
            MovePhase::Settled,
            None,
        ))
        .await
        .expect("seed settled in record");
    journal
        .put_move(&out_leg_record(OUT_NET, MovePhase::Settled, None))
        .await
        .expect("seed settled out record");

    let report = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect("the resume records the attempt");
    assert_eq!(
        attempt(&report).at_ms,
        started,
        "the recovered attempt must be stamped at the probe's start, not recovery time"
    );
    // Durable too: the persisted attempt carries the same start time.
    let state = probe_state(&journal).await;
    assert_eq!(state.attempts.first().map(|a| a.at_ms), Some(started));
}

#[tokio::test]
async fn crash_window_repair_records_the_attempt_and_clears_the_session() {
    // Both legs already terminal (the §5.0.4 crash window between the legs settling and
    // the outcome write): the next `probe` reconstructs the keys from the session,
    // reattaches idempotently, records the attempt, terminalizes the umbrella row with
    // the S-net-outflow cost, and clears the session — all without a live federation.
    let (runtime, journal) = fixture().await;
    journal
        .begin_probe_session(&CANDIDATE, &session(Some(OUT_NET)))
        .await
        .expect("seed session");
    let mut done_in = leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT);
    done_in.status = IntentStatus::Done;
    seed_intent(&journal, &done_in).await;
    let mut done_out = out_leg_intent(OUT_NET);
    done_out.status = IntentStatus::Done;
    seed_intent(&journal, &done_out).await;
    journal
        .put_move(&leg_record(
            in_key(),
            SOURCE,
            CANDIDATE,
            DELIVERED_IN,
            MovePhase::Settled,
            None,
        ))
        .await
        .expect("seed settled in record");
    journal
        .put_move(&out_leg_record(OUT_NET, MovePhase::Settled, None))
        .await
        .expect("seed settled out record");

    let report = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect("both legs terminal: the resume records the attempt");
    assert!(attempt(&report).ok);
    assert_eq!(report.verdict_before, ActiveProbeVerdict::NeverProbed);
    assert_eq!(report.verdict_after, ActiveProbeVerdict::Insufficient);
    assert_eq!(report.in_key, in_key());
    assert_eq!(report.out_key, Some(out_key(OUT_NET)));

    let state = probe_state(&journal).await;
    assert_eq!(state.in_flight, None);
    assert_eq!(state.attempts.len(), 1);
    assert!(state.attempts[0].ok);
    assert_eq!(state.attempts[0].amount_msat, AMOUNT);

    let row = journal
        .operation(&OperationRef::Key(umbrella_key()))
        .await
        .expect("read umbrella")
        .expect("umbrella row exists");
    assert_eq!(row.status, OperationStatus::Succeeded);
    // Cost = S net outflow: leg IN debit (19_500 delivered + 300 receive + 200 send
    // fees) minus leg OUT credit (15_000) = 5_000 msat.
    assert_eq!(
        row.kind,
        OperationKind::Probe {
            fed: CANDIDATE,
            from: SOURCE,
            amount_msat: Msat(AMOUNT),
            cost_msat: Some(Msat(5_000)),
        }
    );
}

// ---- fault attribution (asserted against the verdict) --------------------------------

#[tokio::test]
async fn candidate_refused_pay_on_leg_out_writes_a_demoting_attempt() {
    let (runtime, journal) = fixture().await;
    journal
        .begin_probe_session(&CANDIDATE, &session(Some(OUT_NET)))
        .await
        .expect("seed session");
    let mut done = leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT);
    done.status = IntentStatus::Done;
    seed_intent(&journal, &done).await;
    journal
        .put_move(&leg_record(
            in_key(),
            SOURCE,
            CANDIDATE,
            DELIVERED_IN,
            MovePhase::Settled,
            None,
        ))
        .await
        .expect("seed settled in record");
    // Leg OUT terminally failed at settlement: C's send reached a terminal Failed state
    // — the classified candidate-refused pay, the redeemability core.
    let mut failed = out_leg_intent(OUT_NET);
    failed.status = IntentStatus::Failed;
    seed_intent(&journal, &failed).await;
    journal
        .put_move(&out_leg_record(
            OUT_NET,
            MovePhase::Failed,
            Some("lnv2 send deterministically rejected the invoice: FederationNotSupported"),
        ))
        .await
        .expect("seed failed out record");

    let report = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect("a demoting failure still records an attempt (Ok report)");
    assert!(!attempt(&report).ok);
    assert!(
        attempt(&report)
            .error
            .as_deref()
            .unwrap_or("")
            .contains("leg OUT"),
        "{report:?}"
    );
    assert_eq!(
        report.verdict_after,
        ActiveProbeVerdict::Failed,
        "the candidate-refused pay demotes"
    );
    let state = probe_state(&journal).await;
    assert_eq!(state.in_flight, None);
    assert_eq!(state.attempts.len(), 1);
    assert!(!state.attempts[0].ok);
    // The whole delivered amount never came back: cost = 19_500 + 300 + 200 = 20_000.
    let row = journal
        .operation(&OperationRef::Key(umbrella_key()))
        .await
        .expect("read umbrella")
        .expect("umbrella row exists");
    assert_eq!(row.status, OperationStatus::Failed);
    assert!(matches!(
        row.kind,
        OperationKind::Probe {
            cost_msat: Some(Msat(20_000)),
            ..
        }
    ));
}

#[tokio::test]
async fn candidate_refused_mint_on_leg_in_writes_a_demoting_attempt() {
    let (runtime, journal) = fixture().await;
    journal
        .begin_probe_session(&CANDIDATE, &session(None))
        .await
        .expect("seed session");
    // A permanent CreateInvoice-on-C failure: intent Failed with the executor diagnostic
    // threaded to the ledger (§8.3), no move record (the mint never committed).
    seed_intent(&journal, &leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT)).await;
    journal
        .set_status(
            &in_key(),
            IntentStatus::Failed,
            Some("the candidate federation refused to mint the incoming contract"),
        )
        .await
        .expect("terminalize leg IN");

    let report = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect("a demoting failure still records an attempt");
    assert!(!attempt(&report).ok);
    assert_eq!(report.verdict_after, ActiveProbeVerdict::Failed);
    assert_eq!(report.out_key, None, "leg OUT never existed");
    let state = probe_state(&journal).await;
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.in_flight, None);
}

#[tokio::test]
async fn failed_leg_diagnostic_read_error_defers_without_demoting() {
    let (runtime, journal, db) = fixture_with_db().await;
    journal
        .begin_probe_session(&CANDIDATE, &session(None))
        .await
        .expect("seed session");
    seed_intent(&journal, &leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT)).await;
    journal
        .set_status(
            &in_key(),
            IntentStatus::Failed,
            Some("the candidate federation refused to mint the incoming contract"),
        )
        .await
        .expect("terminalize leg IN");
    corrupt_ledger_key_index(&db, &in_key()).await;

    let err = runtime
        .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
        .await
        .expect_err("a corrupt diagnostic source must fail closed");
    assert!(
        err.to_string().contains("reading its diagnostic failed"),
        "{err}"
    );
    assert!(
        err.to_string().contains("session retained"),
        "the operator must know the probe will resume, not demote: {err}"
    );
    let state = probe_state(&journal).await;
    assert!(
        state.in_flight.is_some(),
        "the session survives because attribution could not be read safely"
    );
    assert!(
        state.attempts.is_empty(),
        "a diagnostic read error must never fabricate a demoting attempt"
    );
}

#[tokio::test]
async fn stranded_leg_and_source_and_local_faults_write_umbrella_only_outcomes() {
    // Three umbrella-only shapes, each asserted against the VERDICT (history untouched):
    // (a) a Stranded leg IN (send settled, receive never credited — ambiguous);
    // (b) a local parametric leg IN refusal (receive fee over cap);
    // (c) a source-side leg OUT mint refusal (no artifacts — CreateInvoice on S).

    // (a) Stranded leg IN.
    {
        let (runtime, journal) = fixture().await;
        journal
            .begin_probe_session(&CANDIDATE, &session(None))
            .await
            .expect("seed session");
        let mut failed = leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT);
        failed.status = IntentStatus::Failed;
        seed_intent(&journal, &failed).await;
        journal
            .put_move(&leg_record(
                in_key(),
                SOURCE,
                CANDIDATE,
                DELIVERED_IN,
                MovePhase::Stranded,
                Some(
                    "send settled but receive was not credited: receive invoice expired; \
                      payment preimage saved on the move record",
                ),
            ))
            .await
            .expect("seed stranded record");
        let report = runtime
            .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
            .await
            .expect("a stranded leg is an umbrella-only NoAttempt, not an Err");
        assert!(
            matches!(report.outcome, ProbeOutcome::NoAttempt(_)),
            "{report:?}"
        );
        let state = probe_state(&journal).await;
        assert!(state.attempts.is_empty(), "no attempt: verdict untouched");
        assert_eq!(state.in_flight, None);
        // Stranded still debited the source: cost = full exposure (20_000 msat).
        let row = journal
            .operation(&OperationRef::Key(umbrella_key()))
            .await
            .expect("read umbrella")
            .expect("umbrella row exists");
        assert!(matches!(
            row.kind,
            OperationKind::Probe {
                cost_msat: Some(Msat(20_000)),
                ..
            }
        ));
    }

    // (b) Local parametric leg IN refusal (§5.0.2: must not demote).
    {
        let (runtime, journal) = fixture().await;
        journal
            .begin_probe_session(&CANDIDATE, &session(None))
            .await
            .expect("seed session");
        seed_intent(&journal, &leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT)).await;
        journal
            .set_status(
                &in_key(),
                IntentStatus::Failed,
                Some("fee over cap (receive side exceeds fee_cap)"),
            )
            .await
            .expect("terminalize leg IN");
        let report = runtime
            .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
            .await
            .expect("a parametric refusal is an umbrella-only NoAttempt");
        match &report.outcome {
            ProbeOutcome::NoAttempt(d) => assert!(d.contains("fee over cap"), "{d}"),
            other => panic!("expected NoAttempt, got {other:?}"),
        }
        let state = probe_state(&journal).await;
        assert!(state.attempts.is_empty());
        assert_eq!(state.in_flight, None);
    }

    // (c) Source-side leg OUT mint refusal (S refused; candidate honesty not in question).
    {
        let (runtime, journal) = fixture().await;
        journal
            .begin_probe_session(&CANDIDATE, &session(Some(OUT_NET)))
            .await
            .expect("seed session");
        let mut done = leg_intent(in_key(), SOURCE, CANDIDATE, AMOUNT);
        done.status = IntentStatus::Done;
        seed_intent(&journal, &done).await;
        journal
            .put_move(&leg_record(
                in_key(),
                SOURCE,
                CANDIDATE,
                DELIVERED_IN,
                MovePhase::Settled,
                None,
            ))
            .await
            .expect("seed settled in record");
        seed_intent(&journal, &out_leg_intent(OUT_NET)).await;
        journal
            .set_status(
                &out_key(OUT_NET),
                IntentStatus::Failed,
                Some("source federation refused to mint the return invoice"),
            )
            .await
            .expect("terminalize leg OUT");
        let report = runtime
            .active_probe(CANDIDATE, SOURCE, &ProbePolicy::default(), Actor::User)
            .await
            .expect("a source-side fault is an umbrella-only NoAttempt");
        match &report.outcome {
            ProbeOutcome::NoAttempt(d) => {
                assert!(d.contains("source federation refused to mint"), "{d}")
            }
            other => panic!("expected NoAttempt, got {other:?}"),
        }
        // §P3: leg OUT failed, so its move exists — the report keeps the out_key handle.
        assert_eq!(
            report.out_key,
            Some(out_key(OUT_NET)),
            "out_key preserved on leg-OUT failure"
        );
        let state = probe_state(&journal).await;
        assert!(state.attempts.is_empty());
        assert_eq!(state.in_flight, None);
    }
}
