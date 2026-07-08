//! Ledger goldens (spec §7): the FULL `advance` transition matrix plus the
//! `kind_from_action` / `status_from_intent` pure mappings.

use wallet_core::{
    advance, kind_from_action, status_from_intent, Action, Actor, DiscoverySource, FederationId,
    FeeBreakdown, GatewayUrl, IdempotencyKey, IntentStatus, Msat, OperationId, OperationKind,
    OperationRecord, OperationStatus, RawOpUpdate, ReasonCode, SourceStatus, WriteKind,
};
use OperationStatus::{Awaiting, Failed, Started, Succeeded};
use WriteKind::{Authoritative, Repair};

const FED: FederationId = FederationId([1; 32]);
const OTHER: FederationId = FederationId([2; 32]);

/// A `Pay` row at `status`/`repaired`, created and last-touched at t=100. `Pay` is chosen so
/// the enrichment goldens can observe op-id/gateway/amount fills.
fn pay_record(status: OperationStatus, repaired: bool) -> OperationRecord {
    OperationRecord {
        seq: 1,
        correlation_key: IdempotencyKey("pay:0101…:nonce".into()),
        kind: OperationKind::Pay {
            fed: FED,
            invoice_amount: None,
            payment_hash: None,
            op_id: None,
            gateway: None,
        },
        actor: Actor::User,
        reason: ReasonCode::UserInitiated,
        status,
        created_at_ms: 100,
        updated_at_ms: 100,
        fees: FeeBreakdown::default(),
        error: None,
        repaired,
    }
}

/// The common `advance` shape (t=200, no enrichment/error) — the matrix's status-only cells.
fn adv(
    rec: &OperationRecord,
    status: OperationStatus,
    write: WriteKind,
) -> Option<OperationRecord> {
    advance(rec, status, 200, None, None, write)
}

fn op(byte: u8) -> OperationId {
    OperationId([byte; 32])
}

fn gw() -> GatewayUrl {
    GatewayUrl("https://gw.example".into())
}

/// The op-evidence enrichment a `record_update`/backfill carries for a `Pay` row.
fn pay_evidence() -> RawOpUpdate {
    RawOpUpdate {
        op_id: Some(op(0x07)),
        gateway: Some(gw()),
        invoice_amount: Some(Msat(50_000)),
        payment_hash: Some([0xab; 32]),
        fees: Some(FeeBreakdown {
            fee_cap: Some(Msat(1_000)),
            receive_fee: None,
            send_fee_quoted: Some(Msat(42)),
        }),
        fees_definitive: false,
    }
}

#[test]
fn definitive_settlement_fees_replace_a_stale_estimate() {
    // A raw row wrote a pre-call fee ESTIMATE; settlement could not derive the real fee
    // (fees None) but IS definitive — the terminal row must show the fee as unknown, never
    // freeze the estimate as an observed cost. `fee_cap` (the caller's bound) survives.
    let mut rec = pay_record(Started, false);
    rec.fees.fee_cap = Some(Msat(1_000));
    rec.fees.receive_fee = Some(Msat(77)); // stale pre-call estimate
    let upd = RawOpUpdate {
        fees: Some(FeeBreakdown::default()),
        fees_definitive: true,
        ..Default::default()
    };
    let next = advance(&rec, Succeeded, 300, Some(&upd), None, Authoritative).expect("terminal");
    assert_eq!(next.fees.receive_fee, None, "stale estimate cleared");
    assert_eq!(
        next.fees.fee_cap,
        Some(Msat(1_000)),
        "fee_cap merges, never cleared"
    );
}

#[test]
fn non_definitive_fee_updates_never_wipe_a_known_fee() {
    // The pre-§15-fix merge semantics stay for NON-definitive updates: None keeps.
    let mut rec = pay_record(Started, false);
    rec.fees.receive_fee = Some(Msat(77));
    let upd = RawOpUpdate {
        fees: Some(FeeBreakdown::default()),
        fees_definitive: false,
        ..Default::default()
    };
    let next = advance(&rec, Awaiting, 300, Some(&upd), None, Authoritative).expect("advance");
    assert_eq!(
        next.fees.receive_fee,
        Some(Msat(77)),
        "merge keeps the known fee"
    );
}

// --- create ---

#[test]
fn create_shape_is_started_and_unrepaired() {
    // The append-once create: a fresh row starts `Started`, unrepaired, with created ==
    // updated and no error. (The durable builder lives in §9; this documents the shape the
    // transition matrix starts from.)
    let rec = pay_record(Started, false);
    assert_eq!(rec.status, Started);
    assert!(!rec.repaired);
    assert_eq!(rec.created_at_ms, rec.updated_at_ms);
    assert_eq!(rec.error, None);
}

// --- same-status enrichment ---

#[test]
fn same_status_enrichment_bumps_updated_at_and_fills_fields() {
    let rec = pay_record(Started, false);
    let next = advance(
        &rec,
        Started,
        200,
        Some(&pay_evidence()),
        None,
        Authoritative,
    )
    .expect("same-status enrichment is permitted");

    assert_eq!(next.status, Started);
    assert_eq!(next.updated_at_ms, 200, "enrichment bumps updated_at_ms");
    assert_eq!(next.created_at_ms, 100, "created_at_ms is preserved");
    assert!(!next.repaired);
    match next.kind {
        OperationKind::Pay {
            invoice_amount,
            payment_hash,
            op_id,
            gateway,
            ..
        } => {
            assert_eq!(op_id, Some(op(0x07)));
            assert_eq!(gateway, Some(gw()));
            assert_eq!(invoice_amount, Some(Msat(50_000)));
            assert_eq!(payment_hash, Some([0xab; 32]));
        }
        other => panic!("kind changed: {other:?}"),
    }
    assert_eq!(next.fees.send_fee_quoted, Some(Msat(42)));
    assert_eq!(next.fees.fee_cap, Some(Msat(1_000)));
}

#[test]
fn enrichment_never_clobbers_a_known_value_with_none() {
    // A partial update (only a gateway) must not wipe an already-filled op id / fee.
    let mut rec = pay_record(Started, false);
    rec.kind = OperationKind::Pay {
        fed: FED,
        invoice_amount: Some(Msat(50_000)),
        payment_hash: Some([0xab; 32]),
        op_id: Some(op(0x07)),
        gateway: None,
    };
    rec.fees.send_fee_quoted = Some(Msat(42));
    let upd = RawOpUpdate {
        gateway: Some(gw()),
        ..Default::default()
    };
    let next = advance(&rec, Started, 200, Some(&upd), None, Authoritative).expect("enrichment");
    match next.kind {
        OperationKind::Pay { op_id, gateway, .. } => {
            assert_eq!(op_id, Some(op(0x07)), "op id preserved");
            assert_eq!(gateway, Some(gw()), "gateway filled");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(next.fees.send_fee_quoted, Some(Msat(42)), "fee preserved");
}

// --- forward transitions + the authoritative terminal is unrepaired ---

#[test]
fn forward_started_to_awaiting_to_succeeded() {
    let started = pay_record(Started, false);
    let awaiting = adv(&started, Awaiting, Authoritative).expect("Started → Awaiting");
    assert_eq!(awaiting.status, Awaiting);
    assert!(!awaiting.repaired);

    let done = adv(&awaiting, Succeeded, Authoritative).expect("Awaiting → Succeeded");
    assert_eq!(done.status, Succeeded);
    assert!(!done.repaired, "an authoritative terminal is not repaired");
    assert_eq!(done.updated_at_ms, 200);
}

#[test]
fn forward_transition_to_success_clears_a_stale_non_terminal_error() {
    // A non-terminal row that somehow carries an `error` (the additive same-status fill can
    // leave one) must not drag that failure text onto a later `Succeeded` row: a forward
    // status CHANGE redefines the outcome exactly, mirroring the repaired-terminal
    // supersession, so `error: None` clears it (ADR-0014 audit honesty).
    let mut awaiting = pay_record(Awaiting, false);
    awaiting.error = Some("transient send rejection".into());
    let done = advance(&awaiting, Succeeded, 200, None, None, Authoritative)
        .expect("Awaiting → Succeeded is a forward transition");
    assert_eq!(done.status, Succeeded);
    assert!(!done.repaired);
    assert_eq!(
        done.error, None,
        "a forward transition to success clears a stale non-terminal error"
    );
}

#[test]
fn same_status_enrichment_preserves_a_recorded_error() {
    // The additive rule is scoped to SAME-status enrichment: a partial post-call update
    // (error: None) at the same status must not wipe an already-recorded failure — this is
    // the boundary the forward-transition clear above deliberately does NOT cross.
    let mut awaiting = pay_record(Awaiting, false);
    awaiting.error = Some("known failure".into());
    let enriched = advance(
        &awaiting,
        Awaiting,
        200,
        Some(&pay_evidence()),
        None,
        Authoritative,
    )
    .expect("same-status enrichment is permitted");
    assert_eq!(enriched.status, Awaiting);
    assert_eq!(
        enriched.error.as_deref(),
        Some("known failure"),
        "same-status enrichment keeps a known error (additive None never clobbers)"
    );
}

// --- regression ---

#[test]
fn regression_awaiting_to_started_is_a_no_op() {
    let rec = pay_record(Awaiting, false);
    assert!(
        adv(&rec, Started, Authoritative).is_none(),
        "a status regression writes nothing"
    );
}

// --- terminal immutability ---

#[test]
fn authoritative_terminal_rejects_everything_authoritative() {
    for status in [Succeeded, Failed] {
        let rec = pay_record(status, false);
        assert!(
            advance(
                &rec,
                Failed,
                200,
                Some(&pay_evidence()),
                Some("late"),
                Authoritative
            )
            .is_none(),
            "an authoritative terminal is immutable to further authoritative writes"
        );
        assert!(
            adv(&rec, status, Authoritative).is_none(),
            "even a same-status re-write is rejected on a non-repaired terminal"
        );
    }
}

// --- the repaired single-supersession exception ---

#[test]
fn repaired_terminal_is_superseded_exactly_once_by_authoritative() {
    let soft = pay_record(Failed, true);
    let superseded = advance(
        &soft,
        Succeeded,
        200,
        Some(&pay_evidence()),
        None,
        Authoritative,
    )
    .expect("a repaired terminal yields once to an authoritative write");
    assert_eq!(superseded.status, Succeeded);
    assert!(!superseded.repaired, "the supersession clears the flag");
    // The now non-repaired terminal is immutable: a SECOND authoritative write is a no-op.
    assert!(
        advance(&superseded, Failed, 300, None, Some("nope"), Authoritative).is_none(),
        "superseded exactly once — the row is now an ordinary immutable terminal"
    );
}

#[test]
fn repaired_terminal_can_be_superseded_to_non_terminal_by_authoritative() {
    let mut soft = pay_record(Failed, true);
    soft.error = Some("presumed failed: not in registry after 1h".into());
    let awaiting = advance(
        &soft,
        Awaiting,
        200,
        Some(&pay_evidence()),
        None,
        Authoritative,
    )
    .expect("a repaired terminal yields to an authoritative non-terminal status");

    assert_eq!(awaiting.status, Awaiting);
    assert!(!awaiting.repaired);
    assert_eq!(awaiting.error, None);
    match awaiting.kind {
        OperationKind::Pay { op_id, gateway, .. } => {
            assert_eq!(op_id, Some(op(0x07)));
            assert_eq!(gateway, Some(gw()));
        }
        other => panic!("kind changed: {other:?}"),
    }
}

#[test]
fn repaired_soft_succeeded_behaves_like_repaired_soft_failed() {
    // Both soft-Succeeded and soft-Failed are defeasible terminals: one authoritative write
    // supersedes, clearing the flag and adopting the real outcome/error.
    let soft_succeeded = pay_record(Succeeded, true);
    let corrected = advance(
        &soft_succeeded,
        Failed,
        200,
        None,
        Some("real failure"),
        Authoritative,
    )
    .expect("a repaired Succeeded yields to authority just like a repaired Failed");
    assert_eq!(corrected.status, Failed);
    assert!(!corrected.repaired);
    assert_eq!(corrected.error.as_deref(), Some("real failure"));
}

#[test]
fn supersession_to_success_clears_the_stale_repair_error() {
    // False-repair recovery (§10.3): reconcile soft-failed a late join with a "presumed
    // failed" diagnostic; the real call then returns success. The authoritative write
    // REPLACES the repair's guess, so the stale failure text must NOT survive onto the
    // Succeeded audit row (the additive "None never clobbers" fill applies to enrichment,
    // not to a terminal supersession).
    let mut soft = pay_record(Failed, true);
    soft.error = Some("presumed failed: not in registry after 1h".into());
    let recovered = advance(&soft, Succeeded, 200, None, None, Authoritative)
        .expect("a repaired Failed yields once to authority");
    assert_eq!(recovered.status, Succeeded);
    assert!(
        !recovered.repaired,
        "the supersession clears the repaired flag"
    );
    assert_eq!(
        recovered.error, None,
        "the authoritative success clears the stale repair error"
    );
}

#[test]
fn repair_never_supersedes_any_terminal() {
    for repaired in [true, false] {
        for status in [Succeeded, Failed] {
            let rec = pay_record(status, repaired);
            assert!(
                advance(&rec, Failed, 200, None, Some("guess"), Repair).is_none(),
                "a repair write must never supersede a terminal (repaired={repaired}, {status:?})"
            );
        }
    }
}

// --- write-kind governs the repaired flag on a non-terminal → terminal write ---

#[test]
fn repair_soft_fail_of_a_started_row_sets_repaired() {
    let started = pay_record(Started, false);
    let soft = advance(&started, Failed, 200, None, Some("never reached"), Repair)
        .expect("a Started row may be soft-failed by repair");
    assert_eq!(soft.status, Failed);
    assert!(soft.repaired, "a repair-written terminal is defeasible");
    assert_eq!(soft.error.as_deref(), Some("never reached"));
}

#[test]
fn repair_enrichment_adopting_op_id_stays_non_terminal_unrepaired() {
    // A repair that adopts an op id and moves a Started row to Awaiting (the deduped-retry
    // in-flight case, §10.3) is non-terminal, so `repaired` stays false.
    let started = pay_record(Started, false);
    let awaiting = advance(&started, Awaiting, 200, Some(&pay_evidence()), None, Repair)
        .expect("repair adopts the op id and moves to Awaiting");
    assert_eq!(awaiting.status, Awaiting);
    assert!(!awaiting.repaired);
}

// --- kind_from_action ---

#[test]
fn kind_from_action_mapping() {
    let fee_cap = Msat(500);
    assert_eq!(
        kind_from_action(&Action::Move {
            from: FED,
            to: OTHER,
            amount: Msat(40_000),
            fee_cap,
        }),
        OperationKind::Move {
            from: FED,
            to: OTHER,
            amount: Msat(40_000),
            send_op: None,
            recv_op: None,
            gateway: None,
            evacuation: false,
        }
    );
    assert_eq!(
        kind_from_action(&Action::Evacuate {
            from: FED,
            to: OTHER,
            amount: Msat(40_000),
            fee_cap,
        }),
        OperationKind::Move {
            from: FED,
            to: OTHER,
            amount: Msat(40_000),
            send_op: None,
            recv_op: None,
            gateway: None,
            evacuation: true,
        }
    );
    assert_eq!(
        kind_from_action(&Action::DirectInflow {
            to: OTHER,
            amount: Msat(50_000),
            fee_cap,
        }),
        OperationKind::DirectInflow {
            to: OTHER,
            amount: Msat(50_000),
            recv_op: None,
            gateway: None,
        }
    );
    assert_eq!(
        kind_from_action(&Action::RefuseInflow {
            fed: FED,
            reason: ReasonCode::OverCap,
        }),
        OperationKind::Refusal { fed: FED }
    );
}

// --- discovery kinds (§5.1.2/§5.1.4a): advance + enrichment no-op ---

/// A `Discover` row at `status`, created and last-touched at t=100. Count-only kind, so it is
/// created complete (no in-flight op-ids to fill later).
fn discover_record(status: OperationStatus) -> OperationRecord {
    OperationRecord {
        seq: 7,
        correlation_key: IdempotencyKey("discover:manual:nonce".into()),
        kind: OperationKind::Discover {
            source: DiscoverySource::Manual,
            status: SourceStatus::Ok,
            found: 3,
            structurally_passed: 2,
            rejected: 1,
        },
        actor: Actor::User,
        reason: ReasonCode::UserInitiated,
        status,
        created_at_ms: 100,
        updated_at_ms: 100,
        fees: FeeBreakdown::default(),
        error: None,
        repaired: false,
    }
}

#[test]
fn discovery_kinds_advance_forward_and_ignore_op_enrichment() {
    // A `Discover` row advances Started -> Succeeded, preserving identity; a stray op-evidence
    // enrichment is a NO-OP on its count fields (they carry no in-flight op-ids/gateway/amounts).
    let rec = discover_record(Started);
    let next = advance(
        &rec,
        Succeeded,
        200,
        Some(&pay_evidence()),
        None,
        Authoritative,
    )
    .expect("Started -> Succeeded");
    assert_eq!(next.status, Succeeded);
    assert_eq!(
        next.created_at_ms, 100,
        "advance preserves the row's identity"
    );
    assert_eq!(next.updated_at_ms, 200);
    assert_eq!(
        next.kind,
        discover_record(Started).kind,
        "op-evidence enrichment never touches a Discover row's counts"
    );

    // An `AutoJoin` row terminalizes the same way, counts intact.
    let auto = OperationRecord {
        kind: OperationKind::AutoJoin {
            considered: 4,
            joined: 1,
            blocked_concurrent: 1,
            blocked_weekly: 0,
            blocked_lifetime: 2,
        },
        correlation_key: IdempotencyKey("autojoin:nonce".into()),
        ..discover_record(Started)
    };
    let auto_next = advance(
        &auto,
        Succeeded,
        200,
        Some(&pay_evidence()),
        None,
        Authoritative,
    )
    .expect("terminal");
    assert_eq!(
        auto_next.kind, auto.kind,
        "AutoJoin counts survive enrichment"
    );

    // An `Approve` row (a user vouch) is a terminal `Succeeded` audit fact; a terminal row is
    // immutable, so a later advance is a no-op.
    let approve = OperationRecord {
        kind: OperationKind::Approve { fed: FED },
        correlation_key: IdempotencyKey("approve:0101…:nonce".into()),
        status: Succeeded,
        ..discover_record(Succeeded)
    };
    assert_eq!(
        advance(&approve, Failed, 200, None, Some("x"), Authoritative),
        None,
        "a terminal Approve row is immutable"
    );
}

// --- status_from_intent ---

#[test]
fn status_from_intent_total_mapping() {
    assert_eq!(status_from_intent(IntentStatus::Pending), Started);
    assert_eq!(status_from_intent(IntentStatus::Executing), Started);
    assert_eq!(status_from_intent(IntentStatus::Awaiting), Awaiting);
    assert_eq!(status_from_intent(IntentStatus::Done), Succeeded);
    assert_eq!(status_from_intent(IntentStatus::Failed), Failed);
}
