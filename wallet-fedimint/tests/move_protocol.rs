//! Pure golden tests for the cross-federation move state machine (spec §3.3, §5).
//!
//! Every test is pure and deterministic: no async, no I/O, no fedimint SDK. They pin the
//! RESUME invariants (no double-invoice, no double-pay, `DirectInflow` never pays) and
//! the merge rule (never drop `fee_cap`, never blank an existing leg).

use wallet_core::{FederationId, IdempotencyKey, Msat};
use wallet_fedimint::{
    assemble_move_record, next_step, GatewayUrl, Invoice, Leg, MoveParams, MovePhase, MoveRecord,
    MoveStep, OpArtifact, OperationId, Preimage,
};

const FED_A: FederationId = FederationId([0xAA; 32]);
const FED_B: FederationId = FederationId([0xBB; 32]);
const RECV_OP: OperationId = OperationId([0x01; 32]);
const SEND_OP: OperationId = OperationId([0x02; 32]);
const FOREIGN_OP: OperationId = OperationId([0x03; 32]);
const STALE_RECV_OP: OperationId = OperationId([0x04; 32]);
const STALE_SEND_OP: OperationId = OperationId([0x05; 32]);

fn key(s: &str) -> IdempotencyKey {
    IdempotencyKey(s.to_string())
}

fn gateway() -> GatewayUrl {
    GatewayUrl("https://gw.example".to_string())
}

fn invoice() -> Invoice {
    Invoice("lnbc1pexample".to_string())
}

fn stale_invoice() -> Invoice {
    Invoice("lnbc1pstale".to_string())
}

/// A fresh `Move` (A→B) at the very start: no invoice, no ops.
fn fresh_move(k: &str) -> MoveRecord {
    MoveRecord {
        key: key(k),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
        invoice: None,
        recv_op: None,
        send_op: None,
        phase: MovePhase::Created,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    }
}

/// Test 1: a `Move` driven CreateInvoice→Pay→AwaitSettle→Done yields exactly that
/// `next_step` sequence.
#[test]
fn move_happy_path() {
    let mut rec = fresh_move("move-happy");
    assert_eq!(next_step(&rec), MoveStep::CreateInvoice);

    // CreateInvoice done: invoice + receive op now known.
    rec.invoice = Some(invoice());
    rec.recv_op = Some(RECV_OP);
    rec.phase = MovePhase::Invoiced;
    assert_eq!(next_step(&rec), MoveStep::Pay);

    // Pay done: send op now known.
    rec.send_op = Some(SEND_OP);
    rec.phase = MovePhase::Sending;
    assert_eq!(next_step(&rec), MoveStep::AwaitSettle);

    // Settled.
    rec.phase = MovePhase::Settled;
    assert_eq!(next_step(&rec), MoveStep::Done);
}

/// Test 2: resume with the invoice already created (and a receive op), no send op yet,
/// `send_required` ⇒ `Pay`, NEVER `CreateInvoice` (no double-invoice).
#[test]
fn resume_after_invoice_no_double_invoice() {
    let mut rec = fresh_move("move-resume-invoice");
    rec.invoice = Some(invoice());
    rec.recv_op = Some(RECV_OP);
    rec.phase = MovePhase::Invoiced;

    assert_eq!(next_step(&rec), MoveStep::Pay);
    assert_ne!(next_step(&rec), MoveStep::CreateInvoice);
}

/// Test 3: resume with the send op already recorded ⇒ `AwaitSettle`, NEVER `Pay`
/// (no double-pay).
#[test]
fn resume_after_send_no_double_pay() {
    let mut rec = fresh_move("move-resume-send");
    rec.invoice = Some(invoice());
    rec.recv_op = Some(RECV_OP);
    rec.send_op = Some(SEND_OP);
    rec.phase = MovePhase::Sending;

    assert_eq!(next_step(&rec), MoveStep::AwaitSettle);
    assert_ne!(next_step(&rec), MoveStep::Pay);
}

/// A degraded backfill may recover the send leg before recovering the receive invoice.
/// `send_op` is still a hard resume guard: the move must wait for settlement, not mint
/// a second invoice.
#[test]
fn resume_after_send_only_no_double_invoice_or_pay() {
    let k = key("move-send-only");
    let params = MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
    };
    let artifacts = vec![OpArtifact {
        move_id: k,
        leg: Leg::Send,
        op_id: SEND_OP,
        amount: Msat(100_000),
        invoice: None,
    }];

    let rec = assemble_move_record(params, &artifacts, None);

    assert_eq!(rec.invoice, None);
    assert_eq!(rec.recv_op, None);
    assert_eq!(rec.send_op, Some(SEND_OP));
    assert_eq!(rec.phase, MovePhase::Sending);
    assert_eq!(next_step(&rec), MoveStep::AwaitSettle);
    assert_ne!(next_step(&rec), MoveStep::CreateInvoice);
    assert_ne!(next_step(&rec), MoveStep::Pay);
}

#[test]
fn manual_retry_does_not_recover_the_terminal_attempts_sdk_artifacts() {
    let public_key = key("move-manual-retry");
    let retry_key = key("retry:17:move-manual-retry:1");
    let params = MoveParams {
        key: public_key.clone(),
        operation_key: retry_key,
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
    };
    let preceding_attempt = vec![OpArtifact {
        move_id: public_key.clone(),
        leg: Leg::Receive,
        op_id: RECV_OP,
        amount: Msat(100_000),
        invoice: Some(stale_invoice()),
    }];

    let rec = assemble_move_record(params, &preceding_attempt, None);

    assert_eq!(rec.key, public_key);
    assert_eq!(rec.invoice, None);
    assert_eq!(rec.recv_op, None);
    assert_eq!(rec.phase, MovePhase::Created);
    assert_eq!(next_step(&rec), MoveStep::CreateInvoice);
}

/// Test 4: a `DirectInflow` (`send_required = false`) skips `Pay` entirely — full
/// sequence is `CreateInvoice → AwaitSettle → Done`.
#[test]
fn direct_inflow_skips_pay() {
    let mut rec = MoveRecord {
        key: key("inflow"),
        from: None,
        to: FED_B,
        amount: Msat(50_000),
        fee_cap: Msat(1_000),
        gateway: gateway(),
        send_required: false,
        invoice: None,
        recv_op: None,
        send_op: None,
        phase: MovePhase::Created,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    };
    assert_eq!(next_step(&rec), MoveStep::CreateInvoice);

    // Invoice created; the payer is external, so we wait for settlement — never pay.
    rec.invoice = Some(invoice());
    rec.recv_op = Some(RECV_OP);
    rec.phase = MovePhase::Invoiced;
    assert_eq!(next_step(&rec), MoveStep::AwaitSettle);
    assert_ne!(next_step(&rec), MoveStep::Pay);

    rec.phase = MovePhase::Settled;
    assert_eq!(next_step(&rec), MoveStep::Done);
}

/// Test 5: terminal phases resolve deterministically — `Settled` ⇒ `Done`;
/// `Failed`/`Refunded` ⇒ `Failed` (terminal).
#[test]
fn terminal_phases() {
    // A fully-driven Move record; only the phase changes below.
    let mut rec = fresh_move("move-terminal");
    rec.invoice = Some(invoice());
    rec.recv_op = Some(RECV_OP);
    rec.send_op = Some(SEND_OP);

    rec.phase = MovePhase::Settled;
    assert_eq!(next_step(&rec), MoveStep::Done);

    rec.phase = MovePhase::Failed;
    assert_eq!(next_step(&rec), MoveStep::Failed);

    rec.phase = MovePhase::Refunded;
    assert_eq!(next_step(&rec), MoveStep::Failed);

    // §3: `Stranded` (send settled, receive not credited) is terminal — it routes to the same
    // `Failed` surface (`perform` returns `Permanent(outcome)`), NOT back to a step.
    rec.phase = MovePhase::Stranded;
    assert_eq!(next_step(&rec), MoveStep::Failed);

    // A terminal phase is decided FIRST: even a record missing its invoice does not
    // loop back to CreateInvoice once it has failed.
    let mut orphan = fresh_move("move-terminal-orphan");
    orphan.phase = MovePhase::Failed;
    assert_eq!(next_step(&orphan), MoveStep::Failed);
}

/// Test 6: assemble merges BOTH legs — a `Receive` artifact (from B) and a `Send`
/// artifact (from A) → a record carrying `recv_op` + `invoice` AND `send_op`, with
/// `fee_cap` preserved.
#[test]
fn assemble_merges_both_legs() {
    let k = key("move-merge");
    let params = MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_345),
        gateway: gateway(),
        send_required: true,
    };
    let artifacts = vec![
        OpArtifact {
            move_id: k.clone(),
            leg: Leg::Receive,
            op_id: RECV_OP,
            amount: Msat(100_000),
            invoice: Some(invoice()),
        },
        OpArtifact {
            move_id: k.clone(),
            leg: Leg::Send,
            op_id: SEND_OP,
            amount: Msat(100_000),
            invoice: None,
        },
    ];

    let rec = assemble_move_record(params, &artifacts, None);

    assert_eq!(rec.recv_op, Some(RECV_OP));
    assert_eq!(rec.invoice, Some(invoice()));
    assert_eq!(rec.send_op, Some(SEND_OP));
    assert_eq!(rec.fee_cap, Msat(2_345));
    assert_eq!(rec.from, Some(FED_A));
    assert_eq!(rec.phase, MovePhase::Sending);
}

/// Backfill pages op-log entries newest-first. If multiple artifacts for the same
/// move/leg are present, the newest one must win instead of being overwritten by an
/// older entry later in the slice.
#[test]
fn assemble_preserves_newest_duplicate_leg_artifacts() {
    let k = key("move-duplicate-leg");
    let params = MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(4_321),
        gateway: gateway(),
        send_required: true,
    };
    let artifacts = vec![
        OpArtifact {
            move_id: k.clone(),
            leg: Leg::Receive,
            op_id: RECV_OP,
            amount: Msat(100_000),
            invoice: Some(invoice()),
        },
        OpArtifact {
            move_id: k.clone(),
            leg: Leg::Receive,
            op_id: STALE_RECV_OP,
            amount: Msat(100_000),
            invoice: Some(stale_invoice()),
        },
        OpArtifact {
            move_id: k.clone(),
            leg: Leg::Send,
            op_id: SEND_OP,
            amount: Msat(100_000),
            invoice: None,
        },
        OpArtifact {
            move_id: k,
            leg: Leg::Send,
            op_id: STALE_SEND_OP,
            amount: Msat(100_000),
            invoice: None,
        },
    ];

    let rec = assemble_move_record(params, &artifacts, None);

    assert_eq!(rec.recv_op, Some(RECV_OP));
    assert_eq!(rec.invoice, Some(invoice()));
    assert_eq!(rec.send_op, Some(SEND_OP));
    assert_eq!(rec.fee_cap, Msat(4_321));
    assert_eq!(rec.phase, MovePhase::Sending);
}

/// Test 7: assemble does NOT blank an existing leg — a cached record with `send_op` and
/// EMPTY artifacts still has `send_op` afterward (no blanking) and keeps `fee_cap`.
#[test]
fn assemble_does_not_blank_existing_leg() {
    let k = key("move-noblank");
    let cached = MoveRecord {
        key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(999),
        gateway: gateway(),
        send_required: true,
        invoice: Some(invoice()),
        recv_op: Some(RECV_OP),
        send_op: Some(SEND_OP),
        phase: MovePhase::Sending,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    };
    let params = MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(999),
        gateway: gateway(),
        send_required: true,
    };

    let rec = assemble_move_record(params, &[], Some(cached));

    // No artifact arrived, but the cached legs survive untouched.
    assert_eq!(rec.send_op, Some(SEND_OP));
    assert_eq!(rec.recv_op, Some(RECV_OP));
    assert_eq!(rec.invoice, Some(invoice()));
    assert_eq!(rec.fee_cap, Msat(999));
}

/// Test 8: a `DirectInflow` assemble is receive-only — params with `send_required =
/// false` plus a single `Receive` artifact → `recv_op`/`invoice` set, `send_op` `None`,
/// `from` `None`.
#[test]
fn assemble_directinflow_receive_only() {
    let k = key("inflow-assemble");
    let params = MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: None,
        to: FED_B,
        amount: Msat(20_000),
        fee_cap: Msat(500),
        gateway: gateway(),
        send_required: false,
    };
    let artifacts = vec![OpArtifact {
        move_id: k.clone(),
        leg: Leg::Receive,
        op_id: RECV_OP,
        amount: Msat(20_000),
        invoice: Some(invoice()),
    }];

    let rec = assemble_move_record(params, &artifacts, None);

    assert_eq!(rec.recv_op, Some(RECV_OP));
    assert_eq!(rec.invoice, Some(invoice()));
    assert_eq!(rec.send_op, None);
    assert_eq!(rec.from, None);
    assert!(!rec.send_required);
    assert_eq!(rec.phase, MovePhase::Invoiced);
}

/// Terminal settlement outcomes are not recoverable from op artifacts alone. Once a
/// cached record knows the move is terminal, assemble must not re-derive it back to
/// `Sending`/`Invoiced` and accidentally make it drivable again.
#[test]
fn assemble_preserves_cached_terminal_phase() {
    for (phase, step) in [
        (MovePhase::Settled, MoveStep::Done),
        (MovePhase::Failed, MoveStep::Failed),
        (MovePhase::Refunded, MoveStep::Failed),
        // §3: `Stranded` is a terminal phase, so `derive_phase` (via `assemble_move_record`)
        // must preserve it and `next_step` route it to the terminal `Failed` surface.
        (MovePhase::Stranded, MoveStep::Failed),
    ] {
        let k = key(&format!("move-terminal-{phase:?}"));
        let cached = MoveRecord {
            key: k.clone(),
            from: Some(FED_A),
            to: FED_B,
            amount: Msat(100_000),
            fee_cap: Msat(777),
            gateway: gateway(),
            send_required: true,
            invoice: Some(invoice()),
            recv_op: Some(RECV_OP),
            send_op: Some(SEND_OP),
            phase,
            outcome: Some(format!("{phase:?}")),
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
        };
        let params = MoveParams {
            key: k.clone(),
            operation_key: k,
            from: Some(FED_A),
            to: FED_B,
            amount: Msat(100_000),
            fee_cap: Msat(777),
            gateway: gateway(),
            send_required: true,
        };

        let rec = assemble_move_record(params, &[], Some(cached));

        assert_eq!(rec.phase, phase);
        assert_eq!(next_step(&rec), step);
        assert_eq!(rec.send_op, Some(SEND_OP));
        assert_eq!(rec.fee_cap, Msat(777));
    }
}

/// §2.3/§3: the preimage and the two fee quotes are executor-persisted facts that op
/// artifacts do NOT carry, so — like the terminal `outcome` — they must survive re-assembly
/// via the cache. A `Stranded` record carrying its saved preimage and both quotes, re-assembled
/// with a fresh backfill of its receive+send artifacts, must keep all three (else a resume would
/// lose the recovery proof and the history's fee accounting).
#[test]
fn assemble_keeps_cached_preimage_and_fee_quotes() {
    let k = key("move-preimage-fees");
    let cached = MoveRecord {
        key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
        invoice: Some(invoice()),
        recv_op: Some(RECV_OP),
        send_op: Some(SEND_OP),
        phase: MovePhase::Stranded,
        outcome: Some("send settled but receive was not credited".to_string()),
        preimage: Some(Preimage([0x5c; 32])),
        receive_fee_quoted: Some(Msat(120)),
        send_fee_quoted: Some(Msat(340)),
    };
    let params = MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
    };
    // A fresh backfill re-supplies both op legs but NOT the preimage/fee quotes.
    let artifacts = vec![
        OpArtifact {
            move_id: k.clone(),
            leg: Leg::Receive,
            op_id: RECV_OP,
            amount: Msat(100_000),
            invoice: Some(invoice()),
        },
        OpArtifact {
            move_id: k,
            leg: Leg::Send,
            op_id: SEND_OP,
            amount: Msat(100_000),
            invoice: None,
        },
    ];

    let rec = assemble_move_record(params, &artifacts, Some(cached));

    assert_eq!(rec.preimage, Some(Preimage([0x5c; 32])));
    assert_eq!(rec.receive_fee_quoted, Some(Msat(120)));
    assert_eq!(rec.send_fee_quoted, Some(Msat(340)));
    // The terminal phase is preserved alongside them, so the move stays terminal on resume.
    assert_eq!(rec.phase, MovePhase::Stranded);
    assert_eq!(next_step(&rec), MoveStep::Failed);
}

/// A fresh `Evacuate` may be sized DOWN by the executor (reserving the fees the dying
/// source must pay) and persisted with the pre-receive `put_move`. Re-assembly must keep
/// that persisted amount: params carry the intent's full desired amount, and rebuilding
/// from them would silently revert the sizing — the §7 Pay-step cap re-check derives the
/// receive fee as `invoice_amount − amount`, so a reverted (larger) amount zeroes the
/// receive fee out of the fee-cap guard on every resume.
#[test]
fn assemble_keeps_cached_downsized_amount() {
    let k = key("evac-downsized");
    let params = || MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000), // the intent's full desired amount
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
    };
    // The pre-receive record: amount sized down to 95_000, no legs yet.
    let pre_op = MoveRecord {
        key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(95_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
        invoice: None,
        recv_op: None,
        send_op: None,
        phase: MovePhase::Created,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    };

    // The exact crash window: the receive op committed (backfill recovers it), but the
    // invoiced record was never persisted — only the pre-op cache carries the sizing.
    let artifacts = vec![OpArtifact {
        move_id: k.clone(),
        leg: Leg::Receive,
        op_id: RECV_OP,
        amount: Msat(95_000),
        invoice: Some(invoice()),
    }];
    let rec = assemble_move_record(params(), &artifacts, Some(pre_op.clone()));
    assert_eq!(rec.amount, Msat(95_000));
    assert_eq!(rec.invoice, Some(invoice()));
    assert_eq!(next_step(&rec), MoveStep::Pay);

    // If the whole MoveRecord cache is lost but the receive op survives in the fedimint
    // client DB, the op metadata still recovers the sized amount. This keeps the Pay-step
    // receive-fee calculation (`invoice_amount - amount`) from reverting to the intent's
    // larger desired amount and saturating the receive-side fee to zero.
    let rec = assemble_move_record(params(), &artifacts, None);
    assert_eq!(rec.amount, Msat(95_000));
    assert_eq!(rec.invoice, Some(invoice()));
    assert_eq!(next_step(&rec), MoveStep::Pay);

    // A plain resume of the persisted invoiced record keeps the sizing too.
    let mut invoiced = pre_op;
    invoiced.invoice = Some(invoice());
    invoiced.recv_op = Some(RECV_OP);
    invoiced.phase = MovePhase::Invoiced;
    let rec = assemble_move_record(params(), &[], Some(invoiced));
    assert_eq!(rec.amount, Msat(95_000));

    // With no cache, params seed the amount as before.
    let rec = assemble_move_record(params(), &[], None);
    assert_eq!(rec.amount, Msat(100_000));
}

/// Op artifacts are keyed by `move_id`; artifacts from another move must not attach
/// invoice/op ids to this record.
#[test]
fn assemble_ignores_foreign_move_artifacts() {
    let k = key("move-filter");
    let foreign = key("move-filter-foreign");
    let params = MoveParams {
        key: k.clone(),
        operation_key: k,
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(321),
        gateway: gateway(),
        send_required: true,
    };
    let artifacts = vec![
        OpArtifact {
            move_id: foreign.clone(),
            leg: Leg::Receive,
            op_id: FOREIGN_OP,
            amount: Msat(100_000),
            invoice: Some(invoice()),
        },
        OpArtifact {
            move_id: foreign,
            leg: Leg::Send,
            op_id: SEND_OP,
            amount: Msat(100_000),
            invoice: None,
        },
    ];

    let rec = assemble_move_record(params, &artifacts, None);

    assert_eq!(rec.invoice, None);
    assert_eq!(rec.recv_op, None);
    assert_eq!(rec.send_op, None);
    assert_eq!(rec.phase, MovePhase::Created);
    assert_eq!(rec.fee_cap, Msat(321));
}

/// A two-fed move backfills one leg per client, so a single `reconcile` pass can hand
/// `assemble_move_record` a `Send`-only artifact set while the cache already holds the
/// `Receive` leg (invoice + `recv_op`) from an earlier pass. That non-invoice-bearing
/// artifact must ADD `send_op` without blanking the cached invoice/`recv_op`, and
/// `fee_cap` must survive.
#[test]
fn assemble_partial_backfill_send_leg_does_not_blank_receive() {
    let k = key("move-partial-backfill");
    let cached = MoveRecord {
        key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(1_234),
        gateway: gateway(),
        send_required: true,
        invoice: Some(invoice()),
        recv_op: Some(RECV_OP),
        send_op: None,
        phase: MovePhase::Invoiced,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    };
    let params = MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(1_234),
        gateway: gateway(),
        send_required: true,
    };
    // Only the send leg comes back this pass (client A's op-log); it carries no invoice.
    let artifacts = vec![OpArtifact {
        move_id: k,
        leg: Leg::Send,
        op_id: SEND_OP,
        amount: Msat(100_000),
        invoice: None,
    }];

    let rec = assemble_move_record(params, &artifacts, Some(cached));

    // The cached receive leg is untouched; the send leg is now attached.
    assert_eq!(rec.invoice, Some(invoice()));
    assert_eq!(rec.recv_op, Some(RECV_OP));
    assert_eq!(rec.send_op, Some(SEND_OP));
    assert_eq!(rec.fee_cap, Msat(1_234));
    assert_eq!(rec.phase, MovePhase::Sending);
}

/// Producer contract (spec §4): a `Receive` artifact must carry its invoice. A `recv_op`
/// recorded without one would let `next_step` re-issue `CreateInvoice` and orphan the
/// live receive op, so `assemble_move_record` enforces it with a `debug_assert!`. Gated
/// to debug builds (where `debug_assertions` is on) so a `--release` test run, which
/// elides the assertion, does not expect a panic that never fires.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "must carry its invoice")]
fn assemble_receive_artifact_without_invoice_trips_contract() {
    let k = key("move-bad-receive");
    let params = MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
    };
    let artifacts = vec![OpArtifact {
        move_id: k,
        leg: Leg::Receive,
        op_id: RECV_OP,
        amount: Msat(100_000),
        invoice: None,
    }];

    let _ = assemble_move_record(params, &artifacts, None);
}

/// Release counterpart of the contract test: in `--release` (the `debug_assert!` elided),
/// an invoice-less `Receive` artifact must NOT produce the contradictory `recv_op = Some,
/// invoice = None` half-state that would make `next_step` re-issue `CreateInvoice` over a
/// live receive op. `assemble_move_record` keeps the two atomic, so the bad artifact is
/// dropped: with no cache the record is the honest empty state, and a cached receive leg
/// survives untouched. Gated to release builds, where the assertion never fires.
#[cfg(not(debug_assertions))]
#[test]
fn assemble_invoice_less_receive_artifact_never_half_states() {
    let k = key("move-bad-receive-release");
    let params = || MoveParams {
        key: k.clone(),
        operation_key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
    };
    let bad_receive = || {
        vec![OpArtifact {
            move_id: k.clone(),
            leg: Leg::Receive,
            op_id: RECV_OP,
            amount: Msat(100_000),
            invoice: None,
        }]
    };

    // No cache: the malformed artifact is dropped, leaving no recv_op-without-invoice.
    let rec = assemble_move_record(params(), &bad_receive(), None);
    assert_eq!(rec.recv_op, None);
    assert_eq!(rec.invoice, None);
    // The honest "no recoverable evidence" state, NOT a contradictory half-state.
    assert_eq!(rec.phase, MovePhase::Created);
    assert!(
        !(rec.recv_op.is_some() && rec.invoice.is_none()),
        "must never produce a recv_op-without-invoice half-state"
    );

    // Cached receive leg present: the invoice-less artifact must not blank it.
    let cached = MoveRecord {
        key: k.clone(),
        from: Some(FED_A),
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
        gateway: gateway(),
        send_required: true,
        invoice: Some(invoice()),
        recv_op: Some(RECV_OP),
        send_op: None,
        phase: MovePhase::Invoiced,
        outcome: None,
        preimage: None,
        receive_fee_quoted: None,
        send_fee_quoted: None,
    };
    let rec = assemble_move_record(params(), &bad_receive(), Some(cached));
    assert_eq!(rec.recv_op, Some(RECV_OP));
    assert_eq!(rec.invoice, Some(invoice()));
    assert_eq!(rec.fee_cap, Msat(2_000));
    assert_eq!(next_step(&rec), MoveStep::Pay);
}
