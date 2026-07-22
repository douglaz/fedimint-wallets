#[rustfmt::skip]
mod golden {
use wallet_core::*;

macro_rules! id { ($id:expr) => { FederationId([$id; 32]) }; }
macro_rules! msat { ($amount:expr) => { Msat($amount) }; }
macro_rules! balance {
    ($spendable:expr) => {
        FedBalance { spendable: msat!($spendable), in_flight: msat!(0), claimable: msat!(0), reserved_fee: msat!(0) }
    };
}
macro_rules! fed {
    // eligible_to_fund defaults to true (a healthy, scorer-eligible fed); the 7-arg form sets it.
    ($id:expr, $balance:expr, $probed:expr, $shutdown:expr, $healthy:expr) => { fed!($id, $balance, $probed, 0, $shutdown, $healthy, true) };
    ($id:expr, $balance:expr, $probed:expr, $reputation:expr, $shutdown:expr, $healthy:expr) => { fed!($id, $balance, $probed, $reputation, $shutdown, $healthy, true) };
    ($id:expr, $balance:expr, $probed:expr, $reputation:expr, $shutdown:expr, $healthy:expr, $elig:expr) => { FederationStatus { id: id!($id), balance: balance!($balance), probed_ok: $probed, reputation: $reputation, shutdown_notice: $shutdown, healthy: $healthy, eligible_to_fund: $elig } };
}
// Golden fixture bps for the proportional funding-move fee cap (br-ljj.2): 100 bps (1%), so a
// move's `fee_cap` is `amount / 100` and the source `available` is `budget * 10000/10100`.
const GOLDEN_MOVE_BPS: u16 = 100;
macro_rules! snap {
    ([$($fed:expr),*], $spending:expr, $standby:expr, $cap:expr, $target:expr, $standby_target:expr, $now:expr) => { AllocatorSnapshot { federations: vec![$($fed),*], spending_fed: $spending, standby_fed: $standby, per_fed_cap: msat!($cap), target_spending_balance: msat!($target), standby_target: msat!($standby_target), max_fee: msat!(500), max_fee_bps_of_move: GOLDEN_MOVE_BPS, min_move: Msat(0), reservations: Reservations::default(), now: $now } };
}
macro_rules! decision {
    ($action:expr, $reason:expr, $occurrence:expr, $key:expr) => { vec![AllocatorDecision { action: $action, reason: $reason, occurrence: $occurrence, idempotency_key: $key }] };
}
macro_rules! move_action {
    // A funding move's `fee_cap` is PROPORTIONAL (br-ljj.2): it follows from the amount rather
    // than being the absolute `snapshot.max_fee`, which only `evacuate!` still carries.
    ($from:expr, $to:expr, $amount:expr) => { Action::Move { from: id!($from), to: id!($to), amount: msat!($amount), fee_cap: msat!($amount * GOLDEN_MOVE_BPS as u64 / 10_000) } };
}
macro_rules! evacuate {
    ($from:expr, $to:expr, $amount:expr) => { Action::Evacuate { from: id!($from), to: id!($to), amount: msat!($amount), fee_cap: msat!(500) } };
}
macro_rules! refuse {
    // `diagnostics` is intentionally omitted from the expected value: `RefusalDiagnostics`
    // compares equal always (it is observational metadata), so these goldens assert the
    // DECISION — which fed, which reason — not the incidental figures. The figures are
    // covered by the `refusal_*` content tests below.
    ($fed:expr, $reason:expr) => { Action::RefuseInflow { fed: id!($fed), reason: $reason, diagnostics: RefusalDiagnostics::default() } };
}

fn occ(n: u64) -> Occurrence { Occurrence(n) }

fn hexid(n: u8) -> String { FederationId([n; 32]).to_hex() }

// Idempotency keys are stable per logical intent (no clock) EXCEPT for the stamped
// `Occurrence` (T10): see allocator::idem_*. Build the expected key the same way the
// allocator does instead of hard-coding 64-char hex.
fn move_key(from: u8, to: u8, occurrence: u64) -> IdempotencyKey {
    IdempotencyKey(format!("move:{}:{}:{occurrence}", hexid(from), hexid(to)))
}
fn evac_key(from: u8, to: u8, occurrence: u64) -> IdempotencyKey {
    IdempotencyKey(format!("evac:{}:{}:{occurrence}", hexid(from), hexid(to)))
}
fn reason_tag(reason: ReasonCode) -> &'static str {
    match reason {
        ReasonCode::SpendingBelowTarget => "spending_below_target",
        ReasonCode::StandbyBelowTarget => "standby_below_target",
        ReasonCode::ShutdownNotice => "shutdown_notice",
        ReasonCode::Unhealthy => "unhealthy",
        ReasonCode::OverCap => "over_cap",
        ReasonCode::NotProbed => "not_probed",
        ReasonCode::LowReputation => "low_reputation",
        ReasonCode::UserInitiated => "user_initiated",
        ReasonCode::StandingInstruction => "standing_instruction",
        ReasonCode::ActiveProbe => "active_probe",
    }
}
fn refuse_key(fed: u8, reason: ReasonCode, occurrence: u64) -> IdempotencyKey {
    IdempotencyKey(format!("refuse:{}:{}:{occurrence}", reason_tag(reason), hexid(fed)))
}

#[test]
fn move_tops_up_spending_below_target() {
    let snapshot = snap!([fed!(1, 20_000, true, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 60_000, 0, 1000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(move_action!(2, 1, 40_000), ReasonCode::SpendingBelowTarget, occ(1), move_key(2, 1, 1)));
}

#[test]
fn move_funds_distinct_warm_standby() {
    let snapshot = snap!([fed!(1, 80_000, true, false, true), fed!(2, 5_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 50_000, 20_000, 2000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(move_action!(1, 2, 15_000), ReasonCode::StandbyBelowTarget, occ(1), move_key(1, 2, 1)));
}

#[test]
fn self_fund_standby_is_silent_noop() {
    let snapshot = snap!([fed!(1, 80_000, true, false, true)], Some(id!(1)), Some(id!(1)), 100_000, 50_000, 100_000, 2500);
    assert!(decide(&snapshot, occ(1)).is_empty());
}

#[test]
fn sub_floor_standby_shortfall_is_silent_dust() {
    // Standby 96_000 vs target 100_000: the 4_000 shortfall is below the 5_000 protocol
    // move floor (lnv2's minimum incoming contract) — effectively AT target. The 24h soak
    // logged 91 doomed sub-minimum moves retried every tick before this floor existed.
    // Silent like the self-fund no-op: no move, no refusal.
    let mut snapshot = snap!([fed!(1, 200_000, true, false, true), fed!(2, 96_000, true, false, true)], Some(id!(1)), Some(id!(2)), 500_000, 50_000, 100_000, 2600);
    snapshot.min_move = Msat(5_000);
    assert!(decide(&snapshot, occ(1)).is_empty());
}

#[test]
fn floor_does_not_block_a_real_shortfall() {
    // Same shape with a 6_000 shortfall (>= the floor): the move is emitted exactly as
    // without the floor.
    let mut snapshot = snap!([fed!(1, 200_000, true, false, true), fed!(2, 94_000, true, false, true)], Some(id!(1)), Some(id!(2)), 500_000, 50_000, 100_000, 2700);
    snapshot.min_move = Msat(5_000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(move_action!(1, 2, 6_000), ReasonCode::StandbyBelowTarget, occ(1), move_key(1, 2, 1)));
}

#[test]
fn sub_floor_available_crumbs_skip_the_move_but_keep_the_refusal() {
    // A REAL 50_000 shortfall, but the source's spendable surplus after its own target is only
    // 3_500 (53_500 - 50_000), and the proportional fee reserve leaves 3_466 fundable of that
    // (max_fundable(3_500, 100)) — sub-floor crumbs that could only fail lnv2's minimum at perform
    // time. No move; the shortfall refusal STILL records why the standby stays underfunded
    // (unlike the dust case, this is actionable).
    let mut snapshot = snap!([fed!(1, 53_500, true, false, true), fed!(2, 0, true, false, true)], Some(id!(1)), Some(id!(2)), 500_000, 50_000, 50_000, 2800);
    snapshot.min_move = Msat(5_000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(refuse!(2, ReasonCode::StandbyBelowTarget), ReasonCode::StandbyBelowTarget, occ(1), refuse_key(2, ReasonCode::StandbyBelowTarget, 1)));
}

#[test]
fn evacuate_on_shutdown_notice() {
    // fed 2 is the configured standby and is healthy/probed: `safest_other` picks it as
    // the evacuation destination. `amount` is the evacuating fed's FULL spendable (§4.2
    // refinement): an evacuation reserves no fee_cap of its own — the executor's
    // `size_fresh_evacuation` sizes for affordability (fees included) at perform time.
    let snapshot = snap!([fed!(1, 50_000, true, true, true), fed!(2, 30_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 100_000, 0, 3000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(evacuate!(1, 2, 50_000), ReasonCode::ShutdownNotice, occ(1), evac_key(1, 2, 1)));
}

#[test]
fn evacuate_with_no_eligible_destination_degrades_to_refuse_inflow() {
    // A single-federation snapshot: nowhere eligible to evacuate `to`, so the condition
    // still surfaces (never silently dropped), but only as an advisory RefuseInflow.
    let snapshot = snap!([fed!(1, 50_000, true, true, true)], Some(id!(1)), None, 100_000, 100_000, 0, 3500);
    assert_eq!(decide(&snapshot, occ(1)), decision!(refuse!(1, ReasonCode::ShutdownNotice), ReasonCode::ShutdownNotice, occ(1), refuse_key(1, ReasonCode::ShutdownNotice, 1)));
}

#[test]
fn refuse_over_per_fed_cap() {
    // Spending fed is already at the cap, so it cannot be topped up to target.
    let snapshot = snap!([fed!(1, 50_000, true, false, true), fed!(2, 30_000, true, false, true)], Some(id!(1)), Some(id!(2)), 50_000, 80_000, 0, 4000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(refuse!(1, ReasonCode::OverCap), ReasonCode::OverCap, occ(1), refuse_key(1, ReasonCode::OverCap, 1)));
}

#[test]
fn do_not_fund_unprobed_federation() {
    // High reputation must NOT promote an unprobed fed past the probe gate (ADR-0017).
    let snapshot = snap!([fed!(1, 10_000, false, 100, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 60_000, 0, 5000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(refuse!(1, ReasonCode::NotProbed), ReasonCode::NotProbed, occ(1), refuse_key(1, ReasonCode::NotProbed, 1)));
}

#[test]
fn refuse_already_over_cap_balance() {
    // fed 2 is over the cap from its own balance (not from our funding): flag it (ADR-0018).
    let snapshot = snap!([fed!(1, 40_000, true, false, true), fed!(2, 90_000, true, false, true)], Some(id!(1)), None, 50_000, 40_000, 0, 6000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(refuse!(2, ReasonCode::OverCap), ReasonCode::OverCap, occ(1), refuse_key(2, ReasonCode::OverCap, 1)));
}

#[test]
fn low_reputation_blocks_receive() {
    // Negative reputation demotes below the receive floor: do not fund into it (ADR-0017).
    let snapshot = snap!([fed!(1, 20_000, true, -1, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 60_000, 0, 7000);
    assert_eq!(decide(&snapshot, occ(1)), decision!(refuse!(1, ReasonCode::LowReputation), ReasonCode::LowReputation, occ(1), refuse_key(1, ReasonCode::LowReputation, 1)));
}

// The `refuse!` goldens above assert WHICH refusal (fed + reason), since `RefusalDiagnostics`
// compares equal always. These two assert the recorded FIGURES directly — the diagnostic the
// journal keeps so a refusal is reconstructible after a restart (the motivating case).
fn first_refusal_diagnostics(decisions: &[AllocatorDecision]) -> RefusalDiagnostics {
    decisions
        .iter()
        .find_map(|d| match &d.action {
            Action::RefuseInflow { diagnostics, .. } => Some(*diagnostics),
            _ => None,
        })
        .expect("a RefuseInflow decision")
}

#[test]
fn refusal_records_the_full_shortfall_arithmetic() {
    // fed 1 wants 50_000 to reach target, but cap_room (40_000) and then the source's
    // available surplus (9_901 = max_fundable(10_000, 100), the proportional fee reserve of
    // br-ljj.2) each bind below it, so only 9_901 moves. The recorded figures — including the
    // source fed and its raw spendable — let a reader recover exactly that chain without the
    // live snapshot. `max_fee` is None: funding sizes off `max_fee_bps_of_move`, not the
    // absolute cap, and `available` already carries the bps reserve.
    let snapshot = snap!([fed!(1, 60_000, true, false, true), fed!(2, 10_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 110_000, 0, 9100);
    let diag = first_refusal_diagnostics(&decide(&snapshot, occ(1)));
    assert_eq!(diag.source, Some(id!(2)));
    assert_eq!(diag.want, Some(Msat(50_000)));
    assert_eq!(diag.source_spendable, Some(Msat(10_000)));
    assert_eq!(diag.max_fee, None);
    assert_eq!(diag.available, Some(Msat(9_901)));
    assert_eq!(diag.cap_room, Some(Msat(40_000)));
    assert_eq!(diag.amount, Some(Msat(9_901)));
    assert_eq!(diag.min_move, Some(Msat(0)));
}

#[test]
fn refusal_with_no_usable_source_records_available_none() {
    // The spending fed is below target but there is no standby to fund it. `available`,
    // `source`, and `source_spendable` are all None — NOT Some(0) — the distinction between
    // "no source" and "a source with no surplus". This is the exact ambiguity the motivating
    // unreproducible refusal turned on. `max_fee` is None on every funding refusal (br-ljj.2):
    // funding sizes off `max_fee_bps_of_move`, so the absolute cap is not the binding figure.
    let snapshot = snap!([fed!(1, 20_000, true, false, true)], Some(id!(1)), None, 100_000, 60_000, 0, 9200);
    let diag = first_refusal_diagnostics(&decide(&snapshot, occ(1)));
    assert_eq!(diag.want, Some(Msat(40_000)));
    assert_eq!(diag.source, None);
    assert_eq!(diag.source_spendable, None);
    assert_eq!(diag.available, None);
    assert_eq!(diag.max_fee, None);
    assert_eq!(diag.amount, Some(Msat(0)));
}

#[test]
fn receive_blocked_refusal_records_source_side_figures_only() {
    // fed 1 (spending) is unprobed, so fund_into refuses at the receive-blocker gate BEFORE
    // cap room / the amount are computed: those stay None, but the source-side figures (the
    // standby fed 2, its 80_000 spendable, available 79_208 = max_fundable(80_000, 100)) are known.
    // `max_fee` is None — funding sizes off `max_fee_bps_of_move` (br-ljj.2).
    let snapshot = snap!([fed!(1, 10_000, false, 100, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 60_000, 0, 9300);
    let diag = first_refusal_diagnostics(&decide(&snapshot, occ(1)));
    assert_eq!(diag.want, Some(Msat(50_000)));
    assert_eq!(diag.source, Some(id!(2)));
    assert_eq!(diag.source_spendable, Some(Msat(80_000)));
    assert_eq!(diag.available, Some(Msat(79_208)));
    assert_eq!(diag.max_fee, None);
    assert_eq!(diag.cap_room, None);
    assert_eq!(diag.amount, None);
}

// --- br-ljj.2: proportional funding-move fee cap (core-correctness guards) ---

#[test]
fn absolute_cap_no_longer_saturates_funding() {
    // The saturation bug: under the old absolute reservation, a `max_fee` >= the source surplus
    // zeroed `available` and refused the move. Funding now uses the proportional bps cap, so
    // even a `max_fee` far exceeding the surplus still emits a move sized off the budget.
    let mut snapshot = snap!([fed!(1, 10_000, true, false, true), fed!(2, 100_000, true, false, true)], Some(id!(1)), Some(id!(2)), 10_000_000, 1_000_000, 0, 40_001);
    snapshot.max_fee = Msat(10_000_000); // dwarfs the 100_000 surplus — would fully saturate the old model
    let amount = decide(&snapshot, occ(1))
        .iter()
        .find_map(|d| match &d.action {
            Action::Move {
                from, to, amount, ..
            } if *from == id!(2) && *to == id!(1) => Some(amount.0),
            _ => None,
        })
        .expect("a funding move despite max_fee >> surplus");
    // available = max_fundable(100_000, 100) = 99_010 (exact inverse); cap_room and want larger.
    assert_eq!(amount, 99_010);
}

#[test]
fn funding_move_fee_cap_fits_the_source_budget() {
    // Invariant: amount + fee_cap(amount) <= source budget, so the reserved spend never
    // overdraws the source. Source surplus 500_000, bps 100.
    let snapshot = snap!([fed!(1, 10_000, true, false, true), fed!(2, 500_000, true, false, true)], Some(id!(1)), Some(id!(2)), 10_000_000, 1_000_000, 0, 40_002);
    let (amount, fee_cap) = decide(&snapshot, occ(1))
        .iter()
        .find_map(|d| match &d.action {
            Action::Move {
                from,
                amount,
                fee_cap,
                ..
            } if *from == id!(2) => Some((amount.0, fee_cap.0)),
            _ => None,
        })
        .expect("a funding move");
    assert_eq!(amount, 495_050); // max_fundable(500_000, 100), exact inverse
    assert_eq!(fee_cap, 4_950); // 495_050 * 100/10000
    assert!(
        amount + fee_cap <= 500_000,
        "amount {amount} + fee_cap {fee_cap} overdraws the 500_000 budget"
    );
}

#[test]
fn evacuate_keeps_absolute_cap_and_still_drains_a_small_remnant() {
    // Evacuate MUST keep the ABSOLUTE `max_fee` (br-ljj.2). A tiny dying-fed remnant far below
    // that cap is still evacuated — a proportional cap would compute below any base fee (here
    // 100*100/10000 = 1 msat) and refuse the drain, losing the remnant.
    let mut snapshot = snap!([fed!(1, 100, true, true, true), fed!(2, 30_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 100_000, 0, 40_003);
    snapshot.max_fee = Msat(50_000);
    let (amount, fee_cap) = decide(&snapshot, occ(1))
        .iter()
        .find_map(|d| match &d.action {
            Action::Evacuate {
                from,
                amount,
                fee_cap,
                ..
            } if *from == id!(1) => Some((amount.0, fee_cap.0)),
            _ => None,
        })
        .expect("an evacuation of the small remnant");
    assert_eq!(amount, 100); // the full remnant
    assert_eq!(fee_cap, 50_000); // ABSOLUTE max_fee, NOT proportional (which would be 1 msat)
}

#[test]
fn diagnose_20260721_refusal_is_a_partial_topup_co_emission() {
    // DIAGNOSIS of the 2026-07-21 "unexplained" refusal, reconstructed from the recorded
    // balances (spending 3_936_126, standby 1_987_774, target 50_000_000, max_fee 50_000,
    // production per_fed_cap 1_500_000_000). It was NEVER anomalous: `fund_into` emits a move
    // AND a shortfall refusal in the SAME tick. The standby could fund only 1_937_774 of the
    // spending fed's 46_063_874 shortfall (after the 50_000 fee reserve), so the tick emits
    // BOTH a partial top-up move (1_937_774 — the one that "later succeeded" when it settled)
    // AND a SpendingBelowTarget refusal for the ~44M it could not cover. The earlier
    // investigation looked for a refusal INSTEAD OF a move and couldn't reconcile them; the
    // new figures (amount>0 → a move WAS emitted; want ≫ amount → the source was exhausted)
    // make the co-emission self-evident, which is exactly what the diagnostics exist to show.
    let mut snapshot = snap!([fed!(1, 3_936_126, true, false, true), fed!(2, 1_987_774, true, false, true)], Some(id!(1)), Some(id!(2)), 1_500_000_000, 50_000_000, 0, 20_260_721);
    snapshot.max_fee = Msat(50_000);
    snapshot.min_move = Msat(5_000);
    let decisions = decide(&snapshot, occ(1));

    // A partial top-up move for exactly the standby's fundable surplus (standby − max_fee).
    let move_amount = decisions
        .iter()
        .find_map(|d| match &d.action {
            Action::Move {
                from, to, amount, ..
            } if *from == id!(2) && *to == id!(1) => Some(amount.0),
            _ => None,
        })
        .expect("a partial top-up move 2 -> 1");
    assert_eq!(move_amount, 1_937_774, "1_987_774 standby − 50_000 max_fee");

    // ...co-emitted with a SpendingBelowTarget refusal whose figures show the source was
    // exhausted well short of the shortfall.
    let refusal = decisions
        .iter()
        .find(|d| matches!(d.action, Action::RefuseInflow { .. }))
        .expect("a co-emitted refusal");
    assert_eq!(refusal.reason, ReasonCode::SpendingBelowTarget);
    let diag = first_refusal_diagnostics(&decisions);
    assert_eq!(diag.source, Some(id!(2)));
    assert_eq!(diag.want, Some(Msat(46_063_874)));
    assert_eq!(diag.available, Some(Msat(1_937_774)));
    assert_eq!(diag.source_spendable, Some(Msat(1_987_774)));
    assert_eq!(diag.max_fee, Some(Msat(50_000)));
    assert_eq!(diag.amount, Some(Msat(1_937_774)));
    // amount == available == the move amount: the source, not the cap, was the binding limit.
    assert_eq!(diag.amount, diag.available);
}

#[test]
fn evacuation_drained_source_refusal_records_source_figures_without_max_fee() {
    // fed 1 has a shutdown notice but 0 spendable: `safest_other` guarantees the destination
    // (fed 2) has cap room, so `amount == 0` means the SOURCE is drained. Record the source
    // side; `max_fee` is None because an evacuation does not reserve the fee cap. The
    // figure-blind goldens can't catch a regression here — this asserts the figures directly.
    let snapshot = snap!([fed!(1, 0, true, true, true), fed!(2, 30_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 100_000, 0, 9900);
    let diag = first_refusal_diagnostics(&decide(&snapshot, occ(1)));
    assert_eq!(diag.source, Some(id!(1)));
    assert_eq!(diag.available, Some(Msat(0)));
    assert_eq!(diag.source_spendable, Some(Msat(0)));
    assert_eq!(diag.cap_room, Some(Msat(70_000)));
    assert_eq!(diag.amount, Some(Msat(0)));
    assert_eq!(diag.max_fee, None);
    assert_eq!(diag.want, None);
    assert_eq!(diag.min_move, None);
}

#[test]
fn colliding_over_cap_refusals_keep_the_populated_figures() {
    // target_spending (150_000) exceeds per_fed_cap (100_000): fed 1 is BOTH over cap and
    // below target. The top-level over-cap site emits empty figures and fund_into's over-cap
    // emits full ones under the SAME key; the populated refusal must win the dedup
    // (push_decision replace-if-richer), or the row would say only "fed 1, over cap".
    let snapshot = snap!([fed!(1, 120_000, true, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 150_000, 0, 9400);
    let decisions = decide(&snapshot, occ(1));
    let refusals = decisions
        .iter()
        .filter(|d| matches!(d.action, Action::RefuseInflow { .. }))
        .count();
    assert_eq!(refusals, 1, "the two same-key over-cap refusals dedup to one");
    let diag = first_refusal_diagnostics(&decisions);
    assert!(diag.is_populated(), "the populated over-cap refusal must survive dedup");
    assert_eq!(diag.want, Some(Msat(30_000)));
    assert_eq!(diag.cap_room, Some(Msat(0)));
    assert_eq!(diag.source, Some(id!(2)));
}

#[test]
fn cap_and_liquidity_refusals_do_not_collide() {
    // cap_room=40k, want=50k. The source (fed 2) has 10k spendable, and the TopUp reserves its
    // own PROPORTIONAL fee_cap, so available=9_901 (max_fundable(10_000, 100), §4.2 + br-ljj.2).
    // Both OverCap and SpendingBelowTarget remain true policy signals for the same destination.
    let snapshot = snap!([fed!(1, 60_000, true, false, true), fed!(2, 10_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 110_000, 0, 9000);
    assert_eq!(
        decide(&snapshot, occ(1)),
        vec![
            AllocatorDecision { action: move_action!(2, 1, 9_901), reason: ReasonCode::SpendingBelowTarget, occurrence: occ(1), idempotency_key: move_key(2, 1, 1) },
            AllocatorDecision { action: refuse!(1, ReasonCode::OverCap), reason: ReasonCode::OverCap, occurrence: occ(1), idempotency_key: refuse_key(1, ReasonCode::OverCap, 1) },
            AllocatorDecision { action: refuse!(1, ReasonCode::SpendingBelowTarget), reason: ReasonCode::SpendingBelowTarget, occurrence: occ(1), idempotency_key: refuse_key(1, ReasonCode::SpendingBelowTarget, 1) },
        ]
    );
}

#[test]
fn evacuation_amount_is_clamped_to_destination_cap_room() {
    let snapshot = snap!([fed!(1, 50_000, true, true, true), fed!(2, 95_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 100_000, 0, 9500);
    assert_eq!(decide(&snapshot, occ(1)), decision!(evacuate!(1, 2, 5_000), ReasonCode::ShutdownNotice, occ(1), evac_key(1, 2, 1)));
}

#[test]
fn evacuation_with_zero_spendable_balance_degrades_to_refuse_inflow() {
    // fed 1 has a shutdown notice but nothing to move: an executable Evacuate of 0 is a
    // no-op the executor should never see (mirrors fund_into's amount > 0 guard).
    let snapshot = snap!([fed!(1, 0, true, true, true), fed!(2, 30_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 100_000, 0, 9800);
    assert_eq!(decide(&snapshot, occ(1)), decision!(refuse!(1, ReasonCode::ShutdownNotice), ReasonCode::ShutdownNotice, occ(1), refuse_key(1, ReasonCode::ShutdownNotice, 1)));
}

#[test]
fn occurrence_is_stamped_into_the_idempotency_key() {
    // T10: the SAME logical decision at two different occurrences must yield DIFFERENT
    // keys, so a recurrence after a prior `Done` is not permanently skipped.
    let snapshot = snap!([fed!(1, 20_000, true, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 60_000, 0, 1000);

    let first = decide(&snapshot, occ(1));
    let second = decide(&snapshot, occ(2));

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].action, second[0].action);
    assert_eq!(first[0].reason, second[0].reason);
    assert_ne!(first[0].idempotency_key, second[0].idempotency_key);
    assert_eq!(first[0].idempotency_key, move_key(2, 1, 1));
    assert_eq!(second[0].idempotency_key, move_key(2, 1, 2));
}

// ---- §4.2 per-tick reservation + §4.1 tie-break + §15.3 eligibility goldens ----

#[test]
fn two_evacuations_into_one_destination_share_cap_room() {
    // §4.2: two shutting-down feds evacuate into the same healthy destination (fed 3). The
    // `credited` reservation makes the SECOND evacuation see the first's pending inbound, so
    // the two amounts sum to EXACTLY fed 3's cap room and fill it to the cap — never past.
    let snapshot = snap!([fed!(1, 50_000, true, true, true), fed!(2, 50_000, true, true, true), fed!(3, 40_000, true, false, true)], None, Some(id!(3)), 100_000, 0, 0, 100);
    let decisions = decide(&snapshot, occ(1));
    assert_eq!(
        decisions,
        vec![
            AllocatorDecision { action: evacuate!(1, 3, 50_000), reason: ReasonCode::ShutdownNotice, occurrence: occ(1), idempotency_key: evac_key(1, 3, 1) },
            AllocatorDecision { action: evacuate!(2, 3, 10_000), reason: ReasonCode::ShutdownNotice, occurrence: occ(1), idempotency_key: evac_key(2, 3, 1) },
        ]
    );
    let into_dest: u64 = evac_amounts_into(&decisions, id!(3));
    let cap_room = 100_000 - 40_000;
    assert!(into_dest <= cap_room, "evacuations must fit the destination cap room");
    assert_eq!(40_000 + into_dest, 100_000, "destination is filled to the cap, never over");
}

#[test]
fn evacuation_into_standby_plus_topup_never_exceed_cap() {
    // §4.2: fed 1 evacuates into the standby (fed 2) while fed 2 is ALSO topped up from the
    // spending surplus (fed 3) in the same tick. The `credited` reservation makes the
    // standby-funding move see the evacuation's pending inbound, so their joint credit fills
    // fed 2 to exactly the cap and the residual want is refused as OverCap.
    let snapshot = snap!([fed!(1, 10_000, true, true, true), fed!(2, 70_000, true, false, true), fed!(3, 100_000, true, false, true)], Some(id!(3)), Some(id!(2)), 100_000, 50_000, 100_000, 200);
    let decisions = decide(&snapshot, occ(1));
    assert_eq!(
        decisions,
        vec![
            AllocatorDecision { action: evacuate!(1, 2, 10_000), reason: ReasonCode::ShutdownNotice, occurrence: occ(1), idempotency_key: evac_key(1, 2, 1) },
            AllocatorDecision { action: move_action!(3, 2, 20_000), reason: ReasonCode::StandbyBelowTarget, occurrence: occ(1), idempotency_key: move_key(3, 2, 1) },
            AllocatorDecision { action: refuse!(2, ReasonCode::OverCap), reason: ReasonCode::OverCap, occurrence: occ(1), idempotency_key: refuse_key(2, ReasonCode::OverCap, 1) },
        ]
    );
    let into_standby: u64 = evac_amounts_into(&decisions, id!(2)) + move_amounts_into(&decisions, id!(2));
    assert!(70_000 + into_standby <= 100_000, "evacuation + top-up must not exceed the cap");
    assert_eq!(70_000 + into_standby, 100_000);
}

#[test]
fn evacuations_drain_the_full_spendable_balance() {
    // §4.2 refinement: evacuations reserve NO fee_cap of their own — each drains its full
    // spendable and the executor's `size_fresh_evacuation` owns affordability (fees
    // included) at perform time. The `debited` accounting (amount + fee_cap per emitted
    // move) still conservatively bounds any SUBSEQUENT same-tick move from the same source.
    let snapshot = snap!([fed!(1, 50_000, true, true, true), fed!(2, 30_000, true, true, true), fed!(3, 0, true, false, true)], None, Some(id!(3)), 10_000_000, 0, 0, 300);
    let decisions = decide(&snapshot, occ(1));
    assert_eq!(
        decisions,
        vec![
            AllocatorDecision { action: evacuate!(1, 3, 50_000), reason: ReasonCode::ShutdownNotice, occurrence: occ(1), idempotency_key: evac_key(1, 3, 1) },
            AllocatorDecision { action: evacuate!(2, 3, 30_000), reason: ReasonCode::ShutdownNotice, occurrence: occ(1), idempotency_key: evac_key(2, 3, 1) },
        ]
    );
}

#[test]
fn small_balance_evacuation_is_not_zeroed_by_the_fee_reserve() {
    // Regression (live evacuate gate): a dying fed whose spendable (400) is BELOW the
    // per-move fee cap (500) must still evacuate — the old own-fee reserve zeroed the
    // amount and degraded the evacuation to a refusal, abandoning small balances.
    let snapshot = snap!([fed!(1, 400, true, true, true), fed!(2, 0, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 100_000, 0, 310);
    assert_eq!(decide(&snapshot, occ(1)), decision!(evacuate!(1, 2, 400), ReasonCode::ShutdownNotice, occ(1), evac_key(1, 2, 1)));
}

#[test]
fn tie_break_picks_lower_id_when_pinned_standby_ineligible() {
    // §4.1: the pinned standby (fed 4) is scorer-ineligible, so the fallback runs. Two
    // eligible destinations tie (fed 2 and fed 3, identical but for id); `safest_other` picks
    // the SMALLEST id (fed 2) deterministically — NOT the first in `federations` order (fed 3
    // is listed first), proving the choice is order-independent.
    let snapshot = snap!([fed!(1, 50_000, true, true, true), fed!(4, 0, true, 0, false, true, false), fed!(3, 0, true, false, true), fed!(2, 0, true, false, true)], None, Some(id!(4)), 100_000, 0, 0, 400);
    assert_eq!(decide(&snapshot, occ(1)), decision!(evacuate!(1, 2, 50_000), ReasonCode::ShutdownNotice, occ(1), evac_key(1, 2, 1)));
}

#[test]
fn scorer_rejected_fed_is_never_an_evacuation_destination() {
    // §15.3: fed 2 has a live gateway (probed_ok, healthy) and cap room, but the scorer
    // rejected it (`eligible_to_fund = false`, e.g. a joined 1-of-1). It must NEVER be chosen
    // as an evacuation destination; with no vetted destination the shutdown condition degrades
    // to an advisory RefuseInflow rather than draining fed 1 into a distrusted fed.
    let snapshot = snap!([fed!(1, 50_000, true, true, true), fed!(2, 40_000, true, 0, false, true, false)], None, Some(id!(2)), 100_000, 100_000, 0, 500);
    assert_eq!(decide(&snapshot, occ(1)), decision!(refuse!(1, ReasonCode::ShutdownNotice), ReasonCode::ShutdownNotice, occ(1), refuse_key(1, ReasonCode::ShutdownNotice, 1)));
}

#[test]
fn cross_operation_reservations_only_change_source_availability_and_cap_room() {
    // The outbound reservation must still BIND the move for this golden to prove anything about
    // source availability. Under br-ljj.2's proportional reserve the source keeps far more of its
    // budget than the old flat `− max_fee 500` left it, so the reservation is sized to bind:
    // budget = 900 − 600 = 300, available = max_fundable(300, 100) = 298, below the 400 want.
    // want stays 400 (500 target − 100 spendable), NOT 200 — the 200 speculative inbound on the
    // destination is deliberately not credited toward its target, only against its cap room.
    let mut snapshot = snap!([fed!(1, 100, true, false, true), fed!(2, 900, true, false, true)], Some(id!(1)), Some(id!(2)), 1_000, 500, 0, 600);
    snapshot
        .reservations
        .per_fed_inbound
        .insert(id!(1), msat!(200));
    snapshot
        .reservations
        .per_fed_outbound
        .insert(id!(2), msat!(600));

    let decisions = decide(&snapshot, occ(1));
    assert!(
        decisions.iter().any(|decision| matches!(
            decision.action,
            Action::Move { amount: Msat(298), .. }
        )),
        "the source reservation reduces available funds without treating speculative inbound as target credit"
    );

    snapshot
        .reservations
        .per_fed_inbound
        .insert(id!(1), msat!(900));
    assert!(decide(&snapshot, occ(1))
        .iter()
        .all(|decision| !matches!(decision.action, Action::Move { .. })));
}

#[test]
fn speculative_receive_reservation_does_not_suppress_a_needed_top_up() {
    let mut snapshot = snap!([fed!(1, 100, true, false, true), fed!(2, 2_000, true, false, true)], Some(id!(1)), Some(id!(2)), 1_100, 500, 0, 601);
    snapshot
        .reservations
        .per_fed_inbound
        .insert(id!(1), msat!(500));

    assert!(decide(&snapshot, occ(1)).iter().any(|decision| matches!(
        decision.action,
        Action::Move {
            to,
            amount: Msat(400),
            ..
        } if to == id!(1)
    )));
}

// Sum of `Evacuate` amounts targeting `dest` across the emitted decisions.
fn evac_amounts_into(decisions: &[AllocatorDecision], dest: FederationId) -> u64 {
    decisions.iter().filter_map(|d| match &d.action {
        Action::Evacuate { to, amount, .. } if *to == dest => Some(amount.0),
        _ => None,
    }).sum()
}

// Sum of `Move` amounts targeting `dest` across the emitted decisions.
fn move_amounts_into(decisions: &[AllocatorDecision], dest: FederationId) -> u64 {
    decisions.iter().filter_map(|d| match &d.action {
        Action::Move { to, amount, .. } if *to == dest => Some(amount.0),
        _ => None,
    }).sum()
}

}
