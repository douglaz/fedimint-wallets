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
macro_rules! snap {
    ([$($fed:expr),*], $spending:expr, $standby:expr, $cap:expr, $target:expr, $standby_target:expr, $now:expr) => { AllocatorSnapshot { federations: vec![$($fed),*], spending_fed: $spending, standby_fed: $standby, per_fed_cap: msat!($cap), target_spending_balance: msat!($target), standby_target: msat!($standby_target), max_fee: msat!(500), now: $now } };
}
macro_rules! decision {
    ($action:expr, $reason:expr, $occurrence:expr, $key:expr) => { vec![AllocatorDecision { action: $action, reason: $reason, occurrence: $occurrence, idempotency_key: $key }] };
}
macro_rules! move_action {
    ($from:expr, $to:expr, $amount:expr) => { Action::Move { from: id!($from), to: id!($to), amount: msat!($amount), fee_cap: msat!(500) } };
}
macro_rules! evacuate {
    ($from:expr, $to:expr, $amount:expr) => { Action::Evacuate { from: id!($from), to: id!($to), amount: msat!($amount), fee_cap: msat!(500) } };
}
macro_rules! refuse {
    ($fed:expr, $reason:expr) => { Action::RefuseInflow { fed: id!($fed), reason: $reason } };
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

#[test]
fn cap_and_liquidity_refusals_do_not_collide() {
    // cap_room=40k, want=50k. The source (fed 2) has 10k spendable, and the TopUp reserves
    // its own fee_cap (500), so available=9_500 (§4.2). Both OverCap and SpendingBelowTarget
    // remain true policy signals for the same destination.
    let snapshot = snap!([fed!(1, 60_000, true, false, true), fed!(2, 10_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 110_000, 0, 9000);
    assert_eq!(
        decide(&snapshot, occ(1)),
        vec![
            AllocatorDecision { action: move_action!(2, 1, 9_500), reason: ReasonCode::SpendingBelowTarget, occurrence: occ(1), idempotency_key: move_key(2, 1, 1) },
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
