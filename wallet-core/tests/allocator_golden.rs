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
    ($id:expr, $balance:expr, $probed:expr, $shutdown:expr, $healthy:expr) => { fed!($id, $balance, $probed, 0, $shutdown, $healthy) };
    ($id:expr, $balance:expr, $probed:expr, $reputation:expr, $shutdown:expr, $healthy:expr) => { FederationStatus { id: id!($id), balance: balance!($balance), probed_ok: $probed, reputation: $reputation, shutdown_notice: $shutdown, healthy: $healthy } };
}
macro_rules! snap {
    ([$($fed:expr),*], $spending:expr, $standby:expr, $cap:expr, $target:expr, $standby_target:expr, $now:expr) => { AllocatorSnapshot { federations: vec![$($fed),*], spending_fed: $spending, standby_fed: $standby, per_fed_cap: msat!($cap), target_spending_balance: msat!($target), standby_target: msat!($standby_target), max_fee: msat!(500), now: $now } };
}
macro_rules! decision {
    ($action:expr, $reason:expr, $occurrence:expr, $key:expr) => { vec![AllocatorDecision { action: $action, reason: $reason, occurrence: $occurrence, idempotency_key: $key, requires_auth: false }] };
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
    // the evacuation destination; `amount` is the evacuating fed's own spendable balance.
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
    // cap_room=40k, want=50k, available=10k: both OverCap and
    // SpendingBelowTarget are true policy signals for the same destination.
    let snapshot = snap!([fed!(1, 60_000, true, false, true), fed!(2, 10_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 110_000, 0, 9000);
    assert_eq!(
        decide(&snapshot, occ(1)),
        vec![
            AllocatorDecision { action: move_action!(2, 1, 10_000), reason: ReasonCode::SpendingBelowTarget, occurrence: occ(1), idempotency_key: move_key(2, 1, 1), requires_auth: false },
            AllocatorDecision { action: refuse!(1, ReasonCode::OverCap), reason: ReasonCode::OverCap, occurrence: occ(1), idempotency_key: refuse_key(1, ReasonCode::OverCap, 1), requires_auth: false },
            AllocatorDecision { action: refuse!(1, ReasonCode::SpendingBelowTarget), reason: ReasonCode::SpendingBelowTarget, occurrence: occ(1), idempotency_key: refuse_key(1, ReasonCode::SpendingBelowTarget, 1), requires_auth: false },
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
}
