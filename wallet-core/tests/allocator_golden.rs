#[rustfmt::skip]
mod golden {
use wallet_core::*;

macro_rules! id { ($id:expr) => { FederationId([$id; 32]) }; }
macro_rules! msat { ($amount:expr) => { Msat($amount) }; }
macro_rules! guardian { ($g:expr) => { GuardianId(vec![$g]) }; }
macro_rules! fed {
    ($id:expr, $balance:expr, $probed:expr, $shutdown:expr, $healthy:expr) => { fed!($id, $balance, $probed, 0, $shutdown, $healthy) };
    ($id:expr, $balance:expr, $probed:expr, $reputation:expr, $shutdown:expr, $healthy:expr) => { FederationStatus { id: id!($id), balance: msat!($balance), probed_ok: $probed, reputation: $reputation, guardians: if $id == 1 { vec![guardian!(1), guardian!(2), guardian!(3)] } else { vec![guardian!(4), guardian!(5), guardian!(6)] }, shutdown_notice: $shutdown, healthy: $healthy } };
}
macro_rules! snap {
    ([$($fed:expr),*], $spending:expr, $standby:expr, $cap:expr, $target:expr, $standby_target:expr, $now:expr) => { AllocatorSnapshot { federations: vec![$($fed),*], spending_fed: $spending, standby_fed: $standby, per_fed_cap: msat!($cap), target_spending_balance: msat!($target), standby_target: msat!($standby_target), max_fee: msat!(500), now: $now } };
}
macro_rules! decision {
    ($action:expr, $reason:expr, $key:expr) => { vec![AllocatorDecision { action: $action, reason: $reason, max_fee: msat!(500), idempotency_key: $key, requires_auth: false }] };
}
macro_rules! topup {
    ($from:expr, $to:expr, $amount:expr) => { Action::TopUpSpending { from: id!($from), to: id!($to), amount: msat!($amount) } };
}
macro_rules! standby {
    ($from:expr, $to:expr, $amount:expr) => { Action::FundStandby { from: id!($from), to: id!($to), amount: msat!($amount) } };
}
macro_rules! evacuate {
    ($from:expr, $reason:expr) => { Action::Evacuate { from: id!($from), reason: $reason } };
}
macro_rules! refuse {
    ($fed:expr, $reason:expr) => { Action::RefuseAllocation { fed: id!($fed), reason: $reason } };
}

// Idempotency keys are stable per logical intent (no clock): see allocator::idem.
// The federation identity is `hex(FederationId)` now (was a local u32), so we build the
// expected key the same way the allocator does instead of hard-coding 64-char hex.
fn ikey(kind: &str, from: Option<u8>, to: Option<u8>, amount: u64) -> IdempotencyKey {
    let f = from.map_or_else(|| "-".to_string(), |n| FederationId([n; 32]).to_hex());
    let t = to.map_or_else(|| "-".to_string(), |n| FederationId([n; 32]).to_hex());
    IdempotencyKey(format!("{kind}:{f}:{t}:{amount}"))
}

#[test]
fn top_up_spending_below_target() {
    let snapshot = snap!([fed!(1, 20_000, true, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 60_000, 0, 1000);
    assert_eq!(decide(&snapshot), decision!(topup!(2, 1, 40_000), ReasonCode::SpendingBelowTarget, ikey("topup", Some(2), Some(1), 40_000)));
}

#[test]
fn fund_independent_warm_standby() {
    let snapshot = snap!([fed!(1, 80_000, true, false, true), fed!(2, 5_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 50_000, 20_000, 2000);
    assert_eq!(decide(&snapshot), decision!(standby!(1, 2, 15_000), ReasonCode::StandbyBelowTarget, ikey("standby", Some(1), Some(2), 15_000)));
}

#[test]
fn evacuate_on_shutdown_notice() {
    let snapshot = snap!([fed!(1, 50_000, true, true, true)], Some(id!(1)), None, 100_000, 100_000, 0, 3000);
    assert_eq!(decide(&snapshot), decision!(evacuate!(1, ReasonCode::ShutdownNotice), ReasonCode::ShutdownNotice, ikey("evacuate", Some(1), None, 0)));
}

#[test]
fn refuse_over_per_fed_cap() {
    // Spending fed is already at the cap, so it cannot be topped up to target.
    let snapshot = snap!([fed!(1, 50_000, true, false, true), fed!(2, 30_000, true, false, true)], Some(id!(1)), Some(id!(2)), 50_000, 80_000, 0, 4000);
    assert_eq!(decide(&snapshot), decision!(refuse!(1, ReasonCode::OverCap), ReasonCode::OverCap, ikey("refuse:over_cap", None, Some(1), 0)));
}

#[test]
fn do_not_fund_unprobed_federation() {
    // High reputation must NOT promote an unprobed fed past the probe gate (ADR-0017).
    let snapshot = snap!([fed!(1, 10_000, false, 100, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 60_000, 0, 5000);
    assert_eq!(decide(&snapshot), decision!(refuse!(1, ReasonCode::NotProbed), ReasonCode::NotProbed, ikey("refuse:not_probed", None, Some(1), 0)));
}

#[test]
fn refuse_already_over_cap_balance() {
    // fed 2 is over the cap from its own balance (not from our funding): flag it (ADR-0018).
    let snapshot = snap!([fed!(1, 40_000, true, false, true), fed!(2, 90_000, true, false, true)], Some(id!(1)), None, 50_000, 40_000, 0, 6000);
    assert_eq!(decide(&snapshot), decision!(refuse!(2, ReasonCode::OverCap), ReasonCode::OverCap, ikey("refuse:over_cap", None, Some(2), 0)));
}

#[test]
fn low_reputation_blocks_receive() {
    // Negative reputation demotes below the receive floor: do not fund into it (ADR-0017).
    let snapshot = snap!([fed!(1, 20_000, true, -1, false, true), fed!(2, 80_000, true, false, true)], Some(id!(1)), Some(id!(2)), 100_000, 60_000, 0, 7000);
    assert_eq!(decide(&snapshot), decision!(refuse!(1, ReasonCode::LowReputation), ReasonCode::LowReputation, ikey("refuse:low_reputation", None, Some(1), 0)));
}

#[test]
fn no_independent_standby_when_guardians_overlap() {
    // Standby shares a guardian with the spending fed -> not real insurance, do not fund it (ADR-0010).
    let spending = FederationStatus { id: id!(1), balance: msat!(80_000), probed_ok: true, reputation: 0, guardians: vec![guardian!(1), guardian!(2), guardian!(3)], shutdown_notice: false, healthy: true };
    let standby = FederationStatus { id: id!(2), balance: msat!(5_000), probed_ok: true, reputation: 0, guardians: vec![guardian!(3), guardian!(9), guardian!(9)], shutdown_notice: false, healthy: true };
    let snapshot = AllocatorSnapshot { federations: vec![spending, standby], spending_fed: Some(id!(1)), standby_fed: Some(id!(2)), per_fed_cap: msat!(100_000), target_spending_balance: msat!(50_000), standby_target: msat!(20_000), max_fee: msat!(500), now: 8000 };
    assert_eq!(decide(&snapshot), decision!(refuse!(2, ReasonCode::NoIndependentStandby), ReasonCode::NoIndependentStandby, ikey("refuse:no_independent_standby", None, Some(2), 0)));
}
}
