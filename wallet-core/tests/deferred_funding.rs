//! br-0vg: a funding goal withheld by the move floor must be reportable.
//!
//! The dust rule itself is correct and stays: a shortfall below the floor could only fail at
//! perform time, and a refusal row every tick would be the noise the floor removes. What was
//! wrong is that the withholding was invisible EVERYWHERE — no decision, no suppressed entry, no
//! ledger row, and (verified on the deployed build and on main) not a single log line at any
//! level. A standby only drains when something spends from it, so a shortfall parked below the
//! floor stays there forever while the wallet looks idle and healthy.
//!
//! These tests pin the diagnostic AND, more importantly, that adding it moved no money.

use std::collections::BTreeMap;
use wallet_core::*;

fn fed(byte: u8) -> FederationId {
    FederationId([byte; 32])
}

fn status(id: FederationId, spendable: u64) -> FederationStatus {
    FederationStatus {
        id,
        balance: FedBalance {
            spendable: Msat(spendable),
            in_flight: Msat(0),
            claimable: Msat(0),
            reserved_fee: Msat(0),
        },
        probed_ok: true,
        reputation: 0,
        shutdown_notice: false,
        healthy: true,
        eligible_to_fund: true,
    }
}

/// The live k8s wallet's exact state on 2026-08-23: standby 301,586 msat below a 500,000 target,
/// spending 561,670 msat above its own, both feds eligible, 300 bps proportional cap.
fn live_wallet_snapshot() -> AllocatorSnapshot {
    AllocatorSnapshot {
        federations: vec![status(fed(0xA), 5_561_670), status(fed(0xB), 198_414)],
        spending_fed: Some(fed(0xA)),
        standby_fed: Some(fed(0xB)),
        per_fed_cap: Msat(100_000_000),
        target_spending_balance: Msat(5_000_000),
        standby_target: Msat(500_000),
        max_fee: Msat(50_000),
        max_fee_bps_of_move: 300,
        evac_fee_base_msat: Msat(0),
        evac_fee_bps: 0,
        min_move: Msat(5_000),
        route_economics_by_pair: BTreeMap::new(),
        reservations: Reservations::default(),
        now: 1_787_463_262_203,
    }
}

fn with_route(mut snapshot: AllocatorSnapshot, floor: u64) -> AllocatorSnapshot {
    snapshot.route_economics_by_pair.insert(
        (fed(0xA), fed(0xB)),
        RouteEconomics {
            resolved_gateway: None,
            min_viable_amount: Msat(floor),
            status: RouteStatus::Routable,
        },
    );
    snapshot
}

const SHORTFALL: u64 = 301_586;

#[test]
fn a_shortfall_under_the_route_floor_is_reported_instead_of_vanishing() {
    // The floor sits above the shortfall, so the move is correctly withheld.
    let snapshot = with_route(live_wallet_snapshot(), 5_000_000);
    let outcome = decide_with_diagnostics(&snapshot, Occurrence(10_552), &GoalBlockers::default());

    assert!(
        outcome.decisions.is_empty() && outcome.suppressed.is_empty(),
        "the dust rule still withholds the move: {outcome:?}"
    );
    assert_eq!(
        outcome.deferred,
        vec![DeferredFunding {
            dest: fed(0xB),
            source: Some(fed(0xA)),
            reason: ReasonCode::StandbyBelowTarget,
            want: Msat(SHORTFALL),
            floor: Msat(5_000_000),
            floor_source: DeferralFloor::RouteMinViable,
        }],
        "an operator must be able to see the goal, its shortfall, and the floor that blocked it"
    );
}

#[test]
fn the_protocol_floor_and_the_route_floor_are_distinguishable() {
    // Below lnv2's minimum incoming contract, with NO route priced: only a bigger gap clears it.
    let mut snapshot = live_wallet_snapshot();
    snapshot.federations = vec![status(fed(0xA), 5_561_670), status(fed(0xB), 499_000)];
    let outcome = decide_with_diagnostics(&snapshot, Occurrence(1), &GoalBlockers::default());
    let goal = outcome
        .deferred
        .first()
        .expect("a 1,000 msat gap is below the 5,000 msat protocol floor");
    assert_eq!(goal.want, Msat(1_000));
    assert_eq!(goal.floor, Msat(5_000));
    assert_eq!(
        goal.floor_source,
        DeferralFloor::ProtocolMinMove,
        "a protocol dust gap must not be reported as an economics problem, or an operator will \
         go looking at gateway fees for a gap that no route change can fix"
    );
}

#[test]
fn a_shortfall_that_clears_the_floor_is_funded_and_not_reported_as_deferred() {
    // Same wallet, but the pair's economics fit: the move must be emitted, nothing deferred.
    let snapshot = with_route(live_wallet_snapshot(), 10_000);
    let outcome = decide_with_diagnostics(&snapshot, Occurrence(10_552), &GoalBlockers::default());

    assert_eq!(
        outcome.deferred,
        vec![],
        "nothing was withheld: {outcome:?}"
    );
    assert!(
        matches!(
            outcome.decisions.as_slice(),
            [AllocatorDecision {
                action: Action::Move { amount, .. },
                reason: ReasonCode::StandbyBelowTarget,
                ..
            }] if *amount == Msat(SHORTFALL)
        ),
        "{:?}",
        outcome.decisions
    );
}

/// The property that makes this change safe to land on a money path: the diagnostic channel is
/// additive. Whatever the snapshot, `decide_with_diagnostics` must emit exactly what
/// `decide_with_blockers` emits — the deferred list is the only new information.
#[test]
fn adding_the_diagnostic_changes_no_decision_anywhere() {
    let mut cases = vec![
        ("unpriced pair", live_wallet_snapshot()),
        (
            "floor above the gap",
            with_route(live_wallet_snapshot(), 5_000_000),
        ),
        (
            "floor below the gap",
            with_route(live_wallet_snapshot(), 10_000),
        ),
    ];
    // A gap under the protocol floor, an at-target wallet, and a source with no surplus.
    let mut at_target = live_wallet_snapshot();
    at_target.federations = vec![status(fed(0xA), 5_561_670), status(fed(0xB), 500_000)];
    cases.push(("standby at target", at_target));
    let mut no_surplus = live_wallet_snapshot();
    no_surplus.federations = vec![status(fed(0xA), 5_000_000), status(fed(0xB), 198_414)];
    cases.push(("spending at target", no_surplus));
    let mut unroutable = live_wallet_snapshot();
    unroutable.route_economics_by_pair.insert(
        (fed(0xA), fed(0xB)),
        RouteEconomics {
            resolved_gateway: None,
            min_viable_amount: Msat(0),
            status: RouteStatus::Unroutable,
        },
    );
    cases.push(("unroutable pair", unroutable));

    for (label, snapshot) in cases {
        let occurrence = Occurrence(10_552);
        let blockers = GoalBlockers::default();
        let (decisions, suppressed) = decide_with_blockers(&snapshot, occurrence, &blockers);
        let outcome = decide_with_diagnostics(&snapshot, occurrence, &blockers);
        assert_eq!(decisions, outcome.decisions, "decisions drifted: {label}");
        assert_eq!(
            suppressed, outcome.suppressed,
            "suppressed drifted: {label}"
        );
        assert_eq!(
            decide(&snapshot, occurrence),
            outcome.decisions,
            "decide() drifted: {label}"
        );
    }
}
