//! Pure golden tests for [`MovePlan::from_action`] (spec §3.1/§7): the gateway-free mapping
//! from an `Action` to the move parameters the executor performs. Every executable action
//! maps to a plan; everything Phase 1 does not perform maps to `None`.

use wallet_core::{Action, FederationId, Msat, ReasonCode};
use wallet_fedimint::MovePlan;

const FED_A: FederationId = FederationId([0xAA; 32]);
const FED_B: FederationId = FederationId([0xBB; 32]);

#[test]
fn move_maps_to_a_two_leg_plan() {
    let action = Action::Move {
        from: FED_A,
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
    };
    let plan = MovePlan::from_action(&action).expect("Move is executable");
    assert_eq!(plan.from, Some(FED_A));
    assert_eq!(plan.to, FED_B);
    assert_eq!(plan.amount, Msat(100_000));
    assert_eq!(plan.fee_cap, Msat(2_000));
    // A Move must pay a send leg.
    assert!(plan.send_required);
}

#[test]
fn direct_inflow_maps_to_a_receive_only_plan() {
    let action = Action::DirectInflow {
        to: FED_B,
        amount: Msat(50_000),
        fee_cap: Msat(500),
    };
    let plan = MovePlan::from_action(&action).expect("DirectInflow is executable");
    // Receive-only: no source federation, no send leg.
    assert_eq!(plan.from, None);
    assert_eq!(plan.to, FED_B);
    assert_eq!(plan.amount, Msat(50_000));
    assert_eq!(plan.fee_cap, Msat(500));
    assert!(!plan.send_required);
}

#[test]
fn evacuate_is_unsupported_in_phase_1() {
    // Modeled, but the executor returns `Unsupported` for it — so there is no plan.
    let action = Action::Evacuate {
        from: FED_A,
        to: FED_B,
        amount: Msat(100_000),
        fee_cap: Msat(2_000),
    };
    assert_eq!(MovePlan::from_action(&action), None);
}

#[test]
fn advisory_actions_have_no_plan() {
    // `RefuseInflow`/`Cap` are policy signals, never executed as a move.
    let refuse = Action::RefuseInflow {
        fed: FED_A,
        reason: ReasonCode::OverCap,
    };
    let cap = Action::Cap {
        fed: FED_A,
        reason: ReasonCode::OverCap,
    };
    assert_eq!(MovePlan::from_action(&refuse), None);
    assert_eq!(MovePlan::from_action(&cap), None);
}

#[test]
fn plan_send_required_always_agrees_with_from() {
    // The load-bearing invariant `next_step`/`assemble_move_record` debug-assert: a plan's
    // `send_required` matches `from.is_some()` for every executable action.
    for action in [
        Action::Move {
            from: FED_A,
            to: FED_B,
            amount: Msat(1),
            fee_cap: Msat(1),
        },
        Action::DirectInflow {
            to: FED_B,
            amount: Msat(1),
            fee_cap: Msat(1),
        },
    ] {
        let plan = MovePlan::from_action(&action).expect("executable");
        assert_eq!(plan.send_required, plan.from.is_some());
    }
}
