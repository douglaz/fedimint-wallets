//! Pure golden tests for the integer fee model (spec §6): the `GatewayFee` floor (exactly
//! fedimint's `PaymentFee` arithmetic), the fixed-point `gross_up` solver, and the
//! `total_within_cap` check. No async, no I/O, no floats — every assertion is exact integer
//! millisatoshis.

use wallet_core::Msat;
use wallet_fedimint::{gross_up, predicted_net, total_within_cap, GatewayFee, GrossUp};

/// A pure federation-fee closure: `base + floor(ppm * amount / 1_000_000)`. Floors like
/// fedimint's own `PaymentFee` — and so does [`GatewayFee::on`], so the solved invoice nets
/// the recipient EXACTLY the target against the real floored fees, not a msat over.
/// `base = ppm = 0` gives the zero fee.
fn fed_fee(base: u64, ppm: u64) -> impl Fn(Msat) -> Msat {
    move |amount: Msat| {
        let ppm_part = amount.0.saturating_mul(ppm).saturating_div(1_000_000);
        Msat(base.saturating_add(ppm_part))
    }
}

/// Recompute, forward from the solved invoice, exactly what the recipient nets — the
/// independent check that the gross-up landed on the §6 fixed point. Mirrors the executor's
/// real path: gateway fee on the invoice, federation fee on the resulting contract.
fn recipient_nets(g: &GrossUp, gw: GatewayFee, fed: impl Fn(Msat) -> Msat) -> Msat {
    // Saturating, mirroring the solver's and fedimint's real arithmetic (`subtract_from`
    // saturates), so the degenerate `net = 0` case (invoice 0, fee > 0) nets 0, not a panic.
    let contract = Msat(g.invoice_amount.0.saturating_sub(gw.on(g.invoice_amount).0));
    // The solver's own `contract_amount` must equal the gateway-reduced invoice.
    assert_eq!(
        contract, g.contract_amount,
        "contract_amount is invoice − gateway fee"
    );
    Msat(contract.0.saturating_sub(fed(contract).0))
}

/// The full set of invariants every solved gross-up must hold (spec §6):
/// - the recipient nets EXACTLY `net` (never less);
/// - `receive_quote == invoice_amount − net` (the total receive-side cost);
/// - the invoice is MINIMAL: one msat less would under-credit the recipient.
fn assert_solves_exactly(net: Msat, gw: GatewayFee, fed: impl Fn(Msat) -> Msat) -> GrossUp {
    let g = gross_up(net, gw, &fed).expect("a fee with gateway ppm < 100% is solvable");

    assert_eq!(
        recipient_nets(&g, gw, &fed),
        net,
        "recipient must net EXACTLY the target"
    );
    assert!(
        recipient_nets(&g, gw, &fed).0 >= net.0,
        "recipient must never be under-credited"
    );
    assert_eq!(
        g.receive_quote,
        Msat(g.invoice_amount.0 - net.0),
        "receive_quote is invoice_amount − net"
    );

    // Minimality: below the solved invoice the recipient falls short (skip when the invoice
    // already equals net, i.e. the zero-fee floor, where net itself is minimal).
    if g.invoice_amount.0 > net.0 {
        let below = g.invoice_amount.0 - 1;
        let one_less = GrossUp {
            invoice_amount: Msat(below),
            contract_amount: Msat(below.saturating_sub(gw.on(Msat(below)).0)),
            receive_quote: Msat(0),
        };
        assert!(
            recipient_nets(&one_less, gw, &fed).0 < net.0,
            "one msat less than the solved invoice must under-credit"
        );
    }
    g
}

#[test]
fn gateway_fee_floors_ppm_and_adds_base() {
    // base + floor(ppm * amount / 1e6) — byte-for-byte fedimint's `PaymentFee::absolute_fee`.
    let fee = GatewayFee {
        base_msat: Msat(100),
        ppm: 5_000,
    };
    // Exact multiple: 5000 * 1_000_000 / 1e6 = 5000, no rounding.
    assert_eq!(fee.on(Msat(1_000_000)), Msat(5_100));
    // Fractional ppm part FLOORS: 5000 * 1 / 1e6 = 0.005 → 0, so just the base.
    assert_eq!(fee.on(Msat(1)), Msat(100));
    // Zero amount is just the base.
    assert_eq!(fee.on(Msat(0)), Msat(100));

    // Floor is load-bearing (it matches the gateway's real `subtract_from`): 5000 * 201 / 1e6 =
    // 1.005 → floor 1 (ceil would give 2 and leave the recipient a msat over target).
    let ppm_only = GatewayFee {
        base_msat: Msat(0),
        ppm: 5_000,
    };
    assert_eq!(ppm_only.on(Msat(201)), Msat(1));
}

#[test]
fn gateway_fee_matches_fedimint_saturating_ppm_overflow() {
    let fee = GatewayFee {
        base_msat: Msat(7),
        ppm: u64::MAX,
    };
    assert_eq!(
        fee.on(Msat(2)),
        Msat(7 + u64::MAX.saturating_div(1_000_000))
    );
}

#[test]
fn zero_fee_gross_up_is_identity() {
    let net = Msat(1_000_000);
    let gw = GatewayFee {
        base_msat: Msat(0),
        ppm: 0,
    };
    let g = assert_solves_exactly(net, gw, fed_fee(0, 0));
    // No fees anywhere: invoice == contract == net, nothing withheld.
    assert_eq!(g.invoice_amount, net);
    assert_eq!(g.contract_amount, net);
    assert_eq!(g.receive_quote, Msat(0));
}

#[test]
fn gateway_ppm_only_gross_up() {
    // 1% gateway ppm, no base, no federation fee.
    let net = Msat(1_000_000);
    let gw = GatewayFee {
        base_msat: Msat(0),
        ppm: 10_000,
    };
    let g = assert_solves_exactly(net, gw, fed_fee(0, 0));
    // The invoice must exceed net to absorb the gateway cut.
    assert!(g.invoice_amount.0 > net.0);
    // With no federation fee the recipient's credit IS the contract amount.
    assert_eq!(g.contract_amount, net);
}

#[test]
fn gateway_base_only_gross_up() {
    // A flat gateway base fee, no ppm, no federation fee: invoice = net + base exactly.
    let net = Msat(500_000);
    let gw = GatewayFee {
        base_msat: Msat(2_000),
        ppm: 0,
    };
    let g = assert_solves_exactly(net, gw, fed_fee(0, 0));
    assert_eq!(g.invoice_amount, Msat(502_000));
    assert_eq!(g.receive_quote, Msat(2_000));
}

#[test]
fn federation_fee_only_gross_up() {
    // No gateway fee; the federation charges base + 0.3%.
    let net = Msat(1_000_000);
    let gw = GatewayFee {
        base_msat: Msat(0),
        ppm: 0,
    };
    let fed = fed_fee(500, 3_000);
    let g = assert_solves_exactly(net, gw, &fed);
    // With no gateway fee, invoice == contract (nothing withheld before the federation).
    assert_eq!(g.invoice_amount, g.contract_amount);
    assert!(g.invoice_amount.0 > net.0);
}

#[test]
fn both_fees_gross_up_is_a_fixed_point() {
    // Both a gateway fee (base + 0.5%) AND a federation fee (base + 0.3%): the invoice is a
    // genuine fixed point — the gateway fee is on the invoice, the federation fee on the
    // resulting contract, neither on `net`.
    let net = Msat(1_000_000);
    let gw = GatewayFee {
        base_msat: Msat(1_000),
        ppm: 5_000,
    };
    let fed = fed_fee(400, 3_000);
    assert_solves_exactly(net, gw, &fed);
}

#[test]
fn gross_up_is_stable_and_monotonic() {
    let gw = GatewayFee {
        base_msat: Msat(1_000),
        ppm: 5_000,
    };
    let fed = fed_fee(400, 3_000);

    // Stable: the pure solver returns an identical result for identical inputs.
    let a = gross_up(Msat(1_000_000), gw, &fed);
    let b = gross_up(Msat(1_000_000), gw, &fed);
    assert_eq!(a, b);

    // Monotonic: a larger net never yields a smaller invoice / contract / receive-quote.
    let mut prev = gross_up(Msat(0), gw, &fed).expect("solvable");
    for net in [Msat(1), Msat(1_000), Msat(1_000_000), Msat(9_999_999)] {
        let g = gross_up(net, gw, &fed).expect("solvable");
        assert!(
            g.invoice_amount.0 >= prev.invoice_amount.0,
            "invoice monotonic in net"
        );
        assert!(
            g.contract_amount.0 >= prev.contract_amount.0,
            "contract monotonic in net"
        );
        // And each still nets exactly its target.
        assert_eq!(recipient_nets(&g, gw, &fed), net);
        prev = g;
    }
}

#[test]
fn gross_up_never_under_credits_across_a_sweep() {
    // Sweep a range of nets against a realistic combined fee and assert the never-under
    // invariant everywhere (with exact convergence).
    let gw = GatewayFee {
        base_msat: Msat(50),
        ppm: 5_000,
    };
    let fed = fed_fee(2, 3_000);
    for net in (0..200_000).step_by(1_777).map(Msat) {
        assert_solves_exactly(net, gw, &fed);
    }
}

#[test]
fn gross_up_rejects_unsolvable_gateway_fee_instead_of_hanging() {
    // A gateway that keeps 100% (or more) of every invoice (ppm ≥ 1_000_000) has NO fixed
    // point: no invoice can net a positive amount. The solver must report "unsolvable"
    // (`None`) rather than spin forever — this is a value a broken/hostile gateway can
    // advertise, so the executor turns it into a terminal error instead of a hang.
    let net = Msat(100_000);
    for ppm in [1_000_000_u64, 1_000_001, 5_000_000, u64::MAX] {
        let gw = GatewayFee {
            base_msat: Msat(0),
            ppm,
        };
        assert_eq!(
            gross_up(net, gw, fed_fee(0, 0)),
            None,
            "gateway ppm {ppm} (>= 100%) has no solution"
        );
        // A base fee on top does not change unsolvability (it only grows the invoice).
        let gw_with_base = GatewayFee {
            base_msat: Msat(1_000),
            ppm,
        };
        assert_eq!(gross_up(net, gw_with_base, fed_fee(500, 3_000)), None);
    }

    // The exact boundary: one ppm below 100% is still solvable and terminates (even though
    // the invoice is large), and nets exactly the target.
    let gw = GatewayFee {
        base_msat: Msat(0),
        ppm: 999_999,
    };
    let g = gross_up(net, gw, fed_fee(0, 0)).expect("ppm 999_999 (< 100%) is solvable");
    assert_eq!(recipient_nets(&g, gw, fed_fee(0, 0)), net);
}

#[test]
fn total_within_cap_sums_both_legs() {
    // A Move: both legs count against the cap.
    assert!(total_within_cap(Msat(100), Msat(50), Msat(150)));
    assert!(!total_within_cap(Msat(100), Msat(51), Msat(150)));
    // Exactly at the cap is within.
    assert!(total_within_cap(Msat(100), Msat(0), Msat(100)));
    assert!(!total_within_cap(Msat(101), Msat(0), Msat(100)));
}

#[test]
fn direct_inflow_cap_check_ignores_the_send_leg() {
    // A DirectInflow has no send leg (`send_quote == 0`), so only the receive side counts —
    // a receive cost that fits the cap passes even though a Move with the same receive cost
    // plus a send leg would not.
    let receive_quote = Msat(400);
    let send_quote = Msat(300);
    let fee_cap = Msat(500);

    // DirectInflow: receive-only, within cap.
    assert!(total_within_cap(receive_quote, Msat(0), fee_cap));
    // Same receive cost as a Move (with the send leg) blows the cap.
    assert!(!total_within_cap(receive_quote, send_quote, fee_cap));
}

#[test]
fn predicted_net_matches_the_solver_for_a_constant_fee() {
    // When the async re-quote loop CONVERGES (fee at the solved contract == the fee the
    // solve used), the verification pass must confirm exact net — no clamp, no drift.
    let gw = GatewayFee {
        base_msat: Msat(10),
        ppm: 1_000,
    };
    let net = Msat(449_968);
    let fed = Msat(1_234);
    let g = gross_up(net, gw, |_| fed).expect("solvable");
    assert_eq!(predicted_net(g.invoice_amount, gw, fed), net);
}

#[test]
fn never_over_verification_catches_the_stale_fee_over_credit() {
    // The live 3.A evacuate-smoke failure shape: a solve carrying a STALE constant fee
    // (100) meets a real fee of 22 at the solved contract — predicted_net exposes the
    // +78 over-credit the unverified loop would have minted, and re-solving with the
    // verified fee lands exactly on net (never over).
    let gw = GatewayFee {
        base_msat: Msat(0),
        ppm: 0,
    };
    let net = Msat(950);
    let stale = gross_up(net, gw, |_| Msat(100)).expect("solvable");
    assert_eq!(stale.invoice_amount, Msat(1_050));
    let real_fee = Msat(22);
    let predicted = predicted_net(stale.invoice_amount, gw, real_fee);
    assert_eq!(
        predicted,
        Msat(1_028),
        "over-credits by 78 without verification"
    );
    // The executor's response: RE-SOLVE with the verified fee (not a linear shrink).
    let resolved = gross_up(net, gw, |_| real_fee).expect("solvable");
    assert_eq!(predicted_net(resolved.invoice_amount, gw, real_fee), net);
}

#[test]
fn resolve_with_the_verified_fee_closes_a_ppm_overshoot_in_one_pass() {
    // With a large gateway ppm (50%), shrinking the invoice by the excess would only
    // close HALF the overshoot per pass (net moves by (1 - ppm/1e6) per invoice msat) —
    // a geometric decay that never terminates in bounded passes. A full re-solve with
    // the verified fee closes it in ONE pass because the solver models the ppm exactly.
    let gw = GatewayFee {
        base_msat: Msat(0),
        ppm: 500_000, // 50%
    };
    let net = Msat(10_000);
    let stale_fee = Msat(1_000);
    let real_fee = Msat(0);
    let stale = gross_up(net, gw, |_| stale_fee).expect("solvable");
    let predicted = predicted_net(stale.invoice_amount, gw, real_fee);
    assert!(
        predicted.0 > net.0,
        "stale solve over-credits under the real fee"
    );
    let resolved = gross_up(net, gw, |_| real_fee).expect("solvable");
    assert_eq!(predicted_net(resolved.invoice_amount, gw, real_fee), net);
}

#[test]
fn oscillating_step_fee_yields_a_safe_under_netting_fallback() {
    // A two-valued oscillation: solving with the LOWER fee produces a contract whose real
    // fee is the HIGHER one — the prediction under-nets (safe, never over), which is the
    // executor's fallback candidate when exactness cannot be reached in bounded passes.
    let gw = GatewayFee {
        base_msat: Msat(0),
        ppm: 0,
    };
    let net = Msat(950);
    let (f_lo, f_hi) = (Msat(22), Msat(100));
    let solved_lo = gross_up(net, gw, |_| f_lo).expect("solvable");
    let predicted = predicted_net(solved_lo.invoice_amount, gw, f_hi);
    assert!(
        predicted.0 < net.0,
        "under-nets: never-over holds for the fallback"
    );
    assert!(
        net.0 - predicted.0 <= (f_hi.0 - f_lo.0),
        "under by at most the fee step"
    );
    // The executor's fallback RESTATES the receive quote to the VERIFIED cost
    // (invoice − predicted): the solve's own invoice − net would UNDERSTATE the real fees
    // by the shortfall and weaken every downstream fee-cap check.
    let honest_quote = solved_lo.invoice_amount.0 - predicted.0;
    assert!(
        honest_quote > solved_lo.receive_quote.0,
        "restated quote exceeds the stale one"
    );
    assert_eq!(
        honest_quote,
        solved_lo.receive_quote.0 + (net.0 - predicted.0),
        "restatement adds exactly the shortfall"
    );
}
