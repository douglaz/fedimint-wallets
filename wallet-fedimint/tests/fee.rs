//! Pure golden tests for the integer fee model (spec §6): the `GatewayFee` ceil, the
//! fixed-point `gross_up` solver, and the `total_within_cap` check. No async, no I/O, no
//! floats — every assertion is exact integer millisatoshis.

use wallet_core::Msat;
use wallet_fedimint::{gross_up, total_within_cap, GatewayFee, GrossUp};

/// A pure federation-fee closure: `base + floor(ppm * amount / 1_000_000)`. Floors like
/// fedimint's own `PaymentFee` (the gross-up rounds the *gateway* fee up to compensate, so
/// the recipient is never short even against a floored federation fee). `base = ppm = 0`
/// gives the zero fee.
fn fed_fee(base: u64, ppm: u64) -> impl Fn(Msat) -> Msat {
    move |amount: Msat| {
        let ppm_part = (u128::from(ppm) * u128::from(amount.0) / 1_000_000) as u64;
        Msat(base + ppm_part)
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
    let g = gross_up(net, gw, &fed);

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
fn gateway_fee_ceil_rounds_up_and_adds_base() {
    // base + ceil(ppm * amount / 1e6).
    let fee = GatewayFee {
        base_msat: Msat(100),
        ppm: 5_000,
    };
    // Exact multiple: 5000 * 1_000_000 / 1e6 = 5000, no rounding.
    assert_eq!(fee.on(Msat(1_000_000)), Msat(5_100));
    // Fractional ppm part rounds UP: 5000 * 1 / 1e6 = 0.005 → 1.
    assert_eq!(fee.on(Msat(1)), Msat(101));
    // Zero amount is just the base.
    assert_eq!(fee.on(Msat(0)), Msat(100));

    // Ceil vs floor is load-bearing: 5000 * 201 / 1e6 = 1.005 → ceil 2 (floor would give 1).
    let ppm_only = GatewayFee {
        base_msat: Msat(0),
        ppm: 5_000,
    };
    assert_eq!(ppm_only.on(Msat(201)), Msat(2));
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
    let mut prev = gross_up(Msat(0), gw, &fed);
    for net in [Msat(1), Msat(1_000), Msat(1_000_000), Msat(9_999_999)] {
        let g = gross_up(net, gw, &fed);
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
