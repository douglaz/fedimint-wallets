//! The PURE, integer-only fee model for a cross-federation move (spec §6).
//!
//! This module is **pure Rust**: no fedimint SDK, no async, no I/O, no floats — every
//! value is integer millisatoshis and every operation is deterministic. It is the
//! golden-testable half of the fee policy. The executor (a later, I/O-bearing step)
//! supplies the live gateway/federation fee numbers and calls into the two pure entry
//! points here: [`gross_up`] (size the receive invoice so the recipient nets EXACTLY the
//! target) and [`total_within_cap`] (enforce `fee_cap` over both legs).
//!
//! # Why a fixed point (spec §6)
//! `amount` is the NET credit the destination must end up with, but BOTH receive-side fees
//! scale with the GROSS invoice: the gateway takes a ppm cut of the invoice FIRST
//! (invoice → on-federation `contract_amount`), then the federation charges its tx fee on
//! that `contract_amount`. So the invoice size is the solution to
//! `contract_amount − fed_fee(contract_amount) == net`, with
//! `contract_amount = invoice_amount − gateway_fee(invoice_amount)` — a fixed point, never
//! a fee applied to `net` directly. [`gross_up`] solves it with integer arithmetic.
//!
//! # Exact inversion of the real on-federation math (verified against the pinned lnv2 source)
//! The gateway's real receive fee is `fedimint_lnv2_common`'s `PaymentFee::subtract_from`,
//! which FLOORS its ppm part after `u64` saturating multiplication
//! (`saturating_mul(ppm).saturating_div(1_000_000)` + base), and the
//! federation's real tx fee is exactly `LightningClientModule::receive_fee_quote(contract)`
//! (`create_contract_and_fetch_invoice`: `contract_amount = receive_fee.subtract_from(amount)`;
//! the claim then pays the quoted federation fee). So [`GatewayFee::on`] mirrors that FLOOR and
//! the executor quotes the federation fee on the SOLVED contract — the prediction equals the
//! deterministic reality msat-for-msat, and the recipient nets EXACTLY `net` (spec §6, the
//! ADR-0022 cheap-lever gate). Rounding the gateway fee UP would over-quote it by up to one
//! msat and leave the recipient a msat OVER target — off the exact-net contract.

use wallet_core::Msat;

/// A gateway's payment fee: a flat `base_msat` plus a `ppm` (parts-per-million) share of
/// the amount it is quoted on. Bridges fedimint's `PaymentFee { base, parts_per_million }`
/// (spec §6); the executor builds one from the pinned gateway's `routing_info`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayFee {
    pub base_msat: Msat,
    pub ppm: u64,
}

impl GatewayFee {
    /// The fee this gateway charges on `amount`: `base + floor(ppm * amount / 1_000_000)`.
    ///
    /// The ppm part FLOORS (integer division), byte-for-byte identical to fedimint's real
    /// `PaymentFee::absolute_fee` (`msats.saturating_mul(ppm).saturating_div(1_000_000)` +
    /// `base`), the arithmetic behind `subtract_from` that the gateway applies to size the
    /// on-federation contract. Matching it exactly is what lets [`gross_up`] land the recipient
    /// on EXACTLY `net`: our predicted `contract_amount = invoice − on(invoice)` then equals the
    /// gateway's real `subtract_from(invoice)` with no rounding gap (spec §6).
    pub fn on(&self, amount: Msat) -> Msat {
        // Match fedimint's `absolute_fee`: saturate the u64 multiplication BEFORE dividing.
        // Using a widened multiply here would overestimate the gateway fee at overflow edges.
        let ppm_part = amount.0.saturating_mul(self.ppm).saturating_div(1_000_000);
        Msat(self.base_msat.0.saturating_add(ppm_part))
    }
}

/// The solved receive-side sizing (spec §6). `invoice_amount` is what the payer sends;
/// `contract_amount` is what lands on B's federation after the gateway's cut; the recipient
/// then nets `contract_amount − fed_fee(contract_amount)` (== the caller's `net`).
/// `receive_quote` is the total receive-side cost, `invoice_amount − net` (gateway + fed).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrossUp {
    pub invoice_amount: Msat,
    pub contract_amount: Msat,
    pub receive_quote: Msat,
}

/// The gateway ppm rate at (or above) which the fixed point has NO solution: the gateway
/// keeps 100% (or more) of every marginal msat, so the recipient can never net a positive
/// amount however large the invoice. [`gross_up`] returns `None` at or above this rate rather
/// than searching to `u64::MAX` on a broken/hostile gateway's advertised fee (spec §6). This
/// is the exact convergence boundary: a base fee and the (bounded) federation fee are constant
/// offsets that only grow the invoice, so any `ppm < 1_000_000` is solvable. Public so the
/// executor's `solve_gross_up` can tell this cause apart from a doubling-exhausted `None`
/// (spec §15.11) when it explains an unsolvable receive.
pub const UNSOLVABLE_GATEWAY_PPM: u64 = 1_000_000;

/// Solve the §6 receive-side fixed point: find the smallest `invoice_amount` such that,
/// after the gateway's cut and the federation's tx fee, the recipient nets EXACTLY `net`
/// (and NEVER less). Returns `None` when the gateway fee makes the fixed point unsolvable
/// (its ppm rate is ≥ 100%, so no invoice nets a positive amount — see
/// [`UNSOLVABLE_GATEWAY_PPM`]); every solvable fee yields `Some`.
///
/// - `net` — the credit the destination must end up with.
/// - `recv_gateway` — the gateway's receive fee, quoted on `invoice_amount`.
/// - `recv_fed_fee` — the federation's receive tx fee, quoted on `contract_amount`. This is
///   the caller-supplied SEAM that keeps `gross_up` pure: production passes a closure over
///   the client's quote API; tests pass a pure function. It is called on `contract_amount`
///   (the post-gateway value), NEVER on `net` (spec §6).
///
/// The recipient's net is a NON-DECREASING step function of the invoice — one extra msat of
/// invoice adds at most one msat of total fee, so `forward` grows by 0 or 1 per msat and never
/// overshoots. The minimal invoice that nets exactly `net` is therefore found by a monotone
/// binary search: bracket an upper bound by doubling (bounded by `u64::MAX`, which for a
/// solvable fee always clears `net`), then bisect. This converges in O(log) fee evaluations
/// for ANY solvable fee — including near-100% gateway rates — so a pathological quote can never
/// make the search run long, and an unsolvable one (even `u64::MAX` falls short) returns `None`.
pub fn gross_up(
    net: Msat,
    recv_gateway: GatewayFee,
    recv_fed_fee: impl Fn(Msat) -> Msat,
) -> Option<GrossUp> {
    // No fixed point exists when the gateway takes the entire invoice: `forward` below stays
    // pinned at 0 for every candidate. Reject up front instead of searching to `u64::MAX` on a
    // misconfigured/hostile gateway's advertised ppm (spec §6); the doubling bound below is the
    // generic backstop for any other non-convergent closure.
    if recv_gateway.ppm >= UNSOLVABLE_GATEWAY_PPM {
        return None;
    }

    // Forward pass: given a candidate invoice, what does the recipient actually net?
    let forward = |invoice_amount: u64| -> u64 {
        let contract = invoice_amount.saturating_sub(recv_gateway.on(Msat(invoice_amount)).0);
        contract.saturating_sub(recv_fed_fee(Msat(contract)).0)
    };

    // Lower bound: with zero fees the invoice equals `net`; any fee only grows it. Bracket an
    // upper bound where the recipient clears `net`, doubling until it does (or `u64::MAX` still
    // falls short → no solution).
    let mut hi = net.0;
    while forward(hi) < net.0 {
        if hi == u64::MAX {
            return None;
        }
        hi = hi.saturating_mul(2);
    }
    // Invariant: `forward(net.0) < net <= forward(hi)` once we bisect (the `lo` end may equal
    // `net.0`). Find the minimal invoice in `[net.0, hi]` with `forward(invoice) >= net`.
    let mut lo = net.0;
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if forward(mid) >= net.0 {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    // `lo` is the answer when it already clears `net` (the zero-fee / net==0 case); otherwise
    // `hi` is the first invoice that does. `forward` steps by ≤ 1, so the winner nets EXACTLY
    // `net` and one msat less under-credits.
    let invoice_amount = if forward(lo) >= net.0 { lo } else { hi };

    let invoice_amount = Msat(invoice_amount);
    let contract_amount = Msat(
        invoice_amount
            .0
            .saturating_sub(recv_gateway.on(invoice_amount).0),
    );
    // Total receive-side cost = gateway fee + federation fee = invoice_amount − net.
    let receive_quote = Msat(invoice_amount.0.saturating_sub(net.0));
    Some(GrossUp {
        invoice_amount,
        contract_amount,
        receive_quote,
    })
}

/// Whether the summed cost of both legs fits under `fee_cap` (spec §6). For a `Move` both
/// quotes are real; for a `DirectInflow` the send leg is absent so `send_quote == Msat(0)`
/// and only the receive side counts.
pub fn total_within_cap(receive_quote: Msat, send_quote: Msat, fee_cap: Msat) -> bool {
    receive_quote.0.saturating_add(send_quote.0) <= fee_cap.0
}

/// What the recipient nets for a FIXED `invoice_amount` under `recv_gateway` and a federation
/// fee of `recv_fed_fee` quoted at that invoice's contract: the §6 forward pass as a pure,
/// standalone check. The executor uses it to VERIFY the async fixed point's exit: the fed fee
/// is a step function of the contract amount, so the bounded re-quote loop can exhaust its
/// passes on an oscillation and exit with a solve whose constant-fee assumption no longer
/// matches the fee at the solved contract — netting the recipient MORE than `net` (an
/// over-credit that can also push the destination past its hard per-fed cap).
pub fn predicted_net(invoice_amount: Msat, recv_gateway: GatewayFee, recv_fed_fee: Msat) -> Msat {
    let contract = invoice_amount
        .0
        .saturating_sub(recv_gateway.on(invoice_amount).0);
    Msat(contract.saturating_sub(recv_fed_fee.0))
}
