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
//! a fee applied to `net` directly. [`gross_up`] solves it with integer arithmetic, always
//! rounding so the recipient nets AT LEAST `net` (never under-credits).

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
    /// The fee this gateway charges on `amount`: `base + ceil(ppm * amount / 1_000_000)`.
    ///
    /// The ppm part rounds **UP** (integer ceil), deliberately: [`gross_up`] must never
    /// leave the recipient short, so over-estimating the fee by at most one msat is the
    /// safe rounding direction. (Fedimint's own `PaymentFee` floors its ppm part; quoting
    /// UP here keeps the solved invoice at least large enough once the real, floored fee is
    /// applied on-federation.)
    pub fn on(&self, amount: Msat) -> Msat {
        // u128 so `ppm * amount` cannot overflow before the divide (ppm ≤ ~1e6, amount ≤
        // u64::MAX); `div_ceil` rounds the ppm part UP.
        let numerator = u128::from(self.ppm) * u128::from(amount.0);
        let ppm_part = numerator.div_ceil(1_000_000);
        let ppm_part = u64::try_from(ppm_part).unwrap_or(u64::MAX);
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

/// A generous bound on the fixed-point iterations. Each pass adds the FULL remaining
/// shortfall to the invoice, so for any sane fee (ppm slope well under 1) the deficit
/// decays geometrically and converges in a handful of passes; this bound only backstops a
/// pathological fee closure. A defensive tail loop then still guarantees the never-under-
/// credit invariant even in the (unreachable-for-real-fees) event the bound is hit.
const MAX_ITERS: u32 = 64;

/// Solve the §6 receive-side fixed point: find the smallest `invoice_amount` such that,
/// after the gateway's cut and the federation's tx fee, the recipient nets EXACTLY `net`
/// (and NEVER less).
///
/// - `net` — the credit the destination must end up with.
/// - `recv_gateway` — the gateway's receive fee, quoted on `invoice_amount`.
/// - `recv_fed_fee` — the federation's receive tx fee, quoted on `contract_amount`. This is
///   the caller-supplied SEAM that keeps `gross_up` pure: production passes a closure over
///   the client's quote API; tests pass a pure function. It is called on `contract_amount`
///   (the post-gateway value), NEVER on `net` (spec §6).
///
/// The solver is a monotone upward fixed point: start at `invoice_amount = net` and, while
/// the forward computation lands short, bump the invoice by exactly the shortfall. Because
/// every real fee has slope ≤ 1 (a fee is a fraction of what it is charged on), each bump
/// cannot overshoot `net`, so the iteration climbs from below and stops at the minimal
/// invoice that makes the recipient net exactly `net`.
pub fn gross_up(
    net: Msat,
    recv_gateway: GatewayFee,
    recv_fed_fee: impl Fn(Msat) -> Msat,
) -> GrossUp {
    // Forward pass: given a candidate invoice, what does the recipient actually net?
    let forward = |invoice_amount: u64| -> u64 {
        let contract = invoice_amount.saturating_sub(recv_gateway.on(Msat(invoice_amount)).0);
        contract.saturating_sub(recv_fed_fee(Msat(contract)).0)
    };

    // Lower bound: with zero fees the invoice equals `net`; any fee only grows it.
    let mut invoice_amount = net.0;
    for _ in 0..MAX_ITERS {
        let got = forward(invoice_amount);
        if got >= net.0 {
            break;
        }
        // `got <= net` holds throughout (slope ≤ 1), so this shortfall never underflows.
        invoice_amount = invoice_amount.saturating_add(net.0 - got);
    }
    // Defensive: guarantee the recipient is never under-credited even if the bounded loop
    // above stopped short for a pathological (non-real) fee closure. A no-op for sane fees.
    while forward(invoice_amount) < net.0 {
        invoice_amount = invoice_amount.saturating_add(1);
    }

    let invoice_amount = Msat(invoice_amount);
    let contract_amount = Msat(
        invoice_amount
            .0
            .saturating_sub(recv_gateway.on(invoice_amount).0),
    );
    // Total receive-side cost = gateway fee + federation fee = invoice_amount − net.
    let receive_quote = Msat(invoice_amount.0.saturating_sub(net.0));
    GrossUp {
        invoice_amount,
        contract_amount,
        receive_quote,
    }
}

/// Whether the summed cost of both legs fits under `fee_cap` (spec §6). For a `Move` both
/// quotes are real; for a `DirectInflow` the send leg is absent so `send_quote == Msat(0)`
/// and only the receive side counts.
pub fn total_within_cap(receive_quote: Msat, send_quote: Msat, fee_cap: Msat) -> bool {
    receive_quote.0.saturating_add(send_quote.0) <= fee_cap.0
}
