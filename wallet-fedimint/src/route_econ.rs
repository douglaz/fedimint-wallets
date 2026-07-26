//! Live per-pair route pricing for allocator snapshots.
//!
//! I/O stays here, outside `wallet_core::decide`: a tick supplies live gateway and federation
//! quotes, then the pure allocator reads the resulting `RouteEconomics` values.

use crate::fee::GatewayFee;
use crate::multi_client::MultiClient;
use crate::types::GatewayUrl;
use fedimint_core::runtime;
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;
use wallet_core::{AllocatorSnapshot, FederationId, Msat, RouteEconomics, RouteStatus};

const PPM: u128 = crate::fee::UNSOLVABLE_GATEWAY_PPM as u128;
const BPS: u128 = 10_000;

/// A complete economic argmin is intentionally bounded. If a federation registers more
/// candidates, this tick leaves the pair missing (the allocator's permissive fallback) instead
/// of claiming a prefix winner is globally cheapest.
const MAX_ROUTE_GATEWAYS: usize = 4;

/// Two ordered pairs, at most four candidates per pair, plus each pair's gateway-list and two
/// federation-fee calls. This is a hard whole-tick cap, shared across planner revisions.
const MAX_ROUTE_CALLS: usize = 24;
const ROUTE_WORK_BUDGET_MS: u64 = 10_000;

#[derive(Debug)]
pub(crate) struct RouteQuoteBudget {
    deadline_ms: u64,
    remaining_calls: usize,
}

impl RouteQuoteBudget {
    pub fn starting_at(now_ms: u64) -> Self {
        Self {
            deadline_ms: now_ms.saturating_add(ROUTE_WORK_BUDGET_MS),
            remaining_calls: MAX_ROUTE_CALLS,
        }
    }

    async fn run<T>(&mut self, future: impl Future<Output = T>) -> Option<T> {
        if self.remaining_calls == 0 {
            return None;
        }
        self.remaining_calls -= 1;
        self.run_before_deadline(future).await
    }

    pub(crate) async fn run_before_deadline<T>(
        &self,
        future: impl Future<Output = T>,
    ) -> Option<T> {
        let remaining_ms = self
            .deadline_ms
            .checked_sub(crate::runtime::now_ms())
            .filter(|remaining| *remaining > 0)?;
        runtime::timeout(Duration::from_millis(remaining_ms), future)
            .await
            .ok()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PairFeeQuotes {
    recv_gateway: GatewayFee,
    send_gateway: GatewayFee,
    /// Upper bounds for the federation step fees over every amount this tick can emit.
    recv_fed: Msat,
    send_fed: Msat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FloorEstimate {
    Routable(Msat),
    UneconomicAtAnySize,
    /// The affine slopes do not prove that a move fits, but integer fee flooring can still make
    /// a finite small amount fit. Leaving the pair missing is safer than a false permanent status.
    Indeterminate,
}

/// Closed-form break-even for an affine upper envelope of the four fee components.
///
/// Gateway ppm terms are relaxed upward; federation step fees are supplied as constants quoted at
/// the maximum affordable contract for this tick. `Routable(floor)` is therefore conservative:
/// the real route fits at `floor` whenever that amount is reachable by this snapshot.
fn min_viable_amount(quotes: PairFeeQuotes, bps: u16) -> FloorEstimate {
    if quotes.recv_gateway.ppm as u128 >= PPM {
        return FloorEstimate::UneconomicAtAnySize;
    }
    if quotes.recv_gateway == zero_gateway_fee()
        && quotes.send_gateway == zero_gateway_fee()
        && quotes.recv_fed.0 == 0
        && quotes.send_fed.0 == 0
    {
        return FloorEstimate::Routable(Msat(0));
    }

    let p = PPM - quotes.recv_gateway.ppm as u128;
    let q = PPM.saturating_add(quotes.send_gateway.ppm as u128);
    let k = quotes
        .recv_fed
        .0
        .saturating_add(quotes.recv_gateway.base_msat.0) as u128;
    let c = quotes
        .send_fed
        .0
        .saturating_add(quotes.send_gateway.base_msat.0) as u128;

    // A safe lower bound can prove the persistent status without trusting the federation-fee
    // upper envelope: each gateway ppm floor is greater than its affine value minus one. At and
    // above the protocol's minimum contract, an adverse combined slope contributes a growing
    // margin; together with the bases it only needs to cover the two one-msat rounding margins.
    // The gateway fees alone are then strictly greater than the cap for every performable size.
    let gateway_bases = quotes
        .recv_gateway
        .base_msat
        .0
        .saturating_add(quotes.send_gateway.base_msat.0) as u128;
    let gateway_slope = (quotes.recv_gateway.ppm as u128)
        .saturating_add(quotes.send_gateway.ppm as u128)
        .saturating_mul(BPS);
    let cap_gateway_slope = (bps as u128).saturating_mul(PPM);
    let common_denominator = PPM.saturating_mul(BPS);
    let lower_bound_margin_at_protocol_floor = gateway_bases
        .saturating_mul(common_denominator)
        .saturating_add(
            (crate::executor::MINIMUM_INCOMING_CONTRACT_MSAT as u128)
                .saturating_mul(gateway_slope.saturating_sub(cap_gateway_slope)),
        );
    if gateway_slope >= cap_gateway_slope
        && lower_bound_margin_at_protocol_floor >= 2u128.saturating_mul(common_denominator)
    {
        return FloorEstimate::UneconomicAtAnySize;
    }

    let cap_slope = p.saturating_mul(BPS.saturating_add(bps as u128));
    let fee_slope = BPS.saturating_mul(q);
    if cap_slope <= fee_slope {
        // A continuous model cannot amortise the fixed cost here, but integer flooring can admit
        // a finite window (for example 149 msat of fee under a 150-msat cap). Do not permanently
        // disable that route on an approximation.
        return FloorEstimate::Indeterminate;
    }

    let denominator = cap_slope - fee_slope;
    // `ceil(invoice)` can add one msat before the send ppm term is applied. Reserve that entire
    // transformed rounding error, plus one msat for the floored proportional cap. This is the
    // margin the former `c + 1` expression missed when send ppm was non-zero.
    let rounding_reserve = 1u128.saturating_add(q.div_ceil(PPM));
    let numerator = BPS.saturating_mul(
        q.saturating_mul(k)
            .saturating_add(p.saturating_mul(c.saturating_add(rounding_reserve))),
    );
    let floor = numerator.div_ceil(denominator);
    FloorEstimate::Routable(Msat(u64::try_from(floor).unwrap_or(u64::MAX)))
}

fn zero_gateway_fee() -> GatewayFee {
    GatewayFee {
        base_msat: Msat(0),
        ppm: 0,
    }
}

/// The upper-envelope fee at one net amount, used only to rank serving gateways. Federation fees
/// are included even though they are gateway-independent, matching the full route-cost contract.
fn modelled_fee(net: Msat, quotes: PairFeeQuotes) -> u128 {
    if quotes.recv_gateway.ppm as u128 >= PPM {
        return u128::MAX;
    }
    let k = quotes
        .recv_fed
        .0
        .saturating_add(quotes.recv_gateway.base_msat.0) as u128;
    let invoice = (net.0 as u128)
        .saturating_add(k)
        .saturating_mul(PPM)
        .div_ceil(PPM - quotes.recv_gateway.ppm as u128);
    let send_gateway = invoice
        .saturating_mul(quotes.send_gateway.ppm as u128)
        .div_ceil(PPM)
        .saturating_add(quotes.send_gateway.base_msat.0 as u128);
    invoice
        .saturating_sub(net.0 as u128)
        .saturating_add(send_gateway)
        .saturating_add(quotes.send_fed.0 as u128)
}

#[derive(Clone, Debug)]
struct GatewayQuotes {
    gateway: GatewayUrl,
    recv: GatewayFee,
    send: GatewayFee,
}

fn select_gateway(
    candidates: Vec<GatewayQuotes>,
    recv_fed: Msat,
    send_fed: Msat,
    reference: Msat,
    bps: u16,
) -> Option<RouteEconomics> {
    let mut routable = Vec::new();
    let mut uneconomic = Vec::new();
    let mut indeterminate = false;
    for candidate in candidates {
        let quotes = PairFeeQuotes {
            recv_gateway: candidate.recv,
            send_gateway: candidate.send,
            recv_fed,
            send_fed,
        };
        let cost = modelled_fee(reference, quotes);
        match min_viable_amount(quotes, bps) {
            FloorEstimate::Routable(floor) => routable.push((cost, candidate.gateway, floor)),
            FloorEstimate::UneconomicAtAnySize => {
                uneconomic.push((cost, candidate.gateway));
            }
            FloorEstimate::Indeterminate => indeterminate = true,
        }
    }

    if let Some((_, gateway, min_viable_amount)) =
        routable.into_iter().min_by_key(|(cost, _, _)| *cost)
    {
        return Some(RouteEconomics {
            resolved_gateway: Some(gateway),
            min_viable_amount,
            status: RouteStatus::Routable,
        });
    }
    if indeterminate {
        return None;
    }
    let (_, gateway) = uneconomic.into_iter().min_by_key(|(cost, _)| *cost)?;
    Some(RouteEconomics {
        resolved_gateway: Some(gateway),
        min_viable_amount: Msat(0),
        status: RouteStatus::UneconomicAtAnySize,
    })
}

fn no_serving_route(candidate_count: usize) -> Option<RouteEconomics> {
    // An empty registry is not proof that no route exists: devimint does not auto-register its
    // usable LDK gateway. Leave the pair missing so the allocator takes the transient,
    // permissive fallback and the executor can surface its existing "pass one explicitly"
    // diagnostic. A listed candidate (including an operator pin) that fails both-end validation
    // is positive evidence that the pair is currently unroutable.
    (candidate_count > 0).then_some(RouteEconomics {
        resolved_gateway: None,
        min_viable_amount: Msat(0),
        status: RouteStatus::Unroutable,
    })
}

pub(crate) async fn price_missing_pairs(
    mc: &MultiClient,
    pinned_gateway: Option<&GatewayUrl>,
    snapshot: &AllocatorSnapshot,
    budget: &mut RouteQuoteBudget,
    priced: &mut BTreeMap<(FederationId, FederationId), RouteEconomics>,
) {
    for (from, to) in funding_pairs(snapshot) {
        if priced.contains_key(&(from, to)) {
            continue;
        }
        if let Some(economics) =
            pair_economics(mc, pinned_gateway, snapshot, from, to, budget).await
        {
            if economics.status == RouteStatus::UneconomicAtAnySize {
                tracing::warn!(
                    from = %from.to_hex(),
                    to = %to.to_hex(),
                    gateway = %economics
                        .resolved_gateway
                        .as_ref()
                        .map_or("<none>", |gateway| gateway.0.as_str()),
                    max_fee_bps_of_move = snapshot.max_fee_bps_of_move,
                    "route economics: no move size is proven to fit the proportional fee cap; \
                     rebalancing across this pair is disabled until the cap or route changes"
                );
            }
            priced.insert((from, to), economics);
        }
    }
}

async fn pair_economics(
    mc: &MultiClient,
    pinned_gateway: Option<&GatewayUrl>,
    snapshot: &AllocatorSnapshot,
    from: FederationId,
    to: FederationId,
    budget: &mut RouteQuoteBudget,
) -> Option<RouteEconomics> {
    let maximum = maximum_funding_amount(snapshot, from, to);
    if maximum.0 == 0 {
        return None;
    }
    let candidates = match pinned_gateway {
        Some(gateway) => vec![gateway.clone()],
        None => {
            let gateways = budget.run(mc.gateways(&to)).await?.ok()?;
            if gateways.len() > MAX_ROUTE_GATEWAYS {
                tracing::debug!(
                    federation = %to.to_hex(),
                    gateways = gateways.len(),
                    "route economics: gateway list exceeds the per-tick quote cap; leaving pair unpriced"
                );
                return None;
            }
            gateways
        }
    };
    let candidate_count = candidates.len();

    let mut serving = Vec::new();
    for gateway in candidates {
        let recv = match budget
            .run(mc.maybe_receive_gateway_fee(&to, &gateway))
            .await?
        {
            Ok(Some(fee)) => fee,
            Ok(None) => continue,
            Err(_) => return None,
        };
        let send = match budget
            .run(mc.maybe_direct_swap_send_gateway_fee(&from, &gateway))
            .await?
        {
            Ok(Some(fee)) => fee,
            Ok(None) => continue,
            Err(_) => return None,
        };
        serving.push(GatewayQuotes {
            gateway,
            recv,
            send,
        });
    }
    if serving.is_empty() {
        return no_serving_route(candidate_count);
    }

    // Both federation fee functions are non-decreasing (affine `base + ppm·contract`), so sampling
    // at the largest contract any performable move can reach — `maximum + fee_cap`, the grossed-up
    // outgoing contract that delivers `maximum` net — upper bounds every fee the snapshot can
    // perform. `recv_fed`/`send_fed` enter the model as the fixed cost `k`/`c`.
    //
    // The RECEIVE quote is a pure fee computation, so sample it at that whole ceiling directly. The
    // SEND quote is NOT: `send_fee_quote_for_amount` note-selects real inventory to fund the
    // outgoing contract (SDK `fee_quote` → `create_final_inputs_and_outputs`). For a pair NOT bound
    // by source budget the ceiling is comfortably fundable and the quote succeeds — the strict upper
    // bound. Only a source-CONSTRAINED pair (`maximum` ≈ the whole budget, with no fed-fee headroom
    // above the move plus its proportional reserve) fails that dry-run with `InsufficientBalance`;
    // the pre-fix code then dropped the pair to the permissive `min_move` fallback (NO floor) for
    // exactly the tight standby it most needs to gate. There, fall back to sampling at the fundable
    // `maximum`: a strict upper bound for every send contract `<= maximum`, under-shooting the affine
    // fee over the reserve tail `(maximum, maximum + fee_cap]` by at most `fee_cap · ppm/1e6` — a
    // second-order term whose only effect is that a marginally-admitted move may fail the
    // perform-time fee-cap recheck (`keep_cheapest_fitting`) and retry: churn for one tick, never an
    // over-cap spend or over-credit. This preserves the strict bound wherever it can be quoted and
    // still gates the source-constrained pairs the pre-fix permissive drop left ungated.
    let sample = federation_quote_sample(maximum, snapshot.max_fee_bps_of_move);
    let recv_fed = budget.run(mc.receive_fee_quote(&to, sample)).await?.ok()?;
    let send_fed = match budget
        .run(mc.send_fee_quote_for_amount(&from, sample))
        .await?
    {
        Ok(fee) => fee,
        Err(_) => budget
            .run(mc.send_fee_quote_for_amount(&from, maximum))
            .await?
            .ok()?,
    };
    select_gateway(
        serving,
        recv_fed,
        send_fed,
        maximum,
        snapshot.max_fee_bps_of_move,
    )
}

fn federation_quote_sample(maximum: Msat, max_fee_bps_of_move: u16) -> Msat {
    Msat(
        maximum
            .0
            .saturating_add(wallet_core::move_fee_cap(maximum, max_fee_bps_of_move).0),
    )
}

fn funding_pairs(snapshot: &AllocatorSnapshot) -> Vec<(FederationId, FederationId)> {
    match (snapshot.spending_fed, snapshot.standby_fed) {
        (Some(spending), Some(standby)) if spending != standby => {
            vec![(standby, spending), (spending, standby)]
        }
        _ => Vec::new(),
    }
}

fn maximum_funding_amount(
    snapshot: &AllocatorSnapshot,
    from: FederationId,
    to: FederationId,
) -> Msat {
    let source = snapshot.federations.iter().find(|fed| fed.id == from);
    let destination = snapshot.federations.iter().find(|fed| fed.id == to);
    let (Some(source), Some(destination)) = (source, destination) else {
        return Msat(0);
    };
    let (want, protected) =
        if snapshot.spending_fed == Some(to) && snapshot.standby_fed == Some(from) {
            (
                snapshot
                    .target_spending_balance
                    .0
                    .saturating_sub(destination.balance.spendable.0),
                0,
            )
        } else if snapshot.standby_fed == Some(to) && snapshot.spending_fed == Some(from) {
            (
                snapshot
                    .standby_target
                    .0
                    .saturating_sub(destination.balance.spendable.0),
                snapshot.target_spending_balance.0,
            )
        } else {
            return Msat(0);
        };
    let budget = source
        .balance
        .spendable
        .0
        .saturating_sub(protected)
        .saturating_sub(snapshot.reservations.outbound(from).0);
    let available = wallet_core::max_fundable(budget, snapshot.max_fee_bps_of_move);
    let cap_room = snapshot
        .per_fed_cap
        .0
        .saturating_sub(destination.balance.spendable.0)
        .saturating_sub(snapshot.reservations.inbound(to).0);
    Msat(want.min(available).min(cap_room))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fee;

    fn gateway(base: u64, ppm: u64) -> GatewayFee {
        GatewayFee {
            base_msat: Msat(base),
            ppm,
        }
    }

    fn quotes(recv_ppm: u64, send_ppm: u64, recv_fed: u64, send_fed: u64) -> PairFeeQuotes {
        PairFeeQuotes {
            recv_gateway: gateway(7, recv_ppm),
            send_gateway: gateway(11, send_ppm),
            recv_fed: Msat(recv_fed),
            send_fed: Msat(send_fed),
        }
    }

    fn true_fee(net: u64, quotes: PairFeeQuotes) -> u64 {
        let gross = fee::gross_up(Msat(net), quotes.recv_gateway, |_| quotes.recv_fed)
            .expect("test quotes are solvable");
        gross
            .receive_quote
            .0
            .saturating_add(quotes.send_gateway.on(gross.invoice_amount).0)
            .saturating_add(quotes.send_fed.0)
    }

    fn cap(net: u64, bps: u16) -> u64 {
        (net as u128 * bps as u128 / BPS) as u64
    }

    fn routable_floor(quotes: PairFeeQuotes, bps: u16) -> u64 {
        let FloorEstimate::Routable(floor) = min_viable_amount(quotes, bps) else {
            panic!("fixture must have a favorable slope")
        };
        floor.0
    }

    #[test]
    fn floor_never_under_estimates_the_true_break_even() {
        let fixtures = [
            (quotes(0, 0, 3, 5), 300),
            (quotes(100, 200, 17, 29), 300),
            (quotes(1_500, 2_000, 101, 211), 500),
            (quotes(9_999, 12_345, 777, 999), 1_000),
        ];
        for (quotes, bps) in fixtures {
            let floor = routable_floor(quotes, bps);
            assert!(
                true_fee(floor, quotes) <= cap(floor, bps),
                "{quotes:?}: floor {floor} does not fit"
            );
            for amount in [floor, floor.saturating_add(1), floor.saturating_mul(2)] {
                assert!(
                    true_fee(amount, quotes) <= cap(amount, bps),
                    "{quotes:?}: amount {amount} above the floor does not fit"
                );
            }
        }
    }

    #[test]
    fn floor_is_monotone_in_every_fee_component() {
        let base = PairFeeQuotes {
            recv_gateway: gateway(3, 100),
            send_gateway: gateway(5, 200),
            recv_fed: Msat(7),
            send_fed: Msat(11),
        };
        let baseline = routable_floor(base, 300);
        for dearer in [
            PairFeeQuotes {
                recv_gateway: gateway(4, 100),
                ..base
            },
            PairFeeQuotes {
                recv_gateway: gateway(3, 101),
                ..base
            },
            PairFeeQuotes {
                send_gateway: gateway(6, 200),
                ..base
            },
            PairFeeQuotes {
                send_gateway: gateway(5, 201),
                ..base
            },
            PairFeeQuotes {
                recv_fed: Msat(8),
                ..base
            },
            PairFeeQuotes {
                send_fed: Msat(12),
                ..base
            },
        ] {
            assert!(routable_floor(dearer, 300) >= baseline);
        }
    }

    #[test]
    fn adverse_slope_boundary_is_indeterminate_not_permanently_uneconomic() {
        let quotes = PairFeeQuotes {
            recv_gateway: gateway(0, 100),
            send_gateway: gateway(0, 29_900),
            recv_fed: Msat(0),
            send_fed: Msat(0),
        };
        assert_eq!(min_viable_amount(quotes, 300), FloorEstimate::Indeterminate);
        assert_eq!(true_fee(5_000, quotes), 149);
        assert_eq!(cap(5_000, 300), 150);
        assert_eq!(
            select_gateway(
                vec![GatewayQuotes {
                    gateway: GatewayUrl("https://boundary.example".into()),
                    recv: quotes.recv_gateway,
                    send: quotes.send_gateway,
                }],
                quotes.recv_fed,
                quotes.send_fed,
                Msat(5_000),
                300,
            ),
            None,
            "an indeterminate route stays missing instead of becoming a permanent status"
        );
    }

    #[test]
    fn federation_fee_sample_covers_large_funding_amounts_and_their_cap() {
        assert_eq!(
            federation_quote_sample(Msat(5_000_000), 300),
            Msat(5_150_000)
        );
    }

    #[tokio::test]
    async fn preflight_deadline_does_not_consume_the_quote_call_cap() {
        let budget = RouteQuoteBudget {
            deadline_ms: 0,
            remaining_calls: 7,
        };
        assert_eq!(budget.run_before_deadline(async { 1 }).await, None);
        assert_eq!(budget.remaining_calls, 7);
    }

    #[test]
    fn an_unsolvable_receive_gateway_is_uneconomic_at_every_size() {
        assert_eq!(
            min_viable_amount(quotes(1_000_000, 0, 0, 0), 10_000),
            FloorEstimate::UneconomicAtAnySize
        );
    }

    #[test]
    fn gateway_lower_bound_proves_a_persistently_uneconomic_route() {
        let quotes = PairFeeQuotes {
            recv_gateway: gateway(1, 10_000),
            send_gateway: gateway(1, 20_000),
            recv_fed: Msat(0),
            send_fed: Msat(0),
        };
        assert_eq!(
            min_viable_amount(quotes, 300),
            FloorEstimate::UneconomicAtAnySize
        );
    }

    #[test]
    fn proportional_over_cap_route_is_uneconomic_with_zero_base_fees() {
        let quotes = PairFeeQuotes {
            recv_gateway: gateway(0, 0),
            send_gateway: gateway(0, 40_000),
            recv_fed: Msat(0),
            send_fed: Msat(0),
        };
        assert_eq!(
            min_viable_amount(quotes, 300),
            FloorEstimate::UneconomicAtAnySize
        );
    }

    #[test]
    fn an_empty_registry_is_missing_not_definitively_unroutable() {
        assert_eq!(no_serving_route(0), None);
        assert_eq!(
            no_serving_route(1).map(|route| route.status),
            Some(RouteStatus::Unroutable)
        );
    }

    #[test]
    fn cheapest_serving_gateway_uses_both_legs_combined() {
        let selected = select_gateway(
            vec![
                GatewayQuotes {
                    gateway: GatewayUrl("https://first.example".into()),
                    recv: gateway(1, 10_000),
                    send: gateway(1, 10_000),
                },
                GatewayQuotes {
                    gateway: GatewayUrl("https://cheap.example".into()),
                    recv: gateway(50, 100),
                    send: gateway(50, 100),
                },
            ],
            Msat(3),
            Msat(5),
            Msat(1_000_000),
            300,
        )
        .expect("a route is selected");
        assert_eq!(
            selected.resolved_gateway,
            Some(GatewayUrl("https://cheap.example".into()))
        );
        assert_eq!(selected.status, RouteStatus::Routable);
    }

    #[test]
    fn a_proven_routable_gateway_beats_a_cheaper_uneconomic_candidate() {
        let selected = select_gateway(
            vec![
                GatewayQuotes {
                    gateway: GatewayUrl("https://cheap-but-uneconomic.example".into()),
                    recv: gateway(1, 40_000),
                    send: gateway(1, 0),
                },
                GatewayQuotes {
                    gateway: GatewayUrl("https://viable.example".into()),
                    recv: gateway(1_000, 0),
                    send: gateway(0, 0),
                },
            ],
            Msat(0),
            Msat(0),
            Msat(10_000),
            300,
        )
        .expect("the pair has a proven routable candidate");
        assert_eq!(
            selected.resolved_gateway,
            Some(GatewayUrl("https://viable.example".into()))
        );
        assert_eq!(selected.status, RouteStatus::Routable);
    }

    #[test]
    fn an_indeterminate_candidate_prevents_a_false_pair_uneconomic_status() {
        assert_eq!(
            select_gateway(
                vec![
                    GatewayQuotes {
                        gateway: GatewayUrl("https://boundary.example".into()),
                        recv: gateway(0, 100),
                        send: gateway(0, 29_900),
                    },
                    GatewayQuotes {
                        gateway: GatewayUrl("https://proven-uneconomic.example".into()),
                        recv: gateway(1, 40_000),
                        send: gateway(1, 0),
                    },
                ],
                Msat(0),
                Msat(0),
                Msat(5_000),
                300,
            ),
            None
        );
    }
}
