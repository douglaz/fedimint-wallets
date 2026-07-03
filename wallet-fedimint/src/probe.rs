//! The "sense" layer (Phase 2 step 2.1): turn each OPEN federation into the real
//! inputs the pure `wallet_core::scorer::score` / `wallet_core::allocator::decide`
//! consume (`docs/phase2-plan.md`).
//!
//! Split deliberately in two:
//! - a PURE assembler ([`assemble_facts`] / [`assemble_status`]) that maps a raw,
//!   serde-able [`ProbeResult`] into a `FederationFacts` / `FederationStatus`. No
//!   fedimint types, no I/O, no async — golden-tested from recorded fixtures, so it
//!   is in the fast rb-lite gate.
//! - an I/O [`FedimintProbeRunner`] that produces a [`ProbeResult`] from a live
//!   `MultiClient` client handle.
//!
//! **Probes are LIGHT — NO sats spent (decision).** Structural facts come from the
//! authenticated `ClientConfig` and are FREE (ADR-0019). The empirical fields use
//! light reachability + capability proxies (a timed threshold consensus read, a
//! registered gateway that answers `routing_info`, the presence of the wallet
//! module), NEVER a real receive/pay round-trip. That is why the two scorer inputs
//! that would normally
//! require an active probe — `round_trip_ok` and `peg_out_quotable` — are filled here
//! from no-sats proxies, each documented at its assignment.
//!
//! The runner is I/O: it is validated live on devimint by the maintainer, NOT by the
//! rb-lite gate. Every fedimint API it touches is verified against the pinned source
//! (`douglaz/fedimint` @ `b108ec6`).

use crate::multi_client::MultiClient;
use crate::types::GatewayUrl;
use fedimint_client::db::ChronologicalOperationLogKey;
use fedimint_client::ClientHandleArc;
use fedimint_core::NumPeers;
use fedimint_lnv2_client::common::LightningInvoice;
use fedimint_lnv2_client::LightningOperationMeta;
use fedimint_wallet_client::WalletClientModule;
use std::sync::Arc;
use std::time::Instant;
use wallet_core::{FedBalance, FederationFacts, FederationId, FederationStatus, Module, Msat};

/// The raw result of probing ONE federation: a plain, serde-able data struct with no
/// fedimint types, so it can be recorded as a golden fixture and fed to the pure
/// assembler without a live client.
///
/// Fields group as: structural facts (free, from the authed `ClientConfig`), light
/// empirical signals (no sats), lifecycle, and balance (msat).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProbeResult {
    // ---- Structural facts from the authenticated config (ADR-0019, FREE) ----
    /// Number of guardians = the authed config's api-endpoint count.
    pub guardian_count: u32,
    /// Consensus threshold (`2f+1`) derived from the guardian set.
    pub threshold: u32,
    /// Whether the wallet module's bitcoin network is mainnet.
    pub is_mainnet: bool,
    /// Every consensus module kind present in the config (e.g. `mint`, `wallet`,
    /// `ln`, `lnv2`), verbatim strings so the pure assembler owns the mapping.
    pub module_kinds: Vec<String>,
    /// Whether an `lnv2` module is present (its own field because a fed with no
    /// LNv2 cannot send/receive at all — it gates eligibility unconditionally).
    pub has_lnv2: bool,

    // ---- Light empirical signals (the trust gate, ADR-0017 — NO sats spent) ----
    /// A threshold consensus read answered: quorum is live.
    pub quorum_live: bool,
    /// Wall-clock latency (ms) of that threshold read.
    pub latency_ms: u32,
    /// The federation has a usable lnv2 gateway route (the no-sats proxy for
    /// "can route a receive/pay"). If the caller pinned a gateway, this means that
    /// exact gateway validates for the federation; otherwise it means the federation's
    /// first registered gateway validates, matching the executor's default selection.
    pub gateway_available: bool,
    /// The wallet (on-chain peg-out) module is present in the authed config (the
    /// no-sats proxy for "peg-out is quotable").
    pub wallet_module_present: bool,

    // ---- Lifecycle ----
    /// The federation is winding down (do not fund it).
    pub shutdown_scheduled: bool,

    // ---- Balance (msat) ----
    /// Spendable ecash balance.
    pub spendable_msat: u64,
    /// Value committed to in-flight (pending) outgoing sends.
    pub in_flight_msat: u64,
    /// Value of pending incoming receives not yet claimed.
    pub claimable_msat: u64,
}

/// Map a raw [`ProbeResult`] into the scorer's `FederationFacts`. PURE: no I/O, no
/// async, total over the input. Every `FederationFacts` field is filled here.
pub fn assemble_facts(p: &ProbeResult, id: FederationId) -> FederationFacts {
    FederationFacts {
        id,
        // Structural facts map straight through (they came from the authed config).
        guardian_count: p.guardian_count,
        threshold: p.threshold,
        is_mainnet: p.is_mainnet,
        modules: p.module_kinds.iter().map(|k| module_from_kind(k)).collect(),
        // Own empirical probe: quorum answered a threshold consensus read.
        quorum_live: p.quorum_live,
        // PROXY (no sats): a usable lnv2 gateway route is the stand-in for "a
        // receive/pay round-trip would succeed". We never perform the real
        // round-trip (decision: light probes), so gateway availability is what the
        // probe gate reads for `round_trip_ok`. This fails CLOSED — no usable route
        // reads as not-routable, the safe direction for a trust gate.
        round_trip_ok: p.gateway_available,
        // PROXY (no sats): the wallet module (the on-chain peg-out path) being
        // present in the authed config is the stand-in for "a peg-out is quotable".
        // We do not fetch a live peg-out quote here.
        peg_out_quotable: p.wallet_module_present,
        latency_ms: p.latency_ms,
        shutdown_scheduled: p.shutdown_scheduled,
        has_lnv2: p.has_lnv2,
        // The Fedimint Observer is an UNTRUSTED prior wired only in Phase 3
        // (ADR-0020). The sense layer never fabricates it, so it is always `None`
        // here — a missing prior is never a rejection, it just adds no rank bonus.
        observer: None,
    }
}

/// Map a raw [`ProbeResult`] into the allocator's `FederationStatus`. PURE: no I/O,
/// no async, total over the input. Every `FederationStatus` field is filled here.
pub fn assemble_status(p: &ProbeResult, id: FederationId) -> FederationStatus {
    FederationStatus {
        id,
        balance: FedBalance {
            spendable: Msat(p.spendable_msat),
            in_flight: Msat(p.in_flight_msat),
            claimable: Msat(p.claimable_msat),
            // `reserved_fee` is set by the allocator/executor when it PLANS a move,
            // not sensed from the federation — a fresh probe reserves nothing.
            reserved_fee: Msat(0),
        },
        // `probed_ok` = BOTH liveness (quorum answered) AND a usable route (the
        // pinned gateway, if supplied, or the executor-default first registered
        // gateway answers `routing_info`): the two no-sats empirical signals the allocator's
        // receive-gating reads before it directs an inflow into a federation.
        probed_ok: p.quorum_live && p.gateway_available,
        // Reputation comes from the Phase-3 Observer; the sense layer is neutral.
        reputation: 0,
        shutdown_notice: p.shutdown_scheduled,
        // `healthy` tracks quorum liveness; `shutdown_notice` (above) drives the
        // separate evacuation path in the allocator.
        healthy: p.quorum_live,
    }
}

/// Map a fedimint module-kind string to the scorer's `Module`. The kind strings are
/// the module `KIND` constants verified against the pinned source
/// (`modules/*-common/src/lib.rs`): `mint`, `wallet`, `ln`, `lnv2`, `meta`.
fn module_from_kind(kind: &str) -> Module {
    match kind {
        "mint" => Module::Mint,
        "ln" => Module::Ln,
        "lnv2" => Module::Lnv2,
        "wallet" => Module::Wallet,
        "meta" => Module::Meta,
        _ => Module::Other,
    }
}

/// Probes each open federation into a [`ProbeResult`] over a shared [`MultiClient`].
/// I/O — validated live on devimint, NOT in the rb-lite gate.
pub struct FedimintProbeRunner {
    mc: Arc<MultiClient>,
    pinned_gateway: Option<GatewayUrl>,
}

/// Op-log page size for the pending-balance scan. Paging runs to exhaustion, so this
/// only trades round-trips against per-page memory; it is not a coverage cap.
const PROBE_OPLOG_PAGE_SIZE: usize = 100;

impl FedimintProbeRunner {
    pub fn new(mc: Arc<MultiClient>) -> Self {
        Self::with_pinned_gateway(mc, None)
    }

    pub fn with_pinned_gateway(mc: Arc<MultiClient>, pinned_gateway: Option<GatewayUrl>) -> Self {
        Self { mc, pinned_gateway }
    }

    /// Probe one open federation. LIGHT — NO sats spent: structural facts from the
    /// authenticated `ClientConfig` (free), empirical fields from light reachability
    /// + capability proxies, never a real receive/pay round-trip.
    pub async fn probe(&self, id: &FederationId) -> anyhow::Result<ProbeResult> {
        let client = self.mc.client(id)?;

        // ---- Structural facts: FREE, from the authenticated config (ADR-0019) ----
        let config = client.config().await;

        // Guardian count + threshold. The guardian set IS the api-endpoint set;
        // `NumPeers::threshold()` is `total - max_evil` = `2f+1` (verified in
        // fedimint-core/src/peer_id.rs).
        let num_endpoints = config.global.api_endpoints.len();
        let guardian_count = num_endpoints as u32;
        let threshold = NumPeers::from(num_endpoints).threshold() as u32;

        // Every module kind, verbatim; the pure assembler owns the string→`Module`
        // mapping. `has_lnv2` / `wallet_module_present` are derived here against the
        // pinned `KIND` constants so the runner, not the assembler, knows the truth.
        let module_kinds: Vec<String> = config
            .modules
            .values()
            .map(|m| m.kind.as_str().to_string())
            .collect();
        let has_lnv2 = module_kinds
            .iter()
            .any(|k| k == fedimint_lnv2_client::common::KIND.as_str());
        let wallet_module_present = module_kinds
            .iter()
            .any(|k| k == fedimint_wallet_client::common::KIND.as_str());

        // network → is_mainnet, read from the wallet module's authed config. Only
        // meaningful when the wallet module is present; a fed without it can never be
        // mainnet-gated anyway (it fails the scorer's required-modules floor), so
        // `false` is the correct conservative answer there.
        let is_mainnet = if wallet_module_present {
            client
                .get_first_module::<WalletClientModule>()?
                .get_network()
                == bitcoin::Network::Bitcoin
        } else {
            false
        };

        // ---- Light empirical signals: NO sats spent (ADR-0017) ----

        // `quorum_live` + `latency_ms`: a single THRESHOLD consensus read
        // (`session_count` requests current consensus, needing a threshold of
        // guardians to agree — verified in fedimint-api-client). Success ⇒ quorum is
        // live; the wall-clock is the observed latency. No money moves.
        let started = Instant::now();
        let quorum_live = client.api().session_count().await.is_ok();
        let latency_ms = u32::try_from(started.elapsed().as_millis()).unwrap_or(u32::MAX);

        // `gateway_available`: a pinned lnv2 gateway, if supplied, must answer
        // `routing_info` for this federation; otherwise the executor-default first registered
        // lnv2 gateway must answer. NOTE (runbook §4): devimint does NOT auto-register its
        // LDK gateway, so the live tick passes that gateway URL directly. As the
        // `round_trip_ok` proxy this fails CLOSED: a missing lnv2 module, empty list,
        // stale/unreachable gateway, invalid pinned gateway, or unreachable gateway
        // registry all read as not-routable, producing a scorable `ProbeResult`
        // instead of hiding a dead federation behind `Err`.
        let gateway_available = self.gateway_available(id, has_lnv2).await;

        // ---- Balance (msat) ----
        // `?` here (and on `get_first_module` for `is_mainnet` above) is intentional and
        // NOT the gateway read's degrade-to-`false`: `balance`/`config`/module reads are
        // LOCAL (the client's own db), so a failure means the fed genuinely can't be
        // sensed — Err is the honest result, and the caller skips it like `open_all`
        // does. The gateway read degrades to `false` instead because it is a NETWORK
        // capability signal whose absence is itself a meaningful (fail-closed) fact.
        let spendable_msat = self.mc.balance(id).await?.0;
        // `in_flight` / `claimable` are a best-effort scan of the op-log for pending
        // (no cached final outcome) lnv2 legs. Light: a local db read, no network,
        // no sats. The allocator decides on `spendable` today; these fields exist so
        // the model can later account for pending value without a balance rewrite.
        let (in_flight_msat, claimable_msat) = pending_lnv2_balances(&client).await;

        // `shutdown_scheduled`: the pinned config exposes no non-admin shutdown-
        // notice signal (only an auth-gated `shutdown` endpoint), so v1 reports
        // `false`. Real shutdown detection + `Evacuate` is Phase 3 per the plan.
        let shutdown_scheduled = false;

        Ok(ProbeResult {
            guardian_count,
            threshold,
            is_mainnet,
            module_kinds,
            has_lnv2,
            quorum_live,
            latency_ms,
            gateway_available,
            wallet_module_present,
            shutdown_scheduled,
            spendable_msat,
            in_flight_msat,
            claimable_msat,
        })
    }

    async fn gateway_available(&self, id: &FederationId, has_lnv2: bool) -> bool {
        if !has_lnv2 {
            return false;
        }

        if let Some(gateway) = &self.pinned_gateway {
            return match self.mc.validate_gateway(id, gateway).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(
                        federation = %id.to_hex(),
                        gateway = %gateway.0,
                        error = ?e,
                        "probe: pinned gateway failed routing-info validation"
                    );
                    false
                }
            };
        }

        match self.mc.gateways(id).await {
            Ok(gateways) => {
                let Some(gateway) = gateways.into_iter().next() else {
                    return false;
                };
                match self.mc.validate_gateway(id, &gateway).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(
                            federation = %id.to_hex(),
                            gateway = %gateway.0,
                            error = ?e,
                            "probe: default registered gateway failed routing-info validation"
                        );
                        false
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    federation = %id.to_hex(),
                    error = ?e,
                    "probe: treating gateway registry read failure as unavailable"
                );
                false
            }
        }
    }
}

/// Sum the pending (unsettled) lnv2 OUTGOING value from `client`'s op-log, returning
/// `(in_flight_msat, claimable_msat)`. Best-effort and light: a local db read only (no
/// network, no sats). An op with a cached final outcome has settled and is skipped; an
/// unsettled Send contributes its invoice amount to `in_flight` (funding a send commits
/// those funds out of `spendable` as soon as the send op exists). Malformed / non-lnv2
/// ops are skipped.
///
/// `claimable` is deliberately always `0`: a receive op exists from the moment its
/// invoice is CREATED — before any payment arrives — and the op-log caches only the
/// TERMINAL outcome, so a light read cannot tell an unpaid open invoice from a paid-
/// but-unclaimed contract. Counting every open receive would report money that has not
/// arrived as claimable — the unsafe over-report direction for a balance field — so we
/// report `0` until the paid-but-unclaimed state is available (that needs the receive
/// state machine's update stream; Phase 3).
///
/// Because outcomes are cached only once an op's update stream reaches a terminal
/// state, a freshly-reopened client can transiently OVER-report `in_flight` (a send that
/// already settled but whose outcome has not been re-cached still looks unsettled) until
/// it re-subscribes; this is why the value is advisory (not consumed by `decide` yet)
/// and the whole runner is validated live.
async fn pending_lnv2_balances(client: &ClientHandleArc) -> (u64, u64) {
    let log = client.operation_log();
    let mut last_seen: Option<ChronologicalOperationLogKey> = None;
    let mut in_flight = 0u64;
    // Always 0 — a light op-log read cannot confirm a receive was paid (see fn doc).
    let claimable = 0u64;
    loop {
        let page = log
            .paginate_operations_rev(PROBE_OPLOG_PAGE_SIZE, last_seen)
            .await;
        let page_len = page.len();
        if let Some((key, _)) = page.last() {
            last_seen = Some(*key);
        }
        for (_key, entry) in page {
            // Only lnv2 ops carry a receive/send lightning leg; skip the rest.
            if entry.operation_module_kind() != fedimint_lnv2_client::common::KIND.as_str() {
                continue;
            }
            // A cached final outcome means the op already settled (claimed / paid /
            // refunded / expired); only still-pending ops are in-flight/claimable.
            // Decoding to `Value` cannot fail, so this never panics.
            if entry.outcome::<serde_json::Value>().is_some() {
                continue;
            }
            let Ok(meta) = entry.try_meta::<LightningOperationMeta>() else {
                continue;
            };
            match meta {
                LightningOperationMeta::Send(send) => {
                    in_flight = in_flight.saturating_add(invoice_msat(&send.invoice));
                }
                // An open receive is an invoice created BEFORE any payment arrives; a
                // light op-log read can't prove it was paid (see fn doc), so it never
                // contributes to `claimable` — counting it would fabricate arrived funds.
                LightningOperationMeta::Receive(_) => {}
                // A gateway-minted LNURL receive is not part of our move protocol.
                LightningOperationMeta::LnurlReceive(_) => {}
            }
        }
        // A short (or empty) page is the last (`paginate_operations_rev` returns up
        // to `limit` newest-first), so fewer than `limit` means the log is exhausted.
        if page_len < PROBE_OPLOG_PAGE_SIZE {
            break;
        }
    }
    (in_flight, claimable)
}

/// The msat amount of an lnv2 leg's BOLT11 invoice, or `0` for an amountless invoice.
fn invoice_msat(invoice: &LightningInvoice) -> u64 {
    let LightningInvoice::Bolt11(bolt11) = invoice;
    bolt11.amount_milli_satoshis().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wallet_core::scorer::ReasonCode as ScorerReason;
    use wallet_core::types::ReasonCode as AllocReason;
    use wallet_core::{decide, score, Action, AllocatorSnapshot, Occurrence, ScorerPolicy};

    // ---- Recorded fixtures -------------------------------------------------------
    //
    // A healthy fed's probe, serialized as JSON exactly as the runner would emit it.
    // Decoding this string is the "recorded fixture" path: it proves `ProbeResult` is
    // serde-able and drives the pure assembler without a live client.
    const HEALTHY_PROBE_JSON: &str = r#"{
        "guardian_count": 4,
        "threshold": 3,
        "is_mainnet": true,
        "module_kinds": ["mint", "wallet", "ln", "lnv2"],
        "has_lnv2": true,
        "quorum_live": true,
        "latency_ms": 42,
        "gateway_available": true,
        "wallet_module_present": true,
        "shutdown_scheduled": false,
        "spendable_msat": 1000000,
        "in_flight_msat": 5000,
        "claimable_msat": 7000
    }"#;

    fn healthy_probe() -> ProbeResult {
        serde_json::from_str(HEALTHY_PROBE_JSON).expect("healthy fixture is valid ProbeResult JSON")
    }

    fn fed_id(byte: u8) -> FederationId {
        FederationId([byte; 32])
    }

    #[test]
    fn probe_result_json_round_trips() {
        let probe = healthy_probe();
        let json = serde_json::to_string(&probe).expect("serialize");
        let back: ProbeResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(probe, back);
    }

    #[test]
    fn healthy_fed_is_eligible_via_score() {
        let facts = assemble_facts(&healthy_probe(), fed_id(1));
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(
            verdict.eligible_to_fund,
            "a 4-guardian mainnet fed with mint+wallet+lnv2, live quorum and a gateway \
             must be fundable; reasons: {:?}",
            verdict.reasons
        );
        // Rank is only meaningful when eligible, and must be non-zero here.
        assert!(verdict.rank_score > 0);
    }

    #[test]
    fn structural_facts_map_straight_through() {
        let probe = healthy_probe();
        let facts = assemble_facts(&probe, fed_id(1));
        assert_eq!(facts.id, fed_id(1));
        assert_eq!(facts.guardian_count, 4);
        assert_eq!(facts.threshold, 3);
        assert!(facts.is_mainnet);
        assert!(facts.has_lnv2);
        assert_eq!(
            facts.modules,
            vec![Module::Mint, Module::Wallet, Module::Ln, Module::Lnv2]
        );
        // The documented no-sats proxies.
        assert_eq!(facts.round_trip_ok, probe.gateway_available);
        assert_eq!(facts.peg_out_quotable, probe.wallet_module_present);
        // The Observer prior is never sensed here (Phase 3).
        assert!(facts.observer.is_none());
    }

    #[test]
    fn no_lnv2_fed_is_ineligible() {
        let mut probe = healthy_probe();
        probe.has_lnv2 = false;
        probe.module_kinds = vec!["mint".into(), "wallet".into(), "ln".into()];
        let facts = assemble_facts(&probe, fed_id(2));
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(!verdict.eligible_to_fund);
        assert!(
            verdict.reasons.contains(&ScorerReason::NoLnv2),
            "reasons: {:?}",
            verdict.reasons
        );
    }

    #[test]
    fn shutdown_scheduled_fed_is_unfundable_and_flags_status() {
        let mut probe = healthy_probe();
        probe.shutdown_scheduled = true;
        let facts = assemble_facts(&probe, fed_id(3));
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(!verdict.eligible_to_fund);
        assert!(verdict.reasons.contains(&ScorerReason::ShutdownScheduled));

        let status = assemble_status(&probe, fed_id(3));
        assert!(status.shutdown_notice);
        // A shutdown notice does not, by itself, mean quorum is dead.
        assert!(status.healthy);
    }

    #[test]
    fn quorum_dead_fed_is_not_probed_ok_and_unhealthy() {
        let mut probe = healthy_probe();
        probe.quorum_live = false;
        let status = assemble_status(&probe, fed_id(4));
        assert!(!status.probed_ok);
        assert!(!status.healthy);

        // And the scorer's probe gate rejects it.
        let facts = assemble_facts(&probe, fed_id(4));
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(!verdict.eligible_to_fund);
        assert!(verdict.reasons.contains(&ScorerReason::ProbeFailed));
    }

    #[test]
    fn no_gateway_fails_probe_gate_and_status() {
        // The `round_trip_ok` / `probed_ok` gateway proxy fails CLOSED.
        let mut probe = healthy_probe();
        probe.gateway_available = false;
        let facts = assemble_facts(&probe, fed_id(5));
        assert!(!facts.round_trip_ok);
        let verdict = score(&facts, &ScorerPolicy::default());
        assert!(!verdict.eligible_to_fund);
        assert!(verdict.reasons.contains(&ScorerReason::ProbeFailed));

        let status = assemble_status(&probe, fed_id(5));
        assert!(!status.probed_ok);
    }

    #[test]
    fn balances_map_into_fed_balance() {
        let status = assemble_status(&healthy_probe(), fed_id(1));
        assert_eq!(status.balance.spendable, Msat(1_000_000));
        assert_eq!(status.balance.in_flight, Msat(5_000));
        assert_eq!(status.balance.claimable, Msat(7_000));
        // A fresh probe reserves no fee — that is the allocator/executor's job.
        assert_eq!(status.balance.reserved_fee, Msat(0));
    }

    #[test]
    fn assembled_statuses_drive_decide_to_rebalance() {
        // Two distinct healthy feds: spending is under target, standby is funded.
        // decide() must move funds from the standby into the spending fed. This proves
        // the whole sense→decide wire.
        let mut spending_probe = healthy_probe();
        spending_probe.spendable_msat = 10_000;
        let spending = assemble_status(&spending_probe, fed_id(0xaa));

        let mut standby_probe = healthy_probe();
        standby_probe.spendable_msat = 5_000_000;
        let standby = assemble_status(&standby_probe, fed_id(0xbb));

        let snapshot = AllocatorSnapshot {
            federations: vec![spending.clone(), standby.clone()],
            spending_fed: Some(spending.id),
            standby_fed: Some(standby.id),
            per_fed_cap: Msat(100_000_000),
            target_spending_balance: Msat(1_000_000),
            standby_target: Msat(1_000_000),
            max_fee: Msat(10_000),
            now: 0,
        };

        let decisions = decide(&snapshot, Occurrence(1));
        let moved = decisions.iter().find_map(|d| match &d.action {
            Action::Move {
                from, to, amount, ..
            } if *from == standby.id && *to == spending.id => Some(*amount),
            _ => None,
        });
        assert_eq!(
            moved,
            Some(Msat(990_000)),
            "decide() should top the spending fed up to target from the standby; got {decisions:?}"
        );
        // And the recorded decision is the top-up reason, not an advisory refusal.
        assert!(decisions
            .iter()
            .any(|d| d.reason == AllocReason::SpendingBelowTarget));
    }
}
