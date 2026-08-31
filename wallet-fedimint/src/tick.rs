//! The orchestrator "decide" glue (Phase 2 step 2.2): the standing-instruction
//! [`TickPolicy`] and the PURE [`build_snapshot`] that turns a batch of recorded
//! [`ProbeResult`]s into the `wallet_core::AllocatorSnapshot` the pure allocator
//! (`wallet_core::decide`) consumes (`docs/archive/phase2-plan.md`).
//!
//! Split like [`crate::probe`]: everything here is PURE (no I/O, no async, no
//! fedimint types) so it sits in the fast rb-lite gate — a recorded `ProbeResult`
//! fixture + a policy → `build_snapshot` → `decide` yields the expected decisions.
//! The I/O that actually probes the live feds and drives the executor is
//! [`crate::runtime::Runtime::tick`] / [`crate::runtime::Runtime::status`].
//!
//! ## Standing instruction (ADR-0014)
//! [`TickPolicy`] is the user's standing instruction: the per-fed cap (ADR-0018), the
//! spending/standby targets, the move fee cap, and the spending/standby designation.
//! The designation is OPTIONAL — when the operator does not pin it, `build_snapshot`
//! AUTO-designates from the SCORED-eligible feds: rank first, then spendable balance,
//! then federation id for determinism. An INELIGIBLE fed (no-lnv2 / unhealthy / wrong
//! network / probe-failed) is never auto-designated, though it still appears in the
//! snapshot's `federations` so the allocator can see an over-cap balance sitting on it.

use crate::probe::{assemble_facts, assemble_status, ProbeResult};
use std::collections::{BTreeMap, BTreeSet};
use wallet_core::{
    score, Action, ActiveProbeVerdict, AllocatorDecision, AllocatorGoal, AllocatorSnapshot,
    ExecutionSummary, FederationId, FederationStatus, FederationVerdict, Msat, Occurrence,
    ReasonCode, ScorerPolicy,
};

// ---- v1 default standing instruction (documented) --------------------------------
//
// These are deliberately small, WoS-style spending-wallet numbers, chosen so the
// defaults are internally consistent: the per-fed cap sits comfortably above the sum
// of the two targets, so the standing targets never immediately trip the cap. Every
// one is overridable by a `wallet-cli` flag.

/// Default spending-fed target balance: 100k sats (100_000_000 msat). Below this, a
/// tick tops the spending fed up from the standby.
const DEFAULT_TARGET_SPENDING_BALANCE: Msat = Msat(100_000_000);
/// Default warm-standby target balance: 100k sats. Below this, a tick funds the
/// standby from the spending fed's surplus (its balance ABOVE the spending target).
const DEFAULT_STANDBY_TARGET: Msat = Msat(100_000_000);
/// Default per-fed balance cap (ADR-0018): 5M sats (0.05 BTC). Well above the two
/// targets, so it bounds accumulation without fighting the standing targets.
const DEFAULT_PER_FED_CAP: Msat = Msat(5_000_000_000);
/// Default ABSOLUTE per-move fee cap: 50 sats (50_000 msat). It bounds NO allocator-emitted
/// action: funding `Move`s use `DEFAULT_MAX_FEE_BPS_OF_MOVE`, and `Evacuate` uses the
/// `evac_fee_base_msat` + `evac_fee_bps` pair. It survives as the default for the user-initiated
/// money verbs and as the probe leg cap, which is what `--max-fee` tightens. Saying otherwise
/// points an operator at a knob that cannot bound an evacuation at all.
const DEFAULT_MAX_FEE: Msat = Msat(50_000);

/// Default PROPORTIONAL funding-move fee cap: 300 bps (3%) of the amount moved. See
/// `wallet_api::Policy` default for the derivation (preserves the pilot's ~258 bps effective
/// cap with headroom over realistic gateway fees). Tighten with `--max-fee-bps-of-move`.
const DEFAULT_MAX_FEE_BPS_OF_MOVE: u16 = 300;

/// Default evacuation fee-cap base: 200 sats (200_000 msat).
const DEFAULT_EVAC_FEE_BASE: Msat = Msat(200_000);
/// Default evacuation fee-cap proportional part: 300 bps (3%) of the net DELIVERED.
const DEFAULT_EVAC_FEE_BPS: u16 = 300;

/// The standing instruction for one orchestrator tick (ADR-0014). Sensible v1 defaults
/// (see the module constants) are provided by [`Default`]; `wallet-cli` overrides any
/// field from a flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickPolicy {
    /// ADR-0018 hard per-fed balance cap: the allocator refuses to push a fed above it.
    pub per_fed_cap: Msat,
    /// Target balance for the spending fed; below it, top up from the standby.
    pub target_spending_balance: Msat,
    /// Target balance for the warm standby; below it, fund from the spending surplus.
    pub standby_target: Msat,
    /// ABSOLUTE per-move fee cap. It bounds NO allocator-emitted action: funding `Move`s use
    /// `max_fee_bps_of_move` and `Evacuate` uses the `evac_fee_*` pair. It survives as the
    /// default cap for the user-initiated money verbs and as the probe leg cap.
    pub max_fee: Msat,
    /// PROPORTIONAL fee cap for funding `Move`s, in basis points of the amount moved
    /// (1..=10000; Policy rejects 0). Sizing reserves `amount + amount*bps/10000` from the source budget.
    pub max_fee_bps_of_move: u16,
    /// BASE component of the evacuation fee cap, in millisatoshis. ENFORCED, with `evac_fee_bps`,
    /// against the DELIVERED net; see `wallet_api::Policy`.
    pub evac_fee_base_msat: Msat,
    /// PROPORTIONAL component of the evacuation fee cap, in basis points of the DELIVERED net —
    /// not of the amount asked for (0..=10000); see `wallet_api::Policy`.
    pub evac_fee_bps: u16,
    /// The allocation epoch (T10) stamped into each decision's idempotency key.
    pub occurrence: Occurrence,
    /// Operator-pinned spending fed. `None` ⇒ auto-designate from the scored-eligible feds.
    pub spending_fed: Option<FederationId>,
    /// Operator-pinned standby fed. `None` ⇒ auto-designate the next eligible fed.
    pub standby_fed: Option<FederationId>,
    /// Wall-clock epoch copied into the snapshot's `now`. The allocator does not read it
    /// today; carrying it on the policy keeps the snapshot's clock a single, pure input
    /// so any future time-based decision stays testable without a real clock.
    pub now: u64,
    /// The active-probe policy the §5.1.3 FUNDING GATE evaluates a discovered (`AutoJoined`)
    /// fed's sustained-pass verdict under — a standing-instruction knob like `per_fed_cap`.
    /// Default is the conservative `ProbePolicy::default()` (3 successes spanning >= 24h within
    /// a 7d ttl); an operator may loosen the window (they own the risk tradeoff), which is also
    /// how a live gate demonstrates the probe-pass -> fund path without a 24h wait.
    pub probe_gate_policy: wallet_core::ProbePolicy,
    /// The logical allocator goals durable work already owns (br-p93), projected by the daemon's
    /// preceding reconcile or by the standalone Runtime's own durable scan. Per-CYCLE state like
    /// `occurrence`/`now`, not an operator knob: `Policy` cannot set it and `Default` is EMPTY for
    /// callers that do not plan allocator work. Carrying it here keeps route pricing and planning
    /// on one value; each apply/commit seam re-scans to close a planning race.
    pub blocked: wallet_core::GoalBlockers,
}

impl Default for TickPolicy {
    fn default() -> Self {
        Self {
            per_fed_cap: DEFAULT_PER_FED_CAP,
            target_spending_balance: DEFAULT_TARGET_SPENDING_BALANCE,
            standby_target: DEFAULT_STANDBY_TARGET,
            max_fee: DEFAULT_MAX_FEE,
            max_fee_bps_of_move: DEFAULT_MAX_FEE_BPS_OF_MOVE,
            evac_fee_base_msat: DEFAULT_EVAC_FEE_BASE,
            evac_fee_bps: DEFAULT_EVAC_FEE_BPS,
            occurrence: Occurrence(0),
            spending_fed: None,
            standby_fed: None,
            now: 0,
            probe_gate_policy: wallet_core::ProbePolicy::default(),
            blocked: wallet_core::GoalBlockers::default(),
        }
    }
}

/// Build the `wallet_core::AllocatorSnapshot` a tick decides over. PURE: no I/O, no
/// async, total over the inputs.
///
/// - `federations`: `assemble_status` for EVERY probed fed (so the allocator sees an
///   over-cap balance even on a fed we would never fund into).
/// - designation: `policy`'s `spending_fed`/`standby_fed` when set, else AUTO from the
///   SCORED-eligible feds — ordered by scorer rank, then `spendable`, then ascending
///   federation id for determinism.
/// - caps/targets/max_fee/now: copied straight from `policy`.
pub fn build_snapshot(
    probes: &[(FederationId, ProbeResult)],
    policy: &TickPolicy,
    scorer_policy: &ScorerPolicy,
    auto_joined: &BTreeSet<FederationId>,
    active_probes: &BTreeMap<FederationId, ActiveProbeVerdict>,
) -> AllocatorSnapshot {
    // Score every fed once (pure): the verdict both stamps `eligible_to_fund` on each
    // status (§15.3 — the destination gate the evacuator reads) AND drives
    // auto-designation below. `verdicts` is parallel to `probes`.
    let verdicts: Vec<FederationVerdict> = probes
        .iter()
        .map(|(id, probe)| score(&assemble_facts(probe, *id), scorer_policy))
        .collect();

    let federations: Vec<FederationStatus> = probes
        .iter()
        .zip(&verdicts)
        .map(|((id, probe), verdict)| FederationStatus {
            // An operator PIN overrides the scorer's verdict (§15.3 refinement, found by the
            // live evacuate gate): an explicit `--spending`/`--standby` is the operator
            // vouching for that exact fed (the same semantics that already exempt pins from
            // scorer AUTO-designation), so a pinned fed stays an eligible evacuation
            // DESTINATION even when the scorer rejects it structurally — otherwise a pinned
            // standby on a scorer-rejected network (devimint is regtest) would turn every
            // evacuation into a refusal. The §15.3 gate exists for the FALLBACK scan: money
            // must never drain into an arbitrary scorer-rejected fed the operator never
            // chose. Liveness/route gating still applies to pins unchanged (`receive_blocker`
            // reads `probed_ok`/reputation), and — because the `&& probe_gate_ok` below folds
            // into `eligible_to_fund`, which `receive_blocker` also reads — a pinned AutoJoined
            // fed stays probe-gated too: a pin never bypasses the probe gate (§5.1.3).
            eligible_to_fund: (verdict.eligible_to_fund
                || policy.spending_fed == Some(*id)
                || policy.standby_fed == Some(*id))
                && probe_gate_ok(*id, auto_joined, active_probes),
            ..assemble_status(probe, *id)
        })
        .collect();

    // Scored-eligible feds ranked for auto-designation: keep only the fundable ones,
    // ordered by rank score first.
    // `spendable` is a secondary tie-breaker so equal-quality feds prefer the one already
    // holding the most usable balance; id makes the choice deterministic.
    let mut eligible_destinations: Vec<(FederationId, u32, u64)> = probes
        .iter()
        .zip(&verdicts)
        .filter_map(|((id, probe), verdict)| {
            (verdict.eligible_to_fund && probe_gate_ok(*id, auto_joined, active_probes))
                .then_some((*id, verdict.rank_score, probe.spendable_msat))
        })
        .collect();
    eligible_destinations.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.0.cmp(&b.0))
    });
    let eligible_spending: Vec<(FederationId, u32, u64)> = eligible_destinations
        .iter()
        .copied()
        .filter(|(id, _, _)| !auto_joined.contains(id))
        .collect();

    // Auto spending is the best-ranked eligible fed that is NOT an operator-pinned standby.
    // Auto-joined discovered feds are excluded from this pick so they never become the source
    // that must prove their own active probe. They can still be auto-designated as destinations
    // once their active probe has passed.
    // Excluding the pinned standby matters when the operator pins ONLY `--standby` and that fed
    // ranks highest: without the guard, both roles would collapse onto it and the allocator would
    // silently no-op the rebalance the operator explicitly designated a standby for.
    let spending_fed = policy.spending_fed.or_else(|| {
        eligible_spending
            .iter()
            .map(|(id, _, _)| *id)
            .find(|id| Some(*id) != policy.standby_fed)
    });
    // Auto standby is the best-ranked eligible fed that is NOT the spending fed,
    // so an operator-pinned spending fed still gets a distinct auto standby.
    let standby_fed = policy.standby_fed.or_else(|| {
        eligible_destinations
            .iter()
            .map(|(id, _, _)| *id)
            .find(|id| Some(*id) != spending_fed)
    });

    AllocatorSnapshot {
        federations,
        spending_fed,
        standby_fed,
        per_fed_cap: policy.per_fed_cap,
        target_spending_balance: policy.target_spending_balance,
        standby_target: policy.standby_target,
        max_fee: policy.max_fee,
        max_fee_bps_of_move: policy.max_fee_bps_of_move,
        evac_fee_base_msat: policy.evac_fee_base_msat,
        evac_fee_bps: policy.evac_fee_bps,
        // lnv2's minimum incoming contract: a fund/top-up sized below this could only fail
        // at perform time, so the allocator treats a sub-floor shortfall as dust.
        min_move: Msat(crate::executor::MINIMUM_INCOMING_CONTRACT_MSAT),
        // Left EMPTY here, and filled by the caller's I/O step: pricing a pair needs live fee
        // quotes, and this function is pure. Empty reads as "not priced this tick", the
        // permissive `min_move` fallback — which is also exactly what the pure golden tests and
        // any probe-only assembler want.
        route_economics_by_pair: BTreeMap::new(),
        reservations: wallet_core::Reservations::default(),
        now: policy.now,
    }
}

fn probe_gate_ok(
    id: FederationId,
    auto_joined: &BTreeSet<FederationId>,
    active_probes: &BTreeMap<FederationId, ActiveProbeVerdict>,
) -> bool {
    !auto_joined.contains(&id) || active_probes.get(&id) == Some(&ActiveProbeVerdict::Passed)
}

/// The operator-PINNED feds (`policy.spending_fed`/`standby_fed`) that are ABSENT from a built
/// snapshot's `federations` — i.e. a fed the operator explicitly designated whose probe FAILED
/// this tick, so [`probe_all`](crate::runtime::Runtime::tick) dropped it. PURE, total over the
/// inputs; returns each missing pin once, in `spending`-then-`standby` order.
///
/// An AUTO-designated fed can never be missing (it is only ever chosen from present probes), so
/// this reports ONLY unsensed EXPLICIT pins. The tick uses it to fail LOUDLY rather than silently
/// no-op the operator's explicitly-designated rebalance and report a false success to a scheduler
/// gating on the exit code — the drop is otherwise only a stderr warn from `probe_all`, leaving the
/// tick's exit code at 0, which is exactly the false-success its non-zero-on-failure contract
/// exists to prevent. (An auto pick is degraded safely by dropping; an explicit pin is not.)
pub fn missing_pinned_feds(policy: &TickPolicy, snapshot: &AllocatorSnapshot) -> Vec<FederationId> {
    let mut missing = Vec::new();
    for pinned in [policy.spending_fed, policy.standby_fed]
        .into_iter()
        .flatten()
    {
        let present = snapshot.federations.iter().any(|f| f.id == pinned);
        if !present && !missing.contains(&pinned) {
            missing.push(pinned);
        }
    }
    missing
}

/// Operator-pinned feds that PROBED this tick but failed the lnv2/probe gate the allocator needs
/// before it can route money through them. The probe already reflects the CLI-pinned gateway when
/// one was supplied (it validates that exact gateway for the fed) and otherwise the
/// registered-gateway set, so this is the usable-route/liveness check for BOTH the
/// pinned-`--gateway` path AND the default gateway-selection path — a pin the operator relies on
/// default selection for is honored or failed just like one behind an explicit `--gateway`.
///
/// The check is intentionally limited to explicit spending/standby pins. Auto-designation already
/// excludes failed probes through the scorer gate, while an explicit pin is a request to use that
/// exact fed: quietly downgrading it to an advisory `NotProbed` decision that `apply` skips would
/// make a scheduled tick look successful even though the requested rebalance could not run.
pub fn unusable_pinned_feds(
    policy: &TickPolicy,
    probes: &[(FederationId, ProbeResult)],
) -> Vec<FederationId> {
    let mut unusable = Vec::new();
    for pinned in [policy.spending_fed, policy.standby_fed]
        .into_iter()
        .flatten()
    {
        if unusable.contains(&pinned) {
            continue;
        }
        if let Some((_, probe)) = probes.iter().find(|(id, _)| *id == pinned) {
            let status = assemble_status(probe, pinned);
            if !probe.has_lnv2 || !status.probed_ok {
                unusable.push(pinned);
            }
        }
    }
    unusable
}

/// Operator-pinned feds that are present in the snapshot but not fundable as destinations after
/// the full `build_snapshot` gate. This catches the §5.1 AutoJoined active-probe gate layered on
/// top of the scorer/pin rule: a pin keeps ordinary user-owned feds eligible, but it must not
/// bypass the probe gate for agent-auto-joined candidates.
pub fn unfundable_pinned_feds(
    policy: &TickPolicy,
    snapshot: &AllocatorSnapshot,
) -> Vec<FederationId> {
    let mut unfundable = Vec::new();
    for pinned in [policy.spending_fed, policy.standby_fed]
        .into_iter()
        .flatten()
    {
        if unfundable.contains(&pinned) {
            continue;
        }
        if let Some(fed) = snapshot.federations.iter().find(|f| f.id == pinned) {
            if !fed.eligible_to_fund {
                unfundable.push(pinned);
            }
        }
    }
    unfundable
}

/// The operator-pinned inputs a tick could NOT honor this pass, each as a human-readable problem
/// line (empty when every pin is usable). Three PIN-ONLY failure modes — an auto pick is only ever
/// chosen from usable probes, so it is never reported:
/// - a pin ABSENT from the snapshot (its probe failed this tick — [`missing_pinned_feds`]);
/// - a pin PRESENT but not fundable after snapshot assembly, such as an AutoJoined candidate
///   without a `Passed` active probe ([`unfundable_pinned_feds`]);
/// - a pin PRESENT but unusable for lnv2 moves this tick — no lnv2 module, dead quorum, or no
///   usable gateway route ([`unusable_pinned_feds`]) — subject to the voucher rules below.
///
/// Admitted work fixes a source-only pin's false bail: a rebalance `A -> B` routes
/// through the DESTINATION's gateway (spec §7's shared-gateway internal swap — the executor even
/// proves that gateway serves the source before minting), so a pinned SOURCE `A` needs only live
/// quorum and `B`'s gateway to serve it, NOT its own registered gateway. A surviving admitted
/// `Move` or `Evacuate` names the current round's exact route endpoints; the planner concretely
/// preflights it when the round has route-I/O budget, and the executor revalidates before sending
/// either way. Thus either endpoint is stronger evidence than the coarse per-fed `probed_ok`
/// proxy: that proxy checks the explicitly supplied gateway when present, or otherwise scans the
/// federation's registered gateways for one that validates, while the admitted endpoint identifies
/// the shared route this decision will use. Only a pin whose rebalance did NOT survive as admitted
/// executable work is failed on the coarser raw probe gate.
///
/// `admitted` contains work which conflict projection allowed and which may execute; `suppressed`
/// contains work withheld before any concrete preflight. Suppressed work never vouches for an
/// endpoint merely by appearing in a decision list. A suppressed executable can vouch only for its
/// SOURCE, and only when the conflicting durable holder has the same associated source. Advisory
/// recurrences are similarly source-and-goal-associated. This avoids a policy rotation turning an
/// old `A -> B` holder into a voucher for fresh funding into unusable `A`.
///
/// PURE, total over the inputs. [`Runtime::tick`](crate::runtime::Runtime::tick) turns any problem
/// into a hard non-zero bail — a money op must never report a pinned rebalance it could not run as
/// success (the exit-code contract a scheduler gates on). [`Runtime::status`](crate::runtime::Runtime::status),
/// a dry run, surfaces the same problems as warnings so its scored view still prints: it is the
/// command an operator runs to SEE why a tick is failing.
pub fn pinned_input_problems(
    policy: &TickPolicy,
    snapshot: &AllocatorSnapshot,
    probes: &[(FederationId, ProbeResult)],
    admitted: &[AllocatorDecision],
    suppressed: &[AllocatorDecision],
    blockers: &wallet_core::GoalBlockers,
) -> Vec<String> {
    let mut problems = Vec::new();
    let missing = missing_pinned_feds(policy, snapshot);
    if !missing.is_empty() {
        problems.push(format!(
            "pinned federation(s) {} failed to probe this tick, so their rebalance was not \
             evaluated; check stderr for the probe error and retry",
            hexes(&missing)
        ));
    }
    // A present pin that `build_snapshot` still marks not fundable was rejected by the AutoJoined
    // active-probe gate (§5.1.3) — the ONLY gate that can leave a PIN not fundable, since a pin
    // already satisfies the scorer/eligibility OR of `eligible_to_fund`. Unlike the raw lnv2/route
    // gate below, this one is NOT relaxed by an executable move: a pinned AutoJoined fed appearing
    // as a move SOURCE proves only that this tick's gateway route is usable, never that the
    // discovered fed passed its active probe (source-side moves are gated on evacuation, not on
    // `eligible_to_fund`, so an unproven pin CAN surface as a `from`). A pin does not vouch for
    // empirical redeemability (§5.1.3), so it must still fail loudly rather than let `tick` drain
    // an unproven auto-joined partition; approve or successfully probe the fed to fund from it.
    let unfundable = unfundable_pinned_feds(policy, snapshot);
    if !unfundable.is_empty() {
        problems.push(format!(
            "pinned federation(s) {} failed the fundability gate this tick, so their rebalance \
             could not run; if this is an auto-joined candidate, run successful active probes or \
             approve it before funding it",
            hexes(&unfundable)
        ));
    }
    // Missing and active-probe/fundability pins stay loud above.  The evidence order is deliberately
    // narrow: an admitted endpoint wins; then an exact suppressed source/holder pair wins; only then
    // does a current funding receive-gate refusal dominate advisory recurrence evidence.  A suppressed
    // executable candidate is still current source evidence for its exact durable holder, whereas a
    // zero advisory was neither preflighted nor executable and must never hide a fresh `NotProbed`.
    let unusable: Vec<FederationId> = unusable_pinned_feds(policy, probes)
        .into_iter()
        .filter(|id| !unfundable.contains(id))
        .filter(|id| {
            !fed_in_executable_move(*id, admitted)
                && !suppressed_source_vouches(*id, suppressed, blockers)
                && (has_funding_receive_gate_refusal(*id, admitted)
                    || !advisory_recurrence_vouches(*id, admitted, suppressed, blockers))
        })
        .collect();
    if !unusable.is_empty() {
        problems.push(format!(
            "pinned federation(s) {} failed the lnv2/probe gate this tick, so their rebalance \
             could not run; ensure they have lnv2, live quorum, and a usable gateway route — if \
             you passed --gateway, confirm it serves this federation; otherwise pass one against \
             devimint (see docs/devimint-runbook.md §4), then retry",
            hexes(&unusable)
        ));
    }
    problems
}

/// Whether `id` is the source or destination of an admitted executable `Move` OR `Evacuate` — a
/// rebalance leg allowed by conflict projection which can reach `apply` (advisory
/// `RefuseInflow`/`Cap` never does). Planning preflights it when route-I/O budget is available and
/// the executor revalidates before sending, so it is stronger evidence than the coarse per-fed
/// probe gate.
///
/// `Evacuate` counts as of Phase 3.A (it is no longer withheld by [`decisions_to_apply`]). The
/// evacuating SOURCE is unhealthy/shutting-down BY DEFINITION — that is exactly why it is being
/// drained — so its own red probe gate must not abort the evacuate; only the DESTINATION needs to
/// be route-eligible, and `safest_other` already picks the destination from healthy, cap-roomed
/// feds. Treating the source `from` as route-validated here keeps a pinned dying fed from failing
/// the tick on the very unhealthiness that triggered its evacuation.
fn fed_in_executable_move(id: FederationId, decisions: &[AllocatorDecision]) -> bool {
    decisions.iter().any(|d| {
        matches!(
            &d.action,
            Action::Move { from, to, .. } | Action::Evacuate { from, to, .. }
                if *from == id || *to == id
        )
    })
}

/// A funding destination's current receive-gate refusal. `receive_blocker` currently reports
/// `NotProbed`; the arithmetic marker keeps an unrelated advisory with the same reason from
/// becoming a pin gate.
fn has_funding_receive_gate_refusal(id: FederationId, decisions: &[AllocatorDecision]) -> bool {
    decisions.iter().any(|decision| {
        matches!(
            &decision.action,
            Action::RefuseInflow { fed, reason: ReasonCode::NotProbed, diagnostics }
                if *fed == id && diagnostics.want.is_some() && diagnostics.max_fee_bps.is_some()
        )
    })
}

/// Suppressed work was not preflighted. It can therefore vouch only for its source, and only if
/// the exact conflicting holder retains that same source association.
fn suppressed_source_vouches(
    id: FederationId,
    suppressed: &[AllocatorDecision],
    blockers: &wallet_core::GoalBlockers,
) -> bool {
    suppressed.iter().any(|decision| {
        let source = match &decision.action {
            Action::Move { from, .. } | Action::Evacuate { from, .. } => *from,
            _ => return false,
        };
        source == id
            && blockers.has_blocking_holder_from(
                decision,
                wallet_core::Actor::Agent {
                    occurrence: decision.occurrence,
                },
                source,
            )
    })
}

/// A zero/advisory recurrence has no route preflight. It is only evidence of a held goal when both
/// the current recurrence and its durable holder name the same source.
fn advisory_recurrence_vouches(
    id: FederationId,
    admitted: &[AllocatorDecision],
    suppressed: &[AllocatorDecision],
    blockers: &wallet_core::GoalBlockers,
) -> bool {
    admitted.iter().chain(suppressed).any(|decision| {
        let Action::RefuseInflow {
            fed,
            reason,
            diagnostics,
        } = &decision.action
        else {
            return false;
        };
        if matches!(reason, ReasonCode::ShutdownNotice | ReasonCode::Unhealthy) {
            *fed == id && blockers.holds_goal_from(AllocatorGoal::Evacuate(*fed), *fed)
        } else {
            diagnostics.source.is_some_and(|source| {
                source == id && blockers.holds_goal_from(AllocatorGoal::FundInto(*fed), source)
            })
        }
    })
}

/// Comma-join federation ids as hex for a diagnostic message.
fn hexes(ids: &[FederationId]) -> String {
    ids.iter()
        .map(|id| id.to_hex())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The decisions a tick drives through the executor. PURE, total over the input. As of
/// Phase 3.A this is the FULL decision list: `Move`/`DirectInflow`/`Evacuate` are all
/// executable and the executor performs each (`MovePlan::from_action` maps `Evacuate` to
/// the same send-required plan as `Move`), while the advisory `RefuseInflow`/`Cap` pass
/// through and the pure `apply` skips them via `Action::is_executable`. `Evacuate` is no
/// longer dropped — evacuation execution is a Phase-3 GOAL, not a non-goal. Callers still
/// keep the full decision list for reporting; this seam remains the single, documented
/// "what reaches `apply`" projection.
pub fn decisions_to_apply(decisions: &[AllocatorDecision]) -> Vec<AllocatorDecision> {
    decisions.to_vec()
}

/// One federation's scored view for the `status` (dry-run) report: its fundability
/// verdict plus the allocator status assembled from the same probe.
#[derive(Clone, Debug, PartialEq)]
pub struct ScoredFed {
    pub id: FederationId,
    pub verdict: FederationVerdict,
    pub status: FederationStatus,
    /// The ACTIVE-probe verdict against the snapshot's designated spending fed (phase 5
    /// §5.0.6), computed with the DEFAULT `ProbePolicy`. `None` when no spending fed is
    /// designated (nothing to evaluate the pair against). Display + 5.1-gate material;
    /// 5.0 gates nothing on it.
    pub active_probe: Option<ActiveProbeVerdict>,
    /// The POST-GATE fundability from the same `build_snapshot` the tick applies — i.e. what
    /// `tick` will actually do, not just the scorer's structural verdict. It layers the §5.1.3
    /// AutoJoined active-probe gate (and pin override) on top of `verdict.eligible_to_fund`, so
    /// an `AutoJoined` fed reads `false` until its active probe Passes even when the scorer
    /// (which ignores the active probe, §5.0.6) accepts it. Surfaced so `status` cannot report
    /// `eligible=true` for a fed `tick` would refuse.
    pub gated_eligible: bool,
}

/// The result of [`crate::runtime::Runtime::tick`]: the decisions the allocator produced
/// and the [`ExecutionSummary`] from applying them (advisory `RefuseInflow`/`Cap`
/// decisions are surfaced here but count as `skipped` by `apply`, never executed).
#[derive(Clone, Debug)]
pub struct TickReport {
    pub decisions: Vec<AllocatorDecision>,
    pub summary: ExecutionSummary,
    pub spending_fed: Option<FederationId>,
    pub standby_fed: Option<FederationId>,
}

/// The result of [`crate::runtime::Runtime::status`]: the per-fed scored view, the
/// designation `build_snapshot` chose, and the decisions that WOULD run — a dry run,
/// nothing applied.
#[derive(Clone, Debug)]
pub struct StatusReport {
    pub scored: Vec<ScoredFed>,
    pub spending_fed: Option<FederationId>,
    pub standby_fed: Option<FederationId>,
    pub decisions: Vec<AllocatorDecision>,
    /// Funding goals the move floor withheld (br-0vg). A tick emits no decision and no ledger row
    /// for these — a dry-run `status` is the one place an operator can see them, and without it a
    /// permanently-deferred rebalance is indistinguishable from an idle, healthy wallet.
    pub deferred: Vec<wallet_core::DeferredFunding>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use wallet_core::{decide, Actor, GoalBlockers, IdempotencyKey, ReasonCode};

    fn fed_id(byte: u8) -> FederationId {
        FederationId([byte; 32])
    }

    fn held_funding_blockers(from: FederationId, to: FederationId) -> GoalBlockers {
        let decision = AllocatorDecision {
            action: Action::Move {
                from,
                to,
                amount: Msat(1),
                fee_cap: Msat(0),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence: Occurrence(1),
            idempotency_key: IdempotencyKey("held-pin-boundary".to_owned()),
        };
        let mut blockers = GoalBlockers::default();
        blockers.hold_decision(
            &decision,
            Actor::Agent {
                occurrence: Occurrence(1),
            },
        );
        blockers
    }

    fn held_evacuation_blockers(from: FederationId, to: FederationId) -> GoalBlockers {
        let decision = AllocatorDecision {
            action: Action::Evacuate {
                from,
                to,
                amount: Msat(1),
                fee_cap: Msat(0),
                gateway: None,
                fee_cap_components: None,
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence: Occurrence(1),
            idempotency_key: IdempotencyKey("held-evacuation-pin-boundary".to_owned()),
        };
        let mut blockers = GoalBlockers::default();
        blockers.hold_decision(
            &decision,
            Actor::Agent {
                occurrence: Occurrence(1),
            },
        );
        blockers
    }

    /// A healthy, scorer-eligible probe holding `spendable_msat` (4-guardian mainnet fed
    /// with mint+wallet+lnv2, live quorum, a usable gateway).
    fn healthy_probe(spendable_msat: u64) -> ProbeResult {
        ProbeResult {
            guardian_count: 4,
            threshold: 3,
            is_mainnet: true,
            module_kinds: vec!["mint".into(), "wallet".into(), "ln".into(), "lnv2".into()],
            has_lnv2: true,
            quorum_live: true,
            latency_ms: 42,
            gateway_available: true,
            wallet_module_present: true,
            expiry_timestamp_secs: None,
            config_expiry_secs: None,
            meta_module_expiry_secs: None,
            status_scheduled_shutdown: false,
            shutdown_scheduled: false,
            spendable_msat,
            in_flight_msat: 0,
            claimable_msat: 0,
        }
    }

    /// An INELIGIBLE probe (no lnv2): the scorer rejects it, so it must never be
    /// auto-designated no matter how much `spendable` it holds.
    fn ineligible_probe(spendable_msat: u64) -> ProbeResult {
        let mut probe = healthy_probe(spendable_msat);
        probe.has_lnv2 = false;
        probe.module_kinds = vec!["mint".into(), "wallet".into(), "ln".into()];
        probe
    }

    fn build_snapshot(
        probes: &[(FederationId, ProbeResult)],
        policy: &TickPolicy,
        scorer_policy: &ScorerPolicy,
    ) -> AllocatorSnapshot {
        super::build_snapshot(
            probes,
            policy,
            scorer_policy,
            &BTreeSet::new(),
            &BTreeMap::new(),
        )
    }

    fn eligible_of(snapshot: &AllocatorSnapshot, id: FederationId) -> bool {
        snapshot
            .federations
            .iter()
            .find(|f| f.id == id)
            .expect("fed present in snapshot")
            .eligible_to_fund
    }

    fn move_between(
        decisions: &[AllocatorDecision],
        from: FederationId,
        to: FederationId,
    ) -> Option<(Msat, ReasonCode)> {
        decisions.iter().find_map(|d| match &d.action {
            Action::Move {
                from: f,
                to: t,
                amount,
                ..
            } if *f == from && *t == to => Some((*amount, d.reason)),
            _ => None,
        })
    }

    #[test]
    fn default_policy_is_internally_consistent() {
        let policy = TickPolicy::default();
        // Every standing target is non-zero (a zeroed target would silently disable a
        // whole rebalance path).
        assert!(policy.target_spending_balance.0 > 0);
        assert!(policy.standby_target.0 > 0);
        assert!(policy.max_fee.0 > 0);
        // The per-fed cap sits above the sum of the two targets, so the standing
        // instruction can hold both without immediately tripping the cap.
        assert!(policy.per_fed_cap.0 > policy.target_spending_balance.0 + policy.standby_target.0);
        // No designation by default — the tick auto-designates.
        assert!(policy.spending_fed.is_none());
        assert!(policy.standby_fed.is_none());
    }

    #[test]
    fn auto_designation_uses_spendable_as_rank_tie_breaker() {
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let snapshot = build_snapshot(&probes, &TickPolicy::default(), &ScorerPolicy::default());
        // Equal-rank eligible feds prefer the one already holding the most spendable balance.
        assert_eq!(snapshot.spending_fed, Some(fed_id(1)));
        assert_eq!(snapshot.standby_fed, Some(fed_id(2)));
        // Every probed fed lands in the snapshot's federations list.
        assert_eq!(snapshot.federations.len(), 2);
    }

    #[test]
    fn auto_designation_prefers_scorer_rank_before_spendable() {
        let mut stronger = healthy_probe(100_000);
        stronger.guardian_count = 5;
        stronger.threshold = 4;
        let weaker = healthy_probe(5_000_000);
        let probes = vec![(fed_id(1), weaker), (fed_id(2), stronger)];
        let snapshot = build_snapshot(&probes, &TickPolicy::default(), &ScorerPolicy::default());
        // Rank is primary: the structurally stronger fed becomes spending even though it
        // currently holds less spendable balance.
        assert_eq!(snapshot.spending_fed, Some(fed_id(2)));
        assert_eq!(snapshot.standby_fed, Some(fed_id(1)));
    }

    #[test]
    fn auto_designation_excludes_ineligible_fed() {
        // The ineligible fed holds the MOST spendable, but must never be designated.
        let probes = vec![
            (fed_id(3), ineligible_probe(10_000_000)),
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let snapshot = build_snapshot(&probes, &TickPolicy::default(), &ScorerPolicy::default());
        assert_eq!(snapshot.spending_fed, Some(fed_id(1)));
        assert_eq!(snapshot.standby_fed, Some(fed_id(2)));
        // The ineligible fed is still in the snapshot (so the allocator can see its
        // balance), just never designated.
        assert_eq!(snapshot.federations.len(), 3);
        assert!(snapshot.federations.iter().any(|f| f.id == fed_id(3)));
    }

    #[test]
    fn a_pin_overrides_the_scorer_verdict_for_evacuation_eligibility() {
        // Fed 2 is scorer-REJECTED (no lnv2). Unpinned it must stay ineligible_to_fund —
        // the §15.3 gate that keeps the evacuation FALLBACK from draining into an
        // arbitrary rejected fed. Pinned as standby, the operator's explicit vouch makes
        // it a valid evacuation destination (the same pin semantics that bypass scorer
        // auto-designation); auto-designation itself must still never pick it.
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), ineligible_probe(1_000_000)),
        ];
        let unpinned = build_snapshot(&probes, &TickPolicy::default(), &ScorerPolicy::default());
        assert!(!eligible_of(&unpinned, fed_id(2)));

        let pinned_policy = TickPolicy {
            standby_fed: Some(fed_id(2)),
            ..TickPolicy::default()
        };
        let pinned = build_snapshot(&probes, &pinned_policy, &ScorerPolicy::default());
        assert!(eligible_of(&pinned, fed_id(2)));
        // The pin vouches for the DESTINATION role only; the scorer-rejected fed is still
        // never auto-designated as spending.
        assert_eq!(pinned.spending_fed, Some(fed_id(1)));
    }

    #[test]
    fn auto_joined_fed_is_fundable_only_after_active_probe_passes() {
        let probes = vec![(fed_id(1), healthy_probe(5_000_000))];
        let auto_joined = BTreeSet::from([fed_id(1)]);

        let no_probe = super::build_snapshot(
            &probes,
            &TickPolicy::default(),
            &ScorerPolicy::default(),
            &auto_joined,
            &BTreeMap::new(),
        );
        assert!(!eligible_of(&no_probe, fed_id(1)));

        let failed_probe = super::build_snapshot(
            &probes,
            &TickPolicy::default(),
            &ScorerPolicy::default(),
            &auto_joined,
            &BTreeMap::from([(fed_id(1), ActiveProbeVerdict::Failed)]),
        );
        assert!(!eligible_of(&failed_probe, fed_id(1)));

        let passed_probe = super::build_snapshot(
            &probes,
            &TickPolicy::default(),
            &ScorerPolicy::default(),
            &auto_joined,
            &BTreeMap::from([(fed_id(1), ActiveProbeVerdict::Passed)]),
        );
        assert!(eligible_of(&passed_probe, fed_id(1)));
    }

    #[test]
    fn auto_joined_pin_does_not_bypass_probe_gate() {
        let probes = vec![(fed_id(1), healthy_probe(5_000_000))];
        let auto_joined = BTreeSet::from([fed_id(1)]);
        let policy = TickPolicy {
            standby_fed: Some(fed_id(1)),
            ..TickPolicy::default()
        };
        let snapshot = super::build_snapshot(
            &probes,
            &policy,
            &ScorerPolicy::default(),
            &auto_joined,
            &BTreeMap::new(),
        );
        assert!(!eligible_of(&snapshot, fed_id(1)));
        assert_eq!(snapshot.standby_fed, Some(fed_id(1)));
    }

    #[test]
    fn pinned_auto_joined_destination_is_refused_until_active_probe_passes() {
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(0)),
        ];
        let auto_joined = BTreeSet::from([fed_id(2)]);
        let policy = TickPolicy {
            per_fed_cap: Msat(100_000_000),
            target_spending_balance: Msat(1_000_000),
            standby_target: Msat(2_000_000),
            max_fee: Msat(10_000),
            spending_fed: Some(fed_id(1)),
            standby_fed: Some(fed_id(2)),
            ..TickPolicy::default()
        };

        let gated = super::build_snapshot(
            &probes,
            &policy,
            &ScorerPolicy::default(),
            &auto_joined,
            &BTreeMap::new(),
        );
        let gated_decisions = decide(&gated, policy.occurrence);
        assert!(
            gated_decisions.iter().any(|d| matches!(
                &d.action,
                Action::RefuseInflow {
                    fed,
                    reason: ReasonCode::NotProbed,
                    ..
                } if *fed == fed_id(2)
            )),
            "unpassed auto-joined standby must be refused, not funded: {gated_decisions:?}"
        );
        assert!(
            !gated_decisions
                .iter()
                .any(|d| matches!(&d.action, Action::Move { to, .. } if *to == fed_id(2))),
            "pinning must not bypass the probe gate: {gated_decisions:?}"
        );
        let gated_problems = pinned_input_problems(
            &policy,
            &gated,
            &probes,
            &gated_decisions,
            &[],
            &wallet_core::GoalBlockers::default(),
        );
        assert!(
            gated_problems.iter().any(
                |p| p.contains("failed the fundability gate")
                    && p.contains(&fed_id(2).to_hex())
            ),
            "a pinned AutoJoined fed blocked by the active-probe gate must fail loudly: {gated_problems:?}"
        );
        let held_gated_problems = pinned_input_problems(
            &policy,
            &gated,
            &probes,
            &gated_decisions,
            &[],
            &held_funding_blockers(fed_id(1), fed_id(2)),
        );
        assert!(
            held_gated_problems.iter().any(
                |p| p.contains("failed the fundability gate")
                    && p.contains(&fed_id(2).to_hex())
            ),
            "a held rebalance must not relax the active-probe/fundability gate: {held_gated_problems:?}"
        );

        let spending_pin_policy = TickPolicy {
            target_spending_balance: Msat(2_000_000),
            standby_target: Msat(0),
            spending_fed: Some(fed_id(2)),
            standby_fed: Some(fed_id(1)),
            ..policy.clone()
        };
        let spending_pin = super::build_snapshot(
            &probes,
            &spending_pin_policy,
            &ScorerPolicy::default(),
            &auto_joined,
            &BTreeMap::new(),
        );
        let spending_pin_decisions = decide(&spending_pin, spending_pin_policy.occurrence);
        assert!(
            spending_pin_decisions.iter().any(|d| matches!(
                &d.action,
                Action::RefuseInflow {
                    fed,
                    reason: ReasonCode::NotProbed,
                    ..
                } if *fed == fed_id(2)
            )),
            "unpassed auto-joined spending pin must be refused, not topped up: {spending_pin_decisions:?}"
        );
        assert!(
            !spending_pin_decisions
                .iter()
                .any(|d| matches!(&d.action, Action::Move { to, .. } if *to == fed_id(2))),
            "spending pin must not bypass the probe gate: {spending_pin_decisions:?}"
        );
        let spending_pin_problems = pinned_input_problems(
            &spending_pin_policy,
            &spending_pin,
            &probes,
            &spending_pin_decisions,
            &[],
            &wallet_core::GoalBlockers::default(),
        );
        assert!(
            spending_pin_problems.iter().any(
                |p| p.contains("failed the fundability gate")
                    && p.contains(&fed_id(2).to_hex())
            ),
            "a pinned AutoJoined spending fed blocked by the active-probe gate must fail loudly: {spending_pin_problems:?}"
        );

        let passed = super::build_snapshot(
            &probes,
            &policy,
            &ScorerPolicy::default(),
            &auto_joined,
            &BTreeMap::from([(fed_id(2), ActiveProbeVerdict::Passed)]),
        );
        let passed_decisions = decide(&passed, policy.occurrence);
        assert!(
            passed_decisions.iter().any(|d| matches!(
                &d.action,
                Action::Move { from, to, .. } if *from == fed_id(1) && *to == fed_id(2)
            )),
            "passed auto-joined standby can be funded: {passed_decisions:?}"
        );
        assert!(
            pinned_input_problems(
                &policy,
                &passed,
                &probes,
                &passed_decisions,
                &[],
                &wallet_core::GoalBlockers::default(),
            )
            .is_empty(),
            "a passed AutoJoined pin should not fail the tick guard"
        );
    }

    #[test]
    fn pinned_auto_joined_source_move_does_not_bypass_the_fundability_gate() {
        // The active-probe gate must fail loudly even when the pinned AutoJoined fed drives an
        // executable move as its SOURCE. Fed 1 is AutoJoined + unproven, pinned `--spending`, and
        // holds a surplus; fed 2 is a healthy user-owned standby under its target. The allocator
        // funds the standby FROM fed 1 (source-side moves are gated on evacuation, not on
        // `eligible_to_fund`), so fed 1 appears in an executable `Move`. That move only proves the
        // tick's gateway route is usable — NOT that fed 1 passed its active probe — so the
        // fundability problem must still be reported (§5.1.3: a pin does NOT bypass the probe gate,
        // and `tick` must never silently drain an unproven auto-joined partition).
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let auto_joined = BTreeSet::from([fed_id(1)]);
        let policy = TickPolicy {
            per_fed_cap: Msat(100_000_000),
            target_spending_balance: Msat(1_000_000),
            standby_target: Msat(2_000_000),
            max_fee: Msat(10_000),
            spending_fed: Some(fed_id(1)),
            standby_fed: Some(fed_id(2)),
            ..TickPolicy::default()
        };
        let snapshot = super::build_snapshot(
            &probes,
            &policy,
            &ScorerPolicy::default(),
            &auto_joined,
            &BTreeMap::new(),
        );
        // The unproven auto-joined spending pin is not fundable itself.
        assert!(!eligible_of(&snapshot, fed_id(1)));
        let decisions = decide(&snapshot, policy.occurrence);
        // Sanity: the rebalance really is a `Move fed1 -> fed2` with fed 1 (the unproven pin) as
        // the source, so `fed_in_executable_move(fed1)` is true this tick.
        assert!(
            move_between(&decisions, fed_id(1), fed_id(2)).is_some(),
            "expected a fund-standby Move fed1 -> fed2 sourcing from the unproven pin: {decisions:?}"
        );
        assert!(fed_in_executable_move(fed_id(1), &decisions));
        // Despite driving that move, the fundability gate must still fail loudly.
        let problems = pinned_input_problems(
            &policy,
            &snapshot,
            &probes,
            &decisions,
            &[],
            &wallet_core::GoalBlockers::default(),
        );
        assert!(
            problems
                .iter()
                .any(|p| p.contains("failed the fundability gate")
                    && p.contains(&fed_id(1).to_hex())),
            "an unproven AutoJoined spending pin must fail even when it drives a source move: {problems:?}"
        );
    }

    #[test]
    fn user_approved_or_user_joined_feds_keep_existing_scorer_or_pin_rule() {
        let healthy = vec![(fed_id(1), healthy_probe(5_000_000))];
        let healthy_snapshot =
            build_snapshot(&healthy, &TickPolicy::default(), &ScorerPolicy::default());
        assert!(
            eligible_of(&healthy_snapshot, fed_id(1)),
            "a user-owned healthy fed is grandfathered off the active-probe gate"
        );

        let rejected = vec![(fed_id(2), ineligible_probe(1_000_000))];
        let pinned_policy = TickPolicy {
            standby_fed: Some(fed_id(2)),
            ..TickPolicy::default()
        };
        let pinned = build_snapshot(&rejected, &pinned_policy, &ScorerPolicy::default());
        assert!(
            eligible_of(&pinned, fed_id(2)),
            "a user-owned pinned fed keeps the existing pin override"
        );
    }

    #[test]
    fn auto_joined_fed_is_never_auto_designated_as_spending() {
        let probes = vec![
            (fed_id(1), healthy_probe(10_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let auto_joined = BTreeSet::from([fed_id(1)]);
        let active_probes = BTreeMap::from([(fed_id(1), ActiveProbeVerdict::Passed)]);
        let snapshot = super::build_snapshot(
            &probes,
            &TickPolicy::default(),
            &ScorerPolicy::default(),
            &auto_joined,
            &active_probes,
        );
        assert_eq!(
            snapshot.spending_fed,
            Some(fed_id(2)),
            "auto spending must choose the non-auto-joined eligible fed"
        );
        assert_eq!(
            snapshot.standby_fed,
            Some(fed_id(1)),
            "a passed auto-joined fed remains eligible as a destination"
        );
    }

    #[test]
    fn auto_spending_excludes_a_pinned_standby() {
        // The operator pins ONLY the standby, to the fed that currently holds the MOST
        // spendable. The spending auto-pick must skip it and choose the next eligible fed,
        // never collapse both roles onto the pinned standby (a self-fund the allocator
        // silently no-ops, defeating the explicit standby designation).
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let policy = TickPolicy {
            standby_fed: Some(fed_id(1)),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        assert_eq!(snapshot.standby_fed, Some(fed_id(1)));
        assert_eq!(
            snapshot.spending_fed,
            Some(fed_id(2)),
            "spending must be distinct from the pinned standby, not the best-ranked fed"
        );
    }

    #[test]
    fn missing_pinned_feds_flags_an_unsensed_pin() {
        // Operator pins spending=fed 9, but fed 9 failed to probe this tick so it is absent from
        // the probe batch. `build_snapshot` still designates it (from the policy pin), but it is
        // NOT in `federations` — `missing_pinned_feds` must report it so the tick fails loudly
        // instead of silently reporting `decisions: none` / success to a scheduler.
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let policy = TickPolicy {
            spending_fed: Some(fed_id(9)),
            standby_fed: Some(fed_id(1)),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        assert_eq!(snapshot.spending_fed, Some(fed_id(9)));
        assert_eq!(missing_pinned_feds(&policy, &snapshot), vec![fed_id(9)]);
    }

    #[test]
    fn missing_pinned_feds_empty_when_pins_present_or_auto() {
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        // Both pins present -> nothing missing.
        let pinned = TickPolicy {
            spending_fed: Some(fed_id(1)),
            standby_fed: Some(fed_id(2)),
            ..TickPolicy::default()
        };
        let snap = build_snapshot(&probes, &pinned, &ScorerPolicy::default());
        assert!(missing_pinned_feds(&pinned, &snap).is_empty());
        // Fully auto-designated -> there are no explicit pins to honor, so nothing is ever
        // reported missing (an auto pick is only ever chosen from present probes).
        let auto = TickPolicy::default();
        let snap = build_snapshot(&probes, &auto, &ScorerPolicy::default());
        assert!(missing_pinned_feds(&auto, &snap).is_empty());
    }

    #[test]
    fn unusable_pinned_feds_flags_explicit_pins_only() {
        let mut no_gateway = healthy_probe(5_000_000);
        no_gateway.gateway_available = false;
        let mut no_lnv2 = healthy_probe(100_000);
        no_lnv2.has_lnv2 = false;
        no_lnv2.gateway_available = true;
        let mut dead_quorum = healthy_probe(100_000);
        dead_quorum.quorum_live = false;
        dead_quorum.gateway_available = true;
        let probes = vec![
            (fed_id(1), no_gateway),
            (fed_id(2), no_lnv2),
            (fed_id(3), dead_quorum),
            (fed_id(4), healthy_probe(100_000)),
        ];

        let policy = TickPolicy {
            spending_fed: Some(fed_id(1)),
            standby_fed: Some(fed_id(3)),
            ..TickPolicy::default()
        };
        assert_eq!(
            unusable_pinned_feds(&policy, &probes),
            vec![fed_id(1), fed_id(3)]
        );

        let policy = TickPolicy {
            spending_fed: Some(fed_id(2)),
            standby_fed: Some(fed_id(4)),
            ..TickPolicy::default()
        };
        assert_eq!(unusable_pinned_feds(&policy, &probes), vec![fed_id(2)]);

        assert!(
            unusable_pinned_feds(&TickPolicy::default(), &probes).is_empty(),
            "auto-designation has no explicit pins to fail loudly"
        );
    }

    #[test]
    fn pinned_input_problems_flag_absent_and_unusable_pins() {
        // fed 1 PROBED but has no usable gateway on the DEFAULT route (no `--gateway` anywhere in
        // this pure path). fed 3 PROBED but quorum is dead. fed 9 is ABSENT (never probed). fed 2
        // is healthy and routable.
        let mut no_gateway = healthy_probe(5_000_000);
        no_gateway.gateway_available = false;
        let mut dead_quorum = healthy_probe(100_000);
        dead_quorum.quorum_live = false;
        let probes = vec![
            (fed_id(1), no_gateway),
            (fed_id(2), healthy_probe(100_000)),
            (fed_id(3), dead_quorum),
        ];

        // Pin spending=1 (present but unusable) and standby=9 (absent): BOTH problems reported, so
        // the tick fails loudly instead of exiting 0 on the default-route `RefuseInflow`.
        let policy = TickPolicy {
            spending_fed: Some(fed_id(1)),
            standby_fed: Some(fed_id(9)),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        let decisions = decide(&snapshot, policy.occurrence);
        let problems = pinned_input_problems(
            &policy,
            &snapshot,
            &probes,
            &decisions,
            &[],
            &wallet_core::GoalBlockers::default(),
        );
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems
                .iter()
                .any(|p| p.contains("failed to probe") && p.contains(&fed_id(9).to_hex())),
            "{problems:?}"
        );
        assert!(
            problems.iter().any(
                |p| p.contains("failed the lnv2/probe gate") && p.contains(&fed_id(1).to_hex())
            ),
            "{problems:?}"
        );
        let held_problems = pinned_input_problems(
            &policy,
            &snapshot,
            &probes,
            &decisions,
            &[],
            &held_funding_blockers(fed_id(1), fed_id(9)),
        );
        assert!(
            held_problems
                .iter()
                .any(|p| p.contains("failed to probe") && p.contains(&fed_id(9).to_hex())),
            "a held source association must not relax a missing pin: {held_problems:?}"
        );

        // A pinned STANDBY whose quorum is DEAD (fed 3) is now EVACUATED, not pre-failed. As of
        // Phase 3.A a dead/unhealthy fed drives an `Evacuate` (drain it into `safest_other`), and
        // `fed_in_executable_move` treats that evacuate leg as route-validated — the evacuating
        // SOURCE is unhealthy BY DEFINITION, so its own red probe gate must not abort the evacuate.
        // The raw per-fed gate still SEES fed 3 as unusable, but `pinned_input_problems` clears it
        // because it drives an executable evacuate this tick. (A dead-quorum evacuate that genuinely
        // cannot complete surfaces loudly at EXECUTION via `summary.failed`, not this pure pre-gate.)
        let dead_quorum_pin = TickPolicy {
            spending_fed: Some(fed_id(2)),
            standby_fed: Some(fed_id(3)),
            ..TickPolicy::default()
        };
        let dead_quorum_snap = build_snapshot(&probes, &dead_quorum_pin, &ScorerPolicy::default());
        let dead_quorum_decisions = decide(&dead_quorum_snap, dead_quorum_pin.occurrence);
        // Sanity: the dead standby really is being evacuated into the healthy fed.
        assert!(
            dead_quorum_decisions.iter().any(|d| matches!(
                &d.action,
                Action::Evacuate { from, to, .. } if *from == fed_id(3) && *to == fed_id(2)
            )),
            "expected an evacuate draining the dead standby; decisions: {dead_quorum_decisions:?}"
        );
        // The raw probe gate still flags fed 3 (dead quorum)...
        assert!(unusable_pinned_feds(&dead_quorum_pin, &probes).contains(&fed_id(3)));
        // ...but because fed 3 drives an executable evacuate this tick, the tick does NOT pre-fail.
        assert!(
            pinned_input_problems(
                &dead_quorum_pin,
                &dead_quorum_snap,
                &probes,
                &dead_quorum_decisions,
                &[],
                &wallet_core::GoalBlockers::default(),
            )
            .is_empty(),
            "an evacuating pinned fed must not be pre-failed on its own probe gate"
        );

        // A present + routable pin is clean, even though a DIFFERENT (undesignated) fed is
        // unusable — only the operator's own pins are honored-or-failed.
        let ok = TickPolicy {
            spending_fed: Some(fed_id(2)),
            ..TickPolicy::default()
        };
        let ok_snap = build_snapshot(&probes, &ok, &ScorerPolicy::default());
        let ok_decisions = decide(&ok_snap, ok.occurrence);
        assert!(pinned_input_problems(
            &ok,
            &ok_snap,
            &probes,
            &ok_decisions,
            &[],
            &wallet_core::GoalBlockers::default(),
        )
        .is_empty());

        // Fully auto (no pins) never reports a problem, even with an unusable fed present:
        // auto-designation degrades safely by excluding it.
        let auto = TickPolicy::default();
        let auto_snap = build_snapshot(&probes, &auto, &ScorerPolicy::default());
        let auto_decisions = decide(&auto_snap, auto.occurrence);
        assert!(pinned_input_problems(
            &auto,
            &auto_snap,
            &probes,
            &auto_decisions,
            &[],
            &wallet_core::GoalBlockers::default(),
        )
        .is_empty());
    }

    #[test]
    fn a_held_rebalance_without_a_current_same_source_recurrence_does_not_relax_the_raw_pin_gate() {
        // A durable route is not an uncorrelated raw-pin voucher. A current recurrence must pair
        // the same goal and source; otherwise an old route can hide a policy-rotated pin.
        let mut no_gateway = healthy_probe(100_000);
        no_gateway.gateway_available = false;

        for (shape, blockers) in [
            ("funding", held_funding_blockers(fed_id(1), fed_id(2))),
            ("evacuation", held_evacuation_blockers(fed_id(1), fed_id(2))),
        ] {
            let destination_pinned = TickPolicy {
                standby_fed: Some(fed_id(2)),
                ..TickPolicy::default()
            };
            let destination_probes = vec![
                (fed_id(1), healthy_probe(100_000)),
                (fed_id(2), no_gateway.clone()),
            ];
            let destination_snapshot = build_snapshot(
                &destination_probes,
                &destination_pinned,
                &ScorerPolicy::default(),
            );
            let destination_problems = pinned_input_problems(
                &destination_pinned,
                &destination_snapshot,
                &destination_probes,
                &[],
                &[],
                &blockers,
            );
            assert!(
                destination_problems.iter().any(
                    |problem| problem.contains("failed the lnv2/probe gate")
                        && problem.contains(&fed_id(2).to_hex())
                ),
                "a held {shape} destination must not relax a stale raw pin: {destination_problems:?}"
            );

            let source_pinned = TickPolicy {
                spending_fed: Some(fed_id(1)),
                ..TickPolicy::default()
            };
            let source_probes = vec![
                (fed_id(1), no_gateway.clone()),
                (fed_id(2), healthy_probe(100_000)),
            ];
            let source_snapshot =
                build_snapshot(&source_probes, &source_pinned, &ScorerPolicy::default());
            assert!(
                !pinned_input_problems(
                    &source_pinned,
                    &source_snapshot,
                    &source_probes,
                    &[],
                    &[],
                    &blockers,
                )
                .is_empty(),
                "a held {shape} source alone must not relax a raw pin"
            );
        }
    }

    #[test]
    fn raw_pin_vouchers_are_associated_and_an_admitted_endpoint_wins() {
        let mut unavailable_a = healthy_probe(100_000);
        unavailable_a.gateway_available = false;
        let probes = vec![
            (fed_id(1), unavailable_a),
            (fed_id(2), healthy_probe(100_000)),
            (fed_id(3), healthy_probe(100_000)),
        ];
        let policy = TickPolicy {
            spending_fed: Some(fed_id(1)),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        assert!(unusable_pinned_feds(&policy, &probes).contains(&fed_id(1)));

        let funding_advisory = AllocatorDecision {
            action: Action::RefuseInflow {
                fed: fed_id(2),
                reason: ReasonCode::SpendingBelowTarget,
                diagnostics: wallet_core::RefusalDiagnostics {
                    source: Some(fed_id(1)),
                    ..Default::default()
                },
            },
            reason: ReasonCode::SpendingBelowTarget,
            occurrence: Occurrence(2),
            idempotency_key: IdempotencyKey("refuse:funding-advisory".to_owned()),
        };
        let held_a_to_b = held_funding_blockers(fed_id(1), fed_id(2));
        assert!(
            pinned_input_problems(
                &policy,
                &snapshot,
                &probes,
                std::slice::from_ref(&funding_advisory),
                &[],
                &held_a_to_b,
            )
            .is_empty(),
            "a held funding advisory vouches only for its paired source"
        );
        let held_c_to_b = held_funding_blockers(fed_id(3), fed_id(2));
        assert!(
            !pinned_input_problems(
                &policy,
                &snapshot,
                &probes,
                &[funding_advisory],
                &[],
                &held_c_to_b,
            )
            .is_empty(),
            "a same-goal holder with a different source must not form a cross-product voucher"
        );

        let evacuation_advisory = AllocatorDecision {
            action: Action::RefuseInflow {
                fed: fed_id(1),
                reason: ReasonCode::ShutdownNotice,
                diagnostics: Default::default(),
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence: Occurrence(2),
            idempotency_key: IdempotencyKey("refuse:empty-evacuation".to_owned()),
        };
        assert!(
            pinned_input_problems(
                &policy,
                &snapshot,
                &probes,
                &[evacuation_advisory],
                &[],
                &held_evacuation_blockers(fed_id(1), fed_id(2)),
            )
            .is_empty(),
            "a held empty evacuation advisory vouches for its own source"
        );

        let suppressed_a_to_b = AllocatorDecision {
            action: Action::Move {
                from: fed_id(1),
                to: fed_id(2),
                amount: Msat(1),
                fee_cap: Msat(0),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence: Occurrence(2),
            idempotency_key: IdempotencyKey("move:suppressed-a-b".to_owned()),
        };
        assert!(
            pinned_input_problems(
                &policy,
                &snapshot,
                &probes,
                &[],
                std::slice::from_ref(&suppressed_a_to_b),
                &held_a_to_b,
            )
            .is_empty(),
            "a suppressed recurrence vouches only when its blocking holder has the same source"
        );
        let suppressed_a_to_b_with_resourced_holder = AllocatorDecision {
            action: Action::Move {
                from: fed_id(1),
                to: fed_id(2),
                amount: Msat(1),
                fee_cap: Msat(0),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence: Occurrence(2),
            idempotency_key: IdempotencyKey("move:resourced-a-b".to_owned()),
        };
        assert!(
            !pinned_input_problems(
                &policy,
                &snapshot,
                &probes,
                &[],
                &[suppressed_a_to_b_with_resourced_holder],
                &held_c_to_b,
            )
            .is_empty(),
            "a re-sourced holder must not relax a suppressed candidate's source"
        );

        let receive_gate = AllocatorDecision {
            // The old policy's held A -> B funding goal does not vouch for this rotated policy's
            // fresh attempt to fund unusable A: its current receive gate must stay loud.
            action: Action::RefuseInflow {
                fed: fed_id(1),
                reason: ReasonCode::NotProbed,
                diagnostics: wallet_core::RefusalDiagnostics {
                    want: Some(Msat(1)),
                    max_fee_bps: Some(100),
                    ..Default::default()
                },
            },
            reason: ReasonCode::NotProbed,
            occurrence: Occurrence(2),
            idempotency_key: IdempotencyKey("refuse:not-probed-a".to_owned()),
        };
        assert!(has_funding_receive_gate_refusal(
            fed_id(1),
            std::slice::from_ref(&receive_gate)
        ));
        assert!(
            pinned_input_problems(
                &policy,
                &snapshot,
                &probes,
                std::slice::from_ref(&receive_gate),
                std::slice::from_ref(&suppressed_a_to_b),
                &held_a_to_b,
            )
            .is_empty(),
            "the exact current suppressed source voucher outranks a simultaneous receive refusal"
        );
        assert!(
            pinned_input_problems(
                &policy,
                &snapshot,
                &probes,
                &[suppressed_a_to_b, receive_gate],
                &[],
                &held_a_to_b,
            )
            .is_empty(),
            "a current admitted endpoint must beat the concurrent coarse receive-gate refusal"
        );
    }

    #[test]
    fn pinned_input_problems_allow_a_source_only_pin_without_its_own_gateway() {
        // A rebalance `A -> B` routes through B's gateway (spec §7's shared-gateway swap; the
        // executor even proves that gateway serves the source before minting), so a pinned SOURCE
        // needs only live quorum + B's gateway to serve it — NOT its own registered gateway. Here
        // spending A is pinned, holds a surplus, and has NO usable gateway of its own
        // (`probed_ok == false`), while standby B is under target and healthy. The tick funds B
        // FROM A, so A drives an executable `Move` whose route the runtime already validated; the
        // pin must therefore NOT fail the tick even though A's own raw probe gate is red.
        let mut source_no_gateway = healthy_probe(5_000_000);
        source_no_gateway.gateway_available = false; // -> probed_ok false, but quorum still live
        let probes = vec![
            (fed_id(1), source_no_gateway),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let policy = TickPolicy {
            per_fed_cap: Msat(100_000_000),
            target_spending_balance: Msat(1_000_000),
            standby_target: Msat(2_000_000),
            max_fee: Msat(10_000),
            spending_fed: Some(fed_id(1)),
            standby_fed: Some(fed_id(2)),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        let decisions = decide(&snapshot, policy.occurrence);
        // Sanity: the rebalance really is a `Move A -> B` with A (the gateway-less pin) as source.
        assert!(
            move_between(&decisions, fed_id(1), fed_id(2)).is_some(),
            "expected a fund-standby Move fed1 -> fed2; decisions: {decisions:?}"
        );
        // The raw per-fed probe gate flags A (it has no gateway of its own)...
        assert_eq!(unusable_pinned_feds(&policy, &probes), vec![fed_id(1)]);
        // ...but because A drives an executable Move this tick, the tick does NOT fail on it.
        assert!(
            pinned_input_problems(
                &policy,
                &snapshot,
                &probes,
                &decisions,
                &[],
                &wallet_core::GoalBlockers::default(),
            )
            .is_empty(),
            "a source-only pin whose rebalance is actually running must not fail the tick"
        );
    }

    #[test]
    fn evacuate_decisions_are_applied() {
        // An unhealthy fed with a healthy destination makes `decide` emit an `Evacuate`.
        // As of Phase 3.A the tick MUST drive it (the executor performs evacuate as a
        // send-required move), so `decisions_to_apply` KEEPS it — draining the dying fed.
        let mut sick = healthy_probe(5_000_000);
        sick.quorum_live = false; // -> unhealthy -> evacuation_reason
        let probes = vec![(fed_id(1), sick), (fed_id(2), healthy_probe(100_000))];
        let policy = TickPolicy {
            per_fed_cap: Msat(100_000_000),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        let decisions = decide(&snapshot, policy.occurrence);
        assert!(
            decisions
                .iter()
                .any(|d| matches!(&d.action, Action::Evacuate { from, to, .. }
                    if *from == fed_id(1) && *to == fed_id(2))),
            "expected an evacuate decision draining the unhealthy fed; decisions: {decisions:?}"
        );
        let applied = decisions_to_apply(&decisions);
        assert!(
            applied
                .iter()
                .any(|d| matches!(&d.action, Action::Evacuate { from, .. } if *from == fed_id(1))),
            "evacuate must reach what the tick applies; applied: {applied:?}"
        );

        // ...while advisory decisions still pass through here unchanged — they are dropped only
        // at `apply` (via `Action::is_executable`), not by this projection. The under-target
        // spending fed with no usable source yields a `RefuseInflow`, which survives here.
        let advisory_count = |ds: &[AllocatorDecision]| {
            ds.iter()
                .filter(|d| matches!(&d.action, Action::RefuseInflow { .. }))
                .count()
        };
        assert!(advisory_count(&decisions) > 0, "decisions: {decisions:?}");
        assert_eq!(
            advisory_count(&applied),
            advisory_count(&decisions),
            "advisory decisions pass through decisions_to_apply unchanged; applied: {applied:?}"
        );
    }

    #[test]
    fn tiny_evacuate_decision_is_still_applied() {
        // `decisions_to_apply` must not apply its own dust policy. A small evacuation may come
        // from destination cap room rather than source balance, and the Phase 3.A contract is
        // that EVERY Evacuate reaches the executor.
        let tiny = AllocatorDecision {
            action: Action::Evacuate {
                from: fed_id(1),
                to: fed_id(2),
                amount: Msat(1),
                fee_cap: Msat(50_000),
                gateway: None,
                fee_cap_components: None,
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence: Occurrence(0),
            idempotency_key: wallet_core::IdempotencyKey("evac-tiny".into()),
        };
        let kept = decisions_to_apply(std::slice::from_ref(&tiny));
        assert_eq!(kept, vec![tiny]);
    }

    #[test]
    fn explicit_designation_overrides_auto() {
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let policy = TickPolicy {
            spending_fed: Some(fed_id(2)),
            standby_fed: Some(fed_id(1)),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        assert_eq!(snapshot.spending_fed, Some(fed_id(2)));
        assert_eq!(snapshot.standby_fed, Some(fed_id(1)));
    }

    #[test]
    fn build_snapshot_then_decide_funds_standby() {
        // Spending fed is well funded, standby is under target: decide funds the standby
        // from the spending fed's SURPLUS (its balance above the spending target).
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let policy = TickPolicy {
            per_fed_cap: Msat(100_000_000),
            target_spending_balance: Msat(1_000_000),
            standby_target: Msat(2_000_000),
            max_fee: Msat(10_000),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        assert_eq!(snapshot.spending_fed, Some(fed_id(1)));
        assert_eq!(snapshot.standby_fed, Some(fed_id(2)));

        let decisions = decide(&snapshot, policy.occurrence);
        // want = standby_target - standby.spendable = 2_000_000 - 100_000 = 1_900_000;
        // available surplus = spending.spendable - target = 5_000_000 - 1_000_000 = 4M;
        // amount = min(want, cap_room, surplus) = 1_900_000.
        assert_eq!(
            move_between(&decisions, fed_id(1), fed_id(2)),
            Some((Msat(1_900_000), ReasonCode::StandbyBelowTarget)),
            "decisions: {decisions:?}"
        );
    }

    #[test]
    fn build_snapshot_then_decide_tops_up_spending() {
        // Explicit designation: the spending fed is under target and the standby is
        // funded — decide tops the spending fed up from the standby.
        let probes = vec![
            (fed_id(1), healthy_probe(5_000_000)),
            (fed_id(2), healthy_probe(100_000)),
        ];
        let policy = TickPolicy {
            per_fed_cap: Msat(100_000_000),
            target_spending_balance: Msat(1_000_000),
            standby_target: Msat(0),
            max_fee: Msat(10_000),
            spending_fed: Some(fed_id(2)),
            standby_fed: Some(fed_id(1)),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        let decisions = decide(&snapshot, policy.occurrence);
        // want = target - spending.spendable = 1_000_000 - 100_000 = 900_000; the standby
        // has 5M available, so the whole 900_000 is topped up from it.
        assert_eq!(
            move_between(&decisions, fed_id(1), fed_id(2)),
            Some((Msat(900_000), ReasonCode::SpendingBelowTarget)),
            "decisions: {decisions:?}"
        );
    }

    #[test]
    fn over_cap_fed_yields_refuse_decision() {
        // A fed already above the per-fed cap is a cap violation the allocator refuses
        // (advisory RefuseInflow/OverCap), regardless of designation.
        let probes = vec![(fed_id(1), healthy_probe(5_000_000))];
        let policy = TickPolicy {
            per_fed_cap: Msat(1_000_000),
            target_spending_balance: Msat(100_000),
            standby_target: Msat(0),
            ..TickPolicy::default()
        };
        let snapshot = build_snapshot(&probes, &policy, &ScorerPolicy::default());
        let decisions = decide(&snapshot, policy.occurrence);
        assert!(
            decisions.iter().any(|d| matches!(
                &d.action,
                Action::RefuseInflow {
                    fed,
                    reason: ReasonCode::OverCap,
                    ..
                } if *fed == fed_id(1)
            )),
            "expected an over-cap refusal for fed 1; decisions: {decisions:?}"
        );
    }
}
