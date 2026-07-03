//! The orchestrator "decide" glue (Phase 2 step 2.2): the standing-instruction
//! [`TickPolicy`] and the PURE [`build_snapshot`] that turns a batch of recorded
//! [`ProbeResult`]s into the `wallet_core::AllocatorSnapshot` the pure allocator
//! (`wallet_core::decide`) consumes (`docs/phase2-plan.md`).
//!
//! Split like [`crate::probe`]: everything here is PURE (no I/O, no async, no
//! fedimint types) so it sits in the fast rb-lite gate — a recorded `ProbeResult`
//! fixture + a policy → `build_snapshot` → `decide` yields the expected decisions.
//! The I/O that actually probes the live feds and drives the executor is
//! [`crate::runtime::Runtime::tick`] / [`crate::runtime::Runtime::status`].
//!
//! ## Standing instruction (ADR-0009)
//! [`TickPolicy`] is the user's standing instruction: the per-fed cap (ADR-0018), the
//! spending/standby targets, the move fee cap, and the spending/standby designation.
//! The designation is OPTIONAL — when the operator does not pin it, `build_snapshot`
//! AUTO-designates from the SCORED-eligible feds: rank first, then spendable balance,
//! then federation id for determinism. An INELIGIBLE fed (no-lnv2 / unhealthy / wrong
//! network / probe-failed) is never auto-designated, though it still appears in the
//! snapshot's `federations` so the allocator can see an over-cap balance sitting on it.

use crate::probe::{assemble_facts, assemble_status, ProbeResult};
use wallet_core::{
    score, Action, AllocatorDecision, AllocatorSnapshot, ExecutionSummary, FederationId,
    FederationStatus, FederationVerdict, Msat, Occurrence, ScorerPolicy,
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
/// Default per-move fee cap: 50 sats (50_000 msat). A no-surprises bound on a single
/// rebalance's total (both-legs) cost; tighten it with `--max-fee`.
const DEFAULT_MAX_FEE: Msat = Msat(50_000);

/// The standing instruction for one orchestrator tick (ADR-0009). Sensible v1 defaults
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
    /// Per-move fee cap stamped onto every rebalance `Move` this tick emits.
    pub max_fee: Msat,
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
}

impl Default for TickPolicy {
    fn default() -> Self {
        Self {
            per_fed_cap: DEFAULT_PER_FED_CAP,
            target_spending_balance: DEFAULT_TARGET_SPENDING_BALANCE,
            standby_target: DEFAULT_STANDBY_TARGET,
            max_fee: DEFAULT_MAX_FEE,
            occurrence: Occurrence(0),
            spending_fed: None,
            standby_fed: None,
            now: 0,
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
) -> AllocatorSnapshot {
    let federations: Vec<FederationStatus> = probes
        .iter()
        .map(|(id, probe)| assemble_status(probe, *id))
        .collect();

    // Scored-eligible feds ranked for auto-designation: keep only the fundable ones
    // (the pure scorer gates on structural + probe facts), ordered by rank score first.
    // `spendable` is a secondary tie-breaker so equal-quality feds prefer the one already
    // holding the most usable balance; id makes the choice deterministic.
    let mut eligible: Vec<(FederationId, u32, u64)> = probes
        .iter()
        .filter_map(|(id, probe)| {
            let verdict = score(&assemble_facts(probe, *id), scorer_policy);
            verdict
                .eligible_to_fund
                .then_some((*id, verdict.rank_score, probe.spendable_msat))
        })
        .collect();
    eligible.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.2.cmp(&a.2))
            .then_with(|| a.0.cmp(&b.0))
    });

    // Auto spending is the best-ranked eligible fed that is NOT an operator-pinned standby.
    // Excluding the pinned standby matters when the operator pins ONLY `--standby` and that fed
    // ranks highest: without the guard, both roles would collapse onto it and the allocator would
    // silently no-op the rebalance the operator explicitly designated a standby for.
    let spending_fed = policy.spending_fed.or_else(|| {
        eligible
            .iter()
            .map(|(id, _, _)| *id)
            .find(|id| Some(*id) != policy.standby_fed)
    });
    // Auto standby is the best-ranked eligible fed that is NOT the spending fed,
    // so an operator-pinned spending fed still gets a distinct auto standby.
    let standby_fed = policy.standby_fed.or_else(|| {
        eligible
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
        now: policy.now,
    }
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

/// The operator-pinned inputs a tick could NOT honor this pass, each as a human-readable problem
/// line (empty when every pin is usable). Two PIN-ONLY failure modes — an auto pick is only ever
/// chosen from usable probes, so it is never reported:
/// - a pin ABSENT from the snapshot (its probe failed this tick — [`missing_pinned_feds`]);
/// - a pin PRESENT but unusable for lnv2 moves this tick — no lnv2 module, dead quorum, or no
///   usable gateway route ([`unusable_pinned_feds`]) — UNLESS it already drives an executable
///   `Move` in `decisions`, in which case its rebalance is genuinely running (see below).
///
/// The `decisions` refinement fixes a source-only pin's false bail: a rebalance `A -> B` routes
/// through the DESTINATION's gateway (spec §7's shared-gateway internal swap — the executor even
/// proves that gateway serves the source before minting), so a pinned SOURCE `A` needs only live
/// quorum and `B`'s gateway to serve it, NOT its own registered gateway. The runtime's `plan_tick`
/// already validated that exact end-to-end route before emitting the `Move`, so a pinned fed
/// appearing as the `from`/`to` of a surviving `Move` is provably usable this tick even when its
/// own `probed_ok` proxy (which reads only its first registered gateway) is false. Only a pin whose
/// rebalance did NOT survive as an executable `Move` is failed on the coarser raw probe gate.
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
    decisions: &[AllocatorDecision],
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
    // A pin that already drives an executable `Move` this tick is usable by construction — its
    // full route was validated before the move was emitted — so drop it from the raw probe-gate
    // failures. Otherwise a source-only pin (which routes through the destination's gateway)
    // falsely fails the tick on its own missing registered gateway.
    let unusable: Vec<FederationId> = unusable_pinned_feds(policy, probes)
        .into_iter()
        .filter(|id| !fed_in_executable_move(*id, decisions))
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

/// Whether `id` is the source or destination of an executable `Move` in `decisions` — a rebalance
/// leg that will actually reach `apply` (advisory `RefuseInflow`/`Cap` never do, and `Evacuate` is
/// withheld by [`decisions_to_apply`]). A pinned fed in such a move had its end-to-end route
/// validated by the runtime, so it is not failed on the coarser per-fed probe gate.
fn fed_in_executable_move(id: FederationId, decisions: &[AllocatorDecision]) -> bool {
    decisions
        .iter()
        .any(|d| matches!(&d.action, Action::Move { from, to, .. } if *from == id || *to == id))
}

/// Comma-join federation ids as hex for a diagnostic message.
fn hexes(ids: &[FederationId]) -> String {
    ids.iter()
        .map(|id| id.to_hex())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The decisions a Phase-2 tick actually drives through the executor. PURE, total over
/// the input. `Move`/`DirectInflow` stay; the advisory `RefuseInflow`/`Cap` stay too (the
/// pure `apply` already skips them via `Action::is_executable`, and the tick surfaces them
/// from the full decision list). `Evacuate` is DROPPED here: it is executable per
/// `is_executable`, but the Phase-1 executor still returns `Unsupported` for it and
/// evacuation EXECUTION is a Phase-3 non-goal — driving it would only record a misleading
/// terminal `Failed` intent and poison its idempotency key for the rest of the occurrence.
/// Callers keep the FULL decision list for reporting; this only trims what reaches `apply`.
pub fn decisions_to_apply(decisions: &[AllocatorDecision]) -> Vec<AllocatorDecision> {
    decisions
        .iter()
        .filter(|d| !matches!(d.action, Action::Evacuate { .. }))
        .cloned()
        .collect()
}

/// One federation's scored view for the `status` (dry-run) report: its fundability
/// verdict plus the allocator status assembled from the same probe.
#[derive(Clone, Debug, PartialEq)]
pub struct ScoredFed {
    pub id: FederationId,
    pub verdict: FederationVerdict,
    pub status: FederationStatus,
}

/// The result of [`crate::runtime::Runtime::tick`]: the decisions the allocator produced
/// and the [`ExecutionSummary`] from applying them (advisory `RefuseInflow`/`Cap`
/// decisions are surfaced here but count as `skipped` by `apply`, never executed).
#[derive(Clone, Debug)]
pub struct TickReport {
    pub decisions: Vec<AllocatorDecision>,
    pub summary: ExecutionSummary,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use wallet_core::{decide, ReasonCode};

    fn fed_id(byte: u8) -> FederationId {
        FederationId([byte; 32])
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
        let problems = pinned_input_problems(&policy, &snapshot, &probes, &decisions);
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

        let dead_quorum_pin = TickPolicy {
            spending_fed: Some(fed_id(2)),
            standby_fed: Some(fed_id(3)),
            ..TickPolicy::default()
        };
        let dead_quorum_snap = build_snapshot(&probes, &dead_quorum_pin, &ScorerPolicy::default());
        let dead_quorum_decisions = decide(&dead_quorum_snap, dead_quorum_pin.occurrence);
        let problems = pinned_input_problems(
            &dead_quorum_pin,
            &dead_quorum_snap,
            &probes,
            &dead_quorum_decisions,
        );
        assert!(
            problems.iter().any(|p| {
                p.contains("failed the lnv2/probe gate") && p.contains(&fed_id(3).to_hex())
            }),
            "{problems:?}"
        );

        // A present + routable pin is clean, even though a DIFFERENT (undesignated) fed is
        // unusable — only the operator's own pins are honored-or-failed.
        let ok = TickPolicy {
            spending_fed: Some(fed_id(2)),
            ..TickPolicy::default()
        };
        let ok_snap = build_snapshot(&probes, &ok, &ScorerPolicy::default());
        let ok_decisions = decide(&ok_snap, ok.occurrence);
        assert!(pinned_input_problems(&ok, &ok_snap, &probes, &ok_decisions).is_empty());

        // Fully auto (no pins) never reports a problem, even with an unusable fed present:
        // auto-designation degrades safely by excluding it.
        let auto = TickPolicy::default();
        let auto_snap = build_snapshot(&probes, &auto, &ScorerPolicy::default());
        let auto_decisions = decide(&auto_snap, auto.occurrence);
        assert!(pinned_input_problems(&auto, &auto_snap, &probes, &auto_decisions).is_empty());
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
            pinned_input_problems(&policy, &snapshot, &probes, &decisions).is_empty(),
            "a source-only pin whose rebalance is actually running must not fail the tick"
        );
    }

    #[test]
    fn evacuate_decisions_are_not_applied() {
        // An unhealthy fed with a healthy destination makes `decide` emit an `Evacuate`.
        // The Phase-2 tick must NOT drive it (evacuation execution is a Phase-3 non-goal and
        // the executor returns `Unsupported`), so `decisions_to_apply` drops it — while the
        // full decision list the tick reports still carries it.
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
                .any(|d| matches!(&d.action, Action::Evacuate { from, .. } if *from == fed_id(1))),
            "expected an evacuate decision for the unhealthy fed; decisions: {decisions:?}"
        );
        let applied = decisions_to_apply(&decisions);
        assert!(
            !applied
                .iter()
                .any(|d| matches!(d.action, Action::Evacuate { .. })),
            "evacuate must be filtered out of what the tick applies; applied: {applied:?}"
        );
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
                } if *fed == fed_id(1)
            )),
            "expected an over-cap refusal for fed 1; decisions: {decisions:?}"
        );
    }
}
