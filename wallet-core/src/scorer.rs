//! Pure federation scorer: turns per-federation facts into a fundability verdict.
//!
//! Encodes the trust model from the ADRs:
//! - ADR-0017: empirical probes GATE; reputation/priors only rank-or-demote and may
//!   NEVER promote a federation past the probe floor.
//! - ADR-0019: TRUST derives only from the authenticated `ClientConfig` (guardian
//!   count → threshold, module set, network) and the wallet's OWN probes. Everything
//!   else (Observer, meta, Nostr) is a hint.
//! - ADR-0020: the Fedimint Observer is an UNTRUSTED prior used only behind the gate;
//!   it may rank or demote among probe-passed feds, never fund or block one.
//!
//! This is pure Rust: no I/O, no async, no networking, no floats. Facts are GIVEN.

use crate::probe::ActiveProbeVerdict;
use crate::FederationId;

/// Fedimint consensus module kinds we care about for fundability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Module {
    Mint,
    Ln,
    Lnv2,
    Wallet,
    Meta,
    Other,
}

/// The UNTRUSTED Observer prior (ADR-0020). Integers only so scoring stays exact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverPrior {
    /// Aggregate guardian uptime, in per-mille (0..=1000).
    pub uptime_permille: u16,
    /// Backing balance from the `/utxos` sum (NOT the buggy `deposits` field).
    pub backing_sats: u64,
    /// Activity count over the last 7 days.
    pub activity_7d: u32,
}

/// Everything the scorer is allowed to look at for one federation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FederationFacts {
    pub id: FederationId,
    // Structural facts from the authenticated config (ADR-0019):
    pub guardian_count: u32,
    pub threshold: u32,
    pub is_mainnet: bool,
    pub modules: Vec<Module>,
    // Our own empirical probes (the trust gate, ADR-0017):
    pub quorum_live: bool,
    pub round_trip_ok: bool,
    pub peg_out_quotable: bool,
    pub latency_ms: u32,
    // Lifecycle:
    pub shutdown_scheduled: bool,
    // Fedimint Lightning v2 (T16): a fed with no LNv2 cannot send/receive at all, so
    // this gates eligibility unconditionally rather than through `required_modules`.
    pub has_lnv2: bool,
    // Untrusted prior, used only behind the gate (ADR-0020):
    pub observer: Option<ObserverPrior>,
    /// The ACTIVE probe verdict (phase 5 §5.0.6), evaluated against the snapshot's
    /// designated SPENDING fed (the pair 5.1's gate must trust). `None` = never probed /
    /// no designated source — never a rejection by itself. 5.0 surfaces it only; the
    /// scorer deliberately does NOT read it (the `Discovered`-fed gate is 5.1's wire-up).
    pub active_probe: Option<ActiveProbeVerdict>,
}

/// Tunable structural floor. `Default` is the v1 policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScorerPolicy {
    pub min_guardians: u32,
    pub min_threshold: u32,
    pub require_mainnet: bool,
    pub required_modules: Vec<Module>,
}

impl Default for ScorerPolicy {
    fn default() -> Self {
        Self {
            min_guardians: 4,
            min_threshold: 3,
            require_mainnet: true,
            required_modules: vec![Module::Mint, Module::Wallet],
        }
    }
}

/// Why a federation was rejected or demoted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReasonCode {
    NoFaultTolerance,
    TooFewGuardians,
    WrongNetwork,
    MissingModule,
    ProbeFailed,
    ShutdownScheduled,
    LowObserverUptime,
    NoLnv2,
    /// The claimed `m`-of-`n` threshold is structurally dishonest: `m == 0`,
    /// `m > n`, or `m` below the BFT bound `n − (n−1)/3`. The scorer is the trust
    /// boundary (§1) and later discovery assemblers will feed it attacker-influenced
    /// facts, so a config claiming a weaker-than-BFT threshold (e.g. 3-of-100) is
    /// rejected rather than ranked equal to an honest 3-of-4.
    InvalidThreshold,
}

/// The scorer's verdict for one federation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FederationVerdict {
    pub eligible_to_fund: bool,
    /// Only meaningful when `eligible_to_fund` is true; `0` otherwise.
    pub rank_score: u32,
    pub reasons: Vec<ReasonCode>,
}

// Scoring constants (integers; keep golden tests exact).
const STRUCTURAL_WEIGHT: u32 = 100;
const PEG_OUT_BONUS: u32 = 50;
const HIGH_UPTIME_PERMILLE: u16 = 900;
const LOW_UPTIME_DEMOTION: u32 = 100;
const MAX_OBSERVER_PRIOR_BONUS: u32 = STRUCTURAL_WEIGHT - 1;

/// Score one federation. PURE: deterministic, no I/O, no floats.
pub fn score(facts: &FederationFacts, policy: &ScorerPolicy) -> FederationVerdict {
    let mut reasons = Vec::new();

    // ---- Structural floor: hard rejects (collect every failing reason). ----
    let mut floor_ok = true;

    // A 1-of-1 "federation" has no fault tolerance at all (ADR-0019, "Code Orange").
    if facts.guardian_count < 2 {
        reasons.push(ReasonCode::NoFaultTolerance);
        floor_ok = false;
    } else if facts.guardian_count < policy.min_guardians || facts.threshold < policy.min_threshold
    {
        reasons.push(ReasonCode::TooFewGuardians);
        floor_ok = false;
    }

    // Threshold trust floor (§1): the m-of-n threshold must be structurally honest.
    // Reject a nonsensical bound (m == 0 or m > n) and any threshold below fedimint's
    // own BFT bound `n − (n−1)/3` (the SDK's `NumPeers::threshold`). Every real
    // federation satisfies this exactly, so nothing live is rejected; a discovered
    // config CLAIMING a weaker threshold is rejected as dishonest. `bft_threshold` is
    // SATURATING so this check still executes (never panics) for attacker-supplied
    // facts including `guardian_count == 0`.
    if facts.threshold == 0
        || facts.threshold > facts.guardian_count
        || facts.threshold < bft_threshold(facts.guardian_count)
    {
        reasons.push(ReasonCode::InvalidThreshold);
        floor_ok = false;
    }

    if policy.require_mainnet && !facts.is_mainnet {
        reasons.push(ReasonCode::WrongNetwork);
        floor_ok = false;
    }

    if policy
        .required_modules
        .iter()
        .any(|m| !facts.modules.contains(m))
    {
        reasons.push(ReasonCode::MissingModule);
        floor_ok = false;
    }

    // Do not fund a federation that is winding down.
    if facts.shutdown_scheduled {
        reasons.push(ReasonCode::ShutdownScheduled);
        floor_ok = false;
    }

    // A fed with no Lightning v2 cannot send or receive at all (T16): unconditional,
    // like the guardian-count floor above, not a tunable `ScorerPolicy` toggle.
    if !facts.has_lnv2 {
        reasons.push(ReasonCode::NoLnv2);
        floor_ok = false;
    }

    // ---- Probe gate: hard reject (ADR-0017). ----
    let gate_ok = facts.quorum_live && facts.round_trip_ok;
    if !gate_ok {
        reasons.push(ReasonCode::ProbeFailed);
    }

    // Eligibility = floor AND gate. The Observer prior is BEHIND the gate and
    // cannot enter this decision: a probe-failed fed stays ineligible regardless.
    let eligible_to_fund = floor_ok && gate_ok;

    // Rank is only meaningful for eligible feds; otherwise it is forced to 0 so an
    // untrusted prior can never lift a rejected fed above an eligible one.
    let rank_score = if eligible_to_fund {
        rank(facts, &mut reasons)
    } else {
        0
    };

    FederationVerdict {
        eligible_to_fund,
        rank_score,
        reasons,
    }
}

/// Fedimint's own BFT threshold for `n` guardians: `n − f` with `f = (n−1)/3`
/// (`NumPeers::threshold`). SATURATING so it never panics on attacker-supplied facts
/// (`n == 0` yields `0`); the structural floor rejects `n == 0` via `NoFaultTolerance`.
fn bft_threshold(n: u32) -> u32 {
    n.saturating_sub(n.saturating_sub(1) / 3)
}

/// Deterministic integer rank for an eligible federation. May push `LowObserverUptime`.
fn rank(facts: &FederationFacts, reasons: &mut Vec<ReasonCode>) -> u32 {
    // Structural strength: the m-of-n threshold is the security parameter. Clamp the
    // term to `guardian_count` (defense-in-depth — the structural floor already rejects
    // `threshold > guardian_count`, so an eligible fed can never reach here with one).
    let mut score = facts
        .threshold
        .min(facts.guardian_count)
        .saturating_mul(STRUCTURAL_WEIGHT);

    // A quotable peg-out path is extra own-probe confidence.
    if facts.peg_out_quotable {
        score = score.saturating_add(PEG_OUT_BONUS);
    }

    // Latency penalty (saturating): slower federations rank lower.
    score = score.saturating_sub(facts.latency_ms / 10);

    // Untrusted Observer prior contributes ONLY when present (ADR-0020). A missing
    // prior is never a rejection; it just adds nothing. Cap the whole prior below
    // one structural threshold step so untrusted data stays a low-weight hint.
    if let Some(observer) = &facts.observer {
        score = score.saturating_add(observer_bonus(observer));

        // Low aggregate uptime demotes (never below an identical high-uptime fed),
        // but does not gate eligibility.
        if observer.uptime_permille < HIGH_UPTIME_PERMILLE {
            reasons.push(ReasonCode::LowObserverUptime);
            score = score.saturating_sub(LOW_UPTIME_DEMOTION);
        }
    }

    score
}

/// Capped Observer contribution. This is intentionally smaller than one threshold
/// step because the Observer is an untrusted prior, not a trust signal.
fn observer_bonus(observer: &ObserverPrior) -> u32 {
    let uptime_bonus = u32::from(observer.uptime_permille.min(1000)) / 20;
    uptime_bonus
        .saturating_add(backing_bonus(observer.backing_sats))
        .saturating_add(activity_bonus(observer.activity_7d))
        .min(MAX_OBSERVER_PRIOR_BONUS)
}

/// Small tiered bonus for Observer-reported backing balance (from the `/utxos` sum).
fn backing_bonus(backing_sats: u64) -> u32 {
    match backing_sats {
        s if s >= 1_000_000_000 => 30, // >= 10 BTC
        s if s >= 100_000_000 => 20,   // >= 1 BTC
        s if s >= 10_000_000 => 10,    // >= 0.1 BTC
        _ => 0,
    }
}

/// Small tiered bonus for Observer-reported 7-day activity.
fn activity_bonus(activity_7d: u32) -> u32 {
    match activity_7d {
        a if a >= 1000 => 20,
        a if a >= 100 => 10,
        a if a >= 10 => 5,
        _ => 0,
    }
}
