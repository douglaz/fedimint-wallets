//! The ACTIVE probe's pure verdict (phase 5 §5.0.3): a sustained-window pass over a
//! bounded, durable attempt history. PURE: no I/O, no async, no clock — `now_ms` is an
//! input — so the whole verdict table is golden-tested.
//!
//! The durable storage (`0x08` probe rows), the runtime verb that RUNS a probe, and the
//! CLI surface live in `wallet-fedimint`/`wallet-cli`; this module owns only the attempt
//! shape, the policy knobs, and the verdict function they parameterize.

use crate::types::FederationId;

/// Default probe amount: 20 sats (§5.0.2) — leg OUT still redeems comfortably above the
/// lnv2 5-sat minimum incoming CONTRACT after observed fees.
pub const PROBE_AMOUNT_MSAT: u64 = 20_000;
/// Default per-leg fee cap: 10 sats (§5.0.2).
pub const PROBE_LEG_FEE_CAP_MSAT: u64 = 10_000;

/// One finished probe attempt (§5.0.3) — the durable verdict evidence. Only leg outcomes
/// attributable to the CANDIDATE produce attempts at all; preflight/local/gateway faults
/// live solely on the umbrella ledger row (the §5.0.3 scoping rule).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProbeAttempt {
    pub at_ms: u64,
    pub ok: bool,
    /// The spending federation the attempt probed FROM (forensics; the verdict itself is
    /// source-agnostic for failures — see the scoping rule on [`probe_verdict`]).
    pub from: FederationId,
    /// The attempt's money parameters, recorded so the verdict can refuse to count a
    /// dust-sized success toward the trust gate (the qualifying rule) — CLI overrides
    /// must not silently weaken what `Passed` means.
    pub amount_msat: u64,
    pub leg_fee_cap_msat: u64,
    /// The failed leg + verbatim error for a failed attempt (`None` on success) — the
    /// same text as the failing move's ledger row and the umbrella `Probe` row.
    pub error: Option<String>,
}

/// The probe's runtime + verdict knobs (§5.0.2/§5.0.3). NOT persisted: it parameterizes a
/// pure function over durable attempts, and `status` always evaluates with the DEFAULT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbePolicy {
    // -- runtime knobs (the money side) --
    pub amount_msat: u64,
    pub leg_fee_cap_msat: u64,
    // -- verdict knobs (read by the PURE verdict fn) --
    pub min_successes: u32,
    /// Successes must SPAN this (ADR-0017 "sustained"). Default 24h.
    pub min_span_ms: u64,
    /// The verdict's whole evaluation window; the NEWEST success must be younger than
    /// this. Default 7d — exactly the §5.0.4 retention window for pass evaluation.
    pub ttl_ms: u64,
}

impl Default for ProbePolicy {
    fn default() -> Self {
        Self {
            amount_msat: PROBE_AMOUNT_MSAT,
            leg_fee_cap_msat: PROBE_LEG_FEE_CAP_MSAT,
            min_successes: 3,
            min_span_ms: 24 * 60 * 60 * 1000,
            ttl_ms: 7 * 24 * 60 * 60 * 1000,
        }
    }
}

/// The cached, expiring verdict (§5.0.3). Only `Passed` ever gates IN (and the gate
/// itself is 5.1's wire-up — 5.0 computes and surfaces, gating nothing).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveProbeVerdict {
    Passed,
    NeverProbed,
    /// Successes so far, but not yet count+span.
    Insufficient,
    /// A retained stale success exists; current in-window evidence is empty.
    Expired,
    /// Newest in-window attempt is a candidate failure, no prior pass in evidence.
    Failed,
    /// A qualifying pass existed, then a failure demoted it.
    FailedSinceLastPass,
}

/// The §5.0.3 verdict over a bounded attempt history. Rules (each a golden):
///
/// - WINDOW: attempts older than `ttl_ms` are ignored entirely (ADR-0017's "sustained"
///   pass is RECENT evidence). An empty window with a retained stale SUCCESS is
///   [`Expired`](ActiveProbeVerdict::Expired) (success evidence aged out); empty
///   history — or only stale failures, the negative signal having aged past the whole
///   evidence window — is [`NeverProbed`](ActiveProbeVerdict::NeverProbed).
/// - SUFFIX: within the window only the CONTIGUOUS SUCCESS SUFFIX counts — the successes
///   strictly after the most recent failure ("a fresh sustained window rebuilds" is
///   literal).
/// - QUALIFYING: within that suffix only successes with `from == source AND amount ≥
///   policy.amount AND fee cap ≤ policy's` count. The source condition makes a pass
///   PAIR-PROVEN (routing is a (source, candidate) property); FAILURES count regardless
///   of source or money parameters — candidate dishonesty generalizes, routability does
///   not.
/// - `Passed` iff the qualifying suffix holds ≥ `min_successes` successes whose oldest
///   and newest span ≥ `min_span_ms` (the newest is in-window by construction).
/// - An empty suffix (the newest in-window attempt is a failure) is
///   [`FailedSinceLastPass`](ActiveProbeVerdict::FailedSinceLastPass) when a qualifying
///   pass exists in the retained evidence before that failure, else
///   [`Failed`](ActiveProbeVerdict::Failed) — a first-ever failing candidate must be
///   distinguishable from one that merely has not accumulated successes yet.
pub fn probe_verdict(
    attempts: &[ProbeAttempt],
    source: FederationId,
    now_ms: u64,
    policy: &ProbePolicy,
) -> ActiveProbeVerdict {
    if attempts.is_empty() {
        return ActiveProbeVerdict::NeverProbed;
    }
    // Defensive chronological order (the journal appends in order; a pure fn stays total).
    let mut sorted: Vec<&ProbeAttempt> = attempts.iter().collect();
    sorted.sort_by_key(|a| a.at_ms);

    // The window is a time SUFFIX of the sorted history: everything at or younger than ttl.
    let window_start = sorted.partition_point(|a| now_ms.saturating_sub(a.at_ms) > policy.ttl_ms);
    let window = &sorted[window_start..];
    if window.is_empty() {
        return if sorted.iter().any(|a| a.ok) {
            ActiveProbeVerdict::Expired
        } else {
            ActiveProbeVerdict::NeverProbed
        };
    }

    // The contiguous success suffix: successes strictly after the most recent in-window
    // failure (which, the window being a time suffix, is the most recent failure overall).
    let suffix = match window.iter().rposition(|a| !a.ok) {
        Some(pos) => &window[pos + 1..],
        None => window,
    };
    if suffix.is_empty() {
        // The newest in-window attempt is a failure. Distinguish demotion-of-a-pass from a
        // first-ever failure by searching the retained evidence BEFORE that failure for any
        // contiguous qualifying pass.
        let failure_idx = window_start + window.iter().rposition(|a| !a.ok).expect("suffix empty");
        return if prior_qualifying_pass(&sorted[..failure_idx], source, policy) {
            ActiveProbeVerdict::FailedSinceLastPass
        } else {
            ActiveProbeVerdict::Failed
        };
    }

    let qualifying: Vec<&ProbeAttempt> = suffix
        .iter()
        .filter(|a| qualifies(a, source, policy))
        .copied()
        .collect();
    if let (Some(first), Some(last)) = (qualifying.first(), qualifying.last()) {
        if qualifying.len() as u64 >= u64::from(policy.min_successes)
            && last.at_ms.saturating_sub(first.at_ms) >= policy.min_span_ms
        {
            return ActiveProbeVerdict::Passed;
        }
    }
    ActiveProbeVerdict::Insufficient
}

/// A success counting toward `Passed` for `source` (§5.0.3's qualifying rule): pair-proven
/// (same source) and at least as strong as the evaluating policy's money parameters.
fn qualifies(a: &ProbeAttempt, source: FederationId, policy: &ProbePolicy) -> bool {
    a.ok && a.from == source
        && a.amount_msat >= policy.amount_msat
        && a.leg_fee_cap_msat <= policy.leg_fee_cap_msat
}

/// Whether any contiguous run of successes in `attempts` (already chronological) satisfies
/// the qualifying count+span for `source` — the "a qualifying pass existed" evidence that
/// separates `FailedSinceLastPass` from a first-ever `Failed`.
fn prior_qualifying_pass(
    attempts: &[&ProbeAttempt],
    source: FederationId,
    policy: &ProbePolicy,
) -> bool {
    attempts
        .split(|a| !a.ok)
        .any(|run| run_qualifies(run, source, policy))
}

fn run_qualifies(run: &[&ProbeAttempt], source: FederationId, policy: &ProbePolicy) -> bool {
    let qualifying: Vec<&&ProbeAttempt> = run
        .iter()
        .filter(|a| qualifies(a, source, policy))
        .collect();
    match (qualifying.first(), qualifying.last()) {
        (Some(first), Some(last)) => {
            qualifying.len() as u64 >= u64::from(policy.min_successes)
                && last.at_ms.saturating_sub(first.at_ms) >= policy.min_span_ms
        }
        _ => false,
    }
}
