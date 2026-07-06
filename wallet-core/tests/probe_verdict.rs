//! §5.0.8 pure goldens for the active-probe verdict (`wallet_core::probe_verdict`):
//! the full §5.0.3 rule table — window, contiguous qualifying suffix, source scoping,
//! fault demotion, and every verdict variant — over a pinned `now` with no clock.

use wallet_core::{
    probe_verdict, ActiveProbeVerdict as V, FederationId, ProbeAttempt, ProbePolicy,
    PROBE_AMOUNT_MSAT, PROBE_LEG_FEE_CAP_MSAT,
};

const NOW: u64 = 1_000_000_000_000;
const HOUR: u64 = 3_600_000;

fn fed(n: u8) -> FederationId {
    FederationId([n; 32])
}

/// The default probing source in these goldens.
fn src() -> FederationId {
    fed(0xA)
}

fn ok_from(hours_ago: u64, from: FederationId) -> ProbeAttempt {
    ProbeAttempt {
        at_ms: NOW - hours_ago * HOUR,
        ok: true,
        from,
        amount_msat: PROBE_AMOUNT_MSAT,
        leg_fee_cap_msat: PROBE_LEG_FEE_CAP_MSAT,
        error: None,
    }
}

fn ok(hours_ago: u64) -> ProbeAttempt {
    ok_from(hours_ago, src())
}

fn fail_from(hours_ago: u64, from: FederationId) -> ProbeAttempt {
    ProbeAttempt {
        at_ms: NOW - hours_ago * HOUR,
        ok: false,
        from,
        amount_msat: PROBE_AMOUNT_MSAT,
        leg_fee_cap_msat: PROBE_LEG_FEE_CAP_MSAT,
        error: Some("probe leg OUT failed: rejected".into()),
    }
}

fn fail(hours_ago: u64) -> ProbeAttempt {
    fail_from(hours_ago, src())
}

fn verdict(attempts: &[ProbeAttempt]) -> V {
    probe_verdict(attempts, src(), NOW, &ProbePolicy::default())
}

#[test]
fn empty_history_is_never_probed() {
    assert_eq!(verdict(&[]), V::NeverProbed);
}

#[test]
fn insufficient_below_min_successes() {
    // Two spanning successes: count 2 < 3.
    assert_eq!(verdict(&[ok(30), ok(1)]), V::Insufficient);
}

#[test]
fn insufficient_below_min_span() {
    // Three successes crammed into 2 hours: count ok, span 2h < 24h.
    assert_eq!(verdict(&[ok(3), ok(2), ok(1)]), V::Insufficient);
}

#[test]
fn passed_at_exactly_min_successes_and_exact_span() {
    // Exactly 3 successes whose oldest..newest span is EXACTLY 24h (inclusive ≥).
    assert_eq!(verdict(&[ok(24), ok(12), ok(0)]), V::Passed);
}

#[test]
fn window_rule_makes_a_success_just_past_ttl_invisible() {
    // §5.0.8's literal case: successes at 8d / 2d / 1h with a 7d ttl are Insufficient,
    // not Passed — the 8d success is outside the window ENTIRELY.
    assert_eq!(verdict(&[ok(8 * 24), ok(2 * 24), ok(1)]), V::Insufficient);
}

#[test]
fn success_at_exactly_ttl_is_still_visible() {
    // At EXACTLY ttl old the attempt is still in the window ("older than ttl" is strict):
    // 7d / 2d / 1h passes (span 7d − 1h ≥ 24h, count 3)...
    assert_eq!(verdict(&[ok(7 * 24), ok(2 * 24), ok(1)]), V::Passed);
    // ...and one millisecond past ttl it drops out, demoting the same shape to
    // Insufficient (the boundary is the verdict flip).
    let mut just_past = ok(7 * 24);
    just_past.at_ms -= 1;
    assert_eq!(verdict(&[just_past, ok(2 * 24), ok(1)]), V::Insufficient);
}

#[test]
fn stale_pass_reads_expired_never_probed_only_without_any_success() {
    // A retained stale success (outside the window) is the evidence that a pass existed:
    // Expired, never NeverProbed.
    assert_eq!(verdict(&[ok(30 * 24)]), V::Expired);
    // Only stale FAILURES: the negative signal has aged past the whole evidence window.
    assert_eq!(verdict(&[fail(30 * 24)]), V::NeverProbed);
}

#[test]
fn first_ever_failure_is_failed_not_insufficient() {
    // The negative signal must survive — a first-ever failing candidate is
    // distinguishable from one that merely has not accumulated successes yet.
    assert_eq!(verdict(&[fail(1)]), V::Failed);
    // A single (non-pass) success before the failure does not change that.
    assert_eq!(verdict(&[ok(10), fail(1)]), V::Failed);
}

#[test]
fn trailing_failure_after_a_qualifying_pass_is_failed_since_last_pass() {
    // 3 spanning successes (a qualifying pass), then a failure: immediate demotion,
    // labeled as the loss of a pass.
    assert_eq!(
        verdict(&[ok(50), ok(25), ok(2), fail(1)]),
        V::FailedSinceLastPass
    );
}

#[test]
fn suffix_rule_counts_only_successes_after_the_most_recent_failure() {
    // success, failure, success×3: passes IFF the last three alone satisfy count+span.
    // Last three span 48h ≥ 24h → Passed (the pre-failure success contributes nothing).
    assert_eq!(
        verdict(&[ok(72), fail(50), ok(49), ok(25), ok(1)]),
        V::Passed
    );
    // Last three span only 2h → Insufficient, even though counting the pre-failure
    // success would have satisfied count+span ("a fresh sustained window rebuilds").
    assert_eq!(
        verdict(&[ok(72), fail(50), ok(3), ok(2), ok(1)]),
        V::Insufficient
    );
}

#[test]
fn source_scoping_makes_a_pass_pair_proven() {
    // An A→C pass gates C for A only: the same history evaluated for B is Insufficient
    // (its successes are non-qualifying for B), never Passed and never Failed.
    let history = [ok(48), ok(24), ok(0)];
    assert_eq!(
        probe_verdict(&history, src(), NOW, &ProbePolicy::default()),
        V::Passed
    );
    assert_eq!(
        probe_verdict(&history, fed(0xB), NOW, &ProbePolicy::default()),
        V::Insufficient
    );
}

#[test]
fn a_candidate_fault_failure_from_any_source_demotes_for_all_sources() {
    // A pass from A, then a candidate-fault failure recorded from B: candidate
    // dishonesty generalizes, so BOTH sources are demoted (A loses its pass; B, which
    // never had one, reads the plain failure).
    let history = [ok(50), ok(25), ok(2), fail_from(1, fed(0xB))];
    assert_eq!(
        probe_verdict(&history, src(), NOW, &ProbePolicy::default()),
        V::FailedSinceLastPass
    );
    assert_eq!(
        probe_verdict(&history, fed(0xB), NOW, &ProbePolicy::default()),
        V::Failed
    );
}

#[test]
fn non_qualifying_successes_never_pass_but_their_failures_still_demote() {
    let policy = ProbePolicy::default();
    // Successes below the policy amount never count toward Passed...
    let dust = |hours_ago: u64| ProbeAttempt {
        amount_msat: policy.amount_msat - 1,
        ..ok(hours_ago)
    };
    assert_eq!(verdict(&[dust(48), dust(24), dust(0)]), V::Insufficient);
    // ...as do successes probed under a LOOSER fee cap than the policy's...
    let loose = |hours_ago: u64| ProbeAttempt {
        leg_fee_cap_msat: policy.leg_fee_cap_msat + 1,
        ..ok(hours_ago)
    };
    assert_eq!(verdict(&[loose(48), loose(24), loose(0)]), V::Insufficient);
    // ...but a FAILURE demotes regardless of its money parameters.
    let dust_fail = ProbeAttempt {
        amount_msat: 1,
        leg_fee_cap_msat: policy.leg_fee_cap_msat + 1,
        ..fail(1)
    };
    assert_eq!(verdict(&[ok(48), ok(24), dust_fail]), V::Failed);
}

#[test]
fn shrunk_policy_windows_evaluate_the_same_history_differently() {
    // The §5.0.7/§5.0.8 smoke contract in pure form: a shrunken --min-span-secs policy
    // reads `passed` from a tight cluster while the DEFAULT policy still reads
    // `insufficient` for the same durable history.
    let history = [ok(3), ok(2), ok(1)];
    assert_eq!(verdict(&history), V::Insufficient);
    let shrunk = ProbePolicy {
        min_span_ms: HOUR,
        ..ProbePolicy::default()
    };
    assert_eq!(probe_verdict(&history, src(), NOW, &shrunk), V::Passed);
}
