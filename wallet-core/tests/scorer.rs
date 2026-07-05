use wallet_core::{
    score, FederationFacts, FederationId, Module, ObserverPrior, ScorerPolicy,
    ScorerReasonCode as ReasonCode,
};

/// An eligible 4-of-4 mainnet federation: all probes pass, no shutdown, no prior.
/// Tests clone this and tweak the one field under test.
fn healthy() -> FederationFacts {
    FederationFacts {
        id: FederationId([1; 32]),
        guardian_count: 4,
        threshold: 4,
        is_mainnet: true,
        modules: vec![Module::Mint, Module::Wallet, Module::Ln],
        quorum_live: true,
        round_trip_ok: true,
        peg_out_quotable: true,
        latency_ms: 100,
        shutdown_scheduled: false,
        has_lnv2: true,
        observer: None,
    }
}

#[test]
fn reject_single_guardian() {
    let facts = FederationFacts {
        guardian_count: 1,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::NoFaultTolerance));
}

#[test]
fn reject_wrong_network() {
    let facts = FederationFacts {
        is_mainnet: false,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::WrongNetwork));
}

#[test]
fn reject_missing_module() {
    let facts = FederationFacts {
        modules: vec![Module::Mint, Module::Ln],
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::MissingModule));
}

#[test]
fn reject_probe_failed() {
    let facts = FederationFacts {
        round_trip_ok: false,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::ProbeFailed));
}

#[test]
fn reject_shutdown() {
    let facts = FederationFacts {
        shutdown_scheduled: true,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::ShutdownScheduled));
}

#[test]
fn reject_no_lnv2() {
    // T16: a fed with no Lightning v2 cannot send/receive at all, so it is ineligible
    // regardless of otherwise-perfect structural facts and probes.
    let facts = FederationFacts {
        has_lnv2: false,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::NoLnv2));
}

#[test]
fn eligible_healthy_fed() {
    let verdict = score(&healthy(), &ScorerPolicy::default());
    assert!(verdict.eligible_to_fund);
    // No reject reason was raised for a fully healthy fed.
    assert!(verdict.reasons.is_empty());
    assert!(verdict.rank_score > 0);
}

#[test]
fn observer_cannot_promote_past_gate() {
    // The key invariant (ADR-0017/0020): a probe-FAILED fed with a PERFECT untrusted
    // prior is STILL ineligible. The prior lives behind the gate.
    let facts = FederationFacts {
        round_trip_ok: false,
        observer: Some(ObserverPrior {
            uptime_permille: 1000,
            backing_sats: u64::MAX,
            activity_7d: u32::MAX,
        }),
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::ProbeFailed));
}

#[test]
fn observer_demotes_lower_uptime() {
    // Two eligible feds, identical except Observer uptime.
    let high = FederationFacts {
        observer: Some(ObserverPrior {
            uptime_permille: 1000,
            backing_sats: 200_000_000,
            activity_7d: 500,
        }),
        ..healthy()
    };
    let low = FederationFacts {
        observer: Some(ObserverPrior {
            uptime_permille: 600,
            backing_sats: 200_000_000,
            activity_7d: 500,
        }),
        ..healthy()
    };
    let policy = ScorerPolicy::default();
    let high_verdict = score(&high, &policy);
    let low_verdict = score(&low, &policy);

    assert!(high_verdict.eligible_to_fund);
    assert!(low_verdict.eligible_to_fund);
    // The lower-uptime fed ranks strictly below the otherwise-identical high one.
    assert!(low_verdict.rank_score < high_verdict.rank_score);
    assert!(low_verdict.reasons.contains(&ReasonCode::LowObserverUptime));
}

#[test]
fn observer_prior_cannot_outweigh_threshold_step() {
    // A perfect untrusted prior should not outrank a structurally stronger fed.
    let stronger = FederationFacts {
        threshold: 4,
        observer: None,
        ..healthy()
    };
    let observer_favored_weaker = FederationFacts {
        threshold: 3,
        observer: Some(ObserverPrior {
            uptime_permille: 1000,
            backing_sats: u64::MAX,
            activity_7d: u32::MAX,
        }),
        ..healthy()
    };
    let policy = ScorerPolicy::default();
    let stronger_verdict = score(&stronger, &policy);
    let weaker_verdict = score(&observer_favored_weaker, &policy);

    assert!(stronger_verdict.eligible_to_fund);
    assert!(weaker_verdict.eligible_to_fund);
    assert!(weaker_verdict.rank_score < stronger_verdict.rank_score);
}

#[test]
fn missing_observer_still_eligible() {
    let facts = FederationFacts {
        observer: None,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(verdict.eligible_to_fund);
}

// ---- §1 threshold trust floor ----

#[test]
fn reject_zero_threshold() {
    // §1: a 0-of-n threshold is structurally impossible; hard-reject it and surface the
    // reason in the verdict.
    let facts = FederationFacts {
        threshold: 0,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::InvalidThreshold));
}

#[test]
fn reject_threshold_above_guardian_count() {
    // §1: m > n is impossible. Rejected, and (being ineligible) rank is forced to 0.
    let facts = FederationFacts {
        guardian_count: 4,
        threshold: 5,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::InvalidThreshold));
    assert_eq!(verdict.rank_score, 0);
}

#[test]
fn reject_below_bft_threshold_3_of_100() {
    // §1: 3-of-100 claims fault tolerance far weaker than fedimint's BFT bound (67-of-100).
    // A discovered config CLAIMING this is rejected as structurally dishonest, not ranked
    // equal to an honest 3-of-4.
    let facts = FederationFacts {
        guardian_count: 100,
        threshold: 3,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::InvalidThreshold));
}

#[test]
fn accept_bft_threshold_3_of_4() {
    // §1: 3-of-4 is EXACTLY the BFT bound (4 − (4−1)/3 = 3). Nothing live is rejected.
    let facts = FederationFacts {
        guardian_count: 4,
        threshold: 3,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(verdict.eligible_to_fund);
    assert!(!verdict.reasons.contains(&ReasonCode::InvalidThreshold));
}

#[test]
fn accept_bft_threshold_67_of_100() {
    // §1: 67-of-100 is the BFT bound for 100 guardians (100 − 99/3 = 67); it passes.
    let facts = FederationFacts {
        guardian_count: 100,
        threshold: 67,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(verdict.eligible_to_fund);
    assert!(!verdict.reasons.contains(&ReasonCode::InvalidThreshold));
    assert!(verdict.rank_score > 0);
}

#[test]
fn zero_guardians_yields_a_verdict_without_panicking() {
    // §1: the BFT floor is SATURATING, so it never underflows on attacker-supplied facts —
    // guardian_count == 0 still yields a verdict (rejected by NoFaultTolerance), no panic.
    let facts = FederationFacts {
        guardian_count: 0,
        threshold: 0,
        ..healthy()
    };
    let verdict = score(&facts, &ScorerPolicy::default());
    assert!(!verdict.eligible_to_fund);
    assert!(verdict.reasons.contains(&ReasonCode::NoFaultTolerance));
}
