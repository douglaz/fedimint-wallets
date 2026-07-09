use wallet_core::{
    adaptive_sleep_ms, discover_pass_plan, probe_budget_ok, probe_budget_usage, probe_next_due,
    probe_next_due_at, probe_pass_expiry_anchor_ms, probe_verdict, probe_wake_due_ms,
    ActiveProbeVerdict as V, Actor, AdaptiveSleepDeadlines, FederationId, FeeBreakdown,
    IdempotencyKey, Msat, Occurrence, OperationKind, OperationRecord, OperationStatus,
    ProbeAttempt, ProbeBudget, ProbePolicy, ReasonCode, WatchPolicy, WATCH_BUSY_SPIN_FLOOR_MS,
};

const SECOND: u64 = 1_000;
const MINUTE: u64 = 60 * SECOND;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

fn fed(n: u8) -> FederationId {
    FederationId([n; 32])
}

fn policy() -> WatchPolicy {
    WatchPolicy::default()
}

fn gate_policy() -> ProbePolicy {
    ProbePolicy::default()
}

#[test]
fn adaptive_sleep_uses_base_interval_when_no_deadlines_are_nearer() {
    let policy = policy();
    let deadlines = AdaptiveSleepDeadlines {
        last_discover_ms: 1_000,
        ..AdaptiveSleepDeadlines::default()
    };

    assert_eq!(adaptive_sleep_ms(1_000, &policy, &deadlines), 10 * MINUTE);
}

#[test]
fn adaptive_sleep_uses_nearest_expiry_lead_probe_due_and_discover_due() {
    let policy = policy();
    let now = 100 * HOUR;

    let expiry = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        expiries_ms: vec![now + policy.evacuation_lead_ms + 2 * MINUTE],
        probe_due_ms: vec![now + 7 * MINUTE],
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(adaptive_sleep_ms(now, &policy, &expiry), 2 * MINUTE);

    let probe = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        expiries_ms: vec![now + policy.evacuation_lead_ms + 8 * MINUTE],
        probe_due_ms: vec![now + 3 * MINUTE],
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(adaptive_sleep_ms(now, &policy, &probe), 3 * MINUTE);

    let discover = AdaptiveSleepDeadlines {
        last_discover_ms: now - policy.discover_every_ms + 4 * MINUTE,
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(adaptive_sleep_ms(now, &policy, &discover), 4 * MINUTE);
}

#[test]
fn adaptive_sleep_applies_routine_floor_but_concrete_deadlines_bypass_it() {
    let policy = policy();
    let now = 10 * HOUR;

    let overdue_discover = AdaptiveSleepDeadlines {
        last_discover_ms: now - policy.discover_every_ms - 1,
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(
        adaptive_sleep_ms(now, &policy, &overdue_discover),
        policy.min_interval_ms,
        "routine discovery cadence is floored"
    );

    let imminent_expiry = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        expiries_ms: vec![now + policy.evacuation_lead_ms + 5 * SECOND],
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(
        adaptive_sleep_ms(now, &policy, &imminent_expiry),
        5 * SECOND,
        "evacuation deadline bypasses the 30s routine floor"
    );

    let due_now = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        probe_due_ms: vec![now],
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(
        adaptive_sleep_ms(now, &policy, &due_now),
        WATCH_BUSY_SPIN_FLOOR_MS
    );
}

#[test]
fn adaptive_sleep_floors_in_window_expiry_to_min_interval_not_busy_spin() {
    let policy = policy();
    let now = 10 * HOUR;

    // An expiry whose evacuation point is already behind `now` — the pre-shutdown
    // window the watch loop exists to handle. The tick evacuates on this cycle, so a
    // sub-min_interval re-wake buys nothing; without a floor this pins the loop to the
    // 1s busy-spin floor for the whole (default 1h) window. It must re-check at the min
    // interval instead.
    let in_window_expiry = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        expiries_ms: vec![now + policy.evacuation_lead_ms - 5 * SECOND],
        ..AdaptiveSleepDeadlines::default()
    };
    assert_ne!(
        adaptive_sleep_ms(now, &policy, &in_window_expiry),
        WATCH_BUSY_SPIN_FLOOR_MS,
        "an in-window expiry must not busy-spin"
    );
    assert_eq!(
        adaptive_sleep_ms(now, &policy, &in_window_expiry),
        policy.min_interval_ms,
        "an in-window expiry re-checks at min_interval"
    );

    // Exactly at the evacuation point also floors to min_interval (delay == 0).
    let at_evac_point = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        expiries_ms: vec![now + policy.evacuation_lead_ms],
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(
        adaptive_sleep_ms(now, &policy, &at_evac_point),
        policy.min_interval_ms
    );
}

#[test]
fn adaptive_sleep_in_window_expiry_never_sleeps_past_a_short_notice_shutdown() {
    // When the evacuation window is shorter than the min interval, flooring an in-window
    // expiry to min_interval would sleep past the shutdown and eat a load-bearing
    // evacuation retry. The wake must be capped at the time remaining until expiry.
    let mut policy = policy();
    policy.evacuation_lead_ms = 10 * SECOND;
    policy.min_interval_ms = 30 * SECOND;
    let now = 10 * HOUR;

    // In-window (evac point = now - 2s) with the actual shutdown only 8s away.
    let imminent_shutdown = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        expiries_ms: vec![now + 8 * SECOND],
        ..AdaptiveSleepDeadlines::default()
    };
    let sleep = adaptive_sleep_ms(now, &policy, &imminent_shutdown);
    assert_eq!(sleep, 8 * SECOND, "wake lands before the 8s shutdown");
    assert!(
        sleep < policy.min_interval_ms,
        "a short-notice shutdown bypasses the min_interval floor"
    );

    // Sub-second time-to-shutdown must be honored exactly, not rounded up to the 1s
    // busy-spin floor (which would sleep past the expiry and lose the last retry).
    let sub_second = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        expiries_ms: vec![now + 500],
        ..AdaptiveSleepDeadlines::default()
    };
    let sleep = adaptive_sleep_ms(now, &policy, &sub_second);
    assert_eq!(
        sleep, 500,
        "a 500ms-to-shutdown wake is not floored past the expiry"
    );
    assert!(sleep < WATCH_BUSY_SPIN_FLOOR_MS);

    // Freshly entered a long (default) window: still capped at min_interval, no busy-spin.
    let mut long_window = policy;
    long_window.evacuation_lead_ms = HOUR;
    let entering_long_window = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        expiries_ms: vec![now + HOUR - SECOND],
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(
        adaptive_sleep_ms(now, &long_window, &entering_long_window),
        long_window.min_interval_ms,
        "a long evacuation window re-checks at min_interval, not 1s"
    );
}

#[test]
fn adaptive_sleep_clamps_to_base_and_backlog_uses_min_interval() {
    let policy = policy();
    let now = 10 * HOUR;

    let far_discover = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        probe_due_ms: vec![now + HOUR],
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(
        adaptive_sleep_ms(now, &policy, &far_discover),
        policy.base_interval_ms
    );

    let backlog = AdaptiveSleepDeadlines {
        last_discover_ms: now,
        discover_backlog: true,
        ..AdaptiveSleepDeadlines::default()
    };
    assert_eq!(
        adaptive_sleep_ms(now, &policy, &backlog),
        policy.min_interval_ms
    );
}

#[test]
fn probe_next_due_covers_every_verdict() {
    let policy = policy();
    let gate = gate_policy();
    let now = 1_000 * HOUR;
    let last_attempt = now - HOUR;
    let last_invocation = now - 2 * HOUR;

    assert_eq!(
        probe_next_due_at(V::NeverProbed, None, None, now, &policy, &gate),
        now
    );
    assert_eq!(
        probe_next_due(V::NeverProbed, None, Some(last_invocation), &policy, &gate),
        last_invocation + policy.probe_retry_backoff_ms
    );
    assert_eq!(
        probe_next_due(
            V::Insufficient,
            Some(last_attempt),
            Some(last_invocation),
            &policy,
            &gate
        ),
        last_attempt + policy.probe_build_interval_ms(&gate)
    );
    assert_eq!(
        probe_next_due(
            V::Expired,
            Some(last_attempt),
            Some(last_invocation),
            &policy,
            &gate
        ),
        last_attempt + policy.probe_build_interval_ms(&gate)
    );
    assert_eq!(
        probe_next_due(V::Failed, Some(last_attempt), None, &policy, &gate),
        last_attempt + policy.probe_retry_backoff_ms
    );
    assert_eq!(
        probe_next_due(
            V::FailedSinceLastPass,
            Some(last_attempt),
            None,
            &policy,
            &gate
        ),
        last_attempt + policy.probe_retry_backoff_ms
    );
    assert_eq!(
        probe_next_due(V::Passed, Some(last_attempt), None, &policy, &gate),
        last_attempt + gate.ttl_ms - policy.probe_refresh_lead_ms
    );
}

#[test]
fn probe_next_due_clamps_refresh_lead_to_half_the_ttl() {
    let policy = WatchPolicy {
        probe_refresh_lead_ms: 12 * HOUR,
        ..WatchPolicy::default()
    };
    let gate = ProbePolicy {
        ttl_ms: 2 * HOUR,
        ..ProbePolicy::default()
    };
    let newest_success = 10 * DAY;

    assert_eq!(
        probe_next_due(V::Passed, Some(newest_success), None, &policy, &gate),
        newest_success + HOUR
    );
}

#[test]
fn probe_build_interval_rounds_up_to_span_successes() {
    let policy = WatchPolicy {
        min_interval_ms: 1,
        ..WatchPolicy::default()
    };
    let gate = ProbePolicy {
        min_successes: 4,
        min_span_ms: 1_000,
        ..gate_policy()
    };

    assert_eq!(policy.probe_build_interval_ms(&gate), 334);
    assert_eq!(
        probe_next_due(V::Insufficient, Some(10_000), None, &policy, &gate),
        10_334
    );
}

#[test]
fn passed_probe_refresh_uses_pass_expiry_anchor_not_newest_success() {
    let policy = policy();
    let gate = gate_policy();
    let source = fed(1);
    let other_source = fed(2);
    let first = 10 * DAY;
    let newest_qualifying = first + gate.min_span_ms;
    let later_non_qualifying = newest_qualifying + HOUR;
    let attempts = vec![
        probe_attempt(first, source, gate.amount_msat, gate.leg_fee_cap_msat),
        probe_attempt(
            first + gate.min_span_ms / 2,
            source,
            gate.amount_msat,
            gate.leg_fee_cap_msat,
        ),
        probe_attempt(
            newest_qualifying,
            source,
            gate.amount_msat,
            gate.leg_fee_cap_msat,
        ),
        probe_attempt(
            later_non_qualifying,
            other_source,
            gate.amount_msat,
            gate.leg_fee_cap_msat,
        ),
    ];

    assert_eq!(
        probe_verdict(&attempts, source, later_non_qualifying, &gate),
        V::Passed
    );
    let refresh_base = probe_pass_expiry_anchor_ms(&attempts, source, later_non_qualifying, &gate);
    assert_eq!(refresh_base, Some(first));
    assert_eq!(
        probe_next_due(V::Passed, refresh_base, None, &policy, &gate),
        first + gate.ttl_ms - policy.probe_refresh_lead_ms
    );
    assert_ne!(
        probe_next_due(V::Passed, refresh_base, None, &policy, &gate),
        later_non_qualifying + gate.ttl_ms - policy.probe_refresh_lead_ms
    );
}

#[test]
fn passed_probe_expiry_anchor_moves_when_surplus_successes_keep_the_pass_alive() {
    let gate = gate_policy();
    let source = fed(1);
    let first = 10 * DAY;
    let attempts = vec![
        probe_attempt(first, source, gate.amount_msat, gate.leg_fee_cap_msat),
        probe_attempt(
            first + HOUR,
            source,
            gate.amount_msat,
            gate.leg_fee_cap_msat,
        ),
        probe_attempt(
            first + 2 * HOUR,
            source,
            gate.amount_msat,
            gate.leg_fee_cap_msat,
        ),
        probe_attempt(
            first + gate.min_span_ms,
            source,
            gate.amount_msat,
            gate.leg_fee_cap_msat,
        ),
        probe_attempt(
            first + gate.min_span_ms + HOUR,
            source,
            gate.amount_msat,
            gate.leg_fee_cap_msat,
        ),
    ];

    assert_eq!(
        probe_verdict(&attempts, source, first + gate.min_span_ms + HOUR, &gate),
        V::Passed
    );
    assert_eq!(
        probe_pass_expiry_anchor_ms(&attempts, source, first + gate.min_span_ms + HOUR, &gate),
        Some(first + HOUR)
    );
}

#[test]
fn probe_next_due_uses_recent_invocation_backoff_as_floor() {
    let policy = policy();
    let gate = ProbePolicy {
        ttl_ms: 2 * HOUR,
        ..gate_policy()
    };
    let now = 20 * DAY;
    let old_attempt = now - DAY;
    let recent_no_attempt_invocation = now - 5 * MINUTE;
    let retry_floor = recent_no_attempt_invocation + policy.probe_retry_backoff_ms;

    assert_eq!(
        probe_next_due(
            V::Insufficient,
            Some(old_attempt),
            Some(recent_no_attempt_invocation),
            &policy,
            &gate
        ),
        retry_floor
    );
    assert_eq!(
        probe_next_due(
            V::Failed,
            Some(old_attempt),
            Some(recent_no_attempt_invocation),
            &policy,
            &gate
        ),
        retry_floor
    );
    assert_eq!(
        probe_next_due(
            V::Passed,
            Some(now - 2 * HOUR),
            Some(recent_no_attempt_invocation),
            &policy,
            &gate
        ),
        retry_floor
    );
}

#[test]
fn probe_budget_counts_only_agent_money_moving_probe_rows() {
    let rows = vec![
        probe_row(0, Some(Msat(20_000))),
        probe_row(1, None),
        probe_row(2, Some(Msat(5_000))),
        probe_row_with_actor(3, Some(Msat(10_000)), Actor::User),
        non_probe_row(4),
    ];
    let usage = probe_budget_usage(&rows);

    assert_eq!(usage.attempts, 2);
    assert_eq!(usage.spend_msat, 25_000);
    assert!(probe_budget_ok(
        usage.attempts,
        usage.spend_msat,
        &ProbeBudget::default()
    ));
    assert!(!probe_budget_ok(
        50,
        0,
        &ProbeBudget {
            max_probe_attempts_per_week: 50,
            max_probe_spend_per_week_msat: 50_000,
        }
    ));
    assert!(!probe_budget_ok(
        1,
        50_000,
        &ProbeBudget {
            max_probe_attempts_per_week: 50,
            max_probe_spend_per_week_msat: 50_000,
        }
    ));
}

#[test]
fn budget_blocked_never_probed_wake_is_floored_to_min_interval() {
    let policy = policy();
    let gate = gate_policy();
    let now = 10 * HOUR;
    let due = probe_next_due_at(V::NeverProbed, None, None, now, &policy, &gate);
    assert_eq!(due, now);

    let wake_due = probe_wake_due_ms(due, now, false, None, &policy);
    assert_eq!(wake_due, now + policy.min_interval_ms);

    let sleep = adaptive_sleep_ms(
        now,
        &policy,
        &AdaptiveSleepDeadlines {
            last_discover_ms: now,
            probe_due_ms: vec![wake_due],
            ..AdaptiveSleepDeadlines::default()
        },
    );
    assert_eq!(sleep, policy.min_interval_ms);
    assert_ne!(sleep, WATCH_BUSY_SPIN_FLOOR_MS);
}

#[test]
fn budget_blocked_probe_wake_waits_for_due_and_budget_reset() {
    let policy = policy();
    let now = 10 * HOUR;
    let next_due = now + 5 * MINUTE;
    let reset_after_due = now + HOUR;

    assert_eq!(
        probe_wake_due_ms(next_due, now, false, Some(reset_after_due), &policy),
        reset_after_due
    );

    let reset_before_due = now + MINUTE;
    assert_eq!(
        probe_wake_due_ms(next_due, now, false, Some(reset_before_due), &policy),
        next_due
    );
}

#[test]
fn discover_pass_plan_bounds_window_advances_cursor_and_defers_overflow() {
    let ids = vec![fed(1), fed(2), fed(3), fed(4)];

    let first = discover_pass_plan(None, &ids, 2);
    assert_eq!(first.window, vec![fed(1), fed(2)]);
    assert_eq!(first.next_cursor, Some(fed(2)));
    assert!(!first.wrapped);

    let second = discover_pass_plan(first.next_cursor, &ids, 2);
    assert_eq!(second.window, vec![fed(3), fed(4)]);
    assert_eq!(second.next_cursor, Some(fed(4)));
    assert!(second.wrapped);
}

#[test]
fn discover_pass_plan_zero_cap_does_not_advance_cursor() {
    let ids = vec![fed(1), fed(2), fed(3), fed(4)];

    let plan = discover_pass_plan(Some(fed(2)), &ids, 0);

    assert!(plan.window.is_empty());
    assert_eq!(plan.next_cursor, None);
    assert!(!plan.wrapped);
}

#[test]
fn discover_pass_plan_starts_after_cursor_wraps_and_keeps_fresh_ids_in_rotation() {
    let ids = vec![fed(1), fed(2), fed(3), fed(4)];
    let wrapped = discover_pass_plan(Some(fed(3)), &ids, 3);
    assert_eq!(wrapped.window, vec![fed(4), fed(1), fed(2)]);
    assert_eq!(wrapped.next_cursor, Some(fed(2)));
    assert!(wrapped.wrapped);

    let with_fresh_before_cursor = vec![fed(0), fed(1), fed(2), fed(3), fed(4)];
    let plan = discover_pass_plan(Some(fed(2)), &with_fresh_before_cursor, 3);
    assert_eq!(plan.window, vec![fed(3), fed(4), fed(0)]);
    assert_eq!(plan.next_cursor, Some(fed(0)));
    assert!(plan.wrapped);
}

#[test]
fn discover_pass_plan_wraps_when_cap_reaches_last_id_from_a_successor() {
    let ids = vec![fed(0), fed(1), fed(2), fed(3), fed(4)];

    let plan = discover_pass_plan(Some(fed(2)), &ids, 2);

    assert_eq!(plan.window, vec![fed(3), fed(4)]);
    assert_eq!(plan.next_cursor, Some(fed(4)));
    assert!(plan.wrapped);
}

#[test]
fn discover_pass_plan_keeps_backlog_when_stale_cursor_reaches_end_without_wrap() {
    let ids = vec![fed(0), fed(1), fed(3), fed(4)];

    let plan = discover_pass_plan(Some(fed(2)), &ids, 2);

    assert_eq!(plan.window, vec![fed(3), fed(4)]);
    assert_eq!(plan.next_cursor, Some(fed(4)));
    assert!(!plan.wrapped);
}

#[test]
fn discover_pass_plan_does_not_clear_backlog_when_cursor_at_max_and_cap_defers_tail() {
    let ids = vec![fed(1), fed(2), fed(3), fed(4)];

    let plan = discover_pass_plan(Some(fed(4)), &ids, 2);

    assert_eq!(plan.window, vec![fed(1), fed(2)]);
    assert_eq!(plan.next_cursor, Some(fed(2)));
    assert!(!plan.wrapped);
}

fn probe_row(seq: u64, cost_msat: Option<Msat>) -> OperationRecord {
    probe_row_with_actor(
        seq,
        cost_msat,
        Actor::Agent {
            occurrence: Occurrence(seq),
        },
    )
}

fn probe_attempt(
    at_ms: u64,
    from: FederationId,
    amount_msat: u64,
    leg_fee_cap_msat: u64,
) -> ProbeAttempt {
    ProbeAttempt {
        at_ms,
        ok: true,
        from,
        amount_msat,
        leg_fee_cap_msat,
        error: None,
    }
}

fn probe_row_with_actor(seq: u64, cost_msat: Option<Msat>, actor: Actor) -> OperationRecord {
    OperationRecord {
        seq,
        correlation_key: IdempotencyKey(format!("probe:key:{seq}")),
        kind: OperationKind::Probe {
            fed: fed(1),
            from: fed(2),
            amount_msat: Msat(20_000),
            cost_msat,
        },
        actor,
        reason: ReasonCode::ActiveProbe,
        status: OperationStatus::Succeeded,
        created_at_ms: seq,
        updated_at_ms: seq,
        fees: FeeBreakdown::default(),
        error: None,
        repaired: false,
    }
}

fn non_probe_row(seq: u64) -> OperationRecord {
    OperationRecord {
        seq,
        correlation_key: IdempotencyKey(format!("tick:key:{seq}")),
        kind: OperationKind::Tick {
            occurrence: Occurrence(seq),
            decisions: 0,
            performed: 0,
            failed: 0,
        },
        actor: Actor::Agent {
            occurrence: Occurrence(seq),
        },
        reason: ReasonCode::StandingInstruction,
        status: OperationStatus::Succeeded,
        created_at_ms: seq,
        updated_at_ms: seq,
        fees: FeeBreakdown::default(),
        error: None,
        repaired: false,
    }
}
