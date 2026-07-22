//! Pure watch-loop scheduler helpers (phase 5.2).
//!
//! All interval fields are milliseconds. The runtime owns clocks, I/O, and filtering
//! (for example, only passing auto-joined federations into these helpers); this module
//! only computes deadlines, budgets, and round-robin windows over explicit inputs.

use crate::ledger::{Actor, OperationKind, OperationRecord};
use crate::probe::{ActiveProbeVerdict, ProbePolicy};
use crate::types::{FederationId, Msat};
use std::collections::BTreeSet;

pub const WATCH_BUSY_SPIN_FLOOR_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProbeBudget {
    pub max_probe_attempts_per_week: u32,
    pub max_probe_spend_per_week_msat: u64,
}

impl Default for ProbeBudget {
    fn default() -> Self {
        Self {
            max_probe_attempts_per_week: 50,
            max_probe_spend_per_week_msat: 50_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchPolicy {
    pub base_interval_ms: u64,
    pub min_interval_ms: u64,
    pub evacuation_lead_ms: u64,
    pub discover_every_ms: u64,
    pub discover_pass_deadline_ms: u64,
    pub per_preview_timeout_ms: u64,
    pub max_candidates_per_pass: usize,
    pub probe_refresh_lead_ms: u64,
    pub probe_retry_backoff_ms: u64,
    pub probe_budget: ProbeBudget,
}

impl WatchPolicy {
    pub fn probe_build_interval_ms(self, gate_policy: &ProbePolicy) -> u64 {
        let denominator = u64::from(gate_policy.min_successes.saturating_sub(1)).max(1);
        self.min_interval_ms
            .max(gate_policy.min_span_ms.div_ceil(denominator))
    }
}

impl Default for WatchPolicy {
    fn default() -> Self {
        Self {
            base_interval_ms: 10 * 60 * 1000,
            min_interval_ms: 30 * 1000,
            evacuation_lead_ms: 60 * 60 * 1000,
            discover_every_ms: 6 * 60 * 60 * 1000,
            discover_pass_deadline_ms: 60 * 1000,
            per_preview_timeout_ms: 20 * 1000,
            max_candidates_per_pass: 256,
            probe_refresh_lead_ms: 12 * 60 * 60 * 1000,
            probe_retry_backoff_ms: 60 * 60 * 1000,
            probe_budget: ProbeBudget::default(),
        }
    }
}

/// Per-verdict probe deadline (phase 5.2.3). `0` means immediately due for callers that do
/// not need to preserve a non-zero `now_ms`; use [`probe_next_due_at`] when the exact
/// immediate deadline matters. For [`ActiveProbeVerdict::Passed`], the timestamp input must be
/// the pass-expiry anchor from `probe_pass_expiry_anchor_ms`, not merely the newest success.
pub fn probe_next_due(
    verdict: ActiveProbeVerdict,
    last_attempt_or_pass_expiry_anchor_ms: Option<u64>,
    last_invocation_ms: Option<u64>,
    policy: &WatchPolicy,
    gate_policy: &ProbePolicy,
) -> u64 {
    probe_next_due_at(
        verdict,
        last_attempt_or_pass_expiry_anchor_ms,
        last_invocation_ms,
        0,
        policy,
        gate_policy,
    )
}

pub fn probe_next_due_at(
    verdict: ActiveProbeVerdict,
    last_attempt_or_pass_expiry_anchor_ms: Option<u64>,
    last_invocation_ms: Option<u64>,
    now_ms: u64,
    policy: &WatchPolicy,
    gate_policy: &ProbePolicy,
) -> u64 {
    match verdict {
        ActiveProbeVerdict::NeverProbed => last_invocation_ms
            .map(|last| last.saturating_add(policy.probe_retry_backoff_ms))
            .unwrap_or(now_ms),
        ActiveProbeVerdict::Insufficient | ActiveProbeVerdict::Expired => {
            let attempt_due = last_attempt_or_pass_expiry_anchor_ms
                .unwrap_or(now_ms)
                .saturating_add(policy.probe_build_interval_ms(gate_policy));
            invocation_backoff_floor(attempt_due, last_invocation_ms, policy)
        }
        ActiveProbeVerdict::Failed | ActiveProbeVerdict::FailedSinceLastPass => {
            let attempt_due = last_attempt_or_pass_expiry_anchor_ms
                .unwrap_or(now_ms)
                .saturating_add(policy.probe_retry_backoff_ms);
            invocation_backoff_floor(attempt_due, last_invocation_ms, policy)
        }
        ActiveProbeVerdict::Passed => {
            let ttl_deadline = last_attempt_or_pass_expiry_anchor_ms
                .or(last_invocation_ms)
                .unwrap_or(now_ms)
                .saturating_add(gate_policy.ttl_ms);
            let refresh_lead = policy.probe_refresh_lead_ms.min(gate_policy.ttl_ms / 2);
            invocation_backoff_floor(
                ttl_deadline.saturating_sub(refresh_lead),
                last_invocation_ms,
                policy,
            )
        }
    }
}

fn invocation_backoff_floor(
    deadline_ms: u64,
    last_invocation_ms: Option<u64>,
    policy: &WatchPolicy,
) -> u64 {
    last_invocation_ms.map_or(deadline_ms, |last| {
        deadline_ms.max(last.saturating_add(policy.probe_retry_backoff_ms))
    })
}

pub fn probe_budget_ok(
    attempts_last_7d: u32,
    spend_last_7d_msat: u64,
    budget: &ProbeBudget,
) -> bool {
    attempts_last_7d < budget.max_probe_attempts_per_week
        && spend_last_7d_msat < budget.max_probe_spend_per_week_msat
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProbeBudgetUsage {
    pub attempts: u32,
    pub spend_msat: u64,
}

/// Count only money-moving probe umbrella rows. `NoAttempt` refusals have
/// `cost_msat: None` and are excluded from both counters.
pub fn probe_budget_usage<'a>(
    rows_filtered_to_7d: impl IntoIterator<Item = &'a OperationRecord>,
) -> ProbeBudgetUsage {
    let mut usage = ProbeBudgetUsage::default();
    for row in rows_filtered_to_7d {
        if !matches!(row.actor, Actor::Agent { .. }) {
            continue;
        }
        if let OperationKind::Probe {
            cost_msat: Some(Msat(cost)),
            ..
        } = &row.kind
        {
            usage.attempts = usage.attempts.saturating_add(1);
            usage.spend_msat = usage.spend_msat.saturating_add(*cost);
        }
    }
    usage
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdaptiveSleepDeadlines {
    pub last_discover_ms: u64,
    pub discover_backlog: bool,
    /// Corroborated federation expiry timestamps. The scheduler subtracts
    /// [`WatchPolicy::evacuation_lead_ms`] from each.
    pub expiries_ms: Vec<u64>,
    /// Probe due timestamps, already filtered to the auto-joined/refreshed set by the caller.
    pub probe_due_ms: Vec<u64>,
}

pub fn adaptive_sleep_ms(
    now_ms: u64,
    policy: &WatchPolicy,
    deadlines: &AdaptiveSleepDeadlines,
) -> u64 {
    let discover_delay = if deadlines.discover_backlog {
        policy.min_interval_ms
    } else {
        deadlines
            .last_discover_ms
            .saturating_add(policy.discover_every_ms)
            .saturating_sub(now_ms)
    };
    let routine = policy
        .base_interval_ms
        .min(discover_delay)
        .max(policy.min_interval_ms)
        .min(policy.base_interval_ms);

    // Expiry deadlines are evaluated at their evacuation point. Once that point is
    // already at/behind `now` — the pre-shutdown window §5.2 exists to handle — the
    // tick evacuates on this very cycle, so re-waking sooner than `min_interval_ms`
    // buys nothing and would pin the loop to the 1s busy-spin floor for the whole
    // window. Floor an in-window expiry to the min interval; honor a future one exactly.
    // Probe deadlines are discrete, self-consuming events, so they keep the tight floor.
    let expiry_delay = deadlines
        .expiries_ms
        .iter()
        .map(|expiry| {
            let evac_point = expiry.saturating_sub(policy.evacuation_lead_ms);
            match evac_point.checked_sub(now_ms) {
                // Future evacuation point: wake exactly then.
                Some(delay) if delay > 0 => delay,
                // In-window (evacuation point already reached): drive/retry the evacuation
                // without busy-spinning the whole window — cap the cadence at min_interval
                // — but never sleep past the actual shutdown, so a load-bearing retry still
                // lands before expiry even when evacuation_lead < min_interval.
                _ => policy.min_interval_ms.min(expiry.saturating_sub(now_ms)),
            }
        })
        .min();
    // Probe deadlines can recur at ~0 indefinitely (e.g. a budget-blocked NeverProbed fed
    // is due every cycle), so they keep the busy-spin floor. An expiry is one-shot —
    // `add_expiry_deadline` drops it once it passes — so a genuine sub-second time-to-
    // shutdown is honored exactly, never rounded up past the expiry and lost.
    let probe_delay = deadlines
        .probe_due_ms
        .iter()
        .map(|deadline| {
            deadline
                .saturating_sub(now_ms)
                .max(WATCH_BUSY_SPIN_FLOOR_MS)
        })
        .min();
    let concrete = match (expiry_delay, probe_delay) {
        (Some(e), Some(p)) => Some(e.min(p)),
        (only, None) | (None, only) => only,
    };

    match concrete {
        Some(delay) => routine.min(delay),
        None => routine,
    }
}

pub fn probe_wake_due_ms(
    next_due_ms: u64,
    now_ms: u64,
    budget_ok: bool,
    budget_reset_ms: Option<u64>,
    policy: &WatchPolicy,
) -> u64 {
    if budget_ok {
        return next_due_ms;
    }
    let budget_ready_ms =
        budget_reset_ms.unwrap_or_else(|| now_ms.saturating_add(policy.min_interval_ms));
    next_due_ms.max(budget_ready_ms)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiscoverPassPlan {
    pub window: Vec<FederationId>,
    pub next_cursor: Option<FederationId>,
    pub wrapped: bool,
}

pub fn discover_pass_plan(
    cursor: Option<FederationId>,
    all_candidate_ids_sorted: &[FederationId],
    max_candidates_per_pass: usize,
) -> DiscoverPassPlan {
    discover_pass_plan_in_rotation(
        cursor,
        all_candidate_ids_sorted,
        all_candidate_ids_sorted,
        max_candidates_per_pass,
    )
}

pub fn discover_pass_plan_in_rotation(
    cursor: Option<FederationId>,
    rotation_order: &[FederationId],
    current_candidate_ids_sorted: &[FederationId],
    max_candidates_per_pass: usize,
) -> DiscoverPassPlan {
    if current_candidate_ids_sorted.is_empty() {
        return DiscoverPassPlan {
            window: Vec::new(),
            next_cursor: None,
            wrapped: true,
        };
    }
    if max_candidates_per_pass == 0 {
        return DiscoverPassPlan {
            window: Vec::new(),
            next_cursor: None,
            wrapped: false,
        };
    }

    let current_ids = current_candidate_ids_sorted
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let order = if rotation_order.is_empty() {
        current_candidate_ids_sorted
    } else {
        rotation_order
    };
    let len = order.len();
    let cursor_index = cursor.and_then(|cursor| order.iter().position(|id| *id == cursor));
    let successor = cursor.and_then(|cursor| {
        order
            .iter()
            .position(|id| current_ids.contains(id) && *id > cursor)
    });
    let start = cursor_index.map_or_else(|| successor.unwrap_or(0), |index| (index + 1) % len);
    let take = max_candidates_per_pass.min(current_ids.len());
    let mut window = Vec::with_capacity(take);
    let mut crossed_end = false;
    let mut index = start;
    for _ in 0..len {
        if cursor_index.is_some() && start != 0 && index == len - 1 {
            crossed_end = true;
        }
        let id = order[index];
        if current_ids.contains(&id) {
            window.push(id);
            if window.len() == take {
                break;
            }
        }
        index = (index + 1) % len;
    }
    let wrapped = window.len() == current_ids.len() || crossed_end;
    let next_cursor = window.last().copied();
    DiscoverPassPlan {
        window,
        next_cursor,
        wrapped,
    }
}
