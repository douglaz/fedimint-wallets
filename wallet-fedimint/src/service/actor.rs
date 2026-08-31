use super::driver::{self, Registry};
use super::*;
use crate::runtime::{
    ledger_nonce, move_key, now_ms, occurrence_from_nonce, probe_cost, probe_gated_members,
    probe_out_fee_cap, probe_umbrella_key, PROBE_BUDGET_WINDOW_MS,
};
use crate::tick::{build_snapshot, decisions_to_apply, pinned_input_problems};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use wallet_core::{
    admit_intent, intent_status_transition_allowed, probe_verdict, Action, ActiveProbeVerdict,
    Actor, AllocatorDecision, AllocatorGoal, AllocatorSnapshot, DecideAndJournal, GoalBlockers,
    IntentStatus, Journal, Occurrence, OperationKind, OperationStatus, ProbePolicy, ReasonCode,
    Reservations, RouteEconomics, RouteStatus, ScorerPolicy,
};

struct PendingWaiter {
    target: AwaitTarget,
    deadline: Instant,
    reply: oneshot::Sender<ServiceResult<AwaitOutcome>>,
}

#[derive(Clone)]
struct ProbeBudgetEntry {
    key: IdempotencyKey,
    effective_at_ms: u64,
    cost_msat: Option<u64>,
    active: bool,
    reserved_msat: u64,
}

#[derive(Default)]
struct ProbeBudgetState {
    entries: Vec<ProbeBudgetEntry>,
    load_error: Option<String>,
}

/// Federations whose sampled balances can be invalidated by this action.
///
/// This single projection is shared by durable intent transitions, direct raw-terminal
/// mutations, and commit-time freshness checks so those paths cannot drift apart.
pub(super) fn balance_federations(action: &Action) -> BTreeSet<FederationId> {
    match action {
        Action::Move { from, to, .. } | Action::Evacuate { from, to, .. } => {
            BTreeSet::from([*from, *to])
        }
        Action::DirectInflow { to, .. } | Action::Receive { to, .. } => BTreeSet::from([*to]),
        Action::Pay { from, .. } => BTreeSet::from([*from]),
        Action::Join { .. } | Action::Recover { .. } | Action::RefuseInflow { .. } => {
            BTreeSet::new()
        }
    }
}

struct TickBatch {
    key: IdempotencyKey,
    decisions: u32,
    pending: BTreeSet<IdempotencyKey>,
    performed: u32,
    failed: u32,
    error: Option<String>,
}

/// Whether a fresh Agent request may already have changed durable admission state when its caller
/// receives an error.  CommitTick uses this to distinguish a proven pre-upsert failure from a
/// requested write whose durability is known or conservatively unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FreshMutationDisposition {
    DefiniteNoMutation,
    RequestedMutationPossible,
    UnknownIdentityPoison,
}

/// The actor's internal error contract for the shared direct/CommitTick fresh admission path.
/// Public direct callers receive only `error`; CommitTick additionally needs the mutation
/// disposition before deciding whether a later sibling needs a goal/reservation fold.
#[derive(Debug)]
struct DecideOpError {
    error: ServiceError,
    disposition: FreshMutationDisposition,
}

impl DecideOpError {
    fn definite(error: ServiceError) -> Self {
        Self {
            error,
            disposition: FreshMutationDisposition::DefiniteNoMutation,
        }
    }

    fn requested(error: ServiceError) -> Self {
        Self {
            error,
            disposition: FreshMutationDisposition::RequestedMutationPossible,
        }
    }

    fn into_service(self) -> ServiceError {
        self.error
    }
}

impl From<ServiceError> for DecideOpError {
    fn from(error: ServiceError) -> Self {
        Self::definite(error)
    }
}

type PreparedTickRound = (
    Arc<FedimintJournal>,
    Option<Arc<Runtime>>,
    ProbeFacts,
    TickPolicy,
    u64,
    u64,
    Option<Intent>,
);

struct ActorState {
    runtime: Option<Arc<Runtime>>,
    journal: Arc<FedimintJournal>,
    executor: Arc<dyn Executor>,
    registry: Registry,
    waiters: HashMap<IdempotencyKey, Vec<PendingWaiter>>,
    policy: Policy,
    perform_timeout: Option<std::time::Duration>,
    generation: u64,
    /// Bumped on every accepted PutPolicy. A tick round is stamped with the value it
    /// planned under; CommitTick refuses if this has since advanced (§6a P1 ruling).
    policy_generation: u64,
    /// Non-forgeable fresh-probe authority and non-wrapping policy identity. The authority is
    /// stable for this actor; the version identity is replaced after each durable `PutPolicy`.
    probe_policy_authority: Arc<()>,
    probe_policy_version: Arc<()>,
    /// Membership world view used by off-actor planning.  Unlike per-goal
    /// admission counters it invalidates every plan after Join/Recover progress.
    world_generation: u64,
    world_generation_poisoned: bool,
    /// Per-actor capability authority for terminal leases.  Epochs alone collide across actors,
    /// so a lease must prove both its epoch and the actor which issued it before it can alter
    /// balance generations or release the live fence.
    external_terminal_authority: Arc<()>,
    external_terminal_epoch: u64,
    external_terminal_live: bool,
    external_terminal_poisoned: bool,
    /// Per-actor capability authority for membership-publication leases.  Epochs alone collide
    /// across actors, so a lease must prove its issuer before it can advance the membership
    /// world or release the live publication fence.
    membership_authority: Arc<()>,
    membership_epoch: u64,
    membership_live: bool,
    membership_poisoned: bool,
    probe_budget: ProbeBudgetState,
    /// Exactly one detached recovery task reconciles all ownership made unknown by
    /// post-`DriverFinished` journal-read faults.  The generation closes the race
    /// between that task's durable scan and a later fault arriving on the actor.
    ownership_recovery_active: bool,
    ownership_recovery_generation: u64,
    policy_wake: tokio::sync::watch::Sender<u64>,
    tick_batches: Vec<TickBatch>,
    /// Full rows captured for ordered, one-child scheduler handoff. Only `ReconcileDecide`
    /// advances this queue; durable ownership recovery must not race an off-actor planner by
    /// clearing it.
    parked_evacuation_markers: Vec<Intent>,
    /// The exact parked parent offered to the current scheduler cycle. A later reconciliation
    /// may release only this row: the remaining queue was never handed to that cycle's planner.
    parked_evacuation_handoff: Option<Intent>,
    /// A deliberate planner clear, or a recovery-only claim which atomically consumes this exact
    /// structural marker, makes the next retry's renewed marker a continuation of the current
    /// scheduler decision, not new policy information. Suppress that one wake only after the
    /// clear/claim is armed; DriverFinished cleans up abandoned attempts.
    marker_clear_wake_suppression: BTreeSet<(IdempotencyKey, u32)>,
    /// Per-logical-goal admission counters.  Unlike the pending scan, these
    /// deliberately retain a terminal admission long enough for an older
    /// off-actor plan to see that it was superseded.
    goal_admissions: BTreeMap<AllocatorGoal, u64>,
    /// A durable-admission ambiguity is not an arithmetic overflow.  Retain
    /// its diagnostic so all later plan/commit refusals describe the actual
    /// fail-closed condition.
    goal_admissions_poisoned: Option<String>,
    tick_token_authority: Arc<()>,
    balance_generations: BTreeMap<FederationId, u64>,
    balance_generations_poisoned: bool,
    /// A durable intent transition completed but its affected balance facts could not be
    /// identified.  Issuing a token after that would let a tick treat an unknown fact as fresh.
    /// This is deliberately fail-closed rather than merely diagnostic.
    balance_facts_poisoned: Option<String>,
    balance_facts_authority: Arc<()>,
    #[cfg(test)]
    fail_after_fresh_admission: Option<IdempotencyKey>,
}

/// What a reconciliation pass may do with a current qualifying structural marker. This is actor
/// policy rather than a public boolean because preserving, handing work to a planner, and
/// recovery-only re-driving have materially different money-path authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconciliationMarkerPolicy {
    PreservePlannerOwned,
    CaptureForPlanner,
    RedriveWithoutPlanner,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    mut receiver: mpsc::Receiver<Command>,
    sender: mpsc::WeakSender<Command>,
    accepting: Arc<AtomicBool>,
    runtime: Option<Arc<Runtime>>,
    journal: Arc<FedimintJournal>,
    executor: Arc<dyn Executor>,
    policy: Policy,
    perform_timeout: Option<std::time::Duration>,
    registry: driver::Registry,
    policy_wake: tokio::sync::watch::Sender<u64>,
) {
    #[cfg(test)]
    let scheduler_fixture_executor = runtime
        .as_deref()
        .is_some_and(Runtime::scheduler_tick_fixture_enabled_for_test);
    let executor = runtime.as_ref().map_or(executor.clone(), |runtime| {
        #[cfg(test)]
        if scheduler_fixture_executor {
            executor.clone()
        } else {
            Arc::new(runtime.service_executor(Some(policy.per_fed_cap)))
        }
        #[cfg(not(test))]
        Arc::new(runtime.service_executor(Some(policy.per_fed_cap)))
    });
    let budget_journal = journal.clone();
    let budget_policy = policy.clone();
    let mut budget_loader = Some(tokio::spawn(async move {
        load_probe_budget(&budget_journal, &budget_policy).await
    }));
    let mut state = ActorState {
        runtime,
        journal,
        executor,
        registry,
        waiters: HashMap::new(),
        policy,
        perform_timeout,
        generation: 0,
        policy_generation: 0,
        probe_policy_authority: Arc::new(()),
        probe_policy_version: Arc::new(()),
        world_generation: 0,
        world_generation_poisoned: false,
        external_terminal_authority: Arc::new(()),
        external_terminal_epoch: 0,
        external_terminal_live: false,
        external_terminal_poisoned: false,
        membership_authority: Arc::new(()),
        membership_epoch: 0,
        membership_live: false,
        membership_poisoned: false,
        probe_budget: ProbeBudgetState {
            entries: Vec::new(),
            load_error: Some("probe budget state is still loading".to_owned()),
        },
        ownership_recovery_active: false,
        ownership_recovery_generation: 0,
        policy_wake,
        tick_batches: Vec::new(),
        parked_evacuation_markers: Vec::new(),
        parked_evacuation_handoff: None,
        marker_clear_wake_suppression: BTreeSet::new(),
        goal_admissions: BTreeMap::new(),
        goal_admissions_poisoned: None,
        tick_token_authority: Arc::new(()),
        balance_generations: BTreeMap::new(),
        balance_generations_poisoned: false,
        balance_facts_poisoned: None,
        balance_facts_authority: Arc::new(()),
        #[cfg(test)]
        fail_after_fresh_admission: None,
    };

    loop {
        let deadline = state.next_deadline();
        tokio::select! {
            command = receiver.recv() => {
                let Some(command) = command else {
                    accepting.store(false, Ordering::Release);
                    abort_loader(&mut budget_loader);
                    for abort in driver::aborts(&state.registry) {
                        abort.abort();
                    }
                    state.drain_waiters(ServiceError::ActorStopped);
                    break;
                };
                if let Command::Shutdown { reply } = command {
                    accepting.store(false, Ordering::Release);
                    abort_loader(&mut budget_loader);
                    let (finish, finished) = oneshot::channel();
                    let token = ShutdownToken {
                        aborts: driver::aborts(&state.registry),
                        registry: state.registry.clone(),
                        finish: Some(finish),
                    };
                    if let Err(Ok(token)) = reply.send(Ok(token)) {
                        // Caller vanished: fall back to the Drop path (abort + finish
                        // immediately; the actor's drain below still lands everything an
                        // undropped driver already submitted).
                        drop(token);
                    }
                    let _ = finished.await;
                    while let Ok(command) = receiver.try_recv() {
                        state.handle(command, sender.upgrade().map(|sender| WalletClient {
                            sender,
                            accepting: accepting.clone(),
                        }).as_ref(), false).await;
                    }
                    state.drain_waiters(ServiceError::ShuttingDown);
                    break;
                }
                let client = sender.upgrade().map(|sender| WalletClient {
                    sender,
                    accepting: accepting.clone(),
                });
                state.handle(command, client.as_ref(), true).await;
            }
            () = wait_for_deadline(deadline) => state.expire_waiters(),
            result = wait_for_budget_loader(&mut budget_loader) => {
                budget_loader.take();
                state.probe_budget = match result {
                    Ok(budget) => budget,
                    Err(error) => ProbeBudgetState {
                        entries: Vec::new(),
                        load_error: Some(format!("probe budget loader failed: {error}")),
                    },
                };
            }
        }
    }
}

async fn wait_for_budget_loader(
    loader: &mut Option<JoinHandle<ProbeBudgetState>>,
) -> Result<ProbeBudgetState, tokio::task::JoinError> {
    match loader {
        Some(loader) => loader.await,
        None => std::future::pending().await,
    }
}

fn abort_loader(loader: &mut Option<JoinHandle<ProbeBudgetState>>) {
    if let Some(loader) = loader.take() {
        loader.abort();
    }
}

fn reserve_action_for_commit(reservations: &mut Reservations, action: &Action) {
    let outbound = match action {
        Action::Move {
            from,
            amount,
            fee_cap,
            ..
        }
        | Action::Evacuate {
            from,
            amount,
            fee_cap,
            ..
        }
        | Action::Pay {
            from,
            amount,
            fee_cap,
            ..
        } => Some((*from, Msat(amount.0.saturating_add(fee_cap.0)))),
        _ => None,
    };
    if let Some((from, amount)) = outbound {
        let slot = reservations.per_fed_outbound.entry(from).or_insert(Msat(0));
        slot.0 = slot.0.saturating_add(amount.0);
    }

    let inbound = match action {
        Action::Move { to, amount, .. }
        | Action::Evacuate { to, amount, .. }
        | Action::DirectInflow { to, amount, .. }
        | Action::Receive { to, amount, .. } => Some((*to, *amount)),
        _ => None,
    };
    if let Some((to, amount)) = inbound {
        let slot = reservations.per_fed_inbound.entry(to).or_insert(Msat(0));
        slot.0 = slot.0.saturating_add(amount.0);
    }
    let target_credit = match action {
        Action::Move { to, amount, .. } | Action::Evacuate { to, amount, .. } => {
            Some((*to, *amount))
        }
        _ => None,
    };
    if let Some((to, amount)) = target_credit {
        let slot = reservations
            .per_fed_target_credit
            .entry(to)
            .or_insert(Msat(0));
        slot.0 = slot.0.saturating_add(amount.0);
    }
}

/// A storage error after a fresh journal mutation is ambiguous: the write may have committed even
/// though its caller observed an error.  Keep the remainder of this batch safe without treating a
/// replay of an already-existing key as a second mutation.
fn fold_unknown_fresh_commit_mutation(
    blocked: &mut GoalBlockers,
    commit_reservations: &mut Reservations,
    decision: &AllocatorDecision,
    occurrence: Occurrence,
    decision_existed: bool,
    mutation_unknown: bool,
) {
    if mutation_unknown && !decision_existed {
        blocked.hold_decision(decision, Actor::Agent { occurrence });
        reserve_action_for_commit(commit_reservations, &decision.action);
    }
}

fn executable_destination(action: &Action) -> Option<FederationId> {
    match action {
        Action::Move { to, .. } | Action::Evacuate { to, .. } => Some(*to),
        _ => None,
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// The actor-owned tick plan. Standalone tick/status call this same function directly; the daemon
/// reaches it through `DecideTickRound`, so route pricing and `decide()` cannot drift.
#[derive(Clone, Debug)]
pub(crate) struct PlannedTickRound {
    /// What this tick may act on: everything `decide()` produced MINUS the work suppressed by the
    /// conflict projection. Route preflight, apply and commit all read this list.
    pub(crate) decisions: Vec<AllocatorDecision>,
    /// The decisions the conflict projection withheld (br-p93) — work whose logical goal another
    /// intent already owns. Kept separately from admitted work because it did not complete route
    /// preflight and can only supply the narrow source-associated pin voucher.
    pub(crate) suppressed: Vec<AllocatorDecision>,
    /// Ordinary planned work deferred by an exclusive replacement. It is deliberately distinct
    /// from conflict suppression: deferred work must not vouch for an in-flight holder.
    pub(crate) replacement_deferred: Vec<AllocatorDecision>,
    /// Funding goals the move floor withheld (br-0vg). Diagnostic only: never admitted, never
    /// reserved, never journaled per tick — `status` is where an operator reads them.
    pub(crate) deferred: Vec<wallet_core::DeferredFunding>,
    pub(crate) probes: Vec<(FederationId, crate::probe::ProbeResult)>,
    pub(crate) active_probes: BTreeMap<FederationId, ActiveProbeVerdict>,
    pub(crate) snapshot: AllocatorSnapshot,
    /// The exact blocker projection used for pinned-input validation. A
    /// replacement shadow excludes only its retiring parent.
    pub(crate) blocked: GoalBlockers,
    pub(crate) replacement: Option<super::EvacuationReplacementPlan>,
    pub(crate) marker_disposition: Option<super::EvacuationMarkerDisposition>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplacementExchangeOutcome {
    Committed,
    Uncommitted,
}

/// Whether a failed marked-evacuation replacement is known not to have crossed
/// the journal exchange boundary.  This is deliberately a typed contract:
/// marker cleanup must not infer the boundary from an error message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplacementFailureDisposition {
    DefiniteUncommitted,
    /// The journal autocommit closure rejected the exchange before commit. The
    /// marker remains durable repair evidence even though no child was written.
    DefiniteUncommittedRetainMarker,
    PostExchangeAmbiguous,
}

/// Internal failure result for the actor-owned half of a marked evacuation
/// replacement.  The caller owns tick terminalization and the exact-parent
/// marker CAS, so it needs the exchange-boundary proof alongside the public
/// error it must return or report.
#[derive(Debug)]
struct ReplacementCommitError {
    error: ServiceError,
    disposition: ReplacementFailureDisposition,
}

impl ReplacementCommitError {
    fn definite_uncommitted(error: ServiceError) -> Self {
        Self {
            error,
            disposition: ReplacementFailureDisposition::DefiniteUncommitted,
        }
    }

    fn post_exchange_ambiguous(error: ServiceError) -> Self {
        Self {
            error,
            disposition: ReplacementFailureDisposition::PostExchangeAmbiguous,
        }
    }

    fn definite_uncommitted_retain_marker(error: ServiceError) -> Self {
        Self {
            error,
            disposition: ReplacementFailureDisposition::DefiniteUncommittedRetainMarker,
        }
    }
}

/// Route observations are cycle-scoped, not shadow-plan-scoped.  In
/// particular, when a qualifying marker no longer yields a same-source child,
/// normal fallback inherits the shadow's spent quote budget, successful
/// prices, and failed concrete-preflight facts.
struct RoutePlanningState {
    budget: Option<crate::route_econ::RouteQuoteBudget>,
    priced: BTreeMap<(FederationId, FederationId), RouteEconomics>,
    invalidated: BTreeSet<(FederationId, FederationId)>,
}

pub(crate) async fn plan_tick_round(
    journal: &FedimintJournal,
    runtime: Option<&Runtime>,
    probes: Vec<(FederationId, crate::probe::ProbeResult)>,
    policy: &TickPolicy,
    sensed_at_ms: u64,
    route_budget: Option<crate::route_econ::RouteQuoteBudget>,
) -> Result<PlannedTickRound, ExecError> {
    plan_tick_round_for_marker(
        journal,
        runtime,
        probes,
        policy,
        sensed_at_ms,
        route_budget,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn plan_tick_round_for_marker(
    journal: &FedimintJournal,
    runtime: Option<&Runtime>,
    probes: Vec<(FederationId, crate::probe::ProbeResult)>,
    policy: &TickPolicy,
    sensed_at_ms: u64,
    route_budget: Option<crate::route_econ::RouteQuoteBudget>,
    preferred_replacement_parent: Option<&Intent>,
) -> Result<PlannedTickRound, ExecError> {
    // A shadow round is exclusive only while it can actually produce the
    // successor it reserved capacity for.  Do not turn a disappeared
    // same-source evacuation into an idle cycle: the old marker remains live,
    // but independently eligible work still deserves the ordinary plan.
    let mut probes = probes;
    let mut route_state = RoutePlanningState {
        budget: route_budget,
        priced: BTreeMap::new(),
        invalidated: BTreeSet::new(),
    };
    let shadow = plan_tick_round_inner(
        journal,
        runtime,
        probes.as_mut_slice(),
        policy,
        sensed_at_ms,
        &mut route_state,
        true,
        preferred_replacement_parent,
    )
    .await?;
    if shadow.marker_disposition.is_some() && shadow.replacement.is_none() {
        let mut ordinary = plan_tick_round_inner(
            journal,
            runtime,
            probes.as_mut_slice(),
            policy,
            sensed_at_ms,
            &mut route_state,
            false,
            None,
        )
        .await?;
        // The exclusive shadow did not yield the only admissible child.  Preserve its exact
        // one-cycle marker disposition while ordinary work gets this cycle's normal plan.
        ordinary.marker_disposition = shadow.marker_disposition;
        return Ok(ordinary);
    }
    Ok(shadow)
}

async fn qualifying_replacement_parent(
    journal: &FedimintJournal,
    policy: &TickPolicy,
    preferred: Option<&Intent>,
) -> Result<Option<Intent>, ExecError> {
    let qualifies = |intent: &Intent| {
        matches!(intent.actor, Actor::Agent { occurrence } if occurrence.0 < u64::MAX)
            && matches!(intent.action, Action::Evacuate { .. })
            && intent.evacuation_refusal.as_ref().is_some_and(|evidence| {
                wallet_core::evacuation_cap_qualifies_replacement(
                    evidence,
                    wallet_core::EvacFeeCap {
                        base_msat: policy.evac_fee_base_msat,
                        bps: policy.evac_fee_bps,
                    },
                )
            })
    };
    if let Some(preferred) = preferred {
        return Ok(journal
            .get(&preferred.idempotency_key)
            .await?
            .filter(|current| current == preferred && qualifies(current)));
    }
    Ok(journal.pending().await?.into_iter().find(qualifies))
}

#[allow(clippy::too_many_arguments)]
async fn plan_tick_round_inner(
    journal: &FedimintJournal,
    runtime: Option<&Runtime>,
    probes: &mut [(FederationId, crate::probe::ProbeResult)],
    policy: &TickPolicy,
    sensed_at_ms: u64,
    route_state: &mut RoutePlanningState,
    replacements_enabled: bool,
    preferred_replacement_parent: Option<&Intent>,
) -> Result<PlannedTickRound, ExecError> {
    let candidates = journal.list_candidates_report().await?;
    let joined = journal.list_federations().await?;
    let auto_joined = probe_gated_members(
        joined.into_iter().map(|(id, _)| id),
        candidates
            .candidates
            .iter()
            .map(|(id, record)| (*id, record.state)),
    );
    let reservations = project_allocator_reservations(journal).await?;
    // A structural marker remains retryable unless the current cap grants a
    // component-wise monotone, *effective* improvement at an observed net.
    // Choose one parent in pending-index order and reserve this cycle solely
    // for its shadow replacement.
    let replacement_parent = if replacements_enabled {
        #[cfg(test)]
        journal.wait_before_replacement_scan_for_test().await;
        qualifying_replacement_parent(journal, policy, preferred_replacement_parent).await?
    } else {
        None
    };
    let mut shadow_policy = policy.clone();
    let mut shadow_reservations = reservations.clone();
    if let Some(parent) = &replacement_parent {
        shadow_policy.blocked = policy.blocked.excluding_key(&parent.idempotency_key);
        shadow_reservations =
            project_allocator_reservations_excluding(journal, &parent.idempotency_key).await?;
    }
    let active_policy = if replacement_parent.is_some() {
        &shadow_policy
    } else {
        policy
    };
    let active_reservations = if replacement_parent.is_some() {
        &shadow_reservations
    } else {
        &reservations
    };
    let mut evacuation_fallback: Option<(FederationId, PlannedTickRound)> = None;
    // The round whose refusal first named an unroutable designated pair. A re-designation that ends
    // up funding nothing reverts to it so that refusal (§Q5) is not lost.
    let mut route_blocked_fallback: Option<PlannedTickRound> = None;
    let mut route_revisions = 0usize;

    loop {
        let round = build_tick_round(
            journal,
            runtime,
            probes,
            active_policy,
            sensed_at_ms,
            &auto_joined,
            active_reservations,
            route_state.budget.as_mut(),
            &mut route_state.priced,
            &route_state.invalidated,
        )
        .await?;
        if let Some((source, fallback)) = &evacuation_fallback {
            let still_evacuating = round.decisions.iter().any(|decision| {
                matches!(decision.action, Action::Evacuate { from, .. } if from == *source)
            });
            if !still_evacuating {
                return Ok(finish_replacement_round(
                    fallback.clone(),
                    replacement_parent.as_ref(),
                ));
            }
        }
        // A discarded reconcile cycle supplies no budget. It pays for neither route economics nor
        // the pre-existing concrete route preflight.
        let (Some(runtime), Some(budget)) = (runtime, route_state.budget.as_ref()) else {
            return Ok(finish_replacement_round(round, replacement_parent.as_ref()));
        };
        let Some(problem) = budget
            .run_before_deadline(runtime.first_move_route_problem(&round.decisions))
            .await
        else {
            tracing::warn!(
                "tick: route-work deadline expired before concrete preflight completed; \
                 leaving final validation to perform"
            );
            return Ok(finish_replacement_round(round, replacement_parent.as_ref()));
        };
        let Some(problem) = problem else {
            // No EMITTED move failed preflight — but `decide()` may have route-BLOCKED the designated
            // pair (`Unroutable`/`UneconomicAtAnySize` → a refusal, no move). On origin/main that pair
            // was always an emitted move whose preflight failure marked the destination unavailable and
            // re-drove allocation onto another routable pairing; pre-classifying the skip loses that
            // re-designation and wedges a ≥3-federation wallet on the unroutable top-scored pairing.
            // Restore it: drop the blocked destination and re-drive so allocation re-designates onto
            // another eligible pairing, keeping the FIRST such round as the fallback.
            if let Some((_, to)) = first_route_blocked_designation(&round) {
                if crate::runtime::mark_gateway_unavailable(probes, to) {
                    tracing::warn!(
                        to = %to.to_hex(),
                        "tick: the designated funding pair is unroutable this cycle; re-driving \
                         allocation onto another eligible pairing"
                    );
                    route_blocked_fallback.get_or_insert_with(|| round.clone());
                    route_revisions += 1;
                    if route_revisions > probes.len() {
                        return Ok(finish_replacement_round(
                            route_blocked_fallback.take().unwrap_or(round),
                            replacement_parent.as_ref(),
                        ));
                    }
                    continue;
                }
            }
            // No further route-blocked pair to re-drive. If a re-designation was in progress and this
            // final round funds nothing, no routable alternative exists, so revert to the round whose
            // refusal names the original block (§Q5 keeps that diagnostic visible). Otherwise this
            // round — the re-designated routable pairing, or the original when none was needed — is
            // the result.
            if let Some(fallback) = route_blocked_fallback.take() {
                let funds_a_move = round
                    .decisions
                    .iter()
                    .any(|decision| matches!(decision.action, Action::Move { .. }));
                if !funds_a_move {
                    return Ok(finish_replacement_round(
                        fallback,
                        replacement_parent.as_ref(),
                    ));
                }
            }
            return Ok(finish_replacement_round(round, replacement_parent.as_ref()));
        };
        if problem.evacuation_source_route
            && round.decisions.iter().any(|decision| {
                matches!(decision.action, Action::Evacuate { from, .. } if from == problem.from)
            })
        {
            evacuation_fallback = Some((problem.from, round.clone()));
        }

        // A concrete preflight failure invalidates the exact pair's earlier `Routable` evidence.
        // Keep it in the accumulator only to prevent another quote pass; `build_tick_round`
        // excludes invalidated entries from every later snapshot.
        route_state.invalidated.insert((problem.from, problem.to));
        let changed = crate::runtime::mark_gateway_unavailable(probes, problem.mark_unavailable);
        tracing::warn!(
            from = %problem.from.to_hex(),
            to = %problem.to.to_hex(),
            marked_unavailable = %problem.mark_unavailable.to_hex(),
            gateway = %problem
                .gateway
                .as_ref()
                .map_or("<none>", |gateway| gateway.0.as_str()),
            error = %problem.error,
            "tick: planned send-required route failed gateway validation; revising the fundable set"
        );
        if !changed {
            return Ok(finish_replacement_round(round, replacement_parent.as_ref()));
        }
        route_revisions += 1;
        if route_revisions > probes.len() {
            return Ok(finish_replacement_round(round, replacement_parent.as_ref()));
        }
    }
}

/// Turn a shadow allocation into the only work in an exclusive replacement
/// round. If allocation no longer yields a same-source evacuation, carry an exact one-cycle
/// disposition which clears only the coherent Pending marker at commit.  The caller then returns
/// to ordinary retry on its next normal cycle; no immediate policy wake is permitted.
fn finish_replacement_round(
    mut round: PlannedTickRound,
    parent: Option<&Intent>,
) -> PlannedTickRound {
    let Some(parent) = parent else {
        return round;
    };
    let source = match parent.action {
        Action::Evacuate { from, .. } => from,
        _ => return round,
    };
    let fresh = round
        .decisions
        .iter()
        .find(|decision| matches!(decision.action, Action::Evacuate { from, .. } if from == source))
        .cloned();
    // Preserve all non-child planned work as suppressed facts.  The replacement is exclusive at
    // commit (only `replacement.fresh` is admitted), but pinned-input validation must still see
    // both the allocator's original suppressions and the ordinary work deferred by exclusivity.
    // Otherwise a third holder can disappear from the validation projection merely because the
    // marker selected a child. Keep these separate from conflict suppression: the latter has
    // narrow holder-voucher semantics in pinned-input validation.
    if let Some(fresh) = fresh.as_ref() {
        round.replacement_deferred = round
            .decisions
            .iter()
            .filter(|decision| *decision != fresh)
            .cloned()
            .collect();
    }
    round.decisions.clear();
    let evidence = parent
        .evacuation_refusal
        .clone()
        .expect("qualifying replacement parent carries evidence");
    round.replacement = fresh.map(|fresh| super::EvacuationReplacementPlan {
        parent: parent.clone(),
        old_key: parent.idempotency_key.clone(),
        old_attempt: parent.attempt,
        evidence: evidence.clone(),
        fresh,
    });
    if round.replacement.is_none() {
        round.marker_disposition = Some(super::EvacuationMarkerDisposition {
            parent: parent.clone(),
        });
    }
    round
}

/// The off-actor half of a tick decision: the network-heavy route pricing + concrete route
/// preflight (via [`plan_tick_round`]) plus the pure pinned-input check. It runs in a task spawned
/// off the actor's `select!` loop so pricing a stalled federation cannot block the single-owner
/// actor. `planned_generation` is the generation captured on the actor turn by
/// [`ActorState::prepare_tick_round`]; the commit-time check validates it is still current, so a
/// `PutPolicy` landing during this off-actor window refuses the stale batch rather than admitting it.
async fn plan_tick_off_actor(
    journal: Arc<FedimintJournal>,
    runtime: Option<Arc<Runtime>>,
    facts: ProbeFacts,
    policy: TickPolicy,
    planned_generation: u64,
    planned_world_generation: u64,
    preferred_replacement_parent: Option<Intent>,
) -> ServiceResult<TickRound> {
    let route_budget = facts
        .price_routes
        .then(|| crate::route_econ::RouteQuoteBudget::starting_at(facts.now_ms));
    #[cfg(test)]
    let fixture_round = runtime
        .as_deref()
        .and_then(Runtime::scheduler_tick_test_plan);
    let round = {
        #[cfg(test)]
        if let Some(round) = fixture_round {
            PlannedTickRound {
                decisions: round.decisions,
                suppressed: round.suppressed,
                replacement_deferred: round.replacement_deferred,
                deferred: round.deferred,
                probes: round.probes,
                active_probes: round.active_probes,
                snapshot: round.snapshot,
                blocked: round.blockers,
                replacement: round.replacement,
                marker_disposition: round.marker_disposition,
            }
        } else {
            plan_tick_round_for_marker(
                journal.as_ref(),
                runtime.as_deref(),
                facts.probes,
                &policy,
                facts.now_ms,
                route_budget,
                preferred_replacement_parent.as_ref(),
            )
            .await
            .map_err(storage)?
        }
        #[cfg(not(test))]
        {
            plan_tick_round_for_marker(
                journal.as_ref(),
                runtime.as_deref(),
                facts.probes,
                &policy,
                facts.now_ms,
                route_budget,
                preferred_replacement_parent.as_ref(),
            )
            .await
            .map_err(storage)?
        }
    };
    let mut validation_decisions = round
        .replacement
        .as_ref()
        .map(|replacement| vec![replacement.fresh.clone()])
        .unwrap_or_else(|| round.decisions.clone());
    validation_decisions.extend(round.replacement_deferred.clone());
    let problems = pinned_input_problems(
        &policy,
        &round.snapshot,
        &round.probes,
        &validation_decisions,
        &round.suppressed,
        &round.blocked,
    );
    if !problems.is_empty() {
        return Err(ServiceError::Storage(format!(
            "tick: {}",
            problems.join("; ")
        )));
    }
    Ok(TickRound {
        decisions: round.decisions,
        replacement_deferred: round.replacement_deferred,
        occurrence: facts.occurrence,
        spending_fed: round.snapshot.spending_fed,
        planned_generation,
        planned_world_generation,
        admission_snapshot: facts.admission_snapshot,
        replacement: round.replacement,
        marker_disposition: round.marker_disposition,
    })
}

/// The designated funding pair (either direction) that `decide()` route-BLOCKED this round: a pair
/// whose `route_economics_by_pair` status is `Unroutable`/`UneconomicAtAnySize` and into whose
/// destination no move was emitted. This is the pre-classified analogue of a concrete preflight
/// failure — [`plan_tick_round`] drops the destination and re-drives so allocation re-designates onto
/// a routable pairing instead of wedging. Returns the `(from, to)` of the block; `to` is the
/// destination to drop.
fn first_route_blocked_designation(
    round: &PlannedTickRound,
) -> Option<(FederationId, FederationId)> {
    let (Some(spending), Some(standby)) = (round.snapshot.spending_fed, round.snapshot.standby_fed)
    else {
        return None;
    };
    if spending == standby {
        return None;
    }
    // The single designated pair, both directions — the only pairs `decide()` funds.
    for (from, to) in [(standby, spending), (spending, standby)] {
        let blocked = matches!(
            round
                .snapshot
                .route_economics_by_pair
                .get(&(from, to))
                .map(|economics| economics.status),
            Some(RouteStatus::Unroutable | RouteStatus::UneconomicAtAnySize)
        );
        // Only re-designate when `decide()` actually WANTED a non-dust funding of `to` that the
        // ROUTE blocked — a refusal naming `to` that is (a) route/shortfall-caused, not `OverCap`
        // (a cap-full destination is not helped by re-designating away from it), and (b) over a
        // real, at-or-above-floor gap. A sub-floor DUST shortfall must not trigger an unrequested
        // full-target rebalance onto some other fed: an `Unroutable` pair returns silently there
        // (no refusal), but an `UneconomicAtAnySize` pair still emits its §Q5 `UneconomicRoute`
        // refusal even at dust (`allocator::fund_into`, the `!uneconomic` skip), so the reason alone
        // cannot distinguish dust — gate on the gap itself via the refusal's diagnostics.
        let route_refused = round.decisions.iter().any(|decision| {
            let Action::RefuseInflow {
                fed,
                reason,
                diagnostics,
            } = &decision.action
            else {
                return false;
            };
            *fed == to
                && *reason != ReasonCode::OverCap
                && diagnostics
                    .want
                    .zip(diagnostics.min_move)
                    .is_some_and(|(want, min_move)| want.0 >= min_move.0)
        });
        let funded = round
            .decisions
            .iter()
            .any(|decision| matches!(decision.action, Action::Move { to: dest, .. } if dest == to));
        if blocked && route_refused && !funded {
            return Some((from, to));
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn build_tick_round(
    journal: &FedimintJournal,
    runtime: Option<&Runtime>,
    probes: &[(FederationId, crate::probe::ProbeResult)],
    policy: &TickPolicy,
    sensed_at_ms: u64,
    auto_joined: &BTreeSet<FederationId>,
    reservations: &Reservations,
    route_budget: Option<&mut crate::route_econ::RouteQuoteBudget>,
    priced: &mut BTreeMap<(FederationId, FederationId), RouteEconomics>,
    invalidated: &BTreeSet<(FederationId, FederationId)>,
) -> Result<PlannedTickRound, ExecError> {
    let scorer_policy = ScorerPolicy::default();
    let preliminary = build_snapshot(
        probes,
        policy,
        &scorer_policy,
        auto_joined,
        &BTreeMap::new(),
    );
    let active_probes = active_probe_verdicts(
        journal,
        probes,
        preliminary.spending_fed,
        &policy.probe_gate_policy,
        sensed_at_ms,
    )
    .await;
    let mut snapshot = build_snapshot(probes, policy, &scorer_policy, auto_joined, &active_probes);
    snapshot.reservations = reservations.clone();
    if let (Some(runtime), Some(budget)) = (runtime, route_budget) {
        runtime
            .price_missing_routes(&snapshot, budget, priced, &policy.blocked)
            .await;
    }
    snapshot.route_economics_by_pair = priced
        .iter()
        .filter(|(pair, _)| !invalidated.contains(pair))
        .map(|(pair, economics)| (*pair, economics.clone()))
        .collect();
    // br-p93: withhold work that duplicates an in-flight allocator goal inside `decide`, BEFORE
    // the allocator charges its intra-tick `credited`/`debited` reservations. A discarded
    // evacuation must not consume destination room and thereby suppress an independent evacuation
    // sharing that destination. This also happens before concrete route preflight and the revision
    // loop above can run any I/O on the withheld work.
    let actor = Actor::Agent {
        occurrence: policy.occurrence,
    };
    let wallet_core::AllocatorOutcome {
        decisions,
        suppressed,
        deferred,
    } = wallet_core::decide_with_diagnostics(&snapshot, policy.occurrence, &policy.blocked);
    for goal in &deferred {
        tracing::debug!(
            dest = %goal.dest.to_hex(),
            reason = ?goal.reason,
            want_msat = goal.want.0,
            floor_msat = goal.floor.0,
            floor_source = ?goal.floor_source,
            "tick: funding goal deferred below the move floor"
        );
    }
    for decision in &suppressed {
        tracing::warn!(
            key = %decision.idempotency_key.0,
            holder = ?policy.blocked.blocking_holder(decision, actor)
                .map(|key| key.0.as_str()),
            "tick: withholding a decision that conflicts with in-flight allocator work"
        );
    }
    Ok(PlannedTickRound {
        decisions,
        suppressed,
        replacement_deferred: Vec::new(),
        deferred,
        probes: probes.to_vec(),
        active_probes,
        snapshot,
        blocked: policy.blocked.clone(),
        replacement: None,
        marker_disposition: None,
    })
}

pub(crate) async fn active_probe_verdicts(
    journal: &FedimintJournal,
    probes: &[(FederationId, crate::probe::ProbeResult)],
    spending: Option<FederationId>,
    gate_policy: &ProbePolicy,
    sensed_at_ms: u64,
) -> BTreeMap<FederationId, ActiveProbeVerdict> {
    let Some(source) = spending else {
        return BTreeMap::new();
    };
    let mut active = BTreeMap::new();
    for (id, _) in probes {
        if *id == source {
            continue;
        }
        match journal.probe_record(id).await {
            Ok(record) => {
                active.insert(
                    *id,
                    probe_verdict(
                        &record.map(|record| record.attempts).unwrap_or_default(),
                        source,
                        sensed_at_ms,
                        gate_policy,
                    ),
                );
            }
            Err(error) => tracing::warn!(
                federation = %id.to_hex(),
                ?error,
                "tick: unreadable probe record; omitting active-probe verdict"
            ),
        }
    }
    active
}

async fn project_strict_reservations(journal: &FedimintJournal) -> Result<Reservations, ExecError> {
    let intents = journal.reservation_intents().await?;
    Ok(wallet_core::project_reservations(&intents))
}

/// Load the artifact-aware reservation view used only by tokenized allocator work.
///
/// An undecodable derived record cannot be trusted, but it also must not make the allocator fail
/// open: omit it from the map so wallet-core applies the intent's strict action fallback. A
/// transient database read still aborts planning/commit.
async fn project_allocator_reservations(
    journal: &FedimintJournal,
) -> Result<Reservations, ExecError> {
    project_allocator_reservations_excluding(journal, &IdempotencyKey(String::new())).await
}

/// Artifact-aware allocator reservations with exactly one old intent omitted
/// for the replacement shadow.  This is not a general release primitive.
pub(crate) async fn project_allocator_reservations_excluding(
    journal: &FedimintJournal,
    excluded: &IdempotencyKey,
) -> Result<Reservations, ExecError> {
    let intents: Vec<_> = journal
        .reservation_intents()
        .await?
        .into_iter()
        .filter(|intent| intent.idempotency_key != *excluded)
        .collect();
    let mut records = BTreeMap::new();
    for intent in &intents {
        if !matches!(
            intent.action,
            Action::Move { .. } | Action::Evacuate { .. } | Action::DirectInflow { .. }
        ) {
            continue;
        }
        match journal.get_move(&intent.idempotency_key).await {
            Ok(Some(record)) => {
                records.insert(intent.idempotency_key.clone(), record);
            }
            Ok(None) => {}
            Err(ExecError::Permanent(error)) => {
                tracing::warn!(
                    key = %intent.idempotency_key.0,
                    %error,
                    "allocator reservation: corrupt derived record; retaining strict action reservation"
                );
            }
            Err(error @ ExecError::Retryable(_))
            | Err(error @ ExecError::StructuralEvacuationRefusal(_))
            | Err(error @ ExecError::Unsupported) => {
                return Err(error);
            }
        }
    }
    Ok(wallet_core::project_allocator_reservations(
        &intents, &records,
    ))
}

impl ActorState {
    fn structural_marker_qualifies(
        &self,
        evidence: &wallet_core::EvacuationRefusalEvidence,
    ) -> bool {
        wallet_core::evacuation_cap_qualifies_replacement(
            evidence,
            wallet_core::EvacFeeCap {
                base_msat: self.policy.evac_fee_base_msat,
                bps: self.policy.evac_fee_bps,
            },
        )
    }

    async fn handle(&mut self, command: Command, client: Option<&WalletClient>, intake: bool) {
        match command {
            Command::DecideOp { req, reply } => {
                let result = if intake {
                    match client {
                        Some(client) => self.decide_op(req, client).await,
                        None => Err(ServiceError::ActorStopped),
                    }
                } else {
                    Err(ServiceError::ShuttingDown)
                };
                let _ = reply.send(result);
            }
            Command::DecideProbe { candidate, reply } => {
                let result = if intake {
                    match client {
                        Some(client) => self.decide_probe(candidate, client).await,
                        None => Err(ServiceError::ActorStopped),
                    }
                } else {
                    Err(ServiceError::ShuttingDown)
                };
                let _ = reply.send(result);
            }
            Command::JournalTransition {
                key,
                transition,
                reply,
            } => {
                // Refresh is a read-bearing transition: even a key which no longer has an
                // in-memory reservation must report a ledger-read fault to its driver.  That
                // prevents a completed probe from releasing its sole owner before a later
                // retry has observed its durable terminal row.
                let refresh_budget = matches!(&transition, JournalTransition::Refresh);
                // Capture the action before a durable mutation. Looking the intent up afterwards
                // would both race an external writer and silently leave balance-fact tokens fresh
                // when that read failed.
                let balance_action = match self.transition_balance_action(&key, &transition).await {
                    Ok(action) => action,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                };
                let resolve_waiters = transition_may_resolve(&transition);
                let terminal_membership_transition = transition_terminal_status(&transition);
                // A marker is evidence, not a release primitive. Wake only when this actor's
                // current policy grants it a monotone, effective cap increase. Equal, crossed,
                // and decreased caps stay Pending for the normal reconciliation interval (a later
                // PutPolicy wakes separately), avoiding marker-write wake storms. A deliberate
                // marker clear suppresses exactly the next renewed marker, but merely *observing*
                // that suppression must not consume it before the reset is durable.
                let reset_suppression_key = match &transition {
                    JournalTransition::ResetRetryable {
                        expected_attempt, ..
                    } => Some((key.clone(), *expected_attempt)),
                    _ => None,
                };
                let suppress_marker_wake = reset_suppression_key
                    .as_ref()
                    .is_some_and(|key| self.marker_clear_wake_suppression.contains(key));
                let structural_marker_qualifies = !suppress_marker_wake
                    && match &transition {
                        JournalTransition::ResetRetryable {
                            structural_refusal: Some(evidence),
                            ..
                        } => self.structural_marker_qualifies(evidence),
                        _ => false,
                    };
                let reset_retryable =
                    matches!(&transition, JournalTransition::ResetRetryable { .. });
                // `Applied` is the successful reply shape for both an intent write and
                // process/probe bookkeeping.  Only the former invalidates balance facts:
                // DriverFinished tears down a registry entry and Refresh re-reads a probe
                // budget, neither changes an intent reservation.  In particular, do not
                // mistake either for an unknown-action write and poison every later tick.
                let intent_mutation = transition_is_intent_mutation(&transition);
                let finished = match &transition {
                    JournalTransition::DriverFinished {
                        generation,
                        expected_attempt,
                        retry_awaiter,
                    } => Some((*generation, *expected_attempt, *retry_awaiter)),
                    _ => None,
                };
                let mut result = self.apply_transition(&key, transition).await;
                if result.is_ok() && refresh_budget {
                    result = self
                        .refresh_probe_budget(&key)
                        .await
                        .map(|()| TransitionResult::Applied);
                }
                if let Ok(transition_result) = &result {
                    // A failed CAS did not mutate durable state.  Refresh and DriverFinished are
                    // probes/process bookkeeping respectively and intentionally have no action.
                    if intent_mutation && transition_mutated(transition_result) {
                        if let Some(action) = balance_action.as_ref() {
                            self.record_balance_change(action);
                            if terminal_membership_transition
                                && matches!(action, Action::Join { .. } | Action::Recover { .. })
                            {
                                self.bump_world_generation();
                            }
                        } else {
                            // A writer reporting a mutation without the identity captured before
                            // that write means balance-fact invalidation is no longer knowable.
                            // Fail closed rather than issuing a fresh-looking token.
                            self.poison_balance_facts(format!(
                                "intent transition {} reported a mutation without an affected action",
                                key.0
                            ));
                        }
                    }
                    if structural_marker_qualifies {
                        self.policy_wake.send_modify(|generation| {
                            *generation = generation.wrapping_add(1);
                        });
                    }
                    if reset_suppression_key.is_some() {
                        // `ResetRetryable` reports Applied only after its Executing->Pending write
                        // commits.  A pre-commit error must leave the one-shot entry available.
                        if let Some(key) = reset_suppression_key.as_ref() {
                            self.marker_clear_wake_suppression.remove(key);
                        }
                    }
                    if let Some((generation, expected_attempt, retry_awaiter)) = finished {
                        self.marker_clear_wake_suppression
                            .remove(&(key.clone(), expected_attempt));
                        if intake {
                            if let Some(client) = client {
                                self.finish_driver(
                                    &key,
                                    generation,
                                    expected_attempt,
                                    retry_awaiter,
                                    client,
                                )
                                .await;
                            }
                        } else {
                            driver::finish(&self.registry, &key, generation);
                        }
                    }
                    if resolve_waiters {
                        self.resolve_key(&key).await;
                    }
                    self.observe_tick_outcome(&key, finished.is_some()).await;
                } else if intent_mutation {
                    // A writer can commit and then report an error.  The pre-write lookup names the
                    // only balances which could have changed, so conservatively stale that scope
                    // rather than permanently refusing unrelated balance facts.  Only an attempted
                    // mutation with no known action has to fail closed globally.
                    if let Some(action) = balance_action.as_ref() {
                        self.record_balance_change(action);
                        if terminal_membership_transition
                            && matches!(action, Action::Join { .. } | Action::Recover { .. })
                        {
                            self.bump_world_generation();
                        }
                    } else {
                        self.poison_balance_facts(format!(
                            "intent transition {} returned an ambiguous durability error without an affected action",
                            key.0
                        ));
                    }
                    // A reset can commit then report an error. Re-read only the exact Pending
                    // attempt before consuming a one-shot suppression; a pre-commit error or a
                    // changed row retains it for this driver's later reset/finish.
                    if reset_retryable {
                        let durable_marker = self.journal.get(&key).await.map(|intent| {
                            intent.as_ref().is_some_and(|intent| {
                                intent.status == IntentStatus::Pending
                                    && reset_suppression_key.as_ref().is_some_and(
                                        |(_, expected_attempt)| intent.attempt == *expected_attempt,
                                    )
                                    && intent.evacuation_refusal.as_ref().is_some_and(|evidence| {
                                        self.structural_marker_qualifies(evidence)
                                    })
                            })
                        });
                        let durable_marker = durable_marker.unwrap_or(false);
                        if durable_marker && suppress_marker_wake {
                            if let Some(key) = reset_suppression_key.as_ref() {
                                self.marker_clear_wake_suppression.remove(key);
                            }
                        } else if durable_marker {
                            self.policy_wake.send_modify(|generation| {
                                *generation = generation.wrapping_add(1);
                            });
                        }
                    }
                }
                let _ = reply.send(result);
            }
            Command::SetOperationArtifact {
                key,
                expected_attempt,
                operation_id,
                invoice,
                reply,
            } => {
                let result = match self.artifact_write_action(&key, expected_attempt).await {
                    Ok(Some(action)) => {
                        match self
                            .journal
                            .set_operation_artifact_if_attempt(
                                &key,
                                expected_attempt,
                                operation_id,
                                invoice.as_ref(),
                            )
                            .await
                        {
                            Ok(changed) => {
                                if changed {
                                    self.record_balance_change(&action);
                                    self.resolve_key(&key).await;
                                }
                                Ok(changed)
                            }
                            Err(error) => {
                                // The attempt/action fence was read before this writer ran.  It may
                                // have committed before returning Err, so stale exactly its source.
                                self.record_balance_change(&action);
                                Err(storage(error))
                            }
                        }
                    }
                    Ok(None) => Ok(false),
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Command::PutMove {
                key,
                expected_attempt,
                record,
                reply,
            } => {
                let result = match self.artifact_write_action(&key, expected_attempt).await {
                    Ok(Some(action)) => {
                        match self
                            .journal
                            .put_move_if_attempt(&key, expected_attempt, record.as_ref())
                            .await
                        {
                            Ok(changed) => {
                                if changed {
                                    self.record_balance_change(&action);
                                    self.resolve_key(&key).await;
                                }
                                Ok(changed)
                            }
                            Err(error) => {
                                // As above, preserve other federations' usable balance facts while
                                // conservatively invalidating both sides of this known move.
                                self.record_balance_change(&action);
                                Err(storage(error))
                            }
                        }
                    }
                    Ok(None) => Ok(false),
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Command::Snapshot { scope, reply } => {
                let _ = reply.send(self.snapshot(scope).await);
            }
            Command::ResolveAwait {
                key,
                target,
                deadline,
                waiter,
            } => {
                if !intake {
                    let _ = waiter.send(Err(ServiceError::ShuttingDown));
                } else {
                    self.resolve_or_park(key, target, deadline, waiter).await;
                }
            }
            Command::ReconcileDecide { reply } => {
                let result = if intake {
                    match client {
                        Some(client) => self.reconcile(client).await,
                        None => Err(ServiceError::ActorStopped),
                    }
                } else {
                    Err(ServiceError::ShuttingDown)
                };
                let _ = reply.send(result);
            }
            Command::ReconcileDurable { reply } => {
                let result = if intake {
                    match client {
                        Some(client) => {
                            self.reconcile_durable(
                                client,
                                ReconciliationMarkerPolicy::PreservePlannerOwned,
                            )
                            .await
                        }
                        None => Err(ServiceError::ActorStopped),
                    }
                } else {
                    Err(ServiceError::ShuttingDown)
                };
                let _ = reply.send(result);
            }
            Command::ReconcileRecoveryOnlyCycle { reply } => {
                let result = if intake {
                    match client {
                        Some(client) => {
                            self.reconcile_durable(
                                client,
                                ReconciliationMarkerPolicy::RedriveWithoutPlanner,
                            )
                            .await
                        }
                        None => Err(ServiceError::ActorStopped),
                    }
                } else {
                    Err(ServiceError::ShuttingDown)
                };
                let _ = reply.send(result);
            }
            Command::AbandonParkedEvacuationHandoff { reply } => {
                // A scheduler cycle ended before CommitTick. Never clear durable evidence here:
                // discard every in-memory snapshot from that cycle, so the next healthy
                // ReconcileDecide scans and recaptures the durable markers instead of treating
                // an old parked snapshot as authority to clear one.
                self.parked_evacuation_markers.clear();
                self.parked_evacuation_handoff = None;
                let _ = reply.send(Ok(()));
            }
            #[cfg(test)]
            Command::ParkedEvacuationHandoffStateForTest { reply } => {
                let _ = reply.send(Ok((
                    self.parked_evacuation_markers.len(),
                    self.parked_evacuation_handoff.is_some(),
                )));
            }
            Command::RecoverDriverOwnership { reply } => {
                let result = if intake {
                    match client {
                        Some(client) => self
                            .reconcile_durable(
                                client,
                                ReconciliationMarkerPolicy::PreservePlannerOwned,
                            )
                            .await
                            .map(|_| self.ownership_recovery_generation),
                        None => Err(ServiceError::ActorStopped),
                    }
                } else {
                    Err(ServiceError::ShuttingDown)
                };
                let _ = reply.send(result);
            }
            Command::FinishDriverOwnershipRecovery { generation, reply } => {
                let complete = self.ownership_recovery_active
                    && generation == self.ownership_recovery_generation;
                if complete {
                    self.ownership_recovery_active = false;
                }
                let _ = reply.send(Ok(complete));
            }
            Command::DecideTickRound { facts, reply } => {
                if intake {
                    // Do the ms-scale bookkeeping on the actor turn, then price routes OFF the
                    // actor loop and reply from the spawned task, so the actor keeps serving
                    // admissions, driver transitions, waiter deadlines, and shutdown while the tick
                    // plans (ADR-0024). The scheduler still awaits this reply before it commits.
                    match self.prepare_tick_round(facts) {
                        Ok((
                            journal,
                            runtime,
                            facts,
                            policy,
                            planned_generation,
                            planned_world_generation,
                            preferred_replacement_parent,
                        )) => {
                            tokio::spawn(async move {
                                let result = plan_tick_off_actor(
                                    journal,
                                    runtime,
                                    facts,
                                    policy,
                                    planned_generation,
                                    planned_world_generation,
                                    preferred_replacement_parent,
                                )
                                .await;
                                let _ = reply.send(result);
                            });
                        }
                        Err(error) => {
                            let _ = reply.send(Err(error));
                        }
                    }
                } else {
                    let _ = reply.send(Err(ServiceError::ShuttingDown));
                }
            }
            Command::CommitTick {
                round,
                balances,
                balance_facts,
                tick_key,
                reply,
            } => {
                let result = if intake {
                    match client {
                        Some(client) => {
                            self.commit_tick(round, balances, balance_facts, tick_key, client)
                                .await
                        }
                        None => Err(ServiceError::ActorStopped),
                    }
                } else {
                    Err(ServiceError::ShuttingDown)
                };
                let _ = reply.send(result);
            }
            #[cfg(test)]
            Command::FailAfterFreshAdmissionForTest { key, reply } => {
                self.fail_after_fresh_admission = Some(key);
                let _ = reply.send(Ok(()));
            }
            Command::BeginExternalTerminalMutation { action, reply } => {
                let result = self.begin_external_terminal_mutation(&action);
                let _ = reply.send(result);
            }
            Command::EndExternalTerminalMutation { lease, reply } => {
                let result = self.end_external_terminal_mutation(lease);
                let _ = reply.send(result);
            }
            Command::BeginMembershipMutation { reply } => {
                let result = self.begin_membership_mutation();
                let _ = reply.send(result);
            }
            Command::EndMembershipMutation { lease, reply } => {
                let result = self.end_membership_mutation(lease);
                let _ = reply.send(result);
            }
            Command::Shutdown { reply } => {
                let _ = reply.send(Err(ServiceError::ShuttingDown));
            }
            Command::IssueTickPlanToken { reply } => {
                let _ = reply.send(self.issue_tick_plan_token().await);
            }
            Command::IssueBalanceFactsToken { reply } => {
                let _ = reply.send(self.issue_balance_facts_token());
            }
            Command::IssueProbePolicySnapshot { reply } => {
                let _ = reply.send(Ok(ProbePolicySnapshot {
                    authority: self.probe_policy_authority.clone(),
                    version: self.probe_policy_version.clone(),
                    policy: Arc::new(self.policy.clone()),
                }));
            }
            Command::GetPolicy { reply } => {
                let _ = reply.send(Ok(self.policy.clone()));
            }
            Command::PutPolicy { policy, reply } => {
                let result = if !intake {
                    Err(ServiceError::ShuttingDown)
                } else {
                    match policy.validate() {
                        Err(error) => Err(refused(
                            RefuseReason::PolicyInvalid,
                            format!("invalid policy field {}: {error}", error.offending_field()),
                        )),
                        Ok(()) => match self.journal.put_policy(&policy).await {
                            Err(error) => Err(storage(error)),
                            Ok(()) => {
                                if let Some(runtime) = &self.runtime {
                                    self.executor = Arc::new(
                                        runtime.service_executor(Some(policy.per_fed_cap)),
                                    );
                                }
                                self.policy = policy.clone();
                                self.policy_generation = self.policy_generation.wrapping_add(1);
                                self.probe_policy_version = Arc::new(());
                                // A policy edit is itself new scheduler information.  Do not let
                                // a clear from the prior policy generation suppress the marker
                                // produced by a driver which was already executing across it.
                                self.marker_clear_wake_suppression.clear();
                                self.policy_wake.send_modify(|generation| {
                                    *generation = generation.wrapping_add(1);
                                });
                                Ok(policy)
                            }
                        },
                    }
                };
                let _ = reply.send(result);
            }
        }
    }

    /// Forget the parked snapshot of exactly this parent without touching durable state.
    ///
    /// Only a commit outcome that deliberately KEEPS the durable marker for a later cycle needs
    /// this. Outcomes that clear the marker leave a changed row behind, so the drain's
    /// full-parent CAS already misses their stale snapshots.
    fn consume_parked_evacuation_marker(&mut self, parent: &Intent) {
        self.parked_evacuation_markers
            .retain(|parked| parked != parent);
        if self.parked_evacuation_handoff.as_ref() == Some(parent) {
            self.parked_evacuation_handoff = None;
        }
    }

    fn suppress_next_marker_wake(&mut self, parent: &Intent) {
        self.marker_clear_wake_suppression
            .insert((parent.idempotency_key.clone(), parent.attempt));
    }

    /// Clear an exact planner marker and suppress its next renewed wake only when a retryable write
    /// error leaves the commit boundary ambiguous. An exact reread only decides whether a parked
    /// snapshot is stale and must not be requeued. A permanent closure/validation error cannot have
    /// committed, so it must not suppress a later qualifying wake.
    async fn clear_marker_with_confirmation(
        &mut self,
        parent: &Intent,
    ) -> Result<bool, (ServiceError, bool)> {
        let mut expected_cleared = parent.clone();
        expected_cleared.evacuation_refusal = None;
        match self
            .journal
            .clear_marked_evacuation_if_pending(parent)
            .await
        {
            Ok(cleared) => {
                if cleared {
                    self.suppress_next_marker_wake(parent);
                } else if matches!(
                    self.journal.get(&parent.idempotency_key).await,
                    Ok(Some(current)) if current == expected_cleared
                ) {
                    // A prior ambiguous clear can leave this call observing a CAS miss. It is
                    // still this actor's exact markerless parent, so its next renewed marker is
                    // not new scheduler information.
                    self.suppress_next_marker_wake(parent);
                }
                Ok(cleared)
            }
            Err(error) => {
                // Only a retryable write error may have crossed an ambiguous commit boundary. A
                // permanent closure/validation error is definitely uncommitted, so suppression
                // there would incorrectly swallow new scheduler information after repair.
                if matches!(error, ExecError::Retryable(_)) {
                    self.suppress_next_marker_wake(parent);
                }
                let confirmed = matches!(
                    self.journal.get(&parent.idempotency_key).await,
                    Ok(Some(current)) if current == expected_cleared
                );
                Err((storage(error), confirmed))
            }
        }
    }

    /// Release only the exact row offered to the prior scheduler cycle. Unselected parked rows
    /// remain queued for later one-child rounds. Do not call this from `ReconcileDurable`:
    /// ownership recovery is allowed during an off-actor replacement plan.
    async fn release_parked_evacuation_markers_at_reconcile(&mut self) -> ServiceResult<()> {
        let Some(parent) = self.parked_evacuation_handoff.take() else {
            return Ok(());
        };
        self.parked_evacuation_markers
            .retain(|parked| parked != &parent);
        match self.clear_marker_with_confirmation(&parent).await {
            Ok(true) => {}
            Ok(false) => {}
            Err((error, confirmed)) => {
                // An unreadable or mismatched reread says nothing about whether this full-parent
                // CAS committed. Keep the exact snapshot, but rotate it behind independent queued
                // parents so one corrupt row cannot permanently starve their replacement rounds.
                // A byte-exact confirmed clear has already registered suppression and must not
                // requeue its stale snapshot.
                if !confirmed {
                    self.parked_evacuation_markers.push(parent);
                }
                return Err(error);
            }
        }
        Ok(())
    }

    /// The ms-scale, actor-serialized half of a tick decision: record the sensed balances and
    /// capture the planning inputs (journal, runtime, tick policy, and the generation the plan will
    /// be tagged with) atomically under the actor turn. The network-heavy planning itself runs OFF
    /// the actor loop — see [`plan_tick_off_actor`] — so route pricing (gateway + federation RPCs,
    /// up to the route-work budget) cannot stall admissions, driver transitions, waiter deadlines,
    /// or shutdown (ADR-0024). Capturing `policy_generation` HERE, before planning, is what keeps
    /// the commit-time drift guard intact: a `PutPolicy` that lands during the off-actor plan bumps
    /// the live generation, so the commit check refuses the now-stale batch — exactly as before,
    /// when planning ran on the actor turn and no `PutPolicy` could interleave at all.
    fn prepare_tick_round(&mut self, facts: ProbeFacts) -> ServiceResult<PreparedTickRound> {
        self.validate_tick_plan_token(&facts.admission_snapshot)?;
        let mut policy = TickPolicy::from(&self.policy);
        policy.occurrence = facts.occurrence;
        policy.now = facts.now_ms;
        // The scheduler's cycle carries the blocker set its reconcile derived; the tick policy is
        // where planning and route pricing both read it (br-p93).
        // The capability carries the actor's own pending projection.  Callers'
        // advisory facts cannot omit that baseline.
        policy.blocked = facts.admission_snapshot.blocked.clone();
        policy.blocked.extend(&facts.blocked);
        Ok((
            Arc::clone(&self.journal),
            self.runtime.clone(),
            facts,
            policy,
            self.policy_generation,
            self.world_generation,
            self.parked_evacuation_handoff.clone(),
        ))
    }

    async fn issue_tick_plan_token(&self) -> ServiceResult<super::GoalAdmissionSnapshot> {
        if let Some(diagnostic) = &self.goal_admissions_poisoned {
            return Err(ServiceError::Storage(format!(
                "tick admission authority is poisoned; refusing plans until restart: {diagnostic}"
            )));
        }
        if self.world_generation_poisoned
            || self.external_terminal_poisoned
            || self.membership_poisoned
        {
            return Err(ServiceError::Storage(
                "tick authority generation is poisoned; refusing plans until restart".to_owned(),
            ));
        }
        if self.external_terminal_live || self.membership_live {
            return Err(refused(
                RefuseReason::Conflict,
                "external terminal or membership mutation lease is in flight; refusing tick authority"
                    .to_owned(),
            ));
        }
        let pending = self.journal.pending().await.map_err(storage)?;
        let blocked = GoalBlockers::from_intents(&pending);
        Ok(super::GoalAdmissionSnapshot {
            authority: Arc::clone(&self.tick_token_authority),
            counters: self.goal_admissions.clone(),
            blocked,
            world_generation: self.world_generation,
            membership_epoch: self.membership_epoch,
        })
    }

    fn validate_tick_plan_token(
        &self,
        snapshot: &super::GoalAdmissionSnapshot,
    ) -> ServiceResult<()> {
        if let Some(diagnostic) = &self.goal_admissions_poisoned {
            return Err(ServiceError::Storage(format!(
                "tick admission authority is poisoned; refusing commit: {diagnostic}"
            )));
        }
        if self.world_generation_poisoned {
            return Err(ServiceError::Storage(
                "membership world generation overflowed; refusing commit".to_owned(),
            ));
        }
        if self.external_terminal_poisoned
            || self.external_terminal_live
            || self.membership_poisoned
            || self.membership_live
            || snapshot.membership_epoch != self.membership_epoch
        {
            return Err(ServiceError::Storage(
                "external terminal or membership mutation lease is in flight or superseded; refusing commit"
                    .to_owned(),
            ));
        }
        if !snapshot.is_issued_by(&self.tick_token_authority) {
            return Err(ServiceError::Storage(
                "tick: missing or foreign actor-issued admission token".to_owned(),
            ));
        }
        if snapshot.world_generation != self.world_generation {
            return Err(refused(
                RefuseReason::Conflict,
                format!(
                    "tick token was issued for membership world generation {}, current is {}",
                    snapshot.world_generation, self.world_generation
                ),
            ));
        }
        Ok(())
    }

    /// Record this immediately after `decide_and_journal` has made a fresh
    /// Agent allocator intent durable.  It intentionally precedes hold,
    /// preemption and driver spawning: any of those later operations may fail
    /// or terminalize, but an older off-actor plan must still see the
    /// intervening admission.
    fn record_goal_admission(&mut self, decision: &AllocatorDecision, actor: Actor) {
        let Some(goal) = AllocatorGoal::of_decision(decision, actor) else {
            return;
        };
        let counter = self.goal_admissions.entry(goal).or_insert(0);
        match counter.checked_add(1) {
            Some(next) => *counter = next,
            None => {
                self.goal_admissions_poisoned = Some(
                    "tick admission watermark overflowed; refusing work until restart".to_owned(),
                );
                tracing::error!(
                    ?goal,
                    "agent admission watermark overflowed; future tick plans fail closed"
                );
            }
        }
    }

    fn bump_world_generation(&mut self) {
        match self.world_generation.checked_add(1) {
            Some(next) => self.world_generation = next,
            None => {
                self.world_generation_poisoned = true;
                tracing::error!(
                    "membership world generation overflowed; future tick work fails closed"
                );
            }
        }
    }

    fn record_membership_admission(&mut self, action: &Action) {
        if matches!(action, Action::Join { .. } | Action::Recover { .. }) {
            self.bump_world_generation();
        }
    }

    fn begin_external_terminal_mutation(
        &mut self,
        action: &Action,
    ) -> ServiceResult<super::ExternalTerminalMutationLease> {
        let balance_federations = balance_federations(action);
        if balance_federations.is_empty() {
            return Err(ServiceError::Storage(
                "external terminal mutation requires a balance-affecting action".to_owned(),
            ));
        }
        if self.external_terminal_poisoned || self.external_terminal_live {
            return Err(refused(
                RefuseReason::Conflict,
                "external terminal mutation lease is already in flight".to_owned(),
            ));
        }
        self.external_terminal_live = true;
        Ok(super::ExternalTerminalMutationLease {
            authority: Arc::clone(&self.external_terminal_authority),
            epoch: self.external_terminal_epoch,
            balance_federations,
        })
    }

    fn end_external_terminal_mutation(
        &mut self,
        lease: super::ExternalTerminalMutationLease,
    ) -> ServiceResult<()> {
        if !Arc::ptr_eq(&lease.authority, &self.external_terminal_authority)
            || !self.external_terminal_live
            || lease.epoch != self.external_terminal_epoch
        {
            return Err(ServiceError::Storage(
                "external terminal mutation lease is missing or stale; balance facts remain fenced while a valid lease may be in flight"
                    .to_owned(),
            ));
        }
        let Some(next_epoch) = self.external_terminal_epoch.checked_add(1) else {
            self.external_terminal_poisoned = true;
            return Err(ServiceError::Storage(
                "external terminal mutation epoch overflowed; refusing until restart".to_owned(),
            ));
        };
        for federation in lease.balance_federations {
            self.bump_balance_generation(federation);
        }
        if self.balance_generations_poisoned {
            return Err(ServiceError::Storage(
                "balance-facts generation overflowed; refusing until restart".to_owned(),
            ));
        }
        self.external_terminal_epoch = next_epoch;
        self.external_terminal_live = false;
        Ok(())
    }

    fn begin_membership_mutation(&mut self) -> ServiceResult<super::MembershipMutationLease> {
        if self.membership_poisoned || self.membership_live {
            return Err(refused(
                RefuseReason::Conflict,
                "membership mutation lease is already in flight; refusing tick authority"
                    .to_owned(),
            ));
        }
        self.membership_live = true;
        Ok(super::MembershipMutationLease {
            authority: Arc::clone(&self.membership_authority),
            epoch: self.membership_epoch,
        })
    }

    fn end_membership_mutation(
        &mut self,
        lease: super::MembershipMutationLease,
    ) -> ServiceResult<()> {
        if !Arc::ptr_eq(&lease.authority, &self.membership_authority)
            || !self.membership_live
            || lease.epoch != self.membership_epoch
        {
            self.membership_poisoned = true;
            return Err(ServiceError::Storage(
                "membership mutation lease is missing or stale; refusing ticks until restart"
                    .to_owned(),
            ));
        }
        let Some(next) = self.membership_epoch.checked_add(1) else {
            self.membership_poisoned = true;
            return Err(ServiceError::Storage(
                "membership mutation epoch overflowed; refusing ticks until restart".to_owned(),
            ));
        };
        // The authority remains live until both epoch and the allocator's membership world have
        // advanced, so no old token can be relabelled between those two steps.
        self.bump_world_generation();
        self.membership_epoch = next;
        self.membership_live = false;
        Ok(())
    }

    fn admission_snapshot_conflicts(
        &self,
        snapshot: &super::GoalAdmissionSnapshot,
        decision: &AllocatorDecision,
        actor: Actor,
    ) -> bool {
        self.goal_admissions.iter().any(|(goal, current)| {
            *current > snapshot.counters.get(goal).copied().unwrap_or_default()
                && goal.conflicts_with_decision(decision, actor)
        })
    }

    fn issue_balance_facts_token(&self) -> ServiceResult<super::BalanceFactsToken> {
        if self.external_terminal_poisoned || self.external_terminal_live {
            return Err(refused(
                RefuseReason::Conflict,
                "external terminal mutation lease is in flight; refusing balance facts".to_owned(),
            ));
        }
        if let Some(reason) = &self.balance_facts_poisoned {
            return Err(ServiceError::Storage(format!(
                "tick balance facts are poisoned; refusing token: {reason}"
            )));
        }
        if self.balance_generations_poisoned {
            return Err(ServiceError::Storage(
                "tick balance-facts generation overflowed; refusing samples".to_owned(),
            ));
        }
        Ok(super::BalanceFactsToken {
            authority: Arc::clone(&self.balance_facts_authority),
            generations: self.balance_generations.clone(),
        })
    }

    fn validate_balance_facts(&self, facts: &super::BalanceFactsToken) -> ServiceResult<()> {
        if let Some(reason) = &self.balance_facts_poisoned {
            return Err(ServiceError::Storage(format!(
                "tick balance facts are poisoned; refusing commit: {reason}"
            )));
        }
        if self.balance_generations_poisoned {
            return Err(ServiceError::Storage(
                "tick balance-facts generation overflowed; refusing commit".to_owned(),
            ));
        }
        if !facts.is_issued_by(&self.balance_facts_authority) {
            return Err(ServiceError::Storage(
                "tick: missing or foreign actor-issued balance-facts token".to_owned(),
            ));
        }
        if self.external_terminal_poisoned || self.external_terminal_live {
            return Err(ServiceError::Storage(
                "external terminal mutation lease is in flight or superseded; refusing commit"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn poison_balance_facts(&mut self, reason: String) {
        tracing::error!(%reason, "balance-facts authority poisoned after intent transition");
        self.balance_facts_poisoned.get_or_insert(reason);
    }

    /// Capture the action before a one-shot artifact write. An absent/stale attempt is the fenced
    /// writer's ordinary false result; a read error aborts before any mutation.
    async fn artifact_write_action(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
    ) -> ServiceResult<Option<Action>> {
        self.journal.get(key).await.map_err(storage).map(|intent| {
            intent
                .filter(|intent| {
                    intent.idempotency_key == *key && intent.attempt == expected_attempt
                })
                .map(|intent| intent.action)
        })
    }

    /// Return the pre-write action for every durable transition which can alter a reservation or
    /// balance fact.  Requiring this lookup before the write is intentional: if the row is absent
    /// or unreadable we must not make a mutation whose affected federations are unknown.
    async fn transition_balance_action(
        &self,
        key: &IdempotencyKey,
        transition: &JournalTransition,
    ) -> ServiceResult<Option<Action>> {
        match transition {
            JournalTransition::Upsert { intent, .. } => Ok(Some(intent.action.clone())),
            // An absent CAS is a normal false comparison, not an unsafe mutation.  Preserve
            // that no-op contract rather than rejecting it during the pre-write lookup.
            JournalTransition::CompareAndSet { .. } => self
                .journal
                .get(key)
                .await
                .map_err(storage)
                .map(|intent| intent.map(|intent| intent.action)),
            JournalTransition::SetStatus { .. }
            | JournalTransition::SetRawTerminal { .. }
            | JournalTransition::ResetRetryable { .. } => self
                .journal
                .get(key)
                .await
                .map_err(storage)?
                .map(|intent| intent.action)
                .ok_or_else(|| {
                    ServiceError::Storage(format!(
                        "intent {} disappeared before its durable transition",
                        key.0
                    ))
                })
                .map(Some),
            JournalTransition::DriverFinished { .. } | JournalTransition::Refresh => Ok(None),
        }
    }

    fn record_balance_change(&mut self, action: &Action) {
        for federation in balance_federations(action) {
            self.bump_balance_generation(federation);
        }
    }

    fn bump_balance_generation(&mut self, federation: FederationId) {
        let generation = self.balance_generations.entry(federation).or_insert(0);
        match generation.checked_add(1) {
            Some(next) => *generation = next,
            None => self.balance_generations_poisoned = true,
        }
    }

    fn balance_facts_changed_for(
        generations_at_commit: &BTreeMap<FederationId, u64>,
        facts: &super::BalanceFactsToken,
        action: &Action,
    ) -> bool {
        balance_federations(action).into_iter().any(|federation| {
            generations_at_commit
                .get(&federation)
                .copied()
                .unwrap_or_default()
                != facts
                    .generations
                    .get(&federation)
                    .copied()
                    .unwrap_or_default()
        })
    }

    async fn commit_tick(
        &mut self,
        round: super::TickRound,
        balances: BTreeMap<FederationId, Msat>,
        balance_facts: super::BalanceFactsToken,
        existing_tick_key: Option<IdempotencyKey>,
        client: &WalletClient,
    ) -> ServiceResult<CommitTickReport> {
        let super::TickRound {
            decisions,
            replacement_deferred,
            occurrence,
            planned_generation,
            planned_world_generation,
            admission_snapshot,
            replacement,
            marker_disposition,
            ..
        } = round;
        // A forged round must not pair two independently authoritative marker outcomes. Reject it
        // before any authority check or durable marker action; each parent remains repair evidence
        // for a later, valid round.
        if replacement.is_some() && marker_disposition.is_some() {
            self.abandon_tick_marker_snapshots(replacement.as_ref(), marker_disposition.as_ref());
            let error = ServiceError::Storage(
                "CommitTick: replacement and marker-clear disposition cannot share a round"
                    .to_owned(),
            );
            if let Some(key) = existing_tick_key.as_ref() {
                self.record_tick_failed(key, &error.to_string()).await;
            }
            return Err(error);
        }
        // A replacement shadow intentionally has no ordinary `decisions`:
        // its one admissible action is carried separately.  Every early
        // authority refusal must nevertheless audit that fresh child, not an
        // empty shadow shell (and never its retired parent).
        let audit_decisions = replacement
            .as_ref()
            .map(|replacement| vec![replacement.fresh.clone()])
            .unwrap_or_else(|| decisions.clone());
        // Policy-generation guard (§6a P1): a PutPolicy may have landed while the daemon
        // was validating routes over the network between DecideTickRound and here. These
        // decisions were sized against caps/targets the operator has since changed, so we
        // refuse the whole batch — journaling nothing — and let the next cycle replan
        // under the current policy. No money op is admitted on stale sizing.
        if planned_generation != self.policy_generation {
            let message = format!(
                "tick planned under policy generation {planned_generation}, current is {}",
                self.policy_generation
            );
            let refused = audit_decisions
                .iter()
                .map(|decision| TickRefusal {
                    key: decision.idempotency_key.clone(),
                    reason: RefuseReason::PolicySuperseded,
                    message: message.clone(),
                })
                .collect();
            self.abandon_tick_marker_snapshots(replacement.as_ref(), marker_disposition.as_ref());
            // The daemon opens its scheduler tick row before awaiting route pricing.  A policy
            // edit may therefore supersede this plan after that row is Started; terminalize that
            // already-open row instead of returning early and leaking Started forever.
            if let Some(key) = existing_tick_key.as_ref() {
                self.finish_tick_batch(TickBatch {
                    key: key.clone(),
                    decisions: audit_decisions.len() as u32,
                    pending: BTreeSet::new(),
                    performed: 0,
                    failed: audit_decisions.len() as u32,
                    error: Some(message),
                })
                .await;
            }
            return Ok(CommitTickReport {
                accepted: Vec::new(),
                refused,
            });
        }
        if self.world_generation_poisoned || planned_world_generation != self.world_generation {
            let message = format!(
                "tick planned under membership world generation {planned_world_generation}, current is {}",
                self.world_generation
            );
            let refused = audit_decisions
                .iter()
                .map(|decision| TickRefusal {
                    key: decision.idempotency_key.clone(),
                    reason: RefuseReason::Conflict,
                    message: message.clone(),
                })
                .collect();
            self.abandon_tick_marker_snapshots(replacement.as_ref(), marker_disposition.as_ref());
            if let Some(key) = existing_tick_key.as_ref() {
                self.finish_tick_batch(TickBatch {
                    key: key.clone(),
                    decisions: audit_decisions.len() as u32,
                    pending: BTreeSet::new(),
                    performed: 0,
                    failed: audit_decisions.len() as u32,
                    error: Some(message),
                })
                .await;
            }
            return Ok(CommitTickReport {
                accepted: Vec::new(),
                refused,
            });
        }
        if let Err(error) = self.validate_tick_plan_token(&admission_snapshot) {
            self.abandon_tick_marker_snapshots(replacement.as_ref(), marker_disposition.as_ref());
            if let Some(key) = existing_tick_key.as_ref() {
                self.record_tick_failed(key, &error.to_string()).await;
            }
            return Err(error);
        }
        if let Err(error) = self.validate_balance_facts(&balance_facts) {
            self.abandon_tick_marker_snapshots(replacement.as_ref(), marker_disposition.as_ref());
            if let Some(key) = existing_tick_key.as_ref() {
                self.record_tick_failed(key, &error.to_string()).await;
            }
            return Err(error);
        }
        // Freeze the comparison before any decision in this batch can itself
        // advance a generation. Shared-destination independent work must not
        // invalidate a later sibling merely because the first became durable.
        let balance_generations_at_commit = self.balance_generations.clone();
        if audit_decisions
            .iter()
            .any(|decision| decision.occurrence != occurrence)
        {
            let error =
                ServiceError::Storage("CommitTick: decisions span multiple occurrences".to_owned());
            self.abandon_tick_marker_snapshots(replacement.as_ref(), marker_disposition.as_ref());
            if let Some(key) = existing_tick_key.as_ref() {
                self.record_tick_failed(key, &error.to_string()).await;
            }
            return Err(error);
        }
        let now = now_ms();
        let tick_key = existing_tick_key
            .unwrap_or_else(|| IdempotencyKey(format!("tick:{}:{}", occurrence.0, ledger_nonce())));
        // The tick row is auxiliary bookkeeping, not admission for the money operations.
        // Preserve the standalone tick invariant: a fault here must not suppress evacuations.
        if let Err(error) = self
            .journal
            .record_tick_started(&tick_key, occurrence, now)
            .await
        {
            tracing::warn!(?error, "CommitTick: recording the Started tick row failed");
        }
        // A replacement may consume exactly one child. Keep the otherwise-planned decisions as
        // durable audit facts, but never pass them to admission/reservation folding or report
        // them as accepted work.
        for deferred in replacement_deferred
            .iter()
            .filter(|decision| decision.action.is_executable())
        {
            if let Err(error) = self
                .journal
                .record_tick_dropped_refusal(
                    deferred,
                    occurrence,
                    now,
                    "deferred: replacement-exclusive one-child round",
                    false,
                )
                .await
            {
                tracing::warn!(
                    key = %deferred.idempotency_key.0,
                    ?error,
                    "CommitTick: recording replacement-exclusive deferred audit failed"
                );
            }
        }
        if let Err(error) = self
            .journal
            .record_refusals_with_note(
                &replacement_deferred,
                occurrence,
                now,
                Some("deferred: replacement-exclusive one-child round"),
            )
            .await
        {
            tracing::warn!(
                ?error,
                "CommitTick: recording replacement-exclusive deferred advisory audit failed"
            );
        }
        // br-p93, the final conflict check: re-derive the in-flight goals HERE rather than trust
        // the set the caller planned against, so a batch that bypassed planning — or one whose
        // plan raced work admitted since — still cannot re-issue an in-flight goal under a fresh
        // occurrence. Fail-closed: an unreadable scan means the eligibility is unknown, which is a
        // no-commit condition for the whole batch, exactly like a failed reconcile.
        //
        // The scan is taken once, but each decision this loop ADMITS is folded back in below, so
        // the guard covers a batch carrying the same goal twice as well as one duplicating durable
        // work. `decide_with_blockers` cannot emit that pair (one funding goal per designated
        // destination, one evacuation per source, and `push_decision` dedups keys), but "a caller
        // bypassed planning" is precisely the threat model this check exists for, and against that
        // caller the pre-loop scan alone would admit both.
        let mut blocked = match self.journal.pending().await {
            Ok(pending) => GoalBlockers::from_intents(&pending),
            Err(error) => {
                let error = storage(error);
                self.abandon_tick_marker_snapshots(
                    replacement.as_ref(),
                    marker_disposition.as_ref(),
                );
                self.record_tick_failed(&tick_key, &error.to_string()).await;
                return Err(error);
            }
        };
        let mut report = CommitTickReport::default();
        let mut first_error = None;
        let mut failed = 0_u32;
        // A commit-time target is reservation-aware, not merely balance-aware:
        // pending user receives/direct inflows and already admitted earlier
        // batch legs have promised inbound value even when the fresh probe has
        // not observed it yet.
        let mut commit_reservations = match project_allocator_reservations(&self.journal).await {
            Ok(reservations) => reservations,
            Err(error) => {
                // The allocator reservation view still starts from a complete, fail-closed intent
                // scan: if one intent row is unreadable, no executable decision may be admitted.
                // Keep this a soft, per-decision tick refusal (the existing
                // CommitTick API contract) while refusing the complete batch.
                let message = storage(error).to_string();
                self.abandon_tick_marker_snapshots(
                    replacement.as_ref(),
                    marker_disposition.as_ref(),
                );
                report.refused.extend(
                    decisions_to_apply(&audit_decisions)
                        .into_iter()
                        .filter(|decision| decision.action.is_executable())
                        .map(|decision| TickRefusal {
                            key: decision.idempotency_key,
                            reason: RefuseReason::StorageError,
                            message: message.clone(),
                        }),
                );
                if let Err(error) = self
                    .journal
                    .record_refusals(&audit_decisions, occurrence, now)
                    .await
                {
                    tracing::warn!(
                        ?error,
                        "CommitTick: recording advisory refusal rows after reservation fault failed"
                    );
                }
                self.finish_tick_batch(TickBatch {
                    key: tick_key,
                    decisions: audit_decisions.len() as u32,
                    pending: BTreeSet::new(),
                    performed: 0,
                    failed: report.refused.len() as u32,
                    error: Some(message),
                })
                .await;
                return Ok(report);
            }
        };
        // A no-child disposition is authoritative only after every pre-admission authority and
        // fail-closed journal projection above has succeeded. It alone may clear its exact marker.
        if let Some(disposition) = marker_disposition.as_ref() {
            match self
                .clear_marker_with_confirmation(&disposition.parent)
                .await
            {
                Ok(true) => {
                    // Deliberately no policy wake and no driver admission. The next regular
                    // scheduler cycle sees an ordinary unmarked Pending evacuation.
                }
                Ok(false) => tracing::warn!(
                    key = %disposition.parent.idempotency_key.0,
                    "CommitTick: marker-clear disposition no longer owned its exact Pending evacuation"
                ),
                Err((error, _)) => {
                    // `plan_tick_round_for_marker` replans this fallback's ordinary decisions with
                    // the original live blockers and reservation projection still present. A failed
                    // best-effort disposition clear therefore cannot make those independently sized
                    // decisions unsafe; retain the exact parent for the next reconcile and commit
                    // them. Keep the no-work path loud so a scheduler-only disposition fault is not
                    // silently reported as a successful tick.
                    if decisions.is_empty() {
                        self.record_tick_failed(&tick_key, &error.to_string()).await;
                        return Err(error);
                    }
                    tracing::warn!(
                        key = %disposition.parent.idempotency_key.0,
                        ?error,
                        "CommitTick: retaining marker after disposition-clear fault; committing independently replanned ordinary decisions"
                    );
                }
            }
        }
        // Replacement is deliberately a one-child batch.  Nothing below may
        // admit an ordinary sibling against the old parent's hypothetical
        // released capacity.
        if let Some(replacement) = replacement {
            // This is an operator-correctable pre-exchange refusal. Classify it from the typed
            // parent captured by planning rather than from an error string returned below: a later
            // daemon cycle must retain the structural marker and choose an occurrence strictly
            // beyond the old parent.
            if let Actor::Agent {
                occurrence: old_occurrence,
            } = replacement.parent.actor
            {
                if replacement.fresh.occurrence <= old_occurrence
                    || replacement.fresh.idempotency_key == replacement.old_key
                {
                    // Retaining the durable marker only reaches that later cycle if this refusal
                    // also consumes the snapshot the parking reconciliation took of this exact
                    // parent: the next `ReconcileDecide` drains that snapshot first, and its
                    // full-parent CAS would match the untouched row and release the evidence.
                    // Dropping it here writes nothing durable, so the same reconciliation
                    // recaptures the marker for another replacement attempt.
                    self.consume_parked_evacuation_marker(&replacement.parent);
                    let message = super::replacement_occurrence_error(
                        old_occurrence,
                        replacement.fresh.occurrence,
                    );
                    let report = CommitTickReport {
                        accepted: Vec::new(),
                        refused: vec![TickRefusal {
                            key: replacement.fresh.idempotency_key.clone(),
                            reason: RefuseReason::Conflict,
                            message: message.clone(),
                        }],
                    };
                    self.finish_tick_batch(TickBatch {
                        key: tick_key,
                        decisions: 1,
                        pending: BTreeSet::new(),
                        performed: 0,
                        failed: 1,
                        error: Some(message),
                    })
                    .await;
                    return Ok(report);
                }
            }
            if self.admission_snapshot_conflicts(
                &admission_snapshot,
                &replacement.fresh,
                Actor::Agent { occurrence },
            ) {
                let error = refused(
                    RefuseReason::Conflict,
                    "replacement goal was admitted after its eligibility snapshot".to_owned(),
                );
                // The child was never exchanged. Keep the structural evidence and discard only
                // this exact parked offer so a later reconciliation can recapture it.
                self.consume_parked_evacuation_marker(&replacement.parent);
                self.record_tick_failed(&tick_key, &error.to_string()).await;
                return Err(error);
            }
            let child = match self
                .commit_evacuation_replacement(
                    &replacement,
                    occurrence,
                    &balances,
                    &balance_facts,
                    &balance_generations_at_commit,
                    &blocked,
                    now,
                    client,
                )
                .await
            {
                Ok(child) => child,
                Err(ReplacementCommitError {
                    error,
                    disposition: ReplacementFailureDisposition::DefiniteUncommitted,
                }) => {
                    // The exchange is definitely uncommitted, but that is not authority to erase
                    // the parent's structural repair evidence. Forget only the exact parked
                    // snapshot so the next reconciliation recaptures the unchanged marker.
                    self.consume_parked_evacuation_marker(&replacement.parent);
                    match error {
                        ServiceError::Refused { reason, message } => {
                            let report = CommitTickReport {
                                accepted: Vec::new(),
                                refused: vec![TickRefusal {
                                    key: replacement.fresh.idempotency_key,
                                    reason,
                                    message: message.clone(),
                                }],
                            };
                            self.finish_tick_batch(TickBatch {
                                key: tick_key,
                                decisions: 1,
                                pending: BTreeSet::new(),
                                performed: 0,
                                failed: 1,
                                error: Some(message),
                            })
                            .await;
                            return Ok(report);
                        }
                        error => {
                            self.record_tick_failed(&tick_key, &error.to_string()).await;
                            return Err(error);
                        }
                    }
                }
                Err(ReplacementCommitError {
                    error,
                    disposition: ReplacementFailureDisposition::PostExchangeAmbiguous,
                }) => {
                    // An exchange may have committed.  Its marker is durable
                    // recovery evidence and must remain until exact
                    // confirmation authorizes a cleanup. Forget the exact parked snapshot too:
                    // the following ReconcileDecide would otherwise drain-clear this deliberately
                    // retained full parent before it can be recaptured from durable state.
                    self.consume_parked_evacuation_marker(&replacement.parent);
                    self.record_tick_failed(&tick_key, &error.to_string()).await;
                    return Err(error);
                }
                Err(ReplacementCommitError {
                    error,
                    disposition: ReplacementFailureDisposition::DefiniteUncommittedRetainMarker,
                }) => {
                    // The journal proved this exchange did not commit, but its
                    // pre-commit validation found durable state that prevents
                    // this child. Retain the marker for a later child rather
                    // than treating that validation failure as authority to
                    // release the parent. Consume only the exact parked
                    // handoff: otherwise the next reconciliation would
                    // drain-clear this deliberately retained full parent before
                    // recapturing it.
                    self.consume_parked_evacuation_marker(&replacement.parent);
                    self.record_tick_failed(&tick_key, &error.to_string()).await;
                    return Err(error);
                }
            };
            let mut report = CommitTickReport::default();
            if let Some(child) = child {
                report.accepted.push(child);
            } else {
                // A confirmed-uncommitted CAS miss/no-child result retains the marker for a later
                // occurrence. It is not ambiguous, so do not poison admission authority.
                self.consume_parked_evacuation_marker(&replacement.parent);
                report.refused.push(TickRefusal {
                    key: replacement.fresh.idempotency_key,
                    reason: RefuseReason::Conflict,
                    message: "evacuation replacement parent was no longer exclusively pending"
                        .to_owned(),
                });
            }
            let batch = TickBatch {
                key: tick_key,
                decisions: 1,
                pending: report.accepted.iter().cloned().collect(),
                performed: 0,
                failed: report.refused.len() as u32,
                error: None,
            };
            if batch.pending.is_empty() {
                self.finish_tick_batch(batch).await;
            } else {
                self.tick_batches.push(batch);
            }
            return Ok(report);
        }
        for decision in decisions_to_apply(&decisions) {
            // Advisory allocator decisions are durable refusal facts, not executable work.
            // `record_refusals` below is their single ledger writer, matching the standalone
            // executor path's `apply_with_allocator_admission` projection.
            if !decision.action.is_executable() {
                continue;
            }
            if let Some(destination) = executable_destination(&decision.action) {
                if !balances.contains_key(&destination) {
                    let message = format!(
                        "decision {} has no fresh balance for destination {}",
                        decision.idempotency_key.0,
                        destination.to_hex()
                    );
                    report.refused.push(TickRefusal {
                        key: decision.idempotency_key.clone(),
                        reason: RefuseReason::Conflict,
                        message: message.clone(),
                    });
                    if let Err(error) = self
                        .journal
                        .record_tick_dropped_refusal(&decision, occurrence, now, &message, false)
                        .await
                    {
                        tracing::warn!(
                            key = %decision.idempotency_key.0,
                            ?error,
                            "CommitTick: recording missing-destination refusal failed"
                        );
                    }
                    continue;
                }
            }
            if Self::balance_facts_changed_for(
                &balance_generations_at_commit,
                &balance_facts,
                &decision.action,
            ) {
                let message = format!(
                    "decision {} touches balance facts changed after the fresh sample",
                    decision.idempotency_key.0
                );
                report.refused.push(TickRefusal {
                    key: decision.idempotency_key.clone(),
                    reason: RefuseReason::Conflict,
                    message: message.clone(),
                });
                if let Err(error) = self
                    .journal
                    .record_tick_dropped_refusal(&decision, occurrence, now, &message, false)
                    .await
                {
                    tracing::warn!(
                        key = %decision.idempotency_key.0,
                        ?error,
                        "CommitTick: recording stale-balance-facts refusal failed"
                    );
                }
                continue;
            }
            let decision_existed = match self.journal.get(&decision.idempotency_key).await {
                Ok(Some(intent))
                    if matches!(
                        intent.status,
                        IntentStatus::Done | IntentStatus::Awaiting | IntentStatus::Failed
                    ) =>
                {
                    let message = format!(
                        "tick decision {} already has terminal durable work",
                        decision.idempotency_key.0
                    );
                    if let Err(error) = self
                        .journal
                        .record_tick_dropped_refusal(&decision, occurrence, now, &message, false)
                        .await
                    {
                        tracing::warn!(key = %decision.idempotency_key.0, ?error, "CommitTick: recording terminal replay refusal failed");
                    }
                    report.refused.push(TickRefusal {
                        key: decision.idempotency_key,
                        reason: RefuseReason::Conflict,
                        message,
                    });
                    continue;
                }
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(error) => {
                    let error = storage(error);
                    self.record_tick_failed(&tick_key, &error.to_string()).await;
                    return Err(error);
                }
            };
            if blocked.blocks_decision(&decision, Actor::Agent { occurrence }) {
                let message = format!(
                    "decision {} conflicts with allocator work already in flight",
                    decision.idempotency_key.0
                );
                tracing::warn!(key = %decision.idempotency_key.0, "{message}");
                if let Err(error) = self
                    .journal
                    .record_tick_dropped_refusal(&decision, occurrence, now, &message, true)
                    .await
                {
                    tracing::warn!(
                        key = %decision.idempotency_key.0,
                        ?error,
                        "CommitTick: recording a conflict-suppressed decision failed"
                    );
                }
                report.refused.push(TickRefusal {
                    key: decision.idempotency_key,
                    reason: RefuseReason::Conflict,
                    message,
                });
                continue;
            }
            if self.admission_snapshot_conflicts(
                &admission_snapshot,
                &decision,
                Actor::Agent { occurrence },
            ) {
                let message = format!(
                    "decision {} conflicts with Agent work admitted after its eligibility snapshot",
                    decision.idempotency_key.0
                );
                tracing::warn!(key = %decision.idempotency_key.0, "{message}");
                if let Err(error) = self
                    .journal
                    .record_tick_dropped_refusal(&decision, occurrence, now, &message, true)
                    .await
                {
                    tracing::warn!(
                        key = %decision.idempotency_key.0,
                        ?error,
                        "CommitTick: recording a watermark-suppressed decision failed"
                    );
                }
                report.refused.push(TickRefusal {
                    key: decision.idempotency_key,
                    reason: RefuseReason::Conflict,
                    message,
                });
                continue;
            }
            if !decision_existed {
                if let Some((to, amount, target)) = self.funding_target_for(&decision) {
                    let Some(destination_balance) = balances.get(&to).copied() else {
                        let message = format!(
                            "decision {} has no fresh balance for destination {}",
                            decision.idempotency_key.0,
                            to.to_hex()
                        );
                        report.refused.push(TickRefusal {
                            key: decision.idempotency_key,
                            reason: RefuseReason::Conflict,
                            message,
                        });
                        continue;
                    };
                    let shortfall = wallet_core::funding_shortfall(
                        target,
                        destination_balance,
                        commit_reservations.target_credit(to),
                        0,
                    );
                    if amount.0 > shortfall {
                        let message = format!(
                            "decision {} exceeds destination {}'s fresh target shortfall; replan",
                            decision.idempotency_key.0,
                            to.to_hex()
                        );
                        tracing::warn!(key = %decision.idempotency_key.0, "{message}");
                        if let Err(error) = self
                            .journal
                            .record_tick_dropped_refusal(
                                &decision, occurrence, now, &message, false,
                            )
                            .await
                        {
                            tracing::warn!(
                                key = %decision.idempotency_key.0,
                                ?error,
                                "CommitTick: recording a fresh-target dropped decision failed"
                            );
                        }
                        report.refused.push(TickRefusal {
                            key: decision.idempotency_key,
                            reason: RefuseReason::Conflict,
                            message,
                        });
                        continue;
                    }
                }
            }
            let request = OpRequest {
                decision: decision.clone(),
                actor: Actor::Agent { occurrence },
                now_ms: now,
                balances: balances.clone(),
                probe_session_nonce: None,
                // The scheduler's own moves self-heal via the open_all retry; the dest-side 503
                // gate is for FRESH user admissions only, so this path never fails fast.
                dest_unavailable: None,
            };
            match self
                .decide_op_with_allocator_reservations(request, client, Some(&commit_reservations))
                .await
            {
                Ok(decided) => {
                    // Now durable, so it holds its goal against the REST of this batch (br-p93).
                    blocked.hold_decision(&decision, Actor::Agent { occurrence });
                    if !decision_existed {
                        reserve_action_for_commit(&mut commit_reservations, &decision.action);
                    }
                    report.accepted.push(decided.key);
                }
                Err(DecideOpError { error, disposition }) => {
                    if disposition == FreshMutationDisposition::UnknownIdentityPoison {
                        // A mismatched reread says the requested fresh identity cannot be safely
                        // attributed.  The helper has poisoned future authority; this in-flight
                        // batch must also stop before a later decision can bypass that fail-closed
                        // boundary (including one conflicting only with the stored row).
                        self.record_tick_failed(&tick_key, &error.to_string()).await;
                        return Err(error);
                    }

                    // Only a known/potential requested fresh mutation holds its logical goal and
                    // strict reservation for later siblings.  A re-read proving absence is a
                    // pre-upsert fault, so folding it would create a phantom same-goal/source
                    // refusal inside this batch.
                    let requested_mutation =
                        disposition == FreshMutationDisposition::RequestedMutationPossible;
                    match error {
                        ServiceError::Refused { reason, message } => {
                            fold_unknown_fresh_commit_mutation(
                                &mut blocked,
                                &mut commit_reservations,
                                &decision,
                                occurrence,
                                decision_existed,
                                requested_mutation,
                            );
                            if let Err(error) = self
                                .journal
                                .record_tick_dropped_refusal(
                                    &decision, occurrence, now, &message, false,
                                )
                                .await
                            {
                                tracing::warn!(
                                    key = %decision.idempotency_key.0,
                                    ?error,
                                    "CommitTick: recording a dropped-decision refusal failed"
                                );
                            }
                            report.refused.push(TickRefusal {
                                key: decision.idempotency_key,
                                reason,
                                message,
                            });
                        }
                        error => {
                            // Later fresh-path work (hold/preemption/driver setup) runs only
                            // after a successful journal admission, and is marked requested above.
                            // Definite pre-upsert faults still surface as the tick error but do
                            // not manufacture a batch-local goal or reservation holder.
                            fold_unknown_fresh_commit_mutation(
                                &mut blocked,
                                &mut commit_reservations,
                                &decision,
                                occurrence,
                                decision_existed,
                                requested_mutation,
                            );
                            failed = failed.saturating_add(1);
                            tracing::warn!(
                                key = %decision.idempotency_key.0,
                                ?error,
                                "CommitTick: decision failed; continuing batch"
                            );
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                        }
                    }
                }
            }
        }
        if let Err(error) = self
            .journal
            .record_refusals(&decisions, occurrence, now)
            .await
        {
            tracing::warn!(?error, "CommitTick: recording advisory refusal rows failed");
        }
        let result = first_error.map_or_else(|| Ok(report.clone()), Err);
        let batch = TickBatch {
            key: tick_key,
            decisions: decisions.len() as u32,
            pending: report.accepted.iter().cloned().collect(),
            performed: 0,
            failed: (report.refused.len() as u32).saturating_add(failed),
            error: result.as_ref().err().map(ToString::to_string),
        };
        if batch.pending.is_empty() {
            self.finish_tick_batch(batch).await;
        } else {
            self.tick_batches.push(batch);
        }
        result
    }

    /// An early CommitTick rejection has not validated either planner result. Abandon only the
    /// matching in-memory offers so the next reconciliation recaptures both durable parents; the
    /// explicit, validated no-child disposition below is the only disposition path allowed to
    /// clear its marker.
    fn abandon_tick_marker_snapshots(
        &mut self,
        replacement: Option<&super::EvacuationReplacementPlan>,
        disposition: Option<&super::EvacuationMarkerDisposition>,
    ) {
        if let Some(replacement) = replacement {
            self.consume_parked_evacuation_marker(&replacement.parent);
        }
        if let Some(disposition) = disposition {
            self.consume_parked_evacuation_marker(&disposition.parent);
        }
    }

    /// Actor-only half of the marked evacuation exchange.  The journal owns
    /// atomicity; this owner rechecks every authority and fresh fact which was
    /// unavailable to the off-actor shadow planner.
    #[allow(clippy::too_many_arguments)]
    async fn commit_evacuation_replacement(
        &mut self,
        replacement: &super::EvacuationReplacementPlan,
        occurrence: Occurrence,
        balances: &BTreeMap<FederationId, Msat>,
        facts: &super::BalanceFactsToken,
        generations_at_commit: &BTreeMap<FederationId, u64>,
        blockers: &GoalBlockers,
        now: u64,
        client: &WalletClient,
    ) -> Result<Option<IdempotencyKey>, ReplacementCommitError> {
        if replacement.fresh.occurrence != occurrence {
            return Err(ReplacementCommitError::definite_uncommitted(
                ServiceError::Storage(
                    "replacement child occurrence differs from its tick round".to_owned(),
                ),
            ));
        }
        let current_cap = wallet_core::EvacFeeCap {
            base_msat: self.policy.evac_fee_base_msat,
            bps: self.policy.evac_fee_bps,
        };
        let Action::Evacuate {
            from,
            to,
            amount,
            fee_cap,
            fee_cap_components: Some(components),
            ..
        } = &replacement.fresh.action
        else {
            return Err(ReplacementCommitError::definite_uncommitted(
                ServiceError::Storage(
                    "replacement plan does not contain a component-capped evacuation".to_owned(),
                ),
            ));
        };
        if *components != current_cap || current_cap.at(*amount) != *fee_cap {
            return Err(ReplacementCommitError::definite_uncommitted(refused(
                RefuseReason::PolicySuperseded,
                "replacement child no longer exactly matches the current evacuation fee cap"
                    .to_owned(),
            )));
        }
        // The exchange is an admission, not a best-effort repair.  Its
        // source affordability and destination capacity are both facts from
        // this exact sample; accepting with either endpoint absent would turn
        // an unknown fact into a release of the marker's reservation.
        if !balances.contains_key(from) || !balances.contains_key(to) {
            return Err(ReplacementCommitError::definite_uncommitted(refused(
                RefuseReason::Conflict,
                format!(
                    "replacement requires fresh balances for both endpoints {} -> {}",
                    from.to_hex(),
                    to.to_hex()
                ),
            )));
        }
        let old = self
            .journal
            .get(&replacement.old_key)
            .await
            // This is before the exchange boundary, but it is not an authoritative logical
            // disposition. Retain the structural marker and terminalize the tick rather than
            // clearing durable repair evidence because storage was unreadable.
            .map_err(|error| ReplacementCommitError::post_exchange_ambiguous(storage(error)))?
            .ok_or_else(|| {
                ReplacementCommitError::definite_uncommitted(refused(
                    RefuseReason::Conflict,
                    "replacement parent disappeared before exchange".to_owned(),
                ))
            })?;
        if let Actor::Agent {
            occurrence: old_occurrence,
        } = old.actor
        {
            if replacement.fresh.occurrence <= old_occurrence
                || replacement.fresh.idempotency_key == replacement.old_key
            {
                return Err(ReplacementCommitError::definite_uncommitted(refused(
                    RefuseReason::Conflict,
                    super::replacement_occurrence_error(
                        old_occurrence,
                        replacement.fresh.occurrence,
                    ),
                )));
            }
        }
        if old != replacement.parent
            || old.attempt != replacement.old_attempt
            || old.status != IntentStatus::Pending
            || old.evacuation_refusal.as_ref() != Some(&replacement.evidence)
            || !matches!(old.actor, Actor::Agent { .. })
            || !matches!(old.action, Action::Evacuate { from: old_from, .. } if old_from == *from)
        {
            return Ok(None);
        }
        if !wallet_core::evacuation_cap_qualifies_replacement(&replacement.evidence, current_cap) {
            return Ok(None);
        }
        if Self::balance_facts_changed_for(generations_at_commit, facts, &old.action)
            || Self::balance_facts_changed_for(
                generations_at_commit,
                facts,
                &replacement.fresh.action,
            )
        {
            return Err(ReplacementCommitError::definite_uncommitted(refused(
                RefuseReason::Conflict,
                "replacement endpoints changed after the fresh balance sample".to_owned(),
            )));
        }
        let pending = self
            .journal
            .pending()
            .await
            .map_err(|error| ReplacementCommitError::post_exchange_ambiguous(storage(error)))?;
        let shadow_blockers = GoalBlockers::from_intents(
            pending
                .iter()
                .filter(|intent| intent.idempotency_key != replacement.old_key),
        );
        // The actor token's projection must also not gain a third holder while
        // planning happened off actor.
        let token_shadow = blockers.excluding_key(&replacement.old_key);
        if shadow_blockers.blocks_decision(&replacement.fresh, Actor::Agent { occurrence })
            || token_shadow.blocks_decision(&replacement.fresh, Actor::Agent { occurrence })
        {
            return Ok(None);
        }
        let shadow_reservations =
            project_allocator_reservations_excluding(&self.journal, &replacement.old_key)
                .await
                .map_err(|error| ReplacementCommitError::post_exchange_ambiguous(storage(error)))?;
        let child_intent =
            Intent::from_decision(&replacement.fresh, Actor::Agent { occurrence }, now);
        admit_intent(
            &child_intent,
            Some(balances),
            Some(self.policy.per_fed_cap),
            &shadow_reservations,
        )
        .map_err(|error| ReplacementCommitError::definite_uncommitted(refusal_from_exec(error)))?;
        let exchanged = self
            .journal
            .replace_marked_evacuation(
                &replacement.old_key,
                replacement.old_attempt,
                &replacement.evidence,
                &replacement.fresh,
                now,
                &replacement.parent,
            )
            .await;
        let committed = match exchanged {
            Ok(exchanged) => exchanged,
            // `replace_marked_evacuation` maps autocommit commit failures and
            // every write seam to Retryable. A Permanent error here therefore
            // came from its autocommit closure's validation before commit, so no
            // exchange row can have been written. Keep the journal diagnostic,
            // retain the marker for a later child key, and do not poison fresh
            // goal or balance-facts authority.
            Err(error @ ExecError::Permanent(_)) => {
                return Err(ReplacementCommitError::definite_uncommitted_retain_marker(
                    storage(error),
                ));
            }
            // Every remaining failure is classified from one exact reread. In
            // production this is Retryable: its commit acknowledgement may be
            // lost, so only the complete durable outcomes below decide whether
            // the marker may be released. The otherwise-unreachable remaining
            // ExecError variants use the same fail-closed confirmation.
            // Corruption/mixed rows stay ambiguous.
            Err(error) => match self
                .replacement_exchange_outcome(replacement, &replacement.parent, &child_intent)
                .await
            {
                Ok(ReplacementExchangeOutcome::Committed) => true,
                Ok(ReplacementExchangeOutcome::Uncommitted) => {
                    // The caller turns this into a generic "no longer exclusively pending"
                    // conflict. `replace_marked_evacuation` also rejects genuine corruption
                    // through this same `Err` channel, and the two that leave EVERY reread row
                    // untouched — incoherent parent move artifacts, and a second live agent
                    // evacuation on the source — do confirm here. A dirty child namespace does
                    // NOT: `replacement_child_namespace` still reports `Contaminated`, which
                    // matches neither outcome and stays post-exchange ambiguous below. Keep the
                    // money-path signal of the two that land here instead of dropping it.
                    tracing::warn!(
                        ?error,
                        key = %replacement.old_key.0,
                        "CommitTick: replacement exchange refused and confirmed uncommitted"
                    );
                    return Ok(None);
                }
                Err(confirmation_error) => {
                    let diagnostic = format!(
                        "replacement exchange outcome is ambiguous after error {error:?}; \
                         exact confirmation failed: {confirmation_error:?}"
                    );
                    self.goal_admissions_poisoned
                        .get_or_insert_with(|| diagnostic.clone());
                    self.poison_balance_facts(diagnostic.clone());
                    // The child may be durable, but this commit did not prove
                    // it, so it starts no owner HERE.  What is fenced is
                    // PLANNING: both authorities stay poisoned until restart,
                    // so no fresh round is minted against ambiguous facts.
                    // Durable ownership recovery deliberately still runs —
                    // `reconcile_durable` rehydrates whichever side actually
                    // committed — because ADR-0029 forbids stranding a dying
                    // federation's balance behind an unproven audit read.
                    return Err(ReplacementCommitError::post_exchange_ambiguous(
                        ServiceError::Storage(diagnostic),
                    ));
                }
            },
        };
        if !committed {
            return Ok(None);
        }
        self.record_goal_admission(&replacement.fresh, Actor::Agent { occurrence });
        let mut endpoints = balance_federations(&old.action);
        endpoints.extend(balance_federations(&child_intent.action));
        for endpoint in endpoints {
            self.bump_balance_generation(endpoint);
        }
        self.resolve_key(&replacement.old_key).await;
        #[cfg(test)]
        if self
            .journal
            .take_stop_after_evacuation_replacement_before_child_driver_for_test()
        {
            // Deliberately leave only the durable Pending child.  The recovery
            // test tears this actor down immediately and proves a new service
            // rehydrates exactly that one owner before driving it.
            return Ok(Some(replacement.fresh.idempotency_key.clone()));
        }
        let external_admission = counts_against_external_cap_for_intent(&child_intent);
        self.ensure_driver(child_intent, client, external_admission, None);
        Ok(Some(replacement.fresh.idempotency_key.clone()))
    }

    /// Prove the outcome of a reported exchange failure.  A post-commit
    /// transport/database error is not permission to retry: only the exact
    /// sidecar + retired parent + deterministic child shape says which side of
    /// the atomic boundary was reached.
    async fn replacement_exchange_outcome(
        &self,
        replacement: &super::EvacuationReplacementPlan,
        old_before: &Intent,
        expected_child: &Intent,
    ) -> Result<ReplacementExchangeOutcome, ExecError> {
        // A commit acknowledgement may be lost together with a transient confirmation read. Give
        // only Retryable storage errors a few fair retries; a structural/mixed result is not made
        // safer by rereading it and remains an immediate fail-closed ambiguity.
        for attempt in 0..3 {
            match self
                .replacement_exchange_outcome_once(replacement, old_before, expected_child)
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(ExecError::Retryable(error)) if attempt < 2 => {
                    tracing::warn!(
                        attempt,
                        %error,
                        "replacement exchange confirmation read retrying"
                    );
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded confirmation loop returns on its final attempt")
    }

    /// One exact confirmation snapshot attempt. The wrapper retries only transient read failures.
    async fn replacement_exchange_outcome_once(
        &self,
        replacement: &super::EvacuationReplacementPlan,
        old_before: &Intent,
        expected_child: &Intent,
    ) -> Result<ReplacementExchangeOutcome, ExecError> {
        let relation = self
            .journal
            .evacuation_canonical_successor(&replacement.old_key)
            .await?;
        let old = self.journal.get(&replacement.old_key).await?;
        let child = self.journal.get(&replacement.fresh.idempotency_key).await?;
        let namespace = self
            .journal
            .replacement_child_namespace(&replacement.fresh.idempotency_key)
            .await?;
        match (relation, old, child, namespace) {
            (Some(relation), Some(old), Some(child), _)
                if relation.old_key == replacement.old_key
                    && relation.old_attempt == replacement.old_attempt
                    && relation.new_key == replacement.fresh.idempotency_key
                    && relation.new_attempt == 0
                    && relation.occurrence == replacement.fresh.occurrence
                    && relation.refusal == replacement.evidence
                    && old.status == IntentStatus::Failed
                    && old.attempt == replacement.old_attempt
                    && old.evacuation_refusal.as_ref() == Some(&replacement.evidence)
                    && child == *expected_child =>
            {
                Ok(ReplacementExchangeOutcome::Committed)
            }
            (
                None,
                Some(old),
                None,
                crate::journal::ReplacementChildNamespace::Pristine,
            ) if old == *old_before => {
                Ok(ReplacementExchangeOutcome::Uncommitted)
            }
            _ => Err(ExecError::Permanent(
                "replacement exchange reread was incomplete or did not exactly match either outcome"
                    .to_owned(),
            )),
        }
    }

    /// A scheduler funding move must still fit the destination's *fresh*
    /// standing-target gap.  This mirrors the existing source-affordability
    /// admission check without resizing or retargeting an actor-approved plan.
    fn funding_target_for(
        &self,
        decision: &AllocatorDecision,
    ) -> Option<(FederationId, Msat, Msat)> {
        let Action::Move { to, amount, .. } = &decision.action else {
            return None;
        };
        let target = match decision.reason {
            ReasonCode::SpendingBelowTarget => self.policy.spending_target,
            ReasonCode::StandbyBelowTarget => self.policy.standby_target,
            _ => return None,
        };
        Some((*to, *amount, target))
    }

    async fn observe_tick_outcome(&mut self, key: &IdempotencyKey, driver_finished: bool) {
        if !self
            .tick_batches
            .iter()
            .any(|batch| batch.pending.contains(key))
        {
            return;
        }
        let intent = match self.journal.get(key).await {
            Ok(Some(intent)) => intent,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(key = %key.0, ?error, "TickBatch: reading driver outcome failed");
                return;
            }
        };
        let outcome = match intent.status {
            IntentStatus::Done | IntentStatus::Awaiting => Some((true, None)),
            IntentStatus::Failed => Some((false, Some(format!("decision {} failed", key.0)))),
            IntentStatus::Pending | IntentStatus::Executing if driver_finished => Some((
                false,
                Some(format!(
                    "decision {} driver ended in {}",
                    key.0,
                    intent_status_label(intent.status)
                )),
            )),
            IntentStatus::Pending | IntentStatus::Executing => None,
        };
        let Some((performed, error)) = outcome else {
            return;
        };
        let mut completed = Vec::new();
        for (index, batch) in self.tick_batches.iter_mut().enumerate() {
            if !batch.pending.remove(key) {
                continue;
            }
            if performed {
                batch.performed = batch.performed.saturating_add(1);
            } else {
                batch.failed = batch.failed.saturating_add(1);
                if batch.error.is_none() {
                    batch.error = error.clone();
                }
            }
            if batch.pending.is_empty() {
                completed.push(index);
            }
        }
        for index in completed.into_iter().rev() {
            let batch = self.tick_batches.remove(index);
            self.finish_tick_batch(batch).await;
        }
    }

    async fn finish_tick_batch(&self, batch: TickBatch) {
        let status = if batch.failed == 0 && batch.error.is_none() {
            OperationStatus::Succeeded
        } else {
            OperationStatus::Failed
        };
        let generated_error = (batch.failed > 0).then(|| {
            format!(
                "tick: {} decision(s) did not apply (performed={} failed={})",
                batch.failed, batch.performed, batch.failed
            )
        });
        let error = batch.error.as_deref().or(generated_error.as_deref());
        if let Err(record_error) = self
            .journal
            .record_tick_terminal(
                &batch.key,
                Some((batch.decisions, batch.performed, batch.failed)),
                status,
                error,
                now_ms(),
            )
            .await
        {
            tracing::warn!(
                tick = %batch.key.0,
                ?record_error,
                "TickBatch: recording the terminal tick row failed"
            );
        }
    }

    async fn record_tick_failed(&self, key: &IdempotencyKey, diagnostic: &str) {
        if let Err(error) = self
            .journal
            .record_tick_terminal(
                key,
                None,
                OperationStatus::Failed,
                Some(diagnostic),
                now_ms(),
            )
            .await
        {
            tracing::warn!(?error, "CommitTick: recording the failed tick row failed");
        }
    }

    async fn decide_op(
        &mut self,
        req: OpRequest,
        client: &WalletClient,
    ) -> ServiceResult<DecidedOp> {
        self.decide_op_with_allocator_reservations(req, client, None)
            .await
            .map_err(DecideOpError::into_service)
    }

    async fn decide_op_with_allocator_reservations(
        &mut self,
        req: OpRequest,
        client: &WalletClient,
        allocator_reservations: Option<&Reservations>,
    ) -> Result<DecidedOp, DecideOpError> {
        let key = req.decision.idempotency_key.clone();
        if let Some(existing) = self.journal.get(&key).await.map_err(storage_refusal)? {
            return self
                .decide_existing(req, existing, client)
                .await
                .map_err(DecideOpError::definite);
        }
        // FRESH branch: no existing intent for this key (the existing-key attach returned above,
        // so this can never intercept an idempotent replay). Goal-bearing agent decisions bypass
        // CommitTick when callers use the public client directly, so project the durable live
        // goals here too. The scan is deliberately after the attach path and before journaling:
        // an unreadable live view is an unknown eligibility, while an existing key remains an
        // idempotent re-drive rather than a second admission.
        if AllocatorGoal::of_decision(&req.decision, req.actor).is_some() {
            let blocked = self
                .journal
                .pending()
                .await
                .map(|pending| GoalBlockers::from_intents(&pending))
                .map_err(storage_refusal)?;
            if blocked.blocks_decision(&req.decision, req.actor) {
                return Err(DecideOpError::definite(refused(
                    RefuseReason::Conflict,
                    format!(
                        "decision {} conflicts with allocator work already in flight",
                        key.0
                    ),
                )));
            }
        }
        // If a dest-side handler flagged the destination as joined-but-not-open, fail fast with
        // 503 instead of journaling a Pending row that would stall — the receive/direct-inflow
        // driver on the invoice deadline, a move parked unfunded. Money-safe: nothing is debited
        // before the destination opens. This is the single-owner, race-free placement — the actor
        // has just determined fresh-vs-existing under exclusive ownership, so a benign staleness
        // (the flag races `to` opening) only costs the caller a retry and can never lose an attach.
        if let Some(fed) = req.dest_unavailable {
            return Err(DecideOpError::definite(
                ServiceError::DestinationUnavailable(format!(
                    "federation {} is joined but not currently open; it is reconnecting — retry shortly",
                    fed.to_hex()
                )),
            ));
        }
        if let Some(nonce) = req.probe_session_nonce.as_deref() {
            self.validate_probe_leg_session(&req.decision.action, nonce)
                .await?;
        }

        let external_admission = counts_against_external_cap(&req.decision, req.actor);
        if external_admission && driver::external_len(&self.registry) >= EXTERNAL_DRIVER_CAP {
            tracing::warn!(key = %key.0, cap = EXTERNAL_DRIVER_CAP, "DecideOp: driver admission cap reached");
            return Err(DecideOpError::definite(refused(
                RefuseReason::Conflict,
                format!("driver admission cap {EXTERNAL_DRIVER_CAP} reached"),
            )));
        }

        let hold = self.hold_disposition(&req).await?;
        // Do not map this error and return directly.  A journal `upsert` may have committed before
        // reporting its storage error, so the fresh Agent path must resolve that ambiguity before
        // handing the original typed refusal to either a direct caller or CommitTick.
        let decide_result = match allocator_reservations {
            Some(reservations) => {
                wallet_core::decide_and_journal_with_allocator_reservations(
                    self.journal.as_ref(),
                    &req.decision,
                    req.actor,
                    req.now_ms,
                    Some(&req.balances),
                    Some(self.policy.per_fed_cap),
                    reservations,
                )
                .await
            }
            None => {
                wallet_core::decide_and_journal(
                    self.journal.as_ref(),
                    &req.decision,
                    req.actor,
                    req.now_ms,
                    Some(&req.balances),
                    Some(self.policy.per_fed_cap),
                )
                .await
            }
        };
        let decided = match decide_result {
            Ok(decided) => decided,
            Err(exec_error) => {
                let error = refusal_from_exec(exec_error);
                if AllocatorGoal::of_decision(&req.decision, req.actor).is_some()
                    && matches!(
                        &error,
                        ServiceError::Refused {
                            reason: RefuseReason::StorageError,
                            ..
                        }
                    )
                {
                    let disposition = self
                        .resolve_ambiguous_fresh_agent_admission(&req, &key)
                        .await;
                    return Err(DecideOpError { error, disposition });
                }
                return Err(DecideOpError::definite(error));
            }
        };
        let intent = match decided {
            DecideAndJournal::Drive(intent) => *intent,
            DecideAndJournal::Skip | DecideAndJournal::TerminalFailed => {
                return Err(DecideOpError::definite(ServiceError::Storage(format!(
                    "fresh intent {} unexpectedly resolved as an existing intent",
                    key.0
                ))));
            }
        };
        self.record_goal_admission(&req.decision, req.actor);
        self.record_membership_admission(&req.decision.action);
        self.record_balance_change(&req.decision.action);
        #[cfg(test)]
        if self.fail_after_fresh_admission.as_ref() == Some(&key) {
            self.fail_after_fresh_admission = None;
            return Err(DecideOpError::requested(ServiceError::Storage(format!(
                "injected post-fresh-admission failure for {}",
                key.0
            ))));
        }
        self.apply_hold_disposition(hold, req.actor)
            .await
            .map_err(DecideOpError::requested)?;
        self.ensure_driver(
            intent.clone(),
            client,
            external_admission,
            req.probe_session_nonce,
        );
        Ok(DecidedOp {
            key,
            status: intent.status,
            deduplicated: false,
        })
    }

    /// Resolve a typed storage error from the core fresh-Agent decision path.  Its `upsert` may
    /// already be durable, while an earlier core read can fail before any write.  Re-read the exact
    /// key so a known absence remains a definite non-admission.  A matching row is the one fresh
    /// admission which must invalidate old allocator and balance-facts capabilities; the normal
    /// success path below remains the only other place that records those bumps.
    async fn resolve_ambiguous_fresh_agent_admission(
        &mut self,
        req: &OpRequest,
        key: &IdempotencyKey,
    ) -> FreshMutationDisposition {
        match self.journal.get(key).await {
            Ok(Some(intent)) if is_matching_fresh_agent_intent(&intent, req) => {
                self.record_goal_admission(&req.decision, req.actor);
                self.record_balance_change(&req.decision.action);
                FreshMutationDisposition::RequestedMutationPossible
            }
            Ok(None) => {
                // The storage fault was definitely before `upsert`; do not manufacture a
                // watermark or stale otherwise-fresh balance facts.
                FreshMutationDisposition::DefiniteNoMutation
            }
            Err(error) => {
                // The requested goal and its affected balance identities remain known even though
                // the exact durable row cannot be read.  Conservatively advance precisely those
                // capabilities, rather than letting an old plan over-admit a possibly durable move.
                tracing::warn!(
                    key = %key.0,
                    ?error,
                    "fresh Agent upsert ambiguity reread failed; conservatively advancing known capabilities"
                );
                self.record_goal_admission(&req.decision, req.actor);
                self.record_balance_change(&req.decision.action);
                FreshMutationDisposition::RequestedMutationPossible
            }
            Ok(Some(intent)) => {
                // A row at the requested key which is not the requested first Agent attempt is
                // corrupt or externally concurrent.  Neither its allocator identity nor all of
                // its balance effects are safe to infer from this failed fresh admission.  Refuse
                // future authority narrowly instead of relabelling old tokens as current.
                self.goal_admissions_poisoned.get_or_insert_with(|| {
                    format!(
                        "fresh Agent upsert ambiguity reread mismatched intent at key {}",
                        key.0
                    )
                });
                self.balance_facts_poisoned.get_or_insert_with(|| {
                    format!(
                        "fresh Agent upsert ambiguity reread mismatched intent at key {}",
                        key.0
                    )
                });
                tracing::error!(
                    key = %key.0,
                    stored_actor = ?intent.actor,
                    stored_reason = ?intent.reason,
                    "fresh Agent upsert ambiguity reread found a mismatched intent; poisoning tick authority"
                );
                FreshMutationDisposition::UnknownIdentityPoison
            }
        }
    }

    async fn decide_existing(
        &mut self,
        req: OpRequest,
        existing: Intent,
        client: &WalletClient,
    ) -> ServiceResult<DecidedOp> {
        let key = existing.idempotency_key.clone();
        if existing.status == IntentStatus::Failed && req.actor == Actor::User {
            validate_manual_retry_anchor(&existing.action, &req.decision.action)?;
            // lnv2 allows ONE payment attempt per invoice: once the prior attempt
            // committed its send op (`operation_id` is the pre/post-fund reservation
            // boundary), a re-`pay` can only dedup-reattach to that dead op — it can
            // never succeed. Refuse loudly instead of refreshing an unwinnable intent.
            // Failed pays WITHOUT an op (fee over cap, no gateway route) never reached
            // the federation, so those stay manually retryable below.
            if matches!(existing.action, Action::Pay { .. }) && existing.operation_id.is_some() {
                return Err(refused(
                    RefuseReason::Conflict,
                    "this invoice already consumed its single payment attempt (the prior \
                     attempt was refunded or failed after submission); request a fresh \
                     invoice from the payee"
                        .to_owned(),
                ));
            }
            let external_admission = counts_against_external_cap(&req.decision, req.actor);
            if external_admission && driver::external_len(&self.registry) >= EXTERNAL_DRIVER_CAP {
                return Err(refused(
                    RefuseReason::Conflict,
                    format!("driver admission cap {EXTERNAL_DRIVER_CAP} reached"),
                ));
            }
            let hold = self.hold_disposition(&req).await?;
            let mut refreshed = Intent::from_decision(&req.decision, req.actor, req.now_ms);
            refreshed.attempt = existing.attempt.checked_add(1).ok_or_else(|| {
                refused(
                    RefuseReason::Conflict,
                    "manual retry attempt counter overflow".to_owned(),
                )
            })?;
            self.admit_refreshed(&refreshed, &req.balances).await?;
            // Commit the evacuation retry before preempting its probe hold. If the
            // post-commit preemption fails, reconcile can see the durable evacuation and
            // finish the preemption before re-driving it; the reverse order could release
            // the hold with no accepted replacement intent.
            self.journal
                .retry_failed_intent(&refreshed)
                .await
                .map_err(storage_refusal)?;
            self.record_balance_change(&refreshed.action);
            // This retry is durable before the subsequent hold/driver work.  Invalidate every
            // old membership-world plan now: either later step may fail, but neither may relabel
            // a plan minted before this Join/Recover retry.
            self.record_membership_admission(&refreshed.action);
            self.apply_hold_disposition(hold, req.actor).await?;
            self.ensure_driver(refreshed.clone(), client, external_admission, None);
            return Ok(DecidedOp {
                key,
                status: IntentStatus::Pending,
                deduplicated: false,
            });
        }

        if existing.status == IntentStatus::Done {
            validate_terminal_dedup_anchor(&existing.action, &req.decision.action)?;
            return Ok(DecidedOp {
                key,
                status: IntentStatus::Done,
                deduplicated: true,
            });
        }

        if let Some(nonce) = req.probe_session_nonce.as_deref() {
            self.validate_probe_leg_session(&req.decision.action, nonce)
                .await?;
        }
        validate_live_attach(&existing.action, &req.decision.action)?;
        let external_admission = counts_against_external_cap(&req.decision, req.actor);
        let probe_session_nonce = req.probe_session_nonce;

        let result = wallet_core::decide_and_journal(
            self.journal.as_ref(),
            &req.decision,
            req.actor,
            req.now_ms,
            None,
            None,
        )
        .await
        .map_err(refusal_from_exec)?;
        match result {
            DecideAndJournal::Drive(intent) => self.ensure_driver(
                (*intent).clone(),
                client,
                external_admission,
                probe_session_nonce,
            ),
            DecideAndJournal::Skip if existing.status == IntentStatus::Awaiting => {
                self.ensure_driver(existing.clone(), client, true, None)
            }
            DecideAndJournal::Skip | DecideAndJournal::TerminalFailed => {}
        }
        Ok(DecidedOp {
            key,
            status: existing.status,
            deduplicated: true,
        })
    }

    async fn admit_refreshed(
        &self,
        intent: &Intent,
        balances: &BTreeMap<FederationId, Msat>,
    ) -> ServiceResult<()> {
        let reservations = project_strict_reservations(&self.journal)
            .await
            .map_err(storage_refusal)?;
        admit_intent(
            intent,
            Some(balances),
            Some(self.policy.per_fed_cap),
            &reservations,
        )
        .map_err(refusal_from_exec)
    }

    async fn hold_disposition(&self, req: &OpRequest) -> ServiceResult<HoldDisposition> {
        let Some(source) = spending_federation(&req.decision.action) else {
            return Ok(HoldDisposition::None);
        };
        let Some(record) = self
            .journal
            .probe_record(&source)
            .await
            .map_err(storage_refusal)?
        else {
            return Ok(HoldDisposition::None);
        };
        let Some(session) = record.in_flight else {
            return Ok(HoldDisposition::None);
        };
        if matches!(req.decision.action, Action::Evacuate { .. }) {
            return Ok(HoldDisposition::Preempt {
                candidate: source,
                session,
            });
        }
        if req.probe_session_nonce.as_deref() == Some(session.nonce.as_str()) {
            return Ok(HoldDisposition::None);
        }
        Err(refused(
            RefuseReason::FedHeldByProbe,
            format!(
                "federation {} is held by probe session {}",
                source.to_hex(),
                session.nonce
            ),
        ))
    }

    async fn validate_probe_leg_session(
        &self,
        action: &Action,
        session_nonce: &str,
    ) -> ServiceResult<()> {
        let Action::Move { from, to, .. } = action else {
            return Err(refused(
                RefuseReason::Conflict,
                "a probe session may own only a move leg".to_owned(),
            ));
        };
        let is_leg_in = self
            .journal
            .probe_record(to)
            .await
            .map_err(storage_refusal)?
            .and_then(|record| record.in_flight)
            .is_some_and(|session| session.nonce == session_nonce && session.from == *from);
        let is_leg_out = self
            .journal
            .probe_record(from)
            .await
            .map_err(storage_refusal)?
            .and_then(|record| record.in_flight)
            .is_some_and(|session| session.nonce == session_nonce && session.from == *to);
        if is_leg_in || is_leg_out {
            Ok(())
        } else {
            Err(refused(
                RefuseReason::Conflict,
                "probe session is no longer active".to_owned(),
            ))
        }
    }

    async fn apply_hold_disposition(
        &mut self,
        disposition: HoldDisposition,
        fallback_actor: Actor,
    ) -> ServiceResult<()> {
        let HoldDisposition::Preempt { candidate, session } = disposition else {
            return Ok(());
        };
        driver::abort_probe_session(&self.registry, candidate, &session.nonce);
        let key = probe_umbrella_key(&candidate, &session.nonce);
        let occurrence = occurrence_from_nonce(&session.nonce)
            .map_err(|error| ServiceError::Storage(error.to_string()))?;
        let in_key = move_key(
            &session.from,
            &candidate,
            Msat(session.amount_msat),
            Msat(session.leg_fee_cap_msat),
            occurrence,
        );
        let in_record = self.journal.get_move(&in_key).await.map_err(storage)?;
        let out_record = match (in_record.as_ref(), session.out_net_msat) {
            (Some(in_record), Some(out_net_msat)) => {
                let out_net = Msat(out_net_msat);
                let out_fee_cap =
                    probe_out_fee_cap(in_record.amount, out_net, Msat(session.leg_fee_cap_msat));
                let out_key = move_key(&candidate, &session.from, out_net, out_fee_cap, occurrence);
                self.journal.get_move(&out_key).await.map_err(storage)?
            }
            _ => None,
        };
        let cost = probe_cost(in_record.as_ref(), out_record.as_ref());
        let actor = self
            .journal
            .operation(&crate::journal::OperationRef::Key(key.clone()))
            .await
            .map_err(storage)?
            .map_or(fallback_actor, |row| row.actor);
        self.journal
            .record_probe_outcome(
                &candidate,
                &session.nonce,
                None,
                &key,
                OperationKind::Probe {
                    fed: candidate,
                    from: session.from,
                    amount_msat: Msat(session.amount_msat),
                    cost_msat: cost,
                },
                actor,
                OperationStatus::Failed,
                Some("probe preempted by evacuation; no attempt recorded"),
            )
            .await
            .map_err(storage)?;
        self.refresh_probe_budget(&key).await?;
        Ok(())
    }

    async fn decide_probe(
        &mut self,
        candidate: ProbeCandidate,
        client: &WalletClient,
    ) -> ServiceResult<ProbeDecision> {
        if let Some(session) = self
            .journal
            .probe_record(&candidate.federation)
            .await
            .map_err(storage)?
            .and_then(|record| record.in_flight)
        {
            if let ProbeAdmission::ResumeOnly { expected_nonce } = &candidate.admission {
                if session.nonce != *expected_nonce {
                    return Err(refused(
                        RefuseReason::Conflict,
                        format!(
                            "probe resume deferred: federation {} now has durable session {}, not \
                             expected session {}",
                            candidate.federation.to_hex(),
                            session.nonce,
                            expected_nonce
                        ),
                    ));
                }
            }
            self.ensure_probe_budget_loaded()?;
            let key = probe_umbrella_key(&candidate.federation, &session.nonce);
            if !self
                .probe_budget
                .entries
                .iter()
                .any(|entry| entry.key == key)
            {
                self.probe_budget.entries.push(ProbeBudgetEntry {
                    key: key.clone(),
                    effective_at_ms: candidate.now_ms,
                    cost_msat: None,
                    active: matches!(candidate.actor, Actor::Agent { .. }),
                    reserved_msat: probe_budget_reservation(
                        session.amount_msat,
                        session.leg_fee_cap_msat,
                    ),
                });
            }
            let decision = ProbeDecision {
                candidate: candidate.federation,
                key,
                session,
                deduplicated: true,
            };
            self.ensure_probe_driver(&decision, candidate.actor, client);
            return Ok(decision);
        }

        let fresh_policy = match &candidate.admission {
            ProbeAdmission::Fresh(snapshot)
                if snapshot
                    .is_current_for(&self.probe_policy_authority, &self.probe_policy_version) =>
            {
                snapshot.policy().clone()
            }
            ProbeAdmission::Fresh(_) => {
                return Err(refused(
                    RefuseReason::PolicySuperseded,
                    format!(
                        "probe policy snapshot for federation {} is no longer current",
                        candidate.federation.to_hex()
                    ),
                ));
            }
            ProbeAdmission::ResumeOnly { expected_nonce } => {
                return Err(refused(
                    RefuseReason::Conflict,
                    format!(
                        "probe resume deferred: federation {} no longer has expected durable \
                         session {}",
                        candidate.federation.to_hex(),
                        expected_nonce
                    ),
                ));
            }
        };
        self.ensure_probe_budget_loaded()?;
        let in_flight = self.journal.reservation_intents().await.map_err(storage)?;
        if in_flight
            .iter()
            .any(|intent| spending_federation(&intent.action) == Some(candidate.federation))
        {
            return Err(refused(
                RefuseReason::Conflict,
                format!(
                    "probe deferred: an in-flight intent already spends from federation {}",
                    candidate.federation.to_hex()
                ),
            ));
        }

        self.check_probe_budget(candidate.now_ms)?;
        let session = ProbeSession {
            nonce: ledger_nonce(),
            from: candidate.source,
            amount_msat: fresh_policy.probe_amount.0,
            leg_fee_cap_msat: fresh_policy.max_fee.0,
            c_spendable_before_in_msat: candidate.baseline.0,
            out_net_msat: None,
            started_at_ms: candidate.now_ms,
        };
        let key = probe_umbrella_key(&candidate.federation, &session.nonce);
        self.journal
            .begin_probe_session(&candidate.federation, &session)
            .await
            .map_err(storage)?;
        self.probe_budget.entries.push(ProbeBudgetEntry {
            key: key.clone(),
            effective_at_ms: candidate.now_ms,
            cost_msat: None,
            active: matches!(candidate.actor, Actor::Agent { .. }),
            reserved_msat: probe_budget_reservation(session.amount_msat, session.leg_fee_cap_msat),
        });
        self.journal
            .record_probe_invocation(
                &key,
                OperationKind::Probe {
                    fed: candidate.federation,
                    from: candidate.source,
                    amount_msat: fresh_policy.probe_amount,
                    cost_msat: None,
                },
                candidate.actor,
                candidate.now_ms,
            )
            .await
            .map_err(storage)?;
        let decision = ProbeDecision {
            candidate: candidate.federation,
            key,
            session,
            deduplicated: false,
        };
        self.ensure_probe_driver(&decision, candidate.actor, client);
        Ok(decision)
    }

    fn check_probe_budget(&mut self, now_ms: u64) -> ServiceResult<()> {
        self.ensure_probe_budget_loaded()?;
        self.probe_budget.entries.retain(|entry| {
            entry.active || now_ms.saturating_sub(entry.effective_at_ms) < PROBE_BUDGET_WINDOW_MS
        });
        let attempts = self
            .probe_budget
            .entries
            .iter()
            .filter(|entry| entry.cost_msat.is_some())
            .count() as u32;
        let spend_msat = self
            .probe_budget
            .entries
            .iter()
            .filter_map(|entry| entry.cost_msat)
            .fold(0u64, u64::saturating_add);
        let active = self
            .probe_budget
            .entries
            .iter()
            .filter(|entry| entry.active)
            .count() as u32;
        let reserved_spend = self
            .probe_budget
            .entries
            .iter()
            .filter(|entry| entry.active)
            .map(|entry| entry.reserved_msat)
            .fold(0u64, u64::saturating_add);
        let next_attempts = attempts.saturating_add(active).saturating_add(1);
        let next_spend =
            spend_msat
                .saturating_add(reserved_spend)
                .saturating_add(probe_budget_reservation(
                    self.policy.probe_amount.0,
                    self.policy.max_fee.0,
                ));
        if next_attempts > self.policy.max_probe_attempts_per_week
            || next_spend > self.policy.max_probe_spend_per_week.0
        {
            return Err(refused(
                RefuseReason::BudgetExhausted,
                "weekly probe budget exhausted or fully reserved".to_owned(),
            ));
        }
        Ok(())
    }

    fn ensure_probe_budget_loaded(&self) -> ServiceResult<()> {
        if let Some(error) = &self.probe_budget.load_error {
            return Err(ServiceError::Storage(format!(
                "probe budget state could not be loaded: {error}"
            )));
        }
        Ok(())
    }

    async fn refresh_probe_budget(&mut self, key: &IdempotencyKey) -> ServiceResult<()> {
        let row = self
            .journal
            .operation(&crate::journal::OperationRef::Key(key.clone()))
            .await
            .map_err(storage)?;
        let Some(row) = row else {
            return Ok(());
        };
        let OperationKind::Probe { cost_msat, .. } = row.kind else {
            return Ok(());
        };
        if !matches!(row.actor, Actor::Agent { .. }) {
            return Ok(());
        }
        let reserved_msat = self
            .probe_budget
            .entries
            .iter()
            .find(|entry| entry.key == *key)
            .map_or_else(
                || probe_budget_reservation(self.policy.probe_amount.0, self.policy.max_fee.0),
                |entry| entry.reserved_msat,
            );
        self.probe_budget.entries.retain(|entry| entry.key != *key);
        if let Some(Msat(cost_msat)) = cost_msat {
            self.probe_budget.entries.push(ProbeBudgetEntry {
                key: key.clone(),
                effective_at_ms: row.created_at_ms.max(row.updated_at_ms),
                cost_msat: Some(cost_msat),
                active: false,
                reserved_msat: 0,
            });
        } else if !row.status.is_terminal() {
            self.probe_budget.entries.push(ProbeBudgetEntry {
                key: key.clone(),
                effective_at_ms: row.created_at_ms.max(row.updated_at_ms),
                cost_msat: None,
                active: true,
                reserved_msat,
            });
        }
        Ok(())
    }

    fn ensure_probe_driver(
        &mut self,
        decision: &ProbeDecision,
        actor: Actor,
        client: &WalletClient,
    ) {
        if driver::contains(&self.registry, &decision.key) {
            return;
        }
        let generation = self.next_generation();
        driver::spawn_probe(
            &self.registry,
            generation,
            decision.clone(),
            actor,
            self.probe_policy(),
            self.policy.per_fed_cap,
            self.runtime.clone(),
            client.clone(),
        );
    }

    fn probe_policy(&self) -> ProbePolicy {
        self.policy.probe_policy()
    }

    async fn apply_transition(
        &self,
        key: &IdempotencyKey,
        transition: JournalTransition,
    ) -> ServiceResult<TransitionResult> {
        match transition {
            JournalTransition::Upsert {
                expected_attempt,
                intent,
            } => {
                if intent.idempotency_key != *key {
                    return Err(ServiceError::Storage(
                        "transition key does not match the intent key".to_owned(),
                    ));
                }
                let existing = self
                    .journal
                    .get(key)
                    .await
                    .map_err(storage)?
                    .ok_or_else(|| {
                        ServiceError::Storage(
                            "transition Upsert cannot create an absent intent".to_owned(),
                        )
                    })?;
                if existing.attempt != expected_attempt
                    || intent.attempt != expected_attempt
                    || existing.action != intent.action
                    || existing.actor != intent.actor
                    || existing.reason != intent.reason
                    || existing.idempotency_key != intent.idempotency_key
                    || existing.created_at_ms != intent.created_at_ms
                    || existing.max_fee != intent.max_fee
                    || existing.operation_id != intent.operation_id
                    || existing.invoice != intent.invoice
                {
                    return Err(ServiceError::Storage(
                        "transition Upsert changed immutable intent identity".to_owned(),
                    ));
                }
                self.journal.upsert(&intent).await.map_err(storage)?;
                Ok(TransitionResult::Applied)
            }
            JournalTransition::CompareAndSet {
                expected_attempt,
                expected,
                new,
            } => self
                .journal
                .set_status_if(key, expected_attempt, expected, new)
                .await
                .map(TransitionResult::Compared)
                .map_err(storage),
            JournalTransition::ResetRetryable {
                expected_attempt,
                structural_refusal,
            } => self
                .journal
                .reset_retryable(key, expected_attempt, structural_refusal)
                .await
                .map(|_| TransitionResult::Applied)
                .map_err(storage),
            JournalTransition::SetRawTerminal {
                fence,
                status,
                error,
            } => self
                .journal
                .set_raw_terminal_if_fenced(key, &fence, status, error.as_deref())
                .await
                .map(TransitionResult::Compared)
                .map_err(storage),
            JournalTransition::SetStatus {
                expected_attempt,
                status,
                error,
            } => {
                // Preserve the actor protocol's benign compare result for a late driver.  The
                // durable writer repeats this shared predicate atomically, so this read is only
                // the reply-shaping fast path, never the fence itself.
                let Some(current) = self.journal.get(key).await.map_err(storage)? else {
                    return Ok(TransitionResult::Compared(false));
                };
                if current.attempt != expected_attempt
                    || !intent_status_transition_allowed(current.status, status)
                {
                    return Ok(TransitionResult::Compared(false));
                }
                match self
                    .journal
                    .set_status(key, expected_attempt, status, error.as_deref())
                    .await
                {
                    Ok(()) => Ok(TransitionResult::Applied),
                    // `set_status` has no compare result.  Its error can be post-commit, and the
                    // caller captured the action before entering this writer; propagate it so the
                    // command handler invalidates that known action's balance facts.
                    Err(error) => Err(storage(error)),
                }
            }
            JournalTransition::DriverFinished { .. } => {
                // This is process bookkeeping only.  In particular, do not refresh the intent
                // before deregistering the finished generation: that read can fail, while the
                // completion still must release local ownership so an out-of-actor recovery pass
                // can rehydrate the durable work.
                Ok(TransitionResult::Applied)
            }
            JournalTransition::Refresh => Ok(TransitionResult::Applied),
        }
    }

    async fn snapshot(&self, scope: SnapshotScope) -> ServiceResult<Snapshot> {
        match scope {
            SnapshotScope::Intent(key) => self
                .journal
                .get(&key)
                .await
                .map(Snapshot::Intent)
                .map_err(storage),
            SnapshotScope::Reservations => project_strict_reservations(&self.journal)
                .await
                .map(Snapshot::Reservations)
                .map_err(storage),
            SnapshotScope::Registry => Ok(Snapshot::Registry {
                drivers: driver::len(&self.registry),
            }),
            SnapshotScope::Probe(fed) => self
                .journal
                .probe_record(&fed)
                .await
                .map(Snapshot::Probe)
                .map_err(storage),
        }
    }

    async fn reconcile(&mut self, client: &WalletClient) -> ServiceResult<ReconcileReport> {
        // Ambiguous exchange authority is deliberately sticky until restart/recovery. A parked
        // full-parent snapshot is only a one-cycle planning handoff in a healthy actor; while
        // poisoned it must neither be offered nor cleared, or a later reconciliation could erase
        // the durable marker whose ambiguity caused the poison.
        let planner_authority_poisoned = self.goal_admissions_poisoned.is_some();
        if planner_authority_poisoned {
            self.parked_evacuation_handoff = None;
        } else if let Err(error) = self.release_parked_evacuation_markers_at_reconcile().await {
            // The full-parent CAS remains fail-closed and the unprocessed snapshots remain
            // parked. A marker-local release fault must not suppress the strict durable scan
            // below: that scan rehydrates unrelated work and observes whichever side of an
            // ambiguity actually committed before fresh scheduler authority is issued.
            tracing::warn!(
                ?error,
                "ReconcileDecide: parked evacuation marker release failed; retaining its snapshots and continuing durable recovery"
            );
        }
        // This is the only scan permitted to add parked markers. It captures the exact full
        // parent only when this scheduler reconciliation itself skips that parent for the
        // replacement planner; durable recovery must never rescan and overwrite the list.
        let report = self
            .reconcile_durable(client, ReconciliationMarkerPolicy::CaptureForPlanner)
            .await;
        // Offer one exact snapshot to this scheduler cycle. Selecting after the scan preserves
        // pending-index order for new captures, while a failed clear has already rotated its row
        // behind independent queued parents. Do this even when durable reconciliation fails so
        // the captured prefix still receives its bounded next-cycle release.
        if !planner_authority_poisoned {
            self.parked_evacuation_handoff = self.parked_evacuation_markers.first().cloned();
        }
        let mut report = report?;
        // Do not let tick ineligibility prevent crash recovery.  In particular a pending
        // Join/Recover must regain its driver (and unrelated intents must be rehydrated) before
        // this authoritative eligibility mint refuses the cycle.  A caller receiving this scoped
        // error must skip planning, while the spawned drivers continue to converge durable work.
        report.admission_snapshot = match self.issue_tick_plan_token().await {
            Ok(token) => token,
            Err(error) => {
                // This cycle cannot hand its captures to CommitTick. Drop every in-memory
                // snapshot without a durable CAS, so the next healthy reconciliation recaptures
                // the current marker instead of releasing this stale offer.
                self.parked_evacuation_markers.clear();
                self.parked_evacuation_handoff = None;
                return Err(error);
            }
        };
        Ok(report)
    }

    /// Rehydrate all durable live work without issuing scheduler authority.  Ownership
    /// recovery uses this narrower operation because a tick-ineligible result is not a
    /// storage failure and must not make its retry loop spin forever after rehydration.
    async fn reconcile_durable(
        &mut self,
        client: &WalletClient,
        marker_policy: ReconciliationMarkerPolicy,
    ) -> ServiceResult<ReconcileReport> {
        let pending = self.journal.pending().await.map_err(storage)?;
        // br-p93: project the in-flight allocator goals from the RAW scan, before the registry
        // ownership filter below. An intent a live driver already owns is re-driven by nobody
        // (`redriven` stays 0) yet is still very much in flight, so filtering first would let the
        // next tick re-issue exactly the goal that driver is working on. A scan fault propagates
        // as a reconcile failure, which the scheduler treats as unknown — no commit at all.
        let blocked = GoalBlockers::from_intents(&pending);
        // Recovery may observe the crash window after an evacuation intent committed but
        // before its probe preemption committed. Resolve every such durable hold before
        // any orphan is allowed to drive, so a probe leg and its preempting evacuation
        // can never both be started by this pass.
        for intent in &pending {
            let Action::Evacuate { from, .. } = &intent.action else {
                continue;
            };
            let from = *from;
            let Some(session) = self
                .journal
                .probe_record(&from)
                .await
                .map_err(storage)?
                .and_then(|record| record.in_flight)
            else {
                continue;
            };
            self.apply_hold_disposition(
                HoldDisposition::Preempt {
                    candidate: from,
                    session,
                },
                intent.actor,
            )
            .await?;
        }

        let mut report = ReconcileReport {
            blocked,
            ..ReconcileReport::default()
        };
        for mut intent in pending {
            // Only a qualifying current policy hands this marker to the
            // replacement planner. Equal, crossed, decreased, unrelated and
            // integer-effectively-equal edits deliberately re-drive the
            // original Retryable attempt.
            if self.marker_is_planner_owned(&intent) {
                match marker_policy {
                    ReconciliationMarkerPolicy::PreservePlannerOwned => continue,
                    ReconciliationMarkerPolicy::CaptureForPlanner => {
                        if self.goal_admissions_poisoned.is_none()
                            && self.qualifying_pending_evacuation_marker(&intent)
                            && !self.parked_evacuation_markers.contains(&intent)
                        {
                            self.parked_evacuation_markers.push(intent.clone());
                        }
                        continue;
                    }
                    ReconciliationMarkerPolicy::RedriveWithoutPlanner => {
                        // An ambiguous fresh admission must preserve its exact evidence: ownership
                        // can recover only after restart proves a new authority. Otherwise this
                        // cycle has committed not to plan, so discard only its stale in-memory
                        // handoff and let the normal Pending/Executing path re-own the old work.
                        if self.goal_admissions_poisoned.is_some() {
                            continue;
                        }
                        self.consume_parked_evacuation_marker(&intent);
                        // Pending -> Executing atomically clears the durable evidence. Arm the
                        // existing one-shot suppression before that claim, so a renewed structural
                        // refusal does not wake a scheduler cycle which still cannot plan.
                        self.suppress_next_marker_wake(&intent);
                    }
                }
            }
            // A probe session is the durable owner of its legs. Once that session has
            // resolved (including evacuation preemption), an orphaned leg is stale and
            // must not be re-driven on a later recovery pass.
            let probe_session_nonce = if intent.reason == wallet_core::ReasonCode::ActiveProbe {
                let Some(nonce) = self.probe_leg_session_nonce(&intent).await? else {
                    self.fail_orphaned_probe_leg(&intent).await?;
                    continue;
                };
                Some(nonce)
            } else {
                None
            };
            if driver::owns_intent(&self.registry, &intent) {
                continue;
            }
            if intent.status == IntentStatus::Executing {
                self.journal
                    .set_status(
                        &intent.idempotency_key,
                        intent.attempt,
                        IntentStatus::Pending,
                        None,
                    )
                    .await
                    .map_err(storage)?;
                intent.status = IntentStatus::Pending;
                report.executing_normalized += 1;
            }
            let external_admission = counts_against_external_cap_for_intent(&intent);
            self.ensure_driver(intent, client, external_admission, probe_session_nonce);
            report.redriven += 1;
        }
        let awaiting = self.journal.awaiting().await.map_err(storage)?;
        for intent in awaiting {
            if driver::contains(&self.registry, &intent.idempotency_key) {
                continue;
            }
            let external_admission = counts_against_external_cap_for_intent(&intent);
            self.ensure_driver(intent, client, external_admission, None);
            report.awaiters_rehydrated += 1;
        }
        Ok(report)
    }

    /// Terminalize a recovery-discovered probe leg whose durable session has already resolved.
    ///
    /// `Journal::set_status` has no compare result: `Ok(())` therefore means this invocation
    /// changed the row and permits the usual scoped generation bump.  An error can follow a
    /// committed transaction too; this intent already identifies the affected federations, so
    /// preserve unrelated facts while conservatively advancing this action's generations.
    async fn fail_orphaned_probe_leg(&mut self, intent: &Intent) -> ServiceResult<()> {
        match self
            .journal
            .set_status(
                &intent.idempotency_key,
                intent.attempt,
                IntentStatus::Failed,
                Some("probe session is no longer active"),
            )
            .await
        {
            Ok(()) => {
                self.record_balance_change(&intent.action);
                Ok(())
            }
            Err(error) => {
                self.record_balance_change(&intent.action);
                Err(storage(error))
            }
        }
    }

    async fn probe_leg_session_nonce(&self, intent: &Intent) -> ServiceResult<Option<String>> {
        let Action::Move {
            from,
            to,
            amount,
            fee_cap,
            ..
        } = &intent.action
        else {
            return Ok(None);
        };
        let (from, to, amount, fee_cap) = (*from, *to, *amount, *fee_cap);

        for (candidate, source) in [(to, from), (from, to)] {
            let Some(session) = self
                .journal
                .probe_record(&candidate)
                .await
                .map_err(storage)?
                .and_then(|record| record.in_flight)
            else {
                continue;
            };
            if session.from != source {
                continue;
            }
            let occurrence = occurrence_from_nonce(&session.nonce)
                .map_err(|error| ServiceError::Storage(error.to_string()))?;
            if move_key(&from, &to, amount, fee_cap, occurrence) == intent.idempotency_key {
                return Ok(Some(session.nonce));
            }
        }
        Ok(None)
    }

    fn ensure_driver(
        &mut self,
        intent: Intent,
        client: &WalletClient,
        external_admission: bool,
        probe_session_nonce: Option<String>,
    ) {
        if driver::contains(&self.registry, &intent.idempotency_key) {
            driver::request_redrive(&self.registry, &intent.idempotency_key);
            return;
        }
        let generation = self.next_generation();
        if intent.status == IntentStatus::Awaiting {
            driver::spawn_awaiter(
                &self.registry,
                generation,
                intent,
                self.runtime.clone(),
                client.clone(),
                external_admission,
            );
        } else {
            // Every production driver carries the one-shot actor writer for raw artifacts and
            // MoveRecords; Join/Recover additionally use it for their short publication fence.
            // Detached tests/custom executors retain their supplied implementation.
            let executor: Arc<dyn Executor> = self.runtime.as_ref().map_or_else(
                || self.executor.clone(),
                |runtime| {
                    #[cfg(test)]
                    if runtime.scheduler_tick_fixture_enabled_for_test() {
                        Arc::new(wallet_core::MockExecutor::new())
                    } else {
                        Arc::new(runtime.service_executor_with_client(
                            Some(self.policy.per_fed_cap),
                            client.clone(),
                        ))
                    }
                    #[cfg(not(test))]
                    Arc::new(runtime.service_executor_with_client(
                        Some(self.policy.per_fed_cap),
                        client.clone(),
                    ))
                },
            );
            driver::spawn_intent(
                &self.registry,
                generation,
                intent,
                self.journal.clone(),
                executor,
                client.clone(),
                self.perform_timeout,
                external_admission,
                probe_session_nonce,
            );
        }
    }

    async fn finish_driver(
        &mut self,
        key: &IdempotencyKey,
        generation: u64,
        expected_attempt: u32,
        retry_awaiter: bool,
        client: &WalletClient,
    ) {
        let Some(finished) = driver::finish(&self.registry, key, generation) else {
            return;
        };
        let intent = match self.journal.get(key).await {
            Ok(intent) => intent,
            Err(error) => {
                tracing::warn!(key = %key.0, ?error, "DriverFinished: intent refresh failed");
                self.schedule_ownership_recovery(client.clone());
                return;
            }
        };
        let Some(intent) = intent else {
            return;
        };
        let (external_admission, probe_session_nonce) = match &finished.kind {
            driver::DriverKind::Intent {
                external_admission,
                probe_session_nonce,
            } => (*external_admission, probe_session_nonce.clone()),
            driver::DriverKind::Awaiter { external_admission } => (*external_admission, None),
            driver::DriverKind::Probe { .. } => (false, None),
        };
        let same_attempt = intent.attempt == expected_attempt;
        let hand_off_awaiting = same_attempt
            && intent.status == IntentStatus::Awaiting
            && (matches!(&finished.kind, driver::DriverKind::Intent { .. })
                || finished.redrive_requested);
        let honor_requested_redrive =
            same_attempt && intent.status == IntentStatus::Pending && finished.redrive_requested;
        // A manual retry can replace attempt N while N's registered driver still owns the
        // process-local slot.  Its admission marks that owner for redrive; once N finishes,
        // hand the slot directly to the newer Pending attempt instead of requiring an unrelated
        // reconcile pass.  Keep the same-attempt Pending case explicit above: it is only safe
        // when a caller actually requested the redrive.
        let hand_off_newer_pending_attempt =
            !same_attempt && intent.status == IntentStatus::Pending && finished.redrive_requested;
        let retry_same_awaiter = retry_awaiter
            && matches!(&finished.kind, driver::DriverKind::Awaiter { .. })
            && same_attempt
            && intent.status == IntentStatus::Awaiting;
        let planner_owned_marker = self.marker_is_planner_owned(&intent);
        if hand_off_awaiting
            || honor_requested_redrive
            || hand_off_newer_pending_attempt
            || retry_same_awaiter
        {
            if planner_owned_marker {
                return;
            }
            self.ensure_driver(intent, client, external_admission, probe_session_nonce);
        }
    }

    fn marker_is_planner_owned(&self, intent: &Intent) -> bool {
        matches!(intent.actor, Actor::Agent { occurrence } if occurrence.0 < u64::MAX)
            && matches!(intent.action, Action::Evacuate { .. })
            && intent.evacuation_refusal.as_ref().is_some_and(|evidence| {
                wallet_core::evacuation_cap_qualifies_replacement(
                    evidence,
                    wallet_core::EvacFeeCap {
                        base_msat: self.policy.evac_fee_base_msat,
                        bps: self.policy.evac_fee_bps,
                    },
                )
            })
    }

    /// A parked marker is a complete Pending parent, not a key or a fresh rescan result. The
    /// exact parent is later passed to the journal's full-row CAS, so a changed row can never
    /// release a newly qualifying evacuation.
    fn qualifying_pending_evacuation_marker(&self, intent: &Intent) -> bool {
        intent.status == IntentStatus::Pending
            && matches!(intent.actor, Actor::Agent { occurrence } if occurrence.0 < u64::MAX)
            && matches!(intent.action, Action::Evacuate { .. })
            && intent.operation_id.is_none()
            && intent.invoice.is_none()
            && intent
                .evacuation_refusal
                .as_ref()
                .is_some_and(|evidence| self.structural_marker_qualifies(evidence))
    }

    /// Recover ownership after a finished driver's post-removal intent refresh fails.  This task
    /// deliberately runs outside the serial actor: database faults back off without delaying
    /// unrelated accounting, while `reconcile` reads only durable live work and refuses to attach
    /// a second driver when another owner won the race.
    fn schedule_ownership_recovery(&mut self, client: WalletClient) {
        self.ownership_recovery_generation = self.ownership_recovery_generation.wrapping_add(1);
        if self.ownership_recovery_active {
            return;
        }
        self.ownership_recovery_active = true;
        tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_millis(25);
            const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
            loop {
                match client.recover_driver_ownership().await {
                    Ok(generation) => {
                        match client.finish_driver_ownership_recovery(generation).await {
                            Ok(true) => return,
                            // A later fault was queued while this task reconciled.  This same
                            // task owns it too.  Pace this successful-but-stale scan exactly like
                            // a failed scan: a persistent post-DriverFinished `get` fault must
                            // not turn one coalesced worker into a tight durable-rescan loop.
                            Ok(false) => {
                                tracing::warn!(
                                    ?backoff,
                                    "DriverFinished ownership recovery generation changed; retrying"
                                );
                                tokio::time::sleep(backoff).await;
                                backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                            }
                            Err(ServiceError::ActorStopped | ServiceError::ShuttingDown) => return,
                            Err(error) => {
                                tracing::warn!(
                                    ?error,
                                    "DriverFinished ownership recovery completion failed"
                                );
                                return;
                            }
                        }
                    }
                    Err(ServiceError::ActorStopped | ServiceError::ShuttingDown) => return,
                    Err(error) => {
                        tracing::warn!(
                            ?error,
                            ?backoff,
                            "DriverFinished ownership recovery failed; retrying"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
                    }
                }
            }
        });
    }

    fn next_generation(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.generation
    }

    async fn resolve_or_park(
        &mut self,
        key: IdempotencyKey,
        target: AwaitTarget,
        deadline: Instant,
        waiter: oneshot::Sender<ServiceResult<AwaitOutcome>>,
    ) {
        match self.await_outcome(&key, target).await {
            Ok(Some(outcome)) => {
                let _ = waiter.send(Ok(outcome));
            }
            Err(error) => {
                let _ = waiter.send(Err(error));
            }
            Ok(None) if deadline <= Instant::now() => {
                let _ = waiter.send(Err(ServiceError::Timeout));
            }
            Ok(None) => self.waiters.entry(key).or_default().push(PendingWaiter {
                target,
                deadline,
                reply: waiter,
            }),
        }
    }

    async fn await_outcome(
        &self,
        key: &IdempotencyKey,
        target: AwaitTarget,
    ) -> ServiceResult<Option<AwaitOutcome>> {
        let Some(intent) = self.journal.get(key).await.map_err(storage)? else {
            return Err(ServiceError::NotFound(format!(
                "operation {} was not found",
                key.0
            )));
        };
        if target == AwaitTarget::InvoiceArtifact {
            let invoice = intent.invoice.clone().or(self
                .journal
                .move_record(key)
                .await
                .map_err(storage)?
                .and_then(|record| record.invoice));
            if let Some(invoice) = invoice {
                return Ok(Some(AwaitOutcome::Invoice(invoice)));
            }
        }
        if matches!(intent.status, IntentStatus::Done | IntentStatus::Failed) {
            return Ok(Some(AwaitOutcome::Terminal(Box::new(intent))));
        }
        Ok(None)
    }

    async fn resolve_key(&mut self, key: &IdempotencyKey) {
        let Some(waiters) = self.waiters.remove(key) else {
            return;
        };
        let intent = match self.journal.get(key).await.map_err(storage) {
            Ok(Some(intent)) => intent,
            Ok(None) => {
                let error = ServiceError::NotFound(format!("operation {} was not found", key.0));
                for waiter in waiters {
                    let _ = waiter.reply.send(Err(error.clone()));
                }
                return;
            }
            Err(error) => {
                for waiter in waiters {
                    let _ = waiter.reply.send(Err(error.clone()));
                }
                return;
            }
        };
        let terminal = matches!(intent.status, IntentStatus::Done | IntentStatus::Failed);
        let needs_move_invoice = intent.invoice.is_none()
            && waiters
                .iter()
                .any(|waiter| waiter.target == AwaitTarget::InvoiceArtifact);
        let invoice = if needs_move_invoice {
            match self.journal.move_record(key).await.map_err(storage) {
                Ok(record) => record.and_then(|record| record.invoice),
                Err(error) => {
                    for waiter in waiters {
                        let _ = waiter.reply.send(Err(error.clone()));
                    }
                    return;
                }
            }
        } else {
            intent.invoice.clone()
        };
        let mut parked = Vec::new();
        for waiter in waiters {
            if waiter.target == AwaitTarget::InvoiceArtifact {
                if let Some(invoice) = &invoice {
                    let _ = waiter
                        .reply
                        .send(Ok(AwaitOutcome::Invoice(invoice.clone())));
                    continue;
                }
            }
            if terminal {
                let _ = waiter
                    .reply
                    .send(Ok(AwaitOutcome::Terminal(Box::new(intent.clone()))));
            } else {
                parked.push(waiter);
            }
        }
        if !parked.is_empty() {
            self.waiters.insert(key.clone(), parked);
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.waiters
            .values()
            .flatten()
            .map(|waiter| waiter.deadline)
            .min()
    }

    fn expire_waiters(&mut self) {
        let now = Instant::now();
        let mut pending = HashMap::new();
        for (key, waiters) in self.waiters.drain() {
            let mut live = Vec::new();
            for waiter in waiters {
                if waiter.deadline <= now {
                    let _ = waiter.reply.send(Err(ServiceError::Timeout));
                } else {
                    live.push(waiter);
                }
            }
            if !live.is_empty() {
                pending.insert(key, live);
            }
        }
        self.waiters = pending;
    }

    fn drain_waiters(&mut self, error: ServiceError) {
        for (_, waiters) in self.waiters.drain() {
            for waiter in waiters {
                let _ = waiter.reply.send(Err(error.clone()));
            }
        }
    }
}

#[derive(Clone)]
enum HoldDisposition {
    None,
    Preempt {
        candidate: FederationId,
        session: ProbeSession,
    },
}

async fn load_probe_budget(journal: &FedimintJournal, policy: &Policy) -> ProbeBudgetState {
    let now_ms = now_ms();
    let rows = match journal
        .probe_budget_ledger_rows(now_ms, PROBE_BUDGET_WINDOW_MS)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            return ProbeBudgetState {
                entries: Vec::new(),
                load_error: Some(storage(error).to_string()),
            };
        }
    };
    let mut state = ProbeBudgetState::default();
    for row in rows {
        if !matches!(row.actor, Actor::Agent { .. }) {
            continue;
        }
        let OperationKind::Probe {
            fed,
            amount_msat,
            cost_msat,
            ..
        } = row.kind
        else {
            continue;
        };
        let active = cost_msat.is_none() && !row.status.is_terminal();
        if cost_msat.is_none() && !active {
            continue;
        }
        let reserved_msat = if active {
            match journal.probe_record(&fed).await {
                Ok(Some(record)) => record
                    .in_flight
                    .filter(|session| {
                        probe_umbrella_key(&fed, &session.nonce) == row.correlation_key
                    })
                    .map_or_else(
                        || probe_budget_reservation(amount_msat.0, policy.max_fee.0),
                        |session| {
                            probe_budget_reservation(session.amount_msat, session.leg_fee_cap_msat)
                        },
                    ),
                Ok(None) => probe_budget_reservation(amount_msat.0, policy.max_fee.0),
                Err(error) => {
                    return ProbeBudgetState {
                        entries: Vec::new(),
                        load_error: Some(storage(error).to_string()),
                    };
                }
            }
        } else {
            0
        };
        state.entries.push(ProbeBudgetEntry {
            key: row.correlation_key,
            effective_at_ms: row.created_at_ms.max(row.updated_at_ms),
            cost_msat: cost_msat.map(|amount| amount.0),
            active,
            reserved_msat,
        });
    }
    state
}

/// Upper-bound an active probe's source-net outflow: a completed round trip can consume
/// both legs' fee caps, while a failed return leg can strand the principal plus leg-IN
/// fees. The larger exposure is reserved until the durable session resolves.
fn probe_budget_reservation(amount_msat: u64, leg_fee_cap_msat: u64) -> u64 {
    amount_msat
        .saturating_add(leg_fee_cap_msat)
        .max(leg_fee_cap_msat.saturating_mul(2))
}

fn spending_federation(action: &Action) -> Option<FederationId> {
    match action {
        Action::Move { from, .. } | Action::Evacuate { from, .. } | Action::Pay { from, .. } => {
            Some(*from)
        }
        Action::DirectInflow { .. }
        | Action::Receive { .. }
        | Action::Join { .. }
        | Action::Recover { .. }
        | Action::RefuseInflow { .. } => None,
    }
}

fn intent_status_label(status: IntentStatus) -> &'static str {
    match status {
        IntentStatus::Pending => "pending",
        IntentStatus::Executing => "executing",
        IntentStatus::Done => "done",
        IntentStatus::Awaiting => "awaiting",
        IntentStatus::Failed => "failed",
    }
}

fn counts_against_external_cap(decision: &wallet_core::AllocatorDecision, actor: Actor) -> bool {
    actor == Actor::User
        && decision.reason != wallet_core::ReasonCode::ActiveProbe
        && !matches!(decision.action, Action::Evacuate { .. })
}

fn counts_against_external_cap_for_intent(intent: &Intent) -> bool {
    intent.actor == Actor::User
        && intent.reason != wallet_core::ReasonCode::ActiveProbe
        && !matches!(intent.action, Action::Evacuate { .. })
}

fn validate_live_attach(existing: &Action, requested: &Action) -> ServiceResult<()> {
    let matches = match (existing, requested) {
        (
            Action::Pay {
                from: old_from,
                amount: old_amount,
                fee_cap: old_fee,
                payment_hash: old_hash,
                ..
            },
            Action::Pay {
                from,
                amount,
                fee_cap,
                payment_hash,
                ..
            },
        ) => (old_from, old_amount, old_fee, old_hash) == (from, amount, fee_cap, payment_hash),
        (
            Action::Receive {
                to: old_to,
                amount: old_amount,
                fee_cap: old_fee,
                nonce: old_nonce,
                ..
            },
            Action::Receive {
                to,
                amount,
                fee_cap,
                nonce,
                ..
            },
        ) => (old_to, old_amount, old_fee, old_nonce) == (to, amount, fee_cap, nonce),
        // The preselected `gateway` is excluded on purpose (as `..` already excludes it for the
        // Pay/Receive arms above): it is a routing HINT the executor may substitute, not part of
        // the money identity, so a re-plan that picks a cheaper gateway must still attach to the
        // live intent for the same from/to/amount/fee_cap.
        (
            Action::Move {
                from: old_from,
                to: old_to,
                amount: old_amount,
                fee_cap: old_fee,
                ..
            },
            Action::Move {
                from,
                to,
                amount,
                fee_cap,
                ..
            },
        )
        | (
            Action::Evacuate {
                from: old_from,
                to: old_to,
                amount: old_amount,
                fee_cap: old_fee,
                ..
            },
            Action::Evacuate {
                from,
                to,
                amount,
                fee_cap,
                ..
            },
        ) => (old_from, old_to, old_amount, old_fee) == (from, to, amount, fee_cap),
        (
            Action::DirectInflow {
                to: old_to,
                amount: old_amount,
                fee_cap: old_fee,
            },
            Action::DirectInflow {
                to,
                amount,
                fee_cap,
            },
        ) => (old_to, old_amount, old_fee) == (to, amount, fee_cap),
        (
            Action::Join {
                federation: old_fed,
                invite: old_invite,
                ..
            },
            Action::Join {
                federation, invite, ..
            },
        ) => (old_fed, old_invite) == (federation, invite),
        (
            Action::Recover {
                federation: old_fed,
                invite: old_invite,
            },
            Action::Recover {
                federation, invite, ..
            },
        ) => (old_fed, old_invite) == (federation, invite),
        (Action::RefuseInflow { .. }, Action::RefuseInflow { .. }) => existing == requested,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(refused(
            RefuseReason::SizingConflict {
                field: "request sizing".to_owned(),
            },
            "same-key request conflicts with the live intent's sizing fields".to_owned(),
        ))
    }
}

fn validate_manual_retry_anchor(existing: &Action, requested: &Action) -> ServiceResult<()> {
    let matches = match (existing, requested) {
        (
            Action::Pay {
                payment_hash: old_hash,
                ..
            },
            Action::Pay { payment_hash, .. },
        ) => old_hash == payment_hash,
        (
            Action::Receive {
                to: old_to,
                amount: old_amount,
                nonce: old_nonce,
                ..
            },
            Action::Receive {
                to, amount, nonce, ..
            },
        ) => (old_to, old_amount, old_nonce) == (to, amount, nonce),
        (
            Action::DirectInflow {
                to: old_to,
                amount: old_amount,
                ..
            },
            Action::DirectInflow { to, amount, .. },
        ) => (old_to, old_amount) == (to, amount),
        _ => existing == requested,
    };
    if matches {
        Ok(())
    } else {
        Err(refused(
            RefuseReason::Conflict,
            "manual retry changed the operation's idempotency anchor".to_owned(),
        ))
    }
}

fn validate_terminal_dedup_anchor(existing: &Action, requested: &Action) -> ServiceResult<()> {
    let matches = match (existing, requested) {
        (
            Action::Pay {
                payment_hash: old_hash,
                ..
            },
            Action::Pay { payment_hash, .. },
        ) => old_hash == payment_hash,
        (
            Action::Receive {
                to: old_to,
                amount: old_amount,
                nonce: old_nonce,
                ..
            },
            Action::Receive {
                to, amount, nonce, ..
            },
        ) => (old_to, old_amount, old_nonce) == (to, amount, nonce),
        (
            Action::DirectInflow {
                to: old_to,
                amount: old_amount,
                ..
            },
            Action::DirectInflow { to, amount, .. },
        ) => (old_to, old_amount) == (to, amount),
        (
            Action::Join {
                federation: old_fed,
                invite: old_invite,
                ..
            },
            Action::Join {
                federation, invite, ..
            },
        ) => (old_fed, old_invite) == (federation, invite),
        _ => existing == requested,
    };
    if matches {
        Ok(())
    } else {
        Err(refused(
            RefuseReason::Conflict,
            "same-key request changed the completed operation's idempotency anchor".to_owned(),
        ))
    }
}

fn transition_may_resolve(transition: &JournalTransition) -> bool {
    match transition {
        JournalTransition::CompareAndSet { new, .. } => {
            matches!(new, IntentStatus::Done | IntentStatus::Failed)
        }
        JournalTransition::DriverFinished { .. } => true,
        JournalTransition::Upsert { .. } => true,
        JournalTransition::SetStatus { .. }
        | JournalTransition::SetRawTerminal { .. }
        | JournalTransition::ResetRetryable { .. }
        | JournalTransition::Refresh => true,
    }
}

fn transition_terminal_status(transition: &JournalTransition) -> bool {
    match transition {
        JournalTransition::CompareAndSet { new, .. } => {
            matches!(new, IntentStatus::Done | IntentStatus::Failed)
        }
        JournalTransition::SetStatus { status, .. } => {
            matches!(status, IntentStatus::Done | IntentStatus::Failed)
        }
        JournalTransition::SetRawTerminal { status, .. } => {
            matches!(status, IntentStatus::Done | IntentStatus::Failed)
        }
        JournalTransition::Upsert { intent, .. } => {
            matches!(intent.status, IntentStatus::Done | IntentStatus::Failed)
        }
        JournalTransition::DriverFinished { .. }
        | JournalTransition::ResetRetryable { .. }
        | JournalTransition::Refresh => false,
    }
}

/// Whether a successful transition actually changed an intent.  Keep this separate from
/// `transition_may_resolve`: a false CAS is a successful request/response but not a durable
/// mutation.
fn transition_mutated(result: &TransitionResult) -> bool {
    match result {
        TransitionResult::Compared(changed) => *changed,
        TransitionResult::Applied => true,
    }
}

/// Whether this transition can write an intent row.  Keep this separate from reply
/// shape: [`JournalTransition::DriverFinished`] and [`JournalTransition::Refresh`]
/// deliberately return `Applied` so their registry/waiter/probe handling still runs.
fn transition_is_intent_mutation(transition: &JournalTransition) -> bool {
    matches!(
        transition,
        JournalTransition::Upsert { .. }
            | JournalTransition::CompareAndSet { .. }
            | JournalTransition::SetStatus { .. }
            | JournalTransition::SetRawTerminal { .. }
            | JournalTransition::ResetRetryable { .. }
    )
}

pub(super) fn storage(error: ExecError) -> ServiceError {
    let message = match error {
        ExecError::Retryable(message) | ExecError::Permanent(message) => message,
        ExecError::StructuralEvacuationRefusal(evidence) => evidence.diagnostic,
        ExecError::Unsupported => "journal operation is unsupported".to_owned(),
    };
    ServiceError::Storage(message)
}

fn storage_refusal(error: ExecError) -> ServiceError {
    as_storage_refusal(storage(error))
}

/// A row discovered after a failed fresh upsert is evidence for the requested admission only when
/// it is still that exact first Agent intent.  A key match alone is insufficient: a mismatched row
/// must fail closed rather than receive this request's allocator watermark.
fn is_matching_fresh_agent_intent(intent: &Intent, req: &OpRequest) -> bool {
    intent.idempotency_key == req.decision.idempotency_key
        && intent.attempt == 0
        && intent.action == req.decision.action
        && intent.reason == req.decision.reason
        && intent.actor == req.actor
}

fn as_storage_refusal(error: ServiceError) -> ServiceError {
    match error {
        ServiceError::Storage(message) => refused(RefuseReason::StorageError, message),
        error => error,
    }
}

pub(super) fn refusal_from_exec(error: ExecError) -> ServiceError {
    let message = match error {
        ExecError::Retryable(message) | ExecError::Permanent(message) => message,
        ExecError::StructuralEvacuationRefusal(evidence) => evidence.diagnostic,
        ExecError::Unsupported => "operation is unsupported".to_owned(),
    };
    let reason = if message.contains("conflicts with the existing request") {
        RefuseReason::SizingConflict {
            field: "request sizing".to_owned(),
        }
    } else if message.contains("insufficient balance after reservations") {
        RefuseReason::InsufficientAfterReservations
    } else if message.contains("per-fed cap") {
        RefuseReason::OverCap
    } else if message.starts_with("journal:") || message.starts_with("journal db error:") {
        RefuseReason::StorageError
    } else {
        RefuseReason::Conflict
    };
    refused(reason, message)
}

fn refused(reason: RefuseReason, message: String) -> ServiceError {
    ServiceError::Refused { reason, message }
}

#[cfg(test)]
mod route_blocked_designation_tests {
    use super::*;
    use wallet_core::{Occurrence, RefusalDiagnostics};

    // A(spending, deficit), B(standby, surplus): the designated funding direction is (B, A).
    const A: FederationId = FederationId([0xAA; 32]);
    const B: FederationId = FederationId([0xBB; 32]);
    const MIN_MOVE: u64 = 5_000;

    fn snapshot(
        route: BTreeMap<(FederationId, FederationId), RouteEconomics>,
    ) -> AllocatorSnapshot {
        AllocatorSnapshot {
            federations: vec![],
            spending_fed: Some(A),
            standby_fed: Some(B),
            per_fed_cap: Msat(0),
            target_spending_balance: Msat(0),
            standby_target: Msat(0),
            max_fee: Msat(0),
            max_fee_bps_of_move: 100,
            evac_fee_base_msat: Msat(200_000),
            evac_fee_bps: 300,
            min_move: Msat(MIN_MOVE),
            route_economics_by_pair: route,
            reservations: Reservations::default(),
            now: 0,
        }
    }

    fn blocked(status: RouteStatus) -> BTreeMap<(FederationId, FederationId), RouteEconomics> {
        BTreeMap::from([(
            (B, A),
            RouteEconomics {
                resolved_gateway: None,
                min_viable_amount: Msat(0),
                status,
            },
        )])
    }

    fn refuse(fed: FederationId, reason: ReasonCode, want: u64) -> AllocatorDecision {
        AllocatorDecision {
            action: Action::RefuseInflow {
                fed,
                reason,
                diagnostics: RefusalDiagnostics {
                    want: Some(Msat(want)),
                    min_move: Some(Msat(MIN_MOVE)),
                    ..Default::default()
                },
            },
            reason,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey(String::new()),
        }
    }

    fn round(snapshot: AllocatorSnapshot, decisions: Vec<AllocatorDecision>) -> PlannedTickRound {
        PlannedTickRound {
            decisions,
            suppressed: vec![],
            replacement_deferred: vec![],
            deferred: vec![],
            probes: vec![],
            active_probes: BTreeMap::new(),
            snapshot,
            blocked: GoalBlockers::default(),
            replacement: None,
            marker_disposition: None,
        }
    }

    #[test]
    fn non_dust_route_block_redesignates() {
        // An Unroutable pair with a real (at-or-above-floor) shortfall re-designates.
        let r = round(
            snapshot(blocked(RouteStatus::Unroutable)),
            vec![refuse(A, ReasonCode::SpendingBelowTarget, MIN_MOVE + 1_000)],
        );
        assert_eq!(first_route_blocked_designation(&r), Some((B, A)));
    }

    #[test]
    fn dust_uneconomic_does_not_redesignate() {
        // §Q5 emits an UneconomicRoute refusal even at a sub-floor DUST gap; the gap guard
        // (`want >= min_move`) must keep this from triggering an unrequested full-target rebalance.
        let r = round(
            snapshot(blocked(RouteStatus::UneconomicAtAnySize)),
            vec![refuse(A, ReasonCode::UneconomicRoute, 1)],
        );
        assert_eq!(first_route_blocked_designation(&r), None);
    }

    #[test]
    fn non_dust_uneconomic_redesignates() {
        let r = round(
            snapshot(blocked(RouteStatus::UneconomicAtAnySize)),
            vec![refuse(A, ReasonCode::UneconomicRoute, MIN_MOVE + 1)],
        );
        assert_eq!(first_route_blocked_designation(&r), Some((B, A)));
    }

    #[test]
    fn overcap_only_refusal_does_not_redesignate() {
        // Cap-caused, not route-caused: re-designating away does not help a cap-full destination.
        let r = round(
            snapshot(blocked(RouteStatus::Unroutable)),
            vec![refuse(A, ReasonCode::OverCap, MIN_MOVE + 1_000)],
        );
        assert_eq!(first_route_blocked_designation(&r), None);
    }

    #[test]
    fn funded_destination_does_not_redesignate() {
        let mv = AllocatorDecision {
            action: Action::Move {
                from: B,
                to: A,
                amount: Msat(MIN_MOVE + 1_000),
                fee_cap: Msat(60),
                gateway: None,
            },
            reason: ReasonCode::SpendingBelowTarget,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey(String::new()),
        };
        let r = round(
            snapshot(blocked(RouteStatus::Unroutable)),
            vec![
                mv,
                refuse(A, ReasonCode::SpendingBelowTarget, MIN_MOVE + 1_000),
            ],
        );
        assert_eq!(first_route_blocked_designation(&r), None);
    }

    #[test]
    fn routable_pair_does_not_redesignate() {
        // A Routable status is never a wedge regardless of any refusal.
        let r = round(
            snapshot(blocked(RouteStatus::Routable)),
            vec![refuse(A, ReasonCode::SpendingBelowTarget, MIN_MOVE + 1_000)],
        );
        assert_eq!(first_route_blocked_designation(&r), None);
    }

    #[test]
    fn no_designation_returns_none() {
        let mut s = snapshot(BTreeMap::new());
        s.spending_fed = None;
        assert_eq!(first_route_blocked_designation(&round(s, vec![])), None);
    }
}
