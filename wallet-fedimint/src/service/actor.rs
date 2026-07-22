use super::driver::{self, Registry};
use super::*;
use crate::runtime::{
    ledger_nonce, move_key, now_ms, occurrence_from_nonce, probe_cost, probe_gated_members,
    probe_out_fee_cap, probe_umbrella_key, PROBE_BUDGET_WINDOW_MS,
};
use crate::tick::{build_snapshot, decisions_to_apply, pinned_input_problems};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use wallet_core::{
    admit_intent, probe_verdict, Action, Actor, DecideAndJournal, IntentStatus, Journal,
    OperationKind, OperationStatus, ProbePolicy, ScorerPolicy,
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

struct TickBatch {
    key: IdempotencyKey,
    decisions: u32,
    pending: BTreeSet<IdempotencyKey>,
    performed: u32,
    failed: u32,
    error: Option<String>,
}

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
    probe_budget: ProbeBudgetState,
    policy_wake: tokio::sync::watch::Sender<u64>,
    tick_balances: Option<(wallet_core::Occurrence, BTreeMap<FederationId, Msat>)>,
    tick_batches: Vec<TickBatch>,
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
    let executor = runtime.as_ref().map_or(executor, |runtime| {
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
        probe_budget: ProbeBudgetState {
            entries: Vec::new(),
            load_error: Some("probe budget state is still loading".to_owned()),
        },
        policy_wake,
        tick_balances: None,
        tick_batches: Vec::new(),
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

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

impl ActorState {
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
                let refresh_budget = matches!(&transition, JournalTransition::Refresh)
                    && self
                        .probe_budget
                        .entries
                        .iter()
                        .any(|entry| entry.key == key);
                let resolve_waiters = transition_may_resolve(&transition);
                let finished_generation = match &transition {
                    JournalTransition::DriverFinished { generation } => Some(*generation),
                    _ => None,
                };
                let result = self.apply_transition(&key, transition).await;
                if result.is_ok() {
                    if let Some(generation) = finished_generation {
                        if intake {
                            if let Some(client) = client {
                                self.finish_driver(&key, generation, client).await;
                            }
                        } else {
                            driver::finish(&self.registry, &key, generation);
                        }
                    }
                    if refresh_budget {
                        self.refresh_probe_budget(&key).await;
                    }
                    if resolve_waiters {
                        self.resolve_key(&key).await;
                    }
                    self.observe_tick_outcome(&key, finished_generation.is_some())
                        .await;
                }
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
            Command::DecideTickRound {
                facts,
                route_failures,
                reply,
            } => {
                let result = if intake {
                    self.decide_tick_round(facts, &route_failures).await
                } else {
                    Err(ServiceError::ShuttingDown)
                };
                let _ = reply.send(result);
            }
            Command::CommitTick {
                decisions,
                balances,
                tick_key,
                planned_generation,
                reply,
            } => {
                let result = if intake {
                    match client {
                        Some(client) => {
                            self.commit_tick(
                                decisions,
                                balances,
                                tick_key,
                                planned_generation,
                                client,
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
            Command::Shutdown { reply } => {
                let _ = reply.send(Err(ServiceError::ShuttingDown));
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

    async fn decide_tick_round(
        &mut self,
        facts: ProbeFacts,
        route_failures: &[crate::runtime::MoveRouteProblem],
    ) -> ServiceResult<TickRound> {
        let mut policy = TickPolicy::from(&self.policy);
        policy.occurrence = facts.occurrence;
        policy.now = facts.now_ms;
        let reservations = self.project_reservations().await?;
        let candidates = self
            .journal
            .list_candidates_report()
            .await
            .map_err(storage)?;
        let joined = self.journal.list_federations().await.map_err(storage)?;
        let auto_joined = probe_gated_members(
            joined.into_iter().map(|(id, _)| id),
            candidates
                .candidates
                .iter()
                .map(|(id, record)| (*id, record.state)),
        );
        let balances = facts_from_probes(&facts.probes);
        let mut probes = facts.probes;
        let mut evacuation_fallback: Option<(FederationId, TickRound)> = None;
        for failure in route_failures {
            let round = self
                .build_tick_round(&probes, &policy, &auto_joined, &reservations)
                .await?;
            if let Some((fallback_source, fallback)) = &evacuation_fallback {
                let still_evacuating = round.decisions.iter().any(|decision| {
                    matches!(decision.action, Action::Evacuate { from, .. } if from == *fallback_source)
                });
                if !still_evacuating {
                    self.remember_tick_balances(facts.occurrence, &balances);
                    return Ok(fallback.clone());
                }
            }
            if failure.evacuation_source_route
                && round.decisions.iter().any(|decision| {
                    matches!(decision.action, Action::Evacuate { from, .. } if from == failure.from)
                })
            {
                evacuation_fallback = Some((failure.from, round));
            }
            if !crate::runtime::mark_gateway_unavailable(&mut probes, failure.mark_unavailable) {
                break;
            }
        }
        let round = self
            .build_tick_round(&probes, &policy, &auto_joined, &reservations)
            .await?;
        if let Some((fallback_source, fallback)) = evacuation_fallback {
            let still_evacuating = round.decisions.iter().any(|decision| {
                matches!(decision.action, Action::Evacuate { from, .. } if from == fallback_source)
            });
            if !still_evacuating {
                self.remember_tick_balances(facts.occurrence, &balances);
                return Ok(fallback);
            }
        }
        self.remember_tick_balances(facts.occurrence, &balances);
        Ok(round)
    }

    async fn build_tick_round(
        &self,
        probes: &[(FederationId, crate::probe::ProbeResult)],
        policy: &TickPolicy,
        auto_joined: &std::collections::BTreeSet<FederationId>,
        reservations: &Reservations,
    ) -> ServiceResult<TickRound> {
        let preliminary = build_snapshot(
            probes,
            policy,
            &ScorerPolicy::default(),
            auto_joined,
            &BTreeMap::new(),
        );
        let mut active_probes = BTreeMap::new();
        if let Some(source) = preliminary.spending_fed {
            for (id, _) in probes {
                if *id == source {
                    continue;
                }
                match self.journal.probe_record(id).await {
                    Ok(record) => {
                        active_probes.insert(
                            *id,
                            probe_verdict(
                                &record.map(|record| record.attempts).unwrap_or_default(),
                                source,
                                policy.now,
                                &policy.probe_gate_policy,
                            ),
                        );
                    }
                    Err(error) => tracing::warn!(
                        federation = %id.to_hex(),
                        ?error,
                        "DecideTickRound: unreadable probe record; omitting verdict"
                    ),
                }
            }
        }
        let mut snapshot = build_snapshot(
            probes,
            policy,
            &ScorerPolicy::default(),
            auto_joined,
            &active_probes,
        );
        snapshot.reservations = reservations.clone();
        let decisions = wallet_core::decide(&snapshot, policy.occurrence);
        let problems = pinned_input_problems(policy, &snapshot, probes, &decisions);
        if !problems.is_empty() {
            return Err(ServiceError::Storage(format!(
                "tick: {}",
                problems.join("; ")
            )));
        }
        Ok(TickRound {
            decisions,
            spending_fed: snapshot.spending_fed,
            standby_fed: snapshot.standby_fed,
            planned_generation: self.policy_generation,
        })
    }

    fn remember_tick_balances(
        &mut self,
        occurrence: wallet_core::Occurrence,
        balances: &BTreeMap<FederationId, Msat>,
    ) {
        self.tick_balances = Some((occurrence, balances.clone()));
    }

    async fn commit_tick(
        &mut self,
        decisions: Vec<AllocatorDecision>,
        fresh_balances: Option<BTreeMap<FederationId, Msat>>,
        existing_tick_key: Option<IdempotencyKey>,
        planned_generation: u64,
        client: &WalletClient,
    ) -> ServiceResult<CommitTickReport> {
        // Policy-generation guard (§6a P1): a PutPolicy may have landed while the daemon
        // was validating routes over the network between DecideTickRound and here. These
        // decisions were sized against caps/targets the operator has since changed, so we
        // refuse the whole batch — journaling nothing — and let the next cycle replan
        // under the current policy. No money op is admitted on stale sizing.
        if planned_generation != self.policy_generation {
            self.tick_balances = None;
            let refused = decisions
                .iter()
                .map(|decision| TickRefusal {
                    key: decision.idempotency_key.clone(),
                    reason: RefuseReason::PolicySuperseded,
                    message: format!(
                        "tick planned under policy generation {planned_generation}, current is {}",
                        self.policy_generation
                    ),
                })
                .collect();
            return Ok(CommitTickReport {
                accepted: Vec::new(),
                refused,
            });
        }
        let occurrence = decisions
            .first()
            .map(|decision| decision.occurrence)
            .or_else(|| {
                self.tick_balances
                    .as_ref()
                    .map(|(occurrence, _)| *occurrence)
            })
            .ok_or_else(|| ServiceError::Storage("CommitTick: no decided round".to_owned()))?;
        if decisions
            .iter()
            .any(|decision| decision.occurrence != occurrence)
        {
            return Err(ServiceError::Storage(
                "CommitTick: decisions span multiple occurrences".to_owned(),
            ));
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
        if let Err(error) = self
            .ensure_fresh_tick_decisions(&decisions, occurrence)
            .await
        {
            self.record_tick_failed(&tick_key, &error.to_string()).await;
            self.tick_balances = None;
            return Err(error);
        }
        let balances = match fresh_balances {
            Some(balances) => balances,
            None => match self
                .tick_balances
                .as_ref()
                .filter(|(planned, _)| *planned == occurrence)
                .map(|(_, balances)| balances.clone())
            {
                Some(balances) => balances,
                None => {
                    let error = ServiceError::Storage(format!(
                        "CommitTick: no decided balance facts for occurrence {}",
                        occurrence.0
                    ));
                    self.record_tick_failed(&tick_key, &error.to_string()).await;
                    self.tick_balances = None;
                    return Err(error);
                }
            },
        };
        let mut report = CommitTickReport::default();
        let mut first_error = None;
        let mut failed = 0_u32;
        for decision in decisions_to_apply(&decisions) {
            // Advisory allocator decisions are durable refusal facts, not executable work.
            // `record_refusals` below is their single ledger writer, matching the standalone
            // executor path's `apply_with_admission` projection.
            if !decision.action.is_executable() {
                continue;
            }
            let request = OpRequest {
                decision: decision.clone(),
                actor: Actor::Agent { occurrence },
                now_ms: now,
                balances: balances.clone(),
                probe_session_nonce: None,
            };
            match self.decide_op(request, client).await {
                Ok(decided) => report.accepted.push(decided.key),
                Err(ServiceError::Refused { reason, message }) => {
                    if let Err(error) = self
                        .journal
                        .record_tick_dropped_refusal(&decision, occurrence, now, &message)
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
                Err(error) => {
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
        self.tick_balances = None;
        result
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
        let status = if batch.failed == 0 {
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

    async fn ensure_fresh_tick_decisions(
        &self,
        decisions: &[AllocatorDecision],
        occurrence: wallet_core::Occurrence,
    ) -> ServiceResult<()> {
        for decision in decisions_to_apply(decisions) {
            if let Some(intent) = self
                .journal
                .get(&decision.idempotency_key)
                .await
                .map_err(storage)?
            {
                if matches!(
                    intent.status,
                    IntentStatus::Done | IntentStatus::Awaiting | IntentStatus::Failed
                ) {
                    return Err(ServiceError::Storage(format!(
                        "tick: occurrence {} would replay already-terminal decision {}",
                        occurrence.0, decision.idempotency_key.0
                    )));
                }
            }
        }
        Ok(())
    }

    async fn decide_op(
        &mut self,
        req: OpRequest,
        client: &WalletClient,
    ) -> ServiceResult<DecidedOp> {
        let key = req.decision.idempotency_key.clone();
        if let Some(existing) = self.journal.get(&key).await.map_err(storage_refusal)? {
            return self.decide_existing(req, existing, client).await;
        }
        if let Some(nonce) = req.probe_session_nonce.as_deref() {
            self.validate_probe_leg_session(&req.decision.action, nonce)
                .await?;
        }

        let external_admission = counts_against_external_cap(&req.decision, req.actor);
        if external_admission && driver::external_len(&self.registry) >= EXTERNAL_DRIVER_CAP {
            tracing::warn!(key = %key.0, cap = EXTERNAL_DRIVER_CAP, "DecideOp: driver admission cap reached");
            return Err(refused(
                RefuseReason::Conflict,
                format!("driver admission cap {EXTERNAL_DRIVER_CAP} reached"),
            ));
        }

        let hold = self.hold_disposition(&req).await?;
        let decided = wallet_core::decide_and_journal(
            self.journal.as_ref(),
            &req.decision,
            req.actor,
            req.now_ms,
            Some(&req.balances),
            Some(self.policy.per_fed_cap),
        )
        .await
        .map_err(refusal_from_exec)?;
        let intent = match decided {
            DecideAndJournal::Drive(intent) => *intent,
            DecideAndJournal::Skip | DecideAndJournal::TerminalFailed => {
                return Err(ServiceError::Storage(format!(
                    "fresh intent {} unexpectedly resolved as an existing intent",
                    key.0
                )))
            }
        };
        self.apply_hold_disposition(hold, req.actor).await?;
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
        let reservations = self
            .project_reservations()
            .await
            .map_err(as_storage_refusal)?;
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
        self.refresh_probe_budget(&key).await;
        Ok(())
    }

    async fn decide_probe(
        &mut self,
        candidate: ProbeCandidate,
        client: &WalletClient,
    ) -> ServiceResult<ProbeDecision> {
        self.ensure_probe_budget_loaded()?;
        if let Some(session) = self
            .journal
            .probe_record(&candidate.federation)
            .await
            .map_err(storage)?
            .and_then(|record| record.in_flight)
        {
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
            amount_msat: self.policy.probe_amount.0,
            leg_fee_cap_msat: self.policy.max_fee.0,
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
                    amount_msat: self.policy.probe_amount,
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

    async fn refresh_probe_budget(&mut self, key: &IdempotencyKey) {
        let row = match self
            .journal
            .operation(&crate::journal::OperationRef::Key(key.clone()))
            .await
        {
            Ok(row) => row,
            Err(error) => {
                tracing::warn!(key = %key.0, ?error, "JournalTransition::Refresh: probe budget refresh failed");
                return;
            }
        };
        let Some(row) = row else {
            return;
        };
        let OperationKind::Probe { cost_msat, .. } = row.kind else {
            return;
        };
        if !matches!(row.actor, Actor::Agent { .. }) {
            return;
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
            JournalTransition::Upsert(intent) => {
                if intent.idempotency_key != *key {
                    return Err(ServiceError::Storage(
                        "transition key does not match the intent key".to_owned(),
                    ));
                }
                self.journal.upsert(&intent).await.map_err(storage)?;
                Ok(TransitionResult::Applied)
            }
            JournalTransition::CompareAndSet { expected, new } => self
                .journal
                .set_status_if(key, expected, new)
                .await
                .map(TransitionResult::Compared)
                .map_err(storage),
            JournalTransition::SetStatus { status, error } => {
                self.journal
                    .set_status(key, status, error.as_deref())
                    .await
                    .map_err(storage)?;
                Ok(TransitionResult::Applied)
            }
            JournalTransition::OperationArtifact {
                operation_id,
                invoice,
            } => {
                self.journal
                    .set_operation_artifact(key, operation_id, invoice.as_ref())
                    .await
                    .map_err(storage)?;
                Ok(TransitionResult::Applied)
            }
            JournalTransition::DriverFinished { .. } => Ok(TransitionResult::Applied),
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
            SnapshotScope::Reservations => self
                .project_reservations()
                .await
                .map(Snapshot::Reservations),
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

    async fn project_reservations(&self) -> ServiceResult<Reservations> {
        let intents = self.journal.reservation_intents().await.map_err(storage)?;
        let mut records = BTreeMap::new();
        for intent in &intents {
            if let Some(record) = self
                .journal
                .move_record(&intent.idempotency_key)
                .await
                .map_err(storage)?
            {
                records.insert(intent.idempotency_key.clone(), record);
            }
        }
        Ok(wallet_core::project_reservations(&intents, |key| {
            records.get(key).cloned()
        }))
    }

    async fn reconcile(&mut self, client: &WalletClient) -> ServiceResult<ReconcileReport> {
        let pending = self.journal.pending().await.map_err(storage)?;

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

        let mut report = ReconcileReport::default();
        for mut intent in pending {
            // A probe session is the durable owner of its legs. Once that session has
            // resolved (including evacuation preemption), an orphaned leg is stale and
            // must not be re-driven on a later recovery pass.
            let probe_session_nonce = if intent.reason == wallet_core::ReasonCode::ActiveProbe {
                let Some(nonce) = self.probe_leg_session_nonce(&intent).await? else {
                    self.journal
                        .set_status(
                            &intent.idempotency_key,
                            IntentStatus::Failed,
                            Some("probe session is no longer active"),
                        )
                        .await
                        .map_err(storage)?;
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
                    .set_status(&intent.idempotency_key, IntentStatus::Pending, None)
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

    async fn probe_leg_session_nonce(&self, intent: &Intent) -> ServiceResult<Option<String>> {
        let Action::Move {
            from,
            to,
            amount,
            fee_cap,
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
            driver::spawn_intent(
                &self.registry,
                generation,
                intent,
                self.journal.clone(),
                self.executor.clone(),
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
        client: &WalletClient,
    ) {
        let Some(finished) = driver::finish(&self.registry, key, generation) else {
            return;
        };
        let intent = match self.journal.get(key).await {
            Ok(intent) => intent,
            Err(error) => {
                tracing::warn!(key = %key.0, ?error, "DriverFinished: intent refresh failed");
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
        let hand_off_awaiting = intent.status == IntentStatus::Awaiting
            && (matches!(&finished.kind, driver::DriverKind::Intent { .. })
                || finished.redrive_requested);
        let honor_requested_redrive =
            intent.status == IntentStatus::Pending && finished.redrive_requested;
        if hand_off_awaiting || honor_requested_redrive {
            self.ensure_driver(intent, client, external_admission, probe_session_nonce);
        }
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

fn facts_from_probes(
    probes: &[(FederationId, crate::probe::ProbeResult)],
) -> BTreeMap<FederationId, Msat> {
    probes
        .iter()
        .map(|(id, probe)| (*id, Msat(probe.spendable_msat)))
        .collect()
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
        (
            Action::Move {
                from: old_from,
                to: old_to,
                amount: old_amount,
                fee_cap: old_fee,
            },
            Action::Move {
                from,
                to,
                amount,
                fee_cap,
            },
        )
        | (
            Action::Evacuate {
                from: old_from,
                to: old_to,
                amount: old_amount,
                fee_cap: old_fee,
            },
            Action::Evacuate {
                from,
                to,
                amount,
                fee_cap,
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
        JournalTransition::OperationArtifact { invoice, .. } => invoice.is_some(),
        JournalTransition::DriverFinished { .. } => true,
        JournalTransition::Upsert(_) => true,
        JournalTransition::SetStatus { .. } | JournalTransition::Refresh => true,
    }
}

pub(super) fn storage(error: ExecError) -> ServiceError {
    let message = match error {
        ExecError::Retryable(message) | ExecError::Permanent(message) => message,
        ExecError::Unsupported => "journal operation is unsupported".to_owned(),
    };
    ServiceError::Storage(message)
}

fn storage_refusal(error: ExecError) -> ServiceError {
    as_storage_refusal(storage(error))
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
