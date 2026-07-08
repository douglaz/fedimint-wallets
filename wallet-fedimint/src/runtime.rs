//! [`Runtime`] — the thin async façade the headless frontend drives (spec §9). It owns the
//! shared fedimint I/O (`MultiClient`) + durable journal (`FedimintJournal`) and exposes the
//! engine verbs `wallet-cli` needs on top of `wallet_core::{apply, reconcile}`:
//!
//! - [`Runtime::direct_inflow`] — journal + drive a `DirectInflow` intent (spec §7): the
//!   executor sizes + cap-checks the receive invoice (§6 fixed point), mints it, persists the
//!   `MoveRecord`, and returns `Awaiting`; we then surface the BOLT11 (the payer is external).
//! - [`Runtime::do_move`] — journal + drive a cross-federation `Move` (spec §7): B (`to`)
//!   receives, A (`from`) pays through the shared gateway's internal swap, both legs settle.
//!   Synchronous — `perform` runs the whole two-leg move to `Done` (never `Awaiting`).
//! - [`Runtime::await_move`] — finalize an `Awaiting` inflow: await its `recv_op`, and on the
//!   `Claimed` state mark the intent `Done` via the journal CAS (spec §9.5).
//! - [`Runtime::reconcile`] — the resume loop (spec §9): rebuild `MoveRecord`s from the op-log
//!   for pending + awaiting intents BEFORE re-driving, re-drive `pending()` only (so a `Move`
//!   left `Pending` by a transient fault is re-driven here), then report the still-`Awaiting`
//!   set (finalized out-of-band by `await-move` in a one-shot CLI).
//!
//! `Evacuate` now drives through the executor as a send-required move (Phase 3.A), so the tick
//! can flee a dying federation, not just top up a standby. The `Runtime` holds an optional pinned
//! gateway (⟦D4⟧; devimint's LDK gateway is not auto-registered, runbook §4) that a FRESH move
//! resolves through — a resumed move reuses the gateway already recorded in its `MoveRecord`.

use crate::discovery::{
    auto_join_kind, discover_kind, discovery_actor, run_discover_pass, AutoJoinCounts,
    CandidateSource, DiscoverReport, DiscoveryBackend, PreviewedCandidate, DISCOVERY_REASON,
};
use crate::executor::FedimintExecutor;
use crate::journal::{
    CandidateListReport, CandidateState, FedimintJournal, OperationRef, ProbeSession,
};
use crate::move_protocol::{MovePhase, MoveRecord};
use crate::multi_client::{MultiClient, ReceiveState};
use crate::probe::{assemble_facts, assemble_status, FedimintProbeRunner, ProbeResult};
use crate::tick::{
    build_snapshot, decisions_to_apply, pinned_input_problems, ScoredFed, StatusReport, TickPolicy,
    TickReport,
};
use crate::types::{GatewayUrl, Invoice};
use async_trait::async_trait;
use fedimint_core::config::ClientConfig;
use fedimint_core::encoding::{Decodable, DynRawFallback};
use fedimint_core::module::registry::ModuleDecoderRegistry;
use fedimint_core::runtime;
use fedimint_core::NumPeers;
use std::time::Duration;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use wallet_core::{
    probe_verdict, score, Action, ActiveProbeVerdict, Actor, AllocatorDecision, AllocatorSnapshot,
    DiscoveryPolicy, ExecError, ExecutionSummary, Executor, FederationFacts, FederationId,
    IdempotencyKey, Intent, IntentStatus, Journal, Module, Msat, Occurrence, OperationKind,
    OperationStatus, PerformOutcome, ProbeAttempt, ProbePolicy, ReasonCode, ScorerPolicy,
};

/// Wall-clock in unix millis for the ledger's `created_at_ms` (§8/§9.4). `seq` is the
/// ordering authority; this is display material, so a pre-epoch clock degrades to `0`
/// rather than failing a money op. The durable §9.4 injected clock is a later run's concern.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A fresh 128-bit nonce as 32 lowercase-hex chars for a per-attempt ledger key (§10.1 — a
/// 32-bit nonce risks birthday collisions over a wallet lifetime, aliasing two attempts onto
/// one `0x06` entry). The runtime owns randomness (the journal stays deterministic, §9.3);
/// this draws from fedimint's CSPRNG.
fn ledger_nonce() -> String {
    use std::fmt::Write as _;
    let bytes = fedimint_core::core::OperationId::new_random().0;
    let mut out = String::with_capacity(32);
    for byte in &bytes[..16] {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The result of a [`Runtime::direct_inflow`] call: the intent's key (the durable handle the
/// operator passes to `await-move`), the surfaced BOLT11 to pay (read from the persisted
/// `MoveRecord`, so a re-run returns the SAME invoice — no second mint), and the intent status.
#[derive(Clone, Debug)]
pub struct DirectInflowOutcome {
    pub key: IdempotencyKey,
    pub invoice: Option<Invoice>,
    pub status: Option<IntentStatus>,
}

/// The result of a [`Runtime::do_move`] call: the move intent's key (the durable handle), the
/// terminal intent status, and — when the move did not settle — the reason recorded on its
/// `MoveRecord`. A `Move` is synchronous (spec §7): `perform` drives both legs to `Done` (or
/// `Failed`), so unlike [`DirectInflowOutcome`] there is no invoice to surface and no external
/// payer to await. A `Pending` status means a transient fault left the move re-drivable via
/// `reconcile` (or a re-run of `move` with the same occurrence + `--gateway`).
#[derive(Clone, Debug)]
pub struct MoveOutcome {
    pub key: IdempotencyKey,
    pub status: Option<IntentStatus>,
    pub outcome: Option<String>,
}

/// The terminal result of [`Runtime::await_move`]: the inflow settled (`Done`) or did not
/// (`Failed`, carrying the reason). `await_move` blocks on the receive leg, so it never
/// returns while the intent is still merely `Awaiting`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalizeOutcome {
    Done,
    Failed(String),
}

/// Counts + keys from a [`Runtime::reconcile`] pass (spec §9). `performed`/`failed`/`skipped`
/// come from the `wallet_core::reconcile` re-drive of pending intents; `awaiting` is the set of
/// `DirectInflow` intents whose external payer has not settled — reported (not re-driven) so the
/// operator can `await-move` each. `retryable` is the §15.11 subset of `failed` that was left
/// `Pending` for a later retry (a transient timeout/transport fault), so a scheduler driving
/// `reconcile` in a loop can tell "will clear on a later pass" from a terminal `failed − retryable`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileSummary {
    pub performed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub retryable: usize,
    pub awaiting: usize,
    pub awaiting_keys: Vec<IdempotencyKey>,
}

#[derive(Clone, Debug)]
struct TickPlan {
    raw_probes: Vec<(FederationId, ProbeResult)>,
    probes: Vec<(FederationId, ProbeResult)>,
    active_probes: BTreeMap<FederationId, ActiveProbeVerdict>,
    snapshot: AllocatorSnapshot,
    decisions: Vec<AllocatorDecision>,
}

#[derive(Clone, Debug)]
struct EvacuationFallback {
    from: FederationId,
    plan: TickPlan,
}

struct MoveRouteProblem {
    from: FederationId,
    to: FederationId,
    /// The federation whose gateway is marked unavailable in the planning probe copy so
    /// `plan_tick` re-runs allocation onto a different route. This is ALWAYS the selected
    /// destination `to`: a destination that cannot receive is skipped directly, and a source
    /// leg that the destination-selected gateway cannot serve is retried against another
    /// eligible destination (an evacuation additionally captures a fallback plan first). There
    /// is no route problem that leaves the destination usable, so this is never absent.
    mark_unavailable: FederationId,
    gateway: Option<GatewayUrl>,
    error: String,
    evacuation_source_route: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SendRouteKind {
    Move,
    Evacuate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TerminalReplay {
    key: IdempotencyKey,
    status: IntentStatus,
}

/// Wraps an [`Executor`] so each `perform` is bounded by a wall-clock deadline (§15.9). A tick
/// blocks on `await_send`/`await_receive` (the SDK long-polls up to 60 min/request), so one
/// stalled gateway would otherwise freeze probing and every other decision. On timeout the perform
/// future is DROPPED — the move engine is crash-safe (a later reconcile rebuilds the record from
/// the op-log and reattaches, never re-minting/re-paying) — and the intent is left `Pending` via
/// the `Retryable` path, so the tick moves on and the summary counts it.
struct TimeoutExecutor<E> {
    inner: E,
    timeout: Option<Duration>,
}

impl<E> TimeoutExecutor<E> {
    fn new(inner: E, timeout: Option<Duration>) -> Self {
        Self { inner, timeout }
    }
}

#[async_trait]
impl<E: Executor> Executor for TimeoutExecutor<E> {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        match self.timeout {
            Some(deadline) => match runtime::timeout(deadline, self.inner.perform(intent)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(ExecError::Retryable(format!(
                    "perform exceeded the {}s deadline for intent {}; leaving it Pending for the \
                     next reconcile",
                    deadline.as_secs(),
                    intent.idempotency_key.0
                ))),
            },
            None => self.inner.perform(intent).await,
        }
    }
}

/// The engine façade over one wallet's shared fedimint clients + journal (spec §9).
pub struct Runtime {
    mc: Arc<MultiClient>,
    journal: Arc<FedimintJournal>,
    pinned_gateway: Option<GatewayUrl>,
    /// The hard per-fed balance cap enforced at perform time (§15.2), threaded into the executor.
    /// `None` disables it (the operator's `--allow-over-cap`). For a tick this is the policy's
    /// `per_fed_cap`; for an operator verb it is the ADR-0018 default unless overridden.
    hard_cap: Option<Msat>,
    /// Per-`perform` wall-clock deadline (§15.9). `None` disables the deadline.
    perform_timeout: Option<Duration>,
}

impl Runtime {
    pub fn new(
        mc: Arc<MultiClient>,
        journal: Arc<FedimintJournal>,
        pinned_gateway: Option<GatewayUrl>,
        hard_cap: Option<Msat>,
        perform_timeout: Option<Duration>,
    ) -> Self {
        Self {
            mc,
            journal,
            pinned_gateway,
            hard_cap,
            perform_timeout,
        }
    }

    /// A fresh executor sharing this runtime's clients + journal + pinned gateway + hard cap.
    /// Cheap (`Arc` clones); made per call so each verb gets a `&self`-only executor. Used
    /// DIRECTLY for the non-`perform` helper calls (`backfill_move_record` /
    /// `validate_direct_inflow_amount`); the `perform`-driving paths wrap it via
    /// [`Self::driving_executor`] to apply the tick deadline.
    fn executor(&self) -> FedimintExecutor {
        FedimintExecutor::new(
            self.mc.clone(),
            self.journal.clone(),
            self.pinned_gateway.clone(),
            self.hard_cap,
        )
    }

    /// The executor `wallet_core::apply`/`reconcile` drive, wrapped with the §15.9 per-`perform`
    /// deadline so one stalled gateway can never freeze the whole tick.
    fn driving_executor(&self) -> TimeoutExecutor<FedimintExecutor> {
        TimeoutExecutor::new(self.executor(), self.perform_timeout)
    }

    /// The BOLT11 surfaced for an intent (spec §7's `invoice_for`): read the persisted
    /// `MoveRecord.invoice`. `None` before the invoice is minted (or for a non-move intent).
    pub async fn invoice_for(&self, key: &IdempotencyKey) -> Result<Option<Invoice>, ExecError> {
        Ok(self
            .journal
            .get_move(key)
            .await?
            .and_then(|rec| rec.invoice))
    }

    /// Route an inflow to `to` netting EXACTLY `amount` (spec §6/§7). Builds a `DirectInflow`
    /// decision under a deterministic key and drives it through `wallet_core::apply`: `perform`
    /// sizes + cap-checks + mints the receive invoice, persists the `MoveRecord`, and returns
    /// `Awaiting` (the payer is external). Idempotent on the key — a re-run of the same
    /// (`to`, `amount`, `fee_cap`, `occurrence`) finds the `Awaiting` intent and SKIPS the drive
    /// (no second invoice), while we still surface the already-minted invoice from the journal.
    pub async fn direct_inflow(
        &self,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        occurrence: Occurrence,
    ) -> anyhow::Result<DirectInflowOutcome> {
        let key = direct_inflow_key(&to, amount, fee_cap, occurrence);
        if self.journal.get(&key).await.map_err(exec_err)?.is_none() {
            // The preflight exists to catch DETERMINISTIC rejections (lnv2 dust) before an
            // intent is journaled. A RETRYABLE failure here (e.g. the never-over quote loop
            // not settling this instant) must NOT hard-fail the command pre-journal — there
            // would be no pending intent for `reconcile`/a same-occurrence re-run to
            // re-drive. Proceed to journal + drive instead: `perform` re-quotes from
            // scratch, and if the quotes are still unstable it leaves the intent `Pending`
            // for the re-drive paths, which is the documented behavior.
            match self
                .executor()
                .validate_direct_inflow_amount(to, amount)
                .await
            {
                Ok(()) => {}
                Err(ExecError::Retryable(reason)) => tracing::warn!(
                    %reason,
                    "direct-inflow preflight retryable; journaling the intent and driving anyway"
                ),
                Err(e) => return Err(exec_err(e)),
            }
        }
        let decision = AllocatorDecision {
            action: Action::DirectInflow {
                to,
                amount,
                fee_cap,
            },
            // A plain operator verb (§8): the ledger records it as user-initiated.
            reason: ReasonCode::UserInitiated,
            occurrence,
            idempotency_key: key.clone(),
        };
        let executor = self.driving_executor();
        let _summary = wallet_core::apply(
            self.journal.as_ref(),
            &executor,
            std::slice::from_ref(&decision),
            Actor::User,
            now_ms(),
        )
        .await;

        // Read the intent + its derived record together so we can complete a transition that a
        // crash in `await_move` interrupted (spec §9.5): if `settle_move` wrote a terminal record
        // phase but the process died before the intent CAS landed, the intent is stuck Awaiting
        // over already-final receive state. Finish that transition here before reporting status.
        let mut status = self
            .journal
            .get(&key)
            .await
            .map_err(exec_err)?
            .map(|i| i.status);
        let record = self.journal.get_move(&key).await.map_err(exec_err)?;
        if status == Some(IntentStatus::Awaiting) {
            match record.as_ref().map(|rec| rec.phase) {
                Some(MovePhase::Settled) => {
                    self.finalize(&key, IntentStatus::Done).await?;
                    status = Some(IntentStatus::Done);
                }
                Some(MovePhase::Failed) => {
                    self.finalize(&key, IntentStatus::Failed).await?;
                    status = Some(IntentStatus::Failed);
                }
                _ => {}
            }
        }
        let invoice = record.and_then(|rec| rec.invoice);
        Ok(DirectInflowOutcome {
            key,
            invoice,
            status,
        })
    }

    /// Transfer `amount` net ecash from federation `from` to `to` through the shared gateway's
    /// internal swap (spec §7): B (`to`) receives, A (`from`) pays, both legs settle. Builds a
    /// `Move` decision under a deterministic key and drives it through `wallet_core::apply`;
    /// `perform` runs the WHOLE two-leg move to completion (it is synchronous — it returns
    /// `Done` when settled, never `Awaiting`), so this returns once the move is terminal.
    ///
    /// Idempotent on the key: a re-run of the same (`from`, `to`, `amount`, `fee_cap`,
    /// `occurrence`) reattaches to the in-flight/settled move (backfill + the lnv2 send dedup)
    /// and never re-mints or re-pays. A transient fault leaves the intent `Pending` (re-drivable
    /// by `reconcile` or a same-occurrence re-run with `--gateway`); a `Permanent` fault (fee
    /// over cap, refund/failed settlement) leaves it `Failed`, its reason on the `MoveRecord`.
    ///
    /// `reason`/`actor` are the ledger provenance (§8 / phase 5 §5.0.5): the CLI `move` verb
    /// passes `UserInitiated`/`User`; [`Self::active_probe`] threads `ActiveProbe` plus its
    /// caller's actor so both probe legs are explained in `history`.
    #[allow(clippy::too_many_arguments)]
    pub async fn do_move(
        &self,
        from: FederationId,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        occurrence: Occurrence,
        reason: ReasonCode,
        actor: Actor,
    ) -> anyhow::Result<MoveOutcome> {
        let key = move_key(&from, &to, amount, fee_cap, occurrence);
        let decision = AllocatorDecision {
            action: Action::Move {
                from,
                to,
                amount,
                fee_cap,
            },
            reason,
            occurrence,
            idempotency_key: key.clone(),
        };
        let executor = self.driving_executor();
        let _summary = wallet_core::apply(
            self.journal.as_ref(),
            &executor,
            std::slice::from_ref(&decision),
            actor,
            now_ms(),
        )
        .await;

        let status = self
            .journal
            .get(&key)
            .await
            .map_err(exec_err)?
            .map(|i| i.status);
        let outcome = self
            .journal
            .get_move(&key)
            .await
            .map_err(exec_err)?
            .and_then(|rec| rec.outcome);
        Ok(MoveOutcome {
            key,
            status,
            outcome,
        })
    }

    /// Run one discovery pass (§5.1.2): union source announcements, authenticate configs without
    /// joining, write candidate rows, and optionally auto-join within the configured caps.
    pub async fn discover(
        &self,
        sources: Vec<Box<dyn CandidateSource>>,
        policy: DiscoveryPolicy,
    ) -> anyhow::Result<DiscoverReport> {
        let nonce = ledger_nonce();
        run_discover_pass(&sources, &policy, self, now_ms(), &nonce).await
    }

    /// Run ONE active probe of `candidate` from spending federation `from` (phase 5
    /// §5.0.5): a two-leg, exact-net round trip on the real money path — leg IN mints
    /// `policy.amount_msat` on the candidate through the ordinary `Move` machinery, leg
    /// OUT redeems the affordably-sized delta back, and the finished attempt lands in the
    /// durable `0x08` history the pure [`probe_verdict`] evaluates.
    ///
    /// Ok = an ATTEMPT was recorded (a clean pass, or a demoting candidate-fault
    /// failure). Every other exit is an error: the no-attempt terminal exits (preflight/
    /// local/no-route/inconclusive — umbrella row `Failed`, session cleared, no demotion)
    /// and the transient still-pending legs (session RETAINED; a re-run of `probe`
    /// resumes it — step 0 below).
    pub async fn active_probe(
        &self,
        candidate: FederationId,
        from: FederationId,
        policy: &ProbePolicy,
        actor: Actor,
    ) -> anyhow::Result<ProbeReport> {
        let record = self
            .journal
            .probe_record(&candidate)
            .await
            .map_err(exec_err)?;
        let attempts_before = record
            .as_ref()
            .map(|r| r.attempts.clone())
            .unwrap_or_default();

        // §5.0.5 step 0: resume FIRST — an in-flight session owns this invocation (its
        // parameters, including `from`, are fixed); the fresh path below must not run.
        let (mut session, resuming) = match record.and_then(|r| r.in_flight) {
            Some(session) => {
                if session.from != from {
                    tracing::warn!(
                        session_from = %session.from.to_hex(),
                        requested_from = %from.to_hex(),
                        "probe: resuming the in-flight session; its recorded source wins"
                    );
                }
                (session, true)
            }
            None => {
                // Fresh probe: sample the no-sweep BASELINE before anything else. An
                // unopened candidate reads 0 — safe, because the preflight below refuses
                // it before any money path (leg OUT, the only baseline consumer, is
                // unreachable); an OPEN candidate whose read fails bails here, pre-session
                // (nothing durable written yet), rather than record a too-low baseline
                // that would weaken the §5.0.4 guard.
                let baseline = if self.mc.federations().contains(&candidate) {
                    self.mc
                        .balance(&candidate)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("probe: sampling the candidate baseline failed: {e}")
                        })?
                        .0
                } else {
                    0
                };
                let session = ProbeSession {
                    nonce: ledger_nonce(),
                    from,
                    amount_msat: policy.amount_msat,
                    leg_fee_cap_msat: policy.leg_fee_cap_msat,
                    c_spendable_before_in_msat: baseline,
                    out_net_msat: None,
                    started_at_ms: now_ms(),
                };
                self.journal
                    .begin_probe_session(&candidate, &session)
                    .await
                    .map_err(exec_err)?;
                (session, false)
            }
        };
        let occurrence = occurrence_from_nonce(&session.nonce)?;
        let amount = Msat(session.amount_msat);
        let leg_fee_cap = Msat(session.leg_fee_cap_msat);
        // The MONEY params are the SESSION's, not the caller's flags: a resume runs the
        // legs with the stored amount/fee_cap, so the verdict must qualify the resulting
        // attempt against those same values — otherwise an operator changing `--amount` on
        // resume would judge the just-spent attempt against thresholds it was never run
        // with (flipping it qualifying/non-qualifying). On a FRESH probe the session was
        // built FROM these same flags, so this is a no-op there. The verdict-WINDOW fields
        // (min_successes/span/ttl) stay the caller's.
        let effective_policy = ProbePolicy {
            amount_msat: session.amount_msat,
            leg_fee_cap_msat: session.leg_fee_cap_msat,
            ..policy.clone()
        };
        let run = ProbeRun {
            candidate,
            source: session.from,
            actor,
            verdict_before: probe_verdict(
                &attempts_before,
                session.from,
                now_ms(),
                &effective_policy,
            ),
            nonce: session.nonce.clone(),
            umbrella_key: probe_umbrella_key(&candidate, &session.nonce),
            amount,
            leg_fee_cap,
            in_key: move_key(&session.from, &candidate, amount, leg_fee_cap, occurrence),
            effective_policy,
            started_at_ms: session.started_at_ms,
        };

        // §5.0.5 step 1 — umbrella row then preflight, for a FRESH probe or a pre-leg-IN
        // resume ONLY (both re-enter here; §5.0.4's disambiguation): once leg IN is
        // journaled money may have moved, so fresh-probe balance/cap checks no longer hold
        // and would misclassify a recoverable probe as a new local error.
        let leg_in_journaled = self
            .journal
            .get(&run.in_key)
            .await
            .map_err(exec_err)?
            .is_some();
        if session.out_net_msat.is_none() && !leg_in_journaled {
            if resuming {
                tracing::info!(
                    candidate = %candidate.to_hex(),
                    "probe: resuming a pre-leg-IN session; re-running the preflight"
                );
            }
            // Session-first, umbrella second (§5.0.5): `record_started` is idempotent, so
            // a pre-umbrella resume recreates the row here; recording must succeed before
            // any money moves (the phase-4 auditability contract).
            self.journal
                .record_started(
                    &run.umbrella_key,
                    probe_kind(&run, None),
                    actor,
                    ReasonCode::ActiveProbe,
                    now_ms(),
                    None,
                )
                .await
                .map_err(exec_err)?;
            if let Err(diagnostic) = self.probe_preflight(&session, candidate).await {
                return self.finish_probe_no_attempt(&run, &diagnostic, None).await;
            }
        }

        // Re-sample the no-sweep BASELINE immediately before leg IN — after the slow
        // preflight/route validation (gateway HTTP), during which a candidate-side receive
        // state machine could settle asynchronously and change the balance. Sampling here
        // (vs. pre-preflight) folds any such settlement into the pre-existing baseline, so
        // the exact-match resume guard isolates the probe delta precisely instead of
        // false-aborting a valid resume as "delta consumed" (a safe-direction failure, but
        // avoidable). ONLY before leg IN credits the candidate (`!leg_in_journaled`); a
        // post-IN resume keeps its recorded baseline. Best-effort: a read failure keeps the
        // early baseline (already durable), and a same-nonce write only fires on a change.
        if !leg_in_journaled && self.mc.federations().contains(&candidate) {
            if let Ok(fresh) = self.mc.balance(&candidate).await {
                if fresh.0 != session.c_spendable_before_in_msat {
                    session.c_spendable_before_in_msat = fresh.0;
                    self.journal
                        .begin_probe_session(&candidate, &session)
                        .await
                        .map_err(exec_err)?;
                }
            }
        }

        // §5.0.5 step 3 — leg IN (journals the intent; a resume reattaches idempotently).
        let in_outcome = self
            .do_move(
                run.source,
                candidate,
                run.amount,
                run.leg_fee_cap,
                occurrence,
                ReasonCode::ActiveProbe,
                actor,
            )
            .await?;
        match in_outcome.status {
            Some(IntentStatus::Done) => {}
            Some(IntentStatus::Failed) => {
                return self
                    .finish_probe_failed_leg(&run, ProbeLeg::In, &run.in_key, None, None)
                    .await;
            }
            other => anyhow::bail!(
                "probe leg IN {} did not settle (status {}); transient — re-run `probe` to \
                 resume (session retained)",
                run.in_key.0,
                intent_status_label_opt(other)
            ),
        }
        let in_rec = self
            .journal
            .get_move(&run.in_key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| {
                anyhow::anyhow!("probe leg IN settled but its move record is missing")
            })?;
        // Leg IN's DELIVERED net (possibly a verified hair under the ask) — durable on the
        // move record, so the sizing budget survives a crash.
        let delivered_in = in_rec.amount;

        // §5.0.5 steps 4-5 — size leg OUT with budget = the delivered net, persist the
        // sized amount BEFORE journaling leg OUT (a resume never re-sizes).
        let out_net = match session.out_net_msat {
            Some(persisted) => Msat(persisted),
            None => {
                // Size leg OUT against a budget REDUCED by a fee-jitter margin. The final
                // fee cap (`probe_out_fee_cap`) is bounded by the FULL delivered_in for
                // no-sweep, so sizing out_net a margin smaller leaves that cap headroom
                // above the sizing-time fee ESTIMATE — absorbing the small upward re-quote
                // the Pay step can produce (observed live: an 8432-msat actual vs an
                // 8417-msat estimate deferred the whole probe). The margin becomes bounded
                // extra RESIDUE on the candidate (accepted, §5.0.9 decision 6); it stays
                // well under the leg fee cap, so the "residue < fee cap" invariant holds.
                let sizing_budget = Msat(delivered_in.0.saturating_sub(PROBE_FEE_MARGIN_MSAT));
                match self
                    .executor()
                    .size_probe_leg_out(candidate, run.source, sizing_budget, run.leg_fee_cap)
                    .await
                {
                    Ok(Some(sized)) => {
                        // First `out_net_msat` fill. Two callers racing this window (both
                        // re-sizing, both journaling a leg OUT against the same delta) is a
                        // CONCURRENCY hazard the wallet's SINGLE-WRITER architecture forecloses
                        // in v1: the RocksDB store is opened under an exclusive `db.lock` (a
                        // second process blocks at open) and the probe verb runs synchronously.
                        // The crash-then-resume case is sequential (a dead process holds no
                        // lock; the resume is the only live writer and journals ONE leg). This
                        // is the SAME concurrency precondition §5.0.1's no-sweep isolation rests
                        // on — Phase 6's long-running app must revisit the whole probe under a
                        // per-probe reservation, not a lone CAS here (which would be false
                        // safety while the balance sampling + no-sweep guard share the exposure).
                        session.out_net_msat = Some(sized.0);
                        self.journal
                            .begin_probe_session(&candidate, &session)
                            .await
                            .map_err(exec_err)?;
                        sized
                    }
                    Ok(None) => {
                        // The post-IN feasibility abort: a LOCAL parameter/fee-environment
                        // error, NOT a redeemability failure (§5.0.5 step 4).
                        let diagnostic = format!(
                            "probe leg OUT infeasible: the delivered {} msat cannot afford any \
                             redeem whose contract clears the lnv2 minimum within the {} msat \
                             leg fee cap (shortfall is parametric, not a redeemability failure)",
                            delivered_in.0, run.leg_fee_cap.0
                        );
                        return self
                            .finish_probe_no_attempt(
                                &run,
                                &diagnostic,
                                probe_cost(Some(&in_rec), None),
                            )
                            .await;
                    }
                    Err(e) => anyhow::bail!(
                        "probe leg OUT sizing failed transiently ({e:?}); re-run `probe` to \
                         resume (session retained)"
                    ),
                }
            }
        };
        let out_fee_cap = probe_out_fee_cap(delivered_in, out_net, run.leg_fee_cap);
        let out_key = move_key(&candidate, &run.source, out_net, out_fee_cap, occurrence);

        // §5.0.4 no-sweep guard on the not-yet-journaled window (trivially true on the
        // fresh path; load-bearing on a sized-but-unjournaled resume): leg OUT may start
        // only while the candidate still holds baseline + delta. Once the out intent is
        // journaled the money path owns it like any other move — no guard before DRIVING.
        if self
            .journal
            .get(&out_key)
            .await
            .map_err(exec_err)?
            .is_none()
        {
            let c_spendable = self.mc.balance(&candidate).await.map_err(|e| {
                anyhow::anyhow!(
                    "probe: reading the candidate balance for the no-sweep check failed \
                     transiently ({e}); re-run `probe` to resume (session retained)"
                )
            })?;
            if !no_sweep_ok(
                c_spendable,
                Msat(session.c_spendable_before_in_msat),
                delivered_in,
            ) {
                let diagnostic = "probe delta consumed before redemption; inconclusive";
                return self
                    .finish_probe_no_attempt(&run, diagnostic, probe_cost(Some(&in_rec), None))
                    .await;
            }
            // Re-check the SOURCE cap on resume too (the fresh preflight's check is stale
            // once a resume can span an inflow): if `from` drifted above the cap between the
            // legs, `do_move(candidate -> from)` would deterministically fail ADR-0018 after
            // leg IN already spent — the same guaranteed inconclusive spend the fresh
            // preflight prevents. Abort umbrella-only BEFORE the doomed return move.
            if let Some(cap) = self.hard_cap {
                let src_spendable = self.mc.balance(&run.source).await.map_err(|e| {
                    anyhow::anyhow!(
                        "probe: reading the source balance for the resume cap check failed                          transiently ({e}); re-run `probe` to resume (session retained)"
                    )
                })?;
                if src_spendable.0 > cap.0 {
                    let diagnostic =
                        "probe source rose above the per-fed cap between legs; inconclusive";
                    return self
                        .finish_probe_no_attempt(&run, diagnostic, probe_cost(Some(&in_rec), None))
                        .await;
                }
            }
        }

        // Leg OUT — sized exactly, same nonce-derived occurrence.
        let out_outcome = self
            .do_move(
                candidate,
                run.source,
                out_net,
                out_fee_cap,
                occurrence,
                ReasonCode::ActiveProbe,
                actor,
            )
            .await?;
        match out_outcome.status {
            Some(IntentStatus::Done) => {}
            Some(IntentStatus::Failed) => {
                return self
                    .finish_probe_failed_leg(
                        &run,
                        ProbeLeg::Out,
                        &out_key,
                        Some(&in_rec),
                        Some(out_key.clone()),
                    )
                    .await;
            }
            other => anyhow::bail!(
                "probe leg OUT {} did not settle (status {}); transient — re-run `probe` to \
                 resume (session retained)",
                out_key.0,
                intent_status_label_opt(other)
            ),
        }

        // §5.0.5 step 6 — both legs settled: ONE atomic outcome write (attempt appended,
        // session cleared, umbrella row Succeeded with the S-net-outflow cost).
        // Fail closed on a missing out record (as leg IN does): `Done` proves leg OUT
        // settled, but a cache-loss recovery could leave `get_move` empty, and recording
        // `cost = full debit` (credit 0) would persist a successful probe in history as if
        // NONE of the funds came back. Deferring (session retained) lets a re-run rebuild
        // the record and record the true S-net-outflow cost.
        let out_rec = self
            .journal
            .get_move(&out_key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "probe leg OUT settled but its move record is missing; transient — re-run \
                 `probe` to resume (session retained)"
                )
            })?;
        let cost = probe_cost(Some(&in_rec), Some(&out_rec));
        let attempt = ProbeAttempt {
            at_ms: run.started_at_ms,
            ok: true,
            from: run.source,
            amount_msat: run.amount.0,
            leg_fee_cap_msat: run.leg_fee_cap.0,
            error: None,
        };
        let committed = self
            .journal
            .record_probe_outcome(
                &candidate,
                &run.nonce,
                Some(attempt.clone()),
                &run.umbrella_key,
                probe_kind(&run, cost),
                actor,
                OperationStatus::Succeeded,
                None,
            )
            .await
            .map_err(exec_err)?;
        Self::note_probe_commit(committed, &run.nonce);
        let after = self.probe_attempts(&candidate).await?;
        Ok(ProbeReport {
            verdict_before: run.verdict_before,
            outcome: ProbeOutcome::Attempt(attempt),
            verdict_after: probe_verdict(&after, run.source, now_ms(), &run.effective_policy),
            in_key: run.in_key,
            out_key: Some(out_key),
        })
    }

    /// The §5.0.5 step-1 preflight for a fresh (or pre-leg-IN resumed) probe. `Err`
    /// carries the LOCAL / no-shared-route diagnostic that terminalizes the umbrella row
    /// with NO attempt (neither demotes — §5.0.3's scoping rule).
    async fn probe_preflight(
        &self,
        session: &ProbeSession,
        candidate: FederationId,
    ) -> Result<(), String> {
        let open = self.mc.federations();
        if !open.contains(&candidate) {
            return Err(format!(
                "candidate federation {} is not joined/open",
                candidate.to_hex()
            ));
        }
        if !open.contains(&session.from) {
            return Err(format!(
                "source federation {} is not joined/open",
                session.from.to_hex()
            ));
        }
        let source_spendable = self
            .mc
            .balance(&session.from)
            .await
            .map_err(|e| format!("reading the source balance failed: {e}"))?;
        let candidate_spendable = self
            .mc
            .balance(&candidate)
            .await
            .map_err(|e| format!("reading the candidate balance failed: {e}"))?;
        probe_local_faults(
            candidate,
            session.from,
            source_spendable,
            candidate_spendable,
            Msat(session.amount_msat),
            Msat(session.leg_fee_cap_msat),
            self.hard_cap,
        )?;
        // The existing move-route preflight in BOTH directions (§15.6): leg IN proves
        // S -> C and leg OUT must be known routable before money lands on C. The
        // verbatim route error is the umbrella diagnostic — pair reachability, never
        // candidate honesty.
        self.validate_executor_move_route(SendRouteKind::Move, session.from, candidate)
            .await
            .map_err(|problem| problem.error)?;
        self.validate_executor_move_route(SendRouteKind::Move, candidate, session.from)
            .await
            .map_err(|problem| problem.error)
    }

    /// Terminalize a probe with NO attempt (§5.0.5's local/route/inconclusive exits):
    /// session cleared + umbrella row `Failed` in one dbtx, verdict history untouched.
    /// Note a probe finalizer that lost to a stale-nonce guard in `record_probe_outcome`
    /// (its `false` return). Under single-writer v1 (exclusive `db.lock` + the synchronous
    /// verb) two finalizers for one session cannot race, so `committed` is always true; the
    /// `debug_assert` pins that invariant for tests/dev, and the release warn flags the
    /// Phase-6 concurrency case (where the returned report could disagree with history)
    /// instead of silently discarding the signal.
    fn note_probe_commit(committed: bool, nonce: &str) {
        if !committed {
            tracing::warn!(
                nonce,
                "probe: stale finalizer — durable history holds a different outcome for this \
                 session; the returned report may not match it (a concurrency case \
                 single-writer v1 forecloses; Phase-6 revisit)"
            );
        }
        debug_assert!(
            committed,
            "stale probe finalizer for {nonce} (unreachable under single-writer v1)"
        );
    }

    async fn finish_probe_no_attempt(
        &self,
        run: &ProbeRun,
        diagnostic: &str,
        cost: Option<Msat>,
    ) -> anyhow::Result<ProbeReport> {
        let committed = self
            .journal
            .record_probe_outcome(
                &run.candidate,
                &run.nonce,
                None,
                &run.umbrella_key,
                probe_kind(run, cost),
                run.actor,
                OperationStatus::Failed,
                Some(diagnostic),
            )
            .await
            .map_err(exec_err)?;
        Self::note_probe_commit(committed, &run.nonce);
        // No attempt was recorded, so the trust verdict is unchanged from the run's start.
        Ok(ProbeReport {
            verdict_before: run.verdict_before,
            outcome: ProbeOutcome::NoAttempt(diagnostic.to_string()),
            verdict_after: run.verdict_before,
            in_key: run.in_key.clone(),
            out_key: None,
        })
    }

    /// Terminalize a probe whose leg FAILED (§5.0.3's fault attribution): a
    /// candidate-attributable failure records a DEMOTING attempt and returns the report;
    /// source/gateway/ambiguous/local faults record an umbrella-only outcome (no attempt,
    /// no demotion) and surface as an error.
    async fn finish_probe_failed_leg(
        &self,
        run: &ProbeRun,
        leg: ProbeLeg,
        leg_key: &IdempotencyKey,
        in_rec: Option<&MoveRecord>,
        out_key: Option<IdempotencyKey>,
    ) -> anyhow::Result<ProbeReport> {
        let (leg_rec, diagnostic) = self.leg_failure_details(leg_key).await.map_err(|e| {
            anyhow::anyhow!(
                "probe leg {} {} failed, but reading its diagnostic failed ({e:?}); \
                 re-run `probe` to resume (session retained)",
                leg.label(),
                leg_key.0
            )
        })?;
        let error_text = format!("probe leg {} failed: {diagnostic}", leg.label());
        let cost = match leg {
            ProbeLeg::In => probe_cost(leg_rec.as_ref(), None),
            ProbeLeg::Out => probe_cost(in_rec, leg_rec.as_ref()),
        };
        match classify_leg_failure(leg, leg_rec.as_ref(), &diagnostic) {
            LegFault::Candidate => {
                let attempt = ProbeAttempt {
                    at_ms: run.started_at_ms,
                    ok: false,
                    from: run.source,
                    amount_msat: run.amount.0,
                    leg_fee_cap_msat: run.leg_fee_cap.0,
                    error: Some(error_text.clone()),
                };
                let committed = self
                    .journal
                    .record_probe_outcome(
                        &run.candidate,
                        &run.nonce,
                        Some(attempt.clone()),
                        &run.umbrella_key,
                        probe_kind(run, cost),
                        run.actor,
                        OperationStatus::Failed,
                        Some(&error_text),
                    )
                    .await
                    .map_err(exec_err)?;
                Self::note_probe_commit(committed, &run.nonce);
                let after = self.probe_attempts(&run.candidate).await?;
                Ok(ProbeReport {
                    verdict_before: run.verdict_before,
                    outcome: ProbeOutcome::Attempt(attempt),
                    verdict_after: probe_verdict(
                        &after,
                        run.source,
                        now_ms(),
                        &run.effective_policy,
                    ),
                    in_key: run.in_key.clone(),
                    out_key,
                })
            }
            LegFault::UmbrellaOnly => {
                // Preserve the failed out leg's handle on the report (finish_probe_no_attempt
                // defaults it None for the pre-leg-OUT refusals): when leg OUT itself failed,
                // its move exists and `out_key` is the operator's direct handle to inspect it.
                let mut report = self.finish_probe_no_attempt(run, &error_text, cost).await?;
                report.out_key = out_key;
                Ok(report)
            }
        }
    }

    /// A failed leg's `(move record, diagnostic)`: the record's terminal `outcome` first,
    /// else the ledger row's `error` (the §8.3 threaded executor diagnostic — several
    /// permanent failures never reach a terminal `MoveRecord.outcome`).
    async fn leg_failure_details(
        &self,
        key: &IdempotencyKey,
    ) -> Result<(Option<MoveRecord>, String), ExecError> {
        let rec = self.journal.get_move(key).await?;
        if let Some(outcome) = rec.as_ref().and_then(|r| r.outcome.clone()) {
            return Ok((rec, outcome));
        }
        let ledger_error = self
            .journal
            .operation(&OperationRef::Key(key.clone()))
            .await?
            .and_then(|row| row.error);
        Ok((
            rec,
            ledger_error.unwrap_or_else(|| "move failed with no recorded diagnostic".to_string()),
        ))
    }

    /// The fed's retained probe attempts (empty when never probed).
    async fn probe_attempts(&self, fed: &FederationId) -> anyhow::Result<Vec<ProbeAttempt>> {
        Ok(self
            .journal
            .probe_record(fed)
            .await
            .map_err(exec_err)?
            .map(|r| r.attempts)
            .unwrap_or_default())
    }

    /// Finalize an `Awaiting` `DirectInflow` (spec §9.5): reattach to its `recv_op` (rebuilt
    /// from the op-log so a lost cache still finds it), await the receive leg, and on `Claimed`
    /// mark the intent `Done` via the journal CAS. Blocks until the receive reaches a final
    /// state. Idempotent: an already-`Done` intent returns `Done` without re-awaiting.
    ///
    /// `expected_fed`, when supplied, guards against finalizing the wrong federation's inflow;
    /// the destination is otherwise read authoritatively from the intent's `MoveRecord`.
    pub async fn await_move(
        &self,
        key: &IdempotencyKey,
        expected_fed: Option<FederationId>,
    ) -> anyhow::Result<FinalizeOutcome> {
        let intent = self
            .journal
            .get(key)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| anyhow::anyhow!("no intent found for key {}", key.0))?;
        match intent.status {
            IntentStatus::Done => {
                if let Some(fed) = expected_fed {
                    let rec = self.move_record_for_guard(&intent).await?;
                    ensure_expected_fed(key, &rec, fed)?;
                }
                return Ok(FinalizeOutcome::Done);
            }
            IntentStatus::Failed => {
                let rec = if expected_fed.is_some() {
                    Some(self.move_record_for_guard(&intent).await?)
                } else {
                    self.journal.get_move(key).await.map_err(exec_err)?
                };
                if let (Some(fed), Some(rec)) = (expected_fed, rec.as_ref()) {
                    ensure_expected_fed(key, rec, fed)?;
                }
                return Ok(FinalizeOutcome::Failed(
                    rec.and_then(|rec| rec.outcome)
                        .unwrap_or_else(|| format!("intent {} already failed", key.0)),
                ));
            }
            IntentStatus::Awaiting => {}
            other @ (IntentStatus::Pending | IntentStatus::Executing) => anyhow::bail!(
                "intent {} is {other:?}, not awaiting — run `direct-inflow`/`reconcile` first",
                key.0
            ),
        }

        // Rebuild the record from the op-log so we reattach to the existing recv_op even if the
        // MoveRecord cache was lost (spec §9.2), then await the external payer's payment.
        let executor = self.executor();
        let rec = executor
            .backfill_move_record(&intent)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| anyhow::anyhow!("intent {} is not an executable move", key.0))?;
        if let Some(fed) = expected_fed {
            ensure_expected_fed(key, &rec, fed)?;
        }
        let recv_op = rec.recv_op.ok_or_else(|| {
            anyhow::anyhow!("awaiting intent {} has no receive op to finalize", key.0)
        })?;

        let outcome = match self.mc.await_receive(&rec.to, recv_op).await? {
            ReceiveState::Claimed => {
                self.settle_move(&rec, MovePhase::Settled, None).await?;
                self.finalize(key, IntentStatus::Done).await?;
                FinalizeOutcome::Done
            }
            ReceiveState::Expired => {
                let msg = "receive invoice expired before payment".to_string();
                self.settle_move(&rec, MovePhase::Failed, Some(msg.clone()))
                    .await?;
                self.finalize(key, IntentStatus::Failed).await?;
                FinalizeOutcome::Failed(msg)
            }
            ReceiveState::Failed(msg) => {
                self.settle_move(&rec, MovePhase::Failed, Some(msg.clone()))
                    .await?;
                self.finalize(key, IntentStatus::Failed).await?;
                FinalizeOutcome::Failed(msg)
            }
        };
        Ok(outcome)
    }

    /// The resume loop (spec §9): rebuild `MoveRecord`s from the op-log for pending + awaiting
    /// intents BEFORE re-driving (so a re-drive of an intent that crashed mid-receive reattaches
    /// to its op instead of minting a second invoice), re-drive `pending()` (Pending|Executing)
    /// ONLY via `wallet_core::reconcile`, then report the still-`Awaiting` set — subscription-
    /// owned, finalized out-of-band by `await-move` in this one-shot CLI.
    ///
    /// The clients are assumed already opened by the caller (the CLI runs `open_all` at startup,
    /// satisfying §9.1); `reconcile` operates on the open set.
    pub async fn reconcile(&self) -> anyhow::Result<ReconcileSummary> {
        let executor = self.executor();

        // §9.2: rebuild the derived records for every intent we might re-drive or finalize.
        let mut backfill_set = self.journal.pending().await;
        backfill_set.extend(self.journal.awaiting().await.map_err(exec_err)?);
        for intent in &backfill_set {
            if let Err(e) = executor.backfill_move_record(intent).await {
                tracing::warn!(
                    key = %intent.idempotency_key.0,
                    error = ?e,
                    "reconcile: could not rebuild move record; leaving it for a later pass"
                );
            }
        }

        // §9.4: re-drive pending() only; Failed/Permanent stay terminal, Awaiting is skipped.
        // Wrap the drive with the §15.9 per-perform deadline (the backfill above uses the raw
        // executor, since it makes no `perform` call).
        let driving = self.driving_executor();
        let exec = wallet_core::reconcile(self.journal.as_ref(), &driving).await;

        // §10.3: repair stuck non-terminal ledger rows (raw pay/recv, join, tick) from op-log +
        // registry evidence. Best-effort — a repair I/O fault must not fail the whole reconcile
        // (the intent re-drive above already committed its own money-path progress).
        match self.journal.repair_ledger(self.mc.as_ref()).await {
            Ok(summary) if summary.repaired > 0 => {
                tracing::info!(
                    repaired = summary.repaired,
                    "reconcile: repaired stuck ledger rows"
                )
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(
                error = ?e,
                "reconcile: ledger repair pass failed; leaving rows for a later pass"
            ),
        }

        // §9.3: surface the Awaiting set so the operator drives `await-move` for each.
        let awaiting = self.journal.awaiting().await.map_err(exec_err)?;
        Ok(ReconcileSummary {
            performed: exec.performed,
            failed: exec.failed,
            skipped: exec.skipped,
            retryable: exec.retryable,
            awaiting: awaiting.len(),
            awaiting_keys: awaiting
                .into_iter()
                .map(|intent| intent.idempotency_key)
                .collect(),
        })
    }

    /// ONE orchestrator tick (Phase 2 step 2.2, `docs/phase2-plan.md`): probe every open
    /// federation → build the `AllocatorSnapshot` (via `build_snapshot` — `score()` +
    /// designation) → `decide()` → `wallet_core::apply` the decisions through the
    /// [`FedimintExecutor`], which performs the resulting `Move`s AND `Evacuate`s (each a
    /// send-required move, synchronous to `Done`). Advisory `RefuseInflow` decisions are
    /// surfaced in the returned [`TickReport`] but never executed (`apply` skips them via
    /// `Action::is_executable`). As of Phase 3.A an `Evacuate` is executed like a `Move`
    /// (draining a dying fed into `safest_other`), no longer withheld from `apply`.
    /// Returns the FULL decision list + the [`ExecutionSummary`].
    ///
    /// The scorer runs at [`ScorerPolicy::default`] (the v1 structural floor); the money policy
    /// (caps/targets/fees + designation) comes from `policy`. A `Move` needs a routable shared
    /// gateway — supply it as this runtime's pinned gateway (devimint does not auto-register its
    /// LDK gateway; §4), exactly as `do_move` does. The probe route gate validates that same
    /// pinned gateway when present, so decisions match the route the executor will use.
    pub async fn tick(&self, policy: &TickPolicy) -> anyhow::Result<TickReport> {
        // §10.4: open a `Started` tick row BEFORE probing (a per-attempt `tick:` key, §10.1), so
        // a crash mid-tick leaves a durable row that reconcile repairs after 1h. Ledger recording
        // is auxiliary to the money op, so a storage fault here is logged, never fatal.
        let tick_key = IdempotencyKey(format!("tick:{}:{}", policy.occurrence.0, ledger_nonce()));
        if let Err(e) = self
            .journal
            .record_tick_started(&tick_key, policy.occurrence, now_ms())
            .await
        {
            tracing::warn!(error = ?e, "tick: recording the Started tick row failed");
        }

        // `plan_tick` scans the candidate registry (`auto_joined_candidates`) before a plan
        // exists, so a storage fault there can error out AFTER the `Started` row was written.
        // Terminalize the tick `Failed` on that path too, or `history/show` leaves it in-flight
        // until reconcile repairs it an hour later (§10.4), same as the bail paths below.
        let plan = match self.plan_tick(policy, &ScorerPolicy::default()).await {
            Ok(plan) => plan,
            Err(e) => {
                self.record_tick_failed(&tick_key, &e.to_string()).await;
                return Err(e);
            }
        };
        // A tick is a money op: an operator-pinned fed that could not be sensed or failed the
        // lnv2/probe gate this pass means the requested rebalance was NOT evaluated. Fail LOUDLY
        // (non-zero exit) rather than let `decide` degrade it to an advisory `RefuseInflow` that
        // `apply` skips, which would report a false success to a scheduler gating on the exit code.
        // Both bail paths land a `Failed` tick row WITH the diagnostic before returning (§10.4).
        let problems = pinned_input_problems(policy, &plan.snapshot, &plan.probes, &plan.decisions);
        if !problems.is_empty() {
            let error = format!("tick: {}", problems.join("; "));
            self.record_tick_failed(&tick_key, &error).await;
            anyhow::bail!("{error}");
        }
        if let Err(e) = self
            .ensure_fresh_tick_decisions(&plan.decisions, policy.occurrence)
            .await
        {
            self.record_tick_failed(&tick_key, &e.to_string()).await;
            return Err(e);
        }
        let executor = self.driving_executor();
        let summary = wallet_core::apply(
            self.journal.as_ref(),
            &executor,
            &decisions_to_apply(&plan.decisions),
            Actor::Agent {
                occurrence: policy.occurrence,
            },
            now_ms(),
        )
        .await;

        // §10.4: one `Refusal` row per advisory decision, then terminalize the tick with its
        // decision/apply counts. Both are auxiliary recordings — log a fault, never fail the tick.
        if let Err(e) = self
            .journal
            .record_refusals(&plan.decisions, policy.occurrence, now_ms())
            .await
        {
            tracing::warn!(error = ?e, "tick: recording refusal rows failed");
        }
        let counts = Some((
            plan.decisions.len() as u32,
            summary.performed as u32,
            summary.failed as u32,
        ));
        let (tick_status, tick_error) = tick_terminal(&summary);
        if let Err(e) = self
            .journal
            .record_tick_terminal(
                &tick_key,
                counts,
                tick_status,
                tick_error.as_deref(),
                now_ms(),
            )
            .await
        {
            tracing::warn!(error = ?e, "tick: recording the terminal tick row failed");
        }
        Ok(TickReport {
            decisions: plan.decisions,
            summary,
        })
    }

    /// Terminalize a tick row `Failed` on a bail path (§10.4) with zero counts + its diagnostic.
    /// Best-effort: a recording fault must not mask the bail's own error.
    async fn record_tick_failed(&self, key: &IdempotencyKey, error: &str) {
        if let Err(e) = self
            .journal
            .record_tick_terminal(key, None, OperationStatus::Failed, Some(error), now_ms())
            .await
        {
            tracing::warn!(error = ?e, "tick: recording the failed tick row failed");
        }
    }

    /// A DRY-RUN tick (Phase 2 step 2.2): probe → `score()` → `build_snapshot` → `decide()`, but
    /// DO NOT apply. Returns the per-fed scored view (each fed's `FederationVerdict` +
    /// `FederationStatus`), the designation `build_snapshot` chose, and the decisions that WOULD
    /// run. No money moves — this is `wallet-cli status`.
    ///
    /// Unlike [`Runtime::tick`], `status` does NOT bail on an unsensed / unusable pin: its whole
    /// job is to SHOW the operator why a tick would fail, so hard-failing before assembling the
    /// scored view would blank out exactly the diagnostic they ran it for. It surfaces each such
    /// pin problem as a `warn!` (to stderr) and still returns the full scored view + would-run
    /// decisions. The route check reflects the pinned gateway when one was supplied, same as `tick`.
    pub async fn status(&self, policy: &TickPolicy) -> anyhow::Result<StatusReport> {
        let scorer_policy = ScorerPolicy::default();
        let plan = self.plan_tick(policy, &scorer_policy).await?;
        // Surface (do NOT bail on) any pinned-input problem the equivalent `tick` would fail on, so
        // the operator sees BOTH the warning and the full scored view that explains it.
        for problem in pinned_input_problems(policy, &plan.snapshot, &plan.probes, &plan.decisions)
        {
            tracing::warn!("status: {problem}");
        }
        match self
            .terminal_replayed_executable_decisions(&plan.decisions)
            .await
        {
            Ok(replays) if !replays.is_empty() => tracing::warn!(
                "status: occurrence {} would replay already-terminal/subscription-owned decision(s) {}; \
                 tick will fail until --occurrence is advanced",
                policy.occurrence.0,
                describe_terminal_replays(&replays)
            ),
            Err(e) => tracing::warn!(
                "status: could not check whether this occurrence replays terminal decisions: {e}"
            ),
            _ => {}
        }
        // §5.0.6: the ACTIVE-probe verdict is SOURCE-RELATIVE — evaluated against the
        // snapshot's designated SPENDING fed (the fed that would fund the candidate),
        // always with the DEFAULT policy (the CLI's shrink-only overrides never reach the
        // production surface). Filled onto the facts (the field 5.1's gate reads) and the
        // scored row (the `status` display); the scorer itself ignores it in 5.0.
        let spending = plan.snapshot.spending_fed;
        let mut scored = Vec::with_capacity(plan.raw_probes.len());
        for (id, probe) in &plan.raw_probes {
            let active_probe = match spending {
                // The designated spending fed cannot probe ITSELF (a probe is a candidate
                // pair): leave its own row's verdict `None`/`-` rather than reporting a
                // bogus self-probe `never`/stale state on one of status's key rows.
                Some(source) if source == *id => None,
                Some(_) => plan.active_probes.get(id).copied(),
                None => None,
            };
            let mut facts = assemble_facts(probe, *id);
            facts.active_probe = active_probe;
            // The POST-GATE fundability the tick actually applies (§5.1.3), read from the exact
            // snapshot `plan_tick` decided on — NOT re-derived from `score()`, which ignores the
            // active probe. `build_snapshot` maps 1:1 over the probes, so the fed is always
            // present; the `is_some_and` default is a defensive fail-closed, not a real branch.
            let gated_eligible = plan
                .snapshot
                .federations
                .iter()
                .find(|f| f.id == *id)
                .is_some_and(|f| f.eligible_to_fund);
            scored.push(ScoredFed {
                id: *id,
                verdict: score(&facts, &scorer_policy),
                status: assemble_status(probe, *id),
                active_probe,
                gated_eligible,
            });
        }
        Ok(StatusReport {
            scored,
            spending_fed: plan.snapshot.spending_fed,
            standby_fed: plan.snapshot.standby_fed,
            decisions: plan.decisions,
        })
    }

    /// Probe, build, decide, and fold executor-route facts back into the probe view before the
    /// caller either reports a dry run or applies money moves.
    ///
    /// Without an explicit `--gateway`, the executor routes each fresh `Move`/`Evacuate` through
    /// the destination federation's FIRST registered gateway, then requires that same gateway to
    /// serve the send leg. The raw probe only knows whether each federation has some usable
    /// gateway. This loop validates the exact executor route for each FRESH decided send-required
    /// action; when a destination's concrete route cannot support the move, that destination is
    /// marked unavailable in the planning copy and the pure `build_snapshot`/`decide` path runs
    /// again. Same-key replays are left to `apply`, which resumes the stored intent and its cached
    /// `MoveRecord` gateway. The preflight uses the same `decisions_to_apply` projection as
    /// `apply`.
    /// `status` still reports the RAW scored probe view so a route-revision does not relabel a
    /// healthy federation as generally unprobed just because this tick's concrete move route failed.
    async fn plan_tick(
        &self,
        policy: &TickPolicy,
        scorer_policy: &ScorerPolicy,
    ) -> anyhow::Result<TickPlan> {
        let raw_probes = self.probe_all().await;
        let mut probes = raw_probes.clone();
        let auto_joined = self.auto_joined_candidates().await?;
        let mut route_revisions = 0usize;
        let mut evacuation_fallback: Option<EvacuationFallback> = None;
        loop {
            let preliminary = build_snapshot(
                &probes,
                policy,
                scorer_policy,
                &auto_joined,
                &BTreeMap::new(),
            );
            let active_probes = self
                .active_probe_verdicts(&probes, preliminary.spending_fed)
                .await;
            let snapshot =
                build_snapshot(&probes, policy, scorer_policy, &auto_joined, &active_probes);
            let decisions = wallet_core::decide(&snapshot, policy.occurrence);
            if let Some(fallback) = &evacuation_fallback {
                let still_trying_evacuation = decisions.iter().any(|d| {
                    matches!(&d.action, Action::Evacuate { from, .. } if *from == fallback.from)
                });
                if !still_trying_evacuation {
                    return Ok(fallback.plan.clone());
                }
            }
            let Some(problem) = self.first_move_route_problem(&decisions).await else {
                return Ok(TickPlan {
                    raw_probes,
                    probes,
                    active_probes,
                    snapshot,
                    decisions,
                });
            };

            if problem.evacuation_source_route {
                evacuation_fallback = Some(EvacuationFallback {
                    from: problem.from,
                    plan: TickPlan {
                        raw_probes: raw_probes.clone(),
                        probes: probes.clone(),
                        active_probes: active_probes.clone(),
                        snapshot: snapshot.clone(),
                        decisions: decisions.clone(),
                    },
                });
            }
            let changed = mark_gateway_unavailable(&mut probes, problem.mark_unavailable);
            tracing::warn!(
                from = %problem.from.to_hex(),
                to = %problem.to.to_hex(),
                marked_unavailable = %problem.mark_unavailable.to_hex(),
                gateway = %problem.gateway.as_ref().map(|g| g.0.as_str()).unwrap_or("<none>"),
                error = %problem.error,
                "tick: planned send-required route failed executor gateway validation; revising this tick's fundable set"
            );
            if !changed {
                return Ok(TickPlan {
                    raw_probes,
                    probes,
                    active_probes,
                    snapshot,
                    decisions,
                });
            }
            route_revisions += 1;
            if route_revisions > probes.len() {
                return Ok(TickPlan {
                    raw_probes,
                    probes,
                    active_probes,
                    snapshot,
                    decisions,
                });
            }
        }
    }

    /// The funding gate's PROBE-GATED set: every JOINED (`0x03`) federation that is NOT
    /// provably user-owned. Deriving it from joined MEMBERSHIP minus `UserApproved` candidates
    /// — rather than from `AutoJoined` candidate rows alone — fails CLOSED on two windows an
    /// AutoJoined-only set misses, both of which would otherwise let `tick` fund an
    /// agent-created member PRE-PROBE (defeating §5.1's "probes gate, discovery never
    /// promotes" invariant on the money path): (a) a crash between the Agent `join` and the
    /// `AutoJoined` candidate write leaves a member with a `Discovered`/absent `0x09` row;
    /// (b) a `0x03`-only restore leaves every agent-joined member with no `0x09` row.
    /// `discover`'s step-0 recovery repairs such rows, but `tick`/`build_snapshot` never run
    /// it, so the GATE itself must be conservative. Only an explicit `UserApproved` row (a
    /// user `join`/`approve`) exempts a member; a poison `0x09` row cannot PROVE user
    /// ownership, so it never exempts (the member stays gated by construction).
    async fn auto_joined_candidates(&self) -> anyhow::Result<BTreeSet<FederationId>> {
        let report = self
            .journal
            .list_candidates_report()
            .await
            .map_err(exec_err)?;
        let joined = self.journal.list_federations().await.map_err(exec_err)?;
        Ok(probe_gated_members(
            joined.into_iter().map(|(id, _)| id),
            report.candidates.iter().map(|(id, rec)| (*id, rec.state)),
        ))
    }

    async fn active_probe_verdicts(
        &self,
        probes: &[(FederationId, ProbeResult)],
        spending: Option<FederationId>,
    ) -> BTreeMap<FederationId, ActiveProbeVerdict> {
        let Some(source) = spending else {
            return BTreeMap::new();
        };
        let mut active = BTreeMap::new();
        for (id, _) in probes {
            if *id == source {
                continue;
            }
            match self.journal.probe_record(id).await {
                Ok(record) => {
                    let verdict = probe_verdict(
                        &record.map(|r| r.attempts).unwrap_or_default(),
                        source,
                        now_ms(),
                        &ProbePolicy::default(),
                    );
                    active.insert(*id, verdict);
                }
                Err(e) => {
                    tracing::warn!(
                        federation = %id.to_hex(),
                        error = ?e,
                        "tick: unreadable probe record; omitting the active-probe verdict"
                    );
                }
            }
        }
        active
    }

    async fn ensure_fresh_tick_decisions(
        &self,
        decisions: &[AllocatorDecision],
        occurrence: Occurrence,
    ) -> anyhow::Result<()> {
        let replays = self
            .terminal_replayed_executable_decisions(decisions)
            .await?;
        anyhow::ensure!(
            replays.is_empty(),
            "tick: occurrence {} would replay already-terminal/subscription-owned decision(s) {}; pass a fresh \
             --occurrence for a new rebalance, or use the same occurrence only to retry a \
             Pending/Executing tick",
            occurrence.0,
            describe_terminal_replays(&replays)
        );
        Ok(())
    }

    /// The same-occurrence decisions whose key already maps to an intent `apply` treats as
    /// TERMINAL, so re-driving them this tick is impossible without a fresh `--occurrence`. This
    /// MUST mirror `apply`'s terminal set (`wallet-core/src/executor.rs`): `Done` (idempotent
    /// replay of a settled intent), `Awaiting` (a `DirectInflow` owned by its subscription), and
    /// `Failed` (terminal until a manual reset — a recurring tick must not resurrect it). `apply`
    /// skips a `Failed` replay as `terminal_failed_skipped`, which `wallet-cli` turns into a
    /// non-zero tick exit; including it here lets `tick` fail early with the "advance --occurrence"
    /// remedy and lets the `status` dry run surface the SAME stale-occurrence signal.
    async fn terminal_replayed_executable_decisions(
        &self,
        decisions: &[AllocatorDecision],
    ) -> anyhow::Result<Vec<TerminalReplay>> {
        let mut replays = Vec::new();
        let mut seen = BTreeSet::new();
        for decision in decisions {
            if !tick_applies_decision(decision) || !seen.insert(decision.idempotency_key.clone()) {
                continue;
            }
            if let Some(intent) = self
                .journal
                .get(&decision.idempotency_key)
                .await
                .map_err(exec_err)?
            {
                if matches!(
                    intent.status,
                    IntentStatus::Done | IntentStatus::Awaiting | IntentStatus::Failed
                ) {
                    replays.push(TerminalReplay {
                        key: decision.idempotency_key.clone(),
                        status: intent.status,
                    });
                }
            }
        }
        Ok(replays)
    }

    /// The first route problem in this tick's fresh, apply-bound send-required decisions.
    /// Destination failures and send-gateway source failures both mark the selected
    /// destination unavailable, letting `plan_tick` rerun allocation and fall through to a
    /// later eligible federation when one can actually serve the route. If every destination
    /// fails an evacuation source-route preflight, `plan_tick` falls back to the last
    /// evacuation plan and lets `apply` surface the real execution failure loudly instead of
    /// silently reporting that nothing needed to run.
    async fn first_move_route_problem(
        &self,
        decisions: &[AllocatorDecision],
    ) -> Option<MoveRouteProblem> {
        let decisions = decisions_to_apply(decisions);
        for decision in &decisions {
            let problem = match &decision.action {
                Action::Move { from, to, .. } => {
                    if self.has_existing_intent(decision).await {
                        continue;
                    }
                    self.validate_executor_move_route(SendRouteKind::Move, *from, *to)
                        .await
                        .err()
                }
                Action::Evacuate { from, to, .. } => {
                    if self.has_existing_intent(decision).await {
                        continue;
                    }
                    self.validate_executor_move_route(SendRouteKind::Evacuate, *from, *to)
                        .await
                        .err()
                }
                _ => None,
            };
            let Some(problem) = problem else {
                continue;
            };
            return Some(problem);
        }
        None
    }

    async fn has_existing_intent(&self, decision: &AllocatorDecision) -> bool {
        match self.journal.get(&decision.idempotency_key).await {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(
                    key = %decision.idempotency_key.0,
                    error = ?e,
                    "tick: could not read existing intent before route preflight; leaving route validation to apply"
                );
                true
            }
        }
    }

    /// Preflight the executor's concrete gateway route for a fresh send-required action.
    ///
    /// Destination failures mean this tick's chosen target cannot receive through the same
    /// gateway the executor will use, so `plan_tick` marks that destination unavailable and
    /// reruns allocation. Source-side failures are also tied to the destination-selected
    /// gateway: if that gateway cannot serve the source, another eligible destination may
    /// still work and should be tried before the executor commits any receive-side artifact.
    async fn validate_executor_move_route(
        &self,
        kind: SendRouteKind,
        from: FederationId,
        to: FederationId,
    ) -> Result<(), MoveRouteProblem> {
        // Mirror the executor's `resolve_gateway` SCAN (§15.6): the route is usable iff SOME
        // gateway in the destination's registered set (or the single pinned gateway) serves the
        // destination AND — for a send — the source.
        let candidates = match self.route_gateway_candidates(&to).await {
            Ok(candidates) => candidates,
            Err(error) => {
                return Err(MoveRouteProblem {
                    from,
                    to,
                    mark_unavailable: to,
                    gateway: None,
                    error,
                    evacuation_source_route: false,
                });
            }
        };
        // Validate candidates in registration order, short-circuiting on the first that serves the
        // whole route; a gateway that fails the destination never has its source checked.
        let mut outcomes = Vec::with_capacity(candidates.len());
        for gateway in &candidates {
            let dest_ok = self.mc.validate_gateway(&to, gateway).await.is_ok();
            let source_ok = dest_ok && self.mc.validate_gateway(&from, gateway).await.is_ok();
            outcomes.push((dest_ok, source_ok));
            if source_ok {
                break;
            }
        }
        match scan_route(&outcomes) {
            RouteScan::Routable(_) => Ok(()),
            // A gateway served the destination but none of those also served the source → a
            // source-route problem (an evacuation may re-target another destination).
            RouteScan::SourceUnserved(i) => Err(source_route_problem(
                kind,
                from,
                to,
                candidates[i].clone(),
                "no gateway serving the destination also serves the source".into(),
            )),
            // No candidate served the destination at all → a destination problem.
            RouteScan::DestinationUnserved => Err(MoveRouteProblem {
                from,
                to,
                mark_unavailable: to,
                gateway: candidates.first().cloned(),
                error: "no registered gateway serves the destination".into(),
                evacuation_source_route: false,
            }),
        }
    }

    /// The gateway candidates the executor would SCAN for a move into `to` (§15.6): the single
    /// pinned gateway, or the destination's registered lnv2 set. `Err` (empty / unreadable) is a
    /// destination-route problem the caller reports against `to`.
    async fn route_gateway_candidates(&self, to: &FederationId) -> Result<Vec<GatewayUrl>, String> {
        if let Some(gateway) = &self.pinned_gateway {
            return Ok(vec![gateway.clone()]);
        }
        let gateways = self
            .mc
            .gateways(to)
            .await
            .map_err(|e| format!("listing destination gateways failed: {e}"))?;
        if gateways.is_empty() {
            return Err(format!(
                "no lnv2 gateway registered for destination federation {}",
                to.to_hex()
            ));
        }
        Ok(gateways)
    }

    /// Probe every OPEN federation into a `(FederationId, ProbeResult)` list, BEST-EFFORT: a fed
    /// whose probe errors (a local db/config read genuinely failed) is warn-logged and skipped,
    /// mirroring [`MultiClient::open_all`]'s poison-tolerance so one un-probeable fed cannot
    /// strand the whole tick. A skipped fed simply drops out of the snapshot — the allocator then
    /// cannot fund it or from it, which is the safe degradation (never a bad move).
    async fn probe_all(&self) -> Vec<(FederationId, ProbeResult)> {
        let runner =
            FedimintProbeRunner::with_pinned_gateway(self.mc.clone(), self.pinned_gateway.clone());
        let mut probes = Vec::new();
        for id in self.mc.federations() {
            match runner.probe(&id).await {
                Ok(probe) => probes.push((id, probe)),
                Err(e) => tracing::warn!(
                    federation = %id.to_hex(),
                    error = ?e,
                    "tick: skipping federation that failed to probe"
                ),
            }
        }
        probes
    }

    /// Persist the settled/failed phase (+ optional outcome message) of a finalized move's
    /// `MoveRecord`, keeping the derived cache consistent with the intent's terminal status.
    async fn settle_move(
        &self,
        rec: &MoveRecord,
        phase: MovePhase,
        outcome: Option<String>,
    ) -> anyhow::Result<()> {
        let mut settled = rec.clone();
        settled.phase = phase;
        if outcome.is_some() {
            settled.outcome = outcome;
        }
        self.journal.put_move(&settled).await.map_err(exec_err)
    }

    /// CAS the intent from `Awaiting` to a terminal status. `Ok(false)` means a concurrent
    /// finalize already moved it (idempotent) — not an error.
    async fn finalize(&self, key: &IdempotencyKey, new: IntentStatus) -> anyhow::Result<()> {
        self.journal
            .set_status_if(key, IntentStatus::Awaiting, new)
            .await
            .map_err(exec_err)?;
        Ok(())
    }

    async fn move_record_for_guard(
        &self,
        intent: &wallet_core::Intent,
    ) -> anyhow::Result<MoveRecord> {
        if let Some(rec) = self
            .journal
            .get_move(&intent.idempotency_key)
            .await
            .map_err(exec_err)?
        {
            return Ok(rec);
        }
        self.executor()
            .backfill_move_record(intent)
            .await
            .map_err(exec_err)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "intent {} is not an executable move",
                    intent.idempotency_key.0
                )
            })
    }
}

#[async_trait]
impl DiscoveryBackend for Runtime {
    async fn joined_federations(&self) -> anyhow::Result<BTreeSet<FederationId>> {
        Ok(self
            .journal
            .list_federations()
            .await
            .map_err(exec_err)?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }

    async fn joined_federation_invites(
        &self,
    ) -> anyhow::Result<Vec<(FederationId, fedimint_core::invite_code::InviteCode)>> {
        let mut invites = Vec::new();
        for (id, info) in self.journal.list_federations().await.map_err(exec_err)? {
            match info
                .invite
                .parse::<fedimint_core::invite_code::InviteCode>()
            {
                Ok(invite) => invites.push((id, invite)),
                Err(e) => tracing::warn!(
                    federation = %id.to_hex(),
                    error = ?e,
                    "discover: joined federation registry invite is invalid; cannot seed candidate row"
                ),
            }
        }
        Ok(invites)
    }

    async fn get_candidate(
        &self,
        id: FederationId,
    ) -> anyhow::Result<Option<crate::CandidateRecord>> {
        self.journal.get_candidate(&id).await.map_err(exec_err)
    }

    async fn put_candidate(&self, record: crate::CandidateRecord) -> anyhow::Result<()> {
        self.journal.put_candidate(&record).await.map_err(exec_err)
    }

    async fn list_candidates(&self) -> anyhow::Result<Vec<(FederationId, crate::CandidateRecord)>> {
        self.journal.list_candidates().await.map_err(exec_err)
    }

    async fn agent_created_federation(&self, id: FederationId) -> anyhow::Result<bool> {
        self.journal
            .agent_created_federation(&id)
            .await
            .map_err(exec_err)
    }

    async fn preview(
        &self,
        invite: &fedimint_core::invite_code::InviteCode,
    ) -> anyhow::Result<PreviewedCandidate> {
        let config = self.mc.preview_config(invite).await?;
        let id = crate::multi_client::bridge_federation_id(config.calculate_federation_id());
        Ok(PreviewedCandidate {
            id,
            facts: facts_from_client_config(id, &config),
        })
    }

    async fn auto_join_counts(&self, now_ms: u64) -> anyhow::Result<AutoJoinCounts> {
        let passed = self.passed_probe_feds(now_ms).await;
        Ok(AutoJoinCounts {
            concurrent_unproven: self
                .journal
                .concurrent_unproven(&passed)
                .await
                .map_err(exec_err)?,
            weekly_auto_joins: self
                .journal
                .weekly_auto_joins(now_ms)
                .await
                .map_err(exec_err)?,
            lifetime_auto_joins: self.journal.lifetime_auto_joins().await.map_err(exec_err)?,
        })
    }

    async fn join_as_agent(
        &self,
        id: FederationId,
        invite: fedimint_core::invite_code::InviteCode,
        occurrence: Occurrence,
        now_ms: u64,
    ) -> anyhow::Result<crate::JoinOutcome> {
        let key = IdempotencyKey(format!("join:{}:{}", id.to_hex(), ledger_nonce()));
        self.journal
            .record_started(
                &key,
                OperationKind::Join { fed: id },
                Actor::Agent { occurrence },
                ReasonCode::StandingInstruction,
                now_ms,
                None,
            )
            .await
            .map_err(exec_err)?;
        let outcome = match self.mc.join(invite).await {
            Ok(outcome) => outcome,
            Err(e) => {
                let _ = self
                    .journal
                    .record_terminal(
                        &key,
                        OperationStatus::Failed,
                        now_ms,
                        Some(&e.to_string()),
                        None,
                    )
                    .await;
                return Err(e);
            }
        };
        let note = (!outcome.newly_joined).then_some(crate::JOIN_NOOP_REOPEN_NOTE);
        self.journal
            .record_terminal(&key, OperationStatus::Succeeded, now_ms, note, None)
            .await
            .map_err(exec_err)?;
        Ok(outcome)
    }

    async fn record_discover(
        &self,
        key: IdempotencyKey,
        occurrence: Occurrence,
        report: &crate::DiscoverSourceReport,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        self.journal
            .record_terminal_operation(
                &key,
                discover_kind(report),
                discovery_actor(occurrence),
                DISCOVERY_REASON,
                now_ms,
            )
            .await
            .map_err(exec_err)
    }

    async fn record_auto_join(
        &self,
        key: IdempotencyKey,
        occurrence: Occurrence,
        report: &crate::AutoJoinReport,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        self.journal
            .record_terminal_operation(
                &key,
                auto_join_kind(report),
                discovery_actor(occurrence),
                DISCOVERY_REASON,
                now_ms,
            )
            .await
            .map_err(exec_err)
    }
}

/// The candidate ids whose live probe verdict can EXEMPT them from the concurrent-unproven cap
/// (§5.1.4): every `AutoJoined` row PLUS every poison-skipped id. `concurrent_unproven` counts
/// skipped ids fail-closed, so `passed_probe_feds` must be able to clear a skipped id that has
/// since Passed — otherwise a corrupt `AutoJoined` row whose probe passed would consume a
/// concurrent slot forever. Mirrors [`Runtime::auto_joined_candidates`]'s fail-closed set.
fn probe_gate_candidate_ids(report: &CandidateListReport) -> BTreeSet<FederationId> {
    let mut ids: BTreeSet<FederationId> = report
        .candidates
        .iter()
        .filter(|(_, rec)| rec.state == CandidateState::AutoJoined)
        .map(|(id, _)| *id)
        .collect();
    ids.extend(report.skipped_ids.iter().copied());
    ids
}

impl Runtime {
    async fn passed_probe_feds(&self, now_ms: u64) -> BTreeSet<FederationId> {
        let report = match self.journal.list_candidates_report().await {
            Ok(report) => report,
            Err(e) => {
                tracing::warn!(error = ?e, "discover: candidate scan failed while computing passed probes");
                return BTreeSet::new();
            }
        };
        let mut passed = BTreeSet::new();
        for id in probe_gate_candidate_ids(&report) {
            let attempts = match self.journal.probe_record(&id).await {
                Ok(record) => record.map(|r| r.attempts).unwrap_or_default(),
                Err(e) => {
                    tracing::warn!(
                        federation = %id.to_hex(),
                        error = ?e,
                        "discover: probe record unreadable; treating candidate as unproven"
                    );
                    continue;
                }
            };
            let sources: BTreeSet<_> = attempts.iter().map(|attempt| attempt.from).collect();
            if sources.into_iter().any(|source| {
                probe_verdict(&attempts, source, now_ms, &ProbePolicy::default())
                    == ActiveProbeVerdict::Passed
            }) {
                passed.insert(id);
            }
        }
        passed
    }
}

fn facts_from_client_config(id: FederationId, config: &ClientConfig) -> FederationFacts {
    let num_endpoints = config.global.api_endpoints.len();
    let module_kinds: Vec<String> = config
        .modules
        .values()
        .map(|module| module.kind.as_str().to_owned())
        .collect();
    let has_lnv2 = module_kinds
        .iter()
        .any(|kind| kind == fedimint_lnv2_client::common::KIND.as_str());
    FederationFacts {
        id,
        guardian_count: num_endpoints as u32,
        threshold: threshold_for_endpoints(num_endpoints),
        is_mainnet: wallet_network(config) == Some(bitcoin::Network::Bitcoin),
        modules: module_kinds
            .iter()
            .map(|kind| module_from_kind(kind))
            .collect(),
        quorum_live: false,
        round_trip_ok: false,
        peg_out_quotable: false,
        latency_ms: 0,
        shutdown_scheduled: false,
        has_lnv2,
        observer: None,
        active_probe: None,
    }
}

fn threshold_for_endpoints(num_endpoints: usize) -> u32 {
    if num_endpoints == 0 {
        return 0;
    }
    NumPeers::from(num_endpoints).threshold() as u32
}

fn wallet_network(config: &ClientConfig) -> Option<bitcoin::Network> {
    config
        .modules
        .values()
        .find(|module| module.kind == fedimint_wallet_client::common::KIND)
        .and_then(|module| match &module.config {
            DynRawFallback::Decoded(config) => config
                .as_any()
                .downcast_ref::<fedimint_wallet_client::config::WalletClientConfig>()
                .map(|config| config.network.0),
            DynRawFallback::Raw { raw, .. } => {
                fedimint_wallet_client::config::WalletClientConfig::consensus_decode_whole(
                    raw,
                    &ModuleDecoderRegistry::default(),
                )
                .ok()
                .map(|config| config.network.0)
            }
        })
}

fn module_from_kind(kind: &str) -> Module {
    match kind {
        "mint" => Module::Mint,
        "ln" => Module::Ln,
        "lnv2" => Module::Lnv2,
        "wallet" => Module::Wallet,
        "meta" => Module::Meta,
        _ => Module::Other,
    }
}

fn ensure_expected_fed(
    key: &IdempotencyKey,
    rec: &MoveRecord,
    expected: FederationId,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        rec.to == expected,
        "intent {} receives into {}, not {}",
        key.0,
        rec.to.to_hex(),
        expected.to_hex()
    );
    Ok(())
}

/// The deterministic idempotency key for a CLI-driven `DirectInflow` (mirrors the allocator's
/// `move:`/`evac:` key scheme). Stable across re-runs of the same request so `apply` dedups it;
/// bumping `occurrence` produces a fresh key for a genuinely new inflow.
fn direct_inflow_key(
    to: &FederationId,
    amount: Msat,
    fee_cap: Msat,
    occurrence: Occurrence,
) -> IdempotencyKey {
    IdempotencyKey(format!(
        "direct-inflow:{}:{}:{}:{}",
        to.to_hex(),
        amount.0,
        fee_cap.0,
        occurrence.0
    ))
}

/// The deterministic idempotency key for a CLI-driven `Move` (mirrors the allocator's `move:`
/// scheme and [`direct_inflow_key`]'s all-params form). Stable across re-runs of the same
/// request so `apply` dedups it (no re-mint/re-pay); bumping `occurrence` produces a fresh key
/// for a genuinely new move. All params participate, so a same-`from`/`to`/`occurrence` request
/// with a DIFFERENT amount/cap is a distinct move rather than silently dedup'd to the old one.
fn move_key(
    from: &FederationId,
    to: &FederationId,
    amount: Msat,
    fee_cap: Msat,
    occurrence: Occurrence,
) -> IdempotencyKey {
    IdempotencyKey(format!(
        "move:{}:{}:{}:{}:{}",
        from.to_hex(),
        to.to_hex(),
        amount.0,
        fee_cap.0,
        occurrence.0
    ))
}

/// The §5.0.5 probe report: the verdicts around ONE recorded attempt plus the leg keys.
/// Returned only when an attempt was recorded (a pass, or a demoting candidate-fault
/// failure); no-attempt terminal exits and transient still-pending legs are errors.
#[derive(Clone, Debug)]
pub struct ProbeReport {
    pub verdict_before: ActiveProbeVerdict,
    pub outcome: ProbeOutcome,
    pub verdict_after: ActiveProbeVerdict,
    pub in_key: IdempotencyKey,
    /// `None` when the probe never reached leg OUT (a leg-IN failure).
    pub out_key: Option<IdempotencyKey>,
}

/// A probe invocation's terminal, operator-visible result. `active_probe` returns this
/// (Ok) for EVERY terminal outcome — success, a candidate-attributable leg failure, OR an
/// umbrella-only no-attempt refusal — so the CLI can honor its §5.0.7 scriptable stdout
/// contract in the failure cases too. `active_probe` reserves `Err` for genuinely
/// TRANSIENT defers (a balance read failed, session retained for a re-run).
#[derive(Clone, Debug)]
pub enum ProbeOutcome {
    /// A recorded attempt: a full round trip (`ok`) or a candidate-attributable leg
    /// failure (`!ok`) — both durably appended to the probe history and reflected in
    /// `verdict_after`.
    Attempt(ProbeAttempt),
    /// A terminal umbrella-only refusal that recorded NO attempt (a source/route/local
    /// fault, an inconclusive resume, or a parametric infeasibility): the trust verdict is
    /// unchanged. Carries the verbatim diagnostic.
    NoAttempt(String),
}

/// One probe invocation's fixed identity, threaded through the §5.0.5 exits.
struct ProbeRun {
    candidate: FederationId,
    source: FederationId,
    actor: Actor,
    verdict_before: ActiveProbeVerdict,
    nonce: String,
    umbrella_key: IdempotencyKey,
    amount: Msat,
    leg_fee_cap: Msat,
    in_key: IdempotencyKey,
    /// The policy the legs actually run under — money fields locked to the session, so a
    /// resumed attempt is judged against the parameters it was spent with (not the flags).
    effective_policy: ProbePolicy,
    /// The probe's START time, from the durable session (persisted before leg IN). The
    /// recorded attempt's `at_ms` uses THIS, not `now_ms()`: a crash-then-delayed-resume
    /// must stamp the evidence at when the probe happened, not at recovery time — the
    /// verdict is driven entirely by `at_ms`, so a recovery-time stamp could keep a stale
    /// probe inside the ttl window or synthesize the span a `Passed` needs.
    started_at_ms: u64,
}

/// Which probe leg a move drives: IN mints on the candidate (S → C), OUT redeems back
/// (C → S). Decides fault attribution — each step's HOST differs per leg.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProbeLeg {
    In,
    Out,
}

impl ProbeLeg {
    fn label(self) -> &'static str {
        match self {
            ProbeLeg::In => "IN",
            ProbeLeg::Out => "OUT",
        }
    }
}

/// §5.0.3's fault attribution verdict for a failed leg.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegFault {
    /// The candidate itself refused (mint on leg IN / pay on leg OUT): a DEMOTING attempt.
    Candidate,
    /// Source, gateway, ambiguous, or local-parametric: umbrella row only, no attempt.
    /// Safety holds without demotion because NO-ATTEMPT ≠ PASS — the candidate simply
    /// does not progress toward `Passed`.
    UmbrellaOnly,
}

/// Classify a failed probe leg from what the move machinery already exposes (§5.0.3):
/// the failing STEP (derived from which artifacts the move record holds), the terminal
/// settlement phase, and the executor's diagnostic. PURE, unit-tested. Demotes ONLY on a
/// candidate-hosted step's failure that is not a recognized local/gateway/corruption
/// signature — when attribution is genuinely unclear, the fault is AMBIGUOUS and does
/// not demote.
fn classify_leg_failure(leg: ProbeLeg, rec: Option<&MoveRecord>, error: &str) -> LegFault {
    match rec.map(|r| r.phase) {
        // A `Stranded` leg (send settled, receive never credited) cannot distinguish a
        // thieving gateway from a broken candidate; `Refunded` failed downstream and made
        // the payer whole. Neither demotes.
        Some(MovePhase::Stranded) | Some(MovePhase::Refunded) => return LegFault::UmbrellaOnly,
        // The send leg reached a terminal FAILED settlement: the PAYER owns it. Leg OUT's
        // payer is the candidate — the redeemability core.
        Some(MovePhase::Failed) => {
            return if leg == ProbeLeg::Out {
                LegFault::Candidate
            } else {
                LegFault::UmbrellaOnly
            };
        }
        _ => {}
    }
    // No terminal settlement phase: a Permanent executor error mid-step. Which step, from
    // the record's artifacts (`next_step`'s own derivation): no invoice and no send op =
    // `CreateInvoice` (runs on the move's DESTINATION); invoice without a send op = `Pay`
    // (runs on the move's SOURCE); both present = an await-step oddity (ambiguous).
    let has_invoice = rec.is_some_and(|r| r.invoice.is_some());
    let has_send_op = rec.is_some_and(|r| r.send_op.is_some());
    let candidate_hosted_step = if !has_invoice && !has_send_op {
        leg == ProbeLeg::In // mint hosted on C ⇔ the move's destination is C ⇔ leg IN
    } else if !has_send_op {
        leg == ProbeLeg::Out // pay hosted on C ⇔ the move's source is C ⇔ leg OUT
    } else {
        return LegFault::UmbrellaOnly;
    };
    if candidate_hosted_step && !is_known_non_candidate_error(error) {
        LegFault::Candidate
    } else {
        LegFault::UmbrellaOnly
    }
}

/// Error signatures OUR OWN machinery produces for local-parametric, fee-environment,
/// gateway-TOCTOU, expiry, and corruption faults — never candidate dishonesty, so they
/// must not demote even when they surface on a candidate-hosted step (§5.0.2/§5.0.3).
/// These are free-text couplings to diagnostics emitted in the executors; the test
/// `non_candidate_signatures_match_an_emit_site` pins each one to its emitting source
/// so a reworded diagnostic cannot silently start demoting candidates.
const NON_CANDIDATE_SIGNATURES: &[&str] = &[
    "fee over cap",                     // receive-side + pay-step cap refusals (local)
    "lnv2 requires at least",           // minimum-incoming-contract refusal (parametric)
    "no invoice can net the requested", // unsolvable gross-up (local/fee environment)
    "destination would exceed the per-fed cap", // ADR-0018 local cap refusal
    "gateway receive fee changed between quote and mint", // §15.7 TOCTOU (gateway-timed)
    "receive op is missing the quoted contract amount", // corruption
    "receive contract check failed",    // corruption
    "parsing move invoice",             // corruption
    "move invoice expired before the send leg", // §15.4 expiry belt (timing)
    "move invoice carries no amount",   // malformed/corrupt return invoice (source-side, not C)
    "reached with no",                  // internal invariant breaches
    "executor does not support this action",
];

fn is_known_non_candidate_error(error: &str) -> bool {
    if NON_CANDIDATE_SIGNATURES
        .iter()
        .any(|sig| error.contains(sig))
    {
        return true;
    }

    // Pinned SDK b108ec6 exposes these as deterministic send rejections
    // (`modules/fedimint-lnv2-client/src/lib.rs:1231-1249`). They are gateway
    // limits or a timing race around an already-minted invoice, not evidence that
    // the candidate federation refuses redemption.
    error.contains("lnv2 send deterministically rejected the invoice:")
        && (error.contains("Gateway fee exceeds the allowed limit")
            || error.contains("Gateway expiration time exceeds the allowed limit")
            || error.contains("Invoice has expired"))
}

/// §5.0.5: the wallet's NET OUTFLOW FROM the source — leg IN's total S debit (the
/// delivered net + both leg-IN fee quotes; the send settled iff the phase is `Settled`
/// or `Stranded`) minus leg OUT's S credit (its delivered net, iff `Settled`). `None`
/// when no money left the source (leg IN never settled its send, or refunded whole). On
/// a clean pass this is fees + the small residue; on a hostile candidate whose leg OUT
/// never redeems it is fees + the WHOLE delivered amount — the honest exposure number.
fn probe_cost(in_rec: Option<&MoveRecord>, out_rec: Option<&MoveRecord>) -> Option<Msat> {
    let debit = in_rec.and_then(|r| match r.phase {
        MovePhase::Settled | MovePhase::Stranded => Some(
            r.amount
                .0
                .saturating_add(r.receive_fee_quoted.map_or(0, |f| f.0))
                .saturating_add(r.send_fee_quoted.map_or(0, |f| f.0)),
        ),
        _ => None,
    })?;
    let credit = out_rec
        .and_then(|r| (r.phase == MovePhase::Settled).then_some(r.amount.0))
        .unwrap_or(0);
    Some(Msat(debit.saturating_sub(credit)))
}

/// §5.0.4's no-sweep precondition for the sized-but-unjournaled leg-OUT RESUME window:
/// leg OUT may start only when the candidate holds EXACTLY the pre-probe baseline plus
/// the delivered delta. Leg IN credits C exactly `delivered_in` (never-over; fees are
/// paid by the source), so an untouched C sits at exactly `baseline + delivered_in`. A
/// `>=` check is fooled by SPEND-THEN-REPLENISH: C held 100, delta 20 (→120); spend 15
/// (→105), then an unrelated inflow of 20 (→125) — `125 >= 120` passes though 15 sats of
/// the redemption would now come from other funds. Any deviation (below OR above) means
/// intervening activity touched C between the crash and this resume, so the delta's
/// provenance is no longer certain: abort INCONCLUSIVE (§5.0.4) rather than risk a sweep.
fn no_sweep_ok(c_spendable: Msat, baseline: Msat, delivered_in: Msat) -> bool {
    c_spendable.0 == baseline.0.saturating_add(delivered_in.0)
}

/// PURE core of [`Runtime::auto_joined_candidates`] (§5.1.3 funding gate): the probe-gated
/// set = JOINED members minus the `UserApproved` ones. Membership-minus-UserApproved (NOT
/// AutoJoined-rows-only) is what fails closed on the crash/restore windows where an
/// agent-created member's `0x09` row is still `Discovered`/`Rejected`/absent — those would
/// otherwise read as ungated on `tick` and fund pre-probe.
fn probe_gated_members(
    joined: impl IntoIterator<Item = FederationId>,
    candidate_states: impl IntoIterator<Item = (FederationId, CandidateState)>,
) -> BTreeSet<FederationId> {
    let user_approved: BTreeSet<FederationId> = candidate_states
        .into_iter()
        .filter_map(|(id, state)| (state == CandidateState::UserApproved).then_some(id))
        .collect();
    joined
        .into_iter()
        .filter(|id| !user_approved.contains(id))
        .collect()
}

/// Leg OUT's effective cap: the operator's per-leg cap still bounds fees, but the
/// return leg must also prove `out_net + actual drive-time fees <= delivered_in`.
/// The executor re-quotes send fees at `Pay`, so using the remaining delivered delta
/// as the move's fee cap keeps a fee spike from spending pre-probe candidate funds.
/// Fee-jitter margin reserved when sizing leg OUT (§5.0.2): the fee QUOTE at sizing time
/// can come in a few msat under the ACTUAL fee re-quoted at the Pay step, and the return
/// leg's cap is bounded tight by the no-sweep budget — with no margin, that jitter breaches
/// the cap and defers the probe. Sized out of the redeemed budget, it lands as bounded
/// extra candidate residue (accepted, §5.0.9 decision 6), always far below the leg fee cap.
const PROBE_FEE_MARGIN_MSAT: u64 = 1_000;

fn probe_out_fee_cap(delivered_in: Msat, out_net: Msat, leg_fee_cap: Msat) -> Msat {
    Msat(leg_fee_cap.0.min(delivered_in.0.saturating_sub(out_net.0)))
}

/// The probe's umbrella [`OperationKind`] (§5.0.5), with `cost_msat` = the terminal cost
/// (or `None` on `record_started` / no-money exits).
fn probe_kind(run: &ProbeRun, cost_msat: Option<Msat>) -> OperationKind {
    OperationKind::Probe {
        fed: run.candidate,
        from: run.source,
        amount_msat: run.amount,
        cost_msat,
    }
}

/// The umbrella ledger key `probe:<fed-hex>:<nonce>` (§5.0.5).
fn probe_umbrella_key(fed: &FederationId, nonce: &str) -> IdempotencyKey {
    IdempotencyKey(format!("probe:{}:{nonce}", fed.to_hex()))
}

/// The nonce-derived occurrence embedded in both probe legs' `move:` keys (§5.0.5): the
/// keys stay reconstructible from the session alone, and a 64-bit random head never
/// collides with user moves' small occurrence integers.
fn occurrence_from_nonce(nonce: &str) -> anyhow::Result<Occurrence> {
    let head = nonce
        .get(..16)
        .ok_or_else(|| anyhow::anyhow!("probe session nonce {nonce:?} is too short"))?;
    let value = u64::from_str_radix(head, 16)
        .map_err(|e| anyhow::anyhow!("probe session nonce {nonce:?} is not hex: {e}"))?;
    Ok(Occurrence(value))
}

/// The §5.0.5 LOCAL preflight faults, pure over sampled balances: self-probe, a source
/// too poor to fund `amount + leg fee cap`, a candidate without ADR-0018 cap room for
/// `amount`. The SOURCE needs no cap-room check: leg IN debits it by strictly more than
/// leg OUT returns, so the return always fits the room leg IN just created.
fn probe_local_faults(
    candidate: FederationId,
    source: FederationId,
    source_spendable: Msat,
    candidate_spendable: Msat,
    amount: Msat,
    leg_fee_cap: Msat,
    hard_cap: Option<Msat>,
) -> Result<(), String> {
    if candidate == source {
        return Err(format!(
            "cannot probe federation {} from itself",
            candidate.to_hex()
        ));
    }
    let needed = amount.0.saturating_add(leg_fee_cap.0);
    if source_spendable.0 < needed {
        return Err(format!(
            "insufficient source balance: {} holds {} msat, below amount + leg fee cap = {needed} msat",
            source.to_hex(),
            source_spendable.0
        ));
    }
    if let Some(cap) = hard_cap {
        if candidate_spendable.0.saturating_add(amount.0) > cap.0 {
            return Err(format!(
                "insufficient candidate cap room: {} holds {} msat and the {} msat probe amount \
                 would exceed the per-fed cap {} msat",
                candidate.to_hex(),
                candidate_spendable.0,
                amount.0,
                cap.0
            ));
        }
        // Leg OUT mints BACK into the source, which runs the same ADR-0018 perform-time cap
        // enforcement. Leg IN first debits the source by `amount + fees` and leg OUT credits
        // back strictly less, so an untouched source ENDING ≤ its start means a source that
        // starts AT-OR-BELOW the cap can never breach it on the return leg. But a source
        // already ABOVE the cap (a transient inbound) would spend leg IN and then fail leg
        // OUT umbrella-only with "destination would exceed the per-fed cap" — a GUARANTEED
        // inconclusive spend. Refuse it here as a LOCAL fault before any money moves.
        if source_spendable.0 > cap.0 {
            return Err(format!(
                "probe source {} holds {} msat, already above the per-fed cap {} msat: the \
                 return leg would breach it — reduce the source below the cap first",
                source.to_hex(),
                source_spendable.0,
                cap.0
            ));
        }
    }
    Ok(())
}

fn intent_status_label_opt(status: Option<IntentStatus>) -> &'static str {
    status.map_or("absent", intent_status_label)
}

fn mark_gateway_unavailable(probes: &mut [(FederationId, ProbeResult)], id: FederationId) -> bool {
    let Some((_, probe)) = probes.iter_mut().find(|(probe_id, _)| *probe_id == id) else {
        return false;
    };
    if !probe.gateway_available {
        return false;
    }
    probe.gateway_available = false;
    true
}

/// The verdict of scanning a destination's gateway set for a usable route (§15.6). Given, in
/// registration order, each candidate's `(serves_destination, serves_source)` validation outcomes
/// (`serves_source` is `true` for a receive-only route), decide whether SOME gateway serves BOTH
/// ends. PURE, so the "first gateway dead / second alive" and "serves only the destination" cases
/// are unit-tested without a live gateway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RouteScan {
    /// A fully-valid gateway (both ends served) was found at this index.
    Routable(usize),
    /// Some gateway served the destination (index given) but none of those served the source.
    SourceUnserved(usize),
    /// No gateway served the destination at all.
    DestinationUnserved,
}

fn scan_route(candidates: &[(bool, bool)]) -> RouteScan {
    let mut first_dest_ok: Option<usize> = None;
    for (i, &(dest_ok, source_ok)) in candidates.iter().enumerate() {
        if !dest_ok {
            continue;
        }
        if source_ok {
            return RouteScan::Routable(i);
        }
        first_dest_ok.get_or_insert(i);
    }
    match first_dest_ok {
        Some(i) => RouteScan::SourceUnserved(i),
        None => RouteScan::DestinationUnserved,
    }
}

fn source_route_problem(
    kind: SendRouteKind,
    from: FederationId,
    to: FederationId,
    gateway: GatewayUrl,
    error: String,
) -> MoveRouteProblem {
    MoveRouteProblem {
        from,
        to,
        mark_unavailable: to,
        gateway: Some(gateway),
        error: format!("source gateway validation failed: {error}"),
        evacuation_source_route: matches!(kind, SendRouteKind::Evacuate),
    }
}

/// Whether a decision is one the tick drives through `apply` — kept in lockstep with
/// [`decisions_to_apply`](crate::tick::decisions_to_apply), so the stale-occurrence guard in
/// [`Runtime::terminal_replayed_executable_decisions`] checks EXACTLY the set `apply` runs. As
/// of Phase 3.A that is every executable action (`Move`/`Evacuate`/`DirectInflow`); `Evacuate` is
/// no longer excluded, so a same-occurrence re-tick of a now-terminal evacuate fails loudly like a
/// Move instead of silently reporting success.
fn tick_applies_decision(decision: &AllocatorDecision) -> bool {
    decision.action.is_executable()
}

fn describe_terminal_replays(replays: &[TerminalReplay]) -> String {
    replays
        .iter()
        .map(|replay| format!("{} ({})", replay.key.0, intent_status_label(replay.status)))
        .collect::<Vec<_>>()
        .join(", ")
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

fn tick_terminal(summary: &ExecutionSummary) -> (OperationStatus, Option<String>) {
    if summary.failed == 0 && summary.terminal_failed_skipped == 0 {
        return (OperationStatus::Succeeded, None);
    }

    (
        OperationStatus::Failed,
        Some(format!(
            "tick: {} decision(s) did not apply (performed={} skipped={} failed={} \
             terminal_failed_skipped={} retryable={})",
            summary.failed + summary.terminal_failed_skipped,
            summary.performed,
            summary.skipped,
            summary.failed,
            summary.terminal_failed_skipped,
            summary.retryable
        )),
    )
}

/// Bridge an [`ExecError`] into an `anyhow::Error` for the CLI surface. `ExecError` carries its
/// diagnostic string in the variant, so `Debug` renders the useful context.
fn exec_err(e: ExecError) -> anyhow::Error {
    anyhow::anyhow!("{e:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use fedimint_bip39::Mnemonic;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt as _;
    use wallet_core::{FederationId, Intent, Journal, Msat, Occurrence};

    const FED_A: FederationId = FederationId([0xAA; 32]);
    const FED_B: FederationId = FederationId([0xBB; 32]);
    const FED_C: FederationId = FederationId([0xCC; 32]);
    const FED_D: FederationId = FederationId([0xDD; 32]);

    #[test]
    fn probe_gated_set_is_joined_members_minus_user_approved() {
        // The §5.1.3 funding gate must probe-gate every JOINED member that is not provably
        // user-owned — closing the crash/restore bypass where an agent-created member's 0x09
        // row is still Discovered/absent and an AutoJoined-rows-only set would read it ungated.
        let joined = [FED_A, FED_B, FED_C, FED_D];
        let states = [
            (FED_A, CandidateState::UserApproved), // user-owned -> UNGATED
            (FED_B, CandidateState::AutoJoined),   // agent-owned -> gated
            (FED_C, CandidateState::Discovered),   // crash: joined member, stale row -> gated
            // FED_D: joined member with NO candidate row (0x03-only restore) -> gated
        ];
        let gated = probe_gated_members(joined, states);
        assert!(!gated.contains(&FED_A), "UserApproved member is not probe-gated");
        assert!(gated.contains(&FED_B), "AutoJoined member is probe-gated");
        assert!(
            gated.contains(&FED_C),
            "a joined member with a stale Discovered row (crash window) is probe-gated"
        );
        assert!(
            gated.contains(&FED_D),
            "a joined member with no candidate row (restore) is probe-gated, not ungated"
        );
        // A Discovered candidate that is NOT joined never reaches the gate (not in `joined`).
        let not_joined = probe_gated_members([FED_A], [(FED_B, CandidateState::Discovered)]);
        assert_eq!(not_joined, BTreeSet::from([FED_A]));
    }

    async fn runtime_fixture() -> (Runtime, Arc<FedimintJournal>) {
        let db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(db));
        (Runtime::new(mc, journal.clone(), None, None, None), journal)
    }

    fn direct_inflow_intent(key: IdempotencyKey, to: FederationId, status: IntentStatus) -> Intent {
        Intent {
            idempotency_key: key,
            action: Action::DirectInflow {
                to,
                amount: Msat(100_000),
                fee_cap: Msat(1_000),
            },
            max_fee: Some(Msat(1_000)),
            status,
            reason: ReasonCode::UserInitiated,
            actor: Actor::User,
            created_at_ms: 0,
        }
    }

    fn direct_inflow_record(
        key: IdempotencyKey,
        to: FederationId,
        phase: MovePhase,
        outcome: Option<&str>,
    ) -> MoveRecord {
        MoveRecord {
            key,
            from: None,
            to,
            amount: Msat(100_000),
            fee_cap: Msat(1_000),
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: false,
            invoice: Some(Invoice("lnbc1ptest".into())),
            recv_op: Some(crate::types::OperationId([0x07; 32])),
            send_op: None,
            phase,
            outcome: outcome.map(str::to_string),
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
        }
    }

    fn tick_move_decision(key: &str, from: FederationId, to: FederationId) -> AllocatorDecision {
        AllocatorDecision {
            action: Action::Move {
                from,
                to,
                amount: Msat(100_000),
                fee_cap: Msat(1_000),
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey(key.to_string()),
        }
    }

    fn tick_evacuate_decision(
        key: &str,
        from: FederationId,
        to: FederationId,
    ) -> AllocatorDecision {
        AllocatorDecision {
            action: Action::Evacuate {
                from,
                to,
                amount: Msat(100_000),
                fee_cap: Msat(1_000),
            },
            reason: ReasonCode::ShutdownNotice,
            occurrence: Occurrence(0),
            idempotency_key: IdempotencyKey(key.to_string()),
        }
    }

    #[test]
    fn direct_inflow_key_is_deterministic_and_param_sensitive() {
        let to = FederationId([0xCD; 32]);
        let base = direct_inflow_key(&to, Msat(100_000), Msat(1_100_000), Occurrence(0));
        // Same inputs -> same key: a re-run of the same request dedups (no second invoice).
        assert_eq!(
            base,
            direct_inflow_key(&to, Msat(100_000), Msat(1_100_000), Occurrence(0))
        );
        // Each parameter participates, so a genuinely different inflow gets a distinct key.
        assert_ne!(
            base,
            direct_inflow_key(&to, Msat(100_001), Msat(1_100_000), Occurrence(0))
        );
        assert_ne!(
            base,
            direct_inflow_key(&to, Msat(100_000), Msat(1_100_001), Occurrence(0))
        );
        assert_ne!(
            base,
            direct_inflow_key(&to, Msat(100_000), Msat(1_100_000), Occurrence(1))
        );
        assert_ne!(
            base,
            direct_inflow_key(
                &FederationId([0xCE; 32]),
                Msat(100_000),
                Msat(1_100_000),
                Occurrence(0)
            )
        );
        // The key embeds the destination hex + the three numeric params, in order.
        assert_eq!(
            base.0,
            format!("direct-inflow:{}:100000:1100000:0", to.to_hex())
        );
    }

    #[test]
    fn move_key_is_deterministic_and_param_sensitive() {
        let base = move_key(&FED_A, &FED_B, Msat(50_000), Msat(2_000), Occurrence(0));
        // Same inputs -> same key: a re-run of the same move dedups (no re-mint / no re-pay).
        assert_eq!(
            base,
            move_key(&FED_A, &FED_B, Msat(50_000), Msat(2_000), Occurrence(0))
        );
        // Every parameter participates, so a genuinely different move gets a distinct key.
        assert_ne!(
            base,
            move_key(&FED_B, &FED_B, Msat(50_000), Msat(2_000), Occurrence(0)),
            "swapping the source federation must change the key"
        );
        assert_ne!(
            base,
            move_key(&FED_A, &FED_A, Msat(50_000), Msat(2_000), Occurrence(0)),
            "changing the destination must change the key"
        );
        assert_ne!(
            base,
            move_key(&FED_A, &FED_B, Msat(50_001), Msat(2_000), Occurrence(0)),
            "a different amount must not dedup to the old move"
        );
        assert_ne!(
            base,
            move_key(&FED_A, &FED_B, Msat(50_000), Msat(2_001), Occurrence(0))
        );
        assert_ne!(
            base,
            move_key(&FED_A, &FED_B, Msat(50_000), Msat(2_000), Occurrence(1))
        );
        // The key embeds both federation hexes + the three numeric params, in order.
        assert_eq!(
            base.0,
            format!("move:{}:{}:50000:2000:0", FED_A.to_hex(), FED_B.to_hex())
        );
    }

    #[tokio::test]
    async fn await_move_done_retry_honors_expected_fed() {
        let (runtime, journal) = runtime_fixture().await;
        let key = IdempotencyKey("done-direct-inflow".into());
        journal
            .upsert(&direct_inflow_intent(
                key.clone(),
                FED_A,
                IntentStatus::Done,
            ))
            .await
            .expect("upsert intent");
        journal
            .put_move(&direct_inflow_record(
                key.clone(),
                FED_A,
                MovePhase::Settled,
                None,
            ))
            .await
            .expect("put move");

        let err = runtime
            .await_move(&key, Some(FED_B))
            .await
            .expect_err("wrong fed guard must fail");
        assert!(err.to_string().contains("receives into"));
        assert_eq!(
            runtime.await_move(&key, Some(FED_A)).await.expect("done"),
            FinalizeOutcome::Done
        );
    }

    #[tokio::test]
    async fn await_move_failed_retry_returns_failed_status() {
        let (runtime, journal) = runtime_fixture().await;
        let key = IdempotencyKey("failed-direct-inflow".into());
        journal
            .upsert(&direct_inflow_intent(
                key.clone(),
                FED_A,
                IntentStatus::Failed,
            ))
            .await
            .expect("upsert intent");
        journal
            .put_move(&direct_inflow_record(
                key.clone(),
                FED_A,
                MovePhase::Failed,
                Some("receive invoice expired before payment"),
            ))
            .await
            .expect("put move");

        assert_eq!(
            runtime.await_move(&key, None).await.expect("failed retry"),
            FinalizeOutcome::Failed("receive invoice expired before payment".into())
        );
    }

    #[tokio::test]
    async fn direct_inflow_repairs_awaiting_over_failed_record_and_hides_dead_invoice() {
        let (runtime, journal) = runtime_fixture().await;
        let to = FED_A;
        let amount = Msat(100_000);
        let fee_cap = Msat(1_000);
        let occurrence = Occurrence(0);
        let key = direct_inflow_key(&to, amount, fee_cap, occurrence);

        // Simulate a crash inside `await_move`: the record was written `Failed` (its invoice now
        // dead) but the intent CAS to `Failed` never landed, leaving the intent stuck `Awaiting`.
        journal
            .upsert(&direct_inflow_intent(
                key.clone(),
                to,
                IntentStatus::Awaiting,
            ))
            .await
            .expect("upsert intent");
        journal
            .put_move(&direct_inflow_record(
                key.clone(),
                to,
                MovePhase::Failed,
                Some("receive invoice expired before payment"),
            ))
            .await
            .expect("put move");

        let outcome = runtime
            .direct_inflow(to, amount, fee_cap, occurrence)
            .await
            .expect("direct_inflow");

        // The stuck `Awaiting` intent is repaired to `Failed`, so the CLI (which gates stdout on a
        // non-`Failed` status) never surfaces the dead invoice as payable.
        assert_eq!(outcome.status, Some(IntentStatus::Failed));
        assert_eq!(
            journal.get(&key).await.expect("get").map(|i| i.status),
            Some(IntentStatus::Failed)
        );
    }

    #[tokio::test]
    async fn tick_bails_when_a_pinned_fed_cannot_be_probed() {
        // The fixture has NO joined federations, so `probe_all` yields an empty batch and any
        // pinned fed is necessarily absent from the snapshot. A tick pinning a spending fed must
        // therefore fail LOUDLY (so a scheduler gating on the exit code never mistakes an
        // un-evaluated, explicitly-pinned rebalance for success) rather than report `decisions:
        // none` and exit 0. An UNPINNED (fully auto) tick over the same empty batch is a no-op, not
        // an error — auto designation degrades safely.
        let (runtime, _journal) = runtime_fixture().await;
        let pinned = TickPolicy {
            spending_fed: Some(FED_A),
            ..TickPolicy::default()
        };
        let err = runtime
            .tick(&pinned)
            .await
            .expect_err("a pinned fed that cannot be probed must fail the tick");
        assert!(err.to_string().contains("failed to probe"), "{err}");

        let report = runtime
            .tick(&TickPolicy::default())
            .await
            .expect("an all-auto tick over an empty fed set is a clean no-op");
        assert!(report.decisions.is_empty());
    }

    #[tokio::test]
    async fn tick_route_preflight_skips_existing_move_intents() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-existing", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("upsert existing move intent");

        let problem = runtime
            .first_move_route_problem(std::slice::from_ref(&decision))
            .await;

        assert!(
            problem.is_none(),
            "same-key replay must be left to apply/executor so it can reuse the stored intent and cached gateway"
        );
    }

    #[tokio::test]
    async fn tick_route_preflight_checks_fresh_move_intents() {
        let (runtime, _journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-fresh", FED_A, FED_B);

        let problem = runtime
            .first_move_route_problem(std::slice::from_ref(&decision))
            .await
            .expect("fresh move should be preflighted against executor gateway selection");

        assert_eq!(problem.from, FED_A);
        assert_eq!(problem.to, FED_B);
        assert_eq!(problem.mark_unavailable, FED_B);
    }

    #[tokio::test]
    async fn tick_route_preflight_skips_existing_evacuate_intents() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_evacuate_decision("evac-existing", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("upsert existing evacuate intent");

        let problem = runtime
            .first_move_route_problem(std::slice::from_ref(&decision))
            .await;

        assert!(
            problem.is_none(),
            "same-key evacuate replay must be left to apply/executor so it can reuse the stored intent and cached gateway"
        );
    }

    #[tokio::test]
    async fn tick_route_preflight_checks_fresh_evacuate_intents() {
        let (runtime, _journal) = runtime_fixture().await;
        let decision = tick_evacuate_decision("evac-fresh", FED_A, FED_B);

        let problem = runtime
            .first_move_route_problem(std::slice::from_ref(&decision))
            .await
            .expect("fresh evacuate should be preflighted against executor gateway selection");

        assert_eq!(problem.from, FED_A);
        assert_eq!(problem.to, FED_B);
        assert_eq!(problem.mark_unavailable, FED_B);
    }

    #[test]
    fn evacuation_source_route_failure_revises_destination() {
        let problem = source_route_problem(
            SendRouteKind::Evacuate,
            FED_A,
            FED_B,
            GatewayUrl("https://gw.example".into()),
            "not connected".into(),
        );

        assert_eq!(problem.from, FED_A);
        assert_eq!(problem.to, FED_B);
        assert_eq!(problem.mark_unavailable, FED_B);
        assert!(problem.evacuation_source_route);
        assert!(
            problem.error.contains("source gateway validation failed"),
            "{}",
            problem.error
        );
    }

    #[test]
    fn move_source_route_failure_still_revises_destination() {
        let problem = source_route_problem(
            SendRouteKind::Move,
            FED_A,
            FED_B,
            GatewayUrl("https://gw.example".into()),
            "not connected".into(),
        );

        assert_eq!(problem.mark_unavailable, FED_B);
        assert!(!problem.evacuation_source_route);
    }

    #[test]
    fn scan_route_picks_the_first_gateway_serving_the_whole_route() {
        // §15.6. First gateway dead (serves neither), second serves both -> routable via #1.
        assert_eq!(
            scan_route(&[(false, false), (true, true)]),
            RouteScan::Routable(1)
        );
        // A gateway serving ONLY the destination is skipped when the source needs it; with no
        // other gateway serving the source the route is source-unserved (re-target the dest).
        assert_eq!(scan_route(&[(true, false)]), RouteScan::SourceUnserved(0));
        // Serves-only-dest, then a gateway serving both -> routable via the second.
        assert_eq!(
            scan_route(&[(true, false), (true, true)]),
            RouteScan::Routable(1)
        );
        // No gateway serves the destination at all, and an empty candidate set.
        assert_eq!(
            scan_route(&[(false, false)]),
            RouteScan::DestinationUnserved
        );
        assert_eq!(scan_route(&[]), RouteScan::DestinationUnserved);
        // A receive-only route (source always "served") is routable on the first dest-ok gateway.
        assert_eq!(scan_route(&[(true, true)]), RouteScan::Routable(0));
    }

    #[tokio::test]
    async fn perform_timeout_leaves_a_stalled_intent_pending() {
        use wallet_core::MemJournal;

        // §15.9. An executor whose `perform` never resolves (a stalled gateway long-poll).
        struct NeverResolves;
        #[async_trait]
        impl Executor for NeverResolves {
            async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
                std::future::pending::<()>().await;
                unreachable!("pending() never resolves")
            }
        }

        let journal = MemJournal::new();
        let decision = tick_move_decision("stall", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("upsert pending intent");

        // Wrap the never-resolving executor with a short deadline and drive it via reconcile.
        let executor = TimeoutExecutor::new(NeverResolves, Some(Duration::from_millis(50)));
        let summary = wallet_core::reconcile(&journal, &executor).await;

        // The perform timed out: counted as a (retryable) failure, NOT performed, and the intent
        // is left Pending for the next reconcile — never resurrected to a terminal status.
        assert_eq!(summary.performed, 0);
        assert_eq!(summary.failed, 1);
        assert_eq!(
            journal
                .get(&decision.idempotency_key)
                .await
                .expect("get")
                .map(|i| i.status),
            Some(IntentStatus::Pending)
        );
    }

    #[tokio::test]
    async fn tick_rejects_already_terminal_same_occurrence_replays() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-stale", FED_A, FED_B);
        let mut done = Intent::from_decision(&decision, Actor::User, 0);
        done.status = IntentStatus::Done;
        journal.upsert(&done).await.expect("upsert done intent");

        let replays = runtime
            .terminal_replayed_executable_decisions(std::slice::from_ref(&decision))
            .await
            .expect("freshness scan");
        assert_eq!(
            replays,
            vec![TerminalReplay {
                key: decision.idempotency_key.clone(),
                status: IntentStatus::Done,
            }]
        );

        let err = runtime
            .ensure_fresh_tick_decisions(std::slice::from_ref(&decision), Occurrence(0))
            .await
            .expect_err("same-occurrence terminal replay must fail a tick");
        let msg = err.to_string();
        assert!(msg.contains("already-terminal"), "{msg}");
        assert!(msg.contains("fresh --occurrence"), "{msg}");
        assert!(msg.contains("move-stale"), "{msg}");
    }

    #[tokio::test]
    async fn tick_rejects_failed_same_occurrence_replays() {
        // A `Failed` intent is terminal in `apply` (skipped as `terminal_failed_skipped`, which the
        // CLI turns into a non-zero tick exit). The freshness scan must flag it too so `tick` fails
        // early with the "advance --occurrence" remedy and `status` surfaces the same signal.
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-failed", FED_A, FED_B);
        let mut failed = Intent::from_decision(&decision, Actor::User, 0);
        failed.status = IntentStatus::Failed;
        journal.upsert(&failed).await.expect("upsert failed intent");

        let replays = runtime
            .terminal_replayed_executable_decisions(std::slice::from_ref(&decision))
            .await
            .expect("freshness scan");
        assert_eq!(
            replays,
            vec![TerminalReplay {
                key: decision.idempotency_key.clone(),
                status: IntentStatus::Failed,
            }]
        );

        let err = runtime
            .ensure_fresh_tick_decisions(std::slice::from_ref(&decision), Occurrence(0))
            .await
            .expect_err("same-occurrence terminal Failed replay must fail a tick");
        let msg = err.to_string();
        assert!(msg.contains("already-terminal"), "{msg}");
        assert!(msg.contains("fresh --occurrence"), "{msg}");
        assert!(msg.contains("move-failed"), "{msg}");
    }

    #[tokio::test]
    async fn tick_freshness_allows_pending_same_occurrence_retries() {
        let (runtime, journal) = runtime_fixture().await;
        let decision = tick_move_decision("move-pending", FED_A, FED_B);
        journal
            .upsert(&Intent::from_decision(&decision, Actor::User, 0))
            .await
            .expect("upsert pending intent");

        assert!(runtime
            .terminal_replayed_executable_decisions(std::slice::from_ref(&decision))
            .await
            .expect("freshness scan")
            .is_empty());
        runtime
            .ensure_fresh_tick_decisions(std::slice::from_ref(&decision), Occurrence(0))
            .await
            .expect("pending same-occurrence tick remains retryable");
    }

    #[test]
    fn tick_terminal_marks_apply_failures_as_failed() {
        let clean = ExecutionSummary {
            performed: 2,
            skipped: 1,
            failed: 0,
            terminal_failed_skipped: 0,
            retryable: 0,
        };
        assert_eq!(tick_terminal(&clean), (OperationStatus::Succeeded, None));

        let retryable = ExecutionSummary {
            performed: 1,
            skipped: 0,
            failed: 1,
            terminal_failed_skipped: 0,
            retryable: 1,
        };
        let (status, error) = tick_terminal(&retryable);
        assert_eq!(status, OperationStatus::Failed);
        let error = error.expect("failed tick carries diagnostic");
        assert!(error.contains("failed=1"), "{error}");
        assert!(error.contains("retryable=1"), "{error}");

        let terminal_skip = ExecutionSummary {
            performed: 0,
            skipped: 1,
            failed: 0,
            terminal_failed_skipped: 1,
            retryable: 0,
        };
        let (status, error) = tick_terminal(&terminal_skip);
        assert_eq!(status, OperationStatus::Failed);
        assert!(error
            .expect("terminal skip carries diagnostic")
            .contains("terminal_failed_skipped=1"));
    }

    /// §8: the operator verbs stamp `Actor::User` + `ReasonCode::UserInitiated` on the intent
    /// they journal (replacing the old hardcoded dummy reason). With no federation joined the
    /// two-leg drive fails retryably and leaves the intent `Pending`, but the journaled intent
    /// already carries the ledger identity.
    #[tokio::test]
    async fn user_move_intent_carries_user_actor_and_reason() {
        let (runtime, journal) = runtime_fixture().await;
        let outcome = runtime
            .do_move(
                FED_A,
                FED_B,
                Msat(10_000),
                Msat(500),
                Occurrence(0),
                ReasonCode::UserInitiated,
                Actor::User,
            )
            .await
            .expect("do_move returns even when the drive is retryable");
        let intent = journal
            .get(&outcome.key)
            .await
            .expect("get")
            .expect("the move intent is journaled");
        assert_eq!(intent.actor, Actor::User);
        assert_eq!(intent.reason, ReasonCode::UserInitiated);
    }

    // ---- phase 5 §5.0.8: the pure probe pieces --------------------------------------

    /// A leg move record with the given phase/artifacts, for the classification table.
    fn probe_leg_rec(phase: MovePhase, invoice: bool, send_op: bool) -> MoveRecord {
        MoveRecord {
            key: IdempotencyKey("move:leg".into()),
            from: Some(FED_A),
            to: FED_B,
            amount: Msat(20_000),
            fee_cap: Msat(10_000),
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: true,
            invoice: invoice.then(|| Invoice("lnbc1pexample".into())),
            recv_op: invoice.then_some(crate::types::OperationId([0x07; 32])),
            send_op: send_op.then_some(crate::types::OperationId([0x09; 32])),
            phase,
            outcome: None,
            preimage: None,
            receive_fee_quoted: Some(Msat(300)),
            send_fee_quoted: Some(Msat(200)),
        }
    }

    #[test]
    fn probe_out_fee_cap_never_allows_return_debit_above_delivered_delta() {
        assert_eq!(
            probe_out_fee_cap(Msat(19_500), Msat(15_000), Msat(10_000)),
            Msat(4_500),
            "leg OUT can spend at most delivered_in - out_net in fees"
        );
        assert_eq!(
            probe_out_fee_cap(Msat(30_000), Msat(15_000), Msat(10_000)),
            Msat(10_000),
            "the operator's leg fee cap still bounds the return leg"
        );
        assert_eq!(
            probe_out_fee_cap(Msat(15_000), Msat(16_000), Msat(10_000)),
            Msat(0),
            "a corrupt oversized out_net cannot mint extra fee budget"
        );
    }

    #[test]
    fn classification_table_demotes_only_candidate_refused_legs() {
        use ProbeLeg::{In, Out};
        let rejected = "lnv2 send deterministically rejected the invoice: FederationNotSupported";
        // Terminal settlement phases: Stranded/Refunded never demote; a terminal FAILED
        // send demotes only when the payer is the candidate (leg OUT).
        for leg in [In, Out] {
            let rec = probe_leg_rec(MovePhase::Stranded, true, true);
            assert_eq!(
                classify_leg_failure(leg, Some(&rec), "x"),
                LegFault::UmbrellaOnly
            );
            let rec = probe_leg_rec(MovePhase::Refunded, true, true);
            assert_eq!(
                classify_leg_failure(leg, Some(&rec), "x"),
                LegFault::UmbrellaOnly
            );
        }
        let failed = probe_leg_rec(MovePhase::Failed, true, true);
        assert_eq!(
            classify_leg_failure(Out, Some(&failed), rejected),
            LegFault::Candidate
        );
        assert_eq!(
            classify_leg_failure(In, Some(&failed), rejected),
            LegFault::UmbrellaOnly,
            "leg IN's payer is the SOURCE — its send failure never demotes the candidate"
        );

        // CreateInvoice step (no artifacts): hosted on the destination — the candidate
        // for leg IN only, and only for a non-local error.
        assert_eq!(
            classify_leg_failure(In, None, "the federation refused to mint"),
            LegFault::Candidate
        );
        assert_eq!(
            classify_leg_failure(In, None, "fee over cap (receive side exceeds fee_cap)"),
            LegFault::UmbrellaOnly,
            "§5.0.2: a parametric refusal must not demote"
        );
        assert_eq!(
            classify_leg_failure(
                In,
                None,
                "gateway receive fee changed between quote and mint; re-run"
            ),
            LegFault::UmbrellaOnly,
            "the §15.7 TOCTOU refusal is gateway-timed, not candidate dishonesty"
        );
        assert_eq!(
            classify_leg_failure(
                In,
                None,
                "destination would exceed the per-fed cap (999+20000 > 1000 msat) for federation x"
            ),
            LegFault::UmbrellaOnly,
            "the ADR-0018 cap refusal is local policy, not candidate dishonesty"
        );
        assert_eq!(
            classify_leg_failure(Out, None, "anything at all"),
            LegFault::UmbrellaOnly,
            "leg OUT's mint is hosted on the SOURCE"
        );

        // Pay step (invoice, no send op): hosted on the source of the move — the
        // candidate for leg OUT only, and only for a non-local error.
        let at_pay = probe_leg_rec(MovePhase::Invoiced, true, false);
        assert_eq!(
            classify_leg_failure(Out, Some(&at_pay), rejected),
            LegFault::Candidate
        );
        assert_eq!(
            classify_leg_failure(
                Out,
                Some(&at_pay),
                "fee over cap: the fixed receive quote 900 msat alone exceeds fee_cap 500 msat"
            ),
            LegFault::UmbrellaOnly
        );
        assert_eq!(
            classify_leg_failure(
                Out,
                Some(&at_pay),
                "move invoice expired before the send leg could pay it (move x); re-run"
            ),
            LegFault::UmbrellaOnly,
            "the §15.4 expiry belt is a timing artifact"
        );
        for sdk_rejection in [
            "lnv2 send deterministically rejected the invoice: Gateway fee exceeds the allowed limit",
            "lnv2 send deterministically rejected the invoice: Gateway expiration time exceeds the allowed limit",
            "lnv2 send deterministically rejected the invoice: Invoice has expired",
        ] {
            assert_eq!(
                classify_leg_failure(Out, Some(&at_pay), sdk_rejection),
                LegFault::UmbrellaOnly,
                "{sdk_rejection} is gateway-parametric/timing, not candidate dishonesty"
            );
        }
        assert_eq!(
            classify_leg_failure(In, Some(&at_pay), rejected),
            LegFault::UmbrellaOnly,
            "leg IN's pay is hosted on the SOURCE"
        );

        // Both artifacts present without a terminal phase: an await-step oddity —
        // genuinely unclear attribution never demotes.
        let odd = probe_leg_rec(MovePhase::Sending, true, true);
        assert_eq!(
            classify_leg_failure(Out, Some(&odd), "x"),
            LegFault::UmbrellaOnly
        );
    }

    #[test]
    fn non_candidate_signatures_match_an_emit_site() {
        // `is_known_non_candidate_error` matches free text emitted by the executors. If
        // an emit site rewords its diagnostic without updating the signature list, a
        // local/gateway fault on a candidate-hosted step silently turns into a wrongful
        // demotion — pin every signature to a source that still emits it.
        let emitting_sources = [
            include_str!("executor.rs"),
            include_str!("../../wallet-core/src/executor.rs"),
        ];
        for sig in NON_CANDIDATE_SIGNATURES {
            assert!(
                emitting_sources.iter().any(|src| src.contains(sig)),
                "signature {sig:?} no longer appears in any emitting source; update \
                 NON_CANDIDATE_SIGNATURES together with the reworded diagnostic"
            );
        }
    }

    #[test]
    fn probe_cost_is_the_source_net_outflow() {
        let settled_in = probe_leg_rec(MovePhase::Settled, true, true);
        let mut settled_out = probe_leg_rec(MovePhase::Settled, true, true);
        settled_out.amount = Msat(15_000);
        // Clean pass: (20_000 + 300 + 200) − 15_000 = fees + residue.
        assert_eq!(
            probe_cost(Some(&settled_in), Some(&settled_out)),
            Some(Msat(5_500))
        );
        // Leg OUT never redeemed: the WHOLE delivered amount + fees is the exposure.
        assert_eq!(probe_cost(Some(&settled_in), None), Some(Msat(20_500)));
        let failed_out = probe_leg_rec(MovePhase::Failed, true, true);
        assert_eq!(
            probe_cost(Some(&settled_in), Some(&failed_out)),
            Some(Msat(20_500))
        );
        // A STRANDED leg IN still debited the source in full.
        let stranded_in = probe_leg_rec(MovePhase::Stranded, true, true);
        assert_eq!(probe_cost(Some(&stranded_in), None), Some(Msat(20_500)));
        // No settled send on leg IN = no money left the source.
        let refunded_in = probe_leg_rec(MovePhase::Refunded, true, true);
        assert_eq!(probe_cost(Some(&refunded_in), None), None);
        assert_eq!(probe_cost(None, None), None);
    }

    #[test]
    fn no_sweep_guard_requires_baseline_plus_delta() {
        // Baseline 100, delta 20: an EXACTLY untouched candidate (120) passes…
        assert!(no_sweep_ok(Msat(120), Msat(100), Msat(20)));
        // …a plain 15-sat spend (105) fails — still exceeds the delta alone (a delta-only
        // check would be fooled) yet not baseline + delta…
        assert!(!no_sweep_ok(Msat(105), Msat(100), Msat(20)));
        assert!(!no_sweep_ok(Msat(119), Msat(100), Msat(20)));
        // …and SPEND-THEN-REPLENISH (spend 15, receive 20 unrelated -> 125) also fails:
        // `>=` would pass, but 15 sats of a redemption would now be pre-existing funds.
        assert!(!no_sweep_ok(Msat(125), Msat(100), Msat(20)));
    }

    #[test]
    fn probe_local_faults_reject_self_probe_poor_source_and_capped_candidate() {
        let ok = probe_local_faults(
            FED_B,
            FED_A,
            Msat(30_000),
            Msat(0),
            Msat(20_000),
            Msat(10_000),
            Some(Msat(1_000_000)),
        );
        assert_eq!(ok, Ok(()));
        // Self-probe.
        let err = probe_local_faults(
            FED_A,
            FED_A,
            Msat(30_000),
            Msat(0),
            Msat(20_000),
            Msat(10_000),
            None,
        )
        .expect_err("self-probe");
        assert!(err.contains("from itself"), "{err}");
        // Source short of amount + leg fee cap.
        let err = probe_local_faults(
            FED_B,
            FED_A,
            Msat(29_999),
            Msat(0),
            Msat(20_000),
            Msat(10_000),
            None,
        )
        .expect_err("poor source");
        assert!(err.contains("insufficient source balance"), "{err}");
        // Candidate without cap room for the probe amount.
        let err = probe_local_faults(
            FED_B,
            FED_A,
            Msat(30_000),
            Msat(990_000),
            Msat(20_000),
            Msat(10_000),
            Some(Msat(1_000_000)),
        )
        .expect_err("capped candidate");
        assert!(err.contains("insufficient candidate cap room"), "{err}");
        // Source ALREADY above the cap: leg OUT would breach it -> refuse before any spend
        // (a guaranteed inconclusive probe otherwise). Source has amount + fee headroom and
        // the candidate has room, so ONLY the over-cap source triggers this.
        let err = probe_local_faults(
            FED_B,
            FED_A,
            Msat(1_100_000),
            Msat(0),
            Msat(20_000),
            Msat(10_000),
            Some(Msat(1_000_000)),
        )
        .expect_err("over-cap source");
        assert!(err.contains("already above the per-fed cap"), "{err}");
        // No hard cap disables the room check.
        assert_eq!(
            probe_local_faults(
                FED_B,
                FED_A,
                Msat(30_000),
                Msat(990_000),
                Msat(20_000),
                Msat(10_000),
                None,
            ),
            Ok(())
        );
    }

    #[test]
    fn occurrence_and_umbrella_key_derive_from_the_session_nonce() {
        let occ = occurrence_from_nonce("000000000000002a0000000000000000").expect("valid nonce");
        assert_eq!(occ, Occurrence(42));
        occurrence_from_nonce("shorty").expect_err("too-short nonce");
        occurrence_from_nonce("zzzzzzzzzzzzzzzz0000000000000000").expect_err("non-hex nonce");
        assert_eq!(
            probe_umbrella_key(&FED_A, "0011").0,
            format!("probe:{}:0011", FED_A.to_hex())
        );
    }

    #[tokio::test]
    async fn direct_inflow_repairs_awaiting_over_settled_record_to_done() {
        let (runtime, journal) = runtime_fixture().await;
        let to = FED_A;
        let amount = Msat(100_000);
        let fee_cap = Msat(1_000);
        let occurrence = Occurrence(0);
        let key = direct_inflow_key(&to, amount, fee_cap, occurrence);

        // Simulate the symmetric crash inside `await_move`: the record was written `Settled`,
        // but the intent CAS to `Done` never landed.
        journal
            .upsert(&direct_inflow_intent(
                key.clone(),
                to,
                IntentStatus::Awaiting,
            ))
            .await
            .expect("upsert intent");
        journal
            .put_move(&direct_inflow_record(
                key.clone(),
                to,
                MovePhase::Settled,
                None,
            ))
            .await
            .expect("put move");

        let outcome = runtime
            .direct_inflow(to, amount, fee_cap, occurrence)
            .await
            .expect("direct_inflow");

        assert_eq!(outcome.status, Some(IntentStatus::Done));
        assert_eq!(
            journal.get(&key).await.expect("get").map(|i| i.status),
            Some(IntentStatus::Done)
        );
    }

    #[test]
    fn probe_gate_candidate_ids_covers_auto_joined_and_skipped_rows() {
        use fedimint_core::invite_code::InviteCode;
        use fedimint_core::util::SafeUrl;
        use fedimint_core::PeerId;
        use std::str::FromStr as _;

        fn invite(id: FederationId) -> InviteCode {
            let fed_id =
                fedimint_core::config::FederationId::from_str(&id.to_hex()).expect("valid fed id");
            InviteCode::new(
                SafeUrl::parse("https://probe-gate.example").expect("valid url"),
                PeerId::from(0),
                fed_id,
                None,
            )
        }
        fn row(id: FederationId, state: CandidateState) -> crate::CandidateRecord {
            crate::CandidateRecord {
                id,
                invite: invite(id),
                source: wallet_core::DiscoverySource::Manual,
                discovered_at_ms: 0,
                structural: crate::StructuralOutcome::Passed,
                structural_checked_at_ms: 0,
                state,
                updated_at_ms: 0,
            }
        }

        let auto = FederationId([0x01; 32]);
        let discovered = FederationId([0x02; 32]);
        let skipped = FederationId([0x03; 32]);
        let report = CandidateListReport {
            candidates: vec![
                (auto, row(auto, CandidateState::AutoJoined)),
                (discovered, row(discovered, CandidateState::Discovered)),
            ],
            skipped_ids: BTreeSet::from([skipped]),
            skipped_rows: 1,
            skipped_unidentified: 0,
        };

        // A poison-skipped id joins the probe-gate set so a later Passed probe can clear the
        // concurrent cap; a plain `Discovered` row (no partition) never counts.
        let ids = probe_gate_candidate_ids(&report);
        assert_eq!(ids, BTreeSet::from([auto, skipped]));
        assert!(!ids.contains(&discovered));
    }

    #[test]
    fn threshold_for_endpoints_handles_zero_without_underflow() {
        assert_eq!(threshold_for_endpoints(0), 0);
        assert_eq!(threshold_for_endpoints(1), 1);
        assert_eq!(threshold_for_endpoints(4), 3);
    }
}
