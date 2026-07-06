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

use crate::executor::FedimintExecutor;
use crate::journal::FedimintJournal;
use crate::move_protocol::{MovePhase, MoveRecord};
use crate::multi_client::{MultiClient, ReceiveState};
use crate::probe::{assemble_facts, assemble_status, FedimintProbeRunner, ProbeResult};
use crate::tick::{
    build_snapshot, decisions_to_apply, pinned_input_problems, ScoredFed, StatusReport, TickPolicy,
    TickReport,
};
use crate::types::{GatewayUrl, Invoice};
use async_trait::async_trait;
use fedimint_core::runtime;
use std::time::Duration;
use std::{collections::BTreeSet, sync::Arc};
use wallet_core::{
    score, Action, Actor, AllocatorDecision, AllocatorSnapshot, ExecError, ExecutionSummary,
    Executor, FederationId, IdempotencyKey, Intent, IntentStatus, Journal, Msat, Occurrence,
    OperationStatus, PerformOutcome, ReasonCode, ScorerPolicy,
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
    pub async fn do_move(
        &self,
        from: FederationId,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        occurrence: Occurrence,
    ) -> anyhow::Result<MoveOutcome> {
        let key = move_key(&from, &to, amount, fee_cap, occurrence);
        let decision = AllocatorDecision {
            action: Action::Move {
                from,
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

        let plan = self.plan_tick(policy, &ScorerPolicy::default()).await;
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
        let plan = self.plan_tick(policy, &scorer_policy).await;
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
        let scored = plan
            .raw_probes
            .iter()
            .map(|(id, probe)| ScoredFed {
                id: *id,
                verdict: score(&assemble_facts(probe, *id), &scorer_policy),
                status: assemble_status(probe, *id),
            })
            .collect();
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
    async fn plan_tick(&self, policy: &TickPolicy, scorer_policy: &ScorerPolicy) -> TickPlan {
        let raw_probes = self.probe_all().await;
        let mut probes = raw_probes.clone();
        let mut route_revisions = 0usize;
        let mut evacuation_fallback: Option<EvacuationFallback> = None;
        loop {
            let snapshot = build_snapshot(&probes, policy, scorer_policy);
            let decisions = wallet_core::decide(&snapshot, policy.occurrence);
            if let Some(fallback) = &evacuation_fallback {
                let still_trying_evacuation = decisions.iter().any(|d| {
                    matches!(&d.action, Action::Evacuate { from, .. } if *from == fallback.from)
                });
                if !still_trying_evacuation {
                    return fallback.plan.clone();
                }
            }
            let Some(problem) = self.first_move_route_problem(&decisions).await else {
                return TickPlan {
                    raw_probes,
                    probes,
                    snapshot,
                    decisions,
                };
            };

            if problem.evacuation_source_route {
                evacuation_fallback = Some(EvacuationFallback {
                    from: problem.from,
                    plan: TickPlan {
                        raw_probes: raw_probes.clone(),
                        probes: probes.clone(),
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
                return TickPlan {
                    raw_probes,
                    probes,
                    snapshot,
                    decisions,
                };
            }
            route_revisions += 1;
            if route_revisions > probes.len() {
                return TickPlan {
                    raw_probes,
                    probes,
                    snapshot,
                    decisions,
                };
            }
        }
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
            .do_move(FED_A, FED_B, Msat(10_000), Msat(500), Occurrence(0))
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
}
