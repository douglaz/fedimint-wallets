//! [`Runtime`] — the thin async façade the headless frontend drives (spec §9). It owns the
//! shared fedimint I/O (`MultiClient`) + durable journal (`FedimintJournal`) and exposes the
//! three engine verbs `wallet-cli` needs on top of `wallet_core::{apply, reconcile}`:
//!
//! - [`Runtime::direct_inflow`] — journal + drive a `DirectInflow` intent (spec §7): the
//!   executor sizes + cap-checks the receive invoice (§6 fixed point), mints it, persists the
//!   `MoveRecord`, and returns `Awaiting`; we then surface the BOLT11 (the payer is external).
//! - [`Runtime::await_move`] — finalize an `Awaiting` inflow: await its `recv_op`, and on the
//!   `Claimed` state mark the intent `Done` via the journal CAS (spec §9.5).
//! - [`Runtime::reconcile`] — the resume loop (spec §9): rebuild `MoveRecord`s from the op-log
//!   for pending + awaiting intents BEFORE re-driving, re-drive `pending()` only, then report
//!   the still-`Awaiting` set (finalized out-of-band by `await-move` in a one-shot CLI).
//!
//! DirectInflow ONLY in this step: a `Move`/`Evacuate` intent still maps to `Unsupported` in
//! the executor. The `Runtime` holds an optional pinned gateway (⟦D4⟧; devimint's LDK gateway
//! is not auto-registered, runbook §4) that a FRESH move resolves through — a resumed move
//! reuses the gateway already recorded in its `MoveRecord`.

use crate::executor::FedimintExecutor;
use crate::journal::FedimintJournal;
use crate::move_protocol::{MovePhase, MoveRecord};
use crate::multi_client::{MultiClient, ReceiveState};
use crate::types::{GatewayUrl, Invoice};
use std::sync::Arc;
use wallet_core::{
    Action, AllocatorDecision, ExecError, FederationId, IdempotencyKey, IntentStatus, Journal,
    Msat, Occurrence, ReasonCode,
};

/// The result of a [`Runtime::direct_inflow`] call: the intent's key (the durable handle the
/// operator passes to `await-move`), the surfaced BOLT11 to pay (read from the persisted
/// `MoveRecord`, so a re-run returns the SAME invoice — no second mint), and the intent status.
#[derive(Clone, Debug)]
pub struct DirectInflowOutcome {
    pub key: IdempotencyKey,
    pub invoice: Option<Invoice>,
    pub status: Option<IntentStatus>,
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
/// operator can `await-move` each.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileSummary {
    pub performed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub awaiting: usize,
    pub awaiting_keys: Vec<IdempotencyKey>,
}

/// The engine façade over one wallet's shared fedimint clients + journal (spec §9).
pub struct Runtime {
    mc: Arc<MultiClient>,
    journal: Arc<FedimintJournal>,
    pinned_gateway: Option<GatewayUrl>,
}

impl Runtime {
    pub fn new(
        mc: Arc<MultiClient>,
        journal: Arc<FedimintJournal>,
        pinned_gateway: Option<GatewayUrl>,
    ) -> Self {
        Self {
            mc,
            journal,
            pinned_gateway,
        }
    }

    /// A fresh executor sharing this runtime's clients + journal + pinned gateway. Cheap
    /// (`Arc` clones); made per call so each verb gets a `&self`-only executor.
    fn executor(&self) -> FedimintExecutor {
        FedimintExecutor::new(
            self.mc.clone(),
            self.journal.clone(),
            self.pinned_gateway.clone(),
        )
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
            self.executor()
                .validate_direct_inflow_amount(to, amount)
                .await
                .map_err(exec_err)?;
        }
        let decision = AllocatorDecision {
            action: Action::DirectInflow {
                to,
                amount,
                fee_cap,
            },
            // The reason is decision metadata only (never persisted on the Intent), so the
            // exact inflow reason does not matter here.
            reason: ReasonCode::SpendingBelowTarget,
            occurrence,
            idempotency_key: key.clone(),
            requires_auth: false,
        };
        let executor = self.executor();
        let _summary = wallet_core::apply(
            self.journal.as_ref(),
            &executor,
            std::slice::from_ref(&decision),
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
        let exec = wallet_core::reconcile(self.journal.as_ref(), &executor).await;

        // §9.3: surface the Awaiting set so the operator drives `await-move` for each.
        let awaiting = self.journal.awaiting().await.map_err(exec_err)?;
        Ok(ReconcileSummary {
            performed: exec.performed,
            failed: exec.failed,
            skipped: exec.skipped,
            awaiting: awaiting.len(),
            awaiting_keys: awaiting
                .into_iter()
                .map(|intent| intent.idempotency_key)
                .collect(),
        })
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
        (Runtime::new(mc, journal.clone(), None), journal)
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
