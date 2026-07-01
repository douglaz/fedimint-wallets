//! [`FedimintExecutor`] — the async [`wallet_core::Executor`] that turns a journaled
//! `Intent` into real cross-federation ecash movement (spec §7).
//!
//! # Status: COMPILE-ONLY scaffold (step 4b)
//! The PURE pieces this drives — [`fee::gross_up`], [`MovePlan::from_action`],
//! [`next_step`], [`assemble_move_record`] — are golden-tested. `perform` itself is I/O glue
//! over [`MultiClient`] + [`FedimintJournal`]; it is structured faithfully to §7 and both
//! compiles and type-checks against the pinned fedimint API, but is validated live on a quiet
//! machine (the load-contended gate defers devimint). Do not read the absence of a unit test
//! here as untested logic: the decisions live in the pure functions above.
//!
//! # The perform loop (spec §7)
//! `from_action` → `assemble_record` (cached MoveRecord + backfilled op artifacts, so a
//! replayed move REATTACHES instead of re-minting) → loop on [`next_step`]:
//! - `CreateInvoice`: size the invoice via the §6 fixed point, cap-check the receive side,
//!   `receive`, persist; a `DirectInflow` returns `Awaiting` here (its payer is external).
//! - `Pay`: re-quote the send leg, cap-check BOTH legs, `pay` (the client dedups), persist.
//! - `AwaitSettle`: await the send leg (authoritative); on success await the fast receive
//!   claim; a `DirectInflow` returns `Awaiting` (its `recv_op` subscription owns the claim).
//! - `Done`/`Failed`: terminal.

use crate::fee;
use crate::journal::FedimintJournal;
use crate::move_protocol::{
    assemble_move_record, next_step, MoveMeta, MoveParams, MovePhase, MovePlan, MoveRecord,
    MoveRole, MoveStep,
};
use crate::multi_client::{MultiClient, ReceiveState, SendOutcome, SendState};
use crate::types::{GatewayUrl, Invoice};
use async_trait::async_trait;
use lightning_invoice::Bolt11Invoice;
use std::str::FromStr as _;
use std::sync::Arc;
use wallet_core::{ExecError, Executor, FederationId, Intent, Msat, PerformOutcome};

/// How many times to re-quote the federation receive fee at the refined contract amount
/// while sizing the invoice. `receive_fee_quote` is async but [`fee::gross_up`]'s fed-fee
/// closure is sync, so the executor resolves the (contract-amount-dependent) fee with a
/// short async fixed point; a couple of passes converge for any real fee (ppm slope < 1).
const FED_FEE_REQUOTE_PASSES: u32 = 3;

/// The production [`Executor`]: shared, `Send + Sync`, holds `Arc`s to the fedimint I/O
/// (`MultiClient`) and the durable journal (spec §2, `&self` + interior mutability).
pub struct FedimintExecutor {
    mc: Arc<MultiClient>,
    journal: Arc<FedimintJournal>,
}

impl FedimintExecutor {
    pub fn new(mc: Arc<MultiClient>, journal: Arc<FedimintJournal>) -> Self {
        Self { mc, journal }
    }

    /// Rebuild the derived [`MoveRecord`] FIRST (spec §7): merge the journaled cache, the
    /// backfilled op-log artifacts (receive leg on `to`, send leg on `from`), and the plan's
    /// params, so a replayed move reattaches to its existing ops rather than re-minting.
    async fn assemble_record(
        &self,
        intent: &Intent,
        plan: &MovePlan,
    ) -> Result<MoveRecord, ExecError> {
        let cached = self.journal.get_move(&intent.idempotency_key).await?;

        // Backfill both sides: the receive leg lives on `to`, the send leg on `from`. For a
        // single-fed self-move (`from == to`, Phase 1) one client holds both legs, so skip
        // the duplicate scan. `assemble_move_record` filters artifacts to this `move_id`.
        let mut artifacts = self.mc.backfill_ops(&plan.to).await.map_err(retryable)?;
        if let Some(from) = plan.from {
            if from != plan.to {
                artifacts.extend(self.mc.backfill_ops(&from).await.map_err(retryable)?);
            }
        }

        // Pin the gateway (spec §3.1/§4): a resumed move reuses the one already recorded so a
        // crash never reselects a different or non-shared gateway; a fresh move resolves one
        // now (persisted at the first `put_move`).
        let gateway = match &cached {
            Some(rec) => rec.gateway.clone(),
            None => self.resolve_gateway(&plan.to).await?,
        };

        let params = MoveParams {
            key: intent.idempotency_key.clone(),
            from: plan.from,
            to: plan.to,
            amount: plan.amount,
            fee_cap: plan.fee_cap,
            gateway,
            send_required: plan.send_required,
        };
        Ok(assemble_move_record(params, &artifacts, cached))
    }

    /// Resolve a gateway for a move into `to` via the federation's registered lnv2 gateways
    /// (spec §7). Errors `Permanent` when none is available (Phase 1 has no fallback here).
    async fn resolve_gateway(&self, to: &FederationId) -> Result<GatewayUrl, ExecError> {
        self.mc
            .gateways(to)
            .await
            .map_err(retryable)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                ExecError::Permanent(format!(
                    "no lnv2 gateway available to route a move into federation {}",
                    to.to_hex()
                ))
            })
    }

    /// Size the receive invoice via the §6 fixed point and cap-check the receive side ONCE
    /// (spec §7 `CreateInvoice`). The gateway fee comes from `routing_info`; the federation
    /// fee is resolved by a short async fixed point (see [`FED_FEE_REQUOTE_PASSES`]). Returns
    /// the gross invoice amount; the invoice is then fixed (never re-quoted on resume).
    async fn gross_up(&self, rec: &MoveRecord) -> Result<Msat, ExecError> {
        let gateway_fee = self
            .mc
            .receive_gateway_fee(&rec.to, &rec.gateway)
            .await
            .map_err(retryable)?;

        // Quote the federation fee at the net amount, solve, then re-quote at the solved
        // contract amount and re-solve until it stops moving (spec §6 fixed point).
        let mut fed_fee = self
            .mc
            .receive_fee_quote(&rec.to, rec.amount)
            .await
            .map_err(retryable)?;
        let mut grossed = fee::gross_up(rec.amount, gateway_fee, |_contract| fed_fee);
        for _ in 0..FED_FEE_REQUOTE_PASSES {
            let requoted = self
                .mc
                .receive_fee_quote(&rec.to, grossed.contract_amount)
                .await
                .map_err(retryable)?;
            if requoted == fed_fee {
                break;
            }
            fed_fee = requoted;
            grossed = fee::gross_up(rec.amount, gateway_fee, |_contract| fed_fee);
        }

        // Cap-check the receive side alone (spec §6/§7): for a `DirectInflow` this is the
        // whole check; for a `Move` the send leg is re-checked at `Pay`.
        if !fee::total_within_cap(grossed.receive_quote, Msat(0), rec.fee_cap) {
            return Err(ExecError::Permanent(
                "fee over cap (receive side exceeds fee_cap)".into(),
            ));
        }
        Ok(grossed.invoice_amount)
    }
}

#[async_trait]
impl Executor for FedimintExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        // Only `Move`/`DirectInflow` are executable moves; `Evacuate` (Phase 2) and advisory
        // actions map to `None` → `Unsupported` (§7).
        let Some(plan) = MovePlan::from_action(&intent.action) else {
            return Err(ExecError::Unsupported);
        };

        // FIRST: rebuild the record from the intent + backfilled op artifacts, so a replayed
        // move reattaches (no re-quote, no spurious over-cap fail).
        let mut rec = self.assemble_record(intent, &plan).await?;

        loop {
            match next_step(&rec) {
                MoveStep::CreateInvoice => {
                    let invoice_amount = self.gross_up(&rec).await?;
                    let meta = MoveMeta {
                        move_id: rec.key.clone(),
                        role: MoveRole::Receive,
                        from: rec.from,
                        to: rec.to,
                    };
                    let (invoice, recv_op) = self
                        .mc
                        .receive(
                            &rec.to,
                            invoice_amount,
                            Some(rec.gateway.clone()),
                            meta.to_value(),
                        )
                        .await
                        .map_err(retryable)?;
                    rec.invoice = Some(invoice);
                    rec.recv_op = Some(recv_op);
                    rec.phase = MovePhase::Invoiced;
                    self.journal.put_move(&rec).await?;

                    // A `DirectInflow`'s payer is EXTERNAL: surface the invoice, mark the
                    // intent `Awaiting`; the `recv_op` subscription finalizes it (§9.5).
                    if !rec.send_required {
                        return Ok(PerformOutcome::Awaiting);
                    }
                }
                MoveStep::Pay => {
                    let invoice = rec.invoice.clone().ok_or_else(|| {
                        ExecError::Permanent("Pay step reached with no invoice".into())
                    })?;
                    let from = rec.from.ok_or_else(|| {
                        ExecError::Permanent("Pay step reached with no source federation".into())
                    })?;

                    // Re-check the cap NOW (spec §6/§7). The receive cost is recovered
                    // crash-safely from the fixed invoice (`invoice_amount − amount`); the
                    // send fee is re-quoted from the (possibly changed) gateway + federation.
                    let invoice_msat = invoice_amount_msat(&invoice)?;
                    let receive_quote = Msat(invoice_msat.saturating_sub(rec.amount.0));
                    let send_gateway_fee = self
                        .mc
                        .send_gateway_fee(&from, &rec.gateway, &invoice)
                        .await
                        .map_err(retryable)?;
                    let send_tx_fee = self
                        .mc
                        .send_fee_quote(&from, &invoice)
                        .await
                        .map_err(retryable)?;
                    let send_quote = Msat(
                        send_gateway_fee
                            .on(Msat(invoice_msat))
                            .0
                            .saturating_add(send_tx_fee.0),
                    );
                    if !fee::total_within_cap(receive_quote, send_quote, rec.fee_cap) {
                        return Err(ExecError::Permanent("fee over cap".into()));
                    }

                    let meta = MoveMeta {
                        move_id: rec.key.clone(),
                        role: MoveRole::Send,
                        from: rec.from,
                        to: rec.to,
                    };
                    let send_op = match self
                        .mc
                        .pay(&from, invoice, Some(rec.gateway.clone()), meta.to_value())
                        .await
                        .map_err(retryable)?
                    {
                        // All three are the SAME committed send (the client dedups on the
                        // deterministic op-id): reattach, never double-pay (spec §4).
                        SendOutcome::Started(op)
                        | SendOutcome::AlreadyInFlight(op)
                        | SendOutcome::AlreadyPaid(op) => op,
                    };
                    rec.send_op = Some(send_op);
                    rec.phase = MovePhase::Sending;
                    self.journal.put_move(&rec).await?;
                }
                MoveStep::AwaitSettle => {
                    // A `DirectInflow` reaching `AwaitSettle` on resume is still owned by its
                    // `recv_op` subscription (§9.5), not this drive: surface `Awaiting`.
                    if !rec.send_required {
                        return Ok(PerformOutcome::Awaiting);
                    }
                    let from = rec.from.ok_or_else(|| {
                        ExecError::Permanent("AwaitSettle reached with no source federation".into())
                    })?;
                    let send_op = rec.send_op.ok_or_else(|| {
                        ExecError::Permanent("AwaitSettle reached with no send op".into())
                    })?;

                    // The SEND leg is authoritative (A pays → swap → preimage). Await it
                    // first; only on success wait on the now-fast receive claim.
                    match self
                        .mc
                        .await_send(&from, send_op)
                        .await
                        .map_err(retryable)?
                    {
                        SendState::Success(_preimage) => {
                            let recv_op = rec.recv_op.ok_or_else(|| {
                                ExecError::Permanent(
                                    "send settled but the record has no receive op".into(),
                                )
                            })?;
                            match self
                                .mc
                                .await_receive(&rec.to, recv_op)
                                .await
                                .map_err(retryable)?
                            {
                                ReceiveState::Claimed => rec.phase = MovePhase::Settled,
                                ReceiveState::Expired => {
                                    rec.phase = MovePhase::Failed;
                                    rec.outcome =
                                        Some("send settled but receive invoice expired".into());
                                }
                                ReceiveState::Failed(msg) => {
                                    rec.phase = MovePhase::Failed;
                                    rec.outcome = Some(msg);
                                }
                            }
                        }
                        SendState::Refunded => {
                            rec.phase = MovePhase::Refunded;
                            rec.outcome = Some("send refunded".into());
                        }
                        SendState::Failed(msg) => {
                            rec.phase = MovePhase::Failed;
                            rec.outcome = Some(msg);
                        }
                    }
                    self.journal.put_move(&rec).await?;
                }
                MoveStep::Done => return Ok(PerformOutcome::Done),
                MoveStep::Failed => return Err(ExecError::Permanent("move failed".into())),
            }
        }
    }
}

/// Map a transient fedimint/I/O error to [`ExecError::Retryable`] (leave the intent
/// `Pending` so the next `reconcile` retries). Fee-over-cap and unsupported actions are the
/// only `Permanent`/`Unsupported` outcomes, raised explicitly above.
fn retryable(e: anyhow::Error) -> ExecError {
    ExecError::Retryable(e.to_string())
}

/// The gross msat amount of a (fixed) move invoice, recovered by parsing the BOLT11 — the
/// crash-safe input to the send-side cap re-check (spec §7). A malformed/amountless invoice
/// is `Permanent` (it can only come from a corrupt record, not a transient fault).
fn invoice_amount_msat(invoice: &Invoice) -> Result<u64, ExecError> {
    let bolt11 = Bolt11Invoice::from_str(&invoice.0)
        .map_err(|e| ExecError::Permanent(format!("parsing move invoice: {e}")))?;
    bolt11
        .amount_milli_satoshis()
        .ok_or_else(|| ExecError::Permanent("move invoice carries no amount".into()))
}
