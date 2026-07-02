//! [`FedimintExecutor`] — the async [`wallet_core::Executor`] that turns a journaled
//! `Intent` into real cross-federation ecash movement (spec §7).
//!
//! # Status: DirectInflow AND Move are both LIVE (step 4b-live-2)
//! The PURE pieces this drives — [`fee::gross_up`], [`MovePlan::from_action`],
//! [`next_step`], [`assemble_move_record`] — are golden-tested. `perform` itself is I/O glue
//! over [`MultiClient`] + [`FedimintJournal`], structured faithfully to §7. The `DirectInflow`
//! branch (receive-only) is wired end-to-end and driven from `wallet-cli`
//! (`direct-inflow` / `await-move` / `reconcile`, via [`crate::runtime::Runtime`]) against a
//! live devimint federation; its `smoke_directinflow_devimint.sh` asserts the recipient nets
//! EXACTLY the target. The `Move` branch (the cross-federation transfer) now EXECUTES its full
//! two-leg send path — receive on `to`, re-quote + cap-check + `pay` from `from`, await both,
//! settle → `Done` — synchronously (`perform` returns `Done`, never `Awaiting`, for a Move). It
//! is resume-safe: `assemble_record` reattaches a replayed move to its existing invoice/recv_op/
//! send_op (the send op-id is deterministic; a re-`pay` returns `AlreadyInFlight`/`AlreadyPaid`),
//! so a crash never re-mints or re-pays. `Evacuate` (Phase 2) stays `Unsupported`. Do not read
//! the absence of a happy-path unit test here as untested logic: the pure decisions are
//! golden-tested above, and the live two-leg drive is exercised by `smoke_move_devimint.sh`.
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
    assemble_move_record, next_step, Leg, MoveMeta, MoveParams, MovePhase, MovePlan, MoveRecord,
    MoveRole, MoveStep, OpArtifact,
};
use crate::multi_client::{MultiClient, ReceiveState, SendOutcome, SendState};
use crate::types::{GatewayUrl, Invoice};
use async_trait::async_trait;
use lightning_invoice::Bolt11Invoice;
use std::str::FromStr as _;
use std::sync::Arc;
use wallet_core::{ExecError, Executor, FederationId, Intent, Msat, PerformOutcome};

/// Pinned lnv2 requires the gateway-reduced incoming contract to be at least 5 sats
/// (`MINIMUM_INCOMING_CONTRACT_AMOUNT`) before it will mint a receive invoice.
pub const MINIMUM_INCOMING_CONTRACT_MSAT: u64 =
    fedimint_lnv2_common::MINIMUM_INCOMING_CONTRACT_AMOUNT.msats;

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
    /// An explicitly pinned lnv2 gateway (Phase 1 pins the gateway, ⟦D4⟧). When set,
    /// [`Self::resolve_gateway`] uses it for a FRESH move instead of the federation's
    /// registered list — devimint does NOT auto-register its LDK gateway into that list, so
    /// `mc.gateways` is empty there (runbook §4) and the CLI must supply the URL directly. A
    /// RESUMED move ignores this and reuses the gateway already pinned in its `MoveRecord`.
    pinned_gateway: Option<GatewayUrl>,
}

impl FedimintExecutor {
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

    /// Rebuild the derived [`MoveRecord`] for `intent` from the op-log (spec §9.2) and persist
    /// it, so a subsequent `perform` / finalize REATTACHES to the existing invoice + ops instead
    /// of re-minting (the resume-loop backfill, driven by [`crate::runtime::Runtime`]). Returns
    /// the assembled record, or `None` when the intent is not an executable move.
    pub async fn backfill_move_record(
        &self,
        intent: &Intent,
    ) -> Result<Option<MoveRecord>, ExecError> {
        let Some(plan) = MovePlan::from_action(&intent.action) else {
            return Ok(None);
        };
        let had_cached_record = self
            .journal
            .get_move(&intent.idempotency_key)
            .await?
            .is_some();
        let rec = self.assemble_record(intent, &plan).await?;
        if had_cached_record || has_move_artifact(&rec) {
            self.journal.put_move(&rec).await?;
        }
        Ok(Some(rec))
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
        // now (persisted at the first `put_move`). If the cache was lost but the receive-only
        // op already exists, finalization/replay no longer needs the gateway at all: use a
        // local sentinel instead of failing on an empty gateway list.
        let gateway = match gateway_from_cache_or_recovered(
            cached.as_ref(),
            plan,
            &intent.idempotency_key,
            &artifacts,
        ) {
            Some(gateway) => gateway,
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

    /// Resolve a gateway for a FRESH move into `to` (spec §7): the explicitly pinned gateway
    /// wins (⟦D4⟧; devimint's LDK gateway is not auto-registered, so the CLI passes it directly
    /// — runbook §4), else the federation's first registered lnv2 gateway.
    ///
    /// "None available" is `Retryable`, NOT `Permanent`: a resume verb (`reconcile`/`await-move`)
    /// carries no pinned gateway, so re-driving an intent that has none cached must leave it
    /// `Pending` (re-drivable once the operator re-runs `direct-inflow --gateway` to supply one),
    /// never terminally `Failed`. The fresh `direct-inflow` path never hits this — its
    /// `pick_receive_gateway` guarantees a gateway before the runtime is built.
    async fn resolve_gateway(&self, to: &FederationId) -> Result<GatewayUrl, ExecError> {
        if let Some(gateway) = &self.pinned_gateway {
            return Ok(gateway.clone());
        }
        self.mc
            .gateways(to)
            .await
            .map_err(retryable)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                ExecError::Retryable(format!(
                    "no lnv2 gateway available to route a move into federation {} \
                     (pass one explicitly — devimint does not auto-register its LDK gateway)",
                    to.to_hex()
                ))
            })
    }

    /// Size the receive invoice via the §6 fixed point. The gateway fee comes from
    /// `routing_info`; the federation fee is resolved by a short async fixed point (see
    /// [`FED_FEE_REQUOTE_PASSES`]). Callers then apply the lnv2 minimum-contract and fee-cap
    /// checks appropriate to their path.
    async fn quote_receive_gross_up(
        &self,
        to: &FederationId,
        gateway: &GatewayUrl,
        amount: Msat,
    ) -> Result<fee::GrossUp, ExecError> {
        let gateway_fee = self
            .mc
            .receive_gateway_fee(to, gateway)
            .await
            .map_err(retryable)?;

        // Quote the federation fee at the net amount, solve, then re-quote at the solved
        // contract amount and re-solve until it stops moving (spec §6 fixed point).
        let mut fed_fee = self
            .mc
            .receive_fee_quote(to, amount)
            .await
            .map_err(retryable)?;
        let mut grossed = solve_gross_up(amount, gateway_fee, fed_fee)?;
        for _ in 0..FED_FEE_REQUOTE_PASSES {
            let requoted = self
                .mc
                .receive_fee_quote(to, grossed.contract_amount)
                .await
                .map_err(retryable)?;
            if requoted == fed_fee {
                break;
            }
            fed_fee = requoted;
            grossed = solve_gross_up(amount, gateway_fee, fed_fee)?;
        }
        Ok(grossed)
    }

    /// Preflight a fresh CLI `DirectInflow` before it is journaled. This catches the
    /// deterministic lnv2 dust rejection (`AmountTooSmall`) while still letting any existing
    /// pending intent re-drive through `perform`, where the same guard marks it terminal.
    pub async fn validate_direct_inflow_amount(
        &self,
        to: FederationId,
        amount: Msat,
    ) -> Result<(), ExecError> {
        let gateway = self.resolve_gateway(&to).await?;
        let grossed = self.quote_receive_gross_up(&to, &gateway, amount).await?;
        ensure_minimum_incoming_contract(amount, grossed.contract_amount)
    }

    /// Size the receive invoice via the §6 fixed point and cap-check the receive side ONCE
    /// (spec §7 `CreateInvoice`). The gateway fee comes from `routing_info`; the federation
    /// fee is resolved by a short async fixed point (see [`FED_FEE_REQUOTE_PASSES`]). Returns
    /// the gross invoice amount; the invoice is then fixed (never re-quoted on resume).
    async fn gross_up(&self, rec: &MoveRecord) -> Result<Msat, ExecError> {
        let grossed = self
            .quote_receive_gross_up(&rec.to, &rec.gateway, rec.amount)
            .await?;
        ensure_minimum_incoming_contract(rec.amount, grossed.contract_amount)?;

        // Cap-check the receive side alone (spec §6/§7): for a `DirectInflow` this is the
        // whole check; for a `Move` the send leg is re-checked at `Pay`.
        if !fee::total_within_cap(grossed.receive_quote, Msat(0), rec.fee_cap) {
            return Err(ExecError::Permanent(
                "fee over cap (receive side exceeds fee_cap)".into(),
            ));
        }
        Ok(grossed.invoice_amount)
    }

    /// For a cross-federation `Move`, prove the pinned receive gateway also serves the source
    /// federation before minting B's invoice. Without this check a destination-only gateway can
    /// create an invoice that A can never pay through the required shared-gateway direct swap,
    /// leaving the move pending under a bad pinned gateway.
    async fn validate_move_gateway_before_receive(
        &self,
        rec: &MoveRecord,
    ) -> Result<(), ExecError> {
        if !rec.send_required {
            return Ok(());
        }
        let from = rec.from.ok_or_else(|| {
            ExecError::Permanent(
                "Move record requires a send leg but has no source federation".into(),
            )
        })?;
        self.mc
            .validate_gateway(&from, &rec.gateway)
            .await
            .map_err(retryable)
    }
}

#[async_trait]
impl Executor for FedimintExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        // `Evacuate` (Phase 2) and advisory actions map to `None` → `Unsupported` (§7).
        let Some(plan) = MovePlan::from_action(&intent.action) else {
            return Err(ExecError::Unsupported);
        };

        // Step 4b-live-2: BOTH executable move shapes run here. A `DirectInflow` (receive-only,
        // `send_required == false`) returns `Awaiting` after minting its invoice (its payer is
        // external). A `Move` (`send_required == true`) drives on through the irreversible `Pay`
        // and both `AwaitSettle` legs to `Done`, synchronously (spec §7). `Evacuate`/advisory
        // actions already mapped to `None` above → `Unsupported`.

        // FIRST: rebuild the record from the intent + backfilled op artifacts, so a replayed
        // move reattaches (no re-quote, no spurious over-cap fail).
        let mut rec = self.assemble_record(intent, &plan).await?;

        loop {
            match next_step(&rec) {
                MoveStep::CreateInvoice => {
                    self.validate_move_gateway_before_receive(&rec).await?;
                    let invoice_amount = self.gross_up(&rec).await?;
                    // For a `Move`, persist the chosen gateway BEFORE the non-idempotent receive
                    // call. If the process dies after B's receive op commits but before the
                    // invoice/op-id cache write below, backfill can recover the op but not the
                    // gateway. This pre-op record makes the gateway authoritative on replay.
                    if rec.send_required {
                        self.journal.put_move(&rec).await?;
                    }
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
                    // KILLPOINT (§5 backfill window): the receive op is now committed in the
                    // CLIENT db, but our MoveRecord (recv_op + invoice) is NOT yet persisted. A
                    // crash here forces backfill to recover the recv op by `move_id` on resume,
                    // proving no SECOND invoice is minted.
                    maybe_crash("before-move-record");
                    rec.invoice = Some(invoice);
                    rec.recv_op = Some(recv_op);
                    rec.phase = MovePhase::Invoiced;
                    self.journal.put_move(&rec).await?;
                    // KILLPOINT: the MoveRecord (recv_op + invoice) is persisted and the receive
                    // leg is committed, but the irreversible `Pay` has not run. A crash here must
                    // resume straight into `Pay` (reattaching the fixed invoice), never re-mint.
                    maybe_crash("after-receive-commit");

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
                    // KILLPOINT: the invoice exists but NO send has been started yet. A crash
                    // here must let reconcile pay EXACTLY once on resume.
                    maybe_crash("before-send");
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
                    // KILLPOINT (§5 backfill window): the send op is committed in the CLIENT db,
                    // but our MoveRecord does NOT yet carry `send_op`. A crash here must NOT
                    // double-pay: backfill recovers the send op by `move_id`; if that misses, a
                    // re-`pay` dedups to `AlreadyInFlight`/`AlreadyPaid`.
                    maybe_crash("after-send-commit");
                    rec.send_op = Some(send_op);
                    rec.phase = MovePhase::Sending;
                    self.journal.put_move(&rec).await?;
                }
                MoveStep::AwaitSettle => {
                    // A `DirectInflow` reaching `AwaitSettle` on resume is still owned by its
                    // `recv_op` subscription (§9.5), not this drive: surface `Awaiting`. Persist
                    // the reassembled record FIRST: a crash between lnv2 `receive` committing and
                    // the first `put_move` (the `CreateInvoice` arm) can leave the derived cache
                    // unpersisted, and this resume rebuilt it from the op-log — re-persisting here
                    // repairs the cache so `invoice_for`/later reattaches find the already-minted
                    // invoice without a separate reconcile (spec §9.2).
                    if !rec.send_required {
                        self.journal.put_move(&rec).await?;
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
                // A `Refunded`/`Failed` phase is terminal (spec §7): the send self-refunded or a
                // leg failed. Surface the recorded reason so the CLI/log names the actual cause.
                MoveStep::Failed => {
                    return Err(ExecError::Permanent(
                        rec.outcome
                            .clone()
                            .unwrap_or_else(|| "move refunded/failed".into()),
                    ))
                }
            }
        }
    }
}

/// Solve the §6 receive-side fixed point for a constant federation fee, mapping the pure
/// solver's "no solution" (a gateway advertising a ≥100% ppm receive fee) to a terminal
/// [`ExecError::Permanent`] instead of letting the solver — or a re-drive of it — hang. Such a
/// fee is deterministically unsolvable for this gateway, so the intent must fail terminally
/// (the operator fixes/repins the gateway and re-runs under a fresh occurrence), never spin.
fn solve_gross_up(
    net: Msat,
    gateway_fee: fee::GatewayFee,
    fed_fee: Msat,
) -> Result<fee::GrossUp, ExecError> {
    fee::gross_up(net, gateway_fee, |_contract| fed_fee).ok_or_else(|| {
        ExecError::Permanent(format!(
            "gateway receive fee is {} ppm (>= 100% of the invoice); no invoice can net the \
             requested {} msat",
            gateway_fee.ppm, net.0
        ))
    })
}

fn ensure_minimum_incoming_contract(amount: Msat, contract_amount: Msat) -> Result<(), ExecError> {
    if contract_amount.0 < MINIMUM_INCOMING_CONTRACT_MSAT {
        return Err(ExecError::Permanent(format!(
            "direct inflow amount too small: net {} msat produces a {} msat incoming contract; \
             lnv2 requires at least {} msat",
            amount.0, contract_amount.0, MINIMUM_INCOMING_CONTRACT_MSAT
        )));
    }
    Ok(())
}

/// Map a transient fedimint/I/O error to [`ExecError::Retryable`] (leave the intent
/// `Pending` so the next `reconcile` retries). Fee-over-cap and unsupported actions are the
/// only `Permanent`/`Unsupported` outcomes, raised explicitly above.
fn retryable(e: anyhow::Error) -> ExecError {
    ExecError::Retryable(e.to_string())
}

/// Crash-smoke deterministic hook (spec §5/§10): abort the process at the named killpoint IFF
/// `WALLET_CLI_CRASH_AT` equals `point`. `abort()` (not `exit`) makes the kill uncatchable and
/// unclean — it simulates a `kill -9`/OOM, so the crash-window resume paths (§5/§9) run for real
/// rather than unwinding cleanly. A strict NO-OP when the var is unset or names a DIFFERENT point,
/// so it never perturbs a normal run; the two-fed `smoke_crash_move_devimint.sh` (which runs the
/// DEBUG binary) sets it per killpoint to drive the crash gate.
///
/// This is test-only fault injection, so it is gated to `debug_assertions` builds — the crate's
/// established test-hook pattern (see `move_protocol.rs`). A `--release` production wallet binary
/// compiles the abort out entirely: no `WALLET_CLI_CRASH_AT` value can crash the money path there.
#[cfg(debug_assertions)]
fn maybe_crash(point: &str) {
    if crash_point_matches(std::env::var("WALLET_CLI_CRASH_AT").ok().as_deref(), point) {
        std::process::abort();
    }
}

/// Release counterpart: the fault injector is elided, so every killpoint call is a zero-cost
/// no-op and no environment can abort a production binary mid-move.
#[cfg(not(debug_assertions))]
fn maybe_crash(_point: &str) {}

/// Whether the `WALLET_CLI_CRASH_AT` value (`None` when unset) selects `point`. Split out from
/// [`maybe_crash`] so the match logic is unit-tested WITHOUT touching process-global env or the
/// uncatchable abort path. In a `--release` non-test build the hook above is elided and this
/// predicate is unused; it stays defined (and tested) rather than gated so `cargo test --release`
/// still compiles the unit test.
#[cfg_attr(not(debug_assertions), allow(dead_code))]
fn crash_point_matches(configured: Option<&str>, point: &str) -> bool {
    configured == Some(point)
}

fn gateway_from_cache_or_recovered(
    cached: Option<&MoveRecord>,
    plan: &MovePlan,
    key: &wallet_core::IdempotencyKey,
    artifacts: &[OpArtifact],
) -> Option<GatewayUrl> {
    if let Some(rec) = cached {
        if plan.send_required || has_move_artifact(rec) {
            return Some(rec.gateway.clone());
        }
    }
    if !plan.send_required
        && artifacts.iter().any(|artifact| {
            artifact.move_id == *key && artifact.leg == Leg::Receive && artifact.invoice.is_some()
        })
    {
        return Some(recovered_receive_only_gateway());
    }
    None
}

fn has_move_artifact(rec: &MoveRecord) -> bool {
    rec.invoice.is_some() || rec.recv_op.is_some() || rec.send_op.is_some()
}

fn recovered_receive_only_gateway() -> GatewayUrl {
    GatewayUrl("recovered-receive-only-gateway-not-used".to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use fedimint_bip39::Mnemonic;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt as _;
    use wallet_core::{Action, IdempotencyKey, IntentStatus};

    const FED_A: FederationId = FederationId([0xAA; 32]);
    const FED_B: FederationId = FederationId([0xBB; 32]);

    /// A constructible executor over an in-memory db — enough to exercise the `perform` gate,
    /// which decides `Move`/`Evacuate` BEFORE any federation I/O (no join needed).
    async fn test_executor() -> FedimintExecutor {
        let db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(db));
        FedimintExecutor::new(mc, journal, None)
    }

    fn intent(action: Action) -> Intent {
        let max_fee = action.fee_cap();
        Intent {
            idempotency_key: IdempotencyKey("gate-test".into()),
            action,
            max_fee,
            status: IntentStatus::Pending,
        }
    }

    /// Step 4b-live-2 un-gates `Move`: `perform` must NO LONGER map it to `Unsupported`. With no
    /// federation joined in this fixture it cannot reach the source/destination clients, so the
    /// first I/O (`backfill_ops`/gateway resolution during `assemble_record`) surfaces a
    /// RETRYABLE error — the intent stays `Pending`, re-drivable on the next reconcile. What
    /// matters here is only that the terminal `Unsupported` gate is gone; the live two-leg drive
    /// is exercised by `smoke_move_devimint.sh`.
    #[tokio::test]
    async fn move_is_no_longer_unsupported() {
        let executor = test_executor().await;
        let action = Action::Move {
            from: FED_A,
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(10_000),
        };
        let err = executor
            .perform(&intent(action))
            .await
            .expect_err("no federation joined in the fixture, so the move can't reach its clients");
        assert!(
            matches!(err, ExecError::Retryable(_)),
            "Move must attempt real I/O (Retryable when the fed isn't joined), never Unsupported: {err:?}"
        );
    }

    /// `Evacuate` is Phase 2: `perform` maps it to `Unsupported` (via `MovePlan::from_action`).
    #[tokio::test]
    async fn evacuate_is_unsupported() {
        let executor = test_executor().await;
        let action = Action::Evacuate {
            from: FED_A,
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(10_000),
        };
        assert!(matches!(
            executor.perform(&intent(action)).await,
            Err(ExecError::Unsupported)
        ));
    }

    #[test]
    fn maybe_crash_is_a_noop_unless_the_env_var_matches() {
        // The pure predicate: only an EXACT hit selects the abort. Unset (`None`) and a
        // different killpoint are both no-ops, so a normal run is never perturbed.
        assert!(
            !crash_point_matches(None, "before-send"),
            "an unset WALLET_CLI_CRASH_AT never crashes"
        );
        assert!(
            !crash_point_matches(Some("after-send-commit"), "before-send"),
            "a DIFFERENT killpoint never crashes"
        );
        assert!(
            crash_point_matches(Some("before-send"), "before-send"),
            "an exact match selects the crash"
        );
    }

    #[test]
    fn solve_gross_up_rejects_unsolvable_gateway_fee_as_permanent() {
        // A gateway advertising a >= 100% receive fee (ppm >= 1_000_000) makes the receive
        // fixed point unsolvable; the executor must turn that into a terminal `Permanent`
        // (fail the intent, never hand the pure solver a fee it would search forever on).
        let unsolvable = fee::GatewayFee {
            base_msat: Msat(0),
            ppm: 1_000_000,
        };
        let err = solve_gross_up(Msat(100_000), unsolvable, Msat(0))
            .expect_err(">= 100% gateway fee has no solution");
        assert!(matches!(err, ExecError::Permanent(msg) if msg.contains("ppm")));

        // A realistic fee (0.5% gateway ppm + flat federation fee) solves and nets the target.
        let solvable = fee::GatewayFee {
            base_msat: Msat(50),
            ppm: 5_000,
        };
        let grossed =
            solve_gross_up(Msat(100_000), solvable, Msat(200)).expect("a sub-100% fee is solvable");
        assert!(grossed.invoice_amount.0 >= 100_000);
    }

    #[test]
    fn minimum_incoming_contract_guard_matches_pinned_lnv2_boundary() {
        assert_eq!(MINIMUM_INCOMING_CONTRACT_MSAT, 5_000);
        ensure_minimum_incoming_contract(Msat(4_000), Msat(5_000))
            .expect("lnv2 accepts exactly the minimum incoming contract");

        let err = ensure_minimum_incoming_contract(Msat(3_999), Msat(4_999))
            .expect_err("contract below lnv2's minimum is terminal");
        assert!(matches!(err, ExecError::Permanent(msg) if msg.contains("amount too small")));
    }

    #[test]
    fn receive_only_recovery_does_not_require_gateway_resolution() {
        let key = IdempotencyKey("direct-inflow:recover".into());
        let plan = MovePlan {
            from: None,
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            send_required: false,
        };
        let artifacts = vec![OpArtifact {
            move_id: key.clone(),
            leg: Leg::Receive,
            op_id: crate::types::OperationId([0x42; 32]),
            invoice: Some(Invoice("lnbc1recover".into())),
        }];

        assert_eq!(
            gateway_from_cache_or_recovered(None, &plan, &key, &artifacts),
            Some(recovered_receive_only_gateway())
        );

        let send_plan = MovePlan {
            from: Some(FED_A),
            send_required: true,
            ..plan
        };
        assert_eq!(
            gateway_from_cache_or_recovered(None, &send_plan, &key, &artifacts),
            None
        );
    }

    #[test]
    fn pre_op_cached_gateway_pins_moves_but_not_receive_only_retries() {
        let key = IdempotencyKey("direct-inflow:pre-op".into());
        let plan = MovePlan {
            from: None,
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            send_required: false,
        };
        let mut cached = MoveRecord {
            key: key.clone(),
            from: None,
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            gateway: GatewayUrl("https://stale.example".into()),
            send_required: false,
            invoice: None,
            recv_op: None,
            send_op: None,
            phase: MovePhase::Created,
            outcome: None,
        };

        assert_eq!(
            gateway_from_cache_or_recovered(Some(&cached), &plan, &key, &[]),
            None,
            "a receive-only gateway-only cache must not block an explicit retry from repinning"
        );

        let send_plan = MovePlan {
            from: Some(FED_A),
            send_required: true,
            ..plan.clone()
        };
        let mut move_cached = cached.clone();
        move_cached.from = Some(FED_A);
        move_cached.send_required = true;
        assert_eq!(
            gateway_from_cache_or_recovered(Some(&move_cached), &send_plan, &key, &[]),
            Some(GatewayUrl("https://stale.example".into())),
            "a Move pre-op cache records the gateway chosen before non-idempotent receive"
        );

        cached.invoice = Some(Invoice("lnbc1cached".into()));
        assert_eq!(
            gateway_from_cache_or_recovered(Some(&cached), &plan, &key, &[]),
            Some(GatewayUrl("https://stale.example".into())),
            "once an invoice exists, the recorded gateway is part of the durable receive"
        );
    }
}
