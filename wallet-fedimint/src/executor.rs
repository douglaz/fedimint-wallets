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
//! so a crash never re-mints or re-pays. `Evacuate` (Phase 3.A) maps to the SAME send-required
//! plan as `Move` (`MovePlan::from_action`), so it drives the identical validated two-leg path —
//! the money engine can now flee a dying federation, not just top up a standby. Do not read
//! the absence of a happy-path unit test here as untested logic: the pure decisions are
//! golden-tested above, and the live two-leg drive is exercised by `smoke_move_devimint.sh`
//! (and the deferred `smoke_evacuate_devimint.sh` for the evacuate tick).
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
use crate::multi_client::{MultiClient, ReceiveState, SendError, SendOutcome, SendState};
use crate::types::{GatewayUrl, Invoice};
use async_trait::async_trait;
use lightning_invoice::Bolt11Invoice;
use std::str::FromStr as _;
use std::sync::Arc;
use wallet_core::{Action, ExecError, Executor, FederationId, Intent, Msat, PerformOutcome};

/// Pinned lnv2 requires the gateway-reduced incoming contract to be at least 5 sats
/// (`MINIMUM_INCOMING_CONTRACT_AMOUNT`) before it will mint a receive invoice.
pub const MINIMUM_INCOMING_CONTRACT_MSAT: u64 =
    fedimint_lnv2_common::MINIMUM_INCOMING_CONTRACT_AMOUNT.msats;

/// How many times to re-quote the federation receive fee at the refined contract amount
/// while sizing the invoice. `receive_fee_quote` is async but [`fee::gross_up`]'s fed-fee
/// closure is sync, so the executor resolves the (contract-amount-dependent) fee with a
/// short async fixed point; a couple of passes converge for any real fee (ppm slope < 1).
const FED_FEE_REQUOTE_PASSES: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FreshMoveCost {
    invoice_amount: Msat,
    receive_quote: Msat,
    send_quote: Msat,
}

impl FreshMoveCost {
    fn total_fee(self) -> Msat {
        Msat(self.receive_quote.0.saturating_add(self.send_quote.0))
    }

    fn source_debit(self) -> Msat {
        Msat(self.invoice_amount.0.saturating_add(self.send_quote.0))
    }
}

fn evacuation_cost_fits(cost: FreshMoveCost, fee_cap: Msat, spendable: Msat) -> bool {
    cost.total_fee() <= fee_cap && cost.source_debit() <= spendable
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FreshSendRequiredGatewayFees {
    receive: fee::GatewayFee,
    send: fee::GatewayFee,
}

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
    /// The hard per-fed balance cap (ADR-0018) enforced at PERFORM time (§15.2): a non-evacuation
    /// inflow that would push its destination over the cap is refused pre-mint, and a fresh
    /// evacuation is downsized to the destination's remaining cap room. `None` disables the check
    /// (the operator's `--allow-over-cap` override). The §4.2 same-tick reservation sizes joint
    /// moves, but its snapshot can be stale by perform time and the operator verbs consult no cap
    /// at all — this is the belt that enforces the cap at the moment money actually moves.
    hard_cap: Option<Msat>,
}

impl FedimintExecutor {
    pub fn new(
        mc: Arc<MultiClient>,
        journal: Arc<FedimintJournal>,
        pinned_gateway: Option<GatewayUrl>,
        hard_cap: Option<Msat>,
    ) -> Self {
        Self {
            mc,
            journal,
            pinned_gateway,
            hard_cap,
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
            // Scan for a gateway serving BOTH ends of a send-required move (§15.6); a
            // receive-only inflow (`plan.from == None`) validates only the destination.
            None => self.resolve_gateway(&plan.to, plan.from).await?,
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

    /// Resolve a gateway for a FRESH move into `to` (spec §7, §15.6): the explicitly pinned
    /// gateway wins (⟦D4⟧; devimint's LDK gateway is not auto-registered, so the CLI passes it
    /// directly — runbook §4). Otherwise SCAN the federation's registered lnv2 gateway list for
    /// the first that VALIDATES — for a send-required move (`from == Some`) against BOTH `to` and
    /// `from` (the shared-gateway internal swap needs both ends), for a receive-only inflow
    /// (`from == None`) against `to` alone. A stale first-registered gateway must not make a
    /// healthy fed unroutable (the SDK's own `select_gateway` scans until responsive).
    ///
    /// "None validates" is `Retryable`, NOT `Permanent`: a resume verb (`reconcile`/`await-move`)
    /// carries no pinned gateway, so re-driving an intent that has none cached must leave it
    /// `Pending` (re-drivable once the operator re-runs `direct-inflow --gateway` to supply one),
    /// never terminally `Failed`. The fresh `direct-inflow` path never hits this — its
    /// `pick_receive_gateway` guarantees a gateway before the runtime is built.
    async fn resolve_gateway(
        &self,
        to: &FederationId,
        from: Option<FederationId>,
    ) -> Result<GatewayUrl, ExecError> {
        if let Some(gateway) = &self.pinned_gateway {
            return Ok(gateway.clone());
        }
        let gateways = self.mc.gateways(to).await.map_err(retryable)?;
        for gateway in &gateways {
            if self.gateway_serves_route(to, from.as_ref(), gateway).await {
                return Ok(gateway.clone());
            }
        }
        Err(ExecError::Retryable(format!(
            "no lnv2 gateway available to route a move into federation {} \
             (scanned {} registered gateway(s); pass one explicitly — devimint does not \
             auto-register its LDK gateway)",
            to.to_hex(),
            gateways.len(),
        )))
    }

    /// Whether `gateway` can route this move (§15.6): it must serve the RECEIVE end `to`, and for
    /// a send-required move ALSO the SEND end `from` (`routing_info` serves both). A `routing_info`
    /// fetch failure for either end reads as "does not serve", so the scan passes over it.
    async fn gateway_serves_route(
        &self,
        to: &FederationId,
        from: Option<&FederationId>,
        gateway: &GatewayUrl,
    ) -> bool {
        if self.mc.validate_gateway(to, gateway).await.is_err() {
            return false;
        }
        match from {
            Some(from) => self.mc.validate_gateway(from, gateway).await.is_ok(),
            None => true,
        }
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
        self.quote_receive_gross_up_with_gateway_fee(to, amount, gateway_fee)
            .await
    }

    async fn quote_receive_gross_up_with_gateway_fee(
        &self,
        to: &FederationId,
        amount: Msat,
        gateway_fee: fee::GatewayFee,
    ) -> Result<fee::GrossUp, ExecError> {
        // §15.10: the verify / re-solve / bisect loop is extracted into the free
        // [`resolve_receive_gross_up`] generic over an async federation-fee-quote closure so it
        // is golden-testable over scripted quote streams. Production quotes the LIVE federation
        // fee at each candidate contract amount; behavior is byte-identical to the welded form.
        resolve_receive_gross_up(amount, gateway_fee, |contract| async move {
            self.mc
                .receive_fee_quote(to, contract)
                .await
                .map_err(retryable)
        })
        .await
    }

    /// Preflight a fresh CLI `DirectInflow` before it is journaled. This catches the
    /// deterministic lnv2 dust rejection (`AmountTooSmall`) while still letting any existing
    /// pending intent re-drive through `perform`, where the same guard marks it terminal.
    pub async fn validate_direct_inflow_amount(
        &self,
        to: FederationId,
        amount: Msat,
    ) -> Result<(), ExecError> {
        // A DirectInflow is receive-only, so validate the gateway against the destination only.
        let gateway = self.resolve_gateway(&to, None).await?;
        let grossed = self.quote_receive_gross_up(&to, &gateway, amount).await?;
        ensure_minimum_incoming_contract(amount, grossed.contract_amount)
    }

    /// Size the receive invoice via the §6 fixed point and apply the lnv2 minimum-contract guard
    /// (spec §7 `CreateInvoice`). The gateway fee comes from `routing_info`; the federation fee is
    /// resolved by a short async fixed point (see [`FED_FEE_REQUOTE_PASSES`]). Returns the sized
    /// invoice; the invoice is then fixed (never re-quoted on resume). The receive-side fee-cap
    /// check is applied by the CALLER (the `CreateInvoice` arm), which first persists the computed
    /// `receive_fee_quoted` on the record so a "fee over cap" refusal is explained in history
    /// (spec §2.3).
    async fn gross_up(&self, rec: &MoveRecord) -> Result<fee::GrossUp, ExecError> {
        let grossed = self
            .quote_receive_gross_up(&rec.to, &rec.gateway, rec.amount)
            .await?;
        ensure_minimum_incoming_contract(rec.amount, grossed.contract_amount)?;
        Ok(grossed)
    }

    /// Re-run the §15.7 committed-contract check for a receive op recovered from the op-log.
    /// This closes the crash window after `mc.receive` commits but before the post-receive
    /// `MoveRecord` write: resume may skip `CreateInvoice`, so `Pay`/receive-only `Awaiting` must
    /// verify the op's durable contract against the quoted contract stored in `custom_meta`.
    async fn verify_recovered_receive_contract(&self, rec: &MoveRecord) -> Result<(), ExecError> {
        let recv_op = rec.recv_op.ok_or_else(|| {
            ExecError::Permanent("receive contract check reached with no receive op".into())
        })?;
        // `receive_contract_amounts` hits only the destination's LOCAL op-log, so its ONLY transient
        // failure is the destination client not being open this pass (a later reconcile can open it).
        // With the client open, an op-not-found / wrong-leg / malformed-quote error is durable
        // corruption a re-drive can never clear — classify it Permanent so a poisoned intent fails
        // loudly instead of livelocking Pending forever.
        let (committed, quoted) = match self.mc.receive_contract_amounts(&rec.to, recv_op).await {
            Ok(amounts) => amounts,
            Err(e) => {
                return Err(classify_receive_contract_read_error(
                    e,
                    self.mc.federations().contains(&rec.to),
                    &rec.key.0,
                ))
            }
        };
        verify_replayable_receive_contract(committed, quoted)
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

    /// A fresh `Evacuate` may be sized by the allocator as the source's full spendable balance
    /// (`min(spendable, cap_room)`). A normal move invoice is grossed up and then paid with
    /// send-side fees, so asking the dying federation to net its full balance would require it to
    /// spend more than it has. Before minting the destination invoice, quote the move cost and
    /// reduce only fresh, side-effect-free evacuation records to the largest net amount the source
    /// can actually fund under `fee_cap`. The sized amount is persisted with the pre-receive
    /// `put_move` and honored on re-assembly (`assemble_move_record` prefers the cached amount),
    /// so a resume after the invoice is minted keeps the Pay-step cap re-check honest.
    ///
    /// "Nothing evacuable fits" is `Retryable`, NOT `Permanent` (same convention as
    /// `resolve_gateway`): the `None` can come from a TRANSIENT shortfall — the source's funds are
    /// momentarily in flight (the send dry-run hits `InsufficientBalanceError`, treated as unfit),
    /// or a fee quote ticked up between attempts — and this runs BEFORE any side effect, on every
    /// pre-receive resume. Terminally `Failed`-ing here would abandon funds on a dying federation
    /// the wallet could have drained one tick later, defeating the whole point of a flee. Leaving
    /// the intent `Pending` lets the next tick retry once the shortfall clears; a source holding
    /// only sub-minimum dust simply keeps retrying harmlessly (nothing meaningful is stranded).
    async fn size_fresh_evacuation(
        &self,
        action: &Action,
        rec: &mut MoveRecord,
    ) -> Result<(), ExecError> {
        // The full ask comes from the ACTION, not `rec.amount`: a resumed pre-receive record
        // may already carry a previously sized-down amount, and re-sizing (no side effect has
        // happened yet) must start over from the intent so a fee drop between retries can
        // still evacuate the full desired amount.
        let &Action::Evacuate {
            amount: desired, ..
        } = action
        else {
            return Ok(());
        };
        if has_move_artifact(rec) {
            return Ok(());
        }
        let from = rec.from.ok_or_else(|| {
            ExecError::Permanent(
                "Evacuate record requires a send leg but has no source federation".into(),
            )
        })?;
        // §15.2: an evacuation must not push its DESTINATION over the hard per-fed cap. Clamp the
        // desired net to the destination's remaining cap room BEFORE costing; a destination already
        // at/above the cap yields zero room, a LOUD terminal refusal (never a 0-msat move, never a
        // wrapped-around huge room). This runs only for a FRESH evacuation (`has_move_artifact`
        // returned early above), so a resumed, already-minted evacuation is never refused here.
        let desired = self.clamp_desired_to_cap_room(rec, desired).await?;
        let spendable = self.mc.balance(&from).await.map_err(retryable)?;
        let Some(amount) = self
            .max_affordable_evacuation_net(
                &from,
                &rec.to,
                &rec.gateway,
                desired,
                rec.fee_cap,
                spendable,
            )
            .await?
        else {
            return Err(ExecError::Retryable(format!(
                "no evacuable amount fits: desired {} msat cannot reserve move fees within source \
                 balance {} msat and fee_cap {} msat (retrying — a later tick may succeed once \
                 in-flight funds settle or the fee quote eases)",
                desired.0, spendable.0, rec.fee_cap.0
            )));
        };
        if amount < desired {
            tracing::warn!(
                from = %from.to_hex(),
                to = %rec.to.to_hex(),
                requested_msat = desired.0,
                executable_msat = amount.0,
                spendable_msat = spendable.0,
                fee_cap_msat = rec.fee_cap.0,
                "executor: reducing fresh evacuation amount to reserve move fees"
            );
        }
        rec.amount = amount;
        Ok(())
    }

    /// Clamp a fresh evacuation's desired net to the DESTINATION's remaining hard-cap room
    /// (§15.2). `None` cap disables the check. A destination already at/above the cap has zero
    /// room and is a LOUD terminal refusal (an evacuation cannot legitimately overflow its
    /// destination), never a 0-msat move.
    async fn clamp_desired_to_cap_room(
        &self,
        rec: &MoveRecord,
        desired: Msat,
    ) -> Result<Msat, ExecError> {
        let Some(cap) = self.hard_cap else {
            return Ok(desired);
        };
        let dest = self.mc.balance(&rec.to).await.map_err(retryable)?;
        match evacuation_cap_room(dest, cap) {
            Some(room) => Ok(Msat(desired.0.min(room.0))),
            None => Err(ExecError::Permanent(format!(
                "no cap room at destination: federation {} holds {} msat at/above the per-fed cap \
                 {} msat, so an evacuation cannot drain into it",
                rec.to.to_hex(),
                dest.0,
                cap.0
            ))),
        }
    }

    /// Enforce the hard per-fed cap on a NON-evacuation inflow before minting (§15.2): refuse
    /// terminally when the destination's live balance plus the inflow amount would exceed the cap.
    /// `None` cap disables the check. An evacuation is downsized instead (see
    /// [`Self::clamp_desired_to_cap_room`]).
    async fn enforce_destination_cap(&self, rec: &MoveRecord) -> Result<(), ExecError> {
        let Some(cap) = self.hard_cap else {
            return Ok(());
        };
        let dest = self.mc.balance(&rec.to).await.map_err(retryable)?;
        if would_exceed_cap(dest, rec.amount, cap) {
            return Err(ExecError::Permanent(format!(
                "destination would exceed the per-fed cap ({}+{} > {} msat) for federation {}",
                dest.0,
                rec.amount.0,
                cap.0,
                rec.to.to_hex()
            )));
        }
        Ok(())
    }

    /// The largest net amount (≤ `desired`) the source can fund under `fee_cap`, or `None`
    /// when nothing evacuable fits.
    ///
    /// `evacuation_candidate_fits` is NOT monotone over the full `[0, desired]` range: it is
    /// false BELOW the lnv2 minimum-incoming-contract threshold as well as above the budget
    /// ceiling, so an unclamped bisection can probe into the too-small region and skip a
    /// feasible window entirely (e.g. desired 500_000 with only ~5_500 msat affordable). The
    /// §6 gross-up guarantees `contract_amount = net + fed_fee ≥ net`, so every net at or
    /// above [`MINIMUM_INCOMING_CONTRACT_MSAT`] clears the contract minimum and the predicate
    /// is monotone (fits-then-doesn't) on `[MINIMUM_INCOMING_CONTRACT_MSAT, desired]` — the
    /// search is clamped to that range. A net that would only fit BELOW the floor (< 5 sats,
    /// with the contract lifted over the minimum by the federation fee alone) is reported as
    /// not evacuable; the fast path still handles a small `desired` asked for outright.
    ///
    /// Resilience note (accepted trade-off): a transient error on ANY of the ~log2(desired)
    /// sizing probes aborts the whole sizing as `Retryable`, discarding bisection progress —
    /// the next tick restarts it from scratch. On a genuinely flaky federation this can fail
    /// to converge for a while; mitigated by the 24h evacuation lead (guardians are usually
    /// still healthy when the signal fires) and by retry-on-every-tick. Per-probe retries
    /// can be added later without changing the search's shape.
    async fn max_affordable_evacuation_net(
        &self,
        from: &FederationId,
        to: &FederationId,
        gateway: &GatewayUrl,
        desired: Msat,
        fee_cap: Msat,
        spendable: Msat,
    ) -> Result<Option<Msat>, ExecError> {
        let gateway_fees = self
            .fresh_send_required_gateway_fees(from, to, gateway)
            .await?;
        if self
            .evacuation_candidate_fits(from, to, desired, fee_cap, spendable, gateway_fees)
            .await?
        {
            return Ok(Some(desired));
        }

        let found = largest_fitting_amount(
            MINIMUM_INCOMING_CONTRACT_MSAT,
            desired.0.saturating_sub(1),
            |amount| {
                self.evacuation_candidate_fits(
                    from,
                    to,
                    Msat(amount),
                    fee_cap,
                    spendable,
                    gateway_fees,
                )
            },
        )
        .await?;
        Ok(found.map(Msat))
    }

    async fn fresh_send_required_gateway_fees(
        &self,
        from: &FederationId,
        to: &FederationId,
        gateway: &GatewayUrl,
    ) -> Result<FreshSendRequiredGatewayFees, ExecError> {
        let receive = self
            .mc
            .receive_gateway_fee(to, gateway)
            .await
            .map_err(retryable)?;
        let send = self
            .mc
            .direct_swap_send_gateway_fee(from, gateway)
            .await
            .map_err(retryable)?;
        Ok(FreshSendRequiredGatewayFees { receive, send })
    }

    async fn evacuation_candidate_fits(
        &self,
        from: &FederationId,
        to: &FederationId,
        amount: Msat,
        fee_cap: Msat,
        spendable: Msat,
        gateway_fees: FreshSendRequiredGatewayFees,
    ) -> Result<bool, ExecError> {
        let Some(cost) = self
            .quote_fresh_send_required_cost(from, to, amount, gateway_fees)
            .await?
        else {
            return Ok(false);
        };
        Ok(evacuation_cost_fits(cost, fee_cap, spendable))
    }

    async fn quote_fresh_send_required_cost(
        &self,
        from: &FederationId,
        to: &FederationId,
        amount: Msat,
        gateway_fees: FreshSendRequiredGatewayFees,
    ) -> Result<Option<FreshMoveCost>, ExecError> {
        if amount.0 == 0 {
            return Ok(None);
        }
        let grossed = self
            .quote_receive_gross_up_with_gateway_fee(to, amount, gateway_fees.receive)
            .await?;
        if grossed.contract_amount.0 < MINIMUM_INCOMING_CONTRACT_MSAT {
            return Ok(None);
        }

        let send_gateway_quote = gateway_fees.send.on(grossed.invoice_amount);
        let outgoing_contract_amount = Msat(
            grossed
                .invoice_amount
                .0
                .saturating_add(send_gateway_quote.0),
        );
        let send_tx_fee = match self
            .mc
            .send_fee_quote_for_amount(from, outgoing_contract_amount)
            .await
        {
            Ok(fee) => fee,
            // The send-side dry-run balances the hypothetical outgoing contract against the
            // source's REAL note inventory, so a candidate too large to fund fails HERE with
            // the mint's `InsufficientBalanceError` — before `evacuation_cost_fits` ever sees
            // a cost. That is a definitive "does not fit" (the source debit already exceeds
            // spendable), not a transient fault: report the candidate as unquotable so the
            // sizing search keeps probing smaller amounts. Without this, a fresh full-balance
            // evacuation (`desired == spendable`, the common shutdown case) errors `Retryable`
            // on its very FIRST probe — invoice + gateway fee already exceed the balance — and
            // the downsizing search never runs.
            Err(e) if is_insufficient_balance(&e) => return Ok(None),
            Err(e) => return Err(retryable(e)),
        };
        Ok(Some(FreshMoveCost {
            invoice_amount: grossed.invoice_amount,
            receive_quote: grossed.receive_quote,
            send_quote: Msat(send_gateway_quote.0.saturating_add(send_tx_fee.0)),
        }))
    }
}

#[async_trait]
impl Executor for FedimintExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        // Only the advisory `RefuseInflow` action maps to `None` → `Unsupported` (§7);
        // `Move`/`Evacuate`/`DirectInflow` all yield an executable plan.
        let Some(plan) = MovePlan::from_action(&intent.action) else {
            return Err(ExecError::Unsupported);
        };

        // BOTH send-required move shapes run here identically. A `DirectInflow` (receive-only,
        // `send_required == false`) returns `Awaiting` after minting its invoice (its payer is
        // external). A `Move` OR `Evacuate` (`send_required == true`) drives on through the
        // irreversible `Pay` and both `AwaitSettle` legs to `Done`, synchronously (spec §7):
        // an evacuate is just a move that drains a dying fed. Advisory actions already mapped
        // to `None` above → `Unsupported`.

        // FIRST: rebuild the record from the intent + backfilled op artifacts, so a replayed
        // move reattaches (no re-quote, no spurious over-cap fail).
        let mut rec = self.assemble_record(intent, &plan).await?;
        self.size_fresh_evacuation(&intent.action, &mut rec).await?;
        // §15.2: an Evacuate was downsized to its destination's cap room by
        // `size_fresh_evacuation`; every OTHER inflow (a DirectInflow or a topping-up Move) is
        // refused pre-mint below if it would push the destination over the cap.
        let is_evacuate = matches!(intent.action, Action::Evacuate { .. });

        loop {
            match next_step(&rec) {
                MoveStep::CreateInvoice => {
                    self.validate_move_gateway_before_receive(&rec).await?;
                    if !is_evacuate {
                        self.enforce_destination_cap(&rec).await?;
                    }
                    let grossed = self.gross_up(&rec).await?;
                    // §2.3: persist the receive quote on the record BEFORE the cap check, so a
                    // "fee over cap" refusal — which returns before any money moves — is still
                    // explained in history (a derived-cache write; no money moves). It rides on
                    // every subsequent `put_move` below and is stored on the refusal path too.
                    rec.receive_fee_quoted = Some(grossed.receive_quote);
                    // Cap-check the receive side alone (spec §6/§7): for a `DirectInflow` this is
                    // the whole check; for a `Move` the send leg is re-checked at `Pay`. Over cap →
                    // persist the quote first (so the refusal is in history), then refuse terminally.
                    if !fee::total_within_cap(grossed.receive_quote, Msat(0), rec.fee_cap) {
                        self.journal.put_move(&rec).await?;
                        return Err(ExecError::Permanent(
                            "fee over cap (receive side exceeds fee_cap)".into(),
                        ));
                    }
                    let invoice_amount = grossed.invoice_amount;
                    // A move may have accepted a verified hair-under solve: the DELIVERED net is
                    // invoice − receive_quote. The adjustment to `rec.amount` happens AFTER
                    // `mc.receive` commits (below), NOT here — lowering the cached amount before
                    // the receive exists would make a crash/transient-failure retry prefer the
                    // smaller cached amount over the intent's ask (`assemble_move_record`) and mint
                    // a fresh invoice for less than requested even though fees may have settled; a
                    // fresh attempt must re-quote from the intent's full ask.
                    let delivered = Msat(
                        grossed
                            .invoice_amount
                            .0
                            .saturating_sub(grossed.receive_quote.0),
                    );
                    // The net this move will actually deliver (== rec.amount for an exact solve),
                    // committed in the receive op's own MoveMeta below UNCONDITIONALLY (§15.11): the
                    // MoveMeta amount is documented as the honest crash-safe delivered amount, so a
                    // receive-only `DirectInflow` that settles a hair under must record `delivered`,
                    // not the ask. A crash that loses the post-receive cache write then recovers the
                    // HONEST amount from the op itself (backfill prefers recovered op metadata over
                    // the intent's ask) — the Pay-step cap re-check can never be weakened by a stale
                    // higher amount.
                    let net_amount = delivered_move_amount(delivered, rec.amount);
                    // Persist the record BEFORE the non-idempotent receive call — for BOTH move
                    // shapes. If the process dies after B's receive op commits but before the
                    // invoice/op-id cache write below, backfill recovers the op from the op-log but
                    // NOT the executor-only facts it does not carry: for a `Move` the chosen gateway
                    // (authoritative on replay), and for EITHER shape the §2.3 `receive_fee_quoted`
                    // set just above. A `DirectInflow` has no later `Pay` arm to re-derive that
                    // quote, so without this pre-op write a crash in that window would finalize its
                    // history with the receive quote blanked.
                    self.journal.put_move(&rec).await?;
                    let meta = MoveMeta {
                        move_id: rec.key.clone(),
                        role: MoveRole::Receive,
                        amount: net_amount,
                        from: rec.from,
                        to: rec.to,
                    };
                    let (invoice, recv_op) = self
                        .mc
                        .receive(
                            &rec.to,
                            invoice_amount,
                            Some(rec.gateway.clone()),
                            meta.receive_value_with_contract_quote(grossed.contract_amount),
                        )
                        .await
                        .map_err(retryable)?;
                    // §15.7 never-over TOCTOU: lnv2 re-fetches `routing_info` inside
                    // `create_contract_and_fetch_invoice` and sizes the COMMITTED contract with the
                    // FRESH gateway fee, so a fee DROP between our verified quote and the mint would
                    // commit a larger contract and net the destination MORE than asked (a gateway
                    // can time this). Read the committed contract and compare against our sized
                    // `contract_amount`; on mismatch refuse BEFORE recording/surfacing/paying —
                    // safe because the invoice is unpaid at this point (for a Move we are the only
                    // payer; a DirectInflow's invoice has not been surfaced), and the orphaned
                    // receive op simply expires unclaimed. A retry (fresh occurrence) re-quotes.
                    let (committed_contract, quoted_contract) = self
                        .mc
                        .receive_contract_amounts(&rec.to, recv_op)
                        .await
                        .map_err(retryable)?;
                    verify_replayable_receive_contract(committed_contract, quoted_contract)?;
                    // KILLPOINT (§5 backfill window): the receive op is now committed in the
                    // CLIENT db, but our MoveRecord (recv_op + invoice) is NOT yet persisted. A
                    // crash here forces backfill to recover the recv op by `move_id` on resume,
                    // proving no SECOND invoice is minted.
                    maybe_crash("before-move-record");
                    rec.invoice = Some(invoice);
                    rec.recv_op = Some(recv_op);
                    rec.phase = MovePhase::Invoiced;
                    // The invoice is now FIXED, so the delivered net is a fact: record it
                    // as the move's amount so the Pay-step cap re-check
                    // (`invoice − rec.amount`) counts the honest receive cost. Crash-safe
                    // BOTH ways: the committed receive op's MoveMeta above carries the same
                    // adjusted amount (cache loss recovers it via backfill), and a crash
                    // BEFORE the receive committed left no reduced amount anywhere — a
                    // fresh retry re-quotes from the intent's full ask.
                    if rec.amount != net_amount {
                        tracing::warn!(
                            requested_msat = rec.amount.0,
                            delivered_msat = net_amount.0,
                            "executor: fee fixed point settled a hair under; adjusting move net"
                        );
                        rec.amount = net_amount;
                    }
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

                    self.verify_recovered_receive_contract(&rec).await?;

                    // §15.4 belt: parse the (fixed) BOLT11 once and refuse a move whose invoice
                    // has already EXPIRED. Paying an expired invoice can only earn a deterministic
                    // send rejection that would otherwise reset the move to `Pending` and livelock;
                    // fail terminally so a fresh occurrence re-mints a live invoice.
                    let bolt11 = parse_move_invoice(&invoice)?;
                    if bolt11.is_expired() {
                        return Err(ExecError::Permanent(format!(
                            "move invoice expired before the send leg could pay it (move {}); \
                             re-run under a fresh occurrence to re-mint",
                            rec.key.0
                        )));
                    }
                    let invoice_msat = bolt11.amount_milli_satoshis().ok_or_else(|| {
                        ExecError::Permanent("move invoice carries no amount".into())
                    })?;

                    // Re-check the cap NOW (spec §6/§7). The receive cost is recovered
                    // crash-safely from the fixed invoice (`invoice_amount − amount`); the
                    // send fee is re-quoted from the (possibly changed) gateway + federation.
                    let receive_quote = Msat(invoice_msat.saturating_sub(rec.amount.0));
                    let send_gateway_fee = self
                        .mc
                        .send_gateway_fee(&from, &rec.gateway, &invoice)
                        .await
                        .map_err(retryable)?;
                    let send_gateway_quote = send_gateway_fee.on(Msat(invoice_msat));
                    let outgoing_contract_amount =
                        Msat(invoice_msat.saturating_add(send_gateway_quote.0));
                    let send_tx_fee = self
                        .mc
                        .send_fee_quote_for_amount(&from, outgoing_contract_amount)
                        .await
                        .map_err(retryable)?;
                    let send_quote = Msat(send_gateway_quote.0.saturating_add(send_tx_fee.0));
                    // §2.3: persist the send quote on the record BEFORE the cap check, so the
                    // paradigm failure this field must explain — the "fee over cap" refusal, which
                    // returns before any send commits — is fully in history. A derived-cache write;
                    // no money moves (the `pay` below is the only irreversible step).
                    rec.send_fee_quoted = Some(send_quote);
                    // §2.3: also (re)persist the receive quote here. On a cache-loss resume that
                    // reconstructs the record from the op-log and re-drives straight into `Pay`
                    // (skipping `CreateInvoice`, where the quote is first stored), `receive_fee_quoted`
                    // is blanked — but the receive cost is a fact of the FIXED invoice
                    // (`invoice − amount`, already recomputed above for the cap re-check), so restore
                    // it and a completed move's history explains BOTH legs' fees. Equal to the value
                    // `CreateInvoice` stored, so this never disagrees with it.
                    rec.receive_fee_quoted = Some(receive_quote);
                    self.journal.put_move(&rec).await?;
                    // §15.5: Permanent ONLY when the FIXED receive quote alone exceeds the cap; a
                    // send re-quote spike is Retryable (a later attempt may quote lower — 15.4's
                    // expiry belt bounds the retry horizon), so a transient spike never terminally
                    // strands funds on a dying fed mid-evacuation.
                    pay_step_cap_verdict(receive_quote, send_quote, rec.fee_cap)?;

                    let meta = MoveMeta {
                        move_id: rec.key.clone(),
                        role: MoveRole::Send,
                        amount: rec.amount,
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
                        .map_err(map_send_error)?
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
                        self.verify_recovered_receive_contract(&rec).await?;
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
                        SendState::Success(preimage) => {
                            // §3: A's payment SETTLED — persist the preimage FIRST, BEFORE awaiting
                            // the receive, so a crash after this point can never lose the recovery
                            // proof for a stranded move. THEN await the receive.
                            rec.preimage = Some(preimage);
                            self.journal.put_move(&rec).await?;
                            let recv_op = rec.recv_op.ok_or_else(|| {
                                ExecError::Permanent(
                                    "send settled but the record has no receive op".into(),
                                )
                            })?;
                            // Transport faults bubble as `Retryable` via `map_err(retryable)` BEFORE
                            // this decision — only an op-TERMINAL non-`Claimed` receive strands
                            // (spec §3): the send debited the source but the destination was never
                            // credited (the misbehaving-gateway T4 case), which re-driving cannot
                            // fix. `settle_after_successful_send` maps it to `Stranded` (loud,
                            // terminal), naming the saved preimage.
                            let receive_state = self
                                .mc
                                .await_receive(&rec.to, recv_op)
                                .await
                                .map_err(retryable)?;
                            let (phase, outcome) = settle_after_successful_send(receive_state);
                            rec.phase = phase;
                            rec.outcome = outcome;
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
                // A `Refunded`/`Failed`/`Stranded` phase is terminal (spec §7): the send
                // self-refunded, a leg failed, or the send settled but the receive was not credited
                // (§3). Surface the recorded reason so the CLI/log names the actual cause — for a
                // `Stranded` move that reason names the saved preimage.
                MoveStep::Failed => {
                    return Err(ExecError::Permanent(
                        rec.outcome
                            .clone()
                            .unwrap_or_else(|| "move refunded/failed".into()),
                    ));
                }
            }
        }
    }
}

/// The largest `amount` in `[floor, hi]` for which `fits` holds, by bisection. `None` when
/// the range is empty or nothing in it fits. The CALLER owns the monotonicity argument:
/// `fits` must be fits-then-doesn't as the amount grows over `[floor, hi]` (see
/// `max_affordable_evacuation_net` — the floor is what makes its predicate monotone).
/// Requires `floor ≥ 1`.
async fn largest_fitting_amount<F, Fut>(
    floor: u64,
    mut hi: u64,
    mut fits: F,
) -> Result<Option<u64>, ExecError>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<bool, ExecError>>,
{
    debug_assert!(floor > 0, "a zero floor would underflow the sentinel below");
    if hi < floor {
        return Ok(None);
    }
    // `lo` trails the largest amount VERIFIED to fit; it starts one below the floor as the
    // "nothing verified yet" sentinel and only ever advances to probed-true amounts, so the
    // loop never evaluates `fits` outside `[floor, hi]`.
    let mut lo = floor - 1;
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        if fits(mid).await? {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    Ok((lo >= floor).then_some(lo))
}

/// The §6 receive-side gross-up loop (spec §15.10), extracted generic over an async
/// federation-fee-quote closure `fed_fee_quote` (contract amount → federation tx fee) so it is
/// unit-testable over scripted quote streams WITHOUT a live `MultiClient`. Byte-identical to the
/// welded original: production passes a closure over `MultiClient::receive_fee_quote`, tests pass
/// a scripted stream. The `fed_fee_quote` closure owns the transport error mapping (it returns
/// `Result<Msat, ExecError>` directly).
///
/// Quote the federation fee at the net amount, solve, then VERIFY the fee at the solved contract
/// and re-solve until the verified prediction is exact (spec §6 fixed point, exit condition on the
/// NET, not on fee equality).
///
/// NEVER-OVER is the hard half of the exact-net contract: the federation fee is a STEP function of
/// the contract amount, so a bounded loop can oscillate without settling — and an unverified exit
/// can mint an invoice netting the recipient MORE than `amount`, breaking exact-net AND potentially
/// pushing the destination past its hard per-fed cap (the allocator sized the move by cap_room). So
/// each pass verifies `predicted_net` with the fee quoted AT the current solve's contract:
///
/// - exact → done (a converged fee always lands here: the solver nets exactly `net` for the fee it
///   was handed);
/// - a hair UNDER → remember it as a SAFE fallback (never-over holds), then keep re-solving for
///   exact;
/// - OVER → re-solve with the fresher fee (a full re-solve, not a linear invoice shrink: with a ppm
///   gateway fee, shrinking the invoice by the excess only closes a fraction of the overshoot per
///   pass).
///
/// On exhaustion return the safe under-netting candidate if one was seen (a true two-step
/// oscillation always yields one — solving with the higher fee under-nets under the lower); only a
/// genuinely unstable quote stream errors `Retryable`.
async fn resolve_receive_gross_up<F, Fut>(
    amount: Msat,
    gateway_fee: fee::GatewayFee,
    mut fed_fee_quote: F,
) -> Result<fee::GrossUp, ExecError>
where
    F: FnMut(Msat) -> Fut,
    Fut: std::future::Future<Output = Result<Msat, ExecError>>,
{
    let mut fed_fee = fed_fee_quote(amount).await?;
    let mut grossed = solve_gross_up(amount, gateway_fee, fed_fee)?;
    let mut safe_under: Option<fee::GrossUp> = None;
    let mut last_over_invoice: Option<u64> = None;
    // `0..=` so EVERY solve is verified, including the one built on the final pass — a
    // stable quote staircase that reaches its exact fixed point on the last re-solve
    // must be accepted, not dropped to `Retryable` unverified.
    for pass in 0..=FED_FEE_REQUOTE_PASSES {
        let verified_fee = fed_fee_quote(grossed.contract_amount).await?;
        let predicted = fee::predicted_net(grossed.invoice_amount, gateway_fee, verified_fee);
        match predicted.0.cmp(&amount.0) {
            std::cmp::Ordering::Equal => return Ok(grossed),
            std::cmp::Ordering::Less => {
                // Never-over holds; keep as the fallback and still try for exact — a
                // verified hair-under solve is the ACCEPTED degradation for every path
                // (live feds cannot guarantee msat-exact: the claim-time fee model gap
                // already under-delivers a hair, which the smokes' slack tolerates;
                // demanding quote-time exactness would spuriously retry real inflows).
                // RESTATE the receive quote to the VERIFIED cost (`invoice − predicted`,
                // what the recipient actually pays): the solve's own `invoice − amount`
                // assumes the requested net was delivered and would UNDERSTATE the cost
                // by the shortfall — every downstream fee-cap check (DirectInflow's
                // receive-side cap, fresh-evacuation costing) reads this field, and an
                // understated quote could approve a move whose real fees exceed
                // `fee_cap`. Send-required moves additionally adjust `rec.amount` to
                // the delivered net at CreateInvoice, keeping the Pay re-check honest.
                safe_under = Some(fee::GrossUp {
                    receive_quote: Msat(grossed.invoice_amount.0.saturating_sub(predicted.0)),
                    ..grossed
                });
            }
            std::cmp::Ordering::Greater => {
                last_over_invoice = Some(grossed.invoice_amount.0);
            }
        }
        if pass == FED_FEE_REQUOTE_PASSES {
            break;
        }
        fed_fee = verified_fee;
        grossed = solve_gross_up(amount, gateway_fee, fed_fee)?;
    }
    // Exactness was not reached in bounded passes. Close the remaining gap with a
    // VERIFIED bisection over the invoice itself: each probe verifies the fee AT the
    // candidate's own contract, so the search needs NO fee monotonicity to stay SAFE —
    // its result is always a verified never-over invoice adjacent to a verified
    // over-netting one (a frontier). On an adversarial non-monotone curve that frontier
    // may not be the GLOBAL maximum never-over invoice (accepted: under-delivery stays
    // bounded by the receive fee and is honestly restated in the quote; safety — never
    // over — is unconditional). Seeding:
    //   - `lo` = the best VERIFIED under-netting candidate when one was seen (returning
    //     it outright could leave a whole fee step on the table when a verified
    //     over-netting invoice exists to bisect toward), else `amount` (always nets
    //     ≤ amount: fees are non-negative).
    //   - `hi` = a verified over-netting invoice; if NO pass over-netted there is
    //     nothing to bisect toward — return the best under candidate directly.
    let (mut lo, mut lo_quote): (u64, Option<Msat>) = match &safe_under {
        Some(under) => (under.invoice_amount.0, None),
        None => (amount.0, None),
    };
    let Some(mut hi) = last_over_invoice else {
        return match safe_under {
            Some(under) => Ok(under),
            // Unreachable for a deterministic stream (every pass returned Equal would
            // have exited; no over and no under means no pass ran) — clean retry.
            None => Err(ExecError::Retryable(
                "receive fee quotes did not converge to a never-over invoice".into(),
            )),
        };
    };
    if hi <= lo {
        // The over candidate sits at/below the under seed (non-monotone curve): the
        // under candidate is already the best verified frontier we can prove.
        if let Some(under) = safe_under {
            return Ok(under);
        }
        hi = lo.saturating_add(1);
    }
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        let mid_invoice = Msat(mid);
        let mid_contract = Msat(mid.saturating_sub(gateway_fee.on(mid_invoice).0));
        let mid_fee = fed_fee_quote(mid_contract).await?;
        if fee::predicted_net(mid_invoice, gateway_fee, mid_fee).0 > amount.0 {
            hi = mid;
        } else {
            lo = mid;
            lo_quote = Some(mid_fee);
        }
    }
    let invoice_amount = Msat(lo);
    let contract_amount = Msat(lo.saturating_sub(gateway_fee.on(invoice_amount).0));
    let fed_fee = match lo_quote {
        Some(fed_fee) => fed_fee,
        None => fed_fee_quote(contract_amount).await?,
    };
    let predicted = fee::predicted_net(invoice_amount, gateway_fee, fed_fee);
    if predicted.0 > amount.0 {
        // Unreachable for a deterministic quote stream (invoice = amount cannot net over
        // with non-negative fees); a non-deterministic stream gets a clean retry.
        return Err(ExecError::Retryable(
            "receive fee quotes did not converge to a never-over invoice".into(),
        ));
    }
    Ok(fee::GrossUp {
        invoice_amount,
        contract_amount,
        // The verified honest cost (invoice − predicted), same restatement convention
        // as the safe-under fallback above.
        receive_quote: Msat(lo.saturating_sub(predicted.0)),
    })
}

/// Solve the §6 receive-side fixed point for a constant federation fee, mapping the pure
/// solver's "no solution" to a terminal [`ExecError::Permanent`] instead of letting the solver —
/// or a re-drive of it — hang. The `None` has two distinguishable causes (spec §15.11): a gateway
/// advertising a ≥100% ppm receive fee (no invoice nets a positive amount), or the doubling
/// search exhausting `u64::MAX` without clearing `net`. Either way the fee is deterministically
/// unsolvable for this gateway, so the intent fails terminally (the operator fixes/repins the
/// gateway and re-runs under a fresh occurrence), never spins.
fn solve_gross_up(
    net: Msat,
    gateway_fee: fee::GatewayFee,
    fed_fee: Msat,
) -> Result<fee::GrossUp, ExecError> {
    fee::gross_up(net, gateway_fee, |_contract| fed_fee).ok_or_else(|| {
        // §15.11: name the ACTUAL cause. `gross_up` returns `None` either because the gateway
        // ppm is ≥ 100% (the recipient can never net a positive amount) or because the doubling
        // bracket exhausted `u64::MAX` without clearing `net` — with a constant fed fee only the
        // former can occur, but the message stays honest about both.
        let cause = if gateway_fee.ppm >= fee::UNSOLVABLE_GATEWAY_PPM {
            format!(
                "gateway receive fee is {} ppm (>= 100% of the invoice)",
                gateway_fee.ppm
            )
        } else {
            format!(
                "the receive-side fixed point did not converge below u64::MAX \
                 (gateway {} ppm, federation fee {} msat)",
                gateway_fee.ppm, fed_fee.0
            )
        };
        ExecError::Permanent(format!(
            "{cause}; no invoice can net the requested {} msat",
            net.0
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

/// Whether an SDK error is the mint's `InsufficientBalanceError` — the send-side fee-quote
/// dry-run's way of saying the source cannot fund the probed outgoing contract at all
/// (verified against the pinned source: the mint's funding selection propagates it
/// `?`-converted, so it sits in the `anyhow` chain un-wrapped). The evacuation sizing search
/// reads it as "this candidate does not fit", never as a transport fault.
fn is_insufficient_balance(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<fedimint_mint_client::InsufficientBalanceError>()
            .is_some()
    })
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

/// Parse a (fixed) move invoice's BOLT11 — the crash-safe input to the §7 send-side cap re-check
/// and the §15.4 expiry belt. A malformed invoice is `Permanent` (it can only come from a corrupt
/// record, not a transient fault).
fn parse_move_invoice(invoice: &Invoice) -> Result<Bolt11Invoice, ExecError> {
    Bolt11Invoice::from_str(&invoice.0)
        .map_err(|e| ExecError::Permanent(format!("parsing move invoice: {e}")))
}

/// Whether minting `amount` into a destination already holding `dest` would push it past the hard
/// per-fed `cap` (§15.2). SATURATING — a colossal amount can never wrap around to "fit".
fn would_exceed_cap(dest: Msat, amount: Msat, cap: Msat) -> bool {
    dest.0.saturating_add(amount.0) > cap.0
}

/// The destination's remaining hard-cap room for an evacuation: `cap − dest`, SATURATING (§15.2).
/// `Some(room)` with `room > 0` bounds the evacuation net; a destination already AT/ABOVE the cap
/// yields room 0, reported as `None` — the caller turns that into a LOUD terminal refusal, never a
/// 0-msat move and never a wrapped-around huge room.
fn evacuation_cap_room(dest: Msat, cap: Msat) -> Option<Msat> {
    let room = cap.0.saturating_sub(dest.0);
    (room > 0).then_some(Msat(room))
}

/// The §15.5 Pay-step cap verdict over the two legs. The receive quote is FIXED (the invoice is
/// minted); the send quote is re-quoted each attempt and can transiently spike:
///   - `Permanent` ONLY when the fixed receive quote ALONE exceeds the cap (no send re-quote can
///     rescue it — a terminal condition);
///   - `Retryable` when the receive fits but the total (with this attempt's send quote) does not
///     (a later attempt may re-quote the send leg lower);
///   - `Ok(())` when both legs fit.
fn pay_step_cap_verdict(
    receive_quote: Msat,
    send_quote: Msat,
    fee_cap: Msat,
) -> Result<(), ExecError> {
    if receive_quote.0 > fee_cap.0 {
        return Err(ExecError::Permanent(format!(
            "fee over cap: the fixed receive quote {} msat alone exceeds fee_cap {} msat",
            receive_quote.0, fee_cap.0
        )));
    }
    if !fee::total_within_cap(receive_quote, send_quote, fee_cap) {
        return Err(ExecError::Retryable(format!(
            "send fee quote over cap this attempt (receive {} + send {} > fee_cap {} msat); retrying",
            receive_quote.0, send_quote.0, fee_cap.0
        )));
    }
    Ok(())
}

/// The §3 stranded-move outcome message: A's send SETTLED but B's receive was not credited. Names
/// the receive-side `detail` and the durable recovery artifact (the saved preimage) so history/UI
/// can present a debited-not-credited move honestly rather than as a silent loss.
fn stranded_outcome(detail: &str) -> String {
    format!(
        "send settled but receive was not credited: {detail}; \
         payment preimage saved on the move record"
    )
}

/// Given a SETTLED send leg (the preimage is already persisted on the record), map the awaited
/// receive state to the resulting terminal `(phase, outcome)` (spec §3). `Claimed` → `Settled` with
/// no failure outcome; any op-terminal non-claim STRANDS (terminal, loud) — the send debited the
/// source but the destination was never credited, which re-driving cannot fix. Pure so the
/// transition is unit-testable without a live federation.
fn settle_after_successful_send(receive: ReceiveState) -> (MovePhase, Option<String>) {
    match receive {
        ReceiveState::Claimed => (MovePhase::Settled, None),
        ReceiveState::Expired => (
            MovePhase::Stranded,
            Some(stranded_outcome("receive invoice expired")),
        ),
        ReceiveState::Failed(msg) => (MovePhase::Stranded, Some(stranded_outcome(&msg))),
    }
}

/// The §15.7 never-over TOCTOU verdict: the lnv2 mint re-fetches the gateway fee and sizes the
/// COMMITTED contract with it, so a fee change between our verified quote and the mint shows up as
/// `committed != quoted`. A DROP mints a LARGER contract (the destination would net MORE than
/// asked); a strict inequality refuses either direction terminally BEFORE the invoice is surfaced
/// or paid (the unpaid invoice expires unclaimed; a re-run re-quotes). Pure so the comparison is
/// unit-testable without a live federation.
fn verify_committed_receive_contract(committed: Msat, quoted: Msat) -> Result<(), ExecError> {
    if committed != quoted {
        return Err(ExecError::Permanent(
            "gateway receive fee changed between quote and mint; re-run".into(),
        ));
    }
    Ok(())
}

/// Classify a failure from reading the committed receive contract (§15.7 resume check). That read
/// touches only the destination's LOCAL op-log, so the ONLY transient cause is the destination
/// client not being open on this pass (`dest_open == false`); a later reconcile can open it, so stay
/// `Retryable`. With the client open, an op-not-found / wrong-leg / malformed-quote error is durable
/// corruption a re-drive can never clear, so it is `Permanent` (loud terminal) rather than a Pending
/// livelock — the same deterministic-vs-transient split §15.4 makes for send rejections. Pure so the
/// classification is unit-tested without a live federation.
fn classify_receive_contract_read_error(
    err: anyhow::Error,
    dest_open: bool,
    move_key: &str,
) -> ExecError {
    if dest_open {
        ExecError::Permanent(format!(
            "receive contract check failed on a durable op-log read (move {move_key}); the receive \
             op is corrupt or missing: {err}"
        ))
    } else {
        retryable(err)
    }
}

fn verify_replayable_receive_contract(
    committed: Msat,
    quoted: Option<Msat>,
) -> Result<(), ExecError> {
    let quoted = quoted.ok_or_else(|| {
        ExecError::Permanent(
            "receive op is missing the quoted contract amount; re-run under a fresh occurrence"
                .into(),
        )
    })?;
    verify_committed_receive_contract(committed, quoted)
}

/// The honest net a receive actually delivers: `delivered` when the §6 fee fixed point settled a
/// hair UNDER the ask, else the exact `ask` (§15.11). Committed UNCONDITIONALLY into the receive
/// op's `MoveMeta.amount` — the documented crash-safe amount — so a receive-only `DirectInflow`
/// records the delivered net, not the ask. Never over: a `delivered ≥ ask` keeps the ask.
fn delivered_move_amount(delivered: Msat, ask: Msat) -> Msat {
    if delivered < ask {
        delivered
    } else {
        ask
    }
}

/// Map a classified [`SendError`] from `pay` to the executor's terminal/retryable dispositions
/// (§15.4): a deterministic `Rejected` is `Permanent` (re-driving the same invoice can never
/// succeed — a fresh occurrence must re-mint), a `Transport` fault stays `Retryable`.
fn map_send_error(e: SendError) -> ExecError {
    match e {
        SendError::Rejected(msg) => ExecError::Permanent(msg),
        SendError::Transport(err) => ExecError::Retryable(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fedimint_bip39::Mnemonic;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt as _;
    use wallet_core::{Action, Actor, IdempotencyKey, IntentStatus, ReasonCode};

    const FED_A: FederationId = FederationId([0xAA; 32]);
    const FED_B: FederationId = FederationId([0xBB; 32]);

    /// A constructible executor over an in-memory db — enough to exercise the `perform` gate,
    /// which decides `Move`/`Evacuate` BEFORE any federation I/O (no join needed).
    async fn test_executor() -> FedimintExecutor {
        let db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(db));
        FedimintExecutor::new(mc, journal, None, None)
    }

    fn intent(action: Action) -> Intent {
        let max_fee = action.fee_cap();
        Intent {
            idempotency_key: IdempotencyKey("gate-test".into()),
            action,
            max_fee,
            status: IntentStatus::Pending,
            reason: ReasonCode::UserInitiated,
            actor: Actor::User,
            created_at_ms: 0,
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

    /// Phase 3.A un-gates `Evacuate`: `MovePlan::from_action` now maps it to the SAME
    /// send-required plan as `Move` (drain `from` into `to`), so `perform` drives it through
    /// the identical validated two-leg path instead of returning `Unsupported`. Assert the
    /// pure mapping threads from/to/amount/fee_cap through with `send_required == true`.
    #[test]
    fn evacuate_maps_to_a_send_required_plan() {
        let action = Action::Evacuate {
            from: FED_A,
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(10_000),
        };
        let plan = MovePlan::from_action(&action).expect("Evacuate must map to a plan");
        assert_eq!(plan.from, Some(FED_A));
        assert_eq!(plan.to, FED_B);
        assert_eq!(plan.amount, Msat(50_000));
        assert_eq!(plan.fee_cap, Msat(10_000));
        assert!(
            plan.send_required,
            "an evacuate drains `from` into `to`, so it requires a send leg like a Move"
        );
    }

    #[test]
    fn evacuation_fee_fit_reserves_source_side_fees() {
        let full_balance_with_fees = FreshMoveCost {
            invoice_amount: Msat(100_100),
            receive_quote: Msat(100),
            send_quote: Msat(200),
        };
        assert!(
            !evacuation_cost_fits(full_balance_with_fees, Msat(1_000), Msat(100_000)),
            "a full-balance net evacuation cannot fit once receive/send fees make the source debit exceed spendable"
        );

        let quoted_down = FreshMoveCost {
            invoice_amount: Msat(99_700),
            receive_quote: Msat(100),
            send_quote: Msat(200),
        };
        assert!(
            evacuation_cost_fits(quoted_down, Msat(1_000), Msat(100_000)),
            "a quoted-down evacuation fits when invoice + send fees stay within source spendable and fee_cap"
        );

        let over_cap = FreshMoveCost {
            receive_quote: Msat(900),
            send_quote: Msat(200),
            ..quoted_down
        };
        assert!(
            !evacuation_cost_fits(over_cap, Msat(1_000), Msat(100_000)),
            "fee_cap still bounds the total move cost"
        );
    }

    /// The downsizing search must not assume its predicate is monotone below the lnv2
    /// minimum-contract floor. Regression for the skipped-window bug: with desired 500_000
    /// and only ~5_500 msat affordable, an unclamped bisection from 0 halves straight from
    /// the over-budget region into the below-minimum region (250_000 → … → 3_906, all
    /// unfit) and abandons the evacuation; the clamped search stays in `[5_000, desired]`,
    /// where fits-then-doesn't holds, and finds the window.
    #[tokio::test]
    async fn downsizing_search_finds_a_feasible_window_above_the_contract_floor() {
        let affordable = |amount: u64| async move { Ok(amount <= 5_500) };
        let found = largest_fitting_amount(MINIMUM_INCOMING_CONTRACT_MSAT, 499_999, affordable)
            .await
            .expect("probes never fail");
        assert_eq!(found, Some(5_500));
    }

    #[tokio::test]
    async fn downsizing_search_edge_cases() {
        // Nothing in range fits → None (the genuinely-infeasible evacuation).
        let none = largest_fitting_amount(5_000, 100_000, |_| async { Ok(false) })
            .await
            .expect("probes never fail");
        assert_eq!(none, None);

        // Everything fits → the top of the range.
        let all = largest_fitting_amount(5_000, 100_000, |_| async { Ok(true) })
            .await
            .expect("probes never fail");
        assert_eq!(all, Some(100_000));

        // An empty range (desired below the floor) is None without probing.
        let empty = largest_fitting_amount(5_000, 4_999, |_| async {
            panic!("an empty range must not be probed")
        })
        .await
        .expect("probes never run");
        assert_eq!(empty, None);

        // Exactly the floor fitting is found, one msat under the floor is out of scope.
        let at_floor =
            largest_fitting_amount(5_000, 100_000, |amount| async move { Ok(amount <= 5_000) })
                .await
                .expect("probes never fail");
        assert_eq!(at_floor, Some(5_000));
    }

    /// Regression for the aborted full-balance evacuation: the send-side dry-run quote fails
    /// with the mint's `InsufficientBalanceError` when a probed candidate cannot be funded, and
    /// the sizing search must classify that as "does not fit" (keep probing smaller amounts) —
    /// never as a `Retryable` transport fault that aborts the search. The classifier walks the
    /// whole anyhow chain so an added `.context(...)` wrap cannot silently break it.
    #[test]
    fn insufficient_balance_is_classified_as_unfit_not_transport_failure() {
        let root = fedimint_mint_client::InsufficientBalanceError {
            requested_amount: fedimint_core::Amount::from_msats(100_000),
            total_amount: fedimint_core::Amount::from_msats(60_000),
        };
        let plain = anyhow::Error::from(root.clone());
        assert!(is_insufficient_balance(&plain));

        let wrapped = anyhow::Error::from(root).context("quoting send fee for evacuation probe");
        assert!(is_insufficient_balance(&wrapped));

        assert!(
            !is_insufficient_balance(&anyhow::anyhow!("connection reset by peer")),
            "an ordinary transport error must stay Retryable"
        );
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
    fn destination_cap_math_refuses_over_cap_and_downsizes_evacuations() {
        // §15.2. A non-evacuation inflow is refused pre-mint when dest + amount would exceed the
        // cap, and permitted right up to the cap (inclusive).
        let cap = Msat(5_000_000);
        assert!(would_exceed_cap(Msat(4_900_000), Msat(200_000), cap));
        assert!(!would_exceed_cap(Msat(4_800_000), Msat(200_000), cap));
        assert!(!would_exceed_cap(Msat(0), cap, cap));
        // SATURATING: a colossal amount can never wrap around to "fit".
        assert!(would_exceed_cap(Msat(1), Msat(u64::MAX), cap));

        // An evacuation is downsized to the destination's remaining cap room...
        assert_eq!(
            evacuation_cap_room(Msat(4_000_000), cap),
            Some(Msat(1_000_000))
        );
        // ...and clamped to min(desired, room): a small desired stays, a large one is capped.
        let room = evacuation_cap_room(Msat(4_000_000), cap).expect("positive room");
        assert_eq!(500_000_u64.min(room.0), 500_000);
        assert_eq!(9_000_000_u64.min(room.0), 1_000_000);
        // A destination already AT or ABOVE the cap yields NO room — a loud refusal, never a
        // 0-msat move and never a wrapped-around huge room (saturating).
        assert_eq!(evacuation_cap_room(cap, cap), None);
        assert_eq!(evacuation_cap_room(Msat(cap.0 + 1), cap), None);
    }

    #[test]
    fn pay_step_cap_verdict_splits_retryable_from_permanent() {
        // §15.5. Both legs fit -> Ok.
        let cap = Msat(10_000);
        assert!(pay_step_cap_verdict(Msat(3_000), Msat(4_000), cap).is_ok());
        // The receive quote fits but the send re-quote spiked the total over cap -> Retryable
        // (a later attempt may re-quote the send leg lower), NOT a terminal strand.
        assert!(matches!(
            pay_step_cap_verdict(Msat(3_000), Msat(9_000), cap),
            Err(ExecError::Retryable(_))
        ));
        // The FIXED receive quote alone exceeds the cap -> Permanent (unrescuable).
        assert!(matches!(
            pay_step_cap_verdict(Msat(11_000), Msat(0), cap),
            Err(ExecError::Permanent(_))
        ));
        // Receive exactly at the cap is fine; a send spike above it is Retryable, not Permanent.
        assert!(matches!(
            pay_step_cap_verdict(cap, Msat(1), cap),
            Err(ExecError::Retryable(_))
        ));
    }

    #[test]
    fn deterministic_send_rejection_fails_the_move_permanently() {
        // §15.4. A deterministic rejection from the send leg (expired / wrong-currency /
        // unsupported / fee-limit) maps to a terminal Permanent with an actionable message — the
        // move does NOT reset to Pending and livelock. A transport fault stays Retryable.
        let rejected = map_send_error(SendError::Rejected(
            "lnv2 send deterministically rejected the invoice: Invoice has expired".into(),
        ));
        assert!(matches!(rejected, ExecError::Permanent(msg) if msg.contains("expired")));

        let transport = map_send_error(SendError::Transport(anyhow::anyhow!(
            "connection reset by peer"
        )));
        assert!(matches!(transport, ExecError::Retryable(_)));
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
            amount: Msat(50_000),
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
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
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

    // ---- §15.10: the extracted gross-up loop, golden over scripted quote streams -----------
    //
    // Each fed-fee "stream" is a pure function of the CONTRACT amount (the federation fee is a
    // step function of the contract, spec §6). `resolve_receive_gross_up` verifies every candidate
    // against the fee at ITS OWN contract, so the SACRED invariant is: whatever invoice it accepts,
    // the recipient nets ≤ the ask (never over). `assert_never_over` recomputes that independently.

    /// Run the extracted loop against a pure `fed`-fee stream (contract → federation fee).
    async fn resolve_with_fed<Fed: Fn(u64) -> u64>(
        amount: Msat,
        gw: fee::GatewayFee,
        fed: Fed,
    ) -> Result<fee::GrossUp, ExecError> {
        resolve_receive_gross_up(amount, gw, |contract| {
            let quote = fed(contract.0);
            async move { Ok::<Msat, ExecError>(Msat(quote)) }
        })
        .await
    }

    /// Assert the accepted invoice is verified NEVER-OVER: recompute the recipient's net with the
    /// fee at the returned contract and require it ≤ the ask; also check the reported contract and
    /// receive quote are the honest derived values. Holds for ANY pure fed-fee stream by the loop's
    /// per-candidate verification, so it is the right golden regardless of which branch was taken.
    async fn assert_never_over<Fed: Fn(u64) -> u64 + Copy>(
        amount: Msat,
        gw: fee::GatewayFee,
        fed: Fed,
    ) -> fee::GrossUp {
        let g = resolve_with_fed(amount, gw, fed)
            .await
            .expect("a pure deterministic stream always converges to a never-over invoice");
        let net = fee::predicted_net(g.invoice_amount, gw, Msat(fed(g.contract_amount.0)));
        assert!(
            net.0 <= amount.0,
            "NEVER-OVER VIOLATED: invoice {} nets {} > asked {}",
            g.invoice_amount.0,
            net.0,
            amount.0
        );
        assert_eq!(
            g.contract_amount,
            Msat(g.invoice_amount.0.saturating_sub(gw.on(g.invoice_amount).0)),
            "contract must be the gateway-reduced invoice"
        );
        assert_eq!(
            g.receive_quote,
            Msat(g.invoice_amount.0.saturating_sub(net.0)),
            "receive_quote must be the honest cost invoice − net"
        );
        g
    }

    const ZERO_GW: fee::GatewayFee = fee::GatewayFee {
        base_msat: Msat(0),
        ppm: 0,
    };

    #[tokio::test]
    async fn gross_up_stream_stable_converges_exactly() {
        // A constant fee converges on the first verify (Equal): invoice = amount + fee, exact net.
        let g = assert_never_over(Msat(100_000), ZERO_GW, |_c| 200).await;
        assert_eq!(g.invoice_amount, Msat(100_200));
        assert_eq!(
            fee::predicted_net(g.invoice_amount, ZERO_GW, Msat(200)),
            Msat(100_000)
        );

        // Same, but through a real (non-zero) gateway fee so the extraction's gateway.on() path is
        // exercised end to end — still exactly never-over.
        let gw = fee::GatewayFee {
            base_msat: Msat(50),
            ppm: 5_000,
        };
        let _ = assert_never_over(Msat(100_000), gw, |_c| 200).await;
    }

    #[tokio::test]
    async fn gross_up_stream_two_step_oscillation_stays_never_over() {
        // The fee flips high↔low across the two candidate invoices the solve ping-pongs between, so
        // no pass reaches Equal — the loop must fall back to a verified never-over frontier.
        let fed = |c: u64| if c <= 100_400 { 600 } else { 200 };
        let _ = assert_never_over(Msat(100_000), ZERO_GW, fed).await;
    }

    #[tokio::test]
    async fn gross_up_stream_staircase_converges_on_the_last_pass() {
        // A monotone staircase that only reaches its exact fixed point on the FINAL re-solve — the
        // `0..=PASSES` inclusive bound must accept it rather than drop to Retryable unverified.
        let fed = |c: u64| {
            let over = c.saturating_sub(100_000);
            (100 + (over / 100) * 100).min(400)
        };
        let g = assert_never_over(Msat(100_000), ZERO_GW, fed).await;
        // It converges EXACTLY (an Equal exit), netting the full ask.
        assert_eq!(
            fee::predicted_net(g.invoice_amount, ZERO_GW, Msat(fed(g.contract_amount.0))),
            Msat(100_000)
        );
    }

    #[tokio::test]
    async fn gross_up_stream_non_monotone_over_below_under_stays_never_over() {
        // A non-monotone stream where the verified OVER candidate sits at a SMALLER invoice than the
        // verified UNDER candidate (`hi <= lo`): the loop must fall back to the under frontier, never
        // bisecting into an over-netting invoice.
        let fed = |c: u64| match c {
            100_000 => 500,
            100_500 => 600,
            100_600 => 100,
            100_100 => 50,
            100_050 => 40,
            _ => 500,
        };
        let _ = assert_never_over(Msat(100_000), ZERO_GW, fed).await;
    }

    #[tokio::test]
    async fn gross_up_stream_changing_between_pass_loop_and_bisection_stays_never_over() {
        // The fee regime CHANGES once the pass loop exhausts and bisection begins: the pass phase
        // (first 5 quotes: 1 seed + 4 verifies) oscillates so no Equal is reached, then the fee
        // DROPS to a constant for the bisection. The bisection re-verifies with the CURRENT fee, so
        // the accepted invoice is never-over under the regime it was actually verified against.
        let calls = std::cell::Cell::new(0u64);
        let g = resolve_receive_gross_up(Msat(100_000), ZERO_GW, |contract| {
            let n = calls.get();
            calls.set(n + 1);
            let quote = if n < 5 {
                if contract.0 <= 100_400 {
                    600
                } else {
                    200
                }
            } else {
                200
            };
            async move { Ok::<Msat, ExecError>(Msat(quote)) }
        })
        .await
        .expect("a stream that changes between phases still converges to a never-over invoice");
        // Verified against the bisection-phase fee (200), the accepted invoice nets ≤ the ask.
        assert!(
            fee::predicted_net(g.invoice_amount, ZERO_GW, Msat(200)).0 <= 100_000,
            "accepted invoice {} nets over the ask under the bisection-phase fee",
            g.invoice_amount.0
        );
        assert_eq!(
            g.contract_amount,
            Msat(
                g.invoice_amount
                    .0
                    .saturating_sub(ZERO_GW.on(g.invoice_amount).0)
            )
        );
    }

    // ---- §15.7: never-over TOCTOU verdict on the committed contract -----------------------

    #[test]
    fn committed_contract_mismatch_is_permanent_match_proceeds() {
        // Equal committed contract → proceeds (the gateway fee did not move between quote and mint).
        verify_committed_receive_contract(Msat(95_000), Msat(95_000))
            .expect("an unchanged committed contract proceeds to surface/pay");
        verify_replayable_receive_contract(Msat(95_000), Some(Msat(95_000)))
            .expect("a recovered unchanged receive proceeds to surface/pay");
        // A fee DROP mints a LARGER contract than we sized → the destination would net MORE than
        // asked → refuse terminally (do NOT surface/pay); a re-run re-quotes.
        let over = verify_committed_receive_contract(Msat(96_000), Msat(95_000))
            .expect_err("a larger committed contract is refused");
        assert!(
            matches!(&over, ExecError::Permanent(msg) if msg.contains("fee changed between quote and mint")),
            "{over:?}"
        );
        // A fee RISE mints a smaller contract → still a mismatch → refused (strict equality).
        assert!(matches!(
            verify_committed_receive_contract(Msat(94_000), Msat(95_000)),
            Err(ExecError::Permanent(_))
        ));
        let missing = verify_replayable_receive_contract(Msat(95_000), None)
            .expect_err("a recovered receive without quoted contract metadata cannot be verified");
        assert!(
            matches!(&missing, ExecError::Permanent(msg) if msg.contains("missing the quoted contract amount")),
            "{missing:?}"
        );
    }

    #[test]
    fn corrupt_receive_contract_read_is_permanent_open_transient_closed() {
        // Destination client not open this pass → the read could not run for a transient reason a
        // later reconcile can fix → Retryable (leave Pending), NOT a terminal failure.
        let closed = classify_receive_contract_read_error(
            anyhow::anyhow!("federation deadbeef not joined/opened"),
            false,
            "move-1",
        );
        assert!(
            matches!(&closed, ExecError::Retryable(msg) if msg.contains("not joined/opened")),
            "{closed:?}"
        );
        // Destination client IS open, yet the local op-log read failed → durable corruption
        // (op absent / wrong leg / malformed quote) a re-drive can never clear → Permanent, so the
        // poisoned intent fails loudly instead of livelocking Pending forever.
        let open = classify_receive_contract_read_error(
            anyhow::anyhow!("operation abc is not a receive operation"),
            true,
            "move-1",
        );
        assert!(
            matches!(&open, ExecError::Permanent(msg)
                if msg.contains("corrupt or missing") && msg.contains("move-1")),
            "{open:?}"
        );
    }

    // ---- §3: the Stranded transition (send settled, receive not credited) -----------------

    #[test]
    fn settle_after_send_strands_on_terminal_non_claim() {
        // A claimed receive settles cleanly, no failure outcome.
        assert_eq!(
            settle_after_successful_send(ReceiveState::Claimed),
            (MovePhase::Settled, None)
        );
        // An expired receive after a settled send STRANDS, naming the saved preimage.
        let (phase, outcome) = settle_after_successful_send(ReceiveState::Expired);
        assert_eq!(phase, MovePhase::Stranded);
        let msg = outcome.expect("a stranded move carries an outcome");
        assert!(msg.contains("preimage saved"), "{msg}");
        assert!(msg.contains("receive invoice expired"), "{msg}");
        // A failed receive strands too, carrying the failure detail plus the saved preimage.
        let (phase, outcome) =
            settle_after_successful_send(ReceiveState::Failed("forfeited".into()));
        assert_eq!(phase, MovePhase::Stranded);
        let msg = outcome.expect("a stranded move carries an outcome");
        assert!(
            msg.contains("preimage saved") && msg.contains("forfeited"),
            "{msg}"
        );
    }

    #[test]
    fn successful_send_then_terminal_failed_receive_strands_with_preimage() {
        // Mirror the `AwaitSettle` Success arm without live I/O: persist the preimage FIRST (§3),
        // then map the op-terminal receive. A failed receive after a settled send leaves the record
        // `Stranded`, still carrying the preimage, routed to the terminal `Failed` surface with a
        // "preimage saved" outcome (`perform` returns `Permanent(outcome)`).
        let mut rec = MoveRecord {
            key: IdempotencyKey("move-strand".into()),
            from: Some(FED_A),
            to: FED_B,
            amount: Msat(100_000),
            fee_cap: Msat(2_000),
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: true,
            invoice: Some(Invoice("lnbc1pstrand".into())),
            recv_op: Some(crate::types::OperationId([0x01; 32])),
            send_op: Some(crate::types::OperationId([0x02; 32])),
            phase: MovePhase::Sending,
            outcome: None,
            preimage: None,
            receive_fee_quoted: Some(Msat(120)),
            send_fee_quoted: Some(Msat(340)),
        };
        let preimage = crate::types::Preimage([0x9a; 32]);
        rec.preimage = Some(preimage);
        let (phase, outcome) = settle_after_successful_send(ReceiveState::Failed(
            "gateway claimed A but never funded B".into(),
        ));
        rec.phase = phase;
        rec.outcome = outcome;

        assert_eq!(rec.phase, MovePhase::Stranded);
        assert_eq!(
            rec.preimage,
            Some(preimage),
            "the recovery proof is preserved"
        );
        assert_eq!(next_step(&rec), MoveStep::Failed);
        let msg = rec.outcome.clone().expect("stranded outcome present");
        assert!(msg.contains("preimage saved"), "{msg}");
        assert!(msg.contains("never funded B"), "{msg}");
    }

    // ---- §15.11: DirectInflow hair-under records the DELIVERED net unconditionally ----------

    #[test]
    fn delivered_move_amount_records_hair_under_unconditionally() {
        // Exact solve: the ask is delivered.
        assert_eq!(
            delivered_move_amount(Msat(50_000), Msat(50_000)),
            Msat(50_000)
        );
        // A hair under (receive-only DirectInflow OR send-required Move alike): the DELIVERED net is
        // committed into MoveMeta.amount, not the ask — the honest crash-safe amount (§15.11).
        assert_eq!(
            delivered_move_amount(Msat(49_990), Msat(50_000)),
            Msat(49_990)
        );
        // Never over: a delivered ≥ ask keeps the ask (the gross-up never over-delivers).
        assert_eq!(
            delivered_move_amount(Msat(50_001), Msat(50_000)),
            Msat(50_000)
        );
    }
}
