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
//! send_op (the send op-id is deterministic; a re-`pay` returns `AlreadyInFlight`),
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
use crate::journal::{FedimintJournal, LedgerRepairOracle};
use crate::move_protocol::{
    assemble_move_record, next_step, Leg, MoveMeta, MoveParams, MovePhase, MovePlan, MoveRecord,
    MoveRole, MoveStep, OpArtifact,
};
use crate::multi_client::{MultiClient, ReceiveState, SendError, SendOutcome, SendState};
use crate::types::{GatewayUrl, Invoice};
use async_trait::async_trait;
use fedimint_core::invite_code::InviteCode;
use lightning_invoice::Bolt11Invoice;
use std::collections::BTreeMap;
use std::str::FromStr as _;
use std::sync::Arc;
use wallet_core::{
    Action, Actor, EvacFeeCap, ExecError, Executor, FederationId, Intent, Journal, Msat,
    OperationId, PerformOutcome,
};

/// Pinned lnv2 requires the gateway-reduced incoming contract to be at least 5 sats
/// (`MINIMUM_INCOMING_CONTRACT_AMOUNT`) before it will mint a receive invoice.
pub const MINIMUM_INCOMING_CONTRACT_MSAT: u64 =
    fedimint_lnv2_common::MINIMUM_INCOMING_CONTRACT_AMOUNT.msats;

/// How many times to re-quote the federation receive fee at the refined contract amount
/// while sizing the invoice. `receive_fee_quote` is async but [`fee::gross_up`]'s fed-fee
/// closure is sync, so the executor resolves the (contract-amount-dependent) fee with a
/// short async fixed point; a couple of passes converge for any real fee (ppm slope < 1).
const FED_FEE_REQUOTE_PASSES: u32 = 3;

/// The money-path move fallback prices registered gateways within this wall-clock budget. Without a
/// bound a federation advertising a large or unresponsive gateway set makes a single perform issue
/// one fee round-trip per gateway before it can pick a route, capped only by `perform_timeout`. A
/// TIME budget rather than a candidate-count cap bounds the work WITHOUT a deterministic blind
/// spot: when gateways answer quickly — the normal case — every registered gateway is still priced,
/// so a fitting route in ANY position is found, and only a genuinely slow/large registry is
/// truncated. A truncated scan then falls through to plain both-ends validation rather than falsely
/// reporting "no route fits the cap" over an unexamined suffix. `perform_timeout` remains the
/// backstop for a single slow round-trip.
const FALLBACK_MOVE_ROUTE_BUDGET_MS: u64 = 10_000;

/// The mint's per-input/per-output BASE fee at the pin: 100 msat, and non-configurable —
/// `FeeConsensus::new` hard-codes it (`fedimint-mint-common/src/config.rs`).
const MINT_FEE_BASE_MSAT: u64 = 100;

/// The mint's per-note PROPORTIONAL fee CEILING at the pin: `FeeConsensus::new` REFUSES a
/// federation configuring more than 1_000 ppm. The live rate is not readable through any seam the
/// sizing search has, so the oscillation bound uses this ceiling — erring LARGE, which only makes
/// the search probe more, where erring small would refuse an executable evacuation unprobed.
const MINT_FEE_PPM_CEILING: u64 = 1_000;

/// How many note-selection boundaries the robustness probe visits per direction. DELIBERATELY
/// BOUNDED, not complete: the oscillation bound `A` bounds ONE vertical fee jump and says nothing
/// about how many boundaries separate a failing probe from a feasible window, so a window beyond
/// this many boundaries is MISSED and the evacuation keeps retrying. ADR-0029 accepts that
/// residual explicitly — the consequence is a retry, not a burn.
const NOTE_BOUNDARY_PROBES: usize = 8;

/// The per-leg gateway ppm envelope gateways are INTENDED to advertise (`SEND_FEE_LIMIT` 1.5%,
/// `RECEIVE_FEE_LIMIT` 0.5%). Nothing ENFORCES it at our pin: `PaymentFee` derives a lexicographic
/// `PartialOrd` over (base, parts_per_million) and the limit check is a single `.le(..)`, so a
/// compliant base admits an arbitrary ppm. Crossing it WARNS and never rejects — our fee cap is the
/// only real bound and already refuses a route it cannot afford, whereas a second, stricter
/// admissibility test could refuse an evacuation over the only live route and strand the funds
/// (ADR-0029).
const SEND_PPM_ENVELOPE: u64 = 15_000;
const RECEIVE_PPM_ENVELOPE: u64 = 5_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FreshMoveCost {
    invoice_amount: Msat,
    receive_quote: Msat,
    send_quote: Msat,
}

impl FreshMoveCost {
    /// The DELIVERED NET this quote actually credits the destination: the fixed invoice minus
    /// the receive-side fee quoted against it (CONTEXT.md, "Delivered net").
    ///
    /// **This is the only amount a fee cap may be computed from.** The SIZED ASK — the candidate
    /// the search is probing — is an intention; it is what we will request an invoice for, not
    /// what arrives, and the two diverge whenever the gross-up settles a verified hair under.
    /// Capping against the ask authorises a proportional fee on value nobody received.
    ///
    /// It is derived HERE, from the cost, so no caller can supply the wrong number. That is the
    /// whole point: the same defect — a cap bound to the ask rather than the delivery — reached
    /// five separate call sites while each one was free to pass its own idea of "the net".
    ///
    /// Well-defined for every quote the search can admit. `resolve_receive_gross_up` maintains
    /// `invoice_amount - receive_quote == the verified delivered net` on every return path: an
    /// exact solve returns it unchanged, and both the hair-under and bisection tails RESTATE
    /// `receive_quote` as `invoice - predicted` precisely so this subtraction stays honest.
    /// `CandidateQuote::Unquotable` carries no cost and never reaches a cap comparison.
    fn delivered_net(self) -> Msat {
        Msat(self.invoice_amount.0.saturating_sub(self.receive_quote.0))
    }

    fn total_fee(self) -> Msat {
        Msat(self.receive_quote.0.saturating_add(self.send_quote.0))
    }

    fn source_debit(self) -> Msat {
        Msat(self.invoice_amount.0.saturating_add(self.send_quote.0))
    }
}

/// What one probed evacuation amount costs, from the sizing search's point of view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CandidateQuote {
    /// The candidate priced: its full two-leg cost.
    Priced(FreshMoveCost),
    /// The candidate cannot be quoted at all — its gateway-reduced contract is below the lnv2
    /// minimum, or the source's note inventory cannot fund the probed outgoing contract.
    /// `source_shortfall` is how far the source fell short WHEN THE MINT MEASURED IT
    /// (`InsufficientBalanceError` carries both figures); `None` when the candidate failed for a
    /// reason with no measurable gap. The gap matters because a note-selection boundary can make
    /// a LARGER amount fundable again, and only a measured, small gap licenses probing above.
    Unquotable { source_shortfall: Option<u64> },
}

impl CandidateQuote {
    /// The affordability verdict for this candidate: whether the source can fund the whole
    /// `source_debit`, and by how much it missed when it cannot.
    fn affordability(self, spendable: Msat) -> ProbeVerdict {
        match self {
            CandidateQuote::Priced(cost) => match cost.source_debit().0.checked_sub(spendable.0) {
                Some(0) | None => ProbeVerdict::fits(),
                Some(shortfall) => ProbeVerdict::missed_by(shortfall),
            },
            CandidateQuote::Unquotable { source_shortfall } => match source_shortfall {
                Some(shortfall) => ProbeVerdict::missed_by(shortfall),
                None => ProbeVerdict::missed_immeasurably(),
            },
        }
    }
}

/// One probed amount's verdict for [`largest_fitting_amount`]. `shortfall` is how far the
/// candidate missed by in msat, and is what licenses a boundary probe above it;
/// [`u64::MAX`] means the miss was not measurable, which never licenses one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProbeVerdict {
    fits: bool,
    shortfall: u64,
}

impl ProbeVerdict {
    fn fits() -> Self {
        Self {
            fits: true,
            shortfall: 0,
        }
    }

    fn missed_by(shortfall: u64) -> Self {
        Self {
            fits: false,
            shortfall,
        }
    }

    fn missed_immeasurably() -> Self {
        Self::missed_by(u64::MAX)
    }
}

fn evacuation_cost_fits(cost: FreshMoveCost, fee_cap: Msat, spendable: Msat) -> bool {
    cost.total_fee() <= fee_cap && cost.source_debit() <= spendable
}

fn raw_fee_cap_error(operation: &str, quote: u64, fee_cap: Msat) -> ExecError {
    ExecError::Permanent(format!(
        "{operation} fee quote {quote} msat exceeds fee cap {} msat",
        fee_cap.0
    ))
}

fn raw_pay_quote_error(lowest_quote: Option<u64>, fee_cap: Msat, from: FederationId) -> ExecError {
    // Pre-fund quote failures TERMINALIZE (Permanent): a user pay is one-shot — the
    // pre-6a CLI returned the error and was done, and leaving the intent Pending would
    // let a background reconcile settle it hours later, after the user already paid the
    // bill another way ("thought it failed, later succeeded"). A deliberate retry is a
    // new operation (docs/phase6a-plan.md §6a.6; ADR-0024). Matches raw receive
    // (`raw_fee_cap_error`), which already terminalizes its over-cap quote.
    match lowest_quote {
        Some(quote) => {
            let fee_cap = fee_cap.0;
            ExecError::Permanent(format!(
                "raw pay fee quote {quote} msat exceeds fee cap {fee_cap} msat"
            ))
        }
        None => {
            let federation = from.to_hex();
            ExecError::Permanent(format!(
                "no lnv2 gateway produced a send fee quote for federation {federation}"
            ))
        }
    }
}

fn pre_fund_reservation_error(error: ExecError) -> ExecError {
    let reason = match error {
        ExecError::Retryable(reason) | ExecError::Permanent(reason) => reason,
        ExecError::Unsupported => "reservation journal read is unsupported".to_owned(),
    };
    ExecError::Retryable(format!(
        "reservation scan failed before funding; leaving the intent pending: {reason}"
    ))
}

fn keep_cheapest_fitting<T>(
    best: Option<(Msat, T)>,
    candidate: (Msat, T),
    fee_cap: Msat,
) -> Option<(Msat, T)> {
    if candidate.0 > fee_cap {
        return best;
    }
    match best {
        Some((cheapest, _)) if cheapest <= candidate.0 => best,
        _ => Some(candidate),
    }
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
    /// fresh operations use it instead of the federation's registered list — devimint does NOT
    /// auto-register its LDK gateway into that list, so `mc.gateways` is empty there (runbook §4)
    /// and the CLI must supply the URL directly. A RESUMED move ignores this and reuses the
    /// gateway already pinned in its `MoveRecord`.
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
        let operation_key = intent.operation_correlation_key();

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
            &operation_key,
            &artifacts,
        ) {
            Some(gateway) => gateway,
            // Prefer the route `decide()` already priced this move against (still validated), then
            // scan for the cheapest gateway serving BOTH ends of a send-required move (§15.6); a
            // receive-only inflow (`plan.from == None`) validates only the destination.
            //
            // An `Evacuate` arrives here with a NON-final amount: `size_fresh_evacuation` runs
            // after this and downsizes the ask to what the dying federation can actually afford
            // under `fee_cap`. Pricing routes against the pre-sizing ask would judge them on an
            // amount no one will ever move, so an evacuation resolves by VALIDATION only.
            None => {
                self.resolve_move_gateway(
                    plan,
                    action_gateway(&intent.action),
                    move_amount_is_final(&intent.action),
                )
                .await?
            }
        };

        let params = MoveParams {
            key: intent.idempotency_key.clone(),
            operation_key,
            from: plan.from,
            to: plan.to,
            amount: plan.amount,
            fee_cap: plan.fee_cap,
            fee_cap_components: plan.fee_cap_components,
            gateway,
            send_required: plan.send_required,
        };
        Ok(assemble_move_record(params, &artifacts, cached))
    }

    /// The gateway a move should actually use (spec §7, §15.6), in precedence order:
    ///
    /// 1. the explicitly PINNED gateway (⟦D4⟧; devimint's LDK gateway is not auto-registered, so
    ///    the CLI passes it directly — runbook §4). An operator pin overrides route selection
    ///    entirely, planning included;
    /// 2. the route `decide()` preselected on the action — but only while it still SERVES both
    ///    ends. It is a HINT, not a constraint: `perform` can run long after planning (retries,
    ///    restarts), and failing terminally on a gateway that has since gone away would strand
    ///    the move — the worst outcome. Money safety is the UNCHANGED `fee_cap` re-checked at the
    ///    Pay step, not gateway identity: a substitute that costs more simply fails the cap;
    /// 3. otherwise price the registered routes at the action amount and choose the cheapest one
    ///    whose full fee fits the unchanged cap — but only when `amount_is_final`. A fresh
    ///    `Evacuate` is resolved by plain both-ends VALIDATION instead, because its `plan.amount`
    ///    is the un-downsized full ask that `size_fresh_evacuation` has not yet reduced: judging
    ///    routes against `fee_cap` at that amount would refuse the drain (`Retryable`, every
    ///    tick) BEFORE the downsizing search that exists to make it affordable ever ran, and
    ///    route economics must never gate an evacuation.
    ///
    /// The gateway actually used lands on the durable `MoveRecord` and from there on the ledger
    /// row's `OperationKind::Move.gateway`, so any substitution is auditable after the fact.
    async fn resolve_move_gateway(
        &self,
        plan: &MovePlan,
        preselected: Option<&GatewayUrl>,
        amount_is_final: bool,
    ) -> Result<GatewayUrl, ExecError> {
        if let Some(gateway) = &self.pinned_gateway {
            return Ok(gateway.clone());
        }
        if let Some(preselected) = preselected {
            if self
                .gateway_serves_route(&plan.to, plan.from.as_ref(), preselected)
                .await
            {
                return Ok(preselected.clone());
            }
            tracing::warn!(
                to = %plan.to.to_hex(),
                gateway = %preselected.0,
                "executor: the planned gateway no longer serves this move; re-resolving under the \
                 same fee cap"
            );
        }
        if !amount_is_final {
            return self.resolve_gateway(&plan.to, plan.from).await;
        }
        self.resolve_fallback_move_gateway(plan).await
    }

    /// Resolve the first registered gateway that VALIDATES (`to`, plus `from` when the move has a
    /// send leg) — the pre-route-economics behavior, kept for every shape whose amount cannot be
    /// priced: a receive-only inflow, a sizing probe, and a fresh evacuation (whose `plan.amount`
    /// is still the un-downsized full balance). Funding moves use
    /// [`Self::resolve_fallback_move_gateway`] first, because their action carries an amount and
    /// cap against which routes can be compared economically.
    ///
    /// "None validates" is `Retryable`, NOT `Permanent`: a resume verb (`reconcile`/`await-move`)
    /// carries no pinned gateway, so re-driving an intent that has none cached must leave it
    /// `Pending` (re-drivable once the operator supplies one), never terminally `Failed`.
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

    /// Re-resolve a missing/dead move hint at the move's actual amount. Only routes whose complete
    /// receive+send quote fits the action's unchanged cap enter the argmin; this prevents a
    /// reference-amount winner from failing pre-mint while another serving route would fit.
    ///
    /// Two shapes cannot be priced at all and fall back to plain both-ends VALIDATION
    /// ([`Self::resolve_gateway`], i.e. exactly what this path did before route economics):
    ///
    /// - a RECEIVE-ONLY inflow (`plan.from == None`, every `DirectInflow`) has no send leg to
    ///   price, and no source federation to price it from;
    /// - a scan where NO candidate could be quoted at `plan.amount` (every send dry-run hit
    ///   `InsufficientBalanceError`, reported as `Ok(None)` — see
    ///   `quote_fresh_send_required_cost`), which says the amount itself is unfundable rather
    ///   than that the routes are dear.
    ///
    /// A fresh `Evacuate` never reaches this function at all ([`Self::resolve_move_gateway`]
    /// routes it by validation): its `plan.amount` is the pre-sizing ask, so ANY verdict taken
    /// here — cap-refusal included — would pre-empt the `size_fresh_evacuation` search that makes
    /// the drain affordable, and strand a dying federation's funds.
    ///
    /// Only "some route priced, none fits the cap" keeps the cap-shaped `Retryable`: there the cap
    /// — not liveness — is the reason, and reporting it is what makes the refusal legible.
    async fn resolve_fallback_move_gateway(
        &self,
        plan: &MovePlan,
    ) -> Result<GatewayUrl, ExecError> {
        let Some(from) = plan.from else {
            return self.resolve_gateway(&plan.to, None).await;
        };
        let gateways = self.mc.gateways(&plan.to).await.map_err(retryable)?;
        let scan_deadline_ms =
            crate::runtime::now_ms().saturating_add(FALLBACK_MOVE_ROUTE_BUDGET_MS);
        let mut cheapest = None;
        let mut priced_any = false;
        let mut fully_scanned = true;
        for gateway in &gateways {
            if crate::runtime::now_ms() >= scan_deadline_ms {
                fully_scanned = false;
                tracing::debug!(
                    to = %plan.to.to_hex(),
                    registered = gateways.len(),
                    "executor: move fallback gateway scan hit its time budget; \
                     leaving final routing to plain validation"
                );
                break;
            }
            let gateway_fees = match self
                .fresh_send_required_gateway_fees(&from, &plan.to, gateway)
                .await
            {
                Ok(fees) => fees,
                Err(error) => {
                    tracing::debug!(
                        gateway = %gateway.0,
                        ?error,
                        "executor: move fallback gateway quote failed"
                    );
                    continue;
                }
            };
            let cost = match self
                .quote_fresh_send_required_cost(&from, &plan.to, plan.amount, gateway_fees)
                .await
            {
                Ok(CandidateQuote::Priced(cost)) => {
                    priced_any = true;
                    cost.total_fee()
                }
                Ok(CandidateQuote::Unquotable { .. }) => continue,
                Err(error) => {
                    tracing::debug!(
                        gateway = %gateway.0,
                        ?error,
                        "executor: move fallback federation quote failed"
                    );
                    continue;
                }
            };
            cheapest = keep_cheapest_fitting(cheapest, (cost, gateway), plan.fee_cap);
        }
        if let Some((_, gateway)) = cheapest {
            return Ok(gateway.clone());
        }
        // Only conclude "no gateway fits the cap" when the WHOLE registered set was examined. A scan
        // truncated by the time budget has an unexamined suffix that may contain a fitting route, so
        // it must not report a hard cap refusal — fall through to plain both-ends validation, which
        // can still pick a serving gateway from the suffix (the cap is re-enforced at perform).
        if priced_any && fully_scanned {
            return Err(ExecError::Retryable(format!(
                "no lnv2 gateway can route move {} -> {} within fee cap {} msat \
                 (scanned all {} registered gateway(s))",
                from.to_hex(),
                plan.to.to_hex(),
                plan.fee_cap.0,
                gateways.len(),
            )));
        }
        self.resolve_gateway(&plan.to, Some(from)).await
    }

    /// Whether `gateway` serves both required ends of this move (§15.6).
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
        ensure_minimum_incoming_contract("direct inflow", amount, grossed.contract_amount)
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
        ensure_minimum_incoming_contract("direct inflow", rec.amount, grossed.contract_amount)?;
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
    /// `resolve_gateway`): the refusal can come from a TRANSIENT shortfall — the source's funds are
    /// momentarily in flight (the send dry-run hits `InsufficientBalanceError`, treated as unfit),
    /// or a fee quote ticked up between attempts — and this runs BEFORE any side effect, on every
    /// pre-receive resume. Terminally `Failed`-ing here would abandon funds on a dying federation
    /// the wallet could have drained one tick later, defeating the whole point of a flee. Leaving
    /// the intent `Pending` lets the next tick retry once the shortfall clears; a source holding
    /// only sub-minimum dust simply keeps retrying harmlessly (nothing meaningful is stranded).
    /// A refusal that the analytic slopes prove STRUCTURAL still stays `Retryable`, but says so —
    /// a silent indefinite retry is not something an operator can act on.
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
            amount: desired,
            fee_cap_components,
            ..
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
        let to = rec.to;
        let gateway = rec.gateway.clone();
        // §15.2: an evacuation must not push its DESTINATION over the hard per-fed cap. Clamp the
        // desired net to the destination's remaining cap room BEFORE costing; a destination already
        // at/above the cap yields zero room, a LOUD terminal refusal (never a 0-msat move, never a
        // wrapped-around huge room). This runs only for a FRESH evacuation (`has_move_artifact`
        // returned early above), so a resumed, already-minted evacuation is never refused here.
        // The search is bounded ABOVE by this clamped ask: evacuations are exempt from
        // `enforce_destination_cap`, so nothing downstream would catch a larger size.
        let desired = self.clamp_desired_to_cap_room(rec, desired).await?;
        let spendable = self.mc.balance(&from).await.map_err(retryable)?;
        // The cap RULE this evacuation is enforced against — components when the allocator
        // snapshotted them, else the stored absolute cap as a constant (see
        // [`evacuation_cap_rule`]).
        let cap = evacuation_cap_rule(fee_cap_components, rec.fee_cap);
        // ONE gateway-fee snapshot for the WHOLE search — two HTTP fetches, one per leg — reused
        // by both sizing passes and every boundary probe. Re-fetching between passes would let
        // the slope move after a pass had already fixed its bound. Only the per-candidate quotes
        // below are repeated, and those are local: `fee_quote` opens a DB transaction and runs
        // the input/output builder against it with no federation request.
        let gateway_fees = self
            .fresh_send_required_gateway_fees(&from, &to, &gateway)
            .await?;
        if let Some(warning) = ppm_envelope_warning(gateway_fees) {
            tracing::warn!(
                from = %from.to_hex(),
                to = %to.to_hex(),
                gateway = %gateway.0,
                "executor: {warning}"
            );
        }
        let sizing = size_evacuation(desired, spendable, cap, |amount| {
            self.quote_fresh_send_required_cost(&from, &to, amount, gateway_fees)
        })
        .await?;
        let amount = match sizing {
            EvacuationSizing::Sized(amount) => amount,
            EvacuationSizing::Refused(reason) => {
                tracing::warn!(
                    from = %from.to_hex(),
                    to = %to.to_hex(),
                    requested_msat = desired.0,
                    spendable_msat = spendable.0,
                    cap_base_msat = cap.base_msat.0,
                    cap_bps = cap.bps,
                    "executor: no evacuable amount fits — {reason}"
                );
                return Err(ExecError::Retryable(format!(
                    "no evacuable amount fits: desired {} msat cannot reserve move fees within \
                     source balance {} msat under an evacuation fee cap of {} msat + {} bps \
                     (retrying — a later tick may succeed once in-flight funds settle or the fee \
                     quote eases); {reason}",
                    desired.0, spendable.0, cap.base_msat.0, cap.bps
                )));
            }
        };
        if amount < desired {
            tracing::warn!(
                from = %from.to_hex(),
                to = %to.to_hex(),
                requested_msat = desired.0,
                executable_msat = amount.0,
                spendable_msat = spendable.0,
                // The cap at the ASK is not what will be enforced — the quote's delivered net
                // is — but no quote is in hand here. Labelled as the ceiling it is.
                fee_cap_ceiling_msat = cap.at(amount).0,
                "executor: reducing fresh evacuation amount to reserve move fees"
            );
        }
        apply_evacuation_sizing(rec, cap, amount);
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

    /// Phase 5 §5.0.5 sizing seam: the largest net a probe's leg OUT (`from` = the
    /// candidate, `to` = the probing source) can redeem within `budget` — leg IN's
    /// DELIVERED net, NOT the candidate's live balance — and the per-leg `fee_cap`. This
    /// is the `size_fresh_evacuation` affordability search reused without the shutdown
    /// reason: sizing finds `out_net` using only the delivered delta as spendable budget.
    /// The runtime then drives the return leg with the remaining-delta fee cap
    /// (`delivered_in - out_net`) so the Pay-step re-quote cannot spend pre-existing
    /// candidate funds. Resolves the same gateway a fresh `from → to` move would (pinned
    /// gateway first, else the destination's registered set). `Ok(None)` = no out move
    /// whose CONTRACT clears the lnv2 floor is affordable from this budget (the caller's
    /// §5.0.5 step-4 local abort).
    ///
    /// A probe leg keeps the ABSOLUTE per-leg cap it is given — expressed here as the same
    /// cap shape with a ZERO rate, so `cap.at(n)` is constant — and does NOT take the
    /// evacuation viability post-check: a probe deliberately moves a tiny amount whose fee is a
    /// large fraction of it, which is the point of the probe, not a route that fails to serve.
    pub(crate) async fn size_probe_leg_out(
        &self,
        from: FederationId,
        to: FederationId,
        budget: Msat,
        fee_cap: Msat,
    ) -> Result<Option<Msat>, ExecError> {
        let gateway = self.resolve_gateway(&to, Some(from)).await?;
        let gateway_fees = self
            .fresh_send_required_gateway_fees(&from, &to, &gateway)
            .await?;
        let cap = EvacFeeCap {
            base_msat: fee_cap,
            bps: 0,
        };
        let search =
            search_evacuation_net(budget, budget, cap, oscillation_bound(budget), |amount| {
                self.quote_fresh_send_required_cost(&from, &to, amount, gateway_fees)
            })
            .await?;
        Ok(search.sized.map(|(net, _)| net))
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

    async fn quote_fresh_send_required_cost(
        &self,
        from: &FederationId,
        to: &FederationId,
        amount: Msat,
        gateway_fees: FreshSendRequiredGatewayFees,
    ) -> Result<CandidateQuote, ExecError> {
        if amount.0 == 0 {
            return Ok(CandidateQuote::Unquotable {
                source_shortfall: None,
            });
        }
        let grossed = self
            .quote_receive_gross_up_with_gateway_fee(to, amount, gateway_fees.receive)
            .await?;
        if grossed.contract_amount.0 < MINIMUM_INCOMING_CONTRACT_MSAT {
            // Too SMALL for lnv2, not too large for the source: no shortfall exists to measure,
            // and probing larger amounts on this candidate's account would be probing on a gap
            // that was never there.
            return Ok(CandidateQuote::Unquotable {
                source_shortfall: None,
            });
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
            // the downsizing search never runs. The mint reports the GAP as well as the refusal,
            // and the sizing search needs it: a note-selection boundary can make a larger amount
            // fundable again, so a small measured gap licenses probing above this candidate.
            Err(e) => {
                return match insufficient_balance_shortfall(&e) {
                    Some(shortfall) => Ok(CandidateQuote::Unquotable {
                        source_shortfall: Some(shortfall),
                    }),
                    None => Err(retryable(e)),
                }
            }
        };
        Ok(CandidateQuote::Priced(FreshMoveCost {
            invoice_amount: grossed.invoice_amount,
            receive_quote: grossed.receive_quote,
            send_quote: Msat(send_gateway_quote.0.saturating_add(send_tx_fee.0)),
        }))
    }
}

impl FedimintExecutor {
    async fn verify_raw_receive_fee_cap(
        &self,
        intent: &Intent,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        operation_id: OperationId,
        invoice: &Invoice,
    ) -> Result<(), ExecError> {
        let (committed_contract, _) = self
            .mc
            .receive_contract_amounts(&to, operation_id)
            .await
            .map_err(retryable)?;
        let federation_quote = self
            .mc
            .receive_fee_quote(&to, committed_contract)
            .await
            .map_err(retryable)?;
        let actual_quote = amount
            .0
            .saturating_sub(committed_contract.0)
            .saturating_add(federation_quote.0);
        if actual_quote > fee_cap.0 {
            self.journal
                .set_operation_artifact(&intent.idempotency_key, operation_id, Some(invoice))
                .await?;
            return Err(ExecError::Permanent(format!(
                "raw receive committed fee {actual_quote} msat exceeds fee cap {} msat",
                fee_cap.0
            )));
        }
        Ok(())
    }

    async fn enforce_pre_fund_admission(&self, intent: &Intent) -> Result<(), ExecError> {
        let Some((source, destination)) = pre_fund_endpoints(&intent.action) else {
            return Ok(());
        };
        if self
            .journal
            .get_move(&intent.idempotency_key)
            .await?
            .is_some_and(|record| {
                matches!(
                    record.phase,
                    MovePhase::Sending
                        | MovePhase::Settled
                        | MovePhase::Refunded
                        | MovePhase::Failed
                        | MovePhase::Stranded
                )
            })
        {
            return Ok(());
        }

        let mut in_flight = self
            .journal
            .reservation_intents()
            .await
            .map_err(pre_fund_reservation_error)?;
        in_flight.retain(|other| other.idempotency_key != intent.idempotency_key);
        let mut records = BTreeMap::new();
        for other in &in_flight {
            if let Some(record) = self
                .journal
                .get_move(&other.idempotency_key)
                .await
                .map_err(pre_fund_reservation_error)?
            {
                records.insert(other.idempotency_key.clone(), record);
            }
        }
        let reservations =
            wallet_core::project_reservations(&in_flight, |key| records.get(key).cloned());
        let mut balances = BTreeMap::new();
        if let Some(source) = source {
            balances.insert(source, self.mc.balance(&source).await.map_err(retryable)?);
        }
        if self.hard_cap.is_some() {
            if let Some(destination) = destination {
                balances.insert(
                    destination,
                    self.mc.balance(&destination).await.map_err(retryable)?,
                );
            }
        }
        wallet_core::admit_intent(intent, Some(&balances), self.hard_cap, &reservations)
    }

    /// Drive the network work for one journaled intent. Raw operations complete one SDK
    /// issue step; move-shaped intents resume through the existing per-leg loop.
    pub async fn drive_intent_step(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        match &intent.action {
            Action::Pay {
                from,
                invoice,
                amount,
                fee_cap,
                payment_hash,
                gateway,
                ..
            } => {
                if let Some(operation_id) = self
                    .mc
                    .find_send_op_by_payment_hash(*from, *payment_hash)
                    .await?
                {
                    let observation = self.mc.observe_op(*from, operation_id).await?;
                    if observation
                        .terminal
                        .as_ref()
                        .is_none_or(|terminal| terminal.succeeded)
                    {
                        self.journal
                            .set_operation_artifact(&intent.idempotency_key, operation_id, None)
                            .await?;
                        if observation.terminal.is_some() {
                            self.journal
                                .record_raw_observation(
                                    &intent.idempotency_key,
                                    operation_id,
                                    &observation,
                                )
                                .await?;
                        }
                        return Ok(if observation.terminal.is_some() {
                            PerformOutcome::Done
                        } else {
                            PerformOutcome::AwaitingAlreadyInFlight
                        });
                    }
                }
                validate_raw_pay_invoice(invoice)?;
                self.enforce_pre_fund_admission(intent).await?;
                // Same precedence as `resolve_gateway`: the intent's own gateway, else the
                // constructor pin (walletd.toml / standalone --gateway — devimint's LDK gateway
                // is never in the registered list), else the fed's registered scan. The pin is
                // deliberately NOT journaled into the intent, so a pin change applies to
                // re-drives after a restart.
                let candidates = match gateway.clone().or_else(|| self.pinned_gateway.clone()) {
                    Some(gateway) => vec![gateway],
                    None => self.mc.gateways(from).await.map_err(retryable)?,
                };
                // Scan ALL candidates and keep the CHEAPEST that fits the cap, not the first fitter.
                // `lowest_quote` already tracked the minimum for the over-cap diagnostic; it now also
                // decides selection so a user pay takes the cheapest route, matching the move path's
                // cheapest-serving-both-ends selection (the fee cap remains the money backstop).
                let mut cheapest_fitting: Option<(Msat, GatewayUrl)> = None;
                let mut lowest_quote = None;
                for candidate in candidates {
                    let gateway_fee =
                        match self.mc.send_gateway_fee(from, &candidate, invoice).await {
                            Ok(fee) => fee,
                            Err(_) => continue,
                        };
                    let gateway_quote = gateway_fee.on(*amount);
                    let contract = Msat(amount.0.saturating_add(gateway_quote.0));
                    let federation_quote =
                        match self.mc.send_fee_quote_for_amount(from, contract).await {
                            Ok(quote) => quote,
                            Err(_) => continue,
                        };
                    let total = gateway_quote.0.saturating_add(federation_quote.0);
                    lowest_quote =
                        Some(lowest_quote.map_or(total, |lowest: u64| lowest.min(total)));
                    cheapest_fitting =
                        keep_cheapest_fitting(cheapest_fitting, (Msat(total), candidate), *fee_cap);
                }
                let gateway = match cheapest_fitting {
                    Some((_, gateway)) => gateway,
                    None => {
                        return Err(raw_pay_quote_error(lowest_quote, *fee_cap, *from));
                    }
                };
                let meta = serde_json::json!({
                    "role": "send",
                    "correlation_key": intent.operation_correlation_key().0,
                });
                let outcome = self
                    .mc
                    .pay(from, invoice.clone(), Some(gateway), meta)
                    .await
                    .map_err(map_raw_pay_send_error)?;
                let (operation_id, already_in_flight) = match outcome {
                    SendOutcome::Started(operation_id) => (operation_id, false),
                    // In flight OR settled — the awaiter attaches and terminalizes from the
                    // op's own final state (a settled op's await resolves immediately).
                    SendOutcome::AlreadyInFlight(operation_id) => (operation_id, true),
                };
                self.journal
                    .set_operation_artifact(&intent.idempotency_key, operation_id, None)
                    .await?;
                return Ok(if already_in_flight {
                    PerformOutcome::AwaitingAlreadyInFlight
                } else {
                    PerformOutcome::Awaiting
                });
            }
            Action::Receive {
                to,
                amount,
                fee_cap,
                gateway,
                ..
            } => {
                if let Some(operation_id) = intent.operation_id {
                    let invoice = intent.invoice.as_ref().ok_or_else(|| {
                        ExecError::Permanent(
                            "raw receive operation artifact has no durable invoice".into(),
                        )
                    })?;
                    self.verify_raw_receive_fee_cap(
                        intent,
                        *to,
                        *amount,
                        *fee_cap,
                        operation_id,
                        invoice,
                    )
                    .await?;
                    return Ok(PerformOutcome::Awaiting);
                }
                if let Some((invoice, operation_id)) = self
                    .mc
                    .find_receive_artifact_by_correlation_key(
                        to,
                        &intent.operation_correlation_key(),
                    )
                    .await?
                {
                    self.verify_raw_receive_fee_cap(
                        intent,
                        *to,
                        *amount,
                        *fee_cap,
                        operation_id,
                        &invoice,
                    )
                    .await?;
                    self.journal
                        .set_operation_artifact(
                            &intent.idempotency_key,
                            operation_id,
                            Some(&invoice),
                        )
                        .await?;
                    return Ok(PerformOutcome::AwaitingAlreadyInFlight);
                }
                self.enforce_pre_fund_admission(intent).await?;
                // Pin fallback as in the raw pay arm above (and `resolve_gateway`).
                let candidates = match gateway.clone().or_else(|| self.pinned_gateway.clone()) {
                    Some(gateway) => vec![gateway],
                    None => self.mc.gateways(to).await.map_err(retryable)?,
                };
                let mut selected_gateway = None;
                let mut lowest_quote = None;
                let mut minimum_contract_error = None;
                let mut quote_unavailable = false;
                for candidate in candidates {
                    let gateway_fee = match self.mc.receive_gateway_fee(to, &candidate).await {
                        Ok(fee) => fee,
                        Err(_) => {
                            quote_unavailable = true;
                            continue;
                        }
                    };
                    let gateway_quote = gateway_fee.on(*amount);
                    let contract = Msat(amount.0.saturating_sub(gateway_quote.0));
                    if let Err(error) =
                        ensure_minimum_incoming_contract("raw receive", *amount, contract)
                    {
                        minimum_contract_error = Some(error);
                        continue;
                    }
                    let federation_quote = match self.mc.receive_fee_quote(to, contract).await {
                        Ok(quote) => quote,
                        Err(_) => {
                            quote_unavailable = true;
                            continue;
                        }
                    };
                    let total = gateway_quote.0.saturating_add(federation_quote.0);
                    lowest_quote =
                        Some(lowest_quote.map_or(total, |lowest: u64| lowest.min(total)));
                    // Keep the CHEAPEST fitting candidate, not the first (matching the raw pay arm).
                    selected_gateway =
                        keep_cheapest_fitting(selected_gateway, (Msat(total), candidate), *fee_cap);
                }
                let gateway = match selected_gateway {
                    Some((_, gateway)) => gateway,
                    None if lowest_quote.is_some() && !quote_unavailable => {
                        return Err(raw_fee_cap_error(
                            "raw receive",
                            lowest_quote.expect("checked above"),
                            *fee_cap,
                        ));
                    }
                    None if lowest_quote.is_some() => {
                        return Err(ExecError::Retryable(format!(
                            "raw receive fee quote {} msat exceeds fee cap {} msat",
                            lowest_quote.expect("checked above"),
                            fee_cap.0
                        )));
                    }
                    None if minimum_contract_error.is_some() && !quote_unavailable => {
                        return Err(minimum_contract_error.expect("checked above"));
                    }
                    None => {
                        return Err(ExecError::Retryable(format!(
                            "no lnv2 gateway produced a receive fee quote for federation {}",
                            to.to_hex()
                        )));
                    }
                };
                let meta = serde_json::json!({
                    "role": "receive",
                    "correlation_key": intent.operation_correlation_key().0,
                });
                let (invoice, operation_id) = self
                    .mc
                    .receive(to, *amount, Some(gateway), meta)
                    .await
                    .map_err(retryable)?;
                self.verify_raw_receive_fee_cap(
                    intent,
                    *to,
                    *amount,
                    *fee_cap,
                    operation_id,
                    &invoice,
                )
                .await?;
                self.journal
                    .set_operation_artifact(&intent.idempotency_key, operation_id, Some(&invoice))
                    .await?;
                return Ok(PerformOutcome::Awaiting);
            }
            Action::Join {
                federation,
                invite,
                membership_preexisting,
            } => {
                let invite = InviteCode::from_str(invite).map_err(|error| {
                    ExecError::Permanent(format!("parsing federation invite: {error}"))
                })?;
                let outcome = self.mc.join(invite.clone()).await.map_err(retryable)?;
                let registered_invite = if *membership_preexisting || outcome.newly_joined {
                    None
                } else {
                    self.journal
                        .get_federation(federation)
                        .await?
                        .map(|info| info.invite)
                };
                let newly_joined = recovered_join_was_new(
                    *membership_preexisting,
                    outcome.newly_joined,
                    &invite.to_string(),
                    registered_invite.as_deref(),
                );
                if intent.actor == Actor::User {
                    // The network join succeeded. Persist explicit user ownership before the
                    // terminal transition; a retry is idempotent, while AutoJoined remains owned
                    // by the agent until the separately-audited approve verb runs.
                    self.journal
                        .mark_candidate_user_approved(*federation, &invite)
                        .await?;
                }
                self.journal
                    .record_join_outcome(&intent.idempotency_key, newly_joined)
                    .await?;
                return Ok(PerformOutcome::Done);
            }
            Action::Recover { invite, .. } => {
                let invite = InviteCode::from_str(invite).map_err(|error| {
                    ExecError::Permanent(format!("parsing federation invite: {error}"))
                })?;
                // Recovery is complete-or-fail (D5): a failed module recovery, a transport fault, or
                // the refuse-if-registered guard terminalizes this intent `Failed` with the
                // SDK/refusal diagnostic (`Permanent`, not `Retryable`); a `Failed` intent is then
                // retried only by the deliberate Failed+User manual path. A crash MID-recovery
                // instead leaves the intent `Executing`, and reconcile DOES auto-re-drive it on the
                // next startup — which is money-safe under D3/D4: the fresh partition was never
                // registered (the crashed attempt never reached `complete_recovery`), so the re-drive
                // recovers into a clean FRESH prefix; or the fed IS registered, and the
                // refuse-if-registered guard makes the re-drive an honest, deterministic refusal.
                // Either way it terminalizes and never double-recovers or wedges Pending forever.
                self.mc
                    .recover(invite, &intent.idempotency_key)
                    .await
                    .map_err(|error| ExecError::Permanent(error.to_string()))?;
                return Ok(PerformOutcome::Done);
            }
            Action::DirectInflow { .. }
            | Action::Move { .. }
            | Action::Evacuate { .. }
            | Action::RefuseInflow { .. } => {}
        }

        self.enforce_pre_fund_admission(intent).await?;

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
                    // The DELIVERED net for this re-quote, and the cap it entitles. Derived HERE,
                    // before the gate below, because the gate must not admit a receive fee that the
                    // Pay step will then refuse: `rec.fee_cap` was computed at the SIZED ask, so
                    // using it here enforces a cap the delivery never earned and pushes the refusal
                    // past `mc.receive` — after the operation has committed and can only be left
                    // orphaned. Nothing mutates `rec.amount`: lowering the cached ask before the
                    // receive exists would make a crash retry mint a fresh invoice for less than
                    // requested (see the §15.11 note below).
                    let requote_delivered = grossed.delivered_net();
                    let cap_rule = evacuation_cap_rule(plan.fee_cap_components, rec.fee_cap);
                    // Cap-check the receive side alone (spec §6/§7): for a `DirectInflow` this is
                    // the whole check; for a `Move` the send leg is re-checked at `Pay`. Over cap →
                    // persist the quote first, so the refusal is in history either way.
                    //
                    // The DISPOSITION splits by shape, and only for an evacuation. Nothing has
                    // committed here — `mc.receive` is still below — so a fee that drifted up
                    // between sizing and this re-quote is a transient fact about one quote, not a
                    // property of the move. Terminalizing it strands a dying federation's balance
                    // until a fresh occurrence is emitted, when re-running `size_fresh_evacuation`
                    // would simply re-size against the new prices. That is the same reasoning the
                    // viability gate directly below already applies; having the two disagree was
                    // the file contradicting itself. For a `Move`/`DirectInflow` the ask is the
                    // user's own and `Permanent` remains right: there is no dying federation to
                    // drain and no reason to keep retrying a request that no longer prices.
                    if !fee::total_within_cap(
                        grossed.receive_quote,
                        Msat(0),
                        cap_rule.at(requote_delivered),
                    ) {
                        self.journal.put_move(&rec).await?;
                        let over_cap = format!(
                            "fee over cap (receive side {} msat exceeds the {} msat cap at the \
                             {} msat this would deliver)",
                            grossed.receive_quote.0,
                            cap_rule.at(requote_delivered).0,
                            requote_delivered.0
                        );
                        return Err(if is_evacuate {
                            ExecError::Retryable(over_cap)
                        } else {
                            ExecError::Permanent(over_cap)
                        });
                    }
                    // VIABILITY, checked here and not only at Pay. The Pay step terminalizes
                    // `Permanent` when the receive fee exceeds what the move delivers, and by then
                    // `mc.receive` has committed — so a gateway that raises its receive base
                    // between sizing and this re-quote gets admitted (its fee still fits the cap),
                    // mints, commits, and is then permanently refused, abandoning the drain with an
                    // orphaned receive op. That is the opposite of the posture `size_fresh_
                    // evacuation` takes: funds on a dying federation are left retryable, never
                    // terminally abandoned. `fits_cap` makes the same argument — the admission side
                    // is the one that has to move — and this is the admission side.
                    //
                    // Only the FIXED-invoice half moves forward. The send leg is re-quoted at Pay
                    // and its `Retryable` disposition rightly stays there.
                    if is_evacuate && grossed.receive_quote.0 > requote_delivered.0 {
                        self.journal.put_move(&rec).await?;
                        return Err(ExecError::Retryable(format!(
                            "the receive leg now costs more than this move delivers ({} msat of \
                             receive fee against {} msat delivered); refusing BEFORE minting so \
                             the balance stays where it is and a later quote can still drain it",
                            grossed.receive_quote.0, requote_delivered.0
                        )));
                    }
                    let invoice_amount = grossed.invoice_amount;
                    // A move may have accepted a verified hair-under solve: the DELIVERED net is
                    // invoice − receive_quote. The adjustment to `rec.amount` happens AFTER
                    // `mc.receive` commits (below), NOT here — lowering the cached amount before
                    // the receive exists would make a crash/transient-failure retry prefer the
                    // smaller cached amount over the intent's ask (`assemble_move_record`) and mint
                    // a fresh invoice for less than requested even though fees may have settled; a
                    // fresh attempt must re-quote from the intent's full ask.
                    let delivered = requote_delivered;
                    // The net this move will actually deliver (== rec.amount for an exact solve),
                    // committed in the receive op's own MoveMeta below UNCONDITIONALLY (§15.11): the
                    // MoveMeta amount is documented as the honest crash-safe delivered amount, so a
                    // receive-only `DirectInflow` that settles a hair under must record `delivered`,
                    // not the ask. A crash that loses the post-receive cache write then recovers the
                    // HONEST amount from the op itself (backfill prefers recovered op metadata over
                    // the intent's ask) — the Pay-step cap re-check can never be weakened by a stale
                    // higher amount.
                    let net_amount = delivered_move_amount(delivered, rec.amount);
                    // The cap RULE, so a hair-under settle re-derives its cap from what is
                    // actually delivered. `rec.fee_cap` was computed by `apply_evacuation_sizing`
                    // AT `rec.amount`; when `net_amount` comes in under that, keeping it would
                    // enforce a cap the executed net never entitled — the same planned-vs-executed
                    // hole `apply_evacuation_sizing` closes at the sizing seam, reachable through a
                    // second door. For a non-evacuation move (and a legacy intent carrying no
                    // components) the rule is the stored cap as a CONSTANT, so `at()` returns it
                    // unchanged at any net and this is a no-op.
                    let delivered_fee_cap = cap_rule.at(net_amount);
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
                        move_id: intent.operation_correlation_key(),
                        role: MoveRole::Receive,
                        amount: net_amount,
                        // Commit the ENFORCED cap beside the net it was computed at — which is
                        // `net_amount`, NOT `rec.amount`: a hair-under settle delivers less than
                        // the sized ask, and `rec.fee_cap` still holds the cap computed at that
                        // larger ask. Committing that here would pair a smaller `amount` with a
                        // larger cap in the one record replay trusts. This is the crash-safe half
                        // of the clamp fix: the killpoint below sits between this op committing and
                        // the `MoveRecord` write, and without the cap here replay would rebuild
                        // `fee_cap` from the intent's PLANNED amount and authorise a fee the
                        // executed net never entitled.
                        fee_cap: Some(delivered_fee_cap),
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
                            enforced_fee_cap_msat = delivered_fee_cap.0,
                            "executor: fee fixed point settled a hair under; adjusting move net"
                        );
                        // Through the sizing seam, so the amount and its cap move TOGETHER here
                        // exactly as they do at `size_fresh_evacuation`. Assigning `rec.amount`
                        // alone would leave `fee_cap` at the larger ask's value, and the Pay-step
                        // re-check below would then admit a fee this net never entitled.
                        apply_evacuation_sizing(&mut rec, cap_rule, net_amount);
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
                    // ADR-0029: a route that costs more than it delivers does not SERVE, and
                    // sizing's post-check is re-run here for the same reason the cap is — the
                    // send leg is re-quoted each attempt and can have moved since sizing. Only
                    // an evacuation takes it: it is the shape that chunk-drains on its own
                    // remainder, while a funding `Move`'s proportional cap already keeps its fee
                    // far under its amount and the user verbs deliberately allow a small receive
                    // to cost a large fraction of itself.
                    if is_evacuate {
                        evacuation_viability_verdict(receive_quote, send_quote, rec.amount)?;
                    }

                    let meta = MoveMeta {
                        move_id: intent.operation_correlation_key(),
                        role: MoveRole::Send,
                        amount: rec.amount,
                        // The same enforced cap as the receive leg's meta: whichever leg backfill
                        // recovers, reassembly gets the cap this move was authorised under.
                        fee_cap: Some(rec.fee_cap),
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
                        // Both are the SAME committed send (the client dedups on the
                        // deterministic op-id): reattach, never double-pay (spec §4).
                        SendOutcome::Started(op) | SendOutcome::AlreadyInFlight(op) => op,
                    };
                    // KILLPOINT (§5 backfill window): the send op is committed in the CLIENT db,
                    // but our MoveRecord does NOT yet carry `send_op`. A crash here must NOT
                    // double-pay: backfill recovers the send op by `move_id`; if that misses, a
                    // re-`pay` dedups to `AlreadyInFlight`.
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
                            // the receive, so a crash after this point can never lose the evidence
                            // that the send leg completed. It EVIDENCES the send; it is not a way
                            // to recover a stranded move (see the `Stranded` note at the top of
                            // `move_protocol`). THEN await the receive.
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
                            // credited, which re-driving cannot fix.
                            // `settle_after_successful_send` maps it to `Stranded` (loud,
                            // terminal); the `Stranded` note at the top of `move_protocol` says
                            // what that observation does and does not establish.
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
                // (§3). Surface the recorded outcome so the CLI/log reports the terminal state —
                // for a `Stranded` move it states what was observed without claiming a cause.
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

/// The route `decide()` preselected for a move-shaped action, if any. Deliberately read off the
/// `Action` rather than carried on [`MovePlan`]: the plan is the pure, gateway-FREE projection of
/// what to move, and the gateway is a routing hint the executor may override.
fn action_gateway(action: &Action) -> Option<&GatewayUrl> {
    match action {
        Action::Move { gateway, .. } | Action::Evacuate { gateway, .. } => gateway.as_ref(),
        _ => None,
    }
}

/// Whether the plan's `amount` is FINAL when the gateway is resolved — i.e. whether candidate
/// routes may be judged against `fee_cap` at that amount.
///
/// False for an `Evacuate` alone: `assemble_record` (and so gateway resolution) runs BEFORE
/// `size_fresh_evacuation`, which downsizes the drain until the quoted cost fits the absolute
/// `max_fee` cap. A cap verdict taken at the pre-sizing ask would refuse the evacuation
/// (`Retryable`, every tick) without the downsizing search ever running — stranding a dying
/// federation's balance, which is exactly what that search exists to prevent. Route economics
/// never gates an evacuation (`wallet-core`'s `evacuate_decision`), and this is where that
/// promise is kept on the perform side.
fn move_amount_is_final(action: &Action) -> bool {
    !matches!(action, Action::Evacuate { .. })
}

fn pre_fund_endpoints(action: &Action) -> Option<(Option<FederationId>, Option<FederationId>)> {
    match action {
        Action::Move { from, to, .. } => Some((Some(*from), Some(*to))),
        Action::DirectInflow { to, .. } | Action::Receive { to, .. } => Some((None, Some(*to))),
        Action::Pay { from, .. } => Some((Some(*from), None)),
        Action::Evacuate { .. }
        | Action::Join { .. }
        | Action::Recover { .. }
        | Action::RefuseInflow { .. } => None,
    }
}

fn recovered_join_was_new(
    membership_preexisting: bool,
    join_reported_new: bool,
    intent_invite: &str,
    registered_invite: Option<&str>,
) -> bool {
    !membership_preexisting
        && (join_reported_new || registered_invite.is_some_and(|stored| stored == intent_invite))
}

#[async_trait]
impl Executor for FedimintExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        self.drive_intent_step(intent).await
    }
}

/// The largest `amount` in `[floor, hi]` that PROBES as fitting, by a ROBUST bisection.
/// Requires `floor ≥ 1`.
///
/// CONTRACT — this replaces the former monotonicity requirement, which an amount-dependent fee
/// cap genuinely breaks. `cap(a) = base + bps*a` is not monotone against a real fee curve:
/// writing `fee(a) − cap(a) = (bases − base) + (ppm_total − bps_rate)*a`, the slope can go either
/// way, so the feasible set is a TOP window (bases above the cap base, rate below it) or a BOTTOM
/// one — and every term floors independently (both gateway fees, both federation fees, and the
/// per-note MINT fee), so near the crossing the verdict genuinely oscillates msat to msat.
///
/// So: **returns a probed-fitting amount at least as large as every amount the BOUNDED probe
/// reaches.** That is BEST-EFFORT, NOT "every amount that fits with `2A` msat of slack": `A`
/// bounds ONE vertical fee jump and says nothing about how many note-selection boundaries
/// separate a failing probe from a feasible window, nor their spacing, so a window beyond
/// [`NOTE_BOUNDARY_PROBES`] boundaries is MISSED and the evacuation keeps retrying. ADR-0029
/// accepts that residual: the consequence is a retry, not a burn. `None` only when no such amount
/// exists.
///
/// Where a plain bisection collapses `hi` the moment a candidate fails, this one first asks
/// whether the miss is within the oscillation bound `A` (`shortfall <= A`, EQUALITY INCLUDED —
/// the boundary belongs to the probe, not the refusal), and if so probes the note-selection
/// boundaries ABOVE the failing candidate before discarding everything above it. That is what
/// finds a TOP window: a discontinuity can make a LARGER amount fit again, and one false probe
/// below the window would otherwise throw the whole window away — the exact livelock the
/// amount-dependent cap would have reintroduced.
///
/// Structural windows are found by this search (or by its caller's second pass). What it does NOT
/// chase is the OSCILLATION near the crossing, where floor jitter flips the verdict msat to msat:
/// there every fitting amount fits with ~zero slack, and a zero-slack fit buys no margin against
/// the Pay-step re-quote. It is not a weakening of the search — it names the residue the search is
/// not obliged to chase. Zero-slack amounts remain perfectly USABLE: `fee::total_within_cap`
/// compares with `<=`, so an exact-cap candidate is admitted and survives an unchanged re-quote.
async fn largest_fitting_amount<F, Fut>(
    floor: u64,
    mut hi: u64,
    oscillation: u64,
    mut probe: F,
) -> Result<Option<u64>, ExecError>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<ProbeVerdict, ExecError>>,
{
    debug_assert!(floor > 0, "a zero floor would underflow the sentinel below");
    if hi < floor {
        return Ok(None);
    }
    // `lo` trails the largest amount VERIFIED to fit; it starts one below the floor as the
    // "nothing verified yet" sentinel and only ever advances to probed-true amounts, so the
    // loop never evaluates `probe` outside `[floor, hi]`.
    let mut lo = floor - 1;
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        let verdict = probe(mid).await?;
        if verdict.fits {
            lo = mid;
            continue;
        }
        if verdict.shortfall <= oscillation {
            // Every candidate here is > `mid`, so `lo` still advances and the range still
            // shrinks: the loop terminates exactly as the plain bisection does.
            if let Some(found) =
                first_fitting_boundary(note_boundaries_above(mid, hi), &mut probe).await?
            {
                lo = found;
                continue;
            }
        }
        hi = mid - 1;
    }
    Ok((lo >= floor).then_some(lo))
}

/// Probe `candidates` in order and return the FIRST that fits, or `None` when none does. The
/// order is the caller's: nearest boundary first, so the bisection resumes from the closest
/// verified fit rather than jumping past intermediate ones.
async fn first_fitting_boundary<F, Fut>(
    candidates: Vec<u64>,
    probe: &mut F,
) -> Result<Option<u64>, ExecError>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<ProbeVerdict, ExecError>>,
{
    for candidate in candidates {
        if probe(candidate).await?.fits {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

/// The note-selection boundaries adjacent to `amount` and ABOVE it, at most `ceiling`, at most
/// [`NOTE_BOUNDARY_PROBES`] of them, and ordered LARGEST FIRST.
///
/// The pinned mint's denominations are POWERS OF TWO msat (`Tiered::gen_denominations` with
/// `DEFAULT_DENOMINATION_BASE = 2` in fedimint-mint-server), and the mint charges per input and
/// per output — so which notes a transaction selects, and hence its fee, can only change where
/// the amount crosses a multiple of a tier. These are those crossings, DERIVED from the
/// denominations rather than guessed offsets: `A` bounds the VERTICAL fee jump and says nothing
/// about the HORIZONTAL distance to the next discontinuity, so an offset-guessing probe can obey
/// "a bounded number of candidates" and still miss every executable amount.
///
/// LARGEST FIRST for two reasons that agree: the caller wants the largest amount that fits, and
/// the crossing of a LARGER tier is the more significant discontinuity — the mint's per-note fee
/// scales with the denomination crossed, so that is where a jump big enough to have caused the
/// failure lives. Probing the nearest 1-, 2- and 4-msat crossings instead would spend the whole
/// budget within a few hundred msat of a candidate that already failed.
///
/// One honest limitation: the tiers are read off the NET, while the fee is charged on the invoice
/// and the outgoing contract, which sit a gateway base and ppm away. The offset is near-constant
/// across a search, so a large-tier crossing in the net still lands close to the corresponding one
/// in the contract, but this is a bounded best-effort probe — as its contract says — and not an
/// exact enumeration of the fee curve's discontinuities.
fn note_boundaries_above(amount: u64, ceiling: u64) -> Vec<u64> {
    let mut out: Vec<u64> = Vec::new();
    for k in (0..u64::BITS).rev() {
        // The next multiple of this tier STRICTLY above `amount`. NON-INCREASING as `k` falls
        // (every multiple of `2^k` is one of `2^(k-1)`), so the list comes out sorted descending
        // and equal neighbours dedup in one step. A tier whose crossing is out of range is
        // skipped rather than terminating the scan: the smaller tiers below it are still in range.
        let candidate = ((amount >> k) as u128 + 1) << k;
        if candidate > u128::from(ceiling) {
            continue;
        }
        let candidate = candidate as u64;
        if out.last() != Some(&candidate) {
            out.push(candidate);
        }
        if out.len() == NOTE_BOUNDARY_PROBES {
            break;
        }
    }
    out
}

/// The mirror of [`note_boundaries_above`]: the last amount before each tier crossing that
/// `amount` sits above, at least `floor`, and — by the same "largest candidate first" rule —
/// NEAREST first, which below the amount IS largest first.
///
/// Nearest-first is also right on its own terms here: the caller reaches this list because the
/// top just failed, and the crossing that raised its fee is the one immediately below it. The
/// list still widens geometrically (each tier's crossing is at least twice as far), so a top
/// sitting well above the crossing that broke it is still reached.
fn note_boundaries_below(amount: u64, floor: u64) -> Vec<u64> {
    let mut out: Vec<u64> = Vec::new();
    for k in 0..u64::BITS {
        let step = 1u128 << k;
        let boundary = (u128::from(amount) / step) * step;
        // Non-increasing in `k`, so the list is sorted descending and `break` is safe.
        let Some(candidate) = boundary.checked_sub(1) else {
            break;
        };
        if candidate < u128::from(floor) {
            break;
        }
        let candidate = candidate as u64;
        if out.last() != Some(&candidate) {
            out.push(candidate);
        }
        if out.len() == NOTE_BOUNDARY_PROBES {
            break;
        }
    }
    out
}

/// The OSCILLATION BOUND `A`: how far ONE note-selection discontinuity can move the quoted cost.
/// It decides only whether a failing probe is close enough to be worth probing the adjacent tier
/// boundaries before a range is discarded — it is NEVER a safety margin on the cap itself, which
/// keeps comparing exactly (`fee::total_within_cap`).
///
/// `A ≈ 6 + 2*(300*tiers + 100) + 2_100 + 2*ceil(v_max * mint_ppm / 1e6)`: the two legs'
/// consolidation bound over the mint's tier count, the fixed lnv2/mint terms, and — the term that
/// must not be dropped — the mint's PROPORTIONAL per-note fee, which scales with the DENOMINATION
/// crossed. A tier-count-only bound can be smaller than a real jump by orders of magnitude, and
/// the `shortfall <= A` rules would then refuse an executable evacuation without probing a nearby
/// viable amount, which is the failure they exist to prevent.
///
/// `v_max` bounds the value of one selected input or change note. Neither it nor the live mint
/// rate is readable through this seam, so both are taken at their pinned worst case: the source's
/// own spendable balance bounds any note it can hold, and [`MINT_FEE_PPM_CEILING`] is the highest
/// rate `FeeConsensus::new` will accept. Both err LARGE deliberately — an over-estimate only
/// probes more, an under-estimate refuses unprobed.
fn oscillation_bound(v_max: Msat) -> u64 {
    let tiers = u128::from(u64::BITS - v_max.0.max(1).leading_zeros());
    let per_note_proportional =
        (u128::from(v_max.0) * u128::from(MINT_FEE_PPM_CEILING)).div_ceil(1_000_000);
    let bound = 6u128
        + 2 * (300 * tiers + u128::from(MINT_FEE_BASE_MSAT))
        + 2_100
        + 2 * per_note_proportional;
    bound.min(u128::from(u64::MAX)) as u64
}

/// The cap RULE a fresh evacuation is sized and enforced under: the components the allocator
/// snapshotted onto the action, or — for a LEGACY intent planned before they existed — the
/// stored absolute cap expressed as the same shape with a ZERO rate, which makes `cap.at(n)`
/// constant at that stored value.
///
/// A legacy evacuation therefore keeps exactly the bound it was admitted under. It must NOT adopt
/// the current policy's components: an in-flight evacuation's cap is an admitted parameter, and
/// silently moving it is the same hazard as re-deriving every cap from a live `Policy` at
/// execution time (which is why that design was rejected — an operator's `policy set` could
/// shrink an in-flight cap, and `pay_step_cap_verdict` is PERMANENT when the fixed receive quote
/// alone exceeds it, so a routine edit could terminally kill an evacuation whose invoice is
/// already minted).
fn evacuation_cap_rule(components: Option<EvacFeeCap>, planned_cap: Msat) -> EvacFeeCap {
    components.unwrap_or(EvacFeeCap {
        base_msat: planned_cap,
        bps: 0,
    })
}

/// Commit a completed sizing onto the record: the executed net AND the cap recomputed at it.
///
/// The two are written TOGETHER, and ONLY through this function, so a downsized drain can never
/// keep the planned-amount cap — the money hole this is the seam for. Plan 75_000 sats and clamp to
/// 1_000, and the enforced cap moves from 2_450 sats to 230; leaving `fee_cap` alone would authorise
/// a fee more than ten times what the executed net entitles.
///
/// There are TWO callers, because the net drops in two places: `size_fresh_evacuation` (the search
/// picks a smaller net than desired) and the receive fixed point (a verified hair-under settle
/// delivers less than the sized ask). The second is the easier one to miss — it looks like a
/// cosmetic amount correction — so route any future amount change through here rather than
/// assigning `rec.amount` directly.
fn apply_evacuation_sizing(rec: &mut MoveRecord, cap: EvacFeeCap, net: Msat) {
    rec.amount = net;
    rec.fee_cap = cap.at(net);
}

/// Whether a candidate's quoted cost fits the cap at the net that quote actually DELIVERS.
///
/// It takes no caller-supplied amount on purpose. Admitting at `cap.at(sized_ask)` while the
/// executor enforces `cap.at(delivered_net)` lets a quote in the band `cap.at(delivered)+1 ..=
/// cap.at(sized)` pass sizing and then be refused at the Pay step — after the receive operation
/// has already committed, leaving it orphaned and unclaimed. With stable quotes that repeats,
/// so the admission side is the one that has to move.
fn fits_cap(cost: FreshMoveCost, cap: EvacFeeCap) -> bool {
    fee::total_within_cap(
        cost.receive_quote,
        cost.send_quote,
        cap.at(cost.delivered_net()),
    )
}

/// The COMBINED pass-2 predicate: affordability, and the cap at what the quote DELIVERS. The
/// verdict's shortfall is the LARGER of the two gaps, so a candidate that misses both is only
/// probed around when BOTH misses are within one discontinuity.
///
/// It takes no candidate amount at all, which is the point rather than an accident. Both halves
/// read the QUOTE: affordability compares `invoice + send_quote` against the source's spendable
/// balance, and the cap compares the fee against `invoice − receive_quote`. The candidate the
/// search was probing is an intention; once a quote exists, every question worth asking is about
/// the quote. Removing the parameter is what stops a caller reintroducing the ask as a cap basis.
fn combined_verdict(quote: CandidateQuote, cap: EvacFeeCap, spendable: Msat) -> ProbeVerdict {
    let affordability = quote.affordability(spendable);
    let cap_gap = match quote {
        CandidateQuote::Priced(cost) => cost
            .total_fee()
            .0
            .saturating_sub(cap.at(cost.delivered_net()).0),
        // An unquotable candidate has no cost to compare against the cap; affordability alone
        // decides it.
        CandidateQuote::Unquotable { .. } => 0,
    };
    if affordability.fits && cap_gap == 0 {
        return ProbeVerdict::fits();
    }
    ProbeVerdict::missed_by(affordability.shortfall.max(cap_gap))
}

/// The complete sizing decision for one fresh evacuation.
#[derive(Clone, Debug, PartialEq, Eq)]
enum EvacuationSizing {
    /// The net to execute. The caller recomputes the enforced cap at it
    /// ([`apply_evacuation_sizing`]).
    Sized(Msat),
    /// Nothing executable was found. The reason names a STRUCTURAL refusal when the analytic
    /// slopes say no amount can fit, and says so honestly when the bounded probe merely came up
    /// empty — probe exhaustion is INCONCLUSIVE, not proof.
    Refused(String),
}

/// The result of the two-pass sizing search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EvacuationSearch {
    /// The largest probed net that fits BOTH affordability and the cap, with its quoted cost.
    sized: Option<(Msat, FreshMoveCost)>,
    /// The largest probed net the SOURCE can afford, cap ignored — pass 1's own result. The
    /// structural-refusal diagnostic needs it: its first condition is about the cap failing AT
    /// the largest affordable amount.
    largest_affordable: Option<(Msat, FreshMoveCost)>,
}

/// TWO PASSES, because a single bisection on the combined predicate is not sufficient (§2c(a)).
///
/// PASS 1 bisects the AFFORDABILITY constraint ALONE over `[MINIMUM_INCOMING_CONTRACT_MSAT, T]`
/// and then checks the cap at the result. PASS 2 runs only when the cap fails there, and bisects
/// the COMBINED predicate over `[MINIMUM_INCOMING_CONTRACT_MSAT, pass1]` — in the regime that
/// reaches it, `fee − cap` is increasing in the amount, so the combined predicate really is
/// fits-then-doesn't and the bottom window is found. Nothing is refused until BOTH fail.
///
/// A single combined bisection is what the worked case defeats: with gateway bases 99 + 49 sats
/// against a configured 20-sat cap base, a combined ppm under the cap rate and a 10_000-sat
/// balance, the full ask fails on AFFORDABILITY, a combined bisection then probes ~5_002 sats,
/// fails on the CAP, collapses `hi` to 5_001 and discards the feasible window [~5_120, ~9_802]
/// entirely — returning to the very livelock the base+proportional cap exists to kill. The window
/// is missed STRUCTURALLY, not for want of slack (at 9_000 sats the fee is ~193 against a ~290
/// cap, ~97 sats of room). Pass 1's affordability-only bisection finds it regardless.
///
/// Affordability is NOT strictly monotone either: mint fees are per input and per output, so the
/// note COUNT enters the source debit, and raising the ask can cross a note-selection boundary
/// that REDUCES the input/change count and so LOWERS the debit. Pass 1 therefore carries the same
/// robustness treatment as pass 2 — see [`largest_fitting_amount`], whose boundary probes are the
/// mirror of pass 2's, because here a LARGER amount can fit.
///
/// Both passes are bounded above by `desired`, which the caller has already clamped to the
/// destination's remaining cap room: evacuations are exempt from `enforce_destination_cap`, so an
/// unbounded search could size one clean over the ADR-0018 hard cap with nothing downstream to
/// catch it.
async fn search_evacuation_net<F, Fut>(
    desired: Msat,
    spendable: Msat,
    cap: EvacFeeCap,
    oscillation: u64,
    mut quote: F,
) -> Result<EvacuationSearch, ExecError>
where
    F: FnMut(Msat) -> Fut,
    Fut: std::future::Future<Output = Result<CandidateQuote, ExecError>>,
{
    let floor = MINIMUM_INCOMING_CONTRACT_MSAT;
    let mut search = EvacuationSearch {
        sized: None,
        largest_affordable: None,
    };
    // FAST PATH: the full ask, which is what an evacuation wants whenever the source can fund it.
    // It runs BEFORE the floor check on purpose. `floor` bounds the BISECTION RANGE, not
    // executability: the incoming contract is `net + fed_fee`, so a net under the 5_000 msat
    // protocol minimum can still mint a contract above it once the federation fee is added.
    // Returning early without ever quoting strands exactly the dust remnant an evacuation exists
    // to sweep — it would retry forever reporting "no fit" for an amount nothing ever priced.
    let top = quote(desired).await?;
    let top_affordable = top.affordability(spendable).fits;
    if let CandidateQuote::Priced(cost) = top {
        if top_affordable {
            search.largest_affordable = Some((desired, cost));
            if fits_cap(cost, cap) {
                search.sized = Some((desired, cost));
                return Ok(search);
            }
        }
    }

    // Below the floor there is no bisection range left: the fast path above was the only
    // candidate this route has, and it did not serve.
    if desired.0 < floor {
        return Ok(search);
    }

    // PASS 1 — affordability alone. When the full ask was already affordable it IS pass 1's
    // answer (nothing larger is in range), so only the cap can have refused it.
    let pass1 = if top_affordable {
        Some(desired.0)
    } else {
        largest_fitting_amount(floor, desired.0, oscillation, |amount| {
            let quoted = quote(Msat(amount));
            async move { Ok(quoted.await?.affordability(spendable)) }
        })
        .await?
    };
    let Some(pass1) = pass1 else {
        return Ok(search);
    };
    let CandidateQuote::Priced(pass1_cost) = quote(Msat(pass1)).await? else {
        // A deterministic quote stream cannot un-price an amount it just priced; a
        // non-deterministic one simply gets no sizing this pass and retries.
        return Ok(search);
    };
    // `largest_affordable` is ASSIGNED from this fresh re-quote — set when it is affordable and
    // CLEARED when it is not. Recording conditionally is not enough: when the fast path was
    // affordable-but-over-cap it already stored a sample at this same amount (:2195), and a
    // conditional write would leave that stale sample standing while the fresh quote at the same
    // amount says the source can no longer fund it. `no_fitting_amount_reason` then measures a
    // cap trend on a superseded cost and can tell an operator to raise a money knob for what is
    // really a transient price or balance movement. The freshest evidence about an amount wins,
    // including when it is negative.
    //
    // When `pass1` came from the bisection instead, nothing was recorded at :2195 (that write is
    // gated on `top_affordable`), so clearing is a no-op and the assignment is still correct.
    let pass1_verdict = combined_verdict(CandidateQuote::Priced(pass1_cost), cap, spendable);
    search.largest_affordable =
        (pass1_cost.source_debit() <= spendable).then_some((Msat(pass1), pass1_cost));
    // BOTH constraints on the fresh quote, through the one predicate — not the cap alone. This
    // re-quote is not the one the bisection accepted, and the hole is live precisely when it
    // moved: in the `top_affordable` branch the fast path's quote already failed the cap, so
    // reaching here REQUIRES the price to have changed. `Priced` does not imply affordable —
    // `source_debit` carries the send tx fee on top of what the mint dry-run funded — so a
    // cap-only check can size an amount the source cannot fund, mint, commit the receive, and
    // strand it when `Pay` cannot pay. Round 6 guarded this seam at pass 2 and left pass 1; the
    // shared predicate is what stops there being a third.
    if pass1_verdict.fits {
        search.sized = Some((Msat(pass1), pass1_cost));
        return Ok(search);
    }

    // PASS 2 — the cap failed at the largest affordable amount, so any feasible set is a BOTTOM
    // window. In the INCREASING regime this finds nothing, harmlessly: pass 1 already ruled it
    // out from above.
    let pass2 = largest_fitting_amount(floor, pass1, oscillation, |amount| {
        let quoted = quote(Msat(amount));
        async move { Ok(combined_verdict(quoted.await?, cap, spendable)) }
    })
    .await?;
    if let Some(pass2) = pass2 {
        // REVALIDATE. This is a fresh quote, not the one `combined_verdict` accepted inside the
        // bisection: another intent can change the source's note inventory mid-search, so the
        // re-quote can stay `Priced` while carrying a larger mint fee. Trusting the verdict here
        // would let an over-cap evacuation reach invoice creation and be refused only at Pay —
        // after the receive committed. Same seam, one function further along.
        let requote = quote(Msat(pass2)).await?;
        if let CandidateQuote::Priced(cost) = requote {
            if combined_verdict(requote, cap, spendable).fits {
                search.sized = Some((Msat(pass2), cost));
            }
        }
    }
    Ok(search)
}

/// The ECONOMIC-VIABILITY POST-CHECK (ADR-0029): a route only SERVES when the chunk it can carry
/// delivers at least what it costs, `total_fee <= net`.
///
/// Without it the cap's amount-independent base is a burn licence: at the 5-sat lnv2 contract
/// floor a 200-sat base cap admits a chunk that burns ~200 sats to move ~5, and the remainder
/// re-emits every watch cycle with no minimum-progress guard, no attempt budget and no fee
/// accounting — a 75_000-sat balance drains in ~365 such chunks, delivering ~1_953 sats and
/// burning ~73_047, a ~97.4% loss. That is the evacuation destroying the balance it exists to
/// rescue.
///
/// It is a POST-check on the search RESULT, never a term inside the fits predicate: `fee(n) <= n`
/// is false at small `n` and true above `base/(1 − rate)`, so folding it in would re-break the
/// fits-then-doesn't shape the bisection depends on.
///
/// The TOP is checked FIRST, because for the affine model efficiency `fee(n)/n = base/n + rate`
/// falls as `n` grows — but the top is not a proof. The real quote is not affine: two gateway
/// fees, two federation fees and the per-note mint fee each floor independently and the note
/// COUNT can change between adjacent amounts, so the fee can jump by more than the one-msat gain
/// in `n`, and the top can fail while a slightly smaller candidate passes. The adjacent tier
/// boundaries BELOW the top are therefore probed before any refusal — ALWAYS, whatever the
/// shortfall's magnitude. ADR-0029 permits a refusal only when the shortfall exceeds the
/// oscillation bound `A` **and that is an analytically proven structural refusal**; a bare
/// shortfall over `A` is inconclusive, because `A` bounds ONE vertical fee jump and two nearby
/// note-count drops can each stay under it while together exceeding it. Gating the probe on the
/// magnitude stranded exactly the executable evacuations this function exists to find, and did so
/// on every tick, because stable quotes reproduce the branch.
///
/// A refusal here is INCONCLUSIVE about the route in general: `A` bounds ONE discontinuity, so
/// several boundaries can cumulatively exceed it with a smaller viable amount still beyond them,
/// and the probe is deliberately bounded. The caller stays `Retryable` and must not mark the route
/// unavailable — stranding rather than burning, the posture ADR-0018 already accepts.
async fn evacuation_viability<F, Fut>(
    sized: (Msat, FreshMoveCost),
    spendable: Msat,
    cap: EvacFeeCap,
    oscillation: u64,
    mut quote: F,
) -> Result<EvacuationSizing, ExecError>
where
    F: FnMut(Msat) -> Fut,
    Fut: std::future::Future<Output = Result<CandidateQuote, ExecError>>,
{
    let (net, cost) = sized;
    // Viability compares the fee against what the route DELIVERS, not against the ask — the
    // Pay-step mirror of this check already reads the delivered net (`rec.amount` is the
    // hair-under-adjusted value by then), so measuring the ask here would let a route pass
    // sizing and fail execution over the same inequality.
    let shortfall = cost.total_fee().0.saturating_sub(cost.delivered_net().0);
    if shortfall == 0 {
        return Ok(EvacuationSizing::Sized(net));
    }
    // The bounded probe runs REGARDLESS of how far the top missed. ADR-0029 permits a refusal
    // only when the shortfall exceeds `A` **AND that is an analytically proven structural
    // refusal**: "a bare shortfall over `A` is inconclusive, not proof". `A` bounds ONE vertical
    // fee jump, so two nearby note-count drops can each be bounded by `A` while their cumulative
    // reduction exceeds it, leaving a smaller candidate that satisfies both the cap and
    // `fee <= net`. Returning early on the bare magnitude skipped those candidates and — because
    // stable quotes take the same branch every tick — stranded a dying federation that had an
    // executable evacuation available. That is the livelock this bead exists to remove, so the
    // magnitude may colour the refusal MESSAGE but must not gate the probe.
    for candidate in note_boundaries_below(net.0, MINIMUM_INCOMING_CONTRACT_MSAT) {
        let CandidateQuote::Priced(cost) = quote(Msat(candidate)).await? else {
            continue;
        };
        let candidate = Msat(candidate);
        // Cap at what this candidate DELIVERS, matching `fits_cap` and the executor. The
        // viability half (`total_fee <= delivered`) moves with it: a route "serves" when it
        // delivers at least what it costs (CONTEXT.md, "Serves"), and the thing it delivers is
        // the delivered net, not the amount we asked for.
        let delivered = cost.delivered_net();
        if evacuation_cost_fits(cost, cap.at(delivered), spendable)
            && cost.total_fee().0 <= delivered.0
        {
            return Ok(EvacuationSizing::Sized(candidate));
        }
    }
    // The magnitude is reported so an operator can tell a near-miss from a route that is far from
    // serving, but it does NOT change the disposition: both cases stay `Retryable` and neither is
    // proof, per ADR-0029.
    let relation = if shortfall > oscillation {
        format!(
            "by more than one note-selection discontinuity ({shortfall} msat over {oscillation})"
        )
    } else {
        format!("by at most one note-selection discontinuity ({shortfall} msat)")
    };
    Ok(EvacuationSizing::Refused(format!(
        "the largest chunk this route can carry costs more than it delivers ({} msat of fees \
         against {} msat delivered, missing {relation}); the route does not serve, so the balance \
         is left where it is rather than burned. The adjacent note-selection boundaries were \
         probed and none served — a BOUNDED probe, so this is not proof that no viable amount \
         exists",
        cost.total_fee().0,
        cost.delivered_net().0
    )))
}

/// Size a fresh evacuation end to end: the two-pass search, then the economic-viability
/// post-check, then the structural-refusal diagnostic when nothing survives.
async fn size_evacuation<F, Fut>(
    desired: Msat,
    spendable: Msat,
    cap: EvacFeeCap,
    mut quote: F,
) -> Result<EvacuationSizing, ExecError>
where
    F: FnMut(Msat) -> Fut,
    Fut: std::future::Future<Output = Result<CandidateQuote, ExecError>>,
{
    // `v_max` — the largest single note that can be selected or returned as change — is bounded
    // by the source's own balance.
    let oscillation = oscillation_bound(spendable);
    let search = search_evacuation_net(desired, spendable, cap, oscillation, &mut quote).await?;
    let Some(sized) = search.sized else {
        return Ok(EvacuationSizing::Refused(
            no_fitting_amount_reason(cap, &search, &mut quote).await?,
        ));
    };
    evacuation_viability(sized, spendable, cap, oscillation, &mut quote).await
}

/// Why the search found nothing, and — when the measured slopes prove it — that the refusal is
/// STRUCTURAL rather than incidental. A silent indefinite retry is the failure this whole change
/// exists to kill; a refusal an operator can act on is not.
async fn no_fitting_amount_reason<F, Fut>(
    cap: EvacFeeCap,
    search: &EvacuationSearch,
    mut quote: F,
) -> Result<String, ExecError>
where
    F: FnMut(Msat) -> Fut,
    Fut: std::future::Future<Output = Result<CandidateQuote, ExecError>>,
{
    let Some((affordable_net, affordable_cost)) = search.largest_affordable else {
        return Ok(
            // NOT "no probed amount was affordable" — an earlier probe may well have been, and
            // then the final re-quote moved above budget, which is exactly the case that leaves
            // no affordable sample recorded. Saying otherwise sends an operator looking for a
            // balance problem when the balance was fine and the price moved.
            "no affordable sample remained at the end of the search (the source may have funds \
             in flight, or a quote moved mid-search; a later tick can succeed)"
                .to_string(),
        );
    };
    let floor = Msat(MINIMUM_INCOMING_CONTRACT_MSAT);
    let CandidateQuote::Priced(floor_cost) = quote(floor).await? else {
        return Ok(format!(
            "the cap refused every probed amount up to the largest affordable {} msat",
            affordable_net.0
        ));
    };
    // Both sample points are DELIVERED nets, not asks. The cap is `base + bps*D/10_000`, so its
    // slope is exactly `bps/10_000` in D — and only in D. Sampling the asks and calling the rise
    // `bps * span` overstates the cap's trend by `bps * Δδ` (δ = ask − delivered), which can flip
    // condition (i) and assert a PERMANENT structural cause — "raise evac_fee_base_msat" — for an
    // incidental miss. That is the re-derivation this needed, not a substitution of one number.
    match structural_refusal_cause(
        cap,
        (floor_cost.delivered_net(), floor_cost.total_fee()),
        (affordable_cost.delivered_net(), affordable_cost.total_fee()),
    ) {
        Some(cause) => Ok(format!(
            "structural refusal — {cause} (largest affordable ask {} msat delivers {} msat and \
             quotes {} msat of fees against a {} msat cap)",
            affordable_net.0,
            affordable_cost.delivered_net().0,
            affordable_cost.total_fee().0,
            cap.at(affordable_cost.delivered_net()).0
        )),
        None => Ok(format!(
            "the cap refused every probed amount up to the largest affordable {} msat, without a \
             structural cause — the refusal may clear on a later quote",
            affordable_net.0
        )),
    }
}

/// The structural half of the refusal diagnostic: whether the measured fee trend proves that
/// nothing can fit, and which cause to report. `None` when neither condition holds — an incidental
/// refusal a later quote may clear.
///
/// BOTH conditions are needed. `low`/`high` are `(DELIVERED NET, total_fee)` at two real quotes —
/// delivered, not the asks that produced them, and that is load-bearing rather than cosmetic. The
/// cap is `base + bps*D/10_000`, so `s_c = bps / 10_000` holds exactly against `D` and against
/// nothing else. Sampling asks makes the measured cap rise overstate the real one by
/// `bps * Δδ` (δ = ask − delivered), which can flip condition (i) and report a PERMANENT
/// structural cause — telling an operator to raise a money knob — for an incidental miss:
///
/// (i)  `s_c >= s_f` and the largest AFFORDABLE amount fails the cap (the caller's only route
///      here). Feasibility rises with amount, so nothing SMALLER can help.
/// (ii) `s_f >= s_c` and the COMPLETE minimum fixed intercept exceeds the cap's BASE — not the
///      gateway bases alone: the intercept also carries the fixed lnv2 federation fees and the
///      per-note mint component, so gateway bases UNDER the cap base can still sum with those to
///      an intercept above it, at which point `fee(a) − cap(a) > 0` at every amount and the gap
///      only widens. Wording it as "the gateways' bases alone" would report a false cause.
///
/// Condition (i) alone cannot fire in the livelock case — a low configured cap base with a zero
/// rate, where the fixed component alone sinks every amount — which is why an earlier revision
/// carrying only (i) left that case silent.
///
/// `s_f` and the intercept are MEASURED from the two quotes rather than reassembled from the two
/// gateway ppms and the four federation ppms: the composed slope is exactly what these points
/// bracket, and the federation and per-note mint components are not separately readable here — so
/// measuring is both the honest and the more complete way to get the very intercept (ii) is
/// stated in terms of. Condition (ii) additionally requires the floor quote to have FAILED the cap
/// as measured, so the cause can never be reported off an extrapolation alone.
fn structural_refusal_cause(
    cap: EvacFeeCap,
    low: (Msat, Msat),
    high: (Msat, Msat),
) -> Option<String> {
    let (low_amount, low_fee) = (i128::from(low.0 .0), i128::from(low.1 .0));
    let (high_amount, high_fee) = (i128::from(high.0 .0), i128::from(high.1 .0));
    let span = high_amount - low_amount;
    if span <= 0 {
        // One point cannot establish a trend.
        return None;
    }
    let rise = high_fee - low_fee;
    let cap_rise = i128::from(cap.bps) * span;
    let fee_rise = rise * 10_000;

    let mut causes = Vec::new();
    if cap_rise >= fee_rise {
        // NOTE the asymmetry with condition (ii) below, which IS a proof: if the fixed intercept
        // alone exceeds the cap base then `fee(a) - cap(a) > 0` at every amount, with no sampling
        // involved. Condition (i) is weaker and its wording now says so.
        //
        // TWO SAMPLES CANNOT PROVE THIS, and the wording no longer pretends otherwise. The fee
        // curve is explicitly non-monotone — per-note mint fees DROP at note-selection
        // boundaries, which is the whole reason the bounded probe exists — so both endpoints can
        // miss the cap while an unprobed middle window fits. ADR-0029 is explicit that probe
        // exhaustion alone stays inconclusive and must not mark the route unavailable. Reported
        // as a measured trend the operator can act on, not as proof that no amount can ever fit;
        // a complete boundary proof or a global bound is what would license the stronger claim,
        // and neither is computed here.
        causes.push(
            "the cap's trend across the two amounts probed rises no faster than the fee's, so \
             nothing SMALLER is likely to fit either; raise evac_fee_base_msat or evac_fee_bps. \
             NOTE this is a two-point measurement on a fee curve that is NOT monotone — evidence, \
             not proof, since an unprobed amount between them may still serve"
                .to_string(),
        );
    }
    // The fixed component of the measured line: `fee(a) − s_f*a` at the low point.
    let intercept = low_fee - rise * low_amount / span;
    if fee_rise >= cap_rise && intercept > i128::from(cap.base_msat.0) && low.1 > cap.at(low.0) {
        causes.push(format!(
            "the fixed component alone — both gateways' bases plus the fixed federation and \
             per-note mint fees, ~{intercept} msat — exceeds the cap base {} msat, so no amount \
             can fit at any size",
            cap.base_msat.0
        ));
    }
    (!causes.is_empty()).then(|| causes.join("; "))
}

/// §2d: WARN when a gateway advertises a fee RATE outside the envelope the SDK's own limits were
/// meant to express, and never reject on it.
///
/// Returned rather than logged so the check is exercised directly; the caller logs it. Rejecting
/// would contradict ADR-0029: our cap is the only real bound at this pin, it already admits or
/// refuses a route on price, and a second, stricter admissibility test can refuse an evacuation
/// over the ONLY live route — zero bases with a 15_001-ppm send leg is a route the 200-sat + 3%
/// cap happily admits, and a ppm rejection would strand those funds. The warning is how an
/// operator learns why a route prices the way it does; honest defaults are nowhere near it (the
/// gateway default is 2 sats + 3_000 ppm).
fn ppm_envelope_warning(fees: FreshSendRequiredGatewayFees) -> Option<String> {
    let mut out = Vec::new();
    if fees.send.ppm > SEND_PPM_ENVELOPE {
        out.push(format!(
            "send {} ppm (intended envelope {SEND_PPM_ENVELOPE})",
            fees.send.ppm
        ));
    }
    if fees.receive.ppm > RECEIVE_PPM_ENVELOPE {
        out.push(format!(
            "receive {} ppm (intended envelope {RECEIVE_PPM_ENVELOPE})",
            fees.receive.ppm
        ));
    }
    (!out.is_empty()).then(|| {
        format!(
            "gateway advertises an out-of-envelope fee rate: {} — the SDK's fee limits do not \
             bound the ppm at this pin (PaymentFee compares base first), so only our fee cap \
             does; the route is NOT refused on this",
            out.join(", ")
        )
    })
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

fn ensure_minimum_incoming_contract(
    operation: &str,
    amount: Msat,
    contract_amount: Msat,
) -> Result<(), ExecError> {
    if contract_amount.0 < MINIMUM_INCOMING_CONTRACT_MSAT {
        return Err(ExecError::Permanent(format!(
            "{operation} amount too small: net {} msat produces a {} msat incoming contract; \
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

/// How far the source fell SHORT of funding the probed outgoing contract, when the failure was
/// the mint's `InsufficientBalanceError` — the send-side fee-quote dry-run's way of saying the
/// source cannot fund this candidate at all (verified against the pinned source: the mint's
/// funding selection propagates it `?`-converted, so it sits in the `anyhow` chain un-wrapped).
/// `Some` is the evacuation sizing search's "this candidate does not fit"; `None` is any other
/// error, which stays a transport fault and therefore `Retryable`.
///
/// The GAP, not just the fact, because the error carries both the requested and the available
/// figure: it is what lets the search tell a candidate that missed by one note-selection boundary
/// from one that missed by half the balance. Only the former is worth probing ABOVE, since a
/// LARGER amount can cross a boundary that reduces the input/change count and become fundable
/// again. The classifier walks the whole `anyhow` chain so an added `.context(..)` wrap cannot
/// silently break it.
fn insufficient_balance_shortfall(e: &anyhow::Error) -> Option<u64> {
    e.chain()
        .find_map(|cause| cause.downcast_ref::<fedimint_mint_client::InsufficientBalanceError>())
        .map(|insufficient| {
            insufficient
                .requested_amount
                .msats
                .saturating_sub(insufficient.total_amount.msats)
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

/// The Pay-step re-check of the ECONOMIC-VIABILITY rule (ADR-0029): an evacuation chunk must
/// deliver at least what it costs. Classified exactly as [`pay_step_cap_verdict`] classifies its
/// own check, deliberately inventing no third verdict class:
///   - `Permanent` when the FIXED receive quote alone already exceeds the net (the invoice is
///     minted, so no send re-quote can rescue it);
///   - `Retryable` when the total does (a later attempt may re-quote the send leg lower).
fn evacuation_viability_verdict(
    receive_quote: Msat,
    send_quote: Msat,
    net: Msat,
) -> Result<(), ExecError> {
    if receive_quote.0 > net.0 {
        return Err(ExecError::Permanent(format!(
            "route does not serve: the fixed receive quote {} msat alone exceeds the {} msat this \
             evacuation would deliver",
            receive_quote.0, net.0
        )));
    }
    if receive_quote.0.saturating_add(send_quote.0) > net.0 {
        return Err(ExecError::Retryable(format!(
            "route does not serve this attempt (receive {} + send {} > the {} msat delivered); \
             retrying rather than burning more than the chunk moves",
            receive_quote.0, send_quote.0, net.0
        )));
    }
    Ok(())
}

/// The §3 stranded-move outcome message: A's send SETTLED but B's receive was not credited. Names
/// the receive-side `detail` and then states the honest uncertainty — not proven lost, not proven
/// recoverable — so history/UI presents a debited-not-credited move without claiming either a loss
/// or a recovery. It deliberately does NOT point at the saved preimage: that preimage claims A's
/// OUTGOING contract and cannot credit B (see the `Stranded` note at the top of `move_protocol`).
/// The operator procedure is the runbook's.
///
/// The leading substring "send settled but receive was not credited" is an ANCHOR: the daily
/// stranded check in `docs/real-sats-pilot-runbook.md` greps `wallet-cli show` output for it, so a
/// reword here silently disables that check. `stranded_outcome_keeps_the_runbook_anchor` pins it.
fn stranded_outcome(detail: &str) -> String {
    format!(
        "send settled but receive was not credited: {detail}; \
         not proven lost, not proven recoverable — preserve the data directory and follow the \
         stranded-move entry in docs/real-sats-pilot-runbook.md"
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
        SendError::InvoiceRejected(msg) | SendError::RouteRejected(msg) => {
            ExecError::Permanent(msg)
        }
        SendError::Transport(err) => ExecError::Retryable(err.to_string()),
    }
}

fn map_raw_pay_send_error(e: SendError) -> ExecError {
    match e {
        SendError::InvoiceRejected(msg) => ExecError::Permanent(msg),
        // Deterministic per route: a retry rebuilds the same ordered candidate list and
        // re-selects the same gateway, so Retryable here reconcile-loops forever on the
        // rejected route. One-shot user semantics: terminalize; a deliberate retry is a
        // new operation (and may see a changed gateway set).
        SendError::RouteRejected(msg) => ExecError::Permanent(msg),
        // Ambiguous — the send MAY have been issued. Retryable is a RESUME, not a blind
        // retry: the deterministic send op id + lnv2 dedup reattach instead of re-paying.
        SendError::Transport(err) => ExecError::Retryable(err.to_string()),
    }
}

fn validate_raw_pay_invoice(invoice: &Invoice) -> Result<(), ExecError> {
    let parsed = Bolt11Invoice::from_str(&invoice.0)
        .map_err(|error| ExecError::Permanent(format!("parsing raw pay invoice: {error}")))?;
    if parsed.is_expired() {
        return Err(ExecError::Permanent("raw pay invoice has expired".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fedimint_bip39::Mnemonic;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::{IDatabaseTransactionOpsCore as _, IRawDatabaseExt as _};
    use wallet_core::{Action, Actor, IdempotencyKey, IntentStatus, ReasonCode};

    const FED_A: FederationId = FederationId([0xAA; 32]);
    const FED_B: FederationId = FederationId([0xBB; 32]);

    #[test]
    fn raw_receive_is_rechecked_as_a_destination_before_minting() {
        let action = Action::Receive {
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            nonce: "retry".into(),
            gateway: None,
        };
        assert_eq!(pre_fund_endpoints(&action), Some((None, Some(FED_B))));
    }

    #[test]
    fn evacuation_reaches_its_perform_time_downsizer_without_full_amount_admission() {
        let action = Action::Evacuate {
            from: FED_A,
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            gateway: None,
            fee_cap_components: None,
        };
        assert_eq!(pre_fund_endpoints(&action), None);
    }

    #[test]
    fn expired_raw_pay_is_terminal_before_gateway_fee_quoting() {
        let invoice = Invoice(
            "lnbc25m1pvjluezpp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqdq5vdhkven9v5sxyetpdeessp5zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygs9q5sqqqqqqqqqqqqqqqqsgq2a25dxl5hrntdtn6zvydt7d66hyzsyhqs4wdynavys42xgl6sgx9c4g7me86a27t07mdtfry458rtjr0v92cnmswpsjscgt2vcse3sgpz3uapa".into(),
        );
        assert!(matches!(
            validate_raw_pay_invoice(&invoice),
            Err(ExecError::Permanent(reason)) if reason.contains("expired")
        ));
    }

    /// A constructible executor over an in-memory db — enough to exercise the `perform` gate,
    /// which decides `Move`/`Evacuate` BEFORE any federation I/O (no join needed).
    async fn test_executor() -> FedimintExecutor {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        FedimintExecutor::new(mc, journal, None, None)
    }

    fn intent(action: Action) -> Intent {
        let max_fee = action.fee_cap();
        Intent {
            idempotency_key: IdempotencyKey("gate-test".into()),
            attempt: 0,
            action,
            max_fee,
            status: IntentStatus::Pending,
            reason: ReasonCode::UserInitiated,
            actor: Actor::User,
            created_at_ms: 0,
            operation_id: None,
            invoice: None,
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
            gateway: None,
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
            gateway: None,
            fee_cap_components: None,
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

    #[test]
    fn keep_cheapest_fitting_selects_the_cheapest_within_cap() {
        // Over-cap candidates are skipped — the fee cap stays the money backstop for pay/receive.
        assert_eq!(
            keep_cheapest_fitting(None, (Msat(1_001), "a"), Msat(1_000)),
            None
        );
        // The boundary (cost == cap) fits.
        assert_eq!(
            keep_cheapest_fitting(None, (Msat(1_000), "a"), Msat(1_000)),
            Some((Msat(1_000), "a"))
        );
        // A cheaper fitting candidate replaces a dearer incumbent...
        assert_eq!(
            keep_cheapest_fitting(Some((Msat(900), "a")), (Msat(700), "b"), Msat(1_000)),
            Some((Msat(700), "b"))
        );
        // ...but an equal or dearer one does not, so the first cheapest is stable.
        assert_eq!(
            keep_cheapest_fitting(Some((Msat(700), "b")), (Msat(700), "c"), Msat(1_000)),
            Some((Msat(700), "b"))
        );
        assert_eq!(
            keep_cheapest_fitting(Some((Msat(700), "b")), (Msat(900), "d"), Msat(1_000)),
            Some((Msat(700), "b"))
        );
    }

    #[test]
    fn join_recovery_preserves_the_membership_creator() {
        assert!(recovered_join_was_new(false, true, "invite-a", None));
        assert!(recovered_join_was_new(
            false,
            false,
            "invite-a",
            Some("invite-a")
        ));
        assert!(!recovered_join_was_new(
            false,
            false,
            "invite-a",
            Some("invite-b")
        ));
        assert!(!recovered_join_was_new(
            true,
            false,
            "invite-a",
            Some("invite-a")
        ));
    }

    /// The downsizing search must not assume its predicate is monotone below the lnv2
    /// minimum-contract floor. Regression for the skipped-window bug: with desired 500_000
    /// and only ~5_500 msat affordable, an unclamped bisection from 0 halves straight from
    /// the over-budget region into the below-minimum region (250_000 → … → 3_906, all
    /// unfit) and abandons the evacuation; the clamped search stays in `[5_000, desired]`,
    /// where fits-then-doesn't holds, and finds the window.
    #[tokio::test]
    async fn downsizing_search_finds_a_feasible_window_above_the_contract_floor() {
        let found = largest_fitting_amount(
            MINIMUM_INCOMING_CONTRACT_MSAT,
            499_999,
            0,
            |amount| async move { Ok(verdict(amount <= 5_500)) },
        )
        .await
        .expect("probes never fail");
        assert_eq!(found, Some(5_500));
    }

    /// A plain fits/doesn't probe with NO measurable miss: the robustness rule must not fire on
    /// it (an immeasurable shortfall never licenses a boundary probe), so these cases pin the
    /// bisection's unchanged behaviour.
    fn verdict(fits: bool) -> ProbeVerdict {
        if fits {
            ProbeVerdict::fits()
        } else {
            ProbeVerdict::missed_immeasurably()
        }
    }

    #[tokio::test]
    async fn downsizing_search_edge_cases() {
        // Nothing in range fits → None (the genuinely-infeasible evacuation).
        let none = largest_fitting_amount(5_000, 100_000, 0, |_| async { Ok(verdict(false)) })
            .await
            .expect("probes never fail");
        assert_eq!(none, None);

        // Everything fits → the top of the range.
        let all = largest_fitting_amount(5_000, 100_000, 0, |_| async { Ok(verdict(true)) })
            .await
            .expect("probes never fail");
        assert_eq!(all, Some(100_000));

        // An empty range (desired below the floor) is None without probing.
        let empty = largest_fitting_amount(5_000, 4_999, 0, |_| async {
            panic!("an empty range must not be probed")
        })
        .await
        .expect("probes never run");
        assert_eq!(empty, None);

        // Exactly the floor fitting is found, one msat under the floor is out of scope.
        let at_floor = largest_fitting_amount(5_000, 100_000, 0, |amount| async move {
            Ok(verdict(amount <= 5_000))
        })
        .await
        .expect("probes never fail");
        assert_eq!(at_floor, Some(5_000));
    }

    /// Regression for the aborted full-balance evacuation: the send-side dry-run quote fails
    /// with the mint's `InsufficientBalanceError` when a probed candidate cannot be funded, and
    /// the sizing search must classify that as "does not fit" (keep probing smaller amounts) —
    /// never as a `Retryable` transport fault that aborts the search. The classifier walks the
    /// whole anyhow chain so an added `.context(...)` wrap cannot silently break it, and it
    /// recovers the GAP as well, which is what licenses probing a larger amount across a
    /// note-selection boundary.
    #[test]
    fn insufficient_balance_is_classified_as_unfit_not_transport_failure() {
        let root = fedimint_mint_client::InsufficientBalanceError {
            requested_amount: fedimint_core::Amount::from_msats(100_000),
            total_amount: fedimint_core::Amount::from_msats(60_000),
        };
        let plain = anyhow::Error::from(root.clone());
        assert!(insufficient_balance_shortfall(&plain).is_some());
        assert_eq!(insufficient_balance_shortfall(&plain), Some(40_000));

        let wrapped = anyhow::Error::from(root).context("quoting send fee for evacuation probe");
        assert!(insufficient_balance_shortfall(&wrapped).is_some());
        assert_eq!(insufficient_balance_shortfall(&wrapped), Some(40_000));

        assert_eq!(
            insufficient_balance_shortfall(&anyhow::anyhow!("connection reset by peer")),
            None,
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
    fn fallback_selection_uses_actual_cost_and_keeps_the_same_cap() {
        let cap = Msat(1_000);
        let high_base = GatewayUrl("https://high-base.example".into());
        let low_base = GatewayUrl("https://low-base.example".into());
        let over_cap = GatewayUrl("https://over-cap.example".into());

        let mut best = None;
        best = keep_cheapest_fitting(best, (Msat(900), &high_base), cap);
        best = keep_cheapest_fitting(best, (Msat(300), &low_base), cap);
        best = keep_cheapest_fitting(best, (Msat(1_001), &over_cap), cap);

        assert_eq!(best, Some((Msat(300), &low_base)));
        assert_eq!(cap, Msat(1_000), "fallback must not widen the action's cap");
    }

    #[test]
    fn deterministic_send_rejection_fails_the_move_permanently() {
        // §15.4. A deterministic rejection from the send leg (expired / wrong-currency /
        // unsupported / fee-limit) maps to a terminal Permanent with an actionable message — the
        // move does NOT reset to Pending and livelock. A transport fault stays Retryable.
        let rejected = map_send_error(SendError::InvoiceRejected(
            "lnv2 send deterministically rejected the invoice: Invoice has expired".into(),
        ));
        assert!(matches!(rejected, ExecError::Permanent(msg) if msg.contains("expired")));

        let rejected = map_send_error(SendError::RouteRejected(
            "lnv2 send rejected the selected gateway route: fee limit".into(),
        ));
        assert!(matches!(rejected, ExecError::Permanent(msg) if msg.contains("fee limit")));

        let transport = map_send_error(SendError::Transport(anyhow::anyhow!(
            "connection reset by peer"
        )));
        assert!(matches!(transport, ExecError::Retryable(_)));
    }

    #[test]
    fn raw_pay_route_rejection_is_terminal_before_funding() {
        let rejected = map_raw_pay_send_error(SendError::RouteRejected(
            "lnv2 send rejected the selected gateway route: fee limit".into(),
        ));
        assert!(matches!(rejected, ExecError::Permanent(msg) if msg.contains("fee limit")));
    }

    #[test]
    fn raw_receive_fee_over_cap_is_terminal_before_funding() {
        assert!(matches!(
            raw_fee_cap_error("raw receive", 300_000, Msat(200_000)),
            ExecError::Permanent(message)
                if message == "raw receive fee quote 300000 msat exceeds fee cap 200000 msat"
        ));
    }

    #[test]
    fn raw_pay_fee_over_cap_is_terminal_before_funding() {
        assert!(matches!(
            raw_pay_quote_error(Some(300_000), Msat(200_000), FED_A),
            ExecError::Permanent(message)
                if message == "raw pay fee quote 300000 msat exceeds fee cap 200000 msat"
        ));
    }

    #[test]
    fn raw_pay_missing_gateway_quote_is_terminal_before_funding() {
        assert!(matches!(
            raw_pay_quote_error(None, Msat(200_000), FED_A),
            ExecError::Permanent(message)
                if message.contains("no lnv2 gateway produced a send fee quote")
        ));
    }

    #[test]
    fn the_preselected_route_is_read_off_the_move_shaped_actions_only() {
        let planned = GatewayUrl("https://planned.example".into());
        assert_eq!(
            action_gateway(&Action::Move {
                from: FED_A,
                to: FED_B,
                amount: Msat(50_000),
                fee_cap: Msat(1_000),
                gateway: Some(planned.clone()),
            }),
            Some(&planned)
        );
        assert_eq!(
            action_gateway(&Action::Evacuate {
                from: FED_A,
                to: FED_B,
                amount: Msat(50_000),
                fee_cap: Msat(1_000),
                gateway: Some(planned.clone()),
                fee_cap_components: None,
            }),
            Some(&planned)
        );
        // A legacy row (or an unpriced pair) simply carries none, which is the pre-route-economics
        // behavior: resolve at perform time.
        assert_eq!(
            action_gateway(&Action::Move {
                from: FED_A,
                to: FED_B,
                amount: Msat(50_000),
                fee_cap: Msat(1_000),
                gateway: None,
            }),
            None
        );
    }

    #[tokio::test]
    async fn a_pin_beats_the_preselected_route_and_a_dead_hint_re_resolves() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db.clone()));
        let plan = MovePlan {
            from: Some(FED_A),
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            send_required: true,
            fee_cap_components: None,
        };
        let planned = GatewayUrl("https://planned.example".into());

        // §Q4: an operator pin is the HIGHEST precedence — it wins over `decide()`'s preselection
        // without validating it, exactly as it already wins over the registered-set scan.
        let pin = GatewayUrl("https://pinned.example".into());
        let pinned = FedimintExecutor::new(mc.clone(), journal.clone(), Some(pin.clone()), None);
        assert_eq!(
            pinned
                .resolve_move_gateway(&plan, Some(&planned), true)
                .await
                .expect("a pin never needs to resolve"),
            pin
        );

        // §Q2: with no pin, a preselected gateway that no longer serves the route (no federation
        // is open here, so nothing validates) must NOT strand the move terminally — it falls
        // through to re-resolution and, when that finds nothing either, stays RETRYABLE so a
        // later tick can complete it. The move's `fee_cap` is untouched throughout: the cap, not
        // gateway identity, is what bounds what a substitute may spend.
        let unpinned = FedimintExecutor::new(mc, journal, None, None);
        let error = unpinned
            .resolve_move_gateway(&plan, Some(&planned), true)
            .await
            .expect_err("no gateway can serve an unopened federation");
        assert!(
            matches!(error, ExecError::Retryable(_)),
            "a dead preselected gateway must leave the move re-drivable, not failed: {error:?}"
        );
        assert_eq!(plan.fee_cap, Msat(1_000));
    }

    #[test]
    fn a_fresh_evacuation_is_routed_before_its_amount_is_final() {
        // The evacuation ordering trap: `assemble_record` resolves the gateway, and only THEN
        // does `size_fresh_evacuation` downsize the drain to what fits the absolute `max_fee`.
        // A dying fed holding 10_000_000 msat whose destination has 8_000_000 of cap room emits
        // `Evacuate { amount: 8_000_000, fee_cap: max_fee }` — an amount the source CAN afford,
        // so the send dry-run prices it fine and the quote simply lands over the cap. Judging
        // routes on that cap here would fail the evacuation `Retryable` every tick and the
        // downsizing bisection would never run, stranding the balance.
        assert!(!move_amount_is_final(&Action::Evacuate {
            from: FED_A,
            to: FED_B,
            amount: Msat(8_000_000),
            fee_cap: Msat(1_000),
            gateway: None,
            fee_cap_components: None,
        }));
        // Every other move shape carries the amount it will actually move, so its routes ARE
        // comparable against the cap.
        assert!(move_amount_is_final(&Action::Move {
            from: FED_A,
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
            gateway: None,
        }));
        assert!(move_amount_is_final(&Action::DirectInflow {
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
        }));
    }

    #[tokio::test]
    async fn a_receive_only_inflow_resolves_without_a_source_federation() {
        // A `DirectInflow` maps to `from: None` (`MovePlan::from_action`), so with no pinned
        // gateway and no preselected route it reaches the fallback resolver — which has no send
        // leg to price. It must VALIDATE the destination alone (the pre-route-economics
        // behavior). Treating the missing source as a Permanent error there terminally fails
        // EVERY direct inflow on a daemon that pins no gateway (the pin is optional host config).
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db));
        let executor = FedimintExecutor::new(mc, journal, None, None);

        let plan = MovePlan::from_action(&Action::DirectInflow {
            to: FED_B,
            amount: Msat(50_000),
            fee_cap: Msat(1_000),
        })
        .expect("DirectInflow maps to a plan");
        assert_eq!(plan.from, None, "a direct inflow is receive-only");

        let error = executor
            .resolve_move_gateway(&plan, None, true)
            .await
            .expect_err("no federation is open in this fixture");
        assert!(
            matches!(error, ExecError::Retryable(_)),
            "a receive-only inflow must stay re-drivable, not fail terminally: {error:?}"
        );
    }

    #[tokio::test]
    async fn poison_reservation_row_does_not_terminalize_a_healthy_move() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mc = Arc::new(MultiClient::new(db, journal_db.clone(), mnemonic).await);
        let journal = Arc::new(FedimintJournal::new(journal_db.clone()));
        let executor = FedimintExecutor::new(mc, journal, None, None);

        // The poison row targets the JOURNAL's partition — on its own db after the split.
        let app_db = journal_db.with_prefix(vec![0x00]);
        let mut dbtx = app_db.begin_transaction().await;
        let mut poison_index_key = vec![0x04, 0x00];
        poison_index_key.extend_from_slice(b"missing-intent");
        dbtx.raw_insert_bytes(&poison_index_key, &[])
            .await
            .expect("insert dangling reservation index");
        dbtx.commit_tx_result()
            .await
            .expect("commit dangling reservation index");

        let error = executor
            .perform(&intent(Action::Move {
                from: FED_A,
                to: FED_B,
                amount: Msat(50_000),
                fee_cap: Msat(10_000),
                gateway: None,
            }))
            .await
            .expect_err("fail closed before any network I/O");
        assert!(
            matches!(error, ExecError::Retryable(_)),
            "an unrelated poison row must leave the healthy move re-drivable: {error:?}"
        );
    }

    #[test]
    fn raw_receive_fee_over_cap_is_terminal_when_all_quotes_are_known() {
        assert!(matches!(
            raw_fee_cap_error("raw receive", 300_000, Msat(200_000)),
            ExecError::Permanent(message)
                if message == "raw receive fee quote 300000 msat exceeds fee cap 200000 msat"
        ));
    }

    #[test]
    fn minimum_incoming_contract_guard_matches_pinned_lnv2_boundary() {
        assert_eq!(MINIMUM_INCOMING_CONTRACT_MSAT, 5_000);
        ensure_minimum_incoming_contract("direct inflow", Msat(4_000), Msat(5_000))
            .expect("lnv2 accepts exactly the minimum incoming contract");

        let err = ensure_minimum_incoming_contract("raw receive", Msat(3_999), Msat(4_999))
            .expect_err("contract below lnv2's minimum is terminal");
        assert!(matches!(
            err,
            ExecError::Permanent(msg)
                if msg.starts_with("raw receive amount too small:")
        ));
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
            fee_cap_components: None,
        };
        let artifacts = vec![OpArtifact {
            move_id: key.clone(),
            leg: Leg::Receive,
            op_id: crate::types::OperationId([0x42; 32]),
            amount: Msat(50_000),
            invoice: Some(Invoice("lnbc1recover".into())),
            fee_cap: None,
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
            fee_cap_components: None,
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
        // An expired receive after a settled send STRANDS, stating the honest uncertainty.
        let (phase, outcome) = settle_after_successful_send(ReceiveState::Expired);
        assert_eq!(phase, MovePhase::Stranded);
        let msg = outcome.expect("a stranded move carries an outcome");
        assert!(
            msg.contains("not proven lost, not proven recoverable"),
            "{msg}"
        );
        assert!(msg.contains("receive invoice expired"), "{msg}");
        // A failed receive strands too, carrying the failure detail. The detail is the REAL string
        // `map_receive_state` mints — a fabricated one would let the test pass on a message the
        // wallet cannot actually produce.
        let (phase, outcome) = settle_after_successful_send(ReceiveState::Failed(
            crate::multi_client::RECEIVE_FAILURE_DETAIL.into(),
        ));
        assert_eq!(phase, MovePhase::Stranded);
        let msg = outcome.expect("a stranded move carries an outcome");
        assert!(
            msg.contains("not proven lost, not proven recoverable")
                && msg.contains(crate::multi_client::RECEIVE_FAILURE_DETAIL),
            "{msg}"
        );
    }

    #[test]
    fn failure_details_keep_the_runbook_ambiguous_prefixes() {
        // The daily check's AMBIGUOUS branch greps `wallet-cli show` output for these two
        // prefixes (docs/real-sats-pilot-runbook.md, check 1). They are grep couplings exactly
        // like the stranded anchor below, so pin them for the same reason: rewording either
        // prefix compiles and leaves the suite green while silently disabling that branch.
        assert!(
            crate::multi_client::SEND_FAILURE_DETAIL.starts_with("send failed:"),
            "{}",
            crate::multi_client::SEND_FAILURE_DETAIL
        );
        assert!(
            crate::multi_client::RECEIVE_FAILURE_DETAIL.starts_with("receive failed:"),
            "{}",
            crate::multi_client::RECEIVE_FAILURE_DETAIL
        );
        // The stranded outcome embeds the receive detail, and the runbook's `case` is
        // first-match, so a stranded row must still match the STRANDED arm before this one.
        let stranded = stranded_outcome(crate::multi_client::RECEIVE_FAILURE_DETAIL);
        assert!(
            stranded.starts_with("send settled but receive was not credited"),
            "{stranded}"
        );
    }

    #[test]
    fn stranded_outcome_keeps_the_runbook_anchor() {
        // The daily stranded check in docs/real-sats-pilot-runbook.md greps `wallet-cli show`
        // output for this exact leading substring. Rewording the outcome without updating that
        // grep in the same change silently disables the check, so pin the anchor here.
        // The assertion pins the STRING, not the runbook file: `docs/` is outside the nix source
        // filter (`flake.nix` `rustSrc.paths` lists only the manifests and the five crate dirs),
        // so an `include_str!` of the runbook from this crate's test code fails to build in the
        // sandbox. Do not add one back.
        const ANCHOR: &str = "send settled but receive was not credited";
        let msg = stranded_outcome("receive invoice expired");
        assert!(msg.starts_with(ANCHOR), "{msg}");
        // And the message must never send an operator after the preimage: it claims A's OUTGOING
        // contract and cannot credit B, so it recovers nothing here.
        assert!(!msg.contains("preimage"), "{msg}");
    }

    #[test]
    fn successful_send_then_terminal_failed_receive_strands() {
        // Mirror the `AwaitSettle` Success arm without live I/O: persist the preimage FIRST (§3),
        // then map the op-terminal receive. A failed receive after a settled send leaves the record
        // `Stranded`, still carrying the preimage as evidence the send leg completed, routed to the
        // terminal `Failed` surface (`perform` returns `Permanent(outcome)`).
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
            crate::multi_client::RECEIVE_FAILURE_DETAIL.into(),
        ));
        rec.phase = phase;
        rec.outcome = outcome;

        assert_eq!(rec.phase, MovePhase::Stranded);
        assert_eq!(
            rec.preimage,
            Some(preimage),
            "the evidence that the send leg settled is preserved"
        );
        assert_eq!(next_step(&rec), MoveStep::Failed);
        let msg = rec.outcome.clone().expect("stranded outcome present");
        assert!(
            msg.contains("not proven lost, not proven recoverable"),
            "{msg}"
        );
        assert!(
            msg.contains(crate::multi_client::RECEIVE_FAILURE_DETAIL),
            "{msg}"
        );
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

    /// A hair-under settle must recompute the cap at the DELIVERED net, not keep the one computed
    /// at the sized ask.
    ///
    /// This is the CLAMP-SAFETY invariant reached through the second door. `size_fresh_evacuation`
    /// writes `amount` and `fee_cap` together at the sized net; the receive fixed point can then
    /// deliver a hair under that, and an implementation that adjusts only `amount` leaves a cap
    /// belonging to the larger ask sitting on the record the Pay-step re-check and replay both
    /// trust.
    ///
    /// HONEST LIMIT OF THIS TEST: it pins the ARITHMETIC of the paired write and documents the
    /// invariant, but it drives `apply_evacuation_sizing` directly rather than the receive fixed
    /// point. The defect it was written for was not wrong arithmetic — the helper was always
    /// correct — it was the receive path assigning `rec.amount` WITHOUT going through the helper.
    /// So this test would NOT go red if that bypass were reintroduced. Catching that needs a
    /// fixture driving `drive_intent_step` through a hair-under settle against a mocked
    /// multi-client, which does not exist yet; until it does, the guard is the doc comment on
    /// `apply_evacuation_sizing` naming both callers.
    #[test]
    fn a_hair_under_settle_recomputes_the_cap_at_the_delivered_net() {
        let cap = EvacFeeCap {
            base_msat: Msat(200_000),
            bps: 300,
        };
        let mut rec = evacuation_record(Msat(75_000_000), Msat(50_000));
        // The sizing seam: 75_000 sats sized, cap = 200_000 + 3% of 75_000_000 = 2_450_000 msat.
        apply_evacuation_sizing(&mut rec, cap, Msat(75_000_000));
        assert_eq!(rec.amount, Msat(75_000_000));
        assert_eq!(rec.fee_cap, Msat(2_450_000));

        // The receive fixed point settles a hair under.
        let delivered = delivered_move_amount(Msat(74_000_000), rec.amount);
        assert_eq!(delivered, Msat(74_000_000));
        apply_evacuation_sizing(&mut rec, cap, delivered);

        // The cap MOVED with the amount: 200_000 + 3% of 74_000_000 = 2_420_000.
        assert_eq!(rec.amount, Msat(74_000_000));
        assert_eq!(
            rec.fee_cap,
            Msat(2_420_000),
            "the cap must be recomputed at the delivered net; keeping 2_450_000 would authorise \
             30_000 msat the executed net never entitled"
        );

        // A non-evacuation move carries no components: the rule is the stored cap as a CONSTANT,
        // so the same recompute at a lower net leaves it untouched.
        let constant = evacuation_cap_rule(None, Msat(50_000));
        assert_eq!(constant.at(Msat(75_000_000)), Msat(50_000));
        assert_eq!(constant.at(Msat(1_000)), Msat(50_000));
    }

    // --- Evacuation fee cap: sizing, enforcement, diagnostics (ADR-0029) ----------------------
    //
    // Every fixture below runs the PRODUCTION composition: the real §6 gross-up loop
    // (`resolve_receive_gross_up`), the real contract floor, the real cost assembly, the real
    // two-pass search and post-check. Only the two answers a live federation would give — the
    // destination's receive tx fee and the source's send-side dry-run — are scripted, which is
    // the same seam `resolve_receive_gross_up` is already tested through (§15.10).

    /// THE SEAM: a quote inside `cap.at(delivered)+1 ..= cap.at(sized_ask)` must be REFUSED.
    ///
    /// This band is the whole defect. The sizing search used to admit at `cap.at(sized_ask)`
    /// while the executor enforced `cap.at(delivered_net)`, so a quote in between passed sizing,
    /// minted and COMMITTED a receive operation, and was then refused at the Pay step — leaving
    /// the receive orphaned and unclaimed. With stable quotes the next watch cycle did it again.
    ///
    /// The live devimint gate could not see this: it ran with 6.7% fee headroom and an 80 msat
    /// hair-under, so the band was 2 msat wide and the real fee was nowhere near it. Neither
    /// could the rest of the suite — converting the cap basis moved no existing test. A fixture
    /// has to sit the fee STRICTLY INSIDE the band or it proves nothing about which net is used.
    ///
    /// Numbers are the live gate's, so the band is the one production actually produced:
    /// sized ask 449_998, delivered 449_918, `cap.at(sized) = 213_499`, `cap.at(delivered) =
    /// 213_497`. Every cost below delivers 449_918; only `total_fee` moves.
    #[test]
    fn a_quote_between_the_delivered_cap_and_the_sized_cap_is_refused() {
        const DELIVERED: u64 = 449_918;
        // A cost delivering DELIVERED whose two legs sum to `total_fee`.
        let cost_with_total = |total_fee: u64| {
            let receive_quote = total_fee - 200_000;
            FreshMoveCost {
                invoice_amount: Msat(DELIVERED + receive_quote),
                receive_quote: Msat(receive_quote),
                send_quote: Msat(200_000),
            }
        };

        assert_eq!(PILOT_CAP.at(Msat(449_998)), Msat(213_499), "cap at the ask");
        assert_eq!(
            PILOT_CAP.at(Msat(DELIVERED)),
            Msat(213_497),
            "cap at delivery"
        );

        // Every fixture really does deliver DELIVERED — otherwise the band is not the band.
        for total in [213_497, 213_498, 213_499] {
            assert_eq!(cost_with_total(total).delivered_net(), Msat(DELIVERED));
        }

        // AT the delivered cap: admitted. `total_within_cap` compares `<=`, and moving the basis
        // must not quietly turn the boundary into a refusal.
        assert!(
            fits_cap(cost_with_total(213_497), PILOT_CAP),
            "a fee exactly at the delivered cap still executes"
        );

        // INSIDE the band: refused, though both fit the cap taken on the ask. RED-FIRST — the
        // pre-fix predicate admits both of these, which is the bug.
        assert!(
            !fits_cap(cost_with_total(213_498), PILOT_CAP),
            "one msat over the DELIVERED cap is refused, even though it is under the 213_499 \
             cap the ask would have authorised"
        );
        assert!(
            !fits_cap(cost_with_total(213_499), PILOT_CAP),
            "and a fee landing exactly on the ASK's cap is refused too — that is the entire \
             point: the ask never arrived, so it never entitled that fee"
        );
    }

    /// `combined_verdict`'s cap gap reads the delivered net — pinned, because reverting it alone
    /// leaves the rest of the suite green. ONE site, not two: the viability half was deleted from
    /// this test when it turned out to assert only on its own literals, and the NOTE below says
    /// what remains uncovered.
    ///
    /// The seam test above pins `fits_cap`. That is one of five call sites this design pass
    /// converted, and the commit's own strongest evidence — that changing the basis moved no
    /// existing test — is exactly the argument that the other four are unguarded. This pins the
    /// two that are pure functions of a quote; the pre-receive gate and the executor's recompute
    /// need a driven `drive_intent_step` fixture and are called out as owed, not silently missing.
    #[test]
    fn combined_verdict_measures_its_cap_gap_against_the_delivered_net() {
        const DELIVERED: u64 = 449_918;
        // Delivers DELIVERED, and costs exactly one msat more than the DELIVERED cap allows —
        // while still sitting under the cap the ask (449_998 -> 213_499) would have authorised.
        let in_band = FreshMoveCost {
            invoice_amount: Msat(DELIVERED + 13_498),
            receive_quote: Msat(13_498),
            send_quote: Msat(200_000),
        };
        assert_eq!(in_band.delivered_net(), Msat(DELIVERED));
        assert_eq!(in_band.total_fee(), Msat(213_498));
        assert_eq!(PILOT_CAP.at(Msat(DELIVERED)), Msat(213_497));
        assert_eq!(PILOT_CAP.at(Msat(449_998)), Msat(213_499));

        // `combined_verdict` — the pass-2 predicate. Its cap gap must be measured against the
        // delivered cap, so this candidate MISSES by exactly one msat. Against the ask's cap it
        // would report a gap of zero and be admitted.
        let verdict = combined_verdict(
            CandidateQuote::Priced(in_band),
            PILOT_CAP,
            Msat(100_000_000), // affordable, so only the cap half can refuse it
        );
        assert!(
            !verdict.fits,
            "combined_verdict must refuse a quote over the DELIVERED cap"
        );
        assert_eq!(
            verdict.shortfall, 1,
            "and the gap is measured against cap.at(delivered) = 213_497, not cap.at(ask)"
        );

        // NOTE on what is NOT pinned here, so the name does not overclaim: the viability half of
        // the boundary probe (`total_fee <= delivered`, executor.rs) and the pre-mint gate both
        // read the delivered net too, but neither is reachable from a pure fixture — they need a
        // driven quote stream. An earlier version of this test "covered" viability by asserting
        // `500_000 > 449_918` on its own literals, which called no production code and stayed
        // green against the ask-based version. That is the exact vacuity this suite exists to
        // refuse, so it is deleted rather than left reading as coverage. `br-evac-cap-driven-basis-v07` owns that fixture;
        // where the driven fixture belongs.
    }

    /// PASS 1: the final re-quote must be revalidated, not admitted on the cap alone.
    ///
    /// The seam is only reachable when the BISECTION picks `pass1 != desired`. With
    /// `top_affordable` true, `pass1 == desired`, and one net cannot be both
    /// affordable-and-over-cap (the fast path's condition, `cap(N) < S-N`) and
    /// cap-fitting-and-unaffordable (the re-quote's, `S-N < fee <= cap(N)`) — those are direct
    /// contradictions. So the fast path here is UNAFFORDABLE, which sends the search into pass 1's
    /// bisection.
    ///
    /// Prices move the way they actually move: the FIRST quote of an amount is what the bisection
    /// saw, and a LATER quote of the SAME amount is the moved one — which is exactly the
    /// "the re-quote is not the one the bisection accepted" hazard the guard exists for.
    #[tokio::test]
    async fn pass_one_revalidates_its_final_requote() {
        use std::cell::RefCell;
        use std::collections::HashMap;
        let spendable = Msat(500_000);
        let desired = Msat(450_000);
        let seen: RefCell<HashMap<u64, u32>> = RefCell::new(HashMap::new());
        let quote = |net: Msat| {
            let nth = {
                let mut m = seen.borrow_mut();
                let c = m.entry(net.0).or_insert(0);
                *c += 1;
                *c
            };
            async move {
                // First sighting: cheap, affordable, cap-fitting — what the bisection accepts.
                // Later sighting of the SAME amount: the price moved. Still under the cap
                // (fee 150_000 <= cap.at(450_000) = 213_500) but the source can no longer fund
                // it (debit 450_000 + 150_000 = 600_000 > 500_000).
                let (receive_quote, send_quote) = if net.0 == 450_000 && nth == 1 {
                    // The FAST PATH's quote: unaffordable, so the search does not return there
                    // and `top_affordable` is false — which is what sends it into pass 1's
                    // bisection and makes `pass1 != desired`, the only shape this seam has.
                    (Msat(1_000), Msat(400_000))
                } else if nth == 1 {
                    (Msat(1_000), Msat(1_000))
                } else {
                    (Msat(1_000), Msat(149_000))
                };
                Ok(CandidateQuote::Priced(FreshMoveCost {
                    invoice_amount: Msat(net.0 + receive_quote.0),
                    receive_quote,
                    send_quote,
                }))
            }
        };

        let search = search_evacuation_net(
            desired,
            spendable,
            PILOT_CAP,
            oscillation_bound(spendable),
            quote,
        )
        .await
        .expect("no fault");

        // UNCONDITIONAL. An `if let Some(..)` here would skip its own body on the passing path —
        // the fix makes `sized` None — so the test would prove nothing about the code it names
        // and would keep passing if drift stopped reaching the seam entirely.
        assert_eq!(
            search.sized.map(|(n, c)| (n.0, c.source_debit().0)),
            None,
            "the moved re-quote is unaffordable, so nothing may be sized on it"
        );
        assert_eq!(
            search
                .largest_affordable
                .map(|(n, c)| (n.0, c.source_debit().0)),
            None,
            "and the unaffordable sample must not be recorded for the diagnostics either"
        );
        // And prove the seam was actually REACHED: the final re-quote is a SECOND sighting of the
        // amount pass 1 settled on. Without this the two assertions above are satisfied by a
        // search that never got there.
        // Prove the seam was REACHED, by the amount pass 1 actually settles on — not by "some
        // amount somewhere was quoted twice", which is satisfied incidentally: the fast path and
        // the bisection both probe 450_000, so that weaker form passes whether or not the final
        // re-quote ever runs. 449_999 is what the bisection returns here, and its SECOND sighting
        // is the re-quote at the admission this test is named for.
        let sightings = seen.borrow();
        assert!(
            sightings.get(&449_999).copied().unwrap_or(0) >= 2,
            "449_999 was not re-quoted, so pass 1's admission was never reached and the \
             assertions above prove nothing about it: {sightings:?}"
        );
    }

    /// A stale affordable sample must be CLEARED when the fresh re-quote says otherwise.
    ///
    /// The fast path records `largest_affordable` when its quote is affordable but over the cap
    /// (executor.rs, the `top_affordable` branch). `pass1` is then that same amount, and its
    /// re-quote can come back unfundable. A merely CONDITIONAL record leaves the earlier sample
    /// standing, and `no_fitting_amount_reason` goes on to measure a cap trend against a cost the
    /// source can no longer fund — telling an operator to raise `evac_fee_base_msat` for what is
    /// really a transient price movement.
    ///
    /// The pass-1 revalidation test cannot cover this: its fixture makes the fast path
    /// UNAFFORDABLE in order to reach the bisection, so nothing is ever recorded there.
    ///
    /// RED-FIRST against a conditional write instead of an assignment.
    #[tokio::test]
    async fn a_stale_affordable_sample_is_cleared_by_an_unaffordable_requote() {
        use std::cell::Cell;
        let spendable = Msat(500_000);
        let desired = Msat(200_000);
        let calls = Cell::new(0_u32);
        let quote = |net: Msat| {
            let n = calls.get();
            calls.set(n + 1);
            async move {
                // Call 0 — the fast path: AFFORDABLE (debit 450_000) but over cap.at(200_000) =
                // 206_000, so it records `largest_affordable` and falls through with
                // `top_affordable` true, making `pass1 == desired`.
                // Call 1+ — the re-quote at that same amount, now unfundable (debit 601_000).
                let (receive_quote, send_quote) = if n == 0 {
                    (Msat(125_000), Msat(125_000))
                } else {
                    (Msat(1_000), Msat(400_000))
                };
                Ok(CandidateQuote::Priced(FreshMoveCost {
                    invoice_amount: Msat(net.0 + receive_quote.0),
                    receive_quote,
                    send_quote,
                }))
            }
        };

        let search = search_evacuation_net(
            desired,
            spendable,
            PILOT_CAP,
            oscillation_bound(spendable),
            quote,
        )
        .await
        .expect("no fault");

        assert_eq!(
            search
                .largest_affordable
                .map(|(n, c)| (n.0, c.source_debit().0)),
            None,
            "the fast path's sample is superseded by the fresh re-quote at the SAME amount, which \
             the source cannot fund — leaving it would let the refusal diagnostics measure a cap \
             trend on a cost that no longer holds"
        );
    }

    /// PASS 2: its final re-quote must be revalidated too.
    ///
    /// Reaching pass 2 at all requires pass 1 to FAIL THE CAP at the largest affordable amount —
    /// otherwise pass 1 admits and returns. So everything above 340_000 is priced over the cap,
    /// and the affordable, cap-fitting amounts live in the bottom window pass 2 searches. The
    /// moved re-quote is again a LATER quote of the same amount: what the bisection accepted is
    /// not what the admission sees.
    #[tokio::test]
    async fn pass_two_revalidates_its_final_requote() {
        use std::cell::RefCell;
        use std::collections::HashMap;
        let spendable = Msat(500_000);
        let desired = Msat(400_000);
        let seen: RefCell<HashMap<u64, u32>> = RefCell::new(HashMap::new());
        let quote = |net: Msat| {
            let nth = {
                let mut m = seen.borrow_mut();
                let c = m.entry(net.0).or_insert(0);
                *c += 1;
                *c
            };
            async move {
                // Above 340_000 every sighting is dear, so the fast path is unaffordable and
                // pass 1's bisection settles just under that. A FIRST sighting anywhere below is
                // cheap — what both bisections accept. A LATER sighting is the moved price, and
                // it is dear enough to be BOTH over the cap and unfundable, which is what makes
                // pass 1 fail the cap (so the search reaches pass 2) and what pass 2's final
                // re-quote must then reject.
                let (receive_quote, send_quote) = if net.0 > 340_000 || nth > 1 {
                    (Msat(1_000), Msat(260_000))
                } else {
                    (Msat(1_000), Msat(1_000))
                };
                Ok(CandidateQuote::Priced(FreshMoveCost {
                    invoice_amount: Msat(net.0 + receive_quote.0),
                    receive_quote,
                    send_quote,
                }))
            }
        };

        let search = search_evacuation_net(
            desired,
            spendable,
            PILOT_CAP,
            oscillation_bound(spendable),
            quote,
        )
        .await
        .expect("no fault");

        // UNCONDITIONAL, for the same reason as the pass-1 test: on the passing path `sized` is
        // None, so a conditional assertion would never execute.
        assert_eq!(
            search.sized.map(|(n, c)| (n.0, c.source_debit().0)),
            None,
            "pass 2's final re-quote is unaffordable, so nothing may be sized on it"
        );
        // Same, for the amount pass 2 settles on. The weaker "any amount twice" form is
        // satisfied by the pass-1 probes that precede this and proves nothing here.
        let sightings = seen.borrow();
        assert!(
            sightings.get(&339_997).copied().unwrap_or(0) >= 2,
            "339_997 was not re-quoted, so pass 2's admission was never reached: {sightings:?}"
        );
    }

    /// The shipped evacuation cap: 200 sats + 3%.
    const PILOT_CAP: EvacFeeCap = EvacFeeCap {
        base_msat: Msat(200_000),
        bps: 300,
    };

    /// The runbook's ABSOLUTE `--max-fee`, expressed as the same cap shape with a zero rate —
    /// which is exactly what the code did before ADR-0029, and what a LEGACY intent still gets.
    /// Fixtures use it as the red-first baseline.
    const OLD_ABSOLUTE_CAP: EvacFeeCap = EvacFeeCap {
        base_msat: Msat(50_000),
        bps: 0,
    };

    fn gw(base_msat: u64, ppm: u64) -> fee::GatewayFee {
        fee::GatewayFee {
            base_msat: Msat(base_msat),
            ppm,
        }
    }

    type FedFee = Box<dyn Fn(Msat) -> Msat>;

    struct TestRoute {
        receive_gateway: fee::GatewayFee,
        send_gateway: fee::GatewayFee,
        /// The DESTINATION federation's receive tx fee, quoted on the contract amount.
        recv_fed_fee: FedFee,
        /// The SOURCE's send-side fee, quoted on the outgoing contract: the lnv2 module fee plus
        /// the per-note MINT fee. This is where a note-count discontinuity enters, exactly as it
        /// does live.
        send_fed_fee: FedFee,
    }

    impl TestRoute {
        /// A route with the given gateway fees and negligible federation/mint fees — the
        /// PRECONDITION the pinned aggregates in the viability fixture are computed under.
        fn new(receive_gateway: fee::GatewayFee, send_gateway: fee::GatewayFee) -> Self {
            Self {
                receive_gateway,
                send_gateway,
                recv_fed_fee: Box::new(|_| Msat(0)),
                send_fed_fee: Box::new(|_| Msat(0)),
            }
        }

        fn with_send_fed_fee(mut self, quote: impl Fn(Msat) -> Msat + 'static) -> Self {
            self.send_fed_fee = Box::new(quote);
            self
        }

        /// The same composition `FedimintExecutor::quote_fresh_send_required_cost` performs.
        async fn quote(&self, net: Msat, spendable: Msat) -> Result<CandidateQuote, ExecError> {
            if net.0 == 0 {
                return Ok(CandidateQuote::Unquotable {
                    source_shortfall: None,
                });
            }
            let grossed =
                resolve_receive_gross_up(net, self.receive_gateway, |contract| async move {
                    Ok((self.recv_fed_fee)(contract))
                })
                .await?;
            if grossed.contract_amount.0 < MINIMUM_INCOMING_CONTRACT_MSAT {
                return Ok(CandidateQuote::Unquotable {
                    source_shortfall: None,
                });
            }
            let send_gateway_quote = self.send_gateway.on(grossed.invoice_amount);
            let outgoing = Msat(grossed.invoice_amount.0 + send_gateway_quote.0);
            let send_tx_fee = (self.send_fed_fee)(outgoing);
            // The mint's dry-run: the source must fund the whole outgoing contract plus its fee,
            // and reports the GAP when it cannot (`InsufficientBalanceError`).
            let required = outgoing.0 + send_tx_fee.0;
            if required > spendable.0 {
                return Ok(CandidateQuote::Unquotable {
                    source_shortfall: Some(required - spendable.0),
                });
            }
            Ok(CandidateQuote::Priced(FreshMoveCost {
                invoice_amount: grossed.invoice_amount,
                receive_quote: grossed.receive_quote,
                send_quote: Msat(send_gateway_quote.0 + send_tx_fee.0),
            }))
        }

        async fn size(&self, desired: Msat, spendable: Msat, cap: EvacFeeCap) -> EvacuationSizing {
            size_evacuation(desired, spendable, cap, |net| self.quote(net, spendable))
                .await
                .expect("the scripted route never faults")
        }

        async fn search(
            &self,
            desired: Msat,
            spendable: Msat,
            cap: EvacFeeCap,
        ) -> EvacuationSearch {
            search_evacuation_net(
                desired,
                spendable,
                cap,
                oscillation_bound(spendable),
                |net| self.quote(net, spendable),
            )
            .await
            .expect("the scripted route never faults")
        }

        async fn cost_at(&self, net: Msat, spendable: Msat) -> FreshMoveCost {
            match self.quote(net, spendable).await.expect("no fault") {
                CandidateQuote::Priced(cost) => cost,
                other => panic!("expected {net:?} to price, got {other:?}"),
            }
        }
    }

    impl EvacuationSizing {
        fn expect_sized(&self) -> Msat {
            match self {
                EvacuationSizing::Sized(net) => *net,
                EvacuationSizing::Refused(reason) => {
                    panic!("expected a sized evacuation: {reason}")
                }
            }
        }

        fn expect_refused(&self) -> &str {
            match self {
                EvacuationSizing::Sized(net) => panic!("expected a refusal, got {net:?}"),
                EvacuationSizing::Refused(reason) => reason,
            }
        }
    }

    /// The gateway shape measured on the pilot: at a 1_000_000 msat net the receive leg costs
    /// exactly 8_900 msat and the send leg exactly 8_948 — the 17_848 msat (1.7848%) figure
    /// ADR-0029 records. The ppms are chosen so the fixture reproduces those two numbers
    /// EXACTLY at that amount (asserted below), rather than approximating them.
    fn pilot_route() -> TestRoute {
        TestRoute::new(gw(2_000, 6_840), gw(2_000, 6_887))
    }

    #[tokio::test]
    async fn the_pilot_fixture_reproduces_the_measured_swap_cost() {
        let route = pilot_route();
        let cost = route.cost_at(Msat(1_000_000), Msat(u64::MAX)).await;
        assert_eq!(cost.receive_quote, Msat(8_900), "measured receive leg");
        assert_eq!(cost.send_quote, Msat(8_948), "measured send leg");
        assert_eq!(cost.total_fee(), Msat(17_848));
    }

    /// CLAMP SAFETY — the money hole this bead exists to close, and the one an implementation can
    /// leave open while passing everything else. The allocator plans 75_000 sats and stamps the
    /// cap at THAT amount (2_450 sats); the executor can only afford 1_000 sats, so the cap it
    /// enforces must be 230 sats — base + 3% of what MOVED, not of what was planned.
    #[tokio::test]
    async fn the_enforced_cap_follows_the_executed_net_not_the_planned_amount() {
        let route = TestRoute::new(gw(2_000, 3_000), gw(2_000, 3_000));
        let desired = Msat(75_000_000);
        // Exactly the source debit of a 1_000_000 msat net over this route (invoice 1_005_015 +
        // a 5_015 send quote), so the affordability search lands on 1_000_000 and not a msat
        // more: one msat of net needs one more msat of invoice, which the balance cannot cover.
        let spendable = Msat(1_010_030);

        let net = route
            .size(desired, spendable, PILOT_CAP)
            .await
            .expect_sized();
        assert_eq!(net, Msat(1_000_000), "clamped by what the source can fund");

        let mut rec = evacuation_record(desired, PILOT_CAP.at(desired));
        assert_eq!(
            rec.fee_cap,
            Msat(2_450_000),
            "the PLANNED cap, at the planned amount"
        );
        apply_evacuation_sizing(&mut rec, PILOT_CAP, net);
        assert_eq!(rec.amount, Msat(1_000_000));
        assert_eq!(
            rec.fee_cap,
            Msat(230_000),
            "200_000 base + floor(1_000_000 * 300 / 10_000) — of the EXECUTED net"
        );
        assert_ne!(
            rec.fee_cap,
            Msat(2_450_000),
            "keeping the planned cap would authorise more than 10x the entitlement"
        );
    }

    /// CAP ARITHMETIC. The cap is computed on the NET while each charged fee is computed on its
    /// own real SDK base — the receive fee on the GROSS invoice, the send fee on the invoice. At
    /// a 1_000_000 msat net this route's invoice is 1_100_000, so a cap taken on the gross would
    /// be 233_000 instead of 230_000. Exact-cap executes (`total_within_cap` compares `<=`);
    /// one msat more is refused, and it is refused at 230_001 — under the 233_000 a gross-based
    /// cap would have allowed.
    #[tokio::test]
    async fn exact_cap_is_admitted_and_cap_plus_one_msat_is_refused() {
        let net = Msat(1_000_000);
        let spendable = Msat(10_000_000);
        // Receive: 44_997 base + ~5% of the GROSS invoice, a rate whose floor does not step at
        // 1_100_000 — so the minimal invoice that nets exactly 1_000_000 IS 1_100_000, the
        // amount ADR-0029's own worked example uses. Send: a flat base.
        let exact = TestRoute::new(gw(44_997, 50_003), gw(130_000, 0));
        let cost = exact.cost_at(net, spendable).await;
        assert_eq!(cost.invoice_amount, Msat(1_100_000));
        assert_eq!(
            cost.receive_quote,
            Msat(100_000),
            "charged on the 1_100_000 gross"
        );
        assert_eq!(cost.total_fee(), Msat(230_000));
        assert_eq!(
            PILOT_CAP.at(net),
            Msat(230_000),
            "the cap is taken on the NET"
        );
        assert_eq!(
            cost.delivered_net(),
            net,
            "this fixture is an EXACT solve, so the delivered net IS the ask — which is what \
             makes the pinned cap numbers below comparable to the ask at all"
        );
        assert!(
            fits_cap(cost, PILOT_CAP),
            "a quote landing EXACTLY on the cap executes — total_within_cap compares `<=`"
        );
        assert_eq!(
            exact.size(net, spendable, PILOT_CAP).await.expect_sized(),
            net,
            "so the whole ask is evacuated in one operation"
        );

        // One msat more, and the SAME net is refused.
        let over = TestRoute::new(gw(44_997, 50_003), gw(130_001, 0));
        let over_cost = over.cost_at(net, spendable).await;
        assert_eq!(over_cost.total_fee(), Msat(230_001));
        assert!(
            !fits_cap(over_cost, PILOT_CAP),
            "cap plus one msat is refused at this net"
        );
        let cap_taken_on_the_gross = PILOT_CAP.base_msat.0 + 1_100_000 * 300 / 10_000;
        assert_eq!(cap_taken_on_the_gross, 233_000);
        assert!(
            230_001 < cap_taken_on_the_gross,
            "230_001 sits UNDER a cap taken on the 1_100_000 GROSS invoice, so refusing it is \
             exactly what pins the cap to the NET rather than to what the SDK charges on"
        );
        // The search then downsizes rather than giving up — correct, and not in tension with the
        // above: what it must never do is EXECUTE at a net whose quote broke the cap.
        let downsized = over.size(net, spendable, PILOT_CAP).await.expect_sized();
        assert!(downsized < net, "downsized to {downsized:?}");
        let downsized_cost = over.cost_at(downsized, spendable).await;
        assert!(fits_cap(downsized_cost, PILOT_CAP));
    }

    /// A FULL-BALANCE evacuation drains the source in ONE operation. "At full size" is
    /// impossible — `source_debit = invoice + send_quote` and `invoice = net + receive_quote`, so
    /// any positive fee makes the debit exceed the net — the claim is that the executed net is
    /// the MAXIMAL amount whose net plus both fee legs fits spendable, and that this is one
    /// operation rather than the ~27 chunks the absolute cap produced.
    #[tokio::test]
    async fn a_full_balance_evacuation_drains_in_one_operation() {
        let route = pilot_route();
        let balance = Msat(75_000_000);

        let net = route.size(balance, balance, PILOT_CAP).await.expect_sized();
        let cost = route.cost_at(net, balance).await;
        // Pinned for this fixture: the exact maximal net and its exact source debit.
        assert_eq!(net, Msat(73_973_545));
        assert_eq!(
            cost.source_debit(),
            Msat(75_000_000),
            "the balance, to the msat"
        );
        assert!(cost.source_debit() <= balance, "never overdraws");
        // One msat more does not fit — this is the maximum, not merely a large amount.
        assert!(
            !matches!(
                route.quote(Msat(net.0 + 1), balance).await.expect("no fault"),
                CandidateQuote::Priced(cost) if cost.source_debit() <= balance
            ),
            "the executed net is the MAXIMAL affordable one"
        );
        // ADR-0029 quotes ~73_685 sats by applying the measured 1.7848% flat rate to the whole
        // balance. That over-charges, because the 2-sat bases do not scale with the amount: the
        // exact figure sits a little above it. Both agree to well under a percent.
        let flat_rate_estimate = 73_685_000f64;
        assert!(
            ((net.0 as f64 - flat_rate_estimate) / flat_rate_estimate).abs() < 0.005,
            "within half a percent of ADR-0029's flat-rate estimate: {net:?}"
        );

        // RED-FIRST: the OLD absolute 50_000 msat cap over the SAME route sizes a chunk of a few
        // thousand sats — the ~1/27th chunk-drain the fee cap was changed to end.
        let chunk = route
            .size(balance, balance, OLD_ABSOLUTE_CAP)
            .await
            .expect_sized();
        assert_eq!(
            chunk,
            Msat(3_326_248),
            "~3_326 sats — a 22nd of the balance"
        );
        assert!(
            net.0 / chunk.0 >= 20,
            "at least twenty operations where the new cap needs one"
        );
        // ADR-0029 quotes ~27 chunks, from the measured 1.7848% applied flat to the whole
        // balance against the 50-sat cap. The exact figure here is ~22 for the same reason the
        // delivered net is a little higher than its estimate: the 2-sat bases do not scale, so
        // the effective rate at 75_000 sats is below the rate measured at 1_000.
    }

    /// ECONOMIC VIABILITY — the largest money-loss hole. A gateway with COMPLIANT bases (send 99
    /// sats, receive 49) and a hostile, SEND-HEAVY ppm split (send 940_000, receive 10_000): the
    /// largest cap-fitting chunk delivers ~5.35 sats for a ~200-sat fee, so the balance would
    /// drain in ~365 chunks, delivering ~1_953 sats and burning ~73_047. The route does not
    /// serve, and the evacuation must NOT proceed over it.
    ///
    /// The split matters and a combined figure would be wrong: solvability is governed by the
    /// RECEIVE ppm alone (it shrinks the contract directly), so the hostile load has to sit on
    /// the SEND leg. Both ppms are therefore pinned separately.
    fn burning_route() -> TestRoute {
        TestRoute::new(gw(49_000, 10_000), gw(99_000, 940_000))
    }

    #[tokio::test]
    async fn a_route_that_costs_more_than_it_delivers_does_not_serve() {
        let route = burning_route();
        let balance = Msat(75_000_000);

        let refusal = route.size(balance, balance, PILOT_CAP).await;
        let reason = refusal.expect_refused();
        assert!(
            reason.contains("costs more than it delivers"),
            "the refusal must name the viability rule: {reason}"
        );

        // The chunk the search WOULD have taken without the post-check, and what it costs.
        let search = route.search(balance, balance, PILOT_CAP).await;
        let (chunk, cost) = search
            .sized
            .expect("the cap admits a chunk; only viability refuses it");
        assert_eq!(
            chunk,
            Msat(5_357),
            "the largest cap-fitting net, ~5.36 sats"
        );
        assert_eq!(
            cost.total_fee(),
            Msat(200_160),
            "~200 sats of fees to move ~5.4"
        );
        assert_eq!(
            PILOT_CAP.at(chunk),
            Msat(200_160),
            "sitting exactly at the cap"
        );
        assert!(cost.total_fee() > chunk, "it burns more than it delivers");
    }

    /// The red half of the viability criterion: WITHOUT the post-check the same route drains the
    /// balance in ~365 chunks, delivering ~1_953 sats while burning ~73_047. Driven through the
    /// real search, one chunk per round, exactly as the watch cycle would re-emit them.
    #[tokio::test]
    async fn without_the_viability_check_the_same_route_burns_the_balance() {
        let route = burning_route();
        let mut remaining = Msat(75_000_000);
        let mut delivered = 0u64;
        let mut chunks = 0u32;
        while let Some((net, cost)) = route.search(remaining, remaining, PILOT_CAP).await.sized {
            delivered += net.0;
            remaining = Msat(remaining.0 - cost.source_debit().0);
            chunks += 1;
            assert!(chunks < 1_000, "the drain must terminate");
        }
        let burned = 75_000_000 - delivered;
        assert!((360..=370).contains(&chunks), "~365 chunks, got {chunks}");
        assert!(
            (1_940_000..=1_970_000).contains(&delivered),
            "~1_953 sats delivered, got {delivered} msat"
        );
        assert!(
            (73_030_000..=73_060_000).contains(&burned),
            "~73_047 sats burned, got {burned} msat"
        );
        assert!(
            burned * 100 / 75_000_000 > 95,
            "a ~97% loss — the evacuation destroying the balance it exists to rescue"
        );
    }

    /// The viability rule is a POST-CHECK, never a term in the fits predicate: the search returns
    /// the same `n*` either way, and the check is applied to THAT result. Folding `fee <= n` into
    /// the predicate would re-break the fits-then-doesn't shape the bisection needs — and would
    /// change `n*`, which this pins against.
    #[tokio::test]
    async fn the_viability_check_is_a_post_check_on_the_search_result() {
        let route = burning_route();
        let balance = Msat(75_000_000);
        let search = route.search(balance, balance, PILOT_CAP).await;
        let (n_star, cost) = search
            .sized
            .expect("the search itself finds a cap-fitting chunk");
        assert_eq!(n_star, Msat(5_357));

        // The post-check, applied to that result, is what refuses.
        let verdict = evacuation_viability(
            (n_star, cost),
            balance,
            PILOT_CAP,
            oscillation_bound(balance),
            |net| route.quote(net, balance),
        )
        .await
        .expect("no fault");
        verdict.expect_refused();

        // And the full pipeline's search result is unchanged by the post-check being there.
        let full = route.search(balance, balance, PILOT_CAP).await;
        assert_eq!(full.sized.map(|(net, _)| net), Some(n_star));
    }

    /// NOTE-COUNT DISCONTINUITY. The search's top fails the viability check by a few hundred msat
    /// ONLY because crossing a 2^17 msat tier raised the mint's per-note fee; one note-selection
    /// boundary below, the same route serves. It must EVACUATE, not refuse.
    #[tokio::test]
    async fn a_note_count_discontinuity_at_the_top_probes_below_instead_of_refusing() {
        // Zero gateway fees, so the net, the invoice and the outgoing contract coincide and the
        // discontinuity sits exactly on the tier it names.
        let route = TestRoute::new(gw(0, 0), gw(0, 0)).with_send_fed_fee(|outgoing| {
            Msat(if outgoing.0 >= 131_072 {
                132_000
            } else {
                130_000
            })
        });
        let spendable = Msat(263_100);

        let search = route.search(spendable, spendable, PILOT_CAP).await;
        let (top, cost) = search.sized.expect("the cap admits the top");
        assert_eq!(
            top,
            Msat(131_100),
            "the largest affordable, cap-fitting net"
        );
        assert_eq!(cost.total_fee(), Msat(132_000));
        // RED-FIRST: a top-only implementation refuses here — the top costs more than it delivers.
        assert!(
            cost.total_fee() > top,
            "the top fails the viability check by 900 msat"
        );

        let net = route
            .size(spendable, spendable, PILOT_CAP)
            .await
            .expect_sized();
        assert_eq!(
            net,
            Msat(131_071),
            "one tier boundary below, under the fee jump, the route serves"
        );
        let served = route.cost_at(net, spendable).await;
        assert!(
            served.total_fee().0 <= net.0,
            "and it delivers more than it costs"
        );
    }

    /// A shortfall LARGER than one oscillation bound must still probe below before refusing.
    ///
    /// ADR-0029: "Refuse only when the top fails by strictly MORE than `A` AND that is an
    /// analytically proven structural refusal ... a bare shortfall over `A` is inconclusive, not
    /// proof." `A` bounds ONE vertical fee jump, so two nearby note-count drops can each stay
    /// under it while together exceeding it — leaving a serving candidate just below a top that
    /// missed by more than `A`.
    ///
    /// RED-FIRST: an implementation that returns early on `shortfall > A` refuses here, and
    /// because the quotes are stable it takes that same branch every tick — stranding a dying
    /// federation that had an executable evacuation the whole time. That is the livelock this
    /// bead exists to remove, reintroduced by its own refusal path.
    #[tokio::test]
    async fn a_shortfall_over_the_oscillation_bound_still_probes_below() {
        // THREE levels, so the top fails by more than `A` and a LOWER boundary still serves:
        //   >= 131_072  fee 500_000 — far over any cap, so the search cannot go here
        //   >= 131_071  fee 150_000 — the top: cap-fitting and affordable, but costs MORE than it
        //                             delivers, missing by 18_929 — MORE than one bound (16_306)
        //   >= 131_000  fee 140_000 — one drop of 10_000, still costs more than it delivers
        //   <  131_000  fee 130_000 — a second drop of 10_000; the route now serves
        //
        // Each drop alone is under `A`; only together do they exceed it. That is exactly the case
        // a bare `shortfall > A` refusal discards, and both drops sit inside the bounded probe's
        // reach (it walks tier-aligned boundaries within ~130 msat of the top, not distant tiers).
        let route = TestRoute::new(gw(0, 0), gw(0, 0)).with_send_fed_fee(|outgoing| {
            Msat(if outgoing.0 >= 131_072 {
                500_000
            } else if outgoing.0 >= 131_071 {
                150_000
            } else if outgoing.0 >= 131_000 {
                140_000
            } else {
                130_000
            })
        });
        let spendable = Msat(1_000_000);

        let search = route.search(spendable, spendable, PILOT_CAP).await;
        let (top, cost) = search.sized.expect("the cap admits the top");
        assert_eq!(
            top,
            Msat(131_071),
            "the largest cap-fitting, affordable net"
        );
        assert_eq!(cost.total_fee(), Msat(150_000));

        // STRICTLY GREATER than one oscillation bound — the branch that used to refuse outright.
        let shortfall = cost.total_fee().0 - top.0;
        let bound = oscillation_bound(spendable);
        assert!(
            shortfall > bound,
            "fixture must exercise the over-A branch: shortfall {shortfall} vs A {bound}"
        );

        // It must nonetheless probe down and find the serving candidate.
        let net = route
            .size(spendable, spendable, PILOT_CAP)
            .await
            .expect_sized();
        assert_eq!(
            net,
            Msat(130_943),
            "past the second drop the fee falls under the net and the route serves"
        );
        let served = route.cost_at(net, spendable).await;
        assert!(served.total_fee().0 <= net.0);
    }

    /// NON-MONOTONE SIZING, INCREASING REGIME. Gateway bases 99 + 49 sats EXCEED this fixture's
    /// CONFIGURED 20-sat cap base (no executable gateway base can exceed the 200-sat default, so
    /// stating it against the default would make the regime untestable), while the combined ppm
    /// (5_000) sits BELOW the 300 bps cap rate. Feasibility therefore RISES with the amount: the
    /// feasible set is a TOP window, and the evacuation still proceeds.
    #[tokio::test]
    async fn the_increasing_regime_finds_the_top_window() {
        let route = TestRoute::new(gw(49_000, 2_500), gw(99_000, 2_500));
        let cap = EvacFeeCap {
            base_msat: Msat(20_000),
            bps: 300,
        };
        let balance = Msat(10_000_000);

        let net = route.size(balance, balance, cap).await.expect_sized();
        assert_eq!(net, Msat(9_802_620), "the top of the window, ~9_802 sats");
        let cost = route.cost_at(net, balance).await;
        assert!(
            cost.total_fee() <= cap.at(net),
            "fee {:?} against cap {:?}",
            cost.total_fee(),
            cap.at(net)
        );

        // The window bottom is ~5_123 sats, comfortably clear of the ~5_002 sats a combined
        // bisection probes first, so the miss below is STRUCTURAL and not a matter of slack.
        let bottom_cost = route.cost_at(Msat(5_002_500), balance).await;
        assert!(
            bottom_cost.total_fee() > cap.at(Msat(5_002_500)),
            "the first combined probe fails the cap"
        );

        // RED-FIRST: the strict single bisection on the COMBINED predicate — the search this code
        // had, whose contract required monotonicity — probes ~5_002 sats, fails on the cap,
        // collapses `hi` and discards the whole feasible window.
        let naive =
            largest_fitting_amount(MINIMUM_INCOMING_CONTRACT_MSAT, balance.0, 0, |amount| {
                let quoted = route.quote(Msat(amount), balance);
                async move { Ok(combined_verdict(quoted.await?, cap, balance)) }
            })
            .await
            .expect("no fault");
        assert_eq!(
            naive, None,
            "a combined bisection returns to the exact livelock this change exists to kill"
        );
    }

    /// NON-MONOTONE SIZING, DECREASING REGIME — the mirror. A zero-base gateway with a combined
    /// 400 bps rate ABOVE the cap's 300: feasibility FALLS with the amount, so the feasible set
    /// is a BOTTOM window that pass 2 finds, and a cap-compliant amount drains.
    #[tokio::test]
    async fn the_decreasing_regime_finds_the_bottom_window() {
        let route = TestRoute::new(gw(0, 20_000), gw(0, 20_000));
        let balance = Msat(75_000_000);

        let net = route.size(balance, balance, PILOT_CAP).await.expect_sized();
        assert!(
            net.0 <= 20_000_000,
            "a cap-compliant amount, not the whole balance: {net:?}"
        );
        assert_eq!(net, Msat(18_490_738));
        let cost = route.cost_at(net, balance).await;
        assert!(cost.total_fee() <= PILOT_CAP.at(net));
        // One msat more breaks the cap: this is the top of the bottom window.
        let over = route.cost_at(Msat(net.0 + 1), balance).await;
        assert!(over.total_fee() > PILOT_CAP.at(Msat(net.0 + 1)));
    }

    /// SIZING NEVER EXCEEDS `desired`. The caller has already clamped `desired` to the
    /// destination's remaining ADR-0018 cap room, and evacuations are EXEMPT from
    /// `enforce_destination_cap`, so nothing downstream would catch a larger size. Both passes
    /// are bounded above by it even when the source could fund far more.
    #[tokio::test]
    async fn sizing_never_exceeds_the_destination_cap_room() {
        let route = TestRoute::new(gw(49_000, 2_500), gw(99_000, 2_500));
        let cap = EvacFeeCap {
            base_msat: Msat(20_000),
            bps: 300,
        };
        let balance = Msat(75_000_000);
        let cap_room = Msat(10_000_000);

        let clamped = route.size(cap_room, balance, cap).await.expect_sized();
        assert!(
            clamped <= cap_room,
            "sized {clamped:?} over {cap_room:?} of cap room"
        );

        // Unclamped, the same source funds a far larger drain — so the bound is what constrains
        // it, not the route.
        let unclamped = route.size(balance, balance, cap).await.expect_sized();
        assert!(
            unclamped.0 > cap_room.0 * 5,
            "the source could fund {unclamped:?}, which is what makes the bound load-bearing"
        );
    }

    /// LIVELOCK / GENUINE REFUSAL. A gateway whose summed two-leg quote strictly exceeds the cap
    /// at EVERY amount — bases 99 + 49 sats against a configured 100-sat cap base with a ZERO
    /// rate — stays `Retryable` AND says why. Red-first: without the diagnostic it retries
    /// silently forever, which is the failure this change exists to kill.
    #[tokio::test]
    async fn a_fixed_component_above_the_cap_base_refuses_with_a_structural_diagnostic() {
        let route = TestRoute::new(gw(49_000, 2_500), gw(99_000, 2_500));
        let cap = EvacFeeCap {
            base_msat: Msat(100_000),
            bps: 0,
        };
        let balance = Msat(75_000_000);

        let reason = route.size(balance, balance, cap).await;
        let reason = reason.expect_refused();
        assert!(
            reason.contains("structural refusal"),
            "a silent indefinite retry fails this criterion: {reason}"
        );
        assert!(
            reason.contains("fixed component alone"),
            "the cause must be the complete intercept, not the cap trend: {reason}"
        );
        // With a ZERO cap rate the cap-trend condition CANNOT fire, which is exactly why the
        // intercept condition has to exist.
        assert!(
            !reason.contains("cap's trend"),
            "condition (i) cannot fire here: {reason}"
        );
    }

    /// A fee above base + proportional is still REFUSED — the cap must bound a hostile gateway,
    /// not merely stop refusing. Here the source could comfortably afford the move; only the cap
    /// stands in the way.
    #[tokio::test]
    async fn a_fee_above_base_plus_proportional_is_still_refused() {
        let route = TestRoute::new(gw(5_000_000, 0), gw(0, 0));
        let balance = Msat(75_000_000);
        // Affordable at every size, and far over the cap at every size.
        let cost = route.cost_at(Msat(1_000_000), balance).await;
        assert!(cost.source_debit() <= balance, "the source can afford it");
        assert!(
            cost.total_fee() > PILOT_CAP.at(Msat(1_000_000)),
            "but the cap refuses it"
        );

        route
            .size(balance, balance, PILOT_CAP)
            .await
            .expect_refused();
    }

    /// Base fees above the OLD absolute cap no longer livelock. The refusal half IS assertable —
    /// under the 50_000 msat absolute cap the 148_000 msat of bases alone never fit at any size.
    #[tokio::test]
    async fn base_fees_above_the_old_absolute_cap_no_longer_livelock() {
        let route = TestRoute::new(gw(49_000, 2_500), gw(99_000, 2_500));
        let balance = Msat(75_000_000);

        let old = route.size(balance, balance, OLD_ABSOLUTE_CAP).await;
        assert!(
            old.expect_refused().contains("structural refusal"),
            "the absolute cap refuses at every size — the livelock"
        );

        let net = route.size(balance, balance, PILOT_CAP).await.expect_sized();
        assert!(
            net.0 > 70_000_000,
            "base + proportional admits the same route: {net:?}"
        );
    }

    /// STRUCTURAL REFUSAL IS DIAGNOSABLE, on the adversarial knife-edge fixture: policy
    /// (base 100_000 msat, bps 300), gateway receive (base 3, ppm 0), send (base 99_999, ppm
    /// 29_999). `None` is deliberately NOT asserted as the required outcome — `total_within_cap`
    /// admits an exact-cap candidate, so if the search happens to probe a fitting amount,
    /// returning it is CORRECT. What is required is that a REFUSAL is never silent.
    #[tokio::test]
    async fn the_knife_edge_fixture_is_never_refused_silently() {
        let route = TestRoute::new(gw(3, 0), gw(99_999, 29_999));
        let cap = EvacFeeCap {
            base_msat: Msat(100_000),
            bps: 300,
        };
        // Sized so the largest affordable net is 1_000_000 msat, below the ~2_090_000 msat where
        // this route's fee finally falls under the cap.
        let spendable = Msat(1_130_002);

        match route.size(spendable, spendable, cap).await {
            EvacuationSizing::Sized(net) => {
                let cost = route.cost_at(net, spendable).await;
                assert!(
                    cost.total_fee() <= cap.at(net),
                    "returning a fitting amount is correct, but it must actually fit"
                );
            }
            EvacuationSizing::Refused(reason) => {
                assert!(
                    reason.contains("structural refusal"),
                    "a refusal here must carry the diagnostic: {reason}"
                );
                assert!(
                    reason.contains("cap's trend"),
                    "and must name the cap trend: {reason}"
                );
            }
        }
    }

    /// THE PPM WARNING FIRES, AND DOES NOT GATE. Both halves: the warning is produced for a send
    /// ppm of 29_999 under a sub-limit base, and the very same route is still ADMITTED when the
    /// cap admits it. Rejecting on ppm would contradict ADR-0029 and could strand funds behind
    /// the only live route.
    #[tokio::test]
    async fn an_out_of_envelope_ppm_warns_without_gating_the_route() {
        let fees = FreshSendRequiredGatewayFees {
            receive: gw(3, 0),
            send: gw(99_999, 29_999),
        };
        let warning =
            ppm_envelope_warning(fees).expect("29_999 ppm is outside the 15_000 envelope");
        assert!(warning.contains("send 29999 ppm"));
        assert!(
            warning.contains("NOT refused"),
            "the warning states it does not gate"
        );

        // Honest defaults (the lnv2 gateway default is 2 sats + 3_000 ppm) stay silent.
        assert_eq!(
            ppm_envelope_warning(FreshSendRequiredGatewayFees {
                receive: gw(2_000, 3_000),
                send: gw(2_000, 3_000),
            }),
            None
        );

        // ...and the flagged route is still sized, because the cap admits it.
        let route = TestRoute::new(fees.receive, fees.send);
        let cap = EvacFeeCap {
            base_msat: Msat(100_000),
            bps: 300,
        };
        let balance = Msat(10_000_000);
        let net = route.size(balance, balance, cap).await.expect_sized();
        assert!(net.0 > 9_000_000, "not refused on ppm alone: {net:?}");
    }

    /// Pass 1's affordability bisection is NOT strictly monotone either: mint fees are per input
    /// and per output, so raising the ask can cross a note-selection boundary that REDUCES the
    /// input/change count and LOWERS the source debit. Here the affordable set is
    /// `[5_000, 110_000] ∪ [131_072, 140_000]`, and the naive bisection collapses onto the gap.
    #[tokio::test]
    async fn pass_one_probes_across_a_note_count_boundary_instead_of_collapsing() {
        let route = TestRoute::new(gw(0, 0), gw(0, 0)).with_send_fed_fee(|outgoing| {
            Msat(if outgoing.0 >= 131_072 {
                20_000
            } else {
                50_000
            })
        });
        let spendable = Msat(160_000);
        let desired = Msat(140_000);

        // RED-FIRST: the strict bisection stops at the bottom interval.
        let naive =
            largest_fitting_amount(MINIMUM_INCOMING_CONTRACT_MSAT, desired.0, 0, |amount| {
                let quoted = route.quote(Msat(amount), spendable);
                async move { Ok(quoted.await?.affordability(spendable)) }
            })
            .await
            .expect("no fault");
        assert_eq!(
            naive,
            Some(110_000),
            "the naive bisection discards the feasible upper interval"
        );

        // The robust one probes the tier boundary above the failing candidate and finds it.
        let net = route
            .size(desired, spendable, PILOT_CAP)
            .await
            .expect_sized();
        assert_eq!(
            net, desired,
            "the whole ask is affordable across the boundary"
        );
    }

    /// A LEGACY evacuation intent — one journaled before the cap components existed — keeps the
    /// ABSOLUTE cap it was admitted under, and never retroactively adopts the current policy's.
    #[test]
    fn a_legacy_evacuation_intent_keeps_its_stored_absolute_cap() {
        let stored = Msat(50_000);
        let cap = evacuation_cap_rule(None, stored);
        assert_eq!(cap.base_msat, stored);
        assert_eq!(
            cap.bps, 0,
            "a zero rate makes the cap constant at the stored value"
        );
        assert_eq!(cap.at(Msat(1_000_000)), stored);
        assert_eq!(
            cap.at(Msat(75_000_000)),
            stored,
            "and it does not grow with the amount"
        );
        assert_ne!(cap.at(Msat(1_000_000)), PILOT_CAP.at(Msat(1_000_000)));

        // A journaled `Action::Evacuate` written before the field existed decodes with no
        // components, so it takes exactly that path.
        let legacy: Action = serde_json::from_value(serde_json::json!({
            "Evacuate": {
                "from": vec![0xAAu8; 32],
                "to": vec![0xBBu8; 32],
                "amount": 75_000_000,
                "fee_cap": 50_000,
            }
        }))
        .expect("a legacy Evacuate row must still decode");
        let Action::Evacuate {
            fee_cap,
            fee_cap_components,
            ..
        } = legacy
        else {
            panic!("expected an Evacuate")
        };
        assert_eq!(fee_cap_components, None);
        assert_eq!(
            evacuation_cap_rule(fee_cap_components, fee_cap).at(Msat(1_000)),
            Msat(50_000)
        );
    }

    /// A fresh evacuation carries its components, so the cap tracks the executed net.
    #[test]
    fn a_planned_evacuation_carries_its_cap_components_into_the_plan() {
        let action = Action::Evacuate {
            from: FED_A,
            to: FED_B,
            amount: Msat(75_000_000),
            fee_cap: PILOT_CAP.at(Msat(75_000_000)),
            gateway: None,
            fee_cap_components: Some(PILOT_CAP),
        };
        let plan = MovePlan::from_action(&action).expect("Evacuate maps to a plan");
        assert_eq!(plan.fee_cap_components, Some(PILOT_CAP));
        assert_eq!(plan.fee_cap, Msat(2_450_000));
        assert_eq!(
            evacuation_cap_rule(plan.fee_cap_components, plan.fee_cap).at(Msat(1_000_000)),
            Msat(230_000)
        );

        // A funding `Move` carries none: its cap does not depend on the executed amount.
        let funding = Action::Move {
            from: FED_A,
            to: FED_B,
            amount: Msat(1_000_000),
            fee_cap: Msat(30_000),
            gateway: None,
        };
        let plan = MovePlan::from_action(&funding).expect("Move maps to a plan");
        assert_eq!(plan.fee_cap_components, None);
        assert_eq!(
            evacuation_cap_rule(plan.fee_cap_components, plan.fee_cap).at(Msat(75_000_000)),
            Msat(30_000),
            "a Move's cap is its planned cap at every amount"
        );
    }

    /// The Pay-step re-check of the viability rule classifies exactly as the cap re-check does,
    /// and applies to evacuations only (its caller gates it) — no third verdict class.
    #[test]
    fn the_pay_step_viability_recheck_mirrors_the_cap_verdict() {
        // Fits: the fee is under what the chunk delivers.
        assert!(evacuation_viability_verdict(Msat(100), Msat(200), Msat(1_000)).is_ok());
        // Exactly break-even still serves (`<=`, the same boundary as the cap).
        assert!(evacuation_viability_verdict(Msat(400), Msat(600), Msat(1_000)).is_ok());
        // A send-side breach a fresh attempt could clear stays Retryable.
        assert!(matches!(
            evacuation_viability_verdict(Msat(400), Msat(601), Msat(1_000)),
            Err(ExecError::Retryable(_))
        ));
        // The FIXED receive quote alone breaking it is terminal — no re-quote can rescue it.
        assert!(matches!(
            evacuation_viability_verdict(Msat(1_001), Msat(0), Msat(1_000)),
            Err(ExecError::Permanent(_))
        ));
    }

    /// The oscillation bound must include the mint's PROPORTIONAL per-note term: a tier-count-only
    /// bound can be smaller than a real jump by orders of magnitude, and the `shortfall <= A`
    /// rules would then refuse an executable evacuation without probing.
    #[test]
    fn the_oscillation_bound_scales_with_the_denominations_at_risk() {
        // Calibration point from the derivation: eleven tiers, no proportional term, gives
        // 2A ≈ 18_000 msat.
        assert_eq!(
            oscillation_bound(Msat(1_024)),
            6 + 2 * (300 * 11 + 100) + 2_100 + 2 * 2
        );
        // A 75_000-sat balance can hold a note whose per-note proportional fee dwarfs the
        // tier-count terms.
        let large = oscillation_bound(Msat(75_000_000));
        assert_eq!(large, 168_506);
        let tier_terms_only = 6 + 2 * (300 * 27 + 100) + 2_100;
        assert_eq!(tier_terms_only, 18_506);
        assert_eq!(
            large - tier_terms_only,
            150_000,
            "the per-note proportional term is EIGHT TIMES the tier-count terms here — dropping \
             it would shrink the bound by ~89% and refuse an executable evacuation unprobed"
        );
    }

    /// The probe schedule is derived from the mint's power-of-two denominations, LARGEST
    /// candidate first, and is bounded.
    #[test]
    fn note_boundaries_follow_the_mint_tiers_largest_first() {
        let above = note_boundaries_above(123_125, 140_000);
        assert_eq!(
            above.first(),
            Some(&131_072),
            "2^17, the largest tier crossing in range"
        );
        assert!(above.len() <= NOTE_BOUNDARY_PROBES);
        assert!(
            above.windows(2).all(|w| w[0] > w[1]),
            "sorted, descending: {above:?}"
        );
        assert!(above.iter().all(|c| *c > 123_125 && *c <= 140_000));

        let below = note_boundaries_below(131_100, MINIMUM_INCOMING_CONTRACT_MSAT);
        assert_eq!(
            below.first(),
            Some(&131_099),
            "nearest below is largest below"
        );
        assert!(
            below.contains(&131_071),
            "and the 2^17 crossing is reached: {below:?}"
        );
        assert!(below.len() <= NOTE_BOUNDARY_PROBES);
        assert!(
            below.windows(2).all(|w| w[0] > w[1]),
            "sorted, descending: {below:?}"
        );

        // A range with no room above yields nothing rather than probing out of bounds.
        assert!(note_boundaries_above(140_000, 140_000).is_empty());
        assert!(note_boundaries_below(5_000, MINIMUM_INCOMING_CONTRACT_MSAT).is_empty());
    }

    /// A minimal fresh-evacuation record, as `assemble_record` would build it before sizing runs.
    fn evacuation_record(amount: Msat, fee_cap: Msat) -> MoveRecord {
        MoveRecord {
            key: wallet_core::IdempotencyKey("evac:test".into()),
            from: Some(FED_A),
            to: FED_B,
            amount,
            fee_cap,
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: true,
            invoice: None,
            recv_op: None,
            send_op: None,
            phase: MovePhase::Created,
            outcome: None,
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
        }
    }
}
