//! The PURE, deterministic core of a cross-federation ecash move (spec §3.3, §4, §5).
//!
//! This module is **pure Rust**: no fedimint SDK, no async, no I/O, no networking, no
//! floats. It is the golden-testable heart of the money path. The fedimint-SDK pieces —
//! `MultiClient`, the journal, the executor — live in LATER steps and call into the two
//! pure functions here: [`next_step`] (what side effect a move needs next) and
//! [`assemble_move_record`] (rebuild the derived record from its durable sources).

use crate::types::{GatewayUrl, Invoice, OperationId};
use wallet_core::{Action, FederationId, IdempotencyKey, Msat};

pub use wallet_core::{MovePhase, MoveRecord};

/// Where a move currently sits in its lifecycle (spec §3.3).
///
/// `Created`/`Invoiced`/`Sending` are derivable from which op-ids/invoice are known.
/// The terminal phases — `Settled`/`Refunded`/`Failed`/`Stranded` — encode the SETTLEMENT
/// outcome, which is learned by awaiting the operations, not from the presence of op-ids;
/// they are therefore preserved across re-assembly (§5).
///
/// `Stranded` (spec §3, settled decision 3) is the misbehaving-gateway terminal: A's send
/// SETTLED (we hold the preimage) but B's receive was NOT credited (expired/failed op). It is
/// terminal like `Refunded`/`Failed` — an op-log-terminal receive cannot be fixed by
/// re-driving — but DISTINCT so the ledger/UI can say "debited, not credited — payment proof
/// saved" rather than a silent loss; the preimage on the [`MoveRecord`] is the durable
/// recovery artifact.
/// The next side effect a move needs, computed purely from a [`MoveRecord`] (spec §3.3).
/// RESUME, not restart: once a step's artifact is recorded, that step is never re-issued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MoveStep {
    CreateInvoice,
    Pay,
    AwaitSettle,
    Done,
    Failed,
}

/// A DERIVED index over a move (spec §3.3) — NOT the source of truth (that is the
/// fedimint op-log, §5). The PARAMS (from/to/amount/fee_cap/gateway/send_required) come
/// from the durable Intent; the op-ids + invoice come from the op-log artifacts. It is
/// rebuilt by [`assemble_move_record`] and consumed by [`next_step`].
/// Which leg of a move an op-log artifact belongs to (spec §4). A cross-fed move spans
/// two operations: a `Receive` on the destination (B) and a `Send` on the source (A).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leg {
    Send,
    Receive,
}

/// One op-log entry's contribution to a move, recovered from `custom_meta` (spec §4).
/// Backfill returns these per-op, NOT full [`MoveRecord`]s: a single client's op-log
/// only ever sees ONE leg, and the move's params live in the journaled Intent, not the
/// op meta.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpArtifact {
    pub move_id: IdempotencyKey,
    pub leg: Leg,
    pub op_id: OperationId,
    /// The net move amount committed in the op's [`MoveMeta`]. This is the crash-safe
    /// recovery source for a fresh evacuation that was sized down before minting its invoice:
    /// if the journal cache is lost, the recovered receive op still tells the Pay-step cap
    /// check which net amount the fixed invoice was intended to deliver.
    pub amount: Msat,
    /// The `Receive` leg carries the invoice; the `Send` leg leaves this `None`.
    pub invoice: Option<Invoice>,
}

/// The move's parameters, sourced by the caller from the durable Intent (the future
/// executor maps an `Action` → `MoveParams`, keeping `move_protocol` decoupled from the
/// `Action` enum). AUTHORITATIVE for from/to/amount/fee_cap/gateway/send_required.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveParams {
    pub key: IdempotencyKey,
    pub from: Option<FederationId>,
    pub to: FederationId,
    pub amount: Msat,
    pub fee_cap: Msat,
    pub gateway: GatewayUrl,
    pub send_required: bool,
}

/// The gateway-free, pure projection of an executable [`Action`] into the parameters a
/// move needs (spec §7). It carries ONLY what is derivable from the `Action` itself; the
/// gateway is resolved by the executor at run time (from `mc.gateways`, then pinned in the
/// durable [`MoveRecord`]), and the idempotency key comes from the `Intent`, so neither
/// lives here — keeping [`MovePlan::from_action`] a pure, gateway-free mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MovePlan {
    /// Source federation: `Some` for a `Move` (a send leg is required), `None` for a
    /// `DirectInflow` (receive-only).
    pub from: Option<FederationId>,
    pub to: FederationId,
    /// The NET credit the destination must end up with (spec §6).
    pub amount: Msat,
    pub fee_cap: Msat,
    /// `Move` = true (pay a send leg), `DirectInflow` = false (receive-only). Always agrees
    /// with `from.is_some()`.
    pub send_required: bool,
}

impl MovePlan {
    /// Map an [`Action`] to a [`MovePlan`], or `None` for anything the executor does not
    /// perform as a move (spec §3.1/§7):
    ///
    /// - `Move` → `Some` with `from = Some`, `send_required = true`.
    /// - `Evacuate` → `Some` with `from = Some`, `send_required = true` — the SAME send-
    ///   required shape as `Move` (spec §7 / ADR-0018): `from` (the dying fed) pays an
    ///   invoice minted on `to`, so it reuses the identical validated two-leg + idempotent-
    ///   replay + gross-up path. LN-only: v1 validates that the destination-selected gateway
    ///   also serves the source, giving the same internal-swap route as `Move`. No peg-out.
    /// - `DirectInflow` → `Some` with `from = None`, `send_required = false` (receive-only).
    /// - Raw pay/receive/join and `RefuseInflow` → `None`; their lifecycles do not use a
    ///   two-leg [`MovePlan`].
    pub fn from_action(a: &Action) -> Option<MovePlan> {
        match a {
            // A `Move` and an `Evacuate` are the same executable send-required move (drain
            // `from` into `to`); they differ only in the reason/idempotency scheme the pure
            // allocator stamps, which lives on the `Action`/`Intent`, not the plan.
            Action::Move {
                from,
                to,
                amount,
                fee_cap,
            }
            | Action::Evacuate {
                from,
                to,
                amount,
                fee_cap,
            } => Some(MovePlan {
                from: Some(*from),
                to: *to,
                amount: *amount,
                fee_cap: *fee_cap,
                send_required: true,
            }),
            Action::DirectInflow {
                to,
                amount,
                fee_cap,
            } => Some(MovePlan {
                from: None,
                to: *to,
                amount: *amount,
                fee_cap: *fee_cap,
                send_required: false,
            }),
            // These actions do not use the two-leg move protocol. Raw operations have their
            // own driver steps; advisory policy signals never execute.
            Action::Pay { .. }
            | Action::Receive { .. }
            | Action::Join { .. }
            | Action::RefuseInflow { .. } => None,
        }
    }
}

/// Which leg of a move an operation's `custom_meta` tags (spec §4). Serialized as
/// `"send"`/`"receive"` inside [`MoveMeta`]. This mirrors [`Leg`], but is the durable,
/// serde-tagged form embedded in the fedimint op meta (whereas [`Leg`] is the in-memory
/// backfill artifact); the two are kept separate so the on-the-wire vocabulary is explicit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MoveRole {
    Send,
    Receive,
}

/// The move-coordination metadata embedded in EVERY receive/send operation's `custom_meta`
/// (spec §4/§5). Fedimint commits it atomically with the operation, so it is how a lost
/// [`MoveRecord`] is repaired on backfill: the op-log is the source of truth, this ties an
/// op-id back to its `move_id` and leg.
///
/// Pure serde over `wallet_core` types (no fedimint SDK); the executor builds one when it
/// calls `receive`/`send`, and `MultiClient::backfill_ops` decodes it back. The move's
/// occurrence (spec §4) is NOT a separate field: it is already embedded in `move_id` (the
/// allocator stamps it into the idempotency key), and backfill keys purely on `move_id`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MoveMeta {
    /// `== MoveRecord.key == Intent key` — the join key across both legs (embeds occurrence).
    pub move_id: IdempotencyKey,
    pub role: MoveRole,
    /// The net amount the destination should receive. For a fresh evacuation this may be lower
    /// than the allocator's desired amount after the executor sizes it down to reserve fees; it
    /// is committed with the op so full journal-loss recovery keeps the fixed invoice's fee-cap
    /// accounting honest.
    pub amount: Msat,
    /// The move's source federation (`None` for a `DirectInflow`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<FederationId>,
    pub to: FederationId,
}

/// Extra `custom_meta` key committed on RECEIVE ops for the §15.7 crash-resume TOCTOU guard.
///
/// It is intentionally outside [`MoveMeta`]'s core shape: send ops do not carry it, and
/// serde ignores the key when backfill decodes the ordinary move metadata. The value is the
/// exact contract amount the quote solver expected before minting; because it is committed
/// atomically with the receive op, a resume after `receive` committed can still compare the
/// durable contract against the quote before surfacing or paying the invoice.
pub const RECEIVE_CONTRACT_QUOTED_META_KEY: &str = "receive_contract_quoted";

impl MoveMeta {
    /// Serialize to the `serde_json::Value` that lnv2 `receive`/`send` commit as
    /// `custom_meta`. Infallible in practice (all fields are plain serde types).
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("MoveMeta is always serializable")
    }

    /// Recover a [`MoveMeta`] from an operation's `custom_meta`, or `None` when the value is
    /// not a move meta (a bare receive/pay tagged only with a `role`, say). Backfill treats
    /// a value that DOES look like a move (`move_id` present) but fails to decode as
    /// malformed and warns; that discrimination lives at the `backfill_ops` call site.
    pub fn from_value(value: &serde_json::Value) -> Option<MoveMeta> {
        serde_json::from_value(value.clone()).ok()
    }

    /// Serialize receive-leg metadata with the quoted contract amount needed by the §15.7
    /// committed-contract check. Backfill still decodes the ordinary [`MoveMeta`] fields from this
    /// value because unknown JSON keys are ignored.
    pub fn receive_value_with_contract_quote(&self, quoted_contract: Msat) -> serde_json::Value {
        let mut value = self.to_value();
        let object = value
            .as_object_mut()
            .expect("MoveMeta serializes to a JSON object");
        object.insert(
            RECEIVE_CONTRACT_QUOTED_META_KEY.to_string(),
            serde_json::to_value(quoted_contract).expect("Msat is serializable"),
        );
        value
    }

    /// Extract the quoted receive contract amount from `custom_meta`. Missing means the op was
    /// not created by the §15.7-aware receive path; malformed means the op metadata is corrupt.
    pub fn receive_contract_quote_from_value(
        value: &serde_json::Value,
    ) -> Result<Option<Msat>, serde_json::Error> {
        value
            .get(RECEIVE_CONTRACT_QUOTED_META_KEY)
            .map(|quoted| serde_json::from_value(quoted.clone()))
            .transpose()
    }
}

/// The next step for a move, computed purely (spec §3.3):
///
/// - `invoice` and `send_op` are both `None` ⇒ `CreateInvoice`.
/// - else `send_required` and `send_op` is `None` ⇒ `Pay`.
/// - else still in flight ⇒ `AwaitSettle`.
/// - else terminal: `Settled` ⇒ `Done`; `Refunded`/`Failed` ⇒ `Failed`.
///
/// Load-bearing RESUME invariants (the no-double-act guarantee):
/// - `invoice.is_some()` ⇒ never `CreateInvoice` (no double-invoice).
/// - `send_op.is_some()` ⇒ never `CreateInvoice` or `Pay` (no double-invoice/pay).
/// - `send_required == false` (a `DirectInflow`) ⇒ never `Pay`; it goes
///   `CreateInvoice → AwaitSettle → Done`.
///
/// Terminal phases are decided FIRST so a `Failed`/`Refunded` move is never told to
/// restart a step — e.g. a creation failure that left no invoice must resolve to
/// `Failed`, not loop back to `CreateInvoice`.
pub fn next_step(rec: &MoveRecord) -> MoveStep {
    // Intent invariant (spec §3.1/§3.3): the two action shapes are `Move`
    // (`from = Some`, `send_required = true`) and `DirectInflow` (`from = None`,
    // `send_required = false`) — so `send_required` and `from.is_some()` always agree.
    // A contradictory record (`from = None` + `send_required = true`) would route to
    // `Pay` with no source federation to pay from; catch it at the decision point.
    debug_assert_eq!(
        rec.send_required,
        rec.from.is_some(),
        "send_required must match from.is_some() (Move => Some / DirectInflow => None)"
    );
    match rec.phase {
        MovePhase::Settled => return MoveStep::Done,
        // `Stranded` (spec §3) shares the terminal `Failed` surface: `perform` returns
        // `Permanent(outcome)`, naming the debited-not-credited state with its saved preimage.
        MovePhase::Refunded | MovePhase::Failed | MovePhase::Stranded => return MoveStep::Failed,
        MovePhase::Created | MovePhase::Invoiced | MovePhase::Sending => {}
    }
    if rec.invoice.is_none() && rec.send_op.is_none() {
        return MoveStep::CreateInvoice;
    }
    if rec.send_required && rec.send_op.is_none() {
        return MoveStep::Pay;
    }
    MoveStep::AwaitSettle
}

/// Assemble a [`MoveRecord`] by merging three sources (spec §5), newest-known wins:
///
/// 1. `params` — AUTHORITATIVE for the move's parameters (from/to/**fee_cap**/gateway/
///    send_required). These come from the durable Intent and are NEVER dropped.
/// 2. `artifacts` — the op-log entries for this `move_id`: a `Receive` leg fills
///    `recv_op` (and `invoice`), a `Send` leg fills `send_op`, and either leg can recover
///    the move's net amount from committed `MoveMeta`. Authoritative for op-ids.
/// 3. `cached` — the previously-known `MoveRecord`, the fallback for any leg an artifact
///    does not (re)supply. For `amount` the preference is: RECOVERED OP METADATA first
///    (a committed op's `MoveMeta` amount is definitive — the invoice was fixed at that
///    sizing; the executor commits the hair-under-adjusted net there precisely so a lost
///    cache write cannot resurrect a stale higher ask and weaken the Pay-step fee-cap
///    re-check), then the CACHED amount (the executor sizes a fresh evacuation DOWN and
///    persists that decision before the non-idempotent receive — no artifact exists yet
///    in that window), then `params.amount`. Rebuilding from `params` alone would
///    silently revert the sizing, and the §7 Pay-step cap re-check derives the receive
///    fee as `invoice_amount − amount`. For an exact-net `Move`/`DirectInflow` all three
///    agree.
///
/// The merge **never blanks an existing leg**: a missing artifact cannot erase an op-id
/// already known from `cached` (a one-client backfill only sees one leg), and `fee_cap`
/// is always taken from `params` so it is never dropped. `phase` is re-derived from
/// which fields are set, EXCEPT that a terminal phase already recorded in `cached` (the
/// settlement outcome, which op artifacts do not carry) is preserved.
///
/// **Caller contract — `artifacts` MUST be newest-first per leg.** Backfill produces them
/// via `paginate_operations_rev` (reverse op-log order), and for each leg the FIRST match
/// in the slice wins; a later (older) duplicate is ignored. `OpArtifact` carries no
/// ordering key, so the merge trusts this slice order — a misordered slice would silently
/// record a stale `recv_op`/`send_op`. The ordering is the producer's responsibility
/// (enforced at the `backfill_ops` source in a later step), not re-checkable here.
pub fn assemble_move_record(
    params: MoveParams,
    artifacts: &[OpArtifact],
    cached: Option<MoveRecord>,
) -> MoveRecord {
    // Intent invariant (spec §3.1/§3.3): `Move` => `from = Some` + `send_required = true`;
    // `DirectInflow` => `from = None` + `send_required = false`. The two fields are never
    // independent in a well-formed intent; catch a contradictory one at construction.
    debug_assert_eq!(
        params.send_required,
        params.from.is_some(),
        "send_required must match from.is_some() (Move => Some / DirectInflow => None)"
    );

    let cached = cached.filter(|cached| {
        debug_assert_eq!(
            cached.key, params.key,
            "cached MoveRecord key must match MoveParams key"
        );
        cached.key == params.key
    });

    // Backfill pages op-log entries newest-first. Take the first matching artifact for
    // each leg, then fall back to the cache for legs the current backfill did not see.
    let mut artifact_invoice = None;
    let mut artifact_recv_op = None;
    let mut artifact_send_op = None;

    // Cache fallback means a missing artifact never blanks an existing leg (a missing
    // leg here means "this client's op-log didn't see it", not "it doesn't exist").
    let cached_amount = cached.as_ref().map(|c| c.amount);
    let cached_invoice = cached.as_ref().and_then(|c| c.invoice.clone());
    let cached_recv_op = cached.as_ref().and_then(|c| c.recv_op);
    let cached_send_op = cached.as_ref().and_then(|c| c.send_op);
    let cached_phase = cached.as_ref().map(|c| c.phase);
    let cached_outcome = cached.as_ref().and_then(|c| c.outcome.clone());
    // The preimage and the fee quotes are executor-persisted facts that op artifacts do NOT
    // carry (the op-log has no notion of our fee_cap accounting or the settled preimage), so
    // like `outcome` they survive re-assembly only via the cache (spec §2.3/§3).
    let cached_preimage = cached.as_ref().and_then(|c| c.preimage);
    let cached_receive_fee_quoted = cached.as_ref().and_then(|c| c.receive_fee_quoted);
    let cached_send_fee_quoted = cached.as_ref().and_then(|c| c.send_fee_quoted);
    let mut artifact_amount = None;

    for artifact in artifacts.iter().filter(|a| a.move_id == params.key) {
        if artifact_amount.is_none() {
            artifact_amount = Some(artifact.amount);
        }
        match artifact.leg {
            Leg::Receive => {
                if artifact_recv_op.is_some() {
                    continue;
                }
                // Spec §4: a `Receive` op-log artifact ALWAYS carries its invoice (backfill
                // recovers it from lnv2 op meta). A `recv_op` recorded WITHOUT its invoice is
                // a contradictory half-state: `next_step` would see no invoice and re-issue
                // `CreateInvoice`, orphaning/duplicating the already-live receive operation.
                debug_assert!(
                    artifact.invoice.is_some(),
                    "a Leg::Receive OpArtifact must carry its invoice (spec §4)"
                );
                // Behind that debug contract, enforce the invariant in RELEASE too by keeping
                // `recv_op` and `invoice` ATOMIC: refuse an invoice-less Receive artifact
                // entirely rather than record `recv_op` without `invoice`. A cached receive
                // leg (if any) then still survives via `.or(cached_*)` below; the dangerous
                // `recv_op = Some, invoice = None` state is simply never representable here.
                let Some(invoice) = artifact.invoice.clone() else {
                    continue;
                };
                artifact_recv_op = Some(artifact.op_id);
                artifact_invoice = Some(invoice);
            }
            Leg::Send => {
                if artifact_send_op.is_none() {
                    artifact_send_op = Some(artifact.op_id);
                }
            }
        }
    }

    // `.or(cached_*)` is the no-blank guarantee: when this backfill pass saw no artifact for
    // a leg (`artifact_* == None`), the cached op-id/invoice carries through untouched.
    let invoice = artifact_invoice.or(cached_invoice);
    let recv_op = artifact_recv_op.or(cached_recv_op);
    let send_op = artifact_send_op.or(cached_send_op);
    let phase = derive_phase(cached_phase, invoice.is_some(), send_op.is_some());

    MoveRecord {
        key: params.key,
        from: params.from,
        to: params.to,
        // The cached amount is the executor's persisted sizing decision (see the doc
        // contract above). If the cache is gone, recover the committed op metadata amount
        // before falling back to the intent's original amount.
        amount: artifact_amount.or(cached_amount).unwrap_or(params.amount),
        fee_cap: params.fee_cap,
        gateway: params.gateway,
        send_required: params.send_required,
        invoice,
        recv_op,
        send_op,
        phase,
        outcome: cached_outcome,
        preimage: cached_preimage,
        receive_fee_quoted: cached_receive_fee_quoted,
        send_fee_quoted: cached_send_fee_quoted,
    }
}

/// Re-derive a move's [`MovePhase`] from which fields are set (spec §5). A terminal
/// phase already recorded in the cache (the settlement OUTCOME, which op artifacts do
/// not carry) is preserved: re-deriving from op-ids alone must not un-settle a finished
/// move.
fn derive_phase(cached: Option<MovePhase>, has_invoice: bool, has_send_op: bool) -> MovePhase {
    if let Some(
        phase
        @ (MovePhase::Settled | MovePhase::Refunded | MovePhase::Failed | MovePhase::Stranded),
    ) = cached
    {
        return phase;
    }
    match (has_invoice, has_send_op) {
        (_, true) => MovePhase::Sending,
        (true, false) => MovePhase::Invoiced,
        (false, false) => MovePhase::Created,
    }
}
