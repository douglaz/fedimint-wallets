//! The operation ledger types (spec §7, implementing `docs/operation-history-spec.md` §2).
//!
//! Pure data + serde + pure transition helpers, golden-tested here. The DURABLE storage,
//! journal integration, standalone `record_*` methods, and reconcile repair live in
//! `wallet-fedimint` (spec §9–§10) and are built in a later run; this module owns only the
//! model and the pure [`advance`] transition function they will call.
//!
//! Authority split (both docs): the history spec is normative for the MODEL — three durable
//! structures, append-once / advance-forward / terminal-immutable write discipline,
//! correlation keys — and this module's shapes are the impl spec §7 refinement of it
//! (`reason` mandatory via [`crate::ReasonCode::UserInitiated`], gateways `Option`).

use crate::executor::IntentStatus;
use crate::types::{
    Action, FederationId, GatewayUrl, IdempotencyKey, Msat, Occurrence, OperationId, ReasonCode,
};

/// One row per user-meaningful operation (history spec §2). Append-only: a row is created
/// once, its status may advance `Started`/`Awaiting` → terminal, and a TERMINAL row is
/// immutable — with the single [`advance`] exception for a defeasible [`Self::repaired`]
/// terminal.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperationRecord {
    /// Monotonic per-wallet sequence (durable counter, incremented in the same dbtx). The
    /// ordering authority — robust to clock skew; wall-clock is display material.
    pub seq: u64,
    /// Joins ledger ↔ journal ↔ `MoveRecord`. For journaled ops this IS the intent's
    /// `IdempotencyKey`; raw/tick ops use per-attempt, nonce-only keys (history spec §2).
    pub correlation_key: IdempotencyKey,
    pub kind: OperationKind,
    /// Who initiated it — the audit discriminator ADR-0014 needs.
    pub actor: Actor,
    /// The real reason (§8 — always present; plain user verbs carry
    /// [`ReasonCode::UserInitiated`]).
    pub reason: ReasonCode,
    pub status: OperationStatus,
    /// Unix millis. `created_at` is first observation; `updated_at` is the last transition
    /// (terminal time). `seq` is authoritative for order; these answer "when".
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub fees: FeeBreakdown,
    /// Terminal failure/refusal detail, verbatim (the executor diagnostic / `MoveRecord`
    /// outcome).
    pub error: Option<String>,
    /// Set when this row's terminal `Failed` came from reconcile's NEGATIVE-inference
    /// repair (§10.3): such a failure is DEFEASIBLE — [`advance`] permits one
    /// evidence-carrying supersession (see the [`advance`] rule).
    pub repaired: bool,
}

/// Who initiated the operation. `Copy` so [`crate::apply`] can stamp it onto every intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Actor {
    User,
    /// A tick/standing-instruction action; `occurrence` identifies the allocation epoch.
    Agent {
        occurrence: Occurrence,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationStatus {
    Started,
    Awaiting,
    Succeeded,
    Failed,
}

impl OperationStatus {
    /// A `Succeeded`/`Failed` row is terminal (immutable, save the [`advance`] repaired
    /// exception).
    pub fn is_terminal(self) -> bool {
        matches!(self, OperationStatus::Succeeded | OperationStatus::Failed)
    }

    /// Forward-progress rank: `Started` → `Awaiting` → terminal. Same rank is enrichment;
    /// a strictly-lower rank is a regression (rejected by [`advance`]). Both terminal
    /// states share the top rank — terminal-immutability, not rank, separates them.
    fn rank(self) -> u8 {
        match self {
            OperationStatus::Started => 0,
            OperationStatus::Awaiting => 1,
            OperationStatus::Succeeded | OperationStatus::Failed => 2,
        }
    }
}

/// Typed, complete per-kind details. Amounts are NET unless stated.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationKind {
    Join {
        fed: FederationId,
    },
    // GROSS invoiced amount — the user's input, known BEFORE any resolution, so the pre-call
    // Started row is complete; the NET credit is amount_invoiced − fees.receive_fee (lnv2 raw
    // receive deducts fees from the invoiced amount, unlike the exact-net DirectInflow).
    Receive {
        fed: FederationId,
        amount_invoiced: Msat,
        op_id: Option<OperationId>,
        gateway: Option<GatewayUrl>,
    },
    // amount+hash None on the pre-parse Started row (§10.1 — a malformed invoice never yields
    // them); filled by the post-parse record_update BEFORE the SDK call — the hash is the
    // durable link that lets repair recover DEDUPED retries (§10.3).
    Pay {
        fed: FederationId,
        invoice_amount: Option<Msat>,
        payment_hash: Option<[u8; 32]>,
        op_id: Option<OperationId>,
        gateway: Option<GatewayUrl>,
    },
    DirectInflow {
        to: FederationId,
        amount: Msat,
        recv_op: Option<OperationId>,
        gateway: Option<GatewayUrl>,
    },
    Move {
        from: FederationId,
        to: FederationId,
        amount: Msat,
        send_op: Option<OperationId>,
        recv_op: Option<OperationId>,
        gateway: Option<GatewayUrl>,
        evacuation: bool,
    },
    Refusal {
        fed: FederationId,
    },
    Tick {
        occurrence: Occurrence,
        decisions: u32,
        performed: u32,
        failed: u32,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FeeBreakdown {
    pub fee_cap: Option<Msat>,
    /// Receive-side cost. EXACT only on a `Succeeded` intent-backed row (the fixed invoice's
    /// cost, realized at claim); a QUOTE otherwise — raw pre-call estimates (§9.3) and
    /// unclaimed/refused/stranded intent rows (§2.3: what it WOULD have cost).
    pub receive_fee: Option<Msat>,
    /// Send-side cost: the pay-time quote, from the `MoveRecord` (§2).
    pub send_fee_quoted: Option<Msat>,
}

/// The enrichment payload the standalone `record_update` (§9.3) also takes — all fields
/// pure, so it is declared here in `wallet-core::ledger`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawOpUpdate {
    pub op_id: Option<OperationId>,
    pub gateway: Option<GatewayUrl>,
    pub invoice_amount: Option<Msat>,
    pub payment_hash: Option<[u8; 32]>,
    pub fees: Option<FeeBreakdown>,
    /// True when `fees` is a SETTLEMENT-derived statement (the §9.3 definitive backfill):
    /// the two cost fields (`receive_fee`/`send_fee_quoted`) then REPLACE the stored values
    /// outright — including replacing a stale pre-call ESTIMATE with `None` when the
    /// settlement could not derive the real fee — instead of merging. A terminal row must
    /// never present an estimate as an observed cost. `fee_cap` always merges (it is the
    /// caller's bound, not a settlement observation), and non-definitive updates keep the
    /// never-wipe-a-known-fee merge.
    pub fees_definitive: bool,
}

/// Whether a ledger write is AUTHORITATIVE (evidence-carrying) or a defeasible REPAIR
/// conclusion (§7/§10.3). The single-supersession exception hinges on this flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteKind {
    /// A normal, evidence-carrying write: the journal-integrated intent transition, an
    /// op-log outcome, a user `await-* --key`. Authority — supersedes a defeasible repaired
    /// terminal exactly once.
    Authoritative,
    /// A reconcile absence-of-evidence conclusion (§10.3). Defeasible: a terminal it writes
    /// carries `repaired: true`, and it NEVER supersedes any terminal row.
    Repair,
}

/// The [`OperationKind`] for an executable [`Action`]. Op ids and gateway start `None`; the
/// §9.2 refresh fills them from the `0x02` move row on every ledger write.
///
/// `Action::RefuseInflow` maps to [`OperationKind::Refusal`] — it is advisory, not an
/// executor intent (§5).
pub fn kind_from_action(action: &Action) -> OperationKind {
    match action {
        Action::Move {
            from, to, amount, ..
        } => OperationKind::Move {
            from: *from,
            to: *to,
            amount: *amount,
            send_op: None,
            recv_op: None,
            gateway: None,
            evacuation: false,
        },
        Action::Evacuate {
            from, to, amount, ..
        } => OperationKind::Move {
            from: *from,
            to: *to,
            amount: *amount,
            send_op: None,
            recv_op: None,
            gateway: None,
            evacuation: true,
        },
        Action::DirectInflow { to, amount, .. } => OperationKind::DirectInflow {
            to: *to,
            amount: *amount,
            recv_op: None,
            gateway: None,
        },
        Action::RefuseInflow { fed, .. } => OperationKind::Refusal { fed: *fed },
    }
}

/// Total mapping from an intent's [`IntentStatus`] to the ledger's [`OperationStatus`]
/// (§7): `Pending`/`Executing` → `Started`, `Awaiting` → `Awaiting`, `Done` → `Succeeded`,
/// `Failed` → `Failed`.
pub fn status_from_intent(status: IntentStatus) -> OperationStatus {
    match status {
        IntentStatus::Pending | IntentStatus::Executing => OperationStatus::Started,
        IntentStatus::Awaiting => OperationStatus::Awaiting,
        IntentStatus::Done => OperationStatus::Succeeded,
        IntentStatus::Failed => OperationStatus::Failed,
    }
}

/// The append-once / advance-forward / terminal-immutable rule as a PURE function.
///
/// Given the STORED `record`, return the row to write (`Some`) or `None` for a no-op write.
/// `seq`/`correlation_key`/`kind` identity, `actor`, `reason`, and `created_at_ms` are
/// preserved; `status`, `updated_at_ms`, the `kind`'s op-ids/gateway/amounts, `fees`,
/// `error`, and `repaired` are updated.
///
/// Returns `None` (no write) ONLY when the stored record is already TERMINAL (save the
/// repaired exception below) or when `new_status` would REGRESS (e.g. `Awaiting → Started`).
/// A NON-terminal row may always be ENRICHED (op-ids/gateway/fees/error filled in) at the
/// SAME status — `record_update`'s normal post-call path — bumping `updated_at_ms`. At the
/// same status the `error` fill is additive (`None` never clobbers a known failure); a
/// forward transition instead sets `error` to the incoming value EXACTLY, so a success
/// (`error: None`) sheds any prior diagnostic rather than dragging it onto the terminal row.
///
/// ONE principled exception: a terminal row written by REPAIR carries `repaired: true`, and
/// exactly ONE [`WriteKind::Authoritative`] write may supersede it (clearing the flag) — a
/// late-returning join call, an `await-send --key` reporting the real outcome, or a
/// journal-integrated status write replaces the repair's absence-of-evidence guess. Because
/// that write REPLACES the guess wholesale, `error` tracks its outcome exactly — a success
/// (`error: None`) clears the stale repair diagnostic rather than merge-preserving it. Repair
/// writes never supersede anything terminal. This is what makes a clock-skewed false repair
/// self-healing instead of permanently blocking the real writer.
pub fn advance(
    record: &OperationRecord,
    new_status: OperationStatus,
    now_ms: u64,
    upd: Option<&RawOpUpdate>,
    error: Option<&str>,
    write: WriteKind,
) -> Option<OperationRecord> {
    if record.status.is_terminal() {
        // Terminal-immutable, save the single repaired-row supersession: only an
        // AUTHORITATIVE write may replace a defeasible repaired terminal, exactly once
        // (the replacement clears `repaired`, so a second authoritative write finds a
        // non-repaired terminal and is rejected here). A REPAIR write never supersedes a
        // terminal (repaired or not).
        if !(record.repaired && write == WriteKind::Authoritative) {
            return None;
        }
    } else if new_status.rank() < record.status.rank() {
        // Regression on a non-terminal row (e.g. Awaiting → Started): no write.
        return None;
    }

    let mut next = record.clone();
    next.status = new_status;
    next.updated_at_ms = now_ms;
    // A terminal row is `repaired` iff a REPAIR write put it there; any authoritative write
    // (including the one superseding a prior repaired terminal) clears it. Non-terminal rows
    // are never `repaired`.
    next.repaired = write == WriteKind::Repair && new_status.is_terminal();
    if let Some(upd) = upd {
        enrich_kind(&mut next.kind, upd);
        if let Some(fees) = upd.fees {
            merge_fees(&mut next.fees, &fees, upd.fees_definitive);
        }
    }
    if record.status.is_terminal() || new_status != record.status {
        // A status CHANGE redefines the outcome, so `error` reflects the new status EXACTLY,
        // never inheriting a prior diagnostic. Two cases reach here:
        //   * the repaired-terminal supersession — an authoritative write REPLACES the
        //     repair's guess wholesale (reaching past the terminal guard means
        //     `record.repaired && Authoritative`);
        //   * a non-terminal forward transition (e.g. `Awaiting → Succeeded`) — a success
        //     (`error: None`) must not drag a stale failure onto the terminal row.
        // Either way `error: None` clears any prior text, honoring the audit-honesty invariant
        // (ADR-0014) that a `Succeeded` row carries no failure diagnostic.
        next.error = error.map(|e| e.to_owned());
    } else if let Some(error) = error {
        // SAME-status enrichment only: additive fill — a `None` never clobbers an
        // already-recorded error (a partial post-call update must not wipe a known failure).
        next.error = Some(error.to_owned());
    }
    Some(next)
}

/// Fill in-flight op-ids/gateway/amounts on the kind from a [`RawOpUpdate`], never clobbering
/// a known value with `None`. `RawOpUpdate` is the single-op raw-verb payload (§9.3), so it
/// fills the single-op kinds; a `Move`'s two op-ids are refreshed from the `MoveRecord` copy
/// (§9.2), not from this update, so only its gateway is touched here.
fn enrich_kind(kind: &mut OperationKind, upd: &RawOpUpdate) {
    match kind {
        OperationKind::Join { .. } | OperationKind::Refusal { .. } | OperationKind::Tick { .. } => {
        }
        OperationKind::Receive { op_id, gateway, .. } => {
            fill(op_id, upd.op_id);
            fill_gateway(gateway, &upd.gateway);
        }
        OperationKind::Pay {
            invoice_amount,
            payment_hash,
            op_id,
            gateway,
            ..
        } => {
            fill(invoice_amount, upd.invoice_amount);
            fill(payment_hash, upd.payment_hash);
            fill(op_id, upd.op_id);
            fill_gateway(gateway, &upd.gateway);
        }
        OperationKind::DirectInflow {
            recv_op, gateway, ..
        } => {
            fill(recv_op, upd.op_id);
            fill_gateway(gateway, &upd.gateway);
        }
        OperationKind::Move { gateway, .. } => {
            fill_gateway(gateway, &upd.gateway);
        }
    }
}

fn fill<T: Copy>(slot: &mut Option<T>, incoming: Option<T>) {
    if incoming.is_some() {
        *slot = incoming;
    }
}

fn fill_gateway(slot: &mut Option<GatewayUrl>, incoming: &Option<GatewayUrl>) {
    if incoming.is_some() {
        *slot = incoming.clone();
    }
}

/// Field-wise fill of a [`FeeBreakdown`] — an incoming `Some` component overwrites, a `None`
/// leaves the existing value (so a partial update never wipes a known fee). EXCEPT when the
/// update is settlement-DEFINITIVE (`RawOpUpdate::fees_definitive`): the two cost fields then
/// replace outright, so a settlement that could not derive the real fee clears a stale
/// pre-call estimate instead of freezing it onto a terminal row. `fee_cap` always merges.
fn merge_fees(into: &mut FeeBreakdown, from: &FeeBreakdown, definitive: bool) {
    fill(&mut into.fee_cap, from.fee_cap);
    if definitive {
        into.receive_fee = from.receive_fee;
        into.send_fee_quoted = from.send_fee_quoted;
    } else {
        fill(&mut into.receive_fee, from.receive_fee);
        fill(&mut into.send_fee_quoted, from.send_fee_quoted);
    }
}
