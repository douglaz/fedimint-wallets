/// A federation's 32-byte identity. Bridges `fedimint_core::config::FederationId`
/// (a `sha256::Hash`); a local `u32` peer/index is meaningless across federations.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct FederationId(pub [u8; 32]);

impl FederationId {
    /// Lowercase hex of the 32 bytes. Used to build stable, human-greppable
    /// idempotency keys without pulling in a `hex` dependency.
    pub fn to_hex(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            // Writing to a `String` is infallible.
            let _ = write!(out, "{byte:02x}");
        }
        out
    }
}

/// A millisatoshi amount (and fees). The arithmetic here is unit-agnostic, so the
/// relabel from the former `Sats` keeps every numeric value as-is (no ×1000 scaling).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Msat(pub u64);

/// A monotonic allocation epoch (T10). Stable while a condition persists, but
/// advances once the underlying intent settles, so recurrence stays live: the same
/// logical decision at two different occurrences produces two different
/// [`IdempotencyKey`]s (see `allocator::decide`), rather than being permanently
/// skipped after the first is marked `Done`.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Occurrence(pub u64);

/// The stable per-intent key: dedupes the same logical intent across evaluation
/// ticks and crashes, while the embedded [`Occurrence`] lets a legitimately
/// recurring decision produce a fresh key once the prior occurrence settles.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct IdempotencyKey(pub String);

/// Structured per-federation balance (T13), at msat granularity. The allocator
/// decides on `spendable`; the other fields exist so the model can later account for
/// fees/caps/retries without another balance-shape rewrite.
///
/// `in_flight`/`claimable`/`reserved_fee` are carried but not yet read by `decide()`
/// (§5.4): a conscious shape-stability trade-off — keeping them here means the later
/// fee/cap/retry accounting does not force another balance-shape rewrite. A fresh probe
/// sets them to zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FedBalance {
    pub spendable: Msat,
    pub in_flight: Msat,
    pub claimable: Msat,
    pub reserved_fee: Msat,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FederationStatus {
    pub id: FederationId,
    pub balance: FedBalance,
    pub probed_ok: bool,
    pub reputation: i32,
    pub shutdown_notice: bool,
    pub healthy: bool,
    /// The scorer's fundability verdict for this fed (§15.3): whether it passed the
    /// structural + probe gate. Snapshot assembly (`build_snapshot`) is the only place
    /// the verdict exists, so probe-only assemblers set it `false`. Gates evacuation
    /// DESTINATIONS (`eligible_for_evacuation`) — the allocator will not drain a dying
    /// fed into a scorer-rejected one (e.g. a joined 1-of-1) just because it is reachable.
    pub eligible_to_fund: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AllocatorSnapshot {
    /// Every probed federation, one status each. Iteration order is SIGNIFICANT and must
    /// be STABLE across ticks: `decide()` walks it in order to emit evacuation/refusal
    /// decisions, so the order feeds decision ordering. (The one place order does NOT
    /// decide the outcome is `safest_other`'s fallback, which picks the smallest
    /// `FederationId` among eligibles rather than the first in this vec — §4.1.)
    pub federations: Vec<FederationStatus>,
    pub spending_fed: Option<FederationId>,
    pub standby_fed: Option<FederationId>,
    pub per_fed_cap: Msat,
    pub target_spending_balance: Msat,
    pub standby_target: Msat,
    /// The ABSOLUTE per-move fee cap. Since br-ljj.2 this bounds ONLY `Evacuate` (a proportional
    /// cap on a small dying-fed remnant would compute below any realistic base fee and refuse
    /// the drain). Funding `Move`s use `max_fee_bps_of_move` instead.
    pub max_fee: Msat,
    /// The PROPORTIONAL fee cap for funding `Move`s, in basis points of the amount moved
    /// (1..=10000; Policy rejects 0). Funding-move sizing reserves `amount + amount*bps/10000` from the source
    /// budget — `amount ≤ budget * 10000/(10000+bps)` — so a positive budget never saturates
    /// `available` to zero, and the stamped `fee_cap` scales with the move. Does NOT bound
    /// `Evacuate` (see `max_fee`).
    pub max_fee_bps_of_move: u16,
    /// The smallest fund/top-up move worth emitting, injected by the I/O layer from the
    /// protocol floor (lnv2 refuses incoming contracts below its 5-sat minimum). A top-up
    /// whose whole SHORTFALL is below this is dust — the destination is effectively at
    /// target, and the move could only fail at perform time, every tick, forever (the 24h
    /// soak logged 91 such doomed sub-minimum moves). Zero disables the floor.
    pub min_move: Msat,
    /// Durable cross-operation reservations projected from the journal. The allocator's
    /// local `credited`/`debited` maps remain the intra-batch layer.
    pub reservations: Reservations,
    pub now: u64,
}

/// A move A→B is a protocol (ADR-0022): B creates an invoice, A pays it via a shared
/// gateway, B claims the contract. `Action` models this split between executable
/// money-moves and advisory policy signals (T12).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Action {
    /// Route the next receive here. The cheap PRIMARY lever: directing an inflow
    /// costs nothing to *move* (no source balance is spent), but the receive
    /// itself still has a fee — the gateway + federation receive-side cost that
    /// grosses up the invoice (spec §6). `amount` is the NET credit the
    /// destination must end up with; `fee_cap` bounds that receive-side cost.
    DirectInflow {
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
    },
    /// Rebalance existing balance from one federation to another.
    Move {
        from: FederationId,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
    },
    /// Move a federation's balance out ahead of a shutdown/health problem. Executed
    /// since Phase 3.A as a send-required move (the same validated two-leg path as
    /// `Move`), LN-only per ADR-0018.
    Evacuate {
        from: FederationId,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
    },
    /// Pay a user-supplied BOLT11 directly from one federation. The payment hash is the
    /// natural user-API idempotency anchor; all sizing fields remain in the intent so an
    /// attach can verify the original reservation bounds.
    Pay {
        from: FederationId,
        invoice: Invoice,
        amount: Msat,
        fee_cap: Msat,
        payment_hash: [u8; 32],
        gateway: Option<GatewayUrl>,
    },
    /// Mint a raw receive invoice on one federation. `nonce` distinguishes deliberate
    /// repeated receives because the request has no natural external anchor.
    Receive {
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        nonce: String,
        gateway: Option<GatewayUrl>,
    },
    /// Join a federation under the invite-derived operation identity.
    Join {
        federation: FederationId,
        invite: String,
        /// Whether membership already existed when this intent was admitted. Recovery uses
        /// this durable fact to distinguish a no-op reopen from a crash after this intent
        /// persisted the federation registry but before it terminalized its ledger row.
        membership_preexisting: bool,
    },
    /// Advisory: do not route the next inflow to `fed` / do not cap allocation here.
    /// Never becomes an executor `Intent` (see `Action::is_executable`); the ledger's
    /// `Refusal` kind records the concept. `diagnostics` carries the balance/threshold
    /// figures that produced the refusal so it stays reconstructible after a restart.
    RefuseInflow {
        fed: FederationId,
        reason: ReasonCode,
        diagnostics: RefusalDiagnostics,
    },
}

/// The balance/threshold figures a `RefuseInflow` was decided from, persisted alongside the
/// refusal so "why didn't the wallet act?" is answerable from the journal row alone, without
/// live tracing (the motivating case: a refusal whose arithmetic could not be reconstructed
/// after the pod that logged it restarted). These are the figures at FIRST observation: a
/// persisting condition re-ticks under the same idempotency key and `record_refusals` keeps
/// the first row (§9.3 append-once), so the figures do not track later ticks.
///
/// Every field is optional because the refusal sites compute different subsets: a
/// `receive_blocker` gate refuses before cap room or the move amount is known, and an
/// evacuation with no safe destination has neither a shortfall nor an amount. `available` is
/// `None` (not `Some(0)`) precisely when there was no usable funding source — the case that
/// distinguishes "the source had nothing to give" from "there was no source at all".
///
/// These are OBSERVATIONAL metadata, not part of the refusal's identity: two refusals of the
/// same federation for the same reason are the same advisory signal regardless of the figures
/// captured at each. `PartialEq`/`Eq` are therefore hand-written to compare equal always, so
/// equality agrees with the idempotency key (`allocator::idem_refuse`), which likewise
/// excludes them — that agreement is the reason. The actor's sizing-conflict recheck
/// (`service::actor`) also compares `RefuseInflow` actions by value, but that arm is
/// unreachable for refusals (they are filtered as non-executable before any attach), so it is
/// defensive here, not load-bearing.
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct RefusalDiagnostics {
    /// The federation that would have SOURCED the move, when there was a usable one. Names the
    /// fed the source-side figures (`available`, `source_spendable`) describe.
    pub source: Option<FederationId>,
    /// The shortfall the decision was trying to fill (`target − spendable`), when it had one.
    /// `None` for an evacuation, which drains its source rather than filling a target.
    pub want: Option<Msat>,
    /// The largest amount fundable from the source: since br-ljj.2, `budget * 10000/(10000+bps)`
    /// where `budget = source_spendable − reservations − (standby path) the spending target` and
    /// `bps = max_fee_bps_of_move`. Proportional, so it never saturates to zero for a positive
    /// budget (the old absolute-cap bug). `None` when there was no usable source at all (as
    /// opposed to `Some(Msat(0))`, a source with no surplus).
    pub available: Option<Msat>,
    /// The source federation's raw spendable balance, the top of the `available` chain.
    pub source_spendable: Option<Msat>,
    /// The ABSOLUTE fee cap. `None` on a FUNDING refusal since br-ljj.2 — funding sizing uses
    /// the proportional `max_fee_bps_of_move` (and `available` already reflects it), so the
    /// absolute cap is not the funding constraint. Also `None` on an evacuation refusal, which
    /// does not pre-reserve it. (A follow-up may record `max_fee_bps_of_move` here for full
    /// funding-refusal reconstructibility.)
    pub max_fee: Option<Msat>,
    /// The destination's remaining per-fed cap room, once it had been computed.
    pub cap_room: Option<Msat>,
    /// The move amount the allocator settled on before refusing the remainder.
    pub amount: Option<Msat>,
    /// The protocol move floor (`min_move`) in effect, below which a move is dust.
    pub min_move: Option<Msat>,
}

impl RefusalDiagnostics {
    /// Whether any figure was recorded. Used to prefer a populated refusal over an empty
    /// same-key one when the allocator dedups (`allocator::push_decision`) and to omit the
    /// wire object for a figure-less refusal. Destructured so a field added later must be
    /// added here too (or the compiler complains) — otherwise it would be silently dropped
    /// from both the dedup preference and the daemon projection.
    pub fn is_populated(&self) -> bool {
        let Self {
            source,
            want,
            available,
            source_spendable,
            max_fee,
            cap_room,
            amount,
            min_move,
        } = self;
        source.is_some()
            || want.is_some()
            || available.is_some()
            || source_spendable.is_some()
            || max_fee.is_some()
            || cap_room.is_some()
            || amount.is_some()
            || min_move.is_some()
    }
}

impl PartialEq for RefusalDiagnostics {
    /// Always equal: the figures are observational metadata, so refusal identity (and hence
    /// equality) is `fed` + `reason`, matching the idempotency key. See the type doc.
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for RefusalDiagnostics {}

impl Action {
    /// Whether `apply()` should create an executor `Intent` for this action.
    /// `RefuseInflow` is a policy signal (recorded/surfaced only), not work.
    pub fn is_executable(&self) -> bool {
        matches!(
            self,
            Action::DirectInflow { .. }
                | Action::Move { .. }
                | Action::Evacuate { .. }
                | Action::Pay { .. }
                | Action::Receive { .. }
                | Action::Join { .. }
        )
    }

    /// The fee budget authoritative for this action, if it has one.
    /// `Move`/`Evacuate` carry a `fee_cap` bounding the total move cost;
    /// `DirectInflow` carries one bounding its receive-side gross-up (spec §6), and
    /// raw pay/receive intents retain their user-supplied sizing bound. `Join` and
    /// advisory actions have no fee budget.
    pub fn fee_cap(&self) -> Option<Msat> {
        match self {
            Action::Move { fee_cap, .. }
            | Action::Evacuate { fee_cap, .. }
            | Action::DirectInflow { fee_cap, .. }
            | Action::Pay { fee_cap, .. }
            | Action::Receive { fee_cap, .. } => Some(*fee_cap),
            Action::Join { .. } | Action::RefuseInflow { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReasonCode {
    SpendingBelowTarget,
    StandbyBelowTarget,
    ShutdownNotice,
    Unhealthy,
    OverCap,
    NotProbed,
    LowReputation,
    /// A plain user verb (`direct-inflow`/`move`): the operator initiated it directly, so
    /// there is no allocator reason. Mandatory-but-honest (§8) — the ledger's `reason` is
    /// always present.
    UserInitiated,
    /// An active-probe row (phase 5 §5.0.5): the umbrella `probe:` row and both probe leg
    /// moves carry this, so `history` explains every probe as one audited operation family
    /// (reason tag `"active_probe"`).
    ActiveProbe,
    /// A `Tick` ledger row: the run exists because the standing instruction executed. The
    /// run's individual decisions carry their OWN reasons on their own rows — a tick has no
    /// single allocator reason.
    StandingInstruction,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AllocatorDecision {
    pub action: Action,
    pub reason: ReasonCode,
    /// The epoch stamped into `idempotency_key` (T10): see `allocator::decide`.
    pub occurrence: Occurrence,
    pub idempotency_key: IdempotencyKey,
}

// --- Identity newtypes (spec §6) ---
//
// Pure data wrappers with serde derives and no fedimint SDK dependency. They live in
// `wallet-core` because the ledger types ([`crate::ledger`]) reference `OperationId`/
// `GatewayUrl` and must be pure + golden-testable here; `wallet-fedimint` re-exports them
// (its `types.rs`) so its public API is unchanged. Each doc line records how the value
// parses into its fedimint counterpart in `wallet-fedimint`, so the intent stays
// unambiguous without pulling the SDK into `wallet-core`.

/// A fedimint operation's 32-byte identity. Bridges `fedimint_core::core::OperationId`. The
/// deterministic op-id is the client's own send-dedup anchor, so it is the durable handle
/// recorded on a `MoveRecord` (in `wallet-fedimint`).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct OperationId(pub [u8; 32]);

/// A Lightning payment preimage (32 bytes) — proof a send leg settled.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Preimage(pub [u8; 32]);

/// A gateway endpoint URL. Parses to a fedimint `SafeUrl` via `SafeUrl::parse(&self.0)` in
/// `wallet-fedimint`. Pinned on the durable `MoveRecord` so a resumed move never reselects a
/// different gateway after a crash (P2-7: it lives on the record, NOT the intent).
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct GatewayUrl(pub String);

/// A BOLT11 invoice string. Parses to a `Bolt11Invoice` via `FromStr` in `wallet-fedimint`.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Invoice(pub String);

/// Where a durable move currently sits in its lifecycle. The type lives in core because
/// reservation projection is pure and must not depend on the fedimint adapter crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MovePhase {
    Created,
    Invoiced,
    Sending,
    Settled,
    Refunded,
    Failed,
    Stranded,
}

/// Durable derived artifacts for a move-shaped intent. Network code owns the writes; core
/// consumes only the phase and sizing fields when projecting reservations.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MoveRecord {
    pub key: IdempotencyKey,
    pub from: Option<FederationId>,
    pub to: FederationId,
    pub amount: Msat,
    pub fee_cap: Msat,
    pub gateway: GatewayUrl,
    pub send_required: bool,
    pub invoice: Option<Invoice>,
    pub recv_op: Option<OperationId>,
    pub send_op: Option<OperationId>,
    pub phase: MovePhase,
    pub outcome: Option<String>,
    pub preimage: Option<Preimage>,
    pub receive_fee_quoted: Option<Msat>,
    pub send_fee_quoted: Option<Msat>,
}

/// Cross-operation reservations that have not yet been absorbed by live balances.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reservations {
    pub per_fed_outbound: std::collections::BTreeMap<FederationId, Msat>,
    pub per_fed_inbound: std::collections::BTreeMap<FederationId, Msat>,
}

impl Reservations {
    pub fn outbound(&self, fed: FederationId) -> Msat {
        self.per_fed_outbound.get(&fed).copied().unwrap_or(Msat(0))
    }

    pub fn inbound(&self, fed: FederationId) -> Msat {
        self.per_fed_inbound.get(&fed).copied().unwrap_or(Msat(0))
    }
}
