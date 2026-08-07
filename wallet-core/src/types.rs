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
    /// The ABSOLUTE per-move fee cap. `decide()` currently copies it to emitted `Evacuate`
    /// actions; funding `Move`s use `max_fee_bps_of_move`. A bare proportional cap on a small
    /// dying-fed remnant could fall below any realistic base fee and refuse the drain, which is
    /// why its replacement keeps a base term. The evacuation knobs below are not read by any
    /// enforcement path yet. Once `br-evac-cap-enforce-vn6` moves `Evacuate` onto them, this field
    /// will bound no emitted action.
    pub max_fee: Msat,
    /// The PROPORTIONAL fee cap for funding `Move`s, in basis points of the amount moved
    /// (1..=10000; Policy rejects 0). Funding-move sizing reserves `amount + amount*bps/10000`
    /// from the source budget by the EXACT integer inverse (`allocator::max_fundable`) — the
    /// largest `amount` with `amount + floor(amount*bps/10000) ≤ budget`, NOT the naive
    /// `floor(budget*10000/(10000+bps))`, which undershoots by 1 msat and can spuriously refuse
    /// a viable move at the `min_move` floor. So an absolute cap larger than the surplus no
    /// longer cliffs `available` to zero (the saturation bug); a sub-unit budget still floors to
    /// 0. The stamped `fee_cap` scales with the move. Does NOT bound `Evacuate` (see `max_fee`).
    pub max_fee_bps_of_move: u16,
    /// BASE component of the evacuation fee cap, in millisatoshis.
    pub evac_fee_base_msat: Msat,
    /// PROPORTIONAL component of the evacuation fee cap, in basis points of the amount evacuated
    /// (0..=10000).
    pub evac_fee_bps: u16,
    /// The smallest fund/top-up move worth emitting, injected by the I/O layer from the
    /// protocol floor (lnv2 refuses incoming contracts below its 5-sat minimum). A top-up
    /// whose whole SHORTFALL is below this is dust — the destination is effectively at
    /// target, and the move could only fail at perform time, every tick, forever (the 24h
    /// soak logged 91 such doomed sub-minimum moves). Zero disables the floor.
    pub min_move: Msat,
    /// Per-ORDERED-PAIR `(from, to)` route economics, supplied by the I/O layer so `decide()`
    /// stays PURE (`docs/archive/route-economics-decisions.md`). Where `min_move` is one protocol
    /// constant for every pair, this is what it actually costs to route a funding move through
    /// the cheapest gateway serving BOTH ends of THAT pair — so a pair whose fees can never fit
    /// the proportional cap is not funded at all, and a pair that needs a bigger move to amortise
    /// its fees gets a bigger floor.
    ///
    /// A pair is ABSENT when the I/O layer could not price it this tick (first tick, a quote RPC
    /// failed, the per-tick quote budget ran out). Absence is TRANSIENT and therefore PERMISSIVE:
    /// `decide()` falls back to the bare `min_move` floor (§Q5). It is never cached across
    /// ticks — a stale-low floor would under-block, and under-blocking churns forever.
    pub route_economics_by_pair:
        std::collections::BTreeMap<(FederationId, FederationId), RouteEconomics>,
    /// Durable cross-operation reservations projected from the journal. The allocator's
    /// local `credited`/`debited` maps remain the intra-batch layer.
    pub reservations: Reservations,
    pub now: u64,
}

/// What it costs to route a funding move across ONE ordered federation pair this tick, and
/// whether it can be routed at all. Computed by the I/O layer from live fee quotes and read by
/// `allocator::decide` (see [`AllocatorSnapshot::route_economics_by_pair`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteEconomics {
    /// The CHEAPEST gateway serving BOTH ends of the pair (the pinned gateway when the operator
    /// set one — a pin overrides route selection entirely). `None` when nothing serves the pair.
    /// Stamped onto the emitted `Action::Move`/`Evacuate` as the perform-time route HINT.
    pub resolved_gateway: Option<GatewayUrl>,
    /// The smallest NET amount whose modelled move cost still fits the PROPORTIONAL per-move fee
    /// cap. An explicit UPPER bound (every fee component is rounded UP), so it over-blocks rather
    /// than under-blocks: a deferred top-up's shortfall keeps growing until it clears the floor,
    /// whereas under-blocking would emit a move that fails the cap at perform time every tick,
    /// forever. Meaningful only for [`RouteStatus::Routable`].
    pub min_viable_amount: Msat,
    pub status: RouteStatus,
}

/// Whether an ordered pair can carry a funding move, and if not, why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteStatus {
    /// Some gateway serves both ends and the modelled cost fits the cap at or above
    /// `min_viable_amount`.
    Routable,
    /// No gateway serves BOTH ends of the pair this tick — nothing can be routed at any size.
    /// Transient by nature (a gateway may come back), so it is not surfaced as its own refusal;
    /// the shortfall refusal already records that the destination stayed underfunded.
    Unroutable,
    /// A gateway serves both ends, but a conservative lower-bound proof shows that no positive
    /// move size can fit `max_fee_bps_of_move`. Ambiguous adverse-slope cases remain missing
    /// rather than receiving this permanent classification. This status is surfaced as its own
    /// [`ReasonCode::UneconomicRoute`] refusal.
    UneconomicAtAnySize,
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
        /// The route `decide()` priced this move against — the pair's
        /// [`RouteEconomics::resolved_gateway`]. A HINT, not a constraint: `perform` uses it
        /// only while it still serves both ends, and otherwise re-resolves under the SAME
        /// `fee_cap` (the cap, not gateway identity, is the money backstop). `None` when the
        /// pair was not priced this tick, which is exactly the pre-route-economics behavior
        /// (resolve at perform time).
        ///
        /// `Action` is serde-persisted inside the durable `Intent`, so this MUST stay
        /// `#[serde(default)]`: rows written before the field existed carry no key.
        #[serde(default)]
        gateway: Option<GatewayUrl>,
    },
    /// Move a federation's balance out ahead of a shutdown/health problem. Executed
    /// since Phase 3.A as a send-required move (the same validated two-leg path as
    /// `Move`), LN-only per ADR-0018.
    Evacuate {
        from: FederationId,
        to: FederationId,
        amount: Msat,
        fee_cap: Msat,
        /// The preselected route, exactly as on `Move` (and `#[serde(default)]` for the same
        /// durable-format reason). An evacuation is never BLOCKED by route economics — draining
        /// a dying federation must not be gated on a fee model — so this is only ever a cheaper
        /// starting point, `None` whenever the pair was not priced.
        #[serde(default)]
        gateway: Option<GatewayUrl>,
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
    /// Rebuild a federation's funded balance from the seed via `ClientPreview::recover`
    /// (`docs/archive/wallet-recovery-spec.md`). A DELIBERATE, user-initiated last resort — the auto-join
    /// and driver-retry paths keep calling [`Action::Join`], and `decide()` must NEVER emit this
    /// (recovery recovers into a FRESH client partition, invisible to the allocator until it
    /// completes). It is executable but carries no money source/destination and no fee budget.
    Recover {
        federation: FederationId,
        invite: String,
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
    /// The largest amount fundable from the source: since br-ljj.2, the exact integer maximum
    /// `amount` with `amount + floor(amount*bps/10000) ≤ budget` (`allocator::max_fundable`),
    /// where `budget = source_spendable − reservations − (standby path) the spending target` and
    /// `bps = max_fee_bps_of_move`. It exceeds the naive `floor(budget*10000/(10000+bps))` by up
    /// to 1 msat by design. Proportional — an oversized absolute cap no longer cliffs it to zero
    /// (the old saturation bug); a sub-unit budget still floors to 0. `None` when there was no
    /// usable source at all (as opposed to `Some(Msat(0))`, a source with no surplus).
    pub available: Option<Msat>,
    /// The source federation's raw spendable balance, the top of the `available` chain.
    pub source_spendable: Option<Msat>,
    /// The ABSOLUTE fee cap. `None` on a FUNDING refusal since br-ljj.2 — funding sizing uses
    /// the proportional `max_fee_bps_of_move` (and `available` already reflects it), so the
    /// absolute cap is not the funding constraint. Also `None` on an evacuation refusal, which
    /// does not pre-reserve it. The proportional bps that sized a funding refusal is recorded
    /// separately in `max_fee_bps` (br-nsx).
    pub max_fee: Option<Msat>,
    /// The `max_fee_bps_of_move` in effect that sized the funding move. `Some(..)` on a funding
    /// refusal; `None` on an evacuation refusal or a default/figure-less row.
    ///
    /// The explicit default documents the persisted journal contract: legacy refusal rows do
    /// not contain this key and decode it as `None`.
    #[serde(default)]
    pub max_fee_bps: Option<u16>,
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
            max_fee_bps,
            cap_room,
            amount,
            min_move,
        } = self;
        source.is_some()
            || want.is_some()
            || available.is_some()
            || source_spendable.is_some()
            || max_fee.is_some()
            || max_fee_bps.is_some()
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
                | Action::Recover { .. }
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
            Action::Join { .. } | Action::Recover { .. } | Action::RefuseInflow { .. } => None,
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
    /// The route between the funding source and this federation is
    /// [`RouteStatus::UneconomicAtAnySize`]: live quotes proved that no move size can clear the
    /// cap. Emitted VISIBLY (rather than skipped silently like sub-dust) because it disables
    /// rebalancing for the pair indefinitely and is fixed by raising the cap or changing gateway.
    UneconomicRoute,
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
