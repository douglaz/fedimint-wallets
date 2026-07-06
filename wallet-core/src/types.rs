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
    pub max_fee: Msat,
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
    /// Advisory: do not route the next inflow to `fed` / do not cap allocation here.
    /// Never becomes an executor `Intent` (see `Action::is_executable`); the ledger's
    /// `Refusal` kind records the concept.
    RefuseInflow {
        fed: FederationId,
        reason: ReasonCode,
    },
}

impl Action {
    /// Whether `apply()` should create an executor `Intent` for this action.
    /// `RefuseInflow` is a policy signal (recorded/surfaced only), not work.
    pub fn is_executable(&self) -> bool {
        matches!(
            self,
            Action::DirectInflow { .. } | Action::Move { .. } | Action::Evacuate { .. }
        )
    }

    /// The fee budget authoritative for this action, if it has one.
    /// `Move`/`Evacuate` carry a `fee_cap` bounding the total move cost;
    /// `DirectInflow` carries one bounding its receive-side gross-up (spec §6).
    /// Advisory actions are never executed, so they have none.
    pub fn fee_cap(&self) -> Option<Msat> {
        match self {
            Action::Move { fee_cap, .. }
            | Action::Evacuate { fee_cap, .. }
            | Action::DirectInflow { fee_cap, .. } => Some(*fee_cap),
            Action::RefuseInflow { .. } => None,
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
