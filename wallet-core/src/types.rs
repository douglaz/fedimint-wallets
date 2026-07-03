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
}

#[derive(Clone, Debug, PartialEq)]
pub struct AllocatorSnapshot {
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
    /// Advisory: do not route the next inflow to `fed`. Never becomes an executor
    /// `Intent` (see `Action::is_executable`).
    RefuseInflow {
        fed: FederationId,
        reason: ReasonCode,
    },
    /// Advisory: cap further allocation into `fed`. Never becomes an executor
    /// `Intent`.
    Cap {
        fed: FederationId,
        reason: ReasonCode,
    },
}

impl Action {
    /// Whether `apply()` should create an executor `Intent` for this action.
    /// `RefuseInflow`/`Cap` are policy signals (recorded/surfaced only), not work.
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
            Action::RefuseInflow { .. } | Action::Cap { .. } => None,
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
}

#[derive(Clone, Debug, PartialEq)]
pub struct AllocatorDecision {
    pub action: Action,
    pub reason: ReasonCode,
    /// The epoch stamped into `idempotency_key` (T10): see `allocator::decide`.
    pub occurrence: Occurrence,
    pub idempotency_key: IdempotencyKey,
    pub requires_auth: bool,
}
