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

/// A guardian's cross-federation identity, used by the ADR-0010 warm-standby
/// overlap/independence check (a local `u32` peer index is meaningless across feds).
///
/// CANONICAL ENCODING (load-bearing invariant): this MUST be the guardian's
/// consensus public-key bytes, encoded identically for every federation in a single
/// `AllocatorSnapshot`. `allocator::shares_guardian` compares these bytes for EXACT
/// equality, so it can only detect a shared guardian when both feds encode it the
/// same way. The pubkey is the right anchor because it is the guardian's
/// cryptographic identity (same key = same signer = NOT independent); an api-url is
/// a mutable host address, not a stable identity. Mixing encodings (one fed by
/// pubkey, another by URL) would make an overlap read as independent and fail OPEN —
/// funding a non-independent standby and silently defeating the insurance. The
/// producer (wallet-fedimint, a later step) owns enforcing this single encoding when
/// it populates guardians from real federation config.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct GuardianId(pub Vec<u8>);

/// A millisatoshi amount (and fees). The arithmetic here is unit-agnostic, so the
/// relabel from the former `Sats` keeps every numeric value as-is (no ×1000 scaling).
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Msat(pub u64);

/// A monotonic allocation epoch (T10). Defined here so the identity layer is
/// complete, but NOT yet wired into [`IdempotencyKey`] (see its note); the full
/// design folds it into the durable intent key to distinguish recurring intents.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Occurrence(pub u64);

/// The stable per-intent key: dedupes the same logical intent across evaluation
/// ticks and crashes. Built from `hex(FederationId)`s. NOTE: the [`Occurrence`]
/// epoch is NOT yet folded into the key, so two recurring intents with identical
/// params collide — once one is `Done`, the repeat is permanently skipped. Adding an
/// occurrence to the key (to revive recurring allocations) is deferred (TODOS T10);
/// the trailing numeric in today's key is the amount, a stand-in slot.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct IdempotencyKey(pub String);

#[derive(Clone, Debug, PartialEq)]
pub struct FederationStatus {
    pub id: FederationId,
    pub balance: Msat,
    pub probed_ok: bool,
    pub reputation: i32,
    pub guardians: Vec<GuardianId>,
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Action {
    TopUpSpending {
        from: FederationId,
        to: FederationId,
        amount: Msat,
    },
    FundStandby {
        from: FederationId,
        to: FederationId,
        amount: Msat,
    },
    Evacuate {
        from: FederationId,
        reason: ReasonCode,
    },
    RefuseAllocation {
        fed: FederationId,
        reason: ReasonCode,
    },
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
    NoIndependentStandby,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AllocatorDecision {
    pub action: Action,
    pub reason: ReasonCode,
    pub max_fee: Msat,
    pub idempotency_key: IdempotencyKey,
    pub requires_auth: bool,
}
