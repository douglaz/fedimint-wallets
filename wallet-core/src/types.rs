#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FederationId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sats(pub u64);

#[derive(Clone, Debug, PartialEq)]
pub struct FederationStatus {
    pub id: FederationId,
    pub balance: Sats,
    pub probed_ok: bool,
    pub reputation: i32,
    pub guardians: Vec<u32>,
    pub shutdown_notice: bool,
    pub healthy: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AllocatorSnapshot {
    pub federations: Vec<FederationStatus>,
    pub spending_fed: Option<FederationId>,
    pub standby_fed: Option<FederationId>,
    pub per_fed_cap: Sats,
    pub target_spending_balance: Sats,
    pub standby_target: Sats,
    pub max_fee: Sats,
    pub now: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    TopUpSpending {
        from: FederationId,
        to: FederationId,
        amount: Sats,
    },
    FundStandby {
        from: FederationId,
        to: FederationId,
        amount: Sats,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    pub max_fee: Sats,
    pub idempotency_key: String,
    pub requires_auth: bool,
}
