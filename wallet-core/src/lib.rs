//! Dependency-light core wallet logic: sync pure decision functions
//! (`allocator::decide`, `scorer::score`) plus async I/O traits (`Executor`,
//! `Journal`). No fedimint, no network, no db.

pub mod allocator;
pub mod discovery;
pub mod executor;
pub mod ledger;
pub mod probe;
pub mod scorer;
pub mod types;
pub mod watch;

pub use allocator::decide;
pub use discovery::{
    auto_join_budget, BudgetVerdict, DiscoveryPolicy, DiscoverySource, SourceStatus,
    STRUCTURAL_RECHECK_BACKOFF_MS,
};
pub use executor::*;
pub use ledger::*;
pub use probe::{
    probe_pass_expiry_anchor_ms, probe_verdict, ActiveProbeVerdict, ProbeAttempt, ProbePolicy,
    PROBE_AMOUNT_MSAT, PROBE_LEG_FEE_CAP_MSAT,
};
pub use scorer::ReasonCode as ScorerReasonCode;
pub use scorer::{
    score, score_structural, FederationFacts, FederationVerdict, Module, ObserverPrior,
    ScorerPolicy,
};
pub use types::*;
pub use watch::{
    adaptive_sleep_ms, discover_pass_plan, probe_budget_ok, probe_budget_usage, probe_next_due,
    probe_next_due_at, AdaptiveSleepDeadlines, DiscoverPassPlan, ProbeBudget, ProbeBudgetUsage,
    WatchPolicy, WATCH_BUSY_SPIN_FLOOR_MS,
};
