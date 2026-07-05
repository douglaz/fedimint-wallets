//! Dependency-light core wallet logic: sync pure decision functions
//! (`allocator::decide`, `scorer::score`) plus async I/O traits (`Executor`,
//! `Journal`). No fedimint, no network, no db.

pub mod allocator;
pub mod executor;
pub mod ledger;
pub mod scorer;
pub mod types;

pub use allocator::decide;
pub use executor::*;
pub use ledger::*;
pub use scorer::ReasonCode as ScorerReasonCode;
pub use scorer::{score, FederationFacts, FederationVerdict, Module, ObserverPrior, ScorerPolicy};
pub use types::*;
