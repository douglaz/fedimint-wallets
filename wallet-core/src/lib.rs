//! Pure, dependency-free core wallet logic.

pub mod allocator;
pub mod executor;
pub mod scorer;
pub mod types;

pub use allocator::decide;
pub use executor::*;
pub use scorer::ReasonCode as ScorerReasonCode;
pub use scorer::{score, FederationFacts, FederationVerdict, Module, ObserverPrior, ScorerPolicy};
pub use types::*;
