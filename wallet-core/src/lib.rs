//! Pure, dependency-free core wallet logic.

pub mod allocator;
pub mod executor;
pub mod types;

pub use allocator::decide;
pub use executor::*;
pub use types::*;
