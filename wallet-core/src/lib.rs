//! Pure, dependency-free core wallet logic.

pub mod allocator;
pub mod types;

pub use allocator::decide;
pub use types::*;
