//! wallet-fedimint identity newtypes (spec §6).
//!
//! These pure data newtypes (`OperationId`, `Preimage`, `GatewayUrl`, `Invoice`) now live in
//! [`wallet_core::types`] so the ledger types ([`wallet_core::ledger`]) can reference them
//! while staying pure and golden-testable. `wallet-fedimint` re-exports them here so its
//! public API — and every `crate::types::…` reference across this crate — is unchanged.

pub use wallet_core::{GatewayUrl, Invoice, OperationId, Preimage};
