//! wallet-fedimint — the crate that will own all fedimint I/O for the cross-federation
//! move (spec §1).
//!
//! **Shipped so far:**
//! - Step 1 — the pure pieces: the deterministic [`move_protocol`] state machine and the
//!   identity newtypes ([`OperationId`], [`Preimage`], [`GatewayUrl`], [`Invoice`]).
//! - Step 2 — durable storage: [`FedimintJournal`], an async [`wallet_core::Journal`] backed
//!   by a fedimint `Database` at prefix `[0x00]` (spec §8), plus the [`MoveRecord`] cache and
//!   the [`FederationInfo`] registry.
//!
//! The remaining fedimint-SDK pieces — `MultiClient`, the `FedimintExecutor`, the
//! runtime/backfill loop — arrive in steps 3-4.

pub mod journal;
pub mod move_protocol;
pub mod types;

pub use journal::{FederationInfo, FederationListReport, FedimintJournal};
pub use move_protocol::{
    assemble_move_record, next_step, Leg, MoveParams, MovePhase, MoveRecord, MoveStep, OpArtifact,
};
pub use types::{GatewayUrl, Invoice, OperationId, Preimage};
