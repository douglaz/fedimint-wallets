//! wallet-fedimint — the crate that will own all fedimint I/O for the cross-federation
//! move (spec §1).
//!
//! **This step (Phase 1 step 1) ships ONLY the pure pieces** and pulls in no fedimint /
//! network / db / async dependencies yet: the deterministic [`move_protocol`] state
//! machine and the identity newtypes ([`OperationId`], [`Preimage`], [`GatewayUrl`],
//! [`Invoice`]). The fedimint-SDK pieces — `MultiClient`, the `Database`-backed journal,
//! the `FedimintExecutor`, the runtime/backfill loop — arrive in steps 2-4.

pub mod move_protocol;
pub mod types;

pub use move_protocol::{
    assemble_move_record, next_step, Leg, MoveParams, MovePhase, MoveRecord, MoveStep, OpArtifact,
};
pub use types::{GatewayUrl, Invoice, OperationId, Preimage};
