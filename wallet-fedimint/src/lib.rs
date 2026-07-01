//! wallet-fedimint — the crate that will own all fedimint I/O for the cross-federation
//! move (spec §1).
//!
//! **Shipped so far:**
//! - Step 1 — the pure pieces: the deterministic [`move_protocol`] state machine and the
//!   identity newtypes ([`OperationId`], [`Preimage`], [`GatewayUrl`], [`Invoice`]).
//! - Step 2 — durable storage: [`FedimintJournal`], an async [`wallet_core::Journal`] backed
//!   by a fedimint `Database` at prefix `[0x00]` (spec §8), plus the [`MoveRecord`] cache and
//!   the [`FederationInfo`] registry.
//! - Step 3 — [`MultiClient`]: one `fedimint_client::Client` per federation over the SAME
//!   `Database` (client `i` at prefix `[0x01] ++ u32_le(db_prefix)`, spec §4), with the
//!   client LIFECYCLE (join/open_all/balance/federations).
//! - Step 4a — the raw lnv2 money PRIMITIVES on [`MultiClient`] (gateways / receive / pay /
//!   await_receive / await_send) with the [`SendOutcome`]/[`ReceiveState`]/[`SendState`]
//!   outcome enums, exposed through `wallet-cli`.
//!
//! The remaining fedimint-SDK pieces — the `FedimintExecutor` (fee gross-up,
//! `MoveRecord`/`Action` wiring), op-log backfill, the runtime/reconcile loop — arrive in
//! step 4b.

pub mod journal;
pub mod move_protocol;
pub mod multi_client;
pub mod types;

pub use journal::{FederationInfo, FederationListReport, FedimintJournal};
pub use move_protocol::{
    assemble_move_record, next_step, Leg, MoveParams, MovePhase, MoveRecord, MoveStep, OpArtifact,
};
pub use multi_client::{MultiClient, ReceiveState, SendOutcome, SendState};
pub use types::{GatewayUrl, Invoice, OperationId, Preimage};
