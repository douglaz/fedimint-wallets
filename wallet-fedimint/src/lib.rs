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
//! Step 4b adds:
//! - the PURE fee model [`fee`] (the fixed-point gross-up + cap check) and the pure
//!   [`MovePlan`]/[`MoveMeta`] mapping — golden-tested;
//! - the [`FedimintExecutor`] (impl [`wallet_core::Executor`]) plus the `MultiClient` fee-quote
//!   / `backfill_ops` I/O.
//!
//! Step 4b-live-1 (this step) makes the `DirectInflow` path EXECUTE end-to-end and drives it
//! through the [`runtime::Runtime`] façade (`direct_inflow` / `await_move` / `reconcile`, spec
//! §9), exposed via `wallet-cli`. The recipient nets EXACTLY the target — the gross-up floors
//! the gateway fee to invert fedimint's real `PaymentFee` (spec §6). `Move` stays `Unsupported`.

pub mod executor;
pub mod fee;
pub mod journal;
pub mod move_protocol;
pub mod multi_client;
pub mod runtime;
pub mod types;

pub use executor::FedimintExecutor;
pub use fee::{gross_up, total_within_cap, GatewayFee, GrossUp};
pub use journal::{FederationInfo, FederationListReport, FedimintJournal};
pub use move_protocol::{
    assemble_move_record, next_step, Leg, MoveMeta, MoveParams, MovePhase, MovePlan, MoveRecord,
    MoveRole, MoveStep, OpArtifact,
};
pub use multi_client::{MultiClient, ReceiveState, SendOutcome, SendState};
pub use runtime::{DirectInflowOutcome, FinalizeOutcome, ReconcileSummary, Runtime};
pub use types::{GatewayUrl, Invoice, OperationId, Preimage};
