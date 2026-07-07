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
//! Step 4b-live-1 made the `DirectInflow` path EXECUTE end-to-end (recipient nets EXACTLY the
//! target — the gross-up floors the gateway fee to invert fedimint's real `PaymentFee`, spec §6).
//! Step 4b-live-2 un-gates the cross-federation `Move`: the [`FedimintExecutor`] drives its
//! full two-leg send path (receive on `to`, re-quote + cap-check + pay from `from`, await both,
//! settle → `Done`), resume-safe (no re-mint/re-pay on replay), driven via `wallet-cli move`
//! through the [`runtime::Runtime`] façade (`do_move`). Phase 3.A maps `Evacuate` onto that same
//! send-required path so a shutdown tick can drain a dying federation into the safest other fed.
//!
//! Phase 2 step 2.2 wires the whole engine into ONE orchestrator tick: [`probe`] each open
//! fed → `wallet_core::score` → [`tick::build_snapshot`] → `wallet_core::decide` →
//! `wallet_core::apply` (through the Phase-1 [`FedimintExecutor`]), so the wallet rebalances per
//! the standing-instruction [`tick::TickPolicy`]. Exposed as [`runtime::Runtime::tick`] (decide +
//! apply) and [`runtime::Runtime::status`] (decide only, a dry run). `build_snapshot` is PURE
//! (golden-tested); the live tick is validated on the two-fed devimint harness at step 2.3.

pub mod executor;
pub mod fee;
pub mod journal;
pub mod move_protocol;
pub mod multi_client;
pub mod probe;
pub mod runtime;
pub mod tick;
pub mod types;

pub use executor::FedimintExecutor;
pub use fee::{gross_up, predicted_net, total_within_cap, GatewayFee, GrossUp};
pub use journal::{
    prune_probe_attempts, FederationInfo, FederationListReport, FedimintJournal,
    LedgerRepairOracle, OperationRef, ProbeRecord, ProbeSession, RawOpObservation, RawTerminal,
    RepairSummary, PROBE_HISTORY_CAP,
};
pub use move_protocol::{
    assemble_move_record, next_step, Leg, MoveMeta, MoveParams, MovePhase, MovePlan, MoveRecord,
    MoveRole, MoveStep, OpArtifact,
};
pub use multi_client::{
    parse_invoice, InvoiceDetails, JoinOutcome, MultiClient, ReceiveState, SendOutcome, SendState,
};
pub use probe::{assemble_facts, assemble_status, FedimintProbeRunner, ProbeResult};
pub use runtime::{
    DirectInflowOutcome, FinalizeOutcome, MoveOutcome, ProbeOutcome, ProbeReport,
    ReconcileSummary, Runtime,
};
pub use tick::{build_snapshot, ScoredFed, StatusReport, TickPolicy, TickReport};
pub use types::{GatewayUrl, Invoice, OperationId, Preimage};
