//! [`FedimintJournal`] — the durable, async [`wallet_core::Journal`] backed by a fedimint
//! [`Database`] (spec §8). All journal rows live under the **app prefix `[0x00]`**
//! (`db.with_prefix(vec![0x00])`); the per-federation clients use `[0x01, ..]` (see
//! [`crate::multi_client::MultiClient`]).
//!
//! # Encoding (settled): serde over the RAW byte API, NOT fedimint `Encodable`
//! Our row values are versioned `serde_json` envelopes containing plain types ([`Intent`],
//! [`MoveRecord`], [`FederationInfo`]); the keys are type-tagged bytes. We therefore use the
//! `Database`'s RAW byte transaction API (`raw_insert_bytes`/`raw_get_bytes`/
//! `raw_remove_entry`/`raw_find_by_prefix`) rather than the typed `Encodable` key/value
//! machinery.
//!
//! # Key layout (within the `[0x00]` prefix)
//! Each raw key is `[tag] ++ id_bytes`:
//! - `0x01` `IntentKey(IdempotencyKey)`     → JSON row v1([`Intent`])
//! - `0x02` `MoveKey(IdempotencyKey)`       → JSON row v1([`MoveRecord`])
//! - `0x03` `FederationKey(FederationId)`   → JSON row v1([`FederationInfo`])
//! - `0x04` `PendingIndexKey(status, key)`  → `()` (empty) — drives the status scans
//! - `0x05` `LedgerRowKey(be64(seq))`       → JSON row v1([`OperationRecord`]); this is
//!   exactly nine bytes, and the embedded `OperationRecord.seq` must equal the key sequence
//! - `0x0a` `WatchStateKey`                 → JSON row v1([`WatchState`])
//! - `0x0b` `PolicyKey`                     → JSON row v1([`wallet_api::Policy`])
//!
//! `IdempotencyKey` is a `String`, so `id_bytes` is its UTF-8; `FederationId` is 32 bytes.
//!
//! Only the SCANNED statuses are indexed:
//! - `Pending`/`Executing` — read by [`Journal::pending`] (the re-drive set);
//! - `Failed`              — read by [`Journal::failed`];
//! - `Awaiting`            — read by [`FedimintJournal::awaiting`], the resume loop's
//!   subscription-rehydration set (spec §9.3). A `DirectInflow` whose external payer has not
//!   paid must be re-found after a restart to re-`subscribe` its `recv_op`, yet it is NEVER
//!   in [`Journal::pending`] — it is subscription-owned, never re-driven through `perform`.
//!
//! Only the terminal `Done` status is unindexed: nothing scans it, so a `PendingIndexKey`
//! row for it would be dead weight in durable storage.
//!
//! # Atomicity (load-bearing, spec §8)
//! An [`Intent`] row and its `PendingIndexKey` move **together in one `[0x00]` dbtx**: a
//! status change removes the old index entry and inserts the new one in the SAME
//! `begin_transaction … commit_tx`, so a scan never sees an Intent indexed under a status it
//! no longer holds. Symmetrically, [`Journal::pending`]/[`Journal::failed`] read the index
//! AND the intents they reference from ONE `begin_transaction_nc` snapshot, so a status
//! change committed mid-scan can neither double-count nor drop an intent.

use crate::move_protocol::MoveRecord;
use async_trait::async_trait;
use fedimint_core::db::{AutocommitError, Database, DatabaseError, IDatabaseTransactionOpsCore};
use fedimint_core::invite_code::InviteCode;
use futures::StreamExt;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use wallet_api::Policy;
use wallet_core::{
    advance, assess_evacuation_structural_refusal, intent_status_transition_allowed,
    kind_from_action, replaceable_evacuation_record_is_pristine, status_from_intent, Action, Actor,
    AllocatorDecision, DiscoverySource, EvacuationRefusalEvidence, ExecError, FederationId,
    FeeBreakdown, GatewayUrl, IdempotencyKey, Intent, IntentStatus, Journal, Msat, Occurrence,
    OperationId, OperationKind, OperationRecord, OperationStatus, ProbeAttempt, ProbePolicy,
    RawOpUpdate, ReasonCode, RefusalDiagnostics, WriteKind,
};

/// The app-state partition prefix (spec §4/§8). Clients live at `[0x01, ..]`, see
/// [`crate::multi_client::MultiClient`].
const APP_PREFIX: u8 = 0x00;

// Type tags within the app prefix.
const TAG_INTENT: u8 = 0x01;
const TAG_MOVE: u8 = 0x02;
const TAG_FEDERATION: u8 = 0x03;
const TAG_PENDING_INDEX: u8 = 0x04;
// Operation ledger (spec §9.1): the append-only history the user reads.
const TAG_LEDGER_ROW: u8 = 0x05; // `0x05 ++ be64(seq)` → JSON row v1(OperationRecord)
const TAG_LEDGER_KEY_INDEX: u8 = 0x06; // `0x06 ++ correlation_key_utf8` → be64(seq)
const TAG_LEDGER_COUNTER: u8 = 0x07; // `0x07` (single key) → be64(next_seq)
const TAG_PROBE: u8 = 0x08; // `0x08 ++ fed_id` → JSON row v1(ProbeRecord) (phase 5 §5.0.4)
const TAG_CANDIDATE: u8 = 0x09; // `0x09 ++ fed_id` → JSON row v1(CandidateRecord) (phase 5 §5.1.1)
const TAG_WATCH_STATE: u8 = 0x0a; // `0x0a` → JSON row v1(WatchState) (phase 5 §5.2.5)
const TAG_POLICY: u8 = 0x0b; // `0x0b` → JSON row v1(wallet_api::Policy) (phase 6a §6a.6)

/// Exact corrupt/missing ledger keys are durable operator-repair work, not an unbounded queue.
/// Counter-driven direct-row reconciliation advances in chunks of this size, so a corrupt counter
/// cannot turn one watch access into an enormous loop or allocation.
const WATCH_FLOOR_UNREADABLE_KEY_LIMIT: usize = 256;
/// A caller which needs an occurrence drains several durable reconciliation chunks without waiting
/// for the outer watch cadence. Each chunk is its own autocommit and the yield between chunks keeps
/// the actor cancellable and fair. This is a work budget, not a time budget.
const WATCH_FLOOR_IMMEDIATE_DRAIN_CHUNK_BUDGET: usize = 16;

// Immutable evacuation supersession audit relation.  The canonical row is indexed by its old key;
// the reverse row maps the child key back to it without a scan.
const TAG_EVACUATION_SUPERSESSION: u8 = 0x0c;
const TAG_EVACUATION_SUPERSESSION_REVERSE: u8 = 0x0d;

/// Rows older than this are eligible for reconcile's NEGATIVE-inference repairs (§10.3): a
/// fresh non-terminal row may belong to an operation still in flight in another process, so
/// absence-of-evidence conclusions are deferred one hour and written SOFT (`repaired: true`).
const REPAIR_AGE_MS: u64 = 60 * 60 * 1000;

/// `FederationInfo.joined_at` is unix SECONDS; a join-attempt row's `created_at_ms` is millis
/// from the same device clock. The join-repair arbitration (§10.3) compares them within this
/// symmetric slack around the seconds→millis conversion.
const JOINED_AT_SLACK_MS: u64 = 60_000;

/// Version for every JSON value row. Future schema changes should add a new version and
/// migrate explicitly from old row shapes instead of mutating the version-1 contract.
///
/// **v1 value-encoding contract (deliberate, durable).** Row values are `serde_json` of the
/// plain types via their derived `Serialize`, so the 32-byte id newtypes (`FederationId`,
/// `OperationId`, `Preimage`) encode as JSON arrays of 32 integers — verbose (~130 bytes vs
/// ~66 for hex) but generated-correct. This was chosen over a hand-written compact codec on
/// purpose: this is the durable money-path, and a provably-correct derive beats hand-rolled
/// hex/base64/bincode (de)serialization for a few KB of savings on a personal wallet. A
/// compact encoding changes the on-disk bytes, so adopting one is a `ROW_VERSION` bump + a
/// migration, NOT an in-place edit of the v1 rows.
const ROW_VERSION: u8 = 1;

/// Per-federation registry row (spec §8): enough to re-open the client on resume (§9.1)
/// and to back it up (ADR-0003). `db_prefix` is the client's partition index (its
/// `[0x01, <db_prefix>]` byte layout); `joined_at` is a unix-seconds timestamp.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FederationInfo {
    pub invite: String,
    pub db_prefix: u32,
    pub joined_at: u64,
}

/// Immutable audit relation between a structurally refused agent evacuation and its fresh
/// replacement.  It deliberately duplicates the evidence held briefly on the old intent: after the
/// exchange the old row is terminal and the sidecar is the durable, queryable explanation.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EvacuationSupersessionRecord {
    pub old_key: IdempotencyKey,
    pub old_attempt: u32,
    pub new_key: IdempotencyKey,
    pub new_attempt: u32,
    pub old_occurrence: Occurrence,
    pub occurrence: Occurrence,
    pub source: FederationId,
    pub old_cap_components: Option<wallet_core::EvacFeeCap>,
    pub new_cap_components: Option<wallet_core::EvacFeeCap>,
    pub refusal: EvacuationRefusalEvidence,
    pub superseded_at_ms: u64,
}

/// The two independent immediate links for an evacuation key.  A replacement may itself be
/// replaced, so the middle of `A -> B -> C` has both fields populated.  Do not collapse this into
/// a single relation: callers rendering audit history need both facts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EvacuationSupersessionNeighbors {
    /// The relation whose child is the queried key.
    pub predecessor: Option<EvacuationSupersessionRecord>,
    /// The relation whose parent is the queried key.
    pub successor: Option<EvacuationSupersessionRecord>,
}

/// Exact outcome of the child namespace half of an uncertain marked-evacuation
/// exchange.  This is deliberately not a bool: callers must distinguish a
/// positively proven empty namespace from one which contains a protocol
/// artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplacementChildNamespace {
    Pristine,
    Contaminated,
}

/// Result of a federation-registry scan, including poison rows skipped along the way.
///
/// The resume loop can use this instead of [`FedimintJournal::list_federations`] when it
/// needs a countable signal that some stored registry rows were not reopened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FederationListReport {
    pub federations: Vec<(FederationId, FederationInfo)>,
    pub skipped_rows: usize,
}

/// Result of a candidate-registry scan, including poison rows skipped along the way.
///
/// The ordinary [`FedimintJournal::list_candidates`] call stays poison-tolerant for listing and
/// discovery progress. Tick planning uses this report so an undecodable row with a well-formed
/// federation id can still be treated conservatively by the auto-join probe gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CandidateListReport {
    pub candidates: Vec<(FederationId, CandidateRecord)>,
    pub skipped_ids: BTreeSet<FederationId>,
    pub skipped_rows: usize,
    /// Skipped rows whose federation id could be recovered from NEITHER the (malformed) key NOR
    /// the (undecodable) value. They cannot be attributed to a fed, so the funding gate cannot
    /// act on them, but each still counts fail-closed against the concurrent auto-join cap (any
    /// one could be an unproven `AutoJoined` partition) — exactly like the id-recoverable
    /// [`Self::skipped_ids`].
    pub skipped_unidentified: usize,
}

struct LedgerRowsReport {
    rows: Vec<OperationRecord>,
    skipped_rows: usize,
}

/// Durable per-fed ACTIVE-probe state (phase 5 §5.0.4): the bounded attempt history the
/// pure `probe_verdict` evaluates, plus the in-flight session identity a crashed probe
/// resumes from. One `0x08` row per federation, upserted in its own dbtx.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProbeRecord {
    pub attempts: Vec<ProbeAttempt>,
    pub in_flight: Option<ProbeSession>,
}

/// The durable probe IDENTITY (§5.0.4), written BEFORE leg IN is journaled. A `move:`
/// intent key is deterministic from `(from, to, amount, fee_cap, occurrence =
/// nonce-derived u64)`, so leg IN's key is reconstructible from the session alone; the
/// session is UPDATED with `out_net_msat` after sizing and BEFORE leg OUT is journaled,
/// after which both keys are reconstructible. Cleared in the SAME atomic write that
/// records the finished attempt ([`FedimintJournal::record_probe_outcome`]).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProbeSession {
    /// 32 lowercase-hex chars; also names the umbrella `probe:<fed-hex>:<nonce>` row.
    pub nonce: String,
    /// The probe's source federation — resolved per §5.0.7 and FIXED for the session.
    pub from: FederationId,
    pub amount_msat: u64,
    pub leg_fee_cap_msat: u64,
    /// The candidate's spendable balance BEFORE leg IN — the no-sweep BASELINE (§5.0.4):
    /// a sized-but-unjournaled leg OUT may start only while
    /// `C.spendable ≥ baseline + delivered_in`, so redeeming can never touch funds that
    /// are not the probe's own delta.
    pub c_spendable_before_in_msat: u64,
    /// Leg OUT's sized net, persisted after the affordability search and before leg OUT
    /// is journaled. A resume NEVER re-sizes: it drives with exactly this value.
    pub out_net_msat: Option<u64>,
    pub started_at_ms: u64,
}

/// Hard backstop on retained probe attempts per fed (§5.0.4): time-aware retention keeps
/// every sub-default-`ttl` attempt (plus the newest success and newest attempt regardless
/// of age), bounded by this many newest rows. At the scheduler's few-probes-per-day
/// cadence this holds years; only a script hammering `probe` can hit it (self-inflicted —
/// the ledger keeps the full narrative regardless).
pub const PROBE_HISTORY_CAP: usize = 256;

/// The durable candidate-registry row (phase 5 §5.1.1): a fed the wallet LEARNED about (from a
/// discovery source) but has not necessarily joined. Distinct from the JOINED membership
/// registry (`0x03` [`FederationInfo`]); membership authority stays there, and this row's
/// [`CandidateState`] distinguishes agent- from user-owned for the gate (§5.1.3) and the
/// auto-join budget (§5.1.4). One `0x09` row per fed, upserted in its own dbtx.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CandidateRecord {
    pub id: FederationId,
    pub invite: InviteCode,
    pub source: DiscoverySource,
    pub discovered_at_ms: u64,
    /// The authenticated STRUCTURAL verdict (the free floor: guardian count, threshold/BFT,
    /// network, modules — the scorer's structural half). Refreshed on rediscovery, not frozen.
    pub structural: StructuralOutcome,
    /// When [`Self::structural`] was last computed (a config fetch). Discovery re-checks a row
    /// older than the recheck backoff (§5.1.1), so a fed initially `Rejected` for a now-
    /// upgradeable property is reconsidered without a config fetch every pass.
    pub structural_checked_at_ms: u64,
    pub state: CandidateState,
    pub updated_at_ms: u64,
}

/// The authenticated structural-floor outcome for a candidate (§5.1.1); the reason mirrors the
/// scorer's `ReasonCode`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StructuralOutcome {
    Passed,
    Rejected(String),
}

/// A candidate's lifecycle state (§5.1.1). The gate (§5.1.3) treats only [`AutoJoined`] as
/// agent-owned/probe-gated; the budget (§5.1.4) counts it against the concurrent cap. A user
/// `join`/`approve` moves a candidate to [`UserApproved`] (§5.1.4a).
///
/// [`AutoJoined`]: CandidateState::AutoJoined
/// [`UserApproved`]: CandidateState::UserApproved
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CandidateState {
    /// Structurally rejected — not fundable NOW, but NOT a permanent blacklist: kept so it is
    /// not re-fetched every pass, and reconsidered after the structural recheck backoff (a fed
    /// can enable a required module under the same id and later pass).
    Rejected,
    /// Structurally vetted, NOT joined — surface-only until the user or the loop joins it.
    Discovered,
    /// AUTO-joined by the agent (a client partition exists); now probeable AND probe-GATED for
    /// funding, and COUNTED against the auto-join caps (§5.1.4). The probe verdict (5.0, read
    /// live from `probe_record`) is NOT stored here — `probe_record` stays the source of truth.
    AutoJoined,
    /// A user EXPLICITLY approved a candidate (§5.1.4a): it leaves the probe GATE and the
    /// CONCURRENT cap for the grandfathered USER-joined path. Reached from `Discovered` (a
    /// plain `wallet-cli join`) OR from `AutoJoined` (an `approve`). It does NOT leave the
    /// LIFETIME cap: that counts immutable agent-join history, and approval does not reclaim
    /// the partition — else approving old auto-joins would reopen the budget (§5.1.4/§5.1.4a).
    UserApproved,
}

/// Single-row watch scheduler checkpoint (phase 5 §5.2.5).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WatchState {
    pub occurrence: u64,
    pub last_discover_ms: u64,
    pub discover_cursor: Option<FederationId>,
    pub discover_backlog: bool,
    /// Candidate order snapshot for a deadline/cap-truncated discovery rotation. A cursor alone
    /// cannot distinguish older deferred source-only ids from fresh ids announced on restart.
    pub discover_rotation: Vec<FederationId>,
    /// Whether every canonical, counter-addressable ledger sequence through the observed counter has
    /// been validated and no unreadable or missing canonical row remains, so `occurrence` is the
    /// complete durable Agent floor. Noncanonical poison rows remain the separate concern of the
    /// history and budget readers; this bit does not certify them.
    ///
    /// Old direct Agent admissions predate this proof and can leave a persisted WatchState lower
    /// than the ledger. Conversely, an opaque canonical row may encode *any* `u64` occurrence: a
    /// supplied scalar bound, even `u64::MAX`, is not evidence that it is high enough. There is no
    /// safe scalar override; repairing this requires the exact row bytes or a new allocation
    /// epoch/key namespace. Older checkpoints lack the migration metadata below, so each access
    /// scans one bounded canonical ledger chunk and writes its known floor atomically. A complete
    /// scan sets this bit and makes later reads and ordinary checkpoint updates constant-time.
    /// While bounded scan backlog or an unreadable/missing row remains, the bit stays false and
    /// later accesses continue the backlog and retry exact keys. This compatibility metadata is
    /// intentional: watch checkpoints are live production rows.
    #[serde(default)]
    pub agent_floor_reconciled: bool,
    /// Whether the bounded legacy-ledger migration has initialized its canonical cursor. This deliberately differs from
    /// `agent_floor_reconciled`: an initialized scan can still have bounded sequence backlog or
    /// await operator repair of an exact unreadable/missing row.
    #[serde(default)]
    pub agent_floor_scan_initialized: bool,
    /// Exclusive ledger sequence high-water examined by the floor migration. A named serde default
    /// is required because old live rows must start at the beginning of the append-only ledger.
    #[serde(default = "watch_state_scan_high_water_default")]
    pub agent_floor_scan_high_water: u64,
    /// Exact raw `TAG_LEDGER_ROW` keys which were unreadable or missing during migration. This is
    /// bounded by the number of repair rows and lets later access retry only those exact keys rather
    /// than repeatedly scanning the full history.
    #[serde(default)]
    pub agent_floor_unreadable_ledger_keys: Vec<Vec<u8>>,
}

fn watch_state_scan_high_water_default() -> u64 {
    0
}

/// A generation at this value cannot name a strictly newer successor.  Do not
/// persist it as the standalone floor or advance a watch checkpoint into a
/// repeated generation.
pub(crate) const OCCURRENCE_EXHAUSTED_ERROR: &str =
    "occurrence exhausted at u64::MAX; choose an occurrence below u64::MAX because no newer successor exists";

pub(crate) fn ensure_occurrence_has_successor(occurrence: u64) -> Result<(), ExecError> {
    if occurrence == u64::MAX {
        return Err(ExecError::Permanent(OCCURRENCE_EXHAUSTED_ERROR.to_owned()));
    }
    Ok(())
}

/// The note a no-op re-open `join:` row carries in its `error` (§10.2): a `Succeeded` join that
/// opened an ALREADY-joined fed, creating NO new partition. The auto-join accounting (§5.1.4)
/// keys on it to EXCLUDE such rows from the partition counts, so the agent auto-join path
/// (5.1b) MUST write exactly this string — the same one `wallet-cli join`'s user path uses.
pub const JOIN_NOOP_REOPEN_NOTE: &str = "already joined (concurrent/prior); no-op re-open";

/// Trailing-7d window for the weekly auto-join rate cap (§5.1.4).
const AUTO_JOIN_WEEKLY_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Durable [`wallet_core::Journal`] over a fedimint [`Database`], isolated to prefix `[0x00]`.
#[derive(Clone, Debug)]
pub struct FedimintJournal {
    /// Already `with_prefix(vec![0x00])`; all raw keys here are relative to that partition.
    db: Database,
    /// Retained in test builds so restart tests can make a fresh journal over
    /// the same durable store without retaining any process-local test state.
    #[cfg(test)]
    test_root_db: Database,
    /// [`Journal::store_id`]: identity of `db`'s underlying storage, captured in [`Self::new`]
    /// from the pre-`with_prefix` handle (see there for why `with_prefix` itself can't supply
    /// it).
    store_id: usize,
    /// The injected ledger clock (spec §9.4): unix millis for `updated_at_ms` on the
    /// journal-integrated ledger writes and for repair's age heuristics. `seq` is the ordering
    /// authority — the clock is display material plus the one repair dependency (§10.3), so it
    /// is injectable (production [`SystemTime::now`]; tests pin it via [`Self::with_clock`]).
    clock: fn() -> u64,
    /// Unit-test seam for the post-driver-finish intent refresh.  It intentionally faults the
    /// shared read helper so the actor recovery path is tested against the same `Journal::get`
    /// failure it handles in production.
    #[cfg(test)]
    fail_intent_reads: Arc<AtomicUsize>,
    /// Persistent, key-specific version of the post-driver-finish read fault.  The ownership
    /// recovery regression uses this rather than a broad database failure so it can keep
    /// producing the exact `DriverFinished` race while the durable scan itself remains healthy.
    #[cfg(test)]
    persistent_intent_read_faults: Arc<Mutex<BTreeSet<IdempotencyKey>>>,
    #[cfg(test)]
    fail_operation_reads: Arc<AtomicUsize>,
    /// Inject an error after `set_status` has durably committed, exercising the actor's
    /// scoped durability-ambiguity recovery.
    #[cfg(test)]
    fail_after_status_writes: Arc<AtomicUsize>,
    /// Inject an error after an atomic retryable reset committed.  This is separate from the
    /// status writer because a structural marker and its scheduler wake have a distinct
    /// post-commit ambiguity contract.
    #[cfg(test)]
    fail_after_retryable_resets: Arc<AtomicUsize>,
    /// Inject errors after the actor-routed artifact writers durably commit.
    #[cfg(test)]
    fail_after_artifact_writes: Arc<AtomicUsize>,
    #[cfg(test)]
    fail_after_move_writes: Arc<AtomicUsize>,
    /// Inject an error after `upsert` has durably committed.  CommitTick uses this to prove it
    /// treats a storage refusal as a potentially durable fresh admission.
    #[cfg(test)]
    fail_after_upserts: Arc<AtomicUsize>,
    /// Inject a pre-transaction marker-clear fault. This isolates CommitTick's already-Started
    /// audit-row terminalization from exchange/admission faults.
    #[cfg(test)]
    fail_before_marker_clears: Arc<AtomicUsize>,
    /// Inject a definite pre-transaction marker-clear refusal. Unlike an I/O fault, this cannot
    /// have committed, so actor tests use it to distinguish permanent validation from ambiguity.
    #[cfg(test)]
    fail_before_marker_clears_permanently: Arc<AtomicUsize>,
    /// Inject an error after a marker clear has durably committed. This models the same ambiguous
    /// commit boundary as other writer seams; callers must exact-reread before deciding whether
    /// the next marker wake is a continuation of their own clear.
    #[cfg(test)]
    fail_after_marker_clears: Arc<AtomicUsize>,
    /// Inject an error after the marked-evacuation exchange has durably
    /// committed.  Unlike a normal write error this leaves all three exchange
    /// rows present, so actor code must prove the outcome by exact reread.
    #[cfg(test)]
    fail_after_evacuation_replacements: Arc<AtomicUsize>,
    /// Arm one confirmation intent read only after the replacement commit has completed.
    #[cfg(test)]
    fail_confirmation_read_after_evacuation_replacements: Arc<AtomicUsize>,
    /// Inject an exchange error before its transaction opens.  This models the
    /// one outcome whose exact reread can prove that no child namespace was
    /// touched.
    #[cfg(test)]
    fail_before_evacuation_replacements: Arc<AtomicUsize>,
    /// Make a post-commit exchange error leave an intentionally incomplete
    /// relation.  Service tests use this only to prove that confirmation poisons
    /// authority rather than guessing which side of the exchange committed.
    #[cfg(test)]
    corrupt_after_evacuation_replacements: Arc<AtomicUsize>,
    /// One-shot service-test seam.  The exchange is already durable when this is
    /// consumed; stopping before the actor installs the child driver models a
    /// process loss in exactly that narrow recovery window.
    #[cfg(test)]
    stop_after_evacuation_replacement_before_child_driver: Arc<AtomicBool>,
    /// Test-only durable corruption seam: replace the just-committed intent row before returning
    /// the configured post-upsert error.  This models a concurrent/corrupt row whose exact key is
    /// readable but whose request identity is not the one the fresh caller attempted.
    #[cfg(test)]
    replace_after_upsert: Arc<Mutex<Option<Intent>>>,
    /// One-shot targeted read fault which waits for a caller-selected number of successful
    /// `Journal::get` calls.  This distinguishes CommitTick's pre-read from the core helper's
    /// replay read without making every intent read fail.
    #[cfg(test)]
    fail_intent_read_after_successes: Arc<Mutex<Option<usize>>>,
    /// One-shot delayed scan faults let actor tests target the replacement's
    /// second, pre-exchange admission scan rather than CommitTick's initial
    /// whole-round projection.
    #[cfg(test)]
    fail_pending_read_after_successes: Arc<Mutex<Option<usize>>>,
    #[cfg(test)]
    fail_reservation_read_after_successes: Arc<Mutex<Option<usize>>>,
    #[cfg(test)]
    pending_reads: Arc<AtomicUsize>,
    /// A one-shot pause after a durable pending scan.  This lets the actor test queue a later
    /// `DriverFinished` read fault between the scan and its generation acknowledgement.
    #[cfg(test)]
    pending_read_pause: Arc<Mutex<Option<Arc<PendingReadPause>>>>,
    /// A one-shot pause immediately before the planner's authoritative replacement-parent scan.
    /// This lets service tests insert a marker after planning starts without retaining a redundant
    /// production pre-scan solely to make that race deterministic.
    #[cfg(test)]
    replacement_scan_pause: Arc<Mutex<Option<Arc<PendingReadPause>>>>,
    /// A one-shot pause after the scheduler's raw watch-floor retry checkpoint read.  The
    /// scheduler owns cancellation around this read, so this seam proves an abort does not wait
    /// behind post-cycle inspection.
    #[cfg(test)]
    watch_floor_immediate_retry_read_pause: Arc<Mutex<WatchFloorImmediateRetryReadPause>>,
    /// Key-scoped one-shot transaction rendezvous.  It is deliberately not a
    /// global test barrier: unrelated journal tests and keys must never join
    /// this forced overlap.
    #[cfg(test)]
    replacement_write_rendezvous: Arc<Mutex<BTreeMap<IdempotencyKey, Arc<tokio::sync::Barrier>>>>,
    /// A one-shot pause immediately after a `put_move_if_attempt` write commits and before its
    /// postverify read.  It makes cleanup-race tests deterministic without being a production
    /// synchronization primitive.
    #[cfg(test)]
    post_move_write_pauses: Arc<Mutex<BTreeMap<IdempotencyKey, Arc<PostMoveWritePause>>>>,
    /// Barrier after the WatchState read/check/write closure has read its snapshot and before its
    /// autocommit returns. It deterministically makes two callers race the commit boundary.
    #[cfg(test)]
    watch_state_autocommit_pause: Arc<Mutex<Option<Arc<WatchStateAutocommitPause>>>>,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct PendingReadPause {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
type WatchFloorImmediateRetryReadPause = Option<(usize, Arc<PendingReadPause>)>;

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct PostMoveWritePause {
    committed: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Debug)]
struct WatchStateAutocommitPause {
    barrier: tokio::sync::Barrier,
    arrivals: AtomicUsize,
}

#[cfg(test)]
impl PostMoveWritePause {
    pub(crate) async fn wait_until_committed(&self) {
        self.committed.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_waiters();
    }
}

#[cfg(test)]
impl PendingReadPause {
    pub(crate) async fn wait_until_started(&self) {
        self.started.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_waiters();
    }
}

#[cfg(test)]
async fn wait_replacement_write_rendezvous_for_test(
    rendezvous: &Arc<Mutex<BTreeMap<IdempotencyKey, Arc<tokio::sync::Barrier>>>>,
    key: &IdempotencyKey,
) {
    let barrier = rendezvous
        .lock()
        .expect("replacement write rendezvous lock poisoned")
        .get(key)
        .cloned();
    let Some(barrier) = barrier else {
        return;
    };
    barrier.wait().await;
}

#[cfg(test)]
fn clear_replacement_write_rendezvous_for_test(
    rendezvous: &Arc<Mutex<BTreeMap<IdempotencyKey, Arc<tokio::sync::Barrier>>>>,
    key: &IdempotencyKey,
) {
    rendezvous
        .lock()
        .expect("replacement write rendezvous lock poisoned")
        .remove(key);
}

impl FedimintJournal {
    /// Wrap a fedimint [`Database`], isolating every journal row under the app prefix `[0x00]`.
    ///
    /// Two `FedimintJournal`s built from the SAME underlying `Database` share storage (the
    /// `[0x00]` partition over one inner `Arc`): a row written by one is visible to the other.
    ///
    /// [`Self::store_id`] (spec §2, the in-process single-writer guard) is captured HERE, from
    /// `db` itself, before `with_prefix` wraps it: `with_prefix` always allocates a fresh
    /// adapter `Arc`, so two `FedimintJournal`s built from clones of the same `db` would
    /// otherwise get different post-prefix pointers even though they share the same backing
    /// store. `Database::clone` shares its inner `Arc` unchanged, so reading the identity off
    /// a clone of the ORIGINAL `db` (via the public `into_inner`) gives two such calls the
    /// SAME id, while an unrelated `Database` gets a different one.
    pub fn new(db: Database) -> Self {
        Self::with_clock(db, system_now_ms)
    }

    /// Like [`Self::new`] but with an injected ledger clock (spec §9.4) — the testing seam for
    /// the repair heuristics that read `created_at_ms`/`updated_at_ms`. Production uses
    /// [`system_now_ms`]; a skewed-clock repair test pins a fixed/jumping value here.
    pub fn with_clock(db: Database, clock: fn() -> u64) -> Self {
        let store_id = Arc::as_ptr(&db.clone().into_inner()) as *const () as usize;
        Self {
            db: db.with_prefix(vec![APP_PREFIX]),
            #[cfg(test)]
            test_root_db: db.clone(),
            store_id,
            clock,
            #[cfg(test)]
            fail_intent_reads: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            persistent_intent_read_faults: Arc::new(Mutex::new(BTreeSet::new())),
            #[cfg(test)]
            fail_operation_reads: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_after_status_writes: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_after_retryable_resets: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_after_artifact_writes: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_after_move_writes: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_after_upserts: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_before_marker_clears: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_before_marker_clears_permanently: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_after_marker_clears: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_after_evacuation_replacements: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_confirmation_read_after_evacuation_replacements: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            fail_before_evacuation_replacements: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            corrupt_after_evacuation_replacements: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            stop_after_evacuation_replacement_before_child_driver: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            replace_after_upsert: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            fail_intent_read_after_successes: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            fail_pending_read_after_successes: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            fail_reservation_read_after_successes: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            pending_reads: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            pending_read_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            replacement_scan_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            watch_floor_immediate_retry_read_pause: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            replacement_write_rendezvous: Arc::new(Mutex::new(BTreeMap::new())),
            #[cfg(test)]
            post_move_write_pauses: Arc::new(Mutex::new(BTreeMap::new())),
            #[cfg(test)]
            watch_state_autocommit_pause: Arc::new(Mutex::new(None)),
        }
    }

    /// Construct a new journal/runtime-facing handle over this exact backing
    /// store.  Unlike cloning the journal, this loses every test seam and cache
    /// owned by the old process.
    #[cfg(test)]
    pub(crate) fn reopen_for_test(&self) -> Self {
        Self::with_clock(self.test_root_db.clone(), self.clock)
    }

    #[cfg(test)]
    async fn wait_replacement_write_rendezvous_for_test(&self, key: &IdempotencyKey) {
        wait_replacement_write_rendezvous_for_test(&self.replacement_write_rendezvous, key).await;
    }

    #[cfg(test)]
    pub(crate) fn pause_after_next_move_write_for_test(
        &self,
        key: IdempotencyKey,
    ) -> Arc<PostMoveWritePause> {
        let pause = Arc::new(PostMoveWritePause {
            committed: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let previous = self
            .post_move_write_pauses
            .lock()
            .expect("post-move-write pause lock poisoned")
            .insert(key, Arc::clone(&pause));
        assert!(
            previous.is_none(),
            "only one post-move-write pause may be installed per key"
        );
        pause
    }

    #[cfg(test)]
    pub(crate) fn rendezvous_two_watch_state_autocommits_for_test(&self) {
        *self
            .watch_state_autocommit_pause
            .lock()
            .expect("watch-state autocommit pause lock poisoned") =
            Some(Arc::new(WatchStateAutocommitPause {
                barrier: tokio::sync::Barrier::new(2),
                arrivals: AtomicUsize::new(0),
            }));
    }

    /// Pause exactly one raw `watch_floor_immediate_retry_needed` checkpoint after it has read
    /// the durable row.  This is deliberately a read seam rather than a scheduler hook: the
    /// regression is that the outer scheduler must cancel the real inspection future.
    #[cfg(test)]
    pub(crate) fn pause_watch_floor_immediate_retry_read_after_for_test(
        &self,
        successful_reads_before_pause: usize,
    ) -> Arc<PendingReadPause> {
        let pause = Arc::new(PendingReadPause {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let previous = self
            .watch_floor_immediate_retry_read_pause
            .lock()
            .expect("watch-floor immediate-retry pause lock poisoned")
            .replace((successful_reads_before_pause, Arc::clone(&pause)));
        assert!(
            previous.is_none(),
            "only one watch-floor immediate-retry pause may be installed"
        );
        pause
    }

    #[cfg(test)]
    async fn wait_watch_floor_immediate_retry_read_for_test(&self) {
        // Remove before waiting so cancellation cannot leave a later scheduler cycle paused.
        let pause = {
            let mut configured = self
                .watch_floor_immediate_retry_read_pause
                .lock()
                .expect("watch-floor immediate-retry pause lock poisoned");
            let Some((remaining, _)) = configured.as_mut() else {
                return;
            };
            if *remaining > 0 {
                *remaining -= 1;
                return;
            }
            configured
                .take()
                .expect("watch-floor immediate-retry pause remains installed")
                .1
        };
        pause.started.notify_one();
        pause.release.notified().await;
    }

    #[cfg(test)]
    async fn wait_watch_state_autocommit_rendezvous_for_test(
        pause: &Option<Arc<WatchStateAutocommitPause>>,
    ) {
        let Some(pause) = pause else {
            return;
        };
        if pause.arrivals.fetch_add(1, Ordering::SeqCst) < 2 {
            pause.barrier.wait().await;
        }
    }

    #[cfg(test)]
    async fn wait_after_move_write_for_test(&self, key: &IdempotencyKey) {
        // Remove before waiting: the test can make an N+1 writer run while the N writer is
        // paused, and that newer writer must not inherit this one-shot pause.
        let pause = self
            .post_move_write_pauses
            .lock()
            .expect("post-move-write pause lock poisoned")
            .remove(key);
        let Some(pause) = pause else {
            return;
        };
        pause.committed.notify_one();
        pause.release.notified().await;
    }

    #[cfg(test)]
    pub(crate) fn fail_next_intent_reads_for_test(&self, count: usize) {
        self.fail_intent_reads.store(count, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn persistently_fail_intent_read_for_test(&self, key: IdempotencyKey) {
        self.persistent_intent_read_faults
            .lock()
            .expect("persistent intent fault lock poisoned")
            .insert(key);
    }

    #[cfg(test)]
    pub(crate) fn clear_persistent_intent_read_fault_for_test(&self, key: &IdempotencyKey) {
        self.persistent_intent_read_faults
            .lock()
            .expect("persistent intent fault lock poisoned")
            .remove(key);
    }

    /// Test seam for callers which refresh an operation-ledger row after updating a
    /// durable probe outcome.  Keep this separate from intent reads: probe umbrellas
    /// are ledger-backed, not intent-backed.
    #[cfg(test)]
    pub(crate) fn fail_next_operation_reads_for_test(&self, count: usize) {
        self.fail_operation_reads.store(count, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_status_write_for_test(&self) {
        self.fail_after_status_writes.store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_retryable_reset_for_test(&self) {
        self.fail_after_retryable_resets.store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_artifact_write_for_test(&self) {
        self.fail_after_artifact_writes.store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_move_write_for_test(&self) {
        self.fail_after_move_writes.store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_upsert_for_test(&self) {
        self.fail_after_upserts.store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_before_next_marker_clear_for_test(&self) {
        self.fail_before_marker_clears.store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_before_next_marker_clear_permanently_for_test(&self) {
        self.fail_before_marker_clears_permanently
            .store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_marker_clear_for_test(&self) {
        self.fail_after_marker_clears.store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_evacuation_replacement_for_test(&self) {
        self.fail_after_evacuation_replacements
            .store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_after_next_evacuation_replacement_with_confirmation_read_for_test(&self) {
        self.fail_after_evacuation_replacements
            .store(1, Ordering::SeqCst);
        self.fail_confirmation_read_after_evacuation_replacements
            .store(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_before_next_evacuation_replacement_for_test(&self) {
        self.fail_before_evacuation_replacements
            .store(1, Ordering::SeqCst);
    }

    /// Seed one stale child-owned row without a child intent. The marked
    /// replacement autocommit must reject this namespace during its pre-commit
    /// validation; service tests use it to distinguish that definite outcome
    /// from a retryable commit acknowledgement ambiguity.
    #[cfg(test)]
    pub(crate) async fn seed_stale_replacement_child_namespace_for_test(
        &self,
        key: &IdempotencyKey,
    ) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&move_key(key), b"stale replacement child namespace")
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)
    }

    /// The exchange transaction completes, then its canonical audit half is
    /// removed before the injected error is returned.  This is deliberately
    /// impossible in production; it is a discriminating test seam for the
    /// actor's ambiguous-confirmation fail-closed path.
    #[cfg(test)]
    pub(crate) fn fail_after_next_evacuation_replacement_ambiguously_for_test(&self) {
        self.corrupt_after_evacuation_replacements
            .store(1, Ordering::SeqCst);
        self.fail_after_evacuation_replacements
            .store(1, Ordering::SeqCst);
    }

    /// Make the next successful actor exchange return before it registers its
    /// child driver.  Production never observes this seam; it exists solely to
    /// verify restart recovery from the durable exchange boundary.
    #[cfg(test)]
    pub(crate) fn stop_after_evacuation_replacement_before_child_driver_for_test(&self) {
        self.stop_after_evacuation_replacement_before_child_driver
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn take_stop_after_evacuation_replacement_before_child_driver_for_test(
        &self,
    ) -> bool {
        self.stop_after_evacuation_replacement_before_child_driver
            .swap(false, Ordering::SeqCst)
    }

    /// Replace the next successful `upsert`'s durable intent row before it reports its configured
    /// post-commit fault.  The replacement must keep the key and indexed status stable so this
    /// seam isolates identity ambiguity rather than testing index repair.
    #[cfg(test)]
    pub(crate) fn replace_after_next_upsert_for_test(&self, intent: Intent) {
        *self
            .replace_after_upsert
            .lock()
            .expect("post-upsert replacement lock poisoned") = Some(intent);
    }

    #[cfg(test)]
    pub(crate) fn fail_one_intent_read_after_successes_for_test(&self, successes: usize) {
        *self
            .fail_intent_read_after_successes
            .lock()
            .expect("intent read fault lock poisoned") = Some(successes);
    }

    #[cfg(test)]
    pub(crate) fn fail_one_pending_read_after_successes_for_test(&self, successes: usize) {
        *self
            .fail_pending_read_after_successes
            .lock()
            .expect("pending read fault lock poisoned") = Some(successes);
    }

    #[cfg(test)]
    pub(crate) fn fail_one_reservation_read_after_successes_for_test(&self, successes: usize) {
        *self
            .fail_reservation_read_after_successes
            .lock()
            .expect("reservation read fault lock poisoned") = Some(successes);
    }

    #[cfg(test)]
    pub(crate) fn reset_pending_reads_for_test(&self) {
        self.pending_reads.store(0, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn pending_reads_for_test(&self) -> usize {
        self.pending_reads.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn pause_next_pending_read_for_test(&self) -> Arc<PendingReadPause> {
        let pause = Arc::new(PendingReadPause {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *self
            .pending_read_pause
            .lock()
            .expect("pending read pause lock poisoned") = Some(Arc::clone(&pause));
        pause
    }

    #[cfg(test)]
    pub(crate) fn pause_before_next_replacement_scan_for_test(&self) -> Arc<PendingReadPause> {
        let pause = Arc::new(PendingReadPause {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *self
            .replacement_scan_pause
            .lock()
            .expect("replacement scan pause lock poisoned") = Some(Arc::clone(&pause));
        pause
    }

    #[cfg(test)]
    pub(crate) async fn wait_before_replacement_scan_for_test(&self) {
        let pause = self
            .replacement_scan_pause
            .lock()
            .expect("replacement scan pause lock poisoned")
            .take();
        if let Some(pause) = pause {
            pause.started.notify_one();
            pause.release.notified().await;
        }
    }

    /// The ledger's wall-clock in unix millis (the injected [`Self::clock`]).
    fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    /// CAS a raw Pay/Receive terminal status against the ledger row that repair actually
    /// terminalized.  A retry replaces the public-key row with a higher attempt, and an
    /// authoritative same-attempt artifact can supersede a soft repair, so both must benignly
    /// lose rather than terminalize a reservation on stale evidence.
    pub async fn set_raw_terminal_if_fenced(
        &self,
        key: &IdempotencyKey,
        fence: &RawIntentTerminalFence,
        status: IntentStatus,
        _error: Option<&str>,
    ) -> Result<bool, ExecError> {
        // The fence names the completed ledger outcome, not merely any terminal ledger row.
        // This public sink is also used by actor-routed repair, so never let a caller turn a
        // successful ledger repair into a Failed intent (or vice versa).
        let status_matches_ledger = matches!(
            (fence.expected_ledger_status, status),
            (OperationStatus::Succeeded, IntentStatus::Done)
                | (OperationStatus::Failed, IntentStatus::Failed)
        );
        if !status_matches_ledger {
            return Ok(false);
        }
        self.db
            .autocommit(
                |dbtx, _| {
                    Box::pin(async move {
                        let index_key = ledger_key_index(key);
                        let Some(index) = dbtx.raw_get_bytes(&index_key).await.map_err(db_err)?
                        else {
                            return Ok(false);
                        };
                        if read_be64(&index) != Some(fence.expected_seq) {
                            return Ok(false);
                        }
                        let row_key = ledger_row_key(fence.expected_seq);
                        let Some(row_bytes) = dbtx.raw_get_bytes(&row_key).await.map_err(db_err)?
                        else {
                            return Ok(false);
                        };
                        let row = decode_canonical_ledger_row(&row_key, &row_bytes)?;
                        let Some((fed, op_id, _)) = raw_row_parts(&row.kind) else {
                            return Ok(false);
                        };
                        if row.correlation_key != *key
                            || row.seq != fence.expected_seq
                            || fed != fence.fed
                            || op_id != fence.expected_op
                            || raw_role(&row.kind) != Some(fence.role)
                            || row.status != fence.expected_ledger_status
                        {
                            return Ok(false);
                        }
                        let ikey = intent_key(key);
                        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
                            return Ok(false);
                        };
                        let mut intent = decode_row_result::<Intent>("intent", &ikey, &bytes)?;
                        if intent.attempt != fence.expected_attempt
                            || !matches!(intent.action, Action::Pay { .. } | Action::Receive { .. })
                            || intent_status_is_terminal(intent.status)
                        {
                            return Ok(false);
                        }
                        let intent_fed = match intent.action {
                            Action::Pay { from, .. } => from,
                            Action::Receive { to, .. } => to,
                            _ => unreachable!("raw action check above admits only pay/receive"),
                        };
                        if intent_fed != fence.fed {
                            return Ok(false);
                        }
                        // Repair's fenced ledger write is already durable.  Its recovered raw
                        // operation identity must become part of this exact intent before the
                        // terminal status releases it: a failed Pay with a committed operation
                        // cannot be manually retried, because the SDK would only rediscover that
                        // same operation.  Never overwrite a conflicting intent identity.
                        match (intent.operation_id, fence.expected_op) {
                            (Some(recorded), expected) if Some(recorded) != expected => {
                                return Ok(false);
                            }
                            (None, Some(recovered)) => intent.operation_id = Some(recovered),
                            _ => {}
                        }
                        let old_status = intent.status;
                        intent.status = status;
                        // Repair already made the fenced ledger terminal durable before it asks
                        // this sink to release the reservation.  Do not re-run the ordinary
                        // intent+ledger writer here: an uncertain hash-dedup repair must retain
                        // both its `repaired` bit and its audit note until an authoritative
                        // observation deliberately supersedes it.
                        write_intent_and_pending_index(dbtx, &ikey, key, old_status, &intent)
                            .await?;
                        Ok(true)
                    })
                },
                None,
            )
            .await
            .map_err(|e| match e {
                AutocommitError::CommitFailed { last_error, .. } => db_err(last_error),
                AutocommitError::ClosureError { error, .. } => error,
            })
    }

    /// Atomically begin a deliberate retry of a terminal-failed intent while preserving the
    /// failed attempt's immutable ledger row. The intent key remains the public idempotency
    /// anchor; the ledger index advances to a fresh row for the new attempt, so history retains
    /// both truthful outcomes and `operation(key)` resolves the currently active attempt.
    pub async fn retry_failed_intent(&self, refreshed: &Intent) -> Result<(), ExecError> {
        if refreshed.status != IntentStatus::Pending {
            return Err(ExecError::Permanent(
                "journal: a manual retry must restart as Pending".to_owned(),
            ));
        }
        let key = &refreshed.idempotency_key;
        let ikey = intent_key(key);
        let mut dbtx = self.db.begin_transaction().await;
        let old_bytes = dbtx
            .raw_get_bytes(&ikey)
            .await
            .map_err(db_err)?
            .ok_or_else(|| ExecError::Permanent("journal: retry intent not found".to_owned()))?;
        let old: Intent = decode_row_result("intent", &ikey, &old_bytes)?;
        if old.status != IntentStatus::Failed {
            return Err(ExecError::Permanent(format!(
                "journal: intent {} is not Failed and cannot be manually retried",
                key.0
            )));
        }
        // A structural replacement retires this public parent permanently.  Reopening it would
        // create two live evacuations for one source while the immutable sidecar still says that
        // the child is its sole successor.
        if let Some(bytes) = dbtx
            .raw_get_bytes(&evacuation_supersession_key(key))
            .await
            .map_err(db_err)?
        {
            let relation: EvacuationSupersessionRecord = decode_row_result(
                "evacuation supersession",
                &evacuation_supersession_key(key),
                &bytes,
            )?;
            validate_supersession_endpoints(&relation)?;
            return Err(ExecError::Permanent(
                "journal: a superseded evacuation parent can never be retried".to_owned(),
            ));
        }
        let expected_attempt = old.attempt.checked_add(1).ok_or_else(|| {
            ExecError::Permanent("journal: manual retry attempt counter overflow".to_owned())
        })?;
        if refreshed.attempt != expected_attempt {
            return Err(ExecError::Permanent(format!(
                "journal: manual retry attempt must advance from {} to {}",
                old.attempt, expected_attempt
            )));
        }

        dbtx.raw_remove_entry(&pending_index_key(IntentStatus::Failed, key))
            .await
            .map_err(db_err)?;
        dbtx.raw_insert_bytes(&ikey, &encode_row(refreshed)?)
            .await
            .map_err(db_err)?;
        dbtx.raw_insert_bytes(&pending_index_key(IntentStatus::Pending, key), &[])
            .await
            .map_err(db_err)?;
        // The 0x02 row is the derived state of the attempt that just failed. In
        // particular, a cached terminal MovePhase would make the refreshed Pending
        // intent fail again before doing any work. The old attempt's immutable ledger
        // row remains its audit record; this cache is safe and necessary to reset.
        dbtx.raw_remove_entry(&move_key(key))
            .await
            .map_err(db_err)?;

        let (next_seq, successor) = next_ledger_sequence_in(&mut dbtx).await?;
        let now = self.now_ms();
        let row = fresh_intent_record(next_seq, refreshed, OperationStatus::Started, now, None);
        note_ledger_insert_in(&mut dbtx, &row, next_seq).await?;
        dbtx.raw_insert_bytes(&ledger_counter_key(), &successor.to_be_bytes())
            .await
            .map_err(db_err)?;
        dbtx.raw_insert_bytes(&ledger_row_key(next_seq), &encode_row(&row)?)
            .await
            .map_err(db_err)?;
        dbtx.raw_insert_bytes(&ledger_key_index(key), &next_seq.to_be_bytes())
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)
    }

    // --- inherent read helpers (shared by the trait methods) ---

    /// Load and decode the [`Intent`] stored under `key`, or `None` if absent.
    async fn read_intent(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        #[cfg(test)]
        {
            let mut after_successes = self
                .fail_intent_read_after_successes
                .lock()
                .expect("intent read fault lock poisoned");
            if matches!(*after_successes, Some(0)) {
                *after_successes = None;
                return Err(ExecError::Retryable(format!(
                    "journal: injected intent read failure for --key {}",
                    key.0
                )));
            }
            if let Some(remaining) = after_successes.as_mut() {
                *remaining -= 1;
            }
        }
        #[cfg(test)]
        if self
            .persistent_intent_read_faults
            .lock()
            .expect("persistent intent fault lock poisoned")
            .contains(key)
            || self
                .fail_intent_reads
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(ExecError::Retryable(format!(
                "injected intent refresh read failure for --key {}",
                key.0
            )));
        }
        let raw_key = intent_key(key);
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? else {
            return Ok(None);
        };
        let intent: Intent = decode_row_result("intent", &raw_key, &bytes)?;
        // The row's own key MUST match the one we looked up. A mismatch means a corrupt row
        // or a key-encoding collision, not a real hit; a targeted `get` returning the wrong
        // intent would be worse than an error.
        if intent.idempotency_key != *key {
            return Err(ExecError::Permanent(format!(
                "journal: intent row under {key:?} carries mismatched key {:?}",
                intent.idempotency_key
            )));
        }
        Ok(Some(intent))
    }

    /// Load every [`Intent`] currently indexed under any of `statuses`, from a SINGLE
    /// `begin_transaction_nc` snapshot (spec §8): the index scan AND the intent reads share
    /// one consistent view, so a status change committed mid-scan can neither surface an
    /// intent twice nor drop one (the atomic write keeps each intent's index entry in
    /// lockstep with its status, and one snapshot reads exactly one committed point).
    ///
    /// The ONE scan helper behind the operational scans and the stricter decide-time
    /// reservation scan. Operational reconcile/resume scans skip poison rows so one corrupt
    /// entry cannot strand healthy recovery work. Admission passes `fail_on_corruption = true`:
    /// a malformed/dangling index entry, missing intent, corrupt row, key mismatch, or status
    /// skew makes the reservation view incomplete, so deciding from it must fail closed.
    async fn intents_indexed_as(
        &self,
        statuses: &[IntentStatus],
        fail_on_corruption: bool,
    ) -> Result<Vec<Intent>, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;

        // 1. Collect the idempotency keys from per-status prefixes. Use a set so a corrupt
        //    store that indexes one key under two scanned statuses cannot surface it twice.
        let mut keys = BTreeSet::new();
        for status in statuses.iter().copied() {
            let prefix = pending_index_prefix(status);
            let mut stream = dbtx.raw_find_by_prefix(&prefix).await.map_err(db_err)?;
            while let Some((raw_key, _)) = stream.next().await {
                // raw_key = [TAG_PENDING_INDEX, status_byte] ++ idempotency_key_bytes (UTF-8).
                // Validate UTF-8 in place; only allocate the owned key on success.
                match raw_key.get(2..).map(std::str::from_utf8) {
                    Some(Ok(key)) => {
                        keys.insert(IdempotencyKey(key.to_owned()));
                    }
                    _ if fail_on_corruption => {
                        return Err(ExecError::Permanent(format!(
                            "journal: malformed intent index key {raw_key:?}"
                        )));
                    }
                    _ => tracing::warn!(?raw_key, "journal: skipping malformed index key"),
                }
            }
        } // drop the stream so `dbtx` is free to re-borrow for the reads below.

        // 2. Read each referenced intent from the SAME snapshot. The `statuses` re-check is a
        //    belt-and-suspenders guard against any index/intent skew (none can arise from the
        //    atomic write, but a corrupt store should not surface a wrong-status intent).
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let raw_key = intent_key(&key);
            match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
                Some(bytes) => match decode_row_result::<Intent>("intent", &raw_key, &bytes) {
                    Ok(intent) if intent.idempotency_key != key && fail_on_corruption => {
                        return Err(ExecError::Permanent(format!(
                            "journal: intent index key {} disagrees with embedded key {}",
                            key.0, intent.idempotency_key.0
                        )));
                    }
                    Ok(intent) if intent.idempotency_key != key => {
                        tracing::warn!(
                            index_key = %key.0,
                            embedded_key = %intent.idempotency_key.0,
                            "journal: index/intent key mismatch, skipping",
                        );
                    }
                    Ok(intent) if statuses.contains(&intent.status) => out.push(intent),
                    Ok(intent) if fail_on_corruption => {
                        return Err(ExecError::Permanent(format!(
                            "journal: intent index for {} has unexpected status {:?}",
                            key.0, intent.status
                        )));
                    }
                    Ok(intent) => tracing::warn!(
                        key = %key.0,
                        status = ?intent.status,
                        "journal: index/intent status skew, skipping",
                    ),
                    Err(error) if fail_on_corruption => return Err(error),
                    Err(e) => {
                        tracing::warn!(key = %key.0, error = ?e, "journal: skipping corrupt intent row");
                    }
                },
                None if fail_on_corruption => {
                    return Err(ExecError::Permanent(format!(
                        "journal: intent index references missing intent {}",
                        key.0
                    )));
                }
                None => {
                    tracing::warn!(key = %key.0, "journal: index references missing intent, skipping");
                }
            }
        }
        Ok(out)
    }

    /// List every intent currently `Awaiting` (spec §9.3) — a `DirectInflow` whose external
    /// payer has not settled. This is the resume loop's subscription-rehydration set: on
    /// restart it re-`subscribe`s each one's `recv_op` so the claim is still observed.
    ///
    /// DELIBERATELY separate from [`Journal::pending`]:
    /// an `Awaiting` intent must be re-FOUND after a restart but must NEVER be re-DRIVEN
    /// through `perform` (that would mint a second invoice). `pending()` therefore still
    /// returns `Pending|Executing` only; `awaiting()` is the parallel, re-drive-free scan.
    ///
    /// Poison-tolerant like every other scan (see [`Self::intents_indexed_as`]): one
    /// corrupt/dangling `Awaiting` row is skipped (warn-logged), NOT fatal — resume is the
    /// costliest place to hard-fail, since a single bad row would otherwise strand the
    /// rehydration of every OTHER healthy inflow. It still returns a `Result` so a transient
    /// storage error surfaces as [`ExecError::Retryable`] for the resume loop to retry.
    pub async fn awaiting(&self) -> Result<Vec<Intent>, ExecError> {
        self.intents_indexed_as(&[IntentStatus::Awaiting], false)
            .await
    }

    // --- app-specific async methods (NOT part of the wallet-core Journal trait) ---

    /// Read the derived [`MoveRecord`] cached for `key` (spec §5), if any.
    ///
    /// Surfaces failures via `Result`: a momentary storage error is
    /// [`ExecError::Retryable`] (the resume loop, §9.1, retries) and a decode error is
    /// [`ExecError::Permanent`].
    pub async fn get_move(&self, key: &IdempotencyKey) -> Result<Option<MoveRecord>, ExecError> {
        let raw_key = move_key(key);
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? else {
            return Ok(None);
        };
        Ok(Some(decode_row_result("move record", &raw_key, &bytes)?))
    }

    /// Upsert a standalone derived [`MoveRecord`] cache. Intent-backed records must use
    /// [`Self::put_move_if_attempt`], so a late writer cannot recreate an old attempt's cache
    /// after a manual retry has reset it.
    pub async fn put_move(&self, rec: &MoveRecord) -> Result<(), ExecError> {
        let value = encode_row(rec)?;
        let mut dbtx = self.db.begin_transaction().await;
        if dbtx
            .raw_get_bytes(&intent_key(&rec.key))
            .await
            .map_err(db_err)?
            .is_some()
        {
            return Err(ExecError::Permanent(
                "journal: intent-backed MoveRecord requires put_move_if_attempt".to_owned(),
            ));
        }
        dbtx.raw_insert_bytes(&move_key(&rec.key), &value)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Upsert an intent-backed derived [`MoveRecord`] only while `expected_attempt` still owns
    /// `key`. `Ok(false)` means the cache was not retained because the attempt mismatched, the
    /// intent was terminal, or a structural-evacuation marker owned the row. If a post-write
    /// ownership check retires this attempt, it restores the exact pre-write cache only for that
    /// same retired attempt (or removes this call's row when there was no prior cache). A missing
    /// or newer attempt always loses its cache: callers must not recreate the cache that a manual
    /// retry deliberately removed for a newer attempt.
    pub async fn put_move_if_attempt(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        rec: &MoveRecord,
    ) -> Result<bool, ExecError> {
        if rec.key != *key {
            return Err(ExecError::Permanent(
                "journal: MoveRecord key does not match attempt fence".to_owned(),
            ));
        }
        let value = encode_row(rec)?;
        let mut dbtx = self.db.begin_transaction().await;
        let ikey = intent_key(key);
        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
            return Ok(false);
        };
        let intent: Intent = decode_row_result("intent", &ikey, &bytes)?;
        if intent.idempotency_key != *key
            || intent.attempt != expected_attempt
            || matches!(intent.status, IntentStatus::Done | IntentStatus::Failed)
            // A structural evacuation marker is exclusively planner-owned and pre-artifact.
            // A late driver/backfill must not poison either its replacement or one-cycle clear;
            // a legitimate retry claim consumes this marker atomically before any cache write.
            || intent.evacuation_refusal.is_some()
        {
            return Ok(false);
        }
        #[cfg(test)]
        self.wait_replacement_write_rendezvous_for_test(key).await;
        // Claim the intent row in this transaction even though its bytes do
        // not change.  The MoveRecord cache is derived state, but a structural
        // replacement retires the intent in the same database: without this
        // write-write fence, two snapshot transactions can both observe
        // Pending and commit a retired parent plus its late artifact.
        dbtx.raw_insert_bytes(&ikey, &bytes).await.map_err(db_err)?;
        let mkey = move_key(key);
        // This is derived state, but it may be a useful prior artifact from a concurrent durable
        // recovery.  The post-write ownership fence below must undo only our own overwrite, never
        // blindly delete that prior row.
        let prior_move_bytes = dbtx.raw_get_bytes(&mkey).await.map_err(db_err)?;
        dbtx.raw_insert_bytes(&mkey, &value).await.map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        #[cfg(test)]
        {
            self.wait_replacement_write_rendezvous_for_test(key).await;
            clear_replacement_write_rendezvous_for_test(&self.replacement_write_rendezvous, key);
        }
        #[cfg(test)]
        self.wait_after_move_write_for_test(key).await;
        // A replacement can have retired this parent between our Pending read
        // and this derived-row commit on stores whose snapshot transactions do
        // not report a same-row write conflict. Re-read durably and undo only
        // OUR bytes if we lost that race; callers then see the normal
        // stale-writer `false`, never a terminal parent carrying executable
        // artifacts or a deleted prior same-attempt cache. An attempt-N+1/manual
        // retry must instead retain its cache deletion.
        let mut verify = self.db.begin_transaction().await;
        #[derive(Clone, Copy)]
        enum OwnershipAfterWrite {
            Current,
            /// A terminal result or planner-owned refusal marker for this very attempt preserves
            /// its prior cache. Both retire the driver before a cache may be retained.
            RetiredSameAttempt,
            /// A missing/retried row must not regain stale N state and never restores a prior row.
            Other,
        }
        let ownership = match verify.raw_get_bytes(&ikey).await.map_err(db_err)? {
            Some(current) => {
                let current: Intent = decode_row_result("intent", &ikey, &current)?;
                if current.idempotency_key != *key || current.attempt != expected_attempt {
                    OwnershipAfterWrite::Other
                } else if matches!(current.status, IntentStatus::Done | IntentStatus::Failed)
                    || current.evacuation_refusal.is_some()
                {
                    OwnershipAfterWrite::RetiredSameAttempt
                } else {
                    OwnershipAfterWrite::Current
                }
            }
            None => OwnershipAfterWrite::Other,
        };
        if !matches!(ownership, OwnershipAfterWrite::Current)
            && verify.raw_get_bytes(&mkey).await.map_err(db_err)?.as_ref() == Some(&value)
        {
            // Raw-byte equality is this call's local writer token. The registered single per-key
            // driver/awaiter plus ExternalTerminalMutationLease, or a standalone tick's exclusive
            // database ownership, means another legitimate writer cannot ABA these bytes between
            // this write and cleanup. A future multi-writer protocol must carry an explicit writer
            // token instead.
            match (ownership, prior_move_bytes) {
                (OwnershipAfterWrite::RetiredSameAttempt, Some(prior)) => verify
                    .raw_insert_bytes(&mkey, &prior)
                    .await
                    .map_err(db_err)?,
                _ => verify.raw_remove_entry(&mkey).await.map_err(db_err)?,
            };
        }
        verify.commit_tx_result().await.map_err(db_err)?;
        if !matches!(ownership, OwnershipAfterWrite::Current) {
            return Ok(false);
        }
        #[cfg(test)]
        if self
            .fail_after_move_writes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Retryable(
                "injected error after durable MoveRecord write".to_owned(),
            ));
        }
        Ok(true)
    }

    /// Read an immutable evacuation supersession relation by either the retired parent key or the
    /// live child key.  A reverse row that cannot be decoded, or that points at a missing/mismatched
    /// canonical row, is corruption and fails closed rather than silently hiding audit history.
    pub async fn evacuation_supersession(
        &self,
        key: &IdempotencyKey,
    ) -> Result<Option<EvacuationSupersessionRecord>, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        let canonical = evacuation_supersession_key(key);
        if let Some(bytes) = dbtx.raw_get_bytes(&canonical).await.map_err(db_err)? {
            let row: EvacuationSupersessionRecord =
                decode_row_result("evacuation supersession", &canonical, &bytes)?;
            if row.old_key != *key {
                return Err(ExecError::Permanent(
                    "journal: evacuation supersession canonical key has mismatched endpoint".into(),
                ));
            }
            validate_complete_supersession(&mut dbtx, &row).await?;
            return Ok(Some(row));
        }
        let reverse = evacuation_supersession_reverse_key(key);
        let Some(bytes) = dbtx.raw_get_bytes(&reverse).await.map_err(db_err)? else {
            return Ok(None);
        };
        let old: IdempotencyKey =
            decode_row_result("evacuation supersession reverse", &reverse, &bytes)?;
        let old_key = evacuation_supersession_key(&old);
        let row_bytes = dbtx
            .raw_get_bytes(&old_key)
            .await
            .map_err(db_err)?
            .ok_or_else(|| {
                ExecError::Permanent(
                    "journal: supersession reverse index points at a missing canonical row".into(),
                )
            })?;
        let row: EvacuationSupersessionRecord =
            decode_row_result("evacuation supersession", &old_key, &row_bytes)?;
        if row.old_key != old || row.new_key != *key {
            return Err(ExecError::Permanent(
                "journal: incoherent evacuation supersession reverse index".into(),
            ));
        }
        validate_complete_supersession(&mut dbtx, &row).await?;
        Ok(Some(row))
    }

    /// Read only the immediate canonical successor of `key`.
    ///
    /// Unlike [`Self::evacuation_supersession`], this intentionally does not fall back to the
    /// reverse index.  Exchange confirmation asks whether the *attempted parent* acquired its
    /// child, so a predecessor (`A -> B` while confirming an uncommitted `B -> C`) is not evidence
    /// that the attempted exchange committed.  A present canonical relation remains strict: its
    /// endpoint and reverse half must both be coherent in this snapshot.
    pub(crate) async fn evacuation_canonical_successor(
        &self,
        key: &IdempotencyKey,
    ) -> Result<Option<EvacuationSupersessionRecord>, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        evacuation_canonical_successor_in_tx(&mut dbtx, key).await
    }

    /// Read the predecessor and successor independently.  In particular, this preserves both
    /// links for a replacement that is later replaced itself (`A -> B -> C`).  The older
    /// [`Self::evacuation_supersession`] API remains available for callers that deliberately need
    /// a relation by either endpoint.
    pub async fn evacuation_supersession_neighbors(
        &self,
        key: &IdempotencyKey,
    ) -> Result<EvacuationSupersessionNeighbors, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        evacuation_supersession_neighbors_in_tx(&mut dbtx, key).await
    }

    /// Read one bounded display page of supersession neighbors from one snapshot.  Sidecars are
    /// audit augmentation, not the ledger itself: malformed or half-written sidecars therefore
    /// degrade to absent links for this display page, while the strict reader continues to reject
    /// them at atomic replacement/confirmation boundaries. Storage faults still fail the read.
    pub async fn evacuation_supersession_neighbors_for_display_keys(
        &self,
        keys: &[IdempotencyKey],
    ) -> Result<BTreeMap<IdempotencyKey, EvacuationSupersessionNeighbors>, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        let mut links = BTreeMap::new();
        for key in keys {
            if links.contains_key(key) {
                continue;
            }
            match evacuation_supersession_neighbors_in_tx(&mut dbtx, key).await {
                Ok(neighbors) => {
                    links.insert(key.clone(), neighbors);
                }
                Err(ExecError::Permanent(error)) => {
                    tracing::warn!(
                        key = %key.0,
                        %error,
                        "ignoring malformed evacuation supersession sidecar in presentation"
                    );
                    links.insert(key.clone(), EvacuationSupersessionNeighbors::default());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(links)
    }

    /// Read the intent linked to one operation row for a DISPLAY projection.
    ///
    /// `show` resolves the ledger row first; this read only AUGMENTS it with the live linked
    /// status and any structural-refusal marker.  So, exactly like the bulk sidecar projection
    /// above, a MALFORMED intent row degrades to absent with a `warn!` instead of blanking the
    /// ledger row an operator asked for mid-incident.  Storage faults still fail the read: they
    /// are retryable, and answering a transient fault with "no marker" would be a false display.
    /// Every money path keeps the strict [`Journal::get`].
    pub async fn intent_for_display(
        &self,
        key: &IdempotencyKey,
    ) -> Result<Option<Intent>, ExecError> {
        match self.read_intent(key).await {
            Ok(intent) => Ok(intent),
            Err(ExecError::Permanent(error)) => {
                tracing::warn!(
                    key = %key.0,
                    %error,
                    "ignoring malformed linked intent row in presentation"
                );
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Prove whether every key owned by a prospective replacement child is
    /// absent.  A reported exchange failure is uncommitted only after this
    /// exact check returns [`ReplacementChildNamespace::Pristine`].
    pub(crate) async fn replacement_child_namespace(
        &self,
        key: &IdempotencyKey,
    ) -> Result<ReplacementChildNamespace, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        Ok(if child_namespace_is_empty(&mut dbtx, key).await? {
            ReplacementChildNamespace::Pristine
        } else {
            ReplacementChildNamespace::Contaminated
        })
    }

    /// Consume one planner-owned structural refusal marker only if every durable identity field
    /// still exactly matches the shadow that produced the disposition. This leaves the evacuation
    /// Pending and writes no supersession relation: ordinary reconciliation may retry it on the
    /// next normal cycle, but this commit never starts a driver or policy wake.
    pub(crate) async fn clear_marked_evacuation_if_pending(
        &self,
        planned_parent: &Intent,
    ) -> Result<bool, ExecError> {
        #[cfg(test)]
        if self
            .fail_before_marker_clears_permanently
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Permanent(
                "injected permanent pre-marker-clear refusal".to_owned(),
            ));
        }
        #[cfg(test)]
        if self
            .fail_before_marker_clears
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Retryable(
                "injected pre-marker-clear fault".to_owned(),
            ));
        }
        let planned_parent = planned_parent.clone();
        let cleared = self
            .db
            .autocommit(
                move |dbtx, _| {
                    let planned_parent = planned_parent.clone();
                    Box::pin(async move {
                        let old_key = &planned_parent.idempotency_key;
                        let ikey = intent_key(old_key);
                        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
                            return Ok(false);
                        };
                        let mut old: Intent = decode_row_result("intent", &ikey, &bytes)?;
                        if old != planned_parent {
                            return Ok(false);
                        }
                        let Action::Evacuate { from, .. } = &old.action else {
                            return Ok(false);
                        };
                        if old.status != IntentStatus::Pending
                            || !matches!(old.actor, Actor::Agent { .. })
                            || old.evacuation_refusal.is_none()
                            || old.operation_id.is_some()
                            || old.invoice.is_some()
                        {
                            return Ok(false);
                        }
                        // A sidecar means this parent may have a child, and another live source
                        // holder means this is no longer the planner's exclusive class. Neither is
                        // a harmless stale disposition.
                        if dbtx
                            .raw_get_bytes(&evacuation_supersession_key(old_key))
                            .await
                            .map_err(db_err)?
                            .is_some()
                        {
                            return Ok(false);
                        }
                        ensure_no_other_live_agent_evacuation_holder(dbtx, old_key, *from).await?;
                        if let Some(bytes) = dbtx
                            .raw_get_bytes(&move_key(old_key))
                            .await
                            .map_err(db_err)?
                        {
                            let record: MoveRecord =
                                decode_row_result("move record", &move_key(old_key), &bytes)?;
                            if !replaceable_evacuation_record_is_pristine(&old, &record) {
                                return Err(ExecError::Permanent(
                                    "journal: marker clear found committed or incoherent move artifacts"
                                        .into(),
                                ));
                            }
                        }
                        old.evacuation_refusal = None;
                        dbtx.raw_insert_bytes(&ikey, &encode_row(&old)?)
                            .await
                            .map_err(db_err)?;
                        Ok(true)
                    })
                },
                None,
            )
            .await
            .map_err(|error| match error {
                AutocommitError::CommitFailed { last_error, .. } => db_err(last_error),
                AutocommitError::ClosureError { error, .. } => error,
            })?;
        #[cfg(test)]
        if cleared
            && self
                .fail_after_marker_clears
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Err(ExecError::Retryable(
                "injected post-marker-clear fault".to_owned(),
            ));
        }
        Ok(cleared)
    }

    /// Atomically retire exactly the marker-bearing pending agent evacuation and create one fresh
    /// child.
    ///
    /// `ServiceActor::commit_tick` and the exclusive-DB
    /// `Runtime::replace_marked_evacuation_standalone` call this only after their
    /// policy/world/balance/goal authority checks and generation fencing. It is deliberately
    /// crate-private so no API or scheduler path can bypass that authority.
    pub(crate) async fn replace_marked_evacuation(
        &self,
        old_key: &IdempotencyKey,
        old_attempt: u32,
        evidence: &EvacuationRefusalEvidence,
        fresh: &AllocatorDecision,
        now_ms: u64,
        planned_parent: &Intent,
    ) -> Result<bool, ExecError> {
        #[cfg(test)]
        if self
            .fail_before_evacuation_replacements
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Retryable(
                "journal: injected error before evacuation replacement".to_owned(),
            ));
        }
        let old_key = old_key.clone();
        let evidence = evidence.clone();
        let fresh = fresh.clone();
        let planned_parent = planned_parent.clone();
        #[cfg(test)]
        let replacement_write_rendezvous = self.replacement_write_rendezvous.clone();
        let exchanged = self
            .db
            .autocommit(
                |dbtx, _| {
                    let old_key = old_key.clone();
                    let evidence = evidence.clone();
                    let fresh = fresh.clone();
                    let planned_parent = planned_parent.clone();
                    #[cfg(test)]
                    let replacement_write_rendezvous = replacement_write_rendezvous.clone();
                    Box::pin(async move {
                        let old_ikey = intent_key(&old_key);
                        let Some(old_bytes) =
                            dbtx.raw_get_bytes(&old_ikey).await.map_err(db_err)?
                        else {
                            return Ok(false);
                        };
                        let mut old: Intent =
                            decode_row_result("intent", &old_ikey, &old_bytes)?;
                        let sidecar_key = evacuation_supersession_key(&old_key);
                        let (old_from, old_components) = match &old.action {
                            Action::Evacuate {
                                from,
                                fee_cap_components,
                                ..
                            } => (*from, *fee_cap_components),
                            _ => {
                                return Err(ExecError::Permanent(
                                    "journal: marked replacement requires an Evacuate parent".into(),
                                ))
                            }
                        };
                        let (new_from, new_components, new_amount, new_fee_cap) = match &fresh.action {
                            Action::Evacuate {
                                from,
                                amount,
                                fee_cap,
                                fee_cap_components,
                                ..
                            } => (*from, *fee_cap_components, *amount, *fee_cap),
                            _ => {
                                return Err(ExecError::Permanent(
                                    "journal: marked replacement requires an Evacuate child".into(),
                                ))
                            }
                        };
                        let old_cap = old_components.unwrap_or(wallet_core::EvacFeeCap {
                            base_msat: match &old.action {
                                Action::Evacuate { fee_cap, .. } => *fee_cap,
                                _ => unreachable!("Evacuate parent was checked above"),
                            },
                            bps: 0,
                        });
                        if let Actor::Agent {
                            occurrence: old_occurrence,
                        } = old.actor
                        {
                            if fresh.occurrence <= old_occurrence
                                || fresh.idempotency_key == old_key
                            {
                                return Err(ExecError::Permanent(
                                    crate::service::replacement_occurrence_error(
                                        old_occurrence,
                                        fresh.occurrence,
                                    ),
                                ));
                            }
                        }
                        if let Some(sidecar_bytes) =
                            dbtx.raw_get_bytes(&sidecar_key).await.map_err(db_err)?
                        {
                            let sidecar: EvacuationSupersessionRecord = decode_row_result(
                                "evacuation supersession",
                                &sidecar_key,
                                &sidecar_bytes,
                            )?;
                            validate_complete_supersession(dbtx, &sidecar).await?;
                            validate_marked_evacuation_evidence(
                                &old,
                                old_cap,
                                &sidecar.refusal,
                                new_components,
                                new_amount,
                                new_fee_cap,
                            )?;
                            if evidence != sidecar.refusal
                                || !supersession_relation_matches_request(
                                    &sidecar,
                                    &old,
                                    &fresh,
                                )
                            {
                                return Err(ExecError::Permanent(
                                    "journal: evacuation already has a different successor".into(),
                                ));
                            }
                            if old.status != IntentStatus::Failed
                                || old.attempt != old_attempt
                                || old.evacuation_refusal.as_ref() != Some(&sidecar.refusal)
                            {
                                return Err(ExecError::Permanent(
                                    "journal: supersession replay found an incoherent retired parent"
                                        .into(),
                                ));
                            }
                            let child_key = intent_key(&sidecar.new_key);
                            let child_bytes = dbtx
                                .raw_get_bytes(&child_key)
                                .await
                                .map_err(db_err)?
                                .ok_or_else(|| {
                                    ExecError::Permanent(
                                        "journal: supersession replay is missing its child intent"
                                            .into(),
                                    )
                                })?;
                            let child: Intent =
                                decode_row_result("intent", &child_key, &child_bytes)?;
                            validate_replayed_supersession_child(
                                &child,
                                &fresh,
                                sidecar.superseded_at_ms,
                            )?;
                            validate_intent_indexes_and_ledger_identity(dbtx, &old).await?;
                            validate_intent_indexes_and_ledger_identity(dbtx, &child).await?;
                            return Ok(true);
                        }
                        // This is an exact CAS on the complete row captured by the
                        // planner, not a handful of fields which happen to govern
                        // replacement today.  A changed action, actor, diagnostic,
                        // timestamp, or artifact reference means this exchange was
                        // not planned against the durable parent now in the store.
                        // A pre-existing complete sidecar is the separately
                        // validated idempotent replay path above.
                        if old != planned_parent {
                            return Ok(false);
                        }
                        validate_marked_evacuation_evidence(
                            &old,
                            old_cap,
                            &evidence,
                            new_components,
                            new_amount,
                            new_fee_cap,
                        )?;
                        let child = Intent::from_decision(
                            &fresh,
                            Actor::Agent {
                                occurrence: fresh.occurrence,
                            },
                            now_ms,
                        );
                        if old.attempt != old_attempt
                            || old.status != IntentStatus::Pending
                            || old.evacuation_refusal.as_ref() != Some(&evidence)
                            || !matches!(old.actor, Actor::Agent { .. })
                            || old_from != new_from
                            || fresh.idempotency_key == old_key
                            || fresh.occurrence
                                == match old.actor {
                                    Actor::Agent { occurrence } => occurrence,
                                    Actor::User => unreachable!(),
                                }
                            || old.operation_id.is_some()
                            || old.invoice.is_some()
                        {
                            return Ok(false);
                        }
                        ensure_no_other_live_agent_evacuation_holder(dbtx, &old_key, old_from)
                            .await?;
                        let new_ikey = intent_key(&fresh.idempotency_key);
                        ensure_child_namespace_empty(dbtx, &fresh.idempotency_key).await?;
                        if let Some(bytes) = dbtx
                            .raw_get_bytes(&move_key(&old_key))
                            .await
                            .map_err(db_err)?
                        {
                            let record: MoveRecord =
                                decode_row_result("move record", &move_key(&old_key), &bytes)?;
                            if !replaceable_evacuation_record_is_pristine(&old, &record) {
                                return Err(ExecError::Permanent(
                                    "journal: refused evacuation has committed or incoherent move artifacts"
                                        .into(),
                                ));
                            }
                        }
                        #[cfg(test)]
                        wait_replacement_write_rendezvous_for_test(
                            &replacement_write_rendezvous,
                            &old_key,
                        )
                        .await;

                        let old_status = old.status;
                        let old_occurrence = match old.actor {
                            Actor::Agent { occurrence } => occurrence,
                            Actor::User => unreachable!(),
                        };
                        old.status = IntentStatus::Failed;
                        let diagnostic = format!(
                            "superseded after measured structural evacuation refusal; successor {}",
                            fresh.idempotency_key.0
                        );
                        write_intent_and_index(
                            dbtx,
                            &old_ikey,
                            &old_key,
                            old_status,
                            &old,
                            now_ms,
                            Some(&diagnostic),
                        )
                        .await?;

                        dbtx.raw_insert_bytes(&new_ikey, &encode_row(&child)?)
                            .await
                            .map_err(db_err)?;
                        dbtx.raw_insert_bytes(
                            &pending_index_key(IntentStatus::Pending, &fresh.idempotency_key),
                            &[],
                        )
                        .await
                        .map_err(db_err)?;
                        // This is a distinct key, so append a distinct Started ledger identity in
                        // the same exchange transaction; OperationRecord v1 remains untouched.
                        write_intent_ledger_row(dbtx, &child, now_ms, None).await?;

                        let relation = EvacuationSupersessionRecord {
                            old_key: old_key.clone(),
                            old_attempt,
                            new_key: fresh.idempotency_key.clone(),
                            new_attempt: 0,
                            old_occurrence,
                            occurrence: fresh.occurrence,
                            source: old_from,
                            old_cap_components: old_components,
                            new_cap_components: new_components,
                            refusal: evidence,
                            superseded_at_ms: now_ms,
                        };
                        dbtx.raw_insert_bytes(&sidecar_key, &encode_row(&relation)?)
                            .await
                            .map_err(db_err)?;
                        dbtx.raw_insert_bytes(
                            &evacuation_supersession_reverse_key(&fresh.idempotency_key),
                            &encode_row(&old_key)?,
                        )
                        .await
                        .map_err(db_err)?;
                        Ok(true)
                    })
                },
                None,
            )
            .await
            .map_err(|e| match e {
                AutocommitError::CommitFailed { last_error, .. } => db_err(last_error),
                AutocommitError::ClosureError { error, .. } => error,
            })?;
        #[cfg(test)]
        {
            wait_replacement_write_rendezvous_for_test(&replacement_write_rendezvous, &old_key)
                .await;
            clear_replacement_write_rendezvous_for_test(&replacement_write_rendezvous, &old_key);
        }
        #[cfg(test)]
        if self
            .corrupt_after_evacuation_replacements
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            let mut dbtx = self.db.begin_transaction().await;
            dbtx.raw_remove_entry(&evacuation_supersession_key(&old_key))
                .await
                .map_err(db_err)?;
            dbtx.commit_tx_result().await.map_err(db_err)?;
        }
        #[cfg(test)]
        if self
            .fail_after_evacuation_replacements
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            if self
                .fail_confirmation_read_after_evacuation_replacements
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                *self
                    .fail_intent_read_after_successes
                    .lock()
                    .expect("intent read fault lock poisoned") = Some(0);
            }
            return Err(ExecError::Retryable(
                "journal: injected error after durable evacuation replacement".to_owned(),
            ));
        }
        Ok(exchanged)
    }

    /// Register (or update) a federation in the durable registry (spec §8/§9.1, ADR-0003).
    pub async fn put_federation(
        &self,
        id: &FederationId,
        info: &FederationInfo,
    ) -> Result<(), ExecError> {
        let value = encode_row(info)?;
        let mut dbtx = self.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&federation_key(id), &value)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Publish a recovered client partition and terminalize its owning intent in one journal
    /// transaction. This closes the final crash window: startup can never observe a registered,
    /// live recovered client while the recovery intent still looks executable and re-drive it
    /// into [`crate::MultiClient`]'s required refuse-if-registered guard.
    ///
    /// This is only the success commit. Recovery still has no durable in-progress marker or resume
    /// state: failures leave the fresh, unregistered partition inert as specified by D3/D4.
    pub async fn complete_recovery(
        &self,
        id: &FederationId,
        info: &FederationInfo,
        invite: &InviteCode,
        key: &IdempotencyKey,
        expected_attempt: u32,
    ) -> Result<bool, ExecError> {
        let now_ms = self.now_ms();
        // Autocommit-retry (like `set_status_if`), NOT a bare `begin_transaction`/`commit_tx`: this
        // is the terminal commit of an hours-long module-recovery replay, so a write-conflict with a
        // concurrent watch-cycle write to the same candidate/registry key must be RE-TRIED rather
        // than surfaced as an error that discards the whole replay (safe — the retry recovers into a
        // fresh prefix — but wasteful). The entire check-and-write runs inside one retried closure so
        // a loser re-reads state before re-applying. Snapshot the clock once so retries reuse it.
        self.db
            .autocommit(
                |dbtx, _| {
                    Box::pin(async move {
                        let ikey = intent_key(key);
                        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
                            return Err(ExecError::Permanent(
                                "journal: recovery intent not found".to_owned(),
                            ));
                        };
                        let mut intent = decode_row_result::<Intent>("intent", &ikey, &bytes)?;
                        let matches_recovery = matches!(
                            &intent.action,
                            Action::Recover {
                                federation,
                                invite: intent_invite,
                            } if federation == id && intent_invite == &info.invite
                        );
                        if !matches_recovery {
                            return Err(ExecError::Permanent(
                                "journal: recovery completion does not match its intent".to_owned(),
                            ));
                        }
                        if intent.attempt != expected_attempt
                            || intent.status != IntentStatus::Executing
                        {
                            return Ok(false);
                        }

                        intent.status = IntentStatus::Done;
                        dbtx.raw_insert_bytes(&federation_key(id), &encode_row(info)?)
                            .await
                            .map_err(db_err)?;
                        // Record durable USER ownership in the SAME transaction (D4.6). Without a
                        // `UserApproved` candidate row the recovered fed reads as an agent-discovered
                        // member, so `probe_gated_members` keeps it probe-gated and the allocator
                        // never spends from it. A deliberate recovery confers user ownership exactly
                        // like `join`; preserve an existing `UserApproved` (idempotent).
                        write_recovered_user_ownership(dbtx, id, invite, now_ms).await?;
                        write_intent_and_index(
                            dbtx,
                            &ikey,
                            key,
                            IntentStatus::Executing,
                            &intent,
                            now_ms,
                            None,
                        )
                        .await?;
                        Ok(true)
                    })
                },
                None,
            )
            .await
            .map_err(|e| match e {
                AutocommitError::CommitFailed { last_error, .. } => db_err(last_error),
                AutocommitError::ClosureError { error, .. } => error,
            })
    }

    /// Read a single federation's registry row.
    ///
    /// Surfaces failures via `Result` (see [`Self::get_move`]) so the resume loop (§9.1) can
    /// retry a transient storage hiccup instead of crashing the wallet. Unlike the bulk
    /// [`Self::list_federations`] (which SKIPS a poison row to keep other federations
    /// resumable), this targeted read surfaces a corrupt row as [`ExecError::Permanent`]: the
    /// caller asked for THIS id specifically and should learn it is unreadable.
    pub async fn get_federation(
        &self,
        id: &FederationId,
    ) -> Result<Option<FederationInfo>, ExecError> {
        let raw_key = federation_key(id);
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? else {
            return Ok(None);
        };
        Ok(Some(decode_row_result(
            "federation registry",
            &raw_key,
            &bytes,
        )?))
    }

    /// List every registered federation (the resume loop, §9.1, opens a client per entry).
    ///
    /// This gates re-opening EVERY client on resume, so it is POISON-TOLERANT like the index
    /// scans: a single malformed key or undecodable value is SKIPPED, never fatal — one bad
    /// registry row must not block resuming all the other (healthy, fund-holding)
    /// federations. Use [`Self::list_federations_report`] when the caller needs a structured
    /// count of skipped poison rows; this convenience method returns only the healthy rows.
    pub async fn list_federations(&self) -> Result<Vec<(FederationId, FederationInfo)>, ExecError> {
        Ok(self.list_federations_report().await?.federations)
    }

    /// List registered federations and report how many malformed/undecodable rows were
    /// skipped. A transient storage error on the scan itself is still
    /// [`ExecError::Retryable`] so the resume loop can retry the whole list operation.
    pub async fn list_federations_report(&self) -> Result<FederationListReport, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        let mut stream = dbtx
            .raw_find_by_prefix(&[TAG_FEDERATION])
            .await
            .map_err(db_err)?;
        let mut federations = Vec::new();
        let mut skipped_rows = 0;
        while let Some((raw_key, value)) = stream.next().await {
            // raw_key = [TAG_FEDERATION] ++ 32-byte FederationId.
            let Some(id) = raw_key.get(1..).and_then(|b| <[u8; 32]>::try_from(b).ok()) else {
                skipped_rows += 1;
                tracing::warn!(
                    ?raw_key,
                    "journal: skipping federation row with malformed key"
                );
                continue;
            };
            match decode_row_result::<FederationInfo>("federation registry", &raw_key, &value) {
                Ok(info) => federations.push((FederationId(id), info)),
                Err(e) => {
                    skipped_rows += 1;
                    tracing::warn!(?raw_key, error = ?e, "journal: skipping undecodable federation row");
                }
            }
        }
        Ok(FederationListReport {
            federations,
            skipped_rows,
        })
    }

    // --- standalone operation-ledger recording (spec §9.3, no intent involved) ---

    /// Create a `Started` ledger row for a raw/tick/join op keyed on a per-attempt,
    /// nonce-only `key` (§9.3/§10.1). Idempotent: a re-drive of the same key never appends a
    /// second row (the `0x06` guard). `fee_cap` seeds the fee breakdown; op-ids/fees are filled
    /// later by [`Self::record_update`]/[`Self::record_terminal`].
    pub async fn record_started(
        &self,
        key: &IdempotencyKey,
        kind: OperationKind,
        actor: Actor,
        reason: ReasonCode,
        now_ms: u64,
        fee_cap: Option<Msat>,
    ) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, key, |existing, seq| match existing {
            Some(_) => None,
            None => Some(OperationRecord {
                seq,
                correlation_key: key.clone(),
                kind,
                actor,
                reason,
                status: OperationStatus::Started,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                fees: FeeBreakdown {
                    fee_cap,
                    receive_fee: None,
                    send_fee_quoted: None,
                },
                error: None,
                repaired: false,
            }),
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Create or touch an active-probe umbrella row for a scheduler/manual invocation.
    /// Resumed probes keep the original correlation key, so `updated_at_ms` is the retry
    /// timestamp the watch scheduler uses for backoff.
    pub async fn record_probe_invocation(
        &self,
        key: &IdempotencyKey,
        kind: OperationKind,
        actor: Actor,
        now_ms: u64,
    ) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, key, |existing, seq| match existing {
            Some(existing) if existing.status.is_terminal() => None,
            Some(existing) => {
                let mut next = existing.clone();
                next.updated_at_ms = now_ms;
                Some(next)
            }
            None => Some(OperationRecord {
                seq,
                correlation_key: key.clone(),
                kind,
                actor,
                reason: ReasonCode::ActiveProbe,
                status: OperationStatus::Started,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                fees: FeeBreakdown::default(),
                error: None,
                repaired: false,
            }),
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Enrich a raw op's ledger row (§9.3): fill op-id/gateway/amount/hash/fees. When the op id
    /// first appears the row advances `Started → Awaiting` (the federation accepted the op — a
    /// distinct, surfaced state); otherwise it is a same-status enrichment (the post-parse
    /// amount+hash write before the SDK call). Uses the injected clock for `updated_at_ms`.
    pub async fn record_update(
        &self,
        key: &IdempotencyKey,
        upd: RawOpUpdate,
    ) -> Result<(), ExecError> {
        let now = self.now_ms();
        let mut dbtx = self.db.begin_transaction().await;
        reject_legacy_intent_backed_raw_writer(&mut dbtx, key, "record_update").await?;
        ledger_upsert_in(&mut dbtx, key, |existing, _seq| {
            let existing = existing?;
            // A repaired terminal is defeasible. Never feed the same terminal status back into
            // `advance` for a non-terminal update, or the authoritative write would clear
            // `repaired` while leaving the row terminal and immutable.
            let target = if upd.op_id.is_some()
                && (existing.status == OperationStatus::Started
                    || (existing.repaired && existing.status.is_terminal()))
            {
                OperationStatus::Awaiting
            } else if existing.repaired
                && existing.status == OperationStatus::Failed
                && raw_update_has_enrichment(&upd)
            {
                OperationStatus::Started
            } else if existing.repaired && existing.status.is_terminal() {
                return None;
            } else {
                existing.status
            };
            advance(
                &existing,
                target,
                now,
                Some(&upd),
                None,
                WriteKind::Authoritative,
            )
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Terminalize a raw op's ledger row (§9.3). The terminal write CARRIES the final
    /// enrichment (`upd`) — the definitive raw-op costs are only known AT settlement and
    /// terminal-immutability forbids enriching afterwards, so they land here, atomically with
    /// the transition. No-op if the key has no row or is already terminal.
    pub async fn record_terminal(
        &self,
        key: &IdempotencyKey,
        status: OperationStatus,
        now_ms: u64,
        error: Option<&str>,
        upd: Option<RawOpUpdate>,
    ) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        reject_legacy_intent_backed_raw_writer(&mut dbtx, key, "record_terminal").await?;
        ledger_upsert_in(&mut dbtx, key, |existing, _seq| {
            let existing = existing?;
            advance(
                &existing,
                status,
                now_ms,
                upd.as_ref(),
                error,
                WriteKind::Authoritative,
            )
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Persist a raw terminal observation only while `expected_attempt` still owns this public
    /// key.  In particular, an SDK result from attempt N must not terminalize the ledger row that
    /// a manual retry has replaced with attempt N+1.
    pub async fn record_raw_observation_if_attempt(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        op: OperationId,
        observation: &RawOpObservation,
    ) -> Result<bool, ExecError> {
        let update = RawOpUpdate {
            op_id: Some(op),
            gateway: observation.gateway.clone(),
            invoice_amount: observation.invoice_amount,
            payment_hash: observation.payment_hash,
            fees: Some(observation.fees),
            fees_definitive: observation.terminal.is_some(),
        };
        let (status, error) = match &observation.terminal {
            Some(terminal) => (
                if terminal.succeeded {
                    OperationStatus::Succeeded
                } else {
                    OperationStatus::Failed
                },
                terminal.error.as_deref(),
            ),
            // This sink is deliberately usable for an in-flight prelookup too.  It preserves the
            // same attempt fence even though the outcome is not terminal yet.
            None => (OperationStatus::Awaiting, None),
        };
        let now = self.now_ms();
        let ikey = intent_key(key);
        let mut dbtx = self.db.begin_transaction().await;
        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
            return Ok(false);
        };
        let intent: Intent = decode_row_result("intent", &ikey, &bytes)?;
        if intent.attempt != expected_attempt || intent_status_is_terminal(intent.status) {
            return Ok(false);
        }

        let mut applied = false;
        ledger_upsert_in(&mut dbtx, key, |existing, _seq| {
            let existing = existing?;
            let (fed, existing_op, _) = raw_row_parts(&existing.kind)?;
            let intent_fed = match &intent.action {
                Action::Pay { from, .. } => *from,
                Action::Receive { to, .. } => *to,
                _ => return None,
            };
            if fed != intent_fed || existing_op.is_some_and(|recorded| recorded != op) {
                return None;
            }
            // A crash can leave the ledger's authoritative conclusion durable while the intent
            // is still Executing/Pending.  Re-observing that exact operation/outcome is a
            // successful no-op, so the core driver can make the remaining intent transition.
            // A soft repair stays defeasible: authoritative evidence is still allowed to replace
            // it (including its definitive settlement fields).
            if existing.status.is_terminal() && !existing.repaired {
                if existing.status == status && existing_op == Some(op) {
                    applied = true;
                }
                return None;
            }
            let next = advance(
                &existing,
                status,
                now,
                Some(&update),
                error,
                WriteKind::Authoritative,
            );
            applied = next.is_some();
            next
        })
        .await?;
        if !applied {
            return Ok(false);
        }
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(true)
    }

    /// Atomically attach a raw SDK artifact to the attempt that created it.  The attempt and
    /// non-terminal checks protect a retried public key; the operation-id check makes the artifact
    /// monotonic rather than letting a late SDK response replace a durable identity.
    pub async fn set_operation_artifact_if_attempt(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        operation_id: OperationId,
        invoice: Option<&wallet_core::Invoice>,
    ) -> Result<bool, ExecError> {
        let ikey = intent_key(key);
        let now = self.now_ms();
        let mut dbtx = self.db.begin_transaction().await;
        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
            return Ok(false);
        };
        let mut intent = decode_row_result::<Intent>("intent", &ikey, &bytes)?;
        if intent.attempt != expected_attempt || intent_status_is_terminal(intent.status) {
            return Ok(false);
        }
        if intent
            .operation_id
            .is_some_and(|recorded| recorded != operation_id)
        {
            return Ok(false);
        }
        // Normally both artifacts move together.  The sole exception is crash convergence: a
        // current, exact terminal raw row already proves this operation concluded, so attach a
        // missing matching artifact to the still-nonterminal intent without rewriting that
        // immutable ledger conclusion.
        let index_key = ledger_key_index(key);
        let Some(index) = dbtx.raw_get_bytes(&index_key).await.map_err(db_err)? else {
            return Ok(false);
        };
        let Some(seq) = read_be64(&index) else {
            return Ok(false);
        };
        let row_key = ledger_row_key(seq);
        let Some(row_bytes) = dbtx.raw_get_bytes(&row_key).await.map_err(db_err)? else {
            return Ok(false);
        };
        let row = decode_canonical_ledger_row(&row_key, &row_bytes)?;
        let Some((fed, recorded_op, _)) = raw_row_parts(&row.kind) else {
            return Ok(false);
        };
        let intent_fed = match &intent.action {
            Action::Pay { from, .. } => *from,
            Action::Receive { to, .. } => *to,
            _ => return Ok(false),
        };
        if row.correlation_key != *key
            || row.seq != seq
            || fed != intent_fed
            || recorded_op.is_some_and(|recorded| recorded != operation_id)
        {
            return Ok(false);
        }
        let terminal_convergence = row.status.is_terminal();
        if terminal_convergence && recorded_op != Some(operation_id) {
            return Ok(false);
        }

        intent.operation_id = Some(operation_id);
        if let Some(invoice) = invoice {
            intent.invoice = Some(invoice.clone());
        }
        dbtx.raw_insert_bytes(&ikey, &encode_row(&intent)?)
            .await
            .map_err(db_err)?;
        if !terminal_convergence {
            write_intent_ledger_row(&mut dbtx, &intent, now, None).await?;
        }
        dbtx.commit_tx_result().await.map_err(db_err)?;
        #[cfg(test)]
        if self
            .fail_after_artifact_writes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Retryable(
                "injected error after durable operation artifact write".to_owned(),
            ));
        }
        Ok(true)
    }

    /// Record whether a join created membership or merely reopened an existing federation.
    /// The ledger transition precedes the intent's terminal status so a crash cannot erase the
    /// `newly_joined` distinction.
    pub async fn record_join_outcome(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        newly_joined: bool,
    ) -> Result<bool, ExecError> {
        let now = self.now_ms();
        let mut dbtx = self.db.begin_transaction().await;
        let ikey = intent_key(key);
        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
            return Ok(false);
        };
        let intent: Intent = decode_row_result("intent", &ikey, &bytes)?;
        let Action::Join { federation, .. } = &intent.action else {
            return Ok(false);
        };
        if intent.idempotency_key != *key
            || intent.attempt != expected_attempt
            || intent_status_is_terminal(intent.status)
        {
            return Ok(false);
        }

        let mut applied = false;
        ledger_upsert_in(&mut dbtx, key, |existing, _seq| {
            let existing = existing?;
            if existing.correlation_key != *key
                || !matches!(existing.kind, OperationKind::Join { fed } if fed == *federation)
            {
                return None;
            }
            // A crash after the authoritative ledger outcome committed but before core
            // terminalized the intent may re-drive this same attempt. Its already-authoritative
            // Succeeded row is idempotent. A repaired terminal, by contrast, is only a
            // defeasible reconciliation conclusion and `advance` must receive this
            // authoritative outcome to supersede it.
            if existing.status == OperationStatus::Succeeded && !existing.repaired {
                applied = true;
                return None;
            }
            // Non-repaired terminals are immutable. A repaired Succeeded or Failed terminal gets
            // the Authoritative `advance` below, which clears its repair marker and replaces any
            // stale repair diagnostic with this actual join outcome.
            if existing.status.is_terminal() && !existing.repaired {
                return None;
            }
            let next = advance(
                &existing,
                OperationStatus::Succeeded,
                now,
                None,
                (!newly_joined).then_some(JOIN_NOOP_REOPEN_NOTE),
                WriteKind::Authoritative,
            );
            applied = next.is_some();
            next
        })
        .await?;
        if !applied {
            return Ok(false);
        }
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(true)
    }

    /// Observe and correlate an externally-awaited raw operation before entering the service
    /// actor's terminal-mutation lease.  This is deliberately the only half that consults the
    /// SDK/op-log; the returned preparation is later committed by
    /// [`Self::finalize_raw_operation`] with database work only.
    #[allow(clippy::too_many_arguments)]
    pub async fn prepare_raw_operation_terminal(
        &self,
        oracle: &dyn LedgerRepairOracle,
        fed: FederationId,
        op: OperationId,
        key: &IdempotencyKey,
        expected_attempt: u32,
        role: RawOperationRole,
    ) -> Result<PreparedRawOperationTerminal, ExecError> {
        let Some(row) = self.operation(&OperationRef::Key(key.clone())).await? else {
            return Ok(PreparedRawOperationTerminal {
                notes: vec![format!("no ledger row for --key {}; not recording", key.0)],
                expected_attempt,
                update: None,
                fence: None,
                observed_status: None,
            });
        };
        let Some(fence) = self.capture_raw_repair_fence(&row, role).await? else {
            return Ok(PreparedRawOperationTerminal {
                notes: vec![format!(
                    "--key {} changed before terminal preparation; not recording",
                    key.0
                )],
                expected_attempt,
                update: None,
                fence: None,
                observed_status: None,
            });
        };
        if fence.expected_attempt != Some(expected_attempt) {
            return Ok(PreparedRawOperationTerminal {
                notes: vec![format!(
                    "--key {} no longer belongs to awaiting attempt {expected_attempt}; \
                     not recording",
                    key.0
                )],
                expected_attempt,
                update: None,
                fence: None,
                observed_status: None,
            });
        }
        let needs_correlation_proof = match raw_operation_row_matches(&row, role, fed, op) {
            Ok(needs_proof) => needs_proof,
            Err(reason) => {
                return Ok(PreparedRawOperationTerminal {
                    notes: vec![format!(
                        "--key {} does not match this operation ({reason}); not recording",
                        key.0
                    )],
                    expected_attempt,
                    update: None,
                    fence: None,
                    observed_status: None,
                });
            }
        };
        if needs_correlation_proof {
            match oracle
                .find_op_by_correlation_key(fed, &fence.attempt_correlation_key)
                .await
            {
                Ok(Some(found)) if found == op => {}
                Ok(_) => {
                    return Ok(PreparedRawOperationTerminal {
                        notes: vec![format!(
                            "--key {} has no recorded op id and the op-log does not tie this \
                              operation to it; not recording (reconcile repairs it)",
                            key.0
                        )],
                        expected_attempt,
                        update: None,
                        fence: None,
                        observed_status: None,
                    });
                }
                Err(error) => {
                    return Ok(PreparedRawOperationTerminal {
                        notes: vec![format!(
                            "could not verify --key {} against the op-log: {error:?}; not recording",
                            key.0
                        )],
                        expected_attempt,
                        update: None,
                        fence: None,
                        observed_status: None,
                    });
                }
            }
        }

        // An observation is the terminal outcome and its definitive settlement enrichment, not
        // merely an optional fee quote.  If it cannot be read, leave both rows non-terminal and
        // surface the failure so the awaiter/reconcile loop retries rather than terminalizing an
        // unobserved operation.
        let observation = oracle.observe_op(fed, op).await?;
        let Some(terminal) = observation.terminal else {
            return Err(ExecError::Retryable(format!(
                "raw operation {:?} for --key {} is still in flight",
                op.0, key.0
            )));
        };
        let observed_status = if terminal.succeeded {
            OperationStatus::Succeeded
        } else {
            OperationStatus::Failed
        };
        let update = RawOpUpdate {
            op_id: Some(op),
            gateway: observation.gateway,
            invoice_amount: observation.invoice_amount,
            payment_hash: observation.payment_hash,
            fees: Some(observation.fees),
            fees_definitive: true,
        };
        Ok(PreparedRawOperationTerminal {
            notes: Vec::new(),
            expected_attempt,
            update: Some(update),
            fence: Some(fence),
            observed_status: Some(observed_status),
        })
    }

    /// Commit a raw terminal observation prepared before the external terminal-mutation lease.
    /// This performs only journal/intent database writes; in particular it never awaits SDK or
    /// network I/O while the lease is live.
    pub async fn finalize_raw_operation(
        &self,
        key: &IdempotencyKey,
        status: OperationStatus,
        error: Option<&str>,
        prepared: PreparedRawOperationTerminal,
    ) -> Result<Vec<String>, ExecError> {
        let PreparedRawOperationTerminal {
            notes,
            expected_attempt,
            update,
            fence,
            observed_status,
        } = prepared;
        let (Some(update), Some(fence), Some(observed_status)) = (update, fence, observed_status)
        else {
            return self
                .raw_terminal_noop_is_stale(key, expected_attempt, notes, "preparation")
                .await;
        };
        if status != observed_status {
            return Err(ExecError::Permanent(format!(
                "raw terminal status {:?} conflicts with observed {:?}",
                status, observed_status
            )));
        }
        // A prepared result is only valid for the exact ledger attempt and intent attempt read
        // before the SDK observation.  This transaction intentionally performs no I/O other than
        // DB work; a ledger failure aborts before the intent write, so callers cannot report a
        // released reservation without a terminal audit row.
        if self
            .finalize_raw_terminal_if_fenced(key, status, error, &update, &fence)
            .await?
        {
            return Ok(notes);
        }
        // A failed fence is only benign when another attempt won, the intent disappeared, or it
        // is already terminal.  If this exact attempt still owns a non-terminal reservation, it
        // needs another await/reconcile pass rather than silently losing its only subscription.
        self.raw_terminal_noop_is_stale(key, expected_attempt, notes, "fence")
            .await
    }

    /// A raw terminal finalizer may benignly lose a stale attempt, but it must never silently
    /// abandon the same durable non-terminal reservation.  The explicit prepare attempt is the
    /// correlation point even when preparation could not construct a ledger fence.
    async fn raw_terminal_noop_is_stale(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        notes: Vec<String>,
        stage: &str,
    ) -> Result<Vec<String>, ExecError> {
        if self.get(key).await?.is_some_and(|intent| {
            intent.attempt == expected_attempt && !intent_status_is_terminal(intent.status)
        }) {
            return Err(ExecError::Retryable(format!(
                "raw terminal {stage} no-op left attempt {expected_attempt} for --key {} \
                 non-terminal; retrying ownership",
                key.0
            )));
        }
        Ok(notes)
    }

    async fn finalize_raw_terminal_if_fenced(
        &self,
        key: &IdempotencyKey,
        status: OperationStatus,
        error: Option<&str>,
        update: &RawOpUpdate,
        fence: &RawRepairFence,
    ) -> Result<bool, ExecError> {
        let Some(expected_attempt) = fence.expected_attempt else {
            return Ok(false);
        };
        let intent_status = match status {
            OperationStatus::Succeeded => IntentStatus::Done,
            OperationStatus::Failed => IntentStatus::Failed,
            OperationStatus::Started | OperationStatus::Awaiting => return Ok(false),
        };
        let now = self.now_ms();
        let mut dbtx = self.db.begin_transaction().await;
        let ikey = intent_key(key);
        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
            return Ok(false);
        };
        let mut intent: Intent = decode_row_result("intent", &ikey, &bytes)?;
        let role_matches = matches!(
            (&intent.action, fence.role),
            (Action::Pay { .. }, RawOperationRole::Send)
                | (Action::Receive { .. }, RawOperationRole::Receive)
        );
        if intent.attempt != expected_attempt
            || !role_matches
            || intent_status_is_terminal(intent.status)
            || !intent_status_transition_allowed(intent.status, intent_status)
        {
            return Ok(false);
        }
        let action_fed = match &intent.action {
            Action::Pay { from, .. } => *from,
            Action::Receive { to, .. } => *to,
            _ => unreachable!("role_matches admits only raw actions"),
        };
        if action_fed != fence.fed {
            return Ok(false);
        }
        // The terminal observation is authoritative about the SDK operation that completed.
        // Adopt it before changing intent status so a failed Pay cannot be manually retried into
        // the same completed SDK operation.  A different durable operation belongs to another
        // attempt and is a benign stale no-op.
        if let Some(observed_op) = update.op_id {
            match intent.operation_id {
                Some(recorded) if recorded != observed_op => return Ok(false),
                None => intent.operation_id = Some(observed_op),
                _ => {}
            }
        }

        let mut ledger_satisfied = false;
        ledger_upsert_in(&mut dbtx, key, |existing, seq| {
            let existing = existing?;
            let (fed, op_id, _) = raw_row_parts(&existing.kind)?;
            if existing.correlation_key != *key
                || seq != fence.expected_seq
                || fed != fence.fed
                || raw_role(&existing.kind) != Some(fence.role)
                || op_id != fence.expected_op
            {
                return None;
            }
            // A prior authoritative finalizer can commit the ledger half and crash before it
            // releases this intent.  Its ordinary terminal row is exactly the terminal evidence
            // this finalizer prepared, so preserve that immutable audit row and complete only the
            // matching intent half in this same transaction.  A repaired terminal remains
            // defeasible: `advance` below is the one authoritative replacement that clears its
            // repaired bit and records this observation.
            if existing.status == status && existing.status.is_terminal() && !existing.repaired {
                ledger_satisfied = true;
                return None;
            }
            if existing.status != fence.expected_ledger_status {
                return None;
            }
            let next = advance(
                &existing,
                status,
                now,
                Some(update),
                error,
                WriteKind::Authoritative,
            );
            ledger_satisfied = next.is_some();
            next
        })
        .await?;
        if !ledger_satisfied {
            return Ok(false);
        }
        let old_status = intent.status;
        intent.status = intent_status;
        write_intent_and_index(&mut dbtx, &ikey, key, old_status, &intent, now, error).await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(true)
    }

    /// Open a `Tick` ledger row `Started` before the agent decides (§9.3). Idempotent per
    /// `tick:<occurrence>:<nonce>` key.
    pub async fn record_tick_started(
        &self,
        key: &IdempotencyKey,
        occurrence: Occurrence,
        now_ms: u64,
    ) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, key, |existing, seq| match existing {
            Some(_) => None,
            None => Some(OperationRecord {
                seq,
                correlation_key: key.clone(),
                kind: OperationKind::Tick {
                    occurrence,
                    decisions: 0,
                    performed: 0,
                    failed: 0,
                },
                actor: Actor::Agent { occurrence },
                reason: ReasonCode::StandingInstruction,
                status: OperationStatus::Started,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                fees: FeeBreakdown::default(),
                error: None,
                repaired: false,
            }),
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Terminalize the `Tick` row with an explicit `status` (`Succeeded`/`Failed`) + `counts`
    /// and, on a bail path, the diagnostic `error` (§9.3/§10.4). A bail path lands `Failed`
    /// with zero-or-partial counts — a boolean flag could only fake it as a successful tick.
    pub async fn record_tick_terminal(
        &self,
        key: &IdempotencyKey,
        counts: Option<(u32, u32, u32)>,
        status: OperationStatus,
        error: Option<&str>,
        now_ms: u64,
    ) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, key, |existing, _seq| {
            let existing = existing?;
            let mut next = advance(
                &existing,
                status,
                now_ms,
                None,
                error,
                WriteKind::Authoritative,
            )?;
            if let (
                Some((d, p, f)),
                OperationKind::Tick {
                    decisions,
                    performed,
                    failed,
                    ..
                },
            ) = (counts, &mut next.kind)
            {
                *decisions = d;
                *performed = p;
                *failed = f;
            }
            Some(next)
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// One terminal `Refusal` row per advisory `RefuseInflow` decision (§9.3), keyed by its
    /// EXISTING `refuse:` idempotency key — so re-ticks of the same occurrence dedup via `0x06`
    /// automatically. A refusal is the durable answer to "why didn't the wallet act?"; it is a
    /// completed advisory fact (`Succeeded`, immutable), and the `reason` carries the why.
    pub async fn record_refusals(
        &self,
        decisions: &[AllocatorDecision],
        occurrence: Occurrence,
        now_ms: u64,
    ) -> Result<(), ExecError> {
        self.record_refusals_with_note(decisions, occurrence, now_ms, None)
            .await
    }

    /// Record advisory refusals while preserving their allocator diagnostics and,
    /// when supplied, an additional non-diagnostic audit note.
    pub async fn record_refusals_with_note(
        &self,
        decisions: &[AllocatorDecision],
        occurrence: Occurrence,
        now_ms: u64,
        note: Option<&str>,
    ) -> Result<(), ExecError> {
        for decision in decisions {
            let Action::RefuseInflow {
                fed, diagnostics, ..
            } = &decision.action
            else {
                continue;
            };
            let fed = *fed;
            let diagnostics = *diagnostics;
            let reason = decision.reason;
            let key = &decision.idempotency_key;
            let mut dbtx = self.db.begin_transaction().await;
            ledger_upsert_in(&mut dbtx, key, |existing, seq| match existing {
                Some(_) => None,
                None => Some(OperationRecord {
                    seq,
                    correlation_key: key.clone(),
                    kind: OperationKind::Refusal { fed, diagnostics },
                    actor: Actor::Agent { occurrence },
                    reason,
                    status: OperationStatus::Succeeded,
                    created_at_ms: now_ms,
                    updated_at_ms: now_ms,
                    fees: FeeBreakdown::default(),
                    error: note.map(str::to_owned),
                    repaired: false,
                }),
            })
            .await?;
            dbtx.commit_tx_result().await.map_err(db_err)?;
        }
        Ok(())
    }

    /// Record an executable allocator decision that was valid at plan time but failed the
    /// actor's current-state admission recheck at commit time.
    pub async fn record_tick_dropped_refusal(
        &self,
        decision: &AllocatorDecision,
        occurrence: Occurrence,
        now_ms: u64,
        message: &str,
        conflict_suppressed: bool,
    ) -> Result<(), ExecError> {
        let (fed, amount) = match &decision.action {
            Action::Move { to, amount, .. } => (*to, Some(*amount)),
            Action::Evacuate { from, amount, .. } => (*from, Some(*amount)),
            Action::DirectInflow { to, .. } | Action::Receive { to, .. } => (*to, None),
            Action::Pay { from, .. } => (*from, None),
            Action::RefuseInflow { fed, .. } => (*fed, None),
            Action::Join { federation, .. } | Action::Recover { federation, .. } => {
                (*federation, None)
            }
        };
        let conflict_suppressed = conflict_suppressed && amount.is_some_and(|amount| amount.0 > 0);
        let key = IdempotencyKey(format!(
            "tick-drop:{}:{}",
            occurrence.0, decision.idempotency_key.0
        ));
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, &key, |existing, seq| match existing {
            Some(_) => None,
            None => Some(OperationRecord {
                seq,
                correlation_key: key.clone(),
                // A commit-time admission drop of an EXECUTABLE decision — not an
                // allocator refusal — so there is no shortfall arithmetic to carry; the
                // `error` field below records why it was dropped. A nonzero conflict suppression
                // records its emitted zero and observational discriminator, so the row alone
                // distinguishes it from a genuine zero-sized refusal.
                kind: OperationKind::Refusal {
                    fed,
                    diagnostics: RefusalDiagnostics {
                        amount: conflict_suppressed.then_some(Msat(0)),
                        conflict_suppressed,
                        ..Default::default()
                    },
                },
                actor: Actor::Agent { occurrence },
                reason: decision.reason,
                status: OperationStatus::Succeeded,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                fees: FeeBreakdown::default(),
                error: Some(format!(
                    "commit-time admission refused {}: {message}",
                    decision.idempotency_key.0
                )),
                repaired: false,
            }),
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Record a completed non-money fact row (discover/autojoin/approve) in one dbtx. Idempotent
    /// per key: an existing row is left untouched, matching append-once ledger discipline.
    pub async fn record_terminal_operation(
        &self,
        key: &IdempotencyKey,
        kind: OperationKind,
        actor: Actor,
        reason: ReasonCode,
        now_ms: u64,
    ) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, key, |existing, seq| match existing {
            Some(_) => None,
            None => Some(OperationRecord {
                seq,
                correlation_key: key.clone(),
                kind,
                actor,
                reason,
                status: OperationStatus::Succeeded,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                fees: FeeBreakdown::default(),
                error: None,
                repaired: false,
            }),
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    // --- ledger scans (spec §9.3, poison-tolerant) ---

    /// Every canonically keyed, decodable ledger row, ascending by `seq`. Poison rows (including
    /// malformed keys and key/row sequence mismatches) are skipped + warned; a storage error surfaces.
    async fn scan_ledger_rows(&self) -> Result<Vec<OperationRecord>, ExecError> {
        Ok(self.scan_ledger_rows_report().await?.rows)
    }

    /// Operation-ledger scan with a report of skipped poison rows. The public history path
    /// remains poison-tolerant, but auto-join budget counters consume `skipped_rows` so corrupt
    /// ledger history cannot make hard caps fail open.
    async fn scan_ledger_rows_report(&self) -> Result<LedgerRowsReport, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        let mut stream = dbtx
            .raw_find_by_prefix(&[TAG_LEDGER_ROW])
            .await
            .map_err(db_err)?;
        let mut rows = Vec::new();
        let mut skipped_rows = 0;
        while let Some((raw_key, value)) = stream.next().await {
            match decode_canonical_ledger_row(&raw_key, &value) {
                Ok(rec) => rows.push(rec),
                Err(e) => {
                    skipped_rows += 1;
                    tracing::warn!(?raw_key, error = ?e, "journal: skipping undecodable ledger row")
                }
            }
        }
        Ok(LedgerRowsReport { rows, skipped_rows })
    }

    /// Newest-first ledger scan for `history` (§11): up to `limit` rows with `seq < before_seq`
    /// (when set). The `0x05` scan is ascending by `be64(seq)`, so a reverse suffices — the
    /// spec's only pagination mechanism (non-goal: no index beyond the seq scan).
    pub async fn history(
        &self,
        limit: usize,
        before_seq: Option<u64>,
    ) -> Result<Vec<OperationRecord>, ExecError> {
        let mut rows = self.scan_ledger_rows().await?;
        rows.reverse();
        Ok(rows
            .into_iter()
            .filter(|r| before_seq.is_none_or(|b| r.seq < b))
            .take(limit)
            .collect())
    }

    /// Newest-first time-windowed ledger rows needed by the watch probe scheduler. An unresolved
    /// probe remains visible regardless of age because its durable session can still resume and
    /// spend; only terminal probe history expires with the requested horizon.
    pub async fn probe_schedule_ledger_rows(
        &self,
        now_ms: u64,
        horizon_ms: u64,
    ) -> Result<Vec<OperationRecord>, ExecError> {
        Ok(self
            .probe_schedule_ledger_rows_report(now_ms, horizon_ms)
            .await?
            .rows)
    }

    /// Probe-budget reconstruction must fail closed: a skipped row could be an in-window
    /// automated probe attempt or spend that is required to enforce the hard weekly limits.
    pub(crate) async fn probe_budget_ledger_rows(
        &self,
        now_ms: u64,
        horizon_ms: u64,
    ) -> Result<Vec<OperationRecord>, ExecError> {
        let report = self
            .probe_schedule_ledger_rows_report(now_ms, horizon_ms)
            .await?;
        if report.skipped_rows != 0 {
            return Err(ExecError::Permanent(format!(
                "journal: cannot reconstruct probe budget: {} ledger row(s) were corrupt",
                report.skipped_rows
            )));
        }
        Ok(report.rows)
    }

    async fn probe_schedule_ledger_rows_report(
        &self,
        now_ms: u64,
        horizon_ms: u64,
    ) -> Result<LedgerRowsReport, ExecError> {
        let cutoff_ms = now_ms.saturating_sub(horizon_ms);
        let mut dbtx = self.db.begin_transaction_nc().await;
        let mut stream = dbtx
            .raw_find_by_prefix_sorted_descending(&[TAG_LEDGER_ROW])
            .await
            .map_err(db_err)?;
        let mut rows = Vec::new();
        let mut skipped_rows = 0;
        while let Some((raw_key, value)) = stream.next().await {
            match decode_canonical_ledger_row(&raw_key, &value) {
                Ok(rec) => {
                    let unresolved_probe = matches!(
                        &rec.kind,
                        OperationKind::Probe {
                            cost_msat: None,
                            ..
                        }
                    ) && !rec.status.is_terminal();
                    if rec.created_at_ms < cutoff_ms
                        && rec.updated_at_ms < cutoff_ms
                        && !unresolved_probe
                    {
                        continue;
                    }
                    rows.push(rec);
                }
                Err(e) => {
                    skipped_rows += 1;
                    tracing::warn!(?raw_key, error = ?e, "journal: skipping undecodable ledger row")
                }
            }
        }
        Ok(LedgerRowsReport { rows, skipped_rows })
    }

    /// Resolve a single ledger row by correlation key OR seq (§9.3, for `show`).
    pub async fn operation(
        &self,
        sel: &OperationRef,
    ) -> Result<Option<OperationRecord>, ExecError> {
        #[cfg(test)]
        if self
            .fail_operation_reads
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Retryable(
                "injected operation refresh read failure".to_owned(),
            ));
        }
        let mut dbtx = self.db.begin_transaction_nc().await;
        let seq = match sel {
            OperationRef::Seq(seq) => *seq,
            OperationRef::Key(key) => {
                let Some(seq_bytes) = dbtx
                    .raw_get_bytes(&ledger_key_index(key))
                    .await
                    .map_err(db_err)?
                else {
                    return Ok(None);
                };
                read_be64(&seq_bytes).ok_or_else(|| {
                    ExecError::Permanent(format!("journal: corrupt ledger seq index for {}", key.0))
                })?
            }
        };
        let row_key = ledger_row_key(seq);
        match dbtx.raw_get_bytes(&row_key).await.map_err(db_err)? {
            Some(bytes) => Ok(Some(decode_canonical_ledger_row(&row_key, &bytes)?)),
            None => Ok(None),
        }
    }

    // --- active-probe state (phase 5 §5.0.4) ---

    /// Read a federation's `0x08` probe row. TARGETED getter that FAILS CLOSED on an
    /// undecodable row (like `get`/`get_move`/`operation`): it decides whether a probe
    /// session is in flight, and a swallowed corrupt row would restart a probe that is
    /// already live, spending twice. Only SCANS are poison-tolerant.
    pub async fn probe_record(&self, fed: &FederationId) -> Result<Option<ProbeRecord>, ExecError> {
        let raw_key = probe_key(fed);
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? else {
            return Ok(None);
        };
        Ok(Some(decode_row_result("probe record", &raw_key, &bytes)?))
    }

    /// Write (or update) the fed's in-flight [`ProbeSession`] — the fresh path's opening
    /// write, and the sizing update that persists `out_net_msat` before leg OUT is
    /// journaled. Read-modify-write in one dbtx; fails closed on a corrupt row.
    pub async fn begin_probe_session(
        &self,
        fed: &FederationId,
        session: &ProbeSession,
    ) -> Result<(), ExecError> {
        let raw_key = probe_key(fed);
        let mut dbtx = self.db.begin_transaction().await;
        let mut rec = match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
            Some(bytes) => decode_row_result::<ProbeRecord>("probe record", &raw_key, &bytes)?,
            None => ProbeRecord::default(),
        };
        // A FRESH probe (a new nonce) must never clobber a DIFFERENT live session
        // (§5.0.5: resume runs FIRST, so a fresh caller reaching here with another
        // probe's `in_flight` set skipped resume) — overwriting would orphan the prior
        // session's legs + umbrella row. A SAME-nonce write is the legitimate in-place
        // update (persisting `out_net_msat` after sizing leg OUT, or a resume re-deriving
        // its own session), so it is allowed.
        if let Some(existing) = &rec.in_flight {
            if existing.nonce != session.nonce {
                return Err(ExecError::Permanent(format!(
                    "begin_probe_session: federation {} already has a different in-flight \
                     probe ({}); resume or finish it before starting a new one",
                    fed.to_hex(),
                    existing.nonce
                )));
            }
        }
        rec.in_flight = Some(session.clone());
        dbtx.raw_insert_bytes(&raw_key, &encode_row(&rec)?)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// The ONE terminal write for every probe exit after a session exists (§5.0.4), in ONE
    /// dbtx: clear `in_flight`, terminalize the umbrella `probe:` ledger row (create-or-
    /// advance — a crash between the session write and `record_started` leaves no row, and
    /// the resumed outcome must still land as history), and append the attempt when
    /// `attempt` is `Some` (leg outcomes; `None` for the no-attempt terminal exits, which
    /// ALSO clear their session here — a stale session must never survive a terminal exit).
    /// All parts commit or fail together, so the verdict history, the session, and
    /// `history`'s umbrella row can never disagree.
    ///
    /// `session_nonce` must match the currently in-flight session; otherwise this is a
    /// replay/stale finalizer and no history or ledger row is touched (`Ok(false)`).
    ///
    /// `kind` is the [`OperationKind::Probe`] with its FINAL `cost_msat` — used whole on
    /// the create path, and its cost is copied onto an advanced existing row (§5.0.5:
    /// cost is filled at terminalization). `Ok(true)` means the matching session was
    /// terminalized.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_probe_outcome(
        &self,
        fed: &FederationId,
        session_nonce: &str,
        attempt: Option<ProbeAttempt>,
        umbrella_key: &IdempotencyKey,
        kind: OperationKind,
        actor: Actor,
        status: OperationStatus,
        error: Option<&str>,
    ) -> Result<bool, ExecError> {
        let now = self.now_ms();
        let raw_key = probe_key(fed);
        let mut dbtx = self.db.begin_transaction().await;

        // Probe row: clear only the matching session, append + prune the attempt history.
        // A duplicate/out-of-order finalizer for an already-cleared nonce is an idempotent
        // replay; a different nonce belongs to a newer live probe and must not be cleared.
        let mut rec = match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
            Some(bytes) => decode_row_result::<ProbeRecord>("probe record", &raw_key, &bytes)?,
            None => ProbeRecord::default(),
        };
        match rec.in_flight.as_ref() {
            Some(session) if session.nonce == session_nonce => {}
            Some(session) => {
                tracing::warn!(
                    federation = %fed.to_hex(),
                    expected_nonce = %session_nonce,
                    active_nonce = %session.nonce,
                    "journal: ignoring stale probe outcome for a different active session"
                );
                return Ok(false);
            }
            None => {
                tracing::warn!(
                    federation = %fed.to_hex(),
                    expected_nonce = %session_nonce,
                    "journal: ignoring duplicate probe outcome for an already-cleared session"
                );
                return Ok(false);
            }
        }
        rec.in_flight = None;
        if let Some(attempt) = attempt {
            rec.attempts.push(attempt);
            rec.attempts = prune_probe_attempts(std::mem::take(&mut rec.attempts), now);
        }
        dbtx.raw_insert_bytes(&raw_key, &encode_row(&rec)?)
            .await
            .map_err(db_err)?;

        // Umbrella ledger row, same dbtx: create-or-advance to the terminal status, with
        // the final cost stamped onto the kind.
        let error_owned = error.map(str::to_owned);
        ledger_upsert_in(&mut dbtx, umbrella_key, |existing, seq| match existing {
            Some(existing) => {
                let mut next = advance(
                    &existing,
                    status,
                    now,
                    None,
                    error_owned.as_deref(),
                    WriteKind::Authoritative,
                )?;
                if let (
                    OperationKind::Probe { cost_msat, .. },
                    OperationKind::Probe {
                        cost_msat: final_cost,
                        ..
                    },
                ) = (&mut next.kind, &kind)
                {
                    *cost_msat = *final_cost;
                }
                Some(next)
            }
            None => Some(OperationRecord {
                seq,
                correlation_key: umbrella_key.clone(),
                kind,
                actor,
                reason: ReasonCode::ActiveProbe,
                status,
                created_at_ms: now,
                updated_at_ms: now,
                fees: FeeBreakdown::default(),
                error: error_owned,
                repaired: false,
            }),
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(true)
    }

    // --- candidate registry (phase 5 §5.1.1, tag 0x09) ---

    /// Upsert the `0x09` candidate row for its fed (§5.1.1). One row per fed, its own dbtx —
    /// the same write discipline as the probe/federation registries.
    pub async fn put_candidate(&self, rec: &CandidateRecord) -> Result<(), ExecError> {
        let raw_key = candidate_key(&rec.id);
        let mut next = rec.clone();
        let mut dbtx = self.db.begin_transaction().await;
        // `UserApproved` is durable user ownership; NO discovery refresh may demote it. A pass
        // reads a candidate (Discovered/AutoJoined), runs a slow network preview, then writes back
        // its now-stale copy — `Rejected` on a failed structural, `Discovered`/`AutoJoined`
        // otherwise (discovery.rs's preview and auto-join loops). Meanwhile `/v1/join` or
        // `/v1/approve` may have committed `UserApproved`. This in-transaction read is the freshest
        // view, so preserve the approval against EVERY stale non-`UserApproved` refresh, not just
        // the `AutoJoined` one. Other state transitions retain their existing semantics.
        if let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
            if let Ok(current) = decode_candidate_row(rec.id, &raw_key, &bytes) {
                if current.state == CandidateState::UserApproved
                    && rec.state != CandidateState::UserApproved
                {
                    next.state = CandidateState::UserApproved;
                    next.updated_at_ms = next.updated_at_ms.max(current.updated_at_ms);
                }
            }
        }
        let value = encode_row(&next)?;
        dbtx.raw_insert_bytes(&raw_key, &value)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Record ownership after a successful explicit user join. A user join promotes an absent,
    /// Discovered, Rejected, or unreadable candidate to `UserApproved`; an `AutoJoined` row stays
    /// agent-owned so only the audited `approve` verb can release its probe gate/concurrent slot.
    pub async fn mark_candidate_user_approved(
        &self,
        id: FederationId,
        invite: &InviteCode,
    ) -> Result<(), ExecError> {
        let now_ms = self.now_ms();
        let raw_key = candidate_key(&id);
        let fresh = CandidateRecord {
            id,
            invite: invite.clone(),
            source: DiscoverySource::Manual,
            discovered_at_ms: now_ms,
            structural: StructuralOutcome::Passed,
            structural_checked_at_ms: now_ms,
            state: CandidateState::UserApproved,
            updated_at_ms: now_ms,
        };
        let mut dbtx = self.db.begin_transaction().await;
        let next = match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
            Some(bytes) => match decode_candidate_row(id, &raw_key, &bytes) {
                Ok(current)
                    if matches!(
                        current.state,
                        CandidateState::AutoJoined | CandidateState::UserApproved
                    ) =>
                {
                    return Ok(())
                }
                Ok(mut current) => {
                    current.state = CandidateState::UserApproved;
                    current.updated_at_ms = now_ms;
                    current
                }
                // A successful explicit join carries enough authenticated id/invite evidence to
                // replace a poisoned candidate row instead of leaving the member probe-gated.
                Err(error) => {
                    tracing::warn!(
                        federation = %id.to_hex(),
                        ?error,
                        "journal: replacing unreadable candidate after successful user join"
                    );
                    fresh
                }
            },
            None => fresh,
        };
        dbtx.raw_insert_bytes(&raw_key, &encode_row(&next)?)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Read one federation's candidate row. TARGETED getter that FAILS CLOSED on an undecodable
    /// row (like `get_federation`/`probe_record`): the caller asked for THIS id and should learn
    /// it is unreadable. Only the bulk [`Self::list_candidates`] scan is poison-tolerant.
    pub async fn get_candidate(
        &self,
        id: &FederationId,
    ) -> Result<Option<CandidateRecord>, ExecError> {
        let raw_key = candidate_key(id);
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? else {
            return Ok(None);
        };
        Ok(Some(decode_candidate_row(*id, &raw_key, &bytes)?))
    }

    /// List every candidate row (§5.1.1), POISON-TOLERANT like the other registry scans: a
    /// malformed key or undecodable value is SKIPPED (warn-logged), never fatal — one corrupt
    /// candidate must not strand discovery of the rest. A transient storage error on the scan
    /// still surfaces as [`ExecError::Retryable`].
    pub async fn list_candidates(&self) -> Result<Vec<(FederationId, CandidateRecord)>, ExecError> {
        Ok(self.list_candidates_report().await?.candidates)
    }

    /// Candidate-registry scan with a report of skipped poison rows. This is the same
    /// poison-tolerant scan as [`Self::list_candidates`], but callers that need fail-closed
    /// behavior can conservatively account for `skipped_ids`.
    pub async fn list_candidates_report(&self) -> Result<CandidateListReport, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        let mut stream = dbtx
            .raw_find_by_prefix(&[TAG_CANDIDATE])
            .await
            .map_err(db_err)?;
        let mut candidates = Vec::new();
        let mut skipped_ids = BTreeSet::new();
        let mut skipped_rows = 0;
        let mut skipped_unidentified = 0;
        while let Some((raw_key, value)) = stream.next().await {
            // raw_key = [TAG_CANDIDATE] ++ 32-byte FederationId.
            let Some(id) = raw_key.get(1..).and_then(|b| <[u8; 32]>::try_from(b).ok()) else {
                // A malformed key hides the fed id, so recover it from the row VALUE: a
                // corrupt-key `AutoJoined` row must still fail closed against BOTH the funding
                // gate and the concurrent cap, not vanish (it would otherwise bypass the gate
                // and free a concurrent slot). If the value is ALSO undecodable the id is
                // unrecoverable — the gate cannot act, but it still counts fail-closed for the
                // cap via `skipped_unidentified`.
                skipped_rows += 1;
                match decode_row_result::<CandidateRecord>("candidate", &raw_key, &value) {
                    Ok(rec) => {
                        tracing::warn!(
                            ?raw_key,
                            id = %rec.id.to_hex(),
                            "journal: candidate row has a malformed key; recovered embedded id, counting fail-closed"
                        );
                        skipped_ids.insert(rec.id);
                    }
                    Err(e) => {
                        skipped_unidentified += 1;
                        tracing::warn!(?raw_key, error = ?e, "journal: skipping candidate row with malformed key and unrecoverable id");
                    }
                }
                continue;
            };
            let id = FederationId(id);
            match decode_candidate_row(id, &raw_key, &value) {
                Ok(rec) => candidates.push((id, rec)),
                Err(e) => {
                    skipped_rows += 1;
                    skipped_ids.insert(id);
                    tracing::warn!(?raw_key, error = ?e, "journal: skipping undecodable candidate row")
                }
            }
        }
        Ok(CandidateListReport {
            candidates,
            skipped_ids,
            skipped_rows,
            skipped_unidentified,
        })
    }

    /// Atomically approve an `AutoJoined` candidate (§5.1.4a): flip it to `UserApproved` and
    /// append the user-visible `Approve` ledger row in the same dbtx. Refuses every other state.
    pub async fn approve_auto_joined_candidate(
        &self,
        id: FederationId,
        key: &IdempotencyKey,
        now_ms: u64,
    ) -> Result<(), ExecError> {
        let raw_key = candidate_key(&id);
        let mut dbtx = self.db.begin_transaction().await;
        let bytes = dbtx
            .raw_get_bytes(&raw_key)
            .await
            .map_err(db_err)?
            .ok_or_else(|| {
                ExecError::Permanent(format!("candidate {} is not AutoJoined", id.to_hex()))
            })?;
        let mut candidate = decode_candidate_row(id, &raw_key, &bytes)?;
        if candidate.state != CandidateState::AutoJoined {
            return Err(ExecError::Permanent(format!(
                "candidate {} is {:?}, not AutoJoined",
                id.to_hex(),
                candidate.state
            )));
        }
        candidate.state = CandidateState::UserApproved;
        candidate.updated_at_ms = now_ms;
        dbtx.raw_insert_bytes(&raw_key, &encode_row(&candidate)?)
            .await
            .map_err(db_err)?;
        ledger_upsert_in(&mut dbtx, key, |existing, seq| match existing {
            Some(_) => None,
            None => Some(OperationRecord {
                seq,
                correlation_key: key.clone(),
                kind: OperationKind::Approve { fed: id },
                actor: Actor::User,
                reason: ReasonCode::UserInitiated,
                status: OperationStatus::Succeeded,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                fees: FeeBreakdown::default(),
                error: None,
                repaired: false,
            }),
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    // --- auto-join accounting (phase 5 §5.1.4) ---

    /// Total agent-created partitions EVER — the lifetime-cap count (§5.1.4). Reads the
    /// immutable ledger join history, NOT the mutable candidate state (§P1): the count of
    /// `actor: Agent` `join:` rows that SUCCEEDED and created a NEW partition, plus the same
    /// registry-backed non-terminal Agent join evidence used to recover a crash after the
    /// partition write. Monotonic, so approval (which leaves the partition in place) keeps
    /// counting and the finite-set guarantee holds. Undecodable ledger rows count fail-closed
    /// because any one may be a successful new-partition Agent join.
    pub async fn lifetime_auto_joins(&self) -> Result<u32, ExecError> {
        let report = self.scan_ledger_rows_report().await?;
        let mut counted = BTreeSet::new();
        for row in &report.rows {
            if is_agent_new_partition_join(row) {
                if let Some(fed) = join_row_fed(row) {
                    counted.insert(fed);
                }
            } else if let Some(fed) = self.registry_backed_non_terminal_agent_join(row).await? {
                counted.insert(fed);
            }
        }
        Ok(count_saturating_u32(
            counted.len().saturating_add(report.skipped_rows),
        ))
    }

    /// Agent-created partitions in the trailing 7 days — the weekly rate-cap count (§5.1.4):
    /// the same filter as [`Self::lifetime_auto_joins`], windowed on each join's
    /// `created_at_ms` (when the attempt began; a join Started and Succeeded near-instantly).
    /// Undecodable ledger rows cannot be windowed, so they count fail-closed until repaired.
    pub async fn weekly_auto_joins(&self, now_ms: u64) -> Result<u32, ExecError> {
        let report = self.scan_ledger_rows_report().await?;
        let mut counted = BTreeSet::new();
        for row in &report.rows {
            if now_ms.saturating_sub(row.created_at_ms) >= AUTO_JOIN_WEEKLY_WINDOW_MS {
                continue;
            }
            if is_agent_new_partition_join(row) {
                if let Some(fed) = join_row_fed(row) {
                    counted.insert(fed);
                }
            } else if let Some(fed) = self.registry_backed_non_terminal_agent_join(row).await? {
                counted.insert(fed);
            }
        }
        Ok(count_saturating_u32(
            counted.len().saturating_add(report.skipped_rows),
        ))
    }

    /// Whether durable evidence says this federation was created by the agent. Used to recover a
    /// crash after the partition was created but before the candidate row flipped to
    /// `AutoJoined`.
    ///
    /// A terminal successful Agent new-partition row is direct evidence. A non-terminal Agent
    /// join row is enough when the joined registry already contains the fed and the attempt began
    /// no later than the registry timestamp (with slack). That fails closed for slow joins that
    /// wrote the partition long after the Agent row was created, while still ignoring attempts
    /// that clearly started after a pre-existing membership.
    pub async fn agent_created_federation(&self, id: &FederationId) -> Result<bool, ExecError> {
        let report = self.scan_ledger_rows_report().await?;
        if report.rows.iter().any(|row| {
            matches!(row.kind, OperationKind::Join { fed } if fed == *id)
                && is_agent_new_partition_join(row)
        }) {
            return Ok(true);
        }

        for row in &report.rows {
            if self.registry_backed_non_terminal_agent_join(row).await? == Some(*id) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn registry_backed_non_terminal_agent_join(
        &self,
        row: &OperationRecord,
    ) -> Result<Option<FederationId>, ExecError> {
        let Some(fed) = join_row_fed(row) else {
            return Ok(None);
        };
        if !is_non_terminal_agent_join_for(row, fed) {
            return Ok(None);
        }
        let Some(info) = self.get_federation(&fed).await? else {
            return Ok(None);
        };
        Ok(
            join_attempt_could_have_created_registry_entry(row.created_at_ms, info.joined_at)
                .then_some(fed),
        )
    }

    /// Auto-joined candidates whose probe is not yet `Passed` — the concurrent-cap count
    /// (§5.1.4). Counts `0x09` rows with `state == AutoJoined` whose id is NOT in `passed`
    /// (the caller builds `passed` from the live probe verdicts). Counting live `AutoJoined`
    /// rows (one per real partition) keeps this free of attempt/no-op noise; unlike the
    /// lifetime cap, an APPROVED fed correctly leaves this count (it left the in-flight
    /// probing surface via the `AutoJoined -> UserApproved` transition).
    ///
    /// FAILS CLOSED on corruption, exactly like the runtime's `auto_joined_candidates` funding
    /// gate: an undecodable candidate row could be an unproven `AutoJoined` partition, so each
    /// skipped id (that has not since Passed) counts against the concurrent cap. Otherwise a
    /// single corrupt `AutoJoined` row would silently shrink the in-flight count and admit one
    /// auto-join past the cap. Rows whose id is unrecoverable (`skipped_unidentified`) cannot be
    /// Passed-filtered, so they count unconditionally — the fully-conservative direction.
    pub async fn concurrent_unproven(
        &self,
        passed: &BTreeSet<FederationId>,
    ) -> Result<u32, ExecError> {
        let report = self.list_candidates_report().await?;
        let live = report
            .candidates
            .iter()
            .filter(|(id, rec)| rec.state == CandidateState::AutoJoined && !passed.contains(id))
            .count();
        let skipped = report
            .skipped_ids
            .iter()
            .filter(|id| !passed.contains(id))
            .count();
        Ok(count_saturating_u32(
            live.saturating_add(skipped)
                .saturating_add(report.skipped_unidentified),
        ))
    }

    // --- reconcile repair (spec §10.3) ---

    /// Scan the FULL ledger for non-terminal (`Started`/`Awaiting`) rows and repair the stuck
    /// ones (§10.3). POSITIVE inferences (an op-log outcome; the registry contains the fed)
    /// apply immediately as ordinary terminal writes; NEGATIVE inferences (marking `Failed` on
    /// ABSENCE of evidence) are deferred one hour AND written SOFT (`repaired: true`), so a
    /// clock-skewed false `Failed` is superseded by the real writer instead of blocking it.
    /// Move-shaped intent rows are never repaired here — their journal integration (§9.2) owns
    /// them. Raw pay/receive intent rows are repaired from their lnv2 op-log witness below.
    pub async fn repair_ledger(
        &self,
        oracle: &dyn LedgerRepairOracle,
    ) -> Result<RepairSummary, ExecError> {
        let sink = DirectRawIntentTerminalSink { journal: self };
        self.repair_ledger_with_terminal_sink(oracle, &sink).await
    }

    /// As [`Self::repair_ledger`], but delegates only raw Pay/Receive terminal
    /// intent synchronization to `sink`.  The expensive ledger/op-log scan and
    /// ledger-row repair remain direct and off-actor.
    pub async fn repair_ledger_with_terminal_sink(
        &self,
        oracle: &dyn LedgerRepairOracle,
        sink: &dyn RawIntentTerminalSink,
    ) -> Result<RepairSummary, ExecError> {
        let now = self.now_ms();
        let rows = self.scan_ledger_rows().await?;
        let mut summary = RepairSummary::default();

        // `join:` rows arbitrate PER ATTEMPT against the membership registry (§10.3): registry
        // presence alone cannot bless every lingering attempt (a stale interrupted attempt must
        // not flip Succeeded because a LATER retry joined), so group by fed and arbitrate.
        // Terminal Succeeded attempts participate as ordering evidence: a later retry may have
        // already terminalized authoritatively, and older Started attempts must then fail as
        // superseded instead of becoming a second soft success.
        let mut join_by_fed: BTreeMap<FederationId, Vec<&OperationRecord>> = BTreeMap::new();
        for row in &rows {
            if let (KeyClass::Join, OperationKind::Join { fed }) =
                (classify_key(&row.correlation_key), &row.kind)
            {
                if !row.status.is_terminal() || row.status == OperationStatus::Succeeded {
                    join_by_fed.entry(*fed).or_default().push(row);
                }
            }
        }
        for (fed, mut attempts) in join_by_fed {
            attempts.sort_by_key(|r| (r.created_at_ms, r.seq));
            summary.repaired += self.repair_join_fed(fed, &attempts, now).await?;
        }

        // `pay:`/`recv:` rows repair from op-log evidence. `tick:` and discovery maintenance
        // rows have no external op-log witness, so stale non-terminal rows soft-fail after the
        // age gate instead of staying in-flight forever.
        for row in &rows {
            if row.status.is_terminal() {
                // A prior pass may have committed the ledger terminal but lost its actor sink
                // response.  Retry that intent synchronization only while this exact row remains
                // the key's current attempt; an older terminal row must never poison N+1.
                if classify_key(&row.correlation_key) == KeyClass::Raw {
                    let Some(role) = raw_role(&row.kind) else {
                        continue;
                    };
                    // This capture is the terminal-row retry's linearization point.  If a retry
                    // has selected N+1, do not even ask the sink to touch its reservation.
                    let Some(fence) = self.capture_raw_repair_fence(row, role).await? else {
                        continue;
                    };
                    if raw_terminal_repair_must_not_sink_intent(row) {
                        // `RAW_NEVER_REACHED` is the one deliberate no-evidence soft repair.
                        // Its terminal ledger diagnosis remains defeasible until an authoritative
                        // driver/artifact supersedes it; terminal-row retries must not turn that
                        // deliberately live intent into a terminal reservation.
                        continue;
                    }
                    let status = if row.status == OperationStatus::Succeeded {
                        IntentStatus::Done
                    } else {
                        IntentStatus::Failed
                    };
                    let Some(terminal_fence) =
                        fence.terminal_sink_fence(row.status, fence.expected_op)
                    else {
                        continue;
                    };
                    if let Err(error) = self
                        .sync_raw_intent_terminal(
                            &row.correlation_key,
                            &terminal_fence,
                            status,
                            row.error.clone(),
                            sink,
                        )
                        .await
                    {
                        // The terminal ledger row is already durable.  Keep the pass
                        // successful so the next scan can retry this sink without hiding
                        // the committed accounting advancement.
                        tracing::warn!(
                            key = %row.correlation_key.0,
                            ?error,
                            "journal: terminal raw intent synchronization failed; will retry"
                        );
                    }
                }
                continue;
            }
            match classify_key(&row.correlation_key) {
                KeyClass::Raw => match self.repair_raw(row, oracle, now, sink).await {
                    Ok(repaired) => summary.repaired += repaired,
                    Err(e) => {
                        tracing::warn!(
                            key = %row.correlation_key.0,
                            error = ?e,
                            "journal: raw ledger repair failed for one row; continuing"
                        );
                    }
                },
                KeyClass::Tick | KeyClass::Discovery => {
                    // A crash between a Started row and terminal write is otherwise unrepairable
                    // (later invocations use fresh nonces); age-gate keeps a live invocation's row
                    // safe from a concurrent reconcile.
                    if now.saturating_sub(row.created_at_ms) >= REPAIR_AGE_MS {
                        self.apply_repair(
                            &row.correlation_key,
                            OperationStatus::Failed,
                            now,
                            None,
                            Some(INTERRUPTED_NO_TERMINAL.to_owned()),
                            WriteKind::Repair,
                        )
                        .await?;
                        summary.repaired += 1;
                    }
                }
                // Join is handled above; move-shaped intent rows and other rows are untouched.
                KeyClass::Join | KeyClass::Other => {}
            }
        }
        Ok(summary)
    }

    /// Capture the attempt fence for a raw repair from one database snapshot.  The public
    /// idempotency key deliberately does not identify a retry in the SDK: the attempt's
    /// correlation key does.  Never read the intent after an op-log request, because a retry can
    /// replace both the selected ledger row and the intent attempt in that gap.
    async fn capture_raw_repair_fence(
        &self,
        scanned: &OperationRecord,
        role: RawOperationRole,
    ) -> Result<Option<RawRepairFence>, ExecError> {
        let key = &scanned.correlation_key;
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(index) = dbtx
            .raw_get_bytes(&ledger_key_index(key))
            .await
            .map_err(db_err)?
        else {
            return Ok(None);
        };
        if read_be64(&index) != Some(scanned.seq) {
            return Ok(None);
        }
        let row_key = ledger_row_key(scanned.seq);
        let Some(row_bytes) = dbtx.raw_get_bytes(&row_key).await.map_err(db_err)? else {
            return Ok(None);
        };
        let current = decode_canonical_ledger_row(&row_key, &row_bytes)?;
        let Some((scanned_fed, scanned_op, _)) = raw_row_parts(&scanned.kind) else {
            return Ok(None);
        };
        let Some((fed, op_id, _)) = raw_row_parts(&current.kind) else {
            return Ok(None);
        };
        // The index sequence alone is not sufficient: a concurrent writer may have enriched the
        // row with a different raw operation while preserving its sequence.
        if current.correlation_key != *key
            || current.seq != scanned.seq
            || current.status != scanned.status
            || raw_role(&current.kind) != Some(role)
            || fed != scanned_fed
            || op_id != scanned_op
        {
            return Ok(None);
        }

        let intent = match dbtx.raw_get_bytes(&intent_key(key)).await.map_err(db_err)? {
            Some(bytes) => Some(decode_row_result::<Intent>(
                "intent",
                &intent_key(key),
                &bytes,
            )?),
            None => None,
        };
        let (expected_attempt, attempt_correlation_key, intent_nonterminal) = match intent {
            Some(intent)
                if matches!(
                    (&intent.action, role),
                    (Action::Pay { .. }, RawOperationRole::Send)
                        | (Action::Receive { .. }, RawOperationRole::Receive)
                ) =>
            {
                let action_fed = match &intent.action {
                    Action::Pay { from, .. } => *from,
                    Action::Receive { to, .. } => *to,
                    _ => unreachable!("matches above admits only raw actions"),
                };
                if action_fed != fed {
                    return Ok(None);
                }
                (
                    Some(intent.attempt),
                    intent.operation_correlation_key(),
                    !intent_status_is_terminal(intent.status),
                )
            }
            Some(_) => return Ok(None),
            // Standalone raw ledger rows predate intent-backed rows and have no reservation to
            // sink.  Keep their repair behavior, while intent-backed repair is always fenced.
            None => (None, key.clone(), false),
        };
        Ok(Some(RawRepairFence {
            expected_seq: scanned.seq,
            expected_attempt,
            attempt_correlation_key,
            intent_nonterminal,
            fed,
            expected_op: op_id,
            role,
            expected_ledger_status: current.status,
        }))
    }

    /// Arbitrate the `join:` attempts (`attempts`, oldest-first) for one `fed` against the
    /// registry (§10.3). Non-terminal rows are the only rows written; terminal Succeeded rows are
    /// included only as ordering evidence so an older Started row is not blessed after a later
    /// retry already completed. Returns how many rows it repaired.
    async fn repair_join_fed(
        &self,
        fed: FederationId,
        attempts: &[&OperationRecord],
        now: u64,
    ) -> Result<usize, ExecError> {
        match self.get_federation(&fed).await? {
            Some(info) => {
                let in_window = || {
                    attempts
                        .iter()
                        .filter(|r| join_attempt_matches_joined_at(r.created_at_ms, info.joined_at))
                };
                let in_window_count = in_window().count();
                // Winner: an already-terminal successful retry is authoritative attempt-level
                // evidence and prevents creating a duplicate soft success. Otherwise, newest
                // attempt inside the window, else (backward clock jump) newest overall —
                // membership is registry-proven either way. `attempts` is sorted oldest-first.
                let terminal_success_winner = attempts
                    .iter()
                    .rev()
                    .find(|r| r.status == OperationStatus::Succeeded)
                    .map(|r| r.seq);
                let winner_seq = terminal_success_winner.or_else(|| {
                    in_window()
                        .next_back()
                        .or_else(|| attempts.last())
                        .map(|r| r.seq)
                });
                // Exactly one candidate → certain; zero or many → ambiguous, note it.
                let ambiguous = terminal_success_winner.is_none() && in_window_count != 1;
                let mut repaired = 0;
                for row in attempts {
                    if row.status.is_terminal() {
                        continue;
                    }
                    if Some(row.seq) == winner_seq {
                        self.apply_repair(
                            &row.correlation_key,
                            OperationStatus::Succeeded,
                            now,
                            None,
                            ambiguous.then(|| JOIN_AMBIGUOUS_NOTE.to_owned()),
                            WriteKind::Repair,
                        )
                        .await?;
                    } else {
                        self.apply_repair(
                            &row.correlation_key,
                            OperationStatus::Failed,
                            now,
                            None,
                            Some(JOIN_SUPERSEDED.to_owned()),
                            WriteKind::Repair,
                        )
                        .await?;
                    }
                    repaired += 1;
                }
                Ok(repaired)
            }
            None => {
                // Registry absent: soft-fail attempts older than 1h; leave fresh ones for a
                // later pass (they may be in flight in another process).
                let mut repaired = 0;
                for row in attempts {
                    if row.status.is_terminal() {
                        continue;
                    }
                    if now.saturating_sub(row.created_at_ms) >= REPAIR_AGE_MS {
                        self.apply_repair(
                            &row.correlation_key,
                            OperationStatus::Failed,
                            now,
                            None,
                            Some(JOIN_NOT_REGISTERED.to_owned()),
                            WriteKind::Repair,
                        )
                        .await?;
                        repaired += 1;
                    }
                }
                Ok(repaired)
            }
        }
    }

    /// Repair one non-terminal `pay:`/`recv:` row (§10.3). Returns 1 if it wrote, else 0.
    async fn repair_raw(
        &self,
        row: &OperationRecord,
        oracle: &dyn LedgerRepairOracle,
        now: u64,
        sink: &dyn RawIntentTerminalSink,
    ) -> Result<usize, ExecError> {
        let Some((fed, op_id, payment_hash)) = raw_row_parts(&row.kind) else {
            return Ok(0);
        };
        let Some(role) = raw_role(&row.kind) else {
            return Ok(0);
        };
        let Some(fence) = self.capture_raw_repair_fence(row, role).await? else {
            return Ok(0);
        };
        let key = &row.correlation_key;
        match op_id {
            // Awaiting with a known op id (the common stuck case: crash after `record_update`,
            // or the user never ran `await-* --key`): read the op-log outcome directly.
            Some(op) => {
                let obs = oracle.observe_op(fed, op).await?;
                if obs.terminal.is_some() {
                    // A row whose op id was ADOPTED by an earlier hash-dedup pass (its error still
                    // carries HASH_DEDUP_NOTE, written while the op was in flight) is still an
                    // UNCERTAIN attempt-level attribution at settlement: terminalizing it as a
                    // clean authoritative `Succeeded` would let `advance` shed the note, so history
                    // would silently claim certainty it never had. Keep it SOFT and re-carry the
                    // note so the audit trail stays truthful (§10.3). A genuinely op-id-tracked row
                    // (the common crash-after-`record_update` case) is authoritative with no note.
                    let adopted_by_hash = row
                        .error
                        .as_deref()
                        .is_some_and(|e| e.starts_with(HASH_DEDUP_NOTE));
                    let (write, note) = if adopted_by_hash {
                        (WriteKind::Repair, Some(HASH_DEDUP_NOTE))
                    } else {
                        (WriteKind::Authoritative, None)
                    };
                    if self
                        .apply_observation_if_current(&fence, key, op, &obs, now, write, note)
                        .await?
                    {
                        if let Err(error) = self
                            .sync_raw_intent_from_observation(key, &fence, op, &obs, sink)
                            .await
                        {
                            tracing::warn!(key = %key.0, ?error, "journal: raw repair sink failed; will retry");
                        }
                        return Ok(1);
                    }
                    return Ok(0);
                }
                // Still in flight → leave Awaiting (truthful) for a later pass.
                Ok(0)
            }
            None => {
                // 1. The primary backfill: find the op by its `correlation_key` in `custom_meta`.
                if let Some(op) = oracle
                    .find_op_by_correlation_key(fed, &fence.attempt_correlation_key)
                    .await?
                {
                    let obs = oracle.observe_op(fed, op).await?;
                    if self
                        .apply_observation_if_current(
                            &fence,
                            key,
                            op,
                            &obs,
                            now,
                            WriteKind::Authoritative,
                            None,
                        )
                        .await?
                    {
                        if let Err(error) = self
                            .sync_raw_intent_from_observation(key, &fence, op, &obs, sink)
                            .await
                        {
                            tracing::warn!(key = %key.0, ?error, "journal: raw repair sink failed; will retry");
                        }
                        return Ok(1);
                    }
                    return Ok(0);
                }
                // 2. A deduped retry reuses the ORIGINAL op, so its key is in no op's meta; the
                //    durably-written payment hash is the recovery link (pay rows only).
                if let Some(hash) = payment_hash {
                    if let Some(op) = oracle.find_send_op_by_payment_hash(fed, hash).await? {
                        let obs = oracle.observe_op(fed, op).await?;
                        // A hash may resolve an original SDK attempt, but it does not prove that a
                        // later manual retry owns that operation while it is in flight or if it
                        // failed. The current retry's correlation key (or an already-recorded
                        // current op) remains authoritative; only a hash-only terminal success can
                        // settle the paid invoice. Leave N+1 live rather than adopting/sinking N's
                        // in-flight or failed operation.
                        if fence.expected_attempt.is_some_and(|attempt| attempt > 0)
                            && !matches!(obs.terminal.as_ref(), Some(terminal) if terminal.succeeded)
                        {
                            return Ok(0);
                        }
                        // Attempt attribution is uncertain (deduped retry OR never-sent
                        // attempt), so this is a SOFT correlation with the ambiguity recorded.
                        if self
                            .apply_observation_if_current(
                                &fence,
                                key,
                                op,
                                &obs,
                                now,
                                WriteKind::Repair,
                                Some(HASH_DEDUP_NOTE),
                            )
                            .await?
                        {
                            if let Err(error) = self
                                .sync_raw_intent_from_observation(key, &fence, op, &obs, sink)
                                .await
                            {
                                tracing::warn!(key = %key.0, ?error, "journal: raw repair sink failed; will retry");
                            }
                            return Ok(1);
                        }
                        return Ok(0);
                    }
                }
                // 3. Nothing found: after 1h, a NEGATIVE inference — soft-`Failed` (truthful at
                //    attempt granularity: a no-hash row was malformed or crashed pre-parse).
                if now.saturating_sub(row.created_at_ms) >= REPAIR_AGE_MS {
                    let applied = self
                        .apply_raw_repair_if_current(
                            &fence,
                            key,
                            OperationStatus::Failed,
                            now,
                            None,
                            Some(RAW_NEVER_REACHED.to_owned()),
                            WriteKind::Repair,
                        )
                        .await?;
                    // Absence of evidence is deliberately SOFT. Keep the intent re-drivable:
                    // the next authoritative Pending→Executing claim will supersede this repaired
                    // ledger conclusion if a late operation appears or a retry reaches the SDK.
                    return Ok(usize::from(applied));
                }
                Ok(0)
            }
        }
    }

    async fn sync_raw_intent_from_observation(
        &self,
        key: &IdempotencyKey,
        fence: &RawRepairFence,
        op: OperationId,
        observation: &RawOpObservation,
        sink: &dyn RawIntentTerminalSink,
    ) -> Result<(), ExecError> {
        let Some(terminal) = &observation.terminal else {
            return Ok(());
        };
        let status = if terminal.succeeded {
            OperationStatus::Succeeded
        } else {
            OperationStatus::Failed
        };
        let Some(terminal_fence) = fence.terminal_sink_fence(status, Some(op)) else {
            return Ok(());
        };
        self.sync_raw_intent_terminal(
            key,
            &terminal_fence,
            if terminal.succeeded {
                IntentStatus::Done
            } else {
                IntentStatus::Failed
            },
            terminal.error.clone(),
            sink,
        )
        .await
    }

    async fn sync_raw_intent_terminal(
        &self,
        key: &IdempotencyKey,
        fence: &RawIntentTerminalFence,
        status: IntentStatus,
        error: Option<String>,
        sink: &dyn RawIntentTerminalSink,
    ) -> Result<(), ExecError> {
        // The fence was captured in the same snapshot that proved this ledger row current.  The
        // sink rechecks it atomically with the intent write, so neither a retry N+1 nor an
        // authoritative same-sequence supersession can be terminalized by this old observation.
        let _ = sink.set_raw_terminal(key, fence, status, error).await?;
        Ok(())
    }

    /// Apply a repair observation only if the correlation-key index still selects the exact row
    /// scanned by this pass and that row still belongs to the observed operation.  A manual retry
    /// moves the index to a new sequence; a delayed N observation then returns `false` without
    /// touching N+1's ledger row or intent.
    #[allow(clippy::too_many_arguments)]
    async fn apply_observation_if_current(
        &self,
        fence: &RawRepairFence,
        key: &IdempotencyKey,
        op: OperationId,
        obs: &RawOpObservation,
        now: u64,
        write: WriteKind,
        note: Option<&str>,
    ) -> Result<bool, ExecError> {
        let upd = RawOpUpdate {
            op_id: Some(op),
            gateway: obs.gateway.clone(),
            invoice_amount: obs.invoice_amount,
            payment_hash: obs.payment_hash,
            fees: Some(obs.fees),
            fees_definitive: obs.terminal.is_some(),
        };
        let (status, term_error) = match &obs.terminal {
            Some(t) => (
                if t.succeeded {
                    OperationStatus::Succeeded
                } else {
                    OperationStatus::Failed
                },
                t.error.clone(),
            ),
            None => (OperationStatus::Awaiting, None),
        };
        let error = combine_note(note, term_error);
        let mut applied = false;
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, key, |existing, seq| {
            let existing = existing?;
            let (current_fed, current_op, _) = raw_row_parts(&existing.kind)?;
            if seq != fence.expected_seq
                || current_fed != fence.fed
                || raw_role(&existing.kind) != Some(fence.role)
                || current_op != fence.expected_op
                || existing.status != fence.expected_ledger_status
            {
                return None;
            }
            let next = advance(&existing, status, now, Some(&upd), error.as_deref(), write);
            applied = next.is_some();
            next
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(applied)
    }

    /// The negative (no-op-log-evidence) repair is just as stale-sensitive as a positive
    /// observation.  It must not turn a retry's current row into Failed merely because an older
    /// scan became old enough while the operator retried it.
    #[allow(clippy::too_many_arguments)]
    async fn apply_raw_repair_if_current(
        &self,
        fence: &RawRepairFence,
        key: &IdempotencyKey,
        status: OperationStatus,
        now: u64,
        upd: Option<RawOpUpdate>,
        error: Option<String>,
        write: WriteKind,
    ) -> Result<bool, ExecError> {
        let mut applied = false;
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, key, |existing, seq| {
            let existing = existing?;
            let (fed, op_id, _) = raw_row_parts(&existing.kind)?;
            if seq != fence.expected_seq
                || fed != fence.fed
                || raw_role(&existing.kind) != Some(fence.role)
                || op_id != fence.expected_op
                || existing.status != fence.expected_ledger_status
            {
                return None;
            }
            let next = advance(
                &existing,
                status,
                now,
                upd.as_ref(),
                error.as_deref(),
                write,
            );
            applied = next.is_some();
            next
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(applied)
    }

    /// One repair write in its own dbtx: re-read the CURRENT row inside the dbtx and re-apply
    /// [`advance`], so a row that changed since the scan is handled correctly (a terminal row
    /// no-ops, terminal-immutability holds). `write == Repair` marks a written terminal
    /// defeasible (`repaired: true`).
    async fn apply_repair(
        &self,
        key: &IdempotencyKey,
        status: OperationStatus,
        now: u64,
        upd: Option<RawOpUpdate>,
        error: Option<String>,
        write: WriteKind,
    ) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        ledger_upsert_in(&mut dbtx, key, |existing, _seq| {
            let existing = existing?;
            advance(
                &existing,
                status,
                now,
                upd.as_ref(),
                error.as_deref(),
                write,
            )
        })
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    // --- watch scheduler state (phase 5 §5.2.5, tag 0x0a) ---

    /// Load the single watch-state row. An absent or legacy row makes one bounded pass over the
    /// canonical ledger sequence and persists its known Agent floor. Reusing an already-journaled
    /// occurrence could collide with operation keys. A corrupt checkpoint still fails closed, while
    /// a corrupt history row is warned and skipped and keeps migration pending for a later retry.
    pub async fn get_watch_state(&self) -> Result<WatchState, ExecError> {
        let raw_key = watch_state_key();
        #[cfg(test)]
        let pause = self
            .watch_state_autocommit_pause
            .lock()
            .expect("watch-state autocommit pause lock poisoned")
            .clone();
        self.db
            .autocommit(
                move |dbtx, _| {
                    let raw_key = raw_key.clone();
                    #[cfg(test)]
                    let pause = pause.clone();
                    Box::pin(async move {
                        let bytes = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)?;
                        let state = watch_state_with_agent_floor_in(dbtx, &raw_key, bytes).await?;
                        #[cfg(test)]
                        Self::wait_watch_state_autocommit_rendezvous_for_test(&pause).await;
                        Ok(state)
                    })
                },
                None,
            )
            .await
            .map_err(map_autocommit_error)
    }

    /// Store the complete watch-state checkpoint.
    #[cfg(test)]
    pub async fn put_watch_state(&self, state: &WatchState) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&watch_state_key(), &encode_row(state)?)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    /// Update only the discovery checkpoint fields, preserving a concurrently advanced
    /// occurrence from the row read inside this transaction.
    pub async fn put_watch_discovery_state(
        &self,
        discover_cursor: Option<FederationId>,
        discover_backlog: bool,
        last_discover_ms: Option<u64>,
        discover_rotation: Vec<FederationId>,
    ) -> Result<WatchState, ExecError> {
        let raw_key = watch_state_key();
        #[cfg(test)]
        let pause = self
            .watch_state_autocommit_pause
            .lock()
            .expect("watch-state autocommit pause lock poisoned")
            .clone();
        self.db
            .autocommit(
                move |dbtx, _| {
                    let raw_key = raw_key.clone();
                    let discover_cursor = discover_cursor;
                    let discover_rotation = discover_rotation.clone();
                    #[cfg(test)]
                    let pause = pause.clone();
                    Box::pin(async move {
                        let bytes = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)?;
                        let mut state =
                            watch_state_with_agent_floor_in(dbtx, &raw_key, bytes).await?;
                        state.discover_cursor = discover_cursor;
                        state.discover_backlog = discover_backlog;
                        state.discover_rotation = discover_rotation;
                        if let Some(last_discover_ms) = last_discover_ms {
                            state.last_discover_ms = last_discover_ms;
                        }
                        dbtx.raw_insert_bytes(&raw_key, &encode_row(&state)?)
                            .await
                            .map_err(db_err)?;
                        #[cfg(test)]
                        Self::wait_watch_state_autocommit_rendezvous_for_test(&pause).await;
                        Ok(state)
                    })
                },
                None,
            )
            .await
            .map_err(map_autocommit_error)
    }

    /// Advance the persisted occurrence by one, preserving the discovery checkpoint fields.
    pub async fn advance_watch_occurrence(&self) -> Result<WatchState, ExecError> {
        self.drain_watch_floor_immediately().await?;
        let raw_key = watch_state_key();
        #[cfg(test)]
        let pause = self
            .watch_state_autocommit_pause
            .lock()
            .expect("watch-state autocommit pause lock poisoned")
            .clone();
        self.db
            .autocommit(
                move |dbtx, _| {
                    let raw_key = raw_key.clone();
                    #[cfg(test)]
                    let pause = pause.clone();
                    Box::pin(async move {
                        let bytes = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)?;
                        let mut state =
                            watch_state_with_agent_floor_in(dbtx, &raw_key, bytes).await?;
                        if !state.agent_floor_reconciled {
                            return Err(watch_floor_reconciliation_required_error(&state));
                        }
                        state.occurrence = state.occurrence.checked_add(1).ok_or_else(|| {
                            ExecError::Permanent(
                                "watch scheduler occurrence exhausted at u64::MAX; restore a checkpoint below \
                                 u64::MAX before scheduling another cycle"
                                    .to_owned(),
                            )
                        })?;
                        dbtx.raw_insert_bytes(&raw_key, &encode_row(&state)?)
                            .await
                            .map_err(db_err)?;
                        #[cfg(test)]
                        Self::wait_watch_state_autocommit_rendezvous_for_test(&pause).await;
                        Ok(state)
                    })
                },
                None,
            )
            .await
            .map_err(map_autocommit_error)
    }

    /// Record an occurrence used by a standalone tick without moving the watch checkpoint
    /// backwards. The daemon advances this same row before planning; keeping its floor at every
    /// standalone occurrence makes its next child strictly newer than a marked standalone parent.
    pub async fn observe_watch_occurrence(&self, occurrence: u64) -> Result<WatchState, ExecError> {
        // Check before starting the write transaction: a standalone MAX occurrence
        // cannot ever yield a strictly newer daemon child, so it must not poison
        // the durable watch floor.
        ensure_occurrence_has_successor(occurrence)?;
        self.drain_watch_floor_immediately().await?;
        let raw_key = watch_state_key();
        self.db
            .autocommit(
                move |dbtx, _| {
                    let raw_key = raw_key.clone();
                    Box::pin(async move {
                        let bytes = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)?;
                        let mut state =
                            watch_state_with_agent_floor_in(dbtx, &raw_key, bytes).await?;
                        if !state.agent_floor_reconciled {
                            return Err(watch_floor_reconciliation_required_error(&state));
                        }
                        state.occurrence = state.occurrence.max(occurrence);
                        dbtx.raw_insert_bytes(&raw_key, &encode_row(&state)?)
                            .await
                            .map_err(db_err)?;
                        Ok(state)
                    })
                },
                None,
            )
            .await
            .map_err(map_autocommit_error)
    }

    /// Reconcile only the bounded WatchState migration before a scheduler cycle reads any other
    /// state. This deliberately does not allocate an occurrence: partial-view and reconcile
    /// semantics remain the authority boundary for actual work.
    ///
    /// A valid suffix can need several calls because each call commits at most the fixed immediate
    /// drain budget. Callers that schedule continuously may yield and retry the preflight; callers
    /// that run one standalone operation receive the actionable reconciliation error instead.
    pub async fn preflight_watch_floor_drain(&self) -> Result<WatchState, ExecError> {
        self.drain_watch_floor_immediately().await
    }

    /// Drain a valid legacy-ledger floor before allocation without waiting for a normal scheduler
    /// interval. One direct-row chunk commits per iteration, and yielding after every commit makes
    /// this safe to cancel and prevents it from monopolising an actor turn. An unreadable/missing
    /// row remains repair-only: it never authorises an occurrence.
    async fn drain_watch_floor_immediately(&self) -> Result<WatchState, ExecError> {
        let mut last = None;
        for _ in 0..WATCH_FLOOR_IMMEDIATE_DRAIN_CHUNK_BUDGET {
            let state = self.get_watch_state().await?;
            if state.agent_floor_reconciled {
                return Ok(state);
            }
            if !state.agent_floor_unreadable_ledger_keys.is_empty() {
                // Retrying the same unreadable repair keys in the remaining batch cannot make
                // forward progress. Persisted state records them for an operator restore, so stop
                // now and let recovery work continue without monopolising this scheduler turn.
                return Err(watch_floor_reconciliation_required_error(&state));
            }
            last = Some(state);
            tokio::task::yield_now().await;
        }
        // Do not perform a seventeenth transaction for a sixteen-chunk budget. The caller can
        // inspect this exact durable checkpoint and either immediately retry (scheduler) or follow
        // the actionable status procedure (standalone).
        Err(watch_floor_reconciliation_required_error(
            &last.expect("nonzero drain budget always records a state"),
        ))
    }

    /// Whether a scheduler should promptly start another bounded allocation-drain batch. This
    /// deliberately excludes unreadable repair rows: they require an operator repair and must never
    /// turn into a busy retry loop. It is a non-mutating checkpoint read: calling `get_watch_state`
    /// here would secretly consume a seventeenth chunk after a sixteen-chunk budget.
    pub async fn watch_floor_immediate_retry_needed(&self) -> Result<bool, ExecError> {
        let raw_key = watch_state_key();
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? else {
            return Ok(false);
        };
        #[cfg(test)]
        self.wait_watch_floor_immediate_retry_read_for_test().await;
        let state: WatchState = decode_row_result("watch state", &raw_key, &bytes)?;
        // A concurrent status/read may have completed the final chunk after the allocation's
        // typed bounded-backlog error. Retry immediately in that case too: the next cycle can
        // allocate, whereas sleeping the routine cadence would strand already-complete work.
        Ok(state.agent_floor_unreadable_ledger_keys.is_empty())
    }

    // --- user policy (phase 6a §6a.6, tag 0x0b) ---

    /// Load the stored standing instruction. Absence is distinct from the default so startup
    /// can seed exactly once without resetting a policy edited by the user.
    pub async fn get_policy(&self) -> Result<Option<Policy>, ExecError> {
        let raw_key = policy_key();
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? else {
            return Ok(None);
        };
        decode_row_result("policy", &raw_key, &bytes).map(Some)
    }

    /// Atomically insert `seed` only when no policy exists, returning the authoritative row.
    pub async fn seed_policy(&self, seed: &Policy) -> Result<Policy, ExecError> {
        let raw_key = policy_key();
        let mut dbtx = self.db.begin_transaction().await;
        let policy = match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
            Some(bytes) => decode_row_result("policy", &raw_key, &bytes)?,
            None => {
                dbtx.raw_insert_bytes(&raw_key, &encode_row(seed)?)
                    .await
                    .map_err(db_err)?;
                seed.clone()
            }
        };
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(policy)
    }

    /// Replace the standing instruction in one durable row write.
    pub async fn put_policy(&self, policy: &Policy) -> Result<(), ExecError> {
        let mut dbtx = self.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&policy_key(), &encode_row(policy)?)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }
}

async fn watch_state_with_agent_floor_in(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    raw_key: &[u8],
    bytes: Option<Vec<u8>>,
) -> Result<WatchState, ExecError> {
    // The counter is an allocation authority only if it names the immediate successor of the
    // physical ledger tail. Checking the descending tail costs O(1), prevents a low or absent
    // counter from hiding a later Agent occurrence, and prevents a direct writer from overwriting
    // that tail at the falsely low sequence.
    let next_seq = ledger_counter_matches_tail_in(dbtx).await?;
    watch_state_with_agent_floor_at_next_in(dbtx, raw_key, bytes, next_seq).await
}

/// Reconcile a watch checkpoint against a counter value already validated by
/// [`ledger_counter_matches_tail_in`]. The allocation path passes its single authoritative counter
/// read here so an Agent admission does not perform a redundant descending tail scan.
async fn watch_state_with_agent_floor_at_next_in(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    raw_key: &[u8],
    bytes: Option<Vec<u8>>,
    next_seq: u64,
) -> Result<WatchState, ExecError> {
    let mut state = match bytes {
        Some(bytes) => decode_row_result::<WatchState>("watch state", raw_key, &bytes)?,
        None => WatchState::default(),
    };
    if !state.agent_floor_scan_initialized {
        // Legacy checkpoints used to scan the entire row prefix here. A corrupt counter or a large
        // valid history made that first status read unbounded. Start the durable direct-row cursor
        // instead; every access below advances at most one chunk.
        state.agent_floor_scan_initialized = true;
        state.agent_floor_scan_high_water = 0;
        state.agent_floor_unreadable_ledger_keys.clear();
        state.agent_floor_reconciled = false;
    }
    if state.agent_floor_scan_high_water > next_seq {
        // A partial WatchState can also be restored independently of its ledger. Its frontier and
        // exact repair keys may name a newer, incompatible snapshot, so retaining them would turn a
        // safe older ledger restore into a permanent backward-cursor failure. Restart a bounded
        // canonical pass, but retain `occurrence` as a monotonic floor.
        state.agent_floor_scan_high_water = 0;
        state.agent_floor_unreadable_ledger_keys.clear();
        state.agent_floor_reconciled = false;
    } else if state.agent_floor_reconciled && state.agent_floor_scan_high_water != next_seq {
        // Never trust a completed bit when its asserted frontier no longer names the validated
        // counter: scan the newly exposed suffix before another Agent allocation.
        state.agent_floor_reconciled = false;
    }
    if !state.agent_floor_reconciled {
        // The bounded legacy pass remembers every unreadable raw key and a durable append-only
        // high-water. Retry only those keys, then direct-read the rows appended since that
        // high-water; never rescan the already-covered canonical range while repair is outstanding.
        if state.agent_floor_unreadable_ledger_keys.len() > WATCH_FLOOR_UNREADABLE_KEY_LIMIT {
            return Err(watch_floor_repair_bound_error(
                "persisted unreadable-key list exceeds the repair bound",
            ));
        }
        let mut unreadable_keys = BTreeSet::new();
        let mut max_agent_occurrence = state.occurrence;
        for key in &state.agent_floor_unreadable_ledger_keys {
            match dbtx.raw_get_bytes(key).await.map_err(db_err)? {
                None => {
                    warn_missing_watch_ledger_row(key);
                    remember_unreadable_watch_ledger_key(&mut unreadable_keys, key.clone())?;
                }
                Some(value) => {
                    match observe_agent_occurrence(key, &value, &mut max_agent_occurrence) {
                        Ok(()) => {}
                        Err(error) => {
                            warn_unreadable_watch_ledger_row(&error, key);
                            remember_unreadable_watch_ledger_key(
                                &mut unreadable_keys,
                                key.clone(),
                            )?;
                        }
                    }
                }
            }
        }
        if next_seq < state.agent_floor_scan_high_water {
            return Err(watch_floor_repair_bound_error(
                "ledger counter moved backward below the durable scan high-water",
            ));
        }
        let scan_high_water = scan_watch_floor_ledger_chunk(
            dbtx,
            state.agent_floor_scan_high_water,
            next_seq,
            &mut max_agent_occurrence,
            &mut unreadable_keys,
        )
        .await?;
        state.occurrence = max_agent_occurrence;
        state.agent_floor_scan_high_water = scan_high_water;
        state.agent_floor_unreadable_ledger_keys = unreadable_keys.into_iter().collect();
        state.agent_floor_reconciled =
            state.agent_floor_unreadable_ledger_keys.is_empty() && scan_high_water == next_seq;
        dbtx.raw_insert_bytes(raw_key, &encode_row(&state)?)
            .await
            .map_err(db_err)?;
    }
    Ok(state)
}

async fn ledger_next_seq_in(dbtx: &mut impl IDatabaseTransactionOpsCore) -> Result<u64, ExecError> {
    match dbtx
        .raw_get_bytes(&ledger_counter_key())
        .await
        .map_err(db_err)?
    {
        Some(bytes) => {
            read_be64(&bytes).ok_or_else(|| ledger_tail_counter_error("ledger counter is corrupt"))
        }
        None => Ok(0),
    }
}

/// Verify the append counter against only the lexicographically greatest ledger key. Ledger row
/// keys are `[TAG_LEDGER_ROW] ++ be64(seq)`, so this catches both hidden out-of-counter rows and
/// malformed tail keys without turning a status or admission path into a full scan.
async fn ledger_counter_matches_tail_in(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
) -> Result<u64, ExecError> {
    let next_seq = ledger_next_seq_in(dbtx).await?;
    let mut rows = dbtx
        .raw_find_by_prefix_sorted_descending(&[TAG_LEDGER_ROW])
        .await
        .map_err(db_err)?;
    match rows.next().await {
        Some((raw_key, _)) => {
            let highest_seq = canonical_ledger_seq_from_raw_key(&raw_key).map_err(|_| {
                ledger_tail_counter_error(
                    "physical ledger tail key is not the canonical 9-byte ledger sequence key",
                )
            })?;
            let expected_next = highest_seq.checked_add(1).ok_or_else(|| {
                ledger_tail_counter_error(
                    "highest ledger row uses u64::MAX and has no valid append successor",
                )
            })?;
            if next_seq != expected_next {
                return Err(ledger_tail_counter_error(
                    "ledger counter does not equal the successor of the highest ledger row",
                ));
            }
        }
        None if next_seq != 0 => {
            return Err(ledger_tail_counter_error(
                "ledger counter is nonzero but the ledger has no rows",
            ));
        }
        None => {}
    }
    Ok(next_seq)
}

/// Reserve no state, but validate that `seq` is a safe next append sequence and compute its
/// successor before a caller stages any ledger write. This is actor-independent: a stale counter
/// must not let either User or Agent admissions overwrite physical history.
async fn next_ledger_sequence_in(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
) -> Result<(u64, u64), ExecError> {
    let seq = ledger_counter_matches_tail_in(dbtx).await?;
    let successor = seq.checked_add(1).ok_or_else(|| {
        ExecError::Permanent(
            "journal: ledger sequence exhausted at u64::MAX; restore a checkpoint with an allocatable ledger successor"
                .to_owned(),
        )
    })?;
    Ok((seq, successor))
}

fn canonical_ledger_seq_from_raw_key(raw_key: &[u8]) -> Result<u64, ExecError> {
    let Some(seq_bytes) = raw_key.strip_prefix(&[TAG_LEDGER_ROW]) else {
        return Err(ExecError::Permanent(
            "journal: ledger row key has an invalid tag".to_owned(),
        ));
    };
    let Some(seq) = read_be64(seq_bytes) else {
        return Err(ExecError::Permanent(
            "journal: ledger row key is not the canonical 9-byte ledger sequence key".to_owned(),
        ));
    };
    if raw_key.len() != 9 {
        return Err(ExecError::Permanent(
            "journal: ledger row key is not the canonical 9-byte ledger sequence key".to_owned(),
        ));
    }
    Ok(seq)
}

fn decode_canonical_ledger_row(raw_key: &[u8], value: &[u8]) -> Result<OperationRecord, ExecError> {
    let key_seq = canonical_ledger_seq_from_raw_key(raw_key)?;
    let row: OperationRecord = decode_row_result("ledger row", raw_key, value)?;
    if row.seq != key_seq {
        return Err(ExecError::Permanent(format!(
            "journal: ledger row sequence {} does not match canonical key sequence {key_seq}",
            row.seq
        )));
    }
    Ok(row)
}

fn observe_agent_occurrence(
    raw_key: &[u8],
    value: &[u8],
    max_occurrence: &mut u64,
) -> Result<(), ExecError> {
    let row = decode_canonical_ledger_row(raw_key, value)?;
    if let Actor::Agent { occurrence } = row.actor {
        *max_occurrence = (*max_occurrence).max(occurrence.0);
    }
    Ok(())
}

fn warn_unreadable_watch_ledger_row(error: &ExecError, raw_key: &[u8]) {
    tracing::warn!(
        ?error,
        raw_key = ?raw_key,
        "watch-state floor migration skipped unreadable ledger row; repair from backup and retry watch access"
    );
}

fn warn_missing_watch_ledger_row(raw_key: &[u8]) {
    tracing::warn!(
        raw_key = ?raw_key,
        "watch-state floor migration found missing ledger row; restore from backup and retry watch access"
    );
}

fn watch_floor_repair_bound_error(detail: &str) -> ExecError {
    ExecError::Permanent(format!(
        "journal: watch-state floor migration {detail}; stop walletd, preserve the store, and restore malformed ledger rows from backup"
    ))
}

/// A physical-tail/counter disagreement makes the next sequence unknowable. This is an allocation
/// fence for every fresh ledger row, regardless of whether its actor is User or Agent.
fn ledger_tail_counter_error(detail: &str) -> ExecError {
    ExecError::Permanent(format!(
        "journal: ledger tail/counter inconsistency: {detail}; all fresh ledger append/admissions \
         are fenced; stop walletd, preserve the store, and restore the counter and ledger from one \
         consistent trusted backup"
    ))
}

/// An Agent occurrence is an allocation, not merely a checkpoint update.  A caller can observe
/// bounded migration progress through `get_watch_state`/`GET /v1/watch/status`; a nonzero repair
/// count requires restoring those rows before retrying.
fn watch_floor_reconciliation_required_error(state: &WatchState) -> ExecError {
    let next_step = if state.agent_floor_unreadable_ledger_keys.is_empty() {
        "daemon: retry GET /v1/watch/status until the bounded ledger backlog converges; \
         standalone: re-run the same tick until it converges"
    } else {
        "restore the unreadable ledger rows from backup, then retry the applicable daemon GET \
         /v1/watch/status or standalone tick"
    };
    ExecError::Permanent(format!(
        "journal: watch-state floor reconciliation is incomplete \
          (scan high-water {}, {} unreadable rows); {next_step}",
        state.agent_floor_scan_high_water,
        state.agent_floor_unreadable_ledger_keys.len(),
    ))
}

/// Narrow classifier for the *valid-or-repair* allocation fence emitted above. Scheduler retry
/// policy must not treat a tail/counter fence, arbitrary storage error, or another cycle failure as
/// a reason to spin merely because a stale WatchState happens to show backlog.
pub(crate) fn is_watch_floor_reconciliation_required(error: &ExecError) -> bool {
    matches!(
        error,
        ExecError::Permanent(message)
            if message.starts_with(
                "journal: watch-state floor reconciliation is incomplete"
            )
    )
}

fn remember_unreadable_watch_ledger_key(
    unreadable_keys: &mut BTreeSet<Vec<u8>>,
    raw_key: Vec<u8>,
) -> Result<(), ExecError> {
    if unreadable_keys.contains(&raw_key) {
        return Ok(());
    }
    if unreadable_keys.len() == WATCH_FLOOR_UNREADABLE_KEY_LIMIT {
        return Err(watch_floor_repair_bound_error(
            "has more unreadable or missing ledger rows than the repair bound",
        ));
    }
    unreadable_keys.insert(raw_key);
    Ok(())
}

/// Direct-read at most one bounded range of canonical ledger rows. If existing unreadable repair
/// work fills the durable key budget, retain it and make no speculative progress: advancing past a
/// new missing row we cannot name would falsely certify the floor.
async fn scan_watch_floor_ledger_chunk(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    start: u64,
    next_seq: u64,
    max_agent_occurrence: &mut u64,
    unreadable_keys: &mut BTreeSet<Vec<u8>>,
) -> Result<u64, ExecError> {
    if unreadable_keys.len() == WATCH_FLOOR_UNREADABLE_KEY_LIMIT {
        return Ok(start);
    }
    let end = start
        .saturating_add(WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64)
        .min(next_seq);
    for seq in start..end {
        let key = ledger_row_key(seq);
        match dbtx.raw_get_bytes(&key).await.map_err(db_err)? {
            None => {
                warn_missing_watch_ledger_row(&key);
                remember_unreadable_watch_ledger_key(unreadable_keys, key)?;
            }
            Some(value) => {
                if let Err(error) = observe_agent_occurrence(&key, &value, max_agent_occurrence) {
                    warn_unreadable_watch_ledger_row(&error, &key);
                    remember_unreadable_watch_ledger_key(unreadable_keys, key)?;
                }
            }
        }
        if unreadable_keys.len() == WATCH_FLOOR_UNREADABLE_KEY_LIMIT {
            return Ok(seq.saturating_add(1));
        }
    }
    Ok(end)
}

// --- repair support (spec §10.3) -------------------------------------------------------

fn raw_update_has_enrichment(upd: &RawOpUpdate) -> bool {
    upd.op_id.is_some()
        || upd.gateway.is_some()
        || upd.invoice_amount.is_some()
        || upd.payment_hash.is_some()
        || upd.fees.is_some()
}

const JOIN_SUPERSEDED: &str = "superseded by a later join attempt";
const JOIN_NOT_REGISTERED: &str =
    "join did not complete — federation not in the registry; re-run join";
const JOIN_AMBIGUOUS_NOTE: &str =
    "overlapping attempts; correlation uncertain — membership itself is registry-proven";
const INTERRUPTED_NO_TERMINAL: &str = "interrupted — no terminal report";
const RAW_NEVER_REACHED: &str = "never reached the federation";
const HASH_DEDUP_NOTE: &str = "correlated by payment hash to an existing payment of this invoice; \
     attempt-level correlation uncertain (deduped retry or never-sent attempt); the matched \
     operation is authoritative";

/// The only raw terminal repair that intentionally has no settlement evidence.  It remains a
/// ledger-only, defeasible diagnosis: terminal-row sink retries must not convert its live intent
/// into `Failed`.  Keep this exact marker guard narrow so witnessed soft repairs (for example
/// hash-dedup attribution) still release the matching reservation through their fenced sink.
fn raw_terminal_repair_must_not_sink_intent(row: &OperationRecord) -> bool {
    row.status == OperationStatus::Failed
        && row.repaired
        && row.error.as_deref() == Some(RAW_NEVER_REACHED)
}

fn intent_status_is_terminal(status: IntentStatus) -> bool {
    matches!(status, IntentStatus::Done | IntentStatus::Failed)
}

/// Which repair family a correlation key belongs to (§10.3), by its `<verb>:` prefix.
#[derive(PartialEq, Eq)]
enum KeyClass {
    Join,
    Tick,
    Discovery,
    Raw,
    Other,
}

fn classify_key(key: &IdempotencyKey) -> KeyClass {
    let s = key.0.as_str();
    if s.starts_with("join:") {
        KeyClass::Join
    } else if s.starts_with("tick:") {
        KeyClass::Tick
    } else if s.starts_with("discover:")
        || s.starts_with("autojoin:")
        || s.starts_with("approve:")
        || s.starts_with("watch-probe-skip:")
    {
        KeyClass::Discovery
    } else if s.starts_with("pay:") || s.starts_with("recv:") {
        KeyClass::Raw
    } else {
        KeyClass::Other
    }
}

/// `(fed, op_id, payment_hash)` for a raw `Pay`/`Receive` kind; `None` for anything else.
fn raw_row_parts(
    kind: &OperationKind,
) -> Option<(FederationId, Option<OperationId>, Option<[u8; 32]>)> {
    match kind {
        OperationKind::Pay {
            fed,
            op_id,
            payment_hash,
            ..
        } => Some((*fed, *op_id, *payment_hash)),
        OperationKind::Receive { fed, op_id, .. } => Some((*fed, *op_id, None)),
        _ => None,
    }
}

/// Combine an uncertainty `note` with an op's terminal `error` into the row's `error`.
fn combine_note(note: Option<&str>, term_error: Option<String>) -> Option<String> {
    match (note, term_error) {
        (Some(note), Some(err)) => Some(format!("{note} ({err})")),
        (Some(note), None) => Some(note.to_owned()),
        (None, err) => err,
    }
}

/// Select a single ledger row by correlation key or seq (for `show`).
pub enum OperationRef {
    Key(IdempotencyKey),
    Seq(u64),
}

/// Which raw lnv2 leg an external await is finalizing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawOperationRole {
    Send,
    Receive,
}

/// Correlation and op-log observation captured before the actor's raw-terminal lease.  Its
/// fields stay private so callers cannot manufacture a terminal settlement without the journal's
/// row/role checks.
pub struct PreparedRawOperationTerminal {
    notes: Vec<String>,
    expected_attempt: u32,
    update: Option<RawOpUpdate>,
    fence: Option<RawRepairFence>,
    observed_status: Option<OperationStatus>,
}

#[cfg(test)]
impl PreparedRawOperationTerminal {
    /// Test-only unfenced preparation for exercising a caller's finalizer/lease cleanup when its
    /// in-memory SDK fixture cannot supply an op-log observation.
    pub(crate) fn unfenced_for_test(expected_attempt: u32) -> Self {
        Self {
            notes: Vec::new(),
            expected_attempt,
            update: None,
            fence: None,
            observed_status: None,
        }
    }
}

/// The durable identity captured before raw repair/finalization leaves the database.  `None`
/// attempt is retained solely for old standalone ledger rows, which have no intent reservation
/// to synchronize.
#[derive(Clone, Debug)]
struct RawRepairFence {
    expected_seq: u64,
    expected_attempt: Option<u32>,
    attempt_correlation_key: IdempotencyKey,
    /// Captured with the ledger row and matching intent in one database snapshot.
    /// Already-terminal intents need no actor sink retry.
    intent_nonterminal: bool,
    fed: FederationId,
    expected_op: Option<OperationId>,
    role: RawOperationRole,
    expected_ledger_status: OperationStatus,
}

impl RawRepairFence {
    /// Convert a repair scan fence into the sink's post-write fence.  The sink only accepts an
    /// intent-backed row and only after the expected terminal ledger state has committed.
    fn terminal_sink_fence(
        &self,
        expected_ledger_status: OperationStatus,
        expected_op: Option<OperationId>,
    ) -> Option<RawIntentTerminalFence> {
        (self.intent_nonterminal && expected_ledger_status.is_terminal()).then_some(
            RawIntentTerminalFence {
                expected_seq: self.expected_seq,
                expected_attempt: self.expected_attempt?,
                fed: self.fed,
                expected_op,
                role: self.role,
                expected_ledger_status,
            },
        )
    }
}

/// Every fact a raw repair terminal sink must recheck in its one intent-only transaction.
///
/// It is public because [`RawIntentTerminalSink`] is an extension point.  Its fields stay private;
/// callers that need to relay one through another component use [`Self::new`], while the database
/// transaction remains the authority and rechecks every value before changing an intent.
#[derive(Clone, Debug)]
pub struct RawIntentTerminalFence {
    expected_seq: u64,
    expected_attempt: u32,
    fed: FederationId,
    expected_op: Option<OperationId>,
    role: RawOperationRole,
    expected_ledger_status: OperationStatus,
}

impl RawIntentTerminalFence {
    /// Name the exact terminal ledger row that authorizes a raw intent terminal transition.
    /// Construction grants no authority: [`FedimintJournal::set_raw_terminal_if_fenced`] verifies
    /// every field atomically before it writes the intent or its pending index.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        expected_seq: u64,
        expected_attempt: u32,
        fed: FederationId,
        expected_op: Option<OperationId>,
        role: RawOperationRole,
        expected_ledger_status: OperationStatus,
    ) -> Self {
        Self {
            expected_seq,
            expected_attempt,
            fed,
            expected_op,
            role,
            expected_ledger_status,
        }
    }
}

/// The sole repair write that changes a raw Pay/Receive intent's terminality.
/// Service callers inject an actor-backed implementation so repair's op-log scan
/// stays off actor while its reservation-releasing CAS remains serialized.
#[async_trait]
pub trait RawIntentTerminalSink: Send + Sync {
    async fn set_raw_terminal(
        &self,
        key: &IdempotencyKey,
        fence: &RawIntentTerminalFence,
        status: IntentStatus,
        error: Option<String>,
    ) -> Result<bool, ExecError>;
}

struct DirectRawIntentTerminalSink<'a> {
    journal: &'a FedimintJournal,
}

#[async_trait]
impl RawIntentTerminalSink for DirectRawIntentTerminalSink<'_> {
    async fn set_raw_terminal(
        &self,
        key: &IdempotencyKey,
        fence: &RawIntentTerminalFence,
        status: IntentStatus,
        error: Option<String>,
    ) -> Result<bool, ExecError> {
        self.journal
            .set_raw_terminal_if_fenced(key, fence, status, error.as_deref())
            .await
    }
}

/// Verify that an externally supplied raw operation handle belongs to a ledger row before an
/// immutable terminal write. `Ok(true)` means the row has no op id and needs correlation proof.
pub fn raw_operation_row_matches(
    row: &OperationRecord,
    role: RawOperationRole,
    fed: FederationId,
    op: OperationId,
) -> Result<bool, String> {
    let (row_fed, row_op) = match (&row.kind, role) {
        (OperationKind::Pay { fed, op_id, .. }, RawOperationRole::Send) => (fed, op_id),
        (OperationKind::Receive { fed, op_id, .. }, RawOperationRole::Receive) => (fed, op_id),
        _ => return Err("its kind is not the awaited pay/receive operation".to_owned()),
    };
    if *row_fed != fed {
        return Err("it belongs to a different federation".to_owned());
    }
    match row_op {
        Some(existing) if *existing != op => {
            Err("it already tracks a different operation".to_owned())
        }
        Some(_) => Ok(false),
        None => Ok(true),
    }
}

fn raw_role(kind: &OperationKind) -> Option<RawOperationRole> {
    match kind {
        OperationKind::Pay { .. } => Some(RawOperationRole::Send),
        OperationKind::Receive { .. } => Some(RawOperationRole::Receive),
        _ => None,
    }
}

/// A count of the rows a [`FedimintJournal::repair_ledger`] pass terminalized/advanced.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepairSummary {
    pub repaired: usize,
}

/// The op-log evidence reconcile repair (§10.3) needs for raw `pay:`/`recv:` rows, abstracted
/// so the repair DECISION logic is testable on `MemDatabase` without a live federation (the
/// runtime supplies a [`crate::MultiClient`]-backed adapter; tests supply a mock).
#[async_trait]
pub trait LedgerRepairOracle: Send + Sync {
    /// The op on `fed` whose `custom_meta` carries this `correlation_key` (§10.3 primary
    /// backfill). Op ids are per-attempt-unique, so a hit is THE op.
    async fn find_op_by_correlation_key(
        &self,
        fed: FederationId,
        key: &IdempotencyKey,
    ) -> Result<Option<OperationId>, ExecError>;
    /// A SEND op on `fed` whose invoice payment-hash matches `hash` (§10.3 dedup recovery: an
    /// `AlreadyInFlight` retry reuses the ORIGINAL op — its key is in no op's
    /// meta, so the durably-written hash is the link).
    async fn find_send_op_by_payment_hash(
        &self,
        fed: FederationId,
        hash: [u8; 32],
    ) -> Result<Option<OperationId>, ExecError>;
    /// Observe an already-identified op's current state + definitive settlement enrichment. The
    /// terminal read is NON-BLOCKING: a still-in-flight op yields `terminal: None` (leave
    /// `Awaiting`), never a hang.
    async fn observe_op(
        &self,
        fed: FederationId,
        op: OperationId,
    ) -> Result<RawOpObservation, ExecError>;
}

/// What [`LedgerRepairOracle::observe_op`] learned about a raw op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawOpObservation {
    /// `Some` once the op reached a terminal state; `None` while still in flight.
    pub terminal: Option<RawTerminal>,
    pub gateway: Option<GatewayUrl>,
    /// Definitive settlement fees (§9.3 backfill) — the field matching the op's leg is set.
    pub fees: FeeBreakdown,
    pub invoice_amount: Option<Msat>,
    pub payment_hash: Option<[u8; 32]>,
}

/// A terminal op outcome: whether it settled, plus any failure detail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawTerminal {
    pub succeeded: bool,
    pub error: Option<String>,
}

#[async_trait]
impl Journal for FedimintJournal {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError> {
        let value = encode_row(intent)?;
        let ikey = intent_key(&intent.idempotency_key);

        let mut dbtx = self.db.begin_transaction().await;
        // Atomic with the write below: if this key already exists under a DIFFERENT *indexed*
        // status, drop the stale `PendingIndexKey` first so a scan never finds the Intent
        // indexed under a status it no longer holds (upsert may overwrite an Intent's status).
        if let Some(old_bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? {
            let old = decode_row_result::<Intent>("intent", &ikey, &old_bytes)?;
            if old.attempt != intent.attempt {
                return Err(ExecError::Permanent(format!(
                    "journal: stale attempt {} for current attempt {}",
                    intent.attempt, old.attempt
                )));
            }
            if !intent_status_transition_allowed(old.status, intent.status) {
                return Err(ExecError::Permanent(format!(
                    "journal: invalid status transition {:?} -> {:?}",
                    old.status, intent.status
                )));
            }
            if old.status != intent.status && is_indexed(old.status) {
                dbtx.raw_remove_entry(&pending_index_key(old.status, &intent.idempotency_key))
                    .await
                    .map_err(db_err)?;
            }
        }
        dbtx.raw_insert_bytes(&ikey, &value).await.map_err(db_err)?;
        // Only the scanned statuses are indexed; `Done` gets no row (see module docs).
        if is_indexed(intent.status) {
            dbtx.raw_insert_bytes(
                &pending_index_key(intent.status, &intent.idempotency_key),
                &[],
            )
            .await
            .map_err(db_err)?;
        }
        // §9.2: the ledger row for this intent commits in the SAME dbtx (create-or-advance).
        // `upsert` never carries a terminal failure diagnostic, so `error = None` (the
        // `MoveRecord.outcome` fallback still applies on a Failed status).
        write_intent_ledger_row(&mut dbtx, intent, self.now_ms(), None).await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        #[cfg(test)]
        let replacement = {
            self.replace_after_upsert
                .lock()
                .expect("post-upsert replacement lock poisoned")
                .take()
        };
        #[cfg(test)]
        if let Some(replacement) = replacement {
            if replacement.idempotency_key != intent.idempotency_key
                || replacement.status != intent.status
            {
                return Err(ExecError::Permanent(
                    "journal: invalid post-upsert test replacement identity or status".to_owned(),
                ));
            }
            // The normal write above already atomically established the row/index/ledger shape.
            // This seam changes only the durable intent bytes under that same indexed key, precisely
            // to present the actor with a readable mismatched row after a reported post-commit error.
            let replacement_value = encode_row(&replacement)?;
            let mut dbtx = self.db.begin_transaction().await;
            dbtx.raw_insert_bytes(&ikey, &replacement_value)
                .await
                .map_err(db_err)?;
            dbtx.commit_tx_result().await.map_err(db_err)?;
        }
        #[cfg(test)]
        if self
            .fail_after_upserts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Retryable(
                "journal: injected error after durable intent upsert".to_owned(),
            ));
        }
        Ok(())
    }

    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        self.read_intent(key).await
    }

    async fn set_status(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        status: IntentStatus,
        // §8.3/§9.2: the terminal failure diagnostic. It becomes the ledger row's `error` on a
        // `Failed` transition (executor string first, `MoveRecord.outcome` as fallback).
        error: Option<&str>,
    ) -> Result<(), ExecError> {
        let ikey = intent_key(key);
        let mut dbtx = self.db.begin_transaction().await;
        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
            return Err(ExecError::Permanent("journal: intent not found".into()));
        };
        let mut intent = decode_row_result::<Intent>("intent", &ikey, &bytes)?;
        if intent.attempt != expected_attempt {
            return Err(ExecError::Permanent(format!(
                "journal: stale attempt {expected_attempt} for current attempt {}",
                intent.attempt
            )));
        }
        let old_status = intent.status;
        if !intent_status_transition_allowed(old_status, status) {
            return Err(ExecError::Permanent(format!(
                "journal: invalid status transition {old_status:?} -> {status:?}"
            )));
        }
        intent.status = status;

        write_intent_and_index(
            &mut dbtx,
            &ikey,
            key,
            old_status,
            &intent,
            self.now_ms(),
            error,
        )
        .await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        #[cfg(test)]
        if self
            .fail_after_status_writes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Retryable(
                "injected error after durable intent status write".to_owned(),
            ));
        }
        Ok(())
    }

    /// The single-writer claim: read the intent row; if absent or its status != `expected`,
    /// make no change and return `Ok(false)`; otherwise set `status = new`, rewrite the
    /// intent row, and move the `PendingIndexKey` in the SAME dbtx as the read and the status
    /// check. The autocommit wrapper retries write conflicts: a loser re-reads the winner's
    /// status and returns `Ok(false)`, so at most one caller observes `Ok(true)` for a given
    /// `expected -> new` transition.
    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        // The CAS carries no failure diagnostic, so the ledger row's `error` on a `Failed`
        // transition falls back to `MoveRecord.outcome` (§9.2). Snapshot the clock once so a
        // conflict-retry of the autocommit closure reuses one timestamp.
        let now = self.now_ms();
        self.db
            .autocommit(
                |dbtx, _| {
                    Box::pin(async move {
                        let ikey = intent_key(key);
                        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
                            return Ok(false);
                        };
                        let mut intent = decode_row_result::<Intent>("intent", &ikey, &bytes)?;
                        if intent.attempt != expected_attempt
                            || intent.status != expected
                            || !intent_status_transition_allowed(intent.status, new)
                        {
                            return Ok(false);
                        }
                        intent.status = new;
                        if expected == IntentStatus::Pending && new == IntentStatus::Executing {
                            // A fresh claim consumes any planning handoff.  A later refusal must
                            // write new evidence for this attempt; stale evidence is never reused.
                            intent.evacuation_refusal = None;
                        }

                        write_intent_and_index(dbtx, &ikey, key, expected, &intent, now, None)
                            .await?;
                        Ok(true)
                    })
                },
                None,
            )
            .await
            .map_err(|e| match e {
                AutocommitError::CommitFailed { last_error, .. } => db_err(last_error),
                AutocommitError::ClosureError { error, .. } => error,
            })
    }

    async fn reset_retryable(
        &self,
        key: &IdempotencyKey,
        expected_attempt: u32,
        structural_refusal: Option<EvacuationRefusalEvidence>,
    ) -> Result<(), ExecError> {
        let now = self.now_ms();
        self.db
            .autocommit(
                |dbtx, _| {
                    let structural_refusal = structural_refusal.clone();
                    Box::pin(async move {
                        let ikey = intent_key(key);
                        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
                            return Err(ExecError::Permanent("journal: intent not found".into()));
                        };
                        let mut intent = decode_row_result::<Intent>("intent", &ikey, &bytes)?;
                        if intent.attempt != expected_attempt
                            || intent.status != IntentStatus::Executing
                        {
                            return Err(ExecError::Permanent(
                                "journal: retryable reset requires the current Executing attempt"
                                    .into(),
                            ));
                        }
                        intent.status = IntentStatus::Pending;
                        intent.evacuation_refusal = structural_refusal;
                        write_intent_and_index(
                            dbtx,
                            &ikey,
                            key,
                            IntentStatus::Executing,
                            &intent,
                            now,
                            None,
                        )
                        .await
                    })
                },
                None,
            )
            .await
            .map_err(|e| match e {
                AutocommitError::CommitFailed { last_error, .. } => db_err(last_error),
                AutocommitError::ClosureError { error, .. } => error,
            })?;
        #[cfg(test)]
        if self
            .fail_after_retryable_resets
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ExecError::Retryable(
                "injected error after durable retryable reset".to_owned(),
            ));
        }
        Ok(())
    }

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        #[cfg(test)]
        {
            self.pending_reads.fetch_add(1, Ordering::SeqCst);
            let mut after_successes = self
                .fail_pending_read_after_successes
                .lock()
                .expect("pending read fault lock poisoned");
            if matches!(*after_successes, Some(0)) {
                *after_successes = None;
                return Err(ExecError::Retryable(
                    "journal: injected pending scan failure".to_owned(),
                ));
            }
            if let Some(remaining) = after_successes.as_mut() {
                *remaining -= 1;
            }
        }
        let pending = self
            .intents_indexed_as(&[IntentStatus::Pending, IntentStatus::Executing], false)
            .await;
        #[cfg(test)]
        {
            let pause = {
                self.pending_read_pause
                    .lock()
                    .expect("pending read pause lock poisoned")
                    .take()
            };
            if let Some(pause) = pause {
                pause.started.notify_waiters();
                pause.release.notified().await;
            }
        }
        pending
    }

    async fn awaiting(&self) -> Result<Vec<Intent>, ExecError> {
        self.intents_indexed_as(&[IntentStatus::Awaiting], false)
            .await
    }

    async fn reservation_intents(&self) -> Result<Vec<Intent>, ExecError> {
        #[cfg(test)]
        {
            let mut after_successes = self
                .fail_reservation_read_after_successes
                .lock()
                .expect("reservation read fault lock poisoned");
            if matches!(*after_successes, Some(0)) {
                *after_successes = None;
                return Err(ExecError::Retryable(
                    "journal: injected reservation scan failure".to_owned(),
                ));
            }
            if let Some(remaining) = after_successes.as_mut() {
                *remaining -= 1;
            }
        }
        self.intents_indexed_as(
            &[
                IntentStatus::Pending,
                IntentStatus::Executing,
                IntentStatus::Awaiting,
            ],
            true,
        )
        .await
    }

    async fn failed(&self) -> Vec<Intent> {
        self.intents_indexed_as(&[IntentStatus::Failed], false)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = ?e, "journal: failed scan failed this pass, returning empty");
                Vec::new()
            })
    }

    async fn move_record(&self, key: &IdempotencyKey) -> Result<Option<MoveRecord>, ExecError> {
        self.get_move(key).await
    }

    fn store_id(&self) -> usize {
        self.store_id
    }
}

/// Record durable USER ownership for a recovered federation inside an existing dbtx — the same
/// `UserApproved` candidate write a manual `join` performs (§5.1.4a, see
/// [`FedimintJournal::mark_candidate_user_approved`]), but ATOMIC with the recovery commit (D4.6).
/// [`crate::runtime::probe_gated_members`] gates every joined member that is NOT `UserApproved`, so
/// without this the recovered fed's funds return but the allocator never spends from it. Preserves
/// an existing `UserApproved` (idempotent, no demote/re-timestamp); every other state —
/// `Discovered`/`Rejected`/`AutoJoined`/absent — is promoted, and an unreadable row is replaced
/// (recovery carries an authenticated id+invite). Promoting `AutoJoined` is deliberate and diverges
/// from the join path's [`FedimintJournal::mark_candidate_user_approved`], which leaves an
/// `AutoJoined` row agent-owned: a seed recovery is an explicit user claim of ownership. (The
/// non-`absent` branches are unreachable in the real lost-`journal.db`/fresh-host scenarios — the
/// candidate row is lost together with the registry row, so the `absent` branch runs — but every
/// branch resolves toward user ownership if reached.)
async fn write_recovered_user_ownership(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    id: &FederationId,
    invite: &InviteCode,
    now_ms: u64,
) -> Result<(), ExecError> {
    let raw_key = candidate_key(id);
    let fresh = CandidateRecord {
        id: *id,
        invite: invite.clone(),
        source: DiscoverySource::Manual,
        discovered_at_ms: now_ms,
        structural: StructuralOutcome::Passed,
        structural_checked_at_ms: now_ms,
        state: CandidateState::UserApproved,
        updated_at_ms: now_ms,
    };
    let next = match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
        Some(bytes) => match decode_candidate_row(*id, &raw_key, &bytes) {
            // Already user-owned: leave it untouched (idempotent).
            Ok(current) if current.state == CandidateState::UserApproved => return Ok(()),
            Ok(mut current) => {
                current.state = CandidateState::UserApproved;
                current.updated_at_ms = now_ms;
                current
            }
            Err(error) => {
                tracing::warn!(
                    federation = %id.to_hex(),
                    ?error,
                    "journal: replacing unreadable candidate as UserApproved on recovery"
                );
                fresh
            }
        },
        None => fresh,
    };
    dbtx.raw_insert_bytes(&raw_key, &encode_row(&next)?)
        .await
        .map_err(db_err)?;
    Ok(())
}

/// Rewrite only the Intent row and move its `PendingIndexKey` entry from `old_status` to
/// `new_intent.status`, in the caller's already-open `dbtx`.
///
/// This is intentionally separate from [`write_intent_and_index`]: raw-repair's terminal sink
/// runs *after* its fenced ledger repair is durable and must not rewrite that ledger conclusion.
async fn write_intent_and_pending_index(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    ikey: &[u8],
    key: &IdempotencyKey,
    old_status: IntentStatus,
    new_intent: &Intent,
) -> Result<(), ExecError> {
    if old_status != new_intent.status && is_indexed(old_status) {
        dbtx.raw_remove_entry(&pending_index_key(old_status, key))
            .await
            .map_err(db_err)?;
    }
    if is_indexed(new_intent.status) {
        dbtx.raw_insert_bytes(&pending_index_key(new_intent.status, key), &[])
            .await
            .map_err(db_err)?;
    }
    let value = encode_row(new_intent)?;
    dbtx.raw_insert_bytes(ikey, &value).await.map_err(db_err)?;
    Ok(())
}

/// Rewrite the Intent row and move its `PendingIndexKey` entry from `old_status` to
/// `new_intent.status`, in the caller's already-open `dbtx` — the one-dbtx atomicity contract
/// (spec §8) shared by [`Journal::set_status`] and [`Journal::set_status_if`]. The ledger row
/// for this intent advances in the SAME dbtx (§9.2), so ledger and journal commit or fail
/// together.
async fn write_intent_and_index(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    ikey: &[u8],
    key: &IdempotencyKey,
    old_status: IntentStatus,
    new_intent: &Intent,
    now_ms: u64,
    error: Option<&str>,
) -> Result<(), ExecError> {
    write_intent_and_pending_index(dbtx, ikey, key, old_status, new_intent).await?;
    write_intent_ledger_row(dbtx, new_intent, now_ms, error).await?;
    Ok(())
}

// --- operation ledger (spec §9) --------------------------------------------------------

fn system_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn ledger_row_key(seq: u64) -> Vec<u8> {
    tagged(TAG_LEDGER_ROW, &seq.to_be_bytes())
}

fn ledger_key_index(key: &IdempotencyKey) -> Vec<u8> {
    tagged(TAG_LEDGER_KEY_INDEX, key.0.as_bytes())
}

const ALL_INTENT_STATUSES: [IntentStatus; 5] = [
    IntentStatus::Pending,
    IntentStatus::Executing,
    IntentStatus::Awaiting,
    IntentStatus::Done,
    IntentStatus::Failed,
];

fn validate_supersession_endpoints(
    relation: &EvacuationSupersessionRecord,
) -> Result<(), ExecError> {
    if relation.old_key == relation.new_key {
        return Err(ExecError::Permanent(
            "journal: evacuation supersession has identical parent and child keys".into(),
        ));
    }
    Ok(())
}

fn supersession_relation_matches_request(
    relation: &EvacuationSupersessionRecord,
    old: &Intent,
    fresh: &AllocatorDecision,
) -> bool {
    let (old_source, old_cap_components) = match &old.action {
        Action::Evacuate {
            from,
            fee_cap_components,
            ..
        } => (*from, *fee_cap_components),
        _ => return false,
    };
    let new_cap_components = match &fresh.action {
        Action::Evacuate {
            fee_cap_components, ..
        } => *fee_cap_components,
        _ => return false,
    };
    relation.old_key == old.idempotency_key
        && relation.old_attempt == old.attempt
        && relation.new_key == fresh.idempotency_key
        && relation.new_attempt == 0
        && relation.old_occurrence
            == match old.actor {
                Actor::Agent { occurrence } => occurrence,
                Actor::User => return false,
            }
        && relation.occurrence == fresh.occurrence
        && relation.source == old_source
        && relation.old_cap_components == old_cap_components
        && relation.new_cap_components == new_cap_components
}

/// A replay may arrive after the child has progressed.  Its lifecycle is intentionally not part of
/// request identity, but the immutable action/actor/reason/attempt/creation identity must still be
/// exactly the exchange's child.
fn validate_replayed_supersession_child(
    child: &Intent,
    fresh: &AllocatorDecision,
    superseded_at_ms: u64,
) -> Result<(), ExecError> {
    if !matches!(
        child.status,
        IntentStatus::Pending
            | IntentStatus::Executing
            | IntentStatus::Awaiting
            | IntentStatus::Done
            | IntentStatus::Failed
    ) || child.idempotency_key != fresh.idempotency_key
        || child.attempt != 0
        || child.action != fresh.action
        || child.actor
            != (Actor::Agent {
                occurrence: fresh.occurrence,
            })
        || child.reason != fresh.reason
        || child.created_at_ms != superseded_at_ms
    {
        return Err(ExecError::Permanent(
            "journal: supersession replay found a changed child identity".into(),
        ));
    }
    Ok(())
}

/// Read both immediate sides of a key from an already-open snapshot.  This is deliberately not
/// built on `evacuation_supersession`: a middle node has a canonical successor and a reverse
/// predecessor, and both must remain visible to an audit projection.
async fn evacuation_supersession_neighbors_in_tx(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    key: &IdempotencyKey,
) -> Result<EvacuationSupersessionNeighbors, ExecError> {
    let canonical = evacuation_supersession_key(key);
    let successor = match dbtx.raw_get_bytes(&canonical).await.map_err(db_err)? {
        None => None,
        Some(bytes) => {
            let row: EvacuationSupersessionRecord =
                decode_row_result("evacuation supersession", &canonical, &bytes)?;
            if row.old_key != *key {
                return Err(ExecError::Permanent(
                    "journal: evacuation supersession canonical key has mismatched endpoint".into(),
                ));
            }
            validate_complete_supersession(dbtx, &row).await?;
            Some(row)
        }
    };

    let reverse = evacuation_supersession_reverse_key(key);
    let predecessor = match dbtx.raw_get_bytes(&reverse).await.map_err(db_err)? {
        None => None,
        Some(bytes) => {
            let old: IdempotencyKey =
                decode_row_result("evacuation supersession reverse", &reverse, &bytes)?;
            let predecessor_key = evacuation_supersession_key(&old);
            let row_bytes = dbtx
                .raw_get_bytes(&predecessor_key)
                .await
                .map_err(db_err)?
                .ok_or_else(|| {
                    ExecError::Permanent(
                        "journal: supersession reverse index points at a missing canonical row"
                            .into(),
                    )
                })?;
            let row: EvacuationSupersessionRecord =
                decode_row_result("evacuation supersession", &predecessor_key, &row_bytes)?;
            if row.old_key != old || row.new_key != *key {
                return Err(ExecError::Permanent(
                    "journal: incoherent evacuation supersession reverse index".into(),
                ));
            }
            validate_complete_supersession(dbtx, &row).await?;
            Some(row)
        }
    };
    Ok(EvacuationSupersessionNeighbors {
        predecessor,
        successor,
    })
}

/// Read the attempted parent's canonical successor in an already-open snapshot.  Absence is
/// deliberately final even if this key has a reverse predecessor: the latter describes a different
/// exchange and cannot confirm the attempted one.
async fn evacuation_canonical_successor_in_tx(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    key: &IdempotencyKey,
) -> Result<Option<EvacuationSupersessionRecord>, ExecError> {
    let canonical = evacuation_supersession_key(key);
    let Some(bytes) = dbtx.raw_get_bytes(&canonical).await.map_err(db_err)? else {
        return Ok(None);
    };
    let row: EvacuationSupersessionRecord =
        decode_row_result("evacuation supersession", &canonical, &bytes)?;
    if row.old_key != *key {
        return Err(ExecError::Permanent(
            "journal: evacuation supersession canonical key has mismatched endpoint".into(),
        ));
    }
    validate_complete_supersession(dbtx, &row).await?;
    Ok(Some(row))
}

/// Validate the canonical row and its reverse half in the caller's existing snapshot.  Keeping
/// this one helper behind both the reader and the replacement replay path prevents a successful
/// lookup from silently accepting a half-written audit relation.
async fn validate_complete_supersession(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    relation: &EvacuationSupersessionRecord,
) -> Result<(), ExecError> {
    validate_supersession_endpoints(relation)?;
    let canonical_key = evacuation_supersession_key(&relation.old_key);
    let canonical = dbtx
        .raw_get_bytes(&canonical_key)
        .await
        .map_err(db_err)?
        .ok_or_else(|| {
            ExecError::Permanent("journal: supersession canonical row disappeared".into())
        })?;
    let stored: EvacuationSupersessionRecord =
        decode_row_result("evacuation supersession", &canonical_key, &canonical)?;
    if stored != *relation || stored.old_key != relation.old_key {
        return Err(ExecError::Permanent(
            "journal: incoherent evacuation supersession canonical row".into(),
        ));
    }
    let reverse_key = evacuation_supersession_reverse_key(&relation.new_key);
    let reverse = dbtx
        .raw_get_bytes(&reverse_key)
        .await
        .map_err(db_err)?
        .ok_or_else(|| {
            ExecError::Permanent("journal: supersession canonical row has no reverse index".into())
        })?;
    let reverse_old: IdempotencyKey =
        decode_row_result("evacuation supersession reverse", &reverse_key, &reverse)?;
    if reverse_old != relation.old_key {
        return Err(ExecError::Permanent(
            "journal: incoherent evacuation supersession reverse index".into(),
        ));
    }
    Ok(())
}

async fn ensure_child_namespace_empty(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    key: &IdempotencyKey,
) -> Result<(), ExecError> {
    if child_namespace_is_empty(dbtx, key).await? {
        Ok(())
    } else {
        Err(ExecError::Permanent(format!(
            "journal: replacement child namespace is not empty for {}",
            key.0
        )))
    }
}

/// The common, complete namespace probe for both the exchange transaction and
/// retryable-error outcome confirmation.  Keep all child-owned direct rows and
/// every intent-status index here: checking only the intent row would turn a
/// stale move, ledger identity, or index corruption into permission to retry.
async fn child_namespace_is_empty(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    key: &IdempotencyKey,
) -> Result<bool, ExecError> {
    let direct_keys = [
        intent_key(key),
        move_key(key),
        ledger_key_index(key),
        evacuation_supersession_key(key),
        evacuation_supersession_reverse_key(key),
    ];
    for raw_key in direct_keys {
        if dbtx
            .raw_get_bytes(&raw_key)
            .await
            .map_err(db_err)?
            .is_some()
        {
            return Ok(false);
        }
    }
    for status in ALL_INTENT_STATUSES {
        let index = pending_index_key(status, key);
        if dbtx.raw_get_bytes(&index).await.map_err(db_err)?.is_some() {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn validate_intent_indexes_and_ledger_identity(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    intent: &Intent,
) -> Result<(), ExecError> {
    for status in ALL_INTENT_STATUSES {
        let has_index = dbtx
            .raw_get_bytes(&pending_index_key(status, &intent.idempotency_key))
            .await
            .map_err(db_err)?
            .is_some();
        if has_index != (status == intent.status && is_indexed(status)) {
            return Err(ExecError::Permanent(format!(
                "journal: supersession replay found incoherent {:?} index for {}",
                status, intent.idempotency_key.0
            )));
        }
    }
    let index_key = ledger_key_index(&intent.idempotency_key);
    let seq_bytes = dbtx
        .raw_get_bytes(&index_key)
        .await
        .map_err(db_err)?
        .ok_or_else(|| {
            ExecError::Permanent(format!(
                "journal: supersession replay missing ledger identity for {}",
                intent.idempotency_key.0
            ))
        })?;
    let seq = read_be64(&seq_bytes).ok_or_else(|| {
        ExecError::Permanent("journal: corrupt ledger seq index during supersession replay".into())
    })?;
    let row_key = ledger_row_key(seq);
    let row_bytes = dbtx
        .raw_get_bytes(&row_key)
        .await
        .map_err(db_err)?
        .ok_or_else(|| {
            ExecError::Permanent(
                "journal: supersession replay ledger index points at no row".into(),
            )
        })?;
    let row = decode_canonical_ledger_row(&row_key, &row_bytes)?;
    if row.seq != seq
        || row.correlation_key != intent.idempotency_key
        || row.kind != kind_from_action(&intent.action)
        || row.actor != intent.actor
        || row.reason != intent.reason
        || row.status != status_from_intent(intent.status)
        || row.created_at_ms != intent.created_at_ms
        || row.fees.fee_cap != intent.max_fee
    {
        return Err(ExecError::Permanent(format!(
            "journal: supersession replay found changed ledger identity for {}",
            intent.idempotency_key.0
        )));
    }
    Ok(())
}

async fn ensure_no_other_live_agent_evacuation_holder(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    old_key: &IdempotencyKey,
    source: FederationId,
) -> Result<(), ExecError> {
    for status in [
        IntentStatus::Pending,
        IntentStatus::Executing,
        IntentStatus::Awaiting,
    ] {
        let prefix = pending_index_prefix(status);
        let index_keys = {
            let mut rows = dbtx.raw_find_by_prefix(&prefix).await.map_err(db_err)?;
            let mut keys = Vec::new();
            while let Some((index_key, _)) = rows.next().await {
                keys.push(index_key);
            }
            keys
        };
        for index_key in index_keys {
            let key_bytes = index_key.strip_prefix(prefix.as_slice()).ok_or_else(|| {
                ExecError::Permanent("journal: malformed live evacuation status index".into())
            })?;
            let key = IdempotencyKey(String::from_utf8(key_bytes.to_vec()).map_err(|_| {
                ExecError::Permanent(
                    "journal: live evacuation status index has non-UTF-8 key".into(),
                )
            })?);
            let ikey = intent_key(&key);
            let bytes = dbtx
                .raw_get_bytes(&ikey)
                .await
                .map_err(db_err)?
                .ok_or_else(|| {
                    ExecError::Permanent(
                        "journal: live evacuation status index points at no intent".into(),
                    )
                })?;
            let intent: Intent = decode_row_result("intent", &ikey, &bytes)?;
            if intent.idempotency_key != key || intent.status != status {
                return Err(ExecError::Permanent(
                    "journal: incoherent live evacuation status index".into(),
                ));
            }
            if intent.idempotency_key == *old_key {
                continue;
            }
            if matches!(intent.actor, Actor::Agent { .. })
                && matches!(intent.action, Action::Evacuate { from, .. } if from == source)
            {
                return Err(ExecError::Permanent(format!(
                    "journal: another live agent evacuation already holds source {}",
                    source.to_hex()
                )));
            }
        }
    }
    Ok(())
}

fn validate_marked_evacuation_evidence(
    old: &Intent,
    effective_old_cap: wallet_core::EvacFeeCap,
    evidence: &EvacuationRefusalEvidence,
    new_components: Option<wallet_core::EvacFeeCap>,
    new_amount: Msat,
    new_fee_cap: Msat,
) -> Result<(), ExecError> {
    let Action::Evacuate {
        amount: old_amount,
        fee_cap: old_fee_cap,
        ..
    } = old.action
    else {
        return Err(ExecError::Permanent(
            "journal: marked replacement requires an Evacuate parent".into(),
        ));
    };
    if old.max_fee != Some(old_fee_cap) {
        return Err(ExecError::Permanent(
            "journal: evacuation parent max_fee disagrees with its action fee cap".into(),
        ));
    }
    let new_components = new_components.ok_or_else(|| {
        ExecError::Permanent(
            "journal: replacement child must carry evacuation fee-cap components".into(),
        )
    })?;
    if new_components.at(new_amount) != new_fee_cap {
        return Err(ExecError::Permanent(
            "journal: fresh evacuation cap components do not match its fee cap".into(),
        ));
    }
    if evidence.cap_components != effective_old_cap
        || evidence.requested_net.0 == 0
        || evidence.requested_net > old_amount
        || evidence.diagnostic.is_empty()
    {
        return Err(ExecError::Permanent(
            "journal: evacuation refusal evidence does not describe the current parent".into(),
        ));
    }
    let sample_is_sensible = |sample: &wallet_core::EvacuationQuoteSample| {
        sample.delivered_net.0 > 0
            && sample.delivered_net <= evidence.requested_net
            && sample.fee_cap == effective_old_cap.at(sample.delivered_net)
            && sample.total_fee > sample.fee_cap
            && u128::from(sample.delivered_net.0)
                .checked_add(u128::from(sample.total_fee.0))
                .is_some_and(|source_debit| source_debit <= u128::from(evidence.source_spendable.0))
    };
    if !sample_is_sensible(&evidence.low)
        || !sample_is_sensible(&evidence.high)
        || evidence.low.delivered_net >= evidence.high.delivered_net
        || !assess_evacuation_structural_refusal(effective_old_cap, &evidence.low, &evidence.high)
            .is_some_and(|assessment| assessment.is_structural())
        || !wallet_core::evacuation_cap_qualifies_replacement(evidence, new_components)
    {
        return Err(ExecError::Permanent(
            "journal: replacement evacuation cap is not a justified monotone increase".into(),
        ));
    }
    Ok(())
}

fn ledger_counter_key() -> Vec<u8> {
    vec![TAG_LEDGER_COUNTER]
}

fn read_be64(bytes: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(bytes).ok().map(u64::from_be_bytes)
}

/// Note every Agent ledger append in the same transaction. An initialized, reconciled WatchState
/// advances its exclusive scan frontier only when that frontier exactly names this append's
/// sequence: jumping a partial or stale frontier would falsely certify unseen history. User appends
/// return before reading WatchState, leaving their suffix for the next bounded drain.
///
/// If migration has a valid backlog, this transaction cannot commit its progress without also
/// committing the caller's prospective admission. Fail closed instead; callers must use
/// `get_watch_state` (and therefore `/v1/watch/status`) to converge the bounded migration first.
async fn note_ledger_insert_in(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    record: &OperationRecord,
    seq: u64,
) -> Result<(), ExecError> {
    // User appends have no allocator authority. In particular, never read or rewrite the hot
    // WatchState row on their path: the next drain/Agent admission scans their suffix durably.
    // This trades immediate frontier maintenance for avoiding a global write hotspot on user ops.
    if matches!(record.actor, Actor::User) {
        return Ok(());
    }

    let raw_watch_key = watch_state_key();
    let watch_bytes = dbtx.raw_get_bytes(&raw_watch_key).await.map_err(db_err)?;
    let Actor::Agent { occurrence } = record.actor else {
        unreachable!("User rows returned before WatchState access");
    };
    let mut watch =
        watch_state_with_agent_floor_at_next_in(dbtx, &raw_watch_key, watch_bytes, seq).await?;
    if !watch.agent_floor_reconciled {
        return Err(watch_floor_reconciliation_required_error(&watch));
    }
    watch.occurrence = watch.occurrence.max(occurrence.0);
    let inserted_high_water = seq.checked_add(1).ok_or_else(|| {
        ExecError::Permanent("journal: ledger sequence exhausted at u64::MAX".to_owned())
    })?;
    if !(watch.agent_floor_scan_initialized
        && watch.agent_floor_reconciled
        && watch.agent_floor_scan_high_water == seq)
    {
        return Err(watch_floor_reconciliation_required_error(&watch));
    }
    // The row is being inserted in this transaction and is therefore known readable.
    watch.agent_floor_scan_high_water = inserted_high_water;
    dbtx.raw_insert_bytes(&raw_watch_key, &encode_row(&watch)?)
        .await
        .map_err(db_err)?;
    Ok(())
}

/// The ONE writer for every ledger row (spec §9.2). Given a caller-supplied `dbtx` and a
/// correlation `key`, look up `0x06`:
/// - PRESENT → read the existing `0x05` row, call `build(Some(existing), seq)`; `None` is a
///   no-op (terminal-immutable / no-change), `Some` overwrites the row at the SAME seq.
/// - ABSENT → allocate the next `seq` from `0x07`, call `build(None, seq)`; `Some` inserts the
///   row + the `0x06` index + the incremented counter (all in this dbtx), `None` touches
///   nothing (no seq is burned).
async fn ledger_upsert_in(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    key: &IdempotencyKey,
    build: impl FnOnce(Option<OperationRecord>, u64) -> Option<OperationRecord>,
) -> Result<(), ExecError> {
    let index_key = ledger_key_index(key);
    if let Some(seq_bytes) = dbtx.raw_get_bytes(&index_key).await.map_err(db_err)? {
        let seq = read_be64(&seq_bytes).ok_or_else(|| {
            ExecError::Permanent(format!("journal: corrupt ledger seq index for {}", key.0))
        })?;
        let row_key = ledger_row_key(seq);
        let bytes = dbtx
            .raw_get_bytes(&row_key)
            .await
            .map_err(db_err)?
            .ok_or_else(|| {
                ExecError::Permanent(format!(
                    "journal: ledger index for {} points at a missing row (seq {seq})",
                    key.0
                ))
            })?;
        let existing = decode_canonical_ledger_row(&row_key, &bytes)?;
        if let Some(next) = build(Some(existing), seq) {
            dbtx.raw_insert_bytes(&row_key, &encode_row(&next)?)
                .await
                .map_err(db_err)?;
        }
    } else {
        let (next_seq, successor) = next_ledger_sequence_in(dbtx).await?;
        if let Some(rec) = build(None, next_seq) {
            // An Agent ledger admission can be made by public paths that never touch the watch
            // scheduler. Raise the durable floor and its append-only scan high-water in this SAME
            // transaction. A legacy checkpoint is initialized through its bounded canonical cursor,
            // never by assuming historical rows were readable.
            note_ledger_insert_in(dbtx, &rec, next_seq).await?;
            dbtx.raw_insert_bytes(&ledger_counter_key(), &successor.to_be_bytes())
                .await
                .map_err(db_err)?;
            dbtx.raw_insert_bytes(&ledger_row_key(next_seq), &encode_row(&rec)?)
                .await
                .map_err(db_err)?;
            dbtx.raw_insert_bytes(&index_key, &next_seq.to_be_bytes())
                .await
                .map_err(db_err)?;
        }
    }
    Ok(())
}

/// Reject the historical standalone raw writers when this transaction selects an intent-owned raw
/// row.  The intent existence check and current-ledger-row selection deliberately share the caller's
/// transaction: a delayed result from attempt N must not inspect one attempt and then mutate a
/// retry's N+1 row after a separate snapshot/commit boundary.
///
/// Standalone raw rows have no intent key, and non-raw intent rows have no raw operation identity,
/// so both remain available to the legacy writers that still own those production paths.
async fn reject_legacy_intent_backed_raw_writer(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    key: &IdempotencyKey,
    writer: &str,
) -> Result<(), ExecError> {
    let ikey = intent_key(key);
    if dbtx.raw_get_bytes(&ikey).await.map_err(db_err)?.is_none() {
        return Ok(());
    }

    let index_key = ledger_key_index(key);
    let Some(seq_bytes) = dbtx.raw_get_bytes(&index_key).await.map_err(db_err)? else {
        return Ok(());
    };
    let seq = read_be64(&seq_bytes).ok_or_else(|| {
        ExecError::Permanent(format!("journal: corrupt ledger seq index for {}", key.0))
    })?;
    let row_key = ledger_row_key(seq);
    let bytes = dbtx
        .raw_get_bytes(&row_key)
        .await
        .map_err(db_err)?
        .ok_or_else(|| {
            ExecError::Permanent(format!(
                "journal: ledger index for {} points at a missing row (seq {seq})",
                key.0
            ))
        })?;
    let row = decode_canonical_ledger_row(&row_key, &bytes)?;
    if raw_row_parts(&row.kind).is_some() {
        return Err(ExecError::Permanent(format!(
            "journal: legacy {writer} cannot mutate intent-backed raw operation {}; \
             use an attempt-fenced raw writer",
            key.0
        )));
    }
    Ok(())
}

/// Read the `0x02` [`MoveRecord`] for `key` from the caller's `dbtx` — the same-partition,
/// same-dbtx read that refreshes an intent-backed ledger row's fees/op-ids/gateway (§9.2).
async fn read_move_row_in(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    key: &IdempotencyKey,
) -> Result<Option<MoveRecord>, ExecError> {
    let raw_key = move_key(key);
    match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
        Some(bytes) => Ok(Some(decode_row_result("move record", &raw_key, &bytes)?)),
        None => Ok(None),
    }
}

/// Advance (or create) the ledger row that describes `intent`, refreshing fees/op-ids/gateway
/// from the `0x02` move row on EVERY write (§9.2 — an in-flight `DirectInflow`/`Move` carries
/// its `recv_op`/`send_op`/gateway/fee before it settles, and `history`/`show` must reflect
/// that). Runs inside the caller's dbtx.
async fn write_intent_ledger_row(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
    intent: &Intent,
    now_ms: u64,
    error: Option<&str>,
) -> Result<(), ExecError> {
    let move_rec = read_move_row_in(dbtx, &intent.idempotency_key).await?;
    let status = status_from_intent(intent.status);
    // §9.2: on `Failed`, the executor-provided diagnostic first, `MoveRecord.outcome` fallback.
    let err_owned: Option<String> = if status == OperationStatus::Failed {
        error
            .map(str::to_owned)
            .or_else(|| move_rec.as_ref().and_then(|m| m.outcome.clone()))
    } else {
        None
    };
    ledger_upsert_in(dbtx, &intent.idempotency_key, |existing, seq| {
        let mut next = match existing {
            Some(rec) => advance(
                &rec,
                status,
                now_ms,
                None,
                err_owned.as_deref(),
                WriteKind::Authoritative,
            )?,
            None => fresh_intent_record(seq, intent, status, now_ms, err_owned.as_deref()),
        };
        if let Some(mv) = &move_rec {
            refresh_from_move(&mut next, mv);
        }
        refresh_from_intent_artifact(&mut next, intent);
        Some(next)
    })
    .await
}

fn refresh_from_intent_artifact(record: &mut OperationRecord, intent: &Intent) {
    let Some(operation_id) = intent.operation_id else {
        return;
    };
    match &mut record.kind {
        wallet_core::OperationKind::Pay { op_id, .. }
        | wallet_core::OperationKind::Receive { op_id, .. } => *op_id = Some(operation_id),
        _ => {}
    }
}

/// A fresh ledger row for an intent's first observation (§9.2). Op-ids/gateway/receive/send
/// fees start empty and are filled by [`refresh_from_move`] on this and every later write.
fn fresh_intent_record(
    seq: u64,
    intent: &Intent,
    status: OperationStatus,
    now_ms: u64,
    error: Option<&str>,
) -> OperationRecord {
    OperationRecord {
        seq,
        correlation_key: intent.idempotency_key.clone(),
        kind: kind_from_action(&intent.action),
        actor: intent.actor,
        reason: intent.reason,
        status,
        created_at_ms: intent.created_at_ms,
        updated_at_ms: now_ms,
        fees: FeeBreakdown {
            fee_cap: intent.max_fee,
            receive_fee: None,
            send_fee_quoted: None,
        },
        error: error.map(str::to_owned),
        repaired: false,
    }
}

/// Copy the `0x02` move row's op-ids, gateway, quoted fees, executed amount and enforced fee
/// cap onto an intent-backed ledger row (§9.2). `Move`'s two op-ids come from here (not the
/// single-op `RawOpUpdate`); a `None` on the move row never clobbers a value already on the
/// ledger row.
///
/// The amount and the cap are stamped TOGETHER, and only on the two move-shaped kinds. The row
/// is seeded from the PLANNED action (`kind_from_action`, `fresh_intent_record`), so for an
/// evacuation the sizing search clamped, both seeded values describe a move that never
/// happened. Refreshing just one is worse than refreshing neither: `amount = planned,
/// fee_cap = enforced` is internally false — an auditor recomputing the cap from the displayed
/// amount derives a different number — which is why they are written as one pair here, exactly
/// as `apply_evacuation_sizing` writes them onto the [`MoveRecord`] this reads (ADR-0029).
///
/// The pair is stamped ONLY once a leg has COMMITTED, which is what
/// [`crate::executor::has_move_artifact`] tests.
/// Before that the move row holds a pre-operation DRAFT: `size_fresh_evacuation` re-sizes it from
/// the intent on every pre-receive pass, and the pre-mint cap re-check persists it and then
/// returns `Retryable` — so a row stamped from a draft would report an amount and a cap that no
/// operation ever ran under, and a permanent failure there would freeze that pair onto an
/// immutable terminal row.
///
/// It CALLS that predicate rather than re-deriving it, and must keep doing so: `has_move_artifact`
/// is precisely what stops sizing rewriting `amount`/`fee_cap`, so any narrowing of it that this
/// gate did not follow would let sizing move a pair the ledger had already stamped — silently, and
/// with no test failing.
///
/// The op-ids, gateway and quoted fees below are NOT gated: each is already `None` until it is
/// real, and a gateway is chosen pre-mint and is informative on a row that never got further.
fn refresh_from_move(rec: &mut OperationRecord, mv: &MoveRecord) {
    let committed = crate::executor::has_move_artifact(mv);
    match &mut rec.kind {
        OperationKind::Move {
            send_op,
            recv_op,
            gateway,
            amount,
            ..
        } => {
            if mv.send_op.is_some() {
                *send_op = mv.send_op;
            }
            if mv.recv_op.is_some() {
                *recv_op = mv.recv_op;
            }
            *gateway = Some(mv.gateway.clone());
            if committed {
                *amount = mv.amount;
                rec.fees.fee_cap = Some(mv.fee_cap);
            }
        }
        OperationKind::DirectInflow {
            recv_op,
            gateway,
            amount,
            ..
        } => {
            if mv.recv_op.is_some() {
                *recv_op = mv.recv_op;
            }
            *gateway = Some(mv.gateway.clone());
            if committed {
                *amount = mv.amount;
                rec.fees.fee_cap = Some(mv.fee_cap);
            }
        }
        _ => {}
    }
    if mv.receive_fee_quoted.is_some() {
        rec.fees.receive_fee = mv.receive_fee_quoted;
    }
    if mv.send_fee_quoted.is_some() {
        rec.fees.send_fee_quoted = mv.send_fee_quoted;
    }
}

// --- key encoding ---

fn intent_key(key: &IdempotencyKey) -> Vec<u8> {
    tagged(TAG_INTENT, key.0.as_bytes())
}

fn move_key(key: &IdempotencyKey) -> Vec<u8> {
    tagged(TAG_MOVE, key.0.as_bytes())
}

fn federation_key(id: &FederationId) -> Vec<u8> {
    tagged(TAG_FEDERATION, &id.0)
}

fn probe_key(id: &FederationId) -> Vec<u8> {
    tagged(TAG_PROBE, &id.0)
}

fn candidate_key(id: &FederationId) -> Vec<u8> {
    tagged(TAG_CANDIDATE, &id.0)
}

fn watch_state_key() -> Vec<u8> {
    vec![TAG_WATCH_STATE]
}

fn policy_key() -> Vec<u8> {
    vec![TAG_POLICY]
}

fn evacuation_supersession_key(key: &IdempotencyKey) -> Vec<u8> {
    tagged(TAG_EVACUATION_SUPERSESSION, key.0.as_bytes())
}

fn evacuation_supersession_reverse_key(key: &IdempotencyKey) -> Vec<u8> {
    tagged(TAG_EVACUATION_SUPERSESSION_REVERSE, key.0.as_bytes())
}

/// Whether `row` is an AGENT `join:` row that SUCCEEDED and created a NEW partition (§5.1.4):
/// `actor: Agent`, a `Join` kind, `Succeeded`, and NOT a no-op re-open. Failed attempts and
/// no-op re-opens write `join:` rows too but created no partition, so they never count — the
/// no-op re-open is the ONE `Succeeded` join case with no partition, marked by
/// [`JOIN_NOOP_REOPEN_NOTE`] in `error`. This reads the immutable, monotonic history the
/// lifetime/weekly caps trust, never the mutable candidate state (§P1).
fn is_agent_new_partition_join(row: &OperationRecord) -> bool {
    matches!(row.actor, Actor::Agent { .. })
        && matches!(row.kind, OperationKind::Join { .. })
        && row.status == OperationStatus::Succeeded
        && row.error.as_deref() != Some(JOIN_NOOP_REOPEN_NOTE)
}

fn join_row_fed(row: &OperationRecord) -> Option<FederationId> {
    match row.kind {
        OperationKind::Join { fed } => Some(fed),
        _ => None,
    }
}

fn is_non_terminal_agent_join_for(row: &OperationRecord, id: FederationId) -> bool {
    matches!(row.actor, Actor::Agent { .. })
        && matches!(row.kind, OperationKind::Join { fed } if fed == id)
        && !row.status.is_terminal()
}

fn join_attempt_matches_joined_at(created_at_ms: u64, joined_at_secs: u64) -> bool {
    let joined_at_ms = joined_at_secs.saturating_mul(1000);
    created_at_ms >= joined_at_ms.saturating_sub(JOINED_AT_SLACK_MS)
        && created_at_ms <= joined_at_ms.saturating_add(JOINED_AT_SLACK_MS)
}

fn join_attempt_could_have_created_registry_entry(created_at_ms: u64, joined_at_secs: u64) -> bool {
    let joined_at_ms = joined_at_secs.saturating_mul(1000);
    created_at_ms <= joined_at_ms.saturating_add(JOINED_AT_SLACK_MS)
}

fn count_saturating_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

/// §5.0.4 TIME-AWARE probe-attempt retention (a count-only cap could truncate the very
/// successes the 24h `min_span` needs whenever probes run more often than span/cap). Keep
/// every attempt younger than the DEFAULT `ttl_ms` — exactly the verdict's PASS-evaluation
/// window, so pruning can never flip a pass — PLUS the newest SUCCESS and the newest
/// attempt regardless of age (the evidence distinguishing `Expired` from `NeverProbed`
/// after a long quiet spell), bounded by the newest [`PROBE_HISTORY_CAP`] rows. `attempts`
/// is chronological (append order); the result preserves that order. Pure, so retention is
/// unit-tested without a database.
pub fn prune_probe_attempts(attempts: Vec<ProbeAttempt>, now_ms: u64) -> Vec<ProbeAttempt> {
    let default_ttl_ms = ProbePolicy::default().ttl_ms;
    let newest = attempts.len().checked_sub(1);
    // `probe_verdict` qualifies a stale success by its SOURCE and STRENGTH (amount ≥,
    // fee cap ≤ the evaluating policy), so retaining only ONE whole-fed newest success
    // would let a later success from a different source — or a weaker `--amount`/`--fee-cap`
    // smoke probe from the SAME source — evict the stale success that proves an older
    // DEFAULT-sized pass, turning that pair's aged-out `Expired` into a false `NeverProbed`.
    // Keep, per source: (a) the newest success (any strength) AND (b) the newest success
    // that qualifies under the DEFAULT policy — the strength `status`/gating actually
    // evaluate. Both are bounded by the joined-fed count (small). BOUND (deliberate): only
    // the default policy's stale evidence is preserved, not every possible strictness. No
    // 5.0 caller evaluates a NON-default policy over STALE evidence — `status` and 5.1's
    // gate both read the DEFAULT-policy `active_probe` verdict (§5.0.6), and the `probe`
    // verb evaluates its own (possibly stricter) flags only against FRESH post-attempt
    // state. A future gate that trusts a stricter-than-default policy must revisit
    // retention (retaining every strictness would require keeping all successes, defeating
    // the bound); flagged for 5.1, not built speculatively here.
    let default_policy = ProbePolicy::default();
    let default_qualifies = |a: &ProbeAttempt| {
        a.ok && a.amount_msat >= default_policy.amount_msat
            && a.leg_fee_cap_msat <= default_policy.leg_fee_cap_msat
    };
    let mut newest_success_by_source: std::collections::BTreeMap<FederationId, usize> =
        std::collections::BTreeMap::new();
    let mut newest_default_success_by_source: std::collections::BTreeMap<FederationId, usize> =
        std::collections::BTreeMap::new();
    for (i, a) in attempts.iter().enumerate() {
        if a.ok {
            newest_success_by_source.insert(a.from, i);
        }
        if default_qualifies(a) {
            newest_default_success_by_source.insert(a.from, i);
        }
    }
    let mut kept: Vec<ProbeAttempt> = attempts
        .into_iter()
        .enumerate()
        .filter(|(i, a)| {
            now_ms.saturating_sub(a.at_ms) <= default_ttl_ms
                || Some(*i) == newest
                || newest_success_by_source.get(&a.from) == Some(i)
                || newest_default_success_by_source.get(&a.from) == Some(i)
        })
        .map(|(_, a)| a)
        .collect();
    // The hard backstop wins over the keep rules: retain only the newest CAP rows.
    let excess = kept.len().saturating_sub(PROBE_HISTORY_CAP);
    kept.split_off(excess)
}

fn tagged(tag: u8, id_bytes: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + id_bytes.len());
    k.push(tag);
    k.extend_from_slice(id_bytes);
    k
}

/// `[TAG_PENDING_INDEX, status_byte] ++ idempotency_key_bytes`.
fn pending_index_key(status: IntentStatus, key: &IdempotencyKey) -> Vec<u8> {
    let id_bytes = key.0.as_bytes();
    let mut k = pending_index_prefix(status);
    k.reserve(id_bytes.len());
    k.extend_from_slice(id_bytes);
    k
}

fn pending_index_prefix(status: IntentStatus) -> Vec<u8> {
    vec![TAG_PENDING_INDEX, status_byte(status)]
}

/// A stable byte per [`IntentStatus`] for the index's second key byte. Only the
/// [`is_indexed`] statuses (`Pending`/`Executing`/`Failed`/`Awaiting`) ever reach a
/// `PendingIndexKey`, but every status maps to a byte so the unindexed `Done` value is still
/// well-defined.
fn status_byte(status: IntentStatus) -> u8 {
    match status {
        IntentStatus::Pending => 0,
        IntentStatus::Executing => 1,
        IntentStatus::Done => 2,
        IntentStatus::Awaiting => 3,
        IntentStatus::Failed => 4,
    }
}

/// Whether a status gets a `PendingIndexKey` row. Only the SCANNED statuses are indexed:
/// `Pending`/`Executing` (read by [`Journal::pending`]), `Failed` ([`Journal::failed`]), and
/// `Awaiting` ([`FedimintJournal::awaiting`], the resume-time subscription-rehydration scan,
/// spec §9.3). Only the terminal `Done` is never scanned, so indexing it would leave a dead
/// row in durable storage.
fn is_indexed(status: IntentStatus) -> bool {
    matches!(
        status,
        IntentStatus::Pending
            | IntentStatus::Executing
            | IntentStatus::Failed
            | IntentStatus::Awaiting
    )
}

// --- error mapping ---

/// Treat storage-layer failures as transient → `Retryable`, including commit failures. The
/// caller's next reconcile/resume pass retries rather than deciding a partial durable state is
/// terminal.
fn db_err(e: DatabaseError) -> ExecError {
    ExecError::Retryable(format!("journal db error: {e}"))
}

/// Keep all optimistic WatchState read/check/write operations on the same repository error
/// boundary as the other journal autocommits.
fn map_autocommit_error(error: AutocommitError<ExecError>) -> ExecError {
    match error {
        AutocommitError::CommitFailed { last_error, .. } => db_err(last_error),
        AutocommitError::ClosureError { error, .. } => error,
    }
}

#[derive(serde::Serialize)]
struct StoredRowRef<'a, T> {
    version: u8,
    data: &'a T,
}

#[derive(serde::Deserialize)]
struct StoredRow {
    version: u8,
    data: serde_json::Value,
}

fn encode_row<T>(value: &T) -> Result<Vec<u8>, ExecError>
where
    T: Serialize,
{
    serde_json::to_vec(&StoredRowRef {
        version: ROW_VERSION,
        data: value,
    })
    .map_err(serde_err)
}

/// Decode a row for a `Result`-returning read. A decode failure is data corruption →
/// [`ExecError::Permanent`] (not transient), surfaced rather than panicked.
fn decode_row_result<T>(kind: &str, key: &[u8], bytes: &[u8]) -> Result<T, ExecError>
where
    T: DeserializeOwned,
{
    let row: StoredRow = serde_json::from_slice(bytes).map_err(|e| decode_err(kind, key, e))?;
    if row.version != ROW_VERSION {
        return Err(ExecError::Permanent(format!(
            "journal: unsupported {kind} row version {} for {key:?} (supported: {ROW_VERSION})",
            row.version
        )));
    }
    serde_json::from_value(row.data).map_err(|e| decode_err(kind, key, e))
}

fn decode_candidate_row(
    key_id: FederationId,
    raw_key: &[u8],
    bytes: &[u8],
) -> Result<CandidateRecord, ExecError> {
    let rec: CandidateRecord = decode_row_result("candidate", raw_key, bytes)?;
    if rec.id != key_id {
        return Err(ExecError::Permanent(format!(
            "journal: candidate row key id {} does not match embedded id {} for {raw_key:?}",
            key_id.to_hex(),
            rec.id.to_hex()
        )));
    }
    Ok(rec)
}

/// A serde encode/decode failure is a data/logic bug, not transient → `Permanent`.
fn serde_err(e: serde_json::Error) -> ExecError {
    ExecError::Permanent(format!("journal serde error: {e}"))
}

fn decode_err(kind: &str, key: &[u8], e: serde_json::Error) -> ExecError {
    ExecError::Permanent(format!("journal: failed to decode {kind} row {key:?}: {e}"))
}

#[cfg(test)]
mod replacement_foundation_tests {
    use super::*;
    use crate::Invoice;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt;
    use wallet_core::{EvacuationQuoteSample, Journal, MovePhase};

    const NOW: u64 = 1_700_000_000_000;

    fn fed(n: u8) -> FederationId {
        FederationId([n; 32])
    }

    fn cap(base: u64, bps: u16) -> wallet_core::EvacFeeCap {
        wallet_core::EvacFeeCap {
            base_msat: Msat(base),
            bps,
        }
    }

    fn evidence() -> EvacuationRefusalEvidence {
        EvacuationRefusalEvidence {
            cap_components: cap(10, 100),
            requested_net: Msat(100),
            source_spendable: Msat(200),
            low: EvacuationQuoteSample {
                delivered_net: Msat(10),
                total_fee: Msat(20),
                fee_cap: Msat(10),
            },
            high: EvacuationQuoteSample {
                delivered_net: Msat(100),
                total_fee: Msat(30),
                fee_cap: Msat(11),
            },
            diagnostic: "two measured refusals".into(),
            measured_at_ms: NOW,
        }
    }

    fn decision(key: &str, occurrence: u64, fee: wallet_core::EvacFeeCap) -> AllocatorDecision {
        AllocatorDecision {
            action: Action::Evacuate {
                from: fed(1),
                to: fed(2),
                amount: Msat(100),
                fee_cap: fee.at(Msat(100)),
                gateway: None,
                fee_cap_components: Some(fee),
            },
            reason: wallet_core::ReasonCode::ShutdownNotice,
            occurrence: Occurrence(occurrence),
            idempotency_key: IdempotencyKey(key.into()),
        }
    }

    async fn marked_parent(journal: &FedimintJournal, key: &str) -> Intent {
        let parent = decision(key, 1, cap(10, 100));
        let mut intent = Intent::from_decision(
            &parent,
            Actor::Agent {
                occurrence: Occurrence(1),
            },
            NOW,
        );
        intent.evacuation_refusal = Some(evidence());
        journal.upsert(&intent).await.expect("seed marked parent");
        intent
    }

    fn make_journal() -> FedimintJournal {
        FedimintJournal::with_clock(MemDatabase::new().into_database(), || NOW)
    }

    async fn seed_agent_refusal(journal: &FedimintJournal, occurrence: u64) {
        let reason = ReasonCode::SpendingBelowTarget;
        let decision = AllocatorDecision {
            action: Action::RefuseInflow {
                fed: fed(1),
                reason,
                diagnostics: Default::default(),
            },
            reason,
            occurrence: Occurrence(occurrence),
            idempotency_key: IdempotencyKey(format!("refuse:watch-floor:{occurrence}")),
        };
        journal
            .record_refusals(&[decision], Occurrence(occurrence), NOW)
            .await
            .expect("seed non-Tick Agent ledger row");
    }

    /// Seed a legacy checkpoint with more canonical valid rows than one migration access can read.
    /// The one Agent row inside the first chunk makes the post-convergence successor deterministic.
    async fn seed_legacy_valid_watch_backlog(journal: &FedimintJournal) {
        seed_agent_refusal(journal, 29).await;
        journal
            .put_watch_state(&WatchState::default())
            .await
            .expect("replace the current checkpoint with a legacy shape");
        let mut valid = journal
            .history(10, None)
            .await
            .expect("read valid ledger template")
            .into_iter()
            .next()
            .expect("seed Agent refusal exists");
        valid.actor = Actor::User;
        let mut dbtx = journal.db.begin_transaction().await;
        for seq in 1..=WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 {
            valid.seq = seq;
            if seq == 10 {
                valid.actor = Actor::Agent {
                    occurrence: Occurrence(73),
                };
            } else {
                valid.actor = Actor::User;
            }
            dbtx.raw_insert_bytes(
                &ledger_row_key(seq),
                &encode_row(&valid).expect("encode valid backlog row"),
            )
            .await
            .expect("seed canonical valid backlog row");
        }
        dbtx.raw_insert_bytes(
            &ledger_counter_key(),
            &(WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 1).to_be_bytes(),
        )
        .await
        .expect("publish valid backlog counter");
        dbtx.commit_tx_result()
            .await
            .expect("commit valid legacy backlog");
    }

    #[tokio::test]
    async fn advance_and_observe_immediately_drain_valid_multi_chunk_backlog() {
        let journal = make_journal();
        seed_legacy_valid_watch_backlog(&journal).await;

        assert_eq!(
            journal
                .advance_watch_occurrence()
                .await
                .expect(
                    "advance drains valid chunks without waiting for status or scheduler cadence"
                )
                .occurrence,
            74,
            "the first allocation is strictly greater than the historical Agent maximum"
        );

        journal
            .put_watch_state(&WatchState::default())
            .await
            .expect("repeat from a legacy checkpoint for standalone observation");
        assert!(
            journal
                .observe_watch_occurrence(90)
                .await
                .expect("standalone observation drains valid backlog immediately")
                .agent_floor_reconciled
        );
        assert_eq!(
            journal
                .observe_watch_occurrence(90)
                .await
                .expect("observe remains monotonic after immediate drain")
                .occurrence,
            90
        );
    }

    #[tokio::test]
    async fn autocommit_retries_a_forced_concurrent_watch_advance_conflict() {
        let journal = Arc::new(make_journal());
        journal
            .get_watch_state()
            .await
            .expect("seed reconciled empty state");
        journal.rendezvous_two_watch_state_autocommits_for_test();
        let (left, right) = tokio::join!(
            journal.advance_watch_occurrence(),
            journal.advance_watch_occurrence()
        );
        let mut occurrences = [
            left.expect("left advance").occurrence,
            right.expect("right advance").occurrence,
        ];
        occurrences.sort_unstable();
        assert_eq!(
            occurrences,
            [1, 2],
            "one autocommit closure must retry its stale read rather than losing an occurrence"
        );
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read authoritative state")
                .occurrence,
            2
        );
    }

    #[tokio::test]
    async fn forced_migration_get_and_advance_conflict_preserves_floor_and_allocation() {
        let journal = Arc::new(make_journal());
        seed_legacy_valid_watch_backlog(journal.as_ref()).await;
        journal.rendezvous_two_watch_state_autocommits_for_test();
        let (migration, advance) = tokio::join!(
            journal.get_watch_state(),
            journal.advance_watch_occurrence()
        );
        assert!(
            migration.is_ok(),
            "migration reader retries its forced stale snapshot"
        );
        let advanced = advance.expect("advance retries after migration conflict");
        assert_eq!(advanced.occurrence, 74);
        assert!(advanced.agent_floor_reconciled);
        assert_eq!(
            advanced.agent_floor_scan_high_water,
            WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 1
        );
    }

    #[tokio::test]
    async fn forced_discovery_and_advance_conflict_preserves_both_fields() {
        let journal = Arc::new(make_journal());
        journal
            .get_watch_state()
            .await
            .expect("seed reconciled watch state");
        journal.rendezvous_two_watch_state_autocommits_for_test();
        let (discovery, advance) = tokio::join!(
            journal.put_watch_discovery_state(
                Some(FederationId([0x33; 32])),
                true,
                Some(77),
                vec![]
            ),
            journal.advance_watch_occurrence()
        );
        assert_eq!(advance.expect("advance").occurrence, 1);
        let discovery = discovery.expect("discovery update retries its stale snapshot");
        assert_eq!(discovery.occurrence, 1);
        assert_eq!(discovery.discover_cursor, Some(FederationId([0x33; 32])));
        assert!(discovery.discover_backlog);
        assert_eq!(discovery.last_discover_ms, 77);
    }

    #[tokio::test]
    async fn opaque_canonical_row_fences_high_legacy_watch_state_until_exact_restore() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        journal
            .put_watch_state(&WatchState {
                // A nonzero/high old scalar is still not an allocation proof: the opaque canonical
                // row may encode any u64 Agent occurrence. Old direct Agent admissions could also
                // have left this scalar below their ledger row, so neither direction is authority.
                occurrence: 9_000,
                last_discover_ms: 17,
                ..WatchState::default()
            })
            .await
            .expect("seed legacy checkpoint");
        let row_key = ledger_row_key(0);
        let mut dbtx = journal.db.begin_transaction_nc().await;
        let original = dbtx
            .raw_get_bytes(&row_key)
            .await
            .expect("read valid row for later restore")
            .expect("seed row exists");
        drop(dbtx);
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&row_key, b"corrupt Agent row")
            .await
            .expect("corrupt canonical row");
        dbtx.commit_tx_result().await.expect("commit corruption");

        assert!(
            journal.advance_watch_occurrence().await.is_err(),
            "unreadable canonical row must block occurrence allocation"
        );
        assert!(
            journal.observe_watch_occurrence(9_001).await.is_err(),
            "a supplied standalone observation cannot override an opaque canonical occurrence"
        );
        let blocked_key = IdempotencyKey("refuse:watch-floor:opaque-blocked-direct".to_owned());
        let blocked_decision = AllocatorDecision {
            action: Action::RefuseInflow {
                fed: fed(1),
                reason: ReasonCode::SpendingBelowTarget,
                diagnostics: Default::default(),
            },
            reason: ReasonCode::SpendingBelowTarget,
            occurrence: Occurrence(9_001),
            idempotency_key: blocked_key.clone(),
        };
        assert!(
            journal
                .record_refusals(
                    std::slice::from_ref(&blocked_decision),
                    Occurrence(9_001),
                    NOW,
                )
                .await
                .is_err(),
            "a fresh direct Agent append cannot bypass an unreadable canonical occurrence"
        );
        let blocked = journal
            .get_watch_state()
            .await
            .expect("status remains available while repair is required");
        assert!(!blocked.agent_floor_reconciled);
        assert_eq!(
            blocked.agent_floor_unreadable_ledger_keys,
            vec![row_key.clone()]
        );
        assert_eq!(
            journal
                .get(&blocked_key)
                .await
                .expect("read blocked fresh append"),
            None,
            "the blocked Agent admission creates neither an intent nor a ledger row"
        );

        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&row_key, &original)
            .await
            .expect("restore exact valid row");
        dbtx.commit_tx_result()
            .await
            .expect("commit row restoration");
        assert!(
            journal
                .get_watch_state()
                .await
                .expect("restored row converges status")
                .agent_floor_reconciled
        );
        assert_eq!(
            journal
                .advance_watch_occurrence()
                .await
                .expect("allocation resumes only after valid restore")
                .occurrence,
            9_001
        );
        journal
            .record_refusals(&[blocked_decision], Occurrence(9_002), NOW)
            .await
            .expect("a fresh Agent append resumes only after exact-row restoration");
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read converged restored checkpoint")
                .occurrence,
            9_002,
            "the restored canonical scan and new direct Agent append share one durable floor"
        );
    }

    #[tokio::test]
    async fn direct_agent_admission_cannot_bypass_unreconciled_watch_floor() {
        let journal = make_journal();
        seed_legacy_valid_watch_backlog(&journal).await;
        let key = IdempotencyKey("refuse:watch-floor:blocked-direct".to_owned());
        let decision = AllocatorDecision {
            action: Action::RefuseInflow {
                fed: fed(1),
                reason: ReasonCode::SpendingBelowTarget,
                diagnostics: Default::default(),
            },
            reason: ReasonCode::SpendingBelowTarget,
            occurrence: Occurrence(90),
            idempotency_key: key.clone(),
        };
        assert!(
            journal
                .record_refusals(std::slice::from_ref(&decision), Occurrence(90), NOW)
                .await
                .is_err(),
            "direct Agent admission must fail closed before a partial floor can be certified"
        );
        let mut dbtx = journal.db.begin_transaction_nc().await;
        assert!(dbtx
            .raw_get_bytes(&ledger_row_key(257))
            .await
            .expect("inspect prospective ledger row")
            .is_none());
        assert!(dbtx
            .raw_get_bytes(&ledger_key_index(&key))
            .await
            .expect("inspect prospective key index")
            .is_none());
        assert!(
            dbtx.raw_get_bytes(&ledger_counter_key())
                .await
                .expect("inspect ledger counter")
                .is_some_and(|counter| read_be64(&counter) == Some(257)),
            "blocked admission must not allocate or burn a ledger sequence"
        );
        drop(dbtx);

        assert!(
            !journal
                .get_watch_state()
                .await
                .expect("first status chunk remains observable")
                .agent_floor_reconciled
        );
        assert!(
            journal
                .get_watch_state()
                .await
                .expect("second status chunk converges")
                .agent_floor_reconciled
        );
        journal
            .record_refusals(&[decision], Occurrence(90), NOW)
            .await
            .expect("direct Agent admission succeeds after status convergence");
        let mut dbtx = journal.db.begin_transaction_nc().await;
        assert!(dbtx
            .raw_get_bytes(&ledger_row_key(257))
            .await
            .expect("read admitted ledger row")
            .is_some());
        assert!(dbtx
            .raw_get_bytes(&ledger_key_index(&key))
            .await
            .expect("read admitted key index")
            .is_some());
    }

    #[tokio::test]
    async fn counter_tail_mismatch_blocks_status_and_direct_agent_overwrite() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        let mut high = journal
            .history(10, None)
            .await
            .expect("read valid Agent row")
            .into_iter()
            .next()
            .expect("seed row exists");
        high.seq = 5;
        high.actor = Actor::Agent {
            occurrence: Occurrence(80),
        };
        let high_key = ledger_row_key(5);
        let high_bytes = encode_row(&high).expect("encode high Agent row");
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&high_key, &high_bytes)
            .await
            .expect("seed out-of-counter high Agent row");
        dbtx.raw_remove_entry(&ledger_counter_key())
            .await
            .expect("remove counter");
        dbtx.commit_tx_result()
            .await
            .expect("commit missing-counter mismatch");
        assert!(
            journal.get_watch_state().await.is_err(),
            "a missing counter cannot hide the physical high Agent tail"
        );

        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&ledger_counter_key(), &5_u64.to_be_bytes())
            .await
            .expect("seed low counter which would overwrite row five");
        dbtx.commit_tx_result()
            .await
            .expect("commit low-counter mismatch");
        let key = IdempotencyKey("refuse:watch-floor:tail-mismatch".to_owned());
        let decision = AllocatorDecision {
            action: Action::RefuseInflow {
                fed: fed(1),
                reason: ReasonCode::SpendingBelowTarget,
                diagnostics: Default::default(),
            },
            reason: ReasonCode::SpendingBelowTarget,
            occurrence: Occurrence(81),
            idempotency_key: key.clone(),
        };
        assert!(
            journal
                .record_refusals(&[decision], Occurrence(81), NOW)
                .await
                .is_err(),
            "direct Agent admission must not overwrite the physical tail at a low counter"
        );
        let mut dbtx = journal.db.begin_transaction_nc().await;
        assert_eq!(
            dbtx.raw_get_bytes(&high_key)
                .await
                .expect("read protected high row"),
            Some(high_bytes)
        );
        assert!(dbtx
            .raw_get_bytes(&ledger_key_index(&key))
            .await
            .expect("read rejected direct index")
            .is_none());
        assert_eq!(
            dbtx.raw_get_bytes(&ledger_counter_key())
                .await
                .expect("read protected low counter"),
            Some(5_u64.to_be_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn stale_counter_blocks_fresh_user_admission_without_overwriting_the_tail() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        let mut high = journal
            .history(10, None)
            .await
            .expect("read valid row")
            .into_iter()
            .next()
            .expect("seed row exists");
        high.seq = 5;
        high.actor = Actor::User;
        let high_key = ledger_row_key(5);
        let high_bytes = encode_row(&high).expect("encode physical user tail");
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&high_key, &high_bytes)
            .await
            .expect("seed physical tail");
        dbtx.raw_insert_bytes(&ledger_counter_key(), &5_u64.to_be_bytes())
            .await
            .expect("seed stale counter at occupied tail sequence");
        dbtx.commit_tx_result().await.expect("commit stale counter");

        let mut user = unmarked_parent("evac:fresh-user-stale-counter");
        user.actor = Actor::User;
        assert!(
            journal.upsert(&user).await.is_err(),
            "User admission must validate the physical tail before allocating a sequence"
        );
        let mut dbtx = journal.db.begin_transaction_nc().await;
        assert_eq!(
            dbtx.raw_get_bytes(&high_key)
                .await
                .expect("read protected tail"),
            Some(high_bytes)
        );
        assert!(dbtx
            .raw_get_bytes(&ledger_key_index(&user.idempotency_key))
            .await
            .expect("read absent fresh User index")
            .is_none());
        assert_eq!(
            dbtx.raw_get_bytes(&ledger_counter_key())
                .await
                .expect("read protected stale counter"),
            Some(5_u64.to_be_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn stale_counter_rolls_back_user_retry_intent_indexes_cache_and_ledger() {
        let journal = make_journal();
        let mut parent = unmarked_parent("evac:user-retry-stale-counter");
        parent.actor = Actor::User;
        let cache = pristine_record(&parent);
        seed_parent_and_cache(&journal, &parent, &cache).await;
        journal
            .set_status(
                &parent.idempotency_key,
                parent.attempt,
                IntentStatus::Failed,
                Some("retry"),
            )
            .await
            .expect("terminalize User parent");
        let mut high = journal
            .history(10, None)
            .await
            .expect("read User ledger template")
            .into_iter()
            .next()
            .expect("parent row exists");
        high.seq = 5;
        let high_key = ledger_row_key(5);
        let high_bytes = encode_row(&high).expect("encode physical tail");
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&high_key, &high_bytes)
            .await
            .expect("seed physical tail");
        dbtx.raw_insert_bytes(&ledger_counter_key(), &5_u64.to_be_bytes())
            .await
            .expect("seed stale retry counter");
        dbtx.commit_tx_result()
            .await
            .expect("commit stale retry counter");

        let snapshot_keys = vec![
            intent_key(&parent.idempotency_key),
            pending_index_key(IntentStatus::Failed, &parent.idempotency_key),
            pending_index_key(IntentStatus::Pending, &parent.idempotency_key),
            move_key(&parent.idempotency_key),
            ledger_counter_key(),
            ledger_key_index(&parent.idempotency_key),
            ledger_row_key(0),
            high_key.clone(),
        ];
        let mut dbtx = journal.db.begin_transaction_nc().await;
        let mut before = Vec::new();
        for key in &snapshot_keys {
            before.push(
                dbtx.raw_get_bytes(key)
                    .await
                    .expect("snapshot stale User retry input"),
            );
        }
        drop(dbtx);

        let mut retry = parent.clone();
        retry.attempt += 1;
        retry.status = IntentStatus::Pending;
        assert!(
            journal.retry_failed_intent(&retry).await.is_err(),
            "User retry must validate tail before replacing its failed attempt"
        );
        let mut dbtx = journal.db.begin_transaction_nc().await;
        let mut after = Vec::new();
        for key in &snapshot_keys {
            after.push(
                dbtx.raw_get_bytes(key)
                    .await
                    .expect("snapshot rejected User retry input"),
            );
        }
        assert_eq!(
            after, before,
            "rejected User retry must roll back intent/index/cache/counter/ledger bytes"
        );
    }

    #[tokio::test]
    async fn user_admission_at_ledger_sequence_max_rolls_back_without_overflow() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        let mut tail = journal
            .history(10, None)
            .await
            .expect("read valid row")
            .into_iter()
            .next()
            .expect("seed row exists");
        tail.seq = u64::MAX - 1;
        tail.actor = Actor::User;
        let tail_key = ledger_row_key(u64::MAX - 1);
        let tail_bytes = encode_row(&tail).expect("encode max-minus-one tail");
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&tail_key, &tail_bytes)
            .await
            .expect("seed max-minus-one physical tail");
        dbtx.raw_insert_bytes(&ledger_counter_key(), &u64::MAX.to_be_bytes())
            .await
            .expect("seed tail-consistent exhausted counter");
        dbtx.commit_tx_result()
            .await
            .expect("commit exhausted counter");

        let mut user = unmarked_parent("evac:user-ledger-sequence-max");
        user.actor = Actor::User;
        assert!(
            journal.upsert(&user).await.is_err(),
            "no actor may wrap the ledger counter at u64::MAX"
        );
        let mut dbtx = journal.db.begin_transaction_nc().await;
        assert_eq!(
            dbtx.raw_get_bytes(&tail_key)
                .await
                .expect("read protected exhausted tail"),
            Some(tail_bytes)
        );
        assert!(dbtx
            .raw_get_bytes(&ledger_row_key(u64::MAX))
            .await
            .expect("read impossible successor row")
            .is_none());
        assert!(dbtx
            .raw_get_bytes(&ledger_key_index(&user.idempotency_key))
            .await
            .expect("read absent overflow User index")
            .is_none());
        assert_eq!(
            dbtx.raw_get_bytes(&ledger_counter_key())
                .await
                .expect("read unwrapped exhausted counter"),
            Some(u64::MAX.to_be_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn mismatched_ledger_row_sequence_remains_unreadable_until_repaired() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        journal
            .put_watch_state(&WatchState::default())
            .await
            .expect("seed legacy checkpoint");
        let key = ledger_row_key(0);
        let mut row = journal
            .history(10, None)
            .await
            .expect("read valid row")
            .into_iter()
            .next()
            .expect("seed row exists");
        row.seq = 7;
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&key, &encode_row(&row).expect("encode mismatched row"))
            .await
            .expect("seed valid JSON with mismatched sequence");
        dbtx.commit_tx_result()
            .await
            .expect("commit sequence mismatch");

        let first = journal
            .get_watch_state()
            .await
            .expect("status records a sequence mismatch as repair work");
        assert!(!first.agent_floor_reconciled);
        assert_eq!(first.agent_floor_unreadable_ledger_keys, vec![key.clone()]);
        let retry = journal
            .get_watch_state()
            .await
            .expect("unrepaired valid JSON must remain unreadable");
        assert!(!retry.agent_floor_reconciled);
        assert_eq!(retry.agent_floor_unreadable_ledger_keys, vec![key]);
    }

    #[tokio::test]
    async fn short_valid_agent_key_is_skipped_by_history_budget_and_watch_floor() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        let mut malformed = journal
            .history(10, None)
            .await
            .expect("read canonical Agent template")
            .into_iter()
            .next()
            .expect("canonical Agent row exists");
        malformed.seq = 0;
        malformed.actor = Actor::Agent {
            occurrence: Occurrence(99),
        };
        let short_key = vec![TAG_LEDGER_ROW, 0];
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(
            &short_key,
            &encode_row(&malformed).expect("encode valid Agent-shaped poison"),
        )
        .await
        .expect("seed short ledger key");
        dbtx.commit_tx_result()
            .await
            .expect("commit short ledger key");

        let report = journal
            .scan_ledger_rows_report()
            .await
            .expect("scan tolerates malformed key as poison");
        assert_eq!(report.skipped_rows, 1);
        assert!(
            report
                .rows
                .iter()
                .all(|row| !matches!(row.actor, Actor::Agent { occurrence } if occurrence.0 == 99)),
            "a short key must not become Agent history evidence"
        );
        assert!(journal
            .history(10, None)
            .await
            .expect("public history omits malformed keys")
            .iter()
            .all(|row| !matches!(row.actor, Actor::Agent { occurrence } if occurrence.0 == 99)));
        assert!(
            journal.probe_budget_ledger_rows(NOW, 1).await.is_err(),
            "hard probe-budget reconstruction must fail closed on malformed ledger key poison"
        );

        journal
            .put_watch_state(&WatchState::default())
            .await
            .expect("force canonical watch-floor migration");
        let watch = journal
            .get_watch_state()
            .await
            .expect("tail and canonical counter range ignore noncanonical extra key");
        assert!(watch.agent_floor_reconciled);
        assert_eq!(
            watch.occurrence, 29,
            "watch floor uses only canonical counter-addressable Agent rows"
        );
    }

    #[tokio::test]
    async fn direct_agent_ledger_admission_raises_and_reconciles_watch_floor() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;

        let state = journal
            .get_watch_state()
            .await
            .expect("recover watch floor");
        assert_eq!(state.occurrence, 29);
        let mut dbtx = journal.db.begin_transaction_nc().await;
        let bytes = dbtx
            .raw_get_bytes(&watch_state_key())
            .await
            .expect("inspect watch row")
            .expect("Agent ledger admission creates watch floor");
        let persisted: WatchState =
            decode_row_result("watch state", &watch_state_key(), &bytes).expect("decode watch row");
        assert_eq!(persisted.occurrence, 29);
        assert!(persisted.agent_floor_reconciled);
    }

    #[tokio::test]
    async fn reconciled_watch_reads_fail_closed_on_an_out_of_counter_tail() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        let initialized = journal
            .get_watch_state()
            .await
            .expect("first access completes the one-time legacy migration");
        assert!(initialized.agent_floor_reconciled);
        assert!(initialized.agent_floor_scan_initialized);

        // The O(1) tail check must reject a row the allocation counter does not cover, even after
        // a prior reconciliation. Otherwise a low counter could hide a high Agent occurrence.
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&ledger_row_key(999), b"not valid json")
            .await
            .expect("seed out-of-counter poison ledger tail");
        dbtx.commit_tx_result().await.expect("commit poison row");

        assert!(
            journal.get_watch_state().await.is_err(),
            "a tail beyond the counter is inconsistent append state, not ignorable poison"
        );
    }

    #[tokio::test]
    async fn unreconciled_watch_floor_retries_only_tracked_counter_rows_and_appended_rows() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        journal
            .put_watch_state(&WatchState::default())
            .await
            .expect("seed legacy checkpoint without migration metadata");
        let tracked_key = ledger_row_key(1);
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&tracked_key, b"not valid json")
            .await
            .expect("seed corrupt canonical counter row");
        dbtx.raw_insert_bytes(&ledger_counter_key(), &2_u64.to_be_bytes())
            .await
            .expect("publish the corrupt canonical row");
        dbtx.commit_tx_result().await.expect("commit corrupt row");

        let first = journal
            .get_watch_state()
            .await
            .expect("first migration records exact unreadable key");
        assert_eq!(first.occurrence, 29);
        assert!(first.agent_floor_scan_initialized);
        assert!(!first.agent_floor_reconciled);
        assert_eq!(
            first.agent_floor_unreadable_ledger_keys,
            vec![tracked_key.clone()]
        );
        assert_eq!(
            first.agent_floor_scan_high_water, 2,
            "the bounded initial scan reaches the published counter high-water"
        );

        let mut repaired = journal
            .history(10, None)
            .await
            .expect("read a valid row shape")
            .into_iter()
            .next()
            .expect("Agent refusal exists");
        repaired.seq = 1;
        repaired.actor = Actor::Agent {
            occurrence: Occurrence(47),
        };
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(
            &tracked_key,
            &encode_row(&repaired).expect("encode repaired row"),
        )
        .await
        .expect("restore tracked row from valid data");
        dbtx.commit_tx_result()
            .await
            .expect("commit operator repair");

        let repaired = journal
            .get_watch_state()
            .await
            .expect("bounded retry accepts repaired tracked row");
        assert!(
            repaired.agent_floor_reconciled,
            "only the exact tracked canonical key and direct appended sequences are reconsidered"
        );
        assert!(
            repaired.agent_floor_unreadable_ledger_keys.is_empty(),
            "a valid repair clears the exact durable retry key"
        );
        assert_eq!(
            repaired.occurrence, 47,
            "a repaired Agent row still raises the durable floor"
        );
    }

    #[tokio::test]
    async fn counter_hole_fails_closed_until_the_physical_tail_is_restored() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        journal
            .put_watch_state(&WatchState::default())
            .await
            .expect("seed legacy checkpoint");
        let missing_key = ledger_row_key(1);
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&ledger_counter_key(), &2_u64.to_be_bytes())
            .await
            .expect("create append-counter hole after the Agent row");
        dbtx.commit_tx_result().await.expect("commit counter hole");

        assert!(
            journal.get_watch_state().await.is_err(),
            "a counter hole is inconsistent append state and cannot be migrated past"
        );

        let mut restored = journal
            .history(10, None)
            .await
            .expect("read valid row shape")
            .into_iter()
            .next()
            .expect("Agent refusal exists");
        restored.seq = 1;
        restored.actor = Actor::Agent {
            occurrence: Occurrence(43),
        };
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(
            &missing_key,
            &encode_row(&restored).expect("encode restored Agent row"),
        )
        .await
        .expect("restore the missing row from valid backup bytes");
        dbtx.commit_tx_result()
            .await
            .expect("commit valid hole restoration");

        let repaired = journal
            .get_watch_state()
            .await
            .expect("restored physical tail completes bounded reconciliation");
        assert!(repaired.agent_floor_reconciled);
        assert!(repaired.agent_floor_unreadable_ledger_keys.is_empty());
        assert_eq!(repaired.occurrence, 43);
    }

    #[tokio::test]
    async fn nonzero_counter_without_a_ledger_tail_fails_closed() {
        let journal = make_journal();
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&ledger_counter_key(), &u64::MAX.to_be_bytes())
            .await
            .expect("seed corrupt huge counter");
        dbtx.commit_tx_result()
            .await
            .expect("commit corrupt huge counter");

        assert!(
            journal.get_watch_state().await.is_err(),
            "a nonzero counter with no tail cannot safely name a bounded canonical range"
        );
    }

    #[tokio::test]
    async fn counter_hole_fails_closed_before_watch_floor_migration() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&ledger_counter_key(), &2_u64.to_be_bytes())
            .await
            .expect("append a counter sequence without its row");
        dbtx.commit_tx_result().await.expect("commit appended hole");

        assert!(
            journal.get_watch_state().await.is_err(),
            "the counter must not claim a successor beyond the physical ledger tail"
        );
    }

    #[tokio::test]
    async fn unreconciled_valid_appended_range_over_bound_converges_in_bounded_chunks() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        let checkpoint = WatchState {
            occurrence: 29,
            agent_floor_scan_initialized: true,
            agent_floor_scan_high_water: 1,
            agent_floor_unreadable_ledger_keys: vec![ledger_row_key(0)],
            ..WatchState::default()
        };
        journal
            .put_watch_state(&checkpoint)
            .await
            .expect("seed unresolved bounded-retry checkpoint");
        let mut valid = journal
            .history(10, None)
            .await
            .expect("read valid ledger shape")
            .into_iter()
            .next()
            .expect("Agent refusal exists");
        valid.actor = Actor::User;
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&ledger_row_key(0), b"still corrupt")
            .await
            .expect("seed the one pending repair key");
        for seq in 1..=WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 1 {
            valid.seq = seq;
            dbtx.raw_insert_bytes(
                &ledger_row_key(seq),
                &encode_row(&valid).expect("encode valid appended row"),
            )
            .await
            .expect("append a valid row");
        }
        dbtx.raw_insert_bytes(
            &ledger_counter_key(),
            &(WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 2).to_be_bytes(),
        )
        .await
        .expect("publish more than one chunk of valid appends");
        dbtx.commit_tx_result()
            .await
            .expect("commit valid appended range");

        let first = journal
            .get_watch_state()
            .await
            .expect("first access processes only one bounded chunk");
        assert!(!first.agent_floor_reconciled);
        assert_eq!(
            first.agent_floor_scan_high_water,
            WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 1
        );
        let second = journal
            .get_watch_state()
            .await
            .expect("second access completes the valid append backlog");
        assert_eq!(
            second.agent_floor_scan_high_water,
            WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 2
        );
        assert!(
            !second.agent_floor_reconciled,
            "the one original corrupt key remains unresolved while valid rows converge"
        );

        valid.seq = 0;
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(
            &ledger_row_key(0),
            &encode_row(&valid).expect("encode repaired key"),
        )
        .await
        .expect("restore exact corrupt key");
        dbtx.commit_tx_result().await.expect("commit exact repair");
        assert!(
            journal
                .get_watch_state()
                .await
                .expect("repair clears after backlog converged")
                .agent_floor_reconciled
        );
    }

    #[tokio::test]
    async fn valid_appended_backlog_reports_false_with_zero_unreadable_rows_until_caught_up() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        journal
            .put_watch_state(&WatchState {
                occurrence: 29,
                agent_floor_scan_initialized: true,
                agent_floor_scan_high_water: 1,
                ..WatchState::default()
            })
            .await
            .expect("seed an initialized scan frontier behind valid append-only rows");

        let mut valid = journal
            .history(10, None)
            .await
            .expect("read valid ledger shape")
            .into_iter()
            .next()
            .expect("Agent refusal exists");
        valid.actor = Actor::User;
        let mut dbtx = journal.db.begin_transaction().await;
        for seq in 1..=WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 1 {
            valid.seq = seq;
            dbtx.raw_insert_bytes(
                &ledger_row_key(seq),
                &encode_row(&valid).expect("encode valid appended row"),
            )
            .await
            .expect("append valid row");
        }
        dbtx.raw_insert_bytes(
            &ledger_counter_key(),
            &(WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 2).to_be_bytes(),
        )
        .await
        .expect("publish more than one bounded chunk");
        dbtx.commit_tx_result()
            .await
            .expect("commit valid append backlog");

        let first = journal
            .get_watch_state()
            .await
            .expect("first bounded backlog scan");
        assert!(!first.agent_floor_reconciled);
        assert!(
            first.agent_floor_unreadable_ledger_keys.is_empty(),
            "false reconciliation with no keys is valid scan backlog, not repair work"
        );
        assert_eq!(
            first.agent_floor_scan_high_water,
            WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 1
        );

        let complete = journal
            .get_watch_state()
            .await
            .expect("second bounded scan catches up");
        assert!(complete.agent_floor_reconciled);
        assert!(complete.agent_floor_unreadable_ledger_keys.is_empty());
        assert_eq!(
            complete.agent_floor_scan_high_water,
            WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 2
        );
    }

    #[tokio::test]
    async fn initialized_watch_state_agent_insert_updates_floor_and_high_water_without_migration() {
        let journal = make_journal();
        journal
            .put_watch_state(&WatchState {
                occurrence: 3,
                agent_floor_reconciled: true,
                agent_floor_scan_initialized: true,
                ..WatchState::default()
            })
            .await
            .expect("seed initialized reconciled state");
        seed_agent_refusal(&journal, 29).await;

        let mut dbtx = journal.db.begin_transaction_nc().await;
        let bytes = dbtx
            .raw_get_bytes(&watch_state_key())
            .await
            .expect("read raw watch row")
            .expect("Agent insert keeps watch state durable");
        let state: WatchState =
            decode_row_result("watch state", &watch_state_key(), &bytes).expect("decode raw row");
        assert_eq!(state.occurrence, 29);
        assert_eq!(state.agent_floor_scan_high_water, 1);
        assert!(state.agent_floor_scan_initialized);
        assert!(state.agent_floor_reconciled);
    }

    #[tokio::test]
    async fn agent_insert_is_fenced_while_a_partial_scan_has_prior_sequences() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        let mut valid = journal
            .history(10, None)
            .await
            .expect("read valid ledger shape")
            .into_iter()
            .next()
            .expect("Agent refusal exists");
        valid.seq = 2;
        valid.actor = Actor::User;
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(
            &ledger_row_key(2),
            &encode_row(&valid).expect("encode valid prior row"),
        )
        .await
        .expect("seed later valid row while sequence 1 remains absent");
        dbtx.raw_insert_bytes(&ledger_counter_key(), &3_u64.to_be_bytes())
            .await
            .expect("publish prior counter range");
        dbtx.commit_tx_result()
            .await
            .expect("commit partial backlog");
        journal
            .put_watch_state(&WatchState {
                occurrence: 29,
                agent_floor_scan_initialized: true,
                agent_floor_scan_high_water: 1,
                ..WatchState::default()
            })
            .await
            .expect("seed partial scan frontier");

        let blocked = journal
            .record_refusals(
                &[AllocatorDecision {
                    action: Action::RefuseInflow {
                        fed: fed(1),
                        reason: ReasonCode::SpendingBelowTarget,
                        diagnostics: Default::default(),
                    },
                    reason: ReasonCode::SpendingBelowTarget,
                    occurrence: Occurrence(47),
                    idempotency_key: IdempotencyKey("refuse:watch-floor:47".to_owned()),
                }],
                Occurrence(47),
                NOW,
            )
            .await;
        assert!(
            blocked.is_err(),
            "a direct Agent admission must not skip an unreconciled floor"
        );
        let mut dbtx = journal.db.begin_transaction_nc().await;
        let bytes = dbtx
            .raw_get_bytes(&watch_state_key())
            .await
            .expect("read raw partial checkpoint")
            .expect("blocked Agent insert leaves the prior watch row");
        let after_insert: WatchState =
            decode_row_result("watch state", &watch_state_key(), &bytes).expect("decode watch row");
        assert_eq!(after_insert.occurrence, 29);
        assert_eq!(
            after_insert.agent_floor_scan_high_water, 1,
            "blocked Agent admission must not advance the unknown older counter range"
        );
        assert!(
            dbtx.raw_get_bytes(&ledger_row_key(3))
                .await
                .expect("inspect blocked prospective ledger row")
                .is_none(),
            "the blocked admission must not create a ledger row"
        );
        drop(dbtx);

        let after_scan = journal
            .get_watch_state()
            .await
            .expect("bounded reader inspects the earlier range");
        assert!(!after_scan.agent_floor_reconciled);
        assert_eq!(
            after_scan.agent_floor_unreadable_ledger_keys,
            vec![ledger_row_key(1)],
            "the formerly skipped hole is durably named for repair"
        );
        assert_eq!(after_scan.agent_floor_scan_high_water, 3);
    }

    #[tokio::test]
    async fn retry_failed_agent_intent_updates_watch_high_water_without_public_migration_read() {
        let journal = make_journal();
        let parent = unmarked_parent("evac:retry-watch-high-water");
        journal.upsert(&parent).await.expect("seed Agent parent");
        journal
            .put_watch_state(&WatchState {
                occurrence: 1,
                agent_floor_reconciled: true,
                agent_floor_scan_initialized: true,
                agent_floor_scan_high_water: 1,
                ..WatchState::default()
            })
            .await
            .expect("seed current initialized state");
        journal
            .set_status(
                &parent.idempotency_key,
                parent.attempt,
                IntentStatus::Failed,
                Some("retry"),
            )
            .await
            .expect("terminalize first attempt");
        let mut retry = parent.clone();
        retry.attempt += 1;
        retry.status = IntentStatus::Pending;
        journal
            .retry_failed_intent(&retry)
            .await
            .expect("retry appends a second Agent ledger row");

        let mut dbtx = journal.db.begin_transaction_nc().await;
        let bytes = dbtx
            .raw_get_bytes(&watch_state_key())
            .await
            .expect("read raw watch row")
            .expect("retry preserves watch state");
        let state: WatchState =
            decode_row_result("watch state", &watch_state_key(), &bytes).expect("decode raw row");
        assert_eq!(state.occurrence, 1);
        assert_eq!(
            state.agent_floor_scan_high_water, 2,
            "the retry append bypasses ledger_upsert_in but still advances durable metadata"
        );
        assert!(state.agent_floor_reconciled);
    }

    #[tokio::test]
    async fn retry_failed_agent_intent_rolls_back_all_staged_writes_when_floor_is_unreconciled() {
        let journal = make_journal();
        let parent = unmarked_parent("evac:retry-watch-floor-fence");
        let cache = pristine_record(&parent);
        seed_parent_and_cache(&journal, &parent, &cache).await;
        journal
            .set_status(
                &parent.idempotency_key,
                parent.attempt,
                IntentStatus::Failed,
                Some("retry"),
            )
            .await
            .expect("terminalize parent before manual retry");

        let mut template = journal
            .history(10, None)
            .await
            .expect("read a valid ledger template")
            .into_iter()
            .next()
            .expect("parent ledger row exists");
        template.actor = Actor::User;
        let mut dbtx = journal.db.begin_transaction().await;
        for seq in 1..=WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 {
            template.seq = seq;
            dbtx.raw_insert_bytes(
                &ledger_row_key(seq),
                &encode_row(&template).expect("encode valid backlog row"),
            )
            .await
            .expect("seed valid canonical backlog row");
        }
        dbtx.raw_insert_bytes(
            &ledger_counter_key(),
            &(WATCH_FLOOR_UNREADABLE_KEY_LIMIT as u64 + 1).to_be_bytes(),
        )
        .await
        .expect("publish canonical backlog counter");
        dbtx.commit_tx_result()
            .await
            .expect("commit canonical backlog");
        journal
            .put_watch_state(&WatchState::default())
            .await
            .expect("make retry encounter an incomplete legacy migration");

        let snapshot_keys = vec![
            intent_key(&parent.idempotency_key),
            pending_index_key(IntentStatus::Failed, &parent.idempotency_key),
            pending_index_key(IntentStatus::Pending, &parent.idempotency_key),
            move_key(&parent.idempotency_key),
            ledger_counter_key(),
            ledger_key_index(&parent.idempotency_key),
            ledger_row_key(0),
            watch_state_key(),
        ];
        let mut dbtx = journal.db.begin_transaction_nc().await;
        let mut before = Vec::new();
        for key in &snapshot_keys {
            before.push(
                dbtx.raw_get_bytes(key)
                    .await
                    .expect("snapshot retry transaction input"),
            );
        }
        drop(dbtx);

        let mut retry = parent.clone();
        retry.attempt += 1;
        retry.status = IntentStatus::Pending;
        assert!(
            journal.retry_failed_intent(&retry).await.is_err(),
            "retry append must be fenced while bounded floor migration remains incomplete"
        );

        let mut dbtx = journal.db.begin_transaction_nc().await;
        let mut after = Vec::new();
        for key in &snapshot_keys {
            after.push(
                dbtx.raw_get_bytes(key)
                    .await
                    .expect("snapshot rolled-back retry transaction input"),
            );
        }
        assert_eq!(
            after, before,
            "the refused retry must roll back intent/index/cache/ledger/watch writes byte-for-byte"
        );
    }

    #[tokio::test]
    async fn pre_change_watch_state_json_defaults_new_migration_metadata() {
        let journal = make_journal();
        let old_json = serde_json::json!({
            "version": ROW_VERSION,
            "data": {
                "occurrence": 7,
                "last_discover_ms": 8,
                "discover_cursor": null,
                "discover_backlog": false,
                "discover_rotation": []
            }
        });
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(
            &watch_state_key(),
            &serde_json::to_vec(&old_json).expect("encode pre-change JSON"),
        )
        .await
        .expect("seed old watch JSON shape");
        dbtx.commit_tx_result()
            .await
            .expect("commit old watch JSON");

        let state = journal
            .get_watch_state()
            .await
            .expect("old JSON shape migrates through serde defaults");
        assert_eq!(state.occurrence, 7);
        assert!(state.agent_floor_scan_initialized);
        assert_eq!(state.agent_floor_scan_high_water, 0);
        assert!(state.agent_floor_reconciled);
        assert!(state.agent_floor_unreadable_ledger_keys.is_empty());
    }

    #[tokio::test]
    async fn legacy_watch_floor_scan_skips_corrupt_rows_without_marking_reconciled() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;
        journal
            .put_watch_state(&WatchState::default())
            .await
            .expect("seed legacy unreconciled watch checkpoint");
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&ledger_row_key(1), b"not valid json")
            .await
            .expect("seed corrupt canonical ledger row");
        dbtx.raw_insert_bytes(&ledger_counter_key(), &2_u64.to_be_bytes())
            .await
            .expect("publish corrupt canonical row");
        dbtx.commit_tx_result().await.expect("commit corrupt row");

        let state = journal
            .get_watch_state()
            .await
            .expect("corrupt legacy row must not halt watch access");
        assert_eq!(
            state.occurrence, 29,
            "known Agent rows still raise the floor"
        );
        assert!(
            !state.agent_floor_reconciled,
            "a skipped canonical row must force a retry after repair"
        );
        let mut dbtx = journal.db.begin_transaction_nc().await;
        let bytes = dbtx
            .raw_get_bytes(&watch_state_key())
            .await
            .expect("inspect incomplete migration")
            .expect("incomplete migration persists its known floor");
        let persisted: WatchState =
            decode_row_result("watch state", &watch_state_key(), &bytes).expect("decode watch row");
        assert_eq!(persisted.occurrence, 29);
        assert!(
            !persisted.agent_floor_reconciled,
            "corrupt canonical data must not be recorded as fully reconciled"
        );
        drop(dbtx);

        let mut restored = journal
            .history(10, None)
            .await
            .expect("read valid row shape")
            .into_iter()
            .next()
            .expect("Agent refusal exists");
        restored.seq = 1;
        restored.actor = Actor::Agent {
            occurrence: Occurrence(31),
        };
        let mut dbtx = journal.db.begin_transaction().await;
        dbtx.raw_insert_bytes(
            &ledger_row_key(1),
            &encode_row(&restored).expect("encode valid restored row"),
        )
        .await
        .expect("restore corrupt row from valid backup bytes");
        dbtx.commit_tx_result().await.expect("commit valid restore");

        let restored = journal
            .get_watch_state()
            .await
            .expect("only valid restoration completes migration");
        assert!(restored.agent_floor_reconciled);
        assert_eq!(restored.occurrence, 31);
    }

    #[tokio::test]
    async fn missing_watch_discovery_write_recovers_non_tick_agent_floor() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;

        let state = journal
            .put_watch_discovery_state(Some(fed(2)), true, Some(NOW), vec![fed(3)])
            .await
            .expect("write discovery state above recovered floor");
        assert_eq!(state.occurrence, 29);
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read persisted watch state")
                .occurrence,
            29
        );
    }

    #[tokio::test]
    async fn missing_watch_advance_recovers_non_tick_agent_floor() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;

        let state = journal
            .advance_watch_occurrence()
            .await
            .expect("advance above recovered floor");
        assert_eq!(state.occurrence, 30);
    }

    #[tokio::test]
    async fn missing_watch_observation_recovers_non_tick_agent_floor() {
        let journal = make_journal();
        seed_agent_refusal(&journal, 29).await;

        let state = journal
            .observe_watch_occurrence(7)
            .await
            .expect("preserve recovered floor while observing an older occurrence");
        assert_eq!(state.occurrence, 29);
    }

    async fn seed_stale_watch_checkpoint(journal: &FedimintJournal) {
        // Simulate an on-disk legacy checkpoint written before the direct Agent ledger admission
        // floor was centralized. `put_watch_state` is a test-only raw seed seam, so this leaves
        // the old JSON shape's defaulted reconciliation bit false for the next public access.
        seed_agent_refusal(journal, 29).await;
        journal
            .put_watch_state(&WatchState {
                occurrence: 3,
                last_discover_ms: 17,
                discover_cursor: Some(fed(4)),
                discover_backlog: true,
                discover_rotation: vec![fed(5)],
                ..Default::default()
            })
            .await
            .expect("seed stale persisted watch checkpoint");
    }

    #[tokio::test]
    async fn stale_watch_read_recovers_and_persists_non_tick_agent_floor() {
        let journal = make_journal();
        seed_stale_watch_checkpoint(&journal).await;

        let state = journal
            .get_watch_state()
            .await
            .expect("read recovered watch floor");
        assert_eq!(state.occurrence, 29);
        let mut dbtx = journal.db.begin_transaction_nc().await;
        let bytes = dbtx
            .raw_get_bytes(&watch_state_key())
            .await
            .expect("inspect persisted watch row")
            .expect("watch row remains present");
        let persisted: WatchState =
            decode_row_result("watch state", &watch_state_key(), &bytes).expect("decode watch row");
        assert_eq!(
            persisted.occurrence, 29,
            "legacy recovery persists the reconciled floor"
        );
        assert!(persisted.agent_floor_reconciled);
    }

    #[tokio::test]
    async fn stale_watch_discovery_write_recovers_non_tick_agent_floor() {
        let journal = make_journal();
        seed_stale_watch_checkpoint(&journal).await;

        let state = journal
            .put_watch_discovery_state(Some(fed(2)), false, Some(NOW), vec![fed(3)])
            .await
            .expect("write discovery state above historical floor");
        assert_eq!(state.occurrence, 29);
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read repaired watch checkpoint")
                .occurrence,
            29
        );
    }

    #[tokio::test]
    async fn stale_watch_advance_recovers_non_tick_agent_floor() {
        let journal = make_journal();
        seed_stale_watch_checkpoint(&journal).await;

        let state = journal
            .advance_watch_occurrence()
            .await
            .expect("advance above historical floor");
        assert_eq!(state.occurrence, 30);
    }

    #[tokio::test]
    async fn stale_watch_observation_recovers_non_tick_agent_floor() {
        let journal = make_journal();
        seed_stale_watch_checkpoint(&journal).await;

        let state = journal
            .observe_watch_occurrence(7)
            .await
            .expect("preserve historical floor while observing an older occurrence");
        assert_eq!(state.occurrence, 29);
    }

    #[tokio::test]
    async fn watch_occurrence_exhaustion_refuses_max_without_saturating_or_rewriting_state() {
        let journal = make_journal();
        let initial = WatchState {
            occurrence: u64::MAX - 1,
            last_discover_ms: 17,
            discover_cursor: Some(fed(9)),
            discover_backlog: true,
            discover_rotation: vec![fed(8)],
            agent_floor_reconciled: true,
            agent_floor_scan_initialized: true,
            ..WatchState::default()
        };
        journal
            .put_watch_state(&initial)
            .await
            .expect("seed near-max checkpoint");

        let observe = journal
            .observe_watch_occurrence(u64::MAX)
            .await
            .expect_err("a standalone MAX occurrence cannot poison the watch floor");
        assert!(
            matches!(observe, ExecError::Permanent(ref message) if message.contains("occurrence exhausted")),
            "{observe:?}"
        );
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read unmodified checkpoint"),
            initial,
            "MAX must be rejected before its standalone observation writes"
        );

        journal
            .put_watch_state(&WatchState {
                occurrence: u64::MAX,
                ..initial.clone()
            })
            .await
            .expect("seed corrupt/exhausted checkpoint");
        let advance = journal
            .advance_watch_occurrence()
            .await
            .expect_err("advance must fail closed rather than saturating at MAX");
        assert!(
            matches!(advance, ExecError::Permanent(ref message) if message.contains("watch scheduler occurrence exhausted")),
            "{advance:?}"
        );
        assert_eq!(
            journal
                .get_watch_state()
                .await
                .expect("read exhausted checkpoint")
                .occurrence,
            u64::MAX,
            "a failed advance must not rewrite the exhausted value"
        );
    }

    fn unmarked_parent(key: &str) -> Intent {
        let decision = decision(key, 1, cap(10, 100));
        Intent::from_decision(
            &decision,
            Actor::Agent {
                occurrence: Occurrence(1),
            },
            NOW,
        )
    }

    async fn seed_parent_and_cache(journal: &FedimintJournal, intent: &Intent, cache: &MoveRecord) {
        journal.put_move(cache).await.expect("seed prior cache");
        journal.upsert(intent).await.expect("seed unmarked parent");
    }

    fn pristine_record(intent: &Intent) -> MoveRecord {
        let Action::Evacuate {
            from,
            to,
            amount,
            fee_cap,
            ..
        } = intent.action
        else {
            panic!("test parent must be an evacuation");
        };
        MoveRecord {
            key: intent.idempotency_key.clone(),
            from: Some(from),
            to,
            amount,
            fee_cap,
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: true,
            invoice: None,
            recv_op: None,
            send_op: None,
            phase: MovePhase::Created,
            outcome: None,
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
        }
    }

    #[tokio::test]
    async fn postverify_marker_same_attempt_restores_prior_pristine_cache() {
        let journal = make_journal();
        let parent = unmarked_parent("evac:postverify-marker");
        let prior = pristine_record(&parent);
        seed_parent_and_cache(&journal, &parent, &prior).await;
        let mut written = prior.clone();
        written.receive_fee_quoted = Some(Msat(1));
        let pause = journal.pause_after_next_move_write_for_test(parent.idempotency_key.clone());
        let writer_journal = journal.clone();
        let writer_key = parent.idempotency_key.clone();
        let writer = tokio::spawn(async move {
            writer_journal
                .put_move_if_attempt(&writer_key, parent.attempt, &written)
                .await
        });

        pause.wait_until_committed().await;
        let mut marked = journal
            .get(&parent.idempotency_key)
            .await
            .expect("read writer parent")
            .expect("writer parent exists");
        marked.evacuation_refusal = Some(evidence());
        journal.upsert(&marked).await.expect("durably mark parent");
        pause.release();

        assert_eq!(
            writer.await.expect("writer task"),
            Ok(false),
            "a postverify marker retires this writer"
        );
        assert_eq!(
            journal
                .get_move(&parent.idempotency_key)
                .await
                .expect("read restored cache"),
            Some(prior),
            "the same attempt's planner-owned marker preserves its prior pristine cache"
        );
    }

    #[tokio::test]
    async fn postverify_terminal_same_attempt_restores_prior_artifact_row() {
        let journal = make_journal();
        let parent = unmarked_parent("evac:postverify-terminal");
        let mut prior = pristine_record(&parent);
        prior.phase = MovePhase::Invoiced;
        prior.invoice = Some(Invoice("prior-artifact".into()));
        seed_parent_and_cache(&journal, &parent, &prior).await;
        let written = pristine_record(&parent);
        let pause = journal.pause_after_next_move_write_for_test(parent.idempotency_key.clone());
        let writer_journal = journal.clone();
        let writer_key = parent.idempotency_key.clone();
        let writer = tokio::spawn(async move {
            writer_journal
                .put_move_if_attempt(&writer_key, parent.attempt, &written)
                .await
        });

        pause.wait_until_committed().await;
        journal
            .set_status(
                &parent.idempotency_key,
                parent.attempt,
                IntentStatus::Done,
                None,
            )
            .await
            .expect("terminalize same attempt");
        pause.release();

        assert_eq!(writer.await.expect("writer task"), Ok(false));
        assert_eq!(
            journal
                .get_move(&parent.idempotency_key)
                .await
                .expect("read restored artifact"),
            Some(prior),
            "a terminal same attempt preserves its prior artifact row"
        );
    }

    #[tokio::test]
    async fn postverify_newer_attempt_removes_byte_identical_prior_cache() {
        // A newer attempt can write byte-identical cache data before the old writer postverifies.
        // It is still Other: preserving the N cache would recreate stale state after the retry.
        let journal = make_journal();
        let parent = unmarked_parent("evac:postverify-n-plus-one");
        let prior = pristine_record(&parent);
        seed_parent_and_cache(&journal, &parent, &prior).await;
        let pause = journal.pause_after_next_move_write_for_test(parent.idempotency_key.clone());
        let writer_journal = journal.clone();
        let writer_key = parent.idempotency_key.clone();
        let written = prior.clone();
        let writer = tokio::spawn(async move {
            writer_journal
                .put_move_if_attempt(&writer_key, parent.attempt, &written)
                .await
        });

        pause.wait_until_committed().await;
        journal
            .set_status(
                &parent.idempotency_key,
                parent.attempt,
                IntentStatus::Failed,
                Some("manual retry"),
            )
            .await
            .expect("fail old attempt");
        let mut retry = parent.clone();
        retry.attempt += 1;
        retry.status = IntentStatus::Pending;
        journal
            .retry_failed_intent(&retry)
            .await
            .expect("install newer attempt");
        assert!(journal
            .put_move_if_attempt(&retry.idempotency_key, retry.attempt, &prior)
            .await
            .expect("newer writer succeeds"));
        pause.release();
        assert_eq!(writer.await.expect("old writer task"), Ok(false));
        assert_eq!(
            journal
                .get_move(&parent.idempotency_key)
                .await
                .expect("read N+1 cache"),
            None,
            "Other cleanup removes byte-identical data rather than restoring N's prior cache"
        );
    }

    #[tokio::test]
    async fn postverify_missing_attempt_removes_byte_identical_prior_cache() {
        // Absence is also Other.  The old cache bytes match exactly, so this catches a cleanup
        // exemption based on whether the old write changed bytes.
        let journal = make_journal();
        let parent = unmarked_parent("evac:postverify-missing");
        let prior = pristine_record(&parent);
        seed_parent_and_cache(&journal, &parent, &prior).await;
        let pause = journal.pause_after_next_move_write_for_test(parent.idempotency_key.clone());
        let writer_journal = journal.clone();
        let writer_key = parent.idempotency_key.clone();
        let written = prior.clone();
        let writer = tokio::spawn(async move {
            writer_journal
                .put_move_if_attempt(&writer_key, parent.attempt, &written)
                .await
        });

        pause.wait_until_committed().await;
        let mut tx = journal.db.begin_transaction().await;
        tx.raw_remove_entry(&intent_key(&parent.idempotency_key))
            .await
            .expect("remove current intent");
        tx.raw_remove_entry(&pending_index_key(
            IntentStatus::Pending,
            &parent.idempotency_key,
        ))
        .await
        .expect("remove pending index");
        tx.commit_tx_result().await.expect("commit missing intent");
        pause.release();
        assert_eq!(writer.await.expect("missing writer task"), Ok(false));
        assert_eq!(
            journal
                .get_move(&parent.idempotency_key)
                .await
                .expect("read missing-intent cache"),
            None,
            "a missing intent also never restores byte-identical prior bytes"
        );
    }

    #[tokio::test]
    async fn marked_precheck_rejects_direct_move_write() {
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:marked-direct-write").await;
        assert_eq!(
            journal
                .put_move_if_attempt(
                    &parent.idempotency_key,
                    parent.attempt,
                    &pristine_record(&parent),
                )
                .await,
            Ok(false),
            "a planner-owned structural marker rejects a direct writer before it writes a cache"
        );
        assert_eq!(
            journal
                .get_move(&parent.idempotency_key)
                .await
                .expect("read marker cache"),
            None
        );
    }

    #[tokio::test]
    async fn terminal_same_attempt_rejects_direct_move_write() {
        let journal = make_journal();
        let parent = unmarked_parent("evac:terminal-direct-write");
        journal.upsert(&parent).await.expect("seed parent");
        journal
            .set_status(
                &parent.idempotency_key,
                parent.attempt,
                IntentStatus::Done,
                None,
            )
            .await
            .expect("terminalize same attempt");

        assert_eq!(
            journal
                .put_move_if_attempt(
                    &parent.idempotency_key,
                    parent.attempt,
                    &pristine_record(&parent),
                )
                .await,
            Ok(false),
            "a terminal intent rejects a direct cache write even at the same attempt"
        );
        assert_eq!(
            journal
                .get_move(&parent.idempotency_key)
                .await
                .expect("read terminal cache"),
            None,
            "the rejected direct write leaves no terminal cache artifact"
        );
    }

    #[tokio::test]
    async fn replacement_replay_validates_full_child_relation_and_fences_parent_retry() {
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:parent").await;
        let child = decision("evac:child", 2, cap(10, 200));
        assert!(journal
            .replace_marked_evacuation(
                &parent.idempotency_key,
                parent.attempt,
                &evidence(),
                &child,
                NOW,
                &parent,
            )
            .await
            .expect("exchange"));
        assert!(journal
            .replace_marked_evacuation(
                &parent.idempotency_key,
                parent.attempt,
                &evidence(),
                &child,
                NOW + 1,
                &parent,
            )
            .await
            .expect("time-independent exact replay"));
        Journal::set_status(
            &journal,
            &child.idempotency_key,
            0,
            IntentStatus::Executing,
            None,
        )
        .await
        .expect("claim child");
        assert!(journal
            .replace_marked_evacuation(
                &parent.idempotency_key,
                parent.attempt,
                &evidence(),
                &child,
                NOW + 2,
                &parent,
            )
            .await
            .expect("replay accepts progressed child"));
        assert_eq!(
            journal.history(10, None).await.expect("history").len(),
            2,
            "replays must not append replacement rows"
        );
        let mut changed_action = child.clone();
        let Action::Evacuate { fee_cap, .. } = &mut changed_action.action else {
            unreachable!("fixture is an evacuation");
        };
        *fee_cap = Msat(999);
        assert!(journal
            .replace_marked_evacuation(
                &parent.idempotency_key,
                parent.attempt,
                &evidence(),
                &changed_action,
                NOW + 3,
                &parent,
            )
            .await
            .is_err());
        let mut changed_reason = child.clone();
        changed_reason.reason = ReasonCode::Unhealthy;
        assert!(journal
            .replace_marked_evacuation(
                &parent.idempotency_key,
                parent.attempt,
                &evidence(),
                &changed_reason,
                NOW + 3,
                &parent,
            )
            .await
            .is_err());
        let mut retired = journal
            .get(&parent.idempotency_key)
            .await
            .expect("read")
            .expect("parent");
        retired.status = IntentStatus::Pending;
        retired.attempt += 1;
        assert!(
            journal.retry_failed_intent(&retired).await.is_err(),
            "a canonical superseded parent cannot become a second live evacuation"
        );
        assert_eq!(journal.pending().await.expect("scan").len(), 1);
    }

    #[tokio::test]
    async fn supersession_neighbors_keep_both_links_for_a_replaced_replacement() {
        let journal = make_journal();
        let a = marked_parent(&journal, "evac:chain-a").await;
        let b = decision("evac:chain-b", 2, cap(10, 200));
        journal
            .replace_marked_evacuation(&a.idempotency_key, a.attempt, &evidence(), &b, NOW, &a)
            .await
            .expect("A -> B exchange");

        // B is now a qualifying, unstarted parent in a later planning epoch.
        let mut b_parent = journal
            .get(&b.idempotency_key)
            .await
            .expect("read B")
            .expect("B exists");
        let mut b_evidence = evidence();
        b_evidence.cap_components = cap(10, 200);
        b_evidence.low.fee_cap = b_evidence.cap_components.at(b_evidence.low.delivered_net);
        b_evidence.high.fee_cap = b_evidence.cap_components.at(b_evidence.high.delivered_net);
        b_evidence.high.total_fee = Msat(40);
        b_parent.evacuation_refusal = Some(b_evidence.clone());
        journal.upsert(&b_parent).await.expect("mark B");
        let c = decision("evac:chain-c", 3, cap(10, 300));

        // A pre-commit exchange fault must not let B's reverse predecessor (A -> B) masquerade as
        // evidence that the attempted B -> C exchange committed.  This is the exact shape the
        // actor and standalone confirmation paths must classify as uncommitted before they clear
        // B's marker; a dual-key reader would return A -> B here.
        journal.fail_before_next_evacuation_replacement_for_test();
        assert!(matches!(
            journal
                .replace_marked_evacuation(
                    &b.idempotency_key,
                    b_parent.attempt,
                    &b_evidence,
                    &c,
                    NOW + 1,
                    &b_parent,
                )
                .await,
            Err(ExecError::Retryable(_))
        ));
        assert!(
            journal
                .evacuation_canonical_successor(&b.idempotency_key)
                .await
                .expect("read absent B canonical successor")
                .is_none(),
            "an uncommitted B -> C has no canonical successor even though B has a predecessor"
        );
        let predecessor = journal
            .evacuation_supersession(&b.idempotency_key)
            .await
            .expect("read B's predecessor through the dual-key API")
            .expect("A -> B predecessor");
        assert_eq!(predecessor.old_key, a.idempotency_key);
        assert_eq!(predecessor.new_key, b.idempotency_key);
        assert_eq!(predecessor.old_attempt, a.attempt);
        assert_eq!(predecessor.new_attempt, 0);
        assert_eq!(predecessor.occurrence, b.occurrence);
        assert_eq!(predecessor.refusal, evidence());
        assert_eq!(predecessor.superseded_at_ms, NOW);
        assert_eq!(
            journal
                .get(&b.idempotency_key)
                .await
                .expect("read B after fault"),
            Some(b_parent.clone()),
            "the fault neither retires nor clears the attempted parent"
        );
        assert!(
            journal
                .get(&c.idempotency_key)
                .await
                .expect("read absent C after fault")
                .is_none(),
            "the fault does not create the attempted child"
        );
        journal
            .replace_marked_evacuation(
                &b.idempotency_key,
                b_parent.attempt,
                &b_evidence,
                &c,
                NOW + 1,
                &b_parent,
            )
            .await
            .expect("B -> C exchange");
        assert_eq!(
            journal
                .evacuation_canonical_successor(&b.idempotency_key)
                .await
                .expect("read committed B canonical successor")
                .expect("B -> C successor")
                .new_key,
            c.idempotency_key,
            "a committed B -> C remains confirmable by the strict reader"
        );

        let a_links = journal
            .evacuation_supersession_neighbors(&a.idempotency_key)
            .await
            .expect("read A links");
        assert!(a_links.predecessor.is_none());
        assert_eq!(
            a_links.successor.expect("A successor").new_key,
            b.idempotency_key
        );
        let b_links = journal
            .evacuation_supersession_neighbors(&b.idempotency_key)
            .await
            .expect("read B links");
        assert_eq!(
            b_links.predecessor.expect("B predecessor").old_key,
            a.idempotency_key
        );
        assert_eq!(
            b_links.successor.expect("B successor").new_key,
            c.idempotency_key
        );
        let c_links = journal
            .evacuation_supersession_neighbors(&c.idempotency_key)
            .await
            .expect("read C links");
        assert_eq!(
            c_links.predecessor.expect("C predecessor").old_key,
            b.idempotency_key
        );
        assert!(c_links.successor.is_none());

        // Exact exchange confirmation for B -> C only needs B's canonical successor relation.
        // Damage to the older A -> B relation must not turn an already committed B -> C exchange
        // into an ambiguity that an actor/standalone caller cannot safely resolve.
        let mut tx = journal.db.begin_transaction().await;
        tx.raw_remove_entry(&evacuation_supersession_key(&a.idempotency_key))
            .await
            .expect("damage older canonical relation");
        tx.commit_tx_result()
            .await
            .expect("commit older relation damage");
        assert_eq!(
            journal
                .evacuation_supersession(&b.idempotency_key)
                .await
                .expect("B successor remains independently confirmable")
                .expect("B -> C relation")
                .new_key,
            c.idempotency_key
        );
    }

    #[tokio::test]
    async fn replacement_replay_accepts_every_coherent_child_lifecycle_status() {
        for status in [
            IntentStatus::Pending,
            IntentStatus::Executing,
            IntentStatus::Awaiting,
            IntentStatus::Done,
            IntentStatus::Failed,
        ] {
            let journal = make_journal();
            let parent = marked_parent(&journal, &format!("evac:parent-status-{status:?}")).await;
            let child = decision(&format!("evac:child-status-{status:?}"), 2, cap(10, 200));
            assert!(journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &evidence(),
                    &child,
                    NOW,
                    &parent,
                )
                .await
                .expect("exchange"));
            if status != IntentStatus::Pending {
                Journal::set_status(&journal, &child.idempotency_key, 0, status, None)
                    .await
                    .expect("advance child");
            }
            assert!(journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &evidence(),
                    &child,
                    NOW + 1,
                    &parent,
                )
                .await
                .expect("replay after coherent child progress"));
        }
    }

    #[tokio::test]
    async fn replacement_rejects_stale_child_namespace_third_holder_and_reverse_damage() {
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:parent-a").await;
        let child = decision("evac:child-a", 2, cap(10, 200));
        let stale = MoveRecord {
            key: child.idempotency_key.clone(),
            from: Some(fed(1)),
            to: fed(2),
            amount: Msat(100),
            fee_cap: Msat(12),
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: true,
            invoice: None,
            recv_op: None,
            send_op: None,
            phase: MovePhase::Created,
            outcome: None,
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
        };
        let mut tx = journal.db.begin_transaction().await;
        tx.raw_insert_bytes(
            &move_key(&child.idempotency_key),
            &encode_row(&stale).expect("row"),
        )
        .await
        .expect("stale move");
        tx.commit_tx_result().await.expect("commit stale move");
        assert_eq!(
            journal
                .replacement_child_namespace(&child.idempotency_key)
                .await
                .expect("inspect stale child namespace"),
            ReplacementChildNamespace::Contaminated,
            "a stale MoveRecord is not an uncommitted replacement child"
        );
        assert!(
            journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &evidence(),
                    &child,
                    NOW,
                    &parent,
                )
                .await
                .is_err(),
            "even an unstarted MoveRecord makes the fresh child namespace non-empty"
        );

        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:parent-b").await;
        let blocker = Intent::from_decision(
            &decision("evac:blocker", 9, cap(10, 200)),
            Actor::Agent {
                occurrence: Occurrence(9),
            },
            NOW,
        );
        journal.upsert(&blocker).await.expect("third holder");
        assert!(
            journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &evidence(),
                    &decision("evac:child-b", 2, cap(10, 200)),
                    NOW,
                    &parent,
                )
                .await
                .is_err(),
            "the transaction scans for another live Agent evacuation of the source"
        );

        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:parent-c").await;
        let child = decision("evac:child-c", 2, cap(10, 200));
        journal
            .replace_marked_evacuation(
                &parent.idempotency_key,
                parent.attempt,
                &evidence(),
                &child,
                NOW,
                &parent,
            )
            .await
            .expect("exchange");
        let mut tx = journal.db.begin_transaction().await;
        tx.raw_remove_entry(&evacuation_supersession_reverse_key(&child.idempotency_key))
            .await
            .expect("remove reverse");
        tx.commit_tx_result().await.expect("commit reverse damage");
        assert!(
            journal
                .evacuation_supersession(&parent.idempotency_key)
                .await
                .is_err(),
            "canonical lookup validates its reverse half in the same snapshot"
        );

        for (label, status) in [
            ("intent", None),
            ("move", None),
            ("ledger-index", None),
            ("canonical", None),
            ("reverse", None),
            ("pending-index", Some(IntentStatus::Pending)),
            ("executing-index", Some(IntentStatus::Executing)),
            ("awaiting-index", Some(IntentStatus::Awaiting)),
            ("done-index", Some(IntentStatus::Done)),
            ("failed-index", Some(IntentStatus::Failed)),
        ] {
            let journal = make_journal();
            let parent = marked_parent(&journal, &format!("evac:parent-stale-{label}")).await;
            let child = decision(&format!("evac:child-stale-{label}"), 2, cap(10, 200));
            let raw_key = match label {
                "intent" => intent_key(&child.idempotency_key),
                "move" => move_key(&child.idempotency_key),
                "ledger-index" => ledger_key_index(&child.idempotency_key),
                "canonical" => evacuation_supersession_key(&child.idempotency_key),
                "reverse" => evacuation_supersession_reverse_key(&child.idempotency_key),
                _ => pending_index_key(status.expect("status row"), &child.idempotency_key),
            };
            let mut tx = journal.db.begin_transaction().await;
            tx.raw_insert_bytes(&raw_key, &[1])
                .await
                .expect("seed stale direct child namespace row");
            tx.commit_tx_result().await.expect("commit stale row");
            assert!(
                journal
                    .replace_marked_evacuation(
                        &parent.idempotency_key,
                        parent.attempt,
                        &evidence(),
                        &child,
                        NOW,
                        &parent,
                    )
                    .await
                    .is_err(),
                "{label} stale child row must fence exchange"
            );
        }

        for (label, mutate_canonical) in [("canonical", true), ("reverse", false)] {
            let journal = make_journal();
            let parent = marked_parent(&journal, &format!("evac:parent-corrupt-{label}")).await;
            let child = decision(&format!("evac:child-corrupt-{label}"), 2, cap(10, 200));
            journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &evidence(),
                    &child,
                    NOW,
                    &parent,
                )
                .await
                .expect("exchange");
            let key = if mutate_canonical {
                evacuation_supersession_key(&parent.idempotency_key)
            } else {
                evacuation_supersession_reverse_key(&child.idempotency_key)
            };
            let mut tx = journal.db.begin_transaction().await;
            tx.raw_insert_bytes(&key, &[1])
                .await
                .expect("corrupt sidecar half");
            tx.commit_tx_result().await.expect("commit corruption");
            assert!(journal
                .evacuation_supersession(&parent.idempotency_key)
                .await
                .is_err());
            assert!(
                journal
                    .evacuation_supersession(&child.idempotency_key)
                    .await
                    .is_err(),
                "{label} corruption must fail sidecar lookup from either endpoint"
            );
            let display = journal
                .evacuation_supersession_neighbors_for_display_keys(&[
                    parent.idempotency_key.clone(),
                    child.idempotency_key.clone(),
                ])
                .await
                .expect("corrupt sidecars do not poison the bounded display projection");
            assert_eq!(
                display.get(&parent.idempotency_key),
                Some(&EvacuationSupersessionNeighbors::default()),
                "{label} parent link degrades to absent for display"
            );
            assert_eq!(
                display.get(&child.idempotency_key),
                Some(&EvacuationSupersessionNeighbors::default()),
                "{label} child link degrades to absent for display"
            );
        }
    }

    /// `show <key>` is the documented first step of a structural-refusal incident: it resolves the
    /// ledger row and only THEN augments it with the linked intent's live status and marker. So a
    /// corrupt intent row must degrade to absent rather than blank the row the operator asked for,
    /// while a retryable storage fault — which says nothing about whether a marker exists — must
    /// still fail loudly instead of displaying a false "no marker".
    #[tokio::test]
    async fn malformed_linked_intent_degrades_for_display_while_storage_faults_still_fail() {
        let journal = make_journal();
        let marked = marked_parent(&journal, "evac:display-marker").await;
        assert_eq!(
            journal
                .intent_for_display(&marked.idempotency_key)
                .await
                .expect("a readable marker is read normally"),
            Some(marked.clone()),
            "the display projection returns the exact live marker while the row is readable"
        );

        let mut tx = journal.db.begin_transaction().await;
        tx.raw_insert_bytes(&intent_key(&marked.idempotency_key), &[1])
            .await
            .expect("corrupt the linked intent row");
        tx.commit_tx_result().await.expect("commit corruption");
        let strict = journal
            .get(&marked.idempotency_key)
            .await
            .expect_err("the money-path read still refuses a corrupt intent row");
        assert!(
            matches!(strict, ExecError::Permanent(_)),
            "corruption is permanent, not retryable: {strict:?}"
        );
        assert_eq!(
            journal
                .intent_for_display(&marked.idempotency_key)
                .await
                .expect("a corrupt intent row must not blank the operation row show resolved"),
            None,
            "a malformed intent row degrades to absent for display"
        );

        journal.fail_next_intent_reads_for_test(1);
        let faulted = journal
            .intent_for_display(&IdempotencyKey("evac:display-fault".to_owned()))
            .await
            .expect_err("a storage fault is not permission to display absence");
        assert!(
            matches!(faulted, ExecError::Retryable(_)),
            "the retryable class still propagates: {faulted:?}"
        );
    }

    #[tokio::test]
    async fn marker_clear_requires_the_exact_pending_planner_owned_evacuation() {
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:clear-marker").await;
        assert!(!journal
            .clear_marked_evacuation_if_pending(&Intent {
                attempt: parent.attempt + 1,
                ..parent.clone()
            },)
            .await
            .expect("wrong attempt is not clearable"));
        assert!(journal
            .get(&parent.idempotency_key)
            .await
            .expect("read retained marker")
            .expect("parent exists")
            .evacuation_refusal
            .is_some());
        let mut stale_parent = parent.clone();
        stale_parent.reason = wallet_core::ReasonCode::Unhealthy;
        assert!(!journal
            .clear_marked_evacuation_if_pending(&stale_parent)
            .await
            .expect("changed full parent is not clearable"));
        assert!(journal
            .clear_marked_evacuation_if_pending(&parent,)
            .await
            .expect("exact marker clear"));
        let cleared = journal
            .get(&parent.idempotency_key)
            .await
            .expect("read cleared marker")
            .expect("parent remains pending");
        assert_eq!(cleared.status, IntentStatus::Pending);
        assert_eq!(cleared.evacuation_refusal, None);
        assert!(!journal
            .clear_marked_evacuation_if_pending(&parent,)
            .await
            .expect("cleared marker cannot be cleared twice"));

        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:clear-marker-with-holder").await;
        let _other = marked_parent(&journal, "evac:other-live-holder").await;
        assert!(
            journal
                .clear_marked_evacuation_if_pending(&parent)
                .await
                .is_err(),
            "a second live agent evacuation from the same source is corruption/ambiguity, never a clear"
        );
    }

    #[tokio::test]
    async fn replacement_requires_the_full_planned_parent_before_the_exchange() {
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:full-parent-cas").await;
        let mut changed = parent.clone();
        changed.reason = ReasonCode::Unhealthy;
        journal
            .upsert(&changed)
            .await
            .expect("persist a changed but still marker-bearing parent");
        let child = decision("evac:full-parent-cas-child", 2, cap(10, 200));

        assert!(
            !journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &evidence(),
                    &child,
                    NOW,
                    &parent,
                )
                .await
                .expect("a stale full-parent CAS is a benign refusal"),
            "matching key/attempt/evidence alone must not replace a changed parent"
        );
        assert_eq!(
            journal
                .get(&parent.idempotency_key)
                .await
                .expect("read unchanged parent"),
            Some(changed.clone())
        );
        assert!(journal
            .get(&child.idempotency_key)
            .await
            .expect("read absent child")
            .is_none());

        assert!(journal
            .replace_marked_evacuation(
                &changed.idempotency_key,
                changed.attempt,
                changed
                    .evacuation_refusal
                    .as_ref()
                    .expect("changed parent retains marker"),
                &child,
                NOW,
                &changed,
            )
            .await
            .expect("the current full parent is exchangeable"));
    }

    #[tokio::test]
    async fn replacement_and_attempt_fenced_move_writer_have_exactly_one_winner() {
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:parent-race").await;
        let child = decision("evac:child-race", 2, cap(10, 200));
        let non_pristine = MoveRecord {
            key: parent.idempotency_key.clone(),
            from: Some(fed(1)),
            to: fed(2),
            amount: Msat(100),
            fee_cap: Msat(11),
            gateway: GatewayUrl("https://gw.example".into()),
            send_required: true,
            invoice: None,
            recv_op: None,
            send_op: None,
            phase: MovePhase::Invoiced,
            outcome: None,
            preimage: None,
            receive_fee_quoted: None,
            send_fee_quoted: None,
        };
        let replacement_journal = journal.clone();
        let writer_journal = journal.clone();
        let parent_key = parent.idempotency_key.clone();
        let writer_key = parent.idempotency_key.clone();
        let replacement_evidence = evidence();
        let (replacement, writer) = tokio::join!(
            replacement_journal.replace_marked_evacuation(
                &parent_key,
                parent.attempt,
                &replacement_evidence,
                &child,
                NOW,
                &parent,
            ),
            writer_journal.put_move_if_attempt(&writer_key, parent.attempt, &non_pristine),
        );
        let replacement_won = replacement.as_ref().is_ok_and(|accepted| *accepted);
        assert!(
            replacement_won,
            "the planner-owned marker replacement wins: {replacement:?}"
        );
        assert_eq!(
            writer,
            Ok(false),
            "marked parents reject late cache writers"
        );
        let parent_after = journal
            .get(&parent.idempotency_key)
            .await
            .expect("read raced parent")
            .expect("parent remains auditable");
        let child_after = journal
            .get(&child.idempotency_key)
            .await
            .expect("read raced child");
        if replacement_won {
            assert_eq!(parent_after.status, IntentStatus::Failed);
            assert_eq!(
                child_after.map(|intent| intent.status),
                Some(IntentStatus::Pending)
            );
            assert_eq!(
                journal
                    .get_move(&parent.idempotency_key)
                    .await
                    .expect("read raced artifact"),
                None,
                "the marker fence prevents a late writer from creating parent artifacts"
            );
            assert!(journal
                .evacuation_supersession(&parent.idempotency_key)
                .await
                .expect("read replacement link")
                .is_some());
        } else {
            assert_eq!(parent_after.status, IntentStatus::Pending);
            assert!(child_after.is_none());
            assert!(journal
                .get_move(&parent.idempotency_key)
                .await
                .expect("read winning artifact")
                .is_some());
            assert!(journal
                .evacuation_supersession(&parent.idempotency_key)
                .await
                .expect("read absent replacement link")
                .is_none());
        }
    }

    /// The two durable orderings at the actual CAS boundary are both safe.
    /// This is intentionally expressed against `FedimintJournal`, rather than
    /// a mock Journal: `set_status_if` clears the marker and moves the index
    /// in the same transaction that decides the Pending -> Executing winner.
    #[tokio::test]
    async fn replacement_and_pending_claim_have_one_durable_executable_winner() {
        for claim_first in [true, false] {
            let journal = make_journal();
            let parent = marked_parent(
                &journal,
                if claim_first {
                    "evac:claim-first-parent"
                } else {
                    "evac:replacement-first-parent"
                },
            )
            .await;
            let child = decision(
                if claim_first {
                    "evac:claim-first-child"
                } else {
                    "evac:replacement-first-child"
                },
                2,
                cap(10, 200),
            );

            if claim_first {
                assert!(
                    Journal::set_status_if(
                        &journal,
                        &parent.idempotency_key,
                        parent.attempt,
                        IntentStatus::Pending,
                        IntentStatus::Executing,
                    )
                    .await
                    .expect("claim"),
                    "the claim wins its CAS"
                );
                assert!(
                    !journal
                        .replace_marked_evacuation(
                            &parent.idempotency_key,
                            parent.attempt,
                            &evidence(),
                            &child,
                            NOW,
                            &parent,
                        )
                        .await
                        .expect("replacement loses claimed parent"),
                    "the replacement observes the CAS winner"
                );
                let claimed = journal
                    .get(&parent.idempotency_key)
                    .await
                    .expect("read claim winner")
                    .expect("parent remains");
                assert_eq!(claimed.status, IntentStatus::Executing);
                assert_eq!(
                    claimed.evacuation_refusal, None,
                    "claim atomically consumes the marker"
                );
                assert_eq!(
                    journal.pending().await.expect("live index"),
                    vec![claimed.clone()],
                    "the original is the only executable index row"
                );
                assert!(journal
                    .get(&child.idempotency_key)
                    .await
                    .expect("child lookup")
                    .is_none());
                assert!(journal
                    .get_move(&parent.idempotency_key)
                    .await
                    .expect("parent move lookup")
                    .is_none());
                assert_eq!(
                    journal
                        .evacuation_supersession_neighbors(&parent.idempotency_key)
                        .await
                        .expect("no sidecar"),
                    EvacuationSupersessionNeighbors::default()
                );
            } else {
                assert!(
                    journal
                        .replace_marked_evacuation(
                            &parent.idempotency_key,
                            parent.attempt,
                            &evidence(),
                            &child,
                            NOW,
                            &parent,
                        )
                        .await
                        .expect("replacement"),
                    "the atomic exchange wins before the claim"
                );
                assert!(
                    !Journal::set_status_if(
                        &journal,
                        &parent.idempotency_key,
                        parent.attempt,
                        IntentStatus::Pending,
                        IntentStatus::Executing,
                    )
                    .await
                    .expect("stale claim"),
                    "a claim cannot resurrect the retired parent"
                );
                let retired = journal
                    .get(&parent.idempotency_key)
                    .await
                    .expect("read retired parent")
                    .expect("parent remains auditable");
                let executable = journal
                    .get(&child.idempotency_key)
                    .await
                    .expect("read child")
                    .expect("child exists");
                assert_eq!(retired.status, IntentStatus::Failed);
                assert_eq!(executable.status, IntentStatus::Pending);
                assert_eq!(
                    journal.pending().await.expect("live index"),
                    vec![executable.clone()],
                    "only the child is executable"
                );
                assert!(journal
                    .get_move(&parent.idempotency_key)
                    .await
                    .expect("parent move lookup")
                    .is_none());
                assert_eq!(
                    journal
                        .evacuation_supersession_neighbors(&parent.idempotency_key)
                        .await
                        .expect("parent link")
                        .successor
                        .expect("forward link")
                        .new_key,
                    child.idempotency_key
                );
                assert!(
                    journal
                        .replace_marked_evacuation(
                            &parent.idempotency_key,
                            parent.attempt,
                            &evidence(),
                            &child,
                            NOW,
                            &parent,
                        )
                        .await
                        .expect("exact replay"),
                    "replay preserves the one linked child"
                );
            }
        }
    }

    #[tokio::test]
    async fn replacement_rejects_incoherent_evidence_and_child_cap() {
        for evidence in [
            {
                let mut e = evidence();
                e.requested_net = Msat(101);
                e
            },
            {
                let mut e = evidence();
                e.low.fee_cap = Msat(99);
                e
            },
        ] {
            let journal = make_journal();
            let parent = marked_parent(&journal, "evac:evidence").await;
            assert!(journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &evidence,
                    &decision("evac:evidence-child", 2, cap(10, 200)),
                    NOW,
                    &parent,
                )
                .await
                .is_err());
        }
        let journal = make_journal();
        let mut parent = marked_parent(&journal, "evac:rollback-evidence").await;
        let mut rollback_evidence = evidence();
        rollback_evidence.measured_at_ms = NOW.saturating_sub(1);
        parent.evacuation_refusal = Some(rollback_evidence.clone());
        journal
            .upsert(&parent)
            .await
            .expect("seed rollback-clock evidence");
        assert!(
            journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &rollback_evidence,
                    &decision("evac:rollback-evidence-child", 2, cap(10, 200)),
                    NOW,
                    &parent,
                )
                .await
                .expect("display-clock rollback is valid evidence"),
            "evidence timestamp is display-only and does not order the parent"
        );
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:bad-child-cap").await;
        let mut child = decision("evac:bad-child-cap-child", 2, cap(10, 200));
        let Action::Evacuate { fee_cap, .. } = &mut child.action else {
            unreachable!("fixture is an evacuation");
        };
        *fee_cap = Msat(1);
        assert!(
            journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &evidence(),
                    &child,
                    NOW,
                    &parent,
                )
                .await
                .is_err(),
            "child cap must equal its components at the child amount"
        );
    }

    #[tokio::test]
    async fn replacement_evidence_requires_affordable_structural_samples_not_requested_balance() {
        let mut accepted = evidence();
        accepted.requested_net = Msat(100);
        accepted.source_spendable = Msat(95);
        accepted.high.delivered_net = Msat(80);
        accepted.high.total_fee = Msat(12);
        accepted.high.fee_cap = cap(10, 100).at(Msat(80));
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:affordable").await;
        let mut parent_row = journal
            .get(&parent.idempotency_key)
            .await
            .expect("read parent")
            .expect("parent");
        parent_row.evacuation_refusal = Some(accepted.clone());
        journal.upsert(&parent_row).await.expect("replace marker");
        assert!(journal
            .replace_marked_evacuation(
                &parent.idempotency_key,
                parent.attempt,
                &accepted,
                &decision("evac:affordable-child", 2, cap(10, 200)),
                NOW,
                &parent_row,
            )
            .await
            .expect("requested net may exceed spendable when both samples are affordable"));

        let mut unaffordable = accepted.clone();
        unaffordable.high.total_fee = Msat(20); // 80 + 20 exceeds the 95-msat spendable balance.
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:unaffordable").await;
        let mut parent_row = journal
            .get(&parent.idempotency_key)
            .await
            .expect("read parent")
            .expect("parent");
        parent_row.evacuation_refusal = Some(unaffordable.clone());
        journal.upsert(&parent_row).await.expect("replace marker");
        assert!(journal
            .replace_marked_evacuation(
                &parent.idempotency_key,
                parent.attempt,
                &unaffordable,
                &decision("evac:unaffordable-child", 2, cap(10, 200)),
                NOW,
                &parent_row,
            )
            .await
            .is_err());

        let mut non_structural = evidence();
        non_structural.low.total_fee = Msat(11);
        non_structural.high.total_fee = Msat(20);
        let journal = make_journal();
        let parent = marked_parent(&journal, "evac:non-structural").await;
        let mut parent_row = journal
            .get(&parent.idempotency_key)
            .await
            .expect("read parent")
            .expect("parent");
        parent_row.evacuation_refusal = Some(non_structural.clone());
        journal.upsert(&parent_row).await.expect("replace marker");
        assert!(
            journal
                .replace_marked_evacuation(
                    &parent.idempotency_key,
                    parent.attempt,
                    &non_structural,
                    &decision("evac:non-structural-child", 2, cap(10, 200)),
                    NOW,
                    &parent_row,
                )
                .await
                .is_err(),
            "two over-cap samples without either shared structural predicate are insufficient"
        );
    }
}
