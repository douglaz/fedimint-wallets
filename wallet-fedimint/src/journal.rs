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
//! - `0x0a` `WatchStateKey`                 → JSON row v1([`WatchState`])
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
use std::sync::Arc;
use wallet_core::{
    advance, kind_from_action, status_from_intent, Action, Actor, AllocatorDecision,
    DiscoverySource, ExecError, FederationId, FeeBreakdown, GatewayUrl, IdempotencyKey, Intent,
    IntentStatus, Journal, Msat, Occurrence, OperationId, OperationKind, OperationRecord,
    OperationStatus, ProbeAttempt, ProbePolicy, RawOpUpdate, ReasonCode, WriteKind,
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

/// Single-row watch scheduler checkpoint (phase 5 §5.2.5). The self-running loop is
/// greenfield, so this is the v1 shape on disk: no compatibility shims or migration
/// branches.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WatchState {
    pub occurrence: u64,
    pub last_discover_ms: u64,
    pub discover_cursor: Option<FederationId>,
    pub discover_backlog: bool,
    /// Candidate order snapshot for a deadline/cap-truncated discovery rotation. A cursor alone
    /// cannot distinguish older deferred source-only ids from fresh ids announced on restart.
    pub discover_rotation: Vec<FederationId>,
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
    /// [`Journal::store_id`]: identity of `db`'s underlying storage, captured in [`Self::new`]
    /// from the pre-`with_prefix` handle (see there for why `with_prefix` itself can't supply
    /// it).
    store_id: usize,
    /// The injected ledger clock (spec §9.4): unix millis for `updated_at_ms` on the
    /// journal-integrated ledger writes and for repair's age heuristics. `seq` is the ordering
    /// authority — the clock is display material plus the one repair dependency (§10.3), so it
    /// is injectable (production [`SystemTime::now`]; tests pin it via [`Self::with_clock`]).
    clock: fn() -> u64,
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
            store_id,
            clock,
        }
    }

    /// The ledger's wall-clock in unix millis (the injected [`Self::clock`]).
    fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    // --- inherent read helpers (shared by the trait methods) ---

    /// Load and decode the [`Intent`] stored under `key`, or `None` if absent.
    async fn read_intent(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
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

    /// Upsert the derived [`MoveRecord`] cache for its key (spec §5; rebuilt from op-log).
    pub async fn put_move(&self, rec: &MoveRecord) -> Result<(), ExecError> {
        let value = encode_row(rec)?;
        let mut dbtx = self.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&move_key(&rec.key), &value)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
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

    /// Persist a terminal raw-op observation before the intent transition makes its ledger row
    /// immutable. This reuses the same authoritative enrichment path as reconcile repair.
    pub async fn record_raw_observation(
        &self,
        key: &IdempotencyKey,
        op: OperationId,
        observation: &RawOpObservation,
    ) -> Result<(), ExecError> {
        self.apply_observation(
            key,
            op,
            observation,
            self.now_ms(),
            WriteKind::Authoritative,
            None,
        )
        .await
    }

    /// Record whether a join created membership or merely reopened an existing federation.
    /// The ledger transition precedes the intent's terminal status so a crash cannot erase the
    /// `newly_joined` distinction.
    pub async fn record_join_outcome(
        &self,
        key: &IdempotencyKey,
        newly_joined: bool,
    ) -> Result<(), ExecError> {
        self.record_terminal(
            key,
            OperationStatus::Succeeded,
            self.now_ms(),
            (!newly_joined).then_some(JOIN_NOOP_REOPEN_NOTE),
            None,
        )
        .await
    }

    /// Complete an externally-awaited raw pay/receive through the same durable journal used by
    /// the issue driver. `Ok(notes)` preserves the CLI's best-effort audit diagnostics; an
    /// intent status-write failure remains fatal so a caller cannot report completion while the
    /// reservation stays live.
    #[allow(clippy::too_many_arguments)]
    pub async fn finalize_raw_operation(
        &self,
        oracle: &dyn LedgerRepairOracle,
        fed: FederationId,
        op: OperationId,
        key: &IdempotencyKey,
        role: RawOperationRole,
        status: OperationStatus,
        error: Option<&str>,
    ) -> Result<Vec<String>, ExecError> {
        let Some(row) = self.operation(&OperationRef::Key(key.clone())).await? else {
            return Ok(vec![format!(
                "no ledger row for --key {}; not recording",
                key.0
            )]);
        };
        let needs_correlation_proof = match raw_operation_row_matches(&row, role, fed, op) {
            Ok(needs_proof) => needs_proof,
            Err(reason) => {
                return Ok(vec![format!(
                    "--key {} does not match this operation ({reason}); not recording",
                    key.0
                )]);
            }
        };
        if needs_correlation_proof {
            match oracle.find_op_by_correlation_key(fed, key).await {
                Ok(Some(found)) if found == op => {}
                Ok(_) => {
                    return Ok(vec![format!(
                        "--key {} has no recorded op id and the op-log does not tie this \
                         operation to it; not recording (reconcile repairs it)",
                        key.0
                    )]);
                }
                Err(error) => {
                    return Ok(vec![format!(
                        "could not verify --key {} against the op-log: {error:?}; not recording",
                        key.0
                    )]);
                }
            }
        }

        let mut notes = Vec::new();
        let update = match oracle.observe_op(fed, op).await {
            Ok(observation) => RawOpUpdate {
                op_id: Some(op),
                gateway: observation.gateway,
                invoice_amount: observation.invoice_amount,
                payment_hash: observation.payment_hash,
                fees: Some(observation.fees),
                fees_definitive: observation.terminal.is_some(),
            },
            Err(observe_error) => {
                notes.push(format!(
                    "could not read settlement fees for {op:?}: {observe_error:?}"
                ));
                RawOpUpdate {
                    op_id: Some(op),
                    ..RawOpUpdate::default()
                }
            }
        };
        if let Err(record_error) = self
            .record_terminal(key, status, self.now_ms(), error, Some(update))
            .await
        {
            notes.push(format!(
                "recording the terminal ledger row failed: {record_error:?}"
            ));
        }

        let intent_status = match status {
            OperationStatus::Succeeded => IntentStatus::Done,
            OperationStatus::Failed => IntentStatus::Failed,
            OperationStatus::Started | OperationStatus::Awaiting => return Ok(notes),
        };
        if let Some(intent) = self.get(key).await? {
            let role_matches = matches!(
                (&intent.action, role),
                (Action::Pay { .. }, RawOperationRole::Send)
                    | (Action::Receive { .. }, RawOperationRole::Receive)
            );
            if role_matches {
                self.set_status(key, intent_status, error).await?;
            }
        }
        Ok(notes)
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
        for decision in decisions {
            let Action::RefuseInflow { fed, .. } = &decision.action else {
                continue;
            };
            let fed = *fed;
            let reason = decision.reason;
            let key = &decision.idempotency_key;
            let mut dbtx = self.db.begin_transaction().await;
            ledger_upsert_in(&mut dbtx, key, |existing, seq| match existing {
                Some(_) => None,
                None => Some(OperationRecord {
                    seq,
                    correlation_key: key.clone(),
                    kind: OperationKind::Refusal { fed },
                    actor: Actor::Agent { occurrence },
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
        }
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

    /// Every decodable ledger row, ascending by `seq` (the `0x05` prefix scan order). Poison
    /// rows are skipped + warned like every other scan; a storage error surfaces.
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
            match decode_row_result::<OperationRecord>("ledger row", &raw_key, &value) {
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

    /// Newest-first time-windowed ledger rows needed by the watch probe scheduler.
    pub async fn probe_schedule_ledger_rows(
        &self,
        now_ms: u64,
        horizon_ms: u64,
    ) -> Result<Vec<OperationRecord>, ExecError> {
        let cutoff_ms = now_ms.saturating_sub(horizon_ms);
        let mut dbtx = self.db.begin_transaction_nc().await;
        let mut stream = dbtx
            .raw_find_by_prefix_sorted_descending(&[TAG_LEDGER_ROW])
            .await
            .map_err(db_err)?;
        let mut rows = Vec::new();
        while let Some((raw_key, value)) = stream.next().await {
            match decode_row_result::<OperationRecord>("ledger row", &raw_key, &value) {
                Ok(rec) => {
                    if rec.created_at_ms < cutoff_ms && rec.updated_at_ms < cutoff_ms {
                        continue;
                    }
                    rows.push(rec);
                }
                Err(e) => {
                    tracing::warn!(?raw_key, error = ?e, "journal: skipping undecodable ledger row")
                }
            }
        }
        Ok(rows)
    }

    /// Resolve a single ledger row by correlation key OR seq (§9.3, for `show`).
    pub async fn operation(
        &self,
        sel: &OperationRef,
    ) -> Result<Option<OperationRecord>, ExecError> {
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
            Some(bytes) => Ok(Some(decode_row_result("ledger row", &row_key, &bytes)?)),
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
        let value = encode_row(rec)?;
        let mut dbtx = self.db.begin_transaction().await;
        dbtx.raw_insert_bytes(&candidate_key(&rec.id), &value)
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
                continue;
            }
            match classify_key(&row.correlation_key) {
                KeyClass::Raw => match self.repair_raw(row, oracle, now).await {
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
    ) -> Result<usize, ExecError> {
        let Some((fed, op_id, payment_hash)) = raw_row_parts(&row.kind) else {
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
                    self.apply_observation(key, op, &obs, now, write, note)
                        .await?;
                    self.sync_raw_intent_from_observation(key, &obs).await?;
                    return Ok(1);
                }
                // Still in flight → leave Awaiting (truthful) for a later pass.
                Ok(0)
            }
            None => {
                // 1. The primary backfill: find the op by its `correlation_key` in `custom_meta`.
                if let Some(op) = oracle.find_op_by_correlation_key(fed, key).await? {
                    let obs = oracle.observe_op(fed, op).await?;
                    self.apply_observation(key, op, &obs, now, WriteKind::Authoritative, None)
                        .await?;
                    self.sync_raw_intent_from_observation(key, &obs).await?;
                    return Ok(1);
                }
                // 2. A deduped retry reuses the ORIGINAL op, so its key is in no op's meta; the
                //    durably-written payment hash is the recovery link (pay rows only).
                if let Some(hash) = payment_hash {
                    if let Some(op) = oracle.find_send_op_by_payment_hash(fed, hash).await? {
                        let obs = oracle.observe_op(fed, op).await?;
                        // Attempt attribution is uncertain (deduped retry OR never-sent
                        // attempt), so this is a SOFT correlation with the ambiguity recorded.
                        self.apply_observation(
                            key,
                            op,
                            &obs,
                            now,
                            WriteKind::Repair,
                            Some(HASH_DEDUP_NOTE),
                        )
                        .await?;
                        self.sync_raw_intent_from_observation(key, &obs).await?;
                        return Ok(1);
                    }
                }
                // 3. Nothing found: after 1h, a NEGATIVE inference — soft-`Failed` (truthful at
                //    attempt granularity: a no-hash row was malformed or crashed pre-parse).
                if now.saturating_sub(row.created_at_ms) >= REPAIR_AGE_MS {
                    self.apply_repair(
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
                    return Ok(1);
                }
                Ok(0)
            }
        }
    }

    async fn sync_raw_intent_from_observation(
        &self,
        key: &IdempotencyKey,
        observation: &RawOpObservation,
    ) -> Result<(), ExecError> {
        let Some(terminal) = &observation.terminal else {
            return Ok(());
        };
        self.sync_raw_intent_terminal(
            key,
            if terminal.succeeded {
                IntentStatus::Done
            } else {
                IntentStatus::Failed
            },
            terminal.error.as_deref(),
        )
        .await
    }

    async fn sync_raw_intent_terminal(
        &self,
        key: &IdempotencyKey,
        status: IntentStatus,
        error: Option<&str>,
    ) -> Result<(), ExecError> {
        let Some(intent) = self.get(key).await? else {
            return Ok(());
        };
        if matches!(intent.action, Action::Pay { .. } | Action::Receive { .. }) {
            self.set_status(key, status, error).await?;
        }
        Ok(())
    }

    /// Apply an op observation to a raw row: terminal → `Succeeded`/`Failed` carrying the
    /// definitive settlement enrichment; in-flight → `Awaiting`. `note` records an uncertain
    /// (hash-dedup) attribution; `write` decides whether the terminal is defeasible.
    async fn apply_observation(
        &self,
        key: &IdempotencyKey,
        op: OperationId,
        obs: &RawOpObservation,
        now: u64,
        write: WriteKind,
        note: Option<&str>,
    ) -> Result<(), ExecError> {
        let upd = RawOpUpdate {
            op_id: Some(op),
            gateway: obs.gateway.clone(),
            invoice_amount: obs.invoice_amount,
            payment_hash: obs.payment_hash,
            fees: Some(obs.fees),
            // A TERMINAL observation's fees are the §9.3 definitive settlement statement:
            // they must replace any pre-call estimate (even with `None` — an unknown
            // settlement fee must not be papered over by a stale estimate). An in-flight
            // observation merges as usual.
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
        self.apply_repair(
            key,
            status,
            now,
            Some(upd),
            combine_note(note, term_error),
            write,
        )
        .await
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

    /// Load the single watch-state row, seeding an absent row as [`WatchState::default`].
    /// A corrupt row fails closed like the targeted `0x08` probe read: reusing an unknown
    /// occurrence could collide with already-journaled tick keys.
    pub async fn get_watch_state(&self) -> Result<WatchState, ExecError> {
        let raw_key = watch_state_key();
        let mut dbtx = self.db.begin_transaction_nc().await;
        let Some(bytes) = dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? else {
            return Ok(WatchState::default());
        };
        decode_row_result("watch state", &raw_key, &bytes)
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
        let mut dbtx = self.db.begin_transaction().await;
        let mut state = match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
            Some(bytes) => decode_row_result::<WatchState>("watch state", &raw_key, &bytes)?,
            None => WatchState::default(),
        };
        state.discover_cursor = discover_cursor;
        state.discover_backlog = discover_backlog;
        state.discover_rotation = discover_rotation;
        if let Some(last_discover_ms) = last_discover_ms {
            state.last_discover_ms = last_discover_ms;
        }
        dbtx.raw_insert_bytes(&raw_key, &encode_row(&state)?)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(state)
    }

    /// Advance the persisted occurrence by one, preserving the discovery checkpoint fields.
    pub async fn advance_watch_occurrence(&self) -> Result<WatchState, ExecError> {
        let raw_key = watch_state_key();
        let mut dbtx = self.db.begin_transaction().await;
        let mut state = match dbtx.raw_get_bytes(&raw_key).await.map_err(db_err)? {
            Some(bytes) => decode_row_result::<WatchState>("watch state", &raw_key, &bytes)?,
            None => WatchState {
                occurrence: max_tick_occurrence_in(&mut dbtx).await?,
                ..WatchState::default()
            },
        };
        state.occurrence = state.occurrence.saturating_add(1);
        dbtx.raw_insert_bytes(&raw_key, &encode_row(&state)?)
            .await
            .map_err(db_err)?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(state)
    }
}

async fn max_tick_occurrence_in(
    dbtx: &mut impl IDatabaseTransactionOpsCore,
) -> Result<u64, ExecError> {
    let mut stream = dbtx
        .raw_find_by_prefix(&[TAG_LEDGER_ROW])
        .await
        .map_err(db_err)?;
    let mut max_occurrence = 0;
    while let Some((raw_key, value)) = stream.next().await {
        let row = match decode_row_result::<OperationRecord>("ledger row", &raw_key, &value) {
            Ok(row) => row,
            Err(e) => {
                tracing::warn!(?raw_key, error = ?e, "journal: skipping undecodable ledger row");
                continue;
            }
        };
        if let OperationKind::Tick { occurrence, .. } = row.kind {
            max_occurrence = max_occurrence.max(occurrence.0);
        }
    }
    Ok(max_occurrence)
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

/// Which repair family a correlation key belongs to (§10.3), by its `<verb>:` prefix.
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
    /// `AlreadyInFlight`/`AlreadyPaid` retry reuses the ORIGINAL op — its key is in no op's
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
        Ok(())
    }

    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        self.read_intent(key).await
    }

    async fn set_status(
        &self,
        key: &IdempotencyKey,
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
        let old_status = intent.status;
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
                        if intent.status != expected {
                            return Ok(false);
                        }
                        intent.status = new;

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

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        self.intents_indexed_as(&[IntentStatus::Pending, IntentStatus::Executing], false)
            .await
    }

    async fn awaiting(&self) -> Result<Vec<Intent>, ExecError> {
        self.intents_indexed_as(&[IntentStatus::Awaiting], false)
            .await
    }

    async fn reservation_intents(&self) -> Result<Vec<Intent>, ExecError> {
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

    async fn set_operation_artifact(
        &self,
        key: &IdempotencyKey,
        operation_id: OperationId,
        invoice: Option<&wallet_core::Invoice>,
    ) -> Result<(), ExecError> {
        let ikey = intent_key(key);
        let mut dbtx = self.db.begin_transaction().await;
        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
            return Err(ExecError::Permanent("journal: intent not found".into()));
        };
        let mut intent = decode_row_result::<Intent>("intent", &ikey, &bytes)?;
        intent.operation_id = Some(operation_id);
        if let Some(invoice) = invoice {
            intent.invoice = Some(invoice.clone());
        }
        dbtx.raw_insert_bytes(&ikey, &encode_row(&intent)?)
            .await
            .map_err(db_err)?;
        write_intent_ledger_row(&mut dbtx, &intent, self.now_ms(), None).await?;
        dbtx.commit_tx_result().await.map_err(db_err)?;
        Ok(())
    }

    fn store_id(&self) -> usize {
        self.store_id
    }
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

fn ledger_counter_key() -> Vec<u8> {
    vec![TAG_LEDGER_COUNTER]
}

fn read_be64(bytes: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(bytes).ok().map(u64::from_be_bytes)
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
        let existing: OperationRecord = decode_row_result("ledger row", &row_key, &bytes)?;
        if let Some(next) = build(Some(existing), seq) {
            dbtx.raw_insert_bytes(&row_key, &encode_row(&next)?)
                .await
                .map_err(db_err)?;
        }
    } else {
        let counter_key = ledger_counter_key();
        let next_seq = match dbtx.raw_get_bytes(&counter_key).await.map_err(db_err)? {
            Some(bytes) => read_be64(&bytes)
                .ok_or_else(|| ExecError::Permanent("journal: corrupt ledger counter".into()))?,
            None => 0,
        };
        if let Some(rec) = build(None, next_seq) {
            dbtx.raw_insert_bytes(&counter_key, &(next_seq + 1).to_be_bytes())
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

/// Copy the `0x02` move row's op-ids, gateway, and quoted fees onto an intent-backed ledger
/// row (§9.2). `Move`'s two op-ids come from here (not the single-op `RawOpUpdate`); a `None`
/// on the move row never clobbers a value already on the ledger row.
fn refresh_from_move(rec: &mut OperationRecord, mv: &MoveRecord) {
    match &mut rec.kind {
        OperationKind::Move {
            send_op,
            recv_op,
            gateway,
            ..
        } => {
            if mv.send_op.is_some() {
                *send_op = mv.send_op;
            }
            if mv.recv_op.is_some() {
                *recv_op = mv.recv_op;
            }
            *gateway = Some(mv.gateway.clone());
        }
        OperationKind::DirectInflow {
            recv_op, gateway, ..
        } => {
            if mv.recv_op.is_some() {
                *recv_op = mv.recv_op;
            }
            *gateway = Some(mv.gateway.clone());
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
