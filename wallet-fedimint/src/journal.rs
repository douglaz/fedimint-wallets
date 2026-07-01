//! [`FedimintJournal`] — the durable, async [`wallet_core::Journal`] backed by a fedimint
//! [`Database`] (spec §8). All journal rows live under the **app prefix `[0x00]`**
//! (`db.with_prefix(vec![0x00])`); the per-federation clients use `[0x01, ..]` (a later step).
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
use futures::StreamExt;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use wallet_core::FederationId;
use wallet_core::{ExecError, IdempotencyKey, Intent, IntentStatus, Journal};

/// The app-state partition prefix (spec §4/§8). Clients live at `[0x01, ..]` (a later step).
const APP_PREFIX: u8 = 0x00;

// Type tags within the app prefix.
const TAG_INTENT: u8 = 0x01;
const TAG_MOVE: u8 = 0x02;
const TAG_FEDERATION: u8 = 0x03;
const TAG_PENDING_INDEX: u8 = 0x04;

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

/// Durable [`wallet_core::Journal`] over a fedimint [`Database`], isolated to prefix `[0x00]`.
#[derive(Clone, Debug)]
pub struct FedimintJournal {
    /// Already `with_prefix(vec![0x00])`; all raw keys here are relative to that partition.
    db: Database,
    /// [`Journal::store_id`]: identity of `db`'s underlying storage, captured in [`Self::new`]
    /// from the pre-`with_prefix` handle (see there for why `with_prefix` itself can't supply
    /// it).
    store_id: usize,
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
        let store_id = Arc::as_ptr(&db.clone().into_inner()) as *const () as usize;
        Self {
            db: db.with_prefix(vec![APP_PREFIX]),
            store_id,
        }
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
    /// The ONE scan helper behind [`Journal::pending`], [`Journal::failed`], AND
    /// [`FedimintJournal::awaiting`] — so all three handle a poison row IDENTICALLY (the
    /// asymmetry of an earlier strict `awaiting` variant is gone). Scans are the always-on
    /// reconcile/resume path, NOT a targeted read: a malformed/dangling index entry, a
    /// missing intent, a corrupt row, or an index/intent status skew is SKIPPED (warn-logged)
    /// so one poison row cannot strand the healthy entries (or crash-loop the executor); the
    /// referenced Intent's real status is still re-checked before it is returned. Only a
    /// transient STORAGE error (the prefix scan or a row read failing at the db layer) is
    /// surfaced, as [`ExecError::Retryable`], so the caller can retry the whole pass — the
    /// `Vec`-returning trait methods swallow even that (see [`Journal::pending`]).
    async fn intents_indexed_as(
        &self,
        statuses: &[IntentStatus],
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
                    Ok(intent) if intent.idempotency_key != key => tracing::warn!(
                        index_key = %key.0,
                        embedded_key = %intent.idempotency_key.0,
                        "journal: index/intent key mismatch, skipping",
                    ),
                    Ok(intent) if statuses.contains(&intent.status) => out.push(intent),
                    Ok(intent) => tracing::warn!(
                        key = %key.0,
                        status = ?intent.status,
                        "journal: index/intent status skew, skipping",
                    ),
                    Err(e) => {
                        tracing::warn!(key = %key.0, error = ?e, "journal: skipping corrupt intent row");
                    }
                },
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
    /// NOT a [`Journal`] trait method, and DELIBERATELY separate from [`Journal::pending`]:
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
        self.intents_indexed_as(&[IntentStatus::Awaiting]).await
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
    ) -> Result<(), ExecError> {
        let ikey = intent_key(key);
        let mut dbtx = self.db.begin_transaction().await;
        let Some(bytes) = dbtx.raw_get_bytes(&ikey).await.map_err(db_err)? else {
            return Err(ExecError::Permanent("journal: intent not found".into()));
        };
        let mut intent = decode_row_result::<Intent>("intent", &ikey, &bytes)?;
        let old_status = intent.status;
        intent.status = status;

        write_intent_and_index(&mut dbtx, &ikey, key, old_status, &intent).await?;
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

                        write_intent_and_index(dbtx, &ikey, key, expected, &intent).await?;
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

    async fn pending(&self) -> Vec<Intent> {
        // The trait returns `Vec`, so a transient storage error can't be surfaced: warn and
        // return empty for this pass (the index is durable; the next reconcile retries).
        self.intents_indexed_as(&[IntentStatus::Pending, IntentStatus::Executing])
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = ?e, "journal: pending scan failed this pass, returning empty");
                Vec::new()
            })
    }

    async fn failed(&self) -> Vec<Intent> {
        self.intents_indexed_as(&[IntentStatus::Failed])
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = ?e, "journal: failed scan failed this pass, returning empty");
                Vec::new()
            })
    }

    fn store_id(&self) -> usize {
        self.store_id
    }
}

/// Rewrite the Intent row and move its `PendingIndexKey` entry from `old_status` to
/// `new_intent.status`, in the caller's already-open `dbtx` — the one-dbtx atomicity contract
/// (spec §8) shared by [`Journal::set_status`] and [`Journal::set_status_if`].
async fn write_intent_and_index(
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

/// A serde encode/decode failure is a data/logic bug, not transient → `Permanent`.
fn serde_err(e: serde_json::Error) -> ExecError {
    ExecError::Permanent(format!("journal serde error: {e}"))
}

fn decode_err(kind: &str, key: &[u8], e: serde_json::Error) -> ExecError {
    ExecError::Permanent(format!("journal: failed to decode {kind} row {key:?}: {e}"))
}
