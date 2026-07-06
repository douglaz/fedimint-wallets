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
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use wallet_core::{
    advance, kind_from_action, status_from_intent, Action, Actor, AllocatorDecision, ExecError,
    FederationId, FeeBreakdown, GatewayUrl, IdempotencyKey, Intent, IntentStatus, Journal, Msat,
    Occurrence, OperationId, OperationKind, OperationRecord, OperationStatus, RawOpUpdate,
    ReasonCode, WriteKind,
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

/// Rows older than this are eligible for reconcile's NEGATIVE-inference repairs (§10.3): a
/// fresh non-terminal row may belong to an operation still in flight in another process, so
/// absence-of-evidence conclusions are deferred one hour and written SOFT (`repaired: true`).
const REPAIR_AGE_MS: u64 = 60 * 60 * 1000;

/// `FederationInfo.joined_at` is unix SECONDS; a join-attempt row's `created_at_ms` is millis
/// from the same device clock. The join-repair arbitration (§10.3) compares them with this
/// slack added to the seconds→millis conversion.
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

    // --- ledger scans (spec §9.3, poison-tolerant) ---

    /// Every decodable ledger row, ascending by `seq` (the `0x05` prefix scan order). Poison
    /// rows are skipped + warned like every other scan; a storage error surfaces.
    async fn scan_ledger_rows(&self) -> Result<Vec<OperationRecord>, ExecError> {
        let mut dbtx = self.db.begin_transaction_nc().await;
        let mut stream = dbtx
            .raw_find_by_prefix(&[TAG_LEDGER_ROW])
            .await
            .map_err(db_err)?;
        let mut rows = Vec::new();
        while let Some((raw_key, value)) = stream.next().await {
            match decode_row_result::<OperationRecord>("ledger row", &raw_key, &value) {
                Ok(rec) => rows.push(rec),
                Err(e) => {
                    tracing::warn!(?raw_key, error = ?e, "journal: skipping undecodable ledger row")
                }
            }
        }
        Ok(rows)
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

    // --- reconcile repair (spec §10.3) ---

    /// Scan the FULL ledger for non-terminal (`Started`/`Awaiting`) rows and repair the stuck
    /// ones (§10.3). POSITIVE inferences (an op-log outcome; the registry contains the fed)
    /// apply immediately as ordinary terminal writes; NEGATIVE inferences (marking `Failed` on
    /// ABSENCE of evidence) are deferred one hour AND written SOFT (`repaired: true`), so a
    /// clock-skewed false `Failed` is superseded by the real writer instead of blocking it.
    /// Intent-keyed rows are NEVER repaired here — the journal integration (§9.2) owns them.
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

        // `pay:`/`recv:` and `tick:` rows repair individually.
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
                KeyClass::Tick => {
                    // A crash between a tick's Started and terminal write is otherwise
                    // unrepairable (later ticks use fresh nonces); age-gate keeps a live tick's
                    // row safe from a concurrent reconcile.
                    if now.saturating_sub(row.created_at_ms) >= REPAIR_AGE_MS {
                        self.apply_repair(
                            &row.correlation_key,
                            OperationStatus::Failed,
                            now,
                            None,
                            Some(TICK_INTERRUPTED.to_owned()),
                            WriteKind::Repair,
                        )
                        .await?;
                        summary.repaired += 1;
                    }
                }
                // Join handled above; intent-keyed / other rows are never repaired here.
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
                // `joined_at` is unix SECONDS; a row's `created_at_ms` is millis — convert (+slack).
                let cutoff = info
                    .joined_at
                    .saturating_mul(1000)
                    .saturating_add(JOINED_AT_SLACK_MS);
                let in_window = || attempts.iter().filter(|r| r.created_at_ms <= cutoff);
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
                    return Ok(1);
                }
                Ok(0)
            }
        }
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
const TICK_INTERRUPTED: &str = "interrupted — no terminal report";
const RAW_NEVER_REACHED: &str = "never reached the federation";
const HASH_DEDUP_NOTE: &str = "correlated by payment hash to an existing payment of this invoice; \
     attempt-level correlation uncertain (deduped retry or never-sent attempt); the matched \
     operation is authoritative";

/// Which repair family a correlation key belongs to (§10.3), by its `<verb>:` prefix.
enum KeyClass {
    Join,
    Tick,
    Raw,
    Other,
}

fn classify_key(key: &IdempotencyKey) -> KeyClass {
    let s = key.0.as_str();
    if s.starts_with("join:") {
        KeyClass::Join
    } else if s.starts_with("tick:") {
        KeyClass::Tick
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
        Some(next)
    })
    .await
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
