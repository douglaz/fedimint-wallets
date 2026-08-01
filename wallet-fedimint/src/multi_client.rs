//! `MultiClient` — one `fedimint_client::Client` per joined federation, all sharing a
//! single async fedimint `Database` (spec §1/§4). Owns the client LIFECYCLE (step 3:
//! join / open_all / balance / federations) and the raw lnv2 money PRIMITIVES (step 4a:
//! gateways / receive / pay / await_receive / await_send). The `FedimintExecutor` — fee
//! gross-up, `MoveRecord`/`Action` wiring, op-log backfill — lands on top in step 4b.

use crate::fee::GatewayFee;
use crate::journal::{
    FederationInfo, FedimintJournal, LedgerRepairOracle, RawOpObservation, RawTerminal,
};
use crate::move_protocol::{Leg, MoveMeta, OpArtifact};
use crate::types::{GatewayUrl, Invoice, OperationId, Preimage};
use async_trait::async_trait;
use fedimint_bip39::{Bip39RootSecretStrategy, Mnemonic};
use fedimint_client::db::ChronologicalOperationLogKey;
use fedimint_client::module::oplog::UpdateStreamOrOutcome;
use fedimint_client::secret::RootSecretStrategy as _;
use fedimint_client::{Client, ClientBuilder, ClientHandleArc, RootSecret};
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::config::ClientConfig;
use fedimint_core::core::OperationId as FedimintOperationId;
use fedimint_core::db::{Database, IDatabaseTransactionOpsCore};
use fedimint_core::invite_code::InviteCode;
use fedimint_core::util::SafeUrl;
use fedimint_core::BitcoinHash as _;
use fedimint_core::{runtime, Amount};
use fedimint_lnv2_client::common::gateway_api::{PaymentFee, RoutingInfo};
use fedimint_lnv2_client::common::{Bolt11InvoiceDescription, LightningInvoice};
use fedimint_lnv2_client::{
    FinalReceiveOperationState, FinalSendOperationState, LightningClientModule,
    LightningOperationMeta, ReceiveOperationState, SendOperationState, SendPaymentError,
};
use futures::lock::Mutex;
use futures::{FutureExt, StreamExt};
use lightning_invoice::Bolt11Invoice;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::str::FromStr as _;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wallet_core::{ExecError, FederationId, FeeBreakdown, IdempotencyKey, Msat};

/// Tag byte for a per-federation client partition (spec §4 "Storage"): client `i` lives
/// at `[CLIENT_PREFIX_TAG] ++ u32_le(db_prefix)`, exactly 5 bytes. Fixed-length is
/// load-bearing: a variable-length prefix could alias (`[0x01,0x00]` vs `[0x01],[0x00,..]`).
const CLIENT_PREFIX_TAG: u8 = 0x01;

/// The join lock is held across the federation-config preview fetch. Cap that fetch so a slow or
/// unreachable federation cannot hold `join_lock` — and thereby queue every other join/recovery
/// (NOT money ops; pay/receive/move never take the lock) — indefinitely. Generous enough for a
/// healthy multi-guardian preview (each guardian call is itself request-bounded), short enough to
/// bound the worst case. On timeout the join/recovery fails cleanly: its recovery reservation (if
/// any) drops on the early return and no fresh db partition was allocated yet.
const PREVIEW_UNDER_LOCK_TIMEOUT: Duration = Duration::from_secs(60);

/// One fedimint client per joined federation. `db` is the CLIENT store — each client `i`
/// at `[0x01] ++ u32_le(db_prefix)`, plus fedimint's own client secret — while the app
/// journal lives in its OWN separate `Database` (see [`Self::new`] for why co-locating
/// them wedges fedimint's long-held lnv2 transactions). Concrete type, no trait
/// (ADR-0021) — `MultiClient` is the one production impl.
pub struct MultiClient {
    db: Database,
    journal: FedimintJournal,
    connectors: ConnectorRegistry,
    root_secret: RootSecret,
    /// A plain sync lock, not an async one: every critical section here is a pure map
    /// read/insert with no `.await` inside it, so a `std::sync::RwLock` is the right,
    /// non-async-poisoning-prone tool, while still letting [`Self::federations`] stay a
    /// sync fn (spec §4).
    clients: RwLock<BTreeMap<FederationId, ClientHandleArc>>,
    /// Serializes db-prefix allocation and initial client creation so two concurrent joins
    /// cannot initialize different federations into the same per-fed partition.
    join_lock: Mutex<()>,
    /// Process-local reservation held from recovery admission through final registration. A join
    /// for the same federation checks this under `join_lock`, so it cannot initialize a second
    /// client while the long recovery replay runs. This is deliberately NOT durable recovery
    /// lifecycle state: a failed/cancelled recovery drops the reservation and leaves its fresh
    /// partition inert, exactly as D3/D4 require.
    active_recoveries: RwLock<BTreeSet<FederationId>>,
    /// Pooled HTTP client for DIRECT gateway reads (`routing_info`). The SDK's
    /// `GatewayApi` route is deliberately bypassed for these: its per-URL
    /// `ConnectionPool` treats every http(s) connection as disconnected
    /// (`HttpConnection::is_connected()` is hard-coded `false` at the pin), so every
    /// request after the first re-enters the RECONNECT path and sleeps a Fibonacci
    /// backoff starting at 500 ms — measured live by the §6a.9 responsiveness gate as
    /// 550-730 ms added to every pre-fund fee quote, with concurrent quotes coalescing
    /// behind one leader. A quote is a pure HTTP read with no state-machine
    /// involvement, so a plain pooled client is strictly better; the SDK's own
    /// settlement calls (send_payment, invoice fetch inside the lnv2 state machines)
    /// keep their path — that latency is async settlement, not responsiveness.
    gateway_http: reqwest::Client,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinDeadlineOutcome {
    Joined(JoinOutcome),
    DeadlineElapsed,
}

#[derive(Clone, Copy)]
struct JoinDeadline {
    started_at: Instant,
    budget: Duration,
}

impl JoinDeadline {
    fn new(budget: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            budget,
        }
    }

    fn remaining(self) -> Option<Duration> {
        let remaining = self.budget.checked_sub(self.started_at.elapsed())?;
        (!remaining.is_zero()).then_some(remaining)
    }
}

struct JoinDeadlineElapsed;

impl MultiClient {
    /// Derive the root secret once from `mnemonic` (`StandardDoubleDerive` — the
    /// per-federation mix-in happens INSIDE the fedimint builder on join/open; callers
    /// must never pre-derive it, per the builder's own contract). `db` holds ONLY the
    /// fedimint clients (+ the client secret); `journal_db` is a SEPARATE store for the
    /// app journal. They MUST be different RocksDBs (the 24h soak, 2026-07): fedimint
    /// tunes RocksDB to a 2MB write buffer with NO extra memtable history, so any
    /// fedimint transaction held open while a co-located writer (our journal's
    /// tick/ledger churn) flushes ~2MB loses its snapshot's memtable history and the
    /// commit fails `TryAgain` (mapped to `WriteConflict` REGARDLESS of key
    /// disjointness). The known instance — lnv2's `receive_lnurl_task` panicking across
    /// its long-poll — is fixed at our pinned rev (upstream PR #8816), but the isolation
    /// stands: our churn must never share a memtable with fedimint's transactions.
    pub async fn new(db: Database, journal_db: Database, mnemonic: Mnemonic) -> Self {
        let root_secret = RootSecret::StandardDoubleDerive(
            Bip39RootSecretStrategy::<12>::to_root_secret(&mnemonic),
        );
        let connectors = ConnectorRegistry::build_from_client_defaults()
            .bind()
            .await
            .expect("binding the default client connectors performs no I/O and cannot fail");
        Self {
            journal: FedimintJournal::new(journal_db),
            db,
            connectors,
            root_secret,
            clients: RwLock::new(BTreeMap::new()),
            join_lock: Mutex::new(()),
            active_recoveries: RwLock::new(BTreeSet::new()),
            // Bounded: a quote is pre-fund and advisory (no money has moved), so timing out
            // against an unresponsive gateway is safe — the driver surfaces Retryable and the
            // intent stays Pending. The SDK path had NO deadline here, which left a driver
            // hanging on a black-holed quote until abandonment.
            gateway_http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("static reqwest client configuration cannot fail"),
        }
    }

    /// Join `invite`'s federation, assigning it the next `db_prefix` and persisting a
    /// [`FederationInfo`] row. Idempotent: a federation already joined (in-memory, or
    /// recorded in the journal from a previous run) is opened instead of re-joined.
    pub async fn join(&self, invite: InviteCode) -> anyhow::Result<JoinOutcome> {
        match self.join_inner(invite, None).await? {
            JoinDeadlineOutcome::Joined(outcome) => Ok(outcome),
            JoinDeadlineOutcome::DeadlineElapsed => {
                unreachable!("unbounded joins do not install a deadline")
            }
        }
    }

    pub(crate) async fn join_before_deadline(
        &self,
        invite: InviteCode,
        deadline: Duration,
    ) -> anyhow::Result<JoinDeadlineOutcome> {
        self.join_inner(invite, Some(JoinDeadline::new(deadline)))
            .await
    }

    async fn join_inner(
        &self,
        invite: InviteCode,
        deadline: Option<JoinDeadline>,
    ) -> anyhow::Result<JoinDeadlineOutcome> {
        let id = bridge_federation_id(invite.federation_id());

        if self.has_client(&id) {
            return Ok(JoinDeadlineOutcome::Joined(JoinOutcome::opened(id)));
        }

        let _join_guard = match join_deadline(deadline, self.join_lock.lock()).await {
            Ok(guard) => guard,
            Err(JoinDeadlineElapsed) => return Ok(JoinDeadlineOutcome::DeadlineElapsed),
        };

        if self.has_client(&id) {
            return Ok(JoinDeadlineOutcome::Joined(JoinOutcome::opened(id)));
        }
        self.ensure_recovery_not_in_progress(&id)?;
        if let Some(info) = self
            .journal
            .get_federation(&id)
            .await
            .map_err(|e| anyhow::anyhow!("reading federation registry: {e:?}"))?
        {
            // Registered on a previous run (or by a concurrent process): open, don't re-join. We
            // already hold `join_lock` here, so call the locked body directly (the lock is not
            // re-entrant).
            return Ok(JoinDeadlineOutcome::Joined(JoinOutcome::opened(
                self.open_one_locked(&info).await?,
            )));
        }

        let preview = match join_deadline(deadline, async {
            // Bound the preview even when the caller sets NO deadline (a user-initiated join), so an
            // unreachable fed cannot hold `join_lock` — and queue every other join — indefinitely.
            // When a caller deadline is also present, whichever bound is tighter fires first.
            let build_and_preview = async {
                self.client_builder()
                    .await?
                    .preview(self.connectors.clone(), &invite)
                    .await
            };
            runtime::timeout(PREVIEW_UNDER_LOCK_TIMEOUT, build_and_preview)
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "federation preview timed out after {}s while holding the join lock",
                        PREVIEW_UNDER_LOCK_TIMEOUT.as_secs()
                    )
                })?
        })
        .await
        {
            Ok(preview) => preview?,
            Err(JoinDeadlineElapsed) => return Ok(JoinDeadlineOutcome::DeadlineElapsed),
        };
        let db_prefix = self.next_db_prefix().await?;
        let client = match join_deadline(
            deadline,
            preview.join(self.client_db(db_prefix), self.root_secret.clone()),
        )
        .await
        {
            Ok(Ok(client)) => Arc::new(client),
            Ok(Err(e)) => {
                self.remove_client_partition_best_effort(db_prefix).await;
                return Err(e);
            }
            Err(JoinDeadlineElapsed) => {
                self.remove_client_partition_best_effort(db_prefix).await;
                return Ok(JoinDeadlineOutcome::DeadlineElapsed);
            }
        };

        let joined_id = bridge_federation_id(client.federation_id());
        if joined_id != id {
            self.remove_client_partition_best_effort(db_prefix).await;
            anyhow::bail!(
                "joined federation id {} did not match invite id {}",
                joined_id.to_hex(),
                id.to_hex()
            );
        }
        let info = FederationInfo {
            invite: invite.to_string(),
            db_prefix,
            joined_at: unix_now(),
        };
        self.journal
            .put_federation(&joined_id, &info)
            .await
            .map_err(|e| anyhow::anyhow!("persisting federation registry: {e:?}"))?;
        self.clients
            .write()
            .expect("client map lock poisoned")
            .insert(joined_id, client);
        Ok(JoinDeadlineOutcome::Joined(JoinOutcome {
            id: joined_id,
            newly_joined: true,
        }))
    }

    /// Rebuild `invite`'s federation from the seed via [`ClientPreview::recover`]
    /// (`docs/archive/wallet-recovery-spec.md`, D3/D4). A DELIBERATE, user-initiated last resort: the
    /// auto-join and driver-retry paths keep calling [`Self::join`], and the allocator's
    /// `decide()` never emits it.
    ///
    /// - If the federation is still **REGISTERED** (a durable registry row exists — open OR
    ///   registered-but-unopened) → REFUSE: a surviving journal still holds non-terminal `Pay`/
    ///   `Move` intents that reconcile re-drives, and recovery's fresh EMPTY oplog would miss the
    ///   cross-restart send-dedup → double-pay. Recovery only ever runs where NO registry row (and
    ///   thus no surviving intent) exists. Otherwise recover into a **FRESH**
    ///   [`Self::next_db_prefix`]; V1 never wipes or reuses a partition, so any pre-existing
    ///   partition is left untouched and inert (recovery always INSERTS a new registry row for the
    ///   unregistered fed).
    /// - Await completion via [`Client::wait_for_all_recoveries`] — the pinned SDK returns `Err` on
    ///   a failed module recovery instead of parking forever, so recovery is complete-or-fail —
    ///   while a side task logs progress from [`Client::subscribe_to_recovery_progress`].
    /// - **Completion is stronger than that call returning:** the recovery-phase handle omits every
    ///   recovered module from its live registry and sits mid-init, so we stop it, REOPEN the
    ///   now-`done` partition (the normal init path, which registers every module), and drain its
    ///   state machines before registering — only then is `balance` readable (D4/D5).
    /// - The final [`FedimintJournal::complete_recovery`] commit + the in-memory insert run under
    ///   `join_lock` with a re-checked `has_client`, so the registration cannot silently replace a
    ///   client that went live meanwhile. On any failure the fresh partition is abandoned inert; a
    ///   retry gets a clean prefix (D4.7).
    pub async fn recover(
        &self,
        invite: InviteCode,
        recovery_key: &IdempotencyKey,
    ) -> anyhow::Result<FederationId> {
        let id = bridge_federation_id(invite.federation_id());

        // Fast refuse before contending for the lock; re-checked authoritatively under it. Recovery
        // refuses ANY registered fed (open OR registered-but-unopened), not merely an open one: a
        // surviving journal still holds re-drivable `Pay`/`Move` intents that a fresh empty oplog
        // would double-pay (D3).
        ensure_recover_not_registered(self.is_registered(&id).await?, &id)?;

        // Serialize FRESH-prefix allocation + partition materialization with `join` (both take
        // `next_db_prefix`). Held only across the preview fetch (bounded by `PREVIEW_UNDER_LOCK_TIMEOUT`
        // below) + `recover` init — NOT the long module replay, which runs after the lock is released.
        let join_guard = self.join_lock.lock().await;
        ensure_recover_not_registered(self.is_registered(&id).await?, &id)?;
        // Keep this per-fed reservation from BEFORE prefix allocation through the in-memory insert.
        // The global join lock can then be released after the fresh partition is materialized,
        // letting unrelated federation joins proceed, while `join`/`open_one` still cannot open a
        // second client for THIS federation until the reservation drops (D4 concurrency).
        let _recovery_reservation = self.reserve_recovery(id)?;
        // Bound the preview so an unreachable/slow fed cannot hold `join_lock` (and queue every other
        // join/recovery) indefinitely. On timeout `_recovery_reservation` drops on the early return
        // and no fresh partition was allocated yet (`next_db_prefix` runs below), so a retry is clean.
        let builder = self.client_builder().await?;
        let preview = runtime::timeout(
            PREVIEW_UNDER_LOCK_TIMEOUT,
            builder.preview(self.connectors.clone(), &invite),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "federation {} preview timed out after {}s while holding the join lock; recovery abandoned",
                id.to_hex(),
                PREVIEW_UNDER_LOCK_TIMEOUT.as_secs(),
            )
        })??;
        let db_prefix = self.next_db_prefix().await?;
        // `recover` initializes the fresh partition (its config row makes the prefix visible to a
        // concurrent `next_db_prefix`) and spawns the module-recovery task; the replay is awaited
        // below. Failure here leaves the fresh partition inert — a retry gets a clean prefix.
        let recovery_client = preview
            .recover(self.client_db(db_prefix), self.root_secret.clone(), None)
            .await?;
        drop(join_guard);

        // Block on the sole completion gate; a side task logs progress (never a completion signal).
        wait_for_recoveries_with_progress(&recovery_client).await?;

        let recovered_id = bridge_federation_id(recovery_client.federation_id());
        if recovered_id != id {
            anyhow::bail!(
                "recovered federation id {} did not match invite id {}",
                recovered_id.to_hex(),
                id.to_hex()
            );
        }

        // The recovery-phase handle is NOT usable and must NOT be registered as-is: fedimint omits
        // every recovered module from THIS handle's live registry and leaves it mid-init, so
        // `balance`/send would fail "Primary module not available" while `federations()` reported
        // the fed open (so `open_all` would never self-heal it). Stop it, then reopen the partition
        // — now that the module recovery state is persisted `done`, `open` registers every module
        // and yields a client that can read balance and send.
        recovery_client.shutdown().await;
        let client = self.open_recovered_partition(db_prefix).await?;
        // Reopening starts the state machines that materialize recovered notes into spendable
        // balance (the gateway's own recovery path waits on this same gate before reading balance).
        // Register only after it completes, so `balance` never reads a transient zero.
        client.wait_for_all_active_state_machines().await?;

        let info = FederationInfo {
            invite: invite.to_string(),
            db_prefix,
            joined_at: unix_now(),
        };
        // Re-acquire the join lock for the final commit + insert. The recovery reservation has kept
        // `join`/`open_one` off this federation throughout the replay; re-checking `has_client`
        // under the lock closes the last window so the registration can never silently replace a
        // client that went live meanwhile (it cannot, given the reservation — the invariant is
        // enforced here, not merely assumed).
        let _commit_guard = self.join_lock.lock().await;
        if self.has_client(&id) {
            // Unreachable given the reservation, but never REPLACE a live client: refuse and shut
            // down our fresh handle. We hold the only `Arc` reference, so `try_unwrap` yields the
            // `ClientHandle` for a clean shutdown (a failed unwrap falls through to `Drop`).
            if let Ok(handle) = Arc::try_unwrap(client) {
                handle.shutdown().await;
            }
            anyhow::bail!(
                "federation {} went live during recovery; refusing to replace the live client",
                id.to_hex()
            );
        }
        // Publish the registry row, record durable user ownership, and terminalize this recovery's
        // intent atomically (D4.6). Otherwise a crash after registration but before the driver's
        // ordinary Done write would reopen this client on startup and re-drive the still-Executing
        // action into the refuse-if-registered path, falsely reporting a completed recovery as
        // failed.
        self.journal
            .complete_recovery(&id, &info, &invite, recovery_key)
            .await
            .map_err(|e| anyhow::anyhow!("committing recovered federation: {e:?}"))?;
        self.clients
            .write()
            .expect("client map lock poisoned")
            .insert(id, client);
        Ok(id)
    }

    /// Whether `id` currently holds a process-local recovery reservation. Read by both
    /// [`Self::ensure_recovery_not_in_progress`] (the join path) and [`Self::open_one`] (the
    /// scheduler open path), so no open path can materialize a second client on the fresh partition
    /// an in-flight recovery owns (D4 concurrency).
    fn recovery_in_progress(&self, id: &FederationId) -> bool {
        self.active_recoveries
            .read()
            .expect("active recovery set lock poisoned")
            .contains(id)
    }

    /// Whether a recovery of `id` is in flight — the reservation is held from before the fresh
    /// partition is allocated until AFTER `recover` inserts the live client. The daemon reads this
    /// so a terminal recovery status is not reported while `/v1/balance` still omits the fed: in the
    /// window between `complete_recovery`'s commit (which terminalizes the intent) and the in-memory
    /// client insert, the op would otherwise read succeeded with the fed not yet open.
    pub fn is_recovering(&self, id: &FederationId) -> bool {
        self.recovery_in_progress(id)
    }

    fn ensure_recovery_not_in_progress(&self, id: &FederationId) -> anyhow::Result<()> {
        if self.recovery_in_progress(id) {
            anyhow::bail!(
                "federation {} recovery is already in progress; wait for it to finish before \
                 joining",
                id.to_hex()
            );
        }
        Ok(())
    }

    fn reserve_recovery(&self, id: FederationId) -> anyhow::Result<RecoveryReservation<'_>> {
        if !self
            .active_recoveries
            .write()
            .expect("active recovery set lock poisoned")
            .insert(id)
        {
            anyhow::bail!(
                "federation {} recovery is already in progress; wait for it to finish",
                id.to_hex()
            );
        }
        Ok(RecoveryReservation {
            active: &self.active_recoveries,
            id,
        })
    }

    /// Open every already-joined federation, BEST-EFFORT: a federation whose client fails
    /// to open is warn-logged and skipped, never aborting the batch. This mirrors the
    /// journal's own poison-tolerance ([`FedimintJournal::list_federations`] skips bad rows
    /// precisely so one federation cannot strand the others) — one un-openable fed must not
    /// block seeing the healthy feds' balances or joining a new one. Each opened client
    /// self-resumes its own state machines (spec §9.1) — we never re-implement that resume.
    pub async fn open_all(&self, feds: &[FederationInfo]) -> anyhow::Result<()> {
        for info in feds {
            if let Err(e) = self.open_one(info).await {
                tracing::warn!(
                    db_prefix = info.db_prefix,
                    error = ?e,
                    "multi_client: skipping federation that failed to open"
                );
            }
        }
        Ok(())
    }

    /// This federation's spendable balance, at msat granularity.
    pub async fn balance(&self, id: &FederationId) -> anyhow::Result<Msat> {
        let client = self
            .clients
            .read()
            .expect("client map lock poisoned")
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("federation {} not joined/opened", id.to_hex()))?;
        let amount = client.get_balance_for_btc().await?;
        Ok(Msat(amount.msats))
    }

    /// Every federation this `MultiClient` currently holds an open client for.
    pub fn federations(&self) -> Vec<FederationId> {
        self.clients
            .read()
            .expect("client map lock poisoned")
            .keys()
            .copied()
            .collect()
    }

    pub fn spawn_expiry_wake_tasks(
        &self,
        subscribed: &mut BTreeSet<FederationId>,
        wake_tx: tokio::sync::mpsc::Sender<(FederationId, Option<u64>)>,
    ) -> Vec<runtime::JoinHandle<()>> {
        let clients = self
            .clients
            .read()
            .expect("client map lock poisoned")
            .iter()
            .map(|(id, client)| (*id, client.clone()))
            .collect::<Vec<_>>();
        let mut tasks = Vec::new();
        for (id, client) in clients {
            if !subscribed.insert(id) {
                continue;
            }
            let wake_tx = wake_tx.clone();
            tasks.push(runtime::spawn("wallet-watch-expiry-wake", async move {
                let meta_service = client.meta_service().clone();
                let mut stream = Box::pin(
                    meta_service
                        .subscribe_to_field::<u64>(client.db(), "federation_expiry_timestamp"),
                );
                if stream.next().await.is_none() {
                    return;
                }
                while let Some(value) = stream.next().await {
                    let expiry_ms = value
                        .and_then(|value| value.value)
                        .map(|secs| secs.saturating_mul(1000));
                    if wake_tx.send((id, expiry_ms)).await.is_err() {
                        break;
                    }
                }
            }));
        }
        tasks
    }

    /// Fetch and authenticate a federation config from an invite without joining or writing a
    /// client partition (§5.1.2 step 2). This is the same preview fetch `join` uses before the
    /// partition write.
    pub async fn preview_config(&self, invite: &InviteCode) -> anyhow::Result<ClientConfig> {
        let preview = self
            .client_builder()
            .await?
            .preview(self.connectors.clone(), invite)
            .await?;
        Ok(preview.config().clone())
    }

    pub(crate) fn has_client(&self, id: &FederationId) -> bool {
        self.clients
            .read()
            .expect("client map lock poisoned")
            .contains_key(id)
    }

    /// Whether `id` has a durable registry row — open OR registered-but-unopened. Recovery refuses
    /// any registered fed (D3): a surviving journal may hold non-terminal intents a fresh recovery
    /// oplog would double-pay. (Open implies registered, so this subsumes the `has_client` check.)
    async fn is_registered(&self, id: &FederationId) -> anyhow::Result<bool> {
        Ok(self
            .journal
            .get_federation(id)
            .await
            .map_err(|e| anyhow::anyhow!("reading federation registry: {e:?}"))?
            .is_some())
    }

    /// Open one already-joined federation's client from its registry row and insert it into the
    /// map, serializing on `join_lock` so a registered fed is never double-opened. The watch
    /// scheduler's `open_all` races a user `join`'s open for the same fed; without the lock both
    /// could pass the `has_client` check and build two `ClientHandle`s over one db partition — two
    /// executors on one partition, the product-forbidden state (this module's core invariant).
    /// Callers that ALREADY hold `join_lock` (e.g. `join_inner`) must call
    /// [`Self::open_one_locked`] directly, because the lock is not re-entrant.
    async fn open_one(&self, info: &FederationInfo) -> anyhow::Result<FederationId> {
        let _join_guard = self.join_lock.lock().await;
        self.open_one_locked(info).await
    }

    /// The body of [`Self::open_one`], assuming `join_lock` is ALREADY held by the caller so that
    /// the `has_client` checks and the open+insert are serialized against every other join/open of
    /// the same federation.
    async fn open_one_locked(&self, info: &FederationInfo) -> anyhow::Result<FederationId> {
        // Honor the recovery reservation on THIS path too, not just `join_inner` (D4 concurrency):
        // the watch scheduler calls `open_all` → `open_one` for every registered-but-unopened fed
        // each cycle, and a fed whose recovery is in flight owns its fresh partition until `recover`
        // inserts the live client. Opening it here — in the window between `complete_recovery`'s
        // registry write and that insert — would run a SECOND handle over the same partition. Derive
        // the id from the registry row's canonical invite so we can skip BEFORE opening; a
        // malformed/unparseable invite is never the just-registered recovery target, so fall through.
        if let Ok(invite) = InviteCode::from_str(&info.invite) {
            let id = bridge_federation_id(invite.federation_id());
            if self.recovery_in_progress(&id) {
                anyhow::bail!(
                    "federation {} recovery is in progress; skipping open until it registers",
                    id.to_hex()
                );
            }
            // `open_all` may have snapshotted this registry row while recovery was publishing it,
            // then reached it only after recovery inserted the client and released its reservation.
            // Treat that stale work item as already complete instead of opening a second handle on
            // the recovered partition.
            if self.has_client(&id) {
                return Ok(id);
            }
        }
        let client: ClientHandleArc = self
            .client_builder()
            .await?
            .open(
                self.connectors.clone(),
                self.client_db(info.db_prefix),
                self.root_secret.clone(),
            )
            .await
            .map(Arc::new)?;
        let id = bridge_federation_id(client.federation_id());
        // Under `join_lock`: for a normal (parseable-invite) row the `has_client` check above
        // already returned early, so this is reached only when that pre-open check was skipped (a
        // corrupt/unparseable registry invite, whose id is knowable only after `open`). Never
        // REPLACE a live client with a second handle — drop our fresh one, as `join_inner`'s
        // commit-time guard does. We hold the only `Arc`, so `try_unwrap` yields the handle for a
        // clean shutdown (a failed unwrap falls through to `Drop`).
        if self.has_client(&id) {
            if let Ok(handle) = Arc::try_unwrap(client) {
                handle.shutdown().await;
            }
            return Ok(id);
        }
        self.clients
            .write()
            .expect("client map lock poisoned")
            .insert(id, client);
        Ok(id)
    }

    /// Open a partition whose module recovery has COMPLETED into a fully-materialized client.
    /// Unlike the recovery-phase handle returned by [`ClientPreview::recover`], this `open` takes
    /// the normal init path (the persisted module recovery state is `done`), so it registers every
    /// recovered module and yields a client that can read balance and send. The caller MUST have
    /// stopped the recovery-phase client first — two live handles on one partition would run two
    /// executors over the same db.
    async fn open_recovered_partition(&self, db_prefix: u32) -> anyhow::Result<ClientHandleArc> {
        self.client_builder()
            .await?
            .open(
                self.connectors.clone(),
                self.client_db(db_prefix),
                self.root_secret.clone(),
            )
            .await
            .map(Arc::new)
    }

    /// The next unused `db_prefix`: one past the highest already recorded in the
    /// registry OR present in an initialized/orphaned client partition. The root DB scan
    /// closes the crash window where fedimint commits partition `N` but the process dies
    /// before the journal records `N`; the allocator must never reuse that partition for
    /// a different federation.
    async fn next_db_prefix(&self) -> anyhow::Result<u32> {
        let feds = self
            .journal
            .list_federations()
            .await
            .map_err(|e| anyhow::anyhow!("reading federation registry: {e:?}"))?;
        let mut max_db_prefix = feds.iter().map(|(_, info)| info.db_prefix).max();

        let mut dbtx = self.db.begin_transaction_nc().await;
        let mut stream = dbtx.raw_find_by_prefix(&[CLIENT_PREFIX_TAG]).await?;
        while let Some((key, _value)) = stream.next().await {
            let Some(db_prefix_bytes) = key.get(1..5) else {
                tracing::warn!(
                    ?key,
                    "multi_client: skipping malformed client partition key"
                );
                continue;
            };
            let db_prefix = u32::from_le_bytes(
                db_prefix_bytes
                    .try_into()
                    .expect("slice length checked above"),
            );
            max_db_prefix = Some(max_db_prefix.map_or(db_prefix, |max| max.max(db_prefix)));
        }

        max_db_prefix.map_or(Ok(0), |max| {
            max.checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("exhausted u32 federation db prefixes"))
        })
    }

    /// Client `i`'s partition: `db.with_prefix([CLIENT_PREFIX_TAG] ++ u32_le(db_prefix))`.
    fn client_db(&self, db_prefix: u32) -> Database {
        self.db.with_prefix(client_prefix_bytes(db_prefix))
    }

    async fn remove_client_partition_best_effort(&self, db_prefix: u32) {
        if let Err(e) = self.remove_client_partition(db_prefix).await {
            tracing::warn!(
                db_prefix,
                error = ?e,
                "multi_client: failed to remove unfinished client partition"
            );
        }
    }

    async fn remove_client_partition(&self, db_prefix: u32) -> anyhow::Result<()> {
        let mut dbtx = self.db.begin_transaction().await;
        dbtx.raw_remove_by_prefix(&client_prefix_bytes(db_prefix))
            .await?;
        dbtx.commit_tx_result().await?;
        Ok(())
    }

    /// A fresh [`ClientBuilder`] with the modules a devimint federation uses: mint,
    /// wallet, lnv1 `ln`, lnv2 (verified against `~/p/fedimint/fedimint-cli/src/lib.rs`'s
    /// own module registration). No admin creds — Phase 1 never needs guardian access.
    async fn client_builder(&self) -> anyhow::Result<ClientBuilder> {
        let mut builder = Client::builder().await?;
        builder.with_module(fedimint_ln_client::LightningClientInit::default());
        builder.with_module(fedimint_mint_client::MintClientInit);
        builder.with_module(fedimint_wallet_client::WalletClientInit::default());
        builder.with_module(fedimint_lnv2_client::LightningClientInit::default());
        Ok(builder)
    }

    // ---- lnv2 money primitives (spec §4, step 4a) ----------------------------------
    //
    // Thin wrappers over `fedimint_lnv2_client::LightningClientModule` (the shared-gateway
    // internal-swap path validated live in docs/fedimint-mechanics.md §5). NO fee gross-up,
    // no MoveRecord/Action wiring, no op-log backfill — those are step 4b.

    /// This federation's registered lnv2 gateways (its guardian-vetted list) so the caller
    /// can pin one explicitly. NOTE: devimint does NOT auto-register its LDK gateway here,
    /// so this list can be empty even when a usable gateway exists — in that case the caller
    /// passes the gateway URL directly to [`Self::receive`]/[`Self::pay`] (runbook §4).
    pub async fn gateways(&self, id: &FederationId) -> anyhow::Result<Vec<GatewayUrl>> {
        let client = self.client(id)?;
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        let urls = lnv2
            .list_gateways(None)
            .await
            .map_err(|e| anyhow::anyhow!("listing lnv2 gateways for {}: {e}", id.to_hex()))?;
        Ok(urls.into_iter().map(bridge_gateway_url).collect())
    }

    /// Generate a BOLT11 invoice to receive `amount` into `id` via lnv2. NOT idempotent —
    /// each call mints a FRESH invoice/op-id (spec §3), so the caller must persist the
    /// returned pair. `gateway` is passed straight through (`None` → lnv2 auto-selects);
    /// `custom_meta` is committed into the operation meta by fedimint (the move-coordination
    /// hook lands in step 4b).
    pub async fn receive(
        &self,
        id: &FederationId,
        amount: Msat,
        gateway: Option<GatewayUrl>,
        custom_meta: serde_json::Value,
    ) -> anyhow::Result<(Invoice, OperationId)> {
        let client = self.client(id)?;
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        let (invoice, op) = lnv2
            .receive(
                Amount::from_msats(amount.0),
                RECEIVE_EXPIRY_SECS,
                Bolt11InvoiceDescription::Direct(String::new()),
                parse_gateway(gateway)?,
                custom_meta,
            )
            .await
            .map_err(|e| anyhow::anyhow!("lnv2 receive on {}: {e}", id.to_hex()))?;
        Ok((bridge_invoice(&invoice), bridge_op_id(op)))
    }

    /// Pay a BOLT11 invoice from `id` via lnv2. The lnv2 client is the dedup AUTHORITY
    /// (deterministic op-id, ONE attempt per invoice): re-paying an in-flight or settled
    /// invoice returns [`SendOutcome::AlreadyInFlight`] carrying the ORIGINAL
    /// op-id — never a double-pay (spec §4). `custom_meta` is committed into the operation
    /// meta. A failure is a typed [`SendError`] so the caller can tell a DETERMINISTIC
    /// rejection (expired/wrong-currency — re-paying the SAME invoice can never succeed) from a
    /// retryable route/transport fault — §15.4.
    pub async fn pay(
        &self,
        id: &FederationId,
        invoice: Invoice,
        gateway: Option<GatewayUrl>,
        custom_meta: serde_json::Value,
    ) -> Result<SendOutcome, SendError> {
        let client = self.client(id)?;
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        let bolt11 = Bolt11Invoice::from_str(&invoice.0)
            .map_err(|e| anyhow::anyhow!("parsing invoice: {e}"))?;
        map_send_result(
            lnv2.send(bolt11, parse_gateway(gateway)?, custom_meta)
                .await,
        )
    }

    /// Block until `op`'s receive leg on `id` reaches a final state (spec §3's 3-state SM
    /// claims the ecash automatically; we just await).
    pub async fn await_receive(
        &self,
        id: &FederationId,
        op: OperationId,
    ) -> anyhow::Result<ReceiveState> {
        let client = self.client(id)?;
        // Guard the typed await against a swapped op-id (a send op handed to the receive
        // await): the lnv2 helper would panic decoding the other leg's cached outcome, or
        // hang on an in-flight op whose state machine never yields a receive state.
        ensure_lnv2_op_kind(&client, op, Lnv2OpKind::Receive).await?;
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        let state = lnv2
            .await_final_receive_operation_state(unbridge_op_id(op))
            .await?;
        Ok(map_receive_state(state))
    }

    /// Block until `op`'s send leg on `id` reaches a final state (the SM self-refunds on
    /// gateway forfeit/expiry, spec §4).
    pub async fn await_send(
        &self,
        id: &FederationId,
        op: OperationId,
    ) -> anyhow::Result<SendState> {
        let client = self.client(id)?;
        // Symmetric guard to `await_receive`: a receive op-id handed to the send await would
        // panic/hang inside the lnv2 helper; fail cleanly on the mismatch instead.
        ensure_lnv2_op_kind(&client, op, Lnv2OpKind::Send).await?;
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        let state = lnv2
            .await_final_send_operation_state(unbridge_op_id(op))
            .await?;
        Ok(map_send_state(state))
    }

    // ---- fee quotes + op-log backfill (spec §6/§9, step 4b glue) -------------------
    //
    // These are the I/O the `FedimintExecutor` needs to size + cap a move and to reattach
    // to in-flight ops after a crash. They are scaffolded here (compile + verified against
    // the pinned lnv2/client source); the executor's live validation lands on a quiet
    // machine. Every fee here is the FEDERATION tx fee OR the gateway fee — combined by the
    // executor's `gross_up`/cap-check (the `*_fee_quote` client APIs exclude the gateway fee).

    /// The FEDERATION receive-tx fee for receiving `amount` into `id` (spec §6.1), in msat.
    /// This is only the on-federation cost (note selection / change / dust); the gateway's
    /// receive fee is quoted separately via [`Self::receive_gateway_fee`].
    pub async fn receive_fee_quote(&self, id: &FederationId, amount: Msat) -> anyhow::Result<Msat> {
        let client = self.client(id)?;
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        let quote = lnv2.receive_fee_quote(Amount::from_msats(amount.0)).await?;
        Ok(Msat(quote.total().get_bitcoin().msats))
    }

    /// The FEDERATION send-tx fee for an outgoing contract of `amount` from `id` (spec §6.1),
    /// in msat. Only the on-federation cost; the gateway's send fee is quoted via
    /// [`Self::send_gateway_fee`]. lnv2 quotes the send fee on the full outgoing-contract value
    /// (`send_fee.add_to(amount)`), so the executor calls this on invoice + gateway-send-fee —
    /// both at the §7 Pay-step cap re-check and to pre-size a fresh evacuation before it mints
    /// the destination invoice.
    pub async fn send_fee_quote_for_amount(
        &self,
        id: &FederationId,
        amount: Msat,
    ) -> anyhow::Result<Msat> {
        let client = self.client(id)?;
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        let quote = lnv2.send_fee_quote(Amount::from_msats(amount.0)).await?;
        Ok(Msat(quote.total().get_bitcoin().msats))
    }

    /// The pinned gateway's RECEIVE fee for `id` as a pure [`GatewayFee`] (spec §6.2), read
    /// from its `routing_info`. Feeds the executor's receive-side `gross_up`.
    pub async fn receive_gateway_fee(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
    ) -> anyhow::Result<GatewayFee> {
        let routing_info = self.routing_info_for(id, gateway).await?;
        Ok(payment_fee_to_gateway_fee(routing_info.receive_fee))
    }

    /// The receive fee when `gateway` explicitly serves `id`. `Ok(None)` is the gateway's
    /// successful "unsupported federation" response; transport/HTTP/decode failures remain
    /// `Err` so route sensing can leave the pair unknown instead of misclassifying it unroutable.
    pub(crate) async fn maybe_receive_gateway_fee(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
    ) -> anyhow::Result<Option<GatewayFee>> {
        Ok(self
            .maybe_routing_info_for(id, gateway)
            .await?
            .map(|info| payment_fee_to_gateway_fee(info.receive_fee)))
    }

    /// The pinned gateway's SEND fee for paying `invoice` from `id` (spec §6.2), read from
    /// its `routing_info` via `send_parameters` (which picks the direct-swap vs lightning-swap
    /// fee by whether the invoice's payee is the gateway). Feeds the send-leg cap re-quote.
    pub async fn send_gateway_fee(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
        invoice: &Invoice,
    ) -> anyhow::Result<GatewayFee> {
        let routing_info = self.routing_info_for(id, gateway).await?;
        let bolt11 = Bolt11Invoice::from_str(&invoice.0)
            .map_err(|e| anyhow::anyhow!("parsing invoice: {e}"))?;
        let (send_fee, _expiration_delta) = routing_info.send_parameters(&bolt11);
        Ok(payment_fee_to_gateway_fee(send_fee))
    }

    /// The gateway SEND fee for the direct-swap route this wallet creates when it mints a
    /// destination invoice through `gateway` and pays that invoice from `id`. Before the invoice
    /// exists, the executor cannot call [`Self::send_gateway_fee`], but lnv2 invoices minted by
    /// that gateway select the gateway's direct-swap `send_fee_minimum`.
    pub async fn direct_swap_send_gateway_fee(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
    ) -> anyhow::Result<GatewayFee> {
        let routing_info = self.routing_info_for(id, gateway).await?;
        Ok(payment_fee_to_gateway_fee(routing_info.send_fee_minimum))
    }

    /// The direct-swap send fee with the same unsupported-vs-failed distinction as
    /// [`Self::maybe_receive_gateway_fee`].
    pub(crate) async fn maybe_direct_swap_send_gateway_fee(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
    ) -> anyhow::Result<Option<GatewayFee>> {
        Ok(self
            .maybe_routing_info_for(id, gateway)
            .await?
            .map(|info| payment_fee_to_gateway_fee(info.send_fee_minimum)))
    }

    /// Validate that `gateway` serves `id` by asking the gateway for this federation's lnv2
    /// `RoutingInfo`. This uses the same pinned-source API path as the fee quote helpers; callers
    /// use it when they need a preflight without yet having an invoice.
    pub async fn validate_gateway(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
    ) -> anyhow::Result<()> {
        self.routing_info_for(id, gateway).await.map(|_| ())
    }

    /// Page `id`'s op-log to EXHAUSTION (spec §5/§9.2) and recover one [`OpArtifact`] per
    /// operation tagged with a move `custom_meta`. This is how a lost/derived `MoveRecord`
    /// is repaired: the op-log is the source of truth, and each op ties an op-id (+ the
    /// receive leg's invoice) back to its `move_id`.
    ///
    /// Paging runs newest-first via `paginate_operations_rev` until a short page ends it — a
    /// single page would miss older ops and risk re-minting/re-paying. `custom_meta` is
    /// decoded FALLIBLY: a non-lnv2 op or a non-move lnv2 op is skipped silently; an op that
    /// looks like a move (`move_id` present) but fails to decode is warn-logged and skipped,
    /// never panicking.
    pub async fn backfill_ops(&self, id: &FederationId) -> anyhow::Result<Vec<OpArtifact>> {
        let client = self.client(id)?;
        let log = client.operation_log();
        let mut last_seen: Option<ChronologicalOperationLogKey> = None;
        let mut artifacts = Vec::new();
        loop {
            let page = log
                .paginate_operations_rev(BACKFILL_PAGE_SIZE, last_seen)
                .await;
            let page_len = page.len();
            if let Some((key, _)) = page.last() {
                last_seen = Some(*key);
            }
            for (key, entry) in page {
                let op_id = bridge_op_id(key.operation_id);
                // Only lnv2 lightning ops can carry our move meta; mint/wallet/ln ops don't.
                let Ok(meta) = entry.try_meta::<LightningOperationMeta>() else {
                    continue;
                };
                match op_artifact_from_meta(op_id, meta) {
                    Ok(Some(artifact)) => artifacts.push(artifact),
                    Ok(None) => {}
                    Err(e) => tracing::warn!(
                        op = %key.operation_id.fmt_full(),
                        error = ?e,
                        "backfill: skipping op with malformed move meta"
                    ),
                }
            }
            // A short (or empty) page is the last: `paginate_operations_rev` returns up to
            // `limit` newest-first, so fewer than `limit` means the log is exhausted.
            if page_len < BACKFILL_PAGE_SIZE {
                break;
            }
        }
        Ok(artifacts)
    }

    /// Read the durable, COMMITTED incoming-contract amount for the receive op `op` on `id`
    /// (spec §15.7), plus the quoted contract amount the executor committed into `custom_meta`.
    /// lnv2's `create_contract_and_fetch_invoice` re-fetches `routing_info` at mint time and
    /// sizes `contract.commitment.amount` with the FRESH gateway fee, so a gateway-fee change
    /// between our quote and the mint is observable ONLY from the committed contract (not from
    /// the invoice amount we requested). The quoted amount in `custom_meta` makes the same check
    /// replayable after a crash between receive commit and the first post-receive cache write.
    /// Reads only the client's local op-log (no network).
    pub async fn receive_contract_amounts(
        &self,
        id: &FederationId,
        op: OperationId,
    ) -> anyhow::Result<(Msat, Option<Msat>)> {
        let client = self.client(id)?;
        let fed_op = unbridge_op_id(op);
        let entry = client
            .operation_log()
            .get_operation(fed_op)
            .await
            .ok_or_else(|| anyhow::anyhow!("no operation found for id {}", fed_op.fmt_full()))?;
        let meta = entry.try_meta::<LightningOperationMeta>().map_err(|e| {
            anyhow::anyhow!(
                "operation {} is not an lnv2 lightning operation: {e}",
                fed_op.fmt_full()
            )
        })?;
        match meta {
            LightningOperationMeta::Receive(receive) => Ok((
                Msat(receive.contract.commitment.amount.msats),
                MoveMeta::receive_contract_quote_from_value(&receive.custom_meta).map_err(|e| {
                    anyhow::anyhow!(
                        "operation {} has malformed receive contract quote metadata: {e}",
                        fed_op.fmt_full()
                    )
                })?,
            )),
            LightningOperationMeta::Send(_) | LightningOperationMeta::LnurlReceive(_) => {
                anyhow::bail!(
                    "operation {} is not a receive operation; cannot read its committed contract",
                    fed_op.fmt_full()
                )
            }
        }
    }

    /// Fetch the pinned gateway's `RoutingInfo` for `id`, erroring if the gateway is
    /// unreachable or does not serve this federation. Shared by the two gateway-fee getters.
    async fn routing_info_for(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
    ) -> anyhow::Result<RoutingInfo> {
        self.maybe_routing_info_for(id, gateway)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "gateway {} does not serve federation {}",
                    gateway.0,
                    id.to_hex()
                )
            })
    }

    /// Direct `POST {gateway}/routing_info` on [`Self::gateway_http`] (see that field for why
    /// the SDK's `GatewayApi` route is bypassed here). Wire-identical to
    /// `RealGatewayConnection::routing_info` at the pin: the request body is the federation id,
    /// the 200 body is `Option<RoutingInfo>`.
    async fn maybe_routing_info_for(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
    ) -> anyhow::Result<Option<RoutingInfo>> {
        let client = self.client(id)?;
        let federation_id = client.federation_id();
        let url = SafeUrl::parse(&gateway.0)
            .map_err(|e| anyhow::anyhow!("invalid gateway url {:?}: {e}", gateway.0))?
            .join("routing_info")
            .map_err(|e| anyhow::anyhow!("joining routing_info onto {:?}: {e}", gateway.0))?;
        let response = self
            .gateway_http
            .post(url.to_unsafe())
            .json(&federation_id)
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!("fetching routing info from gateway {}: {e}", gateway.0)
            })?;
        let status = response.status();
        anyhow::ensure!(
            status == reqwest::StatusCode::OK,
            "gateway {} routing_info returned status {status}",
            gateway.0
        );
        response
            .json::<Option<RoutingInfo>>()
            .await
            .map_err(|e| anyhow::anyhow!("decoding routing info from gateway {}: {e}", gateway.0))
    }

    /// Clone out the open client for `id`, or error if the federation isn't joined/opened.
    /// Cloning the `Arc` under the (sync) map lock keeps the guard from crossing an await
    /// point in the money methods above. `pub(crate)` so the [`crate::probe`] runner can
    /// read structural facts (`config`), a light status, and the op-log off the same handle.
    pub(crate) fn client(&self, id: &FederationId) -> anyhow::Result<ClientHandleArc> {
        self.clients
            .read()
            .expect("client map lock poisoned")
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("federation {} not joined/opened", id.to_hex()))
    }
}

struct RecoveryReservation<'a> {
    active: &'a RwLock<BTreeSet<FederationId>>,
    id: FederationId,
}

impl Drop for RecoveryReservation<'_> {
    fn drop(&mut self) {
        let removed = self
            .active
            .write()
            .expect("active recovery set lock poisoned")
            .remove(&self.id);
        debug_assert!(
            removed,
            "active recovery reservation must remain held until its guard drops"
        );
    }
}

async fn join_deadline<T>(
    deadline: Option<JoinDeadline>,
    future: impl Future<Output = T>,
) -> Result<T, JoinDeadlineElapsed> {
    let Some(deadline) = deadline else {
        return Ok(future.await);
    };
    let Some(remaining) = deadline.remaining() else {
        return Err(JoinDeadlineElapsed);
    };
    runtime::timeout(remaining, future)
        .await
        .map_err(|_elapsed| JoinDeadlineElapsed)
}

/// Refuse a recovery of a federation that still has a durable registry row — open OR
/// registered-but-unopened (D3). Refusing over *registered*, not merely *open*, is the money-safety
/// boundary: a registered-but-unopened fed's surviving journal still holds non-terminal `Pay`/`Move`
/// intents that reconcile re-drives, and recovery hands back a fresh EMPTY oplog — the cross-restart
/// send-dedup authority — so a re-driven `Pay` would fund a SECOND outgoing contract the gateway
/// already settled and holds the preimage for → automatic double-pay. Refusing every registered fed
/// removes this by construction: recovery only ever runs where no journal (and thus no surviving
/// intent) exists. Pure over the registration bit so the refusal is unit-testable without a live
/// journal.
fn ensure_recover_not_registered(is_registered: bool, id: &FederationId) -> anyhow::Result<()> {
    if is_registered {
        anyhow::bail!(
            "federation {} is still registered; if open, recovery would run a second client on \
             one seed; if its partition won't open, that is an incident — do not recover over a \
             surviving journal",
            id.to_hex()
        );
    }
    Ok(())
}

/// Block until every module recovery completes, logging progress on a detached side task for
/// operator visibility. The progress stream is NEVER a completion authority (the SDK may duplicate
/// updates and its recovery task is not cancellation-aware); the pinned
/// [`Client::wait_for_all_recoveries`] — which returns `Err` on a failed module recovery instead of
/// parking forever — is the sole success/failure gate (D4/D5).
async fn wait_for_recoveries_with_progress(client: &Client) -> anyhow::Result<()> {
    let mut progress_stream = Box::pin(client.subscribe_to_recovery_progress());
    let progress_task = runtime::spawn("wallet-recovery-progress", async move {
        while let Some((module_instance_id, progress)) = progress_stream.next().await {
            tracing::info!(
                module_instance_id,
                progress = %progress,
                "multi_client: recovery progress"
            );
        }
    });
    let result = client.wait_for_all_recoveries().await;
    progress_task.abort();
    result
}

/// Invoice expiry (seconds) passed to lnv2 `receive`. Spec §4 fixes this at one hour; the
/// executor may size it per-move in step 4b. `pub(crate)` so the settlement-stall watchdog can
/// exclude receives still within their invoice validity (a legitimately-unpaid open invoice is not
/// a stall) using the SAME expiry the invoice was actually minted with.
pub(crate) const RECEIVE_EXPIRY_SECS: u32 = 3600;

/// Op-log page size for [`MultiClient::backfill_ops`]. Backfill pages to EXHAUSTION (spec
/// §9.2), so this only trades round-trips against per-page memory; it is not a coverage cap.
const BACKFILL_PAGE_SIZE: usize = 100;

/// Bridge fedimint's `PaymentFee { base, parts_per_million }` to our pure [`GatewayFee`]
/// (spec §6.2). `base` is an `Amount`, so its msat value is `base.msats`.
fn payment_fee_to_gateway_fee(fee: PaymentFee) -> GatewayFee {
    GatewayFee {
        base_msat: Msat(fee.base.msats),
        ppm: fee.parts_per_million,
    }
}

/// Recover the [`OpArtifact`] a single lnv2 operation contributes to a move, or `None` when
/// the op is not part of a move (spec §4/§5). The leg is decided by the op meta VARIANT
/// (`Send`/`Receive`), authoritative over the redundant `role` in `custom_meta`; the receive
/// leg carries its invoice, the send leg leaves it `None` (the [`OpArtifact`] contract).
fn op_artifact_from_meta(
    op_id: OperationId,
    meta: LightningOperationMeta,
) -> anyhow::Result<Option<OpArtifact>> {
    let (leg, custom_meta, invoice) = match meta {
        LightningOperationMeta::Send(send) => (Leg::Send, send.custom_meta, None),
        LightningOperationMeta::Receive(receive) => {
            let LightningInvoice::Bolt11(bolt11) = receive.invoice;
            (
                Leg::Receive,
                receive.custom_meta,
                Some(bridge_invoice(&bolt11)),
            )
        }
        // A gateway-minted LNURL receive is not part of our two-leg move protocol.
        LightningOperationMeta::LnurlReceive(_) => return Ok(None),
    };

    // A move op tags `custom_meta` with a `move_id`; anything else (e.g. a bare wallet-cli
    // receive/pay carrying only a `role`) is not part of a move — skip it silently.
    if custom_meta.get("move_id").is_none() {
        return Ok(None);
    }
    // It claims to be a move op: a decode failure now is genuine corruption (spec §9.2) —
    // surface it (the caller warns + skips) rather than silently dropping a real leg.
    let move_meta = MoveMeta::from_value(&custom_meta).ok_or_else(|| {
        anyhow::anyhow!("op has a move_id but its custom_meta is not a valid MoveMeta")
    })?;
    Ok(Some(OpArtifact {
        move_id: move_meta.move_id,
        leg,
        op_id,
        amount: move_meta.amount,
        invoice,
    }))
}

/// The outcome of an lnv2 `send` (see [`MultiClient::pay`]). The dedup variant is an
/// OUTCOME, not an error: the client recognised an existing operation for this invoice and
/// hands back its op-id so the caller re-attaches instead of paying twice (spec §4 — the
/// client is the dedup authority). lnv2 allows ONE attempt per invoice, so the existing op
/// may be in flight OR settled — the awaiter resolves which from the op's own final state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    /// A fresh payment was submitted; carries its new op-id.
    Started(OperationId),
    /// An operation for this invoice already exists (in flight or settled); carries its
    /// existing op-id — attach and await its true terminal.
    AlreadyInFlight(OperationId),
}

/// The outcome of [`MultiClient::join`] (spec §10.2): the federation id, plus whether THIS
/// call performed a fresh join (`true`) or found the federation already registered/open
/// (`false` — the idempotent fast path, or the concurrent-registration window). The ledger
/// recording terminalizes the pre-written `join:` row truthfully on this discriminator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinOutcome {
    pub id: FederationId,
    pub newly_joined: bool,
}

impl JoinOutcome {
    /// The federation was already known: this call opened (not joined) it.
    fn opened(id: FederationId) -> Self {
        Self {
            id,
            newly_joined: false,
        }
    }
}

/// The parsed BOLT11 details a raw `pay` ledger row needs BEFORE the SDK call (§10.1): the
/// invoice amount (`None` for a zero-amount invoice) and its 32-byte payment hash — the durable
/// link reconcile's dedup repair keys on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvoiceDetails {
    pub amount: Option<Msat>,
    pub payment_hash: [u8; 32],
}

/// Parse a BOLT11 invoice into its ledger-relevant [`InvoiceDetails`]. A parse failure is the
/// synchronous-error path (§10.1): the pre-written `Started` row is terminalized `Failed` with
/// this error, so even a malformed invoice leaves a durable history row.
pub fn parse_invoice(invoice: &Invoice) -> anyhow::Result<InvoiceDetails> {
    let bolt11 =
        Bolt11Invoice::from_str(&invoice.0).map_err(|e| anyhow::anyhow!("parsing invoice: {e}"))?;
    Ok(InvoiceDetails {
        amount: bolt11.amount_milli_satoshis().map(Msat),
        payment_hash: bolt11.payment_hash().to_byte_array(),
    })
}

/// The final state of a receive leg (`await_final_receive_operation_state`).
///
/// NOTE: `Claimed` carries no amount. The underlying `FinalReceiveOperationState::Claimed`
/// has none, and reading the claimed value back would mean decoding the operation meta —
/// that belongs to the step-4b op-log work, not these raw primitives. The receiver already
/// knows the requested amount at `receive`-time and reads the settled figure via `balance`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiveState {
    /// The incoming payment was confirmed and the ecash was minted.
    Claimed,
    /// The invoice expired before it was paid.
    Expired,
    /// The receive reached the SDK's `Failure` terminal; carries `RECEIVE_FAILURE_DETAIL`
    /// (crate-private, so it is named here rather than linked).
    Failed(String),
}

/// The final state of a send leg (`await_final_send_operation_state`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendState {
    /// The payment settled; carries the preimage proving the gateway paid the invoice.
    Success(Preimage),
    /// The payment failed and the outgoing contract was refunded to us.
    Refunded,
    /// The send reached the SDK's `Failure` terminal; the detail says which outcomes that
    /// terminal collapses rather than claiming a cause.
    Failed(String),
}

/// Why an lnv2 `send` produced no [`SendOutcome`] (spec §15.4). Split so the executor can tell a
/// DETERMINISTIC rejection from a transport fault. Route rejections remain distinct from immutable
/// invoice rejections so raw pay may adopt a new pre-fund route without changing the frozen move
/// disposition.
#[derive(Debug)]
pub enum SendError {
    /// The invoice itself cannot become payable (expired / wrong currency / missing amount).
    InvoiceRejected(String),
    /// The selected gateway route was rejected. Terminal for a pinned move route, but an
    /// unfunded raw pay may retry after adopting another gateway.
    RouteRejected(String),
    /// A transient funding or transport fault; retry may succeed.
    Transport(anyhow::Error),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::InvoiceRejected(msg) | SendError::RouteRejected(msg) => write!(f, "{msg}"),
            SendError::Transport(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SendError {}

/// A non-`SendPaymentError` failure inside [`MultiClient::pay`] (bad invoice/gateway/module lookup)
/// is a transport-class fault as far as the send is concerned: it never proves the invoice itself
/// is permanently unpayable, so it stays retryable.
impl From<anyhow::Error> for SendError {
    fn from(e: anyhow::Error) -> Self {
        SendError::Transport(e)
    }
}

/// Whether a [`SendPaymentError`] is a DETERMINISTIC rejection of the invoice — re-submitting the
/// SAME BOLT11 can never succeed (verified against `modules/fedimint-lnv2-client/src/lib.rs`'s
/// variants). Route policy, transport/gateway-reachability, and funding faults are excluded (they
/// may clear on retry, and an unfunded raw-pay attach may replace its route). The dedup variants
/// are handled as OUTCOMES before this is consulted.
fn is_invoice_send_rejection(e: &SendPaymentError) -> bool {
    matches!(
        e,
        SendPaymentError::InvoiceMissingAmount
            | SendPaymentError::InvoiceExpired
            | SendPaymentError::WrongCurrency { .. }
    )
}

fn is_route_send_rejection(e: &SendPaymentError) -> bool {
    matches!(
        e,
        SendPaymentError::FederationNotSupported
            | SendPaymentError::GatewayFeeExceedsLimit
            | SendPaymentError::GatewayExpirationExceedsLimit
    )
}

/// Map lnv2 `send`'s result to a [`SendOutcome`] or a classified [`SendError`]. Dedup errors become
/// non-failure outcomes; immutable invoice rejections and changeable route rejections stay
/// distinguishable; every other failure is transport-class. Pure, so this is unit-tested without a
/// live federation.
fn map_send_result(
    result: Result<FedimintOperationId, SendPaymentError>,
) -> Result<SendOutcome, SendError> {
    match result {
        Ok(op) => Ok(SendOutcome::Started(bridge_op_id(op))),
        Err(SendPaymentError::DuplicatePaymentAttempt(op)) => {
            Ok(SendOutcome::AlreadyInFlight(bridge_op_id(op)))
        }
        Err(e) if is_invoice_send_rejection(&e) => Err(SendError::InvoiceRejected(format!(
            "lnv2 send deterministically rejected the invoice: {e}"
        ))),
        Err(e) if is_route_send_rejection(&e) => Err(SendError::RouteRejected(format!(
            "lnv2 send rejected the selected gateway route: {e}"
        ))),
        Err(e) => Err(SendError::Transport(anyhow::anyhow!("lnv2 send: {e}"))),
    }
}

/// The only receive-failure detail this wallet produces. The SDK exposes one terminal for the
/// distinct mint-output failures described in the string, so the operation state cannot say which
/// occurred. The runbook's stranded-move entry is the canonical operator account.
pub(crate) const RECEIVE_FAILURE_DETAIL: &str =
    "receive failed: either the claim transaction was rejected (so this wallet claimed nothing, \
     which does not prove the contract is unclaimed) or it was accepted and note issuance then \
     failed; lnv2 collapses both into one terminal, so which one occurred is not known from the \
     operation state";

fn map_receive_state(state: FinalReceiveOperationState) -> ReceiveState {
    match state {
        FinalReceiveOperationState::Claimed => ReceiveState::Claimed,
        FinalReceiveOperationState::Expired => ReceiveState::Expired,
        FinalReceiveOperationState::Failure => ReceiveState::Failed(RECEIVE_FAILURE_DETAIL.into()),
    }
}

fn map_send_state(state: FinalSendOperationState) -> SendState {
    match state {
        FinalSendOperationState::Success(preimage) => SendState::Success(Preimage(preimage)),
        FinalSendOperationState::Refunded => SendState::Refunded,
        FinalSendOperationState::Failure => SendState::Failed(
            "send failed: either the funding transaction was rejected or the refund did not \
             complete; lnv2 collapses both into one terminal, so which one occurred is not known \
             from the operation state"
                .into(),
        ),
    }
}

// --- reconcile-repair oracle (spec §10.3): live op-log evidence for raw pay/recv rows -------

impl MultiClient {
    /// Page `fed`'s op-log newest-first and return the op-id of the FIRST lnv2 op whose meta
    /// satisfies `pred` (§10.3 repair search; reuses the `backfill_ops` pagination).
    async fn find_op_matching(
        &self,
        fed: &FederationId,
        mut pred: impl FnMut(&LightningOperationMeta) -> bool,
    ) -> anyhow::Result<Option<(OperationId, LightningOperationMeta)>> {
        let client = self.client(fed)?;
        let log = client.operation_log();
        let mut last_seen: Option<ChronologicalOperationLogKey> = None;
        loop {
            let page = log
                .paginate_operations_rev(BACKFILL_PAGE_SIZE, last_seen)
                .await;
            let page_len = page.len();
            if let Some((key, _)) = page.last() {
                last_seen = Some(*key);
            }
            for (key, entry) in page {
                let Ok(meta) = entry.try_meta::<LightningOperationMeta>() else {
                    continue;
                };
                if pred(&meta) {
                    return Ok(Some((bridge_op_id(key.operation_id), meta)));
                }
            }
            if page_len < BACKFILL_PAGE_SIZE {
                break;
            }
        }
        Ok(None)
    }

    /// Recover a raw receive's durable artifact from the operation log after a crash between
    /// lnv2 committing the receive and the journal recording its op id + invoice.
    pub(crate) async fn find_receive_artifact_by_correlation_key(
        &self,
        fed: &FederationId,
        key: &IdempotencyKey,
    ) -> Result<Option<(Invoice, OperationId)>, ExecError> {
        let key = key.0.clone();
        self.find_op_matching(fed, |meta| {
            matches!(meta, LightningOperationMeta::Receive(_))
                && meta_custom(meta)
                    .and_then(|custom| custom.get("correlation_key"))
                    .and_then(|value| value.as_str())
                    == Some(key.as_str())
        })
        .await
        .map_err(oracle_retryable)
        .and_then(|artifact| match artifact {
            Some((operation_id, LightningOperationMeta::Receive(receive))) => {
                let LightningInvoice::Bolt11(invoice) = receive.invoice;
                Ok(Some((bridge_invoice(&invoice), operation_id)))
            }
            Some(_) => Err(ExecError::Permanent(
                "correlation-key lookup returned a non-receive operation".into(),
            )),
            None => Ok(None),
        })
    }

    /// Non-blocking read of `fed_op`'s send-leg terminal state (§10.3): a cached outcome maps to
    /// a terminal; an uncached stream is polled only for updates already available now. A genuinely
    /// in-flight op yields `None`, so reconcile leaves it `Awaiting` for a later pass rather than
    /// blocking on the update stream.
    async fn observe_send_terminal(
        &self,
        client: &ClientHandleArc,
        fed_op: FedimintOperationId,
    ) -> anyhow::Result<Option<RawTerminal>> {
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        Ok(ready_terminal_from_updates(
            lnv2.subscribe_send_operation_state_updates(fed_op).await?,
            send_terminal,
        ))
    }

    /// Non-blocking read of `fed_op`'s receive-leg terminal state (§10.3). See
    /// [`Self::observe_send_terminal`].
    async fn observe_receive_terminal(
        &self,
        client: &ClientHandleArc,
        fed_op: FedimintOperationId,
    ) -> anyhow::Result<Option<RawTerminal>> {
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        Ok(ready_terminal_from_updates(
            lnv2.subscribe_receive_operation_state_updates(fed_op)
                .await?,
            receive_terminal,
        ))
    }
}

#[async_trait]
impl LedgerRepairOracle for MultiClient {
    async fn find_op_by_correlation_key(
        &self,
        fed: FederationId,
        key: &IdempotencyKey,
    ) -> Result<Option<OperationId>, ExecError> {
        let key = key.0.clone();
        self.find_op_matching(&fed, |meta| {
            meta_custom(meta)
                .and_then(|c| c.get("correlation_key"))
                .and_then(|v| v.as_str())
                == Some(key.as_str())
        })
        .await
        .map(|found| found.map(|(operation_id, _)| operation_id))
        .map_err(oracle_retryable)
    }

    async fn find_send_op_by_payment_hash(
        &self,
        fed: FederationId,
        hash: [u8; 32],
    ) -> Result<Option<OperationId>, ExecError> {
        self.find_op_matching(&fed, |meta| match meta {
            LightningOperationMeta::Send(send) => {
                let LightningInvoice::Bolt11(bolt11) = &send.invoice;
                bolt11.payment_hash().to_byte_array() == hash
            }
            _ => false,
        })
        .await
        .map(|found| found.map(|(operation_id, _)| operation_id))
        .map_err(oracle_retryable)
    }

    async fn observe_op(
        &self,
        fed: FederationId,
        op: OperationId,
    ) -> Result<RawOpObservation, ExecError> {
        let client = self.client(&fed).map_err(oracle_retryable)?;
        let fed_op = unbridge_op_id(op);
        let entry = client
            .operation_log()
            .get_operation(fed_op)
            .await
            .ok_or_else(|| {
                ExecError::Retryable(format!("no operation for id {}", fed_op.fmt_full()))
            })?;
        let meta = entry.try_meta::<LightningOperationMeta>().map_err(|e| {
            ExecError::Permanent(format!("op {} is not an lnv2 op: {e}", fed_op.fmt_full()))
        })?;
        match meta {
            LightningOperationMeta::Send(send) => {
                let LightningInvoice::Bolt11(bolt11) = &send.invoice;
                let invoice_amount = bolt11.amount_milli_satoshis().map(Msat);
                let payment_hash = Some(bolt11.payment_hash().to_byte_array());
                let gateway = Some(bridge_gateway_url(send.gateway.clone()));
                // Definitive send fee (§9.3): exact gateway component (contract − invoice) plus
                // the federation send-tx fee quote on the funded contract. The fee is display-only
                // enrichment; the TERMINAL state is what makes repair truthful, so a fee-quote
                // failure (guardians unreachable, spent-down wallet) must NOT abort terminalizing a
                // settled op — degrade the fee to missing (§10.3) instead of leaving the row stuck.
                let contract = send.contract.amount.msats;
                let gateway_component = contract.saturating_sub(invoice_amount.map_or(0, |m| m.0));
                let send_fee_quoted = self
                    .send_fee_quote_for_amount(&fed, Msat(contract))
                    .await
                    .ok()
                    .map(|fed_fee| Msat(gateway_component.saturating_add(fed_fee.0)));
                let terminal = self
                    .observe_send_terminal(&client, fed_op)
                    .await
                    .map_err(oracle_retryable)?;
                Ok(RawOpObservation {
                    terminal,
                    gateway,
                    fees: FeeBreakdown {
                        fee_cap: None,
                        receive_fee: None,
                        send_fee_quoted,
                    },
                    invoice_amount,
                    payment_hash,
                })
            }
            LightningOperationMeta::Receive(receive) => {
                let LightningInvoice::Bolt11(bolt11) = &receive.invoice;
                let amount_invoiced = bolt11.amount_milli_satoshis().map(Msat);
                let gateway = Some(bridge_gateway_url(receive.gateway.clone()));
                // Definitive receive fee (§9.3): exact gateway deduction (invoice − contract)
                // plus the federation claim-fee quote on the post-gateway contract. As on the send
                // leg, the fee is display-only enrichment — a quote failure degrades it to missing
                // rather than aborting terminalization of a settled receive (§10.3).
                let contract = receive.contract.commitment.amount.msats;
                let gateway_deduction = amount_invoiced.map_or(0, |m| m.0).saturating_sub(contract);
                let receive_fee = self
                    .receive_fee_quote(&fed, Msat(contract))
                    .await
                    .ok()
                    .map(|fed_fee| Msat(gateway_deduction.saturating_add(fed_fee.0)));
                let terminal = self
                    .observe_receive_terminal(&client, fed_op)
                    .await
                    .map_err(oracle_retryable)?;
                Ok(RawOpObservation {
                    terminal,
                    gateway,
                    fees: FeeBreakdown {
                        fee_cap: None,
                        receive_fee,
                        send_fee_quoted: None,
                    },
                    invoice_amount: amount_invoiced,
                    payment_hash: None,
                })
            }
            LightningOperationMeta::LnurlReceive(_) => Err(ExecError::Permanent(format!(
                "op {} is an LNURL receive, not a raw move op",
                fed_op.fmt_full()
            ))),
        }
    }
}

/// The `custom_meta` on an lnv2 op's meta (both raw legs carry one; an LNURL receive does not).
fn meta_custom(meta: &LightningOperationMeta) -> Option<&serde_json::Value> {
    match meta {
        LightningOperationMeta::Send(send) => Some(&send.custom_meta),
        LightningOperationMeta::Receive(receive) => Some(&receive.custom_meta),
        LightningOperationMeta::LnurlReceive(_) => None,
    }
}

/// A cached send outcome → a [`RawTerminal`]; a non-terminal streaming state (never actually
/// cached as an outcome) is treated defensively as in-flight (`None`).
fn send_terminal(state: SendOperationState) -> Option<RawTerminal> {
    match state {
        SendOperationState::Success(_) => Some(RawTerminal {
            succeeded: true,
            error: None,
        }),
        SendOperationState::Refunded => Some(RawTerminal {
            succeeded: false,
            error: Some("send refunded".into()),
        }),
        SendOperationState::Failure => Some(RawTerminal {
            succeeded: false,
            error: Some(
                "send failed: either the funding transaction was rejected or the refund did not \
                 complete; lnv2 collapses both into one terminal, so which one occurred is not \
                 known from the operation state"
                    .into(),
            ),
        }),
        SendOperationState::Funding
        | SendOperationState::Funded
        | SendOperationState::Refunding => None,
    }
}

/// A cached receive outcome → a [`RawTerminal`]; a non-terminal streaming state is in-flight.
fn receive_terminal(state: ReceiveOperationState) -> Option<RawTerminal> {
    match state {
        ReceiveOperationState::Claimed => Some(RawTerminal {
            succeeded: true,
            error: None,
        }),
        ReceiveOperationState::Expired => Some(RawTerminal {
            succeeded: false,
            error: Some("receive expired".into()),
        }),
        ReceiveOperationState::Failure => Some(RawTerminal {
            succeeded: false,
            error: Some(RECEIVE_FAILURE_DETAIL.into()),
        }),
        ReceiveOperationState::Pending | ReceiveOperationState::Claiming => None,
    }
}

/// Return a terminal outcome from a cached lnv2 outcome or from updates already queued on an
/// uncached stream. Never waits for a future update: repair is allowed to observe, not subscribe.
fn ready_terminal_from_updates<U>(
    updates: UpdateStreamOrOutcome<U>,
    terminal: impl Fn(U) -> Option<RawTerminal>,
) -> Option<RawTerminal>
where
    U: 'static,
{
    match updates {
        UpdateStreamOrOutcome::Outcome(state) => terminal(state),
        UpdateStreamOrOutcome::UpdateStream(mut stream) => loop {
            match stream.next().now_or_never() {
                Some(Some(state)) => {
                    if let Some(terminal) = terminal(state) {
                        return Some(terminal);
                    }
                }
                Some(None) | None => return None,
            }
        },
    }
}

/// Op-log I/O faults during repair are transient (a later reconcile retries); never terminal.
fn oracle_retryable(e: anyhow::Error) -> ExecError {
    ExecError::Retryable(e.to_string())
}

/// Which lnv2 leg an operation is. `await_final_{receive,send}_operation_state` each dispatch
/// on ONE state-machine variant, so handing the wrong kind of op-id to a typed await is a
/// latent panic (decoding the other leg's cached final outcome) or hang (an in-flight op whose
/// state machine never yields the awaited variant) — [`ensure_lnv2_op_kind`] turns that into a
/// clean error, since the CLI accepts any 32-byte op-id.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lnv2OpKind {
    Send,
    Receive,
}

impl Lnv2OpKind {
    /// The `await-<label>` command / method name this kind belongs to (also the error vocab).
    fn label(self) -> &'static str {
        match self {
            Lnv2OpKind::Send => "send",
            Lnv2OpKind::Receive => "receive",
        }
    }

    /// The kind an lnv2 operation's meta represents. `LnurlReceive` is a receive-side leg
    /// (`await_final_receive_operation_state` handles it), so it maps to `Receive`.
    fn of(meta: &LightningOperationMeta) -> Self {
        match meta {
            LightningOperationMeta::Send(_) => Lnv2OpKind::Send,
            LightningOperationMeta::Receive(_) | LightningOperationMeta::LnurlReceive(_) => {
                Lnv2OpKind::Receive
            }
        }
    }
}

/// Fail unless `op` on `client` is an lnv2 lightning operation of the `expected` kind, so a
/// swapped op-id fails cleanly instead of panicking/hanging inside the typed await (see
/// [`Lnv2OpKind`]). Reads only the client's local op-log (no network); a valid op-id from
/// `receive`/`pay` is always present by the time its await is called.
async fn ensure_lnv2_op_kind(
    client: &ClientHandleArc,
    op: OperationId,
    expected: Lnv2OpKind,
) -> anyhow::Result<()> {
    let fed_op = unbridge_op_id(op);
    let entry = client
        .operation_log()
        .get_operation(fed_op)
        .await
        .ok_or_else(|| anyhow::anyhow!("no operation found for id {}", fed_op.fmt_full()))?;
    let meta = entry.try_meta::<LightningOperationMeta>().map_err(|e| {
        anyhow::anyhow!(
            "operation {} is not an lnv2 lightning operation: {e}",
            fed_op.fmt_full()
        )
    })?;
    let actual = Lnv2OpKind::of(&meta);
    anyhow::ensure!(
        actual == expected,
        "operation {} is a {} operation, not a {} — await it with `await-{}` instead",
        fed_op.fmt_full(),
        actual.label(),
        expected.label(),
        actual.label(),
    );
    Ok(())
}

/// Parse an optional [`GatewayUrl`] into fedimint's `SafeUrl` via the public constructor
/// (`SafeUrl`'s field is private). `None` stays `None`, letting lnv2 auto-select.
fn parse_gateway(gateway: Option<GatewayUrl>) -> anyhow::Result<Option<SafeUrl>> {
    gateway
        .map(|g| {
            SafeUrl::parse(&g.0).map_err(|e| anyhow::anyhow!("invalid gateway url {:?}: {e}", g.0))
        })
        .transpose()
}

fn bridge_gateway_url(url: SafeUrl) -> GatewayUrl {
    GatewayUrl(url.to_string())
}

fn bridge_invoice(invoice: &Bolt11Invoice) -> Invoice {
    Invoice(invoice.to_string())
}

/// Bridge fedimint's `OperationId([u8; 32])` to ours (both are the same 32 bytes, spec §3).
fn bridge_op_id(op: FedimintOperationId) -> OperationId {
    OperationId(op.0)
}

fn unbridge_op_id(op: OperationId) -> FedimintOperationId {
    FedimintOperationId(op.0)
}

/// `[CLIENT_PREFIX_TAG] ++ u32_le(db_prefix)` — exactly 5 bytes (spec §4).
fn client_prefix_bytes(db_prefix: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(5);
    key.push(CLIENT_PREFIX_TAG);
    key.extend_from_slice(&db_prefix.to_le_bytes());
    key
}

/// Bridge fedimint's `FederationId` (a `sha256::Hash`) to ours (`[u8; 32]`, spec §3).
pub(crate) fn bridge_federation_id(id: fedimint_core::config::FederationId) -> FederationId {
    FederationId(id.0.to_byte_array())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt as _;
    use fedimint_core::PeerId;
    // `FromStr` (for `FederationId::from_str` / `Bolt11Invoice::from_str`) comes in via
    // `use super::*` — the module already imports it for `pay`.

    #[test]
    fn client_prefix_is_fixed_length_and_tagged() {
        let prefix = client_prefix_bytes(0);
        assert_eq!(prefix, vec![CLIENT_PREFIX_TAG, 0, 0, 0, 0]);
        assert_eq!(prefix.len(), 5);

        // Little-endian, as spec'd — 1 in the low byte, not the high byte.
        let prefix = client_prefix_bytes(1);
        assert_eq!(prefix, vec![CLIENT_PREFIX_TAG, 1, 0, 0, 0]);

        let prefix = client_prefix_bytes(u32::MAX);
        assert_eq!(prefix, vec![CLIENT_PREFIX_TAG, 0xff, 0xff, 0xff, 0xff]);
    }

    #[test]
    fn client_prefixes_never_alias_the_app_prefix_or_each_other() {
        // The fixed 5-byte shape is what rules out the aliasing the spec warns about
        // (`[0x01,0x00]` vs `[0x01],[0x00,..]`): every client prefix is exactly 5 bytes,
        // so distinct indices always produce distinct, same-length keys.
        let a = client_prefix_bytes(0);
        let b = client_prefix_bytes(1);
        assert_eq!(a.len(), 5);
        assert_eq!(a.len(), b.len());
        assert_ne!(a, b);

        // No client prefix collides with the app partition tag `[0x00]` (a single-byte
        // prefix, so it can never equal any 5-byte client prefix, but the leading tag
        // byte is the load-bearing part of that guarantee).
        const APP_PREFIX_TAG: u8 = 0x00;
        assert_ne!(a[0], APP_PREFIX_TAG);
    }

    #[test]
    fn federation_id_bridge_round_trips() {
        let fedimint_id = fedimint_core::config::FederationId::dummy();
        let ours = bridge_federation_id(fedimint_id);

        assert_eq!(ours.0, fedimint_id.0.to_byte_array());

        // The reverse direction round-trips through the same hex `wallet_core::FederationId`
        // already exposes (`to_hex`), since `sha256::Hash` has no public from-bytes
        // constructor outside its own crate — only `FederationId`'s own `FromStr`
        // (verified in `fedimint-core/src/config.rs`).
        let back = fedimint_core::config::FederationId::from_str(&ours.to_hex())
            .expect("to_hex() always yields 64 valid hex chars");
        assert_eq!(back, fedimint_id);
    }

    #[test]
    fn msat_bridges_to_fedimint_amount_and_back() {
        let ours = Msat(123_456_789);
        let amount = fedimint_core::Amount::from_msats(ours.0);
        assert_eq!(amount.msats, ours.0);
        assert_eq!(Msat(amount.msats), ours);
    }

    #[test]
    fn mnemonic_to_root_secret_is_deterministic_and_seed_dependent() {
        let mnemonic_a = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mnemonic_a_again = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let mnemonic_b = Mnemonic::from_entropy(&[1u8; 16]).expect("valid 12-word entropy");

        let bytes_a: [u8; 32] =
            Bip39RootSecretStrategy::<12>::to_root_secret(&mnemonic_a).to_random_bytes();
        let bytes_a_again: [u8; 32] =
            Bip39RootSecretStrategy::<12>::to_root_secret(&mnemonic_a_again).to_random_bytes();
        let bytes_b: [u8; 32] =
            Bip39RootSecretStrategy::<12>::to_root_secret(&mnemonic_b).to_random_bytes();

        // Same mnemonic -> same root secret (join/open must derive the same client keys
        // across restarts of the same wallet).
        assert_eq!(bytes_a, bytes_a_again);
        // Different mnemonic -> different root secret (two wallets must never collide).
        assert_ne!(bytes_a, bytes_b);
    }

    #[test]
    fn send_result_maps_dedup_errors_to_outcomes_not_failures() {
        let op = FedimintOperationId::new_random();
        // A fresh submission -> Started, carrying the new op-id.
        assert_eq!(
            map_send_result(Ok(op)).expect("Ok maps to an outcome"),
            SendOutcome::Started(OperationId(op.0))
        );
        // The dedup error is an OUTCOME (not a failure), carrying the EXISTING op-id so
        // the caller re-attaches rather than double-paying (in flight or settled — the
        // awaiter resolves which from the op's own final state).
        assert_eq!(
            map_send_result(Err(SendPaymentError::DuplicatePaymentAttempt(op)))
                .expect("DuplicatePaymentAttempt maps to an outcome"),
            SendOutcome::AlreadyInFlight(OperationId(op.0))
        );
        // Any other send error stays a real failure (never a silent success).
        assert!(map_send_result(Err(SendPaymentError::InvoiceExpired)).is_err());
        assert!(map_send_result(Err(SendPaymentError::FederationNotSupported)).is_err());
    }

    #[test]
    fn send_result_classifies_deterministic_rejections_distinctly_from_transport() {
        // §15.4: deterministic invoice-level rejections must classify as `Rejected` so the
        // executor fails terminally instead of re-driving an invoice that can never become valid.
        for err in [
            SendPaymentError::InvoiceMissingAmount,
            SendPaymentError::InvoiceExpired,
            SendPaymentError::WrongCurrency {
                invoice_currency: lightning_invoice::Currency::Bitcoin,
                federation_currency: lightning_invoice::Currency::Regtest,
            },
        ] {
            assert!(
                matches!(
                    map_send_result(Err(err.clone())),
                    Err(SendError::InvoiceRejected(_))
                ),
                "{err:?} must classify as a deterministic Rejected, not Transport"
            );
        }

        // Gateway policy failures are still deterministic rejections for the selected route, but
        // remain distinct so raw pay can retry after adopting another route.
        for err in [
            SendPaymentError::FederationNotSupported,
            SendPaymentError::GatewayFeeExceedsLimit,
            SendPaymentError::GatewayExpirationExceedsLimit,
        ] {
            assert!(
                matches!(
                    map_send_result(Err(err.clone())),
                    Err(SendError::RouteRejected(_))
                ),
                "{err:?} must classify as a route rejection, not Transport"
            );
        }

        // Transport / reachability / funding faults stay `Transport` (retry may succeed).
        for err in [
            SendPaymentError::FailedToConnectToGateway("reset".into()),
            SendPaymentError::FailedToRequestBlockCount("timeout".into()),
            SendPaymentError::FailedToFundPayment("in flight".into()),
        ] {
            assert!(
                matches!(
                    map_send_result(Err(err.clone())),
                    Err(SendError::Transport(_))
                ),
                "{err:?} must stay a retryable Transport fault"
            );
        }
    }

    #[test]
    fn ready_terminal_reads_an_uncached_ready_final_update() {
        let stream: futures::stream::BoxStream<'static, SendOperationState> = Box::pin(
            futures::stream::iter([SendOperationState::Success([7u8; 32])]),
        );

        let terminal =
            ready_terminal_from_updates(UpdateStreamOrOutcome::UpdateStream(stream), send_terminal)
                .expect("ready final update should repair the row");

        assert!(terminal.succeeded);
        assert_eq!(terminal.error, None);
    }

    #[test]
    fn ready_terminal_drains_ready_nonterminal_prefix_without_waiting() {
        let stream: futures::stream::BoxStream<'static, SendOperationState> =
            Box::pin(futures::stream::iter([
                SendOperationState::Funding,
                SendOperationState::Refunded,
            ]));

        let terminal =
            ready_terminal_from_updates(UpdateStreamOrOutcome::UpdateStream(stream), send_terminal)
                .expect("ready terminal after a ready prefix should repair the row");

        assert!(!terminal.succeeded);
        assert_eq!(terminal.error.as_deref(), Some("send refunded"));
    }

    #[test]
    fn ready_terminal_leaves_pending_stream_in_flight() {
        let stream: futures::stream::BoxStream<'static, ReceiveOperationState> =
            Box::pin(futures::stream::pending());

        assert_eq!(
            ready_terminal_from_updates(
                UpdateStreamOrOutcome::UpdateStream(stream),
                receive_terminal,
            ),
            None
        );
    }

    #[test]
    fn lnv2_op_kinds_are_distinct_and_labelled_for_the_cli() {
        // The send/receive await guards compare kinds, so the two must be distinguishable...
        assert_ne!(Lnv2OpKind::Send, Lnv2OpKind::Receive);
        // ...and the labels must match the `await-<label>` CLI subcommands, so the mismatch
        // error tells the operator exactly which await to use instead.
        assert_eq!(Lnv2OpKind::Send.label(), "send");
        assert_eq!(Lnv2OpKind::Receive.label(), "receive");
    }

    #[test]
    fn receive_state_maps_every_final_state() {
        assert_eq!(
            map_receive_state(FinalReceiveOperationState::Claimed),
            ReceiveState::Claimed
        );
        assert_eq!(
            map_receive_state(FinalReceiveOperationState::Expired),
            ReceiveState::Expired
        );
        assert!(matches!(
            map_receive_state(FinalReceiveOperationState::Failure),
            ReceiveState::Failed(_)
        ));
    }

    #[test]
    fn send_state_maps_every_final_state_and_preserves_the_preimage() {
        let preimage = [7u8; 32];
        assert_eq!(
            map_send_state(FinalSendOperationState::Success(preimage)),
            SendState::Success(Preimage(preimage))
        );
        assert_eq!(
            map_send_state(FinalSendOperationState::Refunded),
            SendState::Refunded
        );
        assert!(matches!(
            map_send_state(FinalSendOperationState::Failure),
            SendState::Failed(_)
        ));
    }

    #[test]
    fn op_id_bridge_round_trips() {
        let op = FedimintOperationId::new_random();
        let ours = bridge_op_id(op);
        assert_eq!(ours.0, op.0);
        assert_eq!(unbridge_op_id(ours), op);
    }

    #[test]
    fn gateway_url_bridges_through_safe_url() -> anyhow::Result<()> {
        // A present gateway parses to a SafeUrl and round-trips back to the same GatewayUrl.
        let parsed = parse_gateway(Some(GatewayUrl("http://127.0.0.1:8175/".into())))?;
        let safe = parsed.expect("Some gateway -> Some SafeUrl");
        assert_eq!(bridge_gateway_url(safe).0, "http://127.0.0.1:8175/");
        // No gateway stays None (lnv2 auto-selects).
        assert!(parse_gateway(None)?.is_none());
        // A malformed gateway url is a clean error, not a panic.
        assert!(parse_gateway(Some(GatewayUrl("not a url".into()))).is_err());
        Ok(())
    }

    #[test]
    fn invalid_invoice_string_is_a_clean_error() {
        // `pay` parses the invoice via `Bolt11Invoice::from_str`; garbage must error cleanly
        // (surfaced as an `anyhow` error), not panic.
        assert!(Bolt11Invoice::from_str("not-a-bolt11-invoice").is_err());
    }

    #[tokio::test]
    async fn next_db_prefix_accounts_for_orphaned_client_partitions() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let multi_client = MultiClient::new(db.clone(), journal_db, mnemonic).await;

        let mut orphaned_client_key = client_prefix_bytes(41);
        orphaned_client_key.push(0x2f);

        let mut dbtx = db.begin_transaction().await;
        dbtx.raw_insert_bytes(&orphaned_client_key, b"initialized client row")
            .await
            .expect("mem db insert succeeds");
        dbtx.commit_tx().await;

        // `recover` recovers into exactly this FRESH prefix (D3): one past the orphaned partition.
        assert_eq!(multi_client.next_db_prefix().await.unwrap(), 42);

        // ...and choosing it is purely READ-ONLY — the orphaned partition (which may still hold a
        // funded federation) is left byte-for-byte untouched, never wiped or reused. This is what
        // makes recovery-into-a-fresh-prefix non-destructive.
        let mut dbtx = db.begin_transaction_nc().await;
        assert_eq!(
            dbtx.raw_get_bytes(&orphaned_client_key)
                .await
                .expect("orphan read succeeds")
                .as_deref(),
            Some(&b"initialized client row"[..])
        );
    }

    #[test]
    fn recover_refuses_a_registered_federation() {
        let id = FederationId([7u8; 32]);
        // A federation that still has a durable registry row cannot be recovered — whether it is
        // open OR merely registered-but-unopened, its surviving journal may hold re-drivable intents
        // a fresh recovery oplog would double-pay (D3). An open fed is a strict subset: open implies
        // registered, so this same guard refuses it.
        let refused = ensure_recover_not_registered(true, &id);
        assert!(refused.is_err());
        let message = refused.unwrap_err().to_string();
        assert!(
            message.contains("still registered"),
            "the refusal must be the actionable 'still registered' message, was: {message}"
        );
        // A federation with NO registry row (a fresh host, or a lost journal — the recovery case)
        // proceeds into a fresh prefix.
        assert!(ensure_recover_not_registered(false, &id).is_ok());
    }

    #[tokio::test]
    async fn recover_refuses_a_registered_but_unopened_federation() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let multi_client = MultiClient::new(db, journal_db, mnemonic).await;

        // Registered-but-unopened: a journal registry row survives but no client is open. This is
        // exactly the dangerous state — the surviving journal still holds re-drivable `Pay`/`Move`
        // intents, so recovery's fresh empty oplog would double-pay. Recovery must refuse it, not
        // just an already-open fed.
        let fed_id = fedimint_core::config::FederationId::dummy();
        let id = bridge_federation_id(fed_id);
        let invite = InviteCode::new(
            SafeUrl::parse("https://registered.example").expect("valid url"),
            PeerId::from(0),
            fed_id,
            None,
        );
        let info = FederationInfo {
            invite: invite.to_string(),
            db_prefix: 0,
            joined_at: 0,
        };
        multi_client
            .journal
            .put_federation(&id, &info)
            .await
            .expect("register the federation without opening a client");

        // No client is open for this fed ...
        assert!(!multi_client.has_client(&id));
        // ... yet recovery refuses at the pre-lock registration check, before any network preview
        // or partition write.
        let refused = multi_client
            .recover(invite, &IdempotencyKey("recover:registered".to_string()))
            .await;
        assert!(refused.is_err());
        let message = refused.unwrap_err().to_string();
        assert!(
            message.contains("still registered"),
            "registered-but-unopened recovery must refuse with the 'still registered' message, \
             was: {message}"
        );
    }

    #[tokio::test]
    async fn open_one_skips_a_federation_reserved_for_recovery() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let multi_client = MultiClient::new(db, journal_db, mnemonic).await;

        // A real invite so `open_one` can derive the fed id from the registry row before touching
        // the partition.
        let fed_id = fedimint_core::config::FederationId::dummy();
        let id = bridge_federation_id(fed_id);
        let invite = InviteCode::new(
            SafeUrl::parse("https://reserved.example").expect("valid url"),
            PeerId::from(0),
            fed_id,
            None,
        );
        let info = FederationInfo {
            invite: invite.to_string(),
            db_prefix: 0,
            joined_at: 0,
        };

        let _reservation = multi_client
            .reserve_recovery(id)
            .expect("reserve the fed for an in-flight recovery");

        // The scheduler's `open_all` → `open_one` must NOT open a second handle on the partition the
        // in-flight recovery owns: it identifies the fed from the registry row's invite and refuses
        // BEFORE the open, so `recover` stays the sole writer of that partition's live client.
        let refused = multi_client.open_one(&info).await;
        assert!(refused.is_err());
        assert!(
            refused
                .unwrap_err()
                .to_string()
                .contains("recovery is in progress"),
            "open_one must skip a reserved fed via the recovery reservation, not attempt the open"
        );
        assert!(!multi_client.has_client(&id));
    }

    #[tokio::test]
    async fn recovery_reservation_blocks_same_federation_until_registration_scope_ends() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let multi_client = MultiClient::new(db, journal_db, mnemonic).await;
        let recovering = FederationId([7u8; 32]);
        let unrelated = FederationId([8u8; 32]);

        let reservation = multi_client
            .reserve_recovery(recovering)
            .expect("first recovery reserves its federation");
        assert!(
            multi_client
                .ensure_recovery_not_in_progress(&recovering)
                .is_err(),
            "a concurrent join must not initialize a second client for the recovering federation"
        );
        assert!(
            multi_client.reserve_recovery(recovering).is_err(),
            "a concurrent recovery must not initialize a second client for the same federation"
        );
        assert!(
            multi_client
                .ensure_recovery_not_in_progress(&unrelated)
                .is_ok(),
            "the per-fed reservation must not block unrelated federation work"
        );

        drop(reservation);
        assert!(
            multi_client
                .ensure_recovery_not_in_progress(&recovering)
                .is_ok(),
            "completion or failure releases the process-local reservation"
        );
    }

    #[tokio::test]
    async fn remove_client_partition_deletes_only_the_unfinished_prefix() {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let multi_client = MultiClient::new(db.clone(), journal_db, mnemonic).await;

        let mut target_key = client_prefix_bytes(7);
        target_key.push(0x2f);
        let mut neighbor_key = client_prefix_bytes(8);
        neighbor_key.push(0x2f);
        let app_key = vec![0x00, 0x2f];

        let mut dbtx = db.begin_transaction().await;
        dbtx.raw_insert_bytes(&target_key, b"unfinished client row")
            .await
            .expect("mem db insert succeeds");
        dbtx.raw_insert_bytes(&neighbor_key, b"neighbor client row")
            .await
            .expect("mem db insert succeeds");
        dbtx.raw_insert_bytes(&app_key, b"app row")
            .await
            .expect("mem db insert succeeds");
        dbtx.commit_tx().await;

        multi_client
            .remove_client_partition(7)
            .await
            .expect("remove target prefix");

        let mut dbtx = db.begin_transaction_nc().await;
        assert!(dbtx
            .raw_get_bytes(&target_key)
            .await
            .expect("target read succeeds")
            .is_none());
        assert_eq!(
            dbtx.raw_get_bytes(&neighbor_key)
                .await
                .expect("neighbor read succeeds")
                .as_deref(),
            Some(&b"neighbor client row"[..])
        );
        assert_eq!(
            dbtx.raw_get_bytes(&app_key)
                .await
                .expect("app read succeeds")
                .as_deref(),
            Some(&b"app row"[..])
        );
    }

    #[tokio::test]
    async fn join_before_deadline_bounds_join_lock_wait() -> anyhow::Result<()> {
        let db = MemDatabase::new().into_database();
        let journal_db = MemDatabase::new().into_database();
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let multi_client = MultiClient::new(db, journal_db, mnemonic).await;
        let _guard = multi_client.join_lock.lock().await;
        let invite = InviteCode::new(
            SafeUrl::parse("https://lock-held.example").expect("valid url"),
            PeerId::from(0),
            fedimint_core::config::FederationId::dummy(),
            None,
        );

        let outcome = multi_client
            .join_before_deadline(invite, Duration::from_millis(10))
            .await?;

        assert_eq!(outcome, JoinDeadlineOutcome::DeadlineElapsed);
        Ok(())
    }
}
