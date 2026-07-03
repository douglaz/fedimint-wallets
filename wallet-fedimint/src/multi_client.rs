//! `MultiClient` — one `fedimint_client::Client` per joined federation, all sharing a
//! single async fedimint `Database` (spec §1/§4). Owns the client LIFECYCLE (step 3:
//! join / open_all / balance / federations) and the raw lnv2 money PRIMITIVES (step 4a:
//! gateways / receive / pay / await_receive / await_send). The `FedimintExecutor` — fee
//! gross-up, `MoveRecord`/`Action` wiring, op-log backfill — lands on top in step 4b.

use crate::fee::GatewayFee;
use crate::journal::{FederationInfo, FedimintJournal};
use crate::move_protocol::{Leg, MoveMeta, OpArtifact};
use crate::types::{GatewayUrl, Invoice, OperationId, Preimage};
use fedimint_bip39::{Bip39RootSecretStrategy, Mnemonic};
use fedimint_client::db::ChronologicalOperationLogKey;
use fedimint_client::secret::RootSecretStrategy as _;
use fedimint_client::{Client, ClientBuilder, ClientHandleArc, RootSecret};
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::core::OperationId as FedimintOperationId;
use fedimint_core::db::{Database, IDatabaseTransactionOpsCore};
use fedimint_core::invite_code::InviteCode;
use fedimint_core::util::SafeUrl;
use fedimint_core::Amount;
use fedimint_core::BitcoinHash as _;
use fedimint_lnv2_client::common::gateway_api::{PaymentFee, RoutingInfo};
use fedimint_lnv2_client::common::{Bolt11InvoiceDescription, LightningInvoice};
use fedimint_lnv2_client::{
    FinalReceiveOperationState, FinalSendOperationState, LightningClientModule,
    LightningOperationMeta, SendPaymentError,
};
use futures::lock::Mutex;
use futures::StreamExt;
use lightning_invoice::Bolt11Invoice;
use std::collections::BTreeMap;
use std::str::FromStr as _;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use wallet_core::{FederationId, Msat};

/// Tag byte for a per-federation client partition (spec §4 "Storage"): client `i` lives
/// at `[CLIENT_PREFIX_TAG] ++ u32_le(db_prefix)`, exactly 5 bytes. Fixed-length is
/// load-bearing: a variable-length prefix could alias (`[0x01,0x00]` vs `[0x01],[0x00,..]`).
const CLIENT_PREFIX_TAG: u8 = 0x01;

/// One fedimint client per joined federation, sharing ONE async `Database`: app state
/// (the journal) lives at `[0x00]`, each client `i` at `[0x01] ++ u32_le(db_prefix)`.
/// Concrete type, no trait (ADR-0021) — `MultiClient` is the one production impl.
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
}

impl MultiClient {
    /// Derive the root secret once from `mnemonic` (`StandardDoubleDerive` — the
    /// per-federation mix-in happens INSIDE the fedimint builder on join/open; callers
    /// must never pre-derive it, per the builder's own contract) and share `db` for the
    /// journal + every per-federation client.
    pub async fn new(db: Database, mnemonic: Mnemonic) -> Self {
        let root_secret = RootSecret::StandardDoubleDerive(
            Bip39RootSecretStrategy::<12>::to_root_secret(&mnemonic),
        );
        let connectors = ConnectorRegistry::build_from_client_defaults()
            .bind()
            .await
            .expect("binding the default client connectors performs no I/O and cannot fail");
        Self {
            journal: FedimintJournal::new(db.clone()),
            db,
            connectors,
            root_secret,
            clients: RwLock::new(BTreeMap::new()),
            join_lock: Mutex::new(()),
        }
    }

    /// Join `invite`'s federation, assigning it the next `db_prefix` and persisting a
    /// [`FederationInfo`] row. Idempotent: a federation already joined (in-memory, or
    /// recorded in the journal from a previous run) is opened instead of re-joined.
    pub async fn join(&self, invite: InviteCode) -> anyhow::Result<FederationId> {
        let id = bridge_federation_id(invite.federation_id());

        if self.has_client(&id) {
            return Ok(id);
        }

        let _join_guard = self.join_lock.lock().await;

        if self.has_client(&id) {
            return Ok(id);
        }
        if let Some(info) = self
            .journal
            .get_federation(&id)
            .await
            .map_err(|e| anyhow::anyhow!("reading federation registry: {e:?}"))?
        {
            return self.open_one(&info).await;
        }

        let db_prefix = self.next_db_prefix().await?;
        let client: ClientHandleArc = self
            .client_builder()
            .await?
            .preview(self.connectors.clone(), &invite)
            .await?
            .join(self.client_db(db_prefix), self.root_secret.clone())
            .await
            .map(Arc::new)?;

        let joined_id = bridge_federation_id(client.federation_id());
        anyhow::ensure!(
            joined_id == id,
            "joined federation id {} did not match invite id {}",
            joined_id.to_hex(),
            id.to_hex()
        );
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
        Ok(joined_id)
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

    fn has_client(&self, id: &FederationId) -> bool {
        self.clients
            .read()
            .expect("client map lock poisoned")
            .contains_key(id)
    }

    /// Open one already-joined federation's client from its registry row and insert it
    /// into the map. `open` reads the federation config already stored in the client's
    /// own db partition, so `info.invite` is not needed here (it exists for
    /// display/backup, per [`FederationInfo`]'s docs).
    async fn open_one(&self, info: &FederationInfo) -> anyhow::Result<FederationId> {
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
        self.clients
            .write()
            .expect("client map lock poisoned")
            .insert(id, client);
        Ok(id)
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
    /// (deterministic op-id): re-paying an in-flight or already-settled invoice returns
    /// [`SendOutcome::AlreadyInFlight`]/[`SendOutcome::AlreadyPaid`] carrying the ORIGINAL
    /// op-id — never an `Err`, never a double-pay (spec §4). `custom_meta` is committed into
    /// the operation meta.
    pub async fn pay(
        &self,
        id: &FederationId,
        invoice: Invoice,
        gateway: Option<GatewayUrl>,
        custom_meta: serde_json::Value,
    ) -> anyhow::Result<SendOutcome> {
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

    async fn maybe_routing_info_for(
        &self,
        id: &FederationId,
        gateway: &GatewayUrl,
    ) -> anyhow::Result<Option<RoutingInfo>> {
        let client = self.client(id)?;
        let lnv2 = client.get_first_module::<LightningClientModule>()?;
        let url = SafeUrl::parse(&gateway.0)
            .map_err(|e| anyhow::anyhow!("invalid gateway url {:?}: {e}", gateway.0))?;
        lnv2.routing_info(&url)
            .await
            .map_err(|e| anyhow::anyhow!("fetching routing info from gateway {}: {e}", gateway.0))
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

/// Invoice expiry (seconds) passed to lnv2 `receive`. Spec §4 fixes this at one hour; the
/// executor may size it per-move in step 4b.
const RECEIVE_EXPIRY_SECS: u32 = 3600;

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

/// The outcome of an lnv2 `send` (see [`MultiClient::pay`]). The dedup variants are
/// OUTCOMES, not errors: the client recognised an existing operation for this invoice and
/// hands back its op-id so the caller re-attaches instead of paying twice (spec §4 — the
/// client is the dedup authority).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    /// A fresh payment was submitted; carries its new op-id.
    Started(OperationId),
    /// A payment for this invoice is already in progress; carries its existing op-id.
    AlreadyInFlight(OperationId),
    /// This invoice was already paid; carries the settled op-id.
    AlreadyPaid(OperationId),
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
    /// The receive failed (programming error or malicious federation).
    Failed(String),
}

/// The final state of a send leg (`await_final_send_operation_state`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendState {
    /// The payment settled; carries the preimage proving the gateway paid the invoice.
    Success(Preimage),
    /// The payment failed and the outgoing contract was refunded to us.
    Refunded,
    /// The send failed (programming error or malicious federation).
    Failed(String),
}

/// Map lnv2 `send`'s result to a [`SendOutcome`]: the two dedup errors become non-failure
/// outcomes carrying the existing op-id; every other error is a real failure. Pure, so the
/// dedup mapping is unit-tested without a live federation.
fn map_send_result(
    result: Result<FedimintOperationId, SendPaymentError>,
) -> anyhow::Result<SendOutcome> {
    match result {
        Ok(op) => Ok(SendOutcome::Started(bridge_op_id(op))),
        Err(SendPaymentError::PaymentInProgress(op)) => {
            Ok(SendOutcome::AlreadyInFlight(bridge_op_id(op)))
        }
        Err(SendPaymentError::InvoiceAlreadyPaid(op)) => {
            Ok(SendOutcome::AlreadyPaid(bridge_op_id(op)))
        }
        Err(e) => Err(anyhow::anyhow!("lnv2 send: {e}")),
    }
}

fn map_receive_state(state: FinalReceiveOperationState) -> ReceiveState {
    match state {
        FinalReceiveOperationState::Claimed => ReceiveState::Claimed,
        FinalReceiveOperationState::Expired => ReceiveState::Expired,
        FinalReceiveOperationState::Failure => ReceiveState::Failed(
            "receive failed (programming error or malicious federation)".into(),
        ),
    }
}

fn map_send_state(state: FinalSendOperationState) -> SendState {
    match state {
        FinalSendOperationState::Success(preimage) => SendState::Success(Preimage(preimage)),
        FinalSendOperationState::Refunded => SendState::Refunded,
        FinalSendOperationState::Failure => {
            SendState::Failed("send failed (programming error or malicious federation)".into())
        }
    }
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
fn bridge_federation_id(id: fedimint_core::config::FederationId) -> FederationId {
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
        // The two dedup errors are OUTCOMES (not failures), each carrying the EXISTING
        // op-id so the caller re-attaches rather than double-paying.
        assert_eq!(
            map_send_result(Err(SendPaymentError::PaymentInProgress(op)))
                .expect("PaymentInProgress maps to an outcome"),
            SendOutcome::AlreadyInFlight(OperationId(op.0))
        );
        assert_eq!(
            map_send_result(Err(SendPaymentError::InvoiceAlreadyPaid(op)))
                .expect("InvoiceAlreadyPaid maps to an outcome"),
            SendOutcome::AlreadyPaid(OperationId(op.0))
        );
        // Any other send error stays a real failure (never a silent success).
        assert!(map_send_result(Err(SendPaymentError::InvoiceExpired)).is_err());
        assert!(map_send_result(Err(SendPaymentError::FederationNotSupported)).is_err());
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
        let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
        let multi_client = MultiClient::new(db.clone(), mnemonic).await;

        let mut orphaned_client_key = client_prefix_bytes(41);
        orphaned_client_key.push(0x2f);

        let mut dbtx = db.begin_transaction().await;
        dbtx.raw_insert_bytes(&orphaned_client_key, b"initialized client row")
            .await
            .expect("mem db insert succeeds");
        dbtx.commit_tx().await;

        assert_eq!(multi_client.next_db_prefix().await.unwrap(), 42);
    }
}
