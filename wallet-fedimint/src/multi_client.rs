//! `MultiClient` — one `fedimint_client::Client` per joined federation, all sharing a
//! single async fedimint `Database` (spec §1/§4). Owns the client LIFECYCLE for Phase 1
//! step 3: join / open_all / balance / federations. Receive/pay/await/backfill (the
//! money-moving calls) land with the `FedimintExecutor` in step 4.

use crate::journal::{FederationInfo, FedimintJournal};
use fedimint_bip39::{Bip39RootSecretStrategy, Mnemonic};
use fedimint_client::secret::RootSecretStrategy as _;
use fedimint_client::{Client, ClientBuilder, ClientHandleArc, RootSecret};
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::db::{Database, IDatabaseTransactionOpsCore};
use fedimint_core::invite_code::InviteCode;
use fedimint_core::BitcoinHash as _;
use futures::lock::Mutex;
use futures::StreamExt;
use std::collections::BTreeMap;
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
    use std::str::FromStr as _;

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
