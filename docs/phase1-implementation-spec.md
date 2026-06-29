# Phase 1 implementation spec — prove the money path

Detailed design for Phase 1 of the integration phase
([integration-phase-plan.md](./integration-phase-plan.md)). Grounded in the validated
mechanics ([fedimint-mechanics.md](./fedimint-mechanics.md)) and the devimint runbook.
**Status: DRAFT — iterating.** Open decisions are marked `⟦Dn⟧` and collected in §10.

## 0. Goal + scope
Build the smallest thing that proves a cross-federation ecash move works and survives a
crash. Exit gate: a devimint test moves ecash A→B via `apply()`, survives `reconcile()`
(killed mid-move), with no double-pay.

**In scope:** `MultiClient` (join / open / balance / receive / pay), `FedimintExecutor`
(executes the `Move` action), `MoveRecord` coordination + `SqliteJournal`, the resume loop,
the devimint test harness.

**Out of scope (Phase 2/3, but types are shaped now):** scorer/allocator wiring, discovery
(Nostr/Observer), the orchestrator tick + triggers, executing `DirectInflow`/`Evacuate`,
the scorer's gateway probing, UI. We *define* the full `Action`/balance types but Phase 1
only *executes* `Move`.

## 1. Crate layout
```
wallet-core/        (exists; PURE, no fedimint dep)  scorer, allocator, executor(traits+logic), types
wallet-fedimint/    (NEW; depends on fedimint-client + wallet-core)
    multi_client.rs    MultiClient: the per-fed fedimint_client::Client registry
    executor.rs        FedimintExecutor: impl wallet_core::Executor over MultiClient
    journal.rs         SqliteJournal: impl wallet_core::Journal + the MoveRecord store
    move_protocol.rs   MoveRecord + the pure phase/next-step logic (testable without fedimint)
    runtime.rs         wiring: open-all + reconcile on startup
```
Rationale: `wallet-core` stays pure and mock-tested; all fedimint/async/I-O lives in
`wallet-fedimint`. Only `wallet-fedimint` needs devimint to test. ⟦D1⟧ confirm crate name
(`wallet-fedimint` vs `wallet-runtime`).

## 2. Async boundary  ⟦D2⟧
`fedimint-client` is async (tokio); `wallet-core::Executor::perform` is currently sync.
Two options:
- **(A, recommended) Adapter blocks.** Keep `wallet-core` 100% sync (its tested
  apply/reconcile/drive untouched). `FedimintExecutor::perform` holds a `tokio::runtime::Handle`
  and `handle.block_on(async { ... })` the fedimint calls. The orchestrator owns the runtime.
- (B) Make `Executor::perform` (and apply/reconcile) `async` via `async-trait`. Cleaner call
  graph, but churns the tested pure executor and makes the pure core depend on an async shape.
Recommend **A**: the pure core's value is being sync + deterministic; don't infect it.

## 3. Types

### 3.1 Identity — real `FederationId`  ⟦D3⟧
`wallet-core` currently has `FederationId(pub u32)` (placeholder). Real id is
`fedimint_core::config::FederationId` (32-byte hash). Options:
- (A, recommended) `wallet-fedimint` uses the real `fedimint_core::config::FederationId`
  everywhere; the pure `wallet-core` snapshot/decisions keep a stable opaque id (a newtype
  over `[u8;32]`) and the adapter maps 1:1. Keep wallet-core dependency-free by making its
  `FederationId` a `[u8;32]` newtype (small change to the placeholder, no fedimint dep).
- (B) wallet-core depends on fedimint-core for the id type (breaks purity).
Recommend **A** — change wallet-core `FederationId(u32)` → `FederationId([u8;32])` (T14).

### 3.2 Action (wallet-core, T12) — define all, execute Move
```rust
pub enum Action {
    // executable money-moves (Phase 1 executes only Move):
    Move { from: FederationId, to: FederationId, amount_msat: u64, fee_cap_msat: u64, occurrence: u64 },
    DirectInflow { to: FederationId },                 // Phase 2
    Evacuate { from: FederationId, to: FederationId, amount_msat: u64, fee_cap_msat: u64, occurrence: u64 }, // Phase 2
    // advisory policy (NOT executed; emitted to the app):
    RefuseInflow,  Cap { fed: FederationId, limit_msat: u64 },
}
```

### 3.3 Balance (T13) — structured, msat
```rust
pub struct FederationBalance { pub spendable_msat: u64, pub in_flight_msat: u64, pub claimable_msat: u64, pub reserved_fee_msat: u64 }
```
Phase 1 populates `spendable_msat` from `client.get_balance()` (the only number the SDK
gives, §6 mechanics); the rest are computed by the app from MoveRecords (`in_flight`,
`reserved_fee`) and pending receives (`claimable`). Phase 1 may leave the latter three at 0
until Phase 2 needs them, but the struct ships now (no v2 rewrite).

### 3.4 MoveRecord (wallet-fedimint) — the coordination state
```rust
pub struct MoveRecord {
    pub idempotency_key: String,   // == the wallet-core Intent key; includes `occurrence`
    pub from: FederationId, pub to: FederationId,
    pub amount_msat: u64, pub fee_cap_msat: u64,
    pub gateway: String,                 // the shared gateway URL (pinned)
    pub invoice: Option<String>,         // persisted when B.receive returns (NOT idempotent)
    pub recv_op_id: Option<[u8;32]>,     // B side
    pub send_op_id: Option<[u8;32]>,     // A side
    pub phase: MovePhase,                // Created|Invoiced|Sending|Settled|Refunded|Failed (derivable, stored for clarity)
    pub outcome: Option<String>,
}
```
`move_protocol.rs::next_step(&MoveRecord) -> MoveStep` is pure (testable without fedimint),
returning `CreateInvoice | Pay | AwaitBoth | Done | Failed`. RESUME, never restart: if
`invoice.is_some()` never re-`CreateInvoice`; if `send_op_id.is_some()` never re-`Pay`.

## 4. MultiClient (wallet-fedimint)
```rust
pub struct MultiClient { clients: BTreeMap<FederationId, ClientHandleArc>, db: Database, mnemonic: Mnemonic }
impl MultiClient {
    async fn join(&mut self, invite: InviteCode) -> Result<FederationId>;       // build+join, persist invite + db-prefix
    async fn open_all(&mut self, feds: &[(FederationId, DbPrefix, InviteCode)]) -> Result<()>; // startup
    fn client(&self, id: &FederationId) -> Option<&ClientHandleArc>;
    async fn balance(&self, id: &FederationId) -> Result<u64>;                   // get_balance().msats
    async fn receive(&self, id: &FederationId, amount_msat: u64, gw: &str) -> Result<(String /*invoice*/, [u8;32] /*op*/)>;
    async fn pay(&self, id: &FederationId, invoice: &str, gw: &str) -> Result<[u8;32] /*op*/>;
    async fn await_send(&self, id: &FederationId, op: [u8;32]) -> Result<SendOutcome>;   // Success(preimage)|Refunded|Failed
    async fn await_receive(&self, id: &FederationId, op: [u8;32]) -> Result<RecvOutcome>; // Claimed|Expired
}
```
Concrete fedimint calls (validated / from harbor): build via `Client::builder(db).with_module(..)`,
`get_default_client_secret(root, fed_id, 0)`, `preview(connectors, invite).join/.open`;
receive/pay via `client.get_first_module::<LightningClientModule /*lnv2*/>().receive(amount, 3600,
desc, Some(gw), Null)` and `.send(invoice, Some(gw), Null)`; balance via `client.get_balance()`;
await via `subscribe_*_operation_state_updates(op)`. **Gateway is explicit in Phase 1**
(`--gateway` required, per validation) ⟦D4⟧.

### Storage ⟦D5⟧
- (A, recommended, Fedi pattern) One `Database` (RocksDB on device), each client opened on a
  **key-prefix slice** (`db.with_prefix(n)`); prefix 0 = our app tables, 1.. = each fed. One
  fsync domain. Our `SqliteJournal`... but RocksDB + SQLite is two engines. Reconcile: either
  (A1) RocksDB for fedimint + a separate SQLite for our app state (two files), or (A2) one
  store. Lean A1: RocksDB per-fed-prefix for fedimint clients (native, fast), SQLite for our
  coordination/journal (queryable `status`).
- (B, harbor pattern) wrap fedimint's mem-db into our SQLite as a BLOB. Simpler one-file, but
  full-state write amplification per commit.
Recommend **A1** (RocksDB for clients, SQLite for our journal).

## 5. FedimintExecutor (impl wallet_core::Executor)
```rust
impl Executor for FedimintExecutor {
    fn perform(&mut self, intent: &Intent) -> Result<(), ExecError> {
        self.rt.block_on(async {
            match &intent.action {
                Action::Move { from, to, amount_msat, fee_cap_msat, .. } => {
                    let mut rec = self.journal.get_move(&intent.idempotency_key)
                        .unwrap_or_else(|| MoveRecord::new(intent));   // resume or create
                    loop { match next_step(&rec) {
                        CreateInvoice => { let (inv, op) = self.mc.receive(to, *amount_msat, &rec.gateway).await?;
                                           rec.invoice = Some(inv); rec.recv_op_id = Some(op); self.journal.put_move(&rec)?; }
                        Pay           => { let op = self.mc.pay(from, rec.invoice.as_ref().unwrap(), &rec.gateway).await?;
                                           rec.send_op_id = Some(op); self.journal.put_move(&rec)?; }
                        AwaitBoth     => { let s = self.mc.await_send(from, rec.send_op_id.unwrap()).await?;
                                           let r = self.mc.await_receive(to, rec.recv_op_id.unwrap()).await?;
                                           rec.phase = settle(s, r); self.journal.put_move(&rec)?; }
                        Done   => return Ok(()),
                        Failed => return Err(ExecError::Permanent),
                    }}
                }
                _ => Err(ExecError::Unsupported),   // Phase 1 executes only Move
            }
        })
    }
}
```
Idempotency: keyed by `intent.idempotency_key` (carries `occurrence`, T10). The MoveRecord
persists op-ids BEFORE awaiting, so a crash mid-`perform` resumes via `next_step` and the
client's own dedup (re-`pay` → `InvoiceAlreadyPaid`, validated). **Persist-before-act** is
the invariant: write the MoveRecord row before/with each fedimint call.

## 6. SqliteJournal + tables
```sql
CREATE TABLE intents(   idempotency_key TEXT PRIMARY KEY, action_json TEXT, status TEXT, updated_at INTEGER);
CREATE TABLE moves(     idempotency_key TEXT PRIMARY KEY, from_fed BLOB, to_fed BLOB, amount_msat INTEGER,
                        fee_cap_msat INTEGER, gateway TEXT, invoice TEXT, recv_op_id BLOB, send_op_id BLOB,
                        phase TEXT, outcome TEXT);
CREATE TABLE federations(fed_id BLOB PRIMARY KEY, invite_code TEXT, db_prefix INTEGER, joined_at INTEGER);
```
`SqliteJournal` implements `wallet_core::Journal` (`upsert/get/set_status/pending/failed`)
over `intents`, plus `get_move/put_move` over `moves`, plus the `federations` registry
(backed up per ADR-0003). rusqlite (sync) — fits the sync executor. ⟦D6⟧ rusqlite vs sqlx.

## 7. Resume loop (runtime.rs)
On startup: read `federations` → `MultiClient::open_all(...)` (each client's executor
self-resumes its own state machines). Then `wallet_core::reconcile(journal, executor)`:
re-drive `pending`+`failed` intents; the `FedimintExecutor` re-attaches to the persisted
op-ids via the MoveRecord and `next_step` (no restart). This is the crash-safety path the
gate test exercises.

## 8. Test plan
- **Pure unit (no fedimint, every `cargo test`):** `move_protocol::next_step` resume-from-every-phase
  (no double-invoice / no double-pay); `SqliteJournal` contract tests against a tempfile;
  `wallet_core::apply/reconcile` with the existing `MockExecutor`.
- **devimint e2e (`--features devimint-e2e`, gated):** the gate — `apply(Move A→B)` moves
  ecash and `reconcile()` after a kill-at-each-step yields exactly-once (assert balances +
  no double-pay); plus the misbehaving-gateway double (T4). Bootstrap the fed once per
  session (runbook §6). ⟦D7⟧ two-fed harness: needs fed-1 join + a gateway connected to both.

## 9. Build order within Phase 1
1. `move_protocol.rs` + its pure tests (no deps) — the resume state machine.
2. `SqliteJournal` + tests (tempfile).
3. `MultiClient` (join/open/balance/receive/pay) — devimint smoke (single fed: receive→send).
4. `FedimintExecutor` wiring `apply` → real ecash — devimint single-fed self-move.
5. Two-fed harness + the crash/reconcile gate test.

## 10. Open decisions (iterate here)
- ⟦D1⟧ crate name: `wallet-fedimint` (rec) vs `wallet-runtime`.
- ⟦D2⟧ async boundary: adapter `block_on` keeping core sync (rec) vs make core async.
- ⟦D3⟧ `FederationId`: change wallet-core to `[u8;32]` newtype + map (rec) vs fedimint dep.
- ⟦D4⟧ gateway: explicit `--gateway` in Phase 1 (rec, matches validation) vs auto-select now.
- ⟦D5⟧ storage: RocksDB(clients)+SQLite(journal) two files (rec) vs single-DB BLOB (harbor).
- ⟦D6⟧ sqlite driver: rusqlite sync (rec, fits sync executor) vs sqlx async.
- ⟦D7⟧ scope of the gate: single-fed self-move first, then two-fed (rec) vs two-fed immediately.
