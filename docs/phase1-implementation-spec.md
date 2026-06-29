# Phase 1 implementation spec — prove the money path

Detailed design for Phase 1 of the integration phase
([integration-phase-plan.md](./integration-phase-plan.md)). Grounded in the validated
mechanics ([fedimint-mechanics.md](./fedimint-mechanics.md)) and the devimint runbook.
**Status: iterating.** Decisions settled in §10.

## 0. Goal + scope
Smallest thing that proves a cross-federation ecash move works and survives a crash. Exit
gate: a devimint test moves ecash A→B via `apply()`, survives `reconcile()` (killed
mid-move), with no double-pay.

**In scope:** async refactor of the `wallet-core` executor; `MultiClient` (join / open /
balance / receive / pay); `FedimintExecutor` (executes `Move`); `MoveRecord` + the
fedimint-`Database`-backed journal; the resume loop; the devimint harness.

**Out of scope (Phase 2/3; types shaped now, not executed):** scorer/allocator wiring,
discovery, the orchestrator tick, executing `DirectInflow`/`Evacuate`, UI.

## 1. Crate layout
```
wallet-core/        (exists; no fedimint/network/db dep; async traits + sync pure logic)
    scorer.rs, allocator.rs        pure sync fns (decide/score) — unchanged
    executor.rs                    Executor + Journal traits NOW ASYNC; apply/reconcile async
    types.rs                       newtypes (see §3)
wallet-fedimint/    (NEW; depends on fedimint-client + wallet-core)
    multi_client.rs    MultiClient over fedimint_client::Client per fed
    executor.rs        FedimintExecutor: impl async wallet_core::Executor
    journal.rs         FedimintJournal: impl async wallet_core::Journal over fedimint Database (prefix 0)
    move_protocol.rs   MoveRecord + pure sync next_step (testable without fedimint)
    runtime.rs         open-all + reconcile on startup
```
`wallet-core` stays free of fedimint/network/db; all I/O lives behind its async traits and
is implemented in `wallet-fedimint`.

## 2. Async model (settled: 100% async, never `block_on`)
- The `wallet-core` **`Executor` and `Journal` traits become `async`** (via `async-trait`),
  and **`apply`/`reconcile`/`drive` become `async fn`** that `.await` them. `MockExecutor`
  /`MemJournal` become async impls; their tests run under `#[tokio::test]`. This is a
  refactor of already-tested code (build-order step 0) — behavior identical, signatures async.
- **Pure CPU functions stay sync:** `allocator::decide`, `scorer::score`,
  `move_protocol::next_step`. They never block on I/O, so they are async-compatible as-is
  (you just call them inside async code). Do NOT make them async for its own sake.
- `MultiClient`, `FedimintExecutor`, `FedimintJournal`, `runtime` are all async. **No
  `block_on`, no `spawn_blocking` for our own code** — the storage engine (fedimint
  `Database`) is itself async, so nothing forces a sync bridge.

## 3. Types — newtypes throughout ("when in doubt, a newtype")
```rust
// wallet-core/types.rs
pub struct FederationId(pub [u8; 32]);   // was u32 placeholder (T14)
pub struct Msat(pub u64);                // amounts AND fees, msat granularity
pub struct Occurrence(pub u64);          // T10 epoch
pub struct IdempotencyKey(pub String);   // the intent key; carries occurrence
// wallet-fedimint
pub struct OperationId(pub [u8; 32]);    // mirror fedimint OperationId
pub struct Preimage(pub [u8; 32]);
pub struct DbPrefix(pub u64);
pub struct GatewayUrl(pub String);       // SafeUrl under the hood
pub struct Invoice(pub String);          // Bolt11 string
```

### 3.1 Action (wallet-core, T12) — define all, execute only `Move` in Phase 1
```rust
pub enum Action {
    Move { from: FederationId, to: FederationId, amount: Msat, fee_cap: Msat, occurrence: Occurrence },
    DirectInflow { to: FederationId },                                            // Phase 2
    Evacuate { from: FederationId, to: FederationId, amount: Msat, fee_cap: Msat, occurrence: Occurrence }, // Phase 2
    RefuseInflow,  Cap { fed: FederationId, limit: Msat },                        // advisory; not executed
}
```

### 3.2 Balance (T13) — structured
```rust
pub struct FederationBalance { pub spendable: Msat, pub in_flight: Msat, pub claimable: Msat, pub reserved_fee: Msat }
```
Phase 1 fills `spendable` from `client.get_balance()`; the rest computed from MoveRecords
/pending receives (may stay 0 until Phase 2). Ships now to avoid a v2 rewrite.

### 3.3 MoveRecord (wallet-fedimint) — the coordination state
```rust
pub struct MoveRecord {
    pub key: IdempotencyKey,             // == the wallet-core Intent key
    pub from: FederationId, pub to: FederationId,
    pub amount: Msat, pub fee_cap: Msat,
    pub gateway: GatewayUrl,             // shared gateway, pinned
    pub invoice: Option<Invoice>,        // persisted when B.receive returns (NOT idempotent)
    pub recv_op: Option<OperationId>,    // B side
    pub send_op: Option<OperationId>,    // A side
    pub phase: MovePhase,                // Created|Invoiced|Sending|Settled|Refunded|Failed
    pub outcome: Option<String>,
}
pub enum MoveStep { CreateInvoice, Pay, AwaitBoth, Done, Failed }
pub fn next_step(rec: &MoveRecord) -> MoveStep;   // PURE sync; RESUME not restart
```
Invariant: `invoice.is_some()` ⇒ never `CreateInvoice`; `send_op.is_some()` ⇒ never `Pay`.

## 4. MultiClient (wallet-fedimint) — all async
```rust
pub struct MultiClient { clients: BTreeMap<FederationId, ClientHandleArc>, db: fedimint_core::db::Database, mnemonic: Mnemonic }
impl MultiClient {
    async fn join(&mut self, invite: InviteCode) -> Result<FederationId>;     // build+join at next DbPrefix; record in journal
    async fn open_all(&mut self, feds: &[(FederationId, DbPrefix)]) -> Result<()>;
    fn client(&self, id: &FederationId) -> Option<&ClientHandleArc>;
    async fn balance(&self, id: &FederationId) -> Result<Msat>;               // get_balance()
    async fn receive(&self, id: &FederationId, amount: Msat, gw: &GatewayUrl) -> Result<(Invoice, OperationId)>;
    async fn pay(&self, id: &FederationId, invoice: &Invoice, gw: &GatewayUrl) -> Result<OperationId>;
    async fn await_send(&self, id: &FederationId, op: OperationId) -> Result<SendOutcome>;   // Success(Preimage)|Refunded|Failed
    async fn await_receive(&self, id: &FederationId, op: OperationId) -> Result<RecvOutcome>; // Claimed|Expired
}
```
Concrete fedimint calls (validated / from harbor): `Client::builder(db).with_module(..)`,
`get_default_client_secret(root, fed_id, 0)`, `preview(connectors, invite).join/.open`;
`client.get_first_module::<LightningClientModule>().receive(amount, 3600, desc, Some(gw),
Null)` / `.send(invoice, Some(gw), Null)`; `client.get_balance()`;
`subscribe_*_operation_state_updates(op)`. **Gateway explicit in Phase 1** (validation showed
the LDK gateway isn't auto-vetted; supply `--gateway`/`Some(gw)`).

### Storage (settled: RocksDB, single async Database)
One fedimint `Database` (RocksDB on device). Each client opens on a **key-prefix slice**
`db.with_prefix(prefix)`; `DbPrefix(0)` = our app state (§6), `DbPrefix(1..)` = each fed
(Fedi's pattern). One async engine, one fsync domain, no sync driver. The per-fed secret is
derived `get_default_client_secret(root, fed_id, device_index=0)`.

## 5. FedimintExecutor (impl async `wallet_core::Executor`)
```rust
#[async_trait]
impl Executor for FedimintExecutor {
    async fn perform(&self, intent: &Intent) -> Result<(), ExecError> {
        match &intent.action {
            Action::Move { from, to, amount, fee_cap, .. } => {
                let mut rec = self.journal.get_move(&intent.key).await?
                    .unwrap_or_else(|| MoveRecord::new(intent));            // resume or create
                loop { match next_step(&rec) {
                    CreateInvoice => { let (inv, op) = self.mc.receive(to, *amount, &rec.gateway).await?;
                                       rec.invoice = Some(inv); rec.recv_op = Some(op); self.journal.put_move(&rec).await?; }
                    Pay           => { let op = self.mc.pay(from, rec.invoice.as_ref().unwrap(), &rec.gateway).await?;
                                       rec.send_op = Some(op); self.journal.put_move(&rec).await?; }
                    AwaitBoth     => { let s = self.mc.await_send(from, rec.send_op.unwrap()).await?;
                                       let r = self.mc.await_receive(to, rec.recv_op.unwrap()).await?;
                                       rec.phase = settle(s, r); self.journal.put_move(&rec).await?; }
                    Done   => return Ok(()),
                    Failed => return Err(ExecError::Permanent),
                }}
            }
            _ => Err(ExecError::Unsupported),    // Phase 1 executes only Move
        }
    }
}
```
**Persist-before-act:** write the MoveRecord (op-id/invoice) before/with each fedimint call,
so a crash mid-`perform` resumes via `next_step` + the client's own dedup (re-`pay` →
`InvoiceAlreadyPaid`, validated). Idempotency keyed by `IdempotencyKey` (carries `Occurrence`).

## 6. Journal storage — fedimint `Database`, prefix 0 (async)
`FedimintJournal` implements the async `wallet_core::Journal` over `db.with_prefix(0)`. No
SQL, no SQLite — typed `Encodable` rows (Fedi stores app data this way). Key spaces:
```
IntentKey(IdempotencyKey)      -> Intent { action, status }           // the wallet-core intent log
MoveKey(IdempotencyKey)        -> MoveRecord                          // §3.3
FederationKey(FederationId)    -> FederationInfo { invite, db_prefix, joined_at }   // backed up, ADR-0003
PendingIndexKey(status, key)   -> ()                                  // index for `pending()`/`failed()` scans
```
`pending()`/`failed()` are prefix scans over `PendingIndexKey`. All methods `async`.

## 7. Resume loop (runtime.rs, async)
Startup: read `FederationKey` rows → `MultiClient::open_all(...)` (each client's executor
self-resumes its own state machines). Then `wallet_core::reconcile(journal, executor).await`:
re-drive `pending`+`failed` intents; `FedimintExecutor` re-attaches to persisted op-ids via
the MoveRecord + `next_step` (no restart). This is the crash-safety path the gate exercises.

## 8. Test plan
- **Pure unit (no fedimint, `cargo test`):** `move_protocol::next_step` resume-from-every-phase
  (no double-invoice/double-pay); the async executor `apply`/`reconcile` with async
  `MockExecutor`/`MemJournal` under `#[tokio::test]`.
- **devimint e2e (`--features devimint-e2e`):** the gate — `apply(Move A→B)` moves ecash;
  `reconcile()` after a kill-at-each-step is exactly-once (assert balances + no double-pay);
  misbehaving-gateway double (T4). Fed bootstrapped once per session (runbook §6).

## 9. Build order
0. **Foundational `wallet-core` refactor (behavior-preserving), land first:**
   (a) async `Executor`/`Journal` traits + async `apply`/`reconcile`/`drive` + async
   `MockExecutor`/`MemJournal` + `#[tokio::test]`; (b) newtypes in `types.rs` —
   `FederationId([u8;32])`, `Msat`, `Occurrence`, `IdempotencyKey` — which ripples into the
   allocator/scorer signatures + their golden-test fixtures. Everything below depends on it.
1. `move_protocol.rs` + pure tests (the resume state machine).
2. `FedimintJournal` over a fedimint `Database` + tests (in-memory `Database`).
3. `MultiClient` (join/open/balance/receive/pay) + devimint single-fed smoke (receive→send).
4. `FedimintExecutor` wiring `apply` → real ecash + devimint single-fed self-move.
5. Two-fed harness + the crash/reconcile gate test.

## 10. Decisions (settled)
- ⟦D1⟧ crate name **`wallet-fedimint`**.
- ⟦D2⟧ **100% async**, `Executor`/`Journal`/`apply`/`reconcile` async; pure fns stay sync;
  **no `block_on`/`spawn_blocking`** for our code.
- ⟦D3⟧ **newtypes throughout**; `FederationId([u8;32])` replaces the `u32` placeholder.
- ⟦D4⟧ gateway **explicit** in Phase 1 (matches validation).
- ⟦D5⟧ storage: **single fedimint `Database` (RocksDB)**, prefix 0 = app/journal, 1.. =
  clients. No SQLite.
- ⟦D6⟧ ~~sqlite driver~~ **moot** — no SQLite; journal is the async fedimint `Database`.
- ⟦D7⟧ gate: **single-fed self-move first, then two-fed**.
