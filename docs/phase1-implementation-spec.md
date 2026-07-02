# Phase 1 implementation spec — prove the money path

Detailed design for Phase 1 of the integration phase
([integration-phase-plan.md](./integration-phase-plan.md)). Grounded in the validated
mechanics ([fedimint-mechanics.md](./fedimint-mechanics.md)) and the devimint runbook.
API claims verified against `~/p/fedimint` (branch `docs/custodial-receive-spec`).
**Status: hardened through 12 codex passes (gpt-5.5 / xhigh).** Decisions settled in §11.

## Executive summary
Phase 1 turns the built, tested **pure decision core** (scorer / allocator / executor) into
code that actually **moves ecash across federations and survives crashes**. It adds one crate,
`wallet-fedimint`, holding all fedimint I/O; `wallet-core` stays pure (no fedimint/network/db).

- **What gets built:** a `MultiClient` (one `fedimint_client::Client` per federation, one async
  RocksDB with per-fed key prefixes), a `FedimintExecutor` that performs **two actions** —
  `Move` (cross-fed transfer A→B) and `DirectInflow` (route a receive to a chosen fed, the
  cheap primary lever) — plus the op-log-backed journal, the resume loop, and **`wallet-cli`**
  (a first-class, permanent headless frontend over the engine — ADR-0023 — that is also the
  devimint integration-test driver). The Android app is the *other* frontend, built later.
- **The money path (validated live):** a cross-fed move is **two ordinary fedimint operations**
  — `B.receive` then `A.send` through a shared gateway's internal swap. The fedimint clients
  self-resume their own state machines, so we don't re-implement them; we own only a thin
  coordination record linking the two op-ids.
- **Crash safety (the hard part):** the **fedimint op-log is the source of truth** — we embed a
  `move_id` in each operation's `custom_meta`, and on startup **backfill** our records from the
  op-log (including `Executing`/`Awaiting` intents) **before any retry**, so a crash never
  double-pays or double-invoices. The send dedup is the client's own (deterministic op-id →
  `InvoiceAlreadyPaid`). `DirectInflow`'s external-payer claim is finalized by a `recv_op`
  subscription, not by blocking `apply`.
- **Fees:** `fee_cap` bounds the **total** move cost; the destination credit is grossed up via a
  **fixed point** (gateway fee on the invoice, federation fee on the post-gateway contract); the
  send leg is re-quoted right before paying.
- **Settled decisions:** 100% async (no `block_on`); newtypes throughout (`FederationId([u8;32])`,
  `Msat`, …); one RocksDB (no SQLite); gateway pinned in the durable intent; concrete-over-traits.
- **Exit gate:** a devimint test moves ecash A→B via `apply()` and survives `reconcile()` killed
  at every step (no double-pay, no second payable invoice), plus a `DirectInflow` that nets
  exactly the target amount.
- **Build order:** (0) behavior-preserving `wallet-core` async + newtype refactor → (1) the pure
  move state machine → (2) the journal over fedimint's `Database` → (3) `MultiClient` +
  `wallet-cli` join/balance → (4) `FedimintExecutor` + fees + `wallet-cli` move/reconcile →
  (5) two-fed crash/reconcile gate via real `wallet-cli` process kills. `wallet-cli` grows with
  the engine, not bolted on at the end.

## 0. Goal + scope
Smallest thing that proves a cross-federation ecash move works and survives a crash. Exit
gate: a devimint test moves ecash A→B via `apply()`, survives `reconcile()` (killed at every
step), with **no double-pay and no second committed/payable invoice**. (A kill in the
pre-commit window — gateway minted the invoice before the client persisted the receive op —
leaves only an unpaid orphan that expires, not a double-credit; see §5. The gate is scoped
to post-commit crashes, not that window.) **Also gated:** a `DirectInflow` generates + claims
a receive on the chosen federation — the cheap primary lever (ADR-0022), proven alongside the
expensive swap, not deferred.

**In scope:** async refactor of the `wallet-core` executor + identity newtypes; `MultiClient`
(join/open/balance/receive/pay); `FedimintExecutor` (executes `Move` AND `DirectInflow`); the
op-log-backed durability model + the `Database`-backed journal; the resume/backfill loop; the
devimint harness.

**Out of scope (Phase 2/3; types shaped now, not executed):** scorer/allocator wiring,
discovery, the orchestrator tick, executing `Evacuate`, UI.

## 1. Crate layout
The wallet is an **engine** with **two co-equal frontends** over its public API — `wallet-cli`
(headless, permanent) and, later, the Android app (Slint UI). Neither reaches around the engine
API; the engine assumes no UI (ADR-0023).
```
wallet-core/        (no fedimint/network/db dep; async I/O traits + sync pure logic)
wallet-fedimint/    (the ENGINE; depends on fedimint-client + wallet-core)
    multi_client.rs  MultiClient over fedimint_client::Client per fed
    executor.rs      FedimintExecutor: impl async wallet_core::Executor
    journal.rs       FedimintJournal: impl async wallet_core::Journal over fedimint Database (prefix [0x00])
    move_protocol.rs MoveRecord + pure sync next_step + op-log backfill mapping
    runtime.rs       open-all + op-log backfill + reconcile on startup
wallet-cli/         (FIRST-CLASS frontend, kept forever — ADR-0023; a clap bin over the engine:
                     join / balance / receive / pay / move / reconcile / list-feds. The
                     dogfooding surface AND the devimint integration-test driver.)
# (later) the Android frontend — Slint UI + JNI — is the OTHER frontend over the same engine.
```
(There is NO `move_sm.rs`; the per-leg state-machine model was retired, ADR-0022.)

## 2. Async model (settled: 100% async, never `block_on`/`spawn_blocking` for our code)
- `wallet-core` **`Executor`/`Journal` traits become `#[async_trait]`**, and
  **`apply`/`reconcile`/`drive` become `async fn`**. Pure CPU fns stay sync
  (`allocator::decide`, `scorer::score`, `move_protocol::next_step`).
- **`Executor::perform(&self, ..) -> Result<PerformOutcome, ExecError>`** (was `&mut self`,
  unit Ok): the executor holds `Arc<MultiClient>` + `Arc<FedimintJournal>`; shared, `Send +
  Sync`. Mock impls use interior mutability (`Mutex<…>`). `Journal` methods are `&self` too.
- **`PerformOutcome { Done, Awaiting }`** + a new **`IntentStatus::Awaiting`**: `Done` → mark
  the intent `Done`. `Awaiting` (a `DirectInflow` whose EXTERNAL payer hasn't paid yet) → mark
  `Awaiting`, which `reconcile` does **NOT** re-drive — it is owned by the `recv_op`
  subscription, which marks the intent `Done` on `Claimed` / `Failed` on expiry (§9.5). **The
  runtime starts that subscription the moment the intent transitions to `Awaiting`** (not only
  on boot, where §9.3 rehydrates it) — else a payment that arrives before any restart is never
  observed. This prevents an unpaid inflow from being marked complete or stuck forever.
- `ExecError` gets explicit variants (was unit-like): `Retryable(String)` (leave the intent
  `Pending` so the next `reconcile` retries it), `Permanent(String)` (mark `Failed` —
  **terminal; NOT auto-re-driven**), `Unsupported` (`Evacuate` or advisory actions only —
  **`DirectInflow` and `Move` ARE executed in Phase 1**; → `Failed`).
  `drive` branches on these. **This changes the executor's prior reconcile semantics**
  (which re-drove `pending`+`failed`): `reconcile` now re-drives **`pending()` only**; a
  `Failed`/`Permanent` intent stays failed until an explicit manual retry resets it to
  `Pending` (so fee-over-cap / unsupported don't re-run every launch). **`apply` must also
  treat `Failed` as terminal** — a repeated allocator tick with the same idempotency key must
  NOT refresh a `Failed` intent back to `Pending` (only a manual retry does), or fee-over-cap/
  unsupported failures would re-run every tick.
- The storage engine (fedimint `Database`) is itself async, so nothing forces a sync bridge.
- **Single-writer guard (codex):** `lnv2 receive()`/`send()` are NOT idempotent, and `wallet-cli`
  is multi-process (ADR-0023), so two `perform`s for one intent must never run concurrently.
  Two-layer guard: (a) RocksDB takes an exclusive per-directory lock — two `wallet-cli` processes
  cannot open the same DB dir at once (the OS serializes them); (b) WITHIN a process, the
  `Journal` transitions `Pending → Executing` via a **compare-and-swap** (`set_status_if`, old→new
  in one dbtx) and `drive` calls `perform` ONLY if it won the CAS — so `apply` racing `reconcile`
  on the same intent yields exactly one `perform`. `MemDatabase` (tests) has no dir lock, so the
  CAS is what the concurrency test exercises.

## 3. Identity + types — newtypes throughout
```rust
// wallet-core/types.rs
pub struct FederationId(pub [u8; 32]);   // bridges fedimint_core::config::FederationId(sha256::Hash)
pub struct GuardianId(pub Vec<u8>);      // canonical guardian pubkey bytes ONLY (NOT api-url, NOT a local peer index) — code is authoritative
pub struct Msat(pub u64);                // amounts AND fees
pub struct Occurrence(pub u64);          // T10 epoch
pub struct IdempotencyKey(pub String);   // the intent key; carries occurrence + hex(FederationId)s
// wallet-fedimint
pub struct OperationId(pub [u8; 32]);  pub struct Preimage(pub [u8; 32]);
pub struct GatewayUrl(pub String);  // parse to fedimint SafeUrl via SafeUrl::parse(&self.0)
pub struct Invoice(pub String);     // parse to Bolt11Invoice via FromStr
```
**Identity ripple (bigger than just `FederationId`, P2):** `FederationId` becomes the 32-byte
consensus hash (local `u32` peer indices are meaningless across feds). (This originally also added
`FederationStatus.guardians: Vec<GuardianId>` for ADR-0010 guardian-overlap/independence — since
DROPPED as unfeasible in fedimint; that field + `GuardianId` were removed.) The allocator's
idempotency-key formatting (currently `u32`) formats `hex(FederationId)` + `Occurrence`.

### 3.1 Action (T12) — define all; Phase 1 executes `Move` + `DirectInflow`
```rust
pub enum Action {
    Move { from: FederationId, to: FederationId, amount: Msat, fee_cap: Msat, gateway: GatewayUrl, occurrence: Occurrence },
    DirectInflow { to: FederationId, amount: Msat, fee_cap: Msat, gateway: GatewayUrl, occurrence: Occurrence }, // Phase 1: receive on `to`
    Evacuate { from: FederationId, to: FederationId, amount: Msat, fee_cap: Msat, gateway: GatewayUrl, occurrence: Occurrence }, // Phase 2
    RefuseInflow,  Cap { fed: FederationId, limit: Msat },                                      // advisory; not executed
}
```
The **`gateway` is part of the durable `Move`/`Evacuate` intent** (picked once: Phase 2 by the
allocator from the gateways shared by both feds, Phase 1 from the bundled config), so a
resumed move reads `rec.gateway` from the intent and never reselects a different or
non-shared gateway after a crash.

### 3.2 Balance (T13) — structured; Phase 1 fills `spendable`
```rust
pub struct FederationBalance { pub spendable: Msat, pub in_flight: Msat, pub claimable: Msat, pub reserved_fee: Msat }
```
`spendable` from `client.get_balance_for_btc()`; the rest computed from MoveRecords (may be 0
until Phase 2). Ships now (no v2 rewrite).

### 3.3 MoveRecord — a DERIVED index, not the source of truth (see §5)
```rust
pub struct MoveRecord {
    pub key: IdempotencyKey,             // == Intent key == the move_id embedded in op custom_meta
    pub from: Option<FederationId>, pub to: FederationId, pub amount: Msat, pub fee_cap: Msat,
    pub gateway: GatewayUrl,
    pub send_required: bool,              // Move = true; DirectInflow = false (receive-only, from = None)
    pub invoice: Option<Invoice>, pub recv_op: Option<OperationId>, pub send_op: Option<OperationId>,
    pub phase: MovePhase, pub outcome: Option<String>,
}
pub enum MoveStep { CreateInvoice, Pay, AwaitSettle, Done, Failed }
pub fn next_step(rec: &MoveRecord) -> MoveStep;   // PURE; RESUME not restart; respects send_required
```
`next_step`: `invoice` None → `CreateInvoice`; else if `send_required && send_op` None → `Pay`;
else if not settled → `AwaitSettle`; else `Done`/`Failed`. So a `DirectInflow`
(`send_required=false`) goes `CreateInvoice → AwaitSettle → Done` and never hits `Pay`.
Invariants: `invoice.is_some()` ⇒ never `CreateInvoice`; `send_op.is_some()` ⇒ never `Pay`.

## 4. MultiClient (wallet-fedimint) — all async. Real fedimint API.
```rust
impl MultiClient {
    async fn join(&mut self, invite: InviteCode) -> Result<FederationId>;
    async fn open_all(&mut self, feds: &[(FederationId, DbPrefix)]) -> Result<()>;
    fn client(&self, id: &FederationId) -> Option<&ClientHandleArc>;
    async fn balance(&self, id: &FederationId) -> Result<Msat>;
    async fn receive(&self, id, amount: Msat, gw: &GatewayUrl, move_id: &IdempotencyKey) -> Result<(Invoice, OperationId)>;
    async fn pay(&self, id, invoice: &Invoice, gw: &GatewayUrl, move_id: &IdempotencyKey) -> Result<SendStart>;
    async fn await_send(&self, id, op: OperationId) -> Result<SendOutcome>;     // Success(Preimage)|Refunded|Failed
    async fn await_receive(&self, id, op: OperationId) -> Result<RecvOutcome>;  // Claimed|Expired
    async fn backfill_ops(&self, id: &FederationId) -> Result<Vec<OpArtifact>>;  // page op-log, read custom_meta
}
pub enum SendStart { Started(OperationId), AlreadyInFlight(OperationId), AlreadyPaid(OperationId) }
pub enum Leg { Send, Receive }
pub struct OpArtifact { pub move_id: IdempotencyKey, pub leg: Leg, pub op_id: OperationId, pub invoice: Option<Invoice> }
```
**Backfill returns per-op artifacts, NOT full MoveRecords:** one client's op-log only sees
**one leg** of a move, and the move's params (`fee_cap`, `amount`, `gateway`) live in the
journaled `Intent`, not the op meta. So `reconcile` ASSEMBLES the MoveRecord by merging, per
`move_id`: the `Intent` (authoritative for params) + the `OpArtifact`s from each client (recv
leg from B, send leg from A; authoritative for op-ids/invoice) + any cached `MoveRecord`. The
merge never overwrites a leg with a blank or drops `fee_cap`.
**Verified fedimint calls (this branch):**
- Build: `Client::builder().await?` → `.with_module(..)` → `ClientPreview::join(db, secret)`
  (first) or `ClientBuilder::open(connectors, db, secret)` (existing). NOT `Client::builder(db)`.
- Secret: pass **`RootSecret::StandardDoubleDerive(global_root_secret)`** to `join`/`open` —
  the builder derives the per-fed client secret INTERNALLY (mixing in `federation_id` via
  `get_default_client_secret`/`to_inner`). **Apps must NOT pre-derive it** (the builder docs
  say so); a raw derived secret won't match the signature and re-deriving it would reopen the
  wrong funds. (`RootSecret::Custom(prederived)` only if you truly pre-derive.)
- Balance: `client.get_balance_for_btc()` (or `get_balance_for_unit(AmountUnit::BITCOIN)`). NOT `get_balance()`.
- lnv2: `client.get_first_module::<LightningClientModule>()?` then
  `.receive(Amount::from_msats(n), 3600, Bolt11InvoiceDescription::Direct(String::new()), Some(SafeUrl::parse(&gw.0)?), custom_meta_json)`
  → `(Bolt11Invoice, OperationId)`; `.send(Bolt11Invoice::from_str(&inv.0)?, Some(SafeUrl::parse(&gw.0)?), custom_meta_json)` → `OperationId`,
  (use public constructors — `SafeUrl`'s field is private, `Direct` takes a `String`),
  mapping `Err(PaymentInProgress(op))`/`Err(InvoiceAlreadyPaid(op))` → `AlreadyInFlight/AlreadyPaid` (NOT failure).
- `custom_meta_json` (the `Value` arg) carries `{move_id, occurrence, role: "send"|"receive", from, to}` — this
  is how a lost MoveRecord is repaired (§5).
- Await: `.await_final_send_operation_state(op)` / `.await_final_receive_operation_state(op)`.

### Storage (settled: one async fedimint `Database`, RocksDB)
`Database::with_prefix(Vec<u8>)` is the real API. Byte layout uses **FIXED-LENGTH prefixes**
(load-bearing — variable-length prefixes could alias, e.g. `[0x01,0x00]` vs `[0x01],[0x00,..]`):
**app state = `[0x00]`** (1 byte); **client `i` = `[0x01] ++ u32_le(db_prefix)`** (exactly 5
bytes). `db_prefix: u32` is the client's stable per-fed index; the `[0x01] ++ u32_le` encoding
is the load-bearing contract (never reuse an index for a different fed). `DbPrefix` newtype
wraps the `Vec<u8>`. One async engine, one fsync domain, no sync driver.

## 5. Durability model — op-log is the source of truth (REWRITTEN per codex P0)
The public lnv2 `receive()`/`send()` commit the fedimint operation in the **client's** DB
before returning; our `MoveRecord` (prefix `[0x00]`) is a **separate** commit. They are NOT
atomic, and there is no public API to enlist app rows in the operation's dbtx. So we do NOT
rely on "persist-before-act". Instead:

- **The fedimint operation log is the durable truth.** Every `receive`/`send` is started with
  `custom_meta = {move_id, occurrence, role, from, to}`, committed atomically with the
  operation by fedimint. lnv2 op meta also stores the `gateway`/`contract`/`invoice`.
- **The `MoveRecord` is a derived cache/index** of (the Intent) + (the op-log entries tagged
  with this `move_id`). It is best-effort to keep current; it is never the only copy.
- **Startup BACKFILL precedes any retry:** `reconcile` first pages each client's op-log
  (`paginate_operations_rev`), reads `custom_meta` → per-op `OpArtifact`s keyed by `move_id`,
  and ASSEMBLES each `MoveRecord` = journaled `Intent` (params) + merged op artifacts (op-ids
  /invoice, one leg per client) BEFORE `next_step` can issue anything. Params never come from
  the op-log; op-ids/invoice never come from the Intent.
- **Crash-window behavior, made explicit:**
  - Crash after `receive()` commits, before MoveRecord write → backfill finds the receive op
    by `move_id` ⇒ no second invoice.
  - Crash after `send()` commits, before MoveRecord write → backfill finds the send op; even
    if missed, a re-`send` returns `AlreadyInFlight/AlreadyPaid(op)` (deterministic op-id) ⇒
    no double-pay (as long as the client DB survives; restore-from-seed mid-move is the one
    residual hazard, bounded by the v1 balance cap, ADR-0018).
  - The only true orphan: crash after the gateway mints B's invoice but before the receive op
    commits — the invoice expires unpaid; the Intent record tells us the move was intended, so
    we surface/retry cleanly. Bounded to one move.
  - **Backfill requires the client DB to survive (a process crash).** A device loss /
    restore-from-seed has NO op-log or send-dedup state to scan, so backfill cannot repair it
    — a resent invoice could double-pay. That is the bounded restore hazard: mitigate by
    backing up in-flight move state (ADR-0003) so we detect/avoid the resend, else accept the
    bound from the v1 balance cap (ADR-0018). Backfill is for crashes, not device loss.

## 6. Fee policy (NEW per codex P1) — `fee_cap` must be enforced by US
`send()` does NOT enforce our `fee_cap`; it only enforces lnv2's high built-in cap (100 sat +
1.5% send). `fee_cap` bounds the **total cost of moving `amount`**, so the preflight must sum
BOTH legs, before the irreversible `Pay`:
The total fee has TWO sources per leg — combine both (the `*_fee_quote` APIs do NOT include
the gateway fee):
1. **Federation tx fee** via the client APIs `receive_fee_quote(amount)` (B) / `send_fee_quote(..)`
   (A) — these account for note selection/change/dust (config constants under-quote), but quote
   ONLY the on-federation tx fee.
2. **Gateway fee** SEPARATELY from the pinned gateway's `routing_info`: `receive_fee` (B side)
   and `send_parameters(&invoice)`'s send fee (A side).
3. `receive_quote = recv_tx_fee + recv_gateway_fee`; `send_quote = send_tx_fee + send_gateway_fee`;
   `total = send_quote + receive_quote`. If `total > fee_cap` → `ExecError::Permanent("fee over cap")`,
   abort. (`quote()` in §7 returns these sums.)
**`amount` is the NET credit the destination must end up with.** BOTH fee parts scale with the
GROSS invoice amount — the gateway `receive_fee` has a ppm, AND `receive_fee_quote` (the
federation tx fee) depends on the received/gross amount — so the gross-up is a **fixed point**
evaluated on the right base, not on `amount`. The gateway takes its fee FIRST (invoice →
on-federation `contract_amount`), then the federation fee applies to the contract, so:
`contract_amount = invoice_amount − recv_gateway_fee(invoice_amount)`, and find `invoice_amount`
s.t. `contract_amount − receive_fee_quote(contract_amount) = amount`. **Quote the gateway fee on
`invoice_amount` and the federation fee on `contract_amount` (never on the net `amount`)**;
iterate to convergence. A funds `invoice_amount + send_quote`; B nets exactly `amount`.
- **Receive side** (the invoice size + cap-relevant receive cost) is computed once **at the
  `CreateInvoice` step** and the invoice amount is then **fixed** (never re-quoted on resume).
- **Send side**: the gateway can change its send fee after the invoice exists, and lnv2
  `send()` re-fetches routing info at send time, so **re-quote the send leg immediately before
  each `Pay`** and abort (`Permanent`) if `current send_quote + receive_quote > fee_cap`. The
  invoice stays fixed; only the cap re-check moves to `Pay`.
`reserved_fee` tracks `total`. For `DirectInflow` only the receive side applies (`send_quote = 0`).

## 7. FedimintExecutor::perform (async; op-log-aware)
```rust
async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {  // Done | Awaiting (§2)
    let Some(p) = MovePlan::from_action(&intent.action) else { return Err(Unsupported) };
    let mut rec = self.assemble_record(intent, &p).await?;        // FIRST: intent + backfilled artifacts, so a replayed
                                                                  // move reattaches (no re-quote, no spurious over-cap fail).
    loop { match next_step(&rec) {                                 // respects rec.send_required
        CreateInvoice => { // size the invoice (fixed point, §6) + cap-check the receive side ONCE here
                           let inv_amt = self.gross_up(&p).await?;                    // §6 fixed-point; receive cap-check
                           let (inv, op) = self.mc.receive(&p.to, inv_amt, &p.gateway, &intent.key).await?;
                           rec.invoice = Some(inv); rec.recv_op = Some(op); self.journal.put_move(&rec).await?;
                           if !rec.send_required {                                    // DirectInflow: payer is EXTERNAL —
                               return Ok(PerformOutcome::Awaiting);                   // surface invoice; intent stays AWAITING,
                           } }                                                        // claim finalized by the recv_op subscription (§9.5)
        Pay           => { // re-check the cap NOW. receive cost is recomputed crash-safely from the (op-log-recoverable)
                           // invoice: receive_quote = invoice_amount − amount. send fee is quoted FROM the invoice.
                           let inv = rec.invoice.as_ref().unwrap();
                           let receive_quote = amount_of(inv) - p.amount;            // = recv fee; both inputs survive a crash
                           if self.send_quote(&p, inv).await? + receive_quote > p.fee_cap { return Err(Permanent("fee over cap".into())); }
                           match self.mc.pay(p.from.as_ref().unwrap(), inv, &p.gateway, &intent.key).await? {
                               Started(op)|AlreadyInFlight(op)|AlreadyPaid(op) => rec.send_op = Some(op), }
                           self.journal.put_move(&rec).await?; }
        AwaitSettle   => { if !rec.send_required { return Ok(PerformOutcome::Awaiting); }   // DirectInflow: subscription owns the claim
                           // Move: the SEND leg is authoritative (A pays → swap → preimage). Await it first; only on Success
                           // wait on the (now-fast) receive claim — never block on B's invoice expiry for a refund.
                           match self.mc.await_send(p.from.as_ref().unwrap(), rec.send_op.unwrap()).await? {
                               Success(pre) => { let r = self.mc.await_receive(&p.to, rec.recv_op.unwrap()).await?; rec.phase = settle_ok(pre, r); }
                               Refunded|Failed => rec.phase = MovePhase::Refunded, }
                           self.journal.put_move(&rec).await?; }
        Done => return Ok(PerformOutcome::Done), Failed => return Err(Permanent("move failed".into())),
    }}
}
```
`assemble_record` (called FIRST) merges the Intent with the backfilled op-log artifacts (§4),
so a replayed move reattaches to its existing ops. `gross_up` (§6) sizes the invoice via the
fixed point + cap-checks the receive side, **once at `CreateInvoice`** (invoice then fixed);
the **send side is re-quoted at each `Pay`** (the gateway may have changed fees). `DirectInflow`
returns after `CreateInvoice` — its payer is external, so the claim is finalized async (§9.5).
`PerformOutcome::Awaiting` is a marker (wallet-core is pure and can't carry a BOLT11 type); the
**invoice is surfaced via a runtime read API** `Runtime::invoice_for(&intent.key) -> Option<Invoice>`
(reads the journal's `MoveRecord.invoice`), which the UI displays and the §10 test pays.

## 8. Journal storage — fedimint `Database`, prefix `[0x00]` (async)
`FedimintJournal` implements async `wallet_core::Journal` over `db.with_prefix(vec![0x00])`.
Rows use the fedimint `Database` RAW byte API, not typed `Encodable`: keys are type-tagged bytes
inside the `[0x00]` prefix, and values are versioned serde JSON envelopes:
```
0x01 ++ IdempotencyKey bytes -> v1(Intent)
0x02 ++ IdempotencyKey bytes -> v1(MoveRecord)            // derived cache (§5); rebuilt from op-log
0x03 ++ FederationId bytes   -> v1(FederationInfo { invite, db_prefix, joined_at }) // backed up, ADR-0003
0x04 ++ status_byte ++ key   -> ()                        // for pending()/failed()/awaiting() scans
```
The Intent write + its `PendingIndexKey` update happen in **one prefix-`[0x00]` dbtx** (atomic
within our state). `pending()`/`failed()`/`awaiting()` are raw prefix scans that load and
re-check each referenced Intent before returning it.

## 9. Resume loop (runtime.rs, async)
1. read `FederationKey` rows → `MultiClient::open_all(...)` (each client self-resumes its SMs).
2. **op-log backfill + merge:** for each client, `mc.backfill_ops(id)` **pages the op-log to
   exhaustion** (`paginate_operations_rev` until no more, NOT a single page — a missed op
   re-mints/re-pays) and decodes each `custom_meta` **fallibly** (skip + warn-log a malformed
   entry, never panic); group `OpArtifact`s by
   `move_id`; for each **`pending()` (Pending|Executing) AND `Awaiting`** Intent, assemble
   `MoveRecord` = Intent params + merged artifacts (+ cached record) and persist it — backfilling
   `Executing` too is required, else a crash in the receive-commit-before-MoveRecord window
   re-drives without the op artifact and mints a second invoice. Merge never drops `fee_cap`.
3. **Rehydrate `Awaiting` subscriptions:** for each `Awaiting` intent (a `DirectInflow` whose
   external payer hasn't settled), re-`subscribe` to its `recv_op` so the claim is still observed
   after a restart — otherwise the intent stays `Awaiting` forever. (`Awaiting` is NOT re-driven
   through `perform`; only its subscription is reattached.)
4. `wallet_core::reconcile(journal, executor).await` — re-drive **`pending()` only**
   (Pending|Executing); `Failed`/`Permanent` stay terminal and **`Awaiting` is skipped**
   (subscription-owned, §2/§9.5); `perform` sees backfilled MoveRecords + reattaches via op-ids.
5. **DirectInflow claims are finalized asynchronously:** `perform` returns once the invoice
   exists (the payer is external), so the `recv_op` subscription (step 3) observes `Claimed`,
   marks the intent `Done`, and updates balance — `apply` never blocks on an external payment.

## 10. Test plan
- **Pure unit (`cargo test`):** `next_step` resume-from-every-phase; async `apply`/`reconcile`
  with async `MockExecutor`/`MemJournal` (`#[tokio::test]`); `ExecError` retry vs terminal.
- **devimint e2e — CLI-driven (ADR-0023):** the money-path + crash gates drive the **`wallet-cli`
  binary** against a devimint federation (as devimint drives `fedimint-cli`), NOT the Rust API
  in-process. This makes the crash cases real: run `wallet-cli move …`, kill the process
  mid-move, then `wallet-cli reconcile`, and assert balances. **Crashes fire at DETERMINISTIC
  killpoints, not timing sleeps:** `wallet-cli` reads a test-only `WALLET_CLI_CRASH_AT=<named
  step>` (e.g. `after-receive-commit`, `after-send-commit`, `before-move-record`) and
  `std::process::abort()`s exactly there — so each crash-window case is reproducible, not racy.
  The cases —
  (a) `apply(Move A→B)` moves ecash; (b) crash after receive-commit-before-MoveRecord → backfill
  prevents a second invoice; (c) crash after send-commit-before-MoveRecord → no double-pay;
  (d) restore-from-seed mid-move (client DB gone): backfill CANNOT repair (no op-log) — assert
  the bounded hazard, i.e. backed-up move state (ADR-0003) detects/avoids the resend, else the
  loss is bounded by the v1 balance cap (ADR-0018); (e) misbehaving-gateway double (T4);
  (f) **`apply(DirectInflow to=B)`** generates a receive invoice on B and returns `Awaiting`;
  the test reads it via `Runtime::invoice_for(key)` and **pays it from an external LN node**
  (the simulated incoming payment), the subscription observes `Claimed`, the intent → `Done`,
  and B nets exactly `amount` (fixed-point gross-up — the cheap-lever gate, ADR-0022). Fed
  bootstrapped once per session (runbook §6).

## 11. Build order
0. **Foundational `wallet-core` refactor (behavior-preserving):** async `Executor`/`Journal`
   + async `apply`/`reconcile`/`drive` + async mocks/`#[tokio::test]`; `ExecError` variants +
   `PerformOutcome { Done, Awaiting }` + `IntentStatus::Awaiting`;
   newtypes (`FederationId([u8;32])`, `GuardianId`, `Msat`, `Occurrence`, `IdempotencyKey`),
   guardian identity → `Vec<GuardianId>`, idempotency-key formatting. (No `move_sm`.)
1. `move_protocol.rs` (`MoveRecord`, `next_step`, the op-log→MoveRecord mapping) + pure tests.
2. `FedimintJournal` over an in-memory fedimint `Database` using raw byte/serde rows + tests.
3. `MultiClient` (join/open/balance) **+ the `wallet-cli` crate with `join`/`balance`/`list-feds`**
   (ADR-0023) + a devimint smoke that drives `wallet-cli` (join a devimint fed, assert balance).
   Then `MultiClient` receive/pay/await/`backfill_ops` with `custom_meta`.
4. `FedimintExecutor` + `quote`/fee preflight + `assemble_record` merge → real ecash, exposed as
   `wallet-cli move`/`receive`/`reconcile`: `DirectInflow` (receive on a chosen fed, B nets
   `amount`) then `Move` (single-fed self-move), driven + asserted via `wallet-cli`.
5. Two-fed harness + the crash/reconcile/backfill gate via **real `wallet-cli` process kills**
   (`kill -9` mid-move → `wallet-cli reconcile` → no double-pay/double-invoice).

## 12. Decisions (settled)
- ⟦D1⟧ crate `wallet-fedimint`. ⟦D2⟧ 100% async, `&self` + interior mutability, no block_on.
- ⟦D3⟧ newtypes; `FederationId([u8;32])`; guardian identity = `GuardianId` (pubkey/URL).
- ⟦D4⟧ gateway explicit in Phase 1. ⟦D5⟧ one fedimint `Database` (RocksDB), prefixes `[0x00]`/`[0x01,..]`.
- ⟦D6⟧ no SQLite. ⟦D7⟧ single-fed self-move first, then two-fed.
- **⟦D8 (new)⟧ durability = op-log is source of truth** (move_id in `custom_meta` + startup
  backfill), NOT persist-before-act atomic writes.
- **⟦D9 (new)⟧ `fee_cap` enforced by preflight** (routing_info + tx fee) before send.
