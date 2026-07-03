# Operation history — the append-only ledger (spec)

**Requirement.** Track all relevant details of every operation the wallet performs — user- and
agent-initiated — so the user can reconstruct **exactly what happened, why, what it cost, and
when**, at any later time. This is also what makes ADR-0014 real: an on-device agent acting on a
standing instruction is only defensible if every action it takes is durably auditable after the
fact.

**Greenfield note.** Pre-release, no persisted data, no external users: no backwards
compatibility, no migration shims. Types below replace the current shapes cleanly.

## 1. Three durable structures, three jobs (do not conflate)

| Structure | Job | Mutability | Exists? |
|---|---|---|---|
| Intent journal | crash recovery / resume (re-drive set) | mutable status, index-pruned | yes |
| `MoveRecord` | reattachment cache (op-ids/invoice/gateway) | derived, rebuildable | yes |
| **Operation ledger** | the user's record: what/why/cost/when | **append-only** | **this spec** |

The review (2026-07-03) established that the first two cannot serve as history: `Done` intents
are deliberately unscannable, records carry no timestamps/reasons/actual-fees, `MoveRecord` is
rebuildable by design, refusals and raw `receive`/`pay` leave no durable trace at all.

## 2. Data model

All types in `wallet-core` (pure, serde). Storage in `wallet-fedimint` next to the journal.

```rust
/// One row per user-meaningful operation. Append-only: a row is created once, its
/// status may advance Started/Awaiting -> terminal, and a TERMINAL row is immutable.
pub struct OperationRecord {
    /// Monotonic per-wallet sequence (durable counter, incremented in the same dbtx).
    /// The ordering authority — robust to clock skew; wall-clock is for display.
    pub seq: u64,
    /// Joins ledger <-> journal <-> MoveRecord. For journaled ops this IS the intent's
    /// IdempotencyKey; for raw ops it is derived ("pay:<fed>:<op_id>", "recv:<fed>:<op_id>";
    /// "join:<fed>"). Exactly one ledger row per correlation key.
    pub correlation_key: IdempotencyKey,
    pub kind: OperationKind,
    /// Who initiated it — THE audit discriminator ADR-0014 needs.
    pub actor: Actor,
    /// The real reason (allocator ReasonCode for agent ops; None for plain user verbs).
    /// Requires threading AllocatorDecision.reason into the Intent instead of dropping it.
    pub reason: Option<ReasonCode>,
    pub status: OperationStatus,
    /// Unix millis. created_at is first observation; updated_at is the last transition
    /// (terminal time). seq is authoritative for order; these answer "when".
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub fees: FeeBreakdown,
    /// Terminal failure/refusal detail, verbatim (the MoveRecord outcome / error string).
    pub error: Option<String>,
}

pub enum Actor {
    User,
    /// A tick/standing-instruction action; occurrence identifies the allocation epoch.
    Agent { occurrence: Occurrence },
}

pub enum OperationStatus { Started, Awaiting, Succeeded, Failed }

/// Typed, complete per-kind details. Amounts are NET unless stated.
pub enum OperationKind {
    Join     { fed: FederationId },
    /// Raw LN receive (user verb; journal-less today but ledger-recorded).
    Receive  { fed: FederationId, amount: Msat, op_id: OperationId, gateway: Option<GatewayUrl> },
    /// Raw LN pay (user verb).
    Pay      { fed: FederationId, invoice_amount: Msat, op_id: OperationId, gateway: Option<GatewayUrl> },
    /// Executor-driven inflow netting exactly `amount`.
    DirectInflow { to: FederationId, amount: Msat, recv_op: Option<OperationId>, gateway: GatewayUrl },
    /// Cross-fed move/evacuation: BOTH legs correlated in one row.
    Move     { from: FederationId, to: FederationId, amount: Msat,
               send_op: Option<OperationId>, recv_op: Option<OperationId>,
               gateway: GatewayUrl, evacuation: bool },
    /// An advisory decision the allocator recorded but did not execute — the durable
    /// answer to "why didn't the wallet act?".
    Refusal  { fed: FederationId },
    /// One row per tick: the agent ran, with decision/apply counts. Individual moves it
    /// performed get their own Move rows (actor = Agent).
    Tick     { occurrence: Occurrence, decisions: u32, performed: u32, failed: u32 },
}

pub struct FeeBreakdown {
    pub fee_cap: Option<Msat>,
    /// Receive-side cost: EXACT (invoice_amount - net), known at mint.
    pub receive_fee: Option<Msat>,
    /// Send-side cost: the pay-time quote (mark estimate until the SDK exposes the final
    /// contract cost; the Phase-4.A contract-amount quote fix feeds this).
    pub send_fee_quoted: Option<Msat>,
}
```

Net-effect display is derived, not stored: a `Move` shows `from: -(amount + fees)`,
`to: +amount`.

## 3. Storage + write discipline

Rows live in the journal's `[0x00]` app partition:

- `0x05 ++ be64(seq)` → JSON row v1(`OperationRecord`) — natural time-ordered scan.
- `0x06 ++ correlation_key` → `be64(seq)` — key lookup + the exactly-one-row-per-key guard.
- `0x07` → `be64(next_seq)` — the durable counter.

Rules (load-bearing):

1. **Same-dbtx with the journal.** The ledger write/update commits in the SAME transaction as
   the intent upsert / status flip it describes, so ledger and journal can never disagree. The
   natural seam: `FedimintJournal` (it already owns those dbtxs and can read the `MoveRecord`
   row in-partition for fees/op-ids). Raw ops and refusals use explicit `record_*` calls.
2. **Append-once, advance-forward, terminal-immutable.** Create on first observation
   (`Started`/`Awaiting`); update only to advance status and fill fees/op-ids/error; NEVER
   mutate a `Succeeded`/`Failed` row; NEVER delete. Re-drives/replays of the same key update
   the one existing row (found via `0x06`), they never append duplicates.
3. **`seq` orders, clocks display.** The counter increments in the row's own dbtx. Wall-clock
   comes from a `now_ms()` injected clock (testable); a bad clock degrades display, never order.
4. **Failures and refusals are first-class rows.** A `Failed` op, a `Refusal`, an expired
   inflow — all recorded with their reason/error. History without failures is not history.

## 4. Upstream changes this requires

- `Intent` gains `reason: Option<ReasonCode>`, `actor: Actor`, `created_at_ms: u64`
  (`Intent::from_decision` stops dropping the reason; `runtime.rs` stops hardcoding dummies).
  This makes every re-drive path (reconcile, apply-replay) able to maintain the ledger without
  the original decision in hand.
- The executor persists what it already computes: send/receive quotes at `Pay`, and (Phase 4.A)
  the preimage on the `MoveRecord`.
- CLI raw `receive`/`pay` call `record_*` at start; `await-receive`/`await-send` advance the row
  to terminal via the correlation key.
- `Runtime::tick` writes the `Tick` row + `Refusal` rows for advisory decisions (deduped by
  their existing `refuse:` idempotency keys).

## 5. Query surface (wallet-cli; the Android activity screen reads the same API)

- `wallet-cli history [--limit N] [--fed <hex>] [--actor user|agent] [--status ...] [--since <ts>] [--json]`
  — newest-first scan; one line per op: seq, local time, kind, amount, fees, status, reason.
- `wallet-cli show <key|seq>` — the full record: both legs' op-ids, gateway, fee breakdown,
  error, timestamps, and the linked intent status.
- Plain-text default, `--json` for scripts (ADR-0023).

## 6. Tests / gate

- **Pure goldens:** record construction per kind; append-once/terminal-immutability property
  (a terminal row rejects mutation); one-row-per-key under replay; seq monotonicity.
- **Journal tests (MemDatabase):** same-dbtx atomicity — a crash injected between intent flip
  and ledger write is impossible by construction (single commit); scans ordered by seq.
- **Devimint smoke (`smoke_history_devimint.sh`):** join → direct-inflow → move → tick, then
  `history` shows all rows with correct kinds/actors/fees, timestamps non-decreasing, a forced
  failure and a refusal both present. This is the phase exit gate.

## 7. Non-goals (v1)

- No event-sourcing per state transition (one row per op with created/updated times is enough;
  op-ids let a power user drill into the fedimint op-log for transition-level detail).
- No pruning/rotation — a personal wallet's op count is tiny; revisit if rows ever exceed ~10^5.
- No pagination index beyond the seq scan; `--limit` + reverse scan suffices.
