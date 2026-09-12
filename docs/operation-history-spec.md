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
**Authority:** this document is the historical requirement and the motivation for the ledger.
It is **not** authoritative for any shape, key, string or rule: the as-built specification in
[`05-persistence.md`](https://github.com/douglaz/fedimint-wallets-spec/blob/main/05-persistence.md) in the specification repository owns them — the JSON encoding
(`STO-5`), the correlation-key shapes (`STO-6`), the row type and every `OperationKind`
payload (`STO-15`), the write discipline and the pure `advance` rule (`STO-16`), the refresh
from the move record (`STO-17`), the sequence fence (`STO-18`), `history` (`STO-19`), the
`0x06` index (`STO-20`), repair (`STO-24`), evacuation supersession (`STO-25`), the op-log
metadata (`STO-33`, `STO-34`) and the verbatim error strings (`STO-35`). The sketch below is
kept as the original derivation; where it and the `STO` rules differ, the `STO` rule is what
the code does and this sketch is simply out of date. (Known differences: `reason` is mandatory
— user verbs carry `ReasonCode::UserInitiated`; gateways are `Option`; `OperationRecord` has a
`repaired: bool` and terminal immutability has exactly one exception for a repair-written
terminal; the rule-5/6 negative repairs are age-gated (1 hour) soft failures; `Receive` is
`amount_invoiced`; `Pay` carries `payment_hash: Option<[u8;32]>` and `invoice_amount` is
`Option`; `DirectInflow`/`Move` gateways are `Option`; `Refusal` carries `diagnostics`; there
are `Recover`, `Probe`, `Discover`, `AutoJoin` and `Approve` kinds; and the raw-op keys are
`pay:<payment_hash>` / `recv:<to>:<amount>:<nonce>`, not the nonce-only forms below.) The
archived [phase4-implementation-spec.md](./archive/phase4-implementation-spec.md) is history
only and is not a tiebreaker.

```rust
/// One row per user-meaningful operation. Append-only: a row is created once, its
/// status may advance Started/Awaiting -> terminal, and a TERMINAL row is immutable.
pub struct OperationRecord {
    /// Monotonic per-wallet sequence (durable counter, incremented in the same dbtx).
    /// The ordering authority — robust to clock skew; wall-clock is for display.
    pub seq: u64,
    /// Joins ledger <-> journal <-> MoveRecord. For journaled ops this IS the intent's
    /// IdempotencyKey. This sketch proposed per-attempt, nonce-only keys for raw ops; AS BUILT
    /// the correlation key is the intent key, a manual retry keeps that key at `attempt + 1`
    /// and appends a FRESH ledger row (the failed row stays; `0x06` repoints to the new one —
    /// `STO-9`, `STO-20`), and the per-attempt identity in the op's `custom_meta` is the
    /// `retry:<len>:<key>:<attempt>` correlation key (`STO-34`). The crash-safety property
    /// survives: the key is constructible from the RAW input before any side effect (§3 rule 5).
    /// AS BUILT the shapes are `STO-6`'s: `pay:<payment_hash>` (no nonce — paying the same
    /// invoice twice attaches to one operation) and `recv:<to>:<amount>:<nonce>`; the key
    /// that rides in the op's `custom_meta` is the per-attempt correlation key of `STO-34`.
    /// The `pay:<fed>:<nonce>` / `recv:<fed>:<nonce>` forms this sketch originally proposed
    /// were never built. Also as built:
    /// `join:<fed>:<sha256(invite)>` for a user/API join (the `join:<fed>:<nonce>` form is
    /// only the agent's auto-join ledger row), `tick:<occurrence>:<nonce>` (each tick invocation is its own
    /// row, created `Started` before deciding, advanced to terminal with the counts; the
    /// tick's individual moves remain covered by their own intent-keyed rows).
    /// Append-only under retry AS BUILT: a manual retry appends a fresh row and the failed
    /// attempt's row stays as its audit record — two truthful rows (`STO-9`, `STO-20`); only a
    /// `Retryable` re-drive of the SAME attempt advances the existing row (`STO-16`). Retries that lnv2 DEDUPS to the same
    /// underlying payment (`AlreadyInFlight`/`AlreadyPaid`) still record the SHARED
    /// `op_id`; aggregation (fee/amount sums) groups by `op_id` so shared-op attempt rows
    /// are never double-counted. `0x06` maps a correlation key to exactly one CURRENT row (`STO-20`).
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
    /// Raw LN receive (user verb; journal-less today but ledger-recorded). `op_id` is
    /// None on the pre-call `Started` row (§3 rule 5) and filled post-call/by backfill.
    /// NOTE the stated exception: this amount is the GROSS invoiced amount (impl spec §7
    /// names it `amount_invoiced`) — the user's input, known pre-call; the NET credit is
    /// amount − fees.receive_fee (a raw lnv2 receive deducts fees from the invoice,
    /// unlike the exact-net DirectInflow).
    Receive  { fed: FederationId, amount: Msat, op_id: Option<OperationId>, gateway: Option<GatewayUrl> },
    /// Raw LN pay (user verb). `op_id` optional for the same pre-call reason.
    Pay      { fed: FederationId, invoice_amount: Msat, op_id: Option<OperationId>, gateway: Option<GatewayUrl> },
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

Net-effect display is derived, not stored, and is STATUS-DEPENDENT: only a `Succeeded` move
renders `from: -(amount + fees)`, `to: +amount`. A `Started`/`Awaiting` move renders the
debit as in-flight with NO credit yet, and a stranded/failed-after-send row (Phase 4.A)
renders the debit with an explicit "not credited" marker — history must be most accurate
exactly when a move is unsettled.

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
   the one existing row (found via `0x06`), they never append duplicates. ONE exception
   (impl spec §7): a terminal row written by reconcile REPAIR (`repaired: true` — an
   absence-of-evidence conclusion) may be superseded exactly once by an AUTHORITATIVE
   evidence-carrying write; repair writes never supersede anything terminal.
   **Evacuation supersession is different:** for the narrow pre-artifact, structurally marked
   agent case, the parent and a fresh child are distinct operation rows. One atomic journal
   transaction marks the parent `Failed`, inserts the child at an advanced occurrence
   and distinct key, and writes durable forward/reverse sidecars. The child's *`Intent`* is
   `Pending`; its *operation row* is `Started`, because `status_from_intent` maps
   `Pending`/`Executing` onto `OperationStatus::Started` and `OperationStatus` has no `Pending`
   variant (see the enum above). The two vocabularies stay distinct here: the ledger cannot
   represent an intent lifecycle state, and calling the ROW `Pending` invites a projection the
   declared enum cannot express. It does not mutate either
    row's identity or erase the parent's evidence. `show` projects a live `evacuation_refusal`
    marker for its exact key; history intentionally omits that projection rather than doing an
    intent lookup per row. Standalone `status` is dry: a stale/default occurrence warns and returns
    the scored/designation diagnostic with no would-run decisions, while a strictly newer occurrence
    may display a prospective replacement. It writes neither parent nor child; daemon status remains
    strict because it owns occurrence allocation. A replacement round admits only its child;
    executable deferred ordinary decisions remain pinned-input validation facts and produce
    explicit `tick-drop:`-keyed audit rows with the replacement-exclusive note; deferred
    `RefuseInflow` advisories retain their `refuse:` identity and diagnostics with that same
    replacement-exclusive error note.
    Conflict-suppressed decisions retain their separate holder-voucher semantics.
3. **`seq` orders, clocks display.** The counter increments in the row's own dbtx. Wall-clock
   comes from a `now_ms()` injected clock (testable); a bad clock degrades display, never order.
4. **Failures and refusals are first-class rows.** A `Failed` op, a `Refusal`, an expired
   inflow — all recorded with their reason/error. History without failures is not history.
5. **Row before side effect, repaired by backfill (raw ops).** An `op_id` only exists AFTER
   the SDK commits the operation, so a row keyed on it can be lost to a crash — exactly the
   window the ledger must cover. Therefore: the correlation key is known/generated BEFORE the
   SDK call (§2), the `Started` row is written pre-call, and the key rides in the op's
   `custom_meta` (extending the existing role-tag meta). Crash after the SDK call but before
   the `op_id` update → reconcile's op-log backfill re-finds the op by its `custom_meta` key
   and repairs the row (the same pattern `MoveRecord` backfill already uses). Crash before the
   SDK call → the row stays `Started` with no matching op; reconcile marks it soft-`Failed`
   ("never reached the federation") — AGE-GATED (1 hour) and written as a defeasible repair
   (`repaired: true`, rule 2's exception), per impl spec §10.3 — rather than leaving it
   ambiguous forever. A retry is a NEW attempt row (§2), so this terminal marking never
   blocks recovery.
6. **`Join` repairs from the registry.** (The "idempotent re-joins are not rows" rule below is
   NOT what was built: neither the CLI nor the daemon gates on the registry synchronously —
   every join writes an attempt row pre-call (`join:<fed>:<sha256(invite)>` for a user or API
   join; `join:<fed>:<nonce>` is only the agent's auto-join row, `STO-6`) and lets the driver
   decide (`API-22`), and a no-op re-open terminalizes that row `Succeeded` carrying
   `JOIN_NOOP_REOPEN_NOTE` (`STO-35`). The as-built rules are the specification repository's.)
   As planned: already joined → the join verb just (re)opens the
   client, NO ledger row (nothing happened). Not joined → new `join:<fed>:<nonce>` attempt
   row pre-call, updated to terminal post-call. Reconcile repairs a stranded `Started` join
   row from the registry (the authority on membership), PER ATTEMPT with timestamp
   arbitration and soft writes — the full rules live in impl spec §10.3: fed present →
   soft-`Succeeded` for the arbitrated attempt (others soft-`Failed("superseded by a later
   join attempt")`); absent (and > 1h old) → soft-`Failed("join did not complete —
   federation not in the registry; re-run join")`. ("Never joined" would be dishonest —
   local partition state may exist.)

## 4. Upstream changes this requires

- `Intent` gains `reason: Option<ReasonCode>`, `actor: Actor`, `created_at_ms: u64`
  (`Intent::from_decision` stops dropping the reason; `runtime.rs` stops hardcoding dummies).
  This makes every re-drive path (reconcile, apply-replay) able to maintain the ledger without
  the original decision in hand.
- The executor persists what it already computes: send/receive quotes at `Pay`, and (Phase 4.A)
  the preimage on the `MoveRecord`.
- CLI raw `receive`/`pay`: generate the correlation key pre-call (§2), write the `Started` row,
  embed the key in `custom_meta`, update the row with the `op_id` post-call;
  `await-receive`/`await-send` advance the row to terminal via the correlation key; reconcile
  backfills/repairs rows per §3 rule 5.
- `Runtime::tick` writes the `Tick` row + `Refusal` rows for advisory decisions, keyed either
  `refuse:<reason>:<fed>:<occurrence>` or
  `conflict-suppressed:<candidate-key>:<reason>`. A commit-time rejection of an executable
  decision is instead a distinct `tick-drop:<occurrence>:<decision-key>` row; all three key
  forms deduplicate through the correlation-key index.

## 5. Query surface (wallet-cli; the Android activity screen reads the same API)

- `wallet-cli history [--limit N] [--fed <hex>] [--actor user|agent] [--status ...] [--json]`
  — newest-first scan; one line per op: seq, local time, kind, amount, fees, status, reason.
  (A `--since <ts>` filter was considered and dropped for v1 — seq + `--limit` suffice; the
  impl spec §11 owns the exact CLI shape.)
- `wallet-cli show <key|seq>` — the full record: both legs' op-ids, gateway, fee breakdown,
  error, timestamps, linked intent status, and any `supersedes` / `superseded_by` sidecar
  links. Standalone `show --json` and JSON history emit a flattened
  `OperationRecordAuditView`, not a raw `OperationRecord`: the persisted fields remain top-level
  and the two optional links are projected from sidecars without changing the durable record
  shape. Daemon `history`/`show` instead expose the public `OperationView`, with those same
  optional link fields. `show`'s `evacuation_refusal_active` is tri-state: `true` only for an
  exact readable Pending Agent Evacuate marker, `false` for an exact readable inactive intent, and
  omitted when no exact intent is available; history also omits it because it does not read intents.
- Public daemon history is paged newest-first: `GET /v1/history` caps `limit` at 500 and returns
  `next_before_seq` for a full page; pass that value as `before_seq` to retrieve the next page.
- Plain-text default, `--json` for scripts (ADR-0023).

## 6. Tests / gate

- **Pure goldens:** record construction per kind; append-once/terminal-immutability property
  (a terminal row rejects mutation); one-row-per-key under replay; seq monotonicity.
- **Journal tests (MemDatabase):** same-dbtx atomicity — a crash injected between intent flip
  and ledger write is impossible by construction (single commit); scans ordered by seq; the
  §3-rule-5 raw-op windows (row-no-op → reconcile marks Failed; op-no-op_id → backfill repairs
  via `custom_meta`); a failed `join` attempt followed by a successful retry yields two rows.
- **Devimint smoke (`smoke_history_devimint.sh`):** join → direct-inflow → move → tick, then
  `history` shows all rows with correct kinds/actors/fees, timestamps non-decreasing, a forced
  failure and a refusal both present. This is the phase exit gate.

## 7. Non-goals (v1)

- No event-sourcing per state transition (one row per op with created/updated times is enough;
  op-ids let a power user drill into the fedimint op-log for transition-level detail).
- No pruning/rotation — a personal wallet's op count is tiny; revisit if rows ever exceed ~10^5.
- No pagination index beyond the seq scan; `--limit` + reverse scan suffices.
