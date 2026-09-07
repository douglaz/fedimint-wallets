# 05 — Persistence

The durable stores as built, read out of `wallet-fedimint/src/journal.rs`,
`wallet-fedimint/src/multi_client.rs`, `wallet-core/src/{ledger,types,executor}.rs` and
`wallet-api/src/lib.rs` on 2026-09-07. Every key, tag, row type and fence below is the one the
code has; `docs/operation-history-spec.md` §2's sketch and the `journal.rs` module header both
describe an earlier shape and are superseded here where they differ.

## Physical layout

**STO-1** A data directory holds **two** RocksDB stores, not one. `client.db` holds the
federation clients' partitions and the seed; `journal.db` holds the application journal. Inside
`journal.db` every journal key is prefixed `0x00`; inside `client.db` every client partition is
prefixed `0x01`. The prefixes still hold *within* each file, so nothing collides, but "one
database with two partitions" — the wording in `journal.rs`'s header and
`docs/fedimint-mechanics.md` — is stale. A test that plants a raw journal row MUST plant it
under `0x00` or the journal never sees it (`DEF-24`). *The split exists because the journal's
write churn flushed the fedimint client's small no-history memtable and failed its long-held
lnv2 transactions during a 24-hour soak.*

**STO-2** One process owns both stores. The lock is RocksDB's own on `client.db`, opened first
as the exclusivity anchor; a second opener blocks (the daemon) or refuses after a non-blocking
probe (the standalone CLI). `init`, `mnemonic` and `restore-mnemonic` are therefore
while-stopped operations.

**STO-3** Client `i` lives at `[0x01] ++ u32_le(db_prefix)`, exactly five bytes; the fixed
length prevents prefix aliasing. `next_db_prefix` is `1 + max(registry db_prefix, any 0x01 key
found by a raw scan of client.db)`; the raw scan closes the crash window where fedimint
committed partition N and the registry never recorded it. Orphaned partitions are never reused.
A failed join removes its fresh partition best-effort.

**STO-4** The seed is twelve BIP-39 words stored as entropy in fedimint's own client-secret slot
at the root of `client.db`. There is no mnemonic file. It is plaintext (`SEC-10`).

## Journal key layout

**STO-5** Every journal key is `[tag] ++ id_bytes`. Every value except the three flagged is a
JSON envelope `{"version":1,"data":<T>}`; any other version decodes as `Permanent`. There are
exactly thirteen tags:

| Tag | Key | Value |
|---|---|---|
| `0x01` | `++ utf8(idempotency_key)` | `Intent` |
| `0x02` | `++ utf8(key)` | `MoveRecord` |
| `0x03` | `++ FederationId[32]` | `FederationInfo {invite, db_prefix, joined_at (seconds)}` |
| `0x04` | `++ status_byte ++ utf8(key)` | **empty** — the pending index |
| `0x05` | `++ be64(seq)` (exactly 9 bytes) | `OperationRecord` — the ledger |
| `0x06` | `++ utf8(correlation_key)` | **raw `be64(seq)`** |
| `0x07` | (single key) | **raw `be64(next_seq)`** — the ledger counter |
| `0x08` | `++ FederationId[32]` | `ProbeRecord` |
| `0x09` | `++ FederationId[32]` | `CandidateRecord` |
| `0x0a` | (single key) | `WatchState` |
| `0x0b` | (single key) | `wallet_api::Policy` |
| `0x0c` | `++ utf8(old_key)` | `EvacuationSupersessionRecord` |
| `0x0d` | `++ utf8(new_key)` | the old key, as a JSON string |

`status_byte`: Pending 0, Executing 1, Done 2, Awaiting 3, Failed 4. The `journal.rs` header
lists seven of these; the constants block is the truth.

**STO-6** Correlation and idempotency keys are strings with these shapes, and repair
(`STO-24`) classifies rows by their prefix:

| Shape | Producer |
|---|---|
| `move:<from>:<to>:<occurrence>` | allocator |
| `move:<from>:<to>:<amount>:<fee_cap>:<occurrence>` | user move |
| `evac:<from>:<to>:<occurrence>` | allocator, scheduler |
| `refuse:<reason>:<fed>:<occurrence>` | allocator refusal |
| `conflict-suppressed:<candidate-key>:<reason>` | allocator, suppressed candidate |
| `tick-drop:<occurrence>:<decision-key>` | commit-time drop |
| `pay:<payment_hash>` | user pay |
| `recv:<to>:<amount>:<nonce>` | user receive |
| `dinflow:<to>:<amount>:<nonce>` | daemon direct-inflow |
| `direct-inflow:<to>:<amount>:<fee_cap>:<occurrence>` | standalone direct-inflow |
| `join:<fed>:<sha256(invite)>` | intent-backed join |
| `join:<fed>:<nonce>` | agent auto-join ledger row |
| `recover:<fed>:<sha256(invite)>` | recovery |
| `tick:<occurrence>:<nonce>` | one per tick invocation |
| `probe:<fed>:<nonce>` | active probe umbrella row |
| `discover:<label>[:<index>]:<nonce>`, `autojoin:<nonce>`, `approve:<fed>:<nonce>` | discovery |
| `watch-probe-skip:<candidate>:<spending>:<amount>:<bucket>` | scheduler |

`pay:` keys carry the payment hash and no nonce, so paying the same invoice twice attaches to
one operation (`OPS-8`). `docs/operation-history-spec.md` §2's `pay:<fed>:<nonce>` is not
what is built.

## Transaction model

**STO-7** The journal uses fedimint's raw byte transactions only. Reads take a no-commit
snapshot. Plain writes `begin_transaction … commit_tx_result` and map a commit conflict to
`ExecError::Retryable` with no in-journal retry; the caller sees `Retryable`. Decode failure is
`Permanent`; storage failure is `Retryable`.

**STO-8** Every compare-and-swap writer whose guard is read inside the transaction runs under
`db.autocommit(closure, None)`, which re-runs the closure on `WriteConflict` without bound
(`DEF-14`). Those writers are `set_status_if`, `reset_retryable`, `set_raw_terminal_if_fenced`,
`clear_marked_evacuation_if_pending`, `replace_marked_evacuation`, and `complete_recovery`. The
clock is snapshotted once outside the closure so retries share one timestamp.

**STO-9** The `Intent` row: `idempotency_key` (must equal the row key or the read is
`Permanent`), `attempt`, `action`, `max_fee`, `status`, `reason`, `actor`, `created_at_ms`,
`operation_id?`, `invoice?`, and `evacuation_refusal?` (`serde(default)`). The store enforces:
the attempt matches on every write (`upsert`, `set_status` reject a mismatch as `Permanent`;
every `*_if_attempt` returns `false`); the transition table `Pending→any, Executing→any,
Awaiting→{Awaiting,Done,Failed}, Done→Done, Failed→Failed` (`OPS-2`); `Failed→Pending` only via
`retry_failed_intent`, which requires exactly `attempt + 1`, refuses a superseded parent,
deletes the `0x02` move row, and appends a **fresh** ledger row repointing `0x06` (so a crashed
attempt and its retry are two truthful rows).

**STO-10** The pending index `0x04` holds `Pending`, `Executing`, `Awaiting` and `Failed`
intents; **`Done` is never indexed**, which is what makes finished work unscannable. Index and
intent row move in one transaction. `Journal::pending()` returns `[Pending, Executing]` — it
**includes `Executing`** — tolerating corrupt entries; `awaiting()` returns `[Awaiting]` and is
never re-driven; `reservation_intents()` returns `[Pending, Executing, Awaiting]` and **fails
closed** on any corrupt entry, so admission stops rather than under-reserve.

**STO-11** `MoveRecord` (`0x02`) is **partially** rebuildable, contrary to both the ledger
spec ("derived, rebuildable") and `AGENTS.md` ("cannot be re-created"). From the fedimint
op-log the executor can rebuild `recv_op`, `send_op`, `invoice`, `amount` and `fee_cap`; the
terminal `phase`, `preimage`, `outcome`, and both quoted fees survive only in this row. An
intent-backed move record may be written only through `put_move_if_attempt`, which fences on
attempt, non-terminal status and no active refusal marker, re-inserts the unchanged intent bytes
as a write-write fence, and after commit re-verifies ownership in a second transaction, undoing
its own write if it lost. `MovePhase` ∈ `Created, Invoiced, Sending, Settled, Refunded, Failed,
Stranded`.

**STO-12** `WatchState` (`0x0a`): `occurrence`, `last_discover_ms`, `discover_cursor?`,
`discover_backlog`, `discover_rotation`. `advance_watch_occurrence` does a checked `+1` and
`observe_watch_occurrence` a `max`; both reject `u64::MAX`, which is the fail-closed value.

**STO-13** `Policy` (`0x0b`): thirty fields (`API-27`), seeded insert-if-absent by `walletd
init` and again at actor start, which then validates the stored row and refuses to start on an
invalid one. `put_policy` is a plain overwrite; **the journal does not validate** — the actor
does, and the HTTP handler rejects unknown keys (`API-20`). The stored type is permissive
(`STO-31`); `max_fee_bps_of_move`, `evac_fee_base_msat` and `evac_fee_bps` carry named serde
defaults because rows written before them exist.

**STO-14** The federation registry (`0x03`) is written by a plain overwrite after the client
partition exists and before the in-memory client is inserted. `get_federation` fails closed on
a corrupt row; `list_federations_report` skips a malformed key or undecodable value, counts it
in `skipped_rows`, and warns. Three planning surfaces MUST treat `skipped_rows > 0` as "the
world is unknown" rather than plan from the healthy subset (`ALC-46`).

## The operation ledger

**STO-15** `OperationRecord` (`0x05`): `seq, correlation_key, kind, actor, reason, status,
created_at_ms, updated_at_ms, fees {fee_cap?, receive_fee?, send_fee_quoted?}, error?,
repaired`. `reason` is mandatory; user verbs carry `user_initiated`. `OperationKind` has twelve
variants: `Join, Recover, Receive {amount_invoiced, op_id?, gateway?}, Pay {invoice_amount?,
payment_hash?, op_id?, gateway?}, DirectInflow, Move {from, to, amount, send_op?, recv_op?,
gateway?, evacuation}, Refusal {fed, diagnostics (serde default)}, Probe {fed, from,
amount_msat, cost_msat?}, Tick {occurrence, decisions, performed, failed}, Discover, AutoJoin,
Approve`. A refusal row MUST carry the figures that produced it in `diagnostics` (`DEF-2`):
`source, want, available, source_spendable, max_fee, max_fee_bps, cap_room, amount,
conflict_suppressed, min_move`. `RefusalDiagnostics` compares equal to any other, so refusal
identity is `(fed, reason)`.

**STO-16** Write discipline, enforced by the pure `advance` function and one writer
(`ledger_upsert_in`): create on first observation; update only to advance status (rank
`Started < Awaiting < terminal`) and fill fields; a terminal row returns `None` to any further
write **except** that a terminal row written by repair (`repaired: true`) may be superseded
exactly once by an authoritative write. **Nothing deletes a `0x05`, `0x06` or `0x07` key.**
Every intent status change writes its ledger row in the same transaction (`upsert`,
`set_status`, `set_status_if`, `reset_retryable`, `complete_recovery`,
`replace_marked_evacuation`, the artifact and raw-terminal writers), so ledger and journal
cannot disagree about an intent-backed operation.

**STO-17** On every intent-backed ledger write the row refreshes op-ids, gateway and quoted
fees from the `0x02` record, and — once a move artifact exists (invoice, receive op or send op)
— refreshes **both** `amount` and `fee_cap` to the executed amount and the enforced cap
(`DEF-4`). Refreshing one without the other is forbidden: an auditor recomputing the cap from a
planned amount would derive a number nobody enforced.

**STO-18** Sequence assignment is fenced in O(1): before any fresh append the counter `0x07`
must equal `tail_seq + 1`, where the tail is the lexicographically greatest `0x05` key, which
must be a canonical nine-byte key whose embedded `seq` matches. Any disagreement is a
`Permanent` error that fences **every** fresh append, user and agent, with an operator message
to restore from backup. The counter is exhausted at `u64::MAX`.

**STO-19** `history(limit, before_seq)` performs a full ascending scan of `0x05`, reverses it,
filters `seq < before_seq`, and takes `limit` — O(total rows) per page, though a descending
iterator exists and is used elsewhere. **Undecodable rows are skipped with a warning and
otherwise no signal**; the daemon and CLI show a shorter history, not an error (`DEF-10`'s
three rows were invisible this way). The daemon caps `limit` at 500.

**STO-20** `0x06` maps a correlation key to exactly one current row. A retry appends a fresh row
and repoints the index, so older attempts' rows are reachable by `seq` only.
`OperationRef::Key` resolves through the index; `OperationRef::Seq` reads `0x05` directly.

**STO-21** On every fresh insert of an `Actor::Agent{occurrence}` row, and in
`retry_failed_intent`, `note_ledger_insert_in` raises `WatchState.occurrence` to
`max(current, occurrence)` **in the same transaction**. User rows never touch `0x0a`. This is
what keeps the checkpoint at or above every Agent occurrence ever appended, whatever path
admitted it.

**STO-22** An unreadable ledger row MUST NOT fence automation (`DEF-12`). Operational scans
(`history`, `pending()`, `failed()`) skip and warn. The scans that decide money — the probe
budget, the auto-join caps, `reservation_intents()` — fail closed on a corrupt row, so the
blast radius of one bad row is a disabled subsystem with an explicit error, never a silent
under-count and never a permanently stopped scheduler.

**STO-23** An absent `WatchState` is seeded from the ledger's highest `Actor::Agent` occurrence
by an O(ledger) scan that runs once. Discovery cursor, backlog and rotation are not recoverable.

**STO-24** `repair_ledger` scans `0x05` and repairs only these classes, each write fenced on a
re-read of `seq`, federation, role, op and status inside its own transaction:
`join:` rows are arbitrated per federation against the registry (present → the already-succeeded
attempt, else the newest within ±60 s of `joined_at`, else the newest, soft-Succeeded; losers
soft-Failed "superseded"; absent and older than one hour → soft-Failed "not registered");
`pay:`/`recv:` rows are observed from the op-log by correlation key, else by payment hash (a
repair write with a dedup note; an in-flight or failed original is never adopted for a later
attempt), else after one hour soft-Failed "never reached the federation"; `tick:` and discovery
rows older than one hour → soft-Failed "interrupted". Move-shaped intent rows and `recover:`
rows are never repaired. A raw terminal repair re-drives the intent sink except for the
never-reached case.

**STO-25** Evacuation supersession writes two sidecars in the same transaction as the exchange:
`0x0c old_key → {old_key, old_attempt, new_key, new_attempt, old_occurrence, occurrence, source,
old_cap_components?, new_cap_components?, refusal, superseded_at_ms}` and `0x0d new_key →
old_key`. A superseded parent can never be retried; a child's namespace must be empty across
`0x01/0x02/0x06/0x0c/0x0d` and all five `0x04` status keys before creation; at most one live
Agent evacuation per source may exist at exchange time; the replay path validates that both
sidecars exist and agree before returning success without writing (`OPS-30`).

**STO-26** `ProbeRecord` (`0x08`): `attempts` (pruned to the newest 256 while keeping every
attempt within the 7-day TTL, the newest attempt, and per-source newest success) and one
`in_flight` session whose nonce is exclusive; an outcome with a stale nonce is ignored.
`CandidateRecord` (`0x09`): `id` (must equal the key), `invite`, `source`, `discovered_at_ms`,
`structural`, `structural_checked_at_ms`, `state ∈ {Rejected, Discovered, AutoJoined,
UserApproved}`, `updated_at_ms`. `put_candidate` cannot demote `UserApproved`;
`approve_auto_joined_candidate` only promotes from `AutoJoined` and writes the `Approve` ledger
row in the same transaction.

## What is and is not durable

**STO-27** Not durable: the probe verdict policy, the watch/discovery policies (derived from
`Policy` at runtime), the actor's policy generation, the driver in-flight guard, and any
recovery-in-progress marker — a registered-but-unopened federation is the documented ambiguity.

**STO-28** Losing `journal.db` loses the registry, and with it every client partition's
address: `client.db` still holds the ecash, unreachable. The supported path is seed recovery
(`FMI-30`), not a store copy. Losing `client.db` loses the seed and the send-dedup state; seed
recovery from the twelve words rebuilds balances but not dedup (`FMI-32`).

## Compatibility rules for types written to a live store

**STO-29** The types the running daemon writes are: `Intent` (and every `Action` variant inside
it), `MoveRecord`, `FederationInfo`, `OperationRecord` (and every `OperationKind` variant),
`ProbeRecord`, `CandidateRecord`, `WatchState`, `Policy`, `EvacuationSupersessionRecord`. A type
is on this list because the daemon writes it (`DEF-13`).

**STO-30** A field added to a type on that list — including to an already-shipped variant —
MUST carry `#[serde(default)]`, with a **named** default function for a numeric field, because a
bare default yields zero (`DEF-10`; a zero evacuation cap is a livelock). As built the fields
carrying it are `Intent.evacuation_refusal`, `Action::Move.gateway`,
`Action::Evacuate.{gateway, fee_cap_components}`, `OperationKind::Refusal.diagnostics`,
`RefusalDiagnostics.{max_fee_bps, conflict_suppressed}`, and the three `Policy` fields in
`STO-13`. Each MUST be pinned by a test that strips the key from a persisted row and re-reads it
(`CNF-18`).

**STO-31** No type on that list may carry `#[serde(deny_unknown_fields)]` (`DEF-11`): a row
written by a newer build must stay readable by the previous build or a rollback cannot start.
As built, none does; every `deny_unknown_fields` in `wallet-api` is a request-only DTO.

**STO-32** The ledger is greenfield in one respect only: `OperationRecord.repaired` and
`WatchState.discover_rotation` carry no default, on the claim that no row predates them. That
claim is unverified against the long-running deployment's store (`HST-23`).
