# 05 — Persistence

The durable stores as built, read out of `wallet-fedimint/src/journal.rs`,
`wallet-fedimint/src/multi_client.rs`, `wallet-core/src/{ledger,types,executor}.rs` and
`wallet-api/src/lib.rs` on 2026-09-07, re-read against `7225114` on 2026-09-10. Every key, tag,
row type and fence below is the one the
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

**STO-2** One process owns both stores. The lock is fedimint's advisory lock **file**
`client.db.lock`, beside the `client.db` directory (not RocksDB's internal `LOCK` inside it),
taken when `client.db` is opened first as the exclusivity anchor; a second opener blocks (the daemon) or refuses after a non-blocking
probe (the standalone CLI). `init`, `mnemonic` and `restore-mnemonic` are therefore
while-stopped operations.

**STO-3** Client `i` lives at `[0x01] ++ u32_le(db_prefix)`, exactly five bytes; the fixed
length prevents prefix aliasing. `next_db_prefix` is `1 + max(registry db_prefix, any 0x01 key
found by a raw scan of client.db)`, where the scan reads bytes `1..5` of every key under the
`0x01` prefix as `u32_le` and skips (with a warning) any key shorter than five bytes; when the
registry is empty **and** the scan finds nothing the result is `0`; a maximum of `u32::MAX` is
an error, never a wrap. The raw scan closes the crash window where fedimint committed partition
N and the registry never recorded it. Orphaned partitions are never reused. A failed join
removes its fresh partition best-effort.

**STO-4** The seed is twelve BIP-39 words stored as their **16-byte entropy** in fedimint's own
client-secret slot at the root of `client.db`: the SDK's `EncodedClientSecretKey` (client db
prefix byte `0x28`), whose value is the fedimint consensus encoding of the entropy as a
`Vec<u8>`, written once by `Client::store_encodable_client_secret` and never overwritten. The
root secret every client partition derives from is `Bip39RootSecretStrategy::<12>::to_root_secret(mnemonic)`;
an implementation that derives differently recovers different ecash from the same twelve
words. There is no mnemonic file. It is plaintext (`SEC-10`).

## Journal key layout

**STO-5** Every journal key is `[tag] ++ id_bytes`. Every value except the three flagged is a
JSON envelope `{"version":1,"data":<T>}` (`version` is a `u8`; any other version decodes as
`Permanent`; the envelope itself tolerates unknown keys). `<T>` is the type's **serde_json
default representation**, and this rule is the single owner of that encoding contract for every
stored type in `STO-29` and every wire DTO that embeds one of these types (`API-…` rules cite
it rather than restate it):

| Rust shape | JSON form |
|---|---|
| struct | object; keys are the Rust field names verbatim (`snake_case`), no renames |
| enum, unit variant (`IntentStatus`, `OperationStatus`, `MovePhase`, `ReasonCode`, `CandidateState`, `DiscoverySource`, `Actor::User`, `SourceStatus::Ok`, `StructuralOutcome::Passed`) | the variant name as a JSON string, PascalCase verbatim: `"Pending"`, `"UserInitiated"`, `"User"` |
| enum, struct variant (`Action`, `OperationKind`, `Actor::Agent`) | **externally tagged**: `{"Move":{"from":…,"to":…}}`, `{"Agent":{"occurrence":7}}` |
| enum, newtype variant (`SourceStatus::Failed(String)`, `StructuralOutcome::Rejected(String)`) | `{"Failed":"…"}`, `{"Rejected":"…"}` |
| newtype struct (`Msat`, `Occurrence` → `u64`; `IdempotencyKey`, `GatewayUrl`, `Invoice` → `String`) | **transparent**: the bare inner value (`123`, `"move:…"`) |
| `[u8; 32]` (`FederationId`, `OperationId`, `Preimage`, `payment_hash`) | a JSON **array of 32 integers** `[170,170,…]`, never hex, never base64 |
| `u64` / `u32` / `u16` / `bool` | JSON number / boolean; `u64` is written and read at full precision (values above 2⁵³ are legal) |
| `Option<T>` | `null` or the value; the key is always **present** (`"gateway":null`) — no stored type uses `skip_serializing_if`. A field on the `STO-30` default list additionally *decodes* an absent key (a row written by an older build) as its default; the current build still writes the key |
| `Vec<T>` | JSON array |
| `InviteCode` (only in `CandidateRecord.invite`) | the invite's `fed1…` string; an unparsable string is a `Permanent` decode failure |

No stored type carries `rename`, `rename_all`, `tag`, `untagged`, `flatten` or `skip_serializing`
(`MoveMeta`, which rides the op-log rather than the journal, is the one exception: `STO-33`).
There are exactly thirteen tags:

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
| `0x0d` | `++ utf8(new_key)` | the old key: the ordinary envelope around a JSON string, `{"version":1,"data":"<old_key>"}` |

`status_byte`: Pending 0, Executing 1, Done 2, Awaiting 3, Failed 4. The `journal.rs` header
lists seven of these; the constants block is the truth. `be64` is big-endian `u64::to_be_bytes`;
a `0x06`/`0x07` value that is not exactly eight bytes is a `Permanent` "corrupt ledger seq
index"/"ledger counter is corrupt" error.

**STO-6** Correlation and idempotency keys are strings with these shapes, and repair
(`STO-24`) classifies rows by their prefix:

| Shape | Producer |
|---|---|
| `move:<from>:<to>:<occurrence>` | allocator |
| `move:<from>:<to>:<amount>:<fee_cap>:<occurrence>` | user move (`Runtime::move_key`), **and both active-probe legs**, whose `<occurrence>` is derived from the session nonce (below) |
| `evac:<from>:<to>:<occurrence>` | allocator, scheduler |
| `refuse:<reason>:<fed>:<occurrence>` | allocator refusal |
| `conflict-suppressed:<candidate-key>:<reason>` | allocator, suppressed candidate |
| `tick-drop:<occurrence>:<decision-key>` | commit-time drop (`ALC-34`; row shape `STO-15`) |
| `pay:<payment_hash>` | user pay |
| `recv:<to>:<amount>:<nonce>` | user receive |
| `dinflow:<to>:<amount>:<nonce>` | daemon direct-inflow |
| `direct-inflow:<to>:<amount>:<fee_cap>:<occurrence>` | `Runtime::direct_inflow` (occurrence form; reached only by tests — the standalone verb builds `dinflow:`) |
| `join:<fed>:<sha256(invite)>` | intent-backed join |
| `join:<fed>:<nonce>` | agent auto-join ledger row |
| `recover:<fed>:<sha256(invite)>` | recovery |
| `tick:<occurrence>:<nonce>` | one per tick invocation |
| `probe:<fed>:<nonce>` | active probe umbrella row |
| `discover:<label>[:<index>]:<nonce>`, `autojoin:<nonce>`, `approve:<fed>:<nonce>` | discovery |
| `watch-probe-skip:<candidate>:<spending>:<amount>:<bucket>` | scheduler (repair classifies it as a discovery row, `STO-24`) |

Placeholder encodings, all fixed: `<from>`, `<to>`, `<fed>`, `<candidate>`, `<spending>` are
the 64-char lowercase hex of the 32-byte `FederationId`; `<amount>` and `<fee_cap>` are
decimal millisatoshis and `<bucket>` is decimal milliseconds; `<occurrence>`
is the decimal `u64`; `<payment_hash>` and `<sha256(invite)>` are 64-char lowercase hex, the
latter of SHA-256 over the invite string's UTF-8 bytes; `<index>` is a decimal source index;
`<label>` is the discovery source's label; `<reason>` is the `ReasonCode` **tag** owned by
`ALC-51` (`allocator::reason_tag`'s snake_case form, e.g. `spending_below_target`), not the
PascalCase JSON value of the stored `reason` field; `<bucket>` in `watch-probe-skip:` is
`budget_skip_diagnostic_bucket_ms`: the probe budget's reset time in ms when one is known, else
`floor(now_ms / 604_800_000) × 604_800_000` (the rolling 7-day window of `ALC-26`), so repeated
skips of one `(candidate, spending, amount)` within a window dedup to one row. The user-move
shape doubles as the active-probe leg key (`ALC-27`).

A **generated** `<nonce>` is **32 lowercase hex characters** — 16 random bytes
(`Runtime::ledger_nonce` takes the first 16 bytes of a fresh `OperationId`; the daemon's
`handlers::nonce` and the CLI's `cli_nonce` fill 16 bytes from `rand::thread_rng`). A
**caller-supplied** nonce on `recv:`/`dinflow:` keys (`POST /v1/receive`, `/v1/direct-inflow`,
and the standalone `receive`/`direct-inflow` verbs) is any non-empty string of URL-unreserved bytes (`API-36`) embedded
verbatim — `foo` is a valid nonce, and the CLI's `direct-inflow` default is the literal `0`
(`API-41`) — so a parser of stored keys MUST NOT assume the fixed width there. A
`ProbeSession.nonce` (`STO-26`) is the generated
shape, and both probe legs' `move:` keys embed `occurrence_from_nonce(nonce) =
u64::from_str_radix(&nonce[..16], 16)` — the first sixteen hex characters read as a big-endian
`u64` — so the leg keys are reconstructible from the session alone. Probe legs are **not**
namespaced away from user moves: `MoveRequest.occurrence` accepts any `u64`, so a user move with
the same endpoints, amount and cap and an occurrence equal to a nonce head would attach to that
leg. The separation is probabilistic (a random 64-bit head against the small occurrences users
and the scheduler actually supply), not excluded (`DOM-16`); that is a defect, not a settled
shape, and namespacing it moves this key shape — but not `STO-24`'s `classify_key`, which already
sends an unrecognised prefix to the never-repaired class (`F44`).

`pay:` keys carry the payment hash and no nonce, so paying the same invoice twice attaches to
one operation (`OPS-8`). `docs/operation-history-spec.md` §2's `pay:<fed>:<nonce>` is not
what is built. The key an intent writes into the **SDK op-log** differs from its journal key
after a manual retry: `STO-34`.

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

**STO-9** The `Intent` row (`0x01`), field by field (`?` marks `Option<…>`; per `STO-5` every
key is present, `null` when `None`, except the one `serde(default)` field):

| Field | Type | Source |
|---|---|---|
| `idempotency_key` | `IdempotencyKey` | must equal the row key or the read is `Permanent` |
| `attempt` | `u32` | `0` on first admission; `+1` per `retry_failed_intent` |
| `action` | `Action` (`DOM-7`) | |
| `max_fee` | `Msat?` | `action.fee_cap()`: the action's `fee_cap` for `DirectInflow/Move/Evacuate/Pay/Receive`, `None` for `Join/Recover` |
| `status` | `IntentStatus` | |
| `reason` | `ReasonCode` | the decision's reason; user verbs `UserInitiated` |
| `actor` | `Actor` | |
| `created_at_ms` | `u64` | unix millis at admission; becomes the ledger row's `created_at_ms` |
| `operation_id` | `OperationId?` | raw `Pay`/`Receive` artifact |
| `invoice` | `Invoice?` | raw `Receive` artifact |
| `evacuation_refusal` | `EvacuationRefusalEvidence?` | `#[serde(default)]`; `{cap_components: EvacFeeCap {base_msat, bps}, requested_net: Msat, source_spendable: Msat, low: EvacuationQuoteSample, high: EvacuationQuoteSample, diagnostic: String, measured_at_ms: u64}` with `EvacuationQuoteSample {delivered_net: Msat, total_fee: Msat, fee_cap: Msat}` (`DOM-19`) |

The store enforces: the attempt matches on every write (`upsert`, `set_status`,
`reset_retryable` reject a mismatch as `Permanent`; `set_status_if`, `put_move_if_attempt`,
`complete_recovery` and every other `*_if*` writer return `false`); the transition table
`Pending→any, Executing→any, Awaiting→{Awaiting,Done,Failed}, Done→Done, Failed→Failed`
(`OPS-2`); `Failed→Pending` only via `retry_failed_intent`, which requires the caller's row to
be `Pending` with exactly `attempt + 1`, refuses a superseded parent (a `0x0c` row under the
key), moves the `0x04` entry from `Failed` to `Pending`, deletes the `0x02` move row, and
appends a **fresh** ledger row repointing `0x06` (so a crashed attempt and its retry are two
truthful rows). Two writers touch `evacuation_refusal`: `set_status_if(Pending→Executing)`
clears it to `None` (a fresh claim consumes the planning handoff), and `reset_retryable`
(requires `Executing`, writes `Pending`) sets it to exactly the evidence the caller supplies
(`None` clears).

**STO-10** The pending index `0x04` holds `Pending`, `Executing`, `Awaiting` and `Failed`
intents; **`Done` is never indexed**, which is what makes finished work unscannable. Index and
intent row move in one transaction. `Journal::pending()` returns `[Pending, Executing]` — it
**includes `Executing`** — tolerating corrupt entries; `awaiting()` returns `[Awaiting]` and is
never re-driven; `reservation_intents()` returns `[Pending, Executing, Awaiting]` and **fails
closed** on any corrupt entry, so admission stops rather than under-reserve.

**STO-11** `MoveRecord` (`0x02`, `wallet_core::MoveRecord`) is **partially** rebuildable,
contrary to both the ledger spec ("derived, rebuildable") and `AGENTS.md` ("cannot be
re-created"). Its fields, in order:

| Field | Type | Note |
|---|---|---|
| `key` | `IdempotencyKey` | must equal the row key (`put_move_if_attempt` rejects a mismatch as `Permanent`) |
| `from` | `FederationId?` | `None` for a receive-only `DirectInflow` |
| `to` | `FederationId` | |
| `amount` | `Msat` | the executed net (`STO-17`) |
| `fee_cap` | `Msat` | the cap actually enforced; for an `Evacuate` recomputed from `fee_cap_components` at the sized net |
| `gateway` | `GatewayUrl` | **required**, pinned at creation |
| `send_required` | `bool` | `true` for `Move`/`Evacuate`, `false` for `DirectInflow` |
| `invoice` | `Invoice?` | |
| `recv_op` | `OperationId?` | |
| `send_op` | `OperationId?` | |
| `phase` | `MovePhase` | `Created, Invoiced, Sending, Settled, Refunded, Failed, Stranded` |
| `outcome` | `String?` | terminal diagnostic; the ledger's `error` fallback (`STO-16`) |
| `preimage` | `Preimage?` | |
| `receive_fee_quoted` | `Msat?` | |
| `send_fee_quoted` | `Msat?` | |

From the fedimint op-log (`STO-33`) the executor can rebuild `recv_op`, `send_op`, `invoice`,
`amount` and `fee_cap`; the terminal `phase`, `preimage`, `outcome`, and both quoted fees
survive only in this row. A move has an **artifact** (`executor::has_move_artifact`) exactly when
`invoice`, `recv_op` or `send_op` is `Some`. An intent-backed move record may be written only
through `put_move_if_attempt`, which fences on attempt, non-terminal status and no active
refusal marker, re-inserts the unchanged intent bytes as a write-write fence, and after commit
re-verifies ownership in a second transaction: if the intent is gone, on another attempt, or
now terminal/marked, and the `0x02` bytes are still the ones it wrote, it restores the prior
`0x02` bytes (same attempt, retired) or deletes the row (any other loss) and returns `false`.

**STO-12** `WatchState` (`0x0a`): `occurrence`, `last_discover_ms`, `discover_cursor?`,
`discover_backlog`, `discover_rotation`. `advance_watch_occurrence` does a checked `+1` on the
stored value and fails only when that value is already `u64::MAX`, so it can write `u64::MAX`
once; `observe_watch_occurrence` does a `max` and rejects an **input** of `u64::MAX` before
writing. `u64::MAX` is the fail-closed value (`ALC-33`).

**STO-13** `Policy` (`0x0b`): twenty-eight fields (`API-27`), seeded insert-if-absent by `walletd
init` and again at actor start, which then validates the stored row and refuses to start on an
invalid one. `put_policy` is a plain overwrite; **the journal does not validate** — the actor
does, and the HTTP handler rejects unknown keys (`API-20`). The stored type is permissive
(`STO-31`); `max_fee_bps_of_move`, `evac_fee_base_msat` and `evac_fee_bps` carry named serde
defaults because rows written before them exist: `300`, `Msat(200_000)` and `300`. The seeded
row is `Policy::default()`: `per_fed_cap 1_500_000_000`, `spending_target 500_000_000`,
`standby_target 150_000_000`, `max_fee 200_000` (all msat), the three defaults above,
`spending_fed`/`standby_fed` `null`, `probe_min_span_secs 86_400`, `probe_min_successes 3`,
`probe_ttl_secs 604_800`, `probe_amount 20_000`, `max_probe_attempts_per_week 10`,
`max_probe_spend_per_week 500_000`, `base_interval_secs 600`, `min_interval_secs 30`,
`evacuation_lead_secs 3_600`, `discover_every_secs 21_600`, `probe_retry_backoff_secs 3_600`,
`probe_refresh_lead_secs 43_200`, `max_auto_joins_per_week 5`, `auto_join_lifetime_cap 20`,
`max_candidates_per_pass 256`, `per_preview_timeout_secs 20`, `discover_pass_deadline_secs 60`,
`auto_join false`, `require_mainnet true`. Field types: every `*_msat`/`_cap`/`_target`/`_amount`/
`_spend_*`/`max_fee` is `Msat`; `max_fee_bps_of_move` and `evac_fee_bps` are `u16`; the two pins
are `FederationId?`; `probe_min_successes`, `max_probe_attempts_per_week`,
`max_auto_joins_per_week`, `auto_join_lifetime_cap`, `max_candidates_per_pass` are `u32`; every
`*_secs` is `u64`; the two flags are `bool`.

**STO-14** The federation registry (`0x03`) is written by a plain overwrite after the client
partition exists and before the in-memory client is inserted. `get_federation` fails closed on
a corrupt row; `list_federations_report` skips a malformed key or undecodable value, counts it
in `skipped_rows`, and warns. Three planning surfaces MUST treat `skipped_rows > 0` as "the
world is unknown" rather than plan from the healthy subset (`ALC-46`); explicit user and admin
verbs keep the poison-tolerant list.

## The operation ledger

**STO-15** `OperationRecord` (`0x05`): `seq: u64, correlation_key: IdempotencyKey, kind:
OperationKind, actor: Actor, reason: ReasonCode, status: OperationStatus ∈ {Started, Awaiting,
Succeeded, Failed}, created_at_ms: u64, updated_at_ms: u64, fees: FeeBreakdown {fee_cap: Msat?,
receive_fee: Msat?, send_fee_quoted: Msat?}, error: String?, repaired: bool`. `reason` is
mandatory and stored as the PascalCase variant (`"UserInitiated"` on user verbs; the
`user_initiated` spelling is the key tag of `STO-6`). `Actor` is `"User"` or
`{"Agent":{"occurrence":<u64>}}`. `OperationKind` has twelve externally-tagged variants:

| Variant | Fields (in order) |
|---|---|
| `Join` | `fed: FederationId` |
| `Recover` | `fed: FederationId` |
| `Receive` | `fed: FederationId, amount_invoiced: Msat` (gross), `op_id: OperationId?, gateway: GatewayUrl?` |
| `Pay` | `fed: FederationId, invoice_amount: Msat?, payment_hash: [u8;32]?, op_id: OperationId?, gateway: GatewayUrl?` |
| `DirectInflow` | `to: FederationId, amount: Msat, recv_op: OperationId?, gateway: GatewayUrl?` |
| `Move` | `from: FederationId, to: FederationId, amount: Msat, send_op: OperationId?, recv_op: OperationId?, gateway: GatewayUrl?, evacuation: bool` (`true` for an `Action::Evacuate`) |
| `Refusal` | `fed: FederationId, diagnostics: RefusalDiagnostics` (`#[serde(default)]`) |
| `Probe` | `fed: FederationId, from: FederationId, amount_msat: Msat, cost_msat: Msat?` |
| `Tick` | `occurrence: Occurrence, decisions: u32, performed: u32, failed: u32` |
| `Discover` | `source: DiscoverySource ∈ {Observer, Nostr, Manual}, status: SourceStatus ∈ {"Ok", {"Failed": String}}, found: u32, structurally_passed: u32, rejected: u32` |
| `AutoJoin` | `considered: u32, joined: u32, blocked_concurrent: u32, blocked_weekly: u32, blocked_lifetime: u32` |
| `Approve` | `fed: FederationId` |

`kind_from_action` seeds an intent-backed row: `Move`/`Evacuate → Move` (op-ids and gateway
`None`, `evacuation` per variant), `DirectInflow → DirectInflow`, `Pay → Pay {fed: from,
invoice_amount: Some(amount), payment_hash: Some(hash), op_id: None, gateway}`, `Receive →
Receive {fed: to, amount_invoiced: amount, op_id: None, gateway}`, `Join/Recover → {fed:
federation}`, `RefuseInflow → Refusal`. A refusal row MUST carry the figures that produced it in
`diagnostics` (`DEF-2`): `RefusalDiagnostics {source: FederationId?, want: Msat?, available:
Msat?, source_spendable: Msat?, max_fee: Msat?, max_fee_bps: u16? (serde default), cap_room:
Msat?, amount: Msat?, conflict_suppressed: bool (serde default), min_move: Msat?}`.
`RefusalDiagnostics` compares equal to any other, so refusal identity is `(fed, reason)`. A
commit-time drop of an executable decision (`ALC-34`, `record_tick_dropped_refusal`) is also
an `OperationKind::Refusal` row: keyed `tick-drop:<occurrence>:<decision-key>`, `fed` is the
decision's `Move.to` or `Evacuate.from` — the only two arms a stored row can take, since `decide`
emits no `DirectInflow`, `Join`, `Recover`, `Pay` or `Receive` (`ALC-4`) and a refusal never
reaches commit; the writer's match covers all eight variants anyway
(`DirectInflow.to` / `Receive.to` / `Pay.from` / `Join.federation` / `Recover.federation` /
`RefuseInflow.fed`) — `actor Agent{occurrence}`, `reason` = the dropped
decision's reason,
**`status Succeeded`** (the drop itself succeeded; the money did not move), `fees` default,
`error = "commit-time admission refused <decision-key>: <message>"`, `repaired false`, and
`diagnostics` all-`None` except that a nonzero conflict-suppressed candidate records
`amount: Some(0)` and `conflict_suppressed: true`. It is create-if-absent (a re-drop of the
same key is a no-op).

**STO-16** Write discipline, enforced by the pure `advance` function and one writer
(`ledger_upsert_in`): create on first observation; update only to advance status (rank
`Started < Awaiting < terminal`) and fill fields; a terminal row returns `None` to any further
write **except** that a terminal row written by repair (`repaired: true`) may be superseded
exactly once by an authoritative write. **Nothing deletes a `0x05`, `0x06` or `0x07` key.**
Every intent status change writes its ledger row in the same transaction (`upsert`,
`set_status`, `set_status_if`, `reset_retryable`, `complete_recovery`,
`replace_marked_evacuation`, the artifact and raw-terminal writers), so ledger and journal
cannot disagree about an intent-backed operation.

The pure `advance(record, new_status, now_ms, upd?, error?, write_kind)` rule, exactly:
(1) a terminal stored row returns `None` unless `record.repaired && write_kind ==
Authoritative`; (2) a non-terminal row returns `None` when `rank(new_status) <
rank(record.status)`; (3) otherwise the copy gets `status = new_status`, `updated_at_ms =
now_ms`, `repaired = (write_kind == Repair && new_status is terminal)`; (4) `upd` fills the
kind's op-ids/gateway/amount/hash only where the incoming value is `Some` (a `None` never
clobbers), and `fees` merge field-wise the same way **except** when `upd.fees_definitive` is
set, in which case `receive_fee` and `send_fee_quoted` are replaced outright (a settlement that
could not derive a fee clears a stale estimate) while `fee_cap` still merges; (5) `error`: if
the stored row was terminal (the repaired supersession) or the status changes, `error` is set
to **exactly** the incoming value — `None` clears a prior diagnostic; on a same-status
enrichment an incoming `Some` overwrites and a `None` leaves the stored text. `seq`,
`correlation_key`, `actor`, `reason`, `created_at_ms` and the kind's identity fields never
change. The intent-backed writer (`write_intent_ledger_row`) maps `status_from_intent`
(`Pending|Executing → Started`, `Awaiting → Awaiting`, `Done → Succeeded`, `Failed → Failed`),
passes `upd = None`, and passes an `error` only when the mapped status is `Failed`: the
caller's diagnostic if any, else `MoveRecord.outcome`, else `None`. A fresh intent-backed row
(`fresh_intent_record`) takes `created_at_ms` from `Intent.created_at_ms` (not the clock),
`updated_at_ms = now`, `fees.fee_cap = Intent.max_fee`, `repaired = false`. `record_started`
never advances an existing row (it is a create-if-absent); `record_update` chooses its target
status as: `op_id` newly supplied and the row is `Started` (or a repaired terminal) →
`Awaiting`; repaired `Failed` with any enrichment → `Started`; repaired terminal with no
enrichment → no write; otherwise the current status (pure enrichment).

**STO-17** On every intent-backed ledger write the row refreshes op-ids, gateway and quoted
fees from the `0x02` record, and — once a move artifact exists (invoice, receive op or send op,
`STO-11`) — refreshes **both** `amount` and `fee_cap` to the executed amount and the enforced
cap (`DEF-4`). Refreshing one without the other is forbidden: an auditor recomputing the cap
from a planned amount would derive a number nobody enforced. Precisely (`refresh_from_move`):
on a `Move` kind `send_op`/`recv_op` are copied when `Some` on the record, `gateway` is set to
`Some(record.gateway)` unconditionally, and `amount`/`fees.fee_cap` are stamped only when the
artifact exists; a `DirectInflow` kind is treated identically — `recv_op` copied when `Some`,
`gateway` set unconditionally, `amount`/`fees.fee_cap` stamped once the artifact exists, so the
`DEF-4` pairing holds there too; on every kind
`fees.receive_fee`/`fees.send_fee_quoted` are copied when `Some`. After that,
`refresh_from_intent_artifact` sets a `Pay`/`Receive` kind's `op_id` to `Intent.operation_id`
whenever that is `Some`.

**STO-18** Sequence assignment is fenced in O(1): before any fresh append the counter `0x07`
must equal `tail_seq + 1`, where the tail is the lexicographically greatest `0x05` key, which
must be a canonical nine-byte key whose embedded `seq` matches. Any disagreement is a
`Permanent` error that fences **every** fresh append, user and agent, with an operator message
to restore from backup. An absent `0x07` reads as `0`; a nonzero counter over an empty ledger,
a tail key that is not exactly nine bytes, or a tail `seq` of `u64::MAX` are all the same fence.
The first row of a fresh store is `seq 0`. The counter is exhausted at `u64::MAX`.

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
`pay:`/`recv:` rows are observed from the op-log by correlation key (the op whose `custom_meta`
`correlation_key` equals the **attempt** correlation key of `STO-34` — the backing intent's
`operation_correlation_key()`, which is the row's key only on attempt 0 and
`retry:<len>:<key>:<attempt>` after a manual retry; a raw row with no backing intent falls
back to the row's key — the historical `Runtime`-direct raw writers, test-only or legacy today,
since every standalone money verb goes through the actor, `OPS-5`), else by payment hash (a repair write with a
dedup note; an in-flight or failed original is never adopted for a later attempt), else after
one hour soft-Failed "never reached the federation"; `tick:` and discovery rows older than one
hour → soft-Failed "interrupted". The classes are decided by key prefix alone
(`classify_key`): `join:` → join; `tick:` → tick; `discover:`, `autojoin:`, `approve:`,
`watch-probe-skip:` → discovery; `pay:`, `recv:` → raw; everything else (move-shaped intent
rows, `recover:`, `refuse:`, `probe:`, `dinflow:`, `direct-inflow:`, …) is never repaired. The
verbatim strings repair writes, which `STO-35` freezes, are:

| Constant | Stored `error` text |
|---|---|
| `JOIN_SUPERSEDED` | `superseded by a later join attempt` |
| `JOIN_NOT_REGISTERED` | `join did not complete — federation not in the registry; re-run join` |
| `JOIN_AMBIGUOUS_NOTE` | `overlapping attempts; correlation uncertain — membership itself is registry-proven` |
| `INTERRUPTED_NO_TERMINAL` | `interrupted — no terminal report` |
| `RAW_NEVER_REACHED` | `never reached the federation` |
| `HASH_DEDUP_NOTE` | `correlated by payment hash to an existing payment of this invoice; attempt-level correlation uncertain (deduped retry or never-sent attempt); the matched operation is authoritative` |

When a note accompanies an op's own terminal error the stored text is `"{note} ({err})"`
(`combine_note`). A raw terminal repair re-drives the intent sink except for the never-reached
case, which is recognised by `status == Failed && repaired && error == RAW_NEVER_REACHED`
exactly.

**STO-25** Evacuation supersession writes two sidecars in the same transaction as the exchange:
`0x0c old_key → {old_key, old_attempt, new_key, new_attempt, old_occurrence, occurrence, source,
old_cap_components?, new_cap_components?, refusal, superseded_at_ms}` and `0x0d new_key →
old_key`. A superseded parent can never be retried; a child's namespace must be empty across
`0x01/0x02/0x06/0x0c/0x0d` and all five `0x04` status keys before creation; at most one live
Agent evacuation per source may exist at exchange time; the replay path validates that both
sidecars exist and agree before returning success without writing (`OPS-30`).

**STO-26** `ProbeRecord` (`0x08`): `{attempts: Vec<ProbeAttempt>, in_flight: ProbeSession?}`
(both keys always present). `ProbeAttempt` is `{at_ms: u64, ok: bool, from: FederationId,
amount_msat: u64, leg_fee_cap_msat: u64, error: String?}` (`DOM-13`; note the plain `u64`
fields, not `Msat`). `attempts` is chronological append order and is pruned
(`prune_probe_attempts`) in two steps on every outcome write — first keep every attempt with
`now_ms − at_ms ≤ 604_800_000` (the **constant** 7-day `ProbePolicy::default().ttl_ms`, never
`Policy.probe_ttl_secs`), the newest attempt, and per `from` source the newest `ok` attempt and
the newest **default-qualifying** attempt, where default-qualifying means `ok && amount_msat ≥
20_000 && leg_fee_cap_msat ≤ 10_000` (`PROBE_AMOUNT_MSAT`, `PROBE_LEG_FEE_CAP_MSAT`); then keep
only the newest `PROBE_HISTORY_CAP = 256`, which **overrides** the keep rules when more than 256
survive them. `ProbeSession` is `{nonce: String (32 lowercase hex, STO-6), from: FederationId,
amount_msat: u64, leg_fee_cap_msat: u64, c_spendable_before_in_msat: u64, out_net_msat: u64?,
started_at_ms: u64}`: written before leg IN is journaled, updated with `out_net_msat` before
leg OUT is journaled, and cleared in the same transaction that appends the attempt
(`record_probe_outcome`). The nonce is exclusive: an outcome whose nonce differs from the
stored session, or arrives when no session is stored, is ignored (`false`, warning), and the
umbrella ledger row is then not touched either.

`CandidateRecord` (`0x09`): `{id: FederationId` (must equal the key or the read is
`Permanent`)`, invite: InviteCode` (string form, `STO-5`)`, source: DiscoverySource,
discovered_at_ms: u64, structural: StructuralOutcome ∈ {"Passed", {"Rejected": String}},
structural_checked_at_ms: u64, state: CandidateState ∈ {Rejected, Discovered, AutoJoined,
UserApproved}, updated_at_ms: u64}`. `put_candidate` cannot demote `UserApproved`: when the
stored row is `UserApproved` and the incoming one is not, the write keeps `UserApproved` and
`max(incoming.updated_at_ms, stored.updated_at_ms)`; every other field is overwritten.
`approve_auto_joined_candidate` only promotes from `AutoJoined` (absent or any other state is
`Permanent`), sets `updated_at_ms`, and writes the `Approve` ledger row (`actor User`, `reason
UserInitiated`, `status Succeeded`, create-if-absent) in the same transaction. Seed recovery
(`complete_recovery` → `write_recovered_user_ownership`) promotes **any** state including
`AutoJoined` to `UserApproved`, leaves an existing `UserApproved` untouched, and replaces an
unreadable row with `{source: Manual, structural: Passed, state: UserApproved}` stamped `now`.

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
`ProbeRecord`, `CandidateRecord`, `WatchState`, `Policy`, `EvacuationSupersessionRecord` —
**and every type serialized inside one of them**, transitively (`FeeBreakdown`,
`RefusalDiagnostics`, `EvacuationRefusalEvidence` and its samples, `ProbeAttempt`,
`ProbeSession`, `EvacFeeCap`, …), because a field added to a nested type is a field
added to the row — plus `MoveMeta`, which rides the SDK operation log rather than the journal
(`STO-33`). A type is on this list because the daemon writes it, directly or embedded
(`DEF-13`).

**STO-30** A field added to a type on that list — including to an already-shipped variant —
MUST carry `#[serde(default)]`, with a **named** default function for a numeric field, because a
bare default yields zero (`DEF-10`; a zero evacuation cap is a livelock). As built the fields
carrying it are `Intent.evacuation_refusal`, `Action::Move.gateway`,
`Action::Evacuate.{gateway, fee_cap_components}`, `OperationKind::Refusal.diagnostics`,
`RefusalDiagnostics.{max_fee_bps, conflict_suppressed}`, and the three `Policy` fields in
`STO-13` — ten in the journal — plus `MoveMeta.fee_cap` and `MoveMeta.from`, which ride the
SDK op-log's `custom_meta` in `client.db` (`OPS-25`): twelve. Each SHOULD be pinned by a test that strips the key from the serialized type and re-reads it.
Ten are: `Refusal.diagnostics`, `Move.gateway`, `Intent.evacuation_refusal`, both
`RefusalDiagnostics` fields, the three `Policy` fields, and both `MoveMeta` fields. Two are not:
the `Evacuate` defaults, which one bare-`Action` fixture omits both at once (`CNF-18`, `F41`).

**STO-31** No type on that list may carry `#[serde(deny_unknown_fields)]` (`DEF-11`): a row
written by a newer build must stay readable by the previous build or a rollback cannot start.
As built, none does; every `deny_unknown_fields` in `wallet-api` is a request-only DTO.

**STO-32** The ledger is greenfield in one respect only: `OperationRecord.repaired` and
`WatchState.discover_rotation` carry no default, on the claim that no row predates them. That
claim is unverified against the long-running deployment's store (`HST-23`).

## The op-log side: what the wallet writes into `client.db`

**STO-33** Every lnv2 `receive`/`send` the move executor commits carries a `MoveMeta` as the
operation's `custom_meta` (a `serde_json::Value` fedimint stores atomically with the op).
Its JSON is:

| Key | Type | Serde rule |
|---|---|---|
| `move_id` | `String` | the attempt's operation correlation key (`STO-34`): equal to the intent key on attempt 0 |
| `role` | `"send"` \| `"receive"` | `MoveRole`, `#[serde(rename_all = "lowercase")]` — the one lowercase enum in the system |
| `amount` | `u64` msat | the net the destination should receive, after any evacuation down-sizing |
| `fee_cap` | `u64` msat | `Option`, `#[serde(default, skip_serializing_if = "Option::is_none")]`: **omitted** when `None`, never `null`; absent decodes as `None` and reassembly falls back to the intent's planned cap — never to zero |
| `from` | `[u8;32]` | `Option`, same omission rule; absent for a `DirectInflow` |
| `to` | `[u8;32]` | required |

Receive ops additionally carry `receive_contract_quoted` (`u64` msat): the exact contract
amount the quote solver expected before minting, written by
`MoveMeta::receive_value_with_contract_quote` and read back by
`receive_contract_quote_from_value` (absent means a pre-guard op, malformed is corruption);
send ops never carry it, and `MoveMeta::from_value` ignores it as an unknown key.
`MultiClient::backfill_ops` (via `op_artifact_from_meta`) recognises a move op by the
**presence of the `move_id` key** alone: an op without it is skipped silently; an op with it whose value fails to decode as a
`MoveMeta` is corruption (warned and skipped). The leg is taken from the SDK's
`LightningOperationMeta::{Send, Receive}` variant, which is authoritative over `role`.

**STO-34** A raw `Pay` writes `custom_meta = {"role":"send","correlation_key":"<k>"}` and a raw
`Receive` writes `{"role":"receive","correlation_key":"<k>"}`, where `<k>` is
`Intent::operation_correlation_key()`: the intent's `idempotency_key` when `attempt == 0`,
and otherwise `retry:<len>:<key>:<attempt>` with `<len>` the decimal byte length of the
idempotency key, `<key>` the idempotency key verbatim and `<attempt>` the decimal attempt
number (a `pay:` key is 68 bytes, so its first retry is `retry:68:pay:<64 hex>:1`). The same
value is `MoveMeta.move_id` for a move attempt.
Repair's primary op-log lookup (`STO-24`) and every backfill therefore match on this
per-attempt key, so a retry can never adopt the operation that terminally failed the previous
attempt. Neither meta object carries any other key; both are recognised as non-move ops by the
absence of `move_id` (`STO-33`).

**STO-35** Ledger `error` text is persisted verbatim and never re-derived on read: the executor
diagnostic, `MoveRecord.outcome`, a repair note, or `"{note} ({err})"` is stored as written.
It can be *rewritten* only by the writes `STO-16` permits — a repaired soft terminal (such as
`RAW_NEVER_REACHED`) is superseded exactly once by an authoritative write that replaces or
clears the error when SDK evidence arrives, and a same-status non-terminal enrichment may
overwrite it; an unrepaired terminal row's `error` is immutable. Every wording ever written to
a row that reached an unrepaired terminal therefore lives in the store forever, and any
classifier that reads `error` — `is_agent_new_partition_join`
(`JOIN_NOOP_REOPEN_NOTE = "already joined (concurrent/prior); no-op re-open"`, which excludes a
`Succeeded` agent `Join` row from the weekly and lifetime auto-join counts),
`raw_terminal_repair_must_not_sink_intent` (`RAW_NEVER_REACHED`), and the operator reading
history — MUST match the stored text exactly as listed in `STO-24`, and a change to any of
these constants MUST keep matching the legacy wording or it silently changes the money
accounting of rows already on disk.
