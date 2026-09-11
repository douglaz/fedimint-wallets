# 04 — API contract

The daemon's HTTP surface and the CLI that speaks it, as built. Every route, field, status code
and exit code below is read out of `wallet-daemon/src/{server,handlers,error}.rs`,
`wallet-api/src/lib.rs` and `wallet-cli/src/{main,client,render,exit}.rs` on 2026-09-07, re-read
against `7225114` on 2026-09-10. Where
a document elsewhere claims a parameter that does not exist, this document wins and
`11-open-findings.md` records the gap.

## Transport and authentication

**API-1** The daemon serves plain HTTP (`axum::serve` over a `TcpListener`, `server::run`).
There is no TLS, no rate limiting, no CORS handling, **no request or header timeout, no
connection cap, and no body limit beyond the `Json` extractor's default 2 MiB** (`API-35`) —
a client can hold a connection open indefinitely, and `POST /v1/reconcile`, which reads no
body, accepts one of any size.
The trust boundary is the operating-system user (`SEC-1`): anything that can read the token file
can do everything the API can do.

**API-2** Every route, including `/v1/health`, requires `Authorization: Bearer <token>`. The
bearer middleware (`require_bearer`) wraps the whole router, so it runs before routing: an
unauthenticated request to an unknown path or unsupported method is `401`, never `404`/`405`.
The header value MUST begin with the seven bytes `Bearer ` (case-sensitive, exactly one space);
the remainder is compared byte-for-byte in constant time (`constant_time_eq`) against the token
the daemon read at start (`read_token`: file contents with surrounding whitespace trimmed, `SEC-2`).
A missing header, a non-`Bearer` scheme, a length mismatch or a wrong token all return `401` with
the body `{"kind":"unauthorized","refuse_reason":null,"operation_key":null,"message":"missing or
invalid bearer token"}` and **no** `WWW-Authenticate` header.

**API-3** The token is 32 random bytes, lower-hex (64 characters), written `0600` by `walletd
init`, which rotates it. Re-running `init` while the daemon runs blocks on the database lock
rather than rotating underneath it.

**API-4** The bind address defaults to `127.0.0.1:9736`. The address is a free-form string in
`walletd.toml`; `0.0.0.0` is accepted. Loopback is a default, not an enforced invariant
(`SEC-3`).

## Error envelope

**API-5** Every non-2xx response **produced by a handler** is

```json
{ "kind": "refused|failed|unauthorized|not_found|timeout",
  "refuse_reason": "<optional>", "operation_key": "<optional>", "message": "<text>" }
```

All four keys are always present: `refuse_reason` and `operation_key` are JSON `null` when
absent, never omitted. The router has no fallback, so an **authenticated** request to an unknown
path gets axum's default `404` with an empty body, and a known path with an unsupported method
gets `405` with an empty body and an `Allow` header — neither is this envelope (an
unauthenticated one is `401`, `API-2`).

`refuse_reason` is the `RefuseReason` enum, externally tagged with `snake_case` names (`STO-5`).
The nine unit variants serialise as bare strings: `"insufficient_after_reservations"`,
`"fed_held_by_probe"`, `"over_cap"`, `"budget_exhausted"`, `"amount_required"`,
`"storage_error"`, `"policy_invalid"`, `"policy_superseded"`, `"conflict"`. The one struct
variant serialises as an object: `{"sizing_conflict":{"field":"<name>"}}` with two emitted
values: `"amount"` from the daemon handler when a pay's `amount` disagrees with the invoice
(`API-18`), and `"request sizing"` from the actor's `validate_live_attach` when a live operation
key is re-submitted with **any** sizing field changed (`OPS-8`). That check covers every action
kind the actor admits — `Pay`, `Receive`, `Move`, `Evacuate`, `DirectInflow`, `Join`, `Recover`
(the match also has a `RefuseInflow` arm, unreachable because a refusal is never an intent,
`DOM-6`) — and compares each kind's whole identity (endpoints, `amount`, `fee_cap`, payment
hash, nonce, invite), not the fee cap alone; a `POST /v1/direct-inflow` re-sent with the same
`nonce` and a different `fee_cap` is refused here, because `dinflow:<to>:<amount>:<nonce>`
(`STO-6`) does not embed the cap. The actor sets that reason directly; the same reason also
arrives by substring from the core's own `validate_attach`, which runs after the actor's checks
(`OPS-8`, `OPS-39`). How an actor-side
`RefuseReason` is chosen from an `ExecError` message is
`OPS-39`.

**API-6** Status codes map as follows and nowhere else:

| Status | Kind | When |
|---|---|---|
| 401 | unauthorized | `API-2` |
| 404 | not_found | unknown operation key; unknown candidate on approve |
| 422 | refused | `policy_invalid`, `amount_required`, `sizing_conflict`; and every daemon-side request validation failure with no reason (bad invoice, `from == to`, unjoined federation, bad nonce, malformed JSON, bad query or path, unknown policy field) |
| 409 | refused | `insufficient_after_reservations`, `fed_held_by_probe`, `over_cap`, `budget_exhausted`, `storage_error`, `policy_superseded`, `conflict` |
| 409 | failed | a journaled terminal failure surfaced synchronously; carries `operation_key` |
| 503 | failed | shutting down, actor stopped, destination federation joined but not open (fresh key), a balance read failing on an open source federation during money-verb admission, or a `/v1/status` precondition (`API-15`) |
| 504 | timeout | a long-poll or invoice deadline elapsed; carries `operation_key` when the operation was admitted |
| 500 | failed | storage error (`API-37`) |

The two `refused` rows are `refused_status` and are exhaustive over the ten `RefuseReason`
variants — the nine `OPS-39` enumerates plus `amount_required`, which only the daemon handler
mints (`API-18`): `policy_invalid`, `amount_required`, `sizing_conflict` → `422`;
`insufficient_after_reservations`, `fed_held_by_probe`, `over_cap`, `budget_exhausted`,
`storage_error`, `policy_superseded`, `conflict` → `409`; every one of them carries
`"kind":"refused"` and no `operation_key`. (`storage_error` as a *refusal* is the actor's
fail-closed read during admission and is `409`; a handler-side storage fault is the `500`
`failed` of `API-37`.)

The `message` of every daemon-side `422` with no reason carries a fixed prefix by class:
`invalid JSON request body: <axum text>` (any `JsonRejection`, `API-35`), `invalid query
parameters: <axum text>` (any `QueryRejection`), `invalid path parameter: <axum text>` (any
`PathRejection`); the verb-specific validation messages are verbatim in `API-36`.

**API-7** A `refused` response with no `operation_key` means nothing was journaled. A `failed`
response with a key means the operation exists and terminalized. Callers MUST branch on the
key's presence, not the status code alone.

## Routes

**API-8** The complete route table. There are eighteen routes on seventeen paths.

| Method | Path | Returns |
|---|---|---|
| GET | `/v1/balance` | `{total, federations:[FederationView]}` |
| GET | `/v1/federations` | `[FederationView]` |
| GET | `/v1/history` | `{operations:[OperationView], next_before_seq}` |
| GET | `/v1/operations/{key}` | `OperationView` |
| GET | `/v1/status` | dry-run of the next tick (`API-15`) |
| GET | `/v1/watch/status` | `{occurrence, last_discover_ms, discover_cursor, discover_backlog}` |
| GET | `/v1/health` | `HealthView` (`API-16`) |
| POST | `/v1/pay` | 202 `{operation_key}` |
| POST | `/v1/move` | 202 `{operation_key}` |
| POST | `/v1/receive` | 200 `{operation_key, invoice}` |
| POST | `/v1/direct-inflow` | 200 `{operation_key, invoice}` |
| POST | `/v1/join` | 202 `{operation_key}` |
| POST | `/v1/recover` | 202 `{operation_key}` |
| POST | `/v1/approve` | 200 `{operation_key}` |
| GET | `/v1/candidates` | `[CandidateView]` |
| POST | `/v1/reconcile` | `{redriven, awaiters_rehydrated, executing_normalized}` |
| GET | `/v1/policy` | `Policy` |
| PUT | `/v1/policy` | `Policy` as stored |

### Reads

**API-9** `GET /v1/balance` sums the balances of every *open* federation. A federation that is
joined but failed to open is still listed with `balance: null` and is excluded from `total`.
The response is 200 regardless; a client that wants "every joined federation is open" must
check for nulls (the CLI does, and exits 1).

**API-10** `GET /v1/history` reads exactly two query parameters: `limit` (unsigned integer,
default 50, values above 500 are silently capped to 500 by `capped_history_limit`) and
`before_seq` (unsigned integer). `before_seq` is **exclusive**: the page holds rows with
`seq < before_seq`, newest first (`STO-19`). `next_before_seq` is the `seq` of the last row when
the page is full (`rows.len() == limit && limit > 0`), else `null`; pass it back as `before_seq`
for the next page. `limit=0` returns `{"operations":[],"next_before_seq":null}`. A
non-integer or negative value for either parameter is `422` `invalid query parameters: …`
(`API-6`). `HistoryQuery` does not deny unknown fields: **any other query parameter
(`status`, `actor`, `fed`, `kind`, …) is silently ignored and the page is unfiltered** — a
client MUST NOT infer filtering from a `200`. The CLI emulates actor and status filters by
paging client-side (`API-40`) and cannot filter by federation in client mode (`F27` for the
`?status=open` filter the web plan requires). Undecodable ledger rows are skipped without signal
on this route (`STO-19`).

**API-11** `GET /v1/operations/{key}` reads the ledger row for `key` and returns `404`
`no operation found for key <key>` if none exists. `wait` is a serde `bool`: absent means
`false`, the literal `true`/`false` are accepted, anything else is `422` `invalid query
parameters: …`. `?wait=true` first parks on the actor (`resolve_await`, target `Terminal`) and
only then reads the ledger row. That wait is decided against the **intent** store, not the
ledger: a key with no intent row (`refuse:`, `probe:`, `tick:`, `discover:`, `autojoin:`,
`approve:` keys are ledger-only, `STO-6`) is `404` under `wait=true` even though the same GET
without `wait` returns its row; "terminal" means the intent is `Done` or `Failed`, and an
already-terminal intent resolves immediately. The deadline is 60 s (`AWAIT_LONGPOLL_DEADLINE`),
after which the response is `504` `operation wait deadline elapsed` carrying the key. `show`
additionally projects `evacuation_refusal` and `evacuation_refusal_active` from the exact intent
(`OPS-31`); `history` does not, because it does no per-row intent lookup.

**API-12** `OperationView` carries: `seq, updated_at_ms, kind, status, amount?, receive_fee?,
send_fee_quoted?, actor, reason, operation_key, error?`, plus `superseded_by?, supersedes?,
refusal?, evacuation_refusal?, evacuation_refusal_active?` when present; the per-field types and
null-versus-omitted rules are the table in `API-33`, and the `refusal` /
`evacuation_refusal` object schemas are `API-34`. `kind` ∈ `join, recover,
receive, pay, direct-inflow, move, evacuation, refusal, probe, tick, discover, autojoin,
approve` (`kind_and_amount`). `status` ∈ `started | awaiting | succeeded | failed`. `actor` is
`"user"` or `"agent:<occurrence>"`. `reason` is the `reason_tag` vocabulary `ALC-51` owns
(eleven snake_case tags), shared with `/v1/status`; every user-verb row is `user_initiated`. That tag is
the snake_case form the `refuse:` key uses (`STO-6`); the **persisted** row stores
`ReasonCode` in its serde form, PascalCase (`"UserInitiated"`, `STO-5`), and only the
standalone `--json` renderings expose that form (`API-30`) — on the wire `reason` is always the
tag. **The enforced fee cap is not on the view** (`F9`), nor is a federation id.

**API-13** A recovery whose ledger row is `succeeded` (intent `Done`) but whose client handle is
not yet installed is reported as `started`, so a caller never observes "succeeded" alongside a zero balance
(`DEF-18`'s visibility window).

**API-14** `GET /v1/watch/status` is a pure journal read of `WatchState` (`STO-12`).

**API-15** `GET /v1/status` is a dry run of the next scheduler tick against the stored policy
at `occurrence + 1`. It returns `{spending_fed, standby_fed, decisions:[{operation_key, reason,
action}], scored:[{id, gated_eligible}], deferred:[{dest, source, reason, want_msat, floor_msat,
floor_source}]}`. Value domains: `spending_fed`, `standby_fed`, `deferred[].source` are
64-character lower-hex federation ids or `null`; `scored[].id` and `deferred[].dest` are the
same hex, never null (this is the one place a federation id is hex on the wire, `API-32`);
`decisions[].reason` and `deferred[].reason` use the `reason_tag` vocabulary of `API-12`;
`deferred[].floor_source` ∈ `protocol_min_move | route_min_viable`; `want_msat`, `floor_msat`
are unsigned integers; `gated_eligible` is a bool. **`decisions[].action` is explicitly
non-contractual**: it is `format!("{:?}", decision.action)`, the Rust `Debug` rendering of
`wallet_core::Action`, which changes whenever that enum does. A client MUST treat it as opaque
display text and MUST NOT parse it; the CLI prints it verbatim. It
returns **503** before the dry run when the runtime is absent, the federation registry reports a
skipped corrupt row (`status dry-run refuses an incomplete federation registry: …`), any joined
federation is unopened (`status dry-run requires every joined federation to be open; unopened:
<hex, …>. …`), the watch occurrence is at its
fail-closed maximum, or probing errors (`status probe failed: <error>`; the fences of `ALC-46`). This response shape is daemon-private, mirrored by hand in the CLI
client, and the CLI's mirror **drops `deferred`**: an operator using `wallet-cli status` in
client mode cannot see a withheld funding goal. Only the raw endpoint and the poller show it.

**API-16** `GET /v1/health` always returns 200 when authenticated. Body:
`{actor_queue_depth, inflight_drivers, scheduler_alive, automation_ready, automation_blocked?}`.
`scheduler_alive` is the scheduler loop's liveness flag. `automation_ready` is
`automation_blocked.is_none()`. `automation_blocked` is `{reason, detail}` (two strings) with
`reason` ∈ `cycle_failed | partial_federation_view | corrupt_federation_registry` (`ALC-45`).
All five keys are always emitted; `automation_blocked` is JSON `null` when ready, never
omitted. On decode the `wallet-api` type defaults a missing `automation_ready` to `true` and a
missing `automation_blocked` to `null`, so a client reading an older daemon MUST treat an
absent `automation_ready` as unknown, not healthy (`HST-22` does). `inflight_drivers` is
best-effort: a failed actor snapshot reports `0`, not an error. Liveness and
readiness are different answers, and a supervisor that reads only the status code learns
neither. The CLI's `health` verb prints the first three fields and **omits both readiness
fields**.

**API-17** `GET /v1/candidates` returns the candidate registry in raw key order with
`{id, invite, source, discovered_at_ms, structural, structural_checked_at_ms, state,
updated_at_ms}`; `source` ∈ `observer|nostr|manual`, `state` ∈
`discovered|autojoined|userapproved|rejected`, `structural` is `passed` or `rejected:<reason>`
where `<reason>` is the free-form `String` stored in `StructuralOutcome::Rejected` (the
scorer's `rejection_reason`, or the literals `IdMismatch` / `MissingModule`); it is not an
enumerated vocabulary. `id` is a federation-id array (`API-32`); `invite` is the canonical
`InviteCode` string; the three `*_ms` fields (`discovered_at_ms`, `structural_checked_at_ms`,
`updated_at_ms`) are unsigned integers and never null. Newest-first
ordering is a CLI convenience (`API-40`), not a wire property.

### Money verbs

Every money verb builds one `AllocatorDecision` with `reason: user_initiated`, `actor: User`,
samples the balance of every open federation it names — `pay`: `from`; `move`: `from` and `to`;
`receive` and `direct-inflow`: `to` — and submits it to the actor as one command (`OPS-5`,
`API-36`).
The response discards whether the admission was fresh or attached to an in-flight operation with
the same key; a caller cannot distinguish the two from the status code.

**API-18** `POST /v1/pay {invoice, amount?, fee_cap?, fed?}` (unknown fields rejected,
`API-35`; `fed` is a federation-id array, `API-32`). The handler runs, in order: read the
policy through the actor; parse the invoice (`parse_invoice`, failure is `422` `invalid BOLT11
invoice: <error>`); reconcile the amount; resolve `fee_cap` and `fed`; sample the source
balance (`API-36`); submit. An
amountless BOLT11 invoice is `422 amount_required` **whether or not** `amount` is supplied (the
lnv2 send API cannot supply an amount); a stated amount that disagrees
with the invoice is `422 sizing_conflict{amount}`, message `stated amount does not match the
invoice amount`. `fee_cap` defaults to the policy's `max_fee`;
`fed` defaults to the policy's spending federation, else the sole joined federation, else 422
(`resolve_fed`, messages in `API-36`). The source federation's **openness is not gated**: a
joined-but-unopened source samples no balance, the actor sees zero spendable, and the request
is `409 insufficient_after_reservations` — not `503`.
The operation key is derived from the payment hash, so paying the same invoice twice attaches
to the same operation (`OPS-8`). There is no gateway field on the wire (`ADR-0030`).

**API-19** `POST /v1/move {from, to, amount, fee_cap?, occurrence}`; `occurrence` is required
and, with the resolved `fee_cap`, is part of the operation key. A client that omits `fee_cap`
and retries after a policy edit therefore derives a *different* key and admits a second move;
the web plan's money forms exist to pin these values at render time (`F27`). Validation order:
`from == to` is `422` `move from and to must be different federations (from == to is a
no-op)`; then `from` and `to` are each checked against the registry (`ensure_joined`, `422`
`federation <hex> is not joined`); then the policy is read and the key derived. A destination
that is joined but not open is 503 **for a fresh key**; a replay of an
existing key attaches before that check runs and succeeds. As for `pay`, an unopened
**source** is not gated and surfaces as `409 insufficient_after_reservations` (`API-18`).

**API-20** `PUT /v1/policy` accepts a JSON object (a non-object body is `422` `policy must be
a JSON object`) and rejects any key not in the set derived at
runtime from `Policy::default()`'s serialization, with `422` `unknown policy field(s): <k1>,
<k2>` (unknown keys in **lexicographic** order — `serde_json::Map` is a `BTreeMap`; the
`preserve_order` feature is off — comma-space separated, checked before any field is
decoded). Exactly five missing fields are tolerated: `max_fee_bps_of_move` (300),
`evac_fee_base_msat` (200000) and `evac_fee_bps` (300) take their `serde(default)`, and the two
`Option` pins `spending_fed` / `standby_fed` decode as `null` when absent (serde's rule for a
missing `Option` field, `API-35`) — so a body that omits a pin silently **unpins** that
federation rather than being refused. Any other missing field, or a field of the wrong type, is
`422` `policy is not well-formed: <serde error>`. `spending_fed`/`standby_fed`
are federation-id arrays or `null` (`API-32`); the other twenty-six fields are unsigned
integers or booleans, all twenty-eight keys are always present in the `GET` and `PUT`
responses.
`Policy::validate` runs in the actor and refuses with `422 policy_invalid` naming the field.
The stored `Policy` type itself is permissive (`STO-31`); this handler is where strictness
lives, and it MUST stay here so the wire contract cannot drift from the type.

**API-21** `POST /v1/receive {to?, amount, fee_cap?, nonce}` and `POST /v1/direct-inflow` (same
shape). `nonce` is non-empty RFC 3986 unreserved characters (`validate_nonce`, verbatim
refusals in `API-36`) and is part of the key. `to` resolves exactly as `fed` does for `pay`
(explicit → policy `spending_fed` → sole joined → `422`). The daemon
admits the intent and then **blocks up to 30 seconds** (`INVOICE_MINT_DEADLINE`) for the
invoice artifact: terminal
without an invoice is `409 failed` with the key and message `the operation terminalized
without a payable invoice`; the deadline is `504 timeout` with the key and message `invoice
mint deadline elapsed; settlement continues asynchronously`.
Re-submitting the same key re-yields the same invoice (`OPS-8`). `receive` deducts fees from the
invoice; `direct-inflow` grosses the invoice up so the destination is credited `amount`, never
more and possibly less by a bounded receive-fee step (`FMI-15`).

**API-22** `POST /v1/join {invite}` and `POST /v1/recover {invite}` are 202 and asynchronous;
await them with `GET /v1/operations/{key}?wait=true`. The invite is parsed with
`InviteCode::from_str` (failure is `422` `invalid invite code: <error>`) and **canonicalised**
by re-serialising the parsed value before the key is derived, so two spellings of one invite
share a key (`STO-6` for the `join:`/`recover:` key shapes). Neither verb samples balances;
neither gates on the registry synchronously — `join` records whether the membership pre-existed
and lets the driver decide. A recovery of an already-registered
federation is refused in the driver, so the operation terminalizes `failed` rather than the
request returning 4xx (`FMI-30`).

**API-23** `POST /v1/approve {fed}` promotes an `autojoined` candidate to `userapproved`
(`ALC-37`). `404` `candidate <hex> was not found` if no such candidate; `409 conflict`
`candidate <hex> is <State>, not AutoJoined` (`<State>` is the Rust `Debug` name:
`Discovered`, `UserApproved`, `Rejected`) if the state is not `autojoined`; a concurrent
approval that wins the race after that check is also `409 conflict`. The response is **200**
(not 202) `{operation_key}` with a fresh random key `approve:<hex>:<32 hex chars>` per call,
so the verb is not idempotent: the second identical request is the `409`. This is
the one money-adjacent write that goes to the journal directly rather than through the actor.

**API-24** `POST /v1/reconcile` runs durable reconciliation through the actor (`OPS-35`) then a
best-effort ledger repair; a repair fault is logged and the response is still 200.

## The CLI

**API-25** `wallet-cli` is a thin HTTP client by default. It resolves the daemon from
`$XDG_CONFIG_HOME/walletd/client.toml`, else `~/.config/walletd/client.toml` (written by
`walletd init`, `{url, token_path}`), unless both `--url` and
`--token-path` are given, in which case the pointer is not read at all (`WalletdClient::resolve`).
`--url` alone overrides the pointer's URL, `--token-path` alone its token path; the base URL's
trailing `/` is stripped; the token file is read and trimmed. A missing pointer with no `--url`,
or a missing/empty token file, is the not-running error (exit 4); a pointer that exists but does
not parse is exit 1; an unset `HOME` without both overrides is exit 1. Every request carries a
90 s client-side timeout (`REQUEST_TIMEOUT`), chosen to exceed the daemon's 60 s long-poll.
`--standalone`, `--url`, `--token-path` and `--gateway` are global flags (accepted before or
after the verb); `--data-dir` and `--perform-timeout` are top-level only and MUST precede the
verb. `--standalone` opens the stores directly under the exclusive lock (`HST-9`) and
is the only mode for `discover`, `probe`, `tick`, `status` with policy overrides, `history
--fed`, `show <numeric seq>`, and the
`--gateway` break-glass (`ADR-0030`). Passing a standalone-only flag or verb in client mode is a
usage error, exit 1, before any request is made: `--data-dir`, `--perform-timeout` and
`--gateway` are each refused with a message naming the flag; the three agent verbs with
`standalone-only verb: rerun with --standalone (this agent verb has no daemon endpoint)`.

**API-26** The verbs: `join, recover, discover, candidates, approve, balance, list-feds,
receive, pay, await-receive, await-send, direct-inflow, await-move, move, probe, reconcile,
tick, status, health, policy get, policy set, history, show`. Exactly four initiate movement on
the user's behalf: `pay`, `receive`, `move`, `direct-inflow` (`CONTEXT.md` **Money verb**).

**API-27** Every one of the twenty-eight `Policy` fields is settable through `policy set` flags,
which GETs the whole policy, applies the flags, and PUTs the whole struct back. The round trip
goes through the CLI's **typed** `Policy`, so a field the CLI's build does not know is dropped on
GET, omitted on PUT, and reset to its default by the daemon. Within one version this is a correct
read-modify-write; across a version skew it silently resets a newer field (`F40`). `--clear-spending-fed` and `--clear-standby-fed`
conflict with their pin flags. Flag names are the field names with `_` → `-`; msat fields
take unsigned integers; `--auto-join` and `--require-mainnet` take an explicit `true|false`
value; `--max-fee-bps-of-move` is range-checked `1..=10000` and `--evac-fee-bps` `0..=10000`
**at parse time**, so an out-of-range value is a clap usage error (exit 1) and never reaches
`PUT`. Both `policy get` and `policy set` print the resulting `Policy` as pretty-printed JSON
(`serde_json::to_string_pretty`) on stdout.

**API-28** Exit codes:

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | usage, not found, any non-JSON 4xx other than 401, argument parse error |
| 2 | refused at decision time; nothing journaled |
| 3 | failed; a journaled terminal failure, message carries the key |
| 4 | transport: connection refused, timeout, any 5xx, missing pointer or token, await deadline |
| 5 | authentication (401) |

The mapping from an `ApiError` body (`api_error_to_exit`) is by `kind`, with one status
edge: `unauthorized` → 5; `refused` → 2; `failed` with a **5xx** status (503/500, nothing
journaled) → 4, `failed` with any other status (the 409 + key shape) → 3; `timeout` → 4;
`not_found` → 1. A non-JSON error body (`non_api_error_to_exit`) maps by status class: 401 → 5,
5xx → 4, anything else → 1. A 2xx whose body fails to decode is 4. Argument errors: clap's
usage error is remapped from clap's own 2 to **1** (2 is reserved for refusals); `--help` and
`--version` exit 0. `balance` and client-mode `status` exit 1 after printing when any joined
federation is unopened (`API-9`, `API-39`). The stderr line is prefixed by layer:
`refused: <message>: <RefuseReason Debug>`, `failed: <message> (operation key: <key>)`,
`auth error: <message>`; transport and usage messages carry no prefix, and the not-running case
is the two-line `walletd is not running (or not initialized): <detail>` / `start walletd, or
rerun with --standalone`.

**API-29** Output shapes are frozen: a money verb prints `<word> <key>` to stdout and `key: <key>`
to stderr; `receive`/`direct-inflow` print the invoice to stdout; await verbs print `claimed`,
`success`, `done` or `failed: <error>` (`API-38`). `--json` exists on `discover`, `candidates`, `history`
and `show` only. `history` is a ten-column TSV: `seq, updated_at, kind, status, amount,
recv_fee, send_fee_quoted, actor, reason, key`. The remaining verbs' stdout shapes are the
table in `API-39`; the CLI-supplied defaults for the idempotency inputs (`nonce`,
`occurrence`) are `API-41`.

**API-30** Standalone `show --json` and `history --json` render the flattened persisted record
(`OperationRecordAuditView`: every `OperationRecord` field at the top level in its stored serde
form per `STO-5` — `kind` as the externally-tagged `OperationKind` object with op ids, gateway
and enforced cap, `status` and `reason` as PascalCase enum names, `actor` as the enum object,
`correlation_key` rather than `operation_key`, plus `repaired` — with `supersedes`,
`superseded_by`, `evacuation_refusal`, `evacuation_refusal_active` appended when present);
client mode renders `OperationView` (`API-33`).
The two are different shapes by design, and the enforced cap is reachable only through the
former (`F9`).

## What the wire does not carry

**API-31** No gateway on any money request (`ADR-0030`). No federation id on `OperationView`.
No enforced fee cap on `OperationView`. No `?status=open`, `actor` or `fed` filter on history.
No readiness fields in the CLI's `health` rendering. No `deferred` in the CLI's `status`
rendering. `OperationFailure` and `AwaitTarget` are defined DTOs that no route returns or
accepts.

## Wire encoding

**API-32** The JSON representation of every `wallet-api` type follows the serde rules `STO-5`
owns (derived serde, external tagging, `[u8; 32]` as an array of 32 integers, `Option` as
`null`). The wire deltas an implementer MUST know: a `FederationId` is that **array of 32
integers 0–255** on every DTO — `PayRequest.fed`, `MoveRequest.from/to`,
`ReceiveRequest.to`, `DirectInflowRequest.to`, `ApproveRequest.fed`,
`Policy.spending_fed/standby_fed`, `FederationView.id`, `CandidateView.id`,
`WatchStatusView.discover_cursor`, `RefusalDiagnostics.source` — and is **64-character
lower-hex** only inside the daemon-private `/v1/status` body (`API-15`); no route accepts hex
in a request. `Msat` and `Occurrence` are bare unsigned integers — newtype structs, which
serde writes as the inner value with **no** attribute (`STO-5`); they are the only
newtype-shaped wire values. `OperationStatusDto`, `ApiErrorKind` and `RefuseReason` are `snake_case`-renamed
enums (`API-5`, `API-12`); `kind`, `actor`, `reason`, `source`, `state`, `structural` are plain
strings whose vocabularies `API-12` and `API-17` list. Object keys are the Rust field names
verbatim: no struct in `wallet-api` carries `rename`, `rename_all`, `flatten` or
`transparent` (the two newtypes above need none to serialize bare).

**API-33** `OperationView` field table. "always" means the key is present in every response
and is `null` when the value is absent; "omitted" means the key is absent from the JSON when
the value is absent (`skip_serializing_if`) and a client MUST decode a missing key as absent.

| Field | Type | Absent |
|---|---|---|
| `seq` | u64 | never |
| `updated_at_ms` | u64 | never |
| `kind` | string (`API-12`) | never |
| `status` | `started\|awaiting\|succeeded\|failed` | never |
| `amount` | msat | always, `null` |
| `receive_fee` | msat | always, `null` |
| `send_fee_quoted` | msat | always, `null` |
| `actor` | string | never |
| `reason` | string (`API-12`) | never |
| `operation_key` | string | never |
| `error` | string | always, `null` (cleared on success) |
| `superseded_by` | string (key) | omitted |
| `supersedes` | string (key) | omitted |
| `refusal` | object (`API-34`) | omitted; emitted only when `kind == "refusal"` **and** `RefusalDiagnostics::is_populated()` (a figure-less refusal has no key) |
| `evacuation_refusal` | object (`API-34`) | omitted; `show` only, never on `history` |
| `evacuation_refusal_active` | bool | omitted on `history` and when `show` finds no readable intent; `true` = a live Pending agent-evacuation marker, `false` = the intent is readable but not live (`OPS-31`) |

`amount` is the invoiced amount for `receive`, the invoice amount for `pay` (null when unknown),
the net amount for `direct-inflow`/`move`/`evacuation`, the probe amount for `probe`, and null
for every other kind.

**API-34** The two nested objects. `refusal` is `wallet_core::RefusalDiagnostics` with all ten
keys always present: `source` (federation-id array or `null`), `want`, `available`,
`source_spendable`, `max_fee`, `cap_room`, `amount`, `min_move` (each msat or `null`),
`max_fee_bps` (u16 or `null`), `conflict_suppressed` (bool, never null); the meaning of each
figure is `ALC-9` and the storage row is `STO-5`'s. `evacuation_refusal` is
`wallet_core::EvacuationRefusalEvidence`, seven keys, none nullable: `cap_components`
(`{"base_msat": <msat>, "bps": <u16>}`), `requested_net` (msat), `source_spendable` (msat),
`low` and `high` (each `{"delivered_net": <msat>, "total_fee": <msat>, "fee_cap": <msat>}`),
`diagnostic` (string), `measured_at_ms` (u64). Its semantics are `OPS-31`.

## Request decoding and daemon-side validation

**API-35** Every request body is decoded by axum's `Json` extractor and every `JsonRejection`
becomes `422 refused` with message `invalid JSON request body: <axum's own text>` — this
covers a missing or non-`application/json` `Content-Type` header, a syntax error, a type
mismatch, an unknown key (every request DTO is `deny_unknown_fields`), and a body over axum's
default 2 MiB limit (there is no `DefaultBodyLimit` override). A request DTO's `Option` fields
MAY be omitted or sent as `null`; `null` for a required field is a type mismatch. `PUT
/v1/policy` decodes to a `serde_json::Value` first, so its unknown-key refusal is `API-20`'s
message, not this one. `POST /v1/reconcile` has no body extractor: the body is ignored whatever
its size or content type. Request order of keys is irrelevant everywhere, including the
`unknown policy field(s)` listing, which is lexicographic (`API-20`).

**API-36** Trust-boundary validation the daemon performs itself before the actor, with the
verbatim `422` message (no `refuse_reason`) unless stated:

| Check | Where | Message |
|---|---|---|
| nonce empty | `receive`, `direct-inflow` (`validate_nonce`) | `nonce must not be empty` |
| nonce charset: only `A-Z a-z 0-9 - . _ ~` | same | `nonce must contain only unreserved URL characters (A-Z a-z 0-9 - . _ ~)` |
| invoice does not parse | `pay` | `invalid BOLT11 invoice: <error>` |
| invoice amountless | `pay` | `refuse_reason: "amount_required"`, `API-18` |
| stated amount ≠ invoice amount | `pay` | `refuse_reason: {"sizing_conflict":{"field":"amount"}}`, `stated amount does not match the invoice amount` |
| `from == to` | `move` | `move from and to must be different federations (from == to is a no-op)` |
| named federation not in the registry | `pay`, `receive`, `direct-inflow` (explicit or pinned), `move` (both) | `federation <hex> is not joined` |
| no federation named, none joined | `pay`, `receive`, `direct-inflow` | `no federations joined; join one first` |
| no federation named, several joined | same | `multiple federations joined; name the federation explicitly` |
| invite does not parse | `join`, `recover` | `invalid invite code: <error>` |
| `limit`/`before_seq` not unsigned integers | `history` | `invalid query parameters: …` (`API-10`) |
| `wait` not a bool literal | `operations/{key}` | `invalid query parameters: …` (`API-11`) |
| policy body not an object / unknown key / missing or mistyped field | `policy` PUT | `API-20` |

Balance sampling (`sample_balances`) then reads the live balance of every *open* federation
the verb names; a federation that is joined but not open is simply omitted from the sample
(the actor treats it as zero spendable); a balance read that **fails** on an open federation
is `503 failed` `reading balance for federation <hex> failed: <error>` and nothing is admitted.
Amounts and fee caps are not range-checked by the daemon (`0` is passed through to the actor).

**API-37** `5xx` bodies. A journal read or write fault anywhere in a handler (`storage`) is
`500` `{"kind":"failed"}` whose `message` is the **`Debug` rendering** of the
`wallet_core::ExecError` — it MAY contain RocksDB paths and the data directory, and a client
MUST NOT show it to an untrusted party. `503 failed` messages are `wallet service is shutting
down`, `wallet service actor stopped`, the `DestinationUnavailable` text produced by the actor
for a fresh dest-side admission (`API-19`), the balance-read text of `API-36`, and the
`/v1/status` fences of `API-15`. No 5xx carries a `refuse_reason`. The two `504`s — the
`?wait=true` deadline (`API-11`) and the invoice mint deadline (`API-21`) — carry `operation_key`
when the operation was admitted (`HttpError::timeout`); `500` and `503` carry neither.

## CLI verb details

**API-38** The await verbs. `await-receive <key>`, `await-send <key>` and `await-move <key>`
take `--timeout <secs>` (default **600**) and loop on `GET /v1/operations/{key}?wait=true`
(`WalletdClient::await_op`): a `2xx` with a terminal `status` ends the loop; a `2xx` that is
not terminal, or a `504`, re-polls after 200 ms; any other non-`2xx` is mapped by `API-28` and
**not** retried (a dead daemon fails fast); when `--timeout` elapses the exit is 4 with
`await timed out after <n>s waiting for operation <key> to terminalize`. Before printing, the
verb checks the row's `kind` (`AwaitVerb::accepts_kind`): `await-receive` accepts only
`receive`, `await-send` only `pay`, `await-move` accepts `move`, `evacuation`,
`direct-inflow`, `join` and `recover`; a mismatch is exit 1 `operation <key> is a \`<kind>\`
operation, not awaitable with \`<verb>\`` with nothing on stdout. A `succeeded` row prints
`claimed` / `success` / `done` respectively and exits 0; a `failed` row prints `failed:
<error>` — on **stdout**, with `(no diagnostic)` when `error` is null — and exits 3 with
`operation <key> failed: <error>` on stderr. `await-send` prints no preimage (the wire carries
none).

**API-39** Client-mode stdout shapes for the verbs `API-29` does not list. `<hex>` is a
64-character lower-hex federation id; one line per row unless stated.

| Verb | stdout |
|---|---|
| `balance` | `<hex>: <n> msat` per federation, or `<hex>: unavailable (failed to open)` when `balance` is null; then `total (<open>/<joined> federations): <n> msat`; exit 1 if `open < joined` (`API-9`) |
| `list-feds` | `<hex> invite=<invite> joined_at=<secs>` |
| `health` | `actor_queue_depth=<n> inflight_drivers=<n> scheduler_alive=<true\|false>` |
| `reconcile` | `redriven=<n> awaiters_rehydrated=<n> executing_normalized=<n>` |
| `status` | `spending_fed: <hex\|none>`, `standby_fed: <hex\|none>`, `<hex> gated_eligible=<bool>` per `scored`, `<hex>: unavailable (failed to open)` per federation whose `/v1/federations` balance is null (a second GET the verb makes), `decision: <key> reason=<reason> action=<action>` per decision; exit 1 if any unopened (`API-15`) |
| `approve <hex>` | `<hex>`; `key: <key>` on stderr |
| `policy get` / `policy set` | pretty-printed `Policy` JSON (`API-27`) |
| `candidates` | 8-column TSV `id state source discovered_at_ms structural structural_checked_at_ms updated_at_ms invite`; `--json` prints the filtered, sorted array as ONE line of JSON (`CandidateView` objects) |
| `history` | the `API-29` TSV; `-` for a null `amount`/fee; `updated_at` as `YYYY-MM-DDTHH:MM:SS.mmmZ` (`rfc3339_from_millis`, UTC, millisecond precision); `--json` prints one `OperationView` object per line (JSONL), the wire shape verbatim |
| `show <key>` | one `<label>: <value>` line each for `seq, key, kind, status, actor, reason, updated_at, amount_msat, receive_fee_msat, send_fee_quoted_msat, supersedes, superseded_by, evacuation_refusal` (the object as one-line JSON), `evacuation_refusal_active`, then the refusal diagnostic lines when `refusal` is present, then `error`; `-` for every absent value; `--json` prints the `OperationView` as one line |

**API-40** Client-mode filter emulation. `history --limit N` (default 50) requests pages of
`limit=N` (the daemon caps each at 500, `API-10`), applies `--actor user|agent` (`agent`
matches any `actor` tag beginning `agent`) and `--status started|awaiting|succeeded|failed`
to each page, and follows `next_before_seq` until `N` matching rows are collected or the cursor
is `null`; filters therefore apply before the limit, matching the standalone contract, at the
cost of unbounded paging over a long ledger. `candidates --state
discovered|autojoined|userapproved|rejected` filters client-side, then sorts descending by
`(updated_at_ms, id)` before printing.

**API-41** CLI-supplied idempotency inputs. `receive --nonce` is optional: when omitted the CLI
generates 16 random bytes as 32 lower-hex characters (`cli_nonce`), so every nonce-less
`receive` is a distinct operation. `direct-inflow --nonce` defaults to the literal string
`"0"`, so two nonce-less `direct-inflow` calls with the same `--amount` and destination
**attach to one operation and re-yield the same invoice** (`OPS-8`); a caller wanting a second
inflow MUST pass a fresh nonce. `move --occurrence` defaults to `0`; the wire field is required
(`API-19`) and the CLI always sends it. `--fee-cap` and `--to`/`--fed` are omitted from the
request when not given, so the daemon-side policy defaults of `API-18`/`API-19`/`API-21` apply.
