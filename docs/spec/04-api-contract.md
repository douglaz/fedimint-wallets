# 04 — API contract

The daemon's HTTP surface and the CLI that speaks it, as built. Every route, field, status code
and exit code below is read out of `wallet-daemon/src/{server,handlers,error}.rs`,
`wallet-api/src/lib.rs` and `wallet-cli/src/{main,client,render,exit}.rs` on 2026-09-07. Where
a document elsewhere claims a parameter that does not exist, this document wins and
`11-open-findings.md` records the gap.

## Transport and authentication

**API-1** The daemon serves plain HTTP. There is no TLS, no rate limiting, and no CORS handling.
The trust boundary is the operating-system user (`SEC-1`): anything that can read the token file
can do everything the API can do.

**API-2** Every route, including `/v1/health`, requires `Authorization: Bearer <token>`. The
token is compared in constant time against the file the daemon read at start. A missing or wrong
token returns `401` with `{"kind":"unauthorized"}`.

**API-3** The token is 32 random bytes, lower-hex (64 characters), written `0600` by `walletd
init`, which rotates it. Re-running `init` while the daemon runs blocks on the database lock
rather than rotating underneath it.

**API-4** The bind address defaults to `127.0.0.1:9736`. The address is a free-form string in
`walletd.toml`; `0.0.0.0` is accepted. Loopback is a default, not an enforced invariant
(`SEC-3`).

## Error envelope

**API-5** Every non-2xx response is

```json
{ "kind": "refused|failed|unauthorized|not_found|timeout",
  "refuse_reason": "<optional>", "operation_key": "<optional>", "message": "<text>" }
```

`refuse_reason` ∈ `insufficient_after_reservations | fed_held_by_probe | over_cap |
budget_exhausted | sizing_conflict{field} | amount_required | storage_error | policy_invalid |
policy_superseded | conflict`.

**API-6** Status codes map as follows and nowhere else:

| Status | Kind | When |
|---|---|---|
| 401 | unauthorized | `API-2` |
| 404 | not_found | unknown operation key; unknown candidate on approve |
| 422 | refused | `policy_invalid`, `amount_required`, `sizing_conflict`; and every daemon-side request validation failure with no reason (bad invoice, `from == to`, unjoined federation, bad nonce, malformed JSON, bad query or path, unknown policy field) |
| 409 | refused | `insufficient_after_reservations`, `fed_held_by_probe`, `over_cap`, `budget_exhausted`, `storage_error`, `policy_superseded`, `conflict` |
| 409 | failed | a journaled terminal failure surfaced synchronously; carries `operation_key` |
| 503 | failed | shutting down, actor stopped, destination federation joined but not open, or a `/v1/status` precondition (`API-15`) |
| 504 | timeout | a long-poll or invoice deadline elapsed; carries `operation_key` when the operation was admitted |
| 500 | failed | storage error |

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

**API-10** `GET /v1/history` accepts exactly two query parameters: `limit` (default 50, capped
at 500) and `before_seq`. `next_before_seq` is the oldest sequence number of a full page, else
null; pass it back as `before_seq` for the next page. **There is no `status`, `actor`, `fed` or
`kind` filter.** The CLI emulates actor and status filters by paging client-side and cannot
filter by federation in client mode (`F27` for the `?status=open` filter the web plan requires).
Undecodable ledger rows are skipped without signal on this route (`STO-19`).

**API-11** `GET /v1/operations/{key}` returns 404 if no ledger row exists. `?wait=true`
long-polls until the operation is terminal or 60 seconds elapse (`504` carrying the key).
`show` additionally projects `evacuation_refusal` and `evacuation_refusal_active` from the exact
intent (`OPS-31`); `history` does not, because it does no per-row intent lookup.

**API-12** `OperationView` carries: `seq, updated_at_ms, kind, status, amount?, receive_fee?,
send_fee_quoted?, actor, reason, operation_key, error?`, plus `superseded_by?, supersedes?,
refusal?, evacuation_refusal?, evacuation_refusal_active?` when present. `kind` ∈ `join, recover,
receive, pay, direct-inflow, move, evacuation, refusal, probe, tick, discover, autojoin,
approve`. `actor` is `"user"` or `"agent:<occurrence>"`. **The enforced fee cap is not on the
view** (`F9`), nor is a federation id.

**API-13** A recovery whose intent is `Succeeded` but whose client handle is not yet installed
is reported as `started`, so a caller never observes "succeeded" alongside a zero balance
(`DEF-18`'s visibility window).

**API-14** `GET /v1/watch/status` is a pure journal read of `WatchState` (`STO-12`).

**API-15** `GET /v1/status` is a dry run of the next scheduler tick against the stored policy
at `occurrence + 1`. It returns `{spending_fed, standby_fed, decisions:[{operation_key, reason,
action}], scored:[{id, gated_eligible}], deferred:[{dest, source, reason, want_msat, floor_msat,
floor_source}]}`. `action` is the Debug rendering of the action, not a stable wire shape. It
returns **503** before the dry run when the runtime is absent, the federation registry reports a
skipped corrupt row, any joined federation is unopened, the watch occurrence is at its
fail-closed maximum, or probing errors (the fences of `ALC-46`; the corrupt-row and unopened
cases land with PR #40). This response shape is daemon-private, mirrored by hand in the CLI
client, and the CLI's mirror **drops `deferred`**: an operator using `wallet-cli status` in
client mode cannot see a withheld funding goal. Only the raw endpoint and the poller show it.

**API-16** `GET /v1/health` always returns 200 when authenticated. Body:
`{actor_queue_depth, inflight_drivers, scheduler_alive, automation_ready, automation_blocked?}`.
`scheduler_alive` is the scheduler loop's liveness flag. `automation_ready` is
`automation_blocked.is_none()`. `automation_blocked` is `{reason, detail}` with `reason` ∈
`cycle_failed | partial_federation_view | corrupt_federation_registry` (`ALC-45`). Liveness and
readiness are different answers, and a supervisor that reads only the status code learns
neither. The CLI's `health` verb prints the first three fields and **omits both readiness
fields**.

**API-17** `GET /v1/candidates` returns the candidate registry in raw key order with
`{id, invite, source, discovered_at_ms, structural, structural_checked_at_ms, state,
updated_at_ms}`; `source` ∈ `observer|nostr|manual`, `state` ∈
`discovered|autojoined|userapproved|rejected`, `structural` is `passed` or `rejected:<reason>`.
Newest-first ordering is a CLI convenience, not a wire property.

### Money verbs

Every money verb builds one `AllocatorDecision` with `reason: user_initiated`, `actor: User`,
samples the source federation's balance, and submits it to the actor as one command (`OPS-5`).
The response discards whether the admission was fresh or attached to an in-flight operation with
the same key; a caller cannot distinguish the two from the status code.

**API-18** `POST /v1/pay {invoice, amount?, fee_cap?, fed?}` (unknown fields rejected). An
amountless invoice without `amount` is `422 amount_required`; a stated amount that disagrees
with the invoice is `422 sizing_conflict{amount}`. `fee_cap` defaults to the policy's `max_fee`;
`fed` defaults to the policy's spending federation, else the sole joined federation, else 422.
The operation key is derived from the payment hash, so paying the same invoice twice attaches
to the same operation (`OPS-8`). There is no gateway field on the wire (`ADR-0030`).

**API-19** `POST /v1/move {from, to, amount, fee_cap?, occurrence}`; `occurrence` is required
and, with the resolved `fee_cap`, is part of the operation key. A client that omits `fee_cap`
and retries after a policy edit therefore derives a *different* key and admits a second move;
the web plan's money forms exist to pin these values at render time (`F27`). `from == to` is
422. A destination that is joined but not open is 503.

**API-20** `PUT /v1/policy` accepts a JSON object and rejects any key not in the set derived at
runtime from `Policy::default()`'s serialization, with `422 unknown policy field(s): …`. A
missing field that carries `serde(default)` takes its default; any other missing field is 422.
`Policy::validate` runs in the actor and refuses with `422 policy_invalid` naming the field.
The stored `Policy` type itself is permissive (`STO-31`); this handler is where strictness
lives, and it MUST stay here so the wire contract cannot drift from the type.

**API-21** `POST /v1/receive {to?, amount, fee_cap?, nonce}` and `POST /v1/direct-inflow` (same
shape). `nonce` is non-empty RFC 3986 unreserved characters and is part of the key. The daemon
admits the intent and then **blocks up to 30 seconds** for the invoice artifact: terminal
without an invoice is `409 failed` with the key; the deadline is `504 timeout` with the key.
Re-submitting the same key re-yields the same invoice (`OPS-8`). `receive` deducts fees from the
invoice; `direct-inflow` grosses the invoice up so the destination is credited exactly `amount`
(`FMI-15`).

**API-22** `POST /v1/join {invite}` and `POST /v1/recover {invite}` are 202 and asynchronous;
await them with `GET /v1/operations/{key}?wait=true`. A recovery of an already-registered
federation is refused in the driver, so the operation terminalizes `failed` rather than the
request returning 4xx (`FMI-30`).

**API-23** `POST /v1/approve {fed}` promotes an `autojoined` candidate to `userapproved`
(`ALC-37`). 404 if no such candidate; `409 conflict` if the state is not `autojoined`. This is
the one money-adjacent write that goes to the journal directly rather than through the actor.

**API-24** `POST /v1/reconcile` runs durable reconciliation through the actor (`OPS-35`) then a
best-effort ledger repair; a repair fault is logged and the response is still 200.

## The CLI

**API-25** `wallet-cli` is a thin HTTP client by default. It resolves the daemon from
`~/.config/walletd/client.toml` (written by `walletd init`) unless both `--url` and
`--token-path` are given. `--standalone` opens the stores directly under the exclusive lock and
is the only mode for `discover`, `probe`, `tick`, standalone `status`, `history --fed`, and the
`--gateway` break-glass (`ADR-0030`). Passing a standalone-only flag in client mode is a usage
error, exit 1.

**API-26** The verbs: `join, recover, discover, candidates, approve, balance, list-feds,
receive, pay, await-receive, await-send, direct-inflow, await-move, move, probe, reconcile,
tick, status, health, policy get, policy set, history, show`. Exactly four initiate movement on
the user's behalf: `pay`, `receive`, `move`, `direct-inflow` (`CONTEXT.md` **Money verb**).

**API-27** Every one of the thirty `Policy` fields is settable through `policy set` flags, which
GETs the whole policy, applies the flags, and PUTs the whole struct back (read-modify-write, so
a field the client does not know survives). `--clear-spending-fed` and `--clear-standby-fed`
conflict with their pin flags.

**API-28** Exit codes:

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | usage, not found, any non-JSON 4xx, argument parse error |
| 2 | refused at decision time; nothing journaled |
| 3 | failed; a journaled terminal failure, message carries the key |
| 4 | transport: connection refused, timeout, any 5xx, missing pointer or token, await deadline |
| 5 | authentication (401) |

**API-29** Output shapes are frozen: a money verb prints `<word> <key>` to stdout and `key: <key>`
to stderr; `receive`/`direct-inflow` print the invoice to stdout; await verbs print `claimed`,
`success`, `done` or `failed: <error>`. `--json` exists on `discover`, `candidates`, `history`
and `show` only. `history` is a ten-column TSV: `seq, updated_at, kind, status, amount,
recv_fee, send_fee_quoted, actor, reason, key`.

**API-30** Standalone `show` and `history --json` render the flattened persisted record (op ids,
gateway, enforced cap, `repaired`, linked intent status); client mode renders `OperationView`.
The two are different shapes by design, and the enforced cap is reachable only through the
former (`F9`).

## What the wire does not carry

**API-31** No gateway on any money request (`ADR-0030`). No federation id on `OperationView`.
No enforced fee cap on `OperationView`. No `?status=open`, `actor` or `fed` filter on history.
No readiness fields in the CLI's `health` rendering. No `deferred` in the CLI's `status`
rendering. `OperationFailure` is a defined DTO that no route returns.
