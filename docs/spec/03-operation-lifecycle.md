# 03 — Operation lifecycle

How a money operation is admitted, executed, resumed after a crash, and terminalized. Read from
`wallet-core/src/executor.rs`, `wallet-fedimint/src/{executor,move_protocol,runtime}.rs`, and
`wallet-fedimint/src/service/{mod,actor,driver}.rs` on 2026-09-07, re-read against `7225114`
on 2026-09-10. This is the part of the
system the rest protects: the property that a two-leg move killed at any of four named points
completes exactly once.

## Intents and their states

**OPS-1** An executable operation is driven by an intent (`DOM-6`). The intent's
`operation_correlation_key()` is the public key on attempt 0 and `retry:<len>:<key>:<n>` on
attempt `n > 0`; that is what rides in the SDK's `custom_meta` (`STO-34`), so a recovery cannot
attach a retry to the prior attempt's operation.

**OPS-2** The status machine, enforced by every durable writer:

```
Pending   → Pending | Executing | Awaiting | Done | Failed
Executing → Pending | Executing | Awaiting | Done | Failed
Awaiting  → Awaiting | Done | Failed
Done      → Done
Failed    → Failed
```

`Failed → Pending` is not a transition. It is a separate operation, `retry_failed_intent`, that
writes a new attempt (`OPS-10`).

**OPS-3** `Awaiting` is the state of any intent whose effect was issued and whose completion
depends on an external event: a direct inflow waiting for its payer, **and** a raw pay or receive
waiting for settlement. The doc comment on `IntentStatus` says direct-inflow only; it is stale.
Reconcile never re-drives `Awaiting`; an awaiter task owns it (`OPS-16`). One raw pay never
enters `Awaiting`: a drive that attaches to an already-settled send op goes `Executing → Done`
in the driver itself (`OPS-17`).

**OPS-4** A perform returns `Done`, `Awaiting`, or an `ExecError`: `Retryable(msg)` resets
`Executing → Pending` with the marker cleared and counts as both `failed` and `retryable`;
`StructuralEvacuationRefusal(evidence)` resets to `Pending` with `evacuation_refusal = Some`
(`OPS-31`); `Permanent(msg)` sets `Failed` with `msg` as the ledger row's `error`;
`Unsupported` is `Failed` and reachable only if a refusal reaches perform. A retryable failure
does **not** bump the attempt counter. These dispositions apply to errors **returned by
`perform`** only. A journal write that fails inside `drive_intent_step` — the claim, the
re-read, the status write after `Ok`, or the retryable reset — is propagated as an error and
terminalizes nothing: the intent keeps the status of the last committed write and the next
reconcile picks it up (`OPS-43`). In the daemon every actor-routed journal failure reaches the
core as `ExecError::Permanent("wallet service transition failed: …")`; that is still a journal
error, never a `Failed` intent.

## Admission

**OPS-5** Every user verb — in the daemon and in the CLI's standalone mode alike — builds one
`AllocatorDecision` (`reason: UserInitiated`, `actor: User`), samples balances off the actor
(`pay`: `from`; `move`: `from` and `to`; `receive` and `direct-inflow`: `to`; `join` and
`recover`: none), sets `dest_unavailable` only for `move`, `receive` and `direct-inflow` when
`to` is joined but unopened, and submits one `OpRequest` to the actor. The CLI's standalone money
verbs run the same actor without a scheduler; `Runtime::pay/receive/join/await_move` have no
production caller. Two paths bypass the actor: `Runtime::tick` (`OPS-12`) and the standalone
`probe` verb, whose two money legs go through `Runtime::do_move` with no service client (`F42`).
In the daemon a probe leg (`drive_probe_leg`) is admitted through `decide_op` with its session
nonce and `dest_unavailable: None`, then awaited with `resolve_await(key, Terminal, now +
perform_timeout)` — 24 hours when no perform timeout is configured.

**OPS-6** The actor's fresh-key admission, in order: journal read error → `StorageError`; a
goal-bearing agent decision re-scans `pending()` and a conflicting live holder → `Conflict`
(`ALC-30`); a destination that is joined but unopened → `DestinationUnavailable` (503); probe-leg
session validation; the external driver cap (32 user-originated, non-probe, non-evacuation
drivers) → `Conflict`; a source federation held by an in-flight probe session → `FedHeldByProbe`
unless the request is that session's leg or an evacuation (which preempts the probe); then the
core admission with the sampled balances and the policy's per-federation cap. On success:
record the goal, bump balance and membership generations, apply any probe preemption, spawn a
driver.

**OPS-7** Core admission (`admit_intent`) checks: for `Move` and `Pay`, `amount + fee_cap ≤
balance[from] − reservations.outbound(from)`; for `Move`, `Evacuate`, `DirectInflow` and
`Receive`, `balance[to] + reservations.inbound(to) + amount ≤ per_fed_cap`. **`Evacuate` has no
source-balance check and no pre-fund admission at perform time**; its money safety rests
entirely on perform-time sizing (`OPS-21`). With `balances == None` the function checks nothing;
the actor always passes balances. `Runtime::do_move` passes `None` when the key already exists
or either client is unopened (`Runtime::pay`/`receive` pass `None` only when the key exists;
`Runtime::direct_inflow` also when no hard cap is configured or `to` is unopened), and the
standalone `probe` verb reaches it (`F42`) — but a fresh probe first runs `probe_local_faults`,
which requires the source to hold `amount + leg cap` and the candidate to be under the cap, and
the executor's pre-fund admission (`OPS-45`) re-samples balances before any new move IO, so what
is skipped there is the **actor's** admission, not every money check.

The **hard cap** an executor enforces at perform time (`OPS-21`, `OPS-22`, `OPS-45`) is not
one value. Every actor driver — the daemon's and the standalone actor's money verbs alike —
carries the actor's **stored** `Policy.per_fed_cap` (seeded insert-if-absent from
`Policy::default()`, 1,500,000,000 msat), and a daemon-scheduled probe's legs carry the same
stored cap. Standalone `probe` and `discover` build their runtime with `operator_hard_cap(false)
= TickPolicy::default().per_fed_cap`, the **compile-time** 5,000,000,000 msat, not the stored
policy; a standalone probe's preflight and leg sizing therefore run under that constant
(`F43`), while `discover`'s auto-join joins and records candidates and never probes, so it
spends nothing under it. Standalone `tick` and `status` use the standalone tick policy's `per_fed_cap` (the stored policy
with any flag overrides). `operator_hard_cap(true)` — the `None` that disables the cap — has no
caller: no verb exposes `--allow-over-cap`.

**OPS-8** Idempotency. A request whose key already exists attaches: `Done` → deduplicated with
the existing outcome; `Awaiting`, `Pending` or `Executing` → the existing intent is driven or
re-awaited; `Failed` → the retry path (`OPS-10`) for a `User` actor, while a `Failed` key hit by
an `Agent` decision takes the **live** path below (the sizing check, then the core's
`TerminalFailed`), spawns nothing and answers status `Failed`, `deduplicated: true`. "Driven or
re-awaited" means `ensure_driver`: a key a live driver owns only gets its `redrive_requested`
flag; a `Pending` key gets an intent driver; an `Executing` key gets an intent driver that
performs **without** a claim (`OPS-43`); an `Awaiting` key gets an awaiter, counted against the
external driver cap whatever its actor. What attach validates depends on the state. For a
**terminal** key the actor checks only the idempotency **anchor** (`Pay`: `payment_hash`;
`Receive`: `to, amount, nonce`; `DirectInflow`: `to, amount` — the action carries no nonce, the
key does; `Join`: federation and invite; every other action: full equality) and refuses a change
with `409 conflict` "same-key
request changed the completed operation's idempotency anchor"; a re-submitted completed pay with a
different `fee_cap` is therefore a 202 deduplication, not a refusal. For a **live** key the actor
checks the sizing fields (`Pay`: `from, amount, fee_cap, payment_hash`; `Receive`: `to, amount,
fee_cap, nonce`; `Move`/`Evacuate`: `from, to, amount, fee_cap`, ignoring the gateway hint;
`DirectInflow`: `to, amount, fee_cap`; `Join`/`Recover`: federation and invite) and refuses a
mismatch with `422 sizing_conflict`. The core's own `validate_attach` runs only after those actor
checks and only for `Pending`/`Executing`/`Awaiting`. The 202 response does not say whether the
admission was fresh (`API-18`).

**OPS-9** Reservations are projected from `reservation_intents()` (`Pending`, `Executing`,
`Awaiting`; fails closed on a corrupt row). The **strict** projection reserves every non-terminal
intent's full action: `Move`/`Evacuate` outbound `amount + fee_cap` on the source, inbound and
target-credit `amount` on the destination; `DirectInflow`/`Receive` inbound; `Pay` outbound.
The **allocator** projection weakens by move-record phase when the record is trusted
(`Invoiced` keeps all three; `Sending` drops the outbound; terminal drops all; a `Pay` with an
operation id reserves nothing) and is what the tick uses (`ALC-35`). User admission uses the
strict view.

**OPS-10** Retry. Only a `Failed` intent, only by a `User` actor, only preserving the anchor
fields (`Pay` its payment hash; `Receive` its `to, amount, nonce`; `DirectInflow` its `to,
amount`; everything else exact), and never for a `Failed` pay that recorded an operation id ("this
invoice already consumed its single payment attempt"). Before the write the retry is admitted
like a fresh key: the external driver cap, the probe hold (`OPS-6`), and `admit_intent`
(`OPS-7`) on the refreshed intent against the **strict** projection, the request's sampled
balances and the policy `per_fed_cap`. `retry_failed_intent` then writes `Pending`
at `attempt + 1`, deletes the cached move record, appends a fresh ledger row and repoints the key
index (`STO-9`); the new attempt writes the per-attempt correlation key of `STO-34` into the
op-log, and the failed row's error string is persisted verbatim (`STO-35`). Every durable writer
is attempt-fenced, so a stale attempt's write returns `false` and the executor maps it to
`Retryable`. An agent decision whose key is `Done`,
`Awaiting` or `Failed` is skipped, not retried; a new occurrence mints a new key.

**OPS-11** Agent decisions are admitted as a batch by `CommitTick` (`ALC-32`). The batch is
refused whole on a stale policy generation, a changed world generation, an invalid tick-plan or
balance-facts token, or multiple occurrences; each decision is then checked for a fresh
destination balance, unchanged balance facts, terminal replay, conflict, admission watermark, and
— for funding moves — that the amount does not exceed the fresh target shortfall, before going
through `OPS-6`.

**OPS-12** The documented admission exception (`ADR-0031`): `wallet-cli --standalone tick`
holds the exclusive lock, plans, re-scans `pending()` for blockers itself, and applies through
`apply_with_allocator_admission` without the actor. An undocumented second one is
`wallet-cli --standalone probe`, whose legs are admitted by `Runtime::do_move` without the
actor's conflict, goal, driver-cap and probe-hold checks; source funds and the destination cap are
still checked by the probe preflight and the executor's pre-fund admission (`OPS-5`, `F42`). The core functions that make this possible
are `pub`, so "the actor is the sole writer of agent intents" is convention, not enforcement
(`F16`).

## The fully-async model

**OPS-13** The actor is one task with a mailbox of 64 owning `ActorState`. It does: admission,
journal transitions (`Upsert, CompareAndSet, ResetRetryable, SetStatus, SetRawTerminal,
DriverFinished, Refresh`), one-shot artifact writes, snapshots, waiter park and resolve,
reconcile scans, token and lease bookkeeping, policy get and put, shutdown. It performs no
network IO. Tick planning, including route pricing, is spawned off the actor. The two one-shot
artifact commands, `SetOperationArtifact` and `PutMove`, each: read the intent and require
`idempotency_key == key && attempt == expected_attempt` (else reply `Ok(false)` and write
nothing); call the fenced journal writer (`set_operation_artifact_if_attempt`,
`put_move_if_attempt`); on `true` bump the balance generation of every federation the action
touches and wake the key's waiters; on a writer error bump the same generations and reply
`Storage`. The executor maps a `false` reply to `Retryable("raw operation artifact belongs to a
stale or terminal attempt; retry from current intent")` and an error to `Retryable("actor-routed
artifact write failed closed: …")`. The actor's `SetStatus` transition replies
`Compared(false)` — a silent no-op the core counts as `performed` — when the attempt mismatches
or the transition table forbids the move, where the direct journal returns
`Permanent("journal: …")`; `CompareAndSet` replies the journal's boolean; `Upsert` refuses any
change to `action, actor, reason, created_at_ms, max_fee, operation_id, invoice` with
`Storage`.

**OPS-14** A **driver task** runs `drive_intent_step` with an `ActorJournal` that routes writes
through actor transitions and reads the durable journal directly. Drivers are tracked in a
process-local registry keyed by intent key with a generation, an abort handle, and a
`redrive_requested` flag; a `Drop` guard removes the entry only if the generation still matches,
and the task is released only after the guard is armed. `ensure_driver` requests a re-drive if a
driver already owns the key, else spawns an awaiter (status `Awaiting`) or an intent driver.
`finish_driver` re-reads the intent and re-spawns on: same attempt now `Awaiting` (driver-to-
awaiter handoff), same attempt `Pending` with re-drive requested, a newer `Pending` attempt with
re-drive requested, or an awaiter retry — unless the intent is a planner-owned marker.
`finish_driver` re-spawns nothing else: a driver that ended `Retryable` leaves its `Pending` key
**without an owner** until the next `reconcile_durable` pass (`OPS-35`); there is no in-driver
retry loop, so the retry cadence is the scheduler's reconcile cadence (`ALC-32`). A read fault
after deregistration schedules ownership recovery: one detached task per actor,
generation-coalesced, that runs `ReconcileDurable` (marker policy `PreservePlannerOwned`) with a
25 ms backoff doubling to a 1 s ceiling until a scan completes at the current generation or the
actor stops. Cross-restart exactly-once rests on SDK
operation ids, lnv2 dedup and op-log backfill, not on this registry (`ADR-0024`).

**OPS-15** The daemon's per-intent perform timeout wraps the whole drive future and **drops** it
on expiry; the intent stays `Executing` until the next `reconcile_durable` normalizes it to
`Pending`. The `TimeoutExecutor` doc comment says a timeout leaves the intent `Pending` via the
retryable path; that is true only of the standalone executor. Join and recover are never timed
out.

**OPS-16** An **awaiter task** owns an `Awaiting` intent. For a raw `Pay` or `Receive` it MUST:
require `intent.operation_id` (absent → `Permanent`); call the blocking SDK final-state await for
the leg (`await_send` / `await_receive`); map the state to a ledger status — `Success →
Succeeded`, `Refunded → Failed "send refunded"`, `Failed(detail) → Failed detail`, `Claimed →
Succeeded`, `Expired → Failed "receive expired"`; run `prepare_raw_operation_terminal`
(`OPS-46`) **before** taking any lease, since it may touch the SDK; then take the actor
external-terminal lease, run `finalize_raw_operation` (ledger advance and `Awaiting → Done |
Failed` in one transaction, adopting the operation id), and release the lease. A `DirectInflow`
awaiter MUST: re-read the intent (`Done | Failed` → finished; any status other than `Awaiting`
→ `Permanent`); `backfill_move_record`; require `recv_op` (absent → `Permanent`);
`await_receive`; then under the lease `settle_move` (`put_move_if_attempt` with phase `Settled`,
or `Failed` with outcome `"receive invoice expired before payment"` or the SDK detail; a `false`
result → `Retryable`) and `finalize` (CAS `Awaiting → Done | Failed`; a false CAS is success).

Failure classification: an `AwaitOperationError` raised **before** a terminal observation is
`Retryable` for its `Retryable` variant and `Permanent` for `MissingOperation`,
`NotLightningOperation`, `WrongOperationKind` and `UnsupportedLnv2Module`; **any** local error
after the SDK returned a terminal — preparation, finalization, lease begin or end — is
`Retryable("post-terminal-observation local fault: …")` even when the underlying error is
`Permanent`. A `Retryable` outcome sleeps 1 s off the actor and reports
`DriverFinished{retry_awaiter: true}`, which re-spawns the awaiter only while the same attempt is
still `Awaiting`. A `Permanent` outcome routes `SetStatus(Failed, "service awaiter permanent
failure: …")` through the actor; if that transition errors, the awaiter sleeps 1 s and retries
ownership instead. A caller awaits through `resolve_await(key, target, deadline)`: an unknown key
is `NotFound`; target `InvoiceArtifact` resolves as soon as the intent's `invoice` or the move
record's `invoice` exists; target `Terminal` resolves on `Done | Failed`; otherwise a waiter is
parked and every transition on the key re-evaluates it; the deadline is `Timeout`.

**OPS-41** While an external-terminal lease or a membership lease is live, the actor refuses to
issue or validate a tick-plan token or a balance-facts token, so one raw terminal's database
write fences all tick planning for its duration. `ADR-0024` calls this the narrow exception; it
is the only place a driver holds anything across a write, and never across network IO.

**OPS-43** `drive_intent_step(journal, executor, intent)` is the only path by which a journaled
intent is **performed** — issued or re-issued to the SDK — in the daemon (through
`ActorJournal`) and standalone alike. The one other route to network IO is the awaiter of an
`Awaiting` raw pay, receive or direct-inflow, which calls the SDK's await path directly and
never re-performs (`OPS-16`). In order:

1. `intent.status == Pending` → `set_status_if(key, attempt, Pending, Executing)`. `Ok(false)`
   (another claimant, a different attempt, an absent key, or a forbidden transition) → counted
   `skipped`, return `Ok(None)`, no IO. `Err` → counted `failed`, return the error. The claim
   blanks `evacuation_refusal` in the same transaction.
2. `intent.status == Executing` → **no claim**: a crash-recovery resume or a concurrent scan is
   re-performed under the executor's idempotency (`OPS-17`–`OPS-27`).
3. Take the process-local in-flight guard, a `(store_id, key)` set shared by every driver in the
   process; a key already held → `skipped`, `Ok(None)`. The guard is released when the drive
   future completes or is dropped (`OPS-15`).
4. Re-read the intent: it MUST be `Executing` at the same key and attempt, else `skipped`,
   `Ok(None)` (a stale scan snapshot); a read error → `failed`, `Err`.
5. `perform(current)`, then:

| `perform` result | journal write | counts |
|---|---|---|
| `Ok(Done)` | `set_status(key, attempt, Done, None)` | `performed` |
| `Ok(Awaiting \| AwaitingAlreadyInFlight)` | `set_status(key, attempt, Awaiting, None)` | `performed` |
| `Err(Retryable(m))` | `reset_retryable(key, attempt, None)` — `Executing → Pending`, marker blanked | `failed` and `retryable` |
| `Err(StructuralEvacuationRefusal(e))` | `reset_retryable(key, attempt, Some(e))` | `failed` and `retryable` |
| `Err(Permanent(m))` | `set_status(key, attempt, Failed, Some(m))` | `failed` |
| `Err(Unsupported)` | `set_status(key, attempt, Failed, Some("executor does not support this action"))` | `failed` |

The status write after `Ok` is a plain `set_status`, not a CAS; if it fails the error is returned
and the intent stays `Executing` with its side effect done. The next reconcile normalizes it to
`Pending` and re-performs; reassembly then reaches the terminal it already has (a `Settled`
record → `MoveStep::Done` → `Done` with no new IO; a raw op → the awaiter path of `OPS-16` or the
hash attach of `OPS-17`). A failing retryable reset likewise leaves `Executing`;
`reset_retryable` requires the current `Executing` attempt, else `Permanent`.

**OPS-45** Pre-fund admission (`enforce_pre_fund_admission`) runs inside `perform` on every pass
before any new SDK issue: raw `Pay` after the hash lookup and invoice validation, raw `Receive`
after the correlation lookup, `Move` and `DirectInflow` before `assemble_record`. Endpoints:
`Move` → (source, destination); `DirectInflow`, `Receive` → destination only; `Pay` → source
only; `Evacuate`, `Join`, `Recover` → none, returns `Ok`. It is skipped when the cached move
record is trusted (`allocator_record_is_trusted`, `OPS-9`) and its phase is `Sending | Settled |
Refunded | Failed | Stranded`. Reservations: `reservation_intents()` minus this key; a scan
failure → `Retryable("reservation scan failed before funding; leaving the intent pending: …")`.
An intent carrying an allocator goal (`DOM-17`: an agent `Move` with a funding reason, or an
agent `Evacuate`) uses the allocator projection, built from the move records of the other live
`Move`/`Evacuate`/`DirectInflow` intents (a `Permanent` decode error keeps that intent strict
with a warning; any other read error → the same `Retryable`); every other intent uses the strict
projection. Balances: the source is always sampled; the destination only when a hard cap is
configured (which cap, per entry point, is `OPS-7`). Then `admit_intent(intent, balances,
hard_cap, reservations)`
(`OPS-7`); its refusal is `Permanent` and terminalizes the intent.

**OPS-46** Raw terminal preparation and finalization. `prepare_raw_operation_terminal(oracle,
fed, op, key, attempt, role)` runs off-lease and yields a **not-recording** preparation (no fence)
when: the key has no ledger row; the row's repair fence cannot be captured; the fence's attempt is
not `attempt`; `raw_operation_row_matches(row, role, fed, op)` fails; or the row records no op id
and `find_op_by_correlation_key(fed, attempt correlation key)` does not return exactly `op`
(or errors). Otherwise it calls `observe_op` (non-blocking): no terminal →
`Retryable("raw operation {op} for --key {key} is still in flight")`; a terminal gives
`observed_status` (`Succeeded`/`Failed`) and a definitive fee update.
`finalize_raw_operation(key, status, error, prepared)`: a not-recording preparation → the
stale check below; `status ≠ observed_status` → `Permanent("raw terminal status … conflicts
with observed …")`; else `finalize_raw_terminal_if_fenced`, one transaction that requires the
intent at the fence attempt, the action/role pair `Pay`/`Send` or `Receive`/`Receive`, a
non-terminal status whose transition to the target is allowed, and the action's federation equal
to the fence's; adopts the observed op id (a **different** recorded op id → no-op); advances the
ledger row only if it is still at the fence's seq, federation, role and op id **and** at the
fence's expected status — or is already an unrepaired terminal equal to `status`, in which case
only the intent half is written; then writes the intent `Done`/`Failed` with `error`. A no-op
(`raw_terminal_noop_is_stale`): if the same attempt is still non-terminal →
`Retryable("raw terminal {preparation|fence} no-op left attempt {n} for --key {key}
non-terminal; retrying ownership")`, else success. `OPS-16` maps every error here to `Retryable`.

## Perform, per action

**OPS-17** `Pay`. First `find_send_op_by_payment_hash(from, hash)` — the newest lnv2 send op on
`from` whose invoice carries the hash — then `observe_op` on it (non-blocking):

| found op | disposition |
|---|---|
| none | issue a payment (below) |
| non-terminal, `attempt > 0` | `Retryable("payment hash is still in flight for an earlier attempt")` |
| non-terminal, `attempt == 0` | record the op id (`set_operation_artifact_if_attempt`; stale → `Retryable`), return `AwaitingAlreadyInFlight` |
| terminal, succeeded | record the op id, then `record_raw_observation_if_attempt` (ledger `Succeeded` with definitive fees; stale → `Retryable`), return **`Done`** — no awaiter ever runs |
| terminal, refunded or failed | ignored; issue a payment (below) — the SDK's per-invoice dedup returns `AlreadyInFlight` for the same dead op and the awaiter terminalizes it |

Issue: parse the invoice (`Permanent("parsing raw pay invoice: …")`) and reject an expired one
(`Permanent("raw pay invoice has expired")`); pre-fund admission (`OPS-45`). Candidates: the
break-glass gateway for this key alone, else `mc.gateways(from)`; `Action::Pay.gateway` is never
consulted and there is no pin (`ADR-0030`). Per candidate `gw_quote = send_gateway_fee(from, gw,
invoice).on(amount)` then `fed_quote = send_fee_quote_for_amount(from, amount + gw_quote)`;
either quote failing skips the candidate; keep the cheapest `gw_quote + fed_quote ≤ fee_cap`.
None kept: some candidate quoted → `Permanent("raw pay fee quote {lowest} msat exceeds fee cap
{cap} msat")`; nothing quoted from the vetted list → `Permanent("no lnv2 gateway produced a send
fee quote for federation {hex}")`; nothing quoted from a break-glass → `Retryable("break-glass
gateway {url} produced no send fee quote for federation {hex}")`. `mc.pay(from, invoice,
gateway, meta)` with the raw meta (`STO-34`); its error variants map per `FMI-17`:
`InvoiceRejected | RouteRejected → Permanent`, `Transport → Retryable`. Persist the operation id (stale attempt → `Retryable`) and return
`Awaiting` for `Started`, `AwaitingAlreadyInFlight` for `AlreadyInFlight` (both journal as
`Awaiting`). Crash between `pay` and the artifact write: the re-drive finds the op by payment
hash. Terminalization is the awaiter's (`OPS-16`).

**OPS-18** `Receive`. If `intent.operation_id` is recorded: require `intent.invoice`
(`Permanent`), run the committed-fee check below and return `Awaiting`. Else
`find_receive_artifact_by_correlation_key(to, correlation key)` (`STO-34`): found → committed-fee
check, record op id and invoice (stale → `Retryable`), return `AwaitingAlreadyInFlight`. Else
pre-fund admission (`OPS-45`, destination only) and candidates as for pay (break-glass for the
key, else `mc.gateways(to)`); per candidate `gw_quote = receive_gateway_fee(to, gw).on(amount)`,
contract `= amount − gw_quote` which must be `≥ 5,000` msat, then `fed_quote =
receive_fee_quote(to, contract)`; a failed gateway or federation quote marks "quote unavailable"
and skips the candidate; keep the cheapest `gw_quote + fed_quote ≤ fee_cap`. None kept:

| observed | disposition |
|---|---|
| some total quoted, **every** candidate quoted | `Permanent("raw receive fee quote {lowest} msat exceeds fee cap {cap} msat")` |
| some total quoted, a quote was unavailable | `Retryable` with the same text |
| no total, a minimum-contract refusal, every candidate quoted | `Permanent("raw receive amount too small: net {amount} msat produces a {contract} msat incoming contract; lnv2 requires at least 5000 msat")` |
| otherwise | `Retryable("no lnv2 gateway produced a receive fee quote for federation {hex}")`, or `"break-glass gateway {url} produced no receive fee quote for federation {hex}"` |

`mc.receive(to, amount, gateway, meta)` — **not grossed up**, raw meta `STO-34`, invoice expiry
3,600 s (`FMI-16`); an error → `Retryable`. The committed-fee check
(`verify_raw_receive_fee_cap`): read the committed contract, `actual = amount − committed +
receive_fee_quote(to, committed)`; `actual > fee_cap` → persist op id and invoice **first** (so
the orphan is recorded) then `Permanent("raw receive committed fee {actual} msat exceeds fee cap
{cap} msat")`. Persist op id and invoice (stale → `Retryable`), return `Awaiting`.

**OPS-19** `DirectInflow`, `Move` and `Evacuate` share one plan and one step loop. `MovePlan`:
`Move`/`Evacuate` are `send_required` with a source; `DirectInflow` is receive-only; only
`Evacuate` carries `fee_cap_components`. The gateway hint is not on the plan. Pre-fund
admission (`OPS-45`) runs before `assemble_record` for `Move` (both ends) and `DirectInflow`
(destination), **not** for `Evacuate`, and is skipped once a trusted record is past `Invoiced`.

**OPS-20** `assemble_record` reconstructs the working move record from the `0x02` cache plus
`backfill_ops` on the destination (and on the source when distinct), filtered by `move_id ==
operation_correlation_key` (`STO-33`). Backfill pages the op-log newest-first to exhaustion; per
leg the **first** (newest) matching artifact wins; a `Receive` artifact without an invoice is
dropped entirely (never `recv_op` without `invoice`); `amount` is the first matching artifact's,
either leg; `fee_cap` the first artifact carrying one. Precedence: amount artifact > cached >
planned; cap artifact > cached, but the cached cap only when the cache holds `recv_op` or
`send_op` > planned; a leg the artifacts do not supply keeps the cached op id and invoice;
`outcome`, `preimage` and both quoted fees come only from the cache. Phase: a cached terminal
phase (`Settled | Refunded | Failed | Stranded`) is preserved; otherwise `send_op` present →
`Sending`, else invoice present → `Invoiced`, else `Created`. Gateway
(`gateway_from_cache_or_recovered`, `FMI-14`, `ADR-0030` rule 4): a move is **committed** when
the cache holds any of invoice, `recv_op` or `send_op`, **or** the op-log recovered an artifact
for this key. Not committed → the break-glass for this key, else resolve afresh
(`resolve_move_gateway`): a cached draft's gateway is **never** replayed, send-required or not.
Committed with a cache → the cached gateway. Committed without a cache: receive-only with a
recovered invoice → the local sentinel `recovered-receive-only-gateway-not-used`; send-required →
resolve afresh (`F7`).

**OPS-21** `Evacuate` only, and only when no artifact exists yet (`has_move_artifact`: invoice,
`recv_op` or `send_op`): `size_fresh_evacuation`. The ask is the **action's** `amount`, not the
cached one — a pre-artifact record is re-sized from the intent every pass. When a hard cap is
configured, clamp it to the destination's cap room: `room = cap − balance(to)` saturating;
`room == 0` → `Permanent("no cap room at destination: federation {hex} holds {dest} msat
at/above the per-fed cap {cap} msat, so an evacuation cannot drain into it")`; else `desired =
min(amount, room)`. With no hard cap the ask is unclamped. Read the source's spendable
(`balance(from)`); take the cap rule from the action's components (a legacy intent without
components uses `{base: stored cap, bps: 0}`); snapshot one gateway fee per leg for the whole
search — `receive_gateway_fee(to, gw)` and `direct_swap_send_gateway_fee(from, gw)`
(`send_fee_minimum`) — warn, never refuse, on a ppm outside 15,000 send / 5,000 receive
(`FMI-19`); run the search of `OPS-44` (`ALC-22`). `Sized(n)` sets `rec.amount = n, rec.fee_cap
= cap.at(n)`; `Refused(r)` is `Retryable("no evacuable amount fits: desired {d} msat, source
balance {s} msat, evacuation fee cap {base} msat + {bps} bps (retrying — a later tick may succeed
once in-flight funds settle or the quote moves); {r}")`; `StructuralRefused(evidence)` is the
marker (`OPS-31`). A quote transport fault anywhere in the search is `Retryable` and aborts it.

**OPS-44** The evacuation sizing search (`size_evacuation`), given `desired`, `spendable`,
`cap` and a quote `q(n)`.

*Quote* (`quote_fresh_send_required_cost`): `n = 0` → `Unquotable{shortfall: None}`; gross `n`
up at the snapshotted receive gateway fee (`FMI-18`, `OPS-22`'s fixed point); contract
`< 5,000` msat → `Unquotable{None}`; `send_gw = send_fee.on(invoice)`; `send_tx =
send_fee_quote_for_amount(from, invoice + send_gw)`; the mint's `InsufficientBalanceError` →
`Unquotable{shortfall: requested − available}`; any other error → `Retryable` (aborts the
search); else `Priced{invoice, receive_quote, send_quote = send_gw + send_tx}` with `delivered =
invoice − receive_quote`, `total_fee = receive_quote + send_quote`, `source_debit = invoice +
send_quote` (all saturating).

*Verdicts*: **affordable** iff `source_debit ≤ spendable`, shortfall `source_debit −
spendable`; an `Unquotable` candidate is never affordable, shortfall = the mint's gap or
`u64::MAX` when none; **fits cap** iff `total_fee ≤ cap.at(delivered)` (exact-cap admitted);
**combined** fits iff both, shortfall = `max(affordability shortfall, total_fee −
cap.at(delivered))` (0 for the cap term of an `Unquotable`).

*Constants*: floor `F = 5,000` msat; `NOTE_BOUNDARY_PROBES = 8`; oscillation bound `A = 6 +
2·(300·tiers + 100) + 2,100 + 2·ceil(spendable × 1,000 / 1,000,000)` with `tiers = 64 −
leading_zeros(max(spendable, 1))` (the mint's 100 msat base and 1,000 ppm ceiling), in `u128`
saturated to `u64`.

*Boundaries* (the mint's denominations are powers of two): `above(a, hi)` = for `k` from 63
down to 0, `((a >> k) + 1) << k` when `≤ hi`, deduplicated, at most 8, largest first;
`below(a, F)` = for `k` from 0 up, `floor(a / 2^k) × 2^k − 1` while `≥ F`, deduplicated, at
most 8, nearest first.

*Bisection* `largest_fitting(F, hi, A, probe)`: `hi < F` → none; `lo = F − 1`; while `lo <
hi`: `mid = lo + ceil((hi − lo) / 2)`; `probe(mid)` fits → `lo = mid`; else if its shortfall
`≤ A` and some candidate of `above(mid, hi)`, probed in order, fits → `lo = that candidate`;
else `hi = mid − 1`. Result `lo` if `lo ≥ F`, else none.

*Search* (`search_evacuation_net`): (1) fast path `q(desired)`: if `Priced`, affordable and fits
cap → sized at `desired`; if affordable, `hint = desired`. (2) `desired < F` → not sized. (3)
pass 1: `p1 = desired` if the fast path was affordable, else `p1 = largest_fitting(F, desired,
A, affordability of q(·))`; none → not sized. (4) re-quote `q(p1)`: not `Priced` → not sized;
`hint = p1` iff affordable, else `hint` cleared; combined fits → sized at `p1`. (5) pass 2:
`p2 = largest_fitting(F, p1, A, combined of q(·))`; if some `p2`, re-quote it and accept only if
the fresh combined verdict fits.

*Viability post-check* (`evacuation_viability`) on a sized `(n, cost)`: `total_fee ≤
delivered` → `Sized(n)`; else probe `below(n, F)` in order and return `Sized` at the first
candidate whose fresh quote is `Priced` with `total_fee ≤ cap.at(delivered)`, `source_debit ≤
spendable` and `total_fee ≤ delivered`; none → `Refused("the largest chunk this route can carry
costs more than it delivers …")`, never a marker.

*Diagnosis* (`no_fitting_amount_reason`, only when nothing was sized) — every branch is
`Refused` (plain `Retryable`) except the last: no `hint`; `q(hint)` not `Priced`;
`q(hint).source_debit > spendable`; `q(hint)` fits cap ("the cap no longer refuses …"); `q(F)`
not `Priced`; `q(F).source_debit > spendable`; otherwise `low = {delivered, total_fee,
cap.at(delivered)}` from `q(F)`, `high` likewise from `q(hint)`, and `ALC-24`'s `is_structural`
→ `StructuralRefused(EvacuationRefusalEvidence{cap_components: cap, requested_net: desired,
source_spendable: spendable, low, high, diagnostic, measured_at_ms})`, else `Refused`. Only
this branch produces the marker of `OPS-31`. A probe leg (`size_probe_leg_out`) runs the same
search with `cap = {base: leg cap, bps: 0}` and no viability post-check.

**OPS-22** `CreateInvoice`: validate the source end of the gateway if send-required
(`Retryable`); enforce the destination cap unless `Evacuate`, and only when a hard cap is
configured — `balance(to) + rec.amount > cap` → `Permanent("destination would exceed the per-fed
cap ({dest}+{amount} > {cap} msat) for federation {hex}")`; `gross_up` at `rec.amount`
(`FMI-18`; minimum contract → `Permanent("direct inflow amount too small: …")` for every shape);
record the receive quote on the record; **receive-leg cap check** `receive_quote ≤
cap_rule.at(requoted_delivered)` where `requoted_delivered = invoice − receive_quote` and
`cap_rule` is the action's components or `{base: rec.fee_cap, bps: 0}` — over is persisted
first, then `Retryable` for `Evacuate`, `Permanent` otherwise, message "fee over cap (receive
side {q} msat exceeds the {cap} msat cap at the {d} msat this would deliver)"; and for
`Evacuate` a viability pre-check `receive_quote > requoted_delivered` → persist, then
`Retryable`. Then `net = delivered_move_amount(requoted_delivered, rec.amount)` (= the delivered
net when under the ask, else the ask) and `delivered_cap = cap_rule.at(net)`; nothing on the
record is lowered before the receive commits.

**OPS-24** The persistence order at `CreateInvoice` is load-bearing: the draft record (phase
`Created`, gateway, receive quote, no invoice, no receive op) is written **before** `mc.receive`;
the receive op is committed carrying `MoveMeta {move_id, role, amount: net, fee_cap:
delivered_cap, from, to}` plus the quoted contract in meta (`STO-33`); the committed contract is
read back and verified (`OPS-23`); **only then** — after `rec.amount := net` and `rec.fee_cap
:= delivered_cap` when `net < rec.amount` (a no-op cap for a non-evacuation rule) — are
`invoice`, `recv_op` and phase `Invoiced` written to the record. A
committed-then-refused receive therefore leaves an orphan the record does not name, and the
ledger row keeps the planned pair (`F8`). Changing this order would let `has_move_artifact` stop
a later occurrence from re-sizing against fresh prices, so it needs its own design pass.

**OPS-23** The never-over check: the committed incoming contract must equal the quoted one,
else `Permanent` "gateway receive fee changed between quote and mint; re-run". lnv2 re-fetches
`routing_info` at mint time, so a fee drop would otherwise over-credit. The invoice is unpaid
and unsurfaced; the orphan expires. The same check re-runs on every replay that skips
`CreateInvoice` (`verify_recovered_receive_contract`); a receive op whose meta carries no quoted
contract is `Permanent("receive op is missing the quoted contract amount; re-run under a fresh
occurrence")`.

**OPS-25** The enforced cap survives replay because it is **in the receive op's `MoveMeta`**
(`fee_cap: Option<Msat>`, `serde(default)`, absent meaning none and not zero) and reassembly
prefers that over the planned cap once a leg is committed (`DEF-3`). A crash plus cache loss
cannot resurrect the planned-amount cap.

**OPS-26** `Pay` step: verify the recovered receive contract (client closed → `Retryable`;
missing or corrupt with the client open → `Permanent`); parse the fixed invoice; expired →
`Permanent` "move invoice expired before the send leg could pay it"; re-quote the send leg:
`receive_quote = invoice_msat − rec.amount`, `send_gw = send_gateway_fee(from, rec.gateway,
invoice).on(invoice_msat)`, `send_quote = send_gw + send_fee_quote_for_amount(from, invoice_msat
+ send_gw)` (a quote error → `Retryable`); persist both quotes (this also restores
`receive_fee_quoted` after a cache loss); **both-leg cap check** on `rec.fee_cap`: the fixed receive quote alone over
the cap → `Permanent`, the total over → `Retryable`; for `Evacuate` the viability check
(`receive > net` → `Permanent`, `total > net` → `Retryable`); `mc.pay` accepting
`Started | AlreadyInFlight`; persist `send_op`, phase `Sending`.

**OPS-27** `AwaitSettle`: await the **send first**. Any await error → `Retryable`, reservations
retained. `Success(preimage)` → persist the preimage **before** awaiting the receive; any receive
await error → `Retryable`; `Claimed → Settled`; `Expired | Failed → Stranded` with the anchor
string "send settled but receive was not credited". `Refunded → Refunded`; `Failed(msg) →
Failed` — what each `Failed` proves is `FMI-37`, and a forfeited send can still arrive as
`Success` (`FMI-23`). `Settled → Done`; every other terminal phase → `Permanent(outcome)`. **Stranded is
therefore exactly: a settled send with a preimage and an op-terminal non-claim on the receive.**
It is terminal; the allocator view releases both reservations for it. A move perform is
synchronous to `Done`; a direct inflow returns `Awaiting` after minting and is finalized by its
awaiter. A direct inflow that resumes at `AwaitSettle` (invoice and `recv_op` recovered,
`send_required == false`) re-verifies the committed contract (`OPS-23`), re-persists the
reassembled record and returns `Awaiting` with no further IO.

**OPS-28** Four killpoints, compiled only under `debug_assertions` and keyed by
`WALLET_CLI_CRASH_AT`, each with a proven resume:

| Killpoint | State | Resume |
|---|---|---|
| `before-move-record` | receive op committed, record has no invoice/recv_op | backfill by `move_id`; no second mint |
| `after-receive-commit` | record has invoice, no pay | `next_step → Pay` |
| `before-send` | invoice exists, no send | pays exactly once by SDK dedup |
| `after-send-commit` | send committed, record lacks `send_op` | backfill, or re-pay dedups to `AlreadyInFlight` |

`CNF-12` proves all four live. At `before-move-record` the gateway replays from the pre-receive
draft written by `OPS-24`; if that cache is also lost, a send-required move re-resolves its
gateway (`OPS-20`, `F7`) and pays the recovered invoice through whatever it resolves.

**OPS-29** Where fee caps bind, and on what base:

| Action | Pre-mint / pre-fund | Both legs | Base |
|---|---|---|---|
| Pay | cheapest `gw + fed ≤ fee_cap`, else `Permanent` | — | absolute `fee_cap` (default `max_fee`) |
| Receive | cheapest fitting, then committed-contract re-check | — | absolute |
| DirectInflow | receive leg ≤ `fee_cap` (`Permanent`) | — | absolute; gross-up bounded by it |
| Move | receive leg ≤ `fee_cap` (`Permanent`); fallback route priced at the amount | fixed receive + re-quoted send ≤ `fee_cap` | `floor(amount × max_fee_bps_of_move / 10 000)` stamped by the allocator (`ALC-7`) |
| Evacuate | sizing at delivered net; receive leg ≤ `cap.at(delivered)` (`Retryable`) | same, on `cap.at(executed net)` + viability | `base + floor(net × bps / 10 000)` (`ALC-20`) |

**OPS-42** `Join`: parse (`Permanent`), `mc.join` under a membership lease in the daemon (error
→ `Retryable`), compute `newly_joined = !membership_preexisting && (sdk_reported_new ||
registered_invite == the intent's invite)` where `registered_invite` is the registry row's invite,
read only when neither of the first two terms holds; mark the candidate `UserApproved` if the
actor is `User`; `record_join_outcome` in the ledger (a stale attempt → `Retryable`); return
`Done`. `Recover`: parse, `mc.recover` under the lease, **any** error → `Permanent`, `Done`.
Neither has a fee cap or a reservation.

## Evacuation supersession

**OPS-31** The **marker** is `Intent.evacuation_refusal`. It is written only by
`reset_retryable(.., Some(evidence))` on `StructuralEvacuationRefusal`, and cleared by the
`Pending → Executing` claim, any ordinary retryable reset, the deliberate clear disposition, or
the exchange (the retired parent keeps it). The deliberate clear is
`clear_marked_evacuation_if_pending(planned_parent)`, one `autocommit`: the stored row must equal
the planner's parent byte-for-byte, be `Pending`, agent, `Evacuate`, marked, with no
`operation_id` or `invoice`, no `0x0c` sidecar, no other live agent evacuation for the source,
and any move record pristine (else `Permanent`); it blanks the marker only — no driver, no wake —
and is invoked by `CommitTick` for a no-child disposition and by `ReconcileDecide` for the parked
handoff. `show` projects `evacuation_refusal_active` as
`true` for an exact readable `Pending` agent `Evacuate` with a marker, `false` for an exact
readable intent without one, and omits it when no intent is readable; `history` omits it because
it does no per-row intent lookup.

**OPS-30** The exchange (`replace_marked_evacuation`, one `autocommit` transaction): full-row
compare-and-swap on the parent; evidence validated; parent `Pending`, agent, same source, no
artifacts; the child's occurrence strictly greater and key distinct; no other live agent
evacuation for the source; child namespace empty; any parent move record pristine (phase
`Created`, trusted, no invoice, op ids, preimage or outcome) else `Permanent`. Then: parent →
`Failed` with "superseded after measured structural evacuation refusal; successor <key>" and its
ledger row advanced; child intent `Pending` at attempt 0 with its own `Started` row; sidecars
`0x0c` and `0x0d` (`STO-25`). A replay with a coherent sidecar validates and returns success
without writing. `DEF-7` is why this exists; `DEF-23` is what its concurrency test cost to make
real.

**OPS-32** The daemon's commit branch (`commit_evacuation_replacement`) additionally requires the
child's `fee_cap_components` to equal the **current** policy cap and `cap.at(amount)` to equal
its `fee_cap` (else `PolicySuperseded`); fresh balances for both ends; the re-read parent equal
to the planned one byte-for-byte (else no child, marker retained); qualification re-checked
(`ALC-23`); balance facts unchanged; fresh blockers excluding the parent; the allocator
projection excluding the parent; `admit_intent(child)`. On a `Permanent` error the marker is
retained as definitely uncommitted; on any other error the actor re-reads the sidecar and parent
to classify committed / uncommitted / ambiguous, and an ambiguous outcome poisons goal
admissions and balance facts until restart. Success bumps both endpoints' generations, resolves
waiters on the old key, and spawns the child's driver.

**OPS-33** The standalone path requires `--occurrence` strictly greater than the parent's agent
occurrence and refuses `u64::MAX` up front; every replacement-path error retains the marker; a
confirmed-uncommitted outcome bails; a committed one applies just the child with reservations
excluding the parent. Standalone `status` is dry: a stale occurrence warns and returns the
diagnostics with no would-run decisions.

## Reconcile

**OPS-34** Core `reconcile` performs **one** `drive_intent_step` on every intent in `pending()`
(`Pending | Executing`) per pass; despite its name, `drive_to_terminal` does not loop. A step that
returns `Retryable` or a structural refusal leaves the intent `Pending` for a later pass. It never
touches `Awaiting`, `Done` or `Failed`. Re-performing an `Executing` intent
relies on the executor's idempotency (`OPS-17`–`OPS-27`, `OPS-43`). The standalone
`Runtime::reconcile` first runs `backfill_move_record` for every `pending()` and `awaiting()`
intent (a failure is logged and that intent skipped), then core `reconcile` under the
`TimeoutExecutor`, then a best-effort `repair_ledger` (`STO-24`), then reports the `awaiting()`
set and goal blockers from a fresh `pending()` scan. Standalone `await_move` refuses a `Pending |
Executing` intent ("intent {key} is {status}, not awaiting — run `direct-inflow`/`reconcile`
first").

**OPS-35** The daemon's `reconcile_durable`, per pass: scan `pending()` (a scan fault fails the
reconcile and the scheduler treats eligibility as unknown); compute goal blockers before any
filtering; preempt any in-flight probe whose source federation has a pending evacuation
(recording the probe `Failed` "probe preempted by evacuation; no attempt recorded"); per intent
apply the marker policy — a **planner-owned marker** (`marker_is_planner_owned`: agent occurrence
`< u64::MAX`, `Evacuate`, and a marker the current policy cap qualifies to replace per `ALC-23`)
is skipped under `PreservePlannerOwned` (`ReconcileDurable`, `RecoverDriverOwnership`), skipped
and parked for the planner under `CaptureForPlanner` (`ReconcileDecide`; parked only while
`Pending` with no `operation_id` or `invoice` and goal admissions unpoisoned), and re-driven under
`RedriveWithoutPlanner` (`ReconcileRecoveryOnlyCycle`; still skipped while poisoned; its next
renewed marker's wake is suppressed once) — then fail orphaned probe legs whose session is gone
(`Failed` "probe session is no longer active"), skip registry-owned keys, normalize `Executing →
Pending` (plain `set_status`, marker untouched), `ensure_driver`; then scan `awaiting()` and spawn
awaiters for unowned keys. `POST /v1/reconcile` runs this then a best-effort ledger repair
(`STO-24`).

**OPS-36** Reconcile never re-performs: `Awaiting` intents (re-attached only), `Done`, `Failed`,
keys a live driver owns, and planner-owned markers under the preserve or capture policies.

**OPS-37** Repair paths: `repair_ledger` (`STO-24`); `set_raw_terminal_if_fenced`, the one
reservation-releasing write the repair scan routes through the actor, fenced on ledger seq, op,
role, status and intent attempt, `Pay | Receive` only; `backfill_move_record` from the op-log;
`Runtime::direct_inflow` completing an `Awaiting` intent whose record is already terminal
(a crash between `settle_move` and `finalize`).

## Errors

**OPS-38** `ServiceError` and what each means to a caller:

| Variant | Meaning | HTTP |
|---|---|---|
| `Refused {reason, message}` | admission or commit refused; nothing journaled for a fresh key | 422 / 409 (`API-6`) |
| `Storage(msg)` | a durable read or write failed or an internal invariant broke; a fresh agent admission may or may not have committed | 500 |
| `NotFound` | `resolve_await` on an unknown key | 404 |
| `DestinationUnavailable` | fresh destination-side admission to a joined-but-unopened federation; nothing journaled | 503 |
| `Timeout` | the await deadline elapsed; the operation is still live | 504 |
| `ShuttingDown`, `ActorStopped` | the actor is draining or gone | 503 |

**OPS-39** `RefuseReason` is derived from `ExecError` by `refusal_from_exec` **by substring
match on the message**, tested in this order:

| message | reason | producer |
|---|---|---|
| contains `conflicts with the existing request` | `SizingConflict{field: "request sizing"}` | core `validate_attach`: "intent {key} conflicts with the existing request's sizing fields" |
| contains `insufficient balance after reservations` | `InsufficientAfterReservations` | `admit_intent`: "insufficient balance after reservations on federation {hex}: need {n} msat, have {m} msat" |
| contains `per-fed cap` | `OverCap` | `admit_intent`: "destination would exceed the per-fed cap after reservations on federation {hex}: {committed} > {cap} msat" |
| starts with `journal:` or `journal db error:` | `StorageError` | every journal validation error is prefixed `journal:`; every RocksDB error `journal db error:` |
| anything else | `Conflict` | — |

A `ServiceError::Storage` raised on the admission path (the key read, the goal-blocker scan, a
probe-record read) is re-wrapped as `Refused{StorageError}` (`storage_refusal`). Reasons the
actor assigns directly, not by substring: `Conflict` (goal conflict, the driver cap, the
live-attach, retry-anchor and terminal-anchor refusals of `OPS-8`/`OPS-10`, replacement
occurrence and lease conflicts), `FedHeldByProbe`, `PolicySuperseded`, `PolicyInvalid`,
`BudgetExhausted`. The HTTP status per reason is `API-6`. These strings and prefixes are a
load-bearing contract, not decoration; a rewording changes the wire.

**OPS-40** No code comment may carry a causal taxonomy of an unobserved money state (`DEF-20`).
The code states what a state **is**; the runbook holds the operator's account of how it might
arise and what to do.
