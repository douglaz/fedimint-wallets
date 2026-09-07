# 03 — Operation lifecycle

How a money operation is admitted, executed, resumed after a crash, and terminalized. Read from
`wallet-core/src/executor.rs`, `wallet-fedimint/src/{executor,move_protocol,runtime}.rs`, and
`wallet-fedimint/src/service/{mod,actor,driver}.rs` on 2026-09-07. This is the part of the
system the rest protects: the property that a two-leg move killed at any of four named points
completes exactly once.

## Intents and their states

**OPS-1** An executable operation is driven by an intent (`DOM-6`). The intent's
`operation_correlation_key()` is the public key on attempt 0 and `retry:<len>:<key>:<n>` on
attempt `n > 0`; that is what rides in the SDK's `custom_meta`, so a recovery cannot attach a
retry to the prior attempt's operation.

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
Reconcile never re-drives `Awaiting`; an awaiter task owns it (`OPS-16`).

**OPS-4** A perform returns `Done`, `Awaiting`, or an `ExecError`: `Retryable(msg)` resets
`Executing → Pending` with the marker cleared and counts as both `failed` and `retryable`;
`StructuralEvacuationRefusal(evidence)` resets to `Pending` with `evacuation_refusal = Some`
(`OPS-31`); `Permanent(msg)` sets `Failed` with `msg` as the ledger row's `error`;
`Unsupported` is `Failed` and reachable only if a refusal reaches perform. A retryable failure
does **not** bump the attempt counter.

## Admission

**OPS-5** Every user verb — in the daemon and in the CLI's standalone mode alike — builds one
`AllocatorDecision` (`reason: UserInitiated`, `actor: User`), samples the source federation's
balance off the actor, and submits one `OpRequest` to the actor. The CLI's standalone money verbs
run the same actor without a scheduler; the `Runtime::pay/receive/do_move/join/await_move`
functions have no production caller. Only `Runtime::tick` bypasses the actor (`OPS-12`).

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
the actor always passes balances, so this hole is reachable only from the unused standalone
runtime verbs.

**OPS-8** Idempotency. A request whose key already exists attaches: `Done` or `Awaiting` → skip
with the existing outcome; `Failed` → the retry path (`OPS-10`); `Pending` or `Executing` →
drive the existing intent. Attach validates shape: `Pay` must match `from, amount, fee_cap,
payment_hash`; `Receive` `to, amount, fee_cap, nonce`; `Join`/`Recover` federation and invite;
`Move`/`Evacuate` `from, to, amount, fee_cap` ignoring the gateway hint; `DirectInflow` `to,
amount, fee_cap`. A mismatch is `Permanent` "conflicts with the existing request's sizing
fields". The 202 response does not say whether the admission was fresh (`API-18`).

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
invoice already consumed its single payment attempt"). `retry_failed_intent` writes `Pending`
at `attempt + 1`, deletes the cached move record, appends a fresh ledger row and repoints the key
index (`STO-9`). Every durable writer is attempt-fenced, so a stale attempt's write returns
`false` and the executor maps it to `Retryable`. An agent decision whose key is `Done`,
`Awaiting` or `Failed` is skipped, not retried; a new occurrence mints a new key.

**OPS-11** Agent decisions are admitted as a batch by `CommitTick` (`ALC-32`). The batch is
refused whole on a stale policy generation, a changed world generation, an invalid tick-plan or
balance-facts token, or multiple occurrences; each decision is then checked for a fresh
destination balance, unchanged balance facts, terminal replay, conflict, admission watermark, and
— for funding moves — that the amount does not exceed the fresh target shortfall, before going
through `OPS-6`.

**OPS-12** The documented admission exception (`ADR-0031`): `wallet-cli --standalone tick`
holds the exclusive lock, plans, re-scans `pending()` for blockers itself, and applies through
`apply_with_allocator_admission` without the actor. The core functions that make this possible
are `pub`, so "the actor is the sole writer of agent intents" is convention, not enforcement
(`F16`).

## The fully-async model

**OPS-13** The actor is one task with a mailbox of 64 owning `ActorState`. It does: admission,
journal transitions (`Upsert, CompareAndSet, ResetRetryable, SetStatus, SetRawTerminal,
DriverFinished, Refresh`), one-shot artifact writes, snapshots, waiter park and resolve,
reconcile scans, token and lease bookkeeping, policy get and put, shutdown. It performs no
network IO. Tick planning, including route pricing, is spawned off the actor.

**OPS-14** A **driver task** runs `drive_intent_step` with an `ActorJournal` that routes writes
through actor transitions and reads the durable journal directly. Drivers are tracked in a
process-local registry keyed by intent key with a generation, an abort handle, and a
`redrive_requested` flag; a `Drop` guard removes the entry only if the generation still matches,
and the task is released only after the guard is armed. `ensure_driver` requests a re-drive if a
driver already owns the key, else spawns an awaiter (status `Awaiting`) or an intent driver.
`finish_driver` re-reads the intent and re-spawns on: same attempt now `Awaiting` (driver-to-
awaiter handoff), same attempt `Pending` with re-drive requested, a newer `Pending` attempt with
re-drive requested, or an awaiter retry — unless the intent is a planner-owned marker. A read
fault schedules ownership recovery with backoff. Cross-restart exactly-once rests on SDK
operation ids, lnv2 dedup and op-log backfill, not on this registry (`ADR-0024`).

**OPS-15** The daemon's per-intent perform timeout wraps the whole drive future and **drops** it
on expiry; the intent stays `Executing` until the next `reconcile_durable` normalizes it to
`Pending`. The `TimeoutExecutor` doc comment says a timeout leaves the intent `Pending` via the
retryable path; that is true only of the standalone executor. Join and recover are never timed
out.

**OPS-16** An **awaiter task** owns an `Awaiting` intent: it calls the SDK's final-state await
for the leg, then under an actor external-terminal lease runs `prepare_raw_operation_terminal`
(SDK observation plus correlation proof) and `finalize_raw_operation` (ledger advance and
`Awaiting → Done | Failed` in one transaction, adopting the operation id), then releases the
lease. A `Retryable` outcome sleeps and re-finishes; a `Permanent` one sets `Failed`. A caller
awaits through `resolve_await(key, target, deadline)`, which returns immediately on a terminal
intent or an existing invoice artifact and otherwise parks a waiter that every transition wakes;
the deadline is `Timeout`.

**OPS-41** While an external-terminal lease or a membership lease is live, the actor refuses to
issue or validate a tick-plan token or a balance-facts token, so one raw terminal's database
write fences all tick planning for its duration. `ADR-0024` calls this the narrow exception; it
is the only place a driver holds anything across a write, and never across network IO.

## Perform, per action

**OPS-17** `Pay`. Find an existing send op by payment hash; if found, attach (a later attempt
against a still-in-flight op is `Retryable`). Validate and reject an expired invoice
(`Permanent`). Pre-fund admission re-projects reservations excluding self and re-samples the
source balance (`OPS-7`). Candidates: the intent's gateway, else the pin, else the vetted list;
quote `send_gateway_fee + send_fee_quote(amount + gw_fee)` per candidate and keep the cheapest
that fits `fee_cap`; none → `Permanent`. `mc.pay` with `role: send` and the correlation key in
meta; invoice or route rejected → `Permanent`, transport → `Retryable`. Persist the operation id
(stale attempt → `Retryable`) and return `Awaiting`. Crash between `pay` and the artifact write:
the re-drive finds the op by payment hash. Terminalization is the awaiter's (`OPS-16`).

**OPS-18** `Receive`. If an operation id is already recorded, verify the committed contract's fee
against the cap and return `Awaiting`. Else find an orphaned receive by correlation key and
attach. Pre-fund admission (destination cap only). Candidates as for pay; per candidate the
contract is `amount − gw_fee`, must be at least the lnv2 minimum (5,000 msat, `Permanent`), and
its federation quote must fit; keep the cheapest. `mc.receive(amount)` — **not grossed up** —
then verify the committed contract against the cap (this can be `Permanent` after the op
committed; the artifact is persisted first so the orphan is recorded), persist op id and invoice,
return `Awaiting`.

**OPS-19** `DirectInflow`, `Move` and `Evacuate` share one plan and one step loop. `MovePlan`:
`Move`/`Evacuate` are `send_required` with a source; `DirectInflow` is receive-only; only
`Evacuate` carries `fee_cap_components`. The gateway hint is not on the plan. Pre-fund
admission runs for `Move` (both ends) and `DirectInflow` (destination), **not** for `Evacuate`,
and is skipped once a trusted record is past `Invoiced`.

**OPS-20** `assemble_record` reconstructs the working move record from the `0x02` cache plus
`backfill_ops` on the destination (and source), filtered by `move_id == correlation key`. Amount
preference: artifact > cached > planned. Cap preference: artifact cap > cached cap only if a leg
is committed > planned. Gateway: the cached record's if it has an artifact or is send-required,
else resolve afresh (`FMI-14`). The phase is re-derived unless the cache is terminal.

**OPS-21** `Evacuate` only, and only when no artifact exists yet: `size_fresh_evacuation`. Clamp
the planned amount to the destination's cap room (zero room → `Permanent`); read the source's
spendable; take the cap rule from the action's components (a legacy intent without components
uses `{base: stored cap, bps: 0}`); snapshot one gateway fee for both legs; run the two-pass
search over delivered net (`ALC-22`), the viability post-check `total_fee ≤ delivered_net`, and
the diagnostic. `Sized(n)` sets `rec.amount = n, rec.fee_cap = cap.at(n)`; `Refused` is
`Retryable`; `StructuralRefused(evidence)` is the marker (`OPS-31`).

**OPS-22** `CreateInvoice`: validate the source end of the gateway if send-required
(`Retryable`); enforce the destination cap unless `Evacuate` (`Permanent`); `gross_up`
(`FMI-18`, minimum contract `Permanent`); record the receive quote; **receive-leg cap check**
`receive_quote ≤ cap.at(requoted_delivered)` — over is `Retryable` for `Evacuate`, `Permanent`
otherwise — and for `Evacuate` a viability pre-check; compute the never-over net and the
delivered fee cap.

**OPS-24** The persistence order at `CreateInvoice` is load-bearing: the draft record (phase
`Created`, gateway, receive quote, no invoice, no receive op) is written **before** `mc.receive`;
the receive op is committed carrying `MoveMeta {move_id, role, amount: net, fee_cap:
delivered_cap, from, to}` plus the quoted contract in meta; the committed contract is read back
and verified (`OPS-23`); **only then** are `invoice` and `recv_op` written to the record. A
committed-then-refused receive therefore leaves an orphan the record does not name, and the
ledger row keeps the planned pair (`F8`). Changing this order would let `has_move_artifact` stop
a later occurrence from re-sizing against fresh prices, so it needs its own design pass.

**OPS-23** The never-over check: the committed incoming contract must equal the quoted one,
else `Permanent` "gateway receive fee changed between quote and mint; re-run". lnv2 re-fetches
`routing_info` at mint time, so a fee drop would otherwise over-credit. The invoice is unpaid
and unsurfaced; the orphan expires.

**OPS-25** The enforced cap survives replay because it is **in the receive op's `MoveMeta`**
(`fee_cap: Option<Msat>`, `serde(default)`, absent meaning none and not zero) and reassembly
prefers that over the planned cap once a leg is committed (`DEF-3`). A crash plus cache loss
cannot resurrect the planned-amount cap.

**OPS-26** `Pay` step: verify the recovered receive contract (client closed → `Retryable`;
missing or corrupt with the client open → `Permanent`); parse the fixed invoice; expired →
`Permanent` "move invoice expired before the send leg could pay it"; re-quote the send leg and
persist both quotes; **both-leg cap check** on `rec.fee_cap`: the fixed receive quote alone over
the cap → `Permanent`, the total over → `Retryable`; for `Evacuate` the viability check
(`receive > net` → `Permanent`, `total > net` → `Retryable`); `mc.pay` accepting
`Started | AlreadyInFlight`; persist `send_op`, phase `Sending`.

**OPS-27** `AwaitSettle`: await the **send first**. Any await error → `Retryable`, reservations
retained. `Success(preimage)` → persist the preimage **before** awaiting the receive; any receive
await error → `Retryable`; `Claimed → Settled`; `Expired | Failed → Stranded` with the anchor
string "send settled but receive was not credited". `Refunded → Refunded`; `Failed(msg) →
Failed`. `Settled → Done`; every other terminal phase → `Permanent(outcome)`. **Stranded is
therefore exactly: a settled send with a preimage and an op-terminal non-claim on the receive.**
It is terminal; the allocator view releases both reservations for it. A move perform is
synchronous to `Done`; a direct inflow returns `Awaiting` after minting and is finalized by its
awaiter.

**OPS-28** Four killpoints, compiled only under `debug_assertions` and keyed by
`WALLET_CLI_CRASH_AT`, each with a proven resume:

| Killpoint | State | Resume |
|---|---|---|
| `before-move-record` | receive op committed, record has no invoice/recv_op | backfill by `move_id`; no second mint |
| `after-receive-commit` | record has invoice, no pay | `next_step → Pay` |
| `before-send` | invoice exists, no send | pays exactly once by SDK dedup |
| `after-send-commit` | send committed, record lacks `send_op` | backfill, or re-pay dedups to `AlreadyInFlight` |

`CNF-12` proves all four live.

**OPS-29** Where fee caps bind, and on what base:

| Action | Pre-mint / pre-fund | Both legs | Base |
|---|---|---|---|
| Pay | cheapest `gw + fed ≤ fee_cap`, else `Permanent` | — | absolute `fee_cap` (default `max_fee`) |
| Receive | cheapest fitting, then committed-contract re-check | — | absolute |
| DirectInflow | receive leg ≤ `fee_cap` (`Permanent`) | — | absolute; gross-up bounded by it |
| Move | receive leg ≤ `fee_cap` (`Permanent`); fallback route priced at the amount | fixed receive + re-quoted send ≤ `fee_cap` | `floor(amount × max_fee_bps_of_move / 10 000)` stamped by the allocator (`ALC-7`) |
| Evacuate | sizing at delivered net; receive leg ≤ `cap.at(delivered)` (`Retryable`) | same, on `cap.at(executed net)` + viability | `base + floor(net × bps / 10 000)` (`ALC-20`) |

**OPS-42** `Join`: parse (`Permanent`), `mc.join` under a membership lease in the daemon (error
→ `Retryable`), compute whether the join was new from `membership_preexisting` and the registry,
mark the candidate `UserApproved` if the actor is `User`, record the outcome in the ledger, return
`Done`. `Recover`: parse, `mc.recover` under the lease, **any** error → `Permanent`, `Done`.
Neither has a fee cap or a reservation.

## Evacuation supersession

**OPS-31** The **marker** is `Intent.evacuation_refusal`. It is written only by
`reset_retryable(.., Some(evidence))` on `StructuralEvacuationRefusal`, and cleared by the
`Pending → Executing` claim, any ordinary retryable reset, the deliberate clear disposition, or
the exchange (the retired parent keeps it). `show` projects `evacuation_refusal_active` as
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

**OPS-34** Core `reconcile` drives every intent in `pending()` (`Pending | Executing`) to
terminal. It never touches `Awaiting`, `Done` or `Failed`. Re-performing an `Executing` intent
relies on the executor's idempotency (`OPS-17`–`OPS-27`).

**OPS-35** The daemon's `reconcile_durable`, per pass: scan `pending()` (a scan fault fails the
reconcile and the scheduler treats eligibility as unknown); compute goal blockers before any
filtering; preempt any in-flight probe whose source federation has a pending evacuation
(recording the probe `Failed` "preempted by evacuation"); per intent apply the marker policy
(`ALC-32`), fail orphaned probe legs whose session is gone, skip registry-owned keys, normalize
`Executing → Pending`, `ensure_driver`; then scan `awaiting()` and spawn awaiters for unowned
keys. `POST /v1/reconcile` runs this then a best-effort ledger repair (`STO-24`).

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

**OPS-39** `RefuseReason` is derived from `ExecError` **by substring match on the message**:
the strings `admit_intent` produces and the `journal:` prefix are a load-bearing contract, not
decoration. A rewording changes the wire.

**OPS-40** No code comment may carry a causal taxonomy of an unobserved money state (`DEF-20`).
The code states what a state **is**; the runbook holds the operator's account of how it might
arise and what to do.
