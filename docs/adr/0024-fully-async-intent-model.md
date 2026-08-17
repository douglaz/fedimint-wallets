---
status: accepted
---
# The fully-async intent model: no money operation's IO ever blocks another's start

`walletd` (Phase 6a, the 24/7 daemon) abandons the engine's Phase 1-5 execution model —
one process, one exclusive `db.lock`, strictly synchronous verbs — for a **fully-async
intent model**: a single actor task owns the Runtime + journal and serializes ONLY
ms-scale bookkeeping (admission, reservation-releasing artifacts, and ordinary journal
transitions). Planning and route pricing run off actor under generation tokens.
The narrow money-intent exception is the composite post-network, DB-only raw
Pay/Receive/DirectInflow terminal write: it is attempt-fenced and runs under an actor-issued
mutation lease whose completion invalidates balance facts only for the affected federations.
All earlier raw-operation and `MoveRecord` artifacts use one-shot actor DB commands; no driver
holds a lease or the actor across network IO.
Separately, the O(ledger) raw-ledger repair scan runs off actor, but its reservation-releasing
intent repair is a CAS-fenced actor transition; it is not authority to perform an ordinary
off-actor intent write. Every money
operation's network IO runs in its own concurrent driver task, unbounded in duration and
unbounded BY EACH OTHER — a generous admission cap bounds externally-submitted totals
(runaway-script control); agent batches spawn regardless, bounded by policy. **Nothing ever queues behind another
operation's IO — including the agent's own probes and evacuations.**

## The forcing fact

A Lightning payment in flight can take **hours** to resolve (hold invoices, slow HTLC
resolution). Any design that serializes money IO — at any granularity — lets one payment
freeze the wallet for that long, and the owner's product bar is absolute: "anything that
can make the wallet feel unresponsive is a red alert; payments can be urgent." An
evacuation is just a send racing a shutdown window; it is the *last* thing that may queue.

## What replaces serialization as the safety mechanism

The Phase 1-5 money-safety validation (~500 tests, 5 live devimint gates incl. the
four-killpoint crash gate) assumed serialized execution. Under this ADR those guarantees
rest instead on explicit, decide-time mechanisms (spec: `docs/phase6a-plan.md`):

- **Dual reservation views** — fresh user admission reads a strict nonterminal intent projection.
  Tokenized allocator planning/commit reads a validated artifact/phase projection, so a send debit
  already absent from spendable is not subtracted twice. Missing, corrupt, mismatched, oversized,
  or impossible derived state falls back to the strict action reservation.
- **Durable per-fed probe holds** — the active probe's no-sweep isolation, previously free
  from process exclusivity.
- **The in-flight registry** (Drop-guard, in-process only) — reconcile never re-drives what
  a live driver still owns; cross-restart exactly-once stays on the proven deterministic
  op ids + lnv2 dedup + op-log backfill.
- **One shared admission arithmetic**, with the strict view fixed for user decide-time and the
  actor-tokenized view available only to agent commit (or an exclusive standalone runtime).

## Considered options

- **Coarse actor** (one money op at a time): rejected — a pay waits behind a probe for
  minutes under a degraded gateway.
- **User/agent priority lanes**: rejected — priority helps only *between* ops; a
  hold-invoice pay still blocks the next pay for hours.
- **Cap-1 agent lane** (concurrency for users, serialization for the agent): rejected by
  the owner — an evacuation is just a send, and two federations shutting down together is
  exactly when both evacuations must move; analysis showed the cap was conservatism, not a
  correctness requirement.

## Consequences

- The perform path (`Runtime::perform`/`reconcile`/`tick`, `wallet-core::apply/reconcile`)
  is restructured from interleaved decide/journal/IO into actor round-trips + detached
  drivers — the bulk of the 6a build. The existing test suite + crash gates staying green
  through the restructure is the frozen bar (greenfield: schemas may change, validated
  behavior may not).
- Every review finding previously rejected as "unreachable under single-writer" was
  re-dispositioned (spec §6a.1); that rejection class is no longer a valid argument
  anywhere in this codebase.
- The responsiveness gate (pay-during-held-probe starts, first external call, <250 ms) is
  a permanent live gate: it is this ADR's invariant made measurable.
- **Scope note (br-p93).** The invariant above is about admitted money IO, and it still holds for
  everything the forcing fact names: no user operation, no probe, and no evacuation ever waits on
  another operation, and nothing serializes by federation. br-p93 added something narrower, one
  layer up in the agent's DECISION: while a logical allocator goal (fund-into-destination,
  evacuate-source) is durable and non-terminal, the allocator withholds a second key for that same
  goal. Before send, an evacuation holds `amount + fee_cap` on its source; after a validated
  `Sending` artifact, the tokenized allocator view absorbs that debit into the live balance while
  retaining the promised destination inbound. While an evacuation is live, the allocator
  additionally withholds allocator FUNDING touching that source: phase-aware reservation alone
  cannot make overlapping balance effects coherent with the drain's lifecycle. Both are the agent declining to plan work that would
  duplicate or race its own in-flight work — no admitted operation is queued behind another's IO,
  and the cost falls only on the agent's own rebalancing latency. It replaced two global gates that
  suppressed EVERY allocator decision, evacuations included, whenever any intent was retryable or
  re-driven.
