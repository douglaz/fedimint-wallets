---
status: accepted
---
# The fully-async intent model: no money operation's IO ever blocks another's start

`walletd` (Phase 6a, the 24/7 daemon) abandons the engine's Phase 1-5 execution model —
one process, one exclusive `db.lock`, strictly synchronous verbs — for a **fully-async
intent model**: a single actor task owns the Runtime + journal and serializes ONLY
ms-scale bookkeeping (sizing, caps, reservations, journal transitions); every money
operation's network IO runs in its own concurrent driver task, unbounded in duration and
count (one generous global admission cap). **Nothing ever queues behind another
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

- **Phase-aware reservations** — sizing reads journal-visible in-flight intents
  (`pending() ∪ awaiting()`, fail-closed) and reserves only what the live balance has not
  already absorbed.
- **Durable per-fed probe holds** — the active probe's no-sweep isolation, previously free
  from process exclusivity.
- **The in-flight registry** (Drop-guard, in-process only) — reconcile never re-drives what
  a live driver still owns; cross-restart exactly-once stays on the proven deterministic
  op ids + lnv2 dedup + op-log backfill.
- **One shared admission guard** applied identically at user decide-time and agent
  commit-time.

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
