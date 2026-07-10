# Phase 6a — `walletd`: the 24/7 daemon + local API (BUILDABLE spec)

The working-wallet milestone: one process owns the wallet permanently — the watch agent runs
24/7 AND the user can send/receive/inspect at any moment through a local API. Android (6b)
is postponed; this daemon becomes the permanent runtime the app later fronts.

Provenance: the approved + eng-review-cleared design
(`~/.gstack/projects/fedimint-wallets/master-main-design-20260710-031905.md`, 2026-07-10 —
3-round adversarial doc review, 6 eng-review findings, 20 outside-voice items triaged). This
spec is the field-level authority; the design doc is narrative background.

**GREENFIELD.** No backwards compatibility, no migration, no serde wire/compat shims. The
journal schema may change or gain rows freely. The FROZEN thing is validated behavior: the
existing unit suite and the live devimint gates (money path, crash gate at all four
killpoints, tick, discovery, watch) stay green through every change here.

## 6a.0 Shape + premises

- **P1 — the daemon is forced.** The store opens under an exclusive `db.lock`
  (fresh-eyes review 2026-07-05 §337); a 24/7 `watch` and operational verbs cannot coexist
  as separate processes. One process owns the DB; everything else talks to it.
- **P2″ — the fully-async intent model (the owner's hard requirement).** LN payments in
  flight can take HOURS (hold invoices, slow HTLC resolution), so **no money operation's
  network IO may ever block another operation's start**. Every money op is: (a) decide +
  journal the intent — serialized, ms-scale; (b) drive the IO in its own concurrent task —
  journaling per-leg transitions as it goes; (c) journal the terminal — serialized again.
  "Starts immediately" means the wallet is never the bottleneck; LN settlement time is the
  network's business.
- **P3 — local-only, authed.** 127.0.0.1 (configurable port) + a bearer token from a 0600
  file written by `walletd init` (env override for the devimint gates only). Trust boundary,
  honestly: a process running as the same OS user is fully trusted. No TLS/remote/multi-user;
  token rotation = re-run `init` while stopped. NWC (6a.2) is gated on the burn-in soak.
- **P4 — this is the permanent runtime.** Phase 6b's app fronts the same core; the CLI
  becomes a thin API client (ADR-0023 evolves) with a standalone fallback.
- **P5 — wrap the engine, restructure the perform path.** Scorer/allocator/decision logic
  and the fedimint pin are untouched. `Runtime::perform`, `Runtime::reconcile`,
  `Runtime::tick` and `wallet-core`'s `apply`/`reconcile` today interleave decide, journal,
  and network IO in single synchronous calls — splitting that interleave IS the build.

```
                            ┌─────────────────────────────────┐
 axum handlers ──Client──▶  │ ACTOR — owns Runtime + journal  │
 (pay/receive/…)            │ command enum = ms-scale ONLY:   │
                            │ size/reserve/decide, journal    │
 watch scheduler ─Client─▶  │ transitions, snapshot reads,    │
 (workflow daemon)          │ resolve await waiters           │
                            └───────────────┬─────────────────┘
                                            │ spawns + registers
                                            ▼
                            per-operation IO DRIVER tasks
                            (LN legs; secs→hours; journal via
                             Client commands; Drop guard
                             deregisters on Ok / Err / panic)
                                            │
                            in-flight registry:
                            std::sync::Mutex<HashMap<IntentKey, DriverEntry>>
                            (never held across an await)
                            reconcile re-drives ⇔ NO registry entry
```

## 6a.1 The threat list — every single-writer-dependent property, re-dispositioned

Each item below was previously safe BECAUSE the CLI was one-shot single-writer. The daemon
must re-establish each one explicitly; the spec sections cited own the mechanism.

| # | Property (source) | Old guarantee | 6a disposition |
|---|---|---|---|
| TL-1 | **Probe no-sweep isolation** (phase5-plan §5.0, lines 55-68 — "flagged as a Phase-6 precondition") | both probe legs ran synchronously in one process; nothing could spend from candidate `C` between legs, so leg OUT never sweeps pre-existing funds | a **per-fed probe hold** in the actor (§6a.5), with three sharpenings from spec review pass 1: (a) **durable, not registry-bound** — the hold predicate is the probe session's `in_flight` marker in the durable `0x08` record, so a panicked/abandoned driver leaves `C` held until the session resumes or terminalizes (the crash-total session already persists exactly this); (b) **retroactive** — `DecideProbe` DEFERS the probe when any in-flight intent already spends from `C` (visible in the same decide-time scan §6a.4 uses); (c) **evacuation preempts the hold** — `Evacuate(C→safe)` is exempt and drains `C` including the probe delta (rescuing real money outranks a ~20-sat accounting round); the probe then resolves umbrella-only `NoAttempt`/aborted with **no candidate demotion** (insufficient-funds-because-we-evacuated is our fault, not `C`'s). User pays from the spending fed are never affected |
| TL-2 | **Weekly probe budget** (phase5-plan §5.1b rejection: "concurrent-runs budget overrun = v1-unreachable") | one process = one probe at a time; check-then-record could not race | budget check + umbrella-row journal happen in ONE actor command (§6a.2 `DecideProbe`) before the driver spawns — atomic by actor serialization; the agent lane is cap-1 besides (§6a.5) |
| TL-3 | **Exactly-one perform per intent** (phase1-spec "Single-writer guard": dir lock + `Pending→Executing` CAS) | the dir lock serialized processes; the CAS serialized apply-vs-reconcile within one | the CAS **stays** (belt and braces); all transitions now flow through the actor (single-threaded by construction); the registry adds the missing piece — an ABANDONED driver (perform-timeout) cannot be re-driven while its task lives (re-drive ⇔ no registry entry; Drop guard makes the entry's lifetime equal the task's, §6a.3) |
| TL-4 | **Ledger repair vs concurrent writers** (phase4-impl-spec §546/§598: "single-writer by convention, but repair must not corrupt if that breaks") | one-shot process; repair could not overlap money ops | repair's writes are actor commands like everything else; `POST /v1/reconcile` and the scheduler's reconcile step both funnel into the same idempotent actor decide (§6a.6) — overlapping requests coalesce, and the phase-4 repair CAS hardening remains |
| TL-5 | **The watch loop never races itself** (phase5-plan §5.2.0: the loop "IS the single writer") | one loop, sequential cycles, occurrence advanced per cycle | exactly ONE workflow daemon task, started once at boot (§6a.5); the cap-1 agent lane serializes agent money ops; occurrence semantics unchanged |
| TL-6 | **O(ledger) per-cycle scans + coalescing bypass** (phase5-plan §5.2.8 deferrals) | bounded by short-lived processes / accepted for 5.2 | still deferred as performance (no money impact), BUT all O(ledger) reads move OFF-actor (§6a.2) so growth degrades throughput, never pay latency; the soak (§6a.9) is the instrument that decides if an index is needed |

New invariant introduced by 6a itself: **phase-aware reservations** (§6a.4) — the async
model's replacement for "the balance I probed is the balance I spend".

## 6a.2 The actor

One tokio task owns the `Runtime` (and through it the journal + MultiClient). Cloneable
`WalletClient` wraps `mpsc::Sender<Command>` (bounded, 16). Every command carries a
`oneshot` reply. **The enum contains only ms-scale bookkeeping — IO is unrepresentable:**

```rust
enum Command {
    // money-op lifecycle (each = one serialized critical section)
    DecideOp { req: OpRequest, reply: oneshot::Sender<Result<DecidedOp, RefuseReason>> },
        // sizing + caps + phase-aware reservations + probe-hold check (TL-1) +
        // journal the Pending intent + register the driver slot; returns the intent key
    DecideProbe { candidate: FederationId, reply: … },
        // TL-2: budget check + umbrella row + THE DURABLE 0x08 in_flight SESSION MARKER,
        // all in this one critical section (pass 6: the session marker IS the TL-1 hold
        // predicate — writing it later, from the driver, would leave a pre-hold window
        // where a concurrent C-spend breaks no-sweep). The hold exists before this
        // command returns.
    JournalTransition { key: IntentKey, t: Transition, reply: … },  // drivers' per-leg + terminal writes
    // reads (all bounded; O(ledger) scans NEVER here)
    Snapshot { scope: SnapshotScope, reply: … },              // balances/policies for detached readers
    ResolveAwait { key: IntentKey, waiter: oneshot::Sender<OpOutcome> },
        // check-then-park INSIDE the critical section (spec review pass 5): if the
        // intent is already terminal, reply immediately; only a live intent parks in
        // the pending map — otherwise a wait=true issued after the terminal
        // JournalTransition would hang to its deadline. The same mechanism serves
        // ARTIFACT waits (pass 7): a waiter may target the intent's INVOICE artifact
        // instead of its terminal — this is how /v1/receive blocks for the BOLT11 with
        // the mint IO in the driver (off-actor): decide+spawn, park an invoice-artifact
        // waiter with the §6a.6 hard deadline, and the driver's artifact
        // JournalTransition resolves it. Check-then-park applies to artifacts too
        // (pass 8): if the invoice artifact is ALREADY journaled — the idempotent-retry
        // case, where the durable BOLT11 exists — reply immediately, don't park.
    // scheduler bookkeeping
    DecideTickRound { facts: ProbeFacts, route_failures: Vec<MoveRouteProblem>, reply: … },
        // PURE decide: build_snapshot + allocator over sensed facts + accumulated route
        // failures; returns candidate decisions; journals nothing. Spec review pass 2
        // corrected pass 1 here: plan_tick's revision loop AWAITS NETWORK inside —
        // first_move_route_problem → validate_executor_move_route (runtime.rs:2352, 2363)
        // is the §15.6 gateway scan — so the LOOP lives in the workflow daemon:
        //   daemon: probe_all (IO) → DecideTickRound (ms) → validate routes (IO) →
        //   loop with failures → CommitTick when clean (or revisions exhausted).
    CommitTick { decisions: Vec<AllocatorDecision>, reply: … },
        // RE-CHECK then commit (spec review passes 3+5): the daemon-side revision loop
        // takes network time, and user intents accepted meanwhile change the picture —
        // so CommitTick re-runs THE SAME admission guard DecideOp uses (one shared
        // function: §6a.4 reservations + per-fed caps + the TL-1 probe hold — pass 5
        // caught that a caps-only recheck would let an agent move spend from a held C)
        // against CURRENT state inside its critical section, applied SEQUENTIALLY across
        // the batch — each accepted decision folds into the running reservation view
        // before the next is checked (the allocator's push_and_reserve discipline; pass 6
        // caught that per-decision-vs-current-state alone lets two decisions jointly
        // overdraw/over-cap). Any decision that no longer fits is DROPPED with a recorded
        // refusal (the next cycle replans), never force-committed. Fitting decisions are
        // ALL journaled atomically, but their drivers dispatch through the cap-1 agent
        // lane ONE AT A TIME (pass 7: the allocator can emit several executable moves per
        // tick — e.g. top-up + fund-standby — and spawning them all would violate the
        // lane): CommitTick registers + spawns the FIRST; the lane dispatcher (workflow
        // daemon) spawns the next when the previous terminalizes. CommitTick also KEEPS
        // the stale-occurrence guard (ensure_fresh_tick_decisions): a same-occurrence
        // terminal replay fails the tick step LOUDLY, exactly today's semantics (pass 8)
        // — the watch cycle's per-cycle occurrence advance makes it rare, never silent.
        // A journaled agent
        // intent awaiting its turn is simply Pending-with-no-registry-entry — the normal
        // re-drivable state, and the lane dispatcher is its re-driver. Same philosophy
        // as the phase-4 TOCTOU re-checks.
    ReconcileDecide { reply: oneshot::Sender<ReconcileReport> },
        // TL-3/TL-4: orphan set = EXACTLY today's re-drive set minus live drivers —
        // (Pending ∨ Executing) ∧ no registry entry. journal.pending() already returns
        // Pending|Executing (executor.rs:457); a crash after the Pending→Executing CAS
        // must re-drive on restart, same as today — "Pending only" would strand it.
        // The actor REGISTERS + SPAWNS the re-drive drivers inside this critical section
        // and replies with a report of what it did (pass 7: returning an orphan LIST for
        // the caller to spawn lets overlapping reconcile invocations — the scheduler +
        // POST /v1/reconcile — double-drive the same intent; actor-side registration
        // makes the second caller see zero orphans). Agent-lane intents dispatch through
        // the lane as above; awaiter rehydration (§6a.3) rides the same pass.
    Shutdown { reply: … },
}
```

Rules: handlers never await IO and never take the registry mutex across an await; the
`Pending→Executing` CAS lives inside `JournalTransition`; perform-time cap/TOCTOU re-checks
(phase 4) execute as `Snapshot`+`JournalTransition` round-trips. History and any O(ledger)
read run as detached read tasks fed by `Snapshot` — "ms-scale actor" means no network AND no
unbounded scans. The terminal `JournalTransition` also resolves that key's await waiters.

## 6a.3 IO drivers + the in-flight registry

One spawned task per in-flight money operation (user or agent — no special agent status):

- **The actor itself spawns the driver** inside the `DecideOp`/`CommitTick` critical
  section — journal write, registry insert, and `tokio::spawn` are one atomic actor-side
  sequence (spec review pass 2: if the HTTP handler spawned it, a cancelled handler —
  client disconnect between reply and spawn — would leave a phantom registry entry whose
  Drop guard never fires, and reconcile would skip the journaled intent forever). The
  driver owns the network work: it walks the existing `next_step`/`MoveStep` machinery,
  journaling each artifact via `JournalTransition`, then the terminal.
- **`Awaiting` rehydration:** an `Awaiting` DirectInflow is subscription-owned (§9.5), not
  re-driven — in the CLI world `await-move` re-subscribes on demand; the daemon must do it
  itself. At boot and after each reconcile pass, every `Awaiting` intent without a live
  registry entry gets an **awaiter driver** spawned (the recv_op subscription), so a paid
  invoice terminalizes and resolves its `?wait=true` waiters without any client action.
- **Drop guard:** the driver task's wrapper holds a guard that removes `registry[key]` on
  EVERY exit — Ok, Err, panic-unwind. Registry = `std::sync::Mutex<HashMap<…>>` (plain
  mutex: sync bookkeeping, never held across await; Drop can't await).
- **Perform-timeout in-daemon:** the deadline aborts the driver task; the Drop guard fires;
  the intent stays in whatever status it had reached — `Pending` OR `Executing` (the
  `Pending→Executing` CAS may have won before the abort) — and the NEXT reconcile re-drives
  it because the orphan predicate is `(Pending ∨ Executing) ∧ no registry entry` (§6a.2).
  Today's "abandon then process-exit" becomes "abandon then guard-cleanup" — same recovery,
  no double-drive (TL-3).
- Concurrency caps: user lane ~32 concurrent drivers (log-and-reject above — admission
  control, not a bottleneck); agent lane cap-1 (§6a.5).
- Cross-restart exactly-once is NOT the registry's job: it rests on the proven durable
  mechanisms — deterministic op ids + lnv2 dedup + op-log backfill, live-validated at all
  four crash killpoints.

## 6a.4 Phase-aware reservations (the new sizing invariant)

Sizing reads live balances (probe facts) PLUS journal-visible in-flight work. The rule —
**reserve only what the balance has not already absorbed** (funding an lnv2 pay debits
`spendable` the moment the outgoing contract is funded):

| In-flight state (journal) | Source fed reserves | Destination fed reserves (cap room) |
|---|---|---|
| Intent `Pending`/`Executing`, MoveRecord absent · `Created` · `Invoiced` | amount + fee_cap (spend not yet taken) | amount (undelivered credit counts toward the per-fed cap) |
| `Sending` (pay issued ⇒ source already debited) | nothing (balance absorbed it) | amount (still undelivered) |
| `Settled`/terminal · `Refunded` | nothing | nothing (balance reflects reality) |
| `DirectInflow` `Awaiting` | n/a (no source spend) | amount toward cap room |
| Raw `pay` (lnv2 send), pre-fund — **no send-op artifact journaled yet** | amount + fee cap (two concurrent pays from one fed must size against each other — the second 202 sees the first's reservation) | n/a |
| Raw `pay`, post-fund — **send-op artifact journaled** · raw `receive` | nothing (balance absorbed it) | claim credit toward cap at decide time |
| Probe umbrella (in-flight session) | per its legs, same table | per its legs + the TL-1 probe hold |

Implemented inside `DecideOp`'s critical section: reservations derive from
**`journal.pending()` ∪ `journal.awaiting()`** — `Awaiting` is deliberately excluded from
`pending()` (executor.rs:12-14, reconcile does not re-drive it), so an unpaid
`DirectInflow` invoice would otherwise be invisible and a concurrent move into the same
destination could jointly over-cap it when the payer eventually pays (spec review pass 1).
Each intent's `MoveRecord` phase supplies the row; no new in-memory state to rebuild on
restart. **For this scan to see raw ops, EVERY walletd money operation journals an Intent**
(spec review pass 2): raw `pay`/`receive` gain intent kinds instead of being ledger-only —
one uniform lifecycle (decide → intent → driver → terminal) for user pays, moves, inflows,
and probes alike; the append-only ledger stays the audit layer on top, as today. Two
concurrent raw pays from one fed therefore size against each other like any other pair.
Two hardening rules from spec review pass 4: (a) a raw pay's **pre/post-fund boundary is a
durable journal artifact** — the driver journals the lnv2 send op id via
`JournalTransition` the moment the pay is issued, exactly as MoveRecords record their leg
op ids, so restart/admission always knows which reservation row applies; (b) the
reservation scan **fails CLOSED**: today's `Journal::pending()` is infallible
(`-> Vec<Intent>`, executor.rs:133), so a storage read error could masquerade as "zero
reservations" and admit an overdraw — walletd's decide path uses fallible
`pending()`/`awaiting()` reads (greenfield: change the trait), and `DecideOp` REFUSES with
a storage error when the scan errs, never sizing against an empty default. The allocator's same-tick `reserved`/`credited` maps stay for intra-decision
batches; this table governs cross-operation sizing. Getting this table wrong in the strict
direction re-creates the smoke_tick over-reservation failure (allocator refuses affordable
moves); in the loose direction it can overdraw — goldens for every row (§6a.9).

## 6a.5 The watch scheduler = a workflow daemon

Its own spawned task holding a `WalletClient`, with an abort arm in its `select!`. Ports the
5.2 loop unchanged in policy: adaptive wake, **the per-fed meta-expiry `subscribe_to_field`
wake-hint tasks + coalescing cooldown move here from `wallet-cli::run_watch_loop`**, probe
cadence/budget, discovery rotation. Differences from 5.2's in-process loop:

- Steps run in the daemon's own time (probe sweeps, discovery previews = its IO), reaching
  the actor only for `DecideTick`/`DecideProbe`/`ReconcileDecide` round-trips; money work
  spawns ordinary registered drivers in the **cap-1 agent lane** — at most one agent money
  op in flight, ever.
- The TL-1 probe hold: the hold IS the durable `0x08` session's `in_flight` marker — any
  `DecideOp` spending FROM that fed refuses with a clear reason while the session lives,
  **except operations belonging to the holding session itself** (the hold carries the
  probe's intent key; the probe's own leg OUT `C→S` is exempt — spec review pass 4 caught
  the blanket rule deadlocking every probe after leg IN).
  The hold is released ONLY by the session resolving (terminal attempt, `NoAttempt`
  clear, or the evacuation preemption) — **never by the Drop guard** (a panicked
  post-leg-IN driver leaves `C` held until the session resumes, exactly what no-sweep
  needs; spec review pass 2 caught the earlier wording contradicting this).
- Exactly one scheduler instance, started at boot after `reconcile` seeding — TL-5.

## 6a.6 `wallet-api` + the HTTP surface

- **Wire DTOs** with `From<core>` impls (core report types stay serde-free); reuse the
  serde-capable primitives (`Msat`, `FederationId`, ledger rows) inside them.
- **`WalletConfig`** — ONE struct deriving `serde::Deserialize` (TOML) + `clap::Args`
  (standalone flags), single conversions into `TickPolicy`/`WatchPolicy`/`DiscoveryPolicy`.
  `walletd init` scaffolds `<data-dir>/walletd.toml` + the 0600 token. Edit + restart.
- **Endpoints** (bearer token on every route):

| Verb | Endpoint | Semantics |
|---|---|---|
| balance / list-feds | `GET /v1/balance` · `/v1/federations` | actor snapshot |
| history / show | `GET /v1/history` · `/v1/operations/{key}` | detached ledger read |
| status (dry-run) | `GET /v1/status` | detached (probes network); inputs via snapshot |
| watch observability | `GET /v1/watch/status` · `/v1/health` | health = actor queue depth, registry size, scheduler liveness |
| pay / move | `POST /v1/pay` · `/v1/move` | `202` + intent key ≤250 ms; driver async |
| receive / direct-inflow | `POST /v1/receive` · `/v1/direct-inflow` | blocks for the invoice mint under a hard deadline (bounded seconds → timeout error); BOLT11 is the response; settlement async |
| await | `GET /v1/operations/{key}?wait=true` | pending-map long-poll; mandatory deadline; drained-with-error on shutdown |
| join / approve / candidates | `POST /v1/join` · `/v1/approve` · `GET /v1/candidates` | join async like money ops |
| reconcile (admin) | `POST /v1/reconcile` | idempotent crash-recovery pass on demand — the "it's wedged" button |
| discover / probe (manual) | — deferred from v1 | agent covers both; standalone CLI for one-offs |
| tick (manual) | — deliberate omission | the scheduler owns cadence |

- **Idempotency for client retries — per-verb derivation (spec review pass 1):** each verb
  keys off its natural anchor so a timed-out retry ALWAYS collides and a deliberate repeat
  ALWAYS diverges: `pay` → the invoice's **payment_hash** (same invoice = same key however
  many times it's submitted; same amount + different invoice never collides; lnv2's own
  contract dedup backs it); `move`/`evacuate` → (from, to, amount, fee_cap, **occurrence**)
  as today; `receive`/`direct-inflow` → (to, amount, **required client nonce** — minting is
  repeatable by nature, so the caller must distinguish repeats). No optional-nonce
  ambiguity: verbs with a natural anchor take none; verbs without one require it.
  **Pay sizing inputs (pass 8):** the `/v1/pay` request carries an `amount` — REQUIRED for
  an amountless BOLT11, and if present on an amount-carrying invoice it must match — plus a
  fee cap (defaulted from `WalletConfig`'s per-move cap), so the §6a.4 pre-fund reservation
  (`amount + fee_cap`) is always computable; an amountless invoice with no stated amount is
  refused at decide time, never admitted un-reserved.
- The `202` key = the ledger's correlation key — the async API and the audit trail share one
  keyspace.

## 6a.7 `wallet-cli`: thin client + standalone

Every operational verb becomes an HTTP call against `wallet-api` types (daemon URL + token
from config/env); `pay`/`move` print the intent key and `await-*` long-polls, preserving
today's two-phase stdout contracts so the smoke scripts port mechanically. Standalone mode
(daemon stopped; `db.lock` enforces mutual exclusion) **spins up the same actor + driver
components in-process**, runs the one command, shuts down — one code path, no legacy fork.
Clear errors: daemon-not-running (with the two options), 401 (bad token), lock-held.

## 6a.8 Lifecycle + observability

`walletd` runs under systemd (user unit); fatal = exit non-zero, supervisor restarts
(§5.2.6 convention). SIGTERM: stop intake → drain the actor mailbox (ms; every submitted
journal transition lands) → abort drivers → exit 0; restart reconciles (abandon-and-resume
IS the model — hours-long IO makes draining it impossible by design). Observability v1:
`tracing` structured logs (per-op timelines keyed by intent key — the ledger is the durable
timeline); `/v1/health` as above.

## 6a.9 Tests + gates

**CRITICAL / regression (iron rule):** (1) the existing unit suite + `smoke_crash_move`
(all four killpoints) green through the restructured path; (2) the reconcile-vs-live-driver
double-drive test (perform-timeout abandonment cannot double-perform); (3) API double-submit
dedups on the intent key.

**Unit:** every `Command` variant round-trips on a fixture journal; no IO reachable from the
handler match; `None` from `recv()` exits cleanly. Registry Drop-guard on Ok/Err/panic;
orphan re-drive. Reservation goldens for EVERY row of the §6a.4 table, both directions
(no over-reservation, no overdraw) — explicitly including: the `Awaiting` DirectInflow +
concurrent-move joint over-cap case, and two concurrent raw pays from one fed (the second
sizes against the first's reservation). TL-1 probe-hold goldens: op-from-held-fed refuses;
the hold survives a panicked driver (durable session predicate); `DecideProbe` defers on a
pre-existing in-flight C-spend; **evacuation preempts the hold** and the probe resolves
NoAttempt without demoting the candidate. Pending-map: coalescing, deadline, shutdown drain. WalletConfig
TOML/flags equivalence golden. Scheduler: ported 5.2 unit tests unchanged; abort arm; wake-
hint shortens sleep. axum: per-endpoint happy+error, 401, 202-contract, invoice deadline.
CLI: verbs against a mock server; error UX.

**Live gates (merge-blocking, minutes-scale):**
- `smoke_daemon_devimint.sh`: the existing money gates driven through a running `walletd`
  via the CLI-as-client.
- **The responsiveness gate:** watch active, a probe held in flight by a **misbehaving-
  gateway test double** (accepts-contract-never-provides-preimage); `POST /v1/pay` must
  reach its **first external fedimint call** (not just the 202) in <250 ms; two concurrent
  pays never serialize on IO.
- The 5.2c autonomous chain (discover→gate→probes→fund→evacuate) re-run under the daemon's
  scheduler.

**Burn-in (NOT a merge gate):** a 24h+ soak — watch active + periodic user ops; no lock
conflicts, no duplicate intents, ledger reconstructs the session; gates "daily-drive with
real sats" and 6a.2 (NWC).

## 6a.10 Build order

Lanes (worktree-parallel): **A** perform-path restructure (wallet-core executor +
runtime split; the hardest chunk — the existing suite is its gate) ∥ **B** `wallet-api`
(DTOs + WalletConfig). Then **C** `wallet-daemon` (actor, drivers+registry, scheduler port,
axum) ∥ **D** `wallet-cli` client + standalone. Then the gates, then the soak.

Non-goals (v1): NWC (6a.2, soak-gated) · TLS/remote/multi-user · streaming events ·
runtime reconfig · packages/releases (Phase 8) · manual discover/probe endpoints · any UI.

Settled during the eng review (do not re-litigate): the 6a.0 de-risking spike was REJECTED
(build directly; fedimint's state-machine executor is trusted for client-level concurrency —
if the build disproves this, stop and re-plan rather than serializing silently); TODOS.md is
retired (this spec + `br` beads at build time are the backlog).
