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
killpoints, tick, discovery) stay green through every change here; the watch gate's
validated behavior migrates to its daemon replacement gate (§6a.7 deletes the verb, §6a.9
re-validates the ported scheduler).

## 6a.0 Shape + premises

- **P1 — the daemon is forced.** The store opens under an exclusive `db.lock`
  (fresh-eyes review 2026-07-05 §337); a 24/7 `watch` and operational verbs cannot coexist
  as separate processes. One process owns the DB; everything else talks to it.
- **P2″ — the fully-async intent model (the owner's hard requirement).** LN payments in
  flight can take HOURS (hold invoices, slow HTLC resolution), so **no money operation's
  network IO may ever block another operation's start**. Every money op is: (a) decide +
  journal the intent — serialized, ms-scale; (b) drive the IO in its own concurrent task —
  journaling per-leg transitions as it goes; (c) journal the terminal — ordinarily
  serialized again. The narrow raw Pay/Receive/DirectInflow exception observes the network
  first, then performs only its attempt-fenced DB terminal write under an actor-issued
  short mutation lease; ending that lease invalidates balance facts for the action's
  affected federations.
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
                            (LN legs; secs→hours; ordinary journal
                             writes via Client commands; fenced raw
                             terminal DB write under a short actor
                             lease; Drop guard deregisters on
                             Ok / Err / panic)
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
| TL-1 | **Probe no-sweep isolation** (docs/archive/phase5-plan.md §5.0, lines 55-68 — "flagged as a Phase-6 precondition") | both probe legs ran synchronously in one process; nothing could spend from candidate `C` between legs, so leg OUT never sweeps pre-existing funds | a **per-fed probe hold** in the actor (§6a.5), with three sharpenings from spec review pass 1: (a) **durable, not registry-bound** — the hold predicate is the probe session's `in_flight` marker in the durable `0x08` record, so a panicked/abandoned driver leaves `C` held until the session resumes or terminalizes (the crash-total session already persists exactly this); (b) **retroactive** — `DecideProbe` DEFERS the probe when any in-flight intent already spends from `C` (visible in the same decide-time scan §6a.4 uses); (c) **evacuation preempts the hold** — `Evacuate(C→safe)` is exempt and drains `C` including the probe delta (rescuing real money outranks a ~20-sat accounting round); the probe then resolves umbrella-only `NoAttempt`/aborted with **no candidate demotion** (insufficient-funds-because-we-evacuated is our fault, not `C`'s). User pays from the spending fed are never affected |
| TL-2 | **Weekly probe budget** (docs/archive/phase5-plan.md §5.1b rejection: "concurrent-runs budget overrun = v1-unreachable") | one process = one probe at a time; check-then-record could not race | budget check + umbrella-row journal happen in ONE actor command (§6a.2 `DecideProbe`) before the driver spawns — atomic by actor serialization; concurrent probes of different candidates are safe under the same reservations/holds |
| TL-3 | **Exactly-one perform per intent** (phase1-spec "Single-writer guard": dir lock + `Pending→Executing` CAS) | the dir lock serialized processes; the CAS serialized apply-vs-reconcile within one | the CAS **stays** (belt and braces); ordinary intent transitions flow through the actor (single-threaded by construction). The fenced exceptions are the DB-only raw-terminal write under an actor-issued lease and the CAS-fenced actor sink for off-actor ledger repair. The registry adds the missing piece — an ABANDONED driver (perform-timeout) cannot be re-driven while its task lives (re-drive ⇔ no registry entry; Drop guard makes the entry's lifetime equal the task's, §6a.3) |
| TL-4 | **Ledger repair vs concurrent writers** (phase4-impl-spec §546/§598: "single-writer by convention, but repair must not corrupt if that breaks") | one-shot process; repair could not overlap money ops | the ledger/op-log scan remains the **deliberate off-actor exception** because it is O(ledger) (TL-6), but raw Pay/Receive terminal **intent** synchronization returns through the actor. The scheduler, daemon reconcile endpoint, and standalone reconcile/await call `repair_ledger_with_actor` after actor-side durable reconciliation. Ledger advancement is attempt/sequence/status fenced; the actor sink rechecks the terminal ledger fence before changing the matching nonterminal intent. A repair I/O fault is logged and the cycle continues (best-effort; intent re-drive already committed its own money-path progress) |
| TL-5 | **The watch loop never races itself** (docs/archive/phase5-plan.md §5.2.0: the loop "IS the single writer") | one loop, sequential cycles, occurrence advanced per cycle | exactly ONE workflow daemon task, started once at boot (§6a.5) — one decision cadence, though the money ops it spawns run concurrently like any others (owner ruling: an evacuation is just a send and never queues); occurrence semantics unchanged |
| TL-6 | **O(ledger) per-cycle scans + coalescing bypass** (docs/archive/phase5-plan.md §5.2.8 deferrals) | bounded by short-lived processes / accepted for 5.2 | still deferred as performance (no money impact), BUT all O(ledger) reads move OFF-actor (§6a.2) so growth degrades throughput, never pay latency; the soak (§6a.9) is the instrument that decides if an index is needed |

New invariant introduced by 6a itself: **strict fresh-user reservations plus generation-fenced
allocator artifact absorption** (§6a.4) — the async model's replacement for "the balance I probed
is the balance I spend".

## 6a.2 The actor

One tokio task owns the `Runtime` (and through it the journal + MultiClient). Cloneable
`WalletClient` wraps `mpsc::Sender<Command>` (bounded, 16). Every command carries a
`oneshot` reply. **The enum contains only ms-scale bookkeeping — IO is unrepresentable:**

```rust
enum Command {
    // money-op lifecycle (each = one serialized critical section)
    DecideOp { req: OpRequest, reply: oneshot::Sender<Result<DecidedOp, RefuseReason>> },
        // FIRST: same-key short-circuit (pass 9) — if the derived intent key already
        // exists as Pending/Executing, reply with the existing key/status; if it has NO
        // live registry entry (its driver timed out or panicked), register + spawn its
        // re-drive driver right here (pass 10: attach means ENSURE DRIVEN, not wait for
        // the next reconcile); if terminal, reply terminal
        // (phase-1 dedup: Failed stays terminal). Only a genuinely new key proceeds to:
        // sizing + caps + strict nonterminal intent-status reservations + probe-hold check (TL-1) +
        // journal the Pending intent + register + spawn; returns the intent key.
        // Without the short-circuit, an idempotent retry's own reservation could refuse
        // itself against the original's.
    DecideProbe { candidate: FederationId, reply: … },
        // TL-2: budget check + umbrella row + THE DURABLE 0x08 in_flight SESSION MARKER,
        // all in this one critical section (pass 6: the session marker IS the TL-1 hold
        // predicate — writing it later, from the driver, would leave a pre-hold window
        // where a concurrent C-spend breaks no-sweep). The hold exists before this
        // command returns.
    JournalTransition { key: IntentKey, t: Transition, reply: … },  // drivers' per-leg + terminal writes
    SetOperationArtifact { key: IntentKey, attempt: u32, artifact: …, reply: … },
    PutMove { key: IntentKey, attempt: u32, record: MoveRecord, reply: … },
        // One-shot, DB-only reservation-release boundaries. A true fenced write bumps the
        // action's balance generations before the reply; false changes nothing. An ambiguous
        // write stales that pre-looked-up action; only an attempted mutation with no known
        // action poisons all balance facts. No command spans network IO.
    // reads (all bounded; O(ledger) scans NEVER here)
    Snapshot { scope: SnapshotScope, reply: … },              // balances/policies for detached readers
    GetPolicy { reply: … },                                   // the DB Policy row
    PutPolicy { policy: Policy, reply: … },
        // broad review: validate + journal the new Policy in this critical section
        // (serialized like every write), wake the scheduler (deadlines recompute from
        // the new cadences/budgets), reply with the stored version.
    ResolveAwait { key: IntentKey, target: AwaitTarget /* Terminal | InvoiceArtifact */,
                   waiter: oneshot::Sender<OpOutcome> },
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
    DecideTickRound { facts: ProbeFacts, reply: … },
        // Returns a TickRound STAMPED with planned_generation = the actor's policy
        // generation at plan time (bumped on every accepted PutPolicy). The daemon carries
        // it back into CommitTick so a policy change during route validation is caught (P1).
        // Prepares the actor-owned policy/generation facts, then plans the round OFF actor:
        // plan_tick_round performs route pricing/validation there and returns one candidate
        // round; it journals nothing. Its JOURNAL-side
        // inputs — the auto-join probe gate (joined − UserApproved via the 0x09
        // registry), active-probe verdicts, and the gate policy — are read INSIDE this
        // command (ms-scale journal reads), exactly as plan_tick reads them today
        // (pass 10: omitting them from the contract would let the daemon tick fund an
        // auto-joined fed pre-probe). Only NETWORK facts arrive from the daemon. Route
        // validation and any route re-planning are implementation details of the off-actor
        // plan_tick_round call; the scheduler does not carry a route-failure vector or run a daemon
        // revision loop. It performs one DecideTickRound followed by one CommitTick.
    CommitTick { round: TickRound, balances: BTreeMap<FederationId, Msat>,
                 balance_facts: BalanceFactsToken, tick_key: Option<IdempotencyKey>, reply: … },
        // POLICY-GENERATION GUARD FIRST (P1): if round.planned_generation != the actor's
        // current policy generation, a PutPolicy landed during daemon route validation and
        // these decisions were sized against superseded caps/targets — refuse the WHOLE
        // batch (RefuseReason::PolicySuperseded, journaling nothing), the next cycle replans
        // under the current policy. No money op is admitted on stale sizing.
        // RE-CHECK then commit (spec review passes 3+5): off-actor planning takes network
        // time, and user intents accepted meanwhile change the picture —
        // so CommitTick re-runs the same admission ARITHMETIC with its actor-tokenized
        // phase-aware reservation view (DecideOp remains strict): §6a.4 reservations +
        // per-fed caps + the TL-1 probe hold — pass 5
        // caught that a caps-only recheck would let an agent move spend from a held C)
        // against CURRENT state inside its critical section, applied SEQUENTIALLY across
        // the batch — each accepted decision folds into the running reservation view
        // before the next is checked (the allocator's push_and_reserve discipline; pass 6
        // caught that per-decision-vs-current-state alone lets two decisions jointly
        // overdraw/over-cap). Any decision that no longer fits is DROPPED with a recorded
        // refusal (the next cycle replans), never force-committed. Fitting decisions are
        // ALL journaled atomically AND their drivers ALL spawned immediately (owner
        // ruling, grill session: an evacuation is just a send — agent money ops run
        // concurrently exactly like user ops; there is NO agent lane and no queued
        // dispatch. The batch is safe concurrent because every decision's reservations
        // were folded in sequentially at this commit — the sizing already accounts for
        // all of them). CommitTick also keeps the stale-occurrence terminal replay guard:
        // a same-occurrence terminal replay receives a recorded,
        // per-decision conflict refusal while independent decisions in the same CommitTick
        // continue. The watch cycle's per-cycle occurrence advance makes this rare, never
        // silent. Same philosophy as the phase-4 TOCTOU re-checks.
    ReconcileDecide { reply: oneshot::Sender<ReconcileReport> },
        // TL-3/TL-4: orphan set = EXACTLY today's re-drive set minus live drivers —
        // (Pending ∨ Executing) ∧ no registry entry. journal.pending() already returns
        // Pending|Executing (executor.rs:457); a crash after the Pending→Executing CAS
        // must re-drive on restart, same as today — "Pending only" would strand it.
        // The actor REGISTERS + SPAWNS the re-drive drivers inside this critical section
        // and replies with a report of what it did (pass 7: returning an orphan LIST for
        // the caller to spawn lets overlapping reconcile invocations — the scheduler +
        // POST /v1/reconcile — double-drive the same intent; actor-side registration
        // makes the second caller see zero orphans). Re-drive drivers spawn concurrently
        // like all others; awaiter rehydration (§6a.3) rides the same pass.
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
  **Observable behavior freeze (broad review):** the re-drive path normalizes an orphaned
  `Executing` back to `Pending` FIRST, preserving today's asserted observable
  (`perform_timeout_leaves_a_stalled_intent_pending`, runtime.rs:4930) — a timed-out
  operation is seen as Pending, never as a phantom Executing.
  Today's "abandon then process-exit" becomes "abandon then guard-cleanup" — same recovery,
  no double-drive (TL-3).
- Concurrency cap: ONE global cap, ~32 concurrent drivers (log-and-reject above —
  admission control against runaway scripts, not a bottleneck). **Where it applies (broad
  review):** at `DecideOp` admission — i.e. to externally-submitted requests. `CommitTick`
  and `ReconcileDecide` spawn their batches regardless (both are bounded by policy — an
  allocator tick emits a handful of decisions, re-drives are bounded by what's Pending);
  refusing an evacuation because a script opened 32 pays would invert the priorities. The
  actor mailbox is sized ≥ cap + headroom (e.g. 64) so full-capacity driver transitions
  never back-pressure admission. There is deliberately NO per-origin lane and NO agent
  serialization (owner ruling: an evacuation is just a send; no money operation ever
  queues behind another — the reservations/holds are the safety mechanism, not
  serialization).
- Cross-restart exactly-once is NOT the registry's job: it rests on the proven durable
  mechanisms — deterministic op ids + lnv2 dedup + op-log backfill, live-validated at all
  four crash killpoints.

## 6a.4 Journal-visible reservations (strict admission, phase-aware allocator)

Sizing reads live balances (probe facts) PLUS journal-visible in-flight work. There are two
deliberately different projections over the same fail-closed
`journal.reservation_intents()` scan (`Pending | Executing | Awaiting`):

1. **Strict user admission.** `DecideOp`, manual retry, generic `wallet-core` apply/admission,
   and the executor's pre-fund recovery belt for user/probe work retain each nonterminal action's
   complete reservation. This closes admission races when a caller has no actor-issued balance
   authority. The pre-fund belt for an already-tokenized allocator goal reuses the allocator view,
   reading artifacts before its subsequent live balance sample.
2. **Tokenized allocator admission.** Route pricing, allocator planning, `CommitTick`, and
   standalone tick under the exclusive database lock may absorb value proven by a validated raw
   artifact or `MoveRecord` phase. This prevents Pay/Move money already removed from Fedimint
   spendable from being subtracted a second time during a long settlement.

The strict table remains:

| Intent action and status | Source fed reserves | Destination fed reserves |
|---|---|---|
| nonterminal `Move` or `Evacuate` | amount + fee_cap | amount |
| nonterminal raw `pay` | amount + fee_cap | n/a |
| nonterminal raw `receive` | n/a | amount |
| nonterminal `DirectInflow` | n/a | amount |
| `Join`, `Recover`, advisory refusal, or terminal `Done`/`Failed` | nothing | nothing |
| Probe move legs | their underlying `Move` reservations | their underlying `Move` reservations plus the logical TL-1 probe hold |

The allocator projection is:

| Nonterminal action / trusted artifact phase | Source fed reserves | Destination fed reserves |
|---|---|---|
| raw `Pay`, no operation id | amount + fee_cap | n/a |
| raw `Pay`, operation id present | nothing | n/a |
| raw `Receive` | n/a | action amount |
| `Move`/`Evacuate`, missing/invalid record or `Created` | action amount + fee_cap | action amount |
| `Move`/`Evacuate`, `Invoiced` | record amount + record fee_cap | record amount |
| `Move`/`Evacuate`, `Sending` | nothing | record amount |
| `DirectInflow`, missing/invalid record or `Created`/impossible `Sending` | n/a | action amount |
| `DirectInflow`, `Invoiced` | n/a | record amount |
| trusted terminal move phase, or terminal Intent | nothing | nothing |

A record is trusted only when its key, endpoints, `send_required` direction, and nonzero amount
match the Intent action; `Move` and `DirectInflow` require exact amount/fee-cap equality, while
an `Evacuate` may downsize only under its component-derived cap or, for a legacy action, retain
its planned absolute cap. The phase's required receive/send artifacts must also be present (and
impossible sender artifacts on a receive-only inflow are rejected). `Created` never downsizes the
strict action. Missing, undecodable, mismatched, oversized, or impossible state falls back strict;
it never fails open.

Projected inbound has two deliberately separate meanings at every planning/admission boundary:
**all** inbound promises consume hard per-federation cap room, while only wallet-delivered
`Move`/`Evacuate` inbound credits the destination's spending/standby target shortfall. An unpaid
external `Receive` or `DirectInflow` therefore cannot suppress a top-up. `Sending` releases only
the source, never the destination promise. Commit-time validation uses the same target-credit
arithmetic, including earlier accepted decisions in the same batch, so planning cannot repeatedly
propose a full top-up that commit must reject.

The phase-aware view is sound only with the balance generation that authorized it. Every service
raw-artifact or `MoveRecord` write, except Runtime's composite direct terminal write, is a
one-shot, DB-only actor command: the actor captures the matching Intent/action, performs the
attempt-fenced journal write, and on a true write bumps every affected federation before replying
or processing another command. A false attempt fence changes no generation. If the writer returns
an ambiguous durability error after the actor captured the action, it conservatively bumps that
action's affected generations (and a terminal `Join`/`Recover` also bumps the membership world);
only a possibly-mutated write whose action is unknown poisons all balance facts. Thus `CommitTick`
before the artifact sees the strict phase, while artifact first invalidates every older touching
balance token. Runtime's excluded composite direct terminal write remains protected by the existing
external terminal lease, which is never nested around an actor artifact command.

`Awaiting` is deliberately excluded from operational `pending()` (reconcile does not re-drive
subscription-owned work), so the reservation scan includes it explicitly. Every walletd money
operation journals an Intent: raw `pay`/`receive`, moves, inflows, and probe legs share one
lifecycle, with the append-only ledger as the audit layer. The allocator's same-tick
`reserved`/`credited` maps remain for intra-decision batches; these tables govern cross-operation
sizing. Getting the fallback loose can overdraw; keeping the allocator universally strict can size
a shutdown evacuation to zero after an earlier send already reduced spendable (§6a.9).

## 6a.5 The watch scheduler = a workflow daemon

Its own spawned task holding a `WalletClient`, with an abort arm in its `select!`. Ports the
5.2 loop unchanged in policy: adaptive wake, **the per-fed meta-expiry `subscribe_to_field`
wake-hint tasks + coalescing cooldown move here from `wallet-cli::run_watch_loop`**, probe
cadence/budget, discovery rotation. Differences from 5.2's in-process loop:

- Steps run in the daemon's own time (probe sweeps, discovery previews = its IO), reaching
  the actor only for `DecideTick`/`DecideProbe`/`ReconcileDecide` round-trips; money work
  spawns **ordinary registered drivers, concurrent like any user op** (no agent lane —
  two simultaneous evacuations both start immediately; the scheduler is single, its
  spawned money ops are not).
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
- **Configuration splits by WHO decides it (owner ruling, grill session):**
  - **Policy = user data, lives in the DB.** Everything the user decides — targets, caps,
    fees, probe budgets, cadences, discovery caps, spending/standby pins — is ONE `Policy`
    struct in `wallet-api`, stored as a journal row (new partition), **seeded with
    defaults by `walletd init`**, read by the actor at decide time, and **mutable at
    runtime via `GET/PUT /v1/policy`** (+ a `wallet-cli policy` verb) — effective from the
    next decide, no restart. Rationale: the policy IS the standing instruction's
    parameters (ADR-0014) — user data that must version with the journal, ride the
    Phase-7 encrypted app-state backup, and be editable by the future app (which cannot
    edit files). The DRY principle from the eng review survives: the one struct serves
    the DB row, the API DTO, and per-invocation smoke-test flag overrides on the
    standalone verbs.
  - **`walletd.toml` = host/deployment config ONLY** — data dir, bind address/port, token
    file path, log level: things that exist before the DB does or that the host, not the
    user, decides. `walletd init` scaffolds it + the 0600 token. Edit + restart.
  - **Names & defaults (owner-ratified):** binary **`walletd`** / systemd user unit
    `walletd.service` ("walletd" is the working name; a product name at Phase 8 may
    rename it — cheap now, costly later, accepted); default bind **`127.0.0.1:9736`**
    (Lightning's 9735 + 1); default data dir **`~/.local/share/walletd`** (XDG — a
    daemon needs an absolute home, not the CLI-era CWD-relative `./.wallet-cli-data`);
    the CLI client finds the daemon via `~/.config/walletd/` (URL + token path).
  - **Policy activation semantics (broad review):** decisions read the CURRENT policy at
     decide time; admitted intents and their parameters remain immutable in general. The narrow
     `br-n8o` exception is a pre-artifact agent evacuation carrying durable typed structural
     refusal evidence: a component-wise monotone effective evacuation-cap increase at its
     measured delivered-net sample may atomically replace it with a linked, fresh
     occurrence/key. The old operation is `Failed`, the child is `Pending`, and durable
     forward/reverse sidecars preserve the audit chain; an empty bounded probe remains
     `Retryable` evidence-not-proof, never route unavailability. `PUT /v1/policy` wakes the
     scheduler and bumps policy generation, so any round planned under the old policy is
     discarded and a fresh plan/current generation decides the replacement. A claimed
     `Pending -> Executing` consumes the marker. Actor `CommitTick` owns this exchange;
      off-actor planning/reconcile cannot forge it. A replacement round admits exactly its one
      child; deferred ordinary decisions and suppressions stay visible to pinned-input validation.
      Executable deferred decisions are written as explicit `tick-drop:`-keyed audit facts with the
      replacement-exclusive note, while deferred `RefuseInflow` advisories retain their `refuse:`
      identity and diagnostics with that same replacement-exclusive error note.
      The exclusive-DB standalone `tick` is the
     only parallel mutation seam and requires `--occurrence` advanced beyond the marked agent
      occurrence. Its standalone dry `status` counterpart warns on a stale/default occurrence and
     returns the scored/designation diagnostic with no would-run decisions; it never exchanges the
     parent or writes a child. Daemon status remains strict because it owns occurrence allocation.
     `walletd init` inserts defaults ONLY if no Policy row exists (re-running init
     for token rotation never resets policy). `Runtime`'s currently-immutable per-fed-cap/
     timeout constructor fields become per-decide policy reads as part of the restructure.
  - **Shipped policy defaults (owner-set):** `per_fed_cap` **1,500,000 sats** ·
    `spending_target` **500,000 sats** · `standby_target` **150,000 sats** · `max_fee`/move
    200 sats · probe amount 20 sats · probe budget 10 attempts / 500 sats per week ·
    `evacuation_lead` 1h.
- **Endpoints** (bearer token on every route):

| Verb | Endpoint | Semantics |
|---|---|---|
| balance / list-feds | `GET /v1/balance` · `/v1/federations` | actor snapshot |
| history / show | `GET /v1/history` · `/v1/operations/{key}` | detached ledger read; `OperationView.supersedes` / `.superseded_by` expose durable evacuation-replacement audit links. `show` additionally projects a live `evacuation_refusal` marker when present; bounded history omits it to avoid N+1 intent reads. History is newest-first, caps each public page at 500, and returns `next_before_seq`; pass it as `before_seq` for the next page |
| status (dry-run) | `GET /v1/status` | detached, live network probes and no money admission. Requires the daemon's live `Runtime` **and** `MultiClient`, with **every** journal-joined federation open; otherwise `503` rather than previewing a partial world. Its registry scan also counts poison rows: any skipped malformed federation row returns actionable `503` **before** watch-state access or probes, because the healthy subset is not a complete world. Stop the daemon, preserve the data directory, and repair the registry row; do not re-join or delete it. Reads `get_watch_state` only after that gate, which must report a reconciled Agent floor; that bounded migration read may advance one valid batch before returning `503` and directing the operator to `/v1/watch/status`. The prospective occurrence is checked: floor `MAX-1` previews the one legal `MAX` cycle, while floor `MAX` returns `503` because no successor exists |
| watch observability | `GET /v1/watch/status` · `/v1/health` | `/v1/watch/status` reads and persists one bounded WatchState-floor migration batch. `agent_floor_reconciled=false, unreadable_ledger_rows=0` means valid canonical scan backlog; a positive unreadable count is exact-row repair work and remains allocation-fenced. `/v1/health` reports actor queue depth, registry size, scheduler liveness |
| pay / move | `POST /v1/pay` · `/v1/move` | `202` + **operation key** ≤250 ms; driver async ("operation" is the public term — CONTEXT.md; "intent" stays internal) |
| receive / direct-inflow | `POST /v1/receive` · `/v1/direct-inflow` | blocks for the invoice mint under a hard deadline (bounded seconds → timeout error); BOLT11 is the response; settlement async |
| await | `GET /v1/operations/{key}?wait=true` | pending-map long-poll; mandatory deadline; drained-with-error on shutdown |
| join / approve / candidates | `POST /v1/join` · `/v1/approve` · `GET /v1/candidates` | join async **with the full Intent lifecycle** (kind `join`: decide → intent → driver → terminal; a crash after the 202 re-drives via reconcile like any operation — broad review; the ledger join row stays the audit layer) |
| reconcile (admin) | `POST /v1/reconcile` | idempotent crash-recovery pass on demand — the "it's wedged" button |
| policy | `GET /v1/policy` · `PUT /v1/policy` | the standing instruction's parameters (DB-stored `Policy` struct); PUT validates + journals the change, effective next decide |
| discover / probe (manual) | — no daemon endpoint | standalone CLI Runtime-direct one-offs; daemon scheduler drives the automated forms |
| tick (manual) | — no daemon endpoint | standalone CLI Runtime-direct one-shot; the daemon scheduler owns recurring cadence |

- **Error taxonomy (galtland layered results — house style, applied):** `wallet-api`
  errors keep three layers distinctly matchable, never flattened at the boundary:
  (outer) **transport** — daemon unreachable/timeout, produced by the client itself;
  (middle) **refused** — a decide-time `RefuseReason` (insufficient-after-reservations,
  held fed, cap, budget, sizing-field conflict, fail-closed storage error): nothing was
  journaled, safe to retry after fixing the cause; (inner) **failed** — an in-flight
  driver terminal carrying the operation key: durable, visible in history, retry = a new
  deliberate operation. The CLI maps the three to distinct exit codes and messages.
- **Deadline defaults** (constants, not policy): invoice-mint hard deadline **30 s**;
  await long-poll default **60 s** per request (clients re-poll; the waiter's mandatory
  deadline is the request's, so shutdown-drain and timeout semantics stay uniform).
- **Idempotency for client retries — per-verb derivation (spec review pass 1):** each verb
  keys off its natural anchor so a timed-out retry ALWAYS collides and a deliberate repeat
  ALWAYS diverges: `pay` → the invoice's **payment_hash** (same invoice = same key however
  many times it's submitted; same amount + different invoice never collides; lnv2's own
  contract dedup backs it); `move`/`evacuate` → (from, to, amount, fee_cap, **occurrence**)
  as today; `receive`/`direct-inflow` → (to, amount, **required client nonce** — minting is
  repeatable by nature, so the caller must distinguish repeats). No optional-nonce
  ambiguity: verbs with a natural anchor take none; verbs without one require it.
  **Scope (broad review): this table derives USER API keys only.** Agent decision keys
  keep TODAY's derivation untouched (per-logical-intent + occurrence, amount-free —
  allocator.rs:308): folding amount/fee-cap into agent keys would let changed sizing under
  the same occurrence mint a fresh key and bypass the stale-occurrence replay guard. The
  two derivations coexist deliberately; the validated replay behavior is the frozen thing.
  **User-op failure + retry semantics (step-2 review ruling):** PRE-FUND user-op driver
  failures (fee quote over cap, no gateway quote, deterministic route/invoice rejections)
  TERMINALIZE — a user pay is one-shot; leaving it Pending would let a background
  reconcile settle it hours after the user gave up ("thought it failed, later
  succeeded"). Ambiguous mid-send transport errors stay Retryable as a RESUME (the
  deterministic op id + lnv2 dedup reattach, never re-pay). The attach-conflict rule
  applies to LIVE intents only; a user re-request whose key matches a TERMINAL-FAILED
  intent is the phase-1-sanctioned **manual retry** — it refreshes that intent to Pending
  with the new sizing fields (journaled); a key matching Done still dedups.
  **One-attempt-per-invoice carve-out (post fedimint 703a37e8f3f):** lnv2 permits a
  single payment attempt per invoice, so manual retry of a Failed PAY is sanctioned only
  while no send op was committed (pre-fund failures: fee over cap, no gateway route —
  `operation_id` absent). Once the prior attempt committed its op (`operation_id` set),
  a re-`pay` can only dedup-reattach to the dead op and can never succeed; the actor
  refuses it with an explicit "request a fresh invoice" message instead of refreshing.
  **Universal attach rule (passes 11-12):** for EVERY verb, the §6a.2 same-key attach
  verifies ALL sizing fields of the request (fed, amount, fee cap) against the existing
  intent — the key identifies the operation, the sizing check guards its bounds; any
  mismatch (e.g. same receive nonce/to/amount with a changed fee cap) is refused as a
  conflict, never silently attached.
  **Pay sizing inputs (passes 8-9):** the `/v1/pay` request carries an `amount` — if present
  on an amount-carrying invoice it must match — a
  fee cap (defaulted from the DB `Policy`'s per-move cap), and the **source `fed`**
  (defaulted to the designated spending fed), so the §6a.4 pre-fund reservation
  (`amount + fee_cap` against that fed) and the TL-1 held-fed refusal are both enforceable
  BEFORE the `202`. The intent records ALL its sizing fields, and the same-key attach
  (§6a.2) verifies EVERY one of them — fed, amount, fee cap — against the existing intent
  (pass 11: a fed-only conflict check would let an attach drive money under different
  bounds than the original reserved); any mismatch is refused as a conflict. **Amountless
  invoices are refused outright** (step-6 review deviation from the earlier "REQUIRED amount"
  wording: the pinned lnv2 send API — `LightningClientModule::send(bolt11, gateway, meta)` —
  takes no amount parameter, so a stated amount cannot actually be paid; admitting one would
  202 an operation whose driver can only fail. Revisit if the SDK gains an amount override.)
- The `202` key = the ledger's correlation key — the async API and the audit trail share one
  keyspace.

## 6a.7 `wallet-cli`: thin client + standalone

One binary, two modes, **explicitly selected** (grill session): **client mode is the
default** — every operational verb becomes an HTTP call against `wallet-api` types (daemon
URL + token from config/env); if the daemon isn't running the verb fails with a clear
"start walletd, or rerun with `--standalone`" error — never a silent fallback.
**`--standalone` is a deliberate flag** (it takes the exclusive `db.lock`; a silent
fallback would quietly block a supervisor-restarting daemon behind a lock race the user
didn't choose): money/actor verbs spin up the same actor + driver components in-process
with the scheduler OFF, run one command, then shut down — one code path, no legacy fork.
`discover`, `probe`, and `tick` are deliberately Runtime-direct one-shots instead. This is
the isolated `--standalone tick` compatibility exception recorded by ADR-0031: it holds the
exclusive DB lock and re-scans durable goals immediately before apply; it is not a resident
host architecture. `pay`/`move` print the operation key and `await-*` long-polls, preserving
today's two-phase stdout contracts so the smoke scripts port mechanically. **The `watch`
verb is DELETED** — the daemon IS the watch: redundant in client mode, a daemon-without-an-API
in standalone mode (the 5.2c smoke is superseded by the 6a daemon gates, which re-validate
the ported scheduler in its new home). Clear errors: daemon-not-running (with the two
options), 401 (bad token), lock-held.

Standalone `tick` and dry-run `status` also require a complete federation registry before
they open any federation, probe, plan, or access WatchState. Their bootstrap uses the
poison-tolerant registry report but refuses if it skipped even one malformed row; it never
plans from the healthy subset. Preserve and copy the data directory, repair the exact
registry row from a consistent backup, and retry. Do not delete the row or use `join` as a
substitute. Other explicit user/admin standalone verbs retain their existing tolerant-row
behavior unless they require a world-complete planning view.

## 6a.8 Lifecycle + observability

`walletd` runs under systemd (user unit); fatal = exit non-zero, supervisor restarts
(§5.2.6 convention). SIGTERM (broad review, ordering matters): stop intake → **abort
drivers first** → drain the actor mailbox (every already-submitted journal transition
lands; nothing new can arrive after the aborts) → exit 0; restart reconciles
(abandon-and-resume IS the model — hours-long IO makes draining it impossible by design).
Drain-then-abort would let late driver transitions race the exit. Observability v1:
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
NoAttempt without demoting the candidate. Pending-map: coalescing, deadline, shutdown drain. Policy: default
seeding at `walletd init`; `PUT /v1/policy` validation goldens; standalone flag-override
equivalence; `walletd.toml` (host config) parse. Scheduler: ported 5.2 unit tests unchanged; abort arm; wake-
hint shortens sleep. axum: per-endpoint happy+error, 401, 202-contract, invoice deadline.
CLI: verbs against a mock server; error UX.

**Live gates (merge-blocking, minutes-scale):**
- `smoke_daemon_devimint.sh`: the existing money gates driven through a running `walletd`
  via the CLI-as-client.
- **The responsiveness gate:** watch active, a probe held in flight by a **misbehaving-
  gateway test double** (accepts-contract-never-provides-preimage); `POST /v1/pay` must
  reach its **first external fedimint call** (not just the 202) in <250 ms; two concurrent
  pays never serialize on IO. **Contention variant (broad review):** the same measurement
  repeated at FULL designed capacity — the admission cap's worth of concurrent drivers
  churning transitions through the mailbox — so the 250 ms bound is proven under load,
  not just in the two-op happy case.
- The 5.2c autonomous chain (discover→gate→probes→fund→evacuate) re-run under the daemon's
  scheduler.

**Burn-in (NOT a merge gate):** a 24h+ soak — watch active + periodic user ops; no lock
conflicts, no duplicate intents, ledger reconstructs the session; gates "daily-drive with
real sats" and 6a.2 (NWC).

## 6a.10 Build order

Lanes (worktree-parallel): **A** perform-path restructure (wallet-core executor +
runtime split; the hardest chunk — the existing suite is its gate) ∥ **B** `wallet-api`
(DTOs + Policy struct + walletd.toml). Then **C** `wallet-daemon` (actor, drivers+registry, scheduler port,
axum) ∥ **D** `wallet-cli` client + standalone. Then the gates, then the soak.

Non-goals (v1): NWC (6a.2, soak-gated) · TLS/remote/multi-user · streaming events ·
runtime HOST reconfiguration (walletd.toml = edit + restart; user Policy IS runtime-mutable via PUT) · packages/releases (Phase 8) · manual discover/probe endpoints · any UI.

Settled during the eng review (do not re-litigate): the 6a.0 de-risking spike was REJECTED
(build directly; fedimint's state-machine executor is trusted for client-level concurrency —
if the build disproves this, stop and re-plan rather than serializing silently); TODOS.md is
retired (this spec + `br` beads at build time are the backlog).
