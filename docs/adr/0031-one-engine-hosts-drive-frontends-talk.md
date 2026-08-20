---
status: accepted
---
# One engine for every frontend; hosts drive it; `watch_once` is a harness

The wallet targets three frontends — web + walletd, cli + walletd, and the Android app
(Phase 6b) — and they must behave identically. This ADR fixes the architecture that makes
"identically" structural rather than test-enforced, and settles where the br-p93 goal-conflict
invariant lives. Three decisions:

## 1. The service actor is the sole admission and decision layer for resident hosts

"One live key per allocator goal" (br-p93, `wallet-core/src/conflict.rs`) is an
**admission-contract invariant** for actor-hosted work: a property of the serialized admission
point — the service actor's guarded commit path (`decide_op` / `CommitTick`), which exclusively
owns **intent admission** while that host is resident — not a self-defending property of the
journal, and not mere scheduler politeness. Ledger repair is the deliberate off-actor exception:
the O(ledger) scan and ledger-row repair stay off actor, while raw Pay/Receive terminal-intent
synchronization is routed through the actor by `repair_ledger_with_actor`. Both sides are
attempt/sequence/status fenced and neither admits a fresh intent. Consequences of naming it that way:

- There are **two enforcement checks inside the serialized actor** until the journal boundary is
  sealed: `CommitTick` takes one fresh durable scan and folds each admitted decision back into its
  batch view, while the public `decide_op` fresh path re-scans before admitting a goal-bearing
  `Actor::Agent` request that bypassed `CommitTick` (`wallet-fedimint/src/service/actor.rs`). Both
  checks are load-bearing; their race-freedom comes from the actor's exclusive ownership of
  admission, not from the journal.
- The actor scheduler's earlier `GoalBlockers` call sites — reconcile projection, route-pricing
  skip, and planner suppression — are **advisory pre-filters**. They save network I/O and tick
  reservation capacity and are allowed to be wrong; the actor commit seam is load-bearing for
  money safety. Reviews must not demand that those pre-filters grow enforcement duties.
- The shipped `wallet-cli --standalone tick` is a deliberately isolated compatibility exception,
  not a resident host or a model for a future frontend. It holds the wallet's exclusive DB lock,
  has no caller-supplied decision batch, and plans through `decide_with_blockers`, which emits at
  most one key per logical goal. Its own fresh durable re-scan in `Runtime::tick` immediately
   before `apply_with_allocator_admission` is therefore a second, load-bearing seam for that
   one-shot command;
  its earlier standalone reconcile/route/planner projections remain advisory. Keep that seam and
  its separate regression tests until the command moves onto the actor or is retired.
- The residual is stated, not papered over: outside that isolated command, "the actor is the sole
  writer of agent intents" is a convention today, not a compiler-checked fact —
   `wallet-core`'s `apply_with_allocator_admission` and
   `decide_and_journal_with_allocator_reservations` are public, so another in-process caller can
   bypass the seam. Generic public admission sealing (visibility narrowing or an admission token
   only guarded paths can construct) remains separate follow-up
   `br-seal-agent-admission-yfr`; it is not deferred on evacuation supersession.

`br-n8o` now gives the actor a narrower, completed journal-boundary duty: `CommitTick` owns the
serialized exchange of one policy-qualified, pre-artifact structural-refusal evacuation for a
fresh occurrence/key. The exclusive-DB standalone `Runtime::tick` seam performs that same
atomic journal exchange. It writes the durable canonical `EvacuationSupersessionRecord` plus
its reverse sidecar, preserves the retired parent's audit identity, and rejects ambiguity or a
generation/occurrence that is not current; marker claim is consumed on `Pending -> Executing`.
Replacement-path validation, admission, fresh-blocker, CAS-false, and confirmed-uncommitted errors
retain the exact Pending parent marker: only a successful (or exactly confirmed committed) exchange
may consume it. The separate, authoritative planner no-child disposition still clears its exact
marker when no replacement was selected. Standalone `Runtime::status` is a dry diagnostic: for a
stale structural replacement it warns and returns the scored/designation report with no would-run
decisions, rather than advertising an impossible child or deferred ordinary work. It writes neither
the exchange nor a child. Daemon scheduler status remains strict and rejects the same stale
replacement because it owns occurrence allocation.
Those duties do not make the public generic admission surface self-sealing.

## 2. All three resident frontends sit on that one actor; Android embeds it in-process

Web and the default cli mode reach the actor through walletd. The standalone compatibility
command above remains a one-shot operator surface, not another resident frontend engine. The
Android app embeds the **same actor** as a tokio task in the app's own process, with the UI
talking to it over async channels — exactly how walletd hosts it, minus HTTP. JNI is in-process
FFI (the engine is a `.so` loaded into the app process; with a Slint UI the engine/UI boundary
is pure Rust), so JNI appears only at the edges where Rust calls Android platform APIs
(Keystore, WorkManager, lifecycle). There is no second resident engine, no external process,
and no IPC on the phone.

## 3. Hosts drive, the engine decides

Scheduler cycles are engine work exposed to the host; resident loops, timers, and restart
supervision are **host** concerns:

- **walletd** keeps its current driver unchanged: the resident adaptive-sleep loop and the
  settlement-stall watchdog whose self-heal is process-exit + systemd restart
  (`wallet-fedimint/src/service/scheduler.rs`). That is one host's driver, not the engine.
- **Android** (Phase 6b) drives the same cycle from platform wakes (WorkManager /
  foreground-service callbacks). Process death is already survivable — intents are durable
  and reconcile re-attaches on start — but the watchdog's restart model needs an Android
  answer (likely WorkManager retry + reconcile; no supervisor process exists there).
- br-p93 is a **prerequisite** for external-wake driving, and this is why the drive split is
  safe: under the old global gates, a wake arriving while any retry was pending did nothing;
  with conflict-scoped gates, an arbitrary, possibly-overlapping "run a cycle now" cannot
  duplicate work unless the goal model itself fails.

**`Runtime::watch_once` is a `Runtime` dev/test harness, not a standalone production
cycle.** It is the one-shot cycle shape
the Android drive needs, welded to the wrong substrate — a second admission implementation
that br-p93 had to patch in parallel with the scheduler, with parallel test suites pleading
that the two "cannot drift". The Android drive is built on the actor's cycle, never on
`Runtime`; `watch_once` survives only as a harness, and no production scheduler is ever built
on it again. This does not delete or demote the distinct standalone one-shot `Runtime::tick`
compatibility command described above: standalone cli mode already runs with the scheduler OFF, per the
phase-6a §6a.7 decision, and its isolated admission seam remains load-bearing while that
operator command exists.

## Why

br-p93's review surfaced the tell: every conflict check had to land twice (standalone
`watch_once` and the daemon scheduler), the two global gates it replaced existed twice, and
the shared `GoalBlockers` value exists specifically so the two paths "cannot drift into
disagreeing". Code that must be kept equivalent by discipline is one implementation too many
— and the third frontend was about to force the choice of which one Android gets. Deciding
late had a concrete failure mode: Phase 6b's path of least resistance would have been either
porting the resident loop into a foreground service (Doze-fragile, battery-hostile) or
reviving `watch_once` as the mobile scheduler (two admission implementations, forever, with
every future invariant — including the actor-owned exchange and the isolated standalone
equivalent — landing twice).

## Alternatives rejected

- **The journal defends itself (universal atomic goal admission at insertion), now.** Stronger
  in the abstract, but broader than the completed br-n8o replacement exchange. Generic public
  admission sealing remains `br-seal-agent-admission-yfr`; the sidecar-linked, authority- and
  generation-fenced evacuation exchange is not a universal insertion guard.
- **Android on `Runtime::watch_once`.** Permanent two-implementation drift; rejected above.
- **Resident loop in an Android foreground service.** Fights the platform's process model
  instead of using it; the durable-intent + reconcile design already makes externally-driven
  one-shot cycles the natural mobile shape.
