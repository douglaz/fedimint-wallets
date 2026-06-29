---
status: accepted
---
# Integration layer: concrete over traits, a real-not-fake test pyramid, money-path first

Reviewed via `/plan-eng-review` + a codex outside-voice pass. Detailed plan:
[../integration-phase-plan.md](../integration-phase-plan.md).

The integration phase (real fedimint-client I/O on top of the pure scorer/allocator/
executor core) is built **concrete-first**, not as a hexagonal port-and-adapter lattice.

## Decisions

- **Trait line:** add a trait only when it protects pure deterministic logic, OR a second
  real production implementation exists. A test fake + one fedimint adapter is not enough.
  - KEEP the `Executor`/`Journal` traits (isolate the crash-replay/idempotency state
    machine — deterministic, testable without a federation).
  - CONCRETE `MultiClient`/`FedimintRuntime` owns all fedimint I/O (one impl ever).
  - DROP the planned `Discovery`, `Prober`, and `Orchestrator` traits → concrete types
    (`NostrClient` + `ObserverClient` + candidate assembler; `FedimintProbeRunner`; a
    concrete tick runner). Mocks of these would test wiring, not real behavior.
- **No `MockFedimintClient`, ever.** Don't simulate balances, settlement, gateway, or
  consensus — that is fake confidence. The only real test of the money path is devimint.
- **Test pyramid:** fast (pure logic + parsing from recorded REAL fixtures, every
  `cargo test`) → medium (real SQLite + orchestration over fixed snapshots) → slow
  (devimint only, gated behind `--features devimint-e2e`, shared once-per-session fixture,
  regtest mine-on-demand + bounded-timeout polling, never `sleep`).
- **Build order:** Phase 1 = `MultiClient` + `FedimintExecutor` + `SqliteJournal` + the
  devimint harness. Exit gate: ecash moves between two devimint federations via `apply()`
  and survives `reconcile()` with no double-pay. Then Phase 2 (sense+score), Phase 3
  (discovery+triggers).

## Consequences

- The pure core never imports the fedimint SDK; it stays mock-free and deterministic.
- ADR-0006 reframe: V1's active set is ~2 federations (spending + warm standby);
  `MultiClient` manages that small set + ephemeral probe-joins, not an N-client registry.
  "Many federations" is the discovery/probe universe only.
- The devimint harness (TODOS T4) is the Phase 1 deliverable, not later overhead: "until
  `apply()` provably moves ecash through devimint and survives replay, the rest of the
  architecture is decorative."
- Build method note: Phase 1 is real-SDK + devimint work, not a pure-function golden-test
  task, so it is hand-built + devimint-tested rather than driven by the rb-lite loop.
