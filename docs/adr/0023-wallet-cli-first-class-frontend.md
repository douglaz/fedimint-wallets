---
status: accepted
---
# wallet-cli is a first-class, permanent frontend — co-equal with the Android app

The wallet is an **engine** (`wallet-core` pure logic + `wallet-fedimint` SDK integration)
with **two permanent, co-equal frontends** over its public API: the **Android app** (Slint UI,
the consumer product) and **`wallet-cli`** (headless, scriptable). `wallet-cli` is NOT a test
shim — it is a shipped, maintained-forever wallet, as important as the app.

## Why
- **A complete, honest engine API.** Two real frontends force the engine's public API to be
  complete and clean; neither frontend reaches around it into internals.
- **The crash-safety gate becomes natural and real.** The hardest, most important test — kill
  mid-move, restart, prove no double-pay/double-invoice — needs *real process death* against a
  persistent DB. A CLI does this (`wallet-cli move …`, `kill -9`, `wallet-cli reconcile`); an
  in-process `#[tokio::test]` cannot kill and restart its own process.
- **It is how the ecosystem tests itself.** devimint drives `fedimint-cli`; our devimint tests
  drive `wallet-cli` the same way — more realistic than wiring the Rust API in-process.
- **A runnable, dogfoodable, auditable wallet now** — headless, scriptable, long before any UI.
  Fits the no-operator / open-source / on-device-agent ethos (ADR-0009/0014): power users can
  run and audit the entire wallet from a shell.
- **Precedent:** `cyberkrill` (core + CLI, the parent repo here), `fedimint-cli`, and
  harbor/fedi (shared core + a frontend).

## Consequences
- **Workspace:** `wallet-core` (pure) + `wallet-fedimint` (engine/SDK) + `wallet-cli` (bin) +
  (later) the Android frontend. The engine is the shared library; the CLI and the app are two
  frontends over the *same* public API.
- **The engine must not assume a UI.** No interactive prompts / no I/O in the core; frontends
  own all I/O. The public API is the contract both honor — keep it complete and stable.
- **`wallet-cli` is maintained forever:** its own docs, `--help`, and a released binary
  artifact (like fedimint-cli). It is a product surface, not a fixture.
- **Testing:** the money-path and crash/reconcile gates are **CLI-driven** against devimint
  (real process kills), replacing the spec's earlier in-process e2e. Pure logic stays plain
  `cargo test`.
- **Build order:** `wallet-cli` grows step-by-step *alongside* the engine (its `join`/`balance`
  commands land with `MultiClient` in step 3; its `move`/`pay`/`reconcile` + the kill-mid-move
  crash test land with the executor in step 4), not bolted on at the end.
