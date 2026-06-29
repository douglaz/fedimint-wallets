# Integration-phase architecture (locked)

From the pure decision core (scorer + allocator + executor, all on `main`, tested) to a
working on-device agent that actually moves ecash across federations. Source of truth: the
design report, ADRs 0001-0020, `docs/federation-data-sources-spec.md`. Reviewed via
`/plan-eng-review` + a codex outside-voice pass; the abstraction level was trimmed as a
result (see "Right-sizing").

## What exists (pure, on main)
- `scorer::score(&FederationFacts) -> FederationVerdict` — eligibility + rank (10 tests).
- `allocator::decide(&AllocatorSnapshot) -> Vec<AllocatorDecision>` (8 tests).
- `executor::apply/reconcile` over a `Journal` + `Executor` trait (7 tests; `MockExecutor`).

These are the "decide" half: deterministic, no I/O. The integration phase is "sense" + "act".

## Right-sizing (the abstraction line)
Rule: **add a trait only when it protects pure deterministic logic, OR a second real
production implementation exists.** A test fake + one fedimint adapter is not enough. So:

- **KEEP — `Executor` / `Journal` traits.** The one place abstraction earns it: isolates the
  crash-replay/idempotency state machine (our deterministic code), testable without a
  federation. Caveat: those unit tests prove WAL behavior only, NOT that ecash moves — the
  real `FedimintExecutor` + `SqliteJournal` must still be devimint-tested.
- **CONCRETE — `MultiClient` / `FedimintRuntime`.** Owns the fedimint SDK + state. One real
  impl ever; a trait would be ceremony.
- **DROP → concrete — `Prober`** (`FedimintProbeRunner` over MultiClient), **`Discovery`**
  (`NostrClient` + `ObserverClient` + a candidate assembler), **`Orchestrator`** (a concrete
  tick runner). Mock versions of these would only test wiring, not real behavior.

Net: the runtime is **one concrete `FedimintRuntime`/`MultiClient` for all fedimint I/O +
the existing narrow replay executor**, with concrete sensing/discovery/orchestration on top.

ADR-0006 reframe: **V1 holds funds in ~2 federations** (spending + warm standby). "Many
federations" is only the *discovery/probe* universe, not the active set. So `MultiClient`
manages a small active set + ephemeral probe-joins, not an N-client registry.

## Data flow

```
   DISCOVERY (untrusted, concrete)          SENSE (concrete)                  DECIDE (pure)        ACT
 ┌────────────────────────────┐   ┌──────────────────────────────┐   ┌──────────────────┐  ┌─────────────┐
 │ NostrClient  → candidates  │   │ facts assembler, per cand:    │   │  score()         │  │ apply() /   │
 │ ObserverClient → prior+list│──▶│  1 fetch auth ClientConfig    │──▶│   → eligible set │─▶│ reconcile() │
 └────────────────────────────┘   │    (structural, FREE)         │   │  build snapshot  │  │  (Executor  │
                                   │  2 attach Observer prior      │   │   → decide()     │  │   trait)    │
                                   │  3 IF floor passes →           │   └──────────────────┘  └──────┬──────┘
                                   │    active probe (costs sats)  │                                  ▼
                                   │   (FedimintProbeRunner)        │                         FedimintRuntime
                                   └──────────────────────────────┘                          /MultiClient (~2
                                                                                              active + probe-joins)
```

## Trust + cost boundaries (load-bearing)
- **Pure core never imports the fedimint SDK.** scorer/allocator stay plain functions over
  data; the replay executor stays behind its trait. Everything else is concrete glue.
- **Trust gate:** Discovery/Observer are untrusted → candidates + priors only. Trust comes
  from OUR `fetch_config` (authenticated) + OUR probe, then `score()` gates (ADR-0020).
- **Tiered probing (sats safety):** passive signals (config fetch, Observer) are free and
  gate which feds earn an active, sats-spending round-trip probe. Cache with a TTL. Never
  active-probe the whole universe.

## Testing — a real-not-fake pyramid
- **Fast (every `cargo test`):** scorer, allocator, replay/idempotency, AND parsing of
  Observer/Nostr/ClientConfig from **recorded REAL fixtures** (captured from the live
  services during the data-sources research). Real data, real parsers, sub-second. No fake
  fedimint.
- **Medium:** real SQLite + orchestration logic over fixed snapshots. No simulated ecash.
- **Slow (gated, devimint only):** join, receive/pay through a real gateway, move funds via
  `apply()`, restart, `reconcile()`, assert balances + **no double-pay**. Bootstrap
  bitcoind+federation+gateway+lightning **once per session/CI job**; per-test fresh client
  DBs + idempotency keys + invoices; regtest **mine-on-demand + bounded-timeout polling,
  never `sleep`**. Behind `--features devimint-e2e` / a nextest `devimint` profile. PR =
  one smoke path; nightly = full crash/replay/failure matrix.
- **Hard rule: no `MockFedimintClient`.** Don't simulate balances, settlement, gateway, or
  consensus — that is fake confidence. Fakes only for owned pure boundaries (`Executor::
  perform` failure injection) and untrusted-HTTP fixture parsing.

> "If real `apply()` cannot move ecash between two devimint federations and survive replay,
> the rest of the architecture is decorative." So the devimint harness is the Phase 1
> *deliverable*, not overhead.

## Phasing
- **Phase 1 — prove the money path.** `MultiClient`/`FedimintRuntime` (join/balance/receive/
  pay) + `FedimintExecutor` (the 4 actions) + `SqliteJournal` + the **devimint harness**.
  Exit: a devimint test moves ecash between two federations via `apply()` and survives
  `reconcile()` with no double-pay. Candidate set = a bundled invite list (no scorer/
  discovery yet).
- **Phase 2 — sense + decide.** `FedimintProbeRunner` (config-fetch + round-trip + peg-out)
  + the facts assembler → real `FederationFacts` → `score()` → snapshot → `decide()` →
  `apply()`. Recorded-fixture parser tests in the fast layer. Exit: full tick vs devimint.
- **Phase 3 — discovery + triggers.** `ObserverClient` + `NostrClient` (untrusted candidate
  set + prior) + the concrete tick runner's triggers (foreground / WorkManager / push) +
  evacuation on shutdown notice. Exit: self-driving discover → score → rebalance vs devimint.
- Parallel / independent: T1 hardware spikes (Slint camera, Block Store), the Slint UI.

## Reuse (don't reinvent)
- `fedimint-client` — the official client. **Layer 1, do not reinvent** join/receive/pay.
- `harbor` — a real multi-federation fedimint wallet (multi-client + SQLite + receive/pay).
  **Lift its patterns** for MultiClient + storage.
- `devimint` — fedimint's official integration harness. **Use it**; don't build regtest orchestration.
- Android background: **WorkManager** (Layer 1). No custom daemon (Doze kills daemons).

## GSTACK REVIEW REPORT

| Run | Status | Findings (absorbed) |
|-----|--------|---------------------|
| plan-eng-review (Architecture + Tests) | done | Hexagonal 4-trait design trimmed to one replay-executor trait + concrete fedimint runtime; ADR-0006 reframe (V1 active set ~2 feds, not N); real-not-fake test pyramid. |
| codex (outside voice) | done | Converged: KEEP Executor/Journal trait, CONCRETE MultiClient, DROP Discovery/Prober/Orchestrator traits; no MockFedimintClient; devimint harness is the Phase 1 deliverable; "until apply() moves ecash + survives replay, the rest is decorative." |

Scope decisions:
- D1 → review the integration-phase architecture (before coding).
- D2/D3 → trim to concrete-over-traits; build **Phase 1 first** (prove the money path via
  devimint), then sense (Phase 2), then discovery+triggers (Phase 3).

VERDICT: architecture LOCKED (CODEX absorbed). Build Phase 1: MultiClient + FedimintExecutor
+ SqliteJournal + devimint harness; exit gate = ecash moves between two devimint federations
via apply() and survives reconcile() with no double-pay.

NO UNRESOLVED DECISIONS
