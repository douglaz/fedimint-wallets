# Phase 2 plan — sense + decide

Phase 1 built + validated the **money engine** (join/receive/pay/DirectInflow/Move + crash gate).
Phase 2 wires the already-built, golden-tested pure decision core (`scorer::score`,
`allocator::decide`) to **real federation data** and drives the executor — the "sense + decide"
layer of `docs/integration-phase-plan.md`. Everything here builds on the Phase-1 engine; nothing
in `wallet-core`'s pure logic changes (it is the contract).

## Data flow (the whole of Phase 2)
```
SENSE (concrete, wallet-fedimint)                     DECIDE (pure, wallet-core)        ACT
  probe each JOINED fed:                              score(FederationFacts, policy)    apply(decisions)
   1. structural facts from the authenticated  ─────▶  → eligible set            ─────▶ → FedimintExecutor
      ClientConfig (guardian_count, threshold,        build AllocatorSnapshot           (Phase-1 Move/
      is_mainnet, modules, has_lnv2) — FREE            (FedBalance + status per fed)      DirectInflow)
   2. empirical probes (the trust gate, ADR-0017):    decide(snapshot, occurrence)
      quorum_live, round_trip_ok, peg_out_quotable,    → Vec<AllocatorDecision>
      latency_ms                                       (TopUp/Move/DirectInflow/RefuseInflow)
   3. FedBalance (spendable/in_flight/claimable) from
      the client balance + op-log
```

## Scope (ADR-grounded, so the build stays honest)
- **Active set ≈ 2 feds (ADR-0006):** V1 holds a spending fed + a warm standby. `MultiClient`
  manages this small active set; "many federations" is the Phase-3 discovery universe, NOT here.
- **Trust gate (ADR-0017/0019/0020):** trust comes from OUR authenticated config fetch + OUR
  probe, then `score()` gates. Structural facts are free; empirical probes cost a round-trip.
- **Balance cap (ADR-0018):** the allocator already enforces `per_fed_cap`; Phase 2 supplies it.
- **Standing instruction (ADR-0009):** the policy (`per_fed_cap`, `target_spending_balance`,
  `standby_target`, `max_fee`, the spending/standby designation) is the user's standing
  instruction. V1: sensible DEFAULTS overridable by `wallet-cli` flags / a small config file.
- **Out of scope → Phase 3:** Nostr/Observer discovery of NEW candidates, the automated triggers
  (foreground/WorkManager/push), and executing `Evacuate` on a shutdown notice. Phase 2's tick is
  invoked manually (`wallet-cli tick`); it senses + decides + acts over the JOINED feds only.

## Build order
- **2.1 Probe runner + facts assembler** (`wallet-fedimint`): `FedimintProbeRunner` over
  `MultiClient` → real `FederationFacts` (structural from `ClientConfig`, empirical from probes) +
  `FedBalance`/`FederationStatus`. The ASSEMBLER (raw probe results → facts/snapshot) is pure +
  golden-tested from recorded fixtures; the probing itself is I/O (devimint-validated).
- **2.2 Orchestrator tick + CLI** (`wallet-fedimint` runtime + `wallet-cli`): one tick =
  probe → `score()` → build `AllocatorSnapshot` → `decide()` → `apply()` (the Phase-1 executor
  performs the Moves/DirectInflows). `wallet-cli tick` (run one tick) + `wallet-cli status` (show
  the scored/decided view). The policy comes from defaults + CLI/config.
- **2.3 Exit gate (devimint, two-fed):** a full tick over two joined feds with a real imbalance
  drives a real rebalance `apply()` — assert the allocator's decision moved funds as intended
  (reuse the two-fed harness + the reliable await pattern).

## Testing
- **Fast:** recorded-fixture parser/assembler tests (raw config/probe JSON → `FederationFacts` →
  `score()` → `AllocatorSnapshot`), + the existing pure `score`/`decide` golden tests. No devimint.
- **Slow (devimint, gated):** 2.1 probe a live fed → facts match reality; 2.3 the full tick moves
  funds. Reuse the two-fed harness (`docs/devimint-two-fed-harness.patch`) + await-send-first.

## Open decisions (surface before building where genuinely product-shaping)
- **Probe intensity for the active set:** the scorer's `round_trip_ok`/`peg_out_quotable` require
  ACTIVE probes; the joined feds are funds-holding so an occasional round-trip is worth it, but
  cache with a TTL (never re-probe every tick). Confirm the TTL + whether v1 does the sats-spending
  round-trip on the active set or defers it (facts default to the free structural signals).
