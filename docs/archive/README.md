# Archive — finished work, kept for provenance

Everything here describes work that **shipped**. It is kept because it records *why* the code is
the way it is, and because the ADRs and the roadmap cite it as evidence. None of it governs current
behaviour: if a document here disagrees with `docs/` or with the code, the code wins and the
document is history.

**This is not the same as [`../superseded/`](../superseded/).** That directory holds work that
turned out to be *wrong or was retracted*. This one holds work that was *right and is done*.

Looking for what's current? Start at [`../roadmap-to-v1.md`](../roadmap-to-v1.md).

## Completed phase plans and specs

The engine was built in phases; each carries its own `STATUS: COMPLETE` banner with the gate that
proved it. Phases 1 through 6a are all shipped and running in production.

| Document | What it covers |
|---|---|
| [integration-phase-plan.md](./integration-phase-plan.md) | The locked architecture for the whole integration phase; superseded as a *plan* by the roadmap, still the origin of several ADRs |
| [phase1-implementation-spec.md](./phase1-implementation-spec.md) | Proving the money path — join, receive, pay, cross-federation move |
| [phase2-plan.md](./phase2-plan.md) | Sense and decide: probe → score → snapshot → decide → apply |
| [phase3-plan.md](./phase3-plan.md) | Evacuation execution |
| [phase4-plan.md](./phase4-plan.md) · [phase4-implementation-spec.md](./phase4-implementation-spec.md) | Engine hardening and the operation ledger |
| [phase5-plan.md](./phase5-plan.md) | The active probe and federation discovery |

## Discharged implementation specs

Both of these say so themselves: they existed to authorise one implementation bead, and that bead
shipped. Neither is referenced by any live document.

- [wallet-recovery-spec.md](./wallet-recovery-spec.md) — seed-based recovery (`br-m9m`). The
  shipped behaviour is described by [ADR-0025](../adr/0025-recovery-fresh-partition-seed-is-the-backup-unit.md);
  the remaining gap is recorded in [recovery-failure-gate-analysis.md](../recovery-failure-gate-analysis.md).
- [route-economics-decisions.md](./route-economics-decisions.md) — the per-pair economic move
  floor. Its own header notes that no production code ships from it.

## Point-in-time reviews

Snapshots of a codebase that has been substantially rewritten since. Their findings were either
fixed or promoted into ADRs, so read them as history rather than as a to-do list.

- [2026-07-03-engine-review.md](./2026-07-03-engine-review.md)
- [2026-07-05-fresh-eyes-review.md](./2026-07-05-fresh-eyes-review.md)

## Prior art

- [SIMPLE-FEDIMINT-WALLET-REPORT.md](./SIMPLE-FEDIMINT-WALLET-REPORT.md) — the original survey of
  four existing Fedimint wallets that motivated this project. The decisions it fed are now in
  [`../adr/`](../adr/); its most durable finding, that every surveyed wallet fails at seed
  protection, is what [ADR-0026](../adr/0026-seed-at-rest-encryption-headless.md) exists to answer.
