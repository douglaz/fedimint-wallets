# Archive — finished work, kept for provenance

Everything here describes work that **shipped**. It is kept because it records *why* the code is
the way it is, and because the ADRs and the roadmap cite it as evidence. None of it governs current
behaviour: if a document here disagrees with `docs/` or with the code, the code wins and the
document is history.

**This is not the same as [`../superseded/`](../superseded/).** That directory holds work that
turned out to be *wrong or was retracted*. This one holds work that was *right and is done*.

Looking for what's current? Start at [`../roadmap-to-v1.md`](../roadmap-to-v1.md).

## Completed phase plans and specs

The engine was built in phases, all of which shipped; phases 1 through 6a are running in
production. Their banners are not uniform, so trust the roadmap rather than any single header:
`phase2-plan` and `phase4-plan` do say `STATUS: COMPLETE`; `phase3-plan` says only `3.A COMPLETE`
because 3.B/3.C were re-scoped into phase 5; `phase5-plan` marks 5.0 complete and still reads
`NEXT: 5.1`, though 5.1 shipped too; and the phase 1 implementation spec carries a
"hardened through N passes" banner instead. [../roadmap-to-v1.md](../roadmap-to-v1.md) is the
authority on what actually completed.

| Document | What it covers |
|---|---|
| [integration-phase-plan.md](./integration-phase-plan.md) | The locked architecture for the whole integration phase; superseded as a *plan* by the roadmap, still the origin of several ADRs |
| [phase1-implementation-spec.md](./phase1-implementation-spec.md) | Proving the money path — join, receive, pay, cross-federation move |
| [phase2-plan.md](./phase2-plan.md) | Sense and decide: probe → score → snapshot → decide → apply |
| [phase3-plan.md](./phase3-plan.md) | Evacuation execution |
| [phase4-plan.md](./phase4-plan.md) | Engine hardening and the operation ledger. Note its
companion [phase4-implementation-spec.md](../phase4-implementation-spec.md) is NOT archived: the
live ledger spec names it authoritative for field-level shapes, so it still governs. |
| [phase5-plan.md](./phase5-plan.md) | The active probe and federation discovery |

## Discharged implementation specs

Both say so themselves: they existed to authorise one implementation bead, and that bead shipped.
No live *document* links them — but the shipped **code** still cites both as semantic authority
(e.g. `wallet-core/src/allocator.rs` implements "ROUTE ECONOMICS §Q5"; rustdoc across the money path
and `smoke_recover_devimint.sh` cite the recovery spec, including decision labels D3/D4 that
ADR-0025 does not carry). Those citations were repointed here rather than dropped. Archived means
finished, not unread.

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
