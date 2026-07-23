# Simple Fedimint Wallet

A Rust Fedimint wallet project for a private, no-KYC, spending-focused ecash wallet:
Wallet-of-Satoshi-simple on the surface, with an on-device multi-federation
Allocator underneath.

This repo is currently the headless engine and CLI. The Android Slint app is still
planned, not built.

## Current status

As of 2026-07-07, the core engine is past the original analysis/report stage:

- **Phase 1 money engine: complete.** Join, receive, pay, exact-net direct inflow,
  cross-federation move, crash/reconcile recovery, and idempotent replay were
  live-validated on devimint.
- **Phase 2 sense + decide: complete.** Real federation probing feeds scoring,
  snapshot building, allocation decisions, and executor application through
  `wallet-cli tick` / `wallet-cli status`.
- **Phase 3.A evacuation: complete.** Shutdown/degradation signals can trigger an
  LN-only evacuation from a dying federation into an eligible healthy federation.
- **Phase 4 hardening + ledger: complete.** Review P1s are closed, per-federation
  caps are enforced, terminal stranded moves are explicit, and the append-only
  operation ledger is exposed through `wallet-cli history` / `wallet-cli show`.
- **Phase 5.0 active probe: complete.** The wallet can spend a small amount through
  a candidate federation and redeem it back, producing a sustained-window
  redeemability verdict for future discovery-driven funding decisions.

Next work: Phase 5.1 discovery and triggers, then the Android frontend, recovery,
and release hardening. See [docs/roadmap-to-v1.md](./docs/roadmap-to-v1.md).

## What is in this repo

- [wallet-core](./wallet-core/) - dependency-light pure logic: scoring, allocation,
  probe verdicts, ledger types, executor traits, and replay/idempotency behavior.
- [wallet-fedimint](./wallet-fedimint/) - Fedimint SDK integration: multi-federation
  clients, durable journal, executor, runtime, probe runner, move protocol, and
  operation ledger storage.
- [wallet-cli](./wallet-cli/) - the first-class headless frontend. It supports
  joining federations, balance/listing, receiving, paying, direct inflows,
  cross-federation moves, evacuations through `tick`, active probes, reconciliation,
  and ledger inspection.
- [docs/](./docs/) - the build plans, runbooks, ADRs, review notes, and specs.
- [SIMPLE-FEDIMINT-WALLET-REPORT.md](./SIMPLE-FEDIMINT-WALLET-REPORT.md) - the
  original wallet survey and product design report. It is useful background, but the
  ADRs and roadmap supersede it where they differ.

## Allocator policy

The standing instructions the Allocator runs against live in one stored `Policy`, edited
field-by-field with `wallet-cli policy set` and printed by `wallet-cli policy get`. The
balance knobs are `--per-fed-cap`, `--spending-target`, and `--standby-target` (all msat);
the two fee caps are deliberately different shapes:

- `--max-fee` - ABSOLUTE fee cap in msat (a flat ceiling, not scaled by the amount). Of the
  Allocator's own moves it bounds only evacuations, where the amount is whatever remnant a
  dying federation still holds and a proportional cap could compute below the gateway's base
  fee and refuse the drain. It is
  also the default `--fee-cap` for the manual `pay`/`move`/`receive`/`direct-inflow`
  commands, so setting it very low refuses those too.
- `--max-fee-bps-of-move` - PROPORTIONAL fee cap for funding moves (top-up and standby), in
  basis points of the amount moved, `1`-`10000`; default `300` (3%). Funding sizing reserves
  it from the source, so `amount + amount * bps / 10000` always fits the source budget and a
  positive surplus is never refused for being smaller than a flat cap.

A `--max-fee-bps-of-move` of `0` (every funding move would get a zero cap and fail) or above
`10000` is rejected by policy validation. Setting it very low can still under-cap small moves
so they fail at perform time — a per-route economic floor is planned separately.

See [docs/real-sats-pilot-runbook.md](./docs/real-sats-pilot-runbook.md) for suggested
pilot values.

## Local development

The workspace is pinned to `douglaz/fedimint` at commit
`b108ec66ab21b70e1eea35d8663d9941a665ad58`. The Fedimint native dependencies are
expected from the sibling Fedimint checkout's Nix environment:

```bash
nix develop /home/master/p/fedimint -c cargo build --workspace
nix develop /home/master/p/fedimint -c cargo test --workspace
nix develop /home/master/p/fedimint -c cargo clippy --workspace -- -D warnings
```

Live money-path validation uses devimint and the smoke scripts under
[wallet-cli/tests/](./wallet-cli/tests/). Start with
[docs/devimint-runbook.md](./docs/devimint-runbook.md) for the two-federation setup,
gateway pinning details, and known gotchas.

## Design docs

- [CONTEXT.md](./CONTEXT.md) - canonical product language and domain definitions.
- [docs/roadmap-to-v1.md](./docs/roadmap-to-v1.md) - current build sequence and
  definition of "fully featured v1".
- [docs/phase5-plan.md](./docs/phase5-plan.md) - active probe, discovery, and
  self-running loop plan.
- [docs/operation-history-spec.md](./docs/operation-history-spec.md) - append-only
  ledger semantics.
- [docs/adr/](./docs/adr/) - architecture decisions. These are canonical when they
  conflict with older report text.

## Original wallet survey

This project began with a survey of existing Fedimint/ecash wallets. They are not
vendored here and are gitignored when cloned locally for analysis.

| Wallet | Stack | Source |
| --- | --- | --- |
| ecash-app | Flutter + Rust (FRB), Android | <https://github.com/fedimint/ecash-app> |
| harbor | pure Rust (iced), desktop | <https://github.com/HarborWallet/harbor> |
| vipr-wallet | Vue 3 + TS PWA | <https://github.com/ngutech21/vipr-wallet> |
| Fedi | Rust core + React Native/PWA | <https://github.com/fedixyz/fedi> |

## License

AGPL-3.0-or-later. See [LICENSE](./LICENSE) and
[ADR-0009](./docs/adr/0009-license-agpl.md).
