# Simple Fedimint Wallet

A Rust Fedimint wallet project for a private, no-KYC, spending-focused ecash wallet:
Wallet-of-Satoshi-simple on the surface, with an on-device multi-federation
Allocator underneath.

This repo is currently the headless engine, the 24/7 `walletd` daemon, and the CLI.
The Android Slint app is still planned, not built.

## Current status

As of 2026-07-26, the engine, the `walletd` daemon, discovery, and seed recovery are
live and devimint-validated:

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
  redeemability verdict for discovery-driven funding decisions.
- **Phase 5.1 discovery + triggers: complete.** Source-agnostic candidate discovery
  (Observer HTTP + manual), the candidate registry, and probe-gated funding: a
  discovered/auto-joined federation is fundable only after a sustained active-probe
  pass, never on discovery alone.
- **Phase 6a `walletd` daemon + local API: complete.** A 24/7 single-owner daemon
  (axum on 127.0.0.1 + bearer token) owns the DB and runs the watch scheduler;
  `wallet-cli` is a thin client (client mode default, `--standalone` explicit). Route
  pricing and all network IO run OFF the actor so a mid-flight (hours-long LN) payment
  never blocks another operation (ADR-0024); the responsiveness gate holds
  `POST /v1/pay` to its first external call in <250 ms.
- **Seed recovery: complete.** A wallet restores each federation's ecash balance from
  the 12-word seed alone (fedimint recovery), with complete-or-fail semantics (a failed
  module recovery terminalizes rather than hanging forever) — live-validated on devimint.
- **Route economics: complete.** Before each committable tick the allocator prices the
  designated funding pair through the cheapest gateway serving both ends and floors
  moves at that route's economic break-even, so it stops churning uneconomic
  sub-viable moves every tick.

Recovery of ECASH from the seed is done; the remaining durability work — encryption of
the seed at rest (kicked off in [ADR-0026](./docs/adr/0026-seed-at-rest-encryption-headless.md),
build deferred) and an encrypted app-state/history backup — is Phase 7. The Android
frontend (Phase 6b) and release hardening (Phase 8) are next. See
[docs/roadmap-to-v1.md](./docs/roadmap-to-v1.md).

## What is in this repo

- [wallet-core](./wallet-core/) - dependency-light pure logic: scoring, allocation,
  probe verdicts, ledger types, executor traits, and replay/idempotency behavior.
- [wallet-fedimint](./wallet-fedimint/) - Fedimint SDK integration: multi-federation
  clients, durable journal, executor, runtime, probe runner, move protocol, and
  operation ledger storage.
- [wallet-cli](./wallet-cli/) - the first-class frontend, a thin client of `walletd`
  by default (`--standalone` for a direct-DB one-shot). Joins federations,
  balance/listing, receive/pay/direct-inflow, cross-federation moves, evacuations
  through `tick`, active probes, seed recovery (`recover` / `restore-mnemonic`),
  reconciliation, and ledger inspection (`history` / `show`).
- [wallet-daemon](./wallet-daemon/) - `walletd`, the 24/7 daemon: an axum local API
  (127.0.0.1 + bearer token) over a single Runtime-owning actor, with the watch
  scheduler, per-operation IO driver tasks, and the settlement-stall self-heal watchdog.
- [wallet-api](./wallet-api/) - the wire DTOs and the runtime-mutable `Policy` struct
  shared between the daemon and its clients.
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
`10000` is rejected by policy validation. Before each committable tick, the allocator prices the
designated funding pair through the cheapest gateway serving both federations. Small moves wait
until their shortfall clears that route's economic floor; a cap below every serving route's
proportional fee blocks that pair and records an `uneconomic_route` refusal in
`wallet-cli history`. The perform-time cap remains the final money backstop if quotes change.

See [docs/real-sats-pilot-runbook.md](./docs/real-sats-pilot-runbook.md) for suggested
pilot values.

## Local development

The workspace is pinned to `douglaz/fedimint` at commit
`72b1e5beadc5a31a33ebc751764cb2f840a63b5e` (branch `wallet-pin/iroh-recovery-tpe8838`:
the iroh long-poll transport, a recovery-complete-or-fail cherry-pick, and the #8838
single-share TPE fix — see `wallet-fedimint/Cargo.toml`). The Fedimint native
dependencies are expected from the sibling Fedimint checkout's Nix environment:

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
