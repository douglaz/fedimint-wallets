# Simple Fedimint Wallet

A Rust Fedimint wallet project for a private, no-KYC, spending-focused ecash wallet:
Wallet-of-Satoshi-simple on the surface, with an on-device multi-federation
Allocator underneath.

This repo is currently the headless engine, the 24/7 `walletd` daemon, and the CLI.
The Android Slint app is still planned, not built.

## Current status

The system as built is described in [docs/spec/](./docs/spec/README.md); start at its
[executive summary](./docs/spec/executive-summary.md). In one line: the headless engine, the
`walletd` daemon, discovery, seed recovery and route economics are built and devimint-validated;
the web sidecar is a skeleton; there is no phone app; the seed is plaintext on disk. What is next
is in [docs/roadmap-to-v1.md](./docs/roadmap-to-v1.md).

The one long-running deployment is a test rig holding a small real-sats balance, not a pilot.

## What is in this repo

- [wallet-core](./wallet-core/) - dependency-light pure logic: scoring, allocation,
  probe verdicts, ledger types, executor traits, and replay/idempotency behavior.
- [wallet-fedimint](./wallet-fedimint/) - Fedimint SDK integration: multi-federation
  clients, durable journal, executor, runtime, probe runner, move protocol, and
  operation ledger storage.
- [wallet-cli](./wallet-cli/) - the first-class frontend, a thin client of `walletd`
  by default (`--standalone` for a direct-DB one-shot). Joins federations,
  balance/listing, receive/pay/direct-inflow, cross-federation moves, evacuations
  through `tick`, active probes, seed recovery (`recover`),
  reconciliation, and ledger inspection (`history` / `show`).
- [wallet-daemon](./wallet-daemon/) - `walletd`, the 24/7 daemon: an axum local API
  (127.0.0.1 + bearer token) over a single Runtime-owning actor, with the watch
  scheduler, per-operation IO driver tasks, the `restore-mnemonic` recovery command, and the
  settlement-stall self-heal watchdog.
- [wallet-web](./wallet-web/) - the localhost browser sidecar (ADR-0028): a client of
  `walletd` over HTTP, exactly like `wallet-cli`, that renders HTML instead of terminal
  output. Currently the crate skeleton, `wallet-web init` provisioning, and a fail-closed
  startup; the served surfaces land in later Phase 6c beads.
- [wallet-api](./wallet-api/) - the wire DTOs and the runtime-mutable `Policy` struct
  shared between the daemon and its clients.
- [docs/](./docs/) - the build plans, runbooks, ADRs, review notes, and specs.
- [SIMPLE-FEDIMINT-WALLET-REPORT.md](./docs/archive/SIMPLE-FEDIMINT-WALLET-REPORT.md) - the
  original wallet survey and product design report. It is useful background, but the
  ADRs and roadmap supersede it where they differ.

## Allocator policy

The standing instructions the Allocator runs against live in one stored `Policy`, edited
field-by-field with `wallet-cli policy set` and printed by `wallet-cli policy get`. The
balance knobs are `--per-fed-cap`, `--spending-target`, and `--standby-target` (all msat);
the two fee caps are deliberately different shapes:

- `--max-fee` - ABSOLUTE fee cap in msat (a flat ceiling, not scaled by the amount). It bounds
  NO move the Allocator emits: funding moves use `--max-fee-bps-of-move` and evacuations use the
  `--evac-fee-*` pair below. **In an evacuation incident this is not the knob to turn.** It is
  still the default `--fee-cap` for the manual `pay`/`move`/`receive`/`direct-inflow`
  commands, so setting it very low refuses those too.
- `--evac-fee-base-msat` + `--evac-fee-bps` - the EVACUATION cap, `base + floor(delivered *
  bps / 10_000)`, default 200 sats + 3%. The base is msat; the second flag is BASIS POINTS,
  `0`-`10000` — `300` is 3%, not 300 msat, and entering a bps value as msat silently widens the
  cap by orders of magnitude. Zero bps is accepted and means a base-only cap, valid only
  alongside a non-zero base. It is computed from the net the destination is actually CREDITED,
  not the amount asked for. An admitted evacuation normally keeps its cap pair. The narrow
  exception is an Agent evacuation that is still pre-artifact and carries durable typed evidence
  of a structural refusal: a component-wise monotone cap edit that is effectively larger at a
  recorded delivered-net sample lets the daemon atomically retire it and admit a linked successor
  under the new pair. Standalone recovery requires a tick occurrence newer than the marked
  evacuation; ordinary, equal, decreased, or crossed cap edits do not release it.
- `--max-fee-bps-of-move` - PROPORTIONAL fee cap for funding moves (top-up and standby), in
  basis points of the amount moved, `1`-`10000`; default `300` (3%). Funding sizing reserves
  it from the source, so `amount + amount * bps / 10000` always fits the source budget and a
  positive surplus is never refused for being smaller than a flat cap.

A `--max-fee-bps-of-move` of `0` (every funding move would get a zero cap and fail) or above
`10000` is rejected by policy validation. Before each committable tick, if live allocator work has
not conflict-blocked the designated funding pair, the allocator attempts to price it. Absent an
explicit gateway override, candidates come from the destination federation's vetted list; an
override is the sole candidate. Pricing validates `routing_info` at both ends and picks the
cheapest validated candidate, but source-side vetted-list membership is not yet enforced
(`br-s0e`). A pair priced `Routable` waits until its shortfall clears that route's economic
floor or the protocol `min_move` floor, whichever is greater. `Unroutable` blocks the move;
`UneconomicAtAnySize` also records an
`uneconomic_route` refusal in `wallet-cli history`. An absent candidate list, bounded-scan miss,
quote error, or indeterminate floor can instead leave the pair unpriced, in which case allocation
permissively falls back to the protocol `min_move` floor. The perform-time cap remains the final
money backstop if quotes change.

See [docs/real-sats-pilot-runbook.md](./docs/real-sats-pilot-runbook.md) for suggested
pilot values.

## Local development

The workspace is pinned to `douglaz/fedimint` at commit
`72b1e5beadc5a31a33ebc751764cb2f840a63b5e` (branch `wallet-pin/iroh-recovery-tpe8838`:
the iroh long-poll transport, a recovery-complete-or-fail cherry-pick, and the #8838
single-share TPE fix — see `wallet-fedimint/Cargo.toml`). The Fedimint native
dependencies come from this repository's own Nix devshell. Run these commands
from the repository root:

```bash
env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR nix develop -c \
  cargo build --locked --target-dir "$PWD/target-nix" --workspace
env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR nix develop -c \
  cargo test --locked --target-dir "$PWD/target-nix" --workspace
env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR nix develop -c \
  cargo clippy --locked --target-dir "$PWD/target-nix" --workspace --all-targets -- -D warnings
# The 24-hour soak defaults to release wallet binaries:
env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR nix develop -c \
  cargo build --release --locked --target-dir "$PWD/target-nix" -p wallet-daemon -p wallet-cli
```

These are ordinary local-development commands, not certifying live-validation recipes:
they clear both Cargo target-directory environment variables before entering the devshell,
then use an explicit target flag. The exact live runbook and copied smoke headers use a
fixed Nix child-environment allowlist, rebuild `target-nix` clean before each wallet build,
and use a fresh temporary Cargo source home for every certifying Cargo invocation. This prevents
reuse of binaries from an ambiently overridden build or mutable Cargo Git source checkouts.

Live money-path validation uses devimint and the smoke scripts under
[wallet-cli/tests/](./wallet-cli/tests/). Start with
[docs/devimint-runbook.md](./docs/devimint-runbook.md) for the two-federation setup,
gateway pinning details, and known gotchas.

## Design docs

- [docs/spec/](./docs/spec/README.md) - the as-built specification: what the code does today, with
  stable requirement identifiers, known defects, conformance evidence, and open findings. Start at
  its executive summary. It supersedes the status list above and the plans below as the
  description of the system.
- [CONTEXT.md](./CONTEXT.md) - canonical product language and domain definitions.
- [docs/roadmap-to-v1.md](./docs/roadmap-to-v1.md) - current build sequence and
  definition of "fully featured v1".
- [docs/phase6c-web-frontend-plan.md](./docs/phase6c-web-frontend-plan.md) - the browser
  frontend, specced and next to build.
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
