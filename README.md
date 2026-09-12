# Simple Fedimint Wallet

A Rust Fedimint wallet project for a private, no-KYC, spending-focused ecash wallet:
Wallet-of-Satoshi-simple on the surface, with an on-device multi-federation
Allocator underneath.

This repo is currently the headless engine, the 24/7 `walletd` daemon, and the CLI.
The Android Slint app is still planned, not built.

## Current status

The system as built is described in the specification repository,
[douglaz/fedimint-wallets-spec](https://github.com/douglaz/fedimint-wallets-spec); start at its
[executive summary](https://github.com/douglaz/fedimint-wallets-spec/blob/main/executive-summary.md).
What that specification says and the code does not yet do is
[docs/open-findings.md](./docs/open-findings.md), kept here beside the code. In one line: the headless engine, the
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
field-by-field with `wallet-cli policy set` and printed by `wallet-cli policy get`. The rules
are owned by [`06-allocator-and-automation.md`](https://github.com/douglaz/fedimint-wallets-spec/blob/main/06-allocator-and-automation.md) in the specification repository;
this section only maps the flags to them.

| Flag | What it bounds | Rule |
|---|---|---|
| `--per-fed-cap`, `--spending-target`, `--standby-target` (msat) | balances | `ALC-2`, `ALC-9` |
| `--max-fee` (msat, absolute) | the manual `pay`/`move`/`receive`/`direct-inflow` default `--fee-cap`, **and** the per-leg fee cap of every daemon-scheduled active probe (`PolicyExt::probe_policy`); **no allocator-emitted move**, so it is not the knob in an evacuation incident | `ALC-3`, `ALC-25`, `DEF-1`, `DEF-21` |
| `--max-fee-bps-of-move` (basis points, `1`–`10000`, default 300) | funding moves, proportional; sizing reserves `amount + cap` from the source | `ALC-7`, `ALC-8` |
| `--evac-fee-base-msat` + `--evac-fee-bps` (msat + basis points, default 200 sats + 3%) | evacuations, `base + floor(delivered × bps / 10 000)`, computed from what the destination is credited | `ALC-20`–`ALC-22`, `OPS-25` |

`300` bps is 3%, not 300 msat; a bps value entered as msat silently widens the cap by orders of
magnitude. A qualifying raise of the evacuation pair can release a structurally refused
evacuation into a linked successor (`OPS-30`). Route pricing, the per-pair economic floor and the
refusal rows it writes are `ALC-10`–`ALC-13`; automated routing resolves from the vetted list
and is never pinned (`ADR-0030`, `F6` for what is not yet enforced).

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
registering the LDK gateway with the guardians, and known gotchas.

## Design docs

- [douglaz/fedimint-wallets-spec](https://github.com/douglaz/fedimint-wallets-spec) - the
  as-built specification: what the code does today, with stable requirement identifiers, known
  defects, conformance evidence, the ADRs and the glossary (`CONTEXT.md`). Start at its executive
  summary. It supersedes the status list above and the plans below as the description of the
  system. It moved out of this repository on 2026-09-12 so that a specification change and a
  code change are two pull requests against two gates.
- [docs/open-findings.md](./docs/open-findings.md) - what the specification says and the code
  does not yet do, one finding per tracked issue. Stays here because a finding is about this tree.
- [docs/roadmap-to-v1.md](./docs/roadmap-to-v1.md) - current build sequence and
  definition of "fully featured v1".
- [docs/phase6c-web-frontend-plan.md](./docs/phase6c-web-frontend-plan.md) - the browser
  frontend, specced and next to build.
- [docs/operation-history-spec.md](./docs/operation-history-spec.md) - append-only
  ledger semantics.
- [docs/adr/](https://github.com/douglaz/fedimint-wallets-spec/tree/main/docs/adr) in the
  specification repository - architecture decisions. These are canonical when they
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
[ADR-0009](https://github.com/douglaz/fedimint-wallets-spec/blob/main/docs/adr/0009-license-agpl.md).
