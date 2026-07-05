# Fedimint Wallet Analysis & Simple-Wallet Design

Engineering analysis of existing Fedimint/ecash wallets, and a design report for a new one.

## Contents

- **[SIMPLE-FEDIMINT-WALLET-REPORT.md](./SIMPLE-FEDIMINT-WALLET-REPORT.md)** — the report.
  Deep-dives of four existing wallets, plus a design for a new **pure-Rust, Android-only, Slint**
  wallet: Wallet-of-Satoshi-simple on the surface, with an automated multi-federation risk engine
  underneath. A CEO-mode plan review (with an independent second-model challenge) re-sequenced the
  SHIP plan to **foundation-first** — a single-federation wallet architected for the engine, the
  engine on behind a fee-vs-risk EV gate. See §0.5 and the `GSTACK REVIEW REPORT` section.
  The BUILD order has since evolved (the headless engine was built first: Phases 1–2 complete
  — money path, sense+decide — plus Phase 3.A evacuation execution); current build state and
  the path to a full product live in
  **[docs/roadmap-to-v1.md](./docs/roadmap-to-v1.md)** — the ship-configuration decision stays
  gated as the report describes.

## Wallets analyzed (not vendored here — gitignored)

These are upstream projects, cloned locally only for analysis:

| Wallet | Stack | Source |
|--------|-------|--------|
| ecash-app | Flutter + Rust (FRB), Android | https://github.com/fedimint/ecash-app |
| harbor | pure Rust (iced), desktop | https://github.com/HarborWallet/harbor |
| vipr-wallet | Vue 3 + TS PWA | https://github.com/ngutech21/vipr-wallet |
| Fedi | Rust core + React Native/PWA | the Fedi monorepo |

## Design docs

- [CONTEXT.md](./CONTEXT.md) — domain glossary for the new wallet.
- [docs/adr/](./docs/adr/) — architecture decision records (resilience allocator,
  recovery, recurringd, Allocator strategy, licensing, etc.).

## License

AGPL-3.0 — see [LICENSE](./LICENSE) and
[ADR-0009](./docs/adr/0009-license-agpl.md).
