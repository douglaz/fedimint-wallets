# Fedimint Wallet Analysis & Simple-Wallet Design

Engineering analysis of existing Fedimint/ecash wallets, and a design report for a new one.

## Contents

- **[SIMPLE-FEDIMINT-WALLET-REPORT.md](./SIMPLE-FEDIMINT-WALLET-REPORT.md)** — the report.
  Deep-dives of four existing wallets, plus a design for a new **pure-Rust, Android-only, Slint**
  wallet: Wallet-of-Satoshi-simple on the surface, with an automated multi-federation risk engine
  underneath. A CEO-mode plan review (with an independent second-model challenge) re-sequenced the
  build to **foundation-first** — ship a single-federation wallet architected for the engine, turn
  the engine on in v2 behind a fee-vs-risk EV gate. See §0.5 and the `GSTACK REVIEW REPORT` section.

## Wallets analyzed (not vendored here — gitignored)

These are upstream projects, cloned locally only for analysis:

| Wallet | Stack | Source |
|--------|-------|--------|
| ecash-app | Flutter + Rust (FRB), Android | https://github.com/fedimint/ecash-app |
| harbor | pure Rust (iced), desktop | https://github.com/HarborWallet/harbor |
| vipr-wallet | Vue 3 + TS PWA | https://github.com/ngutech21/vipr-wallet |
| Fedi | Rust core + React Native/PWA | the Fedi monorepo |
