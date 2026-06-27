---
status: accepted
---
# v1 is Lightning-only; on-chain deferred

v1 supports only Lightning for user-facing payments: send via BOLT11 / Lightning
Address / LNURL-pay, receive via invoice / Lightning Address. There is no
user-facing on-chain send or receive (peg-in / peg-out) in v1; explicit on-chain
steps are added later. This keeps the wallet WoS-simple and focuses the build.

## Consequences

- Funding is Lightning-only at first. A user whose money is on-chain (some
  exchange withdrawals, cold storage) cannot fund the wallet until on-chain
  receive ships. Accepted for v1.
- The Evacuation ladder is Lightning-only in v1 (shared-gateway swap, then
  public-Lightning). It does NOT have the on-chain peg-out rung yet, so it has no
  gateway-independent escape: a federation that dies together with its gateway
  can strand a (small) balance. This is mitigated by acting EARLY on
  **Shutdown notices** — evacuating over Lightning while the gateway still works —
  and is acceptable because balances are small spending money (see
  [ADR-0001](./0001-allocator-purpose-resilience-not-solvency.md)). On-chain
  peg-out is added with explicit on-chain support.
