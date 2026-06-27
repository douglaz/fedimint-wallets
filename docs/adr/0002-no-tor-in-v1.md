---
status: accepted
---
# No Tor in v1; prioritize reliability, rely on iroh relays

A privacy-branded wallet would conventionally route over Tor. We are NOT using
Tor in v1 for two reasons: (1) Tor's latency and flakiness conflict with the
wallet's core promise of reliable spending and the Allocator's need for timely
probes and payments (see [ADR-0001](./0001-allocator-purpose-resilience-not-solvency.md));
and (2) current Fedimint and Lightning-gateway versions connect over **iroh**,
whose connections frequently traverse a neutral relay, so the federation/gateway
typically sees the relay's IP rather than the user's, giving partial IP hiding
for free.

## Consequences

- Network-level anonymity is NOT a v1 promise. The privacy claim is no-KYC +
  provider-blind ecash (see CONTEXT "Private"), not "nobody can see your traffic."
- iroh-relay IP hiding is incidental, not guaranteed, and must not be marketed as
  a deliberate anonymity feature.
- Tor (or similar) may be reconsidered later as an opt-in, weighed against
  reliability.
