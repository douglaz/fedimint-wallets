---
status: accepted
---
# Warm-standby selection: guardian-independence first, gateway-overlap as tiebreaker

When the Allocator picks the warm-standby federation (see
[ADR-0006](./0006-allocator-concentrated-warm-standby.md)), guardian/operator
**independence from the spending federation is a hard constraint** — a standby
that shares operators provides no sudden-death insurance. Among independent
candidates, prefer one that **shares a healthy gateway** with the spending
federation (enables the cheap internal swap for rebalancing), then break remaining
ties on resilience score.

## Consequences

- Independence is non-negotiable; cheap-swap convenience never overrides it.
- A shared gateway between two operator-independent federations is fine: a gateway
  failure is separate and recoverable (federations have multiple gateways).
- Requires knowing each federation's guardian set to compare overlap (available
  from the federation config / invite code).
