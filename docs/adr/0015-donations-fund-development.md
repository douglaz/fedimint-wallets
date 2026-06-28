---
status: accepted
supersedes: ADR-0008
---
# Open-source, donations fund development; operate no fund-path services

Supersedes [ADR-0008](./0008-open-source-donation-funded.md). The wallet is
open-source (AGPL, [ADR-0009](./0009-license-agpl.md)) and donation-funded, but
donations fund **development**, not server operations. We operate no fund-path
services: we do NOT run our own Lightning gateway (dropping ADR-0008's
secondary-revenue gateway); the device uses federations' existing public gateways.
Any recurringd we run is one-of-many ([ADR-0013](./0013-recurringd-one-of-many.md)),
not a business.

## Consequences

- Dissolves the "donation + recurring server burn = abandonment" problem the CEO
  review flagged: with no operated fund-path infra, there is little recurring cost
  to fund. Donations sustain development (the Bitcoin-Core / Phoenix model), not
  harbor's doomed one.
- Removes the gateway from our regulatory surface (a gateway intermediates value,
  the most money-transmission-flavored activity in the stack).
- Trade-off: reliability now leans on public/community gateways and recurringds we
  do not control, a real cost since reliability is the core promise.
