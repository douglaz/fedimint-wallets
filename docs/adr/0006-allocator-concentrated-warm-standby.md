---
status: accepted
---
# Allocator strategy: concentrated + warm standby, event-driven evacuation

The Allocator does NOT proactively diversify across many federations (that pays
swap fees to move funds that were fine — the EV problem the review flagged).
Instead it keeps most funds in the **spending federation** (concentrated, cheap),
maintains a *small* **warm standby** balance in one other vetted federation so a
sudden, notice-less federation death never fully strands the user, and evacuates
primarily on **Shutdown notices** over Lightning while gateways still work (see
[ADR-0001](./0001-allocator-purpose-resilience-not-solvency.md),
[ADR-0004](./0004-v1-lightning-only.md)).

## Consequences

- Fee cost is bounded: one small standby top-up plus event-driven evacuations,
  not constant rebalancing. This is the bounded answer to the "is the engine
  net-negative on fees?" gate.
- The mental model stays at ~two federations (spending + standby), which keeps the
  unified-balance UX honest and the fragmentation problem small.
- Sudden death is only half-covered: the standby keeps the user spending, but
  funds in the dead federation are stuck until/unless it recovers. Accepted
  because balances are small spending money.
- Preemptively funding the standby costs a swap fee (moving money that was fine);
  accepted as the price of sudden-death protection.
