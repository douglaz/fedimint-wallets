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

## Update (2026-07): standby is best-effort diversification, not verified-independent insurance

[ADR-0010](./0010-warm-standby-selection.md) — which required the standby to be operator-INDEPENDENT
from the spending fed — was DROPPED: independence is unfeasible to verify in fedimint (guardian
consensus pubkeys are random per federation; the api-URL fallback is too weak). So the warm standby
here is now **best-effort diversification** across two distinct federations, NOT a verified-
independent sudden-death guarantee. It still reduces single-federation risk (two feds rarely die
together), and two random feds are usually different operators — but the wallet no longer *proves*
the standby is operator-independent, and must not claim that stronger guarantee in product copy. The
standby is selected by the remaining scorer signals (probe health, structural m-of-n strength, Lnv2,
untrusted prior). A real independence signal could return with a robust operator-identity source
(Phase 3); until then, keep the claim honest.
