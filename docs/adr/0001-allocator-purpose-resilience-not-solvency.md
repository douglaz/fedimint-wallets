---
status: accepted
---
# Allocator purpose: resilience, not solvency hedging

The wallet spreads spending balance across multiple Fedimint federations.
Because federations are treated as ephemeral (a spending tool, not savings) and
federation *solvency* cannot be measured from the client, the allocator's
purpose is **availability / resilience** — keep the user able to spend when a
federation degrades, goes offline, or disappears — not diversification against a
federation being insolvent.

## Consequences

- Probes (liveness, redemption round-trips, gateway availability) measure
  *usability*, which is observable. They are NOT claimed to detect insolvency,
  which is not observable from the client.
- A federation that stays liquid and then rugs its on-chain reserve is
  explicitly out of scope. The only mitigation is "keep balances small," which
  is consistent with this being a spending (not savings) wallet.
- No marketing or UX may imply protection against a federation stealing or
  losing the backing bitcoin.
