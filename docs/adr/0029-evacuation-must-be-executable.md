---
status: accepted
---
# Evacuation must be executable: a proportional fee cap, and a second route when no gateway is shared

Two changes so that draining a dying federation can actually happen, rather than being refused on
price or on routing:

1. **`Evacuate`'s fee cap becomes base + proportional** — an absolute allowance plus a percentage
   of the amount (starting point: **200 sats + 3%**), replacing today's single absolute
   `snapshot.max_fee`.
2. **`Evacuate` gains a second route**: when no gateway serves both federations, it may pay B's
   invoice from A over real Lightning through two different gateways, instead of refusing. This is
   a **best-effort fallback, tried only after the shared-gateway swap**, and it is explicitly NOT a
   guarantee.

`Move` is unchanged: it keeps the swap-only path and its proportional cap. Routine rebalancing can
safely decline and wait; evacuation cannot.

## Why

**The absolute cap cannot fund a real evacuation.** `allocator.rs` passes `fee_cap:
snapshot.max_fee` for `Evacuate` directly beneath a comment stating that "route economics NEVER
gates an evacuation — a dying federation must be drained even when the route prices badly". The
parameter contradicts the intent. At the runbook's own suggested settings, using fees measured on
the pilot gateway:

| | |
|---|---|
| per-federation cap | 75,000,000 msat |
| measured swap cost at 1,000,000 msat | 17,848 msat (1.78%) |
| same rate on a full federation | ~1,338,600 msat |
| `--max-fee` absolute cap | 50,000 msat |
| | **~27× over** |

So a full-balance evacuation would be refused on price today, by the existing route, before any
routing question arises. No evacuation has ever run in production, so this was never observed.

**A flat percentage would fail at the other end.** The protocol permits a gateway to charge up to
`SEND_FEE_LIMIT` (100 sats + 1.5%) plus `RECEIVE_FEE_LIMIT` (50 sats + 0.5%) — about 150 sats + 2%.
The base component dominates at small amounts: a legitimate worst-case fee is 2.2% of a
75,000-sat evacuation but 17% of a 1,000-sat one. A pure percentage refuses exactly the
evacuations that are cheapest in absolute terms, which is the same defect mirrored. Base +
proportional tracks the real cost at every size, which is why the fees themselves are shaped that
way.

**Relationship to [ADR-0018](./0018-v1-evacuation-balance-cap.md), which decided the adjacent
question the other way.** ADR-0018 chose a hard low balance cap *instead of* an escape hatch,
accepting that a dying federation may "strand a capped amount until/unless recovery", with a
gateway-independent escape "pulled into EARLY v2". This ADR does not overturn that. What ADR-0018
deferred was **gateway-independent** escape — on-chain peg-out. A two-gateway Lightning hop is
still gateway-*dependent*: it relaxes "one gateway serves both federations" to "each federation has
some gateway". That is a genuinely weaker requirement, it is Lightning-only so
[ADR-0004](./0004-v1-lightning-only.md) still holds, and it appears not to have been weighed.

**The balance cap stays where it is.** The fallback still fails if the source federation's own
gateway is down, or if no gateway on either side has liquidity, so it reduces the probability of
stranding without making evacuation reliable. Treating it as reliable would justify raising the
per-federation cap, and that is the change that could actually lose money if the assumption proves
optimistic. ADR-0018's low cap remains the real mitigation.

## Consequences

- **`Evacuate` and `Move` no longer share a fee-cap shape.** A reader comparing them will find
  `Move` proportional-only and `Evacuate` base + proportional; that asymmetry is deliberate and
  exists because only one of them must succeed.
- **The cap numbers are a starting point, not a derivation.** 200 sats + 3% covers the
  protocol-permitted worst case (~150 sats + 2%) with headroom at every size. They should be
  revisited if the per-federation cap changes or if measured gateway fees move.
- **The fallback costs more.** Measured on the pilot gateway, the external send leg was 16,064
  msat against 8,948 for the swap on the same 1,000,000 msat — roughly 80% dearer, because the
  lnv2 internal-swap discount (`send_fee_minimum`) does not apply across two gateways. That is
  accepted: it only applies when the alternative is not moving at all.
- **The fallback adds no new trust.** Both legs remain hash-locked, and the destination client
  derives the preimage, so a gateway on either side still cannot take funds without delivering.
  See [ADR-0018](./0018-v1-evacuation-balance-cap.md) and the `Stranded` analysis for what remains
  reachable — a Byzantine destination federation, which is the standard custodial assumption and
  is unchanged by route count.
- **Route ordering is strict.** The swap is always tried first because it is cheaper; the hop is
  reached only when no gateway serves both ends. No reputation or liquidity bar gates the
  fallback: during an evacuation a worse counterparty beats stranding the balance, and every extra
  bar is another way the escape hatch fails to open.
- **Neither change makes evacuation reliable**, and no document should claim it does. The honest
  statement remains: evacuation is best-effort, bounded by the balance cap, and dependent on at
  least one gateway being reachable on each side.
