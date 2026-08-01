---
status: accepted
---
# Evacuation must be executable: a proportional fee cap, and a second route when no gateway is shared

Two changes so that draining a dying federation happens in ONE operation over a route that exists,
rather than in ~27 fee-capped chunks or not at all when no gateway is shared:

1. **`Evacuate`'s fee cap becomes base + proportional** — an absolute allowance plus a percentage
   of the amount (starting point: **200 sats + 3%**), replacing today's single absolute
   `snapshot.max_fee`.
2. **`Evacuate` gains a second route**: when no gateway serves both federations, it may pay B's
   invoice from A over real Lightning through two different gateways, instead of refusing (routing
   IS a genuine refusal today, unlike the fee cap). This is
   a **best-effort fallback, tried only after the shared-gateway swap**, and it is explicitly NOT a
   guarantee.

`Move` is unchanged: it keeps the swap-only path and its proportional cap. Routine rebalancing can
safely decline and wait; evacuation cannot.

## Why

**The absolute cap cannot fund a real evacuation, and it fails in TWO different ways depending on
the gateway's fee shape.** Neither is what an earlier draft of this ADR claimed; both were
established by reading the code rather than reasoning from the parameter.

`size_fresh_evacuation` (`executor.rs:606-667`) searches for the largest net that satisfies
`total_within_cap` — the sum of BOTH legs' quotes, each including the gateway's BASE fee, against
`fee_cap` (`fee.rs:160-164`, `executor.rs:814-829`). What happens next depends entirely on whether
any candidate clears that predicate:

1. **Base fees below the cap → CHUNK-DRAIN.** A smaller net fits, so the evacuation is silently
   downsized (`executor.rs:654-663` warns "reducing fresh evacuation amount"). The allocation
   occurrence advances every watch cycle (`scheduler.rs:692-697`) and `idem_evac` embeds it
   (`allocator.rs:629`), so a fresh `Evacuate` is emitted for the remainder. At the figures below
   the federation drains in roughly 27 operations at essentially the same total fee. This is what
   the pilot's MEASURED gateway does.
2. **Base fees at or above the cap → GENUINE REFUSAL.** The base component does not shrink with
   the amount, so if the two legs' bases alone exceed `fee_cap`, NO candidate ever fits and the
   executor returns `Retryable` (`executor.rs:646-653`) on every tick — a livelock, not a terminal
   failure, so it retries silently forever. A gateway is permitted bases summing to ~150 sats
   against the runbook's 50-sat cap, so this is not hypothetical.

Both defeat the intent. `allocator.rs` passes `fee_cap: snapshot.max_fee` for `Evacuate` directly
beneath a comment stating that "route economics NEVER gates an evacuation — a dying federation must
be drained even when the route prices badly". The parameter contradicts the intent. At the
runbook's own suggested settings, using fees measured on the pilot gateway:

| | |
|---|---|
| per-federation cap | 75,000,000 msat |
| measured swap cost at 1,000,000 msat | 17,848 msat (1.78%) |
| same rate on a full federation | ~1,338,600 msat |
| `--max-fee` absolute cap | 50,000 msat |
| | **~27× over — chunk-drain on this gateway; refusal on a higher-base one** |

No evacuation has ever run in production, so neither failure mode has been observed with real
funds.

**A flat percentage would fail at the other end.** A gateway's INTENDED fee envelope is
`SEND_FEE_LIMIT` (100 sats + 1.5%) plus `RECEIVE_FEE_LIMIT` (50 sats + 0.5%) — about 150 sats + 2%.
Treat that as a design intent, NOT an enforced bound: at our pinned SDK revision the limits do not
actually constrain a gateway, because `PaymentFee` derives a lexicographic `PartialOrd` over
(`base`, `parts_per_million`) and the check is a single `.le(...)`, so a fee with a small base and
an arbitrarily large ppm passes. (This is the upstream defect this project reported separately; our
own cap is what really bounds us, which is another reason it must be shaped correctly.) The base
component dominates at small amounts: a legitimate worst-case fee is 2.2% of a
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
- **The cap numbers are a starting point, not a derivation.** 200 sats + 3% covers the gateway's INTENDED
  envelope (~150 sats + 2%) with headroom at every size. That envelope is not enforced at our pin
  (see above), so this is not a proven bound on the wallet's total cost either: federation receive/send fees and mint-note fees also apply, and those are not bounded
  here. Treat the constants as a pilot starting point, not a worst-case derivation. They should be
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
