---
status: accepted
---
# v1 evacuation: hard low balance cap, not on-chain peg-out

> **Amended by [ADR-0029](./0029-evacuation-must-be-executable.md) (2026-08-01).** This ADR's balance cap and its acceptance of a stranded capped amount both STAND. What ADR-0029 adds is a second, still gateway-DEPENDENT route for evacuation (a Lightning hop between two gateways) and a workable fee cap — the absolute cap here either forces a full-balance evacuation to drain in ~27 chunks (low-base gateway) or refuses it entirely in a silent retry livelock (base fees above the cap), and could not fund a full-balance evacuation. The gateway-INdependent escape this ADR deferred to v2 (on-chain peg-out) remains deferred.

Approved resolution (autoplan final gate, 2026-06-28) for the LN-only-evacuation
stranding risk in [ADR-0004](./0004-v1-lightning-only.md). Rather than pull
on-chain peg-out into v1, v1 enforces a HARD, LOW per-federation balance cap and
surfaces stranded-funds state honestly in the UI. On-chain peg-out (the
gateway-independent escape) is pulled into EARLY v2.

## Consequences

- Caps loss, not probability: a federation + gateway correlated death can still
  strand a capped amount until/unless recovery. Acceptable because the cap is low
  and this is spending money.
- The cap must be ENFORCED (refuse or warn above threshold), not relied on as copy
  (CEO finding #3: "spending wallet only" will not constrain behavior on its own).
  **RESOLVED (2026-08-05): REFUSE, for wallet-controlled balance increases.** "Refuse or warn"
  left this ADR contradicting its own title ("HARD, LOW … cap") and its own "Caps loss"
  consequence, and it left ADR-0029's "The balance cap stays where it is" paragraph — which calls
  this cap "the real mitigation" for a correlated federation+gateway death — resting on something
  a user could click past. (Cited by section, not line: this tree's line refs drift.) A
  dismissible warning is not a cap. So: a wallet-controlled action that would carry a federation
  balance above the threshold is REFUSED, and the user-visible rejection is accepted as the cost.
  **Downsizing to fit satisfies this rule; refusal is only for when downsizing is impossible.**
  Evacuation sizing into a destination's remaining room (`clamp_desired_to_cap_room`,
  `wallet-fedimint/src/executor.rs:670-700`, kept by br-y2j 2c(a)) is the intended behaviour, not
  a violation — reading "would carry the balance above the threshold is REFUSED" strictly enough
  to forbid the clamp would strand a dying federation whenever its only destination sits near
  cap. Refusal applies where the amount is fixed and cannot be reduced: `Receive`,
  `DirectInflow`, and a fixed-amount topping-up `Move`. NOT `Pay` — a `Pay` spends a federation's
  balance OUTWARD (`wallet-daemon/src/handlers.rs:310-316`) and cannot carry any balance above
  the threshold, so there is nothing for this rule to refuse there. The topping-up `Move` is the
  one that matters and the one the code already refuses today: "every OTHER inflow (a
  DirectInflow or a topping-up Move) is refused pre-mint below if it would push the destination
  over the cap" (`wallet-fedimint/src/executor.rs:1303-1305`). Listing `Pay` and omitting `Move`
  would give textual authority to drop the real check and add a meaningless one.
  Warnings remain correct for balances that rise by means the wallet does not control (an
  inbound payment already in flight, a federation-side change), because there is nothing to
  refuse there — surface those and let evacuation handle them.
- The per-federation balance/data model must support the cap and the
  stranded-funds UI from v1.
