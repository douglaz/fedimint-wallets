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
  contradicted this ADR's own title and its "Caps loss" consequence, and left ADR-0029's reliance
  on this cap resting on something a user could click past. A dismissible warning is not a cap.
  Downsizing to fit satisfies the rule — evacuation sizing into a destination's remaining room is
  intended behaviour, not a violation; refusal is for amounts that cannot be reduced. Where the
  wallet does not control the inflow there is nothing to refuse: warn, and let evacuation handle
  it. That principle classifies every verb without an enumeration to keep in sync.
  This SUPERSEDES the `--allow-over-cap` contract in `docs/phase4-implementation-spec.md:760-779`,
  which still disables `hard_cap` and tests that the over-cap operation SUCCEEDS. That escape
  hatch is incompatible with this resolution: whoever implements the refusal must retire it and
  its test rather than leave two live contracts.
- The per-federation balance/data model must support the cap and the
  stranded-funds UI from v1.
