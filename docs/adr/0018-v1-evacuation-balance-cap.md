---
status: accepted
---
# v1 evacuation: hard low balance cap, not on-chain peg-out

> **Amended by [ADR-0029](./0029-evacuation-must-be-executable.md) (2026-08-01).** This ADR's balance cap and its acceptance of a stranded capped amount both STAND. What ADR-0029 adds is a second, still gateway-DEPENDENT route for evacuation (a Lightning hop between two gateways) and a workable fee cap — the absolute cap here could not fund a full-balance evacuation. The gateway-INdependent escape this ADR deferred to v2 (on-chain peg-out) remains deferred.

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
- The per-federation balance/data model must support the cap and the
  stranded-funds UI from v1.
