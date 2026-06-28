---
status: accepted
---
# Sybil-resistant federation selection: probes gate, reputation only demotes

Resolves the contradiction both eng reviews caught between
[ADR-0014](./0014-on-device-agent-standing-instruction.md) ("no curated
allowlist") and [ADR-0016](./0016-device-side-federation-selection.md)
("trust-weight public reputation," which needs a Sybil-resistant trust root, i.e.
a curator). Approved resolution (autoplan final gate, 2026-06-28):

- **Empirical probes GATE.** A federation cannot receive funds until it has
  round-tripped a real probe payment for THIS device over a sustained window.
  Probes are the primary signal; this defeats fresh-key Sybils, since sparse
  Nostr data cannot promote a federation on its own.
- **Public reputation can only DEMOTE, never promote** above the probe floor.
- **Low absolute per-federation exposure cap**, regardless of score (see
  [ADR-0018](./0018-v1-evacuation-balance-cap.md)).
- **User-editable trust-anchor set**: a default set may be bundled, but it is
  replaceable by the user, so it is not an operator-controlled allowlist.

## Consequences

- Probe-primary fixes the cold-start problem (a tiny Fedimint ecosystem has almost
  no review data; reputation is garnish at launch).
- Defangs the money-steering attack ADR-0016 named: a Sybil'd reputation cannot
  move funds, only demote a federation.
- Honesty caveat: if a default anchor set ships, that is a (replaceable) curated
  input, so ADR-0014's regulatory framing must be revisited with counsel before
  v2. **This blocks v2.**
