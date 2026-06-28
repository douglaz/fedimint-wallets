---
status: accepted
---
# Device-side federation selection from trust-weighted public reputation + probes

The device decides which federations to use, keep, or avoid from PUBLIC data
(Nostr reviews/recommendations, federation metadata, Fedimint Observer) combined
with its own empirical probes (liveness, redemption round-trips, gateway
availability). There is no allowlist we curate or publish (see
[ADR-0014](./0014-on-device-agent-standing-instruction.md)); the open-source
selection logic runs on-device over public inputs.

## Consequences

- Public reputation is Sybil-able: naive review-counting is a fund-steering attack
  vector. Selection MUST trust-weight (web-of-trust / a seed set of known reputable
  community keys / signed attestations) and weight empirical probes (a federation
  that actually round-trips a payment) above unverified reviews.
- Cold-start: the Fedimint ecosystem is small, so review data is thin early. With
  few signals, be conservative (prefer well-established federations, smaller
  exposure), never "no reviews, looks fine."
- The selection algorithm being open-source is a feature: auditable, on-device, no
  operator in the loop.
- Supersedes the "curated allowlist" idea from ADR-0007; in
  [ADR-0010](./0010-warm-standby-selection.md) read "publicly-scored federations"
  for "curated allowlist."
- Grounded against real public data in
  [../federation-data-sources-spec.md](../federation-data-sources-spec.md) and refined
  by [ADR-0019](./0019-federation-signals-trust-model.md): Nostr reputation is a weak
  prior only; probes + the authenticated config are the trust.
