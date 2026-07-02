---
status: accepted
---
# Warm-standby selection: guardian-independence first, gateway-overlap as tiebreaker

When the Allocator picks the warm-standby federation (see
[ADR-0006](./0006-allocator-concentrated-warm-standby.md)), guardian/operator
**independence from the spending federation is a hard constraint** — a standby
that shares operators provides no sudden-death insurance. Among independent
candidates, prefer one that **shares a healthy gateway** with the spending
federation (enables the cheap internal swap for rebalancing), then break remaining
ties on resilience score.

## Consequences

- Independence is non-negotiable; cheap-swap convenience never overrides it.
- A shared gateway between two operator-independent federations is fine: a gateway
  failure is separate and recoverable (federations have multiple gateways).
- Requires knowing each federation's guardian set to compare overlap (available
  from the federation config / invite code).

## Guardian-identity source (Phase-2 finding, 2026-07)

The overlap check compares a stable per-guardian identity across feds. The obvious anchor — the
guardian **consensus pubkey** — does NOT work in fedimint: `broadcast_public_keys` are freshly
RANDOM per federation (generated at every config-gen ceremony; the federation id itself derives
from them), so two feds run by the SAME operator never share pubkey bytes, and a pubkey-based
check would ALWAYS read independent and **fail OPEN**. The authenticated client config carries no
cross-federation-stable guardian pubkey.

**Decision:** source `GuardianId` from the guardian's advertised **api-endpoint URL** (from the
config's `global.api_endpoints`) — the only cross-fed-stable shared-operator signal the config
exposes, and always non-empty for a joined fed (so the producer never emits an empty guardian set,
which would silently defeat the check). A self-hosted operator reusing an endpoint across its feds
is correctly detected as shared.

**Known gap:** one operator advertising DIFFERENT hosts per fed still reads as independent. This
weakens the sudden-death insurance against a sophisticated shared operator, but the URL fails in
the SAFE direction for the common self-hosted case. A robust stable-identity source (e.g. a signed
operator identity, or a discovery/Observer-provided operator map) is deferred to Phase 3. See
`wallet-fedimint::probe` and the `GuardianId` doc.
