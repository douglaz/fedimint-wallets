---
status: accepted
---
# Federation consensus/module upgrades: unhandled, accepted for the short pilot

## Context

The wallet reads a federation's config once at join/open and does not track or react
to changes in its `consensus_version` or module versions. Nothing detects or handles a
federation that **upgrades** its modules or consensus while the wallet holds a client
for it. `FederationFacts` carries gateway-availability but not `consensus_version` —
the field was planned (`docs/integration-phase-plan.md`) but never added. This is an
acknowledged-open item, not silently done (codex + fable flagged it).

A federation that upgrades its modules to a version the pinned fedimint client cannot
speak could break that client: balance reads, sends, receives, or recovery for that
federation could fail until the client is upgraded to a compatible version.

## Decision

**Accept this gap for the capped, short-lived pilot; DEFER handling to a follow-up bead
before any long-lived deployment.** Rationale:

- The pilot is short and capped (a willing-to-lose ceiling; see
  [ADR-0018](./0018-v1-evacuation-balance-cap.md) and the real-sats pilot runbook), over
  ~2 hand-picked federations the operator controls or trusts
  ([ADR-0006](./0006-allocator-concentrated-warm-standby.md)). Module upgrades are
  infrequent and operator-visible in that setting.
- Detecting and gracefully handling arbitrary module/consensus upgrades (version
  negotiation, client-upgrade orchestration, evacuation on incompatibility) is a
  substantial feature a capped short pilot does not need to ship.

## Operator guidance (interim)

- Watch the pinned federations for announced module/consensus upgrades. If a pilot
  federation upgrades to a version this build's pinned fedimint client does not support,
  treat it as an incompatibility: pause automated allocation into that federation and, if
  needed, evacuate to a compatible one, then upgrade the wallet's fedimint pin.
- A hard failure surfaces as errors on that federation's operations (balance / send /
  receive / recover), **not** as fund loss — the ecash is still recoverable from the seed
  once a compatible client is restored.

## Consequences

- Until handled, an incompatible federation upgrade degrades that federation's usability
  (operator-recoverable), and the wallet has no automatic detection or self-heal for it.
- A follow-up bead should add `consensus_version` to `FederationFacts`, a compatibility
  check at open/probe time, and a policy for incompatible federations (probe-gate or
  evacuate) — sequenced before any long-lived, uncapped deployment.
