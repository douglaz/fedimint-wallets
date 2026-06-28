---
status: accepted
supersedes: ADR-0007
---
# Auto-allocation is an on-device agent on a standing instruction; no operator, no curated allowlist

Supersedes [ADR-0007](./0007-auto-allocate-disclose-not-consent.md) ("disclose,
not consent"). The wallet auto-manages funds across federations, framed correctly:
it is open-source software the user chose to run on their own device, the project
never holds the user's keys or funds, and it acts on a **standing instruction** the
user gives once, via an explicit, plain-language, gating acknowledgement BEFORE any
funds are received ("auto-managed across federations, best-effort, no guarantees,
federations can fail, loss is possible, spending amounts only"). There is no
allowlist we curate or push; the device selects federations from public reputation
data (see [ADR-0016](./0016-device-side-federation-selection.md)). We operate no
service that holds or directs funds (see [ADR-0015](./0015-donations-fund-development.md)).

## Consequences

- Posture: published software the user runs, deciding on-device over public data,
  on the user's recorded standing instruction. Not a company allocating customer
  funds. This is the strongest "non-custodial software, not a money service"
  framing; the acknowledgement IS the standing instruction.
- The upfront acknowledgement is the civil-consent record (replaces ADR-0007's weak
  post-hoc dismissible banner).
- Reduces but does not eliminate regulatory exposure. Get a jurisdiction-specific
  legal opinion before v2 ships auto-allocation to users holding funds.
- "No operator" must be real, not nominal: run no fund-path service, keep any infra
  (recurringd) as one-of-many (ADR-0013), and keep selection on public data.
