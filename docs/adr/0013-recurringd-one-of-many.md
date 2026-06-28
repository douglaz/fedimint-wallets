---
status: accepted
supersedes: ADR-0005
---
# recurringd: one of many community options, not a sticky default

Supersedes [ADR-0005](./0005-run-own-recurringd.md) ("run our own recurringd as
the default"). To keep the "published software, no operator" posture (see
[ADR-0014](./0014-on-device-agent-standing-instruction.md)), the device chooses a
recurringd from several public/community options by availability and reputation.
We may run one as a fallback, but it must never be the mandatory or sticky
default; otherwise we become the de-facto central observer of receive metadata in
practice even though it is optional in theory.

## Consequences

- Reliability now depends on a diverse set of public recurringds rather than one
  we operate. The device must fail over to another when one is down.
- No single party (including us) sees all users' receive metadata.
- recurringd v2 is stateless and holds no funds, so any of them is custody-safe.
