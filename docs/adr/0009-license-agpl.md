---
status: accepted
---
# License: AGPL-3.0

The wallet and its server components (recurringd, gateway, notifier) are licensed
**AGPL-3.0**. As a donation-funded, open-source wallet (see
[ADR-0008](./0008-open-source-donation-funded.md)), copyleft prevents a funded
competitor from taking the code closed and out-competing the donation-funded
original. AGPL's network clause additionally forces any hosted fork to publish its
server-side modifications, which keeps the whole stack auditable and reinforces
the inspectable-privacy posture.

## Consequences

- Deters proprietary/closed commercial reuse (intended). Others may use the code
  but must keep their forks open under AGPL.
- Google Play permits AGPL apps; some organizations avoid AGPL dependencies.
- All server components we operate must publish source, consistent with the
  zero-retention / auditability commitments (ADR-0005, ADR-0008).
