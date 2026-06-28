---
status: superseded by ADR-0015
---
# Open-source, donation-funded; gateway fees secondary

The wallet is **open-source** and **donation-funded** — Signal/Wikipedia-style:
periodic, gentle, dismissible in-app donation prompts — with **no mandatory user
fee** on payments. Running our own Lightning gateway may earn supplementary
gateway fees, but that is explicitly NOT the primary revenue stream. (License
choice tracked separately, see ADR-0009.)

## Consequences

- Open-source makes the privacy commitments auditable: the zero-retention
  recurringd (ADR-0005) and the curated-allowlist / auto-allocation control plane
  (ADR-0007) can be inspected, which softens the "central control plane" concern.
  It also supports reproducible builds for trust-minimized distribution.
- Donations are paid as Lightning sends to the project, which dogfoods the core
  send flow. The donation prompt must be gentle and dismissible.
- No payment fee keeps the wallet off the "money-transmission business" framing on
  the fee axis, though the fiduciary posture of ADR-0007 remains.
- Donation revenue is uncertain; plan for lean operation and bridge funding, and
  avoid harbor's "free but abandoned" failure mode.
