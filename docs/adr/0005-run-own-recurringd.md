---
status: accepted
---
# Run our own stateless recurringd (zero retention)

The wallet runs its own LNv2 stateless recurringd (`recurringdv2`) to serve
users' Lightning Addresses, rather than depending on a federation's advertised
`recurringd_api`. Reliability is the product's core promise, and a Lightning
Address must not break because a third party's daemon is down. The daemon holds
no funds and cannot claim payments (receive keys derive from the user), so this
adds no custody risk.

## Consequences

- We become the party that sees receive metadata in transit (handle → federation
  → amount → time). We commit to ZERO retention; the stateless v2 design persists
  nothing by default, so this is a logging policy to keep, not extra code.
- Operating one stateless daemon is the infra cost. It joins no federation and
  holds no funds, so it is far lighter than a custodial LNURL server.
- A federation-advertised recurringd may be used as a fallback if our own is
  unreachable (optional).
