---
status: accepted
---
# Auto-allocate without per-action consent; disclose once

The Allocator moves user funds between federations (spending-federation top-ups,
warm-standby funding, evacuation) WITHOUT asking the user before each move. The
user is told ONCE, via a one-time dismissible disclosure the first time funds are
spread across more than one federation, linking to the per-federation health
view. This is a deliberate "disclosure, not consent" stance: per-action consent
would break the WoS-simple promise (a user should not need to understand
federations to spend), while zero disclosure would mean silently placing user
funds with additional custodians the wallet selected.

## Consequences

- The wallet takes a fiduciary-flavored posture: it directs user funds into
  custodians it curates (the curated allowlist). This carries trust and
  regulatory weight and must be named in the product's terms, not buried.
- The one-time disclosure is both a trust gesture and a modest honesty/legal
  shield ("we told you on day one").
- Users who want control can inspect the health view and (later) opt out of
  multi-federation. The default is act-then-disclose.
