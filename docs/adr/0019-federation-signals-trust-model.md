---
status: accepted
---
# Federation signals: trust probes + authenticated config; Observer is an untrusted prior; Nostr is discovery only

Grounded by live research (June 2026; see
[../federation-data-sources-spec.md](../federation-data-sources-spec.md)). The scorer
(ADR-0016/0017) derives TRUST only from (a) the **authenticated `ClientConfig`** (guardian
count → m-of-n threshold, module set, network — unspoofable, bound to `federation_id`)
and (b) the wallet's **own empirical probes** (quorum liveness, LN round-trip, peg-out
quotable, latency). Everything else is a hint:

- **Fedimint Observer** (`observer.fedimint.org/api`) is a rich, cheap aggregate
  (per-guardian uptime/latency, backing UTXOs, activity, online status) but is
  admin-curated (~17 mainnet feds, not a census), explicitly unstable, and its
  `deposits` field is wrong (net peg-in; use the `/utxos` sum). Treat it as an
  UNTRUSTED convenience prior, behind the probe gate — never as the gate.
- **Nostr** is a discovery feed (kind 38173) plus a faint, web-of-trust-weighted
  popularity prior (kind 38000 recommender count). NOT a trust input: live ratings are
  ~85% 5/5, single-client (Amethyst), Sybil-able, with no failure signal.
- **Meta** fields (name, `vetted_gateways`, `recurringd_api`, expiry) are operator-set,
  unenforced, and sparse (usually only `federation_name`) — labels/hints only.

## Consequences

- Probes GATE; reputation can only demote, never promote past the probe floor (ADR-0017
  holds, and the live data makes it emphatic).
- Evacuation trigger: `status.scheduled_shutdown` (consensus-reported, strong) primary;
  `federation_expiry_timestamp` meta secondary.
- A real structural red flag is catchable for free: a 1-of-1 "federation" (no fault
  tolerance, seen live as "Code Orange") is rejected at the config floor.
- Open: consume the Observer API as a bootstrap prior vs. run our own collection
  entirely (sovereignty + full coverage vs. more work) — see the spec's Open decisions.
