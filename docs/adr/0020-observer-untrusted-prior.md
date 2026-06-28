---
status: accepted
---
# The Fedimint Observer is an untrusted prior behind the probe gate, never load-bearing

Resolves the open decision in
[../federation-data-sources-spec.md](../federation-data-sources-spec.md). The trust gate
is the wallet's OWN probes + authenticated config, computed on-device (ADR-0014, 0017).
The Fedimint Observer (and any similar aggregator) is used ONLY as an optional, untrusted,
swappable input behind that gate: for discovery, for pre-filtering the candidate universe,
and for historical depth the wallet cannot compute itself (30-day guardian uptime/latency,
full backing-UTXO set). It may rank or demote among already-probe-passed federations; it
may NEVER fund or block a federation on its own.

## Consequences

- The wallet is correct if the Observer is wrong, down, gone, or never heard of a
  federation: a fed with a passing own-probe + sound config is fundable regardless of
  Observer coverage (~17 feds today); a fed the Observer calls "healthy" is still not
  funded until our own probe passes.
- Treat it as one-of-many aggregators (like recurringds/gateways, ADR-0013): no sticky
  single authority; user-replaceable; self-hostable later.
- Use the `/utxos` sum for backing balance (the `deposits` field is wrong). Pair 30-day
  uptime with a point-in-time own-probe + the `*_outdated` flags.
- Accept the residual costs: a privacy leak (which feds you query) and the maintenance of
  parsing an explicitly-unstable API. Degrade gracefully (probe-only) when it is
  unavailable.
