# Federation public-data spec (scorer inputs)

What public information exists about Fedimint federations, where it comes from, how
reliable it is, and what the on-device scorer (ADR-0016/0017) should actually consume.
Grounded in live data (June 2026): the Fedimint Observer API, `nak` Nostr relay
queries, and `fedimint-cli` joins of real mainnet federations.

## TL;DR

- **Probes are the trust backbone.** Nostr reputation is near-worthless (see below);
  meta is operator-set and spoofable; only the authenticated config (structural) and
  the wallet's own probes are trustworthy. This confirms ADR-0017 (probes GATE,
  reputation only demotes) and ADR-0016.
- **Strongest signals:** guardian count → m-of-n threshold (from the *authenticated*
  config, unspoofable) + empirical probes (quorum liveness, LN round-trip, peg-out
  quotable) + `status.scheduled_shutdown`.
- **Fedimint Observer** is the richest, cheapest aggregate (uptime/latency, backing
  UTXOs, activity, online status) but is admin-curated (~17 mainnet feds, not a
  census), its API is explicitly unstable, and its `deposits` field is wrong (use the
  `/utxos` sum).
- **Nostr** is a discovery feed + weak popularity prior, never a trust input.

## Sources

### A. Authenticated config (from guardians / invite code) — structural, unspoofable, free
The invite code (`fed1…`) carries `federation_id`, ≥ f+1 guardian API URLs, optional
api_secret. Fetching the `ClientConfig` from a guardian quorum yields:
- **Guardian set** `api_endpoints: PeerId → {url, name}`. Count N = `len`.
- **Threshold** is NOT transmitted; derive it: `f = (N-1)/3`, `threshold = 2f+1`
  (`NumPeers`). N=4 ⇒ 3-of-4; N=1 ⇒ 1-of-1 (no fault tolerance — a red flag).
- **Modules present** (mint, ln, lnv2, wallet, meta, stability_pool…), **consensus
  version**, **network** (mainnet/signet), **legacy `global.meta`**.
- `federation_id` = consensus hash of the endpoint set; the config is authenticated
  against the invite's id, so N/threshold/modules cannot be spoofed by a third party.
Cost: one config fetch (also doubles as a liveness probe). Trust: **high**.

### B. The wallet's own probes — ground truth (see "Probe set")
The only signals that exercise real behavior and can't be faked by metadata. Trust:
**highest**. Cost: a few sats + latency for the active ones.

### C. Fedimint Observer API — rich aggregate, cheap, but curated/unstable
Base: `https://observer.fedimint.org/api` (mirror `https://fmo.sirion.io`). It runs a
real fedimint client (consensus history) + polls each guardian every 60s + esplora for
on-chain. Useful endpoints:
- `GET /federations` → summaries `{id, name, last_7d_activity[], deposits, invite,
  nostr_votes{count,avg}, health: online|degraded|offline}`.
- `GET /federations/:id/health` → per-guardian `{avg_uptime, avg_latency(ms),
  latest{block_height, block_outdated, session_count, session_outdated}}` (30-day agg).
- `GET /federations/:id/utxos` → **real backing UTXOs** `{address, out_point, amount}`.
- `GET /federations/:id/config`, `/:id/meta` (merged consensus+override+config),
  `/:id/transactions/count`, `/:id/sessions/count`, `/:id/transactions/histogram`.
- `GET /config/:invite/...` (fetch-on-demand inspector; the "kinda stable" surface).
**Caveats (load-bearing):** admin-curated coverage (~17 mainnet feds tracked vs ~29
announced; not a census); the `/federations` schema is explicitly NOT stable;
`deposits`/`total_assets_msat` is **net peg-in flow, not backing balance** (observed
off by ~7.5×) — **sum `/utxos` instead**; uptime/latency are 30-day smoothed (pair with
the `*_outdated` flags for point-in-time liveness). Trust: medium (you're trusting the
Observer operator + its coverage).

### D. Meta module / `meta_override_url` — advisory, sparse, spoofable
One JSON blob (meta module key 0) or `global.meta` + an HTTP override file. Well-known
keys: `federation_name`, **`federation_expiry_timestamp`** (DateTime; the meta shutdown
convention, read by the client), `federation_successor`, `vetted_gateways` (pubkey
list, no fees), `recurringd_api`, `lnaddress_api`, `welcome_message`, `fedi:*` (app
policy). **All operator-set and unenforced** (a fed can advertise any name/expiry/list);
`meta_override_url` points off-federation to a mutable file (not even guardian-attested).
**Sparse in practice:** 13/17 feds have only `federation_name`; expiry/recurringd/tos
appear on ~1/17. Use as labels/hints, never trust inputs. Trust: **low**.

### E. Nostr — discovery + weak popularity prior only
- **Kind 38173** (NIP-87 fedimint announcement): `d`=federation_id, `u`=invite,
  `n`=network, `modules` CSV, content `{federation_name}`. Real and useful for
  **discovery** (a candidate list of federations + invite codes).
- **Kind 38000** (NIP-87 review): `d`=fed_id, `k`=38173, `rating`=1-5, content `[N/5]`.
  Live reality: ~85% are 5/5, almost all from **one client (Amethyst)**, one event per
  free pubkey (Sybil-able), **no negative/failure events**. ecash-app ignores it; vipr
  counts distinct *recommenders* and discards the stars; Fedi publishes but never reads.
- **No trust-anchor exists yet** for weighting reviewers — ADR-0017's "seed set of
  reputable keys / web-of-trust" is still a placeholder (see Open decisions).
Trust as reputation: **very low**. Use 38173 for discovery; treat 38000
recommender-count as a faint popularity prior, web-of-trust-weighted, never to *promote*
past the probe floor.

### F. Gateways (post-join only) — fees/liveness
Two-stage: federation `gateways` consensus endpoint → URLs; then gateway HTTP
`/routing_info` → `RoutingInfo {lightning_public_key, send_fee_minimum/default,
receive_fee, expiration_delta_*}`. **Only visible after joining** (not in the Observer);
fees cluster tight (base 2000-2500 msat, ppm 3000-5000); the in-protocol `vetted` flag
is **false everywhere**; lnv2 gateway lists are often empty (gateways still register via
lnv1). Trust: the *presence/liveness* of a gateway is real; its self-reported fees are
claims bounded by client-side ceilings.

## Probe set (the wallet's own measurements)

| Probe | How | Pass / signal |
|---|---|---|
| Quorum liveness | `request_current_consensus(session_count)` + `status` | Passes only if 2f+1 guardians agree. `status` gives `peers_online`, `peers_flagged` (want 0), `scheduled_shutdown`. |
| LN round-trip | gateway `/create_bolt11_invoice` → pay → federation `await_incoming_contract` + `await_preimage` | Invoice issued AND preimage observed within timeout; which stage stalls localizes the fault. |
| Gateway availability | `gateways` (consensus) + per-gw `/routing_info` | ≥1 gateway returns `RoutingInfo` with a live `lightning_public_key`; numeric: fees, latency, peer-vettedness count. |
| Peg-out reachability | `block_count` (chain lag), `peg_out_fees(addr,amt)` (quotable?), `wallet_summary` (capacity) | `peg_out_fees = Some`, consensus height tracks the tip, spendable capacity > 0. |
| Latency / degraded quorum | per-guardian `version` RTT, `consensus_ord_latency` | Healthy ≥ 2f+1 reachable+agreeing; degraded f+1…2f; dead ≤ f. |

## Shutdown-notice sources (Evacuation trigger, ADR-0004/0006)
1. **`status.scheduled_shutdown: Option<u64>`** — consensus-reported, strong. Primary.
2. **`federation_expiry_timestamp`** meta — free-text, rare (1/17). Secondary hint.
3. `federation_successor` meta — a migration target if present.

## What the scorer actually consumes (grounded `FederationStatus`)

Reliable, always-present (use these):
- **Structural (config):** guardian count, derived threshold (m-of-n), module set,
  consensus version, network. → the static eligibility floor (reject 1-of-1, wrong
  network, missing modules).
- **Probed (our own, or Observer-measured):** quorum-liveness PASS (the gate),
  uptime/latency, round-trip success, peg-out quotable, backing-UTXO sum, recent
  activity. → the dynamic score; **the round-trip/quorum probe is the GATE** (ADR-0017).
- **Shutdown:** `scheduled_shutdown` / expiry → evacuate.

Hints only (never gate or promote):
- Nostr recommender-count (popularity prior), meta labels, advertised gateway fees.

## Open decisions (for the scorer build)
1. **Observer-as-source vs. own-collection.** Consuming the Observer API is cheap and
   rich, but adds a trust dependency on a curated, unstable, single service with ~17-fed
   coverage. Running our own (config fetch + our own probes + Nostr 38173 discovery) is
   sovereign and full-coverage but more work and needs our own discovery. Likely answer:
   **own probes for the gate + structural config for the floor; Observer optionally as a
   convenience prior / bootstrap, clearly marked untrusted and behind the probe gate.**
2. **Nostr trust anchor (ADR-0017).** With ratings this gameable, a raw count is unsafe;
   weight recommenders by the user's own follow graph / a small seed set, and cap the
   contribution. Still only a tiebreaker.
3. **Coverage / discovery.** ~17 Observer feds + Nostr 38173 announcements is the current
   universe. The scorer must degrade conservatively when a candidate has thin data
   ("few signals → small exposure," ADR-0016), which is the common case today.
