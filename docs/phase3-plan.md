# Phase 3 plan — discovery + triggers + evacuation (close the autonomy loop)

> **STATUS: 3.A COMPLETE (2026-07-04).** Evacuate execution merged (`5315df3`) and the live
> two-fed exit gate PASSED: the real no-auth sense path reports a healthy fed as healthy; a
> forced shutdown made `decide()` emit `Evacuate A→B (reason ShutdownNotice)` at exactly the
> cap-room bound and `apply()` drained A into B (B netted a hair under, NEVER over; A → ~0).
> The gate also caught + fixed a latent over-credit in the shared §6 receive fixed point
> (`5588b44` — never-over verification clamp). The `/status` shutdown signal requires f+1
> corroborating peers. Next: [phase4-plan.md](./phase4-plan.md) (hardening + operation
> ledger, spec review-clean), then 3.B/3.C per [roadmap-to-v1.md](./roadmap-to-v1.md).

Phase 1 built the **money engine**; Phase 2 wired **sense + decide** (probe → score → snapshot →
decide → apply) and proved a self-directed rebalance live on two federations. Phase 3 closes the
loop: the wallet must (a) **flee a dying federation** on its own (the last money-path primitive),
(b) **discover NEW candidate federations** beyond the joined set, and (c) **run itself** on real
triggers instead of a manual `wallet-cli tick`. Per `docs/integration-phase-plan.md` Phase 3.

Nothing in `wallet-core`'s pure logic changes — `decide()` ALREADY emits `Action::Evacuate` and the
scorer already consumes `shutdown_scheduled`. Phase 3 is sensing + I/O + the discovery layer.

## Feasibility (verified before planning — the ADR-0010 lesson)
The evacuation TRIGGER is real and client-observable (no guardian auth), verified in the pinned SDK
(`~/p/fedimint` @ `b108ec6`). The probe's current `shutdown_scheduled = false` hard-code + its
"no non-admin signal" comment are WRONG (they conflated the guardian-only `SHUTDOWN_ENDPOINT` with
the public `STATUS_ENDPOINT`). Three no-auth signals:
- **PRIMARY** `client.get_meta_expiration_timestamp() -> Option<SystemTime>` (`fedimint-client/src/client.rs:647`) — consensus-backed `federation_expiry_timestamp` meta; reactive via `meta_service().subscribe_to_field`.
- **SECONDARY** `client.api().status()?.federation?.scheduled_shutdown: Option<u64>` (`fedimint-api-client/src/api/mod.rs:845`; public `STATUS_ENDPOINT` @ `fedimint-server/src/consensus/api.rs:871`, no `check_auth`).
- **TERTIARY** quorum degradation from the same `/status`: `peers_flagged`/`peers_offline` + `session_count()` stall.
Matches the June-2026 research in `docs/federation-data-sources-spec.md` + [ADR-0019](./adr/0019-federation-signals-trust-model.md).

## Constraints already decided (ADRs)
- **Evacuation is LN-only in v1** ([ADR-0018](./adr/0018-v1-evacuation-balance-cap.md), [ADR-0004](./adr/0004-v1-lightning-only.md)): shared-gateway internal swap → public-LN fallback. NO on-chain peg-out (early v2). Plus a hard low per-federation balance cap to bound stranding.
- **Never silent** ([ADR-0014](./adr/0014-on-device-agent-standing-instruction.md), which superseded ADR-0007): the agent acts on the user's recorded standing instruction, and every evacuation is disclosed/surfaced (the CLI/report shows it; durably auditable once the operation ledger lands — [operation-history-spec.md](./operation-history-spec.md)), not consented per-move.
- **Discovery/Observer/Nostr are UNTRUSTED** ([ADR-0017](./adr/0017-sybil-resistant-selection-probes-gate.md)/[ADR-0019](./adr/0019-federation-signals-trust-model.md)): they can only supply a candidate set + a demote-only prior; the empirical probe gate + authenticated config are the only trust inputs. Nostr kind-38000 ratings are dropped entirely; kind-38173 is a discovery feed only.

## Build order (Evacuate first — self-contained, completes the money path)

### 3.A — Evacuate execution (the last money-path primitive; NO discovery/triggers needed)
The pure `decide()` already emits `Action::Evacuate{from,to,amount,fee_cap,reason:ShutdownNotice}`,
draining the dying fed into `safest_other`, bounded by the destination's `cap_room`. What's missing:
- **3.A.1 Sense — source the signal in the probe** (`wallet-fedimint/src/probe.rs`): replace
  `shutdown_scheduled = false` with real detection — PRIMARY `get_meta_expiration_timestamp()`
  (true when `now > expiry - lead_time`; shipped as `SHUTDOWN_EVACUATION_LEAD_SECS` = 24h —
  NOTE the 2026-07-05 review found this merged-meta signal override-controlled and
  uncorroborated; corroboration is Phase 4 work, phase4-implementation-spec §15.1), plus
  `status().scheduled_shutdown.is_some()`, plus
  quorum-degradation (tertiary). Keep `assemble_facts`/`assemble_status` PURE (raw signals in →
  `shutdown_scheduled` out); golden-test the assembly (expiry-in-window / scheduled / neither).
- **3.A.2 Act — executor performs Evacuate** (`wallet-fedimint/src/executor.rs`): map `Evacuate`
  in `MovePlan::from_action` to the SAME send-required plan as `Move` (from pays an invoice minted
  on `to`), reusing the entire validated two-leg + idempotent-replay + gross-up path. Flip the
  `evacuate_is_unsupported` test to assert it performs. LN-only: if `from`/`to` share the gateway it
  is the internal swap; otherwise it routes public LN (same code path). No peg-out.
- **3.A.3 Un-drop Evacuate in the tick** (`wallet-fedimint/src/tick.rs`): `decisions_to_apply`
  currently filters `Evacuate` out — stop dropping it now the executor supports it; teach
  `fed_in_executable_move` to treat an Evacuate destination as route-validated.
- **3.A.4 Exit gate (devimint):** force a shutdown signal on fed A (real `scheduled_shutdown` via the
  guardian admin path if devimint supports it, else a `WALLET_CLI_FORCE_SHUTDOWN=<fed>` test seam
  like the crash killpoints), tick → `decide` emits `Evacuate A→B` → `apply` drains A into B (B rises,
  A → ~0). Reuse the two-fed harness + await-send-first. Plus a live SENSE check: probe a healthy
  devimint fed → `shutdown_scheduled == false` (proves the read path works against a real fed).

### 3.B — Discovery (untrusted candidate universe beyond the joined set)
`ObserverClient` (`observer.fedimint.org/api` — untrusted aggregate prior; use `/utxos` sum, not the
wrong `deposits` field) + `NostrClient` (kind-38173 discovery feed only) → a candidate assembler that
emits `FederationFacts` for NEW feds, behind the probe gate (ADR-0017). The assembler is PURE +
golden-tested from RECORDED REAL fixtures (captured Observer/Nostr/ClientConfig JSON). Feeds the
scorer's candidate universe; auto-funding only from the curated allowlist (CEO D5), discovered feds
are manual-join. This is the largest, most open-ended track — plan it as its own pass.

### 3.C — Triggers (run the tick without a human)
Replace manual `wallet-cli tick`: the Android-independent part is a headless **`wallet-cli watch`**
loop (tick on an interval + reactively on `subscribe_to_field(federation_expiry_timestamp)` firing).
The Android-coupled part (foreground refresh / WorkManager periodic / push wake) lands WITH the
frontend. Cache probes with a TTL (never re-probe every tick).

## Testing
- **Fast (rb-lite gate):** pure golden tests — the signal assembly (3.A.1), the Evacuate→plan mapping
  (3.A.2), `decisions_to_apply` keeping Evacuate (3.A.3); discovery assembler from fixtures (3.B).
  No devimint in the loop.
- **Slow (devimint, gated, run by hand):** 3.A.4 the full evacuate tick drains a dying fed; the live
  SENSE read. Reuse `docs/devimint-two-fed-harness.patch` + await-send-first.

## Sequencing rationale
3.A first: it is self-contained (reuses the validated Move machinery + tick), needs no discovery or
Android, and is the honest completion of the "risk engine" — today the engine can top-up a standby
but cannot flee a dying federation. 3.B (discovery) and 3.C's Android triggers are larger tracks that
each deserve their own planning pass; the headless part of 3.C can follow 3.A cheaply.
