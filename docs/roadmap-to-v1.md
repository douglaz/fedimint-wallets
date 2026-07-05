# Roadmap to v1 — from headless engine to a fully featured product

The map from today's state to a shippable wallet: WoS-simple on the surface, the
multi-federation risk engine underneath, every action auditable. Detailed plans live per phase;
this doc owns the sequence and the definition of done.

**Relationship to the report's v1/v2 sequencing.** The CEO review
(SIMPLE-FEDIMINT-WALLET-REPORT.md §0.5) re-sequenced the SHIP plan to foundation-first: v1 a
single-federation wallet, the engine ON in v2 behind the fee-vs-risk EV gate. The BUILD order
then evolved: the engine was built first, headless (Phases 1–3), because it is the novel,
risky core. This roadmap supersedes the report on BUILD order only; the SHIP decision — engine
on-by-default at launch vs. single-fed with the engine dormant — remains open and is decided
at Phase 8 by the EV computation + legal opinion. Everything in Phases 4–7 is needed under
either outcome (the ledger, hardening, UI, and recovery serve a single-fed wallet too).

## Where we are

- **Phase 1 — money engine: COMPLETE.** Join/receive/pay/DirectInflow/cross-fed Move,
  crash/reconcile gate live-validated ([phase1-implementation-spec.md](./phase1-implementation-spec.md)).
- **Phase 2 — sense + decide: COMPLETE.** probe → score → snapshot → decide → apply via
  `Runtime::tick`, two-fed exit gate passed ([phase2-plan.md](./phase2-plan.md)).
- **Phase 3.A — Evacuate execution: COMPLETE** (merged `5315df3`; live two-fed exit gate
  passed 2026-07-04 — [phase3-plan.md](./phase3-plan.md)). 3.B discovery + 3.C triggers are
  re-scoped into Phase 5 below.

## Sequence

### Phase 4 — engine hardening + operation ledger ([phase4-plan.md](./phase4-plan.md))
After 3.A (merged), before more automation. Fixes the review backlog — the 2026-07-03
review's P1s (scorer trust floor, strand handling; the send-leg fee-quote base was already
fixed in the 3.A merge) plus the 2026-07-05 fresh-eyes review's P1s (shutdown-signal
corroboration, perform-time cap enforcement, evacuation-destination eligibility, the
deterministic-send-rejection wedge, never-over TOCTOU —
[phase4-implementation-spec.md §15](./phase4-implementation-spec.md)) — and builds the
append-only operation ledger + `history`/`show`
([operation-history-spec.md](./operation-history-spec.md)) — the ADR-0014 auditability
substrate every later phase writes into. **Gate:** a full devimint session is
reconstructible from `wallet-cli history`.

### Phase 5 — discovery + triggers (= 3.B + 3.C) — blocked on the REAL active probe
Today's probe facts are cheap proxies (`round_trip_ok` ⇐ gateway availability,
`peg_out_quotable` ⇐ wallet-module presence). That is fine while the wallet only rebalances
between feds the USER joined, but ADR-0017's trust gate for funding a DISCOVERED federation is
the empirical, sats-spending probe. So Phase 5 starts with **5.0: the real active probe** (a
small self-receive → claim round-trip on the candidate, TTL-cached, tiered behind the free
structural checks per the integration plan) — discovery-driven auto-funding is BLOCKED on it;
until then discovered feds are surface/manual-join only. Then the candidate universe
(`ObserverClient` + Nostr kind-38173, untrusted, probe-gated per ADR-0017/0019/0020) and the
self-running loop (`wallet-cli watch`: interval + reactive `federation_expiry_timestamp`
subscription; probe TTL cache). Every agent action lands in the ledger from day one.
**Gate:** discover → structural floor → ACTIVE probe → score → rebalance runs unattended
against devimint, fully recorded; a candidate failing only the active probe is never funded.

### Phase 6 — Android frontend (Slint) + the WoS-simple surface
The locked architecture (pure Rust, Slint, thin JNI shims). First-run standing-instruction
acknowledgement (ADR-0014 — the consent record, gating any receive); one balance +
send/receive with QR (camera spike first — the known feasibility risk); activity screen = the
ledger; health view (D6 — the CEO-review decision: one unified balance + an optional health
view, report §0.5); instant-view/auth-to-send (ADR-0011); WorkManager tick triggers
(Doze-timing spike). **Gate:** receive → auto-allocate → pay on a real device, agent actions
visible in the activity screen with reasons.

### Phase 7 — durability + recovery
What all four surveyed wallets got wrong: seed encrypted at rest (Android Keystore +
BiometricPrompt), silent backup of the federation set + standing instruction (ADR-0003).
Honest scope (per `fedimint-mechanics.md`): fedimint seed recovery restores ECASH per
federation — NOT operation history, in-flight coordination, or the journal/ledger. So Phase 7
adds an **encrypted app-state backup** (journal + ledger + federation registry — the same
`[0x00]` partition) alongside the seed; a seed-only restore recovers funds, resumes what the
per-fed clients self-resume, and starts the ledger with an explicit "history begins at
restore" row — never silently pretending continuity. **Gate:** device-loss drill both ways —
(a) with app-state backup: funds + full history restored, in-flight ops explained; (b) seed
+ federation set only: funds restored, history honestly marked reset.

### Phase 8 — release hardening
The CEO-review hard gates before real users: fee-vs-risk EV computed at $50–$500 balances
(may conclude the engine defaults OFF at small balances — honor that); jurisdiction-specific
legal opinion on the ADR-0014 posture; refreshed Observer/Nostr fixtures; threat-model pass
(malicious federation, malicious gateway, poisoned discovery feed); repro release builds +
AGPL compliance (ADR-0008/0009). **Gate:** an outside-voice review finds no P1s.

## Definition of "fully featured v1"

| Area | Bar |
|---|---|
| Money | Receive/pay LN; exact-net inflows; cross-fed move + evacuation; hard fee caps both legs |
| Engine | Auto-allocation on a standing instruction; probe-gated scoring; discovery; self-running triggers; evacuate-on-shutdown |
| Auditability | Every operation (incl. failures/refusals) in the append-only ledger: what/why/cost/when/actor |
| Trust/safety | Per-fed balance cap; never-silent agent; scorer trust floor; no double-pay under crash at any killpoint |
| UX | One balance; send/receive with QR; activity + health views; consent gate on first run |
| Durability | Encrypted seed at rest; federation-set backup; verified device-loss recovery |
| Honesty | Copy matches ADR-0006 reality (~2 active feds; discovery universe ≠ active set); solvency caveat |

Explicitly v2+: on-chain peg-out evacuation ladder (ADR-0018), Cashu, iOS, LNURL/lightning
address via recurringd (ADR-0005/0013), multi-device.
