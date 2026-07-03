# Roadmap to v1 — from headless engine to a fully featured product

The map from today's state to a shippable wallet: WoS-simple on the surface, the
multi-federation risk engine underneath, every action auditable. Detailed plans live per phase;
this doc owns the sequence and the definition of done.

## Where we are

- **Phase 1 — money engine: COMPLETE.** Join/receive/pay/DirectInflow/cross-fed Move,
  crash/reconcile gate live-validated ([phase1-implementation-spec.md](./phase1-implementation-spec.md)).
- **Phase 2 — sense + decide: COMPLETE.** probe → score → snapshot → decide → apply via
  `Runtime::tick`, two-fed exit gate passed ([phase2-plan.md](./phase2-plan.md)).
- **Phase 3.A — Evacuate execution: IN FLIGHT** ([phase3-plan.md](./phase3-plan.md)).

## Sequence

### Phase 4 — engine hardening + operation ledger ([phase4-plan.md](./phase4-plan.md))
After 3.A merges, before more automation. Fixes the 2026-07-03 review's P1s (scorer trust
floor, send-leg fee cap, strand handling) and builds the append-only operation ledger +
`history`/`show` ([operation-history-spec.md](./operation-history-spec.md)) — the ADR-0014
auditability substrate every later phase writes into. **Gate:** a full devimint session is
reconstructible from `wallet-cli history`.

### Phase 5 — discovery + triggers (= 3.B + 3.C, unchanged)
The candidate universe beyond the joined set (`ObserverClient` + Nostr kind-38173, untrusted,
probe-gated per ADR-0017/0019/0020) and the self-running loop (`wallet-cli watch`: interval +
reactive `federation_expiry_timestamp` subscription; probe TTL cache). Every agent action lands
in the ledger from day one. **Gate:** discover → score → rebalance runs unattended against
devimint, fully recorded.

### Phase 6 — Android frontend (Slint) + the WoS-simple surface
The locked architecture (pure Rust, Slint, thin JNI shims). First-run standing-instruction
acknowledgement (ADR-0014 — the consent record, gating any receive); one balance +
send/receive with QR (camera spike first — the known feasibility risk); activity screen = the
ledger; health view (D6); instant-view/auth-to-send (ADR-0011); WorkManager tick triggers
(Doze-timing spike). **Gate:** receive → auto-allocate → pay on a real device, agent actions
visible in the activity screen with reasons.

### Phase 7 — durability + recovery
What all four surveyed wallets got wrong: seed encrypted at rest (Android Keystore +
BiometricPrompt), silent backup of the federation set + standing instruction (ADR-0003),
restore-from-seed on a fresh device — including pending/awaiting operations and the ledger's
correlation keys, not just ecash. **Gate:** device-loss drill — restore recovers funds across
all joined feds and the operation history explains any in-flight op.

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
