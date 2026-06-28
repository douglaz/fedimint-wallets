# Build Backlog

Synthesized from the `/autoplan` review (CEO + Design + Eng, dual voices, 2026-06-28).
The ADRs in `docs/adr/` are canonical; this is the actionable task list.

## v1 prerequisites (do before / at the start of the foundation build)

- [ ] **T1 (P1) Feasibility spikes FIRST, each with a written kill criterion + fallback.**
  - Slint live camera preview on Android → fallback: system camera/scanner intent (non-embedded).
  - Android Doze / WorkManager timing → fallback: push-driven (FCM) wake for urgent work, reconcile on open.
  - Block Store recovery on real hardware (new device, same Google account) → fallback: forced manual seed export path.
  - Each spike GATES the build; a fail forces the named architectural fallback, decided now.
- [ ] **T2 (P1) Persisted executor = write-ahead intent log + idempotent replay + reconcile-on-startup.**
  - Define `AllocatorSnapshot -> Vec<AllocatorDecision>` (reason codes, max-fee, idempotency key, retry policy, fake clock). Network only in the executor. Golden-test the pure function. The executor is LIVE in v1 (receive-claim + spending-fed top-up), "dormant engine" is a misnomer.
- [ ] **T3 (P1) Per-federation balance data model in v1** (per-fed + in-flight + claimable-but-unclaimed), even though the UI shows one number, or the v2 "no rewrite" promise breaks.
- [ ] **T4 (P1) devimint money-path harness as a release gate**, plus a **misbehaving-gateway test double** (dry B-side, no-discount, accepts-contract-never-provides-preimage).
  - Chaos: app killed mid-send; killed after pay before claim; Doze + pending evacuation; recurringd down mid-receive; poisoned/sparse reputation; restore on fresh device.
- [ ] **T5 (P1) Fix source-of-truth drift:** ADRs are canonical; report body retired/annotated (done — banner added). Add a UI copy canon (ban "risk engine", "safe", "bank", "mint", "curated", "anonymous" in user-facing text).
- [ ] **T6 (P1, design) Companion UI screen-and-state spec** (the plan is decision-complete, interface-absent). Nail the 3 trust-critical screens:
  - Standing-instruction acknowledgement: three sequential cards, reframe consent as choosing a spending limit, no checkbox, record copy version.
  - Evacuation/degradation alert: money-centric, past-tense for the auto-resolved case ("we moved $40 to a safer spot"); one-verb action only on real strand-risk; never lead with "federation" or scores.
  - Success-that-looks-like-loss: receive "received, adding to your balance…" middle state; recovery "restoring your balance…" skeleton (never "$0"). Plus a notification inventory.

## Resolved decisions (from the final gate)

- [x] **Trust root (ADR-0014 ↔ 0016 contradiction):** probe-gating hybrid (probes gate, reputation only demotes, low absolute cap, user-editable anchors) — see [ADR-0017](./docs/adr/0017-sybil-resistant-selection-probes-gate.md). **Blocks v2.**
- [x] **v1 evacuation:** hard low enforced balance cap + stranded-funds UI; peg-out → early v2 — see [ADR-0018](./docs/adr/0018-v1-evacuation-balance-cap.md).

## v2 gates (do NOT ship the engine until these clear)

- [ ] Demand signal from the v1 closed beta (real WoS/Blink users activating + repeat-paying).
- [ ] Finalize the probe-gating selection spec (ADR-0017) and get a jurisdiction-specific legal opinion on the on-device-agent posture + any bundled anchor set.
- [ ] On-chain peg-out evacuation rung.

## Follow-ons (P2)

- [ ] **T7** Recovery hardening: detect "no lockscreen" at onboarding and force a backup path (don't silently degrade to no-backup with funds incoming); prompt manual seed export at a balance threshold.
- [ ] **T8** Degraded-infra behavior for public recurringd/gateway unavailable/illiquid/censored; instrument real-world availability before claiming "reliable"; decide on one non-sticky recurringd fallback.
- [ ] **T9** Discrete events on `mpsc`/broadcast (not `watch`, which coalesces); budget 5-6 JNI/platform modules (biometric-gated Keystore + WorkManager are real Android lifecycle code, not thin shims).
- [ ] **T10** Allocator/executor key **epoch** (cross-cutting allocator + executor): the idempotency key is per-logical-intent with no occurrence nonce, so once an intent is `Done` an identical decision that legitimately recurs later is permanently skipped (`wallet-core/src/executor.rs` apply). Add an epoch/occurrence that is stable while a condition persists but advances once the intent settles, so replay stays idempotent AND recurrence stays live.
- [ ] **T11** Executor **auth gating**: `apply`/`drive` perform regardless of `AllocatorDecision.requires_auth` (always `false` today). When biometric-to-send / standing-instruction auth exists, hold `requires_auth` intents instead of auto-performing them.
