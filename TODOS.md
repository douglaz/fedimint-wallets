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
- [ ] **T4 (P1) devimint money-path harness as a release gate**, plus a **misbehaving-gateway test double** (dry B-side, no-discount, accepts-contract-never-provides-preimage). Build/run recipe + CLI cheatsheet + gotchas already captured in [docs/devimint-runbook.md](./docs/devimint-runbook.md) (devimint builds + runs here; the core money-path + lnv2 swap + dedup are live-validated, see [docs/fedimint-mechanics.md](./docs/fedimint-mechanics.md)).
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

## Phase 1 integration build — progress + deferred live validation (2026-07)

Build order from `docs/phase1-implementation-spec.md`. All builds/tests run in nix
(`nix develop /home/master/p/fedimint -c cargo …`); the fedimint dep is pinned to
douglaz/fedimint @ `b108ec6`.

**Done + on `main`:**
- [x] Step 0 async executor + identity newtypes; single-writer CAS claim (codex #2).
- [x] Step 1 pure `move_protocol` (MoveRecord/next_step/assemble).
- [x] Step 2 `FedimintJournal` over the fedimint `Database` (raw-byte + serde rows, atomic index).
- [x] Model rebuild T12/T10/T13/T16 (Action=Move/DirectInflow/Evacuate + advisory; occurrence in
      key; structured `FedBalance`; scorer requires Lnv2). T14 real identities also satisfied.
- [x] Step 3 `MultiClient` join/open/balance + first-class `wallet-cli` (ADR-0023) — **join
      validated LIVE** on devimint.
- [x] Step 4a lnv2 money primitives (receive/pay/await) — **receive validated LIVE** (nets exactly
      amount − lnv2 recv fee).
- [x] Step 4b pure core — fixed-point fee `gross_up` (§6) + `Action→MovePlan`
      + `FedimintExecutor::perform` scaffold (compiles) + `MultiClient` fee-quote/backfill_ops.
- [x] Step 4b-live-1 (branch `feat/executor-directinflow`) — `DirectInflow` path EXECUTES: executor
      pinned-gateway + `backfill_move_record`; `runtime::Runtime` (`direct_inflow`/`await_move`/
      `reconcile`, spec §7/§9); `wallet-cli direct-inflow`/`await-move`/`reconcile`. rb-lite gate
      green (compile+clippy+fmt+unit incl. custom_meta shape + key determinism). `smoke_directinflow_
      devimint.sh` written (await-send-first; asserts net == N EXACTLY + idempotent). `Move` stays
      `Unsupported`.

**devimint reliability SOLVED (2026-07-02):** the flaky lnv2 validation was NOT our code and NOT
debug builds/gateway-readiness — it was a test-harness **await ordering**. lnv2's internal swap
funds the receiver's incoming contract as part of the SENDER's send SM completing, so the payer's
`await-send` must reach `Success` BEFORE the wallet's `await-receive` (else await-receive races an
unfunded contract, long-polls `await_incoming_contract`, and retries on transport timeouts). With
that order + release fedimint binaries (`CARGO_PROFILE=release`), the money smoke is 6/6 reliable.
The `Executor should be running` warning was a red herring (the executor runs fine).

- [x] **4a-pay** — VALIDATED LIVE: the full money smoke passes end-to-end (receive→claimed,
      pay→success+preimage, devimint confirms Claimed, re-pay→already-paid/no-double-debit).
- [x] **4b-live-1 DirectInflow** — VALIDATED LIVE (`smoke_directinflow_devimint.sh`): `wallet-cli
      direct-inflow` → invoice → devimint pays → `await-move: done` → wallet nets the target;
      idempotent re-run mints the SAME invoice (no second mint); `reconcile` is a clean no-op on a
      Done inflow (`awaiting=0`, balance unchanged). The FedimintExecutor DirectInflow path +
      `runtime::Runtime` (direct_inflow/await_move/reconcile) + the CLI all work end-to-end.
- [ ] **gross-up never-under-credit (4b-live follow-up)** — the wallet nets ~18 msat (0.018 sat)
      UNDER the target: lnv2 `receive_fee_quote` under-quotes the true claim fee (it can't see the
      mint OUTPUT / note-selection fee — spec §6 "config constants under-quote"). Make `gross_up`
      conservative (model/estimate the mint output fee, or add a bounded buffer) so net is never
      UNDER the target. Smoke asserts a 1-sat tolerance below target for now.
- [ ] **4b-live-2 Move** — single-fed `Move` (receive + pay via the shared gateway) + `wallet-cli
      move` (perform's send leg + await/settle; currently `Unsupported`).
- [x] **Fee-quote base discrepancy** — RESOLVED (verified vs pinned `b108ec6`): fed fee quoted on
      `contract_amount` (spec §6); the gateway ppm now FLOORS (`GatewayFee::on`) to invert
      `PaymentFee::subtract_from`. Residual: the mint-output-fee under-quote above. See memory.
- [ ] **Step 5 crash gate** — `kill -9` `wallet-cli` mid-move at the deterministic `WALLET_CLI_CRASH_AT`
      killpoints → `reconcile` → no double-pay / no second payable invoice (spec §10).

## Phase 1 model corrections (codex state review, 2026-06-29) — LANDED in the model rebuild above

These landed in the model rebuild (`main`), AFTER the devimint spike taught the real operation
state machine. See [docs/integration-phase-plan.md](./docs/integration-phase-plan.md)
"Model corrections" and [ADR-0022](./docs/adr/0022-money-move-model-and-inflow-direction.md).

- [x] **T12 Redesign `Action`/`Intent`** — landed: `DirectInflow { to, amount, fee_cap }`,
  `Move`/`Evacuate { from, to, amount, fee_cap }`, advisory `RefuseInflow`/`Cap`; occurrence (T10)
  in the idempotency key; `apply` only intents executable actions.
- [x] **T13 Structured per-fed balance** — landed: `FedBalance { spendable, in_flight, claimable, reserved_fee }` at msat.
- [x] **T14 Real identities** — landed: `FederationId([u8;32])` = fedimint consensus hash (bridged in
  `MultiClient`); `GuardianId` = canonical guardian pubkey bytes.
- [x] **T15 Durable operation journal** — satisfied by `FedimintJournal` (over the fedimint RocksDB
  `Database`, NOT SQLite) + `move_protocol` op-log backfill/resume. `backfill_ops` resume path is
  scaffolded (compile); its live resume proof is part of the deferred step-5 crash gate.
- [x] **T16 Scorer requires Lnv2** — landed in the scorer default policy.
- [x] **Inflow-direction first** — `DirectInflow` is the cheap primary lever + built before the swap
  (receive path validated live; `DirectInflow`-nets-amount is in the deferred 4b-live gate).
- [ ] **Honesty (ties to T5):** after ADR-0006, v1 holds ~2 active feds; "allocates across many federations" is the candidate/discovery universe + a v2 promise. Keep product copy honest.
