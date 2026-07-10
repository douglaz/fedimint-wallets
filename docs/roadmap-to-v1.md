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
- **Phase 4 — hardening + operation ledger: COMPLETE (2026-07-06)** — all six 2026-07-05
  review P1s closed; the append-only ledger + `history`/`show` shipped; both live exit
  gates passed ([phase4-plan.md](./phase4-plan.md)).
- **Phase 5.0 — active probe: COMPLETE (2026-07-07)** — the sats-spending A->B->A
  redeemability probe passed its live devimint gate and now records durable verdict history
  for discovery-driven funding decisions ([phase5-plan.md](./phase5-plan.md)).
- **Phase 5.1 — discovery: COMPLETE (2026-07-09)** — source-agnostic candidate pipeline (Observer HTTP + Manual; Nostr deferred), the `0x09` candidate registry, and the probe GATE wiring: an agent-discovered/auto-joined federation is fundable only after a sustained active-probe PASS (operator-tunable), never on discovery alone. Live devimint exit gate passed ([phase5-plan.md](./phase5-plan.md)).

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

### Phase 5 — active probe, discovery + triggers (= 3.B + 3.C)
5.0, the empirical sats-spending active probe, is complete: the wallet can mint on a
candidate, redeem the probe delta back to the spending federation, and cache a durable
sustained-window verdict. That proves the trust gate ADR-0017 requires before any
discovery-driven funding. Next is the candidate universe (`ObserverClient` + Nostr
kind-38173, untrusted, probe-gated per ADR-0017/0019/0020) and the self-running loop
(`wallet-cli watch`: interval + reactive `federation_expiry_timestamp` subscription; probe
TTL cache). Every agent action lands in the ledger from day one. Until 5.1 wires discovery
into the gate, discovered federations are surface/manual-join only.
**Gate:** discover → structural floor → ACTIVE probe → score → rebalance runs unattended
against devimint, fully recorded; a candidate failing only the active probe is never funded.

### Phase 6a — `walletd`: the 24/7 daemon + local API (Android postponed; re-sequenced 2026-07-10)
The working-wallet milestone while device issues block the Android build. One process owns
the DB permanently (`db.lock` forbids a 24/7 watch loop + operational commands as separate
processes): new `wallet-daemon` crate (axum on 127.0.0.1 + bearer-token file; a Runtime-
owning actor whose command enum is ms-scale bookkeeping ONLY — never network IO; per-
operation IO driver tasks with a Drop-guard in-flight registry; the 5.2 watch scheduler as
a workflow daemon) + new `wallet-api` crate (wire DTOs + the single `WalletConfig` knob
source) + `wallet-cli` as thin client with a standalone fallback. Core premise: the fully-
async intent model — LN hold invoices mean in-flight payments can last HOURS, so no money
operation's network IO may ever block another operation's start. The buildable spec is
authored as `docs/phase6a-plan.md` from the approved design (eng-review 2026-07-10,
`~/.gstack/projects/fedimint-wallets/master-main-design-20260710-031905.md`).
**Gate (merge):** existing live gates rerun through the daemon + the responsiveness gate —
a pay issued mid-probe starts (first external call) <250 ms, instrumented by a
misbehaving-gateway test double (accepts-contract-never-provides-preimage) that holds a
probe in flight deterministically. **Gate (real sats):** a 24h+ soak burn-in.
**6a.2 (fast-follow, gated on the soak):** the NWC facade (NIP-47) so existing phone
clients drive the wallet — no UI code.

### Phase 6b — Android frontend (Slint) + the WoS-simple surface
The locked architecture (pure Rust, Slint, thin JNI shims). First-run standing-instruction
acknowledgement (ADR-0014 — the consent record, gating any receive); one balance +
send/receive with QR (camera spike first — the known feasibility risk); activity screen = the
ledger; health view (D6 — the CEO-review decision: one unified balance + an optional health
view, report §0.5); instant-view/auth-to-send (ADR-0011); WorkManager tick triggers
(Doze-timing spike). **Gate:** receive → auto-allocate → pay on a real device, agent actions
visible in the activity screen with reasons.

Carried from the retired TODOS.md (2026-07-10 — backlog now lives in the phase specs, then
in `br` beads at build time): the pre-build feasibility spikes with written kill criteria +
fallbacks (Slint camera preview → system-scanner intent; Doze/WorkManager → FCM wake;
Block Store recovery → forced manual seed export); the three trust-critical screen specs
(standing-instruction as spending-limit cards, evacuation alert money-centric/past-tense,
success-that-looks-like-loss middle states) + a notification inventory; the UI copy canon
(ban "risk engine", "safe", "bank", "mint", "curated", "anonymous"; honest "~2 active feds"
copy per ADR-0006); discrete events on `mpsc`/`broadcast` (never `watch`, it coalesces);
budget 5-6 real JNI/platform modules; auth-to-send holds agent intents pending biometric
approval (the deleted `requires_auth` concept returns HERE, not earlier).

### Phase 7 — durability + recovery
What all four surveyed wallets got wrong: seed encrypted at rest (Android Keystore +
BiometricPrompt), silent backup of the federation set + standing instruction (ADR-0003).
Carried from the retired TODOS.md: detect "no lockscreen" at onboarding and force a backup
path (never silently degrade to no-backup with funds incoming); prompt manual seed export
at a balance threshold; degraded-infra behavior (public recurringd/gateway unavailable,
illiquid, censored) instrumented with real-world availability data before claiming
"reliable", with one non-sticky recurringd fallback decided.
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
AGPL compliance (ADR-0008/0009). Carried from the retired TODOS.md: a demand signal from
the v1 closed beta (real WoS/Blink users activating + repeat-paying) gates the engine
default; finalize the ADR-0017 probe-gating selection spec alongside the legal opinion.
**Gate:** an outside-voice review finds no P1s.

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
