# Phase 4 plan — engine hardening + the operation ledger

> **STATUS: PHASE 4 COMPLETE (2026-07-06).** 4.A hardening landed across three reviewed
> runs plus two live-gate fixes (evacuation pin-override + the small-balance fee-reserve
> regression — both caught by the live evacuate smoke, which then PASSED); 4.B ledger
> landed in two reviewed runs plus two checkpoint fixes (settlement-definitive fee
> semantics; blank-op `--key` verification). **Both exit gates passed live on the two-fed
> harness:** `smoke_evacuate_devimint.sh` (never-over evacuation on the shutdown signal)
> and `smoke_history_devimint.sh` (4.C — the full session reconstructible from
> `wallet-cli history`/`show`, incl. an explained fee-cap refusal and agent/refusal rows).
> All six 2026-07-05 review P1s are closed in code. Next: Phase 5
> ([roadmap-to-v1.md](./roadmap-to-v1.md)).

Sequenced from the 2026-07-03 review ([reviews/2026-07-03-engine-review.md](./reviews/2026-07-03-engine-review.md)).
Runs **after Phase 3.A (Evacuate execution) merges** — 4.A edits the same files 3.A has in
flight — and **before 3.B (discovery) / 3.C (triggers)**: the fixes close money-path holes the
automation would otherwise scale, and every operation from 3.B/3.C onward must be born recorded.

Greenfield: no persisted data, no external users — no backwards compatibility or migration
layers anywhere in this phase.

## 4.A — Correctness hardening (the review's P1/P2 backlog)

1. **Scorer trust floor** (`wallet-core/src/scorer.rs`): hard-reject `threshold == 0 ||
   threshold > guardian_count` (new `ReasonCode::InvalidThreshold`); clamp the rank term to
   `guardian_count`. Golden tests: impossible-quorum config is ineligible and rank 0.
   Severity note: NOT currently reachable — the probe derives `threshold` as `2f+1` from the
   guardian set (`NumPeers::threshold()`), so this is defense-in-depth that becomes
   load-bearing when 3.B's discovery assemblers start producing `FederationFacts` from
   attacker-influenced sources; it lands BEFORE 3.B for that reason.
   DECIDE + record: proportional threshold floor (`threshold * 2 > guardian_count`) in the
   default policy, or document absolute-only as the v1 stance in the ADR-0019 trust model.
2. **Send-leg fee quote on the contract amount** — **the quote-base fix LANDED with the 3.A
   merge (`5315df3`)**: `send_fee_quote_for_amount` quotes on an explicit amount and the Pay
   arm computes the full outgoing contract (see
   [phase4-implementation-spec.md](./phase4-implementation-spec.md) §2). Remaining work:
   persist the final quotes on the `MoveRecord` (feeds 4.B). Devimint check: a cap set just
   under the true cost refuses.
3. **Strand handling** (`executor.rs`): persist the send preimage on the `MoveRecord`; on
   success-send + non-claimed-receive, do NOT mark terminal `Failed` with the preimage
   discarded. **SETTLED (impl spec §3/§14.3): a distinct `Stranded` phase, TERMINAL** — an
   op-log-terminal receive cannot be fixed by re-driving; the preimage is persisted and the
   ledger surfaces it loudly. (This plan's earlier "keep retryable" branch is superseded.)
   Golden: success-send never terminal-fails silently.
4. **Allocator polish** (`wallet-core/src/allocator.rs`): tie-break `safest_other`'s fallback
   by `FederationId` (`Ord`), document the `federations` ordering contract; per-tick cap/balance
   reservation so multiple evacuations cannot jointly over-fill one destination (pre-discovery
   requirement); comment the deliberate source-side trust asymmetry.
5. **Dead surface** (`wallet-core`): delete `Action::Cap` (fold into `RefuseInflow`) and
   `requires_auth`; wire `AllocatorSnapshot.now` (CLI passes real time — the evacuation
   lead-time logic from 3.A reads it) or delete it. Keep `FedBalance`'s reserved fields.
6. **2026-07-05 review absorption** — the fresh-eyes review
   ([reviews/2026-07-05-fresh-eyes-review.md](./reviews/2026-07-05-fresh-eyes-review.md))
   added six P1s to 4.A: shutdown-signal corroboration, perform-time per-fed-cap
   enforcement, evacuation-destination scorer eligibility, deterministic-send-rejection
   classification (the expired-invoice wedge), never-over TOCTOU verification, plus the
   gateway-scan / partial-open / tick-deadline P2s. Buildable specs:
   [phase4-implementation-spec.md](./phase4-implementation-spec.md) §15.

## 4.B — The operation ledger

Implement [operation-history-spec.md](./operation-history-spec.md) in full:

1. `wallet-core`: `OperationRecord`/`OperationKind`/`Actor`/`FeeBreakdown` (pure, serde,
   golden-tested); `Intent` gains `reason`/`actor`/`created_at_ms`.
2. `wallet-fedimint`: ledger rows under `[0x00]` tags `0x05`–`0x07`, written in the SAME dbtx
   as the intent transitions they describe; `record_*` for raw ops, refusals, and ticks;
   injected clock.
3. `wallet-cli`: `history` + `show` verbs (plain text default, `--json` for scripts).

## 4.C — Exit gate

- All existing suites + new goldens green; clippy `-D warnings`; fmt.
- Devimint `smoke_history_devimint.sh` (spec §6): a session of join → direct-inflow → move →
  tick (with one forced failure and one refusal) is fully reconstructible from
  `wallet-cli history` — kinds, actors, reasons, fees, non-decreasing timestamps.
- The 4.A fee-cap and strand behaviors validated live on the two-fed harness.

## Non-goals

3.B discovery, 3.C triggers (next, per [roadmap-to-v1.md](./roadmap-to-v1.md)), any UI,
on-chain peg-out (v2), event-sourced transition logs, pruning.
