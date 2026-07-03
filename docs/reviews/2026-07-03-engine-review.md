# Engine + spec review — 2026-07-03

Full-repo review of the code (`wallet-core`, `wallet-fedimint`, `wallet-cli`) and the specs/ADRs,
looking for bugs, inconsistencies, security issues, blindspots, and over-engineering — with a
focused pass on **operation observability** (can the user reconstruct exactly what happened and
when?). Method: four independent audit passes (money-path I/O, pure core, spec/ADR consistency,
operation observability), each finding re-verified against the source before inclusion.

State at review time: `main @ 73560e7` (Phase 1 + Phase 2 complete and live-validated; Phase 3.A
Evacuate execution in flight on `feat/phase3-evacuate-execution`).

**Verdict.** The crash-safety core is genuinely strong — the WAL intent journal (atomic
index+row writes, CAS claims, poison tolerance), the op-log-as-truth backfill, and the exact-net
receive gross-up all held up under adversarial tracing. The real problems cluster in three
places: (1) two money-path P1s in the executor, (2) one trust-model P1 in the scorer, and
(3) the biggest product gap: **there is no user-facing operation history at all** — the durable
layer is a crash-recovery substrate, not a record of what the wallet did. That last one gets its
own spec: [operation-history-spec.md](../operation-history-spec.md), built in
[phase4-plan.md](../phase4-plan.md).

---

## P1 — fix before the engine automates further

### 1. Scorer accepts and top-ranks an impossible quorum (`threshold > guardian_count`)
`wallet-core/src/scorer.rs:118-125` enforces only lower bounds (`guardian_count >= min`,
`threshold >= min`). There is no `threshold <= guardian_count` (or `threshold > 0`) sanity
check, and `rank()` (line 182) multiplies raw `threshold` by `STRUCTURAL_WEIGHT`. A
malformed or malicious config reporting `guardian_count = 4, threshold = 10` passes the floor
and **ranks strictly above every honest federation** — while being unable to ever reach quorum.
**Reachability caveat (P1 → P2 today):** the only current `FederationFacts` producer is the
probe, which DERIVES `threshold` as `2f+1` from the guardian set (`NumPeers::threshold()`,
`probe.rs:49-50`) — so an impossible threshold is not constructible from a real config via
today's path. The guard still lands in Phase 4 as defense-in-depth on the trust boundary
itself: Phase 3.B discovery introduces new facts assemblers (Observer/Nostr/fetched configs),
and the scorer must not rely on every future caller sanitizing an attacker-influenced
structural fact (ADR-0017/0019).
**Fix:** hard-reject `threshold > guardian_count || threshold == 0` in the structural floor
(new `ReasonCode::InvalidThreshold`); clamp the rank term to `guardian_count`.

### 2. Send-leg fee-cap check under-estimates the real cost
`wallet-fedimint/src/executor.rs:353-368`: at `MoveStep::Pay` the gateway fee is quoted
`on(invoice_msat)` and `send_fee_quote` also quotes on the invoice amount — but lnv2 charges
both on the (larger) outgoing-contract amount (`multi_client.rs` documents this as a deferred
"live-validation detail"). The destination still nets exactly `amount` (that side is exact), but
the **`fee_cap`/`--max-fee` promise is soft on the send leg**: a move whose true cost exceeds
the cap can pass `total_within_cap` and pay anyway.
**Fix:** quote both send-side fees on the outgoing-contract amount via the same fixed-point
pattern as the receive side, or at minimum round the send quote conservatively (over-estimate)
so the cap never under-blocks. Record the final quote (feeds the operation ledger, below).

### 3. Send-settled + receive-failed strands funds and discards the preimage
`wallet-fedimint/src/executor.rs:430-452`: on `SendState::Success(_preimage)` the preimage is
dropped; if the subsequent receive await returns `Expired`/`Failed`, the move is marked
terminally `MovePhase::Failed` — **after the money has irreversibly left the source**. No
compensation, no retry, and the one artifact that proves payment (the preimage) is discarded.
The shared-gateway coupling plus `validate_move_gateway_before_receive` makes this near-
unreachable, but the code handles it as a reachable state and then handles it wrong.
**Fix:** persist the preimage on the `MoveRecord`; on success-send + non-claimed-receive do NOT
mark terminal `Failed` — either keep re-awaiting the claim (retryable; the contract is funded)
or introduce a distinct `Stranded` phase that `reconcile` retries and the ledger/UI surfaces
loudly. Never a silent terminal loss.

---

## P0 (product) — no operation history exists

The user-facing requirement is: *track all relevant details of all operations so the user knows
exactly what happened and when.* Today, none of the pieces can answer that:

| Gap | Evidence |
|---|---|
| No timestamps on any operation | Only `FederationInfo.joined_at` exists (`journal.rs:82`). `Intent` and `MoveRecord` carry none; the CLI never sets `TickPolicy.now` (stays 0). |
| Completed ops are unscannable | `Done` intents are deliberately un-indexed (`journal.rs`, `is_indexed`) — correct for resume, fatal for history. No enumeration API exists. |
| The "why" is dropped | `Intent::from_decision` discards `AllocatorDecision.reason`; `runtime.rs:172,245` hardcode a dummy reason with a comment saying it is "never persisted". |
| Actual fees are never recorded | `receive_quote`/`send_quote` are computed at `Pay` (`executor.rs:352-368`) then discarded. Only *caps* persist. The user can never see what an operation cost. |
| No actor distinction | A tick-driven agent `Move` and a CLI `move` produce identical rows. ADR-0014's auditable standing-instruction posture is not durably satisfied. |
| Raw `receive`/`pay` bypass everything | `main.rs:284-333` go straight to `MultiClient`; no wallet-side record beyond a once-printed op id. |
| Refusals unrecorded | `RefuseInflow`/`Cap` are never journaled (`wallet-core/executor.rs:359`) — "why didn't the wallet act?" is unanswerable after the fact. |
| No query surface | The CLI has no `history`/`show` verb; every report is print-once-and-gone. |

Root cause: the design conflates the **crash-recovery journal** (mutable, index-pruned, exists)
and the **user-facing operation ledger** (append-only, timestamped, reasoned — does not exist).
`MoveRecord` cannot serve as the ledger either: it is documented as a derived, rebuildable
cache. The fix is a third durable structure — see
[operation-history-spec.md](../operation-history-spec.md).

---

## P2 — correctness / consistency

- **Multi-evacuation can over-fill a destination.** Each `evacuate_decision` clamps to
  `cap_room(to)` against the SAME snapshot; two dying feds evacuating into the same `to` in one
  tick can jointly exceed `per_fed_cap`. Harmless at the v1 active set (~2 feds), real at N≥3.
  Fix in the allocator when discovery grows the set: decisions must reserve balance/cap within a
  tick (fold into Phase 4 hardening).
- **`safest_other` tie-break depends on `Vec` order** (`allocator.rs:200-205`) — deterministic
  only because callers preserve order. Break ties by `FederationId` (`Ord`) and document the
  `AllocatorSnapshot.federations` ordering contract.
- **Probe facts are proxies, oversold by the specs.** `round_trip_ok ⇐ gateway_available`,
  `peg_out_quotable ⇐ wallet_module_present` (`probe.rs:104-108`). Acceptable v1 proxies, but
  `integration-phase-plan.md` described Phase 2's probe as "config-fetch + round-trip +
  peg-out". Spec fixed in this pass; a real paid round-trip probe stays on the roadmap.
- **Source-side trust is deliberately not gated** on a `Move` (`usable_source` checks only
  evacuation-reason, not `probed_ok`/reputation). Correct — you WANT funds out of a distrusted
  fed — but undocumented; a future "fix" would break it. Comment added to the Phase 4 backlog.
- **`min_threshold` is absolute, not proportional**: a 3-of-100 federation passes the floor and
  ranks equal to a 3-of-4. Decision needed: add a proportional floor (e.g. `threshold * 2 >
  guardian_count`) or record that absolute-only is intentional for v1. → Phase 4 decision item.
- **`unix_now()` clamps a bad clock to 0** (`multi_client.rs:778`) — harmless today
  (`joined_at` is display-only), but the operation ledger makes timestamps load-bearing; the
  ledger spec requires a monotonic `seq` alongside wall-clock for exactly this reason.

## P3 — over-engineering / dead surface (cut or wire in Phase 4)

- `Action::Cap` has zero producers — `decide()` only ever emits `RefuseInflow`. Delete it (fold
  its meaning into `RefuseInflow`) or wire a real producer.
- `AllocatorDecision.requires_auth` is always `false` and never read. Delete until an
  auth-gated action exists (ADR-0011 will reintroduce it with a consumer).
- `AllocatorSnapshot.now` is never read by `decide()` and never set by the CLI. Either delete,
  or (preferred) wire it for the evacuation lead-time logic landing with 3.A and MAKE the CLI
  pass real time.
- `FedBalance.{in_flight, claimable, reserved_fee}` are carried everywhere, read nowhere, and
  `claimable` is hardwired 0 in the probe. Keep (conscious shape-stability trade-off) but note
  they are untested passengers until a consumer lands.
- `FederationListReport.skipped_rows` has no consumer; `recovered_receive_only_gateway()`
  stuffs a magic string into a typed `GatewayUrl` — replace with an `Option`/enum when the
  executor is next open.

## Verified sound (anti-findings — do not "fix")

- **Exact-net receive**: `fee::gross_up` floors the gateway fee to byte-match fedimint,
  converges the fed-fee fixed point, and nets exactly `amount`, never over. Live-validated.
- **No double-pay / double-mint**: deterministic lnv2 send op-ids + `AlreadyInFlight`/
  `AlreadyPaid` collapse + backfill-by-`move_id` cover every crash killpoint. The
  `Pending→Executing` CAS + process-local in-flight guard serialize concurrent drivers.
- **Journal atomicity**: intent row + status index move in one dbtx; scans read one snapshot;
  poison rows are skipped without stranding healthy intents.
- **Allocator money bounds**: never drains a source negative, never funds past `per_fed_cap`,
  never evacuates into a dying/blocked/full fed, standby-funding correctly reserves the
  spending target (no double-drain from one snapshot).
- **Stale-occurrence loudness**: terminal same-occurrence replays fail the tick with a remedy
  instead of silently skipping — the correct money-op exit-code contract.

## Spec fixes landed in this pass

- `integration-phase-plan.md`: status banner (Phases 1–2 COMPLETE, 3.A in flight);
  `SqliteJournal` → `FedimintJournal` (RocksDB); probe description no longer claims a real
  round-trip/peg-out probe.
- `phase3-plan.md`: the "never silent" constraint now cites ADR-0014 (which superseded
  ADR-0007) as the governing decision.
- ADR-0010 references were checked — all correctly annotated as dropped; no change needed.
- The wrong probe.rs "no non-admin shutdown signal" comment is already being fixed by the
  in-flight 3.A task (verified against the SDK; see `phase3-plan.md` Feasibility).

## Fix backlog + sequencing

3.A (in flight) touches `executor.rs`/`probe.rs`/`tick.rs`/`runtime.rs`, so code fixes wait for
its merge, then land as **Phase 4.A** ([phase4-plan.md](../phase4-plan.md)):

1. Scorer: threshold sanity floor + rank clamp (+ decide proportional-threshold stance).
2. Executor: send-leg quote on contract amount; persist preimage; `Stranded`-not-`Failed`
   handling for success-send/failed-receive.
3. Allocator: `Ord` tie-break; per-tick cap/balance reservation (pre-discovery); document
   source-gating asymmetry.
4. Dead surface: drop `Cap` + `requires_auth`; wire-or-drop `now`; CLI passes real time.
5. Then **Phase 4.B: the operation ledger** (the P0) — see the spec.
