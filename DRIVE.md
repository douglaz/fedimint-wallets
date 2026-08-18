# DRIVE — remove the critical safety and verification blockers found in independent review

**Scope:** the critical corrective thread ONLY:
`br-devimint-runbook-mint-na3` → `br-p93` → `br-n8o`, followed by the
discriminating evacuation-cap gates already tracked in
`br-evac-cap-driven-basis-v07` and the applicable `br-vvo` coverage.
Bookkeeping required to make that sequence honest is also in scope: make
`br-p93` cover the production scheduler as well as the `Runtime::watch_once` dev/test
harness, and
make `br-p93`/`br-n8o` block `br-recanary-y2j-ujs`.

NOT in scope for this drive: running the production re-canary (moves real sats
and remains an operator decision), shipping the Phase 6c web feature chain,
Phase 7 seed encryption, the two-gateway evacuation fallback, Android, or the
rest of the repository backlog. Those remain next-step recommendations, not
silent scope expansion.

**Phase:** VERIFY/HARDEN · **Bead:** `br-n8o`
· **Branch:** `fix/br-n8o-evacuation-supersession`
· **Pending:** outer-driver PR / merge / bead closure
**Gate:** `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
· final release tree gate: `GATE_EXIT=0`, 1101 passed / 0 failed across 24 result lines
  (2026-08-20);
  complete gate log: `/tmp/br-n8o-coderabbit-full-workspace-gate.log`.
· pre-history-pagination parked-handoff/real-corruption discrimination gate: `GATE_EXIT=0`,
  1098 passed / 0 failed across 24 result lines (2026-08-20);
  complete gate log: `/tmp/final-discrimination-expanded-full-workspace-gate.log`.
  Focused `wallet-fedimint` fmt and strict clippy also exited 0
  (`/tmp/final-discrimination-final-fmt.log`,
  `/tmp/final-discrimination-final-clippy.log`), and the seven focused regressions exited 0
  (`/tmp/final-discrimination-pass-*.log`).  Production-behavior mutations went red as intended:
  removing plan-error tick terminalization left `Started`; removing token-failure cleanup left
  `(1, true)`; removing the retained-marker error-arm consume made the following reconcile re-drive
  the parent; and marking CommitTick invoked only on `Ok` abandoned the queue to `(0, false)`
  (`/tmp/final-discrimination-*-red.log`).
· pre-final-discrimination WatchState/replacement-liveness tree: `GATE_EXIT=0`, 1097 passed / 0 failed across
  24 result lines (2026-08-18);
  complete gate log: `/tmp/opus-dual-shape-full-workspace-gate.log`.
· pre-partial-restored-state correction: `GATE_EXIT=0` (2026-08-18);
  complete gate log: `/tmp/watchstate-final-full-gate.log`. It predates the correction below.
· pre-WatchState-final-hardening baseline: `GATE_EXIT=0`, 1054 passed / 0 failed (2026-08-18);
  complete gate log: `/tmp/br-n8o-final3-workspace-gate.log`. It predates the changes recorded
  below and is not a gate claim for this working tree. Superseded 1044-test evidence
  (`/tmp/br-n8o-ultimate2-workspace-gate.log`), 1029-test evidence
  (`/tmp/br-n8o-final-workspace-gate.log`) and 1025-test evidence
  (`/tmp/n8o-display-tristate-full-workspace-gate.log`) and earlier 1008-, 1011-, 1012- and
  1013-test runs (`/tmp/rb-lite-p3-full-workspace-gate.log`,
  `/tmp/n8o-r3-iter3-workspace-gate.log`, `/tmp/n8o-sf-r1-workspace-gate.log`,
  `/tmp/n8o-i3-workspace-gate.log`) were accurate when written but predate
  regressions added by the later review rounds below, so they no longer count
  this tree.
· landed baseline on `main` `410eb2f`: GitHub fmt/clippy/test and Nix package/image
  jobs succeeded in run 31822761409 (2026-08-14)
· baseline on `main` `097e461`: EXIT=0, 792 passed / 0 failed (2026-08-12)
· GitHub CI on `097e461`: success, run 31552972223
· `br-devimint-runbook-mint-na3` branch gate: EXIT=0, 792 passed / 0 failed
  (2026-08-14)
· documented default §1 provisioning: EXIT=0 (2026-08-13); the initially
  absent `~/p/fedimint-72b1e5beadc5a31a33ebc751764cb2f840a63b5e`
  worktree was created at the derived pin, patched exactly, and release-built
· clean-shell corrected build + exact-pin live evacuation gate: EXIT=0
  (2026-08-14); an `env -i` run rechecked §1, reset both approved `target-nix`
  directories, rebuilt the Fedimint release and wallet debug binaries from
  scratch through the fixed Nix child-environment allowlist and fresh temporary
  Cargo source homes with explicit `--target-dir`, and then ran §2's exact
  release-binary invocation;
  `performed=1 skipped=1 failed=0 retryable=0`, A drained from 499,998
  to 35,870 msat and B received 449,918 msat

## Done

### Final accepted liveness dispositions (current working tree; uncommitted)

- **Watch-floor liveness — accepted.** Agent ledger appends atomically advance the durable scan
  high-water only when an initialized reconciled frontier exactly equals their sequence, and still
  fence/reconcile and raise the occurrence floor. User appends deliberately avoid the WatchState hot
  row; allocation later discovers them from the ledger counter and drains the suffix. Allocation and
  standalone observation process a bounded, yielding series of valid 256-row chunks. Before any
  planner-marker handoff, the daemon chains bounded valid-backlog batches rather than waiting for its
   normal cadence; standalone reports the durable high-water and requires the explicit same-tick
   retry. Unreadable rows remain repair-only fail-closed while durable intent recovery continues.
- **Opus P1 floor authority, P2 standalone marker retention, and P3 status disposition — accepted.**
  A legacy direct Agent admission can leave WatchState below its ledger, while an unreadable
  canonical row can contain any `u64`; neither a high scalar nor `u64::MAX` is a safe override, so
  advance, standalone observation, and fresh Agent admission remain fenced until exact-row restore.
  Standalone replacement validation/admission/blocker/CAS-false/confirmed-uncommitted errors now
  retain their exact Pending parent with no child or sidecar; a later strictly newer child exchanges
  it directly, while the distinct authoritative no-child disposition still clears. Status has the
  named private `StandaloneDiagnostic`/`DaemonStrict` mode: stale/default standalone status warns and
  returns populated scored/designation diagnostics with no decisions, whereas daemon status remains
  strict; standalone `u64::MAX` rejection is unchanged. Focused P1/P2/P3 regressions, including the
  middle-chain retry without re-mark, marker-disposition clear, and daemon strict/MAX cases, exited
  0 in `/tmp/opus-p1-p3-focused-green.log`; relevant fmt plus strict `wallet-fedimint`/`wallet-cli`
  Clippy exited 0 in `/tmp/opus-p1-p3-focused-strict.log`. Deliberately clearing the
  confirmed-uncommitted marker made the exact-marker assertion fail (EXIT=101,
  `/tmp/opus-p2-marker-retention-mutation-red.log`); omitting stale standalone status's local
  decision clear made its no-impossible-child assertion fail (EXIT=101,
  `/tmp/opus-p3-stale-status-mutation-red.log`). Restored exact workspace gate:
  `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
  exited `GATE_EXIT=0`, 1089 passed / 0 failed across 24 result lines
  (`/tmp/opus-p1-p3-full-workspace-gate.log`).
- **Standalone dual marker-outcome guard — accepted.** A forged test-only TickPlan carrying both a
  replacement and a separate no-child marker disposition now terminalizes its already-open Tick
  audit row as Failed before any marker clear, child, sidecar, deferred audit, or executor action;
  both exact parents remain repair evidence. Removing only that guard let the forged plan complete
  and made the guard-diagnostic assertion fail (EXIT=101,
  `/tmp/opus-dual-shape-guard-mutation-red.log`). Restored focused regression plus strict
  `wallet-fedimint` Clippy exited 0 (`/tmp/opus-dual-shape-focused-green.log`); the final exact
  workspace gate exited `GATE_EXIT=0`, 1097 passed / 0 failed across 24 result lines
  (`/tmp/opus-dual-shape-full-workspace-gate.log`).
- **Federation-registry poison is an unknown world — accepted.** The daemon status preview and
  production scheduler use the federation-list report rather than silently accepting its
  poison-tolerant healthy subset. `/v1/status` returns an actionable `503` before its dry-run when
  any row was skipped. The scheduler logs the count, opens no federation and creates no fresh
  Tick/probe/discovery work, retains default deadlines, and runs only durable recovery (including
  RecoveryOnly marker redrive). Operators preserve the data directory and repair the exact row;
  they never delete it or re-join around it. The deterministic raw-row regressions pin both the
  dry-run fence and recovery-without-fresh-decision behavior. Reverting either report gate made
  its focused regression fail at the handler `503` or default-deadline assertion (both `EXIT=101`,
  `/tmp/codex-p2-mutation-handler-red.log` and
  `/tmp/codex-p2-mutation-scheduler-red.log`); restored focused strict checks and the exact
  workspace gate exited 0 (`/tmp/codex-p2-focused-strict-clippy.log` and
  `/tmp/codex-p2-full-workspace-gate.log`). The final discrimination regressions now start daemon
  status with an absent raw WatchState and prove its raw `0x0a` row remains absent after the `503`,
  with no persisted probe/Tick; the scheduler pairs a healthy retry-open fixture and counting
  discovery source with the corrupt row, proving recovery still completes while neither opens nor
  discovery work begins. Removing each report gate made its focused assertion fail (`EXIT=101`,
  `/tmp/corrupt-registry-daemon-gate-mutation-red.log` and
  `/tmp/corrupt-registry-scheduler-gate-mutation-red.log`). The focused daemon/scheduler and exact
  standalone warning regressions exited 0 (`/tmp/corrupt-registry-focused-green.log`); focused fmt
  and strict Clippy for both affected packages exited 0
  (`/tmp/corrupt-registry-focused-strict.log`). The final exact workspace gate exited
  `GATE_EXIT=0` (`/tmp/corrupt-registry-full-workspace-gate.log`).
- **Replacement pre-exchange storage faults — accepted.** Parent, pending/blocker, and reservation
  projection read faults retain the exact structural marker in actor and standalone paths; no-child
  authoritative/CAS-miss paths remain the only marker-clear paths. Confirmation retries a bounded
  number of transient (`Retryable`) read failures and treats permanent/mixed confirmation as
  ambiguous. A `Permanent` error returned directly by the replacement autocommit is a rolled-back
  closure/validation result, so the actor retains the marker and consumes its parked handoff without
  poisoning wallet-wide goal or balance authority. The next occurrence uses a distinct child key and
  can self-heal. The scheduler-shaped regression and existing Retryable ambiguity checks passed in
  `/tmp/opus-p2-replacement-regressions.log`; mutating the rolled-back Permanent classification back
  to global ambiguity exited 101 at the no-poison assertion
  (`/tmp/opus-p2-permanent-ambiguous-mutation.log`). The following exact workspace gate passed with
  1086 tests (`/tmp/opus-p2p3-final-workspace-gate.log`).
- **Evidence run in this pass:** `cargo test -p wallet-fedimint --test journal` exited 0 (60 tests)
  in `/tmp/krsp-journal3.log`; `cargo test -p wallet-fedimint --lib` exited 0 (532 tests) in
  `/tmp/krsp-lib2.log`; the repository gate exited 0 in `/tmp/krsp-full-gate.log`. The later
  exact-budget/pure-retry, forced-autocommit-conflict, and poisoned-reconcile marker checks exited
  0 in `/tmp/reviewer-followup-focused.log` and `/tmp/poison-regression2.log`; those focused
  checks were followed by the full workspace gate exiting 0 in `/tmp/final-followup-gate.log`.
  The scheduler's typed immediate-retry classifier and pure 16-chunk checkpoint inspection passed
  focused checks in `/tmp/classifier-focused.log` and the subsequent workspace gate in
  `/tmp/classifier-full-gate.log`. Forced migration/discovery WatchState autocommit conflicts and
  standalone marker-clear parity passed in `/tmp/conflicts-expanded.log` and
  `/tmp/standalone-parity.log`; the following workspace gate exited 0 in
  `/tmp/expanded-full-gate.log`. The standalone parity red mutation exited 101 at its independent
  work assertion in `/tmp/standalone-parity-red.log`. The final exact workspace gate exited 0 with
  1084 passed / 0 failed across 24 result lines (`/tmp/br-n8o-release-final-gate.log`). The
  exact-pin live two-federation smoke also exited 0: it funded A, performed one allocator-selected
  A-to-B standby move (`performed=1 skipped=0 failed=0 retryable=0`), moved A from 2,999,950 to
  1,982,862 msat and B from 0 to 999,998 msat, then refused terminal-occurrence replay without
  moving funds (`/tmp/br-n8o-release-live-tick-final.log`).

### Final formal findings disposition (current uncommitted hardening pass)

- **Final-review correction, bounded WatchState floor migration — updated.**
  `WatchState` serde-defaults the live-row reconciliation bit plus explicit migration initialization,
  exclusive ledger high-water, and exact unreadable canonical-ledger keys. A legacy/absent-state access is
  intentionally a migration **writer**: it performs one bounded (256-row) canonical sequence scan,
  retains the highest readable `Actor::Agent` occurrence, persists the high-water and exact corrupt keys, and exposes
  `agent_floor_reconciled` plus the unreadable-row count at `/v1/watch/status`. The bit covers
  canonical counter-addressable sequence rows, not noncanonical poison rows; the latter retain
  their separate history/budget handling. A persisted reconciled state whose high-water differs
  from the validated counter is made unreconciled and progresses through bounded canonical chunks
  before another Agent allocation. Later unreconciled accesses normally retry only those exact keys
  and direct rows appended after the durable high-water; a valid repair clears its key and can raise
  the floor. The restore-specific exception is a partial checkpoint whose high-water is ahead of the
  validated ledger: its incompatible remembered keys are cleared, its scan restarts at zero, and its
  occurrence is retained as a monotonic floor. An O(1) descending tail check requires the counter
  to name exactly the successor of the highest canonical ledger key; a counter hole, low/missing
  counter, or malformed tail fails closed until counter and ledger are restored from one consistent
  backup. Initial and later tail-consistent counter-row reads are bounded
  to 256 direct sequences per access: a large valid ledger persists truthful partial high-water
  progress and remains unreconciled rather than looping, while a valid >256-row append backlog
  converges over later accesses. Reconciled access is keyed O(1). Until reconciliation is true,
  `advance_watch_occurrence`, `observe_watch_occurrence`, and every first Agent-ledger admission
  (including the retry append path) fail closed; advance/observe drain at most 16 yielding,
  separately committed chunks per call. The daemon preflights that drain before reconcile/open and
  retries only valid zero-unreadable batches without revisiting those phases; unreadable, tail, or
  storage preflight faults run durable-only reconcile/repair (never a parked-marker release)
  before fresh allocation is fenced.
  Standalone retries the same tick. User
  appends deliberately leave the WatchState hot row untouched, trading immediate frontier updates
  for a later bounded suffix drain. Once reconciled, an Agent
  insertion atomically raises the floor and advances scan high-water. Operators must stop,
  preserve, inspect, and restore a malformed ledger row or its backup — never delete it blindly —
  then let the next watch access retry; see the real-sats runbook §8. Historical focused journal, marker-fault,
  and daemon-status checks exited 0 (`/tmp/opus-review-relevant-green.log`); the high-water
  and tracked-key mutations each exited 101 at their respective durable assertions
  (`/tmp/opus-mut-watch-high-water-red.log`, `/tmp/opus-mut-watch-tracked-red.log`), the direct
  Agent hook high-water mutation exited 101 (`/tmp/opus-review-mut-agent-hook-red.log`), and
  mutating ordinary-fallback clear handling back to abort made the confirmed post-commit clear
  test exit 101 (`/tmp/opus-review-mut-postcommit-red.log`), and removing bounded chunk progress
  made the >256-valid-append convergence assertion fail (EXIT=101,
  `/tmp/opus-chunk-mut-progress-red.log`). The historical corrected focused checks exited 0
  (`/tmp/opus-chunk-relevant-final.log`) and the historical exact workspace gate exited 0
  (`/tmp/opus-chunk-full-gate-final.log`, superseded by the pre-hardening 1054-test gate). The subsequent rolling-API/backlog-status focused
  check (fmt, `wallet-api` strict Clippy, and both status tests) exited 0
  (`/tmp/opus-backlog-observability-check.log`). The durable false-with-zero-key backlog and
  partial-frontier Agent-insert regressions exited 0 (`/tmp/opus-final-backlog-focused.log`);
  mutating that insert to unconditionally advance its high-water exited 101
  (`/tmp/opus-final-backlog-mut-agent-frontier-red.log`). Those historical high-water mutations
  predate the persisted-reconciled-state/counter mismatch guard and do not evidence it; neither
  does the historical workspace-gate citation above. The prior pass first ran the new stale-restored
  WatchState regression against the old allocation behavior; it exited 101 at the assertion that
  the bounded rescan sees occurrence 7 (`/tmp/watch-stale-highwater-red.log`). Restored, all 55
  journal tests and that prior exact workspace gate exited 0 (`/tmp/journal-tests.log` and
  `/tmp/watchstate-final-full-gate.log`). The partial-state-ahead-of-restored-ledger regression
  separately exited 101 under the old backward-cursor behavior
  (`/tmp/watch-partial-restored-red.log`); restored, all 56 journal tests exited 0
  (`/tmp/watch-partial-journal-tests.log`) and the current exact workspace gate passed as recorded
  in the header. The final scheduler-preflight, User-hot-row, standalone-diagnostic, and daemon-MAX
  focused command exited 0 in `/tmp/opus-final-focused-green.log`; strict relevant Clippy exited 0
  in `/tmp/opus-final-relevant-clippy.log`; and the exact workspace gate exited 0 in
  `/tmp/opus-final-full-gate.log`. Production-behavior mutations removing the early preflight,
  rewriting WatchState on User append, and replacing the standalone retry instruction each exited
  101 at their named assertions in `/tmp/opus-final-mutation-preflight-red.log`,
  `/tmp/opus-final-mutation-user-hot-row-red.log`, and
  `/tmp/opus-final-mutation-standalone-diagnostic-red.log`, respectively. The follow-up
   unreadable/tail recovery regressions exited 0 in `/tmp/opus-recovery-focused.log`; mutating a
   tail preflight back to short-circuit before recovery exited 101 at its post-reconcile hook in
   `/tmp/opus-recovery-mutation-tail-preflight-red.log`. The current scheduler closure adds the
   repeated partial joined/open-view marker hold, exact converged replacement parent/child/sidecar,
   and abortable post-cycle raw floor-read regressions. The real `run_cycle` now owns the
   converged replacement's occurrence, plan, commit, and successful Tick audit (rather than an
   out-of-band test commit); all 23 scheduler tests exited 0 in
   `/tmp/scheduler-forgery-fixed-focused.log`. Bypassing the production preflight ordering made
   the exact >4096-backlog test fail at its bounded-preflight assertion (`EXIT=101`,
   `/tmp/forgery-fixed-preflight-mutation-red.log`); it was restored before the focused run.
   Current strict `wallet-fedimint` Clippy exited 0 (`/tmp/forge-final-clippy.log`) and the exact
   workspace gate exited 0 (`/tmp/forge-final-workspace-gate.log`).
- **Final-review correction, corrupt strict reservations — implemented, locally verified.**
  Actor reconciliation and standalone `tick` no longer invoke a custom executor fallback.
  Unreadable strict reservations retain the exact marked parent and fail closed until repair.
  Focused actor/standalone regressions passed in `/tmp/final-review-focused2.log`.
- **Final-review correction, structural-marker wake suppression — implemented and
  verified.** A deliberate successful clear and the existing exact `Ok(false)` confirmation retain
  their one-shot key/attempt suppression. A retryable marker-clear error remains conservatively
  suppressed because it may have crossed the commit boundary, including when confirmation cannot
  read; a permanent closure/validation error cannot have committed and installs no suppression.
  Suppression is consumed only after a successful reset, a retryable post-commit-error reread
  proving the exact pending attempt, or `DriverFinished` cleanup of an abandoned attempt; a successful
  `PutPolicy` clears prior-generation entries.
  The combined focused regression covers both the retryable ambiguous clear and a repaired
  permanent refusal whose later qualifying wake must arrive
  (`/tmp/br-n8o-final-relevant-checks.log`). Mutating permanent errors back into the suppression
  class made `a permanent clear error must not suppress the later qualifying wake` fail
  (EXIT=101, `/tmp/br-n8o-final-mut-suppression-red.log`), then was restored. The final exact
  workspace gate exited 0 with 1,029 passed / 0 failed
  (`/tmp/br-n8o-final-workspace-gate.log`). The exact-pin isolated two-federation live tick was
  rerun after these production corrections and exited 0: it funded A, performed exactly one
  1,000,000-msat A-to-B allocator move (`performed=1`, `failed=0`, `retryable=0`), left B at
  999,998 msat, and rejected stale-occurrence replay without moving funds
  (`/tmp/br-n8o-live-tick-final3-rerun.log`). This final live rerun followed the bounded migration,
  canonical ledger-allocation, watch-observability, and independent CommitTick-continuation
  corrections above. Its first unchanged launch failed during the pinned Fedimint build when Nix
  removed an ephemeral `/tmp/nix-shell.*` rustc directory before any wallet assertion
  (`/tmp/br-n8o-live-tick-final3.log`); the clean rerun is the cited pass.
- **Prior final confirmation panel (superseded by the corrections above).** Codex reviewed the complete
  `main` diff and reported no actionable correctness defect
  (`/tmp/n8o-round1-final-codex-review.log`, exit 0). Claude independently verified strict
  admission, no hot wake, exact-parent actor/standalone parity, canonical-successor confirmation,
  MAX and parked-snapshot behavior. Its sole P2 (trust a present WatchState to avoid the ledger
  scan) was rejected with the `main` upgrade evidence and four mutation-red stale-checkpoint tests
  in `challenges-round-1.md` (`/tmp/n8o-round1-final-claude-review.log`, exit 0). Focused restored
  coverage exited 0 (`/tmp/n8o-post-mutation-focused-green.log`); fmt plus workspace strict Clippy
  exited 0 (`/tmp/n8o-round1-fmt-strict-clippy.log`). The exact final workspace gate exited 0 with
  1025 passed / 0 failed (`/tmp/n8o-round1-final-workspace-gate.log`, superseded by the current
  1054-test gate), and `git diff --check` exited 0.

- **Strict-final run round 1, carried parked-snapshot hypothesis — verified ALREADY CLOSED, no code
  change.** The `ReconcileDecide` -> stale replacement `CommitTick` -> `ReconcileDecide` release
  defect is real and is already fixed in this tree: the pre-exchange stale-occurrence arm calls
  `consume_parked_evacuation_marker` (`service/actor.rs:2372`), which drops only the in-memory
  snapshot and the matching handoff, so the deliberately retained durable marker survives and the
  next reconciliation recaptures instead of full-parent-CAS releasing it. Re-proved first-hand
  rather than taken from the round-1..5 write-ups below: deleting only that call made
  `stale_replacement_refusal_consumes_its_parked_snapshot_for_the_next_reconcile` exit 101 at
  `a retained marker stays planner-owned instead of being released for ordinary redrive` with
  `redriven: 1` (`/tmp/n8o-sf-r1-parked-mut-red.log`); restored, that regression plus the display,
  neighbors and standalone-confirmation regressions exited 0 (`/tmp/n8o-sf-r1-focused-green.log`).
  The separate MAX/`WatchState` defenses were re-read and left intact.
- **Strict-final run round 1, "dirty child namespace confirms as uncommitted" — accepted (P3,
  comment-only).** Two money-path comments claimed all three of `replace_marked_evacuation`'s
  corruption guards leave the parent byte-identical and therefore confirm as uncommitted. Verified
  against the code: the `Uncommitted` arm of `ServiceActor::replacement_exchange_outcome`
  (`service/actor.rs:3109`) and `Runtime::confirm_standalone_replacement_exchange`
  (`runtime.rs:4256`) both require `ReplacementChildNamespace::Pristine`, and a dirty child
  namespace still reads `Contaminated` at confirmation, so it matches neither arm and stays
  fail-closed post-exchange-ambiguous. The other two guards do confirm as uncommitted. Only the two
  comments (and the round-3 entry below) were corrected; reclassifying the namespace case was
  rejected in `challenges-round-1.md` as an unreachable-state behaviour change that would also be
  strictly less safe.
- **Round 1 verification:** the exact workspace gate
  `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
  recorded `GATE_EXIT=0` with 1012 passed / 0 failed (`/tmp/n8o-sf-r1-workspace-gate.log`), and
  `git diff --check` exited 0. The pre-change baseline of the same gate on this tree was also
  `GATE_EXIT=0` with 1012 passed (`/tmp/n8o-sf-r1-baseline-gate.log`), which is what makes the
  comment-only round provably behaviour-neutral.
- **Strict-final run round 1, iteration 2 — independent re-derivation, no code or test change.**
  The whole `main`..worktree diff was re-read and the round-1 dispositions were re-checked against
  the production source rather than against the write-ups above. The carried parked-snapshot
  hypothesis was re-proved first-hand: deleting only `consume_parked_evacuation_marker`
  (`service/actor.rs:2372`) made
  `stale_replacement_refusal_consumes_its_parked_snapshot_for_the_next_reconcile` exit **101** at
  `a retained marker stays planner-owned instead of being released for ordinary redrive`, reporting
  `redriven: 1` (`/tmp/n8o-sf-r1-i2-mutA-red.log`); restored, the eight focused regressions
  (parked-snapshot, unselected-parked, rotate-on-clear-fault, release-fault-vs-recovery,
  newly-observed-marker shadow, standalone canonical-successor confirmation, display degrade,
  neighbors chain) exited 0 (`/tmp/n8o-sf-r1-i2-focused-green.log`). The iteration-1
  dirty-child-namespace comment correction was re-verified against
  `ServiceActor::replacement_exchange_outcome` (its `Uncommitted` arm requires
  `ReplacementChildNamespace::Pristine`) and `child_namespace_is_empty`, and the "a fresh claim
  consumes the marker" premise against `set_status_if`'s `Pending -> Executing` arm
  (`journal.rs:4993`). Four further hypotheses raised this iteration were rejected with evidence in
  `challenges-round-1.md` (release-build `cfg` on the force/crash-point predicates; daemon-vs-CLI
  `show` key choice; the behaviour-neutral `scheduler.rs` churn hunk; the `preferred` parent scan
  dropping `pending()`'s status filter). The exact workspace gate recorded `GATE_EXIT=0` with
  1012 passed / 0 failed three times: before the mutation (`/tmp/n8o-sf-r1-i2-baseline-gate.log`),
  after it was restored (`/tmp/n8o-sf-r1-i2-workspace-gate.log`), and over the source tree left
  behind (`/tmp/n8o-sf-r1-i2-final-gate.log`); only this citation sentence was edited afterwards,
  and no gate step reads `DRIVE.md`. `git diff --check` exited 0.
- **Strict-final run round 1, iteration 3 — actor-side confirmation regression added (P2,
  test-only).** Instead of re-deriving the same dispositions a third time, this iteration mutated
  every production behaviour this round changed and re-ran its regression, reverting each mutation
  through the editor and proving the revert with `md5sum -c` against a pre-mutation manifest. Eight
  of the nine mutations reddened at their own assertion (the full matrix, with per-mutation logs, is
  in `challenges-round-1.md`). One did not: reverting
  `ServiceActor::replacement_exchange_outcome` (`service/actor.rs:3117`) from the strict
  `evacuation_canonical_successor` back to the dual-key `evacuation_supersession` left the whole
  492-test `wallet-fedimint` lib suite green (`/tmp/n8o-i3-mutF2.log`, EXIT=0) — the discriminating
  regression existed only on the standalone path. That gap matters on the daemon path: for a parent
  that is itself a replacement, the dual-key reader returns the reverse `A -> B` predecessor, which
  matches neither confirmation arm, so a definitely-uncommitted `B -> C` would be reported as
  `PostExchangeAmbiguous` — retaining the marker and poisoning goal-admission and balance-fact
  authority instead of raising the truthful `Conflict` refusal.
  `service::tests::confirmed_uncommitted_exchange_ignores_a_middle_parents_predecessor` now pins the
  daemon side of that parity: one `Conflict` refusal on the child key, `B` still `Pending` with its
  consumed marker cleared, no `C` row, no canonical successor for `B`, the `A -> B` predecessor
  still readable as audit history, and both authority tokens still issuable. Mutation-red proof:
  the same revert made it exit **101** at `a middle parent's predecessor cannot make B -> C
  ambiguous` (`/tmp/n8o-i3-newtest-mut.log`); restored, it exits 0 (`/tmp/n8o-i3-newtest.log`). No
  production behaviour changed this iteration. Adding daemon/CLI-level tests for the `show` display
  degrade was rejected with evidence in `challenges-round-1.md` — the row's DB key encoding is
  private to `wallet-fedimint`, so it would require exporting a raw-write API from production code
  to corrupt a row. The exact workspace gate recorded `GATE_EXIT=0` with 1013 passed / 0 failed
  (`/tmp/n8o-i3-workspace-gate.log`), and `git diff --check` exited 0.
- **Strict-final run round 1, iteration 4 — two unpinned behaviour CLAIMS closed (P2, tests + one
  corrected comment).** The three iterations above re-derived the diff and mutated its production
  behaviours; this one audited the *comments* on the changed money path, running each observable
  claim instead of reading it. Two held up badly. (1) The `PostExchangeAmbiguous` arm claimed
  "recovery is an explicit restart/operator action rather than an automatic drive of ambiguous
  money". False: `reconcile_durable` (`service/actor.rs:4243`) skips only `marker_is_planner_owned`
  rows, and a committed replacement child is `Pending`/`Agent`/`Evacuate` with NO marker, so the
  next ownership-recovery pass rehydrates and drives it. That behaviour is also the correct one —
  in the only ambiguity where a child exists the exchange committed, and ADR-0029 forbids stranding
  a dying federation's balance — so the fix is the comment plus the missing regression, not the
  code. `ambiguous_exchange_recovery_drives_the_committed_child_while_planning_stays_poisoned`
  injects the ambiguity over an exchange that DID commit and asserts `redriven: 1`, the recovered
  owner, and both authorities still refusing; one mutation per property reddened it at its own
  assertion (`/tmp/n8o-i4-mutA-red.log`, `/tmp/n8o-i4-mutB-red.log`, both 101). (2) `reconcile`
  selects the parked handoff *before* propagating a failed durable scan, with an explicit comment
  that this is deliberate — and swapping those two lines left the whole 494-test `wallet-fedimint`
  lib suite green (`/tmp/n8o-i4-mutC-order.log`, EXIT=0), i.e. iteration 3's matrix had missed it.
  `a_marker_captured_by_a_failed_reconcile_is_released_by_the_next_one` now pins it, reddening at
  `the marker captured by the failed pass got its bounded next-cycle release` under that swap
  (`/tmp/n8o-i4-mutD-red.log`, 101). Every mutation was reverted through the editor and proved
  byte-identical with `md5sum -c`. Six further hypotheses were rejected with evidence in
  `challenges-round-1.md` (qualifying-marker wake cadence; duplicate parked snapshots for one key;
  dirty-child-namespace vs the release CAS; the deliberate standalone `status` occurrence
  authority; the documented public history page cap; the retired parent's inert pristine
  `MoveRecord`). The exact workspace gate recorded `GATE_EXIT=0` with 1015 passed / 0 failed
  (`/tmp/n8o-i4-final-gate.log`), and `git diff --check` exited 0.
- **Final panel round 3, `show` display hard-fail — accepted (P2).** Both front ends had added an
  unconditional strict intent read to the `show` projection, so ONE corrupt intent row took away a
  ledger row `main` served — on the exact command runbook §4a and devimint §7 name for a
  structural-evacuation incident. `FedimintJournal::intent_for_display` now applies this commit's
  own adjacent sidecar policy: a MALFORMED row degrades to absent with a `warn!` (the intent read
  only augments an already-resolved ledger row), while a RETRYABLE storage fault still fails,
  because answering a transient fault with `evacuation_refusal_active: false` would be a false
  display rather than a degraded one. Money paths keep the strict `Journal::get`; the now-unused
  `Journal` import in both binaries is the mechanical proof that `show` was their only strict
  intent read. `malformed_linked_intent_degrades_for_display_while_storage_faults_still_fail` pins
  both properties, one independent mutation each: propagating the Permanent class reddened it at
  `a corrupt intent row must not blank the operation row show resolved`
  (EXIT=101, `/tmp/n8o-r3-display-mutA-red.log`); degrading every class reddened it at
  `a storage fault is not permission to display absence`
  (EXIT=101, `/tmp/n8o-r3-display-mutB-red.log`). Restored green:
  `/tmp/n8o-r3-display-restored-green.log`.
- **Final panel round 3, `/v1/status` vs the rotated marker queue — rejected (P2).** The premise is
  real: after a clear fault with two parked markers the actor's handoff and the dry run's pending
  rescan can name different qualifying parents. Nothing durable diverges — status is dry, both
  parents are legitimate replacements, the queue stays bounded and starvation-free, and both
  children still happen, just in the other order. The preview is inherently not the tick
  (`Runtime::status` re-probes the world; the parked handoff is a full-row snapshot), and neither
  proposed fix removes the divergence: an actor round-trip goes stale immediately, and routing
  status through the actor would put a network probe sweep inside the serialized turn. Recorded
  with evidence in `challenges-round-3.md`.
- **Final panel round 3, dropped exchange-refusal reason — accepted (P3).**
  `replace_marked_evacuation` reports corruption (incoherent parent move artifacts, a second live
  agent evacuation on the source) through the same `Err` channel as a benign CAS miss, and both
  confirm as uncommitted, so that money-path signal was being discarded behind
  a generic conflict. The actor and standalone arms now `warn!` the typed error with the parent key;
  control flow, refusal reasons, marker disposition and messages are unchanged, and the CLI's
  default `warn`-on-stderr subscriber puts it beside the standalone bail.
- **Final panel round 3, `Runtime::status` contract text — accepted (P3).** The pin claim was
  verified still true and left intact; the doc now also records the two deliberate authority bails
  (exhausted occurrence, stale marked-replacement occurrence), why they differ in kind, and that
  both stay read-only. `tick.rs`'s echo was checked and correctly left alone — it is scoped to
  pinned-input problems. No behaviour changed.
- **Final panel round 3, deferred work in the pin gate's `admitted` slot — resolved, no change.**
  The reviewer asked for an explicit decision. `first_move_route_problem` runs over the whole
  planned round before `finish_replacement_round` moves non-child decisions into
  `replacement_deferred`, so deferred work carries the same concrete route preflight the gate
  already trusts, unlike never-preflighted `suppressed` work. A replacement round defers ordinary
  pinned work regardless of probe colour and audits each `tick-drop:`, so including it removes an
  inconsistency instead of manufacturing a false success; the next ordinary cycle still bails loudly
  on a genuinely unusable pin.
- **Round 3 verification:** the exact workspace gate
  `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
  recorded `GATE_EXIT=0` with 1011 passed / 0 failed (`/tmp/n8o-r3-workspace-gate.log`), and
  `git diff --check` exited 0.
- **Round 4 — no actionable finding left, re-verified from scratch, no code or test change.** The
  panel's two P2s were re-checked against the production source rather than against the round-3
  write-up: the accepted `show` degrade is in both front ends via
  `FedimintJournal::intent_for_display`, and the rejected `/v1/status` ordering divergence is still
  dry by construction (`Runtime::status` calls `plan_tick`, whose `qualifying_replacement_parent`
  rescans pending rows; the daemon previews at `watch.occurrence + 1`). The three production
  behaviours this pass depends on were re-mutated first-hand and each reddened at its own
  assertion before restoration: dropping `consume_parked_evacuation_marker` failed
  `stale_replacement_refusal_consumes_its_parked_snapshot_for_the_next_reconcile` at `a retained
  marker stays planner-owned instead of being released for ordinary redrive` with `redriven: 1`
  (EXIT=101, `/tmp/n8o-r4-parked-mutation-red.log`); propagating the malformed class from
  `intent_for_display` failed at `a corrupt intent row must not blank the operation row show
  resolved` (EXIT=101, `/tmp/n8o-r4-display-mutA-red.log`); degrading every error class failed at
  `a storage fault is not permission to display absence` (EXIT=101,
  `/tmp/n8o-r4-display-mutB-red.log`). Restored, both regressions exited 0
  (`/tmp/n8o-r4-focused-restored-green.log`), the exact workspace gate recorded `GATE_EXIT=0` with
  1011 passed / 0 failed (`/tmp/n8o-r4-workspace-gate.log`), and `git diff --check` exited 0.
- **Round 5 — no code or test change; the stale gate line in this header was the only defect
  found.** Both round-2 review files were re-read and their premises re-checked in the production
  source: the `show` degrade is live in both front ends, the parked-queue/`/v1/status` ordering
  divergence remains dry and bounded, and the `u64::MAX - 1` daemon preview is still an unreachable
  P3. The parked-marker plumbing was re-derived independently and holds:
  `finish_replacement_round` sets `marker_disposition` only when a parent was observed AND no
  child was produced, so the childless-shadow fallback keys off the authoritative scan;
  a preferred handoff is admitted only on full-row equality, so a changed or cleared parent falls
  out instead of being replaced; and a marker that stops qualifying under an edited policy fails
  `marker_is_planner_owned` and returns to ordinary redrive rather than stranding. The header's
  "current implementation tree" line still cited the 1008-test rb-lite p3 run, which this tree
  outgrew by three regressions across rounds 2-3; it now cites a first-hand run of the exact
  workspace gate, `GATE_EXIT=0` with 1011 passed / 0 failed
  (`/tmp/n8o-r3-iter3-workspace-gate.log`), with `git diff --check` at exit 0.
- **Codex wall-clock finding — accepted.** `EvacuationRefusalEvidence.measured_at_ms` is display
  material, not an ordering authority: replacement validation no longer compares it with the
  parent creation clock, and the rollback-clock replacement regression is green.
- **Claude scheduler-marker parking finding — accepted.** A tick-suppressed cycle captures full
  marker rows and the next `ReconcileDecide` exact-CAS clears only those rows. Durable-only
  reconciliation does not consume or overwrite that hand-off. This is deliberately no-wake; the
  next normal scheduler interval, not a policy wake, retries ordinary work. The suggested global
  delay of real `PutPolicy` wakes was rejected: marker clear emits no wake, while real policy
  updates must retain prompt activation.
- **Stale-occurrence marker retention finding — accepted.** The production cycle is
  `ReconcileDecide` -> plan -> `CommitTick`, so the reconciliation that hands a parent to the
  planner also parks its exact snapshot. The pre-exchange stale-occurrence refusal therefore
  consumes that in-memory snapshot as well. It writes nothing durable: the deliberately retained
  marker survives, and the next `ReconcileDecide` recaptures it instead of full-parent-CAS
  releasing it into an ordinary redrive. Every snapshot no such refusal consumed still drains on
  the next cycle, so the bounded handoff is unchanged, and the separate MAX/`WatchState` defenses
  are untouched. The post-exchange ambiguous arm deliberately keeps its snapshot: there the drain's
  CAS is itself the exact confirmation, since it clears only a parent that is still byte-identical,
  sidecar-free, and the sole live agent evacuation holder for its source.
  The new `stale_replacement_refusal_consumes_its_parked_snapshot_for_the_next_reconcile`
  regression drives reconcile -> stale plan/commit -> reconcile -> strictly newer occurrence. It
  failed against the unfixed tree and again with only the consumption removed (each EXIT=101 at
  `a retained marker stays planner-owned instead of being released for ordinary redrive`;
  `/tmp/n8o-parked-red-1.log` and `/tmp/n8o-parked-mutation-red.log`), and the restored production
  behavior exited 0 (`/tmp/n8o-parked-restored-green.log`). The exact workspace gate
  `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
  then exited 0 with 1006 passed / 0 failed (`/tmp/n8o-round1-gate-1.log`).
- **Replacement-parent scan race finding — accepted.** The authoritative inner planner scan may
  observe a marker written after planning starts even when an earlier observation did not. The
  childless-shadow fallback now keys off the returned exact `marker_disposition`, not a stale
  pre-scan boolean, and the redundant pre-scan is gone. The deterministic
  `newly_observed_marker_shadow_falls_back_to_unrelated_ordinary_work` regression publishes the
  marker immediately before that authoritative scan and proves unrelated C -> D work survives.
  Disabling only the fallback made it fail at the empty decision list (EXIT=101,
  `/tmp/n8o-shadow-race-mutation-red.log`).
- **Parked-marker recovery-order finding — accepted.** A failed exact-CAS release remains logged,
  fail-closed, and parked, but it no longer returns before the strict durable scan can rehydrate
  unrelated work. `parked_marker_release_failure_does_not_suppress_unrelated_durable_recovery`
  uses a permanent same-source-holder refusal and proves the marker survives while both unrelated
  live intents regain drivers. Restoring the early return made the test fail with that exact
  permanent storage error (EXIT=101, `/tmp/n8o-parked-release-mutation-red.log`). This does not
  weaken strict money-path validation or synthesize a wake; normal scheduler pacing remains.
- **Round-1 restored evidence:** the three focused regressions above exited 0 together
  (`/tmp/n8o-round1-focused-restored-green.log`); the actor service suite passed 160/160 and
  strict `wallet-fedimint` clippy exited 0 (`/tmp/n8o-round1-service-clippy.log`). The exact
  workspace gate exited 0 with 1008 passed / 0 failed
  (`/tmp/n8o-round1-final-workspace-gate.log`).
- **rb-lite two-round summary and round-2 P3 cleanup — accepted, not final formal delta-clean.**
  The two rb-lite rounds left no P0–P2 finding; their round-2 P3 cleanup makes standalone
  structural-replacement `status` require `--occurrence` strictly newer than the marked Agent
  parent, just as standalone `tick` does. The stale case exits non-zero but remains dry: it writes
  neither exchange nor child. The CLI help, real-sats runbook, Phase 6a plan, operation-history
  contract, and ADR-0031 now state that operator requirement. The standalone replacement seam now
  returns `Result<Reservations>` rather than an unreachable successful `None`: every definite
  pre-admission/uncommitted outcome stays an error, so the caller records the failed tick and exits
  non-zero; a successful exchange remains the one-child path. The existing same-N rejection,
  dry-status, and N+1 exchange regression exited 0
  (`/tmp/rb-lite-p3-runtime-focused.log`), as did the focused CLI test and rendered `status --help`
  (`/tmp/rb-lite-p3-cli-focused.log`, `/tmp/rb-lite-p3-status-help.stdout`), relevant fmt/clippy
  (`/tmp/rb-lite-p3-fmt-relevant-clippy.log`), and the exact workspace gate, 1008 passed / 0 failed
  (`/tmp/rb-lite-p3-full-workspace-gate.log`). This records rb-lite's two-round cleanup only;
  the final formal review panels and a final formal delta-clean conclusion remain pending.
- **Independent re-verification of the parked-marker round — no further change needed.** The three
  production behaviours that round changed were re-mutated from scratch rather than taken from the
  earlier log set, and each reddened at its own assertion before being restored: dropping
  `consume_parked_evacuation_marker` failed at `a retained marker stays planner-owned instead of
  being released for ordinary redrive` with `redriven: 1`
  (`/tmp/n8o-final-mut1.log`, EXIT=101); restoring `reconcile`'s early return on a parked-marker
  release fault failed at `a marker-local permanent clear error does not abort durable recovery`
  with the same-source-holder storage error (`/tmp/n8o-final-mut2.log`, EXIT=101); and regating the
  childless-shadow fallback on the stale pre-scan boolean instead of the authoritative
  `marker_disposition` failed at `the marker's childless shadow must not discard unrelated eligible
  work` (`/tmp/n8o-final-mut3.log`, EXIT=101). With every mutation reverted, `git diff --check`
  exited 0 and the exact workspace gate
  `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
  recorded `GATE_EXIT=0` with 1008 passed / 0 failed (`/tmp/n8o-final-run-gate-1.log`).
- **Claude replacement exclusivity/pin finding — accepted.** Replacement-deferred decisions are
  separate from conflict suppression, remain visible to pinned-input validation, are never
  admitted/folded/reported accepted, and are written as replacement-exclusive audit rows in actor
  and standalone paths. Executable siblings use the explicit `tick-drop:` audit identity;
  deferred `RefuseInflow` advisories retain their original `refuse:` identity and every allocator
  diagnostic, plus the replacement-exclusive note.
- **Occurrence exhaustion hardening — accepted.** Standalone `Occurrence(u64::MAX)` is refused
  before it can write the watch floor, a tick ledger row, or an intent. The watch checkpoint uses
  checked advancement rather than saturation, and planner ownership/capture excludes a MAX Agent
  parent: a direct/corrupt exhausted parent is ordinary retryable work, never a parked
  impossible-successor marker.
- **Daemon final-occurrence preview — resolved.** A daemon scheduler may allocate
  `Occurrence(u64::MAX)` exactly once from a `u64::MAX - 1` watch floor. Its dedicated status
  dry-run and tick path accept that final work, while standalone `status`/`tick` remain strict and
  marked-replacement children still must be newer than their Agent parent. `/v1/status` refuses a
  consumed final floor, a provisional reconciliation floor (inspect `/v1/watch/status`, then
  retry/repair), and any joined-but-unopened membership view rather than previewing work the
  scheduler will not allocate. The production `run_cycle` regression also records and terminalizes
  the one final tick before checked exhaustion rejects its successor. Focused handler/scheduler
  evidence and the handler-path mutation that fails at the HTTP-200 assertion are in
  `/tmp/p3-daemon-status-followup-focused.log` and
  `/tmp/p3-daemon-status-followup-handler-mutation-red.log`. The subsequent exact workspace gate
  exited 0 in `/tmp/p3-daemon-status-followup-full-gate.log`.
- **Occurrence/deferred-audit evidence:** focused `wallet-fedimint` runtime, journal, and service
  regressions exited 0 in `/tmp/focused-occurrence-tests-final.log` and
  `/tmp/near-max-scheduler-lifecycle.log`, including standalone MAX-before-write, checked watch
  exhaustion, reconcile → stale near-MAX refusal → reconcile → strictly newer MAX-child commit,
  MAX-parent ordinary redrive, and actor/standalone deferred-advisory diagnostic preservation.
- **Dry-run replacement authority:** standalone `status` applies the same non-mutating strict
  occurrence check as its tick exchange, so it returns an actionable error rather than advertising
  a same-occurrence replacement; it also rejects a no-marker MAX occurrence before any watch or
  tick write, while the existing N+1 dry-run remains valid. Changing the replacement check to
  accept equality made its combined same-N/N+1 regression fail (test EXIT=101,
  `/tmp/status-same-occurrence-mutation-red.log`), and removing the entry MAX check made
  `standalone_status_refuses_max_without_writing_watch_state_or_tick` fail (test EXIT=101,
  `/tmp/status-max-acceptance-mutation-red.log`); both mutations were restored.
- **Occurrence discriminating mutations:** changing checked watch advance back to
  `saturating_add` made
  `watch_occurrence_exhaustion_refuses_max_without_saturating_or_rewriting_state` fail
  (test EXIT=101, `/tmp/occurrence-saturating-mutation-red.log`). Independently accepting MAX
  made `standalone_tick_refuses_max_before_writing_watch_state_or_tick` fail (test EXIT=101,
  `/tmp/occurrence-max-acceptance-mutation-red.log`). Both production mutations were restored.
- **Marker presentation finding — accepted.** `show` exposes retained historical evidence plus
  `evacuation_refusal_active`; only a Pending Agent Evacuate marker is active. Bounded history
  avoids intent N+1 reads.
- **Final display tri-state hardening — accepted.** `OperationView` and standalone
  `OperationRecordAuditView` now make `evacuation_refusal_active` an omitted-or-boolean
  projection: `true` only for a readable exact Pending Agent Evacuate marker, `false` for a
  readable exact inactive intent, and omitted (text `-`) for history or an absent/malformed
  degraded intent. The API, daemon mapping, standalone/client JSON and text paths have focused
  coverage in `/tmp/n8o-display-tristate-restored-focused.log` (EXIT=0). Mutating the daemon
  unknown-intent mapping to `Some(false)` made its exact absence assertion fail with EXIT=101
  (`/tmp/n8o-display-tristate-unknown-to-false-mutation-red.log`); the production mapping was
  restored. The exact workspace gate exited 0 with 1025 passed
  (`/tmp/n8o-display-tristate-full-workspace-gate.log`), and `git diff --check` exited 0.
- **Current verification:** exact workspace gate
  `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
  exited 0 after the final lifecycle regressions below. Focused green evidence includes
  `reconcile_decide_parks_exact_markers_across_durable_recovery_then_redrives_only_cas_match`,
  `commit_tick_marker_clear_fault_terminalizes_an_already_started_tick`,
  `policy_superseded_commit_clears_its_exact_marker_disposition_before_returning`, and
  `replacement_round_audits_deferred_executable_and_advisory_without_admitting_siblings`, plus
  `standalone_status_is_dry_and_tick_atomically_replaces_marked_evacuation` (all EXIT=0).
- **Scheduler-marker discriminating mutations:** changing the production capture guard to `false`
  and, independently, bypassing the production drain each made
  `reconcile_decide_parks_exact_markers_across_durable_recovery_then_redrives_only_cas_match`
  fail (each EXIT=101: expected `redriven == 1`, observed `0`); both mutations were restored.
- **Wall-clock discriminating mutation:** restoring the rejected
  `measured_at_ms < created_at_ms` production bound made
  `replacement_rejects_incoherent_evidence_and_child_cap` fail at its rollback assertion
  (EXIT=101, `/tmp/n8o-wallclock-mutation-red.log`); restoring display-only validation made the
  focused test pass (EXIT=0, `/tmp/n8o-wallclock-green.log`).
- **Replacement exchange-boundary hardening:** the actor now carries a typed
  `ReplacementFailureDisposition`, rather than deriving cleanup authority from a public error
  string. Parent reread, replacement pending scan, reservation projection, and admission failures
  are definite-uncommitted: their exact child-namespace/full-parent cleanup runs before the
  original truthful `Storage` or `Refused` outcome is returned, and a pre-opened tick is
  terminalized. A post-exchange mixed reread remains ambiguous, retains its marker, and poisons
  the existing goal/balance authorities.
- **Replacement boundary evidence:** one-shot actor regressions independently fault the replacement
  parent read, second pending scan, and second reservation projection; each observes no child, an
  exact marker clear, and a Failed pre-opened tick. Focused
  `cargo test -p wallet-fedimint service::tests::replacement --lib` plus
  `ambiguous_exchange_confirmation_retains_marker_and_poisons_goal_and_balance_authority` exited
  0 (`/tmp/replacement-focused-final.log`); wallet-fedimint clippy exited 0
  (`/tmp/replacement-clippy-wallet-fedimint.log`).
- **Replacement boundary discriminating mutations:** bypassing the definite-uncommitted marker
  cleanup made `replacement_parent_read_storage_fault_clears_marker_and_terminalizes_tick` fail at
  its exact-marker assertion (EXIT=101, `/tmp/replacement-preexchange-mutation-red.log`).
  Independently, temporarily permitting a post-exchange branch to rewrite the failed parent without
  its marker (and bypassing the status-transition fence required for that invalid rewrite) made
  `ambiguous_exchange_confirmation_retains_marker_and_poisons_goal_and_balance_authority` fail at
  its retained-marker assertion (EXIT=101, `/tmp/replacement-ambiguity-mutation-red.log`). Both
  production mutations were restored.
- **Final replacement verification:** exact workspace gate
  `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
  exited 0 (`/tmp/replacement-workspace-gate-exact.log`).

- **`br-n8o` implementation is complete pending formal review.** A retryable
  pre-artifact sizing refusal now carries typed
  `EvacuationRefusalEvidence`: it is evidence from fresh structural samples, not proof that
  a route is unavailable. Only the guarded actor/standalone commit seam may claim that durable
  marker for a policy-qualified replacement; an effective cap increase must be component-wise
  monotone at the recorded delivered-net sample.
- The claim is one atomic journal exchange: a distinct fresh occurrence/key is inserted
  `Pending`, its parent becomes `Failed`, and forward/reverse durable supersession sidecars
  are written in the same transaction. The exchange refuses ambiguous authority, stale/equal/
  decreased or crossed cap edits, terminal/non-pre-artifact parents, and any outcome that
  would terminalize the parent without its child. Claiming consumes the marker
  (`Pending -> Executing` cannot recreate it).
- `CommitTick` owns the serialized actor exchange; the exclusive-DB standalone `tick` seam
  performs the same exchange and requires `--occurrence` strictly advanced beyond the marked
  agent occurrence. The daemon advances occurrence itself. `OperationView` and standalone
  `show`/JSON history expose `supersedes` / `superseded_by` from the sidecars, preserving both
  audit identities rather than rewriting a ledger row.
- Deterministic evidence: actor race/atomic-child test
  `/tmp/n8o-race.log`, restart/replay test `/tmp/n8o-restart.log`, standalone end-to-end
  exchange `/tmp/n8o-focused-e2e.log`, and red mutations for same-occurrence admission
  (`/tmp/n8o-occurrence-mutation.log`), cap qualification
  (`/tmp/n8o-cap-mutation.log`), standalone exchange
  (`/tmp/br-n8o-mutation-standalone-exchange-red.log`), and child-driver publication
  (`/tmp/br-n8o-mutation-child-driver-red.log`). Each focused green passed; each listed
  production-behaviour mutation failed at its corresponding assertion.
- Follow-up service evidence: focused green runs for the shadow fallback
  (`/tmp/n8o-shadow-green.log`), exactly-confirmed uncommitted exchange
  (`/tmp/n8o-uncommitted-green.log`), mixed-confirmation poison
  (`/tmp/n8o-ambiguous-green.log`), post-write verification branches
  (`/tmp/n8o-postverify-green.log`), and the existing exchange/restart path
  (`/tmp/n8o-replacement-green.log`) each exited 0. Independent production-behaviour
  mutations exited 101 at the targeted assertions: no ordinary shadow fallback
  (`/tmp/n8o-mutation-shadow-red.log`), no uncommitted-marker clear
  (`/tmp/n8o-mutation-uncommitted-red.log`), ambiguous child recovery
  (`/tmp/n8o-mutation-ambiguous-red.log`), and terminal, marker, N+1, and missing
  postverify cache cleanup (`/tmp/n8o-mutation-postverify-terminal-red.log`,
  `/tmp/n8o-mutation-postverify-marker-red.log`,
  `/tmp/n8o-mutation-postverify-nplus-red.log`, and
  `/tmp/n8o-mutation-postverify-missing-red.log`). `cargo fmt --all -- --check`,
  `cargo clippy -p wallet-fedimint --all-targets -- -D warnings`, and
  `git diff --check` each exited 0; no full workspace gate was run in this pass.
- The exact-pin isolated two-federation live tick rebuilt the pinned/patched Fedimint
  harness and the current wallet CLI from a clean `env -i` launch, joined both
  federations, funded A to 2,999,950 msat, and performed one allocator-selected
  A-to-B move. It exited 0 with
  `performed=1 skipped=0 failed=0 retryable=0`; A ended at 1,982,862 msat,
  B received 999,998 msat, and stale-occurrence replay was rejected without
  moving funds. The unchanged exact-pin gate was rerun after the final marker/cache-race
  fixes and again exited 0 with the same safety assertions; complete current-tree log:
  `/tmp/br-n8o-live-tick-post-rblite-final.log`.

### Historical `br-p93` closeout evidence

- Merged the conflict-scoped scheduler/standalone gate as PR #36
  (`75802dfa898fcde45b8cf0102c30a7b1fc9fe281`), after the final 953-test
  workspace gate, exact-pin two-federation live tick, and GitHub devshell/package
  jobs all exited 0. `br-p93` closed; `br-n8o` was then the next P1.
- Read the current project, canonical docs/ADRs, code architecture, current
  backlog and recent delivery evidence.
- Two independent strategy reviews completed. Both found the re-canary unsafe
  to start before the live-gate repair and the two P1 liveness fixes.
- Verified a production-path omission in `br-p93`: the daemon scheduler gates
  all tick commits on global `ReconcileReport.redriven == 0` in
  `wallet-fedimint/src/service/scheduler.rs::tick_may_commit`; changing only the
  `Runtime::watch_once` dev/test harness would leave production unchanged. The standalone
  `Runtime::tick` command is a separate scheduler-off compatibility path.
- Amended `br-p93`'s tracked body so it owns both the production scheduler and
  standalone gates, with an explicit daemon-path red-first requirement.
- Added dependency edges making both `br-p93` and `br-n8o` block
  `br-recanary-y2j-ujs`; the re-canary no longer appears ready while either P1
  remains open.
- Made this live-gate bead block `br-recanary-y2j-ujs` too, and extended that
  checklist with dedicated non-production evidence for the two newly carried
  money paths plus production-safe observations. It explicitly forbids
  manufacturing retryable/refusal failures on the funded pilot.
- Corrected the exact-pin devimint worktree/patch/release procedure and every
  copied smoke invocation header. A prior whole-diff specification review
  converged to `CLEAN` on pass 3; that result predates, and does not overrule,
  the issues found by the final panel.
- Reconciled the same build instructions in the README and crate manifests with
  this repository's own sufficient devshell, and marked retired TODO identifiers
  as historical rather than live backlog.
- Executed the documented §1 blocks while the default derived worktree path was
  absent. The path was created at `72b1e5beadc5a31a33ebc751764cb2f840a63b5e`,
  the two-fed patch was applied exactly, and the complete release workspace
  build exited 0.
- Re-ran the exact §1 and §2 blocks under `env -i`, with only the basic shell
  identity and path restored. The recipes reset both approved build directories
  and rebuilt the Fedimint release plus wallet debug artifacts from scratch
  through the fixed Nix child-environment allowlist and fresh temporary Cargo
  source homes; both builds and the live evacuation smoke exited 0.
- Rebuilt the soak's wallet release binaries in this repository's devshell with
  both inherited target variables cleared and an explicit `target-nix` target:
  `env -u CARGO_TARGET_DIR -u CARGO_BUILD_TARGET_DIR nix develop -c cargo
  build --release --locked --target-dir "$WALLETS_REPO/target-nix"
  -p wallet-daemon -p wallet-cli` → EXIT=0.
- Branch repository gate exited 0: fmt, clippy with warnings denied, and 792
  tests passed / 0 failed.
- The PR `douglaz/fedimint-wallets#35` record reports final local primary and
  consistency panels with no findings, both GitHub CI jobs successful, a
  current-tip automated review, and `bot-gate` exit 0; it squash-merged as
  `410eb2f`, and
  `br-devimint-runbook-mint-na3` is closed on this reviewed successor branch.
- Replaced both `br-p93` wallet-wide retry gates with one logical allocator
  conflict model. A pending agent top-up holds `FundInto(destination)` and a
  pending agent evacuation holds `Evacuate(source)`; occurrence, amount, route,
  and the opposite endpoint cannot disguise the same work as a new goal. User,
  probe, join/recover, and advisory work do not hold allocator goals.
- Carried the projection through standalone and daemon reconciliation, route
  pricing, allocator planning, and commit. `CommitTick`'s fresh durable scan
  plus same-batch fold and public `decide_op`'s fresh per-admission scan are the
  fail-closed actor checks for resident hosts; the isolated
  `wallet-cli --standalone tick` retains its own load-bearing final re-scan
  before apply. Earlier projections are latency/IO-saving prefilters. ADR-0031
  records that every resident production host uses the actor while
  `Runtime::watch_once` remains a dev/test harness.
- Added discriminating standalone and daemon fixtures: permanently retryable
  work stays on its old key, independent evacuation still plans and commits,
  conflicting fresh-occurrence work does not appear, an evacuation owns its
  source only against later allocator funding, registry-owned work remains a
  blocker, blocked route pairs spend no quotes, and same-goal decisions in one
  bypassed batch cannot both be admitted.
- Added the ADR-0031 composition gate proving a poison-tolerant advisory pending
  scan may omit a corrupt row while strict actor admission still refuses every
  candidate before execution. The implementer report records its
  strict-to-lenient production mutation exiting 101 at the `no intent may be
  admitted` assertion.
- Full branch gate exited 0: fmt, clippy with warnings denied, and 812 tests
  passed / 0 failed. The saved scratch-worktree reports record five focused
  production mutations becoming red before byte-for-byte restoration: bypass
  planner suppression, remove blocked-pair route filtering, remove the actor's
  fresh commit scan, remove its same-batch goal fold, and remove the asymmetric
  evacuation-source/funding edge.
- Outer adversarial pass 1's diff-scoped output reported no findings and the
  repo-aware output reported no P0/P1/P2. Its three P3s were valid and fixed: a
  post-upsert probe-preemption storage failure now conservatively holds its goal
  against the rest of that batch; the
  standalone raw-probe planner's durable blocker derivation now has a
  reservation-room composition test; and stale prior-branch gate wording was
  removed. The implementer report records each guard-removal mutation becoming
  red before restoration, and the full 812-test gate exited 0 afterward.
- Outer adversarial pass 2's diff-scoped output reported no findings; the
  repo-aware review found one valid P3 architecture overstatement. ADR-0031, the canonical
  glossary, this drive, and the conflict-model docs now distinguish the actor
  commit seam for every resident host from the isolated
  `wallet-cli --standalone tick` command's own load-bearing final re-scan.
  `Runtime::watch_once` remains a harness and is not a future host architecture.
- Outer adversarial pass 3's diff-scoped output again reported no findings. The
  repo-aware review found two valid P3s: tracked provenance named review tools,
  and standalone pin diagnostics did not directly prove that conflict-suppressed
  but evaluated work counts. Provenance now names reviewer roles instead (the
  current follow-up bead was updated through `br`), and `tick`/`status` share one
  decided-round pin check. The saved mutation report records the retained-only
  helper change producing the pinned-gateway wallet-wide error before
  restoration.
- Outer adversarial pass 4's diff-scoped output reported no findings. The first
  repo-aware attempt timed out; its bounded fallback completed and found one
  valid P3 composition gap: the standalone planner's construction of `decided`
  was not pinned. The reservation-room fixture now proves the produced plan
  retains its evaluated-but-suppressed A goal and uses it to relax A's
  source-only pin. The saved mutation report records the retained-only
  construction making that focused test red before restoration.
- Outer adversarial pass 5 outputs from both reviewer roles reported
  `No findings.` on the 812-test
  tree. The required cross-file consistency pass then found six documentation
  contradictions: refusal-amount semantics, the retired global retry gate in an
  executor comment and the `br-n8o` chain, a deleted watch command in a follow-up
  bead, shifted source anchors, and unconditional route-pricing summaries.
  Those contracts, beads (through `br`), anchors, and summaries now describe the
  conflict-scoped behavior without changing production code.
- Outer adversarial pass 6's diff-scoped output reported no findings; the
  repo-aware output found one final P3: the same source shifts corrected in ADR-0030 still left
  literal stale anchors in two open follow-up beads and the canonical glossary.
  Both bead bodies were corrected through `br`, and the glossary's route-pricing
  range now lands on candidate selection and both fee legs.
- Final clean-tree verification exposed one real money-path gap: if a held
  evacuation's source had already reached zero, the allocator emitted only an
  advisory refusal, so no fresh decision endpoint relaxed that raw-unusable pin
  and the whole round could still reject an independent evacuation. Durable
  blockers retain the original rebalance source without changing goal identity
  or conflicts; actor and standalone pin validation use that source only for the
  raw lnv2/probe gate, while stale destinations, missing pins, and
  active-probe/fundability pins remain loud. Regressions cover empty held
  evacuation, a held funding source omitted from `FundInto(destination)`,
  standalone planning, the fresh post-plan scan, and both action shapes'
  destination-negative boundary. The saved mutation reports record the
  source/wiring boundaries becoming red before restoration. Four remaining
  literal source anchors were also corrected in ADRs and open beads through
  `br`.
- The complete post-fix repository gate exited 0: fmt, clippy with warnings
  denied, and 817 tests passed / 0 failed.
- Both post-fix adversarial outputs reported no remaining code defect. Their
  sole P3 was another set of literal anchors shifted by the final
  `TickPlan` fields/tests; the ADRs, glossary, and affected follow-up bead now
  point at the final symbols. A final consistency pass remains before live proof.
- The final consistency sweep then separated ADR-0029's explicitly historical
  line basis from current-tree citations, refreshed the remaining current ADR,
  glossary, and open-bead anchors, and scoped the Frontend glossary rule around
  the documented standalone compatibility exception. No production behavior
  changed in those corrections.
- The final repo-aware consistency review found one remaining actor admission bypass:
  public `WalletClient::decide_op` could accept a fresh, goal-bearing
  `Actor::Agent` request outside `CommitTick`. The serialized fresh path now
  re-scans durable goals before persistence, preserving existing-key attaches,
  user/probe/advisory work, and independent allocator goals. Its direct-client
  regression's reported removed-guard run exited 101 because the duplicate
  became `Pending`; the restored run exited 0. An independent reviewer output
  reported no production defect and one test-scheduling flake, fixed by waiting
  for the first driver to start. The exact full gate then exited 0 with 818
  passed / 0 failed. ADR
  ownership wording and the affected follow-up beads now name both actor checks,
  the off-actor CAS ledger-repair exception, and their remaining future blockers.
- The final exact-pin live gates ran from clean `env -i` shells through the
  complete runbook §1 build, §2 preflight, and each smoke's documented launch.
  The first standalone tick run failed usefully: its old 100-sat target sat
  below the live route-economic floor, so the allocator correctly emitted no
  move. Raising both scheduler-smoke targets to 1,000 sats (with 3,000 sats
  funded) made the gate discriminate against that stale dust case without
  weakening its exact-net, never-over, or stale-occurrence assertions.
  `smoke_tick_devimint.sh` then exited 0: it decided/performed the 1,000,000-msat
  fund move, B received 999,998 msat, and A fell 1,017,088 msat. The daemon
  launch also exposed that the sanitized Fedimint devshell has no ambient
  `curl`; its readiness probe now uses product client-mode `wallet-cli health`,
  the same authenticated endpoint. The complete rebuilt
  `smoke_daemon_chain_devimint.sh` run exited 0: B was scheduler-funded to
  999,502 msat, walletd restarted cleanly, and forced shutdown evacuated B to a
  590-msat residue with the expected agent audit rows (`CHAIN_GATE_EXIT=0`).
- The final Opus/xhigh hardening pass found three valid lower-severity edges.
  Durable raw-pin relaxation is now source-only and associated with its exact
  logical goal and holder. Admitted work, conflict-suppressed work,
  and advisory recurrences remain separate through pin validation, so an old
  `A -> B` route cannot hide currently unusable pinned destination A after a
  policy rotation or vouch for a same-goal recurrence re-sourced from C. A
  current funding receive-gate refusal for a pin remains loud unless current admitted
  executable work vouches for either endpoint. Every plan-time conflict-suppressed nonzero
  candidate co-emits a refusal with emitted zero together with a serde-defaulted
  `conflict_suppressed` discriminator keyed to its exact withheld executable
  candidate; the final rescan persists its own tick-drop refusal with the same
  zero-amount discriminator. Both planner and final-rescan suppression warnings
  are visible at the standalone CLI's default log level. The associated mutation
  runs failed at the destination-negative, row-explainability, CLI-label, and
  final-rescan assertions before restoration.
  A later final diff review found the policy-rotation counterexample to the
  first source-only implementation. Its public actor regression went red when
  the broad held-source exemption was restored, then passed with associated
  goal/source evidence and receive-gate dominance. A subsequent panel found the
  opposite coexistence boundary: a pinned standby can be both the admitted
  source of the current top-up and the target of a coarse `NotProbed` refusal.
  The admitted current endpoint now wins so that refusal cannot freeze the whole
  round. The recorded production mutation changed `NotProbed` ordering to
  receive-refusal-first; `nix develop -c cargo test -p wallet-fedimint --lib service::tests::admitted_source_route_beats_a_pinned_standby_receive_refusal -- --exact` exited 101 at
  `expect("the admitted source route must relax the raw pin")`, before the
  production ordering was restored.
  The same consistency pass aligned the standalone actor/direct split, removed
  the deleted `watch` wording, and completed the evacuation-cap documentation
  sweep (`br-cqv` closed through `br`).
- The exact Fedimint devshell does not supply `curl`, while the daemon,
  responsiveness, and soak gates need raw authenticated HTTP. The common exact
  launcher now layers this repository's lock-pinned `curl` package through a
  canonical absolute Nix executable; an actual hostile-environment check proved
  store-backed `curl` and Cargo were present while `BASH_ENV` was absent. The
  first implementation's unqualified inner `nix` failed with exit 126 by
  resolving the Fedimint checkout's `nix/` directory; that red run preceded the
  absolute-executable correction.
- The complete post-hardening repository gate exited 0 across 24 test binaries:
  828 passed / 0 failed. Fresh complete `env -i` exact-pin launches then rebuilt
  Fedimint release and wallet debug binaries through the corrected helper.
  `smoke_tick_devimint.sh` exited 0 with B receiving 999,998 msat and A falling
  1,017,088 msat; `smoke_daemon_devimint.sh` exited 0 with join, receive, pay,
  dedup, history, wrong-token 401, and clean SIGTERM all exercised through
  walletd (`DAEMON_GATE_EXIT=0`). After the associated goal/source policy-rotation
  and suppression-correlation corrections, the complete `env -i` exact-pin tick
  launch rebuilt both source trees and passed on the final production tree. The
  log `/tmp/pr36-live-tick-correlation-final.log` ends with
  `PR36_LIVE_TICK_CORRELATION_FINAL_EXIT=0`; B received 999,998 msat and A fell
  1,017,088 msat.

## Now

Before the final P1 correction, the full repository gate and live proof recorded
above were **pre-final-P1 correction evidence**. They did not cover the bounded
actor admission watermark, the fresh destination-shortfall commit check, or the
final suppressed-source voucher ordering. The subsequent parent run had to rerun
the scoped checks before promoting either claim.

During the `br-p93` correction, an actor-issued, goal-scoped admission snapshot
carried from reconcile through `ProbeFacts`, off-actor planning and `CommitTick`.
A durable intervening Agent goal invalidated only old executable decisions it
conflicted with, including the asymmetric `Evacuate(A)` → funding-touching-A
edge; terminal status did not erase that fact. Commit also refused (without
resizing) a funding move above the scheduler's freshly probed destination target
gap. Raw-pin evidence was ordered admitted endpoint, exact suppressed
source/holder, receive refusal, then advisory recurrence. Focused tests passed
locally. The mutation records were narrowly discriminating:
temporarily replacing `goal.conflicts_with_decision(decision, actor)` with
`false` made
`service::tests::commit_tick_watermark_refuses_terminal_same_goal_without_a_second_driver`
exit 101 at `assertion failed: report.accepted.is_empty()`; temporarily
replacing `amount.0 > shortfall` with `false` made
`service::tests::commit_tick_refuses_funding_that_exceeds_the_fresh_destination_shortfall`
exit 101 at that same acceptance assertion; and restoring receive-refusal-first
ordering made `tick::tests::raw_pin_vouchers_are_associated_and_an_admitted_endpoint_wins`
exit 101 at `the exact current suppressed source voucher outranks a simultaneous
receive refusal`. Each production predicate was restored before the focused
green run. At that point, full and live gates had not been run for this
correction.

Post-panel final hardening then added these focused actor tests, each run with
`nix develop -c cargo test -p wallet-fedimint <test-name>` and exiting 0:
`commit_tick_target_gap_excludes_pending_user_direct_inflow`
(a pending externally paid `DirectInflow(B)` still consumes cap room but does
not reduce the standing target: the full-gap Agent funding key is admitted and
both drivers start);
`commit_tick_evacuation_reserves_destination_before_later_funding` (the first
`Evacuate(C→B)` is accepted while the later full-gap `FundInto(B)` is refused);
`shared_destination_batch_does_not_stale_its_later_sibling` (two independent
evacuations into B are both accepted);
`commit_tick_refuses_only_decisions_touched_after_balance_facts_sample` (a
terminal user transition after sampling refuses only the B-touching old
decision and commits the D→E evacuation);
`commit_tick_scopes_an_exact_terminal_replay_and_commits_an_independent_decision`
(the exact terminal key is refused while an independent key commits); and
`direct_tick_plan_token_retains_live_goal_baseline_through_terminalization`
(an empty caller blocker set cannot replan a token-baselined, terminalized
`FundInto(B)` goal). The companion boundary checks
`empty_tick_round_commits_normally`,
`actor_rejects_a_default_or_foreign_tick_plan_token`,
`absent_transition_upsert_cannot_bypass_actor_admission`, and
`missing_evacuation_destination_balance_is_a_scoped_refusal` also exited 0.

The restored mutation-red evidence was saved as
`/tmp/target-credit-external-separation-mutation-red.log` (incorrectly count
external inbound as target credit: exit 101 at the full-gap admission
assertion), `/tmp/pr36-final-hardening-mutation-balance-generation.log`
(disable the frozen generation check: exit 101 because both decisions were
accepted), `/tmp/pr36-final-hardening-mutation-token-baseline.log` (use caller
blockers instead of the token baseline: exit 101 at the absent-FundInto(B)
assertion), `/tmp/pr36-final-hardening-mutation-terminal-scope.log` (restore a
global terminal-replay abort: exit 101 at the scoped-refusal expectation), and
`/tmp/pr36-final-hardening-mutation-dynamic-generation.log` (compare live,
self-mutated generations: exit 101 because the second shared-destination
evacuation was refused). Every mutation was restored before the final focused
green run. At that point, full repository and live-devimint evidence remained
pre-correction.

The current actor hardening uses a non-Clone external terminal-mutation lease:
the service awaits Pay, Receive, and DirectInflow network outcomes before
acquiring it, then releases it only after the short terminal journal mutation.
An unended lease fails closed; End checked-bumps its epoch, so plan and
balance-fact authority minted before it cannot be reused. Raw repair keeps its
O(ledger) op-log scan off actor but routes only raw Pay/Receive terminal intent
status synchronization through the actor. Membership uses a different, short
publication lease: Join, Recover, and retry-open do their potentially long
network work outside actor authority, acquire it only for the durable
registry/client-map publication, then advance the membership epoch. A tick
token minted before that publication is refused, but a slow membership network
wait does not globally fence fresh tick authority.

Historical focused evidence (not a claim about the current dirty tree) used
`cargo test -p wallet-fedimint
service::tests::membership_admission_invalidates_an_older_tick_world_without_global_blocking
-- --exact`. External-terminal behavior is now split across
`service::tests::external_terminal_lease_fails_closed_while_live_but_not_after_ending`
and `service::tests::ended_terminal_lease_stales_only_its_balance_facts`: the
first pins the live global fence, while the second pins per-federation facts
invalidation after End. The membership test replaces the deleted
`membership_admission_invalidates_an_older_tick_world_and_blocks_new_authority`
name. The current publication-specific regression is
`cargo test -p wallet-fedimint
multi_client::tests::final_open_publication_invalidates_a_preexisting_tick_token
-- --exact`.
`service::tests::repair_terminal_sink_routes_raw_status_through_actor_and_stales_balance_facts
-- --exact` also exited 0: the injected repair sink terminalized a raw Pay via
the actor and an older balance-fact token then refused a fed-touching tick
decision. The actor's mailbox serialization is structural: Begin, token
issuance, and CommitTick are commands on the same single-owner turn; Begin
therefore cannot run during a handling CommitTick, while a live lease and an
ended lease's scoped facts are independently rejected by those focused lease
tests.

The preceding external-lease description does not cover discovery membership changes:
service discovery now uses its own opaque membership-mutation lease. Its live/poison/epoch
state gates tick authority only (not raw terminal leases or user money operations); a checked
End bumps membership world generation before clearing live, while a lost/stale End poisons
future tick authority. Raw repair now carries the scanned ledger sequence through its
terminal write and uses an attempt-bound actor sink. If a delayed observation from attempt N
reaches a manually retried N+1 row it benignly no-ops; if the sink faults after the ledger
terminal, a later repair synchronizes the current terminal row's intent only.

Earlier focused evidence, all under `nix develop -c bash -c ...`, was:
`cargo test -p wallet-fedimint --test ledger
repair_uses_captured_fence_across_observation_before_sink_cas -- --exact` and
`cargo test -p wallet-fedimint --test ledger
repair_sink_cas_cannot_terminalize_retry_started_inside_sink -- --exact` and
`cargo test -p wallet-fedimint --test ledger
repair_retries_terminal_intent_sink_after_first_sink_failure -- --exact`, each exit 0.
That pre-freshness-refactor mutation record is not evidence for the current fence implementation:
the current code captures `{ledger seq, intent attempt, attempt correlation}` in one snapshot and
uses that captured attempt for the sink. Current focused greens are
`stale_prepared_raw_finalizer_cannot_terminalize_retry_attempt_n_plus_one`,
`repair_retries_terminal_intent_sink_after_first_sink_failure`, and
`runtime::tests::standalone_planner_derives_blockers_before_reserving_evacuation_room`, each
exit 0. The complete current gate was rerun as
`nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
and exited 0; its full log is `/tmp/pr36-final-raw-attempt-full-gate.log`.
The completed post-driver-fence rerun used that same exact command with a 3,600-second
allowance and exited 0. `live_driver_cannot_regress_a_repair_terminal_for_its_attempt`
holds a live executor, terminalizes its attempt through the actor-backed repair sink, then
submits the stale driver's attempt-stamped `Awaiting` transition; it returns
`Compared(false)` and leaves the intent `Done`. Its production mutation temporarily admitted
`Done → Awaiting`; the focused test exited 101 at the `Compared(false)` assertion
(`/tmp/pr36-driver-fence-mutation-red.log`), then the predicate was restored and the focused
green run exited 0 (`/tmp/driver-fence-restored.log`).
The real token-world mutation changed
the generation inequality to equality; its `DecideTickRound` test exited 101 at “a token
minted before Join admission must not plan a fresh round”, then was restored and rerun green.

Final raw-terminal review correction: the raw repair sink now atomically changes only the
intent row and its pending-status index, with its captured attempt/action/status CAS. It never
rewrites the ledger terminal already committed by the fenced repair. The authoritative
`finalize_raw_terminal_if_fenced` path remains the atomic ledger-plus-intent writer. The
intent-backed hash-dedup regression proves that the repair terminal remains `repaired=true`,
retains its uncertainty note in both `operation` and `history`, releases the matching intent,
and can later be superseded only by an authoritative op-log observation. The temporary mutation
that restored the ordinary intent+ledger write made that regression exit 101 at
`hash-dedup attribution remains defeasible`; it was restored before the green run
(`/tmp/pr36-final-audit-mutation-ledger-rewrite.log`). A total `observe_op` failure during raw
terminal preparation now returns its error without preparing or terminalizing either row; the
next successful observation enriches and terminalizes both. The post-observation race test
retires N inside `observe_op` and verifies N's captured ledger sequence and attempt correlation
cannot affect N+1; the separate sink-boundary test retires N inside the sink and verifies the
attempt CAS. The complete exact repository gate was rerun after these corrections and exited 0:
`nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`;
its log is `/tmp/pr36-final-audit-full-gate.log`.

Final repair-CAS hardening adds a complete `RawIntentTerminalFence` to the
actor/direct terminal sink: expected ledger sequence, raw federation/role/op identity,
terminal ledger status, and intent attempt are re-read in the sink transaction before it
writes only the intent and pending index. The capture also compares the current scanned
status, and observation sinks name the post-observation op identity and terminal status.
The deliberate no-evidence `RAW_NEVER_REACHED` soft `Failed` is explicitly ledger-only:
its terminal-row retry never sinks the still-retriable intent. The two-pass
`raw_negative_repair_keeps_the_intent_retriable_and_the_ledger_defeasible` regression
leaves that intent `Pending` on both passes. The same-attempt regression
`repair_sink_fence_rejects_same_attempt_soft_terminal_superseded_before_cas` records a
soft hash-dedup terminal, authoritatively supersedes that same ledger sequence to
`Awaiting` before the real sink CAS, and proves the sink returns false by retaining the
nonterminal intent. Removing the sink's exact ledger-status comparison made that test exit
101 at its sink-returns-false assertion (`/tmp/pr36-final-repair-cas-mutation-ledger-status.log`);
the comparison was restored before focused green tests. `cargo test -p wallet-fedimint
--test ledger` (47 tests) and `cargo test -p wallet-fedimint service::tests::` (91 tests)
exited 0, as did the exact full gate
`nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`;
its complete log is `/tmp/pr36-final-repair-cas-full-gate.log`.

Final Pay repair identity correction: after the raw terminal sink re-verifies its
complete ledger fence, it atomically adopts the fence's recovered operation id
into the matching nonterminal Pay/Receive intent before changing its terminal
status and pending index. A conflicting existing intent op rejects the sink
CAS; the sink also permits only `Succeeded → Done` and `Failed → Failed`.
The failed intent-backed hash-repair regression now proves `Failed` plus
`operation_id=Some`, while the actor's existing exact committed-op regression
proves a public manual Pay retry is refused. Hash-only repair for a live retry
N+1 now applies only a terminal success: its two-pass regression resolves the
old N operation by the shared hash first as in-flight and then as `Failed`, and
on both passes leaves N+1 `Pending` with its current ledger row `Started` and
`op_id=None`. Correlation-key and known-current-op observations remain
authoritative. Removing the in-flight guard made that regression exit 101 at
the first-pass no-op assertion (`/tmp/pr36-final-hash-retry-mutation-red.log`);
the terminal-success predicate was restored before the focused ledger suite
(51 tests) exited 0. The exact full gate exited 0; its complete log is
`/tmp/pr36-final-hash-retry-full-gate.log`.

Final legacy-writer and artifact API closure removes `Journal`'s unfenced
`set_operation_artifact` method and its `MemJournal`, `FedimintJournal`, and
actor-transition implementations. `FedimintExecutor` keeps the direct
attempt-fenced `set_operation_artifact_if_attempt` path. In the same database
transaction that selects the current ledger row, legacy `record_update` and
`record_terminal` now permanently reject an intent-backed raw Pay/Receive row;
standalone raw rows and non-raw intent rows remain writable by their production
callers. The delayed N→N+1 update and terminal regressions prove each rejection
leaves both N+1 artifacts unchanged. Removing the update rejection
and running `nix develop -c bash -c 'cargo test -p wallet-fedimint --test
ledger delayed_legacy_record_update_from_attempt_n_rejects_retry_n_plus_one_unchanged
-- --exact'` made the delayed-update regression exit 101 at its expected-error
assertion; the guard was restored before the focused green run
(`/tmp/pr36-final-legacy-writer-mutation-red.log`). The exact current-tree gate
`nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace
--all-targets -- -D warnings && cargo test --workspace'` exited 0; its complete
log is `/tmp/pr36-final-legacy-writer-full-gate.log`.

Final lifecycle attempt-boundary closure makes `Journal::set_status` and
`set_status_if` require the caller's `expected_attempt`; core drive/finalize,
actor transitions, wrappers, and test doubles carry the executing intent's
attempt instead of taking a current-row fallback. `FedimintJournal` verifies
that attempt in the transaction before it writes an intent, index, or ledger
row; stale CAS operations return `false`, while a stale direct status write
returns a clear error. The durable and actor paths share the core pure
same-attempt status predicate, so `Done`/`Failed` cannot return to a live
status (only `retry_failed_intent` creates N+1). Durable and memory `upsert`
also reject an existing different attempt, while same-attempt refreshes remain
available. The direct-`FedimintJournal` regression
`direct_durable_drive_cannot_overwrite_a_retried_attempt` blocks attempt N in
`perform`, terminalizes and retries N+1, then releases N; it proves stale N
CAS/status and upsert cannot change N+1. Removing the durable status attempt
check made that exact test exit 101 at its stale-write assertion
(`/tmp/pr36-direct-attempt-mutation.log`); the check was restored. Focused
core and durable journal suites exited 0 in
`/tmp/pr36-attempt-focused.log`, and the exact full repository gate exited 0:
`nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace
--all-targets -- -D warnings && cargo test --workspace'`; its complete log is
`/tmp/pr36-final-status-attempt-full-gate.log`.

Final raw-await and awaiter liveness closure makes exact ordinary terminal raw
ledger rows idempotent for the current attempt: artifact replay may complete a
missing matching intent artifact without rewriting the terminal ledger row, and
the matching terminal observation is a successful no-op. Repaired terminals
remain eligible for their one authoritative supersession. The raw finalizer now
prepares only after a terminal observation, retains its observed terminal status
privately, rejects a contradictory caller status, and returns a retryable error
when its fenced write loses while that same attempt is still nonterminal. The
crash-window regression re-drives both Pending and Executing intents with an
already-Succeeded same-operation ledger row through Done without repair; the
finalizer regressions reject in-flight observation and contradictory status and
retain ownership on a same-attempt false fence result.

Awaiter completion now carries `retry_awaiter`: a failed subscription is logged,
waits one second outside the actor, and only then permits the actor to remove
that generation, re-read the durable intent, and respawn for the same Awaiting
attempt. Normal, terminal, stale, and shutdown completions do not respawn. The
transient-awaiter completion regression proves retry ownership reappears and
then terminalizes without an external reconcile. Temporarily
removing the actor retry predicate made
`cargo test -p wallet-fedimint transient_awaiter_error_reacquires_subscription_then_terminalizes_without_reconcile`
exit 101 at its replacement-owner assertion
(`/tmp/pr36-mutation-red-awaiter.log`); the predicate was restored. The focused
raw-finalizer, crash-window, and awaiter tests exited 0
(`/tmp/pr36-final-focused-tests.log`). The exact current-tree gate
`nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
exited 0; its complete log is
`/tmp/pr36-final-awaiter-liveness-full-gate.log`.

Final await-convergence correction makes `prepare_raw_operation_terminal` carry
the runtime awaiter's expected attempt into its private prepared value. A
preparation that cannot construct a fenced update now re-reads the current
intent during finalization: the same `Pending`, `Executing`, or `Awaiting`
attempt returns a retryable ownership diagnostic; an absent, different-attempt,
or terminal intent remains a benign stale no-op. The same recheck follows a
false fenced finalization. For the crash window where the exact ledger
sequence/federation/role/op is already an *ordinary* matching terminal,
`finalize_raw_terminal_if_fenced` treats the ledger half as idempotently
satisfied and atomically writes only the matching nonterminal intent (including
the observed op adoption). It does not rewrite that terminal row; a
`repaired=true` terminal still takes its one authoritative `advance`.

`DriverFinished` no longer performs a throwaway durable attempt read before it
removes its registry generation. If its post-removal refresh read faults, it
starts an out-of-actor 25ms-to-1s bounded-backoff recovery task that invokes
the normal durable `reconcile`; successful reconcile exits, and
`ActorStopped`/`ShuttingDown` stop it. Reconcile's existing registry ownership
filter prevents a second owner if another actor turn already attached one.
The focused tests
`service::tests::finished_driver_refresh_fault_recovers_awaiter_ownership_without_external_reconcile`,
`raw_finalizer_second_attempt_converges_ordinary_terminal_ledger_and_awaiting_intent`,
and `raw_finalizer_retries_a_correlation_noop_while_the_same_attempt_awaits`
exited 0 in `/tmp/pr36-final-await-convergence-focused.log`. Temporarily
disabling the exact ordinary-terminal idempotence branch made the second test
exit 101 at its intent-release assertion; the branch was restored
(`/tmp/pr36-final-await-convergence-mutation-red.log`). The exact full gate
`nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
exited 0; its complete log is
`/tmp/pr36-final-await-convergence-full-gate.log`.

Final recovery ownership and world-visibility closure paces a stale
`DriverFinished` ownership-recovery acknowledgement with the same 25ms-to-1s
bounded exponential backoff used for a scan error. The sole worker now remains
coalesced under persistent, per-key post-finish `Journal::get` faults instead
of immediately rescanning durable pending work. The paused-time regression
queues a second such fault between the first scan and its generation
acknowledgement, proves the scan count remains one for 24ms, and admits exactly
one rescanned pass at 25ms. Temporarily removing that sleep made
`service::tests::persistent_finished_read_faults_pace_stale_ownership_recovery_scans`
exit 101 at its paced-scan assertion; the backoff was restored
(`/tmp/pr36-final-recovery-order-mutation-no-backoff.log`).

Recovered clients remain absent from `MultiClient`'s process map until
`complete_recovery` atomically writes the registry and `Done`. The recovery
reservation, `join_lock`, and membership lease cover that await and the
synchronous publication of the exact reopened handle. A completion error or
cancellation while it is pending never publishes a handle; a durable-commit
ambiguity can leave a registry row without a handle; the lease end invalidates
older authority and the scheduler's whole-world check skips money
work until its normal open path recovers safely. The focused ordering regressions pause
completion to prove map absence, then prove successful `Done` plus registry
commit publishes; they also prove explicit failure and cancellation never
publish. A `Succeeded` recovery remains status-masked whenever its process
handle is absent — including after reservation release or restart — and the
mask clears as soon as retry-open restores the handle.

Known-action durability ambiguity is now scoped rather than a process-lifetime
wallet outage. If an awaited Intent/artifact/`MoveRecord` writer reports an
error after the actor captured its action, the actor advances only that
action's balance generations; ambiguous terminal Join/Recover also advances
membership world. Old touching samples are refused, independent work can
continue, and newly sampled facts remain usable. Only a potentially-mutated
write whose affected action is unknowable globally poisons balance authority.
Focused regressions cover actor SetStatus, orphan probe terminalization, raw
artifact, MoveRecord, and Join/Recover world changes; three independent
production mutations failed at the intended scoped-staleness/world assertions
with exit 101 before restoration
(`/tmp/mutation-status-known-action-red.log`,
`/tmp/mutation-artifact-known-action-red.log`, and
`/tmp/mutation-ambiguous-membership-world-red.log`).

The final read-only integration review returned `CLEAN`. The exact workspace
gate exited 0 with 945 passed / 0 failed; complete log:
`/tmp/pr36-final-recovery-ambiguity2-full-gate.log`. The exact-pin isolated
two-federation live tick rebuilt the harness and wallet CLI, joined and awaited
both federations, funded A, performed one allocator-selected A-to-B standby
move (`performed=1 skipped=0 failed=0 retryable=0`), moved A from 2,999,950 to
1,982,862 msat and B from 0 to 999,998 msat, and rejected terminal-occurrence
replay without moving funds. It exited 0; complete log:
`/tmp/pr36-live-tick-recovery-ambiguity-final.log`.

Historical recovery evidence found a standalone `await-move` composition bug
when it used scheduler-bearing `WalletClient::reconcile()`. Public/standalone
recovery uses `reconcile_durable()`, while the scheduler keeps
`reconcile()`. Current membership safety is not a long-lived
Join/Recover-wide conflict fence: preparation and replay can wait off actor;
the short publication lease covers only the final durable registry/client-map
visibility change and invalidates tick authority issued before it. The current
regressions are
`service::tests::membership_admission_invalidates_an_older_tick_world_without_global_blocking`,
which proves a slow membership wait does not globally block tick authority,
and
`multi_client::tests::final_open_publication_invalidates_a_preexisting_tick_token`,
which proves the final publication stales a pre-existing token. The older
recovery observation is historical evidence, not a claim that these focused
tests or a full gate were rerun here.

The final exact-pin live tick was rerun from an `env -i` shell with a stable
operator-owned `TMPDIR`, after an earlier clean Fedimint rebuild lost its
ephemeral `/tmp/nix-shell.*` directory before any wallet assertion. The next
run deliberately reached the real Join/await failure above and was not counted
as a pass. After the recovery split, `/tmp/pr36-live-tick-current.sh` rebuilt
the exact pinned/patched Fedimint harness and current wallet CLI, joined and
awaited two distinct federations, funded A, and completed the allocator-selected
standby move: `performed=1 skipped=0 failed=0 retryable=0`; A changed from
2,999,950 to 1,982,862 msat (−1,017,088), B changed from 0 to 999,998 msat,
and replay of the same terminal occurrence failed without moving funds. The
script exited 0; its complete log is
`/tmp/pr36-live-tick-post-final-3.log`.

After the recovery split and this final evidence update, the exact full
repository gate exited 0 with 880 passed / 0 failed; its complete log is
`/tmp/pr36-final-post-live-reconcile-full-gate.log`.

The final pinned Codex/Opus panel then found six changed-path issues. The
integrated correction now (a) sizes allocator funding, route economics, and
commit validation from the same balance-minus-nonterminal-inbound target
shortfall; (b) replaces the long-lived Join/Recover-wide tick fence with short
membership-publication leases around user join/recover, daemon retry-open, and
auto-join visibility changes; (c) reacquires scheduler authority after a
successful retry-open; (d) lets balance-token failure skip only the tick while
probes, discovery, and deadlines continue; (e) hands a registry slot from a
finished attempt N driver to the already-admitted pending N+1 attempt; and
(f) skips raw terminal-sink actor traffic once the matching intent is already
terminal. Mutation-red tests cover each load-bearing branch, including keyed
concurrent scheduler hooks and service-versus-standalone discovery authority.
The accompanying consistency sweep aligns the routing, reservation, membership,
operation-kind, and historical-anchor documentation.

On that integrated tree, the exact full repository gate exited 0 with 892
passed / 0 failed; its complete log is
`/tmp/pr36-final-panel-fixes-full-gate.log`. The exact-pin live tick was also
rerun from the isolated `env -i` shell after all production corrections. It
joined and awaited two federations, funded A, performed the allocator-selected
standby move (`performed=1 skipped=0 failed=0 retryable=0`), moved A from
2,999,950 to 1,982,862 msat and B from 0 to 999,998 msat, and rejected replay of
the terminal occurrence without moving funds. Its complete log is
`/tmp/pr36-live-tick-final-panel-fixes.log`.

The final external-terminal review removed the remaining wallet-wide invalidation:
the short mutation lease still fails closed while its DB write is live, but on
completion it invalidates balance facts only for the federations named by the
immutable action scope. Both external-terminal and membership-publication leases
are bound to their issuing actor. A durable manual user retry invalidates the same
per-federation balance generations before any fallible handoff. After every actual
scheduler commit attempt, the due-probe source and baseline now come from one fresh
probe sample under the current policy; designation faults fail closed rather than
falling back to a configured pin. Focused mutation tests proved the scoped
generation bump, foreign-lease rejection, retry invalidation, fresh designation,
and fresh-baseline assignments are load-bearing. The final read-only integration
review returned `CLEAN`.

On that integrated tree, the exact full repository gate exited 0 with 901 passed /
0 failed; its complete log is `/tmp/pr36-final-integrated-full-gate.log`. The first
exact live attempt lost Nix's ephemeral `/tmp/nix-shell.*` directory during the
clean Fedimint rebuild and reached no wallet assertion
(`/tmp/pr36-live-tick-external-scope-final.log`, exit 101). An unchanged rerun
completed from the isolated `env -i` recipe: it joined and awaited two federations,
funded A, performed the allocator-selected standby move
(`performed=1 skipped=0 failed=0 retryable=0`), moved A from 2,999,950 to
1,982,862 msat and B from 0 to 999,998 msat, and rejected replay of the terminal
occurrence without moving funds. The rerun exited 0; its complete log is
`/tmp/pr36-live-tick-external-scope-final-rerun.log`.

The final Opus pass then exposed two liveness/classification defects and a probe
policy race. Awaiters now preserve typed retryable versus structural failures:
pre-observation malformed operation identities fail the exact attempt, while any
local correlation/persistence fault after a terminal SDK observation retains
ownership and cannot falsify a successful payment or receipt. Direct-inflow
reattachment reconstructs its derived move cache from the operation log before
classifying missing artifacts, and a destination fault after a settled send stays
nonterminal because the receive outcome remains unknown. The ownership retry,
post-observation prepare/finalize boundaries, lease cleanup, cache reconstruction,
and post-send classification all have mutation-red regressions.

Fresh scheduled probes now carry one actor-issued policy capability through
off-actor sensing and revalidate its actor and non-wrapping policy identity for
each admission. A retained session instead carries its exact durable nonce and
can never fall through to fresh work if it completed or was replaced. A policy
update between designation and admission therefore refuses stale fresh work
without cancelling already-admitted retained money. Mutation tests proved the
policy identity check, per-candidate validation, exact-nonce handoff, detached
driver recheck, and no-fresh-fallthrough branches are load-bearing. The remaining
balance/membership movement during network sensing is a bounded pre-existing
freshness interval: current reservations, caps, driver open-set checks, baseline
resampling, and the no-sweep equality guard remain the money-safety authority.
The final read-only integration review returned `CLEAN`.

On that final tree, the exact full repository gate exited 0 with 921 passed /
0 failed; its complete log is `/tmp/pr36-final-probe-policy-full-gate.log`.
The exact-pin `env -i` live tick also rebuilt the pinned/patched Fedimint harness
and current wallet CLI, joined and awaited two federations, funded A, performed
the allocator-selected standby move
(`performed=1 skipped=0 failed=0 retryable=0`), moved A from 2,999,950 to
1,982,862 msat and B from 0 to 999,998 msat, and rejected replay of the terminal
occurrence without moving funds. It exited 0; the complete log is
`/tmp/pr36-live-tick-probe-policy-final.log`.

### Shutdown reservation absorption (current working tree)

- Fresh user admission, manual retry, generic apply, and user/probe pre-fund recovery retain the
  strict nonterminal Intent projection. Tokenized allocator planning/route pricing/`CommitTick`,
  allocator pre-fund recovery, and exclusive standalone tick use the validated artifact/phase
  projection. `Move` and `DirectInflow` records require exact amount/fee-cap equality; an
  `Evacuate` must use its component-derived cap when available or retain its legacy absolute cap.
  Missing, corrupt, mismatched, oversized, impossible, or missing-required-artifact records fall
  back strict; `Sending` retains destination inbound while absorbing its committed source debit.
- All production raw-operation and `MoveRecord` writers except Runtime's composite direct terminal
  write, including state-machine and backfill writes, use one-shot actor DB commands. A true
  attempt-fenced write bumps the exact action federations before reply; false changes no
  generation. An ambiguous error with a pre-looked-up action conservatively bumps only that action's
  balance generations (and terminal Join/Recover also bumps membership world); only a potentially
  mutated write whose action is unknowable poisons all balance facts. The excluded Runtime composite
  direct terminal write remains protected by its existing short external terminal lease. Runtime
  DirectInflow backfill and every runtime-backed intent driver carry the actor writer client.
- Focused green evidence: `cargo test -p wallet-core --test executor` exited 0 (41 passed);
  the Pay/Move artifact-generation races, commit-before-artifact strict ordering, issued-Pay and
  Sending-Move shutdown planner, corrupt-record fallback, same-key attach, standalone allocator
  apply, ambiguous fresh-upsert folding, impossible sender-artifact fallback, and
  ambiguous-terminal scoped invalidation, orphaned-probe ambiguity, and mutation-unknown
  fold-taxonomy tests
  each exited 0. The final read-only closure review returned `CLEAN`. The full repository gate
  `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
  exited 0 with 938 passed / 0 failed; log:
  `/tmp/pr36-final-reservation-absorption5-full-gate.log`.
- Mutation-red proofs each changed production behavior, failed at the named assertion with exit
  101, and were restored before the green gate: raw Pay artifact absorption
  (`/tmp/mutation-pay-red.log`), Sending source release
  (`/tmp/mutation-sending-red.log`), actor artifact generation
  (`/tmp/mutation-generation-red.log`), shutdown planning
  (`/tmp/mutation-shutdown-red.log`), standalone admission composition
  (`/tmp/mutation-standalone-red.log`), strict user admission
  (`/tmp/mutation-user-strict-red.log`), corrupt amount-bound fallback
  (`/tmp/mutation-fallback-red.log`), phase/artifact coherence
  (`/tmp/mutation-artifact-coherence-red.log`), the historical ambiguous-terminal fail-closed
  behavior (`/tmp/mutation-ambiguous-red.log`, since refined to scoped generation invalidation),
  and same-key attach folding
  (`/tmp/mutation-attach-red.log`), ambiguous fresh-upsert folding
  (`/tmp/mutation-allocator-ambiguous-upsert-red.log`), failed/refunded preimage coherence
  (`/tmp/mutation-send-failed-preimage-red.log`), and DirectInflow sender-artifact rejection
  (`/tmp/mutation-direct-inflow-sender-artifacts-red.log`).
- The exact-pin `env -i` live tick rebuilt the pinned/patched Fedimint harness and current wallet
  CLI after the reservation changes, joined and awaited two federations, funded A, performed the
   allocator-selected standby move (`performed=1 skipped=0 failed=0 retryable=0`), moved A from
   2,999,950 to 1,982,862 msat and B from 0 to 999,998 msat, and rejected replay of the terminal
  occurrence without moving funds. It exited 0; the complete log is
  `/tmp/pr36-live-tick-reservation-absorption-final.log`.

### Committed-send closure (current working tree)

- Once a move has durably persisted its outgoing `send_op`, every local `await_send` lookup,
  decoding, module, or subscription error is retryable. Only an observed Fedimint
  `SendState::Refunded` or `SendState::Failed` may terminalize that attempt. This prevents a
  reconstructed client with incomplete local operation metadata from declaring a still-live
  outgoing contract failed and releasing its reservations.
- The production-path regression covers every typed `AwaitOperationError`, preserves the same
  `Sending` record and `send_op`, and leaves the intent nonterminal. Mutating this branch back to
  `AwaitOperationError::into_exec_error` made the structural-error assertion fail with exit 101;
  the change was restored before the green gate.
- The exact full repository gate exited 0 with 940 passed / 0 failed; complete log:
  `/tmp/pr36-final-c624-fixes-full-gate.log`.
- The first exact-pin live rerun failed before wallet assertions when Nix's ephemeral
  `/tmp/nix-shell.i7ONop` disappeared during a clean compile; complete infrastructure-failure log:
  `/tmp/pr36-live-tick-c624-fixes-final.log`. The unchanged isolated rerun rebuilt the pinned
  harness and current wallet CLI, joined and awaited both federations, funded A, performed one
  allocator-selected A-to-B standby move
  (`performed=1 skipped=0 failed=0 retryable=0`), moved A from 2,999,950 to 1,982,862 msat and B
  from 0 to 999,998 msat, and rejected replay of the terminal occurrence without moving funds.
  It exited 0; complete log: `/tmp/pr36-live-tick-c624-fixes-final-rerun.log`.

### Standing-target credit closure (current working tree)

- Destination cap accounting continues to reserve every outstanding inbound promise, including
  externally paid `Receive` and `DirectInflow`. Standing-target arithmetic now uses a separate
  target-credit projection containing only wallet-delivered `Move`/`Evacuate` value. An unpaid
  invoice therefore cannot suppress a required spending/standby top-up, while it still cannot be
  overfilled past the hard cap.
- The split is used consistently by pure allocation, route pricing, serialized `CommitTick`, and
  same-batch durable/ambiguous reservation folding. Existing-key attach skips fresh target
  admission because it creates no new value, but its original reservation remains counted exactly
  once: the regression admits a 60-msat attach plus 940-msat evacuation and refuses a following
  1-msat evacuation as over cap.
- Mutation proofs changed production behavior and exited 101 before restoration: treating external
  inbound as CommitTick target credit refused the full-gap top-up
  (`/tmp/target-credit-external-separation-mutation-red.log`), and reapplying the fresh-target gate
  to an existing attach refused the exact-sized attach
  (`/tmp/target-credit-attach-gate-mutation-red.log`).
- The final read-only integration review returned `CLEAN`. The exact full workspace gate exited 0
  with 941 passed / 0 failed; complete log:
  `/tmp/pr36-final-target-credit2-full-gate.log`.
- The exact-pin isolated two-federation live tick rebuilt the harness and current wallet CLI, joined
  and awaited both federations, funded A, performed one allocator-selected A-to-B standby move
  (`performed=1 skipped=0 failed=0 retryable=0`), moved A from 2,999,950 to 1,982,862 msat and B
  from 0 to 999,998 msat, and rejected terminal-occurrence replay without moving funds. It exited
  0; complete log: `/tmp/pr36-live-tick-target-credit-final.log`.

### Fresh Agent upsert ambiguity watermark (current working tree)

- The shared fresh Agent allocator-goal path now re-reads its exact idempotency key after a typed
  core storage refusal. A matching durable first Agent intent advances exactly one allocator-goal watermark and
  its affected balance generations; absence proves a pre-upsert failure and advances neither. An
  unreadable reread conservatively advances the known requested identities, while a mismatched row
  poisons tick and balance-facts authority rather than attributing unknown durable work to a request.
  Its internal disposition tells `CommitTick` to fold only requested mutations, leave a definite
  absence unheld, and immediately fail a batch with a mismatched durable identity.
- Focused service tests exited 0 for the direct fresh path, the `CommitTick` allocator-reservation
  path, and the pre-upsert-read/absent-key boundary; the focused `upsert` suite ran seven relevant
  service tests and exited 0 (`/tmp/generation-upsert-focused-final.log`). The direct durable-error
  test terminalizes then dedup-attaches the persisted row, proves its source/destination generations
  are exactly one, and proves the attach adds no generation; the absent-key test proves both its
  generation map and old same-goal token remain unchanged/admissible.
- Mutation-red proofs changed production behavior and exited 101 before restoration: removing the
  matching-row goal bump admitted the duplicate old same-goal decision
  (`/tmp/ambiguous-agent-remove-bump-mutation-red.log`), while blindly bumping the absent reread
  path falsely refused it (`/tmp/ambiguous-agent-blind-bump-mutation-red.log`).
- The follow-up disposition tests prove the `CommitTick` pre-upsert fault does not phantom-fold
  same-goal/source siblings, a readable mismatched row terminalizes the whole tick before later
  work and poisons both future token issuers, and an unreadable reread leaves f1/f2 at one while
  the independently accepted f5/f6 action contributes its admission plus started-driver transitions
  (two each; f3/f4 remain absent). They exited 0 in `/tmp/generation-upsert-focused-final.log` and
  `/tmp/generation-reread-green.log`. Mutation-red proofs each exited 101 before restoration:
  treating absence as requested (`/tmp/disposition-definite-mutation-red.log`), treating a known
  reread failure as definite (`/tmp/disposition-requested-mutation-red.log`), and continuing a
  mismatched-identity batch (`/tmp/disposition-unknown-mutation-red.log`), plus separately adding
  an absent-row balance bump (`/tmp/generation-absent-balance-mutation-red.log`), omitting the
  requested source/destination bump (`/tmp/generation-requested-balance-mutation-red.log`), and
  omitting mismatch poisoning (`/tmp/generation-mismatch-poison-mutation-red.log`).
- Branch-specific follow-up mutations also exited 101 before restoration: removing only the
  unreadable-reread branch's balance bump omitted f1/f2
  (`/tmp/generation-unreadable-reread-balance-mutation-red.log`), and removing only
  `balance_facts_poisoned` while retaining goal poison let the balance-facts issuer succeed
  (`/tmp/generation-mismatch-balance-poison-mutation-red.log`).
- The exact full workspace gate exited 0 with 951 passed / 0 failed; complete log:
  `/tmp/pr36-final-upsert-watermark-full-gate.log`.
- The exact-pin isolated two-federation live tick rebuilt the harness and current wallet CLI, joined
  and awaited both federations, funded A, performed one allocator-selected A-to-B standby move
  (`performed=1 skipped=0 failed=0 retryable=0`), moved A from 2,999,950 to 1,982,862 msat and B
  from 0 to 999,998 msat, and rejected terminal-occurrence replay without moving funds. It exited
  0; complete log: `/tmp/pr36-live-tick-upsert-watermark-final.log`.

### Final Opus journal disposition (current working tree)

- H1 accepted: `record_join_outcome` retains its idempotent already-authoritative `Succeeded`
  fast path, but passes a repaired terminal through `advance` as an authoritative outcome. Thus a
  late real result replaces a repaired `Failed`/`JOIN_SUPERSEDED` conclusion, clears `repaired`,
  and clears the stale failure diagnostic. Immutable non-repaired terminals remain unmodified;
  `Failed` remains non-applied, while authoritative `Succeeded` is idempotently true.
  The focused regression seeds an authoritative old same-fed Join winner and a distinct current
  Executing Join, lets repair soft-fail the current row as superseded, then proves the late current
  result succeeds and that its Intent can converge to `Done`.
- The independent repaired-`Succeeded`/no-op regression instead constructs an intent-backed live
  `Executing` Join, uses `repair_ledger` and the registry evidence to produce its repaired
  `Succeeded` conclusion, then calls `record_join_outcome(key, attempt, false)`. It proves the
  result is true, the row is authoritative `Succeeded`, `repaired` is false, and its error is
  exactly `JOIN_NOOP_REOPEN_NOTE`.
- H2 and H3 are intentional/rejected. They do not change the accepted terminal-authority rule.
- Focused final green: `nix develop -c cargo test -p wallet-fedimint --test ledger
  late_join_outcome_supersedes_repaired_join_superseded_failure` exited 0 (1 passed; 60 filtered)
  in `/tmp/opus-h1-join-supersession-green-head.log`. Disabling the repaired-terminal
  supersession guard made that same test exit 101 at `the real join outcome must supersede a
  defeasible repair`; the guard was restored before the green run
  (`/tmp/opus-h1-join-supersession-mutation-red.log`).
- Independent mutation proof: changing only the first `Succeeded && !repaired` fast-path guard
  to an unconditional `Succeeded` fast path made `noop_join_outcome_supersedes_repaired_join_success`
  exit 101 at `the authoritative no-op outcome must clear the repaired marker`
  (`/tmp/repaired-succeeded-noop-join-mutation-red.log`). The guard was restored and the same
  focused command exited 0 (1 passed; 60 filtered) in
  `/tmp/repaired-succeeded-noop-join-final-green.log`.
- The exact full workspace gate exited 0 with 953 passed / 0 failed; complete log:
  `/tmp/pr36-final-join-supersession2-full-gate.log`.
- The first exact-pin live rerun failed during the pinned Fedimint compile because Nix removed its
  ephemeral `/tmp/nix-shell.G1RgbK` directory before rustc could create temporary files; no wallet
  assertion ran (`/tmp/pr36-live-tick-join-supersession-final.log`). The unchanged isolated rerun
  rebuilt the harness and current wallet CLI, joined and awaited both federations, funded A,
  performed one allocator-selected A-to-B standby move
  (`performed=1 skipped=0 failed=0 retryable=0`), moved A from 2,999,950 to 1,982,862 msat and B
  from 0 to 999,998 msat, and rejected terminal-occurrence replay without moving funds. It exited
  0; complete log: `/tmp/pr36-live-tick-join-supersession-final-rerun.log`.

### rb-lite final-review canonical-successor parser miss (current working tree)

- The rb-lite final-review parser missed that `FedimintJournal::evacuation_supersession` is
  intentionally dual-key: looking up chain-middle B can return its reverse A-to-B predecessor.
  Exact replacement confirmation instead asks only whether attempted parent B gained canonical
  successor C. `evacuation_canonical_successor` now returns `None` when B's canonical row is absent
  regardless of that predecessor, while still validating canonical and reverse halves when it is
  present. Actor and standalone exact-confirmation sites use this strict reader; neighbor and
  presentation APIs remain dual-sided.
- The new journal/runtime production seams build A-to-B, make B a coherent marked Pending parent,
  and inject B-to-C pre-commit and post-commit faults. The pre-commit case proves no C child is
  written and only B's marker is cleared as definitely uncommitted; the post-commit case proves
  B-to-C remains confirmed. Focused green commands exited 0:
  `nix develop -c cargo test -p wallet-fedimint
  journal::replacement_foundation_tests::supersession_neighbors_keep_both_links_for_a_replaced_replacement
  -- --exact`,
  `nix develop -c cargo test -p wallet-fedimint
  runtime::tests::standalone_replacement_confirmation_ignores_a_middle_parents_predecessor -- --exact`,
  and `nix develop -c cargo test -p wallet-fedimint
  service::tests::structural_evacuation_marker_is_atomically_replaced_by_one_fresh_planned_child
  -- --exact`.
- Mutation evidence: changing standalone confirmation back to the dual-key reader made the
  standalone production test exit 101 at its exact uncommitted-outcome assertion, reporting that
  confirmation was ambiguous after the injected pre-commit error. The strict reader was restored.
  After restoration, the exact workspace gate `nix develop -c bash -c 'cargo fmt --check && cargo
  clippy --workspace --all-targets -- -D warnings && cargo test --workspace'` exited 0; complete
   output is `/tmp/rb-lite-canonical-successor-full-gate.log`. Final diffcheck follows this record.

### Opus final P2/P3 recovery-only marker redrive and daemon-status runbook

- Reconciliation now carries an internal typed marker disposition: public durable recovery preserves
  planner-owned markers, a healthy scheduler pass captures one exact parent for its planner, and a
  scheduler pass that has already committed not to plan uses recovery-only redrive. The latter drops
  only its exact parked in-memory snapshot/handoff, claims the old Pending work through the normal
  `Pending -> Executing` CAS (which consumes evidence atomically), and never directly clears durable
  marker evidence. Ambiguous goal-admission poison instead preserves the exact marker and starts no
  driver for it.
- The recovery claim arms the existing one-shot marker-wake suppression before its driver can renew a
  structural refusal. Thus a confirmed partial/open-view, unreadable-floor, or tail/storage-fenced
  cycle cannot tight-loop on `policy_wake`; the next healthy pass captures the renewed marker for
  normal replacement planning. A valid zero-unreadable bounded watch backlog is different: it returns
  the immediate typed retry before any reconcile/driver, preserving the exact marker for its next
  prompt healthy planner pass. Scheduler coverage includes partial joined views, the preserved
  exhausted valid watch-floor batch followed by a whole-view replacement sidecar, and a prior parked
  handoff behind an unreadable floor, all without an ineligible fresh Tick.
- `/v1/status` documentation now states its operational prerequisites and side effect precisely:
  live `Runtime` and `MultiClient`, every joined federation open, reconciled `get_watch_state`, checked
  successor (`MAX-1` previews `MAX`; `MAX` is 503), and live probes. The real-sats runbook also calls
  out that status is money-dry but `get_watch_state` is a bounded migration writer: one status request
  can advance a valid batch and then return 503 directing the operator to `/v1/watch/status`.
- Focused green evidence: all 23 scheduler tests exited 0 in
  `/tmp/opus-p2-scheduler-tests.log`; the recovery wake test and poisoned-marker recovery test exited
  0 in `/tmp/opus-p2-recovery-test.log` and `/tmp/opus-p2-poison-test.log`. Mutation evidence:
  preserving recovery-only marker work made the recovery assertion fail with EXIT=101 at
  `left: 0 / right: 1` (`/tmp/opus-p2-mutation-recovery-skip-red.log`); removing its wake
  suppression made the no-immediate-wake assertion fail with EXIT=101
  (`/tmp/opus-p2-mutation-wake-suppression-red.log`). Reviewer follow-up mutations also proved that
  redriving during a valid bounded backlog fails its no-reconcile assertion with EXIT=101
  (`/tmp/opus-p2-review-mutation-valid-backlog-red.log`) and that removing the poisoned-marker guard
  fails `recovery.redriven == 0` with EXIT=101
  (`/tmp/opus-p2-review-mutation-poison-guard-red.log`). All mutations were restored.
- The exact workspace gate `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace
  --all-targets -- -D warnings && cargo test --workspace'` exited 0 with 1086 passed / 0 failed;
  complete output is `/tmp/opus-p2-review-final-workspace-gate.log`.

### History effective-cap cursor pagination P3 regression (current working tree)

- The in-process daemon endpoint test seeds 501 typed `Tick` ledger rows, requests
  `/v1/history?limit=507`, and proves that the first response contains precisely the newest 500
  sequence values, that `next_before_seq` is its final sequence value, and that following the
  cursor returns the remaining older values without a gap, duplicate, or ordering loss.
- The focused daemon command `nix develop -c cargo test -p wallet-daemon
  tests::history_cursor_uses_the_effective_capped_limit_without_losing_rows -- --exact` exited 0
  (1 passed) in `/tmp/history-cap-focused-green.log`. The strict relevant command `nix develop -c
  bash -c 'cargo test -p wallet-daemon
  tests::history_cursor_uses_the_effective_capped_limit_without_losing_rows -- --exact && cargo
  fmt --check && cargo clippy -p wallet-daemon --all-targets -- -D warnings'` exited 0 in
  `/tmp/history-cap-focused-fmt-clippy-green.log`.
- Mutation evidence: changing the cursor fullness condition from the effective capped `limit` to
  the requested `query.limit.unwrap_or(50)` made that exact focused test exit 101 at `a full
  effective page has a cursor`; the production condition was restored
  (`/tmp/history-cap-cursor-requested-limit-mutation-red.log`).
- The exact full workspace gate `nix develop -c bash -c 'cargo fmt --check && cargo clippy
  --workspace --all-targets -- -D warnings && cargo test --workspace'` exited 0; complete output:
  `/tmp/history-cap-full-workspace-gate.log`.

### Historical final CodeRabbit cleanup disposition (before `br-n8o`)

- Accepted: document `Awaiting` as subscription/external-payment work that reconcile does not
  re-perform; correct the route-outage distinction and current destination-list/both-end
  `routing_info` rule; remove the executor test's inert phase loop and duplicate projection; and
  assert every probe fixture's attempt-fenced move-record seed.
- Rejected without code change: there is no live allow-over-cap CLI; archived Q4 is already
  struck; DirectInflow conflict suppression is unreachable; and recovery `Done` → `Failed` is
  fenced.
- Focused verification: the strict executor test exited 0 (1 passed) in
  `/tmp/coderabbit-cleanup-wallet-core-executor.log`; all 11 `probe_runtime` tests exited 0 in
  `/tmp/coderabbit-cleanup-probe-runtime.log`; fmt plus the two relevant clippy targets exited 0
  in `/tmp/coderabbit-cleanup-fmt-clippy.log`.
- The exact final workspace gate exited 0 with 953 passed / 0 failed; complete log:
  `/tmp/pr36-final-coderabbit-cleanup-full-gate.log`.

## Next

1. Outer driver owns the PR, merge, and `br-n8o` closure; do not perform them in this run.
2. `br-evac-cap-driven-basis-v07`: add `TestRoute::with_recv_fed_fee`; use
   separate driven fixtures for the literal delivered-vs-ask refusal band
   before `mc.receive` and for successful `MoveMeta.fee_cap == cap(delivered)`.
3. `br-vvo`: make the live smoke discriminate against the old absolute cap,
   pin the post-revalidation refusal arm, and cover the receive-side PPM
   warning.
4. Stop before `br-recanary-y2j-ujs` and present the proven release to the
   operator; this drive does not spend real sats.

## Open questions for the human

- none
