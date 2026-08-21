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

**Phase:** HARDEN · **Bead:** `br-n8o`
· **Branch:** `chore/close-br-p93`
· **Pending:** land the merged `br-p93` closeout, then implement the serialized
  pre-artifact evacuation replacement
**Gate:** `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
· current final-fixes tree: EXIT=0, 953 passed / 0 failed (2026-08-17);
  complete gate log: `/tmp/pr36-final-coderabbit-cleanup-full-gate.log`
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

- Merged the conflict-scoped scheduler/standalone gate as PR #36
  (`75802dfa898fcde45b8cf0102c30a7b1fc9fe281`), after the final 953-test
  workspace gate, exact-pin two-federation live tick, and GitHub devshell/package
  jobs all exited 0. `br-p93` is closed; `br-n8o` is claimed as the next P1.
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

### Final CodeRabbit cleanup disposition (current working tree)

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

1. `br-n8o`: serialized pre-artifact supersession, preserved and linked audit
   identity, replay consistency, and a concurrent receive-commit race gate.
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
