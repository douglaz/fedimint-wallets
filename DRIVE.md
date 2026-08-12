# DRIVE — remove the critical safety and verification blockers found in independent review

**Scope:** the critical corrective thread ONLY:
`br-devimint-runbook-mint-na3` → `br-p93` → `br-n8o`, followed by the
discriminating evacuation-cap gates already tracked in
`br-evac-cap-driven-basis-v07` and the applicable `br-vvo` coverage.
Bookkeeping required to make that sequence honest is also in scope: make
`br-p93` cover the production scheduler as well as standalone `watch_once`, and
make `br-p93`/`br-n8o` block `br-recanary-y2j-ujs`.

NOT in scope for this drive: running the production re-canary (moves real sats
and remains an operator decision), shipping the Phase 6c web feature chain,
Phase 7 seed encryption, the two-gateway evacuation fallback, Android, or the
rest of the repository backlog. Those remain next-step recommendations, not
silent scope expansion.

**Phase:** HARDEN · **Bead:** `br-devimint-runbook-mint-na3`
· **Branch:** `br-devimint-runbook-mint-na3`
· **Pending:** PR `douglaz/fedimint-wallets#35` follow-up panel and bot round
**Gate:** `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
· baseline on `main` `097e461`: EXIT=0, 792 passed / 0 failed (2026-08-12)
· GitHub CI on `097e461`: success, run 31552972223
· branch gate: EXIT=0, 792 passed / 0 failed (2026-08-14)
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

- Read the current project, canonical docs/ADRs, code architecture, current
  backlog and recent delivery evidence.
- Two independent strategy reviews completed. Both found the re-canary unsafe
  to start before the live-gate repair and the two P1 liveness fixes.
- Verified a production-path omission in `br-p93`: the daemon scheduler gates
  all tick commits on global `ReconcileReport.redriven == 0` in
  `wallet-fedimint/src/service/scheduler.rs::tick_may_commit`; fixing only
  `Runtime::watch_once` would leave production unchanged.
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

## Now

`br-devimint-runbook-mint-na3`: BUILD and PROVE are complete, including the
documented default §1 worktree provisioning, corrected wallet build, exact
release invocation, and live §2 evacuation smoke from a clean environment. PR
`douglaz/fedimint-wallets#35` is in its follow-up review round. No wallet
production code changed; executable smoke edits are fail-fast missing-binary
diagnostics plus portable resolution of the responsiveness test's
`hang_gateway.py` helper. They do not change assertions or money behavior.

## Next

1. `br-p93`: implement its now-amended contract with red-first independent-
   and same-intent fixtures on both daemon scheduler and standalone paths,
   adversarial review, and live devimint.
2. `br-n8o`: serialized pre-artifact supersession, preserved and linked audit
   identity, replay consistency, and a concurrent receive-commit race gate.
3. `br-evac-cap-driven-basis-v07`: add `TestRoute::with_recv_fed_fee`; use
   separate driven fixtures for the literal delivered-vs-ask refusal band
   before `mc.receive` and for successful `MoveMeta.fee_cap == cap(delivered)`.
4. `br-vvo`: make the live smoke discriminate against the old absolute cap,
   pin the post-revalidation refusal arm, and cover the receive-side PPM
   warning.
5. Stop before `br-recanary-y2j-ujs` and present the proven release to the
   operator; this drive does not spend real sats.

## Open questions for the human

- none
