# DRIVE — close out the stranded-move / lnv2-claim thread

**Status: STOPPED at the user's request for a retrospective.** Resume by sweeping this
file into the next branch — do not commit it on its own (see the LAND rule added to the
drive skill in douglaz/skills#16).

**Scope:** the stranded-move thread only — PR #26 (landed) and
`br-adopt-lnv2-claim-retry-d3d`. NOT the wallet-web epic, the evacuation beads
(br-y2j, br-s0e), or the production canary (br-prod-canary-nab).

**Phase:** BUILD (evidence gathered, decision pending) · **Bead:**
`br-adopt-lnv2-claim-retry-d3d` · **Branch:** none yet — work is in a scratch worktree
**Gate:** `nix develop /home/master/p/fedimint -c bash -c '<cmd>'` — the flake path is
required; bare `cargo` fails on missing cmake.

## Done
- PR #26 stranded diagnostics + operator procedure — squash-merged `4971b9f`.
- Bead + first DRIVE.md — `25fc11f`, pushed direct to main. **This was the mistake**
  that produced douglaz/skills#16: unreviewed commit on the default branch.
- main verified healthy post-merge: fmt 0 / clippy 0 / test 0, **715 passed, 0 failed**.
- fedimint#8935 cherry-picked onto our pin in a scratch worktree at
  `<scratch>/fedimint-wt`, commit `eaa35c03067` on top of `72b1e5be`.

## Evidence gathered for br-adopt-lnv2-claim-retry-d3d
- **Applies cleanly.** All three source files (`cli.rs`, `lib.rs`, `receive_sm.rs`) took
  the cherry-pick with no conflict. The single conflict was an import line in
  `modules/fedimint-lnv2-tests/tests/tests.rs`, caused by `InvoiceSendStatus` — a symbol
  from unrelated upstream commit `c270a790569` that our pin predates. Its call sites are
  in pre-existing upstream tests, not the ones #8935 adds, so dropping it from the import
  list is the correct resolution rather than a workaround.
- **Compiles at our pin.** `cargo check -p fedimint-lnv2-client` → 0;
  `cargo check -p fedimint-lnv2-tests --all-targets` → 0. Zero errors. The 8 warnings are
  pre-existing dead-code in `api.rs` (untouched by the cherry-pick) plus nix noise.
- **The three tests #8935 adds pass at our pin**, all in-process, no devimint needed:
  - `receive_sm::tests::decodes_legacy_receive_states` — ok (byte-for-byte legacy encoding)
  - `funded_receive_is_claimed` — ok
  - `reclaim_receive_recovers_parked_claim` — ok ← the retroactive-recovery claim, verified

## Next
Decide the fork-patch question below, then: bump the pin, run the wallet gate, teach the
executor to read the recorded receive SM state, rewrite the runbook's stranded entry.

## Open questions for the human
- **fedimint#8935 is OPEN, not merged.** Adopting now means a FOURTH fork-only patch on
  money-path receive code. Evidence above says it applies and passes cleanly, so the
  technical risk is low; the cost is divergence from upstream, against br-jga. The bead's
  preferred resolution — wait for it to merge, then one pin bump serves both — still looks
  right now that we know the cherry-pick is clean and can be redone at will.
- **The conflict is a staleness signal.** Our pin is ~10 upstream commits behind #8935's
  base. That gap will only grow.
