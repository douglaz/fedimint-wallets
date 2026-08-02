# DRIVE — close out the stranded-move / lnv2-claim thread

**Scope:** the stranded-move thread only — PR #26 (landed) and
`br-adopt-lnv2-claim-retry-d3d`. NOT the wallet-web epic (br-sol, br-t8f, br-ucq, br-pfc,
br-5om, br-4yz, br-nfz), NOT the evacuation beads (br-y2j, br-s0e), NOT the production
canary (br-prod-canary-nab). Those are ready in `br` but out of this drive's boundary.

**Phase:** BUILD · **Bead:** br-adopt-lnv2-claim-retry-d3d · **Branch:** tbd
**Gate:** `nix develop /home/master/p/fedimint -c bash -c '<cmd>'` — the flake path is
required; bare `cargo` fails on missing cmake. Last green 2026-08-02 on 4971b9f (fmt 0 /
clippy -D warnings 0 / test 0, 715 passed / doc 0).

## Done
- PR #26 stranded diagnostics + operator procedure — squash-merged as `4971b9f`.
  Reviewed by `multi-reviewer-loop` (CLEAN: both reviewers + consistency pass on one
  tree), codex bot `+1` on the final SHA, CodeRabbit SUCCESS.
- Secret gist recording the lnv2 claim-retry gap, adversarially reviewed by codex + fable
  and corrected twice: https://gist.github.com/douglaz/22198e186de46a509d962a366a9777da

## Now
`br-adopt-lnv2-claim-retry-d3d` (P2) — adopt fedimint#8935 (lnv2 claim retry +
`reclaim_receive`) and rewrite the Stranded operator procedure.

First step is evidence-gathering with no external blast radius: cherry-pick #8935 onto
`wallet-pin/iroh-recovery-tpe8838` locally, bump the pin, run the wallet gate and the lnv2
integration suite ourselves. Do NOT push the fork branch or the pin bump until the
fork-patch decision below is settled.

Budget: Cargo.lock + at most the executor's receive-state read. Do NOT build: a general
recovery framework, new CLI surface beyond what #8935 already ships, or any change to
`MovePhase::Stranded` itself.

## Open questions for the human
- **fedimint#8935 is OPEN, not merged.** Adopting now means carrying a FOURTH fork-only
  patch on money-path receive code (we already carry tpe lagrange, module-recovery
  reporting, iroh long-poll). The bead's own preferred resolution is to wait for it to
  merge upstream so a single pin bump serves this and br-jga. Recommendation: gather the
  gate evidence locally now, decide after.
- **Scope was inferred, not given.** `/drive` was invoked with no goal. If the intent was
  to drain the wider backlog, say so and I will widen `Scope:` above.
