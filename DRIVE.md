# DRIVE — make an evacuation executable at real fee shapes (br-y2j)

**Scope:** the evacuation fee-cap thread ONLY — `br-y2j` and its three children
`br-evac-cap-policy-r3n` (1/3), `br-evac-cap-enforce-vn6` (2/3),
`br-evac-cap-ledger-x9k` (3/3). NOT the wallet-web epic (br-nfz, br-5om, br-t8f,
br-ucq, br-pfc, br-4yz), NOT br-2aa, NOT br-s0e, NOT the production canary.

**Phase:** HARDEN · **Bead:** `br-evac-cap-enforce-vn6` ·
**Branch:** `feat/br-evac-cap-enforce-vn6`
**Pending:** —
**Gate:** `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
· last green at `b18496c` (fmt 0, clippy 0, **777 passed / 0 failed**, EXIT=0)
· live devimint gate re-run at the SAME commit: EXIT=0

Supersedes the stranded-move drive, which was stopped for a retrospective and whose scope
excluded these beads.

## Done
- `/v1/recover` carved out of the web sidecar (ADR-0028 amendment + plan §6c.3) — `9f1f23b`
- Five untracked gaps filed as beads; br-s0e and br-remove-gateway-pin-yjw demoted — `9f1f23b`
- `br-y2j` decomposed along its deployability seam — `f107c5a`
- **1/3 `br-evac-cap-policy-r3n`** Policy knobs + validation + propagation, no
  enforcement — merged PR #30 (`7d5e69f`). rb-lite clean in 3 rounds, full 3-reviewer
  panel, 742 tests, CodeRabbit no actionable comments.

## Now
**2/3 `br-evac-cap-enforce-vn6`** — the money change. PR #31, HEAD `b18496c`.

FIVE review rounds. The per-round detail lives on the bead; what matters here is the shape:
every round found a different consumer of ONE undefined concept — *which net does the cap bind
to* — and round 2's fix caused round 4's finding. That is the cross-cutting tell, so round 5 is a
DESIGN PASS rather than a fifth patch (`8c0831c`, `b18496c`):

- Every fee cap now computes from the **delivered net** (`invoice − receive_quote`), derived on
  `GrossUp::delivered_net` and `FreshMoveCost::delivered_net` — the types that maintain the
  invariant. `fits_cap` and `combined_verdict` take no caller-supplied amount at all, which is
  what stops a sixth site inventing its own answer.
- `CONTEXT.md` now defines **sized ask** and **delivered net** and retires "executed net", which
  read as the first to one author and the second to another. That ambiguity is the whole defect.
- The structural-refusal slope was RE-DERIVED in delivered space (`dC/dD = bps/10_000` holds
  there and only there), not substituted — codex predicted that trap and a reviewer confirmed I
  had fallen into it.

Decision evidence: fable and codex analysed the sized-ask-vs-delivered-net question
independently and both chose delivered, both said do not land without it.

**Gates at `b18496c`:** workspace EXIT=0, 777 passed / 0 failed. Live two-federation devimint
evacuation gate EXIT=0 (`performed=1 failed=0 retryable=0`, A 499998 → 35870, B netted 449918).

**What that live gate does NOT prove, stated because I claimed otherwise once.** The `fee_cap
213499` it prints is the DECISION-time cap, computed at plan time on the planned amount —
`decide()` has no quote yet, so no delivered net exists there. In that run sizing did not
downsize, so the two candidate bases differed by 2 msat and the fee had 199,287 msat of headroom
under either. The gate proves the knobs are in force and the path executes end to end; the seam
invariant is proven by the unit test instead, red-first.

**Owed, not silently missing:** the pre-receive gate and the executor's recompute have no test
pinning their basis — a driven `drive_intent_step` fixture against a mocked multi-client is what
would cover them, and `br-4yz` is where that harness belongs.

## Next
`br-evac-cap-ledger-x9k` (3/3) — the ledger reporting the enforced cap and executed
amount, plus runbook and README. Then `br-y2j` closes, which unblocks
`br-recanary-y2j-ujs`.

## Open questions for the human
- none. The live devimint gate is not outstanding: it ran at `b18496c` and exited 0 (see the
  gate line at the top). What remains from that work is a filed defect, not an open gate —
  `br-devimint-runbook-mint-na3`, because the runbook's documented invocation omits
  `FM_ENABLE_MODULE_MINT=1` and every smoke dies at `Primary module not available`.
