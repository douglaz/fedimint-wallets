# DRIVE — make an evacuation executable at real fee shapes (br-y2j)

**Scope:** the evacuation fee-cap thread ONLY — `br-y2j` and its three children
`br-evac-cap-policy-r3n` (1/3), `br-evac-cap-enforce-vn6` (2/3),
`br-evac-cap-ledger-x9k` (3/3). NOT the wallet-web epic (br-nfz, br-5om, br-t8f,
br-ucq, br-pfc, br-4yz), NOT br-2aa, NOT br-s0e, NOT the production canary.

**Phase:** HARDEN · **Bead:** `br-evac-cap-enforce-vn6` ·
**Branch:** `feat/br-evac-cap-enforce-vn6`
**Pending:** —
**Gate:** `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
· last green at `0d1f881` (fmt 0, clippy 0, **780 passed / 0 failed**, EXIT=0)
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
**2/3 `br-evac-cap-enforce-vn6`** — the money change. PR #31, reviewed HEAD `0d1f881`.
**TEN review rounds. Nine money defects found and fixed, none of which the test suite caught.**
Both reviewers returned MERGE NOW on the frozen tip, with no P0/P1/P2 code findings.

The shape, since the per-round detail lives on the bead: rounds 2–4 each found a different
consumer of ONE undefined concept — *which net does the cap bind to* — and round 2's fix caused
round 4's finding. Round 5 was therefore a DESIGN PASS rather than a fifth patch: every fee cap
now computes from the **delivered net**, derived on the types that maintain the invariant, and
`fits_cap`/`combined_verdict` take no caller-supplied amount at all. Rounds 6–10 were that design
absorbing its stragglers, plus repeated repair of my own test-shape mistakes.

### Proven, and by what
- The cap is evaluated at the delivered net: direct unit tests on `fits_cap` and
  `combined_verdict`.
- Both search admissions revalidate their final re-quote: `pass_one_revalidates_its_final_requote`
  and `pass_two_revalidates_its_final_requote`, each red against its own guard alone.
- The reservation cannot overflow a saturating cap: the allocator golden, red with
  "attempt to add with overflow".
- The path executes end to end on two live federations including a hair-under delivery: the
  devimint evacuation gate, EXIT=0.

### NOT proven, stated so nobody infers otherwise
- The **pre-mint gate** and the **executor recompute** have no test pinning their delivered-net
  basis. Owner: `br-evac-cap-driven-basis-v07` (NOT `br-4yz` — that is the wallet-web route
  manifest bead, and a reviewer caught me misattributing this debt to it twice).
- The **live gate cannot distinguish the delivered-net basis from the ask basis**: in that
  fixture the two caps differ by 2 msat against ~199_287 msat of headroom, so it passes under
  either. It proves execution, not the basis.
- `TestRoute` has no `with_recv_fed_fee`, so **no composed async fixture can produce
  `delivered != ask`** at all. Same owner.

### Two properties a future editor cannot infer from the tests themselves
- The seam coverage is a property of the **pair**: drift deleting the pass-1 admission would
  still satisfy the 449_999 sightings pin, because pass 2's bisection also touches it; that shape
  is caught by `the_increasing_regime_finds_the_top_window` instead.
- The pinned amounts (449_999, 339_997) are coupled to `largest_fitting_amount`'s mid arithmetic.
  If the stepping changes these fail with the sightings map printed — that means "re-derive the
  settled amount", not "the guard broke".

## Next
`br-evac-cap-ledger-x9k` (3/3) — the ledger reporting the enforced cap and executed
amount, plus runbook and README. Then `br-y2j` closes, which unblocks
`br-recanary-y2j-ujs`.

## Open questions for the human
- none. The live devimint gate is not outstanding: it ran at `b18496c` and exited 0 (see the
  gate line at the top). What remains from that work is a filed defect, not an open gate —
  `br-devimint-runbook-mint-na3`, because the runbook's documented invocation omits
  `FM_ENABLE_MODULE_MINT=1` and every smoke dies at `Primary module not available`.
