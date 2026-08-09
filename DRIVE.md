# DRIVE — make an evacuation executable at real fee shapes (br-y2j)

**Scope:** the evacuation fee-cap thread ONLY — `br-y2j` and its three children
`br-evac-cap-policy-r3n` (1/3), `br-evac-cap-enforce-vn6` (2/3),
`br-evac-cap-ledger-x9k` (3/3). NOT the wallet-web epic (br-nfz, br-5om, br-t8f,
br-ucq, br-pfc, br-4yz), NOT br-2aa, NOT br-s0e, NOT the production canary.

**Phase:** HARDEN · **Bead:** `br-evac-cap-enforce-vn6` ·
**Branch:** `feat/br-evac-cap-enforce-vn6`
**Pending:** —
**Gate:** `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
· workspace gate green at `1da1981` (fmt 0, clippy 0, **789 passed / 0 failed**, EXIT=0)
· live devimint evacuation gate green at `0cb6b2e`, EXIT=0 — a DIFFERENT commit, and the two
  hashes are written out because they diverge. `1da1981` adds no logic: the "probed" qualification
  on five diagnostic strings, this file, and a bead description. The live gate has run green at
  seven commits on this branch, and NONE of them exercised the refusal diagnostics (see NOT proven).

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
**2/3 `br-evac-cap-enforce-vn6`** — the money change. PR #31.

**The shape, since per-round detail lives on the bead.** Rounds 2–4 each found a different
consumer of ONE undefined concept — *which net does the cap bind to* — and round 2's fix caused
round 4's finding. Round 5 was therefore a DESIGN PASS rather than a fifth patch: every fee cap
now computes from the **delivered net**, derived on the types that maintain the invariant, and
`fits_cap`/`combined_verdict` take no caller-supplied amount at all.

**Then it happened a second time, and the second time is the more instructive one.** Three
consecutive rounds each found a different way for ONE cached value to go stale: a conditional
write, an early return that skipped the write, and pass 2 re-probing the very amount the cache
named. Each was fixed at its own site. The fourth site would have been found the same way, because
the defect was never any single write: `no_fitting_amount_reason` measured a fee-vs-cap SLOPE
between a floor quoted at diagnosis time and a high point quoted earlier during the search, and
then used that slope to recommend moving a real-money knob. A slope across two epochs is not a
trend, and no amount of write-site discipline makes two epochs contemporaneous.

The fix (`7167ed8`) deletes the cache rather than guarding it: `largest_affordable` became
`largest_affordable_hint: Option<Msat>`, carrying an amount and no cost, and the diagnostic quotes
BOTH points back to back. A stale hint is now harmless by construction. The framing that settled
it: the execution path already refuses to act on anything but a fresh quote, so holding the
operator-facing diagnosis to a WEAKER freshness standard than the money path holds itself to was
the actual defect.

**PANEL HEALTH — the last four rounds were codex-only.** The Claude Fable reviewer produced its
last verdict at `7167ed8`; three attempts since then failed without reviewing (twice wedged with no
output for 20–50 minutes, once `error_during_execution` after two turns), including one with a
deliberately shortened prompt, so it is not prompt size. Those rounds are DEGRADED: one reviewer is
not a panel, and nothing here should be read as two independent reads. What the single reviewer did
establish is recorded below.

### Proven, and by what
- The cap is evaluated at the delivered net: direct unit tests on `fits_cap` and
  `combined_verdict`.
- Both search admissions revalidate their final re-quote: `pass_one_revalidates_its_final_requote`
  and `pass_two_revalidates_its_final_requote`, each red against its own guard alone.
- The reservation cannot overflow a saturating cap: the allocator golden, red with
  "attempt to add with overflow".
- No fee-knob recommendation can rest on evidence from an earlier epoch: the cached cost is gone
  from the type, so the property holds structurally rather than by write-site discipline. Each
  diagnostic guard has its own emitted reason and its own test.
- The path executes end to end on two live federations including a hair-under delivery: the
  devimint evacuation gate, EXIT=0.

### NOT proven, stated so nobody infers otherwise
- The **pre-mint gate** and the **executor recompute** have no test pinning their delivered-net
  basis. Owner: `br-evac-cap-driven-basis-v07` (NOT `br-4yz` — that is the wallet-web route
  manifest bead, and a reviewer caught me misattributing this debt to it twice).
- The **live gate cannot distinguish the delivered-net basis from the ask basis**: in that
  fixture the two caps differ by 2 msat against ~199_287 msat of headroom, so it passes under
  either. It proves execution, not the basis.
- **The live gate does not exercise the refusal diagnostics at all.** Every diagnostic change on
  this branch lives on paths the smoke never enters, because the smoke drives a SUCCESSFUL
  evacuation. Its green is a regression check, not evidence for those changes.
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
- none. `br-devimint-runbook-mint-na3` is a filed defect, not an open gate: the runbook's
  documented invocation omits `FM_ENABLE_MODULE_MINT=1` and every smoke dies at
  `Primary module not available` without it.
