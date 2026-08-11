# DRIVE — make an evacuation executable at real fee shapes (br-y2j)

**Scope:** the evacuation fee-cap thread ONLY — `br-y2j` and its three children
`br-evac-cap-policy-r3n` (1/3), `br-evac-cap-enforce-vn6` (2/3),
`br-evac-cap-ledger-x9k` (3/3). NOT the wallet-web epic (br-nfz, br-5om, br-t8f,
br-ucq, br-pfc, br-4yz), NOT br-2aa, NOT br-s0e, NOT the production canary.

**Phase:** DONE — the scope above is empty. `br-y2j` and all three children are closed.
**Branch:** — · **Pending:** this closure PR
**Gate:** `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
· 3/3 merged as `50e25e9` (PR #32). Workspace gate green on its final pre-merge tree —
  fmt 0, clippy 0, **792 passed / 0 failed**, EXIT=0 — and `nix build` green on `main` at
  `50e25e9` after the merge.
· **CI is PENDING, not present.** This repo has no `.github/workflows/` in this tree; every
  gate cited above ran locally. A workflow adding fmt + clippy + test in the devshell, plus
  `nix build` of `walletd`, `wallet-cli` and the deployment image, is open in its own PR and
  NOT merged. Do not read the gates above as CI-enforced until it lands.
· live devimint evacuation gate green nine consecutive times across 2/3's branch, last at its
  final pre-merge tree. Those branch SHAs are NOT reachable from `main`: PR #31 was squash-merged,
  so `e9cc97d` is the only hash a future reader can resolve. Do not cite the branch hashes.

**What is NOT closed by this, and must not be inferred from it.** `br-recanary-y2j-ujs` is now
READY — the production re-canary against real sats. It is deliberately outside this drive's scope
and is the operator's call, not a step to be taken because the beads went green. Read the two open
P1s below BEFORE treating `ready` as a recommendation: `br` reports readiness from the dependency
graph, which knows nothing about them.

**TWO OPEN P1s ON THIS PATH, and they qualify the closure above.** The scope is empty because
its beads closed; that is not the same as the evacuation path being free of known serious
defects, and an operator weighing the re-canary needs both of these in front of them:

- **`br-n8o` — a structural refusal cannot be released by any operator action.** The refusal is
  decided against the `(base, bps)` the intent was ADMITTED with, so raising the policy knobs
  reaches only evacuations decided afterwards. The existing intent retries forever on the old
  parameters, and funds stay on the shutting-down federation.
- **`br-p93` — one retryable intent suppresses ticks for every federation.** A single pending
  retryable intent makes `reconcile` report `retryable > 0`, which skips the tick GLOBALLY, so
  every other federation stops receiving allocator decisions too.

Together they are the failure this drive's own work can still produce: an evacuation that refuses
structurally, cannot be un-stuck by changing policy, and quietly stops the wallet deciding
anything anywhere. Neither was in scope here; both were filed during 2/3 and remain open.

Also deferred, lower severity:
`br-w6p` (the enforced cap is invisible from a live daemon — only `--standalone show` prints it),
`br-v8x` (a receive refused AFTER committing freezes the planned pair on a terminal row),
`br-h34` (nine tracked docs name assistant tooling, against the workspace convention),
plus the pre-existing `br-evac-cap-driven-basis-v07`, `br-cqv`, `br-u4i`, `br-vvo`, `br-7xc`.

Supersedes the stranded-move drive, which was stopped for a retrospective and whose scope
excluded these beads.

## Done
- `/v1/recover` carved out of the web sidecar (ADR-0028 amendment + plan §6c.3) — `9f1f23b`
- Five untracked gaps filed as beads; br-s0e and br-remove-gateway-pin-yjw demoted — `9f1f23b`
- `br-y2j` decomposed along its deployability seam — `f107c5a`
- **1/3 `br-evac-cap-policy-r3n`** Policy knobs + validation + propagation, no
  enforcement — merged PR #30 (`7d5e69f`). rb-lite clean in 3 rounds, full 3-reviewer
  panel, 742 tests, CodeRabbit no actionable comments.

## Done — 3/3
**3/3 `br-evac-cap-ledger-x9k`** — merged as `50e25e9` (PR #32). The ledger now reports the cap
it ENFORCED and the amount it EXECUTED, not the pair it planned.

*The defect it fixed, in the past tense so nobody reads it as current:* a clamped row USED TO keep
the planned figures for life, so a post-incident fee audit WOULD HAVE cleared fees the enforced cap
had refused.

That is fixed **for rows carrying committed-leg evidence**, which is the deliberate boundary — see
the draft-row gate below. It is NOT a universal claim about rows written after `50e25e9`: the
`br-v8x` path (a receive that COMMITS and is then refused by the §15.7 contract check before
`invoice`/`recv_op` are persisted) still terminalizes holding the PLANNED pair. An auditor must
not read every post-`50e25e9` row's pair as enforced.

The bead — and 2/3's ADR text — NAMED `wallet-cli history` as that audit surface. **Checked: wrong
command.** `history_tsv` (`wallet-cli/src/main.rs:2921`) emits amount, receive_fee and
send_fee_quoted and has NO cap column; `print_show_record` (`:2953-2954`) prints `amount_msat` and
`fee_cap_msat` adjacent. `show` is where the false pair is visible and where the fix pays off. The
motivation is unchanged — the durable row was wrong either way — but the ADR now names `show`.
Seam: `refresh_from_move` (`wallet-fedimint/src/journal.rs`) copies op-ids, gateway and quoted fees
but neither `fee_cap` nor the amount; `MoveRecord` already carries both.

TWO of its three documentation criteria are ALREADY MET — the bot review on PR #31 pulled that work
into 2/3, which is where it belonged, since the runbook was wrong the moment enforcement landed.
Verify before redoing: the runbook's `policy set` sample carries the evac knobs and says `--max-fee`
does not bound an evacuation, and README describes both caps' shapes, units and ranges.
**Re-verified on this branch** (`README.md:106-123`, `docs/real-sats-pilot-runbook.md:84-111`) —
not redone.

**Merged.** `refresh_from_move` stamps the executed amount and the enforced cap together, on
the two move-shaped kinds only, and only once a leg has committed.

*Evidence.* Red-first, per property, per path:
· the CAP assertion went red against the unfixed code in BOTH tests — `Some(Msat(2450000))` where
  `Some(Msat(230000))` was enforced, the bead's own numbers.
· the AMOUNT assertion was SHADOWED by that failure, so it proves nothing from that run. It was
  reddened separately, by removing only the amount stamp: `Msat(75000000)` vs `Msat(1000000)`, in
  both the clamp path and the reconstruction path. The file was then restored and verified
  byte-identical to a pre-mutation copy before the green re-run.

*Not run, and not claimed.* No live devimint gate on this branch. The change is read/report only —
it alters what a ledger row DISPLAYS, not what any money path decides — so the money behaviour a
live run would exercise is 2/3's, already gated nine times there. Say so rather than implying this
branch inherited that evidence.

*Not proven.* That `wallet-cli history` renders the refreshed pair end-to-end: the tests assert on
the `OperationRecord`, one layer below the CLI's formatting.

*Deliberate.* BUILD ran as a direct implementation rather than through rb-lite — one function,
~15 production lines, seams already located. The panel arrives in HARDEN, where the money-adjacent
rule wants it. BUILD's exit gate is still met on its own terms: the real gate at a real exit code,
and every load-bearing behaviour inverted with its pinning assertion observed to fail.

### HARDEN pass 1 — **DEGRADED** (one reviewer)

**The repo-aware reviewer never ran.** It sat at 0.0% CPU for 11h30m having written zero bytes,
then was killed by exact PID (exit 144). Fourth failure of that reviewer on this thread. A one-reviewer pass is one
opinion, so this pass cannot report `CLEAN` — only `CLEAN_DEGRADED` — and pass 2 must restore the
panel or say plainly that it could not.

**Diff-scoped reviewer: 2 × P2, both verified against the code, both ACCEPTED.**

1. *The stamp fired on drafts, not just executed moves.* `executor.rs:1462`/`:1489` persist a
   SIZED BUT UNMINTED `MoveRecord` and then return `Retryable`, before `mc.receive` commits
   anything. The intent returns to Pending, the row is rewritten, and the unconditional stamp
   wrote that draft pair onto it — permanently, since terminal rows are immutable. Reproduced
   red before fixing (`Some(Msat(230000))` on a row where nothing executed). Both stamps are now
   gated on committed-leg evidence (`invoice`/`recv_op`/`send_op`), which is strictly more
   conservative and costs the audit case nothing: fees only exist once a leg commits. Third test
   added, red-first. Tests 1 and 2 still pass ungated-by-accident — their move rows carry
   committed legs — and the draft test failing without the gate is also the vacuity check that
   `committed` actually discriminates.

   This is the planning-vs-executed conflation of 2/3 arriving through a different door. Worth
   naming: the concept was already known to be the dangerous one on this thread.

2. *The audit surface needs `--standalone`.* Correcting `history` → `show` was still wrong.
   `wallet-cli --standalone show` prints the pair; client-mode `show` renders `OperationView`
   (`wallet-api/src/lib.rs`), which has **no `fee_cap` field at all**. So the row is now right and
   an operator on a normal deployment still cannot see it. NOT fixed here — the wire view never
   carried the cap, so this is a pre-existing gap this work exposed. ADR qualified; filed as
   **`br-w6p`** (P2).

**Three prose claims about operator surfaces have now failed verification on this thread**, two of
them in text written this session. The tell is asserting an audit path from the bead's framing
instead of reading the formatter. Treat "names a CLI command, a knob, or a view" as requiring a
code check BEFORE it is written down.

Gate after the fix: fmt 0, clippy 0, **792 passed / 0 failed**, EXIT=0.

### HARDEN pass 2 — panel restored by SUBSTITUTION

The repo-aware reviewer was replaced with a different model after four failures. Say that plainly:
this is not the documented panel, and the substitute reads the repo the way the original was
supposed to.

**Diff-scoped reviewer: 1 × P2 — verified real, DECLINED for this bead, filed as `br-v8x`.** `mc.receive` commits
a real receive op, then `verify_replayable_receive_contract` can return `Permanent` BEFORE
`invoice`/`recv_op` are persisted, so the gate sees no committed leg and the terminal row freezes
the planned pair. Not a regression — that row showed the planned pair before this bead too. The
proposed fix moves the executor's persistence ordering, which this bead's scope guard forbids and
which is load-bearing: `has_move_artifact` is what stops `size_fresh_evacuation` re-sizing, so
recording a leg the code deliberately abandons would prevent a later occurrence re-pricing.

**Repo-aware reviewer: 2 × P3, both ACCEPTED and fixed.**

1. The gate re-derived `has_move_artifact` inline instead of calling it — and a THIRD, narrower
   variant of the same question exists at `move_protocol.rs:503` (no `invoice` disjunct). No live
   defect: `invoice ⟹ recv_op` holds on both writers. But narrowing `has_move_artifact` later
   would let sizing rewrite a pair the ledger had already stamped, with no test failing. Now
   `pub(crate)` and CALLED, with both sites documenting why they must not drift, and why
   `move_protocol`'s variant asks a different question.
2. The draft test pinned the harmless write. It drove `Pending → Pending` — a same-status rewrite,
   the one case where the stamp costs nothing — while both the doc comment and the ADR justify the
   gate by the TERMINAL case. It would also have passed if no write happened at all. Rewritten to
   drive `Failed`, with an explicit status assertion so "the row is unchanged" cannot be vacuous.
   Re-reddened in its new shape against an always-true gate.

That reviewer also verified independently, and these are recorded as CHECKED rather than assumed: no row
is made worse (a terminal row cannot be re-stamped — `advance` returns `None`); the reverse
mismatch the ADR warns about is unreachable; every consumer of both fields is a formatter, with
Pay-step enforcement, probe cost and reservations all reading `MoveRecord` and never the ledger.

*Not verified by the panel.* No reviewer ran the gate; the 792/0 figure is the driver's own.

**The cross-cutting tell fired.** Two consecutive rounds found different sites deciding ONE
concept — *when is a move committed*. Response was to stop expanding: round 2 produced a bead and
a shared predicate, not a wider diff.

## Done — 2/3
**`br-evac-cap-enforce-vn6`** — the money change. Merged as `e9cc97d` (PR #31).

**The shape, since per-round detail lives on the bead.** Rounds 2–4 each found a different
consumer of ONE undefined concept — *which net does the cap bind to* — and round 2's fix caused
round 4's finding. Round 5 was therefore a DESIGN PASS rather than a fifth patch: every ENFORCED
fee cap now computes from the **delivered net**, derived on the types that maintain the invariant
(the allocator's PLANNING cap is deliberately at the planned amount — sizing has not run yet — and
is superseded once it does), and
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

**PANEL HEALTH — the last four rounds used one reviewer.** The repo-aware reviewer produced its
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
- **The live gate cannot detect a regression to the OLD absolute cap either.** The smoke sets
  `MAX_FEE=1_000_000` msat while the total move fee it asserts is under 50_000, so an evacuation
  that went back to sizing off the absolute `max_fee` would still pass it. Owner: `br-vvo`.
- Two diagnostic branches are unpinned: the arm reached when pass 2 finds an amount and LOSES it on
  revalidation (its test deliberately avoids pass 2), and the receive-side arm of the ppm envelope
  warning, despite a test comment claiming "both halves". Owner: `br-vvo`.

### Two properties a future editor cannot infer from the tests themselves
- The seam coverage is a property of the **pair**: drift deleting the pass-1 admission would
  still satisfy the 449_999 sightings pin, because pass 2's bisection also touches it; that shape
  is caught by `the_increasing_regime_finds_the_top_window` instead.
- The pinned amounts (449_999, 339_997) are coupled to `largest_fitting_amount`'s mid arithmetic.
  If the stepping changes these fail with the sightings map printed — that means "re-derive the
  settled amount", not "the guard broke".

## Next
Nothing in this drive's scope — it is empty and `br-y2j` is closed.

`br-recanary-y2j-ujs` (the production re-canary, real sats) became READY when `br-y2j` closed.
It is the operator's decision, not this drive's next step. The deferred beads listed at the top
are backlog, each with its own acceptance criteria; none is a continuation of this drive.

## Open questions for the human
- none. `br-devimint-runbook-mint-na3` is a filed defect, not an open gate: the runbook's
  documented invocation omits `FM_ENABLE_MODULE_MINT=1` and every smoke dies at
  `Primary module not available` without it.
