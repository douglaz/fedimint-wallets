# DRIVE — make an evacuation executable at real fee shapes (br-y2j)

**Scope:** the evacuation fee-cap thread ONLY — `br-y2j` and its three children
`br-evac-cap-policy-r3n` (1/3), `br-evac-cap-enforce-vn6` (2/3),
`br-evac-cap-ledger-x9k` (3/3). NOT the wallet-web epic (br-nfz, br-5om, br-t8f,
br-ucq, br-pfc, br-4yz), NOT br-2aa, NOT br-s0e, NOT the production canary.

**Phase:** HARDEN · **Bead:** `br-evac-cap-enforce-vn6` ·
**Branch:** `feat/br-evac-cap-enforce-vn6`
**Pending:** —
**Gate:** `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`
· last green 2026-08-07 at `e9c38b6` (fmt 0, clippy 0, **774 passed / 0 failed**)

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
**2/3 `br-evac-cap-enforce-vn6`** — the money change. Committed (`dd6c46c`) and pushed.

Review round 1 is COMPLETE (findings recorded on the bead):
- codex found one P1 — the receive fixed point's hair-under settle kept the cap computed
  at the larger sized ask. **Verified and fixed.**
- the money reviewer found no P0/P1/P2; it reproduced 13 pinned fixtures exactly and
  grep-confirmed the sizing seam is the only writer of `rec.amount`. Five P3s open.
- the skeptic found 11 P2s; one taken (a behavioural no-op that added a hunk to a money
  audit surface), the rest declined as load-bearing.

Round 2 (full panel, run in parallel) is COMPLETE:
- **codex** — P1: the live devimint gate is outstanding and blocks merge, citing this
  repo's own `AGENTS.md`. P2: `assemble_move_record` paired the cached executed amount
  with the intent's planned cap when `backfill_ops` dropped an undecodable `MoveMeta`.
  **Fixed** (`e9c38b6`), with a genuinely red-first test.
- **money reviewer** — no P0/P1/P2. Confirms the round-1 hair-under fix "correct and
  complete", verified the sizing seam is the only writer of `rec.amount`, and checked the
  reassembly fix's final form. Five P3s.
- **skeptic** — no money findings; traced every construct back to a `br-y2j` clause, and
  DEFERRED its own ten remaining round-1 items under the same reasoning that declined
  them.

**Declined with reason, recorded as follow-ups rather than churn:** two executor P3s
(the pre-mint receive gate enforcing at the sized ask rather than the delivered net, and a
sub-floor `desired` reported as an affordability failure). Both are quality-of-refusal,
neither loses money, and every further edit to money code invalidates the clearance the
panel just gave.

**PROVE: the live devimint gate PASSED** 2026-08-07 (exit 0), closing codex's P1. The new cap
arithmetic is verified against live federations, not fixtures:

    decision: evacuate 449998 msat A -> B (fee_cap 213499 msat, reason ShutdownNotice)
    213499 == 200000 + floor(449998 * 300 / 10000)     EXACT

and it differs from the old absolute `max_fee` of 200000, so the new formula is in force rather
than falling back. A drained 499998 -> 35870 msat (~0); B netted 449918, a hair under, never over;
a healthy fed correctly decided NO evacuate.

The gate was RED first. It failed at funding with `Primary module not available` in ~0.4s, and a
CONTROL RUN of the same diagnostic against merged `main` (`7d5e69f`) failed IDENTICALLY — that
controlled comparison is what cleared this branch, not the error text sounding unrelated to fee
caps. Root cause is environmental and pre-existing: devimint at our pin does not enable mint v1,
which is `wallet-cli`'s primary module, and the runbook's documented invocation never sets it.
Filed as `br-devimint-runbook-mint-na3` (P1) — following the runbook as written could not have
produced a green run, so this gate was never being skipped.

Known limitation carried forward, not hidden: the hair-under test pins the arithmetic but
drives the sizing helper directly, so it would NOT go red if the bypass were reintroduced.
Its own doc comment says so. A real guard needs a fixture driving `drive_intent_step`
through a hair-under settle against a mocked multi-client. (The later reassembly test IS
red-first — verified by reverting the fix: 21 passed, 1 failed, `left: Msat(2450000)`,
`right: Msat(230000)`.)

## Next
`br-evac-cap-ledger-x9k` (3/3) — the ledger reporting the enforced cap and executed
amount, plus runbook and README. Then `br-y2j` closes, which unblocks
`br-recanary-y2j-ujs`.

## Open questions for the human
- **`br-y2j`'s live devimint gate is outstanding.** It changes a money path on a daemon
  holding real sats, and no amount of unit tests closes it. It needs the two-federation
  devimint setup and is a separate operation from the code beads. Flagging rather than
  quietly treating green unit tests as satisfying it.
