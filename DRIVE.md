# DRIVE — make an evacuation executable at real fee shapes (br-y2j)

**Scope:** the evacuation fee-cap thread ONLY — `br-y2j` and its three children
`br-evac-cap-policy-r3n` (1/3), `br-evac-cap-enforce-vn6` (2/3),
`br-evac-cap-ledger-x9k` (3/3). NOT the wallet-web epic (br-nfz, br-5om, br-t8f,
br-ucq, br-pfc, br-4yz), NOT br-2aa, NOT br-s0e, NOT the production canary.

**Phase:** HARDEN · **Bead:** `br-evac-cap-enforce-vn6` ·
**Branch:** `feat/br-evac-cap-enforce-vn6`
**Pending:** —
**Gate:** `nix develop -c bash -c 'cargo clippy --all-targets -- -D warnings && cargo test'`
· last green 2026-08-07 (exit 0, 772 passed / 0 failed)

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

**HARDEN is not satisfied yet:** the P1 fix and the revert landed AFTER that panel ran,
so the current tip has unreviewed substantive money code and there is no `cleared` marker.
Round 2 over the current tip is the open work.

Known limitation to carry forward, not to hide: the hair-under test pins the arithmetic
but drives the sizing helper directly, so it would NOT go red if the bypass were
reintroduced. Its own doc comment says so. A real guard needs a fixture driving
`drive_intent_step` through a hair-under settle against a mocked multi-client.

## Next
`br-evac-cap-ledger-x9k` (3/3) — the ledger reporting the enforced cap and executed
amount, plus runbook and README. Then `br-y2j` closes, which unblocks
`br-recanary-y2j-ujs`.

## Open questions for the human
- **`br-y2j`'s live devimint gate is outstanding.** It changes a money path on a daemon
  holding real sats, and no amount of unit tests closes it. It needs the two-federation
  devimint setup and is a separate operation from the code beads. Flagging rather than
  quietly treating green unit tests as satisfying it.
