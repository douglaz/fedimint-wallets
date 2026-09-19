# Conformance results

What this implementation has demonstrated against the specification's conformance scenarios
(`10-conformance-checklist.md` in
[`douglaz/fedimint-wallets-spec`](https://github.com/douglaz/fedimint-wallets-spec)), one row
per `CNF-n` that exists or is withdrawn there. A scenario's text lives in the specification;
this file cites the id and records only the result: passed or not, at which revision, by which
gate or smoke, with which figures observed. A scenario this tree has not passed is a
non-conformance, tracked in [`open-findings.md`](./open-findings.md) (`ADR-0032`); a row whose
finding is not yet written names the bead that will write it.

**Provenance.** The set of ids — one row each — is the specification's chapter 10 and README
withdrawn-identifier index at its `cd4ea86` (2026-09-18). The results were transcribed from
chapter 10 at its `2b9acf2` (the file as it stood before it became the normative scenario suite
in its PR #13), and, for the ids that PRs #3–#12 had already withdrawn, from the same file at
`c6354f3`; `CNF-54`, added by PR #13, has no pre-image row and is assessed against `F27`. Those
chapters named
no revision of this tree; the withdrawn `HST-24`, written at the same time, measured `main` at
`7225114` (2026-09-09), so that is the revision a row without one of its own was last observed
green at or before. Nothing was re-run to write this file. A row is updated when a gate is
re-run, with the new revision and figures.

**How results are produced.** Unit and integration results come from the gate
(`AGENTS.md`), which `.github/workflows/ci.yml` runs on every pull request and on every push to
`main`. Live results come from the
seventeen devimint smokes `wallet-cli/tests/smoke_*devimint.sh`, run by hand from the exact-pin
build of [`devimint-runbook.md`](./devimint-runbook.md) §1. Five launch a single federation
(`dev-fed --num-feds 1`: `smoke_devimint.sh`, `smoke_money_devimint.sh`,
`smoke_directinflow_devimint.sh`, `smoke_recover_devimint.sh`; and `smoke_daemon_devimint.sh`,
whose `dev-fed` has no `--num-feds` and reads no B invite); the other twelve run on the
two-federation harness of §2. No live gate runs in CI, by the workflow's explicit policy. Where a smoke's last green run is recorded is the
`CNF-39` row below.

## Scenarios

| Id | Result | Revision | Produced by | Observed |
|---|---|---|---|---|
| `CNF-8` | Passed | ≤ `7225114` | `smoke_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-9` | **Not passed** | `7225114` | `smoke_money_devimint.sh` | Passed as the pre-rewrite item (receive and pay over lnv2). The current scenario's clause that a second pay attaches while the first is still in flight (added by specification PR #13) is not asserted: the smoke retries only after `await-send` reports success. No finding yet; its finding is an item of `br-new-findings-ops-sto-927`. No run record beside the script (`CNF-39`). |
| `CNF-10` | Passed | ≤ `7225114` | `smoke_directinflow_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-11` | Passed | ≤ `7225114` | `smoke_move_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-12` | **Not passed** | `7225114` | `smoke_crash_move_devimint.sh` | Passed as the pre-rewrite item: an uncatchable abort at each of the four killpoints, `reconcile` completing the move, B rising and A falling exactly once, no second payable invoice. The current scenario's clause that the route recorded with the committed leg is the one the send went through is not asserted (the smoke checks no route), and the route is not persisted at all: `F7`. No run record beside the script (`CNF-39`). |
| `CNF-13` | Passed | ≤ `7225114` | `smoke_tick_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-14` | Passed | ≤ `7225114` | `smoke_evacuate_devimint.sh` | Release-built devimint, debug wallet binaries (`SEC-18`). No run record beside the script (`CNF-39`). |
| `CNF-15` | Passed | ≤ `7225114` | `smoke_history_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-16` | Passed | ≤ `7225114` | `smoke_probe_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-17` | Passed | ≤ `7225114` | `smoke_discover_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-18` | **Not passed** | `7225114` | Unit tests (`wallet-fedimint/tests/move_meta.rs` and siblings) | Of the twelve fields assessed, six are pinned by a strip-one-key test. Six are not: the two `Evacuate` defaults share one bare-`Action` fixture, the two `RefusalDiagnostics` observational fields share one fixture (`wallet-fedimint/tests/ledger.rs`, `refusal_diagnostics_missing_observational_fields_decode_to_defaults`), and the two `Policy` evacuation-fee fields share one (`wallet-api/src/lib.rs`, `policy_missing_evac_fee_fields_decode_with_shipped_defaults`); a fixture that omits several keys at once pins none of them. `F41` counts only the `Evacuate` pair and understates this; re-read under `br-findings-reread-adr-0032-z18`. The rest of the eighteen `STO-30` lists were not assessed at this revision. |
| `CNF-19` | Passed | ≤ `7225114` | `smoke_daemon_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-20` | Passed | ≤ `7225114` | `smoke_daemon_chain_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-21` | Passed | ≤ `7225114` | `smoke_responsiveness_devimint.sh`, with the `hang_gateway.py` double | The pay reached its first external SDK call in under 250 ms, measured by the double's timestamps. No run record beside the script (`CNF-39`). |
| `CNF-22` | Passed | `b5f46de` | `smoke_soak_devimint.sh` | 24 h: 282 iterations, 604 operations, 0 duplicates, 0 retries, 0 watchdog firings. |
| `CNF-23` | Passed | ≤ `7225114` | `smoke_recover_devimint.sh` | No run record beside the script (`CNF-39`). |
| `CNF-24` | **Not passed** | `7225114` | `smoke_evacuate_supersede_devimint.sh` | Passed as the pre-rewrite item; the current scenario's clause that the retired parent reads `Failed` as superseded (added by specification PR #13) is not asserted — the smoke checks the marker and the child links, never the parent's status. No finding yet; its finding is an item of `br-new-findings-ops-sto-927`. Green twice, byte-identical, on two regtest federations. Header record, 2026-09-02: `low=4894 high=1865230 fees=105698/121074 caps=1000/1000`; balances unchanged under refusal (A=1986590, B=0); exactly one child, links reciprocal, parent marker cleared; moved A 1986590→286, B 0→1865230, fee 121074, which fits cap B (400000) and not cap A (1000). |
| `CNF-26` | **Not passed** | `7225114` | `wallet-cli/tests/cli_client.rs` (mock daemon) | Pinned: the exit-code mapping and the client-mode wire shape of `balance`, `history`, `show`, `candidates`, `status`, `pay`, `receive`, the await verbs, `policy get`/`set`, `reconcile`, `health`. Not demonstrated: `join`, `recover`, `move`, `direct-inflow`, `approve`, `list-feds` (exercised only live, `CNF-19`/`CNF-20`); the `health` readiness fields and `status` `deferred`/`suppressed` lines (`API-39`, `F35`); `fee_cap` in `history --json`/`show --json` and `show`'s `fee_cap_msat` line (`API-33`, `F9`); `policy set` preserving keys it has no flag for and refusing a flag for a field the GET did not return (`API-27`, `F40`). |
| `CNF-32` | **Not passed** | `7225114` | — | No single-guardian threshold-decryption regression gate in this repository; the gate that closed the defect is a unit test in the fork's `crypto/tpe`, which `cargo test --workspace` here never runs. No finding tracks this yet (`F19` covers only the fork repoint); its finding is an item of `br-new-findings-fmi-wno`. |
| `CNF-33` | Passed | `0aa175c` (PR #43) | Unit tests | Both red-first. |
| `CNF-34` | Passed | `beba9ab` | Unit test | Red against both guards it pins. |
| `CNF-35` | Passed | ≤ `7225114` | `a_partial_federation_view_reports_why_automation_is_blocked` | — |
| `CNF-36` | **Not passed** | `7225114` | — | The pre-image ticked the old item on `wallet-core/tests/allocator_golden.rs`, whose evacuation goldens stamp cap components that disagree with `max_fee`; the current scenario is a live tick against `CNF-14`'s dying federation, and `smoke_evacuate_devimint.sh` cannot detect a flat-cap regression. `F22`. |
| `CNF-37` | Passed | ≤ `7225114` | `floor_never_under_estimates_the_true_break_even` | Four fee tuples at three amounts each; fixture coverage, not a generated property test. |
| `CNF-38` | Passed | ≤ `7225114` | Unit test | — |
| `CNF-41` | **Not passed** | `7225114` | — | Unit-tested in the fork only; no live gate here. `F24`. |
| `CNF-42` | **Not passed** | `7225114` | — | `smoke_evacuate_supersede_devimint.sh` covers restart-and-reconcile but no kill at `after-receive-commit` on a child. `F25`, `br-supersession-child-killpoint-gate-7c9`. |
| `CNF-43` | **Not passed** | `7225114` | — | `smoke_evacuate_devimint.sh`'s flat cap is far above the fee it asserts, and its route cannot produce `delivered ≠ ask`. `F22`, `F23`. |
| `CNF-47` | **Not passed** | `7225114` | — | Nothing to gate yet. `F11`. |
| `CNF-50` | **Not passed** | `7225114` | — | `F32`. |
| `CNF-51` | Passed | `ee4ba1c` (green in PR #40's CI at `ab52094`) | `a_corrupt_federation_registry_reports_why_automation_is_blocked` | Malformed value planted under the `0x00` partition. |
| `CNF-53` | **Not passed** | `c2ea000` (PR #49) | `smoke_breakglass_devimint.sh` | Passed as the pre-rewrite item: registers the gateway on A only, leaves B's vetted list empty throughout, asserts the usage errors for `tick`, `probe` and `balance --gateway`. The current scenario's usage errors for `status --gateway` and `reconcile --gateway` (added by specification PR #13) are not asserted. No finding yet; its finding is an item of `br-new-findings-ops-sto-927`. No run record beside the script (`CNF-39`). |
| `CNF-54` | **Not passed** | `7225114` | — | Added by specification PR #13 after this tree's last review; the sidecar is a skeleton. `F27`. |

## Withdrawn ids

The specification withdrew these (its `README.md`, withdrawn-identifier index) because they were
process disciplines, build results, harness facts or work items rather than scenarios. What each
recorded when withdrawn is kept here so the id resolves.

| Id | State when withdrawn | Record |
|---|---|---|
| `CNF-1` | Checked | The gate is one command run unpiped with its own exit captured; now a discipline of `AGENTS.md`. |
| `CNF-2` | Checked | Tests watched to fail against broken production behaviour first, one mutation per property; now a discipline of `AGENTS.md`. |
| `CNF-3` | Checked | Live-gate parameters discriminate: the supersession smoke's base-only cap below the summed bases does; the evacuation smoke's cap basis does not (`DEF-25`, now `CNF-43`). |
| `CNF-4` | Checked | `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings` and the specification's `tools/check-all.sh` exit 0 in CI on every pull request and push to `main`. |
| `CNF-5` | Checked | 1,071 tests passed under the gate at `ab52094` (PR #40's head, containing `main` `1e44487`), `REAL_GATE_EXIT=0`, 2026-09-06. |
| `CNF-6` | Checked | CI asserts `Cargo.lock` is unchanged after the cache action (whose `cargo metadata` runs inside the devshell) and before the gate's own fmt, clippy and test steps; `--locked` on clippy and test. |
| `CNF-7` | Checked | `nix build` produces `walletd`, `wallet-cli` and a non-empty OCI image; both answer `--help`. |
| `CNF-25` | Checked | The `hang_gateway.py` double accepts and never answers; a concurrency instrument for `CNF-21`, not the quotes-but-does-not-perform case. |
| `CNF-27` | Checked | The two-federation harness is [`devimint-two-fed-harness.patch`](./devimint-two-fed-harness.patch) applied at the pinned SDK revision, built release. |
| `CNF-28` | Checked | The harness exports `FM_ENABLE_MODULE_LNV2=1`, `FM_ENABLE_MODULE_MINT=1` and `FM_ENABLE_MODULE_WALLET=1`. |
| `CNF-29` | Checked | Binaries rebuilt through a fixed Nix child-environment allowlist with a fresh temporary Cargo source home before each certifying run. |
| `CNF-30` | Checked | The smoke's real exit is captured (`REAL_GATE_EXIT`), not the trailing command's. |
| `CNF-31` | Checked | Binaries rebuilt before every smoke; a stale binary had invalidated a gate twice. |
| `CNF-39` | Unchecked | Fourteen of the seventeen smoke headers carry their own launch block; `smoke_discover_devimint.sh`, `smoke_history_devimint.sh` and `smoke_probe_devimint.sh` point at another smoke's header instead (re-checked 2026-09-19: a header comment line carrying `--exec bash`, i.e. `grep -q '^#.*--exec bash'`, per script). Only `smoke_evacuate_supersede_devimint.sh` records its last green run with figures; the other sixteen runs live in issue close notes and `docs/archive/drive-br-n8o-2026-08.md`. |
| `CNF-40` | Checked | Runbook claims re-run from a clean shell before being called correct, failure signature recorded verbatim. |
| `CNF-44` | Unchecked | No build after `b5f46de` has run against a real federation; see *Exercised against a real federation*. |
| `CNF-45` | Unchecked | The four supersession money-path boundaries unread by a human. `F26`. |
| `CNF-46` | Unchecked | No sidecar route manifest or live gate. `F27`; the sidecar's scenario is now `CNF-54`. |
| `CNF-48` | Unchecked | No restore drill from an app-state snapshot. `F12`. |
| `CNF-49` | Unchecked | The readiness poller has never run from a schedule. `F14`. |
| `CNF-52` | Checked | Every routed smoke registers the LDK gateway on every guardian right after bring-up, except `smoke_breakglass_devimint.sh` (A only, B's list empty) and `smoke_responsiveness_devimint.sh` (registers its double). |

## Exercised against a real federation

The withdrawn `HST-24`'s claim, kept here as `ADR-0032` directs. The one long-running deployment
runs build `b5f46de` (2026-07-26). On 2026-09-19 that commit was reachable from no branch of this
repository, though GitHub still served the object; its source tree is identical to `5133ffb` on
`main` (the PR #23 commit, same date; three stray untracked binaries aside), so cite that one.
`HST-24` recorded `main` at `7225114` as 240 commits past the build, a measurement over a history
this tree no longer carries; on `main` as of 2026-09-19, `git rev-list --count 5133ffb..7225114`
gives 112.
Everything that landed in between — the readiness
fields (`API-16`), the evacuation cap (`ALC-20`), supersession (`OPS-30`), both persistence fixes
(`DEF-10`, `DEF-11`), and every requirement the specification once marked as landing with
PRs #30–#44 — is unexercised against a real federation; its only live exercise is the devimint
regtest smokes above, which are not one.
Upgrading the deployment past `b5f46de` burns its rollback on the first policy write.
