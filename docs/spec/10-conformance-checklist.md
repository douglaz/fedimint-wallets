# 10 — Conformance checklist

What has been demonstrated, by which gate, and what has not. A checked box names a gate that
exists in the repository and was last observed green. For the unit gates that observation is
CI; for the live smokes it is the run recorded in the closing issue's notes (and, for one smoke,
in its header — `CNF-39`). An unchecked box is a claim nobody has earned. Treat it as such.

The unit and integration suite runs in CI on every push. **No live gate does**, by the
workflow's explicit policy: the smokes need a two-federation devimint harness, take minutes to
hours, and a job that silently skips is worse than none. They are run by hand, in a shell prepared
by `docs/devimint-runbook.md` §1.

## Discipline

- [x] **CNF-1** The gate is one command, run unpiped, with its own exit code captured:
      `nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'`.
      `REAL_GATE_EXIT=$?` written into the log and grepped, never read off a `tail`.
- [x] **CNF-2** A test added for a property is watched to fail against the broken **production**
      behaviour first, one mutation per property, and the failure names the assertion that pins
      the property. `DEF-23` and `DEF-24` are what skipping this cost, twice in one week.
- [x] **CNF-3** A live gate's parameters discriminate: the old behaviour fails and the new one
      passes. The supersession smoke sets a base-only cap below the gateway's summed bases so the
      refusal is deterministic; the evacuation smoke does **not** yet discriminate the cap basis
      (`DEF-25`, unchecked below).
- [x] **CNF-40** A runbook claim about what a procedure does is re-run from a clean shell before
      it is called correct (`DEF-15`, `DEF-19`). The failure signature is recorded verbatim.
- [ ] **CNF-39** Every smoke header records its complete launch block and its last green run with
      the figures observed. As built only `smoke_evacuate_supersede_devimint.sh` does; the other
      fifteen carry a launch block and no run record, and their last green runs live in issue
      close notes and `DRIVE.md`, not beside the script.

## Build and unit gates

- [x] **CNF-4** `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings`
      clean, in the devshell, on every push, and (from this change) `docs/spec/tools/check-all.sh`
      exit 0 in the same job (`HST-16`).
- [x] **CNF-5** 1,071 tests pass under the gate at commit `ab52094` (PR #40's head, which
      contains `main` `1e44487`), run as `nix develop -c bash -c 'cargo fmt --all --check && cargo
      clippy --workspace --all-targets -- -D warnings && cargo test --workspace'` with
      `REAL_GATE_EXIT=0` on 2026-09-06. `main` alone is a few fewer. The suite spans all six
      crates plus the integration suites under `wallet-core/tests/` and `wallet-fedimint/tests/`.
- [x] **CNF-6** CI asserts `Cargo.lock` is unchanged **before** any cargo step runs, guarding
      against the cache action rewriting it; build-time drift is guarded by `--locked` on the clippy
      and test steps.
- [x] **CNF-7** `nix build` produces `walletd`, `wallet-cli` and a non-empty OCI image, and both
      binaries answer `--help` (`HST-16`).
- [ ] **CNF-18** Every persisted field that carries `serde(default)` (`STO-30`) is pinned by a
      test that strips the key from the serialized type and re-reads it. Ten of twelve are:
      `Refusal.diagnostics` (PR #42, with the verbatim production error), `Move.gateway`,
      `Intent.evacuation_refusal`, `RefusalDiagnostics.{max_fee_bps, conflict_suppressed}`, and
      the three `Policy` fields, and `MoveMeta.fee_cap` / `MoveMeta.from`
      (`wallet-fedimint/tests/move_meta.rs`). Two are not: the `Evacuate` defaults share one
      bare-`Action` fixture that omits both keys at once (`F41`).
- [x] **CNF-33** The **downgrade** direction is pinned for `Policy`: a row written by the
      current shape decodes under the previous shape's rules, and the handler rejects an unknown
      key (`DEF-11`, PR #43, both red-first).
- [ ] **CNF-32** A single-guardian threshold-decryption regression gate exists **in this
      repository**. It does not: the gate that closed `DEF-16` is a unit test in the fork's
      `crypto/tpe`, which `cargo test --workspace` here never runs; this repository's only
      single-guardian test checks scorer rejection. A repoint that dropped the fix would pass
      every gate here (`FMI-2`).
- [x] **CNF-34** A claim committing inside the supersession exchange window leaves one executable
      intent: the parent `Executing`, no child, no sidecar, `pending()` equal to the parent alone.
      Red against both guards it pins (`DEF-23`, commit `beba9ab`).
- [x] **CNF-35** A joined-but-unopened federation fences the scheduler with
      `partial_federation_view` and writes no probe, tick or watch row
      (`a_partial_federation_view_reports_why_automation_is_blocked`).
- [x] **CNF-36** Allocator behaviour is pinned by golden fixtures (`wallet-core/tests/allocator_golden.rs`)
      whose evacuation cases stamp cap components that disagree with `max_fee`, so an accidental
      return to the flat cap breaks every evacuation golden.
- [x] **CNF-37** `min_viable_amount` never under-estimates the true break-even, pinned by
      `floor_never_under_estimates_the_true_break_even` over four fee tuples at three amounts each
      — fixture coverage, not a generated property test (`ALC-12`).
- [x] **CNF-38** The daemon runs exactly one `u64::MAX` occurrence cycle and then fails closed
      every later one (`ALC-33`).
- [x] **CNF-26** The exit-code mapping (`API-28`) and the client-mode request shape and stdout
      contract of `balance`, `history`, `show`, `candidates`, `status`, `pay`, `receive`, the
      await verbs, `policy get`/`set`, `reconcile` and `health` are pinned against a mock daemon
      (`wallet-cli/tests/cli_client.rs`). `join`, `recover`, `move`, `direct-inflow`,
      `approve` and `list-feds` have no mock-server test; their wire shapes are covered only by
      the live daemon smokes (`CNF-19`, `CNF-20`).

## Live gates — the harness

- [x] **CNF-27** The two-federation harness is `docs/devimint-two-fed-harness.patch` applied to a
      checkout at the **pinned** SDK revision, not an arbitrary one (a different commit can be
      protocol-incompatible with the client), built release.
- [x] **CNF-28** The harness environment exports `FM_ENABLE_MODULE_LNV2=1`,
      `FM_ENABLE_MODULE_MINT=1` and `FM_ENABLE_MODULE_WALLET=1`; without the last two every smoke
      that reads a balance dies with "Primary module not available" (`DEF-19`).
- [x] **CNF-29** Wallet binaries are rebuilt into this repository's target directory through a
      fixed Nix child-environment allowlist with a fresh temporary Cargo source home before each
      certifying run, so an ambiently overridden build or a mutable git source cannot be what
      passed.
- [x] **CNF-31** Binaries are rebuilt before every smoke. A stale binary has silently
      invalidated a gate twice.
- [x] **CNF-30** The smoke's real exit is captured (`REAL_GATE_EXIT`), not the trailing command's.
- [x] **CNF-25** The misbehaving-gateway double (`hang_gateway.py`) accepts a connection and
      never answers. It is a **concurrency** instrument for the responsiveness gate; it proves
      the broader timeout boundary, not the "quotes but does not perform" case, and a stalled
      gateway does not by itself strand a move (`FMI-23`).

## Live gates — what each proves

- [x] **CNF-8** `smoke_devimint.sh`: join and balance against one federation (Phase 1 step 3).
- [x] **CNF-9** `smoke_money_devimint.sh`: receive and pay over lnv2 through the direct-swap path,
      single federation, one gateway.
- [x] **CNF-10** `smoke_directinflow_devimint.sh`: a direct inflow nets the target amount, never
      more and at most 1,000 msat less (the gross-up fixed point plus lnv2's unquoted mint output
      fee, `FMI-15`).
- [x] **CNF-11** `smoke_move_devimint.sh`: a two-leg move A→B through a shared gateway's internal
      swap; B rises by the amount, A falls by amount plus fees.
- [x] **CNF-12** `smoke_crash_move_devimint.sh`: the move survives an uncatchable abort at each of
      the four killpoints (`OPS-28`) and `reconcile` completes it with **no double-pay and no
      second payable invoice** — B rises exactly once, A falls exactly once.
- [x] **CNF-13** `smoke_tick_devimint.sh`: probe → score → snapshot → decide → apply emits and
      performs a fund-standby move chosen by the allocator, not named on the command line.
- [x] **CNF-14** `smoke_evacuate_devimint.sh`: a federation forced to look like it is shutting
      down (debug seam) is drained into `safest_other` by a tick; B rises, A drains to ~0. Uses
      release-built devimint and debug wallet binaries (`SEC-18`).
- [x] **CNF-15** `smoke_history_devimint.sh`: a full session — two joins, direct inflow, raw
      receive, move, a forced fee-cap failure, a tick with induced `OverCap` refusals — is
      reconstructible from `history` and `show`: kinds, actors, reasons, fees, errors, both
      legs' op ids, timestamps non-decreasing by seq.
- [x] **CNF-16** `smoke_probe_devimint.sh`: the active probe mints on the candidate and redeems
      back; combined loss is fees only; every leg and the umbrella row are in `history`; a
      sustained window flips the verdict to `passed`; an unjoined candidate is a no-attempt that
      never demotes.
- [x] **CNF-17** `smoke_discover_devimint.sh`: a manually discovered, auto-joined federation is
      refused funding by a tick — a pin does not bypass the gate — until three probes pass, and
      is funded by the same tick afterwards. Discover, auto-join and agent join rows land in
      `history`.
- [x] **CNF-19** `smoke_daemon_devimint.sh`: receive and pay driven through a running `walletd`
      by the CLI in client mode: HTTP → bearer → handler → actor → driver → ledger → long-poll.
- [x] **CNF-20** `smoke_daemon_chain_devimint.sh`: the autonomous chain under the daemon's own
      scheduler — probe-gated auto-join, scheduled probes to `passed`, an autonomous fund toward
      the standby target **never over**, a restart with the forced-shutdown seam, an autonomous
      evacuation back — with the operator touching only policy.
- [x] **CNF-21** `smoke_responsiveness_devimint.sh`: with a scheduled probe held in flight by the
      hanging double, `POST /v1/pay` reaches its first external SDK call in **< 250 ms**,
      measured externally by the double's timestamps; two pays never serialize; at the admission
      cap's worth of hung drivers the cap-plus-one submit is refused and SIGTERM still exits 0
      promptly.
- [x] **CNF-22** `smoke_soak_devimint.sh`: 24 hours of periodic receive, pay and move under the
      active scheduler; every operation key appears **exactly once** in history, zero user-op
      failures, a stable PID, a clean SIGTERM, no lock or panic lines. Last green on RC
      `b5f46de`: 282 iterations, 604 operations, 0 duplicates, 0 retries, 0 watchdog firings.
- [x] **CNF-23** `smoke_recover_devimint.sh`: the seed plus the invite rebuilds a lost wallet's
      ecash **exactly** (zero slack) in both loss shapes — whole-store loss through `walletd
      restore-mnemonic` + `POST /v1/recover`, and journal-only loss with an orphaned partition
      through standalone — and the recovered federation is `UserApproved` and spendable.
- [x] **CNF-24** `smoke_evacuate_supersede_devimint.sh`: a base-only cap below the gateway's
      measured summed bases produces a deterministic structural refusal; the parent holds an
      active marker with real samples and no artifacts while balances stay unchanged; a
      component-wise raise makes the **scheduler** react without a manual tick; exactly one
      child with reciprocal links and the parent's marker cleared; real movement whose fee fits
      the new cap and not the old; restart plus reconcile changes nothing. Green twice,
      byte-identical, on two regtest federations.

## Not demonstrated

- [ ] **CNF-41** The recovery **failure** path live: a module-recovery failure terminalizes the
      operation `Failed` (not a hang), the partition is left inert, a retry gets a clean prefix.
      Unit-tested in the fork only; needs a fault hook that does not exist (`F24`).
- [ ] **CNF-42** A crash at `after-receive-commit` on a supersession **child**, then restart and
      reconcile. The supersession smoke covers restart-and-reconcile but not a mid-flight kill
      (`F25`, `br-supersession-child-killpoint-gate-7c9`).
- [ ] **CNF-43** The evacuation smoke discriminating the cap **basis**: its flat cap is far above
      the fee it asserts, so a return to sizing off `max_fee` would pass; and the delivered-net
      basis is unpinned at the pre-mint gate and the post-receive recompute because the test route
      cannot produce `delivered ≠ ask` (`DEF-25`, `F22`, `F23`).
- [ ] **CNF-44** Any build after `b5f46de` against a **real** federation: receive, pay, scheduled
      top-up, standby funding, cross-federation move, restart, reconcile, and — for the changed
      path — that the evacuation cap admits a full-balance drain at current gateway prices
      (`HST-24`).
- [ ] **CNF-45** A human reading of the four supersession money-path boundaries (`F26`).
- [ ] **CNF-46** The browser sidecar's route manifest and live gate (`F27`).
- [ ] **CNF-47** A crash-safe seed re-encryption with injected failures at each step (`F11`).
- [ ] **CNF-48** A restore drill from an app-state snapshot plus seed with in-flight operations
      explained (`F12`).
- [ ] **CNF-49** The readiness poller running from a schedule and paging on a transition (`F14`).
- [ ] **CNF-50** A failed `ReconcileDecide` reported as `automation_blocked` (`F32`).
- [ ] **CNF-51** A malformed value under a well-formed registry key fences the scheduler with
      `corrupt_federation_registry` and writes no probe, tick or watch row, planted under the
      `0x00` partition (`DEF-24`). The test exists in PR #40 and is not on `main` (`F2`).
