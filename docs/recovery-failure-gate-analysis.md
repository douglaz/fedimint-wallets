# Recovery FAILURE path (complete-or-fail): live-gate feasibility analysis

**Status: a faithful live devimint gate is NOT feasible with current levers; the property
is covered by unit tests, and a real live gate is deferred to a follow-up that adds a
fault hook. See "Decision" below.**

## What we wanted to gate

A failed *module* recovery must terminate the wallet's `recover` with an `Err` (a `Failed`
intent) instead of hanging forever. This "complete-or-fail" behavior rests on the pinned
fedimint fork patch (upstream draft PR fedimint#8866, commit `5bd145cd6de` on the pinned
rev `72b1e5b…`): `run_module_recoveries_task` publishes a module failure out-of-band on a
`watch` channel, and `wait_for_all_recoveries` races progress against that channel with a
biased `select!`, returning `Err("Module recovery failed: …")`. The wallet propagates it:
`MultiClient::recover` → `wait_for_recoveries_with_progress` → `wait_for_all_recoveries().?`
→ `executor.rs` `Action::Recover` maps the error to `ExecError::Permanent` → the intent
terminalizes `Failed` (not a hang), the fresh db partition is never registered (inert), and
a retry allocates the next prefix.

The recovery SUCCESS path is already strongly gated live (`wallet-cli/tests/smoke_recover_devimint.sh`,
including Phase B, which recovers *past* an orphaned partition into a clean prefix).

## Why a live FAILURE-injection gate is not tractable

1. **The wallet's registered modules recover by history replay, which does not fail cleanly.**
   The wallet registers mint-v1 / wallet-v1 / ln-v1 / lnv2 (`multi_client.rs`), whose
   recovery replays the federation's session history. That fetch loop **retries every
   transport error forever** (randomized backoff, capped ~2 min) or **panics** on a missing
   session — it does not return the `Err` that #8866 converts. The single non-retry escape
   is one `session_count()` consensus call at the very start of each module's recovery
   (returns `Err` once `one_honest()` = 2-of-4 guardians error).

2. **The only external trigger is a millisecond-wide, transport-dependent race.** Killing
   exactly 2-of-4 guardians during that `session_count()` window — after preview+init but
   before block streaming — could produce a genuine failed module recovery, but any miss
   degrades into the retry-forever **hang** the gate is meant to disprove (a false failure),
   and iroh reconnect behavior can turn even a well-timed kill into a hang. devimint exposes
   no "kill guardian N" to `--exec`.

3. **There is no devimint or fedimint fault hook for this.** devimint's only recovery env
   vars are a path selector (`FM_FORCE_V1_MINT_RECOVERY`) and a binary path
   (`FM_RECOVERYTOOL_BASE_EXECUTABLE`); neither injects a failure. The #8866 patch is gated
   in the fork by **in-process unit tests** that mock a `ModuleRecoveryFuture` returning
   `Err` (`fedimint-client/src/client/tests.rs`) — which is the correct place to prove this
   property.

4. **The wrapper-level approximations fail BEFORE a partition is created, so they do not
   exercise the "inert orphan partition / clean-prefix retry" assertions.** A preview/
   transport failure (`recover` against an unreachable/non-existent invite) fails at the
   preview under `PREVIEW_UNDER_LOCK_TIMEOUT`, and the registered-fed refusal fails at
   `ensure_recover_not_registered` — both *before* `next_db_prefix`/`preview.recover(...)`
   allocate the fresh partition. They would only exercise "terminalizes FAILED, not a hang"
   (bounded by an `await-move --timeout`), which the registered-fed refusal already covers
   in the unit test `multi_client::tests::recover_refuses_a_registered_federation`.

## What IS covered today

- The **#8866 module-recovery `Err` channel** — fork in-process unit tests mocking a failing
  `ModuleRecoveryFuture` (the deterministic, correct place for it).
- **`recover` terminalizes FAILED (not a hang) on a pre-partition failure** — the wallet unit
  test `recover_refuses_a_registered_federation`, and the CLI/daemon failure surface
  (`await-move` → `failed:` + exit 3, bounded by `--timeout`).
- **Inert orphan partition + clean-prefix retry mechanics** — the live recover SUCCESS gate
  (`smoke_recover_devimint.sh` Phase B) already recovers past an orphaned partition into the
  next prefix.

## Decision

**Defer a live failure-injection gate; accept the coverage above for the short pilot.** The
property is deterministically unit-tested where it can be (the fork mock), and the
partition/prefix mechanics are live-gated on the success side. A live gate that faithfully
exercises the #8866 `wait_for_all_recoveries` `Err` path end-to-end requires a **fault hook**
that does not exist today, e.g. an env-gated fork knob `FM_TEST_FAIL_MODULE_RECOVERY=<kind>`
that makes a module's `recover()` return `Err` deterministically (mirroring the unit-test
mock), or a test-only module in the dev-fed whose recovery always errors. That is a fedimint
fork change, not a bash smoke — tracked as a follow-up bead.
