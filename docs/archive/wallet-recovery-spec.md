# br-m9m implementation spec: seed-based wallet recovery via `ClientPreview::recover`

Status: **decisions locked** (design interview, 2026-07-24). Supersedes the earlier draft of this
spec, whose D3/D5 were jointly incoherent (see "Why the first attempt failed"). All file:line refs
verified against `main`.

## The gap
Every join goes through `preview.join(self.client_db(prefix), self.root_secret.clone())`
(`multi_client.rs:214`, `InitMode::Fresh` → empty client). `ClientPreview::recover` is never
called. So a funded client cannot be rebuilt from the seed, and a lost `journal.db` orphans the
funded partition while `balance` reads 0.

## The contract this implements (decision 1)
**The backup unit is the seed plus the joined federation IDs — never the local stores.** The
stores carry **no cross-store point-in-time guarantee**: they are restored together from one
snapshot, or not at all. A mismatched (`client.db` from one moment, `journal.db` from another)
pair is **out of contract** and V1 does not defend against it. See `CONTEXT.md` ("Silent backup /
Recovery", "Restore") and ADR-0003. This is what makes the runbook's "losing `journal.db` loses
records, not settled funds" true.

## SDK primitives
- `ClientPreview::recover(db, pre_root_secret, backup: Option<ClientBackup>) -> anyhow::Result<ClientHandle>`
  (`fedimint-client/src/client/builder.rs:1463`). Pass `backup = None` (full epoch-history
  recovery; `download_backup_from_federation` is `#[deprecated]`). Same args as `join`; the only
  difference is `InitMode::Recover` vs `Fresh`.
  **It rejects an already-initialized DB** — which is why V1 only ever recovers into a fresh
  partition (decision 3).
- `Client::wait_for_all_recoveries()` (`client.rs:1846`) — the completion gate.
- `subscribe_to_recovery_progress()` (`client.rs:1864`) — progress display ONLY, never completion
  detection.

**Upstream dependency:** stock `wait_for_all_recoveries()` can never report a *failure* — a failed
module recovery is logged and parked on `futures::future::pending()` forever
(`client.rs`, `run_module_recoveries_task`), making "failed" indistinguishable from "slow". We fix
that at the source (branch `fix/recovery-failure-is-determinate`, upstreaming to
fedimint/fedimint; carried on the `douglaz/fedimint` pin meanwhile) so recovery is
**complete-or-fail**. This spec assumes the patched SDK.

## Design decisions

### D1 — A distinct `recover` verb, never automatic
- Core: new `Action::Recover { federation, invite }` (sibling of `Action::Join`, `types.rs:169`).
  **`decide()` must NEVER emit it**; the auto-join and driver retry paths keep calling `join`.
  User-initiated only. (A brand-new enum variant is additively safe for serde_json; if any
  *existing* persisted type gains a field, it needs `#[serde(default)]`.)
- CLI: `Command::Recover { invite }` (`wallet-cli/src/main.rs:82`), wired at both the
  daemon-backed dispatch (`main.rs:604`) and the standalone dispatch (`main.rs:1321`).
- Daemon: `POST /v1/recover` + `RecoverRequest { invite }`, mirroring `join`
  (`wallet-daemon/src/handlers.rs:400`).
- Executor: the `Action::Recover` perform arm calls `MultiClient::recover(invite)` (mirroring the
  `mc.join` call at `executor.rs:1056`).

### D2 — Seed import (`walletd restore-mnemonic`) — decision 4
Recovery is impossible without a way to supply the seed: `load_or_generate_mnemonic`
(`wallet-daemon/src/main.rs:270`) mints a *random* mnemonic on first daemon start and there is no
import path. Add `walletd restore-mnemonic`, the exact mirror of the existing `walletd mnemonic`
(`main.rs:75`, which is deliberately read-only and "must never mint a seed"):
- **Refuses if a seed already exists. No `--force` in V1.** Overwriting a seed silently strands
  whatever it funded; that is the one irreversible action here.
- **Reads the words from stdin, never argv** (argv leaks into shell history and `ps`). Symmetric
  with `walletd mnemonic` writing the secret to stdout.
- **Validates the BIP-39 checksum** before writing anything.
- **Ordering: `walletd init` → `walletd restore-mnemonic` → *then* start.** `init` does not mint a
  seed (it only seeds the default policy row); only the daemon start path does. Starting first
  mints a random seed and the import then correctly refuses — the documented fix is a clean data
  dir. The runbook must state this ordering explicitly.

### D3 — Refuse if the federation is REGISTERED; else recover into a FRESH prefix (decision 2)
V1 performs **no supersede-in-place**. Resolve the target prefix as:
- If the federation is **REGISTERED** (`journal.get_federation(id)` is `Some` — it has a durable
  registry row, whether or not its client is currently open) → **REFUSE** with an actionable error
  ("federation is still registered; if it is open, recovery would run a second client on one seed;
  if its partition won't open, that is an incident — do not recover over a surviving journal").
- Otherwise (UNregistered: fresh host, or a lost `journal.db`) → allocate `next_db_prefix()`
  (`multi_client.rs:386`) and recover into it.

**Why refuse the registered case, not just the open one (money-safety — adversarial review):** all
three legitimate recovery scenarios have the fed UNregistered at recovery time (fresh host and
lost-`journal.db` have no registry row; a disk-move has both stores and just reopens — no recovery).
The dangerous state is *registered-but-unopened* (`journal.db` survived, `client.db` lost): the
surviving journal still holds non-terminal `Pay`/`Move` intents, and reconcile auto-re-drives them.
Recovery gives a FRESH, EMPTY oplog — but the oplog IS the cross-restart send-dedup authority, so a
re-driven `Pay` misses the dedup and funds a SECOND outgoing contract for an invoice the gateway
already settled and holds the preimage for → **automatic double-pay, no operator action.** Refusing
any registered fed removes this by construction: recovery only ever runs where no journal (and thus
no surviving intent) exists. The corrupt-partition-with-surviving-journal case is handled as an
incident in the runbook (stop, back up, deliberately clear the journal, then recover), never
auto-recovered in V1.

Any pre-existing partition for an UNregistered fed is left **untouched and inert**: `open_all` only
opens *registry* rows, and `next_db_prefix` already scans raw partitions purely to take `max+1` so
it can never be reused for a different federation. Reclaiming orphans is a separate, deliberate GC
command — never automatic, and out of scope here.

This is the decision that removes the entire crash-window problem: nothing is destroyed, so there
is no window in which a crash leaves a federation with neither a registry row nor a partition.

### D4 — `MultiClient::recover` / `recover_inner`
Sibling of `join`/`join_inner` (`multi_client.rs:151-255`):
1. `preview = builder.preview(connectors, invite)` (as join, `multi_client.rs:200`).
2. Resolve the prefix per D3 (fresh, or refuse).
3. `client = preview.recover(self.client_db(prefix), self.root_secret.clone(), None)`.
4. Await completion via `wait_for_all_recoveries()` — which, on the patched SDK, returns `Err` on a
   failed module recovery instead of hanging. Surface progress from
   `subscribe_to_recovery_progress()` on a side task.
5. **Completion is stronger than that call returning**: the handle omits recovered modules from its
   live registry, so **reopen the partition and drain its state machines before registering**.
6. Only then commit: `put_federation(...)` AND **atomically record durable user ownership** — a
   `CandidateState::UserApproved` candidate row, exactly as a successful user `join` does
   (preserving an existing `UserApproved`). **Why (adversarial review):** without it, the recovered
   fed has no `UserApproved` row, so `auto_joined_candidates` treats it as probe-gated and discovery
   later seeds it as `AutoJoined`; under default policy no such fed is eligible as a spending source,
   so **automated allocation stays disabled for every recovered federation**. Recovery is a
   deliberate user action, so it confers user ownership just like `join`. This write is part of the
   same atomic `complete_recovery` dbtx as the registry row + intent terminalization. Under the
   recovery reservation, `join_lock`, and actor membership lease, await that transaction before
   publishing the exact reopened client into the process map. There is no await or cancellation
   point between observing `Ok(true)` and insertion: an explicit error or cancellation while the
   transaction is pending never exposes a handle. An ambiguous durable commit may leave a
   registered-but-unopened row; ending the lease invalidates older authority, and the scheduler's
   whole-world check skips money work until its normal open path restores the handle.
7. On a failure known to precede the atomic publication commit (SDK replay, reopen, or an explicit
   noncommit), the operation terminalizes as failed with the SDK's error; the fresh unregistered
   partition is abandoned inert (free, per D3), and a deliberate retry gets a clean prefix. A
   commit-then-error ambiguity is the exception described above: atomic `Done` + registry may have
   won, the later `Failed` fence loses, and the registered partition must be reopened rather than
   recovered into another prefix.

**Concurrency — the open path must respect the recovery reservation (adversarial review):** the
in-memory `active_recoveries` reservation must be honored by EVERY open path, not just `join_inner`.
The watch scheduler independently calls `open_all` → `open_one` (`scheduler.rs`) for every
registered-but-unopened fed each cycle, and `open_one` takes neither `join_lock` nor checks
`active_recoveries`. `open_one`/`open_all` MUST skip a fed in `active_recoveries`. The reservation
MUST be held from before prefix allocation through the in-memory `clients.insert` (covering the
window between `complete_recovery`'s registry write and the insert, where a scheduler cycle could
otherwise `open_one` the just-registered partition → two handles on one partition). Re-verify
`has_client` under `join_lock` immediately around the commit + insert so the final registration
cannot silently replace a client that went live meanwhile. (Note: D3's refuse-if-registered already
prevents the *old-partition-goes-live-during-replay* race for the pre-completion window, since a fed
under recovery has no registry row until `complete_recovery`; this reservation guard closes the
post-completion window.)

### D5 — Execution model: async, per ADR-0024
ADR-0024 is absolute: every operation's IO "runs in its own concurrent driver task, unbounded in
duration and unbounded BY EACH OTHER — nothing ever queues behind another operation's IO."
Recovery is long-running, so it **must not block the actor**. Follow the existing
`block_for_invoice` shape: admit the operation, return its operation key, drive it in a detached
task; the *caller* may wait under a deadline and otherwise polls. Because the federation is not
registered until recovery completes (D3), a running recovery is invisible to the allocator and
cannot interfere with it.

There is **no stall deadline, no watchdog, and no daemon exit**: with the patched SDK recovery is
complete-or-fail, so there is no indeterminate third state to arbitrate.

## Explicitly NOT built in V1
Dropped as consequences of the decisions above (all were built by the failed first attempt):
supersede-in-place and its wipe; the durable recovery marker, prefix reservation, and
interrupted-recovery resume; the orphan-partition scan (and with it the typed-DB-read panic
hazard); the seed-provenance fingerprint guard (its job was protecting a wipe that no longer
happens); the stall deadline, the scheduler watchdog, and the exit-for-supervised-restart path.

## Non-goals
Not seed-at-rest encryption (Phase 7). Not automatic recovery on startup. Not backup snapshots
(deprecated upstream). Not orphan GC. Not surviving a mismatched two-store restore.

## Verification
- **Unit**: recovery registers the federation only after completion; recovery into a fresh prefix
  never reuses or deletes an existing partition (extend
  `next_db_prefix_accounts_for_orphaned_client_partitions`, `multi_client.rs:1751`); recovery of a
  **registered** federation is refused (open OR registered-but-unopened — the D3 boundary);
  `complete_recovery` records a `UserApproved` candidate so the recovered fed is allocator-eligible;
  `open_one`/`open_all` skip a fed in `active_recoveries`; `decide()` never emits `Action::Recover`;
  `restore-mnemonic` refuses when a seed exists and rejects a bad checksum.
- **Live devimint (the gate — must pass):** `wallet-cli/tests/smoke_recover_devimint.sh`, covering
  BOTH admitted loss shapes and BOTH front ends, each asserting the balance is restored EXACTLY
  (integer equality, zero slack — recovery replays held ecash and charges no fee):
  - *Phase A, whole-store loss under `walletd`* — fund a federation, lose `client.db` AND
    `journal.db` (the seed-only/fresh-host path), then the runbook's disk-dies recipe verbatim:
    `init` → `restore-mnemonic` → start the daemon → `recover <invite>` → `await-move`, into a
    store the daemon itself reports empty first. This is the path an operator uses for real sats,
    and the only one where the detached D5 driver survives to finish the replay in one process.
  - *Phase B, lost `journal.db` only, standalone* — delete just the journal, leaving `client.db`
    (the seed plus phase A's now-ORPHANED partition), then `recover` + `await-move` again: proves
    live that a recovery over a real store still holding an orphaned partition completes and
    restores the same number, and that standalone's re-drive-on-await path finishes a recovery at
    all. (The fresh-prefix arithmetic itself stays unit-covered.)

  Both phases also assert the recovered fed is `UserApproved` (D4.6); phase B then spends from it
  over lnv2.
- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace` — **run inside `nix develop`** (bare cargo fails on a missing `cmake`).

## Why the first attempt failed (provenance)
An rb-lite run against the previous draft ran 25 rounds / ~19h / 4861 insertions and never
converged, with open P1s at the cap. Root cause: that draft mandated supersede-in-place (D3) while
deferring crash-resume as a non-goal (D5) — but the SDK rejects `recover` on an initialized DB, so
in-place *forces* wipe-before-rebuild, turning the deferred non-goal into a data-loss window. The
run then had to invent the marker/resume/guard/watchdog machinery to survive a window this spec
simply never opens. Decisions 1-5 above remove the cause rather than the symptom.

The salvage patch (`/tmp/rb-m9m-run/full-diff.patch`) is retained as a **reference**, not a base.
Mine it for the two discoveries that remain relevant — the completion-semantics fix (reopen +
drain before registering) and the seed-import verb — and ignore the rest, which exists to serve a
design V1 no longer has.
