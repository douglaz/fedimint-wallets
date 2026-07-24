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

### D3 — Always recover into a FRESH prefix; never wipe (decision 2)
V1 performs **no supersede-in-place**. Resolve the target prefix as:
- If the federation is **already open/live** → **refuse** with an actionable error ("already open;
  recovering would run a second client on the same seed — remove it deliberately first"). This
  matches the runbook's *one seed, one live `client.db`, one daemon*.
- Otherwise → allocate `next_db_prefix()` (`multi_client.rs:386`) and recover into it.

Any pre-existing partition for that federation is left **untouched and inert**: `open_all` only
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
6. Only then `put_federation(id, FederationInfo { invite, db_prefix, joined_at })`
   (`journal.rs:548`) and insert into the in-memory registry.
7. On failure: the operation terminalizes as failed with the SDK's error; the fresh unregistered
   partition is abandoned inert (free, per D3). The operator may simply retry — each attempt gets a
   clean prefix.

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
  `next_db_prefix_accounts_for_orphaned_client_partitions`, `multi_client.rs:1751`); recovery of an
  already-live federation is refused; `decide()` never emits `Action::Recover`;
  `restore-mnemonic` refuses when a seed exists and rejects a bad checksum.
- **Live devimint (the gate — must pass):** fund a federation, drop `journal.db`, then
  `restore-mnemonic` + `recover <invite>` on a clean store and assert the balance is restored after
  recovery completes.
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
