# br-m9m implementation spec: seed-based wallet recovery via `ClientPreview::recover`

Purpose: make br-m9m a fully-specified, no-longer-under-specified bead so it can be implemented
carefully (manual + dual-review + a live devimint gate — NOT rb-lite, per the bead's critical
constraint). All file:line refs verified against current `main`.

## The gap (confirmed)
Every join goes through `preview.join(self.client_db(prefix), self.root_secret.clone())`
(`multi_client.rs:214`, `InitMode::Fresh` → empty client). `ClientPreview::recover` is never
called. So a funded client cannot be rebuilt from the seed; a wiped journal orphans the funded
partition and `balance` reads 0.

## The SDK primitives (drop-in siblings of join)
- `ClientPreview::recover(db_no_decoders: Database, pre_root_secret: RootSecret, backup: Option<ClientBackup>) -> anyhow::Result<ClientHandle>` (`fedimint-client/src/client/builder.rs:1463-1489`). Pass `backup = None` — full epoch-history recovery; `download_backup_from_federation` is `#[deprecated]` (removed in v0.13). Same `db`/`secret` args the wallet already passes to `join`. Only difference vs join: `InitMode::Recover` vs `Fresh`.
- Completion gate: `Client::wait_for_all_recoveries() -> anyhow::Result<()>` (`client.rs:1841-1853`). Block on this before treating the client as usable / reading balance. `has_pending_recoveries()` (`client.rs:1826`) for a non-blocking check.
- Progress for UI only: `subscribe_to_recovery_progress() -> impl Stream<Item=(ModuleInstanceId, RecoveryProgress)>` (`client.rs:1859-1864`) — explicitly NOT for completion detection (may duplicate); use it only to surface progress, use `wait_for_all_recoveries` for done-ness.
- Recovery is crash-resumable from persisted `ClientModuleRecoveryState.progress` (`builder.rs:865-882`).

## Design decisions

### D1 — A distinct `recover` verb, never the allocator's job
Add a deliberate operator verb, NOT a flag folded into normal join, and NEVER emitted by
`decide()` (the allocator must never auto-recover — double-pay hazard, below):
- CLI: new `Command::Recover { invite }` (`wallet-cli/src/main.rs:82-86`), dispatched at the
  daemon-backed site (`main.rs:604`) and the standalone site (`main.rs:1321`).
- Daemon: new `POST /v1/recover` handler + `RecoverRequest { invite }` mirroring `join`
  (`wallet-daemon/src/handlers.rs:400-429`).
- Core: new `Action::Recover { federation, invite }` (sibling of `Action::Join`,
  `types.rs:169-176`). Keep it OUT of the allocator — user-initiated only. It is `is_executable`
  but only ever created from the recover verb, never from `decide()`.
  (`#[serde(default)]` is NOT needed on a brand-new variant, but if any existing serde-persisted
  enum gains a field, apply the forward-compat rule; a new variant is additively safe for
  serde_json.)
- Executor: the perform arm for `Action::Recover` calls a new `MultiClient::recover(invite)`
  (mirroring the `mc.join` call at `executor.rs:1056`).

### D2 — `MultiClient::recover_inner` (mirror join_inner, +recover +wait +partition reuse)
Add `recover` / `recover_inner` siblings to `join`/`join_inner` (`multi_client.rs:151-255`). The
sequence:
1. `preview = builder.preview(connectors, invite)` (same as join, `multi_client.rs:200-208`).
2. Resolve the target partition prefix (partition-reuse logic, D3 below).
3. `client = preview.recover(self.client_db(prefix), self.root_secret.clone(), None)`.
4. `client.wait_for_all_recoveries().await?` — BLOCK until every module finishes. While
   waiting, drain `subscribe_to_recovery_progress()` on a task and log/emit progress.
5. On success: `put_federation(id, FederationInfo { invite, db_prefix: prefix, joined_at })`
   (`journal.rs:548-560`) and insert the opened client into the in-memory registry.
6. On failure/deadline: tear down via `remove_client_partition_best_effort(prefix)` ONLY if we
   allocated a fresh prefix in this call (never delete a reused pre-existing partition on a
   transient failure — recovery is resumable; deleting would discard resumable progress).

### D3 — Partition reuse/supersede (never stack a third partition)
Resolve the prefix by federation-id, in priority order:
1. **Registered fed** — `journal.get_federation(id)` → `Some(info)` (`journal.rs:569-583`):
   REUSE `info.db_prefix`. Do NOT allocate. This is a supersede-in-place recovery. Because it
   overwrites a possibly-funded/in-flight client, gate it behind the double-pay guard (D4):
   refuse unless the fed has no in-flight Pending money op, or the operator passes `--force`.
2. **Unregistered but an orphaned partition exists for this fed** (disk-move / wiped-journal
   case): scan `raw_find_by_prefix(&[CLIENT_PREFIX_TAG])` (`multi_client.rs:386-416` shows the
   scan shape) and, for each partition, read its stored client-config federation id; if one
   matches `id`, REUSE that prefix (supersede in place). This needs a NEW helper
   `find_partition_for_federation(id) -> Option<u32>` — there is none today. **Implementation
   risk / fallback:** if reading a partition's fed-id without a full client init proves too
   invasive, fall back to an explicit operator flag `--into-prefix <K>` for the disk-move case,
   AND document that the safe default for "fresh host" recovery is a clean client db (no
   partitions), where case 3 applies. Do NOT silently allocate a new prefix when an orphan for
   the same fed exists — that creates two clients for one fed (double-spend/confusion). If the
   scan is skipped via fallback, the runbook MUST tell the operator to recover onto a clean db.
3. **Fresh host, nothing for this fed** — allocate `next_db_prefix()` (`multi_client.rs:386-416`,
   monotonic max+1, already accounts for orphaned partitions), recover into it, register.

### D4 — Double-pay guard (recovery wipes the oplog)
`docs/fedimint-mechanics.md:65-69` and `:107-112`: recovery restores ecash but WIPES the
operation log, destroying the client-local send-dedup that `send()` relies on; a
recover-while-send-in-flight can re-issue a payment the federation will not dedup (it keys
outgoing contracts by funding outpoint, not payment hash). Therefore:
- Before a supersede-in-place recovery (D3 case 1/2), check the journal for any Pending/Awaiting
  money op (send/move/pay) on the target fed. If present, REFUSE with an actionable error unless
  `--force` is passed. Log the exact op ids.
- Recovery is a deliberate, documented LAST RESORT; it is never automatic and never on the
  driver's retry/auto-join paths (`runtime.rs:3415` auto-join must keep calling join, not
  recover).
- The runbook's "Disk dies" / upgrade-recovery steps point at `recover <invite>` and state the
  in-flight-send caveat.

### D5 — Waiting UX (V1: blocking foreground)
V1 keeps it simple: `recover` blocks until `wait_for_all_recoveries()` returns, emitting periodic
progress. The CLI shows progress and, on completion, prints the restored per-fed balance. Register
the fed on SUCCESS only (a crashed mid-recovery run is re-run from scratch for V1 —
crash-resume-on-open is a follow-up, consistent with the non-goal "not automatic recovery on
startup"). If the daemon path can't hold a long request open, the daemon returns an operation id
and the CLI polls a `recover status` until `wait_for_all_recoveries` completes; pick whichever
matches the existing long-op pattern (`block_for_invoice` style) — decide at implementation from
the handler timeout budget.

## Non-goals (unchanged from the bead)
Not seed-at-rest encryption (Phase 7). Not automatic recovery on startup. Not backup snapshots
(deprecated upstream). V1 crash-resume of an interrupted recovery is out (re-run instead).

## Verification (the gate — must be run; NOT rb-lite)
- Live devimint: fund a fed, drop the journal (or point at a fresh client db), `recover <invite>`,
  assert balance is restored after `wait_for_all_recoveries` completes.
- Unit: the recover path (a) registers the fed, (b) reuses an existing prefix for a
  registered/orphaned fed rather than allocating a new one (extend the
  `next_db_prefix_accounts_for_orphaned_client_partitions` test at `multi_client.rs:1751-1768`),
  (c) refuses supersede-in-place when an in-flight send exists unless forced.
- `cargo test && cargo clippy -- -D warnings && cargo fmt --check`.

## Implementation stance
Money-adjacent + async recovery = money code. Implement MANUALLY with dual-review (codex + fable)
and the live devimint gate. This spec removes the under-specification; the interaction decisions
above (D1-D5) are the ones a fresh agent would otherwise have to guess.
