# Real-sats pilot runbook

The first deployment of walletd with real money. Scope: **small amounts on a single-user
host you treat as hot-wallet-grade** — anyone who can read the disk owns the funds until
Phase 7 lands seed-at-rest encryption. Gate history: Phase 6a steps 1–7, the upstream lnv2
fix (fedimint PR #8816) pinned and burned in (24h soak + 6 smoke gates + 4h soak on the
exact shipped pin, all clean — evidence in `~/p/soak-24h-artifacts/`).

## Day 0 — before the first real sat

### 1. Back up the seed (once, on paper)

The 12-word mnemonic lives ONLY inside `client.db`. Losing that store without a written
backup loses the funds. `walletd mnemonic` blocks on the store's exclusive lock, so the
daemon must be stopped — that is deliberate (secrets are revealed only while stopped, the
same rule as `init`'s token rotation).

```bash
systemctl --user stop walletd
walletd mnemonic          # 12 words on stdout; warnings go to stderr
systemctl --user start walletd
```

Write the words on paper (twice; store separately). Then verify your transcription by
running `walletd mnemonic` again and comparing word-for-word. Do NOT photograph them, do
NOT put them in cloud storage or a password manager synced to one.

### 2. Back up the federation set (whenever it changes)

The seed recovers ecash *within* a federation; recovery needs each federation's invite
code. Record them alongside (not with) the seed words:

```bash
wallet-cli list-feds      # one line per fed: <id> invite=<fed1...> joined_at=<ts>
```

Re-run and re-record after every join. An invite code is not secret (it names the
federation's guardians), but without it recovery means hunting guardians down by hand.

### 3. What the backup does and does not cover

- **Seed + invites** recover the ecash. That is the money.
- **`journal.db`** (operation history, policy, move records) is bookkeeping — losing it
  loses your records and in-flight-operation bookkeeping, not settled funds.
- **Recovery from seed is a LAST resort, not a routine restore.** Fedimint recovery
  rebuilds ecash into a fresh client partition but does not reinstate the operation log —
  or its client-side send dedup. Recovery performed while a send was in flight can
  double-pay (the one real hazard, `docs/fedimint-mechanics.md` §4). Prefer keeping the
  disk alive over re-seeding.
- **Never run two wallets from one seed.** Two clients on the same seed are two
  processes spending the same notes; the federation will let exactly one win and the
  bookkeeping of both is garbage. One seed, one live `client.db`, one daemon.

### 3a. Prefer WSS-transport federations

Choose federations whose guardians speak the **WebSocket (WSS)** API transport. The iroh
transport's long-poll can STALL on sustained waits — a cross-fed Move's receive-claim await
over an iroh federation hung indefinitely in the 2026-07-19/20 incident, recovered only by
the daemon's `perform` timeout re-driving it with a fresh await. WSS federations avoid that
failure mode entirely. (The daemon still bounds each `perform` via
`WALLETD_PERFORM_TIMEOUT_SECS` — 120s in the shipped k8s config — so even an iroh stall
self-recovers in ~2 min, but prefer WSS so the stall does not happen in the first place.)

### 4. Cap the exposure

Pilot policy: keep the total at an amount you are genuinely willing to lose.

**There is no aggregate ceiling in code — `Policy` enforces `per_fed_cap` only.** The total
exposure a policy permits is therefore `per_fed_cap × (number of joined federations)`, so the
per-fed number is what you must size to reach a total you can accept. For the two-federation
pilot, 75k sats per fed is what makes the enforced caps imply a ~150k sat total:

```bash
# 75k sats concentration ceiling per federation
#   -> with the pilot's TWO feds this is the ~150k sat total ceiling.
#   Joining a third fed raises the permitted total to 225k: re-run `policy set` with a
#   lower per-fed cap BEFORE joining, or the ceiling silently moves.
# 50k sats float in the spending fed
# 20k sats in standby
# 50 sats absolute cap: evacuations + manual --fee-cap default
# 3% proportional cap on funding moves (top-up/standby)
wallet-cli policy set \
  --per-fed-cap 75000000 \
  --spending-target 50000000 \
  --standby-target 20000000 \
  --max-fee 50000 \
  --max-fee-bps-of-move 300
wallet-cli policy get              # verify what is actually stored
```

Keep `auto_join` off (the default) for the pilot: automatic federation discovery would raise
the permitted total without an operator deciding to.

(Values are msat, except `--max-fee-bps-of-move`, which is basis points, 1-10000. Raise
them only after a clean first week.)

## Daily — the one-minute glance

```bash
# 1. The loss surface: a Stranded move (send settled, receive not credited) is the ONLY
#    state where money can be in limbo; Refunded pays are money-safe but user-visible.
wallet-cli history --limit 200 | grep -iE "stranded|refunded" || echo clean

# 2. Self-heal accounting: watchdog firings mean settlement silently died and the daemon
#    restarted itself. Since the invoice-expiry fix, an open UNPAID invoice on a quiet
#    pilot no longer false-fires this (a receive counts only once its invoice has actually
#    EXPIRED), so a firing is now a reliable signal — one is survivable news, recurring
#    ones are an investigation.
journalctl --user -u walletd --since yesterday | grep -c "settlement stall" || true

# 3. Restart count (systemd's view):
systemctl --user show walletd -p NRestarts

# 4. Liveness: /v1/health ALWAYS returns HTTP 200 (even with a dead scheduler) — the truth
#    is in the BODY, not the status code. Parse `scheduler_alive`; never trust a 200 alone.
curl -s -H "Authorization: Bearer $(cat "$WALLETD_TOKEN_PATH")" \
  http://127.0.0.1:9736/v1/health \
  | jq -e '.scheduler_alive == true' >/dev/null && echo alive || echo "SCHEDULER DOWN"
```

The `/v1/health` status-code-is-always-200 shape is deliberate (the API requires the bearer
token even for health), so any uptime monitor or k8s probe pointed at it MUST assert
`scheduler_alive` in the JSON body — a check that only looks at the HTTP status will read a
scheduler-dead daemon as healthy.

## Incidents

- **Watchdog restart fired.** Expected self-heal path: the daemon exits, systemd
  restarts it, reconcile re-drives Awaiting operations to their true terminal. Money-safe
  by design. If it fires more than once a week, capture `journalctl` around the firing
  and treat it as a bug (the known upstream trigger is fixed at our pin; a new firing has
  a new cause).
- **A `stranded` row in history.** The move's send leg settled but the receive was not
  credited. **`Stranded` is TERMINAL — waiting and restarting will NOT repair it.**
  `reconcile` re-drives `pending()` only (`Pending`/`Executing`); `Failed`/`Permanent` stay
  terminal and `Awaiting` is subscription-owned, so nothing re-drives a stranded move. Do not
  burn an hour waiting for a self-heal that cannot come. Instead, act immediately: preserve
  `journalctl` + `wallet-cli history` output, and recover using the durable artifact the move
  record saved for exactly this case — **the payment preimage** — together with the recv op-id.
  The preimage is proof the send settled and is what a gateway/federation operator needs to
  reconcile the un-credited leg. Do NOT re-submit the move by hand (the executor's dedup is
  what is protecting you from a double-spend).
- **A pay came back `refunded`/failed after submission.** lnv2 permits ONE payment
  attempt per invoice: the wallet refuses a retry of that same invoice by design
  ("already consumed its single payment attempt"). Get a fresh invoice from the payee.
- **A federation signals shutdown.** The scheduler evacuates on its own (the 6a chain
  gate proves the path). Verify with `wallet-cli history | grep evacuation`, and check
  the destination fed's balance grew accordingly.
- **Disk dies.** On the new host, keep the daemon stopped and restore in this exact order:

  ```bash
  walletd init
  walletd restore-mnemonic < seed.txt
  systemctl --user start walletd

  STARTED=$(wallet-cli recover "$FEDERATION_INVITE")
  RECOVERY_KEY=${STARTED#started }
  wallet-cli await-move "$RECOVERY_KEY" --timeout 86400
  ```

  Repeat `recover` + `await-move` for each recorded invite. **Do not run `wallet-cli join`
  first**: join opens a fresh, empty client for that federation, and recovery correctly
  refuses an already-open federation rather than run two clients on the same seed.
  `walletd init` does not mint a seed; only daemon startup does. Starting before
  `restore-mnemonic` therefore mints a new random seed, after which import refuses to
  overwrite it. If that happens, stop the daemon and start again with a clean data
  directory. Recovery always allocates a fresh prefix and never deletes or reuses an old
  partition. Accept that any operation in flight at the moment of disk loss may need
  manual reconciliation against the federations' view.

## Upgrades — a release that changes the stored-policy schema

A release that ADDS a policy field marks it `#[serde(default)]`, so a policy row persisted by a
previous release still decodes (the new field adopts its shipped default) and walletd starts
normally. Just deploy and restart; re-run `policy set` afterward only if you want to set the new
field to a non-default value.

**Do NOT try to "reset" a stuck policy by wiping `journal.db`.** The federation registry
(federation id → client db-prefix) lives in `journal.db`; wiping it deliberately discards
bookkeeping and leaves the funded client partition inert. `wallet-cli join` then allocates a fresh
EMPTY partition, while `wallet-cli recover` is a last-resort seed-recovery path for actual store
loss, not a policy-reset mechanism. Restoring the backup brings back the same undecodable row. If a
policy row ever fails to decode on a real deployment, stop and treat it as an incident — do not
wipe (see Never).

## Never

- Never run two daemons (or a daemon + `wallet-cli --standalone`) against the same seed
  or data dir. The RocksDB lock protects the same-host case; nothing protects a restored
  copy on a second host.
- Never delete `client.db` "to fix" a stuck state. Reconcile + restart is the fix;
  `client.db` IS the wallet.
- Never share `walletd mnemonic` output, the token file, or a disk image of the data
  dir. Each one is full spend authority.
