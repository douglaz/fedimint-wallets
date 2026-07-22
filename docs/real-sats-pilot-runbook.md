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

The seed recovers ecash *within* a federation; to get there you must rejoin each
federation by invite code first. Record them alongside (not with) the seed words:

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
  restores ecash but wipes the operation log — the client-side send dedup with it. A
  restore performed while a send was in flight can double-pay (the one real hazard,
  `docs/fedimint-mechanics.md` §4). Prefer keeping the disk alive over re-seeding.
- **Never run two wallets from one seed.** Two clients on the same seed are two
  processes spending the same notes; the federation will let exactly one win and the
  bookkeeping of both is garbage. One seed, one live `client.db`, one daemon.

### 4. Cap the exposure

Pilot policy: keep the total at an amount you are genuinely willing to lose. Suggested
starting point (~150k sats total ceiling across feds):

```bash
# 100k sats concentration ceiling per federation
# 50k sats float in the spending fed
# 20k sats in standby
# 50 sats absolute cap: evacuations + manual --fee-cap default
# 3% proportional cap on funding moves (top-up/standby)
wallet-cli policy set \
  --per-fed-cap 100000000 \
  --spending-target 50000000 \
  --standby-target 20000000 \
  --max-fee 50000 \
  --max-fee-bps-of-move 300
wallet-cli policy get              # verify what is actually stored
```

(Values are msat, except `--max-fee-bps-of-move`, which is basis points, 0-10000. Raise
them only after a clean first week.)

## Daily — the one-minute glance

```bash
# 1. The loss surface: a Stranded move (send settled, receive not credited) is the ONLY
#    state where money can be in limbo; Refunded pays are money-safe but user-visible.
wallet-cli history --limit 200 | grep -iE "stranded|refunded" || echo clean

# 2. Self-heal accounting: watchdog firings mean settlement silently died and the daemon
#    restarted itself. One is survivable news; recurring ones are an investigation.
journalctl --user -u walletd --since yesterday | grep -c "settlement stall" || true

# 3. Restart count (systemd's view):
systemctl --user show walletd -p NRestarts
```

## Incidents

- **Watchdog restart fired.** Expected self-heal path: the daemon exits, systemd
  restarts it, reconcile re-drives Awaiting operations to their true terminal. Money-safe
  by design. If it fires more than once a week, capture `journalctl` around the firing
  and treat it as a bug (the known upstream trigger is fixed at our pin; a new firing has
  a new cause).
- **A `stranded` row in history.** The move's send leg settled but the receive was not
  credited. Reconcile re-drives it on every pass and on every restart; give it time and a
  restart before touching anything. If it persists across a restart + an hour, preserve
  `journalctl` + `wallet-cli history` output and debug with the recv op-id from the move
  record — do not re-submit the move by hand (the executor's dedup is what is protecting
  you from a double-spend).
- **A pay came back `refunded`/failed after submission.** lnv2 permits ONE payment
  attempt per invoice: the wallet refuses a retry of that same invoice by design
  ("already consumed its single payment attempt"). Get a fresh invoice from the payee.
- **A federation signals shutdown.** The scheduler evacuates on its own (the 6a chain
  gate proves the path). Verify with `wallet-cli history | grep evacuation`, and check
  the destination fed's balance grew accordingly.
- **Disk dies.** New host: `walletd init`, restore is NOT copying words into a config —
  rejoin each federation from your recorded invites, then run fedimint recovery with the
  seed. Accept that any operation in flight at the moment of death may need manual
  reconciliation against the federations' view.

## Upgrades — a release that changes the stored-policy schema

This is a greenfield, pre-1.0 wallet: the persisted `Policy` has **no serde migration**. When a
release adds a required policy field (e.g. `max_fee_bps_of_move`), the policy row written by the
previous release no longer decodes, and `seed_policy` fails that decode **before the actor
starts** — so walletd will not come up after the upgrade until the stored policy is replaced.
There is no in-place `policy set` fix: `policy set` reads the existing policy first and hits the
same undecodable row.

The recovery leans on the two-store split — **`client.db` holds the ecash + seed; `journal.db`
holds only the operation ledger, policy, and federation registry** (no money). So you replace the
journal without touching the wallet:

```bash
# 0. Confirm nothing is in flight (wiping the journal discards in-flight op state):
wallet-cli history --limit 200 | grep -iE "started|awaiting" || echo "quiescent — safe"
# 1. Stop the daemon.
systemctl --user stop walletd            # or however it is supervised
# 2. Delete ONLY the journal store. NEVER delete client.db — that IS the wallet (see Never).
rm -f "$WALLETD_DATA_DIR/journal.db"
# 3. Restart. A fresh journal re-seeds the DEFAULT policy, so the daemon starts.
systemctl --user start walletd
# 4. Re-join every federation from your recorded invites (§2). The seed in the untouched
#    client.db lets fedimint recover each federation's ecash balance on rejoin.
wallet-cli join <fed1...>                 # once per fed
# 5. Re-apply your standing instruction — the whole Cap the exposure block (§4), INCLUDING the
#    new field, so the policy is not left at defaults.
wallet-cli policy set --per-fed-cap ... --max-fee-bps-of-move 300 ...
# 6. Verify.
wallet-cli policy get                     # the field is present and your values are back
wallet-cli balance                        # ecash recovered per federation
```

Wiping the journal loses operation **history** permanently and forces the rejoin above; it does
not lose money. If a settlement was in flight at step 0, reconcile it against the federations'
view by hand (same caveat as **Disk dies** under Incidents).

## Never

- Never run two daemons (or a daemon + `wallet-cli --standalone`) against the same seed
  or data dir. The RocksDB lock protects the same-host case; nothing protects a restored
  copy on a second host.
- Never delete `client.db` "to fix" a stuck state. Reconcile + restart is the fix;
  `client.db` IS the wallet.
- Never share `walletd mnemonic` output, the token file, or a disk image of the data
  dir. Each one is full spend authority.
