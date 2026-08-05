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
# 1. Unresolved-money terminals this check can identify: a Stranded move (send settled,
#    receive not credited), plus lnv2's ambiguous send/receive `Failure` terminals. A send
#    `Failure` does not distinguish rejected funding from an incomplete refund; a receive
#    `Failure` does not distinguish a rejected claim from accepted-but-failed note issuance.
#
#    DO NOT grep history for "stranded" — that word NEVER appears in its output. `Stranded`
#    shares the terminal `failed` surface (move_protocol.rs §3), and the ten-column TSV carries
#    no error field, so a naive grep prints "clean" even while funds are stranded.
#
#    Terminal-failed money ops are the candidate set; `show` carries the distinguishing error.
#    Collect first, THEN judge -- and distinguish "inspected everything, found nothing" from
#    "could not inspect". `... done || echo clean` was wrong twice over (the loop's status is its
#    LAST body command's), and a verdict that ignores exit codes is wrong a third way: a bouncing
#    daemon makes `show` fail exactly when a stranded move is most likely to exist.
if ! hist=$(wallet-cli history --limit 200); then
  echo "CHECK FAILED - could not read history; rerun before trusting a clean result"
else
  failed=0; stranded=""; ambiguous=""
  for key in $(printf '%s\n' "$hist" | awk -F'\t' \
      '($3=="move"||$3=="evacuation"||$3=="pay"||$3=="receive"||$3=="direct-inflow") && $4=="failed" {print $10}'); do
    if ! detail=$(wallet-cli show "$key"); then failed=1; continue; fi
    case "$detail" in
      *"send settled but receive was not credited"*) stranded="$stranded$key ";;
      *"send failed:"*|*"receive failed:"*) ambiguous="$ambiguous$key ";;
      # Rows written BEFORE the diagnostics were reworded keep the old text. Ledger error
      # strings are persisted, not re-derived (journal.rs CAS falls back to MoveRecord.outcome),
      # so a terminal row from an earlier release still carries this wording. One pattern covers
      # the legacy send and receive forms.
      *"programming error or malicious federation"*) ambiguous="$ambiguous$key ";;
    esac
  done
  if [ "$failed" = 1 ]; then
    echo "CHECK FAILED - could not inspect every candidate; rerun"
  elif [ -n "$stranded$ambiguous" ]; then
    if [ -n "$stranded" ]; then
      echo "STRANDED - investigate immediately: $stranded"
    fi
    if [ -n "$ambiguous" ]; then
      echo "AMBIGUOUS TERMINAL - stop the daemon, preserve its data directory, investigate: $ambiguous"
    fi
  else
    echo clean
  fi
fi

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
- **A stranded move** (found by check 1 above — it appears as a `failed` move whose `show`
  error reads "send settled but receive was not credited"). **This entry is the canonical
  account of what `Stranded` means.** Code comments deliberately point here instead of carrying
  their own explanation, because every previous attempt to enumerate causes in a comment was
  later shown to be wrong. The state has never been observed in the pilot.

  **What it asserts.** Exactly one observation: the send leg reached a SETTLED terminal, and the
  receive leg reached an op-terminal NON-claim (the invoice expired, or lnv2 yielded its single
  `Failure`). That is all.

  **What it does NOT assert.**
  - *Not a gateway fault.* A misbehaving gateway alone cannot produce it. The send leg reaches
    `Success` only against a preimage verified against the outgoing contract, or via the source
    federation's guardians, whose `await_preimage` is populated only on a verified claim. The
    destination's preimage is derived client-side and never transmitted; the only copy that
    leaves the client is threshold-encrypted to the destination's guardians. The gateway cannot
    open that ciphertext before the destination funding transaction is accepted. Only then can it
    collect guardian decryption shares and decrypt the preimage, so it cannot obtain the preimage
    while skipping accepted destination funding.
  - *Not proof of loss, and not proof of malice.* A receive `Failure` proves very little: lnv2
    yields it whenever the mint outputs fail to come back, which collapses two structurally
    different states — the containing transaction was REJECTED (so this wallet's claim changed
    nothing, which is NOT the same as the contract being unclaimed), and the transaction was
    ACCEPTED but note issuance then failed. It therefore does not prove the
    contract was claimed, does not prove funds reached the destination, and does not exclude a
    dishonest federation.

  The honest operator statement is **"not proven lost, and not proven recoverable."** State the
  uncertainty; do not pick a story to fill it.

  **The preimage is NOT the recovery instrument.** It claims the SOURCE's outgoing contract —
  which a settled send already accounts for — and spending that contract is additionally
  authorised to `claim_pk`, not by the preimage alone. Nothing about it credits the destination.
  Where recovery is possible at all, it depends on the destination's **complete client state**.
  If the claim transaction was rejected, the receive state machine's `contract`,
  `claim_keypair`, and `agg_decryption_key` may matter. If the claim was accepted but note
  issuance failed, the mint-output state and seed-derived note material may matter instead.
  The receive terminal does not distinguish those branches. Preserve the whole data directory;
  do not go hunting for the preimage or build tooling to extract it, because it cannot recover
  either branch.

  **`Stranded` is TERMINAL — waiting and restarting will NOT repair it.**
  `reconcile` re-drives `pending()` only (`Pending`/`Executing`); `Failed`/`Permanent` stay
  terminal and `Awaiting` is subscription-owned, so nothing re-drives a stranded move. Do not
  burn an hour waiting for a self-heal that cannot come. Instead, act in this order:

  1. **STOP the daemon and preserve the data directory before anything else.** This is an
     EVIDENCE-PRESERVATION step, not a recovery procedure — there is no recovery procedure to
     run. The directory holds the destination's complete client state for both failure branches;
     losing it forecloses whatever options exist.
  2. **Check whether another wallet instance has copied or snapshotted in-flight client state.**
     This is the FIRST diagnostic — step 1 is preservation, not diagnosis — because it is the
     cheapest thing to rule out and the most severe if true, **not** because it is the
     established cause. It is a hypothesis until you measure it. Note
     what is *not* sufficient: the seed alone. lnv2 declares `type Backup = NoModuleBackup` and
     starts a receive state machine only when handed the specific randomized contract, so a
     seed-only recovery does **not** resume an in-flight receive. Producing a second claimant
     takes duplicated in-flight client state or explicit access to the contract and claim
     material.

     *How to run it.* (a) What the store lock does and does NOT rule out. The lock is a FILE
     INSIDE the data directory (`<data_dir>/client.db.lock`, `wallet-cli/src/main.rs:1222-1240`),
     so it only excludes two processes opening **that same directory**: a second `walletd` waits
     for the lock and resumes only after the owner exits, and `wallet-cli --standalone` fails
     immediately with "another process owns the wallet store." It does NOT make the host safe. A
     restored backup or cloned volume mounted at a DIFFERENT path on this same host has its own
     lock file and runs concurrently with the original — which is exactly the competing claimant
     being diagnosed. (b) So enumerate every copy of the data directory that has ever existed,
     **on this host and off it** — a restored backup, a disk
     or volume snapshot, a container volume clone, a copy carried to a second machine — and for
     each one establish whether any wallet process was ever pointed at it. (c) Bound it by time:
     `wallet-cli show <key> --standalone` dates the move (the daemon is stopped from step 1, and
     plain `show` talks to it over HTTP with no silent fallback, so it would simply fail here),
     and a copy is cleared only if no wallet process HELD IT OPEN AT ANY POINT during that
     window. That is NOT the same test as "no process opened it during the window": a daemon
     started BEFORE the move and still running through it never opens anything inside the
     window, yet is exactly the claimant you are hunting. Compare process LIFETIMES against
     the window (start before/end after both count as overlap), not open events within it. If
     a copy's history cannot establish that, treat it as unresolved rather than cleared.
     If every copy is accounted for, the hypothesis is ruled out — write that down and go to
     step 3. If you do find a duplicated claimant, it is a route *into* the rejected-transaction
     case above, not a separate parallel cause.
  3. **Collect what the shipped commands actually give you.** With the daemon stopped, every
     `show` here needs `--standalone`. `wallet-cli show <key> --standalone` gives the error
     detail AND the send and receive op-ids. That is the extent of it — `show --standalone` stops at
     `send_op`/`recv_op`/`gateway`; the shipped commands expose no recovery procedure, and the
     preimage is not one.
  4. **Do NOT re-submit the move by hand.** The executor's dedup is what is protecting you from
     a double-spend.

  **On forfeit.** A forfeited send NORMALLY ends `Refunded`, not stranded: the gateway's cancel
  signature drives an immediate refund. But "forfeit can never strand" is FALSE. If the gateway
  incorrectly claimed the outgoing contract and the refund transaction is therefore rejected, the
  SDK re-checks `await_preimage` and PROMOTES the leg to send-`Success`
  (`lnv2-client/lib.rs:705-725` at our pin). That promoted success can then meet a non-claimed
  receive and land here. Do not reason from "it was forfeited, so it cannot be stranded."

- **An ambiguous send or receive terminal** (flagged by check 1 above). A send `Failure` can be
  the SIBLING exit of the forfeit block, and it does not land in `Stranded`. If the refund does not
  complete and no preimage is available either, the SDK yields send-`Failure`
  (`lnv2-client/lib.rs:725`), which this wallet records as a plain `MovePhase::Failed`
  (`wallet-fedimint/src/executor.rs:1583-1585`) — the same terminal it uses for a send whose
  funding transaction was simply rejected. In that second case nothing was ever funded and no
  money moved; in the first the outgoing contract WAS funded and its position is unresolved. The
  operation state does not say which one happened. So a `failed` move whose error starts with
  `send failed:` is NOT evidence the money stayed put.

  A receive `Failure`, including a raw receive or direct inflow, has the two mint-leg meanings
  documented in the stranded entry above. It likewise does not establish whether the incoming
  contract was claimed or whether destination notes can be recovered. In either case, **STOP the
  daemon and preserve the complete data directory before concluding anything**; the check cannot
  tell which underlying branch occurred, and selecting one artifact early can discard the state
  needed by the other.
- **A pay came back `refunded`/failed after submission.** lnv2 permits ONE payment
  attempt per invoice: the wallet refuses a retry of that same invoice by design
  ("already consumed its single payment attempt"). Get a fresh invoice from the payee.
- **The Lightning gateway is unavailable.** Automated movement STOPS: every move and every
  evacuation needs one gateway serving both federations, and a second gateway is a deliberate
  non-goal, so there is no alternate route. Funds are not at risk — nothing is in flight, balances
  are untouched, and the allocator refuses rather than half-moving — but do not expect evacuation
  to rescue you from a gateway outage, because it depends on the same gateway. Wait for it to
  return; if it will not, moving funds is a manual operation.
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

A release that ADDS a policy field marks it `#[serde(default = "default_<field>")]` with a NAMED
default function returning the shipped value (the repo's pattern — `wallet-api/src/lib.rs:9`), so
a policy row persisted by a previous release still decodes and walletd starts normally. A PLAIN
`#[serde(default)]` on a numeric field is a BUG: it yields ZERO, not the shipped default, and a
zero fee cap or zero threshold silently disables the thing it was meant to bound. Just deploy and restart; re-run `policy set` afterward only if you want to set the new
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
  or data dir. The store lock protects ONLY processes opening the same data directory —
  it is a file inside that directory (`<data_dir>/client.db.lock`). A restored backup or
  cloned volume at another path is NOT protected, on this host or any other. Do not read
  "same host" as safe; see the stranded-move incident entry.
- Never delete `client.db` "to fix" a stuck state. Reconcile + restart is the fix;
  `client.db` IS the wallet.
- Never share `walletd mnemonic` output, the token file, or a disk image of the data
  dir. Each one is full spend authority.
