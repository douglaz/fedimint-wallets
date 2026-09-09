# 08 — Hosts and deployment

How the engine is run: the `walletd` daemon, the standalone one-shot mode, the browser sidecar
as it exists today, the build, and the one long-running deployment. `ADR-0031` names the roles:
the **engine** decides, a **host** drives it, a **frontend** talks to it and never schedules or
admits work.

## The daemon

**HST-1** `walletd` is the only resident host. One process owns both stores under an exclusive
lock (`STO-2`); every other process is a client of its HTTP API (`04-api-contract.md`).

**HST-2** Subcommands: none (serve), `init`, `mnemonic`, `restore-mnemonic`. Config file:
`$XDG_CONFIG_HOME/walletd/walletd.toml`, else `~/.config/walletd/walletd.toml`; `--config`
overrides.

**HST-3** `walletd.toml` has five keys and rejects any other — including the retired `gateway`
pin, so a file from the pinned era fails startup loudly (`ADR-0030`):

| key | default |
|---|---|
| `data_dir` | `$XDG_DATA_HOME/walletd`, else `~/.local/share/walletd` |
| `address` | `127.0.0.1` |
| `port` | `9736` |
| `token_path` | env `WALLETD_TOKEN_PATH`, else the key, else `<data_dir>/token` |
| `log_level` | `info` (`RUST_LOG` overrides) |

Paths must be absolute (`~` expanded). Two other environment knobs exist:
`WALLETD_PERFORM_TIMEOUT_SECS` (default 600, `0` disables, garbage falls back to the default) and
`WALLETD_SETTLEMENT_STALL_SECS` (default 300, the watchdog deadline, `ALC-40`).

**HST-4** `walletd init`: read-or-default the config and write every key back canonicalized;
create the data directory `0700`; open `client.db` (blocking on the lock if the daemon is
running) and `journal.db`; seed `Policy::default()` if absent (`STO-13`); mint and write the
token `0600`; write `~/.config/walletd/client.toml` `{url, token_path}` for the CLI. It does
**not** mint a seed.

**HST-5** `walletd restore-mnemonic` reads twelve BIP-39 words from stdin only, refuses if a
seed already exists, and stores the entropy. The documented order is `init → restore-mnemonic →
serve`, because serving on a store with no seed **mints a random one** (`SEC-10`). `walletd
mnemonic` prints the twelve words to stdout while the daemon is stopped.

**HST-6** Serve sequence, in order: load config → tracing to stderr → chmod the data directory
`0700` → read the token (fail if empty) → open `client.db` first (the exclusivity anchor) then
`journal.db` (a separate RocksDB, deliberately: a 24-hour soak showed write conflicts when they
shared one) → journal → load or generate the mnemonic → `MultiClient` and `open_all` (per
federation, tolerant of one failing) → **bind the port before starting the service**, so a port
conflict fails before the scheduler admits work → start the actor and scheduler → build a second
detached `Runtime` with no perform timeout for the `/v1/status` dry run → serve.

**HST-7** Shutdown on SIGTERM or SIGINT, on the server task exiting, or on a critical service
task exiting (named in the log): stop intake → abort driver tasks → drain the actor mailbox
(parked long-polls drain with an error) → exit. A fatal exit is non-zero so `Restart=on-failure`
restarts it.

**HST-8** Logging is `tracing` to stderr. No daemon code path logs the token or the seed.
`walletd mnemonic` prints the seed to stdout by design.

## The standalone mode

**HST-9** `wallet-cli --standalone` is the documented admission exception in `ADR-0031`: a
one-shot process that takes the exclusive lock, opens both stores, and drives `Runtime`
directly. It refuses to start if the lock is held ("another process owns the wallet store").
It resolves `data_dir` from `--data-dir`, else `walletd.toml`, else the default.

**HST-10** Standalone-only verbs: `discover`, `probe`, `tick`, `status` (the richer rendering),
`history --fed`, and the `--gateway` break-glass. The break-glass is accepted on `pay`,
`receive`, `move`, `direct-inflow` and the three await verbs, where it is armed for that ONE
operation's key; it is a usage error on `tick`, `probe`, `discover`, `status` and `reconcile`;
it is ignored on verbs that resolve no route (`ADR-0030`). Standalone `tick` and
`status` accept ephemeral policy overrides (`--per-fed-cap`, `--evac-fee-*`, `--occurrence`, …)
that are validated but never persisted (`ALC-3`).

**HST-11** Standalone `tick` and `status` refuse an incomplete federation registry before
opening or planning; explicit user and admin verbs keep their poison-tolerant behaviour
(`ALC-46`).

**HST-12** Standalone is not a second resident engine and is not the model for a future host.
`Runtime::watch_once` is a dev/test harness (`ADR-0031`); no production scheduler is built on
it, and `F17` tracks demoting it further.

## The CLI as a frontend

**HST-13** `wallet-cli` in client mode holds no state beyond the pointer file. It never opens a
store. `04-api-contract.md` `API-25`–`API-31` own its behaviour.

## The browser sidecar as built

**HST-26** `wallet-web` exists as a crate with config, `init`, password hashing, and a
fail-closed startup, and serves **zero routes**. Specifically:

- `wallet-web init` prompts for a password twice on the TTY (never stdin), enforces at least 12
  characters and at most 1,024 **bytes**, hashes with Argon2id (m = 19456 KiB, t = 2, p = 1), and writes
  `~/.config/wallet-web/wallet-web.toml` `0600` atomically into a directory it creates `0700`
  or verifies is owned by the running uid and not group- or other-writable (a `0755` directory passes).
- The config has exactly `port, daemon_url, token_path, password_hash, session_idle_timeout,
  session_absolute_timeout, public_origin`; there is no bind-address key (the bind is hardcoded
  `127.0.0.1`) and no log-level key (`RUST_LOG`).
- Startup refuses: a config with group/other permission bits; a missing or malformed PHC hash;
  Argon2 parameters below the pinned minimums; port 0; a `daemon_url` that is not `http://` +
  a loopback IP **literal** + port (`localhost` is refused because it resolves but can be
  repointed); a relative `token_path`; timeouts above 4h idle / 24h absolute; a `public_origin`
  that is not a strict canonical origin.
- It has no HTTP client and no dependency on the wallet crates; `token_path` and
  `password_hash` are dead at runtime. It is not a flake output.

Everything else in `docs/phase6c-web-frontend-plan.md` — login, sessions, CSRF, the read and
money surfaces, `/healthz` — is unbuilt (`F27`).

## Build and continuous integration

**HST-14** The workspace builds only inside the repository's Nix devshell; bare `cargo` fails on
native dependencies. The gate is

```
nix develop -c bash -c 'cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace'
```

**HST-15** `flake.nix` outputs `packages.walletd` (default), `packages.wallet-cli`,
`packages.curl` (a pinned curl for the runbook), and `packages.walletd-image`, a layered OCI
image named `registry.galtland.network/walletd/walletd:latest` containing `walletd`,
`wallet-cli`, busybox and CA certificates with `walletd` as entrypoint. No `wallet-web`
output.

**HST-16** CI (`.github/workflows/ci.yml`) runs two jobs on push to `main` and on pull
requests, both in the devshell with SHA-pinned actions:

- **gate**: `Cargo.lock` unchanged (checked before any cargo step, against the cache action), `cargo fmt --check`, `cargo clippy --workspace --all-targets
  --locked -- -D warnings`, `cargo test --workspace --locked`, and (from this change)
  `bash docs/spec/tools/check-all.sh`.
- **nix-build**: build `walletd`, `wallet-cli` and the image; assert the image archive is
  non-empty; run both binaries with `--help`.

**HST-17** No live devimint smoke runs in CI, by explicit policy in the workflow: live
federations are too slow, and a job that silently skips is worse than none. The smokes are
manual gates; their last-green evidence lives in the closing issue's notes, and for one smoke in
its header (`CNF-39`).

**HST-18** The unit and integration suite at `ab52094` (PR #40's head, containing `main`
`1e44487`) is 1,071 tests
passing under the gate. About 60% of `wallet-fedimint`'s lines are test code.

## How the daemon is run

**HST-19** The repository ships one deployment artefact besides the image: a systemd **user**
unit `wallet-daemon/deploy/walletd.service` (`ExecStart=%h/.cargo/bin/walletd`,
`Restart=on-failure`, `RestartSec=2`, `TimeoutStopSec=90`, `After=network-online.target`).
Install is `cargo install --path wallet-daemon` then `walletd init`.

**HST-20** There is **no Kubernetes manifest in this repository.** The runbook's reference to a
"shipped k8s config" with a 120-second perform timeout describes a deployment held elsewhere;
the only tracked unit has that environment line commented out at the default 600.

**HST-21** Data directory layout:

```
<data_dir>/              0700, re-asserted on every start, init and restore
  client.db/             the federation clients' RocksDB, including the seed row (plaintext, SEC-10)
  client.db.lock         the exclusivity anchor
  journal.db/            the app journal: intents, moves, ledger, registry, candidates, policy, watch state
  token                  0600, 64 hex characters
~/.config/walletd/walletd.toml    host config, no secrets
~/.config/walletd/client.toml     {url, token_path} for the CLI
~/.config/wallet-web/wallet-web.toml   0600, the Argon2id hash
```

**HST-22** The readiness poller `ops/walletd-watch.py` (standard library only) reads
`/v1/health`, `/v1/balance` and `/v1/status`, and exits `0` quiet, `1` on any alert, `2` when
unreachable or without a token. It alerts on `scheduler_alive == false`, on
`automation_ready == false` (with the reason and detail), and on every deferred funding goal
whose floor is the route floor rather than the protocol minimum. An absent `automation_ready`
(an older daemon) is a note, not an alert. `--state` suppresses output only when there is no
alert and nothing changed; a **standing alert is printed, exits 1, and is posted to the webhook on
every pass**, so the script's own docstring ("pages on transition") overstates it. **Nothing runs it** (`F14`).

## The long-running deployment

**HST-23** One instance of `walletd` has been running the 2026-07-26 build `b5f46de`,
holding a small real-sats balance across two mainnet federations, with real receive, pay, move
and join history. It is a **test deployment**, not production; the operator has said so, and
this set describes it accordingly. Its location, namespace, image digest and balance are
deliberately not recorded in tracked files (`SEC-20`, `DEF-22`).

**HST-24** `main` is 220 commits past that build. Everything in `09-known-defects.md` from
`DEF-3` onward, and every requirement this set marks as landing with PRs #30–#44, is
**unexercised against a real federation** except through the devimint smokes. The deployed
build predates the readiness fields (`API-16`), the evacuation cap (`ALC-20`), supersession
(`OPS-30`), and both persistence fixes (`DEF-10`, `DEF-11`); upgrading it past `b5f46de` burns
its rollback on the first policy write.

**HST-25** What that deployment has demonstrated, from its own ledger: deploy and restart
survival; cross-federation move; an external Lightning send and receive against a counterparty
on a different Lightning node with fees reconciling exactly; ~10,700 ledger rows; and one
standing silent condition, a standby shortfall below the route floor for the whole period
(`F1`). It has never had an evacuation, a stranded move, or a structural refusal.
