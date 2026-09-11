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
overrides. This rule owns the complete **release** environment-variable surface of the three
binaries; none of it is gated at release, and every **wallet-owned** numeric knob falls back
**silently** on a bad value (the runtime's own variables, listed last, do not). Two further variables exist only in debug builds and are compiled out of release:
the fault-injection seams `WALLET_CLI_CRASH_AT` (`OPS-28`) and `WALLET_CLI_FORCE_SHUTDOWN`
(`FMI-26`), owned by `SEC-18`:

| Variable | Read by | Effect | Empty / invalid |
|---|---|---|---|
| `WALLETD_TOKEN_PATH` | `walletd` (all subcommands) | token file, over `walletd.toml` (`HST-4`) | empty = unset; relative path fails startup |
| `WALLETD_PERFORM_TIMEOUT_SECS` | `walletd` serve (`perform_timeout_from_env`) | the per-`perform` deadline `FMI-22` owns | blank or non-numeric → the `FMI-22` default silently |
| `WALLETD_SETTLEMENT_STALL_SECS` | the scheduler's watchdog (`ALC-40`; `walletd` only, standalone runs no scheduler) | deadline, default 300 s; `0` is accepted as a zero-second deadline | non-numeric → 300 silently |
| `RUST_LOG` | `walletd`, `wallet-cli`, `wallet-web` (`EnvFilter::try_from_default_env`) | overrides the level | unset **or unparseable** → `walletd.toml` `log_level` / `warn` / `info` respectively, silently |
| `XDG_CONFIG_HOME` | all three | config home | empty or relative → ignored, `~/.config` |
| `XDG_DATA_HOME` | `walletd`, standalone `wallet-cli` | default `data_dir` | empty or relative → ignored, `~/.local/share` |
| `HOME` | all three | `~` expansion and the XDG fallbacks | read only when a path actually falls back to it: unset or empty → startup error (`HOME is not set; …`) then, and never with absolute `XDG_CONFIG_HOME` + `XDG_DATA_HOME`, an explicit `--config` whose paths are absolute, or standalone `--data-dir` — except `walletd init`, which writes the client pointer under the config home (`write_client_pointer` → `config_home`) *after* seeding the policy and rotating the token, so without an absolute `XDG_CONFIG_HOME` it needs `HOME` whatever `--config` says and fails with state partly mutated; client-mode `wallet-cli` needs it only when `--url` and `--token-path` are not both given (`API-25`) |
| `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` (and lowercase), `NO_PROXY` | `walletd`, `wallet-cli` (`wallet-web` has no outgoing HTTP client) | every `reqwest` client — the CLI's daemon client (`WalletdClient::resolve`), the gateway `routing_info` client (`MultiClient::new`), the Observer client (`ObserverSource::new`) | honoured by `reqwest` 0.12.28's default system-proxy support; no builder calls `no_proxy()`. **Measured 2026-09-11** (`target/debug/wallet-cli` at `4a757de`, `NO_PROXY` unset, `HTTP_PROXY=http://127.0.0.1:18571` pointing at a capturing TCP listener): `wallet-cli --url http://127.0.0.1:9736 --token-path <f> health` arrived at the listener as `GET http://127.0.0.1:9736/v1/health HTTP/1.1` with `authorization: Bearer <token>` in cleartext — a loopback destination is **not** exempt — and the CLI exited 4 on the listener's 502 (`SEC-3`) |
| `TOKIO_WORKER_THREADS` | all three (`#[tokio::main]`, multi-thread runtime) | worker-thread count of the pinned Tokio 1.52.3 runtime | read by Tokio, not the wallet: `0` **panics at startup** rather than falling back. **Measured 2026-09-11** on the nix-built `result/bin/walletd`: `TOKIO_WORKER_THREADS=0 walletd --help` → exit **101**, stdout 0 bytes, stderr `thread 'main' … panicked … "TOKIO_WORKER_THREADS" cannot be set to 0`; the same command with the variable unset → exit 0. A non-numeric value is unmeasured |

The readiness poller's own variables are `HST-22`.

**HST-3** `walletd.toml` has five keys and rejects any other — including the retired `gateway`
pin, so a file from the pinned era fails startup loudly (`ADR-0030`):

| key | default |
|---|---|
| `data_dir` | `$XDG_DATA_HOME/walletd`, else `~/.local/share/walletd` |
| `address` | `127.0.0.1` |
| `port` | `9736` |
| `token_path` | env `WALLETD_TOKEN_PATH`, else the key, else `<data_dir>/token` |
| `log_level` | `info` (`RUST_LOG` overrides) |

Paths must be absolute (`~` and `~/…` expanded via `resolve_path`; anything else non-absolute
fails). `address` is not validated: a bare IPv6 literal is bracketed when the bind string and
the client-pointer URL are built (`authority_host`), a hostname passes through and is resolved
by the bind. Environment knobs are the table in `HST-2`.

**HST-4** `walletd init`: read-or-default the config and write every key back canonicalized;
create the data directory `0700`; open `client.db` (blocking on the lock if the daemon is
running) and `journal.db`; seed `Policy::default()` if absent (`STO-13`); mint and write the
token `0600`; write `~/.config/walletd/client.toml` `{url, token_path}` for the CLI. It does
**not** mint a seed. It prints six stdout lines: `initialized walletd`, then `  host config:`,
`  data dir:`, `  token (0600):`, `  client pointer:`, `  api url:` each followed by the
resolved value. Token path precedence, for every subcommand, is `WALLETD_TOKEN_PATH` (when set
and non-empty) > `walletd.toml` `token_path` > `<data_dir>/token`. On serve the token is read
with `read_token`: the file's mode is **never checked** — a world-readable token file is
accepted silently — the contents are whitespace-trimmed, and an empty file fails startup
(`bearer token file <path> is empty`).

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

**HST-9** `wallet-cli --standalone` is a one-shot process that takes the exclusive lock, opens
both stores, and drives `Runtime` directly. Only its `tick` verb is the documented admission
exception in `ADR-0031` (`OPS-12`); the standalone money verbs run the actor, and `probe`'s
bypass is the undocumented second exception `F42` tracks. It resolves `data_dir` from `--data-dir`, else `walletd.toml` (parsed with the
daemon's own closed schema, so a stale `gateway` key fails here too), else the default; then
asserts the directory `0700` (`HST-19`). The lock mechanism (`check_db_lock`): open-or-create
`<data_dir>/client.db.lock` (truncating), attempt a non-blocking exclusive `flock`
(`fs_lock::FileLock::new_try_exclusive`); contention is exit 1 `another process owns the
wallet store (walletd?); stop it, or use client mode (drop --standalone)`; on success the probe
lock is released and RocksDB's own `open()` of `client.db`, then `journal.db`, is each
wrapped in a **10 s** `tokio::time::timeout` whose expiry is reported as `opening the wallet
store timed out waiting for its lock — another process took it after the pre-check (walletd
restarting?) …` (exit 1). Whether that bound is *observable* is unmeasured: the pinned
`RocksDb::open` runs the blocking open inside `block_in_place`, which a `tokio` timeout cannot
pre-empt, so a daemon that takes the lock in the gap may hold the standalone process until it
releases the lock rather than producing that error. `--perform-timeout <secs>`
(default 600, `0` disables) bounds each executor `perform` exactly as `WALLETD_PERFORM_TIMEOUT_SECS`
does for the daemon (`HST-2`); the env variable is not read by the CLI.

**HST-10** The standalone-only verb shapes and flags are the set `API-25` enumerates (client
mode refuses exactly those with exit 1); this rule owns the break-glass. `--gateway` is accepted on `pay`,
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

- `wallet-web init` takes `--port` (default 9737), `--daemon-url` (default
  `http://127.0.0.1:9736`), `--token-path` (required), `--public-origin` (required),
  `--session-idle-timeout` (default `4h`), `--session-absolute-timeout` (default `24h`), and the
  global `--config` (default `$XDG_CONFIG_HOME/wallet-web/wallet-web.toml`, else
  `~/.config/wallet-web/wallet-web.toml`). It **refuses to run if the config file already
  exists** (`sidecar config <path> already exists; …`), checked before the prompt: there is no
  password-rotation path short of deleting the file. It then prompts for a password twice on
  the controlling TTY (`rpassword`, never stdin; Ctrl-C aborts cleanly with nothing written),
  enforces at least 12 **characters** and at most 1,024 **bytes** (bytes checked first), hashes
  with Argon2id v19 (m = 19456 KiB, t = 2, p = 1, fresh 16-byte salt), validates the whole
  config through the same path startup uses, and writes the file `0600` atomically (`HST-19`)
  into a directory it creates `0700` or verifies is owned by the running uid and not group- or
  other-writable unless the sticky bit is set (a `0755` directory passes; `/tmp` does **not** for
  a non-root sidecar, because the owner-equals-euid check runs before the sticky-bit exemption
  and `/tmp` is root-owned).
- The config has exactly `port, daemon_url, token_path, password_hash, session_idle_timeout,
  session_absolute_timeout, public_origin` and every key but `password_hash` is required (no
  serde defaults); there is no bind-address key (the bind is hardcoded
  `127.0.0.1`) and no log-level key (`RUST_LOG`, `HST-2`). Timeouts are the grammar
  `<integer><s|m|h>` (`parse_duration`; `0s` is accepted — immediate expiry is the fail-closed
  direction).
- Startup (`config::load`) refuses, in this order: a config with any group/other permission bit;
  a config directory not owned by the running uid or group/other-writable without the sticky
  bit; a TOML parse error (reported by position and parser message only — the offending line is
  never quoted, so the hash cannot leak into a journal); a missing, empty or malformed PHC hash;
  a hash that is not `argon2id`, does not declare `v=19`, has a salt under 8 bytes, has no hash
  output, or has `m`, `t` or `p` below the pinned minimums; port 0; a `daemon_url` that is not
  `http://` + a loopback IP **literal** + port with at most a bare trailing `/` (`localhost` is
  refused because it resolves but can be repointed; `::1` is the only IPv6 form; the stored value
  is the parsed socket re-rendered, so `[0:0::1]` becomes `[::1]` and the trailing `/` is
  dropped); a `token_path` that does not resolve to an absolute path (`~/` expands as
  `walletd`'s does); timeouts above 4h idle / 24h absolute or unparseable; a `public_origin`
  that `HST-27` refuses.
- It has no HTTP client and no dependency on the wallet crates; `token_path` and
  `password_hash` are dead at runtime. It is not a flake output (it is in the Nix source
  filter, so the workspace build compiles it).

**HST-27** `public_origin` is stored in the canonical form a browser sends in `Origin`:
`validate_public_origin` **normalises** an accepted input by the rules below and refuses only the
listed ambiguous or unsafe forms, so `https://Wallet.EXAMPLE:443` starts and is stored as
`https://wallet.example`. The rules are *intended* to match the WHATWG URL origin algorithm;
that equivalence has not been measured against any browser, so until it is, treat it as a
possibility rather than a guarantee.
Refused: any `#`; a scheme other than `http`/`https`; userinfo; a non-`/` path or a query; an
empty host; a port that is not a `u16` or is `0`; an IPv4-mapped IPv6 host (`[::ffff:…]`,
which Rust and WHATWG serialise differently); a host ending in `.`; a host whose last label
is all digits or `0x`+hex that is not already a canonical dotted quad (`127.1`, `0x7f.0.0.1`,
`2130706433`, `127.0.0.0x1`, `wallet.1`). Canonicalised: the host is lowercased; a bracketed
IPv6 host is re-rendered by Rust's `Ipv6Addr` `Display` (`[0:0::1]` → `[::1]`); a port equal
to the scheme's default (80/443) is **elided**, so `https://wallet.example:443` is stored as
`https://wallet.example`, and a bare origin is accepted as-is. The function is idempotent on
its own output.

Everything else in `docs/phase6c-web-frontend-plan.md` — login, sessions, CSRF, the read and
money surfaces, `/healthz` — is unbuilt (`F27`).

## Operator response to a stranded move

**HST-28** A `Stranded` move (`DOM-10`: the send leg settled and the receive leg terminally
failed; the transition is `OPS-27`) is terminal and nothing re-drives it (`OPS-35`:
`reconcile` re-drives `pending()` only). The operator response is **evidence preservation, not
recovery** — no recovery
procedure exists, and the preimage is not one — and MUST run in this order
(`docs/real-sats-pilot-runbook.md`, "Stranded is TERMINAL"): (1) stop the daemon and preserve
the **whole** data directory before any diagnosis, because it holds the destination's complete
client state for both receive-failure branches; (2) rule out a duplicated claimant: enumerate
every copy of the data directory that has ever existed on this host or any other (restored
backup, volume snapshot, container clone, copy on a second machine), and clear each only if no
wallet process **held it open at any point during the move's window** — compare process
lifetimes against the window dated by `wallet-cli show <key> --standalone`, not open events
inside it, and treat an unknowable history as unresolved (the store lock excludes only two
processes on the *same* path, `SEC-23`); (3) collect what the shipped commands give, which is
`show --standalone`'s error detail and `send_op`/`recv_op`/`gateway`, and nothing more;
(4) never re-submit the move by hand — the executor's dedup on the existing key is the only
thing preventing a second send. Recorded here from the runbook as `F20`'s owner; the runbook
remains the procedure.

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

This rule also owns how files reach disk. **Secret files** — the bearer token
(`config::write_secret_file`) and `wallet-web.toml` (its own `write_secret_file`) — are written
atomically: create a sibling temp (`<target>.tmp` for the token, `<target>.<pid>.tmp` for the
sidecar config; a stale one is removed first) with `create_new` and mode `0600`, re-assert
`0600` with `set_permissions` so the umask cannot widen it, write, `sync_all`, then `rename`
over the target, so a reader sees the old file or the new one and never a truncated one.
**Non-secret files** — `walletd.toml`, `client.toml` — are plain `std::fs::write` under the
ambient umask and are not re-asserted. **Directories**: `walletd` runs
`ensure_private_data_dir` (`create_dir_all` then `set_permissions(0700)`, on every start) at
the start of `serve`, `init` and `restore-mnemonic` but **not** `mnemonic`, so a data directory
whose mode drifted is re-tightened by the next start, not by a read-only export;
`wallet-web init` creates a missing config directory `0700` (re-asserted) and leaves an
existing one's mode alone (`HST-26`). Nothing chmods RocksDB's own files.

**HST-20** There is **no Kubernetes manifest in this repository**, and no deployment
configuration is tracked (`SEC-20`, `DEF-22`). The only tracked unit
(`wallet-daemon/deploy/walletd.service`) has the perform-timeout environment line commented out
at the default 600; what the long-running deployment actually runs with is held elsewhere and
is not recorded here.

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
`/v1/health`, `/v1/balance` and `/v1/status` (the last with three times the timeout, and its
failure is an alert rather than fatal), and exits `0` quiet, `1` on any alert, `2` when
unreachable or when `WALLETD_TOKEN` and `WALLETD_TOKEN_FILE` are both unset or empty. The token
comes from `WALLETD_TOKEN`, else the contents of the file named by `WALLETD_TOKEN_FILE`
(trimmed) — a missing or unreadable file is an uncaught `OSError`: a traceback and exit `1`,
not `2` (**measured 2026-09-11**, Python 3.14.7, `WALLETD_TOKEN` unset,
`WALLETD_TOKEN_FILE=/nonexistent/token`: exit **1**, stdout 0 bytes, stderr ends
`FileNotFoundError: [Errno 2] No such file or directory: '/nonexistent/token'`; control with
both unset: exit **2**, stderr `walletd-watch: set WALLETD_TOKEN or WALLETD_TOKEN_FILE`); the URL from `--url` /
`WALLETD_URL` (default `http://127.0.0.1:9736`); `--state` / `WALLETD_WATCH_STATE`,
`--webhook` / `WALLETD_WATCH_WEBHOOK`, `--timeout` (seconds, default 10). It alerts on `scheduler_alive == false`, on
`automation_ready == false` (with the reason and detail), and on every deferred funding goal
whose floor is the route floor rather than the protocol minimum. An absent `automation_ready`
(an older daemon) is a note, not an alert. `--state` suppresses output only when there is no
alert and nothing changed; `--always-report` prints even then. A **standing alert is printed, exits 1, and is posted to the webhook on
every pass**, so the script's own docstring ("pages on transition") overstates it. The webhook
is an unvalidated `urllib` URL — any scheme `urllib` opens — receiving a JSON `{"text": …}`
POST; a `URLError` / `TimeoutError` delivery failure is a note, not an exit code, but a
malformed or unsupported URL (`not-a-url`) raises `ValueError` uncaught: traceback, exit `1`
(**measured 2026-09-11**, Python 3.14.7, against a loopback stub daemon answering
`/v1/health`, `/v1/balance`, `/v1/status` with healthy JSON, no `--state` so the first pass
counts as changed: `--webhook not-a-url` → exit **1**, stdout the one-line report, stderr a
traceback ending `ValueError: unknown url type: 'not-a-url'`; control `--webhook
http://127.0.0.1:1` → exit **0**, stdout `note  webhook delivery failed: <urlopen error [Errno
111] Connection refused>`, stderr 0 bytes).
**Nothing runs it** (`F14`).

## The long-running deployment

**HST-23** One instance of `walletd` has been running the 2026-07-26 build `b5f46de`,
holding a small real-sats balance across two mainnet federations, with real receive, pay, move
and join history. It is a **test deployment**, not production; the operator has said so, and
this set describes it accordingly. Its location, namespace, image digest and balance are
deliberately not recorded in tracked files (`SEC-20`, `DEF-22`).

**HST-24** `main` (`7225114`) is 240 commits past that build (`git rev-list --count
b5f46de..7225114`). Everything in `09-known-defects.md` from
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
