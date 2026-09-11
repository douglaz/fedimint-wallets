# 07 — Security requirements

What the code enforces, what it assumes, and what it does not defend. Descriptive: a requirement
here is a control that exists, and a gap is stated as a gap with its finding.

## Threat model

The assets are the seed (which is the funds), the bearer token (which is every operation), and
the ledger (which is the user's whole payment history). Adversaries, in descending order of how
much the design defends against them:

1. **A network adversary between the wallet and a gateway or guardian.** Defended by the
   protocol: blind signatures, hash-locked legs, guardian-verified preimages. The wallet adds
   the never-over check (`OPS-23`) and validates every gateway's `routing_info` at each end.
2. **A misbehaving or malicious gateway.** It sees both legs of every move (`SEC-13`) and can
   quote without performing; the wallet bounds the loss to one operation's amount and terminalizes
   honestly (`FMI-23`). It cannot strand a move alone and cannot open the preimage (`DEF-20`).
3. **A malicious or misconfigured guardian.** Can place a gateway in the vetted list on its
   own (`SEC-17`), and by listing any URL can make the wallet host POST to an address of its
   choosing — loopback, link-local, RFC1918, cloud metadata — because the `routing_info` call
   restricts nothing (`FMI-11`, `F45`); can serve a false shutdown notice through the overridable
   meta field (the wallet requires corroboration, `FMI-26`), cannot forge the authenticated config.
4. **A poisoned discovery feed.** Every candidate's config is re-fetched and structurally scored,
   the Sybil check requires three ids to agree, and nothing is funded before a sats-spending
   probe passes (`ALC-28`, `ALC-37`).
5. **Another process on the same host, or a reader of a backup.** **Not defended.** See
   `SEC-10`, `SEC-12`.

Explicitly not defended against: a compromised operating-system user, a compromised host, an
adversary with the data directory, network-level deanonymization (no Tor, `ADR-0002`), and a
compromised SDK pin.

## The trust boundary

**SEC-1** The trust boundary is the operating-system user. The daemon binds loopback by default
and authenticates with one bearer token that `init` writes as a `0600` file, by default in the
data directory. The file's location follows the precedence in `HST-4` (environment, then config,
then `<data_dir>/token`), and its mode is **not** re-checked when the daemon reads it — a token
file made world-readable after `init` is accepted silently. Anything that can read that file, or
the data directory, can do everything — the code says so in `server.rs`.

**SEC-2** The token is 32 random bytes rendered as lowercase hex, written atomically with mode
`0600` (`HST-19`), never logged, and rotated only by re-running `init` while the daemon is stopped
(`API-3`) — with the caveat that `WALLETD_TOKEN_PATH` is read per process, so an `init` and a
`serve` given different environments rotate and read different files (`HST-4`). The header form
and the 401 envelope are `API-2`. The comparison is constant-time over the **content** after an
early return on a length mismatch: the token's length is observable, its bytes are not. It never
expires.

**SEC-3** The bind address is configuration, not an invariant: `address` in `walletd.toml`
accepts `0.0.0.0`. There is no TLS, so on any non-loopback bind the bearer token crosses the
network in cleartext and a passive observer can replay it against every route — the only safe
deployments are the loopback default or an authenticated tunnel in front of it (the runbook's
posture); the daemon does not enforce either. There is no rate limiting, no CORS handling (`API-1`), and the daemon
installs no request timeout, body limit or connection cap of its own — the JSON extractor's
default body limit and the two handler deadlines (invoice mint, await long-poll) are the only
bounds. An operator who binds beyond loopback has extended the trust boundary to the network with
a static bearer token. Two clients extend it further on their own: `wallet-cli` sends the token to
whatever URL its pointer file or `--url` names, with no loopback or scheme check (the check
`SEC-22` describes exists only in `wallet-web`), and `ops/walletd-watch.py` accepts the token
from the `WALLETD_TOKEN` environment variable and posts the wallet's balance to whatever
`--webhook` URL it is given, any scheme (`HST-22`).

**SEC-4** `/v1/health` requires the token and returns 200 whenever authenticated, so an
unauthenticated uptime probe cannot use it, and an authenticated one that trusts the status code
learns nothing (`API-16`).

**SEC-5** `walletd.toml` holds no secrets, only paths. The CLI's pointer file holds the token's
**path**, not the token. Both are written with the ambient umask, so the token's path and the
daemon URL are ordinarily readable by every local account (`HST-19`); only the token file itself
is `0600`. `wallet-web.toml` holds an Argon2id password hash; `init` writes it `0600`, and every
startup requires that its group and other bits are all clear (`0600`, `0400` and `0700` pass
alike) and that its directory is owned by the running uid and not group- or other-writable
unless sticky — a `0755` directory passes. `wallet-web init` creates that directory `0700` only
when it is absent; an existing directory keeps whatever mode it has and is only checked
(`HST-26`).

**SEC-6** Nothing in the daemon, server or handler code logs the token, the seed, a full invoice,
or a password. The one deliberate exception is `walletd mnemonic`, which prints the seed to stdout
while the daemon is stopped. Three edges sit outside that claim: a storage fault is returned to
the HTTP caller as a 500 whose message is the engine error's debug rendering and can carry
filesystem paths (`API-37`); `wallet-web` logs its `daemon_url`, `public_origin` and `token_path`
at `info` on startup; and `wallet-cli pay <invoice>` takes the BOLT11 on the command line, where a
process listing or shell history can read it, while the mnemonic path deliberately refuses
arguments. `RUST_LOG` is honoured unconditionally by all three binaries, so what the SDK logs is
outside the claim's scope.

## The seed

**SEC-10** The seed is **plaintext**: twelve BIP-39 words stored as entropy in the SDK's
client-secret slot in `client.db`, protected only by the data directory's `0700` mode — no mode
is ever set on `client.db` or `journal.db` themselves. Anyone who can read the directory owns
every satoshi in every federation. The wallet's own crates apply no memory hygiene: the mnemonic,
its entropy, the bearer token and the sidecar password are plain `String`/`Vec<u8>` values that
are dropped without being wiped (the SDK's own `zeroize` use does not extend to them). `ADR-0026`
accepted a passphrase-derived key and deferred the build; the implementation has not started
(`F11`). The runbook's balance ceiling and host full-disk encryption are the only controls, and
both are outside the code.

**SEC-11** A daemon started on a store with no seed **mints one**. The documented order is
`init → restore-mnemonic → serve`; reversing it produces a wallet on a fresh seed whose
recovery target is the old one (`HST-5`). `restore-mnemonic` refuses to overwrite an existing
seed (checked before the words are parsed), reads only from stdin with all whitespace collapsed,
requires a valid BIP-39 checksum and **exactly twelve words**, and writes nothing on any failure.
It re-asserts the `0700` data directory before opening the store, as `init` and `serve` do;
`walletd mnemonic` is the one seed path that does not.

**SEC-12** Loss of `client.db` loses the seed and the send-dedup state. Loss of `journal.db`
loses the federation list and the ledger. Seed recovery rebuilds balances, not dedup, not
history (`FMI-32`, `STO-28`). There is no application-level backup of either (`F12`); what the
operator must hold instead is `SEC-24`.

**SEC-23** One seed, one live `client.db`, one process. The store lock is the file
`<data_dir>/client.db.lock`, so it excludes only a second process opening **that same
directory**: a second `walletd` blocks on it and `wallet-cli --standalone` refuses (`HST-9`). It
does not make the host, let alone the seed, safe: a restored backup, a cloned volume or a copy of
the data directory mounted at any other path — on this host or another — carries its own lock
file and runs concurrently as a **second spender of the same notes**. The federation lets exactly
one of them win each spend, both processes' bookkeeping is then false, and a send re-driven from
the copy misses the original's dedup state (`FMI-32`). Nothing in the code detects or defends
against this; the invariant is the operator's to hold, and the runbook's stranded-move procedure
is built around hunting for its violation. This is a money-loss rule, not a confidentiality one.

**SEC-24** The recovery unit the operator MUST hold outside the wallet is the twelve-word seed
(`walletd mnemonic`, daemon stopped, `SEC-6`) **plus the invite code of every joined
federation** (`wallet-cli list-feds`), re-recorded after every join. Recovery (`FMI-30`) takes
the seed and one invite per federation and nothing else; the seed alone recovers ecash only in
federations whose invites the operator still has. An invite is not secret **unless it carries
the optional `api_secret` part** (`InviteCodePart::ApiSecret`), which the SDK sends as guardian
API authentication: such an invite is a live credential and MUST be stored and shared as one.
`journal.db` is
bookkeeping and is not part of the unit (`ADR-0025`).

## Money-path controls

**SEC-7** Every fee cap that binds **on the amount** is the wallet's own (`OPS-29`). The SDK
also enforces its `SEND_FEE_LIMIT` / `RECEIVE_FEE_LIMIT` at the pin, but lexicographically on
`(base, ppm)` (`FMI-19`): a gateway posting a base above 100 sat (send) or 50 sat (receive) is
refused, and one under that base passes with any ppm — an
admission filter on the gateway's posted fee, not a bound on what a payment costs. That check runs
**after** the wallet's own, not before it, and no wallet crate reads either limit: the executor
quotes the candidates, keeps the cheapest that fits the wallet's cap, and only then calls
`mc.pay` / `mc.receive`, inside which the SDK checks the **already-selected** gateway. A gateway
over the base limit therefore fails the attempt rather than being skipped in favour of the next
candidate — `GatewayFeeExceedsLimit` → `RouteRejected` → `Permanent` for a send, and the receive
refusal → `Retryable` (`FMI-17`, `OPS-17`, `OPS-18`). A move is
refused before minting if the receive leg alone exceeds
the cap, and again before paying if both legs do.

**SEC-8** A committed receive whose contract differs from the quote is refused before the
invoice is surfaced (`OPS-23`); a gateway that lowers its fee between quote and mint cannot
over-credit the wallet, and one that raises it cannot make it pay more than the cap.

**SEC-9** No operation is admitted whose source cannot cover `amount + fee_cap` after
reservations, and none whose destination would exceed the per-federation cap (`OPS-7`), except
that an evacuation has no source check by design and is sized at perform time (`OPS-21`).
There is no aggregate ceiling across federations (`F10`).

**SEC-13** A gateway that carries a move sees both legs and therefore learns the wallet's
cross-federation movement pattern. The design prefers spreading across independent gateways and
federations over routing everything through one the operator runs (`docs/roadmap-to-v1.md`
"Non-goals"); the code does nothing to enforce either.

**SEC-14** Shutdown signals are trusted only when corroborated: the merged meta expiry (which a
federation's override host can serve) never triggers an evacuation alone; the at-join consensus
meta or `f+1` peers' `/status` do (`FMI-26`).

**SEC-15** The Fedimint Observer is a discovery source and nothing else; no field it returns
reaches a scoring or funding decision without a re-fetched, authenticated config (`FMI-28`,
`ADR-0020`).

**SEC-16** A federation the agent joined is fundable only after a sustained window of real
round-trip probes, and a pin does not bypass that gate (`ALC-37`). A user's own `join` is
trusted as the user's decision — **except** over a federation the agent already auto-joined, whose
candidate row stays agent-owned and probe-gated until the audited `approve` verb releases it
(`OPS-42`; seed recovery cannot, since it refuses a registered federation, `FMI-31`).

**SEC-17** The vetted gateway list is a union of what each responding guardian returned, so one
Byzantine or misconfigured guardian can place a gateway in the automated candidate set, and
source-side membership is not re-tested at route time (`FMI-10`, `FMI-13`). The list's order is
a per-call random shuffle stable-sorted by how many guardians returned each URL (`FMI-10`), so
"the first that validates" is non-deterministic among equally-vetted gateways and two calls can
route the same move differently. The threshold check
`ADR-0029` and `ADR-0030` decide is unbuilt (`F6`). A bound on what each guardian's response may
contribute is tracked by `br-gw-threshold-membership-k4t` alone; no ADR records it.

## Build and environment

**SEC-18** The crash killpoints and the forced-shutdown seam are compiled under
`debug_assertions` and read from the environment. A **release** build ignores
`WALLET_CLI_CRASH_AT` and `WALLET_CLI_FORCE_SHUTDOWN`; a debug binary does not (`ALC-50`). The
smokes run debug binaries for exactly this reason; nothing deployed should. Those are the only
gated seams. A release build still reads `WALLETD_PERFORM_TIMEOUT_SECS` and
`WALLETD_SETTLEMENT_STALL_SECS`, both of which change money-path timing, and a value that does
not parse falls back to the default silently rather than failing startup (`HST-2`).

**SEC-19** No Tor and no network-level anonymity (`ADR-0002`). Receiving is private (the
gateway cannot tie funds to an identity); sending leaks the destination to the gateway.

**SEC-20** Deployment identity — hosting provider, cluster, namespace, pod, image digest,
uptime, balance — MUST NOT appear in tracked files (`DEF-22`). The runbook holds the location;
everything else points at the runbook. The historical leak is `F21`.

**SEC-21** The dependency is a personal fork at a fixed revision (`FMI-1`). Its provenance is
the operator's own; there is no reproducible-build attestation and no signature check on the
image (`docs/roadmap-to-v1.md` Phase 8).

## The browser sidecar

**SEC-22** `wallet-web` as built enforces its posture before it serves anything: hardcoded
loopback bind, an Argon2id PHC hash validated at pinned minimum parameters, a `daemon_url` that
must be a loopback IP literal (so a typo cannot ship the daemon's bearer token to a remote host),
the config-file and directory checks of `SEC-5` at every start, session idle and absolute
ceilings of 4 h and 24 h that configuration may only tighten, a `public_origin` canonicalized to
the form a browser is *expected* to send (`HST-27`; that equivalence is unmeasured, and it
becomes a security boundary the day a route compares `Origin`), and fail-closed on any config
defect (`HST-26`). It serves no
routes, so the request-time half of `ADR-0028` — sessions, CSRF, security headers, rate
limiting — does not exist; only the configuration those will consume does. It validates its
`token_path` but never reads or mode-checks the daemon's token. It MUST NOT be documented for
exposure beyond loopback or a trusted overlay until `SEC-10` is closed; `ADR-0028` makes that the
condition.
