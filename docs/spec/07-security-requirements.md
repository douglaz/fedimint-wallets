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
   own (`SEC-17`), can serve a false shutdown notice through the overridable meta field (the
   wallet requires corroboration, `FMI-26`), cannot forge the authenticated config.
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
and authenticates with one bearer token readable from a `0600` file in the data directory.
Anything that can read that file, or the data directory, can do everything — the code says so in
`server.rs`.

**SEC-2** The token is 32 random bytes, hex, written atomically with mode `0600`, compared in
constant time, never logged, and rotated only by re-running `init` while the daemon is stopped
(`API-3`). It never expires.

**SEC-3** The bind address is configuration, not an invariant: `address` in `walletd.toml`
accepts `0.0.0.0`. There is no TLS, no rate limiting, no CORS handling (`API-1`). An operator who
binds beyond loopback has extended the trust boundary to the network with a static bearer token.

**SEC-4** `/v1/health` requires the token and returns 200 whenever authenticated, so an
unauthenticated uptime probe cannot use it, and an authenticated one that trusts the status code
learns nothing (`API-16`).

**SEC-5** `walletd.toml` holds no secrets, only paths. The CLI's pointer file holds the token's
**path**, not the token. `wallet-web.toml` holds an Argon2id password hash and is `0600` in a
directory that `init` creates `0700` and that startup requires to be owned by the running uid and
not group- or other-writable (or sticky); a `0755` directory passes (`HST-26`).

**SEC-6** Nothing in the daemon, server or handler code logs the token, the seed, a full invoice,
or a password. The one deliberate exception is `walletd mnemonic`, which prints the seed to stdout
while the daemon is stopped.

## The seed

**SEC-10** The seed is **plaintext**: twelve BIP-39 words stored as entropy in the SDK's
client-secret slot in `client.db`, protected only by the data directory's `0700` mode. Anyone
who can read the directory owns every satoshi in every federation. `ADR-0026` accepted a
passphrase-derived key and deferred the build; the implementation has not started (`F11`). The
runbook's balance ceiling and host full-disk encryption are the only controls, and both are
outside the code.

**SEC-11** A daemon started on a store with no seed **mints one**. The documented order is
`init → restore-mnemonic → serve`; reversing it produces a wallet on a fresh seed whose
recovery target is the old one (`HST-5`). `restore-mnemonic` refuses to overwrite an existing
seed and reads only from stdin.

**SEC-12** Loss of `client.db` loses the seed and the send-dedup state. Loss of `journal.db`
loses the federation list and the ledger. Seed recovery rebuilds balances, not dedup, not
history (`FMI-32`, `STO-28`). There is no application-level backup of either (`F12`).

## Money-path controls

**SEC-7** Every fee cap that binds is the wallet's own (`OPS-29`). The SDK's fee envelopes do
not bind at the pin (`FMI-19`). A move is refused before minting if the receive leg alone exceeds
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
trusted as the user's decision.

**SEC-17** The vetted gateway list is a union of what each responding guardian returned, so one
Byzantine or misconfigured guardian can place a gateway in the automated candidate set, and
source-side membership is not re-tested at route time (`FMI-10`, `FMI-13`). The threshold check
and the bounded per-peer ingestion `ADR-0030` names as residuals are unbuilt (`F6`).

## Build and environment

**SEC-18** The crash killpoints and the forced-shutdown seam are compiled under
`debug_assertions` and read from the environment. A **release** build ignores
`WALLET_CLI_CRASH_AT` and `WALLET_CLI_FORCE_SHUTDOWN`; a debug binary does not (`ALC-50`). The
smokes run debug binaries for exactly this reason; nothing deployed should.

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
loopback bind, Argon2id at pinned minimum parameters, a `daemon_url` that must be a loopback IP
literal (so a typo cannot ship the daemon's bearer token to a remote host), fail-closed on any
config defect (`HST-26`). It serves no routes, so `ADR-0028`'s session, CSRF and header
requirements are unexercised. It MUST NOT be documented for exposure beyond loopback or a trusted
overlay until `SEC-10` is closed; `ADR-0028` makes that the condition.
