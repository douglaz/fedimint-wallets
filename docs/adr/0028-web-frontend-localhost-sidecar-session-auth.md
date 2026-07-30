---
status: accepted
---
# Web frontend: a localhost sidecar, authenticated as a whole, over the full CLI surface

The third frontend (after `wallet-cli` and the planned Android app — ADR-0023) is a **web UI
served by a sidecar process**: a separate binary in this workspace that talks to `walletd` over
`127.0.0.1` with the bearer token, exactly as `wallet-cli` does, and renders HTML instead of a
terminal. It is the everyday surface for a self-hosting user before the Android app exists.

- **Exposure.** The sidecar binds `127.0.0.1`. Reaching it from a phone is the **operator's**
  job, via a private overlay (Tailscale/WireGuard) or their own reverse proxy. The wallet ships
  no public listener, no certificates, and no renewal story.
- **Authentication covers everything.** There is no unauthenticated surface — not even balance.
  Login is a password verified with Argon2id, carried by an `HttpOnly`, `SameSite=Strict`
  session cookie. Passkeys/WebAuthn are the planned upgrade, so the session layer is built to
  accept a second credential type without rework.
- **No step-up before spending.** One login gates the whole UI; sending does not prompt again.
- **Full parity with `wallet-cli`**, including `join`, `approve`, `recover`, `reconcile`, and
  policy edits.
- **Fail closed.** The sidecar refuses to start without a configured password hash. It is
  provisioned by an explicit init subcommand (`0600`, mode re-asserted on write, as
  `walletd` already does for its own secrets). There is no default credential and no
  first-load setup page.
- **State.** The Argon2id hash and a cookie-signing key live in the sidecar's own `0600` config
  file; sessions live **in memory**. The sidecar never opens `client.db` or `journal.db` — the
  daemon holds those locks exclusively by design.
- **Sessions** use a sliding ~4h idle timeout with an absolute cap of ~24h. Login is
  rate-limited and the password compared in constant time, matching the daemon's token check.
- **Long-running operations.** `/v1/history` gains a `?status=open` filter — a read-only journal
  query, the only daemon change this frontend requires. The UI holds **no** in-flight state: it
  reconstructs outstanding operations from the journal on every load and polls
  `/v1/operations/{key}` for what is on screen. There are no notifications in v1.

## Why

- **Localhost + operator-supplied overlay is the honest self-hosted answer.** It gives
  phone-in-hand use without the wallet owning TLS, certificates, or a public listener — the same
  candour as ADR-0002's refusal to pretend about Tor. Note the corollary: because a reverse proxy
  makes every request arrive from `127.0.0.1`, the bind address is **not** an authentication
  boundary. The app-level auth is load-bearing, not defence in depth.
- **ADR-0011 does not port to a browser.** Its instant-view rests on a hardware-backed,
  non-extractable Android Keystore key and a threat model of *physical possession of an unlocked
  device*. A browser has neither. There, "instant view" would publish balance and full payment
  history to anyone who reaches the listener, which is precisely the exposure the `Private`
  glossary entry exists to avoid. ADR-0011 is hereby scoped to the Android app.
- **Full parity keeps this "the CLI in HTML."** Splitting the surface would mean dropping to a
  terminal for federation management, which defeats the point of the frontend.
- **The daemon is money-critical and in production.** It passed a 24h soak and holds real funds,
  so this design deliberately requires exactly one additive, read-only change to it. Server
  push (SSE) was rejected for v1 on that basis; it remains available later.
- **A browser tab cannot track an hours-long Lightning hold.** Background timers are throttled
  and mobile pages are suspended, so polling only ever covers the on-screen case. The journal is
  the durable truth, so the UI is built to reconstruct rather than remember.
- **Fail-closed provisioning** avoids the failure mode that owns self-hosted wallets: a default
  credential, or a first-load setup page that whoever reaches the listener first can claim.

## Consequences

- **A live session is full wallet control.** With no step-up and full parity, an unattended
  logged-in browser can spend the float *and* call `recover` or `approve`. The session timeout
  is the only mitigation, which is why it is idle-based and absolutely capped. This is a
  deliberate trade of blast radius for the absence of friction; revisit it if the wallet ever
  holds more than the pilot ceiling, and treat passkeys as the upgrade that makes revisiting
  cheap.
- **The password protects seed-recovery-level capability**, so its strength is a real security
  parameter, not a formality. Rate limiting is required, not optional.
- **Sessions do not survive a restart.** Restarting the sidecar is therefore a complete
  "revoke all sessions", and the only one available.
- **Exposure correctness is the operator's responsibility.** A misconfigured proxy exposes the
  login page to whatever the proxy is reachable from. The password is what stands there.
- **Deploying this widens what a host compromise costs** while the seed is still plaintext at
  rest (ADR-0026 accepted, not built). It does not change the seed's exposure, but it adds a
  second process that can spend. Public-internet exposure should wait for ADR-0026.
- **One daemon change** (`?status=open`) is owed by this work; everything else is additive in a
  new crate.
- **`Actor` is unchanged.** Web-initiated operations are `Actor::User`, like `wallet-cli`'s —
  this is a frontend the owner drives, not a delegated authority. A third `Actor` variant was
  considered and deferred with NWC, where a revocable third-party delegation would need it.
