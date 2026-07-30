# Phase 6c — `wallet-web`: the browser frontend (sequenced BEFORE 6b/Android)

The third frontend over the same public API (ADR-0023): a sidecar process that renders HTML.
`wallet-cli` in a browser — the everyday surface for a self-hosting user before the Android app
exists. **The security posture is fixed by [ADR-0028](./adr/0028-web-frontend-localhost-sidecar-session-auth.md);
this spec does not re-open it.**

Numbering note: this is 6c but ships *before* 6b (Android). 6b's identifier is already
referenced from `phase6a-plan.md` and the README, so it keeps its number.

## 6c.0 Shape + premises

- **A new binary crate `wallet-web`** in this workspace. It is a **client of `walletd`**, exactly
  like `wallet-cli`: HTTP to `127.0.0.1`, bearer token read from a token path. It NEVER opens
  `client.db` or `journal.db` — the daemon holds those locks exclusively, and two writers on one
  RocksDB deadlock by design (6a.0).
- **P1 — the daemon is money-critical and in production.** This phase is permitted **exactly one**
  daemon change: the `?status=open` filter (6c.4). Everything else is additive in the new crate.
  No actor changes, no new money paths, no SSE.
- **P2 — the browser holds no wallet state.** Every page reconstructs from the daemon. There is no
  client-side cache of balances, operations, or in-flight work. A closed tab loses nothing because
  the journal is the truth.
- **P3 — no network dependencies at runtime.** No CDN, no external fonts, no telemetry. All assets
  are compiled into the binary. This is a wallet; a third-party asset request is both a privacy
  leak and a supply-chain surface.
- **P4 — the engine is unchanged.** `wallet-web` contains no money logic, no policy logic, and no
  retry logic. It renders what the API says and submits what the user asks. Any temptation to
  compute a balance, re-sum fees, or decide a route in the frontend is a spec violation.

## 6c.1 Exposure

Binds `127.0.0.1` on its own port (default `9737`, configurable). Remote access is the operator's
job via a private overlay or their own reverse proxy — the wallet ships no public listener and no
certificate handling (ADR-0028).

**The bind address is NOT an authentication boundary.** Behind a reverse proxy every request
arrives from `127.0.0.1`. Nothing in the code may treat a loopback peer address as authenticated,
and no route may be exempted from auth on that basis. This is the single most likely way to
introduce a hole here.

`X-Forwarded-For` is **not** trusted for anything (no IP allowlisting, no rate-limit keying by
client IP — rate limiting is global per 6c.2).

## 6c.2 Authentication + sessions

Per ADR-0028:

- **Every route requires a session** except `GET /login`, `POST /login`, and the static asset
  route. There is no unauthenticated read of balance, history, or status.
- **Password** verified with **Argon2id** (`argon2` crate, default params). Compared via the
  verifier's own constant-time check.
- **Session cookie**: `HttpOnly`, `SameSite=Strict`, `Secure` set when the request is HTTPS,
  `Path=/`. Value is a 256-bit random token from a CSPRNG, stored in an in-memory map
  (`token -> {created_at, last_seen}`). Not a JWT; no signing key needed at rest.
- **Lifetime**: sliding **4h idle**, absolute cap **24h**. Both configurable. Expiry is enforced
  server-side on every request, never by cookie `Max-Age` alone.
- **No step-up.** Sending does not re-prompt. This is deliberate; see ADR-0028's consequences.
- **Login rate limiting**: global (not per-IP, which a proxy makes meaningless) — exponential
  backoff after 5 consecutive failures, capped at 30s, reset on success. Failures are logged at
  `warn` with no password material.
- **Logout** clears the server-side entry, not just the cookie.
- **Restarting the sidecar clears all sessions.** That is the documented "revoke everything".

### CSRF

`SameSite=Strict` is the primary defence; it is not the only one.

- Every mutating request (`POST`/`PUT`) carries a **per-session CSRF token** in a hidden form
  field, compared in constant time against the session's token.
- Every mutating request is rejected unless `Origin` (or `Referer` when `Origin` is absent)
  matches the configured public origin, which defaults to the `Host` header.

### Response headers (all routes)

`Content-Security-Policy: default-src 'self'; frame-ancestors 'none'; base-uri 'none'` ·
`X-Content-Type-Options: nosniff` · `Referrer-Policy: no-referrer` ·
`Cache-Control: no-store` on every authenticated page (balances must never land in a shared cache
or the back button after logout).

## 6c.3 The verb surface

**Full `wallet-cli` parity** (ADR-0028), covering every route in `wallet-daemon/src/server.rs`:

| Area | Daemon routes |
|---|---|
| Money | `/v1/pay`, `/v1/receive`, `/v1/move`, `/v1/direct-inflow` |
| Read | `/v1/balance`, `/v1/history`, `/v1/operations/{key}`, `/v1/federations`, `/v1/status`, `/v1/watch/status`, `/v1/health` |
| Federation | `/v1/join`, `/v1/approve`, `/v1/candidates` |
| Admin | `/v1/recover`, `/v1/reconcile`, `/v1/policy` (GET/PUT) |

Destructive/irreversible actions (`recover`, `approve`, `join`, policy edits) require an explicit
**confirmation step** on a separate page that names what will happen in plain language. This is a
UI affordance, not an auth gate — ADR-0028 chose no step-up, and this does not smuggle one back in.

**Operations initiated here are `Actor::User`.** No `Actor` change (ADR-0028).

## 6c.4 Long-running operations — the one daemon change

Lightning payments can hold for **hours or days** (ADR-0024). A browser tab cannot track that:
background timers are throttled and mobile pages are suspended. So:

- **Daemon change (the only one):** `HistoryQuery` gains an optional `status` filter. `?status=open`
  returns operations whose status is `Started` or `Awaiting`. It is a **read-only journal query** —
  it must not touch the actor, the executor, or any money path. Existing behaviour with no `status`
  parameter is unchanged. Cursor semantics (`before_seq`/`next_before_seq`) are preserved.
- **UI:** an **Outstanding** section, rendered on every page load from `?status=open`, listing all
  non-terminal operations with age. This is how a days-old held payment stays visible no matter how
  much history accumulated behind it.
- **Polling:** only for operations currently on screen — `GET /v1/operations/{key}` every 3s while
  visible, stopping on terminal status or when the page is hidden (`visibilitychange`). Polling is a
  live-view convenience, never the tracking mechanism.
- **No notifications in v1** (ADR-0028).

## 6c.5 Rendering + pages

- **Server-rendered HTML** via `askama` (compile-time templates, pure Rust, no build toolchain, no
  runtime template loading).
- **~50 lines of vanilla JS**, compiled into the binary, for exactly two jobs: polling on-screen
  operations, and disabling a submit button after click to prevent double-submit. **No framework, no
  htmx, no bundler, no npm.** If a feature seems to need more JS, it is out of scope.
- The UI degrades honestly without JS: forms work, pages just do not live-update.

**Page inventory:** Login · Dashboard (unified balance, per-federation breakdown, Outstanding,
recent activity) · Send (invoice paste, decoded preview, fee cap, confirm) · Receive (amount →
invoice + QR) · Activity (paginated history, filters) · Operation detail (`/v1/operations/{key}`,
including the preimage and op-ids a `Stranded` move needs) · Federations (list, join, candidates,
approve) · Policy (view + edit) · Admin (reconcile, recover) · Settings.

**QR codes** are rendered **server-side** as inline SVG (`qrcode` crate). No JS QR library, no
external service.

**Copy canon** (carried from the 6b notes in `roadmap-to-v1.md`): never use "risk engine", "safe",
"bank", "mint" (as a verb for the wallet), "curated", or "anonymous". Amounts display in sats with
msat precision where non-zero. Evacuation and agent actions are described in money-centric,
past-tense language with reasons.

## 6c.6 Config, provisioning, lifecycle

- **Config file** (`wallet-web.toml`), written `0600` with the mode re-asserted after write,
  mirroring `wallet-daemon/src/config.rs`. Holds: bind address/port, daemon URL, token path,
  Argon2id password hash (PHC string), session timeouts, public origin.
- **`wallet-web init`** provisions the password (prompt, confirm, hash, write). **The server refuses
  to start without a password hash** — fail closed, no default credential, no first-load setup page
  (ADR-0028).
- **Daemon unreachable**: read pages render a clear banner ("the wallet daemon is not responding");
  money and admin forms are disabled with an explanation rather than failing on submit. `/healthz`
  on the sidecar reports its own liveness plus daemon reachability, so a supervisor can distinguish
  "sidecar down" from "daemon down".
- **Secrets never rendered.** The seed/mnemonic is not exposed by any route, at any time, to any
  session. Seed export stays a CLI-only, daemon-stopped operation (runbook Day 0).
- Structured `tracing` logs; never log the password, the session token, the bearer token, or a full
  invoice.

## 6c.7 Tests + gates

**Unit/integration (`cargo test`, must run in the default suite):**
1. Every non-login route returns 401/redirect without a session — enumerated over the full route
   table, so a newly added route cannot silently default to public.
2. A loopback peer address does **not** authenticate: a request from `127.0.0.1` with no session is
   still rejected.
3. Session expiry: idle beyond 4h rejected; absolute beyond 24h rejected even with continuous use.
4. CSRF: mutating request without a valid token rejected; with a mismatched `Origin` rejected.
5. Login rate limiting engages after 5 failures and resets on success.
6. `wallet-web init` writes `0600`; the server refuses to start with no hash configured.
7. Argon2id verification accepts the right password and rejects the wrong one.
8. Security headers present on every authenticated response.
9. `?status=open` (daemon-side): returns exactly `Started`+`Awaiting`; omitting `status` is
   byte-identical to today's response; cursor paging still works.

**Live gate (devimint, driven like the existing CLI gates):**
Start `walletd` + `wallet-web` against a two-fed devimint. Log in over HTTP. Receive: create an
invoice, pay it externally, watch the dashboard reflect settlement. Send: pay an invoice and see it
terminalize. Confirm an in-flight operation appears under **Outstanding**, survives a full browser
restart (no client state), and is still listed with correct status. Kill `walletd`, confirm the
degraded banner and disabled forms; restart it and confirm recovery.

**Definition of done:** the above pass; `cargo fmt`/`clippy -D warnings` clean; no new dependency on
a JS toolchain; the daemon diff is limited to the `status` filter and its tests.

## 6c.8 Non-goals + size budget

**Explicitly NOT in this phase** — a reviewer finding that asks for any of these is
over-specification, and should be declined with a reference to this section:

- SSE/WebSockets, notifications of any kind, or service workers
- Multi-user, roles, or per-connection credentials (that is NWC's problem, deferred)
- NWC, Nostr, Lightning Address, LNURL, on-chain
- TLS termination, certificate management, or a public listener
- A JS framework, bundler, npm, or any CDN asset
- Theming, i18n, or accessibility beyond correct semantic HTML and labels
- Any change to the actor, executor, scheduler, journal schema, or `Actor` enum
- Caching or mirroring daemon state in the sidecar
- A step-up/biometric gate (ADR-0028 decided against; do not reintroduce)

**Size budget:** one crate, roughly 2,000-3,000 lines including templates and tests, plus the
~30-line daemon filter. Materially more means scope has crept — stop and re-read 6c.8.

## 6c.9 Build order

1. Crate skeleton, config + `init`, fail-closed startup, `0600` handling. *(tests 6)*
2. Auth: Argon2id, sessions, expiry, rate limiting, CSRF, headers, login/logout. *(tests 1-5,7,8)*
3. Daemon `?status=open` filter + its tests, landed as its own PR. *(test 9)*
4. Read surface: dashboard, balance, federations, activity, operation detail, Outstanding.
5. Polling JS + degraded-daemon banner and `/healthz`.
6. Money surface: send, receive (server-side QR), move, direct-inflow.
7. Federation + admin + policy surfaces with confirmation pages.
8. The devimint live gate.
