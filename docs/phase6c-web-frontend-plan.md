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

The bind address is **hard-coded to `127.0.0.1`** — only the port is configurable (default `9737`).
A configurable bind address would let `0.0.0.0` satisfy this spec while violating the fixed posture,
so the option does not exist. Startup must reject any attempt to bind elsewhere.

The **daemon URL's host must be an IP LITERAL** in `127.0.0.0/8` or `::1`, over `http://`; the
server fails to start otherwise. DNS names are rejected outright — including `localhost`, which
resolves but is not a literal and can be repointed. Otherwise a typo sends the full-access bearer token in plaintext to a remote host.

**A dedicated origin is required.** Co-hosting the wallet under a path on an origin shared with
another application is UNSUPPORTED and must be documented as such: `HttpOnly`, `SameSite`, and
Origin checks do not isolate two applications on one origin — the neighbour can fetch authenticated
wallet pages, read the CSRF field out of the HTML, and submit actions. Mount at the root and use a
host-only cookie (no `Domain` attribute).

Remote access is the operator's job via a private overlay or their own reverse proxy — the wallet
ships no public listener and no certificate handling (ADR-0028).

**The bind address is NOT an authentication boundary.** Behind a reverse proxy every request
arrives from `127.0.0.1`. Nothing in the code may treat a loopback peer address as authenticated,
and no route may be exempted from auth on that basis. This is the single most likely way to
introduce a hole here.

`X-Forwarded-For` is **not** trusted for anything (no IP allowlisting, no rate-limit keying by
client IP — rate limiting is global per 6c.2).

## 6c.2 Authentication + sessions

Per ADR-0028:

- **Every route requires a session** except `GET /login`, `POST /login`, and `GET /healthz`
  (6c.6). There is **no static-asset exemption**: CSS/JS for the login page is inlined into that
  page, and every other asset route requires a session.
- **Password** verified with **Argon2id** (`argon2` crate), compared via the verifier's own
  constant-time check. **Parameters are pinned in code** (m=19456 KiB, t=2, p=1 — the OWASP
  baseline) and recorded in the PHC string. Startup **fails** on a PHC string that is malformed, or
  whose algorithm/parameters fall below those minimums. `init` enforces a **12-character minimum**,
  rejects a >1024-byte input before hashing (an unbounded password is a CPU DoS), and reads with no
  echo.
- **Session cookie**: `HttpOnly`, `SameSite=Strict`, `Path=/`, **host-only** (no `Domain`).
  `Secure` is set **iff the configured `public_origin`'s scheme is `https`** — NEVER derived from
  the incoming request and never from `X-Forwarded-Proto`. The sidecar terminates no TLS, so a
  request-derived condition can never fire, and the cookie behind the operator's HTTPS proxy would
  be permanently non-`Secure` and stealable via one induced `http://` request to the same host.
- Cookie value is a 256-bit CSPRNG token. Server-side map `token -> {created_at, last_seen,
  csrf_secret}`. Not a JWT; no signing key at rest.
- **A separate 256-bit CSRF secret per session**, distinct from the cookie value. The session
  cookie must never be echoed into HTML.
- **Both values are regenerated on every successful login**, regardless of any cookie presented
  (session fixation).
- **Lifetime**: sliding **4h idle**, absolute **24h** cap, enforced server-side on every request.
  These are the security contract, so configuration may only ever **tighten** them: values above the
  4h/24h ceilings, or unparseable values, fail startup closed rather than silently widening the
  window that ADR-0028 relies on as its sole mitigation.
- **Polling and `/healthz` are PASSIVE**: they validate the session but must NOT update
  `last_seen`. Otherwise 3-second polling (6c.4) refreshes the idle timer forever and an unattended
  visible tab stays fully authorized for the full 24h — defeating the idle timeout entirely.
- **No step-up.** Sending does not re-prompt. Deliberate; see ADR-0028.
- **Login rate limiting**: global (per-IP is meaningless behind a proxy). The counter is checked
  and incremented **atomically before any hashing**, so concurrent attempts cannot slip past the
  five-failure boundary. Argon2 runs on a **bounded blocking pool** and no lock is held across
  hashing or backoff, so login attempts cannot starve authenticated requests. Exponential backoff
  after 5 consecutive failures, capped at 30s, reset on success.
- **Failed logins and rate-limit engagement are logged at `warn`**, carrying no password material.
  The password is the only control in front of `/v1/recover`, so an operator must be able to see a
  brute-force attempt; silent absorption by the limiter means an attack leaves no trace at all.
- **Logout is `POST`** with CSRF, and clears the server-side entry.
- Restarting the sidecar clears all sessions — the documented "revoke everything".

### CSRF

`SameSite=Strict` is the primary defence; it is not the only one.

- Every mutating request (`POST`/`PUT`) carries the session's **CSRF secret** in a hidden form
  field, compared in constant time. `POST /login` is the one exception — no session exists yet —
  and it is still subject to the origin check below.
- Every mutating request is rejected unless `Origin` (or `Referer` when `Origin` is absent) matches
  the configured `public_origin` **exactly** (scheme + host + effective port). A `Referer` is a full
  URL (`https://host/form`), so it must be **parsed and normalized** to its origin triple before
  comparison — never string-compared whole, which would reject every legitimate request. Reject a
  `Referer` carrying credentials, and reject malformed values. A request carrying **neither** header
  is rejected. `public_origin` is explicit configuration, not derived from
  `Host`: a `Host` header is attacker-controlled and is not an origin.

### Response headers (all routes)

`Content-Security-Policy: default-src 'self'; script-src 'self' 'nonce-<per-response>';
style-src 'self' 'nonce-<per-response>'; frame-ancestors 'none'; base-uri 'none'` — a fresh
CSPRNG nonce per response, emitted on the login page's inlined `<style>`/`<script>`. `'self'` alone
does NOT authorize inline elements, so the bare `default-src 'self'` would have silently blocked the
very assets 6c.2 requires to be inlined. ·
`X-Content-Type-Options: nosniff` · `Referrer-Policy: same-origin` ·
`Cache-Control: no-store` on every authenticated page (balances must never land in a shared cache
or the back button after logout).

**`Referrer-Policy` is `same-origin`, deliberately NOT `no-referrer`.** Under `no-referrer` a
non-CORS form POST has its `Origin` serialized as `null` even same-origin (WHATWG Fetch, "append a
request Origin header"), and `Referer` is suppressed outright — so every login and every money POST
from a real browser would arrive with `Origin: null` and no `Referer`, and be refused by the origin
check above. Hand-crafted unit tests and a scripted live gate set their own headers and would never
see it. `same-origin` keeps both signals on same-origin requests and still sends nothing to third
parties.

## 6c.3 The verb surface

**Parity with everything the daemon API exposes** (ADR-0028) — which is *not* the same as full
`wallet-cli` parity, and the spec previously overclaimed. `wallet-cli` refuses these in client mode
because they have no daemon endpoint, so a sidecar cannot offer them either:
`discover`, `probe`, `tick` (agent verbs), `history --fed`, `show` by numeric sequence, and
`status` with policy overrides (`wallet-cli/src/main.rs:576-592`). They stay CLI+`--standalone`
only. Everything the daemon does expose:

| Area | Daemon routes |
|---|---|
| Money | `/v1/pay`, `/v1/receive`, `/v1/move`, `/v1/direct-inflow` |
| Read | `/v1/balance`, `/v1/history`, `/v1/operations/{key}`, `/v1/federations`, `/v1/status`, `/v1/watch/status`, `/v1/health` |
| Federation | `/v1/join`, `/v1/approve`, `/v1/candidates` |
| Admin | `/v1/recover`, `/v1/reconcile`, `/v1/policy` (GET/PUT) |

`/v1/status`, `/v1/watch/status` and `/v1/health` are surfaced by a **diagnostics panel** on the
Admin page (scheduler alive, watch state, daemon health). Naming them under "parity" without a page
was a coverage gap.

**Policy edits are read-modify-write, never reconstruct.** `PUT /v1/policy` takes a whole `Policy`,
and new policy fields ship `#[serde(default)]`, so a form that rebuilds `Policy` from only the
fields it knows would silently reset any field added later — including money caps. The UI must
`GET` the current policy, merge the edited fields into that value, and `PUT` the result.

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
  parameter is unchanged.
- **The filter MUST be applied inside the journal scan, before `take(limit)`**
  (`wallet-fedimint/src/journal.rs` `history()` already filters `before_seq` inside the same
  iterator chain). Filtering in the handler *after* `history(limit, ..)` returns would filter an
  already-truncated page, so a days-old open operation sitting behind `limit` newer terminal rows
  becomes invisible — which is precisely the failure this feature exists to prevent. This matches
  the CLI's documented contract that filters apply before limit.
- Cursor semantics (`before_seq`/`next_before_seq`) are preserved under the filter.
- An unrecognised `status` value is a **422 `refused`** `ApiError`, matching the daemon's existing
  malformed-query contract — not a 400, and never a silent unfiltered listing.
- `history()` currently **skips undecodable ledger rows silently**. For the `status=open` path that
  would let a corrupt open operation vanish while the UI reports "nothing outstanding". The
  response must signal that rows were skipped via an OPTIONAL integer `skipped_rows` field, and the
  UI must render a fail-closed warning rather than an empty list. **The field is emitted ONLY on the
  `status=open` path**, never on an unfiltered request — otherwise a corrupt store would change the
  unfiltered response and break the byte-identical guarantee above, and the two requirements could
  not both hold. **This response-shape addition is part of the single budgeted daemon change**,
  not a second one: it is the same handler, the same endpoint, and equally read-only. Land it in the
  same PR so the budget stays one reviewable diff.
- **UI:** an **Outstanding** section, rendered on every page load from `?status=open`, listing
  **every** non-terminal operation with its age. One request returns at most `limit` rows, so the
  sidecar must **follow `next_before_seq` to exhaustion**, bounded by a page cap. "All outstanding"
  is a lie if it silently means "the newest 50 outstanding" — and equally a lie if the cap is hit
  and the UI still presents the list as complete. **If the cap is reached, the list must be labelled
  incomplete** and say so in the UI, with a test covering that state. This is how a days-old
  held payment stays visible no matter how much history accumulated behind it.
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
invoice + QR) · Activity (paginated history; the only filter is
the operation status the daemon actually supports — outstanding versus all — since `HistoryQuery`
exposes `limit`, `before_seq` and `status` and nothing else, so richer filtering would require a
second daemon change this phase does not budget) · Operation detail · Federations (list, join,
candidates, approve) · Policy (view + edit) · Admin (reconcile, recover, diagnostics) · Settings
(**change password only** — the old password must be
re-entered, and the NEW password is subject to every check `init` applies — confirmation entry,
the 12-character minimum, and rejection above 1024 bytes before hashing; on success the config is
rewritten atomically and re-asserted to `0600`, the in-memory
verifier is replaced only after that write succeeds, the acting session SURVIVES and every other
session is invalidated. The whole sequence — verify old password, write, swap
verifier, invalidate other sessions — runs in ONE critical section: two sessions changing the
password concurrently could otherwise both pass the old-password check and interleave to leave disk
holding one password and memory another, so the process accepts one until restart and a different
one after. Nothing else belongs here: the sidecar's config is a `0600` file, not
UI-editable, and allocator parameters live on the Policy page. Owned by the auth work, since the
password is the credential it manages).

**Operation detail renders only what `OperationView` carries** — `kind`, `status`, `amount`, fees,
`actor`, `reason`, `error`, and `refusal` diagnostics. It does **not** show a `Stranded` move's
preimage or leg op-ids: the wire DTO carries neither, and the rich move record is `--standalone`
only. A stranded operation therefore renders its `error` string verbatim and links the runbook's
stranded-move incident entry. Note what that entry actually says: no shipped command can display
the preimage, so the runbook stops the daemon to PRESERVE EVIDENCE rather than to run a recovery
procedure. Do not promise the user a recovery flow that does not exist. Adding those fields would need a second daemon change, which
this phase does not budget.

### Idempotency and double-submit (money-critical)

`ReceiveRequest.nonce` and `MoveRequest.occurrence` are **client-supplied idempotency inputs**. A
refresh or resubmit that regenerates them is admitted by the daemon as a *second* money operation.
Disabling a button in JavaScript is not a defence — the spec requires the UI to work without JS.

- Every money form **generates its idempotency value at render time** into a hidden field, so a
  resubmit of the same rendered form carries the same key and dedupes at the daemon.
- **The idempotency input alone is not sufficient — snapshot every policy-derived field the key or
  the action depends on.** When a form leaves the federation or fee cap unset, the daemon resolves
  them from *current policy* on each POST (`handlers.rs`: `let fee_cap =
  request.fee_cap.unwrap_or(policy.max_fee)`), and for a move that resolved cap is an **input to
  the operation key**. A policy edit between the first POST and a retry therefore produces a
  different key from the same `occurrence`, and the move is admitted as a SECOND operation; for
  receive it can silently target a different federation or fail as a sizing conflict. So each
  rendered form must resolve and submit **explicit** values for the federation and fee cap (and any
  future policy-derived field affecting the key), not rely on daemon-side defaults.
- **Pay and Move use Post/Redirect/Get**, so a refresh re-issues a GET, never the POST.
- **Receive and DirectInflow do NOT redirect** — they render their result directly from the POST
  response. Their payable invoice exists *only* in that response body (`ReceiveAccepted`), and
  `OperationView` cannot reconstruct it, so a 303 would discard the one artifact the user needs (a
  browser does not render the body of a redirect). Rendering in place is safe precisely because the
  idempotency key is fixed at render time: a refresh re-POSTs the same key, and the daemon
  re-yields the *same* invoice rather than minting a second one. Do not "fix" this by storing
  transient result state in the sidecar — the same key returning the same invoice is the mechanism.
- Those two flows render a **result page** carrying the invoice, its server-rendered QR, the
  operation key, and a link to the detail page.

### Money form contracts

Each money form states explicitly: which federation (a selector where the DTO accepts one, defaulted
to the policy's spending pin), which fields are optional, and the units. **Amounts are entered as
exact decimal sats with at most three fractional digits and converted to integer msat without
floating point.** Never parse an amount into `f64`.

**QR codes** are rendered **server-side** as inline SVG (`qrcode` crate). No JS QR library, no
external service.

**Copy canon** (carried from the 6b notes in `roadmap-to-v1.md`): never use "risk engine", "safe",
"bank", "mint" (as a verb for the wallet), "curated", or "anonymous". Amounts display in sats with
msat precision where non-zero. Evacuation and agent actions are described in money-centric,
past-tense language with reasons.

## 6c.6 Config, provisioning, lifecycle

- **Config file** (`wallet-web.toml`), written `0600` with the mode re-asserted after write,
  mirroring `wallet-daemon/src/config.rs`. Holds: **port only** (the bind address is hard-coded to
  loopback per 6c.1 and is deliberately NOT configurable), daemon URL, token path, Argon2id
  password hash (PHC string), session timeouts, public origin.
- **`wallet-web init`** provisions the password (prompt, confirm, hash, write). **The server refuses
  to start without a password hash** — fail closed, no default credential, no first-load setup page
  (ADR-0028).
- **Daemon unreachable**: read pages render a clear banner ("the wallet daemon is not responding");
  money and admin forms are disabled with an explanation rather than failing on submit.
- **`GET /healthz` is an explicit, deliberate exception to "every route requires a session"** — a
  supervisor cannot log in. It is therefore restricted to exactly two booleans (sidecar alive,
  daemon reachable) and MUST expose no balance, no operation, no federation, and no version
  detail. It is listed in the 6c.2 exemption list and in the route-enumeration test's allowlist, so
  the carve-out is asserted rather than accidental.
- **Secrets never rendered.** The seed/mnemonic is not exposed by any route, at any time, to any
  session. Seed export stays a CLI-only, daemon-stopped operation (runbook Day 0).
- Structured `tracing` logs; never log the password, the session token, the bearer token, or a full
  invoice.

## 6c.7 Tests + gates

**Unit/integration (`cargo test`, must run in the default suite):**
1. **One final route manifest**, asserted after all routes exist (not only in the auth bead):
   every route either requires a session or is on the explicit allowlist (`GET /login`,
   `POST /login`, `GET /healthz`). A newly added route fails this test by default.
2. Required security headers are present on **every** response — authenticated pages, redirects,
   errors, the login page, and `/healthz` — not only on authenticated pages.
3. **The baseline that everything else assumes: the correct password authenticates and establishes
   a session; a wrong password does not, and returns an indistinguishable failure.** Every other
   auth test here presumes login works; none of them actually assert it.
4. A loopback peer address does **not** authenticate: an unauthenticated request from `127.0.0.1`
   is still rejected.
5. Startup rejects a non-loopback daemon URL, **including `http://localhost:9736` explicitly** —
   a suite that only rejects a routable address passes while a resolve-and-accept implementation ships. (There is no bind-address test: the address is a
   hard-coded constant, not configuration — 6c.1/6c.6.)
6. Session expiry: idle beyond 4h rejected; absolute beyond 24h rejected even under continuous
   use; **and continuous 3s polling still expires at 4h** (polling must not slide `last_seen`).
7. Session and CSRF values are regenerated on login; the hidden CSRF field never equals the cookie.
8. CSRF: mutating request without a valid token rejected; mismatched `Origin` rejected; **neither
   `Origin` nor `Referer` present** rejected; `POST /login` exempt from the token but not the
   origin check.
9. Rate limiting engages after 5 failures and resets on success, **including under concurrent
   attempts** (the boundary must hold), and authenticated requests stay responsive while logins
   are being throttled.
10. `init` writes `0600`; startup fails with no hash, with a malformed PHC string, and with
   Argon2id parameters below the pinned minimums; passwords under 12 chars are rejected;
   oversized input is rejected before hashing.
11. `?status=open` (daemon): returns exactly `Started`+`Awaiting`; omitting `status` is
    byte-identical to today's response; **an open row older than more than one full page of
    terminal rows is still returned** (proves filtering happens before `take(limit)` — a
    handler-side filter must fail this); cursor paging works with the filter on; an unknown status
    value returns 422 `refused`.
12. Outstanding follows `next_before_seq` to exhaustion — with more open operations than one page,
    every one of them is listed.
13. Money idempotency **with JavaScript disabled**: submitting the same rendered receive/move form
    twice produces exactly ONE operation, and the same receive key re-yields the same invoice.
    **The move case must edit `max_fee` between the two submissions and still yield one
    operation** — without that, the test passes even when the form omits the fee cap and lets the
    daemon re-derive it, which is exactly the defect the snapshot requirement exists to prevent
    (a re-derived cap changes `move_key`). Likewise assert Receive keeps its snapshotted federation
    and cap across a policy edit.
14. After `POST /logout`, replaying the pre-logout session cookie on an authenticated route is
    REFUSED. A logout that clears only the browser cookie while leaving the server-side entry
    live passes every other test in this list.
15. Policy round-trip preserves a field the form does not know about (read-modify-write, not
    reconstruct).
16. Receive/DirectInflow result pages carry the invoice and QR from the POST response.

**Live gate (devimint, driven like the existing CLI gates):**
Start `walletd` + `wallet-web` against a two-fed devimint. Log in over HTTP. Receive: create an
invoice through the UI, pay it externally, watch settlement appear. Send: pay an invoice and see it
terminalize. Confirm an in-flight operation appears under **Outstanding**, survives a browser
restart with no cookies or client state, and is still listed with correct status. Kill `walletd`,
confirm the degraded banner and disabled forms; restart and confirm recovery.

**Definition of done:** the above pass; `cargo fmt`/`clippy -D warnings` clean; no JS toolchain; the
daemon diff is limited to the `status` filter and its tests.

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

Steps are named rather than cross-referenced by test number: numeric pointers into §6c.7 have
gone stale twice across revisions, so each step owns the tests its own bead lists.

1. Crate skeleton, config + `init`, fail-closed startup, `0600` handling.
2. Auth: Argon2id, sessions, expiry, rate limiting, CSRF, headers, login/logout, Settings.
3. Daemon `?status=open` filter + its tests, landed as its own PR.
4. Read surface: dashboard, balance, federations, activity, operation detail, Outstanding.
5. Polling JS + degraded-daemon banner and `/healthz`.
6. Money surface: send, receive (server-side QR), move, direct-inflow.
7. Federation + admin + policy surfaces with confirmation pages.
8. The devimint live gate.

### 6c.9a Notes on shape and sequencing

**The build order above is narrative, not a dependency contract.** The only hard orderings are the
ones in the bead graph. In particular the daemon filter (step 3) is deliberately INDEPENDENT of the
auth work: it is read-only daemon work in a different crate, so it can land in parallel and should
not be serialised behind the sidecar.

**The read surface is a real bottleneck, and that is accepted rather than overlooked.** It blocks
the money, admin and polling surfaces, and it is the largest single piece. Splitting an "app shell"
out of it was considered and rejected: the money surface genuinely needs the operation-detail page
to redirect to, and the admin surface genuinely needs the federations list, so the edges are real
dependencies rather than an artefact of bundling. Extracting a shell would manufacture a node to
flatten a graph metric while the true ordering constraint stayed exactly where it is. The
consequence is a narrow frontier: run ONE owner per large bead rather than trying to parallelise
inside them.

**On ADR-0028's "accepts a second credential type without rework":** the testable content of that
statement is a structural one — session creation must take an ALREADY-VERIFIED principal, so the
credential check is a separate step from session establishment. Adding WebAuthn later then touches
the verifier and not the session layer. It is not a behavioural requirement and has no test of its
own beyond that separation being visible in the code.
