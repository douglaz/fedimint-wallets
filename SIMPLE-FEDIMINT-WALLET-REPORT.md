# Building a Simple, Risk-Managed Fedimint Wallet

**A design report informed by four existing wallets**
*Target: pure Rust · Android-only · Slint UI · Draft — 2026-06-26*

---

## 0. Thesis

Build a wallet that **looks like Wallet of Satoshi** (one balance, send, receive, near-zero concepts on screen) but is **non-custodial-of-recovery and risk-managed underneath**: the user's funds are automatically spread across *multiple* Fedimint federations, allocated and de-risked by an engine that scores federations on public data + empirical probes. The complexity lives in Rust; the screen stays empty.

> **One line:** *WoS-simple surface, automated multi-federation risk engine underneath.*

The novel part is the engine — **none of the four wallets surveyed actually automate cross-federation allocation.** Harbor advertised it and never built it; ecash-app, vipr, and Fedi are all manual multimint (the user holds whatever is in whatever federation they joined). That gap is the product opportunity.

---

## 0.5 CEO Review Outcome (2026-06-26, HOLD SCOPE)

A CEO-mode review with an independent second-model challenge **re-sequenced the build**. v1 ships a single-federation, WoS-simple wallet that is *architected for* the engine; the multi-federation engine turns on in v2 with no rewrite.

| # | Decision | Outcome |
|---|----------|---------|
| D1/D7 | Build sequencing | **Foundation-first, architected for the engine.** Single-fed v1; allocator built as a pure function at N=1 + decision journal + storage seam; multi-fed in v2 |
| D2 | Review posture | HOLD SCOPE (harden, don't expand) |
| D3 | Fragmentation | Designated **spending federation** kept topped up (not multi-path sends) |
| D4 | Evacuation | Escalating ladder: shared-gateway swap → public-LN → **on-chain peg-out** → retry+alert. Never silent |
| D5 | Auto-allocation | **Curated, remotely-updatable allowlist** only; discovered feds joinable manually, never auto-funded |
| D6 | Balance UX | One unified balance on home + optional per-federation health/breakdown view |

**Why the re-sequence (the second model's case, accepted):** the engine hedges a risk you can't measure (insolvency); evacuate-on-degrade may fire too late (a failing fed's exit is already jammed); the fee burn may exceed the custody risk removed for small balances (no EV computed); and two platform bets (Slint live-camera preview on Android, Android Doze killing periodic probing) are unproven and possibly fatal. v1 proves those bets cheaply before betting funds on the allocator.

**Hard gates before the engine ships (v2):**
1. Compute fee-vs-risk **EV** at target balance sizes. If net-negative for a $50–$500 user, don't ship the engine.
2. Replace reactive evacuate-on-degrade with **proactive continuous rebalancing** away from concentration (reactive is too late).
3. Reconcile the **spending federation (D3) against the concentration cap (D5/§4.3)** — it is the highest-velocity custodian by design.
4. **Feasibility spikes in v1:** Slint camera-preview surface on Android; Android Doze/WorkManager timing; recovery-from-seed on a real device.
5. Treat the **remote allowlist as a control plane** (sign it, threat-model compromise) and name the **fiduciary/regulatory** posture of directing user funds into chosen custodians.

---

## 1. The custody & recovery model (the spine of the whole design)

Get this exactly right, because it dictates onboarding, backup, and the engine.

| | **Wallet of Satoshi** | **This wallet (Fedimint)** |
|---|---|---|
| Who holds the BTC | One company | A **federation** — m-of-n guardian multisig |
| Trust surface | Single company | A guardian *quorum* (no single rug), but still trusted for **solvency** |
| What the user holds | Nothing (an account) | **Bearer ecash** — blind-signed notes the federation can't link to you |
| Privacy | None (provider sees all) | Federation can't see your balance or history |
| Seed needed? | No | **Yes** — to recover ecash on a new device |
| Lose your phone | Log back in, fine | Need the seed (or a backup) or funds are lost |

**The honest positioning:** *trust-minimized custody (a federation, not a company) + self-custodied recovery.* Not self-custody of funds; not pure custody either.

**Why WoS-grade onboarding is impossible to copy verbatim:** WoS skips the seed because it's fully custodial. You can't. The seed is the only thing between "lost phone" and "lost funds." So the real product question is **not "do we have a seed" but "how do we make the seed invisible until it matters?"**

### The seed-invisibility spectrum (the most important decision in the build)

| Approach | Seen in | UX cost | Recovery robustness |
|---|---|---|---|
| Show 12 words on day 1, force write-down | harbor | High friction, very un-WoS | Strong, user-owned |
| Generate silently, back up invite codes to Nostr, seed revealed only on demand | ecash-app | Low friction | Good (seed + fed list both needed) |
| Generate silently, store in browser, "back up later" nag | vipr | Lowest friction | Weak (seed sits in browser) |
| Social / cloud recovery, user may never see a seed | Fedi | Lowest friction | Strong but complex (custom module) |

**Recommendation:** generate the seed silently at onboarding-finalize (one screen, "Create wallet"), defer the backup, and **auto-back-up the *federation set* (invite codes) durably** (Nostr or cloud) so recovery = seed phrase + the list of federations you were in. The seed recovers ecash *within* each federation; the federation list tells you which to rejoin. For a multi-federation wallet, **durably persisting the set of joined federations is itself a recovery requirement**, not an afterthought.

---

## 2. The four wallets surveyed

| | **ecash-app** | **harbor** | **vipr-wallet** | **Fedi** |
|---|---|---|---|---|
| Stack | Flutter + Rust (FRB) | Pure Rust (iced) | Vue 3 + TS PWA (WASM) | Rust core + RN/PWA |
| Platform | Mobile (Android) | Desktop | Web/PWA | iOS + Android + web |
| Activity | 🟢 active | 🔴 dormant 10mo | 🟢 active | 🟢 active |
| Scale | ~5.5k Rust + 22.7k Dart | ~13.5k Rust | ~25k TS/Vue | ~51k Rust + 184k TS |
| Maintainers | 8 | 14 (Mutiny lineage) | 1 (+ bots) | 52 |
| Fedimint | git master + **v2** | stable 0.7.1 | WASM canary | own fork v0.10-fedi20 |
| Distinctive | NWC, LN-Address, Boltcard | **Cashu + Tor**, x-mint transfer | best CI/CD | the superapp + best architecture |
| Multi-fed | manual | manual | manual | manual (plumbing ready) |
| Seed at rest | ❌ plaintext RocksDB | ⚠️ plaintext in SQLCipher DB | ❌ plaintext IndexedDB | ❌ plaintext JSON |

### What to steal / what to avoid, per wallet

**ecash-app** — *the feature maximalist.*
- ✅ Steal: breadth proves Fedimint can do a lot (LNv1/v2, mint v1/v2, wallet v1/v2, Lightning Address, Boltcard/LNURLw, Nostr backup of invite codes). Its `parse.rs` is a model of a dependency-injected, well-tested payment-string parser. Strong CI (clippy `-D warnings`, fmt, translation lint).
- ❌ Avoid: `multimint.rs` is a **5,473-line god object with 63 `expect()` in live payment paths** — malformed federation metadata *panics the Rust isolate*. The money engine has **1 unit test**. PIN = unsalted SHA-256 of 4–6 digits that encrypts nothing. Dead default relays, single-source price feed, NWC failures silently swallowed.

**harbor** — *the privacy purist (on ice).*
- ✅ Steal: **cleanest architecture** — strict client/UI split, typed message bus, Elm model kept render-only. The only one with **real Tor** (arti) and real **Cashu + Fedimint** with cross-protocol transfers. Only one with **whole-DB encryption (SQLCipher)**. Complete signed cross-platform release pipeline.
- ❌ Avoid: dormant (last commit Sept 2025). Advertised auto-fund-movement **doesn't exist**. LNv2 feature-gated **off**. Password = DB key with no stretching, **auto-saved to OS keyring + auto-read** → prompt-less unlock = OS-session compromise is wallet compromise. Tagged `1.0.0` on alpha software.

**vipr-wallet** — *the polished web app.*
- ✅ Steal: **best CI/CD by far** (trivy, dependency-review, lighthouse, real devimint e2e, release-please, hardened non-root Docker). Exceptional code hygiene (near-zero `any`, type-aware ESLint with `no-floating-promises` as error, redacting logger). Every feature genuinely wired incl. full on-chain peg-in/peg-out.
- ❌ Avoid: **PIN/biometric lock is a cosmetic Vue overlay** — doesn't close the wallet or clear the seed; **WebAuthn unlock never verifies the assertion** (any credential unlocks it). Seed unencrypted in IndexedDB/OPFS *and* in memory. **No CSP.** 2,288-line god-store. Pinned to a canary SDK reached via `as unknown as` internals-mutation. Money-path tests are all against canned mocks; only end-to-end value flow is *receiving*.

**Fedi** — *the superapp / production reference.* (See §3, it's the architecture to copy.)
- ✅ Steal: the **bridge pattern** (one Rust core → native + web), `ts-rs` generated bindings, all-logic-in-Rust, gateway fully hidden, balance-as-pushed-event, **devimint integration tests** against real regtest federations + LND gateway.
- ❌ Avoid: superapp scope (~60% of crates), and **mnemonic stored as plaintext JSON** with no keychain/keystore/DB encryption.

---

## 3. Recommended architecture — pure Rust, Android-only (≈ harbor + Slint)

**Decision (locked):** pure Rust, single Android target, **Slint** UI. This deletes the most complex part of every other option — the entire cross-language bridge. There is no FFI-to-JS, no codegen, no second language. Harbor is the structural template (it's already pure Rust with a client/UI crate split); the only thing harbor can't give you is the toolkit — its `iced` is desktop-only, so the UI is Slint instead.

```
   one cargo workspace, one process, one tokio runtime
   ┌─────────────────────────────────────────────────────────┐
   │  wallet-core   (lib crate)                              │
   │  Fedimint clients · risk engine · gateway selection ·    │
   │  recovery · balance · storage · encryption              │
   └───────────────▲───────────────────────┬─────────────────┘
        state updates │ (tokio watch/mpsc)   │ commands (fn calls / mpsc)
   ┌───────────────┴───────────────────────▼─────────────────┐
   │  wallet-ui   (Slint)  — 6 screens, in-process            │
   └───────────────┬─────────────────────────────────────────┘
                   │ thin JNI shims (only where Android forces it)
   ┌───────────────▼─────────────────────────────────────────┐
   │  Android Keystore · BiometricPrompt · Camera · (NFC)     │
   └─────────────────────────────────────────────────────────┘
   build: cargo-ndk → .so + NativeActivity APK  (or xbuild)
```

**Concrete rules (keep Fedi's *logic* lessons, drop its *plumbing*):**
1. **No bridge. The UI calls core directly.** What was Fedi's `rpc(method, payload) → json` boundary collapses to ordinary async function calls + channels within one binary. The whole UniFFI/wasm/ts-rs codegen layer is gone.
2. **Two crates, one process** — `wallet-core` (lib) + `wallet-ui` (Slint), exactly harbor's `harbor-client`/`harbor-ui` split. Communicate over `tokio::sync::mpsc` (UI→core commands) + `watch`/`mpsc` (core→UI state), which is harbor's in-process "bridge" pattern minus the FFI.
3. **All intelligence in core; the UI is thin.** Gateway selection, failover, retry, recovery, balance computation, *and the risk engine* live in `wallet-core`. The UI renders state and forwards intents — no retry loops, no business logic. (Same division that makes Fedi/harbor robust on flaky networks.)
4. **Balance & transactions are pushed from the local DB, not polled.** Core computes balance from the mint DB and pushes it over a `watch` channel; Slint binds to it reactively → instant, offline-capable balance. (Fedi's `balance` event, done in-process.)
5. **Storage behind a trait** (RocksDB via `fedimint_rocksdb`, or redb). The trait is the clean seam to slot in **at-rest encryption** (§5) — encrypt the DB with a key held in Android Keystore.
6. **Async runtime in-process.** One `tokio` runtime shared by core and the Slint event loop (Slint integrates with an async executor); long ops `spawn` and report back over channels.

**Stack:**
- **Language/UI:** Rust + Slint (`.slint` markup, Rust logic). Native rendering, no webview.
- **Android packaging:** `android-activity` (NativeActivity) + `cargo-ndk` to build per-ABI `.so`, wrapped in a minimal APK; or `xbuild` for a near-Java-free APK.
- **The JNI asterisk:** business logic + UI are pure Rust, but Android **Keystore** (the seed-at-rest fix — non-negotiable), **BiometricPrompt**, **camera frame capture** for QR (decoding is pure Rust via `rqrr`; frame capture is platform), and **NFC** (only if Boltcard later) need thin JNI shims via the `jni` crate. Budget ~3–4 small bridge modules; no Kotlin *business logic*.
- **What this buys you:** one language, one binary, one workspace, no codegen, no two-platform UI duplication. Versus the Fedi stack you delete ~all of the `ui/` tree (≈184k LOC in Fedi), UniFFI, wasm, ts-rs, iOS, and web. The cost is a less mature UI ecosystem and owning the platform glue — acceptable for a ~6-screen wallet.

---

## 4. The risk engine (the novel core)

User-visible: nothing but one balance and send/receive. Under the hood: an allocator across N federations.

### 4.1 The hard constraint that dictates everything

**Ecash is not fungible across federations.** A note from A is worthless in B. So:
- "Balance" = the **sum** of per-federation balances.
- Moving value A→B is *pay an invoice out of A, receive it in B* — it costs a gateway fee each time. **Rebalancing existing funds is not free.**

This splits the levers into **cheap** and **expensive**, and the strategy must lean on the cheap ones:

| Lever | Cost | Role |
|---|---|---|
| **Allocate on inflow** — route each *receive* into the healthiest under-allocated federation | ~free | **Primary** allocation tool |
| **De-risk on spend** — pay *out of* the riskiest / most-concentrated federation | ~free | Sends naturally flatten exposure |
| **Active rebalance** — silently LN-move A→B | gateway fee + fragmentation | **Only** to evacuate a degrading federation |

The high-value automated action is **evacuate**: when a federation's score craters (guardians dropping, withdrawal probe failing), sweep funds out *while you still can*. That's the one time paying to rebalance is unambiguously worth it.

> **Shared-gateway internal swaps make "active rebalance" cheaper** — verified against fedimint `master` (see §4.6). When one gateway serves both A and B, an A→B move settles *internally* at that gateway with no public-Lightning hop. This makes **"federations that share a healthy gateway"** a first-class selection criterion.

### 4.2 The risk score — two buckets

**Public data (passive, cheap, queryable):**
- Guardian set: count + threshold (a *1-of-1* "mint" is just a custodian; *4-of-7 independent operators* is real decentralization), guardian identities & independence.
- Federation age, uptime, track record.
- [Fedimint Observer](https://observer.fedimint.org) metrics (liquidity, activity, consensus health).
- Guardian software version (stale = risk).
- On-chain backing UTXO size (visible; but see caveat).
- Published metadata / ToS / contact, Nostr reputation signals.

**Empirical probes (active):**
- **Quorum liveness** — can you fetch config from enough guardians?
- **Round-trip test** — deposit a few sats via LN, redeem them back; measure success, latency, fee.
- **Withdrawal / peg-out probe** — the real custody test: can money get *out*?
- **Gateway availability & fees** — a federation with no live gateway is useless for Lightning; gateway liveness is a *separate axis* from federation health.
- **API latency / degraded-quorum detection.**

### 4.3 Allocation policy

- **Concentration cap** — never hold more than X% (or absolute Y sats) in any one federation. The core risk control.
- **Inflow-directed allocation** — route receives toward target weights (cheap).
- **Spend-directed de-risking** — spend riskiest/most-concentrated first (cheap).
- **Periodic empirical re-scoring** — re-probe on a schedule; demote on failure.
- **Evacuate-on-degrade** — auto-sweep out of a federation whose score craters (the only fee-worthy active rebalance).
- **Minimum-viable-balance per federation** — avoid dust scattered across feds that can't fund a payment.

### 4.4 Honest caveats (state these plainly in the product)

- **You cannot verify solvency.** Public data shows the guardians' multisig balance but **not total ecash issued** — you can't prove a federation isn't over-issuing. Scoring is a *heuristic risk proxy, not an audit*. **Diversification is the hedge precisely because you can't verify.**
- **Diversification costs fragmentation.** Spread across 15 federations and a single payment may exceed every individual balance → you need multi-federation/multi-path sends or you can't pay. Sweet spot is likely **3–6 federations**, not 50.
- **Every federation is only as usable as its gateway**, and **leaning on one shared gateway concentrates gateway trust** — if it dies, multiple federations lose their cheap bridge at once. Want ~2 overlapping gateways across the cluster, not 1.
- **Internal swaps trade privacy-from-the-bridge for cost** — a shared gateway is party to *both* legs and learns you moved A→B.

### 4.5 The portfolio as a graph

Build the bipartite graph **federations ↔ their registered gateways** (queryable on-network — each federation publishes its gateways). Two federations sharing a healthy gateway = a **cheap-swap edge**. Prefer a portfolio that forms a *connected cluster over 1–2 reliable shared gateways* → cheap internal rebalancing/evacuation. Monitor per-federation gateway liquidity (an A→B swap needs the gateway to hold ecash on the **B** side).

### 4.6 Shared-gateway internal swaps — verified against fedimint `master`

**Verdict: real and shipped today** (in the normal full gateway — *not* the `gatewayd-lite-lnv2` proposal, which is an orthogonal, unimplemented draft). This is the cheap-rebalancing mechanism the engine can lean on, with sharp constraints.

**How it works (implemented):** if one LNv2 gateway is registered with both A and B, and B's receive invoice was minted by *that* gateway, then a send from A whose payee key matches the gateway's own Lightning node triggers `is_direct_swap` → `relay_direct_swap`: the gateway funds B's `IncomingContract` with its **own B-side ecash**, gets the preimage from B's federation, and claims A's `OutgoingContract` — **the public-Lightning `pay()` branch is skipped entirely.** (`modules/fedimint-gwv2-client/src/send_sm.rs`, `gateway/fedimint-gateway-server/src/lib.rs` `is_direct_swap`/`relay_direct_swap`.) A sibling `relay_lnv1_swap` path even bridges LNv1↔LNv2 on a shared gateway.

**It's quoted before you commit, at a cheaper rate.** The client computes `send_fee_minimum` (the direct-swap rate) vs `send_fee_default` (which includes a Lightning-routing reserve) using the *same* payee-key test that selects the swap, and rejects any quote above a hard client-side cap (`SEND_FEE_LIMIT = 100 sat + 1.5%`). So the user sees the real fee up front and a too-greedy gateway is simply never selected.

**Cheaper, not free — and not private:**
- The saving is only the **Lightning routing reserve** (`send_fee_default − send_fee_minimum`). You still pay the gateway's base+ppm margin **and** B's receive fee. A gateway is allowed to set `send_fee_minimum == send_fee_default`, passing you *no* discount while pocketing the saving — so **compare the two fees in `RoutingInfo`** to know if a gateway actually rewards swaps.
- **No privacy gain.** BOLT11 swaps use `PaymentImage::Hash`; the same operator builds B's invoice and matches its payment hash in A, so it correlates both legs. For privacy this is *worse* than two unrelated gateways. Do not market internal swaps as private.

**Liquidity is the real constraint.** The gateway holds **separate ecash inventory per federation**; an A→B swap *consumes its B-side ecash* and credits its A-side. If the gateway is dry on B, the swap **fails safely** (your A-side contract is refunded) but simply doesn't happen. So before relying on an A→B swap, check the gateway's **destination-side** balance (`get-balances` / `ecash_balances`). A shared gateway with a dry B wallet is useless for rebalancing into B.

**Design rules the engine must follow to actually get the cheap path:**
1. Prefer federation clusters that share ≥1 (ideally 2) common gateway pubkey (`RoutingInfo.lightning_public_key`).
2. **Pin the shared gateway explicitly on *both* legs** (B's receive invoice *and* A's send). If you don't, gateway auto-selection may pick a different node and you silently fall back to a real Lightning payment at `send_fee_default`.
3. Gate rebalances on the gateway's destination-side ecash liquidity.
4. Treat the swap as a *cost* optimization only — for risk-evacuation it's great; never rely on it for privacy.

> Net: shared-gateway swaps turn cross-federation **evacuation/rebalance** from "a full Lightning payment" into "one gateway's internal operation, quoted up front, no routing reserve" — provided you choose gateway-overlapping federations, pin the gateway, and watch its per-federation liquidity. This raises the ceiling on how freely the engine can move funds, but the liquidity dependency and zero privacy keep **evacuate-only + passive allocation** the sane default (§4.3).

---

## 5. The thing all four got wrong — fix it

**Seed at rest.** Every wallet surveyed fails here: ecash-app (plaintext RocksDB), vipr (plaintext IndexedDB + memory, cosmetic lock), Fedi (plaintext JSON), harbor (best — SQLCipher — but auto-unlocks via keyring). **This is where you beat all of them** (and the pure-Rust/Android target makes it clean):
- Generate a **hardware-backed key in Android Keystore** (via a thin JNI shim) and use it to **encrypt the local DB**; the storage trait (§3 rule 5) is the seam. The seed lives only inside the encrypted DB — never in plaintext app files.
- If you add a PIN/biometric lock, make it **real**: gate the Keystore key behind **BiometricPrompt / `setUserAuthenticationRequired`** so the DB-encryption key is *unwrappable only after auth* — not a UI overlay over an already-decrypted wallet (vipr's mistake). A PIN should Argon2-derive a key that wraps the seed, not just compare a hash.
- The seed should **never leave `wallet-core` for the UI** except on an explicit "reveal seed" action. With no FFI/JS boundary this is just discipline in one crate — easy to enforce.

**Money-path testing.** All four have great lint/CI but thin behavioral coverage of send/receive. **Copy Fedi's devimint integration harness**: spin up real regtest federations + an LND gateway and drive `wallet-core`'s API directly through join → receive → send → recover → *and the allocation/evacuation engine*. (No FFI to mock — tests call the same Rust functions the UI does.) That harness is what separates a production wallet from a fragile one.

---

## 6. MVP scope & cut-list

### Build first — v1 foundation (single federation, architected for the engine)
1. Workspace skeleton: `wallet-core` (lib) + `wallet-ui` (Slint) + in-process command/state channels; `cargo-ndk`/`xbuild` Android build + JNI shims for Keystore/biometric/camera.
2. **Feasibility spikes up front** (de-risk before building on them): Slint camera-preview surface on Android, Android Doze/WorkManager timing, recovery-from-seed on a real device.
3. Silent seed generation; **Keystore-backed key, encrypted DB** — the win over all four competitors.
4. **One** curated default federation. Lightning send/receive with hidden auto gateway selection + single-retry failover (copy Fedi). Ecash issue/reissue; on-chain deposit/withdraw.
5. Balance + tx list as **pushed events** from local DB; one unified balance (trivially correct at N=1).
6. Recovery: phrase + rejoin-with-recover; durable backup of the (single) federation invite.
7. **Engine seam, dormant:** allocator as a pure function running at N=1, plus the decision journal and storage seam, so v2 turns on multi-federation with no rewrite.
8. devimint money-path integration harness (copy Fedi), gating release.

### v2 — turn on the engine (only after the v1 gates pass, §0.5)
Multi-federation across the curated allowlist; designated spending federation (D3); **proactive** rebalancing + escalating evacuation ladder incl. on-chain peg-out (D4); concentration cap reconciled with the spending fed; per-federation health view (D6). **Gated on the fee-vs-risk EV computation.**

### Defer / cut (learned from Fedi's superapp)
Matrix chat · communities · multispend/group multisig · stability pool / USD-pegged balance · sp-transfer · Nostr social graph · social/seed-split recovery · device registration/transfer · in-app fee/revenue layer · NWC · Lightning Address (nice later, not MVP). These are ~60% of Fedi's crates and the large majority of its TS.

---

## 7. Open decisions (need your call)

1. ~~**Curated vs. dynamic**~~ — **RESOLVED (D5):** curated, remotely-updatable allowlist for auto-funding; discovered feds joinable manually, never auto-funded.
2. ~~**Rebalance aggressiveness**~~ — **RESOLVED (D4/D7):** v1 ships no live engine; v2 uses proactive rebalancing + the escalating evacuation ladder. **The one open gate that matters: compute fee-vs-risk EV at target balance sizes before building the v2 engine (§0.5).**
3. **Seed-invisibility level** — silent-generate + defer backup (recommended), or force write-down day 1?
4. ~~**Platform**~~ — **RESOLVED:** pure Rust, Android-only, Slint UI (≈ harbor structure + Slint toolkit). No iOS, no web, no FFI/JS. See §3.
5. **Cashu too?** — harbor shows Fedimint + Cashu can share one engine. Out of scope for MVP, but the allocation engine generalizes to Cashu mints as just another federation-like custodian.

---

*Sources: per-wallet deep-dives of ecash-app, harbor, vipr-wallet, and the Fedi monorepo, plus the fedimint LNv2/gateway source at `~/p/fedimint` (verified on `master`, §4.6). Architecture target: pure Rust + Android-only + Slint (§3).*

---

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 1 | issues_open | HOLD_SCOPE; re-sequenced engine-first → foundation-first; 7 decisions; 0 critical gaps open |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | — | not run |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 0 | — | not run |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | not run |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | — | not run |

- **CROSS-MODEL:** an outside-voice (independent Claude subagent) challenged engine-first on five independent grounds (unmeasurable target, too-late evacuation, uncomputed EV, unproven Slint/Doze feasibility, fiduciary/control-plane exposure). Accepted; plan re-sequenced to foundation-first (D7=C). No remaining cross-model disagreement.
- **VERDICT:** CEO review complete — scope held, engine deferred to v2 behind hard gates (§0.5). Eng review required before implementation.

NO UNRESOLVED DECISIONS
