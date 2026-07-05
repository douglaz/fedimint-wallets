# Fresh-eyes review — engine, specs, CLI (2026-07-05)

**Base:** `main @ 1bb7a46`. **Method:** five independent review passes over disjoint scopes
(wallet-core pure logic; the wallet-fedimint money path; orchestration + sensing; all specs/ADRs;
wallet-cli + test harness), followed by re-verification of every P1/P2 headline against the code
and the pinned SDK at `~/p/fedimint` (file:line cites below are to that checkout). Gate state at
review time: 185 tests green, clippy clean.

**Verdict.** No P0 (no constructible money-loss or duplication path). The double-mint/double-pay
machinery, journal atomicity, fee-model exactness vs the SDK, and the never-over sizing loop all
held up under adversarial reading — within their own fee model. The material findings are: a P1
trust error in the primary evacuation signal, cap enforcement that exists nowhere at perform time,
an evacuation-destination trust bypass, a liveness wedge covering every deterministic send
rejection, a gateway-controllable TOCTOU hole in the never-over invariant, and P1-level spec drift
that would mislead the Phase 4 implementer. Everything below was verified, not assumed.

---

## P1 — fix before or during Phase 4

### P1-1. The PRIMARY shutdown signal is override-controlled and uncorroborated
`probe.rs:79-92, 187-200, 342-365`. The probe treats `Client::get_meta_expiration_timestamp()` as
the "consensus-backed" primary evacuation trigger and applies **no corroboration** to it — in
deliberate contrast to the f+1 gate on the secondary `/status` signal. But in the pinned SDK the
default meta source is `LegacyMetaSource` (`fedimint-client/src/client/builder.rs:139`), whose
`fetch` merges consensus config meta with an HTTP fetch from the federation's `meta_override_url`
and lets the override **win** on duplicate keys
(`fedimint-client-module/src/meta.rs:87`: `config_iter.chain(overrides).collect()`).

- **Forge direction:** whoever serves `meta_override_url` (often a single party, not the guardian
  quorum) can set `federation_expiry_timestamp = now` and force every wallet probing that fed to
  evacuate — an unwanted two-leg Lightning move (fees up to `fee_cap`) plus denial of the fed,
  mass-triggerable.
- **Block direction:** `fetch` propagates an override-fetch error (`meta.rs:85`: `.await?`), so a
  *down* override host prevents any NEW meta values from being cached (an already-cached expiry
  survives — `MetaService::get_field` serves the DB cache first,
  `fedimint-client/src/meta.rs:48-63`). The lost case is the one that matters operationally: an
  expiry ANNOUNCED after the wallet's last successful fetch (or a fresh join) is never learned —
  and shutdown announcements arrive shortly before shutdown, exactly when the fresh fetch is
  needed. The probe reads `None` and the pre-expiry evacuation window ADR-0018 exists for is
  silently lost.

**Fix:** subject the merged expiry value to the same f+1 corroboration as `/status`, or source it
from a genuinely consensus-backed read — the meta MODULE's consensus values where the federation
runs one, or a fresh authenticated fetch; NOT `client.config().global.meta`, which is the cached
at-join config (`fedimint-client/src/client.rs:485-486`) and would miss any expiry announced after
join. Correct the "consensus-backed" comments either way.

### P1-2. The per-fed cap is enforced NOWHERE at perform time — breachable jointly by the tick and without limit by operator commands
Two limbs of one invariant failure (ADR-0018: the cap "must be ENFORCED — refuse or warn above
threshold"):
- **Automated (joint breach):** `wallet-core/src/allocator.rs:116-120, 261` — every
  `Move`/`Evacuate` is sized against `cap_room = per_fed_cap − dest.spendable` recomputed from the
  **frozen snapshot**; nothing debits inflows already planned this tick, and distinct idempotency
  keys (`evac:A:C` vs `evac:B:C`) keep both decisions. Two sick feds evacuating into the same
  destination in one tick land it at ~2× the cap (ditto an evacuation coinciding with a standby
  top-up) — correlated degradation is exactly evacuation's scenario. The
  `RefuseInflow { OverCap }` advisory fires only on the *next* tick, after the money moved.
- **Operator (no check at all):** `Runtime::do_move` / `Runtime::direct_inflow`
  (`runtime.rs:171-250, 264-311`) never consult the destination balance or ANY cap — the preflight
  validates only the lnv2 minimum-contract floor (`executor.rs:385-393`). One manual
  `wallet-cli move`/`direct-inflow` pushes a federation arbitrarily above the hard cap.

Verified: **no per-fed-cap check exists anywhere in wallet-fedimint at perform time** — the
allocator's snapshot sizing is the only enforcement in the system.

**Fix:** (a) thread a running `planned_inbound: BTreeMap<FederationId, u64>` through `decide`,
debit it after each inbound decision, and add a golden asserting Σ(inbound per destination per
tick) ≤ cap_room; (b) add a perform-time destination-balance check in the EXECUTOR (before
minting the receive invoice), covering operator verbs and tick decisions alike — the snapshot
can be stale by the time `perform` runs (e.g. a pending receive auto-claims mid-tick), so a
planning-time ledger alone still leaves the automatic path breachable. Refuse over-cap, with an
explicit override flag if operators need one — ADR-0018 allows "refuse or warn", it does not
allow silence.

### P1-3. Evacuation fallback can target a scorer-INELIGIBLE federation
`wallet-core/src/allocator.rs:178-206` + `wallet-fedimint/src/tick.rs:110-117` +
`probe.rs:137-159`. `eligible_for_evacuation` requires only not-self, no evacuation reason,
`probed_ok` (liveness+route), `reputation ≥ 0`, and cap room. The scorer's verdict
(`eligible_to_fund` — the structural floor: guardian count, network, modules, fault tolerance) is
consumed **only** for spending/standby auto-designation; `assemble_status` sets `reputation: 0`
unconditionally, and scorer-rejected feds stay in `snapshot.federations` by design. So when the
standby is full or unhealthy, `safest_other`'s fallback will evacuate a dying fed's **entire
balance** into a joined 1-of-1 (same-network, structurally weak) federation the scorer would never
fund — the trust model's largest money move bypasses the trust model. (A WRONG-network destination
does not actually receive the funds — the send leg rejects its invoice as `WrongCurrency`,
`lnv2-client/src/lib.rs:552-557` — but the evacuation then wedges in P1-4's retry loop instead;
either way the evacuation fails its purpose.)

**Fix:** carry fundability into `FederationStatus` (e.g. `eligible_to_fund: bool` set from the
scorer verdict in `build_snapshot`) and require it in `eligible_for_evacuation`. Degrading to
`RefuseInflow` when no vetted destination exists is already the modeled behavior.

### P1-4. EVERY deterministic send rejection is an immortal Retryable livelock at `Pay`
`executor.rs:807-811`, `multi_client.rs:654-666` (`map_send_result`), `multi_client.rs:554`
(`RECEIVE_EXPIRY_SECS = 3600`), `move_protocol.rs:241-262`. `map_send_result` collapses every
non-dedup `SendPaymentError` into a generic `anyhow` error, and the Pay arm maps that whole class
`.map_err(retryable)` → the intent resets to `Pending` → next reconcile re-drives → the same
deterministic rejection, forever. The canonical instance is invoice EXPIRY: the receive invoice is
minted once with a fixed 1h expiry and never re-minted (`next_step` never returns `CreateInvoice`
once `invoice.is_some()` — correct for idempotency), and the SDK rejects an expired invoice before
doing anything (`modules/fedimint-lnv2-client/src/lib.rs:548`) — so any move that cannot pay
within its hour wedges permanently. But `WrongCurrency` (`lib.rs:552-557`),
`FederationNotSupported`, and gateway fee/expiry-limit breaches wedge identically. No money is
lost (nothing was paid), but the corridor is wedged and — for an `Evacuate` — the
flee-a-dying-federation feature silently never completes and never terminally fails.

**Fix:** classify the deterministic `SendPaymentError` variants as `Permanent` in
`map_send_result` (keep transport/gateway-unreachable failures Retryable), and additionally check
expiry at the Pay step (the BOLT11 is already parsed there). Safe by construction: `next_step`
only routes to `Pay` when cache + exhaustive backfill show no send op exists. A fresh occurrence
then retries with a new invoice.

### P1-5. Spec drift that would mis-steer the Phase 4 implementer
*(Doc-only — no live code defect. P1 by TIMING, not severity: this report's P1 bucket is "fix
before or during Phase 4", and these two items actively mislead the implementation about to
start.)*
1. **phase4-implementation-spec §2 + TODOS R2 describe the send-leg fee bug as live; it was fixed
   in the 3.A merge (`5315df3`).** Verified: `multi_client.rs:396-405` is now
   `send_fee_quote_for_amount(&self, id, amount: Msat)` (the invoice-parameter method no longer
   exists) and the Pay arm (`executor.rs:777-792`) quotes the federation fee on the full outgoing
   contract (`invoice + gateway_fee.on(invoice)`). §2's remaining work is item 3 only (persist the
   quotes on `MoveRecord`). As written, §2 sends the implementer hunting for a method that doesn't
   exist and invites a redundant over-estimate on top of the already-correct quote.
2. **`Stranded` retryable-vs-terminal contradiction.** `phase4-plan.md:27-31` and the 2026-07-03
   engine review say Stranded stays re-drivable; `phase4-implementation-spec.md:127-150, 634-635`
   settles the opposite (TERMINAL, `next_step(Stranded) == Failed`). The roadmap routes readers to
   phase4-plan first. Annotate the two older docs with the settled answer.

---

### P1-6. Receive-side fee drift — TWO live never-over holes (gateway fee at mint, federation fee at claim)
The verified-bisection machinery proves never-over against fees frozen at quote time; both
receive-side fees can move after that proof:
- **Gateway fee, quote→mint (adversarially controllable):** the SDK re-fetches `routing_info`
  inside `create_contract_and_fetch_invoice` (`lnv2-client/src/lib.rs:917-935`) and sizes the
  contract with the **fresh** fee (`receive_fee.subtract_from(amount)`). A fee *drop* in the
  window commits a larger contract and the destination nets MORE than asked (and can overrun the
  `cap_room` the move was sized by); a *rise* makes the derived `receive_quote`
  (`invoice − amount`) understate true cost. The timing is controlled by an UNTRUSTED party: a
  gateway can deliberately lower its advertised fee right after being quoted.
- **Federation fee, quote→claim (natural drift):** `fee_quote` is point-in-time over the note
  inventory, and the mint re-runs consolidation/funding at claim time — a lower actual claim fee
  nets the recipient `contract − actual_fee > asked`. The code comment acknowledges only the
  under direction ("the claim-time fee model gap already under-delivers a hair",
  `executor.rs:277-279`); the over direction is equally real, the window is long for a
  `DirectInflow` (until the external payer pays), and P2-5's missing read-back means the overage
  is never even observed.

The magnitude of both is bounded by the quote delta and the SDK's fee limits, but never-over is
stated as unconditional and cap accounting builds on it — this belongs with the money bugs.

**Fix (mint window):** after `mc.receive` commits, compare the op's committed
`contract.commitment.amount` against `grossed.contract_amount`; on mismatch, stop before
surfacing/paying (the invoice is unpaid at that point — for a Move we are the only payer; a
DirectInflow's invoice has not been surfaced). Note this needs a NEW modeled discard/remint
transition: `MultiClient::receive` is deliberately non-idempotent (`multi_client.rs:286-289`) and
`next_step` never re-enters `CreateInvoice` once an invoice exists (`move_protocol.rs:257-261`),
so "re-quote" means recording the mismatched leg as discarded and minting fresh under a new
attempt key — not silently re-driving the same one.
**Fix (claim window):** the claim fires autonomously when the payer pays, so this one cannot be
refused — it must be OBSERVED and accounted: read the settled amount back after `Claimed`
(P2-5's read-back is the detection mechanism), record delivered-vs-asked, and count the overage
in cap accounting.

(The send side has only the quote→`pay` cap-check window — the same re-quote drift as P2-2 —
since no send quote is persisted anywhere today; when Phase 4 §2 adds persisted quotes, restate
the honest send cost from `SendOperationMeta.contract.amount` post-Started.)

---

## P2 — should fix (defects and load-bearing debt); ordered by money-risk

1. **A tick has no deadline.** `runtime.rs:455-476` → `apply` → `AwaitSettle` blocks on
   `await_send`/`await_receive` (SDK long-polls, up to 60-min per-request). One stalled gateway
   freezes probing, other evacuations, everything — precisely when the engine most needs to act.
   Bound each perform (or the tick) with a wall-clock deadline; timeout leaves the intent Pending.
2. **Pay-step over-cap is `Permanent` on ONE fresh quote.** `executor.rs:793-795`. The receive
   component is fixed and was already cap-checked; only the send re-quote (live gateway fetch +
   note-inventory dry-run) can push it over — one spiky quote terminally fails an `Evacuate`,
   stranding funds on the dying fed. Contradicts `size_fresh_evacuation`'s own stance one step
   earlier. Fix: Retryable when the receive part alone fits; Permanent only when it doesn't.
3. **Default gateway selection stops at the FIRST registered gateway; the SDK scans until one
   responds.** `executor.rs:194-200` (`resolve_gateway`: `gateways().next()`),
   `probe.rs:406-419` (same, then validate-or-false), `runtime.rs:767-779` (preflight). The
   pinned SDK's own `select_gateway` loops over ALL registered gateways and takes the first
   responsive one (`lnv2-client/src/lib.rs:481-487`, `GatewaysUnresponsive` only when none
   answer). A federation whose first-registered gateway is stale or unreachable from us is
   treated as unroutable even when another registered gateway works: the probe sets
   `gateway_available = false` → `probed_ok = false`, so the fed is receive-blocked — it can't
   be funded and can't be an evacuation DESTINATION — and default-gateway `move`/`direct-inflow`
   fail. Fail-safe direction, but it silently shrinks the wallet's routable universe to
   "feds whose first gateway happens to work". Fix: scan the registered set like the SDK does —
   and for send-required moves, scan for a gateway valid for BOTH federations
   (`validate_move_gateway_before_receive`/`validate_executor_move_route` require the chosen
   gateway to serve `from` too): a destination-valid-only scan still fails a route where
   gateway #1 serves `to` but only gateway #2 is shared with `from`.

4. **(debt) The verified solve loop has zero unit tests.** `executor.rs:228-380`
   (`quote_receive_gross_up_with_gateway_fee`): bounded verify passes, safe-under restatement,
   verified bisection, degenerate branches — welded to `MultiClient` and exercised only by manual
   devimint smokes. Three of the last five commits patched exactly this flow. Fix: make it (or
   `fee::gross_up`) generic over an async quote closure — the same extraction also makes the
   `CreateInvoice` hair-under path testable — and golden-test scripted quote streams: stable,
   two-step oscillation, staircase-converging-on-last-pass, non-monotone, and
   changing-between-loop-and-bisection.
5. **Delivered amount is never read back after `Claimed`** — a RECORD-accuracy gap, not a live
   cap bug. The receive fee quote is a point-in-time dry-run over the note inventory; the actual
   claim happens later against a changed inventory (either direction), and
   `ReceiveState::Claimed` carries no amount. Cap enforcement itself is unaffected today: every
   tick re-probes the real balance (`probe.rs:330`) and `decide` reads only probed `spendable` —
   but the durable `MoveRecord`/history amounts (and any user-facing "what happened") drift from
   what actually settled — and the read-back is also the only possible DETECTION mechanism for
   P1-6's claim-window over-delivery, which raises its priority beyond bookkeeping. Record
   delivered-vs-intended from the claim transaction's minted value when the Phase 4 ledger lands.
6. **Partial open silently shrinks the wallet's world — for `balance`, `status`, AND `tick`.**
   `open_all` is best-effort (stderr warn, skip; `multi_client.rs:146-157`), and everything
   downstream walks only the open set: `balance` (`main.rs:265-272`) totals the survivors and
   exits 0 while `list-feds` shows all joined feds — the two surfaces disagree; and
   `Runtime::probe_all` (`runtime.rs:786-800`) probes only `mc.federations()`, so `status`/`tick`
   build their snapshot from the surviving subset. `missing_pinned_feds` protects ONLY explicit
   `--spending`/`--standby` pins; under auto-designation a fed that fails to open silently
   vanishes from the universe — the allocator rebalances the remainder on a false global view
   (an evacuation-needing or over-cap fed simply isn't seen), and if every joined fed fails to
   open, `tick` returns an empty plan with exit 0. Surface joined-but-unopened feds in `balance`
   (`unavailable` rows / non-zero exit) and make `tick`/`status` fail loudly — or at minimum
   report — when the probed set is smaller than the joined registry.
7. **`Journal::pending()`/`failed()` return bare `Vec`** (`wallet-core/src/executor.rs:101-104`) —
   a broken store is indistinguishable from "no work"; `reconcile` reports a clean summary while
   stuck money-moves stop being retried. Return `Result`, surface the error.
8. **Fee-blind sizing at every layer — nothing reserves fee headroom.**
   - *Source side (missed by the first draft of this review):* `fund_into` sizes moves against raw
     `spendable` (`allocator.rs:36`) or `spendable − target_spending_balance` (`allocator.rs:57-61`),
     but the executor debits `invoice + send fees` from the source (`executor.rs:777-795`). A move
     sized at full `available` fails on insufficient balance and loops Retryable (feeding P1-4's
     expiry wedge); a standby-funding move sized to the exact surplus leaves the spending fed below
     its target by the fees, triggering a fee-burning top-up cycle next tick.
     phase4-implementation-spec §4 (fee-aware per-tick reservation) already specifies this fix —
     land it as designed.
   - *Inbound side:* cap arithmetic reads only `spendable` (`allocator.rs:22, 116-119, 167-172`).
     `in_flight` is populated (pending sends) but never consulted; `claimable` is deliberately
     hardcoded 0 (`probe.rs:511-529` — a light op-log read cannot distinguish an unpaid open
     invoice from a paid-but-unclaimed contract, and over-reporting would be the unsafe direction).
     So paid-but-unclaimed inbound value is invisible to cap room and can stack over the cap across
     occurrences (compounds P1-2). The missing piece is probe visibility of paid-but-unclaimed
     receives (the receive SM's update stream), THEN counting it toward cap room.
   - *Operator verb (`wallet-cli move`):* `Runtime::do_move` journals a raw `Action::Move`
     (`runtime.rs:264-293`) and only `Evacuate` gets perform-time downsizing
     (`size_fresh_evacuation`, `executor.rs:454-468`). A user moving their full visible balance
     hits the insufficient-balance Retryable loop at `Pay` and — after the invoice's hour —
     ages into P1-4's expired-invoice wedge, on a primary money command. Size (or preflight)
     operator moves against `spendable − expected fees` too.
9. **Evacuation drains through one destination per tick and prefers a nearly-full standby over a
    roomy fallback** (`allocator.rs:192-206, 259-276`): a standby with 1 msat of cap room wins,
    that tick evacuates 1 msat, and only on the NEXT tick — when the filled standby drops out of
    `eligible_for_evacuation` (`cap_room > 0`) — does the fallback get picked. Self-correcting,
    but each such hop wastes a tick of drain throughput under a shutdown clock. Pick the
    destination maximizing `min(spendable, cap_room)`, or emit multiple `Evacuate` decisions once
    P1-2's inbound ledger exists.
10. **Spec authority contradiction on the write discipline.** operation-history-spec rules 2/5/6
    (terminal-immutable; hard `Failed` repairs) vs phase4-implementation-spec §7/§10 (`repaired`
    rows re-advanceable by authoritative writes; 1h-gated SOFT failures; also `--since` dropped and
    `Receive.amount` flipped net→gross). Both claim normativity. Extend the authority note or
    update the older rules.
11. **Doc status staleness batch:** roadmap says 3.A "IN FLIGHT" (merged, gate passed);
    README says "Phases 1–3 complete through sense+decide" (wrong on both ends);
    `phase2-plan.md:36` + `tick.rs:47` cite ADR-0009 (AGPL) for the standing instruction
    (ADR-0014); CONTEXT.md still instructs the forbidden ADR-0010 "guardian-independence" claim
    (ADR-0006 update); devimint-runbook's two-fed guidance predates the harness patch the smokes
    depend on; phase1 spec's `Action` shape (gateway/occurrence fields, `Cap { limit }`) doesn't
    match `types.rs:84-123` as built.
12. **CLI operational posture:** seed stored plaintext in RocksDB with default umask, and the
    CLI has no export/restore verb — ADR-0003/ADR-0011 DO specify seed export for the product
    (Android), but wallet-cli (ADR-0023: a maintained-forever frontend) never grew the command
    (it needs at least a stated dev-grade posture + `chmod 0700` + the export verb);
    relative default `--data-dir` silently mints a fresh wallet per cwd; default fee caps are
    `amount + 1000 sat` (~101× a 10-sat move). Also: the per-fed cap number
    (`DEFAULT_PER_FED_CAP = 5M sats`) is documented only as a code comment (`tick.rs:40-42`,
    "well above the two targets") — no operator-facing policy doc records it, it is ~100× the
    roadmap's illustrative balances, and "well above the targets" sits oddly against ADR-0018's
    "hard, LOW cap" posture. Needs a policy note reconciling the two.
13. **(debt) Missing adversarial harness:** phase-1 exit-gate cases (d) restore-from-seed and
    (e) misbehaving-gateway were never run. The obligation is still on the books — the 1c gate
    text lists both (`integration-phase-plan.md:108-110`) — but Phase 1 was declared complete
    without them and no ACTIVE phase plan (4/5) re-runs them. The gateway-took-the-money-and-
    never-funded double is exactly what Phase 4's Stranded design exists for; put it in Phase 4's
    exit gate where the handling code lands.

---

## P3 — worthwhile, not urgent

- **Seams gated on `debug_assertions`, not a feature:** `WALLET_CLI_FORCE_SHUTDOWN` /
  `WALLET_CLI_CRASH_AT` are compiled out of `--release` (verified), but any dev-profile binary
  reaching real funds re-arms them. A dedicated non-default cargo feature is sturdier.
- **`pending_lnv2_balances` pages the full op-log every probe** for fields nothing reads
  (`probe.rs:524-571`). Drop or defer until a consumer exists.
- **Serial probing, no per-probe timeout** (`runtime.rs:786-801`); a slow `meta_override_url` adds
  up to tens of seconds per fed, serialized. Probe concurrently with a bound.
- **`NumPeers::from(0).threshold()` underflows** (`probe.rs:273`) — unreachable for a validly
  opened fed; guard anyway.
- **Misleading unsolvable-fee error** (`executor.rs:948-954`) names ppm≥100% for both `None`
  causes of `gross_up`.
- **DirectInflow hair-under records the ASKED amount in the op's `MoveMeta`**
  (`executor.rs:702-706` adjusts only when `send_required`) — nothing reads it today, but the field
  is documented as the honest crash-safe amount. Commit `delivered` unconditionally.
- **Sentinel gateway string persisted as a real `GatewayUrl`** (`executor.rs:1045-1047`);
  an `Option`/enum would make no-gateway unrepresentable.
- **Bisection nits** (`executor.rs:311-338`): the `hi <= lo && safe_under == None` branch is
  unreachable; the comment claims the "best" under candidate is kept but `safe_under` keeps the
  *last* (safety unaffected).
- **"a re-`pay` dedups" comment overstates the SDK** (`executor.rs:15, 820-822`): a *refunded*
  attempt re-pays fresh (`payment_attempt + 1`); the real double-pay guarantee is the same-store
  exhaustive backfill — say so.
- **Corrupt intent row is unrepairable through `upsert`** (`journal.rs:359-360`) — decode of the
  old row gates the overwrite; scans skip it, but repair requires raw DB surgery.
- **Corrupt seed row aborts with a MISLEADING error.** `load_or_generate_mnemonic`
  (`main.rs:841-848`) treats load-`Err` as absent (`if let Ok`), but the SDK errors both for an
  absent row AND for an undecodable `EncodedClientSecretKey`; on a corrupt row the CLI generates a
  fresh mnemonic, then aborts on the SDK's refuse-overwrite guard
  (`fedimint-client/src/client.rs:398-401` — so no silent seed replacement is possible, verified)
  with "client secret already exists", pointing the operator away from the real problem. Use
  `load_decodable_client_secret_opt` to distinguish absent (generate) from corrupt (abort naming
  the decode failure).
- **Gateway repin impossible in the `Created`-phase crash window** (`executor.rs:1026-1028`):
  cache-with-no-invoice provably has no side effects, yet the cached gateway still wins over the
  operator's `--gateway`. Alternative: carry the SDK-committed `ReceiveOperationMeta.gateway` on
  `OpArtifact` and drop the pre-op record entirely.
- **`drive` commits outcomes with unconditional `set_status`, not CAS-from-`Executing`**
  (`wallet-core/src/executor.rs:488-528`). Not reachable as a race in the shipped CLI — the
  RocksDB store is opened under an exclusive `*.db.lock` (`fedimint-db-locked/src/lib.rs:27-46`),
  so a second process blocks at open, and the in-process `InFlightPerform` guard covers same-store
  handles. Still worth `set_status_if(Executing, …)` on the four result paths: the state machine's
  soundness is currently a distributed property of every caller being careful rather than a local
  invariant.
- **`fund_into`'s self-fund early-return suppresses the OverCap/liquidity advisories**
  (`allocator.rs:112-114`).
- **Dead surface (greenfield YAGNI):** `Action::Cap` has no producer; `requires_auth` is hardcoded
  false with no reader; `Journal::failed()` has no production caller; `AllocatorSnapshot.now` is
  never read; `Intent.max_fee` duplicates `action.fee_cap()`. (Phase 4 §5 already plans dropping
  the first two — do the rest there too.)
- **Scorer nits:** threshold failure reported as `TooFewGuardians`; `threshold` trusted unbounded
  in `rank()`; ≥4s latency saturates ranks to 0 and degrades the tie-break to raw balance.
- **`ExecutionSummary.failed` conflates** retryable-left-Pending / genuinely-Failed /
  performed-but-status-write-failed; add a `retryable` counter.
- **Smoke hardening:** the money smoke never bounds the PAY debit (a 4× over-debit passes);
  `await-send … || true` + no `timeout` wrappers can hang ~1h at invoice expiry; two single-fed
  smokes retain the awk-`exit` SIGPIPE hazard the two-fed smokes fixed.
- **Doc nits:** `consensus_version` promised in integration-phase-plan §176, never added; phase-2's
  "small config file" never materialized; stale line anchors and two placeholder types in
  phase4-implementation-spec (§7 `kind_from_action(…, rec_ops: ...)`, `advance(…, ops)` untyped);
  unexplained "D6" in roadmap; `SHUTDOWN_EVACUATION_LEAD_SECS = 86_400` and
  `FED_FEE_REQUOTE_PASSES = 3` exist only in code; TODOS SIGPIPE item already fixed in `3859425`;
  integration-plan test pyramid still says "real SQLite".

---

## What checked out (verified, not assumed)

- **Fee-model exactness vs the SDK:** `GatewayFee::on` floors identically to
  `PaymentFee::absolute_fee`; federation fee quoted at the SOLVED contract matches the real claim
  path; send-side `invoice + on(invoice)` equals `send_fee.add_to`. `fee::gross_up` terminates for
  every input and is minimal-invoice/exact-net for constant fees; `net=0/1`, `u64::MAX` edges hold.
- **Never-over discipline within its model:** every exit of the sizing loop is verified at its own
  contract or passes the final gate; no quote stream (including adversarial non-monotone ones) got
  an over-netting invoice past it — the only hole is the gateway-fee TOCTOU (P1-6), outside the
  loop's model.
- **Crash-safety:** all four killpoints resume correctly; journal and client op-logs share ONE
  `Database`, so "op committed but cache lost" cannot arise; backfill-to-exhaustion cannot miss a
  committed op; adjust-after-commit amount plumbing keeps the Pay-step cap input honest across
  every crash window tried.
- **Journal atomicity:** intent + index move in one dbtx everywhere; real CAS via autocommit; scans
  are snapshot-consistent with a status re-check; poison rows skipped on scans, surfaced on reads.
- **Trust plumbing that IS right:** f+1 corroboration on `/status` (exact, early-exit, deduped,
  transport-errors-as-no-signal); `session_count` liveness via 2f+1 `ThresholdConsensus`; backwards
  clock disables rather than forces evacuation; fee caps enforced at every leg; idempotency keys
  deterministic and param-sensitive both ways; terminal-replay guard; op-kind guards on typed
  awaits.
- **CLI stdout/stderr contract** (payables on stdout, diagnostics on stderr, non-zero exit on every
  non-settled money op) is exemplary and tested; smoke assertions are money-grade (never-over hard
  bounds, crash gate requires signal-death, evacuate smoke asserts the exact cap_room amount).
- **Strong docs:** fedimint-mechanics (SDK-grounded, self-correcting), federation-data-sources
  (claims tested against live data), ADR hygiene (status headers, supersession chains), the crash-
  window taxonomy in phase1 §5 and repair defeasibility in phase4 §10.3.

## Priority order

1. P1-1 expiry-signal trust (security; wrong AND blocked evacuations)
2. P1-4 deterministic-send-rejection wedge + P2-2 Pay-step Permanent + P2-3 gateway selection
   (evacuation/route liveness as one bundle)
3. P1-2 + P1-3 allocator cap/eligibility (one change-set: inbound ledger + perform-time
   destination check + fundability in status)
4. P1-6 TOCTOU contract verification + P2-4 solve-loop extraction/tests (never-over lock-in as
   one bundle)
5. P1-5 + P2-10/11 spec repairs (cheap, unblocks Phase 4 implementation)
6. P2-1 tick deadline, P2-5 delivered read-back
7. The rest as Phase 4/5 backlog items
