# Phase 5 plan — the real active probe, discovery, and the self-running loop

> **STATUS: Phase 5.0 (the active probe) COMPLETE (2026-07-07).** Implemented across an
> rb-lite run (died on a transient API overload; finished by hand) + 8 codex verification
> passes (~24 findings, 2 rejected with evidence as v1-unreachable concurrency) + the live
> `smoke_probe_devimint.sh` exit gate, which caught two composition bugs the unit tests
> could not (a missing `--gateway` route pin; a zero-fee-headroom leg-OUT defer) — both
> fixed. The gate PASSED: a real A→B→A round trip proving redeemability (B nets its delta
> then drains back, combined loss fees-only), both legs + the umbrella row auditable as
> `active_probe`, a sustained window flipping the verb's verdict to `passed` while `status`
> stays conservative, and an unjoined-fed probe as a non-zero `NoAttempt` that never
> demotes. NEXT: 5.1 discovery (wire the gate for `Discovered` feds; settle the auto-join
> lifetime bound). 5.0 gates nothing yet by design.


The re-scoped 3.B + 3.C ([roadmap-to-v1.md](./roadmap-to-v1.md)): turn the engine from
"rebalances the feds the user joined" into "discovers, vets, and manages federations
unattended". **5.0 (the active probe) is fully specified here and BLOCKS the rest** —
ADR-0017's trust gate for funding a discovered federation is an empirical, sats-spending
probe, and today's `round_trip_ok` is a free proxy (gateway availability), fine for
user-joined feds only. 5.1/5.2 are plan-level below; their buildable specs follow after
5.0 lands.

Grounding: [ADR-0017](./adr/0017-sybil-resistant-selection-probes-gate.md) (probes GATE,
for THIS device, over a SUSTAINED window; reputation only demotes),
[ADR-0019/0020](./adr/0019-federation-signals-trust-model.md) (discovery inputs are
untrusted), [federation-data-sources-spec.md](./federation-data-sources-spec.md) (the
probe set; Nostr = discovery only), and the pinned SDK — the Cargo pin
`douglaz/fedimint @ b108ec66ab…` (Cargo.lock is authoritative; the local `~/p/fedimint`
checkout sits AHEAD of the pin — docs commits + the two-fed harness patch — so verify
line-precise claims with `git show b108ec6:<path>`; every SDK constant cited below was
verified at the pin that way).

**Greenfield note.** Pre-release, no persisted data, no external users: NO backwards
compatibility, NO migration shims, NO serde compat layers. Shape changes replace outright.

---

## 5.0 — the ACTIVE probe (buildable spec)

### 5.0.1 What a probe IS: a two-leg, exact-net round trip on the real money path

A probe of candidate federation `C` from spending federation `S`:

1. **Leg IN — mint on the candidate:** a normal `Move(S → C, PROBE_AMOUNT)` through a
   shared gateway — the SAME validated two-leg machinery every move uses (exact-net
   gross-up, never-over verification, route preflight, crash-safe resume, ledger row).
   Proves: quorum accepts consensus items, lnv2 mints, a gateway routes to `C`, the
   claim settles — ecash actually lands.
2. **Leg OUT — redeem back:** `Move(C → S, out_net)` where `out_net` is sized by the
   evacuation-affordability search (the `size_fresh_evacuation` sizing path, reused
   without the shutdown reason) run with the sizing BUDGET set to **leg IN's delivered
   net** — NOT the candidate's live balance. The probe must round-trip ITS OWN delta
   only: a candidate that already holds user funds (a user-joined, user-funded fed —
   legitimate in 5.1) must never have unrelated balance swept back to `S` by a probe.
   `out_net + out fees ≤ delivered_in` by construction (the search budget), so
   pre-existing funds are untouched and the residual on `C` = `delivered_in − out_net −
   fees`, bounded by fees + sizing hair (< the leg's fee cap), stays OUR ecash, and is
   counted in `balance`. **This isolation rests on the wallet's SINGLE-WRITER
   architecture, stated explicitly:** the store is opened under an exclusive `db.lock`
   (a second process blocks before it can act) and the probe verb runs both legs
   SYNCHRONOUSLY in one process, so nothing else can spend from `C` between the legs
   in v1 — ecash is not segregated per-probe, and WITHOUT that serialization a
   concurrent spend from an already-funded `C` could consume the delta and leave leg
   OUT redeeming pre-existing funds. Phase 6's long-running app introduces exactly
   that concurrency: the probe MUST be revisited there (a per-probe reservation, or
   serializing probes against other `C`-spending ops) — flagged as a Phase-6
   precondition, not silently assumed away. (Leg IN's delivered net is durable — the move's `MoveMeta.
   amount` / ledger row — so the budget survives a crash.) Proves **REDEEMABILITY** —
   the actual risk a metadata-only score can never see: a federation that mints but
   will not redeem.

Both legs are ordinary intents: journaled, idempotent, resumable by `reconcile`, and
recorded in the operation ledger — a probe is user-auditable history, not a side channel.

**Failure taxonomy:** ANY failed leg ⇒ the probe FAILED; there are no partial passes.
Whether the failure DEMOTES the candidate is a separate, attribution-scoped question —
see 5.0.3's scoping rule: candidate-refused mint (leg IN) and candidate-refused pay
(leg OUT — the damning REDEEMABILITY failure, bounded loss = what leg IN landed) demote;
source, gateway, and ambiguous faults (incl. a `Stranded` leg — the §3 machinery already
preserves the preimage) are umbrella-row history only.

### 5.0.2 Amounts, fees, cost (SDK-floored)

- lnv2 refuses receive CONTRACTS below `MINIMUM_INCOMING_CONTRACT_AMOUNT` = **5 sats**
  (`fedimint-lnv2-common/src/lib.rs:52` at the pin; re-exported as our
  `MINIMUM_INCOMING_CONTRACT_MSAT`). PRECISION: the floor applies to the CONTRACT
  amount (invoice minus the gateway's cut), NOT the recipient's net — our executor's
  `ensure_minimum_incoming_contract(net, contract)` checks the contract, and a 4-sat
  net on a 5-sat contract is explicitly fine (there is a test asserting it). Both
  legs' contracts must clear the floor; nets may sit below it.
- `PROBE_AMOUNT_MSAT = 20_000` (20 sats) default: leg OUT then redeems ~13-17 sats after
  observed devimint/mainnet-cluster fees (gateway base 2-2.5 sats + ppm + fed tx fees),
  comfortably above the 5-sat floor. `PROBE_LEG_FEE_CAP_MSAT = 10_000` (10 sats) per leg.
- **Fee-jitter margin (found by the live smoke):** leg OUT is sized against
  `delivered_in − PROBE_FEE_MARGIN_MSAT` (default 1000 msat), NOT the full delivered net.
  The return leg's fee cap is bounded tight by the no-sweep budget (`delivered_in −
  out_net`), so with no margin a small upward fee re-quote at the Pay step (observed live:
  8432 actual vs 8417 estimate) breaches the cap and defers the whole probe. The margin
  lands as bounded extra candidate residue (accepted, §5.0.9 decision 6), far below the leg
  fee cap, and keeps a normal probe from deferring on ordinary fee jitter.
- Worst-case cost per attempt ≈ both legs' fees ≤ 20 sats, typical ~4-8 sats; worst-case
  LOSS on a hostile candidate = `PROBE_AMOUNT + leg fees` (leg OUT never redeems),
  bounded ≈ 30 sats. Both are `ProbePolicy` fields (5.0.3) with these defaults. There
  is NO amount hard-reject: the floor binds the CONTRACT, and the contract sits above
  the net by the fed fee, so even a sub-5-sat net can be valid (the repo tests exactly
  that boundary). Feasibility on BOTH legs is EMPIRICAL (fees are unknown until
  quoted), so nothing is pre-clamped — the defaults satisfy `amount ≥ floor + leg_fee_cap` (comfortably
  feasible), and an override that turns out infeasible is caught where the machinery
  actually knows: leg IN's quote/min-contract preflight refusing = LOCAL parameter
  error (umbrella row Failed, NO attempt — a parametric refusal must not demote the
  candidate); an infeasible leg OUT is caught by the post-IN re-check (5.0.5 step 4),
  same classification. (Earlier drafts pre-rejected via net-based clamps; both were
  stricter than the SDK and are deliberately gone.)
- Sybil economics (ADR-0017): probing SPENDS our sats, so a Sybil farm's attack is
  making the wallet burn probe fees. Mitigations: probes run only against candidates
  that already passed the FREE tiers (the scorer's structural floor + passive
  liveness/gateway probe); each probe is a visible ledger row; and 5.2's loop enforces
  a global probe budget (5.0's manual verb is user-initiated = the user's own choice).

### 5.0.3 The verdict: sustained-window pass, cached with expiry (pure)

`wallet-core` gains a pure verdict function (golden-tested) over a bounded attempt
history:

```rust
pub struct ProbeAttempt {
    pub at_ms: u64,
    pub ok: bool,
    /// The spending federation the attempt probed FROM (forensics; the verdict itself is
    /// source-agnostic — see the scoping rule below the verdict rules).
    pub from: FederationId,
    /// The attempt's money parameters, recorded so the verdict can refuse to count a
    /// dust-sized success toward the trust gate (see the qualifying rule below) — CLI
    /// overrides must not silently weaken what `Passed` means.
    pub amount_msat: u64,
    pub leg_fee_cap_msat: u64,
    /// The failed leg + verbatim error for a failed attempt (`None` on success) — the
    /// same text as the failing move's ledger row and the umbrella `Probe` row. NOTE:
    /// only leg failures produce attempts at all; preflight failures (no shared route,
    /// local faults) live solely on the umbrella row per 5.0.3's scoping rule.
    pub error: Option<String>,
}
pub struct ProbePolicy {
    // -- runtime knobs (the money side) --
    pub amount_msat: u64,          // default 20_000
    pub leg_fee_cap_msat: u64,     // default 10_000 (per leg)
    // -- verdict knobs (read by the PURE verdict fn) --
    pub min_successes: u32,        // default 3
    pub min_span_ms: u64,          // default 24h — successes must SPAN this (ADR-0017 "sustained")
    pub ttl_ms: u64,               // default 7d — the NEWEST success must be younger than this
}
pub enum ActiveProbeVerdict {
    Passed,
    NeverProbed,
    Insufficient,          // successes so far, but not yet count+span
    Expired,               // a pass existed; its newest success is now older than ttl
    Failed,                // newest in-window attempt is a candidate failure, no prior pass
    FailedSinceLastPass,   // a qualifying pass existed, then a failure demoted it
}

pub fn probe_verdict(attempts: &[ProbeAttempt], source: FederationId, now_ms: u64,
                     policy: &ProbePolicy) -> ActiveProbeVerdict
```

Rules (each a golden). The verdict evaluates a WINDOW: attempts older than `ttl_ms`
are ignored entirely (ADR-0017's "sustained" pass is RECENT evidence — and this aligns
the verdict exactly with 5.0.4's retention, so pruning can never change a verdict).
Within the window, only the CONTIGUOUS SUCCESS SUFFIX counts — the successes strictly
AFTER the most recent failure (any failure discards everything before it from
consideration; "a fresh sustained window rebuilds" is literal) — and within that suffix
only QUALIFYING successes count: `attempt.from == source AND attempt.amount_msat ≥
policy.amount_msat AND attempt.leg_fee_cap_msat ≤ policy.leg_fee_cap_msat`. The SOURCE
condition makes a pass PAIR-PROVEN: a success proves mint+redeem+route for the probing
source only, and gating funding from `B` on an `A→C` pass would send the allocator into
moves whose route preflight can never succeed (routing is a (source, candidate)
property in this engine — and pair-scoped route failures are deliberately excluded from
history, so a stale source-agnostic `Passed` could never self-clear). Failures still
count REGARDLESS of source: candidate dishonesty generalizes; routability does not.
`Passed` iff, over the qualifying suffix: (a) it holds
≥ `min_successes` successes, (b) its oldest and newest span ≥ `min_span_ms`, and (c) its
newest is younger than `ttl_ms` (implied by the window). So
`success, failure, success×3` passes only when the LAST three alone satisfy count+span.
When the suffix is empty because the newest in-window attempt is a failure: with a
prior qualifying pass in evidence = `FailedSinceLastPass` (immediate demotion);
WITHOUT one = `Failed` — a first-ever failing candidate must be distinguishable from
one that merely has not accumulated successes yet (`Insufficient`), or the negative
signal the scheduler/UI preserve would vanish into "keep probing". Empty window with a
retained stale success = `Expired` (a pass existed; it aged out); empty history
entirely = `NeverProbed`; suffix too short/narrow = `Insufficient`. Only `Passed` ever
gates IN.

**Scoping rule — the verdict measures the CANDIDATE's honesty; only
candidate-ATTRIBUTABLE outcomes enter the history.** A SUCCESS (both legs settled)
enters the history and counts toward `Passed` for ITS OWN source (pair-proven — see
the qualifying rule above). A FAILURE becomes a demoting attempt ONLY
when the candidate itself refused: leg IN's invoice-mint/claim refused ON `C`
(`CreateInvoice`-on-C failure, C's contract never claimable), or leg OUT's pay refused
BY `C` (a classified send rejection from C — the redeemability core). Everything else
is recorded on the umbrella row ONLY (verbatim, source named) and writes NO attempt:
NO-SHARED-ROUTE preflight failures (a (source, candidate) pair property — must not
demote C for sources that can reach it; per-move route preflights own reachability at
funding time), SOURCE-side failures (S refusing leg OUT's mint or leg IN's pay is S's
fault), and GATEWAY/AMBIGUOUS faults (a `Stranded` leg — send settled, receive never
credited — cannot distinguish a thieving gateway from a broken candidate, so it must
not demote). Safety is preserved without demotion because NO-ATTEMPT ≠ PASS: a probe
that failed for any reason yields no success either, so the candidate simply does not
progress toward `Passed`. The runtime classifies from what the move machinery already
exposes (the failing step + the Phase-4 error taxonomy: classified send rejections,
`Stranded`, expiry); when attribution is genuinely unclear, the fault is AMBIGUOUS and
does not demote.

### 5.0.4 Durable probe state (journal tag `0x08`)

- `0x08 ++ fed_id` → JSON v1 `ProbeRecord { attempts: Vec<ProbeAttempt>,
  in_flight: Option<ProbeSession> }` — retention is TIME-AWARE, not count-only (a
  count-only cap could truncate the very successes the 24h `min_span` needs whenever
  probes run more often than span/cap): keep every attempt younger than the DEFAULT
  `ttl_ms` — which is exactly the verdict's PASS-evaluation window (5.0.3), so pruning
  cannot flip a pass — PLUS always retain the newest SUCCESS and the newest attempt
  regardless of age (one row each: the evidence that distinguishes `Expired` — "a pass
  existed and went stale" — from `NeverProbed` after a long quiet spell), up to a hard
  bound of `PROBE_HISTORY_CAP = 256` newest. The count bound is a backstop, not a
  guarantee: at the scheduler's few-probes-per-day cadence it holds years of history,
  but a script hammering `probe` once a minute retains only ~4 hours and can truncate
  a real 24h span — self-inflicted; the scheduler never probes near that rate. The
  ledger keeps the full narrative regardless. One row per fed, upserted in its own
  dbtx.
- `ProbeSession { nonce: String /* 32 hex chars */, from: FederationId /* the probe's
  source — resolved per 5.0.7 and fixed for the session */,
  amount_msat: u64, leg_fee_cap_msat: u64,
  c_spendable_before_in_msat: u64 /* the candidate's balance BEFORE leg IN — the
  no-sweep baseline */, out_net_msat: Option<u64>,
  started_at_ms: u64 }` — the durable probe IDENTITY, written BEFORE leg IN is
  journaled. A `move:` intent key is deterministic from
  `(from, to, amount, fee_cap, occurrence = nonce-derived u64)`, so leg IN's key is
  reconstructible from the session alone; leg OUT's amount is SIZED at runtime, so the
  session is UPDATED with `out_net_msat` after sizing and BEFORE leg OUT is journaled —
  after that write both keys are reconstructible. Resume disambiguation is total:
  `out_net_msat: None` + `journal.get(in_key)` is `None` ⇒ the crash hit before leg IN
  was journaled — NOTHING has moved, so this is still a FRESH probe in every sense
  that matters: re-enter the fresh path AT THE PREFLIGHT (reusing the session and its
  umbrella row — never minting new ones) rather than starting leg IN blind; the world
  may have changed since the session was written (the candidate could now be at cap,
  the route gone), and skipping the preflight would convert what should be an
  umbrella-only local refusal into an in-leg failure that could demote an honest
  federation; `out_net_msat: None` + leg IN journaled ⇒
  drive/settle leg IN, then size, persist, proceed; `out_net_msat: Some(n)` + `journal.get(out_key)` is `None` ⇒ sized
  but the intent was never journaled (the crash window between the session update and
  `do_move`) — RE-CHECK the no-sweep precondition first: leg OUT may start (with
  EXACTLY the persisted `n`; never re-size) only while
  `C.spendable ≥ c_spendable_before_in + delivered_in` — the session's pre-probe
  BASELINE plus the delta. (Checking against the delta alone is fooled by pre-existing
  funds: C held 100, delta 20, user spends 15 → spendable 105 still exceeds 20 while a
  third of the delta is gone.) Ecash is fungible, so with baseline + delta intact,
  drawing `n ≤ delivered_in` provably cannot touch pre-existing funds; anything below
  the threshold means redeeming could sweep funds
  that are not the probe's — ABORT as INCONCLUSIVE instead via
  `record_probe_outcome(fed, None, …, Failed("probe delta consumed before redemption;
  inconclusive"))` — session cleared atomically, NO attempt, no demotion. No such guard is needed before DRIVING an already-journaled leg OUT —
  once the out intent exists, the money path owns it like any other move.
  `Some(intent)` ⇒ drive it. The session is cleared in the
  same atomic write that records the finished attempt.
- `FedimintJournal::{record_probe_outcome(fed, attempt: Option<ProbeAttempt>,
  umbrella_key, status, error), begin_probe_session(fed, session), probe_record(fed)}`.
  `probe_record` is a TARGETED getter and FAILS CLOSED on an undecodable row (like
  `get`/`get_move`/`operation`) — it decides whether a session is in flight, and a
  swallowed corrupt row would restart a probe that is already live, spending twice.
  Only SCANS are poison-tolerant. `record_probe_outcome` is the ONE terminal write for
  every exit after a session exists, in ONE dbtx: clear `in_flight`, terminalize the
  umbrella `probe:` ledger row, and append the attempt when `attempt` is `Some` (leg
  outcomes) — `None` for the no-attempt terminal exits (the post-IN feasibility abort
  and the inconclusive no-sweep abort ALSO clear their session this way; a stale
  session must never survive a terminal exit, or later probes would treat an
  already-terminal failure as crash recovery). All parts commit or fail together (the
  same discipline as the intent/ledger integration), so the verdict history, the
  session, and `history`'s umbrella row can never disagree.
- The attempt is recorded AFTER the round trip resolves either way (both legs settled,
  or a leg terminally failed). A crash mid-probe leaves the legs' own intents to
  `reconcile` (they self-resume like any move); the next `probe` invocation finds the
  `in_flight` session, reconstructs the two keys, drives any non-terminal leg, then
  records the attempt and clears the session — see 5.0.5.

### 5.0.5 Runtime verb + keys + ledger

`Runtime::active_probe(candidate, from, policy) -> ProbeReport` (`from` = the resolved
source federation, per 5.0.7):

0. **Resume FIRST — before any fresh-probe work.** Read `probe_record(candidate)`; if
   `in_flight` exists, this invocation IS the crash recovery: reconstruct the leg keys,
   drive per 5.0.4's disambiguation, record the outcome (which clears the session), and
   RETURN. The fresh-probe preflight below must NOT run for a resume — leg IN may
   already have debited `from` and credited `C`, so fresh-probe balance/cap checks no
   longer hold and would misclassify a recoverable probe as a new local error — and no
   NEW umbrella row is created: the resumed attempt terminalizes its ORIGINAL row (the
   umbrella key is `probe:<fed>:<nonce>` and the nonce lives in the session).
1. **Session, then umbrella, then preflight (fresh probes only):** the fresh path
   opens by writing the SESSION (nonce chosen; `c_spendable_before_in` sampled) and
   THEN `record_started` on the umbrella `probe:` row — session-first, because step 0
   can only resume what a session names: a crash between the two leaves a session
   whose umbrella row does not exist yet, and `record_probe_outcome`'s ledger write is
   create-or-advance (the Phase-4 helper creates an absent row), so the resumed
   outcome still lands as history; the opposite order would strand a permanent
   `Started` row no resume could ever find. Then the preflight: candidate is joined +
   open (else a clean diagnostic); candidate ≠ `from`; `from` holds ≥ `amount + leg
   fee cap`; the CANDIDATE has ADR-0018 cap room ≥ `amount` (the source needs none —
   see the cap note below); the existing move-route preflight validates a shared
   gateway serves both directions. A NO-SHARED-ROUTE failure exits via
   `record_probe_outcome(fed, None, …, Failed(<verbatim route error>))` — pair
   reachability, not candidate honesty (5.0.3's scoping rule). LOCAL faults
   (insufficient balance, infeasible policy, insufficient candidate cap room, not
   joined) exit the SAME way with their diagnostic — a failed `probe` invocation must
   never be invisible in `history` (the Phase 4 auditability contract), and every
   terminal exit after the session exists clears it atomically — while writing no
   attempt (no demotion either way).

   **ADR-0018 cap interplay (explicit):** probe legs do NOT bypass the hard per-fed cap
   — the executor's perform-time enforcement and the evacuation-sizing clamp apply
   verbatim. The preflight requires cap room ≥ `amount` on the CANDIDATE only;
   insufficient candidate room is a LOCAL condition (umbrella row Failed, no attempt,
   no demotion) — a fed sitting near its cap is not a dishonest fed. The SOURCE needs a
   preflight `spendable ≤ per_fed_cap` check: leg OUT mints BACK into `from`, running the
   same perform-time cap enforcement. Leg IN first debits `from` by `amount + fees` and
   leg OUT credits back strictly less, so a source that starts AT-OR-BELOW the cap ends
   below it and never breaches — but a source ALREADY ABOVE the cap (a transient inbound)
   would spend leg IN and then fail leg OUT umbrella-only with "destination would exceed
   the per-fed cap", a GUARANTEED inconclusive spend. Refuse an over-cap source as a
   LOCAL fault before any money moves. (An earlier draft claimed the source needed no
   check — true only while `from ≤ cap`, which this preflight now enforces.)
2. *(Session already written in step 1 — fresh probes reach here with a durable
   identity.)*
3. **Leg IN** = `do_move(from → C, amount, leg_fee_cap, occurrence = probe nonce)`.
4. **Post-IN feasibility re-check:** the sizing search runs with budget = leg IN's
   DELIVERED net (which may sit a verified hair under `amount`). If no out move whose
   CONTRACT clears the 5-sat floor is affordable within the leg fee cap from that
   budget (the floor binds the contract, not the net — 5.0.2), this is a LOCAL
   PARAMETER/FEE-ENVIRONMENT error, NOT a redeemability failure: abort via
   `record_probe_outcome(fed, None, …, Failed(<diagnostic naming the delivered amount
   and the shortfall>))` — session cleared, umbrella terminal, NO attempt (no
   demotion). (Under the 5.0.2
   DEFAULTS this branch is nearly unreachable — delivered hair-under is msats — but
   overrides can reach it and it must be defined, not undefined.)
5. **Leg OUT** = size with budget = delivered-in, persist `out_net_msat`, then
   `do_move(C → from, out_net, leg_fee_cap, same nonce)`.
6. Record the outcome via `record_probe_outcome` (5.0.4 — attempt `Some`, session cleared) + return a `ProbeReport { verdict_before, attempt,
   verdict_after, in_key, out_key }`.

Keys/ledger: every ATTEMPT gets an umbrella ledger row —
`OperationKind::Probe { fed, from, amount_msat, cost_msat: Option<u64> }` under key
`probe:<fed-hex>:<nonce>`, `record_started` when the invocation begins, terminalized
Succeeded/`Failed(<verbatim error>)` when the attempt records. `from` makes pair-scoped
failures (no shared route — NO move intent ever exists) name their source in
`history`/`show` and keeps `history --fed <source>` complete; `cost_msat` is filled at
terminalization with the wallet's NET OUTFLOW FROM `S` — total S debit for leg IN minus
total S credit from leg OUT (`None` when no money moved). On a clean pass that equals
fees + the small residue; on a hostile/failed probe where leg OUT never redeems it
equals fees + the WHOLE delivered amount — the honest exposure number (fees-only would
let an unattended scheduler burn `amount_msat` per hostile candidate while "within
budget"). 5.2's sats/week budget sums exactly this field, and attempts/week counts the
rows — one row kind, no re-correlation of the move rows. The
two moves additionally carry their ordinary `move:` intent keys (occurrence = the probe
nonce-derived u64, so probes never collide with user moves); all three rows carry
`reason: ReasonCode::ActiveProbe` (new variant + `reason_tag "active_probe"`) — note
`Runtime::do_move` currently hardcodes `ReasonCode::UserInitiated`, so it gains a
reason/actor parameter (or an internal `do_move_with_provenance`) that the probe verb
threads through — with `actor` = whoever initiated (`User` for the CLI verb;
`Agent { occurrence }` when 5.2's loop schedules it). `history` therefore shows every
probe as one umbrella row plus up to two explained moves.

### 5.0.6 Scorer/status surfacing (gating wire-up lands in 5.1)

- `FederationFacts` gains `active_probe: Option<ActiveProbeVerdict>` (`None` = never
  probed — never a rejection by itself in 5.0). The verdict is SOURCE-RELATIVE: the
  assembler evaluates it against the snapshot's designated SPENDING fed (the fed that
  would fund the candidate — exactly the pair 5.1's gate must trust).
- The tick/status assembler fills it from `probe_record` + `probe_verdict(…, spending)`.
- `wallet-cli status` prints the verdict per fed (`active_probe=passed|never|expired|…`).
- **5.0 does NOT change fundability:** user-joined feds keep today's behavior (the
  roadmap's explicit stance — the cheap proxy is fine while the wallet only rebalances
  feds the USER joined). The `Discovered`-fed gate that REQUIRES `Passed` is 5.1's
  wire-up, one `if` on a field that 5.0 already computes. This keeps 5.0 shippable
  without discovery and keeps the gate's semantics testable purely.

### 5.0.7 CLI

```
wallet-cli probe <fed-hex> [--from <spending-fed-hex>] [--gateway URL]
                 [--amount MSAT] [--fee-cap MSAT-per-leg]
                 [--min-successes N] [--min-span-secs S] [--ttl-secs S]
```
`--gateway` pins the shared lnv2 gateway routing both legs (it must serve BOTH `S` and the
candidate); omitted, the route resolves from each fed's registered gateways — required
explicitly against devimint, whose LDK gateway is not auto-registered into the lnv2 set
(like every other money verb). `--from` names the spending federation `S` explicitly. When omitted: exactly TWO joined
feds ⇒ `S` = the other one (the common probe topology); otherwise the verb refuses with
"pass --from" — deterministic, and deliberately NOT coupled to the tick's designation
logic (a probe must not silently ride whatever auto-designation picked this run).
The five flags override the five `ProbePolicy` fields (defaults per 5.0.2/5.0.3). The
verdict flags exist so the smoke can SHRINK the window and are clamped SHRINK-ONLY:
`--ttl-secs` and `--min-span-secs` above their defaults are REJECTED with a diagnostic
naming the retention rule (5.0.4 retains sub-default-`ttl` attempts only, so a larger
window could not be computed from the durable history it advertises). Production
callers use the defaults, and `status` computes its verdict column with the DEFAULT
policy (the policy is not persisted; it parameterizes a pure function over durable
attempts).
Runs one attempt synchronously; prints `attempt: ok|failed <leg+error>` and
`verdict: <verdict>` to stdout (scriptable), keys/diagnostics to stderr; exits non-zero
on a failed attempt (a probe IS a money op). `status` gains the per-fed verdict column.

### 5.0.8 Tests / exit gate

- **Pure goldens:** the full `probe_verdict` table — never/insufficient-count/
  insufficient-span/expired/failed-since-pass/passed; boundary cases (exactly
  `min_successes`, exactly `ttl`); the WINDOW rule (a success just past `ttl` is
  invisible: successes at 8d/2d/1h with a 7d ttl are `Insufficient`, not `Passed`);
  SOURCE scoping (an `A→C` pass gates `C` for `A` only — evaluated for `B` the same
  history is `Insufficient`; a candidate-fault failure recorded from ANY source demotes
  the verdict for ALL sources); a FIRST-EVER candidate failure is `Failed`, not
  `Insufficient` (the negative signal survives); the suffix rule specifically:
  `success, failure, success×3` passes iff the last three alone satisfy count+span
  (pre-failure successes never count); a trailing failure after a qualifying pass is
  `FailedSinceLastPass`; a NON-QUALIFYING success (amount below the policy's, or fee cap
  above it) never counts toward `Passed` but its failure still demotes.
- **Journal (MemDatabase):** retention keeps sub-`ttl` attempts plus the newest
  success/attempt regardless of age, and enforces the 256 hard bound; a
  stale-pass-then-silence history still reads `Expired`, never `NeverProbed`;
  `probe_record` FAILS
  CLOSED on a corrupt row (never "no session" from garbage); session lifecycle (begin
  writes `in_flight`; `record_probe_attempt` clears it + appends the attempt +
  terminalizes the umbrella row in ONE dbtx — crash between them impossible by
  construction); crash-window repair (in_flight session + terminal legs → attempt
  written + session cleared on the next probe).
- **Runtime unit:** preflight refusals — LOCAL faults (not joined, self-probe,
  insufficient balance, insufficient candidate cap room) terminalize the umbrella row
  `Failed` with the diagnostic and write NO attempt; no-shared-route does the same with
  the route error (also no attempt — the verdict history is untouched either way, per
  5.0.3's scoping rule); FAULT ATTRIBUTION: a candidate-refused mint (leg IN) and a
  candidate-refused pay (leg OUT) each write a demoting attempt, while a source-side
  failure, a `Stranded` leg, and an ambiguous error write umbrella-only outcomes (no
  demotion — asserted against the verdict); resume drives non-terminal legs across
  every session state (session-only pre-umbrella, pre-leg-IN, mid-IN,
  sized-but-unjournaled OUT with the baseline no-sweep guard, mid-OUT).
- **Devimint smoke (`smoke_probe_devimint.sh`, the 5.0 exit gate):** two-fed harness —
  `wallet-cli probe B` runs the live round trip. Mid-probe, B's delta equals the
  PERSISTED DELIVERED net (the executor's verified hair-under is a healthy outcome —
  assert `delivered ≤ PROBE_AMOUNT` and `PROBE_AMOUNT − delivered` small, never over);
  post-probe, B's residue < the out leg's fee cap and the COMBINED S+B wallet total
  falls by fees only, asserted bounded by 2 × leg fee cap (S alone also bears the
  residue left on B, so the smoke asserts the combined delta, per 5.0.1's accepted
  residue); the
  ledger shows the umbrella `probe` row plus both legs with the `active_probe` reason;
  three probes with a shortened `--min-span-secs` flip the PROBE VERB's OWN reported
  `verdict:` to `passed` (the verb computes with its overridden policy), while `status`
  — which always uses the DEFAULT policy — still reports `insufficient` for the same
  history (asserted: the production gate stays conservative under test-shrunk windows);
  a probe against a fed with no shared gateway terminalizes the umbrella row Failed with
  the route error, exits non-zero, and (asserted) leaves the VERDICT history untouched —
  pair reachability never demotes the candidate (5.0.3's scoping rule).

### 5.0.9 Settled decisions

1. The probe rides the EXISTING move machinery end to end — no parallel payment path,
   no new executor arms. New code = the pure verdict, the `0x08` record, the runtime
   orchestration verb, one `ReasonCode`, one umbrella `OperationKind::Probe` ledger row
   (so pre-move candidate faults stay in the audit trail), one facts field, one CLI verb.
2. Leg OUT (redeemability) is REQUIRED for a pass — mint-only proves too little.
3. A post-pass failure demotes immediately; passes must be rebuilt as a sustained window.
4. Probe attempts are durable with time-aware retention (sub-`ttl`, hard-capped at
   256); the ledger holds the full narrative.
5. 5.0 computes and surfaces the verdict but gates nothing; 5.1 wires the gate for
   discovered feds only. User-joined rebalancing is unchanged.
6. Probe residue on the candidate (fees + sizing hair) is accepted, counted, and visible;
   no cleanup machinery.

---

## 5.1 — discovery (BUILDABLE spec)

Turn the wallet from "manage the feds the user joined" into "discover, structurally vet,
and probe-gate candidate federations." 5.0 built the empirical trust gate (`active_probe`);
5.1 builds the CANDIDATE PIPELINE that feeds it and WIRES the gate so a discovered fed is
allocator-fundable only once it has PASSED. Grounded in ADR-0017 (probes gate, discovery
never promotes), ADR-0019/0020 (discovery inputs are UNTRUSTED, the Observer is a swappable
prior behind the gate), and [federation-data-sources-spec.md](./federation-data-sources-spec.md)
(the Observer API §C; Nostr kind-38173 is discovery-only §E; kind-38000 ratings dropped).

**Greenfield note.** Pre-release, no persisted data, no external users: NO backwards
compatibility, NO migration shims, NO serde compat layers.

### 5.1.0 The shape: a source-agnostic pipeline, sources behind a swappable seam

Discovery is UNTRUSTED I/O (HTTP to the Observer, later Nostr relays), and devimint hosts
neither — so the buildable core is the PURE pipeline + durable registry + gate wire-up +
auto-join accounting, and the concrete sources sit behind ONE trait so (a) the logic is
unit-testable against a fixture source, (b) the live devimint gate drives a fixture/manual
source pointed at the harness's fed B, and (c) ADR-0020's "swappable, never load-bearing"
Observer is literally one impl of the seam.

```rust
/// One untrusted candidate announcement (federation id + invite + network). A source NEVER
/// asserts trust — the id is later re-derived from the AUTHENTICATED config and must match.
pub struct CandidateAnnouncement {
    /// The federation id the SOURCE asserts — its RAW claim (Observer `id` field, Nostr `d`
    /// tag), NOT re-derived from `invite`. Kept distinct so the pipeline's Sybil check
    /// (`claimed_id == invite.federation_id() == config.federation_id()`) is meaningful — a
    /// source whose claimed id disagrees with its own invite is internally inconsistent and
    /// dropped. (For `Manual`, the caller supplies the invite and `claimed_id` is the
    /// invite's own id — the check is a no-op there, which is correct: the user IS the source.)
    pub claimed_id: FederationId,
    pub invite: InviteCode,
    pub network_hint: Option<String>,  // e.g. "bitcoin"/"signet" — a hint, re-checked structurally
    pub source: DiscoverySource,       // provenance (Observer | Nostr | Manual), for the ledger
}

pub enum DiscoverySource { Observer, Nostr, Manual }

#[async_trait]
pub trait CandidateSource {
    /// Best-effort AND status-bearing: a source that errors/times out returns
    /// `{ candidates: [], status: Failed(reason) }` — it never blocks discovery of the
    /// others (ADR-0020 "the wallet is correct if the Observer is down"), but a DOWN source
    /// stays distinguishable from a healthy source that truly found nothing, so the ledger
    /// (5.1.2) can record which happened.
    async fn candidates(&self) -> SourceResult;
}

pub struct SourceResult {
    pub candidates: Vec<CandidateAnnouncement>,
    pub status: SourceStatus,
}
pub enum SourceStatus { Ok, Failed(String) }
```

Impls: **ObserverSource** (5.1, HTTP — the richest source), **ManualSource** (5.1, a fixed
invite list from a CLI flag / fixture — the offline + live-gate source), **NostrSource**
(DEFERRED within 5.1 to a follow-on run: same trait, added when a Nostr relay-client dep is
vetted; the Observer already yields the candidate universe, so Nostr is additive, not
blocking). No source is load-bearing; discovery unions whatever the configured sources return.

### 5.1.1 The durable candidate registry (journal tag `0x09`)

A candidate is a fed the wallet learned about but has NOT necessarily joined and has NOT
funded. Distinct from the JOINED registry (`0x03` `FederationKey`→`FederationInfo`, user- and auto-joined membership):

```rust
pub struct CandidateRecord {
    pub id: FederationId,
    pub invite: InviteCode,
    pub source: DiscoverySource,
    pub discovered_at_ms: u64,
    /// The authenticated STRUCTURAL verdict (the free floor: guardian count, threshold/BFT,
    /// network, modules — the scorer's structural half). Refreshed on rediscovery, not frozen.
    pub structural: StructuralOutcome,
    /// When the structural verdict was last computed (a config fetch). Discovery RE-CHECKS a
    /// stale row after `STRUCTURAL_RECHECK_BACKOFF_MS` — so a fed initially `Rejected` for a
    /// missing module (an UPGRADEABLE property under the same id) is reconsidered later, and a
    /// `Discovered` fed's facts do not go permanently stale — without a config fetch every pass.
    pub structural_checked_at_ms: u64,
    pub state: CandidateState,
    pub updated_at_ms: u64,
}

pub enum StructuralOutcome { Passed, Rejected(String) }  // the reason mirrors ReasonCode

pub enum CandidateState {
    /// Structurally rejected — not fundable NOW, but NOT a permanent blacklist: kept so it
    /// is not re-fetched every pass, and reconsidered after `STRUCTURAL_RECHECK_BACKOFF_MS`
    /// (a fed can enable a required module under the same id and later pass).
    Rejected,
    /// Structurally vetted, NOT joined — surface-only until the user or the loop joins it.
    Discovered,
    /// AUTO-joined by the agent (a client partition exists); now probeable AND probe-GATED
    /// for funding, and COUNTED against the auto-join caps (5.1.4). The probe verdict (5.0,
    /// read live from `probe_record`) is NOT stored here — `probe_record` stays the single
    /// source of truth.
    AutoJoined,
    /// A user EXPLICITLY approved a candidate (5.1.4a): it leaves the probe GATE and the
    /// CONCURRENT cap for the grandfathered USER-joined path — the user vouched for it.
    /// Reached from `Discovered` (a plain `wallet-cli join`) OR from `AutoJoined` (an
    /// `approve`). It does NOT leave the LIFETIME cap: that counts immutable successful
    /// agent-join history, and approval does not reclaim the partition (5.1.4/5.1.4a) — else
    /// approving old auto-joins would reopen the budget and defeat the finite-partition bound.
    UserApproved,
}
```

- Keys: `0x09 ++ fed_id` → JSON v1 `CandidateRecord`; scans are poison-tolerant like every
  other registry. One row per fed, upserted in its own dbtx.
- `FedimintJournal::{put_candidate, get_candidate, list_candidates}`.
- Membership authority stays the joined registry (`0x03`); the candidate row's STATE
  distinguishes agent- from user-owned for the gate (5.1.3) and the budget (5.1.4). The
  user-ownership transitions are explicit (5.1.4a): a `Discovered` fed the user `join`s, or
  an `AutoJoined` fed the user `approve`s, both move to `UserApproved` — off the probe gate
  and out of the auto-join caps. `is_auto_joined_discovered(id)` in the gate means
  `state == AutoJoined` specifically (NOT `UserApproved`).

### 5.1.2 The pipeline (pure floor + one config-fetch I/O)

`Runtime::discover(sources, policy) -> DiscoverReport`. Discovery is REFRESHING, not
skip-on-seen — a known candidate is revisited so it is never permanently stranded. Multiple
announcements for ONE fed id in a pass (a mixed-source union can surface a stale AND a
current invite for the same fed) are RECONCILED, not first-wins-deduped: try each distinct
invite through the authenticated fetch (step 2) and adopt the FIRST that authenticates for
that id — so a good invite from a later source is never dropped in favor of an earlier stale
one. Then, per reconciled fed:

0. **Already-joined short-circuit (provenance-preserving).** If the fed is in the `0x03`
   JOINED registry, discovery must NOT re-floor / re-auto-join / budget-charge it (it is
   already joined), and must NOT DOWNGRADE its provenance. So:
   - It already has a candidate row (`AutoJoined` or `UserApproved`): REFRESH its stored
     `invite` to the newest valid one (a rotated invite is still worth keeping current for
     backup/re-open) but keep its STATE, and STOP — no re-floor, no re-auto-join, no
     budget charge. (Ownership is authoritative — an agent fed stays probe-gated, a user fed
     stays grandfathered; only the invite is allowed to move, consistent with step 1's
     refresh rule.)
   - It has a `Discovered`/`Rejected` row but is now joined (a stale row): STOP without
     changing state here — the row is superseded by membership; the OWNERSHIP is set by
     whoever joined it (a USER `join` set `UserApproved` via 5.1.4a; discovery never joins a
     fed outside step 5, which sets `AutoJoined`). A stale floor row is never re-used for
     auto-join because it is already joined.
   - It has NO candidate row but IS joined — a RESTORE that recovered the `0x03` membership
     but not the `0x09` state (§5.1.1 backup contract). Provenance is UNKNOWN, so default to
     the CONSERVATIVE side: seed the row as `AutoJoined` (PROBE-GATED, not `UserApproved`) —
     never infer user ownership from a missing row, or a restored agent fed would silently
     bypass its probe gate. The user promotes it with `approve` (5.1.4a) if it was really
     theirs. (No new join / no lifetime charge — the partition already exists; a restored fed
     that lacks its immutable join-history row simply is not counted, a bounded restore edge.)
1. **Decide whether to (re)fetch.** A NEW id, the stored invite NO LONGER announced (a
   genuine rotation — the known endpoint is gone), or a row whose `structural_checked_at_ms`
   is older than `STRUCTURAL_RECHECK_BACKOFF_MS` (default 7d) → (re)fetch + re-floor below. A
   differing invite that merely COEXISTS with the still-announced stored invite does NOT force
   a re-fetch: the stored invite is still a known-good way to reach the fed, and honoring the
   backoff is a deliberate DoS-defense — otherwise a noisy/hostile source could force an
   authenticated fetch every pass by advertising ever-changing alternate invites (the
   untrusted-source volume/time class deferred to 5.2, §5.1.5). A rotated invite is thus
   adopted within `<=` one backoff window, and auto-join re-validates the invite with a fresh
   fetch regardless, so a truly dead stored invite self-corrects. An
   `AutoJoined`/`UserApproved` candidate (already joined; membership is authority) only has
   its stored `invite` REFRESHED to the newest valid one and is not re-floored. An
   up-to-date `Discovered`/`Rejected` row within the backoff whose stored invite is still
   announced is left as-is (no wasted fetch).
2. **Authenticate the invite:** fetch the `ClientConfig` from the guardian quorum WITHOUT
   joining — reuse `client_builder().preview(connectors, &invite)` (the same authenticated
   fetch `join` does, minus the partition write). Authenticated against the invite's id, so a
   source cannot spoof N/threshold/modules/network. A fetch failure is a TRANSIENT skip
   (retry next pass), recorded on the ledger, and NEVER downgrades an existing row (a
   transiently-unreachable fed keeps its last verdict).
3. **Verify the claimed id (Sybil hygiene):** all three must agree —
   `announcement.claimed_id == invite.federation_id() == config.federation_id()`. The
   invite→config equality is the AUTHENTICITY check; the claimed→invite equality catches a
   source that announced an id inconsistent with the invite it shipped. A mismatch drops the
   candidate (logged; no row change).
4. **Assemble structural facts + run the scorer's STRUCTURAL floor** (the free half of
   `score` — guardian count, BFT threshold, network, module presence; NO probe, NO Observer
   prior), stamping `structural_checked_at_ms = now`. `Passed` → the row becomes/stays
   `Discovered` (or keeps `AutoJoined`/`UserApproved`); `Rejected(reason)` → `Rejected` (a
   previously-`Discovered` fed that regressed drops back to `Rejected`, but an already-JOINED
   fed is not un-joined — membership is one-way in v1).
5. **Consider auto-join (5.1.4) over ALL `Discovered` candidates**, newly-added AND
   pre-existing, within budget — so a fed first seen while `--auto-join` was off, or while a
   cap was exhausted, is picked up on a later pass once budget frees, rather than stranded
   unprobeable forever. **Re-validate before joining:** a pre-existing `Discovered` row was
   floored at `structural_checked_at_ms`, which may predate an upgrade OR a REGRESSION under
   the same id (module removed, guardians dropped). So auto-join FORCES a fresh authenticated
   config fetch + structural re-floor for a cached candidate immediately before the join (a
   newly-floored candidate from steps 2-4 this pass is already current and skips the refetch),
   and joins ONLY if it still passes — otherwise it drops back to `Rejected` and is not
   joined. "Structurally vetted" is thus true AT JOIN TIME, not merely at first discovery.

Every discover invocation writes ledger rows so an unattended pass is fully auditable, split
so each fact lives where it is attributable:
- ONE `OperationKind::Discover { source, status, found, structurally_passed, rejected }` PER
  SOURCE (keyed `discover:<source>:<nonce>`), `status` from the source's `SourceResult`
  (`ok`/`failed:<reason>`) — so a DOWN source (`status: failed, found: 0`) is distinguishable
  from a healthy-but-empty one (`status: ok, found: 0`).
- ONE source-neutral `OperationKind::AutoJoin { considered, joined, blocked_concurrent,
  blocked_weekly, blocked_lifetime }` per invocation (keyed `autojoin:<nonce>`), because
  auto-join runs over the GLOBAL `Discovered` pool (candidates from earlier runs and other
  sources) — a per-source counter would mis-attribute a global budget exhaustion to whichever
  source ran last. This row is where the cap-block diagnostics live.
- Each actual auto-join is ALSO its own `actor: Agent` `join` row (5.1.4), decoupled from the
  discovery source; the candidate's durable `source` field records who ORIGINALLY discovered
  it. So discovery counts are per-source, budget/cap outcomes are source-neutral, and each
  partition the agent created is individually attributable — extending the Phase-4
  auditability contract to every discovery action.

### 5.1.3 The gate wire-up — a discovered fed funds only when PASSED

`build_snapshot` (`tick.rs`) already stamps `eligible_to_fund` from the scorer verdict. 5.1
adds: for an AUTO-JOINED discovered fed, `eligible_to_fund` ALSO requires
`active_probe(source = designated spending fed) == Passed`. USER-joined feds keep the
grandfathered proxy path (eligible on the scorer verdict alone — the roadmap's stance: the
cheap proxy is fine for feds the user chose).

- **Break the self-reference first (ordering rule):** the gate reads
  `active_probe(source = designated spending fed)`, so an `AutoJoined` fed must NEVER
  auto-designate as the spending fed — otherwise it becomes its own source, its self-probe
  is `None` (a fed cannot probe itself, §5.0.6), and the gate oscillates. `build_snapshot`'s
  auto-designation (tick.rs) therefore EXCLUDES `AutoJoined` feds from the spending pick
  (they remain eligible DESTINATIONS once `Passed` — a probed discovered fed is a place to
  PUT funds, not the wallet's spender). An operator may still PIN a `UserApproved` fed as
  spending (it is user-owned by then). This makes the source stable and the gate
  well-defined as balances/rankings change.
- `build_snapshot` gains the auto-joined set (`&BTreeSet<FederationId>` from
  `list_candidates` where `state == AutoJoined`) and the per-fed probe verdict (already
  computed for §5.0.6's status surfacing). The probe gate LAYERS ON TOP of today's
  `verdict.eligible_to_fund || is_pinned` rule (the §15.3 pin refinement) — it does not
  replace it:
  `eligible_to_fund = (scorer_eligible || is_pinned) && probe_gate_ok(id)`, where
  `probe_gate_ok(id) = !is_auto_joined_discovered(id) || active_probe == Some(Passed)`.
  So a USER-joined fed (never in the auto-joined set) keeps EXACTLY today's behavior
  including the pin override; only an AGENT-auto-joined discovered fed additionally requires
  a `Passed` probe.
- This is the one `if` §5.0.6 prepared. It is PURE (the pinned/auto-joined sets + verdict are
  inputs), so it is golden-tested in `tick.rs` without any I/O.
- **The gate's probe policy is an operator knob** (found by the live gate): the funding gate
  evaluates the discovered fed's sustained-pass verdict under a `ProbePolicy` carried on
  `TickPolicy` (`probe_gate_policy`), defaulting to the conservative `ProbePolicy::default()`
  (3 successes spanning >= 24h within a 7d ttl). `tick`/`status` expose
  `--probe-min-span-secs`/`--probe-min-successes`/`--probe-ttl-secs` to tune the sustained
  WINDOW (amount/fee-cap STRENGTH stay at default so real probes qualify). A conservative
  operator keeps the 24h window; loosening it is an explicit risk choice — and is how the live
  gate funds a just-probed fed without a 24h wait.
- A PIN of an auto-joined-discovered fed does NOT bypass the probe gate (`probe_gate_ok`
  ignores pinning): the agent must not fund an unproven discovered fed, and a pin cannot
  vouch for empirical redeemability the way it vouches for a user's own fed. A user who wants
  to fund a discovered fed regardless APPROVES it (5.1.4a), moving it onto the grandfathered
  user-joined path where the pin override applies again.

### 5.1.4 Auto-join, bounded and disclosed (the SETTLED lifetime decision)

Without auto-join a discovered fed can never reach `Passed` unattended, so the loop MAY
auto-join a structurally-vetted candidate. A join moves NO money (an authenticated config
fetch + a client partition), lands as a `join` ledger row with `actor: Agent`, and is
bounded by THREE limits in `DiscoveryPolicy`:

- `max_concurrent_unproven` (default 3): auto-joined feds whose probe is not yet `Passed`.
  Caps the in-flight probing surface. (Counted from the candidate registry's `AutoJoined`
  rows — one row per real partition — so it is naturally free of attempt/no-op noise.)
- `max_auto_joins_per_week` (default 5): a rate limit over SUCCESSFUL NEW-partition agent
  joins in the trailing 7 days.
- **`auto_join_lifetime_cap` (default 20): the SETTLED lifetime bound.** Joins are one-way
  in v1 (no eviction — a documented non-goal), so the rate limits bound the RATE, not the
  total; a hard lifetime cap on total agent-created partitions is the simplest bound that
  keeps a long-running wallet's partition set finite. **Counted from IMMUTABLE agent-join
  HISTORY — the ledger's `actor: Agent` `join` rows that SUCCEEDED and created a NEW
  partition (status `Succeeded` AND `newly_joined`), a monotonic count of every partition the
  agent ever created, NOT the mutable candidate state.** (Failed attempts and no-op re-opens
  also write `join:` rows but created no partition, so they must NOT count — else a few
  transient auto-join failures would exhaust the budget. The weekly cap filters the same way;
  the concurrent cap sidesteps it by counting `AutoJoined` registry rows.) Counting live `AutoJoined`
  rows would let a user flip 20 auto-joins to `UserApproved` (5.1.4a), reset the budget to 0
  while all 20 partitions still exist, and let discovery create 20 more — the finite-set
  guarantee gone. Approval leaves the partition in place, so it must keep counting against
  the lifetime cap; the immutable join history does exactly that. (The CONCURRENT cap, by
  contrast, correctly uses live `AutoJoined` state — it bounds in-flight PROBING surface,
  which an approved fed has left; and the WEEKLY cap uses the trailing-7d window of the same
  join rows.) **Eviction of never-passed candidates is deferred** to a later phase (it needs
  partition-reclamation machinery; flagged, not built) — until then, hitting the lifetime cap
  stops further auto-joins and surfaces a diagnostic, rather than silently churning partitions.

Every limit that BLOCKS an auto-join is `log`+`ledger`-recorded on the source-neutral
`AutoJoin` row's `blocked_*` counts, never silent. Manual (user) joins are unaffected and
uncounted against these caps.

### 5.1.4a User approval — the manual escape hatch off the probe gate

A user can take ownership of a candidate at any time, moving it to `UserApproved` (the
grandfathered path — fundable on the scorer verdict / pin alone, no probe gate). Precisely
which caps this frees (resolving the tension with 5.1.4's immutable lifetime count):
approval removes the fed from the CONCURRENT cap (it leaves the in-flight probing surface
that cap bounds, via the `AutoJoined -> UserApproved` state change) and off the probe gate.
It does NOT free the LIFETIME cap — that counts immutable agent-`join` history, and approval
neither reclaims the partition nor deletes the original Agent join row, so the partition the
agent created keeps counting (the finite-set guarantee). The WEEKLY cap is a trailing-7d
window of the same join rows: approval is irrelevant to it (the join ages out of the window
on its own). So: approval frees CONCURRENT + the probe gate; LIFETIME and WEEKLY are
unaffected.

- `wallet-cli join <invite>` of a candidate in ANY state (`Discovered`, `Rejected`, or no
  row): joins it AND sets `UserApproved` — a user-initiated join is a user vouch, regardless
  of a prior structural rejection or a stale row; the `join` ledger row carries `actor: User`.
  (This is how a user takes ownership of a fed discovery had `Rejected` — the join path, not
  the discovery short-circuit, confers user ownership.)
- `wallet-cli approve <fed>` of an `AutoJoined` candidate: the fed is already joined, so this
  only flips the candidate state `AutoJoined -> UserApproved` (no money, no new membership) —
  but it IS a user-visible ownership change, so it writes a ledger row
  (`OperationKind::Approve { fed }`, `actor: User`, keyed `approve:<fed>:<nonce>`) explaining
  why the fed left the probe gate and the concurrent cap (the Phase-4 auditability contract).
  Without the state flip an agent-auto-joined fed would stay probe-gated and keep counting
  toward the concurrent cap even after the user blessed it (the gap codex flagged).
- A `UserApproved` fed that the user later LEAVES (a future eviction verb) is out of scope
  here (5.1 has no eviction); approval is one-way in v1, like membership.

### 5.1.5 Observer source (the first real `CandidateSource`)

`ObserverSource { base_url, http }` over `reqwest` (rustls; the workspace's HTTP stance):
`GET {base}/federations` → the summaries (`{id, name, invite, ...}`,
[data-sources §C](./federation-data-sources-spec.md)); map each to a `CandidateAnnouncement`
carrying the Observer's OWN `id` as `claimed_id` (NOT re-derived from the invite — so the
5.1.2 `claimed == invite == config` check can actually catch an Observer row whose `id` and
`invite` disagree) plus the parsed `invite`. **UNTRUSTED + swappable + never load-bearing
(ADR-0020):** the Observer only SUGGESTS candidates; every structural fact is re-derived
from the authenticated config (5.1.2), so a wrong/hostile Observer can waste a config fetch
but cannot promote a fed. The unstable `/federations` schema is parsed leniently (unknown
fields ignored; a row that fails to parse is skipped, not fatal). The Observer prior
(uptime/backing/activity — the ADR-0020 rank bonus behind the gate) is a SEPARATE, later
wiring into `FederationFacts.observer`; 5.1's Observer use is discovery only.

**Deferred to 5.2 (volume/time bounds):** a hostile Observer can return unbounded rows, and each reconciled candidate triggers a guardian config `preview` with no per-preview timeout — so a manual `discover --source observer` against a flooded/slow feed can run long. This is low-impact in 5.1 (only a user manually invoking `discover` pays it), but becomes load-bearing when the 5.2 `watch` loop invokes discovery UNATTENDED on a schedule. Add a per-pass candidate cap (log-what-was-dropped, never a silent truncation) + a per-preview timeout THERE, where they can be tested against the autonomous loop.

### 5.1.6 CLI

```
wallet-cli discover [--source observer|manual] [--observer-url URL]
                    [--invite <code>]... [--auto-join] [--gateway URL]
                    [--max-auto-joins-per-week N] [--lifetime-cap N] [--json]
wallet-cli candidates [--state discovered|autojoined|userapproved|rejected] [--json]
wallet-cli approve <fed-hex>   # bless an AutoJoined candidate -> UserApproved (5.1.4a)
```
- `discover` runs the pipeline over the chosen source(s) (`manual` = the `--invite` list, the
  offline + live-gate source; `observer` = the HTTP source). `--auto-join` enables bounded
  agent auto-join. A devimint-only `--scorer-allow-regtest` relaxes the structural floor's
  mainnet requirement for the harness (production discovery keeps `require_mainnet = true`).
  `--gateway` is used for the immediate post-join probe route where applicable; the ongoing
  probe/tick verbs take their OWN `--gateway` (no route is persisted per candidate in 5.1).
  Prints
  a summary (found / structurally passed / rejected / auto-joined) to stdout; diagnostics to
  stderr; exits non-zero only on a usage error (a source being down is not a failure — it is
  an empty contribution, ADR-0020).
- `candidates` lists the durable registry (TSV + `--json`), newest-first, filterable by state.

### 5.1.7 Tests / exit gate

- **Pure goldens (`wallet-core` + `tick.rs`):** the gate rule — an AUTO-JOINED discovered fed
  is `eligible_to_fund` ONLY when `active_probe == Passed`; a USER-joined fed is eligible on
  the scorer verdict alone; a pin does not bypass the probe gate for an auto-joined fed;
  structural-floor pass/reject mapping.
- **Pipeline unit (`wallet-fedimint`, fixture source):** an announcement with a mismatched
  claimed id is dropped; a structurally-rejected config yields a `Rejected` row (not
  re-fetched); a passed config yields a `Discovered` row; the three auto-join caps each
  block and record (concurrent / weekly / lifetime); a down source contributes nothing
  without failing the pass.
- **Journal (MemDatabase):** candidate registry round-trip + poison tolerance; the
  `Discover` ledger row records the counts; auto-join writes an Agent `join` row.
- **Observer source unit:** parse a recorded `/federations` fixture into announcements;
  a malformed row is skipped; a claimed id that mismatches its invite is caught downstream.
- **Devimint smoke (`smoke_discover_devimint.sh`, the 5.1 exit gate):** the two-fed harness
  with a MANUAL source = fed B's invite. **Devimint scaffolding (as every other smoke uses):**
  devimint feds are REGTEST, so the discovery structural floor runs with a devimint scorer
  policy (`require_mainnet = false`) — otherwise the `network` floor rejects B before
  auto-join; and every `probe`/`tick` command carries `--gateway <GW>` (the shared LDK gateway
  is not auto-registered into the lnv2 set) plus explicit `--spending <A> --standby <B>` pins
  on the `tick` steps (regtest feds are scorer-ineligible, so auto-designation picks nothing —
  the same pins `smoke_tick`/`smoke_evacuate` rely on; note the §15.3 pin refinement does NOT
  bypass the PROBE gate for the auto-joined B, which is exactly what this gate tests).
  Steps: `discover --source manual --invite <B> --auto-join --gateway <GW>
  --scorer-allow-regtest` structurally vets B, auto-joins it (Agent join row), leaving it
  `AutoJoined` but NOT fundable — a `tick --spending A --standby B --gateway <GW>` at this
  point does NOT fund B (probe not `Passed`, asserted). Then drive 5.0 probes
  (`probe B --from A --gateway <GW> --min-span-secs 1` ×3) to `Passed`, and the SAME `tick`
  NOW funds B (the gate opened). Assert the full chain in `history`: the per-source `Discover`
  row, the `AutoJoin` row, the Agent `join`, the probes, and the gated-then-ungated funding —
  a candidate that only ever failed the probe is never funded. This is the phase gate
  (discover → structural floor → active probe → score → rebalance, fully recorded).

### 5.1.8 Settled decisions

1. Sources are UNTRUSTED and behind a `CandidateSource` seam; the Observer is one swappable
   impl, never load-bearing (ADR-0020). Nostr kind-38173 slots into the same seam in a
   follow-on; kind-38000 ratings are dropped entirely (data-sources §E).
2. Every structural fact is re-derived from the AUTHENTICATED config; a source can only
   suggest, never promote — the id is re-verified against the invite.
3. The candidate registry (`0x09`) is distinct from joined membership (`0x03`); the
   candidate STATE (`Rejected`/`Discovered`/`AutoJoined`/`UserApproved`) distinguishes
   agent-owned (probe-gated, budgeted) from user-owned (grandfathered) for the gate and the
   budget; a user `join`/`approve` moves a candidate to `UserApproved` (5.1.4a).
4. The gate: an auto-joined discovered fed funds only at `active_probe == Passed`; user-joined
   feds keep the grandfathered proxy path; a pin does not bypass the probe gate for a
   discovered fed.
5. Auto-join is bounded by three caps; the LIFETIME cap (default 20) is the settled finite
   bound — eviction of never-passed candidates is a documented deferral, not built in 5.1.
6. Discovery I/O is best-effort: a down/hostile source is an empty contribution, never a
   failure (the wallet is correct if the Observer is wrong/down/gone).

### 5.1.9 Build order (for rb-lite)

1. **5.1a — the pure core + registry + gate** (no external I/O): **DONE, merged `87b01f4`
   2026-07-08.** Implemented via rb-lite (codex-first after a transient claude overload killed
   attempt 1; 11 rounds clean) + independent verification + one adversarial codex P2 fixed
   (user-join now recovers a corrupt `0x09` row instead of stranding the fed behind the probe
   gate). ~500 tests green; the gate, self-reference break, three-cap accounting, and ledger
   kinds all landed. No live gate here (pure/unit — the live gate is 5.1c). `CandidateAnnouncement`/
   `DiscoverySource`/`CandidateSource` trait + `CandidateRecord`/`CandidateState` + the
   `0x09` journal registry + `Discover` ledger kind + the `build_snapshot` gate rule +
   auto-join accounting (the three caps, read from the ledger/registry). `ManualSource`.
   Golden + MemDatabase tests. NO reqwest.
2. **5.1b — `Runtime::discover` + the CLI verbs + the Observer HTTP source** (adds reqwest): **DONE, merged `6f59ef6` 2026-07-08.** rb-lite (codex-only after the claude *auth* logout — not overload — killed the cycling runs; 6 rounds clean) + independent gate + a DUAL adversarial pass (codex + a claude subagent, since the convergence panel was degraded 1-of-3 with the claude reviewer logged out). Closed a probe-gate bypass (agent-joined member with a stale/absent `0x09` row read ungated on `tick` → now `joined − UserApproved`, fail-closed); rejected 2 findings with evidence (concurrent-runs budget = v1-unreachable single-writer; any-differing-invite refetch = would reopen the deferred Observer DoS). ~500 tests.
   the pipeline (preview-fetch → id-verify → structural floor → registry → bounded
   auto-join), `wallet-cli discover`/`candidates`, `ObserverSource`. Fixture-source unit
   tests + the recorded-`/federations` parse test.
3. **5.1c — the devimint exit-gate smoke** (`smoke_discover_devimint.sh`, run by hand): **DONE 2026-07-09 — LIVE GATE PASSED.** discover -> agent auto-join B (within budget; Discover+AutoJoin+agent-join rows) -> gated tick BAILS (probe-gated; pin does not bypass) with B empty -> 3 probes -> passed -> the SAME tick funds B to ~target (never over). The live gate surfaced + fixed one real code gap: the funding-gate probe policy was hardcoded/un-tunable, now the `--probe-min-span-secs`/`-min-successes`/`-ttl-secs` standing-instruction knobs (default conservative). **Phase 5.1 (discovery) COMPLETE.**

### Non-goals (5.1)

The self-running loop (5.2, `wallet-cli watch`), the live Nostr relay source (a follow-on on
the same seam), the Observer RANK prior (a later `FederationFacts.observer` wiring — 5.1 uses
the Observer for discovery only), candidate/partition EVICTION (deferred; the lifetime cap is
the finite bound instead), and any UI.

## 5.2 — the self-running loop (BUILDABLE spec)

Turn the operator-invoked verbs into an UNATTENDED agent. `wallet-cli watch` is a long-running
process that, on an adaptive cadence, RECONCILES in-flight work, TICKS (allocation +
evacuation), re-PROBES federations whose trust verdict is going stale, and periodically
DISCOVERS candidates — all through the SAME `tick`/`probe`/`discover` verbs, so the loop adds
NO new money path. Its novelty is purely the SCHEDULER: when to wake, what to run, and the
budget that bounds unattended probing. Grounded in ADR-0014 (every scheduled action is an
`Agent` ledger row) and §15.1 (the corroborated shutdown/expiry signal the tick already reads).

**Greenfield note.** Pre-release, no persisted data, no external users: NO backwards
compatibility, NO migration shims, NO serde compat layers.

### 5.2.0 The shape: one process, single writer, adaptive wake

`watch` is the wallet's ONLY unattended actor and holds the exclusive RocksDB `db.lock` for
its whole life — it IS the single writer v1 assumes, so the loop never races itself and needs
no new concurrency machinery. It runs a sequential cycle and SLEEPS between cycles for an
ADAPTIVE interval = `min(base_interval, time-until-nearest-deadline)`, where a "deadline" is an
approaching federation EXPIRY (evacuate before shutdown) or a verdict TTL about to lapse. This
covers the KNOWN deadlines a cycle has already read. To also react PROMPTLY to a signal
published JUST AFTER a cycle (which pure polling would miss for up to `base_interval`), the loop
ADDITIONALLY subscribes: the pinned Fedimint client exposes
`client.meta_service().subscribe_to_field("federation_expiry_timestamp")` (verified at the pin,
`fedimint-client/src/meta.rs`), and the loop's sleep is `select!(sleep(adaptive_interval),
wake_rx.recv())` where each joined fed's subscription stream sends a WAKE on any change. So a
freshly-published expiry interrupts the sleep and the NEXT cycle re-evaluates immediately.
**Safety — the subscription is a WAKE HINT, never a decision:** the merged-meta
`federation_expiry_timestamp` is UNTRUSTED (a single `meta_override_url` host can forge it,
§15.1), so it only triggers a cycle; the ACTUAL evacuation stays the tick's f+1-CORROBORATED
decision. Because the field is UNTRUSTED, subscription wakes are COALESCED (rate-limited), NOT suppressed:
a wake ALWAYS recomputes the adaptive sleep IMMEDIATELY, capping it to any newly-learned deadline
— it NEVER waits out a cooldown, so a genuine short-notice shutdown that lands during the window
still shortens the sleep and is serviced before its deadline (honoring the `<30s`-to-shutdown
guarantee below). The rate-limit applies ONLY to consecutive subscription-triggered CYCLES that
found NO corroborated change: after such a no-op cycle, another purely-subscription-driven cycle
is held off for `min_interval`, so a flapping/hostile `meta_override_url` cannot force more than
one no-op cycle per window. A cycle for a CORROBORATED deadline (or the poll floor) is never
rate-limited. Recomputing the sleep is cheap; running the corroborating cycle is what is
coalesced — flap-proof AND evacuation-safe. The poll-based adaptive wake remains the FLOOR (so the loop still works if a
subscription stream drops or a fed exposes no meta service).

### 5.2.1 The cycle (each iteration, in order)

1. **Reconcile** (`Runtime::reconcile`, §9): re-drive any Pending/Executing intents and rebuild
   move records — a crash mid-cycle self-heals on the next wake before any new decision.
2. **Tick** (`Runtime::tick`): probe → score → snapshot → decide → apply. This ALREADY performs
   both proactive rebalancing AND evacuation of a shutdown-flagged fed (§15.1/Phase 3.A), so the
   loop gets evacuation for free — it just has to tick promptly enough (5.2.2). Each cycle uses a
   FRESH occurrence (5.2.5) so the tick's stale-occurrence guard never wedges the loop.
3. **Schedule probes** (5.2.3): for each AUTO-JOINED fed that is NOT yet fundably `Passed`, run a
   `probe` (source = the designated spending fed) to DRIVE it toward a pass — bounded by the
   global probe budget AND a per-fed retry backoff. This covers the INITIAL probe (a freshly
   auto-joined fed is `NeverProbed`), continued probing (`Insufficient` — accumulate the
   sustained window), RECOVERY (`Failed`/`FailedSinceLastPass` — retry after the backoff, do not
   hammer), and REFRESH (a `Passed` fed within `probe_refresh_lead` of its TTL, so its verdict
   never lapses). Without initial/recovery probing the discover -> probe -> fund pipeline would
   stall on every newly discovered fed.
4. **Discover** (5.2.4), on a longer sub-cadence (`discover_every`): run `Runtime::discover` over
   the configured sources with bounded auto-join, so the candidate universe refreshes unattended.

Every step's actions are already `Agent`-attributed ledger rows (tick decisions, probe umbrella,
discover/autojoin) — an unattended session is fully reconstructible from `history`, no new
ledger surface. A step that FAILS is logged + recorded and does NOT abort the cycle (the loop is
resilient: a transient tick failure must not stop future evacuations/probes); only a fatal
error (lock lost, DB corruption) stops the loop.

### 5.2.2 Adaptive wake — reactive to expiry and TTL

The sleep before the next cycle is `min` over EVERY configured deadline (a larger
`base_interval` must never sleep PAST a sub-cadence):
- `base_interval` (default 10 min) — the routine rebalance cadence;
- `next_discover_due` = `min_interval` when the persisted `discover_backlog` flag is set (a pass
  truncated on its deadline with the cursor un-wrapped, 5.2.4/5.2.5 — drain the backlog over quick
  continuations), else `last_discover_ms + discover_every - now` — so a long base interval cannot
  skip a scheduled discovery pass AND a large feed actually drains rather than waiting 6h between
  continuations;
- for every joined fed with a corroborated expiry, `expiry - now - evacuation_lead` (default lead
  1h): wake in time to EVACUATE before shutdown, not after;
- for every AUTO-JOINED fed with a `Passed` verdict (only those the scheduler actually refreshes —
  NOT a user-joined fed probed manually, which `watch` never re-probes), `ttl_deadline -
  effective_refresh_lead - now` (the SAME `min(probe_refresh_lead, ttl_ms / 2)` clamp §5.2.3 uses,
  NOT the raw lead) — wake to REFRESH BEFORE it lapses; scoping this to refreshed feds stops a
  manually-probed user fed from driving near-1s wakeups once its verdict enters the lead window,
  and the clamp stops a shrunk `--probe-ttl-secs` from putting the deadline in the past;
- for every non-`Passed` auto-joined fed, its next PROBE-DUE deadline per 5.2.3, keyed off
  `last_invocation` (the umbrella row, not just qualifying attempts): `min_interval` only for a
  `NeverProbed` fed with NO prior invocation; `last_invocation + probe_retry_backoff` for one
  whose last invocation was a refusal/`NoAttempt`; `last_attempt + probe_build_interval` for
  `Insufficient`/`Expired`; `last_attempt + probe_retry_backoff` for `Failed`/`FailedSinceLastPass`;
  and, when the global budget is exhausted, the BUDGET-RESET time — so a large `base_interval`
  never sleeps past re-eligibility yet the loop never hot-loops a fed that cannot make progress.
The `min_interval` floor (default 30s) applies ONLY to the ROUTINE cadence — the fallback when
no concrete deadline is sooner — so the loop does not busy-spin when idle. A CONCRETE deadline
(an evacuation lead or a probe-due time) that falls sooner than `min_interval` BYPASSES the
floor: the loop wakes at that deadline (with a tiny ~1s floor only to avoid a true busy-spin), so
a fed already `<30s` from shutdown is still re-ticked BEFORE it shuts down, not after. The upper
clamp stays `base_interval`. Together with the meta-subscription wake-hint above, this is the loop's
"reactive" behavior — prompt to a fresh signal, bounded by the adaptive poll floor.

### 5.2.3 Probe scheduling + the global probe budget

The scheduler is the ONLY unattended probe initiator (a manual `wallet-cli probe` stays
un-budgeted — the operator owns that spend). A fed is PROBE-DUE when it is an auto-joined
candidate that is NOT fundably `Passed` and is past its PER-VERDICT next-due time (defined
precisely below — the generic word "backoff" here defers to those cadences, which differ by
verdict: ~12h `probe_build_interval` for `Insufficient`/`Expired`, ~1h `probe_retry_backoff` for
`Failed*`), OR it is `Passed` with its newest qualifying success within the EFFECTIVE refresh lead
`min(probe_refresh_lead (default 12h), ttl_ms / 2)` of `ttl_ms`. Clamping to the TTL matters
because 5.2.6 lets the operator shrink `--probe-ttl-secs`: with a fixed 12h lead, any TTL below
12h would make a FRESHLY `Passed` fed instantly "within lead" and re-probed (paid) every cycle,
burning the weekly budget; the clamp keeps a fresh pass aging at least half its TTL before a
refresh. Concretely, by verdict, the NEXT-DUE time is:
`NeverProbed` -> now (the one initial probe). `Insufficient` / `Expired` (have evidence, must
BUILD the sustained window) -> `last_attempt + probe_build_interval`, where
`probe_build_interval = max(min_interval, min_span_ms / max(1, min_successes - 1))` (default
`24h / 2 = 12h`): probing every `min_interval` here would be futile AND ruinous — it cannot
shorten the 24h span requirement and would burn the whole weekly budget in ~25 min, so probes
are SPACED to accrue `min_successes` successes across `min_span_ms` in the minimum number of
attempts. `Failed` / `FailedSinceLastPass` -> `last_attempt + probe_retry_backoff` (default 1h;
never hammer a persistently-failing fed). `Passed` -> refresh within `probe_refresh_lead` of the
TTL. Bounded by a GLOBAL `ProbeBudget` in `WatchPolicy` AND these per-verdict cadences:
- `max_probe_attempts_per_week` (default 50): count of `Agent` probe umbrella rows in the
  trailing 7d THAT RECORDED AN ACTUAL ATTEMPT (money moved) — `NoAttempt` refusals (0-sat, no
  route/gateway/source fault) are EXCLUDED. Otherwise one persistently unroutable fed's hourly
  `NoAttempt` retries would hit the 50-cap in ~2 days and starve probing of HEALTHY candidates
  for everyone; the per-fed `last_invocation` backoff already bounds a single fed's retry rate,
  so the GLOBAL cap only needs to bound money-spending attempts.
- `max_probe_spend_per_week_msat` (default 50_000): sum of those (attempt) rows' `cost_msat` (the
  S-net-outflow the probe already records) in the trailing 7d.
Both read from the ledger (the single source of truth), so the budget survives restart with no
extra state.

**Backoff for probes that don't produce an ATTEMPT (the hot-loop fix).** NEXT-DUE keys off the
last probe INVOCATION, NOT just qualifying attempts — because 5.0 records a preflight refusal
(no shared route, missing gateway, source fault) as a `NoAttempt` that writes NO attempt row,
so a fed that can't be probed would otherwise stay `NeverProbed` and re-fire every `min_interval`
forever. Two rules close it:
- The umbrella `Probe` ledger row IS written on EVERY invocation (attempt or `NoAttempt`, §5.0.5);
  the scheduler reads its timestamp as `last_invocation` and backs a fed off by
  `probe_retry_backoff` after any invocation regardless of verdict — so a persistently unroutable
  `NeverProbed` fed retries on the backoff, not every 30s.
- A probe the GLOBAL budget would exceed is SKIPPED (logged + a scheduler diagnostic row, never
  silent), and while the budget is exhausted the wake is set to the BUDGET-RESET time (when the
  oldest counted row ages out of the 7d window) rather than `min_interval` — no spinning on a fed
  that cannot be probed until the budget frees. A fed left un-probed keeps its current verdict
  (gate stays closed if it lapsed — fail-safe).

### 5.2.4 Discovery scheduling + the deferred Observer bounds

On the `discover_every` sub-cadence (default 6h), run `Runtime::discover` with the operator's
`DiscoveryPolicy` (auto-join within its caps). This is where the 5.1b-DEFERRED untrusted-source
VOLUME/TIME bounds land, because the loop makes them load-bearing (an unattended discover over a
hostile Observer must not stall the whole agent):
- `discover_pass_deadline` (default 60s): a WHOLE-PASS wall-clock budget. `Runtime::discover`
  previews candidates SERIALLY (`authenticate_first_valid` awaits each in turn), so a per-preview
  timeout alone is not enough — `256 x 20s ~ 85 min` would exceed the 1h evacuation lead and
  starve a tick/evacuation. The pass stops early once this deadline elapses, keeping the loop's
  discovery step << `min_interval` and the evacuation lead. Discovery runs LAST in the cycle
  precisely so it is the step preempted, never a tick or evacuation. **A deadline-truncated pass
  is NOT deferred the full `discover_every`:** if the pass exits on the deadline with backlog
  remaining (the cursor did not wrap), it SETS the persisted `discover_backlog` flag (5.2.5) so
  the continuation survives a `--once` exit / restart; while that flag is set the next discover-due
  is `min_interval`, not `now + discover_every` (cleared when the cursor wraps) — so a large feed drains over
  successive quick continuations instead of ~3 candidates per 6h. `discover_every` governs only
  a fully-drained pass (cursor wrapped): the routine re-scan cadence.
  - **Fairness (a resume CURSOR, not just "defer"):** `Runtime::discover` walks candidates in
    deterministic sorted order (a `BTreeMap`), so a bare "stop at the deadline, defer the rest"
    would re-process the SAME slow head every pass and STARVE the tail forever. The scheduler
    persists a `discover_cursor` (the last fed id it ATTEMPTED — advanced after EVERY candidate it
    tries, whether the preview authenticated, failed, or timed out, NOT only on success) in the
    `0x0a` watch-state and
    RESUMES the next pass AFTER it — a round-robin over the candidate set that wraps to the start,
    stepping OVER a persistently slow/failing head instead of re-consuming the whole
    `discover_pass_deadline` on it every pass and starving the tail,
    so every candidate is reached within a bounded number of passes regardless of a slow head.
    Fresh NEW-id announcements do NOT jump the queue: they join the SAME bounded rotation (a new
    id is inserted into the sorted candidate order and reached when the cursor arrives). Otherwise
    a hostile Observer emitting enough new feds to fill the deadline/cap every pass would starve
    known candidates' rechecks and deferred auto-joins forever — the exact adversary this bounds.
- `per_preview_timeout` (default 20s): each authenticated config `preview` is ALSO wrapped in a
  timeout, so one unresponsive guardian set cannot consume the whole pass deadline — a timed-out
  preview is the same TRANSIENT skip as a fetch failure (retry next pass, no row downgrade).
- `max_candidates_per_pass` (default 256): cap the WORK PER PASS — process at most 256 candidates
  starting FROM THE CURSOR — NOT the backlog. The overflow is NOT dropped; it stays in the
  rotation and is reached on the next pass as the cursor advances (a `log` of how many were
  deferred this pass, no silent truncation). Dropping the tail would let a source returning `>256`
  ids starve deferred rechecks/auto-joins forever; capping per-pass work + the cursor keeps the
  bounded-round-robin guarantee intact.

### 5.2.5 Occurrence + crash recovery

The loop advances a monotonic `occurrence` each cycle so successive ticks re-decide with fresh
idempotency keys (the tick's stale-occurrence guard otherwise wedges a repeated same-key
decision, §step-2.3). The occurrence is PERSISTED (a small `0x0a` watch-state row: `{ occurrence,
last_discover_ms, discover_cursor, discover_backlog }`, where `discover_backlog` is set when a
pass truncated on `discover_pass_deadline` with the cursor NOT yet wrapped and cleared when it
wraps) so a restart continues the sequence rather than colliding
with a journaled decision from the pre-crash cycle, and the discovery round-robin (5.2.4) resumes
where it left off. The watch-state is CHECKPOINTED after EVERY successful cycle (the advanced
occurrence + cursor made durable BEFORE the next cycle or a `--once` exit), NOT only on a signal
shutdown — else a repeated `watch --once` (the exit gate's driver) would reload the old occurrence,
reuse the prior tick keys, and trip the stale-occurrence guard instead of progressing. On start:
load the watch-state (or seed it), `reconcile`, then enter the loop. Recovery is the same single-writer sequential story as every other verb — no new
concurrency.

### 5.2.6 CLI

```
wallet-cli watch [POLICY FLAGS: --spending --standby --per-fed-cap --spending-target
                  --standby-target --max-fee --gateway --probe-min-span-secs ...]
                 [--base-interval-secs S] [--min-interval-secs S] [--evacuation-lead-secs S]
                 [--discover-every-secs S] [--source observer|manual] [--observer-url URL]
                 [--invite <code>]... [--auto-join] [--scorer-allow-regtest]
                 [--max-auto-joins-per-week N] [--lifetime-cap N]
                 [--max-probe-attempts-per-week N] [--max-probe-spend-per-week-msat N]
                 [--once]
```
- Reuses the SAME `PolicyFlags` the `tick`/`status`/`probe` verbs take (designation, targets,
  gateway, the §5.1.3 gate-policy knobs) AND the full `discover` knob set
  (`--scorer-allow-regtest`, `--max-auto-joins-per-week`, `--lifetime-cap`, sources) — the loop
  is those verbs on a timer, so unattended discovery must be configurable IDENTICALLY to the
  manual `discover` (and the exit gate needs `--scorer-allow-regtest` to auto-join regtest feds). `--once` runs a SINGLE cycle and exits (the testable/cron-friendly
  unit; the exit gate drives `--once` repeatedly rather than a real sleep).
- Runs until SIGINT/SIGTERM: on signal, finish the in-flight op, checkpoint the watch-state, and
  exit 0 (a clean shutdown a supervisor can restart). A fatal loop error exits non-zero.
- Diagnostics (next-wake reason, budget state, per-step outcomes) to stderr; the durable record
  is the ledger.

### 5.2.7 Tests / exit gate

- **Pure scheduler goldens (`wallet-core` or a pure `watch` module):** the adaptive-wake `min`
  (base vs nearest expiry-lead vs nearest ttl-deadline, clamped); the probe-refresh predicate
  (`Expired` / within-lead → due); the budget predicate (attempts + spend caps from a synthetic
  ledger); the candidate-cap truncation count.
- **Journal (MemDatabase):** the `0x0a` watch-state round-trip + monotonic occurrence advance;
  the budget counts computed from seeded ledger rows (attempts + cost in / out of the 7d window).
- **`--once` unit (`wallet-fedimint`, fixtures):** one cycle runs reconcile→tick→probe→discover
  in order; a step failure is logged + recorded but does not abort the cycle; an exhausted budget
  skips + records the probe; a timed-out preview is a transient skip.
- **Devimint smoke (`smoke_watch_devimint.sh`, the 5.2 exit gate):** the FULL unattended chain on
  the two-fed harness driving `watch --once` repeatedly (deterministic, no real sleeps): discover
  (manual source = fed B) → agent auto-join → a cycle where B is probe-gated does NOT fund it →
  the scheduler probes B to a sustained pass (shrunk gate window) → a later cycle FUNDS B → then
  force fed B's shutdown signal and a cycle EVACUATES it. Assert every action is an `Agent` row in
  `history` and the whole discover→probe→score→rebalance→evacuate sequence is reconstructible; a
  candidate that only ever failed the probe is never funded. This is the Phase-5 exit gate.

### 5.2.8 Settled decisions

1. `watch` is the single writer (holds `db.lock`); no new concurrency. The loop is the existing
   verbs on an adaptive timer — NO new money path.
2. "Reactive expiry" = the adaptive-wake `min` over known deadlines PLUS a
   `meta_service().subscribe_to_field` wake-hint (available at the pin) that interrupts the sleep
   on a freshly-published signal — so detection is prompt, not `<= base_interval`. The
   subscription only WAKES the loop; the evacuation decision stays the tick's f+1-corroborated
   path (an untrusted/forged meta costs at most one harmless no-op cycle). Poll-based wake is the
   floor if a stream drops.
3. The global probe budget (attempts/week + sats/week) is enforced ONLY for the scheduler and is
   read from the ledger (no new durable budget state); manual `probe` stays un-budgeted.
4. The 5.1b-deferred Observer volume/time bounds (candidate cap + per-preview timeout) land here,
   where the unattended loop makes them load-bearing; both log-not-silently-truncate.
5. Occurrence is monotonic + persisted (`0x0a`) so restarts never reuse a journaled decision key.
6. A non-fatal step failure is recorded and does not abort the cycle; only lock-loss/corruption
   stops the loop. `--once` is the testable unit; the exit gate drives it repeatedly.

### 5.2.9 Build order (for rb-lite)

1. **5.2a — the pure scheduler + watch-state** (no loop, no I/O): `WatchPolicy`/`ProbeBudget`,
   the adaptive-wake `min`, the probe-refresh predicate, the budget predicate, the candidate-cap
   truncation, the `0x0a` watch-state registry (occurrence + last_discover_ms + discover_cursor). Golden +
   MemDatabase tests. The Observer candidate-cap + per-preview timeout + the whole-pass deadline
   with the resume-cursor rotation in `discovery.rs`.
2. **5.2b — `Runtime::watch_once` + the `wallet-cli watch` loop:** one cycle
   (reconcile→tick→scheduled probes→scheduled discover) wired to the pure scheduler, the
   adaptive sleep, `--once` vs the running loop, SIGINT/SIGTERM checkpoint. Fixture `--once`
   tests.
3. **5.2c — the devimint exit-gate smoke** (`smoke_watch_devimint.sh`, run by hand).

### Non-goals (5.2)

A true meta push-subscription (adaptive-wake poll suffices), a multi-process/distributed
scheduler (single writer), a cron/systemd integration (the operator wraps `watch` themselves),
per-fed probe budgets (one global budget), and any UI.

## Non-goals (Phase 5)

No UI (Phase 6), no on-chain peg-out, no reputation scoring from Nostr ratings (dropped
per the data-sources spec), no probe-result sharing/gossip, no multi-gateway probe
matrices (one validated shared route suffices to pass), no removal/eviction of joined
federations (one-way joins stay; an unfunded fed is inert).
