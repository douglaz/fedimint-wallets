# Phase 5 plan — the real active probe, discovery, and the self-running loop

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

## 5.1 — discovery (plan-level; buildable spec after 5.0)

- **Sources (both UNTRUSTED, ADR-0019/0020):** the Fedimint Observer client (candidate
  list + uptime prior; optional, swappable, never load-bearing) and Nostr kind-38173
  announcements (`d`=federation_id, `u`=invite, network) — DISCOVERY ONLY; kind-38000
  ratings are ignored entirely (tested and rejected,
  [federation-data-sources-spec.md](./federation-data-sources-spec.md) §E).
- **Pipeline:** announcement → invite parse → authenticated config fetch (the structural
  facts are self-authenticating against the federation id) → scorer structural floor →
  candidate registry row (`Discovered`, never auto-funded).
- **The 5.0 gate wire-up:** a `Discovered` fed becomes ALLOCATOR-fundable only when
  `active_probe == Passed` (the one `if` 5.0 prepared). User-JOINED feds keep the
  grandfathered proxy path.
- **SETTLED (direction; the 5.1 spec owns the numbers): the loop MAY auto-join a
  candidate that passed the structural floor, bounded and disclosed.** Without
  auto-join, a freshly discovered federation could never reach `active_probe == Passed`
  unattended and the phase gate below would be unsatisfiable. A join moves NO money (an
  authenticated config fetch + a client partition — bounded local surface), lands in
  the ledger as a `join` row with `actor: Agent` (ADR-0007's disclose-not-consent
  posture: visible, bounded autonomy), and is THROTTLED: a CONCURRENT cap on
  auto-joined-but-not-yet-Passed candidates plus a per-week auto-join budget (numbers
  in the 5.1 spec). Stated honestly: with one-way joins these caps bound the RATE, not
  the lifetime total — a long-running wallet still accumulates joined-candidate
  partitions over months. The 5.1 spec must pick the lifetime bound: a hard lifetime
  auto-join cap (simplest), or a partition-eviction path for never-passed candidates
  (new machinery; currently a non-goal). Manual joins are unaffected and uncounted.

## 5.2 — the self-running loop (plan-level; buildable spec after 5.1)

- `wallet-cli watch`: interval ticks + a reactive `federation_expiry_timestamp`
  subscription (the corroborated signal from §15.1) + probe scheduling off the verdict
  TTLs, all through the SAME tick/probe verbs (the loop adds no new money paths).
- **Budgets:** a global probe budget (attempts/week and sats/week) enforced here — the
  scheduler is the only unattended probe initiator; manual `probe` stays un-budgeted.
- Every scheduled action lands in the ledger as `Agent { occurrence }` — ADR-0014's
  auditability is already the substrate.
- **Gate (the phase exit):** discover → structural floor → active probe → score →
  rebalance runs unattended against devimint, fully reconstructible from `history`;
  a candidate failing ONLY the active probe is never funded.

## Non-goals (Phase 5)

No UI (Phase 6), no on-chain peg-out, no reputation scoring from Nostr ratings (dropped
per the data-sources spec), no probe-result sharing/gossip, no multi-gateway probe
matrices (one validated shared route suffices to pass), no removal/eviction of joined
federations (one-way joins stay; an unfunded fed is inert).
