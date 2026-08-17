---
status: accepted
---
# Automated routing is never pinned; the operator keeps a break-glass

> **Current-tree anchor note (2026-08-16).** Current claims use durable symbol
> anchors: `Runtime::{executor,route_gateway_candidates,reconcile,active_probe,watch_once}`,
> `FedimintExecutor::{perform,resolve_move_gateway,validate_move_gateway_before_receive}`,
> `drive_intent_step`, and `await_standalone`. Exact line citations are used only
> for historical evidence that names its historical commit; they are not
> current-tree implementation anchors.

> **Implementation status — target state, not shipped.** The current daemon configuration still
> exposes `WalletdConfig::gateway` and wires that pin into `Runtime`; automated routes can
> therefore still be pinned today. `br-remove-gateway-pin-yjw` owns removal of that config key and
> the structural automated-routing gateway. The rules below are the accepted destination, not a
> description of current behavior.

Two rules, drawn along a line the code did not previously have:

1. **Automated routing resolves only from the federation's vetted list.** In the target state, the
   scheduler, allocator, probes and evacuation may never be pinned to a gateway. The `gateway` key
   will be removed from `walletd.toml`, so a daemon cannot express one.
2. **The operator keeps a single-invocation break-glass, on money verbs only.** `wallet-cli
   --standalone --gateway <url>` survives for the verbs that move money at an operator's explicit
   instruction, and deliberately routes through a gateway **outside** the vetted list — skipping
   vetted-list membership and the two-end PRESELECTION, though not the operation's own liveness
   check, the fee cap, or ECONOMIC VIABILITY (see Consequences: `Serves` folds viability into the
   word, so "skips the serves-check" must never be read as licensing a move that costs more than
   it delivers). Three dispositions, because "money verbs only" and a five-verb reject list are *different sets*
   and the gap between them is where implementations diverge:
   - **Accepted** on the verbs that route at an operator's direction: the four money verbs plus the provenance-eligible await verbs ("money verbs only" is shorthand
     that understates the accepted set: await verbs are eligible, they are not money verbs) — and this
     ADR must name them, since its own thesis is that unenumerated sets diverge — `pay`,
     `receive`, `move`, `direct-inflow`; plus the await verbs once their recovery is scoped and
     provenance-gated (see below) — and NAME THEM, for the same reason the money verbs are named:
     `await-receive`, `await-send`, `await-move`.
     `await-move` is the one every example uses, so an implementation accepting only it would pass
     a careless review while leaving an operator unable to re-drive a break-glass `pay` or
     `receive`. Test all three.
     **The re-drive case needs a DURABLE origin marker, which does not exist yet.** A capability
     proves only that THIS invocation came through an operator-facing path. The target `Intent`
     persists `Action`, `ReasonCode` and `actor` — all publicly constructible, and the override is
     deliberately non-durable — so on a later `--gateway await-*` a forged `UserInitiated`
     allocator move is indistinguishable from the manual move an operator is legitimately
     re-driving, and the forged-provenance criterion cannot be met by inspecting the intent.
     There is ONE exit, not two: `br-remove-gateway-pin-yjw` must persist an AUTHENTICATED origin
     marker when a money verb creates the intent, and gate the re-drive on it. SPECIFY ITS ABSENT
     CASE, AND BIND IT TO THE INTENT: a marker that authenticates only "operator-originated" is
     replayable — a caller can clone a legitimate operator intent, swap in an allocator-shaped
     `Move` with `UserInitiated`, keep the marker, and pass the forged-provenance test while
     restoring the override. Bind the proof to immutable intent identity (the idempotency key and
     the action it authorises), so it cannot be lifted onto a different intent. On the absent
     case: existing persisted `Intent` rows carry only `Action`, `ReasonCode` and `actor`, so a REQUIRED
     field makes pre-upgrade pending rows fail to decode and be skipped by reconcile — the same
     trap as the `MoveMeta` cap. Optional with a named default, and absent means NOT
     operator-originated, so a legacy row cannot silently inherit the override. UPGRADE WINDOW: a
     standalone `--gateway` operation pending ACROSS the upgrade necessarily has no marker, so
     `await-* --gateway` on it will not re-drive. That is the safe direction, but it must be
     stated in the runbook rather than surprising an operator mid-incident — re-issue rather than
     await. "Narrow acceptance
     so await never carries the override" is NOT available — this ADR settles that the await verbs
     KEEP the flag, and `smoke_money` depends on it. Until the marker ships, do not call await
     re-drives provenance-safe. `direct-inflow` is the classification an implementer is most
     likely to get wrong: it reads like plumbing, but it funds a federation and most devimint
     smokes fund through it, so putting it in either other bucket breaks the funding step.
     CONTEXT.md carries the same four under **Money verb**.
   - **Rejected**, loudly, on the verbs that route AUTOMATICALLY: `discover`, `probe`, `tick`,
     `status`, `reconcile`. Silently ignoring it there is the failure this rule exists to prevent.
   - **Ignored** on verbs that resolve no route at all — `join`, `balance`, `history`, `show`,
     `list-feds`, `policy`, `health`, `candidates`, `approve`, `recover`. The flag is global, so a
     helper script that appends it to every invocation must not break on these; there is nothing
     for it to mean and nothing it can mislead about. `smoke_money` depends on exactly this: its
     helper passes `--gateway` to `join` and `balance` (the `wcli`, `join_fed`, and
     `balance_msat_for_fed` helpers in `smoke_money_devimint.sh`), which is why it is one of the
     two untouched smokes.

The asymmetry is the decision: the CLI has a flag the daemon config does not. That is intentional
and is the thing a future reader will otherwise assume was an oversight.

**Rule 1 binds the API, not just the CLI.** Rejecting `--gateway` on the automated verbs closes
the operator-facing door; it does not close the door itself. An in-process caller can still build
`Runtime::new(.., Some(gateway), ..)` and call the public `tick`. Route preflight also consults
that pin (`Runtime::route_gateway_candidates`), but it only produces a routability verdict and
forwards nothing; the pin reaches EXECUTION through `Runtime::executor`, which builds
`FedimintExecutor::new(.., self.pinned_gateway.clone(), ..)` — that handoff,
not the preflight branch (which change 3 deletes), is what a structural gate must cover. Since this rule is stated
independently of process, the enforcement has to be there too — and it must be STRUCTURAL: the
break-glass lives on a money-only type the automated callers cannot carry. Clearing the override
at each automated entry point does not qualify (see below: `watch_once` composes them and
`FedimintExecutor` is publicly constructible, so the enumeration is bypassable). Whichever
structural shape ships, it needs a test at that boundary — a CLI-level test cannot observe this.

**Rule 1 is about the decision, not the process.** "Automated" means the scheduler/allocator/probe
machinery wherever it runs — including when a human starts it from a terminal. `wallet-cli
--standalone tick` runs the same allocator that walletd runs; `probe` and `discover` write the
same health signals the allocator reads; and `Runtime::reconcile` re-drives EVERY pending intent,
allocator-created moves and evacuations included, so a
flag on it pins automated money operations wholesale. Letting a flag reach those is the daemon pin under
another name: a stale `--gateway` on a standalone `tick` can suppress healthy vetted routes, mark
a destination unusable, and force an evacuation route. The flag survives only where a human is
directing a specific payment, not where they are starting a machine that decides for itself.

**The await verbs are the same backdoor, and banning the flag there would be the wrong fix.**
`await_standalone` currently calls `client.reconcile_durable()` as its first step — durable
rehydration still re-drives every pending intent, not just the awaited key — so
`--gateway <url> await-move <key>` can pin the re-drive of every pending allocator move and
evacuation, exactly the hazard `reconcile` is rejected for. But rejecting the flag on await would
stop an operator awaiting the very payment they just made with the break-glass, and 13 of the 15
devimint smokes call an await verb through a `--gateway` helper (`smoke_daemon`'s awaits go
through the deliberately flag-free client-mode helper, and `smoke_devimint.sh` calls
`await-move` with no flag at all). The required correction therefore keeps the await flag but
scopes `await_standalone` recovery to the requested key instead of re-driving everything. That
removes the *wholesale* hazard, but not all of it: if the requested key names a
scheduler- or allocator-created `Move` or `Evacuate`, scoping still re-drives THAT automated
intent through an executor carrying the override. So the override applies only when the target
intent was USER-INITIATED; awaiting an allocator's own operation must not carry it.

**Enumerating automated entry points is the WRONG fix — separate the override structurally.**
This ADR has now counted the boundary three times and been wrong each time. It said `tick`; then
`tick` and `reconcile`; then `tick`, `reconcile` and `active_probe`
(`Runtime::{executor,reconcile,active_probe}`). It is still short: `Runtime::watch_once` is
public and composes all three plus the discover pass, and `FedimintExecutor` is publicly
re-exported with a public constructor that takes the override, so `FedimintExecutor::perform` can be
handed an allocator-created `Move` or `Evacuate` directly, past every `Runtime` method.
An enumeration cannot close a set that keeps growing — which is this ADR's own thesis about
"money verbs only" turned on itself.
So the requirement is a PROPERTY: automated routing must never observe the override, proven at
the executor boundary — the one place every route resolution funnels through — by a direct
`FedimintExecutor::perform` call on an allocator-created action under an active override. The shape is the
implementer's call among those that make the override STRUCTURALLY unavailable to automated
callers (a money-only type, or construction-path separation). NOTE THE SECOND DOOR: public
`Action::Pay` / `Action::Receive` values can themselves carry `gateway: Some(..)`, which
`drive_intent_step` PREFERS via `gateway.clone().or_else(|| self.pinned_gateway.clone())`, so an
in-process caller can journal a durable unvetted gateway
without touching `pinned_gateway` at all. Closing only the runtime/executor field leaves that
path open — the gate must cover the action-carried value too. Clearing it at each automated entry
point is NOT among them: that is the enumeration this section rejects, and `FedimintExecutor` is
publicly constructible, so it would leave the test red by construction. A direct
`FedimintExecutor::perform` test on an allocator-created action is
the one that proves the structural version.

**Do not implement "user-initiated" as an `Actor` check.** A manually invoked probe calls
`Runtime::active_probe(.., Actor::User)` yet stamps its move legs
`ReasonCode::ActiveProbe` — so an actor test says "user"
for a leg the probe lane created, and `--gateway await-move <probe-leg-key>` would reopen exactly
the backdoor this ADR closes. The gate is the leg's REASON CODE, never the actor that triggered
the enclosing command. Test it with a manual probe leg, because that is the case where the two
disagree.

THREE outcomes, not two — the target's provenance decides which, and the middle one is easy to
miss:
  - `ReasonCode::UserInitiated` on an intent that RESOLVES A ROUTE → the override APPLIES.
  - `ReasonCode::UserInitiated` on an intent that resolves no route → the override APPLIES AND
    NO-OPS. `FedimintExecutor::perform`'s `Action::Join` and `Action::Recover` arms are both in
    this class; prefer testing route resolution GENERICALLY over enumerating actions, because
    `smoke_recover` drives standalone recovery through an always-`--gateway` helper and resumes it
    with `await-move`, so a `Join`-only implementation would refuse the pending recovery. This mirrors
    the Ignored bucket at the verb level and it is load-bearing: `smoke_money`'s `join_fed`
    helper runs `await-move <join-key>` through a `--gateway` helper
    (the `wcli` and `join_fed` helpers in `wallet-cli/tests/smoke_money_devimint.sh`), so
    refusing here breaks one of the two smokes this ADR promises stay untouched.
  - Any AUTOMATED reason code (`ActiveProbe`, allocator- or scheduler-stamped) → REFUSED, loudly.
Refusal is reserved for automated provenance. Do not refuse merely because an intent is not a
money action.

**`ReasonCode` is a caller-supplied field, so it is a DISPATCH rule, not a security boundary.**
An in-process caller can stamp an allocator-shaped `Move` — or an `Evacuate` — `UserInitiated`
and the reason-code check will then apply the override. That is not the structural guarantee this
ADR asks for above, and the two must not be confused: the reason code decides what an
already-authorised override applies to; it cannot decide whether the caller was entitled to one.
The entitlement has to come from something a caller cannot forge — the override travelling as a
money-only capability or type that only the OPERATOR-FACING construction paths can produce —
the four money verbs AND the three provenance-eligible await verbs, which must be able to
construct it or they cannot re-drive a break-glass operation — rather than an
`Option<GatewayUrl>` any constructor may pass. The restriction excludes AUTOMATED callers, not
await callers. Build that, and the reason-code rule becomes
what it should be: routing logic on top of a boundary that already holds.

With that, the rule follows the principle above — awaiting one named operation *is* a human
directing a specific payment, but only when the payment was theirs to direct.

## Why

**A pin and a break-glass were the same field, and that conflation cost real design work.**
This ADR SUPERSEDES Q4 of
[docs/archive/route-economics-decisions.md](../archive/route-economics-decisions.md) outright,
and (with ADR-0029) supersedes that document's Q1 and Q2 in part.

`FedimintExecutor.pinned_gateway` was set from two places with opposite intents. The daemon's
`walletd.toml` key made it a standing property of every automated decision. The CLI flag made it
an operator's one-off. The codebase described both in the same breath and contradicted itself
about which it meant: the daemon configuration called it "a deployment fact, not user policy",
while the executor called it "an operator pin [that] overrides route selection entirely, planning
included".

That ambiguity propagated. A bead specifying evacuation's second route had to decide what a
source-only, destination-only, shared, or unusable pin meant for automatic fallback — four cases,
each with money consequences, on a path no operator had ever pinned. Removing the daemon pin
deletes all four questions instead of answering them.

**The daemon pin's cost was not limited to route selection.** A pinned daemon hands the pin to
every federation probe; `Runtime::active_probe` then validates only that gateway and never scans
the registered list. A failure surfaces as `probed_ok: false`, and the allocator drops
that federation as an evacuation destination (`allocator::receive_blocker`'s `probed_ok` gate
and `allocator::eligible_for_evacuation`). So a pin that served
one end — or a stale one serving neither — meant **no `Action::Evacuate` was ever emitted**, while
executor-level tests would pass. A knob that can silently disable evacuation has no business
being a standing configuration.

**But deleting the flag as well would have removed an incident capability the runbook depends
on.** Explicit-gateway `send`/`receive` skip the vetted list and check only `routing_info`, so the
flag is the *only* way this wallet reaches an unvetted gateway. The runbook's gateway-outage entry
says "moving funds is a manual operation" — and with a dead or empty vetted list, a manual verb
*without* `--gateway` fails exactly as the automated path does. The documented remedy silently
depended on the flag.

The incident it covers is specific and unpleasant: a **live but unadministered federation whose
vetted gateways are all dead**. Consensus still redeems the ecash. Only a guardian can
`gateways add`. Without the break-glass the operator's remaining option is to ship a code
release.

**Nothing deployed relies on the daemon pin.** The production `walletd.toml` is a ConfigMap
carrying exactly `data_dir`, `address`, `port`, `token_path`, `log_level` (in an external checkout,
not a path in this repo). Production has always run unpinned, so nothing deployed can be relying
on the pin today. Note what that does and does not establish about the vetted lists: a production
pay runs with `gateway: None` and scans the SOURCE federation's list, so the 2026-07-28 canary proves ONE
federation's list serves — not every joined federation's. Confirming the rest is a pre-flight
check the implementing bead owns, not a fact this ADR may assume.

## Consequences

- **The break-glass skips PRESELECTION only — VETTED-LIST membership, and that is the point — it is not
  unvalidated.** Precisely: `FedimintExecutor::resolve_move_gateway` returns the named gateway
  without checking it
  **serves** the route, so vetted-list membership and the two-end PRESELECTION are skipped.
  LIVENESS IS NOT: `FedimintExecutor::validate_move_gateway_before_receive` runs unconditionally
  at CreateInvoice, pin included, requiring the gateway to answer
  `routing_info` for the SOURCE federation, and the pre-mint gross-up quotes the destination end —
  so a one-end-only gateway is refused BEFORE anything is minted. Do not describe the break-glass
  as a way to "reach" a one-end-only gateway: it is not, at this pin lnv2 `send` needs the
  source's `routing_info` anyway, and an implementer making that description true by deleting the
  pre-mint check would reintroduce the stranded-unpayable-invoice case that check exists to
  prevent.
  **BUT NOT ECONOMIC VIABILITY.** `Serves` in CONTEXT.md now folds `total_fee <= delivered net`
  into the word, so "skips the serves check" would otherwise authorise bypassing a money-safety
  condition rather than just preselection.
  The viability test must apply on the break-glass path, and today it does not: `total_fee <=
  net` arrives with br-y2j's post-check, which is scoped to evacuation sizing. It matters for a
  small manual `move` under the default cap, where a named gateway can charge more than the net
  while still passing the cap. OWNED BY `br-remove-gateway-pin-yjw`, which carries the
  break-glass — not intent without an owner.
  What
  still applies is the operation's own liveness check — explicit-gateway `send`/`receive` require
  `routing_info` to answer — and the fee cap, which
  is re-checked at the Pay step regardless of how the route was chosen. So a dead gateway still fails. An overpriced
  one *usually* fails — but not atomically, and the difference matters for an UNVETTED gateway:
  the executor quotes `routing_info`, then `MultiClient::pay` has lnv2 `send` fetch `routing_info`
  AGAIN before committing the contract, with no post-commit local cap re-check. A hostile gateway
  can therefore answer cheaply at quote time and dearly at commit time, and the pinned SDK's
  lexicographic `PaymentFee` comparison will not catch the second answer.
  RESIDUAL, stated rather
  than papered over: the cap bounds what the wallet will knowingly agree to, not what a gateway
  outside the vetted list can charge between quote and commit. Vetting is what normally covers
  that, which is precisely what the break-glass sets aside. What an operator overrides is the
  federation's judgement about *which* gateways are admissible; they also accept this residual. For automated
  routing skipping the PRESELECTION was a defect; here it is the required behaviour. Do not "fix"
  it by replacing the override with registered-list enumeration
  (`Runtime::route_gateway_candidates`) or by adding the separate two-end
  `FedimintExecutor::gateway_serves_route` precheck: the former would
  re-impose the membership test the override exists to step around, while the latter would add a
  preflight that the direct operation performs itself. This is NOT a licence to reach a
  one-end-only gateway — the
  operation's own liveness checks still refuse one before anything is minted (see the
  break-glass paragraph above), and they must stay.
- **"On the vetted list" is NOT a threshold-vetted property, and this ADR must not be read as
  claiming it is.** `gateways()` builds a UNION of the peer responses;
  `FilterMapThreshold` thresholds the RESPONSE COUNT,
  then every URL any responding guardian returned is flattened into one set. So a single guardian
  — Byzantine, compromised, or merely misconfigured — can put a gateway into the candidate list
  that no threshold ever admitted. Partial mitigation, worth knowing: the SDK sorts the union by
  how many peers LACK each URL, so a one-guardian entry sorts last and a wallet taking the first
  serving candidate will normally prefer a widely-vetted one. It is a preference, not a bound —
  if the better-vetted gateways do not serve, the one-guardian entry is reachable.
  Selection by largest sized net carries no support term, so once a scan covers the class this
  preference stops deciding anything — which is why threshold-supported membership is the real
  fix — owned by **`br-gw-threshold-membership-k4t`** (query per peer, require `2f+1`) so it cannot
  be lost when the routing beads finish. That bead also owns the INGESTION bound: `gateways()`
  materialises the whole union and computes support quadratically before any wallet-side scan
  limit applies, so a per-peer response bound belongs with the per-peer query path. It is
  deliberately NOT in the evacuation beads — a guardian set on stalling a client has cheaper
  levers (slow responses, timeouts, garbage) those beads correctly do not defend against either,
  and generic response hardening is client-wide, not evacuation spec.


- **Restoring automated movement after a vetted-list failure is guardian-side.** `gateways add`
  is a per-guardian authenticated write, not a consensus item, and a client's view unions the first threshold of
  peer replies. `2f+1` registrations are the minimum for deterministic visibility UNDER THE
  BYZANTINE MODEL THIS ADR ALREADY ASSUMES. `f+1` is not enough: the intersection argument only
  puts a registered peer in the quorum, not a registered peer that ANSWERS TRUTHFULLY — with
  `n=4, f=1`, registering on {A, B} where A is Byzantine admits the quorum {A, C, D}, in which A
  returns an empty list and C, D were never registered, so the union loses the URL. At `2f+1` any
  `n−f` quorum contains at least `f+1` registered peers, so at least one honest registered peer
  answers. `f+1` remains sufficient for CRASH faults only; do not use it here. Requiring *every*
  guardian would make recovery impossible while one is unavailable, which is the incident this
  serves — so register on ALL REACHABLE guardians rather than a computed minimum.
  How many peers must SUPPORT a URL before it is admitted is a separate threshold from how many
  the operator writes to, and the two interact; `br-gw-threshold-membership-k4t` owns calibrating
  both against the fault model and the availability cost. An operator with no guardian cooperation cannot repair the list at all;
  the break-glass moves money in the meantime, it does not fix the federation.
- **Every devimint smoke that routes today pins.** `br-remove-gateway-pin-yjw` carries the
  measured split and converts them; it is the single authority for that count, which has drifted
  every time it was restated in two places.
- **The responsiveness gate is the awkward case.** It pins a never-responding double *because* the
  pin skips validation. Converting it to the vetted list means the double must answer
  `routing_info` and hang only on payment endpoints, which in turn breaks its accept-level timing
  oracle: HTTP connections are pooled and reused, so a
  request-level double is required. Budgeted in `br-remove-gateway-pin-yjw`, not discovered later.
- **The break-glass is deliberately NON-DURABLE, and does not travel on the action.** It is easy
  to assume `Action::Pay { gateway }` / `Receive { gateway }` carry it. They do not: every
  production constructor passes `gateway: None`, and the only code that can set `Some` has no
  production callers. The flag reaches the money verbs through the executor's fallback,
  `gateway.clone().or_else(|| self.pinned_gateway.clone())` in `drive_intent_step`, whose own
  comment records the choice: "The pin is deliberately NOT journaled into the intent, so a pin
  change applies to re-drives after a restart".
  That is the correct semantic for an incident override — it applies to the invocation and to
  re-drives under the same flag, and vanishes when the operator stops passing it. Two things
  follow, and both matter to an implementer: the `.or_else(pinned_gateway)` fallback is NOT dead
  code and must not be deleted, and the flag must NOT be journaled into intents to make a
  "durable break-glass" story true.
  SCOPE THE NON-DURABILITY TO PRE-COMMIT SELECTION. "An operator repeating the operation must
  repeat the flag" is right about CHOOSING a route and wrong once an operation has committed:
  from that point the PERSISTED ROUTE is authoritative and replays over it
  (CONTEXT.md's "Once an operation has committed" entry, br-s0e requirement 5). If a committed
  break-glass `Move` had to re-derive its route from a flag, a restart would either re-resolve
  under an already-committed invoice or stall because the unvetted gateway is not on any list.
  So: no flag, no route selection; but a recorded route replays without one.
  ORDERING CAVEAT: `br-remove-gateway-pin-yjw` lands BEFORE `br-s0e`, and today's backfill retains
  neither the source gateway nor the route kind. So between those two beads a break-glass `Move`
  that commits its receive and then crashes has no recorded route to replay — it re-resolves from
  the vetted list or stalls. Either the route persistence lands with the pin-removal work, or that
  bead states this window explicitly rather than inheriting a guarantee that is not yet true.
- **This does not make routing reliable**, and no document should say so. A federation whose
  guardians vet no reachable gateway is unroutable automatically, and the honest operator
  statement is that the break-glass buys manual movement while the list is repaired.

## Alternatives rejected

- **Delete both pins.** Simplest end state, and it was the original plan. Rejected because it
  removes the only wallet-side escape from a dead vetted list while leaving the runbook that
  depends on it unchanged — trading a real incident capability for tidiness on a wallet holding
  real sats.
- **Keep both.** Leaves `br-s0e` specifying a four-case pin table, pin-bypass rules and four
  pin-case tests on money code, to govern a knob no deployment sets — and leaves a standing
  config able to suppress evacuation entirely.
- **Split into two named settings** (a reachability hint and a hard override). More mechanism than
  the problem needs: the daemon side has no user at all, so the honest split is "delete one, keep
  the other", not "keep both under better names".
