---
status: accepted
---
# Automated routing is never pinned; the operator keeps a break-glass

Two rules, drawn along a line the code did not previously have:

1. **Automated routing resolves only from the federation's vetted list.** The scheduler, the
   allocator, probes and evacuation may never be pinned to a gateway. The `gateway` key is
   removed from `walletd.toml`, so a daemon cannot express one.
2. **The operator keeps a single-invocation break-glass, on money verbs only.** `wallet-cli
   --standalone --gateway <url>` survives for the verbs that move money at an operator's explicit
   instruction, and deliberately routes through a gateway **outside** the vetted list — skipping
   vetted-list membership and the serves-check, though not the operation's own liveness check or
   the fee cap (see Consequences). Three dispositions, because "money verbs only" and a five-verb reject list are *different sets*
   and the gap between them is where implementations diverge:
   - **Accepted** on the verbs that route at an operator's direction: the four money verbs plus the provenance-eligible await verbs ("money verbs only" is shorthand
     that understates the accepted set: await verbs are eligible, they are not money verbs) — and this
     ADR must name them, since its own thesis is that unenumerated sets diverge — `pay`,
     `receive`, `move`, `direct-inflow`; plus the await verbs once their recovery is scoped and
     provenance-gated (see below) — and NAME THEM, for the same reason the money verbs are named:
     `await-receive`, `await-send`, `await-move` (`wallet-cli/src/main.rs:175`, `:184`, `:211`).
     `await-move` is the one every example uses, so an implementation accepting only it would pass
     a careless review while leaving an operator unable to re-drive a break-glass `pay` or
     `receive`. Test all three. `direct-inflow` is the classification an implementer is most
     likely to get wrong: it reads like plumbing, but it funds a federation and most devimint
     smokes fund through it, so putting it in either other bucket breaks the funding step.
     CONTEXT.md carries the same four under **Money verb**.
   - **Rejected**, loudly, on the verbs that route AUTOMATICALLY: `discover`, `probe`, `tick`,
     `status`, `reconcile`. Silently ignoring it there is the failure this rule exists to prevent.
   - **Ignored** on verbs that resolve no route at all — `join`, `balance`, `history`, `show`,
     `list-feds`, `policy`, `health`, `candidates`, `approve`, `recover`. The flag is global, so a
     helper script that appends it to every invocation must not break on these; there is nothing
     for it to mean and nothing it can mislead about. `smoke_money` depends on exactly this: its
     helper passes `--gateway` to `join` and `balance` (`smoke_money_devimint.sh:74`, `:77`,
     `:85`), which is why it is one of the two untouched smokes.

The asymmetry is the decision: the CLI has a flag the daemon config does not. That is intentional
and is the thing a future reader will otherwise assume was an oversight.

**Rule 1 binds the API, not just the CLI.** Rejecting `--gateway` on the automated verbs closes
the operator-facing door; it does not close the door itself. An in-process caller can still build
`Runtime::new(.., Some(gateway), ..)` and call the public `tick`. Route preflight also consults
that pin (`runtime.rs:3127-3129`), but it only produces a routability verdict and forwards
nothing; the pin reaches EXECUTION through `Runtime::executor()`, which builds
`FedimintExecutor::new(.., self.pinned_gateway.clone(), ..)` (`runtime.rs:592-596`) — that handoff,
not the preflight branch (which change 3 deletes), is what a structural gate must cover. Since this rule is stated
independently of process, the enforcement has to be there too: the automated entry points must
reject or clear an override, or the break-glass must live on a money-only runtime type that the
automated ones cannot carry. Whichever shape ships, it needs a test at that boundary — a CLI-level
test cannot observe this.

**Rule 1 is about the decision, not the process.** "Automated" means the scheduler/allocator/probe
machinery wherever it runs — including when a human starts it from a terminal. `wallet-cli
--standalone tick` runs the same allocator that walletd runs; `probe` and `discover` write the
same health signals the allocator reads; and `reconcile` re-drives EVERY pending intent
(`wallet-cli/src/main.rs:681`, `:1590`), allocator-created moves and evacuations included, so a
flag on it pins automated money operations wholesale. Letting a flag reach those is the daemon pin under
another name: a stale `--gateway` on a standalone `tick` can suppress healthy vetted routes, mark
a destination unusable, and force an evacuation route. The flag survives only where a human is
directing a specific payment, not where they are starting a machine that decides for itself.

**The await verbs are the same backdoor, and banning the flag there would be the wrong fix.**
`await_standalone` calls `client.reconcile()` unconditionally as its first step
(`wallet-cli/src/main.rs:1719`) — a full re-drive of every pending intent, not just the awaited
key — so `--gateway <url> await-move <key>` pins the re-drive of every pending allocator move and
evacuation, exactly the hazard `reconcile` is rejected for. But rejecting the flag on await would
stop an operator awaiting the very payment they just made with the break-glass, and 13 of the 15
devimint smokes call an await verb through a `--gateway` helper (`smoke_daemon`'s awaits go
through the deliberately flag-free client-mode helper, and `smoke_devimint.sh` calls
`await-move` with no flag at all). So: the await verbs KEEP the
flag, and `await_standalone`'s recovery is SCOPED TO THE REQUESTED KEY instead of re-driving
everything. That removes the *wholesale* hazard, but not all of it: if the requested key names a
scheduler- or allocator-created `Move` or `Evacuate`, scoping still re-drives THAT automated
intent through an executor carrying the override. So the override applies only when the target
intent was USER-INITIATED; awaiting an allocator's own operation must not carry it.

**Enumerating automated entry points is the WRONG fix — separate the override structurally.**
This ADR has now counted the boundary three times and been wrong each time. It said `tick`; then
`tick` and `reconcile`; then `tick`, `reconcile` and `active_probe` (`runtime.rs:593`,
`:1758-1766`, `:2190-2220`). It is still short: `Runtime::watch_once` (`runtime.rs:1088`) is
public and composes all three plus the discover pass, and `FedimintExecutor` is publicly
re-exported with a public constructor that takes the override, so `Executor::perform` can be
handed an allocator-created `Move` or `Evacuate` directly, past every `Runtime` method.
An enumeration cannot close a set that keeps growing — which is this ADR's own thesis about
"money verbs only" turned on itself.
So the requirement is stated as a PROPERTY, not a shape: automated routing must never observe the
override, and that is proven at the executor boundary — the one place every route resolution
funnels through — with a direct `Executor::perform` call on an allocator-created action under an
active override. Whether the implementation achieves it with a money-only type, constructor
separation, or clearing the override at the automated entry points is the implementer's call; the
test is what binds, and it is the test an enumeration cannot pass by accident. A direct `Executor::perform` test on an allocator-created action is
the one that proves the structural version.

**Do not implement "user-initiated" as an `Actor` check.** A manually invoked probe calls
`active_probe(.., Actor::User)` (`wallet-cli/src/main.rs:1106`) yet stamps its move legs
`ReasonCode::ActiveProbe` (`wallet-fedimint/src/runtime.rs:2234`) — so an actor test says "user"
for a leg the probe lane created, and `--gateway await-move <probe-leg-key>` would reopen exactly
the backdoor this ADR closes. The gate is the leg's REASON CODE, never the actor that triggered
the enclosing command. Test it with a manual probe leg, because that is the case where the two
disagree.

THREE outcomes, not two — the target's provenance decides which, and the middle one is easy to
miss:
  - `ReasonCode::UserInitiated` on an intent that RESOLVES A ROUTE → the override APPLIES.
  - `ReasonCode::UserInitiated` on an intent that resolves no route → the override APPLIES AND
    NO-OPS. `Join` (`wallet-fedimint/src/runtime.rs:1025-1030`) and `Action::Recover` are both in
    this class; prefer testing route resolution GENERICALLY over enumerating actions, because
    `smoke_recover` drives standalone recovery through an always-`--gateway` helper and resumes it
    with `await-move`, so a `Join`-only implementation would refuse the pending recovery. This mirrors
    the Ignored bucket at the verb level and it is load-bearing: `smoke_money`'s `join_fed`
    helper runs `await-move <join-key>` through a `--gateway` helper
    (`wallet-cli/tests/smoke_money_devimint.sh:74`, `:77-79`), so refusing here breaks one of the
    two smokes this ADR promises stay untouched.
  - Any AUTOMATED reason code (`ActiveProbe`, allocator- or scheduler-stamped) → REFUSED, loudly.
Refusal is reserved for automated provenance. Do not refuse merely because an intent is not a
money action.

**`ReasonCode` is a caller-supplied field, so it is a DISPATCH rule, not a security boundary.**
An in-process caller can stamp an allocator-shaped `Move` — or an `Evacuate` — `UserInitiated`
and the reason-code check will then apply the override. That is not the structural guarantee this
ADR asks for above, and the two must not be confused: the reason code decides what an
already-authorised override applies to; it cannot decide whether the caller was entitled to one.
The entitlement has to come from something a caller cannot forge — the override travelling as a
money-only capability or type that only the money-verb construction path can produce, rather than
as an `Option<GatewayUrl>` any constructor may pass. Build that, and the reason-code rule becomes
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
about which it meant: `wallet-daemon/src/config.rs:36-37` called it "a deployment fact, not user
policy", while `wallet-fedimint/src/executor.rs:279` called it "an operator pin [that] overrides
route selection entirely, planning included".

That ambiguity propagated. A bead specifying evacuation's second route had to decide what a
source-only, destination-only, shared, or unusable pin meant for automatic fallback — four cases,
each with money consequences, on a path no operator had ever pinned. Removing the daemon pin
deletes all four questions instead of answering them.

**The daemon pin's cost was not limited to route selection.** A pinned daemon hands the pin to
every federation probe; probing then validates only that gateway and never scans the registered
list (`probe.rs:458`). A failure surfaces as `probed_ok: false`, and the allocator drops
that federation as an evacuation destination (`allocator.rs:444` — `receive_blocker`'s `probed_ok` gate — and `:475`,
`eligible_for_evacuation`). So a pin that served
one end — or a stale one serving neither — meant **no `Action::Evacuate` was ever emitted**, while
executor-level tests would pass. A knob that can silently disable evacuation has no business
being a standing configuration.

**But deleting the flag as well would have removed an incident capability the runbook depends
on.** Explicit-gateway `send`/`receive` skip the vetted list and check only `routing_info`
(`fedimint-lnv2-client/src/lib.rs:574-587`), so the flag is the *only* way this wallet reaches an
unvetted gateway. The runbook's gateway-outage entry says "moving funds is a manual operation"
(`docs/real-sats-pilot-runbook.md:290-295`) — and with a dead or empty vetted list, a manual verb
*without* `--gateway` fails exactly as the automated path does. The documented remedy silently
depended on the flag.

The incident it covers is specific and unpleasant: a **live but unadministered federation whose
vetted gateways are all dead**. Consensus still redeems the ecash. Only a guardian can
`gateways add`. Without the break-glass the operator's remaining option is to ship a code
release.

**Nothing deployed relies on the daemon pin.** The production `walletd.toml` is a ConfigMap
carrying exactly `data_dir`, `address`, `port`, `token_path`, `log_level`
(`/home/master/newmachine/remote-devops/k8s/argo/walletd/walletd.yaml:24-29` — an EXTERNAL
checkout, not a path in this repo). Production has always run unpinned, so nothing deployed
can be relying on the pin today. Note what that does and does not establish about the vetted
lists: a production pay runs with `gateway: None` (`wallet-daemon/src/handlers.rs:313`) and scans
the SOURCE federation's list (`executor.rs:1024-1027`), so the 2026-07-28 canary proves ONE
federation's list serves — not every joined federation's. Confirming the rest is a pre-flight
check the implementing bead owns, not a fact this ADR may assume.

## Consequences

- **The break-glass skips PRESELECTION only — VETTED-LIST membership, and that is the point — it is not
  unvalidated.** Precisely: `resolve_move_gateway` returns the named gateway without checking it
  **serves** the route, so neither vetted-list membership nor the two-ends check applies.
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
  `routing_info` to answer (`fedimint-lnv2-client/src/lib.rs:574-587`) — and the fee cap, which
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
  routing skipping the serves-check was a defect; here it is the required behaviour. Do not
  "fix" it into a serves-check: `gateway_serves_route` validates BOTH ends through the gateway
  (`executor.rs:459-469`), which refuses exactly the one-end-only or half-responsive gateway the
  break-glass exists to reach.
- **"On the vetted list" is NOT a threshold-vetted property, and this ADR must not be read as
  claiming it is.** `gateways()` builds a UNION of the peer responses
  (`fedimint-lnv2-client/src/api.rs:84-116`): `FilterMapThreshold` thresholds the RESPONSE COUNT,
  then every URL any responding guardian returned is flattened into one set. So a single guardian
  — Byzantine, compromised, or merely misconfigured — can put a gateway into the candidate list
  that no threshold ever admitted. Partial mitigation, worth knowing: the SDK sorts the union by
  how many peers LACK each URL, so a one-guardian entry sorts last and a wallet taking the first
  serving candidate will normally prefer a widely-vetted one. It is a preference, not a bound —
  if the better-vetted gateways do not serve, the one-guardian entry is reachable.
  Selection by largest sized net carries no support term, so once a scan covers the class this
  preference stops deciding anything — which is why threshold-supported membership is the real
  fix and is named as the follow-up.


- **Restoring automated movement after a vetted-list failure is guardian-side.** `gateways add`
  is a per-guardian authenticated write, not a consensus item
  (`fedimint-lnv2-server/src/lib.rs:696-704`), and a client's view unions the first threshold of
  peer replies — so it must be run against *every* guardian or the gateway appears
  nondeterministically. An operator with no guardian cooperation cannot repair the list at all;
  the break-glass moves money in the meantime, it does not fix the federation.
- **Every devimint smoke that routes today pins.** `br-remove-gateway-pin-yjw` carries the
  measured split and converts them; it is the single authority for that count, which has drifted
  every time it was restated in two places.
- **The responsiveness gate is the awkward case.** It pins a never-responding double *because* the
  pin skips validation. Converting it to the vetted list means the double must answer
  `routing_info` and hang only on payment endpoints, which in turn breaks its accept-level timing
  oracle: HTTP connections are pooled and reused (`fedimint-connectors/src/http.rs:57-61`), so a
  request-level double is required. Budgeted in `br-remove-gateway-pin-yjw`, not discovered later.
- **The break-glass is deliberately NON-DURABLE, and does not travel on the action.** It is easy
  to assume `Action::Pay { gateway }` / `Receive { gateway }` carry it. They do not: every
  production constructor passes `gateway: None` (`wallet-cli/src/main.rs:1460`, `:1545`;
  `wallet-daemon/src/handlers.rs:313`, `:382`), and the only code that can set `Some` has no
  production callers. The flag reaches the money verbs through the executor's fallback,
  `gateway.clone().or_else(|| self.pinned_gateway.clone())` (`executor.rs:1024`, `:1135`), whose
  own comment records the choice: "The pin is deliberately NOT journaled into the intent, so a
  pin change applies to re-drives after a restart" (`executor.rs:1019-1021`).
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
