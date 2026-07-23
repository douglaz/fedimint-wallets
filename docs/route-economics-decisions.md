# Route economics (`route_economics_by_pair`): the five settled questions

Status: DESIGN DECISION (br-ljj.3). No production code ships from this doc. It authorises an
implementation bead. All file:line refs verified against current `main` (post br-ljj.2).

## Recap of what was already locked (not relitigated)
Ordered-pair-keyed floor `route_economics_by_pair[(from,to)]` carrying `resolved_gateway`,
`min_viable_amount`, and status `Routable | Unroutable | UneconomicAtAnySize`; IO-supplied into
the snapshot so `decide()` stays pure; NO closed form (approximations must be explicit UPPER
bounds — over-blocking self-heals, under-blocking churns forever); computed fresh every tick
(~4 quotes/pair, 2 pairs); O(N²) is a non-issue; planning preselects the route and `perform()`
honours it. Today the snapshot is fee-thin: only flat `max_fee`, proportional
`max_fee_bps_of_move`, and the protocol `min_move` constant reach `decide()` (tick.rs:198-212);
`ProbeResult` carries only `gateway_available: bool` (probe.rs:68-72). Any per-pair economics is
genuinely new IO-supplied snapshot state.

---

## Q1 — Where does the preselected gateway live?
**Decision: add `gateway: Option<GatewayUrl>` to `Action::Move` (and `Action::Evacuate`),
stamped by `decide()` from the pair's `resolved_gateway`.**

Evidence: `Action::Move` (types.rs:133-138) and `Evacuate` (:142-147) have no gateway field;
`Action::Pay` (:151-158) and `Action::Receive` (:161-167) already carry
`gateway: Option<GatewayUrl>`. So gateway-on-Action is the established shape for money-moves;
Move/Evacuate are simply the two the allocator emits rather than the user. `OperationKind::Move`
already persists `gateway: Option<GatewayUrl>` (ledger.rs:121-129), so the durable ledger format
already accommodates a Move gateway. At perform, `assemble_record` (executor.rs:199) already has
a "use the cached/recovered gateway if present, else resolve" branch (:222-232, resolve at :231)
— the preselected `action.gateway` slots straight into that branch.

Rejected alternatives: putting it on `Intent`-but-not-`Action` (decide() is where the pair —
and now, given `resolved_gateway` in the snapshot, the gateway — is chosen; splitting the
selection off Action breaks the Pay/Receive precedent); a side table (loses the plan↔perform
coupling the whole design exists to create).

**Persistence blast radius (must be in the impl bead):** `Action` and `Intent` are both
serde-persisted (executor.rs:21, types.rs:120) and rows are re-decoded on read. A new
`gateway` field on `Action::Move` enters the durable Intent format; existing Intent rows lack
it, so the field MUST be `#[serde(default)]` (same forward-compat rule as the br-ljj.2 Policy
field and br-nsx's `max_fee_bps`), or old-row decode hard-fails.

## Q2 — Preselected gateway is dead at perform time (the central failure path)
**Decision: the preselected gateway is a HINT, not a hard constraint. At perform, use
`action.gateway` iff it still validates (serves both ends); otherwise re-resolve. Money safety
is the unchanged perform-time `fee_cap` on `Action::Move`, NOT gateway identity. Record the
gateway actually used in `OperationKind::Move.gateway` so any substitution is auditable.**

Rationale: `assemble_record`/`perform` can run much later than planning (retries, restarts).
The design removes planning↔perform *selection drift*, not liveness. Failing terminally on a
dead preselected gateway strands the move (worst outcome); blindly re-resolving reintroduces the
drift for money — but the `fee_cap` (proportional, already enforced at perform) means a
re-resolved gateway CANNOT overspend: if the substitute's combined fee exceeds `fee_cap`, the
cap check fails the move (Retryable → park/refuse), which is the correct, money-safe outcome. So
re-resolution is safe precisely because the cap, not the gateway, is the backstop. Prefer the
preselected gateway (avoids a re-quote and pins the economically-validated route in the common
case); fall back to the cheapest validating gateway (see the adjacent finding) under the same
cap when it's gone.

## Q3 — How is `min_viable_amount` searched?
**Decision: a FIXED small quote set per pair (~4: send-fed, send-gateway, recv-fed,
recv-gateway fees at a reference amount) → an explicit AFFINE UPPER-BOUND fee model (round every
component UP) → closed-form break-even for `min_viable_amount`. NO unbounded per-amount search
inside a tick. Hard-cap the quotes per pair per tick.**

Rationale: the fed fee is a non-decreasing STEP function of the contract amount, not affine
(fee.rs:92-95, :167-173), which is why `gross_up` needs a bounded binary search rather than a
closed form. But `min_viable_amount` does NOT need the exact fixed point — it needs a SAFE
floor, and the locked decision requires any approximation to be an explicit UPPER bound
(under-estimating the floor is the unsafe direction). Modelling the step fee by its affine upper
envelope (base + ppm rounded up) makes the modelled net ≤ true net ⇒ modelled `min_viable` ≥
true `min_viable` ⇒ the floor over-blocks slightly, which self-heals (a deferred top-up's
shortfall grows until it clears the floor). This keeps the RPC budget at the locked ~4
quotes/pair (2 pairs) with no in-tick search loop — the real hazard the question names
(unbounded async quoting per tick) is eliminated by construction. If a quote RPC fails this
tick, the entry is MISSING (see Q5), not a partial search.

## Q4 — Interaction with `pinned_gateway`
**Decision: `pinned_gateway` overrides route-economics selection entirely (highest precedence),
exactly as it overrides gateway selection today. When a pin is set, `route_economics_by_pair`
computes `resolved_gateway = pinned_gateway` and quotes `min_viable`/status AGAINST the pin.**

Evidence: precedence everywhere is intent.gateway → `pinned_gateway` → scan the registered list
(executor.rs:265-267 in `resolve_gateway`, :851 raw-pay, :960 raw-receive; probe.rs:463;
runtime.rs:3210). The pin is an explicit operator override that bypasses selection. So: the pin
wins over the Q1 preselected `action.gateway` at perform (matches :265-267). The `fee_cap` still
applies. If the pinned gateway makes a pair `UneconomicAtAnySize`, that is surfaced visibly (the
operator pinned an uneconomic gateway — a misconfiguration worth showing, per the critical
constraint), not silently swallowed.

## Q5 — Fallback when an entry is missing or non-`Routable`
**Decision: synthesise BOTH reviewer proposals by applicability —**
- **MISSING entry** (first tick, or a quote RPC failed this tick — transient absence):
  **permissive fallback to the protocol floor `min_move` alone.** A missing entry is transient;
  blocking on absence would stall all rebalancing on any quote hiccup. The proportional `fee_cap`
  is the money backstop (worst case: one move is attempted and fails the cap — bounded,
  self-healing churn, the exact thing the floor reduces but never a money loss).
- **`Routable`**: floor = `min_viable_amount`.
- **`Unroutable`** (no gateway serves the pair): skip funding for that pair (cannot route).
- **`UneconomicAtAnySize`** (bps set below the gateways' combined ppm): skip funding AND surface
  it VISIBLY (persistent diagnostic / refusal reason). Unlike the deliberately-silent sub-dust
  skip (allocator.rs:222), this silently disables rebalancing for the pair FOREVER and is a
  misconfiguration — the critical constraint demands it be visible.

This uses the status variants for present entries (proposal B) and the permissive protocol-floor
fallback only for the genuinely-absent case (proposal A), so neither swallows the other.

## Adjacent finding — Move first-gateway vs cheapest
**Decision: FIX it inside the route-economics impl bead (do not split out).** The Move path
takes the FIRST validating gateway with no fee comparison (executor.rs:269-272), while
raw-receive scans for `lowest_quote` (executor.rs:964-989). Since computing the per-pair floor
already quotes every validating gateway's fees, selecting the cheapest is free (same quotes):
`resolved_gateway` := argmin over gateways serving BOTH ends of combined (fed+gateway) fee. This
simultaneously (a) removes the asymmetry, (b) gives the Q1 preselected gateway real economic
meaning, and (c) is the value the Q2 fallback re-resolves to. Folding it in avoids two passes
that must agree forever.

---

## Implementation bead to create (acceptance handed off)
Title: "Implement route_economics_by_pair (per-pair economic move floor)".
Must carry: the snapshot field + IO population (tick.rs), the cheapest-serving-both-ends gateway
selection replacing first-validator, the affine-upper-bound `min_viable_amount` with a hard
per-tick quote cap, `gateway: Option<GatewayUrl>` (`#[serde(default)]`) on `Action::Move`/
`Evacuate` stamped by `decide()`, the perform-time hint-with-cap-backstop fallback (Q2), the
pin-precedence (Q4), the missing/status fallbacks (Q5), and VISIBLE surfacing of
`UneconomicAtAnySize`. Test gate: golden tests for each status branch + the upper-bound floor
monotonicity; a devimint move smoke that a sub-floor pair defers and a viable pair moves.
