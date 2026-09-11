# 06 — Allocator and automation

The decision core and the loop that drives it: `wallet-core/src/{allocator,scorer,probe,
discovery,conflict,watch}.rs`, `wallet-fedimint/src/{tick,route_econ,probe,discovery}.rs`, and
the scheduler and actor in `wallet-fedimint/src/service/`. Read 2026-09-07; re-read against
`7225114` and the golden fixtures 2026-09-10.

## The pure allocator

**ALC-1** `decide_with_diagnostics(snapshot, occurrence, blockers) → {decisions, suppressed,
deferred}` is pure. The snapshot carries: the federations in order (order is emission order),
each with `spendable`, `probed_ok`, `reputation`, `shutdown_notice`, `healthy`,
`eligible_to_fund`; the two designated federations; `per_fed_cap`, the two targets,
`max_fee_bps_of_move`, the evacuation cap components, `min_move` (the lnv2 minimum contract,
5,000 msat), `route_economics_by_pair`, and the projected `reservations`. `decisions` and
`suppressed` are byte-identical to what `decide_with_blockers` returns for the money path;
`deferred` is diagnostic and reaches only `status`. The order the I/O layer supplies is the
order `probe_all` yields: **ascending `FederationId`**, byte-lexicographic over the 32 bytes
(`MultiClient::federations` collects a `BTreeSet`), with any federation whose light probe
errored absent (`ALC-48`). That order decides which of two same-tick evacuations into one
destination gets the cap room (`ALC-9`).

**ALC-2** Of the snapshot's policy fields, `decide` reads `per_fed_cap`, the two targets, the
proportional cap, the evacuation components, `min_move`, the route map, reservations and the two
pins. It does **not** read `max_fee` — the flat cap bounds no allocator-emitted action — and it
does not read `now`.

**ALC-3** Two default sets exist. `wallet_api::Policy::default()` seeds the stored policy row
and is what **every** planning path runs under: the daemon, and `--standalone tick`/`status`,
which build their `TickPolicy` from the **stored** policy (`build_standalone_tick_policy` →
`TickPolicy::from(&stored)`, falling back to `Policy::default()` on an empty store) with any
override flags applied — flags are validated and **never persisted** (`OPS-7`). The second set,
`tick::TickPolicy::default()`, governs no planning: its `per_fed_cap` reaches a money path only
as the hard cap of standalone `probe` (`operator_hard_cap(false)`, `OPS-7`, `F43`; standalone
`discover` receives the same cap but its auto-join never probes, so nothing spends under it),
and the rest of it is read by tests and the watch harness. `Policy` validation is `API-20`.

| Knob | `Policy::default()` (daemon **and** standalone `tick`/`status`) | `TickPolicy::default()` (standalone `probe`/`discover` cap; tests) |
|---|---|---|
| `per_fed_cap` | 1,500,000,000 msat | 5,000,000,000 msat |
| `spending_target` / `target_spending_balance` | 500,000,000 | 100,000,000 |
| `standby_target` | 150,000,000 | 100,000,000 |
| `max_fee` (flat; user verbs and the probe leg cap only) | 200,000 | 50,000 |
| probe leg fee cap | `= max_fee` = 200,000 (`PolicyExt::probe_policy`) | 10,000 (`PROBE_LEG_FEE_CAP_MSAT`) |
| probe budget (attempts / msat per 7 d) | 10 / 500,000 | `wallet_core::ProbeBudget::default()`: 50 / 50,000 (the watch harness only) |

Only those six knobs differ. Every other value — the bps and evacuation caps, probe amount and
verdict knobs, the cadences, discovery limits and the two flags — is identical in both sets, and
its number is owned by `STO-13` (the seeded row), not restated here; `max_concurrent_unproven`
= 3 is a constant (`ALC-29`).

The daemon's probe leg cap is therefore twenty times the standalone one, and a daemon attempt
qualifies under the standalone verdict policy only if its cap was ≤ 10,000 (`ALC-25`).

**ALC-4** The procedure, in order. For each federation in snapshot order: if it has an
evacuation reason (`ALC-17`), plan an evacuation or a refusal and admit it against the blockers
(`ALC-30`); independently, if `spendable > per_fed_cap` (strict), record an advisory `OverCap`
refusal with empty diagnostics. Then top up spending: `want = funding_shortfall(...)`, and only
when `want > 0` **and** the spending federation has no evacuation reason (otherwise nothing is
emitted for this step); the source is the standby unless it has an evacuation reason (not
gated on probes or eligibility); `available = max_fundable(standby spendable − outbound
reservations − already debited)`; `fund_into`. Then fund standby under the same two guards,
with the source budget additionally floored at the spending target (`spendable − target −
outbound − debited`) so spending is never drained below it. `decide` never emits
`DirectInflow`, `Join`, `Recover`, `Pay` or `Receive`.

Admission and dedup (`push_if_eligible_and_reserve`, `push_decision`, `push_and_reserve`):

| Outcome | When | Effect |
|---|---|---|
| `ConflictSuppressed` | `blocked.blocks_decision` (`ALC-30`) | decision goes to `suppressed`; nothing reserved |
| `Admitted` | key not already in `decisions` | pushed; a `Move`/`Evacuate` adds `credited[to] += amount`, `debited[from] += amount + fee_cap`, both saturating |
| `Duplicate` | key already present | dropped silently, nothing reserved |
| refusal replace | both the existing and the incoming decision under one key are `RefuseInflow`, the incoming `diagnostics.is_populated()` (any field `Some`, or `conflict_suppressed`) and the existing is not | the incoming **replaces the existing in place**, keeping its position |

The emitted vector order is therefore: for each federation in snapshot order, [its `Evacuate`
or evacuation refusal, then the suppression twin if the evacuation was withheld (`ALC-30`), then
its empty `OverCap`]; then the top-up step's pushes; then the standby step's pushes. Within one
`fund_into` the push order is: receive-blocker refusal (returns) | the `Move` | `OverCap` |
`UneconomicRoute` (returns) | the shortfall refusal. The golden
`cap_and_liquidity_refusals_do_not_collide` pins `[Move, OverCap, SpendingBelowTarget]`.

**ALC-5** `fund_into`'s exits, in evaluation order, and their visibility:

| Condition | Outcome | Visible? |
|---|---|---|
| destination not eligible or not probed | `RefuseInflow NotProbed` with source-side diagnostics; return | refusal row |
| destination reputation < 0 | `RefuseInflow LowReputation`; return | refusal row (unreachable: `ALC-15`) |
| source is the destination | return | **silent** |
| `want < floor` and route not `UneconomicAtAnySize` | `deferred` entry only | **silent on the money path**; `status` and the poller see it (`F1`) |
| `cap_room = cap_room_with(...)` (`ALC-9`); `amount = min(want, cap_room, available)`; if the route is `Unroutable`/`UneconomicAtAnySize` **or `amount < floor`** → `amount = 0` | the crumb rule: an amount that would not clear the floor is never moved, and the shortfall refusal below records why (golden `sub_floor_available_crumbs_skip_the_move_but_keep_the_refusal`) | see below |
| source present and `amount > 0` | `Move` admitted, or conflict-suppressed (`emitted_amount = 0`), or a duplicate key dropped | move / refusal / **silent** |
| route `UneconomicAtAnySize` | `OverCap` first if `want > cap_room`, then `UneconomicRoute`; **return — no shortfall refusal** (golden asserts exactly one row), even when `want < floor` | refusal row(s) |
| `want > cap_room` | `OverCap` refusal with figures | refusal row |
| suppressed, or `amount < min(want, cap_room)` | `SpendingBelowTarget` / `StandbyBelowTarget` refusal | refusal row |

An `Unroutable` pair gets no dedicated refusal; it surfaces as the shortfall refusal because the
amount was forced to zero. When the candidate was conflict-suppressed, **every** refusal this
call pushes (the `OverCap` one included) is keyed `conflict-suppressed:<candidate key>:<tag>`
(`ALC-30`, `STO-6`).

The `RefusalDiagnostics` each site records (persisted on the `Refusal` row, `STO-15`; identity
and `PartialEq` are `fed + reason` only, the figures compare equal always). "src?" means `Some`
iff a usable source existed, else `None`:

| Site | `source` | `want` | `available` | `source_spendable` | `max_fee` | `max_fee_bps` | `cap_room` | `amount` | `conflict_suppressed` | `min_move` |
|---|---|---|---|---|---|---|---|---|---|---|
| receive-blocker refusal | src? | `Some(want)` | src? (`max_fundable` result) | src? | `None` | `Some(bps)` | `None` | `None` | `false` | `Some(min_move)` |
| funding `OverCap` / `UneconomicRoute` / shortfall | src? | `Some(want)` | src? | src? | `None` | `Some(bps)` | `Some(cap_room)` | `Some(emitted)` — `0` if suppressed, else `amount` | as suppressed | `Some(min_move)` |
| top-level `OverCap`, evacuation with no destination | all `None` / `false` (`RefusalDiagnostics::default()`) |
| evacuation, source drained (`amount == 0`) | `Some(from)` | `None` | `Some(src_available)` | `Some(spendable)` | `None` | `None` | `Some(cap_room)` | `Some(0)` | `false` | `None` |
| evacuation suppression twin (`ALC-30`) | `Some(from)` | `None` | `Some(src_available)` | `Some(spendable)` | `None` | `None` | `Some(cap_room)` | `Some(0)` | `true` | `None` |

**ALC-6** `funding_shortfall(target, spendable, target_credit, credited_this_round) = target −
spendable − target_credit − credited`, saturating. `target_credit` counts only pending `Move` and
`Evacuate` deliveries; an external `Receive` or `DirectInflow` in flight does not reduce the
shortfall.

**ALC-7** A funding move's fee cap is **proportional**: `move_fee_cap(amount, bps) = floor(amount
× max_fee_bps_of_move / 10 000)` in `u128`, stamped on the `Move` (`DEF-1`). The policy rejects
0 and values above 10,000 (`API-20`).

**ALC-8** Sizing reserves `amount + cap(amount)` from the source, never a flat cap:
`max_fundable(budget, bps) = ((budget + 1) × 10 000 − 1) / (10 000 + bps)` in `u128`, the exact
integer maximum with `amount + floor(amount × bps / 10 000) ≤ budget` (`DEF-1`).

**ALC-9** The per-federation cap is enforced four ways: `cap_room = per_fed_cap − spendable −
inbound reservations − credited this round` (saturating) clamps every funding and evacuation
amount; an evacuation destination needs `cap_room > 0`; an already-over-cap federation gets an
advisory refusal; `want > cap_room` gets a populated `OverCap` refusal. Admission re-checks it
with fresh balances (`OPS-7`) and the executor re-checks it before minting (`OPS-22`). There is
no aggregate ceiling (`F10`).

## Route economics

**ALC-10** The funding floor is `max(route.min_viable_amount, min_move)` when the pair is
`Routable`, else `min_move`. `Unroutable` and `UneconomicAtAnySize` force the amount to zero.
A shortfall below the floor is **deferred**, not refused, with `floor_source = RouteMinViable`
iff `floor > min_move`, else `ProtocolMinMove`; a shortfall at or above the floor whose clamped
amount falls below it is zeroed and **refused** (the crumb rule, `ALC-5`). Over-blocking
self-heals as the shortfall grows, while under-blocking churns forever, so the floor is an
explicit upper bound and is never cached (`DEF-1`'s sibling decision,
`docs/archive/route-economics-decisions.md`).

**ALC-11** Unpriced is permissive. A pair is left **unpriced** (absent from the map, floor
`min_move`) when: the quote budget or deadline is exhausted; the fundable maximum (`ALC-13`) is
zero; more than four gateways are listed; any gateway fee RPC returns `Err`; the federation
receive quote errors; both send quotes error; the gateway registry is empty; every candidate's
slope is indeterminate; or the pair is conflict-blocked (`ALC-31`). It is `Unroutable` only
when at least one gateway is listed and every listed one fails both-end validation (a fee RPC
answering `Ok(None)` at either end), and `UneconomicAtAnySize` only when the proof in `ALC-12`
holds. The budget is one `RouteQuoteBudget` per tick: a 10,000 ms deadline from the tick's
`now_ms` and **24 calls**, where one call is each of: the destination's gateway list, each
candidate's receive-fee RPC, each candidate's send-fee RPC, the federation receive quote, the
federation send quote, and the send-quote fallback. The concrete preflight (`ALC-32`) runs under
the same deadline but consumes no call. The budget and the `priced` map are shared across every
route revision and across the shadow and ordinary rounds of one plan (`ALC-32`), so a pair
priced once is never re-quoted in that tick.

**ALC-12** `min_viable_amount(quotes, bps)` for one candidate, with `PPM = 10⁶`, `BPS = 10⁴`,
every product in `u128` saturating, `recv`/`send` the gateway `{base_msat, ppm}` at each end,
`recv_fed`/`send_fed` the federation fees sampled per `ALC-13`:

1. `recv.ppm ≥ PPM` → `UneconomicAtAnySize`.
2. **All six** of `recv.base, recv.ppm, send.base, send.ppm, recv_fed, send_fed` zero →
   `Routable(0)`.
3. `p = PPM − recv.ppm`, `q = PPM + send.ppm`, `k = recv_fed + recv.base`, `c = send_fed +
   send.base`.
4. Lower-bound proof from the gateway terms alone, at the 5,000 msat minimum contract:
   `gw_slope = (recv.ppm + send.ppm) × BPS`, `cap_gw_slope = bps × PPM`, `D = PPM × BPS`,
   `margin = (recv.base + send.base) × D + 5 000 × (gw_slope −̇ cap_gw_slope)` (saturating
   subtraction). If `gw_slope ≥ cap_gw_slope && margin ≥ 2 × D` → `UneconomicAtAnySize`.
5. `cap_slope = p × (BPS + bps)`, `fee_slope = BPS × q`. `cap_slope ≤ fee_slope` →
   `Indeterminate` (never a permanent status: integer flooring can still admit a finite window).
6. Else `floor = ceil(BPS × (q × k + p × (c + 1 + ceil(q / PPM))) / (cap_slope − fee_slope))`,
   saturated to `u64::MAX`; `Routable(floor)`.

Every rounding is upward. The two-federation measurement in `F1` reproduced this to the msat.

**ALC-13** Pricing per tick (`price_missing_pairs`, `pair_economics`, `select_gateway`):

- Pairs: exactly `(standby, spending)` and `(spending, standby)` when both are designated and
  distinct, minus pairs `blocks_funding_pair` rejects (`ALC-31`); a pair already in the tick's
  `priced` map is skipped.
- Fundable maximum (`maximum_funding_amount`): for `(standby → spending)`, `want =
  funding_shortfall(target_spending, spending.spendable, target_credit(spending), 0)` and
  `protected = 0`; for `(spending → standby)`, `want = funding_shortfall(standby_target,
  standby.spendable, target_credit(standby), 0)` and `protected = target_spending`; `budget =
  source.spendable − protected − outbound(source)`; `maximum = min(want, max_fundable(budget,
  bps), per_fed_cap − dest.spendable − inbound(dest))`, all saturating. Same-round
  `credited`/`debited` are not consulted here. `maximum == 0` → unpriced.
- Candidates: the destination's vetted list (`ADR-0030`), in list order. Per candidate, the
  receive fee (`maybe_receive_gateway_fee(dest, gw)`) then the send fee
  (`maybe_direct_swap_send_gateway_fee(source, gw)`): `Ok(None)` at either end skips the
  candidate, `Err` unprices the pair. No serving candidate among `n > 0` listed → `Unroutable`
  `{resolved_gateway: None, min_viable_amount: 0}`.
- Federation fees are sampled once at `sample = maximum + move_fee_cap(maximum, bps)`: the
  receive quote at `sample`; the send quote at `sample`, falling back to `maximum` when that
  quote errors (a source-constrained pair), and unpriced if both error.
- Selection: for each serving candidate compute `floor = min_viable_amount` (`ALC-12`) and
  `cost = modelled_fee(maximum)` where, in `u128`, `k = recv_fed + recv.base`, `invoice =
  ceil((maximum + k) × PPM / (PPM − recv.ppm))`, `send_gw = ceil(invoice × send.ppm / PPM) +
  send.base`, `cost = (invoice − maximum) + send_gw + send_fed` (`u128::MAX` when `recv.ppm ≥
  PPM`). If any candidate is `Routable`, the result is the `Routable` candidate with the
  smallest `cost`, ties broken by **first in list order** (`min_by_key`), as
  `{resolved_gateway: Some(gw), min_viable_amount: floor, Routable}`. Else if any candidate is
  `Indeterminate` → unpriced. Else the `UneconomicAtAnySize` candidate with the smallest `cost`
  (same tie-break) as `{resolved_gateway: Some(gw), min_viable_amount: 0,
  UneconomicAtAnySize}`; the pair is warn-logged.

Fee inputs enter from **both** legs: gateway base and ppm each way plus both federation fees.
The `resolved_gateway` is stamped on the emitted `Move` (and on an `Evacuate` of the same pair,
`ALC-17`) as the route hint, so which gateway wins here is wire-visible.

## Scoring and eligibility

**ALC-14** The structural floor (`structural_floor`) collects failing reasons in this order:
`guardian_count < 2` → `NoFaultTolerance`, **else** `guardian_count < min_guardians (4) ||
threshold < min_threshold (3)` → `TooFewGuardians` (a 1-of-1 therefore carries
`NoFaultTolerance` alone, never both); `threshold == 0 || threshold > guardian_count ||
threshold < n − (n−1)/3` (saturating) → `InvalidThreshold`; not mainnet when
`require_mainnet` → `WrongNetwork`; any required module missing (default `Mint`, `Wallet`) →
`MissingModule`; shutdown scheduled → `ShutdownScheduled`; no lnv2 → `NoLnv2` (not
policy-tunable). Discovery's copy of the policy differs only in `require_mainnet`
(`ALC-28`).

**ALC-15** `eligible_to_fund = floor_ok && quorum_live && round_trip_ok`. As assembled by the
light probe, `round_trip_ok` is `gateway_available` (no sats spent), `reputation` is hard-coded
0 and `observer` is always `None`, so `LowReputation` and the entire Observer rank path are
**unreachable in production**. The scorer does not read the active-probe verdict; the
auto-joined probe gate is applied in `tick.rs` and folded into `eligible_to_fund`. The final
snapshot eligibility is `(scorer || pinned) && probe_gate_ok`.

**ALC-16** Rank, `u32` saturating, only for an eligible federation (else 0): `min(threshold,
guardian_count) × 100`, `+ 50` if peg-out quotable, `− latency_ms / 10`, then if an observer
prior is present `+ min(99, uptime_permille.min(1000) / 20 + backing + activity)` and `− 100`
(with `LowObserverUptime`) when `uptime_permille < 900`, where `backing` is 30 / 20 / 10 / 0
for `backing_sats ≥ 10⁹ / 10⁸ / 10⁷ / less` and `activity` is 20 / 10 / 5 / 0 for
`activity_7d ≥ 1000 / 100 / 10 / less` (unreachable in production, `ALC-15`).

Auto-designation (`build_snapshot`): the designatable set is every federation with
`verdict.eligible_to_fund && probe_gate_ok` — a pin does **not** enter it — sorted by rank
**descending**, then `spendable` **descending**, then id **ascending**. `spending_fed` is the
policy pin, else the first designatable federation that is not a probe-gated member
(`ALC-37`) and not the pinned standby. `standby_fed` is the policy pin, else the first
designatable federation that is not `spending_fed` (a passed auto-joined federation may be
standby, never auto-spending). Both may be `None`.

## Evacuation

**ALC-17** `evacuation_reason` is `ShutdownNotice` if the snapshot's `shutdown_notice`, else
`Unhealthy` if not `healthy`; shutdown wins. The destination is the pinned standby if eligible
for evacuation, else the eligible federation with the **smallest id** (byte-lexicographic);
eligible means not self, no evacuation reason of its own, no receive blocker, and cap room
(`cap_room_with > 0`). No destination → a refusal with empty diagnostics. `amount = min(source
spendable − outbound − debited, cap_room)`; zero → a refusal with figures (`ALC-5`); else
`Evacuate {fee_cap: components.at(amount), gateway: route hint, fee_cap_components}` keyed
`evac:<from>:<to>:<occurrence>` (`STO-6`). The hint is the pair's `resolved_gateway` when the
pair happens to be priced (including an `UneconomicAtAnySize` entry's gateway), else `None`.
The stamped cap is the **planning cap at the planned amount**; the executor recomputes at the
delivered net (`OPS-21`).

**ALC-18** Route economics MUST NOT gate an evacuation: no `min_move`, no route status, no fee
pre-reservation on the source (`DEF-3`). A dying federation is drained even when the route
prices badly; the cap, not the floor, is the backstop.

**ALC-19** The evacuation **trigger** lead is the hard-coded 24 hours in
`SHUTDOWN_EVACUATION_LEAD_SECS` (`FMI-26`). The runtime-mutable `evacuation_lead_secs` (default
one hour) is a **wake** lead: the scheduler sleeps until `expiry − lead` and, once inside that
window, re-wakes every `min(min_interval, expiry − now)` — at the minimum interval, but never
past the expiry itself (`ALC-39`). The name suggests the first; it does the second.

**ALC-20** The evacuation cap is `EvacFeeCap {base_msat, bps}.at(net) = base + floor(net × bps /
10 000)`, in `u128` saturated to `u64`. Defaults 200,000 msat and 300 bps. Policy rejects bps
above 10,000 and the pair `(0, 0)`; `bps = 0` alone (a base-only cap) is legal (`DEF-3`).

**ALC-21** Every enforced evacuation cap is computed from the **delivered net** — `invoice −
receive_quote`, what the destination is actually credited — never from the sized ask
(`CONTEXT.md` **Delivered net**). The planning cap is the one deliberate exception and is
superseded by the recomputed cap as soon as sizing runs.

**ALC-22** The allocator stamps the planning cap and the components (`ALC-17`) and nothing
else; the sizing search that turns the planned amount into a delivered net — two passes over
delivered net with a viability post-check — is the executor's and is specified in `OPS-44`
(`OPS-21`). What the allocator side owns is the marker that search leaves behind: a refusal
whose evidence is structural (`ALC-24`) is the marker, carrying two freshly re-quoted samples
(the 5,000 msat floor and the largest probed affordable amount); every other refusal is
`Retryable`.

**ALC-23** A cap edit **qualifies** to replace a marked evacuation iff `new.base ≥ old.base &&
new.bps ≥ old.bps && (new.at(low.net) > old.at(low.net) || new.at(high.net) > old.at(high.net))`.
Component-wise monotone and strictly larger at a recorded sample; a crossed edit (base up, bps
down) or one whose bump truncates away does not qualify, and the daemon's `status` warns for each
marker the current cap cannot replace (`DEF-9`'s sibling fix, commit `746d029`).

**ALC-24** `assess_evacuation_structural_refusal(cap, low, high)`, in `i128`: with `span =
high.net − low.net`, `None` unless `span > 0`; `rise = high.fee − low.fee` (may be negative),
`cap_rise = bps × span`, `fee_rise = rise × 10 000`, and `fixed = low.fee − rise × low.net /
span` with `/` truncating toward zero: `fee_rises_no_faster_than_cap = cap_rise ≥ fee_rise`;
`fixed_component_exceeds_cap_base = fee_rise ≥ cap_rise && fixed > base && low.fee >
cap.at(low.net)`; `is_structural` is the **OR** of the two. The caller must separately establish
both samples are over their caps. It is two-point evidence on a non-monotone curve, not a proof;
since 2026-09-09 nothing requires a proof — the evidence feeds the supersession audit record only
(`F5`, closed).

## Probes and discovery

**ALC-25** `probe_verdict(attempts, source, now, policy)`: no attempts → `NeverProbed`; sort by
`at_ms` (stable); window = attempts with `now − at_ms ≤ ttl` (7 days; the boundary is
in-window); empty window → `Expired` if any attempt ever qualified else `NeverProbed`; suffix =
successes strictly after the most recent in-window failure; empty suffix →
`FailedSinceLastPass` if any contiguous success run before that failure qualifies (count and
span) else `Failed`; qualifying = `ok && same source && amount ≥ policy amount && leg cap ≤
policy cap`; `Passed` iff the qualifying suffix holds `≥ min_successes` (3) and `newest.at_ms −
oldest.at_ms ≥ min_span` (24 h), else `Insufficient`. Failures count regardless of source;
successes only from the same source. The verdict policy is never persisted. The `attempts`
history it reads is pruned on every outcome write per `STO-26`, so the verdict is over the
retained history, not every attempt ever made.

**ALC-26** Two budget checks guard a probe, both over the rolling 7-day window
`PROBE_BUDGET_WINDOW_MS` measured from a row's `max(created_at_ms, updated_at_ms)`:

- The scheduler's pre-filter (`probe_budget_ok`): `attempts < max_attempts && spend <
  max_spend`, counting agent `Probe` rows with a recorded `cost_msat`; its reset time is the
  earliest such row's effective time + 7 d. A fresh probe that fails it is skipped with a
  `watch-probe-skip:<candidate>:<spending>:<amount>:<bucket>` row (`STO-6`; `bucket` = the reset
  time, or `floor(now / 7 d) × 7 d` when no costed row exists) written `Started` then `Failed`
  under `StandingInstruction` with the reason text, and the deadline wakes at the reset
  (`ALC-52`).
- The actor's admission check (`check_probe_budget`), which is what actually refuses: with
  `reservation(a, c) = max(a + c, 2 × c)` for an amount `a` and leg cap `c`, entries retained
  while `active || age < 7 d`, `attempts` = costed entries, `active` = uncosted non-terminal
  umbrella rows (each reserving its session's `reservation`, or the policy's when the session
  is gone), it refuses `BudgetExhausted` iff `attempts + active + 1 > max_attempts || spend +
  Σ reserved + reservation(policy.probe_amount, policy.max_fee) > max_spend`. A wallet at
  `spend = max − 1` therefore passes the pre-filter and is refused here. While its budget state
  is loading or failed to load every probe is refused with a warning and no health flag
  (`ALC-48`).

**ALC-27** The active probe (`FMI-34`), `active_probe_inner`: resume an in-flight session
first (its `from`, amount and leg cap win over the caller's); else sample the candidate's
baseline (`0` when not open) and begin a session; write the umbrella `probe:<fed>:<nonce>` row
(`STO-6`); for a fresh probe or a pre-leg-IN resume, preflight — both federations open,
`source.spendable ≥ amount + leg_cap`, `candidate.spendable + amount ≤ per_fed_cap`,
`source.spendable ≤ per_fed_cap`, and `validate_executor_move_route` both ways — a failure is a
no-attempt that changes no verdict; re-sample the baseline; leg IN as a `Move` keyed by the
user-move shape `move:<from>:<to>:<amount>:<fee_cap>:<occurrence>` (`STO-6`) with
`occurrence = u64(nonce[0..16] as hex)`, awaited to terminal (non-terminal is transient,
session retained); size leg OUT from `delivered_in − 1 000` msat and persist it on the session;
`out_fee_cap = min(leg_cap, delivered_in − out_net)`; no-sweep check `candidate.spendable ==
baseline + delivered_in` (exact equality) and a source-cap re-check `source.spendable ≤ cap`,
both only while leg OUT is not yet journaled; leg OUT; one atomic outcome write (attempt
appended, session cleared, umbrella `Succeeded`, `cost = in.amount + in.receive_fee_quoted +
in.send_fee_quoted − out.amount` counting the in-leg only when `Settled`/`Stranded` and the
out-leg only when `Settled`). Every attempt's `at_ms` is the session's `started_at_ms`, not the
completion time. A leg failure demotes (`ok: false` attempt) only when `classify_leg_failure`
attributes it to the candidate: a `Failed` phase on leg OUT; or no terminal phase and the failing
step is candidate-hosted (mint for leg IN, pay for leg OUT, judged from the record's
invoice/send-op artifacts) and the error carries none of the `NON_CANDIDATE_SIGNATURES`
substrings nor the three pinned-SDK deterministic send rejections. `Stranded`, `Refunded`, a
`Failed` leg IN, and an ambiguous record are umbrella-only failures that record no attempt.

**ALC-37** The **probe gate**: a joined federation with no `UserApproved` candidate row is
auto-joined and fundable only when its active-probe verdict is `Passed`; a missing or poison
candidate row gates fail-closed. `approve` (`API-23`) writes `UserApproved` and exempts it; a user
`join` writes it only when no `AutoJoined` row already exists, because
`mark_candidate_user_approved` leaves an `AutoJoined` row agent-owned (`OPS-42`) — re-joining an
auto-joined federation does **not** release this gate. A pin does not bypass the gate.

**ALC-28** A discovery pass
(`run_discover_pass_bounded_with_rotation_and_probe_policy_with_membership_lease`):

1. Collect from each source under a fair share of the pass deadline (`Observer`, `Manual`; a
   slow or failed source is recorded, never fatal); group announcements by claimed id.
2. Recover agent-joined candidates: a joined federation whose row is `Discovered`/`Rejected`
   and whose registry entry is agent-created becomes `AutoJoined`/`Passed`; a joined
   federation with no row gets one from its joined invite (`source: Manual`, `AutoJoined`,
   `Passed`).
3. The candidate universe is the announced ids ∪ (when `auto_join`) every stored non-joined
   `Discovered` id, sorted; take a rotation window of `max_candidates_per_pass` from the stored
   cursor and rotation (`discover_pass_plan_in_rotation`).
4. Per candidate in the window, stopping at the pass deadline: a **joined** candidate is
   refreshed only if it has no row, or its row is `AutoJoined`/`UserApproved` and some
   announced invite differs from the stored one (the stored invite is replaced); a
   `Discovered`/`Rejected` row of a joined federation is left untouched without a fetch. An
   unjoined candidate is fetched iff it has no row, or (announcements exist and) **the stored
   invite is no longer announced** or `now − structural_checked_at > 7 d`; a differing invite
   that merely coexists with the still-announced stored one does not force a fetch. A fetch
   previews the first invite that authenticates within the per-candidate timeout (`FMI-22`), requires
   `claimed_id == invite id == config id` (Sybil check), and runs `score_structural` with
   `require_mainnet` from the discovery policy: `Passed` keeps an existing
   `AutoJoined`/`UserApproved` state and otherwise writes `Discovered`; rejected writes
   `Rejected(<first reason>)`. The row keeps the existing `source` and `discovered_at_ms`, and
   sets `structural_checked_at_ms = updated_at_ms = now`.
5. One `Discover` ledger row per source; then auto-join if enabled (`ALC-29`); one `AutoJoin`
   row (`autojoin:<nonce>`) always; advance the cursor over attempted ids only; `wrapped` iff
   the window completed, auto-join completed (or stopped for budget) and the plan wrapped;
   `backlog = !wrapped && universe non-empty`.

**ALC-29** Auto-join (`run_auto_join`) considers only non-joined `Discovered` rows in the
window, in window order, with the budget checked before each in this order: `lifetime ≥ cap` →
blocked (stops the pass); `weekly ≥ max` → blocked (stops); `concurrent_unproven ≥ max` → skip
this candidate. Defaults 20 / 5 / 3; the first two are policy fields,
`max_concurrent_unproven` is not runtime-mutable. A candidate not floored this pass is
re-previewed first: an id mismatch writes `Rejected("IdMismatch")`, a structural failure
`Rejected(<reason>)`, a preview error leaves it `Discovered`. A newly joined candidate is
`AutoJoined` and probe-gated, and the three counts are incremented locally for the rest of the
pass; a join that merely reopened an existing membership leaves the row `Discovered`.
`auto_join` defaults to `false`.

## Conflicts

**ALC-30** Suppression is scoped to the **allocator goal** (`DOM-17`), never a global count
(`DEF-6`). A live intent (`Pending`, `Executing`, `Awaiting`) holding a goal blocks a candidate
decision iff the goals are equal, or the holder is `Evacuate(s)` and the candidate is a `Move`
touching `s`; the candidate's own key never blocks it. `FundInto(A)` pending does not block
`Evacuate(A)`; `Evacuate(A)` live blocks any funding move from or into `A` but not another
federation's evacuation into `A`. A suppressed funding decision co-emits its refusals with
`amount: Some(0)` and `conflict_suppressed: true`, each keyed
`conflict-suppressed:<candidate key>:<tag>` (`ALC-5`, `ALC-51`); a suppressed non-zero
evacuation emits its twin `conflict-suppressed:<evac key>:<tag>` (a zero-amount suppressed
evacuation emits nothing). While an evacuation of either designated federation is live, no
allocator top-up is emitted at all.

**ALC-31** Blockers are computed from `pending()` at plan time, folded forward as each decision
in a batch is admitted, and re-scanned at commit. `blocks_funding_pair(source, dest)` (used to
skip pricing) is conservative: it cannot exclude the candidate's own key; a held `FundInto(d)`
blocks every pair into `d`, a held `Evacuate(s)` blocks every pair with `s` at either end.

## The tick

**ALC-32** The daemon's tick is `DecideTickRound` (planned off the actor) followed by
`CommitTick` (on the actor). Plan (`plan_tick_round_for_marker` → `plan_tick_round_inner` →
`build_tick_round`): build the snapshot twice (a preliminary pass with no verdicts to pick the
spending federation, then with active-probe verdicts evaluated against it, the spending
federation itself omitted); copy the allocator reservation projection (`ALC-35`); price routes
(`ALC-13`), then drop from the snapshot any pair in the round's `invalidated` set; decide
(`ALC-1`); then the **route-revision loop**:

1. With no runtime or no route budget (a non-money cycle) the round is returned as planned.
2. `first_move_route_problem` preflights, in `decisions_to_apply` order, each `Move`/`Evacuate`
   whose key has **no** existing intent (an unreadable journal counts as existing), via
   `validate_executor_move_route`: the destination's vetted list in list order, a gateway
   serving the destination and then the source ends the scan as routable; the first problem
   wins. An empty or unreadable list, or no gateway serving the destination, is a
   **destination** problem; a gateway serving the destination but none of those serving the
   source is a **source-route** problem. Either marks the **destination** unavailable
   (`mark_gateway_unavailable(to)`, an in-memory `gateway_available = false` on the probe).
   Deadline expiry before the preflight answers returns the current round with a warning.
3. No problem: if `first_route_blocked_designation` names a designated pair — its
   `route_economics_by_pair` status is `Unroutable`/`UneconomicAtAnySize`, some refusal names
   its destination with a reason other than `OverCap` and `diagnostics.want ≥ min_move`, and no
   `Move` into that destination was emitted — mark that destination unavailable, keep the
   **first** such round as the route-blocked fallback, and re-plan; otherwise return the round,
   except that when a route-blocked fallback exists and this final round emits no `Move`, the
   fallback is returned so the `§Q5` refusal stays visible.
4. A problem: if it is a source-route problem on an emitted `Evacuate` from that source,
   remember this round as the evacuation fallback; add the pair to `invalidated`; mark the
   destination unavailable and re-plan. If the mark changed nothing (already unavailable), or
   a later round no longer evacuates the remembered source (return the evacuation fallback),
   stop.
5. Every re-plan increments `route_revisions`; when it exceeds the probed federation count the
   current round (or the route-blocked fallback) is returned.

Marker handling: which reconcile command captures, preserves or redrives a structural marker is
`OPS-35`'s (`CaptureForPlanner`, `PreservePlannerOwned`, `RedriveWithoutPlanner`); the planner
only consumes what `ReconcileDecide` parked. The shadow round runs first with replacements
enabled — the qualifying parent is the parked one when `ReconcileDecide` offered it, else the
**first** `pending()` intent in index order that is `Actor::Agent` with `occurrence <
u64::MAX`, an `Evacuate`, and whose `evacuation_refusal` qualifies under the current cap
(`ALC-23`); its key is excluded from the blockers and its reservations from the projection.
`finish_replacement_round` takes the first same-source `Evacuate` of the shadow round as the
one-child replacement (`OPS-30`), moves every other planned decision to
`replacement_deferred`, and clears `decisions`; with no such child it records a marker-clear
disposition instead, and the ordinary round is then re-planned (sharing the route state) and
returned with that disposition. Finally validate pinned inputs (`pinned_input_problems` over the
child or the decisions plus `replacement_deferred`; a problem fails the round with
`Storage("tick: …")`). Commit: `OPS-11`, then the replacement branch (`OPS-32`) or the
per-decision loop (`ALC-53`). The tick row is opened before sensing and terminalized when every
accepted driver finishes.

**ALC-33** The occurrence (`DOM-16`) is allocated by `advance_watch_occurrence` once per cycle
before the tick row opens; a checked overflow at `u64::MAX` is `Permanent` and fails the cycle.
The daemon can run exactly one `u64::MAX` cycle and then fails every later one with
`cycle_failed`; standalone refuses it up front.

**ALC-34** Ledger rows per tick (shapes `STO-6`, kinds `STO-15`): one `Tick` row keyed
`tick:<occurrence>:<nonce>` (`Started` before sensing, terminalized with decision / performed /
failed counts; `Succeeded` iff `failed == 0 && batch.error.is_none()` (`finish_tick_batch`; a
zero-decision batch refused for a stale policy generation or a reservation-projection fault
carries `error` and terminalizes `Failed`) — standalone also requires
`terminal_failed_skipped == 0`); one `Refusal` row per advisory decision, status `Succeeded`,
`error` = the batch note when one applies, **append-once** (an existing key is never rewritten,
so the figures are those at first observation); one `tick-drop:<occurrence>:<key>` row per
executable decision dropped at commit (`ALC-53`) or newly suppressed by the standalone re-scan
(`ALC-36`) — a `Refusal` row, status `Succeeded`, `error` = the drop message, diagnostics all
default except `conflict_suppressed = true` and `amount = Some(0)` when the drop was a conflict
on a non-zero move. A whole-batch refusal (`PolicySuperseded`, world-generation drift, an
invalid plan or balance token, or a `pending()` scan fault) terminalizes the tick row `Failed`
and writes no per-decision rows; the one exception is a reservation-projection storage fault,
which writes the advisory refusal rows before terminalizing `Failed`. The commit-time
missing-destination-balance drop inside the fresh-target check writes no `tick-drop` row (the
earlier missing-balance check does). Every ledger write around a tick is best-effort (warn on
error) except the money path.

**ALC-35** The "phase-aware allocator batch": planning and commit use the allocator reservation
projection (`OPS-9`), which reads each in-flight move's record phase to decide how much still
reserves, and is valid only with the balance generation that authorized it (the balance-facts
token). A corrupt record falls back to strict for that intent; a retryable read error aborts.

**ALC-36** The standalone tick (`Runtime::tick`): refuse `occurrence == u64::MAX`; record the
occurrence floor; open the tick row; plan with the same planner (`ALC-32`); refuse a plan
carrying both a replacement and a marker disposition (`Failed` row); re-scan blockers and
write `tick-drop` rows for newly suppressed decisions; validate pins (bail with a `Failed` tick
row); **bail** with a `Failed` row if any executable decision's key already maps to a
`Done`/`Awaiting`/`Failed` intent ("pass a fresh `--occurrence`" — the daemon instead drops such
decisions one by one, `ALC-53`); audit `replacement_deferred`; clear the marker disposition;
apply the replacement or the decisions through `apply_with_allocator_admission`; write refusal
rows; terminalize. `Runtime::watch_once` is a dev/test harness with no production caller
(`ADR-0031`).

## The scheduler cycle

**ALC-38** `run_cycle`, in order: (1) `reconcile_durable` (`OPS-35`, `PreservePlannerOwned`) —
this is the retry cadence of a `Retryable` intent (`OPS-14`): once per cycle, with one
exception — an attach that arrives while a driver owns the key sets `redrive_requested`, and if
that driver leaves the intent `Pending` at the same attempt, `finish_driver` re-spawns
immediately rather than waiting for the cycle (`OPS-14`). The cycle interval is the sleep
`ALC-39` computes — at most `base_interval_secs` (600 s), at least
`min_interval_secs` (30 s), earlier on an expiry or probe deadline, a policy change, or an
expiry-wake subscription; (2) ledger repair through the actor; (3) `list_federations_report` —
**fence A**: `skipped_rows > 0` → recovery-only redrive (`OPS-35`, `RedriveWithoutPlanner`),
return blocked `corrupt_federation_registry`; (4) open missing federations under a membership
lease — **fence B**: any still unopened → recovery-only redrive, return blocked
`partial_federation_view`; (5) `ReconcileDecide` (`OPS-35`, `CaptureForPlanner`): release the
previous parked handoff, capture qualifying markers, issue the tick-plan token (refused while a
lease is live or an authority is poisoned) — on error the cycle continues as **non-money**; (6)
`advance_watch_occurrence` —
**fence C**: failure → `cycle_failed`; (7) if planning may commit (`tick_may_commit` =
reconcile succeeded), open the tick row; (8) light probes → balances → facts (route pricing is
allowed iff the tick may commit; the blockers are the reconcile's); (9) `DecideTickRound`, then
— only when it may commit — the balance-facts token, re-probe for commit balances, `CommitTick`
(`ALC-53`); a token failure terminalizes the tick row `Failed`; (10) if commit was never invoked,
abandon the in-memory handoff; (11) recompute the spending designation from fresh probes under
a fresh policy snapshot (a fresh probe is admitted only when the tick may commit and designation
succeeded); (12) due probes per `ALC-52` and `DecideProbe` each; (13) discovery if due (`ALC-28`;
due = `backlog || now ≥ last_discover + discover_every`, a failed pass records `now` and clears
the backlog); (14) deadlines (`ALC-52`). Steps 1–2 and 8 onward warn and continue on most
errors; the watch-state reads, policy reads, discovery and deadline storage faults fail the
cycle. The cycle is a **noop** iff nothing failed, the reconcile was idle, zero decisions were
planned, the commit accepted and refused nothing, no probe was attempted and the discovery
watch state is unchanged (`ALC-39`).

**ALC-39** Sleep (`adaptive_sleep_ms(now, policy, deadlines)`, all in ms): `discover_delay =
min_interval` when the discovery backlog is set, else `last_discover + discover_every − now`
(saturating); `routine = clamp(min(base, discover_delay), min_interval, base)` (defaults 600 s /
30 s / 6 h). Each expiry deadline yields `evac_point = expiry − evacuation_lead`: `evac_point −
now` when that is positive, else `min(min_interval, expiry − now)`. Each probe deadline yields
`max(due − now, 1 000)` (the busy-spin floor). The sleep is `routine` capped by the minimum of
every expiry and probe delay. The expiry deadlines are, per probed federation, only the
corroborated `config_expiry_secs` and `meta_module_expiry_secs` (never the override meta or
the `/status` flag) and only those still in the future. The wait selects over abort, a policy
change, the timer, and a per-federation expiry-wake subscription to the meta field
`federation_expiry_timestamp`; a wake re-adds the hinted expiry, recomputes the sleep, and is
**coalesced** (`coalesced_subscription_delay_ms`): if the last subscription-triggered cycle was
a noop (`ALC-38`) less than `min_interval` ago, the wake waits the remaining cooldown, unless the
recomputed sleep is shorter, in which case the timer runs instead.

**ALC-40** The settlement-stall watchdog runs after every cycle: count `Awaiting` `Receive` or
`DirectInflow` intents older than the deadline (300 s, `WALLETD_SETTLEMENT_STALL_SECS`)
**whose invoice has been expired for longer than the deadline** (`DEF-8`) — the invoice read
from the intent, or for a `DirectInflow` from its `MoveRecord` (a record read error disarms the
watchdog for this cycle); with no parseable invoice the intent counts once `now − created_at >
3 600 s + deadline`. At least three such, and no `Receive` ledger row (a `DirectInflow` success
does not count) `Succeeded` with `updated_at` inside the deadline window among the newest 4,096
rows → the scheduler task returns, the critical-task guard clears `scheduler_alive` and the
daemon exits non-zero for its supervisor to restart. It writes no ledger row and no status; the
log line is its only artifact, and a journal read error disarms it silently (`ALC-48`).

**ALC-41** `PUT /v1/policy` (`API-20`) validates, stores, swaps the executor's cap, bumps the
policy generation and the probe-policy version, and wakes the scheduler immediately. A round
planned under the old generation is refused whole at commit (`PolicySuperseded`) and its tick
row terminalized `Failed`; a fresh probe planned under the old snapshot is refused the same way.

**ALC-42** Shutdown aborts the cycle at its next await point; a scheduler task that returns or
panics flips `scheduler_alive` to false and names itself on the critical-exit channel, which the
daemon treats as fatal (`HST-7`).

**ALC-43** The occurrence floor: `WatchState.occurrence` is raised in the same transaction as
every agent ledger append (`STO-21`), seeded from the ledger when absent (`STO-23`), advanced by
a checked increment, and rejected at `u64::MAX` before any write. The proof that no admission
path can leave it below the ledger is open (`F18`).

**ALC-44** `GET /v1/status` (`API-15`) and standalone `status` run the planner dry: routes are
priced and the concrete preflight runs (network IO, no writes); it warns per pinned-input
problem, per structural marker the current cap cannot replace (`ALC-23`), per terminal-replaying
decision, and per deferred funding goal; it returns `scored` with `gated_eligible` (read from
the planned snapshot, not re-derived from `score()`) and `deferred`. A replacement child whose
occurrence is not strictly newer than its parent's is a stale plan: the daemon's status
(`DaemonStrict`) fails with that error, the standalone one (`StandaloneDiagnostic`) warns and
returns the scored view with **no** would-run decisions. It is the only surface for a
floor-deferred shortfall.

**ALC-45** Every path that skips planning MUST set `automation_blocked {reason, detail}` before
the cycle sleeps (`DEF-9`). Reasons as built: `cycle_failed` (any cycle error, including
occurrence overflow and discovery or deadline storage faults), `partial_federation_view`, and
`corrupt_federation_registry` (with the skipped-row count). `automation_ready` on `/v1/health`
is its negation (`API-16`).

**ALC-46** Three planning surfaces MUST refuse a partial or corrupt world rather than plan from
the healthy subset: the scheduler (fences A and B, `ALC-38`), `GET /v1/status` (503 before the
dry run), and standalone `tick`/`status` (refuse before opening). Explicit user and admin verbs
keep their poison-tolerant behaviour. A poison registry row is not an absent federation: its
funds may be part of the world the allocator would score. Landed with PR #40 (`ee4ba1c`).

**ALC-47** One planning skip is **not** reported: a failed `ReconcileDecide` (step 5 — a
poisoned tick authority, or a lease live at the wrong moment) skips the tick row, route pricing,
commit and fresh probes, and the cycle still publishes `automation_blocked: None`, so
`/v1/health` reports ready. Only a warning is logged. This is a fourth invisible suppression of
the kind `DEF-9` prohibits (`F32`).

**ALC-48** Fail-closed paths that leave no ledger row and, in several cases, no log line: a
fresh probe dropped because designation failed; the watchdog disarmed by a journal read error;
an unreadable probe record treated as unproven (warn only); every probe refused while the
actor's budget state is loading (warn per federation per cycle, no health flag); probe refusal
backoff; a `record_tick_started` failure (the tick proceeds with no row); a federation whose
light probe errored dropped from the snapshot (warn only, and therefore not evacuated that
tick); a `get_policy` failure in the wait loop, which re-cycles **immediately with no sleep**;
`mark_gateway_unavailable`, which mutates only the in-memory probe list. `F33`.

**ALC-49** The light probe runs up to **four** times per cycle — before planning, before commit,
for designation, and for deadlines — each a live threshold read and gateway validation per
federation (`F34`).

**ALC-50** Test-only seams in production files are `#[cfg(test)]` with no-op twins, except two
that are `#[cfg(debug_assertions)]`: the crash killpoints (`OPS-28`) and `WALLET_CLI_FORCE_SHUTDOWN`
(`FMI-26`). A debug build honours both from the environment; a release build compiles them out
(`SEC-18`).

**ALC-51** The `<reason>` component of the `refuse:` and `conflict-suppressed:` key shapes in
`STO-6` is `reason_tag(ReasonCode)`; this rule owns that mapping, and the wire `reason` field
(`API-12`, `API-15`) carries the same tag. The id placeholders' encoding is `STO-6`'s. The key
text is `format!("{prefix}:{…}")` with `:` separators and no padding:

| `ReasonCode` | tag |
|---|---|
| `SpendingBelowTarget` | `spending_below_target` |
| `StandbyBelowTarget` | `standby_below_target` |
| `ShutdownNotice` | `shutdown_notice` |
| `Unhealthy` | `unhealthy` |
| `OverCap` | `over_cap` |
| `NotProbed` | `not_probed` |
| `LowReputation` | `low_reputation` |
| `UneconomicRoute` | `uneconomic_route` |
| `UserInitiated` | `user_initiated` |
| `StandingInstruction` | `standing_instruction` |
| `ActiveProbe` | `active_probe` |

The refusal key omits the diagnostics on purpose: re-ticks of one `(fed, reason, occurrence)`
dedup to one row (`ALC-34`).

**ALC-52** Probe scheduling (`probe_schedule_inputs`, `service_due_probes`,
`watch_deadlines_with_context`, `probe_next_due_at`), evaluated once for admission (step 12)
and once for the deadlines (step 14) of `ALC-38`:

- Candidates: the probe-gated members (`ALC-37`), in ascending id order. The source is the
  in-flight session's `from` when one exists; else the designated spending federation (the
  candidate itself is skipped); with no spending federation a fresh candidate has no source and
  is neither admitted nor given a deadline (retained sessions keep theirs).
- `verdict = probe_verdict(record.attempts, source, now, gate_policy)` (`ALC-25`).
  `last_invocation` = the newest `max(created_at, updated_at)` over agent `Probe` rows with
  reason `ActiveProbe` for `(candidate, source)`, read by `probe_schedule_ledger_rows` with
  `horizon = max(7 d, retry_backoff)`: a ledger row is dropped when **both** its
  `created_at_ms` and `updated_at_ms` are below `now − horizon`, except a non-terminal `Probe`
  row with `cost_msat: None`, which is kept regardless of age (the budget reconstruction reads
  the same rows and fails `Permanent` on any undecodable row). `base` =
  `None` for `NeverProbed`; the pass-expiry anchor (`probe_pass_expiry_anchor_ms`, the oldest
  qualifying success whose expiry would break `Passed`) for `Passed`; else the newest
  same-source attempt's `at_ms`.
- `due`, with `build = max(min_interval, ceil(min_span / max(min_successes − 1, 1)))`,
  `refresh = min(probe_refresh_lead, ttl / 2)`, and `floor(t) = max(t, last_invocation +
  retry_backoff)` when a last invocation exists:

| Verdict | `due` |
|---|---|
| `NeverProbed` | `last_invocation + retry_backoff`, else `now` |
| `Insufficient`, `Expired` | `floor((base ∨ now) + build)` |
| `Failed`, `FailedSinceLastPass` | `floor((base ∨ now) + retry_backoff)` |
| `Passed` | `floor((base ∨ last_invocation ∨ now) + ttl − refresh)` |

  A retained session that has already journaled leg IN is due `now`.
- Admission order: retained sessions first (stable sort), each as `ResumeOnly {nonce}`; a
  fresh candidate needs `due ≤ now`, the budget pre-filter (`ALC-26`; a miss writes the skip
  row), an open candidate with a sampled baseline, and a current policy snapshot. If **any**
  retained session is admitted this cycle only the retained group is sent to `DecideProbe` and
  fresh probes are deferred (`defer_fresh_probes`).
- Deadlines: for every candidate with a source and not owned by a live actor probe driver,
  `wake = due` for a retained session; else `max(due, budget reset | now + min_interval when
  the budget is exhausted)`; then `max(wake, now + retry_backoff)` if the actor refused this
  candidate this cycle, else `max(wake, now + min_interval)` if fresh probes were deferred.

**ALC-53** `CommitTick`'s per-decision admission, after the whole-batch guards (`ALC-41`'s
policy generation, then the world generation, the plan token, the balance-facts token, and a
single-occurrence check, each refusing the batch), in `decisions_to_apply` order, skipping
advisory decisions, each drop writing a `tick-drop` row (`ALC-34`) unless noted:

1. the destination has no fresh balance → `Conflict`;
2. `balance_facts_changed_for` the federations the action touches → `Conflict`;
3. the key already maps to a `Done`/`Awaiting`/`Failed` intent → `Conflict`; a journal read
   error fails the batch;
4. `blocked.blocks_decision` against a fresh `pending()` scan folded forward per admitted
   decision → `Conflict`, row marked `conflict_suppressed`;
5. `admission_snapshot_conflicts` (a goal admitted after the plan's eligibility snapshot) →
   `Conflict`, row marked `conflict_suppressed`;
6. for a **new** funding `Move`: `amount > funding_shortfall(target, fresh destination balance,
   commit_reservations.target_credit(to), 0)` with `target` chosen by the decision's reason →
   `Conflict` ("replan"); a missing fresh balance here refuses with **no** row;
7. `decide_op_with_allocator_reservations`; on success the goal is held against the rest of
   the batch and, for a new intent, its reservation folded into `commit_reservations`; a
   `Refused` error is a per-decision drop, any other error counts as failed and becomes the
   tick's error after the batch completes.

The advisory refusal rows are written after the loop; the tick row terminalizes when every
accepted driver finishes, else immediately.
