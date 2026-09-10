# 06 — Allocator and automation

The decision core and the loop that drives it: `wallet-core/src/{allocator,scorer,probe,
discovery,conflict,watch}.rs`, `wallet-fedimint/src/{tick,route_econ,probe,discovery}.rs`, and
the scheduler and actor in `wallet-fedimint/src/service/`. Read 2026-09-07.

## The pure allocator

**ALC-1** `decide_with_diagnostics(snapshot, occurrence, blockers) → {decisions, suppressed,
deferred}` is pure. The snapshot carries: the federations in order (order is emission order),
each with `spendable`, `probed_ok`, `reputation`, `shutdown_notice`, `healthy`,
`eligible_to_fund`; the two designated federations; `per_fed_cap`, the two targets,
`max_fee_bps_of_move`, the evacuation cap components, `min_move` (the lnv2 minimum contract,
5,000 msat), `route_economics_by_pair`, and the projected `reservations`. `decisions` and
`suppressed` are byte-identical to what `decide_with_blockers` returns for the money path;
`deferred` is diagnostic and reaches only `status`.

**ALC-2** Of the snapshot's policy fields, `decide` reads `per_fed_cap`, the two targets, the
proportional cap, the evacuation components, `min_move`, the route map, reservations and the two
pins. It does **not** read `max_fee` — the flat cap bounds no allocator-emitted action — and it
does not read `now`.

**ALC-3** Two default sets exist. The daemon's `Policy::default()` (cap 1,500,000,000 msat,
spending target 500,000,000, standby 150,000,000, probe budget 10 attempts / 500,000 msat a
week) governs a stored policy. The standalone `tick.rs` constants (cap 5,000,000,000, targets
100,000,000 each) govern `--standalone tick` when no override is given. Standalone `tick` and
`status` accept override flags that are validated and **never persisted**.

**ALC-4** The procedure, in order. For each federation in snapshot order: if it has an
evacuation reason (`ALC-17`), plan an evacuation or a refusal and admit it against the blockers;
independently, if its spendable exceeds the cap, record an advisory `OverCap` refusal. Then top
up spending: `want = funding_shortfall(...)`, source is the standby unless it has an evacuation
reason (not gated on probes or eligibility), `available = max_fundable(standby spendable −
outbound reservations − already debited)`, `fund_into`. Then fund standby: same, with the source
budget floored at the spending target so spending is never drained below it. `decide` never emits
`DirectInflow`, `Join`, `Recover`, `Pay` or `Receive`.

**ALC-5** `fund_into`'s exits and their visibility:

| Condition | Outcome | Visible? |
|---|---|---|
| destination not eligible or not probed | `RefuseInflow NotProbed` with diagnostics | refusal row |
| destination reputation < 0 | `RefuseInflow LowReputation` | refusal row (unreachable: `ALC-15`) |
| source is the destination | return | **silent** |
| `want < floor` and route not uneconomic | `deferred` entry only | **silent on the money path**; `status` and the poller see it (`F1`) |
| route `Unroutable` or `UneconomicAtAnySize` | amount forced to 0 | see below |
| source present and `amount > 0` | `Move` admitted, or suppressed by a conflict, or a duplicate key dropped | move / refusal / **silent** |
| route uneconomic | `UneconomicRoute` refusal (plus `OverCap` if `want > cap_room`), even when sub-floor | refusal row |
| `want > cap_room` | `OverCap` refusal with figures | refusal row |
| suppressed, or `amount < min(want, cap_room)` | `SpendingBelowTarget` / `StandbyBelowTarget` refusal | refusal row |

An `Unroutable` pair gets no dedicated refusal; it surfaces as the shortfall refusal because the
amount was forced to zero.

**ALC-6** `funding_shortfall(target, spendable, target_credit, credited_this_round) = target −
spendable − target_credit − credited`, saturating. `target_credit` counts only pending `Move` and
`Evacuate` deliveries; an external `Receive` or `DirectInflow` in flight does not reduce the
shortfall.

**ALC-7** A funding move's fee cap is **proportional**: `move_fee_cap(amount, bps) = floor(amount
× max_fee_bps_of_move / 10 000)`, stamped on the `Move` (`DEF-1`). The policy rejects 0 and
values above 10,000.

**ALC-8** Sizing reserves `amount + cap(amount)` from the source, never a flat cap:
`max_fundable(budget, bps) = ((budget + 1) × 10 000 − 1) / (10 000 + bps)`, the exact integer
maximum with `amount + floor(amount × bps / 10 000) ≤ budget` (`DEF-1`).

**ALC-9** The per-federation cap is enforced four ways: `cap_room = per_fed_cap − spendable −
inbound reservations − credited this round` clamps every funding and evacuation amount; an
evacuation destination needs `cap_room > 0`; an already-over-cap federation gets an advisory
refusal; `want > cap_room` gets a populated `OverCap` refusal. Admission re-checks it with fresh
balances (`OPS-7`) and the executor re-checks it before minting (`OPS-22`). There is no
aggregate ceiling (`F10`).

## Route economics

**ALC-10** The funding floor is `max(route.min_viable_amount, min_move)` when the pair is
`Routable`, else `min_move`. `Unroutable` and `UneconomicAtAnySize` force the amount to zero.
A shortfall below the floor is **deferred**, not refused: over-blocking self-heals as the
shortfall grows, while under-blocking churns forever, so the floor is an explicit upper bound and
is never cached (`DEF-1`'s sibling decision, `docs/archive/route-economics-decisions.md`).

**ALC-11** Unpriced is permissive. A pair is left **unpriced** (absent from the map, floor
`min_move`) when: the quote budget (10 s / 24 calls) or deadline is exhausted; the fundable
maximum is zero; more than four gateways are registered; any gateway fee RPC errors; the
federation receive quote errors; both send quotes error; the gateway registry is empty; every
candidate's slope is indeterminate; or the pair is conflict-blocked. It is `Unroutable` only
when listed candidates all fail both-end validation, and `UneconomicAtAnySize` only when the
slope proof holds.

**ALC-12** `min_viable_amount` for a candidate, with `PPM = 10⁶`, `BPS = 10⁴`: if `recv.ppm ≥
PPM` → uneconomic; if all four fee inputs are zero → `Routable(0)`; `p = PPM − recv.ppm`, `q =
PPM + send.ppm`, `k = recv_fed + recv.base`, `c = send_fed + send.base`; a lower-bound proof
using the 5,000 msat minimum contract may declare uneconomic; `cap_slope = p(BPS + bps)`,
`fee_slope = BPS·q`; `cap_slope ≤ fee_slope` → indeterminate; else `floor = ceil(BPS(qk + p(c +
1 + ceil(q/PPM))) / (cap_slope − fee_slope))`. Every rounding is upward. The two-federation
measurement in `F1` reproduced this to the msat.

**ALC-13** Pricing per tick: exactly the two pairs `(standby, spending)` and `(spending,
standby)` when both are designated, minus conflict-blocked pairs. Per pair: the fundable maximum
(shortfall, budget, cap room); candidates = the pin as a singleton, else the destination's
vetted list; per candidate both gateway fees (`Ok(None)` skips it, `Err` unprices the pair);
federation fees sampled at `maximum + cap(maximum)`; `select_gateway` picks the cheapest
`Routable` by modelled fee, else unpriced if any indeterminate, else the cheapest uneconomic.
Fee inputs enter from **both** legs: gateway base and ppm each way plus both federation fees.

## Scoring and eligibility

**ALC-14** The structural floor collects every failing reason: fewer than two guardians
(`NoFaultTolerance`); fewer than the policy's minimum guardians (4) or threshold (3)
(`TooFewGuardians`); threshold zero, above the count, or below the BFT bound `n − (n−1)/3`
(`InvalidThreshold`); not mainnet when required (`WrongNetwork`); a required module missing
(`MissingModule`, default `Mint` and `Wallet`); shutdown scheduled (`ShutdownScheduled`); no
lnv2 (`NoLnv2`, not policy-tunable).

**ALC-15** `eligible_to_fund = floor_ok && quorum_live && round_trip_ok`. As assembled by the
light probe, `round_trip_ok` is `gateway_available` (no sats spent), `reputation` is hard-coded
0 and `observer` is always `None`, so `LowReputation` and the entire Observer rank path are
**unreachable in production**. The scorer does not read the active-probe verdict; the
auto-joined probe gate is applied in `tick.rs` and folded into `eligible_to_fund`. The final
snapshot eligibility is `(scorer || pinned) && probe_gate_ok`.

**ALC-16** `rank = min(threshold, guardians) × 100 + 50 if peg-out quotable − latency_ms / 10 (+
observer bonus ≤ 99, −100 below 90% uptime)`, saturating. Designation orders by rank, then
spendable, then id; an agent-joined federation is never auto-picked as spending.

## Evacuation

**ALC-17** `evacuation_reason` is `ShutdownNotice` if the snapshot's `shutdown_notice`, else
`Unhealthy` if not `healthy`; shutdown wins. The destination is the pinned standby if eligible
for evacuation, else the eligible federation with the **smallest id**; eligible means not self,
no evacuation reason of its own, no receive blocker, and cap room. No destination → a refusal
with empty diagnostics. `amount = min(source spendable − outbound − debited, cap_room)`; zero →
a refusal with figures; else `Evacuate {fee_cap: components.at(amount), gateway: route hint,
fee_cap_components}` keyed `evac:<from>:<to>:<occurrence>`. The stamped cap is the **planning
cap at the planned amount**; the executor recomputes at the executed net (`OPS-21`).

**ALC-18** Route economics MUST NOT gate an evacuation: no `min_move`, no route status, no fee
pre-reservation on the source (`DEF-3`). A dying federation is drained even when the route
prices badly; the cap, not the floor, is the backstop.

**ALC-19** The evacuation **trigger** lead is the hard-coded 24 hours in
`SHUTDOWN_EVACUATION_LEAD_SECS` (`FMI-26`). The runtime-mutable `evacuation_lead_secs` (default
one hour) is a **wake** lead: the scheduler sleeps until `expiry − lead` and then polls at the
minimum interval. The name suggests the first; it does the second.

**ALC-20** The evacuation cap is `EvacFeeCap {base_msat, bps}.at(net) = base + floor(net × bps /
10 000)`, in `u128` saturated to `u64`. Defaults 200,000 msat and 300 bps. Policy rejects bps
above 10,000 and the pair `(0, 0)`; `bps = 0` alone (a base-only cap) is legal (`DEF-3`).

**ALC-21** Every enforced evacuation cap is computed from the **delivered net** — `invoice −
receive_quote`, what the destination is actually credited — never from the sized ask
(`CONTEXT.md` **Delivered net**). The planning cap is the one deliberate exception and is
superseded by the recomputed cap as soon as sizing runs.

**ALC-22** Sizing does not assume monotonicity: `cap(a) = base + bps·a` and the fee curve cross,
so a single bisection can discard a feasible window. The search is two passes over delivered
net with a viability post-check, and its refusal diagnostic carries two freshly re-quoted
samples — the 5,000 msat floor and the largest probed affordable amount (`OPS-21`). A refusal
whose evidence is structural (`ALC-24`) is the marker; every other refusal is `Retryable`.

**ALC-23** A cap edit **qualifies** to replace a marked evacuation iff `new.base ≥ old.base &&
new.bps ≥ old.bps && (new.at(low.net) > old.at(low.net) || new.at(high.net) > old.at(high.net))`.
Component-wise monotone and strictly larger at a recorded sample; a crossed edit (base up, bps
down) or one whose bump truncates away does not qualify, and the daemon's `status` warns for each
marker the current cap cannot replace (`DEF-9`'s sibling fix, commit `746d029`).

**ALC-24** `assess_evacuation_structural_refusal(cap, low, high)`: with `span = high.net −
low.net > 0`, `rise = high.fee − low.fee`, `cap_rise = bps × span`, `fee_rise = rise × 10 000`,
and `fixed = low.fee − rise × low.net / span`: `fee_rises_no_faster_than_cap = cap_rise ≥
fee_rise`; `fixed_component_exceeds_cap_base = fee_rise ≥ cap_rise && fixed > base && low.fee >
cap.at(low.net)`; `is_structural` is the **OR** of the two. The caller must separately establish
both samples are over their caps. It is two-point evidence on a non-monotone curve, not a proof;
since 2026-09-09 nothing requires a proof — the evidence feeds the supersession audit record only
(`F5`, closed).

## Probes and discovery

**ALC-25** `probe_verdict(attempts, source, now, policy)`: no attempts → `NeverProbed`; window =
attempts within `ttl` (7 days); empty window → `Expired` if any attempt ever qualified else
`NeverProbed`; suffix = successes strictly after the most recent in-window failure; empty suffix
→ `FailedSinceLastPass` if a prior qualifying run exists else `Failed`; qualifying = `ok && same
source && amount ≥ policy amount && leg cap ≤ policy cap`; `Passed` iff `≥ min_successes` (3)
spanning `≥ min_span` (24 h), else `Insufficient`. Failures count regardless of source;
successes only from the same source. The verdict policy is never persisted.

**ALC-26** Budget: per rolling 7 days, `attempts < max && spend < max`, counting agent `Probe`
rows with a recorded cost. The actor also counts active reservations and refuses `BudgetExhausted`;
while its budget state is loading or failed to load every probe is refused with a warning and
no health flag (`ALC-48`). Exhaustion writes a `watch-probe-skip` row and wakes at the budget
reset.

**ALC-27** The active probe (`FMI-34`): resume an in-flight session first; else sample the
candidate's baseline and begin a session; write the umbrella `probe:` row; preflight (both open,
source can afford `amount + cap`, candidate under cap, route validates both ways) — failure is a
no-attempt that changes no verdict; re-sample; leg in as a `Move` with a nonce-derived occurrence
and await it to terminal (non-terminal is transient, session retained); size leg out from the
delivered net minus a 1,000 msat margin, persist it on the session; no-sweep check; leg out; one
atomic outcome write (attempt appended, session cleared, umbrella `Succeeded`, cost = source net
outflow). A candidate-side leg failure records a **demoting** failed attempt; an umbrella-only
failure records none.

**ALC-37** The **probe gate**: a joined federation with no `UserApproved` candidate row is
auto-joined and fundable only when its active-probe verdict is `Passed`; a missing or poison
candidate row gates fail-closed. `approve` (`API-23`) or a user `join` writes `UserApproved` and
exempts it. A pin does not bypass the gate.

**ALC-28** A discovery pass: collect from each source under a fair share of the pass budget
(`Observer`, `Manual`; a slow or failed source is recorded, never fatal); recover agent-joined
candidates whose rows are missing or stale; take a rotation window of `max_candidates_per_pass`
from the sorted ids resuming at the stored cursor; per candidate, if joined refresh only, else
fetch if the row is new, its invite changed, or older than 7 days: preview with the per-candidate
timeout, require `claimed_id == invite id == config id` (Sybil check), `score_structural` →
`Discovered` or `Rejected`, write the row; one `Discover` ledger row per source; then auto-join
if enabled (`ALC-29`); one `AutoJoin` row always; advance the cursor over attempted ids only.

**ALC-29** Auto-join budget, checked in this order: `lifetime ≥ cap` → blocked (stops the pass);
`weekly ≥ max` → blocked (stops); `concurrent_unproven ≥ max` → skip this candidate. Defaults
20 / 5 / 3; the first two are policy fields, `max_concurrent_unproven` is not runtime-mutable.
`auto_join` defaults to `false`. A newly joined candidate is `AutoJoined` and probe-gated.

## Conflicts

**ALC-30** Suppression is scoped to the **allocator goal** (`DOM-17`), never a global count
(`DEF-6`). A live intent (`Pending`, `Executing`, `Awaiting`) holding a goal blocks a candidate
decision iff the goals are equal, or the holder is `Evacuate(s)` and the candidate is a `Move`
touching `s`. `FundInto(A)` pending does not block `Evacuate(A)`; `Evacuate(A)` live blocks any
funding move from or into `A` but not another federation's evacuation into `A`. A suppressed
funding decision co-emits a zero-amount refusal with `conflict_suppressed: true`; a suppressed
non-zero evacuation emits `conflict-suppressed:<key>:<reason>`. While an evacuation of either
designated federation is live, no allocator top-up is emitted at all.

**ALC-31** Blockers are computed from `pending()` at plan time, folded forward as each decision
in a batch is admitted, and re-scanned at commit. `blocks_funding_pair(source, dest)` (used to
skip pricing) is conservative: it cannot exclude the candidate's own key.

## The tick

**ALC-32** The daemon's tick is `DecideTickRound` (planned off the actor) followed by
`CommitTick` (on the actor). Plan: build the snapshot twice (a preliminary pass to pick the
spending federation, then with active-probe verdicts); copy reservations; price routes; decide;
run the route-revision loop (a concrete preflight of each emitted move's route; a failure marks
the federation's gateway unavailable in memory and re-plans, bounded by the federation count);
if a qualifying marked parent exists, run a shadow round excluding it and turn the same-source
`Evacuate` into a one-child replacement round (`OPS-30`) or a marker-clear disposition; validate
pinned inputs (a problem fails the round with `Storage("tick: …")`). Commit: `OPS-11`, then the
replacement branch (`OPS-32`) or the per-decision loop. The tick row is opened before sensing
and terminalized when every accepted driver finishes.

**ALC-33** The occurrence (`DOM-16`) is allocated by `advance_watch_occurrence` once per cycle
before the tick row opens; a checked overflow at `u64::MAX` is `Permanent` and fails the cycle.
The daemon can run exactly one `u64::MAX` cycle and then fails every later one with
`cycle_failed`; standalone refuses it up front.

**ALC-34** Ledger rows per tick: one `Tick` row (`Started`, terminalized with decision /
performed / failed counts, `Succeeded` iff nothing failed); one `Refusal` row per advisory
decision; one `tick-drop:<occurrence>:<key>` row per executable decision dropped at commit for
conflict, balance, target or admission reasons. A whole-batch refusal (`PolicySuperseded`,
world drift, storage) terminalizes the tick row `Failed` and writes **no** per-decision rows.
Every ledger write around a tick is best-effort (warn on error) except the money path.

**ALC-35** The "phase-aware allocator batch": planning and commit use the allocator reservation
projection (`OPS-9`), which reads each in-flight move's record phase to decide how much still
reserves, and is valid only with the balance generation that authorized it (the balance-facts
token). A corrupt record falls back to strict for that intent; a retryable read error aborts.

**ALC-36** The standalone tick (`Runtime::tick`): record the occurrence floor, open the tick
row, plan with the same planner, re-scan blockers and write `tick-drop` rows for newly suppressed
decisions, validate pins (bail with a `Failed` tick row), apply the replacement or the decisions
through `apply_with_allocator_admission`, write refusal rows, terminalize. `Runtime::watch_once`
is a dev/test harness with no production caller (`ADR-0031`).

## The scheduler cycle

**ALC-38** `run_cycle`, in order: (1) `reconcile_durable` (`OPS-35`); (2) ledger repair through
the actor; (3) `list_federations_report` — **fence A**: `skipped_rows > 0` → recovery-only
redrive, return blocked `corrupt_federation_registry`; (4) open missing federations under a
membership lease — **fence B**: any still unopened → recovery-only redrive, return blocked
`partial_federation_view`; (5) `ReconcileDecide`: release the previous parked handoff, capture
qualifying markers, issue the tick-plan token (refused while a lease is live or an authority is
poisoned) — on error the cycle continues as **non-money**; (6) `advance_watch_occurrence` —
**fence C**: failure → `cycle_failed`; (7) if planning may commit, open the tick row; (8) light
probes → balances → facts; (9) `DecideTickRound`, then the balance-facts token, re-probe for
commit balances, `CommitTick`; (10) if commit was never invoked, abandon the in-memory handoff;
(11) recompute the spending designation from fresh probes; (12) due probes (retained sessions
first) and `DecideProbe` each; (13) discovery if due; (14) deadlines. Steps 1–2 and 8 onward
warn and continue on most errors; discovery and deadline storage faults fail the cycle.

**ALC-39** Sleep: `adaptive_sleep_ms` = the minimum of a routine delay (`clamp(min(base,
discover_delay), min_interval, base)`, defaults 600 s / 30 s), the earliest federation expiry
minus the wake lead (floored to the minimum interval once inside the window), and the earliest
probe due time (1 s busy-spin floor). The wait selects over abort, a policy change, the timer,
and a per-federation expiry-wake subscription to the meta field `federation_expiry_timestamp`,
rate-limited to the minimum interval.

**ALC-40** The settlement-stall watchdog runs after every cycle: at least three `Awaiting`
receives or direct inflows older than the deadline (300 s, `WALLETD_SETTLEMENT_STALL_SECS`)
**whose invoice has been expired for longer than the deadline** (`DEF-8`), and no receive
`Succeeded` in the window → the scheduler task returns, the critical-task guard clears
`scheduler_alive` and the daemon exits non-zero for its supervisor to restart. It writes no
ledger row and no status; the log line is its only artifact, and a journal read error disarms it
silently (`ALC-48`).

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
decision, and per deferred funding goal; it returns `scored` with `gated_eligible` and
`deferred`. It is the only surface for a floor-deferred shortfall.

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
