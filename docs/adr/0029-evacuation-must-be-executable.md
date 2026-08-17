---
status: accepted
---
# Evacuation must be executable: a proportional fee cap, and a second route when no gateway is shared

Two changes so that draining a dying federation happens in AS FEW OPERATIONS AS THE ROUTE ALLOWS
— one where the route can carry it — over a route that exists, rather than in ~27 fee-capped
chunks or not at all when no gateway is shared. (The Amendment below records the one case where
this deliberately stays multi-operation — a shared route serving only a small net still wins over
a hop that could drain the balance, because the swap is cheaper — and the condition that makes
that safe: every chunk must deliver at least what it costs, or the route does not serve at all.)

1. **`Evacuate`'s fee cap becomes base + proportional** — an absolute allowance plus a percentage
   of the amount (starting point: **200 sats + 3%**), replacing today's single absolute
   `snapshot.max_fee`.
2. **`Evacuate` gains a second route**: when no gateway serves both federations, it may pay B's
   invoice from A over real Lightning through two different gateways, instead of refusing (routing
   IS a genuine refusal today — as the fee cap also is when NO amount fits, though it downsizes
   first where it can). This is
   a **best-effort fallback, tried only after the shared-gateway swap**, and it is explicitly NOT a
   guarantee.

`Move` is unchanged: it keeps the swap-only path and its proportional cap. Routine rebalancing can
safely decline and wait; evacuation cannot.

This ADR supersedes, in part, Q1 and Q2 of
[docs/archive/route-economics-decisions.md](../archive/route-economics-decisions.md): Q1's single
`Option<GatewayUrl>` on `Action::Evacuate` (the hop needs a route kind plus two identities), and
Q2's endpoint-only "use `action.gateway` iff it still validates" hint rule, which br-s0e replaces
with a membership re-check for `Move` as well as `Evacuate`.

## Amendment: what "serves both ends" means, and what strict ordering costs

Both statements above turn on a gateway "serving" a route, which the original text used as if it
were self-evident. It is not, and the loose reading is the one that livelocks.

**A gateway SERVES when it is on the relevant vetted list, VALIDATES, and a viable amount can
actually be sized over it.** Registry presence is not the test: a gateway that is listed but dead,
or that cannot price the route, does not serve it, and treating it as though it did leaves a dying
federation with a listed-but-useless gateway and no way out — precisely the incident the second
route exists for.

*Which* vetted list depends on what is being served, and the shared route and the hop need
different predicates: a **shared route** wants a gateway vetted by BOTH federations, while a **hop
leg** needs only the one federation at its end, each leg judged separately. Note the shared-route
half is the intent rather than today's behaviour — automated selection starts from the
destination's list and validates the source end only by fetching `routing_info` — so closing that
gap is implementation work this ADR names, not a property to assume. `CONTEXT.md` carries the
canonical wording.

Being unable to fund the **full** ask is not a failure to serve. `InsufficientBalanceError` at the
desired amount is the ordinary downsize signal, not a fall-through trigger; reading it as one
would route every full-balance evacuation onto the dearer hop while a healthy shared gateway sat
idle. This has a structural consequence: route selection can no longer precede sizing. Each
candidate is sized with its own fee bases, and the fee charged at its resulting executed net is
what gets compared against that route's cap — the cap — itself computed on the NET — bounds the fee, which is CHARGED ON THE GROSS
(the two are different quantities; conflating them is the error recorded twice below), never
the net itself.

**Strict ordering stays strict even when it costs operations — but only over routes that carry
more than they cost.** With a base+proportional cap the two goals in this ADR can disagree: a
high-ppm shared gateway has a narrow feasible window and may serve only a small net, while a hop
pair would serve the full drain. The swap still wins, and the source drains in several operations.

"Chunking is slow, not lossy" is only true once a chunk must deliver at least what it costs, and
that condition has to be enforced rather than assumed. The cap's BASE component is
amount-independent, so at the lnv2 contract floor (5 sats) a 200-sat base cap admits a chunk that
burns ~200 sats to move 5. The remainder re-emits every watch cycle with no minimum-progress
guard, no attempt budget and no fee accounting, so a 75,000-sat balance drains in ~365 such
chunks — delivering ~1,953 sats and burning ~73,047, a ~97.4% loss. That is not slow-but-safe; it is
the evacuation destroying the balance it exists to rescue.

**The parameterisation matters, and an earlier draft of this ADR got it wrong.** It said "base
just under 200 sats", which CANNOT execute at the pin: `PaymentFee` derives a base-first
lexicographic `PartialOrd` (`gateway_api.rs:190-200`), and the send leg is refused when its fee
exceeds `SEND_FEE_LIMIT` (base 100 sats — `lnv2-client/src/lib.rs:590`, limit at
`gateway_api.rs:209`) while the receive leg is refused against `RECEIVE_FEE_LIMIT` (base 50 sats —
`lib.rs:905`, limit at `gateway_api.rs:223`). A 199-sat-base gateway therefore cannot execute, and the evacuation fails
rather than burning — but WITH DIFFERENT TERMINAL CLASSES by split, and the send-heavy one is the
worse shape. A receive-heavy split fails pre-commit in `MultiClient::receive` and maps `Retryable`.
A send-heavy split fails at the send limit, and `GatewayFeeExceedsLimit` is a route rejection that
`MultiClient::pay` identifies with `is_route_send_rejection` and maps to
`SendError::RouteRejected`; the executor's `map_send_error` then classifies it
**`Permanent`** — so the intent terminally FAILS with a committed receive
outstanding, and because the occurrence advances each watch cycle and `idem_evac` embeds it, a
fresh `Evacuate` is emitted and repeats the mint-then-fail loop. No burn either way; do not write
a test or runbook expectation asserting `Retryable` for the send-heavy case. WHICH leg refuses depends on the split, and the
send-heavy case is the one to reason from: a receive-heavy split (say 49 + 150) fails the receive
limit before anything commits, but a send-heavy split (say 149 + 50, WITH A RECEIVE PPM AT OR UNDER 5,000 —
at a receive base of exactly 50 the lexicographic `le` ties on base and falls through to the ppm,
so a higher one refuses pre-commit and the example goes vacuous) PASSES the receive limit,
mints and commits the receive leg, and strands at the send-limit check — with a committed receive
already outstanding. Either way some leg of a 199-sat total must exceed its limit, since the
compliant maximum is 100 + 50. What the SDK's limits
do NOT prevent is the PPM: because the comparison is lexicographic on `base` first, a compliant
base admits an arbitrary ppm.

So the executable hostile shape is **bases 99 + 49 = 148 sats with an ASYMMETRIC, SEND-HEAVY ppm
split: send 940,000 ppm, receive 10,000 ppm**, and the ~200 sats burned per chunk is the BASE
PLUS THE PPM TERM, not a 200-sat base.

br-y2j owns the pinned fixture and every number in it: do not re-derive them here. Two facts
belong in this ADR because they are decisions, not arithmetic — the SDK's limits bound each leg's
BASE (100 sats send, 50 receive) but not the ppm, since the derived `PaymentFee` comparison is
lexicographic on `base` first; and solvability is governed by the RECEIVE ppm alone, because the
contract is `a − (rb + rp·a)`. Never cite a combined-ppm threshold.

**So serving requires ECONOMIC viability: `total_fee <= executed net`.** A route whose best
available chunk costs more than it delivers does not serve, strict ordering falls through to the
hop, and if neither class serves the evacuation stays `Retryable` — stranding rather than burning,
which is the posture [ADR-0018](./0018-v1-evacuation-balance-cap.md) already accepts.

Three properties of that rule worth stating, because each is easy to get wrong:
- **It is a post-check on the search result, never a term in the fits predicate.** `fee(n) <= n`
  is false at small `n` and true above `base/(1 - rate)`, so folding it into the bisection would
  re-break the fits-then-doesn't monotonicity that search depends on.
- **Check the search's top FIRST, but do not treat it as a proof.** For the affine model
  `fee(n) = base + rate*n`, efficiency `fee(n)/n = base/n + rate` decreases monotonically in `n`,
  so the largest fitting amount is also the most efficient — and that is the right place to look.
  But the real quote is not affine: the two gateway fees, the two federation fees and the
  per-note MINT fee each floor independently, and the note COUNT can change between adjacent
  amounts, so `fee` can jump by more than the one-msat gain in `n`. At such a boundary the top
  can fail `fee <= n` while a slightly smaller candidate passes.
  So: if the top fails by AT MOST the oscillation bound `A` (the same `A` the robustness
  contract uses) — `shortfall <= A`, equality included — probe candidates below it before
  refusing the route. `A` bounds ONE vertical fee jump; it bounds neither the number of
  discontinuities between a failing probe and a feasible window nor their horizontal spacing, so a
  BOUNDED probe cannot guarantee "finds every amount that fits with `2A` of slack". Do not claim
  it does. The contract is weakened deliberately: probe the adjacent note-selection boundaries,
  bounded, and ACCEPT the residual that a feasible window separated from the probe by more
  boundaries than are visited will be missed and the evacuation will keep retrying. That residual
  is bounded in consequence (a retry, not a burn) and is the price of not scanning a
  proven-complete boundary set. br-y2j carries a fixture whose feasible amount sits one boundary
  from the failing probe; it demonstrates the mechanism, not completeness. Refuse only when the top fails by strictly MORE than `A` AND that is an analytically proven
  structural refusal — `A` bounds ONE fee jump, so several boundaries can cumulatively exceed it
  with a serving amount still beyond them; a bare shortfall over `A` is inconclusive, not proof —
  or when the bounded probe finds nothing AND that emptiness is an analytically proven
  structural refusal. Probe EXHAUSTION alone is inconclusive: it stays `Retryable` and must not
  mark the route unavailable, which is the residual this ADR accepts a few lines above. The boundary belongs to the probe, not the refusal:
  `br-y2j` must state the same `<=` or an implementation refuses an executable evacuation at
  exactly `A` — refusing on
  a single top-only reading would discard an executable evacuation at a note-count discontinuity.
- **It applies per route class, including the hop.** The hop stacks two gateways' bases and is
  ~80% dearer on the send leg, so its floor efficiency is worse, not better. The defect is in the
  cap shape and the acceptance rule, not in which route is chosen.

Accepted residual: a gateway pricing exactly at `fee == net` still extracts up to half the balance
across chunks. An aggregate per-evacuation fee budget would bound that, at the cost of durable
spend accounting and an episode identity across occurrence-keyed intents; it is the named
follow-up if that residual is judged unacceptable, not part of this decision.

**Nothing here concerns gateway pins.** Automated routing is never pinned; see
[ADR-0030](./0030-automated-routing-is-never-pinned.md). An earlier draft of the implementing bead
specified a four-case pin-precedence table for evacuation — that table is deleted, not answered.

## Why

> Line references in this section cite the tree at `ed0d679`, where this ADR was recorded — the
> code the diagnosis is ABOUT. Enforcement landed later (`e9cc97d`) and moved several of them.
> They are deliberately NOT renumbered: today's `size_fresh_evacuation` no longer searches against
> a constant `fee_cap`, so a citation pointing at today's lines would attribute the diagnosed
> defect to the code that fixed it.

**The absolute cap cannot fund a real evacuation, and it fails in TWO different ways depending on
the gateway's fee shape.** Neither is what an earlier draft of this ADR claimed; both were
established by reading the code rather than reasoning from the parameter.

`size_fresh_evacuation` (`executor.rs:606-667`) searches for the largest net that satisfies
`total_within_cap` — the sum of BOTH legs' quotes, each including the gateway's BASE fee, against
`fee_cap` (`fee.rs:160-164`, `executor.rs:814-829`). What happens next depends entirely on whether
any candidate clears that predicate:

1. **Base fees below the cap → CHUNK-DRAIN.** A smaller net fits, so the evacuation is silently
   downsized (`executor.rs:654-663` warns "reducing fresh evacuation amount"). The allocation
   occurrence advances every watch cycle (`scheduler.rs:692-697`) and `idem_evac` embeds it
   (`allocator.rs:629`), so a fresh `Evacuate` is emitted for the remainder. At the figures below
   the federation drains in roughly 27 operations at essentially the same total fee. This is what
   the pilot's MEASURED gateway does.
2. **NO AMOUNT FITS THE CAP → GENUINE REFUSAL.** The condition is about the whole search, not
   one quote: a single over-cap quote at the desired size is NOT a refusal, because
   `size_fresh_evacuation` downsizes (see the sizing rules above). Genuine refusal is when no
   amount fits at all — characteristically when the fixed component alone (the two legs' bases
   plus the fee floor) already exceeds the cap, so shrinking the amount cannot help.
   The per-quote test underneath it is: **a summed two-leg quote STRICTLY ABOVE the cap fails.** State it about the
   QUOTE, not about base fees: `total_within_cap` compares `receive_quote + send_quote <= fee_cap`
   (`wallet-fedimint/src/fee.rs:163`). Exact equality is ADMITTED — only cap-plus-one-msat
   refuses, the same boundary as the `shortfall <= A` probe rule. Note the consequence for bases
   specifically: bases summing to exactly the cap still refuse whenever ANY other component (the
   ppm parts, federation or mint fees) is nonzero, because the comparison is on the total. "Base
   fees at or above the cap" is wrong twice over — wrong term, wrong boundary. The base component does not shrink with
   the amount, so if the two legs' bases alone exceed `fee_cap`, NO candidate ever fits and the
   executor returns `Retryable` (`executor.rs:646-653`) on every tick — a livelock, not a terminal
   failure, so it retries silently forever. A gateway is permitted bases summing to ~150 sats
   against the runbook's 50-sat cap, so this is not hypothetical.

Both defeat the intent. `allocator.rs` passes `fee_cap: snapshot.max_fee` for `Evacuate` directly
beneath a comment stating that "route economics NEVER gates an evacuation — a dying federation must
be drained even when the route prices badly". The parameter contradicts the intent. At the
runbook's own suggested settings, using fees measured on the pilot gateway:

| | |
|---|---|
| per-federation cap | 75,000,000 msat |
| measured swap cost at 1,000,000 msat | 17,848 msat (1.78%) |
| same rate on a full federation | ~1,338,600 msat |
| `--max-fee` absolute cap | 50,000 msat |
| | **~27× over — chunk-drain on this gateway; refusal on a higher-base one** |

No evacuation has ever run in production, so neither failure mode has been observed with real
funds.

**A flat percentage would fail at the other end.** A gateway's INTENDED fee envelope is
`SEND_FEE_LIMIT` (100 sats + 1.5%) plus `RECEIVE_FEE_LIMIT` (50 sats + 0.5%) — about 150 sats + 2%.
Treat that as a design intent, NOT an enforced bound: at our pinned SDK revision the limits do not
actually constrain a gateway, because `PaymentFee` derives a lexicographic `PartialOrd` over
(`base`, `parts_per_million`) and the check is a single `.le(...)`, so a fee with a small base and
an arbitrarily large ppm passes. (This is the upstream defect this project reported separately; our
own cap is what really bounds us, which is another reason it must be shaped correctly.) The base
component dominates at small amounts: a legitimate worst-case fee is 2.2% of a
75,000-sat evacuation but 17% of a 1,000-sat one. A pure percentage refuses exactly the
evacuations that are cheapest in absolute terms, which is the same defect mirrored. Base +
proportional tracks the real cost at every size, which is why the fees themselves are shaped that
way.

**Relationship to [ADR-0018](./0018-v1-evacuation-balance-cap.md), which decided the adjacent
question the other way.** ADR-0018 chose a hard low balance cap *instead of* an escape hatch,
accepting that a dying federation may "strand a capped amount until/unless recovery", with a
gateway-independent escape "pulled into EARLY v2". This ADR does not overturn that. What ADR-0018
deferred was **gateway-independent** escape — on-chain peg-out. A two-gateway Lightning hop is
still gateway-*dependent*: it relaxes "one gateway serves both federations" to "each federation has
some gateway". That is a genuinely weaker requirement, it is Lightning-only so
[ADR-0004](./0004-v1-lightning-only.md) still holds, and it appears not to have been weighed as an EXECUTABLE route. Provenance, to be fair to
the earlier decision: ADR-0004 already names the ladder as "shared-gateway swap, then
public-Lightning", so the rung was chosen there. What was never worked out is how to make it
execute — the fee cap, the sizing, the route kind — which is what this ADR supplies.

**The balance cap stays where it is.** The fallback still fails when NO vetted source-side gateway serves or is
reachable — there is no singular "the federation's gateway"; each leg selects from that
federation's vetted list — or if no gateway on either side has liquidity, so it reduces the probability of
stranding without making evacuation reliable. Treating it as reliable would justify raising the
per-federation cap, and that is the change that could actually lose money if the assumption proves
optimistic. ADR-0018's low cap remains the real mitigation, and it is now unconditional:
ADR-0018's Consequences RESOLVED the "refuse or warn" ambiguity on 2026-08-05 in favour of
REFUSING wallet-controlled balance increases above the threshold, precisely so this sentence does
not rest on something a user can click past.

## Consequences

- **The ledger reports the pair that EXECUTED, not the pair that was planned.** Because the cap
  is recomputed at the net the evacuation sized down to, a row still showing the planned amount
  and the planned cap describes a move that never happened, and a post-incident fee audit would
  clear fees the enforced cap refused. The two are therefore refreshed together onto the ledger
  row from the `MoveRecord` that holds them, never one alone: `amount = planned,
  fee_cap = enforced` is internally false, since recomputing the cap from the displayed amount
  yields a different number. They are refreshed only ONCE A LEG HAS COMMITTED — before that the
  move row is a re-sized draft, and a pre-mint refusal would otherwise freeze a never-executed
  pair onto an immutable terminal row.
- **Reading the enforced cap back needs `--standalone`, today.** `wallet-cli --standalone show`
  prints `amount_msat` and `fee_cap_msat` adjacent (`print_show_record`), and its `--json` emits
  the whole `OperationRecord`. Neither daemon-backed view carries the cap: `history` has no cap
  column, and client-mode `show` renders `OperationView` (`wallet-api/src/lib.rs`), which has
  `amount`, `receive_fee` and `send_fee_quoted` and no `fee_cap` field at all. So the row is now
  correct, but on a normal deployment an operator cannot yet see it — a gap this ADR's
  implementation exposes rather than creates, tracked separately.
- **`Evacuate` and `Move` no longer share a fee-cap shape.** A reader comparing them will find
  `Move` proportional-only and `Evacuate` base + proportional; that asymmetry is deliberate and
  exists because only one of them must succeed.
- **The cap numbers are a starting point, not a derivation.** 200 sats + 3% covers the gateway's INTENDED
  envelope (~150 sats + 2%) with headroom at every size. That envelope is not enforced at our pin
  (see above), so this is not a proven bound on the wallet's total cost either: federation receive/send fees and mint-note fees also apply, and those are not bounded
  here. Treat the constants as a pilot starting point, not a worst-case derivation. They should be
  revisited if the per-federation cap changes or if measured gateway fees move.
- **The fallback costs more.** Measured on the pilot gateway, the external send leg was 16,064
  msat against 8,948 for the swap on the same 1,000,000 msat — roughly 80% dearer, because the
  lnv2 internal-swap discount (`send_fee_minimum`) does not apply across two gateways. That is
  accepted: it only applies when the alternative is not moving at all.
- **The fallback adds no new trust.** Both legs remain hash-locked, and the destination client
  derives the preimage, so a gateway on either side still cannot take funds without delivering.
  See [ADR-0018](./0018-v1-evacuation-balance-cap.md) and the `Stranded` analysis for what remains
  reachable — a Byzantine destination federation, which is the standard custodial assumption and
  is unchanged by route count.
- **Route ordering is strict, with ONE bounded exception.** The swap is always tried first
  because it is cheaper; the hop is reached only when no gateway serves both ends — established
  by examining the whole shared candidate set, not a prefix of it.
  THE EXCEPTION covers BOTH truncation causes, because either can leave a serving shared
  candidate unexamined: the candidate-COUNT bound (a guardian can inject entries without bound) and
  the WALL-TIME bound (even a short, honest list can exhaust the per-class deadline if its
  gateways answer slowly). In both cases the hop MAY be taken with a shared candidate unexamined —
  an honest list is not guaranteed a complete scan, only a bounded one.
  MIND THE HOP CLASS'S CARDINALITY, AND EXCLUDE THE DIAGONAL: the pair set is
  `{(s, d) : s ∈ source, d ∈ destination, s ≠ d}`. `N` IS A FIXED CONSTANT, NOT THE PAIR COUNT —
  sizing it FROM the product would make it attacker-controlled, since the union is unbounded.
  Choose it to cover the honest pair space — two six-gateway lists give 36 when DISJOINT (30 only
  if they fully overlap), so 36 is the floor if six-per-side is the design point and accept the same residual the shared class carries: beyond that, a viable pair can sit
  outside the window and not be reached this tick. Covering the honest space is a sizing GOAL, not
  a guarantee under attack. Where the two vetted lists overlap the raw
  product contains `(g, g)`, which is NOT a hop — CONTEXT.md defines a hop as TWO gateways, and
  pricing `(g, g)` with external-send assumptions would persist an unavailable shared gateway as
  though it were one. The cardinality is therefore `|source| × |destination| − |overlap|`, so two honest six-gateway
  lists already exceed a 32-candidate bound without any adversary at all. The bound must therefore
  be sized against the PAIR space. The alternative of traversing the Cartesian product ACROSS
  ticks is NOT available here: this design keeps no cross-tick state and takes a fresh prefix of
  an order the SDK reshuffles per call, so there is nothing to advance. Without a product-sized
  per-tick bound a sole viable low-support pair sits outside a support-ordered window every tick
  and is never reached. "Honest lists fit one scan" is true of the shared class and false of the hop.
  The count bound is —
  count alone is not enough. Fetch `routing_info` ONCE PER UNIQUE `(federation, gateway)` LEG for
  the whole perform, and reuse it across every pair that mentions that leg AND across both sizing
  passes. Per-PAIR fetching repeats identical lookups — two six-gateway lists need 12 unique leg
  snapshots but would make up to 72 calls — and at the 10-second request timeout that alone can
  exhaust `perform_timeout` before the hop is ever reached,
  cancelling the operation and restarting the scan from nothing. Bound the elapsed time per route
  class so the hop is always attempted within the operation's budget — AND SO THAT SETTLEMENT
  STILL FITS. A budget sized only to REACH the hop can consume `perform_timeout` before invoice
  creation and payment, so the future is cancelled and the next tick restarts the scan: a livelock
  that satisfies "the hop was attempted" while never completing. The scan deadlines must reserve
  explicit time for commit, and the acceptance case must assert COMPLETION, not merely that the
  hop was reached. A longer list is scanned
  truncated —
  the hop is therefore reachable without every shared candidate having been examined. A bounded, deliberate departure, stated precisely because the honest version is
  weaker than "never a stranding". `gateways()` shuffles and then STABLE-sorts by how many peers
  lack each URL, so the window is support-ordered and randomised only WITHIN equal-support ties.
  Two consequences, and neither is "random sampling": a low-support serving candidate sitting
  behind a full window of higher-support entries is excluded DETERMINISTICALLY, every tick, not
  probabilistically; and conversely a single guardian's injected entries sort LAST and cannot
  displace a widely-vetted serving candidate from the window at all. What follows in the ordinary case is a dearer route — the hop is
  tried meanwhile, still capped and viability-checked. Stranding needs BOTH the shared candidate
  to keep being missed AND no examined hop pair to work; that is unlikely but not excluded, and it
  is the price of not carrying cross-tick state. During an evacuation a worse counterparty beats
  stranding the balance, which is why the hop is tried rather than waiting for coverage. It is
  NOT a licence to hop while an examined candidate serves.
  No reputation or liquidity bar gates the
  fallback: during an evacuation a worse counterparty beats stranding the balance, and every extra
  bar is another way the escape hatch fails to open.
- **Neither change makes evacuation reliable**, and no document should claim it does. The honest
  statement remains: evacuation is best-effort, bounded by the balance cap, and dependent on at
  least one gateway being reachable on each side.
