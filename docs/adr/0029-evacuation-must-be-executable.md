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
   IS a genuine refusal today, unlike the fee cap). This is
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
what gets compared against that route's cap — the cap bounds the FEE, computed on the net, never
the net itself.

**Strict ordering stays strict even when it costs operations — but only over routes that carry
more than they cost.** With a base+proportional cap the two goals in this ADR can disagree: a
high-ppm shared gateway has a narrow feasible window and may serve only a small net, while a hop
pair would serve the full drain. The swap still wins, and the source drains in several operations.

"Chunking is slow, not lossy" is only true once a chunk must deliver at least what it costs, and
that condition has to be enforced rather than assumed. The cap's BASE component is
amount-independent, so at the lnv2 contract floor (5 sats) a 200-sat base cap admits a chunk that
burns ~200 sats to move 5. The remainder re-emits every watch cycle with no minimum-progress
guard, no attempt budget and no fee accounting, so a 75,000-sat balance drains in ~366 such
chunks — delivering ~1,828 sats and burning ~73,172, a ~97.6% loss. That is not slow-but-safe; it is
the evacuation destroying the balance it exists to rescue.

**The parameterisation matters, and an earlier draft of this ADR got it wrong.** It said "base
just under 200 sats", which CANNOT execute at the pin: `PaymentFee` derives a base-first
lexicographic `PartialOrd` (`gateway_api.rs:190-200`), and the send leg is refused when its fee
exceeds `SEND_FEE_LIMIT` (base 100 sats — `lnv2-client/src/lib.rs:590`, limit at
`gateway_api.rs:209`) while the receive leg is refused against `RECEIVE_FEE_LIMIT` (base 50 sats —
`lib.rs:905`, limit at `gateway_api.rs:223`). A 199-sat-base gateway therefore fails the first
chunk's receive and the evacuation STRANDS (`Retryable`); it does not burn. What the SDK's limits
do NOT prevent is the PPM: because the comparison is lexicographic on `base` first, a compliant
base admits an arbitrary ppm.

So the executable hostile shape is **bases 99 + 49 = 148 sats with ppm ≈ 10,430,000**, and the
~200 sats burned per chunk is the BASE PLUS THE PPM TERM, not a 200-sat base. Derivation, because
the numbers must be reproducible: a chunk fits while `148 + r·n ≤ 200 + 0.03·n`, so the largest
fitting net is `n* = 52/(r − 0.03)`; the sizing search returns that maximum, so `r ≈ 1043%` gives
`n* = 5` sats at a fee of ~200 sats, a source debit of ~205, and `75,000/205 ≈ 366` chunks.
NOTE the viability threshold is far below that: `fee(n*) > n*` only for `r > ~28.2%`
(ppm ≈ 282,200). "Ppm far above the 3% slope" does NOT characterise the hazard — at 10% the route
is economically VIABLE and a refusal assertion would fail. Any fixture must pin both numbers.
The hazard is undiminished; only its attribution moves. State the base/ppm split whenever this
scenario is cited, because "the fee limits do not prevent it" is true of the ppm and false of the
base; on the measured pilot gateway at 1.78% the same mechanism is EXPECTED to drain in one operation.
That expectation is EMPIRICAL, not derived, and this ADR previously claimed a derivation twice
over — both wrong, recorded here so neither is reconstructed. The `2A` robustness contract in
br-y2j is a FEE-SLACK guarantee; it does not bound how far the selected net sits below the true
maximum, so it cannot bound the leftover balance at all. And even read as an amount bound, ~18,000
msat at eleven tiers EXCEEDS the ~13,000-msat minimum source debit of a second chunk, so it would
not exclude one. What actually holds: whether a second chunk is emitted is a MEASURED property of
the sized amount, which the implementing bead pins as a red/green fixture, and if one is emitted
the economic-viability post-check bounds its damage rather than the chunk being free to strand
97% of the balance. Do not restate the one-operation property as a consequence of `2A`.

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
  contract uses) — `shortfall <= A`, equality included — probe a bounded number of candidates
  below it before refusing the route. Refuse only when the top fails by strictly MORE than `A`,
  or when the bounded probe finds nothing. The boundary belongs to the probe, not the refusal:
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
[ADR-0004](./0004-v1-lightning-only.md) still holds, and it appears not to have been weighed.

**The balance cap stays where it is.** The fallback still fails if the source federation's own
gateway is down, or if no gateway on either side has liquidity, so it reduces the probability of
stranding without making evacuation reliable. Treating it as reliable would justify raising the
per-federation cap, and that is the change that could actually lose money if the assumption proves
optimistic. ADR-0018's low cap remains the real mitigation, and it is now unconditional:
ADR-0018's Consequences RESOLVED the "refuse or warn" ambiguity on 2026-08-05 in favour of
REFUSING wallet-controlled balance increases above the threshold, precisely so this sentence does
not rest on something a user can click past.

## Consequences

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
  THE EXCEPTION, stated here because an implementing bead cannot grant itself one: a vetted list
  is untrusted input and can be grown faster than a bounded per-tick scan retires it, so
  requiring completed coverage unconditionally lets a hostile guardian stall an evacuation until
  the federation is gone. If a sweep has not achieved coverage within a budget sized well inside
  the shutdown detection lead, the hop MAY be taken with coverage incomplete, and the reason MUST
  be recorded as such. This is a deliberate departure from strict ordering and it is bounded by
  the same principle that motivates the fallback in the first place: during an evacuation a worse
  counterparty beats stranding the balance. It is NOT a licence to hop on a partial scan in the
  ordinary case — absent the deadline, coverage is required. No reputation or liquidity bar gates the
  fallback: during an evacuation a worse counterparty beats stranding the balance, and every extra
  bar is another way the escape hatch fails to open.
- **Neither change makes evacuation reliable**, and no document should claim it does. The honest
  statement remains: evacuation is best-effort, bounded by the balance cap, and dependent on at
  least one gateway being reachable on each side.
