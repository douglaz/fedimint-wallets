---
status: accepted
---
# Evacuation must be executable: a proportional fee cap, and a second route when no gateway is shared

> Rewritten 2026-09-09 to record the design as settled after the routing model changed
> (ADR-0030). The earlier text specified the hop as a scan over source×destination gateway
> PAIRS with a fixed bound, and required an "analytically proven" refusal before the hop could be
> tried; two independent reviews found the pair space illusory (the legs are independent) and the
> proof requirement not load-bearing. The history, including the fee-limit arithmetic that
> br-y2j pinned, is in git and in `docs/spec/06-allocator-and-automation.md` (`ALC-20`..`ALC-24`).

Draining a dying federation must happen in as few operations as the route allows, over a route
that exists. Two decisions make that true, and a third bounds what they may cost.

## 1. `Evacuate`'s fee cap is base + proportional

An absolute allowance plus a percentage of the amount, starting at **200 sats + 3%**, replacing
the single absolute `max_fee`. **Shipped** (`ALC-20`). The absolute cap failed both ways: with
gateway bases below it a full-balance drain was silently chunked into ~27 operations; with bases
above it no amount ever fit and the evacuation retried forever. A flat percentage would have
refused exactly the small evacuations that are cheapest in absolute terms. Base + proportional
tracks the way gateway fees are actually shaped. `Move` keeps its proportional-only cap: only
evacuation must succeed.

Two facts about the SDK at the pinned revision govern the numbers and stay decisions here: the
SDK's send/receive fee limits bound each leg's BASE (100 sats send, 50 receive) but not the ppm,
because `PaymentFee` compares lexicographically on base first; and receive-side solvability is
governed by the receive ppm alone. br-y2j owns the pinned fixture and every number in it.

**Serving requires economic viability: `total_fee <= delivered net`**, checked as a post-check on
the sizing result, never folded into the search. A route whose best chunk costs more than it
delivers does not serve. Without this, the cap's base component admits a chunk that burns ~200
sats to move 5, and a 75,000-sat balance drains in ~365 such chunks losing ~97%. With it, chunking
is slow, not lossy. Accepted residual: a gateway pricing exactly at `fee == net` can still take up
to half the balance across chunks; an aggregate per-evacuation fee budget is the named follow-up
if that is ever judged unacceptable.

## 2. `Evacuate` gets a second route: a hop over two Lightning nodes

When no gateway serves both federations, the evacuation may mint B's invoice through a gateway B
vets and pay it from A through a gateway A vets, the payment travelling over Lightning between
the two gateways' nodes. `Move` never hops.

- **A hop is two gateways on two different Lightning NODES.** The SDK detects an internal swap by
  comparing the invoice's payee node key with the sending gateway's own node key. Two URLs on one
  node are one gateway: the sender would take the swap path, look for the incoming contract in its
  own database, fail to find it, and refund, every attempt. Both `routing_info` responses carry
  the node key, so the rule is one comparison.
- **The legs are priced independently and paired only by the node-key rule.** Each leg's price
  is its own: the destination leg's receive fee on B, the source leg's EXTERNAL send fee
  (`send_fee_default`, not the swap discount) on A, both read from the `routing_info` already
  fetched to validate, so every candidate's fee is in hand with no further network call. The
  route is the combination with the MINIMUM composed cost at the ask over the validated
  destination gateways and validated source gateways on DIFFERENT nodes, where the composed
  cost is the FULL route cost `quote_fresh_send_required_cost` computes at the ask — the
  destination's receive fee and federation receive fee grossing the ask up to the invoice, then
  the source's external send fee and federation send fee on the contract that invoice implies
  (`FreshMoveCost::total_fee()`) — never the two gateway schedules each read at the bare ask.
  The federation fee quotes are local database reads, so this ranking costs no network calls.
  Ties break on lower receive fee, then on the ordered pair of URLs. "Validated" means
  the candidates examined before the class's time bound below, whether or not that is every
  listed gateway; with none on either side, or no distinct-node combination among them, the
  hop does not serve this attempt. That is a local comparison over two short lists already
  in memory, not a scan: no pair bound, no diagonal rule, and no network cost beyond the
  validations. Sizing then runs the existing search with that fee pair — and if the search
  finds no viable amount, the NEXT combination in composed-cost order is sized, and so on
  until one serves or the class's time bound is reached. Fee curves cross: the combination
  cheapest at the ask can have no viable chunk while a dearer one fits at a smaller amount,
  and since empty sizing writes no set-aside, stopping at the first would select the same
  unusable pair every occurrence. Sizing is local arithmetic over quotes already in hand, so
  this costs no network calls. (A "first destination with any compatible source" shortcut is
  wrong for the same reason in the other direction: with destination fees X:1, Z:2 and source
  fees X:1, Y:100 it picks X/Y at 101 over Z/X at 3.)
- **Bounded, with settlement time reserved.** ONE per-attempt deadline is fixed when fresh
  routing starts and passed through both classes in turn; each class's validations run against
  the time remaining under it (the shape `resolve_fallback_move_gateway` already has, but
  shared, not one bound per class), and the deadline itself sits far enough inside
  `perform_timeout` that invoice creation and payment still fit after it. Today's fresh
  evacuation path has no such bound at all. A scan that hits the deadline is journaled as
  truncated, and the hop is chosen from the candidates examined. Without this,
  enough slow-but-listed gateways consume the whole perform budget before anything is minted,
  the driver is cancelled, and the next occurrence repeats the scan: a livelock that "attempted"
  the hop without ever completing it.
- **Whether the two nodes can reach each other is learned only by paying.** A refunded send leaves
  the destination's contract to expire unclaimed, terminates that operation, and the next
  occurrence emits a fresh evacuation; the invoice is single-use and is never re-paid. The
  principal is not transferred but is not lost; the FEES are: the source federation charges
  transaction and mint fees for each fund-and-refund cycle, so repeated failed attempts do
  reduce the balance being rescued, and a stateless "cheapest" choice would repeat the same
  failure every tick. Hence the record in §3, which exists to bound exactly that loss.
- **Strict ordering: swap first, hop only when no shared gateway serves THIS attempt.** The swap
  is cheaper (measured: 8,948 vs 16,064 msat per million on the pilot gateway). A shared gateway
  that serves only a small chunk still wins and the source drains in several operations; §1's
  viability rule is what makes that safe. The two classes are never sized to compare them.
- **"Does not serve this attempt" is EVIDENCE, not proof.** A shared gateway is unavailable for
  this attempt when it is absent from either federation's vetted list, fails validation, its
  quotes error or time out, it is set aside by §3, or the bounded sizing search over it — run
  AFTER the ordinary downsizing, so a full ask the source cannot fund is sized smaller, never
  read as unavailability — ends with no viable amount, whichever shape that result takes
  (`Refused` or `StructuralRefused`; both are "not this attempt"). That is inconclusive as a
  statement about the route and that is fine: the hop is tried in the same tick, and the next
  fresh attempt starts swap-first again. Structurally this means route selection and sizing
  become ONE loop inside the fresh path — try a candidate, size it, on nothing viable try the
  next, then the next class — replacing today's single resolution before sizing, whose
  refusals return from `perform` before any other route is considered. What "proof"
  would have bought is avoiding a ~0.7%-dearer route on a tick where the bounded search missed
  a swap amount; what it cost was stranding a dying federation behind a distinction the code
  could not compute. The earlier requirement is withdrawn, and `br-u4i` with it.
- **Fallthrough happens only at fresh sizing.** Once a leg has committed, the recorded route is
  authoritative through settlement (ADR-0030 rule 4); a minted invoice is bound to the gateway
  that minted it, so a route is never switched mid-operation.
- **Being unable to fund the full ask is not a failure to serve.** `InsufficientBalanceError` at
  the desired amount is the downsize signal; reading it as a fall-through would route every
  full-balance evacuation onto the dearer hop while a healthy shared gateway sat idle.
- **No bar on the destination gateway** beyond vetting, validation, cap and viability. Both legs
  stay hash-locked, so the hop adds no new trust; during an evacuation a worse counterparty beats
  stranding the balance.

## 3. A gateway that quoted but did not perform is set aside

A bounded, in-memory set-aside with a skip-until time, written on a PERFORM-level failure only
and consulted by both route classes and by `Move`. Without it a gateway that answers
`routing_info` and then hangs or refunds is reselected every tick and a viable alternative is
never reached; that is the livelock the second route exists to prevent, moved one step along.
The record is as wide as what the failure proved, and no wider:
- an ENDPOINT failure — a rejected receive, a hang past the per-request timeout, a swap's
  refunded send — marks `(federation, gateway)` at the end that failed (a swap failure marks
  the gateway on both federations);
- a HOP's refunded send proves only that this source NODE could not reach this destination
  NODE, so it marks the ROUTE `(source node key, destination node key)` and nothing else —
  node keys on both sides, because two URLs on one source node are one node, and marking a
  URL would let its alias repeat the identical failed route. The same source stays eligible
  for other destinations and for `Move`s, and another source node may still try that
  destination.
An empty sizing result does NOT write the record: it is a fact about one route at one amount on
one attempt, the fallthrough already acts on it this tick, and a gateway-wide mark would wrongly
suppress the same gateway on other routes, including `Move`s. Not persisted: a restart forgets,
which is the right amount of memory for a liveness fact. The duration is a constant with a
`ponytail:` note, not a policy field.

## What this rests on: vetted lists that mean something

Shared candidates come from the INTERSECTION of both federations' vetted lists, a hop leg from
the list at its own end, and a route hint holds only while it is on every list it needs to be
on. A federation's vetted list is the set of gateways that at least `NumPeers::threshold()` of
its guardians each return, read per guardian — the SDK's own consensus threshold,
`n − floor((n−1)/3)`, which is `2f+1` only when `n = 3f+1` (four guardians need three, five need
four); the SDK's flattened union, where one guardian could admit a gateway, is not the list. Both are the routing-invariants work that ships before the
hop. Enforcing the threshold can drop a thinly-registered production gateway from automated
routing on ship day, so support is measured on the real federations first and gateways are
registered on every reachable guardian.

## Why not the alternatives

- **Sizing both classes and picking the cheaper.** Adds a full second sizing search to every
  evacuation to save ~0.7% on the rare tick a swap serves only a small chunk. The strict ordering
  plus viability is enough.
- **A pair scan with a fixed bound (the earlier text).** Defended against an adversarial guardian
  inflating a vetted list; that guardian is already inside the federation the user trusts with
  custody and has cheaper levers. The pair space itself does not exist once the legs are seen
  as independent.
- **A proven-refusal gate before the hop (the earlier text).** Required a classification the
  sizing code cannot compute and blocked the hop on a separate bead. Withdrawn above.
- **On-chain peg-out.** ADR-0004 is Lightning-only; ADR-0018's gateway-independent escape stays
  deferred. The hop relaxes "one gateway serves both" to "each federation has some gateway",
  which is weaker but still gateway-dependent, so ADR-0018's low balance cap remains the real
  mitigation and is not raised on the strength of this route.
- **Reputation or liquidity bars on the destination gateway.** Every extra bar is another way
  the escape hatch fails to open.

## Consequences

- Evacuation is best-effort: bounded by the balance cap, dependent on at least one vetted,
  validating gateway on each side, and on the two nodes being able to reach each other. No
  document may call it reliable.
- The ledger reports the pair that EXECUTED and the cap enforced at that net, refreshed together
  once a leg has committed (`ALC-20`); a hop row shows both gateways, and the operator can read
  which route was taken from `history` / `show`.
- The live gate needs two federations whose vetted gateways sit on different Lightning NODES
  with no node vetted by both — node keys, not URLs, since two URLs can front one node. With
  per-guardian registration (`wallet-cli/tests/devimint_lib.sh`) that is registering devimint's
  LND gateway on federation A only and its LDK gateway on federation B only: `dev-fed` connects
  both gateways to A, and the two-fed harness patch connects and funds only the LDK gateway on
  B, so LDK is the one gateway that can serve B. The two run on distinct nodes by construction.
  No harness change is needed.
- The fee-cap constants are a pilot starting point, not a derivation: federation and mint fees
  also apply and are not bounded here. Revisit if the per-federation cap or measured gateway
  fees move.
