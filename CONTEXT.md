# Simple Fedimint Wallet (working title)

A private, no-KYC, **spending**-focused ecash wallet for people who would
otherwise use a custodial Lightning wallet (Wallet of Satoshi, Blink) but want
more privacy. Not a savings tool: the Fedimint environment is still too
ephemeral to trust with stored value.

## Language

**Federation**:
A quorum of guardians (an m-of-n Bitcoin multisig) that issues ecash backed by
the bitcoin it custodies. Treated as **ephemeral** here: a federation can
degrade or disappear, so it holds spending balances, never savings.
_Avoid_: "mint" (reserve that for the Cashu sense, or the verb), "bank"

**Ephemeral**:
A property of federations in this product: they are not assumed durable. The
wallet is designed so that one federation degrading or vanishing does not strand
the user's ability to spend.

**Allocator**:
The component that distributes the user's spending balance across federations to
keep them able to spend when a federation degrades or disappears (see
[ADR-0001](./docs/adr/0001-allocator-purpose-resilience-not-solvency.md)). Its
goal is resilience/availability, not hedging insolvency.
_Avoid_: "risk engine" (implies it hedges solvency risk, which it does not)

**Spending federation**:
The one federation the Allocator keeps topped up to fund everyday sends. Other
joined federations hold standby spending balance the Allocator can pull from.
There is no "savings federation": this wallet does not store value.
_Avoid_: "primary account", "main wallet"

**Warm standby**:
A small balance the Allocator keeps in one vetted federation *other than* the
Spending federation, so a sudden federation failure never leaves the user with
nothing to spend. The Allocator otherwise stays concentrated (see
[ADR-0006](./docs/adr/0006-allocator-concentrated-warm-standby.md)). Selection is
best-effort diversification only — fedimint exposes no verifiable guardian
identity, so the wallet CANNOT prove the standby is operator-independent and
must not claim that in product copy (ADR-0010 was dropped; ADR-0006 records the
honest posture).
_Avoid_: "guardian-independent", "operator-independent" as a guarantee

**Private** (the precise meaning of "more private than WoS/Blink"):
(1) **No KYC** to start. (2) The provider/federation is **blind to your balance
and history** (blind-signed ecash). (3) **Receiving is fully private**: the
gateway/federation cannot tie received funds to your identity or balance.
(4) **Sending leaks the destination** to the Lightning gateway that routes the
payment, though the provider stays blind to your balance. NOT network-level
anonymity (no Tor in v1, see
[ADR-0002](./docs/adr/0002-no-tor-in-v1.md)).
_Avoid_: "anonymous", "untraceable"

**Silent backup / Recovery**:
The seed and the user's joined federation IDs are saved automatically via Android
Block Store (E2E-encrypted to the user's Google account, keyed to the device
lockscreen), with no seed-phrase ceremony at onboarding. On a new device the seed
restores during setup and balances are rebuilt from it via Fedimint recovery. See
[ADR-0003](./docs/adr/0003-recovery-silent-backup.md).
**The backup unit is the seed plus the joined federation IDs — never the wallet's
local stores.** Recovery rebuilds balances from the seed; it does not reinstate a
point in time. Because the money is recoverable this way, losing the bookkeeping
store loses records, not settled funds.
_Avoid_: making "seed phrase backup" the default flow (it is an opt-in export);
calling a copy of the local stores "the backup"

**Restore** (distinct from **Recovery**):
Copying the wallet's local stores back onto a host — an operator action, not the
product's backup path. The stores are one live unit and carry **no cross-store
point-in-time guarantee**, so they are restored **together from a single snapshot,
or not at all**; a mismatched pair is out of contract. When a store is lost, the
supported path is Recovery from the seed and federation IDs, not a store copy.
_Avoid_: using "restore" for seed-based Recovery; implying the stores can be
restored from different moments

**Shutdown notice**:
A federation's machine-readable announcement that it will cease operating (via
the `federation_expiry_timestamp` meta field or the public `/status` endpoint's
`scheduled_shutdown` — ADR-0019; Nostr is discovery-only, not a shutdown
signal, and the meta field can be served by an override host, so the probe must
corroborate it). The Allocator's
**primary** resilience signal: it is planned and gives a window to evacuate,
unlike a surprise outage. Health/liveness probes are the backstop for *unplanned*
degradation.
_Avoid_: "expiry" unless naming a specific metadata timestamp field

**Evacuation**:
Moving a user's balance out of a failing or closing federation into a healthy one.
NOT on-chain: [ADR-0004](docs/adr/0004-v1-lightning-only.md) is lightning-only and ADR-0018's
gateway-independent escape stays deferred, so a peg-out is not an evacuation route today.
Triggered primarily by a **Shutdown notice**,
secondarily by probes detecting degradation. The Allocator's core resilience
action.
_Avoid_: "sweep" (reserve for consolidating many inputs), "withdraw"

**Serves** (of a gateway, with respect to a route or a leg):
A gateway **serves** when it is on the relevant **vetted list**, validates, and an
ECONOMICALLY viable amount can be sized over it — one whose total fee does not exceed
what it actually delivers. A route that can only carry chunks costing more than they
move does not serve, however many times it would settle. Presence in the registry is NOT the
test: a gateway that is listed but dead, or that cannot price the route, does not
serve it.

Which vetted list depends on what is being served, and the two are different
predicates:
- **A shared route** — one gateway carrying both ends — should be served only by a gateway
  vetted by **both** federations. NOTE this is the INTENT, not today's behaviour: automated
  selection starts from the destination's list (`mc.gateways(&to)`) and validates the source end
  only by fetching `routing_info` (`route_econ.rs:301-333`, `executor.rs:344-349`,
  `multi_client.rs:958-964`), so a responsive gateway vetted only by the destination — or since
  revoked by the source — can still carry an automated move. Closing that gap is work, not
  vocabulary; until it is closed, do not cite this entry as though the invariant holds.
- **A hop leg** is served by a gateway vetted by the **one** federation at that end.
  A hop's source leg and destination leg are judged separately, each against its own
  federation's list; neither gateway need be known to the other federation.

Two things are NOT failures to serve, and conflating either with one is how a healthy
gateway gets abandoned for a dearer route:
- Being unable to fund the *full* ask. That is an ordinary instruction to move less.
- Any single over-cap quote at one amount. Only "no amount fits" is a failure.
Genuine failures are: not vetted, does not validate, its quote errors or times out, no amount
can be sized over it at all, or the best amount it CAN carry costs more in fees than it
delivers.
_Avoid_: "supports", "is available for" — both get read as registry presence, which
is the reading that leaves a dying federation with a listed-but-useless gateway and
no way out.

**Break-glass gateway override**:
An operator's explicit, single-invocation instruction to route through a named gateway
**outside the federation's vetted list**. It exists for one incident: a federation whose
vetted gateways are dead, empty, or unreachable from this host, where consensus still
redeems the ecash but no route to it can be selected. Reaching for it is an incident
action, not configuration — it applies to that invocation and nothing else.
_Avoid_: "gateway preference" and "the operator's chosen gateway" — both frame it as a
policy about which counterparty to trust, when the actual question is whether ANY route
exists. Also avoid "pin": automated routing is never pinned, and calling this a pin is
what let a break-glass be mistaken for a routing policy.

**Money verb**:
A `wallet-cli` subcommand that moves value on behalf of the human running it, as opposed to one
that observes state or drives the automated lanes. Exactly four: `pay`, `receive`, `move`,
`direct-inflow`. These are the only verbs that INITIATE movement, and the
**break-glass gateway override** is accepted on them. The await verbs also accept and carry it,
under the provenance rule in ADR-0030, because they complete a payment a human already started —
so the accepted set is "the four money verbs plus the eligible await verbs", not the four
alone. `direct-inflow` is the one an implementer is most likely to misclassify — it reads like plumbing, but it funds a federation and most devimint
smokes fund through it, so classifying it as rejected or ignored breaks the funding step.

**Vetted list**:
The gateways a federation's guardians have admitted for lnv2. It is the ONLY input to
automated route selection — the scheduler, allocator, probes and evacuation choose from
it and nothing else; an operator's **break-glass gateway override** deliberately steps
outside it, but that is never automated. Two cautions, both from ADR-0030: this rule is
NOT yet implemented (today's daemon still honours a pinned gateway), and membership is a
UNION of what each responding guardian returned, so it is not a threshold property. Adding to it is a guardian action, per guardian, not a wallet one:
an operator with no guardian cooperation cannot change it, which is precisely why the
break-glass exists.
_Avoid_: "registered gateways" when you mean routable ones — presence in the list is not
the same as **serving** a route.

**Route hint**:
What an action was PRICED against, carried on the action. Explicitly a hint and not a
constraint: it is used only while it still **holds**, and otherwise the route is
re-resolved under the same fee cap. The cap, never gateway identity, is the money
backstop.

A hint **holds** when it is still on the relevant **vetted list** and still validates. That is
deliberately WEAKER than **serving** — by exactly one term, the affordability sizing — which is
why it needs its own word: a hint that holds may still turn out unaffordable, and is then
re-resolved. Do not substitute "serves" here; the canonical predicate includes affordability and
using it would silently demand a sizing pass the hint path does not run.
As with **Serves**, the membership half is the INTENT, not today's behaviour — the current check
validates endpoints without re-testing the source federation's list — and it becomes true when
the evacuation-hop work closes that gap. Until then, do not cite this entry as though the
membership half holds.
Sizing decides the amount; the hint check decides whether to keep the route it was priced
against. Reading "still holds" as "re-run the affordability search" would price every candidate
twice — but dropping the membership half is worse: a hint priced before the source federation
revoked that gateway would keep routing money outside the vetted list, which is exactly what
ADR-0030 forbids.

A hint names a whole route, not a gateway — so its shape follows the **route kind**.
For a **shared route** that is one gateway, judged against the two-federation
predicate. For a **hop** it is two gateway identities plus the kind, each leg judged
against its own end. A hint is only usable if the route it names still **holds** in the same
shape it was priced in: a shared hint whose gateway is now vetted-and-valid for one end only has
not become a hop hint — it has stopped holding.

**Once an operation has committed, the route should stop being a hint** — a recorded route
replayed as persisted, without re-resolution, since otherwise a restart can pay through a
different gateway than the one the invoice was sized and recorded for. As with **Serves**,
this is the INTENT and not today's behaviour: after cache loss the op artifact carries no
gateway, so reassembly falls back to resolving afresh. The evacuation-hop work is what makes
it true, by persisting the route with the operation. Until then, do not cite this entry as
though the invariant holds.
_Avoid_: calling this a "pin" — it is the opposite, and conflating the two is what
makes route-selection rules contradict each other.

**Shared route** / **Hop**:
A **shared route** is one gateway serving both ends (an internal swap). A **hop** is
two different gateways, one serving each end, bridged over Lightning. The distinction
is economic, not one of trust: both legs stay hash-locked either way, and the hop
simply costs more because the internal-swap discount does not apply across two
gateways.
_Avoid_: "direct" for the shared route — it invites the idea that the hop is
indirect and therefore less safe, which is not the difference.

**Lightning Address**:
A human-readable receive handle (`user@domain`) that resolves via LNURL-pay to
fresh invoices. On Fedimint it is provided by **recurringd**, not a
wallet-operated LNURL server. Reusable and linkable, so it is the "easy" (less
private) receive path; a fresh QR invoice is the "private" path (see "Private").
_Avoid_: treating a Lightning Address as a fully-private receive

**recurringd**:
A Fedimint service that provides LNURL-pay / Lightning Address support by issuing
fresh invoices for a static handle. The client picks the recurringd URL; a
federation may *suggest* one via the meta `recurringd_api` field (a single URL,
not enforced). **A wallet can run its own**: the daemon holds no funds and cannot
claim payments (receive keys derive from the user), so an arbitrary recurringd is
custody-safe. Prefer the **stateless v2** (`recurringdv2`, LNv2) — it joins no
federation and persists nothing — but it still sees receive metadata in transit
(handle → federation → amount → time). The device chooses among several
public/community recurringds; we may run one but only as **one of many**, never a
sticky default (see [ADR-0013](./docs/adr/0013-recurringd-one-of-many.md)).

**Standing instruction**:
The user's one-time, upfront, gating acknowledgement (before any funds are
received) authorizing the on-device software to auto-manage funds across
federations on a best-effort, no-guarantees basis. It is what makes the Allocator
the user's own on-device agent rather than a service that controls funds (see
[ADR-0014](./docs/adr/0014-on-device-agent-standing-instruction.md)).
_Avoid_: "terms of service" (this is a specific in-app consent gate, recorded)

**Incoming contract**:
The federation-held contract a gateway funds when someone pays your Lightning
invoice. The payer's payment **settles immediately** (the gateway gets the
preimage); your balance updates only when your client later comes online,
discovers the contract on the federation stream, derives the claim material, and
claims the ecash. A delayed app open does NOT forfeit an already-settled payment.
Residual risks are **delayed visibility** and **federation/gateway failure before
the claim**, not a refund-on-timeout. (In recurringdv2 LNURL receives the
contract `expiration` field encodes the gateway fee, not a real expiry.)
_Avoid_: implying funds "bounce back" if not claimed quickly

**Operation**:
The user-facing unit of wallet activity — a pay, receive, move, join, probe —
identified by its **operation key** and listed by `history`. Every API/CLI/app
surface speaks of operations; EXECUTABLE operations are driven internally by an
**Intent** — the money ones, and also `join` and `recover`
(`wallet-cli/src/main.rs:1394-1400`, `wallet-fedimint/src/executor.rs:1258`).
_Avoid_: "intent" in any user-facing surface, "transaction"

**Intent**:
The internal durable, executable record inside an executable **Operation**'s
lifecycle: an idempotency-keyed decision driven Pending → Executing → terminal,
crash-resumable via reconcile. NOT money-only — `Action::Join`
(`wallet-fedimint/src/runtime.rs:1025-1030`) and `Action::Recover` are `Intent`s
too, which is why ADR-0030's await
provenance rule has to distinguish "user-initiated" from "resolves a route"
rather than treating those as the same test. Never appears in API type names or user copy.
_Avoid_: exposing "intent" outside the engine

**Policy**:
The **Standing instruction**'s parameters — the user-decided targets, caps,
fees, and budgets the Allocator runs under. User data: stored in the wallet DB
(seeded with defaults, edited at runtime through the wallet's own surfaces),
never in a host config file.
_Avoid_: "settings"/"config" for these (reserve those for host/deployment
concerns like paths and ports, which do live in a config file)







