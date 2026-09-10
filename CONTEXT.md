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

**Structural evacuation refusal evidence**:
Fresh typed sizing samples durably attached to a retryable, **pre-artifact** agent evacuation.
They are evidence that the refusal appears structural, not proof that a route is unavailable:
an empty bounded probe remains `Retryable`. The evidence identifies the delivered-net sample
against which an evacuation-cap edit is evaluated.

**Evacuation supersession**:
The narrow atomic replacement of that marked evacuation after a component-wise monotone
effective cap increase at its measured sample. It retires the old operation as `Failed`,
creates a linked `Pending` child at a **fresh occurrence** and distinct key, and writes durable
forward/reverse audit links (`superseded_by` / `supersedes`). It is actor-owned; standalone
requires an explicitly advanced `--occurrence`. This preserves the old evidence and audit
identity, does not turn evidence into route-unavailability proof, and is not a general retry
or policy-edit escape hatch.

**Sized ask**:
The amount the sizing search committed to for a move — the largest candidate that fit the
cap and the source's spendable balance. It is an INTENTION: the amount the executor will
re-quote and request an invoice for. It is not what arrives.

**Delivered net**:
What the destination is actually credited: the fixed invoice amount minus the receive-side
fee quoted against it. Always ≤ the **sized ask**, and strictly less whenever the gross-up
fixed point settles a verified "hair under". It is a FACT about a specific quote, not a
plan.
**Every ENFORCED fee cap is computed from the delivered net**, never from the sized ask — a cap
computed on an amount nobody received bounds nothing. "Enforced" is load-bearing: the allocator
deliberately stamps a PLANNING cap at the planned amount when it decides an evacuation, because
sizing has not run yet and there is no delivered net to compute from. That planned cap is
superseded by the recomputed one as soon as sizing runs. Do not "correct" the planning half to the
delivered net — it cannot be, and the replay design depends on the two being distinct. The two must be derived identically
everywhere they are compared, or a move can be admitted under one and refused under the
other after its receive leg has already committed.
_Avoid_: "executed net" — it reads as the **sized ask** to one reader and the **delivered
net** to another, and that ambiguity is exactly how the same defect reached five separate
call sites. Say which one you mean.

**Serves** (of a gateway, with respect to a route or a leg):
A gateway **serves** when it is on the relevant **vetted list**, validates, an economically
viable amount can be sized over it (total fee never exceeding what it delivers), and it
**performs** — completes every leg it quoted, both legs on a **shared route**. A **shared route** is served only by a
gateway on **both** federations' lists; each **hop** leg is served by a gateway on the list of the
one federation at its end. Registry presence is not the test, and neither is a single miss:
being unable to fund the full ask is an instruction to move less, and one over-cap quote is not
"no amount fits". An empty bounded sizing result does not refuse the **Evacuation**: it means
that route does not serve THIS attempt, so the next route class is tried, and the next fresh
attempt starts over. A gateway that quoted and then did not perform is set aside for a while
rather than chosen again at once.
Target per [ADR-0029](docs/adr/0029-evacuation-must-be-executable.md); the source-list and
performs clauses are not built yet (open finding F6).
_Avoid_: "supports", "is available for" — both get read as registry presence.

**Break-glass gateway override**:
An operator's explicit, single-invocation instruction to route through a named gateway
**outside the federation's vetted list**. It exists for one incident: a federation whose
vetted gateways are dead, empty, or unreachable from this host, where consensus still
redeems the ecash but no route to it can be selected — and for an operator deliberately
exercising one named gateway while debugging a route. Reaching for it is an incident or
debugging action, not configuration — it applies to the ONE **Operation** the invocation
names (the one it creates, or the one it awaits by **operation key**) and to nothing else.
Naming the operation is the authorization: an operator may name an Allocator-created
operation (a stuck **Evacuation**), and nothing that does not name it can observe it.
_Avoid_: "gateway preference" and "the operator's chosen gateway" — both frame it as a
policy about which counterparty to trust, when the actual question is whether ANY route
exists. Also avoid "pin": automated routing is never pinned, and calling this a pin is
what let a break-glass be mistaken for a routing policy.

**Money verb**:
A `wallet-cli` subcommand that moves value on behalf of the human running it, as opposed to one
that observes state or drives the automated lanes. Exactly four: `pay`, `receive`, `move`,
`direct-inflow`. These are the only verbs that INITIATE movement, and the
**break-glass gateway override** is accepted on them. The await verbs (`await-receive`, `await-send`, `await-move`) also accept it — what it then
applies to is a dispatch rule owned by [ADR-0030](docs/adr/0030-automated-routing-is-never-pinned.md),
not by this glossary. `direct-inflow` is the one an implementer is most likely to misclassify — it reads like plumbing, but it funds a federation and most devimint
smokes fund through it, so classifying it as rejected or ignored breaks the funding step.

**Vetted list**:
The gateways a federation has admitted for lnv2: those that a consensus threshold of its
guardians (the same threshold every consensus answer needs, which is three of four guardians
and four of five) each name, not every gateway any one guardian names. It is the only input to automated route selection; an operator's **break-glass
gateway override** deliberately steps outside it, and nothing automated ever does. Each guardian
keeps its own list and admits a gateway by its own admin action, so getting a gateway vetted
means registering it on enough guardians, which is why the break-glass exists.
Per [ADR-0030](docs/adr/0030-automated-routing-is-never-pinned.md); the threshold rule is the
target (F6, `br-routing-invariants-f6f7-dwx`) — today's read is a union of guardian answers.
_Avoid_: "registered gateways" when you mean routable ones — presence in the list is not
**serving** a route.

**Route hint**:
The route an action was priced against, carried on the action as a hint, never a constraint:
kept only while it **holds**, otherwise re-resolved under the same fee cap before the route is
used. A hint names a whole route in the shape of its kind — one gateway for a **shared route**,
two gateway identities for a **hop** — and stops holding if it no longer holds in that shape.
For an `Evacuate` a holding hint is only a starting point within its route class. The cap, never
gateway identity, is the money backstop.
A hint **holds** when it is still on the relevant **vetted list**, still validates, and has no
recorded failure to **perform** — deliberately one clause weaker than **serves**, the
affordability sizing, so a holding hint can still prove unaffordable and be re-resolved, but a
gateway that quoted and then did not perform never keeps its hint.
Target per [ADR-0029](docs/adr/0029-evacuation-must-be-executable.md); destination-list
membership is checked, the source-list and perform-record halves of "holds" are not yet (F6,
`br-s0e`).
_Avoid_: "pin" — a hint is the opposite of one; "serves" for "holds" — it would silently demand
a sizing pass the hint path does not run.

**Committed route**:
The route recorded with an **Operation** once any leg has committed; from then on it is
replayed as recorded and never re-resolved, so a restart cannot pay through a different gateway
than the one the invoice was sized for. What commits is the route actually RESOLVED for the
operation — equal to the **route hint** only when the hint was retained, never a hint that was
re-resolved; a **break-glass gateway override** chooses a route but never travels on the intent,
so a committed break-glass route replays without the flag.
Target per [ADR-0030](docs/adr/0030-automated-routing-is-never-pinned.md); after cache loss the
operation artifact carries no gateway yet (F7, `br-s0e`).
_Avoid_: "pin"; "persisted route" (ADR-0030's earlier wording for the same thing).

**Shared route** / **Hop**:
A **shared route** is one gateway serving both ends (an internal swap). A **hop** is
two gateways on two DIFFERENT Lightning nodes, one serving each end, bridged over
Lightning. "Different" is about the node behind the gateway, never the URL: two URLs on one
node are one gateway, and treating them as a hop makes the sender look for a swap it cannot
find. The distinction is economic, not one of trust: both legs stay hash-locked either way,
and the hop simply costs more because the internal-swap discount does not apply across two
nodes. Each leg's gateway comes from that end's **vetted list** and each end's fee schedule
is read on its own, but the price of a hop is composed: the destination's receive fee sets the
invoice, and the source's send fee is charged on that invoice, so the two are ranked together,
cheapest composed cost among the different-node combinations first, and the next one is tried
when the first, after ordinary downsizing, still sizes to nothing viable (being unable to fund
the full ask is an instruction to move less, never a reason to move on). Whether the two nodes can actually reach each other over
Lightning is learned only by paying; a failed hop payment sets aside that one node-to-node
route for a while, not the gateways themselves, while a gateway that fails to **perform** at
its own end is set aside as a gateway.
_Avoid_: "direct" for the shared route — it invites the idea that the hop is
indirect and therefore less safe, which is not the difference; "pair scan" for a hop — the
legs are priced separately and only the node rule couples them.

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
(`Action::is_executable` applied to `Intent::action`).
_Avoid_: "intent" in any user-facing surface, "transaction"

**Intent**:
The internal durable, executable record inside an executable **Operation**'s
lifecycle: an idempotency-keyed, decision-driven record that may be `Pending`,
`Executing`, or subscription/external-payment-owned `Awaiting` until terminal,
and is crash-resumable via reconcile. Reconcile does not re-perform `Awaiting`
work. NOT money-only — `Action::Join`
and `Action::Recover` pass `Action::is_executable` through `Intent::action`; they are `Intent`s
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

**Engine**:
The wallet's resident decision-and-admission core: the engine-hosted service actor owns
intent admission, runs the Allocator, and admits every host-driven **Intent** — plus the
executor machinery it drives. In an engine-hosted service, every reservation-changing production
raw artifact and `MoveRecord` write is a one-shot actor command which bumps the affected balance
generations before later allocator authority. The narrow direct-write exceptions admit no fresh
resident intent: the DB-only composite raw terminal write runs under an actor terminal lease, and
the O(ledger) off-actor repair scan routes its reservation-releasing intent sink back through the
actor/CAS fence. The isolated standalone runtime may write directly only while it holds the
wallet's exclusive DB lock. Every resident **Host** embeds the same engine (ADR-0031). The isolated
`wallet-cli --standalone tick` compatibility command is the documented
admission exception: under the wallet's exclusive DB lock it plans and applies one phase-aware
allocator batch through `Runtime`, with its own final conflict re-scan. It is not a second
resident engine or the model for a future host. Admitting agent work anywhere
else is reaching around the engine.
_Avoid_: "backend"; "daemon" (walletd is a **Host** of the engine, not the
engine)

**Host**:
The process that embeds, drives, and supervises the **Engine**: `walletd` on a
server, the Android app on a phone. The host owns scheduling cadence (a
resident loop, or platform wakes), restart supervision, and deployment config —
it decides *when* the engine runs, never *what* the engine decides.
_Avoid_: conflating with **Frontend** (walletd is a host that also transports
two frontends)

**Frontend**:
A user surface over the engine's operation API — `wallet-cli`, the web UI, the
Android UI. A resident frontend talks to the **Engine** (in-process, or through
a **Host** like walletd) and never schedules, supervises, or admits work itself.
The isolated `wallet-cli --standalone tick` compatibility mode is the documented
exception: its one-shot process invokes `Runtime` directly under the exclusive
DB lock; it is not the architecture for a resident frontend.
_Avoid_: "client" (collides with the fedimint client), "app" for non-Android
surfaces
