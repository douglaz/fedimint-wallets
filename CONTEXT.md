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
_Avoid_: making "seed phrase backup" the default flow (it is an opt-in export)

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
Moving a user's balance out of a failing or closing federation into a healthy one
(or out via on-chain peg-out). Triggered primarily by a **Shutdown notice**,
secondarily by probes detecting degradation. The Allocator's core resilience
action.
_Avoid_: "sweep" (reserve for consolidating many inputs), "withdraw"

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
surface speaks of operations; money operations are driven internally by an
**Intent**.
_Avoid_: "intent" in any user-facing surface, "transaction"

**Intent**:
The internal durable, executable record inside a money **Operation**'s
lifecycle: an idempotency-keyed decision driven Pending → Executing → terminal,
crash-resumable via reconcile. Never appears in API type names or user copy.
_Avoid_: exposing "intent" outside the engine

**Policy**:
The **Standing instruction**'s parameters — the user-decided targets, caps,
fees, and budgets the Allocator runs under. User data: stored in the wallet DB
(seeded with defaults, edited at runtime through the wallet's own surfaces),
never in a host config file.
_Avoid_: "settings"/"config" for these (reserve those for host/deployment
concerns like paths and ports, which do live in a config file)







