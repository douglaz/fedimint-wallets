# 01 — Domain model

The entities the code has, with their states. Vocabulary is `CONTEXT.md`'s; where the glossary
defines a term for behaviour that does not exist, this document uses the term for what exists
and `11-open-findings.md` `F6`/`F7` list the difference.

## Federation

**DOM-1** A **federation** is identified by its 32-byte `FederationId`. The wallet knows a
federation through up to three durable records: a **registry row** (`STO-14`: invite, database
prefix, joined-at seconds) meaning the wallet has joined it; a **client partition** in
`client.db` (`STO-3`) holding the fedimint client's state and ecash; and optionally a
**candidate row** (`DOM-12`) from discovery.

**DOM-2** A joined federation is in one of two runtime states: **open** (a live client handle
exists in `MultiClient`) or **registered-but-unopened** (a registry row exists and no handle
does: startup failed to open the partition, or a recovery has committed the registry row and not
yet installed the handle). The second state is the documented ambiguity (`STO-27`), and every
whole-world planning surface refuses to plan while any federation is in it (`ALC-46`).

**DOM-3** A federation is **eligible to fund** when the scorer's structural floor and probe gate
pass or the operator has pinned it as spending or standby, **and** — if the agent joined it — its
active-probe verdict is `Passed` (`ALC-15`, `ALC-37`). A pin overrides the scorer; it does not
override the probe gate.

**DOM-4** The **spending federation** and **standby federation** are either pinned in `Policy`
or auto-designated each tick from the eligible set by rank, then spendable balance, then id. An
agent-joined federation is never auto-designated as spending (`ALC-15`).

## Operation and intent

**DOM-5** An **operation** is the user-facing unit of activity, identified by its **operation
key** (a correlation key, `STO-6`) and listed by `history`. Every operation has exactly one
current ledger row (`STO-20`). Operation kinds: `join, recover, receive, pay, direct-inflow, move,
evacuation, refusal, probe, tick, discover, autojoin, approve` (`STO-15`).

**DOM-6** An **intent** is the internal durable record that drives an *executable* operation:
the money verbs plus `join` and `recover`. It carries an `Action`, an attempt counter, a status,
the actor, and an optional evacuation-refusal marker (`STO-9`). Intents never appear in API type
names (`CONTEXT.md` **Intent**).

**DOM-7** `Action` has eight variants; seven are executable:

| Variant | Who builds it | Note |
|---|---|---|
| `DirectInflow {to, amount, fee_cap}` | user | invoice grossed up so `to` nets `amount`: never over, possibly under by a bounded receive-fee step (`FMI-15`) |
| `Move {from, to, amount, fee_cap, gateway?}` | allocator (funding), user, probe legs | `gateway` is a route hint |
| `Evacuate {from, to, amount, fee_cap, gateway?, fee_cap_components?}` | allocator only | `fee_cap` is the planning cap; the components recompute it |
| `Pay {from, invoice, amount, fee_cap, payment_hash, gateway?}` | user | |
| `Receive {to, amount, fee_cap, nonce, gateway?}` | user | fees deducted from `amount` |
| `Join {federation, invite, membership_preexisting}` | user only — auto-join writes a nonce-keyed ledger row and calls the SDK directly, it builds no intent (`STO-6`, `FMI-8`, `ALC-4`) | |
| `Recover {federation, invite}` | user only | the allocator never emits it, by convention |
| `RefuseInflow {fed, reason, diagnostics}` | allocator | **not executable**; becomes a refusal row, never an intent |

**DOM-8** `IntentStatus` ∈ `Pending, Executing, Awaiting, Done, Failed`. `Pending`: journaled,
no driver owns it, re-drivable. `Executing`: claimed by a compare-and-swap. `Awaiting`: the
effect was issued and completion depends on an external event — a direct-inflow payer, or a raw
pay's or receive's settlement. `Done`: terminal success, **unscannable by status**. `Failed`:
terminal, indexed, retryable only by an explicit user action that mints a new attempt (`OPS-2`,
`OPS-10`).

**DOM-9** `Actor` is `User` or `Agent {occurrence}`. The actor is the audit discriminator
`ADR-0014` needs; it also decides which admission checks apply (`OPS-6`) and which intents hold
allocator goals (`DOM-17`).

**DOM-10** A **move record** is the derived cache of a two-leg operation: both legs' operation
ids, the invoice, the gateway, the enforced fee cap, the quoted fees, the preimage, and a
`MovePhase ∈ Created, Invoiced, Sending, Settled, Refunded, Failed, Stranded`. It is partially
rebuildable from the fedimint op-log (`STO-11`, which owns the exact field list; `STO-33` owns
the op-log metadata it is rebuilt from). **Stranded** means exactly: the send settled
with a preimage and the receive reached an op-terminal non-claim (`OPS-27`). It is not a gateway
failure and the preimage does not recover it (`DEF-20`).

## The ledger row

**DOM-11** An `OperationRecord` is one row per operation: `seq` (the ordering authority),
`correlation_key`, `kind`, `actor`, `reason`, `status ∈ Started, Awaiting, Succeeded, Failed`,
created and updated timestamps (display only), `fees {fee_cap?, receive_fee?,
send_fee_quoted?}`, `error?`, and `repaired` (a terminal conclusion reached by repair from
absence of evidence, supersedable exactly once by an authoritative write). `ReasonCode` ∈
`SpendingBelowTarget, StandbyBelowTarget, ShutdownNotice, Unhealthy, OverCap, NotProbed,
LowReputation, UneconomicRoute, UserInitiated, ActiveProbe, StandingInstruction` (`STO-15`,
`STO-16`).

## Candidates, probes, discovery

**DOM-12** A **candidate** is a federation discovery has seen: `id`, `invite`, `source ∈
Observer, Nostr (unimplemented), Manual`, `structural ∈ Passed | Rejected(reason)`, and `state ∈
Rejected, Discovered, AutoJoined, UserApproved`. `UserApproved` is the state `approve` confers —
and a user `join` only from a state that is not already `AutoJoined` (`OPS-42`) — and it cannot be demoted (`STO-26`). A joined federation with no
`UserApproved` row is **probe-gated** (`DOM-3`).

**DOM-13** A **probe record** per federation holds up to 256 `ProbeAttempt`s — `{at_ms: u64,
ok: bool, from: FederationId, amount_msat: u64, leg_fee_cap_msat: u64, error: String?}`, the
persisted field names verbatim — and at most one in-flight `ProbeSession` (`STO-26` owns both
shapes and the pruning rule). An **active-probe verdict** ∈ `Passed, NeverProbed, Insufficient,
Expired, Failed, FailedSinceLastPass` is computed, never stored, from the attempts, the source
federation and the policy's window (`ALC-25`).

**DOM-14** Two different things are called a probe. The **light probe** (`FedimintProbeRunner`)
runs every cycle on every open federation and spends nothing: structural facts, one threshold
read for liveness, gateway validation, balances, shutdown signals. The **active probe** spends
about 20 sats twice — mint on the candidate, redeem back — to prove redeemability (`ALC-27`).

## Policy, occurrence, watch state

**DOM-15** `Policy` is the standing instruction's twenty-eight parameters, stored as one row
(`STO-13`), validated in the actor, and never in a host config file. Balance knobs
(`per_fed_cap`, `spending_target`, `standby_target`, the two pins); three fee caps of three
shapes (`max_fee` absolute, for user verbs and probe legs; `max_fee_bps_of_move` proportional,
for funding moves; `evac_fee_base_msat` + `evac_fee_bps`, for evacuations); probe window and
budget; scheduler cadence; discovery caps; `auto_join`; `require_mainnet`.

**DOM-16** An **occurrence** is the allocation epoch stamped into every agent decision's key.
The daemon allocates one per cycle by a checked increment of `WatchState.occurrence`; the
standalone tick takes it from `--occurrence` and records it as a floor; probe legs derive theirs
from the session nonce — the first sixteen hex characters of the 32-hex-char nonce read as a
`u64` (`occurrence_from_nonce`, `STO-6`), so a probe occurrence is a 64-bit random head that
is reconstructible from the stored `ProbeSession` alone and is separated from the small
integers the two other sources hand out only probabilistically — a user-supplied `--occurrence`
is any `u64`, and nothing namespaces the two (`STO-6`; a defect, `F44`). The floor never decreases, is raised in the same transaction as any
agent ledger append, and is fail-closed once it reaches `u64::MAX`: the daemon runs that one
cycle and then fails every later one (`ALC-33`, `STO-12`, `STO-21`).

**DOM-17** An **allocator goal** is the identity a live intent holds against re-issue:
`FundInto(dest)` for an agent funding move, `Evacuate(source)` for an agent evacuation. Goals
exclude occurrence, amount, gateway and the other endpoint. User intents and probe legs hold no
goal. A held goal suppresses only the candidate decisions that conflict with it (`ALC-30`,
`ALC-31`).

## Route economics

**DOM-18** `RouteEconomics {resolved_gateway?, min_viable_amount, status ∈ Routable, Unroutable,
UneconomicAtAnySize}` per ordered federation pair, supplied to the snapshot by IO each tick. An
absent entry means **unpriced**, which is permissive (`ALC-11`). `Unroutable` means no candidate
validated at both ends; `UneconomicAtAnySize` means a validated route whose fees outrun the cap
at every amount; `Routable` carries the economic floor.

## Evacuation refusal evidence and supersession

**DOM-19** `EvacFeeCap {base_msat: Msat, bps: u16}` with `at(net) = base_msat + floor(net ×
bps / 10 000)`, computed in `u128` and **saturated** into `u64` (no `(base, bps)` pair can wrap
into a small cap). `EvacuationRefusalEvidence {cap_components: EvacFeeCap, requested_net: Msat,
source_spendable: Msat, low: EvacuationQuoteSample, high: EvacuationQuoteSample, diagnostic:
String, measured_at_ms: u64}` is two freshly quoted delivered-net samples (`low`, `high`, each
`EvacuationQuoteSample {delivered_net: Msat, total_fee: Msat, fee_cap: Msat}`) plus the cap
components admitted with, the requested net, the source's spendable balance, a diagnostic
string, and a unix-millisecond timestamp. It is **evidence** that a refusal looks structural,
never proof a route is unavailable (`ALC-24`). It is persisted on the intent (`STO-9`) and in
the supersession sidecar (`STO-25`).

**DOM-20** An **evacuation supersession** retires a marked pre-artifact agent evacuation as
`Failed` and admits a linked `Pending` child at a fresh occurrence and distinct key under a
qualifying cap increase, writing forward and reverse sidecars in one transaction (`OPS-30`,
`STO-25`). The parent's evidence and identity are preserved; a superseded parent can never be
retried.
