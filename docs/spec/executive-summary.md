# fedimint-wallets — Executive summary

*A single-file orientation to the as-built specification, for a reader who has never seen the
code. Written 2026-09-07 against `main` `1e44487` plus PR #40, both now merged as `ee4ba1c`, and
re-read in full against `7225114` (PR #49, #50) on 2026-09-10: six Rust crates, about 80,000
lines of which roughly 40% is test code, 1,071 tests (measured at `ab52094`, `CNF-5`), 17 live smoke gates, 31 ADRs.*

---

## 1. What this is

A **spending** wallet for Fedimint ecash, built for someone who would otherwise use a custodial
Lightning wallet and wants to not be custodied. It holds small balances across two or more
federations, pays and receives over Lightning through the federations' gateways, and — this is
the part that is novel — runs an on-device **allocator** that moves the balance between
federations on the user's standing instruction: topping up the one used for spending, keeping a
warm standby in another, and evacuating a federation that announces it is shutting down. A
federation is treated as **ephemeral**: it may degrade or vanish, and the wallet's job is to make
sure that never leaves the user unable to spend (`ADR-0001`, `ADR-0006`).

What exists today is the **headless engine**, a **24/7 daemon** (`walletd`) that hosts it behind
a loopback HTTP API, and a **CLI** that is a thin client of that API. There is no phone app. A
browser sidecar has a configuration and a login-password hasher and serves no pages. Everything
user-facing beyond the terminal is future work (`08-hosts-and-deployment.md`).

## 2. The four ideas everything else follows from

**A money operation is a durable intent, not a function call.** Every pay, receive, move, join
and recovery is written to a journal under an idempotency key *before* any network call, then
driven by a task that can be killed at any point and resumed by a reconcile pass. The
cross-federation move — two Lightning legs through one gateway — is proven to survive a crash at
four named points without paying twice or minting a second payable invoice, because the fedimint
client's deterministic operation ids and the journal's compare-and-swap writers together make the
second attempt attach to the first (`03-operation-lifecycle.md`, `CNF-12`).

**Nothing blocks anything else.** A Lightning payment can be held for hours. So no operation's
network IO ever runs on the actor that owns admission; the actor does millisecond bookkeeping and
per-operation driver tasks do the waiting. A pay issued while a probe is stuck reaches its first
external call in under 250 ms, measured (`ADR-0024`, `CNF-21`).

**The ledger is the user's record, and it is append-only.** Every operation — including every
failure and every refusal — is one row in a sequence-numbered ledger; an intent-backed row is
written in the same transaction as the intent transition it describes, while tick, refusal and
discovery rows are best-effort and a few refusal paths carry empty diagnostics (`OVR-4`,
`ALC-5`). Nothing deletes a row. A
terminal row is immutable. A retry is a new row. `wallet-cli history` reconstructs a whole
session (`05-persistence.md`, `CNF-15`).

**The allocator only ever spends what a probe has proven.** A federation the wallet discovered
on its own is fundable only after a sustained window of real, sats-spending round-trip probes
has passed; reputation feeds and metadata can demote a candidate but never promote one. A
federation the user joined by hand is trusted as the user's own decision (`ADR-0017`,
`06-allocator-and-automation.md`).

## 3. What is built, in one paragraph each

**Money.** Join a federation; receive (fee deducted from the invoice) and direct-inflow (invoice
grossed up so the destination is credited the requested amount, never over and at most one
receive-fee step under); pay a BOLT11 invoice
choosing the cheapest gateway that fits a fee cap; move between federations as an internal swap
through a gateway both ends validate; recover ecash from the twelve-word seed plus the
federation's invite, into a fresh partition, never wiping anything (`ADR-0025`).

**Decision.** Once per cycle the daemon probes every open federation (no sats spent), scores them
structurally (guardian count, threshold, modules, mainnet), builds a snapshot with balances and
in-flight reservations, prices the two funding routes (a per-pair economic floor below which a
move costs more than it is worth), and asks a pure function what to do. It emits at most a
top-up move, a standby move, and an evacuation per dying federation, each with a fee cap:
proportional for funding moves, base-plus-proportional for evacuations, computed on what the
destination is actually credited (`ADR-0029`).

**Safety.** Per-federation balance cap enforced at plan time and again before minting. A route
that is uneconomic at any size blocks funding and says so in the ledger. One federation's stuck
operation no longer suppresses decisions for the others. An evacuation that cannot fit its cap at
any amount is marked with durable evidence, and a qualifying cap increase atomically retires it
and admits a linked successor. An unopened federation fences all planning and reports why on
`/v1/health`; so does a corrupt registry row (PR #40, merged 2026-09-08).

**Host.** The daemon owns both RocksDB stores under one lock, serves eighteen routes behind a
bearer token, runs the scheduler with adaptive sleep, wakes early for a federation's announced
expiry, restarts itself on a settlement stall, and refuses to start on an invalid stored policy.
The CLI's `--standalone` mode drives the same engine one-shot under the same lock.

## 4. What is not built, and matters

- **The seed is plaintext on disk.** Anyone who reads the data directory owns the funds. Decided
  and deferred in `ADR-0026`; not started (`F11`).
- **The vetted-list membership check** that would make "one gateway validates at both ends" a
  real invariant is not enforced, and the list is a union of guardian answers (`F6`).
- **A dying federation with no gateway shared with any other cannot be evacuated.** The second
  route `ADR-0029` decided — a real Lightning hop through two gateways — is unbuilt (`F3`).
- **A funding shortfall below the route floor is withheld with no operator signal.** Correct, and
  silent for the whole life of the test deployment (`F1`).
- **Nothing pages.** The readiness signal exists on the wire and a poller exists in `ops/`; no
  cron job runs it (`F14`).
- **No frontend but the CLI.** The web sidecar is a skeleton; Android does not exist (`F27`, `F28`).

## 5. What has been validated, and against what

The unit suite is the floor and was green at `ab52094` (`CNF-5`). Seventeen live smokes against a two-federation devimint
harness cover the money path, the crash gate, the tick, evacuation, discovery, the probe,
history, recovery, the daemon path, the autonomous chain, responsiveness, a 24-hour soak, and
supersession. Only the supersession smoke records its last green run in its header; the others
carry launch blocks, and their last runs live in issue close notes. **None runs in CI**, by explicit
policy.

One daemon has been running the 2026-07-26 build on two mainnet federations with a small real
balance. It is a **test deployment**, not a pilot, and it runs a build 240 commits behind `main` (`HST-24`).
It has demonstrated restart survival, a cross-federation move, and an external Lightning send and
receive with fees reconciling exactly. It has never evacuated, never stranded a move, and has sat
for the whole period on one silent withheld shortfall.

## 6. How this set came to exist, and what to do with it

The repository accumulated its description across a README status list, a roadmap progress log,
two "buildable" phase specs for finished work, a glossary that defines several terms for
behaviour that does not exist, and a 1,700-line work journal. Its issue tracker held 35 open
items with no hierarchy, a third of them review findings on fixes, and five P1s inherited from
treating the test deployment as production. This set was written to replace the description half
of that: one place that says what is built, with a gate on its identifiers and a findings file
tied one-to-one to the issues.

It does not replace the ADRs (they are the decisions), the runbooks (they are the procedures), or
the glossary (it is the vocabulary, though `F6`/`F7` say which entries need trimming). It was
re-read against the code and panel-reviewed on 2026-09-10 (`README.md`, "How much to trust
this"); that reviewed the text, not the code. Read the function before you rely on the sentence.
