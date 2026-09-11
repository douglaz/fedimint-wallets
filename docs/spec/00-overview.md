# 00 — Overview

## Problem

Fedimint federations issue blind-signed ecash backed by a guardian multisig, which gives a
spending wallet the privacy a custodial Lightning wallet cannot: no KYC, a provider blind to
balance and history, fully private receives. They are also **ephemeral** in practice — a
federation can degrade, lose its gateway, or announce it is shutting down — so a wallet that
holds its whole balance in one federation is one outage away from a user who cannot pay.

The system described here spreads a small spending balance across federations and moves it
automatically on the user's standing instruction, so that one federation degrading never leaves
the user with nothing to spend. It does this as an **on-device agent** with no operator, no
curated list and no service in the fund path (`ADR-0014`, `ADR-0015`), and it records every
action it takes and every operation it refuses durably enough that the user can reconstruct what
happened, why, and what it cost. Two declines are not recorded: a funding shortfall deferred
below the route floor, and a duplicate-key drop (`ALC-5`, `F1`).

## What is built

**OVR-1** The repository contains a headless engine (`wallet-core`, pure logic;
`wallet-fedimint`, the SDK integration, journal, executor, runtime, scheduler), a 24/7 daemon
(`wallet-daemon`, binary `walletd`) that hosts it behind a loopback HTTP API, the wire types
(`wallet-api`), a CLI (`wallet-cli`) that is a client of that API by default, and the skeleton
of a browser sidecar (`wallet-web`) that serves nothing yet (`HST-26`).

**OVR-2** Every money operation is a durable, idempotency-keyed intent, journaled before any
network call, driven by a task that may die at any point, and resumed by reconcile without
double-paying (`03-operation-lifecycle.md`). This is the property the rest of the design
protects.

**OVR-3** No money operation's network IO blocks another's start. The actor that owns admission
does millisecond bookkeeping only; drivers wait (`ADR-0024`, `OPS-13`).

**OVR-4** Every operation, failure and refusal is an append-only ledger row. An intent-backed
row is written in the same transaction as the intent transition it describes (`STO-16`); the rows
that describe no intent — tick, refusal, discovery, auto-join — are written best-effort
(`approve` is the exception: its row shares the candidate promotion's transaction, `STO-26`)
and a failed write is logged, not fatal (`ALC-34`). History without failures is not history.

**OVR-5** The allocator funds only what a probe has proven. A discovered federation is fundable
after a sustained window of real sats-spending round trips passes, never on discovery alone
(`ALC-37`, `ADR-0017`).

**OVR-6** The allocator is pure: `decide(snapshot, occurrence, blockers)` reads no IO and is
tested against golden fixtures. Everything it needs — balances, probes, reservations, route
prices — is gathered by the tick and placed in the snapshot (`ALC-1`).

**OVR-7** An **evacuation's** enforced fee cap is recomputed from what the destination is
actually credited, never from the amount asked for (`ALC-21`); a cap computed on an amount nobody
received bounds nothing. A funding `Move` keeps the proportional cap the allocator stamped on
the planned amount even when delivery settles a hair under — the executor updates the amount and
not the cap, which errs conservatively by a few msat (`OPS-22`).

**OVR-8** The user holds the standing instruction's parameters as one stored `Policy`, edited at
runtime through the wallet's own surfaces and never through a host config file (`STO-13`,
`API-20`).

## System context

```
   wallet-cli ──HTTP/bearer──►  walletd  (one process, one lock, two RocksDB stores)
   wallet-web (skeleton)        │
   ops/walletd-watch.py         │  axum handlers ──► actor (admission, journal transitions,
                                │                          tokens, leases; ms-scale only)
                                │        │                    │
                                │        │  spawn         one-shot commands
                                │        ▼                    ▼
                                │   driver tasks ────► FedimintJournal ◄─── scheduler loop
                                │   (perform, await)     journal.db          (reconcile, open,
                                │        │               ┌─ intents          probes, discovery,
                                │        │               ├─ move records     tick plan+commit,
                                │        ▼               ├─ ledger           deadlines)
                                │   MultiClient ◄────────┤─ registry
                                │   client.db            ├─ candidates, probes
                                │   ┌─ seed              ├─ watch state, policy
                                │   ├─ fed A client      └─ supersession sidecars
                                │   ├─ fed B client
                                │   └─ …
                                ▼
            federations (guardian APIs) ◄──► lnv2 gateways ◄──► Lightning
            Fedimint Observer (discovery only, untrusted)
```

**OVR-9** One process owns the wallet. The daemon is the only resident host; the CLI's
`--standalone` mode is a one-shot process that takes the same lock and drives the same engine,
and is the documented exception to "admission goes through the actor" (`ADR-0031`, `HST-9`).

**OVR-10** The daemon is a **host**, not the engine (`CONTEXT.md` **Host**/**Engine**). It owns
cadence, restart and config; the engine owns every decision. A future Android host would embed
the same engine and drive it from platform wakes; that seam is not yet built (`F17`).

## Non-goals, decided

**OVR-11** No on-chain evacuation (`ADR-0004`, `ADR-0018`), no Cashu, no iOS, no multi-device,
and no LNURL/Lightning address in this version. The last is a deviation from `ADR-0004`, which
placed Lightning Address and LNURL-pay **in** v1; the built scope defers them together with their
provider, recurringd (`ADR-0013`), and the only record of that deferral is the roadmap's
"Explicitly v2+" line (`docs/roadmap-to-v1.md`) — no ADR amends `ADR-0004`.

**OVR-12** No Tor. Reliability over network anonymity; "private" means no KYC, a blind provider,
and private receives, not network-level anonymity (`ADR-0002`, `CONTEXT.md` **Private**).

**OVR-13** A two-gateway Lightning route is a non-goal for ordinary movement. Every move resolves
one gateway that validates at both federations. Evacuation is the decided exception — a hop over
two gateways on different Lightning nodes, each leg chosen from its own federation's vetted list
— and that exception is unbuilt (`ADR-0029`, `F3`).

**OVR-14** No compatibility shims, with one deliberate class of exceptions: types written to a
live store gain fields only with `serde(default)` and never carry `deny_unknown_fields`
(`STO-29`–`STO-31`).

## What this document does not decide

Whether the engine ships on by default, and whether the long-running deployment is a pilot or a
test, are open at the product level (`11-open-findings.md`, "The questions still open").
