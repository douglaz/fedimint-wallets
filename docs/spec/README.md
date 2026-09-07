# fedimint-wallets — as-built specification

A description of the wallet **as it exists in this repository on 2026-09-07**, at `main`
`1e44487` plus the open pull request #40. Not a plan, not a roadmap, and not a record of what was
intended: where a plan document, an ADR, a code comment or the glossary says one thing and the
code does another, this set records what the code does and files the difference in
[`11-open-findings.md`](./11-open-findings.md).

It was written by reading the code, not the prose. Every requirement traces to a function the
repository contains; the extraction notes it was built from cite `file:line` for each claim, and
the documents keep the function names so a reader can go and look.

## How to read this

| Document | Contents |
|---|---|
| [`executive-summary.md`](./executive-summary.md) | **Start here.** What the wallet is, what is built, what is not, and how much to trust the rest |
| [`00-overview.md`](./00-overview.md) | The problem, the shape of the solution, the system context, decided non-goals |
| [`01-domain-model.md`](./01-domain-model.md) | Entities and their states: federation, intent, operation, ledger row, move record, policy, candidate, occurrence |
| [`02-fedimint-integration.md`](./02-fedimint-integration.md) | The SDK boundary: the pin, clients and partitions, gateways, the two Lightning legs, recovery, the signals a federation emits |
| [`03-operation-lifecycle.md`](./03-operation-lifecycle.md) | How an intent is admitted, executed, resumed and terminalized; the killpoints; supersession; reconcile |
| [`04-api-contract.md`](./04-api-contract.md) | Every HTTP route and field, the error envelope, the CLI verbs and exit codes |
| [`05-persistence.md`](./05-persistence.md) | The two stores, the thirteen key tags, every persisted row, the transaction model, the ledger's write discipline, the compatibility rules |
| [`06-allocator-and-automation.md`](./06-allocator-and-automation.md) | The pure decision core, route economics, scoring, probes, discovery, evacuation, the tick, the scheduler cycle, and the readiness signal |
| [`07-security-requirements.md`](./07-security-requirements.md) | The threat model and what is actually enforced — including what is not |
| [`08-hosts-and-deployment.md`](./08-hosts-and-deployment.md) | The daemon, the standalone mode, the browser sidecar as built, the build, CI, and the one long-running deployment |
| [`09-known-defects.md`](./09-known-defects.md) | Twenty-five defects this system shipped and fixed, written as prohibitions |
| [`10-conformance-checklist.md`](./10-conformance-checklist.md) | What has been demonstrated, by which gate, and what has not |
| [`11-open-findings.md`](./11-open-findings.md) | **Read before planning.** The gaps between documents and code, each tied to the issue that tracks it |
| [`../../CONTEXT.md`](../../CONTEXT.md) | The glossary. Several entries there describe intent rather than behaviour and say so; `F6` and `F7` list them |
| [`../adr/`](../adr/) | The thirty-one decisions and what was rejected to reach them. Canonical where they conflict with older prose; **not** canonical where they describe unbuilt behaviour (`ADR-0029` half, `ADR-0030`, `ADR-0031` items 2–3) |

Read `00`, `01` and `03` first. `03` is the part that distinguishes this wallet from a thin
fedimint client: a money operation is a durable, idempotency-keyed intent that survives a crash
at any of four named points without double-paying, and everything else is built around that.

## How much to trust this

Every requirement wears the same costume — a MUST, a stable identifier — and the confidence
behind them is not uniform.

- **Every requirement describes code that exists**, was read on 2026-09-07, and is exercised by
  the unit suite (1,071 tests). That is the floor.
- **Most money paths have also been exercised live** against a two-federation devimint harness
  by the smokes in `10-conformance-checklist.md`. That document says which, and which not.
- **Nothing after build `b5f46de` (2026-07-26) has run against a real federation.** The one
  long-running deployment is a test rig at that build; `main` is 220 commits past it. Everything
  the evacuation-cap, supersession, watch-suppression and persistence-fix work changed has only
  devimint evidence (`HST-24`).
- **The extraction notes flagged `[verify]` where a fact was inferred rather than read.** Those
  were either resolved by a second read or dropped; none survive as a requirement. `[gap]` items
  became findings.
- **This set has not itself been reviewed.** It is a first draft written in one sitting from six
  extraction passes. Treat a claim you are about to rely on as a pointer to the function it names,
  and read the function.

## Requirement conventions

Requirements use RFC 2119 keywords — **MUST**, **MUST NOT**, **SHOULD**, **MAY** — in the
descriptive sense: "MUST" means the code does this and a change that stops it doing so is a
regression against this document, not that someone decided it ought to. Each is tagged with a
stable identifier:

| Prefix | Domain |
|---|---|
| `OVR-n` | Overview and scope |
| `DOM-n` | Domain model |
| `FMI-n` | Fedimint SDK integration |
| `OPS-n` | Operation and intent lifecycle |
| `API-n` | HTTP API and CLI contract |
| `STO-n` | Persistence |
| `ALC-n` | Allocator, scoring, probes, discovery, tick and scheduler |
| `SEC-n` | Security |
| `HST-n` | Hosts, frontends and deployment |
| `DEF-n` | Defect prohibitions |
| `CNF-n` | Conformance items |
| `Fn` | Open findings (not gated; tracked one-to-one with `br-…` issues) |

### Identifiers are append-only. Text is not.

An identifier is never reused and never renumbered. Deleting a requirement is permitted — the
gap in the sequence is the tombstone — but a withdrawn id must be listed in the index below so an
old citation still resolves. `tools/check_ids.py` enforces this: duplicate ids, dangling
citations, sequence gaps not listed as withdrawn, and references to ADRs that do not exist all
fail the gate.

### A decision gets its identifier when it is accepted

When a change to the system is accepted, name the identifier that will carry it — the requirement
it amends, or the next free number in the right namespace. Then "did we apply everything?" is a
`grep`, not a memory exercise.

### One owner per rule

A rule lives in one requirement; everywhere else cites it. `DEF-21` is what a second normative
copy costs: seven documents kept saying an evacuation sizes off the flat cap after the code
stopped doing so, and the one an operator reads under pressure was among them.

### Withdrawn identifiers

Deleted from the documents. Never reused. Listed so an older citation still resolves.

| Identifier | Was | Why it went |
|---|---|---|

*(none yet)*

## Gates

```
bash docs/spec/tools/check-all.sh
```

runs the identifier gate. It exits non-zero on any failure and captures each gate's own exit
code rather than the last command's in a pipe. Run it before and after editing this set.

## Relationship to the rest of `docs/`

This set supersedes, as the description of the system: the "Current status" section of
`README.md`, the "Where we are" section of `docs/roadmap-to-v1.md`, `docs/phase6a-plan.md` and
`docs/phase4-implementation-spec.md` as field-level authority, and the shape sketches in
`docs/operation-history-spec.md` §2. Those documents remain useful as the record of *why* the
code is the way it is; they are not the record of *what* it is. Retiring or archiving them is a
separate decision this set does not make.

The runbooks (`docs/real-sats-pilot-runbook.md`, `docs/devimint-runbook.md`) are operator
procedures and remain authoritative for procedure. `CONTEXT.md` remains the glossary. The ADRs
remain the decisions.
