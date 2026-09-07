# 09 — Known defects

Every entry is a defect found in this system, written as a prohibition so the fix cannot be
undone by a later change that looks like cleanup. Most shipped and were fixed; `DEF-12` was
deleted before it merged, `DEF-23` and `DEF-24` were caught in review of the change that carried
them, and `DEF-25` is still open (`F22`). Each entry says which. The provenance for each is
the issue that recorded it (`br-…`, the repository's `.beads/issues.jsonl`) and the pull request
that closed it. Where a defect was only ever observed in review rather than in a running wallet,
the entry says so.

Identifiers are stable and cited from the other documents. They are grouped by theme and not
renumbered.

## Money arithmetic

### DEF-1 — A funding move could pass every check while paying forty times what it moved — *verified in production behaviour, 2026-07-21*

`min_move` was 5 sats and the only fee bound was the absolute `max_fee` of 200 sats, so a 5-sat
move with a 200-sat fee satisfied both. Sizing also reserved the *whole* absolute cap from the
source before computing what was available, so a cap that was large relative to the surplus drove
`available` to zero and refused the move outright.

**Prohibition** — a funding move's fee cap MUST be proportional to the amount moved
(`ALC-7`), and sizing MUST reserve `amount + cap(amount)`, never a flat cap, from the source
(`ALC-8`). The flat `max_fee` bounds no allocator-emitted move.

*Provenance: `br-ljj`, `br-ljj.2`; PR #3.*

### DEF-2 — A refusal recorded only the federation and a reason code, so a real refusal could not be explained afterwards — *observed 2026-07-21*

A refusal at balances where hand arithmetic said a move should have been emitted could not be
reproduced from the row. Nothing on disk held the figures the allocator decided on, so every
downstream fee-policy decision was being designed on top of an unreproduced failure.

**Prohibition** — every refusal row MUST carry the figures that produced it (`STO-15`). A
subsequent field added to that record MUST decode from a row written before it existed
(`DEF-13`).

*Provenance: `br-ljj.1`, `br-ljj.4`, `br-nsx`.*

### DEF-3 — The evacuation was bounded by the flat `max_fee`, which at real gateway prices could not fund a drain — *derived from measured fees, 2026-08-01*

At measured swap costs (~1.8%) a full-federation evacuation was over the flat cap by ~27×. Two
outcomes, both real: when the gateway's two base fees alone fit the cap the balance drained in
~27 chunks across as many ticks; when they did not, no amount fit and the executor returned
`Retryable` forever, a silent livelock beneath a comment saying route economics never gates an
evacuation.

**Prohibition** — the evacuation cap is `base + floor(delivered_net × bps / 10 000)`
(`ALC-20`), computed from the **delivered net**, never the sized ask (`ALC-21`), persisted with
the receive so a replay cannot resurrect the planned cap (`OPS-25`), and reached by a sizing
search that does not assume monotonicity (`ALC-22`). Route economics MUST NOT gate an
evacuation (`ALC-18`).

*Provenance: `br-y2j`, `br-evac-cap-policy-r3n`, `br-evac-cap-enforce-vn6`; PRs #30–#34;
`ADR-0029`.*

### DEF-4 — The ledger reported the planned cap and planned amount for the life of the row — *verified by reading the write path, 2026-08-07*

`FeeBreakdown.fee_cap` was stamped once at plan time and the refresh copied op-ids, gateway and
quoted fees but not the cap or the executed amount. An evacuation planned at 75,000 sats and
clamped to 1,000 recorded a 2,450-sat cap it never enforced, so a post-incident fee audit
validated fees the enforced cap would have refused.

**Prohibition** — the ledger row's cap and amount MUST be the enforced cap and executed amount,
refreshed together (`STO-17`). Refreshing one without the other makes the row internally false.

*Provenance: `br-evac-cap-ledger-x9k`; PR #34.*

### DEF-5 — Gateway selection took the first gateway that validated, not the cheapest — *review, 2026-07*

Manual pay/receive and the move path selected the first validating candidate. A dearer route
was taken whenever it happened to be listed first.

**Prohibition** — automated selection MUST choose the cheapest validated candidate (`FMI-12`).

*Provenance: `br-pay-receive-cheapest-gw-l3g`, `br-route-economics-impl-hc3`; PR #14.*

## Liveness and suppression

### DEF-6 — One retryable intent suppressed ticks for every federation — *verified in both execution paths, 2026-08-09*

The standalone watch skipped the tick whenever reconcile reported *any* retryable pending work,
and the daemon scheduler permitted pricing and commits only when the global re-driven count was
zero. Either way one federation's stuck move halted the allocator wallet-wide, including the tick
that would decide an evacuation for a *different* dying federation. The caution itself was sound:
running a tick against unknown pending-move state risks re-issuing the same work under a fresh
occurrence. The defect was the blast radius.

**Prohibition** — suppression MUST be scoped to the allocator goals that conflict with live work
(`ALC-30`). A stuck intent on federation A MUST NOT suppress an independent decision for
federation B, and the stuck work MUST NOT be re-issued under a fresh occurrence. No global
boolean or counter may reappear under a new name.

*Provenance: `br-p93`; PR #36.*

### DEF-7 — A structural evacuation refusal could not be released by any operator action — *verified end to end, 2026-08-09*

A refusal whose fixed fee component exceeded the admitted cap base retried forever: `ADR-0029`
forbids terminalizing, the pending action carried the `(base, bps)` pair it was admitted with,
the executor recomputed from those rather than from current policy, and no cancel verb existed.
The operator was told there was a structural refusal, raised the only knob the system exposes,
and nothing changed.

**Prohibition** — a pre-artifact agent evacuation carrying durable structural-refusal evidence
MUST be atomically replaceable by a linked child under a qualifying cap increase (`OPS-30`).
Terminalizing the retryable intent is NOT the fix; that strands the balance evacuation exists to
sweep.

*Provenance: `br-n8o`, `br-0fi`; PR #39.*

### DEF-8 — The settlement-stall watchdog restarted the daemon on a quiet wallet — *verified false-fire, 2026-07-25*

Three legitimately unpaid, unexpired invoices with no other receive activity satisfied the
stall predicate, so a low-traffic wallet entered a restart loop until an invoice paid or expired.
Money-safe, but it polluted the one signal that meant "investigate".

**Prohibition** — a receive counts toward a stall only past its invoice's real expiry plus a
grace period (`ALC-40`).

*Provenance: `br-watchdog-quiet-pilot-875`; PR #17.*

### DEF-9 — A fail-closed fence with no reason tag — *review, 2026-09*

The corrupt-registry fence in the scheduler skipped planning while `scheduler_alive` stayed
`true` and nothing reported why. Three fail-closed suppressions had already been found the same
way: correct to refuse, and invisible.

**Prohibition** — every path that skips planning MUST set `automation_blocked` with a reason
and a detail (`ALC-45`). Liveness is not readiness.

*Provenance: `br-rky` item 3; PR #40 commit `ab52094`. The fence and its reason tag are in PR
#40, not on `main` (`F2`).*

## Persistence and compatibility

### DEF-10 — Three ledger rows became permanently undecodable — *observed, 2026-08-15 onward*

`OperationKind::Refusal` gained a `diagnostics` field with no `#[serde(default)]`. Rows written
before that commit failed to decode forever, were silently absent from `history`, and re-warned
on every read (21,843 warnings in 30,000 log lines). Fail-closed machinery that treats an
unreadable row as repair-only (`DEF-12`) would have fenced the scheduler over three audit rows a
default could recover.

**Prohibition** — a field added to an already-shipped persisted variant MUST carry
`#[serde(default)]`, with a named default function for numerics (`STO-30`), and MUST be pinned
by a test that strips the key from a persisted row and re-reads it (`CNF-18`).

*Provenance: `br-yjg`; PR #42.*

### DEF-11 — Upgrading burned its own rollback path — *verified against the deployed build, 2026-08-30*

`Policy` was both a validated wire input and a persisted row, and carried
`#[serde(deny_unknown_fields)]`. A newer build adding two fields then writing the row once made
the row unreadable to the previous build, so a rollback after the first policy write could not
start.

**Prohibition** — no type written to a live store may carry `deny_unknown_fields` (`STO-31`).
Strictness belongs in the request handler, checked against a key set derived from the type
itself (`API-20`). Every other `deny_unknown_fields` in `wallet-api` is a request-only DTO and
MUST stay that way.

*Provenance: `br-c3j`; PR #43. The deployed `b5f46de` predates the fix, so upgrading past it
still burns rollback on the first policy write.*

### DEF-12 — A migration subsystem that would have stopped automation permanently over three unreadable audit rows — *found by inspecting the live store, 2026-08-23; deleted before merge*

A WatchState agent-floor migration treated any unreadable canonical ledger row as repair-only:
the reconciled flag stayed false, and occurrence advance, standalone observation, the scheduler
preflight and every fresh Agent admission failed closed until an operator restored the exact row
bytes from backup. Deployed onto the store that carried `DEF-10`'s three rows, it would have
ended ticks, rebalancing and evacuation. The stale-checkpoint condition it existed to repair did
not exist: the checkpoint and the ledger's highest Agent occurrence were exactly equal.

**Prohibition** — an unreadable ledger row MUST NOT fence automation (`STO-22`). The floor
invariant is held instead by raising the checkpoint in the same transaction as any Agent ledger
append (`STO-21`) and seeding an absent checkpoint from the ledger (`STO-23`).

*Provenance: `DRIVE.md` 2026-08-23 carve-out; `br-e29` carries the remaining proof obligation.*

### DEF-13 — A persisted-type compatibility rule that named the wrong list of types

`AGENTS.md` required `serde(default)` on new fields of live-store types and listed `Policy`,
`Action` and the move records. Ledger rows were not on the list, which is how `DEF-10` shipped.

**Prohibition** — the list of live-store types is the one in `STO-29`; a type is on it because
the running daemon writes it, not because someone remembered to add it.

*Provenance: `br-yjg`.*

### DEF-14 — A recovery commit without the retry every other commit had — *review, 2026-07*

`complete_recovery`'s commit lacked the autocommit retry the rest of the journal uses, so a
transient write conflict at the one moment a recovery becomes durable would have failed it.

**Prohibition** — a compare-and-swap writer whose guard is read inside its own transaction MUST
go through the retrying autocommit; `STO-8` lists the six that do. Plain writers (`STO-7`) map a
conflict to `Retryable` and leave the retry to the caller — `advance_watch_occurrence` is one,
and a conflict there fails the cycle as `cycle_failed` rather than retrying.

*Provenance: `br-recovery-commit-retry-syv`.*

## Federation clients and recovery

### DEF-15 — No seed-recovery path existed, and the runbook said one did — *verified, 2026-07-22*

`ClientPreview::recover` was never called. The runbook's "wipe the journal and rejoin recovers
the ecash" was false: a rejoin allocated a fresh empty partition, orphaned the funded one, and
reported a zero balance.

**Prohibition** — recovery is an explicit verb with complete-or-fail semantics (`FMI-30`), never
a side effect of a join, and never automatic (`FMI-33`). A runbook claim about what a procedure
recovers requires a run (`CNF-40`).

*Provenance: `br-m9m`, `br-recovery-live-devimint-gate-j5p`; PRs #11, #12; `ADR-0025`.*

### DEF-16 — The SDK pin lacked the single-share TPE fix, and the test federation could not show it — *verified against the git objects, 2026-07-25*

At the pinned revision, decrypting with a single decryption share panicked, killing the client's
state-machine executor and freezing every cross-federation operation. The devimint harness runs
four guardians, so no smoke exercised it; a single-guardian federation hit it on every lnv2
decryption.

**Prohibition** — the pin MUST carry the single-share fix, and a single-guardian regression gate
MUST exist because the standard harness cannot catch this class (`CNF-32`). Any repoint of the
dependency MUST carry every fork-only fix (`FMI-2`).

*Provenance: `br-fix-pin-tpe-8838-djs`; PR #13.*

### DEF-17 — Two client handles on one partition — *review, 2026-07-24*

`open_all` did not take the join lock, so the scheduler's open racing a user's join could
briefly run two client handles on one database partition, the one state the client layer
forbids.

**Prohibition** — every open of a federation client MUST be serialized under the join lock
(`FMI-20`).

*Provenance: `br-dual-open-one-race-315`; PR #15.*

### DEF-18 — An unbounded network fetch under the join lock — *review, 2026-07-24*

Recovery and user join held the join lock across an unbounded federation preview, so an
unreachable federation queued every later join and recovery. A comment claimed the fetch was
bounded; it was not.

**Prohibition** — the preview under the join lock MUST be bounded (`FMI-21`).

*Provenance: `br-recover-preview-bound-xyc`; PR #16.*

### DEF-19 — The devimint runbook could not produce a green run as written — *reproduced, 2026-08-07*

The documented environment enabled only the lnv2 module; the client's primary module is mint
v1, which devimint does not enable unless asked. Every smoke that read a balance died in 0.4
seconds. This is why the live evacuation gate had never run on the machine that owned it.

**Prohibition** — a runbook's invocation MUST be re-run from a clean shell before it is called
correct (`CNF-40`); the failure signature MUST be recorded verbatim so the next reader lands on
the fix by grep.

*Provenance: `br-devimint-runbook-mint-na3`; PR #35.*

## Documentation that misled

### DEF-20 — "Stranded" was documented as a gateway failure the preimage could recover — *17 sites across 9 files, 2026-08-01*

The stale description led to a proposal to build preimage-recovery tooling that could not have
recovered anything. Three independent audits of the corrected text each found a false protocol
claim in the causal story it tried to tell.

**Prohibition** — no code comment may carry a causal taxonomy of an unobserved money state
(`OPS-40`). The runbook is the operator account; the code says what the state IS, not why it
arises.

*Provenance: `br-p4h`.*

### DEF-21 — Seven sites still said an evacuation sizes off the flat cap after it stopped doing so

The one that mattered was the runbook's `policy set` sample, which during an incident would have
had an operator raise a knob that cannot bound an evacuation while retries continued unchanged.

**Prohibition** — a rule has one owner; everywhere else cites it (this set's README).

*Provenance: `br-cqv`.*

### DEF-22 — Tracked files carried the test deployment's hosting provider, namespace, pod name, image digest, uptime and exact balance — *2026-09-03*

Redacted at HEAD. The data remains in git history and in the edit history of a pull-request
body.

**Prohibition** — deployment identity MUST NOT appear in tracked files (`SEC-20`). The runbook
holds the location; everything else points at the runbook.

*Provenance: `br-kcw` (the remediation decision is still open — `F21` in
[11-open-findings.md](./11-open-findings.md)).*

## Tests that proved nothing

### DEF-23 — A concurrency test that could not fail — *found by review of PR #39, 2026-09-04*

The test meant to pin "a claim committing inside the exchange window leaves one executable
intent" set up a rendezvous barrier that the production path never waited on. It passed against
a broken exchange. Rebuilt as a take-once park, and watched go red against both of the guards it
now pins.

**Prohibition** — a test added for a property MUST be watched to fail against the broken
production behaviour, one mutation per property (`CNF-2`). A test asserting behaviour that was
already correct is indistinguishable from a test asserting nothing.

*Provenance: `br-n8o` close note; commit `beba9ab`.*

### DEF-24 — A new fence test passed against nothing — *2026-09-06*

The corrupt-registry test planted its poison row in the bare database rather than the journal's
`0x00` partition, so the fence never saw it and the test's first run returned
`automation_blocked: None` for the wrong reason.

**Prohibition** — a test that plants a raw row MUST plant it under the partition the code
reads (`STO-1`), and MUST be watched red before it is trusted (`CNF-2`).

*Provenance: PR #40 commit `ab52094`.*

### DEF-25 — The live evacuation smoke could not detect a regression to the flat cap

It set the flat cap far above the fee it asserted, so an evacuation reverting to the old sizing
would have passed unchanged. Still open as `br-vvo` (`F23`).

**Prohibition** — a live gate MUST discriminate: its parameters MUST be chosen so the old
behaviour fails and the new one passes (`CNF-3`).

*Provenance: `br-vvo`.*
