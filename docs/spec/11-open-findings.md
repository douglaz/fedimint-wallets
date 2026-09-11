# 11 — Open findings

What the code does not yet do that a document, an ADR, or a review said it should. Each finding
names the issue that tracks it (`br-…` in `.beads/issues.jsonl`), or says why none exists, so the
two cannot drift apart silently; when an issue closes, its finding here is marked closed with the pull request, not
deleted.

Written 2026-09-07 against `main` at `1e44487` plus PR #40, both merged as `ee4ba1c` on 2026-09-08;
re-checked against `7225114` on 2026-09-10. Items are
grouped by what they cost if left alone, not by the order they were found.

## The questions still open at the product level

These are not findings against the code. They are decisions nobody has taken, and the code is
built so that either answer remains possible.

1. **Is the long-running deployment a test or a pilot?** The daemon has been running the
   2026-07-26 build `b5f46de` on two mainnet federations with a small real-sats balance. The
   repository's release issues (`br-prod-canary-nab`, `br-recanary-y2j-ujs`) and its alerting
   issue (`br-rky`) still treat it as production; the operator has since said it is a test rig,
   and `AGENTS.md` was changed to say so on 2026-09-08. Every P1 release/ops item in the backlog inherits its priority from the first
   reading. This set records the second, and `08-hosts-and-deployment.md` describes the
   deployment as a test.
2. **Does the engine ship ON by default?** `docs/roadmap-to-v1.md` defers this to Phase 8: a
   fee-vs-risk expected-value computation at $50–$500 balances plus a legal opinion on the
   `ADR-0014` posture. Nothing in Phases 4–7 depends on the answer.
3. **Which frontend is next?** The roadmap says the web sidecar (6c) ships before Android (6b).
   6c's issues were cut on 2026-07-30 and have not moved; the work since went into `F1`–`F9`.

## Money paths that are narrower than their documents claim

**F1. A funding shortfall below the route floor is withheld with no operator signal.** Confirmed
by pricing the live pair with the tick's own code: a 301,586 msat standby shortfall against a
route whose economic floor is ≥ 450,102 msat. The floor is correct (`ALC-10`); what is missing is
any refusal row, decision, or log line when it binds. PR #41 (2026-08-30) added observability for
the deferred-funding case; the issue remains open pending confirmation that the signal is
emitted on the live path. Open — `br-0vg`.

**F3. The evacuation fallback in `ADR-0029` is half built.** The proportional cap exists
(`ALC-20`). The second route — a real Lightning hop through two gateways on different nodes when
none serves both federations — does not. A dying federation with no shared gateway cannot be
drained. `ADR-0029` was rewritten 2026-09-09 (per-leg selection, evidence-based fallthrough, the
node-key rule, a perform-failure record) and `br-s0e` rewritten to match; it depends on
`br-routing-invariants-f6f7-dwx` (F6, F7) and is no longer blocked on F5. Open — `br-s0e`.

**F6. Source-side vetted-list membership is not enforced, and the vetted list is a union.**
Selection starts from the destination federation's list and validates the source end only by
fetching `routing_info` (a route hint is now checked against the destination's list, not the
source's); a gateway vetted by the destination alone, or revoked by the source since, can still
carry an automated move. Separately the SDK's gateway list flattens every URL any responding
guardian returned, so one misconfigured or malicious guardian can place a gateway in the
automated candidate set. Decided 2026-09-09: shared candidates come from the intersection of both
lists, and a list is the gateways at least `NumPeers::threshold()` guardians each return, read
per guardian.
`ADR-0029` and `ADR-0030` record the target; `CONTEXT.md`'s **Serves**, **Vetted list** and
**Route hint** entries each point here as the gap. Open — `br-routing-invariants-f6f7-dwx`
(a bound on each guardian's response stays with `br-gw-threshold-membership-k4t`; no ADR
records that bound — the rewritten `ADR-0030` no longer mentions it, and `SEC-17` says so).

**F7. The route is not persisted with a committed operation.** After cache loss the operation
artifact carries no gateway, so reassembly resolves afresh and a restart can pay through a
different gateway than the one the invoice was sized for. `ADR-0030` records the rule; `CONTEXT.md` **Committed route** points here as the gap.
Open — `br-routing-invariants-f6f7-dwx`.

**F8. A receive refused after commit leaves the ledger row on the planned pair.** When the
never-over check fails after `mc.receive` has committed, the intent terminalizes `Failed`, the
row keeps the planned amount and cap, and terminal immutability forbids correcting it. No funds
moved; what is missing is the amount and cap the abandoned contract was committed at. The
persistence ordering is load-bearing (`OPS-24`), so the fix needs its own design pass. Open —
`br-v8x`.

**F9. The enforced fee cap is not on the wire.** `FeeBreakdown.fee_cap` reaches only
`wallet-cli --standalone show`. The audit `DEF-4` was fixed for cannot be performed against a
running daemon. Open — `br-w6p`.

**F10. There is no aggregate exposure ceiling.** Only `per_fed_cap` is enforced, so the total
a policy permits is `per_fed_cap × joined federations` and rises with every join. Harmless
while `auto_join = false`; urgent the moment it is not. Open — `br-aggregate-cap-th6`.

## Durability and security that the roadmap calls Phase 7

**F11. The seed is plaintext on disk.** `load_or_generate_mnemonic` stores raw entropy with no
application-level encryption. Anyone who can read the data directory owns the funds. `ADR-0026`
accepted a passphrase-derived key and deferred the build; the implementation issue was filed on
2026-08-07 and has not started. This is also `ADR-0028`'s stated precondition for exposing the
web sidecar beyond loopback. Open — `br-seed-at-rest-impl-3c1`.

**F12. There is no app-state backup.** Seed recovery rebuilds ecash, not send-dedup state, the
ledger, or the candidate registry. A host-disk loss recovers the funds and loses the record of
what was in flight — the precondition for a double-pay on an in-flight send (`FMI-32`). Open —
`br-appstate-backup-7o2`.

**F13. Federation consensus and module upgrades are unhandled.** `ADR-0027` accepted this for a
short pilot. The deployment is now six weeks old. No issue: `ADR-0027` owns the decision, and
reopening it is a decision, not a task.

## Observability

**F14. Nothing runs the readiness poller.** `/v1/health` reports `automation_ready` and a tagged
`automation_blocked` (`ALC-45`), and `ops/walletd-watch.py` reads them with an exit-code
contract. No cron entry or CronJob runs it, and the deployed `b5f46de` predates both fields, so
the signal is neither emitted nor read anywhere today. Open — `br-rky`.

**F15. Conflict-suppressed allocator goals are durable but not in status.** The withheld
candidate gets a refusal row; the holder that suppressed it and the "deferred, not evaluated"
distinction are not exposed. Open — `br-suppression-observability-a1b`.

## Admission and architecture

**F16. Agent admission is not sealed.** Public core helpers let an in-process caller construct
and journal an Agent allocator intent outside the actor's serialized admission seam that
`ADR-0031` names as the contract. Open — `br-seal-agent-admission-yfr`.

**F17. The scheduler cycle is not yet an actor command.** `ADR-0031` decisions 2–3: expose the
one-shot cycle so a non-resident host (Android) drives it, and demote `Runtime::watch_once` to a
harness. Not started. Open — `br-scheduler-cycle-command-gpr`.

**F18. The Agent occurrence floor's proof obligation is open.** `STO-21`–`STO-23` state what
holds; the issue asks for a property-level proof that the checkpoint is never below the ledger
across every admission path. Open — `br-e29`.

## The SDK dependency

**F19. The workspace pins a fork carrying three patches**, with a fourth proposed. iroh long-poll,
recovery complete-or-fail, the single-share TPE fix (`FMI-1`); lnv2 claim-retry is proposed in `F20`. Repointing to
upstream requires every one of them to have landed there (`FMI-2`). The long-poll patch does not
remove the iroh stall itself (`FMI-38`), and the transport a federation is reached over is decided
by its invite and config, not the wallet (`FMI-36`), so a repoint changes which patches are carried
but not that exposure. Open — `br-jga`.

**F20. A move that reaches `Stranded` has no recovery procedure.** The pinned lnv2 client parks a
receive whose claim transaction was rejected, permanently. Upstream PR #8935 adds bounded claim
retry and a `reclaim_receive` path, retroactively applicable to records our pin wrote. Not
adopted. The operator response until then is evidence preservation (`HST-28`). Open —
`br-adopt-lnv2-claim-retry-d3d`.

## Tests and gates

**F21. Deployment identity is in git history.** Redacted at HEAD (`DEF-22`); the history, and a
pull-request body's edit history, still carry it. The three options — accept, rewrite history,
or rotate what can be rotated — are the operator's. Open — `br-kcw`.

**F22. The live evacuation smoke cannot detect a regression to the flat cap**, the
post-revalidation refusal arm is unpinned, and the receive-side ppm warning arm is unpinned
(`DEF-25`). Open — `br-vvo`.

**F23. The delivered-net cap basis is not pinned at the two production sites a unit fixture
cannot reach** — the pre-mint gate and the executor's post-receive recompute — because the test
route cannot produce `delivered ≠ ask` at all until it gains a receive-side federation fee.
Open — `br-evac-cap-driven-basis-v07`.

**F24. The recovery FAILURE path has no live gate.** Complete-or-fail is unit-tested in the fork;
inducing a module-recovery failure live needs a fault hook that does not exist. Open —
`br-recovery-failure-live-gate-forkhook-o7b`; analysis in
`docs/recovery-failure-gate-analysis.md`.

**F25. The live supersession gate omits the after-receive-commit killpoint.** The gate is green
twice on two federations and covers restart-and-reconcile; a mid-flight crash on the replacement
child is not in it. Recorded as a deviation in `br-0fi`'s notes, which is closed. Open — `br-supersession-child-killpoint-gate-7c9`.

**F26. Four money-path boundaries of the supersession exchange have not been read by a human.**
`replace_marked_evacuation`, `commit_evacuation_replacement`,
`replace_marked_evacuation_standalone`, and the validator block — about 950 lines — were merged
on the operator's explicit call with this stated. If a supersession defect surfaces, start there.
Recorded in `br-n8o`'s close note.

## Frontends

**F27. The web sidecar is a skeleton.** Config, `init`, password hashing, and a fail-closed
startup exist (`HST-26`); it serves no routes. Eight issues cover the rest and none has moved
since 2026-07-30. Open — `br-2aa`, `br-nfz`, `br-5om`, `br-pfc`, `br-t8f`, `br-ucq`, `br-4yz`,
`br-web-ops-exe`.

**F28. The Android frontend does not exist.** Phase 6b in `docs/roadmap-to-v1.md`. No issue: a
phase, not a task; beads get cut when it is planned.

## Hygiene

**F29. The evacuation-marker predicate is written three times.** In the CLI, the daemon
handlers, and (cap-qualified) the actor. A money-path predicate duplicated per frontend is how
two frontends start disagreeing about whether a marker is active. The rest of the same review's
list — a byte-identical helper in two crates, two parallel ten-field structs, two inert
fail-closed guards, two single-use abstractions — is in the same issue. Open — `br-pv8`.

**F30. The glossary retires "executed net" while four code sites still use it** for a different
contrast (executed vs planned) than the one it was retired for (sized ask vs delivered net).
Open — `br-7xc`.

**F31. Nine tracked documents name development tooling** against the workspace convention.
Open — `br-h34`.

## Found while writing this set

These were surfaced by the code extraction on 2026-09-07 and filed the same day, except `F44` and
`F45`, which the 2026-09-11 spec review surfaced and filed that day.

**F32. A failed `ReconcileDecide` is not reported as blocked.** When the tick-plan token cannot
be issued (a poisoned authority, or a lease live at that instant) the cycle skips the tick row,
route pricing, commit and fresh probes, and still publishes `automation_blocked: None`, so
`/v1/health` says ready. The `HealthView` doc promises `automation_ready = false` whenever the
whole automated cycle is skipped every pass. Only a warning is logged (`ALC-47`). Open — `br-reconciledecide-blocked-unreported-twb`.

**F33. Eight fail-closed paths leave no ledger row, and several no log line.** Listed in
`ALC-48`. Two deserve attention: the settlement-stall watchdog disarms silently on a journal read
error, and a `get_policy` failure in the wait loop re-cycles with no sleep. Open — `br-silent-fail-closed-paths-7th`.

**F34. The light probe runs up to four times per scheduler cycle**, each a live threshold read
and gateway validation per federation (`ALC-49`). Whether that is intentional is undocumented.
Open — `br-light-probe-four-times-per-cycle-swk`.

**F35. The CLI hides both readiness signals.** `wallet-cli health` prints `scheduler_alive` and
omits `automation_ready`/`automation_blocked`; `wallet-cli status` in client mode drops
`deferred`, the only surface for `F1`. Both exist on the wire (`API-15`, `API-16`). An operator
using the CLI in its default mode cannot see either. Open — `br-cli-hides-readiness-and-deferred-mfd`.

**F36. Two evacuation leads with one name.** The trigger lead is a hard-coded 24 hours; the
runtime-mutable `evacuation_lead_secs` governs only when the scheduler wakes (`ALC-19`).
Renaming the policy field, or making the trigger read it, is a decision. Open — `br-evacuation-lead-two-meanings-2uv`.

**F37. `RefuseReason` is classified by message substring** (`OPS-39`). The strings are a wire
contract nobody documented as one. Open — `br-refusereason-substring-contract-nlv`.

**F38. The deployed store may hold rows without a default for two later-added fields.**
`OperationRecord.repaired` and `WatchState.discover_rotation` carry no `serde(default)` on the
claim that no row predates them (`STO-32`). The long-running deployment's store has not been
checked. Open — `br-deployed-store-missing-defaults-cts`.

**F39. Stale doc comments the extraction found** that a reader would act on: `IntentStatus::
Awaiting` "DirectInflow only"; `journal.rs`'s header
listing seven of thirteen tags and describing one database; `Policy`'s "no seed-recovery path
wired yet"; a test comment claiming `Policy` is `deny_unknown_fields`; `fedimint-mechanics.md`'s
"hard fee cap"; the runbook's "shipped k8s config". The four prose items — the runbook's
"shipped k8s config" and "pages on transition" claims, its stale lock-file line citation, and
`fedimint-mechanics.md`'s "hard fee cap" (now cites `FMI-19`) — were fixed 2026-09-10 in the
spec review pass, and the `TimeoutExecutor` item is withdrawn 2026-09-11: its doc comment is
accurate for the `Runtime`-direct paths that are the only ones to build it (`OPS-15`), so four
code-comment items stand, plus `fedimint-mechanics.md`'s stale `executor.rs` line citations, which
the bead carries and this finding had not. The bead was updated to match on 2026-09-11, and its
runbook bullet was struck there: both halves of it — "shipped k8s config" and the `main.rs`
lock-file citation — were fixed on 2026-09-10, and the runbook now names `check_db_lock` with no
line number.
Each is a one-line fix; none is a
behaviour gap. Open — `br-stale-doc-comments-sweep-zjx`.

**F40. `policy set` from an older CLI silently resets a field a newer daemon added.** The CLI
round-trips a typed `Policy`, so an unknown key is dropped on GET, omitted on PUT, and defaulted by
the daemon — a money cap included. Correct within one version; a silent reset across a version skew
(`API-27`). Open — `br-policy-set-drops-unknown-fields-3e1`.

**F41. Two of twelve `serde(default)` persisted fields have no strip-key test.** Ten are pinned
by a strip-or-absent-key decode test on the serialized type; the two `Evacuate` defaults share
one bare-`Action` fixture that omits both keys at once (`CNF-18`, `STO-30`). Two earlier drafts
of this finding under-counted the tests that exist; the bead was corrected each time.
Open — `br-strip-key-tests-missing-2za`.

**F42. `wallet-cli --standalone probe` admits its money legs without the actor.** `active_probe`
runs with no service client, so both legs go through `Runtime::do_move`, which journals and
drives directly: the actor's conflict, goal, driver-cap and probe-hold checks do not run. Source
funds and the destination cap are still checked, by the probe preflight and by the executor's
pre-fund admission (`OPS-5`, `OPS-7`, `OPS-12`). `ADR-0031` documents one exception (`tick`);
this is a second, architectural rather than a money hole.
Open — `br-standalone-probe-bypasses-actor-253`.

**F43. Standalone `probe` enforces the compile-time `per_fed_cap`, not the stored policy's.**
`probe` (and `discover`, whose auto-join never probes and so spends nothing under it) builds its
runtime with `operator_hard_cap(false)` = `TickPolicy::default().per_fed_cap` (5,000,000,000
msat), while the daemon and standalone `tick`/`status` plan against the stored
`Policy.per_fed_cap` (default 1,500,000,000; `OPS-7`, `ALC-3`). An operator who lowers the
stored cap can still have a standalone probe leg mint above it. Fix is to build that cap from the stored
policy as `build_standalone_tick_policy` does and delete `operator_hard_cap`. Open —
`br-standalone-probe-hard-cap-default-o6b`.

**F44. A user move can attach to an active probe leg.** Probe legs use the user-move `move:` key
shape with `occurrence = occurrence_from_nonce(nonce)`, and `MoveRequest.occurrence` accepts any
`u64`, so a user move with the same endpoints, amount and cap and an occurrence equal to a nonce
head attaches to the probe's `Intent` and `MoveRecord` (`STO-6`, `DOM-16`, `OPS-8`). The
separation is probabilistic, not excluded, and probe keys are reconstructible from a session that
history exposes. An implementer building to `STO-6` today is building a key the fix will change.
Two properties of the fix are settled and belong here; the design itself is the bead's.

No **legacy read** is needed whichever variant wins: that is the shim `OVR-14` forbids, and
`STO-24` puts every move-shaped row in the never-repaired class, so rows already written keep
their shape and go on being skipped. (The bead's earlier legacy-read requirement was withdrawn on
2026-09-11.) A new `classify_key` prefix is needed only by the `probe-leg:` variant; a reserved
occurrence range keeps the `move:` shape and the existing classification.

And the **rollout is the hard part, in both directions**. A `ProbeSession` persists only the nonce
and parameters, so each build reconstructs the leg key from the nonce — and every variant that
actually closes the hole changes what gets reconstructed for a session already in flight, whether
by changing the shape or only the occurrence. An upgrade or a rollback mid-probe can therefore
strand or duplicate a leg, so a version field alone is not enough: it needs probes drained on both
sides of the deploy, or two builds that each understand both mappings. Settle that in the bead
before writing code.
Open — `br-probe-user-move-key-collision-fy7`.

**F45. The gateway `routing_info` POST reaches any URL a guardian lists.** `FMI-11`'s validation
POSTs to `SafeUrl::parse(gateway).join("routing_info")` for every URL on a federation's vetted
lnv2 list, and `SafeUrl` only wraps `Url::parse`: loopback, link-local, RFC1918 and
cloud-metadata hosts are not rejected, and the client honours the system proxy environment
(`HST-2`). A guardian that lists `http://169.254.169.254/` or a loopback admin port makes the
wallet host issue that request. `FMI-11` describes the mechanism; nothing restricts the egress.
Open — `br-gateway-url-ssrf-egress-qqg`.

## Closed since this document was first written

**F5. The sizing API cannot distinguish a proven structural refusal from an inconclusive
bounded miss.** Closed 2026-09-09 as dissolved, not fixed: the rewritten `ADR-0029` no longer
requires the distinction. An empty bounded sizing result means the route does not serve THIS
attempt, the hop is tried in the same tick, and the next fresh attempt starts swap-first again;
`ALC-24`'s two-point evidence remains what it is and feeds only the supersession audit record.
`br-u4i` closed.

**F4. Automated routing still honours a pinned gateway.** Closed 2026-09-09: the `walletd.toml`
`gateway` key is gone and rejected, the daemon constructs its runtime unarmed, and the standalone
`--gateway` break-glass is bound to the one operation key the invocation names
(`FedimintExecutor::override_for`), rejected on `tick`/`probe`/`discover`/`status`/`reconcile`
(`HST-3`, `HST-10`, `FMI-14`, `ADR-0030` rewritten). The devimint smokes register the LDK gateway
on every guardian instead of pinning. `br-remove-gateway-pin-yjw` closed.

**F2. The partial/corrupt federation world-view fences were implemented but unmerged.** Closed
2026-09-08: PR #40 merged as `ee4ba1c`; `GET /v1/status` 503s, the scheduler's recovery-only
cycle tagged `corrupt_federation_registry`, and the standalone `tick`/`status` refusal are on
`main` (`ALC-46`, `CNF-51`). `br-19g` closed.
