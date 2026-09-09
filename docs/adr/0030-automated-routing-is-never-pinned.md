---
status: accepted
---
# Automated routing is never pinned; the operator keeps a break-glass bound to one operation

> Rewritten 2026-09-09 to record the shipped design. The earlier text of this ADR mandated a
> money-only capability type, a reason-code gate on await re-drives, and a durable authenticated
> origin marker; two independent reviews concluded all three defend against a threat the executor
> never bounded (in-process code already holds the fedimint client) and that binding the override
> to one operation key makes them unnecessary. The history is in git.

## Decision

1. **Automated routing resolves only from the federation's vetted list.** The allocator, the
   scheduler, probes, route economics and evacuation scan the lists the guardians publish and
   nothing else. `walletd.toml` has no `gateway` key and rejects one; the daemon constructs its
   runtime unarmed; `Runtime::route_gateway_candidates`, `FedimintProbeRunner` and
   `route_econ::price_missing_pairs` have no override input at all.

2. **The operator keeps a break-glass, and it is bound to ONE operation.** `wallet-cli
   --standalone --gateway <url>` arms a `BreakGlass { key, gateway }` on the runtime for the one
   operation the invocation names: the money verb's own idempotency key, or the key an await verb
   was given. `FedimintExecutor::override_for(key)` is the single point every route resolution
   consults, and it answers only for that key. Naming the operation is the authorization.
   - **Accepted** on `pay`, `receive`, `move`, `direct-inflow`, `await-receive`, `await-send`,
     `await-move`.
   - **Rejected**, loudly (usage error), on `tick`, `probe`, `discover`, `status`, `reconcile`:
     those verbs run the same machinery walletd runs, and a gateway on them is the daemon pin
     under another name.
   - **Ignored** on verbs that resolve no route (`join`, `balance`, `history`, `show`,
     `list-feds`, `policy`, `health`, `candidates`, `approve`, `recover`), so a helper that
     appends the flag to every call does not break.

3. **An operator may name an allocator-created operation.** The incident this exists for is a
   federation whose vetted gateways are all dead while consensus still redeems the ecash; the
   stuck operation is then usually the allocator's own evacuation, which holds a reservation no
   manual `move` can get past and which no verb can cancel. `await-move <that key> --gateway`
   re-drives exactly that operation through the named gateway. There is no reason-code gate and
   no origin marker: the key is a selector, not a credential, and the only party who can hand a
   key to the executor is the one running the process.

4. **Committed routes replay; drafts never do.** A move whose receive or send leg has committed
   — on its `MoveRecord`, or recovered from the op-log when the process died between the receive
   and the cache write — keeps the recorded gateway and replays it, flag or no flag. A record
   written before any leg committed (the pre-receive write, or a pre-mint refusal) is a draft: the
   break-glass for that key takes it, and otherwise it is re-resolved rather than replayed, so a
   gateway chosen under a flag never outlives the invocation and a dead vetted gateway never
   sticks. After cache loss a send-required move's committed gateway is not recoverable from the
   op-log (F7, `br-s0e`); such a re-drive resolves afresh, override or not.

5. **The override is non-durable.** It is never journaled into the intent; `Action::Pay` and
   `Action::Receive` still carry a `gateway` field, always `None`, kept only so the previous
   build can decode rows this build writes. Repeating an operation means repeating the flag.

## What the break-glass skips, and what it does not

It skips vetted-list membership and the two-end preselection. It does not skip the operation's
own liveness check (`routing_info` must answer for the source federation before anything is
minted) or the fee cap, which is re-checked at the pay step however the route was chosen. An
evacuation's pre-mint viability check (`total_fee <= delivered net`) also still runs; a manual
`move` has no such check yet, break-glass or not — that arrives with `br-y2j` and will apply to
every route. Residual, stated rather than papered
over: an unvetted gateway can quote cheaply and charge dearly between quote and commit; the cap
bounds what the wallet knowingly agrees to, not what such a gateway does afterwards. Vetting is
what normally covers that, and setting vetting aside is the operator's explicit choice.

## Why

**A pin and a break-glass were one field with two masters.** `FedimintExecutor.pinned_gateway`
was set from `walletd.toml` (a standing property of every automated decision) and from the CLI
flag (an operator's one-off), and the code contradicted itself about which it meant. The cost
was not routing: a pinned daemon handed the pin to every probe, the probe validated only that
URL, and a one-end or stale pin marked the federation unroutable, so **no evacuation was ever
emitted** while unit tests passed. A knob that can silently disable evacuation is not
configuration. Production never set it; the six devimint smokes that did only did so because
devimint never registers its LDK gateway, which is fixed by registering it on every guardian.

**Deleting the flag too would remove the only wallet-side exit from a dead vetted list.** Only a
guardian can add a gateway. Without the break-glass the operator's remaining option is a code
release.

**Binding the override to a key dissolves the machinery the earlier text asked for.** With a
process-wide override, the standalone await verb's recovery pass re-drove every pending intent
under the flag, which motivated scoping recovery to the key, gating on the reason code, and then
authenticating the reason code with a durable marker. With the override keyed, recovery can
re-drive everything unchanged and only the named intent observes the gateway; a tick, probe or
scheduler creates intents with keys nobody named, so the "structural" property holds without a
capability type, and it is proven at the executor boundary by a direct test rather than by
enumerating entry points.

## Consequences

- The proving tests are `the_break_glass_applies_to_the_named_key_only` in
  `wallet-fedimint/src/executor.rs` (an armed executor resolves the override for its key and the
  empty vetted list for an allocator key) and the incident smoke
  `wallet-cli/tests/smoke_breakglass_devimint.sh`: fed B's vetted list empty throughout, walletd's
  scheduler refusing B (`not_probed`) and leaving daemon-admitted moves into B `Pending` on "no
  lnv2 gateway", one `await-move <key> --gateway` completing exactly that move, `move`/`receive`/
  `pay --gateway` routing against the empty list, `tick`/`probe --gateway` refused, `balance
  --gateway` ignored, and a second pending move left untouched. The allocator never creates a
  move into a federation whose probe finds no vetted gateway, so re-driving an allocator-created
  key is proven by the unit test, not live; the live case is an evacuation whose route died
  after it was planned.
- Every devimint smoke registers the LDK gateway on all reachable guardians
  (`wallet-cli/tests/devimint_lib.sh`) and runs unpinned. The responsiveness gate registers its
  never-responding double instead; registration does no liveness check, so the double is reached
  through the vetted list and hangs exactly as it did under the pin.
- Restoring automated movement after a vetted-list failure is guardian-side: register on every
  reachable guardian rather than a computed minimum (`br-gw-threshold-membership-k4t` owns the
  support threshold). The break-glass moves money in the meantime; it does not fix the
  federation.
- `Runtime::new` takes an `Option<BreakGlass>` for construction-time arming;
  `Runtime::arm_break_glass` is the one-shot the CLI uses once a verb knows its key. The legacy
  `Runtime::pay`/`receive` entry points no longer take a gateway.

## Alternatives rejected

- **Delete both pins.** Removes the only incident capability while the runbook still depends on
  it.
- **Keep both.** Leaves a standing config able to suppress evacuation, and a four-case pin table
  for the evacuation fallback to govern a knob no deployment sets.
- **Process-wide override with reason-code gate and durable origin marker.** Defends against
  forged intents from in-process callers who can already pay through any URL directly; costs a
  persisted field with a legacy-row rule and an upgrade-window runbook note; and refuses the
  stuck-evacuation case the break-glass exists for.
- **Key-scoped, but refuse automated reason codes.** Same loss of the evacuation escape, for a
  boundary the reason code cannot provide.
