---
status: accepted
---
# Automated routing is never pinned; the operator keeps a break-glass

Two rules, drawn along a line the code did not previously have:

1. **Automated routing resolves only from the federation's vetted list.** The scheduler, the
   allocator, probes and evacuation may never be pinned to a gateway. The `gateway` key is
   removed from `walletd.toml`, so a daemon cannot express one.
2. **The operator keeps a single-invocation break-glass, on money verbs only.** `wallet-cli
   --standalone --gateway <url>` survives for the verbs that move money at an operator's explicit
   instruction, and deliberately routes through a gateway **outside** the vetted list without
   validating it first. It is **rejected** for `discover`, `probe`, `tick` and `status`.

The asymmetry is the decision: the CLI has a flag the daemon config does not. That is intentional
and is the thing a future reader will otherwise assume was an oversight.

**Rule 1 is about the decision, not the process.** "Automated" means the scheduler/allocator/probe
machinery wherever it runs — including when a human starts it from a terminal. `wallet-cli
--standalone tick` runs the same allocator that walletd runs; `probe` and `discover` write the
same health signals the allocator reads. Letting a flag reach those is the daemon pin under
another name: a stale `--gateway` on a standalone `tick` can suppress healthy vetted routes, mark
a destination unusable, and force an evacuation route. The flag survives only where a human is
directing a specific payment, not where they are starting a machine that decides for itself.

## Why

**A pin and a break-glass were the same field, and that conflation cost real design work.**
`FedimintExecutor.pinned_gateway` was set from two places with opposite intents. The daemon's
`walletd.toml` key made it a standing property of every automated decision. The CLI flag made it
an operator's one-off. The codebase described both in the same breath and contradicted itself
about which it meant: `wallet-daemon/src/config.rs:36-37` called it "a deployment fact, not user
policy", while `wallet-fedimint/src/executor.rs:279` called it "an operator pin [that] overrides
route selection entirely, planning included".

That ambiguity propagated. A bead specifying evacuation's second route had to decide what a
source-only, destination-only, shared, or unusable pin meant for automatic fallback — four cases,
each with money consequences, on a path no operator had ever pinned. Removing the daemon pin
deletes all four questions instead of answering them.

**The daemon pin's cost was not limited to route selection.** A pinned daemon hands the pin to
every federation probe; probing then validates only that gateway and never scans the registered
list (`probe.rs:458`). A failure sets `probed_ok=false` (`probe.rs:153`), and the allocator drops
that federation as an evacuation destination (`allocator.rs:437`, `:471`). So a pin that served
one end — or a stale one serving neither — meant **no `Action::Evacuate` was ever emitted**, while
executor-level tests would pass. A knob that can silently disable evacuation has no business
being a standing configuration.

**But deleting the flag as well would have removed an incident capability the runbook depends
on.** Explicit-gateway `send`/`receive` skip the vetted list and check only `routing_info`
(`fedimint-lnv2-client/src/lib.rs:574-587`), so the flag is the *only* way this wallet reaches an
unvetted gateway. The runbook's gateway-outage entry says "moving funds is a manual operation"
(`docs/real-sats-pilot-runbook.md:290-295`) — and with a dead or empty vetted list, a manual verb
*without* `--gateway` fails exactly as the automated path does. The documented remedy silently
depended on the flag.

The incident it covers is specific and unpleasant: a **live but unadministered federation whose
vetted gateways are all dead**. Consensus still redeems the ecash. Only a guardian can
`gateways add`. Without the break-glass the operator's remaining option is to ship a code
release.

**Nothing deployed relies on the daemon pin.** The production `walletd.toml` is a ConfigMap
carrying exactly `data_dir`, `address`, `port`, `token_path`, `log_level`
(`remote-devops/k8s/argo/walletd/walletd.yaml:24-29`). Production has always run unpinned, and
the 2026-07-28 canary paid a real invoice — so the joined federations' vetted lists are non-empty
and validating, and removing the daemon pin cannot strand one behind an empty list.

## Consequences

- **The break-glass does not validate, and that is the point.** `resolve_move_gateway` returns a
  pinned gateway without checking it serves the route. For automated routing that was a defect;
  for an operator overriding a dead vetted list it is the required behaviour. Do not "fix" it.
- **Restoring automated movement after a vetted-list failure is guardian-side.** `gateways add`
  is a per-guardian authenticated write, not a consensus item
  (`fedimint-lnv2-server/src/lib.rs:696-704`), and a client's view unions the first threshold of
  peer replies — so it must be run against *every* guardian or the gateway appears
  nondeterministically. An operator with no guardian cooperation cannot repair the list at all;
  the break-glass moves money in the meantime, it does not fix the federation.
- **Devimint smokes must register rather than pin.** Only the five daemon-pinning smokes change
  (daemon, soak, responsiveness, recover, daemon_chain); the nine that use the standalone flag are
  untouched. This finally gives the registered-scan path live coverage — today every smoke pins,
  so the code production actually runs has none.
- **The responsiveness gate is the awkward case.** It pins a never-responding double *because* the
  pin skips validation. Converting it to the vetted list means the double must answer
  `routing_info` and hang only on payment endpoints, which in turn breaks its accept-level timing
  oracle: HTTP connections are pooled and reused (`fedimint-connectors/src/http.rs:57-61`), so a
  request-level double is required. Budgeted in `br-remove-gateway-pin-yjw`, not discovered later.
- **`Action::Pay { gateway }` / `Receive { gateway }` become setter-less.** Their only populator
  is the standalone flag, so they remain reachable only through the break-glass. They are hard
  constraints with no registry fallback (`executor.rs:1017-1027`, `:1133-1138`) — deliberately
  retained, and distinct from the `Move`/`Evacuate` route *hint*, which is checked before use.
- **This does not make routing reliable**, and no document should say so. A federation whose
  guardians vet no reachable gateway is unroutable automatically, and the honest operator
  statement is that the break-glass buys manual movement while the list is repaired.

## Alternatives rejected

- **Delete both pins.** Simplest end state, and it was the original plan. Rejected because it
  removes the only wallet-side escape from a dead vetted list while leaving the runbook that
  depends on it unchanged — trading a real incident capability for tidiness on a wallet holding
  real sats.
- **Keep both.** Leaves `br-s0e` specifying a four-case pin table, pin-bypass rules and four
  pin-case tests on money code, to govern a knob no deployment sets — and leaves a standing
  config able to suppress evacuation entirely.
- **Split into two named settings** (a reachability hint and a hard override). More mechanism than
  the problem needs: the daemon side has no user at all, so the honest split is "delete one, keep
  the other", not "keep both under better names".
