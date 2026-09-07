# 02 — Fedimint integration

The boundary between this wallet and the fedimint SDK, as built in
`wallet-fedimint/src/{multi_client,fee,route_econ,probe,discovery}.rs` and the gateway and
recovery paths of `executor.rs`. Facts about the SDK's own behaviour at the pin are stated only
where the wallet's code depends on them and marked where they were read from the wallet rather
than the SDK.

## The dependency

**FMI-1** Every fedimint crate is pinned to one git source and revision: `douglaz/fedimint` at
`72b1e5beadc5a31a33ebc751764cb2f840a63b5e`, which the lockfile resolves as `0.12.0-alpha`. The
branch carries three patches over upstream: the iroh long-poll transport, recovery
complete-or-fail (`wait_for_all_recoveries` returns an error on a failed module recovery instead
of parking), and the single-share threshold-decryption fix without which lnv2 decryption on a
one-guardian federation panics (`DEF-16`).

**FMI-2** Any repoint of the dependency — to upstream or to a newer fork commit — MUST carry
every fork-only patch, or the wallet regresses to the state-machine-executor freeze or loses
complete-or-fail recovery (`F19`). A cross-federation move smoke MUST complete against the
repointed build before it is adopted.

**FMI-3** The client registers four modules: lnv1 `ln`, `mint`, `wallet`, `lnv2`. It does not
register `meta`, so the meta module's consensus expiry value is never read (`FMI-26`). No wallet
code pays or receives through lnv1; it is registered so configs decode and module recovery runs.

**FMI-4** A federation must have the `mint` and `wallet` modules to pass the scorer's structural
floor, and must have `lnv2` to be eligible at all: every money primitive resolves the lnv2 module
and errors without it, and `gateway_available` is false without it (`ALC-14`).

**FMI-5** `is_mainnet` is derived from the wallet module's network in the authenticated config,
`false` if there is no wallet module. `require_mainnet` defaults to true in both the scorer and
discovery policies and is a stored `Policy` field.

## Clients and partitions

**FMI-6** One fedimint client per federation, all live at once, addressed by id in a
`RwLock<BTreeMap>` with no await inside any critical section. Clients live in `client.db`; the
application journal lives in the separate `journal.db` (`STO-1`).

**FMI-7** One seed, many federations. The root secret is
`RootSecret::StandardDoubleDerive(Bip39RootSecretStrategy::<12>::to_root_secret(mnemonic))`;
per-federation derivation happens inside the SDK builder on join and open. The wallet passes no
device index. The mnemonic is stored in the SDK's client-secret slot (`STO-4`).

**FMI-8** `join(invite)`: fast-path if a client exists → take `join_lock` → re-check → refuse if
a recovery is reserved for this id → if a registry row exists, **open** it rather than re-join →
else preview the config under a 60-second bound (`FMI-21`) → allocate the next partition
(`STO-3`) → `preview.join(partition, root_secret)`, removing the partition best-effort on
failure → the joined id must equal the invite's → write the registry row → insert the handle.
In the daemon the last two steps run under an actor membership lease that bumps the world
generation so an in-flight tick plan is refused (`ALC-32`).

**FMI-9** `open_all` at startup is best-effort per federation: a partition that fails to open is
warned and skipped, and that federation is registered-but-unopened (`DOM-2`). The scheduler
retries opening it every cycle and fences planning until it succeeds (`ALC-46`).

**FMI-20** Every open and every join of a federation client MUST be serialized under
`join_lock`; a registered federation is never double-opened and a live handle is never replaced
(`DEF-17`). Money operations never take `join_lock`.

**FMI-21** The config preview under `join_lock` MUST be bounded (60 seconds) for both join and
recovery (`DEF-18`).

## Gateways

**FMI-10** The **vetted list** for a federation is the SDK's `list_gateways` on that federation's
client — the guardians' lnv2 gateway registrations. The SDK returns a **union** of what each
responding guardian returned after thresholding only the response count, so one guardian can
place a gateway in the list; the wallet applies no threshold of its own (`F6`, `SEC-17`). The
devimint harness does not auto-register its gateway, so the list can be empty while a usable
gateway exists.

**FMI-11** `validate_gateway(fed, gw)` is a direct `POST {gateway}/routing_info` with the
federation id as body, on a pooled HTTP client with a 5-second connect and 10-second total
timeout, bypassing the SDK's gateway API because the SDK's connection check is hard-coded false
at the pin and adds 550–730 ms of backoff per quote. `Some(routing_info)` means the gateway
serves that federation; `None` means it answered and does not; a transport, HTTP or decode
failure is an error.

**FMI-12** Automated selection MUST choose the **cheapest** validated candidate (`DEF-5`): for a
raw pay, the cheapest gateway whose gateway-plus-federation send quote fits the cap; for a raw
receive, the cheapest whose receive quote fits; for a planned move, the cheapest `Routable`
candidate by modelled fee (`ALC-13`); for a move whose amount is final, the fallback resolver
prices every registered gateway at the amount within a 10-second budget and keeps the cheapest
that fits.

**FMI-13** `gateway_serves_route(to, from, gw)` checks `routing_info` liveness at each end and
**nothing else**. Vetted-list membership is not re-tested, so a pinned or hinted URL that is on
neither federation's list still passes, and a gateway the source federation has revoked can
still carry an automated move. `CONTEXT.md`'s **Serves** entry says this is intent, not
behaviour (`F6`).

**FMI-14** Gateway precedence for a move or evacuation: (1) the daemon's configured pin,
returned **unvalidated**; (2) the action's route hint, if it still `gateway_serves_route`; (3)
if the amount is final, the fallback resolver (`FMI-12`); (4) the first registered gateway that
validates both ends; (5) none → `Retryable`, never `Permanent`, so the intent stays `Pending` and
a later run with a break-glass override can resume it. A fresh evacuation, whose amount is not
yet final, takes (4) directly. `ADR-0030`'s rule that automated routing is never pinned is
target state (`F4`).

**FMI-15** A **direct inflow** is a receive-only move whose invoice is grossed up so the
destination is credited exactly `amount` after gateway and federation receive fees; the external
payer pays the invoice amount. A raw **receive** invoices `amount` and the recipient nets `amount`
minus fees. The two are different verbs with different ledger semantics (`STO-15`).

## The Lightning legs

**FMI-16** `receive` is **not idempotent**: each call mints a fresh contract, invoice and
operation id. The wallet therefore persists the `(operation_id, invoice)` pair the moment the
call returns and finds an orphaned one by correlation key in `custom_meta` on resume (`OPS-18`).
Invoice expiry is 3,600 seconds.

**FMI-17** `send` is deduplicated by the SDK's deterministic operation id: one attempt per
invoice. A second `send` of the same invoice returns the original operation, which the wallet
maps to "already in flight" and attaches to. Re-calling `send` after a crash therefore cannot
double-pay as long as the source client's database survives; a seed recovery mid-send wipes that
dedup and is the one real double-pay hazard (`FMI-32`).

**FMI-18** Fee shapes. A gateway fee is `base + floor(amount × ppm / 1 000 000)`, saturating,
byte-for-byte the SDK's. The receive gateway fee comes from `routing_info.receive_fee`; the send
gateway fee for a concrete invoice from `routing_info.send_parameters(invoice)`, which the SDK
resolves to the direct-swap minimum when the invoice's payee is the gateway itself. Before an
invoice exists the executor assumes direct swap. Federation fees are `receive_fee_quote(contract)`
and `send_fee_quote(outgoing_contract)`; the send quote note-selects real inventory and can fail
on insufficient balance. `gross_up(net, gateway_fee, fed_fee)` solves the minimal invoice such
that `contract = invoice − gw(invoice)` and `contract − fed(contract) ≥ net` by doubling and
bisection; it is `None` when `ppm ≥ 1 000 000`. Because the federation fee is a step function of
the contract, the executor re-quotes up to three passes and verifies with `predicted_net`.

**FMI-19** The SDK's intended fee envelopes (1.5% send, 0.5% receive) are **not** enforced at the
pin (a lexicographic comparison on the fee type). The wallet only warns above 15 000 / 5 000 ppm.
`docs/fedimint-mechanics.md`'s "hard fee cap" describes what the SDK meant, not what it does.
The wallet's own caps (`OPS-29`) are the only fee bounds that bind.

**FMI-22** Timeouts as built:

| Boundary | Bound |
|---|---|
| config preview under `join_lock` | 60 s |
| `routing_info` HTTP | 5 s connect / 10 s total |
| invoice expiry | 3,600 s |
| per-intent perform (daemon) | `WALLETD_PERFORM_TIMEOUT_SECS`, default 600 s, `0` disables; never applied to join or recover |
| daemon receive-invoice wait | 30 s |
| daemon long-poll | 60 s |
| fallback route scan | 10 s |
| route pricing per tick | 10 s / 24 calls / 4 gateways per pair |
| Observer HTTP | 20 s, 1 MiB body |

**FMI-23** A gateway that quotes but does not perform produces one of three outcomes, none of
which the wallet retries through another gateway: an invoice minted and never funded expires
after 3,600 seconds (a direct inflow stays `Awaiting` until then); a send funded and never
completed is refunded by the SDK's send state machine on gateway forfeit or expiry, and the move
terminalizes `Refunded`; a send that succeeds while the receive reaches a terminal non-claim is
`Stranded` (`OPS-27`). The perform-level record of gateway misbehaviour that `CONTEXT.md`'s
**Serves** entry calls for does not exist.

## Recovery

**FMI-30** Recovery is an explicit verb (`POST /v1/recover`, `wallet-cli recover`) with
**complete-or-fail** semantics: any error before the final commit leaves the fresh partition
inert and unregistered, and a retry allocates the next prefix. The executor maps every recovery
error to `Permanent`. A crash mid-recovery leaves the intent `Executing`; reconcile re-drives it
into a clean fresh prefix or hits the refuse-if-registered guard (`DEF-15`).

**FMI-31** The sequence: refuse if a registry row exists, open **or** unopened → reserve the id
in a process-local set that blocks `join`/`open` for it → preview under the 60-second bound →
allocate a fresh partition → `preview.recover(partition, root_secret, None)` → drop the lock and
block on `wait_for_all_recoveries`, the sole completion authority → recovered id must equal the
invite's → shut the recovery-phase handle down, **reopen** the partition through the normal open
path (the recovery-phase handle omits recovered modules), drain active state machines → re-take
the lock, re-check → one journal transaction writes the registry row, terminalizes the intent
`Done`, and writes a `UserApproved` candidate (`STO-14`) → insert the handle with no await
between.

**FMI-32** What is recovered is what the SDK's module recovery rebuilds from the seed: ecash and
module state. **Not** recovered: the operation log (the cross-restart send-dedup authority), the
journal and ledger, the federation list, in-flight moves. This is why a registered federation is
refused: a surviving journal with a non-terminal pay plus an empty op-log is a double-pay
(`ADR-0025`, `F12`).

**FMI-33** Recovery is never automatic and never a side effect of a join (`DEF-15`). The
partition rule is never-wipe: `preview.recover` rejects an initialized database, so in-place
recovery would need a wipe with a crash window; orphaned partitions accumulate and no garbage
collection command exists.

## Signals a federation emits, and what the wallet does with them

**FMI-24** From the authenticated `ClientConfig`: `guardian_count = api_endpoints.len()`,
`threshold` = the SDK's `2f+1` for that count, module kinds, `has_lnv2`, wallet-module presence,
`is_mainnet`. The scorer rejects a threshold below the BFT bound (`ALC-14`).

**FMI-25** Liveness is one `session_count` threshold read: success is `quorum_live`, wall-clock
is `latency_ms`. A federation whose light probe **errors** is dropped from the snapshot with a
warning and is therefore neither scored nor evacuated that tick (`ALC-48`).

**FMI-26** Shutdown is derived from three signals with a hard-coded 24-hour lead
(`SHUTDOWN_EVACUATION_LEAD_SECS`): the merged meta `federation_expiry_timestamp` (overridable
by the federation's `meta_override_url`, **untrusted**, never schedules alone, warns if
uncorroborated); the at-join consensus config meta `federation_expiry_timestamp`; and per-peer
`/status` `scheduled_shutdown`, corroborated when `f+1 = (n−1)/3 + 1` peers report it. The
meta-module consensus value is modelled and always `None` (`FMI-3`). `ADR-0019`. Debug builds
also honour `WALLET_CLI_FORCE_SHUTDOWN` (`SEC-18`). The runtime-mutable
`Policy.evacuation_lead_secs` (default one hour) governs only when the scheduler wakes, not
when an evacuation triggers (`ALC-19`).

**FMI-27** Balances: `spendable` from `get_balance_for_btc`; `in_flight` is the sum of unsettled
lnv2 send invoice amounts from the local op-log; `claimable` is always zero by design.

**FMI-28** The Fedimint Observer is a discovery source only: `GET {base}/federations`, reading
`id`, `invite` and optional `network` per row. Every candidate's config is re-fetched by preview
before it is scored; nothing the Observer says is load-bearing (`ADR-0020`).

**FMI-29** Nostr is an enum variant and a label. No Nostr source is implemented.

## The active probe at the SDK level

**FMI-34** An active probe is two real `Move` intents through the ordinary executor: leg **in**
mints `probe_amount` (default 20 sats) on the candidate paid by an lnv2 send from the source;
leg **out** redeems an affordably sized delta back, with the leg fee cap `max_fee` (default 10
sats per leg in the standalone verb). The candidate's baseline balance is sampled before leg in
so the exact delta can be isolated, and a no-sweep guard requires the candidate to hold exactly
`baseline + delivered_in` before leg out is journaled (`ALC-27`).

**FMI-35** Orphaned client partitions — from a failed join, a failed recovery, or a lost registry
— are never reused and never collected. The reclaiming command `ADR-0025` describes does not
exist.
