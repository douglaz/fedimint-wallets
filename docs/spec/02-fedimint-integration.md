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
one-guardian federation panics (`DEF-16`). The crates named directly are `fedimint-core`,
`fedimint-api-client`, `fedimint-client`, `fedimint-bip39`, `fedimint-rocksdb`,
`fedimint-connectors`, `fedimint-mint-client`, `fedimint-wallet-client`, `fedimint-ln-client`,
`fedimint-lnv2-client`, `fedimint-lnv2-common`; `lightning-invoice 0.33` and `bitcoin 0.32` are
pinned to the versions the fork's workspace uses so `Bolt11Invoice` and `bitcoin::Network` are
the same types. Transport configuration is `FMI-36`.

**FMI-2** Any repoint of the dependency — to upstream or to a newer fork commit — MUST carry
every fork-only patch, or the wallet regresses to the state-machine-executor freeze or loses
complete-or-fail recovery (`F19`). A cross-federation move smoke MUST complete against the
repointed build before it is adopted.

**FMI-3** The client registers four modules, in this order, on one `ClientBuilder`
(`client_builder`): lnv1 `fedimint_ln_client::LightningClientInit::default()`,
`fedimint_mint_client::MintClientInit`, `fedimint_wallet_client::WalletClientInit::default()`
(no bitcoin RPC configured; the wallet never pegs in or out, the module is registered so configs
decode and so `get_network` can be read for `FMI-5`), and
`fedimint_lnv2_client::LightningClientInit::default()`. The same builder serves preview, join,
open and recover. It does not register `meta`, so the meta module's consensus expiry value is
never read (`FMI-26`). No wallet code pays or receives through lnv1; it is registered so configs
decode and module recovery runs.

**FMI-4** A federation must have the `mint` and `wallet` modules to pass the scorer's structural
floor, and must have `lnv2` to be eligible at all: every money primitive resolves the lnv2 module
and errors without it, and `gateway_available` is false without it (`ALC-14`).

**FMI-5** `is_mainnet` is derived from the wallet module's network in the authenticated config,
`false` if there is no wallet module. The probe reads it through
`WalletClientModule::get_network() == bitcoin::Network::Bitcoin`; discovery reads it from the
previewed `ClientConfig` (`wallet_network`), decoding a `DynRawFallback::Raw` wallet config with
`WalletClientConfig::consensus_decode_whole` and a default decoder registry. `require_mainnet`
defaults to true in both the scorer and discovery policies and is a stored `Policy` field.

## Clients and partitions

**FMI-6** One fedimint client per federation, all live at once, addressed by id in a
`RwLock<BTreeMap>` with no await inside any critical section. Clients live in `client.db`; the
application journal lives in the separate `journal.db` (`STO-1`).

**FMI-7** One seed, many federations. "Same seed, same funds" is this exact derivation, all of
it inside the pinned SDK except the first line:

| Step | Derivation |
|---|---|
| mnemonic → seed | 12 BIP-39 words; `Mnemonic::to_seed_normalized("")` — the passphrase is **empty** |
| seed → root | `DerivableSecret::new_root(seed, b"Fedimint Client Salt")` = HKDF-SHA512 extract with that salt (`Bip39RootSecretStrategy::<12>::to_root_secret`) |
| root → wallet root | `RootSecret::StandardDoubleDerive(root)`; the builder calls `get_default_client_secret(root, federation_id)` on join, open and recover |
| wallet root → per-federation client secret | `root.child_key(ChildId(0)).federation_key(federation_id).child_key(ChildId(0)).child_key(ChildId(0))` — `child_key(c)` re-keys the HKDF with `derive_hmac` over the 16-byte info `b"childkey" ++ u64_be(c)`; `federation_key(id)` does the same over `federation_id[0..8] ++ u64_be(0)` and resets the derivation level to 0 |
| client secret → federation root secret | `ClientBuilder::build` applies `federation_key(config.global.calculate_federation_id())` **a second time** (`federation_root_secret`) — this second federation-id round is what `StandardDoubleDerive` names, and it resets the level to 0 |
| federation root secret → per-module secret | `.child_key(ChildId(0)).child_key(ChildId(module_instance_id))` (`derive_module_secret`, which asserts level 0 — applying it to the client secret directly would panic); the mint module's note keys hang off this |

The wallet passes no device index; the pinned `get_default_client_secret` has no such parameter.
The wallet MUST NOT pre-derive the per-federation secret (`RootSecret::Custom` is never used).
The mnemonic is stored as its 16-byte entropy under the SDK's `EncodedClientSecretKey` (client
db-prefix byte `0x28`) at the root of `client.db` (`STO-4`); `store_encodable_client_secret`
refuses to overwrite an existing value. `walletd serve` and every standalone `wallet-cli` verb
that builds a `MultiClient` run `load_or_generate_mnemonic` — `walletd init` (policy row and token
only) and the journal-only standalone verbs `history`, `show`, `candidates` and `approve` open
`client.db` without touching the seed: an absent secret
mints a fresh random 12-word mnemonic and persists it, a present one is reused, a
present-but-undecodable one aborts. Only `recover` (`load_existing_mnemonic`) and
`walletd mnemonic` refuse to mint (`FMI-33`). A fresh empty `client.db` therefore silently owns
a new seed on first use.

**FMI-8** `join(invite)`: fast-path if a client exists → take `join_lock` → re-check → refuse if
a recovery is reserved for this id → if a registry row exists, **open** it rather than re-join →
else preview the config under a 60-second bound (`FMI-21`) → allocate the next partition
(`STO-3`) → `preview.join(partition, root_secret)`, removing the partition best-effort on
failure → the joined id must equal the invite's → write the registry row → insert the handle.
In the daemon the last two steps run under an actor membership lease that bumps the world
generation so an in-flight tick plan is refused (`ALC-32`). The daemon and auto-join call
`join_before_deadline` with a caller budget: the lock wait, the preview and `preview.join` are
each bounded by the remaining budget, whichever of it and the 60-second preview bound is tighter
fires first, and an elapsed budget returns the distinct outcome `DeadlineElapsed` (not an
error); a budget that elapses inside `preview.join` removes the fresh partition best-effort
exactly as a failed join does.

**FMI-9** `open_all` at startup is best-effort per federation: a partition that fails to open is
warned and skipped, and that federation is registered-but-unopened (`DOM-2`). The scheduler
retries opening it every cycle and fences planning until it succeeds (`ALC-46`).

**FMI-20** Every open and every join of a federation client MUST be serialized under
`join_lock`; a registered federation is never double-opened and a live handle is never replaced
(`DEF-17`). Money operations never take `join_lock`. Before opening, `open_one_locked` derives
the id from the registry row's invite to skip a federation whose recovery is reserved or whose
client is already live; a row whose invite does not parse skips both pre-checks, opens the
partition, and if the id read from the opened client is already live shuts the fresh handle down
and returns the live one.

**FMI-21** The config preview under `join_lock` MUST be bounded (60 seconds) for both join and
recovery (`DEF-18`).

## Gateways

**FMI-10** The **vetted list** for a federation is the SDK's `list_gateways(None)` on that
federation's client — the guardians' lnv2 gateway registrations. What the SDK returns at the
pin (`LightningModuleApi::gateways`): it queries every guardian's `gateways` endpoint under
`FilterMapThreshold`, returning as soon as `NumPeers::threshold()` guardians have answered
(thresholding only the response **count**); takes the **union** of every URL any responder
returned; **random-shuffles** the union; then stable-sorts it by ascending number of responders
that did **not** list the URL. So a gateway named by more guardians sorts first, one named by a
single guardian sorts last but is still present, and equally-vetted gateways come back in a
fresh random order on every call. The wallet applies no threshold of its own and preserves that
order (`F6`, `SEC-17`; the decided target is a per-guardian read admitting only URLs at least
`NumPeers::threshold()` guardians return). Every "first" and every tie-break below inherits
this order. The devimint harness does not auto-register its gateway, so the list can be empty
while a usable gateway exists.

**FMI-11** Gateway validation is a direct `POST {gateway}/routing_info` on a pooled HTTP client
with a 5-second connect and 10-second total timeout, bypassing the SDK's gateway API because the
SDK's `HttpConnection::is_connected` is hard-coded false at the pin and adds 550–730 ms of
backoff per quote. Wire shape, identical to the SDK's `RealGatewayConnection::routing_info`: the
URL is `SafeUrl::parse(gateway).join("routing_info")`; the request body is the federation id
JSON-serialized as its 64-hex-character string; a `200` body is JSON `Option<RoutingInfo>` —
`null` or `{lightning_public_key, lightning_alias?, module_public_key, send_fee_minimum,
send_fee_default, expiration_delta_minimum, expiration_delta_default, receive_fee}` — the SDK's
`RoutingInfo`, decoded as that exact type: `module_public_key` is **required** (a body without
it is a decode error, reported like a dead gateway, not a `null`), `lightning_alias` is
`Option<String>` and omitted when absent; each fee is `PaymentFee {base: <msat integer>,
parts_per_million: <integer>}` (`Amount` serializes as a bare msat integer). The private `maybe_routing_info_for` is tri-state: `Some(routing_info)`
means the gateway serves that federation, `None` (a `200` with `null`) means it answered and
does not, and a transport failure, **any non-200 status** (an lnv1-only gateway's 404 included)
or a decode failure is an error. The public `validate_gateway(fed, gw)` collapses that to
`Result<()>`: "does not serve" and a transport fault both arrive as `Err`, so a caller cannot
tell them apart through it.

**FMI-12** Automated selection MUST choose the **cheapest** validated candidate (`DEF-5`).
The candidate set is one federation's vetted list (`FMI-10`), or the single break-glass gateway
when one is armed (`FMI-14`): for a raw pay, the **source** federation's list, keeping the
cheapest gateway whose gateway-plus-federation send quote fits the cap; for a raw receive, the
**destination**'s list, keeping the cheapest whose receive quote fits; for a planned move, the
cheapest `Routable` candidate by modelled fee (`ALC-13`); for a move whose amount is final, the
fallback resolver prices every gateway on the **destination**'s list at the amount within a
10-second budget and keeps the cheapest that fits. A candidate whose gateway or federation quote
errors is skipped silently. Ties keep the **first-seen** candidate
(`keep_cheapest_fitting` replaces the incumbent only on a strictly lower total), so among
equal-priced gateways the choice follows `FMI-10`'s shuffled order.

**FMI-13** `gateway_serves_route(to, from, gw)` checks `routing_info` liveness at each end and
**nothing else**. Vetted-list membership is not re-tested, so a pinned or hinted URL that is on
neither federation's list still passes, and a gateway the source federation has revoked can
still carry an automated move. `ADR-0029` defines **serves** with source-side membership as
the target; `CONTEXT.md` **Serves** points at the gap (`F6`).

**FMI-14** Gateway precedence for a move or evacuation: (1) the operator's break-glass, when
this invocation armed one for THIS intent's key, returned **unvalidated** (`ADR-0030`; the
daemon never arms one, and an uncommitted cached `MoveRecord` route yields to it while a
committed one replays — "committed" per `OPS-20`: a cache artifact or an op-log artifact, never
a draft); (2) the action's route hint, if it is still on the destination's vetted
list and still `gateway_serves_route`; (3)
if the amount is final, the fallback resolver (`FMI-12`) — and if it priced every candidate and
none fits the cap it returns `Retryable` **without** trying (4), so a tight cap keeps a move
`Pending` even when a validating gateway exists; only a budget-truncated or nothing-priced scan
falls through to (4) the first gateway on the **destination**'s vetted list, in `FMI-10`'s
order, that validates both ends; (5) none → `Retryable`, never `Permanent`, so the intent stays
`Pending` and a later run with a break-glass override can resume it. A fresh evacuation, whose
amount is not yet final, takes (4) directly. Probes and route economics consult (4)'s vetted
list only; the probe's `gateway_available` is true when any gateway on that list, scanned in the
same order, validates.

**FMI-15** A **direct inflow** is a receive-only move whose invoice is grossed up so the
destination is credited `amount` after gateway and federation receive fees — **never more, and
possibly less by a bounded shortfall**: the gross-up returns a verified never-over invoice and,
when the federation's step fee prevents an exact solution, the best verified under-netting
candidate, so the shortfall is bounded by one receive-fee step of that federation, not by a
fixed figure. On the tested route it is tens of msat (lnv2's own receive quote omits the
note-selection-dependent mint output fee) and the live gate tolerates 1,000 msat. The external payer pays the invoice amount. A raw **receive** invoices `amount` and the recipient nets `amount`
minus fees. The two are different verbs with different ledger semantics (`STO-15`).

## The Lightning legs

**FMI-16** `receive` is **not idempotent**: each call mints a fresh contract, invoice and
operation id. The wallet therefore persists the `(operation_id, invoice)` pair the moment the
call returns and finds an orphaned one by correlation key in `custom_meta` on resume (`OPS-18`).
The call is `LightningClientModule::receive(amount, 3600, Bolt11InvoiceDescription::Direct(""),
Some(gateway), custom_meta)`: invoice expiry is 3,600 seconds, the description is the **empty**
string, and the gateway is always supplied — no wallet call site passes `None`, so the SDK's
`select_gateway` auto-selection is never exercised. Inside the SDK the gateway's fresh
`routing_info` is fetched, `contract = receive_fee.subtract_from(amount)` (`FMI-18`), and the
call is refused with `ReceiveError` when the gateway does not serve the federation
(`FederationNotSupported`), when `receive_fee` exceeds the limit (`GatewayFeeExceedsLimit`,
`FMI-19`), or when the contract is below `MINIMUM_INCOMING_CONTRACT_AMOUNT` = 5,000 msat
(`AmountTooSmall`); the wallet pre-checks the minimum itself (`OPS-18`) and maps every
`receive` error to `Retryable`.

**FMI-17** `send` is deduplicated by the SDK's deterministic operation id
`OperationId::from_encodable(&(invoice, 0u64))`: one attempt per invoice. A second `send` of
the same invoice hits `operation_exists` and returns `DuplicatePaymentAttempt(op)` carrying the
original operation, which the wallet maps to `SendOutcome::AlreadyInFlight` and attaches to.
Re-calling `send` after a crash therefore cannot double-pay as long as the source client's
database survives; a seed recovery mid-send wipes that dedup and is the one real double-pay
hazard (`FMI-32`). The wallet always passes `Some(gateway)`. The SDK's own pre-fund checks and
the wallet's `map_send_result` classification:

| `SendPaymentError` variant | SDK condition | Wallet class |
|---|---|---|
| `DuplicatePaymentAttempt(op)` | op-id already in the op-log | `Ok(AlreadyInFlight(op))` — an outcome, not an error |
| `InvoiceMissingAmount` | invoice has no amount | `InvoiceRejected` |
| `InvoiceExpired` | `invoice.is_expired()` | `InvoiceRejected` |
| `WrongCurrency` | invoice currency ≠ the federation's network | `InvoiceRejected` |
| `FederationNotSupported` | `routing_info` returned `null` for the pinned gateway | `RouteRejected` |
| `GatewayFeeExceedsLimit` | `send_parameters` fee not `≤ SEND_FEE_LIMIT` (`FMI-19`) | `RouteRejected` |
| `GatewayExpirationExceedsLimit` | `expiration_delta > 1440` blocks | `RouteRejected` |
| `SelectGateway`, `FailedToConnectToGateway`, `FailedToRequestBlockCount`, `FailedToFundPayment` | transport, consensus-read or funding fault | `Transport` |

A non-`SendPaymentError` failure before the SDK call (invoice or gateway URL parse, missing
lnv2 module) is `Transport`. `InvoiceRejected` and `RouteRejected` are `Permanent` for the
intent, `Transport` is `Retryable` (`OPS-17`).

**FMI-18** Fee shapes. A gateway fee is `base + floor(amount × ppm / 1 000 000)` with the
multiplication saturating in `u64` before the division, byte-for-byte the SDK's
`PaymentFee::absolute_fee` (`GatewayFee::on`). The receive gateway fee comes from
`routing_info.receive_fee`; the send gateway fee for a concrete invoice from
`routing_info.send_parameters(invoice)`, which returns `(send_fee_minimum,
expiration_delta_minimum)` when `invoice.recover_payee_pub_key() == lightning_public_key` and
`(send_fee_default, expiration_delta_default)` otherwise. Before an invoice exists the executor
assumes direct swap (`send_fee_minimum`). Federation fees are `receive_fee_quote(contract)` and
`send_fee_quote(outgoing_contract)`, each read as `quote.total().get_bitcoin().msats`; the send
quote note-selects real inventory and can fail on insufficient balance. `gross_up(net,
gateway_fee, fed_fee)` solves the minimal invoice such that `contract = invoice − gw(invoice)`
and `contract − fed(contract) ≥ net` by doubling and bisection; it is `None` when
`ppm ≥ 1 000 000`. Because the federation fee is a step function of the contract, the executor
re-quotes up to three passes and verifies with `predicted_net`.

**FMI-19** The SDK's gateway-fee limits ARE enforced at the pin, but on the wrong ordering.
`send` refuses `GatewayFeeExceedsLimit` unless `send_fee.le(&SEND_FEE_LIMIT)` where
`SEND_FEE_LIMIT = {base: 100 sat, ppm: 15 000}`; `receive` refuses unless
`receive_fee.le(&RECEIVE_FEE_LIMIT)` where `RECEIVE_FEE_LIMIT = {base: 50 sat, ppm: 5 000}`.
`PaymentFee` derives `PartialOrd`, so `le` is **lexicographic on `(base, parts_per_million)`**,
not a bound on the fee charged: a gateway whose base exceeds the limit's base is refused
whatever its ppm, a gateway whose base is below it is accepted with **any** ppm, and only a
base exactly equal to the limit's has its ppm compared. The intended envelopes (1.5 % send,
0.5 % receive) are therefore not what binds. The wallet maps the send refusal to
`RouteRejected` (`FMI-17`) and the receive refusal to `Retryable`, and separately warns above
15 000 / 5 000 ppm. `docs/fedimint-mechanics.md`'s "hard fee cap" cites this rule. The wallet's
own caps (`OPS-29`) are the only fee bounds that bind on the amount.

**FMI-22** Timeouts as built:

| Boundary | Bound |
|---|---|
| config preview under `join_lock` | 60 s |
| discovery config preview (`preview_config`, no lock) | the watch policy's `per_preview_timeout_ms`, clipped to the pass budget (`ALC-28`); not under the 60 s bound |
| auto-join | the remaining discovery pass budget, as `join_before_deadline` (`FMI-8`) |
| `routing_info` HTTP | 5 s connect / 10 s total |
| invoice expiry | 3,600 s |
| per-intent perform (daemon) | `WALLETD_PERFORM_TIMEOUT_SECS`, default 600 s, `0` disables; never applied to join or recover |
| daemon receive-invoice wait | 30 s |
| daemon long-poll | 60 s |
| fallback route scan | 10 s |
| route pricing per tick | 10 s / 24 calls / 4 gateways per pair |
| Observer HTTP | 20 s, 1 MiB body |
| module recovery | unbounded (`FMI-30`) |
| `wait_for_all_active_state_machines` after a recovery reopen | unbounded |

**FMI-23** A gateway that quotes but does not perform produces one of three outcomes, none of
which the wallet retries through another gateway: an invoice minted and never funded expires
after 3,600 seconds (a direct inflow stays `Awaiting` until then); a send funded and never
completed is refunded by the SDK's send state machine on gateway forfeit or expiry, and the move
terminalizes `Refunded`; a send that succeeds while the receive reaches a terminal non-claim is
`Stranded` (`OPS-27`). "Forfeit ⇒ `Refunded`" is the normal case, not a guarantee: in the SDK's
`Refunding` branch, if the refund outputs are **rejected** (the gateway incorrectly claimed the
outgoing contract), the state machine re-reads `await_preimage(outpoint)` one last time and, if
a preimage verifying against the contract is there, yields `Success(preimage)`; a forfeited
send can therefore be promoted to a settled send and, meeting a non-claimed receive, land
`Stranded`. If neither the refund nor a preimage is available the send yields `Failure`
(`FMI-37`). The perform-level record of gateway misbehaviour that `ADR-0029`'s **serves**
definition (performs clause) requires does not exist.

**FMI-37** What an lnv2 terminal `Failure` proves, and what it does not. The SDK folds two
distinct outcomes into each `Failure` terminal and the wallet cannot tell them apart from the
operation state:

| Leg | `Failure` is reached when | Money position |
|---|---|---|
| send | (a) `SendSMState::Rejected` — the **funding** transaction was rejected, nothing was funded; or (b) `Refunding` and the refund outputs were rejected **and** no verifying preimage was available (`FMI-23`) | (a) nothing moved; (b) the outgoing contract WAS funded and its position is unresolved |
| receive | `ReceiveSMState::Claiming` and `await_primary_module_outputs` on the claim's outputs failed — either the **claim** transaction was rejected (this wallet claimed nothing, which does not prove the contract is unclaimed) or it was accepted and mint note issuance then failed | unknown whether the incoming contract was consumed |

The wallet records the send case as `SendState::Failed(SEND_FAILURE_DETAIL)` and the receive
case as `ReceiveState::Failed(RECEIVE_FAILURE_DETAIL)`; the two detail strings begin
`send failed:` and `receive failed:`, are defined at one site each (each constant is written at
two sites — `map_send_state`/`send_terminal`, `map_receive_state`/`receive_terminal` — that
share it so they cannot drift), and are the runbook's grep
anchors. A move whose error starts `send failed:` is NOT evidence the money stayed put
(`OPS-27`; `docs/real-sats-pilot-runbook.md` "ambiguous send or receive terminal"). `Expired`
on a receive and `Refunded` on a send are the only terminals that establish the funds' position.

**FMI-38** Nothing inside the SDK's await path times out, so a long-poll can stall for as long
as the transport lets it. One incident on the test deployment (a cross-federation move's
receive-claim await, 2026-07-19/20, `docs/real-sats-pilot-runbook.md` §3b "Prefer
WSS-transport federations") hung until the
daemon's per-intent perform timeout (`FMI-22`) re-drove the intent with a fresh await; it was
attributed to the iroh transport by the operator and has not been reproduced or isolated, so
"iroh stalls" is a working hypothesis, not a measured fact. Two bounds exist and they do
different things: the **perform** deadline (`FMI-22`; standalone `--perform-timeout`, default
600 s, `0` disables, `HST-9`) returns `Retryable` and resets an `Executing` intent to `Pending`
for the next reconcile; an await verb's `--timeout` (default 600 s, `API-38`) only makes
`resolve_await` return `Timeout` (exit 4) and leaves the row `Awaiting` — no journal transition,
nothing for reconcile to re-drive. The fork's long-poll patch (`FMI-1`) does not
remove this. The runbook therefore prefers WebSocket-transport federations
(`docs/real-sats-pilot-runbook.md` §3b "Prefer WSS-transport federations").

## Recovery

**FMI-30** Recovery is an explicit verb (`POST /v1/recover`, `wallet-cli recover`) with
**complete-or-fail** semantics as far as the pinned SDK reports: any error before the final
commit leaves the fresh partition unregistered, and a retry allocates the next prefix. The
executor maps every recovery error to `Permanent`. A crash mid-recovery leaves the intent
`Executing`; reconcile re-drives it into a clean fresh prefix or hits the refuse-if-registered
guard (`DEF-15`). Two limits on "fail": (1) the fork patch only makes a module recovery that
**returns** an error reportable; the registered modules recover by replaying session history,
and that fetch loop retries every transport error forever (randomized backoff capped near two
minutes) and panics on a missing session rather than returning — the only non-retried escape
is one `session_count` consensus call at the start of each module's recovery. A recovery
against an unreachable federation therefore **hangs**, unbounded (`FMI-22`), rather than
fails; no live gate exercises the failure path (`F24`,
`docs/recovery-failure-gate-analysis.md`). (2) "Unregistered" is not "empty": a failure after
`wait_for_all_recoveries` returned — the reopen, the state-machine drain, or the final
`complete_recovery` compare-and-swap returning "superseded" — abandons a partition that already
holds the seed's recovered notes. It is never opened because no registry row names it
(`FMI-35`), and the retry recovers the same notes into the next prefix.

**FMI-31** The sequence: refuse if a registry row exists, open **or** unopened → reserve the id
in a process-local set that blocks `join`/`open` for it → preview under the 60-second bound →
allocate a fresh partition → `preview.recover(partition, root_secret, None)` — the third
argument is `Option<ClientBackup>` and is always `None`: recovery runs from the seed with no
backup snapshot → drop the lock and
block on `wait_for_all_recoveries`, the sole completion authority (a side task logs
`subscribe_to_recovery_progress` and is never a completion signal) → recovered id must equal
the invite's → shut the recovery-phase handle down, **reopen** the partition through the normal
open path (the recovery-phase handle omits recovered modules), drain active state machines with
`wait_for_all_active_state_machines` → re-take
the lock, re-check → one journal transaction writes the registry row, terminalizes the intent
`Done`, and writes a `UserApproved` candidate (`STO-14`) → insert the handle with no await
between.

**FMI-32** What is recovered is what the SDK's module recovery rebuilds from the seed by
history replay: ecash and module state. The wallet never calls `backup_to_federation` or
`download_backup_from_federation`, so no encrypted federation backup exists to shorten the
replay or to carry anything else. **Not** recovered: the operation log (the cross-restart
send-dedup authority), the journal and ledger, the federation list, in-flight moves. This is
why a registered federation is refused: a surviving journal with a non-terminal pay plus an
empty op-log is a double-pay (`ADR-0025`, `F12`).

**FMI-33** Recovery is never automatic and never a side effect of a join (`DEF-15`). The
partition rule is never-wipe: `preview.recover` rejects an initialized database, so in-place
recovery would need a wipe with a crash window; orphaned partitions accumulate and no garbage
collection command exists. `recover` is the one verb that refuses a store with no seed
(`FMI-7`), because minting one there would rebuild an empty wallet under a bogus root and
occupy the slot a later `restore-mnemonic` needs.

## Signals a federation emits, and what the wallet does with them

**FMI-24** From the authenticated `ClientConfig`: `guardian_count = api_endpoints.len()`,
`threshold` = the SDK's `NumPeers::threshold()`, which is `n − floor((n−1)/3)` in integer arithmetic (equal to `2f+1` only
when `n ≡ 1 mod 3`; the `probe.rs` comment saying `2f+1` is stale), module kinds (the `kind`
string of every entry in `config.modules`, in module-instance-id order), `has_lnv2`,
wallet-module presence, `is_mainnet`. The scorer rejects a threshold below the BFT bound
(`ALC-14`). The derivation exists twice: discovery's `threshold_for_endpoints` returns `0` for
zero endpoints, the probe's inline `NumPeers::from(n).threshold()` does not guard zero (it
underflows in `max_evil`); an authenticated config always has at least one endpoint, so the
difference is latent.

**FMI-25** Liveness is one `session_count` threshold read: success is `quorum_live`, wall-clock
is `latency_ms`. A federation whose light probe **errors** is dropped from the snapshot with a
warning and is therefore neither scored nor evacuated that tick (`ALC-48`).

**FMI-26** Shutdown is derived from three signals with a hard-coded 24-hour lead
(`SHUTDOWN_EVACUATION_LEAD_SECS`): the merged meta `federation_expiry_timestamp` (overridable
by the federation's `meta_override_url`, **untrusted**, never schedules alone, warns if
uncorroborated); the at-join consensus config meta `federation_expiry_timestamp`; and per-peer
`/status` `scheduled_shutdown`, corroborated when `f+1 = (n−1)/3 + 1` peers report it. The
meta-module consensus value is modelled and always `None` (`FMI-3`). `ADR-0019`. The pure
`derive_shutdown_scheduled(override_expiry, config_expiry, meta_module_expiry,
status_scheduled, now, lead)` combines them: `status_scheduled` alone schedules; else any of
`config_expiry`/`meta_module_expiry` within the lead schedules; `override_expiry` is read and
**never consulted** — an override-only expiry never schedules. Mechanics:

- The config value is `config.global.meta["federation_expiry_timestamp"]`, parsed by
  `parse_meta_expiry_secs` as unix seconds from a trimmed plain decimal, a JSON number, or a
  JSON-quoted decimal; anything else is absent.
- A corroborated expiry schedules when `now + 86 400 ≥ expiry`, which includes an expiry
  already in the past.
- The `/status` signal is a sequential scan over `api.all_peers()` with
  `request_single_peer_federation::<StatusResponse>(STATUS_ENDPOINT)`; a peer counts when
  `federation.scheduled_shutdown.is_some()`, whatever session index it names; a transport or
  decode error counts as no signal and is warned; the scan stops at `f+1` reports. Fewer than
  `f+1` reports warn and do not schedule.
- The runtime also subscribes each open client's `meta_service().subscribe_to_field::<u64>(db,
  "federation_expiry_timestamp")` (`spawn_expiry_wake_tasks`): the first stream item is
  discarded, later values × 1000 become wake deadlines (`ALC-19`); this is a wake signal only,
  never a trigger.

Debug builds also honour `WALLET_CLI_FORCE_SHUTDOWN` (`SEC-18`). The runtime-mutable
`Policy.evacuation_lead_secs` (default one hour) governs only when the scheduler wakes, not
when an evacuation triggers (`ALC-19`).

**FMI-27** Balances: `spendable` from `get_balance_for_btc`; `claimable` is always zero by
design; `in_flight` is `pending_lnv2_balances`: page the op-log newest-first (100 per page, to
exhaustion), keep entries whose `operation_module_kind()` is `lnv2` and whose
`entry.outcome::<serde_json::Value>()` is **absent** (no cached terminal), and for each
`LightningOperationMeta::Send` add the **invoice** amount in msat (an amountless invoice adds
0); `Receive` and `LnurlReceive` add nothing. Because an outcome is cached only once an update
stream reaches a terminal, a freshly reopened client can over-report `in_flight` until it
re-subscribes; the value is advisory and not read by `decide`.

**FMI-28** The Fedimint Observer is a discovery source only: `GET {base}/federations` (the base
with trailing slashes trimmed) on a client with a 20-second total timeout; a non-2xx status, a
`Content-Length` above 1 MiB, or a streamed body exceeding 1 MiB is a source failure. Parsing
(`parse_observer_federations`): an empty body is a healthy empty result; the body must be a
JSON array or an object with a `federations` array, else a source failure; each row needs
string `id` and `invite` — `id` parsed as a 64-hex federation id, `invite` as an `InviteCode` —
with optional string `network` kept as a hint; a row that fails any of these is skipped
silently. A candidate is dropped unless its claimed `id`, the invite's embedded id and the
previewed config's `calculate_federation_id()` all agree. Every candidate's config is
re-fetched by preview before it is scored; nothing the Observer says is load-bearing
(`ADR-0020`).

**FMI-29** Nostr is an enum variant and a label. No Nostr source is implemented.

**FMI-36** Transport. `MultiClient::new` binds one `ConnectorRegistry::build_from_client_defaults()`
shared by preview, join, open and recover: iroh enabled (default iroh DNS, no pkarr DHT),
WebSocket enabled (not forced over Tor), HTTP enabled. `build_from_client_env` is not used, so the
SDK's `FM_*` connector environment overrides are ignored. Which transport a federation is
reached over is decided by its invite code and config, not by the wallet; see `FMI-38` for the
iroh stall.

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
