# br-i9r disposition: gateway await_tx_accepted draft — DROP (do not file), preserve as an evidence note

Decision date: 2026-07-23. Decision: **do not file upstream; retain the draft in-repo as a superseded evidence note.**

## Reasoning (re-read against the #8837 diagnosis)

The draft (dated 2026-07-20, before the #8837 diagnosis concluded) theorises that an
LNv2 receive SM is pinned in `Funding` by an unbounded `await_tx_accepted`, and that this
silently strands every payment through the operation.

Its evidence is entirely negative/inferential: 0 `decryption_key_share` requests in 4h,
0 `submit_transaction` in 7h, an SM stuck at `active=1 inactive=0`. From those absences it
INFERS a block at `await_tx_accepted` (which precedes the share request).

The #8837 diagnosis supplies a confirmed root cause that explains the SAME observations
without the await hole: `tpe::aggregate_dk_shares` panicked on a single decryption share and
**killed the client's `sm-executor`**. A dead executor advances no state machine — so no
shares are requested, nothing is submitted, and every SM appears frozen in place. That is a
strictly better explanation than the draft's, because it is observed (the panic) rather than
inferred from silence, and it accounts for the cross-federation-wide freeze, not just this op.

The draft's own "What I have NOT established" lists the funding txid and its fate as the
central unknown (#2). That was never resolved. Filing a causal claim on top of an unresolved
central unknown is exactly the failure mode of the #8834 episode
(`/tmp/issue8834-evidence-SUPERSEDED.md`), where offered evidence was later retracted.

## What is NOT dropped

Three code-level observations in the draft are real regardless of what triggered the stall,
and are worth keeping as hardening candidates (NOT filed now, no reproduction/txid to anchor
them):
- `await_decryption_shares` awaits `await_tx_accepted(txid)` with no timeout — an unbounded
  await is a latent liveness hazard.
- `await_receive`'s `ReceiveSMState::Funding => {}` bare arm is silent — even a diagnostic
  there would make a stalled receive observable.
- `relay_direct_swap`'s `operation_exists` short-circuit re-awaits a stalled op instead of
  re-driving it — a stalled prior attempt poisons every later attempt on the same contract.

These stay as notes on the retained draft. If a future stall reproduces WITH a captured
funding txid whose fate is known, revisit filing then.

## Action taken
- The draft is moved out of /tmp into `docs/superseded/gateway-await-tx-accepted-draft.md`
  with this decision prepended, so it survives and records the reason (mirrors the
  `issue8834-evidence-SUPERSEDED` pattern).
- No upstream issue filed.

---

## Original draft (2026-07-20, UNFILED — retained verbatim for the record)

> The analysis above supersedes this draft's causal claim. The draft is kept as-is because its
> three code-level observations (unbounded `await_tx_accepted`, the silent `Funding => {}` arm,
> the `operation_exists` re-await) remain valid hardening candidates independent of the #8837
> root cause.

# DRAFT — not filed. LNv2 gateway: a funding transaction that never receives a verdict pins the receive SM, silently stranding every payment through that operation

Status: draft for review. Evidence and inference are separated below; several links in the
chain are **inferred, not proven**, and are listed explicitly at the end.

## Summary

On an LNv2 gateway, `ReceiveStateMachine`'s `Funding` state calls
`await_tx_accepted(outpoint.txid)` **before** it requests decryption shares. That await has no
timeout and no liveness bound. If the funding transaction never receives a verdict — neither
accepted nor rejected — the receive SM is pinned in `Funding` indefinitely.

That single stall then propagates, silently, through three layers:

1. `await_receive` treats `ReceiveSMState::Funding` as a no-op and loops, so it never returns.
2. `relay_direct_swap` short-circuits to `await_receive` whenever `operation_exists`, so every
   subsequent attempt on the same contract blocks in the same place **and submits nothing**.
3. `send_payment` therefore never resolves, so `SendSMState::Sending` never transitions, and the
   sending client never receives a preimage.

The paying client eventually gives up at contract expiry and refunds. Nothing anywhere reports
an error — the gateway logs nothing at all on this path.

## Versions

- `fedimint/gatewayd:v0.11.1`, `fedimint/fedimintd:v0.11.1`, LND backend
- Gateway registered with 4 federations
- Two single-guardian federations, both served by this gateway: source over `wss://`,
  destination over `iroh://`
- Gateway at `RUST_LOG=debug,fm::client::net::api=trace`

## The code path (all coordinates at the `v0.11.1` tag)

`modules/fedimint-gwv2-client/src/receive_sm.rs` — `Funding` awaits shares, but only after
awaiting the transaction:

```rust
async fn await_decryption_shares(...) -> Result<BTreeMap<PeerId, DecryptionKeyShare>, String> {
    global_context.await_tx_accepted(outpoint.txid).await?;   // ← unbounded
    Ok(global_context.module_api().request_with_strategy_retry(
        FilterMapThreshold::new(...),
        DECRYPTION_KEY_SHARE_ENDPOINT.to_owned(),
        ApiRequestErased::new(outpoint),
    ).await)
}
```

`modules/fedimint-gwv2-client/src/lib.rs` — `await_receive` has no exit for `Funding`:

```rust
loop {
    if let Some(GatewayClientStateMachinesV2::Receive(state)) = stream.next().await {
        match state.state {
            ReceiveSMState::Funding => {}          // ← loops forever
            ReceiveSMState::Rejected(..) => return FinalReceiveState::Rejected,
            ...
        }
    }
}
```

…and `relay_direct_swap` routes every repeat attempt straight into it:

```rust
let operation_id = OperationId::from_encodable(&contract);
if self.client_ctx.operation_exists(operation_id).await {
    return Ok(self.await_receive(operation_id).await);     // ← submits nothing
}
```

`modules/fedimint-gwv2-client/src/send_sm.rs` — `Sending` is the only state with a transition,
and its future is `send_payment`, which contains the `relay_direct_swap` call above. If that
never resolves, the SM never leaves `Sending`.

## Observed

A cross-federation move (1,000,000 msat, source → destination, both served by this gateway).
The paying client funded its outgoing contract and then waited indefinitely.

**The gateway's send SM never transitions.** Operation `dc6a55b7_2c2a77fa`, 36 log lines over
6 hours, every one identical and every one inside a `pay_bolt11_invoice_v2` span:

```
DEBUG pay_bolt11_invoice_v2: fedimint_client_module::sm::notifier: Returning state transitions
from DB for notifier subscription operation_id=dc6a55b7_2c2a77fa module_instance=1
active=1 inactive=0
```

`active=1 inactive=0` never changes. A *new* notifier subscription appears roughly every 10
minutes, which indicates `pay_bolt11_invoice_v2` is being re-invoked rather than one call
blocking — consistent with the paying client re-driving on its own timer.

**No decryption shares are ever requested.** `decryption_key_share` appears **0** times in 4h
of gateway logs, at a level that traces every API request. Since that request is issued only
*after* `await_tx_accepted` resolves, this places the block at the await itself rather than at
the share collection.

**No transactions are submitted.** `Finalized and submitting transaction` — which is logged
once per newly built transaction — appears **0** times in 6h. The destination federation
recorded **0** `submit_transaction` calls in 7h. So the transaction whose acceptance is being
awaited was not submitted during the observation window.

**Downstream effect on the payer.** The paying federation's guardian shows 98
`await_preimage` polls in 2h — the client correctly waiting for a preimage that cannot arrive.
Two earlier attempts on the same route both terminated `send refunded` at contract expiry.

## Why it is invisible

Nothing on this path logs. `relay_direct_swap` contains no log statements at all;
`await_receive` contains none; the `Funding` arm is a bare `{}`. Combined with #5238
(`submit_transaction` outcomes not surfaced to the operation), a gateway in this state is
indistinguishable from an idle one.

## What I have NOT established

Listing these explicitly because the chain above is partly inferential:

1. **I have not read the gateway's client database.** I have not confirmed the receive SM's
   actual state, nor that it is the SM counted by `active=1`. `module_instance=1` is lnv2, but I
   did not verify which SM variant it is. The `Funding` conclusion is inferred from the absence
   of `decryption_key_share` requests, not observed directly.
2. **I have not identified the funding txid or its fate.** I do not know whether it was
   submitted and lost, submitted before my window, rejected without the rejection reaching the
   client, or never built. This is the central unknown, and it determines whether the defect is
   the unbounded await or something upstream of it.
3. **The ~10-minute re-invocation is attributed to the payer's tick by correlation only** (the
   payer's reconcile interval is 600s). I did not trace an inbound request to the gateway.
4. **Root cause of the missing verdict is unknown.** This report describes a liveness hole that
   converts that condition into a silent permanent stall; it does not explain the condition.
5. **Not reproduced deliberately.** This is one observed instance, not a recipe.

## Relationship to other issues

- **Not #8834.** That requires a funding transaction that is *submitted and rejected*, producing
  an N+2 cascade of double-spending refunds. Here the gateway submits nothing at all, and no
  rejection is observed. Different precondition, different signature.
- **#5238** is what makes this silent rather than actionable, as with #8834.

## Possible directions

- Bound the `await_tx_accepted` in `await_decryption_shares`, or make the receive SM able to
  fail out of `Funding` on a liveness deadline, so an unresolved transaction cannot pin it.
- `await_receive`'s `Funding => {}` arm silently converts a stalled receive into an infinite
  wait for its caller. Even a diagnostic there would make this observable.
- `relay_direct_swap`'s `operation_exists` short-circuit means a stalled operation poisons every
  future attempt on the same contract. Worth considering whether a stalled prior attempt should
  be re-driven rather than awaited.
