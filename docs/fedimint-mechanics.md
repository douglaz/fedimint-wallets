# How Fedimint actually works (for this wallet)

Grounded in a read of `~/p/fedimint` (branch `docs/custodial-receive-spec`), the harbor
wallet (`~/p/fedimint-wallets/harbor`, fedimint-client 0.7.1), and the Fedi app
(`~/p/fedi`, the production mobile multi-fed wallet). File:line cites are to those trees.
This supersedes the over-specified "Move state machine" framing in an earlier draft of
ADR-0022 — see "What this means for us".

## 1. Client topology: one client per federation, all live at once
- A `fedimint_client::Client` is **per-federation** — one `federation_id`, one config, one
  DB (`fedimint-client/src/client/builder.rs:1024`). A two-federation move uses **two**
  clients over two DBs. There is no cross-federation client object anywhere in fedimint.
- Both reference wallets hold a `Map<FederationId, Client>`, all clients live, addressed by
  id — no "active" client (harbor `lib.rs:278`; Fedi `crates/federations/src/lib.rs:30`).
- **One seed, many feds:** the per-fed client secret is derived deterministically —
  `get_default_client_secret(root_secret, federation_id, device_index)` (harbor
  `fedimint_client.rs:110`, Fedi `federation_v2/mod.rs:636`). `device_index` exists so two
  devices on the same seed don't reuse note-derivation indices (double-spend risk); single
  device → index 0.
- **Storage (lift Fedi's shape):** one global DB (RocksDB on Android) with per-federation
  **key prefixes** — prefix 0 = app state, 1.. = each fed (`runtime/src/storage.rs:31-35`).
  One fsync domain, cheap on mobile. (Harbor instead serializes each fed's KV into one
  SQLite BLOB rewritten per commit — simpler, but full-state write amplification.)

## 2. The idempotency model: the client DB IS the write-ahead log
- Each client's DB is the WAL. The state-machine executor **auto-resumes every active
  state machine on boot** — `run_state_machines_executor_inner` reloads all persisted
  active states and re-drives them (`fedimint-client/src/sm/executor.rs:581`). **We never
  re-implement or resume an in-federation operation; the client does.**
- `OperationId` is **deterministic and caller-supplyable** (`fedimint-core/src/core.rs:57`).
  Money ops content-address it: send = `from_encodable((invoice, attempt))`
  (`lnv2-client/lib.rs:679`), receive = `from_encodable(contract)` (`lib.rs:996`), mint
  reissue = `sha256t(notes)`.
- **Exactly-once = client-local op-id dedup + atomic commit + federation as dedup authority.**
  Starting an op writes the state machine *and* the oplog row in one transaction; the
  client re-submits the same tx forever; the federation's consensus enforces
  ecash-nonce/input uniqueness, so a duplicate submission is a no-op and a double-spend is
  rejected (`transaction/sm.rs:196-228`). The client guarantees the *same* submission.
- **Re-attach after a crash** by `subscribe(operation_id)` — it replays persisted states
  from the DB then continues (`sm/notifier.rs:54`). Keyed purely by op id.

## 3. Receive (the destination/B side)
- `receive(amount, expiry, desc, gateway?, meta) -> (Bolt11Invoice, OperationId)`
  (`lnv2-client/lib.rs:804`). The gateway mints the invoice and funds the incoming
  contract; the 3-state SM (`Pending -> Claiming | Expired`) **claims the ecash
  automatically** — the app calls nothing, just awaits.
- **Generating the invoice is NOT idempotent** — the contract (hence invoice, hence op id)
  is fresh random each call (`tweak.rs:6`). So **persist `(op_id, invoice)` the moment
  `receive()` returns.** On restart the client self-resumes the claim; we re-attach via op
  id and read the invoice back from op meta.
- Danger window: a crash after the gateway mints the invoice but before the client commits
  the receive op leaves an orphan invoice (no sweeper on the manual path). Bounded to one
  move's amount.

## 4. Send (the source/A side) — idempotency is client-local
- `send(invoice, gateway?, meta) -> OperationId` (`lnv2-client/lib.rs:538`). Hard fee cap
  100 sat + 1.5% (`SEND_FEE_LIMIT`); the SM self-refunds on gateway forfeit or expiry.
- **The federation does NOT dedup by payment hash** (outgoing contracts keyed by funding
  outpoint, fresh keys per contract — `lnv2-server/lib.rs:552`). But the **client does**:
  the deterministic op id `from_encodable((invoice, attempt=0))` + an `operation_exists`
  check returns `PaymentInProgress` / `InvoiceAlreadyPaid` (`lib.rs:679-701`). The gateway
  adds a second dedup on the contract.
- **So re-calling `send(invoice)` after a crash cannot double-pay, as long as A's client DB
  survives** (it's persistent + self-resuming). We don't strictly need to persist
  `send_op_id` for safety — A's DB is the dedup. The **one** dangerous case: restore from
  seed mid-send wipes the oplog (recovery restores ecash, not the operation log), so the
  send-dedup is gone and a re-send could double-pay. Bounded, but it's the real hazard.

## 5. The cross-federation move
**Value can only cross between two mints via a bridge both understand: Lightning (a
gateway) or on-chain peg-out/peg-in.** Out-of-band ecash (`spend_notes_oob` →
`reissue_external_notes`) is **intra-federation only** — federation B's mint cannot honor
notes signed by federation A's guardians. (An earlier research note wrongly suggested OOB
for cross-fed; it does not work.)

**The cheap path — shared-gateway internal swap** (verified in code): when the invoice's
payee LN pubkey equals the gateway's own node (i.e. that gateway *issued* B's invoice),
`is_direct_swap` fires (`gateway/lib.rs:3283`): the gateway funds B's incoming contract
directly inside B (`relay_direct_swap`), gets the preimage, and uses it to claim A's
outgoing contract. **No Lightning hop** — two on-mint ecash transactions bridged by one
gateway, priced at `send_fee_minimum`.

Recipe for a cheap A→B move:
```
G = a gateway registered in BOTH A and B, online, with enough B-side ecash liquidity
(invoice, recv_op_id) = B.receive(amount, gateway = G)     # persist BEFORE paying
send_op_id            = A.send(invoice, gateway = G)        # A auto-steers into the swap
# both clients self-drive: A funds outgoing contract -> G direct-swaps into B ->
# B claims (auto); A.send -> Success(preimage). We just await both.
```
Cheaper, **not more private** (G sees both legs), and **bounded by G's B-side liquidity**.
Fallback when no shared gateway: a real Lightning hop (`send_fee_default`, up to the cap).

## 6. Balance reality
- The client gives **one number per federation**: the sum of confirmed spendable mint notes
  (`get_note_counts_by_denomination().total_amount()`). **No pending/in-flight split, no
  cross-fed total.** A spend deletes notes immediately; in-flight value lives in state
  machines, not the balance; balance is zero while recovering.
- So **the app must track pending incoming/outgoing itself** and sum across feds for a
  wallet total. Confirms T3/T13 — the structured msat balance snapshot isn't gold-plating,
  the SDK can't give it to us. Serialize concurrent spends behind a mutex (both wallets do)
  so virtual balance can't go negative.

## 7. Recovery reality (what the seed does and does NOT restore)
- Seed recovery (`MintClientInit::recover`) restores **ecash per federation — but only for
  federations whose invite codes you already have.** Fedimint has no registry of "which
  feds does this seed belong to." It is a parallelized epoch-history scan, fast only
  because of per-fed encrypted backup snapshots (`backup_to_federation`).
- **NOT restored:** the federation list / invite codes, transaction history, in-flight
  sends, any app coordination state.
- **Therefore the app MUST back up, beyond the seed: (1) the federation invite list, (2)
  the in-flight move-coordination state.** Both harbor and Fedi leave the federation list
  device-local (manual re-join after a wipe) and back up *empty* metadata — acceptable for
  a manual wallet, **dangerous for an auto-allocator** that scatters funds across many
  feds. Our ADR-0003 already says Block Store stores "the seed *and* joined federation IDs"
  — now confirmed load-bearing; extend it to also carry in-flight move state.

## 8. What neither reference wallet does: cross-federation movement
Verified by repo-wide grep + a dedicated read: **neither harbor nor Fedi moves value
between federations** (auto or manual). Both are fully siloed per-fed wallets; what looks
cross-fed in Fedi (sp-transfer, multispend) is intra-pool fiat between users of the *same*
federation. So we inherit all the per-fed plumbing for free (client, receive, pay, balance,
backup, self-resume) but **the cross-fed move coordination and the allocator are genuinely
ours to build** — that is exactly where the innovation token is spent (ADR-0021).

## What this means for us (the corrected model)
The move is **two ordinary fedimint operations, each crash-safe and self-resuming inside
its own client.** We do NOT re-implement the send/receive state machines (my earlier
`move_sm` task was wrong to). We own only a **thin coordination record** linking the two
legs — the one thing neither client knows, because neither knows this invoice and this
payment are halves of one move:

```
MoveRecord {            // stored in our app DB, our own prefix; the resume index
  move_id, occurrence,                 // occurrence = T10 epoch (stable while in flight)
  from, to, amount, fee_cap,
  gateway,                             // pin G for the internal swap
  invoice?, recv_op_id?,               // persist when B.receive returns (NOT idempotent)
  send_op_id?,                         // persist when A.send returns
  phase, outcome,                      // phase derivable from which fields exist
}
```
- **Resume loop (lift harbor/Fedi):** on boot, reopen every client (each executor
  self-resumes its own SMs), then walk our Pending MoveRecords and re-`subscribe` to each
  leg's op id. Our record is the resume *index*; the clients hold the resumable state.
- **Phase is derivable, not a re-implemented SM:** no `recv_op_id` → call `B.receive`;
  `recv_op_id` + no `send_op_id` → call `A.send(invoice)`; both present → await outcomes.
  RESUME, never restart (don't re-mint the invoice; the client dedups a re-send if its DB
  is intact).
- **Idempotency markers for our own automated actions** (the Fedi `LastSPv2SweeperWithdrawal`
  pattern): persist the move intent *before* acting so a kill mid-rebalance can't re-issue.
- **The two bounded hazards to design for:** (a) the receive orphan window (crash between
  gateway-mints-invoice and client-commits) — accept the invoice expires, reconcile from
  the intent; (b) restore-from-seed mid-move loses the send-dedup — back up the move state
  (point 7) and/or rely on the v1 hard balance cap (ADR-0018) to bound the loss.

Net: far less to build than a hand-rolled state machine. Lift the per-fed plumbing from
harbor/Fedi; build only the thin two-op-id coordination record + the resume loop + the
allocator on top.
