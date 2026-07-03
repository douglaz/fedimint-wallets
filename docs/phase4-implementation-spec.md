# Phase 4 implementation spec — engine hardening + the operation ledger

Detailed, buildable design for [phase4-plan.md](./phase4-plan.md), implementing
[operation-history-spec.md](./operation-history-spec.md) and the fix backlog from
[reviews/2026-07-03-engine-review.md](./reviews/2026-07-03-engine-review.md). SDK claims
verified against the pin (`~/p/fedimint` @ `b108ec6`); exact citations inline.

**Base:** `main` AFTER Phase 3.A (Evacuate execution) merges — 3.A touches
`executor.rs`/`probe.rs`/`tick.rs`/`runtime.rs`, and this phase edits the same files. Where
3.A changed an anchor named here, the 3.A version wins and the change applies on top.

**Greenfield note.** Pre-release, no persisted data, no external users: NO backwards
compatibility, NO migration shims, NO serde compat layers. Every type/row-shape change below
replaces the old one outright.

## Executive summary

Part I hardens the engine against the review's findings: the scorer gets a threshold sanity
floor (the trust boundary 3.B discovery will lean on), the send-leg fee quote moves to the
correct base so `fee_cap` is a hard bound on both legs, a paid-but-uncredited move becomes a
loud `Stranded` state with its preimage preserved instead of a silent terminal loss, and the
allocator gets deterministic tie-breaks + per-tick reservation. Part II builds the operation
ledger: a third durable structure (append-only, seq-ordered, terminal-immutable) written in
the SAME dbtx as the intent transitions it describes, plus `wallet-cli history`/`show` — the
user can reconstruct exactly what happened, why, what it cost, and when, for every operation
including failures and refusals.

- **Exit gate:** a devimint session (join → direct-inflow → move → tick, with one forced
  failure and one refusal) is fully reconstructible from `wallet-cli history`; a fee cap set
  just under a move's true cost refuses before paying.
- **Build order:** pure-first — scorer/allocator (§1, §4, §5) → newtype moves (§6) → ledger
  types + `Intent` extension (§7, §8) → executor fee/strand changes (§2, §3) → journal ledger
  integration (§9) → raw-op/join/tick recording + repair (§10) → CLI verbs (§11) → smoke.

---

# Part I — hardening (4.A)

## 1. Scorer trust floor (`wallet-core/src/scorer.rs`)

Today the structural floor checks only lower bounds (`scorer.rs:118-125`) and `rank()`
multiplies raw `threshold` (`:182`). Not reachable via today's probe — it DERIVES
`threshold = 2f+1` from the guardian set (`probe.rs`, `NumPeers`) — but the scorer is the
trust boundary and 3.B's discovery assemblers will feed it attacker-influenced facts.

1. Add `ReasonCode::InvalidThreshold` (scorer's `ReasonCode`, `scorer.rs:83-92`).
2. In the structural floor, hard-reject (push reason, `floor_ok = false`) when:
   `facts.threshold == 0 || facts.threshold > facts.guardian_count`.
3. **Proportional floor — SETTLED: require the BFT bound.** Also hard-reject (same reason)
   when `facts.threshold < bft_threshold(facts.guardian_count)` where
   `fn bft_threshold(n: u32) -> u32 { n.saturating_sub(n.saturating_sub(1) / 3) }`
   (fedimint's own `n − f` with `f = (n−1)/3` — `NumPeers::threshold`). SATURATING on
   purpose: the floor collects ALL failing reasons, so this check still EXECUTES for
   `guardian_count == 0` (already rejected by `NoFaultTolerance` above) and must not
   underflow on attacker-supplied facts — the scorer is the trust boundary; no arithmetic
   in it may panic. Golden: `guardian_count = 0` yields a verdict (no panic).
   Every real fedimint federation satisfies it exactly, so nothing live is rejected; a
   discovered config CLAIMING a weaker threshold (e.g. 3-of-100) is rejected as structurally
   dishonest rather than ranked equal to a 3-of-4. Absolute `min_threshold` stays as-is.
4. In `rank()`, clamp the structural term:
   `facts.threshold.min(facts.guardian_count).saturating_mul(STRUCTURAL_WEIGHT)` —
   defense-in-depth even though (2) already rejects the overflow case.

Goldens (extend the scorer suite): `threshold == 0` rejected; `threshold > guardian_count`
rejected + rank 0; `3-of-100` rejected with `InvalidThreshold`; `3-of-4` (= 4 − 1) passes;
`67-of-100` passes; the reason surfaces in `FederationVerdict.reasons`.

## 2. Send-leg fee quote on the contract amount (`multi_client.rs`, `executor.rs`)

**SDK ground truth (verified at the pin):** lnv2's outgoing contract is
`send_fee.add_to(invoice_amount)` (`fedimint-lnv2-client/src/lib.rs:599`) — the GATEWAY fee
is base+ppm ON THE INVOICE amount. The FEDERATION send-tx fee must be quoted on the FULL
contract value: `send_fee_quote`'s doc says "`amount` is the full outgoing contract value
(`send_fee.add_to(invoice_amount)`)" (`lib.rs:875-882`). Our `MultiClient::send_fee_quote`
(`multi_client.rs:396-412`) quotes on the invoice amount instead → the federation component
is under-estimated by the fee on the gateway-fee delta, so `fee_cap` can under-block. No
send-side fixed point is needed (gateway on invoice; federation on contract; no circularity).

1. `MultiClient::send_fee_quote(&self, id, contract: Msat) -> anyhow::Result<Msat>` —
   replace the `invoice: &Invoice` parameter with the explicit contract amount (the caller
   computes it); delete the invoice parsing; quote
   `lnv2.send_fee_quote(Amount::from_msats(contract.0))`. Fix the stale doc comment.
2. Executor `MoveStep::Pay` arm (`executor.rs:340+`):
   ```rust
   let gw_cost = send_gateway_fee.on(Msat(invoice_msat)).0;        // SDK-exact component
   let contract_msat = invoice_msat.saturating_add(gw_cost);       // lib.rs:599
   let fed_fee = self.mc.send_fee_quote(&from, Msat(contract_msat)).await.map_err(retryable)?;
   let send_quote = Msat(gw_cost.saturating_add(fed_fee.0));
   ```
   `total_within_cap(receive_quote, send_quote, rec.fee_cap)` unchanged.
3. **Persist the quotes** on the `MoveRecord` (new fields, §3's table) — BEFORE the cap
   check: in the `Pay` arm, once `send_quote` is computed, set
   `rec.send_fee_quoted = Some(send_quote)` and `put_move` FIRST, THEN run
   `total_within_cap` — the paradigm failure this field must explain is precisely the
   "fee over cap" refusal, which returns before any send commits (persisting a quote on a
   refused move is safe: it is a derived cache write, no money moves). The receive-side cost
   is stored at `CreateInvoice`'s `put_move` (`rec.receive_fee = Some(receive_quote)`) so the
   ledger (§9) never re-parses invoices.

Tests: golden on the arithmetic helper (extract
`fn send_quote(invoice_msat, gw_fee, fed_fee_on_contract) -> Msat` into `fee.rs` if that
reads cleaner); devimint (deferred smoke): a `--fee-cap` set to `true_cost − 1` msat refuses
with "fee over cap" BEFORE paying; `true_cost` passes.

## 3. Stranded moves: preserve the preimage, never a silent terminal loss (`executor.rs`, `move_protocol.rs`)

Today (`executor.rs:430-452`) `SendState::Success(_preimage)` discards the preimage, and a
non-`Claimed` receive marks `MovePhase::Failed` → `ExecError::Permanent` — after the money
irreversibly left the source. That is the misbehaving-gateway case (T4): the gateway claimed
A's payment and did not fund B's contract.

1. `MoveRecord` gains (greenfield row-shape change, no migration):
   ```rust
   pub preimage: Option<Preimage>,        // proof A's payment settled; recovery artifact
   pub receive_fee: Option<Msat>,         // §2 — receive-side cost, set at CreateInvoice
   pub send_fee_quoted: Option<Msat>,     // §2 — send-side quote, set at Pay
   ```
2. `MovePhase` gains a `Stranded` variant. Semantics: TERMINAL (like
   `Refunded`/`Failed` — preserved by `derive_phase`, `move_protocol.rs:361-370`), but
   distinct so the ledger/UI can say "debited, not credited — payment proof saved".
3. `AwaitSettle` arm, on `SendState::Success(preimage)`:
   - FIRST persist: `rec.preimage = Some(preimage); self.journal.put_move(&rec).await?;`
     (a crash after this point can never lose the proof), THEN await the receive.
   - `ReceiveState::Claimed` → `Settled` (unchanged).
   - `ReceiveState::Expired | Failed(msg)` → `rec.phase = MovePhase::Stranded`,
     `rec.outcome = Some("send settled but receive was not credited: <detail>; payment
     preimage saved on the move record")`, `put_move`, and the loop falls through to the
     terminal arm. Transport errors still bubble as `Retryable` via the existing
     `map_err(retryable)` BEFORE reaching these match arms — only op-TERMINAL receive states
     strand.
4. `next_step` (`move_protocol.rs:219+`): `Stranded` → `MoveStep::Failed` (the existing
   terminal surface — `perform` returns `Permanent(outcome)`); `derive_phase` preserves it
   like the other terminal phases.
5. Goldens: `next_step(Stranded) == Failed`; `derive_phase` preserves `Stranded`;
   assemble/merge keeps `preimage`/fee fields from cache (extend the no-blank tests);
   executor unit test: success-send + terminal-failed receive → record is `Stranded`, carries
   the preimage, error mentions "preimage saved".

Explicitly settled: `Stranded` is terminal (an op-log-terminal receive cannot be fixed by
re-driving); recovery tooling (claim with the saved preimage / support escalation) is future
work — the invariant THIS phase buys is that the proof is durable and the state is honest.

## 4. Allocator polish (`wallet-core/src/allocator.rs`)

1. **Deterministic tie-break:** `safest_other`'s fallback (`allocator.rs:200-205`) picks the
   first eligible fed in `Vec` order. Change to the smallest `FederationId` among eligibles:
   `.filter(|fed| eligible_for_evacuation(..)).min_by_key(|fed| fed.id)`. Document on
   `AllocatorSnapshot::federations` that iteration order is otherwise significant and must be
   stable across ticks (it feeds decision ordering).
2. **Per-tick reservation (pre-3.B requirement):** decisions in one `decide()` pass are
   computed against one immutable snapshot; two evacuations into the same destination can
   jointly exceed `per_fed_cap`, and an evacuation source can also be drained by a funding
   move. Add, local to `decide()`:
   ```rust
   let mut credited: BTreeMap<FederationId, u64> = BTreeMap::new(); // pending inbound per fed
   let mut debited:  BTreeMap<FederationId, u64> = BTreeMap::new(); // pending outbound per fed
   ```
   - `cap_room(snapshot, fed)` becomes `cap_room_with(snapshot, fed, &credited)` =
     `per_fed_cap − spendable − credited[fed]` (saturating).
   - The reservation ADJUSTS each branch's EXISTING availability formula — it never replaces
     branch invariants (all saturating, `fee_cap = snapshot.max_fee`):
     - TopUp (standby → spending): `available = spendable − debited[src] − fee_cap`.
     - Standby funding (spending → standby): the surplus floor STAYS —
       `available = (spendable − target_spending_balance) − debited[src] − fee_cap` — the
       spending fed is never drained below its configured target to fund the standby.
     - Evacuation amount: `min(spendable − debited[src] − fee_cap, cap_room_with(..))`.
     The `− fee_cap` term is the move's OWN fee reserve, not just prior moves': the executor
     spends up to `amount + fee_cap` from the source, so an amount chosen against the bare
     balance would be emitted and then fail on insufficient funds. An evacuation may leave
     ≤ `max_fee` behind when actual fees run lower — bounded, honest, and preferable to a
     move that cannot execute.
   - Every emitted `Move`/`Evacuate` then records `credited[to] += amount` and
     `debited[from] += amount + fee_cap` — the conservative bound that makes any number of
     same-source moves provably non-overdrawing (fees are unknowable at decide time but
     bounded by the cap).
   - `eligible_for_evacuation`'s `cap_room > 0` check uses the reserved-aware value.
3. **Document the deliberate asymmetry** (one comment on `usable_source`): source-side trust
   is intentionally NOT gated on `probed_ok`/reputation — draining a distrusted fed is
   desirable; only credit DESTINATIONS are gated (`receive_blocker`).

Goldens: two shutdown feds + one healthy destination → the two `Evacuate` amounts sum to
≤ `cap_room`; evacuation into the standby + a standby top-up in the same tick never jointly
exceed the cap; tie-break picks the lower id when the pinned standby is ineligible and two
eligibles tie.

## 5. Dead surface (`wallet-core`, `wallet-cli`)

1. **Delete `Action::Cap`** (`types.rs:117-121`): no producer exists (`decide()` only emits
   `RefuseInflow`). Remove the variant, its `is_executable`/`fee_cap` arms, the CLI
   `describe_decision` arm, and fold its doc into `RefuseInflow` ("advisory: do not route the
   next inflow / cap allocation here"). The ledger's `Refusal` kind (§7) covers the concept.
2. **Delete `AllocatorDecision.requires_auth`**: always `false`, never read. ADR-0011 will
   reintroduce an auth gate WITH its consumer.
3. **Wire `AllocatorSnapshot.now`:** keep the field (it is the tick's single pure clock
   input); `wallet-cli` sets `TickPolicy.now` from `SystemTime::now()` unix SECONDS in
   `build_tick_policy` (both `tick` and `status`). Note 3.A's probe sources its own `now` for
   shutdown derivation — independent; this makes the snapshot honest for any future
   time-aware `decide()` logic.
4. `FedBalance.{in_flight, claimable, reserved_fee}` stay (conscious shape-stability
   trade-off, documented at the type).

---

# Part II — the operation ledger (4.B)

Implements [operation-history-spec.md](./operation-history-spec.md); this section is the
code-level mapping. Authority split (recorded in both docs): the history spec is normative
for the MODEL — the three-structures separation, the write discipline (append-once /
advance-forward / terminal-immutable / same-dbtx), and the correlation-key rules; **this
spec's §7 is authoritative for the exact field-level Rust shapes** (it refines the history
spec's sketch: `reason` is mandatory via `UserInitiated`, gateways are `Option`).

## 6. Newtype moves (`wallet-fedimint/src/types.rs` → `wallet-core/src/types.rs`)

`OperationId([u8; 32])`, `Preimage([u8; 32])`, `GatewayUrl(String)`, `Invoice(String)` are
pure data newtypes with serde derives and no SDK dependency. Move them into
`wallet-core::types` verbatim; `wallet-fedimint/src/types.rs` re-exports
(`pub use wallet_core::{GatewayUrl, Invoice, OperationId, Preimage};`) so its public API is
unchanged. Motivation: the ledger types (§7) reference `OperationId`/`GatewayUrl` and must be
pure + golden-testable in `wallet-core`.

## 7. Ledger types (`wallet-core/src/ledger.rs`, new module)

The types from operation-history-spec §2, final:

```rust
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperationRecord {
    pub seq: u64,
    pub correlation_key: IdempotencyKey,
    pub kind: OperationKind,
    pub actor: Actor,
    pub reason: ReasonCode,               // §8 — always present; user verbs = UserInitiated
    pub status: OperationStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub fees: FeeBreakdown,
    pub error: Option<String>,
    /// Set when this row's terminal `Failed` came from reconcile's NEGATIVE-inference
    /// repair (§10.3): such a failure is DEFEASIBLE — `advance` permits one
    /// evidence-carrying supersession (see the `advance` rule below).
    pub repaired: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Actor { User, Agent { occurrence: Occurrence } }

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationStatus { Started, Awaiting, Succeeded, Failed }

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationKind {
    Join { fed: FederationId },
    Receive { fed: FederationId, amount_invoiced: Msat, op_id: Option<OperationId>,
              gateway: Option<GatewayUrl> },
              // GROSS invoiced amount — the user's input, known BEFORE any resolution, so
              // the pre-call Started row is complete; the NET credit is
              // amount_invoiced − fees.receive_fee (lnv2 raw receive deducts fees from the
              // invoiced amount, unlike the exact-net DirectInflow)
    Pay { fed: FederationId, invoice_amount: Option<Msat>,
          payment_hash: Option<[u8; 32]>, op_id: Option<OperationId>,
          gateway: Option<GatewayUrl> },   // amount+hash None on the pre-parse Started row
                                           // (§10.1 — a malformed invoice never yields them);
                                           // filled by the post-parse record_update BEFORE
                                           // the SDK call — the hash is the durable link that
                                           // lets repair recover DEDUPED retries (§10.3)
    DirectInflow { to: FederationId, amount: Msat, recv_op: Option<OperationId>,
                   gateway: Option<GatewayUrl> },
    Move { from: FederationId, to: FederationId, amount: Msat,
           send_op: Option<OperationId>, recv_op: Option<OperationId>,
           gateway: Option<GatewayUrl>, evacuation: bool },
    Refusal { fed: FederationId },
    Tick { occurrence: Occurrence, decisions: u32, performed: u32, failed: u32 },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FeeBreakdown {
    pub fee_cap: Option<Msat>,
    /// Receive-side cost. EXACT for intent-backed ops (invoice − net, from the
    /// MoveRecord's fixed invoice); a pre-call QUOTE for raw receives (§9.3 — gateway
    /// deduction + federation claim-fee quote on the post-gateway contract).
    pub receive_fee: Option<Msat>,
    pub send_fee_quoted: Option<Msat>,    // pay-time quote, from the MoveRecord (§2)
}
```

Pure helpers, golden-tested in `wallet-core`:
- `fn kind_from_action(action: &Action, rec_ops: ...) -> OperationKind` — `Action::Move` →
  `Move { evacuation: false }`, `Action::Evacuate` → `Move { evacuation: true }`,
  `Action::DirectInflow` → `DirectInflow`, `Action::RefuseInflow` → `Refusal`.
- `fn status_from_intent(s: IntentStatus) -> OperationStatus` — `Pending|Executing →
  Started`, `Awaiting → Awaiting`, `Done → Succeeded`, `Failed → Failed`.
- `fn advance(record, new_status, now_ms, fees, ops, error) -> Option<OperationRecord>` — the
  append-once/advance-forward/terminal-immutable rule as a PURE function: returns `None`
  (no write) ONLY when the stored record is already TERMINAL, or when the requested status
  would REGRESS (e.g. `Awaiting → Started`) — a NON-terminal row may always be ENRICHED
  (op-ids/gateway/fees/error filled in) at the SAME status (`record_update`' normal
  post-call path is exactly that), bumping `updated_at_ms`. ONE principled exception:
  `OperationRecord` carries `repaired: bool` — a `Failed` written by reconcile's
  NEGATIVE-inference repair (§10.3) sets it, and `advance` permits exactly one
  EVIDENCE-CARRYING transition out of such a row (an update bearing an `op_id` or a real
  awaited terminal outcome), clearing the flag. Absence-of-evidence conclusions are
  defeasible; evidence wins — this is what makes a clock-skewed false repair self-healing
  instead of permanently blocking the real writer. Golden the full transition matrix
  including same-status enrichment, terminal-rejects-everything, and
  repaired-Failed-superseded-by-evidence (once, and only with evidence).

## 8. `Intent` extension + reason threading (`wallet-core/src/executor.rs`, `types.rs`)

1. `ReasonCode` gains `UserInitiated` (+ `reason_tag` arm `"user_initiated"`) AND
   `StandingInstruction` (+ `"standing_instruction"`). The dummy
   `ReasonCode::SpendingBelowTarget` hardcoded in `runtime.rs` `direct_inflow`/`do_move`
   (`:172`, `:245`) becomes `ReasonCode::UserInitiated` — delete the "never persisted"
   comments; it IS persisted now. `Tick` ledger rows carry `StandingInstruction` (truthful:
   the run exists because the standing instruction executed; the run's individual decisions
   carry their OWN reasons on their own rows — a tick has no single allocator reason).
2. `Intent` gains `reason: ReasonCode`, `actor: Actor`, `created_at_ms: u64`.
   `Intent::from_decision(decision: &AllocatorDecision, actor: Actor, now_ms: u64)` — the two
   new parameters are threaded from `apply`:
3. **Failure strings reach the ledger:** `Journal::set_status` gains an error parameter —
   `set_status(key, status, error: Option<&str>)` (greenfield trait change; `MemJournal` and
   all test doubles updated mechanically). `drive()` passes the `ExecError`'s diagnostic
   string on the `Permanent`/`Unsupported` paths and `None` elsewhere — several permanent
   failures ("fee over cap", `Unsupported`, early bails) never reach a terminal `put_move`,
   so `MoveRecord.outcome` alone cannot source the ledger's `error` (§9.2 uses the
   executor-provided error first, `MoveRecord.outcome` as fallback).
4. `apply(journal, executor, decisions, actor: Actor, now_ms: u64)` (and NOT `reconcile` —
   it re-drives stored intents that already carry actor/reason/created_at). Call sites:
   - `Runtime::tick` → `Actor::Agent { occurrence: policy.occurrence }`, `now_ms` from the
     runtime clock (§9.4).
   - `Runtime::direct_inflow`/`do_move` → `Actor::User`.
   - Tests/`MockExecutor` suites updated mechanically.

## 9. Durable ledger + journal integration (`wallet-fedimint/src/journal.rs`)

### 9.1 Key layout (within the `[0x00]` app prefix; extends the existing table)
- `0x05 ++ be64(seq)` → JSON row v1(`OperationRecord`) — time-ordered scan.
- `0x06 ++ correlation_key_utf8` → `be64(seq)` — key→seq lookup; the one-row-per-key guard.
- `0x07` (single key) → `be64(next_seq)` — the durable counter.

### 9.2 Write discipline (normative: operation-history-spec §3)
One private helper does ALL ledger writes, inside a caller-supplied dbtx:

```rust
async fn ledger_upsert_in(dbtx, key, build: impl FnOnce(Option<OperationRecord>, u64 /*seq*/)
    -> Option<OperationRecord>) -> Result<(), ExecError>
```
- Look up `0x06`; absent → allocate `seq` (read-increment-write `0x07` in this dbtx), build
  the fresh record, insert both rows. Present → read `0x05`, call `build(Some(existing))`;
  `None` → no-op (terminal-immutable / no-change); `Some` → overwrite `0x05` (same seq; the
  ONLY permitted mutation is a non-terminal record advancing per §7's pure `advance`).
- Journal-integrated writes happen in the SAME dbtx as the intent write they describe:
  - `Journal::upsert` — after the intent row write: ledger row for `intent` (create-or-advance
    with `status_from_intent`). Fees/ops: read the `0x02` move row (same partition, same dbtx)
    when present and copy `receive_fee`/`send_fee_quoted`/op-ids/gateway into the kind/fees.
  - `write_intent_and_index` (shared by `set_status`/`set_status_if`) — after the index+row
    writes: advance the ledger row to `status_from_intent(new_intent.status)`; on `Failed`
    the `error` is the executor-provided string from `set_status`'s error param (§8.3)
    first, `MoveRecord.outcome` as fallback. Fees/op-ids/gateway are refreshed from the
    `0x02` move row on EVERY ledger write, not only terminal ones — a `DirectInflow` goes
    `Awaiting` right after `perform` persisted its `recv_op`/gateway/receive-fee, and a
    retryable `Move` can carry op ids before resetting to `Pending`; `history`/`show` must
    reflect in-flight metadata. (Same-dbtx read; `perform` persists the record BEFORE the
    status flip — `executor.rs` ordering, verified.)
- Consistency guarantee: ledger and journal commit or fail together; the ledger can never
  claim a state the journal doesn't have.

### 9.3 Standalone recording (no intent involved)
Public async methods on `FedimintJournal` (each one dbtx via the same helper):
- `record_started(key, kind, actor, reason, now_ms, fee_cap)` / `record_terminal(key, status,
  now_ms, error)` / `record_update(key, upd: RawOpUpdate)` with
  `RawOpUpdate { op_id: Option<OperationId>, gateway: Option<GatewayUrl>,
  invoice_amount: Option<Msat>, payment_hash: Option<[u8; 32]>,
  fees: Option<FeeBreakdown> }` (the hash is what the §10.3 dedup repair keys on — the
  post-parse pre-call update writes it) — raw `receive`/`pay` and
  `join` attempts (per-attempt keys from operation-history-spec §2; nonce generated by the
  CALLER — the CLI/runtime own randomness, the journal stays deterministic). The standalone
  path is the ONLY writer for raw rows, so it must carry the parsed `invoice_amount` (a
  `Pay` row otherwise stays amount-less forever) AND the fees: the CLI fills them from the
  SAME quote helpers the executor uses — raw `pay`: `send_gateway_fee` + `send_fee_quote`
  (on the §2 contract base) → `send_fee_quoted`; raw `receive`: BOTH receive-side
  components — the gateway deduction (`routing_info.receive_fee` via `subtract_from` on the
  invoiced amount) PLUS the federation claim fee (`receive_fee_quote` on the post-gateway
  contract amount) → `receive_fee` (omitting the fed component would under-report every
  raw receive on a fed with a non-zero receive tx fee; see the `FeeBreakdown.receive_fee`
  doc — this raw-path value is a QUOTE, unlike the exact intent-backed one). These quotes require a CONCRETE gateway: they
  are filled only when one is known (an explicit `--gateway`, or a pinned executor
  gateway); on the lnv2 AUTO-SELECT path (`None` passed through, the current default
  semantics — unchanged) `gateway` and the fee fields stay `None`, an honest recorded gap
  rather than the CLI silently pinning a gateway to obtain a quote. Quote failures likewise
  degrade to `None` (never block the money op on a fee display).
- `record_tick_started(key, occurrence, now_ms)` and
  `record_tick_terminal(key, counts: Option<(decisions, performed, failed)>,
  status: OperationStatus /* Succeeded | Failed */, error: Option<String>, now_ms)` —
  `Runtime::tick` writes `Started` before deciding, terminal after apply. The terminal call
  carries an explicit status + error so the §10.4 bail paths (pinned-input problems, stale
  occurrence) land as `Failed` rows WITH their diagnostic string and zero-or-partial counts —
  a boolean "terminal" flag could only fake them as successful ticks.
- `record_refusals(decisions, occurrence, now_ms)` — one `Refusal` row per advisory decision,
  keyed by its EXISTING `refuse:` idempotency key (dedup across re-ticks of the same
  occurrence is automatic via `0x06`).
- Scans: `history(limit, before_seq) -> Vec<OperationRecord>` (reverse `0x05` scan) and
  `operation(key | seq) -> Option<OperationRecord>`; poison-tolerant like every other scan
  (skip+warn undecodable rows, surface only storage errors).

### 9.4 Clock
`FedimintJournal::new(db)` gains `with_clock(db, clock: fn() -> u64 /*ms*/)` for tests;
production uses `SystemTime::now()` millis (a bad clock degrades display only — `seq` is the
ordering authority). `Runtime` passes `now_ms` where §8 needs it via the same source.

## 10. Raw ops, join, tick, refusals (`wallet-cli/src/main.rs`, `runtime.rs`, `multi_client.rs`)

1. **Raw `receive`/`pay`** (operation-history-spec §3 rule 5): the CLI generates the
   per-attempt key — `pay:<fed>:<nonce>` / `recv:<fed>:<nonce>`, NONCE-ONLY: the key must be
   constructible from the RAW input BEFORE parsing, because a malformed BOLT11 has no
   payment hash yet its failed attempt must still be a durable history row (the
   synchronous-error path below); dedup/grouping rides on the recorded `op_id`, not the key.
   The nonce is 32 random hex chars = 128 bits, everywhere a nonce appears in a ledger key
   incl. `join:`/`tick:` — 32-bit nonces make birthday collisions realistic over a wallet
   lifetime, and a collision aliases two attempts onto one `0x06` entry. The recorded window
   opens BEFORE any resolution can fail: fed selection (pure registry read) → key generation
   → `record_started` → THEN gateway resolution (`pick_receive_gateway` bails on
   no-registered-gateway — that failure must be a `Failed` row, so it happens inside the
   window) → invoice parse → post-parse `record_update` (amount + payment hash, durable
   BEFORE the SDK call) → the SDK call. So the CLI writes the `Started` row
   (`record_started`) BEFORE calling
   `MultiClient::receive`/`pay`, embeds the key in the op's `custom_meta` (extend the current
   role-tag JSON: `{ "role": "receive", "correlation_key": "<key>" }` — `MoveMeta` for
   journaled moves is UNTOUCHED), then `record_update` with the returned op id — which
   ALSO advances the row `Started → Awaiting`: once the federation returned an op id the
   operation is live-and-awaiting, a distinct state from "may never have reached the
   federation", and `history --status awaiting` must surface it. Completing the flow:
   - **Synchronous SDK errors** (bad invoice, no gateway, failed federation call — no op id
     exists): the CLI's error path calls `record_terminal(Failed, <the real error string>)`
     before bailing — never leave the pre-written row for a generic repair to mislabel.
   - **`SendOutcome::AlreadyPaid(op)`**: the outcome is already terminal at creation time —
     `record_update` + `record_terminal(Succeeded)` immediately (the row records the
     shared op id; op-id grouping keeps aggregation single-counted).
     `AlreadyInFlight(op)` → `Awaiting` like `Started(op)`.
   - **The key is surfaced**: `pay`/`receive` print `key: <correlation_key>` to stderr
     (the handle convention `direct-inflow`/`move` already use), so `await-* --key` is
     actually usable.
   `await-receive`/`await-send` take the correlation key via a new `--key` flag (optional —
   without it they behave as today, ledger row advanced by reconcile repair instead) and
   `record_terminal` on the final state.
2. **Join**: `Command::Join` checks the registry FIRST (`journal.get_federation`): already
   registered → open only, NO ledger row. Otherwise `record_started(join:<fed>:<nonce>)` →
   `multi_client.join(...)` → `record_terminal(Succeeded|Failed)`.
3. **Reconcile repair** (`Runtime::reconcile`): after the existing §9 passes, scan the FULL
   ledger for non-terminal (`Started`/`Awaiting`) rows — no window cap: repair is the ONLY
   path that terminalizes stale rows, so a cap would strand anything beyond it permanently.
   The non-terminal set is what the scan costs, and it is self-shrinking (each repair
   terminalizes); the ledger itself is small by the non-goals (~10^5 rows ceiling).
   **Repair principle:** POSITIVE inferences (an
   op-log outcome found; the registry contains the fed) apply immediately and are ordinary
   terminal writes. NEGATIVE inferences (marking `Failed` on absence of evidence) are (a)
   deferred by a ONE-HOUR row-age heuristic — a fresh `Started` row may belong to an
   operation in flight in another process — and (b) written as SOFT failures
   (`repaired: true`, §7): if the heuristic ever misfires (clock jump, mis-set test clock),
   the real writer's evidence-carrying update supersedes the false `Failed` instead of being
   blocked by terminal immutability. Wall-clock therefore stays non-destructive: it only
   delays a defeasible mark. Per key prefix:
   - `join:` rows → registry present → `Succeeded`; absent (and > 1h old) →
     `Failed("join did not complete — federation not in the registry; re-run join")`.
     The registry is the wallet's MEMBERSHIP authority: a crash between the client-partition
     init and `put_federation` leaves an orphaned partition (`next_db_prefix` already never
     reuses it) and the fed genuinely unusable until a re-join, so this wording is honest —
     "never joined" would not be (local partition state may exist).
   - `pay:`/`recv:` rows with `op_id: None` → search the fed's op-log for the
     `correlation_key` in `custom_meta` (reuse the `backfill_ops` pagination; match on the
     new field). Found → fill `op_id`. NOT found and the row carries a `payment_hash`
     (a `pay:` row that parsed before crashing) → second lookup: scan the fed's lnv2 SEND
     ops for one whose invoice payment-hash matches — a DEDUPED retry
     (`AlreadyInFlight`/`AlreadyPaid`) reuses the ORIGINAL op, so the retry's key is in NO
     op's `custom_meta`; the hash, written durably pre-call, is the recovery link → adopt
     the shared op id (+ terminal outcome if recorded). Still nothing (and > 1h old, per the
     repair principle) → `Failed("never reached the federation")` — truthful at ATTEMPT
     granularity (this attempt never called; a no-hash row was malformed or crashed
     pre-parse).
   - `pay:`/`recv:` rows in `Awaiting` with `op_id: Some` (the COMMON stuck case: crash
     after `record_update`, or the user never ran `await-*` with `--key`) → read that
     op-log entry directly; if it carries a recorded terminal outcome, `record_terminal`
     accordingly; still in flight → leave `Awaiting` (truthful) for a later pass. (The scan
     therefore covers `Started` AND `Awaiting` raw rows; the negative-inference `Failed`
     applies only to `Started` ones — an `Awaiting` row proves the op reached the
     federation.)
   - `tick:` rows still `Started` with `created_at_ms` older than ONE HOUR (far beyond any
     tick's runtime) → `Failed("interrupted — no terminal report")`. A crash between the
     tick's `Started` write and its terminal write is otherwise unrepairable (later ticks use
     fresh nonces). The age threshold keeps a CONCURRENTLY-running tick's row safe from a
     simultaneous reconcile (the CLI is one-shot single-writer by convention, but the ledger
     must not corrupt a live row if that convention is broken; clock dependence here is
     display-only harm at worst).
   - Intent-keyed rows are NEVER repaired here — the journal integration (§9.2) owns them.
4. **Tick + refusals** (`Runtime::tick`): `record_tick_started` before probing (key
   `tick:<occurrence>:<nonce>`, nonce per §2 of the history spec); after apply,
   `record_refusals(...)` then `record_tick_terminal` with the counts. On the bail paths
   (pinned-input problems, stale occurrence) the tick row goes terminal `Failed` with the
   bail message — a refused tick is history too.

## 11. CLI verbs (`wallet-cli/src/main.rs`)

```
wallet-cli history [--limit N (default 50)] [--fed <hex>] [--actor user|agent]
                   [--status started|awaiting|succeeded|failed] [--json]
wallet-cli show <correlation-key | seq> [--json]
```
- `history` scans newest-first and prints ONE TAB-SEPARATED line per record to stdout:
  `seq<TAB>updated_at(RFC3339)<TAB>kind<TAB>status<TAB>amount_msat<TAB>recv_fee_msat<TAB>send_fee_quoted_msat<TAB>actor<TAB>reason<TAB>key`
  where `kind` ∈ `join|receive|pay|direct-inflow|move|evacuation|refusal|tick`, `actor` ∈
  `user|agent:<occurrence>`, `reason` = `reason_tag` (snake_case), unknown fields = `-`.
  The two fee columns are deliberately SEPARATE and the send column is NAMED quoted: the
  receive fee is exact, the send fee is a pay-time estimate until the SDK exposes the final
  contract cost — one collapsed "fees" number would present a quote as exact. Filters apply
  before `--limit`. `--json`: one serde_json `OperationRecord` per line (JSONL), no tab
  table.
- `show` prints the full record multi-line (both op ids, gateway, fee breakdown, timestamps,
  error, linked intent status read live from the journal); `--json` = the raw record.
- Both are read-only and never touch the network (journal scans only) — they must work
  offline. Diagnostics to stderr as everywhere else (ADR-0023).

## 12. Build order

1. §1 scorer + §4 allocator + §5 dead-surface (pure; independently landable).
2. §6 newtype moves (mechanical; unblocks §7).
3. §7 ledger types + §8 `Intent`/`apply` extension (pure; all suites mechanically updated).
4. §2 fee base + §3 strand handling (`MoveRecord` fields land here).
5. §9 journal ledger integration (goldens on `advance` matrix + MemDatabase suites).
6. §10 recording + repair; §11 CLI verbs.
7. §13 smoke (written, run by hand on the two-fed harness).

## 13. Tests / exit gate

- **rb-lite gate (fast):** compile + clippy `-D warnings` + fmt + ALL goldens: scorer floor
  cases (§1), reservation + tie-break (§4), `advance` transition matrix + terminal
  immutability + one-row-per-key (§7/§9), `status_from_intent`/`kind_from_action`, strand
  goldens (§3), fee arithmetic (§2). MemDatabase journal suites: same-dbtx atomicity, seq
  monotonicity + ordering, replay-does-not-duplicate, poison tolerance of ledger scans.
- **Deferred devimint smoke (`wallet-cli/tests/smoke_history_devimint.sh`, the 4.C exit
  gate):** two-fed harness (await-send-first pattern): join A+B → direct-inflow A →
  move A→B → tick (agent move) → one forced failure (fee cap 1 msat) → assert `history`
  shows every row with correct kind/actor/reason/fees, `created_at_ms` non-decreasing by seq
  (NOT `updated_at_ms` — an older row may legitimately finish after newer rows were created;
  seq stays the ordering authority), the failure `Failed` with its error, at least one
  `Refusal` or advisory row when induced, and `show <key>` resolves both legs' op ids. Plus
  §2's fee-cap refusal check.

## 14. Settled decisions

1. Proportional scorer floor = fedimint's own BFT bound `n − (n−1)/3` (§1) — nothing real
   rejected, dishonest configs rejected.
2. Send-side quoting needs NO fixed point; gateway-on-invoice is SDK-exact (§2).
3. `Stranded` is a TERMINAL `MovePhase` with the preimage persisted; recovery tooling is
   future work (§3).
4. Per-tick reservation lives INSIDE `decide()` as local maps — no snapshot mutation, no new
   types (§4).
5. `Action::Cap` and `requires_auth` are deleted, not deprecated (greenfield) (§5).
6. The pure newtypes move to `wallet-core`; `wallet-fedimint` re-exports (§6).
7. `Intent.reason` is non-optional; user verbs carry `ReasonCode::UserInitiated` (§8).
8. Ledger rows share the intent's dbtx (never a separate commit); standalone ops get their
   own dbtx via the same helper (§9).
9. `seq` is the ordering authority; wall-clock is display-only, injected for tests (§9.4).
10. `history`/`show` are offline journal scans; TSV + JSONL output shapes fixed in §11.
11. `Journal::set_status` carries the failure string (§8.3) — the ledger's `error` is the
    executor's diagnostic first, `MoveRecord.outcome` as fallback.
12. The send quote persists BEFORE the cap check (§2.3), so a "fee over cap" refusal is
    fully explained in history.
13. Stuck `Started` tick rows are repaired by reconcile after a 1-hour age threshold (§10.3).

## Scope guard / non-goals

ONLY Phase 4: no discovery (3.B), no watch loop/triggers (3.C), no UI, no on-chain peg-out,
no event-sourced transition log, no pruning, no recovery tooling for `Stranded` (the preimage
is persisted for it). Do not touch the fedimint pin, `MoveMeta`, or the Move/DirectInflow
money logic beyond §2/§3. `cargo fmt` only on files changed.
