# Phase 4 implementation spec — engine hardening + the operation ledger

Detailed, buildable design for [phase4-plan.md](./phase4-plan.md), implementing
[operation-history-spec.md](./operation-history-spec.md) and the fix backlog from
[reviews/2026-07-03-engine-review.md](./reviews/2026-07-03-engine-review.md). SDK claims
verified against the pin (`~/p/fedimint` @ `b108ec6`); exact citations inline.
**Status: hardened through 19 codex passes (gpt-5.4 / xhigh) — 48 findings absorbed,
final pass clean. Updated 2026-07-05:** §2's quote-base fix LANDED with the 3.A merge
(`5315df3`) — §2 now covers only the remaining persistence work — and §15 absorbs the
[2026-07-05 fresh-eyes review](./reviews/2026-07-05-fresh-eyes-review.md)'s P1/P2 backlog
into 4.A. Decisions settled in §14.

**Base:** `main` AFTER Phase 3.A (Evacuate execution) merged (`5315df3`) — 3.A touched
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

Part III (§15, added 2026-07-05) absorbs the fresh-eyes review into 4.A: shutdown-signal
corroboration, perform-time cap enforcement, evacuation-destination eligibility,
deterministic-send-rejection classification, gateway scanning, never-over TOCTOU
verification, partial-open loudness, a tick deadline, and the solve-loop extraction.

- **Exit gate:** a devimint session (join → direct-inflow → move → tick, with one forced
  failure and one refusal) is fully reconstructible from `wallet-cli history`; a fee cap set
  just under a move's true cost refuses before paying.
- **Build order:** §12 is authoritative (pure-first, Part III items interleaved where their
  files are already open).

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

## 2. Send-leg fee quotes — persist them (the quote-base fix LANDED in 3.A)

**SDK ground truth (verified at the pin):** lnv2's outgoing contract is
`send_fee.add_to(invoice_amount)` (`fedimint-lnv2-client/src/lib.rs:599`) — the GATEWAY fee
is base+ppm ON THE INVOICE amount; the FEDERATION send-tx fee is quoted on the FULL contract
value (`lib.rs:875-882`). No send-side fixed point is needed (gateway on invoice; federation
on contract; no circularity).

**Items 1–2 are DONE — landed with the 3.A merge (`5315df3`), verified 2026-07-05:**
`MultiClient::send_fee_quote_for_amount(&self, id, amount: Msat)` quotes on an explicit
amount (`multi_client.rs:396-405`; the old invoice-parameter method no longer exists), and
the `MoveStep::Pay` arm computes
`outgoing_contract_amount = invoice + send_gateway_fee.on(invoice)` and quotes the
federation fee on it (`executor.rs:777-792`), so `fee_cap` already hard-bounds both legs.
Do NOT add a conservative over-estimate on top — the quote is SDK-exact; stacking a margin
would over-block.

3. **The REMAINING work — persist the quotes** on the `MoveRecord` (new fields, §3's table) —
   BEFORE the cap
   check: in the `Pay` arm, once `send_quote` is computed, set
   `rec.send_fee_quoted = Some(send_quote)` and `put_move` FIRST, THEN run
   `total_within_cap` — the paradigm failure this field must explain is precisely the
   "fee over cap" refusal, which returns before any send commits (persisting a quote on a
   refused move is safe: it is a derived cache write, no money moves). The receive-side cost
   is stored at `CreateInvoice`'s `put_move` (`rec.receive_fee_quoted = Some(receive_quote)`)
   so the ledger (§9) never re-parses invoices — and symmetrically, a `gross_up` receive-side
   "fee over cap" failure persists the computed quote on the record BEFORE returning
   `Permanent`, so that refusal is explained in history too. The field is named QUOTED
   deliberately: it becomes the actual paid cost only when the receive CLAIMS (the invoice
   is fixed, so a `Succeeded` row's receive cost is exactly this value); on a never-paid /
   refunded / stranded row it is what the move WOULD have cost — `history` presents receive
   fees as exact only on `Succeeded` rows (§11).

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
   pub receive_fee_quoted: Option<Msat>,  // §2 — receive-side quote, set at CreateInvoice
                                          // (== the paid cost iff the receive claims)
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
     balance would either fail on insufficient funds (plain `Move` — no perform-time sizing
     exists for it) or be silently downsized (Evacuate — 3.A's `size_fresh_evacuation`
     resizes at perform time; the reservation here is the planning-side complement, and the
     two interact: a reserved amount should rarely need downsizing). An evacuation may leave
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
    /// Receive-side cost. EXACT only on a Succeeded intent-backed row (the fixed
    /// invoice's cost, realized at claim); a QUOTE otherwise — raw pre-call estimates
    /// (§9.3) and unclaimed/refused/stranded intent rows (§2.3: what it WOULD have cost).
    pub receive_fee: Option<Msat>,
    pub send_fee_quoted: Option<Msat>,    // pay-time quote, from the MoveRecord (§2)
}
```

`RawOpUpdate` (the enrichment payload §9.3's `record_update` also takes) is declared HERE in
`wallet-core::ledger` — all its fields are pure:

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawOpUpdate {
    pub op_id: Option<OperationId>,
    pub gateway: Option<GatewayUrl>,
    pub invoice_amount: Option<Msat>,
    pub payment_hash: Option<[u8; 32]>,
    pub fees: Option<FeeBreakdown>,
}
```

Pure helpers, golden-tested in `wallet-core`:
- `fn kind_from_action(action: &Action) -> OperationKind` — `Action::Move` →
  `Move { evacuation: false }`, `Action::Evacuate` → `Move { evacuation: true }`,
  `Action::DirectInflow` → `DirectInflow`, `Action::RefuseInflow` → `Refusal`. Op ids and
  gateway start `None`; the §9.2 refresh fills them from the `0x02` move row on every
  ledger write.
- `fn status_from_intent(s: IntentStatus) -> OperationStatus` — `Pending|Executing →
  Started`, `Awaiting → Awaiting`, `Done → Succeeded`, `Failed → Failed`.
- `fn advance(record, new_status, now_ms, upd: Option<&RawOpUpdate>, error: Option<&str>)
  -> Option<OperationRecord>` — the
  append-once/advance-forward/terminal-immutable rule as a PURE function: returns `None`
  (no write) ONLY when the stored record is already TERMINAL, or when the requested status
  would REGRESS (e.g. `Awaiting → Started`) — a NON-terminal row may always be ENRICHED
  (op-ids/gateway/fees/error filled in) at the SAME status (`record_update`' normal
  post-call path is exactly that), bumping `updated_at_ms`. ONE principled exception:
  `OperationRecord` carries `repaired: bool` and every write is flagged as REPAIR or
  AUTHORITATIVE (a parameter on `advance`/the record helpers; reconcile's repair passes are
  the only repair-flagged writers). ANY terminal row written by repair — soft-`Failed`
  (negative inference) AND soft-`Succeeded` (ambiguous join/hash correlation, §10.3) — sets
  `repaired: true`, and `advance` permits exactly one transition out of such a row by ANY
  AUTHORITATIVE write (clearing the flag; e.g. a late-returning join call or an
  `await-send --key` reporting the real outcome replaces the repair's guess) — an op-evidence update for `pay:`/`recv:`, the
  real `record_terminal` for `join:`/`tick:` rows (whose writers have no op id to show), or
  a journal-integrated status write. Repair writes never supersede anything terminal.
  Absence-of-evidence conclusions are defeasible; authority wins — this is what makes a
  clock-skewed false repair self-healing instead of permanently blocking the real writer.
  Golden the full transition matrix including same-status enrichment,
  terminal-rejects-everything, repaired-Failed-superseded-by-authoritative (once), and
  repair-never-supersedes-terminal.

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
    when present and copy `receive_fee_quoted`/`send_fee_quoted`/op-ids/gateway into the kind/fees.
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
  now_ms, error, upd: Option<RawOpUpdate>)` — the terminal write CARRIES the final
  enrichment (fees/ops/gateway) applied atomically WITH the transition: the definitive
  raw-op costs are only known AT settlement, and terminal immutability forbids enriching
  afterwards, so the one terminal write is where they land — / `record_update(key,
  upd: RawOpUpdate)` (`RawOpUpdate` is §7's pure `wallet-core::ledger` type; the hash is
  what the §10.3 dedup repair keys on — the post-parse pre-call update writes it) — raw
  `receive`/`pay` and
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
  doc — this raw-path value is a QUOTE, unlike the exact intent-backed one). These quotes
  require a CONCRETE gateway. Getting one on the default (no `--gateway`) path — the COMMON
  case, whose fees must not be a permanent blank:
  BOTH raw verbs KEEP lnv2 auto-select untouched (pinning the first registered gateway for
  metadata would disable lnv2's gateway FAILOVER — a receive that succeeds today via the
  second gateway would fail synchronously; recording must never regress availability).
  Instead, two-stage capture, same shape for both verbs:
  - Pre-call ESTIMATE (best-effort): quote against the first registered gateway (`receive`)
    / the invoice's issuing gateway when it serves the fed (`pay`); `gateway` field stays
    `None` — the actual auto-selected choice is unknown at this point.
  - DEFINITIVE backfill at settlement: the lnv2 op-log meta records the funded CONTRACT, so
    `await-* --key` and reconcile repair fill the real numbers — `pay`:
    `contract.amount − invoice_amount` (exact gateway component) + the federation fee
    quote → `send_fee_quoted`; `receive`: `amount_invoiced − contract.amount` (exact
    gateway deduction) + the federation claim-fee quote → `receive_fee`. A successful raw
    op always ENDS with its cost recorded even when it started with none.
  Residual quote failures degrade to `None` (never block the money op on a fee display).
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
production uses `SystemTime::now()` millis. `seq` is the ordering authority; the clock is
display material PLUS one real dependency, stated honestly: §10.3's repair heuristics read
`created_at_ms` (the negative-inference age gate and the join-attempt ↔ `joined_at` match),
so clock skew can influence DURABLE repair decisions — which is exactly why negative repairs
are SOFT/defeasible (§7) and why a mismatched join heuristic degrades to the soft-failure
path rather than certainty. Repair-path tests MUST include skewed-clock cases (forward jump
during the 1h window; join attempt stamped after `joined_at`). `Runtime` passes `now_ms`
where §8 needs it via the same source.

## 10. Raw ops, join, tick, refusals (`wallet-cli/src/main.rs`, `runtime.rs`, `multi_client.rs`)

1. **Raw `receive`/`pay`** (operation-history-spec §3 rule 5): the CLI generates the
   per-attempt key — `pay:<fed>:<nonce>` / `recv:<fed>:<nonce>` (`<fed>` = lowercase-hex
   `FederationId`, exactly as in the existing `move:`/`evac:` keys), NONCE-ONLY: the key must be
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
     but FIRST read the ORIGINAL op-log entry's meta (the funded contract + gateway live
     there, not in this deduped attempt), derive the definitive fees per §9.3's backfill
     formula, THEN `record_terminal(Succeeded, upd)` carrying them — terminalizing before
     the meta lookup would freeze the row with blank/estimated fees, breaking §9.3's
     "a successful raw op ends with its cost recorded". (The row records the shared op id;
     op-id grouping keeps aggregation single-counted.)
     `AlreadyInFlight(op)` → `Awaiting` like `Started(op)`.
   - **The key is surfaced**: `pay`/`receive` print `key: <correlation_key>` to stderr
     (the handle convention `direct-inflow`/`move` already use), so `await-* --key` is
     actually usable.
   `await-receive`/`await-send` take the correlation key via a new `--key` flag (optional —
   without it they behave as today, ledger row advanced by reconcile repair instead) and
   `record_terminal` on the final state.
2. **Join**: `Command::Join` checks the registry FIRST (`journal.get_federation`): already
   registered → open only, NO ledger row (the fast path). Otherwise
   `record_started(join:<fed>:<nonce>)` → `multi_client.join(...)` →
   `record_terminal(...)`. `MultiClient::join` RE-CHECKS under its own lock and returns
   `Ok` for a federation another process registered meanwhile — so it must surface which
   case happened: return `JoinOutcome { id, newly_joined: bool }` (mechanical greenfield
   change). `newly_joined` → `Succeeded`; `!newly_joined` (the concurrent window) →
   `Succeeded` with note "already joined (concurrent/prior); no-op re-open" — the
   pre-written row cannot be un-written, so it is terminalized TRUTHFULLY instead of
   masquerading as a fresh membership.
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
   - `join:` rows are repaired PER ATTEMPT (per-attempt keys mean registry presence alone
     cannot bless every lingering row — a stale interrupted attempt must not flip
     `Succeeded` because a LATER retry joined): registry present → `Succeeded` ONLY for the
     NEWEST `Started` attempt for that fed whose `created_at_ms` ≤ the registry row's
     `joined_at` CONVERTED to millis — `FederationInfo.joined_at` is unix SECONDS
     (`journal.rs:82`), so compare against `joined_at * 1000` (+60_000ms slack; both stamps
     come from the same device clock; an unconverted compare would never match and every
     crash-interrupted successful join would be mis-routed to soft-failure). When MORE THAN
     ONE `Started` attempt predates `joined_at` (overlapping `join` processes — the CLI is
     single-writer by convention but repair must not corrupt if that breaks), the newest is
     NOT auto-blessed as certain: it goes soft-`Succeeded` WITH an ambiguity note
     ("overlapping attempts; correlation uncertain — membership itself is registry-proven"),
     and the rest → soft-`Failed("superseded by a later join attempt")`; all writes are
     soft, so any authoritative writer still wins. With exactly one candidate, it is
     soft-`Succeeded` without the note; every OTHER `Started` join row for that fed →
     soft-`Failed("superseded by a later join attempt")`.
     The timestamp window only ARBITRATES between multiple candidates — it never overrides
     the registry: if the registry contains the fed but NO `Started` attempt falls inside
     the window (backward clock jump stamped the row after `joined_at`), the NEWEST
     attempt is still soft-`Succeeded` with the ambiguity note — membership is
     registry-proven, and a crash-interrupted join usually has no later authoritative
     writer to rescue a false `Failed`.
     Registry absent (and > 1h old) →
     soft-`Failed("join did not complete — federation not in the registry; re-run join")`.
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
     op's `custom_meta`; the hash, written durably pre-call, is the recovery link. The
     crash-before-call and crash-after-deduped-call windows are DURABLY INDISTINGUISHABLE
     (nothing is written between the hash update and the SDK call), so a hash match is
     adopted with the ambiguity RECORDED, not papered over. Branch on the matched op's
     state: outcome already TERMINAL → soft-terminal (`repaired: true`) with that outcome
     and a note — "correlated by payment hash to an existing payment of this invoice;
     attempt-level correlation uncertain (deduped retry or never-sent attempt); the matched
     operation is authoritative". Matched op still IN FLIGHT (the crash-after-
     `AlreadyInFlight` case) → adopt the op id and move the row to `Awaiting` (with the
     same note) — no terminal outcome exists yet; the normal `Awaiting` repair path
     observes the final send result on a later pass.
     Money-truth is exact either way (the invoice is paid once; op-id grouping keeps sums
     single-counted); only the attempt attribution is uncertain, and the row says so.
     Still nothing (and > 1h old, per the repair principle) →
     soft-`Failed("never reached the federation")` — truthful at ATTEMPT granularity (a
     no-hash row was malformed or crashed pre-parse).
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
  receive fee is exact ON `Succeeded` ROWS ONLY (elsewhere it is a quote too — §2.3/§7),
  the send fee is a pay-time estimate until the SDK exposes the final contract cost — one
  collapsed "fees" number would present a quote as exact. Filters apply
  before `--limit`. `--json`: one serde_json `OperationRecord` per line (JSONL), no tab
  table.
- `show` prints the full record multi-line (both op ids, gateway, fee breakdown, timestamps,
  error, linked intent status read live from the journal); `--json` = the raw record.
- Both are read-only and never touch the network (journal scans only) — they must work
  offline. Diagnostics to stderr as everywhere else (ADR-0023).

## 12. Build order

1. §1 scorer + §4 allocator + §5 dead-surface + §15.3 eligibility (pure; independently
   landable).
2. §15.1 signal trust + §15.4 send-rejection classification + §15.5 + §15.6 (the
   evacuation-liveness bundle) + §15.2 perform-time cap.
3. §6 newtype moves (mechanical; unblocks §7).
4. §7 ledger types + §8 `Intent`/`apply` extension (pure; all suites mechanically updated).
5. §2.3 quote persistence + §3 strand handling (`MoveRecord` fields land here) + §15.7
   TOCTOU verification + §15.10 solve-loop extraction.
6. §9 journal ledger integration (goldens on `advance` matrix + MemDatabase suites).
7. §10 recording + repair; §11 CLI verbs; §15.8 partial-open + §15.9 tick deadline + §15.11.
8. §13 smoke (written, run by hand on the two-fed harness).

## 13. Tests / exit gate

- **rb-lite gate (fast):** compile + clippy `-D warnings` + fmt + ALL goldens: scorer floor
  cases (§1), reservation + tie-break (§4), `advance` transition matrix + terminal
  immutability + one-row-per-key (§7/§9), `status_from_intent`/`kind_from_action`, strand
  goldens (§3), fee arithmetic (§2), and the §15 suites (corroboration table 15.1, cap
  refusal/downsize 15.2, evacuation eligibility 15.3, send-rejection classification 15.4,
  Retryable-vs-Permanent over-cap 15.5, gateway scan 15.6, TOCTOU mismatch 15.7, tick
  timeout 15.9, solve-loop quote-stream goldens 15.10). MemDatabase journal suites: same-dbtx
  atomicity, seq monotonicity + ordering, replay-does-not-duplicate, poison tolerance of
  ledger scans. The misbehaving-gateway (Stranded) case is covered at the EXECUTOR level with
  a mock gateway double (§3.5's golden); a live adversarial-gateway harness stays deferred to
  Phase 8's threat-model pass — tracked, not forgotten.
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

---

# Part III — 2026-07-05 review absorption (4.A continued)

The [fresh-eyes review](./reviews/2026-07-05-fresh-eyes-review.md) (five independent passes +
8 adversarial verification passes; every claim below verified against code and the SDK pin)
found six P1s. §4.2's reservation already covers the planning half of the cap problem; the
rest lands here. Priority order per the review: 15.1 → 15.2 → 15.3/15.4 → 15.5/15.6 → 15.7+.

## 15. Review fixes

### 15.1 Shutdown-signal trust (`probe.rs`) — review P1-1
`get_meta_expiration_timestamp` reads the MERGED meta: the default `LegacyMetaSource` lets
the federation's `meta_override_url` host OVERWRITE consensus values
(`fedimint-client-module/src/meta.rs:87`) and a failed override fetch blocks caching any new
values (`meta.rs:85`). So today's PRIMARY evacuation trigger is forgeable and blockable by a
single non-quorum party, uncorroborated — while the SECONDARY `/status` signal already
requires f+1 peers.

1. Treat the merged-meta expiry as UNTRUSTED input: derive `shutdown_scheduled` from it ONLY
   when corroborated by at least one of — (a) the same key present in the at-join consensus
   config meta (`client.config().global.meta` — catches feds that declared an expiry from the
   start; NOT sufficient alone for post-join announcements, it is a cached snapshot), (b) the
   meta MODULE's consensus value when the federation runs one (fresh AND consensus-backed),
   (c) the existing f+1-corroborated `/status.scheduled_shutdown` signal.
2. An uncorroborated override-only expiry: `tracing::warn!` (observability — the operator can
   investigate), does NOT trigger evacuation.
3. Fix the "consensus-backed" doc comments on `ProbeResult.expiry_timestamp_secs` and
   `derive_shutdown_scheduled` — they are wrong today.
4. Goldens (pure): corroboration table over (override_expiry, config_expiry, meta_module,
   status_quorum) — override-only never schedules; each corroborator alone + override does.

### 15.2 Per-fed cap enforced at PERFORM time (`executor.rs`, `runtime.rs`, `wallet-cli`) — review P1-2
§4.2's reservation fixes same-tick joint sizing, but the snapshot can be stale by perform
time, and the operator verbs (`do_move`/`direct_inflow`) consult NO cap at all — ADR-0018
says the cap "must be ENFORCED", and today nothing enforces it at the moment money moves.

1. `FedimintExecutor` gains `hard_cap: Option<Msat>` (constructor parameter; `None` disables).
2. In the `CreateInvoice` arm, BEFORE minting: `let dest = self.mc.balance(&rec.to)?;
   if dest + rec.amount > cap → ExecError::Permanent("destination would exceed the per-fed
   cap (<dest>+<amount> > <cap>)")`. For an `Evacuate`, downsize instead (extend
   `size_fresh_evacuation`'s clamp with `cap − dest`) — an evacuation must drain what fits,
   not refuse.
3. `wallet-cli` wires `DEFAULT_PER_FED_CAP` (or the tick's `--cap` value) into the executor
   for ALL verbs; `--allow-over-cap` (operator verbs only) maps to `hard_cap: None` — an
   explicit override, never silence.
4. Goldens: over-cap direct-inflow refused pre-mint; evacuation downsized to cap room;
   `--allow-over-cap` passes.

### 15.3 Evacuation destinations respect the scorer (`tick.rs`, `probe.rs`, `wallet-core`) — review P1-3
The scorer's verdict gates only spending/standby designation; `safest_other`'s fallback will
evacuate an entire dying-fed balance into a scorer-REJECTED fed (e.g. a joined 1-of-1).

1. `FederationStatus` gains `eligible_to_fund: bool`. `build_snapshot` sets it from the
   scorer verdict it already computes per fed; `assemble_status` (probe-only callers) sets
   `false` — snapshot assembly is the only place the verdict exists.
2. `eligible_for_evacuation` additionally requires `fed.eligible_to_fund`.
3. Golden: a scorer-rejected fed with a live gateway is never chosen as an evacuation
   destination; with no vetted destination the decision degrades to `RefuseInflow` (existing
   modeled behavior).

### 15.4 Deterministic send rejections are Permanent (`multi_client.rs`, `executor.rs`) — review P1-4
`map_send_result` collapses every non-dedup `SendPaymentError` into one `anyhow` error and
the Pay arm blanket-maps it `Retryable` — so `InvoiceExpired` (`lnv2 lib.rs:548`),
`WrongCurrency` (`lib.rs:552-557`), `FederationNotSupported`, and fee/expiry-limit breaches
livelock forever (the invoice is never re-minted; the move — including an Evacuate — never
terminates).

1. `map_send_result` classifies deterministic `SendPaymentError` variants into a
   distinguishable error (e.g. `SendOutcome`-adjacent `SendRejection(String)` or a typed
   error the executor can match); transport/gateway-unreachable failures stay generic.
2. The Pay arm maps classified rejections to `ExecError::Permanent` (safe: `next_step` only
   routes to `Pay` when cache + exhaustive backfill show no send op exists — nothing is in
   flight). A fresh occurrence retries with a fresh invoice.
3. Belt: check `invoice.is_expired()` at the Pay step pre-call (the BOLT11 is already parsed
   there).
4. Goldens: classification table for `map_send_result`; executor test — expired-invoice move
   terminally fails with an actionable message instead of resetting to `Pending`.

### 15.5 Pay-step over-cap: Retryable when only the send re-quote spiked (`executor.rs`) — review P2-2
`executor.rs:793-795` returns `Permanent("fee over cap")` on one fresh quote. The receive
component is FIXED (invoice − amount, already cap-checked at CreateInvoice); only the send
re-quote fluctuates — a transient spike terminally fails an Evacuate and strands funds on a
dying fed. Change: `Permanent` only when the fixed receive quote ALONE exceeds the cap;
otherwise `Retryable("send fee quote over cap this attempt")` — 15.4's expiry backstop bounds
the retry horizon. Golden: receive-fits/send-spikes → Retryable; receive-alone-over →
Permanent.

### 15.6 Gateway selection scans and validates BOTH ends (`executor.rs`, `probe.rs`, `runtime.rs`) — review P2-3
`resolve_gateway`/the probe stop at `gateways().next()`; the SDK's own `select_gateway` scans
until responsive (`lnv2 lib.rs:481-487`). A stale first-registered gateway makes a healthy
fed unroutable (probe: `probed_ok=false` → can't fund, can't be an evacuation destination;
executor: default moves fail).

1. `resolve_gateway`: iterate the registered set; for send-required actions validate the
   candidate against BOTH `to` and `from` (`routing_info` serves both) and pick the first
   fully-valid gateway; receive-only actions validate against `to`.
2. Probe `gateway_available`: true iff ANY registered gateway validates (pinned-gateway path
   unchanged — a pin is a request for that exact gateway).
3. Golden/unit: first-gateway-dead second-alive → routable; gateway serving only `to` skipped
   for a move whose `from` needs it.

### 15.7 Never-over TOCTOU verification (`executor.rs`) — review P1-6 (mint window)
The SDK re-fetches `routing_info` inside `create_contract_and_fetch_invoice` and sizes the
contract with the FRESH gateway fee — a fee drop between our verified quote and the mint
commits a larger contract and the destination nets MORE than asked (a gateway can time this
deliberately). After `mc.receive` commits, read the op's committed
`contract.commitment.amount` (durable in `ReceiveOperationMeta`) and compare against
`grossed.contract_amount`: on mismatch, do NOT surface/pay the invoice — fail the intent
`Permanent("gateway receive fee changed between quote and mint; re-run")`. Safe: the invoice
is unpaid at that point (for a Move we are the only payer; a DirectInflow's invoice has not
been surfaced), and the orphaned receive op simply expires unclaimed. A retry (fresh
occurrence / re-run) re-quotes from scratch. The CLAIM-window drift (federation fee at claim
vs quote) cannot be refused — it is detected by the settled-amount read-back that the ledger's
§9.3 backfill already specifies; record delivered-vs-asked there.
Golden/unit: mocked mismatch → Permanent, no surfacing, no pay; match → proceeds.

### 15.8 Partial open fails loudly (`wallet-cli`, `runtime.rs`) — review P2-6
`open_all` is best-effort and everything downstream silently walks the open subset: `balance`
under-reports with exit 0, and `tick`/`status` build snapshots from the survivors (an
unpinned fed that fails to open vanishes from the universe; all-fail → empty plan, exit 0).

1. `balance`: print `<fed>: unavailable (failed to open)` for joined-but-unopened feds and
   exit non-zero when any exists (`total` line labeled `total (N/M federations)`).
2. `tick`: compare the joined registry against the open set BEFORE probing; any
   joined-but-unopened fed → stderr diagnostic + exit non-zero WITHOUT acting (a partial
   world-view must not drive money decisions — same doctrine as `missing_pinned_feds`).
   `status`: report the unopened feds as rows, exit non-zero.
3. Smoke assertion: a wallet with a corrupted fed partition makes `tick` exit non-zero.

### 15.9 Tick deadline (`runtime.rs`) — review P2-1
A tick blocks on `await_send`/`await_receive` (SDK long-polls up to 60 min/request) with no
deadline — one stalled gateway freezes probing and every other decision. Wrap each
`perform` in `tokio::time::timeout` (default 10 min, `--perform-timeout` to override);
timeout → the intent stays `Pending` (Retryable path) and the tick moves on; the summary
counts it. Unit: a never-resolving executor future times out and leaves the intent Pending.

### 15.10 Solve-loop extraction + goldens (`executor.rs`, `fee.rs`) — review P2-4 (debt)
`quote_receive_gross_up_with_gateway_fee` (verify passes + safe-under restatement + verified
bisection) is welded to `MultiClient` and has ZERO unit tests — three of the five commits
before this spec's update patched exactly this flow. Extract it generic over an async quote
closure (`impl AsyncFnMut(Msat) -> Result<Msat, ExecError>` or a boxed-future equivalent —
the same seam `largest_fitting_amount` already uses in sync form) and golden-test scripted
quote streams: stable, two-step oscillation, staircase converging on the last pass,
non-monotone (over below under), and a stream that changes between the pass loop and the
bisection. The same extraction makes the `CreateInvoice` hair-under path testable.

### 15.11 Small fixes bundled with the above (review P3s worth taking now)
- `load_or_generate_mnemonic`: use `load_decodable_client_secret_opt` — absent → generate;
  `Err` (corrupt row) → abort naming the decode failure (today's `if let Ok` masks corruption
  behind a misleading "already exists, cannot overwrite" abort; no silent regeneration is
  possible either way — the SDK refuses overwrite).
- `solve_gross_up`'s unsolvable message: distinguish the two `None` causes (ppm ≥ 100% vs
  doubling exhausted) instead of always blaming ppm.
- DirectInflow hair-under: commit `delivered` into the op's `MoveMeta.amount`
  unconditionally, not only when `send_required` (the field is documented as the honest
  crash-safe amount; today a receive-only safe-under fallback records the ask).
- `ExecutionSummary` gains a `retryable` counter (distinct from terminal `failed`) so
  schedulers can tell "left Pending" from "money-op failed".

## Scope guard / non-goals

ONLY Phase 4: no discovery (3.B), no watch loop/triggers (3.C), no UI, no on-chain peg-out,
no event-sourced transition log, no pruning, no recovery tooling for `Stranded` (the preimage
is persisted for it). Do not touch the fedimint pin, `MoveMeta`, or the Move/DirectInflow
money logic beyond §2/§3. `cargo fmt` only on files changed.
