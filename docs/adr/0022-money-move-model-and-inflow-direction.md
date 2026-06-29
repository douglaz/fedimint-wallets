---
status: accepted
---
# The cross-federation MOVE is a first-class modeled protocol; inflow-direction is the primary lever

From a codex state review (2026-06-29) of the built pure core + the integration plan
([../integration-phase-plan.md](../integration-phase-plan.md)). The architecture
(ADR-0021) holds; the data model was written before the fedimint SDK reality and is
corrected here.

## Decisions

- **Ecash is not fungible across federations.** A move A→B is a protocol: B creates an
  invoice → A pays it (via a shared gateway) → B claims the incoming contract → possibly
  refund. The `Action` set must MODEL this, not hide it.
- **Split the allocator's outputs into executable money-moves vs advisory policy:**
  - Executable (executor intents): `DirectInflow { to }`, `Move { from, to, amount,
    fee_limit, occurrence }`, `Evacuate { from, to, amount, fee_limit, occurrence }`.
  - Advisory (NOT executor intents): `RefuseInflow` / `Cap`. The old `RefuseAllocation`
    is advisory policy (mutates receive routing), never an executor intent.
- **Inflow-direction is the cheap PRIMARY lever.** Directing the next incoming payment to
  the federation that needs funding is ~free; swapping existing balance costs gateway fees.
  Prove the receive + `DirectInflow` path before/with the swap, so the first allocator
  proves cheap allocation, not just expensive rebalance.
- **Idempotency lives at operation granularity, not per-Action.** The journal stores
  durable operation artifacts (operation IDs, invoice, payment hash, gateway pubkey,
  claim/refund state) and RESUMES the same invoice/payment on replay; it never restarts.
  The occurrence/epoch (T10) must land before the `SqliteJournal` schema hardens.
- **Real identities + structured balance.** `FederationId` = the 32-byte consensus hash;
  guardian-independence (ADR-0010) keys on real guardian identity (pubkeys/URLs), not local
  peer indices. `balance: Sats` → `{ spendable, in_flight, claimable, reserved_fee }` at
  msat granularity.
- **Spike before model.** Phase 1 leads with a throwaway devimint spike (drive one A→B move
  by hand, crash at every step) to LEARN the real operation state machine; the journal,
  `Action`/`Intent`, and executor are then modeled from what it taught (ADR-0021 Phase 1a→1c).

## Consequences

- These corrections land in Phase 1b (model-from-reality), after the 1a spike. Tracked as
  TODOS T12-T16.
- Key management shapes storage from day one: sketch seed/Keystore/Block Store/recovery
  (incl. recovery of PENDING operations, not just ecash — ADR-0003/0011) before client DBs.
- Honesty: after ADR-0006, v1 holds ~2 active federations; "allocates across many
  federations" is the candidate/discovery universe + a v2 promise, not the v1 active set.
