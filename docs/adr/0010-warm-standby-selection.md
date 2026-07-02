---
status: dropped
---
# Warm-standby selection: guardian-independence first (DROPPED — unfeasible in fedimint)

**DROPPED (2026-07).** This ADR made guardian/operator **independence** between the spending fed
and the warm standby a HARD constraint — the "sudden-death insurance." Phase 2 established it is
**unfeasible to verify in fedimint**, so the constraint is removed from the design and code.

## Why it can't be done
Verifying independence requires a stable per-guardian identity comparable ACROSS federations. The
authenticated client config exposes none:
- **Consensus pubkeys don't work.** `broadcast_public_keys` are freshly RANDOM per federation
  (generated at every config-gen ceremony; the federation id itself derives from them). Two feds run
  by the SAME operator never share pubkey bytes, so a pubkey-based overlap check ALWAYS reads
  "independent" and **fails OPEN** — a check that silently does nothing.
- **The api-endpoint URL is too weak.** It is the only cross-fed-stable signal the config carries,
  but it only catches an operator who reuses the same endpoint across feds; one advertising
  DIFFERENT hosts per fed still reads as independent. Best-effort, not a guarantee — a false comfort.

An honest "insurance" you cannot verify is worse than none: it invites concentration decisions on a
guarantee that isn't there.

## What changes
- Removed from the code: `GuardianId`, `FederationStatus.guardians`, `ReasonCode::NoIndependentStandby`,
  `allocator::shares_guardian` + its uses, and the probe's `guardian_ids` sourcing.
- **KEPT:** the STRUCTURAL guardian facts `guardian_count` / `threshold` — those ARE in the config and
  feasible; the scorer still uses the m-of-n strength as a resilience signal.
- The warm-standby SHAPE (ADR-0006: a spending fed + a distinct standby fed) stays; the standby is
  selected/funded by the other scorer signals (probe health, structural strength, Lnv2, …), just no
  longer gated on verified operator-independence. See the ADR-0006 note.

## If revisited
A robust operator-identity source — a signed operator identity, or an operator map from the
discovery/Observer layer (Phase 3) — could restore a real (not fail-open) independence signal. Until
such a source exists, do not reintroduce a pubkey- or single-URL-based independence gate.
