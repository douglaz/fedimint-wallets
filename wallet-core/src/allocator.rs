use crate::types::*;
use std::collections::BTreeMap;

/// Remaps today's decision logic into the executable/advisory `Action` vocabulary
/// (ADR-0022, T12): the CURRENT decision structure/thresholds are unchanged, only the
/// action shapes and the idempotency-key scheme differ.
///
/// `occurrence` (T10) is the caller's current allocation epoch; it is stamped into
/// every decision's key so a legitimately recurring decision (after a prior one
/// settled `Done`) produces a fresh key instead of being permanently skipped.
pub fn decide(snapshot: &AllocatorSnapshot, occurrence: Occurrence) -> Vec<AllocatorDecision> {
    let mut decisions = Vec::new();

    // Per-tick reservation (§4.2): every decision in this pass is computed against ONE
    // immutable snapshot, so without bookkeeping two evacuations into the same
    // destination could jointly exceed `per_fed_cap`, and a source could be drained past
    // its balance by several moves. These local maps hold the pending inbound/outbound
    // per fed accumulated so far this pass; each emitted Move/Evacuate adds to them
    // (`credited[to] += amount`, `debited[from] += amount + fee_cap`) so later branches
    // see the already-committed effect. The `+ fee_cap` bound is conservative — actual
    // fees are unknowable at decide time but capped — which makes any number of
    // same-source moves provably non-overdrawing.
    let mut credited: BTreeMap<FederationId, u64> = BTreeMap::new();
    let mut debited: BTreeMap<FederationId, u64> = BTreeMap::new();

    for fed in &snapshot.federations {
        if let Some(reason) = evacuation_reason(fed) {
            let decision =
                evacuate_decision(fed, reason, snapshot, occurrence, &credited, &debited);
            push_and_reserve(&mut decisions, decision, &mut credited, &mut debited);
        }
        // ADR-0018: a federation already over the per-fed cap (e.g. from an inbound
        // payment, not from our funding) is a cap violation the executor must reduce.
        if fed.balance.spendable > snapshot.per_fed_cap {
            push_decision(
                &mut decisions,
                refuse_decision(fed.id, ReasonCode::OverCap, occurrence),
            );
        }
    }

    if let Some(spending) = snapshot.spending_fed.and_then(|id| find(snapshot, id)) {
        if evacuation_reason(spending).is_none()
            && spending.balance.spendable < snapshot.target_spending_balance
        {
            let want = snapshot.target_spending_balance.0 - spending.balance.spendable.0;
            let source = usable_source(snapshot.standby_fed.and_then(|id| find(snapshot, id)));
            // TopUp availability reserves the move's OWN `fee_cap` on the source plus any
            // outbound already committed this pass, so the executor's `amount + fee_cap`
            // spend can never exceed the source balance.
            let available = source.map_or(0, |s| {
                s.balance
                    .spendable
                    .0
                    .saturating_sub(reserved(&debited, s.id))
                    .saturating_sub(snapshot.max_fee.0)
            });
            fund_into(
                snapshot,
                spending,
                source,
                available,
                want,
                FundKind::TopUp,
                occurrence,
                &mut credited,
                &mut debited,
                &mut decisions,
            );
        }
    }

    if let Some(standby) = snapshot.standby_fed.and_then(|id| find(snapshot, id)) {
        if evacuation_reason(standby).is_none()
            && standby.balance.spendable < snapshot.standby_target
        {
            let spending = snapshot.spending_fed.and_then(|id| find(snapshot, id));
            let want = snapshot.standby_target.0 - standby.balance.spendable.0;
            let source = usable_source(spending);
            // The surplus floor STAYS — the spending fed is never drained below its
            // configured target to fund the standby — and, like TopUp, the move's own
            // `fee_cap` and any prior outbound are reserved on top.
            let available = source.map_or(0, |s| {
                s.balance
                    .spendable
                    .0
                    .saturating_sub(snapshot.target_spending_balance.0)
                    .saturating_sub(reserved(&debited, s.id))
                    .saturating_sub(snapshot.max_fee.0)
            });
            fund_into(
                snapshot,
                standby,
                source,
                available,
                want,
                FundKind::Standby,
                occurrence,
                &mut credited,
                &mut debited,
                &mut decisions,
            );
        }
    }

    decisions
}

/// Pending reserved amount for `fed` in a per-tick `credited`/`debited` map (`0` when
/// absent). Keeps the reservation lookups saturating-friendly and readable.
fn reserved(map: &BTreeMap<FederationId, u64>, fed: FederationId) -> u64 {
    map.get(&fed).copied().unwrap_or(0)
}

#[derive(Clone, Copy)]
enum FundKind {
    TopUp,
    Standby,
}

impl FundKind {
    fn reason(self) -> ReasonCode {
        match self {
            FundKind::TopUp => ReasonCode::SpendingBelowTarget,
            FundKind::Standby => ReasonCode::StandbyBelowTarget,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn fund_into(
    snapshot: &AllocatorSnapshot,
    dest: &FederationStatus,
    source: Option<&FederationStatus>,
    available: u64,
    want: u64,
    kind: FundKind,
    occurrence: Occurrence,
    credited: &mut BTreeMap<FederationId, u64>,
    debited: &mut BTreeMap<FederationId, u64>,
    out: &mut Vec<AllocatorDecision>,
) {
    if let Some(blocker) = receive_blocker(dest) {
        push_decision(out, refuse_decision(dest.id, blocker, occurrence));
        return;
    }

    // A self-fund (the standby IS the spending fed) is a genuine no-op: there is
    // nothing to move, so skip it silently rather than recording a decision.
    if source.is_some_and(|src| src.id == dest.id) {
        return;
    }

    // Reservation-aware cap room: any inbound already committed to `dest` this pass is
    // subtracted, so a same-tick evacuation into it and this top-up cannot jointly
    // exceed the cap.
    let cap_room = cap_room_with(snapshot, dest, credited);
    let amount = want.min(cap_room).min(available);
    if let Some(src) = source.filter(|_| amount > 0) {
        push_and_reserve(
            out,
            move_decision(kind, src.id, dest.id, Msat(amount), snapshot, occurrence),
            credited,
            debited,
        );
    }
    if want > cap_room {
        push_decision(
            out,
            refuse_decision(dest.id, ReasonCode::OverCap, occurrence),
        );
    }
    if amount < want.min(cap_room) {
        push_decision(out, refuse_decision(dest.id, kind.reason(), occurrence));
    }
}

/// Push a decision if its idempotency key is not already present; returns whether it was
/// actually pushed (a duplicate key is a silent no-op).
fn push_decision(out: &mut Vec<AllocatorDecision>, decision: AllocatorDecision) -> bool {
    if out
        .iter()
        .all(|existing| existing.idempotency_key != decision.idempotency_key)
    {
        out.push(decision);
        true
    } else {
        false
    }
}

/// Push a decision and, when it is a genuinely-new money move, record its per-tick
/// reservation: `credited[to] += amount`, `debited[from] += amount + fee_cap`. A move
/// that dedups against an existing key reserves nothing (it was already counted).
fn push_and_reserve(
    out: &mut Vec<AllocatorDecision>,
    decision: AllocatorDecision,
    credited: &mut BTreeMap<FederationId, u64>,
    debited: &mut BTreeMap<FederationId, u64>,
) {
    let reservation = match &decision.action {
        Action::Move {
            from,
            to,
            amount,
            fee_cap,
        }
        | Action::Evacuate {
            from,
            to,
            amount,
            fee_cap,
        } => Some((*from, *to, amount.0, fee_cap.0)),
        _ => None,
    };
    if push_decision(out, decision) {
        if let Some((from, to, amount, fee_cap)) = reservation {
            *credited.entry(to).or_insert(0) += amount;
            *debited.entry(from).or_insert(0) += amount + fee_cap;
        }
    }
}

fn find(snapshot: &AllocatorSnapshot, id: FederationId) -> Option<&FederationStatus> {
    snapshot.federations.iter().find(|f| f.id == id)
}

/// The usable funding source, if any. Source-side trust is INTENTIONALLY gated ONLY on
/// `evacuation_reason` (§4.3): draining a distrusted/shutting-down fed is desirable, so a
/// source is NOT gated on `probed_ok`/reputation — only credit DESTINATIONS are
/// (`receive_blocker`). We only refuse to source from a fed that is ITSELF evacuating,
/// because its balance is already spoken for by its own evacuation.
fn usable_source(source: Option<&FederationStatus>) -> Option<&FederationStatus> {
    source.filter(|fed| evacuation_reason(fed).is_none())
}

fn receive_blocker(fed: &FederationStatus) -> Option<ReasonCode> {
    (!fed.probed_ok)
        .then_some(ReasonCode::NotProbed)
        .or_else(|| (fed.reputation < 0).then_some(ReasonCode::LowReputation))
}

fn evacuation_reason(fed: &FederationStatus) -> Option<ReasonCode> {
    fed.shutdown_notice
        .then_some(ReasonCode::ShutdownNotice)
        .or_else(|| (!fed.healthy).then_some(ReasonCode::Unhealthy))
}

/// Reservation-aware cap room for `fed`: `per_fed_cap − spendable − credited[fed]`
/// (saturating). Subtracting the pending inbound already committed to `fed` this pass is
/// what stops two same-tick moves into it from jointly exceeding the cap.
fn cap_room_with(
    snapshot: &AllocatorSnapshot,
    fed: &FederationStatus,
    credited: &BTreeMap<FederationId, u64>,
) -> u64 {
    snapshot
        .per_fed_cap
        .0
        .saturating_sub(fed.balance.spendable.0)
        .saturating_sub(reserved(credited, fed.id))
}

/// True for a federation that is a safe evacuation TARGET: not itself evacuating,
/// not receive-blocked, scorer-eligible to fund (§15.3), and still below the hard per-fed
/// cap once this pass's pending inbound is accounted for. Used by `evacuate_decision` to
/// pick the destination; does not invent new ranking policy, it reuses the same
/// eligibility checks already used for funding decisions above.
fn eligible_for_evacuation(
    snapshot: &AllocatorSnapshot,
    fed: &FederationStatus,
    from: &FederationStatus,
    credited: &BTreeMap<FederationId, u64>,
) -> bool {
    fed.id != from.id
        && evacuation_reason(fed).is_none()
        && receive_blocker(fed).is_none()
        // §15.3: never drain a dying fed into a scorer-REJECTED destination (e.g. a
        // joined 1-of-1) even when it is reachable and has cap room.
        && fed.eligible_to_fund
        && cap_room_with(snapshot, fed, credited) > 0
}

/// The safest eligible OTHER federation to evacuate `from` into: the configured
/// standby if it qualifies, else — deterministically — the eligible federation with the
/// SMALLEST `FederationId` (§4.1 tie-break, so the choice is independent of
/// `snapshot.federations` order). `None` if no eligible destination exists.
fn safest_other<'s>(
    snapshot: &'s AllocatorSnapshot,
    from: &FederationStatus,
    credited: &BTreeMap<FederationId, u64>,
) -> Option<&'s FederationStatus> {
    snapshot
        .standby_fed
        .and_then(|id| find(snapshot, id))
        .filter(|fed| eligible_for_evacuation(snapshot, fed, from, credited))
        .or_else(|| {
            snapshot
                .federations
                .iter()
                .filter(|fed| eligible_for_evacuation(snapshot, fed, from, credited))
                .min_by_key(|fed| fed.id)
        })
}

fn idem_move(from: FederationId, to: FederationId, occurrence: Occurrence) -> IdempotencyKey {
    IdempotencyKey(format!(
        "move:{}:{}:{}",
        from.to_hex(),
        to.to_hex(),
        occurrence.0
    ))
}

fn idem_evac(from: FederationId, to: FederationId, occurrence: Occurrence) -> IdempotencyKey {
    IdempotencyKey(format!(
        "evac:{}:{}:{}",
        from.to_hex(),
        to.to_hex(),
        occurrence.0
    ))
}

fn idem_refuse(fed: FederationId, reason: ReasonCode, occurrence: Occurrence) -> IdempotencyKey {
    IdempotencyKey(format!(
        "refuse:{}:{}:{}",
        reason_tag(reason),
        fed.to_hex(),
        occurrence.0
    ))
}

fn reason_tag(reason: ReasonCode) -> &'static str {
    match reason {
        ReasonCode::SpendingBelowTarget => "spending_below_target",
        ReasonCode::StandbyBelowTarget => "standby_below_target",
        ReasonCode::ShutdownNotice => "shutdown_notice",
        ReasonCode::Unhealthy => "unhealthy",
        ReasonCode::OverCap => "over_cap",
        ReasonCode::NotProbed => "not_probed",
        ReasonCode::LowReputation => "low_reputation",
        ReasonCode::UserInitiated => "user_initiated",
        ReasonCode::StandingInstruction => "standing_instruction",
    }
}

/// A federation needing evacuation, but with no eligible destination in this
/// snapshot, or nothing to move (zero spendable balance): there is no money-move
/// the executor can do automatically, so this degrades to an advisory
/// `RefuseInflow` carrying the same reason rather than an unactionable `Evacuate`
/// with no `to`, or a no-op `Evacuate` of `0` (mirrors `fund_into`'s
/// `.filter(|_| amount > 0)` guard on the Move path).
fn evacuate_decision(
    from: &FederationStatus,
    reason: ReasonCode,
    snapshot: &AllocatorSnapshot,
    occurrence: Occurrence,
    credited: &BTreeMap<FederationId, u64>,
    debited: &BTreeMap<FederationId, u64>,
) -> AllocatorDecision {
    match safest_other(snapshot, from, credited) {
        Some(to) => {
            // Drain the source, reserving any prior same-tick outbound; the destination
            // clamp uses the reservation-aware cap room. UNLIKE the funding moves above,
            // an evacuation does NOT pre-reserve its own `fee_cap` (§4.2 refinement, found
            // by the live evacuate gate): the executor's `size_fresh_evacuation` sizes the
            // evacuation for affordability — fees included — at perform time, and a full
            // fee_cap reserve here would zero out (refuse) any evacuation of a balance at
            // or below the fee cap, abandoning exactly the small dying-fed balances this
            // decision exists to drain. Overdraw safety is preserved by the perform-time
            // sizing plus the conservative `amount + fee_cap` debit recorded below for any
            // subsequent same-tick move from this source.
            let src_available = from
                .balance
                .spendable
                .0
                .saturating_sub(reserved(debited, from.id));
            let amount = Msat(src_available.min(cap_room_with(snapshot, to, credited)));
            if amount.0 == 0 {
                return refuse_decision(from.id, reason, occurrence);
            }
            AllocatorDecision {
                action: Action::Evacuate {
                    from: from.id,
                    to: to.id,
                    amount,
                    fee_cap: snapshot.max_fee,
                },
                reason,
                occurrence,
                idempotency_key: idem_evac(from.id, to.id, occurrence),
            }
        }
        None => refuse_decision(from.id, reason, occurrence),
    }
}

fn refuse_decision(
    fed: FederationId,
    reason: ReasonCode,
    occurrence: Occurrence,
) -> AllocatorDecision {
    AllocatorDecision {
        action: Action::RefuseInflow { fed, reason },
        reason,
        occurrence,
        idempotency_key: idem_refuse(fed, reason, occurrence),
    }
}

fn move_decision(
    kind: FundKind,
    from: FederationId,
    to: FederationId,
    amount: Msat,
    snapshot: &AllocatorSnapshot,
    occurrence: Occurrence,
) -> AllocatorDecision {
    AllocatorDecision {
        action: Action::Move {
            from,
            to,
            amount,
            fee_cap: snapshot.max_fee,
        },
        reason: kind.reason(),
        occurrence,
        idempotency_key: idem_move(from, to, occurrence),
    }
}
