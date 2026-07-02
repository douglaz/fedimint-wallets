use crate::types::*;

/// Remaps today's decision logic into the executable/advisory `Action` vocabulary
/// (ADR-0022, T12): the CURRENT decision structure/thresholds are unchanged, only the
/// action shapes and the idempotency-key scheme differ.
///
/// `occurrence` (T10) is the caller's current allocation epoch; it is stamped into
/// every decision's key so a legitimately recurring decision (after a prior one
/// settled `Done`) produces a fresh key instead of being permanently skipped.
pub fn decide(snapshot: &AllocatorSnapshot, occurrence: Occurrence) -> Vec<AllocatorDecision> {
    let mut decisions = Vec::new();

    for fed in &snapshot.federations {
        if let Some(reason) = evacuation_reason(fed) {
            push_decision(
                &mut decisions,
                evacuate_decision(fed, reason, snapshot, occurrence),
            );
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
            let available = source.map_or(0, |s| s.balance.spendable.0);
            fund_into(
                snapshot,
                spending,
                source,
                available,
                want,
                FundKind::TopUp,
                occurrence,
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
            let available = source.map_or(0, |s| {
                s.balance
                    .spendable
                    .0
                    .saturating_sub(snapshot.target_spending_balance.0)
            });
            fund_into(
                snapshot,
                standby,
                source,
                available,
                want,
                FundKind::Standby,
                occurrence,
                &mut decisions,
            );
        }
    }

    decisions
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

    let cap_room = snapshot
        .per_fed_cap
        .0
        .saturating_sub(dest.balance.spendable.0);
    let amount = want.min(cap_room).min(available);
    if let Some(src) = source.filter(|_| amount > 0) {
        push_decision(
            out,
            move_decision(kind, src.id, dest.id, Msat(amount), snapshot, occurrence),
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

fn push_decision(out: &mut Vec<AllocatorDecision>, decision: AllocatorDecision) {
    if out
        .iter()
        .all(|existing| existing.idempotency_key != decision.idempotency_key)
    {
        out.push(decision);
    }
}

fn find(snapshot: &AllocatorSnapshot, id: FederationId) -> Option<&FederationStatus> {
    snapshot.federations.iter().find(|f| f.id == id)
}

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

fn cap_room(snapshot: &AllocatorSnapshot, fed: &FederationStatus) -> u64 {
    snapshot
        .per_fed_cap
        .0
        .saturating_sub(fed.balance.spendable.0)
}

/// True for a federation that is a safe evacuation TARGET: not itself evacuating,
/// not receive-blocked, and still below the hard per-fed cap. Used by
/// `evacuate_decision` to pick the destination; does not invent new ranking policy,
/// it reuses the same eligibility checks already used for funding decisions above.
fn eligible_for_evacuation(
    snapshot: &AllocatorSnapshot,
    fed: &FederationStatus,
    from: &FederationStatus,
) -> bool {
    fed.id != from.id
        && evacuation_reason(fed).is_none()
        && receive_blocker(fed).is_none()
        && cap_room(snapshot, fed) > 0
}

/// The safest eligible OTHER federation to evacuate `from` into: the configured
/// standby if it qualifies, else the first other eligible federation with cap room
/// in the snapshot. `None` if no eligible destination exists.
fn safest_other<'s>(
    snapshot: &'s AllocatorSnapshot,
    from: &FederationStatus,
) -> Option<&'s FederationStatus> {
    snapshot
        .standby_fed
        .and_then(|id| find(snapshot, id))
        .filter(|fed| eligible_for_evacuation(snapshot, fed, from))
        .or_else(|| {
            snapshot
                .federations
                .iter()
                .find(|fed| eligible_for_evacuation(snapshot, fed, from))
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
) -> AllocatorDecision {
    match safest_other(snapshot, from) {
        Some(to) => {
            let amount = Msat(from.balance.spendable.0.min(cap_room(snapshot, to)));
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
                requires_auth: false,
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
        requires_auth: false,
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
        requires_auth: false,
    }
}
