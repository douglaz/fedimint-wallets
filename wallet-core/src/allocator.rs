use crate::types::*;

pub fn decide(snapshot: &AllocatorSnapshot) -> Vec<AllocatorDecision> {
    let mut decisions = Vec::new();

    for fed in &snapshot.federations {
        if let Some(reason) = evacuation_reason(fed) {
            push_decision(&mut decisions, evacuate_decision(fed.id, reason, snapshot));
        }
        // ADR-0018: a federation already over the per-fed cap (e.g. from an inbound
        // payment, not from our funding) is a cap violation the executor must reduce.
        if fed.balance > snapshot.per_fed_cap {
            push_decision(
                &mut decisions,
                refuse_decision(fed.id, ReasonCode::OverCap, snapshot),
            );
        }
    }

    if let Some(spending) = snapshot.spending_fed.and_then(|id| find(snapshot, id)) {
        if evacuation_reason(spending).is_none()
            && spending.balance < snapshot.target_spending_balance
        {
            let want = snapshot.target_spending_balance.0 - spending.balance.0;
            let source = usable_source(snapshot.standby_fed.and_then(|id| find(snapshot, id)));
            let available = source.map_or(0, |s| s.balance.0);
            fund_into(
                snapshot,
                spending,
                source,
                available,
                want,
                FundKind::TopUp,
                &mut decisions,
            );
        }
    }

    if let Some(standby) = snapshot.standby_fed.and_then(|id| find(snapshot, id)) {
        if evacuation_reason(standby).is_none() && standby.balance < snapshot.standby_target {
            let spending = snapshot.spending_fed.and_then(|id| find(snapshot, id));
            let independent = spending.is_none_or(|spending| !shares_guardian(spending, standby));
            if independent {
                let want = snapshot.standby_target.0 - standby.balance.0;
                let source = usable_source(spending);
                let available = source.map_or(0, |s| {
                    s.balance
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
                    &mut decisions,
                );
            } else {
                push_decision(
                    &mut decisions,
                    refuse_decision(standby.id, ReasonCode::NoIndependentStandby, snapshot),
                );
            }
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

fn fund_into(
    snapshot: &AllocatorSnapshot,
    dest: &FederationStatus,
    source: Option<&FederationStatus>,
    available: u64,
    want: u64,
    kind: FundKind,
    out: &mut Vec<AllocatorDecision>,
) {
    if let Some(blocker) = receive_blocker(dest) {
        push_decision(out, refuse_decision(dest.id, blocker, snapshot));
        return;
    }

    if source.is_some_and(|src| src.id == dest.id) {
        push_decision(
            out,
            refuse_decision(dest.id, ReasonCode::NoIndependentStandby, snapshot),
        );
        return;
    }

    let cap_room = snapshot.per_fed_cap.0.saturating_sub(dest.balance.0);
    let amount = want.min(cap_room).min(available);
    if let Some(src) = source.filter(|_| amount > 0) {
        push_decision(
            out,
            fund_decision(kind, src.id, dest.id, Msat(amount), snapshot),
        );
    }
    if want > cap_room {
        push_decision(out, refuse_decision(dest.id, ReasonCode::OverCap, snapshot));
    }
    if amount < want.min(cap_room) {
        push_decision(out, refuse_decision(dest.id, kind.reason(), snapshot));
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

/// True if `a` and `b` share any guardian. This is byte-exact `GuardianId` equality,
/// so it relies on the canonical-encoding invariant documented on `GuardianId`: every
/// fed in a snapshot must encode each guardian by the SAME canonical pubkey bytes, or
/// an overlap reads as independent and the warm-standby check fails open (ADR-0010).
fn shares_guardian(a: &FederationStatus, b: &FederationStatus) -> bool {
    a.guardians.iter().any(|g| b.guardians.contains(g))
}

fn idem(
    kind: &str,
    from: Option<&FederationId>,
    to: Option<&FederationId>,
    amount: u64,
) -> IdempotencyKey {
    // Stable per logical intent, with NO clock: a downstream executor must be able
    // to dedupe the same persistent intent across evaluation ticks for idempotent
    // replay (TODOS T2). Embedding `now` would re-key every tick and defeat that.
    // Same structure as before, now keyed on `hex(FederationId)` instead of a local
    // `u32`. The trailing numeric is the AMOUNT; the `Occurrence` epoch (§3) is not
    // yet folded into the key, so recurring identical intents still collide (TODOS T10).
    let f = from.map_or_else(|| "-".to_string(), FederationId::to_hex);
    let t = to.map_or_else(|| "-".to_string(), FederationId::to_hex);
    IdempotencyKey(format!("{kind}:{f}:{t}:{amount}"))
}

fn evacuate_decision(
    from: FederationId,
    reason: ReasonCode,
    s: &AllocatorSnapshot,
) -> AllocatorDecision {
    AllocatorDecision {
        action: Action::Evacuate { from, reason },
        reason,
        max_fee: s.max_fee,
        idempotency_key: idem("evacuate", Some(&from), None, 0),
        requires_auth: false,
    }
}

fn refuse_decision(
    fed: FederationId,
    reason: ReasonCode,
    s: &AllocatorSnapshot,
) -> AllocatorDecision {
    AllocatorDecision {
        action: Action::RefuseAllocation { fed, reason },
        reason,
        max_fee: s.max_fee,
        idempotency_key: idem(
            &format!("refuse:{}", reason_tag(reason)),
            None,
            Some(&fed),
            0,
        ),
        requires_auth: false,
    }
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
        ReasonCode::NoIndependentStandby => "no_independent_standby",
    }
}

fn fund_decision(
    kind: FundKind,
    from: FederationId,
    to: FederationId,
    amount: Msat,
    snapshot: &AllocatorSnapshot,
) -> AllocatorDecision {
    let (action, key_kind) = match kind {
        FundKind::TopUp => (Action::TopUpSpending { from, to, amount }, "topup"),
        FundKind::Standby => (Action::FundStandby { from, to, amount }, "standby"),
    };
    AllocatorDecision {
        action,
        reason: kind.reason(),
        max_fee: snapshot.max_fee,
        idempotency_key: idem(key_kind, Some(&from), Some(&to), amount.0),
        requires_auth: false,
    }
}
