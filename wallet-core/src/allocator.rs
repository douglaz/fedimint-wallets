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
            // An externally-caused over-cap fed: no funding shortfall is computed here, so the
            // figures are empty. If this same fed is ALSO below target this pass, `fund_into`
            // emits an over-cap refusal under the same key WITH figures, and `push_decision`
            // lets that populated refusal replace this empty one.
            push_decision(
                &mut decisions,
                refuse_decision(
                    fed.id,
                    ReasonCode::OverCap,
                    occurrence,
                    RefusalDiagnostics::default(),
                ),
            );
        }
    }

    if let Some(spending) = snapshot.spending_fed.and_then(|id| find(snapshot, id)) {
        if evacuation_reason(spending).is_none()
            && spending.balance.spendable < snapshot.target_spending_balance
        {
            let want = snapshot.target_spending_balance.0 - spending.balance.spendable.0;
            let source = usable_source(snapshot.standby_fed.and_then(|id| find(snapshot, id)));
            // TopUp availability: the source budget is its spendable minus prior outbound and
            // same-tick debits; the move's own PROPORTIONAL fee cap is then reserved inside
            // `max_fundable` (`amount + amount*bps/10000 <= budget`), so the executor's
            // `amount + fee_cap` spend can never exceed the source balance — and a positive
            // budget never saturates `available` to zero (the old absolute-cap bug).
            let available = source.map_or(0, |s| {
                let budget = s
                    .balance
                    .spendable
                    .0
                    .saturating_sub(snapshot.reservations.outbound(s.id).0)
                    .saturating_sub(reserved(&debited, s.id));
                max_fundable(budget, snapshot.max_fee_bps_of_move)
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
            // PROPORTIONAL fee cap and any prior outbound are reserved inside `max_fundable`.
            let available = source.map_or(0, |s| {
                let budget = s
                    .balance
                    .spendable
                    .0
                    .saturating_sub(snapshot.target_spending_balance.0)
                    .saturating_sub(snapshot.reservations.outbound(s.id).0)
                    .saturating_sub(reserved(&debited, s.id));
                max_fundable(budget, snapshot.max_fee_bps_of_move)
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

/// The largest funding-move amount whose PROPORTIONAL fee cap still fits `budget`:
/// `amount + amount*bps/10000 <= budget`, i.e. `amount <= budget * 10000/(10000+bps)`. Unlike
/// the former absolute-cap reservation (`budget - max_fee`), a positive budget NEVER saturates
/// to zero — the result is a fraction of budget — which is the saturation bug br-ljj.2 fixes.
/// `u128` intermediate so a `per_fed_cap`-scale budget cannot overflow the `* 10000` multiply.
fn max_fundable(budget: u64, bps: u16) -> u64 {
    let denom = 10_000u128 + bps as u128;
    ((budget as u128 * 10_000) / denom) as u64
}

/// The proportional fee cap stamped on a funding `Move`: `amount * bps / 10000`. Paired with
/// [`max_fundable`] so `amount + move_fee_cap(amount) <= budget` holds under integer floors,
/// keeping the executor's `amount + fee_cap` spend within the source balance.
fn move_fee_cap(amount: Msat, bps: u16) -> Msat {
    Msat((amount.0 as u128 * bps as u128 / 10_000) as u64)
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
    // Source-side figures, shared by every refusal this function can emit. `source`-derived
    // fields are `None` exactly when there is no usable source; `max_fee` is a snapshot
    // constant, recorded even when sourceless so a cap-too-large refusal is legible.
    let source_id = source.map(|s| s.id);
    let source_spendable = source.map(|s| s.balance.spendable);
    let source_available = source.map(|_| Msat(available));

    if let Some(blocker) = receive_blocker(dest) {
        // Refused before cap room / the move amount are computed, so those stay `None`.
        let diagnostics = RefusalDiagnostics {
            source: source_id,
            want: Some(Msat(want)),
            available: source_available,
            source_spendable,
            // Funding sizing uses the proportional `max_fee_bps_of_move`, NOT the absolute cap, so
        // recording `max_fee` here would mislead; `available` already reflects the bps reserve.
        max_fee: None,
            cap_room: None,
            amount: None,
            min_move: Some(snapshot.min_move),
        };
        push_decision(
            out,
            refuse_decision(dest.id, blocker, occurrence, diagnostics),
        );
        return;
    }

    // A self-fund (the standby IS the spending fed) is a genuine no-op: there is
    // nothing to move, so skip it silently rather than recording a decision.
    if source.is_some_and(|src| src.id == dest.id) {
        return;
    }

    // A shortfall below the protocol move floor is DUST: the destination is effectively at
    // target, and a sub-floor move could only fail lnv2's minimum-incoming-contract check at
    // perform time — every tick, forever (the 24h soak logged 91 such doomed moves). Silent
    // like the self-fund no-op: a refusal row every cycle for a sub-5-sat gap would itself
    // be the noise this floor removes.
    if want < snapshot.min_move.0 {
        return;
    }

    // Reservation-aware cap room: any inbound already committed to `dest` this pass is
    // subtracted, so a same-tick evacuation into it and this top-up cannot jointly
    // exceed the cap.
    let cap_room = cap_room_with(snapshot, dest, credited);
    let amount = want.min(cap_room).min(available);
    // Cap/available CRUMBS below the floor are equally unperformable: emit no move — the
    // shortfall refusals below still record WHY the destination stays underfunded.
    let amount = if amount < snapshot.min_move.0 {
        0
    } else {
        amount
    };
    if let Some(src) = source.filter(|_| amount > 0) {
        push_and_reserve(
            out,
            move_decision(kind, src.id, dest.id, Msat(amount), snapshot, occurrence),
            credited,
            debited,
        );
    }
    // Both refusals below share the full arithmetic: `amount` (the emitted move, possibly 0)
    // was clamped by whichever of `want` / `cap_room` / `available` was smallest, and a reader
    // recovers WHICH from these figures alone. `available` is `None` iff there was no source.
    let diagnostics = RefusalDiagnostics {
        source: source_id,
        want: Some(Msat(want)),
        available: source_available,
        source_spendable,
        // Funding sizing uses the proportional `max_fee_bps_of_move`, NOT the absolute cap, so
        // recording `max_fee` here would mislead; `available` already reflects the bps reserve.
        max_fee: None,
        cap_room: Some(Msat(cap_room)),
        amount: Some(Msat(amount)),
        min_move: Some(snapshot.min_move),
    };
    if want > cap_room {
        push_decision(
            out,
            refuse_decision(dest.id, ReasonCode::OverCap, occurrence, diagnostics),
        );
    }
    if amount < want.min(cap_room) {
        push_decision(
            out,
            refuse_decision(dest.id, kind.reason(), occurrence, diagnostics),
        );
    }
}

/// Push a decision if its idempotency key is not already present; returns whether it was
/// actually pushed (a duplicate key is a silent no-op).
///
/// One refinement for refusals: the same fed can be refused for the SAME reason by two sites
/// in one pass under one key — the top-level over-cap check (empty figures) and `fund_into`'s
/// over-cap (full figures) when a fed is both over cap AND below target. When that happens,
/// the later populated refusal REPLACES the earlier empty one in place, so the richer figures
/// survive dedup. This only ever fires refusal-vs-refusal: refusal keys (`refuse:`) never
/// collide with move keys (`move:`/`evac:`), and refusals carry no reservation, so the
/// in-place replace cannot affect `push_and_reserve`'s bookkeeping.
fn push_decision(out: &mut Vec<AllocatorDecision>, decision: AllocatorDecision) -> bool {
    if let Some(existing) = out
        .iter_mut()
        .find(|existing| existing.idempotency_key == decision.idempotency_key)
    {
        if let (
            Action::RefuseInflow {
                diagnostics: incoming,
                ..
            },
            Action::RefuseInflow {
                diagnostics: present,
                ..
            },
        ) = (&decision.action, &existing.action)
        {
            if incoming.is_populated() && !present.is_populated() {
                *existing = decision;
            }
        }
        false
    } else {
        out.push(decision);
        true
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

/// Why a federation cannot RECEIVE credit (`fund_into` top-up/standby AND evacuation
/// destinations both consult this). `eligible_to_fund` gates every receive path (§15.3: never
/// direct money into a scorer-REJECTED fed — e.g. a joined 1-of-1 — nor, once the tick folds the
/// §5.1.3 probe gate into it, into an unproven `AutoJoined` fed). A merely scorer-ineligible or
/// probe-gated fed therefore surfaces `NotProbed` here — read it as "not fundable now", not
/// "never probed"; `active_probe`/`reasons` on the `status` view carry the finer distinction.
fn receive_blocker(fed: &FederationStatus) -> Option<ReasonCode> {
    (!fed.eligible_to_fund || !fed.probed_ok)
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
        .saturating_sub(snapshot.reservations.inbound(fed.id).0)
        .saturating_sub(reserved(credited, fed.id))
}

/// True for a federation that is a safe evacuation TARGET: not itself evacuating,
/// not receive-blocked, and still below the hard per-fed cap once this pass's pending inbound
/// is accounted for. Used by `evacuate_decision` to pick the destination; does not invent new
/// ranking policy, it reuses the same eligibility checks already used for funding decisions above.
fn eligible_for_evacuation(
    snapshot: &AllocatorSnapshot,
    fed: &FederationStatus,
    from: &FederationStatus,
    credited: &BTreeMap<FederationId, u64>,
) -> bool {
    fed.id != from.id
        && evacuation_reason(fed).is_none()
        && receive_blocker(fed).is_none()
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
        ReasonCode::ActiveProbe => "active_probe",
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
                .saturating_sub(snapshot.reservations.outbound(from.id).0)
                .saturating_sub(reserved(debited, from.id));
            let cap_room = cap_room_with(snapshot, to, credited);
            let amount = Msat(src_available.min(cap_room));
            if amount.0 == 0 {
                // `safest_other` only returns a destination with positive cap room
                // (`eligible_for_evacuation`), so `amount == 0` means the SOURCE has nothing
                // left to evacuate after its reservations — not that the destination is
                // capped. Record that: `available == 0` against a positive `cap_room` is the
                // whole story. `want`/`min_move` do not apply (an evacuation drains its source
                // rather than filling a target, and does not gate on the move floor).
                let diagnostics = RefusalDiagnostics {
                    source: Some(from.id),
                    want: None,
                    available: Some(Msat(src_available)),
                    source_spendable: Some(from.balance.spendable),
                    // An evacuation does NOT pre-reserve `max_fee` (the executor sizes for it
                    // at perform time), so `available` here has no fee-cap term — record None
                    // rather than imply a subtraction that did not happen.
                    max_fee: None,
                    cap_room: Some(Msat(cap_room)),
                    amount: Some(amount),
                    min_move: None,
                };
                return refuse_decision(from.id, reason, occurrence, diagnostics);
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
        // No safe destination exists to evacuate into: the condition is surfaced as an
        // advisory refusal with no shortfall arithmetic to record.
        None => refuse_decision(from.id, reason, occurrence, RefusalDiagnostics::default()),
    }
}

fn refuse_decision(
    fed: FederationId,
    reason: ReasonCode,
    occurrence: Occurrence,
    diagnostics: RefusalDiagnostics,
) -> AllocatorDecision {
    AllocatorDecision {
        // `diagnostics` is deliberately absent from `idem_refuse`: the recorded figures are
        // observational, so re-ticks of the same (fed, reason, occurrence) still dedup.
        action: Action::RefuseInflow {
            fed,
            reason,
            diagnostics,
        },
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
            // Proportional cap (br-ljj.2): scales with the move, unlike `Evacuate`'s absolute
            // `snapshot.max_fee`. Sized to fit the source budget by `max_fundable` above.
            fee_cap: move_fee_cap(amount, snapshot.max_fee_bps_of_move),
        },
        reason: kind.reason(),
        occurrence,
        idempotency_key: idem_move(from, to, occurrence),
    }
}
