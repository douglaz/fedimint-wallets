use crate::conflict::GoalBlockers;
use crate::ledger::Actor;
use crate::types::*;
use std::collections::BTreeMap;

/// Remaps today's decision logic into the executable/advisory `Action` vocabulary
/// (ADR-0022, T12): the CURRENT decision structure/thresholds are unchanged, only the
/// action shapes and the idempotency-key scheme differ.
///
/// `occurrence` (T10) is the caller's current allocation epoch; it is stamped into
/// every decision's key so a legitimately recurring decision (after a prior one
/// settled `Done`) produces a fresh key instead of being permanently skipped.
///
/// **This entry point suppresses NOTHING.** It decides against an empty conflict projection, so a
/// goal a durable intent already owns comes back out under this occurrence's fresh key — the same
/// money moved twice. Every production path calls [`decide_with_blockers`] with the goals its
/// reconcile projected; `decide` remains for callers that have nothing in flight by construction,
/// which today means the pure decision-logic tests.
pub fn decide(snapshot: &AllocatorSnapshot, occurrence: Occurrence) -> Vec<AllocatorDecision> {
    decide_with_blockers(snapshot, occurrence, &GoalBlockers::default()).0
}

/// Which floor withheld a funding goal, so an operator can tell a protocol dust gap from a
/// pair whose economics do not (yet) justify the move.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DeferralFloor {
    /// lnv2's minimum incoming contract. Nothing but a bigger gap will clear it.
    ProtocolMinMove,
    /// The pair's `RouteEconomics::min_viable_amount` — the smallest net whose modelled cost
    /// still fits `max_fee_bps_of_move`. Clears if the gap grows, the route gets cheaper, or the
    /// proportional cap is raised.
    RouteMinViable,
}

/// A funding goal [`decide_with_blockers`] wanted to act on and withheld because the shortfall is
/// below the move floor.
///
/// This is DIAGNOSTIC ONLY. It is never admitted, never reserved, and deliberately never journaled
/// per tick — a refusal row every cycle for a gap not worth moving is exactly the noise the floor
/// exists to remove. It exists because the withholding is otherwise invisible: a standby only
/// drains when something spends from it, so a shortfall parked below the floor can stay there
/// forever while the wallet looks idle and healthy (br-0vg).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeferredFunding {
    pub dest: FederationId,
    pub source: Option<FederationId>,
    /// Why the goal existed: `SpendingBelowTarget` or `StandbyBelowTarget`.
    pub reason: ReasonCode,
    /// The shortfall the goal would have funded.
    pub want: Msat,
    /// The floor it failed to clear. Always `>= want`.
    pub floor: Msat,
    pub floor_source: DeferralFloor,
}

/// Everything one pass of the allocator produced, including the work it withheld.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AllocatorOutcome {
    /// What the tick may act on.
    pub decisions: Vec<AllocatorDecision>,
    /// Withheld because another durable intent already owns the logical goal (br-p93).
    pub suppressed: Vec<AllocatorDecision>,
    /// Withheld because the shortfall is below the move floor (br-0vg). Diagnostic only.
    pub deferred: Vec<DeferredFunding>,
}

/// Decide while withholding logical allocator goals that durable work already owns.
///
/// The second vector contains the withheld decisions for diagnostics. Filtering happens before
/// [`push_and_reserve`], so suppressed work cannot consume the intra-tick capacity needed by an
/// independent evacuation or funding move.
///
/// Use [`decide_with_diagnostics`] where the withheld-for-dust goals are also wanted; this
/// function's emitted and suppressed sets are byte-identical either way.
pub fn decide_with_blockers(
    snapshot: &AllocatorSnapshot,
    occurrence: Occurrence,
    blocked: &GoalBlockers,
) -> (Vec<AllocatorDecision>, Vec<AllocatorDecision>) {
    let outcome = decide_with_diagnostics(snapshot, occurrence, blocked);
    (outcome.decisions, outcome.suppressed)
}

/// [`decide_with_blockers`] plus the funding goals withheld by the move floor.
///
/// Adding a diagnostic channel must never change what the tick acts on, so the caller that
/// commits money keeps using the pair above and this one is read by `status`.
pub fn decide_with_diagnostics(
    snapshot: &AllocatorSnapshot,
    occurrence: Occurrence,
    blocked: &GoalBlockers,
) -> AllocatorOutcome {
    let mut decisions = Vec::new();
    let mut suppressed = Vec::new();
    let mut deferred = Vec::new();

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
            let suppression_refusal =
                evacuation_suppression_refusal(&decision, snapshot, &credited, &debited);
            if matches!(
                push_if_eligible_and_reserve(
                    &mut decisions,
                    &mut suppressed,
                    decision,
                    blocked,
                    occurrence,
                    &mut credited,
                    &mut debited,
                ),
                Admission::ConflictSuppressed
            ) {
                // A withheld evacuation reserves nothing and is not executable, but it must leave
                // the same durable audit fact as a withheld funding candidate.  This is built from
                // the pre-admission snapshot, before a suppressed decision could change either
                // reservation map.
                if let Some(refusal) = suppression_refusal {
                    push_decision(&mut decisions, refusal);
                }
            }
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
        let want = funding_shortfall(
            snapshot.target_spending_balance,
            spending.balance.spendable,
            snapshot.reservations.target_credit(spending.id),
            reserved(&credited, spending.id),
        );
        if evacuation_reason(spending).is_none() && want > 0 {
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
                blocked,
                &mut credited,
                &mut debited,
                &mut decisions,
                &mut suppressed,
                &mut deferred,
            );
        }
    }

    if let Some(standby) = snapshot.standby_fed.and_then(|id| find(snapshot, id)) {
        let want = funding_shortfall(
            snapshot.standby_target,
            standby.balance.spendable,
            snapshot.reservations.target_credit(standby.id),
            reserved(&credited, standby.id),
        );
        if evacuation_reason(standby).is_none() && want > 0 {
            let spending = snapshot.spending_fed.and_then(|id| find(snapshot, id));
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
                blocked,
                &mut credited,
                &mut debited,
                &mut decisions,
                &mut suppressed,
                &mut deferred,
            );
        }
    }

    AllocatorOutcome {
        decisions,
        suppressed,
        deferred,
    }
}

/// Pending reserved amount for `fed` in a per-tick `credited`/`debited` map (`0` when
/// absent). Keeps the reservation lookups saturating-friendly and readable.
fn reserved(map: &BTreeMap<FederationId, u64>, fed: FederationId) -> u64 {
    map.get(&fed).copied().unwrap_or(0)
}

/// The largest funding-move amount whose PROPORTIONAL fee cap still fits `budget`: the exact
/// integer maximum `amount` with `amount + floor(amount*bps/10000) <= budget`. Unlike the
/// former absolute-cap reservation (`budget - max_fee`), it is a fraction of budget, so the
/// saturation bug br-ljj.2 fixes is gone — though a tiny budget can still floor to 0 (a 1-msat
/// budget funds nothing).
///
/// The exact inverse, NOT the naive `floor(budget*10000/(10000+bps))`: that form undershoots
/// the true maximum by up to 1 msat, which — when the undershoot drops the result below
/// `min_move` — would fully refuse an otherwise-viable move. Brute-forced over budgets
/// `0..=40000` x bps `1..=10000`: this form is both overdraw-safe (never breaks the invariant)
/// AND maximal (`result + 1` always overdraws). `u128` intermediate so `(budget+1)*10000`
/// cannot overflow even at `u64::MAX`.
///
/// PUBLIC because the route-economics pricing pass must bound the amounts `fund_into` can emit
/// (`wallet-fedimint`'s `route_econ::maximum_funding_amount`): a published floor is only a valid
/// UPPER bound while the fee quotes behind it were taken at or above every amount the allocator
/// can actually move. A second copy of this expression that drifted would silently break that
/// bound in the under-blocking direction, so there is exactly one.
pub fn max_fundable(budget: u64, bps: u16) -> u64 {
    let denom = 10_000u128 + bps as u128;
    (((budget as u128 + 1) * 10_000 - 1) / denom) as u64
}

/// The proportional fee cap stamped on a funding `Move`: `amount * bps / 10000`. Paired with
/// [`max_fundable`] so `amount + move_fee_cap(amount) <= budget` holds under integer floors,
/// keeping the executor's `amount + fee_cap` spend within the source balance.
pub fn move_fee_cap(amount: Msat, bps: u16) -> Msat {
    Msat((amount.0 as u128 * bps as u128 / 10_000) as u64)
}

/// The remaining funding needed to reach `target` after all value that is already
/// wallet-delivered inbound is credited. `target_credit` is the durable snapshot
/// projection of pending `Move`/`Evacuate` deliveries; `same_round_credited` is
/// wallet-delivered work admitted earlier in this allocator pass. Keeping this arithmetic in
/// one helper prevents planning, route pricing, and commit-time validation from treating an
/// unpaid external receive as target value.
pub fn funding_shortfall(
    target: Msat,
    spendable: Msat,
    target_credit: Msat,
    same_round_credited: u64,
) -> u64 {
    target
        .0
        .saturating_sub(spendable.0)
        .saturating_sub(target_credit.0)
        .saturating_sub(same_round_credited)
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
    blocked: &GoalBlockers,
    credited: &mut BTreeMap<FederationId, u64>,
    debited: &mut BTreeMap<FederationId, u64>,
    out: &mut Vec<AllocatorDecision>,
    suppressed: &mut Vec<AllocatorDecision>,
    deferred: &mut Vec<DeferredFunding>,
) {
    // Source-side figures, shared by every refusal this function can emit. `source`-derived
    // fields are `None` exactly when there is no usable source. `max_fee` is NOT among them:
    // since br-ljj.2 every funding refusal below records it as `None` (see the sites), because
    // funding sizes off the proportional `max_fee_bps_of_move`, not the absolute cap.
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
            // The proportional bps that sized `available`, recorded so the funding refusal is
            // reconstructible from the row alone (br-nsx). A snapshot figure, present even here
            // where `source`/`available` are `None` (no usable source).
            max_fee_bps: Some(snapshot.max_fee_bps_of_move),
            cap_room: None,
            amount: None,
            conflict_suppressed: false,
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

    // ROUTE ECONOMICS (`docs/archive/route-economics-decisions.md` §Q5). The I/O layer prices each
    // ordered pair; `decide()` only READS the entry, staying pure:
    //  - ABSENT — permissive: the bare protocol `min_move` floor, i.e. exactly the behavior
    //    before route economics existed. Absence is transient (first tick, a failed quote RPC,
    //    an exhausted per-tick quote budget); blocking on it would stall every rebalance on any
    //    quote hiccup, and the proportional `fee_cap` is still the money backstop.
    //  - `Routable` — the floor rises to the pair's `min_viable_amount`.
    //  - `Unroutable` / `UneconomicAtAnySize` — emit NO move for this pair (see `route_blocked`).
    let route = source.and_then(|src| snapshot.route_economics_by_pair.get(&(src.id, dest.id)));
    // Never BELOW the protocol floor: `min_viable_amount` is IO-supplied, while lnv2's minimum
    // incoming contract is a protocol fact the allocator keeps enforcing itself either way.
    let floor = match route {
        Some(route) if route.status == RouteStatus::Routable => {
            route.min_viable_amount.0.max(snapshot.min_move.0)
        }
        _ => snapshot.min_move.0,
    };
    let route_blocked = matches!(
        route.map(|route| route.status),
        Some(RouteStatus::Unroutable | RouteStatus::UneconomicAtAnySize)
    );
    let uneconomic = route.map(|route| route.status) == Some(RouteStatus::UneconomicAtAnySize);

    // A shortfall below the move floor is DUST: the destination is effectively at target, and a
    // sub-floor move could only fail (lnv2's minimum-incoming-contract check, or the fee cap)
    // at perform time — every tick, forever (the 24h soak logged 91 such doomed moves). Silent
    // like the self-fund no-op: a refusal row every cycle for a gap that is not worth moving
    // would itself be the noise this floor removes. A `Routable` pair's economic floor is the
    // same kind of statement about the same gap, so it is silent for the same reason.
    //
    // `UneconomicAtAnySize` is the deliberate exception: §Q5 requires its standing
    // misconfiguration to remain visible rather than disappear behind the dust rule.
    if want < floor && !uneconomic {
        // Silent on the MONEY path, not silent to an operator who asks. `status` reads this;
        // nothing here is admitted, reserved, or journaled, so the dust rule is unchanged.
        deferred.push(DeferredFunding {
            dest: dest.id,
            source: source_id,
            reason: kind.reason(),
            want: Msat(want),
            floor: Msat(floor),
            floor_source: if floor > snapshot.min_move.0 {
                DeferralFloor::RouteMinViable
            } else {
                DeferralFloor::ProtocolMinMove
            },
        });
        return;
    }

    // Reservation-aware cap room: any inbound already committed to `dest` this pass is
    // subtracted, so a same-tick evacuation into it and this top-up cannot jointly
    // exceed the cap.
    let cap_room = cap_room_with(snapshot, dest, credited);
    let amount = want.min(cap_room).min(available);
    // Cap/available CRUMBS below the floor are equally unperformable, and a pair nothing can
    // route (or that no size can make economic) is unperformable at ANY amount: emit no move —
    // the shortfall refusals below still record WHY the destination stays underfunded.
    let amount = if route_blocked || amount < floor {
        0
    } else {
        amount
    };
    let suppression_candidate = if let Some(src) = source.filter(|_| amount > 0) {
        let candidate = move_decision(
            kind,
            src.id,
            dest.id,
            Msat(amount),
            snapshot,
            occurrence,
            route.and_then(|route| route.resolved_gateway.clone()),
        );
        if matches!(
            push_if_eligible_and_reserve(
                out,
                suppressed,
                candidate.clone(),
                blocked,
                occurrence,
                credited,
                debited,
            ),
            Admission::ConflictSuppressed
        ) {
            Some(candidate)
        } else {
            None
        }
    } else {
        None
    };
    let conflict_suppressed = suppression_candidate.is_some();
    let emitted_amount = if conflict_suppressed { 0 } else { amount };
    // Both refusals below share the full arithmetic. `amount` was clamped by `want` / `cap_room` /
    // `available`, then zeroed when it did not clear the finite route floor. `emitted_amount` also
    // accounts for conflict suppression: a nonzero move parked in `suppressed` was NOT emitted in
    // this pass, so its co-produced durable refusal reports 0 plus `conflict_suppressed = true`
    // rather than claim that money moved. `available` is `None` iff there was no source.
    let diagnostics = RefusalDiagnostics {
        source: source_id,
        want: Some(Msat(want)),
        available: source_available,
        source_spendable,
        // Funding sizing uses the proportional `max_fee_bps_of_move`, NOT the absolute cap, so
        // recording `max_fee` here would mislead; `available` already reflects the bps reserve.
        max_fee: None,
        // The proportional bps that sized `available`, recorded so the funding refusal is
        // reconstructible from the row alone (br-nsx). This struct is SHARED by the shortfall
        // refusal and the co-emitted funding-path over-cap refusal below, so both carry it —
        // both had their `available` sized by this bps.
        max_fee_bps: Some(snapshot.max_fee_bps_of_move),
        cap_room: Some(Msat(cap_room)),
        amount: Some(Msat(emitted_amount)),
        conflict_suppressed,
        // This public diagnostic retains its established meaning: the protocol minimum. A
        // route-economics deferral is deliberately silent; an indefinitely uneconomic route is
        // explained by its specific reason below.
        min_move: Some(snapshot.min_move),
    };
    // Surface the standing misconfiguration INSTEAD of the ordinary shortfall refusal (the
    // shortfall is fully explained by the route being uneconomic), including while the current
    // gap is protocol dust. The over-cap refusal is still pushed first when it applies: it is the
    // POPULATED twin of the figure-less row `decide()` already emitted for an externally over-cap
    // fed, and `push_decision`'s in-place replace only works if it is pushed.
    if uneconomic {
        if want > cap_room {
            push_decision(
                out,
                suppression_refusal_or_ordinary(
                    suppression_candidate.as_ref(),
                    dest.id,
                    ReasonCode::OverCap,
                    occurrence,
                    diagnostics,
                ),
            );
        }
        push_decision(
            out,
            suppression_refusal_or_ordinary(
                suppression_candidate.as_ref(),
                dest.id,
                ReasonCode::UneconomicRoute,
                occurrence,
                diagnostics,
            ),
        );
        return;
    }
    if want > cap_room {
        push_decision(
            out,
            suppression_refusal_or_ordinary(
                suppression_candidate.as_ref(),
                dest.id,
                ReasonCode::OverCap,
                occurrence,
                diagnostics,
            ),
        );
    }
    if conflict_suppressed || amount < want.min(cap_room) {
        push_decision(
            out,
            suppression_refusal_or_ordinary(
                suppression_candidate.as_ref(),
                dest.id,
                kind.reason(),
                occurrence,
                diagnostics,
            ),
        );
    }
}

/// Suppress a conflicting money decision before it can affect the allocator's local reservation
/// maps. The actor is always the allocator agent here; user and probe operations do not enter
/// `decide` and therefore cannot be suppressed by this seam.
#[allow(clippy::too_many_arguments)]
fn push_if_eligible_and_reserve(
    out: &mut Vec<AllocatorDecision>,
    suppressed: &mut Vec<AllocatorDecision>,
    decision: AllocatorDecision,
    blocked: &GoalBlockers,
    occurrence: Occurrence,
    credited: &mut BTreeMap<FederationId, u64>,
    debited: &mut BTreeMap<FederationId, u64>,
) -> Admission {
    if blocked.blocks_decision(&decision, Actor::Agent { occurrence }) {
        push_decision(suppressed, decision);
        Admission::ConflictSuppressed
    } else if push_and_reserve(out, decision, credited, debited) {
        Admission::Admitted
    } else {
        Admission::Duplicate
    }
}

/// The only outcomes of allocator admission. In particular, a duplicate idempotency key is not a
/// conflict suppression: callers which persist the latter as an operator diagnostic must not
/// derive it from a lossy boolean.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Admission {
    Admitted,
    ConflictSuppressed,
    Duplicate,
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
) -> bool {
    let reservation = match &decision.action {
        Action::Move {
            from,
            to,
            amount,
            fee_cap,
            ..
        }
        | Action::Evacuate {
            from,
            to,
            amount,
            fee_cap,
            ..
        } => Some((*from, *to, amount.0, fee_cap.0)),
        _ => None,
    };
    let pushed = push_decision(out, decision);
    if pushed {
        if let Some((from, to, amount, fee_cap)) = reservation {
            // SATURATING, not `+`. `EvacFeeCap::at` saturates by design and `Policy::validate`
            // puts no ceiling on `evac_fee_base_msat`, so a large-but-valid base yields a
            // `fee_cap` near `u64::MAX` and `amount + fee_cap` overflows — a panic in debug, and
            // in release a WRAP to a tiny number, which under-reserves and lets the same balance
            // be spent twice. Saturating fails the other way: the source reads as fully spoken
            // for and further decisions against it are refused, which is the safe direction for a
            // reservation.
            let credited_to = credited.entry(to).or_insert(0);
            *credited_to = credited_to.saturating_add(amount);
            let debited_from = debited.entry(from).or_insert(0);
            *debited_from = debited_from.saturating_add(amount.saturating_add(fee_cap));
        }
    }
    pushed
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

/// The advisory refusal for a withheld executable candidate must never share the ordinary
/// `(reason, fed, occurrence)` refusal key. A retry may already have persisted that ordinary row;
/// instead, name the exact candidate whose conflict made this zero-amount refusal necessary.
fn idem_conflict_suppressed(candidate: &IdempotencyKey, reason: ReasonCode) -> IdempotencyKey {
    IdempotencyKey(format!(
        "conflict-suppressed:{}:{}",
        candidate.0,
        reason_tag(reason)
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
        ReasonCode::UneconomicRoute => "uneconomic_route",
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
                    // An evacuation does NOT pre-reserve its fee cap (the executor sizes for it
                    // at perform time), so `available` here has no fee-cap term — record None
                    // rather than imply a subtraction that did not happen. It is also not the
                    // ABSOLUTE `max_fee` any more (ADR-0029): an evacuation is capped by
                    // `evac_fee_base_msat + evac_fee_bps`, so `max_fee` would name the wrong knob.
                    max_fee: None,
                    // `max_fee_bps` means `max_fee_bps_of_move` — the FUNDING-move rate. An
                    // evacuation is capped by `evac_fee_bps` instead, a different knob, so
                    // recording the funding rate here would misattribute this refusal (br-nsx).
                    // The evacuation's own components are on the emitted `Action::Evacuate`; this
                    // refusal emits no action, and its cause is a drained SOURCE (`available` 0)
                    // rather than any fee cap, so no cap figure describes it.
                    max_fee_bps: None,
                    cap_room: Some(Msat(cap_room)),
                    amount: Some(amount),
                    conflict_suppressed: false,
                    min_move: None,
                };
                return refuse_decision(from.id, reason, occurrence, diagnostics);
            }
            // The evacuation fee cap is BASE + PROPORTIONAL (ADR-0029), not the absolute
            // `snapshot.max_fee`: at the runbook's settings a full-balance drain costs ~27x that
            // cap, which either chunk-drains the federation or refuses it outright. Both
            // components are snapshotted onto the action so the executor can recompute the cap at
            // the net it ACTUALLY executes; the `fee_cap` stamped here is the same formula at the
            // PLANNED amount — the planning value, the per-tick reservation figure, and the
            // pre-execution bound.
            let fee_cap_components = EvacFeeCap {
                base_msat: snapshot.evac_fee_base_msat,
                bps: snapshot.evac_fee_bps,
            };
            AllocatorDecision {
                action: Action::Evacuate {
                    from: from.id,
                    to: to.id,
                    amount,
                    fee_cap: fee_cap_components.at(amount),
                    // Route economics NEVER gates an evacuation — a dying federation must be
                    // drained even when the route prices badly, so `status` is not consulted
                    // here and there is no floor. The pair's gateway is carried when it happens
                    // to have been priced (the funding pairs are), purely so `perform` can skip
                    // a re-scan; otherwise `None` and the executor resolves as it always has.
                    gateway: snapshot
                        .route_economics_by_pair
                        .get(&(from.id, to.id))
                        .and_then(|route| route.resolved_gateway.clone()),
                    fee_cap_components: Some(fee_cap_components),
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

/// The durable advisory twin of a nonzero evacuation withheld by conflict projection.  An
/// evacuation drains `from`, so its source-side availability is deliberately calculated without
/// either funding fee knob: execution sizes its own evacuation fee at perform time.
fn evacuation_suppression_refusal(
    decision: &AllocatorDecision,
    snapshot: &AllocatorSnapshot,
    credited: &BTreeMap<FederationId, u64>,
    debited: &BTreeMap<FederationId, u64>,
) -> Option<AllocatorDecision> {
    let Action::Evacuate {
        from, to, amount, ..
    } = &decision.action
    else {
        return None;
    };
    if amount.0 == 0 {
        return None;
    }
    let from = *from;
    let to = *to;
    let from_status = find(snapshot, from)?;
    let to_status = find(snapshot, to)?;
    let available = from_status
        .balance
        .spendable
        .0
        .saturating_sub(snapshot.reservations.outbound(from).0)
        .saturating_sub(reserved(debited, from));
    let diagnostics = RefusalDiagnostics {
        source: Some(from),
        want: None,
        available: Some(Msat(available)),
        source_spendable: Some(from_status.balance.spendable),
        // `max_fee` and `max_fee_bps` are funding knobs.  Evacuations use their own
        // base-plus-bps cap at perform time and do not pre-reserve it.
        max_fee: None,
        max_fee_bps: None,
        cap_room: Some(Msat(cap_room_with(snapshot, to_status, credited))),
        amount: Some(Msat(0)),
        conflict_suppressed: true,
        min_move: None,
    };
    Some(suppression_refusal_or_ordinary(
        Some(decision),
        from,
        decision.reason,
        decision.occurrence,
        diagnostics,
    ))
}

/// Produce an ordinary refusal unless this is the audit twin of a nonzero executable candidate
/// withheld by conflict projection. The latter is keyed by that candidate, rather than the generic
/// refusal key, so an ordinary refusal from an earlier retry cannot hide it.
fn suppression_refusal_or_ordinary(
    candidate: Option<&AllocatorDecision>,
    fed: FederationId,
    reason: ReasonCode,
    occurrence: Occurrence,
    diagnostics: RefusalDiagnostics,
) -> AllocatorDecision {
    let mut refusal = refuse_decision(fed, reason, occurrence, diagnostics);
    if let Some(candidate) = candidate {
        refusal.idempotency_key = idem_conflict_suppressed(&candidate.idempotency_key, reason);
    }
    refusal
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
    gateway: Option<GatewayUrl>,
) -> AllocatorDecision {
    AllocatorDecision {
        action: Action::Move {
            from,
            to,
            amount,
            // Proportional cap (br-ljj.2): scales with the move, and stays PURELY proportional
            // where `Evacuate` gained a base term (ADR-0029) — routine rebalancing can safely
            // decline and wait, so it needs no allowance for a small move's base fees. Sized to
            // fit the source budget by `max_fundable` above.
            fee_cap: move_fee_cap(amount, snapshot.max_fee_bps_of_move),
            // The pair's cheapest both-ends gateway, i.e. the exact route the `min_viable_amount`
            // floor above was computed against — so planning and perform agree on the economics.
            gateway,
        },
        reason: kind.reason(),
        occurrence,
        idempotency_key: idem_move(from, to, occurrence),
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;

    const A: FederationId = FederationId([0xAA; 32]);
    const B: FederationId = FederationId([0xBB; 32]);

    fn funding(key: &str, from: FederationId) -> AllocatorDecision {
        AllocatorDecision {
            action: Action::Move {
                from,
                to: B,
                amount: Msat(1),
                fee_cap: Msat(0),
                gateway: None,
            },
            reason: ReasonCode::StandbyBelowTarget,
            occurrence: Occurrence(1),
            idempotency_key: IdempotencyKey(key.to_owned()),
        }
    }

    #[test]
    fn admission_distinguishes_duplicate_keys_from_conflict_suppression() {
        let candidate = funding("move:candidate", A);
        let mut out = vec![candidate.clone()];
        let duplicate = push_if_eligible_and_reserve(
            &mut out,
            &mut vec![],
            candidate.clone(),
            &GoalBlockers::default(),
            Occurrence(1),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        );
        assert_eq!(duplicate, Admission::Duplicate);

        let held = funding("move:held", A);
        let mut blockers = GoalBlockers::default();
        blockers.hold_decision(
            &held,
            Actor::Agent {
                occurrence: Occurrence(1),
            },
        );
        let conflict = push_if_eligible_and_reserve(
            &mut vec![],
            &mut vec![],
            candidate,
            &blockers,
            Occurrence(1),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        );
        assert_eq!(conflict, Admission::ConflictSuppressed);
    }
}
