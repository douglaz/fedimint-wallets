//! The conflict model for logical allocator work (br-p93).
//!
//! Reconcile used to be gated wallet-wide: any retryable pending intent (standalone) or any
//! re-driven intent (daemon) suppressed EVERY allocator decision, so one permanently stuck move
//! on federation A froze rebalancing and evacuation for B, C and D. The caution behind that gate
//! was sound — running a tick while a prior-occurrence intent is still `Pending` risks re-issuing
//! the SAME work under a fresh idempotency key (a different key, the same money moved twice) —
//! but the blast radius was the whole wallet.
//!
//! This module narrows suppression to the allocator work that is actually at risk. Two pieces of
//! agent work conflict when they pursue the same LOGICAL GOAL. There is one asymmetric balance-
//! ownership edge: while `Evacuate(A)` is live, later allocator FUNDING moves touching A wait too.
//! Reservations cannot make these distinct logical goals commute: before send the strict/allocator
//! views hold the source, while after send the allocator view correctly absorbs that debit into the
//! live balance. A funding move can still alter either endpoint while the drain remains nonterminal.
//! The cross-goal edge therefore keeps the evacuation's source coherent across both phases. That
//! edge stops at funding: another federation's evacuation INTO A is never withheld.
//!
//! Identity deliberately excludes the occurrence, the amount, the route/gateway, the SOURCE of a
//! funding move, and the DESTINATION of an evacuation. Excluding the occurrence is the whole
//! point: a fresh occurrence is exactly how the same goal would otherwise be re-issued. Excluding
//! sizing and route means a re-planned variant of stuck work is still recognised as that work.
//! Excluding a funding move's source and an evacuation's destination keeps the goal ID from
//! serializing INDEPENDENT work: a pending evacuation of A into B does not block an evacuation of C
//! into B merely because they share B. Reservation projection and admission own that shared
//! destination (ADR-0024): even a Sending move retains its promised inbound amount. The asymmetric
//! edge above follows A, the held evacuation's SOURCE, and applies only to allocator FUNDING; it is
//! not a federation lock on user or probe IO, and not one on evacuation either.
//!
//! Nothing here blocks user operations, active-probe legs, `Join`/`Recover`, or advisory
//! refusals — none of them is allocator work that a later tick could duplicate.
//!
//! Two consequences of that identity are deliberate, and both were checked against the allocator
//! rather than assumed:
//!
//! **No evacuation is ever withheld except its own recurrence.** A pending `FundInto(A)` does not
//! block `Evacuate(A)`. The two can never be co-decided — [`crate::allocator::decide`] funds
//! neither the spending nor the standby federation while it has an evacuation reason, and
//! `usable_source` refuses such a federation as a source — so the only reachable ordering is a
//! funding move issued while A was healthy, followed by an evacuation decided once A started dying.
//! Blocking that evacuation would strand the dying federation's WHOLE balance behind a move that
//! may be stuck indefinitely; the alternative exposure is one in-flight chunk arriving late, which
//! the next tick's `Evacuate(A)` drains once this one is terminal. In the reverse ordering a live
//! `Evacuate(A)` DOES block later allocator FUNDING sourced from or sent into A: these are
//! different logical goals with overlapping balance effects even when the source debit has already
//! been absorbed into a live balance, and a deferred top-up costs nothing but latency. It does NOT block a different
//! federation's evacuation into A — `A` is only
//! a legal evacuation destination once its probe reports it healthy again, and stranding a second
//! dying federation to spare one extra hop would be the same trade, made the wrong way round.
//! Cancelling or superseding the stale funding intent remains `br-n8o`, not this projection.
//!
//! That deferral has a residual worth stating rather than discovering: [`crate::allocator::decide`]
//! funds only the two DESIGNATED federations (`usable_source(standby)` / `usable_source(spending)`),
//! so while an evacuation of either designation is live, both funding directions touch its source
//! and no allocator top-up is emitted at all. It lasts exactly as long as that intent does — a
//! cycle or two in the ordinary case — and it withholds only agent funding: user payments, probes,
//! and every other federation's evacuation keep running. An evacuation that retries forever
//! therefore parks rebalancing until something terminalizes it, which is `br-n8o`. Releasing the
//! edge once the held move has SENT would not buy that case out either: the structural refusal
//! that produces a forever-retrying evacuation is raised while the executor sizes it (the
//! `EvacuationSizing::Refused` arm), before any invoice or send, so the source spend such a
//! release would key on has not happened.
//!
//! **A second chunk of one goal waits for the first.** Since the amount is not part of the
//! identity, chunk 2 of a chunked evacuation — or a follow-up top-up while a partial one is in
//! flight — is withheld until chunk 1 is terminal. ADR-0024 forbids queueing behind ANOTHER
//! operation's IO; this queues a goal behind its own, which is precisely the duplicate the
//! projection exists to stop. Before send, reservations also suppress a recurrence; after Sending,
//! the allocator correctly stops subtracting money already absent from spendable, so a fresh-key
//! recurrence could otherwise size a SECOND drain from the remaining balance.
//!
//! The asymmetric edge above is the one place where agent work does wait on a DIFFERENT
//! operation's IO, and ADR-0024 carries the scope note for it. What that ADR was written to
//! protect is untouched: it is about admitted money IO, and here nothing is admitted-then-queued —
//! the agent simply does not DECIDE funding that would race a drain it cannot see reserved. No
//! user operation, no probe, and no evacuation ever waits, and no federation is locked.

use crate::executor::{Intent, IntentStatus};
use crate::ledger::Actor;
use crate::types::{Action, AllocatorDecision, FederationId, IdempotencyKey, ReasonCode};
use std::collections::BTreeSet;

/// The logical goal one piece of agent allocator work pursues. See the module docs for what is
/// deliberately NOT part of this identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AllocatorGoal {
    /// Bring `destination` up to its standing target (a `SpendingBelowTarget` /
    /// `StandbyBelowTarget` funding move). The source is not part of the goal: re-designation can
    /// legitimately change which federation funds it, and doing it twice is the duplicate.
    FundInto(FederationId),
    /// Drain `source` ahead of a shutdown/health problem. The destination is not part of the
    /// goal: `safest_other` can pick differently between ticks, and draining the same source
    /// twice is the duplicate.
    Evacuate(FederationId),
}

impl AllocatorGoal {
    /// The goal this action pursues, or `None` when it is not agent allocator work.
    ///
    /// `actor` is load-bearing: an operator's own `move`/`evacuate` is their call to make, and a
    /// user retry must never be suppressed because the agent has similar work in flight.
    ///
    /// The filter applies in the HOLDER direction too — a live user op holds no goal — and that is
    /// deliberate for the equality edge: a user `move` is not the agent's rebalance recurring, and
    /// strict user admission reserves its source outbound and destination inbound, so admission (not
    /// this projection) keeps the agent from overdrawing alongside it. The tokenized allocator view
    /// later absorbs a proven send debit while retaining the destination promise. There is no
    /// user-actor `Evacuate` today (no CLI or daemon verb builds one — the
    /// only user-actor fixtures are in tests), so the case is unreachable rather than handled. An
    /// operator-facing evacuate verb must revisit this filter before it lands.
    pub fn of(action: &Action, reason: ReasonCode, actor: Actor) -> Option<Self> {
        if !matches!(actor, Actor::Agent { .. }) {
            return None;
        }
        match action {
            // A `Move` is also the shape an active-probe leg takes, so the allocator reasons —
            // not the action alone — decide whether this is a funding goal.
            Action::Move { to, .. } => matches!(
                reason,
                ReasonCode::SpendingBelowTarget | ReasonCode::StandbyBelowTarget
            )
            .then_some(Self::FundInto(*to)),
            // Only `decide()` emits an agent `Evacuate`, always under `ShutdownNotice`/`Unhealthy`,
            // so the action alone identifies it.
            Action::Evacuate { from, .. } => Some(Self::Evacuate(*from)),
            Action::DirectInflow { .. }
            | Action::Pay { .. }
            | Action::Receive { .. }
            | Action::Join { .. }
            | Action::Recover { .. }
            | Action::RefuseInflow { .. } => None,
        }
    }

    pub fn of_intent(intent: &Intent) -> Option<Self> {
        Self::of(&intent.action, intent.reason, intent.actor)
    }

    pub fn of_decision(decision: &AllocatorDecision, actor: Actor) -> Option<Self> {
        Self::of(&decision.action, decision.reason, actor)
    }

    /// Whether this goal conflicts with a prospective agent allocator decision.
    ///
    /// This is the admission-side form of the durable-holder relation.  The
    /// scheduler's short-lived admission watermark uses it to compare a newly
    /// admitted goal with an older plan without reconstructing fake terminal
    /// intents or broadening the conflict projection.
    pub fn conflicts_with_decision(self, decision: &AllocatorDecision, actor: Actor) -> bool {
        Self::of_decision(decision, actor)
            .is_some_and(|candidate| self.conflicts_with(candidate, &decision.action))
    }

    /// Whether this HELD goal conflicts with one candidate allocator decision.
    ///
    /// Equality prevents duplicate logical work. The only cross-goal edge is deliberately
    /// asymmetric: a live evacuation owns its source balance against later allocator FUNDING
    /// touching that source. Two reverses stay allowed so that no evacuation is ever stranded —
    /// an old top-up does not hold up the evacuation of its destination, and a live evacuation
    /// does not hold up the evacuation of a DIFFERENT federation into its own source.
    fn conflicts_with(self, candidate: AllocatorGoal, action: &Action) -> bool {
        self == candidate
            || matches!(self, Self::Evacuate(source) if funding_move_touches(action, source))
    }

    /// The route-pricing form of [`Self::conflicts_with`] for a prospective funding move, before a
    /// sized decision or idempotency key exists.
    fn conflicts_with_funding_pair(self, source: FederationId, destination: FederationId) -> bool {
        match self {
            Self::FundInto(held_destination) => held_destination == destination,
            Self::Evacuate(held_source) => held_source == source || held_source == destination,
        }
    }
}

/// Whether `action` is a FUNDING move with `federation` at either end.
///
/// `Action::Evacuate` is deliberately not matched. A candidate evacuation is either the held
/// goal's own recurrence — which the equality edge already blocks — or a DIFFERENT dying
/// federation draining into this source, which must never be withheld: `safest_other` prefers the
/// standby, so a stuck `Evacuate(standby)` would otherwise hold another federation's whole balance
/// hostage for as long as it lives. That inflow is safe because `eligible_for_evacuation` only
/// picks a HEALTHY destination; at worst the stuck drain later carries it one hop further.
fn funding_move_touches(action: &Action, federation: FederationId) -> bool {
    matches!(
        action,
        Action::Move { from, to, .. } if *from == federation || *to == federation
    )
}

/// The logical goals durable, non-terminal work already owns, each with the key that owns it.
///
/// Within one actor scheduler cycle, ONE value carries the eligibility from the reconcile pass
/// that derived it through route pricing, planning and commit. The isolated standalone tick
/// re-derives the same model before planning and again before apply rather than carrying a
/// reconcile snapshot. An EMPTY set means "nothing is in flight"; a set is never a substitute for
/// a fail-closed scan — when eligibility is UNKNOWN, neither path may admit allocator work.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GoalBlockers {
    /// The conflict identity and its holder. This deliberately retains the old representation:
    /// neither endpoints excluded from [`AllocatorGoal`] nor route/sizing details may affect
    /// conflict or holder semantics.
    holders: BTreeSet<(AllocatorGoal, IdempotencyKey)>,
    /// Original sources of live agent rebalances, associated with the exact goal/key which held
    /// them. The deliberately narrow goal identity omits a funding source, but raw pin relaxation
    /// must not turn that into an uncorrelated set membership check: after a policy rotation,
    /// an old `A -> B` holder must not vouch for a fresh `C -> A` funding recurrence.
    ///
    /// This observational metadata must not affect goal identity, conflicts, or admission.
    holder_sources: BTreeSet<(AllocatorGoal, IdempotencyKey, FederationId)>,
}

impl GoalBlockers {
    /// Add every holder and its observational source metadata from another
    /// projection.
    ///
    /// Actor-issued eligibility snapshots use this to retain their mandatory
    /// durable baseline while also accepting conservative caller-supplied
    /// blockers.  Union can only suppress additional allocator work; it can
    /// never erase the actor's fail-closed view.
    pub fn extend(&mut self, other: &Self) {
        self.holders.extend(other.holders.iter().cloned());
        self.holder_sources
            .extend(other.holder_sources.iter().cloned());
    }

    /// Project the goals held by a durable `Pending`/`Executing` scan. Terminal intents hold
    /// nothing: `Done` work is finished (a recurrence is legitimate) and `Failed` work is not
    /// coming back without a manual retry. Any other status is treated as live — this is the
    /// fail-closed direction, and it keeps the projection correct for callers that pass a wider
    /// scan than `pending()`.
    ///
    /// Every caller passes `pending()`, which omits `Awaiting`. That is a COMPLETE view of
    /// allocator work rather than the partial one it looks like: both goal-bearing actions map to
    /// a `send_required` move plan, and the move driver returns `Awaiting` only from its
    /// `!send_required` branches, so an agent `Move`/`Evacuate` cannot hold that status. (The
    /// executor's other `Awaiting` returns are in its `Pay` and `Receive` arms, which never build
    /// a move plan and carry no goal at all.) A change that gave either goal-bearing action an
    /// awaiting phase must widen these scans (`reservation_intents()` is the sibling money
    /// projection's answer) or this suppression would silently stop seeing it.
    pub fn from_intents<'a>(intents: impl IntoIterator<Item = &'a Intent>) -> Self {
        let mut blockers = Self::default();
        for intent in intents
            .into_iter()
            .filter(|intent| !matches!(intent.status, IntentStatus::Done | IntentStatus::Failed))
        {
            if let Some(goal) = AllocatorGoal::of_intent(intent) {
                blockers
                    .holders
                    .insert((goal, intent.idempotency_key.clone()));
                blockers.record_holder_source(goal, &intent.idempotency_key, &intent.action);
            }
        }
        blockers
    }

    /// Record a decision this caller has just made durable, so the rest of ITS batch sees the goal
    /// as in flight. A decision carrying no goal (advisory, user, probe) is ignored.
    ///
    /// A durable scan taken before a batch cannot see what the batch itself journals, and the
    /// admission seams re-scan per BATCH, not per decision. Without this, one caller could commit
    /// two keys for the same goal in a single batch — the duplicate this projection exists to stop.
    pub fn hold_decision(&mut self, decision: &AllocatorDecision, actor: Actor) {
        if let Some(goal) = AllocatorGoal::of_decision(decision, actor) {
            self.holders
                .insert((goal, decision.idempotency_key.clone()));
            self.record_holder_source(goal, &decision.idempotency_key, &decision.action);
        }
    }

    /// Whether this decision is agent allocator work that conflicts with something in flight.
    ///
    /// There is deliberately no goal-only variant of this check. The cross-goal evacuation edge
    /// needs the candidate's ACTION, not just its goal, so a `(goal, key)` predicate would be a
    /// strictly weaker test wearing the same name — and every seam that suppresses work has a
    /// whole decision in hand.
    pub fn blocks_decision(&self, decision: &AllocatorDecision, actor: Actor) -> bool {
        self.blocking_holder(decision, actor).is_some()
    }

    /// The first live holder conflicting with `decision`, for eligibility and diagnostics.
    ///
    /// The candidate's own key is EXCLUDED: an intent cannot conflict with itself, and re-driving
    /// a still-`Pending` intent under its own key is the attach path, not a duplicate. Suppression
    /// is about a SECOND key for work already in flight.
    pub fn blocking_holder(
        &self,
        decision: &AllocatorDecision,
        actor: Actor,
    ) -> Option<&IdempotencyKey> {
        let candidate = AllocatorGoal::of_decision(decision, actor)?;
        self.holders
            .iter()
            .find(|(held, holder)| {
                *holder != decision.idempotency_key
                    && held.conflicts_with(candidate, &decision.action)
            })
            .map(|(_, holder)| holder)
    }

    /// Whether any live holder owns exactly `goal` from `source`.
    ///
    /// Goal identity deliberately ignores a funding source; this association is only for the
    /// raw-pin voucher and is therefore intentionally stricter than conflict identity.
    pub fn holds_goal_from(&self, goal: AllocatorGoal, source: FederationId) -> bool {
        self.holder_sources
            .iter()
            .any(|(held, _, held_source)| *held == goal && *held_source == source)
    }

    /// Whether a live holder conflicting with `decision` is associated with `current_source`.
    ///
    /// This examines every holder, rather than relying on [`Self::blocking_holder`]'s diagnostic
    /// first match, and excludes the candidate's own key just like admission does.
    pub fn has_blocking_holder_from(
        &self,
        decision: &AllocatorDecision,
        actor: Actor,
        current_source: FederationId,
    ) -> bool {
        let Some(candidate) = AllocatorGoal::of_decision(decision, actor) else {
            return false;
        };
        self.holders.iter().any(|(held, holder)| {
            *holder != decision.idempotency_key
                && held.conflicts_with(candidate, &decision.action)
                && self
                    .holder_sources
                    .contains(&(*held, holder.clone(), current_source))
        })
    }

    /// Whether a prospective allocator funding pair is blocked before route-quote I/O. No candidate
    /// key exists yet, so this cannot exclude a holder's own key. That is conservative: at worst a
    /// pair owned by the current occurrence goes unpriced for one cycle and uses the normal
    /// permissive fallback. Same-goal funding blocks its destination; a live evacuation blocks
    /// either direction touching its source.
    pub fn blocks_funding_pair(&self, source: FederationId, destination: FederationId) -> bool {
        self.holders
            .iter()
            .any(|(held, _)| held.conflicts_with_funding_pair(source, destination))
    }

    /// The first key holding `goal`, for diagnostics.
    pub fn holder(&self, goal: AllocatorGoal) -> Option<&IdempotencyKey> {
        self.holders
            .iter()
            .find(|(held, _)| *held == goal)
            .map(|(_, holder)| holder)
    }

    pub fn is_empty(&self) -> bool {
        self.holders.is_empty()
    }

    pub fn len(&self) -> usize {
        self.holders.len()
    }

    /// Every held goal, deduplicated. Diagnostics only.
    pub fn goals(&self) -> BTreeSet<AllocatorGoal> {
        self.holders.iter().map(|(goal, _)| *goal).collect()
    }

    fn record_holder_source(&mut self, goal: AllocatorGoal, key: &IdempotencyKey, action: &Action) {
        if let Action::Move { from, .. } | Action::Evacuate { from, .. } = action {
            self.holder_sources.insert((goal, key.clone(), *from));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Msat, Occurrence};

    const A: FederationId = FederationId([0xAA; 32]);
    const B: FederationId = FederationId([0xBB; 32]);
    const C: FederationId = FederationId([0xCC; 32]);

    fn agent(occurrence: u64) -> Actor {
        Actor::Agent {
            occurrence: Occurrence(occurrence),
        }
    }

    fn decision(
        key: &str,
        action: Action,
        reason: ReasonCode,
        occurrence: u64,
    ) -> AllocatorDecision {
        AllocatorDecision {
            action,
            reason,
            occurrence: Occurrence(occurrence),
            idempotency_key: IdempotencyKey(key.to_owned()),
        }
    }

    fn funding(from: FederationId, to: FederationId, amount: u64) -> Action {
        Action::Move {
            from,
            to,
            amount: Msat(amount),
            fee_cap: Msat(amount / 100),
            gateway: None,
        }
    }

    fn evacuation(from: FederationId, to: FederationId, amount: u64) -> Action {
        Action::Evacuate {
            from,
            to,
            amount: Msat(amount),
            fee_cap: Msat(amount / 100),
            gateway: None,
            fee_cap_components: None,
        }
    }

    fn intent(decision: &AllocatorDecision, actor: Actor, status: IntentStatus) -> Intent {
        Intent {
            status,
            ..Intent::from_decision(decision, actor, 0)
        }
    }

    fn blockers(intents: &[Intent]) -> GoalBlockers {
        GoalBlockers::from_intents(intents)
    }

    #[test]
    fn the_same_funding_goal_is_blocked_across_occurrence_sizing_and_source() {
        let stuck = intent(
            &decision(
                "move:old",
                funding(A, B, 100_000),
                ReasonCode::StandbyBelowTarget,
                4,
            ),
            agent(4),
            IntentStatus::Pending,
        );
        let held = blockers(std::slice::from_ref(&stuck));

        // A fresh occurrence, a different size, a different SOURCE and the other funding reason
        // are all the same logical goal: top federation B up. `move:resized` is the funding half
        // of the chunking consequence — an incremental top-up of B waits for the in-flight one.
        for candidate in [
            decision(
                "move:new",
                funding(A, B, 100_000),
                ReasonCode::StandbyBelowTarget,
                5,
            ),
            decision(
                "move:resized",
                funding(A, B, 7),
                ReasonCode::StandbyBelowTarget,
                5,
            ),
            decision(
                "move:resourced",
                funding(C, B, 100_000),
                ReasonCode::SpendingBelowTarget,
                5,
            ),
        ] {
            assert!(
                held.blocks_decision(&candidate, agent(5)),
                "{:?} duplicates the stuck goal",
                candidate.idempotency_key
            );
        }

        // Independent destinations stay fundable, and the holder's own key still attaches.
        assert!(!held.blocks_decision(
            &decision(
                "move:other-dest",
                funding(A, C, 100_000),
                ReasonCode::StandbyBelowTarget,
                5
            ),
            agent(5)
        ));
        assert!(!held.blocks_decision(
            &decision(
                "move:old",
                funding(A, B, 100_000),
                ReasonCode::StandbyBelowTarget,
                4
            ),
            agent(4)
        ));
        assert!(held.blocks_funding_pair(A, B));
        assert!(!held.blocks_funding_pair(A, C));

        // A pending funding move into B does NOT block EVACUATING B, deliberately. The allocator
        // cannot decide both in one pass (it never funds a federation carrying an evacuation
        // reason), so the only ordering this covers is "B was topped up while healthy, then
        // started dying" — and withholding the drain there would strand B's WHOLE balance behind
        // a move that may never terminalize, to avoid one in-flight chunk landing late. See the
        // module docs; superseding the stale funding intent is br-n8o.
        assert!(!held.blocks_decision(
            &decision(
                "evac:b",
                evacuation(B, C, 100_000),
                ReasonCode::ShutdownNotice,
                5
            ),
            agent(5)
        ));
    }

    #[test]
    fn the_same_evacuation_source_is_blocked_but_a_shared_destination_is_not() {
        let stuck = intent(
            &decision(
                "evac:old",
                evacuation(A, B, 500_000),
                ReasonCode::ShutdownNotice,
                4,
            ),
            agent(4),
            IntentStatus::Executing,
        );
        let held = blockers(std::slice::from_ref(&stuck));

        // Same source, fresh occurrence, different size, different destination, different reason.
        // The size case IS the chunking case the module docs call out: a 1 msat follow-up drain of
        // A waits for the 500_000 chunk to terminalize. Before send the outbound reservation also
        // suppresses it; after Sending absorbs that debit, this goal fence still prevents a fresh
        // key from sizing a second drain from the remaining balance.
        assert!(held.blocks_decision(
            &decision("evac:new", evacuation(A, C, 1), ReasonCode::Unhealthy, 5),
            agent(5)
        ));
        // A DIFFERENT dying federation draining into the same destination is independent work —
        // reservations and admission own that shared destination, not this projection.
        assert!(!held.blocks_decision(
            &decision(
                "evac:independent",
                evacuation(C, B, 500_000),
                ReasonCode::ShutdownNotice,
                5
            ),
            agent(5)
        ));
        // Funding that touches the live evacuation SOURCE waits, in either direction. A funding
        // pair sharing only its destination remains independent.
        assert!(held.blocks_funding_pair(A, B));
        assert!(held.blocks_funding_pair(C, A));
        assert!(!held.blocks_funding_pair(C, B));
        assert_eq!(
            held.holder(AllocatorGoal::Evacuate(A))
                .map(|key| key.0.as_str()),
            Some("evac:old")
        );
    }

    #[test]
    fn a_live_evacuation_blocks_only_funding_that_touches_its_source() {
        let stuck = intent(
            &decision(
                "evac:old",
                evacuation(A, B, 500_000),
                ReasonCode::ShutdownNotice,
                4,
            ),
            agent(4),
            IntentStatus::Pending,
        );
        let held = blockers(std::slice::from_ref(&stuck));

        for candidate in [
            decision(
                "move:from-a",
                funding(A, B, 100_000),
                ReasonCode::StandbyBelowTarget,
                5,
            ),
            decision(
                "move:into-a",
                funding(C, A, 100_000),
                ReasonCode::SpendingBelowTarget,
                5,
            ),
        ] {
            assert!(
                held.blocks_decision(&candidate, agent(5)),
                "allocator funding touching the live evacuation source must wait: {:?}",
                candidate.idempotency_key
            );
        }

        for candidate in [
            decision(
                "move:independent",
                funding(C, B, 100_000),
                ReasonCode::StandbyBelowTarget,
                5,
            ),
            decision(
                "evac:independent",
                evacuation(C, B, 100_000),
                ReasonCode::Unhealthy,
                5,
            ),
            // The edge stops at funding. A DIFFERENT dying federation draining into A is the one
            // piece of work that must never wait: `safest_other` prefers the standby, so blocking
            // it would let a stuck `Evacuate(standby)` hold C's whole balance hostage. A is only
            // eligible as a destination once it probes healthy again, and the worst case is that
            // the stuck drain later carries those funds one hop further.
            decision(
                "evac:into-a",
                evacuation(C, A, 100_000),
                ReasonCode::Unhealthy,
                5,
            ),
        ] {
            assert!(
                !held.blocks_decision(&candidate, agent(5)),
                "only funding touching the evacuation source waits: {:?}",
                candidate.idempotency_key
            );
        }
    }

    #[test]
    fn a_decision_held_mid_batch_blocks_the_rest_of_that_batch() {
        // A durable scan cannot see what the batch taking it journals, so an admission seam folds
        // each admitted decision back in. Nothing is in flight to start with.
        let mut held = GoalBlockers::default();
        let first = decision(
            "move:first",
            funding(A, B, 100_000),
            ReasonCode::StandbyBelowTarget,
            5,
        );
        assert!(!held.blocks_decision(&first, agent(5)));
        held.hold_decision(&first, agent(5));
        assert!(
            held.holds_goal_from(AllocatorGoal::FundInto(B), A),
            "a just-held funding decision retains its source: {held:?}"
        );
        assert!(
            !held.holds_goal_from(AllocatorGoal::FundInto(B), B),
            "a held funding destination must not relax a raw pin: {held:?}"
        );

        // A SECOND key for the same goal, later in the same batch, is now blocked...
        assert!(held.blocks_decision(
            &decision(
                "move:second",
                funding(C, B, 40_000),
                ReasonCode::SpendingBelowTarget,
                5
            ),
            agent(5)
        ));
        // ...while the holder itself and independent work still pass.
        assert!(!held.blocks_decision(&first, agent(5)));
        assert!(!held.blocks_decision(
            &decision(
                "move:other-dest",
                funding(A, C, 40_000),
                ReasonCode::StandbyBelowTarget,
                5
            ),
            agent(5)
        ));

        // A decision carrying no goal holds nothing.
        let mut user = GoalBlockers::default();
        user.hold_decision(&first, Actor::User);
        assert!(user.is_empty());
    }

    #[test]
    fn user_probe_join_and_advisory_work_never_blocks_a_tick() {
        let probe_leg = decision(
            "move:probe-leg",
            funding(A, B, 20_000),
            ReasonCode::ActiveProbe,
            4,
        );
        let user_move = decision(
            "move:user",
            funding(A, B, 20_000),
            ReasonCode::UserInitiated,
            4,
        );
        let user_evacuation = decision(
            "evac:user",
            evacuation(A, B, 20_000),
            ReasonCode::ShutdownNotice,
            4,
        );
        let user_pay = decision(
            "pay:user",
            Action::Pay {
                from: A,
                invoice: crate::types::Invoice("lnbc1".to_owned()),
                amount: Msat(10),
                fee_cap: Msat(1),
                payment_hash: [7; 32],
                gateway: None,
            },
            ReasonCode::UserInitiated,
            4,
        );
        let inflow = decision(
            "inflow:user",
            Action::DirectInflow {
                to: B,
                amount: Msat(10),
                fee_cap: Msat(1),
            },
            ReasonCode::UserInitiated,
            4,
        );
        let join = decision(
            "join:auto",
            Action::Join {
                federation: B,
                invite: "invite".to_owned(),
                membership_preexisting: false,
            },
            ReasonCode::UserInitiated,
            4,
        );
        let refusal = decision(
            "refuse:b",
            Action::RefuseInflow {
                fed: B,
                reason: ReasonCode::ShutdownNotice,
                diagnostics: crate::types::RefusalDiagnostics::default(),
            },
            ReasonCode::ShutdownNotice,
            4,
        );

        let held = blockers(&[
            intent(&probe_leg, agent(4), IntentStatus::Pending),
            intent(&user_move, Actor::User, IntentStatus::Pending),
            intent(&user_evacuation, Actor::User, IntentStatus::Pending),
            intent(&user_pay, Actor::User, IntentStatus::Executing),
            intent(&inflow, Actor::User, IntentStatus::Awaiting),
            intent(&join, agent(4), IntentStatus::Pending),
            intent(&refusal, agent(4), IntentStatus::Pending),
        ]);

        assert!(held.is_empty(), "{held:?}");
        assert!(!held.blocks_decision(
            &decision(
                "move:fresh",
                funding(A, B, 100_000),
                ReasonCode::StandbyBelowTarget,
                5
            ),
            agent(5)
        ));
        assert!(
            !held.holds_goal_from(AllocatorGoal::FundInto(B), A)
                && !held.holds_goal_from(AllocatorGoal::FundInto(B), B),
            "non-goal work must not populate rebalance sources: {held:?}"
        );
    }

    #[test]
    fn held_funding_retains_only_its_original_source_for_raw_pin_relaxation() {
        let funding = decision(
            "move:a-b",
            funding(A, B, 100_000),
            ReasonCode::StandbyBelowTarget,
            4,
        );
        let held = blockers(&[intent(&funding, agent(4), IntentStatus::Pending)]);

        assert!(held.holds_goal_from(AllocatorGoal::FundInto(B), A));
        assert!(!held.holds_goal_from(AllocatorGoal::FundInto(B), B));
        assert!(!held.holds_goal_from(AllocatorGoal::FundInto(B), C));
        assert_eq!(
            held.goals(),
            BTreeSet::from([AllocatorGoal::FundInto(B)]),
            "endpoint retention must not alter goal identity"
        );
    }

    #[test]
    fn held_evacuation_retains_only_its_original_source_for_raw_pin_relaxation() {
        let evacuation = decision(
            "evac:c-b",
            evacuation(C, B, 100_000),
            ReasonCode::Unhealthy,
            4,
        );
        let held = blockers(&[intent(&evacuation, agent(4), IntentStatus::Executing)]);

        assert!(held.holds_goal_from(AllocatorGoal::Evacuate(C), C));
        assert!(!held.holds_goal_from(AllocatorGoal::Evacuate(C), B));
        assert!(!held.holds_goal_from(AllocatorGoal::Evacuate(C), A));
        assert_eq!(
            held.goals(),
            BTreeSet::from([AllocatorGoal::Evacuate(C)]),
            "endpoint retention must not alter goal identity"
        );
    }

    #[test]
    fn terminal_work_holds_nothing_and_a_second_holder_still_blocks() {
        let done = decision(
            "move:done",
            funding(A, B, 100_000),
            ReasonCode::StandbyBelowTarget,
            3,
        );
        let failed = decision(
            "evac:failed",
            evacuation(C, B, 100_000),
            ReasonCode::Unhealthy,
            3,
        );
        let held = blockers(&[
            intent(&done, agent(3), IntentStatus::Done),
            intent(&failed, agent(3), IntentStatus::Failed),
        ]);
        assert!(held.is_empty());

        // Two live holders of one goal: the attach exemption is per-key, so the OTHER key is
        // still blocked.
        let first = decision(
            "move:first",
            funding(A, B, 100_000),
            ReasonCode::StandbyBelowTarget,
            3,
        );
        let second = decision(
            "move:second",
            funding(C, B, 100_000),
            ReasonCode::SpendingBelowTarget,
            4,
        );
        let both = blockers(&[
            intent(&first, agent(3), IntentStatus::Pending),
            intent(&second, agent(4), IntentStatus::Pending),
        ]);
        assert_eq!(both.len(), 2);
        assert_eq!(both.goals(), BTreeSet::from([AllocatorGoal::FundInto(B)]));
        assert!(both.blocks_decision(&first, agent(3)));
        assert!(both.blocks_decision(&second, agent(4)));
        assert!(
            both.has_blocking_holder_from(&first, agent(3), C),
            "the second holder, not BTreeSet's first holder, supplies the C association"
        );
        assert!(
            !both.has_blocking_holder_from(&first, agent(3), A),
            "the candidate's own key is excluded from associated-holder evidence"
        );
    }
}
