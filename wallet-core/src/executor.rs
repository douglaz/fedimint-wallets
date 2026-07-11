use crate::ledger::Actor;
use crate::types::{
    Action, AllocatorDecision, FederationId, IdempotencyKey, MovePhase, MoveRecord, Msat,
    OperationId, ReasonCode, Reservations,
};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IntentStatus {
    Pending,
    Executing,
    Done,
    /// A `DirectInflow` whose EXTERNAL payer has not yet paid. Owned by the `recv_op`
    /// subscription (§9.5); `reconcile` does NOT re-drive it through `perform`.
    Awaiting,
    Failed,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Intent {
    pub idempotency_key: IdempotencyKey,
    pub action: Action,
    /// Derived from `action.fee_cap()`: `Some` for money actions carrying an explicit
    /// bound; `None` for the fee-free `Join` and advisory actions.
    pub max_fee: Option<Msat>,
    pub status: IntentStatus,
    /// The allocator reason this intent carries into the ledger (§8) — no longer dropped.
    /// User verbs carry [`ReasonCode::UserInitiated`].
    pub reason: ReasonCode,
    /// Who initiated the intent (§8): `Agent` for a tick, `User` for an operator verb.
    pub actor: Actor,
    /// Unix millis at which the intent was first created — the ledger's `created_at_ms`.
    pub created_at_ms: u64,
    /// Durable raw-operation artifact. For `Pay`, its presence is the pre/post-fund
    /// reservation boundary; for `Receive`, it is the subscription handle.
    pub operation_id: Option<OperationId>,
    /// The invoice minted by a raw `Receive`, persisted before it is surfaced.
    pub invoice: Option<crate::Invoice>,
}

impl Intent {
    /// Build a `Pending` intent from a decision, stamping the ledger identity §8 threads:
    /// the decision's `reason`, the initiating `actor`, and the creation clock `now_ms`.
    pub fn from_decision(decision: &AllocatorDecision, actor: Actor, now_ms: u64) -> Self {
        Self {
            idempotency_key: decision.idempotency_key.clone(),
            action: decision.action.clone(),
            max_fee: decision.action.fee_cap(),
            status: IntentStatus::Pending,
            reason: decision.reason,
            actor,
            created_at_ms: now_ms,
            operation_id: None,
            invoice: None,
        }
    }
}

/// Project the journal-visible work that live balances have not absorbed yet.
pub fn project_reservations(
    intents: &[Intent],
    records: impl Fn(&IdempotencyKey) -> Option<MoveRecord>,
) -> Reservations {
    let mut out = Reservations::default();
    for intent in intents {
        if matches!(intent.status, IntentStatus::Done | IntentStatus::Failed) {
            continue;
        }

        let add_outbound = |out: &mut Reservations, fed, amount: Msat| {
            let slot = out.per_fed_outbound.entry(fed).or_insert(Msat(0));
            slot.0 = slot.0.saturating_add(amount.0);
        };
        let add_inbound = |out: &mut Reservations, fed, amount: Msat| {
            let slot = out.per_fed_inbound.entry(fed).or_insert(Msat(0));
            slot.0 = slot.0.saturating_add(amount.0);
        };

        match &intent.action {
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
            } => {
                let record = records(&intent.idempotency_key);
                let reservation_amount = record.as_ref().map_or(*amount, |record| record.amount);
                let phase = record.map(|record| record.phase);
                match phase {
                    Some(
                        MovePhase::Settled
                        | MovePhase::Refunded
                        | MovePhase::Failed
                        | MovePhase::Stranded,
                    ) => {}
                    Some(MovePhase::Sending) => add_inbound(&mut out, *to, reservation_amount),
                    None | Some(MovePhase::Created | MovePhase::Invoiced) => {
                        if matches!(&intent.action, Action::Move { .. }) {
                            add_outbound(
                                &mut out,
                                *from,
                                Msat(reservation_amount.0.saturating_add(fee_cap.0)),
                            );
                        }
                        add_inbound(&mut out, *to, reservation_amount);
                    }
                }
            }
            Action::DirectInflow { to, amount, .. } => {
                let record = records(&intent.idempotency_key);
                let terminal_record = record.as_ref().is_some_and(|record| {
                    matches!(
                        record.phase,
                        MovePhase::Settled
                            | MovePhase::Refunded
                            | MovePhase::Failed
                            | MovePhase::Stranded
                    )
                });
                if !terminal_record {
                    add_inbound(
                        &mut out,
                        *to,
                        record.as_ref().map_or(*amount, |record| record.amount),
                    );
                }
            }
            Action::Pay {
                from,
                amount,
                fee_cap,
                ..
            } if intent.operation_id.is_none() => {
                add_outbound(&mut out, *from, Msat(amount.0.saturating_add(fee_cap.0)))
            }
            Action::Receive { to, amount, .. } => add_inbound(&mut out, *to, *amount),
            Action::Pay { .. } | Action::Join { .. } | Action::RefuseInflow { .. } => {}
        }
    }
    out
}

/// Outcome counts from [`apply`]/[`reconcile`]. An `Awaiting` outcome counts as
/// `performed` (the side effect ran; only the external settlement is outstanding).
/// `failed` counts attempts that did not complete in this pass, including retryable
/// failures that leave the intent `Pending` for a later retry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutionSummary {
    pub performed: usize,
    pub skipped: usize,
    pub failed: usize,
    /// Existing intents that were already terminal `Failed` and therefore skipped instead of
    /// re-driven. This is a subset of `skipped`; callers that gate money operations on success
    /// can distinguish these from benign idempotent `Done` skips.
    pub terminal_failed_skipped: usize,
    /// Retryable failures that left the intent `Pending` for a later retry (§15.11). A SUBSET of
    /// `failed` (kept there so existing "any failure" gating is unchanged), broken out so a
    /// scheduler can tell "left Pending, will retry" from a terminal `Permanent`/`Unsupported`
    /// money-op failure: `failed − retryable` is the terminal count. This counts `Retryable`
    /// PERFORM outcomes only; a journal-I/O fault surrounding a perform (a failed
    /// `get`/`upsert`/`set_status`) stays in `failed` alone — the journal's own status is
    /// authoritative for whether such an intent is re-driven (`reconcile` scans `Pending`/
    /// `Executing` directly), so this summary field never gates a retry.
    pub retryable: usize,
}

/// The result of a successful [`Executor::perform`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PerformOutcome {
    /// The intent's effect completed; mark it `Done`.
    Done,
    /// The effect was started but completion depends on an EXTERNAL event (a
    /// `DirectInflow` payer paying the surfaced invoice). Mark `Awaiting`; the
    /// `recv_op` subscription finalizes it (§2/§9.5), NOT a re-drive.
    Awaiting,
    /// Raw pay was already in flight in the SDK. It has the same durable lifecycle as
    /// `Awaiting`, but the synchronous CLI preserves the SDK's distinct stdout label.
    AwaitingAlreadyInFlight,
}

/// A typed execution failure (was information-free). The variant decides how
/// [`drive`] updates the intent's status.
///
/// The `Retryable`/`Permanent` payloads are diagnostic context from the real
/// `Executor` impl. `drive` dispatches on the variant and logs the payload before
/// collapsing the result into [`ExecutionSummary`]'s counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecError {
    /// Transient: leave the intent `Pending` so the next [`reconcile`] retries it.
    Retryable(String),
    /// Terminal: mark `Failed`. NOT auto-re-driven; only a manual retry resets it.
    Permanent(String),
    /// The action is not one this executor implementation performs (an `Executor`
    /// need not map every `Action` shape to a side effect — the fedimint executor
    /// returns this only for a non-move action reaching `perform`). → `Failed`.
    Unsupported,
}

#[async_trait]
pub trait Journal: Send + Sync {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError>;
    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError>;
    /// Set the intent's status, carrying the terminal failure diagnostic (§8.3): `drive`
    /// passes the [`ExecError`] string on the `Permanent`/`Unsupported` paths and `None`
    /// elsewhere. A durable journal (§9, later run) records it as the ledger row's `error`
    /// when the executor never reached a terminal `MoveRecord.outcome` to source it from.
    async fn set_status(
        &self,
        key: &IdempotencyKey,
        status: IntentStatus,
        error: Option<&str>,
    ) -> Result<(), ExecError>;
    /// The single-writer claim (spec §2): atomically, if the stored intent's status ==
    /// `expected`, set it to `new` (moving the pending index in the same transaction for
    /// durable impls) and return `Ok(true)`; otherwise make no change and return `Ok(false)`.
    /// `Ok(false)` also covers an absent key. `drive` uses this to claim a `Pending` intent
    /// before performing it, so two concurrent drivers of the same intent can never both win
    /// the claim.
    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError>;
    /// Intents to re-drive on the next pass: `Pending | Executing` ONLY (never
    /// `Awaiting`/`Failed`, which are terminal-or-subscription-owned).
    async fn pending(&self) -> Result<Vec<Intent>, ExecError>;
    /// Subscription-owned intents are scanned separately so reconcile never re-drives them.
    async fn awaiting(&self) -> Result<Vec<Intent>, ExecError> {
        Ok(Vec::new())
    }
    /// The complete journal view used for decide-time reservation projection. Durable
    /// implementations may make this stricter than their operational reconcile/resume scans:
    /// admitting money work from a partial view must fail closed.
    async fn reservation_intents(&self) -> Result<Vec<Intent>, ExecError> {
        let mut intents = self.pending().await?;
        intents.extend(self.awaiting().await?);
        Ok(intents)
    }
    async fn failed(&self) -> Vec<Intent>;
    /// Read the derived move phase used by reservation projection.
    async fn move_record(&self, _key: &IdempotencyKey) -> Result<Option<MoveRecord>, ExecError> {
        Ok(None)
    }
    /// Atomically persist a raw operation's network artifact on its intent.
    async fn set_operation_artifact(
        &self,
        _key: &IdempotencyKey,
        _operation_id: OperationId,
        _invoice: Option<&crate::Invoice>,
    ) -> Result<(), ExecError> {
        Err(ExecError::Unsupported)
    }
    /// A stable identity for this journal's underlying durable store — used ONLY to scope
    /// `drive`'s process-local in-flight-performs guard (a belt-and-suspenders duplicate-perform
    /// guard alongside the CAS above; an `Executing -> Executing` CAS can't provide mutual
    /// exclusion by itself, since re-reading unchanged state can't tell "my own resume" apart
    /// from "a different concurrent claimant"). Two `Journal` VALUES over the SAME underlying
    /// store (e.g. two `FedimintJournal`s built from clones of the same `Database`, which is
    /// documented to share storage) MUST return the same id, or the guard silently fails to
    /// serialize them; two unrelated journals (e.g. independent test fixtures) must not
    /// collide. The default (this value's own address) is correct for any impl that is never
    /// constructed twice over the same shared storage — true of `MemJournal` and the trait's
    /// test doubles; `FedimintJournal` overrides it (its storage CAN be shared across
    /// independently-constructed handles).
    fn store_id(&self) -> usize {
        (self as *const Self).cast::<()>() as usize
    }
}

#[derive(Debug, Default)]
pub struct MemJournal {
    intents: Mutex<BTreeMap<IdempotencyKey, Intent>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InFlightPerformKey {
    store: usize,
    key: IdempotencyKey,
}

static IN_FLIGHT_PERFORMS: OnceLock<Mutex<BTreeSet<InFlightPerformKey>>> = OnceLock::new();

fn in_flight_performs() -> &'static Mutex<BTreeSet<InFlightPerformKey>> {
    IN_FLIGHT_PERFORMS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

struct InFlightPerform {
    key: InFlightPerformKey,
}

impl InFlightPerform {
    fn claim<J: Journal>(journal: &J, key: &IdempotencyKey) -> Option<Self> {
        let key = InFlightPerformKey {
            store: journal.store_id(),
            key: key.clone(),
        };
        let mut in_flight = in_flight_performs()
            .lock()
            .expect("in-flight perform mutex poisoned");
        if in_flight.insert(key.clone()) {
            Some(Self { key })
        } else {
            None
        }
    }
}

impl Drop for InFlightPerform {
    fn drop(&mut self) {
        in_flight_performs()
            .lock()
            .expect("in-flight perform mutex poisoned")
            .remove(&self.key);
    }
}

impl MemJournal {
    pub fn new() -> Self {
        Self::default()
    }

    fn with_status(&self, accept: impl Fn(IntentStatus) -> bool) -> Vec<Intent> {
        self.intents
            .lock()
            .expect("journal mutex poisoned")
            .values()
            .filter(|intent| accept(intent.status))
            .cloned()
            .collect()
    }
}

#[async_trait]
impl Journal for MemJournal {
    async fn upsert(&self, intent: &Intent) -> Result<(), ExecError> {
        self.intents
            .lock()
            .expect("journal mutex poisoned")
            .insert(intent.idempotency_key.clone(), intent.clone());
        Ok(())
    }

    async fn get(&self, key: &IdempotencyKey) -> Result<Option<Intent>, ExecError> {
        Ok(self
            .intents
            .lock()
            .expect("journal mutex poisoned")
            .get(key)
            .cloned())
    }

    async fn set_status(
        &self,
        key: &IdempotencyKey,
        status: IntentStatus,
        // The failure diagnostic is durable-ledger material (§9); the in-memory test journal
        // has no ledger, so it accepts and ignores it.
        _error: Option<&str>,
    ) -> Result<(), ExecError> {
        self.intents
            .lock()
            .expect("journal mutex poisoned")
            .get_mut(key)
            .map(|intent| intent.status = status)
            .ok_or_else(|| ExecError::Permanent("journal: intent not found".into()))
    }

    async fn set_status_if(
        &self,
        key: &IdempotencyKey,
        expected: IntentStatus,
        new: IntentStatus,
    ) -> Result<bool, ExecError> {
        let mut intents = self.intents.lock().expect("journal mutex poisoned");
        match intents.get_mut(key) {
            Some(intent) if intent.status == expected => {
                intent.status = new;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn pending(&self) -> Result<Vec<Intent>, ExecError> {
        Ok(self.with_status(|status| {
            matches!(status, IntentStatus::Pending | IntentStatus::Executing)
        }))
    }

    async fn awaiting(&self) -> Result<Vec<Intent>, ExecError> {
        Ok(self.with_status(|status| status == IntentStatus::Awaiting))
    }

    async fn failed(&self) -> Vec<Intent> {
        self.with_status(|status| status == IntentStatus::Failed)
    }

    async fn set_operation_artifact(
        &self,
        key: &IdempotencyKey,
        operation_id: OperationId,
        invoice: Option<&crate::Invoice>,
    ) -> Result<(), ExecError> {
        let mut intents = self.intents.lock().expect("journal mutex poisoned");
        let intent = intents
            .get_mut(key)
            .ok_or_else(|| ExecError::Permanent("journal: intent not found".into()))?;
        intent.operation_id = Some(operation_id);
        if let Some(invoice) = invoice {
            intent.invoice = Some(invoice.clone());
        }
        Ok(())
    }
}

/// The side-effecting step that turns a journaled [`Intent`] into a real-world effect
/// (later: a real cross-federation Lightning move).
///
/// # `&self` + interior mutability
///
/// `perform` takes `&self`: the real implementation holds shared `Arc`s (a
/// `MultiClient` + a `Database`-backed journal) and must be `Send + Sync`. Test
/// doubles ([`MockExecutor`]) use interior mutability for their recorded state.
///
/// # Idempotency contract (load-bearing)
///
/// `perform` MUST be idempotent keyed on `intent.idempotency_key`. After a crash,
/// [`reconcile`] re-drives any intent left `Pending`/`Executing` and calls `perform`
/// again on an intent that may already have moved money, so a real implementation
/// must dedupe on the key (e.g. reuse the in-flight payment registered under that
/// key) and move the underlying funds AT MOST ONCE per key. [`MockExecutor`] models
/// the contract by recording each key and returning `Ok` without re-performing a key
/// it has already seen.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError>;
}

#[derive(Debug, Default)]
pub struct MockExecutor {
    inner: Mutex<MockState>,
}

#[derive(Debug, Default)]
struct MockState {
    fail: BTreeMap<IdempotencyKey, ExecError>,
    awaiting: BTreeSet<IdempotencyKey>,
    performed_keys: Vec<IdempotencyKey>,
}

impl MockExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    #[doc(hidden)] // test-only failure injection; not part of the supported API surface
    pub fn fail_retryable(&self, key: &str) {
        self.inner.lock().expect("mock mutex poisoned").fail.insert(
            IdempotencyKey(key.to_string()),
            ExecError::Retryable("injected".into()),
        );
    }

    #[doc(hidden)] // test-only failure injection; not part of the supported API surface
    pub fn fail_permanent(&self, key: &str) {
        self.inner.lock().expect("mock mutex poisoned").fail.insert(
            IdempotencyKey(key.to_string()),
            ExecError::Permanent("injected".into()),
        );
    }

    #[doc(hidden)] // test-only failure injection; not part of the supported API surface
    pub fn succeed(&self, key: &str) {
        self.inner
            .lock()
            .expect("mock mutex poisoned")
            .fail
            .remove(&IdempotencyKey(key.to_string()));
    }

    #[doc(hidden)] // test-only: make this key return `Ok(Awaiting)` (a `DirectInflow`)
    pub fn set_awaiting(&self, key: &str) {
        self.inner
            .lock()
            .expect("mock mutex poisoned")
            .awaiting
            .insert(IdempotencyKey(key.to_string()));
    }

    pub fn performed_keys(&self) -> Vec<IdempotencyKey> {
        self.inner
            .lock()
            .expect("mock mutex poisoned")
            .performed_keys
            .clone()
    }
}

#[async_trait]
impl Executor for MockExecutor {
    async fn perform(&self, intent: &Intent) -> Result<PerformOutcome, ExecError> {
        let mut state = self.inner.lock().expect("mock mutex poisoned");
        if state.performed_keys.contains(&intent.idempotency_key) {
            // Idempotent replay: already acted under this key; do not repeat the effect.
            return if state.awaiting.contains(&intent.idempotency_key) {
                Ok(PerformOutcome::Awaiting)
            } else {
                Ok(PerformOutcome::Done)
            };
        }
        if let Some(err) = state.fail.get(&intent.idempotency_key) {
            return Err(err.clone());
        }
        state.performed_keys.push(intent.idempotency_key.clone());
        if state.awaiting.contains(&intent.idempotency_key) {
            Ok(PerformOutcome::Awaiting)
        } else {
            Ok(PerformOutcome::Done)
        }
    }
}

pub async fn apply<J: Journal, E: Executor>(
    journal: &J,
    executor: &E,
    decisions: &[AllocatorDecision],
    actor: Actor,
    now_ms: u64,
) -> ExecutionSummary {
    apply_with_admission(journal, executor, decisions, actor, now_ms, None, None).await
}

pub async fn apply_with_admission<J: Journal, E: Executor>(
    journal: &J,
    executor: &E,
    decisions: &[AllocatorDecision],
    actor: Actor,
    now_ms: u64,
    balances: Option<&BTreeMap<FederationId, Msat>>,
    per_fed_cap: Option<Msat>,
) -> ExecutionSummary {
    let mut summary = ExecutionSummary::default();
    let mut seen = BTreeSet::new();

    for decision in decisions {
        // Advisory actions (`RefuseInflow`) are policy signals, not work: never journal an
        // Intent for them.
        if !decision.action.is_executable() {
            summary.skipped += 1;
            continue;
        }

        if !seen.insert(decision.idempotency_key.clone()) {
            summary.skipped += 1;
            continue;
        }

        match decide_and_journal(journal, decision, actor, now_ms, balances, per_fed_cap).await {
            Ok(DecideAndJournal::TerminalFailed) => {
                summary.skipped += 1;
                summary.terminal_failed_skipped += 1;
            }
            Ok(DecideAndJournal::Skip) => summary.skipped += 1,
            Ok(DecideAndJournal::Drive(intent)) => {
                drive_to_terminal(journal, executor, &intent, &mut summary).await;
            }
            Err(_) => summary.failed += 1,
        }
    }

    summary
}

#[derive(Clone, Debug, PartialEq)]
pub enum DecideAndJournal {
    Drive(Box<Intent>),
    Skip,
    TerminalFailed,
}

/// Deduplicate, validate an attach, project durable reservations, apply admission bounds,
/// and write a fresh `Pending` intent. This function performs no network I/O.
pub async fn decide_and_journal<J: Journal>(
    journal: &J,
    decision: &AllocatorDecision,
    actor: Actor,
    now_ms: u64,
    balances: Option<&BTreeMap<FederationId, Msat>>,
    per_fed_cap: Option<Msat>,
) -> Result<DecideAndJournal, ExecError> {
    let existing = journal.get(&decision.idempotency_key).await?;
    match existing {
        Some(intent) if intent.status == IntentStatus::Failed => {
            validate_attach(&intent, decision)?;
            Ok(DecideAndJournal::TerminalFailed)
        }
        Some(intent) if matches!(intent.status, IntentStatus::Done | IntentStatus::Awaiting) => {
            validate_attach(&intent, decision)?;
            Ok(DecideAndJournal::Skip)
        }
        Some(mut intent) => {
            validate_attach(&intent, decision)?;
            if intent.status == IntentStatus::Pending && intent.operation_id.is_none() {
                let gateway_changed = match (&mut intent.action, &decision.action) {
                    (
                        Action::Pay {
                            gateway: stored_gateway,
                            ..
                        },
                        Action::Pay { gateway, .. },
                    )
                    | (
                        Action::Receive {
                            gateway: stored_gateway,
                            ..
                        },
                        Action::Receive { gateway, .. },
                    ) if stored_gateway != gateway => {
                        *stored_gateway = gateway.clone();
                        true
                    }
                    _ => false,
                };
                if gateway_changed {
                    journal.upsert(&intent).await?;
                }
            }
            Ok(DecideAndJournal::Drive(Box::new(intent)))
        }
        None => {
            let intent = Intent::from_decision(decision, actor, now_ms);
            let in_flight = journal.reservation_intents().await?;
            let mut records = BTreeMap::new();
            for pending in &in_flight {
                if let Some(record) = journal.move_record(&pending.idempotency_key).await? {
                    records.insert(pending.idempotency_key.clone(), record);
                }
            }
            let reservations = project_reservations(&in_flight, |key| records.get(key).cloned());
            admit_intent(&intent, balances, per_fed_cap, &reservations)?;
            journal.upsert(&intent).await?;
            Ok(DecideAndJournal::Drive(Box::new(intent)))
        }
    }
}

fn validate_attach(intent: &Intent, decision: &AllocatorDecision) -> Result<(), ExecError> {
    let matches_sizing = match (&intent.action, &decision.action) {
        (
            Action::Pay {
                from: old_from,
                amount: old_amount,
                fee_cap: old_fee_cap,
                payment_hash: old_hash,
                ..
            },
            Action::Pay {
                from,
                amount,
                fee_cap,
                payment_hash,
                ..
            },
        ) => (old_from, old_amount, old_fee_cap, old_hash) == (from, amount, fee_cap, payment_hash),
        (
            Action::Receive {
                to: old_to,
                amount: old_amount,
                fee_cap: old_fee_cap,
                nonce: old_nonce,
                ..
            },
            Action::Receive {
                to,
                amount,
                fee_cap,
                nonce,
                ..
            },
        ) => (old_to, old_amount, old_fee_cap, old_nonce) == (to, amount, fee_cap, nonce),
        (
            Action::Join {
                federation: old_federation,
                invite: old_invite,
                ..
            },
            Action::Join {
                federation, invite, ..
            },
        ) => (old_federation, old_invite) == (federation, invite),
        (Action::Pay { .. } | Action::Receive { .. } | Action::Join { .. }, _) => false,
        _ => true,
    };
    if !matches_sizing {
        return Err(ExecError::Permanent(format!(
            "intent {} conflicts with the existing request's sizing fields",
            intent.idempotency_key.0
        )));
    }
    Ok(())
}

/// Check one intent against live balances and an already-projected cross-operation view.
/// The fedimint driver reuses this at its pre-fund recovery boundary so a Pending intent
/// journaled while a client was unavailable cannot later fund without admission.
pub fn admit_intent(
    intent: &Intent,
    balances: Option<&BTreeMap<FederationId, Msat>>,
    per_fed_cap: Option<Msat>,
    reservations: &Reservations,
) -> Result<(), ExecError> {
    let Some(balances) = balances else {
        return Ok(());
    };
    let source = match &intent.action {
        Action::Move {
            from,
            amount,
            fee_cap,
            ..
        }
        | Action::Pay {
            from,
            amount,
            fee_cap,
            ..
        } => Some((*from, amount.0.saturating_add(fee_cap.0))),
        Action::Evacuate { .. }
        | Action::DirectInflow { .. }
        | Action::Receive { .. }
        | Action::Join { .. }
        | Action::RefuseInflow { .. } => None,
    };
    if let Some((fed, required)) = source {
        let available = balances
            .get(&fed)
            .copied()
            .unwrap_or(Msat(0))
            .0
            .saturating_sub(reservations.outbound(fed).0);
        if required > available {
            return Err(ExecError::Permanent(format!(
                "insufficient balance after reservations on federation {}: need {required} msat, have {available} msat",
                fed.to_hex()
            )));
        }
    }

    let destination = match &intent.action {
        Action::Move { to, amount, .. }
        | Action::Evacuate { to, amount, .. }
        | Action::DirectInflow { to, amount, .. }
        | Action::Receive { to, amount, .. } => Some((*to, *amount)),
        Action::Pay { .. } | Action::Join { .. } | Action::RefuseInflow { .. } => None,
    };
    if let (Some(cap), Some((fed, amount))) = (per_fed_cap, destination) {
        let committed = balances
            .get(&fed)
            .copied()
            .unwrap_or(Msat(0))
            .0
            .saturating_add(reservations.inbound(fed).0)
            .saturating_add(amount.0);
        if committed > cap.0 {
            return Err(ExecError::Permanent(format!(
                "destination would exceed the per-fed cap after reservations on federation {}: {committed} > {} msat",
                fed.to_hex(), cap.0
            )));
        }
    }
    Ok(())
}

pub async fn reconcile<J: Journal, E: Executor>(journal: &J, executor: &E) -> ExecutionSummary {
    let mut summary = ExecutionSummary::default();

    // Re-drive `pending()` (Pending|Executing) ONLY. `Failed`/`Permanent` stay terminal
    // and `Awaiting` is subscription-owned (§2/§9.4): neither is re-driven here.
    let pending = match journal.pending().await {
        Ok(pending) => pending,
        Err(_) => {
            summary.failed += 1;
            return summary;
        }
    };
    for intent in pending {
        drive_to_terminal(journal, executor, &intent, &mut summary).await;
    }

    summary
}

pub async fn drive_to_terminal<J: Journal, E: Executor>(
    journal: &J,
    executor: &E,
    intent: &Intent,
    summary: &mut ExecutionSummary,
) {
    let _ = drive_intent_step(journal, executor, intent, summary).await;
}

/// Claim and drive one journaled intent through the existing executor lifecycle, recording
/// the terminal transition exactly once. Network work begins only inside this function.
pub async fn drive_intent_step<J: Journal, E: Executor>(
    journal: &J,
    executor: &E,
    intent: &Intent,
    summary: &mut ExecutionSummary,
) -> Result<Option<PerformOutcome>, ExecError> {
    // `drive` only ever receives a `Pending` or `Executing` intent (see `apply`/`reconcile`).
    // A `Pending` intent must first win the durable CAS claim. `Executing` means either
    // crash-recovery resume or another in-process driver already won that claim; the
    // process-local guard below lets crash recovery proceed while skipping live duplicates.
    if intent.status == IntentStatus::Pending {
        match journal
            .set_status_if(
                &intent.idempotency_key,
                IntentStatus::Pending,
                IntentStatus::Executing,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                // Another driver already claimed this intent; not a failure, just lost the race.
                summary.skipped += 1;
                return Ok(None);
            }
            Err(error) => {
                summary.failed += 1;
                return Err(error);
            }
        }
    }

    let Some(_in_flight) = InFlightPerform::claim(journal, &intent.idempotency_key) else {
        summary.skipped += 1;
        return Ok(None);
    };

    let executing = match journal.get(&intent.idempotency_key).await {
        Ok(Some(intent)) if intent.status == IntentStatus::Executing => intent,
        Ok(_) => {
            // This can be a stale `Executing` snapshot from a concurrent scan after the
            // winning driver has already written a terminal status.
            summary.skipped += 1;
            return Ok(None);
        }
        Err(error) => {
            summary.failed += 1;
            return Err(error);
        }
    };

    match executor.perform(&executing).await {
        Ok(outcome) => {
            let next = match outcome {
                PerformOutcome::Done => IntentStatus::Done,
                PerformOutcome::Awaiting | PerformOutcome::AwaitingAlreadyInFlight => {
                    IntentStatus::Awaiting
                }
            };
            if let Err(error) = journal
                .set_status(&intent.idempotency_key, next, None)
                .await
            {
                summary.failed += 1;
                return Err(error);
            }
            summary.performed += 1;
            Ok(Some(outcome))
        }
        Err(ExecError::Retryable(reason)) => {
            tracing::warn!(
                key = %intent.idempotency_key.0,
                reason = %reason,
                "executor perform failed retryably"
            );
            // Leave the intent Pending so the next reconcile retries it (NOT Failed). A
            // retry is not a terminal failure, so no ledger `error` string is threaded (§8.3).
            let reset = journal
                .set_status(&intent.idempotency_key, IntentStatus::Pending, None)
                .await;
            // Retryable: count it in `failed` (unchanged gating) AND in the `retryable` subset
            // (§15.11), so a scheduler can tell a left-Pending retry from a terminal failure.
            summary.failed += 1;
            summary.retryable += 1;
            reset?;
            Err(ExecError::Retryable(reason))
        }
        Err(ExecError::Permanent(reason)) => {
            tracing::warn!(
                key = %intent.idempotency_key.0,
                reason = %reason,
                "executor perform failed permanently"
            );
            // Thread the diagnostic to the ledger's `error` (§8.3): several permanent
            // failures never reach a terminal `MoveRecord.outcome`, so this is the only
            // source for the row's error.
            summary.failed += 1;
            journal
                .set_status(&intent.idempotency_key, IntentStatus::Failed, Some(&reason))
                .await?;
            Err(ExecError::Permanent(reason))
        }
        Err(ExecError::Unsupported) => {
            tracing::warn!(
                key = %intent.idempotency_key.0,
                "executor action unsupported"
            );
            summary.failed += 1;
            journal
                .set_status(
                    &intent.idempotency_key,
                    IntentStatus::Failed,
                    Some("executor does not support this action"),
                )
                .await?;
            Err(ExecError::Unsupported)
        }
    }
}
