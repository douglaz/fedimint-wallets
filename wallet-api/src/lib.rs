//! Wire contracts shared by wallet API servers and clients.

use serde::{Deserialize, Serialize};
use std::fmt;
pub use wallet_core::{FederationId, Msat, RefusalDiagnostics};

/// The shipped `max_fee_bps_of_move` default (300 bps). A named fn so serde's missing-field
/// default and `Policy::default()` cannot drift apart.
fn default_max_fee_bps_of_move() -> u16 {
    300
}

/// The shipped `evac_fee_base_msat` default (200 sats). Named so a missing stored field does not
/// deserialize to numeric zero.
fn default_evac_fee_base_msat() -> Msat {
    Msat(200_000)
}

/// The shipped `evac_fee_bps` default (300 bps).
fn default_evac_fee_bps() -> u16 {
    300
}

/// The standing instruction's user-owned allocation and automation parameters.
///
/// Deliberately NOT `#[serde(deny_unknown_fields)]`, unlike every request type below (br-c3j).
/// `Policy` is both a validated wire DTO and a PERSISTED row, and those two roles want opposite
/// strictness. On the wire, rejecting an unknown key stops a typo silently taking the shipped
/// default — so the daemon still enforces that, in `handlers::put_policy`, against the key set
/// derived from this very type. On the store, rejecting an unknown key is a DOWNGRADE FENCE: the
/// moment a newer build writes a policy carrying a field an older build has never heard of, that
/// older build cannot read its own policy row and will not start, so a bad deploy cannot be rolled
/// back. `seed_policy` decodes this row before the actor starts, which is exactly where a hard
/// failure strands the wallet.
///
/// The `serde(default)` attributes below cover the other direction (a new build reading an old
/// row). Both directions are load-bearing on a funded wallet; keep them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub per_fed_cap: Msat,
    pub spending_target: Msat,
    pub standby_target: Msat,
    /// ABSOLUTE fee cap. It bounds no allocator-emitted action: funding `Move`s use
    /// `max_fee_bps_of_move`, and `Evacuate` uses the `evac_fee_*` pair below.
    /// **In an evacuation incident this is not the knob to turn** — the evacuation cap is
    /// `evac_fee_base_msat` + `evac_fee_bps`. An admitted action's cap is generally immutable.
    /// The narrow exception is a pre-artifact Agent `Evacuate` with durable typed
    /// structural-refusal evidence: a component-wise monotone effective increase at its measured
    /// delivered-net sample wakes the scheduler and atomically replaces that marked parent with a
    /// linked fresh successor. `max_fee` remains load-bearing elsewhere, as the default `fee_cap`
    /// for user-initiated pay/move/receive and as the probe leg cap, so it is not dead.
    pub max_fee: Msat,
    /// PROPORTIONAL fee cap for funding `Move`s, in basis points of the amount moved
    /// (1..=10000; Policy rejects 0). Replaces the absolute `max_fee` for funding so sizing
    /// scales with the move and a positive surplus never saturates to a refused (zero-amount)
    /// move. `#[serde(default)]` so a policy row persisted before this field existed still
    /// decodes (adopting the shipped default) and the daemon starts across the upgrade —
    /// `seed_policy` decodes the stored row before the actor starts, so a hard decode failure
    /// there would strand the wallet (wiping the journal to recover is NOT safe: it also
    /// destroys the federation registry, orphaning the ecash — the wallet has no seed-recovery
    /// path wired yet).
    #[serde(default = "default_max_fee_bps_of_move")]
    pub max_fee_bps_of_move: u16,
    /// BASE component of the evacuation fee cap, in millisatoshis. With `evac_fee_bps` it is the
    /// cap actually ENFORCED on an evacuation, computed as `base + bps * delivered_net / 10_000`
    /// against what the destination is credited — never against the amount that was asked for
    /// (CONTEXT.md, "Delivered net"). The named serde default keeps older stored rows from
    /// decoding this numeric field as zero.
    #[serde(default = "default_evac_fee_base_msat")]
    pub evac_fee_base_msat: Msat,
    /// PROPORTIONAL component of the evacuation fee cap, in basis points of the net DELIVERED to
    /// the destination — `invoice - receive_quote`, not the amount requested, which is larger
    /// whenever the gross-up settles a hair under (CONTEXT.md, "Delivered net"). Applied with
    /// integer FLOOR division, so `cap = base + floor(delivered * bps / 10_000)`. (0..=10000).
    #[serde(default = "default_evac_fee_bps")]
    pub evac_fee_bps: u16,
    pub spending_fed: Option<FederationId>,
    pub standby_fed: Option<FederationId>,
    pub probe_min_span_secs: u64,
    pub probe_min_successes: u32,
    pub probe_ttl_secs: u64,
    pub probe_amount: Msat,
    pub max_probe_attempts_per_week: u32,
    pub max_probe_spend_per_week: Msat,
    pub base_interval_secs: u64,
    pub min_interval_secs: u64,
    pub evacuation_lead_secs: u64,
    pub discover_every_secs: u64,
    pub probe_retry_backoff_secs: u64,
    pub probe_refresh_lead_secs: u64,
    pub max_auto_joins_per_week: u32,
    pub auto_join_lifetime_cap: u32,
    pub max_candidates_per_pass: u32,
    pub per_preview_timeout_secs: u64,
    pub discover_pass_deadline_secs: u64,
    pub auto_join: bool,
    pub require_mainnet: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            per_fed_cap: Msat(1_500_000_000),
            spending_target: Msat(500_000_000),
            standby_target: Msat(150_000_000),
            max_fee: Msat(200_000),
            // br-ljj.2: 300 bps (3%) of the move. Chosen to preserve the pilot's current
            // effective tightness — its 50-sat absolute cap on ~1_938-sat top-up moves is
            // ~258 bps — with ~2.3x headroom over a realistic two-leg gateway fee (~130 bps:
            // the executor fee model's "realistic" 0.5% ppm/leg plus base + federation fees).
            // Tune DOWN once br-ljj.3's per-route economics yield precise per-move fee data.
            max_fee_bps_of_move: default_max_fee_bps_of_move(),
            evac_fee_base_msat: default_evac_fee_base_msat(),
            evac_fee_bps: default_evac_fee_bps(),
            spending_fed: None,
            standby_fed: None,
            // The verdict knobs and amount match wallet_core::ProbePolicy.
            probe_min_span_secs: 24 * 60 * 60,
            probe_min_successes: 3,
            probe_ttl_secs: 7 * 24 * 60 * 60,
            probe_amount: Msat(20_000),
            // This owner-set budget intentionally differs from wallet_core::ProbeBudget.
            max_probe_attempts_per_week: 10,
            max_probe_spend_per_week: Msat(500_000),
            // These scheduler defaults match wallet_core::WatchPolicy.
            base_interval_secs: 10 * 60,
            min_interval_secs: 30,
            evacuation_lead_secs: 60 * 60,
            discover_every_secs: 6 * 60 * 60,
            // Scheduled-probe cadences: match wallet_core::WatchPolicy (the retry backoff
            // is the 5.2c operator knob --probe-retry-backoff-secs; Policy is the sole
            // runtime-mutable home for it under 6a).
            probe_retry_backoff_secs: 60 * 60,
            probe_refresh_lead_secs: 12 * 60 * 60,
            // These discovery defaults match wallet_core::DiscoveryPolicy/WatchPolicy.
            max_auto_joins_per_week: 5,
            auto_join_lifetime_cap: 20,
            max_candidates_per_pass: 256,
            per_preview_timeout_secs: 20,
            discover_pass_deadline_secs: 60,
            auto_join: false,
            require_mainnet: true,
        }
    }
}

impl Policy {
    /// Validate contradictions that would make scheduling or allocation unsafe.
    pub fn validate(&self) -> Result<(), PolicyValidationError> {
        if self.base_interval_secs == 0 {
            return Err(PolicyValidationError::ZeroBaseIntervalSecs);
        }
        if self.min_interval_secs == 0 {
            return Err(PolicyValidationError::ZeroMinIntervalSecs);
        }
        if self.min_interval_secs > self.base_interval_secs {
            return Err(PolicyValidationError::MinIntervalExceedsBaseInterval);
        }
        if self.probe_min_successes == 0 {
            return Err(PolicyValidationError::ZeroProbeMinSuccesses);
        }
        if self.spending_fed.is_some() && self.spending_fed == self.standby_fed {
            return Err(PolicyValidationError::SamePinnedFederation);
        }
        if self.per_fed_cap == Msat(0) {
            return Err(PolicyValidationError::ZeroPerFedCap);
        }
        if self.probe_ttl_secs == 0 {
            return Err(PolicyValidationError::ZeroProbeTtlSecs);
        }
        if self.probe_min_span_secs > self.probe_ttl_secs {
            // Qualifying successes could never span the window while staying inside the
            // ttl: `Passed` becomes silently unreachable while scheduled probes still
            // spend budget.
            return Err(PolicyValidationError::ProbeSpanExceedsTtl);
        }
        if self.spending_target > self.per_fed_cap || self.standby_target > self.per_fed_cap {
            // A target above the cap is self-contradictory: the allocator clamps every
            // fed at `per_fed_cap`, so the target is unreachable and every decide tick
            // emits a fresh OverCap refusal.
            return Err(PolicyValidationError::TargetExceedsPerFedCap);
        }
        if self.probe_retry_backoff_secs == 0 {
            return Err(PolicyValidationError::ZeroProbeRetryBackoffSecs);
        }
        if self.max_fee_bps_of_move == 0 {
            // A zero bps stamps `fee_cap: 0` on every funding move; the executor's pre-mint gate
            // then refuses any nonzero receive quote, so the allocator re-emits the same doomed
            // move each occurrence — a permanent-failure loop. Reject it like the sibling
            // zero-knobs. (A low-but-nonzero bps can still under-cap SMALL moves — that economic
            // floor is br-ljj.3's per-route work, deliberately out of scope here.)
            return Err(PolicyValidationError::ZeroMaxFeeBps);
        }
        if self.max_fee_bps_of_move > 10_000 {
            // Over 100% of the move: fees would exceed the amount moved (nonsensical), and the
            // sizing `amount <= budget * 10000/(10000+bps)` assumes a bounded bps.
            return Err(PolicyValidationError::MaxFeeBpsExceedsCeiling);
        }
        if self.evac_fee_bps > 10_000 {
            // Over 100% of the net delivered: nonsensical, same reasoning as the sibling knob.
            return Err(PolicyValidationError::EvacFeeBpsExceedsCeiling);
        }
        // `evac_fee_bps == 0` is deliberately accepted, unlike `max_fee_bps_of_move`: a base-only
        // evacuation cap is legitimate. Only the pair being zero is invalid.
        if self.evac_fee_base_msat == Msat(0) && self.evac_fee_bps == 0 {
            return Err(PolicyValidationError::ZeroEvacFeeCap);
        }
        Ok(())
    }
}

/// A rejected [`Policy`] field relationship.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyValidationError {
    ZeroBaseIntervalSecs,
    ZeroMinIntervalSecs,
    MinIntervalExceedsBaseInterval,
    ZeroProbeMinSuccesses,
    SamePinnedFederation,
    ZeroPerFedCap,
    ZeroProbeTtlSecs,
    ProbeSpanExceedsTtl,
    TargetExceedsPerFedCap,
    ZeroProbeRetryBackoffSecs,
    MaxFeeBpsExceedsCeiling,
    ZeroMaxFeeBps,
    EvacFeeBpsExceedsCeiling,
    /// The `(evac_fee_base_msat, evac_fee_bps)` PAIR is zero. Either alone may be zero.
    ZeroEvacFeeCap,
}

impl PolicyValidationError {
    pub fn offending_field(&self) -> &'static str {
        match self {
            Self::ZeroBaseIntervalSecs => "base_interval_secs",
            Self::ZeroMinIntervalSecs | Self::MinIntervalExceedsBaseInterval => "min_interval_secs",
            Self::ZeroProbeMinSuccesses => "probe_min_successes",
            Self::SamePinnedFederation => "spending_fed/standby_fed",
            Self::ZeroPerFedCap => "per_fed_cap",
            Self::ZeroProbeTtlSecs => "probe_ttl_secs",
            Self::ProbeSpanExceedsTtl => "probe_min_span_secs/probe_ttl_secs",
            Self::TargetExceedsPerFedCap => "spending_target/standby_target/per_fed_cap",
            Self::ZeroProbeRetryBackoffSecs => "probe_retry_backoff_secs",
            Self::MaxFeeBpsExceedsCeiling | Self::ZeroMaxFeeBps => "max_fee_bps_of_move",
            Self::EvacFeeBpsExceedsCeiling => "evac_fee_bps",
            Self::ZeroEvacFeeCap => "evac_fee_base_msat/evac_fee_bps",
        }
    }
}

impl fmt::Display for PolicyValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBaseIntervalSecs
            | Self::ZeroMinIntervalSecs
            | Self::ZeroProbeMinSuccesses
            | Self::ZeroPerFedCap
            | Self::ZeroProbeTtlSecs
            | Self::ZeroProbeRetryBackoffSecs
            | Self::ZeroMaxFeeBps => {
                write!(formatter, "{} must be non-zero", self.offending_field())
            }
            Self::MinIntervalExceedsBaseInterval => write!(
                formatter,
                "min_interval_secs must not exceed base_interval_secs"
            ),
            Self::SamePinnedFederation => write!(
                formatter,
                "spending_fed/standby_fed must name different federations"
            ),
            Self::ProbeSpanExceedsTtl => write!(
                formatter,
                "{}: the span must not exceed the ttl (Passed would be unreachable)",
                self.offending_field()
            ),
            Self::TargetExceedsPerFedCap => write!(
                formatter,
                "{}: targets must not exceed the per-fed cap",
                self.offending_field()
            ),
            Self::MaxFeeBpsExceedsCeiling => write!(
                formatter,
                "{}: must not exceed 10000 (100% of the move)",
                self.offending_field()
            ),
            Self::EvacFeeBpsExceedsCeiling => write!(
                formatter,
                "{}: must not exceed 10000 (100% of the net delivered)",
                self.offending_field()
            ),
            Self::ZeroEvacFeeCap => write!(
                formatter,
                "{}: must not BOTH be zero (a zero evacuation cap never fits, so the evacuation would retry forever)",
                self.offending_field()
            ),
        }
    }
}

impl std::error::Error for PolicyValidationError {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayRequest {
    pub invoice: String,
    pub amount: Option<Msat>,
    pub fee_cap: Option<Msat>,
    pub fed: Option<FederationId>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveRequest {
    pub from: FederationId,
    pub to: FederationId,
    pub amount: Msat,
    pub fee_cap: Option<Msat>,
    pub occurrence: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiveRequest {
    pub to: Option<FederationId>,
    pub amount: Msat,
    /// Receive-side fee cap; defaults from the Policy. A sizing field: the same-key
    /// attach rule compares it, so a retry after a Policy fee-cap change conflicts
    /// instead of silently attaching under different bounds.
    pub fee_cap: Option<Msat>,
    pub nonce: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectInflowRequest {
    pub to: Option<FederationId>,
    pub amount: Msat,
    /// See [`ReceiveRequest::fee_cap`] — `Action::DirectInflow` carries this bound.
    pub fee_cap: Option<Msat>,
    pub nonce: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRequest {
    pub invite: String,
}

/// Body of `POST /v1/recover`: rebuild a federation's balance from the seed
/// (`docs/archive/wallet-recovery-spec.md`). Mirrors [`JoinRequest`]; the seed must already be present
/// (via `walletd restore-mnemonic` or a prior run).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverRequest {
    pub invite: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApproveRequest {
    pub fed: FederationId,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationAccepted {
    pub operation_key: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReceiveAccepted {
    pub operation_key: String,
    pub invoice: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatusDto {
    Started,
    Awaiting,
    Succeeded,
    Failed,
}

/// The public columns of one operation-ledger history row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationView {
    pub seq: u64,
    pub updated_at_ms: u64,
    pub kind: String,
    pub status: OperationStatusDto,
    pub amount: Option<Msat>,
    pub receive_fee: Option<Msat>,
    pub send_fee_quoted: Option<Msat>,
    pub actor: String,
    pub reason: String,
    pub operation_key: String,
    /// The ledger row's failure diagnostic (audit-honest: cleared on success). Without it a
    /// caller directed to `/v1/operations/{key}` after a failure learns only THAT it failed.
    pub error: Option<String>,
    /// For a structurally superseded evacuation, the fresh child operation key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    /// For a replacement evacuation, the retired parent operation key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    /// For a `refusal` row, the balance/threshold figures the refusal was decided from, so
    /// "why didn't the wallet act?" is answerable from a LIVE daemon (`history`/`show`), not
    /// only from a stopped-wallet journal read. `None` for every other kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<RefusalDiagnostics>,
    /// Exact structural-refusal evidence carried by a still-pending marked evacuation. History
    /// deliberately omits this per-row lookup; `show` projects it for the requested operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evacuation_refusal: Option<wallet_core::EvacuationRefusalEvidence>,
    /// Whether the exact linked intent is an active Pending agent-evacuation marker. `None` means
    /// the display did not resolve a readable exact intent (including history and degraded reads);
    /// `Some(false)` means the exact intent is present but historical or inactive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evacuation_refusal_active: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BalanceResponse {
    pub total: Msat,
    pub federations: Vec<FederationView>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FederationView {
    pub id: FederationId,
    /// `None` means the joined federation could not be opened for this snapshot.
    pub balance: Option<Msat>,
    pub invite: String,
    pub joined_at_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CandidateView {
    pub id: FederationId,
    pub invite: String,
    pub source: String,
    pub discovered_at_ms: u64,
    pub structural: String,
    pub structural_checked_at_ms: u64,
    pub state: String,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HealthView {
    pub actor_queue_depth: usize,
    pub inflight_drivers: usize,
    pub scheduler_alive: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WatchStatusView {
    pub occurrence: u64,
    pub last_discover_ms: u64,
    pub discover_cursor: Option<FederationId>,
    pub discover_backlog: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub operations: Vec<OperationView>,
    pub next_before_seq: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AwaitTarget {
    Terminal,
    InvoiceArtifact,
}

/// A decide-time refusal. No operation was journaled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefuseReason {
    InsufficientAfterReservations,
    FedHeldByProbe,
    OverCap,
    BudgetExhausted,
    SizingConflict {
        field: String,
    },
    AmountRequired,
    StorageError,
    PolicyInvalid,
    /// A tick was planned under an earlier policy generation that a PutPolicy has
    /// since superseded; the whole batch is refused so the next cycle replans.
    PolicySuperseded,
    Conflict,
}

/// A durable terminal failure for an operation that was admitted and journaled.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationFailure {
    pub operation_key: String,
    pub reason: String,
    pub status: OperationStatusDto,
}

/// An HTTP error response body. Client-side transport failures are not represented here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorKind {
    Refused,
    Failed,
    Unauthorized,
    NotFound,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApiError {
    pub kind: ApiErrorKind,
    pub refuse_reason: Option<RefuseReason>,
    /// Present when the error concerns an operation that WAS admitted and journaled
    /// (e.g. an invoice-mint timeout): the durable handle the client can still await or
    /// inspect. Absent for pre-admission refusals.
    pub operation_key: Option<String>,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use std::fmt::Debug;

    fn fed(byte: u8) -> FederationId {
        FederationId([byte; 32])
    }

    fn assert_json_roundtrip<T>(value: T)
    where
        T: Serialize + DeserializeOwned + Debug + PartialEq,
    {
        let encoded = serde_json::to_string(&value).expect("serialize DTO");
        let decoded: T = serde_json::from_str(&encoded).expect("deserialize DTO");
        assert_eq!(decoded, value);
    }

    #[test]
    fn policy_defaults_match_shipped_contract() {
        let policy = Policy::default();
        assert_eq!(policy.per_fed_cap, Msat(1_500_000_000));
        assert_eq!(policy.spending_target, Msat(500_000_000));
        assert_eq!(policy.standby_target, Msat(150_000_000));
        assert_eq!(policy.max_fee, Msat(200_000));
        assert_eq!(policy.max_fee_bps_of_move, 300);
        assert_eq!(policy.evac_fee_base_msat, Msat(200_000));
        assert_eq!(policy.evac_fee_bps, 300);
        assert_eq!(policy.spending_fed, None);
        assert_eq!(policy.standby_fed, None);
        assert_eq!(policy.probe_min_span_secs, 86_400);
        assert_eq!(policy.probe_min_successes, 3);
        assert_eq!(policy.probe_ttl_secs, 604_800);
        assert_eq!(policy.probe_amount, Msat(20_000));
        assert_eq!(policy.max_probe_attempts_per_week, 10);
        assert_eq!(policy.max_probe_spend_per_week, Msat(500_000));
        assert_eq!(policy.base_interval_secs, 600);
        assert_eq!(policy.min_interval_secs, 30);
        assert_eq!(policy.evacuation_lead_secs, 3_600);
        assert_eq!(policy.discover_every_secs, 21_600);
        assert_eq!(policy.probe_retry_backoff_secs, 3_600);
        assert_eq!(policy.probe_refresh_lead_secs, 43_200);
        assert_eq!(policy.max_auto_joins_per_week, 5);
        assert_eq!(policy.auto_join_lifetime_cap, 20);
        assert_eq!(policy.max_candidates_per_pass, 256);
        assert_eq!(policy.per_preview_timeout_secs, 20);
        assert_eq!(policy.discover_pass_deadline_secs, 60);
        assert!(!policy.auto_join);
        assert!(policy.require_mainnet);
    }

    #[test]
    fn policy_json_roundtrip() {
        let policy = Policy {
            spending_fed: Some(fed(1)),
            standby_fed: Some(fed(2)),
            ..Policy::default()
        };
        assert_json_roundtrip(policy);
    }

    #[test]
    fn policy_missing_bps_field_decodes_with_default() {
        // A policy row persisted before `max_fee_bps_of_move` existed must still decode — the
        // daemon's `seed_policy` decodes the stored row BEFORE the actor starts, so a hard
        // failure there would strand the wallet across the upgrade. `#[serde(default)]` adopts
        // the shipped default instead. (`deny_unknown_fields` still rejects EXTRA fields.)
        let mut json = serde_json::to_value(Policy::default()).expect("serialize");
        json.as_object_mut()
            .expect("object")
            .remove("max_fee_bps_of_move");
        let decoded: Policy =
            serde_json::from_value(json).expect("old policy row (no bps field) still decodes");
        assert_eq!(decoded.max_fee_bps_of_move, 300);
    }

    #[test]
    fn policy_missing_evac_fee_fields_decode_with_shipped_defaults() {
        // A row written before these fields existed must receive the shipped values, not numeric
        // zero from a bare serde default.
        let mut json = serde_json::to_value(Policy::default()).expect("serialize");
        let object = json.as_object_mut().expect("object");
        object.remove("evac_fee_base_msat");
        object.remove("evac_fee_bps");
        let decoded: Policy = serde_json::from_value(json)
            .expect("policy row without the evacuation fee fields still decodes");
        assert_eq!(decoded.evac_fee_base_msat, Msat(200_000));
        assert_eq!(decoded.evac_fee_bps, 300);
    }

    #[test]
    fn policy_validation_rejects_each_invalid_rule() {
        let cases = [
            (
                Policy {
                    probe_ttl_secs: 0,
                    probe_min_span_secs: 0,
                    ..Policy::default()
                },
                PolicyValidationError::ZeroProbeTtlSecs,
            ),
            (
                Policy {
                    probe_min_span_secs: 604_801,
                    ..Policy::default()
                },
                PolicyValidationError::ProbeSpanExceedsTtl,
            ),
            (
                Policy {
                    spending_target: Msat(2_000_000_000),
                    ..Policy::default()
                },
                PolicyValidationError::TargetExceedsPerFedCap,
            ),
            (
                Policy {
                    probe_retry_backoff_secs: 0,
                    ..Policy::default()
                },
                PolicyValidationError::ZeroProbeRetryBackoffSecs,
            ),
            (
                Policy {
                    base_interval_secs: 0,
                    ..Policy::default()
                },
                PolicyValidationError::ZeroBaseIntervalSecs,
            ),
            (
                Policy {
                    min_interval_secs: 0,
                    ..Policy::default()
                },
                PolicyValidationError::ZeroMinIntervalSecs,
            ),
            (
                Policy {
                    base_interval_secs: 30,
                    min_interval_secs: 31,
                    ..Policy::default()
                },
                PolicyValidationError::MinIntervalExceedsBaseInterval,
            ),
            (
                Policy {
                    probe_min_successes: 0,
                    ..Policy::default()
                },
                PolicyValidationError::ZeroProbeMinSuccesses,
            ),
            (
                Policy {
                    spending_fed: Some(fed(1)),
                    standby_fed: Some(fed(1)),
                    ..Policy::default()
                },
                PolicyValidationError::SamePinnedFederation,
            ),
            (
                Policy {
                    per_fed_cap: Msat(0),
                    ..Policy::default()
                },
                PolicyValidationError::ZeroPerFedCap,
            ),
            (
                Policy {
                    max_fee_bps_of_move: 10_001,
                    ..Policy::default()
                },
                PolicyValidationError::MaxFeeBpsExceedsCeiling,
            ),
            (
                Policy {
                    max_fee_bps_of_move: 0,
                    ..Policy::default()
                },
                PolicyValidationError::ZeroMaxFeeBps,
            ),
            (
                Policy {
                    evac_fee_base_msat: Msat(0),
                    evac_fee_bps: 0,
                    ..Policy::default()
                },
                PolicyValidationError::ZeroEvacFeeCap,
            ),
            (
                Policy {
                    evac_fee_bps: 10_001,
                    ..Policy::default()
                },
                PolicyValidationError::EvacFeeBpsExceedsCeiling,
            ),
        ];

        for (policy, expected) in cases {
            assert_eq!(policy.validate(), Err(expected.clone()));
            assert!(expected.to_string().contains(expected.offending_field()));
        }
        assert!(PolicyValidationError::ZeroEvacFeeCap
            .offending_field()
            .contains("evac_fee_base_msat"));
        assert!(PolicyValidationError::ZeroEvacFeeCap
            .offending_field()
            .contains("evac_fee_bps"));
        assert_eq!(Policy::default().validate(), Ok(()));
        for policy in [
            Policy {
                spending_fed: Some(fed(1)),
                ..Policy::default()
            },
            Policy {
                standby_fed: Some(fed(1)),
                ..Policy::default()
            },
            // 10_000 bps (100%) is the inclusive ceiling: the rule is `> 10_000`, so the
            // boundary itself must stay accepted.
            Policy {
                max_fee_bps_of_move: 10_000,
                ..Policy::default()
            },
            Policy {
                evac_fee_bps: 0,
                ..Policy::default()
            },
            Policy {
                evac_fee_base_msat: Msat(0),
                ..Policy::default()
            },
            Policy {
                evac_fee_bps: 10_000,
                ..Policy::default()
            },
        ] {
            assert_eq!(policy.validate(), Ok(()));
        }
    }

    #[test]
    fn request_dtos_json_roundtrip() {
        assert_json_roundtrip(PayRequest {
            invoice: "lnbc1example".to_owned(),
            amount: Some(Msat(1_000)),
            fee_cap: Some(Msat(50)),
            fed: Some(fed(1)),
        });
        assert_json_roundtrip(MoveRequest {
            from: fed(1),
            to: fed(2),
            amount: Msat(2_000),
            fee_cap: None,
            occurrence: 4,
        });
        assert_json_roundtrip(ReceiveRequest {
            to: Some(fed(2)),
            amount: Msat(3_000),
            fee_cap: Some(Msat(75)),
            nonce: "receive-1".to_owned(),
        });
        assert_json_roundtrip(DirectInflowRequest {
            to: None,
            amount: Msat(4_000),
            fee_cap: None,
            nonce: "inflow-1".to_owned(),
        });
        assert_json_roundtrip(JoinRequest {
            invite: "fed11example".to_owned(),
        });
        assert_json_roundtrip(ApproveRequest { fed: fed(3) });
    }

    #[test]
    fn request_rejects_unknown_fields() {
        let json =
            r#"{"invoice":"lnbc1example","amount":null,"fee_cap":null,"fed":null,"extra":true}"#;
        let error = serde_json::from_str::<PayRequest>(json).expect_err("unknown field accepted");
        assert!(error.to_string().contains("unknown field `extra`"));
    }

    #[test]
    fn refusal_operation_view_serdes_its_figures() {
        // `RefusalDiagnostics` compares equal always, so a generic round-trip-equality check
        // would pass even if the figures were dropped. Assert them field-by-field, and that a
        // non-refusal view omits the `refusal` object entirely (`skip_serializing_if`).
        let view = OperationView {
            seq: 3,
            updated_at_ms: 1_700_000_000_000,
            kind: "refusal".to_owned(),
            status: OperationStatusDto::Succeeded,
            amount: None,
            receive_fee: None,
            send_fee_quoted: None,
            actor: "agent:1".to_owned(),
            reason: "spending_below_target".to_owned(),
            operation_key: "refuse:spending_below_target:0101:0".to_owned(),
            error: None,
            superseded_by: None,
            supersedes: None,
            refusal: Some(RefusalDiagnostics {
                source: Some(fed(2)),
                want: Some(Msat(50_000)),
                available: Some(Msat(0)),
                source_spendable: Some(Msat(120_000)),
                max_fee: Some(Msat(200_000)),
                max_fee_bps: Some(300),
                cap_room: Some(Msat(96_000)),
                amount: Some(Msat(0)),
                conflict_suppressed: true,
                min_move: Some(Msat(5_000)),
            }),
            evacuation_refusal: None,
            evacuation_refusal_active: None,
        };
        let json = serde_json::to_string(&view).expect("serialize");
        assert!(
            json.contains("\"refusal\""),
            "refusal object present: {json}"
        );
        assert!(
            json.contains("\"max_fee\""),
            "max_fee figure present: {json}"
        );

        let back: OperationView = serde_json::from_str(&json).expect("deserialize");
        let d = back.refusal.expect("refusal figures survive the wire");
        assert_eq!(d.source, Some(fed(2)));
        assert_eq!(d.want, Some(Msat(50_000)));
        assert_eq!(d.available, Some(Msat(0)));
        assert_eq!(d.source_spendable, Some(Msat(120_000)));
        assert_eq!(d.max_fee, Some(Msat(200_000)));
        assert_eq!(d.max_fee_bps, Some(300));
        assert!(d.conflict_suppressed);
        assert_eq!(d.cap_room, Some(Msat(96_000)));
        assert_eq!(d.amount, Some(Msat(0)));
        assert_eq!(d.min_move, Some(Msat(5_000)));
        assert_eq!(
            back.evacuation_refusal_active, None,
            "a legacy/missing wire field remains an unknown display projection"
        );

        let active = OperationView {
            evacuation_refusal_active: Some(true),
            ..view.clone()
        };
        assert_eq!(
            serde_json::to_value(active).expect("serialize active projection")
                ["evacuation_refusal_active"],
            true
        );
        let historical = OperationView {
            evacuation_refusal_active: Some(false),
            ..view.clone()
        };
        assert_eq!(
            serde_json::to_value(historical).expect("serialize inactive projection")
                ["evacuation_refusal_active"],
            false
        );

        // A non-refusal view omits the `refusal` field entirely. Use a `move` view so the
        // string "refusal" cannot appear via `kind`/`operation_key`.
        let move_view = OperationView {
            seq: 4,
            updated_at_ms: 1_700_000_000_000,
            kind: "move".to_owned(),
            status: OperationStatusDto::Awaiting,
            amount: Some(Msat(5_000)),
            receive_fee: None,
            send_fee_quoted: None,
            actor: "user".to_owned(),
            reason: "user_initiated".to_owned(),
            operation_key: "move:example".to_owned(),
            error: None,
            superseded_by: None,
            supersedes: None,
            refusal: None,
            evacuation_refusal: None,
            evacuation_refusal_active: None,
        };
        let move_json = serde_json::to_string(&move_view).expect("serialize non-refusal");
        assert!(
            !move_json.contains("\"refusal\":"),
            "refusal omitted when None: {move_json}"
        );
        assert!(
            !move_json.contains("\"evacuation_refusal_active\":"),
            "unknown marker authority omitted rather than asserted by history: {move_json}"
        );
    }

    #[test]
    fn response_dtos_json_roundtrip() {
        let operation = OperationView {
            seq: 9,
            updated_at_ms: 1_700_000_000_000,
            kind: "move".to_owned(),
            status: OperationStatusDto::Awaiting,
            amount: Some(Msat(5_000)),
            receive_fee: Some(Msat(20)),
            send_fee_quoted: Some(Msat(30)),
            actor: "user".to_owned(),
            reason: "user_initiated".to_owned(),
            operation_key: "move:example".to_owned(),
            error: Some("gateway route rejected".to_owned()),
            superseded_by: None,
            supersedes: None,
            refusal: None,
            evacuation_refusal: None,
            evacuation_refusal_active: None,
        };
        assert_json_roundtrip(OperationAccepted {
            operation_key: "pay:example".to_owned(),
        });
        assert_json_roundtrip(ReceiveAccepted {
            operation_key: "receive:example".to_owned(),
            invoice: "lnbc1example".to_owned(),
        });
        assert_json_roundtrip(operation.clone());
        let federation = FederationView {
            id: fed(1),
            balance: Some(Msat(8_000)),
            invite: "fed11example".to_owned(),
            joined_at_secs: 1_700_000_000,
        };
        assert_json_roundtrip(BalanceResponse {
            total: Msat(8_000),
            federations: vec![federation.clone()],
        });
        assert_json_roundtrip(federation);
        assert_json_roundtrip(CandidateView {
            id: fed(2),
            invite: "fed11candidate".to_owned(),
            source: "observer".to_owned(),
            discovered_at_ms: 10,
            structural: "passed".to_owned(),
            structural_checked_at_ms: 11,
            state: "discovered".to_owned(),
            updated_at_ms: 12,
        });
        assert_json_roundtrip(HealthView {
            actor_queue_depth: 2,
            inflight_drivers: 3,
            scheduler_alive: true,
        });
        assert_json_roundtrip(WatchStatusView {
            occurrence: 7,
            last_discover_ms: 13,
            discover_cursor: Some(fed(3)),
            discover_backlog: true,
        });
        assert_json_roundtrip(HistoryResponse {
            operations: vec![operation],
            next_before_seq: Some(8),
        });
    }

    #[test]
    fn error_and_await_dtos_json_roundtrip() {
        for target in [AwaitTarget::Terminal, AwaitTarget::InvoiceArtifact] {
            assert_json_roundtrip(target);
        }
        let reasons = [
            RefuseReason::InsufficientAfterReservations,
            RefuseReason::FedHeldByProbe,
            RefuseReason::OverCap,
            RefuseReason::BudgetExhausted,
            RefuseReason::SizingConflict {
                field: "amount".to_owned(),
            },
            RefuseReason::AmountRequired,
            RefuseReason::StorageError,
            RefuseReason::PolicyInvalid,
            RefuseReason::Conflict,
        ];
        for reason in reasons {
            assert_json_roundtrip(reason);
        }
        assert_json_roundtrip(OperationFailure {
            operation_key: "pay:failed".to_owned(),
            reason: "gateway rejected payment".to_owned(),
            status: OperationStatusDto::Failed,
        });
        for kind in [
            ApiErrorKind::Refused,
            ApiErrorKind::Failed,
            ApiErrorKind::Unauthorized,
            ApiErrorKind::NotFound,
            ApiErrorKind::Timeout,
        ] {
            assert_json_roundtrip(kind);
        }
        assert_json_roundtrip(ApiError {
            kind: ApiErrorKind::Refused,
            refuse_reason: Some(RefuseReason::OverCap),
            operation_key: None,
            message: "destination would exceed per_fed_cap".to_owned(),
        });
    }
}
