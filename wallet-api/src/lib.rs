//! Wire contracts shared by wallet API servers and clients.

use serde::{Deserialize, Serialize};
use std::fmt;
pub use wallet_core::{FederationId, Msat};

/// The standing instruction's user-owned allocation and automation parameters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub per_fed_cap: Msat,
    pub spending_target: Msat,
    pub standby_target: Msat,
    pub max_fee: Msat,
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
            | Self::ZeroProbeRetryBackoffSecs => {
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
    SizingConflict { field: String },
    AmountRequired,
    StorageError,
    PolicyInvalid,
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
        ];

        for (policy, expected) in cases {
            assert_eq!(policy.validate(), Err(expected.clone()));
            assert!(expected.to_string().contains(expected.offending_field()));
        }
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
