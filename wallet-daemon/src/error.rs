//! The daemon's error boundary: map the layered engine errors (`ServiceError`) and the
//! daemon's own request-validation refusals onto HTTP status + a `wallet_api::ApiError` JSON
//! body. The three galtland layers stay distinctly matchable in the body (spec §6a.6):
//! transport/unavailable, refused (decide-time, nothing journaled), and failed (a journaled
//! operation's terminal), so the step-6 CLI can map each to its own exit code.
//!
//! Status mapping (documented once, applied consistently — spec §6a.6 "pick ONE"):
//! - **401** bad/missing bearer token → `Unauthorized`.
//! - **404** unknown operation key → `NotFound`.
//! - **422** the request itself is malformed or self-contradictory → `Refused` with
//!   `PolicyInvalid` / `AmountRequired` / `SizingConflict`.
//! - **409** the request is well-formed but the CURRENT wallet state refuses it (insufficient
//!   after reservations, held fed, over cap, budget, fail-closed storage read, superseded
//!   policy, admission-cap conflict) → `Refused` with the state reason.
//! - **504** a bounded wait elapsed (the invoice-mint deadline, an await long-poll) →
//!   `Timeout`. Carries the operation key when the op was already admitted.
//! - **503** the actor is stopped or shutting down, OR a FRESH dest-side admission named a
//!   JOINED-but-not-currently-open destination (transport-ish, server side) → `Failed` with no
//!   key; the status code itself is the "unavailable, retry later" signal.
//! - **500** an unexpected server-side storage/read fault → `Failed` with no key.

use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use wallet_api::{ApiError, ApiErrorKind, RefuseReason};
use wallet_fedimint::ServiceError;

/// An HTTP error response: a status code plus the `wallet_api::ApiError` body.
pub struct HttpError {
    pub status: StatusCode,
    pub body: ApiError,
}

impl HttpError {
    fn new(status: StatusCode, kind: ApiErrorKind, message: String) -> Self {
        Self {
            status,
            body: ApiError {
                kind,
                refuse_reason: None,
                operation_key: None,
                message,
            },
        }
    }

    /// A missing/invalid bearer token (spec P3): every route requires it.
    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            ApiErrorKind::Unauthorized,
            "missing or invalid bearer token".to_owned(),
        )
    }

    /// An unknown operation key.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            ApiErrorKind::NotFound,
            message.into(),
        )
    }

    /// The request itself is malformed or self-contradictory in a way with no matching
    /// `RefuseReason` (a bad invoice, `from == to`, an unresolvable federation): 422, refused
    /// layer, no reason code.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: ApiError {
                kind: ApiErrorKind::Refused,
                refuse_reason: None,
                operation_key: None,
                message: message.into(),
            },
        }
    }

    /// The daemon or its dependency is transiently unavailable (actor stopped, a detached
    /// runtime read that faulted): 503, the status code is the "retry later" signal.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ApiErrorKind::Failed,
            message.into(),
        )
    }

    /// A journaled operation reached a terminal FAILED state, surfaced synchronously (e.g. a
    /// receive that terminalized before minting a BOLT11). The "failed" taxonomy layer: not a
    /// 5xx, carries the operation key so the client inspects `/v1/operations/{key}`.
    pub fn failed(operation_key: String, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            body: ApiError {
                kind: ApiErrorKind::Failed,
                refuse_reason: None,
                operation_key: Some(operation_key),
                message: message.into(),
            },
        }
    }

    /// A decide-time refusal produced by the daemon's own request validation (invoice parse,
    /// amount reconciliation, from==to) — nothing was journaled. Same taxonomy layer as an
    /// actor `RefuseReason`, so it shares the refused mapping.
    pub fn refused(reason: RefuseReason, message: impl Into<String>) -> Self {
        Self {
            status: refused_status(&reason),
            body: ApiError {
                kind: ApiErrorKind::Refused,
                refuse_reason: Some(reason),
                operation_key: None,
                message: message.into(),
            },
        }
    }

    /// A bounded-wait deadline elapsed (invoice-mint / await long-poll). `operation_key` is
    /// present when the operation was already admitted and journaled, so the client can still
    /// inspect or re-await it.
    pub fn timeout(message: impl Into<String>, operation_key: Option<String>) -> Self {
        Self {
            status: StatusCode::GATEWAY_TIMEOUT,
            body: ApiError {
                kind: ApiErrorKind::Timeout,
                refuse_reason: None,
                operation_key,
                message: message.into(),
            },
        }
    }
}

/// 422 when the request is inherently invalid/contradictory; 409 when the wallet's current
/// state refuses a well-formed request.
fn refused_status(reason: &RefuseReason) -> StatusCode {
    match reason {
        RefuseReason::PolicyInvalid
        | RefuseReason::AmountRequired
        | RefuseReason::SizingConflict { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        RefuseReason::InsufficientAfterReservations
        | RefuseReason::FedHeldByProbe
        | RefuseReason::OverCap
        | RefuseReason::BudgetExhausted
        | RefuseReason::StorageError
        | RefuseReason::PolicySuperseded
        | RefuseReason::Conflict => StatusCode::CONFLICT,
    }
}

impl From<ServiceError> for HttpError {
    fn from(error: ServiceError) -> Self {
        match error {
            ServiceError::Refused { reason, message } => HttpError::refused(reason, message),
            ServiceError::NotFound(message) => HttpError::not_found(message),
            // A FRESH dest-side admission to a joined-but-unopened federation: 503, the status
            // code is the "retry shortly" signal, carrying the actionable message unchanged.
            ServiceError::DestinationUnavailable(message) => HttpError::unavailable(message),
            ServiceError::Timeout => HttpError::timeout("operation wait deadline elapsed", None),
            ServiceError::ShuttingDown | ServiceError::ActorStopped => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                ApiErrorKind::Failed,
                error.to_string(),
            ),
            ServiceError::Storage(message) => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorKind::Failed,
                message,
            ),
        }
    }
}

impl From<JsonRejection> for HttpError {
    fn from(error: JsonRejection) -> Self {
        HttpError::invalid_request(format!("invalid JSON request body: {}", error.body_text()))
    }
}

// A malformed query string (`?limit=abc`, `?wait=x`) or path segment must produce the same
// `ApiError` JSON body as every other rejection, not axum's default plain-text 400 — the step-6
// CLI maps the body's `kind` to an exit code, so the contract must hold for THIS class too.
impl From<QueryRejection> for HttpError {
    fn from(error: QueryRejection) -> Self {
        HttpError::invalid_request(format!("invalid query parameters: {}", error.body_text()))
    }
}

impl From<PathRejection> for HttpError {
    fn from(error: PathRejection) -> Self {
        HttpError::invalid_request(format!("invalid path parameter: {}", error.body_text()))
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}
