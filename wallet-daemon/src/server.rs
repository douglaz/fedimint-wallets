//! The axum surface: shared [`AppState`], the router, the bearer-token middleware, and the
//! lifecycle (bind → serve → SIGTERM-ordered shutdown). axum lives ONLY in this crate.

use crate::error::HttpError;
use crate::handlers;
use anyhow::Context as _;
use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use wallet_fedimint::{FedimintJournal, MultiClient, Runtime, WalletClient, WalletService};

/// Cloneable shared state handed to every handler. Holds cloneable handles only — the owning
/// [`WalletService`] stays in the lifecycle (`shutdown` consumes it), never in per-request
/// state. `mc`/`runtime` are `None` in the axum fixture tests (no live guardians); the
/// network-touching endpoints degrade explicitly there and are covered at the daemon gate.
#[derive(Clone)]
pub struct AppState {
    pub client: WalletClient,
    pub journal: Arc<FedimintJournal>,
    pub mc: Option<Arc<MultiClient>>,
    pub runtime: Option<Arc<Runtime>>,
    pub scheduler_alive: Arc<AtomicBool>,
    /// The bearer token every route requires (spec P3). A local, same-user secret.
    pub token: Arc<str>,
    /// Invoice-mint hard deadline (spec §6a.6, default 30 s). A field so tests can shorten it.
    pub invoice_deadline: Duration,
    /// Await long-poll default deadline (spec §6a.6, default 60 s).
    pub await_deadline: Duration,
}

/// Build the full router with the bearer-token middleware wrapping EVERY route.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/balance", get(handlers::balance))
        .route("/v1/federations", get(handlers::federations))
        .route("/v1/history", get(handlers::history))
        .route("/v1/operations/{key}", get(handlers::show_operation))
        .route("/v1/status", get(handlers::status))
        .route("/v1/watch/status", get(handlers::watch_status))
        .route("/v1/health", get(handlers::health))
        .route("/v1/pay", post(handlers::pay))
        .route("/v1/move", post(handlers::move_op))
        .route("/v1/receive", post(handlers::receive))
        .route("/v1/direct-inflow", post(handlers::direct_inflow))
        .route("/v1/join", post(handlers::join))
        .route("/v1/approve", post(handlers::approve))
        .route("/v1/candidates", get(handlers::candidates))
        .route("/v1/reconcile", post(handlers::reconcile))
        .route(
            "/v1/policy",
            get(handlers::get_policy).put(handlers::put_policy),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer,
        ))
        .with_state(state)
}

/// Reject any request without a matching `Authorization: Bearer <token>` (spec P3): missing or
/// wrong token → 401 with a `wallet_api::ApiError` body, before the handler runs.
async fn require_bearer(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, HttpError> {
    let provided = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match provided {
        Some(token) if constant_time_eq(token.as_bytes(), state.token.as_bytes()) => {
            Ok(next.run(request).await)
        }
        _ => Err(HttpError::unauthorized()),
    }
}

/// Length-independent-then-content comparison. The trust boundary is same-OS-user (P3), so a
/// timing side channel is not in scope, but a constant-time compare of a bearer secret is
/// cheap and avoids the sharp edge entirely.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Bind the HTTP listener before [`WalletService`] starts. Its scheduler begins its first cycle
/// immediately, so a port conflict must be detected before background work exists to shut down.
pub async fn bind(address: &str) -> anyhow::Result<TcpListener> {
    TcpListener::bind(address)
        .await
        .with_context(|| format!("binding {address}"))
}

/// Serve until SIGTERM/SIGINT, then shut down in the load-bearing order (spec §6a.8):
/// **stop intake → abort drivers FIRST → drain the actor mailbox → exit**. axum's graceful
/// shutdown stops accepting new HTTP; `WalletService::shutdown` then aborts drivers and drains
/// the actor (which drains parked long-poll waiters with an error, so the HTTP drain finishes
/// promptly). Drain-then-abort would let a late driver transition race the exit — do not
/// reorder.
pub async fn run(
    service: WalletService,
    state: AppState,
    listener: TcpListener,
) -> anyhow::Result<()> {
    let mut service = service;
    let address = listener
        .local_addr()
        .context("reading walletd listener address")?;
    tracing::info!(%address, "walletd listening");

    let (signal_tx, signal_rx) = watch::channel(false);

    let serve_signal = signal_rx.clone();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router(state).into_make_service())
            .with_graceful_shutdown(wait_true(serve_signal))
            .await
    });

    let mut server_finished = false;
    let fatal = tokio::select! {
        _ = wait_for_shutdown_signal() => {
            tracing::info!("shutdown signal received; stopping intake");
            None
        }
        result = &mut server => {
            server_finished = true;
            Some(match result {
                Ok(Ok(())) => anyhow::anyhow!("http server exited unexpectedly"),
                Ok(Err(error)) => anyhow::anyhow!("http server failed: {error}"),
                Err(error) => anyhow::anyhow!("http server task panicked: {error}"),
            })
        }
        task = service.critical_task_exit() => {
            Some(anyhow::anyhow!(
                "{} exited unexpectedly",
                task.unwrap_or("critical wallet task monitor"),
            ))
        }
    };
    let _ = signal_tx.send(true);
    // Abort drivers FIRST, then drain the actor mailbox. Restart reconciles the aborted
    // drivers (abandon-and-resume is the model — hours-long IO makes draining them impossible
    // by design).
    let shutdown_result = service.shutdown().await;
    if !server_finished {
        server
            .await
            .context("http server task panicked")?
            .context("http server error")?;
    }
    if let Some(error) = fatal {
        if let Err(shutdown_error) = shutdown_result {
            tracing::warn!(
                ?shutdown_error,
                "wallet service shutdown after fatal task exit failed"
            );
        }
        return Err(error);
    }
    shutdown_result.map_err(|error| anyhow::anyhow!("wallet service shutdown failed: {error}"))?;
    tracing::info!("walletd stopped");
    Ok(())
}

/// Resolve once the shutdown flag flips to `true` (or the sender is dropped).
async fn wait_true(mut rx: watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            break;
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut interrupt = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = term.recv() => {},
            _ = interrupt.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn bind_reports_an_occupied_address_before_service_start() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve test address");
        let address = occupied.local_addr().expect("reserved address");

        let error = super::bind(&address.to_string())
            .await
            .expect_err("second bind unexpectedly succeeded");

        assert!(error.to_string().contains("binding"));
    }
}
