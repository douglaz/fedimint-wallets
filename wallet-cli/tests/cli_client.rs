//! Client-mode CLI tests (spec §6a.9 "CLI: verbs against a mock server") + the standalone error
//! goldens. Each test stands up a tiny axum mock on an ephemeral port, runs the REAL `wallet-cli`
//! binary against it (`--url`/`--token-path`), and asserts (a) the request the CLI sends
//! (path/body), (b) the stdout contract, and (c) the §6a.6 error-taxonomy → exit-code mapping.
//!
//! The mock records every request and returns programmed responses — enough to pin each verb's
//! request shape and the client's response handling without a live daemon (the network-touching
//! money paths are covered by the step-7 daemon gate).

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Router;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wallet_api::{
    ApiError, ApiErrorKind, BalanceResponse, CandidateView, FederationView, HistoryResponse,
    OperationAccepted, OperationStatusDto, OperationView, Policy, ReceiveAccepted, RefuseReason,
};
use wallet_core::{FederationId, Msat};

const BIN: &str = env!("CARGO_BIN_EXE_wallet-cli");

#[derive(Clone, Debug)]
struct Recorded {
    method: String,
    path: String,
    body: String,
}

#[derive(Clone)]
struct MockState {
    log: Arc<Mutex<Vec<Recorded>>>,
    responses: Arc<Mutex<VecDeque<MockResponse>>>,
}

#[derive(Clone)]
struct MockResponse {
    status: u16,
    body: String,
    delay: Duration,
}

async fn mock_handler(State(state): State<MockState>, request: Request) -> impl IntoResponse {
    let method = request.method().to_string();
    let path = request.uri().to_string();
    let bytes: Bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    let body = String::from_utf8_lossy(&bytes).to_string();
    state
        .log
        .lock()
        .unwrap()
        .push(Recorded { method, path, body });
    let response = {
        let mut responses = state.responses.lock().unwrap();
        if responses.len() > 1 {
            responses.pop_front().unwrap()
        } else {
            responses.front().unwrap().clone()
        }
    };
    tokio::time::sleep(response.delay).await;
    (
        StatusCode::from_u16(response.status).unwrap(),
        [(header::CONTENT_TYPE, "application/json")],
        response.body,
    )
}

/// Spawn a one-response mock and return its base URL + the shared request log.
async fn spawn_mock(status: u16, body: String) -> (String, Arc<Mutex<Vec<Recorded>>>) {
    spawn_mock_responses(vec![MockResponse {
        status,
        body,
        delay: Duration::ZERO,
    }])
    .await
}

async fn spawn_mock_responses(responses: Vec<MockResponse>) -> (String, Arc<Mutex<Vec<Recorded>>>) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let state = MockState {
        log: log.clone(),
        responses: Arc::new(Mutex::new(responses.into())),
    };
    let app = Router::new().fallback(mock_handler).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), log)
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique scratch dir under the OS temp dir (no external tempfile dep).
fn scratch() -> PathBuf {
    let unique = format!(
        "wallet-cli-test-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let dir = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_token(dir: &Path) -> PathBuf {
    let path = dir.join("token");
    std::fs::write(&path, "test-bearer-token").unwrap();
    path
}

struct CliOutput {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

/// Run the CLI in client mode against `url`, hermetically (an empty XDG so the real client pointer
/// never interferes), suppressing tracing so stderr carries only the CLI's own `key:`/error lines.
async fn run_client(url: &str, token_path: &Path, xdg: &Path, args: &[&str]) -> CliOutput {
    let mut cmd = tokio::process::Command::new(BIN);
    cmd.env("XDG_CONFIG_HOME", xdg)
        .env("HOME", xdg)
        .env("RUST_LOG", "error")
        .arg("--url")
        .arg(url)
        .arg("--token-path")
        .arg(token_path)
        .args(args);
    let out = cmd.output().await.unwrap();
    CliOutput {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

/// Run the CLI with a raw arg list (no `--url`/`--token-path`), hermetic XDG.
async fn run_raw(xdg: &Path, args: &[&str]) -> CliOutput {
    let out = tokio::process::Command::new(BIN)
        .env("XDG_CONFIG_HOME", xdg)
        .env_remove("XDG_DATA_HOME")
        .env("HOME", xdg)
        .env("RUST_LOG", "error")
        .args(args)
        .output()
        .await
        .unwrap();
    CliOutput {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

fn fed(byte: u8) -> FederationId {
    FederationId([byte; 32])
}

fn json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap()
}

fn api_error(kind: ApiErrorKind, message: &str) -> String {
    json(&ApiError {
        kind,
        refuse_reason: None,
        operation_key: None,
        message: message.to_owned(),
    })
}

// ---- reads ------------------------------------------------------------------------------------

#[tokio::test]
async fn balance_hits_v1_balance_and_prints_rows_and_total() {
    let dir = scratch();
    let token = write_token(&dir);
    let body = json(&BalanceResponse {
        total: Msat(8_000),
        federations: vec![FederationView {
            id: fed(0xab),
            balance: Some(Msat(8_000)),
            invite: "fed11example".to_owned(),
            joined_at_secs: 100,
        }],
    });
    let (url, log) = spawn_mock(200, body).await;

    let out = run_client(&url, &token, &dir, &["balance"]).await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout
            .contains(&format!("{}: 8000 msat", fed(0xab).to_hex())),
        "stdout: {}",
        out.stdout
    );
    assert!(out.stdout.contains("total (1/1 federations): 8000 msat"));
    let req = log.lock().unwrap()[0].clone();
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/v1/balance");
}

#[tokio::test]
async fn balance_partial_view_prints_diagnostics_and_exits_nonzero() {
    let dir = scratch();
    let token = write_token(&dir);
    let body = json(&BalanceResponse {
        total: Msat(8_000),
        federations: vec![
            FederationView {
                id: fed(0xab),
                balance: Some(Msat(8_000)),
                invite: "fed11open".to_owned(),
                joined_at_secs: 100,
            },
            FederationView {
                id: fed(0xcd),
                balance: None,
                invite: "fed11unavailable".to_owned(),
                joined_at_secs: 101,
            },
        ],
    });
    let (url, _) = spawn_mock(200, body).await;

    let out = run_client(&url, &token, &dir, &["balance"]).await;

    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(out.stdout.contains("unavailable (failed to open)"));
    assert!(out.stdout.contains("total (1/2 federations): 8000 msat"));
    assert!(out.stderr.contains("total above covers only the open set"));
}

#[tokio::test]
async fn history_filters_before_limit_by_following_the_cursor() {
    let dir = scratch();
    let token = write_token(&dir);
    let mut newest = succeeded_view("pay:newest");
    newest.seq = 4;
    let mut next = succeeded_view("pay:next");
    next.seq = 3;
    let mut failed_one = succeeded_view("pay:failed-one");
    failed_one.seq = 2;
    failed_one.status = OperationStatusDto::Failed;
    let mut failed_two = succeeded_view("pay:failed-two");
    failed_two.seq = 1;
    failed_two.status = OperationStatusDto::Failed;
    let first = json(&HistoryResponse {
        operations: vec![newest, next],
        next_before_seq: Some(3),
    });
    let second = json(&HistoryResponse {
        operations: vec![failed_one, failed_two],
        next_before_seq: None,
    });
    let (url, log) = spawn_mock_responses(vec![
        MockResponse {
            status: 200,
            body: first,
            delay: Duration::ZERO,
        },
        MockResponse {
            status: 200,
            body: second,
            delay: Duration::ZERO,
        },
    ])
    .await;

    let out = run_client(
        &url,
        &token,
        &dir,
        &["history", "--status", "failed", "--limit", "2"],
    )
    .await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("pay:failed-one"),
        "stdout: {}",
        out.stdout
    );
    assert!(
        out.stdout.contains("pay:failed-two"),
        "stdout: {}",
        out.stdout
    );
    assert!(!out.stdout.contains("pay:newest"), "stdout: {}", out.stdout);
    let log = log.lock().unwrap();
    assert_eq!(log.len(), 2);
    assert!(log[0].path.contains("/v1/history?limit=2"));
    assert!(
        log[1].path.contains("before_seq=3"),
        "path: {}",
        log[1].path
    );
}

#[tokio::test]
async fn candidates_client_mode_sorted_newest_first() {
    let dir = scratch();
    let token = write_token(&dir);
    // The daemon returns the registry in raw DB-key order (here: the older row first). The client
    // must re-sort newest-first (descending `updated_at_ms`) to match the CLI contract and
    // `--standalone`.
    let older = CandidateView {
        id: fed(0x11),
        invite: "fed11older".to_owned(),
        source: "observer".to_owned(),
        discovered_at_ms: 10,
        structural: "valid".to_owned(),
        structural_checked_at_ms: 10,
        state: "discovered".to_owned(),
        updated_at_ms: 10,
    };
    let newer = CandidateView {
        id: fed(0x22),
        invite: "fed11newer".to_owned(),
        source: "observer".to_owned(),
        discovered_at_ms: 20,
        structural: "valid".to_owned(),
        structural_checked_at_ms: 20,
        state: "discovered".to_owned(),
        updated_at_ms: 20,
    };
    let (url, _) = spawn_mock(200, json(&vec![older, newer])).await;

    let out = run_client(&url, &token, &dir, &["candidates"]).await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    let newer_pos = out
        .stdout
        .find(&fed(0x22).to_hex())
        .expect("newer row present");
    let older_pos = out
        .stdout
        .find(&fed(0x11).to_hex())
        .expect("older row present");
    assert!(
        newer_pos < older_pos,
        "candidates must render newest-first; stdout: {}",
        out.stdout
    );
}

// ---- status: fail-loud parity with balance / --standalone (§15.8) -----------------------------

#[tokio::test]
async fn status_all_open_exits_zero() {
    let dir = scratch();
    let token = write_token(&dir);
    let status_body =
        r#"{"spending_fed":"aa","standby_fed":null,"decisions":[],"scored":[]}"#.to_owned();
    let federations = json(&vec![FederationView {
        id: fed(0xaa),
        balance: Some(Msat(5_000)),
        invite: "fed11open".to_owned(),
        joined_at_secs: 1,
    }]);
    let (url, log) = spawn_mock_responses(vec![
        MockResponse {
            status: 200,
            body: status_body,
            delay: Duration::ZERO,
        },
        MockResponse {
            status: 200,
            body: federations,
            delay: Duration::ZERO,
        },
    ])
    .await;

    let out = run_client(&url, &token, &dir, &["status"]).await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("spending_fed: aa"),
        "stdout: {}",
        out.stdout
    );
    let log = log.lock().unwrap();
    assert_eq!(log[0].path, "/v1/status");
    assert_eq!(log[1].path, "/v1/federations");
}

#[tokio::test]
async fn status_partial_open_exits_nonzero_and_prints_unopened() {
    let dir = scratch();
    let token = write_token(&dir);
    // `status` GETs /v1/status (scores only the OPEN set) then /v1/federations to detect any
    // joined-but-unopened fed. A partial universe fails loud (§15.8), like `--standalone status`.
    let status_body =
        r#"{"spending_fed":"aa","standby_fed":null,"decisions":[],"scored":[]}"#.to_owned();
    let federations = json(&vec![
        FederationView {
            id: fed(0xaa),
            balance: Some(Msat(5_000)),
            invite: "fed11open".to_owned(),
            joined_at_secs: 1,
        },
        FederationView {
            id: fed(0xbb),
            balance: None,
            invite: "fed11unopened".to_owned(),
            joined_at_secs: 2,
        },
    ]);
    let (url, _) = spawn_mock_responses(vec![
        MockResponse {
            status: 200,
            body: status_body,
            delay: Duration::ZERO,
        },
        MockResponse {
            status: 200,
            body: federations,
            delay: Duration::ZERO,
        },
    ])
    .await;

    let out = run_client(&url, &token, &dir, &["status"]).await;

    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains(&format!(
            "{}: unavailable (failed to open)",
            fed(0xbb).to_hex()
        )),
        "stdout: {}",
        out.stdout
    );
    assert!(
        out.stderr.contains("failed to open"),
        "stderr: {}",
        out.stderr
    );
}

// ---- phase-1 write (pay): body + stdout + `key:` on stderr -------------------------------------

#[tokio::test]
async fn pay_posts_v1_pay_and_prints_started_key() {
    let dir = scratch();
    let token = write_token(&dir);
    let body = json(&OperationAccepted {
        operation_key: "pay:deadbeef".to_owned(),
    });
    let (url, log) = spawn_mock(202, body).await;

    let out = run_client(
        &url,
        &token,
        &dir,
        &[
            "pay",
            "lnbc1invoice",
            "--amount",
            "1000",
            "--fee-cap",
            "50",
            "--fed",
            &fed(1).to_hex(),
        ],
    )
    .await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    // Phase-1 contract: `started <key>` on stdout, `key: <key>` on stderr.
    assert_eq!(out.stdout.trim(), "started pay:deadbeef");
    assert!(
        out.stderr.contains("key: pay:deadbeef"),
        "stderr: {}",
        out.stderr
    );
    let req = log.lock().unwrap()[0].clone();
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/v1/pay");
    assert!(
        req.body.contains("\"invoice\":\"lnbc1invoice\""),
        "body: {}",
        req.body
    );
    assert!(req.body.contains("\"amount\":1000"), "body: {}", req.body);
    assert!(req.body.contains("\"fee_cap\":50"), "body: {}", req.body);
}

// ---- block-for-invoice (receive): invoice on stdout, key on stderr ----------------------------

#[tokio::test]
async fn receive_posts_v1_receive_and_prints_invoice_then_key() {
    let dir = scratch();
    let token = write_token(&dir);
    let body = json(&ReceiveAccepted {
        operation_key: "receive:xyz".to_owned(),
        invoice: "lnbc1minted".to_owned(),
    });
    let (url, log) = spawn_mock(200, body).await;

    let out = run_client(
        &url,
        &token,
        &dir,
        &["receive", "--amount", "2000", "--nonce", "abc"],
    )
    .await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert_eq!(out.stdout.trim(), "lnbc1minted");
    assert!(
        out.stderr.contains("key: receive:xyz"),
        "stderr: {}",
        out.stderr
    );
    let req = log.lock().unwrap()[0].clone();
    assert_eq!(req.path, "/v1/receive");
    assert!(req.body.contains("\"nonce\":\"abc\""), "body: {}", req.body);
}

// ---- await (GET /v1/operations/{key}?wait=true): terminal words + exit codes -------------------

#[tokio::test]
async fn await_send_succeeded_prints_success() {
    let dir = scratch();
    let token = write_token(&dir);
    let body = json(&succeeded_view("pay:k"));
    let (url, log) = spawn_mock(200, body).await;

    let out = run_client(
        &url,
        &token,
        &dir,
        &["await-send", "pay:k", "--timeout", "5"],
    )
    .await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert_eq!(out.stdout.trim(), "success");
    let req = log.lock().unwrap()[0].clone();
    assert_eq!(req.method, "GET");
    assert!(
        req.path.starts_with("/v1/operations/pay:k"),
        "path: {}",
        req.path
    );
    assert!(req.path.contains("wait=true"), "path: {}", req.path);
}

#[tokio::test]
async fn await_move_succeeded_prints_done() {
    let dir = scratch();
    let token = write_token(&dir);
    let mut view = succeeded_view("move:k");
    view.kind = "move".to_owned();
    let (url, _) = spawn_mock(200, json(&view)).await;
    let out = run_client(
        &url,
        &token,
        &dir,
        &["await-move", "move:k", "--timeout", "5"],
    )
    .await;
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert_eq!(out.stdout.trim(), "done");
}

#[tokio::test]
async fn await_send_rejects_a_key_of_the_wrong_kind() {
    // A valid key from the WRONG verb (a settled receive handed to `await-send`) must not print
    // `success` — automation gating on the stdout word would report a payment that never ran.
    let dir = scratch();
    let token = write_token(&dir);
    let mut view = succeeded_view("receive:k");
    view.kind = "receive".to_owned();
    let (url, _) = spawn_mock(200, json(&view)).await;
    let out = run_client(
        &url,
        &token,
        &dir,
        &["await-send", "receive:k", "--timeout", "5"],
    )
    .await;
    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(out.stdout.trim().is_empty(), "stdout: {}", out.stdout);
    assert!(
        out.stderr.contains("`receive` operation") && out.stderr.contains("await-send"),
        "stderr: {}",
        out.stderr
    );
}

#[tokio::test]
async fn await_send_failed_prints_failed_and_exits_3() {
    let dir = scratch();
    let token = write_token(&dir);
    let mut view = succeeded_view("pay:k");
    view.status = OperationStatusDto::Failed;
    view.error = Some("gateway rejected".to_owned());
    let (url, _) = spawn_mock(200, json(&view)).await;

    let out = run_client(
        &url,
        &token,
        &dir,
        &["await-send", "pay:k", "--timeout", "5"],
    )
    .await;

    // A journaled terminal FAILED is exit 3 (the durable-failure taxonomy layer).
    assert_eq!(out.code, Some(3), "stderr: {}", out.stderr);
    assert_eq!(out.stdout.trim(), "failed: gateway rejected");
}

#[tokio::test]
async fn await_timeout_bounds_a_slow_server_long_poll() {
    let dir = scratch();
    let token = write_token(&dir);
    let mut pending = succeeded_view("pay:pending");
    pending.status = OperationStatusDto::Started;
    let (url, _) = spawn_mock_responses(vec![MockResponse {
        status: 200,
        body: json(&pending),
        delay: Duration::from_secs(5),
    }])
    .await;
    let started = std::time::Instant::now();

    let out = run_client(
        &url,
        &token,
        &dir,
        &["await-send", "pay:pending", "--timeout", "1"],
    )
    .await;

    assert_eq!(out.code, Some(4), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("await timed out after 1s"));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "caller timeout was not enforced promptly: {:?}",
        started.elapsed()
    );
}

// ---- policy get/set ---------------------------------------------------------------------------

#[tokio::test]
async fn policy_get_prints_policy_json() {
    let dir = scratch();
    let token = write_token(&dir);
    let (url, log) = spawn_mock(200, json(&Policy::default())).await;

    let out = run_client(&url, &token, &dir, &["policy", "get"]).await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("\"per_fed_cap\": 1500000000"),
        "stdout: {}",
        out.stdout
    );
    assert_eq!(log.lock().unwrap()[0].method, "GET");
    assert_eq!(log.lock().unwrap()[0].path, "/v1/policy");
}

#[tokio::test]
async fn policy_set_puts_the_edited_field() {
    let dir = scratch();
    let token = write_token(&dir);
    // GET returns default; the CLI edits the named fields and PUTs the whole struct back.
    let (url, log) = spawn_mock(200, json(&Policy::default())).await;

    let out = run_client(
        &url,
        &token,
        &dir,
        &[
            "policy",
            "set",
            "--per-fed-cap",
            "999000",
            "--max-fee-bps-of-move",
            "250",
        ],
    )
    .await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    let log = log.lock().unwrap();
    // First a GET, then a PUT carrying the edited struct.
    assert_eq!(log[0].method, "GET");
    let put = log
        .iter()
        .find(|r| r.method == "PUT")
        .expect("a PUT /v1/policy");
    assert_eq!(put.path, "/v1/policy");
    assert!(
        put.body.contains("\"per_fed_cap\":999000"),
        "put body: {}",
        put.body
    );
    // The proportional funding-move cap lands on its OWN field, not on the absolute `max_fee`.
    assert!(
        put.body.contains("\"max_fee_bps_of_move\":250"),
        "put body: {}",
        put.body
    );
}

// ---- reconcile --------------------------------------------------------------------------------

#[tokio::test]
async fn reconcile_posts_and_prints_counts() {
    let dir = scratch();
    let token = write_token(&dir);
    let body = r#"{"redriven":2,"awaiters_rehydrated":1,"executing_normalized":0}"#.to_owned();
    let (url, log) = spawn_mock(200, body).await;

    let out = run_client(&url, &token, &dir, &["reconcile"]).await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(out.stdout.contains("redriven=2"), "stdout: {}", out.stdout);
    assert_eq!(log.lock().unwrap()[0].method, "POST");
    assert_eq!(log.lock().unwrap()[0].path, "/v1/reconcile");
}

// ---- exit-code goldens: refused / failed / transport / 401 ------------------------------------

#[tokio::test]
async fn refused_maps_to_exit_2() {
    let dir = scratch();
    let token = write_token(&dir);
    let body = json(&ApiError {
        kind: ApiErrorKind::Refused,
        refuse_reason: Some(RefuseReason::OverCap),
        operation_key: None,
        message: "destination would exceed per_fed_cap".to_owned(),
    });
    let (url, _) = spawn_mock(409, body).await;

    let out = run_client(&url, &token, &dir, &["pay", "lnbc1x", "--amount", "1"]).await;

    assert_eq!(out.code, Some(2), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("refused:"), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("per_fed_cap"), "stderr: {}", out.stderr);
}

#[tokio::test]
async fn failed_maps_to_exit_3() {
    let dir = scratch();
    let token = write_token(&dir);
    let body = json(&ApiError {
        kind: ApiErrorKind::Failed,
        refuse_reason: None,
        operation_key: Some("pay:k".to_owned()),
        message: "the operation terminalized without a payable invoice".to_owned(),
    });
    let (url, _) = spawn_mock(409, body).await;

    let out = run_client(&url, &token, &dir, &["receive", "--amount", "1"]).await;

    assert_eq!(out.code, Some(3), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("failed:"), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("pay:k"), "stderr: {}", out.stderr);
}

#[tokio::test]
async fn service_unavailable_maps_to_transport_exit_4() {
    let dir = scratch();
    let token = write_token(&dir);
    let (url, _) = spawn_mock(
        503,
        api_error(ApiErrorKind::Failed, "wallet service actor stopped"),
    )
    .await;

    let out = run_client(&url, &token, &dir, &["balance"]).await;

    assert_eq!(out.code, Some(4), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("wallet service actor stopped"));
    assert!(!out.stderr.contains("failed:"));
}

#[tokio::test]
async fn server_error_failed_maps_to_transport_not_exit_3() {
    let dir = scratch();
    let token = write_token(&dir);
    // A 500 storage/read fault is `Failed` with NO operation key (spec §6a.6). It maps to the
    // transport layer (exit 4), never exit 3 — nothing was journaled, so a script must not treat an
    // internal server fault as a durable operation terminal.
    let (url, _) = spawn_mock(
        500,
        api_error(ApiErrorKind::Failed, "unexpected storage read fault"),
    )
    .await;

    let out = run_client(&url, &token, &dir, &["balance"]).await;

    assert_eq!(out.code, Some(4), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("unexpected storage read fault"));
    assert!(!out.stderr.contains("failed:"), "stderr: {}", out.stderr);
}

#[tokio::test]
async fn non_json_server_error_maps_to_transport_exit_4() {
    let dir = scratch();
    let token = write_token(&dir);
    // walletd's own errors are always a JSON `ApiError`, but an intermediary (or an unexpected
    // 5xx) can return an HTML/plain-text body. A 503/500 is a server-side transient, so it must
    // stay on the transport layer (exit 4) — NOT collapse to a usage error (exit 1) just because
    // the body is not JSON. Consistent with the JSON-5xx case above.
    let (url, _) = spawn_mock(
        503,
        "<html><body>503 Service Unavailable</body></html>".to_owned(),
    )
    .await;

    let out = run_client(&url, &token, &dir, &["balance"]).await;

    assert_eq!(out.code, Some(4), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("503"),
        "stderr should surface the status: {}",
        out.stderr
    );
}

#[tokio::test]
async fn unauthorized_maps_to_exit_5() {
    let dir = scratch();
    let token = write_token(&dir);
    let (url, _) = spawn_mock(401, api_error(ApiErrorKind::Unauthorized, "bad token")).await;

    let out = run_client(&url, &token, &dir, &["balance"]).await;

    assert_eq!(out.code, Some(5), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("auth error"), "stderr: {}", out.stderr);
}

#[tokio::test]
async fn daemon_not_running_maps_to_exit_4_with_two_options() {
    let dir = scratch();
    let token = write_token(&dir);
    // A closed port: a connection-refused failure is the not-running case.
    let out = run_client("http://127.0.0.1:1", &token, &dir, &["balance"]).await;

    assert_eq!(out.code, Some(4), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("walletd is not running"),
        "stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr
            .contains("start walletd, or rerun with --standalone"),
        "stderr: {}",
        out.stderr
    );
}

#[tokio::test]
async fn missing_pointer_maps_to_exit_4() {
    let dir = scratch();
    // No `--url`, and the hermetic XDG has no client pointer → the not-running error, exit 4.
    let out = run_raw(&dir, &["balance"]).await;

    assert_eq!(out.code, Some(4), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("walletd is not running"),
        "stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr
            .contains("start walletd, or rerun with --standalone"),
        "stderr: {}",
        out.stderr
    );
}

#[tokio::test]
async fn explicit_url_and_token_do_not_load_the_client_pointer() {
    let dir = scratch();
    let token = write_token(&dir);
    let pointer_dir = dir.join("walletd");
    std::fs::create_dir_all(&pointer_dir).unwrap();
    std::fs::write(pointer_dir.join("client.toml"), "not valid toml = [").unwrap();
    let body = json(&BalanceResponse {
        total: Msat(0),
        federations: vec![],
    });
    let (url, _) = spawn_mock(200, body).await;

    let out = run_client(&url, &token, &dir, &["balance"]).await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert_eq!(out.stdout.trim(), "total (0/0 federations): 0 msat");
}

#[tokio::test]
async fn status_overrides_are_rejected_in_client_mode() {
    let dir = scratch();
    let out = run_raw(&dir, &["status", "--per-fed-cap", "5"]).await;

    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("status overrides require --standalone"));
}

#[tokio::test]
async fn data_dir_in_client_mode_is_rejected() {
    let dir = scratch();
    // `--data-dir` selects the STANDALONE store; passing it WITHOUT `--standalone` must fail loud
    // (exit 1) rather than silently target the configured daemon's wallet — a spend-from-the-wrong-
    // wallet footgun. The guard fires BEFORE resolving the client pointer, so the hermetic (empty)
    // XDG never turns this into the not-running error instead.
    let out = run_raw(&dir, &["--data-dir", dir.to_str().unwrap(), "balance"]).await;

    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("--data-dir") && out.stderr.contains("--standalone"),
        "stderr: {}",
        out.stderr
    );
    assert!(
        !out.stderr.contains("walletd is not running"),
        "the data-dir guard must precede pointer resolution; stderr: {}",
        out.stderr
    );
}

#[tokio::test]
async fn numeric_show_is_explicitly_standalone_only() {
    let dir = scratch();
    let out = run_raw(&dir, &["show", "123"]).await;

    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(out
        .stderr
        .contains("show by numeric sequence requires --standalone"));
}

// ---- standalone goldens -----------------------------------------------------------------------

#[tokio::test]
async fn clap_usage_errors_use_exit_1_not_refused_code() {
    let dir = scratch();

    let unknown = run_raw(&dir, &["--definitely-not-a-wallet-flag"]).await;
    assert_eq!(unknown.code, Some(1), "stderr: {}", unknown.stderr);
    assert!(
        unknown.stderr.contains("unexpected argument"),
        "{}",
        unknown.stderr
    );

    let missing = run_raw(&dir, &["pay"]).await;
    assert_eq!(missing.code, Some(1), "stderr: {}", missing.stderr);
    assert!(
        missing.stderr.contains("required arguments"),
        "{}",
        missing.stderr
    );
}

#[tokio::test]
async fn clap_help_keeps_success_exit_code() {
    let dir = scratch();
    let out = run_raw(&dir, &["--help"]).await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(out.stdout.contains("Usage:"), "stdout: {}", out.stdout);
}

#[tokio::test]
async fn standalone_defaults_to_walletd_data_directory() {
    let dir = scratch();
    let out = run_raw(&dir, &["--standalone", "balance"]).await;

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert_eq!(out.stdout.trim(), "total (0/0 federations): 0 msat");
    assert!(
        dir.join(".local/share/walletd/client.db").exists(),
        "standalone should open walletd's default XDG store"
    );
    assert!(
        !dir.join(".wallet-cli-data").exists(),
        "the retired CWD-relative store must not be created"
    );
}

#[tokio::test]
async fn standalone_uses_custom_data_dir_from_walletd_host_config() {
    let dir = scratch();
    let data_dir = dir.join("custom-wallet-store");
    std::fs::create_dir_all(&data_dir).unwrap();
    let config_dir = dir.join("walletd");
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(
        config_dir.join("walletd.toml"),
        format!(
            "data_dir = {:?}\nport = 9736\n",
            data_dir.display().to_string()
        ),
    )
    .unwrap();

    // Holding the configured store's lock proves the no-argument standalone path selected that
    // store. A regression to the XDG data default would instead create a fresh wallet and exit 0.
    let lock_path = data_dir.join("client.db.lock");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&lock_path)
        .unwrap();
    let _held = fs_lock::FileLock::new_try_exclusive(file).expect("acquire configured store lock");

    let out = run_raw(&dir, &["--standalone", "balance"]).await;

    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("another process owns the wallet store"),
        "stderr: {}",
        out.stderr
    );
    assert!(
        !dir.join(".local/share/walletd/client.db").exists(),
        "standalone must not mint a fresh default wallet when walletd.toml selects another store"
    );
}

#[tokio::test]
async fn standalone_only_verb_refused_in_client_mode() {
    let dir = scratch();
    // `discover` has no daemon endpoint (§6a.6); in client mode it refuses with the two-options
    // hint rather than silently falling back. No server needed — it fails before any HTTP.
    let out = run_raw(&dir, &["discover"]).await;

    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("standalone-only verb"),
        "stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("--standalone"),
        "stderr: {}",
        out.stderr
    );
}

#[tokio::test]
async fn standalone_lock_held_errors_with_exit_1() {
    let dir = scratch();
    let data_dir = dir.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    // Hold the SAME advisory lock fedimint's store uses (`<data_dir>/client.db.lock`), so the CLI's
    // non-blocking pre-check sees it held — exactly the "walletd owns the store" case.
    let lock_path = data_dir.join("client.db.lock");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&lock_path)
        .unwrap();
    let _held = fs_lock::FileLock::new_try_exclusive(file).expect("acquire the lock in-test");

    let out = run_raw(
        &dir,
        &[
            "--standalone",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "balance",
        ],
    )
    .await;

    assert_eq!(out.code, Some(1), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("another process owns the wallet store"),
        "stderr: {}",
        out.stderr
    );
}

/// Exercise the REAL `--standalone` `WalletService` bring-up (take the lock, open the store, start
/// the actor + drivers — but NOT the watch scheduler, §6a.7 — run the verb through the
/// `WalletClient` command path, shut down) offline — no federations, no network. `policy
/// get`/`set`/`health`/`reconcile` all work with an empty store, so they pin the standalone actor
/// path without a devimint gate.
#[tokio::test]
async fn standalone_policy_get_set_and_health_roundtrip() {
    let dir = scratch();
    let data = dir.join("data");
    let data_arg = data.to_str().unwrap();

    // `policy get` on a fresh store seeds + prints the default policy.
    let out = run_raw(
        &dir,
        &["--standalone", "--data-dir", data_arg, "policy", "get"],
    )
    .await;
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("\"per_fed_cap\": 1500000000"),
        "stdout: {}",
        out.stdout
    );

    // `policy set` journals an edit through the actor's PutPolicy path.
    let out = run_raw(
        &dir,
        &[
            "--standalone",
            "--data-dir",
            data_arg,
            "policy",
            "set",
            "--spending-target",
            "123000",
        ],
    )
    .await;
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("\"spending_target\": 123000"),
        "stdout: {}",
        out.stdout
    );

    // A fresh process re-reads the SAME store: the edit persisted (the actor seeds insert-if-absent).
    let out = run_raw(
        &dir,
        &["--standalone", "--data-dir", data_arg, "policy", "get"],
    )
    .await;
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("\"spending_target\": 123000"),
        "stdout: {}",
        out.stdout
    );

    // `health` reflects the one-shot in-process service. The standalone service starts the actor
    // + drivers but NOT the watch scheduler (§6a.7), so it reports `scheduler_alive=false`: a
    // one-shot CLI command must not run the background rebalancer.
    let out = run_raw(&dir, &["--standalone", "--data-dir", data_arg, "health"]).await;
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("actor_queue_depth="),
        "stdout: {}",
        out.stdout
    );
    assert!(
        out.stdout.contains("scheduler_alive=false"),
        "standalone must not run the watch scheduler; stdout: {}",
        out.stdout
    );

    // `reconcile` runs the actor re-drive AND the off-actor ledger repair (mirroring the daemon's
    // `/v1/reconcile` handler, which is `client.reconcile()` + `journal.repair_ledger`). Against the
    // empty store both are no-ops, so it exits 0 with zero counts — pinning that the standalone
    // recovery path (the only one when walletd is down) actually runs the repair without faulting.
    let out = run_raw(&dir, &["--standalone", "--data-dir", data_arg, "reconcile"]).await;
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("redriven=0")
            && out.stdout.contains("awaiters_rehydrated=0")
            && out.stdout.contains("executing_normalized=0"),
        "stdout: {}",
        out.stdout
    );
}

fn succeeded_view(key: &str) -> OperationView {
    OperationView {
        seq: 1,
        updated_at_ms: 1_700_000_000_000,
        kind: "pay".to_owned(),
        status: OperationStatusDto::Succeeded,
        amount: Some(Msat(1_000)),
        receive_fee: None,
        send_fee_quoted: None,
        actor: "user".to_owned(),
        reason: "user_initiated".to_owned(),
        operation_key: key.to_owned(),
        error: None,
        refusal: None,
    }
}
