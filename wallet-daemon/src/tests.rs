//! axum surface tests (spec §6a.9): per-endpoint happy + error paths against an in-process
//! `WalletService` over a `MemDatabase` fixture journal, `runtime`/`mc` = `None` (no live
//! guardians). Driven with `tower::ServiceExt::oneshot` — no sockets, deterministic.
//!
//! COVERAGE GAP (covered by the daemon gate, step 7): every network-touching path is exercised
//! there, not here — the fresh-admission money path (`/v1/pay`, fresh `/v1/move`) which needs a
//! real BOLT11 + live balances, `/v1/status` (probes the network), and `/v1/balance` /
//! `/v1/federations` with live client balances. Here we prove the HTTP contract itself: 401,
//! the 202 contract, the invoice-mint deadline, and policy get/put.

use crate::server::{router, AppState};
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt as _;
use wallet_api::Policy;
use wallet_core::{
    Action, Actor, AllocatorDecision, ExecError, Executor, FederationId, Intent, IntentStatus,
    Journal, Msat, Occurrence, PerformOutcome, ReasonCode,
};
use wallet_fedimint::{
    move_key, raw_receive_key, CandidateRecord, CandidateState, FederationInfo, FedimintJournal,
    Invoice, MultiClient, StructuralOutcome, WalletService,
};

const TOKEN: &str = "test-bearer-token";

/// Parks forever without journaling an invoice artifact, so a receive/direct-inflow
/// `InvoiceArtifact` wait exercises the deadline path (the "not a hang" contract) and
/// pay/move drivers simply stay in flight.
struct PendingExecutor;

#[async_trait::async_trait]
impl Executor for PendingExecutor {
    async fn perform(&self, _intent: &Intent) -> Result<PerformOutcome, ExecError> {
        std::future::pending().await
    }
}

fn fed(byte: u8) -> FederationId {
    FederationId([byte; 32])
}

fn fixture_policy() -> Policy {
    Policy {
        per_fed_cap: Msat(1_000),
        spending_target: Msat(100),
        standby_target: Msat(100),
        ..Policy::default()
    }
}

async fn fixture() -> (AppState, WalletService, Arc<FedimintJournal>) {
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt as _;
    let journal = Arc::new(FedimintJournal::new(MemDatabase::new().into_database()));
    journal
        .put_federation(
            &fed(2),
            &FederationInfo {
                invite: "fixture federation".to_owned(),
                db_prefix: 2,
                joined_at: 0,
            },
        )
        .await
        .expect("seed joined federation");
    let service =
        WalletService::start_detached(journal.clone(), Arc::new(PendingExecutor), fixture_policy())
            .await
            .expect("start detached fixture service");
    let state = AppState {
        client: service.client(),
        journal: journal.clone(),
        mc: None,
        runtime: None,
        scheduler_alive: service.scheduler_liveness(),
        token: Arc::from(TOKEN),
        // Short deadlines so the invoice-mint / long-poll timeout paths finish fast.
        invoice_deadline: Duration::from_millis(120),
        await_deadline: Duration::from_millis(120),
    };
    (state, service, journal)
}

/// Like [`fixture`], but with a REAL (empty) `MultiClient` attached. fed(2) stays JOINED in the
/// journal yet is ABSENT from `mc.federations()` (no client is ever opened) — i.e. joined-but-not-
/// open, the exact state the dest-side 503 fail-fast (br-u2o) targets. Building the mc performs no
/// I/O (mirrors wallet-fedimint's own mc unit tests), so this remains a deterministic, socketless
/// in-process test.
async fn fixture_with_unopened_dest() -> (AppState, WalletService, Arc<FedimintJournal>) {
    use fedimint_bip39::Mnemonic;
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt as _;
    let (mut state, service, journal) = fixture().await;
    let mnemonic = Mnemonic::from_entropy(&[0u8; 16]).expect("valid 12-word entropy");
    let mc = Arc::new(
        MultiClient::new(
            MemDatabase::new().into_database(),
            MemDatabase::new().into_database(),
            mnemonic,
        )
        .await,
    );
    // No `open_all` — the open set is empty, so every joined fed reads as joined-but-not-open.
    assert!(mc.federations().is_empty());
    state.mc = Some(mc);
    (state, service, journal)
}

fn request(method: &str, uri: &str, token: Option<&str>, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    match body {
        Some(value) => builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&value).expect("serialize body"),
            ))
            .expect("build request"),
        None => builder.body(Body::empty()).expect("build request"),
    }
}

async fn send(state: &AppState, request: Request<Body>) -> (StatusCode, Value) {
    let response = router(state.clone())
        .oneshot(request)
        .await
        .expect("router response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

#[tokio::test]
async fn every_route_requires_the_bearer_token() {
    let (state, service, _) = fixture().await;
    // Missing token.
    let (status, body) = send(&state, request("GET", "/v1/health", None, None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["kind"], "unauthorized");
    // Wrong token, on a mutating route.
    let (status, _) = send(
        &state,
        request("GET", "/v1/policy", Some("wrong-token"), None),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Correct token is NOT rejected by the auth layer.
    let (status, _) = send(&state, request("GET", "/v1/health", Some(TOKEN), None)).await;
    assert_eq!(status, StatusCode::OK);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn move_returns_202_operation_key_fast() {
    // The fresh-admission money path needs live balances (daemon gate); here we prove the 202
    // contract via the same-key attach path: a pre-seeded Pending move dedups and returns its
    // operation key — which IS the ledger correlation key — with a 202.
    let (state, service, journal) = fixture().await;
    // Both endpoints must be joined now that `move` validates membership up front like its
    // sibling verbs; the fixture seeds fed(2), so join fed(1) as the source too.
    journal
        .put_federation(
            &fed(1),
            &FederationInfo {
                invite: "fixture source federation".to_owned(),
                db_prefix: 1,
                joined_at: 0,
            },
        )
        .await
        .expect("seed joined source federation");
    let fee_cap = Msat(5);
    let key = move_key(&fed(1), &fed(2), Msat(10), fee_cap, Occurrence(0));
    let decision = AllocatorDecision {
        action: Action::Move {
            from: fed(1),
            to: fed(2),
            amount: Msat(10),
            fee_cap,
        },
        reason: ReasonCode::UserInitiated,
        occurrence: Occurrence(0),
        idempotency_key: key.clone(),
    };
    journal
        .upsert(&Intent::from_decision(&decision, Actor::User, 1))
        .await
        .expect("seed pending move");

    let body = json!({
        "from": fed(1),
        "to": fed(2),
        "amount": Msat(10),
        "fee_cap": fee_cap,
        "occurrence": 0,
    });
    let (status, response) =
        send(&state, request("POST", "/v1/move", Some(TOKEN), Some(body))).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(response["operation_key"], key.0);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn move_with_equal_from_and_to_is_rejected() {
    let (state, service, _) = fixture().await;
    let body = json!({
        "from": fed(1),
        "to": fed(1),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "occurrence": 0,
    });
    let (status, response) =
        send(&state, request("POST", "/v1/move", Some(TOKEN), Some(body))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response["kind"], "refused");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn move_rejects_an_unjoined_endpoint_before_admission() {
    // `move` validates from/to membership synchronously, like pay/receive/direct-inflow: a
    // destination the wallet has not joined is a 422 refusal before admission, nothing journaled
    // — not a 202 that only fails asynchronously in the driver. The fixture joins fed(2); fed(9)
    // is unjoined.
    let (state, service, journal) = fixture().await;
    journal
        .put_federation(
            &fed(1),
            &FederationInfo {
                invite: "fixture source federation".to_owned(),
                db_prefix: 1,
                joined_at: 0,
            },
        )
        .await
        .expect("seed joined source federation");
    let body = json!({
        "from": fed(1),
        "to": fed(9),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "occurrence": 0,
    });
    let (status, response) =
        send(&state, request("POST", "/v1/move", Some(TOKEN), Some(body))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response["kind"], "refused");
    assert!(response["message"]
        .as_str()
        .expect("message")
        .contains("is not joined"));
    assert!(journal.history(10, None).await.expect("history").is_empty());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn malformed_and_unknown_json_fields_use_the_api_error_body() {
    let (state, service, _) = fixture().await;
    let malformed = Request::builder()
        .method("POST")
        .uri("/v1/move")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{"))
        .expect("malformed request");
    let (status, body) = send(&state, malformed).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["kind"], "refused");

    let body = json!({
        "from": fed(1),
        "to": fed(2),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "occurrence": 0,
        "unexpected": true,
    });
    let (status, response) =
        send(&state, request("POST", "/v1/move", Some(TOKEN), Some(body))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response["kind"], "refused");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn receive_invoice_deadline_returns_timeout_not_a_hang() {
    let (state, service, _) = fixture().await;
    // This fixture attaches NO runtime (`mc: None`), so the destination-openness signal is
    // unknown and the dest-side 503 fail-fast is (correctly) NOT armed — admission proceeds
    // exactly as before. A fresh receive admits (no source balance needed), spawns a driver that
    // never mints an invoice, so the InvoiceArtifact wait must return a Timeout ApiError within
    // the deadline. This is the "not a hang" deadline contract; the joined-but-UNOPENED path
    // (which now fail-fasts to 503, never reaching this deadline) is covered separately below.
    let body = json!({
        "to": fed(2),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "nonce": "recv-nonce-1",
    });
    let (status, response) = send(
        &state,
        request("POST", "/v1/receive", Some(TOKEN), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(response["kind"], "timeout");
    // The op was admitted+journaled, so the timeout carries its key for later inspection.
    assert!(response["operation_key"].is_string());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fresh_receive_to_joined_but_unopened_destination_returns_503() {
    // br-u2o: a FRESH receive whose destination is JOINED but not currently open fails fast with
    // 503 at admission — an actionable "retry shortly" — instead of admitting a Pending row that
    // stalls ~the invoice-mint deadline before a 504. fed(2) is joined in the journal but absent
    // from the (empty) mc open set.
    let (state, service, journal) = fixture_with_unopened_dest().await;
    let body = json!({
        "to": fed(2),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "nonce": "recv-unopened",
    });
    let (status, response) = send(
        &state,
        request("POST", "/v1/receive", Some(TOKEN), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response["kind"], "failed");
    assert!(response["message"]
        .as_str()
        .expect("message")
        .contains("joined but not currently open"));
    // Fail-fast BEFORE admission: nothing is journaled (contrast the admitted-then-stall it
    // replaces), so no money-side state exists and the caller simply retries.
    assert!(journal.history(10, None).await.expect("history").is_empty());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fresh_direct_inflow_to_joined_but_unopened_destination_returns_503() {
    // br-u2o: the direct-inflow sibling of the receive fail-fast — same joined-but-not-open 503.
    let (state, service, journal) = fixture_with_unopened_dest().await;
    let body = json!({
        "to": fed(2),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "nonce": "di-unopened",
    });
    let (status, response) = send(
        &state,
        request("POST", "/v1/direct-inflow", Some(TOKEN), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response["kind"], "failed");
    assert!(response["message"]
        .as_str()
        .expect("message")
        .contains("joined but not currently open"));
    assert!(journal.history(10, None).await.expect("history").is_empty());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn fresh_move_to_joined_but_unopened_destination_returns_503() {
    // br-u2o: `move`'s DESTINATION is gated the same way. Both endpoints are joined (fed(1)
    // source, fed(2) dest) so this is NOT the unjoined-422 path; the dest is joined-but-not-open,
    // so admission fails fast with 503. Source openness is intentionally NOT gated.
    let (state, service, journal) = fixture_with_unopened_dest().await;
    journal
        .put_federation(
            &fed(1),
            &FederationInfo {
                invite: "fixture source federation".to_owned(),
                db_prefix: 1,
                joined_at: 0,
            },
        )
        .await
        .expect("seed joined source federation");
    let body = json!({
        "from": fed(1),
        "to": fed(2),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "occurrence": 0,
    });
    let (status, response) =
        send(&state, request("POST", "/v1/move", Some(TOKEN), Some(body))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response["kind"], "failed");
    assert!(response["message"]
        .as_str()
        .expect("message")
        .contains("joined but not currently open"));
    assert!(journal.history(10, None).await.expect("history").is_empty());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn existing_receive_still_returns_its_minted_invoice_while_destination_closed() {
    // Preserved (br-u2o MUST-PRESERVE): an already-admitted receive whose invoice was already
    // minted stays retrievable via an idempotent replay even after `to` closes. The replay's key
    // already exists, so the actor takes the ATTACH path before the FRESH openness gate — the
    // minted BOLT11 is returned (200), never a 503. Openness gates the FRESH branch only.
    let (state, service, journal) = fixture_with_unopened_dest().await;
    let to = fed(2);
    let key = raw_receive_key(to, Msat(10), "recv-existing");
    let decision = AllocatorDecision {
        action: Action::Receive {
            to,
            amount: Msat(10),
            fee_cap: Msat(5),
            nonce: "recv-existing".to_owned(),
            gateway: None,
        },
        reason: ReasonCode::UserInitiated,
        occurrence: Occurrence(0),
        idempotency_key: key.clone(),
    };
    let mut intent = Intent::from_decision(&decision, Actor::User, 0);
    intent.status = IntentStatus::Awaiting;
    intent.invoice = Some(Invoice("lnbc-minted-fixture".to_owned()));
    journal
        .upsert(&intent)
        .await
        .expect("seed already-minted receive");

    let body = json!({
        "to": to,
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "nonce": "recv-existing",
    });
    let (status, response) = send(
        &state,
        request("POST", "/v1/receive", Some(TOKEN), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["operation_key"], key.0);
    assert_eq!(response["invoice"], "lnbc-minted-fixture");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn receive_rejects_an_explicit_unjoined_federation_before_admission() {
    let (state, service, journal) = fixture().await;
    let body = json!({
        "to": fed(9),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "nonce": "unknown-fed",
    });
    let (status, response) = send(
        &state,
        request("POST", "/v1/receive", Some(TOKEN), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response["kind"], "refused");
    assert!(response["message"]
        .as_str()
        .expect("message")
        .contains("is not joined"));
    assert!(journal.history(10, None).await.expect("history").is_empty());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn approve_maps_missing_and_wrong_candidate_state_to_request_errors() {
    use fedimint_core::invite_code::InviteCode;
    use fedimint_core::util::SafeUrl;
    use fedimint_core::PeerId;
    use std::str::FromStr as _;

    let (state, service, journal) = fixture().await;
    let missing = fed(7);
    let (status, body) = send(
        &state,
        request(
            "POST",
            "/v1/approve",
            Some(TOKEN),
            Some(json!({ "fed": missing })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["kind"], "not_found");

    let approved = fed(8);
    let invite_fed = fedimint_core::config::FederationId::from_str(&approved.to_hex())
        .expect("valid federation id");
    journal
        .put_candidate(&CandidateRecord {
            id: approved,
            invite: InviteCode::new(
                SafeUrl::parse("https://fixture.example").expect("valid URL"),
                PeerId::from(0),
                invite_fed,
                None,
            ),
            source: wallet_core::DiscoverySource::Manual,
            discovered_at_ms: 0,
            structural: StructuralOutcome::Passed,
            structural_checked_at_ms: 0,
            state: CandidateState::UserApproved,
            updated_at_ms: 0,
        })
        .await
        .expect("seed approved candidate");
    let (status, body) = send(
        &state,
        request(
            "POST",
            "/v1/approve",
            Some(TOKEN),
            Some(json!({ "fed": approved })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["kind"], "refused");
    assert_eq!(body["refuse_reason"], "conflict");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn put_policy_validation_names_the_offending_field() {
    let (state, service, _) = fixture().await;
    let mut invalid = fixture_policy();
    invalid.per_fed_cap = Msat(0);
    let (status, response) = send(
        &state,
        request(
            "PUT",
            "/v1/policy",
            Some(TOKEN),
            Some(serde_json::to_value(&invalid).unwrap()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response["kind"], "refused");
    assert_eq!(response["refuse_reason"], "policy_invalid");
    assert!(response["message"]
        .as_str()
        .expect("message string")
        .contains("per_fed_cap"));
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn get_policy_round_trips_a_put() {
    let (state, service, _) = fixture().await;
    let mut updated = fixture_policy();
    updated.per_fed_cap = Msat(2_000);
    updated.spending_target = Msat(250);
    let (status, stored) = send(
        &state,
        request(
            "PUT",
            "/v1/policy",
            Some(TOKEN),
            Some(serde_json::to_value(&updated).unwrap()),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let stored: Policy = serde_json::from_value(stored).expect("stored policy");
    assert_eq!(stored, updated);

    let (status, fetched) = send(&state, request("GET", "/v1/policy", Some(TOKEN), None)).await;
    assert_eq!(status, StatusCode::OK);
    let fetched: Policy = serde_json::from_value(fetched).expect("fetched policy");
    assert_eq!(fetched, updated);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn unknown_operation_key_is_404() {
    let (state, service, _) = fixture().await;
    let (status, body) = send(
        &state,
        request("GET", "/v1/operations/pay:missing", Some(TOKEN), None),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["kind"], "not_found");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn health_reports_the_observability_shape() {
    let (state, service, _) = fixture().await;
    let (status, body) = send(&state, request("GET", "/v1/health", Some(TOKEN), None)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["actor_queue_depth"].is_number());
    assert!(body["inflight_drivers"].is_number());
    // A detached fixture has no scheduler, so liveness is honestly `false`.
    assert_eq!(body["scheduler_alive"], false);
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn init_policy_seed_is_insert_if_absent_across_reinit() {
    // `walletd init` seeds the default Policy via step-4's insert-if-absent; a re-init (token
    // rotation) must NEVER reset an existing policy. This exercises that exact contract.
    use fedimint_core::db::mem_impl::MemDatabase;
    use fedimint_core::db::IRawDatabaseExt as _;
    let journal = FedimintJournal::new(MemDatabase::new().into_database());
    journal
        .seed_policy(&Policy::default())
        .await
        .expect("first init seeds defaults");
    let edited = Policy {
        max_fee: Msat(321_000),
        ..Policy::default()
    };
    journal
        .put_policy(&edited)
        .await
        .expect("user edits policy");
    // Re-init (as `walletd init` does again): insert-if-absent keeps the edited policy.
    journal
        .seed_policy(&Policy::default())
        .await
        .expect("re-init seeds insert-if-absent");
    assert_eq!(
        journal.get_policy().await.expect("read policy"),
        Some(edited)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn init_does_not_rotate_the_token_before_acquiring_the_database_lock() {
    use crate::{config, open_db, run_init};
    use std::sync::atomic::{AtomicU64, Ordering};

    let _env = config::TEST_ENV_LOCK.lock().await;
    std::env::remove_var(config::TOKEN_PATH_ENV);
    static SCRATCH_ID: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "walletd-init-lock-{}-{}",
        std::process::id(),
        SCRATCH_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let data_dir = dir.join("data");
    let token_path = dir.join("token");
    let config_path = dir.join("walletd.toml");
    std::fs::create_dir_all(&data_dir).expect("create fixture data dir");
    std::fs::write(&token_path, "running-daemon-token").expect("seed token");
    std::fs::write(
        &config_path,
        format!(
            "data_dir = {:?}\naddress = \"127.0.0.1\"\nport = 9736\ntoken_path = {:?}\nlog_level = \"info\"\n",
            data_dir.display().to_string(),
            token_path.display().to_string(),
        ),
    )
    .expect("write fixture config");

    let config = config::load(&config_path).expect("load fixture config");
    let database_lock = open_db(&config).await.expect("hold database lock");
    let init_path = config_path.clone();
    let init = tokio::spawn(async move { run_init(&init_path).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        std::fs::read_to_string(&token_path).expect("read unchanged token"),
        "running-daemon-token"
    );
    drop(database_lock);
    tokio::time::timeout(Duration::from_secs(5), init)
        .await
        .expect("init resumes after the lock is released")
        .expect("init task")
        .expect("init succeeds");
    assert_ne!(
        std::fs::read_to_string(&token_path).expect("read rotated token"),
        "running-daemon-token"
    );
}

#[tokio::test]
async fn history_and_watch_status_read_detached() {
    let (state, service, _) = fixture().await;
    let (status, body) = send(&state, request("GET", "/v1/history", Some(TOKEN), None)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["operations"], json!([]));
    assert_eq!(body["next_before_seq"], Value::Null);

    // The watch-status view reads the durable `0x0a` row (defaulted when absent).
    let (status, body) = send(
        &state,
        request("GET", "/v1/watch/status", Some(TOKEN), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["occurrence"], 0);
    assert_eq!(body["discover_backlog"], false);
    service.shutdown().await.expect("shutdown");
}

/// A signed, parseable, AMOUNTLESS BOLT11 (with a payment secret — modern parsing requires one;
/// the classic spec donation vector predates that and fails semantic checks).
fn amountless_invoice() -> String {
    use bitcoin::hashes::{sha256, Hash as _};
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let key = bitcoin::secp256k1::SecretKey::from_slice(&[41; 32]).expect("static test key");
    lightning_invoice::InvoiceBuilder::new(lightning_invoice::Currency::Bitcoin)
        .description("amountless fixture".to_owned())
        .payment_hash(sha256::Hash::hash(&[7; 32]))
        .payment_secret(lightning_invoice::PaymentSecret([42; 32]))
        .current_timestamp()
        .min_final_cltv_expiry_delta(144)
        .build_signed(|hash| secp.sign_ecdsa_recoverable(hash, &key))
        .expect("build amountless invoice")
        .to_string()
}

#[tokio::test]
async fn pay_refuses_an_amountless_invoice_even_with_a_stated_amount() {
    // The pinned lnv2 send API cannot supply an amount, so admitting an amountless invoice
    // would 202 an operation whose driver can only fail (step-6 review P1).
    let (state, service, _) = fixture().await;
    let invoice = amountless_invoice();
    for body in [
        json!({ "invoice": invoice, "amount": 1_000, "fee_cap": null, "fed": null }),
        json!({ "invoice": invoice, "amount": null, "fee_cap": null, "fed": null }),
    ] {
        let (status, response) =
            send(&state, request("POST", "/v1/pay", Some(TOKEN), Some(body))).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(response["refuse_reason"], "amount_required");
        assert!(
            response["message"]
                .as_str()
                .unwrap_or_default()
                .contains("not payable"),
            "body: {response}"
        );
    }
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn show_surfaces_the_failure_diagnostic_of_a_failed_operation() {
    let (state, service, journal) = fixture().await;
    // A terminal-failed ledger row with a diagnostic, exactly what a failed driver leaves.
    let key = wallet_core::IdempotencyKey("tick:7:diagnostic".to_owned());
    journal
        .record_tick_started(&key, Occurrence(7), 1)
        .await
        .expect("open the tick row");
    journal
        .record_tick_terminal(
            &key,
            None,
            wallet_core::OperationStatus::Failed,
            Some("gateway route rejected"),
            2,
        )
        .await
        .expect("terminalize the tick row");
    let (status, body) = send(
        &state,
        request("GET", "/v1/operations/tick:7:diagnostic", Some(TOKEN), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "failed");
    // The actionable diagnostic must reach the caller, not just THAT it failed.
    assert_eq!(body["error"], "gateway route rejected");
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn malformed_query_param_uses_the_api_error_body() {
    // A bad query param (`?limit=abc`) must produce the uniform `ApiError` JSON body, not axum's
    // default plain-text 400 — the step-6 CLI maps `kind` to an exit code for this class too.
    let (state, service, _) = fixture().await;
    let (status, body) = send(
        &state,
        request("GET", "/v1/history?limit=abc", Some(TOKEN), None),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["kind"], "refused");
    assert!(body["message"].is_string());
    service.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn receive_rejects_a_nonce_with_reserved_characters() {
    // A nonce carrying `/` would derive an operation key that can't round-trip as a single
    // `/v1/operations/{key}` path segment; reject it synchronously, nothing journaled.
    let (state, service, journal) = fixture().await;
    let body = json!({
        "to": fed(2),
        "amount": Msat(10),
        "fee_cap": Msat(5),
        "nonce": "bad/nonce",
    });
    let (status, response) = send(
        &state,
        request("POST", "/v1/receive", Some(TOKEN), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response["kind"], "refused");
    assert!(response["message"]
        .as_str()
        .expect("message")
        .contains("nonce"));
    assert!(journal.history(10, None).await.expect("history").is_empty());
    service.shutdown().await.expect("shutdown");
}
