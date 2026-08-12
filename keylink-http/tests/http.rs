#![allow(clippy::expect_used)]

use axum::{
    middleware::{self, Next},
    response::Response,
    Router,
};
use http::Request;
use iroh::{endpoint::presets, Endpoint};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use keylink::{Allowlist, Context, Node};
use keylink_http::{Access, AccessError, Call, Challenge, Gateway, JsonLimits, Principal};

keylink::service! {
    service Inventory;

    rpc Add(left: u64, right: u64) -> u64;
    rpc Count(value: u64) -> u64;
    tell Ignored(value: u64) -> ();
}

#[derive(Clone, Default)]
struct Handler {
    runs: Arc<AtomicU64>,
}

impl inventory::Handler for Handler {
    async fn add(&self, _cx: &Context, left: u64, right: u64) -> u64 {
        left + right
    }

    async fn count(&self, _cx: &Context, _value: u64) -> u64 {
        self.runs.fetch_add(1, Ordering::SeqCst) + 1
    }

    async fn ignored(&self, _cx: &Context, _value: u64) {}
}

#[derive(Clone)]
struct Allow;

impl Access for Allow {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
        Ok(())
    }
}

#[derive(Clone)]
struct AppState;

async fn authenticate(mut request: Request<axum::body::Body>, next: Next) -> Response {
    if let Some(namespace) = request.headers().get("x-test-principal").cloned() {
        request
            .extensions_mut()
            .insert(Principal::new(namespace.as_bytes()).expect("test principal is valid"));
    }
    next.run(request).await
}

async fn local_node() -> Node {
    Node::from_endpoint(
        Endpoint::bind(presets::Minimal)
            .await
            .expect("a local endpoint binds"),
    )
}

struct Running {
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    _keylink: keylink::Server,
    _nodes: Vec<Node>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start<A: Access>(handler: Handler, access: A, limits: Option<JsonLimits>) -> Running {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let keylink =
        inventory::serve(&server_node, handler, Allowlist::anyone()).expect("service starts");
    let client = inventory::Client::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("client connects");
    let challenge = Challenge::new(http::HeaderValue::from_static("Bearer realm=keylink"))
        .expect("valid challenge");
    let gateway = Gateway::<inventory::External, _>::new(client, access, challenge);
    let gateway = if let Some(limits) = limits {
        gateway.limits(limits)
    } else {
        gateway
    }
    .router::<AppState>();
    let app: Router<AppState> = Router::new().nest("/v1/inventory", gateway);
    let app = app
        .layer(middleware::from_fn(authenticate))
        .with_state(AppState);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("listener address");
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Running {
        address,
        task,
        _keylink: keylink,
        _nodes: vec![server_node, client_node],
    }
}

#[tokio::test]
async fn valid_authenticated_http_call_reaches_keylink_and_returns_only_the_value() {
    let running = start(Handler::default(), Allow, None).await;
    let response = reqwest::Client::new()
        .post(format!("http://{}/v1/inventory/rpc/Add", running.address))
        .header("content-type", "application/json")
        .header("x-test-principal", "tenant-a")
        .body(r#"{"left":2,"right":3}"#)
        .send()
        .await
        .expect("HTTP call completes");

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .expect("content type"),
        "application/json"
    );
    assert_eq!(response.text().await.expect("body"), "5");
}

#[derive(Clone, Default)]
struct CountingAccess(Arc<AtomicU64>);

impl Access for CountingAccess {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn error_code(response: reqwest::Response) -> (http::StatusCode, String, HeaderMap) {
    let status = response.status();
    let headers = response.headers().clone();
    let body: serde_json::Value = response.json().await.expect("JSON error");
    let code = body["error"]["code"]
        .as_str()
        .expect("string error code")
        .to_owned();
    (status, code, headers)
}

use http::HeaderMap;

#[tokio::test]
async fn unknown_and_non_rpc_methods_bypass_access() {
    let access = CountingAccess::default();
    let running = start(Handler::default(), access.clone(), None).await;
    let client = reqwest::Client::new();

    for method in ["Missing", "Ignored", "__Status"] {
        let response = client
            .post(format!(
                "http://{}/v1/inventory/rpc/{method}",
                running.address
            ))
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("HTTP call completes");
        let (status, code, _) = error_code(response).await;
        assert_eq!(
            (status, code.as_str()),
            (http::StatusCode::NOT_FOUND, "method_not_found")
        );
    }
    assert_eq!(access.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn adapter_owned_failures_are_json_with_stable_status_headers_and_codes() {
    let running = start(Handler::default(), Allow, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://{}/v1/inventory", running.address);

    let missing_principal = client
        .post(format!("{base}/rpc/Add"))
        .header("content-type", "application/json")
        .body(r#"{"left":1,"right":2}"#)
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, headers) = error_code(missing_principal).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::UNAUTHORIZED, "unauthenticated")
    );
    assert_eq!(
        headers[http::header::WWW_AUTHENTICATE],
        "Bearer realm=keylink"
    );

    let wrong_verb = client
        .get(format!("{base}/rpc/Add"))
        .header("x-test-principal", "tenant-a")
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, headers) = error_code(wrong_verb).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed")
    );
    assert_eq!(headers[http::header::ALLOW], "POST");

    let no_content_type = client
        .post(format!("{base}/rpc/Add"))
        .header("x-test-principal", "tenant-a")
        .body(r#"{"left":1,"right":2}"#)
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, _) = error_code(no_content_type).await;
    assert_eq!(
        (status, code.as_str()),
        (
            http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type"
        )
    );

    let bad_key = client
        .post(format!("{base}/rpc/Add"))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .header("idempotency-key", "bad key")
        .body(r#"{\"left\":1,\"right\":2}"#)
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, _) = error_code(bad_key).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::BAD_REQUEST, "invalid_idempotency_key")
    );

    let malformed = client
        .post(format!("{base}/rpc/Add"))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/problem+json; charset=utf-8")
        .body("{")
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, _) = error_code(malformed).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::BAD_REQUEST, "invalid_request")
    );

    for path in ["/rpc", "/rpc/Add/", "/rpc/Add/extra"] {
        let response = client
            .post(format!("{base}{path}"))
            .header("x-test-principal", "tenant-a")
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("HTTP call completes");
        let (status, code, _) = error_code(response).await;
        assert_eq!(
            (status, code.as_str()),
            (http::StatusCode::NOT_FOUND, "method_not_found")
        );
    }
}

#[derive(Clone)]
struct Deny(AccessError);

impl Access for Deny {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
        Err(self.0)
    }
}

#[tokio::test]
async fn access_denial_precedes_json_validation_and_uses_only_the_cached_challenge() {
    let forbidden = start(Handler::default(), Deny(AccessError::Forbidden), None).await;
    let response = reqwest::Client::new()
        .post(format!("http://{}/v1/inventory/rpc/Add", forbidden.address))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, headers) = error_code(response).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::FORBIDDEN, "forbidden")
    );
    assert!(!headers.contains_key(http::header::WWW_AUTHENTICATE));

    let unauthenticated = start(Handler::default(), Deny(AccessError::Unauthenticated), None).await;
    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/inventory/rpc/Add",
            unauthenticated.address
        ))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, headers) = error_code(response).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::UNAUTHORIZED, "unauthenticated")
    );
    assert_eq!(
        headers[http::header::WWW_AUTHENTICATE],
        "Bearer realm=keylink"
    );
}

#[tokio::test]
async fn request_and_success_response_limits_are_inclusive() {
    let body = r#"{"left":2,"right":3}"#;
    let running = start(
        Handler::default(),
        Allow,
        Some(JsonLimits::new(body.len(), 1).expect("positive limits")),
    )
    .await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/inventory/rpc/Add", running.address);
    let response = client
        .post(&url)
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("HTTP call completes");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "5");

    let response = client
        .post(&url)
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .body(format!("{body} "))
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, _) = error_code(response).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::PAYLOAD_TOO_LARGE, "request_too_large")
    );

    let large_response = start(
        Handler::default(),
        Allow,
        Some(JsonLimits::new(1024, 1).expect("positive limits")),
    )
    .await;
    let response = client
        .post(format!(
            "http://{}/v1/inventory/rpc/Add",
            large_response.address
        ))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .body(r#"{"left":10,"right":2}"#)
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, _) = error_code(response).await;
    assert_eq!(
        (status, code.as_str()),
        (
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "response_too_large"
        )
    );
}

/// Watched red with `scripts/break-once.sh` by replacing
/// `    hasher.update(principal);` with an empty namespace update. The exact async
/// test terminated with the second principal replaying `1` instead of running as `2`.
#[tokio::test]
async fn external_idempotency_keys_are_namespaced_by_principal() {
    let handler = Handler::default();
    let runs = Arc::clone(&handler.runs);
    let running = start(handler, Allow, None).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/inventory/rpc/Count", running.address);

    for (principal, expected) in [("tenant-a", "1"), ("tenant-a", "1"), ("tenant-b", "2")] {
        let response = client
            .post(&url)
            .header("x-test-principal", principal)
            .header("content-type", "application/json")
            .header("idempotency-key", "operation-7")
            .body(r#"{"value":7}"#)
            .send()
            .await
            .expect("HTTP call completes");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.text().await.expect("body"), expected);
    }
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reusing_one_key_for_a_different_method_is_never_http_success() {
    let running = start(Handler::default(), Allow, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://{}/v1/inventory/rpc", running.address);
    let first = client
        .post(format!("{base}/Add"))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .header("idempotency-key", "one-call")
        .body(r#"{"left":1,"right":2}"#)
        .send()
        .await
        .expect("HTTP call completes");
    assert_eq!(first.status(), http::StatusCode::OK);

    let wrong = client
        .post(format!("{base}/Count"))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .header("idempotency-key", "one-call")
        .body(r#"{"value":7}"#)
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, _) = error_code(wrong).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::BAD_GATEWAY, "upstream_error")
    );
}
