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

use keylink::{Allowlist, Context, Keyable, Node, Server};
use keylink_http::{
    Access, AccessError, Addressing, Call, Challenge, Gateway, HttpSurface, JsonLimits, Principal,
};

keylink::service! {
    service Inventory;

    rpc Ping() -> u64;
    rpc Add(left: u64, right: u64) -> u64;
    rpc Count(value: u64) -> u64;
    rpc Echo(value: String) -> String;
    #[serde(rename = "wire-name")]
    rpc Renamed(value: u64) -> u64;
    tell Ignored(value: u64) -> ();
}

#[derive(Clone, Default)]
struct Handler {
    runs: Arc<AtomicU64>,
}

impl inventory::Handler for Handler {
    async fn ping(&self, _cx: &Context) -> u64 {
        self.runs.fetch_add(1, Ordering::SeqCst);
        1
    }

    async fn add(&self, _cx: &Context, left: u64, right: u64) -> u64 {
        self.runs.fetch_add(1, Ordering::SeqCst);
        left + right
    }

    async fn count(&self, _cx: &Context, _value: u64) -> u64 {
        self.runs.fetch_add(1, Ordering::SeqCst) + 1
    }

    async fn echo(&self, _cx: &Context, value: String) -> String {
        self.runs.fetch_add(1, Ordering::SeqCst);
        value
    }

    async fn renamed(&self, _cx: &Context, value: u64) -> u64 {
        value
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
    start_with_keyable(handler, access, limits, Keyable::Any).await
}

async fn start_with_keyable<A: Access>(
    handler: Handler,
    access: A,
    limits: Option<JsonLimits>,
    keyable: Keyable,
) -> Running {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let keylink = Server::builder(&server_node)
        .service(
            inventory::service(handler, Allowlist::anyone())
                .keyable(keyable)
                .build(),
        )
        .spawn()
        .expect("service starts");
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
    .router::<AppState>(
        HttpSurface::new("/v1/inventory", Addressing::Singleton).expect("valid singleton surface"),
    )
    .expect("surface matches singleton gateway");
    let app: Router<AppState> = Router::new().merge(gateway);
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
    assert_eq!(headers[http::header::CONTENT_TYPE], "application/json");
    let body: serde_json::Value = response.json().await.expect("JSON error");
    assert_eq!(body.as_object().map(serde_json::Map::len), Some(1));
    assert_eq!(body["error"].as_object().map(serde_json::Map::len), Some(2));
    assert!(body["error"]["message"].is_string());
    let code = body["error"]["code"]
        .as_str()
        .expect("string error code")
        .to_owned();
    (status, code, headers)
}

use http::{HeaderMap, HeaderValue};

#[tokio::test]
async fn unknown_and_non_rpc_methods_bypass_access() {
    let access = CountingAccess::default();
    let running = start(Handler::default(), access.clone(), None).await;
    let client = reqwest::Client::new();

    for (method, expected) in [
        ("Missing", (http::StatusCode::NOT_FOUND, "method_not_found")),
        ("Ignored", (http::StatusCode::NOT_FOUND, "method_not_found")),
        // Longer than any served method name, so it names no method. It used
        // to answer `400`, and the flip point between the two statuses was the
        // exact length of the longest name — readable byte by byte, before
        // authentication. Now indistinguishable from `Missing`.
        (
            "__Status",
            (http::StatusCode::NOT_FOUND, "method_not_found"),
        ),
    ] {
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
        assert_eq!((status, code.as_str()), expected, "{method}");
    }
    assert_eq!(access.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn missing_principal_uses_the_cached_challenge() {
    let running = start(Handler::default(), Allow, None).await;
    let response = reqwest::Client::new()
        .post(format!("http://{}/v1/inventory/rpc/Add", running.address))
        .header("content-type", "application/json")
        .body(r#"{"left":1,"right":2}"#)
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
async fn every_non_post_verb_is_rejected_with_post_in_allow() {
    let running = start(Handler::default(), Allow, None).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/inventory/rpc/Add", running.address);

    for verb in [
        reqwest::Method::GET,
        reqwest::Method::PUT,
        reqwest::Method::PATCH,
        reqwest::Method::DELETE,
        reqwest::Method::OPTIONS,
        reqwest::Method::HEAD,
    ] {
        let response = client
            .request(verb.clone(), &url)
            .header("x-test-principal", "tenant-a")
            .send()
            .await
            .expect("HTTP call completes");
        assert_eq!(
            response.status(),
            http::StatusCode::METHOD_NOT_ALLOWED,
            "{verb}"
        );
        assert_eq!(
            response.headers()[http::header::CONTENT_TYPE],
            "application/json",
            "{verb}"
        );
        assert_eq!(response.headers()[http::header::ALLOW], "POST", "{verb}");
        if verb != reqwest::Method::HEAD {
            let (_, code, _) = error_code(response).await;
            assert_eq!(code, "method_not_allowed", "{verb}");
        }
    }
}

#[tokio::test]
async fn json_content_type_compatibility_matrix_uses_real_listener() {
    let running = start(Handler::default(), Allow, None).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/inventory/rpc/Add", running.address);

    for content_type in [
        "application/json",
        "application/json; charset=utf-8",
        "Application/JSON; Charset=UTF-8",
        "application/problem+json",
        "application/vnd.keylink+json; version=1",
    ] {
        let response = client
            .post(&url)
            .header("x-test-principal", "tenant-a")
            .header("content-type", content_type)
            .body(r#"{"left":1,"right":2}"#)
            .send()
            .await
            .expect("HTTP call completes");
        assert_eq!(response.status(), http::StatusCode::OK, "{content_type}");
        assert_eq!(response.text().await.expect("body"), "3", "{content_type}");
    }

    for content_type in [
        None,
        Some("text/json"),
        Some("application/jsonp"),
        Some("application/problem+xml"),
    ] {
        let mut request = client
            .post(&url)
            .header("x-test-principal", "tenant-a")
            .body(r#"{"left":1,"right":2}"#);
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        let response = request.send().await.expect("HTTP call completes");
        let (status, code, _) = error_code(response).await;
        assert_eq!(
            (status, code.as_str()),
            (
                http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type"
            ),
            "{content_type:?}"
        );
    }
}

#[tokio::test]
async fn path_matrix_and_external_identity_are_exact() {
    let running = start(Handler::default(), Allow, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://{}/v1/inventory", running.address);

    for (case, path, expected) in [
        (
            "missing method segment",
            "/rpc",
            (http::StatusCode::NOT_FOUND, "method_not_found"),
        ),
        (
            "empty method segment",
            "/rpc/",
            (http::StatusCode::NOT_FOUND, "method_not_found"),
        ),
        (
            "trailing slash",
            "/rpc/Add/",
            (http::StatusCode::NOT_FOUND, "method_not_found"),
        ),
        (
            "extra segment",
            "/rpc/Add/extra",
            (http::StatusCode::NOT_FOUND, "method_not_found"),
        ),
        (
            "unknown method",
            "/rpc/Missing",
            (http::StatusCode::NOT_FOUND, "method_not_found"),
        ),
        // Both are over the length cap, and a segment too long to be any
        // method names no method. Answering `400` here made the cap itself
        // readable: the status flipped at the exact length of the longest
        // served name, and it flipped before the principal was looked at.
        (
            "over-cap wire rename",
            "/rpc/wire-name",
            (http::StatusCode::NOT_FOUND, "method_not_found"),
        ),
        (
            "over-cap cfg-disabled method",
            "/rpc/Disabled",
            (http::StatusCode::NOT_FOUND, "method_not_found"),
        ),
    ] {
        let response = client
            .post(format!("{base}{path}"))
            .header("x-test-principal", "tenant-a")
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("HTTP call completes");
        let (status, code, _) = error_code(response).await;
        assert_eq!((status, code.as_str()), expected, "{case}");
    }

    let response = client
        .post(format!("{base}/rpc/Renamed"))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .body(r#"{"value":7}"#)
        .send()
        .await
        .expect("HTTP call completes");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "7");
}

async fn raw_http(address: std::net::SocketAddr, request: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut socket = tokio::net::TcpStream::connect(address)
        .await
        .expect("raw client connects");
    socket.write_all(request).await.expect("raw request writes");
    socket.shutdown().await.expect("request half closes");
    let mut response = Vec::new();
    socket
        .read_to_end(&mut response)
        .await
        .expect("raw response reads");
    String::from_utf8_lossy(&response).into_owned()
}

#[tokio::test]
async fn malformed_percent_encoded_paths_are_rejected_without_dispatch() {
    let handler = Handler::default();
    let runs = Arc::clone(&handler.runs);
    let running = start(handler, Allow, None).await;
    for path in ["/v1/inventory/rpc/%FF", "/v1/inventory/rpc/%C3%28"] {
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Test-Principal: tenant-a\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
        );
        let response = raw_http(running.address, request.as_bytes()).await;
        assert!(
            response.is_empty() || response.starts_with("HTTP/1.1 400"),
            "{path}: {response}"
        );
    }

    let parser_rejected = raw_http(
        running.address,
        b"POST /v1/inventory/rpc/%ZZ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(parser_rejected.is_empty() || parser_rejected.starts_with("HTTP/1.1 400"));
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn idempotency_header_compatibility_matrix_is_exact() {
    let running = start(Handler::default(), Allow, None).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/inventory/rpc/Add", running.address);

    for key in [
        "x",
        &"x".repeat(keylink::MAX_KEY_LEN - 1),
        &"x".repeat(keylink::MAX_KEY_LEN),
    ] {
        let response = client
            .post(&url)
            .header("x-test-principal", "tenant-a")
            .header("content-type", "application/json")
            .header("idempotency-key", key)
            .body(r#"{"left":1,"right":2}"#)
            .send()
            .await
            .expect("HTTP call completes");
        assert_eq!(
            response.status(),
            http::StatusCode::OK,
            "valid key length {}",
            key.len()
        );
    }

    for key in ["", "bad key", &"x".repeat(keylink::MAX_KEY_LEN + 1)] {
        let response = client
            .post(&url)
            .header("x-test-principal", "tenant-a")
            .header("content-type", "application/json")
            .header("idempotency-key", key)
            .body(r#"{"left":1,"right":2}"#)
            .send()
            .await
            .expect("HTTP call completes");
        let (status, code, _) = error_code(response).await;
        assert_eq!(
            (status, code.as_str()),
            (http::StatusCode::BAD_REQUEST, "invalid_idempotency_key"),
            "invalid key length {}",
            key.len()
        );
    }

    let mut headers = reqwest::header::HeaderMap::new();
    headers.append("idempotency-key", HeaderValue::from_static("one"));
    headers.append("idempotency-key", HeaderValue::from_static("two"));
    let response = client
        .post(&url)
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .headers(headers)
        .body(r#"{"left":1,"right":2}"#)
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, _) = error_code(response).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::BAD_REQUEST, "invalid_idempotency_key")
    );

    for raw_value in [b"\xff".as_slice(), b"bad\x01value".as_slice()] {
        let mut request = b"POST /v1/inventory/rpc/Add HTTP/1.1\r\nHost: localhost\r\nX-Test-Principal: tenant-a\r\nContent-Type: application/json\r\nIdempotency-Key: ".to_vec();
        request.extend_from_slice(raw_value);
        request.extend_from_slice(
            b"\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"left\":1,\"right\":2}",
        );
        let response = raw_http(running.address, &request).await;
        assert!(
            response.is_empty() || response.starts_with("HTTP/1.1 400"),
            "{response}"
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

/// Watched red with `scripts/break-once.sh` by removing the RPC arm's generated
/// `#[serde(deny_unknown_fields)]`. The exact test selected one guard, dispatched
/// `Ping`, and returned HTTP 200 instead of 400.
#[tokio::test]
async fn unknown_argument_fields_fail_before_zero_one_or_many_argument_dispatch() {
    for (method, body) in [
        ("Ping", r#"{"unexpected":1}"#),
        ("Count", r#"{"value":7,"unexpected":1}"#),
        ("Add", r#"{"left":1,"right":2,"unexpected":1}"#),
    ] {
        let handler = Handler::default();
        let runs = Arc::clone(&handler.runs);
        let running = start(handler, Allow, None).await;
        let response = reqwest::Client::new()
            .post(format!(
                "http://{}/v1/inventory/rpc/{method}",
                running.address
            ))
            .header("x-test-principal", "tenant-a")
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .expect("HTTP call completes");
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST, "{method}");
        let (_, code, _) = error_code(response).await;
        assert_eq!(code, "invalid_request", "{method}");
        assert_eq!(runs.load(Ordering::SeqCst), 0, "{method}");
    }
}

#[tokio::test]
async fn request_limits_cover_limit_minus_one_limit_and_limit_plus_one_without_overlimit_dispatch()
{
    let limit = r#"{"value":7} "#.len();
    for (case, body, expected_status, expected_runs) in [
        ("limit minus one", r#"{"value":7}"#, http::StatusCode::OK, 1),
        ("exact limit", r#"{"value":7} "#, http::StatusCode::OK, 1),
        (
            "limit plus one",
            r#"{"value":7}  "#,
            http::StatusCode::PAYLOAD_TOO_LARGE,
            0,
        ),
    ] {
        let handler = Handler::default();
        let runs = Arc::clone(&handler.runs);
        let running = start(
            handler,
            Allow,
            Some(JsonLimits::new(limit, 32).expect("positive limits")),
        )
        .await;
        let response = reqwest::Client::new()
            .post(format!("http://{}/v1/inventory/rpc/Count", running.address))
            .header("x-test-principal", "tenant-a")
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .expect("HTTP call completes");
        assert_eq!(response.status(), expected_status, "{case}");
        assert_eq!(runs.load(Ordering::SeqCst), expected_runs, "{case}");
    }
}

#[tokio::test]
async fn response_limits_cover_limit_minus_one_limit_and_limit_plus_one() {
    for (case, value, expected_status) in [
        ("limit minus one", "aa", http::StatusCode::OK),
        ("exact limit", "aaa", http::StatusCode::OK),
        (
            "limit plus one",
            "aaaa",
            http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ] {
        let handler = Handler::default();
        let runs = Arc::clone(&handler.runs);
        let running = start(
            handler,
            Allow,
            Some(JsonLimits::new(64, 5).expect("positive limits")),
        )
        .await;
        let response = reqwest::Client::new()
            .post(format!("http://{}/v1/inventory/rpc/Echo", running.address))
            .header("x-test-principal", "tenant-a")
            .header("content-type", "application/json")
            .body(format!(r#"{{"value":"{value}"}}"#))
            .send()
            .await
            .expect("HTTP call completes");
        assert_eq!(response.status(), expected_status, "{case}");
        assert_eq!(runs.load(Ordering::SeqCst), 1, "{case}");
        if expected_status == http::StatusCode::INTERNAL_SERVER_ERROR {
            let (_, code, _) = error_code(response).await;
            assert_eq!(code, "response_too_large", "{case}");
        }
    }
}

#[tokio::test]
async fn real_ledger_maps_not_keyable_refusal() {
    let not_keyable =
        start_with_keyable(Handler::default(), Allow, None, Keyable::Only(&["Count"])).await;
    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/inventory/rpc/Add",
            not_keyable.address
        ))
        .header("x-test-principal", "tenant-a")
        .header("content-type", "application/json")
        .header("idempotency-key", "not-keyable")
        .body(r#"{"left":1,"right":2}"#)
        .send()
        .await
        .expect("HTTP call completes");
    let (status, code, _) = error_code(response).await;
    assert_eq!(
        (status, code.as_str()),
        (http::StatusCode::BAD_REQUEST, "idempotency_not_supported")
    );
}

/// Watched red with `scripts/break-once.sh` by replacing
/// `delimited(&mut hasher, principal.as_bytes());`
/// with a call passing `b""`, which drops the principal from the identity. The
/// exact async test terminated with the second principal replaying `1` instead
/// of running as `2`.
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

/// One key on two methods is two identities, and one key twice on one method
/// is one.
///
/// A replay key names an operation, so the method is part of what it names.
/// The gateway hashes the method into the derived key, which is what makes
/// `Add` and `Count` under `one-call` two independent calls rather than a
/// caller replaying `Add`'s answer out of `Count`.
///
/// This row USED to assert `502 upstream_error` on the second call. That
/// status was keylink's own refusal — the core saw one key reach two methods
/// and rejected it — and asserting it made a gateway defect indistinguishable
/// from a correct gateway: a caller doing an ordinary, legal thing got an
/// error naming the upstream. The behaviour changed with the derivation, and
/// the row changed with it rather than being deleted.
///
/// Watched red with `scripts/break-once.sh` (row `HK36`) by replacing
/// `delimited(&mut hasher, method.as_bytes());`
/// with a call passing `b""`, which drops the method from the identity. The
/// row failed on the SECOND call's status: `502` where it asserts `200`.
///
/// Not the replay the first draft of this note predicted, and the difference
/// matters. keylink's core keeps its own record of what a key was used for and
/// refuses to let one reach a second method, so the leak this derivation would
/// otherwise open is caught one layer down — as a `502` on a request that is
/// perfectly legal. The gateway binding does not add the refusal. It removes
/// the need for one, by giving the two calls two identities.
#[tokio::test]
async fn one_key_names_one_method_and_replays_only_within_it() {
    let handler = Handler::default();
    let runs = Arc::clone(&handler.runs);
    let running = start(handler, Allow, None).await;
    let client = reqwest::Client::new();
    let base = format!("http://{}/v1/inventory/rpc", running.address);
    let call = |path: String, body: &'static str| {
        client
            .post(path)
            .header("x-test-principal", "tenant-a")
            .header("content-type", "application/json")
            .header("idempotency-key", "one-call")
            .body(body)
            .send()
    };

    let first = call(format!("{base}/Add"), r#"{"left":1,"right":2}"#)
        .await
        .expect("HTTP call completes");
    assert_eq!(first.status(), http::StatusCode::OK);
    assert_eq!(first.text().await.expect("a body"), "3");

    // Same key, different method. `Count` answers from the shared run counter,
    // so a key that had leaked across methods would answer `Add`'s `3` here.
    let other = call(format!("{base}/Count"), r#"{"value":7}"#)
        .await
        .expect("HTTP call completes");
    assert_eq!(other.status(), http::StatusCode::OK);
    assert_eq!(other.text().await.expect("a body"), "2");
    assert_eq!(runs.load(Ordering::SeqCst), 2, "both methods ran once");

    // Same key, same method: one execution, and the recorded answer.
    let again = call(format!("{base}/Count"), r#"{"value":7}"#)
        .await
        .expect("HTTP call completes");
    assert_eq!(again.status(), http::StatusCode::OK);
    assert_eq!(again.text().await.expect("a body"), "2");
    assert_eq!(runs.load(Ordering::SeqCst), 2, "the replay ran nothing");
}
