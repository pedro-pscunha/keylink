//! Keyed-actor HTTP routing, over a real registry and a real socket.

#![allow(clippy::expect_used)]

use axum::{middleware, response::Response, Router};
use http::Request;
use iroh::{endpoint::presets, Endpoint};
use keylink::prelude::*;
use keylink::{Allowlist, Caller, Node};
use keylink_http::{
    Access, AccessError, ActorGateway, Addressing, Call, Challenge, HttpSurface, Principal,
    StreamingLimits,
};

keylink::service! {
    service Ledger;
    rpc Count(value: u64) -> u64;
    rpc Whoami() -> String;
    recv Read(chunks: u64) -> String;
}

#[derive(Clone)]
struct Book {
    name: String,
}

impl ledger::Actor for Book {
    async fn count(&mut self, _cx: Caller<'_>, value: u64) -> u64 {
        value + 1
    }

    async fn whoami(&mut self, _cx: Caller<'_>) -> String {
        self.name.clone()
    }

    /// The declared value AND the payload both name the actor, so one response
    /// proves which mount answered on both halves of a `recv` reply.
    async fn read(&mut self, _cx: Caller<'_>, chunks: u64) -> (String, keylink::Body) {
        let bytes = self
            .name
            .as_bytes()
            .repeat(usize::try_from(chunks).expect("test-sized body"));
        let declared = Some(bytes.len() as u64);
        (
            self.name.clone(),
            keylink::Body::from_reader(std::io::Cursor::new(bytes), declared),
        )
    }
}

/// Records the actor every authorized call named, so a test can assert that
/// the policy is asked about the id BEFORE the registry is.
#[derive(Clone)]
struct Watching {
    seen: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
}

impl Access for Watching {
    async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
        self.seen
            .lock()
            .expect("the record is not poisoned")
            .push(call.actor.map(|id| id.as_str().to_owned()));
        if call
            .actor
            .is_some_and(|id| id.as_str().starts_with("forbidden"))
        {
            return Err(AccessError::Forbidden);
        }
        Ok(())
    }
}

async fn authenticate(mut request: Request<axum::body::Body>, next: middleware::Next) -> Response {
    request
        .extensions_mut()
        .insert(Principal::new(b"tenant-a").expect("valid principal"));
    next.run(request).await
}

async fn local_node() -> Node {
    Node::from_endpoint(
        Endpoint::bind(presets::Minimal)
            .await
            .expect("local endpoint binds"),
    )
}

struct Running {
    address: std::net::SocketAddr,
    seen: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    _server: keylink::Server,
    _nodes: Vec<Node>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start() -> Running {
    let server_node = local_node().await;
    let client_node = local_node().await;

    let system: ledger::Registry = ledger::Registry::builder(keylink::Eviction::Never).build();
    for name in ["one", "obra/4471", "forbidden"] {
        system
            .mount(
                keylink::ActorId::new(name).expect("a valid id"),
                ledger::spawn(Book {
                    name: name.to_owned(),
                }),
            )
            .expect("mounts");
    }
    let server = keylink::Server::builder(&server_node)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("a keyed system");
    let client = ledger::RegistryClient::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("dials the keyed system");

    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let surface = HttpSurface::new("/v1", Addressing::Actors)
        .expect("valid actor surface")
        .with_streaming(StreamingLimits::new(32, 64, 64, 1).expect("positive limits"));
    let gateway = ActorGateway::<ledger::External, _>::new(
        client,
        Watching {
            seen: std::sync::Arc::clone(&seen),
        },
        Challenge::new(http::HeaderValue::from_static("Bearer realm=keylink"))
            .expect("valid challenge"),
    )
    .router::<()>(surface)
    .expect("actor surface builds");
    let app: Router<()> = Router::new()
        .merge(gateway)
        .layer(middleware::from_fn(authenticate));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("listener address");
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Running {
        address,
        seen,
        task,
        _server: server,
        _nodes: vec![server_node, client_node],
    }
}

impl Running {
    async fn post(&self, path: &str, body: &'static str) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("http://{}{path}", self.address))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .expect("the request completes")
    }

    fn seen(&self) -> Vec<Option<String>> {
        self.seen
            .lock()
            .expect("the record is not poisoned")
            .clone()
    }
}

/// A keyed call reaches the actor its path names, and no other.
///
/// `Whoami` returns the id the actor was mounted under, so the answer IS the
/// address that was resolved. A gateway that dropped the actor and called the
/// registry's first mount would answer with a name, and the wrong one.
#[tokio::test]
async fn a_keyed_call_reaches_the_actor_its_path_names() {
    let running = start().await;

    let response = running.post("/v1/one/rpc/Count", r#"{"value":4}"#).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.text().await.expect("a body"), "5");

    let response = running.post("/v1/one/rpc/Whoami", "{}").await;
    assert_eq!(response.text().await.expect("a body"), r#""one""#);

    // The one that a decoder working on Axum's already-decoded path cannot get
    // right: `%2F` is part of the id, not a separator.
    let response = running.post("/v1/obra%2F4471/rpc/Whoami", "{}").await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.text().await.expect("a body"),
        r#""obra/4471""#,
        "an encoded slash addresses one actor whose id contains a slash"
    );
}

/// `Access` is asked about the actor, and is asked before the registry is.
#[tokio::test]
async fn access_sees_the_validated_actor_before_the_call() {
    let running = start().await;

    let response = running.post("/v1/one/rpc/Count", r#"{"value":1}"#).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        running.seen(),
        [Some("one".to_owned())],
        "the policy was handed the validated id, not a raw segment"
    );

    // A refused actor that EXISTS and one that does not answer identically, so
    // a denial says nothing about which ids are mounted.
    let mounted = running
        .post("/v1/forbidden/rpc/Count", r#"{"value":1}"#)
        .await;
    let status = mounted.status();
    let body = mounted.text().await.expect("a body");
    let absent = running
        .post("/v1/forbidden-absent/rpc/Count", r#"{"value":1}"#)
        .await;
    assert_eq!(status, http::StatusCode::FORBIDDEN);
    assert_eq!(absent.status(), status);
    assert_eq!(absent.text().await.expect("a body"), body);

    // And a PERMITTED actor that is not mounted reaches the registry, which is
    // the only thing that knows: `404`, from upstream rather than from policy.
    let response = running.post("/v1/absent/rpc/Count", r#"{"value":1}"#).await;
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);

    assert_eq!(
        running.seen(),
        [
            Some("one".to_owned()),
            Some("forbidden".to_owned()),
            Some("forbidden-absent".to_owned()),
            Some("absent".to_owned()),
        ],
        "every call reached the policy first, including the two for actors \
         that were never mounted"
    );
}

/// A malformed id is the same refusal a missing method is.
#[tokio::test]
async fn a_malformed_actor_is_refused_without_reaching_access() {
    let running = start().await;

    for path in [
        "/v1//rpc/Count",
        "/v1/a%2/rpc/Count",
        "/v1/a%zz/rpc/Count",
        &format!("/v1/{}/rpc/Count", "x".repeat(keylink::MAX_KEY_LEN + 1)),
    ] {
        let response = running.post(path, r#"{"value":1}"#).await;
        assert_eq!(
            response.status(),
            http::StatusCode::NOT_FOUND,
            "{path}: telling a bad id apart from a missing method says which \
             ids are well formed"
        );
    }
    assert!(
        running.seen().is_empty(),
        "and the policy was never asked about any of them"
    );
}

/// A keyed `recv` streams from the actor its path names, on both halves.
///
/// `Keylink-Result` carries the declared value and the body carries the bytes,
/// and this actor derives both from its own mount id. A route that reached the
/// registry without the actor would answer with another actor's name in both.
///
/// Watched red with `scripts/break-once.sh` (row `HK33`) by replacing
/// `RouteKind::Recv => any(move |request: Request<Body>| {`
/// with `RouteKind::Send => any(move |request: Request<Body>| {`, which drops
/// the arm to the table's `_ => continue` and installs no keyed `recv` route
/// at all. The row answered `404` where it asserts `200`.
///
/// The break is on the ROUTE TABLE rather than on the actor threading, because
/// both address spaces now reach their client through one `Mount::surface` and
/// the rpc rows above already watch that. What is unique here is that the
/// keyed surface installs the streaming route at all.
#[tokio::test]
async fn a_keyed_recv_streams_from_the_actor_its_path_names() {
    let running = start().await;

    let response = running.post("/v1/one/recv/Read", r#"{"chunks":3}"#).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        result_of(&response),
        r#""one""#,
        "the declared value came from the actor the path named"
    );
    assert_eq!(response.text().await.expect("a body"), "oneoneone");

    // The same encoded-slash id the rpc route resolves, on the streaming route.
    let response = running
        .post("/v1/obra%2F4471/recv/Read", r#"{"chunks":2}"#)
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.text().await.expect("a body"), "obra/4471obra/4471");

    // And a malformed id is refused on this route too, without the policy ever
    // being asked about it.
    let refused = running.post("/v1//recv/Read", r#"{"chunks":1}"#).await;
    assert_eq!(refused.status(), http::StatusCode::NOT_FOUND);
    assert_eq!(
        running.seen(),
        [Some("one".to_owned()), Some("obra/4471".to_owned())],
        "the policy saw the two valid ids and nothing else"
    );
}

/// The declared return value, decoded back out of `Keylink-Result`.
fn result_of(response: &reqwest::Response) -> String {
    use base64::Engine as _;

    let encoded = response
        .headers()
        .get("keylink-result")
        .expect("a recv reply carries its declared value")
        .to_str()
        .expect("base64url is header-safe");
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .expect("canonical unpadded base64url");
    String::from_utf8(decoded).expect("the declared value is JSON text")
}
