//! Keyed-actor HTTP routing, over a real registry and a real socket.

#![allow(clippy::expect_used)]

use axum::{middleware, response::Response, Router};
use http::Request;
use iroh::{endpoint::presets, Endpoint};
use keylink::prelude::*;
use keylink::{Allowlist, BlobId, Blobs, Caller, Node};
use keylink_http::{
    Access, AccessError, ActorGateway, Addressing, Call, Challenge, HttpSurface, Principal,
    StreamingLimits,
};

keylink::service! {
    service Ledger;
    rpc Count(value: u64) -> u64;
    rpc Whoami() -> String;
    rpc Tick() -> u64;
    recv Read(chunks: u64) -> String;
    // Every actor owns a DIFFERENT blob, so a ticket that reached the wrong
    // actor's bytes answers with content the test can name.
    blob Take() -> u64;
}

#[derive(Clone)]
struct Book {
    name: String,
    /// This actor's own blob, and how many bytes are in it.
    blob: (BlobId, u64),
    /// How many times this actor really ran a `Tick`, so a replayed answer is
    /// distinguishable from a second execution.
    ticks: std::sync::Arc<std::sync::atomic::AtomicU64>,
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
    async fn tick(&mut self, _cx: Caller<'_>) -> u64 {
        self.ticks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
    }

    async fn take(&mut self, _cx: Caller<'_>) -> (u64, BlobId) {
        (self.blob.1, self.blob.0)
    }

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
    /// Ids refused from now on. A test revokes an actor mid-run, which is the
    /// only way to ask what a grant recorded before the revocation is worth.
    denied: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl Access for Watching {
    async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
        self.seen
            .lock()
            .expect("the record is not poisoned")
            .push(call.actor.map(|id| id.as_str().to_owned()));
        let denied = call.actor.is_some_and(|id| {
            id.as_str().starts_with("forbidden")
                || self
                    .denied
                    .lock()
                    .expect("the record is not poisoned")
                    .contains(id.as_str())
        });
        if denied {
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
    denied: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    ticks: std::collections::HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>>,
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
    let blobs = Blobs::memory();

    let system: ledger::Registry = ledger::Registry::builder(keylink::Eviction::Never).build();
    let mut ticks = std::collections::HashMap::new();
    for name in ["one", "obra/4471", "forbidden"] {
        // Content that names its own actor, so bytes reached through the wrong
        // ticket say whose they are.
        let bytes = format!("bytes of {name}").into_bytes();
        let id = blobs.add_bytes(bytes.clone()).await.expect("stores");
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        ticks.insert(name.to_owned(), std::sync::Arc::clone(&counter));
        system
            .mount(
                keylink::ActorId::new(name).expect("a valid id"),
                ledger::spawn(Book {
                    name: name.to_owned(),
                    blob: (id, bytes.len() as u64),
                    ticks: counter,
                }),
            )
            .expect("mounts");
    }
    let server = keylink::Server::builder(&server_node)
        .service(
            system
                .service(Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("a keyed system");
    let client = ledger::RegistryClient::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("dials the keyed system");

    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let denied = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let surface = HttpSurface::new("/v1", Addressing::Actors)
        .expect("valid actor surface")
        .with_streaming(StreamingLimits::new(32, 64, 64, 1).expect("positive limits"));
    let gateway = ActorGateway::<ledger::External, _>::new(
        client,
        Watching {
            seen: std::sync::Arc::clone(&seen),
            denied: std::sync::Arc::clone(&denied),
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
        denied,
        ticks,
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

    /// How many times one actor really ran `Tick`.
    fn ticks(&self, actor: &str) -> u64 {
        self.ticks[actor].load(std::sync::atomic::Ordering::SeqCst)
    }

    /// A `POST` carrying a replay key.
    async fn replayable(&self, path: &str, key: &str) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("http://{}{path}", self.address))
            .header(http::header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", key)
            .body("{}")
            .send()
            .await
            .expect("the request completes")
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        reqwest::Client::new()
            .get(format!("http://{}{path}", self.address))
            .send()
            .await
            .expect("the request completes")
    }

    /// Refuse one actor from the next request onward.
    fn deny(&self, actor: &str) {
        self.denied
            .lock()
            .expect("the record is not poisoned")
            .insert(actor.to_owned());
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

/// One replay key under two actors is two identities.
///
/// The plan's derivation binds the actor into the Keylink key, so `one` and
/// `obra/4471` under `shared-key` are two calls rather than one caller
/// replaying the other's answer. Each actor counts its own executions, so a
/// leaked identity shows up as a run that never happened.
///
/// Watched red with `scripts/break-once.sh` (row `HK37`) by replacing the
/// actor field of the derivation
/// `.map_or(b"", |id| id.as_str().as_bytes()),`
/// with `.map_or(b"", |_id| b""),`. The run terminated on
/// `the second actor ran its own call rather than replaying the first's`,
/// `left: 0` against `right: 1` — the second actor never executed, because
/// the reply was `one`'s recorded answer.
///
/// The addressing-mode tag beside that field cannot catch this: both calls
/// here are keyed, so both tag as keyed. Only the id tells them apart.
#[tokio::test]
async fn one_replay_key_under_two_actors_is_two_identities() {
    let running = start().await;

    let first = running.replayable("/v1/one/rpc/Tick", "shared-key").await;
    assert_eq!(first.status(), http::StatusCode::OK);
    assert_eq!(first.text().await.expect("a body"), "1");
    assert_eq!(running.ticks("one"), 1);

    let other = running
        .replayable("/v1/obra%2F4471/rpc/Tick", "shared-key")
        .await;
    assert_eq!(other.status(), http::StatusCode::OK);
    assert_eq!(other.text().await.expect("a body"), "1");
    assert_eq!(
        running.ticks("obra/4471"),
        1,
        "the second actor ran its own call rather than replaying the first's"
    );

    // And within one actor the key still replays, so the binding narrowed the
    // identity without removing it.
    let again = running.replayable("/v1/one/rpc/Tick", "shared-key").await;
    assert_eq!(again.text().await.expect("a body"), "1");
    assert_eq!(running.ticks("one"), 1, "the replay ran nothing");
}

/// A blob ticket minted for one actor never reaches another actor's bytes.
///
/// The keyed surface serves the same four route families the singleton one
/// does, so a lookup under `/v1/one/blob/Take` mints a grant and the byte
/// route under the SAME actor serves it. The address is content-derived, so
/// nothing but the ticket's actor field stops the identical `GET` under
/// another actor's path from reading it.
///
/// Watched red with `scripts/break-once.sh` (row `HK38`) by replacing
/// `        actor: surface.actor.clone(),`
/// with `        actor: None,`. The run terminated on
/// `a grant minted for one actor is not a grant under another`, `left: 200`
/// against `right: 404` — the cross-actor `GET` succeeded.
#[tokio::test]
async fn a_blob_ticket_is_bound_to_the_actor_that_minted_it() {
    let running = start().await;

    let minted = running.post("/v1/one/blob/Take", "{}").await;
    assert_eq!(minted.status(), http::StatusCode::CREATED);
    let location = minted
        .headers()
        .get(http::header::LOCATION)
        .expect("a lookup says where the bytes live")
        .to_str()
        .expect("a header-safe path")
        .to_owned();
    assert_eq!(minted.text().await.expect("a body"), "12");

    // Control: the actor that minted the grant can read it.
    let mine = running.get(&location).await;
    assert_eq!(mine.status(), http::StatusCode::OK);
    assert_eq!(mine.text().await.expect("a body"), "bytes of one");

    // The same address, the same principal, the same method — another actor.
    let theirs = location.replacen("/one/", "/obra%2F4471/", 1);
    assert_ne!(theirs, location, "the probe really changed the actor");
    let crossed = running.get(&theirs).await;
    assert_eq!(
        crossed.status(),
        http::StatusCode::NOT_FOUND,
        "a grant minted for one actor is not a grant under another"
    );
}

/// A wrong verb is answered before the actor segment is even read.
///
/// Both refusals are cheap and both are `4xx`, so the order looks like it
/// cannot matter. It does: a caller whose verb is wrong and whose actor id is
/// also malformed would be told only about the id, and would keep retrying a
/// request whose method was never going to be accepted. The verb is a fact
/// about the route and is knowable with no address at all, so it is decided
/// first.
///
/// Watched red with `scripts/break-once.sh` (row `HK39`) by replacing
/// `    if request.method() != route.method() {`
/// with `    if false {`. The row failed on the first case: `404` where it
/// asserts `405`, the malformed id having answered instead.
#[tokio::test]
async fn a_wrong_verb_is_refused_before_the_actor_is_decoded() {
    let running = start().await;

    // Malformed id AND wrong verb. The verb answers.
    let response = running.get("/v1/a%2/rpc/Count").await;
    assert_eq!(response.status(), http::StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()[http::header::ALLOW], "POST");

    // Control: the same verb on a well formed id answers the same way, so the
    // `405` above is about the verb and not about the id.
    let response = running.get("/v1/one/rpc/Count").await;
    assert_eq!(response.status(), http::StatusCode::METHOD_NOT_ALLOWED);

    // And with the right verb, the malformed id is what answers.
    let response = running.post("/v1/a%2/rpc/Count", r#"{"value":1}"#).await;
    assert_eq!(response.status(), http::StatusCode::NOT_FOUND);

    assert!(
        running.seen().is_empty(),
        "no policy was asked about any of them"
    );
}

/// Revoking an actor makes its recorded answer unreachable, from any path.
///
/// The plan's shape: record under `A`, deny `A`, permit `B`, reuse the SAME
/// external key. `A`'s answer must not come back — not to `A`, whose access is
/// gone, and not to `B`, who never made it.
///
/// The two halves fail differently and both matter. `A` is stopped by the
/// policy, which runs before any call is made. `B` is stopped by the key
/// derivation, which gives `B` an identity `A`'s record cannot satisfy — and
/// that half would still hold if the policy were removed entirely.
///
/// Watched red with `scripts/break-once.sh` (row `HK40`) by replacing the
/// actor field of the derivation
/// `.map_or(b"", |id| id.as_str().as_bytes()),`
/// with `.map_or(b"", |_id| b""),`. The row failed on `B`'s execution count:
/// `left: 0` against `right: 1` — `B` replayed the revoked actor's answer.
#[tokio::test]
async fn a_revoked_actors_recorded_answer_replays_to_nobody() {
    let running = start().await;

    let recorded = running.replayable("/v1/one/rpc/Tick", "revocable").await;
    assert_eq!(recorded.status(), http::StatusCode::OK);
    assert_eq!(recorded.text().await.expect("a body"), "1");
    assert_eq!(running.ticks("one"), 1);

    running.deny("one");

    let refused = running.replayable("/v1/one/rpc/Tick", "revocable").await;
    assert_eq!(
        refused.status(),
        http::StatusCode::FORBIDDEN,
        "the recorded answer is not reachable by the actor that recorded it"
    );

    let other = running
        .replayable("/v1/obra%2F4471/rpc/Tick", "revocable")
        .await;
    assert_eq!(other.status(), http::StatusCode::OK);
    assert_eq!(other.text().await.expect("a body"), "1");
    assert_eq!(
        running.ticks("obra/4471"),
        1,
        "the permitted actor ran its own call rather than replaying the revoked one's"
    );
    assert_eq!(
        running.ticks("one"),
        1,
        "and the revoked actor never ran again"
    );
}

/// The keyed surface serves calls, and nothing else.
///
/// No `send`, no status, no lifecycle, no discovery, no administration. This
/// is not an omission to be filled in later: a registry is a private detail of
/// the serving node, and an HTTP surface that could list, spawn or stop actors
/// would make it a public one. `send` is absent for a different reason — a
/// service that declares one cannot be served as an actor at all, which the
/// generated code enforces before any of this is reachable.
///
/// Every path below answers the same `404` an unknown method answers, so the
/// surface does not say which of them are ideas it has heard of.
#[tokio::test]
async fn the_keyed_surface_exposes_no_lifecycle_or_discovery() {
    let running = start().await;

    for path in [
        "/v1/one/send/Upload",
        "/v1/one/tell/Count",
        "/v1/one/status",
        "/v1/one/stop",
        "/v1/one/spawn",
        "/v1/actors",
        "/v1/one",
    ] {
        let response = running.post(path, "{}").await;
        assert_eq!(
            response.status(),
            http::StatusCode::NOT_FOUND,
            "{path}: a keyed surface answers calls and nothing else"
        );
    }
    assert!(
        running.seen().is_empty(),
        "and no policy was asked about any of them"
    );
}
