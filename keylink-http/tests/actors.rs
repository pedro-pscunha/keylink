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
    /// A non-zero `value` is how long to hold the actor's loop, in
    /// milliseconds. An actor that is busy is the only way to fill a mailbox,
    /// and one message is what an actor is processing while the next queue.
    async fn count(&mut self, _cx: Caller<'_>, value: u64) -> u64 {
        if value > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(value)).await;
        }
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

/// Records the actor every authorized call named.
///
/// **This log alone cannot show ORDER.** No registry event enters it, so it
/// reads identically whether the policy runs before the call or after it. The
/// ordering guarantee is held by `a_denied_caller_never_reaches_the_lazy_factory`,
/// which records what the REGISTRY was asked to build — the moment a lazy mount
/// would happen — and by the `forbidden` against `forbidden-absent` body parity
/// below. This comment used to claim the ordering, and a reader who trusted it
/// would have believed the guarantee was pinned here.
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
    system: ledger::Registry,
    seen: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
    asked: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
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

    // Every id the factory is ever asked for, whether or not it mounts one.
    // A lazy mount is the moment a registry LEARNS an id exists, so this is
    // where "a denied caller cannot reach the registry" is measurable.
    let asked: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let factory_asked = std::sync::Arc::clone(&asked);
    let factory_blob = blobs
        .add_bytes(b"bytes of lazy".to_vec())
        .await
        .expect("stores");
    let lazy_ticks = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let factory_ticks = std::sync::Arc::clone(&lazy_ticks);
    let system: ledger::Registry = ledger::Registry::builder(keylink::Eviction::Never)
        // One in flight, one queued. Any larger depth needs more concurrent
        // requests than a test can reliably keep in flight at once, and a
        // saturation row that cannot saturate proves nothing.
        .mailbox_depth(std::num::NonZeroUsize::new(1).expect("positive"))
        // Three eager mounts plus room for exactly ONE lazy build, so a second
        // lazy id meets the cap. A capacity a test cannot reach is a capacity
        // nothing measures.
        .capacity(4)
        .spawn_with(move |id| {
            factory_asked
                .lock()
                .expect("the record is not poisoned")
                .push(id.as_str().to_owned());
            id.as_str().starts_with("lazy").then(|| {
                ledger::spawn(Book {
                    name: id.as_str().to_owned(),
                    blob: (factory_blob, 13),
                    ticks: std::sync::Arc::clone(&factory_ticks),
                })
            })
        })
        .build();
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
                .clone()
                .service(Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("a keyed system");
    let client = ledger::RegistryClient::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("dials the keyed system");

    ticks.insert("lazy".to_owned(), lazy_ticks);
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
        system,
        seen,
        asked,
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

    /// Every id the registry's lazy factory was asked to build.
    fn asked(&self) -> Vec<String> {
        self.asked
            .lock()
            .expect("the record is not poisoned")
            .clone()
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
///
/// Watched red with `scripts/break-once.sh` (row `HK42`) by replacing
/// `let actor = decode_actor(path, route_path)?;`
/// with a constant `"one"`, which is the shape of a gateway that decodes the
/// path and then ignores what it decoded. The `%2F` case answered `"one"`
/// where this asserts `"obra/4471"`.
///
/// This is the flagship row of the keyed surface and it carried no measured
/// red until a review asked for one. Its claim was asserted in prose.
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
///
/// Watched red with `scripts/break-once.sh` (row `HK48`) by replacing
/// `                actor: self.actor.as_ref(),`
/// with `                actor: None,`, which is a policy handed a keyed call
/// it cannot tell from a singleton one. Terminated on `the policy was handed
/// the validated id, not a raw segment`, `left: [None]` against
/// `right: [Some("one")]`.
///
/// The BEFORE half of the name is held by the `forbidden` against
/// `forbidden-absent` parity below, and by
/// `a_denied_caller_never_reaches_the_lazy_factory`. This row's own recorder
/// cannot see order — see the note on `Watching`.
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

/// An unknown method name says nothing about how long a real one is.
///
/// The method segment is bounded before it is decoded, and the bound is the
/// longest served name of that kind. Refusing an over-long segment with a
/// DIFFERENT status from an unknown short one made that length readable: walk
/// the length up one byte at a time and the status flips at the answer. It
/// runs before the principal is looked at, so no credential is needed to walk
/// it.
///
/// Both answers are now the same `404`, byte for byte.
///
/// Watched red with `scripts/break-once.sh` (row `HK41`) by replacing
/// `            _ => GatewayError::MethodNotFound,`
/// with `            _ => GatewayError::InvalidRequest,`. The over-long name
/// answered `400` where this asserts `404`.
#[tokio::test]
async fn an_over_long_method_name_is_refused_like_an_unknown_one() {
    let running = start().await;

    let unknown = running.post("/v1/one/rpc/Nope", "{}").await;
    let status = unknown.status();
    let body = unknown.text().await.expect("a body");
    assert_eq!(status, http::StatusCode::NOT_FOUND);

    // Longer than `Whoami`, the longest `rpc` name this service serves.
    for len in [7, 8, 64, 512] {
        let name = "N".repeat(len);
        let over = running.post(&format!("/v1/one/rpc/{name}"), "{}").await;
        assert_eq!(over.status(), status, "a {len}-byte name");
        assert_eq!(
            over.text().await.expect("a body"),
            body,
            "a {len}-byte name answers what an unknown short one answers"
        );
    }

    assert!(
        running.seen().is_empty(),
        "and none of them reached the policy"
    );
}

/// Revoking an actor makes its BYTES unreachable, from its own path and from
/// any other actor's.
///
/// The plan asks the revocation test to be repeated with a blob result. A blob
/// grant is longer-lived than an rpc answer — it is a row in a table that
/// outlives the call that minted it — so "the policy now refuses this actor" has
/// to reach content that was already addressable when the policy still allowed
/// it.
///
/// Three probes, three different mechanisms:
///
/// 1. The minting actor's own path: refused by the POLICY, which runs before
///    the ticket table is read at all.
/// 2. Another actor's path over the same address: refused by the TICKET, whose
///    actor field cannot match. This holds with the policy removed.
/// 3. A re-mint from the other actor: succeeds, and its `Location` addresses
///    bytes that are that actor's own. The revoked actor's content is not
///    reachable through it, which is what "no B-context ticket reaches A bytes"
///    means when both actors are content-addressed.
///
/// Watched red with `scripts/break-once.sh` (row `HK43`) by replacing
/// `        actor: surface.actor.clone(),`
/// with `        actor: None,`. Terminated on `a permitted actor does not
/// inherit a revoked actor's grant`, `left: 200` against `right: 404`.
///
/// That is PROBE 2, and probe 2 is the half `HK38` already covers through
/// `a_blob_ticket_is_bound_to_the_actor_that_minted_it` — the two rows quote
/// the same line. What is unique here is probe 1: a revocation reaching a
/// grant that was minted while the policy still allowed it. Its mechanism is
/// the prelude's `Forbidden` arm, which `HK44` breaks. Measured under that
/// break: probe 1 terminated on `a revoked actor cannot read the grant it
/// minted`, `left: 200` against `right: 403` — the revoked caller reached the
/// table and found its own still-valid ticket. Recorded here rather than
/// registered a third time on the same literal.
#[tokio::test]
async fn a_revoked_actors_bytes_are_reachable_from_no_path() {
    let running = start().await;

    let minted = running.post("/v1/one/blob/Take", "{}").await;
    assert_eq!(minted.status(), http::StatusCode::CREATED);
    let mine = minted
        .headers()
        .get(http::header::LOCATION)
        .expect("a lookup says where the bytes live")
        .to_str()
        .expect("a header-safe path")
        .to_owned();

    // Control, while `one` is still permitted.
    let before = running.get(&mine).await;
    assert_eq!(before.status(), http::StatusCode::OK);
    assert_eq!(before.text().await.expect("a body"), "bytes of one");

    running.deny("one");

    // 1. The policy refuses, and refuses before the table is read.
    let own = running.get(&mine).await;
    assert_eq!(
        own.status(),
        http::StatusCode::FORBIDDEN,
        "a revoked actor cannot read the grant it minted"
    );

    // 2. The same address under a permitted actor. The ticket refuses.
    let crossed = mine.replacen("/one/", "/obra%2F4471/", 1);
    assert_ne!(crossed, mine, "the probe really changed the actor");
    let theirs = running.get(&crossed).await;
    assert_eq!(
        theirs.status(),
        http::StatusCode::NOT_FOUND,
        "a permitted actor does not inherit a revoked actor's grant"
    );

    // 3. A grant the permitted actor mints itself addresses its OWN bytes.
    let fresh = running.post("/v1/obra%2F4471/blob/Take", "{}").await;
    assert_eq!(fresh.status(), http::StatusCode::CREATED);
    let ours = fresh
        .headers()
        .get(http::header::LOCATION)
        .expect("a lookup says where the bytes live")
        .to_str()
        .expect("a header-safe path")
        .to_owned();
    let served = running.get(&ours).await;
    assert_eq!(served.status(), http::StatusCode::OK);
    assert_eq!(
        served.text().await.expect("a body"),
        "bytes of obra/4471",
        "the fresh grant serves its own actor's content, not the revoked one's"
    );
}

/// A denied caller cannot make the registry mount an actor it has never held.
///
/// A lazy registry builds an actor the first time an id is asked for, so an
/// unauthorized request that reached the registry would leave state behind:
/// a mailbox, a task, a capacity slot. The policy runs before the call is
/// made, so the factory is never asked.
///
/// The recorder is the FACTORY, not the policy. A test that only watched the
/// policy could not tell an ordering violation from a correct run, because both
/// record the same policy entry — this one records the moment the registry
/// learns an id exists, which is the thing that must not happen.
///
/// Watched red with `scripts/break-once.sh` (row `HK44`) by replacing
/// `            Err(AccessError::Forbidden) => Err(GatewayError::Forbidden.response(None)),`
/// with an `Ok(Admitted { method, principal })`, which lets a denied caller
/// through to the call. Terminated on
/// `a denied caller left no id in the factory's record`, `left` carrying
/// `["lazy", "forbidden-lazy"]` against a `right` of `["lazy"]`.
#[tokio::test]
async fn a_denied_caller_never_reaches_the_lazy_factory() {
    let running = start().await;

    // Control: a PERMITTED id the factory will build. This is what reaching
    // the registry looks like, so the assertion below is not satisfied by a
    // gateway that has stopped calling anything.
    let built = running.post("/v1/lazy/rpc/Whoami", "{}").await;
    assert_eq!(built.status(), http::StatusCode::OK);
    assert_eq!(built.text().await.expect("a body"), r#""lazy""#);
    assert_eq!(
        running.asked(),
        ["lazy".to_owned()],
        "control: a permitted call does reach the factory"
    );

    // Denied, and never mounted. `forbidden-lazy` would build if it were asked
    // for, so the factory's silence is about the policy and not about the id.
    let refused = running.post("/v1/forbidden-lazy/rpc/Whoami", "{}").await;
    // The factory FIRST, and deliberately. A denied call that reached the
    // registry answers `404` rather than `403`, so a status assertion placed
    // here would fire first and report a status where the finding is that the
    // registry was touched at all.
    assert_eq!(
        running.asked(),
        ["lazy".to_owned()],
        "a denied caller left no id in the factory's record"
    );
    assert_eq!(refused.status(), http::StatusCode::FORBIDDEN);

    // And a second denied call does not accumulate either.
    let again = running.post("/v1/forbidden-lazy/rpc/Tick", "{}").await;
    assert_eq!(running.asked().len(), 1);
    assert_eq!(again.status(), http::StatusCode::FORBIDDEN);
}

/// No refusal names the actor it refused.
///
/// An id is a caller's own text, and echoing it back is how a shared error
/// surface becomes a confirmation oracle: a body that repeats `secret-tenant`
/// tells a caller that string reached something. Every refusal on the keyed
/// surface is checked here — malformed, denied, permitted-but-absent, and an
/// unknown method — over an id no other test uses.
///
/// `Location` is exempt BY DESIGN and is not tested here: it is minted only on
/// a success, and it echoes the caller's own canonical encoded segment because
/// a relative resource path has to.
///
/// **No break, and the reason is the guarantee.** `GatewayError` is a
/// fieldless enum and its body is rendered from `kind()` alone, so there is no
/// value in the type an id could travel in. Expressing this leak means ADDING a
/// field and threading it to `body()`, which `break-once.sh` cannot write in
/// one line. The row is a canary over a structural absence — like
/// `the_keyed_surface_exposes_no_lifecycle_or_discovery` — and what it catches
/// is the direction this actually fails in: someone enriching an error with the
/// thing that caused it.
#[tokio::test]
async fn no_refusal_repeats_the_actor_id_back() {
    let running = start().await;
    const SECRET: &str = "secret-tenant";

    for (case, path) in [
        ("denied", format!("/v1/forbidden{SECRET}/rpc/Count")),
        ("absent", format!("/v1/{SECRET}/rpc/Count")),
        ("unknown method", format!("/v1/{SECRET}/rpc/Nope")),
        ("malformed", format!("/v1/{SECRET}%2/rpc/Count")),
    ] {
        let response = running.post(&path, r#"{"value":1}"#).await;
        let headers: String = response
            .headers()
            .iter()
            .map(|(name, value)| format!("{name}: {}", value.to_str().unwrap_or("")))
            .collect();
        let body = response.text().await.expect("a body");
        assert!(
            !body.contains(SECRET),
            "{case}: the body repeated the actor id — {body}"
        );
        assert!(
            !headers.contains(SECRET),
            "{case}: a header repeated the actor id — {headers}"
        );
    }
}

/// A stopped actor is a missing one, and a lazy registry rebuilds it.
///
/// The three states a keyed call meets — mounted, stopped, missing — are one
/// state machine, so they are measured in one run rather than three fixtures
/// that could each be right about a different registry.
///
/// The last probe is the one that is easy to get wrong. `one` was mounted
/// eagerly and the factory only builds ids starting with `lazy`, so stopping
/// `one` is permanent; stopping `lazy` is not, because the next call rebuilds
/// it. A test that only stopped an eagerly mounted actor would report
/// "stopped means gone" as a property of the registry when it is a property of
/// that mount.
///
/// Reddened by row `HK42`'s break — `decode_actor(path, route_path)?` replaced
/// by a constant `"one"`. Terminated on `left: 404` against `right: 200`:
/// with every path resolving to `one`, the `lazy` rebuild probe finds a
/// stopped actor. Measured, so this row is not merely assumed to depend on the
/// gateway line it shares with the rest of the file.
#[tokio::test]
async fn a_stopped_actor_answers_as_missing_until_something_remounts_it() {
    let running = start().await;
    let budget = std::time::Duration::from_secs(2);

    let alive = running.post("/v1/one/rpc/Whoami", "{}").await;
    assert_eq!(alive.status(), http::StatusCode::OK);
    assert_eq!(alive.text().await.expect("a body"), r#""one""#);

    running
        .system
        .stop(&keylink::ActorId::new("one").expect("a valid id"), budget)
        .await
        .expect("the actor was running");

    let stopped = running.post("/v1/one/rpc/Whoami", "{}").await;
    assert_eq!(
        stopped.status(),
        http::StatusCode::NOT_FOUND,
        "a stopped actor answers exactly as one that was never mounted"
    );
    let never = running.post("/v1/never-mounted/rpc/Whoami", "{}").await;
    assert_eq!(never.status(), stopped.status());

    // A lazily built actor comes back, because the factory is what mounts it.
    let built = running.post("/v1/lazy/rpc/Whoami", "{}").await;
    assert_eq!(built.status(), http::StatusCode::OK);
    running
        .system
        .stop(&keylink::ActorId::new("lazy").expect("a valid id"), budget)
        .await
        .expect("the actor was running");
    let rebuilt = running.post("/v1/lazy/rpc/Whoami", "{}").await;
    assert_eq!(
        rebuilt.status(),
        http::StatusCode::OK,
        "stopping a lazily built actor removes the mount, not the ability to mount"
    );
    assert_eq!(rebuilt.text().await.expect("a body"), r#""lazy""#);
}

/// Concurrent keyed calls do not cross actors.
///
/// One gateway, one registry, many in-flight requests. `Whoami` answers the id
/// its actor was mounted under, so a call that resolved another actor's mount
/// says so in its own body — which is what makes this a routing test rather
/// than a throughput one.
///
/// Reddened by row `HK42`'s break. Terminated on `obra/4471 in round 0 was
/// answered by another actor`, `left: "one"` against `right: "obra/4471"`.
#[tokio::test]
async fn concurrent_keyed_calls_each_reach_their_own_actor() {
    let running = std::sync::Arc::new(start().await);
    let ids = ["one", "obra/4471", "lazy"];

    let mut calls = Vec::new();
    for round in 0..8 {
        for id in ids {
            let running = std::sync::Arc::clone(&running);
            let encoded = id.replace('/', "%2F");
            calls.push(tokio::spawn(async move {
                let path = format!("/v1/{encoded}/rpc/Whoami");
                let response = running.post(&path, "{}").await;
                (
                    round,
                    id,
                    response.status(),
                    response.text().await.expect("a body"),
                )
            }));
        }
    }

    for call in calls {
        let (round, id, status, body) = call.await.expect("the request task completes");
        assert_eq!(status, http::StatusCode::OK, "{id} in round {round}");
        assert_eq!(
            body,
            format!("\"{id}\""),
            "{id} in round {round} was answered by another actor"
        );
    }
}

/// A registry at capacity refuses a new actor without saying it exists.
///
/// The fixture holds three eager mounts and room for one lazy build, so the
/// SECOND lazy id meets the cap.
///
/// This row carries no external break and needs none: its own control is the
/// measurement. Freeing one slot flips `lazy-b` from `503` to `200` and flips
/// `unbuildable` from `503` back to `404`, in the same run, so a gateway that
/// answered `503` for another reason cannot satisfy it.
///
/// The refusal is `503`, not `404`: the id is perfectly good and a caller
/// retrying later may well get it. And while the registry is full, an id the
/// factory would REFUSE answers `503` too — measured, not assumed, because the
/// first draft of this row asserted `404` there and the run said `503`. The cap
/// is checked before the factory is asked, so a full registry answers nothing
/// at all about which ids exist. That is the stronger property and it was not
/// the one this row was written to expect.
#[tokio::test]
async fn a_registry_at_capacity_refuses_without_an_existence_answer() {
    let running = start().await;

    let first = running.post("/v1/lazy-a/rpc/Whoami", "{}").await;
    assert_eq!(first.status(), http::StatusCode::OK, "the last free slot");

    let full = running.post("/v1/lazy-b/rpc/Whoami", "{}").await;
    assert_eq!(
        full.status(),
        http::StatusCode::SERVICE_UNAVAILABLE,
        "a full registry is a temporary condition, not a missing actor"
    );

    // An id the factory would REFUSE, asked while the registry is full. Same
    // answer, so a full registry separates nothing.
    let refused = running.post("/v1/unbuildable/rpc/Whoami", "{}").await;
    assert_eq!(refused.status(), http::StatusCode::SERVICE_UNAVAILABLE);

    // And the cap really is what refused `lazy-b`: freeing a slot lets it in.
    running
        .system
        .stop(
            &keylink::ActorId::new("lazy-a").expect("a valid id"),
            std::time::Duration::from_secs(2),
        )
        .await
        .expect("the actor was running");
    // With the slot free and still free, the unbuildable id goes back to `404`,
    // which is what makes the `503` above a fact about the cap rather than
    // about that id. Probed BEFORE `lazy-b` is retried, because `lazy-b` takes
    // the freed slot and fills the registry again.
    let refused = running.post("/v1/unbuildable/rpc/Whoami", "{}").await;
    assert_eq!(refused.status(), http::StatusCode::NOT_FOUND);

    let retried = running.post("/v1/lazy-b/rpc/Whoami", "{}").await;
    assert_eq!(
        retried.status(),
        http::StatusCode::OK,
        "the refusal was the cap, not the id"
    );
}

/// A queue of keyed calls WAITS. It is never refused, and the mailbox depth is
/// not observable through this surface at all.
///
/// This row was written to assert a `503`, on the reading that a full mailbox
/// maps to `UpstreamUnavailable` the way a full registry does. It measured
/// `refused 0 of 12`: keylink parks a sender on a full mailbox rather than
/// rejecting it, so `MailboxFull` is not reachable through this gateway path at
/// all. The row asserts what actually happens, and the assertion that would
/// have been written instead is recorded here because it is the one a reader
/// will reach for next.
///
/// What that means for the surface: mailbox pressure on a keyed actor is
/// latency, not an error category. A deployment that needs it to become one
/// has to bound the wait somewhere above this gateway.
///
/// And the depth is genuinely invisible: raise `mailbox_depth` from 1 to 64 and
/// this row still passes, because an actor loop handles one message at a time
/// and twelve 40ms calls serialise at any depth. That is not a hole in the row
/// — it is the finding. There is no assertion here that depends on the depth
/// because no HTTP-observable behaviour does. The fixture keeps a depth of 1
/// so the queue is as short as it can be and the row still sees waiting.
///
/// One actor, twelve calls each holding the loop 40ms. The time bound is a
/// LOWER bound and nothing else — a slow machine only makes it larger, so it
/// cannot flake in the direction that matters. It is discriminating in the
/// direction that does: concurrent calls would finish in about one hold.
///
/// `HK42`'s break leaves this row GREEN, `exit=0`, and that is correct — every
/// path here names one actor already, so redirecting them all to `one` changes
/// nothing it measures.
#[tokio::test]
async fn a_queue_of_keyed_calls_waits_and_is_never_refused() {
    let running = std::sync::Arc::new(start().await);
    const CALLS: usize = 12;
    const HOLD_MS: u64 = 40;

    let started = tokio::time::Instant::now();
    let mut calls = Vec::new();
    for _ in 0..CALLS {
        let running = std::sync::Arc::clone(&running);
        calls.push(tokio::spawn(async move {
            running
                .post("/v1/one/rpc/Count", r#"{"value":40}"#)
                .await
                .status()
        }));
    }
    for call in calls {
        let status = call.await.expect("the request task completes");
        assert_eq!(
            status,
            http::StatusCode::OK,
            "a queued call is served, not refused"
        );
    }
    let elapsed = started.elapsed();

    // Serialised through one actor: at least CALLS - 1 holds must have elapsed.
    // Anything concurrent would finish in about one hold.
    assert!(
        elapsed >= std::time::Duration::from_millis(HOLD_MS * (CALLS as u64 - 1)),
        "the calls queued rather than ran together — {elapsed:?}"
    );

    let after = running.post("/v1/one/rpc/Whoami", "{}").await;
    assert_eq!(after.status(), http::StatusCode::OK);
    assert_eq!(after.text().await.expect("a body"), r#""one""#);
}
