//! Principal-bound blob byte resources, over a real gateway and a real service.
//!
//! Everything here runs against a genuine Keylink service on a real iroh
//! endpoint, reached over a real TCP socket, with a real content store behind
//! it. The blob bytes cross a QUIC connection the gateway opens itself.

#![allow(clippy::expect_used)]

use axum::{middleware, response::Response, Router};
use http::Request;
use iroh::{endpoint::presets, Endpoint};
use keylink::{Allowlist, BlobId, Blobs, Context, Node};
use keylink_http::{
    Access, AccessError, Addressing, Call, Challenge, Gateway, HttpSurface, Principal,
    StreamingLimits,
};

keylink::service! {
    service Vault;
    // `size` is the declared return value, so a lookup answers something the
    // test can compare exactly without touching the bytes.
    blob Fetch(name: String) -> u64;
    // A second blob method, so a ticket can be reached under a method nobody
    // granted it for. It is never called.
    blob Absent(name: String) -> u64;
}

/// Content whose every byte depends on its position, so a slice taken from the
/// wrong offset cannot coincidentally equal the right one.
fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Longer than one 16 KiB bao block and aligned to none of them.
const SPANNING: usize = 70_000;

#[derive(Clone)]
struct Handler {
    known: std::collections::HashMap<String, (BlobId, u64)>,
}

impl vault::Handler for Handler {
    async fn fetch(&self, _cx: &Context, name: String) -> (u64, BlobId) {
        let (id, size) = self.known.get(&name).copied().expect("a known name");
        (size, id)
    }

    async fn absent(&self, cx: &Context, name: String) -> (u64, BlobId) {
        self.fetch(cx, name).await
    }
}

/// Permits everything unless the caller asks to be refused.
///
/// One gateway has to be able to answer both ways, because the properties
/// under test are about ONE ticket table. A second gateway with a second table
/// answers `404` for every address whatever the rule is, so a denial test
/// built that way passes with the guard removed — which is how the first
/// version of two rows here proved nothing.
#[derive(Clone)]
struct Gated;

const DENY_HEADER: &str = "x-test-deny";
const TENANT_HEADER: &str = "x-test-tenant";

impl Access for Gated {
    async fn authorize(&self, call: Call<'_>) -> Result<(), AccessError> {
        if call.headers.get(DENY_HEADER).is_some() {
            return Err(AccessError::Forbidden);
        }
        Ok(())
    }
}

/// The principal a deployment's authentication layer would insert.
///
/// Read from a request header for the same reason: one gateway, one ticket
/// table, two tenants. Nothing under test reads this header — the gateway sees
/// only the `Principal` extension, exactly as it would in production.
async fn authenticate(mut request: Request<axum::body::Body>, next: middleware::Next) -> Response {
    let tenant = request
        .headers()
        .get(TENANT_HEADER)
        .map_or_else(|| b"tenant-a".to_vec(), |value| value.as_bytes().to_vec());
    request
        .extensions_mut()
        .insert(Principal::new(tenant).expect("valid principal"));
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
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    /// What the store holds, so a test can compare bytes without asking the
    /// gateway for them twice.
    content: Vec<u8>,
    _server: keylink::Server,
    _nodes: Vec<Node>,
    /// Present only for the revocable fixture, and held so the relay outlives
    /// the requests that go through it.
    _router: Option<iroh::protocol::Router>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn limits() -> StreamingLimits {
    StreamingLimits::new(1024, 1024, 8 * 1024 * 1024, 4).expect("positive limits")
}

fn challenge() -> Challenge {
    Challenge::new(http::HeaderValue::from_static("Bearer realm=keylink")).expect("valid challenge")
}

async fn start_with(limits: StreamingLimits) -> Running {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let blobs = Blobs::memory();
    let content = payload(SPANNING);

    let mut known = std::collections::HashMap::new();
    for (name, bytes) in [
        ("datasheet", content.clone()),
        ("empty", Vec::new()),
        ("tiny", b"nine byte".to_vec()),
    ] {
        let id = blobs.add_bytes(bytes.clone()).await.expect("stores");
        known.insert(name.to_owned(), (id, bytes.len() as u64));
    }

    let server = keylink::Server::builder(&server_node)
        .service(
            vault::service(Handler { known }, Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("one service with a store");
    let client = vault::Client::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("client connects");

    let surface = HttpSurface::new("/v1", Addressing::Singleton)
        .expect("valid surface")
        .with_streaming(limits);
    let gateway = Gateway::<vault::External, _>::new(client, Gated, challenge())
        .router::<()>(surface)
        .expect("surface builds");
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
        task,
        content,
        _server: server,
        _nodes: vec![server_node, client_node],
        _router: None,
    }
}

async fn start() -> Running {
    start_with(limits()).await
}

/// The same gateway, with every handle pointed at a holder the test can revoke
/// and can stall.
struct Revocable {
    running: Running,
    refusing: std::sync::Arc<std::sync::atomic::AtomicBool>,
    holding: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    released: std::sync::Arc<tokio::sync::Semaphore>,
    seen: tokio::sync::mpsc::UnboundedReceiver<Seen>,
}

impl Revocable {
    fn revoke(&self) {
        self.refusing
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Stall the next data answer, and only that one.
    fn hold(&self) {
        self.holding.store(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn release(&self) {
        self.holding.store(0, std::sync::atomic::Ordering::SeqCst);
        self.released.add_permits(1);
    }

    /// The next thing the holder did. Every branch of its accept reports, so
    /// this cannot outlast the holder.
    async fn seen(&mut self) -> Seen {
        self.seen
            .recv()
            .await
            .expect("the holder reports every outcome")
    }

    /// Wait until a DATA request reaches the holder.
    ///
    /// The size preflight opens and closes a connection of its own before the
    /// reader does, so the first event is a `Closed` that says nothing about
    /// the download. Measured — both concurrency rows read it as the download
    /// and failed with `left: Closed`.
    async fn opened(&mut self) {
        while self.seen().await != Seen::Opened {}
    }
}

async fn start_revocable(limits: StreamingLimits) -> Revocable {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let holder_node = local_node().await;
    let reader_node = local_node().await;
    let blobs = Blobs::memory();
    let content = payload(SPANNING);

    let mut known = std::collections::HashMap::new();
    for (name, bytes) in [
        ("datasheet", content.clone()),
        ("empty", Vec::new()),
        ("tiny", b"nine byte".to_vec()),
    ] {
        let id = blobs.add_bytes(bytes.clone()).await.expect("stores");
        known.insert(name.to_owned(), (id, bytes.len() as u64));
    }

    let server = keylink::Server::builder(&server_node)
        .service(
            vault::service(Handler { known }, Allowlist::anyone())
                .with_blobs(&blobs)
                .build(),
        )
        .spawn()
        .expect("one service with a store");
    let client = vault::Client::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("client connects");

    let refusing = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let holding = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let released = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let (seen_tx, seen) = tokio::sync::mpsc::unbounded_channel();
    // The relay dials the real store from the CLIENT's endpoint, because that
    // is the peer the handler granted.
    let router = iroh::protocol::Router::builder(holder_node.endpoint().clone())
        .accept(
            iroh_blobs::ALPN,
            Holder {
                from: client_node.endpoint().clone(),
                to: server_node.endpoint().addr(),
                refusing: std::sync::Arc::clone(&refusing),
                holding: std::sync::Arc::clone(&holding),
                released: std::sync::Arc::clone(&released),
                seen: seen_tx,
            },
        )
        .spawn();

    let surface = HttpSurface::new("/v1", Addressing::Singleton)
        .expect("valid surface")
        .with_streaming(limits);
    let gateway = Gateway::<Redirect, _>::new(
        Redirected {
            inner: client,
            reader: reader_node.clone(),
            holder: holder_node.endpoint().addr(),
        },
        Gated,
        challenge(),
    )
    .router::<()>(surface)
    .expect("surface builds");
    let app: Router<()> = Router::new()
        .merge(gateway)
        .layer(middleware::from_fn(authenticate));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("listener address");
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Revocable {
        running: Running {
            address,
            task,
            content,
            _server: server,
            _nodes: vec![server_node, client_node, holder_node, reader_node],
            _router: Some(router),
        },
        refusing,
        holding,
        released,
        seen,
    }
}

impl Running {
    fn url(&self, path: &str) -> String {
        format!("http://{}/v1{path}", self.address)
    }

    /// Look one name up and return the whole response.
    async fn lookup(&self, name: &str) -> reqwest::Response {
        reqwest::Client::new()
            .post(self.url("/blob/Fetch"))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(format!(r#"{{"name":"{name}"}}"#))
            .send()
            .await
            .expect("the lookup completes")
    }

    /// Look one name up and return the byte-resource path it was given.
    async fn resource(&self, name: &str) -> String {
        let response = self.lookup(name).await;
        assert_eq!(response.status(), http::StatusCode::CREATED, "{name}");
        response
            .headers()
            .get(http::header::LOCATION)
            .expect("a Location")
            .to_str()
            .expect("ascii")
            .to_owned()
    }

    async fn get(&self, path: &str, headers: &[(http::HeaderName, &str)]) -> reqwest::Response {
        self.send(http::Method::GET, path, headers).await
    }

    async fn head(&self, path: &str, headers: &[(http::HeaderName, &str)]) -> reqwest::Response {
        self.send(http::Method::HEAD, path, headers).await
    }

    async fn send(
        &self,
        method: http::Method,
        path: &str,
        headers: &[(http::HeaderName, &str)],
    ) -> reqwest::Response {
        let mut request =
            reqwest::Client::new().request(method, format!("http://{}{path}", self.address));
        for (name, value) in headers {
            request = request.header(name.clone(), *value);
        }
        request.send().await.expect("the request completes")
    }
}

fn header(response: &reqwest::Response, name: http::HeaderName) -> Option<&str> {
    response.headers().get(name)?.to_str().ok()
}

// ──────────────────────────────────────────── a holder the test can revoke
//
// Four mapped rows turn on a grant that STOPS WORKING while the gateway still
// holds a ticket for it. Nothing in `keylink::Blobs` revokes one, and shutting
// the holder down yields `Error::Transport`, which the gateway answers with a
// bad-gateway rather than the `410` those rows are about.
//
// So the handle the gateway receives is redirected at a holder the test owns.
// That holder RELAYS every request to the real store, so a reader still sees
// real bao from a real provider over a real QUIC connection — until the test
// revokes, after which it answers exactly the way `keylink::BlobsAdmission`
// answers a peer holding no grant: close with `ERR_PERMISSION`. That is the
// one signal `BlobHandle` maps to `Error::BlobRefused`.

/// What the holder did, in the order it did it.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    /// A data request arrived. Not the size preflight.
    Opened,
    /// A revoked holder is about to refuse a connection.
    Refusing,
    /// The connection ended.
    Closed,
}

#[derive(Clone)]
struct Holder {
    from: iroh::Endpoint,
    to: iroh::EndpointAddr,
    refusing: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// How many data answers still have to wait before they are written, so
    /// a test can watch a download that has started and not finished.
    ///
    /// A COUNT and not a flag: a flag stalls every later download too, and the
    /// row that watches a second caller being served then deadlocks on its own
    /// fixture. Measured — the first version hung for two minutes.
    holding: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Permits, not a `Notify`. A notification sent before the holder reaches
    /// its await is lost; a permit waits.
    released: std::sync::Arc<tokio::sync::Semaphore>,
    seen: tokio::sync::mpsc::UnboundedSender<Seen>,
}

impl std::fmt::Debug for Holder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Holder").finish_non_exhaustive()
    }
}

/// Reports the end of a connection however the accept ends, including its `?`
/// paths. The sender lives as long as the router, so without this a test
/// waiting on `Closed` would wait for ever instead of failing.
struct Ended(tokio::sync::mpsc::UnboundedSender<Seen>);

impl Drop for Ended {
    fn drop(&mut self) {
        self.0.send(Seen::Closed).ok();
    }
}

fn preflight_ranges() -> iroh_blobs::protocol::ChunkRangesSeq {
    use iroh_blobs::protocol::ChunkRangesExt;
    iroh_blobs::protocol::ChunkRangesSeq::from_ranges([
        iroh_blobs::protocol::ChunkRanges::last_chunk(),
    ])
}

/// Ask the real provider one request and return its exact answer.
async fn ask(
    from: iroh::Endpoint,
    to: iroh::EndpointAddr,
    request: iroh_blobs::protocol::GetRequest,
) -> Vec<u8> {
    let connection = from
        .connect(to, iroh_blobs::ALPN)
        .await
        .expect("dials the real store");
    let (mut send, mut recv) = connection.open_bi().await.expect("opens a request stream");
    let asked = postcard::to_stdvec(&iroh_blobs::protocol::Request::Get(request))
        .expect("encodes a get request");
    send.write_all(&asked).await.expect("writes the request");
    send.finish().expect("finishes the request");
    recv.read_to_end(16 * 1024 * 1024)
        .await
        .expect("reads the whole answer")
}

impl Holder {
    /// Claim one stall, if any are left.
    fn take_stall(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.holding
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_sub(1)
            })
            .is_ok()
    }

    async fn wait(&self) {
        self.released
            .acquire()
            .await
            .map(tokio::sync::SemaphorePermit::forget)
            .ok();
    }
}

impl iroh::protocol::ProtocolHandler for Holder {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        use std::sync::atomic::Ordering;

        let _ended = Ended(self.seen.clone());
        // Revocation is answered at the connection, which is where keylink's
        // own mount answers a peer that holds no grant at all.
        if self.refusing.load(Ordering::SeqCst) {
            // Reported and stalled BEFORE the close, so a test can park a
            // refusal in flight and refresh the ticket underneath it.
            let stall = self.take_stall();
            self.seen.send(Seen::Refusing).ok();
            if stall {
                self.wait().await;
            }
            connection.close(iroh_blobs::protocol::ERR_PERMISSION, b"permission");
            return Ok(());
        }
        loop {
            let Ok((mut send, mut recv)) = connection.accept_bi().await else {
                break;
            };
            let (request, _) = iroh_blobs::protocol::Request::read_async(&mut recv)
                .await
                .map_err(iroh::protocol::AcceptError::from_err)?;
            let iroh_blobs::protocol::Request::Get(request) = request else {
                break;
            };
            let data = request.ranges != preflight_ranges();
            let answer = ask(self.from.clone(), self.to.clone(), request).await;
            if data {
                let stall = self.take_stall();
                self.seen.send(Seen::Opened).ok();
                if stall {
                    self.wait().await;
                }
            }
            if send.write_all(&answer).await.is_err() {
                break;
            }
            if send.finish().is_err() {
                break;
            }
        }
        Ok(())
    }
}

/// The generated service, with every returned handle pointed at [`Holder`].
#[derive(Clone)]
struct Redirected {
    inner: vault::Client,
    reader: Node,
    holder: iroh::EndpointAddr,
}

struct Redirect;

impl keylink::ExternalService for Redirect {
    const SERVICE: &'static str = <vault::External as keylink::ExternalService>::SERVICE;
    const METHODS: &'static [keylink::ExternalMethod] =
        <vault::External as keylink::ExternalService>::METHODS;
    type Client = Redirected;

    fn invoke<C: keylink::ServiceCodec>(
        client: &Self::Client,
        method: &str,
        arguments: &[u8],
        call: keylink::ExternalCall,
        codec: C,
    ) -> keylink::ExternalFuture<C> {
        let future = vault::External::invoke(&client.inner, method, arguments, call, codec);
        let reader = client.reader.clone();
        let holder = client.holder.clone();
        Box::pin(async move {
            match future.await {
                // The ADDRESS is the real one the handler granted. Only where
                // it is fetched from changes, so nothing about verification is
                // stood in for.
                Ok(keylink::ExternalOutput::Blob { value, handle }) => {
                    Ok(keylink::ExternalOutput::Blob {
                        value,
                        handle: keylink::BlobHandle::new(&reader, holder, handle.id()),
                    })
                }
                other => other,
            }
        })
    }
}

// ────────────────────────────────────────────────────────────── the lookup

/// A lookup answers the declared value and where the bytes live, and moves
/// none of them.
///
/// DEVIATION, and it is the honest limit of this row. The plan's terminator is
/// "blob peer request counter remains zero", and no such counter is reachable
/// from here: the gateway holds a handle it never reads, and the blobs mount
/// that would see a fetch is built by `keylink::Server::spawn` on the serving
/// node, which exposes nothing. What is asserted instead is that a 70 KiB
/// representation produces a response whose whole body is the declared value.
/// That proves the payload did not reach the client. It does NOT exclude a
/// fetch-and-discard, and `blob_get_serves_full_verified_representation` is
/// where the bytes are proved to move only under `GET`.
#[tokio::test]
async fn blob_lookup_returns_exact_value_location_and_no_fetch() {
    let running = start().await;

    let response = running.lookup("datasheet").await;
    assert_eq!(response.status(), http::StatusCode::CREATED);
    assert_eq!(
        header(&response, http::header::CONTENT_TYPE),
        Some("application/json")
    );
    let location = header(&response, http::header::LOCATION)
        .expect("a Location")
        .to_owned();
    assert!(
        location.starts_with("/v1/blob/Fetch/"),
        "prefix-preserving, built from the path the caller reached: {location}"
    );
    let address = location
        .rsplit('/')
        .next()
        .expect("a trailing address segment");
    assert_eq!(address.len(), 64, "a content address, not an opaque handle");

    let body = response.text().await.expect("a body");
    assert_eq!(
        body,
        SPANNING.to_string(),
        "the declared value, and nothing else"
    );
}

/// A range header on a lookup names nothing, and is ignored rather than
/// refused.
///
/// DEVIATION, on the row name and on the behaviour. The plan maps this row as
/// `blob_lookup_rejects_range_headers_before_body_poll` and its route table
/// says to reject both fields. RFC 9110 §14.2 is a MUST the other way — "A
/// server MUST ignore a Range header field received with a request method ...
/// for which range handling is not defined" — and §13.1.5 says the same of
/// `If-Range` without a `Range`. A download manager that attaches `Range` to
/// every request would otherwise meet a hard refusal on a route the
/// specification guarantees drops the field. An HTTP gateway follows the RFC,
/// and the name follows the behaviour.
#[tokio::test]
async fn blob_lookup_ignores_range_headers_before_body_poll() {
    let running = start().await;

    for name in [http::header::RANGE, http::header::IF_RANGE] {
        let response = reqwest::Client::new()
            .post(running.url("/blob/Fetch"))
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(name.clone(), "bytes=0-1")
            .body(r#"{"name":"datasheet"}"#)
            .send()
            .await
            .expect("the request completes");
        assert_eq!(response.status(), http::StatusCode::CREATED, "{name}");
        assert!(
            response.headers().get(http::header::LOCATION).is_some(),
            "{name}: and the lookup answers exactly as it would without it"
        );
    }
}

// ──────────────────────────────────────────────────────────────── the bytes

/// The full representation, verified, with the metadata a cache needs.
#[tokio::test]
async fn blob_get_serves_full_verified_representation() {
    let running = start().await;
    let path = running.resource("datasheet").await;

    let response = running.get(&path, &[]).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        header(&response, http::header::CONTENT_TYPE),
        Some("application/octet-stream")
    );
    assert_eq!(
        header(&response, http::header::ACCEPT_RANGES),
        Some("bytes")
    );
    assert_eq!(
        header(&response, http::header::CACHE_CONTROL),
        Some("private, no-transform"),
        "a ticket is bound to one principal, and a proxy must not recompress \
         bytes named by their own hash"
    );
    assert_eq!(
        header(&response, http::header::CONTENT_LENGTH),
        Some(SPANNING.to_string().as_str()),
        "the verified length, not a guess"
    );
    let tag = header(&response, http::header::ETAG)
        .expect("an ETag")
        .to_owned();
    assert!(
        path.ends_with(tag.trim_matches('"')),
        "the tag IS the content address, so two responses carrying one tag are \
         byte-identical by construction: {tag}"
    );

    let body = response.bytes().await.expect("a body");
    assert_eq!(body.as_ref(), running.content.as_slice());
}

/// An unaligned range arrives exactly, with the range it names.
#[tokio::test]
async fn blob_get_serves_exact_unaligned_range() {
    let running = start().await;
    let path = running.resource("datasheet").await;

    let response = running
        .get(&path, &[(http::header::RANGE, "bytes=1000-39999")])
        .await;
    assert_eq!(response.status(), http::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        header(&response, http::header::CONTENT_RANGE),
        Some("bytes 1000-39999/70000")
    );
    assert_eq!(
        header(&response, http::header::CONTENT_LENGTH),
        Some("39000")
    );

    let body = response.bytes().await.expect("a body");
    assert_eq!(body.as_ref(), &running.content[1000..40_000]);
}

/// HEAD answers for the whole representation and never opens a reader.
///
/// The reader-open counter the plan names is not reachable from here, so what
/// is asserted is the observable consequence: a HEAD carrying a range answers
/// `200` with the FULL length rather than `206` with a partial one. A handler
/// that opened a ranged reader could not produce that pair.
///
/// Watched red with `scripts/break-once.sh` (row `HK19`) by replacing
/// `let wants_bytes = route.kind() == RouteKind::BlobGet;`
/// in `keylink-http/src/gateway.rs` with `let wants_bytes = true;`.
#[tokio::test]
async fn blob_head_ignores_range_and_never_opens_reader() {
    let running = start().await;
    let path = running.resource("datasheet").await;

    for headers in [
        Vec::new(),
        vec![(http::header::RANGE, "bytes=0-9")],
        vec![
            (http::header::RANGE, "bytes=0-9"),
            (http::header::IF_RANGE, "\"nonsense\""),
        ],
    ] {
        let response = running.head(&path, &headers).await;
        assert_eq!(response.status(), http::StatusCode::OK, "{headers:?}");
        assert_eq!(
            header(&response, http::header::CONTENT_LENGTH),
            Some(SPANNING.to_string().as_str()),
            "{headers:?}"
        );
        // No body assertion here. A client discards a HEAD response body by
        // protocol, so `bytes().len() == 0` holds whatever this gateway
        // writes — it would read as a guard and prove nothing. The two
        // assertions above are the ones with teeth, and `HK19` watched them.
    }
}

/// Zero bytes is a representation, and every nonempty range over it fails the
/// same way.
#[tokio::test]
async fn empty_blob_get_head_conditions_and_ranges_are_exact() {
    let running = start().await;
    let path = running.resource("empty").await;

    let response = running.get(&path, &[]).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(header(&response, http::header::CONTENT_LENGTH), Some("0"));
    let tag = header(&response, http::header::ETAG)
        .expect("an ETag")
        .to_owned();
    assert_eq!(response.bytes().await.expect("a body").len(), 0);

    let response = running.head(&path, &[]).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(header(&response, http::header::CONTENT_LENGTH), Some("0"));

    for range in ["bytes=0-0", "bytes=0-"] {
        let response = running.get(&path, &[(http::header::RANGE, range)]).await;
        assert_eq!(
            response.status(),
            http::StatusCode::RANGE_NOT_SATISFIABLE,
            "{range}"
        );
        assert_eq!(
            header(&response, http::header::CONTENT_RANGE),
            Some("bytes */0"),
            "{range}: and it names the length that would make an ask work"
        );
    }
    let response = running
        .get(&path, &[(http::header::RANGE, "bytes=-1")])
        .await;
    assert_eq!(
        response.status(),
        http::StatusCode::OK,
        "a suffix of empty content cannot be written as a Content-Range, so \
         the range is ignored rather than called unsatisfiable"
    );

    let response = running
        .get(&path, &[(http::header::IF_NONE_MATCH, &tag)])
        .await;
    assert_eq!(response.status(), http::StatusCode::NOT_MODIFIED);
}

/// Every reason a resource is absent produces one redacted `404`.
///
/// A guessed address, another principal's, another method's and one evicted
/// from the finite table must be indistinguishable. Telling them apart is an
/// existence oracle over content this caller was never granted.
///
/// Watched red with `scripts/break-once.sh` (row `HK16`) by replacing
/// `principal, method, actor: None, id,`
/// in `keylink-http/src/gateway.rs` with a constant principal, which lets
/// `tenant-z` read what `tenant-a` was handed.
///
/// The first version of this row came back GREEN under that break, `exit=0`.
/// The key was built twice, so forcing it on the read side alone made every
/// lookup MISS — and a row that asserts only absences is satisfied by a
/// gateway that has stopped serving anything. It has one definition and a
/// positive control now.
#[tokio::test]
async fn blob_ticket_absence_is_identical_across_guess_principal_method_and_eviction() {
    let running = start().await;
    let path = running.resource("datasheet").await;
    let guessed = format!("/v1/blob/Fetch/{}", "ab".repeat(32));
    let malformed = "/v1/blob/Fetch/not-an-address".to_owned();
    // The same address under a method nobody granted it for. `Absent` exists
    // only so this row has a second method to reach for.
    let wrong_method = path.replace("/Fetch/", "/Absent/");
    let tenant_header = http::HeaderName::from_static(TENANT_HEADER);

    // Positive control, before anything is evicted. Without it a break that answers `404` for
    // everything satisfies every assertion below, which is exactly how the
    // first version of `HK16` came back green.
    assert_eq!(
        running.get(&path, &[]).await.status(),
        http::StatusCode::OK,
        "control: the caller who was handed this address can still read it"
    );

    // The wrong-method probe runs BEFORE the eviction below. After it, tenant
    // A holds no ticket at all and the probe would prove nothing about method
    // scoping — it would only re-prove eviction under a second name.
    assert_eq!(
        running
            .get(&wrong_method, &[(tenant_header.clone(), "tenant-a")])
            .await
            .status(),
        http::StatusCode::NOT_FOUND,
        "a ticket does not carry across to another method"
    );

    // Evicted: the table holds four. Four other tenants each take a ticket for
    // the same content, which pushes the first one off the end of ONE table.
    // A second gateway would have proved nothing here, because its table is
    // empty whatever this one does.
    for tenant in ["tenant-c", "tenant-d", "tenant-e", "tenant-f"] {
        let response = reqwest::Client::new()
            .post(running.url("/blob/Fetch"))
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(tenant_header.clone(), tenant)
            .body(r#"{"name":"datasheet"}"#)
            .send()
            .await
            .expect("the lookup completes");
        assert_eq!(response.status(), http::StatusCode::CREATED, "{tenant}");
    }

    let mut shapes = Vec::new();
    for (what, absent, tenant) in [
        ("a guessed address", guessed.as_str(), "tenant-a"),
        ("a malformed address", malformed.as_str(), "tenant-a"),
        ("another principal's", path.as_str(), "tenant-z"),
        ("another method's", wrong_method.as_str(), "tenant-a"),
        ("one evicted from the table", path.as_str(), "tenant-a"),
    ] {
        let response = running
            .get(absent, &[(tenant_header.clone(), tenant)])
            .await;
        let status = response.status();
        // Status BEFORE decoding. A `200` carrying bytes fails `json()` first,
        // and the run then reports a decode error instead of naming the leak.
        assert_eq!(status, http::StatusCode::NOT_FOUND, "{what}");
        let body: serde_json::Value = response.json().await.expect("an error envelope");
        assert_eq!(body["error"]["code"], "blob_not_found", "{what}");
        shapes.push((what, status, body));
    }

    let (_, first_status, first_body) = shapes.first().expect("five shapes").clone();
    for (what, status, body) in &shapes {
        assert_eq!(
            (*status, body.clone()),
            (first_status, first_body.clone()),
            "{what} answers the same as every other absence"
        );
    }
}

/// `Access` runs before the ticket lookup, so a denial says nothing about what
/// exists.
///
/// The gateway is the SAME one that issued the ticket, and the denial is a
/// per-request header. That is what makes the comparison mean anything: a
/// second gateway with an empty table answers alike for both paths however the
/// order is written, so the first version of this row proved nothing.
///
/// Watched red with `scripts/break-once.sh` (row `HK20`) by replacing
/// `Err(AccessError::Forbidden) => Err(GatewayError::Forbidden.response(None)),`
/// in `keylink-http/src/gateway.rs` with
/// `Err(AccessError::Forbidden) => Ok(Admitted { method, principal }),`, after
/// which the denied caller reaches the ticket lookup and the resource it holds
/// answers `200` while the invented one still answers `404`.
#[tokio::test]
async fn blob_access_runs_before_ticket_lookup_and_id_details() {
    let running = start().await;
    let path = running.resource("datasheet").await;
    let deny = http::HeaderName::from_static(DENY_HEADER);

    let real = running.get(&path, &[(deny.clone(), "1")]).await;
    let invented = running
        .get(
            &format!("/v1/blob/Fetch/{}", "cd".repeat(32)),
            &[(deny, "1")],
        )
        .await;

    assert_eq!(real.status(), http::StatusCode::FORBIDDEN);
    assert_eq!(invented.status(), http::StatusCode::FORBIDDEN);
    assert_eq!(
        real.json::<serde_json::Value>().await.expect("envelope"),
        invented
            .json::<serde_json::Value>()
            .await
            .expect("envelope"),
        "a resource this caller holds and one that never existed answer alike"
    );
}

/// A byte route takes no request body and no replay key.
///
/// Watched red with `scripts/break-once.sh` (row `HK18`) by replacing
/// `Some(value) => value.to_str().is_ok_and(|value| value.trim() == "0"),`
/// in `keylink-http/src/gateway.rs` with `Some(_) => true,`, after which a
/// caller can push bytes at a route that has no use for them.
#[tokio::test]
async fn blob_get_and_head_reject_body_and_idempotency_after_access() {
    let running = start().await;
    let path = running.resource("datasheet").await;

    let response = running
        .get(
            &path,
            &[(http::HeaderName::from_static("idempotency-key"), "k")],
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json().await.expect("an error envelope");
    assert_eq!(body["error"]["code"], "idempotency_not_supported");

    let response = reqwest::Client::new()
        .get(format!("http://{}{path}", running.address))
        .body("a body a byte route has no use for")
        .send()
        .await
        .expect("the request completes");
    assert_eq!(response.status(), http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

/// Conditions are answered before ranges, and bad grammar is a refusal.
#[tokio::test]
async fn blob_conditions_precede_ranges_and_bad_grammar_is_refused() {
    let running = start().await;
    let path = running.resource("datasheet").await;
    let tag = header(&running.get(&path, &[]).await, http::header::ETAG)
        .expect("an ETag")
        .to_owned();

    // A match wins even with a range in hand: `304` is a complete answer, and
    // a range of a representation the caller already holds is wasted work.
    //
    // Watched red with `scripts/break-once.sh` (row `HK17`) by replacing
    // `Ok(true) => return not_modified(&tag),`
    // in `keylink-http/src/gateway.rs` with `Ok(true) => {}`, after which the
    // range wins and the caller is sent bytes it said it already had.
    let response = running
        .get(
            &path,
            &[
                (http::header::IF_NONE_MATCH, &tag),
                (http::header::RANGE, "bytes=0-9"),
            ],
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::NOT_MODIFIED);
    assert_eq!(header(&response, http::header::ETAG), Some(tag.as_str()));
    assert_eq!(response.bytes().await.expect("a body").len(), 0);

    let response = running
        .get(&path, &[(http::header::IF_NONE_MATCH, "unquoted")])
        .await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);

    // Malformed: no representation could make the ask meaningful.
    for range in ["bytes=5-1", "bytes=0 - 1", "bytes=abc-1"] {
        let response = running.get(&path, &[(http::header::RANGE, range)]).await;
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST, "{range}");
    }

    // Ignored, not refused. RFC 9110 §14.2 lets an origin ignore a `Range` it
    // does not serve, and REQUIRES it to ignore a unit it does not understand.
    // The plan's route table says `400` for a wrong unit and for multiple
    // ranges; the RFC says otherwise, and an HTTP gateway follows the RFC.
    for range in ["items=0-1", "bytes=0-1,4-5"] {
        let response = running.get(&path, &[(http::header::RANGE, range)]).await;
        assert_eq!(response.status(), http::StatusCode::OK, "{range}");
        assert_eq!(
            header(&response, http::header::CONTENT_LENGTH),
            Some(SPANNING.to_string().as_str()),
            "{range}: the whole representation"
        );
        assert_eq!(
            response.bytes().await.expect("a body").len(),
            SPANNING,
            "{range}: and all of it really arrives"
        );
    }

    let response = running
        .get(&path, &[(http::header::RANGE, "bytes=70000-70010")])
        .await;
    assert_eq!(response.status(), http::StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        header(&response, http::header::CONTENT_RANGE),
        Some("bytes */70000")
    );
}

/// An `If-Range` that does not match exactly produces the full representation.
#[tokio::test]
async fn blob_if_range_falls_back_to_the_whole_representation() {
    let running = start().await;
    let path = running.resource("datasheet").await;
    let tag = header(&running.get(&path, &[]).await, http::header::ETAG)
        .expect("an ETag")
        .to_owned();

    let response = running
        .get(
            &path,
            &[
                (http::header::RANGE, "bytes=0-9"),
                (http::header::IF_RANGE, &tag),
            ],
        )
        .await;
    assert_eq!(
        response.status(),
        http::StatusCode::PARTIAL_CONTENT,
        "one exact strong tag permits the range"
    );

    for wrong in [format!("W/{tag}"), "\"other\"".to_owned()] {
        let response = running
            .get(
                &path,
                &[
                    (http::header::RANGE, "bytes=0-9"),
                    (http::header::IF_RANGE, &wrong),
                ],
            )
            .await;
        assert_eq!(response.status(), http::StatusCode::OK, "{wrong}");
        assert_eq!(
            header(&response, http::header::CONTENT_LENGTH),
            Some(SPANNING.to_string().as_str()),
            "{wrong}: the whole representation, never a range of the wrong one"
        );
        assert_eq!(
            response.bytes().await.expect("a body").len(),
            SPANNING,
            "{wrong}: and the body is as long as the header promised. A length \
             header alone would pass while a partial body shipped."
        );
    }
}

// ────────────────────────────────────── a grant that stops working mid-run

/// A `HEAD` rechecks availability on every request, and a refusal removes the
/// exact generation it saw.
///
/// A ticket records that a grant was issued, not that the holder still serves
/// it. Answering from the ticket alone would report a length for content
/// nobody can fetch — and `HEAD` is exactly what a client uses to decide
/// whether to start.
///
/// Watched red with `scripts/break-once.sh` (row `HK23`) by replacing
/// `tickets.remove(ticket, generation);`
/// in `keylink-http/src/gateway.rs` with `let _ = generation;`, after which
/// the refused ticket stays in the table and the next request answers `410`
/// for ever instead of `404`.
#[tokio::test]
async fn blob_head_rechecks_grant_and_removes_stale_generation() {
    let fixture = start_revocable(limits()).await;
    let path = fixture.running.resource("datasheet").await;

    let first = fixture.running.head(&path, &[]).await;
    assert_eq!(
        first.status(),
        http::StatusCode::OK,
        "control: it is served"
    );
    assert_eq!(
        header(&first, http::header::CONTENT_LENGTH),
        Some(SPANNING.to_string().as_str())
    );

    fixture.revoke();

    let second = fixture.running.head(&path, &[]).await;
    assert_eq!(
        second.status(),
        http::StatusCode::GONE,
        "a ticket whose grant is gone is `410`, not a cached length"
    );

    let third = fixture.running.get(&path, &[]).await;
    assert_eq!(
        third.status(),
        http::StatusCode::NOT_FOUND,
        "and the removal was real: the next request finds no ticket at all"
    );
}

/// A refusal from an old generation must not delete a ticket that was
/// refreshed while it was in flight.
///
/// The refusal arrives after an `await`, and a second lookup can have replaced
/// the entry in between. Removing by key alone would delete a working grant on
/// the strength of a stale answer — and the caller who just refreshed it would
/// get `404` for content it holds.
///
/// Watched red with `scripts/break-once.sh` (row `HK24`) by replacing
/// `.is_none_or(|(live, _)| *live != generation)`
/// in `keylink-http/src/blob.rs` with `.is_none()`, which reports exactly
/// that harm: `left: 404, right: 200`.
#[tokio::test]
async fn stale_refusal_cannot_delete_refreshed_ticket() {
    let mut fixture = start_revocable(limits()).await;
    let path = fixture.running.resource("datasheet").await;

    // The refusal must still be IN FLIGHT when the refresh lands. The first
    // version of this row let it complete first, so nothing stale existed by
    // the time the ticket was replaced, and the break came back GREEN at
    // `exit=0`. The holder stalls before it closes now.
    fixture.hold();
    fixture.revoke();
    let address = fixture.running.address;
    let refusal = tokio::spawn({
        let path = path.clone();
        async move {
            reqwest::Client::new()
                .head(format!("http://{address}{path}"))
                .send()
                .await
                .expect("the refused request completes")
                .status()
        }
    });
    assert_eq!(
        fixture.seen().await,
        Seen::Refusing,
        "the holder is about to refuse, and is waiting"
    );

    // A fresh lookup while that refusal is parked: generation 2.
    fixture
        .refusing
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let again = fixture.running.resource("datasheet").await;
    assert_eq!(again, path, "the same content has the same address");

    fixture.release();
    assert_eq!(
        refusal.await.expect("the refused task finishes"),
        http::StatusCode::GONE,
        "the in-flight request still sees its own refusal"
    );

    let response = fixture.running.get(&path, &[]).await;
    assert_eq!(
        response.status(),
        http::StatusCode::OK,
        "and a refreshed grant outlives a refusal that named the one before it"
    );
    assert_eq!(
        response.bytes().await.expect("a body").len(),
        SPANNING,
        "control: it really serves the bytes"
    );
}

/// A download in progress holds no lock that a second request needs.
///
/// The ticket table is a `Mutex`, and the byte routes await a QUIC transfer
/// after reading it. Holding the guard across that await would make one slow
/// download block every other caller of every other resource — the failure
/// that is invisible until the first large file.
///
/// NO ANCHOR, and the reason is that the guard is structural rather than a
/// line. `Tickets::get` clones and returns, so no guard survives the call,
/// and the workspace denies `clippy::await_holding_lock`, which makes
/// reintroducing one a compile error rather than a behaviour to break. This
/// row is the liveness proof beside that lint.
#[tokio::test]
async fn slow_blob_download_does_not_hold_ticket_lock() {
    let mut fixture = start_revocable(limits()).await;
    let path = fixture.running.resource("datasheet").await;

    // The holder stalls the next data answer, so a download starts and does
    // not finish.
    fixture.hold();
    let address = fixture.running.address;
    let stalled = tokio::spawn({
        let path = path.clone();
        async move {
            reqwest::Client::new()
                .get(format!("http://{address}{path}"))
                .send()
                .await
                .expect("the stalled request completes eventually")
                .bytes()
                .await
                .expect("and its body arrives")
                .len()
        }
    });
    fixture.opened().await;

    // A second request, for a different resource, while the first is stuck.
    let second = fixture.running.resource("tiny").await;
    let response = fixture.running.get(&second, &[]).await;
    assert_eq!(
        response.status(),
        http::StatusCode::OK,
        "a second caller is served while the first is mid-transfer"
    );
    assert_eq!(response.bytes().await.expect("a body").len(), 9);

    fixture.release();
    assert_eq!(
        stalled.await.expect("the stalled task finishes"),
        SPANNING,
        "and the first download completes once the holder is let go"
    );
}

/// A client that disconnects mid-download loses its reader and keeps its
/// ticket.
///
/// Cancelling a transfer must not cost the caller the resource: a resumable
/// download is a range request against the same ticket, and a gateway that
/// dropped the entry on disconnect would turn every interrupted download into
/// a new lookup.
///
/// NO ANCHOR: the guard here is the ABSENCE of a removal on the disconnect
/// path, and a break can only add code, not change a line. This is a
/// characterization row, and it fails the moment somebody adds the cleanup
/// that looks tidy.
#[tokio::test]
async fn blob_disconnect_drops_reader_but_keeps_ticket_for_range_resume() {
    let mut fixture = start_revocable(limits()).await;
    let path = fixture.running.resource("datasheet").await;

    fixture.hold();
    let address = fixture.running.address;
    let disconnect = tokio::spawn({
        let path = path.clone();
        async move {
            // A raw socket, closed without reading the body, is the honest
            // shape of a client that went away. `reqwest` would drain first.
            let mut socket = tokio::net::TcpStream::connect(address)
                .await
                .expect("connects");
            let request =
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            tokio::io::AsyncWriteExt::write_all(&mut socket, request.as_bytes())
                .await
                .expect("writes the request");
            socket
        }
    });
    let socket = disconnect.await.expect("the raw request is sent");
    fixture.opened().await;
    drop(socket);
    fixture.release();

    // The ticket must still be there, and a RANGE must resume against it.
    let resumed = fixture
        .running
        .get(&path, &[(http::header::RANGE, "bytes=40000-")])
        .await;
    assert_eq!(
        resumed.status(),
        http::StatusCode::PARTIAL_CONTENT,
        "the ticket outlived the disconnect"
    );
    assert_eq!(
        header(&resumed, http::header::CONTENT_RANGE),
        Some("bytes 40000-69999/70000")
    );
    assert_eq!(
        resumed.bytes().await.expect("a body").as_ref(),
        &fixture.running.content[40_000..]
    );
}
