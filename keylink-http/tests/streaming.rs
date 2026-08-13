#![allow(clippy::expect_used)]

use axum::{middleware, response::Response, Router};
use http::Request;
use iroh::{endpoint::presets, Endpoint};
use keylink::{Allowlist, Context, Node};
use keylink_http::{
    Access, AccessError, Addressing, Call, Challenge, Gateway, HttpSurface, Principal,
    StreamingLimits,
};

keylink::service! {
    service Streaming;
    send Upload(prefix: u64) -> u64;
}

/// What the upload handler tells the test, in the order it happens.
///
/// `Chunk` is what lets a test act only once the serving side provably holds
/// the request. Without it a source that fails immediately can reset the stream
/// before the peer has read the header off it, and then there is no handler to
/// report anything at all.
#[derive(Debug)]
enum Progress {
    Chunk,
    Done(Option<u64>, Result<Vec<u8>, keylink::Error>),
}

#[derive(Clone)]
struct Handler {
    seen: tokio::sync::mpsc::UnboundedSender<Progress>,
}

impl streaming::Handler for Handler {
    async fn upload(&self, _cx: &Context, prefix: u64, mut body: keylink::BodyReader) -> u64 {
        let declared = body.declared_len();
        let mut received = Vec::new();
        let result = loop {
            match body.next_chunk().await {
                Ok(Some(chunk)) => {
                    received.extend_from_slice(&chunk);
                    self.send(Progress::Chunk);
                }
                Ok(None) => break Ok(received),
                Err(error) => break Err(error),
            }
        };
        self.send(Progress::Done(declared, result));
        prefix + 4
    }
}

impl Handler {
    fn send(&self, progress: Progress) {
        self.seen.send(progress).expect("receiver stays open");
    }
}

#[derive(Clone)]
struct Allow;

impl Access for Allow {
    async fn authorize(&self, _call: Call<'_>) -> Result<(), AccessError> {
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
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    _server: keylink::Server,
    _nodes: Vec<Node>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The serving half every test shares: a real Keylink service on a real
/// endpoint, and the client the gateway will call it through.
struct Backend {
    server: keylink::Server,
    nodes: Vec<Node>,
    client: streaming::Client,
    seen: tokio::sync::mpsc::UnboundedReceiver<Progress>,
}

async fn backend() -> Backend {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let (seen_tx, seen) = tokio::sync::mpsc::unbounded_channel();
    let handler = Handler { seen: seen_tx };
    let server = keylink::Server::builder(&server_node)
        .service(streaming::service(handler, Allowlist::anyone()).build())
        .spawn()
        .expect("service starts");
    let client = streaming::Client::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("client connects");
    Backend {
        server,
        nodes: vec![server_node, client_node],
        client,
        seen,
    }
}

fn surface(limits: StreamingLimits) -> HttpSurface {
    HttpSurface::new("/v1", Addressing::Singleton)
        .expect("valid surface")
        .with_streaming(limits)
}

fn challenge() -> Challenge {
    Challenge::new(http::HeaderValue::from_static("Bearer realm=keylink")).expect("valid challenge")
}

async fn serve(gateway: Router<()>, server: keylink::Server, nodes: Vec<Node>) -> Running {
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
        _server: server,
        _nodes: nodes,
    }
}

async fn start(
    limits: StreamingLimits,
) -> (Running, tokio::sync::mpsc::UnboundedReceiver<Progress>) {
    let backend = backend().await;
    let gateway = Gateway::<streaming::External, _>::new(backend.client, Allow, challenge())
        .router::<()>(surface(limits))
        .expect("surface builds");
    (
        serve(gateway, backend.server, backend.nodes).await,
        backend.seen,
    )
}

/// The same gateway with one addition: the invocation future reports its own
/// drop, which is the only way to tell cancellation from a call still running.
async fn start_tracked(
    limits: StreamingLimits,
    dropped: tokio::sync::watch::Sender<bool>,
) -> (Running, tokio::sync::mpsc::UnboundedReceiver<Progress>) {
    let backend = backend().await;
    let gateway = Gateway::<Tracked, _>::new(
        TrackedClient {
            inner: backend.client,
            dropped,
        },
        Allow,
        challenge(),
    )
    .router::<()>(surface(limits))
    .expect("surface builds");
    (
        serve(gateway, backend.server, backend.nodes).await,
        backend.seen,
    )
}

#[derive(Clone)]
struct TrackedClient {
    inner: streaming::Client,
    dropped: tokio::sync::watch::Sender<bool>,
}

struct Tracked;

impl keylink::ExternalService for Tracked {
    const SERVICE: &'static str = <streaming::External as keylink::ExternalService>::SERVICE;
    const METHODS: &'static [keylink::ExternalMethod] =
        <streaming::External as keylink::ExternalService>::METHODS;
    type Client = TrackedClient;

    fn invoke<C: keylink::ServiceCodec>(
        client: &Self::Client,
        method: &str,
        arguments: &[u8],
        call: keylink::ExternalCall,
        codec: C,
    ) -> keylink::ExternalFuture<C> {
        let future = streaming::External::invoke(&client.inner, method, arguments, call, codec);
        let dropped = client.dropped.clone();
        Box::pin(async move {
            struct SignalDrop(tokio::sync::watch::Sender<bool>);
            impl Drop for SignalDrop {
                fn drop(&mut self) {
                    let _ = self.0.send(true);
                }
            }
            let _signal = SignalDrop(dropped);
            future.await
        })
    }
}

#[tokio::test]
async fn send_streams_declared_length_and_exact_result() {
    let (running, mut seen) = start(StreamingLimits::new(32, 4, 4, 1).expect("limits")).await;
    let response = reqwest::Client::new()
        .post(format!("http://{}/v1/send/Upload", running.address))
        .header("content-type", "application/octet-stream")
        .header("keylink-arguments", arguments())
        .body("body")
        .send()
        .await
        .expect("HTTP response");

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "7");
    let (declared, result) = outcome(&mut seen).await;
    assert_eq!(declared, Some(4));
    assert_eq!(result.expect("body reaches handler"), b"body");
}

/// The byte past the limit is refused, and — the part that is not obvious — the
/// serving side is told, rather than being left holding four bytes it believes
/// are the whole upload.
///
/// Watched red with `scripts/break-once.sh` against the guard row `HJ10` names
/// in `keylink/src/body.rs`, the same one-word `is_none`/`is_some` edit. The
/// literal is deliberately not requoted here: it is registered once, and a
/// second copy buys nothing while another row already depends on its count. The
/// exact test terminated with `Ok([98, 111, 100, 121])` — `b"body"`, the
/// truncated upload arriving as a complete one.
#[tokio::test]
async fn send_unknown_length_aborts_at_limit_plus_one() {
    let (running, mut seen) = start(StreamingLimits::new(32, 4, 4, 1).expect("limits")).await;
    let (release, released) = tokio::sync::oneshot::channel();

    let requesting = upload(&running, gated_upload(released, Some(b"!")));
    let watching = release_after_first_chunk(&mut seen, release);
    let (response, (declared, result)) = tokio::join!(requesting, watching);

    assert_eq!(
        response.expect("HTTP response").status(),
        http::StatusCode::BAD_GATEWAY
    );
    assert_eq!(declared, None);
    assert!(
        result.is_err(),
        "the handler must see a body error, not a short upload: {result:?}"
    );
}

/// The same shape as the overflow case with the ceiling out of the way, so what
/// it proves is the source failing rather than the limit firing.
///
/// The HTTP result is deliberately only checked for *not succeeding*. Here the
/// failure starts in the client's own body, so whether the gateway's `502` wins
/// the race to the socket against the client abandoning the request is a
/// property of neither side's guarantee. What the row is about is the far end.
#[tokio::test]
async fn send_body_error_reaches_keylink_reader() {
    let (running, mut seen) = start(StreamingLimits::new(32, 64, 64, 1).expect("limits")).await;
    let (release, released) = tokio::sync::oneshot::channel();

    let requesting = upload(&running, gated_upload(released, None));
    let watching = release_after_first_chunk(&mut seen, release);
    let (response, (declared, result)) = tokio::join!(requesting, watching);

    let status = response.map(|response| response.status());
    assert!(
        !matches!(status, Ok(code) if code.is_success()),
        "an upload whose source failed is never a success: {status:?}"
    );
    assert_eq!(declared, None);
    assert!(
        result.is_err(),
        "a failed upload source must reach the handler as an error: {result:?}"
    );
}

/// The ceiling is inclusive: an upload of exactly the configured size succeeds.
/// The neighbouring case one byte further is
/// [`send_unknown_length_aborts_at_limit_plus_one`], and a limit that refused
/// both would pass a test that only checked the refusal.
#[tokio::test]
async fn send_streams_unknown_length_to_inclusive_limit() {
    let (running, mut seen) = start(StreamingLimits::new(32, 4, 4, 1).expect("limits")).await;
    let chunks = n0_future::stream::iter([
        Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"bo")),
        Ok(bytes::Bytes::from_static(b"dy")),
    ]);

    let response = upload(&running, chunks).await.expect("HTTP response");

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "7");
    let (declared, result) = outcome(&mut seen).await;
    assert_eq!(declared, None, "a chunked upload declares no length");
    assert_eq!(result.expect("body reaches handler"), b"body");
}

/// Nothing outlives the request. When the HTTP client vanishes mid-upload, the
/// Keylink invocation the adapter owns is dropped with it — no detached pump
/// keeps uploading to a peer on behalf of a caller that has gone.
#[tokio::test]
async fn send_disconnect_drops_invocation_and_reader() {
    use tokio::io::AsyncWriteExt as _;

    let (dropped_tx, mut dropped_rx) = tokio::sync::watch::channel(false);
    let (running, mut seen) = start_tracked(
        StreamingLimits::new(32, 64, 64, 1).expect("limits"),
        dropped_tx,
    )
    .await;

    let mut socket = tokio::net::TcpStream::connect(running.address)
        .await
        .expect("raw client connects");
    let head = format!(
        "POST /v1/send/Upload HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/octet-stream\r\n\
         Keylink-Arguments: {}\r\n\
         Transfer-Encoding: chunked\r\n\r\n\
         4\r\nbody\r\n",
        arguments()
    );
    socket
        .write_all(head.as_bytes())
        .await
        .expect("request head and one chunk write");

    // The invocation is running and the peer holds bytes. Only now is a
    // disconnect a disconnect rather than a request that never started.
    await_chunk(&mut seen).await;
    drop(socket);

    dropped_rx
        .changed()
        .await
        .expect("the invocation drop is reported");
    assert!(*dropped_rx.borrow());
}

/// The `Keylink-Arguments` value every send test uses, encoded rather than
/// pasted so the header says what it carries: `{"prefix":3}`.
fn arguments() -> String {
    use base64::Engine as _;

    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"prefix":3}"#)
}

/// One chunked `POST /v1/send/Upload`.
fn upload<S>(
    running: &Running,
    chunks: S,
) -> impl std::future::Future<Output = reqwest::Result<reqwest::Response>>
where
    S: n0_future::Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static,
{
    reqwest::Client::new()
        .post(format!("http://{}/v1/send/Upload", running.address))
        .header("content-type", "application/octet-stream")
        .header("keylink-arguments", arguments())
        .body(reqwest::Body::wrap_stream(chunks))
        .send()
}

/// An upload that sends `body`, then waits, then sends one last item — `Some`
/// bytes, or the source failing.
///
/// The wait is the whole point. A source that ends in the same poll that wrote
/// the request header resets the stream before the peer has read that header
/// off it, and a reset discards whatever has not been read. There would then be
/// no handler to observe anything, and the test would wait forever for a report
/// that no longer has an author.
fn gated_upload(
    release: tokio::sync::oneshot::Receiver<()>,
    last: Option<&'static [u8]>,
) -> impl n0_future::Stream<Item = std::io::Result<bytes::Bytes>> {
    enum Step {
        Body(tokio::sync::oneshot::Receiver<()>),
        Last(tokio::sync::oneshot::Receiver<()>),
        Exhausted,
    }

    n0_future::stream::unfold((Step::Body(release), last), |(step, last)| async move {
        match step {
            Step::Body(gate) => Some((
                Ok(bytes::Bytes::from_static(b"body")),
                (Step::Last(gate), last),
            )),
            Step::Last(gate) => {
                gate.await.expect("the test releases the last item");
                let item = match last {
                    Some(bytes) => Ok(bytes::Bytes::from_static(bytes)),
                    None => Err(std::io::Error::other("source failed")),
                };
                Some((item, (Step::Exhausted, last)))
            }
            Step::Exhausted => None,
        }
    })
}

/// Wait until the handler holds the first chunk, release the source, then take
/// the handler's outcome. Three events, no clock.
async fn release_after_first_chunk(
    seen: &mut tokio::sync::mpsc::UnboundedReceiver<Progress>,
    release: tokio::sync::oneshot::Sender<()>,
) -> (Option<u64>, Result<Vec<u8>, keylink::Error>) {
    await_chunk(seen).await;
    release.send(()).expect("the source is still waiting");
    outcome(seen).await
}

/// Wait until the handler holds body bytes.
async fn await_chunk(seen: &mut tokio::sync::mpsc::UnboundedReceiver<Progress>) {
    match seen.recv().await {
        Some(Progress::Chunk) => {}
        other => panic!("the handler reads a chunk, got {other:?}"),
    }
}

/// Wait for the handler's one terminal report, skipping its progress.
async fn outcome(
    seen: &mut tokio::sync::mpsc::UnboundedReceiver<Progress>,
) -> (Option<u64>, Result<Vec<u8>, keylink::Error>) {
    loop {
        match seen.recv().await {
            Some(Progress::Done(declared, result)) => return (declared, result),
            Some(Progress::Chunk) => continue,
            None => panic!("the handler reports one body outcome"),
        }
    }
}
