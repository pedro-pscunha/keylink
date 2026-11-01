//! End-to-end behaviour of a generated service over a real QUIC connection.
//!
//! Everything here runs against two iroh endpoints on the loopback interface
//! with no relay and no discovery, so the transport is genuine and the tests
//! need no network.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::prelude::*;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use keylink::{
    Allowlist, Body, BodyReader, Context as KeylinkContext, Error, IdempotencyKey, Keyable, Node,
    Server as KeylinkServer,
};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, ReadBuf};

// ────────────────────────────────────────────────────────────── the service

#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub struct Meta {
    pub bytes: u64,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub enum Received {
    Stored { bytes: u64, checksum: u64 },
    Failed { reason: String },
}

keylink::service! {
    service Probe;

    rpc  Ping() -> String;
    rpc  Slow(ms: u64) -> u64;

    recv Download(len: u64) -> Meta;
    recv Mislabelled(declare: u64, actually: u64) -> Meta;

    send Upload(name: String) -> Received;
}

/// The serving side. `rendezvous` is `Some` for exactly one test — the one that
/// proves calls overlap — and `None` everywhere else, where `slow` is a delay.
#[derive(Default)]
struct Server {
    rendezvous: Option<Rendezvous>,
    upload_report: Option<tokio::sync::mpsc::UnboundedSender<Progress>>,
}

/// What the upload handler tells a watching test, in the order it happens.
///
/// `Chunk` exists so a test can make something occur *after* the receiver
/// provably holds the request header and some body, rather than racing it.
#[derive(Debug)]
enum Progress {
    Chunk,
    Done(Received),
}

impl probe::Handler for Server {
    async fn ping(&self, _cx: &KeylinkContext) -> String {
        "pong".to_string()
    }

    async fn slow(&self, _cx: &KeylinkContext, ms: u64) -> u64 {
        match &self.rendezvous {
            Some(rendezvous) => rendezvous.hold(ms).await,
            None => tokio::time::sleep(Duration::from_millis(ms)).await,
        }
        ms
    }

    async fn download(&self, _cx: &KeylinkContext, len: u64) -> (Meta, Body) {
        let meta = Meta {
            bytes: len,
            checksum: Pattern::checksum(len),
        };
        (meta, Body::from_reader(Pattern::new(len), Some(len)))
    }

    /// Declares one length and sends another, which is how a real short read
    /// looks from the receiving end.
    async fn mislabelled(&self, _cx: &KeylinkContext, declare: u64, actually: u64) -> (Meta, Body) {
        let meta = Meta {
            bytes: declare,
            checksum: 0,
        };
        (
            meta,
            Body::from_reader(Pattern::new(actually), Some(declare)),
        )
    }

    async fn upload(&self, _cx: &KeylinkContext, _name: String, mut body: BodyReader) -> Received {
        let mut checksum = Fnv1a::new();
        let mut bytes = 0u64;
        let outcome = loop {
            match body.next_chunk().await {
                Ok(Some(chunk)) => {
                    checksum.update(&chunk);
                    bytes += chunk.len() as u64;
                    self.report(Progress::Chunk);
                }
                Ok(None) => {
                    break Received::Stored {
                        bytes,
                        checksum: checksum.finish(),
                    }
                }
                Err(error) => {
                    break Received::Failed {
                        reason: error.to_string(),
                    }
                }
            }
        };
        self.report(Progress::Done(outcome.clone()));
        outcome
    }
}

impl Server {
    /// Silent unless a test asked to watch, which is every test but one.
    fn report(&self, progress: Progress) {
        if let Some(report) = &self.upload_report {
            report.send(progress).expect("test receiver stays open");
        }
    }
}

// ────────────────────────────────────────────────────────────────── fixture

mod common;
use common::{local_node, rendezvous, Rendezvous, NO_PROGRESS};

/// A running server and a client connected to it. Both nodes are held so the
/// endpoints outlive the test body.
struct Fixture {
    client: probe::Client,
    _server: KeylinkServer,
    _nodes: [Node; 2],
}

async fn start() -> Fixture {
    start_with(Server::default(), Allowlist::anyone()).await
}

async fn start_with(handler: Server, allow: Allowlist) -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let router = probe::serve(&server, handler, allow).expect("one service, no duplicate ALPN");
    let client = probe::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the server");

    Fixture {
        client,
        _server: router,
        _nodes: [server, client_node],
    }
}

// ───────────────────────────────────────────────────────────── unary calls

#[tokio::test]
async fn a_unary_call_round_trips() {
    let fixture = start().await;
    assert_eq!(fixture.client.ping().await.unwrap(), "pong");
}

/// iroh proves who the peer is. It does not decide whether they are welcome,
/// and an allowlist that does not name us must close the door.
#[tokio::test]
async fn a_peer_outside_the_allowlist_is_refused() {
    let stranger = local_node().await;
    let fixture = start_with(Server::default(), Allowlist::only([stranger.id()])).await;

    let error = fixture.client.ping().await.unwrap_err();
    assert!(
        matches!(error, Error::Transport(_)),
        "expected a transport failure, got {error:?}"
    );
}

/// Calls this test drives at once, and arrivals the rendezvous waits for.
const CONCURRENT_CALLS: usize = 8;

/// The headline property of one stream per call, asserted as overlap rather than
/// as elapsed time.
///
/// Every handler announces itself on entry and none is let go until all eight
/// have, so a server that queued the calls behind each other could never fill
/// the rendezvous. What is asserted is therefore *not serialized* rather than
/// *fast*, and a loaded machine can only make it take longer — never make it
/// pass by accident.
///
/// The wall-clock version asserted that eight 150 ms calls finished in under
/// 600 ms, which is a claim about the machine as much as about the code, and it
/// flaked under a concurrent build. **The number went with it, and nothing
/// replaced it.**
///
/// It briefly had a replacement — `eight_overlapping_delays_cost_one_delay`,
/// eight `tokio::time::sleep`s in a `JoinSet` under a paused clock — and that
/// test was deleted rather than kept, because no edit to this crate could
/// redden it. It named no service, no dispatch and no transport: a `Mutex`
/// around `serve_one` left it green, and only editing its own delay helper
/// turned it red. What it actually asserted is that tokio's scheduler runs
/// spawned tasks concurrently, which is tokio's guarantee to test, not ours.
///
/// The deleted assertion was a claim about the **server**, and this test is
/// where that claim lives now — held as overlap rather than as elapsed time,
/// and reddenable by serializing dispatch, which is the property in question.
/// A number nothing can be wrong about buys nothing over that.
#[tokio::test]
async fn concurrent_calls_do_not_queue_behind_each_other() {
    let (rendezvous, mut arrivals) = rendezvous();
    let fixture = start_with(
        Server {
            rendezvous: Some(rendezvous),
            ..Server::default()
        },
        Allowlist::anyone(),
    )
    .await;
    let client = &fixture.client;

    // Distinct arguments, so a reply is evidence of which call it answers. A
    // held handler ignores the number otherwise.
    let calls = async {
        tokio::join!(
            client.slow(0),
            client.slow(1),
            client.slow(2),
            client.slow(3),
            client.slow(4),
            client.slow(5),
            client.slow(6),
            client.slow(7),
        )
    };

    let mut inside: Vec<u64> = Vec::with_capacity(CONCURRENT_CALLS);
    let overlap = async {
        while inside.len() < CONCURRENT_CALLS {
            inside.push(
                arrivals
                    .entered
                    .recv()
                    .await
                    .expect("the serving side holds the sender"),
            );
        }
        arrivals
            .release
            .send(true)
            .expect("the handlers are still parked");
    };

    // Its own statement, so the joined future is dropped here and `inside` is
    // readable in the failure arm below.
    let outcome = tokio::time::timeout(NO_PROGRESS, async { tokio::join!(calls, overlap) }).await;
    let Ok((replies, ())) = outcome else {
        panic!(
            "only {} of {CONCURRENT_CALLS} handlers were inside the server together",
            inside.len()
        );
    };

    inside.sort_unstable();
    assert_eq!(
        inside,
        (0..CONCURRENT_CALLS as u64).collect::<Vec<_>>(),
        "each of the {CONCURRENT_CALLS} calls reached a handler exactly once",
    );

    for (nth, reply) in [
        replies.0, replies.1, replies.2, replies.3, replies.4, replies.5, replies.6, replies.7,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            reply.unwrap(),
            nth as u64,
            "reply {nth} must answer its own call",
        );
    }
}

// ──────────────────────────────────────────────────────────────── streaming

/// A body is not a frame: the 1 MiB header cap does not apply to it, because it
/// is never held in memory.
#[tokio::test]
async fn a_body_far_larger_than_the_header_cap_transfers_intact() {
    const LEN: u64 = 4 * 1024 * 1024;
    assert!(LEN > u64::from(keylink::MAX_HEADER));

    let fixture = start().await;
    let (meta, mut body) = fixture.client.download(LEN).await.unwrap();

    assert_eq!(body.declared_len(), Some(LEN));

    let mut checksum = Fnv1a::new();
    let mut received = 0u64;
    while let Some(chunk) = body.next_chunk().await.unwrap() {
        checksum.update(&chunk);
        received += chunk.len() as u64;
    }

    assert_eq!(received, LEN);
    assert_eq!(checksum.finish(), meta.checksum);
}

/// Yields one chunk, then fails — but not before `release` fires.
///
/// The gate is what makes the test deterministic rather than a race. A stream
/// reset discards whatever the peer has not read yet, request header included,
/// so a source that fails in the same poll that wrote the header would sometimes
/// leave the peer with nothing to dispatch and no outcome to report.
struct FailsWhenReleased {
    yielded: bool,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl AsyncRead for FailsWhenReleased {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.yielded {
            self.yielded = true;
            buf.put_slice(b"body");
            return Poll::Ready(Ok(()));
        }
        match Pin::new(&mut self.release).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Err(io::Error::other("source failed"))),
        }
    }
}

/// A body whose length was never declared ends at the stream's end, so the one
/// thing the receiver must never conclude from a failed source is "complete".
///
/// There is no length to check it against and no sentinel frame to miss: only
/// the difference between finishing the stream and resetting it carries this.
///
/// Watched red with `scripts/break-once.sh` by replacing
/// `        if drained.is_err() && declared_len.is_none() {`
/// with `is_some`, so the reset fires for the one case that does not need it and
/// never for the case that does. The exact test terminated with
/// `Stored { bytes: 4, checksum: 14793735007222968981 }` — the handler counting a
/// failed upload as a whole one.
#[tokio::test]
async fn an_unknown_length_source_error_resets_the_receivers_body() {
    let (report, mut progress) = tokio::sync::mpsc::unbounded_channel();
    let fixture = start_with(
        Server {
            upload_report: Some(report),
            ..Server::default()
        },
        Allowlist::anyone(),
    )
    .await;

    let (release, released) = tokio::sync::oneshot::channel();
    let source = FailsWhenReleased {
        yielded: false,
        release: released,
    };

    let watching = async {
        // The handler holds the header and the first chunk. What follows cannot
        // be mistaken for a request that never arrived.
        assert!(
            matches!(progress.recv().await, Some(Progress::Chunk)),
            "the handler reads the first chunk before the source fails"
        );
        release.send(()).expect("the source is still waiting");
        // The first read can return the body in more than one chunk, whatever
        // the transport does with four bytes under load; every `Chunk` before
        // the outcome is the same fact again.
        loop {
            match progress.recv().await {
                Some(Progress::Chunk) => {}
                Some(Progress::Done(received)) => break received,
                None => panic!("the handler reports one body outcome, got none"),
            }
        }
    };
    let calling = fixture
        .client
        .upload("broken".to_string(), Body::from_reader(source, None));

    let (result, received) = tokio::join!(calling, watching);

    let error = result.expect_err("the caller preserves its local source error");
    assert!(matches!(error, Error::Io(_)), "got {error:?}");
    assert!(
        matches!(received, Received::Failed { .. }),
        "the receiver must not accept a failed source as clean EOF: {received:?}"
    );
}

/// The receiver knows the size before a byte moves, which is what makes
/// refusing an oversized payload possible instead of theoretical.
#[tokio::test]
async fn the_declared_length_arrives_before_the_body() {
    let fixture = start().await;
    let (_meta, body) = fixture.client.download(9_999).await.unwrap();
    assert_eq!(body.declared_len(), Some(9_999));
    assert_eq!(body.received(), 0);
}

/// The streaming counterpart of a truncated frame. A file that stops early must
/// never be mistaken for a complete one.
#[tokio::test]
async fn a_body_shorter_than_declared_is_an_error_not_a_short_file() {
    let fixture = start().await;
    let (_meta, body) = fixture.client.mislabelled(100_000, 4_096).await.unwrap();

    let error = body.copy_to(&mut Vec::new()).await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::BodyTruncated {
                declared: 100_000,
                received: 4_096
            }
        ),
        "got {error:?}"
    );
}

/// And the other direction: a sender that overruns its own declaration is
/// stopped rather than trusted.
#[tokio::test]
async fn a_body_longer_than_declared_is_an_error() {
    let fixture = start().await;
    let (_meta, body) = fixture.client.mislabelled(1_024, 512_000).await.unwrap();

    let error = body.copy_to(&mut Vec::new()).await.unwrap_err();
    assert!(
        matches!(error, Error::BodyOverrun { declared: 1_024 }),
        "got {error:?}"
    );
}

/// Collecting into memory is the one path that can exhaust it, so the cap is a
/// required argument rather than a default nobody reads.
#[tokio::test]
async fn read_to_bytes_refuses_a_body_over_the_callers_cap() {
    let fixture = start().await;
    let (_meta, body) = fixture.client.download(1_000_000).await.unwrap();

    let error = body.read_to_bytes(1_024).await.unwrap_err();
    assert!(
        matches!(error, Error::BodyTooLarge { cap: 1_024 }),
        "got {error:?}"
    );
}

/// A real file leaves the disk on one side and arrives byte-identical on the
/// other, without either side holding it.
/// `fs`, for `Body::from_file` and nothing else in this test. Gated at the test
/// rather than at the file, so `--no-default-features` still builds the rest of
/// `protocol.rs` — including both ALPN pins, which are the two assertions in this
/// file that must never be skipped by a feature flag.
#[cfg(feature = "fs")]
#[tokio::test]
async fn a_file_streams_up_from_disk_intact() {
    const LEN: usize = 3 * 1024 * 1024;

    let contents: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
    let path = std::env::temp_dir().join("keylink-test-upload.bin");
    tokio::fs::write(&path, &contents).await.unwrap();

    let mut expected = Fnv1a::new();
    expected.update(&contents);

    let fixture = start().await;
    let outcome = fixture
        .client
        .upload(
            "payload.bin".to_string(),
            Body::from_file(&path).await.unwrap(),
        )
        .await
        .unwrap();

    match outcome {
        Received::Stored { bytes, checksum } => {
            assert_eq!(bytes, LEN as u64);
            assert_eq!(checksum, expected.finish());
        }
        Received::Failed { reason } => panic!("upload failed: {reason}"),
    }
}

/// Streaming and unary calls share a connection without interfering: the
/// download holds one stream open while ordinary calls come and go on others.
#[tokio::test]
async fn a_download_in_flight_does_not_block_other_calls() {
    let fixture = start().await;
    let (_meta, mut body) = fixture.client.download(2 * 1024 * 1024).await.unwrap();

    // One chunk in hand, the rest still on the wire.
    assert!(body.next_chunk().await.unwrap().is_some());

    assert_eq!(fixture.client.ping().await.unwrap(), "pong");

    let rest = body.drain().await.unwrap();
    assert!(rest > 0);
}

// ─────────────────────────────────────────────────────────── test utilities

/// FNV-1a, computed incrementally so a test never has to hold a payload either.
#[derive(Debug, Clone, Copy)]
struct Fnv1a(u64);

impl Fnv1a {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn update(&mut self, bytes: &[u8]) {
        self.0 = bytes.iter().fold(self.0, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    }

    fn finish(self) -> u64 {
        self.0
    }
}

/// A deterministic byte stream of a known length, generated as it is read.
/// Non-repeating over any 251-byte window, so dropped or duplicated bytes move
/// the checksum instead of cancelling out.
struct Pattern {
    offset: u64,
    len: u64,
}

impl Pattern {
    fn new(len: u64) -> Self {
        Self { offset: 0, len }
    }

    fn byte_at(offset: u64) -> u8 {
        (offset % 251) as u8
    }

    fn checksum(len: u64) -> u64 {
        let mut hash = Fnv1a::new();
        for offset in 0..len {
            hash.update(&[Self::byte_at(offset)]);
        }
        hash.finish()
    }
}

impl AsyncRead for Pattern {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let take = (buf.remaining() as u64).min(self.len - self.offset) as usize;
        for index in 0..take {
            buf.put_slice(&[Self::byte_at(self.offset + index as u64)]);
        }
        self.offset += take as u64;
        Poll::Ready(Ok(()))
    }
}

// ──────────────────────────────────────────────── several services, one node

keylink::service! {
    service Telemetry;

    rpc Echo(text: String) -> String;
}

struct Tele;

impl telemetry::Handler for Tele {
    async fn echo(&self, _cx: &KeylinkContext, text: String) -> String {
        text
    }
}

/// The regression test for a real defect.
///
/// `Router::spawn` calls `Endpoint::set_alpns`, which *overrides* the ALPN list
/// rather than adding to it. Building one router per service therefore left
/// only the last-registered service reachable, silently. Every service has to
/// go into a single router.
#[tokio::test]
async fn two_services_share_one_node_and_both_answer() {
    let server = local_node().await;
    let client_node = local_node().await;

    let _running = keylink::Server::builder(&server)
        .service(probe::service(Server::default(), Allowlist::anyone()).build())
        .service(telemetry::service(Tele, Allowlist::anyone()).build())
        .spawn()
        .expect("distinct ALPNs register cleanly");

    let addr = server.endpoint().addr();
    let probe_client = probe::Client::connect(&client_node, addr.clone())
        .await
        .expect("the first service is reachable");
    let tele_client = telemetry::Client::connect(&client_node, addr)
        .await
        .expect("the second service is reachable too - this is what regressed");

    assert_eq!(probe_client.ping().await.unwrap(), "pong");
    assert_eq!(
        tele_client.echo("olá".to_string()).await.unwrap(),
        "olá",
        "the service registered second must still answer"
    );
}

/// Two services on one ALPN is a configuration error, not a last-one-wins race:
/// the shadowed service would simply never answer.
#[tokio::test]
async fn registering_one_alpn_twice_is_refused() {
    let node = local_node().await;
    let error = keylink::Server::builder(&node)
        .service(probe::service(Server::default(), Allowlist::anyone()).build())
        .service(probe::service(Server::default(), Allowlist::anyone()).build())
        .spawn()
        .expect_err("a duplicate ALPN must not be accepted");

    assert!(
        matches!(error, Error::Config(_)),
        "expected a config error, got {error:?}"
    );
}

/// The derived-ALPN drift gate is keylink's headline safety property. If it
/// fires as a generic transport error, an operator reads "connection failed"
/// and goes looking at the network, when the real cause is two peers built from
/// different types.
#[tokio::test]
async fn a_peer_on_a_different_schema_is_reported_as_a_schema_mismatch() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = probe::serve(&server, Server::default(), Allowlist::anyone()).expect("serves");

    // Same node, same address, an ALPN that is not the one it answers.
    let err = keylink::RpcClient::<probe::Request, probe::Response>::connect(
        &client_node,
        server.endpoint().addr(),
        b"keylink/Probe/0000000000000000",
    )
    .await
    .expect_err("a peer on another contract must not connect");

    assert!(
        matches!(err, Error::SchemaMismatch { .. }),
        "expected a schema mismatch, got: {err:?}"
    );
}

// ─────────────────────────────────────────────────────────────────── layers

/// Refuses one method by name, to prove a layer can see the typed request and
/// say no without reaching the handler.
struct WriteGuard;

impl<Resp: Send + 'static> keylink::Middleware<probe::Request, Resp> for WriteGuard {
    fn handle(
        &self,
        cx: keylink::Context,
        req: probe::Request,
        body: BodyReader,
        next: keylink::Next<probe::Request, Resp>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = (keylink::Outcome<Resp>, Body)> + Send>>
    {
        Box::pin(async move {
            if cx.method() == "Slow" {
                return (
                    keylink::Outcome::Err(keylink::RemoteError::Unauthorized(
                        "Slow needs write access".into(),
                    )),
                    Body::empty(),
                );
            }
            next(cx, req, body).await
        })
    }
}

#[tokio::test]
async fn a_layer_can_refuse_one_method_and_pass_the_rest() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(
            probe::service(Server::default(), Allowlist::anyone())
                .layer(WriteGuard)
                .build(),
        )
        .spawn()
        .expect("serves");

    let client = probe::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("dials");

    // Untouched method still works.
    assert_eq!(client.ping().await.unwrap(), "pong");

    // Guarded method is refused, and the refusal is typed.
    let err = client
        .slow(1)
        .await
        .expect_err("the layer refuses this one");
    assert!(
        matches!(err, Error::Remote(keylink::RemoteError::Unauthorized(_))),
        "expected a typed refusal, got {err:?}"
    );

    // A refused call must not poison the connection.
    assert_eq!(client.ping().await.unwrap(), "pong");
}

/// Refuses a call that carried no idempotency key, to prove a layer closes the
/// half [`keylink::ServiceBuilder::keyable`] cannot: that policy decides which
/// methods *accept* a key and never which ones demand one.
///
/// The `cx.method()` test is in step with the service's own `Keyable` policy, and
/// it is load bearing rather than tidy. On a method outside that policy
/// `cx.call()` can never answer `Some` — a keyed call is refused `NotKeyable`
/// before the chain is entered and an unkeyed one arrives as `None` — so a layer
/// without the test refuses every call to every other method.
struct RequireKey;

impl<Resp: Send + 'static> keylink::Middleware<probe::Request, Resp> for RequireKey {
    fn handle(
        &self,
        cx: keylink::Context,
        req: probe::Request,
        body: BodyReader,
        next: keylink::Next<probe::Request, Resp>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = (keylink::Outcome<Resp>, Body)> + Send>>
    {
        Box::pin(async move {
            if cx.method() == "Ping" && cx.call().is_none() {
                return (
                    keylink::Outcome::Err(keylink::RemoteError::Unauthorized(
                        "Ping needs an idempotency key".into(),
                    )),
                    Body::empty(),
                );
            }
            next(cx, req, body).await
        })
    }
}

/// The layer above, over a real connection, on all three cases that matter:
/// the guarded method without a key, the guarded method with one, and a method
/// the policy leaves out.
///
/// Made red by `scripts/break-once.sh` replacing
/// `if cx.method() == "Ping" && cx.call().is_none() {` with
/// `if cx.call().is_none() {`, which is the shorter layer the caveat is about.
/// The search literal carries its twelve spaces of indentation, so this record's
/// unindented copy is not a second occurrence of it. Watched, under `cargo test`,
/// this test alone reddening:
/// `a method outside the policy: Remote(Unauthorized("Ping needs an idempotency
/// key"))`.
#[tokio::test]
async fn a_layer_can_require_an_idempotency_key() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(
            probe::service(Server::default(), Allowlist::anyone())
                .keyable(Keyable::Only(&["Ping"]))
                .layer(RequireKey)
                .build(),
        )
        .spawn()
        .expect("serves");

    let client = probe::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("dials");

    // The guarded method with no key: the layer sees the absence and refuses.
    let err = client
        .ping()
        .await
        .expect_err("the layer refuses a call carrying no key");
    assert!(
        matches!(err, Error::Remote(keylink::RemoteError::Unauthorized(_))),
        "expected a typed refusal, got {err:?}"
    );

    // The same method with a key reaches the handler, so the layer refuses an
    // absence rather than the method.
    let key = IdempotencyKey::new("ping-1").expect("a valid key");
    assert_eq!(
        client.with_idempotency(key).ping().await.unwrap(),
        "pong",
        "a call carrying a key must reach the handler"
    );

    // A method the policy leaves out passes unkeyed, which is what the
    // `cx.method()` test buys: `cx.call()` is permanently `None` here, so a layer
    // reading the absence alone would refuse this one forever.
    assert_eq!(
        client.slow(1).await.expect("a method outside the policy"),
        1,
        "expected a method outside the policy to pass unkeyed"
    );
}

#[tokio::test]
async fn a_timeout_layer_refuses_a_slow_handler() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(
            probe::service(Server::default(), Allowlist::anyone())
                .layer(keylink::Timeout::after(Duration::from_millis(50)))
                .build(),
        )
        .spawn()
        .expect("serves");

    let client = probe::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("dials");

    let err = client
        .slow(5_000)
        .await
        .expect_err("a handler over budget is refused");
    assert!(
        matches!(err, Error::Remote(keylink::RemoteError::TimedOut { .. })),
        "expected a timeout refusal, got {err:?}"
    );

    // The connection survives the timeout.
    assert_eq!(client.ping().await.unwrap(), "pong");
}

// ───────────────────────────────────────────────────── the synchronous twin

/// The whole point of the blocking API: a synchronous program talks to a
/// keylink service without writing one line of runtime plumbing.
///
/// Not a `#[tokio::test]` on purpose — this must run on a plain thread, which
/// is exactly the situation the blocking API exists for.
/// `blocking`, which supplies both `keylink::blocking::Node` and the generated
/// `probe::blocking::Client` this test dials with. Without it there is no
/// synchronous API for a test about the synchronous API to be about.
#[cfg(feature = "blocking")]
#[test]
fn a_synchronous_program_can_call_a_service() {
    // The server side still needs a runtime; it is a separate one, which is
    // also the realistic shape (server and client are different processes).
    let rt = tokio::runtime::Runtime::new().expect("a runtime for the server");
    let (addr, _guard) = rt.block_on(async {
        let server = local_node().await;
        let running =
            probe::serve(&server, Server::default(), Allowlist::anyone()).expect("serves");
        (server.endpoint().addr(), (server, running))
    });

    let node = keylink::blocking::Node::bind().expect("binds a blocking node");
    let client = probe::blocking::Client::connect(&node, addr).expect("dials");

    assert_eq!(client.ping().expect("a blocking call answers"), "pong");
    assert_eq!(client.slow(1).expect("and another"), 1);
    client.close().expect("closes cleanly");
}

// ─────────────────────────────────────────── foreign protocols on our node

/// Stands in for `iroh-blobs` or `iroh-gossip`: a protocol keylink did not
/// generate, with its own ALPN and its own rules.
#[derive(Debug, Clone)]
struct ForeignEcho;

impl iroh::protocol::ProtocolHandler for ForeignEcho {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> std::result::Result<(), iroh::protocol::AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        tokio::io::copy(&mut recv, &mut send).await?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

const FOREIGN_ALPN: &[u8] = b"not-keylink/echo/1";

/// `Server::router()` cannot register anything: iroh's `Router` accepts
/// protocols only through its builder, and `spawn` consumes it. So a foreign
/// protocol has to join before spawn, or the claim that keylink services can
/// share a node with iroh-blobs is false.
#[tokio::test]
async fn a_foreign_protocol_shares_the_node_with_a_keylink_service() {
    let server = local_node().await;
    let client_node = local_node().await;

    let _running = keylink::Server::builder(&server)
        .service(probe::service(Server::default(), Allowlist::anyone()).build())
        .service(keylink::Service::foreign(FOREIGN_ALPN, ForeignEcho))
        .spawn()
        .expect("a keylink service and a foreign protocol register together");

    let addr = server.endpoint().addr();

    // The keylink service still answers.
    let client = probe::Client::connect(&client_node, addr.clone())
        .await
        .expect("dials the keylink service");
    assert_eq!(client.ping().await.unwrap(), "pong");

    // And so does the foreign one, on its own ALPN.
    let conn = client_node
        .endpoint()
        .connect(addr, FOREIGN_ALPN)
        .await
        .expect("dials the foreign protocol on the same endpoint");
    let (mut send, mut recv) = conn.open_bi().await.expect("opens a stream");
    send.write_all("olá".as_bytes()).await.expect("writes");
    send.finish().expect("finishes");
    let echoed = recv.read_to_end(64).await.expect("reads the echo");
    assert_eq!(echoed, "olá".as_bytes(), "the foreign protocol answered");
}

/// iroh-blobs' own examples and module docs all write
/// `BlobsProtocol::new(&store, None)`, which disables the admission callback
/// entirely: every hash to every peer, and writes accepted. Mounting that
/// beside an allowlisted keylink service puts an unauthenticated protocol on a
/// closed node.
///
/// keylink means to mount blobs itself, with a policy. So mounting it by hand
/// must fail at startup, not merely be warned about in a doc comment.
#[tokio::test]
async fn a_reserved_alpn_cannot_be_mounted_by_hand() {
    let node = local_node().await;
    let error = keylink::Server::builder(&node)
        .service(keylink::Service::foreign(b"/iroh-bytes/4", ForeignEcho))
        .spawn()
        .expect_err("the iroh-blobs ALPN must be refused");

    assert!(
        matches!(error, Error::Config(_)),
        "expected a config error, got {error:?}"
    );
    assert!(
        error.to_string().contains("reserved"),
        "the message must say why: {error}"
    );
}

/// The guard must not overreach: an unrelated foreign protocol still mounts.
#[tokio::test]
async fn a_non_reserved_foreign_alpn_still_mounts() {
    let node = local_node().await;
    let _running = keylink::Server::builder(&node)
        .service(keylink::Service::foreign(FOREIGN_ALPN, ForeignEcho))
        .spawn()
        .expect("an ordinary foreign protocol is unaffected");
}

// ────────────────────────────── the hashed contract must equal the wire type

/// The shape the contract had before the fix: the bare response, not the
/// `Outcome` the wire actually carries.
#[derive(postcard_schema::Schema)]
#[allow(dead_code)]
struct DriftedContract {
    request: keylink::Envelope<probe::Request>,
    response: keylink::Envelope<probe::Response>,
}

/// The same drift on the request side, which arrived with the ledger query: the
/// bare request, not the `WireRequest` the wire carries.
#[derive(postcard_schema::Schema)]
#[allow(dead_code)]
struct RequestDriftedContract {
    request: keylink::Envelope<probe::Request>,
    response: keylink::Envelope<keylink::WireResponse<probe::Response>>,
    kinds: keylink::WireKinds,
}

/// The keyed twin of [`RequestDriftedContract`], and the shape the hand-written
/// actor contract actually had until Wave 1a.
///
/// `actor/remote.rs` declared `request: Envelope<ActorRequest>` while `serve_one`
/// read `WireRequest<Req>` off the frame. The generated path spelled it
/// correctly and this test refused the drift for it; the hand-written contract
/// evaded the test by not being generated, so the keyed ALPN was derived from a
/// type that was not what the keyed wire carried.
///
/// Pinned here so the hole cannot reopen through the generated keyed contract
/// the way it opened through the hand-written one.
#[derive(postcard_schema::Schema)]
#[allow(dead_code)]
struct KeyedRequestDriftedContract {
    request: keylink::Envelope<keylink::Addressed<probe::Request>>,
    response: keylink::Envelope<keylink::WireResponse<probe::Response>>,
    kinds: keylink::WireKinds,
}

/// The other keyed drift, and the one that would be invisible without a test:
/// the request arm forgetting the `Addressed` wrapper altogether, so a keyed
/// contract and a singleton contract hash the same request type.
#[derive(postcard_schema::Schema)]
#[allow(dead_code)]
struct KeyedUnaddressedContract {
    request: keylink::Envelope<keylink::WireRequest<probe::Request>>,
    response: keylink::Envelope<keylink::WireResponse<probe::Response>>,
    kinds: keylink::WireKinds,
}

/// The shape the keyed contract must have.
#[derive(postcard_schema::Schema)]
#[allow(dead_code)]
struct CorrectKeyedContract {
    request: keylink::Envelope<keylink::WireRequest<keylink::Addressed<probe::Request>>>,
    response: keylink::Envelope<keylink::WireResponse<probe::Response>>,
    kinds: keylink::WireKinds,
}

/// The shape it must have: the same two types the wire names.
#[derive(postcard_schema::Schema)]
#[allow(dead_code)]
struct CorrectContract {
    request: keylink::Envelope<keylink::WireRequest<probe::Request>>,
    response: keylink::Envelope<keylink::WireResponse<probe::Response>>,
    kinds: keylink::WireKinds,
}

/// keylink's whole safety story is that the ALPN hashes what the wire carries.
/// It did not: the contract hashed `Envelope<Response>` while the wire carried
/// `Envelope<Outcome<Response>>`, so adding a `RemoteError` variant did not move
/// the ALPN — and inserting one decoded a refusal as a DIFFERENT refusal, which
/// is the exact failure the scheme exists to prevent, inside the error channel.
///
/// This test fails if the contract is ever reverted to the bare response.
#[test]
fn the_alpn_hashes_the_response_type_the_wire_actually_carries() {
    // Must match what the macro builds: concat!(CARGO_PKG_NAME, "/", service).
    // The protocol name is part of the hash, by design.
    const P: &str = concat!(env!("CARGO_PKG_NAME"), "/Probe");

    let drifted = keylink::derive_alpn::<DriftedContract>(P);
    let request_drifted = keylink::derive_alpn::<RequestDriftedContract>(P);
    let correct = keylink::derive_alpn::<CorrectContract>(P);

    assert_ne!(
        drifted, correct,
        "wrapping the response in Outcome must change the hash, or the wrapper \
         is invisible to the gate"
    );
    assert_ne!(
        request_drifted, correct,
        "wrapping the request in WireRequest must change the hash too. A peer \
         that cannot receive a ledger query decodes one as a call, which is the \
         variant confusion this whole scheme exists to refuse"
    );

    let real = String::from_utf8_lossy(probe::alpn()).into_owned();
    let correct_hash = String::from_utf8_lossy(&correct)
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_owned();

    assert!(
        real.ends_with(&correct_hash),
        "the generated ALPN must hash the wire type.\n  generated: {real}\n  expected suffix: {correct_hash}"
    );
}

/// The same assertion for the KEYED contract, which is the one that was wrong.
///
/// Before Wave 1a there was one hand-written actor contract for every service on
/// a node, it declared `Envelope<ActorRequest>`, and `serve_one` read
/// `WireRequest<ActorRequest>`. Two builds whose `WireRequest` shape differed
/// could therefore agree on the actor ALPN and then disagree on the wire — the
/// precise failure the derived ALPN exists to prevent, evading the test above by
/// living outside the macro.
///
/// Three drifts are refused here rather than one. Dropping `WireRequest` is the
/// original defect. Dropping `Addressed` is the new one this shape makes
/// possible, and it is the more dangerous of the two: it would make the keyed
/// contract hash the *singleton's* request type, so a peer holding a singleton
/// client could complete a handshake against an actor system and then send a
/// message with no actor id in it.
#[test]
fn the_keyed_alpn_hashes_the_keyed_wire_type() {
    // Must match what the macro builds: concat!(CARGO_PKG_NAME, "/", service,
    // "/keyed").
    const P: &str = concat!(env!("CARGO_PKG_NAME"), "/Probe/keyed");

    let request_drifted = keylink::derive_alpn::<KeyedRequestDriftedContract>(P);
    let unaddressed = keylink::derive_alpn::<KeyedUnaddressedContract>(P);
    let correct = keylink::derive_alpn::<CorrectKeyedContract>(P);

    assert_ne!(
        request_drifted, correct,
        "wrapping the keyed request in WireRequest must change the hash. A peer \
         that cannot receive a ledger query decodes one as a call, which is the \
         variant confusion this whole scheme exists to refuse"
    );
    assert_ne!(
        unaddressed, correct,
        "wrapping the request in Addressed must change the hash, or a keyed \
         contract and a singleton contract agree about a wire that carries an \
         actor id in one direction and not the other"
    );

    let real = String::from_utf8_lossy(probe::registry_alpn()).into_owned();
    let correct_hash = String::from_utf8_lossy(&correct)
        .rsplit('/')
        .next()
        // NOT `unwrap_or_default`. An empty hash makes `ends_with` below
        // unconditionally true, so the third of the three drifts this test
        // refuses would pass green on a `None` — a guard that quietly does less,
        // in the one test written to close a live defect. `derive_alpn` always
        // emits `{protocol}/{hash:016x}`, so `None` is unreachable rather than
        // tolerable, and it should say so loudly if that ever changes.
        .expect("a derived contract hash always has a '/'-delimited suffix")
        .to_owned();
    assert!(
        !correct_hash.is_empty(),
        "an empty expected suffix would satisfy `ends_with` for any input"
    );

    assert!(
        real.ends_with(&correct_hash),
        "the generated keyed ALPN must hash the keyed wire type.\n  generated: {real}\n  expected suffix: {correct_hash}"
    );
}

/// A service served both ways at once answers on two different ALPNs, and both
/// register on one node.
///
/// One router owns one ALPN, so a collision here would be
/// `ConfigError::DuplicateAlpn` at startup — on a tree where nothing looked
/// wrong, because the two contracts are built from the same `Request` and
/// `Response` and only their wrappers differ.
#[tokio::test]
async fn a_service_serves_as_a_singleton_and_as_an_actor_system_on_one_node() {
    let node = local_node().await;
    let system: probe::Registry = probe::Registry::builder(keylink::Eviction::Never).build();

    assert_ne!(
        probe::alpn(),
        probe::registry_alpn(),
        "the two contracts must not hash the same, or one node cannot serve both"
    );

    let _running = keylink::Server::builder(&node)
        .service(probe::service(Server::default(), Allowlist::anyone()).build())
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, served both ways, on one node");
}

/// The contract hash, pinned to a literal.
///
/// Every other test in this repository compiles BOTH peers from one source
/// tree, so a wire change moves both and the handshake still agrees. That makes
/// wire drift invisible to the whole suite — a test cannot catch a change it
/// participates in. Only a literal recorded outside the type system can.
///
/// # This test going red
///
/// It means the wire format of `probe` changed. Two steps, in order:
///
/// 1. Decide whether the change SHOULD be wire-visible. Adding a method or an
///    error variant should be. A rename or a doc edit should not — if one of
///    those moved this hash, something is in the hashed contract that has no
///    business being there, and that is the bug.
/// 2. Only then update the literal, and say so in the release notes: every
///    peer in the fleet must be rebuilt.
///
/// Never do step 2 without doing step 1. Updating the literal to make a red
/// test green is how a fleet silently splits in half.
///
/// A `postcard-schema` upgrade moves this legitimately.
///
/// # Moved once, deliberately, by the idempotency programme
///
/// `a51c5347dacb0df5` → `aa77e29e5e1ccdc7`. Step 1 was answered yes: the change
/// IS wire-visible and has to be, three times over.
///
/// - The request direction is now `WireRequest<Request>`, because a caller can
///   ask what the ledger knows about one of its keys and that question is not a
///   method on anybody's service.
/// - The response direction is now `WireResponse<Response>` as a two-armed enum
///   rather than a bare `Outcome<Response>`, because that question needs an
///   answer.
/// - `Envelope.idempotency` carries `Idempotency` rather than
///   `IdempotencyKey`, because a caller can now say how long to wait for a call
///   already in flight under the key.
///
/// A peer built before this change would decode a `KeyState` reply as a call
/// reply, which is exactly the postcard variant-confusion the derived ALPN
/// exists to refuse. Refusing it at the handshake is the correct outcome, and
/// costs nothing: keylink is unpublished, so there is no fleet to split.
///
/// # Moved a second time, by the per-service keyed rewrite
///
/// `aa77e29e5e1ccdc7` → `d3fe34471921164e`. **Step 1 was answered before the
/// literal was touched, and the answer is yes.**
///
/// `RemoteError::ActorSchemaMismatch` was deleted. It was the fifth variant of
/// six, and removing it renumbers `Failed` from index 5 to index 4. `RemoteError`
/// is inside every service's hashed contract —
/// `Envelope<WireResponse<Response>>` reaches `WireResponse::Call(Outcome<Resp>)`
/// reaches `Outcome::Err(RemoteError)` — and postcard encodes an enum variant by
/// its declaration index. So a peer built before this change writes a refusal
/// under a number the new one does not have, and a peer built after would read
/// an old `Failed` as whatever now sits at 5. Under a variant *insertion* rather
/// than a removal that is silent and wrong; here it is a decode failure. Either
/// way it is the drift this hash exists to refuse, so the hash MUST move.
///
/// It is not a rename and not a doc edit. Nothing else in `probe`'s transitive
/// contract graph changed: `Envelope`, `WireRequest`, `WireResponse`,
/// `WireKinds` and `Outcome` are untouched, and `probe`'s own `Request` and
/// `Response` are untouched. One cause, one move.
///
/// **This is wire-visible for every service, singletons included** — the keyed
/// rewrite is where the variant became unnecessary, but the cost lands on the
/// whole fleet because `RemoteError` belongs to every service keylink carries.
/// The variant was there to report per-actor schema drift inside one system-wide
/// actor envelope. A keyed system is per-service now, its ALPN hashes
/// `Envelope<WireRequest<Addressed<Request>>>`, drift is refused at the
/// handshake, and a `Registry` of one service physically cannot hold an actor of
/// another. There is nothing left for the variant to say.
///
/// # Moved a third time, to price a refusal a serving side needs
///
/// `d3fe34471921164e` → `d75913b8a4742547`. **Step 1 was answered first, and the
/// answer is yes.**
///
/// `IdempotencyRefusal` gained a seventh variant, `NotKeyable { method: String }`,
/// so that a serving side can say which of its methods accept an idempotency key
/// — [`Keyable`](keylink::Keyable) — and refuse a key on any other. `postcard_schema`
/// hashes the variant *list*, and `IdempotencyRefusal` is inside every service's
/// contract by the same route `RemoteError` is: `Envelope<WireResponse<Response>>`
/// → `WireResponse::Call(Outcome<Resp>)` → `Outcome::Err(RemoteError)` →
/// `RemoteError::Idempotency(IdempotencyRefusal)`. So the move is wire-visible for
/// every service, singletons included, exactly as the second move was.
///
/// **The alternative was to reuse an existing variant, and it was rejected for
/// being false.** `Malformed` says the key is empty or oversized; the key here is
/// well formed and the *method* is what does not take one. `Forgotten` says it ran
/// and the reply is gone; `Unrecordable` says it ran and the reply is not something
/// this ledger holds. **Nothing ran** — which is what rules both of them out, and it
/// is the one fact that does not depend on how either is worded.
///
/// That last clause is deliberate. An earlier version of this paragraph rejected
/// `Unrecordable` on the ground that it *"says the reply carried a payload"*, which
/// was true when written and stopped being true later in this same branch: the
/// retention setting settles `Unrecordable` too, so its wording no longer names a
/// payload. The **conclusion** is untouched, because it never rested on the wording.
/// A refusal that misdescribes itself sends the caller to fix the wrong thing, and
/// the hash exists precisely so that the honest version can be paid for once.
///
/// The other alternative was another `RemoteError::Failed` prefix, which keeps the
/// hash still. Rejected: the prefixes are load-bearing as a `starts_with`
/// discriminator, a guard asserts none is a prefix of another, and adding
/// wire-format vocabulary to dodge a wire-format change trades a checked break for
/// an unchecked one.
///
/// **A prefix was added later and this rejection is not overturned by it.**
/// `RemoteError::OBSERVER_PANICKED` names a cause a peer acts on differently and
/// could not otherwise learn; it was not bought to avoid a hash move. The
/// rejection above is about the second use, and it stands.
///
/// Appended rather than inserted, so no existing variant is renumbered. That is
/// belt and braces here — the hash moves either way and the handshake refuses the
/// old peer either way — and it is the habit that keeps a *silent* misdecode off
/// the table when a hash somehow does not move.
///
/// **Measured, not reasoned about, and it caught a naming change on the way.** The
/// variant was first written `NotKeyed`, which gave `keylink/Probe/68a16a96e45f8da8`
/// and `keylink/Inspect/7ebc3f421c33c62a`; renaming it to `NotKeyable` — because
/// `Keyed` is the keyed-actor word, see [`Keyable`](keylink::Keyable) — moved both
/// again, to the literals below. So the hash is sensitive to a variant's **name**
/// and not only to the shape of what it carries, which is worth knowing before
/// anybody assumes a rename is free. Both pairs were read off a failing run of this
/// file rather than derived.
///
/// One cause, one move: nothing else in `probe`'s contract graph changed in the
/// commit that moved this literal. `Keyable` and `Records` are serving-side
/// configuration and appear on no wire.
///
/// # Moved a fourth time, to put the failure vocabulary on the wire
///
/// `d75913b8a4742547` → `75cb158fa0f636c8`. **Step 1 was answered before the
/// literal was touched, and the answer is yes.**
///
/// `RemoteError` gained four variants — `Withheld`, `AtCapacity`, `BadActorId`
/// and `FollowRefused` — and it reaches every service's contract by the route the
/// third move already walks, so `probe` pays again for a vocabulary none of its
/// own methods produce.
///
/// Step 1 of `CONTRIBUTING.md`'s rule is answered the same way `NotKeyable`
/// answered it. `keylink::Withheld` names the losses a peer reads today off the
/// front of a `Failed(String)` with `starts_with` — *retry this*, *stop calling*,
/// *an observer vetoed a turn that ran* — and a peer built against the old shape
/// decodes none of them. Being refused at the handshake is the correct outcome
/// for a peer that would otherwise match a refusal by its prefix and get a
/// variant it cannot name.
///
/// **Nothing produces the four yet, and the ALPN is paid anyway.** That is the
/// order the change is worth making in: this commit adds the types, the next one
/// re-points the served paths onto them, and a peer is refused once rather than
/// at each of two handshakes.
///
/// Appended, not inserted, for `NotKeyable`'s reason: the hash moves either way,
/// so what the append buys is that no existing variant is renumbered.
///
/// **Both pins moved in this run, and the run does not separate the causes.** The
/// same commit renamed a field of `NodeStatus`, which is in `Inspect`'s graph and
/// no other — see [`the_inspect_contract_hash_has_not_moved`]. So this is one
/// measurement covering two causes, and the attribution below it is read off the
/// contract graphs rather than off the numbers: `RemoteError` is the only one of
/// the two that can reach `probe`.
///
/// **There were two runs, and the literals above are the second one's.** The first
/// run measured the four variants alone. The commit was then amended to give
/// `BadActorId` a `reason` field — an id refused without saying why is the loss
/// this vocabulary exists to stop — and that moved both pins again, before either
/// was pushed. `0c6e6c3`'s own body records both runs and is the source of truth
/// for this entry. Every literal here was read off a failing run rather than
/// derived; none was read off the first one.
///
/// **An amend that re-measures leaves the first measurement's prose behind, and
/// that is worth a sentence because it will recur.** A constant has a test. A
/// sentence *about* the constant has nothing, so it survives the re-measurement
/// that invalidates it. The entry below headed itself with the first run's
/// destination for `Inspect` — a value which, once the amend landed, existed
/// nowhere else in this repository — and stayed that way through a whole merge
/// gate. So a re-measurement rewrites every sentence naming a number, and not
/// only the `PINNED` line the compiler reads.
///
/// # Moved a fifth time, so a peer can ask what an actor is doing
///
/// `75cb158fa0f636c8` → `4697a79e29347129`. **Step 1 was answered before the
/// literal was touched, and the answer is yes — for both of the two causes this
/// commit carries.**
///
/// - **Every `Request` gained `__Status` and every `Response` gained
///   `__Status(Serving)`.** A local `Handle` could ask whether an actor was
///   inside a turn and how deep its queue was; a peer could ask neither, on the
///   service it was already connected to. The pair is generated onto every
///   contract rather than declared by one, so each service pays this once and
///   never again for the same reason.
/// - **`RemoteError` gained `NotAnActor`**, which reaches every contract by the
///   route the third and fourth moves already walk. It is the answer to
///   `__Status` from a singleton served with a handler rather than as an actor —
///   one ALPN carries both ways, so which one is behind a connection is a fact a
///   peer can learn only from an answer.
///
/// A peer built against the old shape must be refused for either cause on its
/// own. It can decode `__Status` in neither direction, and it would meet
/// `NotAnActor` as a refusal variant it cannot name — which is exactly the
/// `starts_with`-shaped mistake the fourth move was made to end.
///
/// Appended and not inserted, for `NotKeyable`'s reason: the hash moves either
/// way, so what the append buys is that no existing variant is renumbered.
///
/// **Both pins moved in this run, and the run does not separate the causes.**
/// Both causes above are shared by every service, so both pins had to move; and
/// `Inspect` carries a third cause of its own — see
/// [`the_inspect_contract_hash_has_not_moved`]. So this is one measurement
/// covering three causes, and the attribution is read off the contract graphs
/// rather than off the numbers. Both literals were read off that one failing
/// run, which reported **two** failures. Neither was computed.
///
/// # Moved a sixth time, for one cause, so a peer can be told to back off
///
/// `4697a79e29347129` → `b228be26a183b341`. **`RemoteError` gained
/// `MailboxFull`**, and that is the whole of it: no `Request` or `Response`
/// changed shape, so this move is narrower than the fifth and the attribution
/// needs no graph reading.
///
/// A peer built against the old shape must be refused, and the reason is the one
/// the fourth move established: it would meet a refusal variant it cannot name.
/// Rendering an overload as something it *can* name is what the variant exists to
/// stop — a peer that reads `TimedOut` retries the same call and makes an
/// overloaded actor worse, which is a wrong action rather than a lost one.
///
/// Appended and not inserted, for `NotKeyable`'s reason, which has now held for
/// three moves running: the hash moves either way, so what the append buys is
/// that no existing variant is renumbered.
///
/// **Both pins moved again, and this time they moved for the same single cause.**
/// `RemoteError` is in every service's contract, so `Inspect` moved for this and
/// for nothing of its own. Both literals were read off one failing run, which
/// reported **two** failures. Neither was computed.
/// # Skipped until keylink has a stable version — Pedro's ruling, 2026-08-10
///
/// A moved contract hash is not a cost here, so a test that **blocks** the move
/// is buying nothing and charging a gate run for it. `AGENTS.md` carries the
/// ruling and `README.md`'s `## Status` states it: nothing is scheduled, grouped
/// or reshaped to keep a pin still.
///
/// **What this does not skip.** The property tests are untouched — that two type
/// sets derive two different contracts, and that a peer built from other types is
/// refused at the handshake, are claims about the mechanism rather than about one
/// literal. Only the literal is parked.
///
/// **Re-enable it the day a stable version is cut**, which is the same day the
/// `AGENTS.md` section goes. Read the new value off the failing run then, exactly
/// as the note below says. Do not compute it.
#[test]
#[ignore = "the ALPN is not a cost until a stable version; see AGENTS.md"]
fn the_contract_hash_has_not_moved() {
    const PINNED: &str = "keylink/Probe/b228be26a183b341";

    assert_eq!(
        String::from_utf8_lossy(probe::alpn()),
        PINNED,
        "\nthe wire format of `probe` changed.\n\
         Read this test's documentation before updating the literal.\n"
    );
}

/// **keylink's own service has a contract too, and this is its first pin.**
///
/// Wave 6 gave keylink a `service!` block of its own — `keylink::inspect` — so
/// the crate now carries two wire contracts rather than a macro and no users of
/// it. This is that contract's literal, pinned on the day it was derived.
///
/// The two ALPNs are **independent**, and that independence is what the wave
/// needed: `ActorStatus` gained `Serialize`, `Deserialize` and `Schema` so that
/// a console could read it, and the pin above proves that decision reached no
/// existing contract. `Probe` does not carry an `ActorStatus` and neither does
/// any keyed or singleton service, because a service's hash walks *its own*
/// request and response types.
///
/// The rule for this literal is the rule for the one above: when it goes red,
/// decide whether the change *should* be visible to a console built against the
/// old shape, and only then update it. A fifth field on `ActorStatus` moves this
/// hash, which is the whole point — an old console is refused at the handshake
/// rather than handed four fields where it expected five.
///
/// # It has moved five times, and these are the decisions
///
/// ## `f62a21fb09137ace` → `e7579f60b94c0ea0`, three causes in one commit
///
/// Two of the three are shared with every service the crate carries, and
/// [`the_contract_hash_has_not_moved`] holds their argument: `__Status` on every
/// `Request` and `Response`, and `RemoteError::NotAnActor`. This is the third,
/// and it is this service's alone.
///
/// **`Inspect` gained `rpc WhyEnded(id) -> Option<EndReason>`.** An ordinary
/// peer of an ordinary service still cannot ask why an actor ended — that state
/// is the one in which a service's own methods are not served — but an operator
/// admitted to this list can now ask about one id instead of following the whole
/// log to find it. It adds **no information class and no availability**:
/// `Follow(0)` already hands the same allowlist every retained
/// [`LifecycleEvent::Ended`](keylink::LifecycleEvent), so this answers a subset
/// of what that answers. It adds no state either — [`Inspector`] gains no field,
/// and the lookup walks the ring `follow` already reads.
///
/// It is **not** `rpc Ended`, which is the name the wave was planned under. That
/// generates `ended`, and every `service!` block already puts an `ended` on its
/// `Handle`; two inherent methods of one name in one impl block is
/// `error[E0592]`, so the rpc could not have compiled. The name is the fix and
/// not a preference.
///
/// The change should be visible to a console. One built against the old shape
/// has no method to call and could decode neither the new request variant nor
/// its reply, so being refused at the handshake is the correct outcome.
///
/// **`Probe`'s hash moved in the same run, and here that is expected rather than
/// evidence**: two of these three causes are shared by every contract. The run
/// reported both failures together and attributes neither.
///
/// [`Inspector`]: keylink::Inspector
///
/// ## `0572763878b7fb9d` → `f62a21fb09137ace`, two causes in one commit
///
/// **`Probe`'s hash moved too**, and this is the one entry where that is not by
/// itself evidence of anything. The commit carries two changes and only one of
/// them is shared: `RemoteError` gained four variants and sits inside every
/// contract, so it moves both pins, while `NodeStatus::lost` became
/// `NodeStatus::dropped` and `NodeStatus` is this service's alone.
/// [`the_contract_hash_has_not_moved`] carries the argument for the shared half.
///
/// The rename is wire-visible and should be. A **field name** is part of the
/// schema exactly as a variant name is — that test measured the variant case and
/// records it — and the old name was the only thing left telling a console that
/// this number is not `EventPage::lost`. One is the channel, dropped before it
/// was ever sequenced; the other is the ring, sequenced and then pushed out from
/// under a console that fell behind. A console reading `dropped` where it expects
/// `lost` is refused at the handshake rather than shown one loss counter under
/// the other's name.
///
/// Both literals were read off a failing run of this file — the **second** of the
/// two runs the commit needed, and the reason this heading named a destination
/// that existed nowhere else in the tree until it was corrected.
/// [`the_contract_hash_has_not_moved`] carries the amend that caused the second
/// run, and the mechanism. Neither run attributes either move to one of the two
/// causes, and nothing here claims it does.
///
/// ## `7bbd34fa83c9060c` → `0572763878b7fb9d`, pricing `IdempotencyRefusal::NotKeyable`
///
/// The full argument is on [`the_contract_hash_has_not_moved`], because the cause
/// belongs to neither service: `IdempotencyRefusal` gained `NotKeyable`, and it sits
/// inside *every* contract by way of `Outcome::Err(RemoteError)` →
/// `RemoteError::Idempotency(..)`. Step 1 was answered yes there.
///
/// **This is the first time both pins moved in one commit, and that is the
/// evidence rather than an embarrassment.** The two entries below each say
/// "`Probe`'s hash did not move" as proof these pins discriminate. The converse
/// proof is this one: a change to a type shared by every service must move *both*,
/// and a change to one service's own request or response types must move exactly
/// one. A shared change that moved only one pin would mean a service was hashing
/// something other than what it carries — which is the drift RFC-05 §6 and
/// [`WireResponse`](keylink::WireResponse)'s own doc comment record as having
/// happened once already.
///
/// `Inspect` carries no idempotency key on any of its own methods and this still
/// moves its ALPN, which is exactly the cost the variant was weighed against. It is
/// paid once, pre-1.0, by a crate with no fleet.
///
/// ## `080c9e481c937d67` → `7bbd34fa83c9060c`, applying Wave 6's review
///
/// Three changes at once, and all three **should** be visible to a console:
///
/// - **`Follow` answers `Result<EventPage, FollowRefused>`.** It used to answer
///   an `EventPage` and had no way to say no, so a cursor ahead of the node's
///   log was *parked* — an entry in the inspector's waiting list that no event
///   could release, and the reason that list stopped being bounded by anything.
///   A console built against the old shape cannot decode a refusal, and a
///   refusal is now the ordinary answer to two ordinary mistakes.
/// - **`EventPage` gained `dropped`.** Events lost by the sink's `try_send`
///   never get a sequence number, so `EventPage::lost` — arithmetic over
///   sequence numbers — could not see them, and the loop this crate documents
///   was blind to the likelier of the two losses. An old console reading a page
///   without the field would report a complete stream that was not one.
/// - **`NodeStatus::id` became `NodeKey`.** Identical bytes on the wire and a
///   different *schema*: a struct where there was an array. That is the point —
///   a raw `[u8; 32]` is assignable between a node key, a blob id, a hash and a
///   secret key, and the newtype is what stops a console pointing one at the
///   wrong one. A peer built against the array is refused at the handshake
///   rather than reading one kind of thirty-two bytes as another.
///
/// **`Probe`'s hash did not move**, again, which is the same evidence it was the
/// first time: three changes to one contract, none to the other, and exactly one
/// of these two tests went red.
///
/// ## `879d78ee456a20b7` → `080c9e481c937d67`, when Wave 9 merged into Wave 6
///
/// Wave 9 gave `Ended` a sixth variant (`GaveUp`) and gave `Ended::Observer` a
/// `fatal` flag. `EndReason` is this service's wire rendering of `Ended` and
/// carries no wildcard arm, deliberately, so the merge did not compile until both
/// were answered here — `E0027` for the field, and `E0004` waiting behind it for
/// the variant. That is the guard working: a textual merge of two branches that
/// never saw each other produced a wire type quietly missing an ending, and the
/// compiler refused it.
///
/// The change **should** be visible to a console. `GaveUp` is the one ending that
/// means *this actor could not be kept alive*, as against every other variant,
/// which means something asked it to stop; and `fatal` is the difference between
/// "this turn was refused" and "there will be no next incarnation". A console
/// built against the old shape would render a supervised actor's death as an
/// unknown ending, so being refused at the handshake is the correct outcome.
///
/// **`Probe`'s hash did not move in the same run**, which is the evidence that
/// these two pins discriminate rather than both being satisfiable: one contract
/// changed, one did not, and exactly one test went red.
///
/// ## `e7579f60b94c0ea0` → `9ef860a8eabbc99c`, for a cause this service does not own
///
/// `RemoteError` gained `MailboxFull`. Nothing in `inspect`'s own request or
/// response types changed, and this contract moved because `RemoteError` is in
/// **every** service's contract — the route the third, fourth and fifth moves of
/// [`the_contract_hash_has_not_moved`] already walk.
///
/// So this is the mirror of the paragraph above: both pins moved in one run, and
/// the evidence that they still discriminate is the *previous* entry, where one
/// moved and one did not. A run in which both move is consistent with a shared
/// cause and proves nothing on its own. Both literals were read off that one
/// failing run, which reported two failures. Neither was computed.
///
/// [`FollowRefused`]: keylink::FollowRefused
/// [`EventPage`]: keylink::EventPage
/// [`NodeKey`]: keylink::NodeKey
/// # Skipped until keylink has a stable version — Pedro's ruling, 2026-08-10
///
/// The pair of this file's other pin, parked for its reason and re-enabled on the
/// same day. This one moves more often than its own contract changes, because
/// `RemoteError` is in every service's contract — so it is the pin that most
/// often blocks a change it has nothing to say about.
#[test]
#[ignore = "the ALPN is not a cost until a stable version; see AGENTS.md"]
fn the_inspect_contract_hash_has_not_moved() {
    const PINNED: &str = "keylink/Inspect/9ef860a8eabbc99c";

    assert_eq!(
        String::from_utf8_lossy(keylink::inspect::alpn()),
        PINNED,
        "\nthe wire format of keylink's own `inspect` service changed.\n\
         Read this test's documentation before updating the literal.\n"
    );
}

// ─────────────────────────────────────────────── and what must NOT move it

// Two declarations of the SAME service, identical in every way except that one
// method of the second is `#[defer]`. The service *name* is folded into the hash —
// `schema_hash(protocol)` takes the protocol string — so two differently named
// services would differ for a reason that has nothing to do with the attribute, and
// the comparison would prove nothing. Same name, two modules, one difference.
//
// Deliberately not the `Probe` above: that one is pinned to a literal, and a second
// declaration of it would be a second thing to keep in step.
mod plain {
    keylink::service! {
        service Deferring;

        rpc Fill(price: u64) -> u64;
        rpc Place(size: u64) -> u64;
    }
}

mod deferring {
    keylink::service! {
        service Deferring;

        rpc Fill(price: u64) -> u64;
        #[defer]
        rpc Place(size: u64) -> u64;
    }
}

/// **Adding `#[defer]` to a method does not move the contract hash**, and this pins
/// it rather than asserting it in a comment.
///
/// `#[defer]` changes who holds a reply channel on the serving side. It does not
/// change `Request`, `Response`, `WireRequest`, `WireResponse`, `Envelope` or
/// `WireKinds` — the six types `Contract` is built from — so there is nothing in the
/// hashed graph for it to touch. A deferred method is an ordinary `rpc` to the
/// caller, blocking on its stream exactly as before.
///
/// The two declarations differ only in the attribute and are the same service by
/// name, so the whole ALPN byte string is compared rather than the hash alone — a
/// stronger assertion than splitting the hash out and one that cannot pass by the
/// protocol prefix having drifted too.
///
/// Both ALPNs are compared, keyed and singleton, because the keyed contract hashes
/// `Addressed<Request>` and a `#[defer]` method must not move that either.
///
/// The one thing this cannot prove is that the hash is *sensitive* to what it
/// should be. Made red by changing the deferring `Place` to take an `i64`: observed
/// `left: "keylink/Deferring/fc7ebbc70dab10a0"`, `right:
/// "keylink/Deferring/aa439906ad24a918"` on the singleton arm.
#[test]
fn adding_defer_to_a_method_does_not_move_the_contract_hash() {
    assert_eq!(
        String::from_utf8_lossy(plain::deferring::alpn()),
        String::from_utf8_lossy(deferring::deferring::alpn()),
        "the ALPN moved for a change that is not on the wire: `#[defer]` decides who \
         holds a reply channel on the serving side, and touches none of the six types \
         `Contract` is built from"
    );
    assert_eq!(
        String::from_utf8_lossy(plain::deferring::registry_alpn()),
        String::from_utf8_lossy(deferring::deferring::registry_alpn()),
        "the keyed ALPN moved for a change that is not on the wire"
    );
}

// The same pattern for a `feed`, and for the same reason it has to be the same
// pattern: two declarations of one service, identical but for the feed.
mod unfed {
    keylink::service! {
        service Fed;

        rpc Fill(price: u64) -> u64;
    }
}

mod fed {
    keylink::service! {
        service Fed;

        rpc Fill(price: u64) -> u64;
        feed Ticks(tick: u64);
        // Two, so the comparison also covers whatever a second one might add. A
        // service with one feed and a service with two must both hash like a
        // service with none.
        feed Fills(fill: [u8; 32]);
    }
}

/// **Adding a `feed` does not move the contract hash.**
///
/// The strongest form of "feeds are not on the wire", and it is stronger than the
/// `#[defer]` twin above rather than merely parallel to it. `#[defer]` is an
/// attribute on a method that *is* on the wire, so the claim there is that the
/// attribute changes nothing. A `feed` adds a whole declaration, with an item type
/// of its own — `[u8; 32]` here, a type the wire graph has never seen — and the
/// claim is that a declaration can exist without touching the contract at all.
///
/// Structurally, that is because a feed is not a sixth `kind`: the muncher puts it
/// in a second list, and `Request`, `Response`, `Contract`, `WireKinds`, `Handler`
/// and both clients expand over the first list only. There is no arm anywhere that
/// has to remember to leave a feed out.
///
/// `[u8; 32]` is not decoration. A `postcard_schema::Schema` reaches every type
/// nested in a hashed one, so an item type that leaked into `Request` or `Response`
/// through some forgotten repetition would move the hash — a plain `u64` would not,
/// `Fill` already carrying one.
///
/// Both ALPNs, keyed and singleton, for the reason the twin gives.
///
/// The sensitivity this cannot prove itself is proved by the twin above, which was
/// made red by changing an argument type. [`the_contract_hash_has_not_moved`] is the
/// other half: **the feed wave did not move its pin**, whatever that pin's value is
/// at the time you read this.
///
/// Named rather than quoted, on purpose. This sentence used to cite the literal, and
/// the literal moved — so the citation pointed at a value that was no longer a pin,
/// and a reader grepping for it found nothing. The claim here was never about the
/// value: it is that *this* wave left that test alone, which is a fact about the test.
/// Note also that "unchanged by this wave" means the **feed** wave and not the most
/// recent one; a later wave did move that pin, deliberately, and its own narrative
/// entry records why.
#[test]
fn adding_a_feed_to_a_service_does_not_move_the_contract_hash() {
    assert_eq!(
        String::from_utf8_lossy(unfed::fed::alpn()),
        String::from_utf8_lossy(fed::fed::alpn()),
        "the ALPN moved for a declaration that is not on the wire: a `feed` has no \
         `Request` variant, no `Response` variant and no `WireKinds` entry, and \
         touches none of the six types `Contract` is built from"
    );
    assert_eq!(
        String::from_utf8_lossy(unfed::fed::registry_alpn()),
        String::from_utf8_lossy(fed::fed::registry_alpn()),
        "the keyed ALPN moved for a declaration that is not on the wire"
    );
}
