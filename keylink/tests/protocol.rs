//! End-to-end behaviour of a generated service over a real QUIC connection.
//!
//! Everything here runs against two iroh endpoints on the loopback interface
//! with no relay and no discovery, so the transport is genuine and the tests
//! need no network.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use keylink::{
    Allowlist, Body, BodyReader, Context as KeylinkContext, Error, Node, Server as KeylinkServer,
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

struct Server;

impl probe::Handler for Server {
    async fn ping(&self, _cx: &KeylinkContext) -> String {
        "pong".to_string()
    }

    async fn slow(&self, _cx: &KeylinkContext, ms: u64) -> u64 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
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
        loop {
            match body.next_chunk().await {
                Ok(Some(chunk)) => {
                    checksum.update(&chunk);
                    bytes += chunk.len() as u64;
                }
                Ok(None) => break,
                Err(error) => {
                    return Received::Failed {
                        reason: error.to_string(),
                    }
                }
            }
        }
        Received::Stored {
            bytes,
            checksum: checksum.finish(),
        }
    }
}

// ────────────────────────────────────────────────────────────────── fixture

mod common;
use common::local_node;

/// A running server and a client connected to it. Both nodes are held so the
/// endpoints outlive the test body.
struct Fixture {
    client: probe::Client,
    _server: KeylinkServer,
    _nodes: [Node; 2],
}

async fn start() -> Fixture {
    start_with(Allowlist::anyone()).await
}

async fn start_with(allow: Allowlist) -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let router = probe::serve(&server, Server, allow).expect("one service, no duplicate ALPN");
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
    let fixture = start_with(Allowlist::only([stranger.id()])).await;

    let error = fixture.client.ping().await.unwrap_err();
    assert!(
        matches!(error, Error::Transport(_)),
        "expected a transport failure, got {error:?}"
    );
}

/// The headline property of one stream per call. Eight handlers that each sleep
/// 150 ms take 1200 ms in series. They must not — and they must not even when
/// every call is driven from a single task, because the concurrency comes from
/// the transport rather than from a thread pool.
#[tokio::test]
async fn concurrent_calls_do_not_queue_behind_each_other() {
    const CALLS: u64 = 8;
    const DELAY_MS: u64 = 150;

    let fixture = start().await;
    let client = &fixture.client;

    let started = Instant::now();
    let results = tokio::join!(
        client.slow(DELAY_MS),
        client.slow(DELAY_MS),
        client.slow(DELAY_MS),
        client.slow(DELAY_MS),
        client.slow(DELAY_MS),
        client.slow(DELAY_MS),
        client.slow(DELAY_MS),
        client.slow(DELAY_MS),
    );
    let elapsed = started.elapsed();

    for result in [
        results.0, results.1, results.2, results.3, results.4, results.5, results.6, results.7,
    ] {
        assert_eq!(result.unwrap(), DELAY_MS);
    }

    let serial = Duration::from_millis(DELAY_MS * CALLS);
    assert!(
        elapsed < serial / 2,
        "{CALLS} concurrent calls took {elapsed:?}; in series they would take {serial:?}"
    );
    println!("{CALLS} concurrent {DELAY_MS}ms calls finished in {elapsed:?} (serial: {serial:?})");
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
        .service(probe::service(Server, Allowlist::anyone()).build())
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
        .service(probe::service(Server, Allowlist::anyone()).build())
        .service(probe::service(Server, Allowlist::anyone()).build())
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
    let _running = probe::serve(&server, Server, Allowlist::anyone()).expect("serves");

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
            probe::service(Server, Allowlist::anyone())
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

#[tokio::test]
async fn a_timeout_layer_refuses_a_slow_handler() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(
            probe::service(Server, Allowlist::anyone())
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
#[test]
fn a_synchronous_program_can_call_a_service() {
    // The server side still needs a runtime; it is a separate one, which is
    // also the realistic shape (server and client are different processes).
    let rt = tokio::runtime::Runtime::new().expect("a runtime for the server");
    let (addr, _guard) = rt.block_on(async {
        let server = local_node().await;
        let running = probe::serve(&server, Server, Allowlist::anyone()).expect("serves");
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
        .service(probe::service(Server, Allowlist::anyone()).build())
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

    let real = String::from_utf8_lossy(probe::keyed_alpn()).into_owned();
    let correct_hash = String::from_utf8_lossy(&correct)
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_owned();

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
    let system: probe::System = probe::System::builder(keylink::Eviction::Never).build();

    assert_ne!(
        probe::alpn(),
        probe::keyed_alpn(),
        "the two contracts must not hash the same, or one node cannot serve both"
    );

    let _running = keylink::Server::builder(&node)
        .service(probe::service(Server, Allowlist::anyone()).build())
        .service(probe::keyed_service(&system, Allowlist::anyone()).build())
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
/// handshake, and a `System` of one service physically cannot hold an actor of
/// another. There is nothing left for the variant to say.
#[test]
fn the_contract_hash_has_not_moved() {
    const PINNED: &str = "keylink/Probe/d3fe34471921164e";

    assert_eq!(
        String::from_utf8_lossy(probe::alpn()),
        PINNED,
        "\nthe wire format of `probe` changed.\n\
         Read this test's documentation before updating the literal.\n"
    );
}
