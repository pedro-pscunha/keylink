//! Retry safety over a real QUIC connection.
//!
//! Every handler here reports the number of times it has run, so a reply is
//! itself the evidence: the same number twice means the handler was not entered
//! again, and no test has to reach inside the serving side to find out.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use iroh::endpoint::presets;
use iroh::Endpoint;
use keylink::{
    Allowlist, Body, BodyReader, Context, Error, IdempotencyKey, IdempotencyRefusal, Middleware,
    Next, Node, Outcome, RemoteError, Server as KeylinkServer,
};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};

// ────────────────────────────────────────────────────────────── the service

#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub struct Sheet {
    pub runs: u64,
    pub bytes: u64,
}

keylink::service! {
    service Till;

    // Answers with the number of times it has run. A repeated answer is proof
    // that the handler was not entered a second time.
    rpc Reserve(sku: String, qty: u32) -> u64;
    rpc Slow(ms: u64) -> u64;

    // The reply carries a payload, which is what makes it unrecordable.
    recv Datasheet(bytes: u64) -> Sheet;
}

#[derive(Default)]
struct Counter(AtomicU64);

impl Counter {
    fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }
}

#[derive(Default)]
struct Till {
    reserves: Counter,
    slows: Counter,
    sheets: Counter,
}

impl till::Handler for Till {
    async fn reserve(&self, _sku: String, _qty: u32) -> u64 {
        self.reserves.next()
    }

    async fn slow(&self, ms: u64) -> u64 {
        let run = self.slows.next();
        tokio::time::sleep(Duration::from_millis(ms)).await;
        run
    }

    async fn datasheet(&self, bytes: u64) -> (Sheet, Body) {
        let sheet = Sheet {
            runs: self.sheets.next(),
            bytes,
        };
        (sheet, Body::from_bytes(vec![0u8; bytes as usize]))
    }
}

// ────────────────────────────────────────────────────────────────── fixture

struct Fixture {
    client: till::Client,
    _server: KeylinkServer,
    _nodes: Vec<Node>,
}

async fn local_node() -> Node {
    Node::from_endpoint(
        Endpoint::bind(presets::Minimal)
            .await
            .expect("a local endpoint binds"),
    )
}

async fn start() -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let running = till::serve(&server, Till::default(), Allowlist::anyone()).expect("serves");
    let client = till::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the server");

    Fixture {
        client,
        _server: running,
        _nodes: vec![server, client_node],
    }
}

fn key(text: &str) -> IdempotencyKey {
    IdempotencyKey::new(text).expect("a valid key")
}

/// An oversized key, built the only way a peer can build one.
///
/// `IdempotencyKey::new` refuses this, and `derive(Deserialize)` does not call
/// it. That gap is exactly what the receive path has to close, so a test of the
/// receive path has to go through the gap rather than around it.
fn smuggled(text: &str) -> IdempotencyKey {
    let encoded = postcard::to_stdvec(&text.to_string()).expect("encodes");
    postcard::from_bytes(&encoded).expect("a peer can encode what our constructor refuses")
}

fn refusal(error: &Error) -> IdempotencyRefusal {
    match error {
        Error::Remote(RemoteError::Idempotency(refusal)) => refusal.clone(),
        other => panic!("expected an idempotency refusal, got {other:?}"),
    }
}

// ─────────────────────────────────────────────────────────── the guarantee

#[tokio::test]
async fn a_repeated_key_does_not_run_the_handler_twice() {
    let fixture = start().await;
    let once = fixture.client.with_idempotency(key("reserve-4471"));
    let first = once.reserve("ADU-014".to_string(), 2).await.unwrap();

    let again = fixture
        .client
        .with_idempotency(key("reserve-4471"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();

    assert_eq!(
        again, first,
        "the repeat must be answered from the record, not by running the handler again"
    );
}

#[tokio::test]
async fn a_different_key_runs_the_handler_again() {
    let fixture = start().await;
    let first = fixture
        .client
        .with_idempotency(key("reserve-1"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();

    let second = fixture
        .client
        .with_idempotency(key("reserve-2"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();

    assert_eq!(second, first + 1, "a second key is a second operation");
}

/// The default path must not change. A call with no key is a call with no
/// record, and it runs every time it is made.
#[tokio::test]
async fn a_call_without_a_key_runs_every_time() {
    let fixture = start().await;
    let first = fixture
        .client
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();

    let second = fixture
        .client
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();

    assert_eq!(second, first + 1);
}

// ──────────────────────────────────────────────────────── the receive path

/// `new` cannot protect the receiving side, so the receiving side checks again.
/// A peer that puts 129 bytes on the wire is refused before it reaches a map
/// keyed by whatever it sent.
#[tokio::test]
async fn an_oversized_key_from_a_peer_is_refused() {
    let fixture = start().await;
    let error = fixture
        .client
        .with_idempotency(smuggled(&"x".repeat(keylink::MAX_KEY_LEN + 1)))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("a key past the cap must not be accepted");

    assert!(
        matches!(refusal(&error), IdempotencyRefusal::Malformed(_)),
        "got {error:?}"
    );
}

#[tokio::test]
async fn an_empty_key_from_a_peer_is_refused() {
    let fixture = start().await;
    let error = fixture
        .client
        .with_idempotency(smuggled(""))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("an empty key must not be accepted");

    assert!(
        matches!(refusal(&error), IdempotencyRefusal::Malformed(_)),
        "got {error:?}"
    );
}

/// A refused key must not consume the key either. The handler never ran, so
/// nothing has been recorded, and a well-formed retry is still a first attempt.
#[tokio::test]
async fn a_refused_key_leaves_the_handler_reachable() {
    let fixture = start().await;
    let _refused = fixture
        .client
        .with_idempotency(smuggled(""))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("refused");

    let run = fixture
        .client
        .with_idempotency(key("good"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("a valid key still works");

    assert_eq!(run, 1, "the refusal must not have run the handler");
}

// ───────────────────────────────────────────────────────────────── scoping

/// A key is a string a peer chose. If the namespace were shared, peer B would
/// read peer A's reply by guessing it — an information leak before it is a
/// collision.
#[tokio::test]
async fn two_peers_using_the_same_key_do_not_collide() {
    let server = local_node().await;
    let first_node = local_node().await;
    let second_node = local_node().await;
    let _running = till::serve(&server, Till::default(), Allowlist::anyone()).expect("serves");
    let addr = server.endpoint().addr();

    let first = till::Client::connect(&first_node, addr.clone())
        .await
        .expect("dials");
    let second = till::Client::connect(&second_node, addr)
        .await
        .expect("dials");

    let a = first
        .with_idempotency(key("shared"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();
    let b = second
        .with_idempotency(key("shared"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();

    assert_eq!(
        b,
        a + 1,
        "the second peer must be run, not answered from the first peer's record"
    );
}

// ───────────────────────────────────────────────────────────── concurrency

/// Two identical calls at once, not one after the other. The record is claimed
/// before the handler is entered, so the second one finds it and is refused
/// rather than queued behind a wakeup nobody can reason about.
#[tokio::test]
async fn a_concurrent_duplicate_is_refused_while_the_first_is_in_flight() {
    let fixture = start().await;
    let scoped = fixture.client.with_idempotency(key("slow"));
    let other = fixture.client.with_idempotency(key("slow"));

    let (first, second) = tokio::join!(scoped.slow(300), other.slow(300));

    let refused = match (first, second) {
        (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
        (first, second) => panic!("exactly one call must be refused, got {first:?} and {second:?}"),
    };
    assert_eq!(refusal(&refused), IdempotencyRefusal::InFlight);
}

/// And the refusal is not the end of the story: once the running call settles,
/// the same key answers from its record.
#[tokio::test]
async fn the_record_is_readable_once_the_in_flight_call_settles() {
    let fixture = start().await;
    let run = fixture
        .client
        .with_idempotency(key("slow"))
        .slow(1)
        .await
        .unwrap();

    let replayed = fixture
        .client
        .with_idempotency(key("slow"))
        .slow(1)
        .await
        .unwrap();

    assert_eq!(replayed, run);
}

// ─────────────────────────────────────────────────────── refusals and bodies

/// Refuses the first call it sees and passes everything after it, so a replayed
/// refusal is distinguishable from a fresh one: if the refusal were not
/// recorded, the second call would reach the handler and succeed.
struct RefuseOnce(AtomicU64);

impl<Resp: Send + 'static> Middleware<till::Request, Resp> for RefuseOnce {
    fn handle(
        &self,
        cx: Context,
        req: till::Request,
        body: BodyReader,
        next: Next<till::Request, Resp>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = (Outcome<Resp>, Body)> + Send>> {
        let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            if first {
                return (
                    Outcome::Err(RemoteError::Unauthorized("not this time".into())),
                    Body::empty(),
                );
            }
            next(cx, req, body).await
        })
    }
}

/// A refusal is a legitimate result of a call, so it is recorded like any other.
/// The rule is one execution per key, not one success per key.
#[tokio::test]
async fn a_refusal_is_recorded_and_replayed() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(
            till::service(Till::default(), Allowlist::anyone())
                .layer(RefuseOnce(AtomicU64::new(0)))
                .build(),
        )
        .spawn()
        .expect("serves");
    let client = till::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("dials");

    let _first = client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("the layer refuses the first call");

    let error = client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("the repeat must replay the refusal, not reach the now-open handler");

    assert!(
        matches!(error, Error::Remote(RemoteError::Unauthorized(_))),
        "got {error:?}"
    );
}

/// A body is streamed and never held. Recording one would contradict the
/// property that makes a 4 GiB transfer cost a bounded amount of memory, so the
/// repeat is refused rather than answered with a header whose payload never
/// arrives.
#[tokio::test]
async fn a_reply_that_carries_a_payload_is_refused_on_repeat() {
    let fixture = start().await;
    let (_sheet, body) = fixture
        .client
        .with_idempotency(key("sheet"))
        .datasheet(4_096)
        .await
        .expect("the first call runs");
    assert_eq!(body.drain().await.unwrap(), 4_096);

    let error = fixture
        .client
        .with_idempotency(key("sheet"))
        .datasheet(4_096)
        .await
        .expect_err("a recorded payload is not something keylink can replay");

    assert_eq!(refusal(&error), IdempotencyRefusal::Unrecordable);
}

// ─────────────────────────────────────────────── one ledger per service

keylink::service! {
    service Depot;

    rpc Reserve(sku: String, qty: u32) -> u64;
}

#[derive(Default)]
struct Depot(Counter);

impl depot::Handler for Depot {
    async fn reserve(&self, _sku: String, _qty: u32) -> u64 {
        self.0.next()
    }
}

/// Two services on one node keep separate ledgers, because a record is an
/// encoded reply of one service's response type. Sharing them would let a value
/// recorded against one schema be replayed into another — the exact
/// variant-confusion the derived ALPN refuses, one layer below the gate.
#[tokio::test]
async fn two_services_on_one_node_do_not_share_a_ledger() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(till::service(Till::default(), Allowlist::anyone()).build())
        .service(depot::service(Depot::default(), Allowlist::anyone()).build())
        .spawn()
        .expect("distinct ALPNs register cleanly");

    let addr = server.endpoint().addr();
    let till_client = till::Client::connect(&client_node, addr.clone())
        .await
        .expect("dials");
    let depot_client = depot::Client::connect(&client_node, addr)
        .await
        .expect("dials");

    till_client
        .with_idempotency(key("shared"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();

    let run = depot_client
        .with_idempotency(key("shared"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .unwrap();

    assert_eq!(
        run, 1,
        "the second service must run, not answer from the first service's record"
    );
}

// ───────────────────────────────────────────────────────── misuse of the API

/// A scoped client carries its key into every call made through it, so it is
/// meant for one. Making a second, DIFFERENT call through it does not corrupt
/// anything: the record answers with the first call's reply, and the generated
/// client refuses a reply to a question it did not ask.
#[tokio::test]
async fn a_second_different_call_through_one_scoped_client_is_refused() {
    let fixture = start().await;
    let scoped = fixture.client.with_idempotency(key("k"));
    scoped.reserve("ADU-014".to_string(), 2).await.unwrap();

    let error = scoped
        .slow(1)
        .await
        .expect_err("the record answers Reserve, and this call asked Slow");

    assert!(
        matches!(error, Error::UnexpectedResponse { .. }),
        "the misuse must be caught, not silently mis-answered: got {error:?}"
    );
}
