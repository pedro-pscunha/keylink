//! Retry safety over a real QUIC connection.
//!
//! Every handler here reports the number of times it has run, so a reply is
//! itself the evidence: the same number twice means the handler was not entered
//! again, and no test has to reach inside the serving side to find out.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

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

mod common;
use common::local_node;

struct Fixture {
    client: till::Client,
    _server: KeylinkServer,
    _nodes: Vec<Node>,
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

/// The sku `RefuseAfterRunning` reacts to. Anything else passes untouched, so
/// the handler's run counter stays readable through an ordinary reply — which
/// is how a test proves a repeat replayed instead of re-running.
const REFUSED_SKU: &str = "REFUSE-ME";

/// Lets the call through and then throws the reply away, which is what a layer
/// doing post-hoc filtering looks like. The handler HAS run, so the refusal is
/// the call's real and final answer.
struct RefuseAfterRunning;

impl<Resp: Send + 'static> Middleware<till::Request, Resp> for RefuseAfterRunning {
    fn handle(
        &self,
        cx: Context,
        req: till::Request,
        body: BodyReader,
        next: Next<till::Request, Resp>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = (Outcome<Resp>, Body)> + Send>> {
        let refuse = matches!(&req, till::Request::Reserve { sku, .. } if sku == REFUSED_SKU);
        Box::pin(async move {
            let reply = next(cx, req, body).await;
            if refuse {
                return (
                    Outcome::Err(RemoteError::Unauthorized("after the fact".into())),
                    Body::empty(),
                );
            }
            reply
        })
    }
}

/// **Restated.** This test used to assert that *every* refusal is recorded,
/// and it passed for a layer that refused without ever reaching the handler —
/// which recorded, and replayed forever, an answer to a call that never ran.
///
/// The rule is one *execution* per key, not one *answer* per key. So the
/// question a refusal has to answer first is whether anything ran. This is the
/// arm where something did: the layer let the call through, the handler
/// executed, and the refusal is the final answer to a call that happened. That
/// answer is recorded and replayed, exactly as a success would be, because a
/// retry must not run the work again.
///
/// The other two arms are the two tests below.
#[tokio::test]
async fn a_refusal_is_recorded_and_replayed() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(
            till::service(Till::default(), Allowlist::anyone())
                .layer(RefuseAfterRunning)
                .build(),
        )
        .spawn()
        .expect("serves");
    let client = till::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("dials");

    let first = client
        .with_idempotency(key("k"))
        .reserve(REFUSED_SKU.to_string(), 2)
        .await
        .expect_err("the layer refuses after the handler ran");
    assert!(
        matches!(first, Error::Remote(RemoteError::Unauthorized(_))),
        "got {first:?}"
    );

    let repeat = client
        .with_idempotency(key("k"))
        .reserve(REFUSED_SKU.to_string(), 2)
        .await
        .expect_err("the repeat must replay the refusal, not run the handler again");
    assert!(
        matches!(repeat, Error::Remote(RemoteError::Unauthorized(_))),
        "got {repeat:?}"
    );

    // The proof that it replayed rather than re-ran, and the reason the layer
    // only refuses one sku: this call reports how many times the handler has
    // been entered. Two — the refused call and this one. If the repeat had
    // re-entered it, this would be three.
    assert_eq!(
        client
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("this sku is not refused"),
        2,
        "the repeat must not have reached the handler"
    );
}

/// Nothing ran, so nothing is owed. This is the headline defect: a layer that
/// refuses **before** dispatch used to settle the key as that refusal, so the
/// caller was told `Unauthorized` forever — for a call that had never happened,
/// by a rule that had since stopped applying.
#[tokio::test]
async fn a_refusal_before_dispatch_leaves_the_key_claimable() {
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

    let refused = client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("the layer refuses the first call without dispatching it");
    assert!(
        matches!(refused, Error::Remote(RemoteError::Unauthorized(_))),
        "got {refused:?}"
    );

    assert_eq!(
        client
            .with_idempotency(key("k"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("the same key must be claimable again, because nothing ran under it"),
        1,
        "the handler must be entered for the first time here"
    );
}

/// The third arm: dispatched, and no answer of its own came out. A `Timeout`
/// drops the wait for a handler that is still running, so whether the work
/// landed is genuinely unknown — and `Interrupted` is the only honest thing to
/// record. Recording the layer's own `TimedOut` would tell the caller a lie
/// with a straight face, forever.
#[tokio::test]
async fn a_refusal_after_dispatch_settles_interrupted() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(
            till::service(Till::default(), Allowlist::anyone())
                .layer(keylink::Timeout::after(Duration::from_millis(50)))
                .build(),
        )
        .spawn()
        .expect("serves");
    let client = till::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("dials");

    let timed_out = client
        .with_idempotency(key("slow"))
        .slow(5_000)
        .await
        .expect_err("the budget expires while the handler runs");
    assert!(
        matches!(timed_out, Error::Remote(RemoteError::TimedOut { .. })),
        "got {timed_out:?}"
    );

    let repeat = client
        .with_idempotency(key("slow"))
        .slow(0)
        .await
        .expect_err("the repeat must not be answered with the layer's refusal");
    assert!(
        matches!(
            repeat,
            Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Interrupted))
        ),
        "a dispatched call with no answer is unknown, not timed out: got {repeat:?}"
    );
}

/// A serving task killed between the accept and the dispatch never entered
/// anything, so its key is vacant rather than `Interrupted`.
///
/// The kill is real, not simulated: the client's connection ends while the
/// layer is still deciding, the accept loop breaks, and `calls.shutdown()`
/// aborts the task where it stands. `Drop for Guard` is then the only code left
/// running, which is exactly why the decision lives there rather than at the
/// call site.
#[tokio::test]
async fn an_abort_before_dispatch_does_not_report_interrupted() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = keylink::Server::builder(&server)
        .service(
            till::service(Till::default(), Allowlist::anyone())
                .layer(StallBeforeDispatch(AtomicU64::new(0)))
                .build(),
        )
        .spawn()
        .expect("serves");

    {
        let client = till::Client::connect(&client_node, server.endpoint().addr())
            .await
            .expect("dials");
        let scoped = client.with_idempotency(key("k"));
        let stalling = scoped.reserve("ADU-014".to_string(), 2);
        tokio::select! {
            _ = stalling => panic!("the layer should still be stalling"),
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        // Every handle goes here, which closes the connection.
    }
    // Long enough for the accept loop to see the close and abort the call.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let reconnected = till::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("dials again");
    assert_eq!(
        reconnected
            .with_idempotency(key("k"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("a key nothing dispatched under must still be claimable"),
        1,
        "the aborted call never entered the handler, so this is its first entry"
    );
}

/// Stalls the first call it sees for longer than the test waits, without ever
/// calling `next`. Everything after it passes straight through.
struct StallBeforeDispatch(AtomicU64);

impl<Resp: Send + 'static> Middleware<till::Request, Resp> for StallBeforeDispatch {
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
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            next(cx, req, body).await
        })
    }
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

// ────────────────────────────────────────── the ledger, asked rather than run

/// The wire path for a ledger query, which has no handler, no layer and no body.
///
/// `Fresh` is the state this exists for: every other one can be discovered by
/// retrying, and discovering `Fresh` by retrying means executing. The exhaustive
/// per-state coverage is the unit test `key_state_reports_every_state`, which
/// can reach eviction; this proves the question and answer survive the wire.
#[tokio::test]
async fn a_key_state_query_crosses_the_wire() {
    let fixture = start().await;

    assert_eq!(
        fixture.client.key_state(key("never")).await.expect("asks"),
        keylink::KeyState::Fresh
    );

    fixture
        .client
        .with_idempotency(key("used"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("runs");

    assert_eq!(
        fixture.client.key_state(key("used")).await.expect("asks"),
        keylink::KeyState::Done
    );
    assert_eq!(
        fixture.client.key_state(key("never")).await.expect("asks"),
        keylink::KeyState::Fresh,
        "asking must not itself claim the key"
    );
}

/// A key one peer used must be invisible to another, and the query is the one
/// place a peer could go looking. Peer-scoped by construction rather than by a
/// check: the id comes from the connection the question arrived on.
#[tokio::test]
async fn a_key_state_query_cannot_read_another_peers_key() {
    let server = local_node().await;
    let first_node = local_node().await;
    let second_node = local_node().await;
    let _running = till::serve(&server, Till::default(), Allowlist::anyone()).expect("serves");

    let first = till::Client::connect(&first_node, server.endpoint().addr())
        .await
        .expect("dials");
    let second = till::Client::connect(&second_node, server.endpoint().addr())
        .await
        .expect("dials");

    first
        .with_idempotency(key("mine"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("runs");

    assert_eq!(
        first.key_state(key("mine")).await.expect("asks"),
        keylink::KeyState::Done
    );
    assert_eq!(
        second.key_state(key("mine")).await.expect("asks"),
        keylink::KeyState::Fresh,
        "one peer must not learn that another used a key"
    );
}

// ───────────────────────────────────────────────────────────────── attaching

/// A second caller under a live key takes the running call's answer instead of
/// being turned away. The two replies are the same run number, and a third,
/// keyless call reports that the handler was entered exactly twice.
#[tokio::test]
async fn a_second_caller_attaches_to_a_live_key() {
    let fixture = start().await;
    let client = fixture.client;

    let slow = {
        let scoped = client.with_idempotency(key("k"));
        tokio::spawn(async move { scoped.slow(400).await })
    };
    // Long enough that the claim is taken and the handler is inside its sleep.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let attached = client
        .with_idempotency(keylink::Idempotency::attaching_for(
            key("k"),
            Duration::from_secs(5),
        ))
        .slow(0)
        .await
        .expect("the attaching caller is answered by the call already running");

    let first = slow
        .await
        .expect("no panic")
        .expect("the first call answers");
    assert_eq!(
        attached, first,
        "the attaching caller must receive the running invocation's own reply"
    );
    assert_eq!(
        client.slow(0).await.expect("runs"),
        2,
        "the handler ran twice: the original and this one - not three times"
    );
}

/// Attaching is bounded. When the budget runs out the answer is `InFlight`,
/// which is what a non-attaching caller gets and is still the truth: a call
/// under this key is in dispatch now.
#[tokio::test]
async fn an_attach_that_outlives_its_budget_returns() {
    let fixture = start().await;
    let client = fixture.client;

    let slow = {
        let scoped = client.with_idempotency(key("k"));
        tokio::spawn(async move { scoped.slow(3_000).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let error = client
        .with_idempotency(keylink::Idempotency::attaching_for(
            key("k"),
            Duration::from_millis(150),
        ))
        .slow(0)
        .await
        .expect_err("the budget runs out while the first call is still running");

    assert_eq!(refusal(&error), IdempotencyRefusal::InFlight);
    slow.abort();
}

/// A settled key is answered at once. Attaching is about waiting for a call in
/// flight, and there is nothing in flight here — waiting out the budget before
/// replaying a record that already exists would be a bug that only ever showed
/// up as latency.
#[tokio::test]
async fn attaching_to_a_settled_key_does_not_wait() {
    let fixture = start().await;
    let first = fixture
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("runs");

    let started = std::time::Instant::now();
    let replayed = fixture
        .client
        .with_idempotency(keylink::Idempotency::attaching_for(
            key("k"),
            Duration::from_secs(30),
        ))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("replays");

    assert_eq!(replayed, first);
    // A generous bound: the assertion is "it did not wait out 30 seconds", and
    // load can only make a real network call slower, never faster.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "a settled key must be answered without waiting: took {:?}",
        started.elapsed()
    );
}

// ─────────────────────────────────────────────── the actor path and a Timeout

#[derive(Default)]
struct TillActor {
    reserves: u64,
    slows: u64,
}

impl till::Actor for TillActor {
    async fn reserve(&mut self, _sku: String, _qty: u32) -> u64 {
        self.reserves += 1;
        self.reserves
    }

    async fn slow(&mut self, ms: u64) -> u64 {
        self.slows += 1;
        let run = self.slows;
        tokio::time::sleep(Duration::from_millis(ms)).await;
        run
    }

    async fn datasheet(&mut self, bytes: u64) -> (Sheet, Body) {
        (Sheet { runs: 0, bytes }, Body::from_bytes(vec![0u8; 0]))
    }
}

/// The defect that made a key and a `Timeout` unusable together on an actor.
///
/// A layer cannot cancel a turn — it drops the *wait* for the reply, and the
/// turn runs on and mutates. The refusal used to be recorded as the key's
/// settled answer, so the caller was told `TimedOut` on every retry forever
/// while the actor had applied the message exactly once.
///
/// Now the turn's own progress decides: it was entered and produced no answer
/// anybody received, so the key is `Interrupted` — which is the truth, and
/// which tells the caller to find out out of band instead of trusting a lie.
///
/// **Not** promised, and deliberately: the retry does not receive the turn's
/// real reply. That needs the guard to travel inside the mailbox and be settled
/// by the turn itself. See RFC-05 §7.
#[tokio::test]
async fn a_timed_out_actor_turn_does_not_record_the_refusal() {
    let server = local_node().await;
    let client_node = local_node().await;
    let actor = till::actor(TillActor::default());
    let _running = keylink::Server::builder(&server)
        .service(
            actor
                .service(Allowlist::anyone())
                .layer(keylink::Timeout::after(Duration::from_millis(50)))
                .build(),
        )
        .spawn()
        .expect("serves");
    let client = till::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("dials");

    let timed_out = client
        .with_idempotency(key("k"))
        .slow(500)
        .await
        .expect_err("the budget expires while the turn runs");
    assert!(
        matches!(timed_out, Error::Remote(RemoteError::TimedOut { .. })),
        "got {timed_out:?}"
    );

    // Let the turn finish, so the state really has been mutated by the time the
    // retry asks. This is the half a `TimedOut` record used to hide.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let repeat = client
        .with_idempotency(key("k"))
        .slow(0)
        .await
        .expect_err("the repeat must not be told the layer's refusal");
    assert_eq!(
        refusal(&repeat),
        IdempotencyRefusal::Interrupted,
        "a turn that ran and whose reply nobody received is unknown, not timed out"
    );

    assert_eq!(
        actor
            .with_state(|state| state.slows)
            .await
            .expect("running"),
        1,
        "the turn ran exactly once, which is what the caller could not be told before"
    );
}
