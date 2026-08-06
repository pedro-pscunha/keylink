//! One actor, reached from this process and from another machine.
//!
//! RFC-04 capability A. The local [`Handle`] is the base case (§5.4); serving it
//! is an addition on top that changes **nothing on the wire** — the same ALPN,
//! the same `Request`, the same `Response`, and the ordinary `Client` on the
//! other end.
//!
//! Two iroh endpoints on loopback, no relay and no discovery.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use keylink::{
    Allowlist, Body, BodyReader, Caller, Context, Middleware, Next, Outcome, RemoteError, Server,
};

mod common;
use common::local_node;

keylink::service! {
    service Ledger;

    /// Holds the actor for `hold_ms`, so serialization is measurable.
    rpc  Post(entry: u64, hold_ms: u64) -> u64;
    rpc  Entries() -> Vec<u64>;
    rpc  WhoCalled() -> Option<String>;
    rpc  Boom() -> u64;
    tell Note(line: String) -> ();
    recv Export() -> u64;
}

// Bulk transfer, on its own service, because a `send` method CANNOT be served
// as an actor — `Handle::service` refuses to compile for a service that declares
// one. RFC-04 §10.4 has the reason: a `send` handler reads its upload inside the
// serial turn, so a peer that declares a large body and stops writing holds the
// actor until the connection idle timeout.
//
// `Ledger` used to declare this method, and this file used to prove the cost by
// measuring it. The cost is not measurable any more, because the shape is not
// reachable. (A `///` here would be an unused doc comment: rustdoc does not
// document a macro invocation.)
keylink::service! {
    service Depot;

    send Ingest(name: String) -> u64;
}

/// The obvious code: a `BTreeMap` behind no lock at all.
#[derive(Default)]
struct Ledger {
    entries: Vec<u64>,
    notes: BTreeMap<String, ()>,
    /// Set on entry to a turn and cleared on exit. If two turns ever overlap,
    /// the second one sees it set.
    inside: bool,
}

impl ledger::Actor for Ledger {
    async fn post(&mut self, _cx: Caller<'_>, entry: u64, hold_ms: u64) -> u64 {
        assert!(!self.inside, "two turns overlapped");
        self.inside = true;
        // `&mut self` is held across an await. Under the concurrent model this
        // is where `clippy::await_holding_lock` would stop the build.
        n0_future::time::sleep(Duration::from_millis(hold_ms)).await;
        self.entries.push(entry);
        self.inside = false;
        self.entries.len() as u64
    }

    async fn entries(&mut self, _cx: Caller<'_>) -> Vec<u64> {
        self.entries.clone()
    }

    /// The caller is an argument, so there is nothing to stash and nothing to
    /// go stale. The hook this replaced asked the state to remember the value
    /// between two calls, and the idiomatic way to write that —
    /// `if let Some(cx) = cx { self.last = Some(cx.peer()) }` — left the last
    /// remote peer in place for the next LOCAL message.
    async fn who_called(&mut self, cx: Caller<'_>) -> Option<String> {
        cx.peer().map(|cx| cx.peer().to_string())
    }

    async fn boom(&mut self, _cx: Caller<'_>) -> u64 {
        panic!("bad message");
    }

    async fn note(&mut self, _cx: Caller<'_>, line: String) {
        self.notes.insert(line, ());
    }

    async fn export(&mut self, _cx: Caller<'_>) -> (u64, Body) {
        let payload = format!("{:?}", self.entries);
        (payload.len() as u64, Body::from_bytes(payload.into_bytes()))
    }
}

/// The concurrent half of the split: uploads, behind an `Arc`, where a slow
/// writer costs its own caller and nobody else.
struct Depot;

impl depot::Handler for Depot {
    async fn ingest(&self, _cx: &Context, _name: String, mut body: BodyReader) -> u64 {
        let mut bytes = 0u64;
        while let Ok(Some(chunk)) = body.next_chunk().await {
            bytes += chunk.len() as u64;
        }
        bytes
    }
}

/// Serve one actor and dial it with the ORDINARY client.
async fn served() -> (Server, ledger::Handle<Ledger>, ledger::Client) {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor(Ledger::default());
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");
    (server, actor, client)
}

// ────────────────────────────────────────────────────────── the local handle

/// The base case, through the generated handle rather than the crate type:
/// state mutated with no lock, no id, no schema hash, no endpoint.
#[tokio::test]
async fn a_local_actor_answers_with_no_network_at_all() {
    let ledger = ledger::actor(Ledger::default());

    assert_eq!(ledger.post(7, 0).await.expect("running"), 1);
    assert_eq!(ledger.post(9, 0).await.expect("running"), 2);
    assert_eq!(ledger.entries().await.expect("running"), vec![7, 9]);
}

/// Two local calls never overlap. Made red by the assertion inside `post`.
#[tokio::test]
async fn two_local_calls_to_one_actor_never_overlap() {
    let ledger = ledger::actor(Ledger::default());

    let calls: Vec<_> = (0..8)
        .map(|n| {
            let ledger = ledger.clone();
            tokio::spawn(async move { ledger.post(n, 5).await })
        })
        .collect();

    for call in calls {
        call.await.expect("no turn overlapped").expect("running");
    }
    assert_eq!(ledger.entries().await.expect("running").len(), 8);
}

/// A local `tell` returns `()` and is ordered, which the remote one is not.
#[tokio::test]
async fn a_local_tell_returns_unit_and_keeps_its_order() {
    let ledger = ledger::actor(Ledger::default());

    // Compiles only because the generated method returns `Result<()>`.
    let unit: () = ledger.note("first".to_owned()).await.expect("running");
    assert_eq!(unit, ());
    ledger.note("second".to_owned()).await.expect("running");

    // The `entries` rpc is queued behind both tells, in this process, so it
    // cannot observe them half-applied.
    ledger.entries().await.expect("running");
}

/// The actor survives a panicking message, and the caller of that message gets
/// the panic — the way an ordinary function call would deliver it.
#[tokio::test]
async fn a_panicking_local_call_costs_one_message_and_reaches_its_caller() {
    let ledger = ledger::actor(Ledger::default());

    let boom = {
        let ledger = ledger.clone();
        tokio::spawn(async move { ledger.boom().await })
    };
    assert!(
        boom.await
            .expect_err("the panic reaches the caller")
            .is_panic(),
        "a local call must not swallow a panic into an error"
    );

    assert_eq!(
        ledger.post(1, 0).await.expect("the actor survived"),
        1,
        "the next message runs against the state as it stands"
    );
}

/// A handle to a stopped actor reports a closed mailbox. RFC-04 §12.1 test 5.
#[tokio::test]
async fn a_handle_to_a_stopped_actor_reports_a_closed_mailbox() {
    let ledger = ledger::actor(Ledger::default());
    let spare = ledger.clone();
    ledger.stop().await;

    assert_eq!(spare.post(1, 0).await, Err(keylink::MailboxClosed));
    assert!(!spare.is_running());
}

// ─────────────────────────────────────────────── the same actor, over QUIC

/// Capability A end to end, with the client that already existed.
#[tokio::test]
async fn a_served_actor_answers_the_ordinary_client() {
    let (server, _actor, client) = served().await;

    assert_eq!(client.post(7, 0).await.expect("answers"), 1);
    assert_eq!(client.entries().await.expect("answers"), vec![7]);

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// The load-bearing claim of capability A: **the wire does not move.** A
/// concurrent service and an actor service derive the same ALPN from the same
/// `service!` block, so a peer cannot tell them apart and does not need to.
#[test]
fn serving_an_actor_uses_the_same_alpn_as_serving_a_handler() {
    // `ledger::alpn()` is what both `service()` and `Handle::service()` pass to
    // `ServiceBuilder::new`. One value, read once, so this asserts the property
    // rather than restating a literal.
    assert!(String::from_utf8_lossy(ledger::alpn()).starts_with("keylink/Ledger/"));
}

/// One router owns one ALPN, so a service cannot be served both ways at once.
/// The refusal is at startup, not on the first confused call.
#[tokio::test]
async fn a_service_cannot_be_served_concurrently_and_as_an_actor_at_once() {
    let node = local_node().await;
    let actor = ledger::actor(Ledger::default());

    let error = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).build())
        .service(ledger::service(ConcurrentLedger, Allowlist::anyone()).build())
        .spawn()
        .expect_err("one ALPN, two services");

    assert!(
        matches!(
            error,
            keylink::Error::Config(keylink::ConfigError::DuplicateAlpn(_))
        ),
        "got {error:?}"
    );
}

/// The `Handler` twin. It exists to prove the ALPN collision above, where every
/// method is unreachable because it never gets to run — and to prove the other
/// half of caller identity, where `who_called` is served for real.
struct ConcurrentLedger;

impl ledger::Handler for ConcurrentLedger {
    async fn post(&self, _cx: &Context, _entry: u64, _hold_ms: u64) -> u64 {
        0
    }
    async fn entries(&self, _cx: &Context) -> Vec<u64> {
        Vec::new()
    }
    /// A `Handler` takes `&Context`, not `Caller`: it is reachable exactly one
    /// way, so "no peer" is not a state it has.
    async fn who_called(&self, cx: &Context) -> Option<String> {
        Some(cx.peer().to_string())
    }
    async fn boom(&self, _cx: &Context) -> u64 {
        0
    }
    async fn note(&self, _cx: &Context, _line: String) {}
    async fn export(&self, _cx: &Context) -> (u64, Body) {
        (0, Body::empty())
    }
}

/// The cost, measured rather than asserted: eight calls that each hold the
/// actor for 60 ms take at least eight times 60 ms.
///
/// This is `protocol.rs::concurrent_calls_do_not_queue_behind_each_other` with
/// the opposite expectation, and having both on record is what makes the trade
/// verifiable rather than asserted.
///
/// # `tokio::join!`, and why the obvious loop was wrong
///
/// The first version of this test built eight futures with `.map(..)` and then
/// awaited them in a `for` loop. **A Rust future does nothing until it is
/// polled**, so that ran eight sequential round trips and the assertion held
/// under *any* serving model — including the concurrent one. It was green, and
/// it proved nothing.
///
/// `join!` is what puts all eight in flight at once, which is exactly why the
/// test it mirrors uses it. Load can only push the elapsed time up, so this
/// assertion never fails for being run on a busy machine.
#[tokio::test]
async fn a_served_actor_serializes_calls_that_arrive_on_separate_streams() {
    const HOLD: u64 = 60;
    const CALLS: u64 = 8;

    let (server, _actor, client) = served().await;
    let started = Instant::now();
    let answers = tokio::join!(
        client.post(0, HOLD),
        client.post(1, HOLD),
        client.post(2, HOLD),
        client.post(3, HOLD),
        client.post(4, HOLD),
        client.post(5, HOLD),
        client.post(6, HOLD),
        client.post(7, HOLD),
    );
    let elapsed = started.elapsed();

    for answer in [
        answers.0, answers.1, answers.2, answers.3, answers.4, answers.5, answers.6, answers.7,
    ] {
        answer.expect("answers");
    }
    assert!(
        elapsed >= Duration::from_millis(HOLD * CALLS),
        "{CALLS} concurrent {HOLD}ms turns took {elapsed:?}; serialized they cannot"
    );
    server.shutdown().await.expect("stops");
}

/// RFC-04 §12.2 test 10, which was never written: the actor sees the proven key
/// of its caller. Made red by passing `None` to `caller` on the served path.
#[tokio::test]
async fn a_served_actor_sees_the_proven_key_of_its_caller() {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor(Ledger::default());
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    assert_eq!(
        client.who_called().await.expect("answers"),
        Some(caller.id().to_string()),
        "the actor must see the dialing peer's proven key"
    );

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// And the half `Caller::Local` exists for: a local message cannot be
/// attributed to a peer at all.
///
/// This used to be a property of *discipline* — the state was handed
/// `Option<&Context>` before each message and had to remember to assign rather
/// than merge, because `if let Some(cx) = cx { .. }` is the idiomatic spelling
/// and leaves the last remote peer in place. It is a property of the *type*
/// now: `Caller::Local` carries no peer, so there is nothing to inherit.
///
/// Made red by passing `Caller::Peer` on the local path.
#[tokio::test]
async fn a_local_call_has_no_caller() {
    let (server, actor, client) = served().await;

    assert!(
        client.who_called().await.expect("answers").is_some(),
        "the remote call sets it"
    );
    assert_eq!(
        actor.who_called().await.expect("running"),
        None,
        "and the local call must clear it"
    );

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// A panic must not unwind out of a connection's task. The remote caller gets a
/// refusal, and the actor is still there afterwards.
#[tokio::test]
async fn a_panicking_message_is_a_refusal_to_a_remote_caller() {
    let (server, _actor, client) = served().await;

    let error = client.boom().await.expect_err("the handler panicked");
    assert!(
        matches!(
            error,
            keylink::Error::Remote(RemoteError::Failed(ref reason))
                if reason.contains("bad message")
        ),
        "got {error:?}"
    );

    assert_eq!(
        client.post(1, 0).await.expect("the actor survived"),
        1,
        "one bad message must not cost the actor"
    );

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// The first of the three §14.8 costs the singleton does **not** pay: a layer
/// can still refuse one method and pass the rest, because the request reaching
/// it is the service's own typed enum rather than an opaque payload.
struct NoPosting;

impl Middleware<ledger::Request, ledger::Response> for NoPosting {
    fn handle(
        &self,
        cx: Context,
        req: ledger::Request,
        body: BodyReader,
        next: Next<ledger::Request, ledger::Response>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = (Outcome<ledger::Response>, Body)> + Send>,
    > {
        Box::pin(async move {
            if matches!(req, ledger::Request::Post { .. }) {
                return (
                    Outcome::Err(RemoteError::Failed("posting is closed".to_owned())),
                    Body::empty(),
                );
            }
            next(cx, req, body).await
        })
    }
}

#[tokio::test]
async fn a_layer_can_still_refuse_one_method_of_a_served_actor() {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor(Ledger::default());
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).layer(NoPosting).build())
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    assert!(
        client.post(1, 0).await.is_err(),
        "the layer refuses this one"
    );
    assert_eq!(
        client.entries().await.expect("and passes this one"),
        Vec::<u64>::new()
    );

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// A refused call must not consume a turn. If the layer ran inside the mailbox,
/// a refusal would cost the same time as a call — and every other caller would
/// wait behind it.
#[tokio::test]
async fn a_refused_call_does_not_reach_the_mailbox() {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor(Ledger::default());
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).layer(NoPosting).build())
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    let _ = client.post(1, 0).await;
    assert_eq!(
        actor.entries().await.expect("running"),
        Vec::<u64>::new(),
        "a refused call must never reach the handler"
    );

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// `recv` streams from a served actor, and the body is drained by the
/// connection AFTER the turn ends — so a download does not hold the mailbox.
#[tokio::test]
async fn a_served_actor_streams_a_body_back() {
    let (server, _actor, client) = served().await;
    client.post(7, 0).await.expect("answers");

    let (len, mut body) = client.export().await.expect("answers");
    let mut received = Vec::new();
    while let Some(chunk) = body.next_chunk().await.expect("streams") {
        received.extend_from_slice(&chunk);
    }

    assert_eq!(received.len() as u64, len);
    assert_eq!(String::from_utf8_lossy(&received), "[7]");

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// An upload goes to a **second service**, because that is now the only shape
/// available: `Handle::service` does not compile for a service declaring a
/// `send` method.
///
/// This test used to serve `send Ingest` on the actor itself and measure the
/// cost. Measuring a hazard is weaker than removing it — RFC-04 §10.4 explains
/// why a `send` handler must read its upload inside the turn, and no layer can
/// bound that, so a peer that declared a large body and stopped writing held
/// the actor until iroh's idle timeout with nothing else answered, including
/// the local handle.
///
/// The split is what `README.md` already recommended for bulk transfer. It is
/// mandatory now, and this is what it looks like: two services, two ALPNs, one
/// node. The compile-time refusal itself is proven by a `compile_fail` doctest
/// on `Handle::service`.
#[tokio::test]
async fn an_upload_goes_to_a_second_service_beside_the_actor() {
    const SIZE: usize = 64 * 1024;

    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor(Ledger::default());
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).build())
        .service(depot::service(Depot, Allowlist::anyone()).build())
        .spawn()
        .expect("two services, two ALPNs, one node");

    let uploads = depot::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");
    let stored = uploads
        .ingest("sheet.bin".to_owned(), Body::from_bytes(vec![7u8; SIZE]))
        .await
        .expect("answers");
    assert_eq!(stored as usize, SIZE);

    // And the actor was never involved, which is the point of the split: an
    // upload cannot hold a mailbox it does not touch.
    assert_eq!(actor.post(1, 0).await.expect("running"), 1);

    uploads.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// One mailbox, two doors. A local call and a remote call to the same actor are
/// serialized against each other, not merely against their own kind.
#[tokio::test]
async fn local_and_remote_messages_share_one_mailbox() {
    let (server, actor, client) = served().await;
    let seen = Arc::new(AtomicUsize::new(0));

    let remote = {
        let seen = seen.clone();
        tokio::spawn(async move {
            let answer = client.post(1, 80).await.expect("answers");
            seen.fetch_add(1, Ordering::SeqCst);
            (answer, client)
        })
    };

    // Give the remote call time to be the one in the mailbox.
    n0_future::time::sleep(Duration::from_millis(20)).await;
    let local = actor.post(2, 0).await.expect("running");
    let (remote_answer, client) = remote.await.expect("no panic");

    assert_eq!(remote_answer, 1, "the remote call took the first turn");
    assert_eq!(local, 2, "and the local call waited for it");

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

// ────────────────────────────────────────── the handler half of the identity

/// A `Handler` reads the calling peer, which it could not do at all before: the
/// proven identity reached the edge of the generated dispatch and was thrown
/// away there, so a concurrent service that wanted to audit or authorize per
/// caller had to take a peer id in the request body — a claim rather than a
/// proof.
#[tokio::test]
async fn a_handler_sees_the_calling_peer() {
    let node = local_node().await;
    let caller = local_node().await;
    let server = ledger::serve(&node, ConcurrentLedger, Allowlist::anyone()).expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    assert_eq!(
        client.who_called().await.expect("answers"),
        Some(caller.id().to_string()),
        "the handler must see the dialing peer's proven key"
    );

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

// ───────────────────────────────────────────────────── shedding queued load

/// `QueueTimeout` refuses a message that is still **queued** and never one that
/// has started, which is the whole difference from `Timeout`.
///
/// `Timeout` on this path drops the wait for the reply and the turn runs to
/// completion anyway, so ten callers get `TimedOut` and the actor still spends
/// ten turns producing replies nobody wants. This one refuses before the work
/// starts, so the load is genuinely shed.
///
/// The proof is the run counter: one turn was held for 400 ms, the second
/// message waited behind it with a 100 ms budget and was refused, and the
/// actor's own count says it ran once.
#[tokio::test]
async fn a_queue_timeout_refuses_before_the_mailbox() {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor(Ledger::default());
    let server = Server::builder(&node)
        .service(
            actor
                .service(Allowlist::anyone())
                .layer(keylink::QueueTimeout::after(Duration::from_millis(100)))
                .build(),
        )
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    let held = {
        let client = client.clone();
        tokio::spawn(async move { client.post(1, 400).await })
    };
    // Long enough that the first message is IN its turn, so the second one is
    // queued behind it rather than racing it for the permit.
    n0_future::time::sleep(Duration::from_millis(80)).await;

    let refused = client
        .post(2, 0)
        .await
        .expect_err("the second message is still queued when its budget expires");
    assert!(
        matches!(
            refused,
            keylink::Error::Remote(RemoteError::TimedOut { .. })
        ),
        "got {refused:?}"
    );

    assert_eq!(held.await.expect("no panic").expect("answers"), 1);
    assert_eq!(
        actor.entries().await.expect("running"),
        vec![1],
        "the refused message must not have reached the state"
    );

    server.shutdown().await.expect("stops");
}

/// The other half, and the one that makes `QueueTimeout` safe to combine with
/// an idempotency key: a message it refuses never ran, so the key is still
/// claimable. `Timeout` cannot promise that, because it cannot tell.
#[tokio::test]
async fn a_queue_timeout_leaves_an_idempotency_key_claimable() {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor(Ledger::default());
    let server = Server::builder(&node)
        .service(
            actor
                .service(Allowlist::anyone())
                .layer(keylink::QueueTimeout::after(Duration::from_millis(100)))
                .build(),
        )
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");
    let key = keylink::IdempotencyKey::new("post-1").expect("a valid key");

    let held = {
        let client = client.clone();
        tokio::spawn(async move { client.post(1, 400).await })
    };
    n0_future::time::sleep(Duration::from_millis(80)).await;

    let _refused = client
        .with_idempotency(key.clone())
        .post(2, 0)
        .await
        .expect_err("refused while queued");
    assert_eq!(held.await.expect("no panic").expect("answers"), 1);

    assert_eq!(
        client
            .with_idempotency(key)
            .post(2, 0)
            .await
            .expect("the key must still be claimable, because nothing ran under it"),
        2,
    );

    server.shutdown().await.expect("stops");
}
