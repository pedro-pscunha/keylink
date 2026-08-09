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
    Allowlist, Body, BodyReader, Caller, Context, Middleware, Next, Observer, Outcome, RemoteError,
    Server, Stop, TurnFut, TurnInfo, TurnOutcome,
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
    ledger
        .stop()
        .await
        .expect("a stop from outside the actor's own turn is never refused");

    assert_eq!(spare.post(1, 0).await, Err(keylink::Unanswered::Gone));
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

/// The budget both bounds below are given, and a hold that outlasts it several
/// times over. A message queued behind that hold has provably run out of budget
/// before the actor is free again, whichever bound is in front of it.
const BUDGET: Duration = Duration::from_millis(100);
const HOLD: u64 = 400;

/// The three facts one keyed message behind a held turn leaves behind.
///
/// Three fields rather than one entry list, because the two bounds now agree on
/// the list and differ in everything leading to it. Collapsing them would leave
/// each of the properties below pinned by the accident that the others hold.
struct BehindAHeldTurn {
    /// What the actor had run once the holding turn was over and the refused
    /// caller had long since been answered — read **before** the retry.
    ///
    /// Its own assertion, and not a step towards the last one, because
    /// `turns.rs` heads a section "The turn runs even if you stop waiting": *a
    /// caller that gives up on a `reserve` has still reserved*. No test drops an
    /// `ask` future and checks that; this line is the whole of what pins it,
    /// and until the fix below it was pinned only by the doubled entry of a
    /// defect.
    before_the_retry: Vec<u64>,
    /// What the retry under the same key was told. The bounds differ here and
    /// nowhere else that a caller can see.
    retry: Result<u64, keylink::Error>,
    /// What the actor had run in the end.
    entries: Vec<u64>,
}

/// One turn holds the actor; a second message queues behind it under an
/// idempotency key, outlives `BUDGET`, and is retried under the same key.
/// Answers on the actor's own entry list, which records what **ran** rather than
/// what any caller was told.
///
/// The holding turn is a **local** call, so it never meets the layer: the bound
/// under test then applies to exactly one message and the two runs differ in
/// nothing else. That both doors share one mailbox is a separate property, and
/// `local_and_remote_messages_share_one_mailbox` is where it is pinned.
///
/// Every entry list here is read through a **local** call issued after the
/// message it reports on was already in the mailbox, so FIFO decides what it
/// sees. Nothing waits on a clock to decide whether a turn has run.
async fn entries_behind_a_held_turn(
    bound: impl Middleware<ledger::Request, ledger::Response>,
) -> BehindAHeldTurn {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor(Ledger::default());
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).layer(bound).build())
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");
    let key = keylink::IdempotencyKey::new("post-2").expect("a valid key");

    let held = {
        let actor = actor.clone();
        tokio::spawn(async move { actor.post(1, HOLD).await })
    };
    // Long enough that the held message is IN its turn, so the next one queues
    // behind it rather than racing it for the permit.
    n0_future::time::sleep(Duration::from_millis(80)).await;

    let refused = client
        .with_idempotency(key.clone())
        .post(2, 0)
        .await
        .expect_err("the queued message outlives its budget behind the held turn");
    assert!(
        matches!(
            refused,
            keylink::Error::Remote(RemoteError::TimedOut { .. })
        ),
        "both bounds refuse this message, and the difference is what the actor does about it \
         afterwards; got {refused:?}"
    );
    assert_eq!(held.await.expect("no panic").expect("running"), 1);

    // Local, and issued behind the refused message, so it reports whether that
    // message ran after its caller had already given up on it.
    let before_the_retry = actor.entries().await.expect("running");

    let retry = client.with_idempotency(key).post(2, 0).await;

    // Local, and issued last, so it sits behind every message above in the one
    // mailbox. FIFO, not a wall-clock guess about when the refused turn ran.
    let entries = actor.entries().await.expect("running");
    server.shutdown().await.expect("stops");
    BehindAHeldTurn {
        before_the_retry,
        retry,
        entries,
    }
}

/// The two bounds through one scenario: each runs a keyed message **once**, and
/// they differ in which retry is allowed to be that once.
///
/// `Timeout` sits outside the mailbox, so its budget expiring drops the wait for
/// the reply and not the turn: the caller reads `TimedOut` while the message is
/// queued, and the turn runs anyway. The key therefore settles `Interrupted` and
/// the retry is refused — the honest answer, because nobody can say from out
/// there whether the work landed. `QueueTimeout` is re-read on the actor's own
/// task, so a message it refuses genuinely never runs and giving the key back is
/// the truth: the retry is a first attempt and it is what does the work.
///
/// # This inverts a pin that recorded a defect
///
/// The `Timeout` half used to assert `[1, 2, 2]` and a retry that succeeded:
/// nothing marked a still-queued message, the ledger gave the key back, the turn
/// ran, and the retry ran the same work a second time — at-least-once, out of
/// the module whose one promise is at-most-once. That pin asked to be changed
/// deliberately by whoever fixed the behaviour, and RFC-04 §16.12 is that
/// commit: the progress marker records reaching a mailbox as its own fact, which
/// an arm that knows nothing ran then overrides.
///
/// Still **one** test, for the reason the pin gave: either half alone says
/// nothing about the difference between the two bounds, and that difference is
/// the whole of what `Timeout`'s and `QueueTimeout`'s rustdoc promise a reader.
/// It is now a difference in the key and not in the work.
#[tokio::test]
async fn a_timeout_settles_the_key_it_gave_up_on_where_a_queue_timeout_leaves_it_claimable() {
    let timed_out = entries_behind_a_held_turn(keylink::Timeout::after(BUDGET)).await;
    assert_eq!(
        timed_out.before_the_retry,
        vec![1, 2],
        "the turn ran after its caller was told TimedOut, which is `turns.rs`'s promise that \
         dropping the wait does not cancel the turn"
    );
    let refused = timed_out
        .retry
        .expect_err("the key of a message that was queued when its caller gave up must settle");
    assert!(
        matches!(
            refused,
            keylink::Error::Remote(RemoteError::Idempotency(
                keylink::IdempotencyRefusal::Interrupted
            ))
        ),
        "a caller that cannot be told whether its work landed must be refused rather than \
         invited to run it again; got {refused:?}"
    );
    assert_eq!(
        timed_out.entries,
        vec![1, 2],
        "the refused retry must not have run the work a second time"
    );

    let queued = entries_behind_a_held_turn(keylink::QueueTimeout::after(BUDGET)).await;
    assert_eq!(
        queued.before_the_retry,
        vec![1],
        "a message refused on the actor's own task never ran, which is what makes releasing \
         its key the truth"
    );
    assert_eq!(
        queued
            .retry
            .expect("the key must still be claimable, because the refusal proved nothing ran"),
        2,
    );
    assert_eq!(
        queued.entries,
        vec![1, 2],
        "the retry under the released key is the only time the work happened"
    );
}

// ─────────────────────────────────────── what a lost turn tells a remote peer

/// The name the observer below answers to, and therefore the exact text a peer
/// reads after `observer refused: `.
///
/// A constant so the test asserts the value that reached the wire rather than a
/// second copy of a literal that could drift from it.
const REFUSER: &str = "refuses-completed-turns";

/// Refuses every turn that **completed**, and passes every turn that panicked.
///
/// Both halves are load-bearing. Passing the panicked turn is what leaves the
/// actor alive for the second call: an observer that refused it would end the
/// actor after the first message, and the refusal call would then come back
/// `NoSuchActor` *legitimately* — which is precisely the answer the test is
/// trying to prove does not happen, so the test would prove nothing.
///
/// The `Stop` reason never reaches the wire; only [`Observer::name`] does. That
/// is why the name is the thing worth choosing, and the thing worth asserting.
struct RefusesWhatCompleted;

impl Observer<Ledger> for RefusesWhatCompleted {
    fn name(&self) -> &'static str {
        REFUSER
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, turn: TurnInfo<'a>) -> TurnFut<'a> {
        Box::pin(async move {
            match turn.outcome {
                TurnOutcome::Completed => Err(Stop::new("nothing was committed")),
                TurnOutcome::Panicked { .. } => Ok(()),
            }
        })
    }
}

/// The `Failed` reason a peer actually read.
///
/// A helper rather than a `matches!` guard, because the useful thing to say on
/// failure is **which** answer arrived instead — `NoSuchActor` above all, which
/// a bare `matches!` would report as a silent `false`.
fn failed_reason(error: &keylink::Error) -> &str {
    match error {
        keylink::Error::Remote(RemoteError::Failed(reason)) => reason,
        other => panic!("expected a RemoteError::Failed, got {other:?}"),
    }
}

/// A turn refused by an observer reaches a remote caller as
/// [`RemoteError::Failed`] carrying `observer refused: {name}`, and a turn that
/// panicked reaches it as `Failed` carrying `panicked: {message}`. One test,
/// both prefixes, because either one on its own proves nothing.
///
/// # Why the refusal must not be `NoSuchActor`
///
/// `NoSuchActor` means "there is nothing here, stop asking", and a peer that
/// reads it is right to give up on the address. A refused turn is the opposite
/// fact: the actor was there, the turn **ran**, and it was not committed. The
/// actor is ending, but it ended *because of this message*, and answering with
/// "no such actor" sends a peer away from an actor that had just answered it —
/// and hides the reason it stopped, which `Handle::ended` names in full.
///
/// # Why both prefixes belong in one test
///
/// `RemoteError::Failed` carries a `String` and not a variant — a variant is
/// inside the hashed contract and would move the ALPN — so the **string is the
/// whole discriminator** and a peer has nothing else to match on. Prefixing
/// only the refusal is therefore a guard that cannot fail: `Display for Panic`
/// writes its message verbatim, and panic text is usually caller-derived
/// (`panic!("{}", input)`), so a handler that panicked with the refusal's exact
/// text would hand a peer a byte-identical `Failed` and no way to tell which of
/// the two happened.
///
/// What keeps that from being reachable *today* is not the wire format at all:
/// `Panic::caught` in `panic.rs` builds every message as
/// `the actor panicked: {payload}`, so the panic side arrives already marked.
/// That is a `pub(crate)` detail of another module, invisible from here and
/// free to change — and if it did change, a test that pinned only the refusal
/// would stay green while the two answers quietly became indistinguishable.
/// Both prefixes are one property about a pair, so they are pinned together,
/// here, by one test that fails when either side loses its own.
#[tokio::test]
async fn both_halves_of_a_lost_turn_carry_their_own_prefix() {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor_with(Ledger::default(), vec![Box::new(RefusesWhatCompleted)]);
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    // The panicking call first, and the order is not cosmetic: the observer
    // passes a panicked turn, so the actor is still there to be refused below.
    let panicked = client.boom().await.expect_err("the handler panicked");
    let panicked_reason = failed_reason(&panicked).to_owned();
    assert!(
        panicked_reason.starts_with("panicked: "),
        "a panic must reach a peer under its own prefix, or it cannot be told from a refusal; \
         got {panicked_reason:?}"
    );
    assert!(
        panicked_reason.contains("bad message"),
        "and it must still carry what the handler panicked with, or the prefix is all a peer \
         gets; got {panicked_reason:?}"
    );

    let refused = client
        .post(1, 0)
        .await
        .expect_err("the observer refused the completed turn");
    assert!(
        !matches!(
            refused,
            keylink::Error::Remote(RemoteError::NoSuchActor { .. })
        ),
        "a refused turn must not be reported as NoSuchActor: the actor was there, the turn ran, \
         and telling a peer to stop asking is a different fact; got {refused:?}"
    );
    // Exact, because every byte of this one is chosen here: the format string
    // in the dispatch and `REFUSER`, with nothing in between.
    assert_eq!(
        failed_reason(&refused),
        format!("observer refused: {REFUSER}"),
        "a refusal must reach a peer under its own prefix, naming the observer that made it"
    );
    assert_ne!(
        panicked_reason,
        failed_reason(&refused),
        "the two answers a peer can only tell apart by their text must not be the same text"
    );

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

/// The name the panicking observer answers to, and therefore the exact text a
/// peer reads after `observer panicked: `.
const PANICKER: &str = "panics-on-completed-turns";

/// Panics on every turn that **completed**, and passes every turn that panicked.
///
/// The same shape as [`RefusesWhatCompleted`] and for the same reason, one arm
/// apart: it panics where that one returns [`Stop`]. Keeping the two structurally
/// identical is what makes the assertion below about the **fold** rather than
/// about two unrelated fixtures.
struct PanicsOnWhatCompleted;

impl Observer<Ledger> for PanicsOnWhatCompleted {
    fn name(&self) -> &'static str {
        PANICKER
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, turn: TurnInfo<'a>) -> TurnFut<'a> {
        Box::pin(async move {
            match turn.outcome {
                TurnOutcome::Completed => panic!("the observer fell over"),
                TurnOutcome::Panicked { .. } => Ok(()),
            }
        })
    }
}

/// An observer that **panicked** reaches a peer under its own prefix, and not
/// under the refusal's.
///
/// **A peer is the worst-informed audience for this fact and it had the same
/// false word.** A local caller can await [`ended`](keylink::LocalActor::ended)
/// and read the panic message out of `Ended::Observer`; a peer has no such second
/// future, so the `Failed` string is the whole of what it learns. It read
/// `observer refused: {name}` about an observer that decided nothing.
///
/// # Three texts, pinned against each other rather than one at a time
///
/// The prefix is the only discriminator a peer has — `RemoteError::Failed`
/// carries a `String` because a variant would move the ALPN — so a prefix that
/// cannot be told from its neighbours discriminates nothing. Two neighbours
/// matter here and both are asserted: `observer refused: `, which this case was
/// folded into, and `panicked: `, which is the **handler** exploding and shares
/// the verb. Asserting only the new text would stay green if it were a prefix of
/// either.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `rendered`'s new
/// arm. Search
/// `Outcome::Err(RemoteError::Failed(format!("{OBSERVER_PANICKED}{name}"))),`
/// and send it back out under the refusal's prefix instead:
/// `Outcome::Err(RemoteError::Failed(format!("{REFUSED}{name}"))),`. Watched:
/// `left: "observer refused: panics-on-completed-turns"`,
/// `right: "observer panicked: panics-on-completed-turns"`.
#[tokio::test]
async fn an_observer_that_panicked_reaches_a_peer_under_its_own_prefix() {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor_with(Ledger::default(), vec![Box::new(PanicsOnWhatCompleted)]);
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");

    let crashed = client
        .post(1, 0)
        .await
        .expect_err("the observer panicked on the completed turn");
    assert!(
        !matches!(
            crashed,
            keylink::Error::Remote(RemoteError::NoSuchActor { .. })
        ),
        "the actor was there and the turn ran, so telling a peer to stop asking is a different \
         fact; got {crashed:?}"
    );
    // Exact, because every byte is chosen here: the format string in the dispatch
    // and `PANICKER`, with nothing in between.
    assert_eq!(
        failed_reason(&crashed),
        format!("observer panicked: {PANICKER}"),
        "an observer that fell over must reach a peer under its own prefix, naming it"
    );
    assert!(
        !failed_reason(&crashed).starts_with(RemoteError::REFUSED),
        "and it must not read as a refusal: nothing decided anything"
    );
    assert!(
        !failed_reason(&crashed).starts_with(RemoteError::PANICKED),
        "nor as a handler fault, which is the other thing a peer acts on differently"
    );

    client.close().await.expect("closes");
    server.shutdown().await.expect("stops");
}

// ───────────────────────────────────────── what `stop()` has done by the time
// ───────────────────────────────────────── it returns

/// Counts turns, and nothing else.
///
/// It exists so that the rounds below cannot pass vacuously. An actor with an
/// **empty** observer list reaches the same teardown, so a version of that test
/// which forgot to attach an observer — or attached one that never ran — would
/// still be green while saying nothing about the observed path the teardown
/// order was written for. The count is asserted every round.
///
/// The increment happens **before** the future is built, which is the shape
/// `Observer::after_turn` is a plain `fn` to allow.
struct CountsTurns {
    turns: Arc<AtomicUsize>,
}

impl Observer<Ledger> for CountsTurns {
    fn name(&self) -> &'static str {
        "counts turns"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        self.turns.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

/// `stop()` returns only once the mailbox is closed — on an actor that **has
/// observers**, which is the case the loop's teardown order was written for.
///
/// # What is being pinned
///
/// `is_running()` is `!mail.is_closed()`, and that flips only when the loop's
/// **receiver** is dropped. So the property is a claim about an order: the loop
/// closes the observers and then the mailbox *before* it publishes the reason
/// it ended, and `stop()` waits for that reason rather than for its own `Stop`
/// message to be accepted. A `stop()` that returned once the message was merely
/// queued would return before the loop had even received it, and every caller
/// that reads `is_running()` afterwards would be reading a live actor. The
/// explicit `drop(inbox)` in the loop is what keeps the mailbox half of that
/// from depending on the order the async block happens to drop its captures in,
/// which no type states and no signature enforces.
///
/// # Why a loop, when one assertion looks like enough
///
/// A single assertion measures a single teardown, and this one is observable
/// only when something sits between the loop's last send and the receiver's
/// drop. On an actor holding nothing that gap is a few instructions with no
/// await in it, so a woken caller cannot land inside it and a green round has
/// proved nothing; give the actor a state or an observer that is slow to
/// release and the same gap is wide enough to read, at which point the answer
/// stops being lucky and becomes uniform — every round wrong. Which of those a
/// round is depends on what the actor was holding rather than on anything this
/// test decides, so it runs the whole start/turn/stop cycle many times instead
/// of trusting one.
///
/// The flavour is not decoration either. On the current-thread runtime every
/// other test in this file uses, the task woken by `stop()` cannot be polled
/// until the actor's task has already finished and dropped everything it
/// captured: the window is unobservable there **by construction**, and this
/// test would be green whatever the loop did. Several workers are what let a
/// woken caller run while the actor's task is still tearing down.
///
/// Nothing here is timed. The rounds buy interleavings, not a wall-clock
/// window, and the assertion is on a flag rather than on elapsed time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_returns_only_once_the_mailbox_is_closed() {
    const ROUNDS: usize = 100;

    for round in 0..ROUNDS {
        let turns = Arc::new(AtomicUsize::new(0));
        let actor = ledger::actor_with(
            Ledger::default(),
            vec![Box::new(CountsTurns {
                turns: turns.clone(),
            })],
        );

        // One real turn, so the observer is on the path being torn down rather
        // than merely attached to it.
        actor.post(1, 0).await.expect("running");
        assert_eq!(
            turns.load(Ordering::SeqCst),
            1,
            "round {round}: the observer did not run, so this round says nothing about the \
             observed path"
        );

        actor
            .stop()
            .await
            .expect("a stop from outside the actor's own turn is never refused");

        assert!(
            !actor.is_running(),
            "round {round}: stop() returned while the mailbox was still open, so a caller woken \
             by it can still be told the actor is running"
        );
    }
}

/// A turn an observer refused must NOT settle its idempotency key as the
/// refusal, because the turn's effect was never committed.
///
/// This is wave A's rule meeting RFC-06's, one layer down. Wave A settles a key
/// on **what ran**: a `Completed` marker records the answer and replays it
/// forever. An observer runs after the method and can veto the turn it just
/// watched — and a vetoed turn's mutation lived in memory and never reached a
/// store, so it is exactly the "it ran, and whether it took effect is unknown"
/// that `Interrupted` exists to say.
///
/// Marking `completed()` where the method returns — which is where it lived
/// until this test — recorded the veto as the call's settled reply. Every retry
/// under that key then read `observer refused: ..` as a final answer about work
/// whose effect had been thrown away. That is RFC-05 §2's defect reached through
/// a different door: an answer recorded for something that did not land.
#[tokio::test]
async fn a_refused_turn_does_not_settle_its_key_as_the_refusal() {
    let node = local_node().await;
    let caller = local_node().await;
    let actor = ledger::actor_with(Ledger::default(), vec![Box::new(RefusesWhatCompleted)]);
    let _server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).build())
        .spawn()
        .expect("serves");
    let client = ledger::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("dials");
    let key = keylink::IdempotencyKey::new("post-once").expect("a valid key");

    let refused = client
        .with_idempotency(key.clone())
        .post(1, 0)
        .await
        .expect_err("the observer refuses the completed turn");
    assert_eq!(
        failed_reason(&refused),
        format!("observer refused: {REFUSER}"),
        "the caller of the refused turn is told who refused it"
    );

    // The ledger outlives the actor, so this reaches the record rather than the
    // dead actor — which is what makes the recorded value observable at all.
    let repeat = client
        .with_idempotency(key)
        .post(1, 0)
        .await
        .expect_err("the key was claimed");
    assert!(
        matches!(
            repeat,
            keylink::Error::Remote(RemoteError::Idempotency(
                keylink::IdempotencyRefusal::Interrupted
            ))
        ),
        "a refused turn is unknown, not a settled refusal: got {repeat:?}"
    );
}
