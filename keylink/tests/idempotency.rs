//! Retry safety over a real QUIC connection.
//!
//! Every handler here reports the number of times it has run, so a reply is
//! itself the evidence: the same number twice means the handler was not entered
//! again, and no test has to reach inside the serving side to find out.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use keylink::{
    ActorId, Allowlist, Body, BodyReader, Caller, Context, Error, Eviction, IdempotencyKey,
    IdempotencyRefusal, Keyable, Middleware, Next, Node, Outcome, Records, RemoteError,
    Server as KeylinkServer,
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
    async fn reserve(&self, _cx: &Context, _sku: String, _qty: u32) -> u64 {
        self.reserves.next()
    }

    async fn slow(&self, _cx: &Context, ms: u64) -> u64 {
        let run = self.slows.next();
        tokio::time::sleep(Duration::from_millis(ms)).await;
        run
    }

    async fn datasheet(&self, _cx: &Context, bytes: u64) -> (Sheet, Body) {
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

/// The same `till`, served under a policy of the test's choosing.
///
/// A fresh node pair per call, deliberately: reusing a live service would prove
/// the behaviour of whatever policy that service was built with rather than of the
/// one under test, and the policy is fixed at `build()`.
async fn start_keyable(keyable: Keyable) -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let running = KeylinkServer::builder(&server)
        .service(
            till::service(Till::default(), Allowlist::anyone())
                .keyable(keyable)
                .build(),
        )
        .spawn()
        .expect("one service, no duplicate ALPN");
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

// ────────────────────────────────── which methods accept a key at all

/// **The headline of the wave: the serving side decides, and a key on a method it
/// does not key is refused rather than ignored.**
///
/// Both directions in one test, because the claim is that the policy
/// *discriminates*. `Only(&["Reserve"])` implemented as "accept everything" and as
/// "refuse everything" are each half-green under a one-sided assertion, and one of
/// those two is what a wiring mistake produces.
///
/// The refusal is asserted whole rather than by `matches!`, so the method name it
/// carries is part of the claim: a refusal that named the wrong method would tell a
/// caller to go and look at code that is fine.
///
/// Made red by `scripts/break-once.sh` replacing
/// `Self::Only(named) => named.contains(&method),` in `Keyable::accepts` with the
/// same line plus `|| true` — a policy that says yes to everything, which is the
/// shape a wiring mistake takes. Watched: `panicked at ..,
/// `Slow` is not in this service's policy: 1`
/// — the `expect_err` fires because the call went through and answered run 1.
/// Within this file that one edit reddens three tests and no more — this one,
/// `a_refused_key_never_reaches_the_ledger` and
/// `a_keyed_actor_service_applies_the_same_policy`. Scoped to this file on purpose:
/// which tests exist here is something this file controls, and a claim reaching past
/// it would not be.
#[tokio::test]
async fn a_key_on_a_method_the_service_does_not_key_is_refused() {
    let fixture = start_keyable(Keyable::Only(&["Reserve"])).await;

    let refused = fixture
        .client
        .with_idempotency(key("k"))
        .slow(0)
        .await
        .expect_err("`Slow` is not in this service's policy");
    assert_eq!(
        refusal(&refused),
        IdempotencyRefusal::NotKeyable {
            method: "Slow".to_owned()
        },
        "the refusal must name the method it refused"
    );

    // **And it refused BEFORE dispatch.** Read the way this file's header says to:
    // the reply IS the evidence. `Slow`'s counter has never been touched, so the
    // first call that gets through must answer run 1.
    //
    // Not redundant with the refusal above, and the reason is the whole point of the
    // line: **moving the refusal to after the handler would leave that assertion
    // green.** The caller receives its `NotKeyable` either way, so a reorder is
    // invisible from the reply — and only the counter can see it. That ordering is
    // what `Context::call`'s rustdoc rests on when it says the accessor can never
    // answer `Some` on a method outside the policy; a handler that ran with a key in
    // hand would falsify it silently.
    //
    // **It has no one-literal break, and this is the honest shape of that.** Every
    // edit that lets the call through — `keys.ledger_for(method)` gaining an
    // `.or(keys.ledger_for("Reserve"))`, say — reddens the `expect_err` above FIRST,
    // watched: ``panicked at .., `Slow` is not in this service's policy: 1``. So the
    // available break does not isolate this assertion. What it guards is a
    // structural reorder of `serve_one`, which is not a one-literal edit and which
    // nothing else here would notice.
    assert_eq!(
        fixture
            .client
            .slow(0)
            .await
            .expect("`Slow` unkeyed is fine"),
        1,
        "the refused keyed call must not have entered the handler",
    );

    assert!(
        fixture
            .client
            .with_idempotency(key("k"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .is_ok(),
        "`Reserve` IS in the policy, so a key on it must still be taken"
    );
}

/// **A refused key never reaches the ledger**, and the ordering is what proves it.
///
/// `IdempotencyKey::validate` is the first step on the road to `Ledger::claim` —
/// nothing else stands between a key and a claimed slot — so a refusal that beats
/// *validation* cannot have claimed a slot or evicted anybody's record. A smuggled
/// oversized key is refusable two ways, and which refusal comes back says which
/// check ran first. That is the strongest statement available from outside: a
/// refusal looks the same on the wire whether the ledger was consulted or not, so
/// the assertion has to be about order.
///
/// The second half is the ledger's own side of it. A *valid* key refused on `Slow`
/// is still a first attempt on `Reserve`, so the refusal spent nothing — not a
/// tombstone, not a slot.
///
/// Made red by `scripts/break-once.sh` replacing
/// `Self::Only(named) => named.contains(&method),` in `Keyable::accepts` with the
/// same line plus `|| true`. A policy that accepts everything is exactly a policy
/// that is not above the ledger: the key then reaches `validate`, and the answer
/// changes identity. Watched: `panicked at ..,
/// assertion `left == right` failed: a key the ledger would refuse as malformed
/// must be refused by the POLICY first, or the policy is not above the ledger`,
/// `left: Malformed("config: invalid idempotency key: 129 bytes exceeds the 128
/// byte cap")`, `right: NotKeyable { method: "Slow" }`. The other two tests that
/// edit reddens are named on
/// `a_key_on_a_method_the_service_does_not_key_is_refused`.
///
/// The break this test would most like — swapping the two statements in `claim_key`
/// — is a reordering rather than a substitution, so `break-once.sh` cannot express
/// it. The break above is what a reordering would *look like from out here*, and it
/// is the same assertion that catches both.
#[tokio::test]
async fn a_refused_key_never_reaches_the_ledger() {
    let fixture = start_keyable(Keyable::Only(&["Reserve"])).await;

    let refused = fixture
        .client
        .with_idempotency(smuggled(&"x".repeat(keylink::MAX_KEY_LEN + 1)))
        .slow(0)
        .await
        .expect_err("refusable two ways over");
    assert_eq!(
        refusal(&refused),
        IdempotencyRefusal::NotKeyable {
            method: "Slow".to_owned()
        },
        "a key the ledger would refuse as malformed must be refused by the POLICY \
         first, or the policy is not above the ledger"
    );

    let _spent_nothing = fixture
        .client
        .with_idempotency(key("shared"))
        .slow(0)
        .await
        .expect_err("refused, because `Slow` is not keyed");
    assert_eq!(
        fixture.client.key_state(key("shared")).await.expect("asks"),
        keylink::KeyState::Fresh,
        "a refused key must leave nothing behind for the query to find"
    );

    let run = fixture
        .client
        .with_idempotency(key("shared"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("the refused key must still be claimable");

    assert_eq!(
        run, 1,
        "a refused key must leave neither a record nor a tombstone behind"
    );
}

/// An unkeyed call is the path this feature must not touch, and `Keyable::None` is
/// the policy most able to touch it: there is no ledger at all on that service, so
/// a call that reached for one would be reaching for `None`.
///
/// Made red by `scripts/break-once.sh` replacing `claim_key`'s
/// `return Claimed::None;` with
/// `return Claimed::Refuse(IdempotencyRefusal::InFlight.into());` — the unkeyed
/// early return, gone. Watched: `panicked at ..,
/// a call with no key needs no policy: Remote(Idempotency(InFlight))`.
///
/// **That break is wide, and the breadth is the finding rather than a weakness of
/// this one.** An earlier version of this record said four and listed
/// four; the fifth,
/// `an_attacher_on_a_recordless_service_waits_and_is_told_it_ran`, was added to this
/// same branch after the record was written — which is precisely why a record must
/// name the tests it reddens rather than count them.
///
/// **And the first version of this paragraph then said five and listed four**,
/// omitting the very test it had just named as the fifth. The same error one level
/// down: a list presented as complete. So this record names them and counts
/// nothing —
/// `a_call_without_a_key_runs_every_time`, `a_refusal_is_recorded_and_replayed`,
/// `a_second_caller_attaches_to_a_live_key`,
/// `an_attacher_on_a_recordless_service_waits_and_is_told_it_ran`, and this test.
/// The unkeyed path is
/// load-bearing under most of the file; what this test adds is the one service the
/// others never build, the one with no ledger at all, where a call reaching for a
/// ledger would be reaching for `None`.
#[tokio::test]
async fn an_unkeyed_call_is_unaffected_by_any_policy() {
    let fixture = start_keyable(Keyable::None).await;

    let first = fixture
        .client
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("a call with no key needs no policy");
    let second = fixture
        .client
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("and neither does the next one");

    assert_eq!(
        second,
        first + 1,
        "an unkeyed call must run every time, on a service with no ledger at all"
    );
}

/// A service with no ledger still answers a ledger query, and the answer is
/// `Fresh` — which is a fact there rather than a fallback, because nothing was ever
/// recorded.
///
/// This is the one branch `answer_key_state` gained: `keys.ledger.as_deref()` is
/// `None`, and the alternative to answering was refusing a question that has no
/// method name to refuse under.
///
/// Made red by `scripts/break-once.sh` replacing
/// `Some(_) | None => KeyState::Fresh,` with
/// `Some(_) | None => KeyState::Interrupted,`. Watched: `panicked at ..,
/// assertion `left == right` failed: no ledger
/// means nothing was recorded, which is what `Fresh` reports`, `left: Interrupted`,
/// `right: Fresh`, and no other test in THIS FILE reddens — the other two
/// `a_key_state_query_*` tests stay green, because they run on a service that has
/// a ledger and never reach the arm.
#[tokio::test]
async fn a_key_state_query_on_a_service_that_keys_nothing_is_fresh() {
    let fixture = start_keyable(Keyable::None).await;

    assert_eq!(
        fixture.client.key_state(key("never")).await.expect("asks"),
        keylink::KeyState::Fresh,
        "no ledger means nothing was recorded, which is what `Fresh` reports"
    );
}

/// The policy is a property of the **service**, not of the singleton dispatch path,
/// so a keyed actor system serving the same schema applies it with no second
/// mechanism.
///
/// It works because `Rpc::method` is implemented for `Addressed<Request>` and
/// forwards to the inner request, so `keyed_service`'s `ServiceBuilder` reads the
/// real method name rather than a constant. That is the whole of why Wave 1 needed
/// no change to `macros.rs`.
///
/// Made red by `scripts/break-once.sh` neutralising this test's own policy. Search,
/// **with its indentation**, which is what keeps it unique against this quote:
/// `.keyable(Keyable::Only(&["Reserve"]))`
/// and replace with `.keyable(Keyable::Any)`.
///
/// The anchor is in this same file, and that is not a reason to describe rather than
/// quote — measured at `7a147c0`, the indented form counts **1** with this quote
/// present while the bare form counts 2. That is the test-literal break on purpose: a keyed service to which the policy was never
/// applied behaves *identically* to one configured `Keyable::Any`, so neutralising
/// the policy here reproduces exactly the defect this test exists to catch.
/// Watched: `panicked at .., `Slow` is not in
/// this service's policy, actor or not: 1`. No other test in this file can redden,
/// since the break is this test's own literal.
///
/// Red a second way, from production code, under the
/// `Self::Only(..) || true` break recorded on
/// `a_key_on_a_method_the_service_does_not_key_is_refused`: watched there as one of
/// three failures, this test among them, which is the evidence that the singleton
/// and keyed paths share one policy rather than two.
///
/// The break this test cannot run is the one in `actor/remote.rs`, where
/// `Rpc::method` for `Addressed<T>` forwards to the inner request. That file belongs
/// to another branch in flight, and `break-once.sh` restores from a backup on
/// `trap EXIT` — an interrupted run would write a stale copy over somebody else's
/// live edit. Standing in for it, and printing the body rather than the signature,
/// because a signature is identical either way:
///
/// ```sh
/// rg -n -A1 'fn method\(&self\) -> &.static str \{' keylink/src/actor/remote.rs
/// ```
///
/// Two matches today, and both must be read: `139: fn method(..) {` followed by
/// `140: self.message.method()`, which is the impl this test rests on, and
/// `573:`/`574: match self {`, a `Rpc` impl inside that file's own test module.
/// A constant at 140 is the defect; a third match is a third `Rpc` impl and wants
/// looking at.
#[tokio::test]
async fn a_keyed_actor_service_applies_the_same_policy() {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: till::System = till::System::builder(Eviction::Never).build();
    let _running = KeylinkServer::builder(&server)
        .service(
            till::keyed_service(&system, Allowlist::anyone())
                .keyable(Keyable::Only(&["Reserve"]))
                .build(),
        )
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = till::KeyedClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the typed keyed client dials the actor system");
    system
        .mount(
            ActorId::new("till").expect("a valid actor id"),
            till::spawn(TillActor::default()),
        )
        .expect("the id is free");

    let keyed = client.with_idempotency(key("k"));
    let refused = till::at(&keyed, ActorId::new("till").expect("valid"))
        .slow(0)
        .await
        .expect_err("`Slow` is not in this service's policy, actor or not");
    assert_eq!(
        refusal(&refused),
        IdempotencyRefusal::NotKeyable {
            method: "Slow".to_owned()
        },
        "the keyed path must read the real method name, not a constant"
    );

    assert!(
        till::at(&keyed, ActorId::new("till").expect("valid"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .is_ok(),
        "`Reserve` is in the policy on this path too"
    );
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
    async fn reserve(&self, _cx: &Context, _sku: String, _qty: u32) -> u64 {
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

// ──────────────────────────────── a key without a stored reply

/// A `till` served with the reply bytes going nowhere.
///
/// `Keyable::Any` deliberately, so nothing in these two tests is decided by
/// Wave 1's policy: every method still accepts a key, and the only thing that
/// changed is what a settled key holds.
async fn start_recordless() -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let running = KeylinkServer::builder(&server)
        .service(
            till::service(Till::default(), Allowlist::anyone())
                .records(Records::None)
                .build(),
        )
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = till::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the server");

    Fixture {
        client,
        _server: running,
        _nodes: vec![server, client_node],
    }
}

/// **`Records::None` gives up the answer and keeps at-most-once.** This is the
/// half that matters: the work runs once and the repeat does not run it again.
///
/// The evidence is the handler's own counter, read through a *third* key. The
/// first call answers 1. If the refused repeat had run the handler, the third key
/// would answer 3; it answers 2, so the repeat entered nothing. A test that only
/// asserted the refusal would be satisfied by a service that ran the work and
/// then refused to say so.
///
/// Made red by `scripts/break-once.sh` replacing `Guard::record`'s
/// `self.ledger.settle(self.peer, &self.key, slot);` with
/// `let _ = slot;` followed by `self.ledger.release(self.peer, &self.key);` — the
/// key given back instead of spent, which is the only shape at-most-once fails in.
/// Watched: `panicked at .., the repeat is refused, not answered: 2`. The `2` is the
/// handler's counter: the repeat ran.
///
/// **That break is very wide** — giving a key back instead of spending it reddens
/// fourteen of this file's tests, every one that asserts a repeat is not re-run.
/// Width is the finding: `settle` is the single line at-most-once rests on.
///
/// **What that break does NOT reach is this test's last assertion**, and it is worth
/// saying which half is unguarded. The break trips the `expect_err` above it, so the
/// counter check never runs. The case the counter check exists for is subtler — a
/// service that refuses the repeat *and* enters the handler anyway — and that would
/// mean `serve_one` claiming after `next(..)` rather than before, which is a
/// reordering of two statements and not a substitution `break-once.sh` can express.
/// Standing in for it, and it is the ordering the whole module rests on:
///
/// ```sh
/// rg -n 'claim_key\(&keys|let \(outcome, body\) = next\(' keylink/src/rpc.rs
/// ```
///
/// Exactly two matches, and the `claim_key` one must carry the LOWER line number.
/// `a_refusal_before_dispatch_leaves_the_key_claimable` and
/// `an_abort_before_dispatch_does_not_report_interrupted` are what would notice if
/// the two ever swapped.
#[tokio::test]
async fn records_none_still_runs_the_work_at_most_once() {
    let fixture = start_recordless().await;

    let first = fixture
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("the first call runs");
    assert_eq!(first, 1, "the handler ran once");

    let _refused = fixture
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("the repeat is refused, not answered");

    let after = fixture
        .client
        .with_idempotency(key("fresh"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("a different key is different work");
    assert_eq!(
        after, 2,
        "the refused repeat must not have entered the handler: 3 here means it did"
    );
}

/// The half `Records::None` gives up, said out loud: the repeat gets
/// `Unrecordable` rather than the reply.
///
/// Asserted against the identical call on a **default** service, in the same test,
/// because the claim is a difference. `Records::Memory` answers 1 twice;
/// `Records::None` answers 1 then refuses. Asserting only the refusal would pass on
/// a service that refused every repeat for some unrelated reason.
///
/// `Unrecordable` and not a variant of its own, and not `Forgotten` either:
/// RFC-05 §5 draws the axis as a permanent property against an eviction, and a
/// configured retention policy is the former. `IdempotencyRefusal::Unrecordable`'s
/// doc carries the three causes and the argument.
///
/// Made red by `scripts/break-once.sh` replacing
/// `Guard::record`'s `recordable` flag to ignore the retention switch, its
/// `matches!` on `self.ledger.records` replaced by `true`. Watched: `panicked at ..,
/// the repeat under `Records::None` is refused, not answered: 1` — the `expect_err`
/// fires because the repeat came back as the recorded reply. It reddens
/// `records_none_still_runs_the_work_at_most_once`,
/// `a_recv_method_is_refused_the_same_way_whatever_the_retention_policy` and
/// `an_attacher_on_a_recordless_service_waits_and_is_told_it_ran` beside this one.
#[tokio::test]
async fn records_none_refuses_the_repeat_instead_of_answering_it() {
    let recording = start().await;
    let first = recording
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("runs");
    assert_eq!(
        recording
            .client
            .with_idempotency(key("k"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("a recording service answers the repeat from its record"),
        first,
        "this is what `Records::Memory` does, and it is the contrast"
    );

    let recordless = start_recordless().await;
    recordless
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("the first call runs on both");
    let refused = recordless
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("the repeat under `Records::None` is refused, not answered");

    assert_eq!(
        refusal(&refused),
        IdempotencyRefusal::Unrecordable,
        "it ran, and this service does not hold its reply"
    );
}

/// **A method whose reply already could not be recorded is unchanged by the
/// switch**, and this is the test that says the two causes have not been merged
/// into a behaviour change.
///
/// `Datasheet` is a `recv`: its reply carries a payload, so it settles
/// `Unrecordable` on *every* service, `Records::Memory` included. Under
/// `Records::None` it settles `Unrecordable` for a second, independent reason. The
/// refusal is identical, and it has to be — README limit 5 is about the method and
/// must not start depending on the service's retention policy.
///
/// This is the test the earlier `Forgotten` design could not have passed. Under it
/// the two causes produced *different* refusals, so a `recv` method's documented
/// behaviour changed when an unrelated setting moved. `Unrecordable` for both is
/// what makes limit 5 a property of the method again.
///
/// Made red by `scripts/break-once.sh` replacing
/// `Guard::record`'s settled slot from
/// `Slot::Unrecordable` to `Slot::Forgotten`, which is the old design restored.
/// Watched: `panicked at .., assertion `left == right` failed: a recv method's
/// refusal must not depend on the service's retention policy`, `left: Forgotten`,
/// `right: Unrecordable`. It also reddens
/// `a_reply_that_carries_a_payload_is_refused_on_repeat`,
/// `records_none_refuses_the_repeat_instead_of_answering_it` and
/// `an_attacher_on_a_recordless_service_waits_and_is_told_it_ran` — and that
/// `a_reply_that_carries_a_payload_is_refused_on_repeat` is in the list is the point
/// of this test: the two causes share one refusal.
#[tokio::test]
async fn a_recv_method_is_refused_the_same_way_whatever_the_retention_policy() {
    for fixture in [start().await, start_recordless().await] {
        fixture
            .client
            .with_idempotency(key("k"))
            .datasheet(8)
            .await
            .expect("the first call runs and streams its body");

        let refused = fixture
            .client
            .with_idempotency(key("k"))
            .datasheet(8)
            .await
            .expect_err("a reply with a payload is never replayed");

        assert_eq!(
            refusal(&refused),
            IdempotencyRefusal::Unrecordable,
            "a recv method's refusal must not depend on the service's retention policy"
        );
    }
}

// ────────────────────────────────────────── the defaults, spelled and not

/// **A service that names neither policy behaves as one that names both values
/// out loud.** That is the whole compatibility claim of both waves — invariant 8 —
/// and it is asserted as a comparison rather than as a re-run of
/// `a_repeated_key_does_not_run_the_handler_twice`.
///
/// `till` is built with no `.keyable()` and no `.records()` call. `depot` names
/// `Keyable::Any` and `Records::Memory` out loud. Both must answer the repeat from
/// the record, and both must be `Done` when asked. Asserting only one of the two
/// services would prove nothing the tests above this line do not.
///
/// **There is no unit half, and that is the point.** Neither `Keyable` nor `Records`
/// has a `Default` impl — deliberately, per RFC-08 §3.2.5, because `Keyable::Any` is
/// the permissive setting and `Records::Memory` holds a caller's payloads for ten
/// minutes, and `Allowlist`'s doc makes the argument for not handing either to
/// somebody who left a field out. So there is no `Keyable::default()` to assert
/// against; the default is one spelled-out line of `ServiceBuilder::new`, and **this
/// test is the only thing that holds it.** The nearest unit tests,
/// `idempotency::tests::policy::the_permissive_policy_keys_every_method` and
/// `idempotency::tests::records::memory_records_and_replays`, hold that those two
/// *values* behave as they did before the types existed — not that a service gets
/// them.
///
/// Within this file, nothing else compares an unnamed default against the same
/// values named explicitly, which is what makes this the guard rather than one of
/// the twenty-six the break below reddens.
///
/// Made red by `scripts/break-once.sh` changing the `keyable` field in
/// `ServiceBuilder::new` from `Keyable::Any` to `Keyable::None` — anchored on that
/// field together with the `records` line below it, because `keyable: Keyable::Any,`
/// on its own also appears in this crate's `any(..)` test helper and
/// `break-once.sh` needs a unique anchor. Watched: `panicked at .., the unnamed
/// default must record: Remote(Idempotency(NotKeyable { method: "Reserve" }))`.
///
/// **That break is very wide** — most of this file fails, since a default of
/// `Keyable::None` means no service keys anything. Width is expected for a default:
/// everything rests on it. The paragraph above says what makes *this* test the guard
/// rather than one of the casualties.
///
/// An earlier version of this record was unreplayable and is worth naming as the
/// defect it was: it quoted the anchor as `keyable: Keyable::default(),`, which has
/// never been in this tree, and it claimed a unit half asserting
/// `Keyable::default()` — an API RFC-08 §3.2.5 exists to refuse. It also named two
/// tests, `the_default_policy_keys_every_method` and `the_default_records_in_memory`,
/// that do not exist under those names.
#[tokio::test]
async fn the_defaults_reproduce_todays_behaviour() {
    let server = local_node().await;
    let client_node = local_node().await;
    let _running = KeylinkServer::builder(&server)
        .service(till::service(Till::default(), Allowlist::anyone()).build())
        .service(
            depot::service(Depot::default(), Allowlist::anyone())
                .keyable(Keyable::Any)
                .records(Records::Memory)
                .build(),
        )
        .spawn()
        .expect("distinct ALPNs register cleanly");

    let addr = server.endpoint().addr();
    let unnamed = till::Client::connect(&client_node, addr.clone())
        .await
        .expect("dials");
    let spelled = depot::Client::connect(&client_node, addr)
        .await
        .expect("dials");

    let first = unnamed
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("the unnamed default must record");
    assert_eq!(
        unnamed
            .with_idempotency(key("k"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("and must replay"),
        first,
        "a service naming no policy must answer the repeat from the record"
    );

    let also = spelled
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("the spelled default must record");
    assert_eq!(
        spelled
            .with_idempotency(key("k"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("and must replay"),
        also,
        "spelling the default out loud must change nothing"
    );

    assert_eq!(
        unnamed.key_state(key("k")).await.expect("asks"),
        keylink::KeyState::Done,
        "and the ledger must agree with the replay"
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

/// **An attacher on a service that holds no replies waits, and is then told the
/// work ran.** The wait is not futile and it is deliberately not refused early.
///
/// Refusing at once could only answer `InFlight`, documented as *ask again when it
/// finishes*, so the caller would ask again for as long as the handler runs — one
/// bounded park traded for a re-dial loop, ending at the worse of the two answers.
/// What the wait buys is `Unrecordable`: it ran, there is no reply, reconcile and
/// stop. `Idempotency::attaching_for`'s doc carries the argument.
///
/// The budget here is five seconds against a 400 ms handler, so the assertion is
/// that the attacher was answered by the *settle* rather than by its own timeout —
/// a timeout would answer `InFlight` and fail on the variant rather than hang.
///
/// The counter is the second half: the handler ran **twice**, once for the original
/// and once for the third keyless call. Three would mean the attacher's own call
/// entered the handler after being refused, which is the failure a refusal-shaped
/// answer could otherwise hide.
///
/// Made red by `scripts/break-once.sh` replacing `Guard::record`'s settled slot
/// with `Slot::Done(frame.clone())` — a service that records after all, which is
/// what an attacher would rather meet. Watched: `panicked at .., the attacher waits
/// for the settle and is then refused: 1`, and no other test in this file reddens.
/// The `expect_err` is what fires, because the attacher is handed the replayed
/// reply.
///
/// The half that break does not reach is `InFlight` versus `Unrecordable` — a
/// budget that expired instead of being woken. That is
/// `an_attach_that_outlives_its_budget_returns`'s claim, below, and the variant
/// assertion here is what would notice the two paths being confused.
#[tokio::test]
async fn an_attacher_on_a_recordless_service_waits_and_is_told_it_ran() {
    let fixture = start_recordless().await;
    let client = fixture.client;

    let slow = {
        let scoped = client.with_idempotency(key("k"));
        tokio::spawn(async move { scoped.slow(400).await })
    };
    // Long enough that the claim is taken and the handler is inside its sleep.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let refused = client
        .with_idempotency(keylink::Idempotency::attaching_for(
            key("k"),
            Duration::from_secs(5),
        ))
        .slow(0)
        .await
        .expect_err("the attacher waits for the settle and is then refused");

    slow.await
        .expect("no panic")
        .expect("the first call answers");
    assert_eq!(
        refusal(&refused),
        IdempotencyRefusal::Unrecordable,
        "the wait must end at the settle, not at its own budget: `InFlight` here \
         would mean the attacher timed out instead of being woken"
    );
    assert_eq!(
        client.slow(0).await.expect("runs"),
        2,
        "the handler ran twice - the original and this one - not three times"
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
    // The assertion is "it did not wait out the attach window", and the only
    // honest ceiling is one a loaded machine cannot cross by accident.
    //
    // This used to read a bare `5s`, under a comment arguing that "load can only
    // make a real network call slower, never faster" — which is the argument
    // AGAINST an upper bound, not for one. CONTRIBUTING.md forbids that shape.
    //
    // **Derived from `MAX_ATTACH`, never written as its own number.** A literal
    // here rots in a direction nothing catches: drop `MAX_ATTACH` below the
    // literal and `elapsed` can no longer reach it, so this test passes forever
    // while asserting nothing. Half the window keeps both edges — a settled
    // replay answers in milliseconds, and a regression waits the whole window.
    let ceiling = keylink::MAX_ATTACH / 2;
    assert!(
        started.elapsed() < ceiling,
        "a settled key must be answered without waiting: took {:?}, ceiling {ceiling:?}",
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
    async fn reserve(&mut self, _cx: Caller<'_>, _sku: String, _qty: u32) -> u64 {
        self.reserves += 1;
        self.reserves
    }

    async fn slow(&mut self, _cx: Caller<'_>, ms: u64) -> u64 {
        self.slows += 1;
        let run = self.slows;
        tokio::time::sleep(Duration::from_millis(ms)).await;
        run
    }

    async fn datasheet(&mut self, _cx: Caller<'_>, bytes: u64) -> (Sheet, Body) {
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
