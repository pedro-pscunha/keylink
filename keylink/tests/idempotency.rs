//! Retry safety over a real QUIC connection.
//!
//! Every handler here reports the number of times it has run, so a reply is
//! itself the evidence: the same number twice means the handler was not entered
//! again, and no test has to reach inside the serving side to find out.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::prelude::*;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::{
    ActorId, Allowlist, Body, BodyReader, Caller, ConfigError, Context, Error, Eviction,
    IdempotencyKey, IdempotencyRefusal, Keyable, Limits, Middleware, Next, Node, Outcome, Records,
    RemoteError, Server as KeylinkServer,
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

#[derive(Default, Clone)]
struct Counter(Arc<AtomicU64>);

impl Counter {
    fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn runs(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct Till {
    reserves: Counter,
    slows: Counter,
    sheets: Counter,
    /// `None` unless a test holds a `slow` turn open: it waits for a permit here
    /// before it sleeps, so the test keeps the handler running for exactly as long
    /// as the order it is reading needs, and not for a duration a stall can spend.
    release: Option<Arc<tokio::sync::Semaphore>>,
}

impl till::Handler for Till {
    async fn reserve(&self, _cx: &Context, _sku: String, _qty: u32) -> u64 {
        self.reserves.next()
    }

    async fn slow(&self, _cx: &Context, ms: u64) -> u64 {
        let run = self.slows.next();
        if let Some(release) = &self.release {
            release
                .acquire()
                .await
                .expect("the test never closes the semaphore")
                .forget();
        }
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

/// The same `till`, served under bounds of the test's choosing.
///
/// A fresh node pair per call, for the reason `start_keyable` gives: the limits are
/// fixed at `build()`.
async fn start_limited(limits: Limits) -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let running = KeylinkServer::builder(&server)
        .service(
            till::service(Till::default(), Allowlist::anyone())
                .limits(limits)
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

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("no test sets a bound to zero")
}

fn key(text: &str) -> IdempotencyKey {
    IdempotencyKey::new(text).expect("a valid key")
}

/// Wait until the ledger holds a running claim under `name`.
///
/// What the tests that start a slow call and then send a second one under the same
/// key need to know, asked of the ledger and not guessed from a sleep: a second
/// call that reaches the server first takes the claim itself, and the test is then
/// about something else. Bounded, so a claim that never lands is a red test.
async fn until_running(client: &till::Client, name: &str) {
    tokio::time::timeout(Duration::from_secs(120), async {
        while client.key_state(key(name)).await.expect("asks") != keylink::KeyState::Running {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the first call's claim reached the ledger within 120 s");
}

/// `till` with every `slow` turn parked on the returned semaphore until the test
/// adds permits, under the ledger's room for attachers.
///
/// `records` is the policy under test; one attacher may wait per peer, which is
/// what [`one_attacher_waiting`] reads.
async fn start_held(records: Option<Records>) -> (Fixture, Arc<tokio::sync::Semaphore>) {
    let server = local_node().await;
    let client_node = local_node().await;
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut service = till::service(
        Till {
            release: Some(release.clone()),
            ..Till::default()
        },
        Allowlist::anyone(),
    )
    .limits(Limits::default().waiting_per_peer(NonZeroUsize::MIN));
    if let Some(records) = records {
        service = service.records(records);
    }
    let running = KeylinkServer::builder(&server)
        .service(service.build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = till::Client::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the server");
    (
        Fixture {
            client,
            _server: running,
            _nodes: vec![server, client_node],
        },
        release,
    )
}

/// Send two attachers for `name` at once and hand back the one that is waiting,
/// once it provably is.
///
/// The ledger lets one attacher wait per peer ([`start_held`]), so of two attachers
/// that reach it while the first call is held, one parks and the other is refused
/// `TooManyRunning` at once. That refusal is the observation: it cannot happen
/// unless the other attacher is already waiting. Nothing else tells a test that a
/// call has parked behind the ledger; a sleep only guesses, and an attacher that
/// arrives after the first call settles is served the record and proves nothing.
async fn one_attacher_waiting(
    client: &till::Client,
    name: &str,
) -> tokio::task::JoinHandle<Result<u64, Error>> {
    let attacher = || {
        let client = client.clone();
        let key = key(name);
        tokio::spawn(async move {
            client
                .with_idempotency(keylink::Idempotency::attaching_for(
                    key,
                    Duration::from_secs(120),
                ))
                .slow(0)
                .await
        })
    };
    let mut first = attacher();
    let mut second = attacher();
    let (refused, waiting) = tokio::select! {
        done = &mut first => (done, second),
        done = &mut second => (done, first),
    };
    let refused = refused
        .expect("no panic")
        .expect_err("the attacher that finishes before the first call settles was refused");
    assert!(
        matches!(refusal(&refused), IdempotencyRefusal::TooManyRunning { .. }),
        "the first attacher to answer must be turned away by the room for one waiter, \
         which is what proves the other is parked"
    );
    waiting
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

/// `Only` preserves policy order when reporting its first unknown name, and the
/// erased service returns that error before a handler can run.
///
/// Search `                .first_unknown(Req::METHODS)` and
/// `                return Err(Error::Config(error));`.
///
/// Made red twice with `scripts/break-once.sh`: filtering the result of
/// `first_unknown(Req::METHODS)` away, and replacing the startup return with
/// `let _ = error;`. Both let server startup succeed and fired the test's
/// `expect_err` before any call could be made.
#[tokio::test]
async fn keyable_only_rejects_its_first_unknown_name_at_startup_before_dispatch() {
    let server = local_node().await;
    let reserves = Counter::default();
    let observed = reserves.clone();
    let service = till::service(
        Till {
            reserves,
            ..Till::default()
        },
        Allowlist::anyone(),
    )
    .keyable(Keyable::Only(&["Reserv", "Slaw"]))
    .build();

    let error = KeylinkServer::builder(&server)
        .service(service)
        .spawn()
        .expect_err("an unknown Only name must refuse startup");
    assert!(matches!(
        error,
        Error::Config(ConfigError::UnknownMethod {
            service: "Till",
            named,
        }) if named == "Reserv"
    ));
    assert_eq!(
        observed.runs(),
        0,
        "startup must fail before handler dispatch"
    );
}

/// `Except` is validated through the same typed builder path as `Only`.
///
/// Search `                .first_unknown(Req::METHODS)`.
///
/// Made red with `scripts/break-once.sh` by filtering the
/// `first_unknown(Req::METHODS)` result away. Server startup succeeded instead of
/// returning the configured `Slaw` name.
#[tokio::test]
async fn keyable_except_rejects_an_unknown_name_at_startup() {
    let server = local_node().await;
    let service = till::service(Till::default(), Allowlist::anyone())
        .keyable(Keyable::Except(&["Reserve", "Slaw"]))
        .build();

    let error = KeylinkServer::builder(&server)
        .service(service)
        .spawn()
        .expect_err("an unknown Except name must refuse startup");
    assert!(matches!(
        error,
        Error::Config(ConfigError::UnknownMethod {
            service: "Till",
            named,
        }) if named == "Slaw"
    ));
}

/// Generated status belongs to the keyable vocabulary even though the caller did
/// not declare it.
///
/// Search `                    "__Status",`.
///
/// Made red with `scripts/break-once.sh` by removing `"__Status"` from generated
/// `METHODS`. Startup returned `Config(UnknownMethod { service: "Till", named:
/// "__Status" })`.
#[tokio::test]
async fn generated_status_is_a_known_keyable_method() {
    let server = local_node().await;
    let service = till::service(Till::default(), Allowlist::anyone())
        .keyable(Keyable::Only(&["__Status"]))
        .build();

    let _running = KeylinkServer::builder(&server)
        .service(service)
        .spawn()
        .expect("generated status is in Rpc::METHODS");
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
/// forwards to the inner request, so `registry.service`'s `ServiceBuilder` reads the
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
/// Two matches today, and both must be read. The first is `139: fn method(..) {`
/// followed by `140: self.message.method()`, which is the impl this test rests on.
/// The second is **the one inside that file's own `#[cfg(test)]` module**, and it is
/// told apart by what sits at it rather than by where it sits: it answers with
/// `match self {` instead of forwarding to an inner request, and `rg` prints it one
/// indent deeper because it is nested in the module. A constant at 140 is the defect;
/// a third match ABOVE the `#[cfg(test)]` line is a third production `Rpc` impl and
/// wants looking at.
///
/// **The second half of that used to be stated as `573`/`574` and had rotted; the
/// first pair had not.** Nothing moved in either impl — `keylink/src/actor/remote.rs`
/// grew above its test module, which pushed the second match down and left the first
/// where it was. The characterisation survived the move; only the position did not.
///
/// Which half rotted is the lesson: **the half stated as an absolute line number, and
/// not the half stated as a relationship.** `139`/`140` are quoted with the text that
/// sits there, so a reader who finds that text at another number has still found the
/// impl; `573`/`574` carried nothing but a position. So the second match is named
/// above by the side of `#[cfg(test)]` it falls on and by the line it answers with,
/// neither of which an insertion above it can move. `139`/`140` keep their numbers
/// because they carry their text, which is the same rule read the other way.
///
/// `records_none_still_runs_the_work_at_most_once`'s record is the shape for the
/// FIRST half of this: it names `Guard::record` and asserts a relationship between
/// two statements, and carries no line number at all. It was **not** clean of the
/// general fault when this branch reached it, and holding it up as though it were is
/// the same mistake one level up — it also said the break reddens "fourteen of this
/// file's tests", and a count over an open set rots on the fifteenth such test
/// anybody writes. That count is gone now: the record states the property and marks
/// the size unmeasured, so the two records agree rather than one recommending a
/// shape the other had already broken. A relationship cannot rot. A number can,
/// whether it indexes a line or a population.
#[tokio::test]
async fn a_keyed_actor_service_applies_the_same_policy() {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: till::Registry = till::Registry::builder(Eviction::Never).build();
    let _running = KeylinkServer::builder(&server)
        .service(
            system
                .service(Allowlist::anyone())
                .keyable(Keyable::Only(&["Reserve"]))
                .build(),
        )
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = till::RegistryClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the typed keyed client dials the actor system");
    system
        .mount(
            ActorId::new("till").expect("a valid actor id"),
            till::spawn(TillActor::default()),
        )
        .expect("the id is free");

    let keyed = client.with_idempotency(key("k"));
    let refused = keyed
        .at(ActorId::new("till").expect("valid"))
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
        keyed
            .at(ActorId::new("till").expect("valid"))
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
        .slow(5_000)
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
/// every test in this file that asserts a repeat is not re-run. Width is the
/// finding: `settle` is the single line at-most-once rests on.
///
/// **How many that is is NOT recorded here, and the omission is deliberate.** This
/// sentence said "fourteen" until this branch. Nothing had measured it, nothing can
/// see it go stale, and it rots on the fifteenth such test anybody writes — the
/// exact fault `.mise/tasks/anchors` warns about for its own row count, sitting in
/// the record this branch had held up as the shape to copy. The property above
/// survives a test being added. A count does not.
///
/// **And a static sweep is not a substitute for running it**, which is worth saying
/// because the sweep looks convincing. The property is written in at least three
/// unrelated assertion shapes here: a refusal, as `expect_err` on the retry; a run
/// counter read through a third key, as this test does; and plain equality with the
/// first answer, as `a_repeated_key_does_not_run_the_handler_twice` does. A sweep
/// keyed on refusals and counters misses that third shape, so it under-reports while
/// reading as exhaustive — measured, by writing one and watching it drop that test.
/// Only `mise run replay-anchors` on this row settles the size, and no gate runs it.
///
/// **R20's two halves age differently, and that is the reason to keep them side by
/// side.** The `claim_key`-before-`next` citation below asserts a RELATIONSHIP
/// between two line numbers while naming neither, so nothing inserted above either
/// one can invalidate it. That half really is the shape to copy. This population
/// half was a bare count, and it rotted quietly in the same doc comment. One
/// durable citation beside one rotted number, in one record, is the clearest
/// evidence on this branch that the fault is the FORM of a claim and not the care
/// of whoever wrote it.
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

/// **`ServiceBuilder::limits` reaches the ledger.** There is no unit half, and that
/// is the point.
///
/// Every other guard on this feature is a unit test inside `src/idempotency.rs`,
/// reaching the bounds through `Ledger::new`. Those prove the ledger's arithmetic
/// and say **nothing** about whether the builder's value arrives. The whole of
/// `ServiceBuilder::limits` can be silently disconnected — a `limits` field
/// assigned in the method and never read in `build` — with all of them green.
/// `the_defaults_reproduce_todays_behaviour` above is the same shape for
/// `records`, and its record makes the same argument.
///
/// `frame_bytes(1)` is chosen because **the default cannot produce this refusal at
/// all.** `MAX_RECORDED_FRAME` is `MAX_HEADER + 4`, deliberately above every frame
/// the wire will pass, so a `Reserve` reply is recordable on any default service.
/// At a ceiling of one byte the reply is over it, settles `Unrecordable`, and the
/// retry is refused. A green here therefore cannot be produced by a discarded
/// `Limits`.
///
/// Made red by `scripts/break-once.sh` deleting the `limits` assignment in
/// `ServiceBuilder::build` — that is, changing
/// `keys: Keys::new(self.keyable, self.records, self.limits),` to pass
/// `Limits::default()` in its place. Watched: `panicked at .., a one-byte frame
/// ceiling must refuse the repeat: 1`. The `1` is the recorded reply coming back:
/// with the configured value discarded, the default ceiling records it and the
/// repeat is answered instead of refused.
///
/// **The break is wider than this test, and the second guard it reddens is in this
/// file.** It reddens this one, which is the only test asserting a refusal the
/// default cannot produce. It also reddens
/// `a_tombstone_bound_below_the_churn_runs_the_work_twice`, and the reason is that
/// `ServiceBuilder::limits` has exactly one caller — the `start_limited` fixture
/// above — which both tests go through. With the configured `Limits` discarded, that
/// fixture's `.tombstones(nz(1))` becomes the default `MAX_TOMBSTONES`, 8192, against
/// a churn of two keys. The tombstone then survives the churn and the retry is
/// refused `Forgotten` rather than running the work a second time, which is the very
/// red that test records for itself at a bound of 64. That second redness was derived
/// from the fixture and the two records, not run, and so is not yet watched.
///
/// The unit guards on this feature do stay green under it: they reach the bounds
/// through `Ledger::new` and never go through the builder at all.
#[tokio::test]
async fn a_frame_ceiling_set_on_the_builder_reaches_the_ledger() {
    let limited = start_limited(Limits::default().frame_bytes(nz(1))).await;

    limited
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("the first call runs and is answered");

    let refused = limited
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect_err("a one-byte frame ceiling must refuse the repeat");

    assert_eq!(
        refusal(&refused),
        IdempotencyRefusal::Unrecordable,
        "over the ceiling is a permanent property of the reply, not an eviction"
    );

    let generous = start_limited(Limits::default()).await;
    let first = generous
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("the default ceiling records");
    assert_eq!(
        generous
            .client
            .with_idempotency(key("k"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("and replays"),
        first,
        "the contrast: the shipped ceiling refuses nothing, so this is the value arriving"
    );
}

/// **A narrow `tombstones` loses at-most-once, and no refusal reports it.**
///
/// This is the one bound on `Limits` whose wrong value the caller cannot detect,
/// so it is the one that needs a guard showing the work running twice rather than
/// a guard showing a refusal. `tombstones(1)` drops the first tombstone as soon as
/// a second key settles for that peer, the key reads `Fresh` again, and the handler
/// is entered a second time. `README.md` limit 4 names exactly this as the
/// degradation a caller cannot see, *because `Fresh` is what "never used" looks
/// like too*.
///
/// `pool_records(1)` is here to make the record go first: a key has to lose its
/// record before its tombstone is what answers. The churn then evicts the
/// tombstone.
///
/// The **run count in the second reply** is the whole assertion. `Till::reserve`
/// returns the number of times it has been entered, so the reply is the evidence and
/// nothing reaches inside the serving side. Four, not two: the two churn keys are
/// entries 2 and 3.
///
/// Made red by `scripts/break-once.sh` raising the tombstone bound above the churn.
/// Anchored on the whole builder line rather than on a `.tombstones(..)` fragment,
/// because the record and the edit are in the SAME file: an indented whole line is
/// unique against this unindented quote, while a fragment is not disambiguated by
/// indentation at all.
///
/// Anchor: `let tight = start_limited(Limits::default().pool_records(nz(1)).tombstones(nz(1))).await;`
/// Replacement: the same line with `.tombstones(nz(64))`.
/// Watched: `panicked at .., a vacant key is Fresh, so the call is dispatched rather
/// than refused: Remote(Idempotency(Forgotten))`.
///
/// **The red fires on the `expect` above the assertion, not on the run count**, and
/// it is worth saying which half that leaves unguarded. At a bound of 64 the
/// tombstone survives the churn, so the retry is refused `Forgotten` and the
/// counter check never runs. That is the correct behaviour being restored — a
/// surviving tombstone is exactly what should refuse — so the break proves the
/// tombstone is what answers here, and the `ran == 4` assertion below is what
/// distinguishes *the work ran again* from *the key was merely vacant*. Nothing
/// available to `break-once.sh` reddens that second half: it would need a service
/// that reports `Fresh` without dispatching, which is not a substitution.
#[tokio::test]
async fn a_tombstone_bound_below_the_churn_runs_the_work_twice() {
    let tight = start_limited(Limits::default().pool_records(nz(1)).tombstones(nz(1))).await;

    let first = tight
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("the first call runs");
    assert_eq!(first, 1, "the handler has been entered once");

    // Two further keys settle for the same peer. The first pushes `k` out of the
    // record pool; the second pushes `k`'s tombstone out of the tombstone pool.
    for other in ["a", "b"] {
        tight
            .client
            .with_idempotency(key(other))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("churn settles");
    }

    let again = tight
        .client
        .with_idempotency(key("k"))
        .reserve("ADU-014".to_string(), 2)
        .await
        .expect("a vacant key is Fresh, so the call is dispatched rather than refused");

    assert_eq!(
        again, 4,
        "a dropped tombstone must let the work run again: the handler counts every \
         entry, and the two churn keys are entries 2 and 3"
    );
}

// ───────────────────────────────────────────────────────── misuse of the API

/// A scoped client carries its key into every call made through it, so it is
/// meant for one. Making a second, DIFFERENT call through it does not corrupt
/// anything: the ledger refuses a different request fingerprint before it can
/// return a reply to a question the caller did not ask.
#[tokio::test]
async fn a_second_different_call_through_one_scoped_client_is_refused() {
    let fixture = start().await;
    let scoped = fixture.client.with_idempotency(key("k"));
    scoped.reserve("ADU-014".to_string(), 2).await.unwrap();

    let error = scoped
        .slow(1)
        .await
        .expect_err("the key was bound to Reserve, and this call asked Slow");

    assert!(
        matches!(
            error,
            Error::Remote(RemoteError::Idempotency(
                IdempotencyRefusal::RequestMismatch
            ))
        ),
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
///
/// The first call is parked on a semaphore and released only once an attacher is
/// provably waiting (`one_attacher_waiting`). Made red by `scripts/break-once.sh`
/// on `keylink/src/rpc.rs`, replacing `if n0_future::time::timeout(budget,
/// notified).await.is_err() {` with `if true {` — an attach that does not wait.
/// Watched: `the first attacher to answer must be turned away by the room for one
/// waiter, which is what proves the other is parked`; the recordless test below
/// goes red on the same edit.
#[tokio::test]
async fn a_second_caller_attaches_to_a_live_key() {
    let (fixture, release) = start_held(None).await;
    let client = fixture.client;

    let slow = {
        let scoped = client.with_idempotency(key("k"));
        tokio::spawn(async move { scoped.slow(0).await })
    };
    until_running(&client, "k").await;
    // The first call is parked in its handler until the permit below, so the
    // attacher has no way to meet a settled key: it either waits or is refused.
    let attacher = one_attacher_waiting(&client, "k").await;
    release.add_permits(16);

    let attached = attacher
        .await
        .expect("no panic")
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
    let (fixture, release) = start_held(Some(Records::None)).await;
    let client = fixture.client;

    let slow = {
        let scoped = client.with_idempotency(key("k"));
        tokio::spawn(async move { scoped.slow(0).await })
    };
    until_running(&client, "k").await;
    let attacher = one_attacher_waiting(&client, "k").await;
    release.add_permits(16);

    let refused = attacher
        .await
        .expect("no panic")
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
    let (fixture, release) = start_held(None).await;
    let client = fixture.client;

    let slow = {
        let scoped = client.with_idempotency(key("k"));
        tokio::spawn(async move { scoped.slow(0).await })
    };
    until_running(&client, "k").await;

    // The first call is parked in its handler until the permit below, so the
    // budget can only run out while it is still running.
    let error = client
        .with_idempotency(keylink::Idempotency::attaching_for(
            key("k"),
            Duration::from_millis(150),
        ))
        .slow(3_000)
        .await
        .expect_err("the budget runs out while the first call is still running");

    assert_eq!(refusal(&error), IdempotencyRefusal::InFlight);
    release.add_permits(16);
    slow.await
        .expect("no panic")
        .expect("the first call answers once released");
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
        .slow(500)
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

// ────────────────────────────────── the ledger that outlives its own process

// Everything below drives the **real** file store on a temp directory, which is
// the house rule `keylink/tests/persist.rs` states: a mocked port cannot exhibit
// the failure these tests are about, because the failure IS a disk.
//
// **A module rather than a file**, which is the difference from
// `keylink/tests/persist.rs` and its `#![cfg(feature = "persist-file")]`. Every
// test above this line is feature-independent and has to keep compiling at
// `--no-default-features`, so the condition goes here and not at the top.
//
// That is not a preference. `.mise/tasks/features` runs
// `cargo check -p keylink --all-targets --no-default-features`, and it is what
// caught this block naming `keylink_persist` unconditionally — five errors, in a
// gate that had already passed lint, docs and the whole test suite.
#[cfg(feature = "persist-file")]
mod stored {
    use super::*;

    /// A scratch directory that removes itself.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("keylink-idempotency-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The name every durable test stores under.
    fn ledger_id() -> keylink_persist::StateId {
        keylink_persist::StateId::new("till/ledger").expect("a valid id")
    }

    /// `till`, served through a record store on `root`.
    ///
    /// **The client's secret is a parameter and the server's is not**, because a
    /// record is keyed on the peer the *server* proved. A restart that dialled from
    /// a fresh identity would read `Fresh` whatever the disk held, and would be
    /// measuring the key rather than the ledger.
    ///
    /// `limit` is the adapter's per-record ceiling, which is how these tests make a
    /// real store refuse a real write.
    async fn start_durable(
        root: &std::path::Path,
        limit: u64,
        caller: [u8; 32],
    ) -> (Fixture, Counter) {
        start_durable_as(root, limit, caller, Limits::default(), false).await
    }

    /// The same, with the two knobs only one test each needs.
    ///
    /// `limits` shrinks a pool so an eviction is one call away instead of a
    /// thousand. `refusing` puts a layer in front that refuses the first call
    /// **without calling through**, which is the one path that releases a claim
    /// rather than settling it.
    async fn start_durable_as(
        root: &std::path::Path,
        limit: u64,
        caller: [u8; 32],
        limits: Limits,
        refusing: bool,
    ) -> (Fixture, Counter) {
        let files = keylink_persist::file::Files::open(root)
            .await
            .expect("opens")
            .limit(limit);
        // A restart test drops the previous fixture and opens the same ledger at
        // once. The old ledger's claim is released when the last task holding it
        // is dropped, and the server's connection tasks are dropped by the
        // runtime after `drop(fixture)` returns: under a load average of about
        // 110 the reopen ran first and read `Busy`. So `Busy` is waited out, on
        // a bound that names what it waited for; any other refusal fails at once.
        let mut records = None;
        for _ in 0..12_000 {
            match keylink::DurableRecords::open(&files, &ledger_id()).await {
                Err(keylink::RecordError::Store(keylink_persist::Error::Busy { .. })) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                opened => {
                    records = Some(opened.expect("nobody else holds this ledger"));
                    break;
                }
            }
        }
        let records = records.expect("the previous fixture released the ledger within 120 s");

        let reserves = Counter::default();
        let till = Till {
            reserves: reserves.clone(),
            ..Till::default()
        };

        let server = local_node().await;
        let client_node = Node::bind_local_with_secret(keylink::SecretKey::from_bytes(&caller))
            .await
            .expect("binds");
        let mut service = till::service(till, Allowlist::anyone())
            .records(Records::Durable(records))
            .limits(limits);
        if refusing {
            service = service.layer(RefuseOnce(AtomicU64::new(0)));
        }
        let running = KeylinkServer::builder(&server)
            .service(service.build())
            .spawn()
            .expect("one service, no duplicate ALPN");
        let client = till::Client::connect(&client_node, server.endpoint().addr())
            .await
            .expect("the client dials the server");

        (
            Fixture {
                client,
                _server: running,
                _nodes: vec![server, client_node],
            },
            reserves,
        )
    }

    /// The whole file store's ceiling when a test is not trying to break it.
    const ROOMY: u64 = 64 * 1024;

    /// The claim record's exact size: nine bytes of framing, a 32-byte peer id, the
    /// key, one version byte, a 32-byte request fingerprint, and one state byte.
    ///
    /// A ceiling set to exactly this admits every claim and refuses every settled
    /// record, because a settled record is the same thing with a reply frame after
    /// it. The two writes are told apart by their size and by nothing else, which is
    /// what lets a test refuse one of them through the real adapter.
    const fn claim_record(key: &str) -> u64 {
        9 + 32 + key.len() as u64 + 1 + 32 + 1
    }

    /// A key answered before a restart is still answered after one.
    #[tokio::test]
    async fn a_settled_key_survives_the_process_that_settled_it() {
        let scratch = Scratch::new("survives");
        let caller = [7_u8; 32];

        let first = {
            let (fixture, runs) = start_durable(&scratch.0, ROOMY, caller).await;
            let first = fixture
                .client
                .with_idempotency(key("reserve-4471"))
                .reserve("ADU-014".to_string(), 2)
                .await
                .expect("the first call runs");
            assert_eq!(runs.runs(), 1);
            first
        };

        let (fixture, runs) = start_durable(&scratch.0, ROOMY, caller).await;
        assert_eq!(
            fixture
                .client
                .key_state(key("reserve-4471"))
                .await
                .expect("asks"),
            keylink::KeyState::Done,
            "a settled key must not read Fresh after a restart"
        );

        let again = fixture
            .client
            .with_idempotency(key("reserve-4471"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("the repeat is answered");
        assert_eq!(
            again, first,
            "the repeat must be answered from the store, byte for byte"
        );
        assert_eq!(
            runs.runs(),
            0,
            "this process's handler must never have been entered"
        );
    }

    /// The record is on the disk before the reply is on the wire.
    ///
    /// Red-tested by discarding `write_settled`'s error in `Guard::record` — which
    /// is what "store it afterwards" amounts to from here. The caller is then
    /// answered `1`, this `expect_err` fires, and the run below it never happens.
    #[tokio::test]
    async fn a_reply_is_not_sent_before_its_record_is_stored() {
        const KEPT_OUT: &str = "kept-out";

        let scratch = Scratch::new("record-first");
        let (fixture, runs) = start_durable(&scratch.0, claim_record(KEPT_OUT), [8_u8; 32]).await;

        let error = fixture
            .client
            .with_idempotency(key(KEPT_OUT))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect_err("the store could not hold the record, so the reply is not sent");
        assert_eq!(
            refusal(&error),
            IdempotencyRefusal::Unrecordable,
            "it ran and the reply is not something this ledger can hold, which is \
             Unrecordable word for word - Interrupted says no reply came out at all"
        );

        // It DID run. That is the whole reason the caller must not be told it
        // succeeded — and the reason the answer is not a refusal that invites an
        // immediate retry.
        assert_eq!(
            runs.runs(),
            1,
            "the handler ran before the record was tried"
        );

        let again = fixture
            .client
            .with_idempotency(key(KEPT_OUT))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect_err("still unknown");
        assert_eq!(refusal(&again), IdempotencyRefusal::Unrecordable);
        assert_eq!(
            runs.runs(),
            1,
            "a key whose record failed must not be free to run again"
        );
    }

    /// A durable store on a service that accepts no key is refused at startup.
    ///
    /// `Keyable::None` builds no ledger, so the store is never read and never
    /// written — and it holds the exclusive claim on its log file for as long
    /// as the service lives. The next service configured to use that ledger
    /// properly is then told `Busy` by one that will never write a record.
    ///
    /// `a_second_process_cannot_claim_a_running_actor` in
    /// `keylink/tests/durable.rs` is the other end of the same rope: it asserts
    /// the `Busy`, and this asserts that a misconfiguration cannot be the
    /// reason for one.
    #[tokio::test]
    async fn a_durable_store_without_keys_is_refused_at_startup() {
        let scratch = Scratch::new("durable-without-keys");
        let files = keylink_persist::file::Files::open(&scratch.0)
            .await
            .expect("opens");
        let records = keylink::DurableRecords::open(&files, &ledger_id())
            .await
            .expect("nobody else holds this ledger");

        let server = local_node().await;
        let service = till::service(Till::default(), Allowlist::anyone())
            .keyable(Keyable::None)
            .records(Records::Durable(records))
            .build();

        let error = KeylinkServer::builder(&server)
            .service(service)
            .spawn()
            .expect_err("a store held and never written must refuse startup");

        assert!(
            matches!(error, Error::Config(ConfigError::DurableWithoutKeys)),
            "got {error:?}"
        );
    }

    /// A key claimed when the process died comes back `Interrupted`.
    ///
    /// Not `Fresh`, which would run the work a second time, and not `Done`, which
    /// would invent a reply nobody produced.
    #[tokio::test]
    async fn an_unsettled_key_recovers_as_interrupted() {
        let scratch = Scratch::new("unsettled");
        let caller = [9_u8; 32];

        {
            let (fixture, _runs) = start_durable(&scratch.0, ROOMY, caller).await;
            let client = fixture.client.clone();
            let calling = tokio::spawn(async move {
                client
                    .with_idempotency(key("in-flight"))
                    .slow(600_000)
                    .await
            });

            // Polled rather than slept: the claim is what this test is about, so it
            // waits for the claim itself instead of for a duration that happens to
            // be long enough on this machine.
            let mut claimed = false;
            for _ in 0..12_000 {
                if fixture
                    .client
                    .key_state(key("in-flight"))
                    .await
                    .expect("asks")
                    == keylink::KeyState::Running
                {
                    claimed = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(claimed, "the call never reached the ledger");

            // The process ending, as closely as one test can spell it: the caller
            // stops waiting and the service stops existing, with the handler still
            // inside its turn.
            calling.abort();
            drop(fixture);
        }

        let (fixture, runs) = start_durable(&scratch.0, ROOMY, caller).await;
        assert_eq!(
            fixture
                .client
                .key_state(key("in-flight"))
                .await
                .expect("asks"),
            keylink::KeyState::Interrupted,
            "a claim that outlived its process is unknown, not vacant"
        );

        let error = fixture
            .client
            .with_idempotency(key("in-flight"))
            .slow(600_000)
            .await
            .expect_err("the key is not free");
        assert_eq!(refusal(&error), IdempotencyRefusal::Interrupted);
        assert_eq!(
            runs.runs(),
            0,
            "nothing about the recovered key may enter a handler"
        );
    }

    /// A refusal before dispatch leaves the key claimable, and a restart must not
    /// take that back.
    ///
    /// `keylink/src/idempotency.rs`'s own header promises it: a refusal that never
    /// reached the handler leaves the key claimable, because that refusal is proof
    /// nothing ran and nothing is owed. The claim is written to the disk before the
    /// layer runs, so without a removal the disk outlives the release and the key
    /// comes back `Interrupted` — permanently unknown, for work that provably never
    /// happened.
    ///
    /// Red-tested by deleting the `pending.push` in `Ledger::release`.
    #[tokio::test]
    async fn a_key_released_before_dispatch_is_vacant_after_a_restart() {
        let scratch = Scratch::new("released");
        let caller = [11_u8; 32];

        {
            let (fixture, runs) =
                start_durable_as(&scratch.0, ROOMY, caller, Limits::default(), true).await;
            let refused = fixture
                .client
                .with_idempotency(key("order-9"))
                .reserve("ADU-014".to_string(), 2)
                .await
                .expect_err("the layer refuses without calling through");
            assert!(
                matches!(refused, Error::Remote(RemoteError::Unauthorized(_))),
                "got {refused:?}"
            );
            assert_eq!(runs.runs(), 0, "nothing was dispatched");
            assert_eq!(
                fixture
                    .client
                    .key_state(key("order-9"))
                    .await
                    .expect("asks"),
                keylink::KeyState::Fresh,
                "a released key is vacant in memory"
            );
        }

        let (fixture, runs) = start_durable(&scratch.0, ROOMY, caller).await;
        assert_eq!(
            fixture
                .client
                .key_state(key("order-9"))
                .await
                .expect("asks"),
            keylink::KeyState::Fresh,
            "and it must still be vacant after a restart"
        );
        let ran = fixture
            .client
            .with_idempotency(key("order-9"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("the key was never spent");
        assert_eq!((ran, runs.runs()), (1, 1), "the retry is a first attempt");
    }

    /// A reply the ledger dropped does not come back from the disk.
    ///
    /// **Driven by the TTL and not by `pool_records`, and that is the whole
    /// design of the test.** A pool eviction is reproducible from the recovered
    /// set — `Ledger::new` runs `evict` before it answers anything — so a
    /// version of this test that shrank the pool passed with the durable
    /// downgrade deleted, and measured recovery's own eviction instead of the
    /// disk. The age clock cannot be reproduced: `Instant` measures this
    /// process's uptime, so every recovered entry is stamped `now`. A `Done`
    /// left on the disk therefore comes back `Done`, which is the difference
    /// this asserts.
    ///
    /// Red-tested by deleting the `pending` extension in `Ledger::expire`: the
    /// restart then reads `Done` and replays a reply the ledger had dropped.
    #[tokio::test]
    async fn a_reply_dropped_by_its_ttl_does_not_come_back_as_done() {
        let scratch = Scratch::new("forgotten");
        let caller = [12_u8; 32];
        let brief = || Limits::default().ttl(Duration::from_millis(50));

        {
            let (fixture, _runs) =
                start_durable_as(&scratch.0, ROOMY, caller, brief(), false).await;
            fixture
                .client
                .with_idempotency(key("order-0"))
                .reserve("ADU-014".to_string(), 2)
                .await
                .expect("runs");

            tokio::time::sleep(Duration::from_millis(80)).await;

            // The first claim expires `order-0` and queues its downgrade; the
            // second drains the queue, because a claim housekeeps before it
            // claims. Two calls rather than a sleep on the drain, so the test
            // waits for the mechanism instead of for a duration.
            for n in 1..3 {
                fixture
                    .client
                    .with_idempotency(key(&format!("order-{n}")))
                    .reserve("ADU-014".to_string(), 2)
                    .await
                    .expect("runs");
            }
            assert_eq!(
                fixture
                    .client
                    .key_state(key("order-0"))
                    .await
                    .expect("asks"),
                keylink::KeyState::Forgotten,
                "the first reply aged out"
            );
        }

        let (fixture, runs) = start_durable_as(&scratch.0, ROOMY, caller, brief(), false).await;
        assert_eq!(
            fixture
                .client
                .key_state(key("order-0"))
                .await
                .expect("asks"),
            keylink::KeyState::Forgotten,
            "a dropped reply must not return, and at-most-once must still hold"
        );
        let refused = fixture
            .client
            .with_idempotency(key("order-0"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect_err("the key is spent");
        assert_eq!(refusal(&refused), IdempotencyRefusal::Forgotten);
        assert_eq!(runs.runs(), 0, "a tombstone must not enter a handler");
    }

    /// The default is unchanged: a service that never names `records` holds its
    /// replies in memory and loses them with the process.
    #[tokio::test]
    async fn a_service_that_names_no_records_still_forgets_on_a_restart() {
        let fixture = start().await;
        let first = fixture
            .client
            .with_idempotency(key("reserve-4471"))
            .reserve("ADU-014".to_string(), 2)
            .await
            .expect("runs");
        assert_eq!(
            fixture
                .client
                .key_state(key("reserve-4471"))
                .await
                .expect("asks"),
            keylink::KeyState::Done
        );
        drop(fixture);

        let fixture = start().await;
        assert_eq!(
            fixture
                .client
                .key_state(key("reserve-4471"))
                .await
                .expect("asks"),
            keylink::KeyState::Fresh,
            "an unconfigured service keeps the behaviour it had before Durable existed"
        );
        assert_eq!(first, 1);
    }
}

keylink::service! {
    service MapCounter;
    rpc Observe(values: std::collections::HashMap<u32, u32>) -> u64;
}
struct MapCounter(Arc<AtomicU64>);
impl map_counter::Handler for MapCounter {
    async fn observe(&self, _cx: &Context, _values: std::collections::HashMap<u32, u32>) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }
}
#[tokio::test]
async fn identical_received_map_bytes_replay_despite_fresh_hashers_and_retry_controls() {
    #[derive(Deserialize)]
    struct Reply {
        body_len: Option<u64>,
        idempotency: Option<keylink::Idempotency>,
        message: keylink::WireResponse<map_counter::Response>,
    }
    async fn call(
        connection: &iroh::endpoint::Connection,
        message: &[u8],
        retry: keylink::Idempotency,
    ) -> keylink::WireResponse<map_counter::Response> {
        let mut header = postcard::to_stdvec(&(Some(0u64), Some(retry))).unwrap();
        header.extend_from_slice(message);
        let mut frame = (header.len() as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&header);
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        send.write_all(&frame).await.unwrap();
        send.finish().unwrap();
        let bytes = recv
            .read_to_end(keylink::MAX_HEADER as usize + 4)
            .await
            .unwrap();
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        assert_eq!(bytes.len(), len + 4);
        let reply: Reply = postcard::from_bytes(&bytes[4..]).unwrap();
        assert_eq!(reply.body_len, Some(0));
        assert!(reply.idempotency.is_none());
        reply.message
    }
    let node = local_node().await;
    let peer = local_node().await;
    let runs = Arc::new(AtomicU64::new(0));
    let _running =
        map_counter::serve(&node, MapCounter(runs.clone()), Allowlist::anyone()).unwrap();
    let connection = peer
        .endpoint()
        .connect(node.endpoint().addr(), map_counter::alpn())
        .await
        .unwrap();
    let values = (0..16u32).map(|n| (n, n + 1)).collect();
    let message = postcard::to_stdvec(&keylink::WireRequest::Call(map_counter::Request::Observe {
        values,
    }))
    .unwrap();
    let key = IdempotencyKey::new("same-map-bytes").unwrap();
    for attempt in 0..16 {
        let retry = if attempt == 0 {
            key.clone().into()
        } else {
            keylink::Idempotency::attaching_for(key.clone(), Duration::from_secs(1))
        };
        assert!(matches!(
            call(&connection, &message, retry).await,
            keylink::WireResponse::Call(Outcome::Ok(map_counter::Response::Observe(1)))
        ));
    }
    // Alter the last scalar value, not transport/retry metadata. Those are
    // different received message bytes and must never replay the first answer.
    let mut changed = message.clone();
    *changed.last_mut().unwrap() ^= 1;
    assert!(matches!(
        call(&connection, &changed, key.into()).await,
        keylink::WireResponse::Call(Outcome::Err(RemoteError::Idempotency(
            IdempotencyRefusal::RequestMismatch
        )))
    ));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
