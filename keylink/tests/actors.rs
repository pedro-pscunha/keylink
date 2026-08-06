//! The keyed actor system, end to end over a real QUIC connection.
//!
//! Two iroh endpoints on the loopback interface, no relay and no discovery, so
//! the transport is genuine and none of this needs a network.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use keylink::{
    ActorClient, ActorId, ActorReply, ActorRequest, Allowlist, Error, Eviction, IdempotencyKey,
    IdempotencyRefusal, MountError, Node, RemoteError, RpcClient, Server, System,
};

// ────────────────────────────────────────────────────────────── two services

keylink::service! {
    service Journal;

    /// Documenting an rpc is the first thing anyone tries, so the macro has to
    /// accept it. `///` desugars to `#[doc = "..."]`, which used to be rejected
    /// with an error pointing at the macro's own internals.
    ///
    /// The attribute reaches the `Request` variant, the `Handler` method, the
    /// `Actor` method, and every client method, so one comment documents the
    /// operation everywhere the operation appears.
    rpc Apply(entry: u64, hold_ms: u64) -> u64;

    /// Every entry this actor has applied, in the order it applied them.
    rpc Entries() -> Vec<u64>;

    #[allow(dead_code)]
    rpc Boom() -> u64;
}

// A second service whose `Request` has an IDENTICAL postcard shape to
// `journal::Request`: one variant, one `u64`, one `u64`. Only the protocol
// name differs, so only the schema hash tells them apart.
//
// That is deliberate. A test built from differently shaped types would pass
// even against a decode-before-verify ordering, because the decode would fail
// on its own. See `same_shape_variant_swap_decodes_as_the_wrong_variant` in
// `node.rs`: postcard encodes a variant by declaration index and carries no
// type identity at all.
keylink::service! {
    service Vault;

    rpc Apply(entry: u64, hold_ms: u64) -> u64;
    rpc Entries() -> Vec<u64>;
    rpc Boom() -> u64;
}

/// State that records what it was actually asked to do, so a test can prove a
/// message never reached it.
#[derive(Clone, Default)]
struct Journal {
    applied: Arc<Mutex<Vec<u64>>>,
}

impl journal::Actor for Journal {
    async fn apply(&mut self, _cx: keylink::Caller<'_>, entry: u64, hold_ms: u64) -> u64 {
        // The await point `abort` cancels at, and the reason `stop` and `abort`
        // leave the state in different shapes.
        tokio::time::sleep(Duration::from_millis(hold_ms)).await;
        self.applied.lock().expect("not poisoned").push(entry);
        entry
    }

    async fn entries(&mut self, _cx: keylink::Caller<'_>) -> Vec<u64> {
        self.applied.lock().expect("not poisoned").clone()
    }

    async fn boom(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        panic!("this message is bad");
    }
}

// ────────────────────────────────────────────────────────────────── fixture

mod common;
use common::local_node;

/// A running actor system and a client connected to it.
struct Fixture {
    system: System,
    client: ActorClient,
    _server: Server,
    _nodes: [Node; 2],
}

async fn start(system: System) -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let running = Server::builder(&server)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = ActorClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");

    Fixture {
        system,
        client,
        _server: running,
        _nodes: [server, client_node],
    }
}

fn id(name: &str) -> ActorId {
    ActorId::new(name).expect("a valid id")
}

fn mounted() -> System {
    System::builder(Eviction::Never).build()
}

// ─────────────────────────────────────────────────────────────── addressing

#[tokio::test]
async fn a_mounted_actor_answers_by_id() {
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(id("obra/4471"), journal::spawn(Journal::default()))
        .expect("the id is free");

    let actor = journal::at(&fixture.client, id("obra/4471"));
    assert_eq!(actor.apply(7, 0).await.unwrap(), 7);
    assert_eq!(actor.entries().await.unwrap(), vec![7]);
}

/// The property the whole design exists for: mounting is a map write, so an
/// actor mounted *after* a peer connected is reachable on that same connection.
/// A per-actor ALPN would need a re-announcement and a fresh handshake.
#[tokio::test]
async fn an_actor_mounted_after_the_client_connected_is_reachable_on_the_same_connection() {
    let fixture = start(mounted()).await;

    // The connection already exists and has negotiated its ALPN.
    let missing = journal::at(&fixture.client, id("later"))
        .entries()
        .await
        .expect_err("nothing is mounted yet");
    assert!(matches!(
        missing,
        Error::Remote(RemoteError::NoSuchActor { .. })
    ));

    fixture
        .system
        .mount(id("later"), journal::spawn(Journal::default()))
        .expect("the id is free");

    assert_eq!(
        journal::at(&fixture.client, id("later"))
            .entries()
            .await
            .unwrap(),
        Vec::<u64>::new(),
        "the same connection must reach an actor that did not exist when it was opened"
    );
}

#[tokio::test]
async fn ten_thousand_actors_are_mounted_and_addressable() {
    const ACTORS: usize = 10_000;

    let fixture = start(System::builder(Eviction::Never).capacity(ACTORS).build()).await;

    let started = Instant::now();
    for n in 0..ACTORS {
        fixture
            .system
            .mount(id(&format!("obra/{n}")), journal::spawn(Journal::default()))
            .expect("every id is distinct");
    }
    let mounting = started.elapsed();
    assert_eq!(fixture.system.len(), ACTORS);

    // The ends and the middle, over one connection.
    for n in [0, ACTORS / 2, ACTORS - 1] {
        let actor = journal::at(&fixture.client, id(&format!("obra/{n}")));
        assert_eq!(
            actor.apply(n as u64, 0).await.unwrap(),
            n as u64,
            "actor obra/{n} must answer"
        );
    }

    // Each actor kept its own state, which is what "keyed" means.
    assert_eq!(
        journal::at(&fixture.client, id("obra/0"))
            .entries()
            .await
            .unwrap(),
        vec![0]
    );
    assert_eq!(
        journal::at(&fixture.client, id("obra/1"))
            .entries()
            .await
            .unwrap(),
        Vec::<u64>::new(),
        "an actor nobody addressed must not have another's state"
    );

    println!("mounted {ACTORS} actors in {mounting:?}");
}

#[tokio::test]
async fn mounting_the_same_id_twice_is_refused_rather_than_replacing_live_state() {
    let system = mounted();
    system
        .mount(id("one"), journal::spawn(Journal::default()))
        .expect("the id is free");

    let error = system
        .mount(id("one"), journal::spawn(Journal::default()))
        .expect_err("the id is taken");

    assert_eq!(error, MountError::Occupied { id: id("one") });
}

// ───────────────────────────────────────────────────────────── the refusals

/// A message to an unmounted id is a typed refusal on a live connection, not a
/// closed connection — and every other actor on it carries on.
#[tokio::test]
async fn the_connection_survives_a_message_to_an_unmounted_actor() {
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(id("real"), journal::spawn(Journal::default()))
        .expect("the id is free");

    let error = journal::at(&fixture.client, id("ghost"))
        .apply(1, 0)
        .await
        .expect_err("nothing is mounted there");
    assert!(
        matches!(error, Error::Remote(RemoteError::NoSuchActor { .. })),
        "got {error:?}"
    );

    assert_eq!(
        journal::at(&fixture.client, id("real"))
            .apply(2, 0)
            .await
            .unwrap(),
        2,
        "a refusal must not poison the connection"
    );
}

/// Telling "never existed" apart from "was stopped" would leak that an id was
/// once real, and the caller's next action is the same either way.
#[tokio::test]
async fn a_stopped_actor_is_indistinguishable_from_one_that_never_existed() {
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(id("was/real"), journal::spawn(Journal::default()))
        .expect("the id is free");
    assert!(fixture.system.stop(&id("was/real")).await);

    let stopped = journal::at(&fixture.client, id("was/real"))
        .apply(1, 0)
        .await
        .expect_err("it is gone");
    let never = journal::at(&fixture.client, id("was/real"))
        .apply(1, 0)
        .await
        .expect_err("and it is still gone");

    assert_eq!(stopped.to_string(), never.to_string());
}

/// The id arrived from a peer, so it never met our constructor.
///
/// The system under test spawns lazily and accepts every id, so the ONLY thing
/// that can refuse an over-long one is the re-check on the receive path. A
/// system without a factory would answer `NoSuchActor` and the test would pass
/// for the wrong reason.
#[tokio::test]
async fn an_oversized_id_is_refused_before_it_reaches_the_registry() {
    let spawned = Arc::new(AtomicUsize::new(0));
    let counter = spawned.clone();
    let fixture = start(
        System::builder(Eviction::Never)
            .spawn_with(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Some(journal::spawn(Journal::default()))
            })
            .build(),
    )
    .await;

    // Encoded as a plain string and decoded as an `ActorId`, which is exactly
    // what a peer can do: `derive(Deserialize)` never calls `new`.
    let oversized: ActorId =
        postcard::from_bytes(&postcard::to_stdvec(&"x".repeat(keylink::MAX_KEY_LEN + 1)).unwrap())
            .expect("a peer can encode what our constructor refuses");

    let error = journal::at(&fixture.client, oversized)
        .apply(1, 0)
        .await
        .expect_err("an over-long id must be refused");

    assert!(
        matches!(error, Error::Remote(RemoteError::Failed(ref why)) if why.contains("actor id")),
        "got {error:?}"
    );
    assert_eq!(
        spawned.load(Ordering::SeqCst),
        0,
        "an over-long id must not reach the registry, let alone spawn state for it"
    );
}

/// RFC-02 test 29, and the reason `Sealed` exists.
///
/// `vault::Request::Apply` and `journal::Request::Apply` have identical postcard
/// encodings. A `Vault` payload therefore decodes cleanly as a `Journal`
/// request — no error, wrong meaning — unless the hash is compared FIRST.
///
/// Made red by comparing the hashes after decoding, or not at all: the actor
/// would then apply entry 99 and the log would not be empty.
#[tokio::test]
async fn a_message_on_a_drifted_schema_is_refused_before_the_payload_is_decoded() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(
            id("ledger"),
            journal::spawn(Journal {
                applied: applied.clone(),
            }),
        )
        .expect("the id is free");

    // A well-formed message of the WRONG type, declaring its own hash.
    let error = fixture
        .client
        .call::<vault::Request, vault::Response>(
            &id("ledger"),
            vault::schema(),
            &vault::Request::Apply {
                entry: 99,
                hold_ms: 0,
            },
            keylink::Body::empty(),
        )
        .await
        .map(|_| ())
        .expect_err("the actor is mounted with a different message type");

    let Error::Remote(RemoteError::ActorSchemaMismatch { ours, theirs }) = error else {
        panic!("expected a schema mismatch, got {error:?}");
    };
    assert_eq!(ours, journal::schema());
    assert_eq!(theirs, vault::schema());

    assert!(
        applied.lock().unwrap().is_empty(),
        "the payload must never have been decoded, let alone handled"
    );

    // And the identical bytes under the right hash DO reach the actor, which is
    // what proves the refusal was about the hash and not about the bytes.
    assert_eq!(
        journal::at(&fixture.client, id("ledger"))
            .apply(99, 0)
            .await
            .unwrap(),
        99
    );
    assert_eq!(*applied.lock().unwrap(), vec![99]);
}

/// A panicking message costs one message. Without the guard the actor's task
/// dies, the registry still lists it, and every later caller is told
/// `NoSuchActor` about an id that is very much configured.
#[tokio::test]
async fn a_panicking_message_costs_one_message_and_not_the_actor() {
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(id("fragile"), journal::spawn(Journal::default()))
        .expect("the id is free");

    let error = journal::at(&fixture.client, id("fragile"))
        .boom()
        .await
        .expect_err("the handler panics");
    assert!(
        matches!(error, Error::Remote(RemoteError::Failed(ref why)) if why.contains("panicked")),
        "got {error:?}"
    );

    assert_eq!(
        journal::at(&fixture.client, id("fragile"))
            .apply(1, 0)
            .await
            .unwrap(),
        1,
        "the actor must still be serving"
    );
}

// ────────────────────────────────────────────────────────────────── lifecycle

#[tokio::test]
async fn a_first_message_spawns_an_actor_when_the_system_was_given_a_factory() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let names = seen.clone();
    let fixture = start(
        System::builder(Eviction::Never)
            .spawn_with(move |id| {
                names.lock().expect("not poisoned").push(id.to_string());
                Some(journal::spawn(Journal::default()))
            })
            .build(),
    )
    .await;

    assert!(fixture.system.is_empty(), "nothing is mounted by hand");

    assert_eq!(
        journal::at(&fixture.client, id("obra/9"))
            .apply(3, 0)
            .await
            .unwrap(),
        3
    );

    assert!(fixture.system.contains(&id("obra/9")));
    assert_eq!(*seen.lock().unwrap(), vec!["obra/9".to_string()]);

    // A second message reuses the actor rather than building another.
    assert_eq!(
        journal::at(&fixture.client, id("obra/9"))
            .entries()
            .await
            .unwrap(),
        vec![3],
        "the second message must reach the actor the first one spawned"
    );
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_factory_that_declines_an_id_produces_no_such_actor() {
    let fixture = start(
        System::builder(Eviction::Never)
            .spawn_with(|id| (id.as_str() == "known").then(|| journal::spawn(Journal::default())))
            .build(),
    )
    .await;

    let error = journal::at(&fixture.client, id("unknown"))
        .apply(1, 0)
        .await
        .expect_err("the factory declines this id");
    assert!(
        matches!(error, Error::Remote(RemoteError::NoSuchActor { .. })),
        "got {error:?}"
    );
    assert!(fixture.system.is_empty());
}

/// Lazy spawn grows the registry from the network, so the cap is what stops it
/// being a memory-exhaustion target.
#[tokio::test]
async fn lazy_spawn_stops_at_the_capacity() {
    let fixture = start(
        System::builder(Eviction::Never)
            .capacity(2)
            .spawn_with(|_| Some(journal::spawn(Journal::default())))
            .build(),
    )
    .await;

    for n in 0..2 {
        assert_eq!(
            journal::at(&fixture.client, id(&format!("a{n}")))
                .apply(n, 0)
                .await
                .unwrap(),
            n
        );
    }

    let error = journal::at(&fixture.client, id("a2"))
        .apply(2, 0)
        .await
        .expect_err("the system is full");
    assert!(
        matches!(error, Error::Remote(RemoteError::Failed(ref why)) if why.contains("capacity")),
        "got {error:?}"
    );
    assert_eq!(fixture.system.len(), 2);
}

/// An actor system that only grows is a leak, and the sweeper runs on its own
/// without anyone asking.
///
/// One-sided on purpose. Whether a BUSY actor survives is a claim about the
/// clock, and it belongs in the unit tests, where time is paused and the answer
/// is not a race — see `eviction_is_by_last_message_and_not_by_mount_time`.
/// Asserting it here needed the background sweeper not to fire between two
/// touches, which is a race this test cannot win under load and did not.
#[tokio::test]
async fn an_idle_actor_is_evicted_without_anyone_asking() {
    let fixture = start(System::builder(Eviction::Idle(Duration::from_millis(60))).build()).await;
    fixture
        .system
        .mount(id("idle"), journal::spawn(Journal::default()))
        .expect("the id is free");
    assert_eq!(fixture.system.len(), 1);

    // No `evict_idle` call: the system's own sweeper has to do this.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        fixture.system.is_empty(),
        "an actor nobody addressed must be evicted by the sweeper alone"
    );
}

/// Eviction is `stop`, not `abort`: the actor is not cut off mid-turn, and its
/// caller gets an answer rather than a severed stream.
#[tokio::test]
async fn eviction_never_cuts_off_a_message_in_flight() {
    let fixture = start(System::builder(Eviction::Idle(Duration::from_millis(30))).build()).await;
    fixture
        .system
        .mount(id("slow"), journal::spawn(Journal::default()))
        .expect("the id is free");

    // In flight for far longer than the eviction budget.
    let answered = journal::at(&fixture.client, id("slow")).apply(5, 250).await;

    assert_eq!(
        answered.unwrap(),
        5,
        "the message in flight must be answered"
    );
}

/// Eviction never touches the ALPN, so a peer connected before it can still
/// reach an actor spawned after it.
#[tokio::test]
async fn the_connection_outlives_the_actors_on_it() {
    let fixture = start(
        System::builder(Eviction::Idle(Duration::from_millis(40)))
            .spawn_with(|_| Some(journal::spawn(Journal::default())))
            .build(),
    )
    .await;

    assert_eq!(
        journal::at(&fixture.client, id("first"))
            .apply(1, 0)
            .await
            .unwrap(),
        1
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(fixture.system.is_empty(), "the first actor was evicted");

    assert_eq!(
        journal::at(&fixture.client, id("second"))
            .apply(2, 0)
            .await
            .unwrap(),
        2,
        "the same connection must still work after every actor on it went away"
    );
}

// ───────────────────────────────────────────────────────────── stop vs abort

/// `stop` drains: the message in flight finishes and its effect on the state is
/// real. Made red by making `stop` an alias of `abort`.
#[tokio::test]
async fn stop_lets_the_message_in_flight_finish() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(
            id("doc"),
            journal::spawn(Journal {
                applied: applied.clone(),
            }),
        )
        .expect("the id is free");

    let client_call = {
        let client = &fixture.client;
        async move { journal::at(client, id("doc")).apply(1, 200).await }
    };
    let stopping = async {
        // Long enough that the message is inside the actor, short enough that
        // it is still running.
        tokio::time::sleep(Duration::from_millis(60)).await;
        fixture.system.stop(&id("doc")).await
    };

    let (answered, was_mounted) = tokio::join!(client_call, stopping);

    assert!(was_mounted);
    assert_eq!(answered.unwrap(), 1, "the caller must still get its answer");
    assert_eq!(
        *applied.lock().unwrap(),
        vec![1],
        "stop preserves the work the actor had already accepted"
    );
}

/// `abort` cancels: the task's future is dropped at its await point, the state
/// goes with it, and the mutation never happened. Made red by making `abort` an
/// alias of `stop`.
#[tokio::test]
async fn abort_destroys_the_message_in_flight() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(
            id("doc"),
            journal::spawn(Journal {
                applied: applied.clone(),
            }),
        )
        .expect("the id is free");

    let client_call = {
        let client = &fixture.client;
        async move { journal::at(client, id("doc")).apply(1, 200).await }
    };
    let aborting = async {
        tokio::time::sleep(Duration::from_millis(60)).await;
        fixture.system.abort(&id("doc")).await
    };

    let (answered, was_mounted) = tokio::join!(client_call, aborting);

    assert!(was_mounted);
    assert!(
        answered.is_err(),
        "an aborted turn has no answer to give, got {answered:?}"
    );
    assert!(
        applied.lock().unwrap().is_empty(),
        "abort destroys the state, so the mutation the message was making is gone"
    );
}

#[tokio::test]
async fn stopping_an_unmounted_id_says_so_rather_than_pretending() {
    let system = mounted();
    assert!(!system.stop(&id("nobody")).await);
    assert!(!system.abort(&id("nobody")).await);
}

// ─────────────────────────────────────────────────────────────── concurrency

/// The actor bargain, both halves in one test: one actor is serial, and two
/// actors are not. Made red by serialising dispatch behind one lock, or by
/// handing each message to a fresh task.
#[tokio::test]
async fn different_actors_run_concurrently_while_one_actor_stays_serial() {
    const HOLD_MS: u64 = 200;

    let fixture = start(mounted()).await;
    for name in ["a", "b"] {
        fixture
            .system
            .mount(id(name), journal::spawn(Journal::default()))
            .expect("the id is free");
    }

    let a = journal::at(&fixture.client, id("a"));
    let b = journal::at(&fixture.client, id("b"));

    let across = Instant::now();
    let (first, second) = tokio::join!(a.apply(1, HOLD_MS), b.apply(1, HOLD_MS));
    let across = across.elapsed();
    first.unwrap();
    second.unwrap();

    let within = Instant::now();
    let (third, fourth) = tokio::join!(a.apply(2, HOLD_MS), a.apply(3, HOLD_MS));
    let within = within.elapsed();
    third.unwrap();
    fourth.unwrap();

    let hold = Duration::from_millis(HOLD_MS);
    assert!(
        across < hold * 3 / 2,
        "two actors must not queue behind each other: {across:?} for two {hold:?} messages"
    );
    assert!(
        within >= hold * 2,
        "one actor must handle its messages one at a time: {within:?} for two {hold:?} messages"
    );

    // Serial, and therefore in order, with no lock anywhere in the handler.
    assert_eq!(a.entries().await.unwrap(), vec![1, 2, 3]);

    println!("across two actors {across:?}, within one actor {within:?} (hold {hold:?})");
}

// ─────────────────────────────────────────────────────────────── the contract

/// Two services with identical message shapes must not share a hash, or the
/// registry cannot tell a `Vault` message from a `Journal` one.
#[test]
fn the_schema_hash_separates_two_services_with_identical_shapes() {
    assert_ne!(journal::schema(), vault::schema());
}

/// The actor system's ALPN covers the envelope in both directions. The hash a
/// caller declares covers the actor's own types. Neither is the other.
#[test]
fn the_actor_alpn_is_not_any_services_alpn() {
    assert_ne!(System::alpn(), journal::alpn());
    assert_ne!(System::alpn(), vault::alpn());
}

/// RFC-04 §14.10 item 6: the defect was reproduced in isolation but never
/// against the crate's own `RemoteError`. This is that test.
///
/// `NoSuchActor { id: String }` has the same postcard encoding as
/// `Failed(String)`, so inserting it changed what an existing refusal MEANS.
/// That is only safe because the ALPN moves with it. This asserts the moving.
#[test]
fn adding_a_remote_error_variant_moves_the_alpn() {
    /// `RemoteError` as it was before the actor variants existed.
    #[derive(postcard_schema::Schema)]
    #[allow(dead_code)]
    enum Before {
        Unauthorized(String),
        TimedOut { millis: u64 },
        Failed(String),
    }

    #[derive(postcard_schema::Schema)]
    #[allow(dead_code)]
    enum OutcomeBefore {
        Ok(journal::Response),
        Err(Before),
    }

    #[derive(postcard_schema::Schema)]
    #[allow(dead_code)]
    struct ContractBefore {
        request: keylink::Envelope<journal::Request>,
        response: keylink::Envelope<OutcomeBefore>,
    }

    const P: &str = concat!(env!("CARGO_PKG_NAME"), "/Journal");
    assert_ne!(
        keylink::derive_alpn::<ContractBefore>(P),
        journal::alpn().to_vec(),
        "a new RemoteError variant must move the ALPN, or a peer built before it \
         decodes one refusal as another"
    );
}

// ──────────────────────────────────────────────────────────── who is calling

// A third service, so that adding a method to `journal` does not break the
// deliberate shape-for-shape pairing it has with `vault`. A `///` here would be
// an unused doc comment: rustdoc does not document a macro invocation.
keylink::service! {
    service Doorman;

    rpc WhoCalled() -> Option<String>;
}

struct Doorman;

impl doorman::Actor for Doorman {
    async fn who_called(&mut self, cx: keylink::Caller<'_>) -> Option<String> {
        cx.peer().map(|cx| cx.peer().to_string())
    }
}

/// RFC-04 §12.2 test 10, on the path the RFC put it: a keyed actor sees the
/// proven key of its caller.
///
/// It had never been written until this file. The identity used to arrive
/// through an `Actor::caller` hook that shipped with a default empty body,
/// which nothing in the tree overrode — a hook with no implementor and no test
/// is indistinguishable from one that does not work. It is an argument now, so
/// a method that wants the caller cannot fail to be given one.
///
/// Made red by passing `Caller::Local` in the mailbox loop.
#[tokio::test]
async fn a_keyed_actor_sees_the_proven_key_of_its_caller() {
    let server = local_node().await;
    let client_node = local_node().await;
    let system = mounted();
    let _running = Server::builder(&server)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = ActorClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");

    system
        .mount(id("gate/1"), doorman::spawn(Doorman))
        .expect("the id is free");

    assert_eq!(
        doorman::at(&client, id("gate/1"))
            .who_called()
            .await
            .unwrap(),
        Some(client_node.id().to_string()),
        "the actor must see the dialing peer's proven key, not None"
    );
}

// ───────────────────────────────────────────── a key against a keyed actor

/// A response the turn produces and the wire cannot carry.
///
/// The `Serialize` is written by hand because no derived one can fail: postcard
/// reports a serialization error only from its flavor, and the `Vec` flavor
/// `ActorReply::encode` uses has none. Without it the arm that decides what a
/// turn owes when it ran and produced nothing sendable is unreachable from a
/// test, and an unreachable arm is a decision nobody has checked.
#[derive(Debug, Clone, serde::Deserialize, postcard_schema::Schema)]
pub struct Untransmittable;

impl serde::Serialize for Untransmittable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this reply never encodes"))
    }
}

// A fourth service, so the shape-for-shape pairing between `journal` and
// `vault` survives a method being added here.
keylink::service! {
    service Ward;

    /// Counts the turns that reached the state. A retry that ran a second time
    /// is visible HERE and nowhere else: a replayed reply is byte identical to
    /// a re-run one, which is the whole reason the ledger records the frame.
    rpc Touch() -> u64;

    /// The count, read without a key, so reading it never settles one.
    rpc Touches() -> u64;

    /// Mutates the state and then answers with something unencodable.
    rpc Unsendable() -> Untransmittable;

    /// Mutates the state and then panics, so the turn was entered and its
    /// effect is whatever the panic left behind.
    rpc Explode() -> u64;
}

#[derive(Default)]
struct Ward {
    touches: u64,
}

impl ward::Actor for Ward {
    async fn touch(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        self.touches += 1;
        self.touches
    }

    async fn touches(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        self.touches
    }

    async fn unsendable(&mut self, _cx: keylink::Caller<'_>) -> Untransmittable {
        self.touches += 1;
        Untransmittable
    }

    async fn explode(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        self.touches += 1;
        panic!("this turn dies after it mutated");
    }
}

/// One `Ward`, reachable both the typed way and the way a hand-rolled peer
/// reaches it.
///
/// The typed `ActorClient` exposes no `with_idempotency`, so nothing in
/// keylink's own client sends a key to a keyed actor. The field is on the
/// ENVELOPE of the system's ALPN, which is an ordinary
/// `Envelope<WireRequest<ActorRequest>>`, so a peer that speaks the protocol by
/// hand reaches it through the public `RpcClient`. Latent, not impossible —
/// which is why these are integration tests and not unit ones.
struct Keyed {
    /// Carries the key. Speaks the envelope directly.
    raw: RpcClient<ActorRequest, ActorReply>,
    /// Reads the actor's own counter, without a key.
    client: ActorClient,
    _system: System,
    _server: Server,
    _nodes: [Node; 2],
}

async fn start_keyed() -> Keyed {
    let server = local_node().await;
    let client_node = local_node().await;
    let system = mounted();
    let running = Server::builder(&server)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let raw = RpcClient::connect(&client_node, server.endpoint().addr(), System::alpn())
        .await
        .expect("the raw client dials the actor system");
    let client = ActorClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the typed client dials the actor system");
    system
        .mount(id("ward"), ward::spawn(Ward::default()))
        .expect("the id is free");

    Keyed {
        raw,
        client,
        _system: system,
        _server: running,
        _nodes: [server, client_node],
    }
}

/// One message to `ward`, under a key.
async fn keyed(
    fixture: &Keyed,
    key: &str,
    request: &ward::Request,
) -> Result<ward::Response, Error> {
    let envelope =
        ActorRequest::new(id("ward"), ward::schema(), request).expect("the message encodes");
    keyed_envelope(fixture, key, envelope).await
}

/// The same, for an envelope built by hand — the only way to reach a payload
/// the actor's own loop cannot decode.
async fn keyed_envelope(
    fixture: &Keyed,
    key: &str,
    envelope: ActorRequest,
) -> Result<ward::Response, Error> {
    let (reply, _body) = fixture
        .raw
        .with_idempotency(IdempotencyKey::new(key).expect("a valid key"))
        .call(envelope, keylink::Body::empty())
        .await?;
    reply.decode()
}

async fn touches(fixture: &Keyed) -> u64 {
    ward::at(&fixture.client, id("ward"))
        .touches()
        .await
        .expect("the actor answers")
}

/// The defect: of the three generated dispatch paths, the keyed `System` loop
/// marked the progress marker nowhere. A key claimed for a message routed there
/// was RELEASED once the turn had already mutated the actor, so the caller's
/// retry ran the work again — at-least-once out of the module whose one promise
/// is at-most-once.
///
/// Proved on the actor's counter and not on the reply, because a replayed reply
/// and a re-run one are byte identical by construction. Made red by deleting
/// the `completed()` call in `spawn`.
#[tokio::test]
async fn a_keyed_call_that_ran_a_turn_does_not_leave_the_key_claimable() {
    let fixture = start_keyed().await;

    let first = keyed(&fixture, "touch", &ward::Request::Touch {})
        .await
        .expect("the first call runs");
    assert!(matches!(first, ward::Response::Touch(1)), "got {first:?}");

    let again = keyed(&fixture, "touch", &ward::Request::Touch {})
        .await
        .expect("the retry is answered from the record");
    assert!(matches!(again, ward::Response::Touch(1)), "got {again:?}");

    assert_eq!(
        touches(&fixture).await,
        1,
        "the retry must not have entered the turn a second time"
    );
}

/// A payload the actor's loop cannot decode is refused before any user code
/// runs, so nothing was entered and the key stays claimable. Settling it would
/// refuse every retry of a message the actor never saw.
///
/// The declared hash is the RIGHT one, so `System::deliver`'s own comparison
/// passes and the message reaches the mailbox loop. That is what makes this a
/// test of `Sealed::open` inside the loop rather than of the check above it.
/// Made red by marking `dispatched()` above the `match` instead of inside its
/// `Ok` arm.
#[tokio::test]
async fn a_keyed_call_whose_payload_does_not_decode_leaves_the_key_claimable() {
    let fixture = start_keyed().await;

    // A variant index no variant has.
    let garbage = ActorRequest::new(id("ward"), ward::schema(), &250u32).expect("the bytes encode");
    let error = keyed_envelope(&fixture, "touch", garbage)
        .await
        .expect_err("the payload does not decode");
    assert!(
        matches!(error, Error::Remote(RemoteError::Failed(_))),
        "got {error:?}"
    );
    assert_eq!(
        touches(&fixture).await,
        0,
        "a payload that does not decode must not reach the turn"
    );

    let ran = keyed(&fixture, "touch", &ward::Request::Touch {})
        .await
        .expect("the key must still be claimable");
    assert!(matches!(ran, ward::Response::Touch(1)), "got {ran:?}");
}

/// The `ActorReply::encode` decision, pinned. The turn ran and mutated the
/// state, and produced nothing the wire can carry.
///
/// Not `completed`: that would record the encoding refusal as this call's
/// settled reply and replay that refusal to every retry, for work that really
/// happened. Not released either: that would run the work again. `Interrupted`
/// is what is left, and it is the truth — "it ran, and whether it took effect
/// is unknown". Made red by moving `completed()` above the encode.
#[tokio::test]
async fn a_keyed_call_whose_reply_does_not_encode_settles_the_key_interrupted() {
    let fixture = start_keyed().await;

    let error = keyed(&fixture, "touch", &ward::Request::Unsendable {})
        .await
        .expect_err("the reply does not encode");
    assert!(
        matches!(error, Error::Remote(RemoteError::Failed(_))),
        "got {error:?}"
    );
    assert_eq!(
        touches(&fixture).await,
        1,
        "the turn ran, which is what makes the key owe something"
    );

    let retried = keyed(&fixture, "touch", &ward::Request::Unsendable {})
        .await
        .expect_err("the key settled, so the retry is refused rather than run");
    assert!(
        matches!(
            retried,
            Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Interrupted))
        ),
        "got {retried:?}"
    );
    assert_eq!(
        touches(&fixture).await,
        1,
        "the retry must not run the turn a second time"
    );
}

/// The panic arm, for the same reason. A turn that panicked was entered, and
/// whatever it mutated before it panicked is in the state the actor carries on
/// with. Giving the key back would run that half-done work again.
///
/// This is also what guards the `dispatched()` mark itself: delete it and this
/// key is released rather than settled, which is the defect one arm along.
#[tokio::test]
async fn a_keyed_call_whose_turn_panicked_settles_the_key_interrupted() {
    let fixture = start_keyed().await;

    let error = keyed(&fixture, "touch", &ward::Request::Explode {})
        .await
        .expect_err("the turn panics");
    assert!(
        matches!(error, Error::Remote(RemoteError::Failed(ref why)) if why.contains("this turn dies")),
        "got {error:?}"
    );
    assert_eq!(
        touches(&fixture).await,
        1,
        "the turn ran, and mutated, before it panicked"
    );

    let retried = keyed(&fixture, "touch", &ward::Request::Explode {})
        .await
        .expect_err("the key settled, so the retry is refused rather than run");
    assert!(
        matches!(
            retried,
            Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Interrupted))
        ),
        "got {retried:?}"
    );
    assert_eq!(
        touches(&fixture).await,
        1,
        "the retry must not run the turn a second time"
    );
}
