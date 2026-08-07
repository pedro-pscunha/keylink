//! The keyed actor system, end to end over a real QUIC connection.
//!
//! Two iroh endpoints on the loopback interface, no relay and no discovery, so
//! the transport is genuine and none of this needs a network.
//!
//! # One system per service, and what that changed here
//!
//! Every fixture in this file used to share one node-wide `keylink::System`, and
//! several tests mounted actors of unrelated services into it. That cannot be
//! written any more: a system is `journal::System`, `M` is that service's own
//! mailbox type, and mounting a `vault` actor into a `journal` system is `E0308`
//! at the call site. `keylink/src/representability.rs` holds that property as a
//! probe; the consequence here is that each service brings its own fixture.
//!
//! Three tests changed shape rather than moving, and each says so where it
//! stands:
//!
//! - the drifted-schema refusal is now a refused **handshake**, because a keyed
//!   ALPN hashes `Addressed<Request>`;
//! - the mailbox-slot property is pinned on a malformed id, which is the refusal
//!   that still happens before routing;
//! - a key reaches a keyed actor through the generated client rather than through
//!   a hand-built envelope on a raw `RpcClient`, which is the capability this
//!   wave was for.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use keylink::{
    ActorId, Addressed, Allowlist, Body, BodyReader, Context, Ended, Error, Eviction,
    IdempotencyKey, IdempotencyRefusal, Middleware, MountError, Next, Node, Outcome, QueueTimeout,
    RemoteError, RpcClient, Server, Timeout,
};
// The port and its file adapter, for the one thing a counter cannot show: what a
// detached actor is still HOLDING. `Claim` is deliberately not in scope — nothing
// here loads or stores, and `mise run framing` is what keeps that honest.
use keylink_persist::{file::Files, Snapshots, StateId};
use tokio::sync::{mpsc, oneshot, Semaphore};

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
// `journal::Request`: same variants, same fields. Only the protocol name
// differs, so only the derived ALPN tells them apart.
//
// That is deliberate, and it is what the handshake test below rests on. A test
// built from differently shaped types would pass even against a
// decode-before-verify ordering, because the decode would fail on its own. See
// `same_shape_variant_swap_decodes_as_the_wrong_variant` in `node.rs`: postcard
// encodes a variant by declaration index and carries no type identity at all.
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

/// A `vault` actor, so that service can actually be served — which the
/// handshake test needs, and which is also what makes it a test about two live
/// contracts rather than about one client dialling nothing.
struct Vault;

impl vault::Actor for Vault {
    async fn apply(&mut self, _cx: keylink::Caller<'_>, entry: u64, _hold_ms: u64) -> u64 {
        entry
    }

    async fn entries(&mut self, _cx: keylink::Caller<'_>) -> Vec<u64> {
        Vec::new()
    }

    async fn boom(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        0
    }
}

// ────────────────────────────────────────────────────────────────── fixture

mod common;
use common::local_node;

/// A running `journal` actor system and a client connected to it.
struct Fixture {
    system: journal::System,
    client: journal::KeyedClient,
    _server: Server,
    _nodes: [Node; 2],
}

async fn start(system: journal::System) -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let running = Server::builder(&server)
        .service(journal::keyed_service(&system, Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = journal::KeyedClient::connect(&client_node, server.endpoint().addr())
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

fn mounted() -> journal::System {
    journal::System::builder(Eviction::Never).build()
}

/// An id a peer can send and our constructor cannot build.
///
/// Encoded as a plain string and decoded as an `ActorId`, which is exactly what
/// `derive(Deserialize)` lets a peer do: it builds the value field by field and
/// never calls `new`.
fn oversized_id() -> ActorId {
    postcard::from_bytes(&postcard::to_stdvec(&"x".repeat(keylink::MAX_KEY_LEN + 1)).unwrap())
        .expect("a peer can encode what our constructor refuses")
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

    let fixture = start(
        journal::System::builder(Eviction::Never)
            .capacity(ACTORS)
            .build(),
    )
    .await;

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
    assert!(fixture
        .system
        .shutdown(&id("was/real"), Duration::from_secs(5))
        .await
        .is_some());

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
///
/// `ActorId::validate` moved from `System::deliver` into `deliver_to_actor` with
/// this wave, and it moved with its ordering intact — before `route`, so an
/// over-long id costs no lookup and no lazily spawned state. This test and the
/// spawn counter are what say so. Made red by moving the `validate` call below
/// the `route` call in `actor/remote.rs`.
#[tokio::test]
async fn an_oversized_id_is_refused_before_it_reaches_the_registry() {
    let spawned = Arc::new(AtomicUsize::new(0));
    let counter = spawned.clone();
    let fixture = start(
        journal::System::builder(Eviction::Never)
            .spawn_with(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Some(journal::spawn(Journal::default()))
            })
            .build(),
    )
    .await;

    let error = journal::at(&fixture.client, oversized_id())
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

/// **One of the two named replacements for
/// `a_message_on_a_drifted_schema_is_refused_before_the_payload_is_decoded`, and
/// the one that covers the case the old test was NOT about.**
///
/// Be exact about the split, because an earlier version of this note claimed the
/// whole inheritance and pointed at one half of it. The deleted guard watched
/// **two builds of one service** drift apart — one peer's actor declaring a hash
/// the other's mount did not agree with. This test watches **two different
/// services**, which is a neighbouring property and not that one. The same-build
/// drift half lives in two other tests, and both are needed to make the claim:
/// `tests/protocol.rs::the_keyed_alpn_hashes_the_keyed_wire_type` pins that a
/// keyed ALPN hashes `Envelope<WireRequest<Addressed<Request>>>` — so a changed
/// request type moves the string a peer must present — and
/// `adding_a_remote_error_variant_moves_the_alpn`, below in this file, pins the
/// same for the reply direction, where the drift is a variant index rather than a
/// field. Between them, a rebuild that changes what either direction encodes
/// cannot present the old ALPN.
///
/// That test carried the assertion *"the payload must never have been decoded,
/// let alone handled"*, and it was red-tested by comparing the declared hash
/// after decoding, or not at all. It is deleted with the mechanism it watched:
/// there is no declared hash on the wire any more, and no
/// `RemoteError::ActorSchemaMismatch` to carry the refusal.
///
/// The property transfers one layer out, and gets stronger. `vault::Request` and
/// `journal::Request` still have identical postcard encodings, so a `vault`
/// payload still decodes cleanly as a `journal` one — no error, wrong meaning.
/// What refuses it now is the ALPN: a keyed contract hashes
/// `Envelope<WireRequest<Addressed<Request>>>` under `<crate>/<Service>/keyed`,
/// so the two services present different bytestrings and iroh aborts the
/// connection before a byte of payload exists. The refusal is earlier, cheaper,
/// and it costs the peer a handshake rather than a stream.
///
/// The other half of the old test — that the actor was never touched — is now
/// unrepresentable rather than refused, and is held as a compile-time probe in
/// `keylink/src/representability.rs`: a `journal::System` cannot hold a `vault`
/// actor.
///
/// **What refuses is the ALPN string, not the type parameter**, and the
/// distinction is worth being exact about because the first draft of this test
/// got it wrong. `RpcClient::connect` takes the ALPN as an untyped
/// `&'static [u8]`, so `RpcClient::<Addressed<vault::Request>, _>::connect(..,
/// journal::keyed_alpn())` connects perfectly happily — the types are not part of
/// the handshake, the bytes they hash into are. What makes the pairing safe is
/// that `vault::KeyedClient` is the only thing that calls `vault::keyed_alpn()`,
/// and both are generated from one declaration.
///
/// Made red by giving both services the same keyed protocol name and hashing a
/// contract that names neither `Addressed` nor `WireRequest` — which is exactly
/// what `tests/protocol.rs::the_keyed_alpn_hashes_the_keyed_wire_type` refuses.
#[tokio::test]
async fn a_peer_on_another_services_keyed_alpn_is_refused_at_the_handshake() {
    let journal_only = local_node().await;
    let both = local_node().await;
    let client_node = local_node().await;

    // The identical shape is the point: this is what would decode cleanly if
    // anything let the bytes through.
    assert_eq!(
        postcard::to_stdvec(&journal::Request::Apply {
            entry: 99,
            hold_ms: 0
        })
        .unwrap(),
        postcard::to_stdvec(&vault::Request::Apply {
            entry: 99,
            hold_ms: 0
        })
        .unwrap(),
        "the two payloads must be byte-identical, or the refusal below proves nothing"
    );

    let journals = mounted();
    journals
        .mount(id("ledger"), journal::spawn(Journal::default()))
        .expect("the id is free");
    let _serving_journal = Server::builder(&journal_only)
        .service(journal::keyed_service(&journals, Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");

    // A peer built against `vault` cannot reach a node serving `journal`'s
    // system, even though its bytes would have decoded there.
    let refused = vault::KeyedClient::connect(&client_node, journal_only.endpoint().addr())
        .await
        .map(|_| ())
        .expect_err("a vault peer must not complete a handshake with journal's system");
    assert!(
        matches!(refused, Error::SchemaMismatch { .. }),
        "the refusal must name the schema, not the network: got {refused:?}"
    );

    // Journal's own client reaches it on the same node.
    let journal_client =
        journal::KeyedClient::connect(&client_node, journal_only.endpoint().addr())
            .await
            .expect("journal's own keyed client dials journal's ALPN");
    assert_eq!(
        journal::at(&journal_client, id("ledger"))
            .apply(99, 0)
            .await
            .unwrap(),
        99
    );

    // And the two identically shaped services coexist on one node, each holding
    // its own actor under the same id.
    let more_journals = mounted();
    let vaults: vault::System = vault::System::builder(Eviction::Never).build();
    more_journals
        .mount(id("ledger"), journal::spawn(Journal::default()))
        .expect("the id is free");
    vaults
        .mount(id("ledger"), vault::spawn(Vault))
        .expect("the id is free");
    let _serving_both = Server::builder(&both)
        .service(journal::keyed_service(&more_journals, Allowlist::anyone()).build())
        .service(vault::keyed_service(&vaults, Allowlist::anyone()).build())
        .spawn()
        .expect("two identically shaped services, two distinct keyed ALPNs");

    let to_journal = journal::KeyedClient::connect(&client_node, both.endpoint().addr())
        .await
        .expect("dials journal's ALPN");
    let to_vault = vault::KeyedClient::connect(&client_node, both.endpoint().addr())
        .await
        .expect("dials vault's ALPN");
    assert_eq!(
        journal::at(&to_journal, id("ledger"))
            .apply(99, 0)
            .await
            .unwrap(),
        99
    );
    assert_eq!(
        vault::at(&to_vault, id("ledger"))
            .apply(99, 0)
            .await
            .unwrap(),
        99
    );
    assert_eq!(
        journal::at(&to_journal, id("ledger"))
            .entries()
            .await
            .unwrap(),
        vec![99],
        "the two systems hold different actors under the same id, on one node"
    );
    assert_eq!(
        vault::at(&to_vault, id("ledger")).entries().await.unwrap(),
        Vec::<u64>::new(),
        "and neither can see the other's state"
    );
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

/// **The keyed twin the singleton test asked for and never had.**
///
/// `tests/singleton.rs::both_halves_of_a_lost_turn_carry_their_own_prefix` pins
/// that a panic reaches a peer as `Failed("panicked: ..")` and an observer's veto
/// as `Failed("observer refused: ..")`, because `RemoteError::Failed` carries a
/// `String` rather than a variant — a variant would sit inside the hashed
/// contract and move every service's ALPN — so **the string is the whole
/// discriminator**.
///
/// It had no keyed counterpart, and the two paths drifted underneath that gap:
/// the keyed loop rendered a panic as `Panic`'s own `Display` while the
/// singleton's dispatch wrapped it in a second prefix, so one peer read `the
/// actor panicked: this message is bad` and the other `panicked: the actor
/// panicked: this message is bad` for the same fault. Both loops were green
/// throughout, because neither test could see the other path.
///
/// This asserts the property that closes it: **the same panic, served both ways
/// from one node, reaches a peer as byte-identical text.** Not "both contain
/// panicked" — the test above this one already says that, and it is exactly what
/// went on passing while the two disagreed.
///
/// The two ALPNs on one node are the point of the fixture rather than an
/// accident: a service can be served as a singleton and as an actor system at
/// once, so this is one process, one state type, two dispatch paths.
///
/// A keyed actor has no observers, so only the panic half has a twin here. The
/// refusal half stays where the observers are.
#[tokio::test]
async fn one_panic_reads_the_same_whichever_way_the_service_is_served() {
    let server = local_node().await;
    let caller = local_node().await;

    let system = mounted();
    system
        .mount(id("fragile"), journal::spawn(Journal::default()))
        .expect("the id is free");
    let singleton = journal::actor(Journal::default());

    let running = Server::builder(&server)
        .service(journal::keyed_service(&system, Allowlist::anyone()).build())
        .service(singleton.service(Allowlist::anyone()).build())
        .spawn()
        .expect("the keyed and singleton ALPNs are distinct");

    let to_keyed = journal::KeyedClient::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials the actor system");
    let to_singleton = journal::Client::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials the same node's singleton");

    let keyed = journal::at(&to_keyed, id("fragile"))
        .boom()
        .await
        .expect_err("the handler panics");
    let single = to_singleton
        .boom()
        .await
        .expect_err("the same handler panics");

    let (Error::Remote(RemoteError::Failed(keyed)), Error::Remote(RemoteError::Failed(single))) =
        (&keyed, &single)
    else {
        panic!("both paths answer a panic with Failed; got {keyed:?} and {single:?}");
    };
    assert_eq!(
        keyed, single,
        "a peer must not be able to tell which dispatch path served it by reading \
         how a panic was rendered"
    );
    assert!(
        keyed.starts_with("panicked: "),
        "and the prefix a peer discriminates on has to be there; got {keyed:?}"
    );
    assert!(
        keyed.contains("this message is bad"),
        "carrying what the handler panicked with; got {keyed:?}"
    );

    to_keyed.close().await.expect("closes");
    to_singleton.close().await.expect("closes");
    running.shutdown().await.expect("stops");
}

// ────────────────────────────────────────────────────────────────── lifecycle

#[tokio::test]
async fn a_first_message_spawns_an_actor_when_the_system_was_given_a_factory() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let names = seen.clone();
    let fixture = start(
        journal::System::builder(Eviction::Never)
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
        journal::System::builder(Eviction::Never)
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
        journal::System::builder(Eviction::Never)
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
    let fixture = start(
        journal::System::builder(Eviction::Idle {
            after: Duration::from_millis(60),
            shutdown_budget: Duration::from_secs(5),
        })
        .build(),
    )
    .await;
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

/// Eviction is `shutdown`, not `abort`: the actor is not cut off mid-turn, and
/// its caller gets an answer rather than a severed stream.
///
/// # What the two numbers mean, because this test changed shape with the budget
///
/// It was `Eviction::Idle(30ms)` against a 250 ms turn, under the name
/// `eviction_never_cuts_off_a_message_in_flight`. "Never" was true, and it was
/// true for the wrong reason: the sweeper dropped the entry and **detached** the
/// task, so nothing could have cut the turn off because nothing was waiting for
/// it. The turn finished on an actor that no longer existed as far as the system
/// was concerned, and if it had never finished, nothing would ever have noticed.
///
/// With a budget the two numbers are independent and both are deliberate. `after`
/// stays 30 ms: the actor is stale almost at once, which is what puts the
/// eviction inside the turn. `shutdown_budget` is five seconds, comfortably more
/// than the 250 ms the turn holds, and **that** is what buys the answer. The name
/// says `inside_its_shutdown_budget` because that is the condition; the converse
/// — a turn that outlives the budget being destroyed — is
/// `an_eviction_whose_budget_expires_destroys_the_actor` below, and the two
/// together are the whole of what an eviction budget means.
#[tokio::test]
async fn eviction_inside_its_shutdown_budget_does_not_cut_off_a_message_in_flight() {
    let fixture = start(
        journal::System::builder(Eviction::Idle {
            after: Duration::from_millis(30),
            shutdown_budget: Duration::from_secs(5),
        })
        .build(),
    )
    .await;
    fixture
        .system
        .mount(id("slow"), journal::spawn(Journal::default()))
        .expect("the id is free");

    // In flight for far longer than `after`, and far inside the budget.
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
        journal::System::builder(Eviction::Idle {
            after: Duration::from_millis(40),
            shutdown_budget: Duration::from_secs(5),
        })
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

// ───────────────────────────────────────────────────────── shutdown vs abort

/// `shutdown` inside its budget drains: the turn in flight AND the one queued
/// behind it both finish, both are answered, and both effects on the state are
/// real. Made red by making `shutdown` an alias of `abort`.
///
/// Two calls and not one, because "queued turns are answered" is the claim and
/// one message in flight does not queue anything. `MAILBOX` is 1, so the second
/// `apply` is buffered behind the first and is the queued turn; the budget covers
/// both holds together, which is the depth-versus-budget relationship 4a's doc
/// names — at depth N a budget has to cover N + 1 turns, not one.
///
/// # The three numbers, and why they are not tighter
///
/// This is one of the wall-clock tests in this file — a paused clock cannot
/// sequence two real QUIC calls against each other — so the margins are wide on
/// purpose rather than snug. What must hold is an ORDER, not a duration: the
/// second call has to reach the mailbox before the shutdown removes the entry,
/// or it is answered `NoSuchActor` and this test fails for a reason that is not
/// about draining. 400 ms of hold, the second call at 40 ms and the shutdown at
/// 120 ms leave two 80 ms windows for a loopback stream on an already-open
/// connection, which is three orders of magnitude of headroom. Do not shrink them
/// to make the suite faster; shrink the hold if anything.
#[tokio::test]
async fn shutdown_inside_its_budget_answers_the_turn_in_flight_and_the_one_queued() {
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

    let running = {
        let client = &fixture.client;
        async move { journal::at(client, id("doc")).apply(1, 400).await }
    };
    let queued = {
        let client = &fixture.client;
        async move {
            // Behind the first, which owns the actor for 400 ms.
            tokio::time::sleep(Duration::from_millis(40)).await;
            journal::at(client, id("doc")).apply(2, 0).await
        }
    };
    let stopping = async {
        // Long enough that the first message is inside the actor and the second
        // is behind it, short enough that the first is still running.
        tokio::time::sleep(Duration::from_millis(120)).await;
        fixture
            .system
            .shutdown(&id("doc"), Duration::from_secs(5))
            .await
    };

    let (first, second, ended) = tokio::join!(running, queued, stopping);

    assert!(
        matches!(ended, Some(Ended::Stopped)),
        "a shutdown that drained inside its budget is Stopped, got {ended:?}"
    );
    assert_eq!(
        first.unwrap(),
        1,
        "the caller in flight must get its answer"
    );
    assert_eq!(second.unwrap(), 2, "and so must the caller behind it");
    assert_eq!(
        *applied.lock().unwrap(),
        vec![1, 2],
        "shutdown preserves the work the actor had already accepted, in order"
    );
}

/// `abort` cancels: the task's future is dropped at its await point, the state
/// goes with it, and the mutation never happened. Made red by making `abort` an
/// alias of a `shutdown` whose budget covers the turn.
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

/// `None` is what the old `stop`'s `false` was, and `abort` keeps the `bool`
/// because it has only ever had one thing to say.
#[tokio::test]
async fn shutting_down_an_unmounted_id_says_so_rather_than_pretending() {
    let system = mounted();
    assert!(system
        .shutdown(&id("nobody"), Duration::from_secs(5))
        .await
        .is_none());
    assert!(!system.abort(&id("nobody")).await);
}

// ───────────────────────────────── an expired budget, and what it must destroy
//
// The three tests below are one claim from three sides: **a teardown nobody
// waited for is not a teardown.** A `JoinHandle` that is dropped rather than
// aborted DETACHES its task on both of keylink's targets — verified against
// n0-future 0.3.2, whose native branch is tokio's and whose wasm branch has no
// `impl Drop for JoinHandle` at all — so every path that let go of one left an
// actor running with its state, and its state's exclusive resources, and no way
// back to it. A counter cannot show that; a file lock can, which is why these
// three reach for the durability port. Nothing here loads or stores a snapshot:
// the claim is used purely as the thing only a dead task releases.

/// A scratch directory that removes itself.
///
/// A third copy of a five-line struct that `tests/durable.rs` and
/// `keylink-persist/src/file.rs` also have, and it stays a copy: the value in it
/// is the *unique path*, and a shared helper would have to be given the calling
/// binary's name to keep that property. Small enough to not be worth a
/// dependency, and small enough to not be worth a fourth crate.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "keylink-actors-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// An actor that will never finish a turn, holding something only a dead task
/// releases.
///
/// `pending()` and not a long `sleep`: a sleep would let a paused clock jump past
/// it and make the test pass for the wrong reason. This loop cannot be drained by
/// any budget, so the only thing that ends it is the abort — which is exactly the
/// question.
///
/// It holds the receiver too, so closing the registry's sender genuinely does
/// nothing here. That is what a wedged actor looks like: the mailbox is shut and
/// the turn does not care.
fn wedged(claim: impl Send + 'static) -> journal::Mounted {
    keylink::mount(|inbox: journal::Mailbox| async move {
        let _wedged = (claim, inbox);
        std::future::pending::<()>().await;
    })
}

/// A `shutdown` whose budget expires **destroys** the actor, and the proof is
/// that its exclusive claim can be taken again.
///
/// Made red by `scripts/break-once.sh`, replacing `drain`'s
/// `std::pin::pin!(task)` with `std::pin::pin!(n0_future::task::spawn(async move
/// { let _ = task.await; }))` — one literal, and the deleted `System::stop`'s
/// exact shape: the expiry then aborts a *wrapper* task and drops the real
/// handle, which detaches. Watched: this test and
/// `an_eviction_whose_budget_expires_destroys_the_actor` both fail on their last
/// line with `Busy { id: StateId("wedged/4471") }`.
///
/// Paused clock, so the fifty milliseconds cost nothing and mean exactly fifty
/// milliseconds. Every await between the mount and the shutdown is the shutdown's
/// own, and the system has no sweeper, so there is no second thing that could
/// have done this.
#[tokio::test(start_paused = true)]
async fn a_shutdown_whose_budget_expires_destroys_the_actor() {
    let scratch = Scratch::new("shutdown-budget");
    let files = Files::open(&scratch.0).await.expect("the store opens");
    let held = StateId::new("wedged/4471").expect("a valid state id");
    let claim = files.claim(&held).await.expect("nothing holds it yet");

    let system = mounted();
    system
        .mount(id("wedged"), wedged(claim))
        .expect("the id is free");

    let ended = system
        .shutdown(&id("wedged"), Duration::from_millis(50))
        .await;

    assert!(
        matches!(ended, Some(Ended::Vanished)),
        "an actor that would not drain was destroyed, which is Vanished, got {ended:?}"
    );
    assert!(system.is_empty(), "and it is out of the map either way");
    files
        .claim(&held)
        .await
        .expect("the destroyed actor released its claim; if this is Busy, it is still running");
}

/// The same, done by the sweeper's budget instead of a caller's.
///
/// `Eviction::Idle`'s own rustdoc claimed *"Eviction is `System::stop`, not
/// `System::abort`"*, and the sweeper it described never awaited anything at all:
/// it dropped entries inside a `retain`, under the write lock, which detached
/// every task it evicted. This is that sentence turned into a test.
///
/// `after` is a minute, which makes the sweeper's own tick thirty seconds; the
/// budget is fifty milliseconds and is the only number this test is about. The two
/// paths into `sweep` race for one write lock and `extract_if` runs before its
/// first await, so whichever gets there removes the entry and the other finds
/// nothing — which is why the assertions below are about the CLAIM and about the
/// map being empty, and not about which of the two did it. Either answer is the
/// one under test: an eviction whose budget expires destroys the actor.
#[tokio::test(start_paused = true)]
async fn an_eviction_whose_budget_expires_destroys_the_actor() {
    let scratch = Scratch::new("eviction-budget");
    let files = Files::open(&scratch.0).await.expect("the store opens");
    let held = StateId::new("wedged/4471").expect("a valid state id");
    let claim = files.claim(&held).await.expect("nothing holds it yet");

    let system: journal::System = journal::System::builder(Eviction::Idle {
        after: Duration::from_secs(60),
        shutdown_budget: Duration::from_millis(50),
    })
    .build();
    system
        .mount(id("wedged"), wedged(claim))
        .expect("the id is free");

    tokio::time::advance(Duration::from_secs(90)).await;
    system.evict_idle().await;

    assert!(system.is_empty(), "the idle actor is evicted");
    files
        .claim(&held)
        .await
        .expect("the evicted actor released its claim; if this is Busy, it was detached");
}

/// A refused mount leaks nothing, on both refusals.
///
/// The second detach, and it was never a budget problem: `System::mount` takes a
/// `Mounted` by value and drops it on `Occupied` and on `AtCapacity`, and the
/// task used to be **already spawned** by then — the generated `spawn` started
/// it. `MountError` hands nothing back, so the caller who was refused had no
/// handle, and neither did the registry. Every refused mount was a live actor
/// nobody could reach, holding whatever its state held.
///
/// `Mounted` carries an unstarted future now, so both refusals drop a loop that
/// has never been polled. Nothing is awaited between the claim and the refusal:
/// the release is synchronous, inside `mount`, which is the strongest form the
/// guarantee has.
///
/// Made red by `scripts/break-once.sh`, replacing `mount`'s
/// `run: Box::pin(run(inbox)),` with `run: { let started =
/// n0_future::task::spawn(run(inbox)); Box::pin(async move { let _ =
/// started.await; }) },` — the defect restored in one literal, spawning at
/// `mount()` and letting the refusal drop the handle. Watched: this test fails at
/// the first re-claim with `Busy { id: StateId("refused/4471") }`, and
/// `registry.rs`'s `neither_refusal_starts_the_actor_it_refused` fails at
/// `left: 2, right: 0` on the same break. That one watches the same property with
/// a counter and no filesystem; this one watches what a counter cannot, which is
/// the resource still held.
#[tokio::test]
async fn a_refused_mount_leaves_no_actor_holding_the_claim_it_was_built_around() {
    let scratch = Scratch::new("refused-mount");
    let files = Files::open(&scratch.0).await.expect("the store opens");
    let held = StateId::new("refused/4471").expect("a valid state id");

    let system: journal::System = journal::System::builder(Eviction::Never)
        .capacity(1)
        .build();
    system
        .mount(id("taken"), journal::spawn(Journal::default()))
        .expect("the id is free");

    let occupied = {
        let claim = files.claim(&held).await.expect("nothing holds it yet");
        system
            .mount(id("taken"), wedged(claim))
            .expect_err("the id is taken")
    };
    assert!(
        matches!(occupied, MountError::Occupied { .. }),
        "{occupied}"
    );
    files
        .claim(&held)
        .await
        .expect("an actor refused as Occupied is never started, so it holds nothing");

    let full = {
        let claim = files.claim(&held).await.expect("the refusal released it");
        system
            .mount(id("second"), wedged(claim))
            .expect_err("the cap is one and one is mounted")
    };
    assert!(matches!(full, MountError::AtCapacity { .. }), "{full}");
    files
        .claim(&held)
        .await
        .expect("nor is an actor refused as AtCapacity");
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

/// Two services with identical message shapes must not share a keyed ALPN, or a
/// peer built for one completes a handshake with the other and its bytes decode
/// cleanly into the wrong meaning.
///
/// This replaces `the_schema_hash_separates_two_services_with_identical_shapes`,
/// which asserted the same separation one layer down, on the per-actor schema
/// hash that no longer exists. Same claim, moved to where it is now enforced.
#[test]
fn the_keyed_alpn_separates_two_services_with_identical_shapes() {
    assert_ne!(journal::keyed_alpn(), vault::keyed_alpn());
}

/// A service's two ALPNs are not each other, so one node can serve the same
/// service as a singleton and as an actor system at once.
#[test]
fn a_services_keyed_alpn_is_not_its_singleton_alpn() {
    assert_ne!(journal::keyed_alpn(), journal::alpn());
    assert_ne!(vault::keyed_alpn(), vault::alpn());
    assert_ne!(journal::keyed_alpn(), vault::alpn());
}

/// RFC-04 §14.10 item 6: the defect was reproduced in isolation but never
/// against the crate's own `RemoteError`. This is that test.
///
/// `NoSuchActor { id: String }` has the same postcard encoding as
/// `Failed(String)`, so inserting it changed what an existing refusal MEANS.
/// That is only safe because the ALPN moves with it. This asserts the moving —
/// and the removal that Wave 1a made is the same fact from the other side, which
/// is why `tests/protocol.rs`'s pinned literal moved with it.
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

// ─────────────────────────────── a layer above a keyed actor, per METHOD

/// Records what each call looked like from outside the mailbox, and refuses one
/// method by name.
///
/// **This is the capability the wave bought**, and the layer is the only place it
/// can be demonstrated: `Context::method` used to answer the constant `"Message"`
/// for every message of every keyed actor, because the real name sat inside a
/// payload no layer could read before a hash check. There was nothing to match
/// on, so per-method middleware and per-method authorization were impossible on
/// this path and possible one module over.
/// What a layer above a keyed actor can read off one call: the actor's own
/// method name, and the id it addressed. Both used to be unavailable — the first
/// because the payload was opaque, the second because it was the only thing that
/// was not.
type Seen = Arc<Mutex<Vec<(String, Option<String>)>>>;

struct PerMethod {
    /// `(method, actor)` for every call that reached this layer.
    seen: Seen,
}

impl Middleware<Addressed<journal::Request>, journal::Response> for PerMethod {
    fn handle(
        &self,
        cx: Context,
        req: Addressed<journal::Request>,
        body: BodyReader,
        next: Next<Addressed<journal::Request>, journal::Response>,
    ) -> Pin<Box<dyn Future<Output = (Outcome<journal::Response>, Body)> + Send>> {
        self.seen
            .lock()
            .expect("not poisoned")
            .push((cx.method().to_owned(), cx.actor().map(ToString::to_string)));
        if cx.method() == "Entries" {
            return Box::pin(async {
                (
                    Outcome::Err(RemoteError::Unauthorized("Entries".to_owned())),
                    Body::empty(),
                )
            });
        }
        next(cx, req, body)
    }
}

/// A layer refuses one keyed method and passes the rest, and it discriminates on
/// the method name rather than on the payload.
///
/// Made red by having `Rpc::method` on `Addressed<T>` answer a constant instead
/// of delegating to the message inside — which is what the old opaque envelope
/// did, and what made this test unwritable.
#[tokio::test]
async fn a_layer_refuses_one_keyed_method_and_passes_the_rest() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server = local_node().await;
    let client_node = local_node().await;
    let system = mounted();
    system
        .mount(id("obra/1"), journal::spawn(Journal::default()))
        .expect("the id is free");

    let _running = Server::builder(&server)
        .service(
            journal::keyed_service(&system, Allowlist::anyone())
                .layer(PerMethod { seen: seen.clone() })
                .build(),
        )
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = journal::KeyedClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");
    let actor = journal::at(&client, id("obra/1"));

    assert_eq!(
        actor.apply(7, 0).await.unwrap(),
        7,
        "the method the layer passes must reach the actor"
    );

    let refused = actor
        .entries()
        .await
        .expect_err("the layer refuses this method by name");
    assert!(
        matches!(refused, Error::Remote(RemoteError::Unauthorized(ref what)) if what == "Entries"),
        "got {refused:?}"
    );

    // The actor ran the passed method and never saw the refused one.
    assert_eq!(actor.apply(8, 0).await.unwrap(), 8);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            ("Apply".to_owned(), Some("obra/1".to_owned())),
            ("Entries".to_owned(), Some("obra/1".to_owned())),
            ("Apply".to_owned(), Some("obra/1".to_owned())),
        ],
        "the layer must see the actor's own method names and the id it addressed"
    );
}

// ──────────────────────────────────────────────────────────── who is calling

// A third service, so that adding a method to `journal` does not break the
// deliberate shape-for-shape pairing it has with `vault`. A `///` here would be
// an unused doc comment: rustdoc does not document a macro invocation.
keylink::service! {
    service Doorman;

    rpc WhoCalled() -> Option<String>;
    rpc Ignored() -> u64;
}

struct Doorman;

impl doorman::Actor for Doorman {
    async fn who_called(&mut self, cx: keylink::Caller<'_>) -> Option<String> {
        cx.peer().map(|cx| cx.peer().to_string())
    }

    async fn ignored(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        0
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
/// It also pins the shape RFC-07 §1.6 had to correct. The mailbox carries an
/// owned `Origin`, the loop borrows a `Caller<'_>` out of it for one turn, and
/// `Caller<'static>::Peer` is uninhabited outside keylink — so if `Origin::caller`
/// ever answered `Caller::Local` for a peer message, this is what says so.
///
/// Made red by passing `Caller::Local` in the mailbox loop, or by having
/// `Origin::caller` map `Peer` to `Local`.
#[tokio::test]
async fn a_keyed_actor_sees_the_proven_key_of_its_caller() {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: doorman::System = doorman::System::builder(Eviction::Never).build();
    let _running = Server::builder(&server)
        .service(doorman::keyed_service(&system, Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = doorman::KeyedClient::connect(&client_node, server.endpoint().addr())
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
/// reports a serialization error only from its flavor, and the `Vec` flavor has
/// none. Without it the arm that decides what a turn owes when it ran and
/// produced nothing sendable is unreachable from a test, and an unreachable arm
/// is a decision nobody has checked.
#[derive(Debug, Clone, serde::Deserialize, postcard_schema::Schema)]
pub struct Untransmittable;

impl serde::Serialize for Untransmittable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this reply never encodes"))
    }
}

/// An argument whose `Deserialize` **panics** where a garbage payload merely
/// fails to decode.
///
/// Hand-written for the same reason [`Untransmittable`]'s `Serialize` is: a
/// derived one cannot do this. The distance between the two types is one token —
/// `Err(..)` against `panic!` — and the distance between their outcomes used to
/// be an actor that answered the next message and one that answered nothing ever
/// again.
///
/// **The decode moved.** It used to run inside the mailbox loop, on the actor's
/// own task, which is why `catching` had to go round it. It now runs in the
/// accept loop, on the connection's task, before the message reaches a mailbox at
/// all — so a panicking `Deserialize` cannot touch the actor even in principle,
/// exactly as on the singleton path. This type is kept to prove that rather than
/// to assert it.
#[derive(Debug, Clone, serde::Serialize, postcard_schema::Schema)]
pub struct Imploding;

impl<'de> serde::Deserialize<'de> for Imploding {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        panic!("this request panics on the way off the wire");
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

    /// Carries an argument that panics while being decoded, which now happens on
    /// the CONNECTION's task and before the message reaches a mailbox — so
    /// nothing is entered, nothing is mutated, and the actor is untouched.
    rpc Implode(charge: Imploding) -> u64;

    /// Mutates the state and then answers with something whose `Serialize`
    /// panics rather than errors. [`Exploding`] is declared further down, with
    /// the `fuse` service that first needed it.
    rpc Detonate() -> Exploding;
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

    // Unreachable by construction: the argument cannot be decoded without
    // panicking, so the accept loop never builds the `Request` this arm needs.
    // Written as a panic rather than a `0` so that a decode which somehow
    // succeeds is loud here instead of passing as a turn that touched nothing.
    async fn implode(&mut self, _cx: keylink::Caller<'_>, _charge: Imploding) -> u64 {
        unreachable!("the argument's `Deserialize` panics before the message is routed");
    }

    async fn detonate(&mut self, _cx: keylink::Caller<'_>) -> Exploding {
        self.touches += 1;
        Exploding
    }
}

/// One `Ward`, reachable through its own generated client.
///
/// **This used to need a raw `RpcClient` and a hand-built envelope.** The typed
/// keyed client had no `with_idempotency`, so nothing in keylink's own client
/// could send a key to a keyed actor: the field is on the envelope, so a peer
/// reached it only by speaking the protocol by hand. That is the capability this
/// wave delivers, and the whole harness is the difference — one generated client,
/// `with_idempotency` on it, and no envelope written out anywhere.
///
/// `raw` survives for one job only: putting bytes on the wire that are NOT a
/// message of this contract, which a typed client cannot do by construction.
struct Keyed {
    client: ward::KeyedClient,
    /// A hand-rolled peer on `ward`'s keyed ALPN, whose request type is a bare
    /// `u32`. Nothing generated can produce an undecodable payload.
    raw: RpcClient<u32, ward::Response>,
    _system: ward::System,
    _server: Server,
    _nodes: [Node; 2],
}

async fn start_keyed() -> Keyed {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: ward::System = ward::System::builder(Eviction::Never).build();
    let running = Server::builder(&server)
        .service(ward::keyed_service(&system, Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = ward::KeyedClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the typed client dials the actor system");
    let raw = RpcClient::connect(&client_node, server.endpoint().addr(), ward::keyed_alpn())
        .await
        .expect("a hand-rolled peer dials the same ALPN");
    system
        .mount(id("ward"), ward::spawn(Ward::default()))
        .expect("the id is free");

    Keyed {
        client,
        raw,
        _system: system,
        _server: running,
        _nodes: [server, client_node],
    }
}

/// The actor's counter, read without a key so reading never settles one.
async fn touches(fixture: &Keyed) -> u64 {
    ward::at(&fixture.client, id("ward"))
        .touches()
        .await
        .expect("the actor answers")
}

/// A keyed handle to `ward`, under `key`.
fn under(fixture: &Keyed, key: &str) -> ward::KeyedClient {
    fixture
        .client
        .with_idempotency(IdempotencyKey::new(key).expect("a valid key"))
}

/// **The acceptance criterion: a key attached through the generated keyed client
/// settles, over two real nodes.**
///
/// It is also the regression test for the defect that started this: of the three
/// generated dispatch paths, the keyed mailbox loop marked the progress marker
/// nowhere. A key claimed for a message routed there was RELEASED once the turn
/// had already mutated the actor, so the caller's retry ran the work again —
/// at-least-once out of the module whose one promise is at-most-once.
///
/// Proved on the actor's counter and not on the reply, because a replayed reply
/// and a re-run one are byte identical by construction. Made red by deleting the
/// `completed()` call from the loop in `macros.rs`.
#[tokio::test]
async fn a_key_attached_through_the_generated_keyed_client_settles() {
    let fixture = start_keyed().await;

    let first = ward::at(&under(&fixture, "touch"), id("ward"))
        .touch()
        .await
        .expect("the first call runs");
    assert_eq!(first, 1);

    let again = ward::at(&under(&fixture, "touch"), id("ward"))
        .touch()
        .await
        .expect("the retry is answered from the record");
    assert_eq!(again, 1);

    assert_eq!(
        touches(&fixture).await,
        1,
        "the retry must not have entered the turn a second time"
    );
}

/// And the ledger the key settles in is **per service**, which is the bill the
/// per-service ALPN comes with rather than a bug.
///
/// One `Ledger` is built per `Service`, so a key claimed against `ward`'s keyed
/// connection means nothing on any other. Stated here because it is invisible
/// from a call site and expensive to rediscover: a caller that retries a
/// cross-service workflow under one key does not get at-most-once across it.
#[tokio::test]
async fn a_key_is_known_to_the_service_it_was_claimed_against_and_no_other() {
    let fixture = start_keyed().await;
    let key = IdempotencyKey::new("scoped").expect("a valid key");

    ward::at(&under(&fixture, "scoped"), id("ward"))
        .touch()
        .await
        .expect("the call runs");

    assert!(
        matches!(
            fixture
                .client
                .key_state(key.clone())
                .await
                .expect("answers"),
            keylink::KeyState::Done
        ),
        "the service that ran it must know the key"
    );

    // A second service on a second node, dialled with the same key.
    let server = local_node().await;
    let caller = local_node().await;
    let system: doorman::System = doorman::System::builder(Eviction::Never).build();
    let _running = Server::builder(&server)
        .service(doorman::keyed_service(&system, Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let elsewhere = doorman::KeyedClient::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials");

    assert!(
        matches!(
            elsewhere.key_state(key).await.expect("answers"),
            keylink::KeyState::Fresh
        ),
        "a key is scoped to one service's ledger, and a caller must not read it as anything else"
    );
}

/// The turn ran, mutated the state, and produced nothing the wire can carry.
///
/// Not `completed`: that would record the encoding refusal as this call's settled
/// reply and replay that refusal to every retry, for work that really happened.
/// Not released either: that would run the work again. `Interrupted` is what is
/// left, and it is the truth — "it ran, and whether it took effect is unknown".
///
/// **The encode moved, and the caller's answer moved with it.** It used to run
/// inside the mailbox loop, so the caller was told
/// `RemoteError::Failed`. The reply is typed now and `serve_one` encodes it on
/// the connection's task, exactly as it does for every other service — so the
/// frame is never written and the caller sees a stream that finished without an
/// answer. That is a behaviour change and it is a convergence: a singleton actor
/// returning an unencodable reply has always behaved this way.
///
/// What did NOT change is what the key owes, and that is the part worth pinning:
/// the loop marked `completed()`, `serve_one` returns before `record`, and
/// `Drop for Guard` reads the marker and settles `Interrupted`. Made red by
/// moving `completed()` out of the loop's success arm.
#[tokio::test]
async fn a_keyed_call_whose_reply_does_not_encode_settles_the_key_interrupted() {
    let fixture = start_keyed().await;

    ward::at(&under(&fixture, "touch"), id("ward"))
        .unsendable()
        .await
        .expect_err("the reply does not encode");
    assert_eq!(
        touches(&fixture).await,
        1,
        "the turn ran, which is what makes the key owe something"
    );

    let retried = ward::at(&under(&fixture, "touch"), id("ward"))
        .unsendable()
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

/// The same decision for a `Serialize` that **panics** where the test above
/// merely errors. The turn ran and mutated and produced nothing transmissible,
/// which is one fact however the encoding failed, so the key settles the one way:
/// `Interrupted`.
///
/// Worth its own case rather than left to the argument that the two are siblings.
/// Only this one is reached by an unwind, and it now unwinds out of `serve_one` on
/// the connection's task — where `Drop for Guard` is the only code left to settle
/// the key, and it still has to get it right.
#[tokio::test]
async fn a_keyed_call_whose_reply_panics_while_encoding_settles_the_key_interrupted() {
    let fixture = start_keyed().await;

    ward::at(&under(&fixture, "touch"), id("ward"))
        .detonate()
        .await
        .expect_err("the reply's `Serialize` panics");
    assert_eq!(
        touches(&fixture).await,
        1,
        "the turn ran, which is what makes the key owe something"
    );

    let retried = ward::at(&under(&fixture, "touch"), id("ward"))
        .detonate()
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

/// The panic arm. A turn that panicked was entered, and whatever it mutated
/// before it panicked is in the state the actor carries on with. Giving the key
/// back would run that half-done work again.
///
/// This is also what guards the `dispatched()` mark itself: delete it and this
/// key is released rather than settled, which is the defect one arm along.
#[tokio::test]
async fn a_keyed_call_whose_turn_panicked_settles_the_key_interrupted() {
    let fixture = start_keyed().await;

    let error = ward::at(&under(&fixture, "touch"), id("ward"))
        .explode()
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

    let retried = ward::at(&under(&fixture, "touch"), id("ward"))
        .explode()
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

/// **The named replacement for
/// `a_keyed_call_whose_payload_does_not_decode_leaves_the_key_claimable`.**
///
/// That test reached the mailbox loop with a payload the loop could not decode,
/// and pinned that nothing ran and the key stayed claimable. The decode is not in
/// the loop any more: `serve_one` reads
/// `WireRequest<Addressed<Request>>` off the frame in the accept loop, which is
/// **before** `claim_key`. So the key is never claimed at all rather than claimed
/// and released, which is a stronger version of the same guarantee — and it is
/// the ordering `serve_one` already gave every other service.
///
/// A typed client cannot express this, which is why the raw peer exists: the
/// payload is a bare `u32`, and `Addressed`'s first field is an `ActorId`, so the
/// decode fails on the id's length prefix before anything else is read.
///
/// Made red by moving `claim_key` above `read_header` in `rpc.rs`, which claims a
/// key for a message that is never dispatched and never answered.
#[tokio::test]
async fn a_garbage_payload_never_reaches_the_actor_and_claims_no_key() {
    let fixture = start_keyed().await;
    let key = IdempotencyKey::new("garbage").expect("a valid key");

    fixture
        .raw
        .with_idempotency(key.clone())
        .call(250u32, Body::empty())
        .await
        .map(|_| ())
        .expect_err("a bare u32 is not an Addressed<ward::Request>");

    assert_eq!(
        touches(&fixture).await,
        0,
        "a payload that does not decode must not reach the turn"
    );
    assert!(
        matches!(
            fixture.client.key_state(key).await.expect("answers"),
            keylink::KeyState::Fresh
        ),
        "a key sent with an undecodable payload must never have been claimed"
    );

    // And the connection carries on, which is what makes this a refusal rather
    // than a denial of service.
    assert_eq!(
        ward::at(&fixture.client, id("ward"))
            .touch()
            .await
            .expect("the actor is still there"),
        1
    );
}

/// **The named replacement for
/// `a_keyed_call_whose_payload_panics_while_decoding_leaves_the_key_claimable`.**
///
/// The old one was about `Sealed::open` running the caller's `Deserialize` on the
/// **actor's** task: an unwind there left the mailbox receiver dropped, so that
/// call and every later one to the same id were answered `NoSuchActor` for an id
/// the registry went on listing. RFC-04 §16.11. `catching` was the fix and the
/// test was its guard.
///
/// The decode is on the connection's task now, one task removed from any actor, so
/// the mechanism the old test guarded no longer exists on this path. The property
/// is what is kept: a message whose `Deserialize` panics costs that call and
/// nothing else, and the key it carried was never claimed.
///
/// Made red by moving the decode back inside the mailbox loop.
#[tokio::test]
async fn a_payload_that_panics_while_decoding_costs_that_call_and_not_the_actor() {
    let fixture = start_keyed().await;
    let key = IdempotencyKey::new("implode").expect("a valid key");

    let keyed = under(&fixture, "implode");
    ward::at(&keyed, id("ward"))
        .implode(Imploding)
        .await
        .map(|_| ())
        .expect_err("the payload's `Deserialize` panics");

    assert_eq!(
        touches(&fixture).await,
        0,
        "a payload that panics while decoding must cost that message and not the actor"
    );
    assert!(
        matches!(
            fixture.client.key_state(key).await.expect("answers"),
            keylink::KeyState::Fresh
        ),
        "nothing ran, so the key must never have been claimed"
    );

    assert_eq!(
        ward::at(&fixture.client, id("ward"))
            .touch()
            .await
            .expect("the actor is still there"),
        1
    );
}

// ────────────────────────────── a budget that expires in a keyed mailbox

// A fifth service. `Hold` is what makes the queue deterministic: while its turn
// is inside the actor, the mailbox is the only place any other message can be,
// and it stays there until this test says otherwise rather than until a sleep
// guessed right.
keylink::service! {
    service Gate;

    /// Holds the actor's one task open until the test releases it.
    rpc Hold() -> u64;

    /// Counts the turns that reached the state. A message refused before its
    /// turn is visible HERE and nowhere else: the refusal a peer reads says
    /// nothing about whether the actor was touched.
    rpc Touch() -> u64;

    /// The count, read without a key, so reading it never settles one.
    rpc Touches() -> u64;
}

struct Gate {
    /// Fires when `hold` enters its turn. Nothing else is sent until this
    /// arrives, which is what makes the actor provably busy rather than
    /// probably busy.
    entered: mpsc::UnboundedSender<()>,
    /// Closed until the test opens it. A `Semaphore` and not a `Notify`,
    /// because a permit added before anyone waits still releases the waiter —
    /// a `notify_one` arriving before `notified()` would hang this forever.
    release: Arc<Semaphore>,
    touches: u64,
}

impl gate::Actor for Gate {
    async fn hold(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        let _ = self.entered.send(());
        self.release
            .acquire()
            .await
            .expect("the test never closes the semaphore")
            .forget();
        self.touches
    }

    async fn touch(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        self.touches += 1;
        self.touches
    }

    async fn touches(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        self.touches
    }
}

/// Reports every call that has passed the `QueueTimeout` above it, carrying the
/// budget that layer stamped.
///
/// This is what makes the wait below a proof rather than a guess. The deadline
/// is `stamped_at + budget`, and this layer runs strictly after `stamped_at`,
/// so waiting one whole budget from the moment this fires ends after the
/// deadline whatever the machine was doing in between. Nothing is asserted
/// about elapsed time; the wait only decides WHEN the actor is let go.
struct Stamped(mpsc::UnboundedSender<Duration>);

impl<Req: Send + 'static, Resp: Send + 'static> Middleware<Req, Resp> for Stamped {
    fn handle(
        &self,
        cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> Pin<Box<dyn Future<Output = (Outcome<Resp>, Body)> + Send>> {
        if let Some(budget) = cx.queueing() {
            let _ = self.0.send(Duration::from_millis(budget.millis()));
        }
        next(cx, req, body)
    }
}

const BUDGET_MS: u64 = 200;

/// Long enough that the hop from the layer to the mailbox loop cannot spend it:
/// that hop is a channel send and a `recv`, with no network and no user code
/// inside it. Every other message in these tests reaches its turn well inside
/// it, and the one that does not is held out by a semaphore rather than by the
/// clock.
const BUDGET: Duration = Duration::from_millis(BUDGET_MS);

/// The keyed request type every bound in these tests is written over.
type GateWire = Addressed<gate::Request>;

/// One `Gate` behind a bound, reached through its own generated client.
struct Gated {
    client: gate::KeyedClient,
    /// Fires when a turn starts.
    entered: mpsc::UnboundedReceiver<()>,
    /// Fires when a call passes the `QueueTimeout`, carrying its budget.
    stamped: mpsc::UnboundedReceiver<Duration>,
    /// Lets `hold` return.
    release: Arc<Semaphore>,
    _system: gate::System,
    _server: Server,
    _nodes: [Node; 2],
}

async fn start_gated() -> Gated {
    gated_behind(QueueTimeout::after(BUDGET)).await
}

/// The same, behind whichever bound is under test.
///
/// `Timeout` and `QueueTimeout` answer the same caller in the same scenario and
/// owe the caller's idempotency key opposite things, so the fixture they are
/// compared through has to be one fixture. `Stamped` stays inside either bound
/// and simply reports nothing under `Timeout`, which stamps no deadline — the
/// tests that read `stamped` are the ones that supply a `QueueTimeout`.
async fn gated_behind(bound: impl Middleware<GateWire, gate::Response>) -> Gated {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: gate::System = gate::System::builder(Eviction::Never).build();
    let (entered_tx, entered) = mpsc::unbounded_channel();
    let (stamped_tx, stamped) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let running = Server::builder(&server)
        .service(
            gate::keyed_service(&system, Allowlist::anyone())
                // `keyed_service` gives back an ordinary `ServiceBuilder`, so
                // this compiles — and until RFC-04 §16.7 the deadline a
                // `QueueTimeout` stamped was read by nobody on this path.
                .layer(bound)
                // Added second, so it sits INSIDE the bound and sees whatever
                // that layer put on the context.
                .layer(Stamped(stamped_tx))
                .build(),
        )
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = gate::KeyedClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");
    system
        .mount(
            id("gate"),
            gate::spawn(Gate {
                entered: entered_tx,
                release: Arc::clone(&release),
                touches: 0,
            }),
        )
        .expect("the id is free");

    Gated {
        client,
        entered,
        stamped,
        release,
        _system: system,
        _server: running,
        _nodes: [server, client_node],
    }
}

/// A keyed handle to `gate`, under `key`.
fn gate_under(fixture: &Gated, key: &str) -> gate::KeyedClient {
    fixture
        .client
        .with_idempotency(IdempotencyKey::new(key).expect("a valid key"))
}

async fn gate_touches(fixture: &Gated) -> u64 {
    gate::at(&fixture.client, id("gate"))
        .touches()
        .await
        .expect("the actor answers")
}

/// Puts one keyed message in the mailbox, keeps it there until its budget has
/// provably run out, and gives back what it was answered.
///
/// Three facts have to hold at the moment the loop takes that message, and none
/// of them is left to a race:
///
/// 1. **It is queued.** It is not sent until `hold` has entered its turn, and
///    the mailbox holds a message while a turn runs, so the mailbox is the only
///    place it can be.
/// 2. **Its deadline has passed.** The wait starts only after the `Stamped`
///    layer has seen the deadline stamped, and lasts a whole budget, so it ends
///    after `stamped_at + budget` however slow anything in between was.
/// 3. **It stays queued for all of that.** The actor is inside `hold`, waiting
///    on a semaphore this test owns, and the permit is added last.
async fn expired_in_the_queue(fixture: &mut Gated, key: &str) -> Result<u64, Error> {
    let keyed = gate_under(fixture, key);
    let client = &fixture.client;
    let release = &fixture.release;
    let entered = &mut fixture.entered;
    let stamped = &mut fixture.stamped;
    let (go, wait) = oneshot::channel();

    let holding = async { gate::at(client, id("gate")).hold().await };
    let queued = async {
        wait.await.expect("the driver says when");
        gate::at(&keyed, id("gate")).touch().await
    };
    let driver = async {
        entered.recv().await.expect("`hold` entered its turn");
        // `hold`'s own stamp, drained here so the next one belongs to the
        // message this is about.
        stamped.recv().await.expect("`hold` was stamped");
        go.send(()).expect("the queued call is waiting for this");

        let budget = stamped.recv().await.expect("the queued call was stamped");
        tokio::time::sleep(budget).await;
        // Only now, with the deadline behind us and the message still in the
        // mailbox, does the loop get to look at it.
        release.add_permits(1);
    };

    let (held, refused, ()) = tokio::join!(holding, queued, driver);
    held.expect("the holding turn answers");
    refused
}

/// The defect: `QueueTimeout` layers onto a keyed service and compiles, and the
/// keyed mailbox loop never read the deadline it stamps. A configured guard that
/// does not guard, at any budget, with no error and no warning — on the path
/// where the identical layer works one module over.
///
/// Made red by deleting the expiry arm from the loop in `macros.rs`.
#[tokio::test]
async fn a_keyed_message_still_queued_when_its_budget_expires_is_refused_before_its_turn() {
    let mut fixture = start_gated().await;

    let refused = expired_in_the_queue(&mut fixture, "queued")
        .await
        .expect_err("a message whose budget ran out in the mailbox must be refused");

    assert!(
        matches!(refused, Error::Remote(RemoteError::TimedOut { millis }) if millis == BUDGET_MS),
        "the refusal must be the one the local path produces, carrying the budget: got {refused:?}"
    );
    assert_eq!(
        gate_touches(&fixture).await,
        0,
        "a refused message must not have entered the turn"
    );
}

/// The interaction that matters, and the reason the fix touches the progress
/// marker nowhere. A message refused while still queued has not run, so
/// `Drop for Guard` must see `NotDispatched` and give the key back: the retry
/// of a call that never happened is a first attempt.
///
/// Marking `dispatched()` on the expiry arm would settle the key `Interrupted`
/// and refuse every retry of that call forever, which is the failure §16.6
/// names one arm along. Made red by marking it.
#[tokio::test]
async fn a_keyed_message_that_expired_in_the_queue_leaves_its_key_claimable() {
    let mut fixture = start_gated().await;

    expired_in_the_queue(&mut fixture, "retried")
        .await
        .expect_err("the budget ran out while it queued");

    // The actor is free now, so this one runs rather than queues.
    let ran = gate::at(&gate_under(&fixture, "retried"), id("gate"))
        .touch()
        .await
        .expect("the key must still be claimable");
    assert_eq!(ran, 1);
    assert_eq!(
        gate_touches(&fixture).await,
        1,
        "the retry must have entered the turn, which is what claimable means"
    );
}

// ────────────────────────────── the one mailbox slot, and who may hold it

/// Sends two messages to `target` while the actor at `gate` is already inside a
/// turn, keeps it there until both budgets have provably run out, and reports what
/// each was answered — together with whether the actor had already been let go
/// when that answer arrived.
///
/// **The pair is deliberately identical in shape and unordered.** Capacity is
/// one, so while the turn runs at most one of the two takes the mailbox slot and
/// at most one is left waiting for a permit — and there is no way to observe
/// which, because a slot is only provably occupied while nothing is draining it.
/// Every assertion built on this is therefore true of both messages or a count
/// over the pair, which is what keeps it free of a race no amount of sleeping
/// would fix.
///
/// Nothing here asserts on elapsed time. The clock decides one thing only: when
/// the actor is let go. Three budgets rather than the one
/// [`expired_in_the_queue`] waits, because this reports an ORDER and not just an
/// expiry — a caller refused at its own deadline is refused two whole budgets
/// before the permit is added, which is the difference between a fact and a
/// photo finish.
async fn two_behind_a_held_turn(
    fixture: &mut Gated,
    target: ActorId,
) -> [(Result<u64, Error>, bool); 2] {
    let client = &fixture.client;
    let release = &fixture.release;
    let entered = &mut fixture.entered;
    let stamped = &mut fixture.stamped;
    let let_go = AtomicBool::new(false);
    let (go, wait) = oneshot::channel();

    let holding = async { gate::at(client, id("gate")).hold().await };
    let sending = async {
        wait.await.expect("the driver says when");
        let answer = || async {
            let answered = gate::at(client, target.clone()).touch().await;
            (answered, let_go.load(Ordering::Acquire))
        };
        tokio::join!(answer(), answer())
    };
    let driver = async {
        entered.recv().await.expect("`hold` entered its turn");
        // `hold`'s own stamp, drained here so the next two belong to the
        // messages this is about.
        stamped.recv().await.expect("`hold` was stamped");
        go.send(()).expect("the two calls are waiting for this");

        let budget = stamped.recv().await.expect("one of the pair was stamped");
        stamped.recv().await.expect("and the other");
        tokio::time::sleep(budget * 3).await;
        let_go.store(true, Ordering::Release);
        release.add_permits(1);
    };

    let (held, (left, right), ()) = tokio::join!(holding, sending, driver);
    held.expect("the holding turn answers");
    [left, right]
}

/// **The named replacement for
/// `a_drifted_message_is_refused_without_taking_the_actors_mailbox_slot`.**
///
/// That test carried a real denial-of-service argument, and it is worth restating
/// rather than losing: capacity is one, so a message that has to be carried to
/// the actor's own task before anyone will look at it takes the one buffer a
/// legitimate caller needs, and holds it for as long as the current turn lasts. A
/// peer can do that with garbage, indefinitely, at no cost to itself. What it
/// watched was `System::deliver`'s hash comparison sitting **before** the send.
///
/// There is no hash comparison any more. What is left in front of the send is
/// `ActorId::validate`, and that is what this pins: a malformed id is refused
/// while the actor is still busy, so it provably never needed a permit. The other
/// pre-send refusals — `NoSuchActor` and `AtCapacity` from `route` — have the same
/// shape and the same position, and this is the one a peer can reach at will
/// against a system that spawns lazily and accepts every id.
///
/// Made red by moving `actor.validate()` below `system.route(..)` — no, that is
/// not enough, because `route` also refuses before the send. Made red by moving
/// the whole `validate` call below `mail.send`, which is what "refuse it once it
/// is already in the queue" amounts to.
#[tokio::test]
async fn a_malformed_id_is_refused_without_taking_the_actors_mailbox_slot() {
    let mut fixture = start_gated().await;

    let answers = two_behind_a_held_turn(&mut fixture, oversized_id()).await;

    for (answered, let_go) in answers {
        let error = answered.expect_err("the id is longer than any id can be");
        assert!(
            matches!(error, Error::Remote(RemoteError::Failed(ref why)) if why.contains("actor id")),
            "a malformed id must be refused on its own terms, not queued for a slot and \
             then told to try again: got {error:?}"
        );
        assert!(
            !let_go,
            "a malformed id must be refused while the actor is still busy, which is what \
             proves it never needed a mailbox permit"
        );
    }
    assert_eq!(
        gate_touches(&fixture).await,
        0,
        "neither message may have entered the turn"
    );
}

/// `deliver_to_local` bounds the wait for a mailbox permit, so a message that
/// cannot get a slot is refused at its budget and — tokio not queueing a send that
/// was cancelled before its permit arrived — provably never queued. The keyed walk
/// awaited `mail.send` with no bound at all.
///
/// The bound used to live in `LocalActor::ask_catching_before`, which this comment
/// named until Wave 1b deleted it along with `Queued<T>`. Its one caller was
/// `Handle::service`'s dispatch, which is `deliver_to_local` now — the twin of
/// `deliver_to_actor`, in keylink, so the two halves of this test's subject are one
/// timeout written once rather than two that disagreed.
///
/// Capacity is one and turns are serial, so with an actor inside a long turn
/// every caller past the first waits there: each holding a QUIC stream, a
/// `BodyReader` and an idempotency key claimed before dispatch, and each
/// learning `TimedOut` only once the turn ends. RFC-04 §16.7 called that milder
/// than it sounds, on the assumption of short turns — which is the assumption
/// `QueueTimeout` exists because nobody can make. A 50 ms budget against a ten
/// second turn was honoured at ten seconds.
///
/// The assertion is a count over the pair rather than a claim about either one:
/// exactly one of two identical messages can hold the slot, so exactly one must
/// be answered before the actor is let go.
///
/// Made red by dropping the bound in `deliver_to_actor` back to a bare
/// `mail.send(..).await`, which leaves that count at zero.
#[tokio::test]
async fn a_caller_that_cannot_get_a_mailbox_permit_is_refused_at_its_budget() {
    let mut fixture = start_gated().await;

    let answers = two_behind_a_held_turn(&mut fixture, id("gate")).await;

    let mut refused_before_the_turn_ended = 0;
    for (answered, let_go) in answers {
        let error = answered.expect_err("both budgets ran out before either could run");
        assert!(
            matches!(error, Error::Remote(RemoteError::TimedOut { millis }) if millis == BUDGET_MS),
            "both paths must refuse with the value the local one produces: got {error:?}"
        );
        refused_before_the_turn_ended += usize::from(!let_go);
    }

    assert_eq!(
        refused_before_the_turn_ended, 1,
        "the caller that could not get a mailbox permit must be refused at its budget, \
         not held for the whole turn it was never going to be admitted into"
    );
    assert_eq!(
        gate_touches(&fixture).await,
        0,
        "a refused message must not have entered the turn"
    );
}

// ────────────────────── the bound that gives up from outside the mailbox

/// Puts one keyed message in the mailbox behind a held turn, lets the bound give
/// up on it from outside, and only then lets the actor go.
///
/// Two facts hold at the moment the caller is answered, and neither is left to a
/// race. **It is queued**: it is not sent until `hold` has entered its turn, and
/// a turn that is running holds no mailbox slot, so the one slot is free and the
/// mailbox is the only place the message can be. **It has not run**: the actor
/// is inside `hold`, waiting on a semaphore this test owns, and the permit is
/// added only after the caller already has its answer.
///
/// Nothing asserts on elapsed time. The clock decides one thing, which is when
/// the layer above gives up.
async fn queued_when_the_caller_gave_up(fixture: &mut Gated, key: &str) -> Result<u64, Error> {
    let keyed = gate_under(fixture, key);
    let client = &fixture.client;
    let release = &fixture.release;
    let entered = &mut fixture.entered;
    let (go, wait) = oneshot::channel();

    let holding = async { gate::at(client, id("gate")).hold().await };
    let queued = async {
        wait.await.expect("the driver says when");
        let answered = gate::at(&keyed, id("gate")).touch().await;
        // Answered first, released second, and that order is what the caller's
        // answer is about: it was told something while its message was still
        // sitting in the mailbox, unrun.
        release.add_permits(1);
        answered
    };
    let driver = async {
        entered.recv().await.expect("`hold` entered its turn");
        go.send(()).expect("the queued call is waiting for this");
    };

    let (held, answered, ()) = tokio::join!(holding, queued, driver);
    // Deliberately not asserted, and the reason is the property under test: the
    // bound is watching this caller too, and `hold` outlasts any budget on
    // purpose, so a `Timeout` refuses it and the turn goes on holding the actor
    // regardless. What ends that turn is the semaphore, not the caller.
    let _ = held;
    answered
}

/// The keyed half of RFC-04 §16.12. It used to be the LATENT half — reaching it
/// needed a hand-built envelope, because the keyed client could not attach a key
/// — and it is an ordinary one now, which is the change worth noting where the
/// old note was.
///
/// A `Timeout` runs outside the mailbox, so its budget expiring drops the wait
/// for the reply and not the turn. The message runs. Releasing its key would
/// therefore hand the retry the same work to do a second time — at-least-once,
/// out of the module whose one promise is at-most-once — so reaching the mailbox
/// is marked, and the key settles `Interrupted` instead.
///
/// Made red by removing the `enqueued()` mark from `deliver_to_actor`, which
/// takes the retry back to a first attempt and the counter to two.
#[tokio::test]
async fn a_keyed_message_a_timeout_gave_up_on_in_the_mailbox_settles_its_key() {
    let mut fixture = gated_behind(Timeout::after(BUDGET)).await;

    let refused = queued_when_the_caller_gave_up(&mut fixture, "gave-up")
        .await
        .expect_err("the budget expires while the message is still queued");
    assert!(
        matches!(refused, Error::Remote(RemoteError::TimedOut { millis }) if millis == BUDGET_MS),
        "got {refused:?}"
    );
    assert_eq!(
        gate_touches(&fixture).await,
        1,
        "the turn runs whether its caller stayed or not, which is exactly why giving the key \
         back would be an invitation to run the work twice"
    );

    let retried = gate::at(&gate_under(&fixture, "gave-up"), id("gate"))
        .touch()
        .await
        .expect_err("a key nobody can report on must be settled, not claimable");
    assert!(
        matches!(
            retried,
            Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Interrupted))
        ),
        "got {retried:?}"
    );
    assert_eq!(
        gate_touches(&fixture).await,
        1,
        "the refused retry must not have run the work a second time"
    );
}

/// The same pair as [`two_behind_a_held_turn`], under keys: exactly one gets the
/// one mailbox slot, and exactly one is left waiting for a permit it never gets.
///
/// **The pair is deliberately identical and unordered**, for that helper's
/// reason: a slot is only provably occupied while nothing is draining it, so
/// every assertion here is a count over the two.
async fn two_keyed_behind_a_held_turn(
    fixture: &mut Gated,
    keys: [&str; 2],
) -> [Result<u64, Error>; 2] {
    let [first, second] = keys.map(|key| gate_under(fixture, key));
    let client = &fixture.client;
    let release = &fixture.release;
    let entered = &mut fixture.entered;
    let (go, wait) = oneshot::channel();

    let holding = async { gate::at(client, id("gate")).hold().await };
    let sending = async {
        wait.await.expect("the driver says when");
        let one = gate::at(&first, id("gate"));
        let two = gate::at(&second, id("gate"));
        let answered = tokio::join!(one.touch(), two.touch());
        release.add_permits(1);
        answered
    };
    let driver = async {
        entered.recv().await.expect("`hold` entered its turn");
        go.send(()).expect("the two calls are waiting for this");
    };

    let (held, (left, right), ()) = tokio::join!(holding, sending, driver);
    // Not asserted, for [`queued_when_the_caller_gave_up`]'s reason.
    let _ = held;
    [left, right]
}

/// What the mark being placed **after** the send buys, and it is the whole
/// reason it is not placed before it.
///
/// One of these two got a mailbox permit and one did not, and no test can say
/// which. The one that did leaves a turn behind that runs, so its key must
/// settle. The one that did not queued nothing — tokio drops a send it cancelled
/// — so its key is owed back and its retry is a first attempt. A mark taken on
/// the way *into* `deliver_to_actor` would be indistinguishable on every other
/// assertion in this file and would burn that second key.
///
/// Made red by moving `progress.enqueued()` above `mail.send` in
/// `deliver_to_actor`, which settles both keys and leaves the count at two.
#[tokio::test]
async fn only_the_keyed_message_that_got_a_mailbox_permit_settles_its_key() {
    let mut fixture = gated_behind(Timeout::after(BUDGET)).await;

    for answered in two_keyed_behind_a_held_turn(&mut fixture, ["slot", "no-slot"]).await {
        let error = answered.expect_err("both budgets ran out before either could be answered");
        assert!(
            matches!(error, Error::Remote(RemoteError::TimedOut { millis }) if millis == BUDGET_MS),
            "got {error:?}"
        );
    }
    assert_eq!(
        gate_touches(&fixture).await,
        1,
        "exactly one of the two can hold the slot, so exactly one turn runs once the actor \
         is let go"
    );

    let mut settled = 0;
    for key in ["slot", "no-slot"] {
        match gate::at(&gate_under(&fixture, key), id("gate"))
            .touch()
            .await
        {
            Ok(_) => {}
            Err(Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Interrupted))) => {
                settled += 1;
            }
            other => panic!("a retry is a first attempt or an Interrupted: got {other:?}"),
        }
    }

    assert_eq!(
        settled, 1,
        "the caller that never got a permit must keep its key, or a message that provably \
         never queued is refused forever for work nobody did"
    );
    assert_eq!(
        gate_touches(&fixture).await,
        2,
        "the released key's retry is the only work left to do, and it must have done it"
    );
}

// ─────────────────────────────── what runs outside `guarded`, and what it costs

/// A response whose `Serialize` **panics** where [`Untransmittable`]'s merely
/// errors. One token apart, and a different outcome entirely — which is the
/// whole reason this type is worth its lines.
#[derive(Debug, Clone, serde::Deserialize, postcard_schema::Schema)]
pub struct Exploding;

impl serde::Serialize for Exploding {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        panic!("this reply panics on the way to the wire");
    }
}

// A sixth service, so the shape-for-shape pairing between `journal` and `vault`
// survives a method being added here.
keylink::service! {
    service Fuse;

    /// Answers with a value whose `Serialize` panics.
    rpc Detonate() -> Exploding;

    /// A message that detonates nothing, for asking afterwards whether the
    /// actor is still there.
    rpc Ping() -> u64;
}

struct Fuse;

impl fuse::Actor for Fuse {
    async fn detonate(&mut self, _cx: keylink::Caller<'_>) -> Exploding {
        Exploding
    }

    async fn ping(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        1
    }
}

/// The scenario that proved the defect, inverted — and then made structural.
///
/// `guarded(..)` covered `__actor_turn` and stopped there. In the keyed mailbox
/// loop `payload.open` ran before it and `ActorReply::encode` one line after it,
/// and both executed **user** `Deserialize`/`Serialize` code on the actor's own
/// task. A panic in either was outside every catch, so it unwound out of the
/// mounted task and produced verbatim the failure `keylink/src/panic.rs` says
/// `guarded` prevents: "one bad message drops the receiver, every later send
/// fails, and the actor is unreachable for the life of the process while the
/// registry still lists it as mounted". This test asserted exactly that, and it
/// held.
///
/// **Both calls have left the actor's task.** The reply travels typed, so
/// `serve_one` encodes it on the connection's task, and the arriving request is
/// decoded there too. So this is no longer a divergence between two dispatchers
/// patched by `catching` on one of them — it is the same code path for every
/// service, and the actor cannot be reached by either unwind.
///
/// The caller's answer changed with it: an unwind inside `serve_one` kills that
/// call's task after the frame was built and before it was written, so the caller
/// sees a stream that finished without a reply rather than
/// `RemoteError::Failed("`Serialize` panicked")`. That is what a singleton actor
/// has always done with an exploding reply, and the convergence is the point.
///
/// Made red by encoding the reply back inside the mailbox loop without a catch,
/// which restores the actor-killing behaviour and turns the last two assertions
/// into their inverses.
#[tokio::test]
async fn a_reply_whose_serialize_panics_costs_that_message_and_not_the_actor() {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: fuse::System = fuse::System::builder(Eviction::Never).build();
    let _running = Server::builder(&server)
        .service(fuse::keyed_service(&system, Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = fuse::KeyedClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");

    system
        .mount(id("fuse"), fuse::spawn(Fuse))
        .expect("the id is free");
    assert_eq!(
        fuse::at(&client, id("fuse")).ping().await.unwrap(),
        1,
        "the actor answers before the message that used to kill it"
    );

    // The turn itself is fine: it returns `Exploding`, and `guarded` sees no
    // panic at all. The unwind happens while the frame is being encoded, on the
    // connection's task.
    fuse::at(&client, id("fuse"))
        .detonate()
        .await
        .map(|_| ())
        .expect_err("the reply's `Serialize` panics");

    assert!(
        system.contains(&id("fuse")),
        "the registry goes on listing it — which is the truth rather than a stale entry \
         nothing reaps"
    );

    assert_eq!(
        fuse::at(&client, id("fuse")).ping().await.unwrap(),
        1,
        "and the next caller is answered, which is the whole point of the encode having \
         left the actor's task"
    );
}
