//! The keyed actor system, end to end over a real QUIC connection.
//!
//! Two iroh endpoints on the loopback interface, no relay and no discovery, so
//! the transport is genuine and none of this needs a network.
//!
//! # One system per service, and what that changed here
//!
//! Every fixture in this file used to share one node-wide `keylink::Registry`, and
//! several tests mounted actors of unrelated services into it. That cannot be
//! written any more: a system is `journal::Registry`, `M` is that service's own
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

use keylink::prelude::*;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use keylink::{
    ActorId, Addressed, Allowlist, Backpressure, Body, BodyReader, Context, Ended, Error, Eviction,
    IdempotencyKey, IdempotencyRefusal, LocalError, Middleware, MountError, Next, Node, Outcome,
    QueueTimeout, RemoteError, RpcClient, Server, Serving, Timeout, Withheld,
};
// The port and its file adapter, for the one thing a counter cannot show: what a
// detached actor is still HOLDING. `Claim` is deliberately not in scope — nothing
// here loads or stores, and `mise run framing` is what keeps that honest.
//
// Gated, and so is every item that uses it — `Scratch`, `wedged`, `Dropped`,
// `wedged_once_drained`, their impls, and the tests of the holding-the-claim run
// below. NOT the file: a `#![cfg]` here would take **43** unrelated tests out of a
// `--no-default-features` build with them. This file holds 48, and 5 need the
// adapter. The condition is `persist-file` and not `persist`, because `Files` is
// the adapter half.
//
// (That said "a hundred-odd" until the merge gate caught it. The figure came from
// `grep -c '^async fn \|#\[tokio::test\]'`, which counts the ATTRIBUTE line and the
// `fn` line as two — an item count read as a test count. `cargo test -p keylink
// --test actors -- --list` counts tests, and counts 48.)
//
// **The compiler says whether that set is right, so no count is written here.**
// `cargo check -p keylink --no-default-features --all-targets` builds this file
// with no error AND no `dead_code` warning, and only one set of attributes does
// both: gate too little and it is an unresolved `keylink_persist`, gate too much
// and the leftover helper is never used. Both halves were observed while this
// was being written.
//
// Attributes and not a `#[cfg] mod`, because wrapping would prefix the test
// names, and a test name here is a thing break records and filters cite.
#[cfg(feature = "persist-file")]
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
    /// Fires the entry number as each turn *enters*, before it holds.
    ///
    /// `None` for every test that does not sequence anything against a turn,
    /// which is most of them. The tests that do had a wall-clock sleep guessing
    /// when a turn had started, and a guess about a real QUIC call plus a
    /// dispatch is the shape that goes red on a loaded machine. This is the same
    /// device `Gate::hold` uses further down this file, for the same reason
    /// written there: provably busy rather than probably busy.
    entered: Option<mpsc::UnboundedSender<u64>>,
    /// Acquired inside the turn, after `entered` has fired and before the hold.
    ///
    /// `None` for every test that does not need one turn to wait for another. What
    /// it buys is *overlap*: two turns can be held inside their handlers at the
    /// same moment, which is a fact a channel can carry and a stopwatch cannot.
    release: Option<Arc<Semaphore>>,
    /// Counts handlers that are inside a turn at the same moment.
    ///
    /// `None` unless a test is about overlap. Shared between actors on purpose:
    /// that is what makes it able to say "two of you were in there at once".
    inside: Option<Arc<Inside>>,
    /// A local keyed reference used from inside one turn to prove the actor
    /// identity is carried through the registry mailbox.
    nested: Option<(journal::Registry, ActorId)>,
    /// A keyed actor to call from this actor, used by the registry ring test.
    forward: Option<(journal::Registry, ActorId)>,
    /// A keyed actor to call at the closing edge of the registry ring.
    cycle: Option<(journal::Registry, ActorId)>,
}

/// How many handlers were inside a turn at the same moment, and the most there
/// ever were.
///
/// The overlap proof, in the one form that discriminates in **both** directions
/// and reads no clock: a peak of two says two turns ran at once, and a peak of one
/// says none ever did. A test that asserts only the second is asserting something a
/// broken counter also satisfies, which is why the concurrency half below asserts
/// two on the same machinery the serial half asserts one on.
#[derive(Default)]
struct Inside {
    live: AtomicUsize,
    peak: AtomicUsize,
}

impl Inside {
    fn enter(&self) {
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(live, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

impl journal::Actor for Journal {
    async fn apply(&mut self, _cx: keylink::Caller<'_>, entry: u64, hold_ms: u64) -> u64 {
        if let Some((system, actor)) = &self.nested {
            let error = journal::at_local(system, actor.clone())
                .entries()
                .await
                .expect_err("a keyed actor must reject its own nested call");
            assert!(matches!(error, LocalError::Reentrant));
        }
        if let Some((system, actor)) = self.forward.clone() {
            journal::at_local(&system, actor)
                .apply(entry, 0)
                .await
                .expect("the first edge of the ring is allowed");
        }
        if let Some((system, actor)) = self.cycle.clone() {
            let error = journal::at_local(&system, actor)
                .apply(entry, 0)
                .await
                .expect_err("a keyed actor must reject the closing edge of a ring");
            assert!(matches!(error, LocalError::Reentrant));
        }
        if let Some(inside) = &self.inside {
            inside.enter();
        }
        if let Some(entered) = &self.entered {
            let _ = entered.send(entry);
        }
        if let Some(release) = &self.release {
            release
                .acquire()
                .await
                .expect("the test never closes the semaphore")
                .forget();
        }
        // The await point `abort` cancels at, and the reason `stop` and `abort`
        // leave the state in different shapes.
        tokio::time::sleep(Duration::from_millis(hold_ms)).await;
        self.applied.lock().expect("not poisoned").push(entry);
        if let Some(inside) = &self.inside {
            inside.leave();
        }
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
    system: journal::Registry,
    client: journal::RegistryClient,
    _server: Server,
    _nodes: [Node; 2],
}

async fn start(system: journal::Registry) -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    // `registry.serve` rather than the builder, and this fixture is where it earns
    // its keep: every keyed test in this file serves through here, so a twin
    // that did not reach the same ALPN would redden the file rather than sit
    // unexercised. The builder form is still what the tests below use whenever
    // they add a layer, a store or a second service — which is most of them.
    let running = system
        .serve(&server, Allowlist::anyone())
        .expect("one service, no duplicate ALPN");
    let client = journal::RegistryClient::connect(&client_node, server.endpoint().addr())
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

fn mounted() -> journal::Registry {
    journal::Registry::builder(Eviction::Never).build()
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

    let actor = fixture.client.at(id("obra/4471"));
    assert_eq!(actor.apply(7, 0).await.unwrap(), 7);
    assert_eq!(actor.entries().await.unwrap(), vec![7]);
}

#[tokio::test]
async fn a_local_keyed_ref_answers_by_id() {
    let system = mounted();
    system
        .mount(id("obra/4471"), journal::spawn(Journal::default()))
        .expect("the id is free");

    let actor = journal::at_local(&system, id("obra/4471"));
    assert_eq!(actor.apply(7, 0).await.unwrap(), 7);
    assert_eq!(actor.entries().await.unwrap(), vec![7]);
}

#[tokio::test]
async fn a_keyed_actor_ending_can_be_awaited_before_shutdown() {
    let system = mounted();
    let actor_id = id("ending");
    system
        .mount(actor_id.clone(), journal::spawn(Journal::default()))
        .expect("the id is free");
    let ending = system
        .ended(&actor_id)
        .expect("a mounted actor has an ending future");

    assert!(matches!(
        system
            .stop(&actor_id, Duration::from_secs(1))
            .await
            .expect("the actor drains"),
        Ended::Stopped
    ));
    assert!(matches!(ending.await, Ended::Stopped));
    assert!(system.ended(&actor_id).is_none());
}

#[tokio::test]
async fn a_local_keyed_ref_rejects_a_reentrant_call() {
    let system = mounted();
    let actor_id = id("nested");
    let state = Journal {
        nested: Some((system.clone(), actor_id.clone())),
        ..Journal::default()
    };
    system
        .mount(actor_id.clone(), journal::spawn(state))
        .expect("the id is free");

    let actor = journal::at_local(&system, actor_id.clone());
    let result = tokio::time::timeout(Duration::from_secs(30), actor.apply(7, 0))
        .await
        .expect("the re-entrant call must resolve without waiting for the actor");
    assert_eq!(result.expect("the outer turn answers"), 7);
    system
        .stop(&actor_id, Duration::from_secs(1))
        .await
        .expect("the actor drains");
}

#[tokio::test]
async fn local_keyed_refs_reject_a_two_actor_ring() {
    let system = mounted();
    let first = id("ring/first");
    let second = id("ring/second");
    system
        .mount(
            first.clone(),
            journal::spawn(Journal {
                forward: Some((system.clone(), second.clone())),
                ..Journal::default()
            }),
        )
        .expect("the first id is free");
    system
        .mount(
            second.clone(),
            journal::spawn(Journal {
                cycle: Some((system.clone(), first.clone())),
                ..Journal::default()
            }),
        )
        .expect("the second id is free");

    assert_eq!(
        journal::at_local(&system, first.clone())
            .apply(7, 0)
            .await
            .expect("the closing edge is refused and the outer turn answers"),
        7
    );
    assert_eq!(
        journal::at_local(&system, first.clone())
            .entries()
            .await
            .expect("the first actor remains live"),
        vec![7]
    );
    assert_eq!(
        journal::at_local(&system, second.clone())
            .entries()
            .await
            .expect("the second actor remains live"),
        vec![7]
    );
    let _ = system.stop(&first, Duration::from_secs(1)).await;
    let _ = system.stop(&second, Duration::from_secs(1)).await;
}

#[tokio::test]
async fn local_keyed_calls_park_when_the_mailbox_is_full() {
    let system: journal::Registry = journal::Registry::builder(Eviction::Never)
        .mailbox_depth(NonZeroUsize::new(1).expect("non-zero depth"))
        .build();
    let actor_id = id("backpressure");
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    system
        .mount(
            actor_id.clone(),
            journal::spawn(Journal {
                entered: Some(entered_tx),
                release: Some(Arc::clone(&release)),
                ..Journal::default()
            }),
        )
        .expect("the id is free");

    let first_system = system.clone();
    let first_id = actor_id.clone();
    let first =
        tokio::spawn(async move { journal::at_local(&first_system, first_id).apply(1, 0).await });
    assert_eq!(entered.recv().await, Some(1), "the first turn is held");

    let second_system = system.clone();
    let second_id = actor_id.clone();
    let second = tokio::spawn(async move {
        journal::at_local(&second_system, second_id)
            .apply(2, 0)
            .await
    });
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if status_of(&system, &actor_id).queued == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the second local call is in the keyed mailbox");

    // The local keyed route uses `send`, not `try_send`: a caller waits for a
    // slot rather than receiving a made-up `Full` refusal.
    let third_ref = journal::at_local(&system, actor_id.clone());
    let mut third = Box::pin(third_ref.apply(3, 0));
    let waker = std::task::Waker::noop();
    let mut task_cx = std::task::Context::from_waker(waker);
    assert!(
        matches!(third.as_mut().poll(&mut task_cx), std::task::Poll::Pending),
        "a full keyed mailbox parks the local call"
    );
    drop(third);

    release.add_permits(2);
    assert_eq!(
        first
            .await
            .expect("first task joins")
            .expect("first answers"),
        1
    );
    assert_eq!(
        second
            .await
            .expect("second task joins")
            .expect("second answers"),
        2
    );
    system
        .stop(&actor_id, Duration::from_secs(1))
        .await
        .expect("the actor drains");
}

#[tokio::test]
async fn a_local_keyed_ref_reports_a_missing_actor_without_a_remote_error() {
    let system = mounted();

    let error = journal::at_local(&system, id("missing"))
        .entries()
        .await
        .expect_err("the registry has no actor or factory for this id");
    assert!(matches!(
        error,
        LocalError::NoSuchActor { id: actor_id } if actor_id == id("missing")
    ));
}

#[tokio::test]
async fn local_keyed_ref_uses_the_factory_and_reports_capacity() {
    let system: journal::Registry = journal::Registry::builder(Eviction::Never)
        .capacity(1)
        .spawn_with(|_| Some(journal::spawn(Journal::default())))
        .build();

    assert_eq!(
        journal::at_local(&system, id("factory/one"))
            .apply(1, 0)
            .await
            .expect("the factory builds the first actor"),
        1
    );
    assert!(system.contains(&id("factory/one")));

    let error = journal::at_local(&system, id("factory/two"))
        .apply(2, 0)
        .await
        .expect_err("the registry cap applies to local keyed calls");
    assert!(matches!(error, LocalError::AtCapacity { capacity: 1 }));
    let _ = system
        .stop(&id("factory/one"), Duration::from_secs(1))
        .await;
}

/// The property the whole design exists for: mounting is a map write, so an
/// actor mounted *after* a peer connected is reachable on that same connection.
/// A per-actor ALPN would need a re-announcement and a fresh handshake.
#[tokio::test]
async fn an_actor_mounted_after_the_client_connected_is_reachable_on_the_same_connection() {
    let fixture = start(mounted()).await;

    // The connection already exists and has negotiated its ALPN.
    let missing = fixture
        .client
        .at(id("later"))
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
        fixture.client.at(id("later")).entries().await.unwrap(),
        Vec::<u64>::new(),
        "the same connection must reach an actor that did not exist when it was opened"
    );
}

#[tokio::test]
async fn ten_thousand_actors_are_mounted_and_addressable() {
    const ACTORS: usize = 10_000;

    let fixture = start(
        journal::Registry::builder(Eviction::Never)
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
        let actor = fixture.client.at(id(&format!("obra/{n}")));
        assert_eq!(
            actor.apply(n as u64, 0).await.unwrap(),
            n as u64,
            "actor obra/{n} must answer"
        );
    }

    // Each actor kept its own state, which is what "keyed" means.
    assert_eq!(
        fixture.client.at(id("obra/0")).entries().await.unwrap(),
        vec![0]
    );
    assert_eq!(
        fixture.client.at(id("obra/1")).entries().await.unwrap(),
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

    let error = fixture
        .client
        .at(id("ghost"))
        .apply(1, 0)
        .await
        .expect_err("nothing is mounted there");
    assert!(
        matches!(error, Error::Remote(RemoteError::NoSuchActor { .. })),
        "got {error:?}"
    );

    assert_eq!(
        fixture.client.at(id("real")).apply(2, 0).await.unwrap(),
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
        .stop(&id("was/real"), Duration::from_secs(5))
        .await
        .is_some());

    let stopped = fixture
        .client
        .at(id("was/real"))
        .apply(1, 0)
        .await
        .expect_err("it is gone");
    let never = fixture
        .client
        .at(id("was/real"))
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
/// `ActorId::validate` moved from `Registry::deliver` into `deliver_keyed` with
/// this wave, and it moved with its ordering intact — before `route`, so an
/// over-long id costs no lookup and no lazily spawned state. This test and the
/// spawn counter are what say so. Made red by moving the `validate` call below
/// the `route` call in `actor/remote.rs`.
#[tokio::test]
async fn an_oversized_id_is_refused_before_it_reaches_the_registry() {
    let spawned = Arc::new(AtomicUsize::new(0));
    let counter = spawned.clone();
    let fixture = start(
        journal::Registry::builder(Eviction::Never)
            .spawn_with(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Some(journal::spawn(Journal::default()))
            })
            .build(),
    )
    .await;

    let error = fixture
        .client
        .at(oversized_id())
        .apply(1, 0)
        .await
        .expect_err("an over-long id must be refused");

    assert!(
        matches!(error, Error::Remote(RemoteError::BadActorId { ref reason, .. }) if reason.contains("actor id")),
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
/// `keylink/src/representability.rs`: a `journal::Registry` cannot hold a `vault`
/// actor.
///
/// **What refuses is the ALPN string, not the type parameter**, and the
/// distinction is worth being exact about because the first draft of this test
/// got it wrong. `RpcClient::connect` takes the ALPN as an untyped
/// `&'static [u8]`, so `RpcClient::<Addressed<vault::Request>, _>::connect(..,
/// journal::registry_alpn())` connects perfectly happily — the types are not part of
/// the handshake, the bytes they hash into are. What makes the pairing safe is
/// that `vault::RegistryClient` is the only thing that calls `vault::registry_alpn()`,
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
        .service(journals.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");

    // A peer built against `vault` cannot reach a node serving `journal`'s
    // system, even though its bytes would have decoded there.
    let refused = vault::RegistryClient::connect(&client_node, journal_only.endpoint().addr())
        .await
        .map(|_| ())
        .expect_err("a vault peer must not complete a handshake with journal's system");
    assert!(
        matches!(refused, Error::SchemaMismatch { .. }),
        "the refusal must name the schema, not the network: got {refused:?}"
    );

    // Journal's own client reaches it on the same node.
    let journal_client =
        journal::RegistryClient::connect(&client_node, journal_only.endpoint().addr())
            .await
            .expect("journal's own keyed client dials journal's ALPN");
    assert_eq!(
        journal_client.at(id("ledger")).apply(99, 0).await.unwrap(),
        99
    );

    // And the two identically shaped services coexist on one node, each holding
    // its own actor under the same id.
    let more_journals = mounted();
    let vaults: vault::Registry = vault::Registry::builder(Eviction::Never).build();
    more_journals
        .mount(id("ledger"), journal::spawn(Journal::default()))
        .expect("the id is free");
    vaults
        .mount(id("ledger"), vault::spawn(Vault))
        .expect("the id is free");
    let _serving_both = Server::builder(&both)
        .service(more_journals.service(Allowlist::anyone()).build())
        .service(vaults.service(Allowlist::anyone()).build())
        .spawn()
        .expect("two identically shaped services, two distinct keyed ALPNs");

    let to_journal = journal::RegistryClient::connect(&client_node, both.endpoint().addr())
        .await
        .expect("dials journal's ALPN");
    let to_vault = vault::RegistryClient::connect(&client_node, both.endpoint().addr())
        .await
        .expect("dials vault's ALPN");
    assert_eq!(to_journal.at(id("ledger")).apply(99, 0).await.unwrap(), 99);
    assert_eq!(to_vault.at(id("ledger")).apply(99, 0).await.unwrap(), 99);
    assert_eq!(
        to_journal.at(id("ledger")).entries().await.unwrap(),
        vec![99],
        "the two systems hold different actors under the same id, on one node"
    );
    assert_eq!(
        to_vault.at(id("ledger")).entries().await.unwrap(),
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

    let error = fixture
        .client
        .at(id("fragile"))
        .boom()
        .await
        .expect_err("the handler panics");
    assert!(
        matches!(
            error,
            Error::Remote(RemoteError::Withheld(Withheld::Panicked { .. }))
        ),
        "got {error:?}"
    );

    assert_eq!(
        fixture.client.at(id("fragile")).apply(1, 0).await.unwrap(),
        1,
        "the actor must still be serving"
    );
}

/// **The keyed twin the singleton test asked for and never had.**
///
/// `tests/singleton.rs::both_halves_of_a_lost_turn_carry_their_own_variant` pins
/// that a panic reaches a peer as `Withheld::Panicked` and an observer's veto as
/// `Withheld::Refused`. Both were prefixes on a `RemoteError::Failed` string for
/// as long as a variant was unaffordable — a variant sits inside the hashed
/// contract and moves every service's ALPN — so **the string was the whole
/// discriminator** until the ALPN was paid.
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
/// This comparison exercises the default keyed spawn with no observers. Keyed
/// `setup(..).feeds(..).mountable()` does accept observers; their refusal behavior is covered with
/// the feed and observer lifecycle tests.
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
        .service(system.service(Allowlist::anyone()).build())
        .service(singleton.service(Allowlist::anyone()).build())
        .spawn()
        .expect("the keyed and singleton ALPNs are distinct");

    let to_keyed = journal::RegistryClient::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials the actor system");
    let to_singleton = journal::Client::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials the same node's singleton");

    let keyed = to_keyed
        .at(id("fragile"))
        .boom()
        .await
        .expect_err("the handler panics");
    let single = to_singleton
        .boom()
        .await
        .expect_err("the same handler panics");

    let (
        Error::Remote(RemoteError::Withheld(Withheld::Panicked { message: keyed })),
        Error::Remote(RemoteError::Withheld(Withheld::Panicked { message: single })),
    ) = (&keyed, &single)
    else {
        panic!("both paths answer a panic with `Withheld::Panicked`; got {keyed:?} and {single:?}");
    };
    assert_eq!(
        keyed, single,
        "a peer must not be able to tell which dispatch path served it by reading \
         how a panic was rendered"
    );
    assert!(
        keyed.contains("this message is bad"),
        "and the handler's own message has to survive the crossing; got {keyed:?}"
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
        journal::Registry::builder(Eviction::Never)
            .spawn_with(move |id| {
                names.lock().expect("not poisoned").push(id.to_string());
                Some(journal::spawn(Journal::default()))
            })
            .build(),
    )
    .await;

    assert!(fixture.system.is_empty(), "nothing is mounted by hand");

    assert_eq!(
        fixture.client.at(id("obra/9")).apply(3, 0).await.unwrap(),
        3
    );

    assert!(fixture.system.contains(&id("obra/9")));
    assert_eq!(*seen.lock().unwrap(), vec!["obra/9".to_string()]);

    // A second message reuses the actor rather than building another.
    assert_eq!(
        fixture.client.at(id("obra/9")).entries().await.unwrap(),
        vec![3],
        "the second message must reach the actor the first one spawned"
    );
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_factory_that_declines_an_id_produces_no_such_actor() {
    let fixture = start(
        journal::Registry::builder(Eviction::Never)
            .spawn_with(|id| (id.as_str() == "known").then(|| journal::spawn(Journal::default())))
            .build(),
    )
    .await;

    let error = fixture
        .client
        .at(id("unknown"))
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
        journal::Registry::builder(Eviction::Never)
            .capacity(2)
            .spawn_with(|_| Some(journal::spawn(Journal::default())))
            .build(),
    )
    .await;

    for n in 0..2 {
        assert_eq!(
            fixture
                .client
                .at(id(&format!("a{n}")))
                .apply(n, 0)
                .await
                .unwrap(),
            n
        );
    }

    let error = fixture
        .client
        .at(id("a2"))
        .apply(2, 0)
        .await
        .expect_err("the system is full");
    assert!(
        matches!(
            error,
            Error::Remote(RemoteError::AtCapacity { capacity: 2 })
        ),
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
        journal::Registry::builder(Eviction::Idle {
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

    // No `evict_idle` call: the system's own sweeper has to do this. Waited for
    // rather than slept for: a fixed sleep is a bet that the sweeper's task is
    // scheduled inside it, and under load it was not.
    until_empty(&fixture.system).await;
}

/// Wait until the sweeper has evicted every actor in `system`.
///
/// Bounded, so a sweeper that never evicts is a red test and not a hung one; the
/// bound is far past anything load can spend (a whole-process stall of 35 s was
/// measured on a loaded machine) and names what it waited for.
async fn until_empty(system: &journal::Registry) {
    tokio::time::timeout(Duration::from_secs(120), async {
        while !system.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the sweeper evicted every idle actor within 120 s");
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
/// With a budget the two numbers are independent and both are deliberate.
/// `after` is short: the actor is stale almost at once, which is what puts the
/// eviction inside the turn. `shutdown_budget` is five seconds, and **that** is
/// what buys the answer. The name says `inside_its_shutdown_budget` because that
/// is the condition; the converse — a turn that outlives the budget being
/// destroyed — is `an_eviction_whose_budget_expires_destroys_the_actor` below,
/// and the two together are the whole of what an eviction budget means.
///
/// # What decides that the turn is in flight, and what does not
///
/// It was a 250 ms turn against a 30 ms `after`, with the actor mounted before
/// the call: the clock started at the mount, so a loaded machine that took the
/// call longer than `after` to arrive saw the sweeper evict an actor that had
/// never been addressed, and the call answered `NoSuchActor`. Nothing in that
/// shape could tell "evicted during the turn" from "evicted before the turn".
///
/// Now the actor is spawned by the call itself, so its clock starts when the
/// message is routed. The turn is parked on a semaphore from the moment it
/// enters, and is let go only after the sweeper has taken the actor out of the
/// system. The order is therefore a fact the test observed, not a duration it
/// hopes for: entered, evicted, answered.
#[tokio::test]
async fn eviction_inside_its_shutdown_budget_does_not_cut_off_a_message_in_flight() {
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let fixture = start(
        journal::Registry::builder(Eviction::Idle {
            after: Duration::from_millis(200),
            shutdown_budget: Duration::from_secs(5),
        })
        .spawn_with({
            let release = release.clone();
            move |_| {
                Some(journal::spawn(Journal {
                    entered: Some(entered_tx.clone()),
                    release: Some(release.clone()),
                    ..Journal::default()
                }))
            }
        })
        .build(),
    )
    .await;

    let calling = async { fixture.client.at(id("slow")).apply(5, 0).await };
    let driving = async {
        entered.recv().await.expect("the turn enters the actor");
        // The turn is parked, and nothing but this test can end it, so the
        // actor leaving the system now is an eviction in the middle of a turn.
        until_empty(&fixture.system).await;
        release.add_permits(1);
    };
    let (answered, ()) = tokio::join!(calling, driving);

    assert_eq!(
        answered.unwrap(),
        5,
        "the message in flight must be answered"
    );
}

/// Eviction never touches the ALPN, so a peer connected before it can still
/// reach an actor spawned after it.
///
/// The idle window is long enough that the second call, which spawns its actor
/// and routes to it at once, cannot be outwaited by a loaded machine between
/// those two steps. The first actor is waited for, not slept for.
#[tokio::test]
async fn the_connection_outlives_the_actors_on_it() {
    let fixture = start(
        journal::Registry::builder(Eviction::Idle {
            after: Duration::from_millis(200),
            shutdown_budget: Duration::from_secs(5),
        })
        .spawn_with(|_| Some(journal::spawn(Journal::default())))
        .build(),
    )
    .await;

    assert_eq!(fixture.client.at(id("first")).apply(1, 0).await.unwrap(), 1);
    until_empty(&fixture.system).await;

    assert_eq!(
        fixture.client.at(id("second")).apply(2, 0).await.unwrap(),
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
/// one message in flight does not queue anything. The default local mailbox depth
/// is 1, so the second
/// `apply` is queued behind the first and is the queued turn; the budget covers
/// both turns together, which is the depth-versus-budget relationship 4a's doc
/// names — at depth N a budget has to cover N + 1 turns, not one.
///
/// # Three facts observed, and no clock
///
/// What must hold is an ORDER, and each step of it is a fact the test waits for
/// rather than a duration it guesses:
///
/// 1. *the actor is busy before anything else happens*: `Journal::entered` fires
///    as the turn is entered, and the turn then parks on `release`;
/// 2. *the queued call is in the mailbox before the shutdown starts*: read from
///    the registry's own `serving`, which counts the mailbox;
/// 3. *the shutdown has taken the actor out of the system before either turn is
///    let go*: `is_empty`, so both turns provably finish inside a drain and not
///    before it.
///
/// Steps 2 and 3 were a 40 ms sleep and a 200 ms hold. Under load the queued call
/// took longer than the sleep, and the shutdown removed the entry first, so it
/// answered `NoSuchActor` and this test failed for a reason that is not about
/// draining.
#[tokio::test]
async fn shutdown_inside_its_budget_answers_the_turn_in_flight_and_the_one_queued() {
    let applied = Arc::new(Mutex::new(Vec::new()));
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(
            id("doc"),
            journal::spawn(Journal {
                applied: applied.clone(),
                entered: Some(entered_tx),
                release: Some(release.clone()),
                inside: None,
                nested: None,
                forward: None,
                cycle: None,
            }),
        )
        .expect("the id is free");

    let running = {
        let client = &fixture.client;
        async move { client.at(id("doc")).apply(1, 0).await }
    };
    let sequenced = async {
        assert_eq!(
            entered.recv().await,
            Some(1),
            "the first turn is inside the actor before anything below runs"
        );
        let behind = fixture.client.at(id("doc"));
        let queued = behind.apply(2, 0);
        let stopping = async {
            tokio::time::timeout(Duration::from_secs(120), async {
                while fixture.system.serving(&id("doc")).map(|row| row.queued) != Some(1) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the second call reached the mailbox behind the turn in flight");
            let doc = id("doc");
            let stopped = fixture.system.stop(&doc, Duration::from_secs(5));
            let letting_go = async {
                until_empty(&fixture.system).await;
                release.add_permits(2);
            };
            tokio::join!(stopped, letting_go).0
        };
        tokio::join!(queued, stopping)
    };

    let (first, (second, ended)) = tokio::join!(running, sequenced);

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
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let fixture = start(mounted()).await;
    fixture
        .system
        .mount(
            id("doc"),
            journal::spawn(Journal {
                applied: applied.clone(),
                entered: Some(entered_tx),
                release: None,
                inside: None,
                nested: None,
                forward: None,
                cycle: None,
            }),
        )
        .expect("the id is free");

    let client_call = {
        let client = &fixture.client;
        // A hold the abort is sequenced against by `entered`, not by a clock:
        // the abort lands the moment the turn is inside, so the hold only has
        // to outlast the gap between those two events, and it is the length of
        // the red a broken `abort` takes to show.
        async move { client.at(id("doc")).apply(1, 5_000).await }
    };
    let aborting = async {
        entered.recv().await.expect("the turn is inside the actor");
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
        .stop(&id("nobody"), Duration::from_secs(5))
        .await
        .is_none());
    assert!(!system.abort(&id("nobody")).await);
}

// ───────────────────────────────── an expired budget, and what it must destroy
//
// The tests below are one claim from five sides: **a teardown nobody waited for
// is not a teardown.** A `JoinHandle` that is dropped rather than aborted
// DETACHES its task on both of keylink's targets — verified against n0-future
// 0.3.2, whose native branch is tokio's and whose wasm branch has no `impl Drop
// for JoinHandle` at all — so every path that let go of one left an actor running
// with its state, and its state's exclusive resources, and no way back to it. A
// counter cannot show that; a file lock can, which is why these reach for the
// durability port. Nothing here loads or stores a snapshot: the claim is used
// purely as the thing only a dead task releases.
//
// Three ways to let go of a handle, and one price for holding on:
//
// - the budget expires, by a caller's `shutdown` and by the sweeper's;
// - the CALL is cancelled, which no budget reaches and which the deleted `stop`
//   was documented to compose with;
// - a mount is refused after the actor was already spawned;
// - and, held rather than let go, the actor outlives its own id by up to one
//   budget — the window a lazily re-spawned peer is refused inside.

/// A scratch directory that removes itself.
///
/// A third copy of a five-line struct that `tests/durable.rs` and
/// `keylink-persist/src/file.rs` also have, and it stays a copy: the value in it
/// is the *unique path*, and a shared helper would have to be given the calling
/// binary's name to keep that property. Small enough to not be worth a
/// dependency, and small enough to not be worth a fourth crate.
///
/// **The `persist-file` gate below is now a third copy too, and that argument does
/// NOT cover it.** The reason above is about the unique path; the feature condition
/// is identical in all three places and duplicated only because the struct is. If a
/// fourth copy appears, that is the point at which the gate is repeated more than
/// the thing it guards, and the shared-helper trade is worth re-opening on the
/// feature's terms rather than on the path's. Noted once, here, rather than three
/// times.
#[cfg(feature = "persist-file")]
struct Scratch(std::path::PathBuf);

#[cfg(feature = "persist-file")]
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

#[cfg(feature = "persist-file")]
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
#[cfg(feature = "persist-file")]
fn wedged(claim: impl Send + 'static) -> journal::Mountable {
    keylink::mount(|inbox: journal::Mailbox| async move {
        let _wedged = (claim, inbox);
        // `pending::<Ended>` and not `pending::<()>`: a loop has to name its
        // ending now, and this one never reaches an end to name.
        std::future::pending::<keylink::Ended>().await
    })
}

/// Fires when whatever it sits beside is dropped.
///
/// A test with no handle on a task cannot ask whether that task is gone, and an
/// `abort` is a *request to the runtime* rather than an action in the caller's
/// frame: the task's future — and the state it owns — is dropped on a scheduler
/// turn after `abort()` returns. So the actor is asked to say so on its way out.
///
/// Put last in a tuple beside the claim, because a tuple drops its fields in
/// declaration order: by the time this fires, the claim beside it is already
/// released. Awaiting it is what makes a destroyed-actor test deterministic
/// instead of a yield-and-hope; a **detached** actor never drops anything, so
/// the wait is bounded by a `timeout` and its expiry is the failure.
#[cfg(feature = "persist-file")]
struct Dropped(Option<oneshot::Sender<()>>);

#[cfg(feature = "persist-file")]
impl Drop for Dropped {
    fn drop(&mut self) {
        if let Some(fired) = self.0.take() {
            let _ = fired.send(());
        }
    }
}

/// An actor that reports the moment its mailbox closes and then will not end.
///
/// `drain` drops the registry's sender *before* it waits out the budget, so
/// `recv` returning `None` is the instant the entry has left the map and the
/// budget has started running. That is the window
/// `Eviction::Idle::shutdown_budget` documents, observed rather than slept
/// through.
#[cfg(feature = "persist-file")]
fn wedged_once_drained(
    claim: impl Send + 'static,
    closed: oneshot::Sender<()>,
) -> journal::Mountable {
    keylink::mount(|mut inbox: journal::Mailbox| async move {
        let _held = claim;
        while inbox.recv().await.is_some() {}
        let _ = closed.send(());
        std::future::pending::<keylink::Ended>().await
    })
}

/// A `shutdown` whose budget expires **destroys** the actor, and the proof is
/// that its exclusive claim can be taken again.
///
/// Made red by `scripts/break-once.sh`, replacing `drain`'s
/// `std::pin::pin!(AbortOnDropHandle::new(task))` with
/// `std::pin::pin!(n0_future::task::spawn(async move { let _ = task.await; }))` —
/// one literal, and the deleted `Registry::stop`'s exact shape: the expiry then
/// aborts a *wrapper* task and drops the real handle, which detaches. Watched:
/// this test and `an_eviction_whose_budget_expires_destroys_the_actor` both fail
/// with `Busy { id: StateId("wedged/4471") }`.
///
/// Paused clock, so the fifty milliseconds cost nothing and mean exactly fifty
/// milliseconds. Every await between the mount and the shutdown is the shutdown's
/// own, and the system has no sweeper, so there is no second thing that could
/// have done this.
#[cfg(feature = "persist-file")]
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

    let ended = system.stop(&id("wedged"), Duration::from_millis(50)).await;

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

/// A `shutdown` whose FUTURE is dropped destroys the actor too, and that is the
/// half no budget can buy.
///
/// The third instance of one defect, and the one a budget does not reach. The
/// entry leaves the map before the first await, so a caller who wraps this call
/// in a `timeout`, a `select!` or a `race` and lets it go has already unmounted
/// the actor — and with a bare `JoinHandle` in `drain`'s frame, dropping that
/// frame **detached** it. Byte for byte the deleted `stop`'s outcome, reached
/// through cancellation instead of through expiry: entry gone, task loose, state
/// and claim still held, and `Registry::abort` answering `false` for an actor that
/// is still running. `Registry::abort` survives cancellation for the one-line
/// reason its own doc gives — it aborts BEFORE its await — and `drain` cannot
/// borrow that trick, because aborting first is the opposite of draining.
///
/// One millisecond against a ten-second budget: the budget cannot be what ends
/// this, and a paused clock makes the millisecond free. Made red by replacing
/// `drain`'s `AbortOnDropHandle::new(task)` with a bare `task` — see the module's
/// own record of what that break printed.
#[cfg(feature = "persist-file")]
#[tokio::test(start_paused = true)]
async fn a_dropped_shutdown_destroys_the_actor_it_has_already_unmounted() {
    let scratch = Scratch::new("dropped-shutdown");
    let files = Files::open(&scratch.0).await.expect("the store opens");
    let held = StateId::new("wedged/4471").expect("a valid state id");
    let claim = files.claim(&held).await.expect("nothing holds it yet");
    let (gone, dropped) = oneshot::channel();

    let system = mounted();
    system
        .mount(id("wedged"), wedged((claim, Dropped(Some(gone)))))
        .expect("the id is free");

    let cut = tokio::time::timeout(
        Duration::from_millis(1),
        system.stop(&id("wedged"), Duration::from_secs(10)),
    )
    .await;

    assert!(
        cut.is_err(),
        "the timeout must be what ended this and not the budget, got {cut:?}"
    );
    assert!(
        system.is_empty(),
        "the entry left the map before the await, which is why a detach is unreachable"
    );
    tokio::time::timeout(Duration::from_secs(5), dropped)
        .await
        .expect("the dropped shutdown aborted the actor; a detached one never ends")
        .expect("the guard is dropped with the state, never sent by hand");
    files
        .claim(&held)
        .await
        .expect("and the state went with it, so its claim is free");
}

/// The same, done by the sweeper's budget instead of a caller's.
///
/// `Eviction::Idle`'s own rustdoc claimed *"Eviction is `Registry::stop`, not
/// `Registry::abort`"*, and the sweeper it described never awaited anything at all:
/// it dropped entries inside a `retain`, under the write lock, which detached
/// every task it evicted. This is that sentence turned into a test.
///
/// `after` is a minute, which makes the sweeper's own tick thirty seconds; the
/// budget is fifty milliseconds and is the only number this test is about.
///
/// # Why this waits for the actor rather than for the call
///
/// The two paths into `sweep` race for one write lock and `extract_if` runs
/// before its first await, so whichever gets there removes the entry and the
/// other finds nothing. So `evict_idle()` returning says nothing about whether
/// *its* teardown was the one that ran, and re-claiming right after it was a
/// timing assumption dressed as an assertion — green, and green because the
/// explicit sweep usually won the lock, not because it had to.
///
/// The `Dropped` guard removes the assumption instead of documenting it: both
/// paths go through the one `drain`, so whichever won, the actor's state is
/// dropped and says so. The `timeout` around that wait is the failure mode — a
/// detached actor never drops anything, so it expires — and under a paused clock
/// it costs nothing.
#[cfg(feature = "persist-file")]
#[tokio::test(start_paused = true)]
async fn an_eviction_whose_budget_expires_destroys_the_actor() {
    let scratch = Scratch::new("eviction-budget");
    let files = Files::open(&scratch.0).await.expect("the store opens");
    let held = StateId::new("wedged/4471").expect("a valid state id");
    let claim = files.claim(&held).await.expect("nothing holds it yet");
    let (gone, dropped) = oneshot::channel();

    let system: journal::Registry = journal::Registry::builder(Eviction::Idle {
        after: Duration::from_secs(60),
        shutdown_budget: Duration::from_millis(50),
    })
    .build();
    system
        .mount(id("wedged"), wedged((claim, Dropped(Some(gone)))))
        .expect("the id is free");

    tokio::time::advance(Duration::from_secs(90)).await;
    system.evict_idle().await;

    assert!(system.is_empty(), "the idle actor is evicted");
    tokio::time::timeout(Duration::from_secs(5), dropped)
        .await
        .expect("an evicted actor past its budget is destroyed; a detached one never ends")
        .expect("the guard is dropped with the state, never sent by hand");
    files
        .claim(&held)
        .await
        .expect("the evicted actor released its claim; if this is Busy, it was detached");
}

/// The window the budget opens, from the side a peer sees: an evicted actor's id
/// is free again while the state behind it is not.
///
/// `sweep` takes the entry out under the write guard and only then waits out the
/// budget, so between those two the id resolves to nothing and the actor is alive
/// and still holding whatever it holds. With a lazy factory that is reachable
/// from the network: the next message routes, finds nothing, builds a new actor,
/// and the new actor goes for a resource its predecessor has not released — a
/// `Busy` for a durable one, for up to one whole budget.
///
/// This is strictly better than what it replaced, where those resources were held
/// **forever** on a detached task, and it is still a new bounded failure a caller
/// can meet. `Eviction::Idle::shutdown_budget` says so; this is that paragraph as
/// a test.
///
/// A real clock and no `advance`, deliberately. Nothing here asserts a duration:
/// the eviction is triggered by making `after` a millisecond, the moment it lands
/// arrives on a channel, and the budget is a minute so it cannot expire inside a
/// test that finishes in milliseconds. A paused clock would auto-advance past
/// that minute the first time this task waited on the filesystem, destroying the
/// actor whose survival is the point.
#[cfg(feature = "persist-file")]
#[tokio::test]
async fn an_evicted_actor_still_holds_its_state_after_its_id_is_free_again() {
    let scratch = Scratch::new("eviction-window");
    let files = Files::open(&scratch.0).await.expect("the store opens");
    let held = StateId::new("wedged/4471").expect("a valid state id");
    let claim = files.claim(&held).await.expect("nothing holds it yet");
    let (closed, evicted) = oneshot::channel();

    let system: journal::Registry = journal::Registry::builder(Eviction::Idle {
        after: Duration::from_millis(1),
        shutdown_budget: Duration::from_secs(60),
    })
    .build();
    system
        .mount(id("wedged"), wedged_once_drained(claim, closed))
        .expect("the id is free");

    // The sweeper's own tick does this, and the actor says when: its mailbox is
    // closed by `drain`, after `extract_if` and before the budget. Bounded, so a
    // teardown that skipped the drain and destroyed the actor outright is a red
    // test rather than a hung one — which is exactly how this was red-tested.
    tokio::time::timeout(Duration::from_secs(10), evicted)
        .await
        .expect("the sweeper evicted the idle actor, closing its mailbox first")
        .expect("the actor sends this itself, on its way past the closed mailbox");

    assert!(
        system.is_empty(),
        "the id is out of the map while the actor it named is still running"
    );
    let refused = files
        .claim(&held)
        .await
        .expect_err("an evicted actor inside its budget still holds its state");
    assert!(
        matches!(refused, keylink_persist::Error::Busy { .. }),
        "the refusal a re-spawned actor for this id would get, got {refused:?}"
    );
    system
        .mount(id("wedged"), journal::spawn(Journal::default()))
        .expect("and the id itself is free to serve again, which is the whole window");
}

/// A refused mount leaks nothing, on both refusals.
///
/// The second detach, and it was never a budget problem: `Registry::mount` takes a
/// `Mountable` by value and drops it on `Occupied` and on `AtCapacity`, and the
/// task used to be **already spawned** by then — the generated `spawn` started
/// it. `MountError` hands nothing back, so the caller who was refused had no
/// handle, and neither did the registry. Every refused mount was a live actor
/// nobody could reach, holding whatever its state held.
///
/// `Mountable` carries an unstarted future now, so both refusals drop a loop that
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
#[cfg(feature = "persist-file")]
#[tokio::test]
async fn a_refused_mount_leaves_no_actor_holding_the_claim_it_was_built_around() {
    let scratch = Scratch::new("refused-mount");
    let files = Files::open(&scratch.0).await.expect("the store opens");
    let held = StateId::new("refused/4471").expect("a valid state id");

    let system: journal::Registry = journal::Registry::builder(Eviction::Never)
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
/// actors are not.
///
/// # This test used to read a stopwatch, and it flaked three times in one run
///
/// It measured two calls to two actors and asserted the pair took less than one
/// and a half holds, then two calls to one actor and asserted they took at least
/// two. The second is safe — load only makes it truer — and the first is the
/// shape CONTRIBUTING.md forbids by name: *"a load spike is not a bug in your
/// code."* Under a loaded machine, two genuinely concurrent 200 ms turns take
/// longer than 300 ms of wall clock and the assertion fails on a correct tree.
///
/// Both halves are counted overlap now, and both get **stronger** under load
/// rather than weaker. `Inside` records how many handlers were inside a turn at
/// the same moment, and the two halves assert opposite peaks on the same counter:
///
/// - **concurrent**: two actors, each announcing its entry and then waiting on a
///   semaphore nobody releases until *both* announcements have arrived. The peak is
///   **2**. If dispatch were serialised anywhere the second announcement could
///   never come, so the failure mode is the timeout guard rather than a wrong
///   number.
/// - **serial**: a third actor, two messages, the second provably in the *mailbox*
///   before the first is let go — `Registry::status` reporting `busy` with
///   `queued == 1` is what makes "in the mailbox" observable rather than
///   inferred. Both turns run, and the peak is **1**.
///
/// A peak of 1 on a counter just watched to reach 2 is a fact about the loop rather
/// than about the instrument, and that pairing is the whole design here. The first
/// version of this rewrite asserted the serial half with
/// `entered.try_recv().is_err()` instead, and **that assertion was vacuous**:
/// watched under `break-once.sh`, releasing the held turn before the check left the
/// test green, because a scheduler hop is not a turn.
///
/// Made red by serialising dispatch behind one lock, or by handing each message to
/// a fresh task. Watched, by sending the concurrent half's second call to the
/// **first** actor's id: the second entry never arrives, and the timeout guard
/// fails the test at 31 s instead of hanging.
#[tokio::test]
async fn different_actors_run_concurrently_while_one_actor_stays_serial() {
    let fixture = start(mounted()).await;
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    // Nobody is let out of a turn until the test says so, and the test does not say
    // so until it has watched every turn it is waiting for arrive.
    let hold = Arc::new(Semaphore::new(0));
    let together = Arc::new(Inside::default());
    let alone = Arc::new(Inside::default());
    let holder = |inside: &Arc<Inside>| Journal {
        applied: Arc::new(Mutex::new(Vec::new())),
        entered: Some(entered_tx.clone()),
        release: Some(Arc::clone(&hold)),
        inside: Some(Arc::clone(inside)),
        nested: None,
        forward: None,
        cycle: None,
    };
    for name in ["a", "b"] {
        fixture
            .system
            .mount(id(name), journal::spawn(holder(&together)))
            .expect("the id is free");
    }
    fixture
        .system
        .mount(id("c"), journal::spawn(holder(&alone)))
        .expect("the id is free");

    let a = fixture.client.at(id("a"));
    let b = fixture.client.at(id("b"));
    let c = fixture.client.at(id("c"));

    // ── two actors overlap ───────────────────────────────────────────────────
    let concurrent = async {
        let (first, second) = tokio::join!(a.apply(1, 0), b.apply(1, 0));
        first.expect("the first actor answers");
        second.expect("the second actor answers");
    };
    let watching = async {
        for _ in 0..2 {
            // The second of these cannot arrive unless two handlers are inside
            // their turns at once, because the first is still waiting on a permit
            // nobody has added.
            assert_eq!(
                entered.recv().await,
                Some(1),
                "two actors must not queue behind each other"
            );
        }
        assert_eq!(
            together.peak(),
            2,
            "two handlers inside a turn at the same moment, counted"
        );
        hold.add_permits(2);
    };
    // The guard is generous by three orders of magnitude and asserts nothing about
    // speed: it turns a serialised dispatch from a hang into a failure.
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(concurrent, watching)
    })
    .await
    .expect("two actors must not queue behind each other");

    // ── one actor does not ───────────────────────────────────────────────────
    let queued = async {
        assert_eq!(
            entered.recv().await,
            Some(2),
            "the message sent first is the turn that starts"
        );

        let behind = fixture.client.at(id("c"));
        let sending = behind.apply(3, 0);
        let checking = async {
            // Waiting until the message is in the MAILBOX rather than somewhere on
            // a QUIC stream. `queued` is what makes that observable, and at the
            // default depth of one it is exactly this call's message.
            let held = loop {
                let row = status_of(&fixture.system, &id("c"));
                if row.queued == 1 {
                    break row;
                }
                tokio::task::yield_now().await;
            };
            assert!(
                held.busy,
                "the first turn is still inside its handler, so the actor is busy \
                 while its next message waits"
            );
            hold.add_permits(2);
            assert_eq!(
                entered.recv().await,
                Some(3),
                "and the queued message gets its turn once the one ahead of it ends"
            );
        };
        let (answered, ()) = tokio::join!(sending, checking);
        answered.expect("the queued call answers");
    };
    let serial = async {
        c.apply(2, 0).await.expect("the holding call answers");
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(serial, queued)
    })
    .await
    .expect("a queued message must be handled once the turn ahead of it ends");

    assert_eq!(
        alone.peak(),
        1,
        "one actor handles its messages one at a time — on the counter that just \
         reported 2 for two actors, which is what makes this 1 a fact about the \
         loop rather than about the counter"
    );
    // Serial, and therefore in order, with no lock anywhere in the handler.
    assert_eq!(c.entries().await.unwrap(), vec![2, 3]);
    assert_eq!(a.entries().await.unwrap(), vec![1]);
}

/// The one row a status assertion is about, by id.
fn status_of(system: &journal::Registry, id: &ActorId) -> keylink::ActorStatus {
    system
        .status()
        .into_iter()
        .find(|row| &row.id == id)
        .expect("the actor is mounted")
}

// ─────────────────────────────────────────────────────────────── the contract

/// Two services with identical message shapes must not share a keyed ALPN, or a
/// peer built for one completes a handshake with the other and its bytes decode
/// cleanly into the wrong meaning.
///
/// This replaces `the_schema_hash_separates_two_services_with_identical_shapes`,
/// which asserted the same separation one layer down, on the per-actor schema
/// hash that no longer exists. Same claim, moved to where it is now enforced.
///
/// Made red by `scripts/break-once.sh keylink/src/macros.rs`, dropping
/// `"/", stringify!($svc)` from `KEYED_PROTOCOL` — both fragments, because
/// dropping the name on its own leaves a `keylink//keyed` with the separator
/// stranded, and the protocol string is hashed, so that spelling moves the digest
/// as well — and two identically-shaped services then hash the same contract under
/// the same protocol string. Watched: `assertion `left != right` failed`, both
/// sides the bytes of `keylink/keyed/0f98f91088f727db` (an ALPN is a `Vec<u8>`, so
/// the panic prints it as decimal bytes rather than as that string).
#[test]
fn the_keyed_alpn_separates_two_services_with_identical_shapes() {
    assert_ne!(journal::registry_alpn(), vault::registry_alpn());
}

/// A service's two ALPNs are not each other, so one node can serve the same
/// service as a singleton and as an actor system at once.
///
/// Made red by `scripts/break-once.sh keylink/src/macros.rs`, replacing
/// `registry_alpn`'s `derive_alpn::<KeyedContract>(KEYED_PROTOCOL)` with
/// `alpn().to_vec()` — the collision `ConfigError::DuplicateAlpn` exists for.
/// Watched: `assertion `left != right` failed`, both sides
/// `keylink/Journal/c1794491a027bccd`.
#[test]
fn a_services_keyed_alpn_is_not_its_singleton_alpn() {
    assert_ne!(journal::registry_alpn(), journal::alpn());
    assert_ne!(vault::registry_alpn(), vault::alpn());
    assert_ne!(journal::registry_alpn(), vault::alpn());
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
            system
                .service(Allowlist::anyone())
                .layer(PerMethod { seen: seen.clone() })
                .build(),
        )
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = journal::RegistryClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");
    let actor = client.at(id("obra/1"));

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
    let system: doorman::Registry = doorman::Registry::builder(Eviction::Never).build();
    let _running = Server::builder(&server)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = doorman::RegistryClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");

    system
        .mount(id("gate/1"), doorman::spawn(Doorman))
        .expect("the id is free");

    assert_eq!(
        client.at(id("gate/1")).who_called().await.unwrap(),
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
    client: ward::RegistryClient,
    /// A hand-rolled peer on `ward`'s keyed ALPN, whose request type is a bare
    /// `u32`. Nothing generated can produce an undecodable payload.
    raw: RpcClient<u32, ward::Response>,
    _system: ward::Registry,
    _server: Server,
    _nodes: [Node; 2],
}

async fn start_keyed() -> Keyed {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: ward::Registry = ward::Registry::builder(Eviction::Never).build();
    let running = Server::builder(&server)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = ward::RegistryClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the typed client dials the actor system");
    let raw = RpcClient::connect(
        &client_node,
        server.endpoint().addr(),
        ward::registry_alpn(),
    )
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
    fixture
        .client
        .at(id("ward"))
        .touches()
        .await
        .expect("the actor answers")
}

/// A keyed handle to `ward`, under `key`.
fn under(fixture: &Keyed, key: &str) -> ward::RegistryClient {
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

    let first = under(&fixture, "touch")
        .at(id("ward"))
        .touch()
        .await
        .expect("the first call runs");
    assert_eq!(first, 1);

    let again = under(&fixture, "touch")
        .at(id("ward"))
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

    under(&fixture, "scoped")
        .at(id("ward"))
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
    let system: doorman::Registry = doorman::Registry::builder(Eviction::Never).build();
    let _running = Server::builder(&server)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let elsewhere = doorman::RegistryClient::connect(&caller, server.endpoint().addr())
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

    under(&fixture, "touch")
        .at(id("ward"))
        .unsendable()
        .await
        .expect_err("the reply does not encode");
    assert_eq!(
        touches(&fixture).await,
        1,
        "the turn ran, which is what makes the key owe something"
    );

    let retried = under(&fixture, "touch")
        .at(id("ward"))
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

    under(&fixture, "touch")
        .at(id("ward"))
        .detonate()
        .await
        .expect_err("the reply's `Serialize` panics");
    assert_eq!(
        touches(&fixture).await,
        1,
        "the turn ran, which is what makes the key owe something"
    );

    let retried = under(&fixture, "touch")
        .at(id("ward"))
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

    let error = under(&fixture, "touch")
        .at(id("ward"))
        .explode()
        .await
        .expect_err("the turn panics");
    assert!(
        matches!(
            error,
            Error::Remote(RemoteError::Withheld(Withheld::Panicked { ref message }))
                if message.contains("this turn dies")
        ),
        "got {error:?}"
    );
    assert_eq!(
        touches(&fixture).await,
        1,
        "the turn ran, and mutated, before it panicked"
    );

    let retried = under(&fixture, "touch")
        .at(id("ward"))
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
        fixture
            .client
            .at(id("ward"))
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
    keyed
        .at(id("ward"))
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
        fixture
            .client
            .at(id("ward"))
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
    /// Queue one turn without waiting for a reply.
    tell Queue() -> ();


    /// The count, read without a key, so reading it never settles one.
    rpc Touches() -> u64;

    /// The actor's second input, so `busy` can be watched over a turn no mailbox
    /// produced. Wave 3 made a feed item a turn; `ActorStatus::busy` has to agree,
    /// and a flag maintained inside the mailbox's own receive would not.
    feed Ticks(tick: u64);
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
    /// Every turn this actor ran, in order: `Some(tick)` for a feed item and
    /// `None` for a mailbox message. What a fairness assertion reads.
    turns: mpsc::UnboundedSender<Option<u64>>,
    /// Held before a feed turn returns, exactly as `release` is for `hold`, so a
    /// test can look at an actor that is provably inside a FEED turn.
    ticked: Arc<Semaphore>,
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
        let _ = self.turns.send(None);
        self.touches
    }

    async fn queue(&mut self, _cx: keylink::Caller<'_>) {
        self.touches += 1;
        let _ = self.turns.send(None);
    }

    async fn touches(&mut self, _cx: keylink::Caller<'_>) -> u64 {
        self.touches
    }

    /// The feed's handler, held on its own semaphore for `hold`'s reason: a test
    /// that wants to look at an actor inside a feed turn needs that turn to still
    /// be going on when it looks.
    async fn on_ticks(&mut self, tick: u64) {
        let _ = self.turns.send(Some(tick));
        self.ticked
            .acquire()
            .await
            .expect("the test never closes the semaphore")
            .forget();
    }
}

#[tokio::test]
async fn a_keyed_shed_refuses_full_mailboxes_locally_and_remotely() {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let system: gate::Registry = gate::Registry::builder(Eviction::Never).build();
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let (turns_tx, _turns) = mpsc::unbounded_channel();
    let (_ticks, tick_rx) = mpsc::channel(1);
    let release = Arc::new(Semaphore::new(0));
    let actor_id = id("shed");

    system
        .mount(
            actor_id.clone(),
            gate::setup(Gate {
                entered: entered_tx,
                release: Arc::clone(&release),
                touches: 0,
                turns: turns_tx,
                ticked: Arc::new(Semaphore::new(0)),
            })
            .feeds(gate::Feeds::new(ticking(tick_rx)))
            .backpressure(Backpressure::Shed(NonZeroUsize::MIN))
            .mountable(),
        )
        .expect("mounts");
    let _server = system
        .serve(&server_node, Allowlist::anyone())
        .expect("serves");
    let client = gate::RegistryClient::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("dials");

    let holding = {
        let client = client.clone();
        let actor_id = actor_id.clone();
        tokio::spawn(async move { client.at(actor_id).hold().await })
    };
    entered.recv().await.expect("the held turn starts");

    let _ = gate::at_local(&system, actor_id.clone())
        .queue()
        .await
        .expect("the one mailbox slot is now occupied");

    assert_eq!(
        gate::at_local(&system, actor_id.clone())
            .touch()
            .await
            .expect_err("local keyed delivery sheds"),
        keylink::LocalError::MailboxFull,
    );
    assert!(matches!(
        client.at(actor_id).touch().await,
        Err(keylink::Error::Remote(RemoteError::MailboxFull)),
    ));

    release.add_permits(1);
    holding
        .await
        .expect("the caller task survives")
        .expect("the held call answers");
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
    client: gate::RegistryClient,
    /// Fires when a turn starts.
    entered: mpsc::UnboundedReceiver<()>,
    /// Fires when a call passes the `QueueTimeout`, carrying its budget.
    stamped: mpsc::UnboundedReceiver<Duration>,
    /// Lets `hold` return.
    release: Arc<Semaphore>,
    /// The near end of the `Ticks` feed. Held for the fixture's whole life, and
    /// that is load-bearing rather than tidy: a feed that ENDS ends the actor.
    ticks: mpsc::Sender<u64>,
    /// Lets a feed turn return.
    ticked: Arc<Semaphore>,
    /// Every turn the actor ran, in order. `Some(tick)` is a feed turn.
    turns: mpsc::UnboundedReceiver<Option<u64>>,
    /// Read by the `status` tests, which is why it is not `_system`.
    system: gate::Registry,
    _server: Server,
    _nodes: [Node; 2],
}

async fn start_gated() -> Gated {
    gated_behind(QueueTimeout::try_after(BUDGET).expect("valid queue timeout budget")).await
}

/// The depth `Gated` is built at unless a test says otherwise: the default, so
/// every test that predates the knob still runs at one slot.
const ONE_SLOT: usize = 1;

/// The same, behind whichever bound is under test.
///
/// `Timeout` and `QueueTimeout` answer the same caller in the same scenario and
/// owe the caller's idempotency key opposite things, so the fixture they are
/// compared through has to be one fixture. `Stamped` stays inside either bound
/// and simply reports nothing under `Timeout`, which stamps no deadline — the
/// tests that read `stamped` are the ones that supply a `QueueTimeout`.
async fn gated_behind(bound: impl Middleware<GateWire, gate::Response>) -> Gated {
    gated_at_depth(bound, ONE_SLOT).await
}

/// The same, at a stated mailbox depth.
///
/// Only the saturation test needs more than one slot, and it needs the rest of
/// this fixture unchanged — two real nodes, the same service, the same bound — so
/// the depth is a parameter here rather than a second fixture drifting beside it.
async fn gated_at_depth(bound: impl Middleware<GateWire, gate::Response>, depth: usize) -> Gated {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: gate::Registry = gate::Registry::builder(Eviction::Never)
        .mailbox_depth(NonZeroUsize::new(depth).expect("a mailbox depth is never zero"))
        .build();
    let (entered_tx, entered) = mpsc::unbounded_channel();
    let (stamped_tx, stamped) = mpsc::unbounded_channel();
    let (turns_tx, turns) = mpsc::unbounded_channel();
    let (ticks, tick_rx) = mpsc::channel(16);
    let release = Arc::new(Semaphore::new(0));
    let ticked = Arc::new(Semaphore::new(0));
    let running = Server::builder(&server)
        .service(
            system
                .service(Allowlist::anyone())
                // `registry.service` gives back an ordinary `ServiceBuilder`, so
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
    let client = gate::RegistryClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");
    system
        .mount(
            id("gate"),
            // `setup(..).feeds(..).mountable()` and not `spawn`: a service that declares a feed has to
            // be handed one, which is the missing-argument the generated
            // `ServableAsActor` check exists to produce.
            gate::setup(Gate {
                entered: entered_tx,
                release: Arc::clone(&release),
                touches: 0,
                turns: turns_tx,
                ticked: Arc::clone(&ticked),
            })
            .feeds(gate::Feeds::new(ticking(tick_rx)))
            .mountable(),
        )
        .expect("the id is free");

    Gated {
        client,
        entered,
        stamped,
        release,
        ticks,
        ticked,
        turns,
        system,
        _server: running,
        _nodes: [server, client_node],
    }
}

/// A feed backed by a channel, so a test decides exactly what is queued and when.
///
/// `poll_fn` over `Receiver::poll_recv`, which is `tests/feeds.rs`'s helper — a
/// plain `Stream` is all a feed wants, and nothing here names a keylink type.
fn ticking(mut items: mpsc::Receiver<u64>) -> impl keylink::Stream<Item = u64> + Send + 'static {
    n0_future::stream::poll_fn(move |cx| items.poll_recv(cx))
}

/// The one row the `gate` fixture's status assertions are about.
///
/// Takes the system rather than the fixture so a test can hold this borrow beside
/// a `&mut` on one of the fixture's channels, which every test below needs.
fn gate_status(system: &gate::Registry) -> keylink::ActorStatus {
    let mut rows = system.status();
    assert_eq!(rows.len(), 1, "the gate fixture mounts exactly one actor");
    rows.remove(0)
}

/// Wait until no turn is running, and hand back that reading.
///
/// Bounded, so a `busy` that never clears fails the test instead of hanging it.
/// Nothing here asserts how long anything took: the guard is three orders of
/// magnitude out, and its expiry means the flag is stuck rather than slow.
async fn until_idle(system: &gate::Registry) -> keylink::ActorStatus {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let row = gate_status(system);
            if !row.busy {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a turn that has answered its caller must clear `busy`")
}

/// Wait until the mailbox holds `messages`, and hand back that reading.
///
/// The deterministic way to know a call is in the MAILBOX rather than somewhere on
/// a QUIC stream, which is what every saturation assertion below rests on. Bounded
/// for `until_idle`'s reason.
async fn until_queued(system: &gate::Registry, messages: u64) -> keylink::ActorStatus {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let row = gate_status(system);
            if row.queued == messages {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{messages} messages must reach the mailbox"))
}

// ───────────────────────────────────────────────────────── what is running now

/// **`busy` is true during a turn and false between turns**, over two real nodes.
///
/// The signal this asserts did not exist before item 4d and could not be faked from
/// what did: `last_seen` is stamped when a message is *routed*, and a mailbox's
/// `capacity` is restored the moment the loop *receives* — before the handler is
/// entered. Both say "busy" about an actor that has not started, and neither says
/// anything at all about a turn that is still going.
///
/// Over the wire on purpose. The flag is set in `turns.rs`'s one loop, and the keyed
/// path reaches that loop through generated code and a mailbox the registry made, so
/// an in-process assertion would leave the delivery path untested.
///
/// Made red by moving the flag into `Mailbox::recv` — which is where the design that
/// asked for this put it — or by clearing it before the handler returns.
#[tokio::test]
async fn busy_is_true_during_a_turn_and_false_between_turns() {
    let mut fixture = start_gated().await;
    let client = &fixture.client;
    let release = &fixture.release;
    let system = &fixture.system;
    let entered = &mut fixture.entered;

    let before = gate_status(system);
    assert!(!before.busy, "nothing has been called yet");
    assert_eq!(before.queued, 0);

    let holding = async { client.at(id("gate")).hold().await };
    let watching = async {
        entered.recv().await.expect("`hold` enters its turn");
        let during = gate_status(system);
        assert!(
            during.busy,
            "the handler is inside `hold`, so a turn is running"
        );
        assert_eq!(
            during.queued, 0,
            "and the mailbox is empty: this message is IN the turn rather than \
             waiting for one, which is the distinction `capacity` alone cannot make"
        );
        release.add_permits(1);
    };
    let (answered, ()) = tokio::join!(holding, watching);
    answered.expect("the held call answers");

    let after = until_idle(system).await;
    assert_eq!(after.queued, 0, "and nothing is left waiting");
}

/// **`busy` stays true across an `await` inside a turn.**
///
/// The claim that makes the flag worth having: a turn parked on I/O is a turn that
/// is running. `Gate::hold` is parked on a semaphore for the whole of this test, so
/// every reading below is taken while the handler is suspended at an `await` and has
/// executed nothing since.
///
/// The reading is taken twice, and the second time with a message provably in the
/// mailbox — which is what makes this more than "the flag we set is still set". A
/// loop that had finished the turn would have taken that message: at the default
/// depth of one, `queued == 1` and `busy` together say the loop is neither at its
/// receive nor between turns.
#[tokio::test]
async fn busy_stays_true_across_an_await_inside_a_turn() {
    // Not `start_gated`: `touch` waits behind `hold` for as long as `watching`
    // takes, and under a load average of about 110 that passed the fixture's
    // 200 ms queue budget, so the queued call answered `TimedOut`. This test is
    // about `busy`, not about the budget, so its budget is one load cannot spend.
    let mut fixture = gated_behind(
        QueueTimeout::try_after(Duration::from_secs(600)).expect("valid queue timeout budget"),
    )
    .await;
    let client = &fixture.client;
    let release = &fixture.release;
    let system = &fixture.system;
    let entered = &mut fixture.entered;

    let holding = async { client.at(id("gate")).hold().await };
    let queued = async { client.at(id("gate")).touch().await };
    let watching = async {
        entered.recv().await.expect("`hold` enters its turn");
        // Many chances for the actor's task to be polled again, none of which the
        // handler can use: it is waiting on a permit nobody has added.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            gate_status(system).busy,
            "a turn suspended at an await is still a turn"
        );

        let held = until_queued(system, 1).await;
        assert!(
            held.busy,
            "and it is still running with a message waiting behind it, which a \
             loop between turns would already have taken"
        );

        release.add_permits(1);
    };

    let (first, second, ()) = tokio::join!(holding, queued, watching);
    first.expect("the held call answers");
    assert_eq!(second.expect("the queued call answers"), 1);
    until_idle(system).await;
}

/// **`busy` is true during a feed turn as well as a mailbox turn.**
///
/// This is the criterion that decided where the flag lives. The design this
/// implements put it inside `Mailbox::recv` — cleared on the way in, set on the way
/// out — and a feed item never touches a mailbox, so that flag reads `false` through
/// every feed turn. After Wave 3 it is worse than wrong: the receive is a
/// synchronous poll the loop skips entirely on any iteration where the feed was
/// ready first, so the flag would be *stale* rather than merely unset.
///
/// So it is maintained in `run_loop`, around the one dispatch both inputs go
/// through, and this test is the difference between the two placements.
#[tokio::test]
async fn busy_is_true_during_a_feed_turn() {
    let mut fixture = start_gated().await;
    let system = &fixture.system;

    fixture.ticks.send(7).await.expect("the feed accepts");
    assert_eq!(
        fixture.turns.recv().await,
        Some(Some(7)),
        "the feed item reaches the handler"
    );

    let during = gate_status(system);
    assert!(during.busy, "a feed item is a turn, and it is running now");
    assert_eq!(
        during.queued, 0,
        "with no mailbox message involved anywhere: a feed does not consume a slot"
    );

    fixture.ticked.add_permits(1);
    until_idle(&fixture.system).await;
}

// ────────────────────────────────────── what a PEER may learn about that turn

/// How long a call that should answer at once is given before it is called stuck.
///
/// Three orders of magnitude out, and nothing here asserts elapsed time: its
/// expiry means a `status` is waiting on something, which is the whole defect
/// this section exists to catch. A bare `await` would express the same guard by
/// hanging, and a guard that fails by hanging says nothing about what happened.
const NOT_STUCK: Duration = Duration::from_secs(30);

/// **A wedged actor answers `status` to a peer, during the turn that wedged
/// it.**
///
/// The wave's central claim, on the keyed door — its singleton twin is
/// `a_wedged_singleton_answers_status_to_its_handle_and_to_a_peer`, and between
/// them they are the only tests in the tree that can fail if `status` ever
/// becomes a message. `macros.rs` states the constraint on
/// `Handle::busy`: this is the question you cannot ask the actor, because a
/// message asking it queues behind the turn you are asking about. So the answer
/// is produced inside the `Dispatch` closure, above `deliver_keyed`, and
/// nothing enters the mailbox.
///
/// **The ordering is the proof, and it is not a clock.** `hold`'s handler is
/// parked on a semaphore with no permits, so its turn cannot end until this test
/// adds one — and the permit is added strictly *after* the peer's `status` has
/// answered. There is no arrangement of events in which that answer was produced
/// after the turn finished.
///
/// **The mailbox is FULL while the question is asked**, which is the second half
/// and the reason for the queued `touch`: at depth one, `queued == 1` means
/// there is no slot left. A `status` delivered as a message could not even be
/// accepted, let alone answered — and `queued` reading `1` rather than `2` is
/// the same fact from the other side, since a status that took a slot would count
/// itself.
///
/// A whole-call `Timeout` rather than the fixture's default `QueueTimeout`,
/// following `a_saturated_mailbox_at_depth_does_not_starve_a_feed`: a message is
/// meant to sit in this mailbox for as long as the test needs, and a queue budget
/// would refuse it for doing exactly that. Nothing here is timed.
///
/// Made red by `scripts/break-once.sh keylink/src/macros.rs`, replacing the keyed
/// closure's interception test —
/// `let __answer = if ::core::matches!(
/// request.message(),
/// Request::__Status { .. }
/// ) {` — with `let __answer = if false {`, so `__Status` falls through to
/// `deliver_keyed` like any other message. Watched:
/// `a `status` that enqueued would wait behind the wedged turn, and this one
/// did: Elapsed(())`. Registered as `S1`.
#[tokio::test]
async fn a_wedged_actor_answers_status_during_its_turn() {
    let mut fixture = gated_behind(Timeout::after(Duration::from_secs(60))).await;
    let client = &fixture.client;
    let release = &fixture.release;
    let system = &fixture.system;
    let entered = &mut fixture.entered;

    let holding = async { client.at(id("gate")).hold().await };
    let queued = async { client.at(id("gate")).touch().await };
    let asking = async {
        entered.recv().await.expect("`hold` enters its turn");
        // The mailbox is full before the question is asked, so the answer below
        // cannot have come through it.
        let held = until_queued(system, 1).await;
        assert!(held.busy, "the turn holding the actor is still running");

        let serving = tokio::time::timeout(NOT_STUCK, client.at(id("gate")).status())
            .await
            .expect("a `status` that enqueued would wait behind the wedged turn, and this one did")
            .expect("a wedged actor still answers the question it cannot be asked");

        assert!(
            serving.busy,
            "the actor is inside `hold` and the answer says so"
        );
        assert_eq!(
            serving.queued, 1,
            "the queued `touch` and nothing else: a `status` that took a mailbox \
             slot would have counted itself here"
        );

        // Only now. Every line above ran while the turn was parked on this
        // semaphore, which is what "during its turn" means.
        release.add_permits(1);
        serving
    };

    let (answered, touched, serving) = tokio::join!(holding, queued, asking);
    answered.expect("the held call answers");
    touched.expect("the queued call answers");

    // And the reading was about the actor rather than about the asking: once the
    // turn is over the same question answers the opposite way.
    until_idle(system).await;
    let after = client
        .at(id("gate"))
        .status()
        .await
        .expect("an idle actor answers too");
    assert_eq!(
        after,
        Serving {
            busy: false,
            queued: 0
        },
        "a `status` reporting the state of asking would read `busy: true` here \
         as well, because it would itself be the turn"
    );
    assert_ne!(
        serving, after,
        "the two readings are of one actor in two states, so a constant answer \
         passes neither"
    );
}

/// **A local caller and a peer read the same `Serving` for one keyed actor.**
///
/// Two doors, one value, asserted by equality rather than field by field. The
/// callers are named because §5 of the plan names the wrong one: on the keyed
/// path the local door is **`Registry::serving`** and not a `Handle` — a keyed
/// actor has no handle, the registry owns its mailbox, and `Registry::serving` is
/// the reader `Registry::status` shares its body with. The peer's door is
/// **`gate::at(..).status()`**, over a real QUIC connection to another node.
///
/// Asserted in **both** states, because a constant satisfies one of them. Idle
/// is the cheap half; wedged is the half that matters, and it is deterministic
/// for `a_wedged_actor_answers_status_during_its_turn`'s reason — the handler is
/// parked on a semaphore, so `busy` cannot change under either reading.
///
/// **Keyed on both sides on purpose.** A keyed mailbox carries only the service's
/// own messages while a `LocalActor`'s channel also carries a `read` and the stop
/// request, so `queued` compares as a predicate across the two kinds and never
/// as a count. Comparing a keyed `Serving` against a singleton one by value would
/// assert something this crate does not claim.
///
/// Made red by `scripts/break-once.sh keylink/src/actor/remote.rs`, replacing
/// `serving_keyed`'s `.serving(actor)` with
/// `.serving(actor).map(|_| Serving { busy: false, queued: 0 })`, so the peer's
/// answer stops coming from the registry. Watched:
/// `assertion `left == right` failed: one actor, one instant, two doors`,
/// `left: Serving { busy: true, queued: 0 }` and
/// `right: Serving { busy: false, queued: 0 }`. Registered as `S2`.
#[tokio::test]
async fn a_local_caller_and_a_peer_read_the_same_serving() {
    let mut fixture = start_gated().await;
    let client = &fixture.client;
    let release = &fixture.release;
    let system = &fixture.system;
    let entered = &mut fixture.entered;
    let who = id("gate");

    let idle_here = system.serving(&who).expect("the actor is mounted");
    let idle_there = client
        .at(who.clone())
        .status()
        .await
        .expect("the peer's question is answered");
    assert_eq!(
        idle_here, idle_there,
        "one actor, one instant, two doors — at rest"
    );
    assert_eq!(
        idle_here,
        Serving {
            busy: false,
            queued: 0
        },
        "and nothing has been called yet"
    );

    let holding = async { client.at(who.clone()).hold().await };
    let comparing = async {
        entered.recv().await.expect("`hold` enters its turn");
        let there = tokio::time::timeout(NOT_STUCK, client.at(who.clone()).status())
            .await
            .expect("a wedged actor answers its peer")
            .expect("the peer's question is answered");
        // Read second and while the same turn is still parked, so the two
        // readings are of one state rather than of two.
        let here = system.serving(&who).expect("the actor is still mounted");
        release.add_permits(1);
        (here, there)
    };

    let (answered, (here, there)) = tokio::join!(holding, comparing);
    answered.expect("the held call answers");

    assert_eq!(here, there, "one actor, one instant, two doors");
    assert!(
        here.busy,
        "and the state they agree on is the one the test arranged"
    );
}

/// **A wedged singleton answers `status` to its own `Handle` and to a peer,
/// with the same value.**
///
/// The second of the two doors that answer with a `Serving`: `registry.service`'s
/// closure answers above `deliver_keyed` and this one answers above
/// `deliver_unkeyed`. A test of one says nothing about the other, and the
/// arms are separate code. The concurrent `service<H: Handler>` door answers
/// `__Status` inside its `Dispatch` too and is the third such closure — it
/// refuses with `NotAnActor` rather than folding a `Serving`, and `S4` guards it.
/// This is also the only place `Own::status` — the synchronous local door,
/// which sends nothing at all — is read.
///
/// The same `Gate` state and the same semaphore as the keyed tests, served the
/// other way: `setup(..).feeds(..).actor()` rather than `setup(..).feeds(..).mountable()`, `service` rather than
/// `registry.service`, `Client` rather than `RegistryClient`. So what differs between
/// this test and its keyed twin is the door and nothing else.
///
/// **No value is written down for `queued`.** The two sides are compared
/// against each other, never against a literal: a `LocalActor`'s channel carries
/// more than the service's own messages, and pinning a number here would assert
/// something `Serving` says it does not claim.
///
/// **Read in two states, because one state is passed by a constant.** Equality
/// between the doors plus `busy` at the wedged instant is satisfied by a
/// `Serving { busy: true, queued: 0 }` hard-coded into `serving_unkeyed`
/// — measured, not assumed, and that is the true reading of those two assertions
/// on their own. The idle pair taken after the permit closes it: the peer's
/// wedged answer and the peer's idle answer have to differ, and no constant can
/// do that. Both of the compared readings are the **peer's**, because the local
/// door folds `LocalActor::serving` directly and never reaches the body a
/// constant would replace. `a_wedged_actor_answers_status_during_its_turn` has
/// carried this guard since it was written; this test did not.
///
/// Made red by `scripts/break-once.sh keylink/src/macros.rs`, replacing
/// `if ::core::matches!(&req, Request::__Status { .. }) {` with `if false {`, so
/// the singleton door delivers the question instead of answering it. Watched:
/// `a `status` that enqueued would wait behind the wedged turn, and this one
/// did: Elapsed(())`. Registered as `S3`.
#[tokio::test]
async fn a_wedged_singleton_answers_status_to_its_handle_and_to_a_peer() {
    let node = local_node().await;
    let caller = local_node().await;
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let (turns_tx, _turns) = mpsc::unbounded_channel();
    let (ticks, tick_rx) = mpsc::channel(16);
    let release = Arc::new(Semaphore::new(0));
    let ticked = Arc::new(Semaphore::new(0));

    let actor = gate::setup(Gate {
        entered: entered_tx,
        release: Arc::clone(&release),
        touches: 0,
        turns: turns_tx,
        ticked,
    })
    .feeds(gate::Feeds::new(ticking(tick_rx)))
    .actor();
    let server = Server::builder(&node)
        .service(actor.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = gate::Client::connect(&caller, node.endpoint().addr())
        .await
        .expect("the client dials the singleton");

    let holding = async { client.hold().await };
    let comparing = async {
        entered.recv().await.expect("`hold` enters its turn");
        let there = tokio::time::timeout(NOT_STUCK, client.status())
            .await
            .expect("a `status` that enqueued would wait behind the wedged turn, and this one did")
            .expect("a wedged singleton still answers");
        // Synchronous, and that is the point of it: it reads a flag beside the
        // mailbox and sends nothing, so it answers while the turn holds the actor.
        let here = actor.status();
        release.add_permits(1);
        (here, there)
    };

    let (answered, (here, there)) = tokio::join!(holding, comparing);
    answered.expect("the held call answers");

    assert_eq!(here, there, "one actor, one instant, two doors");
    assert!(
        here.busy,
        "the actor is inside `hold`, and both doors say so"
    );

    // And the reading was about the actor rather than about the asking: once the
    // turn is over, the same two doors answer the opposite way.
    //
    // Waited out on the LOCAL door deliberately. `serving_unkeyed` is the
    // body a constant answer would be substituted into, so a wait that polled the
    // peer would be satisfied by the very value under test — and a substituted
    // `busy: true` would hang here instead of failing an assertion.
    // `LocalActor::serving` is a different producer and settles on its own.
    let after_here = tokio::time::timeout(NOT_STUCK, async {
        loop {
            let row = actor.status();
            if !row.busy {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a turn that has answered its caller must clear `busy`");
    let after_there = client
        .status()
        .await
        .expect("an idle singleton answers its peer too");

    // Both readings are the PEER's, which is what makes this bite: `here` and
    // `after_here` come from `LocalActor::serving`, and comparing those two would
    // leave the peer's producer free to answer with a constant.
    assert_ne!(
        there, after_there,
        "two peer readings of one actor in two states, so a constant answer \
         passes neither"
    );
    assert_eq!(
        after_here, after_there,
        "one actor, one instant, two doors — at rest"
    );

    // The feed's sender outlives the actor on purpose: dropping it ends the actor,
    // and this test is about one that is running.
    drop(ticks);
    server.shutdown().await.expect("stops");
}

/// **A saturated mailbox at depth > 1 still does not starve a feed.**
///
/// `tests/feeds.rs` pins the fairness rule at the default depth and says what it
/// cannot reach: *"at the default local mailbox depth of 1, on a single-threaded
/// test runtime,
/// the loop
/// takes the slot's message, runs the turn and polls again without yielding, so the
/// parked sender has not been rescheduled and the mailbox is deterministically
/// empty at the next poll."* A depth of three removes that limit — three messages
/// are genuinely in the mailbox, and `queued` says so before anything is released
/// — so this is the saturated case that could not be built before item 4a.
///
/// The order is exact rather than statistical, because everything is loaded while
/// the loop is held: `hold` is turn one and is a mailbox turn, so the loop's
/// alternation gives the **feed** first refusal on turn two. A mailbox-first loop
/// puts the tick last instead.
///
/// Red-tested by deleting the loop's alternation — `mail_first = true` in
/// `keylink/src/turns.rs` — which is `tests/feeds.rs`'s break for the same rule.
/// Watched: the tick arrives fourth.
#[tokio::test]
async fn a_saturated_mailbox_at_depth_does_not_starve_a_feed() {
    const SLOTS: usize = 3;
    /// The same three, as `ActorStatus::queued` reports them. `queued` is a
    /// `u64` and a mailbox depth is a `usize`, because only one of the two
    /// crosses a wire — see `ActorStatus::queued`.
    const SLOTS_SENT: u64 = 3;

    // A whole-call `Timeout` and not the fixture's default `QueueTimeout`: three
    // messages are meant to sit in this mailbox for as long as the test needs, and
    // a queue budget would refuse them for doing it.
    let mut fixture = gated_at_depth(Timeout::after(Duration::from_secs(60)), SLOTS).await;
    let client = &fixture.client;
    let release = &fixture.release;
    let ticks = &fixture.ticks;
    let ticked = &fixture.ticked;
    let system = &fixture.system;
    let entered = &mut fixture.entered;
    let turns = &mut fixture.turns;

    let holding = async { client.at(id("gate")).hold().await };
    let saturating = async {
        // Bound first: `gate::at` returns a borrowing handle, so the three
        // futures have to outlive the temporaries they are built from.
        let (first, second, third) = (
            client.at(id("gate")),
            client.at(id("gate")),
            client.at(id("gate")),
        );
        let (one, two, three) = tokio::join!(first.touch(), second.touch(), third.touch());
        one.expect("answers");
        two.expect("answers");
        three.expect("answers");
    };
    let watching = async {
        entered.recv().await.expect("`hold` enters its turn");

        let full = until_queued(system, SLOTS_SENT).await;
        assert!(
            full.busy,
            "the actor is inside `hold` while its mailbox fills"
        );
        assert_eq!(
            full.queued, SLOTS_SENT,
            "and the mailbox is FULL: three messages in three slots, which is the \
             state a depth of one cannot hold still"
        );

        // Queued last and served first. The feed's item is not in the mailbox and
        // consumed no slot to get here.
        ticks.send(7).await.expect("the feed accepts");
        ticked.add_permits(1);
        release.add_permits(1);

        let mut order = Vec::new();
        for _ in 0..4 {
            order.push(turns.recv().await.expect("four turns run"));
        }
        order
    };

    let (held, (), order) = tokio::join!(holding, saturating, watching);
    held.expect("the held call answers");

    assert_eq!(
        order[0],
        Some(7),
        "the feed item must be taken BEFORE the three messages already queued: a \
         saturated mailbox does not starve a feed. Order was {order:?}"
    );
    assert_eq!(
        &order[1..],
        &[None, None, None],
        "and the three mailbox turns follow it"
    );
}

/// A keyed handle to `gate`, under `key`.
fn gate_under(fixture: &Gated, key: &str) -> gate::RegistryClient {
    fixture
        .client
        .with_idempotency(IdempotencyKey::new(key).expect("a valid key"))
}

async fn gate_touches(fixture: &Gated) -> u64 {
    fixture
        .client
        .at(id("gate"))
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

    let holding = async { client.at(id("gate")).hold().await };
    let queued = async {
        wait.await.expect("the driver says when");
        keyed.at(id("gate")).touch().await
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
    let ran = gate_under(&fixture, "retried")
        .at(id("gate"))
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

    let holding = async { client.at(id("gate")).hold().await };
    let sending = async {
        wait.await.expect("the driver says when");
        let answer = || async {
            let answered = client.at(target.clone()).touch().await;
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
/// watched was `Registry::deliver`'s hash comparison sitting **before** the send.
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
            matches!(error, Error::Remote(RemoteError::BadActorId { ref reason, .. }) if reason.contains("actor id")),
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

/// `deliver_unkeyed` bounds the wait for a mailbox permit, so a message that
/// cannot get a slot is refused at its budget and — tokio not queueing a send that
/// was cancelled before its permit arrived — provably never queued. The keyed walk
/// awaited `mail.send` with no bound at all.
///
/// The bound used to live in `LocalActor::ask_catching_before`, which this comment
/// named until Wave 1b deleted it along with `Queued<T>`. Its one caller was
/// `Handle::service`'s dispatch, which is `deliver_unkeyed` now — the twin of
/// `deliver_keyed`, in keylink, so the two halves of this test's subject are one
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
/// Made red by dropping the bound in `deliver_keyed` back to a bare
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

    let holding = async { client.at(id("gate")).hold().await };
    let queued = async {
        wait.await.expect("the driver says when");
        let answered = keyed.at(id("gate")).touch().await;
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
/// Made red by removing the `enqueued()` mark from `deliver_keyed`, which
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

    let retried = gate_under(&fixture, "gave-up")
        .at(id("gate"))
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

    let holding = async { client.at(id("gate")).hold().await };
    let sending = async {
        wait.await.expect("the driver says when");
        let one = first.at(id("gate"));
        let two = second.at(id("gate"));
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
/// the way *into* `deliver_keyed` would be indistinguishable on every other
/// assertion in this file and would burn that second key.
///
/// Made red by moving `progress.enqueued()` above `mail.send` in
/// `deliver_keyed`, which settles both keys and leaves the count at two.
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
        match gate_under(&fixture, key).at(id("gate")).touch().await {
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
    let system: fuse::Registry = fuse::Registry::builder(Eviction::Never).build();
    let _running = Server::builder(&server)
        .service(system.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = fuse::RegistryClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");

    system
        .mount(id("fuse"), fuse::spawn(Fuse))
        .expect("the id is free");
    assert_eq!(
        client.at(id("fuse")).ping().await.unwrap(),
        1,
        "the actor answers before the message that used to kill it"
    );

    // The turn itself is fine: it returns `Exploding`, and `guarded` sees no
    // panic at all. The unwind happens while the frame is being encoded, on the
    // connection's task.
    client
        .at(id("fuse"))
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
        client.at(id("fuse")).ping().await.unwrap(),
        1,
        "and the next caller is answered, which is the whole point of the encode having \
         left the actor's task"
    );
}
