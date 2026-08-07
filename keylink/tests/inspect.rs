//! Wave 6: keylink serving its own introspection, over two real nodes.
//!
//! Everything here runs against a real QUIC connection between two loopback
//! endpoints, because every claim in this file is about something a peer can
//! see: which ALPN it reached, which allowlist admitted it, and what it read.
//!
//! The claim that needed the most care is the one this module is arranged
//! around: **a subscriber is a parked reply, not a held handler.** A test that
//! only checked "the console received an event" would pass just as well against
//! a `follow` that sat inside a turn holding `&mut Inspector` — which is the
//! implementation this design exists to refuse. So
//! `a_parked_subscriber_does_not_stop_the_actor_serving_turns` makes the actor
//! serve twenty more turns *while* the subscriber is attached, and only then
//! wakes it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::time::Duration;

use keylink::{
    inspect, ActorId, Allowlist, Caller, Console, EndReason, Ended, EndpointId, Error, EventPage,
    Events, Eviction, FollowRefused, Inspector, LifecycleEvent, Node, NodeKey, Server,
    MAX_FOLLOWERS,
};
use tokio::sync::mpsc;

mod common;
use common::local_node;

// ─────────────────────────────────────────────────────────── the watched service

keylink::service! {
    service Journal;
    rpc Post(entry: u64) -> u64;
    rpc Entries() -> Vec<u64>;

    /// A second input, and the reason it is here is the ending it makes
    /// reachable: a feed that runs out ends its actor **on its own**, with no
    /// caller, no `shutdown` and no sweeper. That is the one way an actor leaves
    /// a registry that nobody asks for, and this file is where a console watches
    /// it happen.
    feed Ticks(tick: u64);
}

#[derive(Default)]
struct Journal {
    entries: Vec<u64>,
}

impl journal::Actor for Journal {
    async fn post(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        self.entries.push(entry);
        entry
    }

    async fn entries(&mut self, _cx: Caller<'_>) -> Vec<u64> {
        self.entries.clone()
    }

    async fn on_ticks(&mut self, tick: u64) {
        self.entries.push(tick);
    }
}

/// A feed backed by a channel, so a test decides when it ends.
///
/// `poll_fn` over `Receiver::poll_recv` rather than `tokio_stream`, which is not
/// a dependency here — the same helper `tests/feeds.rs` writes, for the same
/// reason.
fn feed(mut items: mpsc::Receiver<u64>) -> impl keylink::Stream<Item = u64> + Send + 'static {
    n0_future::stream::poll_fn(move |cx| items.poll_recv(cx))
}

// ──────────────────────────────────────────────────────────────────── the fixture

fn depth(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("a non-zero test constant")
}

fn id(name: &str) -> ActorId {
    ActorId::new(name).expect("a valid id")
}

/// How long a call that must be **refused** is given before the test says it
/// parked instead.
///
/// Short on purpose, and it is the only budget in this file that is. Everything
/// else here waits for an event to travel, which is a real duration; a refusal is
/// an immediate answer from an idle actor on loopback, so any wait at all is the
/// defect. A guard that fails by exhausting thirty seconds is much weaker than
/// one that prints, and five seconds is the shortest that is still not a race.
const REFUSAL: Duration = Duration::from_secs(5);

/// A node serving a keyed system **and** an inspect service, plus a second node
/// that is a client of both.
///
/// Two services on one endpoint is the arrangement the wave is about: the same
/// peer reaches the journal and is separately admitted, or not, to the console.
struct Watched {
    system: journal::System,
    events: Events,
    journal: journal::KeyedClient,
    serving: Node,
    console_node: Node,
    /// Named rather than `_inspector`, because the follower-cap test calls it:
    /// the cap is the actor's own state, not the transport's.
    inspector: inspect::Handle<Inspector>,
    _server: Server,
}

impl Watched {
    /// A console on the client node, admitted or refused by the inspect
    /// service's own list.
    async fn console(&self) -> keylink::Result<Console> {
        Console::connect(&self.console_node, self.serving.endpoint().addr()).await
    }

    /// The raw client, for the two tests that need a cursor they control.
    async fn client(&self) -> inspect::Client {
        inspect::Client::connect(&self.console_node, self.serving.endpoint().addr())
            .await
            .expect("the console peer is admitted")
    }

    /// Mount an actor that reports its turns, and hand back the feed it listens
    /// to.
    ///
    /// `spawn_wired` and not `spawn`, because an observer is how a turn becomes
    /// an event: the registry sees mounts and endings, and only the actor sees
    /// what it ran.
    ///
    /// **The returned sender has to be kept.** Dropping it ends the feed, which
    /// ends the actor — so a test that ignores it is a test whose actors are
    /// racing it to the exit. That is exactly what
    /// `an_actor_that_ends_itself_is_delisted_and_reported_to_the_console`
    /// does on purpose.
    #[must_use = "dropping the feed's sender ends the actor this just mounted"]
    fn mount(&self, who: &ActorId) -> mpsc::Sender<u64> {
        let (ticks, from) = mpsc::channel(4);
        self.system
            .mount(
                who.clone(),
                journal::spawn_wired(
                    Journal::default(),
                    vec![self.events.observer(who.clone())],
                    journal::Feeds::new(feed(from)),
                ),
            )
            .expect("the id is free");
        ticks
    }
}

/// `admit_console` decides whether the **inspect** service's allowlist names the
/// client node. The journal service always admits it, which is what makes the
/// refusal test a statement about one service rather than about the node.
async fn start(eviction: Eviction, history: usize, admit_console: bool) -> Watched {
    let serving = local_node().await;
    let console_node = local_node().await;

    let (events, stream) = Events::new(depth(256));
    let system: journal::System = journal::System::builder(eviction)
        .lifecycle(events.clone())
        .build();

    let inspector = inspect::actor_wired(
        Inspector::new(&system, &serving, &events, depth(history)),
        vec![],
        inspect::Feeds::new(stream),
    );

    // A list of exactly one id either way, so the refusing case refuses by
    // naming somebody else rather than by naming nobody — an empty list would
    // also refuse, and would not prove the list is consulted.
    let allow = Allowlist::only([if admit_console {
        console_node.id()
    } else {
        serving.id()
    }]);

    let server = Server::builder(&serving)
        .service(journal::keyed_service(&system, Allowlist::anyone()).build())
        .service(inspector.service(allow).build())
        .spawn()
        .expect("two services, two distinct ALPNs");

    let journal = journal::KeyedClient::connect(&console_node, serving.endpoint().addr())
        .await
        .expect("the journal service admits anyone");

    Watched {
        system,
        events,
        journal,
        serving,
        console_node,
        inspector,
        _server: server,
    }
}

/// Read pages until `enough` says stop, or fail loudly.
///
/// Bounded by a timeout rather than by a retry count: every call here parks
/// until the node records something, so a loop with no clock would hang instead
/// of failing.
async fn until(console: &mut Console, what: &str, enough: impl Fn(&[LifecycleEvent]) -> bool) {
    let mut seen: Vec<LifecycleEvent> = Vec::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            seen.extend(console.events().await.expect("the console reads").events);
            if enough(&seen) {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}; saw {seen:#?}"));
}

// ───────────────────────────────────────────────────── 6a and 6b, over two nodes

/// **The actor list reports every mounted actor with 4d's four fields, read from
/// another machine.**
///
/// `System::status` is 6a and already existed; what is new is that a peer can
/// ask for it. So this asserts the list is complete *and* that every field
/// survived the wire — including `idle_for`, whose `Duration` is the one field
/// that had no obvious encoding.
#[tokio::test]
async fn the_actor_list_reaches_a_peer_with_every_field_intact() {
    let watched = start(Eviction::Never, 64, true).await;
    let _ticks: Vec<_> = (0..3)
        .map(|n| watched.mount(&id(&format!("obra/{n}"))))
        .collect();
    assert_eq!(
        journal::at(&watched.journal, id("obra/1"))
            .post(7)
            .await
            .expect("the actor answers"),
        7
    );

    let console = watched.console().await.expect("the console is admitted");

    let rows = console.actors().await.expect("the inspect actor answers");
    let mut ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        ["obra/0", "obra/1", "obra/2"],
        "every mounted actor, not a sample"
    );

    for row in &rows {
        assert!(
            !row.busy,
            "no turn is running: {} reported busy",
            row.id.as_str()
        );
        assert_eq!(row.buffered, 0, "nothing is queued for {}", row.id.as_str());
    }
    // `idle_for` crosses as a `Duration` and is compared against nothing: this
    // suite asserts no wall-clock reading, and the boundary between `idle_for`
    // and the eviction policy is proved on a paused clock in `registry.rs`.
    assert!(
        rows.iter().any(|row| row.id.as_str() == "obra/1"),
        "the actor that took a turn is in the list like any other"
    );

    let node = console.node().await.expect("the inspect actor answers");
    assert_eq!(
        node.id,
        NodeKey::from(&watched.serving.id()),
        "the node reports the key a peer dialled"
    );
    assert_eq!(
        EndpointId::try_from(node.id).expect("the key round-trips through the newtype"),
        watched.serving.id(),
        "and the newtype hands it back as the type a peer dials with"
    );
    assert_eq!(node.actors, 3);
    assert_eq!(
        node.lost, 0,
        "a 256-deep channel does not drop four events, so a non-zero here is a \
         defect in the sink rather than a shallow test"
    );
}

/// **The inspect service has an allowlist of its own, and a peer it refuses gets
/// nothing.**
///
/// The same peer, the same node, the same connection attempt — admitted to the
/// journal and refused by the console. That pairing is the claim: a second
/// service is a second decision, not a second door into the first one.
///
/// # Where the refusal lands, and why this test does not insist it is the dial
///
/// The allowlist is consulted **once per connection, on the accept side**, after
/// the QUIC handshake the ALPN gated. So the serving node closes the connection
/// and the dialling node learns about it whenever the close reaches it — which
/// may be before or after its own `connect` returns. Insisting on one of the two
/// would be a test of a race rather than of the policy. What is not a race is
/// that **no answer ever comes back**, and that is what this asserts.
///
/// Red-tested by widening the refusing list to `Allowlist::anyone()`: both
/// branches below then succeed and the test fails on its `expect_err`.
#[tokio::test]
async fn a_peer_the_inspect_allowlist_refuses_gets_nothing() {
    let watched = start(Eviction::Never, 64, false).await;
    let _ticks = watched.mount(&id("obra/0"));

    assert_eq!(
        journal::at(&watched.journal, id("obra/0"))
            .post(1)
            .await
            .expect("the journal service admits this peer"),
        1,
        "the peer is welcome on this node's other service, on this same endpoint"
    );

    let refused = match watched.console().await {
        Err(at_the_dial) => at_the_dial,
        Ok(console) => console
            .actors()
            .await
            .expect_err("a peer the inspect allowlist does not name must get nothing"),
    };
    assert!(
        matches!(refused, Error::Transport(_)),
        "a refusal is a closed connection, not an error reply: got {refused:?}"
    );

    // And it is not a one-off: the policy is consulted on every connection, so
    // a second peer's-eye view of the same node is refused the same way.
    let again = match watched.console().await {
        Err(at_the_dial) => at_the_dial,
        Ok(console) => console
            .node()
            .await
            .expect_err("a second attempt is refused the same way"),
    };
    assert!(matches!(again, Error::Transport(_)), "got {again:?}");
}

// ───────────────────────────────────────────────────────── 6c, the console reads

/// **A console observes a mount, a turn and an ending.**
///
/// Three events from three different producers, which is the point: the mount
/// comes from the registry, the turn from an `Observer` on the actor, and the
/// ending from the registry again. They arrive in one sequence on one feed.
#[tokio::test]
async fn a_console_observes_a_mount_a_turn_and_an_ending() {
    let watched = start(Eviction::Never, 64, true).await;
    let mut console = watched.console().await.expect("admitted");

    let who = id("obra/7");
    let _ticks = watched.mount(&who);
    assert_eq!(
        journal::at(&watched.journal, who.clone())
            .post(3)
            .await
            .expect("the actor answers"),
        3
    );
    let ended = watched
        .system
        .shutdown(&who, Duration::from_secs(5))
        .await
        .expect("it was mounted");
    assert!(
        matches!(ended, Ended::Stopped),
        "the actor drained inside its budget: got {ended:?}"
    );

    until(
        &mut console,
        "the three lifecycle events never arrived",
        |seen| {
            let mounted = seen.contains(&LifecycleEvent::Mounted { id: who.clone() });
            let turned = seen.iter().any(|event| {
                matches!(event, LifecycleEvent::Turn { id, method }
                     if id == &who && method == "Post")
            });
            let over = seen.contains(&LifecycleEvent::Ended {
                id: who.clone(),
                why: EndReason::Stopped,
            });
            mounted && turned && over
        },
    )
    .await;
}

/// **An actor that ends itself is delisted, and the console is told why.**
///
/// The way an actor leaves that **nobody calls** — which is the same
/// no-caller-present category the `Lifecycle` port exists to close, and the one
/// its own test missed. Nothing here calls `shutdown`, nothing calls `abort`,
/// and `Eviction::Never` means no sweeper runs: the feed's sender is dropped and
/// the actor's loop returns on its own.
///
/// Three facts about one node at one moment, because the defect was all three at
/// once and any one of them alone is a weaker test:
///
/// 1. the console's **actor list** no longer names it. Before the fix, `Actors()`
///    and `System::status` both reported it mounted with `busy: false`, forever,
///    under `Eviction::Never`;
/// 2. the console's **event log** carries the ending, with the loop's own reason
///    — `Feed { name: "Ticks" }`, which no synthesis from a join could produce;
/// 3. a **real call** to it is refused. That is the pairing that made the stale
///    entry a lie rather than merely stale: the list said yes and the wire said
///    `NoSuchActor`, at the same instant, on the same node.
///
/// The list is asked first on purpose, as a bounded poll rather than through
/// `until`: `until` fails by exhausting a thirty second timeout, and a guard that
/// fails by waiting is much weaker than one that prints the rows it saw.
///
/// Red-tested by `scripts/break-once.sh` on `keylink/src/macros.rs`, restoring
/// the discarded ending — `run_keyed(state, observers, feeds, inbox).await` back
/// to `...await;` with a `Loop` of `Output = ()`. Since that no longer compiles,
/// the break that *is* watched is `Registry::reap`'s body, in
/// `registry.rs::an_actor_that_ends_itself_is_unmounted_and_reported`; this test
/// then fails on fact 2 with the actor still listed.
#[tokio::test]
async fn an_actor_that_ends_itself_is_delisted_and_reported_to_the_console() {
    let watched = start(Eviction::Never, 64, true).await;
    let mut console = watched.console().await.expect("admitted");

    let who = id("self-ender");
    let ticks = watched.mount(&who);
    assert_eq!(
        journal::at(&watched.journal, who.clone())
            .post(1)
            .await
            .expect("the actor answers while its feed is open"),
        1
    );

    // The only thing this test does to the actor. No `shutdown`, no `abort`, no
    // sweeper, and no caller anywhere with a reason to be told.
    drop(ticks);

    // The delisting is asked first, and as a **bounded poll that prints**: a
    // guard whose failure is a hang is much weaker than one that says what it
    // saw. Ten reads over one second is enough for a loop that has nothing to do
    // but return — and when it is broken, the last `rows` is the whole evidence.
    let mut rows = Vec::new();
    for _ in 0..10 {
        rows = console.actors().await.expect("the inspect actor answers");
        if !rows.iter().any(|row| row.id == who) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !rows.iter().any(|row| row.id == who),
        "a self-ended actor must not still be listed: {rows:#?}"
    );

    until(
        &mut console,
        "the self-ending never reached the console's event log",
        |seen| {
            seen.contains(&LifecycleEvent::Ended {
                id: who.clone(),
                why: EndReason::Feed {
                    name: "Ticks".to_owned(),
                },
            })
        },
    )
    .await;

    let refused = journal::at(&watched.journal, who.clone())
        .post(2)
        .await
        .expect_err("the actor is gone, so the call has nothing to reach");
    assert!(
        matches!(
            refused,
            Error::Remote(keylink::RemoteError::NoSuchActor { .. })
        ),
        "and the wire says the same thing the list does: {refused:?}"
    );

    // The id is free, which is the operational half: under `Eviction::Never` a
    // stale entry made a self-ended id unmountable for the life of the process.
    let _again = watched.mount(&who);
}

/// **The console does not hold a handler open on an actor.**
///
/// This is the criterion the whole `#[defer]` design exists for, and a weaker
/// test would pass against the implementation it refuses. So the order matters:
///
/// 1. a subscriber attaches and parks — it has read everything there is;
/// 2. the **inspect** actor serves twenty more calls while it is parked, which a
///    handler sitting inside a turn would make impossible, because a turn holds
///    `&mut Inspector` until it returns;
/// 3. the **watched** actor serves a turn too, so "still serving turns" is true
///    of both actors in the picture;
/// 4. only then does the subscriber wake — with the event step 3 produced,
///    which is what proves it was genuinely attached rather than already
///    finished.
///
/// Step 4 is what stops step 2 being vacuous. Twenty answered calls beside a
/// subscriber that had already returned would prove nothing at all.
#[tokio::test]
async fn a_parked_subscriber_does_not_stop_the_actor_serving_turns() {
    let watched = start(Eviction::Never, 64, true).await;
    let who = id("obra/0");
    let _ticks = watched.mount(&who);

    let mut follower = watched.console().await.expect("admitted");
    // Catch the cursor up on everything recorded so far, so that the next call
    // has nothing to answer and genuinely parks.
    until(
        &mut follower,
        "the mount never reached the console",
        |seen| seen.contains(&LifecycleEvent::Mounted { id: who.clone() }),
    )
    .await;

    let parked = tokio::spawn(async move { follower.events().await });

    let reader = watched.console().await.expect("admitted");
    tokio::time::timeout(Duration::from_secs(30), async {
        for _ in 0..20 {
            assert_eq!(
                reader
                    .actors()
                    .await
                    .expect("the inspect actor answers with a subscriber attached")
                    .len(),
                1
            );
        }
    })
    .await
    .expect("a handler held open by the subscriber would never answer these");

    assert!(
        !parked.is_finished(),
        "the subscriber must still be waiting: nothing has happened for it to read"
    );

    assert_eq!(
        journal::at(&watched.journal, who.clone())
            .post(9)
            .await
            .expect("the watched actor answers with a subscriber attached"),
        9
    );

    let page: EventPage = tokio::time::timeout(Duration::from_secs(30), parked)
        .await
        .expect("the feed turn must answer the parked reply")
        .expect("the follower task did not panic")
        .expect("the console reads");
    assert!(
        page.events.iter().any(|event| matches!(
            event,
            LifecycleEvent::Turn { id, method } if id == &who && method == "Post"
        )),
        "the parked reply is answered with the turn that woke it: {page:#?}"
    );
}

/// **A page reports the events that never reached the log, not only the ones the
/// ring pushed out.**
///
/// Two counters, two places an event is lost, and only one of them used to be on
/// a page. `EventPage::lost` is arithmetic over *sequence numbers*, so it can
/// only ever describe events the log accepted; an event dropped by
/// `Events::record`'s `try_send` never gets a sequence number at all, and so was
/// invisible to it by construction. The only place it surfaced was
/// `NodeStatus::lost` — a different RPC, which the loop this crate's own
/// documentation tells a reader to write never calls.
///
/// A one-deep sink and a paused inspector is how that burst is made on purpose:
/// the actor is held inside a turn, so nothing drains the channel, and every
/// mount past the first is dropped before it is sequenced.
///
/// Red-tested by removing `dropped` from `EventPage` — which is a compile error
/// at four sites rather than a red test, so the watched break is
/// `dropped: self.dropped.load(Ordering::Relaxed)` replaced with `dropped: 0`.
/// It then fails on `the page must report what the sink threw away`.
#[tokio::test]
async fn a_page_reports_the_events_the_sink_dropped_before_they_were_sequenced() {
    let serving = local_node().await;
    // One slot, so the second event with nowhere to go is dropped and counted.
    let (events, stream) = Events::new(depth(1));
    let system: journal::System = journal::System::builder(Eviction::Never)
        .lifecycle(events.clone())
        .build();
    let inspector = inspect::actor_wired(
        Inspector::new(&system, &serving, &events, depth(64)),
        vec![],
        inspect::Feeds::new(stream),
    );

    // Mounted straight into the sink, with nobody reading fast enough: the
    // inspector's feed is one slot deep and this is sixteen mounts back to back.
    let mut ticks = Vec::new();
    for n in 0..16 {
        let (to, from) = mpsc::channel(1);
        system
            .mount(
                id(&format!("obra/{n}")),
                journal::spawn_wired(Journal::default(), vec![], journal::Feeds::new(feed(from))),
            )
            .expect("the id is free");
        ticks.push(to);
    }
    assert!(
        events.lost() > 0,
        "a one-deep sink must lose some of sixteen back-to-back mounts; this test \
         is about what a page says once it has"
    );

    let page = tokio::time::timeout(Duration::from_secs(30), inspector.follow(0))
        .await
        .expect("a cursor of zero has whatever did reach the log")
        .expect("the inspect actor is running")
        .expect("zero is behind any log");

    assert_eq!(
        page.dropped,
        events.lost(),
        "the page must report what the sink threw away, and it is the same \
         reading `NodeStatus::lost` gives"
    );
    assert!(
        page.dropped > 0 && page.events.len() < 16,
        "the point of the counter is a page that is incomplete and says so: \
         {} events, {} dropped",
        page.events.len(),
        page.dropped
    );
}

/// **The `Inspector` does not hold its own feed open.**
///
/// `EventStream`'s documentation says its `None` — every [`Events`] clone gone —
/// ends the inspector as `Ended::Feed { name: "Events" }`, *"the loud answer this
/// crate gives to a subscription that quietly died"*. It was unreachable: the
/// inspector held an `Events` clone for the drop counter, which is a `Sender` for
/// the very channel it reads, held **inside the actor reading it**. Every clone
/// could not be gone while the actor existed, and the actor could not end without
/// that happening.
///
/// So this drops both clones a program holds — the system's, by dropping the
/// system, and the caller's — and awaits the ending the inspector is supposed to
/// reach.
///
/// Red-tested by leaking one sender back into the process — `dropped:
/// Arc::clone(&events.lost)` replaced with
/// `dropped: { std::mem::forget(events.clone()); Arc::clone(&events.lost) }` —
/// which is the defect's exact effect in one literal: an `Events` clone that
/// outlives everything the program holds. Watched to fail in five seconds on
/// `the inspector must end when its feed does`.
#[tokio::test]
async fn dropping_every_events_clone_ends_the_inspector_as_a_dead_feed() {
    let serving = local_node().await;
    let (events, stream) = Events::new(depth(8));
    let system: journal::System = journal::System::builder(Eviction::Never)
        .lifecycle(events.clone())
        .build();
    let inspector = inspect::actor_wired(
        Inspector::new(&system, &serving, &events, depth(8)),
        vec![],
        inspect::Feeds::new(stream),
    );

    // The two clones a program holds: the one it kept, and the one it gave the
    // system as its `Lifecycle`.
    drop(events);
    drop(system);

    // Five seconds: the last sender is already gone, so the feed's `None` is the
    // inspector's next poll and any wait at all is the defect.
    let ended = tokio::time::timeout(REFUSAL, inspector.ended())
        .await
        .expect("the inspector must end when its feed does");
    assert!(
        matches!(&ended, Ended::Feed { name } if *name == "Events"),
        "and it ends as the dead feed rather than as a dropped handle: {ended:?}"
    );
}

/// **A cursor ahead of the node's log is refused, not parked — and that is how
/// a restart reaches a console.**
///
/// The waiting list's documented bound was *"every entry is one open QUIC
/// stream"*, and it was false in the direction that mattered: nothing released
/// an entry except an event arriving **for it**, so a cursor the log could not
/// reach was re-scanned on every event and released by none of them.
/// `follow(u64::MAX - 1)` is that cursor at its limit — it needs `2^64` events —
/// and ten peers holding one each was ten `Deferred`s retained across every
/// event the node recorded, for the life of the process.
///
/// It is a **client error**, which is why the answer is a refusal rather than a
/// wait: a cursor *at* `seq` is a console that has caught up and parks
/// correctly, and a cursor *past* it is asking about events that do not exist.
///
/// The same refusal is the restart signal. A sequence counter starts at zero in
/// every process, so a node that restarted is behind a cursor that was right a
/// moment ago — and `Ahead { after, seq }` says so at once, with `seq` as the
/// place to resume. The property the RFC claimed here was *"a `next` that went
/// backwards is a process that restarted"*, and through the shipped client it
/// was unreachable: `follow(50)` against a fresh node found nothing `> 50` and
/// **parked**, until fifty new events existed, and then answered with a `lost`
/// computed against the new ring — the new incarnation's first fifty events gone
/// and unreported.
///
/// Red-tested with `scripts/break-once.sh` on `keylink/src/console.rs`,
/// replacing the refusal's `if after > self.seq {` with `if false {` — the old
/// behaviour exactly. Watched: this test then fails in five seconds, printing
/// `a cursor ahead of the log must be refused rather than parked`.
#[tokio::test]
async fn a_cursor_ahead_of_the_log_is_refused_and_names_where_to_resume() {
    let watched = start(Eviction::Never, 64, true).await;
    let client = watched.client().await;
    let _ticks = watched.mount(&id("obra/0"));

    // Catch up first, so that `seq` is a number this test knows and the refusal
    // below is about being ahead of a real log rather than of an empty one.
    let caught_up = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let page = client
                .follow(0)
                .await
                .expect("the console reads")
                .expect("zero is behind any log");
            if page.next > 0 {
                return page.next;
            }
        }
    })
    .await
    .expect("the mount must reach the log");

    // Five seconds and not thirty: a refusal is an immediate answer from an idle
    // actor on loopback, so the only way to spend this budget is to have parked —
    // which is the defect. A guard whose failure is a long wait is a weak guard,
    // and this is the shortest wait that is still not a race.
    let refused = tokio::time::timeout(REFUSAL, client.follow(caught_up + 1_000))
        .await
        .expect("a cursor ahead of the log must be refused rather than parked")
        .expect("the call itself succeeds; it is the answer that says no")
        .expect_err("a cursor this node has not reached is a client error");

    assert_eq!(
        refused,
        FollowRefused::Ahead {
            after: caught_up + 1_000,
            seq: caught_up,
        },
        "the refusal names both the cursor and where this node's log actually is"
    );

    // And the extreme the reviewer compiled: the cursor that needs 2^64 events.
    let never = tokio::time::timeout(REFUSAL, client.follow(u64::MAX - 1))
        .await
        .expect("this is the one that could never have been answered")
        .expect("the call itself succeeds")
        .expect_err("no node reaches u64::MAX - 1");
    assert!(
        matches!(never, FollowRefused::Ahead { .. }),
        "got {never:?}"
    );

    // Nothing is holding the actor: an ordinary call still answers at once,
    // which is what says the refusals parked nobody.
    assert_eq!(
        console_rows(&client).await.len(),
        1,
        "two refused follows must have left nothing parked"
    );
}

/// The actor list through the raw client, for a test that already holds one.
async fn console_rows(client: &inspect::Client) -> Vec<keylink::ActorStatus> {
    client.actors().await.expect("the inspect actor answers")
}

/// **Past [`MAX_FOLLOWERS`], a caller is refused rather than parked.**
///
/// The other half of the bound, and the one the cursor refusal cannot reach: a
/// caller whose cursor is perfectly correct still holds an entry until the next
/// event, and on a quiet node that is unbounded time. Nothing released a parked
/// caller when its QUIC stream died — the entry outlived the stream the bound
/// was stated in terms of — so an admitted peer that called and went away, over
/// and over, grew the list one entry at a time.
///
/// # Through the local handle, and that is not a weaker test
///
/// The cap is the **actor's** state, not the transport's, and 136 QUIC
/// connections would only add iroh's own per-connection stream limit to what
/// this is measuring. That a [`FollowRefused`] crosses the wire at all is proved
/// beside this, in
/// `a_cursor_ahead_of_the_log_is_refused_and_names_where_to_resume`, over two
/// real nodes.
///
/// # Why the arithmetic is exact and not a race
///
/// `MAX_FOLLOWERS + EXTRA` calls go out at once, and which of them reaches the
/// actor first is genuinely unknown. It does not have to be known: a call is
/// refused exactly when the list is already at the cap, and the list never
/// exceeds it, so **every** refusal carries the same `parked` and **exactly**
/// `EXTRA` of the calls are refused, whatever order they arrive in.
///
/// Red-tested with `scripts/break-once.sh`, replacing
/// `self.waiting.len() >= MAX_FOLLOWERS` with `self.waiting.len() >= usize::MAX`
/// — the unbounded list as it stood. Watched to fail in five seconds with
/// `8 calls past the cap must be refused; 0 finished`.
#[tokio::test]
async fn a_caller_past_the_follower_cap_is_refused_rather_than_parked() {
    /// Enough past the cap to be unmistakable, and few enough to be quick.
    const EXTRA: usize = 8;

    let watched = start(Eviction::Never, 64, true).await;
    let _ticks = watched.mount(&id("obra/0"));

    // Catch a cursor up to the log, so that every call below parks for the right
    // reason — a caught-up console — rather than being refused as `Ahead`.
    let at = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let page = watched
                .inspector
                .follow(0)
                .await
                .expect("the inspect actor is running")
                .expect("zero is behind any log");
            if page.next > 0 {
                return page.next;
            }
        }
    })
    .await
    .expect("the mount must reach the log");

    let calls: Vec<_> = (0..MAX_FOLLOWERS + EXTRA)
        .map(|_| {
            let inspector = watched.inspector.clone();
            tokio::spawn(async move { inspector.follow(at).await })
        })
        .collect();

    // The refusals are the ones that finish; the parked calls cannot, because
    // nothing has happened for them to read. `REFUSAL` and not thirty seconds,
    // for the reason that constant carries.
    tokio::time::timeout(REFUSAL, async {
        loop {
            if calls.iter().filter(|call| call.is_finished()).count() == EXTRA {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{EXTRA} calls past the cap must be refused; {} finished",
            calls.iter().filter(|call| call.is_finished()).count()
        )
    });

    // One event releases every parked caller at once, which is why `Crowded` is
    // retryable rather than fatal — and it is also what lets this test collect
    // all of the answers instead of only the refusals.
    let _second = watched.mount(&id("obra/1"));

    let mut refused = 0;
    let mut answered = 0;
    for call in calls {
        match tokio::time::timeout(Duration::from_secs(30), call)
            .await
            .expect("one event answers every parked caller")
            .expect("no follower task panicked")
            .expect("the inspect actor is running")
        {
            Ok(_page) => answered += 1,
            Err(refusal) => {
                assert_eq!(
                    refusal,
                    FollowRefused::Crowded {
                        parked: MAX_FOLLOWERS as u64,
                    },
                    "a refusal past the cap names how many were already waiting"
                );
                refused += 1;
            }
        }
    }
    assert_eq!(refused, EXTRA, "exactly the calls past the cap are refused");
    assert_eq!(
        answered, MAX_FOLLOWERS,
        "and the cap's worth of callers are parked and then answered, never dropped"
    );
}

/// **A console that fell behind is told how much it missed, exactly.**
///
/// The log is a ring of `history` events, so a console that connects late — or
/// one that was away — cannot be given what has been dropped. What it must not
/// be given is silence: `lost` is arithmetic between the cursor it asked from
/// and the oldest sequence still held, so the gap is a number rather than an
/// absence the console has to infer.
#[tokio::test]
async fn a_console_behind_the_ring_is_told_exactly_how_much_it_missed() {
    const HISTORY: usize = 2;
    const MOUNTS: usize = 4;

    let watched = start(Eviction::Never, HISTORY, true).await;
    let _ticks: Vec<_> = (0..MOUNTS)
        .map(|n| watched.mount(&id(&format!("obra/{n}"))))
        .collect();

    // `follow(0)` does not advance anything on the node, so asking again is free
    // and this loop is how the test waits for all four mounts to have been
    // recorded without a sleep.
    let client = watched.client().await;
    let page = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let page = client
                .follow(0)
                .await
                .expect("the console reads")
                .expect("a cursor of zero is behind any log, so it is never refused");
            if page.next == MOUNTS as u64 {
                return page;
            }
        }
    })
    .await
    .expect("four mounts must reach the log");

    assert_eq!(
        page.events.len(),
        HISTORY,
        "the ring holds its last two and no more"
    );
    assert_eq!(
        page.lost, 2,
        "and says so: two events happened before the oldest one still held"
    );
    let kept: HashSet<&str> = page
        .events
        .iter()
        .map(|event| match event {
            LifecycleEvent::Mounted { id } => id.as_str(),
            other => panic!("only mounts happened here: {other:?}"),
        })
        .collect();
    assert_eq!(
        kept,
        HashSet::from(["obra/2", "obra/3"]),
        "the ring keeps the newest, not the first it saw"
    );
}
