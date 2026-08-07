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
    inspect, ActorId, Allowlist, Caller, Console, EndReason, Ended, Error, EventPage, Events,
    Eviction, Inspector, LifecycleEvent, Node, Server,
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
    _inspector: inspect::Handle<Inspector>,
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
        _inspector: inspector,
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
        *watched.serving.id().as_bytes(),
        "the node reports the key a peer dialled"
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
            let page = client.follow(0).await.expect("the console reads");
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
