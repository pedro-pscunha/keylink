//! What a mailbox depth does to the CALLER, over a real QUIC connection.
//!
//! Two iroh endpoints on the loopback interface, no relay and no discovery, and
//! one keyed actor behind a served client. It has to be over the wire:
//! `Registry::route` is `pub(super)`, so a test outside the crate reaches a keyed
//! mailbox only through a served client — and every figure below therefore
//! includes stream setup and postcard encode, which is why the measurement runs a
//! control that has those and no queue.
//!
//! # Its own file, and the reason is an allocator
//!
//! `tests/actor_memory.rs` also parameterises `mailbox_depth`, and this does not
//! belong beside it: that file installs a counting global allocator, so every
//! allocation in its process pays an atomic `fetch_add`. That is a tax on exactly
//! the quantity measured here.
//!
//! # One test asserts, one measurement prints
//!
//! [`keylink::RegistryBuilder::mailbox_depth`] makes a claim with two halves — a
//! shape and a duration — and they want opposite harnesses. The shape is exact and
//! belongs in the gate. The duration is a wall-clock reading on a machine doing
//! other things, and an assertion on it would be a coin flip. So the shape is
//! asserted and the duration is printed, in the shape `tests/persist.rs`
//! established.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::prelude::*;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use keylink::{
    Allowlist, Body, BodyReader, Caller, Context, Eviction, Middleware, Next, Node, Outcome, Server,
};
use tokio::sync::{mpsc, oneshot, Semaphore};

mod common;
use common::local_node;

keylink::service! {
    service Queue;

    /// Holds the actor's one task open until the test releases it, so every
    /// message sent after this one is provably waiting rather than probably
    /// waiting.
    rpc Hold() -> u64;

    /// A turn with a reply. What a caller-side clock times.
    ///
    /// An `rpc` and not a `tell`, and that is forced rather than chosen: the
    /// generated keyed client has **no** `tell` method. See the guard below.
    rpc Touch() -> u64;
}

struct Queued {
    /// Fires when `hold` enters its turn.
    entered: mpsc::UnboundedSender<()>,
    /// Closed until the test opens it. A `Semaphore` and not a `Notify`, because a
    /// permit added before anyone waits still releases the waiter.
    release: Arc<Semaphore>,
    /// Fires once per completed turn, so a drain is counted rather than slept for.
    turns: mpsc::UnboundedSender<()>,
}

impl queue::Actor for Queued {
    async fn hold(&mut self, _cx: Caller<'_>) -> u64 {
        let _ = self.entered.send(());
        self.release
            .acquire()
            .await
            .expect("the test never closes the semaphore")
            .forget();
        let _ = self.turns.send(());
        0
    }

    async fn touch(&mut self, _cx: Caller<'_>) -> u64 {
        let _ = self.turns.send(());
        0
    }
}

/// Fires once per call as it reaches the serving side, **before** the mailbox.
///
/// This is what makes the boundary assertion exact instead of timed. A layer runs
/// inside the serving task and `next` is what eventually offers the message to the
/// mailbox, so one signal per call here says every call has arrived and is either
/// in the mailbox or parked in front of it.
///
/// It is not optional. [`keylink::ActorStatus::queued`] cannot make that
/// distinction alone — a parked sender is not observable at all — so without this
/// signal "three of four are in the mailbox" could not be told apart from "the
/// fourth has not landed yet", and the assertion would be a floor instead of an
/// equality.
struct Arrived(mpsc::UnboundedSender<()>);

impl<Req: Send + 'static, Resp: Send + 'static> Middleware<Req, Resp> for Arrived {
    fn handle(
        &self,
        cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> Pin<Box<dyn Future<Output = (Outcome<Resp>, Body)> + Send>> {
        let _ = self.0.send(());
        next(cx, req, body)
    }
}

/// One keyed actor at a stated mailbox depth, reached through its own generated
/// client.
struct Fixture {
    client: queue::RegistryClient,
    /// Fires when `hold` starts its turn.
    entered: mpsc::UnboundedReceiver<()>,
    /// Fires when a call reaches the serving side, before the mailbox.
    arrived: mpsc::UnboundedReceiver<()>,
    /// Lets `hold` return.
    release: Arc<Semaphore>,
    /// Fires once per completed turn.
    turns: mpsc::UnboundedReceiver<()>,
    system: queue::Registry,
    _server: Server,
    _nodes: [Node; 2],
}

/// The one actor every test and the measurement address.
fn only() -> keylink::ActorId {
    keylink::ActorId::new("queued").expect("a valid id")
}

async fn at_depth(depth: usize) -> Fixture {
    let server = local_node().await;
    let client_node = local_node().await;
    let system: queue::Registry = queue::Registry::builder(Eviction::Never)
        .mailbox_depth(NonZeroUsize::new(depth).expect("a mailbox depth is never zero"))
        .build();
    let (entered_tx, entered) = mpsc::unbounded_channel();
    let (arrived_tx, arrived) = mpsc::unbounded_channel();
    let (turns_tx, turns) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let running = Server::builder(&server)
        .service(
            system
                .service(Allowlist::anyone())
                .layer(Arrived(arrived_tx))
                .build(),
        )
        .spawn()
        .expect("one service, no duplicate ALPN");
    let client = queue::RegistryClient::connect(&client_node, server.endpoint().addr())
        .await
        .expect("the client dials the actor system");
    system
        .mount(
            only(),
            queue::spawn(Queued {
                entered: entered_tx,
                release: Arc::clone(&release),
                turns: turns_tx,
            }),
        )
        .expect("the id is free");

    Fixture {
        client,
        entered,
        arrived,
        release,
        turns,
        system,
        _server: running,
        _nodes: [server, client_node],
    }
}

/// The mailbox reading for the fixture's one actor.
fn queued(system: &queue::Registry) -> u64 {
    let mut rows = system.status();
    assert_eq!(rows.len(), 1, "the fixture mounts exactly one actor");
    rows.remove(0).queued
}

/// Wait until the mailbox holds `messages`.
///
/// Bounded, so a reading that never arrives fails the test instead of hanging it.
/// Nothing here asserts how long anything took: the budget is three orders of
/// magnitude out, and its expiry means the count is stuck rather than slow.
async fn until_queued(system: &queue::Registry, messages: u64) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while queued(system) != messages {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{messages} messages must reach the mailbox"));
}

// ──────────────────────────────────────────────────────────────────── the guard

/// **A mailbox at depth `N` holds exactly `N` messages, and releases none of its
/// callers early.**
///
/// This is the shape half of [`keylink::RegistryBuilder::mailbox_depth`]'s claim, and
/// **writing** it is what corrected that claim — the compiler did, before any of it
/// ran: the first draft sent a `tell` and did not compile. The doc said the first
/// `N` callers
/// "hand their message over and go on as if the actor were fast" while "the
/// `N + 1`th feels the whole queue at once". Through a generated keyed client no
/// caller can do either, and the reason is a design decision rather than a timing
/// accident: the sender on a keyed mailbox is **the serving task, not the peer**.
///
/// `__keylink_actor_client_method!` in `keylink/src/macros.rs` generates **no**
/// method at all for a `tell` on a keyed `ActorRef`, with a comment saying why —
/// four keyed refusals are decided after the bytes reach the wire and are reported
/// on the reply, so fire-and-forget to a keyed actor would hide them. So every
/// keyed call is an `rpc` that awaits its own reply, and awaiting a reply means
/// waiting for the actor. That is what the two assertions below hold together: the
/// mailbox took three messages, and all four callers are still waiting.
///
/// # The one caller this test does not build, and why it does not weaken the above
///
/// **A hand-rolled peer reaches this same mailbox without awaiting the actor**, and
/// every part of the spelling is public: [`keylink::RpcClient::tell`] is generic over
/// any `Serialize` request and opens a unidirectional stream, `keylink::Addressed`
/// wraps a request with an actor id, and the generated module exports both `Request`
/// and `registry_alpn()`. `rpc.rs`'s accept loop takes a unidirectional stream on every
/// served connection, keyed included, and `serve_one_tell` reads the actor off the
/// envelope. **The macro arm cited above says this itself** — "a keyed `tell` really
/// would work … `serve_one_tell` walks the same dispatch a call does" — so what is
/// absent is the generated *method*, not the wire path. That is the far side of the
/// same sentence as above: the serving task is the sender, so a `tell` peer has
/// returned by the time the mailbox is even offered the message.
///
/// **The deleted claim is unreachable on that path too, which is why finding this
/// `tell` is not a reason to bring it back.** That caller returns at every depth and
/// never feels the queue — there is no `N`th caller going on quickly and no `N + 1`th
/// feeling the queue at once, because *none* of them feels it. The old sentence was
/// wrong for both kinds of caller, for two different reasons.
///
/// This test uses the generated client deliberately: that is the caller
/// `mailbox_depth`'s doc is written for, and a `tell` peer has no reading to take —
/// it cannot tell a queued message from a lost one, which is the guarantee
/// `RpcClient::tell` documents it does not give.
///
/// What the knob moves is therefore where the queue is *held* and who can *see*
/// it, which is the trade the rest of that doc describes correctly.
///
/// # No clock in any assertion, and no order across streams
///
/// Nothing here *asserts on* a clock, so nothing rests on how loaded the machine is.
/// One clock is read — `until_queued` bounds its poll loop with a 30 second
/// `tokio::time::timeout` — and that budget can only turn a hang into a failure,
/// which its own doc says. The exactness comes from three signals: `hold` is released
/// by a semaphore rather than by a timer, `Arrived` says every message has reached the
/// serving side, and [`keylink::ActorStatus::queued`] is exactly
/// `max_capacity() - capacity()`.
///
/// It asserts nothing about the order the four messages are *handled* in. Each call
/// takes its own QUIC stream and `ActorRef`'s own documentation says two calls can
/// arrive in either order, so a per-message order assertion would be pinning the
/// network. The count is what is exact.
///
/// Red-tested by making `Registry::entry` build every channel at
/// `DEFAULT_MAILBOX_DEPTH` instead of the system's depth — replacing the whole line
/// `            None => (self.mailbox_depth, false),` with
/// `            None => (DEFAULT_MAILBOX_DEPTH, false),`, which is the
/// whole of what the knob does. Watched here: this test panics in `until_queued`
/// at "3 messages must reach the mailbox" after its 30 second budget, because a
/// system asked for three slots that builds one never reaches a `queued` of 3.
///
/// **This break is shared, and the record names every guard it reaches rather than
/// claiming this test is its only reader.** `registry.rs` already records the same
/// substitution for
/// `a_budget_sized_for_the_default_depth_under_drains_at_a_deeper_one`, which is the
/// clue that it must reach more than one. Every call site of `mailbox_depth` outside
/// this file was found by search and then run against the break by name. **The
/// outcomes are not uniform: one guard HANGS rather than reddens**, so a run that
/// stalls here is the break working and not a broken harness. Watched, each by name:
///
/// - this test — panics in `until_queued` at "3 messages must reach the mailbox",
///   after its 30 second budget;
/// - `a_budget_sized_for_the_default_depth_under_drains_at_a_deeper_one`, in
///   `registry.rs`'s `the mailbox's depth` section — fails its `Ended::Vanished`
///   assertion, because a deep system whose mailboxes are shallow drains like a
///   shallow one;
/// - `the_mailbox_depth_is_the_systems_and_defaults_to_one`, in that same section and
///   named after the knob itself — fails `left: 1, right: 4` on the first
///   `max_capacity()` reading of its `depth(4)` system. **Both of those numbers are
///   borrowed from `registry.rs`, and they are recorded anyway.** The 4 is that test's
///   own `depth(4)` and the 1 is `DEFAULT_MAILBOX_DEPTH`, so moving either literal
///   makes this line false while the guard stays correct, the anchor still resolves and
///   `mise run anchors` stays green. They stay because that assertion carries no
///   message at all, so the two integers are the only tokens a replay has to match on —
///   and a message added now would put a sentence in this record that nobody has
///   watched fail;
/// - `a_saturated_mailbox_at_depth_does_not_starve_a_feed` in `tests/actors.rs` —
///   panics after its own 30 second budget, and that number is `actors.rs`'s own
///   `until_queued` rather than this file's same-named helper;
/// - `queued_is_exact_at_depth_and_counts_no_parked_sender`, also in that section —
///   **HANGS. It never reports at all.** Its actor holds every turn, so nothing ever
///   frees a permit: at one slot the second of its three `mail.send(..).await`s parks
///   forever, before the assertion that would have caught the wrong count. Killed
///   here at 120 seconds.
///
/// One further call site is reached and the break is **invisible** there, which is
/// worth a line because it is a guard on this knob that cannot report on it.
/// `ten_thousand_idle_actors_fit_in_the_budget` in `tests/actor_memory.rs` asserts that
/// its three depths cost the *same* bytes per actor, so a break making all three of
/// them depth 1 satisfies it by flattening the thing it measures. Run both ways here:
/// depths 1, 32 and 1,024 each printed **1738 bytes** on the honest tree and **1738
/// bytes** under the break, and the test passed both times. Not one figure moved. That
/// guard is about the allocator's block size and is no evidence about the knob.
///
/// **That 1738 is a reading, not a property, and the difference decides what falsifies
/// this paragraph.** It is a `println!` in `actor_memory.rs`'s phase 5 that no assertion
/// gates — the assertion beside it compares the three depths to within one pointer and
/// names no magnitude — and it comes from a process-wide allocation counter on one
/// machine. The number is in no source file, so unlike the two integers above it cannot
/// be rotted by an edit; it rots by being read on another host. **What the paragraph
/// rests on is the two readings being EQUAL, never their value.** A machine printing
/// some other number leaves the point standing, and three depths disagreeing with each
/// other is what would retract it. The honest-tree half was re-measured on 2026-08-08
/// with `cargo nextest run -p keylink --no-capture` under an `-E` filter naming that
/// test, and printed 1738 at each of the three depths again. The under-break half is
/// still one author's single run and has not been repeated.
///
/// Nothing outside those six call sites was run, and this is not a claim about the
/// whole suite.
///
/// So this break says "the knob is disconnected" and not "this test is the only
/// thing watching the knob". What is unique here is the *caller*-side half. Of the four
/// guards above, `a_saturated_mailbox_at_depth_does_not_starve_a_feed` is the only one
/// with callers over a wire at all, and it checks that they are eventually answered —
/// three `touch` replies. The other three have no caller awaiting a reply at all: one
/// reads only `max_capacity()`, and two put messages into `mail` by hand. **Not one of
/// them asserts that a caller is still waiting at a chosen moment.** That is the
/// assertion only this file
/// makes: a message the mailbox has accepted still leaves its caller waiting, which is
/// precisely the sentence this file corrected.
#[tokio::test]
async fn a_mailbox_holds_exactly_its_depth_and_releases_no_caller_early() {
    /// Deeper than the default, and small enough that the whole population sits well
    /// under QUIC's default hundred concurrent streams. The population is
    /// `SLOTS + 1`, so a mailbox **deeper** than asked for would hold all of it and
    /// read 4 — which is what makes the equality below two-sided rather than a
    /// floor.
    const SLOTS: usize = 3;
    /// One more than the mailbox can hold, so exactly one message is parked
    /// outside it.
    const CALLERS: usize = SLOTS + 1;

    // The same three, as `ActorStatus::queued` reports them: `queued` is a `u64`
    // and a mailbox depth is a `usize`, because only one of the two crosses a wire.
    // Converted rather than written out a second time — a second literal would go on
    // compiling after an edit to `SLOTS`, and would leave the assertions below
    // checking the old number.
    let slots_filled = u64::try_from(SLOTS).expect("three fits in a u64");

    let mut fixture = at_depth(SLOTS).await;
    let client = &fixture.client;
    let system = &fixture.system;
    let release = &fixture.release;
    let entered = &mut fixture.entered;
    let arrived = &mut fixture.arrived;
    let turns = &mut fixture.turns;
    // Fed by each caller the moment its own `await` returns, so "no caller has
    // been released" is a reading rather than an inference.
    let (answered_tx, mut answered) = mpsc::unbounded_channel();
    // Held shut until `hold` is provably inside its turn. Without it the four
    // callers race `hold` for the first turn, and a `touch` handled first would
    // leave the mailbox in a state no assertion here describes.
    let (start, wait_to_start) = oneshot::channel();

    let holding = async { client.at(only()).hold().await };
    let calling = async {
        wait_to_start.await.expect("the watcher starts the callers");
        // Bound first: `queue::at` returns a borrowing handle, so the futures have
        // to outlive the temporaries they are built from.
        let handles: Vec<_> = (0..CALLERS).map(|_| client.at(only())).collect();
        n0_future::join_all(handles.iter().map(|handle| {
            let answered_tx = answered_tx.clone();
            async move {
                let seen = handle.touch().await;
                let _ = answered_tx.send(());
                seen
            }
        }))
        .await
    };
    let watching = async {
        entered.recv().await.expect("`hold` enters its turn");
        // `hold` arrived too, and its message is IN the turn rather than waiting
        // for one — so this drains the one signal that must not be counted as
        // queued.
        arrived.recv().await.expect("`hold` arrived");
        assert_eq!(
            queued(system),
            0,
            "the held message is inside the turn and occupies no slot, which is \
             the distinction a free-capacity reading alone cannot make"
        );

        start.send(()).expect("the callers are waiting to start");
        for _ in 0..CALLERS {
            arrived
                .recv()
                .await
                .expect("every caller's message arrives");
        }

        until_queued(system, slots_filled).await;
        assert_eq!(
            queued(system),
            slots_filled,
            "the mailbox must hold every message its depth allows and not one \
             more: {CALLERS} messages have reached the serving side and {SLOTS} \
             slots exist, so one is parked in a serving task where nothing can \
             count it"
        );
        assert!(
            answered.try_recv().is_err(),
            "no caller may be released by a free slot: all {CALLERS} are still \
             awaiting a reply the held actor has not produced, which is the half \
             of `mailbox_depth`'s doc that used to say the first {SLOTS} would go \
             on as if the actor were fast"
        );

        release.add_permits(1);

        // `CALLERS` turns plus the held one. The message parked outside the
        // mailbox was queued rather than dropped, and this is what says so.
        for _ in 0..=CALLERS {
            turns.recv().await.expect("every message is handled");
        }
    };

    let (held, replies, ()) = tokio::join!(holding, calling, watching);
    held.expect("the held call answers");
    for reply in replies {
        reply.expect("every queued call answers");
    }
}

// ───────────────────────────────────────────────────────────── the measurement

/// **Not a test. The latency number `docs/rfc-04-actors.md` §6.2 made the condition
/// for this knob existing, in a form anybody can re-run.**
///
/// It asserts nothing about time. Every figure below is a wall-clock reading taken
/// on a machine that may have two compilers on it, so it prints its own conditions
/// — the load average and the sccache hit rate — and it is `#[ignore]`d so the gate
/// never pays for it. Run it with:
///
/// ```sh
/// cargo test -p keylink --release --test mailbox_latency -- --ignored --nocapture
/// ```
///
/// # What is held fixed
///
/// `CALLERS` messages at every depth, and a `Touch` turn that does one channel
/// send — so the answer is a function of the queue and not of how far the run got.
/// The two depths are 1 and 32, which is the range
/// [`keylink::RegistryBuilder::mailbox_depth`] claims is free of any memory trade,
/// because `tokio::sync::mpsc` allocates its first block of 32 slots eagerly. **That
/// block size is `tokio`'s and not this repository's**, so it moves on a version bump
/// with nothing here changing — the external-owner case `rpc.rs`'s module doc states in
/// full, and the reason this sentence names the crate that sets it.
/// `ten_thousand_idle_actors_fit_in_the_budget` in `tests/actor_memory.rs` measures the
/// consequence rather than the block size: an empty mailbox costs the same at all three
/// of its depths.
///
/// # What is measured, and it is three quantities
///
/// Two of the three are one caller's own `Instant` around its own `await`, because
/// the claim is about the caller. Every reading therefore contains a stream setup,
/// a postcard encode, a QUIC round trip and one turn — `Registry::route` is
/// `pub(super)` and there is no shorter path from a test.
///
/// - **alone** is the CONTROL, and it is the row that makes the other two readable:
///   `CALLERS` calls one at a time, so no queue can form. It is the whole cost of
///   the four operations above with the queue removed. It stands in the same role
///   as `tests/persist.rs`'s "the same turns with no observer at all".
/// - **burst** is `CALLERS` callers at once with the actor free, fastest and
///   slowest of their own readings. This is where "the `N + 1`th feels the whole
///   queue at once" would show up if a depth moved it.
/// - **drain** is the service side, and it is the only quantity where depth changes
///   the work done: `CALLERS` messages are parked with the actor held, then
///   released, and the whole queue is timed by counting turns. At depth 1 every
///   turn but the first needs a parked sender woken before the loop can receive; at
///   depth 32 the messages are already in the channel. That hop is the only latency
///   a depth can buy.
///
/// The numbers this produced, on the machine and day named, are in
/// [`keylink::RegistryBuilder::mailbox_depth`]'s own documentation.
#[tokio::test]
#[ignore = "a measurement, not an assertion: run with --ignored --nocapture"]
async fn what_a_deeper_mailbox_buys_the_caller() {
    use n0_future::time::Instant;

    /// Thirty-two, matching the deepest depth measured: at 32 the whole burst fits
    /// in the mailbox and at 1 all but one of it parks, which is the widest
    /// contrast the claimed-free range holds. Also well under QUIC's default
    /// hundred concurrent streams, so no reading here includes a caller waiting for
    /// a stream.
    const CALLERS: u32 = 32;

    println!("── what a mailbox depth costs a caller, over real QUIC");
    println!("{}", machine_conditions());
    println!("   every row includes stream setup, postcard encode and one round trip");

    for depth in [1usize, 32] {
        let mut fixture = at_depth(depth).await;

        // The control: one at a time, so the queue is empty at every call.
        let started = Instant::now();
        for _ in 0..CALLERS {
            fixture.client.at(only()).touch().await.expect("answers");
        }
        let alone = micros(started.elapsed(), CALLERS);
        settle(&mut fixture, CALLERS).await;

        // The burst, timed per caller, with the actor free.
        let handles: Vec<_> = (0..CALLERS).map(|_| fixture.client.at(only())).collect();
        let each = n0_future::join_all(handles.iter().map(|handle| async move {
            let started = Instant::now();
            handle.touch().await.expect("answers");
            started.elapsed()
        }))
        .await;
        drop(handles);
        settle(&mut fixture, CALLERS).await;
        let fastest = micros(*each.iter().min().expect("CALLERS is not zero"), 1);
        let slowest = micros(*each.iter().max().expect("CALLERS is not zero"), 1);

        // The drain: every message provably parked, then released, then timed.
        let drained = drain_at_depth(&mut fixture, CALLERS).await;

        println!("   depth {depth:>4}");
        println!("      alone   : {alone:>9.1} µs/call  (the control: no queue)");
        println!("      burst   : {fastest:>9.1} µs fastest, {slowest:>9.1} µs slowest");
        println!(
            "      drain   : {drained:>9.1} µs/queued turn  (service side, the release \
             included)"
        );
    }
}

/// Park `callers` messages behind a held turn, release it, and time the whole
/// queue in microseconds per queued turn.
///
/// The one figure here that depth can move, and the reason it needs its own
/// helper: the queue has to be provably loaded before the clock starts, which is
/// `Arrived` plus a semaphore rather than a sleep.
///
/// **The clock covers `callers + 1` turns and the divisor is `callers`, deliberately.**
/// The held turn's own release is inside the window — the permit is added after
/// `Instant::now()` — so the released actor's wakeup is charged to the queued turns
/// rather than excluded from them. That adds the same absolute overstatement at both
/// depths, which is a *larger share* of the deep row because the deep row is the
/// smaller number. So it compresses the ratio between the two depths rather than
/// widening it, and the contrast reported is a floor on the real one. Hence "per
/// queued turn" and not "per turn": the row names what this divides by.
async fn drain_at_depth(fixture: &mut Fixture, callers: u32) -> f64 {
    use n0_future::time::Instant;

    let client = &fixture.client;
    let release = &fixture.release;
    let entered = &mut fixture.entered;
    let arrived = &mut fixture.arrived;
    let turns = &mut fixture.turns;
    let (start, wait_to_start) = oneshot::channel();

    let holding = async { client.at(only()).hold().await };
    let calling = async {
        wait_to_start.await.expect("the watcher starts the callers");
        let handles: Vec<_> = (0..callers).map(|_| client.at(only())).collect();
        n0_future::join_all(handles.iter().map(|handle| handle.touch())).await
    };
    let timing = async {
        entered.recv().await.expect("`hold` enters its turn");
        arrived.recv().await.expect("`hold` arrived");
        start.send(()).expect("the callers are waiting to start");
        for _ in 0..callers {
            arrived
                .recv()
                .await
                .expect("every caller's message arrives");
        }

        let started = Instant::now();
        release.add_permits(1);
        for _ in 0..=callers {
            turns.recv().await.expect("every message is handled");
        }
        micros(started.elapsed(), callers)
    };

    let (held, replies, each) = tokio::join!(holding, calling, timing);
    held.expect("the held call answers");
    for reply in replies {
        reply.expect("every queued call answers");
    }
    each
}

/// Let one phase's turns finish, so its queue is not the next phase's baseline.
async fn settle(fixture: &mut Fixture, turns: u32) {
    for _ in 0..turns {
        fixture
            .turns
            .recv()
            .await
            .expect("every message is handled");
    }
    while fixture.arrived.try_recv().is_ok() {}
}

/// Microseconds per call, as a float, for the prints above. Not used by any
/// assertion.
fn micros(elapsed: Duration, calls: u32) -> f64 {
    elapsed.as_secs_f64() * 1_000_000.0 / f64::from(calls)
}

/// The machine's own state, printed beside the numbers rather than required of
/// them.
///
/// A precondition on load cannot work here: this repository measured a load average
/// of 10.00 with `pgrep -x cargo`, `pgrep -x rustc` and `pgrep -x mise` all
/// returning zero, so an idle-machine gate can stay shut forever for reasons that
/// have nothing to do with compilers. `.githooks/pre-commit` deleted a bare timing
/// figure for the other half of the same reason: the variance was larger than the
/// quantity claimed. So the conditions are recorded next to the reading instead of
/// asserted before it.
///
/// Not used by any assertion. A command that did not run says so, rather than
/// leaving a blank where a condition should be.
fn machine_conditions() -> String {
    let load = output_of("uptime", &[]).map_or_else(
        || "unavailable: `uptime` did not run".to_owned(),
        |out| match out.split_once("load average") {
            Some((_, tail)) => format!("load average{}", tail.trim_end()),
            None => out.trim().to_owned(),
        },
    );
    let cache = output_of("sccache", &["--show-stats"]).map_or_else(
        || "unavailable: `sccache --show-stats` did not run".to_owned(),
        |out| {
            let pick = |prefix: &str| {
                out.lines()
                    .find(|line| line.starts_with(prefix))
                    .map_or_else(
                        || format!("{prefix} not reported"),
                        |line| line.split_whitespace().collect::<Vec<_>>().join(" "),
                    )
            };
            format!("{} | {}", pick("Compile requests"), pick("Cache hits"))
        },
    );
    format!("   load        : {load}\n   sccache     : {cache}")
}

/// A command's stdout, or nothing when it did not run or did not succeed.
fn output_of(program: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(program)
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}
