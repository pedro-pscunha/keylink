//! A cycle of blocking sends, at N = 2 — the case the crate documents at N = 1.
//!
//! `macros.rs` documents the self-call deadlock thoroughly: "An actor must not
//! call itself. Any method, including `tell`." What it does not say is that the
//! count of actors does not matter. These tests establish that it does not, and
//! they establish the mechanism, because at N = 2 the mechanism is **not** the one
//! the self-call section names.
//!
//! The self-call section's `tell` case is about the *permit*: the mailbox holds one
//! message, the actor released that permit when it took the message it is running,
//! so a self `tell` succeeds only if nobody else took the permit first. That is
//! load-dependent, which is what makes it dangerous.
//!
//! At N = 2 nothing is load-dependent. Both sends succeed on an idle pair, and both
//! callers then block on an **answer** that only a loop can produce. So A → B → A
//! hangs on the first attempt, every time, with no contention anywhere — a harder
//! failure than the one the crate already warns about, and a quieter one. What
//! `LOCAL_MAILBOX_DEPTH = 1` buys is not silence on the send — any depth ≥ 1 is
//! silent for one queued message, and `mpsc::channel(0)` panics, so there is no
//! depth that *would* complain. What 1 buys is that **exactly one** message fits:
//! the ring closes quietly, and the very next caller blocks. Both halves are
//! asserted below.
//!
//! The order of these tests is the argument:
//!
//! - a turn runs with its own permit already free, so exactly one message fits
//!   behind it — the fact the rest depends on;
//! - A → B → A hangs, and so does everything else either actor is asked;
//! - `abort` recovers the *other* actor and destroys the one it names;
//! - a read from inside a turn is the same deadlock through
//!   [`Handle::with_state`];
//! - parking the reply and leaving the turn breaks the cycle — and parking it
//!   without leaving the turn does not, which is the distinction a reader needs;
//! - and the cycle `compose` already breaks: the same publish from inside a turn
//!   hangs under [`Overflow::BlockPublisher`] and completes under
//!   [`Overflow::DropSubscriber`], so that policy is a safety property and not a
//!   preference about throughput.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use keylink::compose::{Overflow, Subscription, Topic};
use keylink::{Caller, Ended, Replied, Reply};
use tokio::sync::Notify;

keylink::service! {
    service Ring;

    /// Hop to the wired peer `hops` times, and answer with the depth reached.
    /// `hops` of 0 answers without calling anyone, so it is the only arm that
    /// can terminate a chain.
    rpc Hop(hops: u64) -> u64;

    /// Answers immediately with the `bump` count. A liveness probe: only a loop
    /// that is polling can produce this answer, so a timeout on it means the
    /// actor is inside a turn.
    rpc Ping() -> u64;

    /// Reads this actor's own state, from inside its own turn, through the
    /// public read seam.
    rpc SelfRead() -> u64;

    /// Holds the turn open until the test releases the gate. Nothing else here
    /// can pin a turn without also sleeping.
    rpc Wait() -> u64;

    /// A message with no reply. `tell` is what makes the permit observable: it
    /// resolves when the **send** completes, so it separates "the mailbox took
    /// it" from "the loop ran it".
    tell Bump() -> ();

    /// Parks its reply, leaves the turn, and hops from a spawned task.
    #[defer]
    rpc ParkedHop(hops: u64) -> u64;

    /// Parks its reply and then hops **without** leaving the turn. The control
    /// for the one above: it isolates leaving the turn from parking the reply.
    #[defer]
    rpc ParkedBlockingHop(hops: u64) -> u64;
}

/// One state, two instances. Two actors of one service is still two actors, and
/// it costs a reader one type instead of two.
struct Peer {
    /// The peer this actor hops to. An `Arc<OnceLock<_>>` because each handle
    /// needs the other one, so neither can be built first — and because a
    /// `Peer` holding a `Handle<Peer>` inline would have no size.
    other: Arc<OnceLock<ring::Handle<Peer>>>,
    /// This actor's own handle, for the self-call probe.
    me: Arc<OnceLock<ring::Handle<Peer>>>,
    /// Bumped on entry to `wait`, so a test can learn the turn has started by
    /// looking rather than by sleeping.
    entered: Arc<AtomicUsize>,
    /// Released by the test, so `wait` holds its turn for as long as the test
    /// needs and not for a wall-clock guess.
    gate: Arc<Notify>,
    /// How many `tell`s this actor has run. Answered by `ping`, so a test can
    /// tell a send that landed in the mailbox from one the loop got to.
    bumps: u64,
}

impl ring::Actor for Peer {
    async fn hop(&mut self, _cx: Caller<'_>, hops: u64) -> u64 {
        if hops == 0 {
            return 0;
        }
        let other = self.other.get().expect("wired").clone();
        // `MailboxClosed` is a real answer here and not a failure: it is what a
        // hop gets when the actor it is calling was aborted out from under it,
        // which is the recovery path this file also measures.
        other
            .hop(hops - 1)
            .await
            .map_or(u64::MAX, |depth| depth + 1)
    }

    async fn ping(&mut self, _cx: Caller<'_>) -> u64 {
        self.bumps
    }

    async fn self_read(&mut self, _cx: Caller<'_>) -> u64 {
        let me = self.me.get().expect("wired").clone();
        me.with_state(|state: &Peer| state.bumps)
            .await
            .unwrap_or(u64::MAX)
    }

    async fn wait(&mut self, _cx: Caller<'_>) -> u64 {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.gate.notified().await;
        self.bumps
    }

    async fn bump(&mut self, _cx: Caller<'_>) {
        self.bumps += 1;
    }

    async fn parked_hop(&mut self, _cx: Caller<'_>, reply: Reply<u64>, hops: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        let other = self.other.get().expect("wired").clone();
        // Off the turn's task, which is the half that matters: the turn returns
        // now, so the loop is polling again by the time the peer calls back.
        tokio::spawn(async move {
            let depth = other
                .hop(hops - 1)
                .await
                .map_or(u64::MAX, |depth| depth + 1);
            deferred.send(depth);
        });
        replied
    }

    async fn parked_blocking_hop(
        &mut self,
        _cx: Caller<'_>,
        reply: Reply<u64>,
        hops: u64,
    ) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        let other = self.other.get().expect("wired").clone();
        // Parked, and then awaited anyway. The turn has not returned, so the
        // loop is not polling, so nothing about the park has helped.
        let depth = other
            .hop(hops - 1)
            .await
            .map_or(u64::MAX, |depth| depth + 1);
        deferred.send(depth);
        replied
    }
}

/// Long enough that a machine under load does not call a live actor hung, short
/// enough that a genuinely hung one costs a test run a second and not a session.
const HANG: Duration = Duration::from_millis(750);

/// For the calls that must finish. Generous, because a failure here is a real
/// failure and not a slow machine.
const SETTLE: Duration = Duration::from_secs(5);

/// Two wired actors, and the gate and counter of the first.
///
/// # The wiring is a strong reference cycle, and that is an exemption
///
/// Each `Peer` holds an `Arc<OnceLock<Handle>>` for its peer **and** for itself,
/// so the `mpsc::Sender`s outlive every handle a test holds. These actors
/// therefore cannot be ended by dropping handles — only `abort`, `stop` or
/// runtime teardown ends them, and two tests below deliberately leave a live
/// actor to teardown.
///
/// Named rather than left silent, because this file warns about leaked busy
/// actors a few tests down and the exemption would otherwise read as an
/// oversight. It is safe here for one reason: `#[tokio::test]` builds a runtime
/// per test and drops it at the end, so nothing outlives the test that made it.
/// A cycle wired this way in a **shared** runtime would leak both actors for the
/// life of the process.
fn ring_of_two() -> (
    ring::Handle<Peer>,
    ring::Handle<Peer>,
    Arc<Notify>,
    Arc<AtomicUsize>,
) {
    let first = Arc::new(OnceLock::new());
    let second = Arc::new(OnceLock::new());
    let gate = Arc::new(Notify::new());
    let entered = Arc::new(AtomicUsize::new(0));

    let a = ring::actor(Peer {
        other: second.clone(),
        me: first.clone(),
        entered: entered.clone(),
        gate: gate.clone(),
        bumps: 0,
    });
    let b = ring::actor(Peer {
        other: first.clone(),
        me: second.clone(),
        entered: Arc::new(AtomicUsize::new(0)),
        gate: Arc::new(Notify::new()),
        bumps: 0,
    });

    first.set(a.clone()).expect("set once");
    second.set(b.clone()).expect("set once");
    (a, b, gate, entered)
}

/// Wait for `a` to be inside its `wait` turn, by looking at the counter the
/// handler bumps on entry rather than by sleeping.
async fn inside_a_turn(entered: &AtomicUsize) {
    for _ in 0..10_000 {
        if entered.load(Ordering::SeqCst) > 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the actor never entered its turn");
}

// ───────────────────────────────────────────── the permit a running turn leaves

/// The fact everything below rests on: a turn runs with its own permit already
/// released, so **exactly one** message fits behind a running turn.
///
/// This is `macros.rs`'s self-`tell` paragraph turned into an observation. The
/// first `bump` completes while the actor is wedged inside `wait` — the mailbox
/// took it — and the second does not, because `LOCAL_MAILBOX_DEPTH` is 1 and the
/// first one is now sitting in it. `tell` is the only method that can show this:
/// an `rpc` would block on the answer either way and could not tell the two
/// apart.
///
/// Made red by `scripts/break-once.sh`, replacing `local.rs`'s
/// `const LOCAL_MAILBOX_DEPTH: usize = 1;` with `= 2;`. Watched: the second
/// `bump` then completes too and the test fails at
/// "a second message must not fit: LOCAL_MAILBOX_DEPTH is 1". That is the right
/// red — it is the depth, and not the loop's release of the permit, that this
/// assertion pins.
#[tokio::test]
async fn a_running_turn_leaves_exactly_one_free_permit_behind_it() {
    let (a, _b, gate, entered) = ring_of_two();

    let waiting = {
        let a = a.clone();
        tokio::spawn(async move { a.wait().await })
    };
    inside_a_turn(&entered).await;

    n0_future::time::timeout(SETTLE, a.bump())
        .await
        .expect("the permit the loop released when it took `wait` is free")
        .expect("the actor is running");

    assert!(
        n0_future::time::timeout(HANG, a.bump()).await.is_err(),
        "a second message must not fit: LOCAL_MAILBOX_DEPTH is 1"
    );

    gate.notify_one();
    n0_future::time::timeout(SETTLE, waiting)
        .await
        .expect("the gate was released")
        .expect("the caller is not itself panicking")
        .expect("the actor is running");
}

// ──────────────────────────────────────────────────────── the two-actor cycle

/// The claim: **A → B → A hangs, on an idle pair, with no contention.**
///
/// A's handler calls B, B's handler calls A, and both callers block for ever.
/// Both sends succeed — the previous test is why: each loop released its permit
/// when it took the message it is running, so B's call to A finds the mailbox
/// open. What neither call gets is an *answer*, because the only task that can
/// produce one is the task that is blocked waiting for it.
///
/// The second half of the test is the part a reader will not predict: **neither
/// actor answers anything else either**, and they are stuck two different ways.
/// A's one free permit was consumed by B's call, so a further call to A blocks in
/// `send`. B's permit is still free, so a further call to B is accepted and then
/// blocks on the answer. One deadlock, two shapes, and the crate names neither.
///
/// That asymmetry is **asserted and not merely stated**, with `tell` rather than
/// `rpc`: a `tell` resolves when the send completes, so it separates a full
/// mailbox from a busy loop. A ping cannot — it times out identically at both
/// ends, which is why the two ping assertions claim only that a loop is not
/// polling. The `tell`s therefore run **first**: a ping to B takes B's free permit
/// on its way to timing out, so the permit assertions have to happen before any
/// ping or they measure the pings instead.
///
/// Reddened by making the cycle not a cycle: `scripts/break-once.sh` on this file,
/// searching for `hop`'s early-return line — pass it **with its eight spaces of
/// indentation** — `if hops == 0 {`, and replacing it with `if hops <= 1 {`, so the
/// chain stops one hop early and never closes the second edge. Watched:
/// "A -> B -> A must hang", in 0.00s, because a chain that stops answers before the
/// timeout can start. That is the intended red, and it is why the break is on the
/// hop count rather than on the crate: the hang is the *absence* of a mechanism, so
/// no crate line can be edited to remove it.
///
/// # The convention, and it is narrower twice over
///
/// `break-once.sh` greps and counts **only its search literal**; the replacement is
/// never searched (`scripts/break-once.sh`: `grep -qF -- "$from"`, then
/// `assert text.count(old) == 1`). So the replacement is always safe to quote, and
/// a one-line search paired with a multi-line replacement is legal — the harness
/// never cares how many lines the edit *site* spans, only the search literal.
///
/// **And the search literal is safe to quote here too**, which is the second
/// narrowing and the one that took these records off the ungated side. A record in
/// the file it breaks does not collide with its own quote provided the search
/// carries the code's **indentation**: this quote sits unindented inside a `///`
/// line while the code carries eight spaces, so the indented literal is unique and
/// the record's copy is not a second occurrence.
///
/// Two earlier versions of this paragraph were wrong in the same direction, and both
/// were reasoned rather than measured. The first said "describe the edit", which
/// left a reader inventing replacement code that had to compile. The second said a
/// search literal "must not appear in the file it targets" — true of the **bare**
/// literal, false of the **indented** one, so it described what was quotable all
/// along. The counts are in `.mise/tasks/anchors`, which registers the indented form
/// and re-checks it on every run.
#[tokio::test]
async fn a_two_actor_cycle_hangs_and_wedges_both_actors() {
    let (a, b, _gate, _entered) = ring_of_two();

    let mut hopping = {
        let a = a.clone();
        tokio::spawn(async move { a.hop(2).await })
    };

    assert!(
        n0_future::time::timeout(HANG, &mut hopping).await.is_err(),
        "A -> B -> A must hang"
    );
    // The asymmetry, asserted with the one method that can see it: a `tell`
    // resolves when the **send** completes rather than when the loop runs it, so it
    // separates a full mailbox from a busy loop. A's one free permit was consumed
    // by B's call, so this blocks in `send`; B's is still free, so the same call to
    // B is accepted.
    //
    // **This has to come before the pings, and that is not a style choice.** A
    // `ping` to B succeeds at the *send* — B's permit is free — and only then times
    // out waiting for the answer, so it leaves B's mailbox full. Asserting the
    // permits afterwards measures the pings instead of the deadlock: written that
    // way round, `b.bump()` blocks and fails with
    // "B's permit is still free, so the send completes: Elapsed(())".
    assert!(
        n0_future::time::timeout(HANG, a.bump()).await.is_err(),
        "A's mailbox is full: B's call took the one permit A released"
    );
    n0_future::time::timeout(SETTLE, b.bump())
        .await
        .expect("B's permit is still free, so the send completes")
        .expect("B is running, wedged but not gone");

    // And neither loop is polling. That is all a ping can say — it times out at
    // both ends, and cannot tell a full mailbox from a busy loop.
    assert!(
        n0_future::time::timeout(HANG, a.ping()).await.is_err(),
        "A is inside the turn that is waiting on B"
    );
    assert!(
        n0_future::time::timeout(HANG, b.ping()).await.is_err(),
        "B is inside the turn that is waiting on A"
    );

    // Not hygiene. A wedged pair holds two tasks that outlive this test's
    // runtime shutdown otherwise, and a leaked busy actor is the failure mode
    // this repo has paid for once already.
    a.abort();
    b.abort();
}

/// The recovery, and the word for it is not "recovers both".
///
/// `abort` on **one** actor frees the other: A's task dies, its queued message
/// from B is dropped, that drop closes B's reply channel, B's handler gets
/// `MailboxClosed` and returns, and B's loop is polling again. So B answers
/// `ping` afterwards — it is a live actor with its state intact.
///
/// A is not recovered. It is destroyed, which is what `abort` documents: "the
/// state is **destroyed**". The original caller is told `MailboxClosed`. Naming
/// this as "recovers both" would be the crate claiming a repair it does not
/// perform, so the guard asserts the asymmetry rather than the phrase.
///
/// Made red by `scripts/break-once.sh`, replacing `LocalActor::abort`'s
/// `self.task.abort();` with `()`. Watched: "the caller of an aborted actor is
/// told, not left hanging: Elapsed(())" — which is one assertion *earlier* than
/// the one this test is named for. Worth recording rather than tidying: with
/// nothing aborted the cycle simply stays wedged, so the first thing to fail is
/// the caller still hanging, and B's freedom is never reached to be checked.
#[tokio::test]
async fn aborting_one_actor_of_a_cycle_frees_the_other_and_destroys_it() {
    let (a, b, _gate, _entered) = ring_of_two();

    let mut hopping = {
        let a = a.clone();
        tokio::spawn(async move { a.hop(2).await })
    };
    // Let the cycle close before aborting it, or this measures a race instead.
    // Waiting on the call itself and not on `b.ping()`: an un-polled `spawn`
    // leaves B idle, so a ping answers at once and proves nothing. This assertion
    // failed exactly that way when it was written the other way round.
    assert!(
        n0_future::time::timeout(HANG, &mut hopping).await.is_err(),
        "the cycle must close before the abort means anything"
    );

    a.abort();

    let answer = n0_future::time::timeout(SETTLE, hopping)
        .await
        .expect("the caller of an aborted actor is told, not left hanging")
        .expect("the caller is not itself panicking");
    assert!(
        answer.is_err(),
        "A was destroyed, so its caller gets MailboxClosed"
    );

    n0_future::time::timeout(SETTLE, b.ping())
        .await
        .expect("aborting A must free B")
        .expect("B is still running: only A was aborted");
    assert!(!a.is_running(), "A was aborted");
    assert!(b.is_running(), "B was not");

    // The other half of "abort is the recovery", and the half `ask`'s
    // documentation now leans on: a wedged actor's `ended` never resolves,
    // because only its loop fills it. Aborting the task drops the sender, and
    // that is what makes this answer at all.
    //
    // **The timeout is the whole of the finding.** The variant is near-tautological
    // — `ended`'s dropped-sender arm can only be `Vanished`, and the `Floor` writes
    // `Fate` rather than this watch — so the `matches!` below is a change-detector
    // for that arm and not evidence about `abort`.
    let ended = n0_future::time::timeout(SETTLE, a.ended())
        .await
        .expect("aborting is what makes a wedged actor's `ended` resolve");
    assert!(
        matches!(ended, Ended::Vanished),
        "an aborted task filled nothing, so the reason is `Vanished`, not {ended:?}"
    );
}

// ────────────────────────────────────────────────────── the read inside a turn

/// A read from inside a turn is the same deadlock, through the public read seam.
///
/// `Handle::with_state` is `LocalActor::read`, and `read` takes a mailbox permit
/// like any other message. On an idle actor the send therefore succeeds — it takes
/// the one free permit the running turn left — and then waits for an answer that
/// only the loop can produce. **That step is inherited, not asserted here:** the
/// single timeout below cannot separate a send that completed from one still
/// blocked, and what establishes the free permit is the first test in this file.
/// The permit is what the crate's self-`tell`
/// paragraph is about; here it is not the binding constraint, and the hang is
/// unconditional rather than load-dependent.
///
/// Made red by `scripts/break-once.sh` on this file, searching for `self_read`'s
/// read line — pass it **with its eight spaces of indentation** —
/// `me.with_state(|state: &Peer| state.bumps)`, and replacing it with
/// `std::future::ready(Ok::<u64, keylink::MailboxClosed>({ let _ = me; 0 }))` — a
/// read that never enters the mailbox, keeping the handle binding used so the break
/// is a behaviour change and not a dead-code build failure. Watched: "a read from
/// inside a turn must hang", in 0.00s.
///
/// Both halves quoted, under the indentation rule the two-actor record above states.
/// The search line is the **first** line of a three-line chain and is a whole line
/// in its own right, which is what makes it quotable; the `.await` and
/// `.unwrap_or` below it are `rustfmt`'s to move, and if it ever reflows this anchor
/// goes to count 0 — loud, and what `.mise/tasks/anchors` exists to catch.
#[tokio::test]
async fn a_read_from_inside_a_turn_is_a_self_deadlock() {
    let (a, _b, _gate, _entered) = ring_of_two();

    let mut reading = {
        let a = a.clone();
        tokio::spawn(async move { a.self_read().await })
    };
    assert!(
        n0_future::time::timeout(HANG, &mut reading).await.is_err(),
        "a read from inside a turn must hang"
    );

    a.abort();
}

// ─────────────────────────────────────────────────────────── the cycle-breaker

/// The claim: parking the reply and **leaving the turn** breaks the cycle.
///
/// A's handler parks its reply, hands the hop to a spawned task and returns. The
/// loop is polling again immediately, so when B calls back, A takes the message
/// and answers it. The full A → B → A chain then completes and the *original*
/// caller gets the depth, from a turn that ended long before the answer existed.
///
/// The mechanism is worth stating exactly, because it is not the permit: the
/// permit was free in the hanging case too. What `Deferred` changes is that the
/// loop goes back to **polling**, so the peer's message is taken and run.
///
/// **Reddened, but not on its distinguishing claim**, and `defer.rs` already has
/// the vocabulary for that: `scripts/break-once.sh` replacing `Deferred::send`'s
/// `Verdict::Committed => reply.answer(Answered::Ran(value)),` with
/// `Verdict::Committed => drop((reply, value)),`. Watched: "the actor is running:
/// MailboxClosed". That proves the parked answer is load-bearing and proves
/// nothing about the *cycle*, because it reddens every `#[defer]` test in the
/// tree.
///
/// The break that would have isolated the cycle does not exist. `Reply::later`'s
/// `Replied(Settled::Later)` → `Replied(Settled::Now)` was tried first and does
/// not compile — `Settled::Now` is `Now(Reply<T>, T)`, so the arm needs the two
/// values `later` exists precisely to not have. What isolates the distinguishing
/// claim here is not a break but the test below, which differs from this one in
/// the `tokio::spawn` alone and hangs.
#[tokio::test]
async fn a_parked_reply_that_leaves_the_turn_breaks_the_cycle() {
    let (a, b, _gate, _entered) = ring_of_two();

    let hopping = {
        let a = a.clone();
        tokio::spawn(async move { a.parked_hop(2).await })
    };

    let depth = n0_future::time::timeout(SETTLE, hopping)
        .await
        .expect("a parked reply must break the cycle rather than hang")
        .expect("the caller is not itself panicking")
        .expect("the actor is running");
    assert_eq!(depth, 2, "A -> B -> A ran to the end and came back");

    // Both are live: nothing was aborted and nothing is wedged.
    n0_future::time::timeout(SETTLE, a.ping())
        .await
        .expect("A is polling")
        .expect("A is running");
    n0_future::time::timeout(SETTLE, b.ping())
        .await
        .expect("B is polling")
        .expect("B is running");
}

/// The control, and the reason the sentence has to say "leaves the turn".
///
/// This handler parks its reply exactly as the one above does, and then awaits
/// the hop **without returning**. It hangs. So `Reply::later` is not the
/// cycle-breaker by itself — returning from the turn is, and `later` is what
/// makes returning possible while a caller is still owed an answer.
///
/// Guarded by construction rather than by a break: the two tests differ in one
/// thing, the `tokio::spawn` around the hop, and they disagree. Reddening this
/// one means deleting that difference, which is the other test.
#[tokio::test]
async fn a_parked_reply_that_stays_in_the_turn_does_not_break_the_cycle() {
    let (a, _b, _gate, _entered) = ring_of_two();

    let mut hopping = {
        let a = a.clone();
        tokio::spawn(async move { a.parked_blocking_hop(2).await })
    };
    assert!(
        n0_future::time::timeout(HANG, &mut hopping).await.is_err(),
        "parking the reply without leaving the turn must still hang"
    );

    a.abort();
}

// ─────────────────────────────────────────── the cycle a policy already breaks

keylink::service! {
    service Room;

    /// Publish `times` items from inside a turn, to a topic this actor
    /// subscribes to and is therefore not reading. Answers with how many
    /// publishes dropped a subscriber.
    rpc Shout(times: u64) -> u64;
}

/// An actor subscribed to its own topic, holding the [`Subscription`] in its
/// state and **never polling it** — which is precisely what an actor inside a
/// turn is.
struct Chatter {
    topic: Topic<u64>,
    _feed: Subscription<u64>,
}

impl room::Actor for Chatter {
    async fn shout(&mut self, _cx: Caller<'_>, times: u64) -> u64 {
        let mut dropped = 0;
        for item in 0..times {
            dropped += self.topic.publish(item).await.dropped as u64;
        }
        dropped
    }
}

fn chatter(overflow: Overflow) -> room::Handle<Chatter> {
    let topic = Topic::new(overflow);
    let feed = topic.subscribe();
    room::actor(Chatter { topic, _feed: feed })
}

/// `Overflow::DropSubscriber` is not a style preference. It is what makes a
/// publish safe from inside a turn, and this is the pair that says so.
///
/// `topic.rs` already states it — "`Overflow::DropSubscriber` never awaits — it
/// completes without yielding, which is worth knowing for a caller that
/// publishes from inside a turn" — and states it as prose. This is the guard.
///
/// `IN_FLIGHT` is 1, so the first item fits and the second finds the subscriber
/// full. Under this policy that unsubscribes the reader and the publish returns;
/// nothing awaits, so no edge of the cycle can block.
///
/// Made red by `scripts/break-once.sh`, replacing `Topic::publish`'s
/// `Overflow::DropSubscriber => self.publish_dropping(item),` with
/// `Overflow::DropSubscriber => self.publish_blocking(item).await,` — the policy
/// swapped for the one that awaits. Watched: "a dropping publish must not hang:
/// nothing in it awaits: Elapsed(())".
#[tokio::test]
async fn publishing_into_your_own_topic_completes_under_drop_subscriber() {
    let room = chatter(Overflow::DropSubscriber);

    let dropped = n0_future::time::timeout(SETTLE, room.shout(2))
        .await
        .expect("a dropping publish must not hang: nothing in it awaits")
        .expect("the actor is running");
    assert_eq!(
        dropped, 1,
        "IN_FLIGHT is 1: the first item fits, the second unsubscribes the reader"
    );

    // And the actor is still taking messages, which is the half that makes this
    // a remedy rather than merely a different failure.
    n0_future::time::timeout(SETTLE, room.shout(1))
        .await
        .expect("still polling")
        .expect("still running");
}

/// The other half of the pair, and the one the crate already documents: under
/// `Overflow::BlockPublisher` the same handler hangs.
///
/// Same actor, same topic, same two publishes. The only difference is the
/// policy, which is what makes the policy the safety property rather than a
/// preference about throughput.
///
/// Made red by `scripts/break-once.sh`, replacing `Topic::publish`'s
/// `Overflow::BlockPublisher => self.publish_blocking(item).await,` with
/// `Overflow::BlockPublisher => self.publish_dropping(item),`. Watched: "a
/// blocking publish into your own topic must hang".
#[tokio::test]
async fn publishing_into_your_own_topic_hangs_under_block_publisher() {
    let room = chatter(Overflow::BlockPublisher);

    let mut shouting = {
        let room = room.clone();
        tokio::spawn(async move { room.shout(2).await })
    };
    assert!(
        n0_future::time::timeout(HANG, &mut shouting).await.is_err(),
        "a blocking publish into your own topic must hang"
    );

    room.abort();
}
