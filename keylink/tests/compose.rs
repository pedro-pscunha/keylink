//! The four compositions, from outside the crate, with the backpressure claim of
//! each one as an assertion rather than a paragraph.
//!
//! | the claim | the test |
//! |---|---|
//! | a publisher fans out to N subscribers, each on its own `feed` | `a_publisher_fans_out_to_three_subscribers_each_on_its_own_feed` |
//! | a topic keeps no history, so a late subscriber is not a buffer | `a_topic_with_nobody_listening_drops_the_item_rather_than_keeping_it` |
//! | a slow subscriber under `DropSubscriber` is unsubscribed and its actor ends **naming the feed** | `a_slow_subscriber_is_unsubscribed_and_its_actor_ends_naming_the_feed` |
//! | a slow subscriber under `BlockPublisher` **stops the publisher** instead of growing a buffer | `a_slow_subscriber_blocks_the_publisher_instead_of_growing_a_buffer` |
//! | a pool spreads turns, and its report agrees with the actor that handled each | `a_pool_spreads_turns_and_each_member_says_which_one_it_is` |
//! | every clone of a pool shares one rotation | `two_clones_of_a_pool_share_one_rotation` |
//! | a delayed run lands after the delay and not before | `a_delayed_run_lands_after_the_delay_and_not_before` |
//! | a repeating run is never re-entered and never queues the ticks it missed | `a_repeating_schedule_never_overlaps_and_never_queues_the_ticks_it_missed` |
//! | a dropped schedule cancels; a detached one does not | `a_dropped_schedule_runs_nothing`, `a_detached_schedule_outlives_its_handle` |
//! | a named actor is reachable by name and unregistered when it ends | `a_named_actor_is_reachable_by_name_and_unregistered_when_it_ends` |
//! | a replacement is not evicted by the ending of what it replaced | `the_ending_of_a_displaced_actor_does_not_evict_the_one_that_replaced_it` |
//!
//! # Two ways time is kept out of the assertions, and they are different ways
//!
//! **The topic tests read no clock at all.** They borrow `tests/feeds.rs`'s
//! device: a `hold` method that parks the actor inside one turn, so "a slow
//! subscriber" is a state the test *constructs* and then releases, rather than a
//! state it waits for and hopes to catch. Every count below is therefore exact.
//! A slow subscriber is precisely one that is inside a turn when the next item
//! arrives, which is the same definition the mailbox already uses.
//!
//! **The scheduler tests pause the clock.** They have no choice — a schedule is
//! *defined* by elapsed time — so they run on
//! `#[tokio::test(start_paused = true)]`. No assertion here waits for real time
//! to pass, and none compares two real instants.
//!
//! They move the clock with **`tokio::time::sleep`, never
//! `tokio::time::advance`**, and that is not a style preference: two of these
//! guards were written with `advance` first and both were **vacuous**. `advance`
//! jumps the clock and then yields once, and a `yield_now` leaves the test task
//! immediately runnable — so the runtime never *parks*, the timer wheel is never
//! turned, and a spawned schedule's `sleep` does not fire at all. The broken
//! versions of `after` and `every` (no delay, and a spawn per period) both passed.
//!
//! A `sleep` in the test parks it. The paused clock then auto-advances to the
//! **earliest** pending deadline, which is either the test's own or the
//! schedule's, so a schedule that should not have fired yet demonstrably has not,
//! and one that should have, has. That is what makes "nine more ticks, if they
//! were queued" a real count rather than a hope.
//!
//! # And one place a bounded spin is used on purpose
//!
//! [`Names`] unregisters from a spawned task, so "the name is gone" becomes true
//! one scheduling turn after the actor ended and there is nothing to await for it
//! — the registry deliberately publishes no signal of its own. [`settles`] yields
//! up to [`SPINS`] times waiting for a condition and then **fails**; [`spun`]
//! yields that many times unconditionally, for the claims that something must
//! *not* have happened. Neither reads a clock, and neither can hang: a broken
//! registry fails the assertion instead of blocking the suite.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use keylink::compose::{after, every, Fanout, Names, Overflow, Pool, Subscription, Topic};
use keylink::{ActorId, Caller, Ended};
use n0_future::time::Duration;
use tokio::sync::{mpsc, oneshot};

// The subscriber under test: it takes fills on a feed, and it can be parked
// inside a turn so that "slow" is constructed rather than awaited.
keylink::service! {
    service Desk;

    /// Holds the loop inside one turn until the test releases it. While the loop
    /// is in here it is not polling its feed, which is exactly what a slow
    /// subscriber is.
    rpc Hold() -> u64;
    /// The feed a [`Topic`] is wired to.
    feed Fills(fill: u64);
}

// A pool member and a registry entry, which want a state that can say which one
// it is and nothing else.
//
// `u64` and not `usize`: a wire type needs `postcard_schema::Schema` and `usize`
// has none, on purpose — its width is the machine's, and two peers need not agree
// about that. `Member::index()` is a `usize` because it is an index into memory,
// so the two are converted at the boundary rather than one of them being wrong.
keylink::service! {
    service Worker;

    /// Answers with this member's own number, so the pool's claim about who
    /// handled a turn can be checked against the actor's.
    rpc Whoami() -> u64;
}

/// How many cooperative yields a bounded spin gets. Large enough that a spawned
/// task has had every chance to run, small enough that a failure is a failure
/// rather than a hang.
const SPINS: usize = 256;

/// Yield until `check` holds. `false` means it never did, which is a failed
/// assertion at the call site rather than a blocked test.
async fn settles(mut check: impl FnMut() -> bool) -> bool {
    for _ in 0..SPINS {
        if check() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

/// Yield [`SPINS`] times whatever happens, for a claim that something must **not**
/// happen: it gives the task that would have done it every opportunity first.
async fn spun() {
    for _ in 0..SPINS {
        tokio::task::yield_now().await;
    }
}

// ───────────────────────────────────────────────────────────── the subscriber

struct Desk {
    /// Every fill this desk handled, so the test can await an arrival rather than
    /// sleep until it has probably happened.
    seen: mpsc::UnboundedSender<u64>,
    /// Sent the moment the loop enters `hold`, so a test knows the actor is
    /// parked inside a turn rather than merely soon to be.
    entered: Option<oneshot::Sender<()>>,
    /// Awaited inside `hold`. Dropping the sender releases it.
    gate: Option<oneshot::Receiver<()>>,
}

impl desk::Actor for Desk {
    async fn hold(&mut self, _cx: Caller<'_>) -> u64 {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        if let Some(gate) = self.gate.take() {
            let _ = gate.await;
        }
        0
    }

    async fn on_fills(&mut self, fill: u64) {
        let _ = self.seen.send(fill);
    }
}

/// The two ends of the hold, kept together so a test cannot release a loop it
/// never waited for.
struct Gate {
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

impl Gate {
    /// Block until the actor is inside `hold`, and hand back the release.
    async fn held(self) -> oneshot::Sender<()> {
        self.entered.await.expect("the actor enters `hold`");
        self.release
    }
}

/// One desk, wired to one subscription.
fn desk(fills: Subscription<u64>) -> (desk::Handle<Desk>, mpsc::UnboundedReceiver<u64>, Gate) {
    let (seen, heard) = mpsc::unbounded_channel();
    let (entered, entered_rx) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    let actor = desk::actor_wired(
        Desk {
            seen,
            entered: Some(entered),
            gate: Some(gate),
        },
        Vec::new(),
        desk::Feeds::new(fills),
    );
    (
        actor,
        heard,
        Gate {
            entered: entered_rx,
            release,
        },
    )
}

// ────────────────────────────────────────────────────────────── the pool member

struct Worker {
    me: u64,
}

impl worker::Actor for Worker {
    async fn whoami(&mut self, _cx: Caller<'_>) -> u64 {
        self.me
    }
}

/// A [`Member`](keylink::compose::Member)'s index as the number a `Worker`
/// reports. Fallible in principle and not in practice — a pool of more than
/// `u64::MAX` members is not a thing — and written as a conversion rather than an
/// `as` cast so that the impossible case is named instead of truncated.
fn numbered(member: keylink::compose::Member) -> u64 {
    u64::try_from(member.index()).expect("a pool smaller than u64::MAX")
}

// ─────────────────────────────────────────────────────────────────── the topic

/// Made red by `scripts/break-once.sh`, replacing `publish_blocking`'s
/// `for one in to {` with `for one in to.into_iter().take(1) {` — a fan-out that
/// reaches the first subscriber and stops, which is the shape of every hand-rolled
/// one that forgot a loop. Watched: this fails at the first publish, "every
/// subscriber takes every item", left `Fanout { delivered: 1, dropped: 0, gone: 0 }`
/// against right `Fanout { delivered: 3, dropped: 0, gone: 0 }`.
#[tokio::test]
async fn a_publisher_fans_out_to_three_subscribers_each_on_its_own_feed() {
    let topic = Topic::new(Overflow::BlockPublisher);

    // Three actors, three subscriptions, three feeds. Nothing about the fan-out
    // reaches the service declaration: `Feeds::new` takes a `Stream`, and a
    // `Subscription` is one.
    let mut desks = Vec::new();
    for _ in 0..3 {
        desks.push(desk(topic.subscribe()));
    }
    assert_eq!(topic.subscribers(), 3);

    for item in [1_u64, 2] {
        assert_eq!(
            topic.publish(item).await,
            Fanout {
                delivered: 3,
                dropped: 0,
                gone: 0
            },
            "every subscriber takes every item"
        );
    }

    for (_actor, heard, _gate) in &mut desks {
        assert_eq!(heard.recv().await, Some(1));
        assert_eq!(heard.recv().await, Some(2), "and in order");
    }
}

/// **This one's own claim cannot be made red, and that is recorded rather than
/// left to be rediscovered.** "A topic keeps no history" denies a buffer that does
/// not exist: nothing in `topic.rs` holds a past item, so no edit turns `subscribe`
/// into a replay — the defect would have to be *added*, and an addition is not a
/// break. Wave 7 (`9a7fd1d`) reached the same verdict when this was written, and
/// called it a regression pin rather than a guard.
///
/// What *is* reddenable is the counting half, and it was watched:
/// `scripts/break-once.sh` replacing `publish_blocking`'s
/// `Ok(()) => delivered += 1,` with `delivered += 2,` fails the second assertion,
/// left `Fanout { delivered: 2, dropped: 0, gone: 0 }` against right
/// `Fanout { delivered: 1, dropped: 0, gone: 0 }`. That proves the assertions are
/// wired to the `Fanout` this publish produced. It does not prove the no-history
/// property, and nothing can.
#[tokio::test]
async fn a_topic_with_nobody_listening_drops_the_item_rather_than_keeping_it() {
    let topic = Topic::new(Overflow::BlockPublisher);

    assert_eq!(
        topic.publish(1_u64).await,
        Fanout {
            delivered: 0,
            dropped: 0,
            gone: 0
        },
        "no subscribers is not an error"
    );

    // The item published before anybody subscribed is gone. A topic is not a
    // queue and holds no history: if it did, a subscriber joining an hour later
    // would be handed an hour of events as its first turn.
    let (_actor, mut heard, _gate) = desk(topic.subscribe());
    assert_eq!(
        topic.publish(2_u64).await,
        Fanout {
            delivered: 1,
            dropped: 0,
            gone: 0
        }
    );
    assert_eq!(heard.recv().await, Some(2), "the first item it can see");
}

/// Red-tested in Wave 7 (`9a7fd1d`), and the output is pasted in that commit
/// rather than here: folding `topic.rs`'s `const IN_FLIGHT: usize = 1` to `8` —
/// the one number the whole no-third-policy argument rests on — turns this red at
/// the second publish, left `Fanout { delivered: 2, dropped: 0, gone: 0 }` against
/// right `Fanout { delivered: 1, dropped: 1, gone: 0 }`. A subscriber holding eight
/// unread items is not slow by this module's definition, so nothing is dropped,
/// nothing is unsubscribed, and no actor ends naming a feed.
#[tokio::test]
async fn a_slow_subscriber_is_unsubscribed_and_its_actor_ends_naming_the_feed() {
    let topic = Topic::new(Overflow::DropSubscriber);
    let (slow, mut slow_heard, slow_gate) = desk(topic.subscribe());
    let (fast, mut fast_heard, _fast_gate) = desk(topic.subscribe());

    // Park the slow desk inside one turn. From here it is not polling its feed.
    let holding = tokio::spawn({
        let slow = slow.clone();
        async move {
            let _ = slow.hold().await;
        }
    });
    let release = slow_gate.held().await;

    // One item fits the slow desk's single slot, and the fast desk takes it.
    assert_eq!(
        topic.publish(1_u64).await,
        Fanout {
            delivered: 2,
            dropped: 0,
            gone: 0
        }
    );
    assert_eq!(fast_heard.recv().await, Some(1));

    // The second finds that slot full. THIS is the decision: the slow subscriber
    // is unsubscribed rather than given a second slot, and the publisher is not
    // delayed by it.
    assert_eq!(
        topic.publish(2_u64).await,
        Fanout {
            delivered: 1,
            dropped: 1,
            gone: 0
        },
        "the slow subscriber is dropped, not buffered"
    );
    assert_eq!(fast_heard.recv().await, Some(2));
    assert_eq!(topic.subscribers(), 1, "and it is gone from the topic");

    // Released, the slow desk drains the one item it did have — nothing was
    // thrown away that it had already been handed — and then its feed ends.
    drop(release);
    holding.await.expect("the held call returns");
    assert_eq!(slow_heard.recv().await, Some(1));
    let ending = slow.ended().await;
    assert!(
        matches!(ending, Ended::Feed { name: "Fills" }),
        "a dropped subscriber ends as its feed ending, loudly, and names it: {ending:?}"
    );

    // And the topic carries on for whoever kept up.
    assert_eq!(
        topic.publish(3_u64).await,
        Fanout {
            delivered: 1,
            dropped: 0,
            gone: 0
        }
    );
    assert_eq!(fast_heard.recv().await, Some(3));
    drop(fast);
}

/// The same break as its `DropSubscriber` sibling, watched against this half.
/// Wave 7 recorded that `IN_FLIGHT` 1 → 8 turns both policies' guards red at once
/// but pasted only the other one's output; this is the missing half.
/// `scripts/break-once.sh` replacing `const IN_FLIGHT: usize = 1;` with `= 8;`
/// watched: "the publisher waits for the slow subscriber rather than buffering for
/// it". With eight slots the second `publish` returns on its first poll instead of
/// parking, which is exactly the buffer this test exists to deny.
#[tokio::test]
async fn a_slow_subscriber_blocks_the_publisher_instead_of_growing_a_buffer() {
    let topic = Topic::new(Overflow::BlockPublisher);
    let (slow, mut slow_heard, slow_gate) = desk(topic.subscribe());

    let holding = tokio::spawn({
        let slow = slow.clone();
        async move {
            let _ = slow.hold().await;
        }
    });
    let release = slow_gate.held().await;

    // The one slot takes the first item.
    assert_eq!(
        topic.publish(1_u64).await,
        Fanout {
            delivered: 1,
            dropped: 0,
            gone: 0
        }
    );

    // The second cannot land, and THIS is the decision: `publish` does not
    // return. One poll is enough to reach the send — `publish_blocking` takes the
    // lock, snapshots, and awaits the first permit — so a `Pending` here is the
    // publisher waiting on the subscriber and not a scheduling artefact.
    assert!(
        n0_future::future::poll_once(topic.publish(2_u64))
            .await
            .is_none(),
        "the publisher waits for the slow subscriber rather than buffering for it"
    );

    // Released, the subscriber catches up and the publisher can move again.
    drop(release);
    holding.await.expect("the held call returns");
    assert_eq!(slow_heard.recv().await, Some(1));

    assert_eq!(
        topic.publish(3_u64).await,
        Fanout {
            delivered: 1,
            dropped: 0,
            gone: 0
        }
    );
    assert_eq!(slow_heard.recv().await, Some(3));

    // Item 2 never arrives, and that is documented rather than accidental: the
    // `publish` carrying it was cancelled by `poll_once` dropping the future, and
    // a cancelled publish delivers to the subscribers it had already reached and
    // to no others.
    assert!(
        slow.is_running(),
        "and the subscriber is still subscribed — nothing was dropped for being slow here"
    );
    assert_eq!(topic.subscribers(), 1);
}

/// Made red by `scripts/break-once.sh`, replacing `publish_blocking`'s
/// `self.lock().retain(|one| !one.is_closed());` with `let _ = gone;` — the gone
/// subscriber still counted, the entry never taken out. Watched: this fails at the
/// count rather than at the `Fanout`, left `2` against right `1`. Counting and
/// removing are two claims and this is the one a report alone cannot make.
#[tokio::test]
async fn a_subscription_dropped_is_a_subscriber_the_next_publish_prunes() {
    let topic = Topic::new(Overflow::BlockPublisher);
    let alive = topic.subscribe();
    drop(topic.subscribe());
    assert_eq!(topic.subscribers(), 2, "nothing sweeps in the background");

    let (_actor, mut heard, _gate) = desk(alive);
    assert_eq!(
        topic.publish(1_u64).await,
        Fanout {
            delivered: 1,
            dropped: 0,
            gone: 1
        },
        "the gone one is counted and removed"
    );
    assert_eq!(heard.recv().await, Some(1));
    assert_eq!(topic.subscribers(), 1);
}

// ──────────────────────────────────────────────────────────────────── the pool

/// Made red on the stronger half, which is the half worth watching:
/// `scripts/break-once.sh` replacing `pick`'s `(Member(at), &self.inner.members[at])`
/// with `(Member(at), &self.inner.members[(at + 1) % self.inner.size.get()])` — a
/// pool that reports one member and dispatches to another. The round-robin
/// assertion above **still passes** under that break; watched: the second one
/// fails, "the pool's report and the actor that answered agree", left
/// `[1, 2, 0, 1, 2, 0, 1]` against right `[0, 1, 2, 0, 1, 2, 0]`. That is the
/// defect a cursor-only test cannot see.
#[tokio::test]
async fn a_pool_spreads_turns_and_each_member_says_which_one_it_is() {
    let pool = Pool::from_fn(NonZeroUsize::new(3).expect("three"), |member| {
        worker::actor(Worker {
            me: numbered(member),
        })
    });
    assert_eq!(pool.size().get(), 3);
    assert_eq!(pool.members().len(), 3);

    let mut picked = Vec::new();
    let mut handled = Vec::new();
    for _ in 0..7 {
        let (who, actor) = pool.pick();
        picked.push(numbered(who));
        handled.push(actor.whoami().await.expect("the member is running"));
    }

    assert_eq!(
        picked,
        vec![0, 1, 2, 0, 1, 2, 0],
        "round-robin, one step per pick"
    );
    // The stronger half: the pool's claim about who handled a turn is checked
    // against the actor's own claim, so a rotation that reported one member and
    // dispatched to another would fail here rather than pass a cursor assertion.
    assert_eq!(
        handled, picked,
        "the pool's report and the actor that answered agree"
    );
}

#[tokio::test]
async fn two_clones_of_a_pool_share_one_rotation() {
    let pool = Pool::from_fn(NonZeroUsize::new(2).expect("two"), |member| {
        worker::actor(Worker {
            me: numbered(member),
        })
    });
    let other = pool.clone();

    let (first, _) = pool.pick();
    let (second, _) = other.pick();
    assert_ne!(
        first.index(),
        second.index(),
        "a clone with its own cursor would send every first request to member zero"
    );
}

// ─────────────────────────────────────────────────────────────── the scheduler

/// Wave 7's break, re-run here for the output that commit did not paste:
/// `scripts/break-once.sh` replacing `after`'s `sleep(delay).await;` with
/// `sleep(Duration::ZERO).await;`. Watched: this fails at the 4,999 ms line, "not
/// before the delay". That is the same break the `advance`-based version of this
/// test **passed**, which is the whole reason the body below sleeps.
#[tokio::test(start_paused = true)]
async fn a_delayed_run_lands_after_the_delay_and_not_before() {
    let (ran, mut heard) = mpsc::unbounded_channel();
    let job = after(Duration::from_secs(5), move || async move {
        let _ = ran.send("ran");
    });

    // One millisecond short. A `sleep` and not an `advance`: sleeping parks this
    // task, so the paused clock auto-advances to the earliest deadline — 4,999 ms,
    // ours — and the schedule's 5,000 ms one demonstrably has not fired. `advance`
    // never parks the runtime, so the schedule's timer would not have been turned
    // at all and this assertion would hold whatever `after` did.
    tokio::time::sleep(Duration::from_millis(4_999)).await;
    assert!(heard.try_recv().is_err(), "not before the delay");

    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(heard.recv().await, Some("ran"), "and then it runs");
    assert!(job.is_finished());
}

/// Red-tested in Wave 7 (`9a7fd1d`), with the output pasted there: `every`'s
/// `work().await` replaced by a spawn per period — the exact pile of pending work
/// this module's backpressure section refuses. Watched: "a run in progress must not
/// be re-entered and its missed ticks must not queue". That break **passed** against
/// the `advance`-based version of this test and fails against this one, which is why
/// the body sleeps.
#[tokio::test(start_paused = true)]
async fn a_repeating_schedule_never_overlaps_and_never_queues_the_ticks_it_missed() {
    let (tick, mut heard) = mpsc::unbounded_channel();
    let (release, gate) = oneshot::channel::<()>();
    let gate = Arc::new(Mutex::new(Some(gate)));

    let sweep = every(Duration::from_millis(100), move || {
        let tick = tick.clone();
        let gate = Arc::clone(&gate);
        async move {
            let _ = tick.send(());
            // The first run, and only the first, is held inside the work.
            let held = gate.lock().unwrap().take();
            if let Some(held) = held {
                let _ = held.await;
            }
        }
    });

    // The first run starts one period in, and parks there.
    assert_eq!(heard.recv().await, Some(()), "the first run starts");

    // A whole second at a hundred-millisecond period is nine more ticks, if a
    // schedule queues the ticks it missed. THIS is the decision: it does not.
    // Nothing is spawned per period, so there is nothing to pile up — the loop is
    // inside `work()` and has no timer pending at all.
    //
    // A `sleep` and not an `advance`, and the difference is the whole guard: this
    // parks, so a schedule that *did* re-arm every period would be auto-advanced
    // through ten of them before this line returns. `advance` would jump the clock
    // once without ever turning the wheel, and a spawn-per-period `every` passed
    // that version of this test.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        heard.try_recv().is_err(),
        "a run in progress must not be re-entered and its missed ticks must not queue"
    );

    // Once the run finishes, exactly one more follows — one period later, not
    // nine at once.
    release.send(()).expect("the run is still held");
    assert_eq!(
        heard.recv().await,
        Some(()),
        "the next run starts after the last one finished"
    );
    assert!(heard.try_recv().is_err(), "and exactly one of them");
    drop(sweep);
}

/// Made red by `scripts/break-once.sh`, replacing `Scheduled`'s `Drop` guard
/// `if !self.detached {` with `if self.detached {` — a handle that detaches on drop,
/// which is the `JoinHandle` behaviour this type exists to invert. Watched: this
/// fails with its own message, "a dropped Scheduled cancels, unlike a dropped
/// JoinHandle, which detaches".
#[tokio::test(start_paused = true)]
async fn a_dropped_schedule_runs_nothing() {
    let (ran, mut heard) = mpsc::unbounded_channel();
    drop(after(Duration::from_secs(1), move || async move {
        let _ = ran.send("ran");
    }));

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        heard.try_recv().is_err(),
        "a dropped `Scheduled` cancels, unlike a dropped `JoinHandle`, which detaches"
    );
}

/// Made red by `scripts/break-once.sh`, replacing `detach`'s
/// `self.detached = true;` with `self.detached = false;` — a `detach` that says so
/// in its name and does nothing, so `Drop` aborts anyway. Watched: left `None`
/// against right `Some("ran")`, "`detach` is the one way to say `run without me`
/// out loud".
#[tokio::test(start_paused = true)]
async fn a_detached_schedule_outlives_its_handle() {
    let (ran, mut heard) = mpsc::unbounded_channel();
    after(Duration::from_secs(1), move || async move {
        let _ = ran.send("ran");
    })
    .detach();

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        heard.recv().await,
        Some("ran"),
        "`detach` is the one way to say `run without me` out loud"
    );
}

// ───────────────────────────────────────────────────────────────── the registry

/// Made red by `scripts/break-once.sh`, replacing the eviction task's
/// `held.remove(&name);` with `held.reserve(0);` — the task still wakes on the
/// ending, still finds its own entry under its own generation, and leaves it there.
/// Watched: "an actor that ended is unregistered". That is [`settles`] failing
/// rather than blocking, which is the reason it is a bounded spin: a registry that
/// stopped evicting would otherwise hang this test instead of printing.
#[tokio::test]
async fn a_named_actor_is_reachable_by_name_and_unregistered_when_it_ends() {
    let names: Names<worker::Handle<Worker>> = Names::new();
    let ledger = ActorId::new("ledger").expect("a name");
    let actor = worker::actor(Worker { me: 7 });

    assert!(
        names
            .register(ledger.clone(), actor.clone(), actor.ended())
            .is_none(),
        "nothing was displaced"
    );
    assert_eq!(names.registered(), 1);
    assert_eq!(names.names(), vec![ledger.clone()]);

    let found = names.get(&ledger).expect("reachable by name");
    assert_eq!(found.whoami().await.expect("running"), 7);

    // The documented surprise, pinned: the registry holds a handle, so dropping
    // every handle the test holds no longer ends the actor.
    drop(actor);
    drop(found);
    let still = names.get(&ledger).expect("still registered");
    assert_eq!(
        still.whoami().await.expect("running"),
        7,
        "a registered actor outlives the caller's own handles"
    );

    // And an ending — any ending — unregisters it.
    still.stop().await;
    assert!(
        settles(|| names.get(&ledger).is_none()).await,
        "an actor that ended is unregistered"
    );
    assert_eq!(names.registered(), 0);
}

/// Red-tested in Wave 7 (`9a7fd1d`), with the output pasted there: dropping the
/// generation comparison from the eviction task lets the first incarnation's ending
/// remove the second's entry, because both are filed under one `ActorId`. Watched:
/// "an eviction that ignored the generation stamp would have removed the
/// replacement", left `0` against right `1`.
#[tokio::test]
async fn the_ending_of_a_displaced_actor_does_not_evict_the_one_that_replaced_it() {
    let names: Names<worker::Handle<Worker>> = Names::new();
    let ledger = ActorId::new("ledger").expect("a name");

    let first = worker::actor(Worker { me: 1 });
    let second = worker::actor(Worker { me: 2 });
    assert!(names
        .register(ledger.clone(), first.clone(), first.ended())
        .is_none());
    let displaced = names
        .register(ledger.clone(), second.clone(), second.ended())
        .expect("the first registration comes back");
    assert_eq!(
        displaced.whoami().await.expect("running"),
        1,
        "and it is the handle that was displaced, not the one that replaced it"
    );

    // The first actor ends. Its eviction task wakes with the name still in the
    // map — filed under the SAME `ActorId` — and must leave the second alone.
    first.stop().await;
    drop(displaced);
    spun().await;

    assert_eq!(
        names.registered(),
        1,
        "an eviction that ignored the generation stamp would have removed the replacement"
    );
    assert_eq!(
        names
            .get(&ledger)
            .expect("still registered")
            .whoami()
            .await
            .expect("running"),
        2,
        "and the name still answers to the actor that replaced the first"
    );
}

/// Made red by `scripts/break-once.sh` on the same generation stamp its sibling
/// watches, from the other side: replacing the eviction task's
/// `.is_some_and(|entry| entry.generation == generation)` with `.is_some()`. The
/// unregistered actor's task then wakes, finds *a* registration under the name —
/// the later, unrelated one — and removes it. Watched: this fails at the second
/// lookup, "untouched".
#[tokio::test]
async fn unregistering_frees_the_name_without_waiting_for_an_ending() {
    let names: Names<worker::Handle<Worker>> = Names::new();
    let ledger = ActorId::new("ledger").expect("a name");
    let actor = worker::actor(Worker { me: 3 });
    assert!(names
        .register(ledger.clone(), actor.clone(), actor.ended())
        .is_none());

    let taken = names.unregister(&ledger).expect("the handle comes back");
    assert_eq!(taken.whoami().await.expect("still running"), 3);
    assert!(
        names.get(&ledger).is_none(),
        "the name is free while the actor runs"
    );
    assert_eq!(names.registered(), 0);

    // The eviction task is still registered. It wakes, finds nothing under the
    // name, and ends — it must not remove an unrelated later registration.
    let other = worker::actor(Worker { me: 4 });
    assert!(names
        .register(ledger.clone(), other.clone(), other.ended())
        .is_none());
    actor.stop().await;
    drop(taken);
    spun().await;
    assert_eq!(
        names
            .get(&ledger)
            .expect("untouched")
            .whoami()
            .await
            .expect("running"),
        4
    );
}
