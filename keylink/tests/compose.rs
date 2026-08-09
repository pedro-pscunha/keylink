//! The five compositions, from outside the crate, with the backpressure claim of
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
//! | a member ending for good reaches the policy once, with its name and reason | `a_member_that_ends_for_good_reaches_the_policy_once` |
//! | a member's own restart never reaches the policy | `a_members_own_restart_never_reaches_the_policy` |
//! | a refusal the author marked fatal reaches the policy without a restart | `a_fatal_refusal_reaches_the_policy_without_a_restart` |
//! | `StopAll` ends every sibling and names the member that caused it | `stop_all_ends_the_group_and_names_the_member_that_caused_it` |
//! | no member's retirement waits on another's to complete | `no_members_retirement_waits_on_another` |
//! | `Forget` removes the member and leaves the others serving | `forget_removes_the_member_and_leaves_the_others_serving` |
//! | a displaced member's ending does not reach the policy as its replacement | `a_displaced_members_ending_does_not_reach_the_policy_as_its_replacement` |
//! | one wake drains every pending add | `adds_made_while_the_policy_runs_are_all_watched` |
//! | a policy that panics ends the group instead of orphaning it | `a_policy_that_panics_ends_the_group_rather_than_orphaning_it` |
//! | a group nobody holds answers its waiters instead of hanging | `a_dropped_group_ends_as_dropped_and_leaves_its_members_running` |
//! | members are reported in the order they were added | `members_are_reported_in_the_order_they_were_added` |
//! | a member asleep in a backoff is retired without waiting it out | `a_member_asleep_in_backoff_is_retired_without_waiting_out_its_backoff` |
//! | an ending that panics ends the group and names the member it was awaiting | `an_ending_that_panics_ends_the_group_as_ending_panicked` |
//! | a panicking `retire` costs its own member and not the teardown | `a_panicking_retire_costs_its_own_member_and_not_the_teardown` |
//! | a keyed actor's own ending reaches a `watch` future, carrying the reason its loop returned | `a_keyed_actors_own_ending_reaches_a_watch_future_with_its_reason` |
//! | every waiter filed under one name is answered, and not just one of them | `two_watchers_on_one_name_are_both_answered` |
//! | two `Lifecycle`s on one `System` both fire, so watching does not cost the console | `two_lifecycles_on_one_system_both_fire` |
//! | an abandoned `watch` unfiles its own waiter and not a later one under that name | `an_abandoned_watch_unfiles_its_own_waiter_and_not_a_later_one` |
//! | a `watch` outliving its `Endings` resolves `Vanished` instead of waiting for ever | `a_watch_outliving_its_endings_resolves_vanished_rather_than_hanging` |
//! | a keyed actor is a `Group` member: it ends, the policy is asked, the siblings are retired | `a_keyed_actor_is_a_group_member_end_to_end` |
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
//!
//! # And one place a clock is read, which is a third thing again
//!
//! The group's section reads a real clock through [`bounded`], and it asserts
//! nothing about what it reads. Three of those guards have a defect whose whole
//! symptom is that an ending never arrives, and an unbounded `await` on one
//! turns that defect into a **hang** — which a harness cannot tell from a slow
//! machine, and which `tests/supervision.rs` records the price of. [`bounded`]
//! makes each of them a named failure instead. It is a liveness bound and never
//! a ceiling on how long the work may take: see [`NO_PROGRESS`] for the one test
//! whose defect is lateness rather than a hang, which derives its member's
//! backoff from that bound rather than picking a number beside it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use keylink::compose::{
    after, every, Endings, Fanout, Group, GroupEnded, Membership, Names, Overflow, Pool,
    Subscription, Topic, Verdict,
};
use keylink::{
    ActorId, Caller, Ended, Eviction, Intensity, Lifecycle, Observer, Stop, TurnFut, TurnInfo,
};
use n0_future::time::Duration;
use n0_future::StreamExt;
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

/// A panicking `T::clone` costs one publish, and not the topic.
///
/// `Topic::lock` recovers a poisoned mutex instead of propagating, and until now the
/// justification for that arm was that it could not be reached: *"Nothing here can
/// panic while the lock is held — `try_send` cannot, and the blocking path releases
/// the lock before it awaits."* That argument is falsified by the file it is written
/// in. `publish_dropping` calls `item.clone()` **inside** `self.lock().retain(..)`,
/// and `T: Clone` is the caller's code, so one panicking `Clone` unwinds out with the
/// guard held.
///
/// The behaviour was always right; only the reason was wrong. This is the reason,
/// written as a test: the panic costs its own publish, and the next `subscribers`,
/// `subscribe` and `publish` on the same topic all still answer.
///
/// Made red by `scripts/break-once.sh keylink/src/compose/topic.rs`, replacing
/// `self.to.lock().unwrap_or_else(PoisonError::into_inner)` with
/// `self.to.lock().expect("poisoned")` — propagation instead of recovery, which is
/// what the old argument said was safe. Watched: the *next* call after the bad
/// publish, `topic.subscribers()`, panicking in its turn with
/// `topic.rs:260: poisoned: PoisonError { .. }`.
///
/// The "never delivered to" one-poll carries its own break, because an assertion
/// that something is *absent* is the easiest kind to write vacuously. Pointed
/// instead at `second` — delivered to before the panic, and left undrained — it
/// goes red with "the third was never delivered to". It can tell the two apart.
#[tokio::test]
async fn a_panicking_clone_costs_one_publish_and_not_the_topic() {
    /// Clones `limit` times and then refuses, so the panic lands part-way through
    /// a fan-out — inside `retain`, with the guard held.
    #[derive(Debug)]
    struct Brittle {
        cloned: Arc<AtomicUsize>,
        limit: usize,
    }

    impl Brittle {
        fn breaking_after(limit: usize) -> Self {
            Self {
                cloned: Arc::new(AtomicUsize::new(0)),
                limit,
            }
        }
    }

    impl Clone for Brittle {
        fn clone(&self) -> Self {
            assert!(
                self.cloned.fetch_add(1, Ordering::SeqCst) < self.limit,
                "this Clone has had enough"
            );
            Self {
                cloned: Arc::clone(&self.cloned),
                limit: self.limit,
            }
        }
    }

    let topic = Topic::new(Overflow::DropSubscriber);
    let mut first = topic.subscribe();
    let mut second = topic.subscribe();
    let mut third = topic.subscribe();

    // Three subscribers, two clones: the third one panics under the lock.
    let poisoning = tokio::spawn({
        let topic = topic.clone();
        async move { topic.publish(Brittle::breaking_after(2)).await }
    });
    let died = poisoning
        .await
        .expect_err("the caller's `Clone` panics inside the fan-out");
    assert!(died.is_panic(), "and it is that panic: {died:?}");

    // The mutex is poisoned from here on. Every call below takes it, and the
    // whole point is that none of them panics.
    //
    // What the recorded break falsifies is that `subscribers()` *answers at all*;
    // the 3 is incidental to it. That value rides on `Vec::retain` keeping the
    // elements it had not yet visited when the predicate unwound, which std does
    // not document and has changed once already. Pinned anyway, because it is true
    // today and a change would be worth hearing about — but if this line ever goes
    // red on its own, read it as news about `retain` and not about `Topic`.
    assert_eq!(
        topic.subscribers(),
        3,
        "the topic outlives one caller's bad Clone"
    );

    // The publish cost exactly itself: the two it reached before the panic keep
    // what they were handed, and the third was never delivered to.
    assert!(first.next().await.is_some(), "delivered before the panic");
    assert!(second.next().await.is_some(), "delivered before the panic");

    // "Never delivered to" is a claim about *now*, so it is asserted now — one
    // poll, no clock. Drained after the next publish instead, it would pass on
    // that publish's item and hold nothing: an empty third and a third that was
    // reached before the panic look identical once a second item is in the
    // channel.
    assert!(
        n0_future::future::poll_once(third.next()).await.is_none(),
        "the third was never delivered to"
    );

    let mut fourth = topic.subscribe();
    assert_eq!(topic.subscribers(), 4, "and it still takes subscribers");
    assert_eq!(
        topic.publish(Brittle::breaking_after(usize::MAX)).await,
        Fanout {
            delivered: 4,
            dropped: 0,
            gone: 0
        },
        "and publishing after a poisoned publish still reaches everyone"
    );
    assert!(third.next().await.is_some(), "including the one skipped");
    assert!(fourth.next().await.is_some());
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

// ─────────────────────────────────────────────────────────────────── the group

/// A liveness bound, never a measurement.
///
/// Two guards below name a defect whose only symptom is that an ending never
/// comes: a dropped group nothing woke, and a member that cannot be retired
/// until its backoff elapses. Unbounded, both **hang** the suite, and
/// `tests/supervision.rs` records what that costs — a hang is not a failure a
/// harness can read, and its own red-test run "sat for minutes reporting
/// nothing". That file uses this same constant for this same reason.
///
/// **This said three, and the third was wrong.** A conductor that dies inside
/// caller code — a panicking policy, a panicking ending — does not hang anybody,
/// because it takes its `watch::Sender` with it, and [`Group::ended`] answers
/// `Dropped` on a dropped sender rather than waiting on one. So those two redden
/// by assertion, in 0.00s, on a `GroupEnded` that is the wrong variant rather
/// than a missing one. Measured, after the breaks were run: only the backoff
/// guard actually reached this bound, at 60.01s. The mistake was reasoning from
/// *"the conductor is gone, so nothing will answer"* without reading the arm
/// that answers precisely when nothing will.
///
/// It is not a ceiling on how long the work may take. Nothing here asserts a
/// duration, and the one test whose defect is *lateness* rather than a hang —
/// `a_member_asleep_in_backoff_is_retired_without_waiting_out_its_backoff` —
/// derives its member's backoff **from this number** rather than choosing one
/// beside it: sixty times this bound. So the margin cannot rot when either
/// moves, and a teardown that waited the backoff out cannot fit inside the
/// bound however loaded the machine is.
const NO_PROGRESS: Duration = Duration::from_secs(60);

/// Await something that must resolve, and fail rather than hang when it does
/// not. See [`NO_PROGRESS`].
async fn bounded<T>(waiting: impl Future<Output = T>) -> T {
    n0_future::time::timeout(NO_PROGRESS, waiting)
        .await
        .expect("this must resolve rather than run for ever")
}

/// Every decision the policy was asked to make, in the order it was asked.
///
/// A list and not a counter: several of these guards are about *which* member
/// the group named and *what reason* it carried, and a counter cannot tell an
/// ending reported under the wrong name from one reported under the right one.
type Asked = Arc<Mutex<Vec<(ActorId, Ended)>>>;

/// A policy that records what it was asked, and answers `verdict` every time.
fn recording(
    verdict: Verdict,
) -> (
    Asked,
    impl FnMut(&ActorId, &Ended) -> Verdict + Send + 'static,
) {
    let asked: Asked = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&asked);
    (asked, move |who, why| {
        seen.lock()
            .expect("not poisoned")
            .push((who.clone(), why.clone()));
        verdict
    })
}

/// What the policy has been asked so far, copied out so that no assertion holds
/// the lock the group's own task takes.
fn decisions(asked: &Asked) -> Vec<(ActorId, Ended)> {
    asked.lock().expect("not poisoned").clone()
}

/// An [`Intensity`], built the way `tests/supervision.rs` builds one.
fn limit(max: u32, recovered_after: Duration) -> Intensity {
    Intensity::new(
        NonZeroU32::new(max).expect("a budget of at least one"),
        recovered_after,
    )
    .expect("a recovery span whose backoff is not zero")
}

/// Refuses the first turn it ever sees and accepts every later one, which is how
/// a member is made to restart exactly once.
///
/// The shape `Persist` has, without a store. A `service!` method that panics does
/// not end its actor by itself, so an observer is what turns a refusal into an
/// ending and therefore into a restart.
///
/// `fatal` picks whether the supervisor may try again. `Stop::fatal` after a turn
/// that **completed** is honoured and spends no budget, which
/// `tests/supervision.rs` pins on its own; here it is the cheapest way to make a
/// member end for good on its first turn.
struct RefuseFirstTurn {
    seen: Arc<AtomicUsize>,
    fatal: bool,
}

impl<A: Send + 'static> Observer<A> for RefuseFirstTurn {
    fn name(&self) -> &'static str {
        "refuse-first-turn"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a A, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        let first = self.seen.fetch_add(1, Ordering::SeqCst) == 0;
        let fatal = self.fatal;
        Box::pin(async move {
            match (first, fatal) {
                (false, _) => Ok(()),
                (true, false) => Err(Stop::new("the first turn is refused")),
                (true, true) => Err(Stop::fatal("this member cannot serve at all")),
            }
        })
    }
}

/// A supervised [`Worker`] that refuses its first turn, and the count of
/// incarnations built.
///
/// The counter is what separates "restarted" from "ended": the group is told
/// about neither, so a test has to read the member's own side.
fn refusing(fatal: bool, recovered_after: Duration) -> (worker::Handle<Worker>, Arc<AtomicUsize>) {
    let built = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(AtomicUsize::new(0));
    let member = {
        let built = Arc::clone(&built);
        worker::supervised(
            move || {
                let built = Arc::clone(&built);
                let seen = Arc::clone(&seen);
                async move {
                    built.fetch_add(1, Ordering::SeqCst);
                    let observers: Vec<Box<dyn Observer<Worker>>> =
                        vec![Box::new(RefuseFirstTurn { seen, fatal })];
                    Ok((Worker { me: 7 }, observers, ()))
                }
            },
            limit(2, recovered_after),
        )
    };
    (member, built)
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
/// `policy(&reported.name, &reason)` with
/// `policy(&reported.name, &Ended::Stopped)` — a policy told a reason the group
/// invented instead of the one the member ended with, which is the shape of every
/// supervisor that reports its own teardown for somebody else's fault.
/// Watched: it fails at the policy's own argument, "with the reason it ended
/// for: Stopped", where the member ended `Vanished`. Exit 101, by value, in
/// 0.01s.
#[tokio::test]
async fn a_member_that_ends_for_good_reaches_the_policy_once() {
    let (asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let ledger = ActorId::new("ledger").expect("a name");
    let actor = worker::actor(Worker { me: 1 });

    assert!(
        group
            .add(ledger.clone(), keylink::membership!(actor))
            .is_none(),
        "nothing was displaced"
    );
    assert_eq!(group.names(), vec![ledger.clone()], "and it is watched");

    // `abort` and not `stop`, so the reason carried out is `Vanished` and cannot
    // be confused with the group tearing the member down itself.
    actor.abort();
    assert!(
        settles(|| !decisions(&asked).is_empty()).await,
        "an ending for good reaches the policy"
    );

    // Every chance to be asked a second time, and it must not be.
    spun().await;
    let seen = decisions(&asked);
    assert_eq!(seen.len(), 1, "asked once, and once only");
    assert_eq!(seen[0].0, ledger, "about the member that ended");
    assert!(
        matches!(seen[0].1, Ended::Vanished),
        "with the reason it ended for: {:?}",
        seen[0].1
    );
    assert!(
        group.names().is_empty(),
        "and `Forget` leaves the group watching nothing"
    );
}

/// **The break this record names reddens one half, and the other half has none.**
/// `scripts/break-once.sh keylink/src/local.rs`, replacing `supervise`'s
/// `if !restarting {` with `if true {` — an outer loop that publishes every
/// incarnation's ending instead of only its last, which is the mistake this
/// module's structural claim rests on not being made. Watched: it fails before
/// the policy is ever consulted, at "the next incarnation answers:
/// MailboxClosed" — the outer loop published the first incarnation's ending, so
/// the handle is dead and there is no second incarnation to answer. Exit 101, by
/// value, in 0.00s.
///
/// That break destroys the restart rather than leaking it, so what it proves is
/// that the restart below is real and observed. The other half — *the policy is
/// not told* — has **no break in `group.rs` at all**, because there is no filter
/// there to break: `Group` decides on every ending it is handed, and only the
/// outer task's exit publishes one. `supervision.rs`'s
/// `a_handle_survives_its_actors_restart` pins the same structure from the other
/// side. Said here rather than counted as a proven guard.
#[tokio::test]
async fn a_members_own_restart_never_reaches_the_policy() {
    let (asked, policy) = recording(Verdict::StopAll);
    let group = Group::new(policy);
    let name = ActorId::new("member").expect("a name");
    // A two-millisecond span is a one-millisecond backoff. Real time, and it is
    // waited for rather than asserted about.
    let (member, built) = refusing(false, Duration::from_millis(2));

    assert!(
        group
            .add(name.clone(), keylink::membership!(member))
            .is_none(),
        "nothing was displaced"
    );

    assert!(
        member.whoami().await.is_err(),
        "the first turn is refused, which ends the incarnation"
    );
    assert_eq!(
        member.whoami().await.expect("the next incarnation answers"),
        7,
        "and the same handle reaches it"
    );
    assert_eq!(
        built.load(Ordering::SeqCst),
        2,
        "two incarnations, so a restart happened"
    );

    // THIS is the decision. Every opportunity to have been told, and it was not.
    spun().await;
    assert!(
        decisions(&asked).is_empty(),
        "a member's own restart is not an ending, and the policy is never asked about one"
    );
    assert_eq!(
        group.names(),
        vec![name.clone()],
        "and the member is still watched"
    );

    // The ending that *is* final does reach it.
    member.abort();
    assert!(
        settles(|| !decisions(&asked).is_empty()).await,
        "the ending that ends the member for good reaches the policy"
    );
    let seen = decisions(&asked);
    assert_eq!(seen.len(), 1, "once, for the one ending that was final");
    assert!(
        matches!(seen[0].1, Ended::Vanished),
        "and it is that ending: {:?}",
        seen[0].1
    );
}

/// Made red by the same break as `a_member_that_ends_for_good_reaches_the_policy_once`:
/// `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
/// `policy(&reported.name, &reason)` with
/// `policy(&reported.name, &Ended::Stopped)`. Watched: it fails at `seen[0].1`,
/// "and handed the supervisor's own reason rather than one the group invented:
/// Stopped", where the supervisor produced `GaveUp { after: 0 }`. Exit 101, by
/// value, in 0.00s — and that line is the one the paragraph below is about.
///
/// One break, two guards, and they are not the same claim: there the reason is
/// `Vanished` and here it is `GaveUp { after: 0 }`, which is a reason **only a
/// supervisor produces**. A group that handed the policy a reason of its own
/// would erase the difference between an actor somebody aborted and one that
/// refused to serve at all.
///
/// **This guard was GREEN under that break until the assertion at the end was
/// added, and the reason is worth keeping.** It asserted `GroupEnded::Member`'s
/// `reason` and a decision *count*, and never the `Ended` the policy was handed.
/// The conductor builds that `GroupEnded` from its own local, so the whole
/// assertion block sat on a path the break does not touch — while the record
/// above claimed a policy argument. Its sibling reddens on the identical literal
/// only because it reads `seen[0].1`. **A record naming what the policy is told
/// needs an assertion on what the policy was told**, and "the caller was told the
/// right thing" is a different claim wearing the same words.
#[tokio::test]
async fn a_fatal_refusal_reaches_the_policy_without_a_restart() {
    let (asked, policy) = recording(Verdict::StopAll);
    let group = Group::new(policy);
    let name = ActorId::new("member").expect("a name");
    let (member, built) = refusing(true, Duration::from_secs(10));

    assert!(
        group
            .add(name.clone(), keylink::membership!(member))
            .is_none(),
        "nothing was displaced"
    );
    assert!(
        member.whoami().await.is_err(),
        "a refused turn owes its caller a refusal"
    );

    match bounded(group.ended()).await {
        GroupEnded::Member { member, reason } => {
            assert_eq!(member, name, "the group names the member that ended");
            assert!(
                matches!(reason, Ended::GaveUp { after: 0, .. }),
                "and carries the supervisor's own reason, which spent no restart: {reason:?}"
            );
        }
        other => panic!("`StopAll` must end the group as `Member`, got {other:?}"),
    }
    assert_eq!(
        built.load(Ordering::SeqCst),
        1,
        "built once, and never rebuilt: a fatal refusal is not retried"
    );

    // **What the POLICY was handed, which is a different question from what the
    // caller was told above.** `GroupEnded::Member` is built by the conductor
    // from its own local, so reading only that leaves the policy's argument
    // unchecked — and this guard was green under exactly that break until it
    // was run.
    let seen = decisions(&asked);
    assert_eq!(seen.len(), 1, "the policy was asked once");
    assert_eq!(seen[0].0, name, "about that member");
    assert!(
        matches!(seen[0].1, Ended::GaveUp { after: 0, .. }),
        "and handed the supervisor's own reason rather than one the group invented: {:?}",
        seen[0].1
    );
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
///
/// ```text
///             Ok(Verdict::StopAll) => {
///                 retire_all(closing(&shared)).await;
/// ```
///
/// with `retire_all(Vec::new()).await;` on the second line — a group that ends
/// itself and leaves every sibling running, which is the outcome this whole
/// module exists to prevent and the one a `GroupEnded` alone cannot tell you
/// about. Watched: the `GroupEnded::Member` arm above still passes, and what
/// fails is the sibling assertion — "every other member is retired, not left
/// serving". So the red lands exactly where the record says the claim lives, and
/// a guard reading only the `GroupEnded` would have stayed green. Exit 101, by
/// value, in 0.00s.
///
/// **Pass the WHOLE fence.** The second line alone,
/// `retire_all(closing(&shared)).await;`, occurs three times in `group.rs`. The
/// literal is unique only because the `Ok(Verdict::StopAll) => {` line above it
/// is part of it. Shortened, `break-once.sh` aborts on a count of 3 before it
/// takes a backup — safe, and a wasted run.
#[tokio::test]
async fn stop_all_ends_the_group_and_names_the_member_that_caused_it() {
    let (asked, policy) = recording(Verdict::StopAll);
    let group = Group::new(policy);
    let first = ActorId::new("reserve").expect("a name");
    let second = ActorId::new("charge").expect("a name");
    let reserve = worker::actor(Worker { me: 1 });
    let charge = worker::actor(Worker { me: 2 });

    assert!(group
        .add(first.clone(), keylink::membership!(reserve))
        .is_none());
    assert!(group
        .add(second.clone(), keylink::membership!(charge))
        .is_none());

    reserve.abort();
    match bounded(group.ended()).await {
        GroupEnded::Member { member, reason } => {
            assert_eq!(
                member, first,
                "the group names the member the policy was asked about"
            );
            assert!(
                matches!(reason, Ended::Vanished),
                "and carries its reason: {reason:?}"
            );
        }
        other => panic!("`StopAll` must end the group as `Member`, got {other:?}"),
    }

    // The half a `GroupEnded` cannot carry, and the one worth watching: the
    // sibling nobody asked about is gone too. Bounded, so a group that ended
    // itself and orphaned it fails here rather than blocking.
    assert!(
        settles(|| !charge.is_running()).await,
        "every other member is retired, not left serving"
    );
    assert!(
        matches!(bounded(charge.ended()).await, Ended::Vanished),
        "and it was aborted, which is what `membership!` retires with"
    );
    assert_eq!(decisions(&asked).len(), 1, "the policy was asked once");
    assert!(group.names().is_empty(), "and the group watches nothing");

    // The control for the one-poll device two of these guards use as a
    // *negative*. A `poll_once` that could never be `Some` would let
    // `forget_removes_the_member_and_leaves_the_others_serving` and
    // `a_displaced_members_ending_does_not_reach_the_policy_as_its_replacement`
    // assert "the group did not end" against a poll that says nothing.
    assert!(
        n0_future::future::poll_once(group.ended()).await.is_some(),
        "a group that HAS ended answers a fresh awaiter on its first poll"
    );
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
/// `retire_all`'s `Ok(fut) => retiring.push(guarded(fut)),` with
/// `Ok(fut) => { let _ = guarded(fut).await; }` — the serial spelling that
/// function's own doc comment refuses, and the one every hand-written teardown
/// reaches for first. Awaited inside the build loop, a member wedged in its
/// retirement stops every later member from being retired at all.
/// Watched: "a member's retirement does not wait on the one retired before it".
/// Exit 101, by value, in 0.00s — and it COMPILES, with no `E0282` and no
/// `E0308`, which is the whole of what the annotation bought.
///
/// **This break was not expressible until `retire_all`'s set was annotated, and
/// that annotation exists for this break and no other reason.** The item type
/// used to come from the single `retiring.push(guarded(fut))` line and from
/// nothing else — the very line this replaces — so the edit left the
/// `FuturesUnordered` with no inference source and the crate stopped compiling
/// instead of the test going red. Measured twice, on two spellings: `E0308`
/// first, then `E0282`. `FuturesUnordered<Guarded<Retiring>>` gives the set its
/// type up front, which is what frees this line to be replaced by anything, and
/// `panic.rs`'s `Guarded` is a named type rather than an `async fn` so that
/// there is something to write there at all. The replacement above was verified
/// to compile against the annotated set: no `E0282`, no `E0308`, no warning.
///
/// **The serial spelling must discard the result, and the first version of this
/// record did not.** `Ok(fut) => guarded(fut).await,` does not compile: `guarded`
/// yields `Result<(), Panic>` and the sibling `Err(_panicked) => panicked += 1,`
/// arm yields `()`, so the match arms disagree. Run as written it exits 101 on
/// E0308 and tests nothing — and a non-zero exit from `break-once.sh` reads as a
/// caught defect, so that failure is the one shaped most like success. The
/// `let _ =` is what makes the arm `()`.
///
/// **Read the anchor line before running it.** The defect is fixed — await the
/// retire future where it is built instead of pushing it — but the line it lives
/// on took three spellings while this test was being written, as `retire_all`
/// gained and lost its panic guards. A literal that no longer occurs aborts
/// `break-once.sh` rather than proving anything.
///
/// The claim is the provable one and not an order `FuturesUnordered` does not
/// promise: every retire future is **built** before any is polled, and no
/// member's retirement waits on another's to complete.
#[tokio::test]
async fn no_members_retirement_waits_on_another() {
    let (_asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    // How many retire closures were CALLED, which is the "built" half, and how
    // many retire futures COMPLETED, which is the "waits on" half.
    let called = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    // What `called` had reached at the moment `quick`'s future was FIRST POLLED.
    // A count read after the teardown cannot carry the "built before polled"
    // claim — one-at-a-time reaches 2 as well, just later — so the claim is
    // read where it is true or not: inside the future, on its first poll.
    let called_when_polled = Arc::new(AtomicUsize::new(0));
    let (release, held) = oneshot::channel::<()>();

    // Neither member ends on its own: `pending` is an ending that never comes, so
    // the only thing that touches either of them is the teardown.
    let quick = {
        let called = Arc::clone(&called);
        let finished = Arc::clone(&finished);
        let called_when_polled = Arc::clone(&called_when_polled);
        Membership::new(std::future::pending(), move || {
            called.fetch_add(1, Ordering::SeqCst);
            async move {
                called_when_polled.store(called.load(Ordering::SeqCst), Ordering::SeqCst);
                finished.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    let stuck = {
        let called = Arc::clone(&called);
        Membership::new(std::future::pending(), move || {
            called.fetch_add(1, Ordering::SeqCst);
            async move {
                let _ = held.await;
            }
        })
    };

    // Teardown is issued in reverse insertion order, so the one that parks for
    // ever is the one added LAST and therefore retired FIRST.
    assert!(group
        .add(ActorId::new("quick").expect("a name"), quick)
        .is_none());
    assert!(group
        .add(ActorId::new("stuck").expect("a name"), stuck)
        .is_none());

    group.stop();

    // THIS is the decision. `stuck`'s retirement never returns, and `quick`'s
    // neither waits behind it nor is skipped by it.
    assert!(
        settles(|| finished.load(Ordering::SeqCst) == 1).await,
        "a member's retirement does not wait on the one retired before it"
    );
    // Read INSIDE `quick`'s future, on its first poll. `called == 2` measured
    // after the fact would also hold for a loop that built and polled one member
    // at a time, which is the spelling `retire_all` refuses — so the count is
    // taken at the only instant that separates the two.
    assert_eq!(
        called_when_polled.load(Ordering::SeqCst),
        2,
        "both retire closures had already run when the first retire future was polled"
    );

    // Released, the teardown finishes and the group answers.
    let _ = release.send(());
    assert!(
        matches!(
            bounded(group.ended()).await,
            GroupEnded::Stopped { retire_panics: 0 }
        ),
        "`stop` ends the group as `Stopped`, and no member's `retire` panicked"
    );
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing the
/// conductor's `            held.members.remove(at)` with
/// `            { let last = held.members.len() - 1; held.members.remove(last) }`
/// — the newest member taken off the books instead of the one the conductor
/// decided about, so the decided one is left behind and `names()` becomes a list
/// of actors the group is no longer watching. Watched: "`Forget` takes the member
/// off the books and leaves the rest". Exit 101, by value, in 0.01s.
///
/// **This record's first substitution rotted, and both halves of it rotted at
/// once.** It read: replace `held.members.remove(at);` with `let _ = at;`. The
/// fix for the `EndingPanicked` leak made that line the enclosing block's
/// *value* — it lost its semicolon — so the literal stopped occurring and
/// `break-once.sh` aborted at its grep, before taking a backup. `mise run
/// anchors` caught it as `FAIL G5: the anchor IS GONE`.
///
/// **And the old replacement would not have compiled either.** `let _ = at;`
/// makes the block yield `()`, which is then bound and read as `taken.retire` on
/// the `Err` arm: measured, `error[E0609]: no field 'retire' on type '()'`, and
/// `break-once.sh` reports that as `── exit=101` — indistinguishable from a
/// caught defect unless you read the log. A rotted anchor and a rotted
/// substitution are two failures and only the first one is loud.
///
/// **Why the replacement removes the wrong member rather than none.** Leaving
/// the decided member on the books is no longer expressible in one line: the
/// block has to *yield an `Entry`*, and the only way to get an owned one is to
/// take it out — `Entry` holds a boxed `FnOnce` and is not `Clone`. So the
/// nearest expressible defect is taking the wrong one, which reddens the same
/// assertion for the same reason. Two candidates were tried and rejected first,
/// both by measurement: `swap_remove(held.members.len() - 1)` and
/// `remove(held.members.len() - 1)` are both `error[E0502]`, because the length
/// read and the removal borrow the `MutexGuard` at once and two-phase borrows do
/// not reach through the `Deref`. Binding `last` first is what makes it compile.
///
/// **THE PRECONDITION, because a wrong-index break is only wrong at some
/// indices.** This substitution takes the LAST member, so it reddens only while
/// the ended member is not the last one. Here the ended member is the FIRST —
/// `leaving` is added before `staying`, and `leaving` is the one aborted — so
/// `last` is `staying` and the decided member is left behind. **Add a member
/// after `staying`, or end `staying` instead, and this break goes green while
/// proving nothing.**
///
/// That is not hypothetical. `held.members.remove(0)` was the other candidate
/// and it is the mirror image: the ended member IS at index 0 here, so `remove(0)`
/// and `remove(at)` are the same call. Measured — `── exit=0`, the test passed
/// under the break. A substitution that indexes must state which index the test
/// puts the ended member at, or the next person to reorder the `add` calls turns
/// this guard off without touching it.
#[tokio::test]
async fn forget_removes_the_member_and_leaves_the_others_serving() {
    let (asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let leaving = ActorId::new("leaving").expect("a name");
    let staying = ActorId::new("staying").expect("a name");
    let gone = worker::actor(Worker { me: 1 });
    let kept = worker::actor(Worker { me: 2 });

    assert!(group
        .add(leaving.clone(), keylink::membership!(gone))
        .is_none());
    assert!(group
        .add(staying.clone(), keylink::membership!(kept))
        .is_none());

    gone.abort();
    assert!(
        settles(|| group.names() == vec![staying.clone()]).await,
        "`Forget` takes the member off the books and leaves the rest"
    );
    assert_eq!(decisions(&asked).len(), 1, "the policy was asked once");

    // The other half, which a `names()` assertion alone cannot make: the ones
    // that stayed are still answering.
    assert_eq!(
        kept.whoami().await.expect("still serving"),
        2,
        "and the others carry on without it"
    );
    assert!(
        n0_future::future::poll_once(group.ended()).await.is_none(),
        "`Forget` does not end the group, and an empty group would still wait"
    );
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing the
/// conductor's
/// `entry.name == reported.name && entry.generation == reported.generation` with
/// `entry.name == reported.name` — the generation stamp dropped, so the displaced
/// member's still-pending ending is decided on under a name its replacement now
/// owns. Watched: "a displaced member's ending is discarded on its stamp, never
/// decided on". Exit 101, by value, in 0.01s.
///
/// The same race `names.rs` documents and the same fix, watched here from the
/// side that matters for a group: not a wrong eviction but a wrong **decision**,
/// and with `StopAll` in the policy that decision ends every other member too.
#[tokio::test]
async fn a_displaced_members_ending_does_not_reach_the_policy_as_its_replacement() {
    let (asked, policy) = recording(Verdict::StopAll);
    let group = Group::new(policy);
    let ledger = ActorId::new("ledger").expect("a name");
    let first = worker::actor(Worker { me: 1 });
    let second = worker::actor(Worker { me: 2 });

    assert!(group
        .add(ledger.clone(), keylink::membership!(first))
        .is_none());
    let displaced = group
        .add(ledger.clone(), keylink::membership!(second))
        .expect("the first membership comes back");

    // The first actor ends, with its ending still filed under the SAME name.
    first.abort();
    spun().await;

    assert!(
        decisions(&asked).is_empty(),
        "a displaced member's ending is discarded on its stamp, never decided on"
    );
    assert_eq!(
        group.names(),
        vec![ledger.clone()],
        "and the replacement is still watched"
    );
    assert_eq!(
        second.whoami().await.expect("running"),
        2,
        "and still serving"
    );
    assert!(
        n0_future::future::poll_once(group.ended()).await.is_none(),
        "`StopAll` on an ending the group had already replaced would have ended it"
    );

    // **And the negative above is not vacuous.** A group that filed nothing, or
    // one whose conductor had died, would satisfy every assertion so far. The
    // same name, watched under the stamp the replacement owns, does reach the
    // policy — so the discarding is a decision about the stamp and not silence.
    second.abort();
    assert!(
        settles(|| !decisions(&asked).is_empty()).await,
        "the replacement's own ending is decided on"
    );
    assert_eq!(
        decisions(&asked).len(),
        1,
        "once, for the replacement and never for what it displaced"
    );

    // What `add` handed back is the means to end the member it displaced, and
    // nothing else. Ending it here is what keeps replacing from being silent.
    displaced.retire().await;
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
/// `held.staged.drain(..)` with `held.staged.drain(..).skip(1)` — a drain that
/// takes every staged member off the list and watches all but the first, which is
/// the off-by-one a hand-written index loop makes. Watched: "one wake drains
/// every staged add: the three filed while the policy ran are all watched".
/// Exit 101, by value, in 0.01s.
///
/// **The anchor row pins the enclosing line and not the call**, which is
/// `for staged in held.staged.drain(..) {`. `held.staged.drain(..)` on its own is
/// a fragment, and a fragment cannot be registered: `FRAGMENT_ANCHORS` in
/// `.mise/tasks/anchors` is a non-growth assertion, so declaring a new one
/// converts a working check into a silent pass. The substitution still edits the
/// `drain(..)` call inside that line. Move the loop and the row reddens; change
/// what the drain feeds and it does not.
///
/// **`steady` is in this test for that break and not for the claim.** The break
/// loses whichever member is first in a drain, and on a current-thread runtime the
/// conductor has not run when the two opening `add` calls are made — so without a
/// member in front of it the break would land on `opener` and redden this guard
/// through the ordinary path, proving nothing about a **late** add. With `steady`
/// first, and `steady` never ending, the loss falls on one of the three the policy
/// files while it is running, which is the claim.
///
/// The invariant's own break — draining *after* the wait instead of before it — is
/// a restructure of the loop rather than a literal, so `break-once.sh` cannot
/// express it.
#[tokio::test]
async fn adds_made_while_the_policy_runs_are_all_watched() {
    let asked: Asked = Arc::new(Mutex::new(Vec::new()));
    // Handed to the policy the moment `Group::new` returns, so that the policy —
    // which runs on the group's own task — can file members while it is running.
    // It holds a `Group` and therefore a cycle, which the first decision breaks by
    // taking it out; the test holds its own clone throughout.
    let inside: Arc<Mutex<Option<Group>>> = Arc::new(Mutex::new(None));
    let group = Group::new({
        let asked = Arc::clone(&asked);
        let inside = Arc::clone(&inside);
        move |who, why| {
            asked
                .lock()
                .expect("not poisoned")
                .push((who.clone(), why.clone()));
            if let Some(group) = inside.lock().expect("not poisoned").take() {
                for at in 0..3 {
                    let name = ActorId::new(format!("late-{at}")).expect("a name");
                    let actor = worker::actor(Worker { me: at });
                    // Ended before it is filed, so the three endings this test
                    // waits for are arriving for members staged while the policy
                    // was on the stack.
                    actor.abort();
                    let _ = group.add(name, keylink::membership!(actor));
                }
            }
            Verdict::Forget
        }
    });
    *inside.lock().expect("not poisoned") = Some(group.clone());

    // A member that never ends. See the record above: it is the break's target,
    // not the claim's.
    assert!(group
        .add(
            ActorId::new("steady").expect("a name"),
            Membership::new(std::future::pending(), || async {}),
        )
        .is_none());
    let opener = ActorId::new("opener").expect("a name");
    let first = worker::actor(Worker { me: 9 });
    assert!(group
        .add(opener.clone(), keylink::membership!(first))
        .is_none());

    first.abort();
    assert!(
        settles(|| decisions(&asked).len() == 4).await,
        "one wake drains every staged add: the three filed while the policy ran are all watched"
    );

    // Exactly those four, each once — a count alone would pass on one member
    // decided about four times.
    spun().await;
    let seen = decisions(&asked);
    assert_eq!(seen.len(), 4, "and no more arrive after them");
    for wanted in [
        opener,
        ActorId::new("late-0").expect("a name"),
        ActorId::new("late-1").expect("a name"),
        ActorId::new("late-2").expect("a name"),
    ] {
        assert_eq!(
            seen.iter().filter(|(who, _)| who == &wanted).count(),
            1,
            "{wanted} was decided about exactly once"
        );
    }
    assert_eq!(
        group.names(),
        vec![ActorId::new("steady").expect("a name")],
        "and `Forget` left only the member that never ended"
    );
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
/// `match catch_unwind(AssertUnwindSafe(|| policy(&reported.name, &reason))) {`
/// with
/// `match Ok::<Verdict, Box<dyn std::any::Any + Send>>(policy(&reported.name, &reason)) {`
/// — the panic uncaught, so the conductor dies with it and every member is left
/// running and unwatched, which is the group-actor problem this module refuses,
/// reached from the other side. Watched: "a panicking policy must end the group,
/// got Dropped". Exit 101, by assertion and in 0.00s, which is what the
/// paragraph below predicts rather than what the record first claimed.
///
/// **This break reddens by assertion and not by timeout**, and the record said
/// the opposite until the break was run. A conductor that unwinds takes its
/// `watch::Sender` with it, and [`Group::ended`] answers `GroupEnded::Dropped`
/// on a dropped sender — so the `match` below fails on the wrong variant, in
/// 0.00s. The [`bounded`] wrapper is cheap insurance here rather than the thing
/// that produces the red.
#[tokio::test]
async fn a_policy_that_panics_ends_the_group_rather_than_orphaning_it() {
    let group = Group::new(|_who, _why| panic!("the policy refuses to decide"));
    let doomed = ActorId::new("doomed").expect("a name");
    let first = worker::actor(Worker { me: 1 });
    let sibling = worker::actor(Worker { me: 2 });

    assert!(group
        .add(doomed.clone(), keylink::membership!(first))
        .is_none());
    assert!(group
        .add(
            ActorId::new("sibling").expect("a name"),
            keylink::membership!(sibling)
        )
        .is_none());

    first.abort();
    match bounded(group.ended()).await {
        GroupEnded::PolicyPanicked { member, message } => {
            assert_eq!(
                member, doomed,
                "the group names the member the policy was deciding about"
            );
            assert_eq!(
                message, "the policy refuses to decide",
                "and carries what it panicked with"
            );
        }
        other => panic!("a panicking policy must end the group, got {other:?}"),
    }
    assert!(
        settles(|| !sibling.is_running()).await,
        "and every other member is retired rather than left running and unwatched"
    );
}

/// **The break this record names reddens one half, and the other half has none.**
/// `scripts/break-once.sh keylink/src/compose/group.rs`, replacing `Shared`'s
///
/// ```text
///     fn drop(&mut self) {
///         self.wake.notify_one();
///     }
/// ```
///
/// with `fn drop(&mut self) {}` — the one wake a dropped group has. Without it a
/// group with two healthy members and no holder is noticed only when some member
/// happens to end, so nothing answers whoever is awaiting [`Group::ended`].
///
/// **The anchor row pins `self.wake.notify_one();`**, the one line inside that
/// body, because a row carries a single literal and the substitution above spans
/// three lines. The row is therefore a rot tripwire on the wake and not on the
/// `Drop` impl: delete the impl and the row reddens, rename `wake` and it
/// reddens, but replace the body with a different single wake and it does not.
/// The fence is what the break needs; this line is what the list can hold.
/// Watched: at the [`NO_PROGRESS`] bound, exactly as the paragraph below
/// predicts — [`bounded`] panics with "this must resolve rather than run for
/// ever: Elapsed(())" after 60.00s inside the test and 62.9s of wall time.
/// Exit 101. One of only two of the twenty-two that reach the bound.
///
/// **That break was GREEN, in 0.00s, until the body below was changed to park
/// the conductor first.** The wake matters only to a conductor already waiting at
/// `wake.notified()`, and the original test reached `drop` before the conductor's
/// first poll ever ran — so that poll found the state gone on its own `upgrade`
/// and answered `Dropped` down a path with no wake in it. The test named a
/// mechanism it could not reach. Parking it is what puts the wake under test, and
/// the `waker` member exists for that and for nothing else. Expect this break to
/// redden **by timeout** now, at the [`NO_PROGRESS`] bound, because a parked
/// conductor that is never woken holds its `watch::Sender` open — unlike the
/// panicking-conductor breaks, which drop theirs and redden by assertion.
///
/// That reddens *"answers its waiters instead of hanging"*. The other half —
/// *"and leaves its members running"* — has **no break**, for
/// `a_topic_with_nobody_listening_drops_the_item_rather_than_keeping_it`'s reason:
/// it denies an action nothing takes. A group ending its members on drop would
/// have to be **added**, and an addition is not a break. Recorded as a regression
/// pin, not a guard. The [`Weak`](std::sync::Weak) the conductor holds is the
/// other candidate and it is not one either: making it an `Arc` is a type error at
/// `conduct`'s signature, which is a compile failure rather than a red test.
#[tokio::test]
async fn a_dropped_group_ends_as_dropped_and_leaves_its_members_running() {
    let (asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let name = ActorId::new("member").expect("a name");
    let member = worker::actor(Worker { me: 5 });

    assert!(group
        .add(name.clone(), keylink::membership!(member))
        .is_none());

    // **The conductor must be PARKED on the wake for this to test the wake.**
    // Nothing observable says "parked", so it is constructed: a second member
    // ends, the policy is asked about it, and the conductor loops back to its
    // wait. Without this the whole test runs before the conductor's first poll,
    // and that poll then finds the state already gone on its own `upgrade` —
    // which answers `Dropped` with no wake involved at all. That is not a
    // hypothetical: this guard was GREEN with `Shared::drop`'s body deleted
    // until this block was added.
    let waker = worker::actor(Worker { me: 6 });
    assert!(group
        .add(
            ActorId::new("waker").expect("a name"),
            keylink::membership!(waker),
        )
        .is_none());
    waker.abort();
    assert!(
        settles(|| !decisions(&asked).is_empty()).await,
        "the conductor has run a whole iteration, so it is back at its wait"
    );

    let held = group.clone();
    // Taken while the group is still held, and it must still be answered.
    let waiting = group.ended();

    drop(group);
    // **`names()` alone cannot carry this claim.** The `Dropped` path never
    // clears `members`, so this vector is what it is whether the group is still
    // watching or has already given up. The poll below is the half that can
    // fail: a group that had ended would answer its `ended()` on the first
    // poll, and `stop_all_ends_the_group_and_names_the_member_that_caused_it`
    // holds the control proving that poll CAN be `Some`.
    assert!(
        n0_future::future::poll_once(held.ended()).await.is_none(),
        "one clone left is a group that has NOT ended"
    );
    assert_eq!(
        held.names(),
        vec![name.clone()],
        "one clone left is a group that is still watching"
    );

    drop(held);
    assert!(
        matches!(bounded(waiting).await, GroupEnded::Dropped),
        "a group nobody holds any more answers its waiters instead of leaving them waiting"
    );
    assert_eq!(
        member.whoami().await.expect("still serving"),
        5,
        "and its members are still running: dropping a group stops watching, it ends nothing"
    );
}

/// **A regression pin on a hazard, not a guard on a property.** It asserts what
/// the code does today, and what it does today is lose a [`Group::stop`] the
/// conductor never observed.
///
/// [`stop`](Group::stop) raises a flag inside `Shared` and returns. The
/// conductor reads that flag only *after* its `Weak` upgrade succeeds. So
/// `stop(); drop(group);` with no await between them destroys `Shared` — and
/// the flag with it — before the conductor has been polled once, the upgrade
/// then fails, and the group ends [`GroupEnded::Dropped`] having retired
/// nothing. On a current-thread runtime that is not a race but the ordinary
/// case: nothing has scheduled the conductor yet.
///
/// **Reproduced before it was written down.** The hazard was first reasoned
/// from the code at a merge gate; this test is what turned it into a fact.
///
/// **It carries its own control, because the first half alone is vacuous.**
/// Dropping a group answers `Dropped` and leaves its members running whether
/// `stop` was called or not, so the first half cannot tell a *lost* stop from a
/// stop nobody made. The second half is the identical sequence with one await
/// between `stop` and the drop, and it ends [`GroupEnded::Stopped`] with the
/// member retired. The difference between the two halves is that await and
/// nothing else, which is what makes the first half a statement about `stop`.
///
/// **No fix, and the reason is that the obvious one does not work.** Moving
/// `stopping` into an [`Arc`] the conductor holds strongly does not help,
/// because the members list dies with `Shared` too — and holding *that* strongly
/// would keep the state alive for ever and stop `Dropped` firing at all, which
/// is the answer `a_dropped_group_ends_as_dropped_and_leaves_its_members_running`
/// exists to protect. Making `stop` drain the members itself is a real fix and it
/// costs `stop`'s "returns at once" promise plus a second route for the
/// `retire_panics` count to reach the watch. The documented remedy is one line
/// on [`Group::stop`]: hold the group until [`ended`](Group::ended) resolves.
#[tokio::test]
async fn a_stop_the_conductor_has_not_seen_is_lost_when_the_last_group_drops() {
    let (_asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let name = ActorId::new("member").expect("a name");
    let member = worker::actor(Worker { me: 1 });

    assert!(group
        .add(name.clone(), keylink::membership!(member))
        .is_none());

    // Taken while the group is still held, so something is left to answer.
    let waiting = group.ended();

    // **No await between these two lines, and that is the whole hazard.** The
    // conductor has not been polled once, so the flag `stop` raises is read by
    // nobody before `drop` takes the state it lives in.
    group.stop();
    drop(group);

    assert!(
        matches!(bounded(waiting).await, GroupEnded::Dropped),
        "a `stop` the conductor never observed is lost with the state it was \
         written into, and the group ends as though nobody had called it"
    );

    // The cost of that, and the reason this is worth a test rather than a
    // sentence: the teardown `stop` asked for never ran.
    spun().await;
    assert_eq!(
        member.whoami().await.expect("still serving"),
        1,
        "and no member was retired, though `stop` was called before the drop"
    );

    // ── the control: the same sequence, one await apart ──────────────────────
    //
    // Without this the half above says nothing. A dropped group answers
    // `Dropped` and leaves its members running whether or not anybody called
    // `stop`, so only the contrast makes the first half a claim about `stop`.
    let (_asked, policy) = recording(Verdict::Forget);
    let observed = Group::new(policy);
    let second = worker::actor(Worker { me: 2 });
    assert!(observed
        .add(
            ActorId::new("observed").expect("a name"),
            keylink::membership!(second),
        )
        .is_none());

    observed.stop();
    // The one difference. The conductor gets a turn, reads the flag, and tears
    // down before anything drops the state the flag lives in.
    let ended = bounded(observed.ended()).await;
    assert!(
        matches!(ended, GroupEnded::Stopped { retire_panics: 0 }),
        "the same `stop`, observed before the drop, ends the group as `Stopped`: {ended:?}"
    );
    assert!(
        settles(|| !second.is_running()).await,
        "and retires the member, which is what the first half lost"
    );
}

/// **This one's claim cannot be made red, and that is recorded rather than left to
/// be rediscovered.** The defect is a `Vec` of members becoming a `HashMap` — a
/// field type, an insert and a read, which is a restructure and not a literal, so
/// `scripts/break-once.sh` cannot express it. The precedent is
/// `two_clones_of_a_pool_share_one_rotation`: recorded as a regression pin, not a
/// guard.
///
/// The three names are chosen so that alphabetical order and insertion order
/// differ, which is what makes a `BTreeMap` visible here too.
#[tokio::test]
async fn members_are_reported_in_the_order_they_were_added() {
    let (_asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let names: Vec<ActorId> = ["reserve", "charge", "ledger"]
        .into_iter()
        .map(|name| ActorId::new(name).expect("a name"))
        .collect();

    let mut actors = Vec::new();
    for (at, name) in names.iter().enumerate() {
        let actor = worker::actor(Worker {
            me: u64::try_from(at).expect("three members"),
        });
        assert!(group
            .add(name.clone(), keylink::membership!(actor))
            .is_none());
        actors.push(actor);
    }
    assert_eq!(
        group.names(),
        names,
        "insertion order, which is also the teardown order"
    );

    // A replacement keeps the displaced member's POSITION: re-registering a
    // rebuilt actor is not a statement that it should now be torn down first.
    let replacement = worker::actor(Worker { me: 9 });
    let displaced = group
        .add(names[0].clone(), keylink::membership!(replacement))
        .expect("the first membership comes back");
    assert_eq!(
        group.names(),
        names,
        "and a replacement keeps the place of the member it displaced"
    );
    displaced.retire().await;
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
/// `membership!`'s `handle.abort();` with `handle.stop().await;` — the teardown
/// `Membership::new`'s doc argues against, and the argument is exactly this test:
/// `stop` puts a message in the mailbox, and a supervised actor polls its inbox
/// only inside the run loop, never across a backoff sleep.
/// Watched: at the [`NO_PROGRESS`] bound — "this must resolve rather than run
/// for ever: Elapsed(())" after 60.01s inside the test and 62.93s of wall time.
/// Exit 101. The teardown was waiting out a backoff of sixty bounds, which is
/// the defect stated as a duration rather than a value.
///
/// **No upper bound on wall-clock time is asserted.** The claim is that the
/// teardown *completes*, which load can only strengthen. The member's backoff is
/// derived from [`NO_PROGRESS`] rather than chosen beside it — sixty times the
/// bound — so a teardown that waited it out cannot fit inside the bound however
/// slow the machine is, and the margin cannot rot when either number moves.
#[tokio::test]
async fn a_member_asleep_in_backoff_is_retired_without_waiting_out_its_backoff() {
    let (_asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let name = ActorId::new("member").expect("a name");
    // A budget of two over a span of 120 bounds is a backoff of 60 of them.
    let (member, built) = refusing(false, NO_PROGRESS * 120);

    assert!(group
        .add(name.clone(), keylink::membership!(member))
        .is_none());
    assert!(
        member.whoami().await.is_err(),
        "the first turn is refused, which ends the incarnation"
    );

    // The state this test is about, constructed rather than waited for: the
    // budget is spent, the supervisor is inside its backoff sleep, and the next
    // incarnation does not exist.
    spun().await;
    assert_eq!(
        built.load(Ordering::SeqCst),
        1,
        "the member is asleep in its backoff, not serving"
    );

    group.stop();
    assert!(
        matches!(
            bounded(group.ended()).await,
            GroupEnded::Stopped { retire_panics: 0 }
        ),
        "the teardown completes while a member is asleep in its backoff"
    );
    assert!(
        matches!(bounded(member.ended()).await, Ended::Vanished),
        "and the member is gone, rather than owed the rest of an hour of backoff"
    );
    assert_eq!(
        built.load(Ordering::SeqCst),
        1,
        "and no further incarnation was ever built"
    );
}

/// An ending that panics instead of arriving.
///
/// A named `async fn` and not an inline `async` block, because a block whose only
/// expression diverges has no output type to infer and the signature here is what
/// supplies one.
async fn panicking_ending() -> Ended {
    panic!("the ending refuses to arrive")
}

/// A member whose retire **closure** panics, before any future exists.
///
/// It counts itself **before** it panics, and that count is load-bearing: a
/// teardown that never reached this member at all would leave the healthy
/// members' count untouched too, so the healthy count alone cannot tell a
/// panicking path from one that was never entered.
fn closure_panics(entered: &Arc<AtomicUsize>) -> Membership {
    let entered = Arc::clone(entered);
    Membership::new(std::future::pending(), move || -> std::future::Ready<()> {
        entered.fetch_add(1, Ordering::SeqCst);
        panic!("the retire closure refuses")
    })
}

/// A member whose retire **future** panics on its first poll. It counts itself
/// first, for [`closure_panics`]' reason.
fn future_panics(entered: &Arc<AtomicUsize>) -> Membership {
    let entered = Arc::clone(entered);
    Membership::new(std::future::pending(), move || async move {
        entered.fetch_add(1, Ordering::SeqCst);
        panic!("the retire future refuses");
    })
}

/// A member whose retirement completes, and counts itself.
///
/// Its ending is [`pending`](std::future::pending), so nothing but a teardown
/// ever touches it.
fn counting(retired: &Arc<AtomicUsize>) -> Membership {
    let retired = Arc::clone(retired);
    Membership::new(std::future::pending(), move || async move {
        retired.fetch_add(1, Ordering::SeqCst);
    })
}

/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
/// `add`'s `let reason = guarded(ended).await.map_err(|panicked| panicked.bare());`
/// with `let reason = Ok(ended.await);` — the caller's ending future left
/// unguarded, so its unwind takes the conductor with it. Every member is then
/// running and unwatched, and every awaiter of [`Group::ended`] is told `Dropped`
/// while the caller still holds a live `Group` — a wrong answer rather than a
/// missing one. Watched: "a panicking ending must end the group as
/// `EndingPanicked`, got Dropped". Exit 101, by assertion and in 0.00s, which is
/// what the paragraph below predicts.
///
/// **This break reddens by assertion and not by timeout**, and the record said
/// the opposite until the break was run. A conductor that unwinds takes its
/// `watch::Sender` with it, and [`Group::ended`] answers `GroupEnded::Dropped`
/// on a dropped sender — so the `match` below fails on the wrong variant, in
/// 0.00s. The [`bounded`] wrapper is cheap insurance here rather than the thing
/// that produces the red.
#[tokio::test]
async fn an_ending_that_panics_ends_the_group_as_ending_panicked() {
    let (asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let doomed = ActorId::new("doomed").expect("a name");
    let sibling = worker::actor(Worker { me: 2 });

    // Only a hand-written `Membership::new` reaches this. `membership!` builds
    // the ending out of a generated `ended()`, which is a watch read.
    assert!(group
        .add(
            doomed.clone(),
            Membership::new(panicking_ending(), || async {}),
        )
        .is_none());
    assert!(group
        .add(
            ActorId::new("sibling").expect("a name"),
            keylink::membership!(sibling),
        )
        .is_none());

    match bounded(group.ended()).await {
        GroupEnded::EndingPanicked { member, message } => {
            assert_eq!(
                member, doomed,
                "the group names the member whose ending it was awaiting"
            );
            assert_eq!(
                message, "the ending refuses to arrive",
                "and carries what that future panicked with"
            );
        }
        other => panic!("a panicking ending must end the group as `EndingPanicked`, got {other:?}"),
    }

    // The policy is not asked, and that is the variant's own argument rather than
    // an omission: a `Verdict` is a decision about an `Ended`, and the value that
    // would have carried one never arrived.
    assert!(
        decisions(&asked).is_empty(),
        "nothing can be asked of the policy about an ending that never arrived"
    );
    assert!(
        settles(|| !sibling.is_running()).await,
        "and every other member is retired rather than left running and unwatched"
    );
}

/// The member the group could not hear is retired too, and this test exists
/// because its sibling above **cannot see that**.
///
/// `an_ending_that_panics_ends_the_group_as_ending_panicked` hands the doomed
/// name a `|| async {}` retire and no actor at all, so the only member it can
/// observe being retired is the healthy one. That left the interesting half
/// untested: `conduct` takes the reported member off the books *before* it
/// reads the reason, and on this path the reason never arrived.
///
/// **Both premises for taking it off the books need the reason to have
/// arrived.** `Forget` carries on without a member that ended; `StopAll` names
/// it rather than retiring an actor that has already ended. Here nothing ended,
/// as far as anyone knows — the future that was going to say so panicked. So the
/// member may be running, and its [`Retire`](keylink::compose::Retire) was the
/// only means anybody had of ending it. `Membership::new` calls carrying on in
/// that state "leaving a caller believing that member is watched".
///
/// Made red by `scripts/break-once.sh keylink/src/compose/group.rs`, replacing
/// the conductor's `            leaving.push(taken.retire);` with
/// `            drop(taken);` — the entry taken off the books and its means of
/// ending dropped with it, which is the defect this test was written against.
/// Watched: "the member whose ending panicked is retired as well: the group
/// cannot hear it, and it holds the only `Retire` anybody had for it". Exit 101,
/// by value, in 0.01s. That replacement restores the code exactly as it stood
/// before this test existed, so the break is the regression rather than an
/// invention — the test was watched failing against it first and passing after.
#[tokio::test]
async fn an_ending_that_panics_retires_the_member_it_could_not_hear() {
    let (_asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let doomed = ActorId::new("doomed").expect("a name");
    // A REAL actor behind the panicking ending, which is the whole difference
    // from the sibling guard: its `retire` aborts something observable.
    let unheard = worker::actor(Worker { me: 1 });
    let sibling = worker::actor(Worker { me: 2 });

    assert!(group
        .add(doomed.clone(), {
            let unheard = unheard.clone();
            Membership::new(panicking_ending(), move || async move {
                unheard.abort();
            })
        })
        .is_none());
    assert!(group
        .add(
            ActorId::new("sibling").expect("a name"),
            keylink::membership!(sibling),
        )
        .is_none());

    match bounded(group.ended()).await {
        GroupEnded::EndingPanicked { member, .. } => {
            assert_eq!(member, doomed, "the group names the member it lost");
        }
        other => panic!("a panicking ending must end the group as `EndingPanicked`, got {other:?}"),
    }

    // The half the sibling guard already covers, kept so that a failure below
    // cannot be read as "the teardown never ran at all".
    assert!(
        settles(|| !sibling.is_running()).await,
        "every other member is retired"
    );

    // THIS is the decision. Nobody else holds a means of ending this member —
    // the group consumed it at `add` — so a teardown that skips it leaves an
    // actor running that nothing can reach.
    assert!(
        settles(|| !unheard.is_running()).await,
        "the member whose ending panicked is retired as well: the group cannot \
         hear it, and it holds the only `Retire` anybody had for it"
    );
}

/// Two breaks, one per half, because the two guards are two pieces of caller code
/// at two moments — and they are now in two different files, which is the change
/// worth reading before running either.
///
/// **The closure half.** `scripts/break-once.sh keylink/src/compose/group.rs`,
/// replacing `retire_all`'s `        match catching(member.end) {` with
/// `        match Ok::<Retiring, crate::Panic>((member.end)()) {` — the closure
/// called uncaught, which is a spelling this function genuinely had, so it is
/// the regression rather than an invention. The `Ok::<_, _>` device is the one
/// `a_policy_that_panics_ends_the_group_rather_than_orphaning_it` uses on the
/// policy, and for the same reason: it removes the catch while leaving both
/// match arms typed. Verified to compile, with one `unused import: catching`
/// warning and nothing else — a warning under `cargo test`, so read the test
/// result and not the exit of the build. Watched: "a panicking retire does not
/// change WHY the group ended, got Dropped". Exit 101, by value, in 0.00s.
///
/// **The future half moved to `keylink/src/panic.rs`, and the reason is the
/// annotation on the set.** It used to read: replace
/// `Ok(fut) => retiring.push(guarded(fut)),` with
/// `Ok(fut) => retiring.push(fut),`, which worked only while the
/// `FuturesUnordered`'s item type was inferred from that one push. It is
/// annotated now — `FuturesUnordered<Guarded<Retiring>>`, which is what makes
/// break 5 expressible at all — so pushing a bare `Retiring` is `E0308` and
/// tests nothing. A break that stops compiling is the failure shaped most like
/// success.
///
/// So the guard is broken where it lives: `scripts/break-once.sh
/// keylink/src/panic.rs`, replacing
/// `            Err(panic) => Poll::Ready(Err(Panic::caught(panic))),` with
/// `            Err(panic) => std::panic::resume_unwind(panic),` — the catch
/// re-throwing, so the retire future's panic unwinds the conductor mid-teardown
/// and `Group::ended` answers `Dropped` on a dropped sender. Watched: "a
/// panicking retire does not change WHY the group ended, got Dropped". Exit 101,
/// by value, in 0.00s.
///
/// **The two halves print the SAME sentence, so the log cannot tell you which
/// one you ran.** Both make the conductor unwind, and both therefore leave
/// [`Group::ended`] answering `Dropped` at the identical assertion. Each run's
/// stdout also carries the *caught* member panic — "the retire closure refuses"
/// — because the panic hook prints before `catch_unwind` swallows it, so that
/// line is present under both and is not the failure. Run one substitution at a
/// time and label its log; there is no way to separate them afterwards.
///
/// **That break is wider than the one it replaces, and the width is the price.**
/// `guarded` is also what wraps a caller's ending future in `Group::add` and a
/// turn in `local.rs`, so this reddens `panic.rs`'s own
/// `guarded_turns_a_panic_into_a_value` and anything else that leans on the
/// catch. Run it against this test's filter and read that one result; a
/// whole-suite run under it says nothing about this guard in particular.
///
/// **What each count says, because one of them alone says nothing.** `entered`
/// is 2 because both bad members were reached: the closure ran, and the future
/// was polled. `retired` is 2 because both healthy members completed. `retired`
/// is the *one bad member costs one member* claim and it is worth nothing
/// without `entered`, since a teardown that skipped all four would also leave
/// `retired` at 2 relative to nothing. **`retire_panics` is what neither of them
/// could say**: that the two bad members were discarded rather than retired
/// anyway, and how many of them there were. It was `Err(_discarded) => {}` and a
/// dropped `Result` until now — a member vanishing out of a teardown with no
/// report anywhere — so the third assertion below is the one that turns this
/// from a silence into a number.
///
/// **Why this test exists at all.** `tests/zz_scratch_panic.rs` covered both
/// halves and was deleted as scratch on 2026-08-08. `tests/compose.rs` is now the
/// only file that names `Group`, so the deletion took the only tests of two
/// guards with it.
#[tokio::test]
async fn a_panicking_retire_costs_its_own_member_and_not_the_teardown() {
    let (_asked, policy) = recording(Verdict::Forget);
    let group = Group::new(policy);
    let retired = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(AtomicUsize::new(0));

    // The two bad members sit in the MIDDLE of the teardown order, so one healthy
    // member is issued before them and one after. An unwind through the loop
    // would orphan whatever follows the panic, and "follows" is what the member
    // added FIRST is: teardown runs in reverse insertion order. So this list is
    // written in reverse, and the comment on each name is its teardown position.
    assert!(group
        .add(
            ActorId::new("retired-last").expect("a name"),
            counting(&retired)
        )
        .is_none());
    assert!(group
        .add(
            ActorId::new("bad-future").expect("a name"),
            future_panics(&entered),
        )
        .is_none());
    assert!(group
        .add(
            ActorId::new("bad-closure").expect("a name"),
            closure_panics(&entered),
        )
        .is_none());
    assert!(group
        .add(
            ActorId::new("retired-first").expect("a name"),
            counting(&retired),
        )
        .is_none());
    assert_eq!(group.names().len(), 4, "four members, two of them bad");

    group.stop();
    match bounded(group.ended()).await {
        // The variant is the old claim — a panicking retire does not change WHY
        // the group ended — and the count is the new one. Both on one match, so
        // that neither can be read without the other.
        GroupEnded::Stopped { retire_panics } => assert_eq!(
            retire_panics, 2,
            "the two panics are counted rather than discarded, one per half: the \
             closure that panicked before it built a future, and the future that \
             panicked on a poll"
        ),
        other => panic!("a panicking retire does not change WHY the group ended, got {other:?}"),
    }
    assert_eq!(
        entered.load(Ordering::SeqCst),
        2,
        "both bad members were reached: the closure ran, and the future was polled"
    );
    assert_eq!(
        retired.load(Ordering::SeqCst),
        2,
        "both healthy members were retired: one bad member costs one member, \
         and the one issued after the panics is not orphaned by them"
    );
}

// ────────────────────────────────────────────────────────────── the keyed side

/// The one [`Ended`] no teardown path can synthesise, which is what makes a
/// waiter carrying it evidence about the **loop** rather than about the
/// teardown.
///
/// `compose::keyed`'s own four-path table is the argument:
/// [`System::abort`](keylink::System::abort) reports [`Ended::Vanished`] and
/// [`System::shutdown`](keylink::System::shutdown) reports [`Ended::Stopped`],
/// and neither one asks the loop anything. So a `GaveUp` that arrives at a
/// waiter came from the loop and from nowhere else.
fn gave_up(after: u32) -> Ended {
    let error: Box<dyn std::error::Error + Send + Sync> = "this member cannot serve at all".into();
    Ended::GaveUp {
        after,
        error: Arc::from(error),
    }
}

/// A keyed actor whose loop ends when the test says so, with the [`Ended`] the
/// test chose.
///
/// Hand-written through [`keylink::mount`] and not `worker::spawn_supervised`,
/// because these claims are about the reason the loop returned. A supervised
/// member's reason is its supervisor's, and making one end at all needs a turn
/// — which needs a served client, since a keyed mailbox is reachable over a QUIC
/// stream or not at all. Nothing below needs a message, so nothing below binds a
/// node.
fn ending_when(told: oneshot::Receiver<Ended>) -> worker::Mounted {
    keylink::mount(move |_inbox: worker::Mailbox| async move {
        // A dropped sender is a test that went away before it said anything.
        // `Vanished` is what an interrupted loop means, and it fails the
        // assertions below rather than hanging them.
        told.await.unwrap_or(Ended::Vanished)
    })
}

/// A second [`Lifecycle`], standing for the console an [`Endings`] must not
/// displace.
struct Watched {
    /// Every id this sink was told about, in the order it was told.
    seen: Arc<Mutex<Vec<ActorId>>>,
}

impl Lifecycle for Watched {
    fn mounted(&self, _id: &ActorId) {}

    fn ended(&self, id: &ActorId, _why: &Ended) {
        self.seen.lock().expect("not poisoned").push(id.clone());
    }
}

/// Made red by `scripts/break-once.sh keylink/src/compose/keyed.rs`, replacing
/// `Lifecycle::ended`'s `            let _ = waiter.tell.send(why.clone());`
/// with `            let _ = waiter.tell.send(Ended::Vanished);` — a waiter
/// answered with a reason the registry never gave it, which is exactly the
/// *synthesised* half of that module's four-path table arriving on the one path
/// that has a true reason. Watched: "the waiter carries the reason the loop
/// returned, fields and all: Vanished", where the loop returned
/// `GaveUp { after: 3 }`. Exit 101, by value, in 0.01s.
///
/// **This substitution reddens five tests, so run it under one filter.** It is
/// the only anchor in this file that does: every guard reaching an ending
/// through `send` fails under it. `a_watch_outliving_its_endings_resolves_vanished_rather_than_hanging`
/// is the exception and stays green, because it reaches `Vanished` through the
/// dropped-sender arm and never through `send` at all.
///
/// `Vanished` is the replacement and not some other constant, because it is what
/// [`System::abort`](keylink::System::abort) reports: a waiter that answered the
/// teardown's reason instead of the loop's would look exactly like this.
#[tokio::test]
async fn a_keyed_actors_own_ending_reaches_a_watch_future_with_its_reason() {
    let endings = Endings::new();
    let system = worker::System::builder(Eviction::Never)
        .lifecycle(endings.clone())
        .build();
    let id = ActorId::new("ledger/7").expect("a name");

    // Before the mount, and that order is the module's miss-window rather than a
    // style: an ending reported before its waiter is filed reaches nobody, and
    // that waiter then waits for the life of the system.
    let mut watching = Box::pin(endings.watch(&id));

    let (ends, told) = oneshot::channel();
    system
        .mount(id.clone(), ending_when(told))
        .expect("the system mounts it");

    // Nothing has ended, so nothing can have been reported. Boxed rather than
    // moved so that this one poll does not consume the future the assertion
    // below awaits — and `stop_all_ends_the_group_and_names_the_member_that_caused_it`
    // holds the control proving a `poll_once` here CAN be `Some`.
    spun().await;
    assert!(
        n0_future::future::poll_once(&mut watching).await.is_none(),
        "a mounted actor that has not ended reports nothing"
    );

    ends.send(gave_up(3))
        .expect("the loop is waiting for its ending");

    let why = bounded(watching).await;
    assert!(
        matches!(why, Ended::GaveUp { after: 3, .. }),
        "the waiter carries the reason the loop returned, fields and all: {why:?}"
    );
}

/// Made red by `scripts/break-once.sh keylink/src/actor/registry.rs`, replacing
/// `SystemBuilder::lifecycle`'s `        self.lifecycle.push(Arc::new(lifecycle));`
/// with `        self.lifecycle = vec![Arc::new(lifecycle)];` — the single slot
/// that method's own doc records as the defect it replaced, where a second
/// `.lifecycle(..)` displaced the first with no error, no warning and no
/// returned value. Watched: "both sinks are told … the `Endings` installed
/// second got `Some(Stopped)`, and the recorder installed first fired: false".
/// Exit 101, by value, in 0.00s.
///
/// **A second substitution, for the other direction.** Replace the same literal
/// with `        if self.lifecycle.is_empty() { self.lifecycle.push(Arc::new(lifecycle)); }`
/// — a single slot that keeps the FIRST sink rather than the last, which is the
/// same defect from the other end. Watched: "both sinks are told … the
/// `Endings` installed second got `None`, and the recorder installed first
/// fired: true". Exit 101, at the [`NO_PROGRESS`] bound, after 60.00s.
///
/// **Two substitutions, two directions, and the message names which one** —
/// which is the thing a guard on a list has to do and the easiest thing to
/// believe without checking. Both were measured against this body, not reasoned
/// about:
///
/// | the defect | `Endings` got | recorder fired | time |
/// |---|---|---|---|
/// | keeps only the LAST | `Some(Stopped)` | `false` | 0.00s |
/// | keeps only the FIRST | `None` | `true` | 60.00s |
/// | keeps NEITHER | `None` | `false` | 60.00s |
///
/// **Both observables are read before either is asserted, and that third row is
/// why.** Asserting the `Endings` first and aborting there would leave the
/// recorder unread, so keep-first and keep-nothing would produce one symptom
/// between them. Reading both into locals costs nothing when both are true and
/// separates all three when they are not. The bottom row is the one still
/// predicted rather than measured — no substitution here induces it.
#[tokio::test]
async fn two_lifecycles_on_one_system_both_fire() {
    let endings = Endings::new();
    let seen: Arc<Mutex<Vec<ActorId>>> = Arc::new(Mutex::new(Vec::new()));
    let system = worker::System::builder(Eviction::Never)
        .lifecycle(Watched {
            seen: Arc::clone(&seen),
        })
        .lifecycle(endings.clone())
        .build();
    let id = ActorId::new("ledger/7").expect("a name");
    let watching = endings.watch(&id);

    let (ends, told) = oneshot::channel();
    system
        .mount(id.clone(), ending_when(told))
        .expect("the system mounts it");
    ends.send(Ended::Stopped)
        .expect("the loop is waiting for its ending");

    // **Both sinks are READ before either is asserted.** Asserting the first and
    // aborting would leave the second observable unread, and three broken
    // implementations then share two symptoms: keep-first and keep-nothing both
    // die here with nothing said about the recorder. Reading both first costs
    // nothing when they are both true, and names both when they are not.
    //
    // `timeout` rather than `bounded`, because `bounded` panics on expiry and the
    // whole point is to carry the answer down to the assertion instead.
    let told_endings = n0_future::time::timeout(NO_PROGRESS, watching).await.ok();
    let told_recorder = settles(|| !seen.lock().expect("not poisoned").is_empty()).await;

    // THIS is the decision, and it is about the sink that was installed FIRST: a
    // program that adds `.lifecycle(endings)` to a builder that already had
    // `.lifecycle(events)` must not lose its console to do it.
    assert!(
        matches!(told_endings, Some(Ended::Stopped)) && told_recorder,
        "both sinks are told, because a second `.lifecycle(..)` adds and does not \
         displace — the `Endings` installed second got {told_endings:?}, and the \
         recorder installed first fired: {told_recorder}"
    );
    assert_eq!(
        seen.lock().expect("not poisoned").clone(),
        vec![id],
        "and about that actor, once"
    );
}

/// Made red by `scripts/break-once.sh keylink/src/compose/keyed.rs`, replacing
/// `Lifecycle::ended`'s `        for waiter in waiters {` with
/// `        for waiter in waiters.into_iter().take(1) {` — a fan-out that reaches
/// the first waiter and stops, which is the shape
/// `a_publisher_fans_out_to_three_subscribers_each_on_its_own_feed` breaks on the
/// topic. Watched: "the second waiter filed under one name is answered, and owns
/// its own copy of the reason rather than a share of one: Some(Vanished)".
/// Exit 101, by value, in 0.00s — the waiter `take(1)` skipped has its sender
/// dropped rather than left hanging, so it answers `Vanished` instead of the
/// reason that was reported.
///
/// Every waiter under one name is answered, which is what the `Vec` in that map
/// is for. One waiter per name would answer whichever `watch` came last and lose
/// the other with no error anywhere.
///
/// **`poll_once` and not an `await`, and that is about the arrangement.** The
/// sends happen inside [`Lifecycle::ended`], which is called here by hand, so
/// both answers are in their channels the moment it returns. One poll each is
/// therefore the whole of what this needs, and a fan-out that stopped early
/// fails it in 0.00s rather than at the [`NO_PROGRESS`] bound.
/// `a_watch_outliving_its_endings_resolves_vanished_rather_than_hanging` holds
/// the control proving a `poll_once` in this file CAN be `None`.
///
/// The sibling claim — *and never twice* — is not here, and the module doc is
/// why: `ended` takes the entry out before it sends, so a second report under
/// the same name finds nothing to answer. That is the miss-window, which
/// `an_abandoned_watch_unfiles_its_own_waiter_and_not_a_later_one` reads from
/// the map's side.
#[tokio::test]
async fn two_watchers_on_one_name_are_both_answered() {
    let endings = Endings::new();
    let id = ActorId::new("ledger/7").expect("a name");

    // Two waiters, one name, neither abandoned. A map of one waiter per name
    // would answer whichever `watch` came last and lose the other silently.
    let mut first = Box::pin(endings.watch(&id));
    let mut second = Box::pin(endings.watch(&id));

    endings.ended(&id, &gave_up(2));

    for (which, waiter) in [("first", &mut first), ("second", &mut second)] {
        let why = n0_future::future::poll_once(waiter).await;
        assert!(
            matches!(why, Some(Ended::GaveUp { after: 2, .. })),
            "the {which} waiter filed under one name is answered, and owns its own \
             copy of the reason rather than a share of one: {why:?}"
        );
    }
}

/// Two breaks, one per half, because the two halves are two claims. Both with
/// `scripts/break-once.sh keylink/src/compose/keyed.rs`.
///
/// **Not a later one.** Replace `Awaited`'s
/// `            under.get_mut().retain(|waiter| waiter.generation != mine);` with
/// `            under.get_mut().clear();` — the whole entry emptied on one
/// abandonment, which is the defect the comment on that very line names: a live
/// caller left waiting on a future nothing can resolve any more. Watched: "the
/// waiter filed after the abandoned one is still filed, and carries the true
/// reason: Vanished". Exit 101, **by value**, in 0.00s. Re-measured against the
/// re-quoted literal after `slot` became `generation`, rather than carried.
///
/// **This record predicted the bound and the prediction was wrong.** It said to
/// expect a timeout, on the reasoning that *unfiling the wrong waiter produces
/// no wrong value, it produces no value*. That sentence stops one step early.
/// `clear()` drops every [`Waiter`] in the vector, the kept one included;
/// dropping a `Waiter` drops its `oneshot::Sender`; and `Awaited::poll` maps a
/// dropped sender to [`Ended::Vanished`] rather than to `Pending`. So it does
/// produce a wrong value, and the guard reddens in no time at all. The error was
/// reasoning about the map without following the `Drop` out of it — the same
/// shape as trusting a mechanism you have not opened.
///
/// **Its own.** Replace `                under.remove();` with
/// `                let _ = &under;` — the emptied key left in the map, so the
/// map grows with names watched *once* rather than with names watched *now*,
/// which is the bound the type claims. Watched: "and neither does an abandoned
/// one: Endings { watching: 1 }". Exit 101, by value, in 0.00s.
///
/// **The FIRST `watching: 0` assertion has no break and is a control, not a
/// guard.** It passes under both of the substitutions above, measured. It is not
/// vacuous — a non-zero count cannot satisfy it, because `Debug` prints one
/// field — but nothing in this file can redden it, so read it as the statement
/// that the report emptied the map and read the second one as the guard.
///
/// No [`System`](keylink::System) here at all. [`Endings`] is a [`Lifecycle`],
/// so the report is made by hand, and a registry between the two would add
/// scheduling to a test about a map.
#[tokio::test]
async fn an_abandoned_watch_unfiles_its_own_waiter_and_not_a_later_one() {
    let endings = Endings::new();
    let id = ActorId::new("ledger/7").expect("a name");

    // Two waiters under one name, filed in that order, and the FIRST is
    // abandoned. Taking the whole entry would unwatch the second one silently.
    let abandoned = endings.watch(&id);
    let kept = endings.watch(&id);
    drop(abandoned);

    endings.ended(
        &id,
        &Ended::Panicked {
            message: "the turn refused".to_string(),
        },
    );

    let why = bounded(kept).await;
    assert!(
        matches!(&why, Ended::Panicked { message } if message == "the turn refused"),
        "the waiter filed after the abandoned one is still filed, and carries the true reason: {why:?}"
    );

    // The other half, and it is a different claim: a waiter abandoned and never
    // reported takes its own key back out. The report above already emptied the
    // map, so this watches and drops a second time to say so.
    assert!(
        format!("{endings:?}").contains("watching: 0"),
        "a reported name leaves no key behind: {endings:?}"
    );
    drop(endings.watch(&id));
    assert!(
        format!("{endings:?}").contains("watching: 0"),
        "and neither does an abandoned one: {endings:?}"
    );
}

/// Made red by `scripts/break-once.sh keylink/src/compose/keyed.rs`, replacing
/// `Awaited::poll`'s `            Poll::Ready(Err(_)) => Ended::Vanished,` with
/// `            Poll::Ready(Err(_)) => Ended::Stopped,` — the whole system going
/// away reported as a clean stop, which is the one thing a caller has to be able
/// to tell it from. Watched: "a waiter nothing can ever report to is answered
/// rather than left waiting: Stopped". Exit 101, by value, in 0.00s.
///
/// **That covers the value half only, and the liveness half needs its own
/// substitution.** The same arm as
/// `            Poll::Ready(Err(_)) => return Poll::Pending,` was expected to
/// redden at the [`NO_PROGRESS`] bound rather than by assertion, so the record
/// said to run it second and expect the minute. Watched: it reddens in 0.00s and
/// **not** at the bound, and not at any assertion in this file. Exit 101, with
/// the test panicking inside tokio at `oneshot.rs:1291`, "called after
/// complete".
///
/// **So this substitution does not model the liveness claim, and that is the
/// finding rather than the result.** Returning `Pending` after the receiver has
/// already answered `Err` leaves the completed [`oneshot::Receiver`] to be
/// polled a second time, which tokio refuses by panicking. The guard goes red,
/// but for a broken receiver contract rather than for the hang the claim is
/// about — a break that fails for its own reason.
///
/// **The liveness half has a defect that DOES model it, and it is not a
/// one-line substitution.** `Awaited` holds a [`Weak`](std::sync::Weak) to the
/// waiter map; a **strong** [`Arc`] there keeps every `oneshot::Sender` alive
/// past the last [`Endings`], so the receiver never errs, the `Err` arm above
/// is never reached, and the caller waits for ever — which is exactly the hang
/// this test denies. That is three literals at once: the field's type, the
/// `Arc::downgrade` that fills it, and the `Weak::upgrade` in `Drop`.
/// `break-once.sh` replaces one literal, so it cannot express this, and the two
/// halves of a three-site edit do not compile on their own.
///
/// Recorded as a **regression pin and not a guard**, which is
/// `members_are_reported_in_the_order_they_were_added`'s label for the same
/// shape: the claim is real and this file cannot redden it. The value half above
/// is a guard and was measured; the liveness half is this paragraph.
///
/// A waiter that nothing can ever report to is answered rather than left
/// waiting, and it is answered with the one reason that says *the whole thing
/// went away* rather than *this actor stopped*.
///
/// Two claims on one arm, and they fail differently: a wrong [`Ended`] here is a
/// caller who cannot tell a dead system from a clean stop, and no [`Ended`] at
/// all is the hang [`Endings`]' own documentation says it refuses — *an
/// `Endings` that was built, watched and never installed would otherwise hang
/// its callers with no signal at all*. The first fails this by value, the second
/// at the [`NO_PROGRESS`] bound.
///
/// Built, watched and never installed is exactly that sentence's case, so no
/// [`System`](keylink::System) is bound here: the map going away with its last
/// clone is the whole of what happens, and the [`Endings`] the system would have
/// held is the clone that is missing.
#[tokio::test]
async fn a_watch_outliving_its_endings_resolves_vanished_rather_than_hanging() {
    let endings = Endings::new();
    let id = ActorId::new("ledger/7").expect("a name");
    let mut watching = Box::pin(endings.watch(&id));

    // Every chance to resolve early, and it must not: an `Endings` that is still
    // alive and has reported nothing owes this waiter nothing yet.
    spun().await;
    assert!(
        n0_future::future::poll_once(&mut watching).await.is_none(),
        "a live `Endings` that reported nothing answers nothing"
    );

    // The last clone. The future holds a `Weak` rather than keeping the map
    // alive, which is what makes this case reachable at all.
    drop(endings);
    let why = bounded(watching).await;
    assert!(
        matches!(why, Ended::Vanished),
        "a waiter nothing can ever report to is answered rather than left waiting: {why:?}"
    );
}

/// **The same literal as
/// `a_keyed_actors_own_ending_reaches_a_watch_future_with_its_reason`, and a
/// different claim.** `scripts/break-once.sh keylink/src/compose/keyed.rs`,
/// replacing `            let _ = waiter.tell.send(why.clone());` with
/// `            let _ = waiter.tell.send(Ended::Vanished);`. There the claim is
/// that a *waiter* is handed the loop's reason. Here it is that the reason
/// survives the two hops a group adds — through the hand-written [`Membership`]
/// below, into the policy's argument, and out as [`GroupEnded::Member`].
/// Watched: "and carries the reason its loop returned rather than one a teardown
/// would have synthesised: Vanished", where the loop returned
/// `GaveUp { after: 1 }`. Exit 101, by value, in 0.00s. That is
/// [`GroupEnded::Member`]'s own field failing, two hops further along than where
/// the sibling guard fails on the same substitution, which is what makes the two
/// records two claims.
///
/// The file already runs one literal for two guards on the policy's reason, and
/// records why they are not one claim; this is that arrangement on the keyed
/// side. `Vanished` is the replacement because it is what
/// [`System::abort`](keylink::System::abort) reports and `abort` is what this
/// member's `retire` calls — a group reading the teardown's reason instead of
/// the loop's would be indistinguishable from a correct one under any other
/// value.
#[tokio::test]
async fn a_keyed_actor_is_a_group_member_end_to_end() {
    let endings = Endings::new();
    let system = worker::System::builder(Eviction::Never)
        .lifecycle(endings.clone())
        .build();
    let id = ActorId::new("ledger/7").expect("a name");
    let (asked, policy) = recording(Verdict::StopAll);
    let group = Group::new(policy);

    let watching = endings.watch(&id);
    let (ends, told) = oneshot::channel();
    system
        .mount(id.clone(), ending_when(told))
        .expect("the system mounts it");

    // The two values a `Membership` wants, both from the keyed side and neither
    // from a handle: the ending from `Endings::watch`, the retiring from
    // `System::abort`. `abort` and not `shutdown`, for the reason
    // `Membership::new` gives about a member sitting in a backoff.
    let retiring = system.clone();
    let retired = id.clone();
    assert!(
        group
            .add(
                id.clone(),
                Membership::new(watching, move || async move {
                    retiring.abort(&retired).await;
                }),
            )
            .is_none(),
        "nothing was displaced"
    );

    // A local sibling, so that `StopAll` has a member to retire that the policy
    // was never asked about — the half a `GroupEnded` cannot carry. Heterogeneous
    // on purpose: one keyed member and one local one in the same group is the
    // thing this module is not generic over a handle type in order to allow.
    let sibling = worker::actor(Worker { me: 2 });
    assert!(group
        .add(
            ActorId::new("sibling").expect("a name"),
            keylink::membership!(sibling),
        )
        .is_none());

    ends.send(gave_up(1))
        .expect("the loop is waiting for its ending");

    match bounded(group.ended()).await {
        GroupEnded::Member { member, reason } => {
            assert_eq!(member, id, "the group names the keyed member that ended");
            assert!(
                matches!(reason, Ended::GaveUp { after: 1, .. }),
                "and carries the reason its loop returned rather than one a teardown \
                 would have synthesised: {reason:?}"
            );
        }
        other => panic!("a keyed member ending must end the group as `Member`, got {other:?}"),
    }

    // **What the POLICY was handed, which is a different question from what the
    // caller was told above.** `a_fatal_refusal_reaches_the_policy_without_a_restart`
    // records the guard that was green under exactly this gap.
    let seen = decisions(&asked);
    assert_eq!(seen.len(), 1, "the policy was asked once");
    assert_eq!(seen[0].0, id, "about the keyed member");
    assert!(
        matches!(seen[0].1, Ended::GaveUp { after: 1, .. }),
        "and handed the keyed loop's own reason: {:?}",
        seen[0].1
    );
    assert!(
        settles(|| !sibling.is_running()).await,
        "and the sibling nobody asked about is retired, not left serving"
    );
}
