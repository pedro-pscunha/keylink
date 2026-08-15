//! The compositions an actor system grows, each with its backpressure written
//! down.
//!
//! ```sh
//! cargo run -p keylink --example foundry
//! ```
//!
//! Nothing here touches the wire. No node is bound, no ALPN moves, and every
//! type comes out of [`keylink::compose`] — these are compositions of things the
//! crate already has: a handle, a `feed`, an [`ActorId`] and a clock.
//!
//! `trading_desk/venue.rs` covers the fan-out half, with [`Topic`](keylink::compose::Topic)
//! and [`LossyTopic`](keylink::compose::LossyTopic). This covers the rest.
//!
//! | act | pattern | what it decides |
//! |---|---|---|
//! | 1 | [`Pool`] | which of N, by a cursor. The caller then decides |
//! | 2 | [`Factory`] | which of N, by a [`Routing`] rule. It decides and answers |
//! | 3 | [`Backpressure::Shed`] against a [`Permit`] | who loses the arguments when a mailbox is full |
//! | 4 | [`KeyedTopic`] | fan-out isolated per key |
//! | 5 | [`Names`] | name to actor, unregistered when the actor ends |
//! | 6 | [`Group`] | a set that ends together, and members joining and leaving it |
//! | 7 | [`after`] / [`every`] | a delayed run and a repeating one |
//!
//! # A shed mailbox loses the caller's arguments, and a permit does not
//!
//! [`Backpressure::Shed`] refuses at once when the mailbox is full, and the
//! message the generated door was about to build is never built — there is no
//! value handed back, because the door assembles it inside itself. A caller that
//! cannot afford that takes a permit first, which makes the admission decision
//! **before** the payload moves. [`Factory::dispatch`] hands back a permit for
//! exactly this reason, so a caller under `Shed` loses nothing through it. Act 3
//! measures both halves.

use std::error::Error as StdError;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::compose::{
    after, every, Added, Factory, Group, GroupEnded, KeyedSubscription, KeyedTopic, Names,
    Overflow, Pool, Retire, Routing, Scheduled, Shard, Verdict,
};
use keylink::prelude::*;
use keylink::{depth, ActorId, Backpressure, Caller, Ended};

keylink::service! {
    service Kiln;

    /// Which kiln answered, so routing is measured rather than recomputed.
    rpc Whoami() -> u64;

    /// One batch fired.
    rpc Fire(batch: u64) -> u64;

    /// How many batches this kiln fired.
    rpc Fired() -> u64;
}

// A SECOND declaration, and the split is forced rather than chosen. A service
// that declares a `feed` cannot be spawned through `actor(state)` at all — the
// generated check refuses it, because an unwired feed is a stream that never
// yields and a caller cannot tell that from a quiet one. So the pool, the
// factory and the group above keep the door that takes no feeds, and the keyed
// topic below gets a declaration that has one.
keylink::service! {
    service Line;

    /// How much this line has taken off the topic.
    rpc Taken() -> u64;

    /// Orders arriving off a keyed topic.
    feed Orders(order: u64);
}

/// One production line, fed and nothing else.
#[derive(Default)]
struct Line {
    taken: u64,
}

impl line::Actor for Line {
    async fn taken(&mut self, _cx: Caller<'_>) -> u64 {
        self.taken
    }

    async fn on_orders(&mut self, order: u64) {
        self.taken += order;
    }
}

struct Kiln {
    me: u64,
    fired: u64,
}

impl Kiln {
    fn new(me: u64) -> Self {
        Self { me, fired: 0 }
    }
}

impl kiln::Actor for Kiln {
    async fn whoami(&mut self, _cx: Caller<'_>) -> u64 {
        self.me
    }

    async fn fire(&mut self, _cx: Caller<'_>, batch: u64) -> u64 {
        self.fired += 1;
        batch
    }

    async fn fired(&mut self, _cx: Caller<'_>) -> u64 {
        self.fired
    }
}

/// A count, without an `unwrap` this workspace denies.
fn count(n: usize) -> Result<NonZeroUsize, Box<dyn StdError>> {
    NonZeroUsize::new(n).ok_or_else(|| "a member count is not zero".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. A pool: N interchangeable actors behind one cursor.
    //
    //  The pool holds no queue at all. `pick` hands back a handle and the wait
    //  is the ordinary mailbox permit, so a slow member slows its own callers
    //  and nobody else's.
    let pool: Pool<kiln::Handle<Kiln>> = Pool::from_fn(count(3)?, |member| {
        kiln::actor(Kiln::new(member.index() as u64))
    });
    let mut visited = Vec::new();
    for _ in 0..3 {
        let (member, handle) = pool.pick();
        visited.push((member.index(), handle.whoami().await?));
    }
    println!("three picks reached members {visited:?}");
    assert_eq!(
        visited,
        vec![(0, 0), (1, 1), (2, 2)],
        "a cursor advances once per pick, so three picks are three members"
    );
    for member in pool.members() {
        member.stop().await?;
    }

    // ── 2. A factory: the same N, with the cursor replaced by a rule.
    //
    //  `ByKey` is `hash(key) % N`, so one key always reaches one member. Which
    //  member is deliberately not recomputed here: hashing the key again would
    //  compare the routing against a copy of itself. What is asserted is the
    //  property a caller relies on — the same key, the same member, every time.
    let factory: Factory<kiln::Handle<Kiln>, u64> =
        Factory::from_fn(count(4)?, Routing::ByKey(Shard::by_hash()), |member| {
            kiln::actor(Kiln::new(member.index() as u64))
        });
    let mut homes = Vec::new();
    for _ in 0..4 {
        homes.push(
            factory
                .dispatch_keyed(77, |member| member.whoami())
                .await??,
        );
    }
    println!("four dispatches of one key all reached member {}", homes[0]);
    assert!(
        homes.windows(2).all(|pair| pair[0] == pair[1]),
        "one key belongs to one member: {homes:?}"
    );

    //  `RoundRobin` is the other rule on the same type, and a caller changes
    //  between them without changing anything else.
    let spread: Factory<kiln::Handle<Kiln>, ()> =
        Factory::from_fn(count(4)?, Routing::RoundRobin, |member| {
            kiln::actor(Kiln::new(member.index() as u64))
        });
    let mut reached = Vec::new();
    for _ in 0..4 {
        reached.push(spread.dispatch(|member| member.whoami()).await??);
    }
    reached.sort_unstable();
    assert_eq!(reached, vec![0, 1, 2, 3], "a cursor visits each once");
    println!("four round-robin dispatches reached {reached:?}");

    // ── 3. What a full mailbox costs, and who pays it.
    //
    //  One slot, and the policy is `Shed`. Holding a permit is what makes this
    //  a measurement rather than a race: this caller owns the only slot, so the
    //  mailbox is full for certain rather than probably.
    let slow = kiln::setup(Kiln::new(9))
        .backpressure(Backpressure::Shed(count(1)?))
        .actor();
    let permit = Own::reserve(&slow).await?;
    assert!(
        matches!(Own::try_reserve(&slow), Ok(None)),
        "the one slot is held, so there is no second one"
    );

    //  The ordinary door refuses at once, and the arguments it was carrying are
    //  gone: the generated door assembles the message inside itself, so there
    //  is no value for a refusal to hand back.
    let shed = slow
        .fire(2)
        .await
        .err()
        .ok_or("a full mailbox under Shed must refuse")?;
    //  The KIND, not merely an error. An actor that had stopped would also
    //  refuse, and it would mean something else entirely.
    assert!(
        matches!(shed, keylink::Unanswered::Full),
        "a Shed policy refuses on fullness, got {shed:?}"
    );
    println!("the full mailbox refused a call outright: {shed:?}");

    //  The permit is the other half. The admission decision was made before the
    //  payload moved, so spending it cannot fail on fullness — which is why
    //  `Factory::dispatch` hands one back rather than calling the door.
    let spent = permit.fire(3).await?;
    println!("and the permit taken beforehand was spent on batch {spent}");
    assert_eq!(slow.fired().await?, 1, "only the admitted call ran");
    slow.stop().await?;

    // ── 4. A keyed topic: fan-out isolated per key.
    //
    //  A subscriber routed to one key never sees another key's items, so one
    //  busy key cannot fill a channel belonging to a quiet one.
    let orders: KeyedTopic<u64, u64> = KeyedTopic::new(Overflow::BlockPublisher(depth!(4)));
    let east: KeyedSubscription<u64, u64> = orders.route(1);
    let west = orders.route(2);
    let east_line = line::setup(Line::default())
        .feeds(line::Feeds::new(east))
        .actor();
    let west_line = line::setup(Line::default())
        .feeds(line::Feeds::new(west))
        .actor();

    let fanout = orders.publish(&1, 5).await;
    println!(
        "publishing to one key reached {} subscriber(s)",
        fanout.delivered
    );
    assert_eq!(fanout.delivered, 1, "one key, one subscriber");
    for _ in 0..200 {
        if east_line.taken().await? == 5 {
            break;
        }
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(east_line.taken().await?, 5, "the routed line read it");
    assert_eq!(west_line.taken().await?, 0, "and the other key saw nothing");

    // ── 5. Names: a map from a name to a live actor, kept honest by the actor.
    //
    //  The registration ends when the actor does, so a name never points at
    //  something that is gone. That is the whole difference between this and a
    //  `HashMap` a caller maintains by hand.
    let names: Names<kiln::Handle<Kiln>> = Names::new();
    let bay = ActorId::new("kiln/bay-1")?;
    let held = kiln::actor(Kiln::new(11));
    let displaced = names.register(bay.clone(), held.clone(), held.ended());
    assert!(displaced.is_none(), "the name was free");
    assert_eq!(names.registered(), 1);
    assert!(names.get(&bay).is_some(), "the name resolves");

    held.stop().await?;
    for _ in 0..200 {
        if names.registered() == 0 {
            break;
        }
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(names.registered(), 0, "an ended actor unregisters itself");
    println!("the name was released by the actor ending, not by a caller");

    // ── 6. A group: a set of actors that end together.
    //
    //  The policy is asked about every ending and answers one of two things.
    //  `StopAll` retires the rest; `Forget` carries on without it. Nothing can
    //  call *into* a group, which is what keeps its one waiting decision from
    //  being an edge a cycle can be built from.
    let group = Group::new(|_who, _why| Verdict::StopAll);
    let first = kiln::actor(Kiln::new(21));
    let second = kiln::actor(Kiln::new(22));
    let alpha = ActorId::new("kiln/alpha")?;
    let beta = ActorId::new("kiln/beta")?;
    assert!(group
        .add(alpha.clone(), keylink::membership!(first))
        .is_filed());
    assert!(group
        .add(beta.clone(), keylink::membership!(second))
        .is_filed());
    assert_eq!(group.names().len(), 2);

    //  A name is REPLACED rather than refused, for `Names::register`'s reason:
    //  a name is freed asynchronously, so refusing a taken one would refuse a
    //  restart nondeterministically. What comes back is a `Retire` and not a
    //  `Membership`, because the displaced member's ending now belongs to the
    //  group — ending it is the only thing left to do with it, and the type
    //  says so.
    let stand_in = kiln::actor(Kiln::new(23));
    match group.add(beta.clone(), keylink::membership!(stand_in)) {
        Added::Displaced(retire) => {
            retire.retire().await;
            println!("a second member under one name displaced the first, and retired it");
        }
        other => return Err(format!("a taken name displaces rather than files: {other:?}").into()),
    }
    //  Await the ENDING rather than reading `is_running`, which is advisory and
    //  says so: `retire` aborts, and the task has not necessarily observed that
    //  by the next line. The first spelling of this passed on my machine and
    //  failed in the gate, which is the whole argument for waiting on the fact
    //  instead of polling a flag.
    let gone = tokio::time::timeout(Duration::from_secs(5), second.ended())
        .await
        .map_err(|_| "the displaced member never ended")?;
    assert!(
        matches!(gone, Ended::Vanished),
        "a retired member is aborted rather than stopped, got {gone:?}"
    );
    assert_eq!(
        group.names().len(),
        2,
        "and the name is still watched, by its replacement"
    );

    //  Leaving on purpose is the other door, and it hands back the same
    //  `Retire`. A caller that takes a member out of a group is then holding
    //  the only thing that can end it.
    let taken: Retire = group.remove(&beta).ok_or("a watched name can be removed")?;
    taken.retire().await;
    assert_eq!(group.names(), vec![alpha.clone()], "one member left");
    println!("beta was removed and retired; the group watches 1");

    //  What discriminates here is `GroupEnded::Member`, which NAMES the member
    //  the policy was asked about. The reason does not: `abort` produces
    //  `Ended::Vanished`, and so does the group retiring every other member,
    //  because `membership!` retires by calling `abort` itself. `stop` would
    //  have produced `Ended::Stopped` and told the two apart.
    first.abort();
    match group.ended().await {
        GroupEnded::Member { member, reason } => {
            println!("the group ended because {member} did: {reason:?}");
            assert_eq!(member, alpha);
        }
        other => return Err(format!("a StopAll policy ends on a member: {other:?}").into()),
    }
    assert!(
        !second.is_running(),
        "the rest of the group was retired with it"
    );

    // ── 7. A clock, on a handle a caller can cancel.
    //
    //  `every` never re-enters a run in progress and never queues a missed
    //  tick, so the period is a floor on the gap rather than a rate.
    let ticks = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&ticks);
    let repeating: Scheduled = every(Duration::from_millis(10), move || {
        let counted = Arc::clone(&counted);
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
        }
    });
    let once = Arc::new(AtomicU32::new(0));
    let fired = Arc::clone(&once);
    let delayed = after(Duration::from_millis(20), move || {
        let fired = Arc::clone(&fired);
        async move {
            fired.fetch_add(1, Ordering::SeqCst);
        }
    });

    for _ in 0..200 {
        if ticks.load(Ordering::SeqCst) >= 3 && delayed.is_finished() {
            break;
        }
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ticks.load(Ordering::SeqCst) >= 3, "the schedule repeats");
    assert_eq!(once.load(Ordering::SeqCst), 1, "a delayed run runs once");
    println!(
        "the repeating run ticked {} time(s) and the delayed one finished: {}",
        ticks.load(Ordering::SeqCst),
        delayed.is_finished()
    );

    //  Dropping a `Scheduled` cancels it. `detach` is the opt-out, for work that
    //  must outlive the handle.
    drop(repeating);
    let settled = ticks.load(Ordering::SeqCst);
    n0_future::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(
        ticks.load(Ordering::SeqCst),
        settled,
        "dropping the handle stops the schedule"
    );
    println!("dropping the handle stopped it at {settled} tick(s)");

    east_line.stop().await?;
    west_line.stop().await?;
    for member in factory.members() {
        member.stop().await?;
    }
    for member in spread.members() {
        member.stop().await?;
    }
    Ok(())
}
