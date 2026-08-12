//! The seven patterns every actor system grows, each with its backpressure
//! written down.
//!
//! Nothing here is on the wire. No iroh type appears in this module and no ALPN
//! moves — these are compositions of things the crate already has: a [`Handle`],
//! a `feed`, an [`ActorId`](crate::ActorId), and [`n0_future`]'s clock.
//!
//! **`service!` generates exactly one line because this module exists**, and that
//! sentence used to read "not one line differently". The line is the
//! [`Reserving`] impl on every generated `Handle`, which is what lets [`Factory`]
//! ask a member for a slot without naming the service. It is the one thing a
//! composition genuinely could not do from outside: the generated handle's
//! mailbox is private to it.
//!
//! | pattern | what it is |
//! |---|---|
//! | [`Topic`] / [`KeyedTopic`] | fan-out. One publisher, N subscribers, each receiving on a `feed`, optionally isolated by key |
//! | [`Pool`] | N interchangeable actors behind one cursor, each saying which it is |
//! | [`Factory`] | the same N, with the cursor replaced by a [`Routing`] rule — including `hash(key) % N` |
//! | [`after`] / [`every`] | a delayed run and a repeating one, on a cancellable handle |
//! | [`Names`] | name → local actor, unregistered when the actor ends |
//! | [`Group`] | a set of actors that end together, or one the rest carry on without |
//! | [`Endings`] | lifecycle ending → future, for compositions that need keyed waiters |
//!
//! Six of the rows are the first six patterns; [`Endings`] is a seventh **row**
//! and not a seventh pattern
//! — it adapts [`Lifecycle::ended`](crate::Lifecycle::ended) for compositions that
//! want to own keyed ending futures. A one-off keyed observation can use
//! [`Registry::ended`](crate::Registry::ended) directly. The seventh **pattern** has
//! no row, and that is the finding rather than an oversight:
//! handle ergonomics turned out to be a table rather than a type — all of it
//! already exists under keylink's own names, and the last section here is the
//! mapping and the argument for not adding an alias.
//!
//! [`Factory`] and [`Pool`] are one pattern's two halves rather than two
//! patterns, and the table lists both because a caller chooses between them: a
//! pool hands back a handle and lets the caller decide, a factory decides and
//! hands back the answer. A factory *holds* a pool, so there is one cursor, one
//! `Box<[H]>` and one `Arc` between them.
//!
//! # Every one of these states its backpressure, because that is what was missing from the versions this is modelled on
//!
//! `kameo/actors/` implements the table's first four in about 2,400 lines, and
//! its versions predate the argument this crate is built around: they assume an
//! **unbounded** mailbox, so a slow consumer is answered by buffering and the
//! buffer has no bound. That converts a latency problem into memory growth,
//! which is the one kameo feature keylink's actor model *actively rejects*
//! rather than defers.
//!
//! A fan-out is where that failure hides best. "Publish to everybody" with a
//! queue per subscriber is an unbounded queue per subscriber, and it is invisible
//! until the day one subscriber is slow. So:
//!
//! | pattern | what a slow consumer costs | how it is bounded |
//! |---|---|---|
//! | [`Topic`] with [`Overflow::BlockPublisher`] | the publisher waits after the configured depth is full | a finite, non-zero channel per subscriber. [`publish`](Topic::publish) returns when every channel has accepted the item, not when every subscriber has consumed it |
//! | [`Topic`] with [`Overflow::BlockThenDrop`] | the publisher waits up to that subscriber's [`Patience`], then unsubscribes it loudly | a finite, non-zero channel per subscriber. Accepted buffered items drain before the stream ends |
//! | [`Topic`] with [`Overflow::DropSubscriber`] | the subscriber is unsubscribed loudly as soon as the configured depth is full | a finite, non-zero channel per subscriber. The item that exceeds the bound ends that subscriber's stream, so its actor ends as [`Ended::Feed`](crate::Ended::Feed) |
//! | [`LossyTopic`] with [`Lossy::DropNewest`] | new items are dropped independently for each full subscriber, which remains subscribed and receives the exact loss count until `u64` saturation, then at least `u64::MAX`, in-band | one finite, non-zero channel per live subscriber, plus one saturating pending-loss counter per subscriber |
//! | [`LossyTopic`] with [`Lossy::DropOldest`] | old items are overwritten for every subscriber that falls behind, which receives Tokio's exact lag count in-band | one shared finite broadcast ring, independent of subscriber count |
//! | [`Pool`] | the caller waits, on that member's own mailbox | the pool holds **no** queue at all. [`pick`](Pool::pick) hands back a handle; the wait is the ordinary mailbox permit |
//! | [`Factory`] | the caller waits, on the chosen member's own mailbox | the factory holds **no** queue at all. [`dispatch`](Factory::dispatch) hands back a [`Permit`](crate::Permit); the wait is the ordinary mailbox permit, taken before the caller's arguments move |
//! | [`every`] | the next run is late | a run in progress is never re-entered and a missed tick is never queued. The period is a floor on the *gap*, not a rate |
//! | [`Names`] | nothing waits | it is a map and a lock. No channel, no queue; bounded by the number of live names |
//! | [`Group`] | a member slow to die delays the group's next **decision** | one decision at a time, in one task. Endings are futures rather than queue entries, so N members are at most N pending endings and a death allocates nothing |
//! | [`add`](Group::add) while the policy is running | its ending waits in a staging list until the next wake — the member itself is filed at once, and a teardown before that wake retires it | the number of `add` calls in flight, drained to empty on that wake. A staging list, not a queue that grows against a slow consumer |
//! | [`Endings`] | nothing waits | a map, a lock, and one `oneshot` per waiter. No channel is ever full, so [`ended`](crate::Lifecycle::ended) cannot block a mount; bounded by the number of **live** [`watch`](Endings::watch) futures, and dropping one takes its waiter back out |
//!
//! **There is no hidden option.** A fan-out either applies backpressure, ends
//! the consumer, or discards within an explicit bound and reports loss in-band.
//! `DropNewest` is exact until its counter saturates. `u64::MAX` then means at
//! least that many skipped items. Nothing is silently queued or silently dropped.
//!
//! [`Group`] is neither of those two, and it is written down here rather than
//! filed under the nearer one. Nothing *produces* an ending — a member already
//! dying cannot be told to slow down — so the group makes **itself** wait instead
//! of pushing back on anyone or dropping anyone. What keeps that from being the
//! unbounded queue under a third name is where its bound comes from: one future
//! per member, built by [`add`](Group::add) and not by the death, so the pending
//! set grows with a membership the caller wrote and never with arrivals it did
//! not. A third shape that grew with arrivals is the buffer this section exists
//! to refuse.
//!
//! Read that as a safety property and not only as a throughput one. An option
//! that waits is an edge a deadlock can be built from; an option that ends the
//! consumer is not, because nothing in it awaits. Which of the two applies, and
//! when the difference bites, is stated on
//! [`publish`](Topic::publish) rather than repeated here. A group waits, so it
//! carries that edge: [`Group`] argues that it cannot close into a cycle —
//! nothing can call *into* a group at all — and states the residual it does not
//! close, which is caller code that blocks inside the policy.
//!
//! # Topic depth is explicit and independent of mailbox depth
//!
//! A mailbox and a topic bound different queues. Mailbox defaults do not limit a
//! topic, and a topic does not read them. Each [`Overflow`] variant carries the
//! finite, non-zero depth for one subscriber channel.
//!
//! A larger topic depth absorbs a finite burst. It does not make the queue
//! unbounded. When the channel reaches that exact depth, the selected policy
//! waits for capacity or removes the subscriber. Raising an actor's mailbox depth
//! does not change this decision because generated feeds bypass the mailbox.
//!
//! # Handle ergonomics: already here, under keylink's names
//!
//! The brief for this module asked for `is_alive`, `stop_gracefully` and
//! `wait_for_shutdown`. All three exist, and **no alias was added**, because a
//! second name for one method is the "one way to build a thing" invariant broken
//! for the sake of a spelling:
//!
//! | asked for | what it is called here | since |
//! |---|---|---|
//! | `is_alive` | [`Handle::is_running`] — advisory, and documented as such | Wave 1 |
//! | `stop_gracefully` | [`Handle::stop`] — ordered, drains what was already queued, then waits | Wave 1, and it waits on the `ended` channel rather than `mail.closed()` since Wave 2 |
//! | `wait_for_shutdown` | [`Handle::ended`] — and it answers *why*, which a `wait_for_shutdown` returning `()` cannot | Wave 2 |
//!
//! What was genuinely absent is on the **composition** path, and this module is
//! where it is filled: a keyed actor reached through [`Registry`](crate::Registry) now
//! has [`Registry::ended`](crate::Registry::ended) for direct observation, while
//! [`Endings`] adapts keyed [`Lifecycle`](crate::Lifecycle) callbacks into
//! futures a composition can own. [`Names`] and [`Group`] both take the ending future as an argument rather
//! than asking a handle for one, which lets local and keyed members share their
//! existing shape.
//!
//! [`Handle`]: crate::LocalActor
//! [`Handle::is_running`]: crate::LocalActor::is_running
//! [`Handle::stop`]: crate::LocalActor::stop
//! [`Handle::ended`]: crate::LocalActor::ended
//! [`n0_future`]: n0_future

mod factory;
mod group;
mod keyed;
mod keyed_topic;
mod lossy;
mod names;
mod pool;
mod schedule;
mod topic;

pub use factory::{Factory, Reserving, Routing, Sample, Shard};
pub use group::{Added, Group, GroupEnded, Membership, Retire, Verdict};
pub use keyed::Endings;
pub use keyed_topic::{KeyedSubscription, KeyedTopic};
pub use lossy::{
    BroadcastDepth, BroadcastDepthError, Lagged, Lossy, LossyFanout, LossySubscription, LossyTopic,
};
pub use names::Names;
pub use pool::{Member, Pool};
pub use schedule::{after, every, Scheduled};
pub use topic::{Fanout, Overflow, Patience, PatienceError, Subscription, Topic};
