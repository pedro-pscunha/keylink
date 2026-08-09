//! The six patterns every actor system grows, each with its backpressure
//! written down.
//!
//! Nothing here is on the wire. No iroh type appears in this module, no ALPN
//! moves, and `service!` generates not one line differently because of it —
//! these are compositions of things the crate already has: a [`Handle`], a
//! `feed`, an [`ActorId`](crate::ActorId), and [`n0_future`]'s clock.
//!
//! | pattern | what it is |
//! |---|---|
//! | [`Topic`] | fan-out. One publisher, N subscribers, each receiving on a `feed` |
//! | [`Pool`] | N interchangeable actors behind one cursor, each saying which it is |
//! | [`after`] / [`every`] | a delayed run and a repeating one, on a cancellable handle |
//! | [`Names`] | name → local actor, unregistered when the actor ends |
//! | [`Group`] | a set of actors that end together, or one the rest carry on without |
//! | [`Endings`] | ending → future, for a keyed actor no handle can be asked |
//!
//! Six types and two functions for the six rows. Five of the rows are the first
//! five patterns; [`Endings`] is a sixth **row** and not a sixth pattern — it is
//! the keyed half of [`Names`] and [`Group`], both of which want an ending future
//! and could not get one out of a keyed actor. The sixth **pattern** has no row,
//! and that is the finding rather than an oversight: handle ergonomics turned out
//! to be a table rather than a type — all of it already exists under keylink's own
//! names, and the last section here is the mapping and the argument for not adding
//! an alias.
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
//! | [`Topic`] with [`Overflow::BlockPublisher`] | the publisher waits | one item in flight per subscriber. [`publish`](Topic::publish) does not return until the slowest has taken it |
//! | [`Topic`] with [`Overflow::DropSubscriber`] | the subscriber is unsubscribed, loudly | one item in flight per subscriber. A second arriving while the first is unread ends that subscriber's stream, so its actor ends as [`Ended::Feed`](crate::Ended::Feed) |
//! | [`Pool`] | the caller waits, on that member's own mailbox | the pool holds **no** queue at all. [`pick`](Pool::pick) hands back a handle; the wait is the ordinary mailbox permit |
//! | [`every`] | the next run is late | a run in progress is never re-entered and a missed tick is never queued. The period is a floor on the *gap*, not a rate |
//! | [`Names`] | nothing waits | it is a map and a lock. No channel, no queue; bounded by the number of live names |
//! | [`Group`] | a member slow to die delays the group's next **decision** | one decision at a time, in one task. Endings are futures rather than queue entries, so N members are at most N pending endings and a death allocates nothing |
//! | [`add`](Group::add) while the policy is running | its ending waits in a staging list until the next wake — the member itself is filed at once, and a teardown before that wake retires it | the number of `add` calls in flight, drained to empty on that wake. A staging list, not a queue that grows against a slow consumer |
//! | [`Endings`] | nothing waits | a map, a lock, and one `oneshot` per waiter. No channel is ever full, so [`ended`](crate::Lifecycle::ended) cannot block a mount; bounded by the number of **live** [`watch`](Endings::watch) futures, and dropping one takes its waiter back out |
//!
//! **There is no third option that hides.** A pattern here either applies
//! backpressure to whoever is producing, or ends the consumer and says so.
//! Nothing is silently buffered, and nothing is silently dropped: `Topic`'s drop
//! policy ends an actor, which is the loudest signal the crate has.
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
//! # Designed against a mailbox of one, and where a deeper one would change this
//!
//! `turns.rs`'s `DEFAULT_LOCAL_MAILBOX_DEPTH` is 1 — as is `registry.rs`'s
//! `DEFAULT_MAILBOX_DEPTH`, the keyed side's — and every number in this module is
//! chosen against that. Both are DEFAULTS: a caller can raise either, and this
//! module's numbers do not move with them, which is the whole of the paragraph
//! below.
//! One item in flight per subscriber matches it: a subscriber is "slow" exactly
//! when it is still inside a turn while the next item arrives, which is the same
//! definition of slow the mailbox already uses.
//!
//! A deeper mailbox would change **one** thing and it is worth naming, because
//! it is the change that would quietly re-introduce the unbounded queue. With a
//! mailbox of *k*, the obvious next step is a subscriber buffer of *k* — "for
//! symmetry" — and the moment that depth is a caller-supplied number, a fan-out
//! is an unbounded queue with an extra argument. If a per-subscriber depth is
//! ever added it must be bounded by the same constant the mailbox is, and a
//! subscriber at its bound must still hit one of the two policies above rather
//! than a third that grows.
//!
//! Nothing in this module reads either constant or depends on its value. Both
//! are private, which is why they are named here as text rather than linked: a
//! reader who greps for them finds the declaration, and an intra-doc link would
//! not resolve.
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
//! What was genuinely absent is on the **keyed** path, and this module is where
//! it is fixed: a keyed actor reached through [`System`](crate::System) has no
//! per-actor `ended()` for a caller to await, so [`Endings`] writes that channel
//! once. [`Names`] and [`Group`] both take the ending future as an argument
//! rather than asking a handle for one, which is what lets either hold a keyed
//! member now that the channel exists — the argument they already had is the
//! change they did not need.
//!
//! [`Handle`]: crate::LocalActor
//! [`Handle::is_running`]: crate::LocalActor::is_running
//! [`Handle::stop`]: crate::LocalActor::stop
//! [`Handle::ended`]: crate::LocalActor::ended
//! [`n0_future`]: n0_future

mod group;
mod keyed;
mod names;
mod pool;
mod schedule;
mod topic;

pub use group::{Group, GroupEnded, Membership, Retire, Verdict};
pub use keyed::Endings;
pub use names::Names;
pub use pool::{Member, Pool};
pub use schedule::{after, every, Scheduled};
pub use topic::{Fanout, Overflow, Subscription, Topic};
