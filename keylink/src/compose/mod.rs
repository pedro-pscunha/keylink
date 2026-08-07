//! The five patterns every actor system grows, each with its backpressure
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
//!
//! Four types and two functions for the first four. The **fifth** pattern is
//! handle ergonomics, and it turned out to be a table rather than a type: all of
//! it already exists under keylink's own names, and the last section here is the
//! mapping and the argument for not adding an alias.
//!
//! # Every one of these states its backpressure, because that is what was
//! missing from the versions this is modelled on
//!
//! `kameo/actors/` implements all four in about 2,400 lines, and its versions
//! predate the argument this crate is built around: they assume an **unbounded**
//! mailbox, so a slow consumer is answered by buffering and the buffer has no
//! bound. That converts a latency problem into memory growth, which is the one
//! kameo feature keylink's actor model *actively rejects* rather than defers.
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
//!
//! **There is no third option.** A pattern here either applies backpressure to
//! whoever is producing, or ends the consumer and says so. Nothing is silently
//! buffered, and nothing is silently dropped: `Topic`'s drop policy ends an
//! actor, which is the loudest signal the crate has.
//!
//! # Designed against a mailbox of one, and where a deeper one would change this
//!
//! `local.rs`'s `LOCAL_MAILBOX_DEPTH` is 1 — as is `registry.rs`'s
//! `DEFAULT_MAILBOX_DEPTH`, the keyed side's — and every number in this module is
//! chosen against that.
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
//! What is genuinely absent is on the **keyed** path, and it is not this
//! module's to fix: a keyed actor reached through [`System`](crate::System) has
//! no per-actor `ended()` for a caller to await. [`Names`] therefore takes the
//! ending future as an argument rather than asking a handle for one, which is
//! what lets it hold a keyed wrapper the day that channel exists.
//!
//! [`Handle`]: crate::LocalActor
//! [`Handle::is_running`]: crate::LocalActor::is_running
//! [`Handle::stop`]: crate::LocalActor::stop
//! [`Handle::ended`]: crate::LocalActor::ended
//! [`n0_future`]: n0_future

mod names;
mod pool;
mod schedule;
mod topic;

pub use names::Names;
pub use pool::{Member, Pool};
pub use schedule::{after, every, Scheduled};
pub use topic::{Fanout, Overflow, Subscription, Topic};
