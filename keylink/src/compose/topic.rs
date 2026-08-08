//! Fan-out: one publisher, N subscribers, each receiving on a `feed`.
//!
//! An actor can be told and can answer. It cannot **push** — nothing in the
//! crate turns one event into a turn against every interested actor, and every
//! user who wants one writes the same `Vec<Sender<_>>` by hand. This is that
//! `Vec`, with the one thing a hand-rolled version leaves out: a stated bound.
//!
//! ```ignore
//! keylink::service! {
//!     service Book;
//!     rpc  Cancel(id: u64) -> ();
//!     feed Fills(fill: Fill);
//! }
//!
//! let topic = Topic::new(Overflow::BlockPublisher);
//! let book = book::actor_wired(Book::new(), vec![], book::Feeds::new(topic.subscribe()));
//! let risk = risk::actor_wired(Risk::new(), vec![], risk::Feeds::new(topic.subscribe()));
//!
//! topic.publish(fill).await;      // both actors take a turn on it
//! ```
//!
//! A subscriber is a [`Subscription`], which is a [`Stream`](crate::Stream) and
//! therefore exactly what [`Feeds::new`] wants. That is the whole reason this
//! pattern waited for feeds: before them there was no way to *deliver* to an
//! actor without a caller, and a `tell` from a fan-out task would have made every
//! push a message somebody in this process chose to send fire-and-forget, which
//! is not what happened.
//!
//! # THE DECISION THIS MODULE OWNS — a slow subscriber is never buffered
//!
//! **Each subscriber holds at most one unread item.** Not "one by default", not
//! "one, configurable": one, and there is no argument that changes it.
//!
//! That leaves exactly two things that can happen when an item arrives for a
//! subscriber that has not read the last one, and [`Overflow`] is which:
//!
//! - [`Overflow::BlockPublisher`] — [`publish`](Topic::publish) does not return
//!   until every subscriber has taken the item. The topic's rate is the slowest
//!   subscriber's rate. One wedged subscriber wedges the publisher, and that is
//!   not a defect of the mode, it *is* the mode.
//! - [`Overflow::DropSubscriber`] — the slow subscriber is unsubscribed, its
//!   stream ends, and its actor ends as
//!   [`Ended::Feed`](crate::Ended::Feed)`{ name }`. The publisher is never
//!   delayed by it. Nothing is lost quietly: an actor ending is the loudest
//!   thing this crate can say.
//!
//! There is deliberately **no third policy**, and the missing one is the one
//! every fan-out reaches for: keep the item for later. A per-subscriber queue is
//! an unbounded queue the moment a subscriber is durably slower than the
//! publisher, and an unbounded queue is what `local.rs` refuses by name — it
//! turns a latency problem into memory growth and a diagnosis into an OOM.
//!
//! There is no [`Default`] for either [`Topic`] or [`Overflow`], for the reason
//! [`Allowlist`](crate::Allowlist) has none: the policy is a decision about the
//! caller's data, so it is typed out at the call site.
//! `representability.rs::HasNoDefault` is where that is checked rather than
//! promised.
//!
//! # Three things this does not promise
//!
//! - **`publish` is not atomic.** Under [`Overflow::BlockPublisher`] it delivers
//!   in subscription order, awaiting each; dropping its future leaves the item
//!   delivered to the subscribers already reached and not to the rest. There is
//!   no rollback, because a delivered item cannot be recalled. A caller that
//!   needs all-or-nothing needs a transaction, and a fan-out is not one.
//! - **No ordering across topics, and no ordering against the mailbox.** A
//!   subscriber's feed items and its mailbox messages are ordered at the actor's
//!   own `select`, which alternates; see `RFC-07 §5.2`.
//! - **No delivery guarantee at all**, in the sense Wave 8's recipe means it.
//!   A feed is a local stream and nothing acks it. [`Fanout`] says what happened
//!   to the *send*, which is not the same as the turn having run.
//!
//! # Cost
//!
//! One `tokio::sync::mpsc` channel of capacity one per subscriber, and one clone
//! of the item per subscriber per publish. A large item wants an
//! [`Arc`] around it; the clone is the caller's to make cheap,
//! because a topic that boxed for you would allocate for the callers whose items
//! are already two words.
//!
//! [`Feeds::new`]: crate::Feeds

use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use tokio::sync::mpsc;

/// How many unread items a subscriber may hold.
///
/// One, and it is a `const` rather than a parameter so that the number appears
/// once in the crate and a reader can find every line that depends on it. See
/// this module's "no third policy" argument for why it is not a knob.
const IN_FLIGHT: usize = 1;

/// What happens when an item arrives for a subscriber that has not read the last
/// one.
///
/// Named at the call site, never defaulted. The two variants are the only two
/// answers that do not grow a queue, which is the whole of
/// [`Topic`]'s design; see the module documentation.
///
/// Not `#[non_exhaustive]`: a caller constructs one and a caller matches one, so
/// sealing it would cost every call site a `_ =>` arm for a third policy that
/// this module argues must not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overflow {
    /// Wait for the subscriber.
    ///
    /// [`Topic::publish`] returns when every subscriber has taken the item, so
    /// the publisher runs at the slowest subscriber's rate. Backpressure reaches
    /// whoever is producing the events, which is where it can actually be
    /// answered.
    ///
    /// The price is coupling: one subscriber that never reads again stops the
    /// topic for everybody. Choose it when losing a subscriber is worse than
    /// slowing the publisher — an order book, a ledger, anything where the
    /// subscriber's state must not skip an event.
    BlockPublisher,
    /// Unsubscribe the subscriber, and let its actor end saying so.
    ///
    /// The subscriber is removed from the topic, which drops the last sender on
    /// its channel; its [`Subscription`] then yields the item it had not read
    /// and ends. A feed that ends ends the actor, as
    /// [`Ended::Feed`](crate::Ended::Feed)`{ name }`, so a dropped subscriber is
    /// an actor that stopped and named the feed that stopped it.
    ///
    /// Choose it when the publisher must not be held up — a price feed, a metric
    /// stream — and when a subscriber that cannot keep up is better off
    /// restarted than fed stale events.
    DropSubscriber,
}

/// What one [`publish`](Topic::publish) did.
///
/// Three counts and not one, because "nobody got it" has three different causes
/// and an operator needs to tell them apart: nobody is subscribed, everybody is
/// too slow, or everybody has gone away.
///
/// Advisory, and deliberately not `#[must_use]`: the loud signal for a dropped
/// subscriber is its actor's [`Ended`](crate::Ended), not this. A publisher that
/// wants to log or meter reads these; one that does not is not hiding anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fanout {
    /// Subscribers the item reached.
    pub delivered: usize,
    /// Subscribers unsubscribed for being too slow.
    ///
    /// Always zero under [`Overflow::BlockPublisher`], which has no such state:
    /// there the publisher waits instead.
    pub dropped: usize,
    /// Subscribers whose [`Subscription`] had already been dropped, and which
    /// this publish removed.
    pub gone: usize,
}

/// One publisher, N subscribers, each receiving on a `feed`.
///
/// Cheap to clone: every clone names the same topic, so the task that publishes
/// and the code that subscribes can each hold one. The [`Overflow`] travels with
/// them — it is the topic's policy and not a per-call argument, because a
/// `publish` whose blocking behaviour depended on who happened to be listening
/// could not be reasoned about at the call site.
///
/// A topic with no subscribers is not an error and does not buffer: `publish`
/// answers `Fanout { delivered: 0, .. }` and the item is dropped. A topic is not
/// a queue and never was; a subscriber that must not miss events subscribes
/// before the first publish.
///
/// # Dropping the last `Topic` clone ends every subscriber
///
/// The topic holds the only sender on each subscriber's channel, so when the last
/// clone goes away every [`Subscription`] yields whatever it had not read and then
/// ends — and a feed that ends ends its actor, as
/// [`Ended::Feed`](crate::Ended::Feed).
///
/// That is the right behaviour and it is worth stating, because it is a second
/// lifecycle rule a reader has to hold: a publisher going away is not silence, it
/// is the end of the subscription. An actor that must survive its publisher wants
/// a topic clone of its own, or a feed that is not a topic. It is the same shape
/// as [`Names`](super::Names)' rule, from the other side — there a registry keeps
/// an actor alive, here a publisher's departure ends one.
///
/// **With one window, and it is the blocking publish.**
/// [`Overflow::BlockPublisher`] snapshots the senders and releases the lock before
/// awaiting any of them — deliberately, because a fan-out holding a mutex across
/// an await is the deadlock this crate is arranged to avoid — so for the duration
/// of that fan-out a clone of every sender lives on the publisher's stack. A last
/// `Topic` clone dropped during that window ends its subscribers when the
/// **publish** finishes, not when the topic does. The delay is bounded by the
/// slowest subscriber, which is the same bound the publisher itself is under.
#[derive(Debug)]
pub struct Topic<T> {
    /// The sending half of every live subscriber's channel, and — outside the
    /// window named on the type above — the **only** place one is held. Removing
    /// an entry therefore closes that subscriber's stream, which is what makes
    /// [`Overflow::DropSubscriber`] loud rather than silent.
    to: Arc<Mutex<Vec<mpsc::Sender<T>>>>,
    overflow: Overflow,
}

impl<T> Topic<T> {
    /// An empty topic with the policy named.
    #[must_use]
    pub fn new(overflow: Overflow) -> Self {
        Self {
            to: Arc::new(Mutex::new(Vec::new())),
            overflow,
        }
    }

    /// A stream of everything published from now on.
    ///
    /// Hand it to a generated `Feeds::new` — it is a
    /// [`Stream`](crate::Stream), which is all a `feed` wants — or drive it
    /// yourself with [`StreamExt::next`](n0_future::StreamExt::next).
    ///
    /// Items published *before* this call are not delivered: a topic holds no
    /// history. Dropping the subscription unsubscribes, and the topic notices on
    /// its next publish rather than immediately, which is why [`Fanout::gone`]
    /// exists.
    #[must_use]
    pub fn subscribe(&self) -> Subscription<T> {
        let (to, from) = mpsc::channel(IN_FLIGHT);
        self.lock().push(to);
        Subscription { from }
    }

    /// How many subscribers are here.
    ///
    /// Counts entries, which includes subscriptions dropped since the last
    /// publish — nothing sweeps them in the background, so this is an upper
    /// bound rather than a live count. Advisory, like
    /// [`is_running`](crate::LocalActor::is_running).
    #[must_use]
    pub fn subscribers(&self) -> usize {
        self.lock().len()
    }

    /// Poison recovery rather than propagation, the call
    /// [`Subscribers`](crate::Subscribers) and the idempotency ledger both make:
    /// one bad publish should cost one publish.
    ///
    /// **And this arm is reachable, by caller code.** An earlier version of this
    /// doc argued it could not be — "`try_send` cannot panic, and the blocking
    /// path releases the lock before it awaits" — and that argument is wrong on
    /// its own file: [`publish_dropping`](Self::publish_dropping) calls
    /// `item.clone()` **inside** `self.lock().retain(..)`, and `T: Clone` is the
    /// caller's implementation. A `Clone` that panics unwinds out of the closure
    /// with the guard held and poisons the mutex.
    ///
    /// So the recovery is load-bearing rather than defensive: without it, one
    /// caller's panicking `Clone` would turn every later `publish`, `subscribe`
    /// and `subscribers` on that topic into a panic of its own, which is the
    /// opposite of "one bad publish costs one publish". Cloning outside the lock
    /// is not the alternative it sounds like — the number of clones is the number
    /// of live subscribers, which is what the lock is protecting. Pinned by
    /// `tests/compose.rs::a_panicking_clone_costs_one_publish_and_not_the_topic`.
    fn lock(&self) -> MutexGuard<'_, Vec<mpsc::Sender<T>>> {
        self.to.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T: Clone> Topic<T> {
    /// Send `item` to every subscriber, under this topic's [`Overflow`].
    ///
    /// Whether this awaits at all is the policy's business:
    /// [`Overflow::BlockPublisher`] awaits every subscriber's permit, and
    /// [`Overflow::DropSubscriber`] never awaits — it completes without yielding,
    /// which is worth knowing for a caller that publishes from inside a turn.
    ///
    /// # An actor must not publish to a topic it subscribes to, under
    /// [`Overflow::BlockPublisher`]
    ///
    /// It is [`Handle`](crate::LocalActor)'s self-call deadlock in a second
    /// costume: the actor is inside a turn, so it is not polling its own feed, so
    /// the permit it is waiting for can only be freed by the loop it is blocking.
    /// No type can detect this — a topic cannot tell one subscriber's channel
    /// from another's — and it is the one composition in this module that
    /// deadlocks rather than degrades.
    pub async fn publish(&self, item: T) -> Fanout {
        match self.overflow {
            Overflow::DropSubscriber => self.publish_dropping(item),
            Overflow::BlockPublisher => self.publish_blocking(item).await,
        }
    }

    /// `try_send` under the lock, and the lock is safe here precisely because
    /// nothing in this function awaits.
    ///
    /// Removal is the mechanism and not a bookkeeping detail: the entry taken out
    /// of the `Vec` is the last sender on that channel, so dropping it ends the
    /// subscriber's stream. That is why this uses `retain` rather than marking an
    /// entry dead — a dead entry nobody dropped would be a subscriber that
    /// silently stopped receiving, which is the failure the whole module is
    /// arranged against.
    fn publish_dropping(&self, item: T) -> Fanout {
        let mut delivered = 0;
        let mut dropped = 0;
        let mut gone = 0;
        self.lock().retain(|to| match to.try_send(item.clone()) {
            Ok(()) => {
                delivered += 1;
                true
            }
            // The subscriber has an unread item. One is the bound, so this one
            // is its last: it keeps what it had and its stream ends.
            Err(mpsc::error::TrySendError::Full(_)) => {
                dropped += 1;
                false
            }
            // The `Subscription` was dropped. Not a policy decision — there is
            // nobody to deliver to — and pruning it here is what keeps the `Vec`
            // bounded by the number of live subscribers.
            Err(mpsc::error::TrySendError::Closed(_)) => {
                gone += 1;
                false
            }
        });
        Fanout {
            delivered,
            dropped,
            gone,
        }
    }

    /// Snapshot the senders, release the lock, then await each one.
    ///
    /// An eager snapshot for [`Subscribers::snapshot`](crate::Subscribers::snapshot)'s
    /// reason, which is the same reason spelled one layer up: a fan-out holding a
    /// mutex across an await is the deadlock this crate is arranged to avoid. A
    /// subscriber that joins during a publish is not delivered to, which is
    /// indistinguishable from having subscribed a moment later.
    ///
    /// Sequential, in subscription order, and not a `join_all`: the publisher's
    /// rate is the slowest subscriber's either way, and one order is easier to
    /// reason about than a set of concurrent ones. It is also what makes the
    /// non-atomicity in the module docs a *prefix* — the subscribers a cancelled
    /// publish reached are the first ones — rather than an arbitrary subset.
    async fn publish_blocking(&self, item: T) -> Fanout {
        let to = self.lock().to_vec();
        let mut delivered = 0;
        let mut gone = 0;
        for one in to {
            // The permit is the bound. `send` resolves when the subscriber has
            // read the previous item, so a publisher cannot outrun a subscriber
            // by even one item.
            match one.send(item.clone()).await {
                Ok(()) => delivered += 1,
                Err(_) => gone += 1,
            }
        }
        if gone > 0 {
            self.lock().retain(|one| !one.is_closed());
        }
        Fanout {
            delivered,
            dropped: 0,
            gone,
        }
    }
}

/// By hand, because `#[derive(Clone)]` would demand `T: Clone` — and a topic is
/// shared whether or not its item type can be copied. `publish` is where the
/// bound belongs.
impl<T> Clone for Topic<T> {
    fn clone(&self) -> Self {
        Self {
            to: Arc::clone(&self.to),
            overflow: self.overflow,
        }
    }
}

/// One subscriber's end of a [`Topic`].
///
/// A [`Stream`](crate::Stream) and nothing else, which is what a `feed` wants.
/// Dropping it unsubscribes.
///
/// **Not `Clone`, and it could not be**: two holders of one subscription would
/// each get *some* of the items, and which ones would depend on who polled
/// first. Two subscribers are two [`subscribe`](Topic::subscribe) calls.
#[derive(Debug)]
pub struct Subscription<T> {
    from: mpsc::Receiver<T>,
}

/// `Unpin` is **inherited, not declared**, and that is the stronger of the two.
///
/// `Receiver<T>` keeps `T` inside the channel's own allocation, so it is `Unpin`
/// for every `T` and so is this — which is what lets `poll_next` reach the
/// receiver through [`Pin::get_mut`] with no projection and no `unsafe`. A
/// hand-written `impl<T> Unpin for Subscription<T> {}` would *claim* the same
/// property instead of proving it, and would keep compiling on the day a field
/// that is genuinely `!Unpin` is added. Left to the auto trait, that day is a
/// compile error at `get_mut` — which is where it should be.
impl<T> crate::Stream for Subscription<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        // `None` here is what ends the actor. Under
        // `Overflow::DropSubscriber` that is the loud ending the policy
        // promises; under `Overflow::BlockPublisher` it means every `Topic`
        // clone was dropped, which is a publisher that went away.
        self.get_mut().from.poll_recv(cx)
    }
}
