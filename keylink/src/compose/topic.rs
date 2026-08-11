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
//! let topic = Topic::new(Overflow::BlockPublisher(std::num::NonZeroUsize::MIN));
//! let book = book::setup(Book::new()).feeds(book::Feeds::new(topic.subscribe())).actor();
//! let risk = risk::setup(Risk::new()).feeds(risk::Feeds::new(topic.subscribe())).actor();
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
//! # THE DECISION THIS MODULE OWNS — a finite subscriber depth, three policies, two loud outcomes
//!
//! **Each subscriber holds at most the finite, non-zero depth carried by
//! [`Overflow`].** There is no unbounded spelling. A larger depth absorbs a
//! finite burst; it cannot hide a subscriber that stays slower than the publisher.
//! Once the configured depth is full, [`Overflow`] says when one of two loud
//! outcomes occurs: the publisher waits, or the subscriber ends.
//!
//! - [`Overflow::BlockPublisher`] waits until every bounded channel accepts the
//!   item. A wedged subscriber wedges the publisher; that is the mode.
//! - [`Overflow::BlockThenDrop`] waits up to a fresh [`Patience`] for each
//!   subscriber in subscription order. A subscriber that does not accept in time
//!   is removed, drains its accepted items, and then ends.
//! - [`Overflow::DropSubscriber`] waits no time. A full subscriber is removed,
//!   drains its accepted items, and then ends.
//!
//! Both removal policies are loud: ending the stream ends a generated actor as
//! [`Ended::Feed`](crate::Ended::Feed)`{ name }`. The difference is only whether
//! the publisher gives that subscriber a bounded chance to catch up.
//!
//! There is no [`Default`] for either [`Topic`] or [`Overflow`], for the reason
//! [`Allowlist`](crate::Allowlist) has none: the policy is a decision about the
//! caller's data, so it is typed out at the call site.
//! `representability.rs::HasNoDefault` is where that is checked rather than
//! promised.
//!
//! # Three things this does not promise
//!
//! - **`publish` is not atomic.** Both waiting policies offer in subscription
//!   order. Dropping a future leaves the item accepted by the prefix already
//!   reached and not by the rest; outer cancellation does not remove the current
//!   subscriber. There is no rollback because an accepted item cannot be recalled.
//! - **No ordering across topics, and no ordering against the mailbox.** A
//!   subscriber's feed items and its mailbox messages are ordered at the actor's
//!   own `select`, which alternates; see `RFC-07 §5.2`.
//! - **No delivery guarantee at all**, in the sense Wave 8's recipe means it.
//!   A feed is a local stream and nothing acks it. [`Fanout`] says what happened
//!   to the *send*, which is not the same as the turn having run.
//!
//! # Cost
//!
//! Each subscriber has one `tokio::sync::mpsc` channel with the configured finite
//! capacity. Each publish clones the item once per subscriber. A large item wants
//! an [`Arc`] around it; the clone is the caller's to make cheap,
//! because a topic that boxed for you would allocate for the callers whose items
//! are already two words.
//!
//! [`Feeds::new`]: crate::Feeds

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use n0_future::time::{sleep_until, Duration, Instant};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

/// How long [`Overflow::BlockThenDrop`] waits for one subscriber.
///
/// The duration is private so every value fits both native timers and a browser's
/// millisecond `setTimeout`: a non-zero whole number of milliseconds no larger
/// than `i32::MAX`.
///
/// The field is intentionally unavailable to callers, so construction always
/// passes through [`try_after`](Self::try_after). This downstream probe fails
/// while that field stays private; the in-crate deadline-overflow test also names
/// the constructor, so this cannot pass merely because the type or constructor
/// disappeared:
///
/// ```compile_fail
/// use keylink::compose::Patience;
///
/// let _ = Patience(Default::default());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Patience(Duration);

/// Why a [`Patience`] could not be represented by every target clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("patience must be a non-zero whole number of milliseconds no larger than i32::MAX")]
pub struct PatienceError;

impl Patience {
    /// Wait for a subscriber for this duration before dropping it.
    ///
    /// # Errors
    ///
    /// Returns [`PatienceError`] for zero, fractional milliseconds, values above
    /// `i32::MAX` milliseconds, or a duration the target clock cannot add to now.
    pub fn try_after(duration: Duration) -> Result<Self, PatienceError> {
        let whole_millis = duration.subsec_nanos().is_multiple_of(1_000_000);
        let portable = duration.as_millis() <= i32::MAX as u128;
        if duration.is_zero() || !whole_millis || !portable {
            return Err(PatienceError);
        }
        Instant::now()
            .checked_add(duration)
            .map(|_| Self(duration))
            .ok_or(PatienceError)
    }
}

/// What happens when an item arrives after a subscriber's finite channel is full.
///
/// Named at the call site with its non-zero channel depth, never defaulted.
/// Three policies choose when one of two loud outcomes occurs: the publisher
/// waits, or the subscriber ends. See the module documentation.
///
/// Not `#[non_exhaustive]`: a caller constructs one and a caller matches one, so
/// sealing it would cost every call site a `_ =>` arm and hide a policy added
/// by this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overflow {
    /// Wait for the subscriber.
    ///
    /// [`Topic::publish`] returns when every subscriber's bounded channel has
    /// accepted the item. This does not mean the subscriber consumed it. Once the
    /// configured depth is full, backpressure reaches
    /// whoever is producing the events, which is where it can actually be
    /// answered.
    ///
    /// The price is coupling: one subscriber that never reads again stops the
    /// topic for everybody. Choose it when losing a subscriber is worse than
    /// slowing the publisher — an order book, a ledger, anything where the
    /// subscriber's state must not skip an event.
    ///
    /// Generated feeds are polled beside the actor mailbox and do not enter it.
    /// Therefore, actor [`Backpressure`](crate::Backpressure) does not change this
    /// topic channel. Select [`DropSubscriber`](Self::DropSubscriber) when the
    /// publisher must not wait for a feed subscriber.
    BlockPublisher(NonZeroUsize),
    /// Wait up to one patience for each subscriber, then unsubscribe it.
    ///
    /// Subscribers are visited sequentially in subscription order. Each receives
    /// a fresh patience; if its bounded channel does not accept in time, it keeps
    /// every item already accepted, receives no new item, and then its stream ends.
    BlockThenDrop {
        /// The finite, non-zero channel depth for each subscriber.
        depth: NonZeroUsize,
        /// How long one full subscriber may hold this publish.
        patience: Patience,
    },
    /// Unsubscribe the subscriber, and let its actor end saying so.
    ///
    /// The subscriber is removed from the topic, which drops the last sender on
    /// its channel; its [`Subscription`] then yields the accepted items it had not
    /// read and ends. A feed that ends ends the actor, as
    /// [`Ended::Feed`](crate::Ended::Feed)`{ name }`, so a dropped subscriber is
    /// an actor that stopped and named the feed that stopped it.
    ///
    /// Choose it when the publisher must not be held up — a price feed, a metric
    /// stream — and when a subscriber that cannot keep up is better off
    /// restarted than fed stale events.
    DropSubscriber(NonZeroUsize),
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
    /// Subscriber channels that accepted the item.
    pub delivered: usize,
    /// Subscribers unsubscribed for being too slow.
    ///
    /// Always zero under [`Overflow::BlockPublisher`], which waits instead.
    /// Under the other policies, each count is a subscriber removed for slowness.
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
/// The topic holds the only sender on each subscriber's channel. When the last
/// clone goes away, each [`Subscription`] yields all unread accepted items and
/// then ends. A feed that ends ends its actor, as
/// [`Ended::Feed`](crate::Ended::Feed).
///
/// That is the right behaviour and it is worth stating, because it is a second
/// lifecycle rule a reader has to hold: a publisher going away is not silence, it
/// is the end of the subscription. An actor that must survive its publisher wants
/// a topic clone of its own, or a feed that is not a topic. It is the same shape
/// as [`Names`](super::Names)' rule, from the other side — there a registry keeps
/// an actor alive, here a publisher's departure ends one.
///
/// **With one window in either waiting policy.** The senders are snapshotted and
/// the subscriber-list lock is released before awaiting. During that fan-out a
/// clone of each sender lives on the publisher's stack, so dropping the last
/// `Topic` ends subscribers when that publish finishes or is cancelled. Under
/// [`Overflow::BlockThenDrop`] each current subscriber clone is dropped as soon
/// as it expires, and the whole publish is ordered against other publishes by a
/// cancellation-safe Tokio mutex that stores no items.
#[derive(Debug)]
pub struct Topic<T> {
    /// The sending half of every live subscriber's channel, and — outside the
    /// window named on the type above — the **only** place one is held. Removing
    /// an entry therefore closes that subscriber's stream, which is what makes
    /// [`Overflow::DropSubscriber`] loud rather than silent.
    to: Arc<Mutex<Vec<mpsc::Sender<T>>>>,
    overflow: Overflow,
    /// Serialises only `BlockThenDrop` publishes; it never stores an item.
    publish_order: Arc<AsyncMutex<()>>,
}

impl<T> Topic<T> {
    /// An empty topic with the policy named.
    #[must_use]
    pub fn new(overflow: Overflow) -> Self {
        Self {
            to: Arc::new(Mutex::new(Vec::new())),
            overflow,
            publish_order: Arc::new(AsyncMutex::new(())),
        }
    }

    /// A stream of everything published from now on, buffered to this topic's
    /// finite subscriber depth.
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
        let depth = match self.overflow {
            Overflow::BlockPublisher(depth) => depth,
            Overflow::BlockThenDrop { depth, .. } => depth,
            Overflow::DropSubscriber(depth) => depth,
        };
        let (to, from) = mpsc::channel(depth.get());
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

    /// Remove exactly the canonical sender for one snapshotted subscriber.
    fn remove_subscriber(&self, sender: &mpsc::Sender<T>) {
        self.lock()
            .retain(|canonical| !canonical.same_channel(sender));
    }
}

impl<T: Clone> Topic<T> {
    /// Offer `item` to every subscriber, under this topic's [`Overflow`].
    ///
    /// A successful delivery means the bounded channel accepted the item. It does
    /// not mean the subscriber consumed it or ran a turn for it.
    ///
    /// Whether this awaits is the policy's business: [`Overflow::BlockPublisher`]
    /// waits without a deadline, [`Overflow::BlockThenDrop`] waits at most one
    /// [`Patience`] per subscriber, and [`Overflow::DropSubscriber`] never waits.
    ///
    /// # Self-publication can wedge or end the actor
    ///
    /// Publishing to the actor's own [`Overflow::BlockPublisher`] topic is
    /// [`Handle`](crate::LocalActor)'s self-call deadlock in a second costume: the
    /// actor is inside a turn, so it cannot poll the feed that would free the
    /// permit. [`Overflow::BlockThenDrop`] cannot wedge forever, but a full
    /// self-feed removes its own subscription after [`Patience`]; after accepted
    /// items drain, that feed can end the actor. No type can detect either case —
    /// a topic cannot identify which subscriber belongs to the publisher.
    pub async fn publish(&self, item: T) -> Fanout {
        match self.overflow {
            Overflow::DropSubscriber(_) => self.publish_dropping(item),
            Overflow::BlockPublisher(_) => self.publish_blocking(item).await,
            Overflow::BlockThenDrop { patience, .. } => {
                let _ordered = self.publish_order.lock().await;
                self.publish_block_then_drop(item, patience).await
            }
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
            // The subscriber's configured depth is full, so this item is its
            // last attempt: it keeps what was already accepted and its stream ends.
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
            // The permit is the bound. `send` resolves when the bounded channel
            // accepts this item; once the configured depth is full, that requires
            // the subscriber to read an earlier item.
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

    /// Snapshot the senders, then give each subscriber a fresh deadline.
    async fn publish_block_then_drop(&self, item: T, patience: Patience) -> Fanout {
        let to = self.lock().to_vec();
        let mut delivered = 0;
        let mut dropped = 0;
        let mut gone = 0;

        for one in to {
            let Some(deadline) = Instant::now().checked_add(patience.0) else {
                self.remove_subscriber(&one);
                drop(one);
                dropped += 1;
                continue;
            };

            // Put the send future in its own scope so a timeout drops its item
            // before the canonical sender is removed.
            let accepted = {
                let mut elapsed = std::pin::pin!(sleep_until(deadline));
                let mut send = std::pin::pin!(one.send(item.clone()));
                std::future::poll_fn(|cx| {
                    if elapsed.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(None);
                    }
                    send.as_mut().poll(cx).map(Some)
                })
                .await
            };

            match accepted {
                Some(Ok(())) => delivered += 1,
                Some(Err(_)) => {
                    self.remove_subscriber(&one);
                    gone += 1;
                }
                None => {
                    self.remove_subscriber(&one);
                    dropped += 1;
                }
            }
            // Once removed, this was the snapshot's last sender for the current
            // subscriber. Drop it before visiting the next subscriber.
            drop(one);
        }

        Fanout {
            delivered,
            dropped,
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
            publish_order: Arc::clone(&self.publish_order),
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
        // Either removal policy makes this the loud ending it promises; under
        // `Overflow::BlockPublisher` it means every `Topic` clone was dropped.
        self.get_mut().from.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use n0_future::StreamExt;

    /// A long-lived patience whose deadline no longer fits expires its subscriber
    /// instead of panicking. Constructed here only to put the clock at that edge;
    /// the public constructor correctly rejects this duration.
    ///
    /// Made red by `scripts/break-once.sh`, replacing the checked `let`/`else`
    /// deadline with `.expect("broken unchecked subscriber deadline")`. Search literal:
    /// `let Some(deadline) = Instant::now().checked_add(patience.0) else { self.remove_subscriber(&one); drop(one); dropped += 1; continue; };`.
    /// Watched: this test panicked with that message instead of reporting one drop.
    #[tokio::test(start_paused = true)]
    async fn an_unrepresentable_subscriber_deadline_expires_immediately() {
        let mut topic = Topic::new(Overflow::BlockThenDrop {
            depth: NonZeroUsize::MIN,
            patience: Patience(Duration::from_millis(1)),
        });
        let mut slow = topic.subscribe();
        assert_eq!(topic.publish(1_u64).await.delivered, 1);
        topic.overflow = Overflow::BlockThenDrop {
            depth: NonZeroUsize::MIN,
            patience: Patience(Duration::MAX),
        };
        assert_eq!(
            topic.publish(2).await,
            Fanout {
                delivered: 0,
                dropped: 1,
                gone: 0,
            }
        );
        assert_eq!(slow.next().await, Some(1));
        assert_eq!(slow.next().await, None);
    }
}
