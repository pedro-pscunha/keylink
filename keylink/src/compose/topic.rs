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
//! Each subscriber has one bounded queue with the configured finite capacity.
//! `BlockPublisher` uses Tokio mpsc; the removal policies use a private local queue
//! so wakes can be deferred until compose locks are free. Each publish makes a
//! finite temporary snapshot with at most one sender and one item clone per
//! snapshotted subscriber. Caller `Clone` and locally rejected `Drop` callbacks run
//! with all topic locks free. A large item wants
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
use tokio::sync::mpsc;

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
/// them — it is the topic's policy and not a per-call argument.
///
/// Waiting publishers use Tokio's bounded channel. The two removal policies use
/// a bounded local queue so receiver wakes and payload destruction can be deferred
/// until every compose lock is free.
///
/// A topic keeps no history. Dropping its final clone closes every subscription;
/// each stream drains accepted items before it ends. During a publish, a finite
/// sender snapshot can retain that lifetime only until the publish finishes or is
/// cancelled. A timed-out `BlockThenDrop` subscriber is closed during its own
/// ordered iteration instead of being retained by the rest of that snapshot.
#[derive(Debug)]
pub struct Topic<T> {
    to: Arc<Mutex<Vec<ChannelSender<T>>>>,
    overflow: Overflow,
    /// Ticket state for the total `BlockThenDrop` decision order. No guard lives
    /// across a poll, a payload callback, or a waker operation.
    publish_order: Arc<Mutex<PublishOrder>>,
}

#[derive(Debug, Default)]
struct PublishOrder {
    next: u64,
    serving: u64,
    cancelled: std::collections::BTreeSet<u64>,
    waiters: std::collections::BTreeMap<u64, std::task::Waker>,
}

#[derive(Debug)]
struct OrderTicket {
    order: Arc<Mutex<PublishOrder>>,
    number: u64,
}

impl OrderTicket {
    fn issue(order: &Arc<Mutex<PublishOrder>>) -> Self {
        let number = {
            let mut state = order.lock().unwrap_or_else(PoisonError::into_inner);
            let number = state.next;
            let Some(next) = state.next.checked_add(1) else {
                panic!("publication ticket overflow");
            };
            state.next = next;
            number
        };
        Self {
            order: Arc::clone(order),
            number,
        }
    }

    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            // A custom clone and the replaced waker's destructor both run with
            // the order mutex free.
            let candidate = cx.waker().clone();
            let (ready, replaced, unused) = {
                let mut state = self.order.lock().unwrap_or_else(PoisonError::into_inner);
                if state.serving == self.number {
                    (true, None, Some(candidate))
                } else {
                    (false, state.waiters.insert(self.number, candidate), None)
                }
            };
            drop(replaced);
            drop(unused);
            if ready {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

impl Drop for OrderTicket {
    fn drop(&mut self) {
        let (discarded, wake) = {
            let mut state = self.order.lock().unwrap_or_else(PoisonError::into_inner);
            let discarded = state.waiters.remove(&self.number);
            if self.number == state.serving {
                state.serving += 1;
                loop {
                    let serving = state.serving;
                    if !state.cancelled.remove(&serving) {
                        break;
                    }
                    state.serving += 1;
                }
                let serving = state.serving;
                (discarded, state.waiters.remove(&serving))
            } else if self.number > state.serving {
                state.cancelled.insert(self.number);
                (discarded, None)
            } else {
                (discarded, None)
            }
        };
        drop(discarded);
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

#[derive(Debug, Default)]
struct WakeBatch(Vec<std::task::Waker>);

impl WakeBatch {
    fn push(&mut self, waker: Option<std::task::Waker>) {
        self.0.extend(waker);
    }

    fn extend(&mut self, wakers: impl IntoIterator<Item = std::task::Waker>) {
        self.0.extend(wakers);
    }
}

impl Drop for WakeBatch {
    fn drop(&mut self) {
        for waker in self.0.drain(..) {
            waker.wake();
        }
    }
}

#[derive(Debug)]
struct QueueState<T> {
    items: std::collections::VecDeque<T>,
    closed: bool,
    receiver_waker: Option<std::task::Waker>,
    capacity_waker: Option<std::task::Waker>,
}

#[derive(Debug)]
struct LocalChannel<T> {
    capacity: usize,
    state: Mutex<QueueState<T>>,
}

impl<T> LocalChannel<T> {
    fn new(capacity: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity.get(),
            state: Mutex::new(QueueState {
                items: std::collections::VecDeque::with_capacity(capacity.get()),
                closed: false,
                receiver_waker: None,
                capacity_waker: None,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, QueueState<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn enqueue(&self, item: T) -> Enqueue<T> {
        let mut state = self.lock();
        if state.closed {
            return Enqueue::Closed(item);
        }
        if state.items.len() == self.capacity {
            return Enqueue::Full(item);
        }
        state.items.push_back(item);
        let wake = state.receiver_waker.take();
        drop(state);
        Enqueue::Accepted(wake)
    }

    fn poll_capacity(&self, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        let candidate = cx.waker().clone();
        let (result, replaced, unused) = {
            let mut state = self.lock();
            if state.closed {
                (Poll::Ready(Err(())), None, Some(candidate))
            } else if state.items.len() < self.capacity {
                (Poll::Ready(Ok(())), None, Some(candidate))
            } else {
                (Poll::Pending, state.capacity_waker.replace(candidate), None)
            }
        };
        drop(replaced);
        drop(unused);
        result
    }

    fn close(&self) -> Vec<std::task::Waker> {
        let mut state = self.lock();
        state.closed = true;
        let wakers = [state.receiver_waker.take(), state.capacity_waker.take()]
            .into_iter()
            .flatten()
            .collect();
        drop(state);
        wakers
    }
}

#[derive(Debug)]
struct SenderLifetime<T>(std::sync::Weak<LocalChannel<T>>);

impl<T> Drop for SenderLifetime<T> {
    fn drop(&mut self) {
        let Some(channel) = self.0.upgrade() else {
            return;
        };
        drop(WakeBatch(channel.close()));
    }
}

#[derive(Debug, Clone)]
struct LocalSender<T> {
    channel: Arc<LocalChannel<T>>,
    _lifetime: Arc<SenderLifetime<T>>,
}

impl<T> LocalSender<T> {
    fn new(capacity: NonZeroUsize) -> (Self, LocalReceiver<T>) {
        let channel = LocalChannel::new(capacity);
        let lifetime = Arc::new(SenderLifetime(Arc::downgrade(&channel)));
        (
            Self {
                channel: Arc::clone(&channel),
                _lifetime: lifetime,
            },
            LocalReceiver { channel },
        )
    }

    fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.channel, &other.channel)
    }
}

#[derive(Debug)]
struct LocalReceiver<T> {
    channel: Arc<LocalChannel<T>>,
}

impl<T> LocalReceiver<T> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let candidate = cx.waker().clone();
        let (result, capacity_waker, replaced, unused) = {
            let mut state = self.channel.lock();
            if let Some(item) = state.items.pop_front() {
                (
                    Poll::Ready(Some(item)),
                    state.capacity_waker.take(),
                    None,
                    Some(candidate),
                )
            } else if state.closed {
                (Poll::Ready(None), None, None, Some(candidate))
            } else {
                (
                    Poll::Pending,
                    None,
                    state.receiver_waker.replace(candidate),
                    None,
                )
            }
        };
        drop(replaced);
        drop(unused);
        if let Some(waker) = capacity_waker {
            waker.wake();
        }
        result
    }
}

impl<T> Drop for LocalReceiver<T> {
    fn drop(&mut self) {
        let (capacity_waker, receiver_waker, items) = {
            let mut state = self.channel.lock();
            state.closed = true;
            (
                state.capacity_waker.take(),
                state.receiver_waker.take(),
                std::mem::take(&mut state.items),
            )
        };
        drop(receiver_waker);
        if let Some(waker) = capacity_waker {
            waker.wake();
        }
        drop(items);
    }
}

enum Enqueue<T> {
    Accepted(Option<std::task::Waker>),
    Full(T),
    Closed(T),
}

#[derive(Debug, Clone)]
enum ChannelSender<T> {
    Tokio(mpsc::Sender<T>),
    Local(LocalSender<T>),
}

impl<T> ChannelSender<T> {
    fn same_channel(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Tokio(left), Self::Tokio(right)) => left.same_channel(right),
            (Self::Local(left), Self::Local(right)) => left.same_channel(right),
            _ => false,
        }
    }
}

#[derive(Debug)]
enum SubscriptionReceiver<T> {
    Tokio(mpsc::Receiver<T>),
    Local(LocalReceiver<T>),
}

impl<T> Topic<T> {
    /// An empty topic with the policy named.
    #[must_use]
    pub fn new(overflow: Overflow) -> Self {
        Self {
            to: Arc::new(Mutex::new(Vec::new())),
            overflow,
            publish_order: Arc::new(Mutex::new(PublishOrder::default())),
        }
    }

    /// A stream of everything published from now on, buffered to this topic's
    /// finite subscriber depth.
    ///
    /// Items published before this call are not delivered. Dropping the returned
    /// stream unsubscribes; the topic removes that closed route on its next publish.
    #[must_use]
    pub fn subscribe(&self) -> Subscription<T> {
        let (to, from) = match self.overflow {
            Overflow::BlockPublisher(depth) => {
                let (to, from) = mpsc::channel(depth.get());
                (ChannelSender::Tokio(to), SubscriptionReceiver::Tokio(from))
            }
            Overflow::BlockThenDrop { depth, .. } | Overflow::DropSubscriber(depth) => {
                let (to, from) = LocalSender::new(depth);
                (ChannelSender::Local(to), SubscriptionReceiver::Local(from))
            }
        };
        self.lock().push(to);
        Subscription { from }
    }

    /// How many subscribers are here.
    #[must_use]
    pub fn subscribers(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> MutexGuard<'_, Vec<ChannelSender<T>>> {
        self.to.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn remove_and_close(&self, sender: &ChannelSender<T>) -> (bool, Vec<std::task::Waker>) {
        let (removed, wakers) = {
            let mut canonical = self.lock();
            let Some(index) = canonical
                .iter()
                .position(|current| current.same_channel(sender))
            else {
                return (false, Vec::new());
            };
            let wakers = match &canonical[index] {
                ChannelSender::Local(sender) => sender.channel.close(),
                ChannelSender::Tokio(_) => Vec::new(),
            };
            (canonical.remove(index), wakers)
        };
        drop(removed);
        (true, wakers)
    }

    #[cfg(test)]
    pub(super) fn list_is_unlocked_for_test(&self) -> bool {
        self.to.try_lock().is_ok()
    }
}

/// Cancellation drops these fields in declaration order: the total-order ticket,
/// then receiver wakes, then every caller payload not accepted by the prefix.
struct OrderedAttempts<T> {
    _ticket: OrderTicket,
    wakes: WakeBatch,
    attempts: Vec<(ChannelSender<T>, Option<T>)>,
}

impl<T: Clone> Topic<T> {
    /// Offer `item` to every subscriber, under this topic's [`Overflow`].
    pub async fn publish(&self, item: T) -> Fanout {
        match self.overflow {
            Overflow::DropSubscriber(_) => self.publish_dropping(item),
            Overflow::BlockPublisher(_) => self.publish_blocking(item).await,
            Overflow::BlockThenDrop { patience, .. } => {
                // Caller clone and original destruction happen before a ticket is issued.
                let attempts = self.snapshot_attempts(item);
                let ticket = OrderTicket::issue(&self.publish_order);
                ticket.wait().await;
                let mut ordered = OrderedAttempts {
                    _ticket: ticket,
                    wakes: WakeBatch::default(),
                    attempts,
                };
                let result = self.publish_block_then_drop(&mut ordered, patience).await;
                drop(ordered);
                result
            }
        }
    }

    fn snapshot_attempts(&self, item: T) -> Vec<(ChannelSender<T>, Option<T>)> {
        let snapshot = self.lock().to_vec();
        let items: Vec<_> = snapshot.iter().map(|_| item.clone()).collect();
        drop(item);
        snapshot
            .into_iter()
            .zip(items.into_iter().map(Some))
            .collect()
    }

    fn publish_dropping(&self, item: T) -> Fanout {
        let mut attempts = self.snapshot_attempts(item);
        let mut delivered = 0;
        let mut dropped = 0;
        let mut gone = 0;
        let mut rejected = Vec::new();
        let mut retired = Vec::new();
        let mut wakes = WakeBatch::default();
        {
            let mut canonical = self.lock();
            for (sender, item) in &mut attempts {
                let Some(index) = canonical
                    .iter()
                    .position(|current| current.same_channel(sender))
                else {
                    continue;
                };
                let Some(item) = item.take() else {
                    continue;
                };
                let ChannelSender::Local(local) = sender else {
                    unreachable!("DropSubscriber always uses a local queue");
                };
                match local.channel.enqueue(item) {
                    Enqueue::Accepted(waker) => {
                        delivered += 1;
                        wakes.push(waker);
                    }
                    Enqueue::Full(item) => {
                        dropped += 1;
                        wakes.extend(local.channel.close());
                        retired.push(canonical.remove(index));
                        rejected.push(item);
                    }
                    Enqueue::Closed(item) => {
                        gone += 1;
                        wakes.extend(local.channel.close());
                        retired.push(canonical.remove(index));
                        rejected.push(item);
                    }
                }
            }
        }
        drop(retired);
        drop(wakes);
        drop(rejected);
        drop(attempts);
        Fanout {
            delivered,
            dropped,
            gone,
        }
    }

    async fn publish_blocking(&self, item: T) -> Fanout {
        let attempts = self.snapshot_attempts(item);
        let mut delivered = 0;
        let mut gone = 0;
        let mut closed = Vec::new();
        for (one, item) in attempts {
            let Some(item) = item else {
                continue;
            };
            let ChannelSender::Tokio(one) = one else {
                unreachable!("BlockPublisher always uses Tokio mpsc");
            };
            match one.send(item).await {
                Ok(()) => delivered += 1,
                Err(_) => {
                    gone += 1;
                    closed.push(one);
                }
            }
        }
        if gone > 0 {
            self.lock().retain(|candidate| match candidate {
                ChannelSender::Tokio(one) => !one.is_closed(),
                ChannelSender::Local(_) => true,
            });
        }
        drop(closed);
        Fanout {
            delivered,
            dropped: 0,
            gone,
        }
    }

    async fn publish_block_then_drop(
        &self,
        ordered: &mut OrderedAttempts<T>,
        patience: Patience,
    ) -> Fanout {
        let mut delivered = 0;
        let mut dropped = 0;
        let mut gone = 0;

        for (one, item) in &mut ordered.attempts {
            if !self
                .lock()
                .iter()
                .any(|canonical| canonical.same_channel(one))
            {
                continue;
            }
            let ChannelSender::Local(local) = one else {
                unreachable!("BlockThenDrop always uses a local queue");
            };

            let Some(deadline) = Instant::now().checked_add(patience.0) else {
                let (removed, wakers) = self.remove_and_close(one);
                if removed {
                    dropped += 1;
                    ordered.wakes.extend(wakers);
                }
                continue;
            };

            let available = {
                let mut elapsed = std::pin::pin!(sleep_until(deadline));
                std::future::poll_fn(|cx| {
                    if elapsed.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(None);
                    }
                    local.channel.poll_capacity(cx).map(Some)
                })
                .await
            };

            match available {
                Some(Ok(())) => {
                    let Some(payload) = item.take() else {
                        continue;
                    };
                    match local.channel.enqueue(payload) {
                        Enqueue::Accepted(waker) => {
                            delivered += 1;
                            ordered.wakes.push(waker);
                        }
                        Enqueue::Closed(payload) => {
                            *item = Some(payload);
                            let (removed, wakers) = self.remove_and_close(one);
                            if removed {
                                gone += 1;
                                ordered.wakes.extend(wakers);
                            }
                        }
                        Enqueue::Full(payload) => {
                            // No other publisher can fill this queue while this
                            // ticket owns the decision order. A receiver can only
                            // make more room.
                            *item = Some(payload);
                            unreachable!("capacity disappeared under the ordered publisher");
                        }
                    }
                }
                Some(Err(())) => {
                    let (removed, wakers) = self.remove_and_close(one);
                    if removed {
                        gone += 1;
                        ordered.wakes.extend(wakers);
                    }
                }
                None => {
                    let (removed, wakers) = self.remove_and_close(one);
                    if removed {
                        dropped += 1;
                        ordered.wakes.extend(wakers);
                    }
                }
            }
        }

        Fanout {
            delivered,
            dropped,
            gone,
        }
    }
}

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
/// This is a `Send + 'static` stream when `T` is `Send + 'static`. It is not
/// cloneable: two consumers of one queue would not be two subscriptions.
#[derive(Debug)]
pub struct Subscription<T> {
    from: SubscriptionReceiver<T>,
}

impl<T> crate::Stream for Subscription<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match &mut self.get_mut().from {
            SubscriptionReceiver::Tokio(from) => from.poll_recv(cx),
            SubscriptionReceiver::Local(from) => from.poll_next(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use n0_future::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CallbackItem<T> {
        armed: bool,
        callback: Arc<dyn Fn() + Send + Sync>,
        _marker: std::marker::PhantomData<T>,
    }

    impl<T> CallbackItem<T> {
        fn new(callback: Arc<dyn Fn() + Send + Sync>) -> Self {
            Self {
                armed: false,
                callback,
                _marker: std::marker::PhantomData,
            }
        }
    }

    impl<T> Clone for CallbackItem<T> {
        fn clone(&self) -> Self {
            (self.callback)();
            Self {
                armed: true,
                callback: Arc::clone(&self.callback),
                _marker: std::marker::PhantomData,
            }
        }
    }

    impl<T> Drop for CallbackItem<T> {
        fn drop(&mut self) {
            if self.armed {
                (self.callback)();
            }
        }
    }

    struct DropOnlyItem {
        armed: bool,
        callback: Arc<dyn Fn() + Send + Sync>,
    }

    impl DropOnlyItem {
        fn new(callback: Arc<dyn Fn() + Send + Sync>) -> Self {
            Self {
                armed: false,
                callback,
            }
        }
    }

    impl Clone for DropOnlyItem {
        fn clone(&self) -> Self {
            Self {
                armed: true,
                callback: Arc::clone(&self.callback),
            }
        }
    }

    impl Drop for DropOnlyItem {
        fn drop(&mut self) {
            if self.armed {
                (self.callback)();
            }
        }
    }

    fn reentrant_publish_completes(topic: &Topic<DropOnlyItem>) -> bool {
        let mut publish = Box::pin(topic.publish(DropOnlyItem::new(Arc::new(|| {}))));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        publish.as_mut().poll(&mut cx).is_ready()
    }

    struct DropSubscriptionOnWake<T> {
        subscription: Mutex<Option<Subscription<T>>>,
    }

    impl<T> std::task::Wake for DropSubscriptionOnWake<T> {
        fn wake(self: Arc<Self>) {
            drop(
                self.subscription
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take(),
            );
        }
    }

    fn register_drop_on_wake<T: Send + 'static>(
        subscription: Subscription<T>,
    ) -> Arc<DropSubscriptionOnWake<T>> {
        let guard = Arc::new(DropSubscriptionOnWake {
            subscription: Mutex::new(Some(subscription)),
        });
        let waker = std::task::Waker::from(Arc::clone(&guard));
        let mut cx = Context::from_waker(&waker);
        let mut held = guard
            .subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(Pin::new(
            held.as_mut()
                .unwrap_or_else(|| panic!("subscription is installed"))
        )
        .poll_next(&mut cx)
        .is_pending());
        drop(held);
        guard
    }

    /// A custom receiver waker may synchronously drop the subscription and its
    /// newly accepted payload. That callback must be able to subscribe for real.
    ///
    /// `scripts/break-once.sh` moved the accepted wake batch under a fresh list
    /// guard, using replacement `{ let _held = self.lock(); drop(wakes); }`.
    /// Watched: "accepted payload Drop ran under the subscriber-list lock".
    #[tokio::test]
    async fn drop_subscriber_accepted_wake_drop_runs_lock_free_and_reenters() {
        let topic = Topic::new(Overflow::DropSubscriber(NonZeroUsize::MIN));
        let subscription = topic.subscribe();
        let _wake_guard = register_drop_on_wake(subscription);
        let callback_topic = topic.clone();
        let lock_free = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_lock_free = Arc::clone(&lock_free);
        let reentered = Arc::new(Mutex::new(None));
        let callback_reentered = Arc::clone(&reentered);
        let callback = Arc::new(move || {
            if callback_topic.to.try_lock().is_err() {
                return;
            }
            let subscription = callback_topic.subscribe();
            *callback_reentered
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(subscription);
            callback_lock_free.store(true, Ordering::SeqCst);
        });

        assert_eq!(
            topic.publish(DropOnlyItem::new(callback)).await.delivered,
            1
        );
        assert!(
            lock_free.load(Ordering::SeqCst),
            "accepted payload Drop ran under the subscriber-list lock"
        );
        assert!(reentered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some());
    }

    /// `BlockThenDrop` must also release its total-order guard before invoking a
    /// receiver waker that can synchronously destroy the accepted payload.
    ///
    /// `scripts/break-once.sh` swapped the ticket and wake-batch fields in
    /// `OrderedAttempts`; replacement order was `wakes` then `_ticket`. Watched:
    /// "accepted payload Drop ran under a compose lock".
    #[tokio::test(start_paused = true)]
    async fn block_then_drop_accepted_wake_drop_runs_lock_free_and_reenters() {
        let topic = Topic::new(Overflow::BlockThenDrop {
            depth: NonZeroUsize::MIN,
            patience: Patience::try_after(Duration::from_millis(1))
                .unwrap_or_else(|error| panic!("one millisecond is portable: {error:?}")),
        });
        let subscription = topic.subscribe();
        let _wake_guard = register_drop_on_wake(subscription);
        let callback_topic = topic.clone();
        let lock_free = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_lock_free = Arc::clone(&lock_free);
        let reentered = Arc::new(Mutex::new(None));
        let callback_reentered = Arc::clone(&reentered);
        let callback = Arc::new(move || {
            if callback_topic.to.try_lock().is_err() {
                return;
            }
            let subscription = callback_topic.subscribe();
            if !reentrant_publish_completes(&callback_topic) {
                return;
            }
            *callback_reentered
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(subscription);
            callback_lock_free.store(true, Ordering::SeqCst);
        });

        assert_eq!(
            topic.publish(DropOnlyItem::new(callback)).await.delivered,
            1
        );
        assert!(
            lock_free.load(Ordering::SeqCst),
            "accepted payload Drop ran under a compose lock"
        );
        assert!(reentered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some());
    }

    /// Cancelling after an accepted prefix releases publication order before it
    /// wakes that receiver or destroys the untouched suffix payload.
    ///
    /// `scripts/break-once.sh` used the same reversed field-order replacement as
    /// the accepted-wake guard. Watched: only one of two lock-free callbacks ran.
    #[tokio::test(start_paused = true)]
    async fn block_then_drop_cancellation_wakes_prefix_and_drops_suffix_lock_free() {
        let topic = Topic::new(Overflow::BlockThenDrop {
            depth: NonZeroUsize::MIN,
            patience: Patience::try_after(Duration::from_millis(10))
                .unwrap_or_else(|error| panic!("ten milliseconds are portable: {error:?}")),
        });
        let mut first = topic.subscribe();
        let mut blocked = topic.subscribe();
        assert_eq!(
            topic
                .publish(DropOnlyItem::new(Arc::new(|| {})))
                .await
                .delivered,
            2
        );
        drop(
            first
                .next()
                .await
                .unwrap_or_else(|| panic!("first backlog")),
        );
        let first = register_drop_on_wake(first);

        let callback_topic = topic.clone();
        let lock_free_callbacks = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&lock_free_callbacks);
        let reentered = Arc::new(Mutex::new(Vec::new()));
        let callback_reentered = Arc::clone(&reentered);
        let callback = Arc::new(move || {
            if callback_topic.to.try_lock().is_err() {
                return;
            }
            let subscription = callback_topic.subscribe();
            let order_released = {
                let order = callback_topic
                    .publish_order
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                order.serving == order.next
            };
            if !order_released {
                return;
            }
            callback_reentered
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(subscription);
            callback_count.fetch_add(1, Ordering::SeqCst);
        });

        assert!(
            n0_future::future::poll_once(topic.publish(DropOnlyItem::new(callback)))
                .await
                .is_none(),
            "publication waits at the full second subscriber"
        );
        assert!(
            first
                .subscription
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none(),
            "the accepted prefix receiver was woken on cancellation"
        );
        assert_eq!(
            lock_free_callbacks.load(Ordering::SeqCst),
            2,
            "accepted-prefix and untouched-suffix destruction are lock-free"
        );
        drop(
            blocked
                .next()
                .await
                .unwrap_or_else(|| panic!("only the original backlog reached suffix")),
        );
        assert!(n0_future::future::poll_once(blocked.next()).await.is_none());
    }

    /// `scripts/break-once.sh` reacquired the queue guard around final item
    /// destruction with replacement `{ let _held = self.channel.lock(); drop(items); }`.
    /// Watched: "payload dropped under queue lock".
    #[tokio::test]
    async fn receiver_drop_destroys_queued_payloads_after_queue_unlock_and_reenters() {
        let topic = Topic::new(Overflow::DropSubscriber(NonZeroUsize::MIN));
        let subscription = topic.subscribe();
        let channel = match &subscription.from {
            SubscriptionReceiver::Local(from) => Arc::downgrade(&from.channel),
            SubscriptionReceiver::Tokio(_) => unreachable!(),
        };
        let callback_topic = topic.clone();
        let reentered = Arc::new(Mutex::new(None));
        let callback_reentered = Arc::clone(&reentered);
        let callback = Arc::new(move || {
            let channel = channel
                .upgrade()
                .unwrap_or_else(|| panic!("receiver retains its queue"));
            assert!(
                channel.state.try_lock().is_ok(),
                "payload dropped under queue lock"
            );
            assert!(
                callback_topic.to.try_lock().is_ok(),
                "payload dropped under list lock"
            );
            assert!(
                callback_topic.publish_order.try_lock().is_ok(),
                "payload dropped under order lock"
            );
            *callback_reentered
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(callback_topic.subscribe());
        });
        assert_eq!(
            topic.publish(DropOnlyItem::new(callback)).await.delivered,
            1
        );

        drop(subscription);
        assert!(reentered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some());
    }

    /// The final publisher closes the queue; the receiver still drains accepted
    /// payloads and destroys them after unlocking.
    ///
    /// `scripts/break-once.sh` used the same queue-guard replacement as the direct
    /// receiver-drop guard. Watched: "payload dropped under queue lock".
    #[tokio::test]
    async fn final_publisher_close_leaves_queued_destruction_callback_lock_free() {
        let topic = Topic::new(Overflow::DropSubscriber(NonZeroUsize::MIN));
        let subscription = topic.subscribe();
        let channel = match &subscription.from {
            SubscriptionReceiver::Local(from) => Arc::downgrade(&from.channel),
            SubscriptionReceiver::Tokio(_) => unreachable!(),
        };
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_called = Arc::clone(&called);
        let callback = Arc::new(move || {
            let channel = channel
                .upgrade()
                .unwrap_or_else(|| panic!("receiver retains its queue"));
            assert!(
                channel.state.try_lock().is_ok(),
                "payload dropped under queue lock"
            );
            callback_called.store(true, Ordering::SeqCst);
        });
        assert_eq!(
            topic.publish(DropOnlyItem::new(callback)).await.delivered,
            1
        );

        drop(topic);
        drop(subscription);
        assert!(called.load(Ordering::SeqCst));
    }

    /// Caller `Clone` runs with the subscriber-list mutex free and can subscribe.
    ///
    /// Search `let items: Vec<_> = snapshot.iter().map(|_| item.clone()).collect();`.
    /// Replace it with `let items = { let _held = self.lock(); snapshot.iter().map(|_| item.clone()).collect() };`.
    /// `scripts/break-once.sh` made the `try_lock` assertion fail before reentry.
    #[tokio::test]
    async fn drop_subscriber_clone_runs_lock_free_and_can_reenter_subscribe() {
        let topic = Topic::new(Overflow::DropSubscriber(NonZeroUsize::MIN));
        let mut first = topic.subscribe();
        let callback_topic = topic.clone();
        let reentered = Arc::new(Mutex::new(None));
        let callback_reentered = Arc::clone(&reentered);
        let callback = Arc::new(move || {
            assert!(
                callback_topic.to.try_lock().is_ok(),
                "caller Clone must not run under the subscriber-list mutex"
            );
            let subscription = callback_topic.subscribe();
            *callback_reentered
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(subscription);
        });

        assert_eq!(
            topic
                .publish(CallbackItem::<()>::new(callback))
                .await
                .delivered,
            1
        );
        assert!(first.next().await.is_some());
        assert!(reentered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some());
    }

    /// A payload returned by `try_send(Full)` is destroyed after the list unlocks.
    /// The callback subscribes for real after the `try_lock` guard proves reentry safe.
    ///
    /// Search `drop(rejected);` and replace it with `let _held = self.lock(); drop(rejected);`.
    /// `scripts/break-once.sh` made the callback's `try_lock` assertion fail.
    #[tokio::test]
    async fn drop_subscriber_full_payload_drop_runs_lock_free_and_can_reenter_subscribe() {
        let topic = Topic::new(Overflow::DropSubscriber(NonZeroUsize::MIN));
        let _full = topic.subscribe();
        let quiet: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        assert_eq!(
            topic
                .publish(CallbackItem::<()>::new(quiet))
                .await
                .delivered,
            1
        );

        let callback_topic = topic.clone();
        let reentered = Arc::new(Mutex::new(None));
        let callback_reentered = Arc::clone(&reentered);
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::clone(&calls);
        let callback = Arc::new(move || {
            if callback_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return;
            }
            assert!(
                callback_topic.to.try_lock().is_ok(),
                "Full payload dropped under list lock"
            );
            let subscription = callback_topic.subscribe();
            *callback_reentered
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(subscription);
        });
        assert_eq!(
            topic
                .publish(CallbackItem::<()>::new(callback))
                .await
                .dropped,
            1
        );
        assert_eq!(
            topic.subscribers(),
            1,
            "the reentrant subscriber survives identity cleanup"
        );
        assert!(reentered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some());
    }

    /// A payload returned by `try_send(Closed)` is destroyed after the list unlocks.
    ///
    /// Search `drop(rejected);` and replace it with `let _held = self.lock(); drop(rejected);`.
    /// `scripts/break-once.sh` made the callback's `try_lock` assertion fail.
    #[tokio::test]
    async fn drop_subscriber_closed_payload_drop_runs_lock_free_and_can_reenter_subscribe() {
        let topic = Topic::new(Overflow::DropSubscriber(NonZeroUsize::MIN));
        let closed = topic.subscribe();
        drop(closed);

        let callback_topic = topic.clone();
        let reentered = Arc::new(Mutex::new(None));
        let callback_reentered = Arc::clone(&reentered);
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::clone(&calls);
        let callback = Arc::new(move || {
            if callback_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return;
            }
            assert!(
                callback_topic.to.try_lock().is_ok(),
                "Closed payload dropped under list lock"
            );
            let subscription = callback_topic.subscribe();
            *callback_reentered
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(subscription);
        });
        assert_eq!(
            topic.publish(CallbackItem::<()>::new(callback)).await.gone,
            1
        );
        assert_eq!(
            topic.subscribers(),
            1,
            "the reentrant subscriber survives identity cleanup"
        );
        assert!(reentered
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some());
    }

    /// `BlockPublisher` also runs caller `Clone` and closed-send `Drop` callbacks
    /// outside the subscriber-list and publication-order state.
    #[tokio::test]
    async fn block_publisher_clone_and_closed_drop_run_outside_custom_locks() {
        let topic = Topic::new(Overflow::BlockPublisher(NonZeroUsize::MIN));
        let closed = topic.subscribe();
        drop(closed);
        let callback_topic = topic.clone();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&callbacks);
        let callback = Arc::new(move || {
            assert!(
                callback_topic.to.try_lock().is_ok(),
                "callback held the list lock"
            );
            assert!(
                callback_topic.publish_order.try_lock().is_ok(),
                "callback held the order lock"
            );
            callback_count.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(
            topic.publish(CallbackItem::<()>::new(callback)).await.gone,
            1
        );
        assert_eq!(
            callbacks.load(Ordering::SeqCst),
            2,
            "one Clone and one closed Drop"
        );
    }

    /// Two overlapping snapshots cannot revive a subscriber removed by the first
    /// callback-free send decision.
    ///
    /// `scripts/break-once.sh` removed the canonical removal from the full arm.
    /// The replacement arm retained close and rejection only. Watched: the later
    /// terminal decision reported `gone: 1` instead of no outcome.
    #[test]
    fn drop_subscriber_orders_terminal_removal_across_two_publishers() {
        #[derive(Default)]
        struct State {
            entered: usize,
            release: bool,
            worker_done: usize,
        }

        struct RacingClone {
            coordinated: bool,
            topic: Topic<Self>,
            state: Arc<(Mutex<State>, std::sync::Condvar)>,
        }

        impl Clone for RacingClone {
            fn clone(&self) -> Self {
                if self.coordinated {
                    assert!(
                        self.topic.to.try_lock().is_ok(),
                        "Clone held the list mutex"
                    );
                    let (state, wake) = &*self.state;
                    let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
                    state.entered += 1;
                    wake.notify_all();
                    state = wake
                        .wait_while(state, |state| !state.release)
                        .unwrap_or_else(PoisonError::into_inner);
                    drop(state);
                }
                Self {
                    coordinated: self.coordinated,
                    topic: self.topic.clone(),
                    state: Arc::clone(&self.state),
                }
            }
        }

        let topic = Topic::new(Overflow::DropSubscriber(NonZeroUsize::MIN));
        let _full = topic.subscribe();
        let state = Arc::new((Mutex::new(State::default()), std::sync::Condvar::new()));
        let first_item = RacingClone {
            coordinated: false,
            topic: topic.clone(),
            state: Arc::clone(&state),
        };
        assert_eq!(topic.publish_dropping(first_item).delivered, 1);

        let publish_one = || {
            let item = RacingClone {
                coordinated: true,
                topic: topic.clone(),
                state: Arc::clone(&state),
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                topic.publish_dropping(item)
            }));
            let (state, wake) = &*state;
            state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .worker_done += 1;
            wake.notify_all();
            result
        };

        std::thread::scope(|scope| {
            let first = scope.spawn(publish_one);
            let (state_lock, wake) = &*state;
            let mut shared = state_lock.lock().unwrap_or_else(PoisonError::into_inner);
            shared = wake
                .wait_while(shared, |state| state.entered < 1 && state.worker_done == 0)
                .unwrap_or_else(PoisonError::into_inner);
            if shared.entered != 1 {
                shared.release = true;
                wake.notify_all();
                drop(shared);
                let _ = first.join();
                panic!("the first publisher must reach Clone without a list-lock panic");
            }
            drop(shared);

            let second = scope.spawn(publish_one);
            let mut shared = state_lock.lock().unwrap_or_else(PoisonError::into_inner);
            shared = wake
                .wait_while(shared, |state| state.entered < 2 && state.worker_done == 0)
                .unwrap_or_else(PoisonError::into_inner);
            let both_snapshotted = shared.entered == 2;
            shared.release = true;
            wake.notify_all();
            drop(shared);

            assert!(
                both_snapshotted,
                "both publishers must reach Clone without a list-lock panic"
            );
            let mut reports = [
                first
                    .join()
                    .unwrap_or_else(|_| panic!("first publisher joins"))
                    .unwrap_or_else(|_| panic!("first publisher does not panic")),
                second
                    .join()
                    .unwrap_or_else(|_| panic!("second publisher joins"))
                    .unwrap_or_else(|_| panic!("second publisher does not panic")),
            ];
            reports.sort_by_key(|report| report.dropped);
            assert_eq!(
                reports,
                [
                    Fanout {
                        delivered: 0,
                        dropped: 0,
                        gone: 0
                    },
                    Fanout {
                        delivered: 0,
                        dropped: 1,
                        gone: 0
                    },
                ]
            );
        });
    }

    /// `BlockThenDrop` clones before taking its order ticket and destroys a
    /// timed-out payload only after releasing that ticket and the list mutex.
    ///
    /// `scripts/break-once.sh` swapped the ticket and wake-batch fields in
    /// `OrderedAttempts`; replacement order was `wakes` then `_ticket`. Watched:
    /// the callback observed an unreleased publication ticket.
    #[tokio::test(start_paused = true)]
    async fn block_then_drop_clone_and_timeout_drop_run_outside_custom_locks() {
        let patience = Patience::try_after(Duration::from_millis(1))
            .unwrap_or_else(|error| panic!("one millisecond is portable: {error:?}"));
        let topic = Topic::new(Overflow::BlockThenDrop {
            depth: NonZeroUsize::MIN,
            patience,
        });
        let _full = topic.subscribe();
        let quiet: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
        assert_eq!(
            topic
                .publish(CallbackItem::<()>::new(quiet))
                .await
                .delivered,
            1
        );

        let callback_topic = topic.clone();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let callback_count = Arc::clone(&callbacks);
        let callback = Arc::new(move || {
            assert!(
                callback_topic.to.try_lock().is_ok(),
                "payload callback held the list lock"
            );
            let order = callback_topic
                .publish_order
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            assert_eq!(
                order.serving, order.next,
                "payload callback ran before its publication ticket was released"
            );
            drop(order);
            callback_count.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(
            topic
                .publish(CallbackItem::<()>::new(callback))
                .await
                .dropped,
            1
        );
        assert_eq!(
            callbacks.load(Ordering::SeqCst),
            2,
            "one Clone and one rejected Drop callback"
        );
    }

    /// A long-lived patience whose deadline no longer fits expires its subscriber
    /// instead of panicking. Constructed here only to put the clock at that edge;
    /// the public constructor correctly rejects this duration.
    ///
    /// Made red by `scripts/break-once.sh`, replacing the checked `let`/`else`
    /// deadline with `.expect("broken unchecked subscriber deadline")`. Search literal:
    /// `let Some(deadline) = Instant::now().checked_add(patience.0) else { self.remove_subscriber(&one); drop(one); dropped += 1; continue; };`.
    /// Watched: this test panicked with that message instead of reporting one drop.
    /// That multiline literal was spent when payloads moved into `OrderedAttempts`.
    /// Current deadline anchor: `            let Some(deadline) = Instant::now().checked_add(patience.0) else {`.
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
