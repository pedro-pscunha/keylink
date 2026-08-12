//! Lossy fan-out that keeps subscribers alive and reports every skipped item.

use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use n0_future::{stream, Stream};
use tokio::sync::{broadcast, mpsc};

/// An item from a lossy subscription, or the number of items skipped before it.
///
/// Callers must match both variants. Ignoring [`Skipped`](Self::Skipped) makes
/// stale state indistinguishable from complete state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lagged<T> {
    /// One item accepted by this receiver.
    Item(T),
    /// This many published items were not retained for this receiver.
    /// `DropNewest` is exact until saturation; `u64::MAX` then means at least
    /// that many skipped items.
    Skipped(NonZeroU64),
}

/// A valid capacity for Tokio's broadcast channel.
///
/// Tokio rounds arbitrary capacities to a power of two internally and refuses
/// capacities above `usize::MAX / 2`. This type is stricter: callers must name
/// the actual power-of-two allocation, no larger than the greatest power of two
/// that Tokio accepts without rounding past its limit.
///
/// The field is private so every value passes through [`try_new`](Self::try_new),
/// whose [`NonZeroUsize`] input makes zero unrepresentable.
///
/// ```compile_fail
/// use keylink::compose::BroadcastDepth;
///
/// let _ = BroadcastDepth::try_new(0);
/// ```
///
/// ```compile_fail
/// use keylink::compose::BroadcastDepth;
/// use std::num::NonZeroUsize;
///
/// let _ = BroadcastDepth(NonZeroUsize::MIN);
/// ```
///
/// Watched break beside this guard: the tuple field was changed from private to
/// public, with replacement `pub struct BroadcastDepth(pub NonZeroUsize);`.
/// `scripts/break-once.sh` selected this doctest, and rustdoc failed because the
/// compile-fail example compiled successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BroadcastDepth(NonZeroUsize);

/// A requested broadcast depth was not a power of two or was too large for Tokio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("broadcast depth must be a non-zero power of two no larger than Tokio's safe maximum")]
pub struct BroadcastDepthError;

impl BroadcastDepth {
    const MAX: usize = 1_usize << (usize::BITS - 2);

    /// Validate one broadcast channel depth.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastDepthError`] for a non-power of two or a value above
    /// Tokio's safe broadcast maximum.
    pub const fn try_new(depth: NonZeroUsize) -> Result<Self, BroadcastDepthError> {
        if !depth.get().is_power_of_two() || depth.get() > Self::MAX {
            return Err(BroadcastDepthError);
        }
        Ok(Self(depth))
    }

    /// The validated channel capacity.
    #[must_use]
    pub const fn get(self) -> NonZeroUsize {
        self.0
    }
}

/// Which bounded item a lossy topic keeps when a receiver cannot keep up.
///
/// Exhaustive because callers construct and match the policy. There is no
/// default: choosing whether old or new data matters is a decision about the
/// caller's data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lossy {
    /// Keep accepted backlog and drop each new item while that backlog is full.
    DropNewest(NonZeroUsize),
    /// Keep the newest items in one Tokio broadcast ring.
    ///
    /// Broadcast storage is a power-of-two ring. [`BroadcastDepth`] requires the
    /// power of two explicitly rather than silently allocating Tokio's rounded
    /// capacity.
    DropOldest(BroadcastDepth),
}

/// What one synchronous [`publish`](LossyTopic::publish) decided.
///
/// It reports retention, not reliable [`Fanout`](super::Fanout) removal: a slow
/// lossy receiver stays subscribed, and neither field counts `dropped` or `gone`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LossyFanout {
    /// Live receivers at the publication decision.
    pub subscribers: usize,
    /// Live receivers that accepted the new item.
    ///
    /// This is every live receiver under [`Lossy::DropOldest`]. Under
    /// [`Lossy::DropNewest`], a full receiver is live but does not accept it.
    pub accepted: usize,
}

/// A lossy local topic whose publisher never waits for subscriber capacity.
///
/// Unlike reliable [`Topic`](super::Topic), neither policy removes a live slow
/// subscriber. Loss is reported in-band as [`Lagged::Skipped`], so every
/// consumer must match both [`Lagged`] variants.
///
/// There is no `Default`: the retention policy and finite depth must be explicit.
/// Its storage is private, so [`new`](Self::new) is the only construction path.
///
/// ```compile_fail
/// use keylink::compose::LossyTopic;
///
/// let _ = LossyTopic::<u8> { inner: unreachable!() };
/// ```
///
/// Watched break beside this guard: the `inner` field and `LossyInner` enum were
/// both made public. The replacement used `pub inner: LossyInner<T>` for the
/// field and `pub enum LossyInner<T>` for its storage type.
/// `scripts/break-once.sh` selected this doctest, and rustdoc failed because the
/// compile-fail example compiled successfully.
pub struct LossyTopic<T> {
    inner: LossyInner<T>,
}

enum LossyInner<T> {
    DropNewest(Arc<NewestPublisher<T>>),
    DropOldest(broadcast::Sender<T>),
}

struct NewestPublisher<T> {
    subscribers: Mutex<Vec<Arc<Mutex<NewestState<T>>>>>,
    publish_order: Mutex<()>,
    depth: NonZeroUsize,
}

struct NewestState<T> {
    to: Option<mpsc::Sender<Tagged<T>>>,
    pending: Option<NonZeroU64>,
}

struct Tagged<T> {
    item: T,
    skipped_before: Option<NonZeroU64>,
}

fn recover<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<T> Drop for NewestPublisher<T> {
    fn drop(&mut self) {
        for subscriber in recover(&self.subscribers).iter() {
            recover(subscriber).to.take();
        }
    }
}

impl<T> Clone for LossyTopic<T> {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            LossyInner::DropNewest(inner) => LossyInner::DropNewest(Arc::clone(inner)),
            LossyInner::DropOldest(to) => LossyInner::DropOldest(to.clone()),
        };
        Self { inner }
    }
}

impl<T: Clone + Send + 'static> LossyTopic<T> {
    /// Create an empty topic with an already validated policy.
    #[must_use]
    pub fn new(policy: Lossy) -> Self {
        let inner = match policy {
            Lossy::DropNewest(depth) => LossyInner::DropNewest(Arc::new(NewestPublisher {
                subscribers: Mutex::new(Vec::new()),
                publish_order: Mutex::new(()),
                depth,
            })),
            Lossy::DropOldest(depth) => {
                let (to, from) = broadcast::channel(usize::from(depth.get()));
                drop(from);
                LossyInner::DropOldest(to)
            }
        };
        Self { inner }
    }

    /// Subscribe to items published after this call.
    #[must_use]
    pub fn subscribe(&self) -> LossySubscription<T> {
        match &self.inner {
            LossyInner::DropOldest(to) => LossySubscription::from_broadcast(to.subscribe()),
            LossyInner::DropNewest(publisher) => {
                let (to, from) = mpsc::channel(publisher.depth.get());
                let state = Arc::new(Mutex::new(NewestState {
                    to: Some(to),
                    pending: None,
                }));
                recover(&publisher.subscribers).push(Arc::clone(&state));
                LossySubscription::from_newest(from, state)
            }
        }
    }

    #[cfg(test)]
    fn seed_pending_for_test(&self, pending: u64) {
        let LossyInner::DropNewest(publisher) = &self.inner else {
            panic!("pending loss exists only under DropNewest");
        };
        let subscribers = recover(&publisher.subscribers);
        let subscriber = subscribers
            .first()
            .unwrap_or_else(|| panic!("the test subscribed first"));
        recover(subscriber).pending = NonZeroU64::new(pending);
    }

    /// Offer one item to every live receiver without waiting.
    ///
    /// [`LossyFanout::subscribers`] counts receivers live at this synchronous
    /// decision. [`LossyFanout::accepted`] counts those whose bounded storage
    /// accepted this item. Under [`Lossy::DropOldest`], replacing an older ring
    /// entry still accepts the new item for every live receiver.
    pub fn publish(&self, item: T) -> LossyFanout {
        match &self.inner {
            LossyInner::DropOldest(to) => {
                let accepted = to.send(item).unwrap_or(0);
                LossyFanout {
                    subscribers: accepted,
                    accepted,
                }
            }
            LossyInner::DropNewest(publisher) => {
                let snapshot = recover(&publisher.subscribers).clone();
                let items: Vec<_> = snapshot.iter().map(|_| item.clone()).collect();
                enum Decision<T> {
                    Accepted,
                    Full(Tagged<T>),
                    Closed(Tagged<T>),
                }
                let mut closed = Vec::new();
                let mut rejected = Vec::new();
                let mut subscribers = 0;
                let mut accepted = 0;
                {
                    let _publish_order = recover(&publisher.publish_order);
                    for (subscriber, item) in snapshot.iter().zip(items) {
                        let decision = {
                            let mut state = recover(subscriber);
                            let entry = Tagged {
                                item,
                                skipped_before: state.pending,
                            };
                            match state.to.as_ref() {
                                Some(to) => match to.try_send(entry) {
                                    Ok(()) => {
                                        state.pending = None;
                                        Decision::Accepted
                                    }
                                    Err(mpsc::error::TrySendError::Full(entry)) => {
                                        let next = state
                                            .pending
                                            .map_or(1, |count| count.get().saturating_add(1));
                                        state.pending = NonZeroU64::new(next);
                                        Decision::Full(entry)
                                    }
                                    Err(mpsc::error::TrySendError::Closed(entry)) => {
                                        Decision::Closed(entry)
                                    }
                                },
                                None => Decision::Closed(entry),
                            }
                        };
                        match decision {
                            Decision::Accepted => {
                                subscribers += 1;
                                accepted += 1;
                            }
                            Decision::Full(entry) => {
                                subscribers += 1;
                                rejected.push(entry);
                            }
                            Decision::Closed(entry) => {
                                closed.push(Arc::clone(subscriber));
                                rejected.push(entry);
                            }
                        }
                    }
                }
                if !closed.is_empty() {
                    recover(&publisher.subscribers).retain(|subscriber| {
                        !closed.iter().any(|closed| Arc::ptr_eq(subscriber, closed))
                    });
                }
                drop(rejected);
                LossyFanout {
                    subscribers,
                    accepted,
                }
            }
        }
    }
}

/// One receiver cursor for a [`LossyTopic`].
///
/// This is always a [`Stream<Item = Lagged<T>>`](Stream), `Send`, and `'static`,
/// including in a browser build. Match both [`Lagged`] variants.
pub struct LossySubscription<T> {
    stream: Pin<Box<dyn Stream<Item = Lagged<T>> + Send + 'static>>,
}

fn broadcast_lag(skipped: u64) -> Option<NonZeroU64> {
    let skipped = NonZeroU64::new(skipped);
    debug_assert!(
        skipped.is_some(),
        "Tokio broadcast reported impossible Lagged(0)"
    );
    skipped
}

impl<T: Clone + Send + 'static> LossySubscription<T> {
    fn from_broadcast(from: broadcast::Receiver<T>) -> Self {
        let stream = stream::unfold(from, |mut from| async move {
            loop {
                match from.recv().await {
                    Ok(item) => return Some((Lagged::Item(item), from)),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let Some(skipped) = broadcast_lag(skipped) else {
                            continue;
                        };
                        return Some((Lagged::Skipped(skipped), from));
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        Self {
            stream: Box::pin(stream),
        }
    }

    fn from_newest(from: mpsc::Receiver<Tagged<T>>, state: Arc<Mutex<NewestState<T>>>) -> Self {
        struct Cursor<T> {
            from: mpsc::Receiver<Tagged<T>>,
            state: Arc<Mutex<NewestState<T>>>,
            held: Option<T>,
            ended: bool,
        }

        let cursor = Cursor {
            from,
            state,
            held: None,
            ended: false,
        };
        let stream = stream::unfold(cursor, |mut cursor| async move {
            if let Some(item) = cursor.held.take() {
                return Some((Lagged::Item(item), cursor));
            }
            if cursor.ended {
                return None;
            }
            enum Ready<T> {
                Entry(Tagged<T>),
                Pending(NonZeroU64),
                Empty,
                Disconnected(Option<NonZeroU64>),
            }

            let ready = {
                let mut state = recover(&cursor.state);
                match cursor.from.try_recv() {
                    Ok(entry) => Ready::Entry(entry),
                    Err(mpsc::error::TryRecvError::Empty) => {
                        state.pending.take().map_or(Ready::Empty, Ready::Pending)
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        Ready::Disconnected(state.pending.take())
                    }
                }
            };
            let entry = match ready {
                Ready::Entry(entry) => entry,
                Ready::Pending(skipped) => {
                    return Some((Lagged::Skipped(skipped), cursor));
                }
                Ready::Disconnected(pending) => {
                    cursor.ended = true;
                    return pending.map(|skipped| (Lagged::Skipped(skipped), cursor));
                }
                Ready::Empty => match cursor.from.recv().await {
                    Some(entry) => entry,
                    None => {
                        cursor.ended = true;
                        let pending = recover(&cursor.state).pending.take();
                        return pending.map(|skipped| (Lagged::Skipped(skipped), cursor));
                    }
                },
            };
            match entry.skipped_before {
                Some(skipped) => {
                    cursor.held = Some(entry.item);
                    Some((Lagged::Skipped(skipped), cursor))
                }
                None => Some((Lagged::Item(entry.item), cursor)),
            }
        });
        Self {
            stream: Box::pin(stream),
        }
    }
}

impl<T> Stream for LossySubscription<T> {
    type Item = Lagged<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tokio documents a positive lag count. Zero is an invariant failure, not
    /// a stream-ending marker.
    ///
    /// Search the `debug_assert!` whose message is `Tokio broadcast reported impossible Lagged(0)`.
    /// Anchor: `        "Tokio broadcast reported impossible Lagged(0)"`.
    /// Replace it with `debug_assert!(true);`. `scripts/break-once.sh` made this test stop panicking.
    #[test]
    #[should_panic(expected = "Tokio broadcast reported impossible Lagged(0)")]
    fn impossible_zero_broadcast_lag_fails_loudly() {
        let _ = broadcast_lag(0);
    }

    use n0_future::StreamExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Weak;

    struct CallbackClone {
        callback: Arc<dyn Fn() + Send + Sync>,
    }

    impl Clone for CallbackClone {
        fn clone(&self) -> Self {
            (self.callback)();
            Self {
                callback: Arc::clone(&self.callback),
            }
        }
    }

    struct CallbackDrop {
        armed: Arc<AtomicBool>,
        callback_on_drop: bool,
        callback: Arc<dyn Fn() + Send + Sync>,
    }

    impl Clone for CallbackDrop {
        fn clone(&self) -> Self {
            Self {
                armed: Arc::clone(&self.armed),
                callback_on_drop: self.armed.load(Ordering::SeqCst),
                callback: Arc::clone(&self.callback),
            }
        }
    }

    impl Drop for CallbackDrop {
        fn drop(&mut self) {
            if self.callback_on_drop {
                (self.callback)();
            }
        }
    }

    fn newest_parts<T>(
        topic: &LossyTopic<T>,
    ) -> (Arc<NewestPublisher<T>>, Arc<Mutex<NewestState<T>>>) {
        let LossyInner::DropNewest(publisher) = &topic.inner else {
            panic!("the test uses DropNewest");
        };
        let state = Arc::clone(
            recover(&publisher.subscribers)
                .first()
                .unwrap_or_else(|| panic!("the test subscribed first")),
        );
        (Arc::clone(publisher), state)
    }

    fn lock_free_reentry<T: Clone + Send + 'static>(
        publisher: Weak<NewestPublisher<T>>,
        state: Weak<Mutex<NewestState<T>>>,
        locks_free: Arc<AtomicBool>,
        reentries: Arc<AtomicUsize>,
    ) -> Arc<dyn Fn() + Send + Sync> {
        Arc::new(move || {
            let publisher = publisher
                .upgrade()
                .unwrap_or_else(|| panic!("the topic remains live"));
            let state = state
                .upgrade()
                .unwrap_or_else(|| panic!("the subscription state remains live"));
            let free = publisher.subscribers.try_lock().is_ok()
                && state.try_lock().is_ok()
                && publisher.publish_order.try_lock().is_ok();
            locks_free.store(free, Ordering::SeqCst);
            if free && reentries.fetch_add(1, Ordering::SeqCst) == 0 {
                let topic = LossyTopic {
                    inner: LossyInner::DropNewest(publisher),
                };
                drop(topic.subscribe());
            }
        })
    }

    /// Watched break: `scripts/break-once.sh` held the subscriber-list guard
    /// across the snapshot and caller clones, then released it afterward. The
    /// replacement introduced
    /// `let subscriber_guard = recover(&publisher.subscribers);` around both operations.
    /// This test failed at "Clone runs without the subscriber-list, state, or
    /// ordering mutex".
    #[test]
    fn drop_newest_clone_runs_lock_free_and_can_reenter_subscribe() {
        let topic = LossyTopic::new(Lossy::DropNewest(NonZeroUsize::MIN));
        let closed = topic.subscribe();
        let (publisher, state) = newest_parts(&topic);
        drop(closed);
        let locks_free = Arc::new(AtomicBool::new(false));
        let reentries = Arc::new(AtomicUsize::new(0));
        let callback = lock_free_reentry(
            Arc::downgrade(&publisher),
            Arc::downgrade(&state),
            Arc::clone(&locks_free),
            Arc::clone(&reentries),
        );

        assert_eq!(topic.publish(CallbackClone { callback }).subscribers, 0);
        assert!(
            locks_free.load(Ordering::SeqCst),
            "Clone runs without the subscriber-list, state, or ordering mutex"
        );
        assert_eq!(reentries.load(Ordering::SeqCst), 1);
        assert_eq!(
            recover(&publisher.subscribers).len(),
            1,
            "identity pruning preserves the reentrant subscription"
        );
    }

    /// Watched break: `scripts/break-once.sh` moved the deferred payload
    /// destruction into the publication-order scope. The replacement called
    /// `drop(rejected);` before that scope ended. This test failed at "a Full
    /// rejected payload is destroyed after every mutex unlocks".
    #[test]
    fn drop_newest_full_rejection_drops_payload_lock_free() {
        let topic = LossyTopic::new(Lossy::DropNewest(NonZeroUsize::MIN));
        let _slow = topic.subscribe();
        let (publisher, state) = newest_parts(&topic);
        let armed = Arc::new(AtomicBool::new(false));
        let locks_free = Arc::new(AtomicBool::new(false));
        let reentries = Arc::new(AtomicUsize::new(0));
        let callback = lock_free_reentry(
            Arc::downgrade(&publisher),
            Arc::downgrade(&state),
            Arc::clone(&locks_free),
            Arc::clone(&reentries),
        );
        let item = || CallbackDrop {
            armed: Arc::clone(&armed),
            callback_on_drop: false,
            callback: Arc::clone(&callback),
        };

        assert_eq!(topic.publish(item()).accepted, 1);
        armed.store(true, Ordering::SeqCst);
        assert_eq!(topic.publish(item()).accepted, 0);
        assert!(
            locks_free.load(Ordering::SeqCst),
            "a Full rejected payload is destroyed after every mutex unlocks"
        );
        assert_eq!(reentries.load(Ordering::SeqCst), 1);
        assert_eq!(recover(&publisher.subscribers).len(), 2);
    }

    /// Watched break: `scripts/break-once.sh` moved the deferred payload
    /// destruction into the publication-order scope. The replacement called
    /// `drop(rejected);` before that scope ended. This test failed at "a Closed
    /// rejected payload is destroyed after every mutex unlocks".
    #[test]
    fn drop_newest_closed_rejection_drops_payload_lock_free_and_prunes_by_identity() {
        let topic = LossyTopic::new(Lossy::DropNewest(NonZeroUsize::MIN));
        let closed = topic.subscribe();
        let (publisher, state) = newest_parts(&topic);
        drop(closed);
        let armed = Arc::new(AtomicBool::new(true));
        let locks_free = Arc::new(AtomicBool::new(false));
        let reentries = Arc::new(AtomicUsize::new(0));
        let callback = lock_free_reentry(
            Arc::downgrade(&publisher),
            Arc::downgrade(&state),
            Arc::clone(&locks_free),
            Arc::clone(&reentries),
        );

        assert_eq!(
            topic.publish(CallbackDrop {
                armed,
                callback_on_drop: false,
                callback,
            }),
            LossyFanout {
                subscribers: 0,
                accepted: 0,
            }
        );
        assert!(
            locks_free.load(Ordering::SeqCst),
            "a Closed rejected payload is destroyed after every mutex unlocks"
        );
        assert_eq!(reentries.load(Ordering::SeqCst), 1);
        assert_eq!(
            recover(&publisher.subscribers).len(),
            1,
            "identity pruning preserves the reentrant subscription"
        );
    }

    /// Watched break: `scripts/break-once.sh` changed the pending-loss increment
    /// from saturating addition to replacement `count.get() + 1`. This test
    /// panicked on overflow at the production increment.
    #[tokio::test]
    async fn drop_newest_pending_loss_saturates_at_u64_max() {
        let topic = LossyTopic::new(Lossy::DropNewest(NonZeroUsize::MIN));
        let mut subscription = topic.subscribe();
        assert_eq!(topic.publish('A').accepted, 1);
        topic.seed_pending_for_test(u64::MAX - 1);
        assert_eq!(topic.publish('B').accepted, 0);
        assert_eq!(topic.publish('C').accepted, 0);
        assert_eq!(subscription.next().await, Some(Lagged::Item('A')));
        assert_eq!(topic.publish('D').accepted, 1);
        assert_eq!(
            subscription.next().await,
            Some(Lagged::Skipped(NonZeroU64::MAX))
        );
        assert_eq!(subscription.next().await, Some(Lagged::Item('D')));
    }
}
