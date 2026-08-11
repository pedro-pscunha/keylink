//! Keyed fan-out: one reliable [`Topic`] per live key.

use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};

use super::{Fanout, Overflow, Subscription, Topic};

#[derive(Debug)]
struct Entry<T> {
    topic: Topic<T>,
    live_routes: AtomicUsize,
}

#[derive(Debug)]
struct Inner<K, T> {
    entries: Mutex<HashMap<K, Arc<Entry<T>>>>,
    overflow: Overflow,
}

impl<K, T> Inner<K, T> {
    fn lock(&self) -> MutexGuard<'_, HashMap<K, Arc<Entry<T>>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One [`Topic`] per live key, created by the first [`route`](Self::route).
///
/// Every key uses the same reliable [`Overflow`] policy and reports the same
/// [`Fanout`] outcome as [`Topic::publish`]. Publishing an unknown key is a
/// no-op: it returns zero counts with literally zero allocations and without
/// creating a route. Dropping a key's last route removes its map entry; an in-flight
/// publish may finish on its existing snapshot. Dropping the last keyed publisher
/// ends every remaining route.
///
/// A route is directly compatible with a generated feed:
///
/// ```
/// use std::num::NonZeroUsize;
/// use keylink::compose::{KeyedTopic, Overflow};
///
/// keylink::service! {
///     service Book;
///     rpc Get() -> ();
///     feed Fills(fill: u64);
/// }
///
/// struct Book;
///
/// impl book::Actor for Book {
///     async fn get(&mut self, _cx: keylink::Caller<'_>) {}
///     async fn on_fills(&mut self, _fill: u64) {}
/// }
///
/// let fills = KeyedTopic::<u64, u64>::new(Overflow::BlockPublisher(NonZeroUsize::MIN));
/// let _book: book::Mountable = book::setup(Book)
///     .feeds(book::Feeds::new(fills.route(7)))
///     .mountable();
/// ```
#[derive(Debug)]
pub struct KeyedTopic<K, T> {
    inner: Arc<Inner<K, T>>,
}

impl<K, T> KeyedTopic<K, T> {
    /// An empty keyed topic whose future keys all use `overflow`.
    #[must_use]
    pub fn new(overflow: Overflow) -> Self {
        Self {
            inner: Arc::new(Inner {
                entries: Mutex::new(HashMap::new()),
                overflow,
            }),
        }
    }
}

impl<K, T> KeyedTopic<K, T>
where
    K: Eq + Hash + Clone,
{
    /// Own the stream for `key`, creating its inner topic on the first route.
    #[must_use]
    pub fn route(&self, key: K) -> KeyedSubscription<K, T> {
        let map_key = key.clone();
        let mut entries = self.inner.lock();
        let entry = if let Some(entry) = entries.get(&key) {
            Arc::clone(entry)
        } else {
            let entry = Arc::new(Entry {
                topic: Topic::new(self.inner.overflow),
                live_routes: AtomicUsize::new(0),
            });
            entries.insert(map_key, Arc::clone(&entry));
            entry
        };
        entry.live_routes.fetch_add(1, Ordering::Relaxed);
        let subscription = entry.topic.subscribe();
        drop(entries);

        KeyedSubscription {
            key,
            entry: Arc::downgrade(&entry),
            inner: Arc::downgrade(&self.inner),
            subscription,
        }
    }
}

impl<K, T> KeyedTopic<K, T>
where
    K: Eq + Hash,
    T: Clone,
{
    /// Publish to the routes for `key` under this keyed topic's policy.
    ///
    /// The key map is released before the inner topic is awaited, so a blocked
    /// key does not delay publishes or route changes for another key.
    pub async fn publish(&self, key: &K, item: T) -> Fanout {
        let entry = self.inner.lock().get(key).cloned();
        match entry {
            Some(entry) => entry.topic.publish(item).await,
            None => Fanout {
                delivered: 0,
                dropped: 0,
                gone: 0,
            },
        }
    }
}

impl<K, T> Clone for KeyedTopic<K, T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// An owned route from one [`KeyedTopic`] key.
///
/// This is a [`Stream`](crate::Stream) whose item is `T`. It owns its key and
/// route registration, so it is `Send + 'static` when `K` and `T` are.
#[derive(Debug)]
pub struct KeyedSubscription<K: Eq + Hash, T> {
    key: K,
    entry: Weak<Entry<T>>,
    inner: Weak<Inner<K, T>>,
    subscription: Subscription<T>,
}

impl<K: Eq + Hash, T> Unpin for KeyedSubscription<K, T> {}

impl<K: Eq + Hash, T> crate::Stream for KeyedSubscription<K, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        Pin::new(&mut self.get_mut().subscription).poll_next(cx)
    }
}

impl<K: Eq + Hash, T> Drop for KeyedSubscription<K, T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut entries = inner.lock();
        let Some(entry) = self.entry.upgrade() else {
            return;
        };
        let previous = entry.live_routes.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "a route count cannot underflow");
        let removed = (previous == 1
            && entries
                .get(&self.key)
                .is_some_and(|current| Arc::ptr_eq(current, &entry)))
        .then(|| entries.remove_entry(&self.key))
        .flatten();
        drop(entries);
        drop(removed);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use n0_future::StreamExt;

    use super::*;

    /// A stale route cleanup must identify its entry, not only its key.
    ///
    /// Made red by the first implementation, which removed by key whenever the
    /// old entry's count reached zero. Replayed after the fix with
    /// `scripts/break-once.sh` by replacing the pointer comparison with
    /// `.is_some_and(|_current| true)`. Watched: the publish returned zero instead
    /// of delivering to the replacement.
    #[derive(Debug)]
    struct ReentrantCloneKey {
        id: u8,
        inner: Weak<Inner<Self, ()>>,
    }

    impl Clone for ReentrantCloneKey {
        fn clone(&self) -> Self {
            let inner = self.inner.upgrade().expect("the topic is alive");
            assert!(
                inner.entries.try_lock().is_ok(),
                "key cloning must not run under the global map lock"
            );
            Self {
                id: self.id,
                inner: Weak::clone(&self.inner),
            }
        }
    }

    impl PartialEq for ReentrantCloneKey {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }

    impl Eq for ReentrantCloneKey {}

    impl std::hash::Hash for ReentrantCloneKey {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.id.hash(state);
        }
    }

    /// Caller `Clone` must run before the global key-map mutex is held.
    ///
    /// Search `        let map_key = key.clone();`. Made red with
    /// `scripts/break-once.sh` by moving that line immediately after the map-lock
    /// acquisition. The clone's `try_lock` assertion observed the held mutex.
    #[test]
    fn route_clones_the_key_before_locking_the_global_map() {
        let topic =
            KeyedTopic::<ReentrantCloneKey, ()>::new(Overflow::BlockPublisher(NonZeroUsize::MIN));
        let key = ReentrantCloneKey {
            id: 1,
            inner: Arc::downgrade(&topic.inner),
        };

        let _route = topic.route(key);
    }

    #[tokio::test]
    async fn late_old_cleanup_does_not_remove_a_replacement_generation() {
        let topic = KeyedTopic::new(Overflow::BlockPublisher(NonZeroUsize::MIN));
        let old = topic.route(1_u64);

        topic.inner.lock().remove(&1);
        let mut replacement = topic.route(1);
        drop(old);

        assert_eq!(topic.publish(&1, 7_u64).await.delivered, 1);
        assert_eq!(replacement.next().await, Some(7));
    }
}
