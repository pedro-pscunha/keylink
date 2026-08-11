//! Process-isolated allocation guard for unknown keyed publishes.
//!
//! This target has its own global allocator, so it must never join the shared
//! `gate` process. The aggregate inventory and `mise.toml` name it as isolated.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use keylink::compose::{Fanout, KeyedTopic, Overflow};

struct Counting;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: this allocator delegates the unchanged layout to `System`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` came from the delegated `System` allocation.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Made red before `KeyedTopic` existed. Replayed with
/// `scripts/break-once.sh` by replacing the exact production literal
/// `let entry = self.inner.lock().get(key).cloned();`. Replacement:
/// `let entry = self.inner.lock().get(key).cloned(); drop(Box::new(item.clone()));`.
/// Watched: one allocation instead of literal zero.
#[test]
fn publishing_an_unknown_key_allocates_exactly_zero_times() {
    let topic = KeyedTopic::<u64, u64>::new(Overflow::BlockPublisher(NonZeroUsize::MIN));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    // Warm the test harness's per-thread output capture before measuring the
    // production operation. The isolated target removes other test threads; this
    // removes the one lazy harness allocation on this thread.
    let mut warmup = std::pin::pin!(topic.publish(&403, 0));
    assert!(matches!(
        Future::poll(warmup.as_mut(), &mut cx),
        Poll::Ready(_)
    ));

    let mut publish = std::pin::pin!(topic.publish(&404, 1));
    ALLOCATIONS.store(0, Ordering::Relaxed);
    let outcome = Future::poll(publish.as_mut(), &mut cx);
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);

    assert_eq!(
        outcome,
        Poll::Ready(Fanout {
            delivered: 0,
            dropped: 0,
            gone: 0,
        })
    );
    assert_eq!(
        allocations, 0,
        "unknown publish must allocate literally nothing"
    );
}
