//! What an idle actor costs, measured rather than asserted.
//!
//! One test in its own binary, because the measurement is a process-wide
//! allocation counter and a second test running beside it would be noise.
//!
//! The number this prints is the answer to "can one service hold ten thousand
//! live actors": it is the framework's own cost per mounted actor, with an
//! empty state, so a real deployment adds `size_of` its own state on top.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::{GlobalAlloc, Layout, System as Libc};
use std::sync::atomic::{AtomicIsize, Ordering};

use keylink::{ActorId, Eviction, System};

/// Counts live heap bytes. Relaxed ordering: this is a measurement, and a
/// missed reordering costs one allocation's worth of accuracy out of millions.
struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        unsafe { Libc.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { Libc.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(
            new_size as isize - layout.size() as isize,
            Ordering::Relaxed,
        );
        unsafe { Libc.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

keylink::service! {
    service Ledger;

    rpc Note(entry: u64) -> u64;
}

/// No state at all, so what is measured is the actor system and nothing else.
struct Empty;

impl ledger::Actor for Empty {
    async fn note(&mut self, entry: u64) -> u64 {
        entry
    }
}

/// The ceiling this design is willing to pay per idle actor.
///
/// Not a measurement — a budget. At ten thousand actors it caps the framework's
/// own footprint at 20 MiB, which is what makes "ten thousand live actors in one
/// service" a sentence about a laptop rather than about a machine.
///
/// The cost is dominated by two allocations: the spawned task, and the mailbox.
/// `tokio::sync::mpsc` allocates its first block of 32 slots eagerly, so the
/// mailbox costs `32 * size_of::<T>()` whatever its capacity is. That is why
/// `Mail` is boxed on the way in: unboxed it is hundreds of bytes, which would
/// be kilobytes per idle actor and tens of megabytes across ten thousand.
const BUDGET_PER_ACTOR: isize = 2048;

#[tokio::test]
async fn ten_thousand_idle_actors_fit_in_the_budget() {
    const ACTORS: usize = 10_000;

    let system = System::builder(Eviction::Never).capacity(ACTORS).build();
    settle().await;

    let before = LIVE.load(Ordering::Relaxed);
    for n in 0..ACTORS {
        system
            .mount(
                ActorId::new(format!("obra/{n}")).expect("a valid id"),
                ledger::spawn(Empty),
            )
            .expect("every id is distinct");
    }
    // Every task has to be polled once to reach the state it idles in.
    settle().await;
    let after = LIVE.load(Ordering::Relaxed);

    let total = after - before;
    let each = total / ACTORS as isize;
    println!("{ACTORS} idle actors cost {total} bytes, {each} bytes each");

    // The boxing decision, in numbers rather than in prose. A mailbox holds one
    // eagerly allocated block of 32 slots, so an unboxed `Mail` would cost this
    // much per idle actor on its own.
    let unboxed = std::mem::size_of::<keylink::Mail>() * 32;
    println!(
        "one Mail is {} bytes; unboxed, a mailbox alone would be {unboxed} bytes per actor",
        std::mem::size_of::<keylink::Mail>()
    );

    assert_eq!(system.len(), ACTORS);
    assert!(
        each <= BUDGET_PER_ACTOR,
        "an idle actor costs {each} bytes, over the {BUDGET_PER_ACTOR} byte budget"
    );
}

/// Let every spawned task run until it parks.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}
