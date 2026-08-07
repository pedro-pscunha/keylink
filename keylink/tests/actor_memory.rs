//! What an idle actor costs, measured rather than asserted.
//!
//! One test in its own binary, because the measurement is a process-wide
//! allocation counter and a second test running beside it would be noise. That
//! is also why the two paths below are two **phases of one test** rather than two
//! tests: cargo runs the tests in a binary on parallel threads, and `LIVE` is
//! process-wide, so a second `#[test]` would measure this one's allocations as
//! well as its own.
//!
//! The numbers this prints are the answer to "can one service hold ten thousand
//! live actors": the framework's own cost per actor, with an empty state, so a
//! real deployment adds `size_of` its own state on top.
//!
//! **There are two mailboxes and they are measured separately.** A keyed actor's
//! mailbox is the registry's — `Mailbox<M> = mpsc::Receiver<Box<M>>`, boxed at the
//! channel. A local actor's is [`LocalActor`](keylink::LocalActor)'s own, and it
//! carries a second channel besides: the `watch` that answers `ended()`. Only the
//! keyed one was measured here for months, which is exactly how a change to the
//! local mailbox could have landed against a remembered number instead of a
//! measured one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::{GlobalAlloc, Layout, System as Libc};
use std::sync::atomic::{AtomicIsize, Ordering};

use keylink::{ActorId, Caller, Eviction};

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
    async fn note(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
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
/// mailbox costs `32 * size_of::<T>()` whatever its capacity is. That is why a
/// mailbox slot is boxed: unboxed it is hundreds of bytes, which would be
/// kilobytes per idle actor and tens of megabytes across ten thousand.
///
/// **The slot type is now per service, and 2048 is therefore scoped to `ledger`
/// on purpose.** Read this before treating a red run as a keylink regression.
///
/// `ledger::Mail` holds this service's own `Request` and a `oneshot::Sender` of
/// its own `Response`, so it is sized to whichever is largest — a caller's
/// decision rather than keylink's. Boxing keeps the *mailbox* out of that: a slot
/// is a pointer, so the eager block is 32 × 8 bytes whatever `Mail` costs.
///
/// It does **not** keep the idle actor out of it, and the tempting sentence that
/// it does was measured and is false. The actor's task is spawned at mount, and
/// the future it runs destructures a `Mail` and holds those fields across the
/// awaits of one turn, so the task allocation is sized to the service's own
/// message. Measured in this test's own shape, at 2,000 actors per service:
///
/// | `size_of::<Mail>()` | bytes per idle actor |
/// |---|---|
/// | 232 (`ledger`, below) | 1,686 |
/// | 1,248 (four `[u64; 32]` args) | 4,662 |
///
/// About 2.9 bytes of idle cost per byte of `Mail`, which puts the ceiling of this
/// budget at a `Mail` of roughly 360 bytes. So `Empty` and a one-field request
/// measure **keylink's floor for a small service**, not a bound any service holds.
///
/// **What that means for the generated mailbox enum.** When `Mail` becomes a
/// per-service enum sized to its largest variant, this number moves for a reason
/// that is the *service's* size and not a keylink regression. The response is to
/// re-derive the budget from the new `size_of::<ledger::Mail>()` — which the test
/// prints — and to check the framework part separately by shrinking the service,
/// not to raise the literal until it passes.
const BUDGET_PER_ACTOR: isize = 2048;

/// The same ceiling, for an actor started with `ledger::actor(..)` instead of
/// mounted in a [`System`](keylink::System).
///
/// A separate literal because a local actor pays for a different set of things
/// and there is no honest way to fold them into one number. It has **two**
/// channels — its mailbox, and the `watch` that carries
/// [`Ended`](keylink::Ended) — and an `Arc<JoinHandle>`, and it has no registry
/// entry, no [`ActorId`] and no `HashMap` slot.
///
/// The mailbox is where the two paths differ most, and it is why this number is
/// recorded before anything moves rather than afterwards. A keyed mailbox slot is
/// a `Box<M>`, so the eagerly allocated block of 32 is 256 bytes whatever the
/// service is. A local mailbox slot is **not boxed**: today its message is a
/// pointer-sized boxed closure and the block is small by accident of that shape,
/// not by decision. Whatever the local slot becomes, it lands in this number, and
/// this number was taken by measurement.
///
/// Measured on this machine, at ten thousand actors of the `ledger` service
/// below, with an empty state:
///
/// | path | bytes per idle actor |
/// |---|---|
/// | keyed, mounted in a `System` | 1,671 |
/// | local, `ledger::actor(..)`   | 1,725 |
///
/// The 54 byte difference is the second channel and the `Arc<JoinHandle>`,
/// against the registry entry the keyed one pays for instead.
const BUDGET_PER_LOCAL_ACTOR: isize = 2048;

#[tokio::test]
async fn ten_thousand_idle_actors_fit_in_the_budget() {
    const ACTORS: usize = 10_000;

    // ── phase 1: keyed, mounted in a registry ────────────────────────────────

    let system: ledger::System = ledger::System::builder(Eviction::Never)
        .capacity(ACTORS)
        .build();
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
    // eagerly allocated block of 32 slots, so an unboxed slot would cost this
    // much per idle actor on its own.
    //
    // `ledger::Mail` and not `keylink::Mail`: the slot is this service's own
    // type, and there is no service-independent number to print any more. That
    // is the honest version of the table `Mailbox`'s documentation carries — the
    // figure moves with whoever wrote the service.
    let unboxed = std::mem::size_of::<ledger::Mail>() * 32;
    println!(
        "one ledger::Mail is {} bytes; unboxed, a mailbox alone would be {unboxed} bytes per actor",
        std::mem::size_of::<ledger::Mail>()
    );

    assert_eq!(system.len(), ACTORS);

    // ── phase 2: local, started with `actor()` ───────────────────────────────
    //
    // Sequential rather than a second `#[test]`, for the reason in the module
    // header. The actors from phase 1 stay alive, which costs nothing here: the
    // measurement is a delta.
    //
    // The `Vec` is reserved BEFORE the baseline is read, so its own block is not
    // counted against the actors it holds. Phase 1 has no equivalent line because
    // the registry's `HashMap` growth genuinely is part of what a mounted actor
    // costs.
    let mut handles: Vec<ledger::Handle<Empty>> = Vec::with_capacity(ACTORS);
    settle().await;

    let before_local = LIVE.load(Ordering::Relaxed);
    for _ in 0..ACTORS {
        handles.push(ledger::actor(Empty));
    }
    settle().await;
    let after_local = LIVE.load(Ordering::Relaxed);

    let total_local = after_local - before_local;
    let each_local = total_local / ACTORS as isize;
    println!("{ACTORS} idle LOCAL actors cost {total_local} bytes, {each_local} bytes each");

    assert_eq!(handles.len(), ACTORS);
    assert!(
        handles.iter().all(ledger::Handle::is_running),
        "a handle that already stopped is not an idle actor"
    );

    assert!(
        each <= BUDGET_PER_ACTOR,
        "a keyed idle actor costs {each} bytes, over the {BUDGET_PER_ACTOR} byte budget \
         scoped to a ledger::Mail of {} bytes — read BUDGET_PER_ACTOR's \
         documentation before moving the literal",
        std::mem::size_of::<ledger::Mail>()
    );
    assert!(
        each_local <= BUDGET_PER_LOCAL_ACTOR,
        "a local idle actor costs {each_local} bytes, over the \
         {BUDGET_PER_LOCAL_ACTOR} byte budget — read BUDGET_PER_LOCAL_ACTOR's \
         documentation before moving the literal"
    );
}

/// Let every spawned task run until it parks.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}
