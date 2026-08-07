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

// The same service with a **kilobyte** of declared arguments, so the third phase
// can ask whether an idle actor's cost is keylink's or the service author's.
//
// Four `[u64; 32]`s, which is the shape Wave 1a measured the old design with —
// kept identical so the two runs are comparable rather than merely both large.
keylink::service! {
    service Bulky;

    rpc Note(a: [u64; 32], b: [u64; 32], c: [u64; 32], d: [u64; 32]) -> u64;
}

/// No state at all, so what is measured is the actor system and nothing else.
struct Empty;

impl ledger::Actor for Empty {
    async fn note(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        entry
    }
}

impl bulky::Actor for Empty {
    async fn note(
        &mut self,
        _cx: Caller<'_>,
        a: [u64; 32],
        _b: [u64; 32],
        _c: [u64; 32],
        _d: [u64; 32],
    ) -> u64 {
        a[0]
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
/// # This budget bounds keylink, and phase 3 is what makes that true
///
/// It did not used to. `Mail` is the service's own type — sized to its largest
/// request and its largest reply — and the task spawned beside the mailbox used
/// to run a future that **inlined the whole turn's state machine**, so an idle
/// actor's cost grew with whatever the caller declared. Measured against the old
/// design: 1,686 bytes per idle actor at a 232 byte `Mail`, and 4,662 at a 1,248
/// byte one. About 2.9 bytes of permanent cost per byte of message, which made
/// this literal a bound on the *caller's* types wearing keylink's name.
///
/// The turn's future is boxed at the loop's await now, exactly as the mailbox
/// slot is boxed at the channel, and for the same reason written one layer in:
/// the two things sized by the service are the message and the turn, and an
/// **idle** actor should be paying for neither. What is left in the task's own
/// frame is the loop — the state, the observers, one `recv`, one pointer.
///
/// So the number to watch is not this one but the *difference* phase 3 measures,
/// and the response to a red run here is to find the allocation, not to raise the
/// literal.
///
/// What it costs is one heap allocation per message for the turn's state machine,
/// on a path that already allocates the mailbox slot, and — for a served call — a
/// QUIC stream and a postcard decode. That is the trade, stated where it is paid:
/// **an allocation per message against a kilobyte per idle actor.**
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
/// The mailbox is where the two paths differ most, and it is why this number was
/// recorded before anything moved rather than afterwards. A keyed mailbox slot is
/// a `Box<M>`, so the eagerly allocated block of 32 is 256 bytes whatever the
/// service is. The local slot was **not** boxed: its message was a pointer-sized
/// boxed closure, so the block was small by accident of that shape rather than by
/// decision, and dropping an enum in where the fat pointer had been would have
/// been a silent thirty-fold. It is boxed now, at the same place and for the same
/// stated reason as the keyed one.
///
/// Measured on this machine, at ten thousand actors of the `ledger` service
/// below, with an empty state:
///
/// | path | before the generated mailbox | after |
/// |---|---|---|
/// | keyed, mounted in a `System` | 1,671 | 1,159 |
/// | local, `ledger::actor(..)`   | 1,725 | 1,469 |
///
/// Both **fell**, which is not what a wave that replaces a fat pointer with an
/// enum sounds like it should do. The mailbox slot is unchanged — `ledger::Mail`
/// is 232 bytes either way — and what moved is the turn's state machine, which
/// the old keyed loop inlined into the task's own future and the new one boxes.
///
/// The local path keeps a 310 byte margin over the keyed one: a second channel,
/// the `watch` that carries `Ended`, and an `Arc<JoinHandle>`, against the
/// registry entry and `HashMap` slot the keyed one pays for instead.
const BUDGET_PER_LOCAL_ACTOR: isize = 2048;

/// How much an idle actor is allowed to cost **because of what the service
/// declares**, over the whole kilobyte between `ledger::Mail` and `bulky::Mail`.
///
/// This is the literal that makes [`BUDGET_PER_ACTOR`] a statement about keylink.
/// Without it, that budget is satisfiable by writing a small service and says
/// nothing about anyone else's; with it, the claim is that the two budgets bound
/// the framework because the *slope* is flat.
///
/// Not zero, and deliberately not: `Empty` is the same state in both, but two
/// services are two `HashMap`s, two `OnceLock`s of ALPN bytes and two
/// monomorphisations, and the allocator's own rounding lands differently. What is
/// asserted is that the difference is **noise beside the message**, not that it
/// is nothing.
const BUDGET_PER_DECLARED_BYTE: isize = 128;

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

    // ── phase 3: the slope ───────────────────────────────────────────────────
    //
    // The one that makes the two budgets above statements about keylink. The
    // same measurement, at a service whose declared arguments are a kilobyte
    // larger — so if an idle actor's cost tracked the message, this would be
    // thousands of bytes higher and the budgets would be bounding whoever wrote
    // the service rather than the framework.

    let bulky: bulky::System = bulky::System::builder(Eviction::Never)
        .capacity(ACTORS)
        .build();
    settle().await;

    let before_bulky = LIVE.load(Ordering::Relaxed);
    for n in 0..ACTORS {
        bulky
            .mount(
                ActorId::new(format!("obra/{n}")).expect("a valid id"),
                bulky::spawn(Empty),
            )
            .expect("every id is distinct");
    }
    settle().await;
    let after_bulky = LIVE.load(Ordering::Relaxed);

    let each_bulky = (after_bulky - before_bulky) / ACTORS as isize;
    let declared =
        std::mem::size_of::<bulky::Mail>() as isize - std::mem::size_of::<ledger::Mail>() as isize;
    println!(
        "one bulky::Mail is {} bytes against ledger's {}, and a bulky idle actor \
         costs {each_bulky} bytes against {each}",
        std::mem::size_of::<bulky::Mail>(),
        std::mem::size_of::<ledger::Mail>(),
    );

    assert_eq!(bulky.len(), ACTORS);
    assert!(
        declared > 512,
        "the two services must differ by enough for the comparison to mean \
         anything; they differ by {declared} bytes of Mail"
    );
    assert!(
        each_bulky - each <= BUDGET_PER_DECLARED_BYTE,
        "an idle actor costs {each_bulky} bytes for a service whose Mail is \
         {declared} bytes larger, against {each} for the small one — a difference \
         of {} bytes, over the {BUDGET_PER_DECLARED_BYTE} byte slack. An idle \
         actor is paying for what the service declares, which is the property the \
         box on the turn's future exists to hold",
        each_bulky - each
    );

    // Last, and the order is deliberate: the slope above is what makes these two
    // statements about keylink, so a run that broke the slope should say so
    // rather than reporting whichever budget it happened to push over first.
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
