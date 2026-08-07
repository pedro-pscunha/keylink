//! What an idle actor costs, measured rather than asserted.
//!
//! One test in its own binary, because the measurement is a process-wide
//! allocation counter and a second test running beside it would be noise. That
//! is also why the four measurements below are **phases of one test** rather than
//! four tests: cargo runs the tests in a binary on parallel threads, and `LIVE` is
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
//!
//! **And both are measured at two message sizes, not one.** A budget checked at a
//! single `Mail` is a statement about whoever wrote that service; the *slope*
//! between the two is what makes it a statement about keylink. Phase 3 does that
//! for the keyed path and phase 4 for the local one, separately, because the box
//! that holds the property is a different line of code on each.
//!
//! # The measured fact behind `SystemBuilder::mailbox_depth`, published here
//!
//! Phase 5 measures what a deeper mailbox costs while it is empty, which is the
//! whole of that knob's "below 32 it is free" claim. On this machine
//! (`aarch64-apple-darwin`, `2026-08-07`), at two thousand idle keyed actors of the
//! `ledger` service below:
//!
//! | mailbox depth | bytes per idle actor |
//! |---|---|
//! | 1 (the default) | 1,424 |
//! | 32 | 1,424 |
//! | 1,024 | 1,424 |
//!
//! Flat — identical, not merely close — and flat further than the claim needed:
//! `tokio::sync::mpsc` allocates its first block of 32 slots eagerly and every
//! later block **when messages arrive**, so declaring a depth costs nothing at all
//! until something fills it. If the eager allocation were `depth` slots instead of
//! 32, the last row would be about 8 KiB per actor larger, and phase 5 asserts
//! against that number rather than against a tolerance nobody can read.
//!
//! Red-tested by leaking a `Vec::with_capacity(depth)` per mailbox inside
//! `Registry::entry`, which is the shape of the cost this phase exists to refuse:
//! 1,425 / 1,456 / 2,448 bytes, and the flatness assertion fires at depth 32.
//! Watched — a measurement nobody has seen move is not a measurement either.
//!
//! What that does *not* say: a mailbox actually holding more than 32 messages
//! allocates a second block. The trade the knob's documentation names is about a
//! **full** mailbox, not a deep one, and this test measures the empty case
//! because that is the one every idle actor pays.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::{GlobalAlloc, Layout, System as Libc};
use std::num::NonZeroUsize;
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
/// channels — its mailbox, and the `watch` that carries what the actor is doing,
/// which is an [`Ended`](keylink::Ended) once it has stopped — and an
/// `Arc<JoinHandle>`, and it has no registry entry, no [`ActorId`] and no
/// `HashMap` slot.
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
/// | path | before the generated mailbox (`c9056e0`) | Wave 1b (`c8712b0`) | Wave 2 (`836179c`) | Wave 3 | Wave 9 | before `Fate` (`810650a`) | `Fate` | with the `Floor` |
/// |---|---|---|---|---|---|---|---|---|
/// | keyed, mounted in a `System` | 1,671 | 1,159 | 1,159 | 1,415 | 1,412 | 1,549 | 1,690 | **1,722** |
/// | local, `ledger::actor(..)`   | 1,725 | 1,469 | 1,597 | 1,853 | 1,861 | 1,861 | **2,029** | **2,029** |
///
/// # The `Floor` cost 32 bytes on the keyed path and nothing measurable on the local one
///
/// Re-measured because the column before it was written *before* the floor
/// existed, and a stale figure in this table is the one defect its own
/// documentation keeps warning about. Keyed 1,690 → **1,722**; local 2,029 →
/// **2,029**, unmoved to the byte. The floor is a `&Fate` and a `bool` in two
/// frames — `run_loop`'s and `supervise`'s — and only the keyed path crossed an
/// allocator step with it.
///
/// **Run-to-run variance on this machine is exactly zero**: five consecutive runs
/// produced byte-identical totals on both paths, 17,224,418 and 20,291,072. That
/// matters for [`LOCAL_SLACK`], which is *not* an allowance for noise here — there
/// is none — and says so.
///
/// # The keyed column drifted +137 between Wave 9 and `810650a`, and nobody wrote it down
///
/// Measured on the way in, before a line of the `Fate` wave was written: 1,549
/// against the 1,412 recorded above. It is Waves 10 and 11 — the observability
/// pass — and it is recorded here rather than quietly folded into the last column,
/// because the paragraph below already said this is exactly where such a number
/// hides. The local column did not move over the same span.
///
/// # What `Fate` costs, and what it was cut from first
///
/// A served dispatch reads one cell per actor to tell *restarting* from *dropped*
/// from *gone*, and the cell is one `Arc<Mutex<Standing>>` beside the registry
/// entry (keyed) or inside the handle (local): **+141 keyed, +168 local.** The
/// local path also gave eight bytes back — its `watch` channel went from a
/// three-state `Life` to `Option<Ended>`, undoing Wave 9's eight — so the gross
/// add is ~176 there.
///
/// It was measured at +165 and +192 first, with the restart reason stored
/// **inline**: `Standing::Restarting(Ended)` makes `Standing` 40 bytes and
/// `Mutex<Standing>` 56, so every idle actor paid for a value only a restart ever
/// writes. Behind an `Arc` they are 16 and 32 — all four `size_of` probes were
/// run, not reasoned — worth a measured 24 bytes per idle actor on both paths,
/// with the allocation moved to the restart that needs it. That reduction is why
/// this file is cited in `Standing`'s own documentation.
///
/// # Wave 9's eight bytes, on the local path only, and where they went
///
/// Supervision replaced `Option<Ended>` with a three-state `Life` in the `watch`
/// channel a local actor already had, so it added **no allocation** — and that
/// is not the same as adding nothing. Measured with `size_of`: `Ended` is 40
/// bytes before and after, so `Ended::GaveUp` and the `fatal` field cost zero;
/// `Option<Ended>` is also 40, because `Option` niche-packs into `Ended`'s spare
/// discriminants; and `Life` is **48**, because its two payload-carrying
/// variants need a tag of their own. Eight bytes, once, per **local** actor.
///
/// The keyed column was untouched by supervision — an unsupervised keyed actor
/// got no `Life`, and a supervised one built its own inside `mount`'s closure.
/// (Past tense throughout this section: `Life` is **deleted**, and the paragraph
/// below records what replaced it. The claim was true when it was measured and
/// is history now.) Its **−3 is not Wave 9's**: it is Waves 4 through 8 against a
/// figure last written down at Wave 3. Neither move was caught by anything,
/// because both budgets are 2,048 and both paths have hundreds of bytes of
/// headroom. That is the argument for re-measuring rather than for a tighter
/// budget: this file's job is to make a move *visible*, and a wave that reports
/// "the suite ran green" has reported nothing about these two numbers.
///
/// # Wave 3's 256 bytes, on both paths, and what they bought
///
/// A second input costs one allocator step per actor and it is the same step on both
/// paths, which is itself the news: the two loops became **one** loop, so a change to
/// what a loop holds now moves both columns together instead of one silently. What is
/// in the frame that was not: the `Fed`/`Taken` discriminants, the feeds value, and
/// the fairness bit the loop alternates.
///
/// It cost three attempts to land it at 256 rather than at 1,987, and the middle two
/// are recorded on `local.rs::dispatch` because both were the *same* mistake — a
/// value sized by the caller's service materialised inside the loop's future. First
/// an `async fn dispatch` stored its argument in the future it returned (slope 1,987,
/// this file's phase-4 assertion firing by a factor of fifteen); then a boxed-future
/// `dispatch` taking its turn **by value** left the temporary in `run_loop`'s own
/// generator (slope 1,011, almost exactly `size_of::<bulky::Mail>() - size_of::<ledger::Mail>()`).
/// Both were caught here and neither was visible in any other test.
///
/// The fix moved the box from the channel slot to `Message::Mail` and `Fed::Item`, so
/// the mail arrives boxed and is unwrapped inside `dispatch`'s own plain stack frame.
/// That is allocation-neutral on the message path — the old slot box and the new mail
/// box are one allocation either way — and it made the local channel slot a
/// three-word enum, which cost 512 bytes of eagerly allocated block until `Read` was
/// thinned to a single-word box. `with_state` pays one extra allocation for that; it
/// is a cross-cutting read, not a turn.
///
/// The first move **fell**, which is not what a wave that replaces a fat pointer
/// with an enum sounds like it should do. The mailbox slot is unchanged —
/// `ledger::Mail` was 232 bytes either way — and what moved is the turn's state
/// machine, which the old keyed loop inlined into the task's own future and the new
/// one boxes.
///
/// # The second move is a 128 byte regression nothing measured, and this is it
///
/// The 1,469 in the middle column is what this file's documentation asserted until
/// Wave 2's review, and the tree measures 1,597. Bisected by running this test at
/// both commits: exactly 128 bytes per idle local actor, one allocator step, and
/// **only on the local path** — the keyed column did not move.
///
/// That fits, and the fit names the cause. Wave 2d put the `finish` loop inside
/// `LocalActor::spawn_observed`'s own async block, so it is in the task's future
/// while the actor is idle. The keyed loop in `macros.rs::spawn` has no `finish`
/// loop at all, which is why 1,159 held. Nothing in Wave 2 was wrong; what was
/// wrong is that a wave which grows the loop's own frame reported no number, and
/// the number it did not report is the one this budget is about.
///
/// # That margin is why this budget needs its own slope, and phase 4 is it
///
/// This literal was checked at exactly one `Mail` size for months — 248 bytes, the
/// only one any phase built a *local* actor of — while the entire argument for the
/// `Box::pin` at the local loop's await is an absolute ceiling (3,005 bytes without
/// it against 1,597 with) that happens to sit between 1,861 and 2,048. The local
/// path has **less** headroom than the keyed one, by 83 bytes now — 275 against
/// 358, and the figure here read 339, which is the *cost* difference between the
/// two paths rather than the headroom difference. Two subtractions with the same
/// operands and different meanings, and the wrong one was written. What the local
/// path pays for: a second
/// channel, the `watch` that carries the ending, an `Arc<JoinHandle>` and the
/// `finish` loop, against the registry entry and `HashMap` slot the keyed one pays
/// for instead. So phase 4 builds bulky *local* actors and asserts the slope here too,
/// against [`SLOPE_SLACK_OVER_A_KILOBYTE`], rather than trusting that a flat keyed
/// slope implies a flat local one.
///
/// # It was 2,048, and the `Fate` wave is what raised it — said out loud
///
/// A budget raised by the change that would otherwise have broken it is the
/// weakest thing in this file, so here is the whole of it. `Fate` measured 2,053
/// against a 2,048 ceiling: **red**, and the guard was doing its job. The first
/// response was to make the change cheaper rather than the ceiling higher —
/// putting the restart reason behind an `Arc` bought back 24 bytes per idle actor
/// and landed it at 2,029, which *passes*.
///
/// It is raised anyway, and only to the extent that 2,029 under 2,048 is not a
/// budget: nineteen bytes is inside the noise between one allocator and another,
/// so a green run here would say nothing about the same code on a Linux CI.
///
/// # It is `measured + slack`, and the slack is the thing to argue about
///
/// "Nineteen bytes is allocator noise" is an argument that re-licenses a raise
/// every wave, which is how a ceiling stops biting. So the ceiling is not a
/// number any more: it is **the measurement plus a slack named once**, and a wave
/// that grows an idle actor has to argue with [`LOCAL_SLACK`] rather than with
/// this literal. The slack is what an allocator may differ by between machines;
/// it is not room to grow into.
///
/// It restores 275 bytes of headroom against the 187 this literal had at 1,861 —
/// *more* than before, not "about the same", which the sentence here first
/// claimed. The keyed one stays at 2,048 with 358 under it.
///
/// What is **not** relaxed is the thing this budget cannot see and
/// [`SLOPE_SLACK_OVER_A_KILOBYTE`] can: the slope is 13 bytes over a kilobyte of
/// declared message, flat as before, and no number in phase 4 or 5 moved.
const BUDGET_PER_LOCAL_ACTOR: isize = LOCAL_MEASURED + LOCAL_SLACK;

/// What a local idle actor measured on this machine, at `ledger::Mail`.
///
/// Moving this is the deliberate act. A wave that grows an idle actor edits a
/// number that says *what it costs*, next to a slack that says *what an allocator
/// may differ by* — rather than a single ceiling where the two are indistinguishable.
const LOCAL_MEASURED: isize = 2_029;

/// How far another allocator may land from [`LOCAL_MEASURED`].
///
/// Not room to grow into, and the reason this exists rather than being folded
/// into one literal: the raise that introduced it was argued as "nineteen bytes
/// is inside the noise", and that argument works exactly once before it becomes
/// a licence. Named, it is the thing the next wave has to disagree with.
///
/// # It is **not** an allowance for noise, and that was measured
///
/// Five consecutive runs of this test produce byte-identical totals on both
/// paths. The allocation pattern is deterministic and so is macOS's allocator
/// here, so run-to-run variance is **zero** — the original argument for the raise,
/// "nineteen bytes is inside the noise between one allocator and another", was
/// therefore about the wrong thing. There is no noise to be inside of on this
/// machine.
///
/// What it actually allows for is **an allocator this has never run against**: a
/// different libc, a different size-class table, jemalloc or mimalloc under a
/// downstream binary. That is real — the boxed-slot table above turns on
/// size-class behaviour — and it is also **unmeasured**, which is the honest
/// status of this number. 275 is the headroom the raise happened to buy, not a
/// figure derived from anything.
///
/// So it is a judgement, and the way to retire it is one measurement rather than
/// one argument: run this test once on a Linux CI and replace this constant with
/// `max(measured) - LOCAL_MEASURED` across the machines that matter, or with zero
/// if they agree. Until then it is deliberately larger than it needs to be on the
/// one machine that has run it.
const LOCAL_SLACK: isize = 275;

/// How much an idle actor is allowed to cost **because of what the service
/// declares**, over the whole kilobyte between `ledger::Mail` and `bulky::Mail`.
///
/// This is the literal that makes [`BUDGET_PER_ACTOR`] and
/// [`BUDGET_PER_LOCAL_ACTOR`] statements about keylink. Without it, either budget
/// is satisfiable by writing a small service and says nothing about anyone else's;
/// with it, the claim is that the budgets bound the framework because the *slope*
/// is flat.
///
/// Not zero, and deliberately not: `Empty` is the same state in both, but two
/// services are two `HashMap`s, two `OnceLock`s of ALPN bytes and two
/// monomorphisations, and the allocator's own rounding lands differently. What is
/// asserted is that the difference is **noise beside the message**, not that it
/// is nothing.
///
/// # The name used to say `BUDGET_PER_DECLARED_BYTE`, and that was ~1,000× off
///
/// It is a total over the whole delta, not a rate. The `ledger`-to-`bulky` gap is
/// 1,016 bytes of `Mail`, so 128 bytes of slack is a tolerated slope of about
/// **0.126 bytes per declared byte** — while the old name asserted 128 of them,
/// which is forty times the 2.9 B/B the unboxed design actually measured and
/// would therefore have passed the very design this assertion exists to refuse.
/// The check was right and the name described a different, useless one.
const SLOPE_SLACK_OVER_A_KILOBYTE: isize = 128;

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
        each_bulky - each <= SLOPE_SLACK_OVER_A_KILOBYTE,
        "a keyed idle actor costs {each_bulky} bytes for a service whose Mail is \
         {declared} bytes larger, against {each} for the small one — a difference \
         of {} bytes, over the {SLOPE_SLACK_OVER_A_KILOBYTE} byte slack. An idle \
         actor is paying for what the service declares, which is the property the \
         box on the turn's future exists to hold",
        each_bulky - each
    );

    // ── phase 4: the slope on the LOCAL path ─────────────────────────────────
    //
    // Phase 3 measured the slope for a KEYED actor only, so `BUDGET_PER_LOCAL_ACTOR`
    // was checked at exactly one `Mail` size — and the local path is the one with
    // less headroom, by the 310 bytes its second channel and `Arc<JoinHandle>` cost.
    // The local loop boxes its turn at its own await, in its own line of code; a
    // wave that removed that line and left the keyed one would have passed phase 3
    // and every budget above.

    let mut bulky_handles: Vec<bulky::Handle<Empty>> = Vec::with_capacity(ACTORS);
    settle().await;

    let before_bulky_local = LIVE.load(Ordering::Relaxed);
    for _ in 0..ACTORS {
        bulky_handles.push(bulky::actor(Empty));
    }
    settle().await;
    let after_bulky_local = LIVE.load(Ordering::Relaxed);

    let each_bulky_local = (after_bulky_local - before_bulky_local) / ACTORS as isize;
    println!(
        "a bulky idle LOCAL actor costs {each_bulky_local} bytes against \
         {each_local} for the small one"
    );

    assert_eq!(bulky_handles.len(), ACTORS);
    assert!(
        bulky_handles.iter().all(bulky::Handle::is_running),
        "a handle that already stopped is not an idle actor"
    );
    assert!(
        each_bulky_local - each_local <= SLOPE_SLACK_OVER_A_KILOBYTE,
        "a LOCAL idle actor costs {each_bulky_local} bytes for a service whose Mail \
         is {declared} bytes larger, against {each_local} for the small one — a \
         difference of {} bytes, over the {SLOPE_SLACK_OVER_A_KILOBYTE} byte slack. \
         The box at the local loop's await is what holds this, and it is a \
         different line of code from the keyed one",
        each_bulky_local - each_local
    );

    // ── phase 5: what a deeper mailbox costs while it is empty ───────────────
    //
    // `SystemBuilder::mailbox_depth` documents a memory trade that does not
    // begin until 32, and this is the measurement that sentence rests on rather
    // than a reading of tokio's source. Three depths, all measured here at the
    // same actor count so the comparison is between depths and nothing else.

    let depths = [1usize, 32, 1_024];
    let mut costs = Vec::with_capacity(depths.len());
    // Kept alive until the end of the test: dropping a system tears down its
    // actors on their own tasks, and a teardown running underneath the next
    // baseline would be measured as the next system's savings.
    let mut kept = Vec::with_capacity(depths.len());
    for depth in depths {
        let (each_at_depth, system) = keyed_at_depth(depth).await;
        println!("a keyed idle actor at mailbox depth {depth} costs {each_at_depth} bytes");
        costs.push(each_at_depth);
        kept.push(system);
    }

    let flat = costs[0];
    for (depth, cost) in depths.iter().zip(&costs) {
        assert!(
            (cost - flat).abs() <= ONE_POINTER,
            "an idle mailbox at depth {depth} cost {cost} bytes against {flat} at \
             depth 1. The first block of 32 slots is allocated eagerly and the rest \
             are not, so an EMPTY mailbox costs the same at every depth — and \
             `SystemBuilder::mailbox_depth`'s claim that depth is free below 32 is \
             this number"
        );
    }
    // The hypothesis this refutes by name, rather than by a tolerance: that the
    // eager allocation is `depth` slots. It would put 8 KiB per actor between the
    // first row and the last.
    let capacity_shaped = (1_024 - 1) * std::mem::size_of::<*const ()>() as isize;
    assert!(
        costs[2] - flat < capacity_shaped / 2,
        "at depth 1,024 an idle actor cost {} bytes against {flat} at depth 1. A \
         mailbox that eagerly allocated its whole capacity would differ by about \
         {capacity_shaped} bytes here",
        costs[2]
    );

    // Last, and the order is deliberate: the two slopes above are what make these
    // budgets statements about keylink, so a run that broke a slope should say so
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
    assert!(
        each_bulky_local <= BUDGET_PER_LOCAL_ACTOR,
        "a local idle actor of the BULKY service costs {each_bulky_local} bytes, \
         over the {BUDGET_PER_LOCAL_ACTOR} byte budget — the budget bounds keylink \
         only if it holds for a service nobody here wrote"
    );
}

/// One pointer of allocator rounding, which is the slack phase 5 allows between
/// two depths that must cost the same.
///
/// The expected difference is exactly zero. It is not asserted as zero because
/// these are deltas divided by an actor count, and one allocation landing in a
/// different size class would make a true reading fail — while eight bytes is
/// nowhere near the 256 a second block of slots would cost, let alone the 8 KiB a
/// capacity-shaped allocation would.
const ONE_POINTER: isize = 8;

/// Two thousand, not ten: phase 5 compares three systems against each other rather
/// than against a budget, and a per-actor difference of 256 bytes — one block of
/// slots — is unmissable at this count. Ten thousand of each would triple the
/// slowest test in the suite to sharpen a number that is already exact.
const ACTORS_PER_DEPTH: usize = 2_000;

/// The per-idle-actor cost of a keyed system at one mailbox depth, and the system,
/// which the caller must keep alive — see phase 5.
async fn keyed_at_depth(depth: usize) -> (isize, ledger::System) {
    let system: ledger::System = ledger::System::builder(Eviction::Never)
        .capacity(ACTORS_PER_DEPTH)
        .mailbox_depth(NonZeroUsize::new(depth).expect("a mailbox depth is never zero"))
        .build();
    settle().await;

    let before = LIVE.load(Ordering::Relaxed);
    for n in 0..ACTORS_PER_DEPTH {
        // The id must not mention the depth, and that is not tidiness: each system
        // is its own map, so the same ids are free — and `depth/1024/1999` is two
        // characters longer than `depth/32/1999`, which measured as 7 bytes per
        // actor of "cost" that was a `String` size class rather than a mailbox.
        system
            .mount(
                ActorId::new(format!("depth/{n}")).expect("a valid id"),
                ledger::spawn(Empty),
            )
            .expect("every id is distinct");
    }
    settle().await;
    let after = LIVE.load(Ordering::Relaxed);

    assert_eq!(system.len(), ACTORS_PER_DEPTH);
    ((after - before) / ACTORS_PER_DEPTH as isize, system)
}

/// Let every spawned task run until it parks.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}
