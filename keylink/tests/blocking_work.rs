//! Blocking work inside a handler, and the one difference that is invisible.
//!
//! A turn is the unit of serialization, so a handler that takes 400 ms holds its
//! actor for 400 ms **whichever way it spends them**. That part is correct and is
//! what an actor is for. What differs is who *else* pays:
//!
//! - `spawn_blocking(..).await` moves the work to the blocking pool and yields, so
//!   the tokio worker is free and unrelated tasks keep running;
//! - blocking the thread **synchronously** holds the turn *and* the worker, and
//!   every other task on that runtime stops.
//!
//! Nothing reddens, nothing warns, and the actor is correct from its own point of
//! view — the damage lands on tasks that have nothing to do with it. That is the
//! profile of a defect that ships, which is why it is worth a guard rather than a
//! paragraph.
//!
//! # These tests need a current-thread runtime, and that is the claim's real shape
//!
//! `#[tokio::test]` is current-thread by default, which is the worst case and the
//! one that makes the effect deterministic: one worker, so one synchronous block
//! stops everything. On a multi-threaded runtime the severity is a ratio rather
//! than a cliff — with `W` workers it takes `W` concurrent synchronous blocks to
//! starve the runtime completely, and fewer to degrade it. `keylink::blocking`'s
//! own runtime is multi-threaded on purpose, so a program using it has `W > 1` and
//! degrades rather than stops. The rule does not change; the blast radius does.
//!
//! The idiom itself is not invented here. `keylink-persist/src/file.rs` uses
//! `tokio::task::spawn_blocking` twice, for a lock-file open and for a
//! write-plus-`sync_all`-plus-rename, and those are the reference for what belongs
//! in the pool: bounded work that returns.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::future::Future;
use std::time::{Duration, Instant};

use keylink::Caller;

keylink::service! {
    service Mill;

    /// Blocks the calling thread for `millis`, inside the turn. The mistake.
    rpc Grind(millis: u64) -> u64;

    /// The same wait, moved to the blocking pool and awaited. The idiom, as
    /// `keylink-persist/src/file.rs` uses it.
    rpc Offload(millis: u64) -> u64;
}

struct Mill;

/// The blocking work itself, in one place on purpose: both methods below spend the
/// same time the same way, so the *only* difference between them is which thread
/// runs this function. That is what makes the pair a controlled comparison rather
/// than two similar tests.
fn nap(millis: u64) -> u64 {
    std::thread::sleep(Duration::from_millis(millis));
    millis
}

/// `spawn_blocking`, behind a name short enough that `rustfmt` leaves its call site
/// on **one** line — which is the only reason this wrapper exists.
///
/// Written inline first, as
/// `tokio::task::spawn_blocking(work).await.expect(..)`, and `cargo fmt` split it
/// across three lines. A multi-line anchor is one the formatter owns and the record
/// does not, so it cannot be promised. Measuring beat predicting here, in one round.
async fn offloaded(work: impl FnOnce() -> u64 + Send + 'static) -> u64 {
    tokio::task::spawn_blocking(work)
        .await
        .expect("pool ran it")
}

impl mill::Actor for Mill {
    async fn grind(&mut self, _cx: Caller<'_>, millis: u64) -> u64 {
        nap(millis)
    }

    async fn offload(&mut self, _cx: Caller<'_>, millis: u64) -> u64 {
        let work = move || nap(millis);
        offloaded(work).await
    }
}

/// Long enough that the two outcomes are an order of magnitude apart, short enough
/// that the whole file costs about a second.
const BLOCK: u64 = 400;

/// What an unrelated task asks the runtime for while the handler is working. Twenty
/// times shorter than the block, so "it woke on time" and "it woke when the block
/// ended" cannot be confused.
const NAP: Duration = Duration::from_millis(20);

/// Run `work`, and report how long an **unrelated** task's short sleep actually
/// took while it ran.
///
/// This is the whole instrument. A `tokio::time::sleep` needs the runtime's timer,
/// and the timer needs the thread — so a thread blocked inside a handler cannot
/// deliver it. The sleep therefore finishes on time on a healthy runtime and no
/// earlier than the block on a starved one.
async fn nap_taken_beside<F: Future>(work: F) -> Duration {
    let sleeper = tokio::spawn(async move {
        let started = Instant::now();
        tokio::time::sleep(NAP).await;
        started.elapsed()
    });
    // Let the sleeper reach its `await` and register the timer before the work
    // begins, or the measurement starts after the damage.
    tokio::task::yield_now().await;

    work.await;
    sleeper.await.expect("the sleeper does not panic")
}

/// The claim: **a synchronous block in a handler starves the whole runtime, and
/// `spawn_blocking` does not.** Same actor, same 400 ms, same unrelated sleeper.
///
/// The two numbers are what make it a guard rather than a description. Beside
/// `offload` the 20 ms nap takes about 20 ms. Beside `grind` it cannot be delivered
/// until the thread is released, so it takes about 400 ms — the sleeper is *held to
/// the length of somebody else's blocking call*, having asked for a twentieth of it.
///
/// Made red by `scripts/break-once.sh` on this file, searching for `offload`'s call
/// to the wrapper above — pass it **with its eight spaces of indentation** —
/// `offloaded(work).await`, and replacing it with `work()` — the mistake, written
/// into the method that exists to avoid it. Watched, at the first assertion:
///
/// > `spawn_blocking` leaves the worker free, so an unrelated 20ms sleep must not be
/// > held to the length of the block: took …
///
/// The elided tail is a wall-clock figure and is **deliberately not quoted**: it
/// moves every run, so quoting it would rot the record on nothing. What it says is
/// the order of magnitude — the 20 ms sleep comes back at four hundred and
/// something, i.e. held to the full length of somebody else's block.
///
/// Both halves quoted, under the indentation rule `tests/cycles.rs` states: this
/// record's copy sits unindented in a `///` line while the code carries eight
/// spaces, so the indented literal is unique. `work()` is quoted rather than
/// described because a reader must not have to invent compiling Rust — `offload`
/// returns `u64`, so a bare `nap(millis);` would not type-check, and an earlier
/// draft of this record made exactly that mistake.
#[tokio::test]
async fn a_synchronous_block_starves_the_runtime_and_spawn_blocking_does_not() {
    let mill = mill::actor(Mill);

    let beside_offload = nap_taken_beside(mill.offload(BLOCK)).await;
    let beside_grind = nap_taken_beside(mill.grind(BLOCK)).await;

    assert!(
        beside_offload < Duration::from_millis(BLOCK / 2),
        "`spawn_blocking` leaves the worker free, so an unrelated {NAP:?} sleep must \
         not be held to the length of the block: took {beside_offload:?}"
    );
    assert!(
        beside_grind >= Duration::from_millis(BLOCK * 3 / 4),
        "a synchronous block holds the worker, so the timer cannot fire until it \
         ends: the {NAP:?} sleep took only {beside_grind:?}"
    );
}

/// The half a reader gets wrong in the other direction: **`spawn_blocking` does not
/// buy concurrency *within* an actor.** The turn is still occupied.
///
/// Two `offload` calls of 400 ms take at least 800 ms, because the second cannot
/// start until the first turn returns — even though the worker was free the whole
/// time and the blocking pool could have run both at once. That is correct and it is
/// the point of a turn; it is worth pinning because "I moved it off the worker"
/// invites the belief that the actor is now parallel.
///
/// A design pin rather than a break record, in `defer.rs`'s sense: serialization is
/// the actor model itself, so there is no one-literal edit that removes it while
/// leaving an actor. Reddening it would mean deleting the mailbox.
#[tokio::test]
async fn spawn_blocking_does_not_make_one_actor_concurrent() {
    let mill = mill::actor(Mill);

    let first = {
        let mill = mill.clone();
        tokio::spawn(async move { mill.offload(BLOCK).await })
    };
    let second = {
        let mill = mill.clone();
        tokio::spawn(async move { mill.offload(BLOCK).await })
    };

    let started = Instant::now();
    first.await.expect("no panic").expect("running");
    second.await.expect("no panic").expect("running");
    let both = started.elapsed();

    assert!(
        both >= Duration::from_millis(BLOCK * 2 * 3 / 4),
        "two turns of {BLOCK}ms cannot overlap, however the work is spent: took {both:?}"
    );
}
