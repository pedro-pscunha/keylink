//! A delayed run and a repeating one, on a handle that cancels.
//!
//! ```ignore
//! // fire once, in five seconds
//! let reminder = after(Duration::from_secs(5), {
//!     let orders = orders.clone();
//!     || async move { let _ = orders.expire(id).await; }
//! });
//!
//! // and every minute until the handle is dropped
//! let sweep = every(Duration::from_secs(60), move || {
//!     let orders = orders.clone();
//!     async move { let _ = orders.sweep().await; }
//! });
//! ```
//!
//! The work is a closure returning a future rather than a message and a handle,
//! and that is the choice this module makes: a scheduler that took
//! `(handle, message)` would need one signature per method kind — a `tell` has no
//! reply, an `rpc` has one nobody is waiting for, and a `blob` has a body — so it
//! would be the generated `Handle`'s job and not a composition. A closure is what
//! a caller already has.
//!
//! # Only `n0_future::time`. There is no `std::time::Instant` here
//!
//! `std::time::Instant::now()` **panics** on `wasm32-unknown-unknown`, and that
//! shipped once in this crate. A scheduler is the module where the mistake is
//! easiest to make, so it is stated rather than assumed: every wait below is
//! [`n0_future::time::sleep`], which is `tokio::time::sleep` natively and
//! `web_time` in a browser, and no clock is read at all — a sleep needs a
//! duration, not a `now`.
//!
//! [`Duration`] is re-exported through `n0_future::time` and is
//! `std::time::Duration` on every target. A `Duration` is not a clock.
//!
//! # Backpressure: a run in progress is never re-entered, and a missed tick is
//! never queued
//!
//! [`every`] sleeps, runs, and sleeps again. So the period is a floor on the
//! **gap between runs**, not a rate: a run that takes longer than the period
//! delays the next one and does not start a second.
//!
//! That is the whole of the scheduler's bound, and it is worth naming what it
//! rules out. [`n0_future::time::interval`]'s default
//! [`MissedTickBehavior::Burst`](n0_future::time::MissedTickBehavior::Burst)
//! fires immediately, repeatedly, to catch up on the ticks it missed — which is a
//! queue of pending work with no limit but elapsed time. Pointed at an actor
//! whose mailbox holds one message, a bursting interval becomes a pile of tasks
//! all waiting for the same permit. So this module does not use `interval` at all,
//! and a reader looking for it finds this paragraph instead.
//!
//! **A slow work function therefore loses ticks, silently.** That is the
//! deliberate trade and the one place in this module where something is dropped
//! without a loud signal: there is nobody to tell. A caller who needs to know how
//! often the work actually ran counts it inside the work, which is the only place
//! that number is true. This is the honest reading of "a schedule is not a
//! queue"; if the ticks matter individually, they are messages and want a
//! [`Topic`](super::Topic) or a mailbox.
//!
//! # The first run of `every` is one period away, not immediate
//!
//! `every(period, work)` sleeps first. `interval`'s first tick is immediate,
//! which makes `every` do two things — "now, and then every period" — and a
//! caller who wants the first one now calls the work themselves. One meaning per
//! function.
//!
//! # A dropped [`Scheduled`] cancels
//!
//! The opposite of [`LocalActor`](crate::LocalActor), deliberately. Dropping a
//! [`JoinHandle`](n0_future::task::JoinHandle) *detaches* on both targets, so a
//! repeating task whose handle was lost would run forever, hold whatever it
//! captured — a `Handle` clone, which keeps an actor alive — and be unreachable.
//! An actor is a thing you addressed and can address again; a schedule is not, so
//! it dies with its handle.
//!
//! Both constructors are `#[must_use]`, so ignoring the handle is a warning
//! rather than a silent no-op, and [`Scheduled::detach`] is the one way to say
//! "run without me" out loud.
//!
//! ## There is no `cancel` method, and its absence is a decision
//!
//! One shipped, briefly: `pub fn cancel(self)`, whose own doc conceded the
//! premise — *"The same thing `drop` does"* — and which had **zero** call sites
//! anywhere in the tree. It is deleted, and the reason is worth keeping so it is
//! not re-derived a third time.
//!
//! Issue #3's refused-designs table already held the row: *"`Timer::cancel`
//! beside 'dropping cancels it' is two ways to do one thing."* That is the whole
//! argument. A second spelling of one behaviour costs a reader the question
//! "which of these two does the thing I mean", costs the crate a permanent
//! compatibility promise once it is published, and buys a `drop(sweep)` that
//! reads marginally more deliberate than `drop(sweep)`.
//!
//! `drop` is the one way, and `a_dropped_schedule_runs_nothing` is the guard
//! behind it. [`detach`](Scheduled::detach) exists for the opposite reason: it
//! is the *only* way to get behaviour `drop` does not give.

use std::fmt;
use std::future::Future;

use n0_future::task::{self, JoinHandle};
use n0_future::time::{sleep, Duration};

/// A running schedule. Dropping it cancels the work.
///
/// Cancellation is a tokio abort, so it is cooperative: work already inside a
/// turn is dropped at its next await point, and work that never awaits runs to
/// completion. Same caveat as [`LocalActor::abort`](crate::LocalActor::abort),
/// same reason.
///
/// Not `Clone`: two holders would each cancel on drop, so the first drop would
/// cancel a schedule the second still believed in.
pub struct Scheduled {
    task: JoinHandle<()>,
    /// Set by [`detach`](Self::detach), read by `Drop`. A flag and not a
    /// consumed `JoinHandle`, because there is no way to take one out of an
    /// `AbortOnDropHandle` without aborting it — which is exactly the thing
    /// `detach` must not do.
    detached: bool,
}

impl Scheduled {
    /// Let the work outlive this handle.
    ///
    /// For a one-shot [`after`] that genuinely is fire-and-forget: nothing can
    /// cancel it afterwards and nothing can await it. On a repeating [`every`]
    /// this is a task that runs until the process ends, which is almost always a
    /// mistake — it is spelled out here so that it has to be typed.
    pub fn detach(mut self) {
        self.detached = true;
    }

    /// Whether the work has finished. Advisory, like
    /// [`is_running`](crate::LocalActor::is_running).
    ///
    /// Always `false` for an [`every`] that has not been cancelled: a repeating
    /// schedule has no end.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Spawn, in the one place both constructors share.
    fn spawning(work: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            task: task::spawn(work),
            detached: false,
        }
    }
}

impl Drop for Scheduled {
    fn drop(&mut self) {
        if !self.detached {
            self.task.abort();
        }
    }
}

impl fmt::Debug for Scheduled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduled")
            .field("finished", &self.task.is_finished())
            .field("detached", &self.detached)
            .finish()
    }
}

/// Run `work` once, `delay` from now.
///
/// The returned [`Scheduled`] cancels on drop, so a caller who does not hold it
/// gets nothing — see [`Scheduled::detach`] for the deliberate opposite.
///
/// # Panics
///
/// Panics when called outside a tokio runtime, like every other spawn.
#[must_use = "dropping the `Scheduled` cancels the work before it runs; hold it, \
              or call `detach` to say that is what you meant"]
pub fn after<F, Fut>(delay: Duration, work: F) -> Scheduled
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Scheduled::spawning(async move {
        sleep(delay).await;
        work().await;
    })
}

/// Run `work` every `period`, one run at a time, until the [`Scheduled`] is
/// dropped.
///
/// The first run is one `period` from now. A run is never re-entered and a missed
/// tick is never queued: see this module's backpressure section, which is the
/// reason this is a `sleep` in a loop rather than an
/// [`Interval`](n0_future::time::Interval).
///
/// # Panics
///
/// Panics when called outside a tokio runtime, like every other spawn. A panic
/// inside `work` ends the schedule — there is no reply channel to carry it and
/// nobody awaiting the task, so the panic hook prints at the fault and
/// [`is_finished`](Scheduled::is_finished) becomes `true`. A repeating schedule
/// that must survive its own work catches inside the work.
#[must_use = "dropping the `Scheduled` cancels the schedule; hold it, or call \
              `detach` to say that is what you meant"]
pub fn every<F, Fut>(period: Duration, mut work: F) -> Scheduled
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Scheduled::spawning(async move {
        loop {
            sleep(period).await;
            // Awaited, not spawned, and this is the line the module's whole
            // backpressure argument rests on: a spawn here would start a run per
            // period regardless of whether the last one finished, which is the
            // unbounded pile of pending work `interval`'s `Burst` produces.
            work().await;
        }
    })
}
