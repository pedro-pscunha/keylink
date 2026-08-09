//! An answer that waits for the verdict of the turn that produced it.
//!
//! # The window this closes
//!
//! [`Deferred::send`](crate::Deferred) used to write to the reply channel from
//! inside the running handler. Its own documentation said so — *"nothing is owed
//! to the turn running now: the value crosses the channel directly, and this
//! turn's observers neither see it nor gate it"* — and that sentence is the whole
//! of the defect. The order for a parked reply was: handler mutates the state →
//! `deferred.send(receipt)` → **the caller has its answer** → handler returns →
//! the observer chain runs → `Persist` spends ~7.6 ms on two `fsync`s. A crash
//! anywhere in that window loses state a caller was already holding a receipt
//! for, and "an answered call is a stored call" was false for every `#[defer]`
//! method that answered from a later turn.
//!
//! So the send is parked as well as the reply. A value handed to
//! `Deferred::send` during a turn is held here until that turn's observer chain
//! has accepted it, and released with the same [`Verdict`] the turn's own caller
//! is answered by. `#[defer]`'s point is untouched: the reply still outlives the
//! turn that **received** it. What it no longer does is outrun the turn that
//! **answered** it.
//!
//! # "Park" is the same word twice, on purpose
//!
//! [`Reply::later`](crate::Reply::later) parks a *channel* past its turn; this
//! parks a *value* until its turn commits. Both are an answer that waits, and
//! RFC-07 §6.8 named this fix in exactly those terms — *"park the send as well as
//! the reply"* — so the vocabulary is the one the decision was written in.
//!
//! # The lot belongs to the turn, and that is what answers three questions
//!
//! A [`Parking`] future is built by `observed_turn` around the handler and
//! nothing else, and it holds the lot itself. Three consequences, none of them a
//! policy:
//!
//! - **Nothing is parked at shutdown.** The lot is a local of the turn, so it
//!   cannot outlive one. `run_loop` reaches `finish` with no held value to decide
//!   about, and that is by construction rather than by a drain nobody would have
//!   watched fail.
//! - **A cancelled turn answers `Gone`.** `abort()` drops the loop's future at
//!   its next await, which drops the lot, which drops the reply channels in it.
//!   The turn never committed, so nothing in it should cross.
//! - **A send from off the turn's task is answered immediately**, because there
//!   is no lot in scope to hold it. That is the residual limit and it is the
//!   honest one: a `Deferred` handed to a spawned task, or reached from a
//!   [`read`](crate::LocalActor::read) closure through interior mutability, is
//!   answering from outside any turn — nothing ran, no observer saw anything,
//!   and there is no commit for that answer to wait for. `Persist`'s module
//!   header states it as the one thing left.
//!
//! # Ambient, and why the alternatives are worse
//!
//! A `Deferred` is parked in turn 1 and sent in turn 5, so it cannot carry a
//! handle to the lot it will need: the lot it must join is the one belonging to
//! whichever turn is running when `send` is called, and that turn does not exist
//! yet when the channel is parked. The two ways to reach it are a handle the
//! actor owns and arms per turn, or the running turn itself.
//!
//! An actor-owned lot with an "a turn is running" flag loses to a race it cannot
//! win: a `Deferred` sent from another task while any turn happens to be running
//! would be attached to a turn it has nothing to do with, and refused with it.
//! Scoping the lot to the turn's own future has no such case — another task is
//! definitionally outside it — so the rule is decidable rather than timing
//! dependent.
//!
//! It is a thread-local rather than `tokio::task_local!` because that macro lives
//! behind tokio's `rt` feature, which keylink does not enable on any target: the
//! browser build has `io-util` and `sync` and nothing else. The save/restore
//! below is what `task_local!` does anyway, and it follows a task across worker
//! threads for the same reason — the exchange happens on whichever thread is
//! **polling**, not on whichever thread started the turn.
//!
//! # `thread_local!` against the "no `std::thread`" invariant, said rather than hoped
//!
//! `thread_local!` expands to a [`std::thread::LocalKey`], so it names that
//! module. It does not do the thing the invariant is about: no OS thread is
//! created, nothing is joined, and there is no `spawn`. The invariant exists
//! because `std::thread` and `std::time::Instant` **panic or fail to build** on
//! `wasm32-unknown-unknown`, and `LocalKey` does neither — `.mise/tasks/wasm`
//! builds this module for that target, twice, with and without `persist`.
//!
//! There is no `n0_future` equivalent to reach for instead, and the alternative
//! that avoids the module entirely is a raw pointer in a `Cell`, which needs
//! `unsafe` in a crate that denies it.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// One answer, waiting for a verdict.
///
/// Type-erased, because one turn can park answers of several types at once — a
/// `Deferred<Receipt>` and a `Deferred<u64>` released by the same handler — and
/// a lot that named the type could hold only one of them. What is erased is the
/// send and not the channel: the closure owns the [`Reply`](crate::Reply) and the
/// value, and the only thing it can do with them is what the verdict says.
pub(crate) type Parked = Box<dyn FnOnce(Verdict) + Send>;

/// What the observer chain decided about the turn that parked an answer.
///
/// The same four cases `turn` folds into an `Answered` for the turn's own
/// caller, read from the same pair of facts in the same place — so a parked
/// caller and the turn's own caller cannot be told two different things about one
/// turn.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Verdict {
    /// Every observer accepted the turn. The value crosses.
    Committed,
    /// An observer refused it. The turn **ran and was not committed**, so a value
    /// it produced must not reach anybody: the parked caller is told
    /// [`TurnLost::Refused`](crate::TurnLost::Refused) naming the same observer
    /// the turn's own caller is told about.
    ///
    /// This is the mirror of the fault that took `Persist::finish` out — a caller
    /// told "not committed" about state that *was* committed — and it points the
    /// other way for the same reason. The refusal ends the actor, so the state
    /// this value was computed from is dropped rather than stored, and "it ran
    /// and was not committed" is exactly what happened to the work behind this
    /// answer.
    Refused {
        /// [`Observer::name`](crate::Observer::name).
        name: &'static str,
    },
    /// An observer **panicked** watching it. The turn ran and was not committed
    /// for the same reason [`Refused`](Self::Refused) states, so the parked caller
    /// is told
    /// [`TurnLost::ObserverPanicked`](crate::TurnLost::ObserverPanicked) naming
    /// the same observer the turn's own caller is told about.
    ///
    /// **Withholding is right here and the word *refused* was not.** A veto means
    /// *do not commit*; a crash means nobody knows whether the observer committed,
    /// which RFC-06 §5.1 argues is the worse of the two. So this arm and
    /// [`Refused`](Self::Refused) do the same thing to the value and owe the
    /// caller different sentences, which is why they are two arms and not one with
    /// a flag.
    ObserverPanicked {
        /// [`Observer::name`](crate::Observer::name).
        name: &'static str,
    },
    /// The handler panicked. The reply channel is dropped, so the parked caller
    /// is told [`TurnLost::Dropped`](crate::TurnLost::Dropped).
    ///
    /// **Not `Faulted`, and it is not a near miss.** There is one
    /// [`Panic`](crate::Panic), it is the original payload rather than a message,
    /// and `resume_unwind` on the original payload is the difference between
    /// forwarding a panic and starting a second one. It belongs to the turn's own
    /// caller, who gets it. A payload cannot be cloned to tell two, and
    /// manufacturing a second one from a string is the thing `panic.rs` refuses.
    ///
    /// **And not `Gone` either**, which is what this arm produced while a dropped
    /// channel had one answer. The loop caught the panic, so the actor is serving
    /// the next message while this caller reads the answer — and `Dropped` is the
    /// same answer [`Deferred`](crate::Deferred) documents for a turn that takes a
    /// parked reply out of the state and then panics. A durability adapter refuses
    /// a panicked turn, so there was nothing committed for the value to be an
    /// answer about; that is why nothing crosses, and it says nothing about the
    /// actor.
    Faulted,
}

thread_local! {
    /// The lot of the turn being polled on this thread, or `None` between polls.
    ///
    /// Exchanged in and out by [`Parking::poll`], so it is set for exactly as
    /// long as a turn's handler is on the stack. Nested exchanges restore what
    /// they displaced, which costs nothing and means a runtime re-entered from
    /// inside a handler cannot cross two actors' lots.
    static PARKING: Cell<Option<Vec<Parked>>> = const { Cell::new(None) };
}

/// Hold this answer for the running turn, or give it now if there is none.
///
/// The `None` branch is the residual limit this module's header names, and it is
/// deliberately not a refusal: a send from outside every turn is answering with
/// something no turn produced, so there is no commit to wait for and withholding
/// it would hang a caller forever.
pub(crate) fn park(answer: Parked) {
    let mut carried = Some(answer);
    // `try_with` and not `with`. A thread whose locals have already been
    // destroyed is not running a turn, which is the `None` case exactly, and
    // raising a panic inside somebody else's destructor is not an improvement on
    // answering immediately.
    let _ = PARKING.try_with(|slot| {
        if let Some(mut lot) = slot.take() {
            // Always `Some`: this closure runs at most once. Spelled as a move
            // out of the capture because that is what pushing the box requires.
            if let Some(answer) = carried.take() {
                lot.push(answer);
            }
            slot.set(Some(lot));
        }
    });
    if let Some(answer) = carried {
        answer(Verdict::Committed);
    }
}

/// Swap this lot with whatever the current thread is holding.
///
/// Its own inverse, which is why entering and leaving a poll are the same call.
///
/// `with` rather than `try_with`, and the argument is that the only caller is
/// `Parking::poll`: a thread cannot be running a future's `poll` and destroying
/// its locals at the same time. If that were ever wrong it would be a panic
/// inside a turn, which `guarded` catches and reports as a faulted turn — loud,
/// which is the direction this crate errs in.
fn exchange(held: &mut Option<Vec<Parked>>) {
    let taken = held.take();
    *held = PARKING.with(|slot| slot.replace(taken));
}

/// A turn's handler, with the lot its parked answers go into.
///
/// Built by `observed_turn` around the handler and nothing else, so "during the
/// turn" means what a reader would guess: the handler's own body, including
/// everything it awaits inline. An observer's `after_turn` runs outside it, which
/// is right — an observer that answered a caller would be a second reply path
/// with no verdict of its own.
pub(crate) struct Parking<F> {
    /// The lot, except while this future is being polled — during a poll it is on
    /// the thread and this holds whatever was displaced there, normally `None`.
    lot: Option<Vec<Parked>>,
    /// The handler.
    inner: F,
}

impl<F> Parking<F> {
    /// Wrap a handler in an empty lot. `Vec::new` allocates nothing, so a turn
    /// that parks no answer pays one pointer-sized `Option` in the frame.
    pub(crate) fn new(inner: F) -> Self {
        Self {
            lot: Some(Vec::new()),
            inner,
        }
    }

    /// Everything the turn parked, in the order it was sent.
    ///
    /// Taken after the handler is finished and **before** the observer chain
    /// borrows the state, so the released answers outlive the borrow the handler
    /// held: a `Vec<Parked>` names no lifetime.
    ///
    /// `unwrap_or_default` and not an `unwrap`: the `None` case is "this was
    /// called from inside a poll", which no caller can do, and an empty lot is
    /// the same thing as no lot to everything downstream.
    pub(crate) fn take(&mut self) -> Vec<Parked> {
        self.lot.take().unwrap_or_default()
    }
}

/// `Unpin` on the inner future rather than a projection, because the one thing
/// this ever wraps is the `Pin<Box<dyn Future>>` a turn's closure returns — which
/// is `Unpin` — and requiring it costs a bound instead of `unsafe`. `lib.rs`
/// denies `unsafe_code`.
impl<F: Future + Unpin> Future for Parking<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { lot, inner } = self.get_mut();
        exchange(lot);
        // A guard and not a line after the poll, because the poll can **unwind**:
        // a panicking handler is the case this crate is arranged around, and a
        // restore that only ran on the happy path would leave one turn's lot
        // visible to the next thing polled on this thread.
        let _restore = Restore(lot);
        Pin::new(inner).poll(cx)
    }
}

/// Puts the lot back where it came from, on the way out of a poll — including the
/// way out of a panicking one.
struct Restore<'a>(&'a mut Option<Vec<Parked>>);

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        exchange(self.0);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::sync::mpsc;

    use super::*;

    /// A `Parked` that reports which answer it is and the verdict it was released
    /// with. Tagged, because the order two of them come back in is one of the
    /// things being asserted.
    fn recording(tag: u8, to: &mpsc::Sender<(u8, Verdict)>) -> Parked {
        let to = to.clone();
        Box::new(move |verdict| {
            let _ = to.send((tag, verdict));
        })
    }

    /// Outside a turn there is no lot, so the answer is given immediately. This
    /// is the residual limit, pinned here as well as in `tests/defer.rs` so that
    /// removing it cannot be an accident.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/parking.rs` on `park`'s
    /// `if let Some(answer) = carried {` → `if let Some(answer) =
    /// Option::<Parked>::None {`, which swallows an off-turn answer. Watched:
    /// `assertion failed: matches!(from.try_recv(), Ok((1, Verdict::Committed)))`.
    #[test]
    fn a_park_outside_a_turn_answers_now() {
        let (to, from) = mpsc::channel();
        park(recording(1, &to));
        assert!(matches!(from.try_recv(), Ok((1, Verdict::Committed))));
    }

    /// Inside one it waits, and the lot comes back **in send order**.
    ///
    /// The order is asserted here and deliberately nowhere else: through two
    /// spawned callers it would be a statement about tokio's scheduler rather than
    /// about the release, and a guard that can pass for the wrong reason is worse
    /// than none. What the loop adds on top of this is one call order in
    /// `observed_turn` — the lot, then the turn's own reply — which is straight
    /// line code in one function.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/parking.rs` on `park`'s
    /// `if let Some(mut lot) = slot.take() {` → `if let Some(mut lot) =
    /// Option::<Vec<Parked>>::None {`, which is the pre-`parking.rs` behaviour
    /// exactly: every send escapes to the immediate branch. Watched: `nothing
    /// crossed during the turn`.
    #[tokio::test]
    async fn a_park_inside_a_turn_waits_for_the_verdict() {
        let (to, from) = mpsc::channel();
        let first = recording(1, &to);
        let second = recording(2, &to);

        let mut parking = Parking::new(Box::pin(async move {
            park(first);
            // An await inside the handler, so the restore/exchange pair is
            // exercised across more than one poll.
            tokio::task::yield_now().await;
            park(second);
        }));
        (&mut parking).await;

        assert!(from.try_recv().is_err(), "nothing crossed during the turn");

        let parked = parking.take();
        assert_eq!(parked.len(), 2);
        for release in parked {
            release(Verdict::Refused { name: "persist" });
        }
        assert!(matches!(
            from.try_recv(),
            Ok((1, Verdict::Refused { name: "persist" }))
        ));
        assert!(matches!(
            from.try_recv(),
            Ok((2, Verdict::Refused { name: "persist" }))
        ));
    }

    /// The thread is left as it was found, so the turn after a panicking one
    /// does not inherit its lot.
    ///
    /// This is the one the `Restore` guard exists for, and a restore written as a
    /// line after the poll would pass every other test in this module. Made red by
    /// `scripts/break-once.sh keylink/src/parking.rs` replacing `Restore::drop`'s
    /// `exchange(self.0);` with a comment. Watched: `a lot left on the thread
    /// would have swallowed this`.
    #[test]
    fn a_panicking_poll_still_restores_the_thread() {
        let mut parking = Parking::new(Box::pin(async { panic!("in the handler") }));
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // One poll by hand: the handler panics before its first await, so
            // there is nothing to drive and no runtime to need.
            let mut cx = Context::from_waker(std::task::Waker::noop());
            Pin::new(&mut parking).poll(&mut cx)
        }));
        assert!(caught.is_err(), "the panic is the point");

        let (to, from) = mpsc::channel();
        park(recording(1, &to));
        assert!(
            matches!(from.try_recv(), Ok((1, Verdict::Committed))),
            "a lot left on the thread would have swallowed this"
        );
    }
}
