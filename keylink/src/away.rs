//! Slow work that leaves a turn, and comes back to the actor as a new one.
//!
//! # The problem this closes
//!
//! A turn always runs to the end, so a handler that spends an hour holds its actor
//! for an hour. [`Reply::later`](crate::Reply::later) is half of the way out: it
//! parks the caller's channel so the handler can return. But the half that comes
//! back was missing. Hand the [`Deferred`] to a spawned task and its `send`
//! crosses at once: no turn runs and no observer gates the answer, so a final
//! compare-and-swap cannot run inside a turn after the slow step. Built by hand,
//! with a feed, the same shape had four problems: the stream ending ends the actor,
//! a panic in the feed handler ends the actor, the spawned task is not owned by the
//! actor and outlives it, and a launching turn that was refused cannot stop the
//! work from starting.
//!
//! [`Deferred::away`] is that shape, owned. The loop keeps the work in a
//! `JoinSet`, starts it only after the launching turn is committed, and runs its
//! output as a new turn through [`Back`], with the caller's reply still pending.
//!
//! # Ambient, for `parking.rs`'s reason
//!
//! `Deferred::away` is called from inside a handler, and what it must reach is the
//! loop that is running that handler. A `Deferred` is parked in one turn and
//! launched in another, so it can carry no handle to that loop. The thing that is
//! always in scope is the running turn, so the loop exposes a lot for the duration
//! of each dispatch, exactly as `parking.rs` does for parked answers and `Chained`
//! does for the chain of actors. The `thread_local!` argument in `parking.rs`
//! applies unchanged: `tokio::task_local!` needs tokio's `rt` feature, which
//! keylink enables on no target.
//!
//! # What the lot holds, and the two ways it is misused
//!
//! The lot holds type-erased launches, because the type of the actor's state is a
//! generic of the loop and not of a thread-local. The loop downcasts them back with
//! the state type it knows. Two things can go wrong and both end the same way: the
//! work is dropped unstarted, and so is the reply it carried, so the caller is told
//! [`Withheld::Dropped`](crate::Withheld::Dropped).
//!
//! - **`away` outside any turn**, for example a unit test that builds the state by
//!   hand. There is no lot, so [`push`] drops the launch.
//! - **`away` with another state type than the running turn's**, for example
//!   `&mut self.sub`. The lot holds the launch, and the loop's downcast to its own
//!   state type refuses it.
//!
//! Neither is a compile error. That is the price of the ambient design, and the
//! first call of the method shows the mistake rather than a rare race.

use std::any::Any;
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::Deferred;

/// The turn that [`Deferred::away`] comes back to.
///
/// `out` is what the off-turn work produced. `reply` is the caller who is still
/// waiting. The method runs as an ordinary turn: the panic guard, the observers
/// and the commit all apply, and an answer sent here is gated by this turn's
/// commit.
///
/// Re-check here what the launching turn assumed. Other turns ran while the work
/// was away, so the state may be different from the one the work was started for.
/// A `back` that is not ready to answer can park the reply in the state, or send
/// it away again with [`Deferred::away`].
///
/// A panic in `back` costs this one message and not the actor, as for an `rpc`.
/// The caller is told [`Withheld::Dropped`](crate::Withheld::Dropped).
///
/// All of that holds whether `back` is an `async fn` or a plain `fn` that does
/// work before it returns a future. The loop calls it from inside the turn's guard
/// and the lot that parks its answers, so an eager `reply.send(..)` is still gated
/// by this turn's commit.
pub trait Back<W, T>: Send + 'static {
    /// Run against the state with the work's output and the parked reply.
    fn back(&mut self, out: W, reply: Deferred<T>) -> impl Future<Output = ()> + Send;
}

/// What an away step becomes when its work is done: a turn, to run on the state.
///
/// The same shape as the closure `observed_turn` takes, which is why a back turn
/// goes through it unchanged.
pub(crate) type Comeback<A> =
    Box<dyn for<'b> FnOnce(&'b mut A) -> Pin<Box<dyn Future<Output = ()> + Send + 'b>> + Send>;

/// One launch: the work, and what to run when it is done.
///
/// The `&'static str` is the method the caller's reply belongs to, so the back turn
/// is observed under the same name as the call that started it.
pub(crate) type Started<A> = Pin<Box<dyn Future<Output = (&'static str, Comeback<A>)> + Send>>;

/// Build the turn that runs `A::back`. A function and not a closure at the one call
/// site, because the signature has to be higher ranked and a closure that is
/// inferred does not get one.
///
/// # The call to `back` is inside the future, not beside it
///
/// `observed_turn` calls this closure **before** it builds the lot that parks a
/// turn's answers and before it enters the panic guard, so anything the closure
/// does itself runs outside both. [`Back::back`] is declared as a function that
/// returns a future, which lets an implementation do work before it returns one:
/// a plain `fn` that mutates the state and calls `reply.send(..)` would answer its
/// caller before the turn's observers had decided, an eager `reply.away(..)` would
/// launch work for a turn that is then refused, and a panic would unwind past the
/// guard and take the actor with it. So the closure builds nothing but an `async`
/// block, and `state.back(..)` is called when that block is first polled, which is
/// inside the lot and inside the guard, exactly where an `async fn` body would
/// run. The generated arms for the service's own methods do the same.
pub(crate) fn comeback<A, W, T>(out: W, reply: Deferred<T>) -> Comeback<A>
where
    A: Back<W, T>,
    W: Send + 'static,
    T: Send + 'static,
{
    Box::new(move |state: &mut A| Box::pin(async move { state.back(out, reply).await }))
}

thread_local! {
    /// The lot of the dispatch being polled on this thread, or `None` between
    /// polls. See `parking.rs`'s `PARKING`: this is its twin, set by
    /// [`Intaking::poll`] for as long as a dispatch is on the stack.
    static INTAKE: Cell<Option<Vec<Box<dyn Any + Send>>>> = const { Cell::new(None) };
}

/// Hand a launch to the dispatch that is running, or drop it if there is none.
///
/// Dropping is the misuse case in the header: it drops the work unpolled and the
/// reply with it. `try_with` and not `with`, for `parking::park`'s reason: a thread
/// whose locals are already destroyed is not running a dispatch, and a panic in
/// somebody else's destructor is not an improvement.
pub(crate) fn push<A: 'static>(started: Started<A>) {
    let mut carried = Some(started);
    let _ = INTAKE.try_with(|slot| {
        if let Some(mut lot) = slot.take() {
            if let Some(started) = carried.take() {
                lot.push(Box::new(started));
            }
            slot.set(Some(lot));
        }
    });
    drop(carried);
}

/// Swap this lot with whatever the current thread is holding. Its own inverse.
fn exchange(held: &mut Option<Vec<Box<dyn Any + Send>>>) {
    let taken = held.take();
    *held = INTAKE.with(|slot| slot.replace(taken));
}

/// A dispatch, with the lot its away launches go into.
///
/// Built by `run_loop` around the dispatch of every kind of turn, so "during the
/// turn" means the handler, the observer chain and the release of what the turn
/// parked: `observed_turn` releases a parked `Deferred::away` inside the poll, with
/// the turn's verdict, and that is the moment a launch is pushed.
pub(crate) struct Intaking<F> {
    /// The lot, except while this future is being polled: during a poll it is on
    /// the thread and this holds whatever was displaced there, normally `None`.
    lot: Option<Vec<Box<dyn Any + Send>>>,
    /// The dispatch.
    inner: F,
}

impl<F> Intaking<F> {
    /// Wrap a dispatch in an empty lot. `Vec::new` allocates nothing, so a turn
    /// that launches nothing pays one pointer-sized `Option` in the frame.
    pub(crate) fn new(inner: F) -> Self {
        Self {
            lot: Some(Vec::new()),
            inner,
        }
    }

    /// Everything the turn launched for state type `A`, in launch order.
    ///
    /// A launch for any other type is dropped here, unpolled. That is the second
    /// misuse in the header, refused at the one place that knows the state type.
    pub(crate) fn take<A: 'static>(&mut self) -> Vec<Started<A>> {
        self.lot
            .take()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|launch| match launch.downcast::<Started<A>>() {
                Ok(started) => Some(*started),
                Err(other) => {
                    drop(other);
                    None
                }
            })
            .collect()
    }
}

/// `Unpin` on the inner future rather than a projection, for `Parking`'s reason:
/// the one thing this wraps is the `Pin<Box<dyn Future>>` a dispatch returns, and
/// requiring it costs a bound instead of `unsafe`.
impl<F: Future + Unpin> Future for Intaking<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { lot, inner } = self.get_mut();
        exchange(lot);
        // A guard and not a line after the poll, because the poll can unwind and
        // one dispatch's lot must not be visible to the next thing polled here.
        let _restore = Restore(lot);
        Pin::new(inner).poll(cx)
    }
}

/// Puts the lot back where it came from, on the way out of a poll, including a
/// panicking one.
struct Restore<'a>(&'a mut Option<Vec<Box<dyn Any + Send>>>);

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        exchange(self.0);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::*;

    /// A launch whose work records whether it was polled and whether it was
    /// dropped. Its output is never reached by any test here, so the comeback is
    /// the cheapest one that type checks.
    struct Probe {
        polled: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl Probe {
        fn new() -> Self {
            Self {
                polled: Arc::new(AtomicBool::new(false)),
                dropped: Arc::new(AtomicBool::new(false)),
            }
        }

        fn launch<A: Send + 'static>(&self) -> Started<A> {
            let polled = Arc::clone(&self.polled);
            let sentinel = Sentinel(Arc::clone(&self.dropped));
            Box::pin(async move {
                let _held = sentinel;
                polled.store(true, Ordering::SeqCst);
                let nothing: Comeback<A> =
                    Box::new(|_state: &mut A| Box::pin(std::future::ready(())));
                ("probe", nothing)
            })
        }

        fn was_polled(&self) -> bool {
            self.polled.load(Ordering::SeqCst)
        }

        fn was_dropped(&self) -> bool {
            self.dropped.load(Ordering::SeqCst)
        }
    }

    struct Sentinel(Arc<AtomicBool>);

    impl Drop for Sentinel {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Outside a dispatch there is no lot, so the launch is dropped and its work is
    /// never polled.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/away.rs` on `push`'s
    /// `if let Some(mut lot) = slot.take() {` (indented eight spaces, because the
    /// record is in this file) and the replacement `if let Some(mut lot) =
    /// Some(Vec::<Box<dyn Any + Send>>::new()) {`, which keeps a launch no loop
    /// will ever collect. Watched: `the work was dropped, not kept`.
    #[test]
    fn a_push_outside_a_loop_drops_the_work_unpolled() {
        let probe = Probe::new();
        push::<u8>(probe.launch());
        assert!(probe.was_dropped(), "the work was dropped, not kept");
        assert!(!probe.was_polled(), "the work never ran");
    }

    /// Inside a dispatch the launch waits, and comes back for the state type it
    /// was made for.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/away.rs` on `push`'s
    /// `lot.push(Box::new(started));` (indented sixteen spaces) and the replacement
    /// `drop(started);`, which is the behaviour before this module existed: nothing
    /// is ever launched. Watched: `the launch is held for the loop`.
    #[tokio::test]
    async fn a_push_inside_a_poll_is_held_for_the_loop() {
        let probe = Probe::new();
        let mut intaking = Intaking::new(Box::pin(async {
            push::<u8>(probe.launch());
            tokio::task::yield_now().await;
        }));
        (&mut intaking).await;

        assert!(!probe.was_dropped(), "the launch is held for the loop");
        let held = intaking.take::<u8>();
        assert_eq!(held.len(), 1);
        assert!(!probe.was_polled(), "holding it does not start it");
    }

    /// A launch made for another state type is refused where the loop collects it,
    /// and dropped unpolled.
    ///
    /// That a wrong type is not accepted is `Any`'s, and nothing in this crate can
    /// weaken it. What is the crate's is the other half, that the refused launch is
    /// dropped and not kept. Made red by `scripts/break-once.sh
    /// keylink/src/away.rs` on `take`'s `drop(other);` (indented twenty spaces) and
    /// the replacement `std::mem::forget(other);`. Watched: `the work was dropped`.
    #[tokio::test]
    async fn a_push_for_another_state_type_drops_the_work() {
        let probe = Probe::new();
        let mut intaking = Intaking::new(Box::pin(async {
            push::<u8>(probe.launch());
        }));
        (&mut intaking).await;

        let held = intaking.take::<String>();
        assert!(
            held.is_empty(),
            "a String-state loop does not take a u8 launch"
        );
        assert!(probe.was_dropped(), "the work was dropped");
        assert!(!probe.was_polled(), "the work never ran");
    }

    /// The thread is left as it was found, so the dispatch after a panicking one
    /// does not inherit its lot.
    ///
    /// This is the one the `Restore` guard exists for, and a restore written as a
    /// line after the poll would pass every other test in this module. Made red by
    /// `scripts/break-once.sh keylink/src/away.rs` on `Restore::drop`'s
    /// `exchange(self.0);` (indented eight spaces) and the replacement `let _ =
    /// &self.0;`. Watched: `a lot left on the thread would have kept this`.
    #[test]
    fn a_panicking_poll_still_restores_the_intake() {
        let mut intaking = Intaking::new(Box::pin(async { panic!("in the handler") }));
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut cx = Context::from_waker(std::task::Waker::noop());
            Pin::new(&mut intaking).poll(&mut cx)
        }));
        assert!(caught.is_err(), "the panic is the point");

        let probe = Probe::new();
        push::<u8>(probe.launch());
        assert!(
            probe.was_dropped(),
            "a lot left on the thread would have kept this"
        );
    }
}
