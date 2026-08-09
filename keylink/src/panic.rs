//! Where a panic in user code stops being an unwind and starts being a value.
//!
//! A turn runs code nobody in this crate wrote against state nobody in this
//! crate owns. An unwind that escapes that call takes the loop with it, and a
//! dead loop is indistinguishable from an actor that was never mounted — so
//! catching is what keeps every "the actor is gone" answer honest.
//!
//! # A primitive of the turn model, not of the actor system
//!
//! This lived in `actor.rs`, now `actor/remote.rs`, which owns the wire wrapper
//! and the walk to a mailbox. That was an inversion:
//! `local.rs` has no transport in it at all and still had to depend on the file
//! that owns the wire format in order to run a turn safely. The keyed, networked
//! system is one **consumer** of this, beside the local handle, the generated
//! code and the observer chain. Nothing here knows what a peer is.

use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A panic that was caught instead of killing an actor.
///
/// Carries the original payload, not a rendering of it, so that a **local**
/// caller can be given the panic it would have got from an ordinary function
/// call — see [`Panic::resume`] and `local.rs`. A remote caller gets
/// [`Display`](fmt::Display) inside [`RemoteError::Failed`], because an unwind
/// does not cross a network.
///
/// Holding the payload costs one thing worth naming: `Box<dyn Any + Send>` is
/// not `Sync`, so `Panic` is `Send` and no longer `Sync`. Nothing in keylink
/// needs it to be — a `Result<T, Panic>` crosses a `oneshot`, which wants
/// `Send` — but a downstream type that stored a `Panic` and asserted `Sync`
/// would stop compiling.
///
/// [`RemoteError::Failed`]: crate::RemoteError::Failed
pub struct Panic {
    message: String,
    /// The `Box` `catch_unwind` handed back. Kept whole: `resume_unwind` on the
    /// original payload is the difference between forwarding a panic and
    /// starting a second one, which would run the panic hook twice and print
    /// two backtraces for one fault.
    payload: Box<dyn std::any::Any + Send>,
}

impl Panic {
    /// Build one from what `catch_unwind` handed back.
    ///
    /// **`pub(crate)`, and that is the whole point of it.** This was a public
    /// `From<Box<dyn Any + Send>>`, which made the type prove nothing: any
    /// caller could write `Panic::from(Box::new(0u8))` and then [`resume`] it,
    /// unwinding with a payload no handler ever produced, under a message
    /// reading "the actor panicked". A type named for a fact has to be
    /// constructible only where the fact is known.
    ///
    /// [`resume`]: Self::resume
    pub(crate) fn caught(payload: Box<dyn std::any::Any + Send>) -> Self {
        let message = message_of(&*payload);
        Self {
            message: format!("the actor panicked: {message}"),
            payload,
        }
    }

    /// What the handler panicked with.
    ///
    /// An accessor rather than [`Display`](fmt::Display), because `Display`
    /// cannot lend a borrow and an [`Observer`](crate::Observer) must be handed
    /// the message without taking the payload — the payload belongs to the
    /// caller that will resume it.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// What the panicking code said, without the framing [`message`] puts on it.
    ///
    /// [`message`] answers *"the actor panicked: …"*, which is true where a turn
    /// ran caller code and false where something else did — `compose::Group`
    /// catches a panicking *ending future*, and an actor that is running
    /// perfectly well must not be reported as having panicked. Reads the payload
    /// this already holds rather than storing a second copy of the text.
    ///
    /// [`message`]: Self::message
    pub(crate) fn bare(&self) -> String {
        message_of(&*self.payload)
    }

    /// Panic in the calling task, with the payload the handler panicked with.
    ///
    /// Does not run the panic hook a second time: the hook already ran where the
    /// panic happened, which is where the useful backtrace is. The cost of that
    /// is a caller-side panic with no location of its own, which is the right
    /// trade — one backtrace at the fault beats two at the wrong place.
    pub fn resume(self) -> ! {
        std::panic::resume_unwind(self.payload)
    }
}

impl fmt::Display for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

/// Names the message. Never the payload, which is `dyn Any` and has no `Debug`.
impl fmt::Debug for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Panic")
            .field("message", &self.message)
            .finish()
    }
}

/// What a caught payload panicked with, as text.
///
/// The two downcasts are the two shapes `panic!` produces, and the fallback is
/// for the third: a payload of some other type, which `panic_any` can make and
/// which has no text at all.
///
/// **Separate from [`Panic::caught`] because the wording is not shared.**
/// `caught` wraps this in *"the actor panicked: …"*, which is true where a turn
/// ran caller code and false everywhere else — `compose::Group` catches a
/// panicking *policy*, which is neither an actor nor a turn, and needs the bare
/// message. The extraction is the part both want; the sentence around it is not.
/// This exists because that second caller had copied these five lines verbatim.
pub(crate) fn message_of(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|panicked| (*panicked).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panicked".to_string())
}

/// Run one future, turning a panic into a value instead of a dead actor.
///
/// A panicking handler must cost one message, not the whole actor. Without
/// this, one bad message drops the receiver, every later send fails, and the
/// actor is unreachable for the life of the process while the registry still
/// lists it as mounted — so callers would be told `NoSuchActor` about an id
/// that is, as far as anyone configured it, very much real.
///
/// Two things it does not fix, both by design. The state may be **logically
/// torn**, which is the same hazard as a poisoned `Mutex` and is recovered from
/// for the same reason. And `panic = "abort"` defeats it entirely.
///
/// # `pub(crate)`, and it was `#[doc(hidden)] pub`
///
/// Generated code never named it. Until Wave 1b it did not have to — the turn was
/// a boxed closure keylink built, and `guarded` was already inside it — and Wave
/// 1b's mailbox enum removed the last reason it might have: every generated arm
/// calls [`turn`](crate::turn), and the guard is inside *that*. So
/// `rg '\$crate::guarded' keylink/src/macros.rs` finds nothing, and neither do
/// `tests/`, `demo/` or the README. A `pub` nothing outside needs is a
/// compatibility promise made by accident.
///
/// A plain `fn` returning [`Guarded`] rather than an `async fn`, so that the
/// future it hands back has a name a caller can write down. [`Guarded`] says
/// which caller needs that and what the alternative cost.
pub(crate) fn guarded<F: Future>(fut: F) -> Guarded<F> {
    Guarded { fut: Box::pin(fut) }
}

/// The future [`guarded`] returns: one panic-catching poll around a boxed future.
///
/// # Named rather than opaque, so a collection of them can be annotated
///
/// This was an `async fn`, whose return type no caller can spell. That costs
/// nothing until something has to *name* it, and one caller does.
/// `compose::Group`'s `retire_all` pushes every guarded ending into a
/// `FuturesUnordered`, and the item type of that set is inferred from the single
/// `retiring.push(guarded(fut))` line and from nothing else. So the set's type
/// is held up by the very line a guard has to replace. The break behind
/// `tests/compose.rs`'s `no_members_retirement_waits_on_another` swaps that push
/// for an `await`, which leaves the set with no inference source at all: the
/// edit fails to compile with `E0282` instead of reddening the test, and a guard
/// that cannot be expressed measures nothing.
///
/// A name fixes it at the collection rather than at the push:
/// `FuturesUnordered<Guarded<Retiring>>` gives the set its item type up front,
/// and the push line is then free to be replaced by anything.
///
/// **The refused alternative was to annotate the set with the opaque type.** It
/// cannot be named, so the only annotation available is
/// `FuturesUnordered<Pin<Box<dyn Future<Output = Result<(), Panic>> + Send>>>`,
/// which needs a `Box::pin` at the push site. Three breaks in `tests/compose.rs`
/// anchor on that push line verbatim, and a literal that no longer occurs aborts
/// `scripts/break-once.sh` — so rewriting it re-quotes all three. One of them is
/// then no longer expressible either: the break that pushes the caller's future
/// *unguarded* pushes a `Retiring`, whose output is `()`, into a set that now
/// demands `Result<(), Panic>`. It stops compiling rather than failing an
/// assertion, which says nothing about teardown. One struct here moves no anchor
/// and disarms no break.
///
/// # Boxed, and that is what keeps it free of `unsafe`
///
/// The field is a `Pin<Box<F>>` and not an `F`. Polling an `F` in place needs a
/// pin projection, and a hand-written projection needs `unsafe`, which
/// `lib.rs`'s `#![deny(unsafe_code)]` lifts exactly once, by name, for something
/// else. The box carries the pinning instead, and makes `Guarded<F>` [`Unpin`]
/// for every `F`, so [`Pin::get_mut`] is enough to reach the field —
/// `compose::keyed`'s `Awaited` is polled the same way for the same reason.
///
/// **Backticks and not an intra-doc link, and that is a repair.** This read
/// ``[`Watch`](crate::Watch)`` and meant the private future in
/// `compose/keyed.rs`. `crate::Watch` is `watch.rs`'s connection-departure
/// future, which is a different type polled with `get_mut` for a different
/// reason — it boxes because iroh hands back an unnameable `impl Future`. The
/// link was **valid**, so `mise run docs` stayed green and no gate in this repo
/// could ever have caught it. `CONTRIBUTING.md` says to reach for backticks when
/// a name is ambiguous, and two futures called `Watch` is that. The private one
/// is now `Awaited`, so the ambiguity is gone as well as the link.
///
/// It is the allocation the `async fn` body already made. What moved is when:
/// the box is built when [`guarded`] is called rather than at the first poll.
/// Nothing runs in between — the inner future is not polled either way — so no
/// caller can observe the difference.
#[must_use = "a guarded future does nothing unless you `.await` or poll it"]
pub(crate) struct Guarded<F> {
    /// The caller's future, owned and pinned. Kept across a caught panic rather
    /// than dropped, because a `Panic` is reported as `Ready` and a future is
    /// not polled after that.
    fut: Pin<Box<F>>,
}

impl<F: Future> Future for Guarded<F> {
    type Output = std::result::Result<F::Output, Panic>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = &mut self.get_mut().fut;
        match std::panic::catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            // Pending is passed through and the future is kept, not abandoned:
            // a guard around a future must not change how many times it is
            // polled. `compose::Group` holds these in a set that polls each of
            // them to completion, and an ending dropped at its first `Pending`
            // would report a member as retired that never was.
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(Panic::caught(panic))),
        }
    }
}

/// Run one **synchronous** step, turning a panic into a value instead of a dead
/// actor.
///
/// [`guarded`] covers a future, and a turn is not the only caller code an actor
/// task runs. The keyed loop decodes the request and encodes the reply on that
/// same task, and both execute a `Deserialize` and a `Serialize` the service
/// author wrote — hand-written ones at that, since a *derived* `Serialize`
/// cannot fail and a test that needs one to fail has to write it out. A panic
/// in either lands one line outside `guarded` and produces, exactly, the failure
/// `guarded`'s own documentation above says it prevents.
///
/// Synchronous on purpose rather than an `async` wrapper: neither call is a
/// future, and wrapping them in one would put a borrow of the state across an
/// await for no reason — the same trap `local.rs`'s observer catch avoids.
///
/// `pub(crate)` for the reason [`guarded`] is: no expansion names it, and the
/// keyed decode/encode sites it was written for are keylink's own.
pub(crate) fn catching<T>(work: impl FnOnce() -> T) -> std::result::Result<T, Panic> {
    std::panic::catch_unwind(AssertUnwindSafe(work)).map_err(Panic::caught)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn guarded_turns_a_panic_into_a_value() {
        let caught = guarded(async { panic!("bad message") }).await;
        let error = caught.expect_err("the panic is caught");
        assert!(error.to_string().contains("bad message"), "{error}");
    }

    #[tokio::test]
    async fn guarded_is_transparent_when_nothing_panics() {
        assert_eq!(guarded(async { 7u8 }).await.expect("no panic"), 7);
    }
}
