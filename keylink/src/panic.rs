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
use std::task::Poll;

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
        let message = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "panicked".to_string());
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
pub(crate) async fn guarded<F: Future>(fut: F) -> std::result::Result<F::Output, Panic> {
    let mut fut = Box::pin(fut);
    std::future::poll_fn(move |cx| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(Panic::caught(panic))),
        }
    })
    .await
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
