//! Watching a turn against the state, from outside the core.
//!
//! keylink has two extension points, and one sentence tells a maintainer which
//! to reach for:
//!
//! > [`Middleware`](crate::Middleware) intercepts a **message on the wire**.
//! > [`Observer`] watches a **turn against the state**.
//!
//! An `Observer` never sees a request or a reply payload. A `Middleware` never
//! sees the state. The two do not overlap, and neither grows toward the other.
//!
//! # What it is for
//!
//! Durability, first: something inside the turn loop that knows a turn happened,
//! so a state can be stored after every turn and before the reply without
//! keylink learning what storage is. `keylink-persist` is the port for the
//! bytes; this is the port for the moment.
//!
//! It has **one** consumer today. The argument for making it generic is the
//! boundary rule above, not a queue of waiting adapters — RFC-06 §0 says so
//! plainly, having withdrawn a second consumer it had claimed.
//!
//! # The actor owns its observers
//!
//! [`Observer::after_turn`] takes `&mut self`, and the loop owns the `Vec`.
//! A shared observer was the first design and it does not work:
//! `keylink_persist::Claim` is `Send + 'static` and **not** `Sync`, and
//! `Claim::store` takes `&mut self` on purpose. Reaching it from a `&self`
//! observer needs an async mutex around the claim.
//!
//! Owning it buys no mutex and no `Sync` bound anywhere, and it means the claim
//! dies with the actor — before [`stop`](crate::LocalActor::stop) returns, which
//! is a property the loop makes true rather than merely likely. It costs the
//! ability to share one observer across two actors, which does not arise: a
//! claim is one state, and a metrics observer holds its registry internally.
//!
//! # Read-only, stated narrowly
//!
//! `after_turn` takes `&A` and never `&mut A`, so no observer can call a
//! `service!` method or assign to a field. That is the whole guarantee. `&A`
//! does **not** stop interior mutability: a state with an `AtomicU64`, a `Mutex`
//! or a `RefCell` field is mutable through it, exactly as
//! `Handle::with_state` already is. `representability.rs` holds the compile-time
//! probe for the part that is true.
//!
//! # Not for the keyed system
//!
//! A keyed actor mounted in a [`System`](crate::System) gets no observers. That
//! is a scope limit rather than an oversight, and it is written here, on
//! [`Observer`], and beside `spawn`, because a user who mounts a keyed actor and
//! expects durability would otherwise get no durability, no compile error and no
//! explanation. RFC-06 §5.3.
//!
//! **The reason changed and shrank.** It used to be that `spawn` ran a second
//! loop over its own dispatcher and never touched
//! [`LocalActor`](crate::LocalActor), which is where this chain lived — two
//! loops, and closing the gap meant merging them. There is one dispatch path
//! now: `spawn`'s loop runs the same generated mailbox a local actor does and
//! hands it an **empty** hook slice. So what is left is the slice, and the
//! question of who owns the `Vec` for a lazily spawned actor — which is Wave 3's
//! to answer, because a feed asks it about the same loop.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The future an observer returns.
///
/// Concrete rather than an `impl Trait`, because `middleware.rs`'s `BoxFut` is
/// `pub(crate)`, takes no lifetime and is `'static` — it cannot hold `&'a A`,
/// and widening it would move the public [`Next`](crate::Next) type.
pub type TurnFut<'a> = Pin<Box<dyn Future<Output = Result<(), Stop>> + Send + 'a>>;

/// Something the actor owns that watches its turns.
///
/// `Send` and not `Sync`: the actor owns it, so nothing is shared.
///
/// Observers run in the order they were given, one after another, inside the
/// same panic guard as the handler. A keyed actor mounted through
/// [`System`](crate::System) has none — see the module documentation.
pub trait Observer<A>: Send + 'static {
    /// Names this observer in [`Ended`] and in the refusal a peer sees.
    fn name(&self) -> &'static str;

    /// Called inside the turn, after the handler and **before the reply**.
    ///
    /// Returning `Err` ends the actor, and the first `Err` stops the rest.
    ///
    /// A plain `fn` and not an `async fn`, so an implementation can do
    /// synchronous work with `state` **before** it builds the future. One that
    /// holds `state` across an await needs `A: Sync`; one that does not, does
    /// not — and keeping that choice with the implementor is what keeps
    /// `A: Sync` out of this design entirely.
    fn after_turn<'a>(&'a mut self, state: &'a A, turn: TurnInfo<'a>) -> TurnFut<'a>;
}

/// What the turn did.
///
/// `#[non_exhaustive]` so a later field is additive. It deliberately carries
/// **no** sequence number and **no** method name: a `seq` "counting from zero
/// per actor" is wrong wherever it could be built, because the turn closure is
/// created on the *caller's* task, on any of N cloned handles.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct TurnInfo<'a> {
    /// How the turn finished.
    pub outcome: TurnOutcome<'a>,
}

/// How a turn finished.
#[derive(Debug, Clone, Copy)]
pub enum TurnOutcome<'a> {
    /// The handler returned.
    Completed,
    /// The handler panicked. The **message**, and never the payload.
    ///
    /// `&'a str` here is load-bearing rather than cosmetic. [`Panic`] holds a
    /// `Box<dyn Any + Send>`, so it is `Send` and not `Sync`, so `&Panic` is not
    /// `Send` — and the loop holds a `TurnInfo` across the observer's await
    /// inside a future that must be `Send`. Carrying `&'a Panic` does not
    /// compile, and two revisions of this design believed it did because their
    /// probe used a unit stand-in for `Panic`, which *is* `Sync`.
    ///
    /// Withholding the payload is right on its own terms too: `ask` resumes it
    /// in the caller, and [`Panic::caught`] exists to stop one being resumed
    /// twice.
    ///
    /// [`Panic`]: crate::Panic
    /// [`Panic::caught`]: crate::Panic
    Panicked {
        /// What the handler panicked with.
        message: &'a str,
    },
}

/// An observer gives up, and says why.
///
/// # There is no `impl Error for Stop`, on purpose
///
/// The blanket `From<E>` below overlaps `core`'s reflexive `impl<T> From<T> for T`
/// exactly when `Stop: Error + Send + Sync + 'static`. So `Stop` can have the
/// blanket conversion or it can be an `Error`, and not both. The blanket is what
/// lets an observer write `Err(io_error.into())`, which is the common case, so
/// it wins — and [`Stop::new`] covers the other one. RFC-06 §10 lists an `Error`
/// impl among the surface; that was a counting slip and this is the note it
/// asked for rather than a silent departure.
pub struct Stop(Box<dyn Error + Send + Sync>);

impl Stop {
    /// The only route for an observer that fails with a message rather than
    /// with an error type.
    ///
    /// Not a convenience. The blanket `From<E>` makes **every** other `From`
    /// impl a coherence error, so `From<String>` and `From<&str>` can never be
    /// added; without this an observer failing with a plain message would have
    /// to build the box by hand.
    #[must_use]
    pub fn new(because: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self(because.into())
    }

    /// The reason, for the loop that turns it into an [`Ended`].
    pub(crate) fn into_reason(self) -> Box<dyn Error + Send + Sync> {
        self.0
    }
}

impl<E: Error + Send + Sync + 'static> From<E> for Stop {
    fn from(error: E) -> Self {
        Self(Box::new(error))
    }
}

impl fmt::Debug for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Stop").field(&self.0.to_string()).finish()
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Why an actor is no longer running.
///
/// [`MailboxClosed`](crate::MailboxClosed) says "gone", and it must not be
/// stretched to say "gave up on durability". This is the second question, asked
/// separately.
///
/// Exhaustive, like [`Departure`](crate::Departure): a caller matches it, and a
/// wildcard arm is the collapse it exists to prevent.
#[derive(Debug, Clone)]
pub enum Ended {
    /// [`stop`](crate::LocalActor::stop) was called, or the last handle was
    /// dropped.
    Stopped,
    /// The loop never reached its exit: [`abort`](crate::LocalActor::abort), or
    /// a runtime shutdown.
    Vanished,
    /// A turn panicked and an observer refused to continue.
    ///
    /// A separate variant from [`Observer`](Self::Observer) on purpose. A
    /// durability adapter refuses a panicked turn, so folding the two would
    /// report the durability adapter for a fault in a `service!` method and
    /// point an operator at the wrong code.
    Panicked {
        /// What the handler panicked with.
        message: String,
    },
    /// An observer returned `Err`, or panicked, after a turn that completed.
    Observer {
        /// [`Observer::name`].
        name: &'static str,
        /// Why it gave up.
        ///
        /// An `Arc` because [`Ended`] is `Clone` and many callers can await it.
        /// The conversion from [`Stop`]'s `Box` happens once, in the loop.
        error: Arc<dyn Error + Send + Sync>,
    },
}

/// Why a turn produced no value.
///
/// Replaces [`MailboxClosed`](crate::MailboxClosed) on the catching paths,
/// because "the actor is gone" and "this turn was refused" ask a caller for
/// different things and the served path has to tell a peer which it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TurnLost {
    /// The actor was gone, or ended while this turn waited.
    #[error("the actor is no longer running")]
    Gone,
    /// An observer refused this turn. It **ran** and was not committed.
    #[error("the {name} observer refused this turn: it ran and was not committed")]
    Refused {
        /// [`Observer::name`].
        name: &'static str,
    },
}
