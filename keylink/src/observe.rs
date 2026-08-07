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
//! `after_turn` takes `&A` and never `&mut A` — and so does
//! [`Observer::finish`], which is why `representability.rs`'s probe implements
//! both — so no observer can call a `service!` method or assign to a field. That
//! is the whole guarantee. `&A`
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

use crate::Panic;

/// The future an observer returns.
///
/// Concrete rather than an `impl Trait`, because `middleware.rs`'s `BoxFut` is
/// `pub(crate)`, takes no lifetime and is `'static` — it cannot hold `&'a A`,
/// and widening it would move the public [`Next`](crate::Next) type.
pub type TurnFut<'a> = Pin<Box<dyn Future<Output = Result<(), Stop>> + Send + 'a>>;

/// The future [`Observer::finish`] returns.
///
/// A second alias rather than a `Result` reused from [`TurnFut`], because
/// `finish` is infallible and an `Output` that could be `Err` would invite a
/// refusal the loop has nothing left to do about — the actor has already ended
/// and its reason is already decided.
pub type FinishFut<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

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

    /// Called once, after the last turn and **before the state is dropped**.
    ///
    /// Infallible: the actor is already ending, so there is nothing an `Err`
    /// could change. `ended` is the same [`Ended`] every waiter is about to be
    /// told, so an observer cannot be given one reason while a caller reads
    /// another.
    ///
    /// Defaulted, so an observer that has nothing to flush says nothing. The
    /// reason to implement it is a policy that batches: RFC-06 §7's debounced
    /// store is only safe with a guaranteed final flush, and this is it.
    ///
    /// A plain `fn` and not an `async fn`, for the reason
    /// [`after_turn`](Self::after_turn) is one — and here the reason is
    /// stronger, because there is no reply to race and the whole point of the
    /// call is the synchronous work with `state` that precedes the flush. An
    /// `async fn` (and an RPITIT, which desugars to one) stores **every**
    /// argument in the future it returns, so `Send` on that future would demand
    /// `A: Sync` for every actor in the crate.
    ///
    /// # What it does not run for
    ///
    /// [`abort`](crate::LocalActor::abort) does not reach this: the task is
    /// cancelled at its next await and the loop never reaches its exit, so there
    /// is no place for the call to happen. Not a policy — a consequence.
    ///
    /// One hole, stated rather than papered over: a turn that never reaches an
    /// await is not interrupted by an abort either, so an actor aborted during
    /// such a turn can still exit the loop normally afterwards, and `finish`
    /// then runs. `abort`'s own documentation already carries that caveat.
    ///
    /// # A panic here is caught and dropped
    ///
    /// There is no caller left to resume it into and no reason left to change:
    /// `ended` has already been decided and handed to whichever observers ran
    /// before this one. The panic hook has already run at the fault, which is
    /// where the backtrace is — the same trade
    /// [`notify`](crate::LocalActor::notify) documents for a `tell`. The rest of
    /// the chain still runs.
    fn finish<'a>(&'a mut self, _state: &'a A, _ended: &'a Ended) -> FinishFut<'a> {
        Box::pin(async {})
    }
}

/// What the turn did.
///
/// `#[non_exhaustive]` so a later field is additive — and
/// [`method`](Self::method) is the proof, having been added to this struct
/// without moving a single call site outside the crate.
///
/// It deliberately carries **no** sequence number: a `seq` "counting from zero
/// per actor" is wrong wherever it could be built, because the message is
/// created on the *caller's* task, on any of N cloned handles.
///
/// # The literal is shut from outside, which is what [`for_test`] is for
///
/// A doctest, because it compiles as its own crate and `representability.rs`
/// cannot: in there the literal is reachable.
///
/// ```compile_fail
/// // Fails with `E0639`: a struct marked `#[non_exhaustive]` cannot be built by
/// // a literal outside the crate that declared it.
/// //
/// // The code is named HERE and deliberately NOT on the fence. rustdoc does not
/// // check the code after `compile_fail`, so an annotated code asserts a
/// // verification it does not perform — this project has shipped five guards
/// // that were satisfiable without proving their claim, and a fence reading
/// // `compile_fail,E0639` is how the sixth would look.
/// //
/// // What stops this passing for the WRONG reason — a renamed field, a moved
/// // type, a typo — are the two lines above the literal. They read both fields
/// // off a value keylink itself built, so every name written below is proved to
/// // exist before it is written, and the only thing left to fail is the
/// // attribute.
/// use keylink::{TurnInfo, TurnOutcome};
///
/// let built = TurnInfo::for_test(TurnOutcome::Completed, "Post");
/// let (outcome, method) = (built.outcome, built.method);
///
/// let _forged = TurnInfo { outcome, method };
/// ```
///
/// [`for_test`]: TurnInfo::for_test
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct TurnInfo<'a> {
    /// How the turn finished.
    pub outcome: TurnOutcome<'a>,
    /// Which declared method this turn ran.
    ///
    /// The **wire** spelling — `"Reserve"`, not `"reserve"` — so an observer and
    /// a [`Middleware`](crate::Middleware) watching the same call name it
    /// identically. `Rpc::method` is where the other half of that agreement is.
    ///
    /// It was absent because a boxed-closure mailbox had no name to report. A
    /// generated mailbox is an enum whose variants *are* the service's methods,
    /// so the name is free at the one dispatch site.
    ///
    /// **A feed reports the feed's name in this field.** Wave 3 adds a second
    /// declared input whose items are turns in every respect that matters here —
    /// they run the chain, they are committed, they can panic — and they are not
    /// methods. This field names whichever declaration produced the turn, which
    /// is why Wave 3 does not have to widen it and must not add a second one.
    pub method: &'static str,
}

impl<'a> TurnInfo<'a> {
    /// Build one, for a test of an [`Observer`] written outside this crate.
    ///
    /// keylink builds every `TurnInfo` a real turn produces, at the one dispatch
    /// site, and never reads one back. This exists for the crate that cannot:
    /// `#[non_exhaustive]` makes the struct literal an error outside keylink
    /// (`E0639`), so a downstream author who implemented [`Observer`] could not
    /// construct the argument to their own `after_turn` and therefore could not
    /// unit-test it at all. RFC-07 §1.8 recorded that; this answers it.
    ///
    /// `#[non_exhaustive]` stays, because it is right — the `method` field
    /// landed under it without breaking anybody, which is the whole argument for
    /// it — and the escape hatch is `#[doc(hidden)]` rather than documented
    /// surface: a later field is additive to `TurnInfo` and a **breaking change
    /// to this signature**, on purpose. It fails loudly at compile time in a
    /// test rather than quietly defaulting a field nobody chose.
    #[doc(hidden)]
    #[must_use]
    pub fn for_test(outcome: TurnOutcome<'a>, method: &'static str) -> Self {
        Self { outcome, method }
    }
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
///
/// # Three answers and not two, and why the third carries a payload
///
/// [`Faulted`](Self::Faulted) used to be expressed as a nested
/// `Result<Result<T, Panic>, TurnLost>`: the fault lived in the *inner* `Err`
/// and this enum could not name it. Two error positions for one question is the
/// two-layer unwrap a caller gets wrong, and it left this type able to say only
/// half of why there is no value.
///
/// So the fault moved in here and brought its payload with it, which costs
/// `Copy`, `Clone` and `Eq` — a [`Panic`] holds `Box<dyn Any + Send>` and is
/// none of the three. That is the honest price: the payload is what
/// [`ask`](crate::LocalActor::ask) resumes into the calling task, and an error
/// type that dropped it would make the crate's whole panic policy
/// unimplementable on top of the catching path.
#[derive(Debug, thiserror::Error)]
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
    /// The turn ran and panicked. The actor **survived** it.
    ///
    /// Distinct from [`Gone`](Self::Gone) on purpose, and the distinction is not
    /// cosmetic: a caller told `Gone` should stop calling, and a caller told
    /// `Faulted` has an actor that is still serving the next message. Collapsing
    /// the two is the inference this variant exists to delete — the loop knows
    /// which happened and now says so.
    #[error("{0}")]
    Faulted(Panic),
}
