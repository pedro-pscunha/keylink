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
//! It has **one** consumer today, and since Wave 5 that consumer is real code
//! rather than a plan: `keylink::Persist`, behind the `persist` feature — named
//! without a link because it is gated and this paragraph is not — which owns a
//! claim and writes a framed snapshot inside every turn it accepts.
//! Every implementor of this trait used to be a test or a probe, which by this
//! repo's own standard left the port unjustified. The argument for making it
//! generic is still the boundary rule above and not a queue of waiting adapters —
//! RFC-06 §0 says so plainly, having withdrawn a second consumer it had claimed.
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
//! # A keyed actor is observed too, since Wave 3
//!
//! This section used to say the opposite, and the paragraph is kept in the past
//! tense because the *shape* of the limit is the interesting part. A keyed actor
//! mounted in a [`Registry`](crate::Registry) got no observers, first because `spawn`
//! ran a second loop over its own dispatcher and never touched
//! [`LocalActor`](crate::LocalActor) — two loops, and closing the gap meant
//! merging them — and then, after Wave 1b made the dispatch one path, because
//! `spawn` handed that one path an **empty** hook slice. RFC-06 §5.3.
//!
//! What was left was the slice and the question of who owns the `Vec` for a
//! lazily spawned actor. Feeds forced it, because a keyed actor with a feed needs
//! the same `select!` a local one does: there is **one loop** now, in `turns.rs`,
//! and both paths enter it with a state, a `Vec` of observers and a set of feeds.
//! The owner of the `Vec` is whoever builds the actor —
//! `<service>::setup(state).observe(observer).feeds(feeds).mountable()` for a keyed one, called from
//! a [`spawn_with`](crate::RegistryBuilder::spawn_with) factory when the actor is
//! built on first message. That triple is also the shape re-incarnation needs.
//!
//! `<service>::spawn(state)` still means "no observers", and it is still the door
//! most keyed actors use. It is now a shorthand rather than a limit.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use postcard_schema::Schema;
use serde::{Deserialize, Serialize};

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
/// [`Registry`](crate::Registry) is watched by whatever its factory gave it — see the
/// module documentation.
///
/// A `feed` item is a turn like any other: [`after_turn`](Self::after_turn) runs
/// for one, [`TurnInfo::method`] names the feed, and an `Err` ends the actor.
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

impl<A, O> Observer<A> for Box<O>
where
    O: Observer<A> + ?Sized,
{
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn after_turn<'a>(&'a mut self, state: &'a A, turn: TurnInfo<'a>) -> TurnFut<'a> {
        (**self).after_turn(state, turn)
    }

    fn finish<'a>(&'a mut self, state: &'a A, ended: &'a Ended) -> FinishFut<'a> {
        (**self).finish(state, ended)
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
/// use std::time::Duration;
/// use keylink::{TurnInfo, TurnOutcome};
///
/// let built = TurnInfo::for_test(TurnOutcome::Completed, "Post", Duration::ZERO);
/// let (outcome, method, duration) = (built.outcome, built.method, built.duration);
///
/// let _forged = TurnInfo { outcome, method, duration };
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
    /// How long the handler took, excluding observer callbacks.
    ///
    /// This is measured around the handler future on the actor task. Observer
    /// time is intentionally not included: the duration describes the work the
    /// method or feed performed, while observer overhead is a separate concern.
    pub duration: Duration,
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
    pub fn for_test(outcome: TurnOutcome<'a>, method: &'static str, duration: Duration) -> Self {
        Self {
            outcome,
            method,
            duration,
        }
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
///
/// # Coherence rules out a `Fatal<E>` a caller converts from — and nothing else
///
/// [`fatal`](Self::fatal) needs to say one more thing than the boxed error
/// carries, and the obvious way to say it — a `Fatal<E>` wrapper an observer
/// writes `Err(Fatal(io_error).into())` against — is **`error[E0119]`**, because
/// the blanket `From<E>` below overlaps every other `From` impl. So the
/// constructor is an inherent associated function beside [`new`](Self::new),
/// with the same `impl Into<Box<..>>` shape, and the discriminator travels
/// beside the error rather than inside its type.
///
/// **That is the whole of what coherence decides, and it is less than it looks.**
/// It rules out the *convertible wrapper*. It says nothing about how the
/// discriminator is spelled once it is a field: a
/// `pub(crate) enum Severity { Retryable, Fatal }` here would have no coherence
/// problem at all — nothing converts *from* it — and would compile.
///
/// It is a `bool` for a weaker and more ordinary reason, and the reason is
/// written down rather than implied because the first version of this note let
/// the coherence argument carry the field's *type*, which it does not reach.
/// **The two-state fact is already on the public surface as a `bool`, twice**:
/// [`is_fatal`](Self::is_fatal) here, and `fatal` on
/// [`Ended::Observer`] — which is argued separately and
/// on its own merits further down. `into_parts` is `pub(crate)` and has two
/// callers, both in `turns.rs`, so an internal enum would buy a self-describing
/// return at those two sites and be a private synonym for a shape the public API
/// spells as a `bool` anyway. That is a judgement about one type against three
/// call sites, not a constraint — and calling it a constraint is what this
/// paragraph is fixing.
pub struct Stop(Box<dyn Error + Send + Sync>, Stance);

/// What a refusal says about the turn and about another incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stance {
    /// The turn was not committed, and another incarnation may fare better.
    Retryable,
    /// The turn was not committed, and another incarnation cannot help.
    Fatal,
    /// The observer cannot say whether the turn was committed.
    Indeterminate,
}

impl Stop {
    /// The only route for an observer that fails with a message rather than
    /// with an error type.
    ///
    /// Not a convenience. The blanket `From<E>` makes **every** other `From`
    /// impl a coherence error, so `From<String>` and `From<&str>` can never be
    /// added; without this an observer failing with a plain message would have
    /// to build the box by hand.
    ///
    /// A refusal made this way — or through the blanket `From` — is one a
    /// supervisor may retry. Say [`fatal`](Self::fatal) when it is not.
    #[must_use]
    pub fn new(because: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self(because.into(), Stance::Retryable)
    }

    /// Refuse, and say that another incarnation cannot help.
    ///
    /// A snapshot written by a newer build, a claim another machine holds, a
    /// configuration that is wrong rather than unlucky. A
    /// [supervised](crate::LocalActor::spawn_supervised) actor gives up at once
    /// on one of these instead of spending its whole budget proving the same
    /// thing several times, each attempt costing a re-acquire.
    ///
    /// It means nothing to an actor nobody is supervising: the actor ends
    /// either way, and the only difference is the `fatal` flag on
    /// [`Ended::Observer`], which says *why* it will not be coming back.
    ///
    /// # It is honoured for a turn that **completed**, and not for one that faulted
    ///
    /// The loop reports a refused *panicked* turn as [`Ended::Panicked`] naming
    /// the fault rather than the observer — a durability adapter refuses every
    /// panicked turn, and reporting the adapter would point an operator at the
    /// wrong code. That ending carries no observer and therefore no flag, so a
    /// supervisor restarts after it. That is deliberate rather than a gap: the
    /// reason the actor is ending is the fault, and a fault is exactly the kind
    /// of thing another incarnation might not repeat.
    #[must_use]
    pub fn fatal(because: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self(because.into(), Stance::Fatal)
    }

    /// Refuse, and say that **the turn may have been committed anyway.**
    ///
    /// For an observer whose write failed and whose undo failed too: a record
    /// that may be stored, so that a caller told *it was not committed* would retry
    /// and apply the work twice. The caller is told
    /// [`Withheld::Indeterminate`] instead, which says to find out before acting.
    ///
    /// Not fatal: a supervised actor restarts, and the new incarnation recovers
    /// from whatever the store holds, which is how the question gets answered.
    #[must_use]
    pub fn indeterminate(because: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self(because.into(), Stance::Indeterminate)
    }

    /// Whether this refusal said another incarnation cannot help.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        self.1 == Stance::Fatal
    }

    /// The reason and the flag, for the loop that turns them into an [`Ended`].
    ///
    /// Named for the pair rather than for the reason: it used to be
    /// `into_reason` returning one box, and a name that keeps promising one
    /// thing while handing back two is how a caller drops the second.
    pub(crate) fn into_parts(self) -> (Box<dyn Error + Send + Sync>, Stance) {
        (self.0, self.1)
    }
}

impl<E: Error + Send + Sync + 'static> From<E> for Stop {
    fn from(error: E) -> Self {
        Self(Box::new(error), Stance::Retryable)
    }
}

impl fmt::Debug for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Stop")
            .field(&self.0.to_string())
            .field(&self.1)
            .finish()
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Why an actor is no longer running.
///
/// [`Withheld::Gone`] says "gone", and it must not be stretched to say "gave up
/// on durability". This is the second question, asked separately: a refused
/// caller is told [`Withheld::Refused`] and the observer's name, and this
/// carries the observer's own error.
///
/// # Exhaustive, and deliberately not `#[non_exhaustive]` — decided once, here
///
/// Like [`Departure`](crate::Departure): a caller matches this, and a wildcard
/// arm is the collapse it exists to prevent.
///
/// [`Feed`](Self::Feed) is the first ending added after the enum had consumers, so
/// Wave 3 is where the cost of that decision could finally be measured instead of
/// predicted. **It was measured, and it was zero**, which is not what the wave was
/// briefed to expect: adding the variant produced **no** `error[E0004]` anywhere in
/// this workspace. Every match on an `Ended` in `keylink/tests` is a single arm plus
/// `other => panic!("got {other:?}")` — a test asserting *which* ending happened,
/// which is not the collapse this enum guards against and would be unaffected by
/// sealing either way. keylink's own code *builds* `Ended` values and reads one
/// only through `Debug`.
///
/// So the churn argument decides nothing in either direction, and saying so matters
/// more than the conclusion: the record must not claim a guard fired when it did
/// not. What is left is the asymmetry between the two failures.
///
/// - Sealed, a downstream matcher is **forced** to write `_ =>`, and that arm
///   silently swallows every ending added later. That is exactly how
///   `TurnLost`'s own gone-arm swallowed a fault for three waves, and it is why
///   Wave 2's review refused to seal that type on this same ground. That arm is
///   [`Withheld::Gone`] now, and the argument moved with it.
/// - Exhaustive, a downstream matcher gets a compile error at the moment a new
///   reason to end exists, which is the moment it has a decision to make. The bill
///   is a major version per ending once this crate is published, and today it is
///   unpublished with no downstream consumers at all.
///
/// The bill is real and is the honest cost of the choice. It is worth paying,
/// because "the actor stopped" and "the actor's feed died" ask an operator for
/// different actions, and a type whose job is to say *which* must not be able to
/// answer "one of the ones you have heard of".
///
/// **What this means for the next ending.** Wave 9's `GaveUp` — a supervisor that
/// stopped restarting — lands the same way, and should expect the same near-zero
/// in-tree churn. `representability.rs::every_ending_is_named` is a `#[cfg(test)]`
/// match with no wildcard, so a new variant is one `E0004` **inside keylink** that
/// points a reader at this paragraph rather than at nothing. Do not reach for
/// `#[non_exhaustive]` to silence it.
///
/// **And it was measured a second time.** Adding [`GaveUp`](Self::GaveUp) produced
/// exactly one `error[E0004]` in this workspace, in `every_ending_is_named` and
/// nowhere else — the prediction above, confirmed rather than assumed. The
/// variant is named there and the enum stays exhaustive.
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
        /// Whether the observer said another incarnation cannot help.
        ///
        /// [`Stop::fatal`] against [`Stop::new`]. A `bool` and not a two-armed
        /// enum because it is one author's yes-or-no about their own refusal
        /// rather than a value set with room to grow, and because the surface
        /// already spells it that way in [`Stop::is_fatal`].
        ///
        /// An observer that panicked lands here with `false`: it made no claim
        /// at all, and inventing one for it would be the silent fallback this
        /// crate refuses everywhere else.
        fatal: bool,
    },
    /// A `feed` produced its last item.
    ///
    /// **The actor stops, and that is the feature.** A feed is an input the actor
    /// has no way to re-open and no way to poll: a market-data subscription that
    /// silently ended leaves the state frozen at whatever it last saw, and every
    /// later read of it is stale data served as fresh. Carrying on would be the
    /// silent fallback this crate refuses everywhere else, so the actor ends and
    /// says which feed did it.
    ///
    /// Carries no error, because a stream that ended reported nothing to carry.
    /// `Stream::poll_next` answering `None` is the whole of what happened; if a
    /// feed wants to explain itself, its item type is the place to do it.
    Feed {
        /// The feed's declared name, spelled the way
        /// [`TurnInfo::method`](TurnInfo::method) spells it — `"Fills"`, not
        /// `"on_fills"`.
        name: &'static str,
    },
    /// A supervisor stopped re-incarnating this actor.
    ///
    /// The only ending a [supervised](crate::LocalActor::spawn_supervised) actor
    /// can reach that an unsupervised one cannot, and the reason the whole enum
    /// is worth having: "gone" is already [`Withheld::Gone`]'s answer, and an
    /// operator needs to know whether this actor stopped because somebody asked
    /// it to or because it could not be kept alive.
    ///
    /// The mailbox closes when this is published, so every later call reports it
    /// — [`Withheld::Gone`] to a door that wanted a value,
    /// [`NotQueued::Closed`](crate::NotQueued::Closed) to one that wanted
    /// only to be queued — and a peer is told
    /// [`RemoteError::NoSuchActor`](crate::RemoteError::NoSuchActor).
    GaveUp {
        /// How many incarnations were spent before giving up.
        ///
        /// **Restarts, not incarnations**, so `0` means the first one was the
        /// last: an [`Intensity`](crate::Intensity) budget that was never
        /// touched, because the factory failed fatally, an observer said
        /// [`Stop::fatal`], or [`stop`](crate::LocalActor::stop) is not what
        /// happened here at all. It is a number this supervisor counted, not a
        /// counter on the actor — `LocalActor::incarnations()` deliberately does
        /// not exist.
        after: u32,
        /// The last incarnation's reason for ending, or the factory's reason for
        /// refusing to build one.
        ///
        /// An `Arc` for [`Observer`](Self::Observer)'s reason: [`Ended`] is
        /// `Clone` and many callers can await it.
        error: Arc<dyn Error + Send + Sync>,
    },
}

/// One line, for a log.
///
/// [`Debug`](fmt::Debug) is derived and prints the fields; this prints the
/// sentence. [`EndReason`]'s `Display` mirrors this one arm for arm, and the
/// mirror is why the two types share a file: this sentence is what an operator
/// reads out of an actor's own ending, and that one is what a peer reads inside
/// [`Withheld::Restarting`]'s. A struct-literal rendering on either side would be
/// noise.
impl fmt::Display for Ended {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => f.write_str("the actor was stopped"),
            Self::Vanished => f.write_str("the actor's task was cancelled"),
            Self::Panicked { message } => write!(f, "a turn panicked: {message}"),
            // **Not "gave up", which was false for half of this variant's
            // producers.** The variant's own doc has always said `Err`, *or
            // panicked*, and an observer that panicked did not give anything up —
            // it fell over. This is the sentence an operator reads in a log, and
            // `EndReason`'s mirror of it is the sentence a peer reads about a
            // restart, so one verb has to be true of both producers: they both
            // ended the incarnation, and neither of them let the turn commit.
            Self::Observer { name, error, .. } => {
                write!(f, "the {name} observer ended this incarnation: {error}")
            }
            Self::Feed { name } => write!(f, "the {name} feed ended"),
            Self::GaveUp { after, error } => {
                write!(f, "gave up after {after} restarts: {error}")
            }
        }
    }
}

/// Why an actor ended, in the form that crosses the wire.
///
/// # A second vocabulary, and the one place it could not be avoided
///
/// This crate refuses parallel types for one concept, and this is a parallel
/// type for [`Ended`]. It exists because [`Ended::Observer`] carries an
/// `Arc<dyn Error + Send + Sync>`, which has no encoding and no schema — a
/// trait object is not data. The choice was this, or dropping the one arm that
/// names *which* observer refused, which is the arm an operator most needs.
///
/// What keeps the two from drifting is that there is exactly one conversion,
/// [`From<&Ended>`](#impl-From<%26Ended>-for-EndReason), it is written with no
/// wildcard arm, and `Ended` is exhaustive. So a new ending is a compile error
/// **here**, on the line that would otherwise have quietly mapped it to
/// something else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub enum EndReason {
    /// [`Ended::Stopped`].
    Stopped,
    /// [`Ended::Vanished`].
    Vanished,
    /// [`Ended::Panicked`].
    Panicked {
        /// What the handler panicked with.
        message: String,
    },
    /// [`Ended::Observer`], with the error rendered through `Display`, because
    /// that is the whole of what survives an `Arc<dyn Error>` crossing a wire.
    Observer {
        /// [`Observer::name`].
        name: String,
        /// Why it gave up.
        error: String,
        /// Whether the observer said another incarnation cannot help.
        ///
        /// Carried rather than dropped because it is the difference between "this
        /// turn was refused" and "this actor is not coming back", and a console
        /// that showed the first for the second would send an operator looking for
        /// a restart that was never going to happen.
        fatal: bool,
    },
    /// [`Ended::Feed`].
    Feed {
        /// The feed's declared name.
        name: String,
    },
    /// [`Ended::GaveUp`] — a supervisor stopped re-incarnating this actor.
    ///
    /// The one ending a console most needs to distinguish, and the reason
    /// [`Ended`] is worth putting on a wire at all: every other variant means
    /// something asked the actor to stop, and this one means it could not be kept
    /// alive.
    GaveUp {
        /// Restarts spent before giving up. `0` means the first incarnation was
        /// the last — see [`Ended::GaveUp::after`], whose sense this copies.
        after: u32,
        /// The last incarnation's reason, or the factory's reason for refusing to
        /// build one, through `Display`.
        error: String,
    },
}

impl From<&Ended> for EndReason {
    /// No wildcard arm, deliberately: see this type's own documentation.
    fn from(ended: &Ended) -> Self {
        match ended {
            Ended::Stopped => Self::Stopped,
            Ended::Vanished => Self::Vanished,
            Ended::Panicked { message } => Self::Panicked {
                message: message.clone(),
            },
            Ended::Observer { name, error, fatal } => Self::Observer {
                name: (*name).to_owned(),
                error: error.to_string(),
                fatal: *fatal,
            },
            Ended::Feed { name } => Self::Feed {
                name: (*name).to_owned(),
            },
            Ended::GaveUp { after, error } => Self::GaveUp {
                after: *after,
                error: error.to_string(),
            },
        }
    }
}

/// The same line [`Ended`] writes, from the form that crossed a wire.
///
/// # Hand-written, and a mirror rather than a second wording
///
/// Every arm writes the sentence [`Display for Ended`](Ended#impl-Display-for-Ended)
/// writes for the arm it was converted from, because the two are one ending seen
/// from two sides and a peer that read a *different* sentence about it would have
/// no way to know it was the same fact. That is the whole reason the two types
/// share a file: the mirror is checkable by reading forty lines, and it is not
/// checkable at all if they sit apart.
///
/// `#[derive(thiserror::Error)]` would spell the same thing in attributes, and
/// this is written out instead for [`Ended`]'s reason — the two must be diffed
/// against each other, and an attribute on the variant and a `write!` in a match
/// arm do not diff.
///
/// # What it is for, which is one caller
///
/// [`Withheld::Restarting`] renders it: a peer whose reply died in a restart is
/// told *another incarnation is coming* **and which ending caused it**. That
/// sentence carried the ending on `main`, through a `"restarting: "` prefix in
/// front of the whole of `Ended`'s `Display`, and lost it for one wave when the
/// prefix became a variant — the `#[error]` text named no field, so an operator
/// reading `err.to_string()` on the far end got a fixed sentence about an ending
/// the value was carrying all along.
///
/// [`Observer::fatal`](Self::Observer::fatal) is not rendered, exactly as
/// [`Ended::Observer`]'s `fatal` is not: it is a supervision flag rather than
/// part of the sentence, and a peer matches the field.
impl fmt::Display for EndReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => f.write_str("the actor was stopped"),
            Self::Vanished => f.write_str("the actor's task was cancelled"),
            Self::Panicked { message } => write!(f, "a turn panicked: {message}"),
            Self::Observer { name, error, .. } => {
                write!(f, "the {name} observer ended this incarnation: {error}")
            }
            Self::Feed { name } => write!(f, "the {name} feed ended"),
            Self::GaveUp { after, error } => {
                write!(f, "gave up after {after} restarts: {error}")
            }
        }
    }
}

/// What a caller may learn about an actor that is still there.
///
/// Two facts, and the pair separates three states where either alone separates
/// two: idle, inside a turn with the slot still free, and inside a turn with the
/// next caller already about to block on the permit.
///
/// [`LocalActor::serving`](crate::LocalActor::serving) folds it for a singleton
/// and [`Registry::serving`](crate::Registry::serving) for one keyed actor, each
/// from the two readings its own side already had. Neither invents a fact and
/// neither takes a new lock the side was not already taking.
///
/// **Two readings, and not one instant.** Each field is loaded as the producer
/// reaches it, about a task on another thread, so the value is stale by the time
/// it is returned and the two halves need not describe the same moment. That is
/// the honest shape for this question and the same one
/// [`Registry::status`](crate::Registry::status) states about its list. Evidence,
/// never a precondition.
///
/// # Two types, because they answer two questions
///
/// [`ActorStatus`](crate::ActorStatus) is **every actor on this node, to an
/// operator**, behind `Inspect`'s own ALPN and its own
/// [`Allowlist`](crate::Allowlist). This is **the actor you just addressed, to
/// its own caller**. The second is not a narrowing of the first, and the two
/// fields it does not carry are *unreachable* rather than dropped:
///
/// - `ActorStatus` is built in exactly one place, over the keyed entry map, so a
///   singleton yields **no row at all** rather than a row with holes. The
///   question has no field-level answer on that side, which is why this is a
///   type rather than `ActorStatus` with two fields hidden.
/// - [`ActorStatus::id`](crate::ActorStatus::id) repeats an id the caller of
///   this one already wrote, and
///   [`ActorStatus::idle_for`](crate::ActorStatus::idle_for) reads the entry's
///   eviction clock, which exists in the registry and nowhere else. A singleton
///   has no entry to read it from, and a second flag meaning "this actor has no
///   registry" is the parallel fact this crate refuses.
///
/// # There is no `is_running`, and each producer already answers it
///
/// On the keyed side the `Option` is that answer and it is exact: `None` is
/// nothing mounted under this id, read **without** mounting one. On the
/// singleton side the caller holds the actor, and
/// [`LocalActor::is_running`](crate::LocalActor::is_running) is a separate
/// question with its own advisory caveat. A `bool` here would be a third
/// spelling of what two doors already carry.
///
/// # `Eq` compares `queued`, and that is a sharp edge the type does not close
///
/// The derive below is field by field, so `==` does the one thing
/// [`queued`](Serving::queued) says never to do: it compares a keyed
/// mailbox's depth against a singleton channel's as a number. **Prose is the
/// only thing preventing that**, and a reader who meets the derive before the
/// field's paragraph would not know it, which is why the warning is here.
///
/// Derived anyway, and the trade is deliberate. Every comparison this crate
/// makes is between two readings of **one** actor served **one** way — the two
/// doors onto a keyed actor, or the two onto a singleton — where the equality is
/// exact and is the entire assertion the guards make. Encoding the side in the
/// type, with a phantom parameter or two structs, would put a type argument on
/// every caller and every wire signature to forbid a comparison nobody has yet
/// written. That is a larger permanent cost than the rule it buys.
///
/// So: compare two of these when both came from the same side, and read the
/// field when they did not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Serving {
    /// Inside a turn right now.
    ///
    /// True from the moment the loop takes a message or a feed item until that
    /// turn's handler returns, including across every `await` inside it. A turn
    /// parked on I/O is a turn that is running.
    ///
    /// **This is the question you cannot ask the actor.** A message asking it
    /// would queue behind the turn that wedged it, and if it ever ran it would
    /// report `true` about itself — the state of asking rather than the state of
    /// the actor. So every producer reads a flag beside the mailbox and sends
    /// nothing.
    pub busy: bool,
    /// Queued and not yet taken.
    ///
    /// **Comparable as a predicate, not as a count.** *Will the next caller
    /// block on the permit* is exact on both sides. The number is not: a keyed
    /// mailbox carries only the service's own messages, while a
    /// [`LocalActor`](crate::LocalActor)'s channel also carries a
    /// [`read`](crate::LocalActor::read) and the stop request, so one queued read
    /// reads `1` there and nothing at all on the keyed side. Never compare two of
    /// these from the two sides by this field.
    ///
    /// A floor on the real backlog either way, and `u64` rather than `usize`,
    /// both for the reasons
    /// [`ActorStatus::queued`](crate::ActorStatus::queued) carries: senders
    /// parked waiting for a permit are not observable at all, and a width the
    /// compiler chooses cannot be half of a contract between two machines.
    pub queued: u64,
}

/// Why a turn produced no value, at the door that hands a caller the fault
/// itself.
///
/// Three variants, and the first of them is [`Withheld`] — the crate's one list
/// of losses, which [`Unanswered`](crate::Unanswered) and
/// [`RemoteError::Withheld`](crate::RemoteError::Withheld) carry too. A local
/// caller and a peer therefore read **one** sentence about one condition,
/// because they read the same value and not two copies of it.
///
/// # It was flat, and flat is what made two enums drift
///
/// This type and `Unanswered` each listed the same six losses as their own
/// variants, in the same order, with the same `#[error]` texts written twice.
/// Nothing held the two lists together except a test that walked them
/// variant-by-variant — and the walk could only compare what was already there,
/// so it caught drift in the *words* and never in the *payloads*. `Restarting`
/// is where that showed: this type carried the ending and `Unanswered` carried
/// a bare unit, and the guard passed on both.
///
/// One shared payload is what deletes the correspondence problem rather than
/// policing it: there is no second list to keep in step.
///
/// # Three variants stay outside `Withheld`, for two different reasons
///
/// [`Faulted`](Self::Faulted) holds the [`Panic`] itself, and that payload is
/// what [`ask`](crate::LocalActor::ask) resumes into the calling task. It cannot
/// move into `Withheld`: `Box<dyn Any + Send>` has no encoding, an unwind does
/// not cross a network, and [`Withheld::Panicked`] is the same fault stated to
/// the audience that can act on it — the message, through `Display`. So the
/// split is by what the audience can be *handed*, not by what happened.
///
/// [`Reentrant`](Self::Reentrant) and [`Full`](Self::Full) are not about a turn at
/// all. Every `Withheld` variant describes what happened to a turn that exists;
/// both of these are refused before anything is queued, so there is no turn for
/// anything to have been withheld from.
/// [`Unanswered::Reentrant`](crate::Unanswered::Reentrant) and
/// [`NotQueued::Reentrant`](crate::NotQueued::Reentrant) are the first fact at the
/// other doors, [`Unanswered::Full`](crate::Unanswered::Full) and
/// [`NotQueued::Full`](crate::NotQueued::Full) the second — and
/// [`Failure`](crate::Failure) is the **fourth** enum carrying both, at the door
/// that answers a call made through the bound.
///
/// Those **eight** `#[error]` texts are the only ones this crate still keeps in
/// step by hand. This sentence said six until a merge gate counted them: it
/// enumerated three enums, and `Failure` was in neither the count nor any guard,
/// so [`Failure::Reentrant`](crate::Failure::Reentrant) was a fourth byte-identical
/// sentence nothing held. `turns.rs`'s
/// `one_vocabulary_and_every_loss_in_it_has_a_producer` compares it now, and
/// compares it **directly**, because no `From` reaches that type. Seven of the
/// eight are byte-identical; [`Failure::Full`](crate::Failure::Full) reads *call*
/// where the other three read *message*, which is the door it answers, and the
/// same guard pins that difference so unifying the two is a decision rather than
/// a tidy-up.
///
/// # `Debug` and `Error`, and no other derive
///
/// [`Panic`] has none, and it holds `Box<dyn Any + Send>` — so `Clone`, `Eq` and
/// `Copy` are unavailable here whatever `Withheld` supports. `Unanswered` is the
/// type that keeps them, and the door decides which a caller gets: a resuming
/// path is handed the payload, a reporting one is handed the words.
///
/// # Exhaustive, and deliberately not `#[non_exhaustive]`
///
/// Like [`Ended`] and [`Departure`](crate::Departure): a caller matches this, and
/// a wildcard arm is the collapse it exists to prevent. `Withheld` makes the same
/// choice for the same reason, and it is the type a *withheld* loss would be added
/// to — so a caller who has matched all four arms here still meets a new one as a
/// compile error, one level in. [`Full`](Self::Full) went on this type instead,
/// and the paragraph above says why: it is not a loss a turn suffered.
#[derive(Debug, thiserror::Error)]
pub enum TurnLost {
    /// The turn was reached and its value was not handed back, and this is which
    /// loss it was.
    ///
    /// Every one of them is a fact about a turn: it ran and was refused, it ran
    /// and panicked, it was answered and the answer was thrown away, or the
    /// incarnation that owed it died. [`Withheld`] carries what each one means
    /// and what a caller should do next.
    #[error("{0}")]
    Turn(Withheld),
    /// The turn ran and panicked. The actor **survived** it.
    ///
    /// Distinct from [`Withheld::Gone`] on purpose, and the distinction is not
    /// cosmetic: a caller told `Gone` should stop calling, and a caller told
    /// `Faulted` has an actor that is still serving the next message.
    ///
    /// It carries the [`Panic`] rather than its message, which is the whole
    /// reason it is not a [`Withheld`] variant: [`ask`](crate::LocalActor::ask)
    /// resumes this payload into the calling task, so the panic is forwarded
    /// rather than described. A peer cannot be handed an unwind and reads
    /// [`Withheld::Panicked`] instead.
    #[error("{0}")]
    Faulted(Panic),
    /// The call was made from inside the target actor's own turn, and would have
    /// deadlocked. **Nothing was queued**, so the actor is untouched.
    ///
    /// Outside [`Withheld`] because there is no turn: the refusal happens before
    /// the message reaches the mailbox, so nothing ran and nothing was withheld.
    /// The actor is not merely still running, it is running the turn this call was
    /// made from.
    ///
    /// [`Unanswered::Reentrant`](crate::Unanswered::Reentrant) is the same fact
    /// at the doors that ask for a value and resume a fault rather than report it,
    /// and [`NotQueued::Reentrant`](crate::NotQueued::Reentrant) at the doors that
    /// ask only to be queued and so have no reply channel to hear a verdict on.
    /// [`Failure::Reentrant`](crate::Failure::Reentrant) is the fourth, at the door
    /// a caller reaches through the bound. Four enums, one fact, because a caller
    /// matches the answer of the door it used.
    #[error("this call was made from inside the target actor's own turn: it would deadlock")]
    Reentrant,
    /// The mailbox was full and this actor was spawned under
    /// [`Backpressure::Shed`](crate::Backpressure::Shed), so the call was refused
    /// instead of parked. **Nothing was queued**, so the actor is untouched.
    ///
    /// Outside [`Withheld`] for [`Reentrant`](Self::Reentrant)'s reason: the
    /// refusal happens before the message reaches the mailbox, so nothing ran and
    /// nothing was withheld. It is the second fact of that shape on these same
    /// four enums, and [`NotQueued::Full`](crate::NotQueued::Full) carries the
    /// whole argument.
    ///
    /// **Unreachable under [`Backpressure::Park`](crate::Backpressure::Park)**,
    /// which is the default on both spawn doors.
    #[error("the actor's mailbox is full and it shed this message")]
    Full,
}

/// Why a call produced no value, on every path that has one.
///
/// **One list, carried by three types.** [`TurnLost::Turn`] hands it to an
/// [`ask_catching`](crate::LocalActor::ask_catching),
/// [`Unanswered::Turn`](crate::Unanswered::Turn) to an
/// [`ask`](crate::LocalActor::ask), and
/// [`RemoteError::Withheld`](crate::RemoteError::Withheld) to a peer. The three
/// differ in what they add around it — a [`Panic`] payload, nothing, a wire
/// envelope — and not in what they say about the loss, because the loss is this
/// value and there is no second copy of it.
///
/// A peer used to read these off the front of a
/// [`RemoteError::Failed`](crate::RemoteError::Failed) string with
/// `starts_with`, against five prefix constants; a local caller read one of two
/// flat enums whose `#[error]` texts were kept in step by a test. Both were
/// spellings of *this list*, written a third and a fourth time.
///
/// [`Panicked`](Self::Panicked) is the one variant a local caller does not
/// normally meet, and the reason is the door rather than an omission:
/// [`ask`](crate::LocalActor::ask) resumes the [`Panic`] into the calling task
/// and [`TurnLost::Faulted`] hands the payload itself over. A peer can be handed
/// neither, so what crosses is the message.
///
/// # Exhaustive, and deliberately not `#[non_exhaustive]`
///
/// The rule is [`FollowRefused`](crate::FollowRefused)'s, and it has two halves:
/// an enum whose every caller does the same thing with every variant is sealed,
/// and an enum a caller must be **made** to look at a new arm of is not. This is
/// the second kind, and three of its variants say so on their own.
/// [`Restarting`](Self::Restarting) means retry, [`Gone`](Self::Gone) means stop
/// calling, and [`Refused`](Self::Refused) means an observer vetoed a turn that
/// ran. A `_ =>` arm picks one of those three answers for a caller who never
/// read the fourth.
///
/// [`Unanswered`](crate::Unanswered) and [`TurnLost`] resolve the same question
/// the same way and for the same reason, and they resolve it by carrying this
/// enum. [`RemoteError`](crate::RemoteError) stays `#[non_exhaustive]` on the
/// other half of the rule: it is *handled*, not exhausted.
///
/// **The ALPN is not the argument, in either direction.** A new variant moves it
/// whichever attribute this type carries, so a drifted peer is refused at the
/// handshake in both worlds. The attribute decides only what a peer that *can*
/// decode a variant is allowed to do with it.
///
/// # `Expired` is representable and no path reaches the one producer it has
///
/// A local `Origin` holds no `Queueing`, so there is no budget to expire.
/// Neither is a link, because both are
/// `#[doc(hidden)]` — rustdoc renders a link to a hidden item as a dead `href`
/// and `-D warnings` does not see it. `ask_catching` **does** build this variant,
/// on the arm that reads an expired budget, and that arm is the producer no path
/// reaches. On the served side a budget *does* expire,
/// and that
/// path answers [`RemoteError::TimedOut`](crate::RemoteError::TimedOut), which
/// carries the millisecond figure this variant has nowhere to put.
///
/// It is kept anyway: the state it names is one `Answered` already has, and a
/// variant written to be *correct* if something reaches it beats a fold into
/// [`Gone`](Self::Gone) — which is a live actor reported as finished, silently.
///
/// **Nothing in the type system says a variant is reachable**, which is how this
/// one reached zero *reachable* producers without anybody noticing. `turns.rs`'s
/// `one_vocabulary_and_every_loss_in_it_has_a_producer` is what says so now: it
/// walks this enum exhaustively and calls the production function that builds
/// each variant, with this one exempted by name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema, thiserror::Error)]
pub enum Withheld {
    /// The turn ran and panicked. The actor **survived** it and answers the next
    /// message.
    ///
    /// Distinct from [`Gone`](Self::Gone) for [`TurnLost::Faulted`]'s reason: a
    /// peer told `Gone` should stop calling, and this one has an actor that is
    /// still serving the connection it asked on.
    #[error("the turn panicked and the actor kept its state: {message}")]
    Panicked {
        /// What the handler panicked with, through `Display`.
        message: String,
    },
    /// An observer refused this turn. It **ran** and was not committed.
    ///
    /// The actor is **not** gone: a refusal that did not declare itself fatal
    /// ends one incarnation and a supervisor starts another.
    #[error("the {name} observer refused this turn: it ran and was not committed")]
    Refused {
        /// [`Observer::name`], owned because a peer's copy is not `'static`.
        name: String,
    },
    /// An observer **panicked** watching this turn. It **ran** and was not
    /// committed.
    ///
    /// Separate from [`Refused`](Self::Refused) because both withhold the value
    /// and only one of them is a decision anybody made. A caller whose observer
    /// fell over was told *the {name} observer refused this turn* about an
    /// observer that decided nothing, until this variant's local ancestor
    /// existed.
    #[error("the {name} observer panicked during this turn: it ran and was not committed")]
    ObserverPanicked {
        /// [`Observer::name`], owned for [`Refused::name`](Self::Refused::name)'s
        /// reason.
        name: String,
    },
    /// An observer refused this turn and **cannot say whether it was committed.**
    ///
    /// The only refusal for which retrying is not safe. [`Refused`](Self::Refused)
    /// promises the turn was not stored, so a caller may run it again. Here a
    /// write failed and the undo failed too, so the work may be on the disk and
    /// would be applied a second time. Find out first: ask the actor for its
    /// state once it is back, or use a key the work carries.
    #[error("the {name} observer could not say whether this turn was committed: it ran, and it may have been")]
    Indeterminate {
        /// [`Observer::name`], owned for [`Refused::name`](Self::Refused::name)'s
        /// reason.
        name: String,
    },
    /// The actor is serving, and this reply was thrown away unanswered.
    ///
    /// A handler took [`Reply::later`](crate::Reply::later) and dropped the
    /// [`Deferred`](crate::Deferred), or panicked while holding the channel.
    /// **Distinct from [`Gone`](Self::Gone)**, which is the whole of this
    /// variant: the actor is still there and answers the next call.
    #[error("the actor is serving and this reply was never answered")]
    Dropped,
    /// The incarnation that owed this answer is gone, and a supervisor is
    /// starting another. **Retrying is the move.**
    ///
    /// Carries the ending as an [`EndReason`] and not an [`Ended`], because an
    /// [`Ended::Observer`] holds an `Arc<dyn Error + Send + Sync>` and a trait
    /// object is not data — that type's own documentation carries the argument.
    ///
    /// **The sentence renders the ending, and for one wave it did not.** On the
    /// served path this condition read `"restarting: "` followed by the whole of
    /// `Ended`'s `Display`, so a peer logging `err.to_string()` learned *why* the
    /// incarnation died. When the prefix became this variant the `#[error]` text
    /// named no field, and the same peer got a fixed sentence about an ending the
    /// value was carrying all along. `{reason}` is what puts it back, through
    /// [`EndReason`]'s own `Display`.
    #[error("the incarnation that owed this answer is gone and another is coming: {reason}")]
    Restarting {
        /// The ending that caused the restart.
        ///
        /// # Four of [`EndReason`]'s shapes cannot appear here
        ///
        /// A restart is granted by `turns.rs`'s `retryable`, and it answers
        /// `false` for [`Ended::Stopped`], [`Ended::Vanished`], [`Ended::GaveUp`]
        /// and an [`Ended::Observer`] that declared itself fatal. `Lent::ending`
        /// settles `Standing::Restarting` only behind that call, and it is the
        /// only producer of that standing — so
        /// [`Stopped`](EndReason::Stopped), [`Vanished`](EndReason::Vanished),
        /// [`GaveUp`](EndReason::GaveUp) and
        /// [`Observer { fatal: true }`](EndReason::Observer) are representable in
        /// this field and unreachable in it. What is left is a fault, a dead feed,
        /// and a non-fatal observer veto: the three endings another incarnation
        /// can be expected to survive.
        ///
        /// It is an `EndReason` rather than a narrower type because the narrower
        /// type would be a second vocabulary for one ending — the cost this whole
        /// enum exists to refuse — and because `retryable` is a function anybody
        /// may change. A caller that matches all six arms stays correct the day it
        /// does.
        reason: EndReason,
    },
    /// The actor is no longer running. **Stop calling.**
    ///
    /// There is no next incarnation: the loop is finished, or a supervisor gave
    /// up, or the actor was aborted or destroyed.
    #[error("the actor is no longer running")]
    Gone,
    /// A [`QueueTimeout`](crate::QueueTimeout) budget expired while the message
    /// waited. It **never started**, so the actor is untouched and still running.
    ///
    /// Has no **reachable** producer, which is narrower than *no producer* and is
    /// the true claim: `ask_catching` builds it for an expired budget, and no
    /// local `Origin` carries a budget for that arm to fire on. See this enum's
    /// own documentation for what reports the expiry on the served side instead,
    /// and why the variant is kept anyway.
    #[error("the queueing budget expired before this turn started: nothing ran")]
    Expired,
}
