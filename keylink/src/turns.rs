//! The turn model, and the local actor that never leaves the process.
//!
//! This is the base case. RFC-04 §5.4 states the reason plainly:
//!
//! > An actor framework whose actors must cross a network to be called is not an
//! > actor framework.
//!
//! [`LocalActor`] owns one value on one task and hands it `&mut self`, one turn
//! at a time. No registry, no `ActorId`, no schema hash. A local message is a
//! **value of the service's own generated enum**, carrying that method's
//! arguments and that method's own reply channel.
//!
//! Generated code wraps this in a typed `Handle`, whose methods are the ones
//! written in `service!`. The plumbing is here rather than in the macro so that
//! the guard, the reply channel and the panic policy exist **once**, where they
//! cannot be forgotten by a kind added later.
//!
//! # The mailbox carries a value, not a closure
//!
//! It used to carry `Box<dyn FnOnce(&mut A, &mut [Box<dyn Observer<A>>]) ->
//! Pin<Box<dyn Future<..>>>>`: every message was a closure that had already
//! captured its own reply channel, so the loop could see that a turn was
//! happening and never what it was or who was owed an answer.
//!
//! Now the loop holds both. [`Turn`] is what a generated mailbox enum
//! implements, [`turn`] is the one function each of its arms calls, and
//! [`Reply<T>`](Reply) is the answer channel the loop is **handed** rather than
//! one sealed inside a closure. Three things follow that could not be built
//! against the closure:
//!
//! - a turn can be answered with something the handler did not return — a
//!   panic, an expired queue budget, an observer's veto — from one place;
//! - the answer can outlive the turn, which is what a deferred reply is;
//! - the loop can be given a second input beside the mailbox, because the
//!   mailbox's item is a value it can match on rather than a call it must make.
//!
//! # One loop, two inputs, both paths
//!
//! The third of those is [`Feeds`], and building it is what finally made the loop
//! singular. There were two loops until Wave 3 — this one, and a second that
//! `service!` wrote into every crate that declared an actor — and Wave 1b had
//! already reduced them to two loops around *one* dispatch. That was enough for
//! the dispatch's own policy to be shared and not enough for anything a loop
//! owns: a keyed actor had no observers and no `finish`, because those live in a
//! loop.
//!
//! A second input forced the question, because a `select` written twice is a
//! `select` that will disagree with itself. So `run_loop` is the loop, it takes
//! its mailbox through a private [`Inbox`] trait so that a `LocalActor`'s channel
//! and a keyed actor's can both reach it, and generated code calls
//! [`run_keyed`] instead of writing a loop at all.
//!
//! A feed item and a mailbox message are both a [`Turn`], dispatched at one call
//! site, so the panic guard, the observer chain and a durability adapter's commit
//! reach a feed item because there is nowhere else for it to go.
//!
//! One boxed closure survives and it is deliberate: [`read`](LocalActor::read).
//! That one takes an arbitrary caller closure rather than a declared method, so
//! there is no enum a macro could generate for it. What it costs is smaller in
//! kind rather than in bytes: a *synchronous* `Box<dyn FnOnce(&A)>` — no future
//! to build, no `&mut A`, no observer chain — where the old turn was a boxed
//! closure whose whole job was to allocate a second box for a future.
//!
//! # Two boxes, and both are about an idle actor
//!
//! The mailbox slot is a `Box<Message<..>>` and the turn's future is boxed at
//! this loop's await. Neither is a micro-optimisation and neither is optional,
//! because both things are sized by **the caller's** service rather than by
//! keylink: a message is as big as its largest variant, and a turn's state
//! machine is as big as whatever a handler holds across an await.
//!
//! Unboxed, they land in the two allocations an actor holds while it is doing
//! nothing at all — the channel's eagerly allocated block of 32 slots, and the
//! task's own future — so ten thousand idle actors would be paying for a
//! kilobyte of arguments none of them has been sent. Boxed, both are pointers
//! while the actor is idle and the bytes exist only while a message does.
//! `tests/actor_memory.rs` measures exactly that and asserts the *slope*: an idle
//! actor of a service with a kilobyte more declared arguments costs the same.
//!
//! The bill is one heap allocation per message for the turn, on a path that
//! already allocates the slot. That is the trade, and it is the same one
//! `Mailbox`'s documentation makes one layer out.
//!
//! # Two types, two error types, on purpose
//!
//! A local call loses its value more than one way, and [`Unanswered`] names each
//! one rather than folding them. It is not [`Error`](crate::Error) for a separate
//! reason: RFC-04 §5.2 and §5.5 reject a handle that is the same type locally and
//! remotely, because such a handle must either claim failures the local case
//! cannot have, or hide failures the remote case does.
//! `Error::SchemaMismatch` has no meaning between two objects in one process.
//!
//! It was a **unit struct** whose one sentence was *the actor is gone*, and every
//! other loss arrived wearing that name: a reply a live actor threw away, an
//! incarnation a supervisor was already replacing, a turn an [`Observer`] refused.
//! Each of those leaves the actor answering the next message, so the sentence was
//! false for them and a caller acting on it stopped calling an actor that was
//! there. The variants carry that argument one at a time, and
//! [`LocalActor::ask`]'s `# Errors` is the list.
//!
//! What the type reports is still narrower than the ways a call can fail, and the
//! difference is worth having in front of you. A call can also panic
//! ([`LocalActor::ask`] resumes it), never return ([`Handle`] documents the
//! self-call deadlock), or — only for [`notify`](LocalActor::notify) — return `Ok`
//! for a message that is then dropped unrun, because `Ok` there means "queued" and
//! nothing else.
//!
//! It is also why [`ended`](LocalActor::ended) exists rather than one more error
//! variant. An [`Observer`] that refuses a turn is named in the error its caller
//! gets — [`Withheld::Refused`] carries the observer's name — and what is
//! *not* there is that observer's own error, because it is an
//! `Arc<dyn Error + Send + Sync>` and a trait object is not data.
//! [`Withheld::Gone`] means "gone" and must not be stretched to mean "gave up on
//! durability", so the second question is asked separately and answered by
//! [`Ended`].
//!
//! The method names and success types match the remote client for `rpc` and
//! `tell`. They do not for the other three: `blob` hands back a
//! [`BlobId`](crate::BlobId) where the client hands back a
//! [`BlobHandle`](crate::BlobHandle), and `send` and `recv` have no local method
//! at all. Moving an actor across that boundary is a mechanical edit for two
//! kinds out of five, and a compile error that needs a decision for the rest —
//! which is the honest version of what §5.3 promised.
//!
//! # Blocking work in a handler, and which thread pays for it
//!
//! A turn is the unit of serialization, so a handler that spends 400 ms holds its
//! actor for 400 ms **however it spends them**. That much is the actor model
//! working. What the handler chooses is who *else* pays.
//!
//! `tokio::task::spawn_blocking(..).await` moves the work to the blocking pool and
//! yields: the turn is occupied, and the runtime's worker thread is free. Blocking
//! the thread **synchronously** occupies both — the turn *and* a worker — so
//! unrelated tasks on that runtime stall. With `W` workers it takes `W` concurrent
//! synchronous blocks to stop the runtime outright and fewer to degrade it; on a
//! current-thread runtime one is enough to stop everything. `blocking::Node` runs a
//! multi-threaded runtime on purpose, so a program built on it degrades rather than
//! stops — the rule is the same and the blast radius is not.
//!
//! **Nothing reports this.** No error is produced, no lint fires, and the actor is
//! correct from its own point of view; the damage lands on tasks that have nothing
//! to do with it. That is the profile of a defect that ships, which is the reason
//! this paragraph exists at all.
//!
//! `keylink-persist/src/file.rs` is the worked example, on both of its blocking
//! calls — a lock-file open, and a write plus `sync_all` plus rename — and it is the
//! reference for what belongs in the pool: bounded work that returns. For work that
//! never returns, a parked actor loop or a blocking `recv` of your own, the pool is
//! the wrong tool and a dedicated `std::thread` is the right one, because a pool
//! slot never given back is a slot every other caller waits for.
//!
//! `spawn_blocking` does **not** make one actor concurrent: the next call still
//! waits for this turn to return. That is the turn doing its job rather than a cost
//! of the pool, and it is the belief worth checking, because moving work off the
//! worker feels like parallelism and is not. Both halves are guarded in
//! `tests/blocking_work.rs`.
//!
//! # A panic reaches the caller of an `ask`
//!
//! If the handler panics, the panic is caught so the **actor survives**, and
//! then resumed in the caller — the same payload, close to the way an ordinary
//! function call would deliver it.
//!
//! Both halves matter. Catching keeps one bad message from costing every later
//! one. Resuming keeps [`Unanswered`] free of **faults**: every variant it has is
//! about delivery or about the actor, and none of them is "the actor is fine and
//! your message exploded". [`TurnLost::Faulted`] is the variant that says exactly
//! that, and it is the one difference between the two enums — it exists on the
//! catching path because that path hands the panic back as a value instead of
//! resuming it.
//!
//! Two structural exceptions. [`notify`](LocalActor::notify) has no channel to
//! carry a panic, so it catches and drops one. And a caller that dropped its
//! future before the turn finished is not there to receive one. In both the
//! panic hook has already run at the fault, which is where the backtrace is.
//!
//! The remote path does the opposite with the same catch — see
//! [`LocalActor::ask_catching`] — because a panic must not travel to another
//! machine as an unwind.
//!
//! # What is not guarded
//!
//! The actor's task is detached, so nothing observes a panic that escapes the
//! loop rather than a turn. Every path library code controls is inside
//! `guarded`, including the closure that produces the turn's future. What is
//! left is a user `Drop` — on the state when the actor ends, or on a reply value
//! dropped here because the caller went away. Either kills the loop, and every
//! handle then reports [`Unanswered`]: the same lie the panic policy exists
//! to remove, through a narrower door. And `panic = "abort"` defeats all of it.
//!
//! [`Handle`]: crate::LocalActor

use std::cell::Cell;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use n0_future::task::JoinHandle;
use n0_future::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};

// By module path and not from the crate root: Wave 2's review found `guarded` and
// `catching` re-exported `pub` with no consumer outside this crate, so they are
// `pub(crate)` now and the root no longer names them.
use crate::panic::{catching, guarded};
use crate::parking::{Parking, Verdict};
use crate::{
    Body, Context, EndReason, Ended, Observer, Origin, Outcome, Panic, Queueing, RemoteError,
    Serving, Stop, TurnInfo, TurnLost, TurnOutcome, Withheld,
};

/// Messages that may wait ahead of the one being handled.
///
/// One, matching the registry's mailbox in `actor.rs`, and for a related reason
/// that is worth spelling out because the bounding mechanism is **not** the
/// same one.
///
/// Over the network, QUIC bounds in-flight calls: a peer cannot have more than
/// `max_concurrent_bidi_streams` messages outstanding, so the queue is bounded
/// by the transport whatever this number is. In-process there is no transport
/// and no such bound — a caller can spawn ten thousand tasks that each send.
///
/// So here the mailbox itself is what waits: `send` on a full channel parks the
/// calling task, which is where a slow actor should be felt.
///
/// Be honest about what that buys. It is **not** a bound on memory: every
/// parked sender still holds its message and its arguments alive, so cost grows
/// with waiting callers whatever this number is — the queue moves into the
/// channel's wait list, which has no length anybody can read. What capacity 1
/// does buy is that a caller cannot get further ahead of the actor than one
/// message, so a caller that measures its own latency sees the actor's.
///
/// That reason survives on its own, and it is now the **only** one holding this
/// value at 1. Two others stood here and neither survived being measured.
///
/// # The deleted argument, and the measurement that deleted it
///
/// This paragraph read: a `LocalActor` is addressed from whatever task holds the
/// handle, including its own turn, so *"a deeper mailbox turns a documented
/// always-deadlock into an intermittent one. An intermittent deadlock is not a
/// slower program; it is a different program in production than in the tests."*
///
/// Tested directly. Control first, required green: `cargo nextest run --test
/// cycles`, 8 of 8 passing. Then `scripts/break-once.sh` raising the depth to
/// **32** — thirty-two and not two, so that "the mailbox happens to be full" is
/// unreachable in those tests. Result: 6 passed, 2 failed, and **every deadlock
/// guard still fired at depth 32**. The two failures are the only assertions that
/// are *about* depth-1 permit arithmetic, and the second of them settles it:
/// assertions run in order, and it panicked at the "A's mailbox is full" line
/// which sits *after* the ring assertion — so the ring still hung at depth 32,
/// with no contention anywhere. Reproduced independently on a separate worktree,
/// same two test names, same two panic lines.
///
/// The hazard is on the **answer**, not on the permit. `macros.rs` names four
/// self-call cases; three of them send into the free slot and then wait forever
/// on an answer only the blocked turn could produce, so depth moves where the
/// hang happens and nothing else. And the one load-dependent case runs the other
/// way: a self-`tell` is *already* intermittent at depth 1, because it succeeds
/// only if nobody else took the permit first, so a deeper mailbox makes that
/// block **less** often. Depth 1 is not the deterministic choice for the one case
/// that varies; it is the choice that maximises the chance of the block.
///
/// The remaining hazard is closed by a mechanism rather than by a number: a
/// re-entrant call is refused at any depth, with [`Unanswered::Reentrant`] or
/// its twin [`NotQueued::Reentrant`] according to which of the two questions the
/// door asked. See [`LocalActor::send`].
///
/// # A default where it used to be a value, and the name says which
///
/// `DEFAULT_MAILBOX_DEPTH` in `registry.rs` is a default and this is now one too,
/// with [`LocalActor::spawn_deep`] and [`LocalActor::spawn_supervised_deep`] as
/// the doors that override it. [`NonZeroUsize`] rather than `usize`, matching
/// [`RegistryBuilder::mailbox_depth`](crate::RegistryBuilder::mailbox_depth) exactly:
/// `mpsc::channel(0)` panics, and `representability.rs` pins both signatures so a
/// widening to `usize` — or to an `Option` whose `None` meant unbounded — is
/// `E0308` rather than a review comment.
///
/// **The default stays 1, and moving it costs more than it buys.** Measured at
/// depth 2 across the suite: exactly **two** tests fail, both the permit
/// arithmetic in `tests/cycles.rs`, and `--test defer --test persist` runs 31 and
/// passes 31 — the `parked()` helper is a 500-iteration retry loop, so it is the
/// loop and not the permit that makes those suites deterministic. So the cost of
/// moving it is two test edits and four doc comments going stale unchecked, and
/// the benefit is nothing the knob does not already give.
///
/// # CAUTION: raising this raises the shutdown budget owed
///
/// `registry.rs`'s warning carries to this door verbatim: a deeper mailbox holds
/// more turns that a shutdown must still drain, by up to N turns, and **nothing
/// can check that for you**. Under-draining is a defect rather than a slow
/// shutdown.
const DEFAULT_LOCAL_MAILBOX_DEPTH: NonZeroUsize = NonZeroUsize::MIN;

/// The whole mailbox policy an actor gets when its spawn names none.
///
/// Derived from the depth above rather than repeating the number, so the two
/// cannot drift. **[`Backpressure::Park`], and that is the half worth pinning**:
/// a default that shed would turn every existing caller's full mailbox from a
/// wait into a dropped message, silently, at the next release.
/// `representability.rs`'s `no_local_mailbox_can_be_asked_to_be_unbounded` asserts
/// the arm in a `const` block, so changing it here is a compile error there.
pub(crate) const DEFAULT_LOCAL_BACKPRESSURE: Backpressure =
    Backpressure::Park(DEFAULT_LOCAL_MAILBOX_DEPTH);

/// The observers an actor lends to one turn.
///
/// An alias so the generated [`Turn`] impl can name the parameter without
/// spelling `[Box<dyn Observer<A>>]` in every expansion, and so a later wave
/// that changes what a turn is lent changes one line here rather than one line
/// per `service!` in the world.
#[doc(hidden)]
pub type Hooks<A> = [Box<dyn Observer<A>>];

/// One message in a generated mailbox, and what it does to the state.
///
/// Implemented **only** by generated code, because only generated code can
/// match on a per-service enum: the variants are the service's own methods.
/// Each arm calls [`turn`] exactly once, which is where the guard, the observer
/// chain, the queue deadline, the progress marks and the reply all live — so an
/// arm can get its *arguments* wrong and cannot get the turn's policy wrong.
///
/// `Send + 'static` and nothing else. The enum is local-only and never
/// serialized, which is the whole reason it can carry a `BodyReader` and a
/// `oneshot::Sender` at all — the wire types are `Request` and `Response`, and
/// this is neither.
#[doc(hidden)]
pub trait Turn<A>: Send + 'static {
    /// Run this message against the state, then the observers.
    ///
    /// `Some` ends the actor, and is the reason [`Ended`] can name the observer
    /// that refused rather than only saying the mailbox closed.
    fn run(self, state: &mut A, hooks: &mut Hooks<A>)
        -> impl Future<Output = Option<Ended>> + Send;
}

/// Where one turn's answer goes.
///
/// A generated mailbox variant carries one of these for its own method's return
/// type. For a method without `#[defer]`, generated code does exactly two things
/// with it — puts it in a variant, and hands it to [`turn`] — and never builds one,
/// because keylink makes the pair. `Answered`, the four-state answer this channel
/// carries, therefore stays `pub(crate)`, and its fourth state was additive to a
/// type nothing outside can match on: a prediction `Answered::Faulted` tested by
/// landing without moving one generated line.
///
/// Contrast the alias this replaces: `Answer<Resp>` was a transparent
/// `oneshot::Sender<..>`, so tokio's channel type was in keylink's public API
/// through a `#[doc(hidden)]` name. A newtype costs one field and buys the
/// promise back.
///
/// # A `#[defer]` method's handler is handed one — an attribute, not a sixth kind
///
/// The distinction is not pedantry. A sixth method *kind* was refused, and it stays
/// refused: a deferred method's wire types, client methods, blocking method and local
/// `Handle` method are byte-for-byte an ordinary `rpc`'s, and only three of the
/// thirteen helper macros in `macros.rs` know the attribute exists. What `#[defer]`
/// changes is who holds this channel during the turn.
///
/// ```ignore
/// keylink::service! {
///     service Orders;
///     #[defer]
///     rpc Place(order: Order) -> Ack;
/// }
///
/// async fn place(&mut self, cx: Caller<'_>, reply: Reply<Ack>, order: Order)
///     -> Replied<Ack>
/// {
///     match self.can_fill(&order) {
///         true  => reply.send(self.fill(order)),          // answered now
///         false => {                                      // answered later
///             let (deferred, replied) = reply.later();
///             self.waiting.push((order, deferred));
///             replied
///         }
///     }
/// }
/// ```
///
/// [`Replied<T>`](Replied) is what makes "forgot to answer" a compile error:
/// there is no other way to make one, `Reply` is not [`Clone`], and both doors
/// consume it. See [`Replied`] for what that does and does not prove.
///
/// # `send` does not send. It hands the channel back to the turn.
///
/// This is the load-bearing detail, and it is the reason a deferred method does
/// not quietly cost a service its durability. [`send`](Self::send) returns the
/// channel and the value inside the [`Replied`] token, and [`turn_deferred`]
/// answers **after** the observer chain — so an answer given inside the turn is
/// committed exactly as an ordinary method's return value is, and an observer that
/// refuses withholds it. If `send` wrote to the channel directly, a durable actor
/// would hand its caller a value the store never accepted, which is the one
/// property RFC-06 exists for.
///
/// [`later`](Self::later) is the other case, and it escapes *this* turn — not
/// every turn. This turn's commit says nothing about the parked answer, because
/// this turn did not produce it; the turn that eventually calls
/// [`Deferred::send`] does, and that turn's commit gates it. See `parking.rs`,
/// which is where the two halves of `#[defer]` meet: the channel outlives the
/// turn that received it, and the value waits for the turn that answered it.
///
/// # Two shapes of destination, because only generated code can name a `Response`
///
/// A deferred method reached from a peer has to put its value inside the service's
/// own `Response` before it crosses the served reply channel — and the parked
/// answer arrives when no generated code is running. So the wrapping goes *into*
/// the channel, through [`wrapping`](Self::wrapping), and that is the one place
/// this type is not a bare `oneshot::Sender`.
///
/// # No door in from outside, held rather than claimed
///
/// The claim is that a downstream crate cannot make a reply channel, so every
/// answer a turn gives is one keylink is holding the other half of. It cannot be
/// a probe in `representability.rs`, because in there `channel` is reachable —
/// the same reason `Origin`'s privacy is held by a doctest, which compiles as its
/// own crate:
///
/// ```compile_fail
/// // Fails with `E0624: associated function `channel` is private`, and the
/// // tuple-struct form with `E0603`, the field not being public either.
/// //
/// // The codes are named HERE and deliberately NOT on the fence. rustdoc does
/// // not check the code after `compile_fail`, so an annotated code asserts
/// // verification it does not perform — this project has shipped four guards
/// // that were satisfiable without proving their claim, and a fence reading
/// // `compile_fail,E0624` is how the fifth would look.
/// //
/// // So this asserts exactly one thing: it does not compile. What stops it
/// // passing because the NAME vanished rather than because the privacy held is
/// // that keylink itself calls `Reply::channel` on two paths, which is a
/// // compile error in the library the moment it is renamed.
/// let (_reply, _answer) = keylink::Reply::<u64>::channel();
/// ```
pub struct Reply<T> {
    to: Answerer<T>,
    /// The idempotency marker this answer settles, when the call came from a
    /// peer. Held **here** rather than in [`turn`] so that a reply which leaves
    /// its turn takes the mark with it: the mark and the answer are one event,
    /// and a parked reply is the case that proves it. Without this, a deferred
    /// served call would settle its key `Interrupted` on success and refuse every
    /// retry of a call that worked.
    settling: Option<crate::middleware::Progress>,
}

/// Where a [`Reply`] actually writes.
///
/// Two arms and not one, because a deferred method's value may have to become
/// something else on the way out — see [`Reply::wrapping`]. The box is paid only
/// by that case; every ordinary turn is [`Direct`](Self::Direct).
enum Answerer<T> {
    /// A `oneshot` keylink owns the other half of.
    Direct(oneshot::Sender<Answered<T>>),
    /// A channel for some other type, plus how a `T` reaches it.
    ///
    /// `FnOnce` and boxed, because the conversion is written by generated code in
    /// the caller's crate and has to outlive the turn that installed it. It is
    /// `Send` and deliberately not `Sync`, which is what a state parking one of
    /// these stays: `A: Sync` is the bound this whole design keeps out.
    Wrapped(Box<dyn FnOnce(Answered<T>) + Send>),
}

impl<T> Reply<T> {
    /// The pair. `pub(crate)`: keylink builds every reply channel, on the two
    /// paths that own the other half.
    pub(crate) fn channel() -> (Self, oneshot::Receiver<Answered<T>>) {
        let (reply, answer) = oneshot::channel();
        (
            Self {
                to: Answerer::Direct(reply),
                settling: None,
            },
            answer,
        )
    }

    /// Answer now — meaning *this turn's* answer is this value.
    ///
    /// The channel is handed back inside the [`Replied`] token rather than
    /// written to, so [`turn_deferred`] answers after the observer chain. See
    /// this type's own documentation for why that is not a detail.
    #[must_use = "the token is the proof that this reply was answered, and a \
                  `#[defer]` handler has to return it"]
    pub fn send(self, value: T) -> Replied<T> {
        Replied(Settled::Now(self, value))
    }

    /// Park the channel and answer from a later turn.
    ///
    /// The [`Deferred<T>`](Deferred) is the answer channel, free of this turn.
    /// Keep it — in the state, most usefully — and [`send`](Deferred::send) on it
    /// whenever the answer exists. Dropping it tells the caller
    /// [`Withheld::Dropped`]: *the actor is serving and this reply was never
    /// answered*, which is true of a handler that decided there is no answer. An
    /// actor that **ends** with a reply still parked in its state answers
    /// [`Withheld::Gone`] instead, and the caller can tell the two apart.
    #[must_use = "the token is the proof that this reply was parked, and a \
                  `#[defer]` handler has to return it"]
    pub fn later(self) -> (Deferred<T>, Replied<T>) {
        (Deferred(self), Replied(Settled::Later))
    }

    /// Turn a channel for `U` into one for `T`, given how a `T` becomes a `U`.
    ///
    /// `#[doc(hidden)]`: the one caller is a generated served arm for a
    /// `#[defer]` method, which is the only code that can name both the method's
    /// return type and the service's `Response`. The mark stays with the outer
    /// channel, so an answer is settled once however many wrappings it crossed.
    #[doc(hidden)]
    pub fn wrapping<S>(self, wrap: impl FnOnce(S) -> T + Send + 'static) -> Reply<S>
    where
        T: Send + 'static,
        S: 'static,
    {
        Reply {
            to: Answerer::Wrapped(Box::new(move |answered| self.answer(answered.map(wrap)))),
            settling: None,
        }
    }

    /// Which idempotency key this answer settles. Installed by the turn, which is
    /// the only thing holding the origin.
    fn settling(mut self, progress: Option<crate::middleware::Progress>) -> Self {
        self.settling = progress;
        self
    }

    /// Answer, if anybody is still listening.
    ///
    /// A dropped receiver is not an error here and never was: a local `tell`
    /// drops it deliberately, and a caller that gave up on its future is gone.
    /// Both mean the same thing to a turn that has already run.
    ///
    /// **The `completed` mark is here and nowhere else.** `Ran` and nothing else,
    /// spelled as a positive match: a `Faulted` turn is not a completed one, and
    /// an `Expired` or `Refused` one produced nothing to record. Marking where a
    /// method returns instead would settle a key as an observer's veto and replay
    /// that veto to every retry, for work whose effect was thrown away — RFC-06
    /// §13.3.
    fn answer(self, answered: Answered<T>) {
        if let (Some(progress), Answered::Ran(_)) = (&self.settling, &answered) {
            progress.completed();
        }
        match self.to {
            Answerer::Direct(sender) => {
                let _ = sender.send(answered);
            }
            Answerer::Wrapped(wrapped) => wrapped(answered),
        }
    }
}

/// Proof that a handler answered its reply channel, or parked it.
///
/// There is no other way to make one. [`Reply`] is not [`Clone`] and its only two
/// doors — [`send`](Reply::send) and [`later`](Reply::later) — each consume it and
/// produce exactly one of these, so a `#[defer]` handler declared to return a
/// `Replied<T>` cannot return without having answered or explicitly parked. This
/// is Pekko's enforced-replies shape, and it is why deferral is safe to add as an
/// attribute where a sixth method kind was refused.
///
/// # What it proves, and the one thing it does not
///
/// It proves the channel was **consumed**, which is stronger than a lint and
/// weaker than "the caller got an answer". A handler that parks a reply and then
/// drops the [`Deferred`] has satisfied this type and told its caller
/// [`Withheld::Dropped`] — correctly, since nothing answered and the actor is
/// there. What no type can force is that somebody eventually calls
/// [`Deferred::send`]; what keylink guarantees is that the caller is never left
/// waiting on a channel nobody holds, and is never told to stop calling an actor
/// that is still serving.
///
/// It also does not survive a panic. A handler that panics before consuming its
/// `Reply` drops it inside the unwinding future, so the caller is told `Dropped`
/// rather than being handed the panic the way [`LocalActor::ask`] hands one on
/// every other kind. The actor still survives, the observer chain still reads
/// [`TurnOutcome::Panicked`], and the panic hook already ran
/// at the fault — which is the trade [`notify`](LocalActor::notify) documents for
/// a `tell`, arriving here for a structural reason instead of a policy one. The
/// surviving actor is what the answer names: `Dropped` is the same sentence for
/// the sanctioned drop above and for this unwind, because the caller's next move
/// is the same in both.
///
/// # Generic, against the sketch, and for the reason in [`Reply`]
///
/// Issue #3 writes it as a bare `Replied`. It carries `T` because
/// [`Reply::send`] hands the channel and the value *back* to the turn rather than
/// writing them, which is what keeps an answer given inside a turn under the
/// observer chain's commit. A payload-free token would have forced `send` to write
/// immediately, and a durable actor's caller would get values the store refused.
///
/// # The guard, and exactly which half of it each piece holds
///
/// **The positive fence is the load-bearing one, and that is not the arrangement a
/// reader would expect.** It compiles a handler written the way `#[defer]` means it
/// to be written, so it fails if the *generated signature* stops demanding a token
/// — a defer arm that forgot the `Reply` argument, or returned `$ret` instead of
/// `Replied<$ret>`, is what would make forgetting to answer legal, and this is what
/// catches it. Red-tested: with the arm's `Output` changed to `()`, this fence goes
/// red with `E0271`.
///
/// ```
/// use keylink::{Caller, Reply, Replied};
///
/// keylink::service! {
///     service Orders;
///     #[defer]
///     rpc Place(size: u64) -> u64;
/// }
///
/// struct Book;
///
/// impl orders::Actor for Book {
///     async fn place(&mut self, _cx: Caller<'_>, reply: Reply<u64>, size: u64)
///         -> Replied<u64> {
///         reply.send(size)
///     }
/// }
/// ```
///
/// The `compile_fail` twin below shows the mistake, and **it proves less than it
/// looks like it does.** Say so, because a reader will otherwise take it for the
/// guard: an empty body not being a `Replied<u64>` is rustc's `E0308`, and it holds
/// whatever keylink does. Red-tested by the same break as above, and it stayed
/// **green** — the impl then mismatched the trait's `Output` instead, so it failed
/// for an unrelated reason while the obligation it claims to pin was gone. That is
/// precisely the shape this project keeps catching, and the honest response is to
/// name which fence is the guard rather than to keep two and believe both.
///
/// ```compile_fail
/// use keylink::{Caller, Reply, Replied};
///
/// keylink::service! {
///     service Orders;
///     #[defer]
///     rpc Place(size: u64) -> u64;
/// }
///
/// struct Book;
///
/// impl orders::Actor for Book {
///     // Fails with `E0308`: expected `Replied<u64>`, found `()`. Codes in prose
///     // and never on the fence — rustdoc does not check them.
///     async fn place(&mut self, _cx: Caller<'_>, _reply: Reply<u64>, _size: u64)
///         -> Replied<u64> {}
/// }
/// ```
///
/// The other half — that a `Replied` cannot be obtained *twice* from one `Reply`,
/// which is what makes "answered once" the only reachable shape — is
/// `representability.rs`'s coherence probe on [`Reply`], [`Deferred`] and this type
/// not being [`Clone`]. That one is falsifiable by adding a derive, and it is.
pub struct Replied<T>(Settled<T>);

/// What a [`Replied`] token carries back to the turn.
enum Settled<T> {
    /// Answered inside the turn: the channel and the value, for the turn to send
    /// once the observers have accepted it.
    Now(Reply<T>, T),
    /// Parked. The channel left with a [`Deferred`] and this turn owes nothing.
    Later,
}

/// A reply channel that outlived its turn.
///
/// Hold it anywhere — the state is the useful place — and answer whenever the
/// answer exists. `Send` and `'static`, so it can also go to another task; not
/// `Clone`, because there is one answer.
///
/// Dropping it without answering is not an error and not silent: the caller's
/// wait ends with [`Withheld::Dropped`], and that answer says the actor is still
/// there. An actor that **ends** with replies parked in its state ends the same
/// waits with [`Withheld::Gone`] — the same silence on the channel, two answers,
/// because the callers' next moves differ.
///
/// # This is how a handler calls another actor — but parking is only half of it
///
/// A handler that awaits another actor whose handler calls back deadlocks, and
/// the generated `Handle` documents why. This type is the way out, and the way
/// out is precise: **returning from the turn** is what breaks the cycle, because
/// the loop goes back to polling and can take the peer's message and run it.
/// [`later`](Reply::later) is what makes returning possible while a caller is
/// still owed an answer — so park the reply, hand the outbound call to a spawned
/// task, and return.
///
/// Parking on its own fixes nothing. A handler that calls `later` and then awaits
/// the peer *without returning* hangs exactly as before: the turn has not ended,
/// so the loop is not polling, so the park bought nothing.
/// `tests/cycles.rs` is a pair of twins that differ in the `tokio::spawn` alone
/// and disagree, which is what makes that a mechanism rather than a suggestion.
///
/// And the mechanism is **not** about the mailbox permit. The permit is free in
/// the hanging case too — a running turn released its own when it took the message
/// it is running — so a cycle-breaker that only freed a permit would break
/// nothing. What was missing was a loop that would *run* the message.
///
/// # What a parked reply resolves to when a LATER turn faults
///
/// **Nothing. It stays parked.** Wave 2a gave `Answered` a `Faulted` state, and
/// the question it raises is whether a fault reaches replies that were parked before
/// it. It does not, and the reason is the same one that makes the whole panic policy
/// worth having: `Faulted` is *that turn's* answer to *that turn's* caller, and the
/// actor survives. So the state survives, so anything parked in it survives, and a
/// later turn can still answer it.
///
/// That is the right answer rather than a convenient one. A parked reply is a
/// promise the state made, and a message that exploded somewhere else is not
/// information about that promise — answering it `Faulted` would tell one caller
/// about another caller's fault, and `Panic` cannot be cloned to tell two.
///
/// Three ways a parked reply *does* end, all of them the state's own doing:
///
/// - somebody calls [`send`](Self::send) in a turn that is then **committed** —
///   the value waits for that turn's observer chain, which is the whole of
///   `parking.rs`;
/// - somebody drops it — including a turn that takes it out of the state and then
///   panics, which is the one case where a fault does resolve one, as `Dropped`.
///   A turn that *sent* it and then panicked is the same case now, for the same
///   reason: nothing that turn produced crosses;
/// - the actor ends, which drops the state and every reply in it, as `Gone`. An
///   observer refusing the faulted turn is how a fault reaches parked replies
///   *indirectly*, and it reaches them the same way `stop()` does.
///
/// The first two leave the actor serving and the third does not, which is the
/// whole difference between the two answers.
///
/// Pinned by `tests/defer.rs::a_reply_parked_before_a_fault_is_still_answered_after_it`.
#[must_use = "dropping a deferred reply answers the caller `Dropped`; that is \
              sometimes right, so say so with `drop`"]
pub struct Deferred<T>(Reply<T>);

impl<T> Deferred<T> {
    /// Answer the caller that has been waiting since the turn this was parked in.
    ///
    /// **The value is owed to the turn running now, and waits for it.** It is
    /// held until that turn's observer chain has accepted it, and then released
    /// with the same verdict the turn's own caller is answered by: an observer
    /// that refuses tells this caller [`Withheld::Refused`] naming the same
    /// observer, and a handler that panics after sending drops the channel, which
    /// is [`Withheld::Dropped`] — the actor caught that panic and is serving.
    ///
    /// That is not what deferral costs — it is what makes it free. `#[defer]`'s
    /// point is that the reply outlives the turn that **received** it, and it
    /// still does. What it no longer does is outrun the turn that **answered**
    /// it, which is what made *"an answered call is a stored call"* false for
    /// every deferred method against a durable actor: the caller held a receipt
    /// for a debit that was still ~7.6 ms of `fsync` away from the disk.
    ///
    /// # The one case that is still immediate, and it is not a hole
    ///
    /// A `Deferred` is `Send` and `'static`, so it can be sent from a spawned
    /// task, or reached from a [`read`](LocalActor::read) closure through
    /// interior mutability. Neither is inside a turn: no handler ran, no observer
    /// saw anything, and there is no commit for the answer to wait for. Those
    /// sends cross immediately, because withholding a value no turn produced
    /// would hang its caller for ever. `Persist`'s module header states it as the
    /// one thing left of the exception, and `tests/defer.rs::
    /// a_deferred_sent_from_another_task_is_not_gated_by_the_turn_that_spawned_it`
    /// pins it.
    ///
    /// # `T: Send + 'static`, which every method's return type already is
    ///
    /// The parked answer is type-erased into the turn's lot — one turn can hold
    /// answers of several types — so the value crosses into a
    /// `Box<dyn FnOnce(..) + Send>`. Every `$ret` a `service!` can declare is
    /// already `Send + 'static`, because [`turn`] demands exactly that of it.
    pub fn send(self, value: T)
    where
        T: Send + 'static,
    {
        let reply = self.0;
        crate::parking::park(Box::new(move |verdict| match verdict {
            Verdict::Committed => reply.answer(Answered::Ran(value)),
            Verdict::Refused { name } => reply.answer(Answered::Refused { name }),
            Verdict::ObserverPanicked { name } => reply.answer(Answered::ObserverPanicked { name }),
            // Dropped, not answered. `Answered::Faulted` needs the `Panic`
            // itself, there is one, and it is the turn's own caller's — see
            // `Verdict::Faulted`.
            Verdict::Faulted => drop(reply),
        }));
    }
}

impl<T> fmt::Debug for Deferred<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Deferred").finish_non_exhaustive()
    }
}

/// What the loop puts on the reply channel.
///
/// Five states rather than two, because "the budget expired while it queued",
/// "an observer refused the turn that ran", "an observer panicked while watching
/// it" and "the turn ran and exploded" are four different facts and a caller that
/// has to unwrap layers to tell them apart will get it wrong.
///
/// A **dropped** sender is not a sixth state here and cannot be: this type is what
/// the loop *sends*, and nothing is sent in that case. It is answered from the
/// actor's own standing instead — three more `TurnLost` variants, none of which
/// belong on this enum. See [`LocalActor::answered`].
///
/// # `Faulted` is a variant and not a `Ran(Err(..))`
///
/// It was `Ran(Result<T, Panic>)`, which made a fault a *shape* of a successful
/// turn: every consumer had to look one level in to find out whether the actor's
/// state was mutated or torn, and the two consumers that matter — the panic
/// policy on the local path and the wire rendering on the served one — both did
/// it differently. Flattening the two puts the distinction in the discriminant,
/// where a `match` has to answer it.
///
/// The `Panic` travels rather than being reduced to a message here. It is the
/// original payload, and `resume_unwind` on the original payload is the
/// difference between forwarding a panic and starting a second one — `panic.rs`
/// says so where the payload is kept.
pub(crate) enum Answered<T> {
    /// The turn ran and produced its value.
    Ran(T),
    /// The turn ran and panicked. The actor survived it.
    Faulted(Panic),
    /// A queueing budget expired while the message waited. Nothing ran.
    Expired,
    /// An observer refused a completed turn. It ran and was not committed.
    Refused {
        /// [`Observer::name`].
        name: &'static str,
    },
    /// An observer **panicked** watching a completed turn. It ran and was not
    /// committed.
    ///
    /// A separate state from [`Refused`](Self::Refused) for the reason
    /// [`Faulted`](Self::Faulted) is separate from `Ran`: the two withhold the
    /// value for the same reason and mean opposite things about the observer. A
    /// veto is a decision an author wrote down; a panic is the absence of one.
    ObserverPanicked {
        /// [`Observer::name`].
        name: &'static str,
    },
}

impl<T> Answered<T> {
    /// Carry the same answer about a different payload type.
    ///
    /// The three payload-free states pass through, which is the whole point: a
    /// `#[defer]` method reached from a peer has its value wrapped in the
    /// service's `Response` on the way out, and a fault, a refusal or an expired
    /// budget must **not** be rewritten as one. Only [`Reply::wrapping`] calls it.
    fn map<U>(self, wrap: impl FnOnce(T) -> U) -> Answered<U> {
        match self {
            Self::Ran(value) => Answered::Ran(wrap(value)),
            Self::Faulted(panic) => Answered::Faulted(panic),
            Self::Expired => Answered::Expired,
            Self::Refused { name } => Answered::Refused { name },
            Self::ObserverPanicked { name } => Answered::ObserverPanicked { name },
        }
    }
}

/// What an observer did to a turn that **completed**, in the two shapes the
/// caller is owed different sentences for.
///
/// The third element of the slot [`observed_turn`] folds used to be
/// [`Stop::is_fatal`](crate::Stop::is_fatal) — a `bool` beside the name — and a
/// panic has
/// no such claim to put in it. Writing `false` there spelled *the author said
/// another incarnation can help* for an author who said nothing at all, and it
/// spelled it the same way a deliberate [`Stop::new`](crate::Stop::new) does, so
/// no reader could tell the crate was guessing. The flag now rides on the arm
/// that has one.
///
/// [`Copy`], because the same value is read twice six lines apart: once to build
/// the [`Verdict`] a parked caller is released with, once to build the answer the
/// turn's own caller reads.
#[derive(Clone, Copy)]
enum ObserverVerdict {
    /// [`Observer::after_turn`] returned `Err`. The flag is
    /// [`Stop::is_fatal`](crate::Stop::is_fatal).
    ///
    /// **`Refused` and not `Vetoed`, which is the word the rest of the path
    /// stopped using.** This value becomes [`Verdict::Refused`] for a parked
    /// caller, [`Answered::Refused`] for the turn's own, and
    /// [`Withheld::Refused`] for a peer — one fact under one name, from the
    /// moment an observer states it to the sentence a caller reads.
    Refused {
        /// [`Observer::name`].
        name: &'static str,
        /// Whether the observer said another incarnation cannot help.
        fatal: bool,
    },
    /// [`Observer::after_turn`] panicked, in either of its two halves.
    ///
    /// No flag, because there is no claim to carry — [`Ended::Observer`]'s own
    /// `fatal` field says so where a caller meets it.
    Panicked {
        /// [`Observer::name`].
        name: &'static str,
    },
}

/// One turn and its observers — RFC-06 §2.6 in one place, so the two catching
/// paths cannot drift apart.
///
/// The observer chain runs **inside** the same [`guarded`] as the handler.
/// Outside it, an observer that panicked would unwind out of a detached task and
/// be swallowed, which is the property this module's header states about every
/// path library code controls.
///
/// # Three things out and not one `Answered`, because two turns read them
///
/// It used to fold the handler's `Result` and the observers' verdict into an
/// `Answered<T>` here. [`turn_deferred`] cannot use that: on a refusal the fold
/// discards the handler's value, and for a deferred method the handler's value is
/// the [`Replied`] token — the only thing holding the reply channel the refusal has
/// to be sent on. So the fold moved out to the two callers, which is where the
/// question "who is answered" belongs, and this function answers only "what
/// happened": the handler's outcome, the name of the observer that refused if one
/// did, and the [`Ended`] either of those implies.
///
/// # Except for the answers the turn parked, which are released HERE
///
/// The one thing this does decide, and it is here rather than in the three
/// callers for the reason the rest of it is not: a [`Deferred::send`] made during
/// the handler is an answer to somebody who is not this turn's caller, so no
/// caller-side fold can see it. It waits in the turn's lot — see `parking.rs` —
/// and is released after the observer chain with the [`Verdict`] read from the
/// same two facts the callers fold, six lines apart, so the two cannot disagree
/// about one turn.
///
/// Putting it here is also what gives [`feed_turn`] the same rule for free: a
/// feed item has no caller and no fold, and its `Deferred::send` is gated
/// anyway.
async fn observed_turn<A, T, F>(
    state: &mut A,
    hooks: &mut [Box<dyn Observer<A>>],
    method: &'static str,
    turn: F,
) -> (Result<T, Panic>, Option<ObserverVerdict>, Option<Ended>)
where
    A: 'static,
    // `Send` on `T` and not on `A`. The handler's value is held across the
    // observer awaits, so it has to cross them; the STATE is only ever borrowed
    // by an observer's own future, and whether that future holds it across an
    // await is the observer's choice. That is what keeps `A: Sync` out of this
    // design — see `Observer::after_turn`.
    T: Send + 'static,
    // No `'static` on `F`. It was there because the closure went INTO the
    // mailbox and had to outlive the sender; now it is built by a match arm and
    // consumed inside this call, so a closure holding a borrow of the message it
    // came out of is fine. `Send` stays: this future holds it across an await.
    F: for<'b> FnOnce(&'b mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'b>> + Send,
{
    // The handler runs inside a lot, so a `Deferred::send` in its body holds the
    // value here instead of writing it. The lot is a local of *this* function
    // rather than anything the loop owns, which is what makes "nothing is parked
    // at `finish`" structural — see `parking.rs`.
    let started = Instant::now();
    let mut parking = Parking::new(turn(state));
    let answer = guarded(&mut parking).await;
    let duration = started.elapsed();
    // Out before the observer chain reborrows the state: a `Vec<Parked>` names no
    // lifetime, while the future in `parking` holds the handler's `&mut A`.
    let parked = parking.take();
    drop(parking);

    // Copied out BEFORE the `Panic` moves into an `Answered`. Building the
    // reason afterwards is `E0382`, not a style preference.
    let panicked: Option<String> = match &answer {
        Err(panic) => Some(panic.message().to_owned()),
        Ok(_) => None,
    };
    let info = TurnInfo {
        outcome: match &panicked {
            Some(message) => TurnOutcome::Panicked { message },
            None => TurnOutcome::Completed,
        },
        method,
        duration,
    };

    // Two things, because the two audiences need different granularity: the
    // [`ObserverVerdict`] is what the caller is told, and the `Arc` is what the [`Ended`]
    // carries. `ObserverVerdict::Refused` holds [`Stop::is_fatal`] as well, because that is
    // the one thing about a refusal a supervisor reads and nothing else does.
    let mut withheld: Option<(ObserverVerdict, Arc<dyn Error + Send + Sync>)> = None;
    for observer in hooks.iter_mut() {
        let name = observer.name();
        // TWO catches, because `after_turn` has two halves that can panic and
        // `guarded` only covers one of them.
        //
        // `guarded` takes a BUILT future, so `guarded(observer.after_turn(..))`
        // evaluates `after_turn` first and catches only what happens after it
        // yields. And `after_turn` is a plain `fn` rather than an `async fn`
        // *precisely* to invite synchronous work before the future exists — so
        // the uncaught half is the half the signature advertises. A panic there
        // would unwind out of a detached task, `ended` would never be filled,
        // and `Ended` would report `Vanished` rather than naming the observer,
        // making the `Err(panic)` arm below dead for half the surface.
        //
        // The obvious repair — wrapping the call in an async block — does not
        // work here: the block would hold `&A` across an await, so it is `Send`
        // only when `A: Sync`, and keeping `A: Sync` out of this design is the
        // reason the signature is shaped this way at all. `catch_unwind` is
        // synchronous, holds nothing across an await, and costs nothing.
        // Shared reborrow first, because `move` would otherwise take the
        // `&mut A` the loop still needs.
        let shared: &A = &*state;
        let watching: &mut dyn Observer<A> = &mut **observer;
        let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            // Consuming the capture is what makes this `FnOnce`. Inference
            // settles on `FnMut` otherwise, and a future borrowing from an
            // `FnMut`'s capture cannot escape the closure body.
            let watching = watching;
            watching.after_turn(shared, info)
        }));
        let ran = match built {
            Ok(future) => guarded(future).await,
            Err(payload) => Err(Panic::caught(payload)),
        };
        match ran {
            Ok(Ok(())) => {}
            Ok(Err(stop)) => {
                let (reason, fatal) = stop.into_parts();
                withheld = Some((ObserverVerdict::Refused { name, fatal }, Arc::from(reason)));
                break;
            }
            // `Panic` implements `Display` and not `Error`, and is not `Sync`,
            // so it cannot fill an `Arc<dyn Error + Send + Sync>` directly.
            // `to_string()` is what `RemoteError::Failed` already does with one.
            //
            // A DIFFERENT arm and not the same one with a `false` in it. Both
            // withhold the value, for the two reasons RFC-06 §2.6 and §5.1 give
            // — a veto says *do not commit*, a crash says *nobody knows whether
            // it committed* — and the fold below is right to withhold on either.
            // What the caller is told is a third question, and it was answered
            // with the second for as long as one slot carried both.
            Err(panic) => {
                let reason: Box<dyn Error + Send + Sync> = panic.to_string().into();
                withheld = Some((ObserverVerdict::Panicked { name }, Arc::from(reason)));
                break;
            }
        }
    }

    // **Every answer this turn parked, released here, in send order.** Before the
    // fold below rather than after, so a turn that answers other callers and then
    // returns a value of its own is read in that order by everybody: what the
    // handler said during its body, then what it returned.
    //
    // Read from the same two facts the fold reads, six lines down, so a parked
    // caller and the turn's own caller cannot be told two different things about
    // one turn. A fault outranks a refusal here for the reason it does there: the
    // ending names the fault, and there is nothing committed either way.
    let verdict = match (&panicked, &withheld) {
        (Some(_), _) => Verdict::Faulted,
        (None, Some((ObserverVerdict::Refused { name, .. }, _))) => Verdict::Refused { name },
        (None, Some((ObserverVerdict::Panicked { name }, _))) => Verdict::ObserverPanicked { name },
        (None, None) => Verdict::Committed,
    };
    for release in parked {
        release(verdict);
    }

    match (withheld, panicked) {
        // Completed and accepted, or faulted and accepted — which is an observer
        // that read `TurnOutcome::Panicked` and chose to continue.
        (None, _) => (answer, None, None),
        // Panicked and withheld: the fault is still the handler's outcome and the
        // actor ends naming the fault rather than the observer. A durability
        // adapter refuses a panicked turn, so reporting the adapter here would
        // point an operator at the wrong code.
        //
        // The refusal's `fatal` flag is dropped with the rest of it, and
        // `Stop::fatal`'s own documentation says so: this ending names the fault,
        // and a fault is the kind of thing another incarnation might not repeat.
        (Some((who, _)), Some(message)) => (answer, Some(who), Some(Ended::Panicked { message })),
        // Completed and withheld: the turn ran and was not committed, so whoever
        // holds the reply owes the caller a refusal rather than an uncommitted
        // value. Which of the two sentences they are owed is `who`, and it is the
        // same `who` the parked callers were released with above.
        (Some((who, error)), None) => {
            let end = match who {
                ObserverVerdict::Refused { name, fatal } => Ended::Observer { name, error, fatal },
                // Never fatal: an observer that panicked made no claim about
                // whether another incarnation would fare better, and answering one
                // for it would be a claim nobody made. It is written here, on the
                // one line that needs a flag, rather than into the slot the caller
                // also reads.
                ObserverVerdict::Panicked { name } => Ended::Observer {
                    name,
                    error,
                    fatal: false,
                },
            };
            (answer, Some(who), Some(end))
        }
    }
}

/// Run one message: the deadline, the handler, the observers, the answer.
///
/// **The one call a generated mailbox arm makes**, and the reason the arms can
/// be mechanical. Everything a turn owes anybody is decided here — so a method
/// kind added later inherits the queue deadline, the panic guard, the observer
/// chain and the idempotency marks by construction rather than by whoever wrote
/// its arm remembering all four.
///
/// `#[doc(hidden)] pub` because the match that calls it is expanded into the
/// **user's** crate.
///
/// # The deadline is read here, on the actor's own task
///
/// A [`QueueTimeout`](crate::QueueTimeout) budget is checked twice, and the two
/// checks answer different questions. The caller-side one bounds the wait for a
/// mailbox permit and says "this message never queued". This one runs the
/// instant the message comes **off** the mailbox and says "it queued and never
/// started". Only the second can be made on the actor's task, and only the
/// actor's task can tell a queued message from a running one — RFC-04 §16.2.
///
/// There used to be **three** hand-written comparisons for that: one here, one
/// in the keyed loop and one in the former inherent `Handle::service` dispatch.
/// Two of them
/// disagreed on wasm at any whole-tick budget because one spelled `>` and the
/// other `>=`. `Queueing::expired` is the one comparison now, asked from the one
/// place a turn begins.
///
/// # And so are the progress marks
///
/// `dispatched` before the handler and `unrun` on a budget that expired, both
/// here; `completed` in [`Reply::answer`], which is the one place an answer
/// happens. That reasoning was written twice, in two dispatchers, before Wave 1b
/// put it in one — and Wave 2c moved the last mark one step further in, because a
/// deferred reply is answered after the turn that received it has returned. A mark
/// beside the answer cannot be forgotten by the path that answers late.
///
/// # And so is the method's name
///
/// `method` is what [`TurnInfo::method`](crate::TurnInfo) reports, and it is a
/// parameter rather than something derived here because only the caller knows
/// it: a generated arm is expanded per method and passes its own literal, and
/// the served arm reads it off the decoded request. Both spell it the way the
/// wire does, which is the agreement `Rpc::method` holds the other half of.
#[doc(hidden)]
pub async fn turn<A, T, F>(
    state: &mut A,
    hooks: &mut Hooks<A>,
    origin: Origin,
    method: &'static str,
    reply: Reply<T>,
    turn: F,
) -> Option<Ended>
where
    A: 'static,
    T: Send + 'static,
    // The origin is handed to the closure BY VALUE rather than lent as a
    // `Caller`, and that is a lifetime decision rather than a style one. A
    // `Caller<'o>` borrowed out here and captured by the closure would make the
    // returned `dyn Future + 'b` require `'o: 'b` for every `'b` the higher
    // ranked bound quantifies over, which is only true for `'o = 'static` —
    // and RFC-07 §1.6 is the proof that a peer's `Caller` never reaches
    // `'static`. Moving the origin inside means every borrow is the future's
    // own.
    F: for<'b> FnOnce(Origin, &'b mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'b>> + Send,
{
    let reply = began(&origin, reply)?;

    let (ran, withheld, end) =
        observed_turn(state, hooks, method, move |state| turn(origin, state)).await;
    // The one place the handler's outcome and the observers' verdict become a
    // discriminant. A fault is answered even when an observer then refused —
    // `Answered::Faulted` is not a value, so the argument for withholding one
    // never applied to it, and withholding would reverse what this module's header
    // promises about a panicking call. A turn that *completed* and was refused is
    // withheld, because a caller must not be handed an uncommitted value.
    //
    // Withholding is the same on both `ObserverVerdict` arms and the sentence is not. The
    // decision needs two answers and the report needs three, and one slot carrying
    // both is what told a caller *the {name} observer refused this turn* about an
    // observer that fell over.
    reply.answer(match (ran, withheld) {
        (Ok(value), None) => Answered::Ran(value),
        (Ok(_), Some(ObserverVerdict::Refused { name, .. })) => Answered::Refused { name },
        (Ok(_), Some(ObserverVerdict::Panicked { name })) => Answered::ObserverPanicked { name },
        (Err(panic), _) => Answered::Faulted(panic),
    });
    end
}

/// Run one message whose handler answers it — now, or from a later turn.
///
/// What a `#[defer]` method's generated arm calls. The difference from [`turn`] is
/// one argument and one sentence: the [`Reply`] goes **into** the handler, and the
/// handler returns a [`Replied`] token proving it did something with it.
///
/// Everything a turn owes anybody is still decided here, which is the reason this
/// is a second function in this file rather than a second dispatcher in generated
/// code: the queue deadline, the panic guard, the observer chain and the marks are
/// the same ones, read from the same [`Origin`], through the same
/// [`observed_turn`].
///
/// `#[doc(hidden)] pub` because the match that calls it is expanded into the
/// **user's** crate.
///
/// # The three ways this ends, and who is answered in each
///
/// - **Answered inside the turn.** [`Reply::send`] handed the channel back, so the
///   value is sent from here, *after* the observer chain — and an observer that
///   refused turns it into [`Answered::Refused`] exactly as it would an ordinary
///   method's return value. A deferred method that answers immediately is not a
///   weaker method.
/// - **Parked.** The channel left with a [`Deferred`] and this turn owes nothing.
///   No mark is made, because no answer has happened; the mark travels with the
///   channel and is made by [`Reply::answer`] whenever it is.
/// - **Panicked before the channel was consumed.** The [`Reply`] was dropped
///   inside the unwinding future, so there is nothing left to answer with and the
///   caller's wait ends [`Withheld::Dropped`] — which names the actor that
///   survives, so the caller is not told to stop calling one that is serving. The
///   observers saw [`TurnOutcome::Panicked`](crate::TurnOutcome::Panicked), and the panic
///   hook ran at the fault. This is the one thing `#[defer]` costs against every
///   other kind, and [`Replied`] says so where a reader meets the type.
#[doc(hidden)]
pub async fn turn_deferred<A, T, F>(
    state: &mut A,
    hooks: &mut Hooks<A>,
    origin: Origin,
    method: &'static str,
    reply: Reply<T>,
    turn: F,
) -> Option<Ended>
where
    A: 'static,
    T: Send + 'static,
    // The origin by value for [`turn`]'s reason, and the reply by value for a
    // plainer one: the handler owns it from here.
    F: for<'b> FnOnce(
            Origin,
            &'b mut A,
            Reply<T>,
        ) -> Pin<Box<dyn Future<Output = Replied<T>> + Send + 'b>>
        + Send,
{
    let reply = began(&origin, reply)?;

    let (ran, withheld, end) = observed_turn(state, hooks, method, move |state| {
        turn(origin, state, reply)
    })
    .await;
    match ran {
        Ok(Replied(Settled::Now(reply, value))) => reply.answer(match withheld {
            None => Answered::Ran(value),
            Some(ObserverVerdict::Refused { name, .. }) => Answered::Refused { name },
            Some(ObserverVerdict::Panicked { name }) => Answered::ObserverPanicked { name },
        }),
        Ok(Replied(Settled::Later)) => {}
        // The `Panic` is dropped rather than reported, because the channel it
        // would have been reported on went into the same future.
        Err(_gone_with_the_reply) => {}
    }
    end
}

/// The deadline and the dispatch mark, shared by the two turns.
///
/// `None` means the budget expired and the caller has already been answered, so
/// the turn must not run. Written once and taken by reference so that the two
/// turns cannot disagree about when a turn begins — which is the disagreement Wave
/// 1b found three copies of, two spelling the comparison `>` and one `>=`.
fn began<T>(origin: &Origin, reply: Reply<T>) -> Option<Reply<T>> {
    // `Progress` shares its `Arc` on clone deliberately, which is what lets a mark
    // made on the actor's task be read back on the connection's.
    let progress = origin.caller().peer().map(Context::progress);
    let expired = origin
        .caller()
        .peer()
        .and_then(Context::queueing)
        .is_some_and(Queueing::expired);

    // Installed before anything can answer, including the expiry below.
    let reply = reply.settling(progress.clone());

    if expired {
        // Nothing ran, so nothing is owed. `unrun` outranks the enqueue mark
        // made when the message got its permit, so `Drop for Guard` sees
        // `NotDispatched` and RELEASES the key: the retry of a call that never
        // happened is a first attempt. Silence here would settle it
        // `Interrupted` and permanently refuse every retry of work nobody did.
        // RFC-04 §16.6, §16.12.
        if let Some(progress) = &progress {
            progress.unrun();
        }
        reply.answer(Answered::Expired);
        return None;
    }

    // What follows is user code, so this is where the work begins.
    if let Some(progress) = &progress {
        progress.dispatched();
    }
    Some(reply)
}

/// Run one item from a `feed`: the handler, the observers, and nothing else.
///
/// The third turn, and it is the one that shows what the other two were carrying
/// for a *caller*. A feed item has no [`Origin`], so there is no queue deadline to
/// read and no idempotency mark to make; it has no [`Reply`], so there is nobody
/// to answer. What is left is [`observed_turn`] — the handler inside the panic
/// guard, then the observer chain — which is the part that is about the **state**
/// rather than about whoever asked.
///
/// `#[doc(hidden)] pub` because the generated `Turn` impl for a feed item is
/// expanded into the **user's** crate.
///
/// # A panic here ends the actor, and that is deliberately unlike an `rpc`
///
/// A panicking `rpc` costs its message and not the actor, and the reason it can is
/// that somebody is **told**: the panic crosses the reply channel as
/// [`TurnLost::Faulted`] and [`ask`](LocalActor::ask) resumes it in the caller. A
/// feed item has no channel and no caller, so the same policy here would drop the
/// fault on the floor and carry on with a state the handler may have left half
/// applied — a silent partial write, which is the one outcome the whole crate is
/// arranged to refuse.
///
/// So a feed's fault is reported the only way a feed can report anything: the
/// actor ends as [`Ended::Panicked`], every observer's `finish` runs, and
/// [`ended`](LocalActor::ended) names it. The panic hook has already printed at
/// the fault.
///
/// This is the same argument [`Ended::Feed`] makes about a feed that *stops*, and
/// it is the reason a `feed` is a different declaration rather than a `tell` from
/// a stream: a `tell` is somebody in this process choosing fire-and-forget, and
/// [`notify`](LocalActor::notify) documents that nobody is told. Nobody chose
/// anything here.
#[doc(hidden)]
pub async fn feed_turn<A, F>(
    state: &mut A,
    hooks: &mut Hooks<A>,
    feed: &'static str,
    turn: F,
) -> Option<Ended>
where
    A: 'static,
    F: for<'b> FnOnce(&'b mut A) -> Pin<Box<dyn Future<Output = ()> + Send + 'b>> + Send,
{
    let (ran, _withheld, end) = observed_turn(state, hooks, feed, turn).await;

    // There is no reply channel to carry a fault and no caller to resume it into,
    // so the ending is the only report a feed's panic can have.
    let faulted = ran.err().map(|panic| Ended::Panicked {
        message: panic.message().to_owned(),
    });

    // `end` first, and the order is the decision: an observer that refused a
    // *panicked* turn is already reported by `observed_turn` as the fault rather than
    // as the adapter, and one that refused a completed one must not have its refusal
    // overwritten by a fault that did not happen.
    end.or(faulted)
}

/// What a poll of an actor's feeds produced.
///
/// Two states and not `Option`, because the second one is not "nothing yet" — it
/// is a fact that ends the actor, and giving it a name is what stops it being
/// confused with [`Poll::Pending`](std::task::Poll::Pending) at the one site that
/// reads both.
///
/// `#[doc(hidden)] pub` because a generated `Feeds` impl builds one.
#[doc(hidden)]
pub enum Fed<T> {
    /// One item, from whichever feed produced it. A [`Turn`] like any other.
    ///
    /// Boxed for [`Message::Mail`]'s reason and it is the same reason exactly: an
    /// item spelled out here would be spelled out in the loop's future, so an idle
    /// actor would pay for the largest item any of its feeds can carry. A generated
    /// poller allocates it, which is the one allocation per item a mailbox message
    /// also costs.
    Item(Box<T>),
    /// A feed produced its last item. The actor ends as [`Ended::Feed`].
    Ended {
        /// The feed's declared name.
        name: &'static str,
    },
}

/// An actor's second input: a set of locally wired streams, polled beside the
/// mailbox in one loop.
///
/// Implemented **only** by generated code, for the same reason [`Turn`] is: the
/// members are the service's own `feed` declarations, so only a per-service type
/// can name them. keylink owns when they are polled and what a poll means; the
/// generated type owns which streams there are.
///
/// # A poll and not a future, which is what makes the loop's `select` honest
///
/// The obvious signature is `async fn next(&mut self)`, and it has a cancellation
/// hazard that a synchronous poll does not: the loop would have to *build* that
/// future, race it against the mailbox, and **drop it** every time the mailbox won.
/// A future dropped after it had taken an item off a stream and before it handed it
/// over loses the item, silently, under load. Polling the streams directly means
/// there is no future to drop — a poll that returns
/// [`Pending`](std::task::Poll::Pending) has taken nothing.
///
/// It also means the loop needs no `select!` macro, which matters more than it
/// looks: `tokio`'s is behind the `macros` feature, which keylink does **not**
/// enable for `wasm32-unknown-unknown` (see `Cargo.toml`'s target blocks), so a
/// `tokio::select!` in this loop would not compile for a browser at all.
///
/// # Fairness is keylink's, in two places
///
/// The loop alternates which input it polls first, so neither the mailbox nor the
/// feeds can starve the other: a saturated feed delays a message by at most one
/// item, and the reverse. A generated `poll_next` rotates among *its own* feeds for
/// the same reason, so a saturated feed cannot starve its siblings either. The
/// mailbox therefore gets half the turns and each of `n` feeds gets `1/2n` of them
/// when everything is ready at once — stated, because "fair" without a number is
/// how a priority knob gets argued for later.
#[doc(hidden)]
pub trait Feeds<A>: Send + 'static {
    /// One item from one feed, which is a turn against the state.
    ///
    /// A single type across every feed of a service — the generated enum, one
    /// variant per declaration — so that a rotation among feeds can be a table
    /// lookup rather than a match written per pair.
    type Item: Turn<A>;

    /// Poll every feed once, starting wherever the last item came from.
    ///
    /// Called directly by the loop, never wrapped in a future it might drop.
    /// Returning [`Fed::Ended`] is terminal: the loop stops the actor, so a feed
    /// is never polled after it has answered `None`.
    fn poll_next(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Fed<Self::Item>>;
}

/// The feeds of an actor that has none.
///
/// A unit and not a `NoFeeds` marker, because `()` already means "nothing" and a
/// named type would be a second way to say it. `spawn` and `spawn_observed` pass
/// one, which is how a `LocalActor` whose service declares no feed pays nothing
/// for them: [`Infallible`](std::convert::Infallible) is uninhabited, so the
/// loop's feed arm is a branch on a value that cannot exist.
impl<A: Send + 'static> Feeds<A> for () {
    type Item = std::convert::Infallible;

    fn poll_next(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Fed<Self::Item>> {
        // Pending forever, and never woken: there is nothing to wake it. The
        // mailbox arm is the only one that can complete.
        std::task::Poll::Pending
    }
}

/// A turn that cannot be built, so the loop's feed arm can be written once.
///
/// The alternative was a second loop for an actor with no feeds, which is the
/// divergence this wave exists to delete.
impl<A: Send> Turn<A> for std::convert::Infallible {
    async fn run(self, _state: &mut A, _hooks: &mut Hooks<A>) -> Option<Ended> {
        match self {}
    }
}

/// One actor, for **chain membership and nothing else**.
///
/// Not a handle, not a key, and it must never enter a map that outlives a turn.
/// Written here rather than left to a reader, because the type is a pointer that
/// happens to identify something and every other use of it would be a lie: it
/// cannot be sent to, cannot be compared across processes, and names an actor
/// only for as long as somebody holds this clone.
///
/// # It costs no field, because the cell already exists
///
/// [`LocalActor`] holds `mail`, `ended`, `fate`, `busy` and `task`.
/// [`spawn_fed`](LocalActor::spawn_fed) makes one `Arc<AtomicBool>` and clones
/// it — one end to the handle, one into the [`Watched`] the loop polls — and
/// `Clone` shares it. So that allocation already identifies the actor, uniquely
/// and across every clone, and this newtype is a second name for it rather than
/// a sixth field.
///
/// # A private newtype from the start, and an `Arc` rather than its address
///
/// Private, never public and never `#[doc(hidden)] pub`: the `Answer<Resp>`
/// alias documented two hundred lines above is the scar that argument comes
/// from, where a transparent alias put tokio's channel type into keylink's
/// public API through a hidden name.
///
/// The first version of this compared `Arc::as_ptr as usize`, and that is a
/// **false positive waiting to happen**. The argument for it was that the chain
/// only holds actors whose turns are on the stack, so the allocation cannot be
/// freed — which is true of the innermost entry and false of the ancestors,
/// which travel in the message and name actors that were alive when it was sent.
/// The reachable sequence: A calls B; A is aborted and every handle dropped,
/// freeing the allocation; C spawns onto the same address; B calls C, and C is
/// refused as if it were A. That breaks a working program with a named error,
/// which is worse than missing a detection. Address reuse is the default and not
/// a corner — a probe allocating an `Arc<AtomicBool>`, recording the pointer,
/// dropping and reallocating hit the same address 1000 times out of 1000.
///
/// A strong reference keeps the allocation alive for exactly the window the
/// chain uses it, so the address cannot be reused inside it. An `Arc` is a
/// non-null pointer, so an entry costs the 8 bytes a `usize` would and [`Chain`]
/// keeps its niche. The price is one atomic increment per entry, paid only on
/// the nested path.
#[derive(Clone)]
pub(crate) struct Actor(Arc<AtomicBool>);

impl Actor {
    /// Build an identity from the actor's shared busy cell.
    pub(crate) fn new(busy: Arc<AtomicBool>) -> Self {
        Self(busy)
    }

    /// The same actor, by allocation and never by address.
    fn is(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// The actors whose turns are on the stack behind the turn being polled.
///
/// Three variants, and the reason is allocation rather than tidiness. Every
/// top-level turn installs `One`, which allocates nothing. `Deep` allocates
/// once, only for a turn reached from inside another turn, and an `Arc` clones
/// without allocating — which matters because every send made during a turn
/// clones the chain into its message.
///
/// # `Arc<Vec<..>>` and not `Arc<[..]>`, and the reason is the mailbox
///
/// A slice `Arc` is a **fat** pointer, so `Chain` came out at 24 bytes, and this
/// type sits in every [`Message::Mail`] — and `tokio::sync::mpsc` allocates its
/// first block of 32 slots eagerly, so those bytes are multiplied by 32 on every
/// idle actor whether or not anything is ever queued. Measured on the local path
/// in `tests/actor_memory.rs`: the fat form cost **536 bytes per idle actor**
/// (2,029 -> 2,565) and the thin one costs **280** (2,029 -> 2,309).
///
/// The double indirection is paid only when a deep chain is walked, which happens
/// on the nested path alone; the fat pointer was paid by every actor in the
/// process. A linked list would be 8 bytes and cheaper still, and it is refused
/// for the reason `One` exists: it allocates a node for **every** top-level turn,
/// which trades cold bytes for work on the hot path.
#[derive(Clone, Default)]
pub(crate) enum Chain {
    /// Nothing is on the stack: this thread is between turns, or on a task that
    /// never ran one — a connection's, or a caller's.
    #[default]
    Empty,
    /// One turn, the common case.
    One(Actor),
    /// A turn reached from inside another turn, outermost first.
    Deep(Arc<Vec<Actor>>),
}

impl Chain {
    /// This chain with one more actor on the end, or unchanged when the actor
    /// running the next turn has no identity to add.
    ///
    /// `None` is a keyed actor or a bare receiver: [`Inbox::identity`] carries
    /// what that means. The inherited chain still **passes through** such a turn
    /// rather than being reset, which costs nothing and is the honest shape — a
    /// hop that cannot be detected must not also erase the hops that can.
    fn with(self, running: Option<Actor>) -> Self {
        let Some(running) = running else {
            return self;
        };
        match self {
            Self::Empty => Self::One(running),
            Self::One(outer) => Self::Deep(Arc::new(vec![outer, running])),
            Self::Deep(outer) => {
                let mut grown = Vec::with_capacity(outer.len() + 1);
                grown.extend(outer.iter().cloned());
                grown.push(running);
                Self::Deep(Arc::new(grown))
            }
        }
    }

    /// Whether this actor's turn is one of the ones on the stack.
    pub(crate) fn holds(&self, who: &Actor) -> bool {
        match self {
            Self::Empty => false,
            Self::One(one) => one.is(who),
            Self::Deep(deep) => deep.iter().any(|each| each.is(who)),
        }
    }
}

thread_local! {
    /// The chain of the turn being polled on this thread, or [`Chain::Empty`]
    /// between polls.
    ///
    /// Exchanged in and out by [`Chained::poll`], so it is set for exactly as
    /// long as a turn's dispatch is on the stack. `parking.rs`'s `PARKING` is the
    /// template, down to the `Drop` guard, and its header carries the whole
    /// argument for a thread-local: `tokio::task_local!` lives behind tokio's
    /// `rt` feature, which keylink enables on no target, and the save/restore
    /// below is what that macro does anyway. It follows a task across worker
    /// threads for the same reason — the exchange happens on whichever thread is
    /// **polling**, not on whichever thread started the turn.
    static CHAIN: Cell<Chain> = const { Cell::new(Chain::Empty) };
}

/// Swap this chain with whatever the current thread is holding. Its own inverse.
fn exchange(held: &mut Chain) {
    let taken = std::mem::take(held);
    *held = CHAIN.with(|slot| slot.replace(taken));
}

/// The chain of the turn this send is being made from, or [`Chain::Empty`].
///
/// `try_with` and not `with`, for `parking::park`'s reason: a thread whose locals
/// have already been destroyed is not running a turn, which is the `Empty` case
/// exactly, and raising a panic inside somebody else's destructor is not an
/// improvement on sending.
pub(crate) fn current() -> Chain {
    CHAIN
        .try_with(|slot| {
            let held = slot.take();
            let seen = held.clone();
            slot.set(held);
            seen
        })
        .unwrap_or_default()
}

/// A turn's dispatch, with the chain the messages it sends will carry.
///
/// Built by [`run_loop`] around [`dispatch`] and nothing else, so "during the
/// turn" means the handler's body, everything it awaits inline, and the observer
/// chain the arm runs — every send that can reach a mailbox from inside the turn.
struct Chained<F> {
    /// The chain, except while this future is being polled — during a poll it is
    /// on the thread and this holds whatever was displaced there, normally
    /// [`Chain::Empty`].
    chain: Chain,
    /// The dispatch.
    inner: F,
}

/// `Unpin` on the inner future rather than a projection, for `Parking`'s reason:
/// the one thing this ever wraps is the `Pin<Box<dyn Future>>` [`dispatch`]
/// returns, which is `Unpin`, and requiring it costs a bound instead of `unsafe`.
impl<F: Future + Unpin> Future for Chained<F> {
    type Output = F::Output;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let Self { chain, inner } = self.get_mut();
        exchange(chain);
        // Per **poll** and not per turn, and a guard rather than a line after the
        // poll. A turn's future moves between worker threads, so the install has
        // to happen wherever it is being polled; and the poll can unwind, which
        // would otherwise leave one turn's chain visible to the next thing polled
        // on this thread — an unrelated actor refused for being on a chain it was
        // never on.
        //
        // **The unwind path is REACHABLE and is NOT tested, and that is said here
        // rather than left to be inferred.** It is not reachable by the two
        // obvious routes: [`turn`] catches the handler with `guarded` and each
        // observer with a `catch_unwind` plus a `guarded`, so neither reaches this
        // frame. What is left is a panic in the generated arm outside `turn`, in
        // the plumbing between those guards, or in a `Drop` run as the dispatch
        // future unwinds — and this crate already treats that as a real ending:
        // [`Watched`]'s own `Drop` exists for "a turn whose unwind escapes the
        // loop". No test in this tree drives it, and no break has been watched
        // against this line.
        //
        // The guard matters *because* the unwind is fatal to the task and not to
        // the thread: the worker goes on to poll other actors, and a chain left
        // behind would refuse one of them by name. `parking.rs` makes the same
        // trade at the same place, for the same reason.
        let _restore = Restore(chain);
        Pin::new(inner).poll(cx)
    }
}

/// Puts the chain back where it came from, on the way out of a poll — including
/// the way out of a panicking one.
struct Restore<'a>(&'a mut Chain);

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        exchange(self.0);
    }
}

/// The one message whose work is a caller's closure rather than a declared
/// method: [`LocalActor::read`].
///
/// Synchronous and takes `&A`, so it cannot await, cannot mutate, and holds
/// nothing across a yield point. That is why it is still a boxed closure while
/// the turn is not: there is no per-service enum a macro could generate for
/// "whatever the caller wants to look at", and the alternative — making every
/// read a declared method — would put a cross-cutting concern into the service's
/// own wire contract.
type Read<A> = Box<Box<dyn FnOnce(&A) + Send>>;

/// A turn, a read, or the request to stop after the messages already queued.
enum Message<A, M> {
    /// One of the service's own methods, as a value of its generated enum,
    /// **behind the pointer it will be dispatched from**.
    ///
    /// Boxed here rather than at the channel, and the difference is the whole of
    /// `tests/actor_memory.rs`'s slope assertion. What must not happen is an unboxed
    /// `M` appearing anywhere in the loop's own future, because that future is what
    /// an idle actor holds — and a `Box<M>` *unwrapped* in the loop to be dispatched
    /// is exactly that. So the box travels from the sender all the way to
    /// [`dispatch`], which unwraps it inside its own plain stack frame.
    ///
    /// It also removes an allocation rather than adding one: the channel used to
    /// carry `Box<Message<A, M>>` — one box for the message, none for the mail — and
    /// now carries the enum directly, which is pointer-sized whatever `M` is.
    ///
    /// # The chain rides here, and it could not ride on `Origin`
    ///
    /// A thread-local cannot cross a task, so what an actor is called *from* has
    /// to travel in the message. The first design put it on [`Origin`], and that
    /// version cannot work: an `Origin` is consumed INTO the opaque `M` before a
    /// `Message` exists — every send builds its payload through a closure typed
    /// `impl FnOnce(Origin, Reply<T>) -> M` — so [`run_loop`] never sees one, and
    /// [`Turn::run`] takes no `Origin` and offers no accessor.
    ///
    /// The consequence was not a matter of taste. `run_loop` could have installed
    /// `Chain::One(self)` from a new parameter, but never `Chain::Deep`, because
    /// the ancestors would be sealed inside `M` — so the check would have caught
    /// a self-call and **silently missed `A → B → A`**, which is the
    /// deterministic hazard the whole item exists to close.
    ///
    /// This enum is private to this module, so carrying it here costs no public
    /// API, no generated line, and raises no ALPN question at all.
    Mail(Box<M>, Chain),
    /// [`LocalActor::read`]. Runs no observers, by design: a read takes `&A` and
    /// cannot mutate, so storing after it is waste.
    Read(Read<A>),
    /// Ends the loop, which drops the receiver, which is what makes every other
    /// handle report a closed mailbox from then on — [`Withheld::Gone`] to a
    /// door that wanted a value, [`NotQueued::Closed`] to one that wanted only to
    /// be queued.
    Stop,
}

/// Where the loop takes its next message from.
///
/// **Private, and that is the whole reason this trait can exist at all.** The two
/// paths into the loop carry different channel items: a [`LocalActor`] queues
/// [`Message<A, M>`] — it has a `read` and a `stop` — and a keyed actor's mailbox
/// is a [`Mailbox<M>`](crate::Mailbox), because the registry is generic in the
/// service's message and knows nothing of `A`. Making those one channel would put
/// the state's type on [`Registry`](crate::Registry), [`Mountable`](crate::Mountable) and
/// `KeyedService`; abstracting the *receive* instead costs two impls and reaches
/// nothing outside this file.
///
/// It is a synchronous poll rather than an `async fn` for [`Feeds`]'s reason, and
/// here it is not even a trade: `mpsc::Receiver::poll_recv` is what the channel
/// offers underneath `recv`.
trait Inbox<A, M>: Send {
    /// The next message, or `None` when every sender is gone.
    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Message<A, M>>>;

    /// A turn is starting, or the one that was running has ended.
    ///
    /// Called by [`run_loop`] around **every** dispatch, for either input, which
    /// is the whole reason it is here rather than inside a receive: a feed item is
    /// a turn and it never comes through a mailbox. `registry.rs`'s `Busy` carries
    /// the rest of that argument.
    ///
    /// Written by whoever has somewhere to put it: [`Keyed`] writes the registry
    /// entry [`Registry::status`](crate::Registry::status) reads, [`Watched`] writes
    /// the cell a [`LocalActor`] carries on its handle, and a bare
    /// `mpsc::Receiver` keeps this default — the supervision tests in this module
    /// hand `supervise` one and want no cell at all.
    ///
    /// # What the refusal that stood here weighed, and what it did not
    ///
    /// It read: *"a `LocalActor` has nowhere to report it to. Its caller holds a
    /// handle and can ask that."* That is true of an **idle** actor and it is the
    /// whole of the case it considered. It is false of a **wedged** one, which is
    /// the actor [`busy`](LocalActor::busy) exists for: asking a wedged
    /// actor queues a message behind the turn that wedged it, so the question
    /// never returns. `is_running` is `!mail.is_closed()` and answers `true` for
    /// both, which is why the two were indistinguishable before this cell.
    ///
    /// The refusal was not wrong about the cost. A cell on a handle is a
    /// reporting surface a `LocalActor` did not have, and it is paid for in the
    /// only way this crate accepts: [`Watched`] clears it on drop, so an unwind
    /// and an [`abort`](LocalActor::abort) cannot leave it lying.
    fn mark_busy(&self, _running: bool) {}

    /// Which actor this loop is, for [`Chain`] membership — or `None` for one
    /// that cannot be named.
    ///
    /// [`mark_busy`](Self::mark_busy)'s idiom, for the same reason: written by
    /// whoever has somewhere to put it. [`Watched`] hands back the cell a
    /// [`LocalActor`] already carries on its handle, so the check costs no field
    /// on that type; a bare `mpsc::Receiver` and a [`Keyed`] mailbox have no such
    /// cell and keep this default.
    ///
    /// # `Lent` must override it, and a `None` default is why that is a risk
    ///
    /// [`Lent`] is a **delegating wrapper**, and it is the inbox the production
    /// supervised path uses: `spawn_supervised` builds a real `Watched` with a
    /// real cell, and `supervise` then wraps it in a `Lent` and calls
    /// [`run_loop`]. Without the forward below, every supervised local actor
    /// would lose this check silently, with no test failing — which is what a
    /// `None` default buys and costs at the same time.
    ///
    /// # What a keyed actor's identity means, stated rather than hidden
    ///
    /// A keyed mailbox carries the sender's [`Chain`] alongside its `M`.
    /// [`Keyed::poll_next`] preserves it, and the loop appends this mailbox's
    /// identity before dispatch. A local keyed reference therefore participates in
    /// the same self-call and ring check as a local handle; a remote connection
    /// normally starts with [`Chain::Empty`] because it is outside any actor turn.
    ///
    fn identity(&self) -> Option<Actor> {
        None
    }

    /// The loop knows why it is ending, and has dropped nothing yet: say what
    /// that means for a reply channel that is about to die.
    ///
    /// Called once, between the loop's exit and `finish` — which is the only
    /// point where a supervisor's *another incarnation follows* decision can be
    /// made **before** the state is dropped. Every [`Deferred`] parked in that
    /// state dies with it, and a peer waiting on one has to be told whether this
    /// actor is coming back; deciding afterwards would be a race against that
    /// answer, reported to the peer as "there is nothing here" for an actor that
    /// is mid-restart.
    ///
    /// Here rather than as a parameter on [`run_loop`] because this is the same
    /// question [`mark_busy`](Self::mark_busy) is: something outside the loop
    /// owns the mailbox and wants to know what the loop is doing. Only [`Lent`]
    /// has a supervisor above it, so every other impl takes the default — a loop
    /// that reached this line and is nobody's incarnation will not run again.
    /// Counted that way rather than "two of three", which is what stood here and
    /// was already stale one impl later.
    ///
    /// **It returns the verdict rather than publishing it**, and that is the
    /// whole reason the give-up case cannot be forgotten again: [`run_loop`]
    /// publishes what this hands back, at one line, on every path out of the
    /// loop. The shape it replaces was `fn ending(&mut self, _: &Ended)` writing
    /// a channel itself, which meant the *only* impl that wrote anything wrote
    /// it on the one arm its author was thinking about — a restart — and left an
    /// actor that had permanently given up reading as one that was still there.
    fn ending(&mut self, _reason: &Ended) -> Standing {
        Standing::Gone
    }

    /// Refuse messages from now on, **without** dropping what is already queued.
    ///
    /// `tokio::sync::mpsc::Receiver::close`, which is not the same as dropping the
    /// receiver: every `Sender::is_closed()` becomes true immediately and the
    /// buffer still drains. Only [`Lent`] and [`supervise`] call it, and only on a
    /// path where there will be no next incarnation.
    ///
    /// # It exists because `is_running()` lied, and only on the supervised path
    ///
    /// An unsupervised actor gets this for free: [`run_loop`] owns its receiver
    /// and drops it, so `!mail.is_closed()` is exact from that instant. A
    /// supervised one does not — [`supervise`] takes the receiver **by value** and
    /// holds it across every incarnation, so between `run_loop` returning on a
    /// permanent give-up and `supervise` returning, `LocalActor::is_running()`
    /// answered `true` for an actor that would never handle another message, and
    /// `ask` queued into a mailbox nobody would drain.
    ///
    /// That window is instructions wide and has no await in it, which is why it is
    /// invisible on a current-thread runtime and why the served answer is **not**
    /// built on it: `deliver_unkeyed` reads the verdict, not the mailbox. This
    /// repairs the public method rather than the wire answer, and the two are
    /// deliberately separate — one fact, two consequences, decided at one place.
    fn close(&mut self);
}

/// A [`LocalActor`]'s mailbox, which carries reads and the stop request too.
impl<A: Send, M: Send> Inbox<A, M> for mpsc::Receiver<Message<A, M>> {
    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Message<A, M>>> {
        self.poll_recv(cx)
    }

    fn close(&mut self) {
        // Fully qualified, and not `Self::close(self)`. That spelling resolves to
        // the inherent method only because inherent methods win path resolution
        // over trait ones — so an upstream rename turns it into unconditional
        // recursion with no diagnostic at all, which is a stack overflow rather
        // than a compile error.
        mpsc::Receiver::close(self);
    }
}

/// A [`LocalActor`]'s mailbox, plus the cell its handle reads to answer
/// [`busy`](LocalActor::busy).
///
/// A newtype for [`Keyed`]'s reason and one it does not have: `mpsc::Receiver` is
/// a foreign type, so there is nowhere on it to put a cell. The bare impl above
/// stays rather than moving here, because the supervision tests in this module
/// hand [`supervise`] a raw receiver and want the empty default.
///
/// # The cell is cleared on drop, and that is the half the loop cannot do
///
/// [`run_loop`] clears it after every dispatch that **returns**. Two endings do
/// not return: a turn whose unwind escapes the loop, and an
/// [`abort`](LocalActor::abort), which cancels the task at its next await. Both
/// end the task without reaching that line, and both drop this. Without the
/// `Drop` the handle would answer "busy" for ever about an actor that is gone.
///
/// **A clear inside `supervise` would not reach either of them** — and it was
/// tried. It is dead, and by two cases rather than by one rule over all three
/// endings `retryable` restarts. `Ended::Panicked` and a non-fatal
/// `Ended::Observer` are values `dispatch` **returned**, so the clear on the next
/// line already ran. `Ended::Feed` is built on the `Taken::Fed(Fed::Ended { .. })`
/// arm, which runs no dispatch, so the flag is not set at that point — earlier
/// mail turns may well have set and cleared it. Either way the supervisor would
/// be storing `false` over `false`.
///
/// It is worse than dead, because `run_keyed_supervised` hands the same function
/// a [`Keyed`], which would make `supervise` a second writer of the registry's
/// flag — and `Busy`'s own invariant in `registry.rs` is that `run_loop` writes
/// it and *nothing else does*.
struct Watched<A, M> {
    rx: mpsc::Receiver<Message<A, M>>,
    busy: Arc<AtomicBool>,
}

impl<A: Send, M: Send> Inbox<A, M> for Watched<A, M> {
    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Message<A, M>>> {
        self.rx.poll_recv(cx)
    }

    fn mark_busy(&self, running: bool) {
        self.busy.store(running, Ordering::Relaxed);
    }

    /// The same cell, read as an identity instead of as a flag. See [`Actor`] for
    /// why one allocation can carry both without either meaning the other.
    fn identity(&self) -> Option<Actor> {
        Some(Actor(Arc::clone(&self.busy)))
    }

    fn close(&mut self) {
        // Fully qualified for the bare impl's reason: an inherent `close` would
        // win path resolution and an upstream rename would turn a delegation into
        // silent recursion.
        mpsc::Receiver::close(&mut self.rx);
    }
}

/// See the type's own "cleared on drop" section. `Relaxed` matches every other
/// write to this cell: the flag is a diagnostic and orders nothing.
impl<A, M> Drop for Watched<A, M> {
    fn drop(&mut self) {
        self.busy.store(false, Ordering::Relaxed);
    }
}

/// A keyed actor's mailbox, which carries only the service's own messages.
///
/// A newtype and not a bare `mpsc::Receiver<Box<M>>`, and the reason is inference
/// rather than taste. While the local channel carried `Box<Message<A, M>>`, that type
/// **was** a `Box<M2>` with `M2 = Message<A, M>`, so two bare impls left
/// `Receiver<Box<Message<..>>>: Inbox<A, ?M>` with two solutions and every call site
/// needing a turbofish — `error[E0283]`, observed. Coherence allows such a pair, the
/// trait parameters differing, which is exactly what makes it a trap: it compiles as
/// a definition and fails at the use. The two channel shapes have diverged since, and
/// the newtype stays so that the next pair of impls does not have to rediscover
/// this.
struct Keyed<M>(crate::Mailbox<M>);

/// [`Message::Read`] and [`Message::Stop`] are unreachable on this path: a keyed
/// actor is read through a method and stopped by
/// [`Registry::stop`](crate::Registry::stop) dropping the registry's sender,
/// which arrives here as `None`. The loop's arms for them are written to be *correct*
/// rather than to panic, which is the choice `Mail`'s `send` and `recv` variants
/// already make one layer out.
impl<A: Send, M: Send> Inbox<A, M> for Keyed<M> {
    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Message<A, M>>> {
        self.0
            .poll_recv(cx)
            .map(|mail| mail.map(|(mail, chain)| Message::Mail(mail, chain)))
    }

    fn identity(&self) -> Option<Actor> {
        Some(self.0.identity())
    }

    /// The mailbox is what the registry kept a handle on, so it is the way back to
    /// the entry [`Registry::status`](crate::Registry::status) reads.
    fn mark_busy(&self, running: bool) {
        self.0.mark_busy(running);
    }

    fn close(&mut self) {
        self.0.close();
    }
}

/// Which of an actor's two inputs produced the next thing to do.
enum Taken<A, M, T> {
    /// A mailbox message, or `None` for a mailbox nobody can send to any more.
    Mail(Option<Message<A, M>>),
    /// A feed item, or the feed that ended.
    Fed(Fed<T>),
}

/// Wait until either input has something, without building a future over either.
///
/// `mail_first` is the fairness knob and it is the loop's, not a caller's: the loop
/// flips it after every item, so the input that lost the last race gets first
/// refusal on the next one. A starved feed is a **correctness** problem — stale
/// state served as fresh — and a starved mailbox is a latency problem, so neither
/// wins by default and there is deliberately no way to ask for one to.
///
/// The arm that is polled second is skipped entirely when the first is ready, which
/// is what `select!` does too. It cannot lose a wakeup: the skipped input is polled
/// on the next iteration, and this future is rebuilt each time round.
async fn next<A, M, F, I>(inbox: &mut I, feeds: &mut F, mail_first: bool) -> Taken<A, M, F::Item>
where
    I: Inbox<A, M>,
    F: Feeds<A>,
{
    std::future::poll_fn(move |cx| {
        use std::task::Poll;

        if mail_first {
            if let Poll::Ready(message) = inbox.poll_next(cx) {
                return Poll::Ready(Taken::Mail(message));
            }
            if let Poll::Ready(fed) = feeds.poll_next(cx) {
                return Poll::Ready(Taken::Fed(fed));
            }
        } else {
            if let Poll::Ready(fed) = feeds.poll_next(cx) {
                return Poll::Ready(Taken::Fed(fed));
            }
            if let Poll::Ready(message) = inbox.poll_next(cx) {
                return Poll::Ready(Taken::Mail(message));
            }
        }
        Poll::Pending
    })
    .await
}

/// **The** place a turn is run, for every input and both paths.
///
/// One generic function rather than a call per arm, so that `rg` answering "where
/// is a turn dispatched" returns one line. A mailbox message and a feed item are
/// both a [`Turn<A>`](Turn), which is the type-level version of the same statement.
///
/// The `Box::pin` is here for the reason this module's header gives, and being here
/// is now part of it: the turn's state machine is sized by the caller's handler, and
/// an unboxed await would put that size in the task's own future — where an actor
/// pays for it while it is idle. Measured at Wave 1b: 3,005 bytes per idle actor
/// without this line against 1,597 with it, at a service with one `u64` argument.
/// One line, both paths.
///
/// # It returns a boxed future and is **not** an `async fn`, and that cost 1,987 bytes
///
/// The obvious spelling is `async fn dispatch(..) -> Option<Ended>` with the
/// `Box::pin` inside, and it silently undoes the box it contains. An `async fn`
/// stores **every argument** in the future it returns, so `turn` — the service's own
/// message, sized to its largest variant — would live in *this* function's future,
/// which `run_loop` then awaits inline and holds in the task's frame while the actor
/// is idle. The inner box would still be there, boxing a future that had already
/// been paid for.
///
/// Caught by `tests/actor_memory.rs`, which is the only reason it is written here
/// rather than in a commit message: a keyed idle actor went from 1,159 to 1,799
/// bytes, and the *slope* — the number that says an idle actor does not pay for what
/// its service declares — went from 0 to **1,987 bytes** over a 1,016 byte message,
/// firing the assertion by a factor of fifteen. The wave's own numbers, after both
/// fixes, are 1,415 keyed and 1,853 local, with the slope back inside its 128 byte
/// slack.
///
/// Returning `Pin<Box<dyn Future>>` puts `turn` inside the allocation instead, so
/// `run_loop`'s frame holds one pointer. Same allocation count as Wave 1b's loop,
/// one dispatch site instead of two.
/// # And it takes the turn **boxed**, which cost another 1,011
///
/// The same defect one layer down. `run_loop` is an `async fn`, so a `T` handed to
/// this one by value would be materialised in `run_loop`'s own frame on the way in —
/// a temporary the generator has to have room for — and that room is per idle actor
/// again. The first fix left exactly `size_of::<Mail>()` of slope behind for this
/// reason: 1,011 bytes over a 1,016 byte message, measured.
///
/// So the box arrives from the sender ([`Message::Mail`]) or from the feed
/// ([`Fed::Item`]) and is unwrapped **here**, in a plain function whose frame is a
/// real stack frame rather than part of any generator. Nothing sized by the caller's
/// service is ever spelled out inside the loop's future.
#[expect(
    clippy::boxed_local,
    reason = "the box is the point, and the lint is reading one frame too few. The \
              value arrives already boxed — from a mailbox slot or a feed's poller — \
              and taking it by value would materialise it in `run_loop`'s generator on \
              the way in, which is what an idle actor holds. Measured at 1,011 bytes \
              of slope over a kilobyte of declared arguments; `tests/actor_memory.rs` \
              is the assertion."
)]
fn dispatch<'a, A: 'a, T: Turn<A>>(
    turn: Box<T>,
    state: &'a mut A,
    hooks: &'a mut Hooks<A>,
) -> Pin<Box<dyn Future<Output = Option<Ended>> + Send + 'a>> {
    Box::pin((*turn).run(state, hooks))
}

/// One actor: its two inputs, its observers, its ending.
///
/// **The one loop.** It used to be two — this one, and a second written by
/// `service!` into every crate that declared an actor — and they disagreed twice
/// before Wave 1b made the *dispatch* one path: one spelled a deadline `>` where the
/// other spelled `>=`, and one rendered a panic bare where the other prefixed it.
/// What was left after that was still two loops around one dispatch, which is why a
/// keyed actor had no observers and no `finish`: those live in a loop, not in a
/// dispatch. Feeds are what made the split untenable rather than merely untidy,
/// because a second input needs a `select` and nobody wanted to write two.
///
/// Returns the reason to the keyed registry, which publishes it through the
/// shared [`Registry::ended`](crate::Registry::ended) watch channel. A local actor
/// also exposes its ending directly through [`LocalActor::ended`]. Everything
/// whose *order* matters happens in here.
///
/// # Teardown order, which is the fix rather than a detail
///
/// `finish` runs after the last turn and **before** anything is dropped, so a flush
/// that needed the state still had it — RFC-06 §7's debounced store is unbuildable
/// otherwise, a debounce being safe only with a guaranteed final flush.
///
/// Then the drops, in this order and for these reasons. `hooks` releases whatever an
/// observer holds — a file claim, for the first adapter — before any waiter is told
/// the actor ended, so `stop().await` returning means the claim is free. `inbox`
/// closes the mailbox before the same moment: `is_running()` is `!mail.is_closed()`,
/// which is false only once the receiver drops, and
/// `stop_drains_what_was_sent_before_it` asserts exactly that immediately after
/// `stop().await`. `feeds` releases the subscriptions. `state` goes last, which
/// answers every [`Deferred`] parked in it — as [`Withheld::Gone`], or as
/// [`Withheld::Restarting`] when a supervisor is putting another incarnation up.
/// Which of the two is not decided here: the verdict is settled into [`Fate`]
/// above, before the first of these drops, and each parked caller reads it.
///
/// Under [`supervise`] the `inbox` this drops is a [`Lent`] — a borrow of the
/// mailbox the outer loop owns — so dropping it releases the borrow and closes
/// nothing. That is the property the whole design rests on: the mailbox outlives
/// the incarnation with no re-queue, and `is_running()` stays `true` across a
/// restart, which is the truth.
///
/// The `inbox` drop is ordering insurance whose absence no test here catches:
/// RFC-06 §2.4 reports one teardown order failing 25 of 25 and the other passing 25
/// of 25, and removing this line and running the guard 25 times on this tree failed
/// 0 of 25. It stays because which order you get depends on capture order and
/// nothing pins that — but it is listed among the guards that were NOT watched to
/// fail, because a guard nobody has seen fail is not a guard.
///
/// **Unreachable on `abort` by construction, not by policy.** `abort()` is
/// `task.abort()` and nothing else: the future is dropped at its next await and the
/// loop never gets here. The one hole is on `abort`'s own doc — a turn that never
/// reaches an await is not interrupted, so such an actor can exit the loop normally
/// afterwards and then this does run.
///
/// # Two catches per observer in the `finish` chain, and no `guarded` around it
///
/// The same decision `observed_turn` makes one rung up, made here for both of its
/// reasons. A single `guarded` around the whole chain would need a built future, so
/// an `async` block, so a block capturing `&state` across an await — `Send` only
/// when `A: Sync`, which is the bound this design exists to keep out. The leak is not
/// a matter of care: this function is spawned under `A: Send + 'static` and nothing
/// more, so an `A: Sync` requirement introduced here is a compile error in keylink
/// itself rather than a downstream surprise. And it would stop the chain at the first
/// panic, where `finish` is a flush: an adapter that fell over must not cost the next
/// adapter its write, and there is no refusal left to short-circuit for.
async fn run_loop<A, M, F, I>(
    mut state: A,
    mut hooks: Vec<Box<dyn Observer<A>>>,
    mut feeds: F,
    mut inbox: I,
    fate: &Fate,
) -> (Ended, Standing)
where
    A: Send + 'static,
    M: Turn<A>,
    F: Feeds<A>,
    I: Inbox<A, M>,
{
    // **Declared first, so it drops last among this body's locals — and every
    // body local drops before a parameter.** `state` is a parameter, so an
    // unwind through this function settles the verdict before the state, and
    // therefore before every `Deferred` parked in it, dies. See [`Floor`] for
    // what that is a floor under and what it is not.
    let mut floor = Floor::armed(fate);
    let mut reason = Ended::Stopped;
    // Which input gets first refusal. Flipped after every item, so the loop
    // alternates rather than preferring one — see `next`.
    let mut mail_first = true;

    loop {
        let taken = next(&mut inbox, &mut feeds, mail_first).await;
        mail_first = !mail_first;

        match taken {
            Taken::Mail(Some(Message::Mail(mail, chain))) => {
                // The two lines around this await are the whole of
                // `ActorStatus::busy`, and they are here — in the one loop, at the
                // one dispatch — rather than inside the receive, so that a feed
                // turn and a mailbox turn cannot disagree about what a turn is.
                inbox.mark_busy(true);
                // The chain the sender was on, plus this actor. Everything sent
                // from inside the dispatch below carries it, which is what lets
                // `LocalActor::send` refuse a ring rather than only a self-call.
                let chained = Chained {
                    chain: chain.with(inbox.identity()),
                    inner: dispatch(mail, &mut state, &mut hooks),
                };
                let ended = chained.await;
                inbox.mark_busy(false);
                if let Some(end) = ended {
                    reason = end;
                    break;
                }
            }
            // No observers and no ending: a read cannot mutate, so there is
            // nothing for an observer to accept or refuse.
            Taken::Mail(Some(Message::Read(read))) => read(&state),
            Taken::Mail(Some(Message::Stop)) => break,
            // Every sender is gone, which for a `LocalActor` is the last handle
            // being dropped and for a keyed actor is the registry unmounting it.
            Taken::Mail(None) => break,
            Taken::Fed(Fed::Item(item)) => {
                inbox.mark_busy(true);
                // A feed item arrives from a stream and not from a sender, so it
                // starts a fresh chain holding this actor alone. A ring closed
                // through a feed is one of the residuals, and it is this line:
                // nothing upstream of the stream is named here.
                let chained = Chained {
                    chain: Chain::Empty.with(inbox.identity()),
                    inner: dispatch(item, &mut state, &mut hooks),
                };
                let ended = chained.await;
                inbox.mark_busy(false);
                if let Some(end) = ended {
                    reason = end;
                    break;
                }
            }
            // No turn ran, so no observer saw one — but `finish` still does, which
            // is what lets a durability adapter flush before the state goes.
            Taken::Fed(Fed::Ended { name }) => {
                reason = Ended::Feed { name };
                break;
            }
        }
    }

    // Before `finish`, before every drop, and therefore before a parked
    // `Deferred` dies with the state. A supervisor decides here whether it is
    // restarting; see `Inbox::ending`.
    //
    // **The one line the whole three-way answer rests on.** Everything below
    // this — the observers' `finish`, the mailbox that closes and drops what was
    // queued behind this turn, the state that takes its parked replies with it —
    // happens after it in this task's program order, so a reader woken by one of
    // those deaths has a happens-before edge to this store through the reply
    // channel it was waiting on. `deliver_unkeyed` and `deliver_keyed` both
    // read the result and cannot be early.
    //
    // Disarmed first, because from here the loop has an answer of its own and a
    // floor that fired afterwards would overwrite a `Restarting` with `Gone` —
    // which is a live actor reported as finished, the mirror of the defect the
    // floor exists for.
    floor.disarm();
    let verdict = inbox.ending(&reason);
    fate.settle(verdict.clone());

    for observer in &mut hooks {
        // Shared reborrow first, and the `FnOnce`-forcing rebind inside, for the
        // reasons `observed_turn` spells out.
        let shared: &A = &state;
        // Borrowed OUT here and not inside: `&reason` taken in the closure body
        // would make the closure own the `Ended` and the future borrow from its own
        // capture, which is `E0515` — and cloning it per observer, which is what
        // rustc suggests, would hand each one its own copy of a fact there is only
        // one of.
        let ending: &Ended = &reason;
        let watching: &mut dyn Observer<A> = &mut **observer;
        let built = catching(move || {
            let watching = watching;
            watching.finish(shared, ending)
        });
        // A panic in either half is dropped rather than reported. There is no caller
        // to resume it into and no reason left to change: `reason` has already been
        // handed to whichever observers ran before this one, and rewriting it here
        // would make two observers of one ending disagree about it. The panic hook
        // has already run at the fault, which is where the backtrace is.
        if let Ok(future) = built {
            let _ = guarded(future).await;
        }
    }

    drop(hooks);
    drop(inbox);
    drop(feeds);
    drop(state);
    (reason, verdict)
}

/// Run one keyed actor: the same loop, entered from a
/// [`Registry`](crate::Registry)'s mailbox.
///
/// `#[doc(hidden)] pub` because the generated keyed `spawn` calls it from inside
/// [`mount`](crate::mount)'s closure, in the **user's** crate. It is the whole of
/// what that closure does now; the ~150 lines it used to be were a second copy of
/// the loop above.
///
/// # The reason is **returned**, and it used to be dropped
///
/// This function's whole body was `let _reason: Ended = run_loop(..).await;`, and
/// the discarded binding was a live defect rather than a stylistic one. A keyed
/// actor that ends **on its own** — a feed that ran out, a handler that panicked,
/// an observer that refused — leaves a registry entry nothing removes:
/// [`Registry::status`](crate::Registry::status) went on listing it with `busy:
/// false` while a real call to it answered
/// [`NoSuchActor`](crate::RemoteError::NoSuchActor), and no
/// [`Lifecycle`](crate::Lifecycle) was told. `keylink::mount`'s `Loop` carries
/// the full argument; the return type here is the half of it that lives in this
/// file.
///
/// A keyed actor's ending is observable through [`Registry::ended`](crate::Registry::ended),
/// which subscribes to the entry by id before the registry reaps it. The ending
/// is therefore no longer lost: the registry and its lifecycle callbacks receive
/// this value as the actor leaves.
#[doc(hidden)]
pub async fn run_keyed<A, M, F>(
    state: A,
    hooks: Vec<Box<dyn Observer<A>>>,
    feeds: F,
    inbox: crate::Mailbox<M>,
) -> Ended
where
    A: Send + 'static,
    M: Turn<A>,
    F: Feeds<A>,
{
    // The registry made this cell and kept a clone beside the entry, so what the
    // loop settles on its way out is what `deliver_keyed` reads. Taken before
    // the mailbox moves into `Keyed`.
    let fate = inbox.fate();
    run_loop(state, hooks, feeds, Keyed(inbox), &fate).await.0
}

// ───────────────────────────────────────────────────────────────── supervision

/// How often a supervised actor may re-incarnate before it is given up on.
///
/// **No `Default`, and it is the `Eviction`/`Retention`/`Allowlist` family
/// rather than a tuning knob.** A number nobody chose is either a restart loop
/// that hides a permanent fault or a give-up that discards a recoverable one,
/// and which of those a wrong value produces depends on the caller's actor.
/// `representability.rs` holds the probe.
///
/// `Intensity::new(3, 10s)` is *three restarts in a row, and an incarnation that
/// stayed up ten seconds has recovered*. The second number is called
/// `recovered_after` and not `within` because it is a **lifetime one incarnation
/// must reach**, not a sliding window over wall clock: `max` consecutive
/// restarts are allowed, and the count resets when one incarnation runs for
/// `recovered_after` without ending.
///
/// The OTP reading — `max` restarts *within* a sliding `recovered_after` —
/// cannot terminate against the backoff below, which is `recovered_after / max`.
/// Spending the budget takes exactly `recovered_after`, so against a window of
/// the same length the oldest restart falls out just as the newest is granted:
/// an actor faulting on every turn restarts for ever at exactly the permitted
/// rate and [`Ended::GaveUp`] is unreachable. RFC-07 §9.1 works the table
/// through.
///
/// # The precondition: `recovered_after` must outlast a FAILING incarnation
///
/// An incarnation that reaches `recovered_after` and *then* fails has recovered
/// by this type's own definition — the count resets, and its failure starts a
/// fresh budget. So an actor whose failing incarnations reliably outlive
/// `recovered_after` never exhausts one, and **[`Ended::GaveUp`] is unreachable
/// for that actor**: it restarts for ever, one `recovered_after` apart.
///
/// That is the policy rather than a hole in it, and OTP's window has the same
/// property. What it costs the caller is that the number is chosen against an
/// actor's **failure period** and not against its latency: pick a
/// `recovered_after` shorter than the gap between failures you would still call
/// broken. `tests/supervision.rs::a_failure_slower_than_its_recovery_span_never_gives_up`
/// holds this exact behaviour, so a later reader finds it asserted rather than
/// discovers it.
///
/// # What [`new`](Self::new) rejects, and what it deliberately does not
///
/// The rejection closes two things exactly, and **narrows a third without
/// closing it.** Worth spelling out, because a constructor that refuses a value
/// invites the reading that everything it accepts is safe:
///
/// - **`Duration::ZERO` — closed.** `ran_for >= Duration::ZERO` is true of every
///   incarnation that ever ran, including one that ended instantly, so the reset
///   fires for **every** actor with no precondition left for a caller to hold.
///   That is the only value of which this is formally true.
/// - **A backoff that truncates to zero — closed.** `recovered_after / max` is
///   integer division on nanoseconds, so anything under `max` nanoseconds is a
///   restart loop with no sleep in it. `new` refuses those too, which is why the
///   guard is written on the backoff.
/// - **A `recovered_after` shorter than a failing incarnation's life — NOT
///   closed, and not closable here.** `Intensity::new(4, Duration::from_nanos(4))`
///   is accepted: its backoff is one nanosecond, and no real incarnation ends in
///   four nanoseconds, so in practice its budget resets on every failure just as
///   `ZERO`'s would. The difference from the two above is not degree but kind —
///   where that line falls is a fact about the caller's actor, not about the
///   value, so nothing a constructor can see decides it.
///
/// So the guard bounds the **sleep**, not the hazard. The precondition above is
/// what carries the rest, and it is prose because there is nothing else for it
/// to be.
///
/// # The backoff is `recovered_after / max`, and there is no knob for it
///
/// Spreading the budget across its own span makes exhausting it cost
/// `recovered_after` instead of microseconds, which is what stops a supervised
/// actor from turning a permanent fault into a hot loop. Exponential backoff
/// with jitter is the wrong instrument at this size: the budget is single
/// digits, so an exponential has no room to grow in, and one actor restarting
/// alone has no herd to disperse.
///
/// [`Ended::GaveUp`]: crate::Ended::GaveUp
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Intensity {
    max: NonZeroU32,
    recovered_after: Duration,
}

impl Intensity {
    /// `max` restarts in a row, and an incarnation lasting `recovered_after` has
    /// recovered. `None` when the backoff those two imply would be zero.
    ///
    /// `NonZeroU32` because a budget of zero is `LocalActor::spawn_fed` — an
    /// actor with one incarnation — spelled through a supervisor that does
    /// nothing, and one way to build a thing is the rule.
    ///
    /// **The rejected case is a supervisor that never sleeps.** The guard is on
    /// the *backoff* rather than on `recovered_after` alone because the backoff
    /// is `recovered_after / max` truncated to nanoseconds, so
    /// `Intensity::new(4, Duration::from_nanos(3))` is as much a busy loop as
    /// `Duration::ZERO` is. Both are refused, and `ZERO` is the case worth
    /// naming: `ran_for >= Duration::ZERO` is true of every incarnation that
    /// ever ran, so the budget resets on every failure for every actor, and the
    /// loop that then never terminates would spin with no `sleep` in it at all.
    ///
    /// **This does not make every accepted value safe**, and the type's own
    /// documentation says which third case it leaves open:
    /// `Intensity::new(4, Duration::from_nanos(4))` is accepted and is still a
    /// budget that resets on every real failure. The guard bounds the sleep; the
    /// precondition bounds the rest, and only the caller knows it.
    ///
    /// `Option` and the same shape as [`NonZeroU32::new`], one argument along,
    /// rather than a panic: a value a caller passed is checked where it is
    /// built, not where it is used.
    #[must_use]
    pub fn new(max: NonZeroU32, recovered_after: Duration) -> Option<Self> {
        let limit = Self {
            max,
            recovered_after,
        };
        if limit.backoff().is_zero() {
            return None;
        }
        Some(limit)
    }

    /// How long to wait between one incarnation and the next.
    ///
    /// **Never zero for an `Intensity` that exists**, which is what [`new`] is
    /// checking: a supervisor always sleeps between incarnations, so no
    /// configuration reachable from safe code turns a permanent fault into a
    /// loop with no yield in it.
    ///
    /// [`new`]: Self::new
    fn backoff(&self) -> Duration {
        self.recovered_after / self.max.get()
    }
}

/// Everything one incarnation needs, in the order the doors already take it.
///
/// The state, observer chain, and feeds a `restarting` factory produces in one
/// closure so that a restart cannot rebuild the state and reuse last
/// incarnation's observers — which for a durability adapter would mean reusing a
/// claim that has to be re-acquired, and for a feed would mean a subscription
/// nobody re-opened.
///
/// A tuple rather than a struct because it *is* those three arguments: a named
/// type here would be a second spelling of a call site that already exists, and
/// the reader would have to learn which field is which anyway.
///
pub type Incarnation<A, F> = (A, Vec<Box<dyn Observer<A>>>, F);

/// What a caller whose reply channel died without an answer is allowed to be told.
///
/// Three answers, and the caller's next move is different for each: retry, do not
/// retry this one call but the actor is fine, stop asking. [`lost`] renders them
/// for a peer, so the two served paths cannot disagree; a local
/// [`answered`](LocalActor::answered) projects the same three onto [`TurnLost`],
/// so the served and local doors cannot disagree either. **One cell, three
/// consumers, one meaning** — the alternative is what this replaced, where the
/// served path read a standing and the local path guessed from the silence.
///
/// # Why this is not the `ended` channel, which looks like it already says this
///
/// [`LocalActor`]'s `watch::Sender<Option<Ended>>` answers *has this actor
/// finished, and why*, for a caller awaiting [`ended`](LocalActor::ended) or
/// [`stop`](LocalActor::stop). It is filled **after** every drop `run_loop`
/// makes — deliberately, so that `stop().await` returning means an observer's
/// file claim is already released — and a reply that died *with* the state is
/// read long before that. Publishing it earlier to serve this question would
/// move a guarantee `stop` is documented on; carrying a second value is what
/// that costs.
///
/// **It also replaced a third state on that channel, rather than adding a
/// mechanism.** Wave 9 gave it a `Life::Restarting(Ended)` arm so that
/// `deliver_unkeyed` and `supervise` could both read a restart off it. Neither
/// does now — the first reads this, and the second reads this too — so the arm
/// went, the channel is back to `Option<Ended>` at eight bytes less per local
/// actor, and `run_keyed_supervised`'s `watch` channel *with no reader at all* is
/// deleted rather than explained.
///
/// It is also not a [`watch`] channel. Nobody awaits this: it is read once, on a
/// dead reply, by a task that is already awake. A `watch` per keyed actor would
/// buy a wait list, a version counter and a `Notify` for a value nothing ever
/// waits on, and a keyed actor's per-idle-actor cost is measured to the byte in
/// `tests/actor_memory.rs`.
#[derive(Debug, Clone, Default)]
pub(crate) enum Standing {
    /// An incarnation is running. A reply that died did so inside a live actor,
    /// which will answer the next call.
    ///
    /// The default, and the value an actor is created with: a loop that has not
    /// decided to end has not ended.
    #[default]
    Running,
    /// The incarnation that owed the answer is gone, and another is coming.
    ///
    /// Behind an [`Arc`] rather than inline, and it is the difference between
    /// this cell costing 80 bytes per idle actor and costing 48. `Ended` is 40
    /// bytes and this variant is written on a *restart* — so the allocation is
    /// paid when one happens, and every actor that never faults pays a pointer.
    /// `tests/actor_memory.rs` is what makes that a measurement rather than a
    /// preference.
    Restarting(Arc<Ended>),
    /// There will be no next incarnation.
    Gone,
}

/// One actor's [`Standing`], shared between its own loop and whoever serves it.
///
/// The shape `registry.rs`'s `Busy` already proved: a cell made where the actor
/// is created, cloned into the mailbox so the loop can write it, and kept beside
/// the registry entry so a dispatch can read it. `Busy` is `pub(crate)` in a
/// private module and has no path from the crate root, so it is named here and
/// not linked — the link that stood here, `crate::Busy`, resolved to nothing.
/// It travels to a keyed actor through
/// [`Mailbox`](crate::Mailbox) for the reason `Busy` does — widening
/// [`mount`](crate::mount)'s closure would put the pairing in *generated* code,
/// one expansion per `service!` in the world.
///
/// **Separate from the busy cell and not folded into it**, though both are one
/// word about one actor. `registry.rs`'s `Busy` is the registry's, written per
/// turn and read by [`Registry::status`](crate::Registry::status); a [`LocalActor`]
/// now carries its own, written through [`Watched`] and read by
/// [`busy`](LocalActor::busy). They stay two cells because they answer two
/// questions — this one says what a dead reply means, to a served dispatch and to
/// a local `ask` alike; that one says whether a turn is running, to a handle's
/// holder — and folding them would force a registry fact onto a path whose reason
/// for existing is that it has no registry.
///
/// This paragraph said a `LocalActor` had no busy cell "on purpose", citing the
/// refusal that used to stand on `Inbox::mark_busy`. That refusal weighed the
/// idle actor and is rewritten there; a paragraph citing it had to move in the
/// same commit, or the crate would document a capability it ships.
///
/// # A `Mutex` and not an atomic
///
/// [`Standing::Restarting`] carries the ending that caused it, because
/// [`Withheld::Restarting`] carries an [`EndReason`] to a peer and to a local
/// caller alike, and the two paths render through one function. An `AtomicU8`
/// would hold the
/// discriminant and drop the sentence, which is the one thing that would make the
/// keyed answer *different* rather than merely late. It is locked once per
/// incarnation and once per dead reply, never on a turn, so there is nothing here
/// to contend for.
#[derive(Debug, Clone, Default)]
pub(crate) struct Fate(Arc<Mutex<Standing>>);

/// Settles [`Standing::Gone`] if the code it guards never reached a verdict of
/// its own.
///
/// # The hole it fills, which the wave that built [`Fate`] left open
///
/// Every settle in this file is on a path the loop *reaches*. Two panics do not
/// reach one: a caller's `factory`, awaited by [`supervise`] with no guard, and a
/// caller's `Feeds::poll_next`, polled by [`next`] inside a `poll_fn` with no
/// guard — the mailbox turn has [`catching`] around it and the second input does
/// not. Either unwinds the actor's whole task, so nothing publishes an ending and
/// nothing settled the verdict: the cell kept `Running` for the rest of the
/// process and a peer whose reply died in that unwind was told `dropped: `, *the
/// actor is serving*, about an actor that would never answer again.
///
/// It was a **regression** against the shape this design replaced, which is why
/// it is a floor here and not a filed defect. `deliver_unkeyed` used to read
/// `is_running()` beside the verdict; an unwind drops the `mpsc::Receiver`, and
/// `Receiver::drop` closes the channel *before* it drains, so `is_closed()` was
/// already true when the queued message's reply died. Deleting the cross-check
/// deleted the only thing covering an unwind.
///
/// # What it rests on, stated rather than assumed
///
/// Drop order: **reverse declaration, and a body's locals before the function's
/// parameters.** So a `Floor` declared first in a body drops before `state`,
/// `inbox` and `feeds`, which are parameters — which is the whole of the
/// ordering claim, and it is the language's rule rather than an optimisation.
///
/// It is a *floor*, not the mechanism. On every path the loop reaches, the
/// verdict is settled explicitly at the top of the teardown and this is
/// disarmed; the floor only has to beat the drops that follow an unwind. The
/// keyed path has a second one in `Mailbox`'s `Drop`, and this one covers the
/// local path, which has no `Mailbox` at all.
///
/// # Why not catch the two panics instead
///
/// Considered and refused for this wave, deliberately rather than by omission:
/// `guarded` around `factory().await` and `catching` around `feeds.poll_next`
/// would remove the unwind rather than floor it, and would turn both into a named
/// [`Ended`] a caller could await. It also changes what a panicking factory and a
/// panicking feed *mean* — today they vanish the task, and that would make them
/// `GaveUp` and an ending respectively. That is a supervision-semantics decision
/// with its own guards to write, and it is not this wave's. The floor is correct
/// under either, and stays correct if the panics are caught later.
struct Floor<'a> {
    fate: &'a Fate,
    armed: bool,
}

impl<'a> Floor<'a> {
    fn armed(fate: &'a Fate) -> Self {
        Self { fate, armed: true }
    }

    /// The guarded code reached a verdict of its own, so this must not overwrite
    /// it.
    ///
    /// **Called immediately before each way the guarded scope returns, and
    /// nowhere else.** The doc here said "at exactly one place per floor", which
    /// [`supervise`] has never satisfied — it has two returns — and stating a
    /// discipline the tree does not keep is how the next author trusts the wrong
    /// one. The rule that matters is not *how many* but *where*: a disarm that is
    /// not the last thing before a return leaves a window with no floor, which is
    /// exactly what an early disarm before `run_loop` did to the backoff.
    ///
    /// # "And nowhere else" includes around a nested call, and that is load-bearing
    ///
    /// Reading this rule as *disarm wherever the guarded code has its own floor*
    /// is the mistake it invites, because a nested call is not a way this scope
    /// returns. `supervise` deliberately stays armed across [`run_loop`]: a
    /// caller's state whose `Drop` panics on the restart path unwinds after
    /// `run_loop`'s floor has been disarmed and after the verdict was settled
    /// `Restarting`, and this is the only floor left for the `Deferred` dying in
    /// that unwind. Disarming there answers *retry, another is coming* about an
    /// actor that will never come back.
    ///
    /// **"Left for" and not "left under": for that one case this floor is
    /// necessarily LATE.** The `Deferred` dies inside `drop(state)`, in
    /// `run_loop`'s frame; this floor fires when the unwind reaches `supervise`'s,
    /// which is strictly outer. So it cannot precede the death — it makes the cell
    /// right for every later reader without making it right for the peer that
    /// death woke. RFC-07 §12.4 carries it as a bounded residual, and it is
    /// unfixable with this mechanism: settling `Gone` *before* `drop(state)` would
    /// overwrite `Restarting` on the normal restart path, which is N2 exactly.
    ///
    /// `a_state_that_panics_on_drop_mid_restart_is_gone_not_restarting` holds it,
    /// so the rule is enforced rather than merely written down.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for Floor<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.fate.settle(Standing::Gone);
        }
    }
}

impl Fate {
    /// Publish what a reply that dies **from here on** means.
    ///
    /// Every caller of this is a place where the actor's future is already
    /// decided and nothing has been dropped yet. There are four, and each states
    /// its own ordering where it is written: [`run_loop`], which calls it with
    /// whatever [`Inbox::ending`] decided; [`supervise`], on the give-up that
    /// happens before any incarnation exists; the three `abort` doors, which
    /// publish *before* they ask the runtime to destroy the task; and
    /// `Mailbox`'s `Drop`, which is the floor under a hand-written loop.
    pub(crate) fn settle(&self, standing: Standing) {
        // The same call the registry's own guards make: a panic under this lock
        // must cost one message, not wedge every later answer.
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = standing;
    }

    /// What a reply that has **just** died means.
    ///
    /// Read only after a reply channel resolved `Err`, which is what makes the
    /// answer sound: the drop that woke the reader happens-after everything the
    /// actor's task did before it, and the settle above is one of those things.
    pub(crate) fn standing(&self) -> Standing {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// The restart accounting, kept by one supervisor for one actor.
///
/// Separate from [`Intensity`] because that is the caller's declaration and this
/// is what happens to it — and because [`Lent`] borrows this across an
/// incarnation while the declaration is `Copy`.
struct Budget {
    limit: Intensity,
    /// Restarts since the last incarnation that reached `recovered_after`.
    used: u32,
    /// Restarts granted in total, which is what [`Ended::GaveUp`] reports.
    spent: u32,
}

impl Budget {
    fn new(limit: Intensity) -> Self {
        Self {
            limit,
            used: 0,
            spent: 0,
        }
    }

    /// Grant a restart, or refuse because the budget is out.
    ///
    /// `ran_for` is how long the incarnation that just ended lasted; a factory
    /// that failed before one existed passes `Duration::ZERO`, which is true and
    /// is why it counts against the budget exactly like a fault. That is what
    /// makes a state another machine has claimed end as [`Ended::GaveUp`] rather
    /// than loop on a re-acquire that will never succeed.
    ///
    /// **An incarnation that reached `recovered_after` and then failed resets
    /// the count**, which is [`Intensity`]'s definition of *recovered* and is the
    /// precondition stated there: an actor whose failures are always slower than
    /// its `recovered_after` never exhausts a budget. `Intensity::new` refuses
    /// the one span for which that is true of every actor.
    fn spend(&mut self, ran_for: Duration) -> bool {
        if ran_for >= self.limit.recovered_after {
            self.used = 0;
        }
        if self.used >= self.limit.max.get() {
            return false;
        }
        self.used += 1;
        self.spent += 1;
        true
    }
}

/// Whether another incarnation is worth building.
///
/// A pure function of the ending, so that the supervisor's decision is readable
/// in one place rather than spread across the loop that makes it.
fn retryable(reason: &Ended) -> bool {
    match reason {
        // Somebody asked, or the last handle went. Restarting would be
        // overruling them.
        Ended::Stopped => false,
        // `abort` drops the task at its next await, so the loop never reaches an
        // ending to report. Written to be correct rather than to panic, which is
        // the choice `Mail`'s `send` and `recv` variants already make.
        Ended::Vanished => false,
        // A supervisor's own ending. No supervisor sees another's.
        Ended::GaveUp { .. } => false,
        // The one ending an author can declare unrepeatable. See `Stop::fatal`.
        Ended::Observer { fatal, .. } => !fatal,
        // A fault and a dead feed are what a restart is for: the state goes back
        // to its last commit, and the factory re-subscribes.
        Ended::Panicked { .. } | Ended::Feed { .. } => true,
    }
}

/// Turn an incarnation's ending into the actor's, now that there will be no more.
fn gave_up(reason: Ended, after: u32) -> Ended {
    let error: Arc<dyn Error + Send + Sync> = match reason {
        // The observer's own error, chain and all, rather than its rendering.
        Ended::Observer { error, .. } | Ended::GaveUp { error, .. } => error,
        other => Arc::from(Box::<dyn Error + Send + Sync>::from(other.to_string())),
    };
    Ended::GaveUp { after, error }
}

/// The mailbox an incarnation is **lent**, and the supervisor's decision point.
///
/// The outer loop owns the receiver and hands one of these to each incarnation,
/// which is the whole structural idea: `task`, `abort()` and the mailbox are the
/// actor's rather than the incarnation's, so a restart needs no re-queue, cannot
/// resolve [`ended`](LocalActor::ended), and leaves
/// [`is_running`](LocalActor::is_running) `true` — which is the truth.
struct Lent<'a, I> {
    inbox: &'a mut I,
    budget: &'a mut Budget,
    /// When this incarnation started, for [`Budget::spend`].
    started: Instant,
}

impl<A: Send, M: Send, I: Inbox<A, M>> Inbox<A, M> for Lent<'_, I> {
    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Message<A, M>>> {
        self.inbox.poll_next(cx)
    }

    fn mark_busy(&self, running: bool) {
        self.inbox.mark_busy(running);
    }

    /// Forwarded, exactly as `mark_busy` above is, and it is the one line that
    /// keeps the re-entrancy check alive on the production supervised path. See
    /// [`Inbox::identity`] for what forgetting it would have cost and why nothing
    /// would have complained.
    fn identity(&self) -> Option<Actor> {
        self.inbox.identity()
    }

    /// Decide — **before** the state is dropped.
    ///
    /// The decision is made here and nowhere else, and it is *published* by
    /// [`run_loop`] one line later, into the cell [`supervise`] reads back. Making
    /// it after `run_loop` returns would be a race with the `Deferred` replies
    /// that die when the state does, and the peer waiting on one would be told
    /// `NoSuchActor` for an actor that is about to answer it.
    ///
    /// **Both arms answer**, which is the correction this shape enforces. The
    /// version this replaces wrote a `watch` channel itself, on the one arm that
    /// granted a restart, and returned nothing: a supervised actor's permanent
    /// give-up published *nothing at all*, so it went on reading as running with
    /// a mailbox `supervise` still owns — and a peer whose reply died on that
    /// give-up was told `dropped: `, *the actor is still there*, about an actor
    /// that would never answer again. A hook that has to hand a value back cannot
    /// have an arm that quietly says nothing.
    ///
    /// # The same arm closes the mailbox, and that is a second consequence rather than a second decision
    ///
    /// A restart keeps the mailbox open with no re-queue, which is the whole
    /// structural idea of [`Lent`]. A give-up must not: [`supervise`] owns the
    /// receiver by value for the actor's whole life, so without this
    /// [`LocalActor::is_running`] answered `true` for an actor that would never
    /// take another message, and [`ask`](LocalActor::ask) queued into a mailbox
    /// nobody would drain. `Receiver::close` refuses new senders immediately and
    /// still lets the buffer drain, which is exactly what dropping the receiver
    /// gives the *un*supervised path for free.
    ///
    /// It is deliberately **not** what a caller whose reply died reads — that is
    /// the `Standing` this returns, and a served dispatch and a local
    /// [`ask`](LocalActor::ask) both read it now. One decision, made here, with two
    /// consequences published at the two places that need them.
    fn ending(&mut self, reason: &Ended) -> Standing {
        if retryable(reason) && self.budget.spend(self.started.elapsed()) {
            return Standing::Restarting(Arc::new(reason.clone()));
        }
        self.close();
        Standing::Gone
    }

    fn close(&mut self) {
        self.inbox.close();
    }
}

/// The outer loop: build an incarnation, run it, decide, repeat.
///
/// **A loop around the actor's own task, not a second task spawning children.**
/// RFC-04 §8 refused a `Supervisor` type and issue #3 refuses `Strategy`,
/// `OneForAll`, `RestForOne` and parent/child trees for a reason that survives
/// the overturn of §8 itself: in OTP `one_for_all` exists because a crashed
/// process leaves its peers holding a dead pid, and here identity does not die
/// with the incarnation — this loop keeps the `mpsc::Sender`, so a handle held
/// anywhere goes on working. **There is no stale reference to repair.**
///
/// Runs on the actor's own task and owns `inbox` for the actor's whole life. The
/// factory produces state, observers and feeds together, so each incarnation
/// gets fresh ones and [`run_loop`] has dropped the old ones — including a
/// durable `Claim`, which `observe.rs` says dies with the actor deliberately —
/// before the next factory call. That is what bounds the re-acquire window to
/// `backoff + claim latency` and stops the process racing itself for its own
/// claim.
///
/// # A restart rolls the state back and does not roll the key back, and that is a gap
///
/// Written here because this is the only frame that knows an incarnation was
/// replaced. The ledger never learns of a restart — it is per service and it
/// **survives** one intact — so `idempotency.rs` cannot state this, and the refusal a
/// caller receives cannot either.
///
/// The chain, and every link is needed. A keyed call reaches a turn, the turn is
/// marked dispatched, the turn panics. A panic **alone** stops here and costs one
/// message: `tests/actors.rs::a_panicking_message_costs_one_message_and_not_the_actor`
/// holds that, and nothing restarts. What produces a restart is an
/// [`Observer`] refusing the faulted turn — the shape a
/// [`Persist`](crate::Persist) adapter always has, since it will not write a
/// half-applied state — which `observed_turn` folds into [`Ended::Panicked`], which
/// [`retryable`] grants. The next incarnation starts from the last committed state, as
/// `tests/supervision.rs::an_actor_restarts_from_its_last_committed_state` asserts.
///
/// So the turn's effect on the state is gone, and the key still reads
/// [`Interrupted`](crate::IdempotencyRefusal::Interrupted) — *it ran, and whether it
/// took effect is unknown* — for the ledger's whole retention. The caller can neither
/// retry under that key nor learn which of the two happened.
///
/// **It is left open, and the reason is not that a test would have to change.** The
/// rollback is of keylink's **state**, not of the **world**. A turn that charged a card
/// over HTTP and then panicked leaves the card charged; this loop restores an in-memory
/// struct and reaches none of that. Releasing the key would let a retry charge it
/// twice, which is the double-effect the ledger exists to prevent. Promoting *the state
/// did not survive* into *the work did not happen* is a claim only the caller can make,
/// because only the caller knows what its handler touched.
///
/// **Nothing guarded the combination until this branch.** `tests/actors.rs` still mounts
/// no supervisor, and no test named an idempotency key on a supervised actor before
/// `tests/supervision.rs::a_keyed_call_whose_turn_panicked_stays_interrupted_across_a_restart`,
/// which was added with this paragraph. **What it pins is the answer a caller gets, not
/// that the answer is right** — the paragraphs above are what argue that, and no test can
/// stand in for them: the claim they make is about the world the handler touched, which
/// the suite cannot reach.
///
/// **Releasing the key instead is not a one-guard change.** Make `Guard::drop`'s
/// `Reached::NotDispatched` test always take the releasing arm, and the guard above
/// reddens together with these in `idempotency.rs`:
/// `claim::a_guard_dropped_after_dispatch_leaves_the_key_interrupted`,
/// `claim::a_guard_dropped_without_a_reply_leaves_the_key_interrupted`,
/// `records::records_none_changes_nothing_about_who_settles` and
/// `state::key_state_reports_every_state`. So *leave it open* is a decision measured
/// against named guards rather than a preference between two readings.
///
/// **Under that release the double effect is observed rather than argued.** The guard
/// above then fails carrying a `Withheld::Panicked`, and a variant whose payload is a
/// panic message can only be produced by ENTERING the handler — so the retry ran the turn
/// a second time on an actor whose first attempt had already panicked. That is the
/// double-effect the paragraph above says the ledger exists to prevent, seen rather than
/// predicted.
///
/// **Measured whole-lib by this branch's merge gate, and not re-run here.** Nothing in
/// this repository replays a recorded break, so the list above names one run rather than
/// a standing check.
async fn supervise<A, M, F, Fut, N, I>(
    mut factory: N,
    limit: Intensity,
    mut inbox: I,
    fate: &Fate,
) -> Ended
where
    A: Send + 'static,
    M: Turn<A>,
    F: Feeds<A>,
    I: Inbox<A, M>,
    N: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<Incarnation<A, F>, Stop>> + Send,
{
    // **One floor for the whole function, disarmed only immediately before each
    // way out.** Everything it covers is a window where `run_loop` — and
    // therefore the floor `run_loop` carries — does not exist: the caller's
    // factory call, the transient-failure retry sleep, and the backoff between
    // one incarnation and the next.
    //
    // It was a *per-iteration* floor first, and that shape had a hole at each
    // end. `continue` drops a loop-body local, so the transient-failure retry —
    // a documented, first-class path — dropped it **armed** and wrote `Gone` over
    // a live `Restarting`: *stop asking*, about an actor the supervisor was
    // actively rebuilding. And it was disarmed for the backoff at the bottom of
    // the body, so an `abort` between incarnations left `Restarting` standing:
    // *retry, another is coming*, about an actor that had just been destroyed.
    // Both are the **false** answer this design refuses rather than the merely
    // less useful one RFC-07 §12.4 accepts.
    //
    // **Staying armed across `run_loop` is REQUIRED, not merely harmless.** This
    // comment said "nothing else drops this, so nothing else can fire it", which
    // reads as a licence to disarm here and re-arm after — and doing that
    // reintroduces a defect:
    //
    //   a caller's state whose `Drop` **panics** on the restart path unwinds
    //   `run_loop` *after* the verdict was settled `Restarting` and after
    //   `run_loop`'s own floor was disarmed. The parked `Deferred` dies inside
    //   that unwind. This floor is the only thing left to catch it, and without
    //   it the peer reads `Withheld::Restarting` — *retry, another is
    //   coming* — about an actor that will never come back.
    //
    //   For *that* case the floor is necessarily late rather than early: the
    //   death is inside `run_loop`'s frame and this fires in `supervise`'s, so it
    //   corrects the cell rather than pre-empting the read. §12.4 records it as a
    //   residual the floor bounds and does not close.
    //
    // So the rule on `Floor::disarm` is *immediately before each way the scope
    // returns*, and `run_loop` is not a way this scope returns. There is no third
    // disarm, and `a_state_that_panics_on_drop_mid_restart_is_gone_not_restarting`
    // is what stops one being added: it is the only guard in this design that
    // holds a line whose correctness was otherwise defended by a comment.
    //
    // Disarming before the returns and nowhere else is also what keeps the
    // give-up guards attributable: a floor that fires *after* an explicit settle
    // re-writes the right answer over a broken one and leaves the break green.
    let mut floor = Floor::armed(fate);
    let mut budget = Budget::new(limit);
    loop {
        let built = factory().await;
        let (state, hooks, feeds) = match built {
            Ok(triple) => triple,
            // No incarnation ran, so nothing published `Restarting` and nothing
            // spent the budget. Both happen here instead, and the ZERO is the
            // honest duration of a build that produced no actor.
            Err(stop) => {
                let (error, fatal) = stop.into_parts();
                if !fatal && budget.spend(Duration::ZERO) {
                    n0_future::time::sleep(limit.backoff()).await;
                    continue;
                }
                // The one give-up `Inbox::ending` cannot see, because no
                // incarnation ever reached a loop to end. It has to be settled
                // **here**, before the return: `inbox` is this function's own
                // parameter and drops on the way out, and dropping it is what
                // kills every message still queued behind an actor that will
                // never exist. Their callers must read *gone*, not *running*.
                floor.disarm();
                fate.settle(Standing::Gone);
                // And the mailbox stops taking them, for `Lent::ending`'s reason:
                // this actor will not run, so a caller must meet a closed mailbox
                // now rather than a permit into a queue nobody will drain.
                inbox.close();
                return Ended::GaveUp {
                    after: budget.spent,
                    error: Arc::from(error),
                };
            }
        };

        // The new incarnation is live from here. Without this the cell would keep
        // the previous one's `Restarting` for ever, and a reply this incarnation
        // dropped while perfectly alive would read as a restart. Published before
        // the loop starts, which is the only side to err on: no reply of this
        // incarnation's can die before it runs.
        fate.settle(Standing::Running);
        let lent = Lent {
            inbox: &mut inbox,
            budget: &mut budget,
            started: Instant::now(),
        };
        let (reason, verdict) = run_loop(state, hooks, feeds, lent, fate).await;

        // The verdict `Lent::ending` made, **returned** rather than read back out
        // of the cell. It was a read: `matches!(fate.standing(), ..)`, which was
        // benign and was one writer away from not being. Four doors settle that
        // cell now — this function, the loop, the two teardowns and `Mailbox`'s
        // own `Drop` — and a supervisor asking a shared cell what it had just
        // decided would be the first of them to be wrong. It has one path to the
        // answer that nothing else can reach.
        let restarting = matches!(verdict, Standing::Restarting(_));
        if !restarting {
            // `run_loop` settled its own verdict, so the floor must not write
            // over it on the way out — that is what made this guard's break go
            // green the first time.
            floor.disarm();
            return match reason {
                // Asked to stop, or every handle went. Not a failure, and not
                // something anybody gave up on.
                Ended::Stopped | Ended::Vanished => reason,
                failed => gave_up(failed, budget.spent),
            };
        }
        // Still armed across this, and that is the whole of N1: there is no
        // `run_loop` between one incarnation and the next, so an `abort` landing
        // here would otherwise leave the previous incarnation's `Restarting`
        // standing for an actor that has been destroyed.
        n0_future::time::sleep(limit.backoff()).await;
    }
}

/// Run one **supervised** keyed actor: the same outer loop, entered from a
/// [`Registry`](crate::Registry)'s mailbox.
///
/// `#[doc(hidden)] pub` for [`run_keyed`]'s reason: the generated keyed
/// `spawn_supervised` calls it from inside [`mount`](crate::mount)'s closure, in
/// the user's crate. The registry is untouched by any of this — `mount` takes a
/// closure over a `Mailbox`, and this is that closure's whole body.
///
/// A supervised keyed actor's ending is observable through
/// [`Registry::ended`](crate::Registry::ended). The registry owns one shared watch
/// channel per live entry; the supervisor's restart verdict remains in the
/// [`Fate`] cell, while the channel resolves only when the final incarnation ends.
///
/// # One mechanism serves both keyed paths, and that is not a coincidence
///
/// [`run_keyed`] returns the ending it used to drop, and so does this — the same
/// return type, reaped at the same site, because a **restart is not an ending**.
/// [`supervise`] runs many incarnations behind one `Output`, and it returns only
/// once there will be no more, which is exactly the instant the registry entry
/// should go. A per-path mechanism would have had to decide that again; a return
/// type does not.
///
/// [`Ended::GaveUp`] is therefore the one ending this path can produce that the
/// unsupervised one cannot, and it now reaches the same
/// [`Lifecycle`](crate::Lifecycle) as every other.
///
/// # What a peer is told when a keyed restart loses its reply
///
/// The reap above settles what happens when a supervised keyed actor is *finished*.
/// It does not settle what a peer hears while one is *between* incarnations, and
/// that is a different answer:
///
/// **A `Deferred` parked in the state dies with the incarnation, and that loss now
/// reaches the peer as [`Withheld::Restarting`]** — *the incarnation that
/// owed you an answer is gone, another is coming, retry*. It used to be
/// [`RemoteError::NoSuchActor`], which reads *there is nothing here, stop asking*
/// about an actor that is about to answer the same peer again, and a client that
/// treats the name as terminal — which is what the name tells it to do — stopped
/// retrying an actor that was coming back.
///
/// **The other two causes are answered too, and neither needed a supervisor.** A
/// restart is one of *three* ways a served reply dies: a handler that takes
/// `reply.later()` and drops the `Deferred` — sanctioned, and what its
/// `#[must_use]` asks for — reads [`Withheld::Dropped`], and an actor that has
/// genuinely ended, or was aborted or evicted, reads [`Withheld::Gone`]. So a
/// plain unsupervised keyed actor no longer says *there is nothing here* about a
/// reply it dropped while running. `deliver_keyed` and `deliver_unkeyed`
/// render all three through one function, [`lost`], so the two paths cannot say
/// different things about one condition.
///
/// The signal is the [`Fate`] cell, settled by this actor's own loop before it
/// drops anything and read off the registry entry — which is why none of it
/// reaches `Mountable`, `mount` or the generated `spawn_supervised`. RFC-07 §12
/// carries the design and §9.10 is where the cost was priced at six touch points
/// and a public type.
///
/// Every *other* message survives a keyed restart: a turn in flight is answered
/// before the loop breaks, and queued turns stay in the mailbox the supervisor
/// owns.
///
/// [`RemoteError::NoSuchActor`]: crate::RemoteError::NoSuchActor
#[doc(hidden)]
pub async fn run_keyed_supervised<A, M, F, Fut, N>(
    factory: N,
    limit: Intensity,
    inbox: crate::Mailbox<M>,
) -> Ended
where
    A: Send + 'static,
    M: Turn<A>,
    F: Feeds<A>,
    N: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<Incarnation<A, F>, Stop>> + Send,
{
    let fate = inbox.fate();
    supervise(factory, limit, Keyed(inbox), &fate).await
}

/// A local call that got no answer, and why.
///
/// Not [`Error`](crate::Error), and deliberately not convertible into one:
/// `Error::Transport`, `Error::SchemaMismatch` and `Error::Closed` have no local
/// counterpart, and a caller must not be able to `?` a local failure into a
/// remote error type and lose which one it was. RFC-04 §5.3.
///
/// # It replaced a unit struct called `MailboxClosed`, and the name was the fault
///
/// That type's own first line read *"The actor is gone. The only way a local call
/// fails."* The second sentence stopped being true when a re-entrant call became
/// refusable, and the first is the reason the type could not simply grow: a
/// refused re-entrant call leaves the actor **alive**, so reporting it as closed
/// would be the same lie `ask_catching`'s `Expired` arm names — a live actor
/// reported as gone through a public API, silently.
///
/// # It was `Undelivered` for one wave, and a downstream author meets two errors
///
/// `MailboxClosed` became `Undelivered` at `4e0fcca`, and `Undelivered` became
/// `Unanswered` at `8acd7be`. Both moves are loud at compile time, so nobody is
/// silently wrong — but they land together and look unrelated: `E0433` on the old
/// path, then `E0382` at the first site that relied on the `Copy` this type no
/// longer has. **They are one intended change**, and the section below says why the
/// derive went.
///
/// `Undelivered` named the *delivery*, and this type is about the **answer**. A
/// re-entrant call is refused before anything is queued, and a turn that ran and
/// was refused by an observer was delivered perfectly. Neither is undelivered.
/// Both are unanswered.
///
/// # Two variants, because six of the eight were never this type's own
///
/// It listed the same six losses [`TurnLost`] did, in the same order, with the
/// same `#[error]` texts written a second time — and one test walked the two
/// lists variant-by-variant to hold the words together. The words stayed in
/// step. The **payloads** did not: [`TurnLost`]'s restart arm carried the
/// [`Ended`] and this type's carried a bare unit, so the door a caller picked
/// decided whether the reason existed, and the guard passed on both.
///
/// [`Withheld`] is that list, once. What is left here is the one refusal that is
/// not about a turn.
///
/// # Losing `Copy` is what buys the reason back
///
/// `Copy` and `Eq` are why the restart arm was a unit: an `Arc<Ended>` is
/// neither, so the *fact* crossed this door and the *reason* did not, and
/// [`ended`](LocalActor::ended) is no second route to it — a restart is not an
/// ending, so that future does not resolve. [`Withheld`] carries an
/// [`EndReason`], which is data rather than a trait object,
/// and it costs `Copy`. `Clone`, `PartialEq` and `Eq` are kept, so a caller that
/// matched and compared still does.
///
/// # Exhaustive, and deliberately not `#[non_exhaustive]`
///
/// [`Ended`] and [`TurnLost`] carry that argument: a caller matches this, and a
/// wildcard arm is the collapse the split exists to prevent. A new loss is added
/// to [`Withheld`], which makes the same choice — so exhaustiveness reaches a
/// caller one level in rather than stopping at this enum's two arms.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unanswered {
    /// The turn was reached and its value was not handed back, and this is which
    /// loss it was.
    ///
    /// [`Withheld`] carries what each one means and what to do next — retry a
    /// restart, stop calling a gone actor, do neither for a reply a live actor
    /// threw away. [`TurnLost::Turn`] is the same value at
    /// [`ask_catching`](LocalActor::ask_catching), and a peer reads it as
    /// [`RemoteError::Withheld`].
    ///
    /// **[`Withheld::Panicked`] does not arrive here, and no local door builds
    /// one.** A fault reaches [`ask_catching`](LocalActor::ask_catching) as
    /// [`TurnLost::Faulted`], carrying the payload, and
    /// [`ask`](LocalActor::ask) resumes that payload into the calling task. So
    /// the panic reaches its caller as a panic, and the message-shaped variant
    /// is what a peer reads instead.
    #[error("{0}")]
    Turn(Withheld),
    /// The call was made from inside the target actor's own turn, and would have
    /// deadlocked.
    ///
    /// The actor is **still running** — it is running the turn this call was made
    /// from. Call the method on `&mut self` instead; it is the same code without
    /// the round trip. If the work must happen after the current turn, spawn a
    /// task that holds a handle, which is not the actor and can wait for a permit
    /// safely.
    ///
    /// Outside [`Withheld`] because nothing was queued: every variant of that
    /// enum is a fact about a turn that exists, and this refusal fires before one
    /// does.
    ///
    /// A `tell` is refused only when the mailbox is **full**, because a self-tell
    /// into a free slot runs after the turn returns and is legitimate. Every
    /// other local call waits for an answer only the blocked turn could produce,
    /// so a free slot does not save it and it is refused at any depth.
    #[error("this call was made from inside the target actor's own turn: it would deadlock")]
    Reentrant,
    /// The mailbox was full and this actor was spawned under
    /// [`Backpressure::Shed`], so the call was refused instead of parked.
    ///
    /// Outside [`Withheld`] for [`Reentrant`](Self::Reentrant)'s reason and not a
    /// weaker one: the refusal happens before the message reaches the mailbox, so
    /// nothing ran and nothing was withheld. It is the second fact of that shape,
    /// and [`NotQueued::Full`] carries the rest of the argument — including why
    /// the arguments are gone and which door loses nothing instead.
    ///
    /// # Why this is a variant here rather than an eighth [`Withheld`]
    ///
    /// This is what the release's major version bought, so the three grounds are
    /// written out rather than pointed at. A fold into `Withheld` would have cost
    /// no variant on this enum and broken no exhaustive match on it.
    ///
    /// 1. **[`Withheld`] is a verdict on a turn that ran.** Every variant it has
    ///    describes what happened to a turn that exists, and a shed message never
    ///    reached the mailbox. [`Reentrant`](Self::Reentrant) is the standing
    ///    precedent and it was decided the same way, before this variant existed.
    /// 2. **[`Withheld`] crosses the wire and this enum does not.** It derives
    ///    `Schema`, so an eighth variant moves the ALPN and the break lands on
    ///    every peer — including deployments that never spawn under
    ///    [`Backpressure::Shed`]. Breaking three local
    ///    enums leaves the wire where it is.
    /// 3. **The downstream break is the same either way.** This type's own
    ///    exhaustiveness heading says a new `Withheld` variant reaches a caller
    ///    *one level in*, so a caller who matched
    ///    [`Turn`](Self::Turn)`(..)` exhaustively is broken by the fold as surely
    ///    as by this variant. The alternative buys no compatibility; it moves the
    ///    same break onto a type that need not move at all.
    ///
    /// Injectivity is **not** one of the grounds, and the `From<NotQueued>` impl
    /// below says so where a reader is likeliest to reach for it.
    ///
    /// **Unreachable under [`Backpressure::Park`]**, which is the default. Retry
    /// after a backoff, or take a [`Permit`] and keep the arguments.
    #[error("the actor's mailbox is full and it shed this message")]
    Full,
}

/// A message that never entered the mailbox, and which of the three ways.
///
/// The error of the doors that ask *did this get queued* — [`LocalActor::notify`]
/// and [`LocalActor::stop`], plus the private `send` they both go through.
/// [`Unanswered`] is the error of the doors that ask *did I get a value*:
/// [`ask`](LocalActor::ask) and [`read`](LocalActor::read).
///
/// # Two types because the two questions have different answers, not different styles
///
/// Everything `Unanswered` names beyond these two states is a verdict on a turn
/// that already exists, and it arrives on a **reply channel**. These doors have
/// none to hear it on: `stop` sends a request that is not a turn, and `notify`
/// drops the receiving half before it sends. So one shared type would put on a
/// public `stop` every state a lost reply can be in, and a caller matching it
/// would be writing arms for states `stop` cannot produce.
///
/// # `stop` is at one of these two, and that trade is taken on purpose
///
/// The split did not reach zero unreachable states. [`stop`](LocalActor::stop)
/// awaits the ending itself and turns a closed mailbox into `Ok` — the actor it
/// was asked to stop is stopped — so [`Reentrant`](Self::Reentrant) is the only
/// variant it can return, and this type's own argument above is live at one of two
/// for one of its two doors.
///
/// **A one-variant error type for `stop` is refused.** Two error types beat three:
/// the third would exist to delete a state that a caller can already see is
/// impossible, because `stop`'s own `# Errors` says so in one sentence and the
/// compiler never asks a caller to match it. That is a trade rather than an
/// oversight, and it is written here because an unstated trade reads as the
/// second one.
///
/// # The name is `NotQueued`, and three others were weighed
///
/// It says what the send observed and nothing about why. `Unqueued` reads as a
/// message that *was* queued and then removed, which is a thing this crate does —
/// `stop` and `abort` drop queued messages unrun — and none of it reaches here.
/// `Unsent` is false on the re-entrant arm, where the caller and the actor are the
/// same task and there was never a send to make. `SendFailed` names the mechanism
/// rather than the outcome, so it would have to be re-read for every door: `notify`
/// fails a send, `stop` refuses before one. The name that survives all three is the
/// one that describes the message's position.
///
/// # Three variants because `send` decides three times, not because three is tidy
///
/// `send` is the one place a message enters the mailbox, and it reaches this type
/// from exactly three decisions: the identity check, which fires before a permit
/// is touched; the channel, which is either closed or is not; and — under
/// [`Backpressure::Shed`] alone — whether there is a free slot at all. A fourth
/// variant therefore needs a fourth *decision* rather than a fourth line —
/// counting `Err` sites would answer a question about formatting.
///
/// **The third decision arrived, and it arrived as a policy rather than as a
/// line.** This heading read *two variants because `send` decides twice* until
/// [`Backpressure`] existed, and the paragraph below predicted its own amendment:
/// a third variant is a change to `send` rather than a variant filed beside two.
/// It is. Under [`Backpressure::Park`] the shed branch cannot be reached, so a
/// caller that never names a policy meets the same two answers it always did.
///
/// Nothing that happens after the message is queued can reach here at all, and
/// that is the structural half: a verdict needs a reply channel and these doors
/// have none.
///
/// # Exhaustive, and deliberately not `#[non_exhaustive]`
///
/// [`Unanswered`]'s own argument — a caller matches this, and a wildcard arm is
/// the collapse the split exists to prevent. **The attribute stays refused across
/// the amendment above**, and paying for it is what the amendment cost: adding
/// [`Full`](Self::Full) broke every exhaustive match on this type in every crate
/// downstream, and that break is the release's headline rather than a footnote.
/// Sealing the enum to avoid the next one would hand every caller the wildcard arm
/// the split exists to prevent, which is a worse trade than a major version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NotQueued {
    /// The loop's end of the mailbox is dropped, so there is nobody left to take
    /// the message.
    ///
    /// `Closed`, and not `Gone` or `Stopped` or `NotRunning`, because this names
    /// the one thing the send observed: the receiver is dropped or it is not, and
    /// it is dropped only when the loop is finished with it. The other three name a
    /// state of the **actor**, and this door never reads one — no `Fate` is
    /// consulted on the way here — so each of them would be an inference wearing
    /// the clothes of an observation. `Stopped` would be the narrowest and the
    /// most wrong: the receiver is also dropped when a supervisor gives up and when
    /// an unsupervised loop simply ends.
    ///
    /// [`Withheld::Gone`] is that same fact stated about the actor, and the two
    /// carry **one sentence** between them: *the actor is no longer running*. Two
    /// variants on two enums for one sentence is what the split chose — a caller
    /// matches the answer of the door it used, and the doors ask different
    /// questions. `one_vocabulary_and_every_loss_in_it_has_a_producer` holds the
    /// two texts together, through the `From<NotQueued> for Unanswered` the crate
    /// actually performs.
    ///
    /// The two used to disagree, and this paragraph used to explain the
    /// disagreement: the value-door's gone arm also absorbed a reply dropped by a
    /// live actor, so it hedged where this cannot. That producer is
    /// [`Withheld::Dropped`] now, and the hedge is gone with it.
    ///
    /// A **restart does not reach here.** The receiver outlives every incarnation
    /// of a supervised actor — the same fact [`busy`](LocalActor::busy)
    /// records, from the other side — so this is the supervision itself ending,
    /// not one life of it.
    #[error("the actor is no longer running")]
    Closed,
    /// The call was made from inside the target actor's own turn, and would have
    /// deadlocked.
    ///
    /// [`Unanswered::Reentrant`] is the same fact through the other pair of
    /// doors, and carries the same message: the actor is **still running** — it is
    /// running the turn this call was made from.
    ///
    /// [`TurnLost::Reentrant`] carries it too, and
    /// [`Failure::Reentrant`](crate::Failure::Reentrant) at the door reached
    /// through the bound — so this sentence lives on four enums, and it is **one
    /// of the two** that still do, now that every other loss is one [`Withheld`]
    /// value read by three doors. [`Full`](Self::Full) is the second, and the two
    /// are the same shape for the same reason: both are
    /// facts about a message that never entered the mailbox, so neither has a turn
    /// for [`Withheld`] to be a verdict on.
    /// `one_vocabulary_and_every_loss_in_it_has_a_producer` holds all three, this
    /// one through `From<NotQueued> for Unanswered` — the widening the crate
    /// actually performs, so the guard cannot agree with a mapping no caller
    /// takes.
    ///
    /// The two doors that answer this are refused under **different halves** of
    /// the rule, and the private `Refuse` parameter is what says which.
    /// [`stop`](LocalActor::stop) waits for an ending only the blocked turn can
    /// reach, so a free mailbox slot does not save it and it is refused at any
    /// depth. [`notify`](LocalActor::notify) waits for a permit and nothing else,
    /// so a self-`tell` into a free slot runs after the turn returns and is
    /// allowed; it is refused only when the mailbox is **full**.
    #[error("this call was made from inside the target actor's own turn: it would deadlock")]
    Reentrant,
    /// The mailbox was full and this actor was spawned under
    /// [`Backpressure::Shed`], so the send was refused instead of parked.
    ///
    /// **The arguments are gone.** Nothing is handed back and nothing can be:
    /// the generated door takes typed arguments and assembles the mailbox enum
    /// inside itself, so there is no value the caller ever named for this variant
    /// to carry. tokio splits the same two cases one layer down and can afford the
    /// other half — `try_send` returns `TrySendError::Full(message)` because the
    /// caller had already given the message up, while `try_reserve` has nothing to
    /// return. keylink's doors are the second shape.
    ///
    /// A caller that cannot afford the loss takes a permit first:
    /// [`try_reserve`](LocalActor::try_reserve) decides admission *before* the
    /// payload moves, and [`Permit`]'s send cannot fail on fullness. That is why
    /// the permit doors do not shed and this one does.
    ///
    /// **Unreachable under [`Backpressure::Park`]**, which is the default on both
    /// spawn doors — so an actor whose spawn never named a policy cannot produce
    /// this. `Closed` and `Reentrant` are facts about *any* mailbox; this one is a
    /// fact about a mailbox its owner asked to refuse.
    ///
    /// [`Unanswered::Full`] and [`TurnLost::Full`] are the same fact at the doors
    /// that wanted a value, [`Failure::Full`](crate::Failure::Full) at the door
    /// reached through the bound, and a peer reads
    /// [`RemoteError::MailboxFull`]. Five names
    /// for one condition, because a caller matches the answer of the door it used
    /// — the arrangement [`Reentrant`](Self::Reentrant) already has. **Three of
    /// the five read alike**, and the two that do not are deliberate:
    /// `Failure::Full` says *call* where they say *message*, which is the door it
    /// answers, and `RemoteError::MailboxFull` says *back off rather than retry*,
    /// because a peer holds no permit to take instead.
    #[error("the actor's mailbox is full and it shed this message")]
    Full,
}

/// The one direction that is total, and the reason [`read`](LocalActor::read) can
/// keep its `?`.
///
/// A send that failed produces one of two states, and `Unanswered` has a name for
/// each: a closed mailbox is one of the ways it means [`Withheld::Gone`], and a
/// re-entrant refusal is the same refusal. Neither reaches
/// [`Withheld::Refused`], which is a verdict on a turn that ran and so cannot
/// exist for a message that never entered the mailbox.
///
/// # What the forward direction buys, since it is the one that exists
///
/// `read` asks *did I get a value*, so its caller is owed `Unanswered` for the
/// whole call — and the first thing that can go wrong inside it is a send, which
/// answers the narrower type. Without this the method would either return two
/// error types or match the send by hand, and a hand-written match is where the
/// two enums start disagreeing about one fact. Widening loses nothing because the
/// mapping above is **injective**: each `NotQueued` state has one name over here
/// and no two collapse onto the same one.
///
/// **Injectivity is a property of this conversion and it decides nothing about
/// where [`Full`](Unanswered::Full) lives.** A merge gate read this paragraph as
/// the reason the release broke three enums, so it is worth saying that it is
/// not: `Full => Turn(Withheld::Full)` is injective too, so the one-variant
/// alternative passes this test exactly as the shipped mapping does. What refuses
/// that alternative is [`Unanswered::Full`]'s own heading, on three grounds that
/// have nothing to do with collisions.
///
/// `impl From` rather than a named method so that `?` performs it, which puts the
/// widening at the call site and leaves nothing to forget. Both callers today are
/// in this file — `read`'s `?`, and [`deliver_unkeyed`]'s explicit conversion,
/// which uses it to decide *which type* renders onto the wire rather than to
/// propagate.
///
/// **The reverse impl does not exist and must not be written.** `Unanswered`
/// answers a question that includes the turn's own verdict, so mapping it back
/// would have to decide what a lost reply means to a door that never had one —
/// which is the collapse this split removed.
impl From<NotQueued> for Unanswered {
    fn from(not_queued: NotQueued) -> Self {
        match not_queued {
            NotQueued::Closed => Self::Turn(Withheld::Gone),
            NotQueued::Reentrant => Self::Reentrant,
            NotQueued::Full => Self::Full,
        }
    }
}

/// What a full mailbox does to the sender.
///
/// The depth lives **inside** the policy, so *unbounded, with a depth* is
/// unwritable: there is no depth to set beside a policy, only a policy that
/// carries one. [`LocalActor::spawn_deep`] and
/// [`LocalActor::spawn_supervised_deep`] take this in place of the bare
/// [`NonZeroUsize`] they used to take, for that reason.
///
/// # `#[non_exhaustive]`, and [`Overflow`] argues the other way from the same rule
///
/// The rule is **who matches**. A caller constructs one of these and hands it to
/// a spawn door; the crate matches it, in `LocalActor`'s private `send`. Sealing
/// restricts matching and not construction, so it costs a caller nothing here and
/// makes a third policy additive. [`Overflow`] refuses the attribute because a
/// caller matches *that* one, and a `_ =>` arm is exactly what sealing would cost
/// it.
///
/// # There is no `Unbounded`, and it is refused rather than deferred
///
/// An unbounded mailbox is `mpsc::unbounded_channel`, whose sender has **no
/// permits**. [`reserve`](LocalActor::reserve),
/// [`try_reserve`](LocalActor::try_reserve) and
/// [`reserve_owned`](LocalActor::reserve_owned) would each need an arm for a
/// reservation that reserves nothing — a representable-but-meaningless state in
/// the one type the whole permit surface rests on. The older argument still
/// stands beside it, at
/// [`RegistryBuilder::mailbox_depth`](crate::RegistryBuilder::mailbox_depth): an
/// unbounded mailbox converts backpressure into memory growth, so a latency
/// problem becomes an OOM in a process that had been reporting healthy.
///
/// [`Overflow`]: crate::compose::Overflow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Backpressure {
    /// Wait for a slot, however long that takes.
    ///
    /// What every keylink mailbox did before this type existed, and the default
    /// on both spawn doors. The sender parks; nothing is dropped and nothing
    /// errors.
    Park(NonZeroUsize),
    /// Refuse at once, and **lose the caller's arguments**.
    ///
    /// A full mailbox answers [`NotQueued::Full`] — or [`Unanswered::Full`] and
    /// [`TurnLost::Full`] at the doors that wanted a value — and the message the
    /// generated door was about to build is never built. Nothing is handed back,
    /// because the generated door takes typed arguments and assembles `M` inside
    /// itself: there is no `Msg` the caller ever named for an error to carry.
    ///
    /// A caller that cannot afford to lose them takes a permit first.
    /// [`try_reserve`](LocalActor::try_reserve) makes the admission decision
    /// *before* the payload moves, and a [`Permit`]'s send cannot fail on
    /// fullness. **The three permit doors do not shed** — that is the whole
    /// reason they exist beside this policy rather than under it.
    Shed(NonZeroUsize),
}

impl Backpressure {
    /// How many messages the mailbox holds, under either policy.
    ///
    /// The one place the depth is read, so the channel and the rustdoc cannot
    /// come to disagree about which number was chosen.
    #[must_use]
    pub const fn depth(self) -> NonZeroUsize {
        match self {
            Self::Park(depth) | Self::Shed(depth) => depth,
        }
    }

    /// Whether a full mailbox refuses instead of parking the sender.
    const fn sheds(self) -> bool {
        matches!(self, Self::Shed(_))
    }
}

/// Which half of the re-entrancy rule a send is under.
///
/// [`LocalActor::send`] **cannot work this out from its argument**, and that is
/// why it is a parameter rather than a match. Compare the two callers that
/// disagree: `answered` and `notify` build byte-identical `Message::Mail` values
/// — `notify` differs only by dropping the receiving half of the reply channel
/// first and by not awaiting — so the same message arrives from the half that
/// refuses always and the half that refuses only under load.
///
/// A private enum rather than a `bool`, so the caller cannot forget to say which
/// half it is and cannot say it the wrong way round without reading as nonsense.
#[derive(Clone, Copy)]
enum Refuse {
    /// The caller waits for an answer only the blocked turn could produce:
    /// `ask`, `ask_catching`, `read`, `stop`, and a served dispatch. A free
    /// mailbox slot does not save any of them, so the chain alone decides.
    Always,
    /// A `tell`. It waits for a permit and nothing else, so it is refused only
    /// when there is no permit to be had.
    WhenFull,
}

/// One value, owned by one task, addressed by `&mut self`.
///
/// Cheap to clone: every clone names the same actor. The actor runs until the
/// last handle is dropped or [`Self::stop`] is called.
///
/// # Two parameters, and why the second one is not a bound
///
/// `A` is the state and `M` is what its mailbox carries — the service's own
/// generated enum, one variant per declared method. The alternative was one
/// parameter and a bound tying the mailbox to the state, and it does not work:
/// the enum is generated into the caller's crate, so keylink cannot name it, and
/// an associated type on a trait keylink *could* name would put the mailbox
/// inside the state's definition, where a state used by two services could only
/// have one.
///
/// The cost is a type parameter on a public type, and it is paid entirely by
/// generated code: a caller writes `inventory::Handle<Warehouse>`, and the
/// generated `Handle` applies `M` itself. The only hand-written sites are inside
/// keylink.
///
/// The methods are split across three impl blocks by what each genuinely needs,
/// rather than one block demanding the union — so [`read`](Self::read) works on
/// a `LocalActor` whose `M` nothing has ever run.
pub struct LocalActor<A, M> {
    mail: mpsc::Sender<Message<A, M>>,
    /// Why the actor stopped, once it has. `None` while it is running — a
    /// channel initialised with a reason would report that reason for a live
    /// actor. A receiver and never a sender, so awaiting it cannot keep the actor
    /// alive, which is exactly what an earlier design built on `mail.closed()`
    /// did, making "the last handle was dropped" unreachable.
    ///
    /// It carried a `Life` with a third `Restarting` state for one wave, so that
    /// two readers could tell a restart from an ending off it. Both read the cell
    /// below now, which is settled early enough to be right; see [`Standing`].
    ended: watch::Receiver<Option<Ended>>,
    /// What a caller whose reply just died is told — a peer through
    /// [`deliver_unkeyed`], and a local one through [`answered`](Self::answered).
    /// **Both, and that is the point**: one cell answers one question, so the two
    /// doors cannot say different things about one dead reply. The keyed twin
    /// lives beside the registry entry; here the handle carries it, because a
    /// **local** actor has no registry entry to hang it on. Serving one changes
    /// nothing about that — it is the same actor with a second door. See
    /// [`Standing`] for why it is not the channel above.
    fate: Fate,
    /// Whether a turn is running right now, for [`busy`](Self::busy).
    ///
    /// The other end of this `Arc` lives in the [`Watched`] the loop is polling,
    /// which is what lets a turn on the actor's own task be read from a caller's.
    /// The keyed twin is `registry.rs`'s `Busy`, kept beside the registry entry;
    /// a local actor has no entry, so the handle carries it — which is [`fate`]'s
    /// arrangement. What differs is the reader, and it is not local against
    /// served: `fate` answers whichever door lost a reply, and both do; this one
    /// answers [`busy`](Self::busy) and nothing else.
    ///
    /// [`fate`]: Self::fate
    busy: Arc<AtomicBool>,
    /// Held so a wedged actor can still be [aborted](Self::abort). Dropping a
    /// `JoinHandle` detaches the task on both targets — it does not cancel it —
    /// so this is the only handle on a turn that never returns.
    task: Arc<JoinHandle<()>>,
    /// What [`send`](Self::send) does when there is no free slot.
    ///
    /// The depth inside it is the same number the channel above was built from,
    /// in the same expression, so the two cannot come to disagree. `send` reads
    /// only which arm it is; the depth is here because a policy without its depth
    /// is the state [`Backpressure`] exists to make unwritable.
    backpressure: Backpressure,
}

/// Starting an actor needs `A: Send`, and the reason is exact: a turn's future
/// holds `&mut A` across an await, and `&mut A` is `Send` only when `A` is. It
/// also needs `M: Turn<A>`, because this is the only block that **runs** a
/// message — the ones below merely put one in a mailbox, and reading needs
/// neither.
impl<A: Send + 'static, M: Turn<A>> LocalActor<A, M> {
    /// Start an actor on its own task.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn. From
    /// synchronous code, enter one first with
    /// [`blocking::Node::enter`](crate::blocking::Node::enter).
    #[must_use]
    pub fn spawn(state: A) -> Self {
        Self::spawn_observed(state, Vec::new())
    }

    /// Start an actor whose turns are watched.
    ///
    /// Named `spawn_observed` and **not** `spawn_with`, because
    /// [`RegistryBuilder::spawn_with`](crate::RegistryBuilder::spawn_with) already
    /// exists and means something else — a factory for keyed actors. Two
    /// meanings on one name in one crate is a reading hazard that costs nothing
    /// to avoid now and cannot be fixed after 1.0.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn.
    #[must_use]
    pub fn spawn_observed(state: A, hooks: Vec<Box<dyn Observer<A>>>) -> Self {
        Self::spawn_fed(state, hooks, ())
    }

    /// Start an actor whose turns are watched and whose loop has a second input.
    ///
    /// The general door: state, observers and feeds together, which is the triple a
    /// keyed actor's factory returns and the one re-incarnation will need. The two
    /// above are shorthands for it.
    ///
    /// `feeds` is the service's own generated `Feeds`, so forgetting to wire a
    /// declared feed is a missing argument to `Feeds::new` rather than a stream that
    /// silently never yields. `()` is the feeds of an actor that has none.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn.
    #[must_use]
    pub fn spawn_fed<F: Feeds<A>>(state: A, hooks: Vec<Box<dyn Observer<A>>>, feeds: F) -> Self {
        Self::spawn_deep(state, hooks, feeds, DEFAULT_LOCAL_BACKPRESSURE)
    }

    /// [`spawn_fed`](Self::spawn_fed)'s triple, plus the mailbox's own policy.
    ///
    /// The general door, and the only reason the three above are not it: a policy
    /// nobody chose is what `DEFAULT_LOCAL_BACKPRESSURE` is for, and requiring
    /// the argument would make every caller name a number it does not care about.
    ///
    /// [`Backpressure`] and not a bare [`NonZeroUsize`], because the depth alone
    /// does not say what happens past the last slot. It carries the depth —
    /// [`NonZeroUsize`], because `mpsc::channel(0)` panics — so *a policy without a
    /// depth* and *a depth without a policy* are both unwritable.
    /// [`RegistryBuilder::mailbox_depth`](crate::RegistryBuilder::mailbox_depth) is the
    /// keyed twin and still takes the bare number: nothing sheds on that path, and
    /// widening it there would be a knob with no producer behind it.
    ///
    /// # `Shed` reaches every door except the three that reserve
    ///
    /// Under [`Backpressure::Shed`] a full mailbox answers [`NotQueued::Full`] —
    /// or [`Unanswered::Full`] and [`TurnLost::Full`] at the doors that wanted a
    /// value — and the caller's arguments are gone. [`reserve`](Self::reserve),
    /// [`try_reserve`](Self::try_reserve) and [`reserve_owned`](Self::reserve_owned)
    /// are unchanged under either policy: they are the lossless way to ask *am I
    /// allowed to send*, and a policy that made them lossy would leave a shedding
    /// actor with no door that keeps a payload.
    ///
    /// # What a deeper mailbox does and does not buy
    ///
    /// It lets a caller get further ahead of the actor, which is the whole of it.
    /// It is **not** a bound on memory — a parked sender holds its message alive
    /// whatever this is — and it does **not** make a self-call safe.
    ///
    /// Every public door but one refuses a re-entrant call by **identity** rather
    /// than by the permit, so depth cannot reach them: [`Unanswered::Reentrant`]
    /// from an [`ask`](Self::ask) or a [`read`](Self::read),
    /// [`TurnLost::Reentrant`] from [`ask_catching`](Self::ask_catching), and
    /// [`NotQueued::Reentrant`] from a [`stop`](Self::stop).
    /// The exception is [`notify`](Self::notify), which waits for
    /// a permit and nothing else: a self-`tell` into a free slot runs after the
    /// turn returns and is legitimate, so it answers [`NotQueued::Reentrant`] only
    /// when the mailbox is **full**. Depth changes how often that one door fires,
    /// and changes nothing about whether the hazard is closed.
    /// `DEFAULT_LOCAL_MAILBOX_DEPTH` carries the measurement, and it is private
    /// because the two doors above are the whole of the public surface for it.
    ///
    /// [`Backpressure::Shed`] does not reach that rule either. The identity check
    /// runs first and answers `Reentrant` before the slot is looked at, so a
    /// shedding actor refuses a re-entrant call for being re-entrant rather than
    /// for being full.
    ///
    /// # Ring detection includes local keyed hops
    ///
    /// The chain travels with registry mailbox messages, and the keyed loop adds
    /// its own identity before dispatch. A local ring that crosses keyed actors is
    /// therefore refused by the same identity check as a ring of `LocalActor`s;
    /// `tests/actors.rs` covers both a self-call and a two-actor keyed ring. A real
    /// network hop starts on another task and cannot carry this in-process chain,
    /// so this guarantee does not extend across a remote connection.
    ///
    /// # CAUTION: a deeper mailbox raises the shutdown budget owed
    ///
    /// A shutdown must still drain what is queued, so raising this raises the
    /// time [`stop`](Self::stop) can take, by up to N turns. Nothing can check
    /// that for you, and under-draining is a defect rather than a slow shutdown.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn.
    #[must_use]
    pub fn spawn_deep<F: Feeds<A>>(
        state: A,
        hooks: Vec<Box<dyn Observer<A>>>,
        feeds: F,
        backpressure: Backpressure,
    ) -> Self {
        // The channel carries the enum directly and `Message::Mail` carries the
        // box: the same one allocation, in the place that keeps an unboxed `M` out
        // of this task's future. `tokio::sync::mpsc`
        // allocates its first block of 32 slots eagerly, so a mailbox costs
        // `32 * size_of::<slot>()` whatever its capacity is — and the slot is no
        // longer a pointer-sized boxed closure but the service's own enum, sized
        // to its largest variant. Unboxed, a service with one four-kilobyte
        // request would pay 128 KB per idle actor for a queue that holds one
        // message. `Mailbox` in `registry.rs` boxes for the same reason and
        // carries the measured table; this end had no box at all, because the
        // closure it used to carry made one by accident.
        let (mail, inbox) = mpsc::channel::<Message<A, M>>(backpressure.depth().get());
        let (ended, watching) = watch::channel(None);
        let fate = Fate::default();
        let held = fate.clone();
        let busy = Arc::new(AtomicBool::new(false));
        let watched = Arc::clone(&busy);
        let task = n0_future::task::spawn(async move {
            // Everything that happens to the state, its observers and its two
            // inputs — including the whole teardown order — is in `run_loop`,
            // which is the one loop both paths run. What is left here is the one
            // thing only this path can do: publish the reason to whoever is
            // awaiting `ended()`.
            //
            // It happens after every drop `run_loop` makes, which is stronger
            // than before: `stop().await` returning now means the state itself is
            // gone, not merely that the observers' claims are.
            let (reason, _verdict) =
                run_loop(state, hooks, feeds, Watched { rx: inbox, busy }, &fate).await;
            let _ = ended.send_replace(Some(reason));
        });
        Self {
            mail,
            ended: watching,
            fate: held,
            busy: watched,
            task: Arc::new(task),
            backpressure,
        }
    }

    /// Start an actor that is **rebuilt** when an incarnation fails.
    ///
    /// The factory produces state, observers and feeds together — the triple
    /// [`spawn_fed`](Self::spawn_fed) takes — so every incarnation gets fresh
    /// ones and the previous ones have already been dropped when it is called.
    /// That ordering is the design rather than a detail: a durable observer owns
    /// an exclusive `Claim` that *cannot* outlive its incarnation, so the claim
    /// is released before the next `open` asks for it and the process never
    /// races itself.
    ///
    /// It is `async` and fallible because claiming is I/O. A factory that fails
    /// spends one restart exactly like a fault, so a state another machine has
    /// taken ends [`Ended::GaveUp`] rather than looping forever on a re-acquire
    /// that will never succeed; [`Stop::fatal`] skips even that.
    ///
    /// ```ignore
    /// let limit = Intensity::new(NonZeroU32::new(3)?, Duration::from_secs(10))?;
    /// let ledger = ledger::supervised(
    ///     move || {
    ///         let store = store.clone();
    ///         let id = id.clone();
    ///         async move {
    ///             let (state, saver) = Persist::<Ledger, _, Whole>::open(&store, &id)
    ///                 .await?
    ///                 .start_from(Ledger::default);
    ///             Ok((state, vec![Box::new(saver) as _], ()))
    ///         }
    ///     },
    ///     limit,
    /// );
    /// ```
    ///
    /// # Four things that fall out of the shape, with no mechanism
    ///
    /// The supervisor is an outer loop on **this** actor's task rather than a
    /// second task spawning children, so: [`abort`](Self::abort) destroys the
    /// actor and not one incarnation; the mailbox outlives an incarnation with
    /// no re-queue; [`ended`](Self::ended) **cannot** resolve on a restart; and
    /// [`is_running`](Self::is_running) is `true` during one.
    ///
    /// # The gate this waited behind, so a later reader knows why
    ///
    /// Supervision was held until a durable [`Observer`] existed, and the reason
    /// is not sequencing hygiene: **a restart is worth exactly the distance
    /// between the state at fault and the state the next incarnation starts
    /// from, and that distance is set by the commit point.** With no commit
    /// point a restart is a panic guard plus data loss. [`Persist`] is that
    /// commit point and its measured price is 7.6 ms per turn.
    ///
    /// RFC-04 §8's sharpest sentence still stands and this does not answer it:
    /// *a peer that vanished is not fixed by restarting a local actor.*
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn.
    ///
    /// [`Persist`]: crate::Persist
    #[must_use]
    pub fn spawn_supervised<N, Fut, F>(factory: N, limit: Intensity) -> Self
    where
        N: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Incarnation<A, F>, Stop>> + Send,
        F: Feeds<A>,
    {
        Self::spawn_supervised_deep(factory, limit, DEFAULT_LOCAL_BACKPRESSURE)
    }

    /// [`spawn_supervised`](Self::spawn_supervised), plus the mailbox's own policy.
    ///
    /// The supervised twin of [`spawn_deep`](Self::spawn_deep), and it exists
    /// because the mailbox is created in exactly two places on this path — here
    /// and there — so a knob that reaches every local actor is two signatures and
    /// cannot be one. The mailbox belongs to the **actor** rather than to an
    /// incarnation, so this policy is set once and survives every restart: a
    /// [`Backpressure::Shed`] actor sheds while it is restarting, exactly as it
    /// does while it is running, because the mailbox never went away.
    ///
    /// # CAUTION: a deeper mailbox raises the shutdown budget owed
    ///
    /// [`spawn_deep`](Self::spawn_deep) carries the whole warning, and under
    /// supervision it is the same warning with more turns behind it.
    ///
    /// # Panics
    ///
    /// Panics when called outside a tokio runtime, like every other spawn.
    #[must_use]
    pub fn spawn_supervised_deep<N, Fut, F>(
        factory: N,
        limit: Intensity,
        backpressure: Backpressure,
    ) -> Self
    where
        N: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Incarnation<A, F>, Stop>> + Send,
        F: Feeds<A>,
    {
        let (mail, inbox) = mpsc::channel::<Message<A, M>>(backpressure.depth().get());
        let (ended, watching) = watch::channel(None);
        let fate = Fate::default();
        let held = fate.clone();
        let busy = Arc::new(AtomicBool::new(false));
        let watched = Arc::clone(&busy);
        let task = n0_future::task::spawn(async move {
            // `supervise` owns the receiver and drops it on the way out, so the
            // mailbox is closed before the ending is published.
            //
            // **That ordering is `spawn_fed`'s and this path does not inherit
            // it**, which is the sentence this comment used to get wrong: there,
            // `run_loop` drops `inbox` before `state` and a peer's `is_closed()`
            // is exact; here the receiver belongs to `supervise` for the actor's
            // whole life, so it is still open at the instant an incarnation's
            // state — and every `Deferred` parked in it — is dropped. What a
            // caller whose reply died reads is the `Fate` below, settled inside
            // the loop before that drop, and never the mailbox. Two readers, not
            // one: a served dispatch through `lost`, and a local `ask` through
            // `answered` — and this instant is exactly where the second one
            // arrives, because a parked `Deferred` dies with that state.
            // The `Watched` outlives every incarnation here, exactly as the
            // receiver inside it does, so the cell is cleared once — when this
            // task ends or is cancelled — rather than per incarnation.
            let reason = supervise(factory, limit, Watched { rx: inbox, busy }, &fate).await;
            let _ = ended.send_replace(Some(reason));
        });
        Self {
            mail,
            ended: watching,
            fate: held,
            busy: watched,
            task: Arc::new(task),
            backpressure,
        }
    }
}

/// Putting a message in a mailbox needs nothing of the state and only `Send` of
/// the message. Notably **not** [`Turn`]: whether an `M` can be run is the
/// spawning block's business, and keeping it out of here is what lets a
/// `LocalActor` be addressed by code that never named the loop.
impl<A, M: Send + 'static> LocalActor<A, M> {
    /// Run one turn and bring back its value, resuming a panic in the caller.
    ///
    /// The argument is a **builder**, not a message: keylink makes the reply
    /// channel and the [`Origin`], hands both in, and the generated arm fills in
    /// the method's own arguments around them. That is what keeps `Origin`'s two
    /// constructors `pub(crate)` while the enum that carries one is generated
    /// into somebody else's crate — generated code receives an origin and never
    /// builds one, on this path as on the keyed one.
    ///
    /// The closure runs here, on the caller's task, before anything is sent. It
    /// is not stored, so it needs no `'static` and never reaches the mailbox.
    ///
    /// # Errors
    ///
    /// [`Unanswered::Turn`] carrying a [`Withheld`] — [`Gone`](Withheld::Gone)
    /// when the actor is gone, before or during the turn;
    /// [`Dropped`](Withheld::Dropped) when a live actor threw this reply away
    /// without answering it; [`Restarting`](Withheld::Restarting) when the
    /// incarnation that owed the answer died and another is coming, carrying the
    /// ending that caused it; [`Refused`](Withheld::Refused) when an
    /// [`Observer`] refused the turn that ran, which leaves the actor alive and
    /// possibly restarting; [`ObserverPanicked`](Withheld::ObserverPanicked)
    /// when one fell over instead of deciding;
    /// [`Expired`](Withheld::Expired) when a queueing budget ran out before the
    /// turn started, which a local origin cannot do today. And
    /// [`Unanswered::Reentrant`] when the call came from inside the target
    /// actor's own turn.
    ///
    /// [`Withheld::Panicked`] is the one that does not arrive: this door resumes
    /// a fault rather than reporting it.
    ///
    /// # The turn runs even if you stop waiting
    ///
    /// Dropping this future does not cancel the turn: the message is already in
    /// another task's mailbox. A caller that gives up on a `reserve` has still
    /// reserved. Use [`ask_catching`](Self::ask_catching) on a served path, and
    /// on any path put the bound inside the method rather than around the call.
    ///
    /// # Nothing bounds the answer on this path, and `abort` is the only recovery
    ///
    /// A turn that never returns leaves this call waiting for ever. The two
    /// **cycle** shapes are documented where they are built — a self-call, on the
    /// generated `Handle`, and a ring of two or more actors, in the section below
    /// that one — and neither is detectable by a type, which is why both are
    /// **refused at runtime** with [`Unanswered::Reentrant`] rather than left to
    /// hang. A turn can also simply await something that never fires, which is
    /// neither shape and which nothing here reaches.
    ///
    /// With a [`QueueTimeout`](crate::QueueTimeout) installed, both dispatch paths
    /// bound the wait for a mailbox *permit*, and neither
    /// bounds the wait for the *answer*. The crate's answer-bound is
    /// [`Timeout`](crate::Timeout) — and it is a
    /// [`Middleware`](crate::Middleware), so it reaches a served dispatch and not
    /// a local call. A local `ask` carries
    /// [`Caller::Local`](crate::Caller::Local), which has no peer and therefore
    /// no queueing budget to spend in the first place. So a local caller behind a
    /// wedged turn gets no timeout and no refusal.
    ///
    /// Neither liveness seam reveals it either, which is the part worth knowing.
    /// [`is_running`](Self::is_running) reads the mailbox, and a wedged actor's
    /// mailbox is open, so it answers `true`. [`ended`](Self::ended) never
    /// resolves, because only the loop fills it and the loop is inside the turn.
    /// Asking `ended` *why* is the usual companion to a lost call, and it is the
    /// one thing that does not work for the one loss with no reason. Supervision
    /// does not reach it: a supervised actor decides to restart when its **loop**
    /// ends, and this loop does not end.
    ///
    /// [`abort`](Self::abort) is the recovery and it is the only one. It destroys
    /// the state — that is the price, and it is why it is a separate method — and
    /// it is also what makes `ended` resolve, as
    /// [`Ended::Vanished`], since an aborted task filled nothing.
    /// In a two-actor ring it frees the other actor and destroys only the one it
    /// names. Guarded at N = 2 by `tests/cycles.rs`.
    ///
    /// # Panics
    ///
    /// With whatever the handler panicked with, after the actor has survived it.
    /// Use [`ask_catching`](Self::ask_catching) where an unwind must not escape
    /// — a connection's task, for one.
    pub async fn ask<T>(&self, mail: impl FnOnce(Origin, Reply<T>) -> M) -> Result<T, Unanswered>
    where
        T: Send + 'static,
    {
        // Three arms and no fold, where there were seven. Six of them mapped one
        // flat enum's variant onto the other's, and one of those six dropped the
        // `Ended` on the way — `Unanswered` was `Copy` and an `Arc` is not, so
        // the fact crossed and the reason did not. The reason crosses now,
        // because `Withheld` is the same value on both sides and carries an
        // `EndReason`, which is data.
        //
        // A **fault** is the exception, because it is not a loss: the value a
        // panicking function produces is the panic, so the caller gets it.
        match self.ask_catching(mail).await {
            Ok(value) => Ok(value),
            Err(TurnLost::Faulted(panic)) => panic.resume(),
            Err(TurnLost::Turn(withheld)) => Err(Unanswered::Turn(withheld)),
            Err(TurnLost::Reentrant) => Err(Unanswered::Reentrant),
            Err(TurnLost::Full) => Err(Unanswered::Full),
        }
    }

    /// Run one turn and be *told* about a panic instead of having it resumed.
    ///
    /// The ways a turn produces no value are the [`TurnLost`] variants, in one
    /// layer. It used to be `Result<Result<T, Panic>, TurnLost>` — two error
    /// positions for one question, which is the shape a caller unwraps wrongly.
    ///
    /// # Errors
    ///
    /// Every [`TurnLost`] variant. [`TurnLost::Turn`] carries exactly the
    /// [`Withheld`] [`ask`](Self::ask) reports, because both doors read the same
    /// value; [`TurnLost::Faulted`] is the difference between the two, and it
    /// carries the [`Panic`] itself where `ask` resumes it; and
    /// [`TurnLost::Reentrant`] when the call came from inside the target actor's
    /// own turn.
    ///
    /// **No local door builds [`Withheld::Panicked`]**, and that is the same
    /// fact from the other side: a fault reaching a local caller is a payload,
    /// so it arrives as [`Faulted`](TurnLost::Faulted). `Panicked` is what a
    /// peer reads, because a peer cannot be handed an unwind.
    pub async fn ask_catching<T>(
        &self,
        mail: impl FnOnce(Origin, Reply<T>) -> M,
    ) -> Result<T, TurnLost>
    where
        T: Send + 'static,
    {
        match self.answered(mail).await? {
            Answered::Ran(value) => Ok(value),
            Answered::Faulted(panic) => Err(TurnLost::Faulted(panic)),
            Answered::Refused { name } => Err(TurnLost::Turn(Withheld::Refused {
                name: name.to_owned(),
            })),
            Answered::ObserverPanicked { name } => {
                Err(TurnLost::Turn(Withheld::ObserverPanicked {
                    name: name.to_owned(),
                }))
            }
            // Unreachable from here today — a local `Origin` carries no
            // `Queueing`, so there is no budget to expire — and answered with the
            // variant that means what happened anyway.
            //
            // This arm read the gone answer behind a comment claiming the
            // impossibility was "typed rather than asserted". It was asserted, by
            // that comment, and the mapping underneath it was wrong: `Gone` means
            // the actor is no longer running, and an expired budget leaves the
            // actor untouched. The first local origin to carry a budget — a
            // deferred reply's, a feed's — would have reported a live actor as
            // gone through a public API, silently.
            //
            // Being unreachable, this arm has **no test and was not watched to
            // fail**, and that is said here rather than left to be assumed:
            // `ask_catching` builds its own `Origin::local()`, so nothing a test
            // can hand it carries a `Queueing`. What is watched is the served
            // twin, `rendered`'s `Expired` arm, which `tests/actors.rs` pins.
            Answered::Expired => Err(TurnLost::Turn(Withheld::Expired)),
        }
    }

    /// Send one message and bring back the loop's own answer.
    ///
    /// The one place a lost reply channel is read, and the reason both public
    /// asks can be exhaustive matches over [`Answered`] instead of a blanket
    /// `Err(_) => Gone`.
    ///
    /// # A dead channel carries no reason, so the reason is read and not inferred
    ///
    /// `RecvError` is a unit type: the sender was dropped without sending, and
    /// that is the whole of what the channel says. The three ways it happens — a
    /// live actor threw the reply away, an incarnation died holding it, the actor
    /// is finished — leave identical silence, so a caller told one of them from
    /// the silence alone would be told it by guess.
    ///
    /// [`Fate`] is what the loop already writes for the served path, and it is
    /// written **before** anything the loop drops. So the death that woke this
    /// future happens-after the store, and [`withheld_for`] reads a settled
    /// value. That function is exhaustive over [`Standing`] on purpose: a fourth
    /// standing is a compile error there rather than an arm quietly folded into
    /// `Gone`, which is exactly how a dropped reply came to be reported as a gone
    /// actor. It is also the whole of the mapping — the served door reads the
    /// same standing through the same three arms, and this door held its own copy
    /// of them until it did not.
    ///
    /// The window this cannot close is the one [`deliver_unkeyed`] states, and it
    /// is the same window for the same reason — the read is after the death, so an
    /// actor that moved on between the two reads as where it is now.
    async fn answered<T>(
        &self,
        mail: impl FnOnce(Origin, Reply<T>) -> M,
    ) -> Result<Answered<T>, TurnLost>
    where
        T: Send + 'static,
    {
        let (reply, answer) = Reply::channel();
        // `Refuse::Always`: both public asks land here, and both wait for an
        // answer only the blocked turn could produce.
        self.send(
            Message::Mail(Box::new(mail(Origin::local(), reply)), current()),
            Refuse::Always,
        )
        .await
        .map_err(|not_queued| match not_queued {
            NotQueued::Closed => TurnLost::Turn(Withheld::Gone),
            NotQueued::Reentrant => TurnLost::Reentrant,
            NotQueued::Full => TurnLost::Full,
        })?;
        // [`withheld_for`] and not a second copy of its three arms. This door and
        // the served one map the same standing onto the same loss, and they held
        // one spelling each — with the same `EndReason::from` in both — until
        // `Withheld` became the type they share. Only the served spelling was
        // guarded, so this one was the copy nothing watched.
        answer
            .await
            .map_err(|_| TurnLost::Turn(withheld_for(self.fate.standing())))
    }

    /// Queue one turn and do not wait for it.
    ///
    /// What a local `tell` uses. The `Ok` means the message is in the mailbox
    /// and nothing more: it has not run, it may panic, it may be dropped unrun
    /// by [`stop`](Self::stop) or [`abort`](Self::abort), and there is no
    /// channel on which to learn any of that. A panic is caught, so the actor
    /// survives, and then dropped, because there is nobody left to tell.
    ///
    /// The message carries a reply channel like every other, and the receiving
    /// half is dropped **here**, before the send. That is the whole of "nothing
    /// is sent back": the loop answers exactly as it does for an `ask` and the
    /// answer lands nowhere. A variant without a reply would be a second shape
    /// for a turn, and a kind added later could pick the wrong one.
    ///
    /// **So [`TurnLost::Faulted`] means nothing here, and could not.** The loop
    /// does answer a fault — `Answered::Faulted` is sent on this path exactly as
    /// on an `ask`'s — into a channel whose other half this method already threw
    /// away. A `tell` therefore cannot learn that its turn exploded, only that it
    /// was queued. The actor still survives it. Nothing about a `tell` changed in
    /// the wave that named the fault; what changed is that the silence is now a
    /// consequence of one dropped receiver rather than of a fact nobody had.
    ///
    /// **It still waits for a mailbox permit**, which is why an actor calling
    /// this on itself would block even though it never waits for a reply — and
    /// why it is the one call refused only when the mailbox is **full**. Into a
    /// free slot a self-`tell` is legitimate and runs after the turn returns; into
    /// a full one it answers [`NotQueued::Reentrant`], which replaces an
    /// intermittent hang with an intermittent error a caller can see.
    ///
    /// # Errors
    ///
    /// [`NotQueued`] and not [`Unanswered`], because this door asks only whether
    /// the message was queued. Both of its states are reachable here:
    /// [`NotQueued::Closed`] when the actor is gone, and [`NotQueued::Reentrant`]
    /// under the full-mailbox half of the rule above. Nothing that happens to the
    /// turn afterwards is reported, and the paragraph above says why — the
    /// receiving half of the channel that would carry it is dropped here.
    pub async fn notify<T>(
        &self,
        mail: impl FnOnce(Origin, Reply<T>) -> M,
    ) -> Result<crate::Queued, NotQueued>
    where
        T: Send + 'static,
    {
        let (reply, answer) = Reply::channel();
        drop(answer);
        // `Refuse::WhenFull`, and this is the one caller that is not
        // `Refuse::Always`. A self-`tell` into a free slot runs after the turn
        // returns and is a working pattern; refusing it would delete one.
        self.send(
            Message::Mail(Box::new(mail(Origin::local(), reply)), current()),
            Refuse::WhenFull,
        )
        .await
        .map(|()| crate::Queued::new())
    }
}

/// Reading, stopping and asking why need nothing of either parameter — which is
/// the honest shape and not a convenience: a read cannot run a turn, so it
/// cannot need to know what a turn is.
impl<A, M> LocalActor<A, M> {
    /// Read the state between turns, running **no** observers.
    ///
    /// [`Handle::with_state`] is this. A read takes `&A` and cannot mutate, so
    /// storing after it is waste — and it also closes a loop a future background
    /// writer would otherwise create, where a writer that read the state through
    /// a turn would mark the actor dirty and schedule another read, forever.
    ///
    /// # The one message that is still a boxed closure
    ///
    /// Every other message is a variant of the service's generated enum. This
    /// one cannot be: its work is an arbitrary caller closure rather than a
    /// declared method, so there is nothing for a macro to generate a variant
    /// from. What it costs is a `Box<dyn FnOnce(&A)>` — **synchronous**, so no
    /// boxed future, no `&mut A` and no observer chain, which is a fraction of
    /// what the turn it used to share a type with cost.
    ///
    /// # And the one channel that is not the loop's own answer type
    ///
    /// It carries `Result<T, Panic>`, and stays that way deliberately. A read
    /// runs no observers, so it can never be refused; it carries no
    /// [`Queueing`], so its budget can never expire. Giving it the four-state
    /// answer every turn uses would add two arms that cannot occur to the one
    /// message whose entire argument is that it is not a turn. So
    /// [`TurnLost::Faulted`] does not reach here — the
    /// panic is caught synchronously above and resumed in the caller, which is
    /// the same policy under a smaller type.
    ///
    /// # Errors
    ///
    /// Two answers, and the door is what narrows it.
    /// [`Unanswered::Reentrant`] when the call came from inside the target actor's
    /// own turn — a read takes a permit and then waits for the loop, so it is
    /// refused under the same half of the rule an [`ask`](Self::ask) is — and
    /// `Unanswered::Turn(`[`Withheld::Gone`]`)` when the actor is gone.
    /// [`Withheld::Refused`] and [`Withheld::Expired`] cannot occur for the
    /// reason above: no observers, no budget. [`Withheld::Dropped`] and
    /// [`Withheld::Restarting`] cannot occur for a different reason, and it is
    /// the next section.
    ///
    /// # A dead read channel is [`Gone`](Withheld::Gone), and that is total rather than inferred
    ///
    /// The private `answered` reads the actor's standing when a reply channel dies,
    /// because a local `ask` can lose its reply to a live actor or to a restart.
    /// This method maps a dead channel straight to `Gone` and reads nothing — the
    /// exact construct that made a dropped reply report a gone actor at the other
    /// door. It is correct here, and the three facts that make it correct are in
    /// three other places, so they are stated in this one:
    ///
    /// - **A queued read survives a restart.** `supervise` owns the receiving half
    ///   for the actor's whole life and only *lends* it to each incarnation, so an
    ///   incarnation boundary drops no queued message. `Restarting` has nothing to
    ///   describe.
    /// - **Nothing suspends between dequeue and send.** The loop's arm for this
    ///   message is a synchronous call — no await between the message leaving the
    ///   channel and the closure running — so the sender cannot be dropped part-way.
    /// - **A panic in the closure is caught and *sent***, as `Err(panic)` below,
    ///   rather than dropped. So `Dropped` has nothing to describe either.
    ///
    /// **What would break it**, since each fact belongs to code that does not know
    /// this line exists: an await inside that loop arm, a re-queue across a restart,
    /// or a panic path that drops the sender instead of sending it. The first two
    /// would make this line answer *the actor is no longer running* about an actor
    /// that is restarting. Giving this method the same standing match `answered`
    /// makes would close that, and it is **refused today as a behaviour change** —
    /// a read whose channel died mid-restart would begin answering
    /// [`Withheld::Restarting`] — rather than because the read is cheaper.
    ///
    /// [`Handle::with_state`]: crate::LocalActor
    pub async fn read<T, F>(&self, read: F) -> Result<T, Unanswered>
    where
        T: Send + 'static,
        F: FnOnce(&A) -> T + Send + 'static,
    {
        let (reply, answer) = oneshot::channel();
        self.send(
            Message::Read(Box::new(Box::new(move |state| {
                // Caught synchronously rather than through `guarded`, and that is
                // not a shortcut: `guarded` takes a future, an async block holding
                // `&A` is `Send` only when `A: Sync`, and requiring `A: Sync` here
                // would put it on every actor in the crate for a closure that cannot
                // await in the first place.
                let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read(state)))
                    .map_err(Panic::caught);
                let _ = reply.send(caught);
            }))),
            // A read is a message too: it takes the free permit and then waits
            // for the loop to run the closure, so the one method that never
            // touches `&mut self` deadlocks exactly like an `ask`.
            Refuse::Always,
        )
        .await?;
        match answer.await.map_err(|_| Unanswered::Turn(Withheld::Gone))? {
            Ok(value) => Ok(value),
            Err(panic) => panic.resume(),
        }
    }

    /// Resolve when this actor ends, and say why.
    ///
    /// Holds a watch receiver and no mailbox sender, so awaiting it does not
    /// keep the actor alive — dropping every handle both ends the actor and
    /// resolves this.
    ///
    /// Public because generated code reaches `LocalActor` only through public
    /// methods, so `Handle::ended` cannot delegate to a private one.
    ///
    /// A turn that never reaches an await point is not interrupted by
    /// [`abort`](Self::abort), so this waits. That is today's behaviour and not
    /// a regression.
    /// A restart is **not** an ending, and that is structural rather than a
    /// filter written here: a supervised actor's incarnations run inside one
    /// task, and only that task's exit publishes an ending at all.
    pub fn ended(&self) -> impl Future<Output = Ended> + Send + 'static {
        let mut watching = self.ended.clone();
        async move {
            // `wait_for` and not `changed()`: a fresh receiver marks the current
            // value as seen, so `changed()` would wait forever on a second call.
            // A dropped sender is an aborted task, which never filled the value.
            match watching.wait_for(Option::is_some).await {
                Ok(seen) => match &*seen {
                    Some(reason) => reason.clone(),
                    // Unreachable: `wait_for` returned, so the predicate held.
                    // Written to be correct rather than to panic.
                    None => Ended::Vanished,
                },
                Err(_) => Ended::Vanished,
            }
        }
    }

    /// Stop after the turns already queued, then wait for the actor to end.
    ///
    /// Ordered: everything sent before this is handled and answered. Anything
    /// queued behind it is dropped unrun when the loop drops the receiver — an
    /// [`ask`](Self::ask) waiting on one is told [`Unanswered`], and a
    /// [`notify`](Self::notify) is told nothing, because it was already told
    /// `Ok`.
    ///
    /// **An actor must not call this on itself**, and it no longer can: it takes
    /// a mailbox permit like any message and then waits for a loop that cannot
    /// exit while it is inside this turn, so it is refused with
    /// [`NotQueued::Reentrant`] before the permit is touched.
    ///
    /// Waits for the receiver to be dropped, which is the loop ending — not for
    /// the state's own `Drop` to have run. A state that flushes in `Drop` needs
    /// its own signal.
    ///
    /// Idempotent, and takes no deadline: `n0_future::time::timeout` composes,
    /// and a parameter here would duplicate it. A turn that never returns is
    /// what [`Self::abort`] is for — with the caveat on that method.
    ///
    /// # Why "timeout composes" is true here, and was not on the keyed path
    ///
    /// [`Registry::stop`](crate::Registry::stop) takes a budget because the
    /// identical sentence was **false** where the registry wrote it: its `stop`
    /// removed the entry from the map and then awaited a bare `JoinHandle<()>`,
    /// so a timeout that fired dropped the entry, dropped the handle, and
    /// *detached* the task — leaving an actor nobody could reach.
    ///
    /// It composes here because this handle survives its own await. `stop` waits
    /// on the [`ended`](Self::ended) channel and owns none of the actor's
    /// lifetime; the `JoinHandle` lives behind an `Arc` in `self`, so a timeout
    /// costs the *wait* and nothing else. The caller still holds a handle, and
    /// [`Self::abort`] is still reachable through it — which is exactly what a
    /// caller who ran out of patience wants next, and exactly what a timed-out
    /// registry `stop` had taken away.
    ///
    /// # Errors
    ///
    /// [`NotQueued::Reentrant`], and it is the **only** error this can return —
    /// the case the paragraph above says must not happen, reported instead of
    /// hanging. It returns **before** waiting, so a refused `stop` costs nothing
    /// and the actor is untouched.
    ///
    /// [`NotQueued::Closed`] is the other state a send can end in and it never
    /// leaves this method: it returns `Ok` for an actor that is already gone.
    /// Stopping a stopped actor is what the caller asked for, and this method is
    /// documented as idempotent; the send's own `Closed` is swallowed here for
    /// that reason and not for want of a channel.
    ///
    /// So the error is [`NotQueued`] rather than [`Unanswered`] twice over: this
    /// door asks only whether the request was queued, and it discards half of even
    /// that.
    ///
    /// **Both halves are held by a test rather than by the paragraph above.**
    /// `cycles::a_self_stop_is_refused_rather_than_wedging_the_actor` produces the
    /// refusal, and `a_second_stop_returns_ok_rather_than_the_send_s_own_closed`
    /// in this file's `mod tests` holds the swallow — which was the argued half
    /// and the unasserted one, because nothing in the workspace read this method's
    /// `Ok`.
    pub async fn stop(&self) -> Result<(), NotQueued> {
        match self.send(Message::Stop, Refuse::Always).await {
            Ok(()) | Err(NotQueued::Closed) => {}
            // `Closed` is swallowed because the actor is already what the caller
            // asked for. These two are not: after either, the actor is **still
            // running and was never asked to stop**, so returning `Ok` would be
            // the one lie this door cannot afford — a caller that then waits for
            // the ending waits forever.
            Err(refused @ (NotQueued::Reentrant | NotQueued::Full)) => return Err(refused),
        }
        // The END channel, not `mail.closed()`. The loop fills it after it has
        // dropped the observers and the mailbox, so returning from here means
        // whatever an observer held — a file claim, for the first adapter — is
        // already released. `mail.closed()` promised neither.
        //
        // `wait_for` and not `changed()`, because a fresh receiver marks the
        // current value as seen and `changed()` would then wait forever on a
        // second call — and this is documented as idempotent. A dropped sender
        // is an aborted task, which is ended too, so the error is ignored.
        let mut watching = self.ended.clone();
        let _ = watching.wait_for(Option::is_some).await;
        Ok(())
    }

    /// Cancel the actor where it stands.
    ///
    /// The state is **destroyed**: the task owns it, and the turn in progress is
    /// dropped at its next await point. That is the difference from
    /// [`Self::stop`], and it is why they are separate methods rather than one
    /// with a flag.
    ///
    /// Cooperative and asynchronous, like every tokio abort. A turn that never
    /// reaches an await point is not interrupted and this returns having done
    /// nothing; and until the runtime drops the task,
    /// [`is_running`](Self::is_running) still says `true`. Callers waiting on a
    /// dropped turn do get [`Unanswered`]: dropping the receiver drains the
    /// buffer, and each queued turn owns its reply channel.
    ///
    /// # What a peer whose reply this destroys is told, and where that comes from
    ///
    /// An aborted task never reaches the loop's own ending hook, so nothing
    /// inside the loop *decides* anything here — but something inside it still
    /// answers: the loop's own floor is a body local, so the cancelled future
    /// drops it before the `state` parameter, and every reply this kills dies
    /// with `Standing::Gone` already settled. A peer reads *gone* rather than
    /// *the actor is serving and dropped your answer*.
    ///
    /// **This method used to settle that itself, and the line is deleted.** It
    /// was written before the floor existed, and once the floor was added no
    /// break of the settle here could be made red — `an_aborted_actor_tells_a_peer_the_actor_is_gone`
    /// stays green with the whole line removed, because the floor is strictly
    /// earlier in the same drop. A line that cannot be made red is not a guard,
    /// it is a second writer nobody can hold to account, so it went.
    /// [`Registry::abort`](crate::Registry::abort) and `drain` keep theirs, and their
    /// guards say exactly what those still cover: a **hand-written** `mount`
    /// loop, which has no `run_loop` and therefore no floor.
    ///
    /// The shape both of those replaced leaned on `!mail.is_closed()`, which is
    /// right for an unsupervised actor and false for a supervised one, whose
    /// receiver belongs to the supervisor's frame and outlives every incarnation.
    pub fn abort(&self) {
        self.task.abort();
    }

    /// Whether the actor is still handling messages.
    ///
    /// Advisory: it can stop between this answer and the next call, which is why
    /// every call reports the loss on its own — [`Unanswered`] or
    /// [`NotQueued`], by door — rather than asking a caller to check first.
    #[must_use]
    pub fn is_running(&self) -> bool {
        !self.mail.is_closed()
    }

    /// Whether a turn is running **right now**.
    ///
    /// [`is_running`](Self::is_running) is `!mail.is_closed()`, so an actor
    /// wedged inside a turn and an actor sitting idle both answer `true` to it.
    /// This is the question that separates them, and it is the one you cannot ask
    /// the actor itself: a message asking would queue behind the turn that
    /// wedged it.
    ///
    /// A [`read`](Self::read) is **not** a turn and never sets this — it runs no
    /// observers and cannot mutate, which is the same answer the keyed side
    /// gives, enforced by the one loop rather than by two decisions.
    ///
    /// Advisory, like everything else about another task: a turn can start or end
    /// between this answer and the next line. `false` from a live actor means "no
    /// turn was running at the instant this loaded".
    ///
    /// # After an abort, and the caveat is the one `abort` already carries
    ///
    /// The cell is cleared when the **loop's** end of the mailbox is dropped — the
    /// receiving end, not the sender this handle holds, and the two part company
    /// on a supervised actor whose receiver outlives every incarnation. So an
    /// escaping unwind and a cancelled task both end with this `false` rather than
    /// stuck at `true`, which is what the loop alone would leave behind: a handle
    /// reporting "busy" for ever about an actor that is gone.
    ///
    /// **But [`abort`](Self::abort) is asynchronous**, exactly as its own
    /// documentation and [`is_running`](Self::is_running)'s say: it cancels the
    /// task at its next await, so this can still answer `true` between the call
    /// and the runtime dropping the task. Measured, immediately after `abort()`:
    /// `busy` and `is_running` both `true`. Await [`ended`](Self::ended) for
    /// the point where both have settled.
    #[must_use]
    pub fn busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed)
    }

    /// How many messages are waiting ahead of the one being handled.
    ///
    /// It ranges over the depth this actor's mailbox was built with: zero or one
    /// under [`spawn`](Self::spawn), and up to the depth under
    /// [`spawn_deep`](Self::spawn_deep), which takes a `NonZeroUsize` and builds
    /// the channel from it. **This paragraph used to say the depth was "a
    /// constant and not a knob", and it was false from the commit that added
    /// that door** — it is corrected here rather than left as the reason a
    /// caller does not read the number.
    ///
    /// What it is good for is the question it answers exactly — **is this
    /// actor's mailbox full, and therefore will the next caller block on the
    /// permit.** What it is still not is a count of the service's own messages,
    /// and the paragraph below says why: this channel carries two other things.
    ///
    /// `ActorStatus::queued` answers that same question on the keyed side, and
    /// the two are **not** counting the same set: a keyed mailbox carries only
    /// the service's own messages, while this channel also carries a
    /// [`read`](Self::read) and the stop request. One queued read reads `1` here.
    /// The "will the next caller block" answer is exact either way; the
    /// equivalence is not.
    ///
    /// It reads the sender, and an [`abort`](Self::abort) still moves it: the
    /// receiver drops, the queued message drops with it, and the permit comes
    /// back. Measured — `1` with a read queued behind a wedged turn, `0` once
    /// [`ended`](Self::ended) has resolved.
    ///
    /// Paired with [`busy`](Self::busy) it separates three states rather
    /// than two: idle, in a turn with the slot free, and in a turn with the next
    /// caller already about to block.
    #[must_use]
    pub fn queued(&self) -> u64 {
        // `u64` and `try_from` for the keyed twin's reason: the widening cannot
        // fail on any target keylink builds for, and a diagnostic must not be the
        // thing that panics if one ever appears.
        u64::try_from(
            self.mail
                .max_capacity()
                .saturating_sub(self.mail.capacity()),
        )
        .unwrap_or(u64::MAX)
    }

    /// [`busy`](Self::busy) and [`queued`](Self::queued) as the one
    /// value both sides of the wire read.
    ///
    /// It calls those two and folds the answers, so there is no third reading of
    /// either fact to drift from them. [`Serving`] carries what the pair does and
    /// does not claim — including that these are **two** loads and not one
    /// instant, so an actor can leave its turn between them.
    ///
    /// Not a liveness answer: a stopped actor still folds a value here, and
    /// [`is_running`](Self::is_running) is the separate question. The keyed twin
    /// is [`Registry::serving`](crate::Registry::serving), whose `Option` *is* that
    /// answer because a registry either has an entry or does not.
    #[must_use]
    pub fn serving(&self) -> Serving {
        Serving {
            busy: self.busy(),
            queued: self.queued(),
        }
    }

    /// Wait for a mailbox slot and hold it. Backpressure, made addressable.
    ///
    /// Every other door waits for a permit *inside* itself and spends it in the
    /// same call, so a caller can observe the wait and never the slot. This one
    /// stops at the slot and hands it over. [`compose::Pool`](crate::compose::Pool)
    /// names the absence as its own missing piece — *"a `try_pick` across N
    /// mailboxes needs a `try_reserve` the handle does not have"* — and
    /// [`try_reserve`](Self::try_reserve) is that door.
    ///
    /// The permit is the admission decision, taken **before** the payload moves.
    /// That is the whole of what it buys: a caller who cannot afford to build a
    /// message and be told no takes one of these first, and a [`Permit`]'s own
    /// send cannot be refused for fullness.
    ///
    /// # What tokio guarantees here, and what it does not
    ///
    /// **Not cancel-safe.** tokio says so of the channel this wraps: *"Cancelling
    /// a call to `reserve` makes you lose your place in the queue."* Dropping this
    /// future therefore costs the wait, and a caller who retries starts again at
    /// the back.
    ///
    /// **Capacity is conserved anyway**, which is the narrower promise and the one
    /// a racing caller needs: a dropped waiter is removed from the wait list and
    /// any permit already assigned to it is returned. So racing N of these and
    /// dropping N−1 leaks no slot and wedges no mailbox.
    ///
    /// **It is therefore not FIFO across callers.** Each cancellation takes the
    /// wait-list lock, and a freed slot can be assigned to a waiter that has
    /// already left. Two tasks racing for one mailbox are ordered by nothing here.
    /// A single task awaiting two of these one after the other is ordered, for the
    /// reason a `tell` is: the mailbox is FIFO.
    ///
    /// # Errors
    ///
    /// [`NotQueued::Closed`] when the loop's end of the mailbox is dropped, and
    /// [`NotQueued::Reentrant`] when the call is made from inside this actor's own
    /// turn.
    ///
    /// **The refusal is under the `Always` half, where [`notify`](Self::notify) is
    /// under the `WhenFull` one, and the difference is that there is no message
    /// yet.** A self-`tell` into a free slot is legitimate because the message it
    /// carries runs after this turn returns. A *permit* carries no message: the
    /// caller decides afterwards whether to spend it on a `tell` — which would be
    /// legitimate — or on an `rpc`, which waits for an answer only the blocked turn
    /// can produce and hangs for ever. A door that cannot see which one is coming
    /// has to refuse the pair, so this is refused at any depth.
    pub async fn reserve(&self) -> Result<Permit<'_, A, M>, NotQueued> {
        if self.inside_own_turn() {
            return Err(NotQueued::Reentrant);
        }
        match self.mail.reserve().await {
            Ok(slot) => Ok(Permit {
                fate: &self.fate,
                slot,
            }),
            Err(_) => Err(NotQueued::Closed),
        }
    }

    /// The same, without waiting. `Ok(None)` means this actor is full right now.
    ///
    /// Three answers where tokio's own `try_reserve` gives two: it folds *full*
    /// and *closed* into one error, and this crate splits them everywhere else —
    /// full is a schedule and closed is a fate. A caller routing across N mailboxes
    /// wants to try the next one on a full and to stop on a closed, and a single
    /// error type makes it read the discriminant to find out.
    ///
    /// # Errors
    ///
    /// [`NotQueued::Closed`] and [`NotQueued::Reentrant`], both exactly as
    /// [`reserve`](Self::reserve) answers them — including the `Always` half of the
    /// re-entrancy rule, which is about what the *permit* can be spent on rather
    /// than about this call waiting. This call never waits.
    pub fn try_reserve(&self) -> Result<Option<Permit<'_, A, M>>, NotQueued> {
        if self.inside_own_turn() {
            return Err(NotQueued::Reentrant);
        }
        match self.mail.try_reserve() {
            Ok(slot) => Ok(Some(Permit {
                fate: &self.fate,
                slot,
            })),
            Err(mpsc::error::TrySendError::Full(())) => Ok(None),
            Err(mpsc::error::TrySendError::Closed(())) => Err(NotQueued::Closed),
        }
    }

    /// The same as [`reserve`](Self::reserve), owned, so the permit can be stored
    /// or cross a spawn.
    ///
    /// [`Permit`] borrows the handle, so it cannot outlive the frame that reserved
    /// it: it cannot be stored in a struct and it cannot be moved into a spawned
    /// task. This is the `'static` half, and it is the **only** difference —
    /// a future holding a borrowed permit across an await is already `Send`.
    ///
    /// Takes `self` because tokio's owned permit does, and it costs one refcount:
    /// a handle is `Clone` and every clone names the same actor, so a caller who
    /// still wants the handle clones it first.
    ///
    /// # A stored permit outlives the check this door makes
    ///
    /// The refusal below is taken **here**, once, on the task that reserves. An
    /// owned permit that is then handed to the actor's own state and spent from
    /// inside one of its turns is a re-entrant `rpc` that nothing refuses, and it
    /// hangs exactly as an unrefused self-call would. That is the price of the
    /// `'static`, it is not reachable by accident, and [`abort`](Self::abort) is
    /// the recovery it shares with every other wedged turn.
    ///
    /// # Errors
    ///
    /// [`NotQueued::Closed`] and [`NotQueued::Reentrant`], as
    /// [`reserve`](Self::reserve) answers them.
    pub async fn reserve_owned(self) -> Result<OwnedPermit<A, M>, NotQueued> {
        if self.inside_own_turn() {
            return Err(NotQueued::Reentrant);
        }
        // The clone is what `reserve_owned` costs: tokio's owned permit consumes a
        // sender, and this handle carries three other fields the permit still
        // needs nothing of except the fate below.
        match self.mail.clone().reserve_owned().await {
            Ok(slot) => Ok(OwnedPermit {
                fate: self.fate,
                slot,
            }),
            Err(_) => Err(NotQueued::Closed),
        }
    }

    /// Where a message enters the mailbox **after** waiting for its own permit.
    ///
    /// It used to be the one place a message entered at all, and this heading said
    /// so. [`reserve`](Self::reserve) is the second: it takes the permit up front
    /// and hands it over, and the caller spends it through [`Permit`] without
    /// coming back here.
    ///
    /// It used to be the one place the box was put on, too. The box moved into
    /// [`Message::Mail`], where it stays until [`dispatch`] takes it off — because a
    /// box that the loop unwraps buys nothing, and that is what
    /// `tests/actor_memory.rs` measured.
    ///
    /// # The re-entrancy check is here, and on the three permit doors, and nowhere else
    ///
    /// Five non-test callers reach **this** function — `answered` (serving both
    /// asks), `notify`, `read`, `stop` and [`deliver_unkeyed`] — and there is no
    /// sixth. Which half of the rule applies is [`Refuse`]'s parameter, because the
    /// message alone cannot say.
    ///
    /// The permit doors ask the same question through the same
    /// [`inside_own_turn`](Self::inside_own_turn) and take no [`Refuse`], because at
    /// a reservation there is no message for one to be about.
    /// [`reserve`](Self::reserve) carries the argument for the half they take.
    ///
    /// **The rule is decided once here and restated at every door, and the
    /// restatements are prose.** A caller meets the rule where it calls, not in a
    /// private function, so each public door, both `Reentrant` variants and the
    /// depth knob say which half they are under — and nothing checks any of those
    /// sentences against this one. What cannot drift is the parameter: moving a
    /// door to the other half is an argument at its call site, and dropping it is a
    /// compile error. So a change here is a sweep for `Reentrant`, not an edit.
    ///
    /// # `capacity() == 0` is exact here, and racy anywhere else
    ///
    /// A permit is freed by exactly one event: this actor's loop taking the next
    /// message. Reaching this branch means the actor's turn is on the calling
    /// stack, so its loop cannot take anything until this call returns — and no
    /// other sender can *free* a permit, only spend one. So the load is not a
    /// check-then-act race against the actor: what it reads cannot change while
    /// the caller is inside the turn that would have to change it.
    ///
    /// # The shed is the third decision, and it runs last on purpose
    ///
    /// [`Backpressure::Shed`] replaces the awaited `send` with a `try_send`, which
    /// is the **only** `try_send` on a mailbox in this crate — `console.rs` has the
    /// other one, on a diagnostic sink that `representability.rs` excludes from the
    /// mailbox rule by name. It runs after the identity check, so a re-entrant call
    /// into a shedding actor is answered [`NotQueued::Reentrant`] rather than
    /// [`NotQueued::Full`]: the deadlock is the more specific fact and the caller
    /// fixes it differently.
    ///
    /// **No door escapes it, and the three that reserve never arrive here.**
    /// [`reserve`](Self::reserve), [`try_reserve`](Self::try_reserve) and
    /// [`reserve_owned`](Self::reserve_owned) take the slot themselves and hand it
    /// over, so a caller that cannot afford to lose its arguments has a door that
    /// keeps them under either policy. Under [`Backpressure::Park`] the branch
    /// below is dead and the function is what it always was.
    async fn send(&self, message: Message<A, M>, refuse: Refuse) -> Result<(), NotQueued> {
        if self.inside_own_turn() {
            match refuse {
                Refuse::Always => return Err(NotQueued::Reentrant),
                Refuse::WhenFull if self.mail.capacity() == 0 => return Err(NotQueued::Reentrant),
                Refuse::WhenFull => {}
            }
        }
        if self.backpressure.sheds() {
            return self
                .mail
                .try_send(message)
                .map_err(|refused| match refused {
                    mpsc::error::TrySendError::Full(_) => NotQueued::Full,
                    mpsc::error::TrySendError::Closed(_) => NotQueued::Closed,
                });
        }
        self.mail.send(message).await.map_err(|_| NotQueued::Closed)
    }

    /// Whether the calling task is inside this actor's own turn.
    ///
    /// One reading for two kinds of door: [`send`](Self::send), which then asks
    /// [`Refuse`] which half of the rule applies, and the three permit doors,
    /// which have no message for a half to be about. It was inline in `send`
    /// while `send` was the only place that asked.
    fn inside_own_turn(&self) -> bool {
        // The `Arc` this handle already carries, read as an identity. Built
        // before the thread-local is touched so the common off-turn path is one
        // clone and one match arm on a unit discriminant.
        let me = Actor(Arc::clone(&self.busy));
        current().holds(&me)
    }
}

/// One reserved mailbox slot, borrowed from the handle that reserved it.
///
/// A slot is held from the moment [`LocalActor::reserve`] returns until this is
/// spent or dropped, so holding one is holding capacity away from every other
/// sender. At the default depth of one that is the actor's whole mailbox.
///
/// **Spending it cannot wait and cannot be refused for fullness**, which is the
/// property the whole type exists for: the admission decision was taken at the
/// reservation, before the caller built anything. What can still happen is
/// everything that happens to a message *after* it is queued — it may be dropped
/// unrun by [`stop`](LocalActor::stop) or [`abort`](LocalActor::abort), and its
/// turn may lose the reply. [`ask`](Self::ask) reports that half; a `tell` through
/// [`notify`](Self::notify) reports nothing, for the reason
/// [`LocalActor::notify`] gives.
///
/// # Deadlock is a caller's to avoid here, and there is one shape
///
/// Holding a permit and then awaiting an answer from the **same** actor on a
/// second door blocks whenever the permit was the last free slot: the second call
/// waits for a permit only this one can return. Spend it or drop it first. The
/// re-entrancy rule does not reach this — it is about a caller inside the actor's
/// turn, and this caller is outside it, holding the slot that turn needs.
#[must_use = "a dropped permit returns the slot and sends nothing; \
              the caller is never told"]
pub struct Permit<'a, A, M> {
    /// Read when a reply channel dies, exactly as [`LocalActor::answered`] reads
    /// it. Borrowed rather than cloned, because the lifetime is already here.
    fate: &'a Fate,
    slot: mpsc::Permit<'a, Message<A, M>>,
}

impl<A, M> Permit<'_, A, M> {
    /// Spend the slot on one turn and bring back its value.
    ///
    /// [`LocalActor::ask`] with the wait already paid. The argument is the same
    /// builder that door takes, for the same reason: keylink makes the reply
    /// channel and the [`Origin`] and the generated arm fills in the method's own
    /// arguments around them.
    ///
    /// # Errors
    ///
    /// [`Unanswered::Turn`] carrying a [`Withheld`], and **not**
    /// [`Unanswered::Reentrant`]: that question was asked and answered when the
    /// permit was reserved, so no spent permit can produce it.
    ///
    /// # Panics
    ///
    /// With whatever the handler panicked with, after the actor has survived it —
    /// [`LocalActor::ask`]'s behaviour, because this is the door a generated `rpc`
    /// goes through on both types.
    pub async fn ask<T>(self, mail: impl FnOnce(Origin, Reply<T>) -> M) -> Result<T, Unanswered>
    where
        T: Send + 'static,
    {
        let Self { fate, slot } = self;
        let (reply, answer) = Reply::channel();
        slot.send(Message::Mail(
            Box::new(mail(Origin::local(), reply)),
            current(),
        ));
        asked(answer, fate).await
    }

    /// Spend the slot on one turn and do not wait for it.
    ///
    /// [`LocalActor::notify`] with the wait already paid, and it returns nothing
    /// rather than a `Result`: that door's two failures are a full mailbox and a
    /// closed one, the permit answered the first, and there is no channel on which
    /// to report the second. A mailbox that closes between the reservation and this
    /// call drops the message unrun — which is what `stop` already does to a
    /// queued one, and which the door that returned `Ok` could not have reported
    /// either.
    pub fn notify<T>(self, mail: impl FnOnce(Origin, Reply<T>) -> M) -> crate::Queued
    where
        T: Send + 'static,
    {
        let Self { fate: _, slot } = self;
        let (reply, answer) = Reply::channel();
        drop(answer);
        slot.send(Message::Mail(
            Box::new(mail(Origin::local(), reply)),
            current(),
        ));
        crate::Queued::new()
    }
}

/// [`Permit`] that owns its sender, so it can be stored or cross a spawn.
///
/// Everything [`Permit`] documents holds here, including the slot it is holding
/// away from every other sender. What it adds is that the holding can now outlive
/// the frame that reserved it, and [`LocalActor::reserve_owned`] states the one
/// hazard that comes with.
#[must_use = "a dropped permit returns the slot and sends nothing; \
              the caller is never told"]
pub struct OwnedPermit<A, M> {
    /// [`Permit`]'s, by value, because there is no handle left to borrow from.
    fate: Fate,
    slot: mpsc::OwnedPermit<Message<A, M>>,
}

impl<A, M> OwnedPermit<A, M> {
    /// [`Permit::ask`], owned.
    ///
    /// # Errors
    ///
    /// [`Permit::ask`]'s.
    ///
    /// # Panics
    ///
    /// [`Permit::ask`]'s.
    pub async fn ask<T>(self, mail: impl FnOnce(Origin, Reply<T>) -> M) -> Result<T, Unanswered>
    where
        T: Send + 'static,
    {
        let Self { fate, slot } = self;
        let (reply, answer) = Reply::channel();
        // The sender tokio hands back is the one this permit was reserved on. It
        // is dropped rather than returned, because a caller who wants a handle
        // clones the one they reserved from — which is what `reserve_owned` costs
        // and says it costs.
        drop(slot.send(Message::Mail(
            Box::new(mail(Origin::local(), reply)),
            current(),
        )));
        asked(answer, &fate).await
    }

    /// [`Permit::notify`], owned.
    pub fn notify<T>(self, mail: impl FnOnce(Origin, Reply<T>) -> M) -> crate::Queued
    where
        T: Send + 'static,
    {
        let Self { fate: _, slot } = self;
        let (reply, answer) = Reply::channel();
        drop(answer);
        drop(slot.send(Message::Mail(
            Box::new(mail(Origin::local(), reply)),
            current(),
        )));
        crate::Queued::new()
    }
}

/// What a permit's `ask` is owed once the message is in the mailbox.
///
/// **One state fewer than [`LocalActor::ask`]'s two-step fold, and the missing one
/// is [`TurnLost::Reentrant`]**: a permit asked that question when it was
/// reserved, so nothing downstream of a spent permit can produce it. That is the
/// reason this is a third reading of [`Answered`] rather than a call into the
/// other two — those start from a send that can still be refused, and this starts
/// from a message that is already in the mailbox.
///
/// Everything else is the same reading. A fault is the caller's value rather than
/// a loss, a dead reply channel is [`withheld_for`] over the standing, and every
/// other way a turn produced nothing is a [`Withheld`].
pub(crate) async fn asked<T>(
    answer: oneshot::Receiver<Answered<T>>,
    fate: &Fate,
) -> Result<T, Unanswered> {
    match answer.await {
        Ok(Answered::Ran(value)) => Ok(value),
        Ok(Answered::Faulted(panic)) => panic.resume(),
        Ok(Answered::Refused { name }) => Err(Unanswered::Turn(Withheld::Refused {
            name: name.to_owned(),
        })),
        Ok(Answered::ObserverPanicked { name }) => {
            Err(Unanswered::Turn(Withheld::ObserverPanicked {
                name: name.to_owned(),
            }))
        }
        Ok(Answered::Expired) => Err(Unanswered::Turn(Withheld::Expired)),
        Err(_) => Err(Unanswered::Turn(withheld_for(fate.standing()))),
    }
}

/// By hand, because `#[derive(Clone)]` would demand `A: Clone` — and `A` is the
/// state, which is never cloned and usually cannot be. `M` is not cloned either:
/// only the sender is.
///
/// **Every field here shares rather than copies**, and `busy` is the one where
/// getting that wrong would be invisible: cloning the `bool` out of the `Arc`
/// would give each handle a frozen snapshot, and a test that reads through the
/// handle it spawned with would still pass.
impl<A, M> Clone for LocalActor<A, M> {
    fn clone(&self) -> Self {
        Self {
            mail: self.mail.clone(),
            ended: self.ended.clone(),
            fate: self.fate.clone(),
            busy: Arc::clone(&self.busy),
            task: self.task.clone(),
            // The one field that is genuinely copied, and it is not an exception
            // to the heading: it is `Copy` because it is the policy itself and not
            // a handle to one. Two clones of a handle cannot come to disagree
            // about it, because nothing ever sets it after the spawn.
            backpressure: self.backpressure,
        }
    }
}

impl<A, M> fmt::Debug for LocalActor<A, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalActor")
            .field("running", &!self.mail.is_closed())
            // The loaded value and never the `Arc`, which would print an address.
            .field("busy", &self.busy.load(Ordering::Relaxed))
            .finish()
    }
}

// ─────────────────────────────────────────── what a peer is told about a turn

/// Render one answer as the pair a served dispatch returns.
///
/// **Two dispatch paths reach this and they used to disagree.** The keyed loop
/// rendered a panic as `Panic`'s own `Display` and the singleton's dispatch
/// wrapped it in a second prefix, so one peer read `the actor panicked: bad
/// message` and the other `panicked: the actor panicked: bad message` for the
/// same fault — and there was no keyed twin of the test that pins the prefix, so
/// both stayed green. One function now, and the two paths cannot drift.
///
/// **The discriminator is a variant now, and that is what this commit bought.**
/// Three of these arms built a [`RemoteError::Failed`] whose *string* told a
/// peer which loss it was, against five prefix constants and a `starts_with` —
/// because a variant sits inside the hashed contract and adding one moves every
/// service's ALPN. `RemoteError::Withheld` is that variant, and the ALPN it
/// costs was paid in the commit before this one. A peer matches
/// [`Withheld`] instead of parsing a sentence whose tail is
/// caller-derived panic text.
///
/// [`lost`] renders the other three losses through the same enum, so the two
/// functions no longer share a vocabulary by agreeing on string literals.
///
/// The stutter is still there and is deliberately not fixed here:
/// `Panic::caught` builds its message as `the actor panicked: {payload}`, and
/// that same string is what [`Ended::Panicked`] and
/// [`TurnOutcome::Panicked`] carry, so [`Withheld::Panicked`]'s own sentence
/// arrives in front of it. Removing one of the two is a decision about
/// `panic.rs`'s vocabulary, and it belongs where `Panic` is rather than inside a
/// dispatch.
pub(crate) fn rendered<Resp>(
    answered: Answered<(Resp, Body)>,
    millis: u64,
) -> (Outcome<Resp>, Body) {
    match answered {
        Answered::Ran((response, body)) => (Outcome::Ok(response), body),
        // A panic must not unwind out of a connection's task, so the remote
        // caller is told a value. The actor is still alive. `Panic`'s own
        // `Display` and no prefix around it: the discriminator is the variant.
        Answered::Faulted(panic) => (
            Outcome::Err(RemoteError::Withheld(Withheld::Panicked {
                message: panic.to_string(),
            })),
            Body::empty(),
        ),
        // NOT `NoSuchActor`, which says "there is nothing here, stop asking".
        // The turn ran and was not committed, and the actor is ending for a
        // reason the peer should be able to read.
        Answered::Refused { name } => (
            Outcome::Err(RemoteError::Withheld(Withheld::Refused {
                name: name.to_owned(),
            })),
            Body::empty(),
        ),
        // Separate from `Refused` because a veto is a decision an author wrote
        // down and a panic is the absence of one. A peer has no `ended()`, so
        // this variant is the whole of what it learns; it read the refusal
        // sentence for both until the two were split.
        Answered::ObserverPanicked { name } => (
            Outcome::Err(RemoteError::Withheld(Withheld::ObserverPanicked {
                name: name.to_owned(),
            })),
            Body::empty(),
        ),
        // Queued and never started. `TimedOut` and **not** `Withheld::Expired`,
        // which says the same thing without the figure: this is the one loss
        // whose useful content is a duration, and the value the other bound
        // produces for the same condition — so a peer cannot tell the two apart
        // by the refusal.
        Answered::Expired => (
            Outcome::Err(RemoteError::TimedOut { millis }),
            Body::empty(),
        ),
    }
}

/// What a peer is told when the channel that owed it an answer was dropped
/// without sending.
///
/// **One function, for [`rendered`]'s reason and with a sharper history.** The
/// two served paths answer the same three causes, and when they were written
/// separately they did not merely drift — the keyed one never split at all, and
/// rendered every cause as `NoSuchActor`. So a plain unsupervised keyed actor
/// that took `reply.later()` and dropped the `Deferred`, which is exactly what
/// that type's `#[must_use]` asks a handler to do, told the peer *no actor is
/// mounted as X* about an actor that was about to answer its next call on the
/// same connection.
///
/// The three answers are three different next moves for the caller, which is the
/// only reason to tell them apart:
///
/// - [`Standing::Restarting`] — **retry, unless you sent a key.** The incarnation
///   that owed the answer is gone and another is coming, and the ending that
///   caused it rides along as an [`EndReason`] — matchable, and rendered into the
///   sentence so a peer that only logs `err.to_string()` still reads it. It was a
///   string with a `"restarting: "` prefix in front of it, which is where the
///   *carries the ending* half of this promise came from and how it survived one
///   wave of the variant not keeping it. A call that carried an
///   [`IdempotencyKey`](crate::IdempotencyKey) is the exception, and it is the caller
///   most likely to act on the word *retry*: a message that reached a mailbox was
///   marked `enqueued`, which `Progress::reached` reports as dispatched, so its key
///   settled [`Interrupted`](crate::IdempotencyRefusal::Interrupted) and **a retry
///   under the same key is refused**. Reconcile and use a different key. `supervise`
///   carries why that is the honest answer rather than a defect.
/// - [`Standing::Running`] — **do not retry blindly, and do not give up on the
///   actor.** It is there, and this one reply was lost.
/// - [`Standing::Gone`] — **stop asking.**
///
/// # It no longer takes the actor's id, because no answer names one
///
/// The `Gone` arm was [`RemoteError::NoSuchActor`], whose whole content is an
/// id, and it was the only thing the parameter fed. All three answers are
/// [`Withheld`] now — facts about a turn rather than about an address — so the
/// parameter went with the variant. Both callers were passing a value they
/// already had, and neither loses anything: a peer knows which actor it
/// addressed.
///
/// [`RemoteError::NoSuchActor`]: crate::RemoteError::NoSuchActor
///
/// # What it is about, which is not what a peer might read into it
///
/// It answers *what is true of this actor now*, not *what killed your reply*. The
/// reader is woken by the death and reads afterwards, so the two coincide except
/// in one direction: an actor may have moved on between the death and the read.
/// A live actor that dropped a reply and then ended reads `Gone`, and *stop
/// asking* is the right instruction for it. A restart whose next incarnation is
/// already running reads `Running`, and *the actor is there* is the right one for
/// that. [`deliver_unkeyed`] and `deliver_keyed` both state the window they
/// cannot close; neither of them can produce a sentence that is false at the
/// moment it is written.
pub(crate) fn lost(standing: Standing) -> RemoteError {
    RemoteError::Withheld(withheld_for(standing))
}

/// Which loss a dropped reply channel was, read off the standing the actor's own
/// loop settled.
///
/// **The mapping itself, extracted because both doors perform it.** [`lost`]
/// wraps it for a peer and [`answered`](LocalActor::answered) wraps it for a
/// local caller, and the two wrote these three arms out separately — including
/// the `EndReason::from(&*reason)` inside the first one — until [`Withheld`]
/// became the vocabulary both of them carry. While the two doors answered
/// different **types** the copy was structural; once they answered the same one
/// it was one decision written twice, and only the served spelling had a guard
/// on it.
///
/// Exhaustive over [`Standing`] on purpose: a fourth standing is a compile error
/// here rather than an arm quietly folded into `Gone`, which is exactly how a
/// dropped reply came to be reported as a gone actor.
fn withheld_for(standing: Standing) -> Withheld {
    match standing {
        // `&*reason` and not `reason`: the `Arc` is one ending shared by every
        // reply that died with that state, and `EndReason` is the form of it that
        // has a schema. `From<&Ended>` is the only conversion, and it has no
        // wildcard arm.
        Standing::Restarting(reason) => Withheld::Restarting {
            reason: EndReason::from(&*reason),
        },
        // **The sentence says what is true of the actor and nothing about where
        // the reply died**, because this arm has three producers and no way to
        // tell them apart:
        //
        // 1. a handler took `reply.later()` and dropped the `Deferred` — the
        //    sanctioned move, and what that type's `#[must_use]` asks for;
        // 2. a handler panicked *after* `reply.later()`, so the `Deferred` died
        //    in the unwind and its panic message is lost rather than rendered;
        // 3. a handler panicked **before** consuming the channel at all, so what
        //    died is the `Reply` itself, inside the turn — `turn_deferred` has
        //    named this case since `#[defer]` landed. The reply never left the
        //    turn, and it was never a `Deferred`.
        //
        // It has been wrong twice, both times by naming a cause. "The reply was
        // taken out of the turn and never answered" describes only (1); "the
        // reply left its turn" is the same claim in different words, and is false
        // of (3) — the case
        // `defer::a_peer_whose_deferred_handler_panicked_reads_the_same_dropped_answer`
        // exercises. What all three share is the actor: it is mounted, it took
        // the message, and it will answer the next one.
        Standing::Running => Withheld::Dropped,
        // **This says something narrower than `NoSuchActor` did, and the change
        // is the point.** It was `NoSuchActor { id }` — *no actor is mounted as
        // X* — which is an answer about an **id**, and this function is only ever
        // reached after a message was delivered to an actor that took it. So the
        // one thing a peer knew for certain here was the one thing the sentence
        // denied.
        //
        // `Withheld::Gone` carries *the actor is no longer running*: this ran and
        // has stopped, there is no next incarnation, stop calling. **A peer that
        // matched `NoSuchActor` to mean "stop calling" still gets that
        // instruction**, from a different variant — and one that no longer
        // conflates *it ran and ended* with *it never existed*. The registry keeps
        // `NoSuchActor` for the second, where it is the whole truth: a lookup that
        // found nothing, before any turn.
        Standing::Gone => Withheld::Gone,
    }
}

/// Answer one peer's request out of this process's own actor, and translate what
/// came back into what goes on the wire.
///
/// The twin of [`deliver_keyed`](crate::deliver_keyed), for a service
/// served as **one** actor through `Servable::service` on its handle rather than
/// as a registry of keyed ones. The two share [`rendered`] and differ only in how they find
/// the mailbox: one routes an [`ActorId`](crate::ActorId), this one already has
/// the actor.
///
/// `#[doc(hidden)] pub` and a free function taking `&LocalActor`, for the reason
/// its twin is one — and for a second reason of its own. Generated code cannot
/// build the [`Origin`] the mailbox carries: both constructors are `pub(crate)`,
/// deliberately, so that a downstream crate cannot make a second origin around a
/// `Context` it already delivered. keylink builds the origin here and hands it
/// to the closure, which fills in the variant around it.
///
/// # The budget is honoured twice, and this is the first
///
/// Here it bounds the wait for a mailbox permit; [`turn`] re-reads the same
/// [`Queueing`] on the actor's own task the instant the message comes off the
/// mailbox. Dropping the send future is what makes this half honest: tokio does
/// not queue a message whose send was cancelled, so a message refused here
/// provably never entered the mailbox and its idempotency key is still
/// claimable. Nothing is marked on that path on purpose — marking would settle
/// the key `Interrupted` and permanently refuse every retry of a call that never
/// happened.
///
/// `enqueued` is marked *after* the send and not before, and that placement is
/// the whole of the precision: from there on the turn runs whether the caller is
/// listening or not, so a caller that gives up is giving up on work that
/// happened. RFC-04 §16.12.
///
/// # It no longer takes the service name, for [`lost`]'s reason
///
/// The name fed one answer and one only: a [`RemoteError::NoSuchActor`] built
/// when the mailbox was already closed. That answer is [`Withheld::Gone`] now —
/// see the arm below — so nothing here names an address, and the parameter went
/// with the variant. The one caller is `service!`'s generated dispatch, which was
/// passing a `stringify!` of a name it already knew.
///
/// [`RemoteError::NoSuchActor`]: crate::RemoteError::NoSuchActor
#[doc(hidden)]
pub async fn deliver_unkeyed<A, M, Resp>(
    actor: &LocalActor<A, M>,
    cx: Context,
    mail: impl FnOnce(Origin, Reply<(Resp, Body)>) -> M,
) -> (Outcome<Resp>, Body)
where
    M: Send + 'static,
    Resp: Send + 'static,
{
    // Read before `cx` moves into the `Origin`.
    let budget = cx.queueing();
    let progress = cx.progress();
    let millis = budget.map_or(0, Queueing::millis);

    let (reply, answer) = Reply::channel();
    // `Refuse::Always`, and `current()` rather than `Chain::Empty`. A peer's
    // request arrives on a connection's task, which has run no turn, so both read
    // empty today and the refusal below is unreachable. They are written as the
    // general case anyway: the day a local door delivers a served call **inline**
    // on the caller's task, this line is already right, and the alternative — a
    // third `Refuse` variant meaning "cannot be re-entrant" — would be a hole
    // that door falls into silently.
    let queueing = actor.send(
        Message::Mail(Box::new(mail(Origin::peer(cx), reply)), current()),
        Refuse::Always,
    );
    let sent = match budget {
        None => queueing.await,
        Some(budget) => {
            match n0_future::time::timeout(
                budget.deadline().saturating_duration_since(Instant::now()),
                queueing,
            )
            .await
            {
                Ok(sent) => sent,
                Err(_elapsed) => {
                    return (
                        Outcome::Err(RemoteError::TimedOut { millis }),
                        Body::empty(),
                    )
                }
            }
        }
    };

    match sent {
        Ok(()) => {}
        // **One condition, one answer — and which of two a peer read used to
        // depend on a race.** A closed mailbox and a reply that died with a
        // `Standing::Gone` are the same fact about the same actor: it ran, and it
        // has stopped. Which of the two paths saw it depended only on whether the
        // mailbox closed before or after this send won its permit, and they
        // answered differently — `NoSuchActor { id }` here,
        // [`Withheld::Gone`] through [`lost`] below.
        //
        // **A singleton has no *never existed* case for `NoSuchActor` to be the
        // whole truth about.** The ALPN either serves this actor or the handshake
        // refuses the peer; there is no lookup that can find nothing. After
        // `stop()` the ALPN stays registered, so this arm is answered for as long
        // as the server runs — and what it has to report is *this ran and has
        // stopped*, which is what `Withheld::Gone` says. `NoSuchActor` stays with
        // the registry lookup that found nothing, before any turn, where it is
        // the whole truth.
        //
        // It is also the answer the crate already gives locally for this exact
        // state: `From<NotQueued> for Unanswered` widens `Closed` onto
        // `Unanswered::Turn(Withheld::Gone)`. A local caller and a peer read one
        // sentence about one condition because they read one value.
        Err(NotQueued::Closed) => {
            return (
                Outcome::Err(RemoteError::Withheld(Withheld::Gone)),
                Body::empty(),
            )
        }
        // NOT `NoSuchActor`, which says "there is nothing here, stop asking":
        // a refused re-entrant call leaves the actor running. A bare `Failed`
        // carrying the error's own text and **not** a `Withheld`, because the
        // `Withheld` variants are the discriminator between causes a peer acts on
        // differently and this one is unreachable from a peer — see the send
        // above. A variant for THIS arm would be a wire-visible discriminator
        // invented for a case nothing can produce.
        //
        // That argument is about this arm and does not generalise: every other
        // loss `rendered` and `lost` answer with IS producible, and each has its
        // own `Withheld`. The premise here is *nothing can produce it*, and that
        // premise is false everywhere except this arm.
        //
        // **The wire sentence belongs to `Unanswered`, so the conversion is
        // explicit.** A bare `refused.to_string()` renders whichever type `send`
        // happens to return — `NotQueued` since the send-shaped doors were split
        // off — so a local refactor could re-point the bytes a peer reads without
        // touching this line. Naming the type here is what keeps the sentence with
        // its owner, and that argument holds whatever the two enums currently say.
        // It is the only wiring in the crate from either local enum's `Display` to
        // bytes a peer reads, and by the paragraph above it is also unreachable —
        // both are true, and neither weakens the other.
        //
        // **No test covers this path.** Nothing calls `deliver_unkeyed` and
        // reads its refusal bytes, so reverting this line to
        // `refused.to_string()` leaves the suite green. A guard would have to be
        // **built** rather than watched, because the paragraph above argues a peer
        // cannot produce a re-entrant call in the first place.
        Err(refused @ NotQueued::Reentrant) => {
            return (
                Outcome::Err(RemoteError::Failed(Unanswered::from(refused).to_string())),
                Body::empty(),
            )
        }
        // **This one IS producible, which is the whole difference from the arm
        // above.** A singleton served under [`Backpressure::Shed`] refuses a peer
        // the moment its mailbox is full, so this is the served half of
        // [`NotQueued::Full`] and it earns the wire-visible discriminator the
        // re-entrant arm was refused.
        //
        // [`RemoteError::MailboxFull`] and never [`RemoteError::TimedOut`]: a peer
        // that reads a timeout retries the same call, and a peer that reads this
        // backs off. The `QueueTimeout` layer above still answers `TimedOut` for
        // its own budget, and the two now say different things because they mean
        // different things — a shed is the far side refusing, a timeout is the far
        // side being slow.
        Err(NotQueued::Full) => return (Outcome::Err(RemoteError::MailboxFull), Body::empty()),
    }
    progress.enqueued();

    match answer.await {
        Ok(answered) => rendered(answered, millis),
        // The sender was dropped without sending, and there are three causes
        // rather than one. Each gets its own answer, because `NoSuchActor` says
        // *there is nothing here, stop asking* and two of the three are lies:
        //
        // 1. **Mid-restart.** A `Deferred` parked in the state dies when the
        //    state does, and the next incarnation is coming.
        // 2. **A live actor dropped one reply.** `#[defer]` makes this a
        //    sanctioned move rather than misuse: `Deferred`'s `#[must_use]`
        //    tells a handler to say `drop` out loud, and a handler that panics
        //    after taking `reply.later()` arrives here too. The actor is there,
        //    ran the turn, and answers the next call on the same connection.
        // 3. **The actor ended**, or was destroyed.
        //
        // The actor's own loop settles which of the three this is, before it
        // drops anything — so the store happens-before the drop that woke this
        // future, and this read cannot be early. `lost` is the rendering, shared
        // with the keyed path so the two cannot answer one condition differently.
        //
        // **Neither `Life` nor `is_running()` is read here any more, and both
        // were wrong on the supervised path.** `Life::Ended` is published after
        // `run_loop` returns — after the state drop this is about — and
        // `is_running()` is `!mail.is_closed()`, which `supervise` holds open for
        // the actor's whole life across every incarnation. Together they rendered
        // a supervised actor's *permanent give-up* as `dropped: `, "the actor is
        // still there", about one that would never answer again.
        //
        // The one direction still open, stated so it is not rediscovered as a
        // bug: this read can be **late**. A restart whose next incarnation has
        // already published `Running` reads as arm 2 rather than arm 1. That
        // window is a supervisor's backoff wide and the sentence arm 2 produces
        // is true when it is read — the actor *is* there — but it will not tell
        // such a caller to retry. Closing it needs a fact about which incarnation
        // owed the answer, and every cheap way to carry one is wrong in the other
        // direction; RFC-07 §12.4 works the generation counter through.
        Err(_) => (Outcome::Err(lost(actor.fate.standing())), Body::empty()),
    }
}

/// What this actor is doing, without delivering anything to it.
///
/// The non-enqueuing twin of [`deliver_unkeyed`], and the singleton half of
/// what a generated `__status` answers. Generated dispatch calls this **instead
/// of** that one, above it in the same closure, and nothing enters the mailbox —
/// which is the whole mechanism rather than an optimisation. [`busy`] states
/// the constraint: a message asking this question would queue behind the turn
/// that wedged the actor, and if it ever ran it would report `true` about
/// itself while its own permit counted in [`queued`]. It would answer the
/// state of asking rather than the state of the actor.
///
/// Governance is untouched by that. The layer chain runs above the dispatch
/// closure, so a [`Middleware`](crate::Middleware) refuses `__Status` per peer
/// and per method exactly as it refuses any other; what the interception removes
/// is the mailbox, not the [`Allowlist`](crate::Allowlist) and not the layer.
///
/// # Errors
///
/// [`Withheld::Gone`] once the mailbox is closed. That is the one condition and
/// the one answer [`deliver_unkeyed`] already gives a peer whose call finds
/// it closed, and the two say it through the same variant on purpose:
/// [`LocalActor::serving`] folds a value for a stopped actor too — both fields
/// at rest — and a peer reading that pair could not tell it from an idle actor.
///
/// **The window it does not close is [`is_running`]'s own**, and it is the same
/// one every question about another task has: the actor can stop between this
/// read and the next line. A supervised actor between incarnations is inside it
/// — its receiver belongs to the supervisor and outlives each one, so the
/// mailbox is open and this answers a `Serving` where a *delivery* would report
/// [`Withheld::Restarting`]. Evidence, never a precondition.
///
/// [`busy`]: LocalActor::busy
/// [`queued`]: LocalActor::queued
/// [`is_running`]: LocalActor::is_running
#[doc(hidden)]
pub fn serving_unkeyed<A, M>(
    actor: &LocalActor<A, M>,
) -> std::result::Result<Serving, RemoteError> {
    if actor.is_running() {
        Ok(actor.serving())
    } else {
        Err(RemoteError::Withheld(Withheld::Gone))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::collections::BTreeSet;
    use std::time::Duration;

    use super::*;
    // `Failure` is the fourth enum carrying the two hand-written sentences, and
    // it is the one this module does not otherwise name.
    use crate::Failure;

    /// None of [`lost`]'s three answers is a [`RemoteError::Failed`], so none
    /// carries a prefix and no `starts_with` will ever find one.
    ///
    /// **It used to be one of three, and the gone arm was the exception.** That
    /// arm answered [`RemoteError::NoSuchActor`](crate::RemoteError::NoSuchActor)
    /// — *no actor is mounted as X* — about an actor that had taken the message
    /// and then stopped. `lost` is reached only after a delivery, so the one fact
    /// a peer could rely on was the one the sentence denied. It is
    /// [`Withheld::Gone`] now: *this ran, it has stopped, stop calling*.
    ///
    /// The instruction a peer acts on is unchanged, and the address-shaped claim
    /// is gone. `NoSuchActor` stays where it is the whole truth — a registry
    /// lookup that found nothing, before any turn.
    ///
    /// This test is what makes the change a decision rather than a drift: the
    /// assertion names the exact value a peer receives from the arm.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/turns.rs` on
    /// [`withheld_for`]'s gone arm: search `Standing::Gone => Withheld::Gone,` and
    /// replace it with `Standing::Gone => Withheld::Dropped,`. Watched:
    /// `left: Withheld(Dropped)`, `right: Withheld(Gone)`.
    ///
    /// **Hand the harness that literal with its eight spaces of indentation.** The
    /// bare form counts 2 in `turns.rs` — the arm, and this record quoting it — and
    /// the harness aborts at "found 2". It used to carry the function's closing
    /// `})` as well, because `answered` held a second copy of the same arm one
    /// indent deeper; that copy is [`withheld_for`] now, so the anchor no longer
    /// has to span a brace to be unique.
    #[test]
    fn the_gone_answer_says_the_actor_ran_and_stopped_rather_than_naming_an_id() {
        assert_eq!(
            lost(Standing::Gone),
            RemoteError::Withheld(Withheld::Gone),
            "the gone arm is reached after a delivery, so it must not answer a \
             question about an id",
        );
    }

    /// Neither served door answers a [`RemoteError::Failed`], which is the whole
    /// of what keeps that variant meaning **a layer somebody wrote refused this**.
    ///
    /// `reply.rs` states the rule on the variant; this is what holds it. It is a
    /// rule about an **absence**, so nothing else can: a served path that starts
    /// building a `Failed` type-checks, renders, and hands a peer a sentence
    /// keylink wrote inside a variant that promises the opposite. Every loss these
    /// two functions can report has a variant of its own, and the `String` a
    /// `Failed` carries is the one thing in this enum keylink neither writes nor
    /// reads.
    ///
    /// The one in-crate `Failed` is [`deliver_unkeyed`]'s
    /// `NotQueued::Reentrant` arm, which is neither of these two functions and is
    /// unreachable from a peer besides — a request arrives on a connection's task,
    /// which has run no turn.
    ///
    /// # What it covers, and the half it cannot force
    ///
    /// [`lost`] is complete here: [`Standing`] has three states and all three are
    /// below. The [`Answered`] sample is hand-written, and `label` is exhaustive
    /// over it — so a sixth state is `error[E0004]` in this test as well as in
    /// [`rendered`], and its author meets both. It does not **force** the entry,
    /// the way [`schema_variants`] forces one for [`Withheld`]: `Answered` is
    /// private, never crosses a wire, and so has no derived list of its own
    /// variants to compare against.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/turns.rs` on [`lost`]'s
    /// body, back to the era this vocabulary replaced: search
    /// `RemoteError::Withheld(withheld_for(standing))` and replace it with
    /// `RemoteError::Failed(withheld_for(standing).to_string())`. Watched:
    /// `lost answers a Failed`.
    #[test]
    fn neither_served_door_answers_a_failed() {
        fn label(one: &Answered<((), Body)>) -> &'static str {
            match one {
                Answered::Ran(_) => "Ran",
                Answered::Faulted(_) => "Faulted",
                Answered::Expired => "Expired",
                Answered::Refused { .. } => "Refused",
                Answered::ObserverPanicked { .. } => "ObserverPanicked",
            }
        }

        for answered in [
            Answered::Ran(((), Body::empty())),
            Answered::Faulted(catching(|| panic!("produced")).expect_err("it panicked")),
            Answered::Expired,
            Answered::Refused { name: "observer" },
            Answered::ObserverPanicked { name: "observer" },
        ] {
            let arm = label(&answered);
            if let (Outcome::Err(error), _body) = rendered(answered, 0) {
                assert!(
                    !matches!(error, RemoteError::Failed(_)),
                    "`rendered`'s {arm} arm answers a `Failed`, which tells a peer that a layer \
                     in its own deployment refused the call: {error:?}",
                );
            }
        }

        for standing in [
            Standing::Running,
            Standing::Restarting(Arc::new(Ended::Stopped)),
            Standing::Gone,
        ] {
            let error = lost(standing);
            assert!(
                !matches!(error, RemoteError::Failed(_)),
                "`lost` answers a `Failed`, which tells a peer that a layer in its own \
                 deployment refused the call: {error:?}",
            );
        }
    }

    /// Every [`Withheld`] there is, sampled once.
    ///
    /// A function and not a `const`, because three of the seven carry a
    /// [`String`].
    ///
    /// # What keeps it complete is [`schema_variants`], and it was not [`produced`]
    ///
    /// This doc used to say that [`produced`] kept it complete: exhaustive over
    /// the same enum, and `error[E0004]` the moment an eighth loss exists. Both
    /// halves are true and the conclusion does not follow. Exhaustiveness forces
    /// an **arm** in `produced`; nothing forces an **entry** here. Add a variant
    /// plus its arm and this compiles unchanged — the guard then walks 7 of 8,
    /// and every assertion in it passes over the seven it was handed.
    ///
    /// What closes that is the one list of variants nobody writes by hand: the
    /// one `#[derive(Schema)]` puts in [`Withheld`]'s wire schema.
    /// [`schema_variants`] reads it, [`variant_name`] names each sample against
    /// it, and the guard compares the two sets — so a loss that reached the type
    /// and not this array is a failing assertion that says which one.
    fn every_withheld() -> [Withheld; 7] {
        [
            Withheld::Panicked {
                message: "boom".to_owned(),
            },
            Withheld::Refused {
                name: "observer".to_owned(),
            },
            Withheld::ObserverPanicked {
                name: "observer".to_owned(),
            },
            Withheld::Dropped,
            Withheld::Restarting {
                reason: EndReason::Stopped,
            },
            Withheld::Gone,
            Withheld::Expired,
        ]
    }

    /// The name [`Withheld`]'s derived schema gives this variant.
    ///
    /// Exhaustive, so an eighth loss is `error[E0004]` here — and on its own that
    /// would be [`produced`]'s hole again, an arm forced where no entry is. It is
    /// half of a pair: the other half is [`schema_variants`], which nobody writes,
    /// and the guard compares what this returns over [`every_withheld`] against
    /// what that reads off the derive.
    ///
    /// The strings are hand-written and **self-checking** — a typo puts a name in
    /// one set and not the other, in both directions.
    fn variant_name(one: &Withheld) -> &'static str {
        match one {
            Withheld::Panicked { .. } => "Panicked",
            Withheld::Refused { .. } => "Refused",
            Withheld::ObserverPanicked { .. } => "ObserverPanicked",
            Withheld::Dropped => "Dropped",
            Withheld::Restarting { .. } => "Restarting",
            Withheld::Gone => "Gone",
            Withheld::Expired => "Expired",
        }
    }

    /// Every variant [`Withheld`] has, off the list its own `#[derive(Schema)]`
    /// wrote.
    ///
    /// **The one enumeration of this type that no hand maintains.** Rust has no
    /// reflection over variants, which is why every other check in this module
    /// forces an *arm* rather than an *entry* — but the derive walks the enum at
    /// compile time to build the wire schema, and the names it writes are the
    /// variant identifiers. So this list moves with the type whether anybody
    /// remembers it or not, which is exactly the property a hand-written sample
    /// cannot have.
    ///
    /// It panics rather than returning an empty set if `Withheld` stops being an
    /// enum on the wire, because an empty set would make the comparison below
    /// hold for nothing.
    fn schema_variants() -> BTreeSet<&'static str> {
        match <Withheld as postcard_schema::Schema>::SCHEMA.ty {
            postcard_schema::schema::DataModelType::Enum(variants) => {
                variants.iter().map(|variant| variant.name).collect()
            }
            other => panic!("`Withheld` is no longer an enum on the wire: {other:?}"),
        }
    }

    /// What the production function filed under this loss actually builds, or
    /// `None` for the one loss nothing reachable builds.
    ///
    /// **It calls [`rendered`] and [`lost`] rather than describing them**, which
    /// is the whole of the reachability claim: a variant whose producer is
    /// rewired away has nothing to hand back, and an arm pointed at the wrong
    /// producer returns a different variant than the one it is filed under.
    ///
    /// Exhaustive over [`Withheld`], which is the enforcement: an eighth loss is
    /// a compile error here, and its author has to name the call that builds it
    /// or say out loud that there is none.
    fn produced(one: &Withheld) -> Option<Withheld> {
        fn from_rendered(answered: Answered<((), Body)>) -> Withheld {
            match rendered(answered, 0).0 {
                Outcome::Err(RemoteError::Withheld(withheld)) => withheld,
                other => panic!("`rendered` no longer answers a `Withheld`: {other:?}"),
            }
        }
        fn from_lost(standing: Standing) -> Withheld {
            match lost(standing) {
                RemoteError::Withheld(withheld) => withheld,
                other => panic!("`lost` no longer answers a `Withheld`: {other:?}"),
            }
        }

        Some(match one {
            Withheld::Panicked { .. } => from_rendered(Answered::Faulted(
                catching(|| panic!("produced")).expect_err("it panicked"),
            )),
            Withheld::Refused { .. } => from_rendered(Answered::Refused { name: "observer" }),
            Withheld::ObserverPanicked { .. } => {
                from_rendered(Answered::ObserverPanicked { name: "observer" })
            }
            Withheld::Dropped => from_lost(Standing::Running),
            Withheld::Restarting { .. } => {
                from_lost(Standing::Restarting(Arc::new(Ended::Stopped)))
            }
            Withheld::Gone => from_lost(Standing::Gone),
            // **The one exemption, and it is by name below rather than by
            // count.** A local `Origin` carries no `Queueing`, so
            // `ask_catching`'s expiry arm cannot fire; the served path answers
            // `RemoteError::TimedOut`, which carries the millisecond figure this
            // variant has nowhere to put.
            //
            // **One construction site, not two.** That local arm is the whole of
            // it, and it is the unreachable one, so no test can drive it. The
            // served condition is not a second site: `rendered` builds `TimedOut`
            // there, which is reachable and which `tests/actors.rs` pins. Counting
            // it as a site of this variant made the exemption look twice as
            // load-bearing as it is, and hid that the served half is guarded.
            Withheld::Expired => return None,
        })
    }

    /// The [`Unanswered`] that means the same thing, or `None` for the one
    /// variant [`ask`](LocalActor::ask) resumes instead of returning.
    ///
    /// Exhaustive over [`TurnLost`].
    fn twin(one: &TurnLost) -> Option<Unanswered> {
        match one {
            TurnLost::Turn(withheld) => Some(Unanswered::Turn(withheld.clone())),
            TurnLost::Reentrant => Some(Unanswered::Reentrant),
            TurnLost::Faulted(_) => None,
            TurnLost::Full => Some(Unanswered::Full),
        }
    }

    /// The name this state carries in the sample the walk below builds.
    ///
    /// # An array is not a match, and these three do not make it one
    ///
    /// [`every_withheld`]'s doc carries the whole argument: exhaustiveness forces
    /// an **arm**, and nothing in Rust forces an **entry** in a sample. What
    /// closed that hole for [`Withheld`] was [`schema_variants`], off a derive —
    /// and [`Withheld`] is the only one of these four enums that has one, because
    /// it is the only one that crosses the wire.
    ///
    /// **This is what is available for the other three, and it is worth saying
    /// what it does and does not buy.** It does not force an entry. It puts
    /// `error[E0004]` **in this module**, in the file that holds the arrays, so
    /// an author who adds a fourth [`NotQueued`] state stops at a compile error
    /// three lines from the array they have to extend — rather than at a guard
    /// that quietly walks 3 of 4 and passes, which is what this test did for
    /// [`Full`](NotQueued::Full) until it was found by review. The literal name
    /// sets in the walk are the other half: a dropped entry fails an assertion
    /// that **names it**, where a length comparison against the array's own
    /// `len()` is a tautology and reports nothing.
    ///
    /// The strings are hand-written and self-checking, for [`variant_name`]'s
    /// reason: a typo puts a name in one set and not the other, in both
    /// directions.
    fn not_queued_name(one: &NotQueued) -> &'static str {
        match one {
            NotQueued::Closed => "Closed",
            NotQueued::Reentrant => "Reentrant",
            NotQueued::Full => "Full",
        }
    }

    /// [`not_queued_name`]'s, for the enum the value doors answer.
    fn unanswered_name(one: &Unanswered) -> &'static str {
        match one {
            Unanswered::Turn(_) => "Turn",
            Unanswered::Reentrant => "Reentrant",
            Unanswered::Full => "Full",
        }
    }

    /// [`not_queued_name`]'s, for the enum `ask_catching` answers.
    ///
    /// [`twin`] is already exhaustive over this type, so the compile error was
    /// here before this function was. What was missing is the half that names a
    /// dropped entry: `twin` answering `None` more often is caught by the count
    /// below it, and an entry that never existed is not.
    fn turn_lost_name(one: &TurnLost) -> &'static str {
        match one {
            TurnLost::Turn(_) => "Turn",
            TurnLost::Faulted(_) => "Faulted",
            TurnLost::Reentrant => "Reentrant",
            TurnLost::Full => "Full",
        }
    }

    /// One vocabulary, every loss in it reachable, and no two of them saying the
    /// same sentence.
    ///
    /// # It replaces a guard whose premise stopped existing
    ///
    /// `every_turn_lost_except_faulted_has_one_undelivered_twin_with_the_same_words`
    /// walked two flat enums variant-by-variant and held eight assertions over
    /// three of them. That correspondence is gone: [`TurnLost`] and
    /// [`Unanswered`] both carry [`Withheld`] now, so the shared arm cannot drift
    /// because there is nothing for it to drift from.
    ///
    /// **The type system is not the replacement, and saying it is would be the
    /// mistake worth naming.** It gives non-drift on the shared arm and nothing
    /// else: not the four hand-written `Reentrant` texts, not the sentences a
    /// `NotQueued` widens onto, not collisions between two `Withheld` variants,
    /// and — most of all — **not reachability**. Nothing in Rust says a variant
    /// has a producer, which is exactly how
    /// [`Expired`](Withheld::Expired) reached zero reachable ones without a gate
    /// noticing. So nine properties are held here by hand, and each one is a way
    /// this could go wrong that the compiler cannot see.
    ///
    /// # The nine
    ///
    /// 1. **The four `Reentrant` texts match.** They are hand-written on
    ///    [`TurnLost`], [`Unanswered`], [`NotQueued`] and [`Failure`], and they
    ///    are one of the two sets in the crate that still are. This property
    ///    covers the first two, through [`twin`]; property 8 covers the fourth,
    ///    which no conversion reaches.
    /// 2. **[`NotQueued`]'s three states read as their widenings do**, through
    ///    `From<NotQueued> for Unanswered` — the conversion the crate performs,
    ///    not a copy that could agree with nothing. That impl is exhaustive, so a
    ///    fourth state is `error[E0004]` on **production** code.
    /// 3. **No two sentences collide across the whole [`Unanswered`] surface**,
    ///    which is every `Withheld` wrapped in [`Turn`](Unanswered::Turn) plus
    ///    `Reentrant` and `Full`. Checking only inside `Withheld` would pass a
    ///    copy-paste that gave [`Gone`](Withheld::Gone) the re-entrancy sentence.
    /// 4. **No `#[error]` text is empty**, which refuses an equality that holds
    ///    because both sides are `""`.
    /// 5. **Every `Withheld` has a producer**, checked by *calling* it — see
    ///    [`produced`]. `Expired` is exempted **by name**, so a second exemption
    ///    is a visible edit rather than a count going up.
    /// 6. **The fault exception is exactly one variant wide**, so a [`twin`] that
    ///    answered `None` to everything — and therefore skipped every comparison
    ///    in property 1 — is refused.
    /// 7. **The [`Withheld`] sample is every variant the enum has**, compared
    ///    against the list `#[derive(Schema)]` wrote — see [`schema_variants`].
    ///    Without it the others are properties of *whatever [`every_withheld`]
    ///    happens to hold*: an eighth loss forces an arm in [`produced`] and in
    ///    [`variant_name`], and neither forces an entry in that array, so the walk
    ///    would cover 7 of 8 and pass.
    /// 8. **[`Failure`]'s two hand-written sentences are compared directly**,
    ///    because no `From` reaches that type from any of the other three.
    ///    `Reentrant` must read as [`Unanswered::Reentrant`] does, and
    ///    [`Failure::Full`] must **not**: it says *call* where the other three say
    ///    *message*, which is the door it answers rather than drift, and the
    ///    inequality is what stops that difference being unified by accident.
    /// 9. **Each of the other three samples is every variant its enum has**,
    ///    against a literal set of names — see [`not_queued_name`]. It is property
    ///    7's job done with the one mechanism these three have, and it is weaker:
    ///    they cross no wire, so no derive writes their variant list and nothing
    ///    forces an *entry*. What it buys is `error[E0004]` **in this module**
    ///    beside the arrays, and an assertion that names the entry that went
    ///    missing.
    ///
    /// # What each property can hold for, if it held for nothing
    ///
    /// Property 5 is the one that carries its own two-sidedness: [`produced`]
    /// runs the real function and the assertion compares *discriminants*, so an
    /// arm pointed at the wrong producer fires and a producer that stopped
    /// building a `Withheld` at all panics inside the helper. The exemption list
    /// is compared against a literal `["Expired"]` rather than a length, which is
    /// what makes it by-name.
    ///
    /// Property 3's count is against the sample's own length, so it catches a
    /// collision and **not** a short sample — seven distinct sentences out of
    /// eight variants pass it, and property 9 is what refuses that. Property 2
    /// asserts both that no two states widen onto one answer and that the walk
    /// wrote as many as it read, and that second count is a **literal 3**: read
    /// off `queued.len()` it compared the array against itself and held for an
    /// array of any length, including one a new variant had been left out of.
    ///
    /// **The equality in property 1's walk is trivially true on the
    /// [`Turn`](TurnLost::Turn) arm** — both sides render `{0}` of the same
    /// value — and that is said rather than left for a reader to discover. The
    /// arms that can fail are `Reentrant` and `Full`, and they are the arms the
    /// property is about.
    ///
    /// # Made red seven times, one per novel property
    ///
    /// By `scripts/break-once.sh`, filtered on this test's own name plus
    /// `-p keylink --lib`. Six edits are in **this** file and the seventh is in
    /// `keylink/src/error.rs`, so a quote of one of the six is a second
    /// occurrence unless it is unindented — the harness aborts at "found 2" — and
    /// the seventh is counted in the file it breaks, where this record is not.
    /// Measured: `Withheld::Expired,` with its twelve spaces counts 1, because
    /// this record's own quote of it is not indented that way.
    ///
    /// **Four of the seven were added when a merge gate found the guard blind to
    /// [`Full`](Unanswered::Full).** The release put that variant on three enums
    /// and grew none of the three samples, so the walk covered 2 of 3, 3 of 4 and
    /// 2 of 3 — and the only mechanical thing holding the array to the enum was a
    /// comparison of `queued.len()` against itself. Breaks (a) and (b) are the two
    /// that were green before it, on a branch whose headline is the variant they
    /// perturb.
    ///
    /// - point [`produced`]'s gone arm at the wrong producer: search
    ///   `Withheld::Gone => from_lost(Standing::Gone),` and replace it with
    ///   `Withheld::Gone => from_lost(Standing::Running),`.
    ///   Watched: `the producer filed under Gone builds Dropped`,
    ///   `left: Discriminant(3)`, `right: Discriminant(5)`.
    /// - give [`every_withheld`]'s `Withheld::Dropped` entry the re-entrancy
    ///   sentence — it cannot be edited in place, so the break replaces the
    ///   `Withheld::Dropped,` line with
    ///   `Withheld::Refused { name: "observer".to_owned() },`, a second copy of an
    ///   existing variant. Watched:
    ///   `two answers share one sentence`, `left: 7`, `right: 8`.
    /// - drop a variant from [`every_withheld`] while leaving the array seven long,
    ///   which is what an eighth loss added to the enum and not to the sample looks
    ///   like from here: search `Withheld::Expired,` and replace it with
    ///   `Withheld::Gone,`. **Hand the harness both with their twelve spaces of
    ///   indentation** — bare, each counts 2, because this line is the second. The
    ///   indented search literal counts 1. Watched:
    ///   `every_withheld is not every Withheld`.
    /// - **(a)** point the widening at the wrong answer: search
    ///   `NotQueued::Full => Self::Full,` and replace it with
    ///   `NotQueued::Full => Self::Reentrant,`. Watched:
    ///   `one fact must read as one sentence: Full and Reentrant`,
    ///   `left: "the actor's mailbox is full and it shed this message"`,
    ///   `right: "this call was made from inside the target actor's own turn: it would deadlock"`.
    ///   **Green before this fix**, because `queued` held two of the three states
    ///   and the arm was never called.
    /// - **(b)** the same at the other door: search
    ///   `TurnLost::Full => Some(Unanswered::Full),` and replace it with
    ///   `TurnLost::Full => Some(Unanswered::Reentrant),`. Watched: the same
    ///   sentence and the same two texts, from property 1's walk rather than
    ///   property 2's. **Green before this fix**, for the same reason: `losses`
    ///   held three of the four.
    /// - **(c)** hand the walk the sample this branch shipped with: search
    ///   `let queued = [NotQueued::Closed, NotQueued::Reentrant, NotQueued::Full];`
    ///   and replace it with
    ///   `let queued = [NotQueued::Closed, NotQueued::Reentrant];`. Watched:
    ///   ``queued` is not every `NotQueued``, `left: {"Closed", "Reentrant"}`,
    ///   `right: {"Closed", "Full", "Reentrant"}`. This is the break that names
    ///   the defect the gate found, and it is the one the old `queued.len()`
    ///   comparison could not express at all.
    /// - **(d)** drift [`Failure`]'s copy of the re-entrancy sentence, in
    ///   `keylink/src/error.rs` and so counted there: search
    ///   `#[error("this call was made from inside the target actor's own turn: it would deadlock")]`
    ///   — which is unique in that file, where the same text appears twice in this
    ///   one — and replace `it would deadlock` with `it deadlocks`. Watched:
    ///   ``one fact must read as one sentence, and `Failure` has no `From` to
    ///   perform the comparison for it``. Nothing held this text before property 8.
    #[test]
    fn one_vocabulary_and_every_loss_in_it_has_a_producer() {
        let every = every_withheld();

        // 7. First, because every property below it is a property of this sample
        // and says nothing about a variant the sample never held.
        assert_eq!(
            every.iter().map(variant_name).collect::<BTreeSet<_>>(),
            schema_variants(),
            "`every_withheld` is not every `Withheld`: the list on the left is written by hand \
             and the one on the right is written by the derive",
        );

        // 5. Reachability, by running the producer rather than describing it.
        let mut exempt = Vec::new();
        for one in &every {
            match produced(one) {
                None => exempt.push(format!("{one:?}")),
                Some(built) => assert_eq!(
                    std::mem::discriminant(&built),
                    std::mem::discriminant(one),
                    "the producer filed under {one:?} builds {built:?}",
                ),
            }
        }
        assert_eq!(
            exempt,
            ["Expired"],
            "`Expired` is the one loss nothing reachable builds, and a second \
             exemption is a decision rather than an omission",
        );

        // 3 and 4. The whole surface a caller of `ask` can meet.
        let surface: Vec<Unanswered> = every
            .iter()
            .cloned()
            .map(Unanswered::Turn)
            .chain([Unanswered::Reentrant, Unanswered::Full])
            .collect();
        assert_eq!(
            surface.iter().map(unanswered_name).collect::<BTreeSet<_>>(),
            BTreeSet::from(["Turn", "Reentrant", "Full"]),
            "the surface is not every `Unanswered`: the set on the left is what \
             this sample holds and the one on the right is written out once",
        );
        let words: BTreeSet<String> = surface.iter().map(ToString::to_string).collect();
        assert_eq!(
            words.len(),
            surface.len(),
            "two answers share one sentence: {words:?}",
        );
        assert!(
            words.iter().all(|text| !text.is_empty()),
            "an empty `#[error]` makes every equality here hold for nothing",
        );

        // 1 and 6. The fault is the one loss `ask` resumes instead of reporting,
        // and `Reentrant` and `Full` are the two sentences four enums still write
        // out by hand.
        let losses = [
            TurnLost::Turn(Withheld::Gone),
            TurnLost::Reentrant,
            TurnLost::Full,
            TurnLost::Faulted(catching(|| panic!("faulted")).expect_err("it panicked")),
        ];
        assert_eq!(
            losses.iter().map(turn_lost_name).collect::<BTreeSet<_>>(),
            BTreeSet::from(["Turn", "Faulted", "Reentrant", "Full"]),
            "`losses` is not every `TurnLost`: the set on the left is what this \
             sample holds and the one on the right is written out once",
        );
        let mut without_a_twin = 0;
        for one in &losses {
            match twin(one) {
                None => without_a_twin += 1,
                Some(mine) => assert_eq!(
                    one.to_string(),
                    mine.to_string(),
                    "one fact must read as one sentence: {one:?} and {mine:?}",
                ),
            }
        }
        assert_eq!(
            without_a_twin, 1,
            "`Faulted` is the one variant `ask` resumes rather than maps",
        );

        // 2. The third enum, through the widening the crate actually performs.
        let queued = [NotQueued::Closed, NotQueued::Reentrant, NotQueued::Full];
        assert_eq!(
            queued.iter().map(not_queued_name).collect::<BTreeSet<_>>(),
            BTreeSet::from(["Closed", "Reentrant", "Full"]),
            "`queued` is not every `NotQueued`: the set on the left is what this \
             sample holds and the one on the right is written out once",
        );
        let mut widened = BTreeSet::new();
        for one in &queued {
            let mine = Unanswered::from(*one);
            assert_eq!(
                one.to_string(),
                mine.to_string(),
                "one fact must read as one sentence: {one:?} and {mine:?}",
            );
            assert!(
                widened.insert(format!("{mine:?}")),
                "two `NotQueued` states must not widen onto one `Unanswered`: {mine:?}",
            );
        }
        assert_eq!(
            widened.len(),
            3,
            "every `NotQueued` state is widened exactly once, and the three is a \
             literal for the reason `[\"Expired\"]` above is one: read off \
             `queued.len()` both sides came from the same hand-written array and \
             the comparison held for any array at all: {widened:?}",
        );

        // 8. The fourth enum, which no conversion reaches, so it is compared
        // against the vocabulary directly.
        assert_eq!(
            Failure::Reentrant.to_string(),
            Unanswered::Reentrant.to_string(),
            "one fact must read as one sentence, and `Failure` has no `From` to \
             perform the comparison for it",
        );
        assert_ne!(
            Failure::Full.to_string(),
            Unanswered::Full.to_string(),
            "`Failure::Full` says *call* where the other three say *message*, \
             which is the door it answers and not drift — this fires when the \
             two are unified, which is a decision rather than a fix",
        );
    }

    /// The state a `Mutex` would otherwise be wrapped around.
    #[derive(Default)]
    struct Counter {
        seen: Vec<u64>,
        inside: bool,
        /// A reply parked past the turn that received it, which is what a
        /// `#[defer]` handler does with one. Held by the state, so it dies when the
        /// state does.
        parked: Option<Deferred<usize>>,
    }

    /// A mailbox by hand, in the shape `service!` generates one.
    ///
    /// **Hand-written on purpose.** This module is where the loop lives, so a
    /// probe that could only be built by the macro would prove the macro and the
    /// loop agree rather than that the loop holds. Every part is the real thing —
    /// a real [`Origin`], a real [`Reply<T>`](Reply), **two different** payload
    /// types so a monomorphisation coincidence cannot mask a fault, and a real
    /// `&mut Counter` held across an await. RFC-07 §1.3's probe is what this
    /// replaces, and its house rule is why nothing here is a stand-in.
    enum Mail {
        /// Push a number, answer with the new length.
        Add {
            origin: Origin,
            n: u64,
            reply: Reply<usize>,
        },
        /// Enter, yield, leave — answering whether anybody else was inside.
        /// A `bool` and not a `usize`, so the two reply channels are distinct
        /// types.
        Alone { origin: Origin, reply: Reply<bool> },
        /// Panic inside the turn.
        Boom { origin: Origin, reply: Reply<usize> },
        /// Never return.
        Wedge { origin: Origin, reply: Reply<usize> },
        /// Enter, say so, and wait to be let go.
        ///
        /// [`Wedge`](Self::Wedge) wedges but never says *when* it started, which is
        /// why `abort_ends_an_actor_that_stop_could_not` sleeps 20 ms to let the
        /// turn begin. A test about `busy` cannot borrow that: it would be
        /// asserting on a flag whose window it guessed at. These two channels make
        /// "the turn has started" and "the turn may finish" both observable, so
        /// every assertion around them is ordered rather than raced.
        Held {
            origin: Origin,
            entered: oneshot::Sender<()>,
            release: oneshot::Receiver<()>,
            reply: Reply<usize>,
        },
        /// Park the reply in the state and answer nothing. The hand-written twin
        /// of what `#[defer]` generates, here for the reason every other variant
        /// is: this is where the loop lives, so a probe only the macro could build
        /// would prove the macro and the loop agree rather than that the loop holds.
        Park { origin: Origin, reply: Reply<usize> },
        /// Answer whatever is parked, from a turn that is not the one that took it.
        Release { origin: Origin, reply: Reply<usize> },
        /// Answer whatever is parked and **then** panic, which is the ordering
        /// `Verdict::Faulted` exists to decide: the value was sent, the turn that
        /// sent it produced nothing committable, and a durability adapter refuses
        /// a panicked turn.
        ReleaseThenBoom { origin: Origin, reply: Reply<usize> },
        /// Reserve on this actor's **own** handle from inside its own turn, and
        /// answer what the door said.
        ///
        /// The handle rides in the message rather than in the state for the
        /// reason [`Held`](Self::Held)'s channels do: [`LocalActor::spawn`] wants
        /// the state before it can hand back a handle to put inside it. Every
        /// other self-call probe in this workspace is in `tests/cycles.rs` and
        /// goes through a generated `Handle`; this one is here because the door
        /// it tests is here.
        Reserving {
            origin: Origin,
            me: LocalActor<Counter, Mail>,
            reply: Reply<Result<(), NotQueued>>,
        },
    }

    impl Turn<Counter> for Mail {
        async fn run(self, state: &mut Counter, hooks: &mut Hooks<Counter>) -> Option<Ended> {
            match self {
                Mail::Add { origin, n, reply } => {
                    turn(state, hooks, origin, "Add", reply, move |_from, state| {
                        Box::pin(async move {
                            state.seen.push(n);
                            state.seen.len()
                        })
                    })
                    .await
                }
                Mail::Alone { origin, reply } => {
                    turn(state, hooks, origin, "Alone", reply, move |_from, state| {
                        Box::pin(async move {
                            let alone = !state.inside;
                            state.inside = true;
                            n0_future::time::sleep(Duration::from_millis(5)).await;
                            state.inside = false;
                            alone
                        })
                    })
                    .await
                }
                Mail::Boom { origin, reply } => {
                    turn(state, hooks, origin, "Boom", reply, move |_from, _state| {
                        Box::pin(async move { panic!("bad message") })
                    })
                    .await
                }
                Mail::Wedge { origin, reply } => {
                    turn(
                        state,
                        hooks,
                        origin,
                        "Wedge",
                        reply,
                        move |_from, _state| {
                            Box::pin(async move {
                                std::future::pending::<()>().await;
                                0usize
                            })
                        },
                    )
                    .await
                }
                Mail::Held {
                    origin,
                    entered,
                    release,
                    reply,
                } => {
                    turn(state, hooks, origin, "Held", reply, move |_from, _state| {
                        Box::pin(async move {
                            let _ = entered.send(());
                            let _ = release.await;
                            0usize
                        })
                    })
                    .await
                }
                Mail::Park { origin, reply } => {
                    turn_deferred(
                        state,
                        hooks,
                        origin,
                        "Park",
                        reply,
                        |_from, state, reply| {
                            Box::pin(async move {
                                let (deferred, replied) = reply.later();
                                state.parked = Some(deferred);
                                replied
                            })
                        },
                    )
                    .await
                }
                Mail::Release { origin, reply } => {
                    turn(state, hooks, origin, "Release", reply, |_from, state| {
                        Box::pin(async move {
                            let parked = state.parked.take();
                            let answered = usize::from(parked.is_some());
                            if let Some(deferred) = parked {
                                deferred.send(99);
                            }
                            answered
                        })
                    })
                    .await
                }
                Mail::ReleaseThenBoom { origin, reply } => {
                    turn(
                        state,
                        hooks,
                        origin,
                        "ReleaseThenBoom",
                        reply,
                        |_from, state| {
                            Box::pin(async move {
                                if let Some(deferred) = state.parked.take() {
                                    deferred.send(99);
                                }
                                panic!("after the send");
                                #[allow(unreachable_code)]
                                0usize
                            })
                        },
                    )
                    .await
                }
                Mail::Reserving { origin, me, reply } => {
                    turn(
                        state,
                        hooks,
                        origin,
                        "Reserving",
                        reply,
                        move |_from, _state| {
                            Box::pin(async move {
                                // A permit that ever came back is dropped at the
                                // end of this statement. What the test reads is
                                // the verdict, and a permit held past the turn
                                // would wedge this actor for good.
                                me.reserve().await.map(|_permit| ())
                            })
                        },
                    )
                    .await
                }
            }
        }
    }

    fn add(n: u64) -> impl FnOnce(Origin, Reply<usize>) -> Mail {
        move |origin, reply| Mail::Add { origin, n, reply }
    }

    #[tokio::test]
    async fn an_actor_mutates_its_state_without_a_lock() {
        let actor = LocalActor::spawn(Counter::default());
        assert_eq!(actor.ask(add(1)).await.expect("running"), 1);
        assert_eq!(actor.ask(add(2)).await.expect("running"), 2);
    }

    /// A permit carries the turn its slot was reserved for, and answers it.
    ///
    /// The cheapest thing that could be wrong here is the permit sending nothing
    /// at all — [`Permit`] holds the slot and a `Drop` would return it silently,
    /// so a caller who never checked would see a message that vanished. The
    /// assertion is on the **answer**, which only a turn that ran can produce.
    #[tokio::test]
    async fn a_permit_spends_its_slot_on_a_turn_that_runs() {
        let actor = LocalActor::spawn(Counter::default());

        let permit = actor.reserve().await.expect("a live actor has a slot");
        assert_eq!(permit.ask(add(1)).await.expect("running"), 1);

        // And the slot is back, so the actor is not wedged behind its own permit.
        assert_eq!(actor.ask(add(2)).await.expect("running"), 2);
    }

    /// The three answers `try_reserve` separates, where tokio's own gives two.
    ///
    /// `Ok(None)` is *full right now* and `Err(Closed)` is a fate. Holding a
    /// permit is what makes the mailbox full without a wedged turn to arrange:
    /// the default local depth is one, so one held slot is the whole of it.
    #[tokio::test]
    async fn try_reserve_separates_a_full_mailbox_from_a_closed_one() {
        // Annotated, because this test never sends a message and `M` is otherwise
        // ambiguous: `Infallible` also implements `Turn<Counter>`.
        let actor: LocalActor<Counter, Mail> = LocalActor::spawn(Counter::default());

        let held = actor.reserve().await.expect("a live actor has a slot");
        assert!(
            actor
                .try_reserve()
                .expect("a live actor is not closed")
                .is_none(),
            "a held permit is the whole of a depth-one mailbox, so the next one is full",
        );

        // Dropping it returns the slot, which is the half a leak would break.
        drop(held);
        assert!(
            actor
                .try_reserve()
                .expect("a live actor is not closed")
                .is_some(),
            "a dropped permit must return its slot",
        );

        actor
            .stop()
            .await
            .expect("a stop from outside is never refused");
        assert_eq!(
            actor.try_reserve().err(),
            Some(NotQueued::Closed),
            "a stopped actor answers a fate, not a schedule",
        );
    }

    /// A shedding mailbox refuses where the default parks, and the permit doors
    /// do neither.
    ///
    /// **The control is the second half, and it is what makes the first half mean
    /// anything.** `Err(NotQueued::Full)` on its own is equally consistent with a
    /// `send` that answers `Full` unconditionally, and no assertion above it would
    /// notice. So the same call is made against a `Park` actor in the same state —
    /// depth one, its single permit held, mailbox full — and it must **not**
    /// return. Parking is only observable as an absence, so a deadline is the
    /// instrument and its expiry is the assertion.
    ///
    /// Depth one, so one held permit is the whole mailbox and no wedged turn has
    /// to be arranged. [`try_reserve`](LocalActor::try_reserve) is asserted under
    /// `Shed` as well, because the policy must not reach the doors that reserve:
    /// that is the promise [`NotQueued::Full`]'s own rustdoc makes to a caller who
    /// cannot afford to lose its arguments, and a shed that swallowed it would
    /// leave a shedding actor with no lossless door at all.
    ///
    /// **Three doors, because the variant reaches a caller by three different
    /// routes and no two of them share an arm.** `notify` returns the send's own
    /// error untouched; `ask` goes through `answered`, whose `map_err` widens onto
    /// [`TurnLost`] and is then folded onto [`Unanswered`]; and `read` uses `?`,
    /// which is the only local door that runs `From<NotQueued> for Unanswered`.
    ///
    /// The third assertion was added because the second did not cover what its
    /// comment claimed. Mis-widening the `From` impl to `Unanswered::Reentrant`
    /// left this test **green** — `ask` never runs that impl — so the arm was
    /// unguarded while a sentence here said it was not. Both breaks are now red,
    /// and each is red at its own assertion.
    #[tokio::test]
    async fn a_shedding_mailbox_refuses_where_the_default_parks() {
        let depth = NonZeroUsize::MIN;

        let shedding: LocalActor<Counter, Mail> = LocalActor::spawn_deep(
            Counter::default(),
            Vec::new(),
            (),
            Backpressure::Shed(depth),
        );
        let held = shedding.reserve().await.expect("a live actor has a slot");
        assert_eq!(
            shedding.notify(add(1)).await,
            Err(NotQueued::Full),
            "a queue-only door answers the shed in its own words",
        );
        assert_eq!(
            shedding.ask(add(1)).await.err(),
            Some(Unanswered::Full),
            "a door that wanted a value answers the same fact in its own words",
        );
        assert_eq!(
            shedding.read(|state: &Counter| state.inside).await.err(),
            Some(Unanswered::Full),
            "`read`'s `?` runs `From<NotQueued>`, which no other local door does",
        );
        assert!(
            shedding
                .try_reserve()
                .expect("a live actor is not closed")
                .is_none(),
            "the permit doors are unchanged: full is still a schedule, not a refusal",
        );
        drop(held);

        // THE CONTROL. Same depth, same held permit, same call — the policy is the
        // only difference, so it is the only thing the answers above can be
        // attributed to.
        let parking: LocalActor<Counter, Mail> = LocalActor::spawn_deep(
            Counter::default(),
            Vec::new(),
            (),
            Backpressure::Park(depth),
        );
        let held = parking.reserve().await.expect("a live actor has a slot");
        assert!(
            n0_future::time::timeout(Duration::from_millis(200), parking.notify(add(1)))
                .await
                .is_err(),
            "the default parks on a full mailbox, so this send must not return at all",
        );
        drop(held);
    }

    /// A reserve from inside the target actor's own turn is refused, at any depth.
    ///
    /// **The stricter half, and `tell`'s is the comparison that makes it a
    /// decision.** A self-`tell` into a free slot is allowed, because the message
    /// it carries runs after this turn returns. A permit carries no message: the
    /// caller chooses afterwards between a `tell`, which would be fine, and an
    /// `rpc`, which waits for an answer only the blocked turn can produce. This
    /// mailbox has a free slot at the moment the handler asks — the loop released
    /// its permit when it took the message it is running — so a `WhenFull` rule
    /// here would have answered `Ok` and handed out the deadlock.
    #[tokio::test]
    async fn a_reserve_from_inside_a_turn_is_refused_before_the_permit() {
        let actor = LocalActor::spawn(Counter::default());
        let me = actor.clone();

        let verdict = actor
            .ask(move |origin, reply| Mail::Reserving { origin, me, reply })
            .await
            .expect("the turn itself runs and answers");

        assert_eq!(
            verdict.expect_err("a self-reserve must not hand out a permit"),
            NotQueued::Reentrant,
            "a reserve from inside the actor's own turn was not refused",
        );

        // The refusal costs nothing: the actor took no permit and is still here.
        assert_eq!(actor.ask(add(1)).await.expect("running"), 1);
    }

    /// An owned permit is `Send + 'static`, so it survives the frame that made it.
    ///
    /// The borrowed form cannot be written this way — it borrows the handle — and
    /// that is the only difference between the two. `macros.rs`'s expansion module
    /// holds the compile-only half of this claim, which covers the borrowed form
    /// as well; this is the half that also runs.
    #[tokio::test]
    async fn an_owned_permit_crosses_a_spawn_and_still_answers() {
        let actor = LocalActor::spawn(Counter::default());

        let permit = actor
            .clone()
            .reserve_owned()
            .await
            .expect("a live actor has a slot");

        let sent = tokio::spawn(async move { permit.ask(add(4)).await });
        assert_eq!(
            sent.await.expect("no panic").expect("running"),
            1,
            "the permit reserved here answered there",
        );
    }

    /// The load-bearing one. Each turn sets a flag on entry, awaits, and asserts
    /// the flag was not set by anyone else on exit. Made red by replacing the
    /// mailbox with a direct call.
    #[tokio::test]
    async fn two_calls_to_one_actor_never_overlap() {
        let actor = LocalActor::spawn(Counter::default());

        let calls = (0..8).map(|_| {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask(|origin, reply| Mail::Alone { origin, reply })
                    .await
                    .expect("running")
            })
        });

        for call in calls.collect::<Vec<_>>() {
            assert!(call.await.expect("no panic"), "a turn overlapped another");
        }
    }

    /// The actor survives, and the caller learns.
    #[tokio::test]
    async fn a_panicking_message_costs_one_message_not_the_actor() {
        let actor = LocalActor::spawn(Counter::default());

        let boom = {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask(|origin, reply| Mail::Boom { origin, reply })
                    .await
            })
        };
        let failure = boom.await.expect_err("the panic reaches the caller");
        assert!(failure.is_panic(), "got {failure:?}");

        // And the very next message runs against the state as it stands.
        assert_eq!(actor.ask(add(7)).await.expect("still running"), 1);
    }

    /// A faulted turn is **answered**, and the answer says the actor is alive.
    ///
    /// The premise this replaces was that a panicking handler's reply sender died
    /// and the caller inferred a fault from a closed channel. It did not: the
    /// panic already crossed the channel, but it crossed it *inside*
    /// `Answered::Ran(Err(..))` — a fault dressed as a value, one level in, where
    /// the served path and the local path each unwrapped it their own way. What
    /// this pins is the discriminant: a fault is its own answer and its own
    /// `TurnLost`, and neither is `Gone`.
    #[tokio::test]
    async fn a_faulted_turn_is_answered_faulted_and_not_gone() {
        let actor = LocalActor::spawn(Counter::default());

        let lost = actor
            .ask_catching(|origin, reply| Mail::Boom { origin, reply })
            .await
            .expect_err("a panicking turn has no value");
        assert!(
            matches!(lost, TurnLost::Faulted(_)),
            "a panicking turn answered {lost:?}"
        );
        assert!(lost.to_string().contains("bad message"), "got {lost}");

        // The half that makes `Faulted` a different fact rather than a second
        // spelling of `Gone`: the actor is still there, and a caller told `Gone`
        // would have stopped calling it.
        assert_eq!(actor.ask(add(7)).await.expect("still running"), 1);
    }

    /// And the new arm does not swallow the real one. An actor that is genuinely
    /// gone is still `Gone`, on both routes to that answer.
    #[tokio::test]
    async fn an_actor_that_stopped_is_still_gone_and_never_faulted() {
        // Route one: the mailbox refuses the send.
        let stopped = LocalActor::spawn(Counter::default());
        let spare = stopped.clone();
        stopped
            .stop()
            .await
            .expect("a stop from outside the actor's own turn is never refused");
        let lost = spare
            .ask_catching(add(1))
            .await
            .expect_err("a stopped actor has no value");
        assert!(
            matches!(lost, TurnLost::Turn(Withheld::Gone)),
            "a stopped actor said {lost:?}"
        );

        // Route two — the one the rewritten arm is on: the message was taken and
        // the loop then died without answering. This is the case whose three
        // reasons stay collapsed, because a dead loop cannot send a fourth
        // answer.
        let aborted = LocalActor::spawn(Counter::default());
        let wedged = {
            let actor = aborted.clone();
            tokio::spawn(async move {
                actor
                    .ask_catching(|origin, reply| Mail::Wedge { origin, reply })
                    .await
            })
        };
        n0_future::time::sleep(Duration::from_millis(20)).await;
        aborted.abort();

        let lost = wedged
            .await
            .expect("the caller is not itself aborted")
            .expect_err("an aborted turn has no value");
        assert!(
            matches!(lost, TurnLost::Turn(Withheld::Gone)),
            "an aborted turn said {lost:?}"
        );
    }

    /// `stop` is ordered: what was sent before it is handled.
    #[tokio::test]
    async fn stop_drains_what_was_sent_before_it() {
        let actor = LocalActor::spawn(Counter::default());
        assert_eq!(actor.ask(add(1)).await.expect("running"), 1);
        actor
            .stop()
            .await
            .expect("a stop from outside the actor's own turn is never refused");
        assert!(!actor.is_running());
    }

    #[tokio::test]
    async fn a_handle_to_a_stopped_actor_reports_a_closed_mailbox() {
        let actor = LocalActor::spawn(Counter::default());
        let spare = actor.clone();
        actor
            .stop()
            .await
            .expect("a stop from outside the actor's own turn is never refused");

        assert_eq!(
            spare.ask(add(1)).await,
            Err(Unanswered::Turn(Withheld::Gone))
        );
        // The same closed mailbox through the other question, so the other type:
        // an `ask` lost a reply channel too and a `notify` never had one.
        assert_eq!(spare.notify(add(1)).await, Err(NotQueued::Closed));
    }

    /// `stop`'s error type is one variant wider than `stop` can produce, and the
    /// half that makes it so was argued and not asserted.
    ///
    /// [`stop`](LocalActor::stop)'s `# Errors` block says
    /// [`NotQueued::Reentrant`] is the **only** error it can return, and that
    /// [`NotQueued::Closed`] "never leaves this method". The first half has a
    /// guard — `cycles::a_self_stop_is_refused_rather_than_wedging_the_actor`
    /// produces the refusal. The second had none: **the one send `stop` makes can
    /// return `Closed`**, and only the `Ok(()) | Err(NotQueued::Closed) => {}` arm
    /// keeps it inside. Nothing anywhere read `stop`'s `Ok`, so that arm could
    /// have been narrowed to `Ok(())` and every test in the workspace would have
    /// stayed green while a caller that stopped an actor twice got an error the
    /// documentation says is unreachable.
    ///
    /// The line above it is asserted too, and it is the reason `wait_for` is not
    /// `changed()`: the second `stop` must **return**. A fresh receiver marks the
    /// current value as seen, so `changed()` on an already-ended actor waits for a
    /// second ending that never comes. The timeout is what separates "returned an
    /// error" from "hung", and only a two-sided check can tell those apart —
    /// `expect_err` on a hang is a test that never finishes.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `stop`'s first
    /// arm. **Pass the arm WITH its twelve spaces of indentation** — the bare line
    /// `Ok(()) | Err(NotQueued::Closed) => {}` occurs twice in this file and the
    /// harness aborts at `found 2` before taking a backup, so this fails safe
    /// rather than quietly breaking the wrong arm. Replace it with a pair that lets
    /// the send's own refusal out:
    ///
    /// ```text
    /// Ok(()) => {}
    /// Err(closed @ NotQueued::Closed) => return Err(closed),
    /// ```
    ///
    /// Quoted unindented, because this record lives in the file its break edits.
    /// Watched: `stopping a stopped actor is what the caller asked for, so its own
    /// Closed must not escape: Err(Closed)`, `left: Err(Closed)`, `right: Ok(())`.
    ///
    /// **The obvious break is not a break, and it was run first.** Deleting the
    /// `| Err(NotQueued::Closed)` alternative alone leaves the match
    /// non-exhaustive, so the harness exits **101 on `error[E0004]`** — a red that
    /// reddens the same way a caught defect does, on a run where the test never
    /// executed. The second arm is what makes the edit compile and therefore what
    /// makes the assertion the thing being measured. This is written down because
    /// the exhaustiveness that makes the mistake easy is the same property the
    /// guard above [`twin`] rests on.
    #[tokio::test]
    async fn a_second_stop_returns_ok_rather_than_the_send_s_own_closed() {
        // Annotated, because this test never sends a message and `M` is otherwise
        // ambiguous: `Infallible` also implements `Turn<Counter>`.
        let actor: LocalActor<Counter, Mail> = LocalActor::spawn(Counter::default());
        actor
            .stop()
            .await
            .expect("a stop from outside the actor's own turn is never refused");

        let again = n0_future::time::timeout(Duration::from_secs(5), actor.stop())
            .await
            .expect("stop is documented as idempotent, so a second one must return");
        assert_eq!(
            again,
            Ok(()),
            "stopping a stopped actor is what the caller asked for, so its own \
             Closed must not escape: {again:?}",
        );
    }

    /// Spawn an actor already inside a turn, and hand back the two ends that
    /// order everything around it: a handle, and the sender that lets the turn go.
    ///
    /// Returns once the turn has **provably started** — `entered` resolving is the
    /// happens-after edge, so no assertion below it is racing the dispatch.
    async fn held() -> (
        LocalActor<Counter, Mail>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<usize, Unanswered>>,
    ) {
        hold(LocalActor::spawn(Counter::default())).await
    }

    /// The same, for an actor the caller spawned — which is how the supervised
    /// path gets the identical treatment without a second copy of this.
    async fn hold(
        actor: LocalActor<Counter, Mail>,
    ) -> (
        LocalActor<Counter, Mail>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<usize, Unanswered>>,
    ) {
        let (entered, started) = oneshot::channel();
        let (release, releasing) = oneshot::channel();
        let turning = {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask(|origin, reply| Mail::Held {
                        origin,
                        entered,
                        release: releasing,
                        reply,
                    })
                    .await
            })
        };
        started.await.expect("the turn started");
        (actor, release, turning)
    }

    /// The two halves of `busy`, in one test and in this order, because
    /// **neither half is a guard on its own**.
    ///
    /// The positive alone passes for an implementation that never clears the flag.
    /// The negative alone passes for one that never sets it — an empty
    /// `Watched::mark_busy`, an `Arc` never wrapped — and it is the half that reads
    /// like the interesting one, so it is the trap.
    ///
    /// Ordered turn → read for the same reason: the recorded break is
    /// `mark_busy(false)` → `mark_busy(true)` on the mail dispatch, which leaves
    /// the flag set **after** a turn. A read performed by an actor that had never
    /// run a turn would read `false` under that break and stay green.
    ///
    /// **Watched break, `Inbox::mark_busy`'s mail arm in `run_loop`,
    /// `mark_busy(false)` → `mark_busy(true)`:**
    /// `a_turn_reports_busy_and_a_read_does_not ... FAILED`, with
    /// `a read is not a turn and must not report busy`.
    #[tokio::test]
    async fn a_turn_reports_busy_and_a_read_does_not() {
        let (actor, release, turning) = held().await;

        // Positive: observed from outside, ordered by `entered` rather than by a
        // sleep. The turn is inside `Held`'s body right now.
        assert!(actor.busy(), "a running turn must report busy");

        release.send(()).expect("the turn is still waiting");
        turning
            .await
            .expect("no panic")
            .expect("the actor answered");

        // Negative: observed from **inside** a read, which runs on the actor's own
        // loop. Sampling from another task would be a timing test; this is ordered
        // by construction. `busy` is an atomic load, not a message, so it
        // cannot queue behind the read it runs inside.
        let seen = {
            let actor = actor.clone();
            actor
                .clone()
                .read(move |_state| actor.busy())
                .await
                .expect("the actor is running")
        };
        assert!(!seen, "a read is not a turn and must not report busy");
    }

    /// The question `queued` answers exactly: will the next caller block.
    ///
    /// **Watched break, `LocalActor::queued`, `max_capacity()` →
    /// `capacity()`:** `a_full_mailbox_is_visible_before_a_caller_blocks_on_it ...
    /// FAILED`, with `left: 0, right: 1`.
    #[tokio::test]
    async fn a_full_mailbox_is_visible_before_a_caller_blocks_on_it() {
        let (actor, release, turning) = held().await;

        // The turn holds the loop, so nothing drains. An empty slot first.
        assert_eq!(actor.queued(), 0, "nothing is queued yet");

        // `notify` returns when the message has a permit, so this is ordered.
        let _ = actor.notify(add(1)).await.expect("the actor is running");
        assert_eq!(
            actor.queued(),
            1,
            "the queued message is what the next caller would block behind"
        );

        release.send(()).expect("the turn is still waiting");
        turning
            .await
            .expect("no panic")
            .expect("the actor answered");
    }

    /// The supervised path wraps its receiver too — which is a **second** call
    /// site and therefore a second thing to forget.
    ///
    /// This is the guard for that, and it exists because its absence was
    /// measured: with `spawn_supervised` passing its raw receiver, **not one test
    /// in the lib suite went red** — including its three siblings here, which all
    /// reach the actor through `spawn_fed`. `busy` would have answered `false`
    /// for ever on every supervised actor and nothing would have said so.
    ///
    /// Named siblings rather than a pass count, because a count rots on the next
    /// test anybody adds: `a_turn_reports_busy_and_a_read_does_not`,
    /// `a_full_mailbox_is_visible_before_a_caller_blocks_on_it` and
    /// `an_aborted_turn_does_not_leave_the_handle_reporting_busy`.
    ///
    /// **Watched break, `spawn_supervised`, `Watched { rx: inbox, busy }` →
    /// `inbox`:** `a_supervised_actor_reports_busy_through_its_handle ... FAILED`,
    /// with `a supervised actor's handle must see its turn`.
    #[tokio::test]
    async fn a_supervised_actor_reports_busy_through_its_handle() {
        let limit = Intensity::new(
            NonZeroU32::new(2).expect("two is not zero"),
            Duration::from_millis(20),
        )
        .expect("a budget whose backoff is not zero");
        let supervised: LocalActor<Counter, Mail> = LocalActor::spawn_supervised(
            || async { Ok((Counter::default(), Vec::new(), ())) },
            limit,
        );

        let (actor, release, turning) = hold(supervised).await;
        assert!(
            actor.busy(),
            "a supervised actor's handle must see its turn"
        );

        release.send(()).expect("the turn is still waiting");
        turning
            .await
            .expect("no panic")
            .expect("the actor answered");
        assert!(!actor.busy(), "and must see it end");
    }

    /// An abort mid-turn leaves no `run_loop` to clear the flag, so `Watched`'s
    /// `Drop` is the only thing that can — and without it a handle reports "busy"
    /// for ever about an actor that is gone.
    ///
    /// `ended()` is the sync point rather than a sleep: it resolves when the task
    /// is gone, which is after the drop that clears the cell.
    ///
    /// **Watched break, `impl Drop for Watched`, delete the `store`:**
    /// `an_aborted_turn_does_not_leave_the_handle_reporting_busy ... FAILED`, with
    /// `an aborted actor is gone, not busy`.
    #[tokio::test]
    async fn an_aborted_turn_does_not_leave_the_handle_reporting_busy() {
        let (actor, _release, turning) = held().await;
        assert!(actor.busy(), "the turn is running");

        actor.abort();
        assert!(
            matches!(actor.ended().await, Ended::Vanished),
            "an aborted actor vanishes"
        );

        assert!(!actor.busy(), "an aborted actor is gone, not busy");
        let _ = turning.await;
    }

    /// A turn that never returns cannot be stopped, and can be aborted.
    #[tokio::test]
    async fn abort_ends_an_actor_that_stop_could_not() {
        let actor = LocalActor::spawn(Counter::default());
        let wedged = {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask(|origin, reply| Mail::Wedge { origin, reply })
                    .await
            })
        };

        // Let the turn start, so the abort lands mid-turn rather than before it.
        n0_future::time::sleep(Duration::from_millis(20)).await;
        actor.abort();

        assert_eq!(
            wedged.await.expect("the caller is not itself aborted"),
            Err(Unanswered::Turn(Withheld::Gone)),
            "an aborted turn reports the actor as gone"
        );
    }

    /// A reply parked past its turn is answered by a later one, and the value goes
    /// to the caller that has been waiting since the first.
    ///
    /// The macro's twin of this is `tests/defer.rs`; this one is about the loop.
    #[tokio::test]
    async fn a_parked_reply_is_answered_by_a_later_turn() {
        let actor: LocalActor<Counter, Mail> = LocalActor::spawn(Counter::default());

        let parked = {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask_catching(|origin, reply| Mail::Park { origin, reply })
                    .await
            })
        };
        // The `Park` turn has to have finished for `Release` to get a permit, so a
        // successful `Release` is itself the proof that the reply outlived its turn.
        let mut released = 0;
        for _ in 0..200 {
            released = actor
                .ask(|origin, reply| Mail::Release { origin, reply })
                .await
                .expect("running");
            if released == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(released, 1, "nothing was ever parked");
        assert_eq!(
            parked.await.expect("no panic").expect("answered"),
            99,
            "the parked caller got the later turn's value"
        );
    }

    /// And the answer when nobody ever answers: **`Withheld::Gone`, not a hang.**
    ///
    /// Here rather than only in `tests/defer.rs` because this is where the variant
    /// is visible — a generated `Handle` never yields a `TurnLost` at all, only
    /// the `Unanswered` [`ask`](LocalActor::ask) maps one into, so the
    /// criterion's own words can only be asserted from inside the crate.
    ///
    /// The timeout is the assertion. A hang is not a failure to a harness.
    #[tokio::test]
    async fn a_parked_reply_whose_actor_ends_is_gone_rather_than_a_hang() {
        let actor: LocalActor<Counter, Mail> = LocalActor::spawn(Counter::default());

        let parked = {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask_catching(|origin, reply| Mail::Park { origin, reply })
                    .await
            })
        };
        // Ordered behind the park, so the park has run by the time this returns.
        actor
            .stop()
            .await
            .expect("a stop from outside the actor's own turn is never refused");

        let lost = n0_future::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("a parked reply must not outlive its actor as a hang")
            .expect("the caller is not itself panicking")
            .expect_err("nothing answered it");
        assert!(
            matches!(lost, TurnLost::Turn(Withheld::Gone)),
            "a reply parked in a state that was dropped said {lost:?}"
        );
    }

    /// An observer that refuses one named method and accepts everything else.
    ///
    /// One method rather than all of them because a refusal **ends the actor**: an
    /// observer that vetoed the `Park` turn too would leave no actor for a later
    /// turn to release anything from, and the test would pass on `Gone` for the
    /// wrong reason. That is not hypothetical — it is what the first version of
    /// this test did, and `Err(Turn(Gone))` is what it printed.
    struct Vetoing {
        method: &'static str,
    }

    impl Observer<Counter> for Vetoing {
        fn name(&self) -> &'static str {
            "vetoing"
        }

        fn after_turn<'a>(
            &'a mut self,
            _state: &'a Counter,
            turn: TurnInfo<'a>,
        ) -> crate::TurnFut<'a> {
            let refuse = turn.method == self.method;
            Box::pin(async move {
                match refuse {
                    true => Err(Stop::new("vetoing refused")),
                    false => Ok(()),
                }
            })
        }
    }

    /// Wait for the `Park` turn to have parked, by reading the state rather than
    /// by sleeping. A `read` takes a mailbox permit like anything else, so it can
    /// only answer once the park's turn has finished.
    async fn until_parked(actor: &LocalActor<Counter, Mail>) {
        for _ in 0..500 {
            if actor
                .read(|state| state.parked.is_some())
                .await
                .expect("running")
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("nothing ever parked");
    }

    /// **The commit point for a released reply.** A turn that answers a parked
    /// caller and is then refused by an observer must not let that answer cross —
    /// and the parked caller is told the same thing the turn's own caller is,
    /// naming the same observer.
    ///
    /// This is where the `#[defer]` durability window closed. Before it,
    /// `Deferred::send` wrote to the channel from inside the handler, so the parked
    /// caller had `99` before any observer had seen anything, and a `Persist` that
    /// then refused left it holding an answer for state that was never stored.
    ///
    /// Here rather than only in `tests/persist.rs` because a **parked** caller is
    /// the half that door cannot reach: `tests/persist.rs` drives the turn's own
    /// caller through a `Handle`, and the released reply is answered by
    /// `parking.rs` on a channel the handler took with `later()`. Both are
    /// asserted below, on the same name, which is the whole finding — the two
    /// callers of one turn must not be told different things about it.
    ///
    /// Made red by `scripts/break-once.sh` on `parking.rs`'s `park`, replacing the
    /// `if let Some(mut lot) = slot.take() {` guard with `if let Some(mut lot) =
    /// Option::<Vec<Parked>>::None {` — the send then escapes to the immediate
    /// branch, which is exactly the old behaviour. Watched: `a released reply
    /// crossed a refused turn: Ok(99)`.
    #[tokio::test]
    async fn a_released_reply_is_refused_with_the_turn_that_released_it() {
        let actor: LocalActor<Counter, Mail> = LocalActor::spawn_observed(
            Counter::default(),
            vec![Box::new(Vetoing { method: "Release" })],
        );

        // The `Park` turn is accepted, so the channel is in the state and the
        // actor is still there — which is what leaves anything to release.
        let parked = {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask_catching(|origin, reply| Mail::Park { origin, reply })
                    .await
            })
        };
        until_parked(&actor).await;

        let releasing = actor
            .ask_catching(|origin, reply| Mail::Release { origin, reply })
            .await;
        assert!(
            matches!(releasing, Err(TurnLost::Turn(Withheld::Refused { ref name })) if name == "vetoing"),
            "the releasing turn's own caller is refused: got {releasing:?}"
        );

        let lost = n0_future::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("a released reply must not hang its caller")
            .expect("the caller is not itself panicking");
        assert!(
            matches!(lost, Err(TurnLost::Turn(Withheld::Refused { ref name })) if name == "vetoing"),
            "a released reply crossed a refused turn: {lost:?}"
        );
    }

    /// [`Vetoing`], one arm apart: it panics where that one returns a [`Stop`].
    ///
    /// Structurally identical on purpose, so the test below differs from the one
    /// above in the observer's **behaviour** and in nothing else.
    struct FallingOver {
        method: &'static str,
    }

    impl Observer<Counter> for FallingOver {
        fn name(&self) -> &'static str {
            "falling-over"
        }

        fn after_turn<'a>(
            &'a mut self,
            _state: &'a Counter,
            turn: TurnInfo<'a>,
        ) -> crate::TurnFut<'a> {
            let fall = turn.method == self.method;
            Box::pin(async move {
                match fall {
                    true => panic!("the observer fell over"),
                    false => Ok(()),
                }
            })
        }
    }

    /// A **parked** caller is told the observer panicked, in the same words the
    /// turn's own caller is told.
    ///
    /// The value is withheld exactly as it is above, and that half is not what
    /// this adds: `Verdict::ObserverPanicked` and `Verdict::Refused` do the same
    /// thing to a parked answer. What it adds is that the two are told **apart**,
    /// on the release path, which is a separate route from the fold the turn's own
    /// caller reads — `parking.rs` releases six lines earlier, from the same pair
    /// of facts, and the two must not disagree about one turn.
    ///
    /// A `Deferred` caller had no other door to this fact. It reads no `Ended`, so
    /// the sentence was the whole of what it got, and it was the refusal's.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/turns.rs` on
    /// [`Deferred::send`]'s release arm. Search
    /// `Verdict::ObserverPanicked { name } => reply.answer(Answered::ObserverPanicked { name }),`
    /// and give it the refusal's answer instead:
    /// `Verdict::ObserverPanicked { name } => reply.answer(Answered::Refused { name }),`.
    /// Both quotes are unindented and the code is not, which is what keeps the
    /// anchor unique in its own file.
    ///
    /// **This record's first anchor was dead on arrival, and the reason is worth
    /// more than the fix.** It quoted the arm's *body* off a three-line block —
    /// uniqueness measured, count 1 — and then `cargo fmt` collapsed the block to
    /// the single line above, taking the count to 0. The check had been run
    /// **before** the formatter, which owns the line breaks and is not the
    /// author's to promise. `.mise/tasks/anchors` names that rot mode; nothing
    /// caught this instance, because the gate checked only **registered** rows and
    /// this record was not one. It is registered now, as `O3`.
    ///
    /// **The sequencing is the lesson and it is not obvious**, so it is written
    /// here rather than left to be inferred: format first, then count. Every anchor
    /// this branch adds was re-measured **after** `cargo fmt` once this was found,
    /// and all five resolve to exactly one. `scripts/break-once.sh`'s own header
    /// carries the rule, because that is where an author meets the harness.
    ///
    /// Watched: `a parked caller must read the same words as the turn's own:
    /// Err(Turn(Refused { name: "falling-over" }))`. **The assertion above it stayed
    /// green under that break**, which is the measurement that makes this test
    /// worth having: the release path and the caller-side fold are two routes to
    /// one fact, and breaking one leaves the other saying the right thing.
    #[tokio::test]
    async fn a_released_reply_reads_the_panic_with_the_turn_that_released_it() {
        let actor: LocalActor<Counter, Mail> = LocalActor::spawn_observed(
            Counter::default(),
            vec![Box::new(FallingOver { method: "Release" })],
        );

        let parked = {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask_catching(|origin, reply| Mail::Park { origin, reply })
                    .await
            })
        };
        until_parked(&actor).await;

        let releasing = actor
            .ask_catching(|origin, reply| Mail::Release { origin, reply })
            .await;
        assert!(
            matches!(
                releasing,
                Err(TurnLost::Turn(Withheld::ObserverPanicked { ref name }))
                    if name == "falling-over"
            ),
            "the releasing turn's own caller reads the panic: got {releasing:?}"
        );

        let lost = n0_future::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("a released reply must not hang its caller")
            .expect("the caller is not itself panicking");
        assert!(
            matches!(
                lost,
                Err(TurnLost::Turn(Withheld::ObserverPanicked { ref name }))
                    if name == "falling-over"
            ),
            "a parked caller must read the same words as the turn's own: {lost:?}"
        );
    }

    /// And the panicking half, which cannot be told the same thing.
    ///
    /// A turn sends a parked value and then panics. The value must not cross — a
    /// durability adapter refuses a panicked turn, so nothing behind that answer
    /// was committed — and it cannot be answered `Faulted` either: there is one
    /// `Panic`, it is the original payload rather than a message, and it belongs
    /// to the turn's own caller. So the channel is dropped, and a dropped channel
    /// inside a **live** actor is [`Withheld::Dropped`].
    ///
    /// **`Dropped` and not `Gone`, and this test is where the two are told apart
    /// by one run.** The first assertion is `Faulted`, which only a loop that
    /// caught the panic and kept going can produce — so the same actor that
    /// answers the second assertion is provably still serving. Answering `Gone`
    /// there would be the crate contradicting itself two lines apart, which is the
    /// defect the variant was added for.
    ///
    /// The two assertions are otherwise the two halves of the commit point: the
    /// turn's own caller gets the panic (so the payload really did go somewhere),
    /// and the parked caller gets no value (so it did not go to two places, and no
    /// uncommitted value crossed).
    ///
    /// Made red by replacing `parking.rs`'s `Verdict::Faulted => drop(reply)` arm
    /// in `Deferred::send` with `Verdict::Faulted => reply.answer(Answered::Ran(value))`.
    /// Watched: `a value sent by a turn that then panicked crossed anyway:
    /// Ok(99)`. Re-run when the assertion moved from `Gone` to `Dropped`, and the
    /// line came back **unchanged**: that break makes the value cross before any
    /// variant is chosen, so it cannot tell one from the other.
    ///
    /// So a second break, which is the one this test's name rests on:
    /// [`answered`](LocalActor::answered)'s
    /// `Standing::Running => Withheld::Dropped,` → `Standing::Running =>
    /// Withheld::Gone,`, the fold the variant was split out of. Watched:
    /// `a value sent by a turn that then panicked crossed anyway: Err(Turn(Gone))`.
    ///
    /// **Hand the harness that literal with its sixteen spaces of indentation, and
    /// the paragraph above is why.** This record lives in the file it breaks, so
    /// the bare form counts 2 here — the arm, and this record's own copy of it —
    /// and the harness aborts at "found 2". The indented form is unique, and it
    /// cannot be written out above without becoming a second copy of itself, which
    /// is the same trap one rung down. The records in `tests/defer.rs` that quote
    /// this arm carry the same clause; this copy is what makes it needed.
    #[tokio::test]
    async fn a_released_reply_is_dropped_when_the_turn_that_released_it_panicked() {
        let actor: LocalActor<Counter, Mail> = LocalActor::spawn(Counter::default());

        let parked = {
            let actor = actor.clone();
            tokio::spawn(async move {
                actor
                    .ask_catching(|origin, reply| Mail::Park { origin, reply })
                    .await
            })
        };
        until_parked(&actor).await;

        let faulted = actor
            .ask_catching(|origin, reply| Mail::ReleaseThenBoom { origin, reply })
            .await;
        assert!(
            matches!(faulted, Err(TurnLost::Faulted(_))),
            "the turn's own caller gets the panic: got {faulted:?}"
        );

        let lost = n0_future::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("a released reply must not hang its caller")
            .expect("the caller is not itself panicking");
        assert!(
            matches!(lost, Err(TurnLost::Turn(Withheld::Dropped))),
            "a value sent by a turn that then panicked crossed anyway: {lost:?}"
        );
    }

    /// The actor ends when nobody holds it, without anybody saying stop.
    #[tokio::test]
    async fn dropping_every_handle_ends_the_actor() {
        // Annotated because nothing here sends a message, so there is nothing
        // for `M` to be inferred from — which is `E0283` and the same hazard
        // `mount`'s doctest pins one layer up.
        let actor: LocalActor<Counter, Mail> = LocalActor::spawn(Counter::default());
        let task = actor.task.clone();
        drop(actor);

        for _ in 0..100 {
            if task.is_finished() {
                return;
            }
            n0_future::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the actor outlived its last handle");
    }

    // ───────────────────────── what a peer whose reply died is told, and when

    /// A supervisor answers on **both** arms, and the refusal is `Gone`.
    ///
    /// This is the shape of the defect it replaces, asserted rather than argued.
    /// The hook used to write a `watch` channel itself, on the one arm that
    /// granted a restart, and return nothing — so a permanent give-up published
    /// no verdict at all, the channel still read `Running`, and the mailbox
    /// `supervise` owns for the actor's whole life still read open. A peer whose
    /// parked reply died on that give-up was told `dropped: `, *the actor is
    /// still there*, about an actor that would never answer again.
    ///
    /// Three calls: the budget grants one restart, refuses the second fault, and
    /// refuses a `Stopped` outright because nobody restarts an actor that was
    /// asked to stop.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `Lent::ending`'s
    /// `Standing::Gone` → `Standing::Running`, which is the old behaviour written
    /// as a value instead of as a silence. Watched: `a spent budget is not a live
    /// actor: Running`.
    #[test]
    fn a_supervisor_that_refuses_a_restart_answers_gone_rather_than_running() {
        let (_mail, mut inbox) = mpsc::channel::<Message<Counter, Mail>>(1);
        let mut budget = Budget::new(
            Intensity::new(
                NonZeroU32::new(1).expect("one restart"),
                Duration::from_secs(10),
            )
            .expect("a ten second backoff is not zero"),
        );
        let mut lent = Lent {
            inbox: &mut inbox,
            budget: &mut budget,
            started: Instant::now(),
        };
        let fault = Ended::Panicked {
            message: "bad message".to_string(),
        };

        let granted = lent.ending(&fault);
        assert!(
            matches!(granted, Standing::Restarting(_)),
            "the first fault is inside the budget: {granted:?}"
        );
        let refused = lent.ending(&fault);
        assert!(
            matches!(refused, Standing::Gone),
            "a spent budget is not a live actor: {refused:?}"
        );
        let asked = lent.ending(&Ended::Stopped);
        assert!(
            matches!(asked, Standing::Gone),
            "nobody restarts an actor that was asked to stop: {asked:?}"
        );
    }

    /// A state that reads its own actor's [`Standing`] **as it dies**.
    ///
    /// Which is the whole point: the state's drop is the instant every
    /// [`Deferred`] parked in it dies, so what this records is exactly what a
    /// peer woken by one of those deaths can read. Nothing here is a stand-in —
    /// it is the real [`supervise`], the real [`run_loop`] and the real [`Lent`].
    struct Dying {
        fate: Fate,
        /// A sender for this actor's own mailbox, so the record below can say
        /// whether it is still taking messages at the same instant.
        mail: mpsc::Sender<Message<Dying, Fatal>>,
        seen: Arc<Mutex<Vec<(Standing, bool)>>>,
    }

    impl Drop for Dying {
        fn drop(&mut self) {
            let record = (self.fate.standing(), self.mail.is_closed());
            self.seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(record);
        }
    }

    /// End this incarnation the way a fault does, with no panic to catch.
    ///
    /// A `Turn` may end the loop by naming an ending, which is what an observer's
    /// refusal does one rung up. Returning [`Ended::Panicked`] rather than
    /// panicking keeps the test's own output clean and reaches the identical arm
    /// of [`retryable`].
    struct Fatal;

    impl Turn<Dying> for Fatal {
        async fn run(self, _state: &mut Dying, _hooks: &mut Hooks<Dying>) -> Option<Ended> {
            Some(Ended::Panicked {
                message: "this incarnation is over".to_string(),
            })
        }
    }

    /// **Two ordering claims, asserted rather than reasoned about**, at the one
    /// instant that can see them.
    ///
    /// A dying incarnation settles two things before its state is dropped: the
    /// verdict a lost reply is answered from, which a served dispatch and a local
    /// `ask` both read, and — on the arm where there will be no
    /// next incarnation — the mailbox, so that `is_running()` and `ask` stop
    /// lying. Both must land before the state dies, because that is when a parked
    /// `Deferred` dies and a peer is woken.
    ///
    /// **There is no way to observe either from outside the actor**, and that is
    /// the finding rather than a limitation of this test. The whole teardown runs
    /// without an await, so on a current-thread runtime nothing else is polled
    /// until it is over and every ordering — including a broken one — reads
    /// correct; on a multi-thread one the window is instructions wide and the
    /// probe becomes a race that hangs when pushed. So the observer is the state
    /// itself, in its own `Drop`.
    ///
    /// Two incarnations and two records. The first fault is inside the budget:
    /// `Restarting`, and the mailbox stays **open**, which is the structural
    /// promise that a restart needs no re-queue. The second is not: `Gone`, and
    /// the mailbox is closed. `start_paused` for `tests/supervision.rs`'s reason —
    /// an incarnation's measured lifetime is then exactly zero, so the budget
    /// cannot reset under the test and the backoff costs no wall time.
    ///
    /// Made red three ways, each watched:
    ///
    /// - `scripts/break-once.sh keylink/src/turns.rs`, replacing `run_loop`'s
    ///   `fate.settle(inbox.ending(&reason));` with a `Drop` guard holding the
    ///   same verdict — decided in the same place, still reaching [`supervise`],
    ///   published at the end of `run_loop` instead, after `drop(state)`. That is
    ///   exactly the "decide afterwards" ordering this design refuses, with
    ///   nothing else changed. Watched: `an incarnation's verdict must be settled
    ///   before its state dies: [(Running, false), (Running, true)]` — the second
    ///   `true` is this same teardown's `close`, which lands on time and is what
    ///   the record still has to show, since only the verdict was moved.
    /// - the same file, deleting `Lent::ending`'s `self.close();`. Watched:
    ///   `a give-up must close the mailbox and a restart must not: [(Restarting(
    ///   Panicked { message: "this incarnation is over" }), false), (Gone, false)]`,
    ///   with `left: [false, false]`.
    /// - the blunter one — deleting the publish outright — is red too, at this
    ///   same assertion: `an incarnation's verdict must be settled before its
    ///   state dies: [(Running, false), (Running, true)]`.
    ///
    /// **That third record used to claim something else and it went stale inside
    /// its own wave.** It read: *red at a `different` assertion, `an incarnation
    /// is there to take it: SendError`, because `supervise` reads the same cell
    /// and stops restarting.* True when it was written, and untrue three commits
    /// later — the same wave stopped `supervise` reading the cell, returning the
    /// verdict from [`run_loop`] instead so that a supervisor could not be wrong
    /// about a decision five doors now write. Deleting the publish no longer
    /// reaches the supervisor at all.
    ///
    /// Worth leaving the correction visible rather than just the fix: a record
    /// whose *reason* is a fact about another function will rot when that function
    /// changes, and nothing in a green test run says so. The replay script greps
    /// each recorded output out of the run for exactly this — a break that is
    /// still red for a different reason is the failure counting exit codes cannot
    /// see, and it is the one that happened.
    #[tokio::test(start_paused = true)]
    async fn an_ending_incarnation_settles_its_verdict_and_its_mailbox_before_its_state() {
        let fate = Fate::default();
        let seen: Arc<Mutex<Vec<(Standing, bool)>>> = Arc::default();
        let (mail, inbox) = mpsc::channel::<Message<Dying, Fatal>>(1);
        let limit = Intensity::new(
            NonZeroU32::new(1).expect("one restart"),
            Duration::from_millis(100),
        )
        .expect("a 100ms backoff is not zero");

        let running = {
            let held = fate.clone();
            let building = fate.clone();
            let sending = mail.clone();
            let recorder = seen.clone();
            n0_future::task::spawn(async move {
                let fate = held;
                supervise(
                    move || {
                        let fate = building.clone();
                        let mail = sending.clone();
                        let seen = recorder.clone();
                        async move { Ok::<_, Stop>((Dying { fate, mail, seen }, Vec::new(), ())) }
                    },
                    limit,
                    inbox,
                    &fate,
                )
                .await
            })
        };

        // The second send waits for the permit the first message freed when an
        // incarnation took it, so the two cannot land in one incarnation — and
        // both are queued long before the give-up closes anything.
        for _ in 0..2 {
            mail.send(Message::Mail(Box::new(Fatal), Chain::Empty))
                .await
                .expect("an incarnation is there to take it");
        }
        drop(mail);

        let ended = n0_future::time::timeout(Duration::from_secs(60), running)
            .await
            .expect("the supervisor gives up rather than restarting for ever")
            .expect("the supervising task is not itself panicking");
        assert!(
            matches!(ended, Ended::GaveUp { after: 1, .. }),
            "one restart was budgeted and the second fault spent nothing left: {ended:?}"
        );

        let seen = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let verdicts: Vec<&Standing> = seen.iter().map(|(standing, _)| standing).collect();
        assert!(
            matches!(
                verdicts.as_slice(),
                [Standing::Restarting(_), Standing::Gone]
            ),
            "an incarnation's verdict must be settled before its state dies: {seen:?}"
        );
        let closed: Vec<bool> = seen.iter().map(|(_, closed)| *closed).collect();
        assert_eq!(
            closed.as_slice(),
            [false, true],
            "a give-up must close the mailbox and a restart must not: {seen:?}"
        );
    }

    /// Drive a supervisor to the point where it is **between** incarnations, and
    /// hold it there.
    ///
    /// The window both guards below are about: the previous incarnation ended
    /// with a restart granted, so the cell holds `Restarting` and there is no
    /// `run_loop` anywhere — and therefore no floor but `supervise`'s own. The
    /// factory parks on the second call rather than a `sleep` being timed, so the
    /// window is entered deterministically and stays open until the test is done
    /// with it.
    ///
    /// `calls` counts factory calls so a test can choose what the second one
    /// does; `open` fires once the chosen call has been entered.
    fn between_incarnations(
        calls: &Arc<std::sync::atomic::AtomicUsize>,
        open: Arc<tokio::sync::Notify>,
        transient: bool,
    ) -> (Fate, mpsc::Sender<Message<Dying, Fatal>>, JoinHandle<Ended>) {
        use std::sync::atomic::AtomicUsize;

        let fate = Fate::default();
        let (mail, inbox) = mpsc::channel::<Message<Dying, Fatal>>(1);
        let limit = Intensity::new(
            NonZeroU32::new(4).expect("four restarts"),
            Duration::from_millis(100),
        )
        .expect("a 25ms backoff is not zero");

        let held = fate.clone();
        let building = fate.clone();
        let counted: Arc<AtomicUsize> = Arc::clone(calls);
        let sending = mail.clone();
        let running = n0_future::task::spawn(async move {
            let fate = held;
            supervise(
                move || {
                    let nth = counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let fate = building.clone();
                    let mail = sending.clone();
                    let open = Arc::clone(&open);
                    async move {
                        match nth {
                            // The first incarnation exists only to fault, so that
                            // a restart is granted and the cell holds `Restarting`.
                            0 => Ok((
                                Dying {
                                    fate,
                                    mail,
                                    seen: Arc::default(),
                                },
                                Vec::new(),
                                (),
                            )),
                            // The second either fails transiently — the documented
                            // path that `continue`s with budget left — or parks.
                            1 if transient => {
                                open.notify_one();
                                Err(Stop::new(std::io::Error::other("not yet")))
                            }
                            _ => {
                                open.notify_one();
                                std::future::pending::<()>().await;
                                unreachable!("parked for the rest of the test")
                            }
                        }
                    }
                },
                limit,
                inbox,
                &fate,
            )
            .await
        });
        (fate, mail, running)
    }

    /// **N1: there is no `run_loop` between incarnations, and the window still
    /// needs a floor.**
    ///
    /// A supervisor that has granted a restart leaves `Restarting` in the cell and
    /// then backs off and calls the factory again — with no `run_loop` alive for
    /// any of it. Destroy the actor in that window and the cell must stop saying
    /// *retry, another incarnation is coming* about an actor that has been
    /// destroyed.
    ///
    /// `LocalActor::abort` used to settle this itself, and **deleting that line
    /// was a defect of mine**: I judged it unreachable because no break of it
    /// could redden `an_aborted_actor_tells_a_peer_the_actor_is_gone`, which aborts an
    /// actor sitting *inside* `run_loop`, where the floor genuinely is earlier.
    /// The line was not unguardable — it was unguarded, and this is the guard it
    /// never had. The remedy is structural rather than the line coming back:
    /// `supervise`'s floor stays armed across the backoff and the factory.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/turns.rs`, disarming
    /// `supervise`'s floor at its declaration. Watched: `an actor destroyed
    /// between incarnations is gone, not restarting: Restarting(Panicked {
    /// message: "this incarnation is over" })`.
    #[tokio::test]
    async fn a_supervisor_between_incarnations_is_gone_when_it_is_destroyed() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let open = Arc::new(tokio::sync::Notify::new());
        let (fate, mail, running) = between_incarnations(&calls, Arc::clone(&open), false);

        mail.send(Message::Mail(Box::new(Fatal), Chain::Empty))
            .await
            .expect("the first incarnation takes it");
        // Parked inside the second factory call: restart granted, no `run_loop`.
        open.notified().await;
        assert!(
            matches!(fate.standing(), Standing::Restarting(_)),
            "the window this is about is the one where a restart is pending"
        );

        // Destroy it where it stands, exactly as `abort` does.
        running.abort();
        let _ = running.await;

        assert!(
            matches!(fate.standing(), Standing::Gone),
            "an actor destroyed between incarnations is gone, not restarting: {:?}",
            fate.standing()
        );
        drop(mail);
    }

    /// **N2: a transient factory failure must not overwrite a live `Restarting`.**
    ///
    /// `Stop::new` — non-fatal, budget left — is a first-class documented path:
    /// the supervisor sleeps and tries again, and the actor is very much coming
    /// back. The per-iteration floor this replaces was a loop-body local, so
    /// `continue` dropped it **armed** and wrote `Gone` over the `Restarting` the
    /// previous incarnation had settled. A peer reading in that window was told
    /// *stop asking* about an actor being rebuilt.
    ///
    /// That is the **false** answer, not the merely less useful one: RFC-07 §12.4
    /// accepts a late read that renders [`Withheld::Dropped`], which is true when it is
    /// written, and refuses the generation counter for producing a sentence that
    /// is not. This was on the wrong side of that line.
    ///
    /// The cell is read directly rather than through a peer, because what is
    /// wrong here is the cell itself and no reply has to die to prove it.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/turns.rs`, moving
    /// `supervise`'s floor back inside the loop body — the shape this replaces,
    /// verbatim. Watched: `a supervisor that is still rebuilding must not read as
    /// gone: Gone`.
    #[tokio::test]
    async fn a_transient_factory_failure_does_not_overwrite_a_pending_restart() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let open = Arc::new(tokio::sync::Notify::new());
        let (fate, mail, running) = between_incarnations(&calls, Arc::clone(&open), true);

        mail.send(Message::Mail(Box::new(Fatal), Chain::Empty))
            .await
            .expect("the first incarnation takes it");
        // The second factory call fails transiently and `continue`s; the third
        // parks, which is how this waits for the retry to have happened.
        open.notified().await;
        open.notified().await;

        assert!(
            matches!(fate.standing(), Standing::Restarting(_)),
            "a supervisor that is still rebuilding must not read as gone: {:?}",
            fate.standing()
        );
        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) >= 3,
            "the transient failure has to have been retried for this to mean anything"
        );

        running.abort();
        let _ = running.await;
        drop(mail);
    }

    /// A state whose `Drop` panics, which is the one unwind that happens **after**
    /// the verdict is settled and after [`run_loop`]'s own floor is disarmed.
    ///
    /// A caller's `Drop` is caller's code, and nothing guards it: `run_loop`'s
    /// teardown runs `drop(state)` outside any `catching`. Drop glue continues
    /// during the unwind, so a `Deferred` parked in such a state still dies — with
    /// `Restarting` in the cell and only [`supervise`]'s floor left above it.
    struct Poisoned;

    impl Drop for Poisoned {
        fn drop(&mut self) {
            panic!("the state exploded on its way out");
        }
    }

    impl Turn<Poisoned> for Fatal {
        async fn run(self, _state: &mut Poisoned, _hooks: &mut Hooks<Poisoned>) -> Option<Ended> {
            Some(Ended::Panicked {
                message: "this incarnation is over".to_string(),
            })
        }
    }

    /// **The guard for the one line in this design that only a comment defended:
    /// [`supervise`]'s floor staying armed across [`run_loop`].**
    ///
    /// A restart is granted, so the cell holds `Restarting` and `run_loop`
    /// disarms its own floor at the settle. Then `drop(state)` panics. The unwind
    /// carries past `run_loop` — whose floor is spent — into `supervise`, and
    /// `supervise`'s floor is the only thing that will ever correct the cell from
    /// `Restarting`, which is *retry, another incarnation is coming*, about an
    /// actor that will never come back.
    ///
    /// # What this asserts, which is the cell's FINAL value and not the instant
    ///
    /// Stated because the distinction is the whole of the residual §12.4 records.
    /// The `Deferred` dies inside `drop(state)`, in `run_loop`'s frame; this floor
    /// fires when the unwind reaches `supervise`'s, which is strictly outer. **So
    /// the floor is necessarily later than the death** — provably, not by a race —
    /// and it corrects the cell for every later reader rather than pre-empting the
    /// read the death woke. That peer can still see `Restarting`.
    ///
    /// This asserts the corrected value, after joining the unwound task, because
    /// that is the part the third disarm would destroy and the part a regression
    /// would show. It does **not** assert what a peer reads at the instant of
    /// death, and no guard here does: `break 20` covers the cell-to-wire step, and
    /// the gap between them is the residual rather than an untested claim.
    ///
    /// **This is why there is no third disarm**, and the history is worth keeping
    /// because the instruction to add one was reasonable and wrong: leaving the
    /// floor armed across `run_loop` was declined on the grounds that it buys
    /// nothing, which understated it. It is load-bearing, and nothing in the
    /// replay set covered it — break 21 disarms at the *declaration*, a different
    /// edit — so correct code was protected by prose.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/turns.rs`, adding the third
    /// disarm: `floor.disarm();` before the `run_loop` call and
    /// `floor = Floor::armed(fate);` after it. Watched: `a state that panicked on
    /// the way out is gone, not restarting: Restarting(Panicked { message: "this
    /// incarnation is over" })`.
    #[tokio::test]
    async fn a_state_that_panics_on_drop_mid_restart_is_gone_not_restarting() {
        let fate = Fate::default();
        let (mail, inbox) = mpsc::channel::<Message<Poisoned, Fatal>>(1);
        let limit = Intensity::new(
            NonZeroU32::new(4).expect("four restarts"),
            Duration::from_millis(100),
        )
        .expect("a 25ms backoff is not zero");

        let running = {
            let held = fate.clone();
            n0_future::task::spawn(async move {
                let fate = held;
                supervise(
                    move || async move { Ok::<_, Stop>((Poisoned, Vec::new(), ())) },
                    limit,
                    inbox,
                    &fate,
                )
                .await
            })
        };

        mail.send(Message::Mail(Box::new(Fatal), Chain::Empty))
            .await
            .expect("the first incarnation takes it");
        // The incarnation ends with a restart granted, and then its state panics
        // on the way out — which unwinds the supervising task.
        let joined = n0_future::time::timeout(Duration::from_secs(60), running)
            .await
            .expect("the task must unwind rather than hang");
        assert!(
            joined.is_err(),
            "this test is about an unwind, so the task must not have returned normally"
        );

        assert!(
            matches!(fate.standing(), Standing::Gone),
            "a state that panicked on the way out is gone, not restarting: {:?}",
            fate.standing()
        );
        drop(mail);
    }
}
