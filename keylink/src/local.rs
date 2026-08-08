//! An actor that never leaves the process.
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
//! A local call **reports** one failure: the actor is gone. That is why
//! [`MailboxClosed`] is a unit struct and why it is not [`Error`](crate::Error).
//! RFC-04 §5.2 and §5.5 reject a handle that is the same type locally and
//! remotely, because such a handle must either claim failures the local case
//! cannot have, or hide failures the remote case does.
//! `Error::SchemaMismatch` has no meaning between two objects in one process.
//!
//! "Reports one failure" is narrower than "fails one way", and the difference is
//! worth having in front of you. A call can also panic ([`LocalActor::ask`]
//! resumes it), never return ([`Handle`] documents the self-call deadlock), or —
//! only for [`notify`](LocalActor::notify) — return `Ok` for a message that is
//! then dropped unrun, because `Ok` there means "queued" and nothing else.
//!
//! It is also why [`ended`](LocalActor::ended) exists rather than a second error
//! variant. An [`Observer`](crate::Observer) that refuses a turn ends the actor,
//! and the caller of that turn is told [`MailboxClosed`] like any other — which
//! is true, and which does not say *why*. `MailboxClosed` means "gone" and must
//! not be stretched to mean "gave up on durability", so the second question is
//! asked separately and answered by [`Ended`](crate::Ended).
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
//! one. Resuming keeps [`MailboxClosed`] honest: it means the actor is gone, and
//! never "the actor is fine and your message exploded".
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
//! handle then reports [`MailboxClosed`]: the same lie the panic policy exists
//! to remove, through a narrower door. And `panic = "abort"` defeats all of it.
//!
//! [`Handle`]: crate::LocalActor

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
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
    Body, Context, Ended, Observer, Origin, Outcome, Panic, Queueing, RemoteError, Stop, TurnInfo,
    TurnLost, TurnOutcome,
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
/// Raising it would let callers run ahead without feeling it, and would make a
/// self-`tell` succeed more often — which is worse, not better, because
/// [`Handle`](crate::LocalActor) documents self-calls as a deadlock and the
/// difference between "usually" and "always" is what makes a deadlock a
/// production incident instead of a test failure.
///
/// # A constant where the keyed one is a knob, and the name says which
///
/// `DEFAULT_MAILBOX_DEPTH` in `registry.rs` is a default and this is a value:
/// nothing overrides it, and the paragraphs below are why.
///
/// [`SystemBuilder::mailbox_depth`](crate::SystemBuilder::mailbox_depth) makes the
/// registry's depth a builder method. This one stays fixed, and the asymmetry is
/// the paragraph above rather than an oversight: on this path, depth has the
/// "matching hazard" the keyed one lacks. A keyed actor is addressed from another
/// task — a QUIC stream's — so a deeper mailbox trades latency for a longer queue
/// and nothing else. A `LocalActor` is addressed from *whatever task holds the
/// handle*, including its own turn, and there a deeper mailbox turns a documented
/// always-deadlock into an intermittent one. An intermittent deadlock is not a
/// slower program; it is a different program in production than in the tests.
///
/// The other half of the argument above stands too, and it is now measurable
/// elsewhere: the parked-sender list has no length anybody can read on **either**
/// path. What the keyed path gained is
/// [`ActorStatus::buffered`](crate::ActorStatus::buffered), which reports what
/// the channel holds and says in its own documentation that it is a floor.
///
/// If a case for a local knob ever arrives, it needs to answer the self-call
/// hazard first, and the answer is not a bigger number.
const LOCAL_MAILBOX_DEPTH: usize = 1;

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
    /// [`TurnLost::Gone`], which is what happens on its own if the actor ends
    /// while a reply is parked in its state.
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
/// [`TurnLost::Gone`] — correctly, since nothing answered. What no type can force
/// is that somebody eventually calls [`Deferred::send`]; what keylink guarantees
/// is that the caller is never left waiting on a channel nobody holds.
///
/// It also does not survive a panic. A handler that panics before consuming its
/// `Reply` drops it inside the unwinding future, so the caller is told `Gone`
/// rather than being handed the panic the way [`LocalActor::ask`] hands one on
/// every other kind. The actor still survives, the observer chain still reads
/// [`TurnOutcome::Panicked`](crate::TurnOutcome), and the panic hook already ran
/// at the fault — which is the trade [`notify`](LocalActor::notify) documents for
/// a `tell`, arriving here for a structural reason instead of a policy one.
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
/// wait ends with [`TurnLost::Gone`], which is exactly what happens by itself when
/// an actor ends with replies parked in its state.
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
///   panics, which is the one case where a fault does resolve one, as `Gone`.
///   A turn that *sent* it and then panicked is the same case now, for the same
///   reason: nothing that turn produced crosses;
/// - the actor ends, which drops the state and every reply in it, as `Gone`. An
///   observer refusing the faulted turn is how a fault reaches parked replies
///   *indirectly*, and it reaches them the same way `stop()` does.
///
/// Pinned by `tests/defer.rs::a_reply_parked_before_a_fault_is_still_answered_after_it`.
#[must_use = "dropping a deferred reply answers the caller `Gone`; that is \
              sometimes right, so say so with `drop`"]
pub struct Deferred<T>(Reply<T>);

impl<T> Deferred<T> {
    /// Answer the caller that has been waiting since the turn this was parked in.
    ///
    /// **The value is owed to the turn running now, and waits for it.** It is
    /// held until that turn's observer chain has accepted it, and then released
    /// with the same verdict the turn's own caller is answered by: an observer
    /// that refuses tells this caller [`TurnLost::Refused`] naming the same
    /// observer, and a handler that panics after sending drops the channel, which
    /// is [`TurnLost::Gone`].
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
/// Four states rather than two, because "the budget expired while it queued",
/// "an observer refused the turn that ran" and "the turn ran and exploded" are
/// three different facts and a caller that has to unwrap layers to tell them
/// apart will get it wrong. A **dropped** sender is the fifth case and stays
/// `TurnLost::Gone` — see [`LocalActor::answered`].
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
        }
    }
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
) -> (Result<T, Panic>, Option<&'static str>, Option<Ended>)
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
    let mut parking = Parking::new(turn(state));
    let answer = guarded(&mut parking).await;
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
    };

    // The third element is [`Stop::is_fatal`], carried out because it is the one
    // thing about a refusal a supervisor reads and nothing else does.
    let mut refused: Option<(&'static str, Arc<dyn Error + Send + Sync>, bool)> = None;
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
                refused = Some((name, Arc::from(reason), fatal));
                break;
            }
            // `Panic` implements `Display` and not `Error`, and is not `Sync`,
            // so it cannot fill an `Arc<dyn Error + Send + Sync>` directly.
            // `to_string()` is what `RemoteError::Failed` already does with one.
            //
            // Never fatal: an observer that panicked made no claim about whether
            // another incarnation would fare better, and answering one for it
            // would be a claim nobody made.
            Err(panic) => {
                let reason: Box<dyn Error + Send + Sync> = panic.to_string().into();
                refused = Some((name, Arc::from(reason), false));
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
    let verdict = match (&panicked, &refused) {
        (Some(_), _) => Verdict::Faulted,
        (None, Some((name, _, _))) => Verdict::Refused { name },
        (None, None) => Verdict::Committed,
    };
    for release in parked {
        release(verdict);
    }

    match (refused, panicked) {
        // Completed and accepted, or faulted and accepted — which is an observer
        // that read `TurnOutcome::Panicked` and chose to continue.
        (None, _) => (answer, None, None),
        // Panicked and refused: the fault is still the handler's outcome and the
        // actor ends naming the fault rather than the observer. A durability
        // adapter refuses a panicked turn, so reporting the adapter here would
        // point an operator at the wrong code.
        //
        // The refusal's `fatal` flag is dropped with the rest of it, and
        // `Stop::fatal`'s own documentation says so: this ending names the fault,
        // and a fault is the kind of thing another incarnation might not repeat.
        (Some((name, _, _)), Some(message)) => {
            (answer, Some(name), Some(Ended::Panicked { message }))
        }
        // Completed and refused: the turn ran and was not committed, so whoever
        // holds the reply owes the caller a refusal rather than an uncommitted
        // value.
        (Some((name, error, fatal)), None) => (
            answer,
            Some(name),
            Some(Ended::Observer { name, error, fatal }),
        ),
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
/// in the keyed loop and one in `Handle::service`'s dispatch, and two of them
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

    let (ran, refused, end) =
        observed_turn(state, hooks, method, move |state| turn(origin, state)).await;
    // The one place the handler's outcome and the observers' verdict become a
    // discriminant. A fault is answered even when an observer then refused —
    // `Answered::Faulted` is not a value, so the argument for withholding one
    // never applied to it, and withholding would reverse what this module's header
    // promises about a panicking call. A turn that *completed* and was refused is
    // withheld, because a caller must not be handed an uncommitted value.
    reply.answer(match (ran, refused) {
        (Ok(value), None) => Answered::Ran(value),
        (Ok(_), Some(name)) => Answered::Refused { name },
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
///   caller's wait ends [`TurnLost::Gone`]. The actor survives, the observers saw
///   [`TurnOutcome::Panicked`](crate::TurnOutcome), and the panic hook ran at the
///   fault. This is the one thing `#[defer]` costs against every other kind, and
///   [`Replied`] says so where a reader meets the type.
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

    let (ran, refused, end) = observed_turn(state, hooks, method, move |state| {
        turn(origin, state, reply)
    })
    .await;
    match ran {
        Ok(Replied(Settled::Now(reply, value))) => reply.answer(match refused {
            None => Answered::Ran(value),
            Some(name) => Answered::Refused { name },
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
    let (ran, _refused, end) = observed_turn(state, hooks, feed, turn).await;

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
    Mail(Box<M>),
    /// [`LocalActor::read`]. Runs no observers, by design: a read takes `&A` and
    /// cannot mutate, so storing after it is waste.
    Read(Read<A>),
    /// Ends the loop, which drops the receiver, which is what makes every other
    /// handle report [`MailboxClosed`] from then on.
    Stop,
}

/// Where the loop takes its next message from.
///
/// **Private, and that is the whole reason this trait can exist at all.** The two
/// paths into the loop carry different channel items: a [`LocalActor`] queues
/// [`Message<A, M>`] — it has a `read` and a `stop` — and a keyed actor's mailbox
/// is a [`Mailbox<M>`](crate::Mailbox), because the registry is generic in the
/// service's message and knows nothing of `A`. Making those one channel would put
/// the state's type on [`System`](crate::System), [`Mounted`](crate::Mounted) and
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
    /// entry [`System::status`](crate::System::status) reads, [`Watched`] writes
    /// the cell a [`LocalActor`] carries on its handle, and a bare
    /// `mpsc::Receiver` keeps this default — the supervision tests in this module
    /// hand `supervise` one and want no cell at all.
    ///
    /// # What the refusal that stood here weighed, and what it did not
    ///
    /// It read: *"a `LocalActor` has nowhere to report it to. Its caller holds a
    /// handle and can ask that."* That is true of an **idle** actor and it is the
    /// whole of the case it considered. It is false of a **wedged** one, which is
    /// the actor [`is_busy`](LocalActor::is_busy) exists for: asking a wedged
    /// actor queues a message behind the turn that wedged it, so the question
    /// never returns. `is_running` is `!mail.is_closed()` and answers `true` for
    /// both, which is why the two were indistinguishable before this cell.
    ///
    /// The refusal was not wrong about the cost. A cell on a handle is a
    /// reporting surface a `LocalActor` did not have, and it is paid for in the
    /// only way this crate accepts: [`Watched`] clears it on drop, so an unwind
    /// and an [`abort`](LocalActor::abort) cannot leave it lying.
    fn mark_busy(&self, _running: bool) {}

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
    /// built on it: `deliver_to_local` reads the verdict, not the mailbox. This
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
/// [`is_busy`](LocalActor::is_busy).
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
/// [`System::stop`](crate::System::stop) dropping the registry's sender, which
/// arrives here as `None`. The loop's arms for them are written to be *correct*
/// rather than to panic, which is the choice `Mail`'s `send` and `recv` variants
/// already make one layer out.
impl<A: Send, M: Send> Inbox<A, M> for Keyed<M> {
    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Message<A, M>>> {
        self.0.poll_recv(cx).map(|mail| mail.map(Message::Mail))
    }

    /// The mailbox is what the registry kept a handle on, so it is the way back to
    /// the entry [`System::status`](crate::System::status) reads.
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
/// Returns the reason instead of publishing it, because only the local path has
/// anywhere to publish it to: a `LocalActor` has an `ended` watch channel and a
/// keyed actor has none. Everything whose *order* matters happens in here.
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
/// answers every [`Deferred`] parked in it as [`TurnLost::Gone`].
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
            Taken::Mail(Some(Message::Mail(mail))) => {
                // The two lines around this await are the whole of
                // `ActorStatus::busy`, and they are here — in the one loop, at the
                // one dispatch — rather than inside the receive, so that a feed
                // turn and a mailbox turn cannot disagree about what a turn is.
                inbox.mark_busy(true);
                let ended = dispatch(mail, &mut state, &mut hooks).await;
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
                let ended = dispatch(item, &mut state, &mut hooks).await;
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
    // channel it was waiting on. `deliver_to_local` and `deliver_to_actor` both
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
/// [`System`](crate::System)'s mailbox.
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
/// [`System::status`](crate::System::status) went on listing it with `busy:
/// false` while a real call to it answered
/// [`NoSuchActor`](crate::RemoteError::NoSuchActor), and no
/// [`Lifecycle`](crate::Lifecycle) was told. `keylink::mount`'s `Loop` carries
/// the full argument; the return type here is the half of it that lives in this
/// file.
///
/// So a keyed actor still has no `ended()` channel for a *caller* to await — a
/// handle is what would hold one, and a keyed actor is addressed by id — but the
/// ending is no longer lost: the registry reaps its own entry with this value.
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
    // loop settles on its way out is what `deliver_to_actor` reads. Taken before
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
/// The triple [`actor_wired`] and [`spawn_wired`] are called with, produced by
/// one closure so that a restart cannot rebuild the state and reuse last
/// incarnation's observers — which for a durability adapter would mean reusing a
/// claim that has to be re-acquired, and for a feed would mean a subscription
/// nobody re-opened.
///
/// A tuple rather than a struct because it *is* those three arguments: a named
/// type here would be a second spelling of a call site that already exists, and
/// the reader would have to learn which field is which anyway.
///
/// [`actor_wired`]: crate::LocalActor::spawn_fed
/// [`spawn_wired`]: crate::run_keyed
pub type Incarnation<A, F> = (A, Vec<Box<dyn Observer<A>>>, F);

/// What a **served** dispatch is allowed to tell a peer whose reply channel died
/// without an answer.
///
/// Three answers, and the peer's next move is different for each: retry, do not
/// retry this one call but the actor is fine, stop asking. [`lost`] is the one
/// place they are rendered, so the two served paths cannot disagree about them.
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
/// `deliver_to_local` and `supervise` could both read a restart off it. Neither
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
/// turn and read by [`System::status`](crate::System::status); a [`LocalActor`]
/// now carries its own, written through [`Watched`] and read by
/// [`is_busy`](LocalActor::is_busy). They stay two cells because they answer two
/// readers — a served dispatch reads this one, a handle's holder reads that one —
/// and folding them would force a registry fact onto a path whose reason for
/// existing is that it has no registry.
///
/// This paragraph said a `LocalActor` had no busy cell "on purpose", citing the
/// refusal that used to stand on `Inbox::mark_busy`. That refusal weighed the
/// idle actor and is rewritten there; a paragraph citing it had to move in the
/// same commit, or the crate would document a capability it ships.
///
/// # A `Mutex` and not an atomic
///
/// [`Standing::Restarting`] carries the ending that caused it, because
/// `Failed("restarting: {reason}")` is what a peer already reads on the local path
/// and the two paths render through one function. An `AtomicU8` would hold the
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
/// it is a floor here and not a filed defect. `deliver_to_local` used to read
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
    /// # The same arm closes the mailbox, and that is a second consequence rather
    /// than a second decision
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
    /// It is deliberately **not** what a served dispatch reads — that is the
    /// `Standing` this returns. One decision, made here, with two consequences
    /// published at the two places that need them.
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
    //   it the peer reads `Failed("restarting: ..")` — *retry, another is
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
/// [`System`](crate::System)'s mailbox.
///
/// `#[doc(hidden)] pub` for [`run_keyed`]'s reason: the generated keyed
/// `spawn_supervised` calls it from inside [`mount`](crate::mount)'s closure, in
/// the user's crate. The registry is untouched by any of this — `mount` takes a
/// closure over a `Mailbox`, and this is that closure's whole body.
///
/// A keyed actor still has no `ended()` channel for a caller to await. It no
/// longer allocates one either: this used to build a `watch` channel with **no
/// reader**, because the supervisor's own restart verdict was a third state on
/// it. That verdict lives in the [`Fate`] cell the registry already made — where
/// a served dispatch can read it too — so the channel is gone rather than
/// explained, and one small allocation per supervised keyed actor went with it.
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
/// reaches the peer as `Failed("restarting: {reason}")`** — *the incarnation that
/// owed you an answer is gone, another is coming, retry*. It used to be
/// [`RemoteError::NoSuchActor`], which reads *there is nothing here, stop asking*
/// about an actor that is about to answer the same peer again, and a client that
/// treats the name as terminal — which is what the name tells it to do — stopped
/// retrying an actor that was coming back.
///
/// **The other two causes are answered too, and neither needed a supervisor.** A
/// restart is one of *three* ways a served reply dies: a handler that takes
/// `reply.later()` and drops the `Deferred` — sanctioned, and what its
/// `#[must_use]` asks for — reads `Failed("dropped: ..")`, and an actor that has
/// genuinely ended, or was aborted or evicted, still reads `NoSuchActor`. So a
/// plain unsupervised keyed actor no longer says *there is nothing here* about a
/// reply it dropped while running. `deliver_to_actor` and `deliver_to_local`
/// render all three through one function, [`lost`], so the two paths cannot say
/// different things about one condition.
///
/// The signal is the [`Fate`] cell, settled by this actor's own loop before it
/// drops anything and read off the registry entry — which is why none of it
/// reaches `Mounted`, `mount` or the generated `spawn_supervised`. RFC-07 §12
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

/// The actor is gone. The only way a local call fails.
///
/// Not [`Error`](crate::Error), and deliberately not convertible into one:
/// `Error::Transport`, `Error::SchemaMismatch` and `Error::Closed` have no local
/// counterpart, and a caller must not be able to `?` a local failure into a
/// remote error type and lose which one it was. RFC-04 §5.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the actor is no longer running")]
pub struct MailboxClosed;

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
    /// What a peer whose reply just died is told. The keyed twin of this cell
    /// lives beside the registry entry; here the handle carries it, because a
    /// singleton has no entry. See [`Standing`] for why it is not the channel
    /// above.
    fate: Fate,
    /// Whether a turn is running right now, for [`is_busy`](Self::is_busy).
    ///
    /// The other end of this `Arc` lives in the [`Watched`] the loop is polling,
    /// which is what lets a turn on the actor's own task be read from a caller's.
    /// The keyed twin is `registry.rs`'s `Busy`, kept beside the registry entry;
    /// a singleton has no entry, so the handle carries it — which is [`fate`]'s
    /// arrangement and, unlike `fate`, this one is read by the handle's holder
    /// rather than by a served dispatch.
    ///
    /// [`fate`]: Self::fate
    busy: Arc<AtomicBool>,
    /// Held so a wedged actor can still be [aborted](Self::abort). Dropping a
    /// `JoinHandle` detaches the task on both targets — it does not cancel it —
    /// so this is the only handle on a turn that never returns.
    task: Arc<JoinHandle<()>>,
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
    /// [`SystemBuilder::spawn_with`](crate::SystemBuilder::spawn_with) already
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
        let (mail, inbox) = mpsc::channel::<Message<A, M>>(LOCAL_MAILBOX_DEPTH);
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
    ///             let (restored, saver) = Persist::<Ledger, _>::open(&store, &id).await?;
    ///             Ok((restored.unwrap_or_default(), vec![Box::new(saver) as _], ()))
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
        let (mail, inbox) = mpsc::channel::<Message<A, M>>(LOCAL_MAILBOX_DEPTH);
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
            // served dispatch reads is the `Fate` below, settled inside the loop
            // before that drop, and never the mailbox.
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
    /// [`MailboxClosed`] when the actor is gone, before or during the turn.
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
    /// that one — and neither is detectable by a type. A turn can also simply
    /// await something that never fires, which is neither shape.
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
    /// [`Ended::Vanished`](crate::Ended), since an aborted task filled nothing.
    /// In a two-actor ring it frees the other actor and destroys only the one it
    /// names. Guarded at N = 2 by `tests/cycles.rs`.
    ///
    /// # Panics
    ///
    /// With whatever the handler panicked with, after the actor has survived it.
    /// Use [`ask_catching`](Self::ask_catching) where an unwind must not escape
    /// — a connection's task, for one.
    pub async fn ask<T>(&self, mail: impl FnOnce(Origin, Reply<T>) -> M) -> Result<T, MailboxClosed>
    where
        T: Send + 'static,
    {
        // Every loss collapses to `MailboxClosed` here, on purpose: a local
        // caller learns WHY from [`ended`](Self::ended), and widening this
        // error would make every existing call site match on a case it has
        // nothing to do about. A **fault** is the exception, because it is not
        // a loss: the value a panicking function produces is the panic, so the
        // caller gets it.
        match self.ask_catching(mail).await {
            Ok(value) => Ok(value),
            Err(TurnLost::Faulted(panic)) => panic.resume(),
            // `Refused` ends the actor, so `MailboxClosed` is true of it by the
            // time a caller reads it. `Expired` does NOT — and it cannot be
            // reported precisely here, because this method has one error type by
            // design. It is unreachable from a local origin today; if a later wave
            // gives one a budget, this is the call that has to widen or the call
            // sites have to move to `ask_catching`, and that is a decision rather
            // than a mapping. Named here so it is a decision somebody makes.
            Err(TurnLost::Gone | TurnLost::Refused { .. } | TurnLost::Expired) => {
                Err(MailboxClosed)
            }
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
    /// [`TurnLost::Gone`] when the actor is gone, [`TurnLost::Refused`] when an
    /// observer refused the turn, [`TurnLost::Faulted`] when the turn ran and
    /// panicked — the actor survived that one and is still running — and
    /// [`TurnLost::Expired`] when a queueing budget ran out before the turn
    /// started, which a local origin cannot do today and which is answered
    /// truthfully rather than as `Gone` for the day one can.
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
            Answered::Refused { name } => Err(TurnLost::Refused { name }),
            // Unreachable from here today — a local `Origin` carries no
            // `Queueing`, so there is no budget to expire — and answered with the
            // variant that means what happened anyway.
            //
            // This arm read `Err(TurnLost::Gone)` behind a comment claiming the
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
            Answered::Expired => Err(TurnLost::Expired),
        }
    }

    /// Send one message and bring back the loop's own answer.
    ///
    /// The one place a lost reply channel is read, and the reason both public
    /// asks can be exhaustive matches over [`Answered`] instead of a blanket
    /// `Err(_) => Gone`.
    ///
    /// # What is still collapsed here, and why that is now the whole of it
    ///
    /// The sender is dropped *without sending* only when the loop is not there
    /// to send: [`abort`](Self::abort), a runtime shutdown, or a panicking
    /// `Drop` — in the state, or in a reply value dropped because its caller
    /// went away. This used to read "all three mean the same thing to a caller,
    /// which is why they share an answer", and that sentence was doing two jobs:
    /// folding three ways of being gone together, which is right, and folding a
    /// **fault** in with them, which was not, because a caller told `Gone` is
    /// told to stop calling an actor that in fact still runs.
    ///
    /// The second job is finished: a turn that panics is answered
    /// [`Answered::Faulted`] by a loop that survived it. What is left is only
    /// the first, and it is not inference — a dead loop cannot send anything, so
    /// there is no fourth answer it could have given. The one fact this arm
    /// still cannot tell apart is *why* the loop died, and that question has its
    /// own channel: [`ended`](Self::ended).
    async fn answered<T>(
        &self,
        mail: impl FnOnce(Origin, Reply<T>) -> M,
    ) -> Result<Answered<T>, TurnLost>
    where
        T: Send + 'static,
    {
        let (reply, answer) = Reply::channel();
        self.send(Message::Mail(Box::new(mail(Origin::local(), reply))))
            .await
            .map_err(|_| TurnLost::Gone)?;
        // `RecvError` is a unit type: there is exactly one way to lose the
        // channel, so this is total rather than blanket.
        answer.await.map_err(|_gone| TurnLost::Gone)
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
    /// this on itself can deadlock even though it never waits for a reply.
    ///
    /// # Errors
    ///
    /// [`MailboxClosed`] when the actor is gone.
    pub async fn notify<T>(
        &self,
        mail: impl FnOnce(Origin, Reply<T>) -> M,
    ) -> Result<(), MailboxClosed>
    where
        T: Send + 'static,
    {
        let (reply, answer) = Reply::channel();
        drop(answer);
        self.send(Message::Mail(Box::new(mail(Origin::local(), reply))))
            .await
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
    /// [`MailboxClosed`] when the actor is gone.
    ///
    /// [`Handle::with_state`]: crate::LocalActor
    pub async fn read<T, F>(&self, read: F) -> Result<T, MailboxClosed>
    where
        T: Send + 'static,
        F: FnOnce(&A) -> T + Send + 'static,
    {
        let (reply, answer) = oneshot::channel();
        self.send(Message::Read(Box::new(Box::new(move |state| {
            // Caught synchronously rather than through `guarded`, and that is
            // not a shortcut: `guarded` takes a future, an async block holding
            // `&A` is `Send` only when `A: Sync`, and requiring `A: Sync` here
            // would put it on every actor in the crate for a closure that cannot
            // await in the first place.
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read(state)))
                .map_err(Panic::caught);
            let _ = reply.send(caught);
        }))))
        .await?;
        match answer.await.map_err(|_| MailboxClosed)? {
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
    /// [`ask`](Self::ask) waiting on one is told [`MailboxClosed`], and a
    /// [`notify`](Self::notify) is told nothing, because it was already told
    /// `Ok`.
    ///
    /// **An actor must not call this on itself.** It takes a mailbox permit like
    /// any message and then waits for a loop that cannot exit while it is inside
    /// this turn.
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
    /// [`System::shutdown`](crate::System::shutdown) takes a budget because the
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
    pub async fn stop(&self) {
        let _ = self.send(Message::Stop).await;
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
    /// dropped turn do get [`MailboxClosed`]: dropping the receiver drains the
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
    /// break of the settle here could be made red — `an_aborted_actor_tells_a_peer_no_such_actor`
    /// stays green with the whole line removed, because the floor is strictly
    /// earlier in the same drop. A line that cannot be made red is not a guard,
    /// it is a second writer nobody can hold to account, so it went.
    /// [`System::abort`](crate::System::abort) and `drain` keep theirs, and their
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
    /// every call reports [`MailboxClosed`] on its own rather than asking a
    /// caller to check first.
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
    /// `is_busy` and `is_running` both `true`. Await [`ended`](Self::ended) for
    /// the point where both have settled.
    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed)
    }

    /// How many messages are waiting ahead of the one being handled.
    ///
    /// Zero or one at the local mailbox depth, which is a constant and not a
    /// knob — never a number to compute a queue-length alert from. What it is
    /// good for is the question it answers exactly — **is this actor's mailbox
    /// full, and therefore will the next caller block on the permit.**
    ///
    /// `ActorStatus::buffered` answers that same question on the keyed side, and
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
    /// Paired with [`is_busy`](Self::is_busy) it separates three states rather
    /// than two: idle, in a turn with the slot free, and in a turn with the next
    /// caller already about to block.
    #[must_use]
    pub fn buffered(&self) -> u64 {
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

    /// The one place a message enters the mailbox.
    ///
    /// It used to be the one place the box was put on, too. The box moved into
    /// [`Message::Mail`], where it stays until [`dispatch`] takes it off — because a
    /// box that the loop unwraps buys nothing, and that is what
    /// `tests/actor_memory.rs` measured.
    async fn send(&self, message: Message<A, M>) -> Result<(), MailboxClosed> {
        self.mail.send(message).await.map_err(|_| MailboxClosed)
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

// The four prefixes a `RemoteError::Failed` carries out of `rendered` and
// `lost`. They live on `RemoteError` itself now, in `reply.rs`, because a peer
// has to match on them and could not name them: this block held them as private
// `const`s for four waves while every caller hard-coded the literal.
//
// Aliased here rather than qualified at eight use sites, because the use sites
// are `format!("{PANICKED}{panic}")` and the qualified spelling inside a format
// string is noise. `reply.rs` carries the whole argument — the property, the
// guard, and what the claim is a property *of*, which is narrower than this
// block used to say.
use crate::RemoteError as Failure;
const PANICKED: &str = Failure::PANICKED;
const REFUSED: &str = Failure::REFUSED;
const RESTARTING: &str = Failure::RESTARTING;
const DROPPED: &str = Failure::DROPPED;

/// Render one answer as the pair a served dispatch returns.
///
/// **Two dispatch paths reach this and they used to disagree.** The keyed loop
/// rendered a panic as `Panic`'s own `Display` and the singleton's dispatch
/// wrapped it in a second prefix, so one peer read `the actor panicked: bad
/// message` and the other `panicked: the actor panicked: bad message` for the
/// same fault — and there was no keyed twin of the test that pins the prefix, so
/// both stayed green. One function now, and the two paths cannot drift.
///
/// The prefixes are load-bearing rather than decorative. `RemoteError::Failed`
/// carries a `String` and not a variant, because a variant sits inside the
/// hashed contract and would move every service's ALPN — so the **string is the
/// whole discriminator** between "your message exploded" and "an observer vetoed
/// the turn it just watched", and panic text is usually caller-derived.
///
/// There are two more now — [`RESTARTING`] and [`DROPPED`], both built in
/// [`deliver_to_local`] on the arm that answers a reply channel that died
/// without sending. The four are declared together above, with the one property
/// they share and the test that holds it: none of them is a prefix of another,
/// and none is a [`RemoteError`] variant, so the contract hash does not move for
/// any of them.
///
/// The stutter is still there and is deliberately not fixed here:
/// `Panic::caught` builds its message as `the actor panicked: {payload}`, and
/// that same string is what [`Ended::Panicked`] and
/// [`TurnOutcome::Panicked`](crate::TurnOutcome) carry. Removing one of the two
/// prefixes is a decision about `panic.rs`'s vocabulary, and it belongs where
/// `Panic` is rather than inside a dispatch.
pub(crate) fn rendered<Resp>(
    answered: Answered<(Resp, Body)>,
    millis: u64,
) -> (Outcome<Resp>, Body) {
    match answered {
        Answered::Ran((response, body)) => (Outcome::Ok(response), body),
        // A panic must not unwind out of a connection's task, so the remote
        // caller is told a value. The actor is still alive.
        Answered::Faulted(panic) => (
            Outcome::Err(RemoteError::Failed(format!("{PANICKED}{panic}"))),
            Body::empty(),
        ),
        // NOT `NoSuchActor`, which says "there is nothing here, stop asking".
        // The turn ran and was not committed, and the actor is ending for a
        // reason the peer should be able to read.
        Answered::Refused { name } => (
            Outcome::Err(RemoteError::Failed(format!("{REFUSED}{name}"))),
            Body::empty(),
        ),
        // Queued and never started. The value the other bound produces for the
        // same condition, so a peer cannot tell the two apart by the refusal.
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
/// - [`Standing::Restarting`] — **retry.** The incarnation that owed the answer
///   is gone and another is coming; the string carries the ending that caused it.
/// - [`Standing::Running`] — **do not retry blindly, and do not give up on the
///   actor.** It is there, and this one reply was lost.
/// - [`Standing::Gone`] — **stop asking.**
///
/// # What it is about, which is not what a peer might read into it
///
/// It answers *what is true of this actor now*, not *what killed your reply*. The
/// reader is woken by the death and reads afterwards, so the two coincide except
/// in one direction: an actor may have moved on between the death and the read.
/// A live actor that dropped a reply and then ended reads `Gone`, and *stop
/// asking* is the right instruction for it. A restart whose next incarnation is
/// already running reads `Running`, and *the actor is there* is the right one for
/// that. [`deliver_to_local`] and `deliver_to_actor` both state the window they
/// cannot close; neither of them can produce a sentence that is false at the
/// moment it is written.
///
/// `id` is the singleton's service name or the keyed actor's own id — the two
/// spellings of "which actor", which is the only thing the paths still differ by.
pub(crate) fn lost(standing: Standing, id: &str) -> RemoteError {
    match standing {
        Standing::Restarting(reason) => RemoteError::Failed(format!("{RESTARTING}{reason}")),
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
        Standing::Running => RemoteError::Failed(format!(
            "{DROPPED}the actor is serving and this reply was never answered"
        )),
        // NOT a `Failed`: this is the one of the three where *there is nothing
        // here, stop asking* is true.
        Standing::Gone => RemoteError::NoSuchActor { id: id.to_owned() },
    }
}

/// Answer one peer's request out of this process's own actor, and translate what
/// came back into what goes on the wire.
///
/// The twin of [`deliver_to_actor`](crate::deliver_to_actor), for a service
/// served as **one** actor through `Handle::service` rather than as a registry
/// of keyed ones. The two share [`rendered`] and differ only in how they find
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
#[doc(hidden)]
pub async fn deliver_to_local<A, M, Resp>(
    actor: &LocalActor<A, M>,
    cx: Context,
    service: &'static str,
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
    let queueing = actor.send(Message::Mail(Box::new(mail(Origin::peer(cx), reply))));
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

    // The id is the service name because a singleton has no other one — and
    // after `stop()` the ALPN stays registered, so this is answered for as long
    // as the server runs.
    let gone = || {
        (
            Outcome::Err(RemoteError::NoSuchActor {
                id: service.to_owned(),
            }),
            Body::empty(),
        )
    };
    if sent.is_err() {
        return gone();
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
        Err(_) => (
            Outcome::Err(lost(actor.fate.standing(), service)),
            Body::empty(),
        ),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::time::Duration;

    use super::*;

    /// The one property the four served-path prefixes share.
    ///
    /// A peer reads `RemoteError::Failed`'s text to tell "your message exploded"
    /// from "the actor is restarting" from "this reply was dropped", because the
    /// alternative — a variant per cause — sits inside the hashed contract and
    /// moves every service's ALPN. A prefix that is a prefix of another makes
    /// `starts_with` ambiguous and the discriminator stops discriminating.
    ///
    /// Written into four rustdocs before it was written into a test. The list is
    /// spelled out here, so **a fifth prefix has to be added to it by hand**; it
    /// names the consts rather than the strings, so it cannot drift from the wire.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/local.rs`, giving [`DROPPED`]
    /// the value `"panicked: dropped: "`. Watched:
    /// `"panicked: " is a prefix of "panicked: dropped: "`.
    #[test]
    fn no_failure_prefix_is_a_prefix_of_another() {
        let all = [PANICKED, REFUSED, RESTARTING, DROPPED];
        for (i, outer) in all.iter().enumerate() {
            for (j, inner) in all.iter().enumerate() {
                assert!(
                    i == j || !inner.starts_with(outer),
                    "{outer:?} is a prefix of {inner:?}",
                );
            }
        }
    }

    /// One of [`lost`]'s three answers is not a [`RemoteError::Failed`] at all,
    /// so it carries no prefix and no `starts_with` will ever find it.
    ///
    /// **Renamed, because the old name promised a property nothing here checks.**
    /// It was `the_four_are_only_what_rendered_and_lost_build` — a set equality,
    /// against a body that asserts one arm. The set half lives in
    /// `remote::no_other_served_failure_begins_with_one_of_the_four`, which
    /// renders the other producers; this one holds the smaller fact that makes
    /// the count come out: three answers, two prefixes.
    ///
    /// It matters to a peer rather than to a reader. A client that matches only
    /// on prefixes never sees *gone*, which is the one answer that means **stop
    /// asking** — so `NoSuchActor` staying a variant is what keeps it impossible
    /// to miss.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/local.rs` on `lost`'s
    /// `Standing::Gone` arm → `RemoteError::Failed(format!("{DROPPED}{id}"))`.
    /// Watched: `left: Failed("dropped: id")`, `right: NoSuchActor { id: "id" }`.
    ///
    /// **This was the third guard shipped with a promissory note instead of an
    /// output**, after the two in `tests/supervision.rs`. It is worth naming as a
    /// pattern rather than a slip: writing the record before running the break
    /// leaves a sentence that reads like evidence, and the two earlier ones were
    /// only caught because a reviewer grepped for the placeholder. The replay
    /// script's premise — every break carries the substring its guard's doc
    /// records — was false for this one, and its expected string came from a run
    /// rather than from here.
    #[test]
    fn the_gone_answer_is_not_a_failed_and_carries_no_prefix() {
        assert_eq!(
            lost(Standing::Gone, "id"),
            RemoteError::NoSuchActor {
                id: "id".to_string()
            },
            "one of the three answers is not a `Failed` at all, so the set of \
             prefixes is smaller than the set of answers"
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
        /// turn begin. A test about `is_busy` cannot borrow that: it would be
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
        stopped.stop().await;
        let lost = spare
            .ask_catching(add(1))
            .await
            .expect_err("a stopped actor has no value");
        assert!(
            matches!(lost, TurnLost::Gone),
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
            matches!(lost, TurnLost::Gone),
            "an aborted turn said {lost:?}"
        );
    }

    /// `stop` is ordered: what was sent before it is handled.
    #[tokio::test]
    async fn stop_drains_what_was_sent_before_it() {
        let actor = LocalActor::spawn(Counter::default());
        assert_eq!(actor.ask(add(1)).await.expect("running"), 1);
        actor.stop().await;
        assert!(!actor.is_running());
    }

    #[tokio::test]
    async fn a_handle_to_a_stopped_actor_reports_a_closed_mailbox() {
        let actor = LocalActor::spawn(Counter::default());
        let spare = actor.clone();
        actor.stop().await;

        assert_eq!(spare.ask(add(1)).await, Err(MailboxClosed));
        assert_eq!(spare.notify(add(1)).await, Err(MailboxClosed));
    }

    /// Spawn an actor already inside a turn, and hand back the two ends that
    /// order everything around it: a handle, and the sender that lets the turn go.
    ///
    /// Returns once the turn has **provably started** — `entered` resolving is the
    /// happens-after edge, so no assertion below it is racing the dispatch.
    async fn held() -> (
        LocalActor<Counter, Mail>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<usize, MailboxClosed>>,
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
        tokio::task::JoinHandle<Result<usize, MailboxClosed>>,
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

    /// The two halves of `is_busy`, in one test and in this order, because
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
        assert!(actor.is_busy(), "a running turn must report busy");

        release.send(()).expect("the turn is still waiting");
        turning
            .await
            .expect("no panic")
            .expect("the actor answered");

        // Negative: observed from **inside** a read, which runs on the actor's own
        // loop. Sampling from another task would be a timing test; this is ordered
        // by construction. `is_busy` is an atomic load, not a message, so it
        // cannot queue behind the read it runs inside.
        let seen = {
            let actor = actor.clone();
            actor
                .clone()
                .read(move |_state| actor.is_busy())
                .await
                .expect("the actor is running")
        };
        assert!(!seen, "a read is not a turn and must not report busy");
    }

    /// The question `buffered` answers exactly: will the next caller block.
    ///
    /// **Watched break, `LocalActor::buffered`, `max_capacity()` →
    /// `capacity()`:** `a_full_mailbox_is_visible_before_a_caller_blocks_on_it ...
    /// FAILED`, with `left: 0, right: 1`.
    #[tokio::test]
    async fn a_full_mailbox_is_visible_before_a_caller_blocks_on_it() {
        let (actor, release, turning) = held().await;

        // The turn holds the loop, so nothing drains. An empty slot first.
        assert_eq!(actor.buffered(), 0, "nothing is queued yet");

        // `notify` returns when the message has a permit, so this is ordered.
        actor.notify(add(1)).await.expect("the actor is running");
        assert_eq!(
            actor.buffered(),
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
    /// reach the actor through `spawn_fed`. `is_busy` would have answered `false`
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
            actor.is_busy(),
            "a supervised actor's handle must see its turn"
        );

        release.send(()).expect("the turn is still waiting");
        turning
            .await
            .expect("no panic")
            .expect("the actor answered");
        assert!(!actor.is_busy(), "and must see it end");
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
        assert!(actor.is_busy(), "the turn is running");

        actor.abort();
        assert!(
            matches!(actor.ended().await, Ended::Vanished),
            "an aborted actor vanishes"
        );

        assert!(!actor.is_busy(), "an aborted actor is gone, not busy");
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
            Err(MailboxClosed),
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

    /// And the answer when nobody ever answers: **`TurnLost::Gone`, not a hang.**
    ///
    /// Here rather than only in `tests/defer.rs` because this is where the variant
    /// is visible — a generated `Handle` folds every loss into `MailboxClosed`, so
    /// the criterion's own words can only be asserted from inside the crate.
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
        actor.stop().await;

        let lost = n0_future::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("a parked reply must not outlive its actor as a hang")
            .expect("the caller is not itself panicking")
            .expect_err("nothing answered it");
        assert!(
            matches!(lost, TurnLost::Gone),
            "a reply parked in a state that was dropped said {lost:?}"
        );
    }

    /// An observer that refuses one named method and accepts everything else.
    ///
    /// One method rather than all of them because a refusal **ends the actor**: an
    /// observer that vetoed the `Park` turn too would leave no actor for a later
    /// turn to release anything from, and the test would pass on `Gone` for the
    /// wrong reason. That is not hypothetical — it is what the first version of
    /// this test did, and `Err(Gone)` is what it printed.
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

    /// **The commit point for a released reply, at the variant a `Handle` folds
    /// away.** A turn that answers a parked caller and is then refused by an
    /// observer must not let that answer cross — and the parked caller is told the
    /// same thing the turn's own caller is, naming the same observer.
    ///
    /// This is where the `#[defer]` durability window closed. Before it,
    /// `Deferred::send` wrote to the channel from inside the handler, so the parked
    /// caller had `99` before any observer had seen anything, and a `Persist` that
    /// then refused left it holding an answer for state that was never stored.
    ///
    /// Here rather than only in `tests/persist.rs` because
    /// [`ask`](LocalActor::ask) collapses every loss to `MailboxClosed`: **the
    /// difference between "refused" and "gone" is only visible from inside the
    /// crate**, and the difference is the whole finding — a caller told `Gone`
    /// stops calling, and a caller told `Refused` knows the turn ran and was not
    /// committed.
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
            matches!(releasing, Err(TurnLost::Refused { name: "vetoing" })),
            "the releasing turn's own caller is refused: got {releasing:?}"
        );

        let lost = n0_future::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("a released reply must not hang its caller")
            .expect("the caller is not itself panicking");
        assert!(
            matches!(lost, Err(TurnLost::Refused { name: "vetoing" })),
            "a released reply crossed a refused turn: {lost:?}"
        );
    }

    /// And the panicking half, which cannot be told the same thing.
    ///
    /// A turn sends a parked value and then panics. The value must not cross — a
    /// durability adapter refuses a panicked turn, so nothing behind that answer
    /// was committed — and it cannot be answered `Faulted` either: there is one
    /// `Panic`, it is the original payload rather than a message, and it belongs
    /// to the turn's own caller. So the channel is dropped, which is `Gone`, and
    /// that is already what `Deferred` documents for a fault that reaches a parked
    /// reply.
    ///
    /// The two assertions are the two halves: the turn's own caller gets the panic
    /// (so the payload really did go somewhere), and the parked caller gets `Gone`
    /// (so it did not go to two places, and no uncommitted value crossed).
    ///
    /// Made red by replacing `parking.rs`'s `Verdict::Faulted => drop(reply)` arm
    /// in `Deferred::send` with `Verdict::Faulted => reply.answer(Answered::Ran(value))`.
    /// Watched: `a value sent by a turn that then panicked crossed anyway:
    /// Ok(99)`.
    #[tokio::test]
    async fn a_released_reply_is_gone_when_the_turn_that_released_it_panicked() {
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
            matches!(lost, Err(TurnLost::Gone)),
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
    /// Made red by `scripts/break-once.sh keylink/src/local.rs` on `Lent::ending`'s
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
    /// verdict a served dispatch reads, and — on the arm where there will be no
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
    /// - `scripts/break-once.sh keylink/src/local.rs`, replacing `run_loop`'s
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
            mail.send(Message::Mail(Box::new(Fatal)))
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
    /// could redden `an_aborted_actor_tells_a_peer_no_such_actor`, which aborts an
    /// actor sitting *inside* `run_loop`, where the floor genuinely is earlier.
    /// The line was not unguardable — it was unguarded, and this is the guard it
    /// never had. The remedy is structural rather than the line coming back:
    /// `supervise`'s floor stays armed across the backoff and the factory.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/local.rs`, disarming
    /// `supervise`'s floor at its declaration. Watched: `an actor destroyed
    /// between incarnations is gone, not restarting: Restarting(Panicked {
    /// message: "this incarnation is over" })`.
    #[tokio::test]
    async fn a_supervisor_between_incarnations_is_gone_when_it_is_destroyed() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let open = Arc::new(tokio::sync::Notify::new());
        let (fate, mail, running) = between_incarnations(&calls, Arc::clone(&open), false);

        mail.send(Message::Mail(Box::new(Fatal)))
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
    /// accepts a late read that renders `dropped: `, which is true when it is
    /// written, and refuses the generation counter for producing a sentence that
    /// is not. This was on the wrong side of that line.
    ///
    /// The cell is read directly rather than through a peer, because what is
    /// wrong here is the cell itself and no reply has to die to prove it.
    ///
    /// Made red by `scripts/break-once.sh keylink/src/local.rs`, moving
    /// `supervise`'s floor back inside the loop body — the shape this replaces,
    /// verbatim. Watched: `a supervisor that is still rebuilding must not read as
    /// gone: Gone`.
    #[tokio::test]
    async fn a_transient_factory_failure_does_not_overwrite_a_pending_restart() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let open = Arc::new(tokio::sync::Notify::new());
        let (fate, mail, running) = between_incarnations(&calls, Arc::clone(&open), true);

        mail.send(Message::Mail(Box::new(Fatal)))
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
    /// Made red by `scripts/break-once.sh keylink/src/local.rs`, adding the third
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

        mail.send(Message::Mail(Box::new(Fatal)))
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
