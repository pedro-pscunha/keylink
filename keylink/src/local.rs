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
use std::pin::Pin;
use std::sync::Arc;

use n0_future::task::JoinHandle;
use n0_future::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};

// By module path and not from the crate root: Wave 2's review found `guarded` and
// `catching` re-exported `pub` with no consumer outside this crate, so they are
// `pub(crate)` now and the root no longer names them.
use crate::panic::{catching, guarded};
use crate::{
    Body, Context, Ended, Observer, Origin, Outcome, Panic, Queueing, RemoteError, TurnInfo,
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
const MAILBOX: usize = 1;

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
/// Opaque on purpose. A generated mailbox variant carries one of these for its
/// own method's return type, and generated code does exactly two things with it:
/// puts it in a variant, and hands it to [`turn`]. It never builds one — keylink
/// makes the pair — and it never reads one, so [`Answered`] stays `pub(crate)`
/// and its fourth state was additive to a type nothing outside can match on.
/// That prediction was tested by [`Answered::Faulted`] landing: not one
/// generated line moved.
///
/// Contrast the alias this replaces: `Answer<Resp>` was a transparent
/// `oneshot::Sender<..>`, so tokio's channel type was in keylink's public API
/// through a `#[doc(hidden)]` name. A newtype costs one field and buys the
/// promise back.
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
#[doc(hidden)]
pub struct Reply<T>(oneshot::Sender<Answered<T>>);

impl<T> Reply<T> {
    /// The pair. `pub(crate)`: keylink builds every reply channel, on the two
    /// paths that own the other half.
    pub(crate) fn channel() -> (Self, oneshot::Receiver<Answered<T>>) {
        let (reply, answer) = oneshot::channel();
        (Self(reply), answer)
    }

    /// Answer, if anybody is still listening.
    ///
    /// A dropped receiver is not an error here and never was: a local `tell`
    /// drops it deliberately, and a caller that gave up on its future is gone.
    /// Both mean the same thing to a turn that has already run.
    fn answer(self, answered: Answered<T>) {
        let _ = self.0.send(answered);
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

/// One turn, its observers, and who is answered — RFC-06 §2.6 in one place, so
/// the two catching paths cannot drift apart.
///
/// The observer chain runs **inside** the same [`guarded`] as the handler.
/// Outside it, an observer that panicked would unwind out of a detached task and
/// be swallowed, which is the property this module's header states about every
/// path library code controls.
async fn observed_turn<A, T, F>(
    state: &mut A,
    hooks: &mut [Box<dyn Observer<A>>],
    method: &'static str,
    turn: F,
) -> (Answered<T>, Option<Ended>)
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
    let answer = guarded(async { turn(state).await }).await;

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

    let mut refused: Option<(&'static str, Arc<dyn Error + Send + Sync>)> = None;
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
                refused = Some((name, Arc::from(stop.into_reason())));
                break;
            }
            // `Panic` implements `Display` and not `Error`, and is not `Sync`,
            // so it cannot fill an `Arc<dyn Error + Send + Sync>` directly.
            // `to_string()` is what `RemoteError::Failed` already does with one.
            Err(panic) => {
                let reason: Box<dyn Error + Send + Sync> = panic.to_string().into();
                refused = Some((name, Arc::from(reason)));
                break;
            }
        }
    }

    // The one place the handler's `Result` becomes a discriminant. Written once
    // and read by two arms below, so a fault cannot be reported as a value on
    // one path and as a fault on the other.
    let answered = match answer {
        Ok(value) => Answered::Ran(value),
        Err(panic) => Answered::Faulted(panic),
    };

    match (refused, panicked) {
        // Completed and accepted: the value. Or faulted and accepted, which is
        // an observer that read `TurnOutcome::Panicked` and chose to continue.
        (None, _) => (answered, None),
        // Panicked and refused: the fault is STILL answered, and then the actor
        // ends. `Answered::Faulted` is not a value, so the argument for
        // withholding a reply never applied to it — and withholding here would
        // reverse what this module's header promises about a panicking call.
        (Some(_), Some(message)) => (answered, Some(Ended::Panicked { message })),
        // Completed and refused: the turn ran and was not committed, so the
        // caller is told it has no answer rather than given an uncommitted one.
        (Some((name, error)), None) => (
            Answered::Refused { name },
            Some(Ended::Observer { name, error }),
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
/// `dispatched` before the handler, `completed` only on an answer an observer
/// let through, `unrun` on a budget that expired. Marking `completed` where the
/// method returns would settle an idempotency key as an observer's veto and
/// replay that veto to every retry, for work whose effect was thrown away —
/// RFC-06 §13.3. That reasoning was written twice, in two dispatchers, and this
/// is where it is written once.
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
    // Read before the origin moves into the closure. `Progress` shares its
    // `Arc` on clone deliberately, which is what lets a mark made on the actor's
    // task be read back on the connection's.
    let progress = origin.caller().peer().map(Context::progress);
    let expired = origin
        .caller()
        .peer()
        .and_then(Context::queueing)
        .is_some_and(Queueing::expired);

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
    let (answered, end) =
        observed_turn(state, hooks, method, move |state| turn(origin, state)).await;
    // `Ran` and nothing else. A `Faulted` turn is not a completed one, and an
    // `Expired` or `Refused` one is handled above or marked by the observer
    // chain's own refusal — spelling this as a positive match is what keeps a
    // fifth `Answered` state from silently settling a key.
    if let (Some(progress), Answered::Ran(_)) = (&progress, &answered) {
        progress.completed();
    }
    reply.answer(answered);
    end
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
type Read<A> = Box<dyn FnOnce(&A) + Send>;

/// A turn, a read, or the request to stop after the messages already queued.
enum Message<A, M> {
    /// One of the service's own methods, as a value of its generated enum.
    Mail(M),
    /// [`LocalActor::read`]. Runs no observers, by design: a read takes `&A` and
    /// cannot mutate, so storing after it is waste.
    Read(Read<A>),
    /// Ends the loop, which drops the receiver, which is what makes every other
    /// handle report [`MailboxClosed`] from then on.
    Stop,
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
    mail: mpsc::Sender<Box<Message<A, M>>>,
    /// Why the actor ended, once it has. `Option`, starting at `None`, because
    /// a channel initialised with a reason reports that reason for a live
    /// actor. A receiver and never a sender, so awaiting it cannot keep the
    /// actor alive — which is exactly what an earlier design built on
    /// `mail.closed()` did, making "the last handle was dropped" unreachable.
    ended: watch::Receiver<Option<Ended>>,
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
    pub fn spawn_observed(mut state: A, mut hooks: Vec<Box<dyn Observer<A>>>) -> Self {
        // `Box<Message<..>>`, and the box is the point. `tokio::sync::mpsc`
        // allocates its first block of 32 slots eagerly, so a mailbox costs
        // `32 * size_of::<slot>()` whatever its capacity is — and the slot is no
        // longer a pointer-sized boxed closure but the service's own enum, sized
        // to its largest variant. Unboxed, a service with one four-kilobyte
        // request would pay 128 KB per idle actor for a queue that holds one
        // message. `Mailbox` in `registry.rs` boxes for the same reason and
        // carries the measured table; this end had no box at all, because the
        // closure it used to carry made one by accident.
        let (mail, mut inbox) = mpsc::channel::<Box<Message<A, M>>>(MAILBOX);
        let (ended, watching) = watch::channel::<Option<Ended>>(None);
        let task = n0_future::task::spawn(async move {
            let mut reason = Ended::Stopped;
            while let Some(message) = inbox.recv().await {
                match *message {
                    Message::Mail(mail) => {
                        // `Box::pin` and not a bare await, for the reason this
                        // module's header gives: the turn's state machine is
                        // sized by the caller's handler, and an unboxed await
                        // would put that size in the task's own future — where
                        // an actor pays for it while it is idle. Measured:
                        // 3,005 bytes per idle actor without this line against
                        // 1,597 with it, at a service with one `u64` argument.
                        // (1,469 at Wave 1b; the `finish` loop below cost the
                        // other 128 — `tests/actor_memory.rs` bisects it.)
                        if let Some(end) = Box::pin(mail.run(&mut state, &mut hooks)).await {
                            reason = end;
                            break;
                        }
                    }
                    // No observers and no ending: a read cannot mutate, so
                    // there is nothing for an observer to accept or refuse.
                    Message::Read(read) => read(&state),
                    Message::Stop => break,
                }
            }
            // The last thing that happens with the state, and it happens BEFORE
            // the three drops below — so `stop().await` returning still means
            // every claim is free, and a flush that needed the state still had
            // it. RFC-06 §7's debounced store is unbuildable without this: a
            // debounce is only safe with a guaranteed final flush.
            //
            // `reason` is already decided, and is lent rather than remade, so
            // every observer is told what every waiter is about to be told.
            //
            // **Unreachable on `abort` by construction, not by policy.**
            // `abort()` is `task.abort()` and nothing else: the future is
            // dropped at its next await and the loop never gets here. The one
            // hole is on `abort`'s own doc — a turn that never reaches an await
            // is not interrupted, so such an actor can exit the loop normally
            // afterwards and then this does run.
            //
            // TWO catches per observer and no `guarded` around the loop, which
            // is the same decision `observed_turn` made one rung up and it is
            // made here for both of its reasons.
            //
            // A single `guarded` around the whole loop would need a built
            // future, so an `async` block, so a block capturing `&state` across
            // an await — `Send` only when `A: Sync`, which is the bound this
            // design exists to keep out. The leak is not a matter of care: this
            // block is spawned under `A: Send + 'static` and nothing more, so an
            // `A: Sync` requirement introduced here is a compile error in
            // keylink itself rather than a downstream surprise.
            //
            // And it would stop the chain at the first panic. `finish` is a
            // flush: an adapter that fell over must not cost the next adapter
            // its write, and there is no refusal left to short-circuit for —
            // `finish` is infallible and the reason is already fixed.
            for observer in &mut hooks {
                // Shared reborrow first, and the `FnOnce`-forcing rebind
                // inside, for the reasons `observed_turn` spells out.
                let shared: &A = &state;
                // Borrowed OUT here and not inside: `&reason` taken in the
                // closure body would make the closure own the `Ended` and the
                // future borrow from its own capture, which is `E0515` — and
                // cloning it per observer, which is what rustc suggests, would
                // hand each one its own copy of a fact there is only one of.
                let ending: &Ended = &reason;
                let watching: &mut dyn Observer<A> = &mut **observer;
                let built = catching(move || {
                    let watching = watching;
                    watching.finish(shared, ending)
                });
                // A panic in either half is dropped rather than reported. There
                // is no caller to resume it into and no reason left to change:
                // `reason` has already been handed to whichever observers ran
                // before this one, and rewriting it here would make two
                // observers of one ending disagree about it. The panic hook has
                // already run at the fault, which is where the backtrace is.
                if let Ok(future) = built {
                    let _ = guarded(future).await;
                }
            }

            // These three, in this order, and the order IS the fix.
            //
            // `drop(hooks)` releases whatever an observer holds — a file claim,
            // for the first adapter — BEFORE anybody is told the actor ended,
            // so `stop().await` returning means the claim is free.
            //
            // `drop(inbox)` closes the mailbox before the same moment.
            // `is_running()` is `!mail.is_closed()`, which is false only once
            // the RECEIVER drops; without this explicit drop the receiver lives
            // until the async block ends, so `stop()` could return with
            // `is_running()` still true — and `stop_drains_what_was_sent_before_it`
            // asserts the opposite immediately after `stop().await`.
            //
            // **Not currently reproducible, and said so rather than claimed.**
            // RFC-06 §2.4 reports one teardown order failing 25 of 25 and the
            // other passing 25 of 25. Removing this line and running the guard
            // 25 times on this tree failed 0 of 25: the block ends, dropping
            // `inbox`, before the woken `stop()` waiter is polled. So the line
            // is ordering insurance whose absence no test here catches. It
            // stays because which order you get depends on the async block's
            // capture order and nothing pins that — but it is listed among the
            // guards that were NOT watched to fail, because a guard nobody has
            // seen fail is not a guard.
            drop(hooks);
            drop(inbox);
            let _ = ended.send(Some(reason));
        });
        Self {
            mail,
            ended: watching,
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
        self.send(Message::Mail(mail(Origin::local(), reply)))
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
        self.send(Message::Mail(mail(Origin::local(), reply))).await
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
        self.send(Message::Read(Box::new(move |state| {
            // Caught synchronously rather than through `guarded`, and that is
            // not a shortcut: `guarded` takes a future, an async block holding
            // `&A` is `Send` only when `A: Sync`, and requiring `A: Sync` here
            // would put it on every actor in the crate for a closure that cannot
            // await in the first place.
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read(state)))
                .map_err(Panic::caught);
            let _ = reply.send(caught);
        })))
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
    pub fn ended(&self) -> impl Future<Output = Ended> + Send + 'static {
        let mut watching = self.ended.clone();
        async move {
            // `wait_for` and not `changed()`: a fresh receiver marks the current
            // value as seen, so `changed()` would wait forever on a second call.
            // A dropped sender is an aborted task, which never filled the value.
            match watching.wait_for(Option::is_some).await {
                Ok(seen) => seen.clone().unwrap_or(Ended::Vanished),
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

    /// The one place a message enters the mailbox, and the one place the box is
    /// put on.
    async fn send(&self, message: Message<A, M>) -> Result<(), MailboxClosed> {
        self.mail
            .send(Box::new(message))
            .await
            .map_err(|_| MailboxClosed)
    }
}

/// By hand, because `#[derive(Clone)]` would demand `A: Clone` — and `A` is the
/// state, which is never cloned and usually cannot be. `M` is not cloned either:
/// only the sender is.
impl<A, M> Clone for LocalActor<A, M> {
    fn clone(&self) -> Self {
        Self {
            mail: self.mail.clone(),
            ended: self.ended.clone(),
            task: self.task.clone(),
        }
    }
}

impl<A, M> fmt::Debug for LocalActor<A, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalActor")
            .field("running", &!self.mail.is_closed())
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
/// The prefixes are load-bearing rather than decorative. `RemoteError::Failed`
/// carries a `String` and not a variant, because a variant sits inside the
/// hashed contract and would move every service's ALPN — so the **string is the
/// whole discriminator** between "your message exploded" and "an observer vetoed
/// the turn it just watched", and panic text is usually caller-derived.
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
            Outcome::Err(RemoteError::Failed(format!("panicked: {panic}"))),
            Body::empty(),
        ),
        // NOT `NoSuchActor`, which says "there is nothing here, stop asking".
        // The turn ran and was not committed, and the actor is ending for a
        // reason the peer should be able to read.
        Answered::Refused { name } => (
            Outcome::Err(RemoteError::Failed(format!("observer refused: {name}"))),
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
    let queueing = actor.send(Message::Mail(mail(Origin::peer(cx), reply)));
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
        Err(_) => gone(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::time::Duration;

    use super::*;

    /// The state a `Mutex` would otherwise be wrapped around.
    #[derive(Default)]
    struct Counter {
        seen: Vec<u64>,
        inside: bool,
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
}
