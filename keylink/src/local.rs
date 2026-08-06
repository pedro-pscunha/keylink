//! An actor that never leaves the process.
//!
//! This is the base case. RFC-04 §5.4 states the reason plainly:
//!
//! > An actor framework whose actors must cross a network to be called is not an
//! > actor framework.
//!
//! [`LocalActor`] owns one value on one task and hands it `&mut self`, one turn
//! at a time. No registry, no `ActorId`, no schema hash, no serialization, no
//! QUIC. A local message is a boxed closure and a `oneshot`.
//!
//! Generated code wraps this in a typed `Handle`, whose methods are the ones
//! written in `service!`. The plumbing is here rather than in the macro so that
//! the guard, the reply channel and the panic policy exist **once**, where they
//! cannot be forgotten by a kind added later.
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
//! [`guarded`], including the closure that produces the turn's future. What is
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

use crate::{guarded, Ended, Observer, Panic, TurnInfo, TurnLost, TurnOutcome};

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
/// parked sender still holds its boxed closure and its arguments alive, so cost
/// grows with waiting callers whatever this number is — the queue moves into the
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

/// One turn against an actor's state.
///
/// Boxed rather than typed as an enum of requests, which buys three things the
/// enum does not: no `Response` variant to extract (so no "wrong variant"
/// failure that cannot happen), no unreachable arm for a method kind the local
/// handle does not serve, and no `Box<dyn Any>` downcast. The generated method
/// captures its own arguments and its own reply channel, both concretely typed.
/// The second parameter is the actor's observers, lent for the length of one
/// turn. The alias is private, and `deliver` only wraps the closure, so nothing
/// public moved with it.
type Turn<A> = Box<
    dyn for<'a> FnOnce(
            &'a mut A,
            &'a mut [Box<dyn Observer<A>>],
        ) -> Pin<Box<dyn Future<Output = Option<Ended>> + Send + 'a>>
        + Send,
>;

/// What the loop puts on the reply channel.
///
/// Three states rather than two, because "the budget expired while it queued"
/// and "an observer refused the turn that ran" are opposite facts and a caller
/// that has to unwrap two layers to tell them apart will get it wrong. A
/// **dropped** sender is the fourth case and stays `TurnLost::Gone`.
enum Answered<T> {
    /// The turn ran. `Err` is a panic the actor survived.
    Ran(Result<T, Panic>),
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
    F: for<'b> FnOnce(&'b mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'b>> + Send + 'static,
{
    let answer = guarded(async { turn(state).await }).await;

    // Copied out BEFORE `reply.send(answer)` moves the `Panic`. Building the
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

    match (refused, panicked) {
        // Completed and accepted: the value.
        (None, _) => (Answered::Ran(answer), None),
        // Panicked and refused: the panic is STILL answered, and then the actor
        // ends. `Err(Panic)` is not `Ok`, so the argument for withholding a
        // reply never applied to it — and withholding here would reverse what
        // this module's header promises about a panicking call.
        (Some(_), Some(message)) => (Answered::Ran(answer), Some(Ended::Panicked { message })),
        // Completed and refused: the turn ran and was not committed, so the
        // caller is told it has no answer rather than given an uncommitted one.
        (Some((name, error)), None) => (
            Answered::Refused { name },
            Some(Ended::Observer { name, error }),
        ),
    }
}

/// A turn, or the request to stop after the turns already queued.
enum Message<A> {
    Turn(Turn<A>),
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

/// What became of a turn that carried a queueing deadline.
///
/// Two states rather than a `Result<Option<..>>`, because the difference between
/// "it ran and panicked" and "it never ran" is the difference between an
/// idempotency key that is settled and one that is still claimable, and a
/// caller that has to unwrap two layers to find that out will get it wrong.
#[derive(Debug)]
pub enum Queued<T> {
    /// The turn ran. `Err` is a panic the actor survived.
    Ran(Result<T, Panic>),
    /// The budget expired while the message was still waiting. **Nothing ran**,
    /// so nothing was mutated and nothing is owed.
    Expired,
}

/// One value, owned by one task, addressed by `&mut self`.
///
/// Cheap to clone: every clone names the same actor. The actor runs until the
/// last handle is dropped or [`Self::stop`] is called.
pub struct LocalActor<A> {
    mail: mpsc::Sender<Message<A>>,
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

/// Everything that carries a turn needs `A: Send`, and the reason is exact: a
/// turn's future holds `&mut A` across an await, and `&mut A` is `Send` only
/// when `A` is. The block below it needs nothing of `A` at all.
impl<A: Send + 'static> LocalActor<A> {
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
        let (mail, mut inbox) = mpsc::channel::<Message<A>>(MAILBOX);
        let (ended, watching) = watch::channel::<Option<Ended>>(None);
        let task = n0_future::task::spawn(async move {
            let mut reason = Ended::Stopped;
            while let Some(message) = inbox.recv().await {
                match message {
                    Message::Turn(turn) => {
                        if let Some(end) = turn(&mut state, &mut hooks).await {
                            reason = end;
                            break;
                        }
                    }
                    Message::Stop => break,
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

    /// Run one turn and bring back its value, resuming a panic in the caller.
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
    pub async fn ask<T, F>(&self, turn: F) -> Result<T, MailboxClosed>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'a>>
            + Send
            + 'static,
    {
        // Both losses collapse to `MailboxClosed` here, on purpose: a local
        // caller learns WHY from [`ended`](Self::ended), and widening this
        // error would make every existing call site match on a case it has
        // nothing to do about.
        match self.ask_catching(turn).await.map_err(|_| MailboxClosed)? {
            Ok(value) => Ok(value),
            Err(panic) => panic.resume(),
        }
    }

    /// Read the state between turns, running **no** observers.
    ///
    /// [`Handle::with_state`] is this. A read takes `&A` and cannot mutate, so
    /// storing after it is waste — and it also closes a loop a future background
    /// writer would otherwise create, where a writer that read the state through
    /// a turn would mark the actor dirty and schedule another read, forever.
    ///
    /// The message type does not change; only the closure does.
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
        self.deliver(Box::new(move |state, _hooks| {
            Box::pin(async move {
                // Caught synchronously rather than through `guarded`, and that
                // is not a shortcut: `guarded` takes a future, an async block
                // holding `&A` is `Send` only when `A: Sync`, and requiring
                // `A: Sync` here would put it on every actor in the crate for a
                // closure that cannot await in the first place.
                let caught =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| read(&*state)))
                        .map_err(Panic::caught);
                let _ = reply.send(caught);
                None
            })
        }))
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

    /// Run one turn and hand back a panic instead of resuming it.
    ///
    /// What a *served* actor uses. A panic that unwound here would travel out of
    /// a connection's task, so the caller across the network gets
    /// [`RemoteError::Failed`](crate::RemoteError::Failed) instead — which is
    /// the same policy the keyed actor system applies, in the same words.
    ///
    /// # Errors
    ///
    /// [`MailboxClosed`] when the actor is gone. A panic is the inner `Err`, not
    /// this one: the actor survived it and is still running.
    pub async fn ask_catching<T, F>(&self, turn: F) -> Result<Result<T, Panic>, TurnLost>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'a>>
            + Send
            + 'static,
    {
        let (reply, answer) = oneshot::channel();
        self.deliver(Box::new(move |state, hooks| {
            Box::pin(async move {
                // `turn(state)` is inside the guard as well as the future it
                // returns, so a closure that panics before it yields one costs
                // the message rather than the actor. The observers are inside
                // the same guard; `observed_turn` owns both halves.
                let (answered, end) = observed_turn(state, hooks, turn).await;
                let _ = reply.send(answered);
                end
            })
        }))
        .await
        .map_err(|_| TurnLost::Gone)?;
        // The sender is dropped without sending when the task died mid-turn:
        // `abort`, a runtime shutdown, or a panicking `Drop` in the state. All
        // three mean the same thing to a caller, which is why they share an
        // answer rather than being told apart.
        match answer.await {
            Err(_) => Err(TurnLost::Gone),
            Ok(Answered::Ran(value)) => Ok(value),
            Ok(Answered::Refused { name }) => Err(TurnLost::Refused { name }),
            // Unreachable without a deadline, and typed rather than asserted.
            Ok(Answered::Expired) => Err(TurnLost::Gone),
        }
    }

    /// Run one turn, refusing it if it is still queued when `deadline` passes.
    ///
    /// What [`QueueTimeout`](crate::QueueTimeout) is built on, and the reason it
    /// can promise something [`Timeout`](crate::Timeout) cannot: the deadline is
    /// checked in two places, and the second is **on the actor's own task,
    /// immediately before the turn begins**. So a refusal here is always a
    /// refusal of work that never started.
    ///
    /// - Waiting for a mailbox permit is bounded. Giving up there means the
    ///   message was never queued at all.
    /// - Having been queued, the deadline is re-read by the closure the actor
    ///   picks up. Past it, the turn is skipped.
    ///
    /// There is no third place to check, and there does not need to be: once
    /// the turn starts, nothing outside can stop it without dropping `&mut A`
    /// mid-mutation, which is the one thing an actor must never do.
    ///
    /// `None` for the deadline is [`ask_catching`](Self::ask_catching).
    ///
    /// # Errors
    ///
    /// [`MailboxClosed`] when the actor is gone. A panic is inside
    /// [`Queued::Ran`], not here: the actor survived it.
    pub async fn ask_catching_before<T, F>(
        &self,
        deadline: Option<Instant>,
        turn: F,
    ) -> Result<Queued<T>, TurnLost>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'a>>
            + Send
            + 'static,
    {
        let Some(deadline) = deadline else {
            return self.ask_catching(turn).await.map(Queued::Ran);
        };

        let (reply, answer) = oneshot::channel();
        let queued = n0_future::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            self.deliver(Box::new(move |state, hooks| {
                Box::pin(async move {
                    // On the ACTOR's task. Nothing has touched the state yet,
                    // so refusing here costs the message and not a half-applied
                    // mutation — and it runs no observers, because nothing ran.
                    if Instant::now() >= deadline {
                        let _ = reply.send(Answered::Expired);
                        return None;
                    }
                    let (answered, end) = observed_turn(state, hooks, turn).await;
                    let _ = reply.send(answered);
                    end
                })
            })),
        )
        .await;

        match queued {
            // The permit never arrived. Dropping the `send` future is what
            // makes this honest: tokio does not queue a message whose send was
            // cancelled, so nothing is in the mailbox to run later.
            Err(_elapsed) => Ok(Queued::Expired),
            Ok(Err(_closed)) => Err(TurnLost::Gone),
            Ok(Ok(())) => match answer.await {
                Err(_) => Err(TurnLost::Gone),
                Ok(Answered::Expired) => Ok(Queued::Expired),
                Ok(Answered::Ran(ran)) => Ok(Queued::Ran(ran)),
                Ok(Answered::Refused { name }) => Err(TurnLost::Refused { name }),
            },
        }
    }

    /// Queue one turn and do not wait for it.
    ///
    /// What a local `tell` uses. The `Ok` means the message is in the mailbox
    /// and nothing more: it has not run, it may panic, it may be dropped unrun
    /// by [`stop`](Self::stop) or [`abort`](Self::abort), and there is no
    /// channel on which to learn any of that. A panic is caught, so the actor
    /// survives, and then dropped, because there is nobody left to tell.
    ///
    /// **It still waits for a mailbox permit**, which is why an actor calling
    /// this on itself can deadlock even though it never waits for a reply.
    ///
    /// # Errors
    ///
    /// [`MailboxClosed`] when the actor is gone.
    pub async fn notify<T, F>(&self, turn: F) -> Result<(), MailboxClosed>
    where
        // `Send` was not required before observers existed. A `tell` is a turn,
        // so it is observed like one, and the handler's value is held across
        // the observer awaits. Free in practice: a declared return type is a
        // wire type, which is already `Send`.
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'a>>
            + Send
            + 'static,
    {
        self.deliver(Box::new(move |state, hooks| {
            Box::pin(async move {
                // A `tell` is a turn, so it is observed like one. Nobody is
                // waiting, so the answer is dropped and only the ending — if an
                // observer refused — travels back to the loop.
                let (answered, end) = observed_turn::<A, T, F>(state, hooks, turn).await;
                drop(answered);
                end
            })
        }))
        .await
    }
}

impl<A> LocalActor<A> {
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
        let _ = self.mail.send(Message::Stop).await;
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

    async fn deliver(&self, turn: Turn<A>) -> Result<(), MailboxClosed> {
        self.mail
            .send(Message::Turn(turn))
            .await
            .map_err(|_| MailboxClosed)
    }
}

/// By hand, because `#[derive(Clone)]` would demand `A: Clone` — and `A` is the
/// state, which is never cloned and usually cannot be.
impl<A> Clone for LocalActor<A> {
    fn clone(&self) -> Self {
        Self {
            mail: self.mail.clone(),
            ended: self.ended.clone(),
            task: self.task.clone(),
        }
    }
}

impl<A> fmt::Debug for LocalActor<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalActor")
            .field("running", &!self.mail.is_closed())
            .finish()
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

    fn add(
        n: u64,
    ) -> impl for<'a> FnOnce(&'a mut Counter) -> Pin<Box<dyn Future<Output = usize> + Send + 'a>>
           + Send
           + 'static {
        move |state: &mut Counter| {
            Box::pin(async move {
                state.seen.push(n);
                state.seen.len()
            })
        }
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
                    .ask(move |state: &mut Counter| {
                        Box::pin(async move {
                            let alone = !state.inside;
                            state.inside = true;
                            n0_future::time::sleep(Duration::from_millis(5)).await;
                            state.inside = false;
                            alone
                        })
                    })
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
                    .ask(|_state: &mut Counter| Box::pin(async move { panic!("bad message") }))
                    .await
            })
        };
        let failure = boom.await.expect_err("the panic reaches the caller");
        assert!(failure.is_panic(), "got {failure:?}");

        // And the very next message runs against the state as it stands.
        assert_eq!(actor.ask(add(7)).await.expect("still running"), 1);
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
                    .ask(|_state: &mut Counter| {
                        Box::pin(async move {
                            std::future::pending::<()>().await;
                            0usize
                        })
                    })
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
        let actor = LocalActor::spawn(Counter::default());
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
