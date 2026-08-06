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

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use n0_future::task::JoinHandle;
use n0_future::time::Instant;
use tokio::sync::{mpsc, oneshot};

use crate::{guarded, Panic};

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
type Turn<A> =
    Box<dyn for<'a> FnOnce(&'a mut A) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> + Send>;

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
    pub fn spawn(mut state: A) -> Self {
        let (mail, mut inbox) = mpsc::channel::<Message<A>>(MAILBOX);
        let task = n0_future::task::spawn(async move {
            while let Some(message) = inbox.recv().await {
                match message {
                    Message::Turn(turn) => turn(&mut state).await,
                    Message::Stop => break,
                }
            }
        });
        Self {
            mail,
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
        match self.ask_catching(turn).await? {
            Ok(value) => Ok(value),
            Err(panic) => panic.resume(),
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
    pub async fn ask_catching<T, F>(&self, turn: F) -> Result<Result<T, Panic>, MailboxClosed>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'a>>
            + Send
            + 'static,
    {
        let (reply, answer) = oneshot::channel();
        self.deliver(Box::new(move |state| {
            Box::pin(async move {
                // `turn(state)` is inside the guard as well as the future it
                // returns, so a closure that panics before it yields one costs
                // the message rather than the actor.
                let _ = reply.send(guarded(async move { turn(state).await }).await);
            })
        }))
        .await?;
        // The sender is dropped without sending when the task died mid-turn:
        // `abort`, a runtime shutdown, or a panicking `Drop` in the state. All
        // three mean the same thing to a caller, which is why they share an
        // error rather than being told apart.
        answer.await.map_err(|_| MailboxClosed)
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
    ) -> Result<Queued<T>, MailboxClosed>
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
            self.deliver(Box::new(move |state| {
                Box::pin(async move {
                    // On the ACTOR's task. Nothing has touched the state yet,
                    // so refusing here costs the message and not a half-applied
                    // mutation.
                    if Instant::now() >= deadline {
                        let _ = reply.send(None);
                        return;
                    }
                    let _ = reply.send(Some(guarded(async move { turn(state).await }).await));
                })
            })),
        )
        .await;

        match queued {
            // The permit never arrived. Dropping the `send` future is what
            // makes this honest: tokio does not queue a message whose send was
            // cancelled, so nothing is in the mailbox to run later.
            Err(_elapsed) => Ok(Queued::Expired),
            Ok(Err(closed)) => Err(closed),
            Ok(Ok(())) => match answer.await {
                Err(_) => Err(MailboxClosed),
                Ok(None) => Ok(Queued::Expired),
                Ok(Some(ran)) => Ok(Queued::Ran(ran)),
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
        T: 'static,
        F: for<'a> FnOnce(&'a mut A) -> Pin<Box<dyn Future<Output = T> + Send + 'a>>
            + Send
            + 'static,
    {
        self.deliver(Box::new(move |state| {
            Box::pin(async move {
                let _ = guarded(async move { turn(state).await }).await;
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
        // Resolves when the loop drops the receiver, which it does on the way
        // out of the `Stop` arm. Immediate if the actor had already stopped.
        self.mail.closed().await;
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
