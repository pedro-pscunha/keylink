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
//! A local call fails one way: the actor is gone. That is why [`MailboxClosed`]
//! is a unit struct and why it is not [`Error`](crate::Error). RFC-04 §5.2 and
//! §5.5 reject a handle that is the same type locally and remotely, because such
//! a handle must either claim failures the local case cannot have, or hide
//! failures the remote case does. `Error::SchemaMismatch` has no meaning between
//! two objects in one process.
//!
//! The method names and the success types match the remote client exactly, so
//! moving an actor across the boundary is a mechanical edit the compiler drives.
//! What does not compile is the code that ignored the difference.
//!
//! # A panic reaches the caller
//!
//! A local call behaves like a local call: if the handler panics, the panic is
//! caught so the **actor survives**, and then resumed in the caller — the same
//! payload, unwinding the same way an ordinary function call would.
//!
//! Both halves matter. Catching keeps one bad message from costing every later
//! one. Resuming keeps [`MailboxClosed`] honest: it means the actor is gone, and
//! never "the actor is fine and your message exploded".
//!
//! The remote path does the opposite with the same catch — see
//! [`LocalActor::ask_catching`] — because a panic must not travel to another
//! machine as an unwind.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use n0_future::task::JoinHandle;
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
/// So here the mailbox itself is the backpressure: `send` on a full channel
/// waits, on the calling task, which is where a slow actor should be felt.
/// Raising this moves messages out of the caller, where they are visible, into a
/// buffer where they are not.
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
    /// # Panics
    ///
    /// With whatever the handler panicked with, after the actor has survived it.
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
        // The turn ran and dropped the sender without sending only if the task
        // died mid-turn, which is a runtime shutdown taking it.
        answer.await.map_err(|_| MailboxClosed)
    }

    /// Queue one turn and do not wait for it.
    ///
    /// What a local `tell` uses. The `Ok` means the message is in the mailbox
    /// and nothing more: it has not run, it may panic, and there is no channel
    /// on which to learn either. A panic is caught, so the actor survives, and
    /// then dropped, because there is nobody left to tell.
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
    /// Ordered: everything sent before this is handled and answered. Everything
    /// sent after it — including anything already queued behind it — is refused
    /// with [`MailboxClosed`], because the loop drops the receiver on its way
    /// out.
    ///
    /// Idempotent, and takes no deadline: `n0_future::time::timeout` composes,
    /// and a parameter here would duplicate it. A turn that never returns is
    /// what [`Self::abort`] is for.
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

    fn add(n: u64) -> impl for<'a> FnOnce(&'a mut Counter) -> Pin<Box<dyn Future<Output = usize> + Send + 'a>>
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
