//! Cross-cutting behaviour, added from outside the generated code.
//!
//! A [`Middleware`] wraps one service's dispatch. It sees the request *typed*,
//! so a per-method rule is an ordinary `match` on the real enum rather than a
//! string comparison, and it can refuse a call by returning
//! [`Outcome::Err`] instead of calling [`Next`].
//!
//! Layers are the reason the reply carries an [`Outcome`]: a timeout or an
//! authorization rule has to be able to say no, and it cannot do that by
//! inventing a return value for a method it knows nothing about.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointId;
use n0_future::time::Instant;

use crate::{ActorId, Body, BodyReader, Outcome, RemoteError};

pub(crate) type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The rest of the chain: the next layer, or the handler itself.
pub type Next<Req, Resp> =
    Arc<dyn Fn(Context, Req, BodyReader) -> BoxFut<(Outcome<Resp>, Body)> + Send + Sync>;

/// Implemented by every generated `Request` enum, so a layer can name the
/// method it is looking at without knowing the service's types.
pub trait Rpc {
    /// The service name, as written in `service!`.
    const SERVICE: &'static str;

    /// The method this request selects, in the `CamelCase` spelling from
    /// `service!`.
    fn method(&self) -> &'static str;

    /// Which actor this request addresses, for a request that names one.
    ///
    /// `None` for an ordinary service, where the whole handler is the address.
    /// Overridden by [`ActorRequest`](crate::ActorRequest), so that a layer can
    /// apply a per-actor rule before dispatch — which is the rule an actor
    /// system can still enforce from outside, now that the method name is
    /// inside the sealed payload.
    fn actor(&self) -> Option<&ActorId> {
        None
    }
}

/// How far one call got before the chain handed an answer back.
///
/// The idempotency ledger cannot read this off the [`Outcome`]: `Outcome::Err`
/// is what a refusing layer returns *and* what a panicking actor turn returns,
/// so the value genuinely cannot say who produced it. See RFC-05 §2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reached {
    /// No layer called through. Nothing ran, so a claimed key owes nothing and
    /// is given back.
    NotDispatched,
    /// The work started and did not report finishing — a `Timeout` that dropped
    /// the wait for a running actor turn, or a connection that died. Whether it
    /// took effect is unknown, which is what `Interrupted` says.
    Dispatched,
    /// The work produced the answer the chain returned. The key settles as that
    /// answer.
    Completed,
}

/// The progress of one call, shared by every clone of its [`Context`].
///
/// `Context` travels **by value** down [`Next`], so each layer holds its own
/// clone; the marker is behind an `Arc` precisely so that all of them, and the
/// accept path that built the first one, look at one value.
///
/// Marked by generated dispatch rather than by a wrapper around the base,
/// because only generated code knows when the *work* began. On the actor path
/// the base is entered while the message is still queued, so a wrapper would
/// report `Dispatched` for a turn that a [`QueueTimeout`](crate::QueueTimeout)
/// then refused before it ever ran.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct Progress(Arc<AtomicU8>);

/// Monotone by construction: `fetch_max` means a late mark cannot walk the
/// state backwards, so the two marks can be made from the actor's task without
/// ordering them against the accept task's read.
const NOT_DISPATCHED: u8 = 0;
const DISPATCHED: u8 = 1;
const COMPLETED: u8 = 2;

impl Progress {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU8::new(NOT_DISPATCHED)))
    }

    /// The work is about to begin. Called by generated dispatch.
    #[doc(hidden)]
    pub fn dispatched(&self) {
        self.0.fetch_max(DISPATCHED, Ordering::Release);
    }

    /// The work produced its answer. Called by generated dispatch.
    #[doc(hidden)]
    pub fn completed(&self) {
        self.0.fetch_max(COMPLETED, Ordering::Release);
    }

    /// Written as thresholds rather than as a `match`, so there is no catch-all
    /// arm and no unreachable state to name: the value is monotone, so every
    /// `u8` maps to exactly one answer.
    pub(crate) fn reached(&self) -> Reached {
        let raw = self.0.load(Ordering::Acquire);
        if raw >= COMPLETED {
            Reached::Completed
        } else if raw >= DISPATCHED {
            Reached::Dispatched
        } else {
            Reached::NotDispatched
        }
    }
}

/// Who is calling, and what they are calling.
///
/// `peer` is the key iroh proved during the handshake — not a claim carried in
/// the request body, which is why it is safe to authorize against.
#[derive(Debug, Clone)]
pub struct Context {
    peer: EndpointId,
    service: &'static str,
    method: &'static str,
    actor: Option<ActorId>,
    progress: Progress,
    queueing: Option<Queueing>,
}

impl Context {
    pub(crate) fn new(peer: EndpointId, service: &'static str, method: &'static str) -> Self {
        Self {
            peer,
            service,
            method,
            actor: None,
            progress: Progress::new(),
            queueing: None,
        }
    }

    /// The budget a [`QueueTimeout`] put on this call, for generated actor
    /// dispatch to honour.
    ///
    /// `None` on every call no `QueueTimeout` layer touched, which is every
    /// call by default.
    #[doc(hidden)]
    #[must_use]
    pub fn queueing(&self) -> Option<Queueing> {
        self.queueing
    }

    /// The progress marker for this call, for generated dispatch to mark.
    ///
    /// A clone shares the marker, which is what lets an actor's turn — running
    /// on another task entirely — report that it started and finished.
    #[doc(hidden)]
    #[must_use]
    pub fn progress(&self) -> Progress {
        self.progress.clone()
    }

    /// Record which actor the request addresses. Called by the accept loop for
    /// every request whose type names one.
    pub(crate) fn with_actor(mut self, actor: Option<ActorId>) -> Self {
        self.actor = actor;
        self
    }

    /// The proven public key of the calling peer.
    pub fn peer(&self) -> EndpointId {
        self.peer
    }

    /// The service being called.
    pub fn service(&self) -> &'static str {
        self.service
    }

    /// The method being called.
    ///
    /// For an actor system this is not the actor's own method: that name lives
    /// inside the sealed payload and is unreadable until the schema check has
    /// passed. Use [`Self::actor`] for a rule at this layer.
    pub fn method(&self) -> &'static str {
        self.method
    }

    /// The actor this message addresses, when the service is an actor system.
    ///
    /// As it arrived. The length cap is enforced by the registry, not here, so
    /// a value seen from a layer may be longer than [`crate::MAX_KEY_LEN`] —
    /// in which case no actor can match it and the call is refused.
    pub fn actor(&self) -> Option<&ActorId> {
        self.actor.as_ref()
    }
}

/// Where a message reached an actor from.
///
/// An actor is reachable two ways — a local [`Handle`](crate::LocalActor) and a
/// served ALPN — so "who is calling" has an answer that a concurrent
/// [`Handler`] never needs: nobody, because the call came from this process.
///
/// **Not `Option<&Context>`.** `authz.rs` records what an `Option` on an
/// identity costs, and this crate's rule is that a known value set is an enum.
/// `None` also invites `cx.map(Context::peer)` at the one call site where being
/// wrong means authorizing a local message against the last peer that happened
/// to dial in.
///
/// ```ignore
/// async fn submit(&mut self, cx: Caller<'_>, order: Order) -> Fill {
///     let who = match cx {
///         Caller::Local => Who::House,
///         Caller::Peer(cx) => Who::Trader(cx.peer()),
///     };
///     // ...
/// }
/// ```
///
/// Exhaustive, like [`Departure`](crate::Departure). A wildcard arm here is the
/// collapse the type exists to prevent.
///
/// [`Handler`]: crate::Middleware
#[derive(Debug, Clone, Copy)]
pub enum Caller<'a> {
    /// From inside this process, through the generated `Handle`. There is no
    /// peer, no handshake and no proven key, so there is nothing to attribute
    /// it to — and pretending otherwise is the failure this variant exists to
    /// make impossible.
    Local,
    /// From a peer iroh proved at the handshake.
    Peer(&'a Context),
}

impl<'a> Caller<'a> {
    /// The calling peer's context, when a peer is calling.
    ///
    /// A convenience for the common "log it, or ignore it" case. Reach for the
    /// `match` when the local case needs its own answer, which is most of the
    /// time an actor is served both ways.
    #[must_use]
    pub fn peer(self) -> Option<&'a Context> {
        match self {
            Self::Local => None,
            Self::Peer(cx) => Some(cx),
        }
    }
}

/// A [`QueueTimeout`]'s budget, as the two things the serving side needs: when
/// it runs out, and how long it was, for the refusal to report.
///
/// Carried on [`Context`] rather than kept inside the layer, because a layer
/// cannot tell a queued message from a running one — only the actor's own task
/// can, and that is where the deadline is checked.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Queueing {
    deadline: Instant,
    millis: u64,
}

impl Queueing {
    /// When the budget runs out.
    #[doc(hidden)]
    #[must_use]
    pub fn deadline(self) -> Instant {
        self.deadline
    }

    /// The budget, for [`RemoteError::TimedOut`].
    #[doc(hidden)]
    #[must_use]
    pub fn millis(self) -> u64 {
        self.millis
    }
}

/// Refuses a message that is still **queued** when its budget expires, and
/// never one that has started.
///
/// # Why this is not `Timeout`
///
/// A layer runs outside the mailbox, so a [`Timeout`]'s budget expiring drops
/// the *wait for the reply*, not the turn: the turn runs to completion whatever
/// the caller does. Ten callers get `TimedOut` at 100 ms and the actor still
/// spends a hundred seconds producing replies nobody is waiting for. That is
/// not load shedding; it is load shedding's shadow.
///
/// This one sheds real load, because it refuses **before** the work starts. The
/// deadline travels on the [`Context`] and is checked twice: once around the
/// wait for a mailbox permit, and once on the actor's own task immediately
/// before the turn begins. So a message it refuses is a message that never ran,
/// which is also what makes it the only bound that composes with an
/// [`IdempotencyKey`](crate::IdempotencyKey) — the key stays claimable.
///
/// # On a `Handler` it refuses nothing, and that is not a lie
///
/// A [`Handler`](crate::Middleware) is dispatched immediately and never queues,
/// so no message it serves is ever "still queued when the budget expired". The
/// answer is honestly zero rather than falsely reported. Use [`Timeout`] there,
/// where dropping the future genuinely cancels the work.
#[derive(Debug, Clone, Copy)]
pub struct QueueTimeout {
    budget: Duration,
}

impl QueueTimeout {
    /// Refuse any message still waiting to start after `budget`.
    #[must_use]
    pub fn after(budget: Duration) -> Self {
        Self { budget }
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> Middleware<Req, Resp> for QueueTimeout {
    fn handle(
        &self,
        mut cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> BoxFut<(Outcome<Resp>, Body)> {
        // The deadline is stamped HERE, when the call reached this layer, so an
        // outer layer that took its time is spent out of this budget rather
        // than added to it.
        cx.queueing = Some(Queueing {
            deadline: Instant::now() + self.budget,
            millis: u64::try_from(self.budget.as_millis()).unwrap_or(u64::MAX),
        });
        next(cx, req, body)
    }
}

/// One layer of behaviour around a service.
///
/// Make the impl generic over `Req: Rpc` and it applies to every service you
/// ever define, which is what keeps a logging or metrics layer from being
/// rewritten per service.
pub trait Middleware<Req, Resp>: Send + Sync + 'static {
    /// Handle one call. Call `next` to continue, or return an
    /// [`Outcome::Err`] to refuse without reaching the handler.
    fn handle(
        &self,
        cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> BoxFut<(Outcome<Resp>, Body)>;
}

/// Refuses a call that outlives its budget.
///
/// Bundled because a timeout is the one layer that is identical for every
/// service and impossible to express without the error channel.
///
/// # Cancellation
///
/// When the budget expires the handler's future is dropped where it stands. A
/// handler that must not be interrupted half way has to do that work in a task
/// the drop cannot reach.
#[derive(Debug, Clone, Copy)]
pub struct Timeout {
    budget: Duration,
}

impl Timeout {
    /// Refuse any call still running after `budget`.
    pub fn after(budget: Duration) -> Self {
        Self { budget }
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> Middleware<Req, Resp> for Timeout {
    fn handle(
        &self,
        cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> BoxFut<(Outcome<Resp>, Body)> {
        let budget = self.budget;
        Box::pin(async move {
            match n0_future::time::timeout(budget, next(cx, req, body)).await {
                Ok(reply) => reply,
                Err(_) => (
                    Outcome::Err(RemoteError::TimedOut {
                        millis: u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
                    }),
                    Body::empty(),
                ),
            }
        })
    }
}

/// Wrap `inner` in `layer`. Used by [`ServiceBuilder`](crate::ServiceBuilder).
pub(crate) fn wrap<Req: Send + 'static, Resp: Send + 'static>(
    layer: Arc<dyn Middleware<Req, Resp>>,
    inner: Next<Req, Resp>,
) -> Next<Req, Resp> {
    Arc::new(move |cx, req, body| layer.handle(cx, req, body, inner.clone()))
}
