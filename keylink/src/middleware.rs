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
use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointId;

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
}

impl Context {
    pub(crate) fn new(peer: EndpointId, service: &'static str, method: &'static str) -> Self {
        Self {
            peer,
            service,
            method,
            actor: None,
        }
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
            match tokio::time::timeout(budget, next(cx, req, body)).await {
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
