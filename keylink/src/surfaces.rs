//! Capabilities shared by generated caller surfaces.

use std::future::Future;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    Addressed, Allowlist, Ended, Granting, Idempotency, IdempotencyKey, KeyState, NotQueued,
    Registry, Result, Rpc, Server, ServiceBuilder, Serving, Unanswered, Watch,
};

/// Operations shared by asynchronous remote caller surfaces.
pub trait Wire {
    /// The contract version negotiated by this connection.
    fn version(&self) -> crate::Version;
    /// Ask what this peer's ledger knows about a key.
    fn key_state(&self, key: IdempotencyKey) -> impl Future<Output = Result<KeyState>> + Send;

    /// Await this peer's departure.
    fn watch(&self) -> Watch;

    /// Close the connection gracefully.
    fn close(self) -> impl Future<Output = Result<()>> + Send;
}

/// Operations shared by blocking remote caller surfaces.
pub trait BlockingWire {
    /// The contract version negotiated by this connection.
    fn version(&self) -> crate::Version;
    /// Ask what this peer's ledger knows about a key.
    fn key_state(&self, key: IdempotencyKey) -> Result<KeyState>;

    /// Close the connection gracefully.
    fn close(self) -> Result<()>;
}

/// Scope a caller to one idempotency key.
pub trait WithIdempotency {
    /// The caller surface that carries the key.
    type Output;

    /// Attach the key to subsequent calls through the returned surface.
    #[must_use = "discarding this value discards the idempotency key"]
    fn with_idempotency(&self, idempotency: impl Into<Idempotency>) -> Self::Output;
}

/// Bound every call made through a caller surface.
///
/// Implemented by the generated asynchronous clients: the plain one, the one that
/// carries an idempotency key, and the keyed registry's. **The blocking clients
/// do not implement it**: a blocking call that has to end sooner is bounded by the
/// thread that made it, and a second timer under the first would answer the same
/// question twice.
pub trait WithDeadline: Sized {
    /// The same surface, with each call's wait bounded by `budget`.
    ///
    /// The caller stops waiting after `budget` and gets
    /// [`Error::DeadlinePassed`](crate::Error::DeadlinePassed), and the budget
    /// travels in the request so the service stops waiting no later than that. A
    /// deadline answers the caller and does not undo work already done, so bound a
    /// keyed call and retry it with the same key. See
    /// [`RpcClient::with_deadline`](crate::RpcClient::with_deadline).
    #[must_use = "discarding this value discards the deadline"]
    fn with_deadline(&self, budget: Duration) -> Self;
}

/// Read one actor's current serving state.
pub trait Status {
    /// Read the serving state without enqueueing a user message.
    fn status(&self) -> impl Future<Output = Result<Serving>> + Send;
}

/// Operations owned by a local actor handle.
pub trait Own: Sized {
    /// The actor state read by [`Own::with_state`].
    type State;
    /// A borrowed reservation carrying the generated service methods.
    type Permit<'a>
    where
        Self: 'a;
    /// An owned reservation carrying the generated service methods.
    type OwnedPermit;

    /// Read the state between turns.
    fn with_state<T, F>(
        &self,
        read: F,
    ) -> impl Future<Output = std::result::Result<T, Unanswered>> + Send
    where
        T: Send + 'static,
        F: FnOnce(&Self::State) -> T + Send + 'static;

    /// Stop after queued messages and wait for the actor to end.
    fn stop(&self) -> impl Future<Output = std::result::Result<(), NotQueued>> + Send;

    /// Cancel the actor where it stands.
    fn abort(&self);

    /// Whether the actor is still handling messages.
    fn is_running(&self) -> bool;

    /// Resolve when the actor ends.
    fn ended(&self) -> impl Future<Output = Ended> + Send + 'static;

    /// Read the actor's serving state.
    fn status(&self) -> Serving;

    /// Wait for and hold one mailbox slot.
    fn reserve(
        &self,
    ) -> impl Future<Output = std::result::Result<Self::Permit<'_>, NotQueued>> + Send;

    /// Try to hold one mailbox slot without waiting.
    fn try_reserve(&self) -> std::result::Result<Option<Self::Permit<'_>>, NotQueued>;

    /// Wait for and own one mailbox slot.
    fn reserve_owned(
        self,
    ) -> impl Future<Output = std::result::Result<Self::OwnedPermit, NotQueued>> + Send;
}

/// Build or directly serve a generated service.
pub trait Servable {
    /// The request carried by the service.
    type Request: Rpc + DeserializeOwned + Send + Sync + 'static;
    /// The response carried by the service.
    type Response: Serialize + Granting + Send + Sync + 'static;

    /// Build the service with its allowlist.
    #[must_use = "a service builder does nothing until registered on a server"]
    fn service(&self, allow: Allowlist) -> ServiceBuilder<Self::Request, Self::Response>;

    /// Serve this service alone on a node.
    fn serve(&self, node: &crate::Node, allow: Allowlist) -> Result<Server> {
        Server::builder(node)
            .service(self.service(allow).build())
            .spawn()
    }
}

/// Generated keyed dispatch for one service's mailbox type.
#[doc(hidden)]
pub trait RegistryDispatch: Sized + Send + 'static {
    /// The service request inside [`Addressed`].
    type Request: Rpc + DeserializeOwned + Send + Sync + 'static;
    /// The service response.
    type Response: Serialize + Granting + Send + Sync + 'static;

    /// Build the addressed service around `registry`.
    fn service(
        registry: &Registry<Self>,
        allow: Allowlist,
    ) -> ServiceBuilder<Addressed<Self::Request>, Self::Response>;
}

impl<M> Servable for Registry<M>
where
    M: RegistryDispatch,
{
    type Request = Addressed<M::Request>;
    type Response = M::Response;

    fn service(&self, allow: Allowlist) -> ServiceBuilder<Self::Request, Self::Response> {
        M::service(self, allow)
    }
}
