//! A lazily connected, self-healing handle for one peer.
//!
//! Dial-per-message is wasteful and holding one client forever is fragile: a
//! connection dies for reasons that have nothing to do with the caller — a
//! sleeping laptop, a relay restart, a NAT rebinding — and every call after
//! that fails until somebody dials again.
//!
//! [`Peer`] connects on first use, reuses the connection, and reconnects
//! **exactly once** when a call proves the connection failed. Application, local
//! I/O, and per-stream errors are returned without replay. A second failure is
//! returned.
//!
//! # Why once, and not until it works
//!
//! "Reconnect once" and "reconnect forever" look identical in review and behave
//! completely differently at three in the morning. Retrying forever turns a peer
//! that is genuinely gone into a caller that never returns, and hides the outage
//! from the code best placed to react to it.
//!
//! One retry covers the case this exists for — a connection that died while
//! nothing was using it — and nothing else. A peer that refuses twice in
//! succession is reporting something the caller has to know about.
//!
//! # What this is not
//!
//! It is a convenience, not a capability. It adds no wire message, no protocol
//! and no guarantee. **It is not a retry policy for calls**: only a *connection*
//! is re-established, and the failed call is re-issued exactly once against it.
//! If a call is not safe to run twice, give it an
//! [`IdempotencyKey`](crate::IdempotencyKey) — that is the mechanism for at-most
//! once, and this one deliberately does not duplicate it.

use std::sync::{Arc, Mutex, PoisonError};

use crate::{EndpointAddr, Error, Node, Result};

/// How a typed client is dialled, so [`Peer`] can re-dial one.
///
/// Implemented by every client `service!` generates, which connects to the
/// address it is handed. Most callers need nothing else.
///
/// # Writing one
///
/// This trait is public and unsealed, and a hand-written impl is the seam for
/// **resolving an address at dial time** rather than at handle-construction
/// time. [`Peer`] calls it on every dial — the first one, the one after
/// [`Peer::disconnect`], and the automatic reconnect — so an impl that consults
/// a directory picks up a peer whose routes changed, without the handle being
/// rebuilt. `examples/waypoint.rs` measures that against a pinned control.
///
/// The returned future is [`Send`], so an impl may not hold a lock across its
/// await. Resolve first, drop the guard, then dial.
pub trait Dial: Sized {
    /// Dial `addr` from `node`.
    fn dial(
        node: &Node,
        addr: EndpointAddr,
    ) -> impl std::future::Future<Output = Result<Self>> + Send;
}

fn is_reconnectable(error: &Error) -> bool {
    matches!(error, Error::Transport(_) | Error::Closed)
}

/// A lazily connected, self-healing client for one peer.
///
/// ```ignore
/// let peer: Peer<inventory::Client> = Peer::new(&node, addr);
///
/// // Connects here. Reuses on every later call. Reconnects once if it died.
/// let item = peer.call(|c| async move { c.get_item(7).await }).await?;
/// ```
#[derive(Debug)]
pub struct Peer<C> {
    node: Node,
    addr: EndpointAddr,
    /// `Arc` so a caller can hold the client across an await while the mutex is
    /// released. The lock is **never** held across an await, which is the rule
    /// `clippy::await_holding_lock` enforces for the whole crate.
    client: Mutex<Option<Arc<C>>>,
}

impl<C: Dial + Send + Sync> Peer<C> {
    /// Name a peer without dialling it. Nothing happens until the first call.
    pub fn new(node: &Node, addr: impl Into<EndpointAddr>) -> Self {
        Self {
            node: node.clone(),
            addr: addr.into(),
            client: Mutex::new(None),
        }
    }

    pub(crate) fn node(&self) -> &Node {
        &self.node
    }

    /// The address this handle dials.
    pub fn addr(&self) -> &EndpointAddr {
        &self.addr
    }

    /// The live client, dialling if there is not one.
    ///
    /// # Errors
    ///
    /// Whatever dialling reports.
    pub async fn connect(&self) -> Result<Arc<C>> {
        // Read and release. Dialling happens with no lock held, so two
        // concurrent first-callers may both dial; one of them wins the slot and
        // the other's client is dropped. That is a wasted handshake, not a bug,
        // and it is much cheaper than holding a lock across a network dial.
        if let Some(client) = self.slot() {
            return Ok(client);
        }
        let fresh = Arc::new(C::dial(&self.node, self.addr.clone()).await?);
        *self.lock() = Some(fresh.clone());
        Ok(fresh)
    }

    /// Forget the current connection. The next call dials a new one.
    ///
    /// Rarely needed: [`Self::call`] does this itself when a call proves the
    /// cached connection failed.
    pub fn disconnect(&self) {
        *self.lock() = None;
    }

    fn disconnect_if(&self, client: &Arc<C>) {
        let mut slot = self.lock();
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, client))
        {
            *slot = None;
        }
    }

    /// Run one call, reconnecting once if it fails.
    ///
    /// The closure receives the live client and is invoked at most twice: once
    /// on the existing connection, and once more on a freshly dialled one if the
    /// first attempt returned [`Error::Transport`] or [`Error::Closed`]. Other
    /// errors are returned without invalidating the cached client. **The second
    /// result is returned whatever it is** — there is no third attempt, though a
    /// second connection failure is removed from the cache.
    ///
    /// # Errors
    ///
    /// A non-connection error from the first attempt, the second attempt's error,
    /// or an error dialling the replacement.
    pub async fn call<T, F, Fut>(&self, mut f: F) -> Result<T>
    where
        F: FnMut(Arc<C>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let client = self.connect().await?;
        let first = f(client.clone()).await;
        let Err(reason) = first else {
            return first;
        };

        // A remote/application or per-stream error proves nothing about the
        // cached connection's health. Only a transport failure or clean close
        // both invalidates this client and permits replay; all other errors are
        // returned without touching a connection another concurrent call may use.
        if !is_reconnectable(&reason) {
            return Err(reason);
        }
        self.disconnect_if(&client);

        let fresh = self.connect().await?;
        let second = f(fresh.clone()).await;
        if second.as_ref().is_err_and(is_reconnectable) {
            self.disconnect_if(&fresh);
        }
        second
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Arc<C>>> {
        self.client.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn slot(&self) -> Option<Arc<C>> {
        self.lock().clone()
    }
}
