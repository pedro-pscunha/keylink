//! A lazily connected, self-healing handle for one peer.
//!
//! Dial-per-message is wasteful and holding one client forever is fragile: a
//! connection dies for reasons that have nothing to do with the caller — a
//! sleeping laptop, a relay restart, a NAT rebinding — and every call after
//! that fails until somebody dials again.
//!
//! [`Peer`] connects on first use, reuses the connection, and reconnects
//! **exactly once** when a call fails. A second failure is returned.
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

use crate::{EndpointAddr, Node, Result};

/// How a typed client is dialled, so [`Peer`] can re-dial one.
///
/// Implemented by every client `service!` generates. A caller does not write
/// this.
pub trait Dial: Sized {
    /// Dial `addr` from `node`.
    fn dial(
        node: &Node,
        addr: EndpointAddr,
    ) -> impl std::future::Future<Output = Result<Self>> + Send;
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
        if let Some(client) = self.slot().clone() {
            return Ok(client);
        }
        let fresh = Arc::new(C::dial(&self.node, self.addr.clone()).await?);
        *self.lock() = Some(fresh.clone());
        Ok(fresh)
    }

    /// Forget the current connection. The next call dials a new one.
    ///
    /// Rarely needed: [`Self::call`] does this itself when a call fails.
    pub fn disconnect(&self) {
        *self.lock() = None;
    }

    /// Run one call, reconnecting once if it fails.
    ///
    /// The closure receives the live client and is invoked at most twice: once
    /// on the existing connection, and once more on a freshly dialled one if the
    /// first attempt returned an error. **The second result is returned
    /// whatever it is** — there is no third attempt.
    ///
    /// # Errors
    ///
    /// The error of the second attempt, or of dialling it.
    pub async fn call<T, F, Fut>(&self, mut f: F) -> Result<T>
    where
        F: FnMut(Arc<C>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let client = self.connect().await?;
        let first = f(client).await;
        let Err(_reason) = first else {
            return first;
        };

        // A failure here may mean the connection is dead, or may mean the
        // handler said no. keylink cannot tell those apart from an `Error`
        // alone, so it re-dials either way and lets the second answer stand.
        // The cost of being wrong is one extra handshake; the cost of not
        // re-dialling is a handle that stays broken forever.
        self.disconnect();
        let fresh = self.connect().await?;
        f(fresh).await
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Arc<C>>> {
        self.client.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn slot(&self) -> Option<Arc<C>> {
        self.lock().clone()
    }
}
