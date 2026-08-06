//! Peers that dialled in, addressable by the accept side.
//!
//! keylink's calls are one-directional by construction: the dialling side asks
//! and the accept side answers. A price feed, a job queue and an order desk all
//! want the other arrangement — the accept side has something to say and the
//! peer is not asking.
//!
//! RFC-04 §4.4's Design B is what this supports, and it adds **nothing to the
//! wire**. The reverse contract is an ordinary `service!` block with its own
//! derived ALPN, served by the side that dialled; the side that accepted dials
//! back and becomes an ordinary client of it. Both directions keep their own
//! allowlist, their own schema gate and their own layers, because both are
//! ordinary services. There is no new method kind, no `WireKinds` field and no
//! second meaning for an existing one.
//!
//! What is left over is bookkeeping: *which peers are here, and what is the
//! handle for talking back to each of them*. That is this type.
//!
//! ```ignore
//! // in the accept side's handler, which now sees who is calling
//! async fn subscribe(&self, cx: &Context) -> Ack {
//!     let back = ticker::Client::connect(&self.node, cx.peer()).await?;
//!     self.subscribers.insert(cx.peer(), back.clone(), back.watch());
//!     Ack
//! }
//!
//! // and later, from anywhere
//! for (peer, client) in self.subscribers.snapshot() {
//!     let _ = client.tick(price).await;
//! }
//! ```
//!
//! # Why this is a type and not a `HashMap` at a call site
//!
//! One reason, and it is the whole of the content: **it evicts on
//! [`Departure`](crate::Departure), not on [`Connection::closed`]**.
//!
//! `Connection::closed` borrows a strong handle, so a registry that waited on
//! one would keep every connection it had ever seen alive — a diagnostic that
//! pins its subject open, which is a leak wearing a diagnostic's clothes.
//! [`Watch`] registers against a weak handle instead, so a subscriber that goes
//! away goes away, and the registry hears about it rather than holding it. The
//! argument is `watch.rs`'s, applied one layer up.
//!
//! The eviction task holds a [`Weak`] on the map for the same reason: dropping
//! the registry must not be blocked by peers that have not left yet.
//!
//! # What this does not promise
//!
//! - **Nothing about delivery.** A handle in here means the connection had not
//!   ended last time anybody looked. A push can still fail, and the answer to
//!   that is the ordinary one: it returns an error.
//! - **One handle per peer.** A peer that subscribes twice replaces its own
//!   entry. Two live connections to one peer are not two subscribers, because
//!   an [`EndpointId`] is the identity and there is only one of those.
//! - **No ordering, no fan-out, no retry.** [`snapshot`](Subscribers::snapshot)
//!   hands back a list; what to do with it is the caller's, and a broadcast
//!   that must not lose messages needs a durable queue rather than a map.
//!
//! [`Connection::closed`]: iroh::endpoint::Connection::closed
//! [`Weak`]: std::sync::Weak

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use iroh::EndpointId;

use crate::Watch;

/// Handles for the peers that dialled in, keyed by proven identity.
///
/// Cheap to clone: every clone names the same registry, so the handler that
/// inserts and the task that pushes can each hold one.
#[derive(Debug)]
pub struct Subscribers<C> {
    peers: Arc<Mutex<HashMap<EndpointId, C>>>,
}

impl<C> Subscribers<C> {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Remember `client` as the way back to `peer`, until `departure` fires.
    ///
    /// `departure` is a [`Watch`] on the connection this handle owns —
    /// `client.watch()` for a generated client, or [`Watch::on`] for a
    /// connection you hold yourself. Passing a watch on the *wrong* connection
    /// is the one way to misuse this, and it is why the two arrive together at
    /// one call site rather than being registered separately.
    ///
    /// Replaces any handle already held for this peer, which is what makes
    /// resubscribing after a reconnect the obvious thing rather than a leak.
    pub fn insert(&self, peer: EndpointId, client: C, departure: Watch)
    where
        C: Send + 'static,
    {
        self.lock().insert(peer, client);

        // Weak, so a registry nobody holds any more is dropped even with
        // subscribers still connected. The task then wakes once, finds nothing,
        // and ends.
        let peers = Arc::downgrade(&self.peers);
        n0_future::task::spawn(async move {
            // Every ending evicts. A peer that said goodbye, a peer that
            // vanished and a connection we closed ourselves are three different
            // things to report and one thing to do about.
            let _departure = departure.await;
            if let Some(peers) = Weak::upgrade(&peers) {
                peers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&peer);
            }
        });
    }

    /// Forget a peer without waiting for its connection to end.
    ///
    /// For a peer that unsubscribed rather than left. The eviction task stays
    /// registered and finds nothing to do, which costs one wake-up.
    pub fn remove(&self, peer: EndpointId) -> Option<C> {
        self.lock().remove(&peer)
    }

    /// How many peers are here.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether nobody is here.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Poison recovery rather than propagation, the same call the idempotency
    /// ledger makes: one bad request should cost one request. Nothing here can
    /// panic while the lock is held, so this arm is unreachable in practice and
    /// must still be typed.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<EndpointId, C>> {
        self.peers.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<C: Clone> Subscribers<C> {
    /// The handle for one peer, if it is still here.
    #[must_use]
    pub fn get(&self, peer: EndpointId) -> Option<C> {
        self.lock().get(&peer).cloned()
    }

    /// Every peer here, as of now.
    ///
    /// Named for what it is. An eager `Vec`, not a lazy `iter()`: the lock is
    /// released before the caller does anything with it, so a push that awaits
    /// cannot be holding it. An iterator returning `C` from behind the lock
    /// would have to keep it, and a broadcast that holds a mutex across an
    /// await is the deadlock this whole crate is arranged to avoid.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(EndpointId, C)> {
        self.lock()
            .iter()
            .map(|(peer, client)| (*peer, client.clone()))
            .collect()
    }
}

impl<C> Default for Subscribers<C> {
    fn default() -> Self {
        Self::new()
    }
}

/// By hand, because `#[derive(Clone)]` would demand `C: Clone` — and a registry
/// is shared whether or not the handles inside it can be copied out.
impl<C> Clone for Subscribers<C> {
    fn clone(&self) -> Self {
        Self {
            peers: Arc::clone(&self.peers),
        }
    }
}
