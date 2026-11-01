//! Every connection, grouped by proven peer; cleanup removes exact registrations.
use crate::LiveConnection;
use iroh::PublicKey;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

type Connections = BTreeMap<PublicKey, BTreeMap<usize, (u64, LiveConnection)>>;
#[derive(Debug, Clone, Copy)]
pub(super) struct Registration {
    peer: PublicKey,
    connection: usize,
    generation: u64,
}
#[derive(Debug, Default)]
struct Inner {
    connections: Mutex<Connections>,
    next: AtomicU64,
}
#[derive(Debug, Default)]
pub(super) struct Live(Arc<Inner>);
impl Live {
    pub(super) fn insert(&self, live: LiveConnection) -> Registration {
        let registration = Registration {
            peer: live.peer().id(),
            connection: live.id(),
            generation: self.0.next.fetch_add(1, Ordering::Relaxed),
        };
        let watch = live.watch();
        self.0
            .connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(registration.peer)
            .or_default()
            .insert(registration.connection, (registration.generation, live));
        let weak = Arc::downgrade(&self.0);
        n0_future::task::spawn(async move {
            let _ = watch.await;
            if let Some(inner) = weak.upgrade() {
                remove(&inner, registration);
            }
        });
        registration
    }
    pub(super) fn remove(&self, registration: Registration) {
        remove(&self.0, registration);
    }
    pub(super) fn snapshot(&self) -> Vec<(Registration, LiveConnection)> {
        self.0
            .connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .flat_map(|(peer, connections)| {
                connections
                    .iter()
                    .map(move |(connection, (generation, live))| {
                        (
                            Registration {
                                peer: *peer,
                                connection: *connection,
                                generation: *generation,
                            },
                            live.clone(),
                        )
                    })
            })
            .collect()
    }
    pub(super) fn len(&self) -> usize {
        self.0
            .connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(BTreeMap::len)
            .sum()
    }
}
fn remove(inner: &Inner, registration: Registration) {
    let mut peers = inner
        .connections
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some(connections) = peers.get_mut(&registration.peer) {
        if connections
            .get(&registration.connection)
            .is_some_and(|(generation, _)| *generation == registration.generation)
        {
            connections.remove(&registration.connection);
        }
        if connections.is_empty() {
            peers.remove(&registration.peer);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    #[tokio::test]
    // Made red with scripts/break-once.sh by making the generation comparison in remove unconditionally `true`.
    // The multi-line or same-file edit is described rather than quoted.
    // Watched: cleanup for the old registration erased the new one (0 rather than 1).
    async fn stale_cleanup_cannot_remove_a_new_registration() {
        let server = crate::Node::bind_local().await.unwrap();
        let client = crate::Node::bind_local().await.unwrap();
        server
            .endpoint()
            .set_alpns(vec![b"live-registration".to_vec()]);
        let accept = async { server.endpoint().accept().await.unwrap().await.unwrap() };
        let (dialed, accepted) = tokio::join!(
            client
                .endpoint()
                .connect(server.endpoint().addr(), b"live-registration"),
            accept
        );
        let _dialed = dialed.unwrap();
        let peer = crate::ProvenPeer::on(&accepted);
        let connection = LiveConnection::new(accepted, peer);
        let live = Live::default();
        let old = live.insert(connection.clone());
        let new = live.insert(connection);
        live.remove(old);
        assert_eq!(live.len(), 1);
        live.remove(new);
        assert_eq!(live.len(), 0);
    }
}
