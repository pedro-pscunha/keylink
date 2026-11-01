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
