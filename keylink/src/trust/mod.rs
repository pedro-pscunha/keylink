//! Signed authority, attenuating delegation, and live admission from one local view.
//!
//! Feed chains with [`Trust::insert`](crate::trust::Trust::insert) or [`present`](crate::trust::present()), then install a [`Guard`](crate::trust::Guard)
//! before dispatch. Feed signed revocations and root changes through
//! [`Revocable::apply`]. Every call checks current standing, even before the
//! connection-closing sweep finishes. Calls already dispatched run to completion.
//!
//! Verifying a previously unseen chain can establish a pending revoker's authority.
//! That decision is retained as a permanent tombstone and closes affected live
//! connections, even when the offered chain itself is consequently refused.
//!
//! Each store is one node's view. Revocation reaches other nodes only as fast as
//! the application's feeder delivers it. Idle expired connections close on
//! [`Change::Expire`](crate::trust::Change::Expire); expiry is always enforced at admission and per call.
//! Revocation records are permanent and currently never pruned. Host clocks are
//! trusted. Idempotency remains per device key, not per delegated principal.

mod cert;
mod chain;
mod guard;
mod live;
mod present;
mod sign;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use iroh::PublicKey;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    Admission, Admittance, AdmittedPeer, LiveConnection, ProvenPeer, Revocable, Rpc, ServiceBuilder,
};
use chain::{standing, structure, Revoked};

pub use cert::{
    Caveats, CertId, Certificate, CertificateError, Delegation, Pattern, PatternFault, Revocation,
    Scope, Validity,
};
pub use chain::{Chain, ChainError, Credential, Roots, Widening};
pub use guard::{Access, AccessError, Call, Guard, WithinCaveats};
pub use present::{present, Presented};
pub use sign::{Opened, Purpose, Signed, SignedError};

/// Maximum links in one chain.
pub const MAX_CHAIN_LINKS: usize = 8;
/// Maximum bytes in one certificate body.
pub const MAX_CERTIFICATE: usize = 4096;
/// Maximum patterns in one scope.
pub const MAX_PATTERNS: usize = 32;
/// Maximum bytes in a pattern name.
pub const MAX_PATTERN_LEN: usize = 255;
/// Maximum bytes in a signed application body.
pub const MAX_SIGNED: usize = 1 << 16;
/// Maximum stored chains per holder.
pub const MAX_CHAINS_PER_HOLDER: usize = 16;
/// Total retained chains; at the cap, new entries are refused rather than silently evicted.
pub const MAX_RETAINED_CHAINS: usize = 1024;
/// Total pending signed revocation records whose authority is not yet established.
pub const MAX_PENDING_REVOCATIONS: usize = 1024;

/// Why a signed change could not be retained. Refusal leaves the prior view intact.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChangeError {
    /// The budget for not-yet-authoritative revocation records is full.
    #[error("the pending revocation budget of {max} records is full")]
    PendingRevocationsFull {
        /// Maximum retained pending records.
        max: usize,
    },
}

/// Seconds since the Unix epoch. Verifiers take an explicit time.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    postcard_schema::Schema,
)]
pub struct Timestamp(u64);
impl Timestamp {
    /// A point in time by Unix seconds.
    pub const fn from_unix_secs(secs: u64) -> Self {
        Self(secs)
    }
    /// Unix seconds.
    pub const fn unix_secs(self) -> u64 {
        self.0
    }
    /// The browser-safe wall clock. Before the epoch reads as zero.
    pub fn now() -> Self {
        Self(
            n0_future::time::SystemTime::now()
                .duration_since(n0_future::time::SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        )
    }
}

#[derive(Debug)]
struct Store {
    roots: Roots,
    chains: BTreeMap<PublicKey, Vec<Arc<Credential>>>,
    revoked: Revoked,
}
impl Store {
    fn standing(&mut self, chain: &Chain, now: Timestamp) -> Result<(), ChainError> {
        standing(chain, &self.roots, &mut self.revoked, now)
    }

    fn credentials(
        &mut self,
        peer: PublicKey,
        now: Timestamp,
        permits: impl Fn(&Credential) -> bool,
    ) -> Vec<Arc<Credential>> {
        let mut candidates = Vec::new();
        if self.roots.contains(&peer) {
            let credential = Credential::root(peer);
            if permits(&credential) {
                candidates.push(Arc::new(credential));
            }
        }
        if let Some(chains) = self.chains.get(&peer) {
            for credential in chains {
                if standing(credential.chain(), &self.roots, &mut self.revoked, now).is_ok()
                    && permits(credential)
                {
                    candidates.push(credential.clone());
                }
            }
        }
        candidates
    }
}
fn retention_order(a: &Credential, b: &Credential) -> std::cmp::Ordering {
    a.caveats()
        .validity
        .not_after()
        .cmp(&b.caveats().validity.not_after())
        .then_with(|| {
            a.chain()
                .links()
                .iter()
                .map(Certificate::id)
                .cmp(b.chain().links().iter().map(Certificate::id))
        })
}

#[derive(Debug)]
struct Inner {
    store: Mutex<Store>,
    live: live::Live,
}

/// A cheap-clone handle to the node's roots, chains, revocations, and live peers.
///
/// This is an [`Admission`] adapter. Its [`Guard`] also checks current standing
/// on every call, before idempotency, layers, or actor lookup. Revoking authority
/// cannot cancel work that already passed preflight.
#[derive(Debug, Clone)]
pub struct Trust(Arc<Inner>);
impl Trust {
    /// Start from explicitly named trusted roots; no implicit default policy.
    pub fn new(roots: Roots) -> Self {
        Self(Arc::new(Inner {
            store: Mutex::new(Store {
                roots,
                chains: BTreeMap::new(),
                revoked: Revoked::new(),
            }),
            live: live::Live::default(),
        }))
    }
    fn store(&self) -> MutexGuard<'_, Store> {
        self.0.store.lock().unwrap_or_else(PoisonError::into_inner)
    }
    /// Verify and retain one chain at the current time. Refused data is not stored.
    /// Identical certificate-id sequences are a no-op. At capacity, the chain
    /// with the earliest expiry is evicted, breaking ties by ordered certificate
    /// IDs. This total order makes retention independent of feed order. Returns
    /// `NotRetained` if the offered
    /// chain loses that comparison, or the first verification fault.
    pub fn insert(&self, chain: Chain) -> Result<(), ChainError> {
        let credential = Arc::new(structure(&chain)?);
        let mut store = self.store();
        let before = store.revoked.accepted_len();
        let result = (|| {
            store.standing(&chain, Timestamp::now())?;
            let held = store.chains.get(&credential.holder());
            let duplicate = held.is_some_and(|chains| {
                chains.iter().any(|c| {
                    c.chain()
                        .links()
                        .iter()
                        .map(Certificate::id)
                        .eq(chain.links().iter().map(Certificate::id))
                })
            });
            if duplicate {
                return Ok(());
            }
            let total: usize = store.chains.values().map(Vec::len).sum();
            if total >= MAX_RETAINED_CHAINS
                && held.is_none_or(|chains| chains.len() < MAX_CHAINS_PER_HOLDER)
            {
                return Err(ChainError::StoreFull {
                    max: MAX_RETAINED_CHAINS as u32,
                });
            }
            let chains = store.chains.entry(credential.holder()).or_default();
            if chains.iter().any(|c| {
                c.chain()
                    .links()
                    .iter()
                    .map(Certificate::id)
                    .eq(chain.links().iter().map(Certificate::id))
            }) {
                return Ok(());
            }
            chains.push(credential);
            if chains.len() > MAX_CHAINS_PER_HOLDER {
                if let Some((index, _)) = chains
                    .iter()
                    .enumerate()
                    .min_by(|(_, a), (_, b)| retention_order(a, b))
                {
                    let offered = index == chains.len() - 1;
                    chains.remove(index);
                    if offered {
                        return Err(ChainError::NotRetained);
                    }
                }
            }
            chains.sort_by(|a, b| retention_order(a, b));
            Ok(())
        })();
        let promoted = store.revoked.accepted_len() != before;
        drop(store);
        if promoted {
            let _ = self.sweep();
        }
        result
    }
    /// Verify a chain against this view's roots and revocations at `now`.
    /// Establishing a pending revocation's authority makes it permanent and closes
    /// affected connections, including when verification returns `Revoked`.
    pub fn verify(&self, chain: &Chain, now: Timestamp) -> Result<Credential, ChainError> {
        let credential = structure(chain)?;
        let mut store = self.store();
        let before = store.revoked.accepted_len();
        let result = store.standing(chain, now).map(|()| credential);
        let promoted = store.revoked.accepted_len() != before;
        drop(store);
        if promoted {
            let _ = self.sweep();
        }
        result
    }
    /// Open a forwarded value against this view's roots and revocations.
    /// Checks signature, chain standing and the purpose's operation caveat before
    /// decoding. Resource caveats are not checked: the application must authorize
    /// the value's resource against the returned credential before using it.
    /// Like `verify`, learning pending revocation authority can close connections.
    pub fn open<T: Serialize + DeserializeOwned>(
        &self,
        signed: &Signed<T>,
        purpose: &Purpose,
        now: Timestamp,
    ) -> Result<Opened<T>, SignedError> {
        // User-provided serde runs outside the store lock.
        let (roots, mut revoked, promoted) = {
            let mut store = self.store();
            // Certificate signatures already verified independently of T. Resolve
            // pending authority in the shared view before copying it for serde.
            let before = store.revoked.accepted_len();
            let _ = store.standing(signed.chain(), now);
            (
                store.roots.clone(),
                store.revoked.clone(),
                store.revoked.accepted_len() != before,
            )
        };
        if promoted {
            let _ = self.sweep();
        }
        signed.open_with(purpose, &roots, &mut revoked, now)
    }
    /// Advisory count of remembered live connections.
    pub fn live(&self) -> usize {
        self.0.live.len()
    }
    /// Add a per-call application policy after the certificate caveats.
    pub fn guard<Req: Rpc + 'static, A: Access>(&self, access: A) -> Guard<Req, A> {
        Guard::new(self.clone(), access)
    }
    /// The presentation door, admitted to anyone because it establishes admission.
    #[must_use = "a service builder does nothing until registered on a server"]
    pub fn presentation(&self) -> ServiceBuilder<present::wire::Request, present::wire::Response> {
        present::service(self.clone())
    }
    fn sweep(&self) -> Vec<PublicKey> {
        let mut closed = BTreeSet::new();
        for (registration, live) in self.0.live.snapshot() {
            let id = live.peer().id();
            if !self.admitting(id) {
                live.close();
                self.0.live.remove(registration);
                closed.insert(id);
            }
        }
        // TODO: wire the separate Blobs::revoke_peer capability for every
        // affected holder, including inactive ones. `closed` names only live
        // connections; closing RPC alone does not retract prior blob grants.
        closed.into_iter().collect()
    }
    fn current(&self, credential: &Credential, now: Timestamp) -> bool {
        let mut store = self.store();
        let before = store.revoked.accepted_len();
        let current = if credential.chain().links().is_empty() {
            store.roots.contains(&credential.holder())
        } else {
            store.standing(credential.chain(), now).is_ok()
        };
        let promoted = store.revoked.accepted_len() != before;
        drop(store);
        if promoted {
            let _ = self.sweep();
        }
        current
    }
    fn admitting(&self, peer: PublicKey) -> bool {
        !self
            .store()
            .credentials(peer, Timestamp::now(), |_| true)
            .is_empty()
    }
    fn permitting(
        &self,
        peer: PublicKey,
        now: Timestamp,
        resource: Option<&str>,
        operation: &str,
    ) -> Vec<Arc<Credential>> {
        self.store()
            .credentials(peer, now, |c| c.permits(resource, operation))
    }
}

/// A change that can narrow admission. Applying it rechecks all live connections.
#[derive(Debug, Clone)]
pub enum Change {
    /// Permanently record a strictly verified signed revocation.
    Revoke(Revocation),
    /// Replace the trusted roots.
    Roots(Roots),
    /// Drop expired chains and close connections left without a standing chain.
    Expire,
}
impl Admission for Trust {
    fn admits(&self, peer: &ProvenPeer) -> Admittance {
        if self.admitting(peer.id()) {
            Admittance::Admitted
        } else {
            Admittance::Refused
        }
    }
    fn admitted(&self, peer: &AdmittedPeer, live: LiveConnection) {
        let registration = self.0.live.insert(live.clone());
        // Insert BEFORE the re-read: a concurrent change either sees this
        // connection in its snapshot or is visible to this check.
        if !self.admitting(peer.id()) {
            live.close();
            self.0.live.remove(registration);
        }
    }
}
impl Revocable for Trust {
    type Change = Change;
    type Error = ChangeError;
    fn apply(&self, change: Change) -> Result<Vec<PublicKey>, ChangeError> {
        {
            let mut store = self.store();
            match change {
                Change::Revoke(revocation) => {
                    let roots = store.roots.clone();
                    let authoritative = roots.contains(&revocation.revoker())
                        || store.chains.values().flatten().any(|credential| {
                            let mut upstream = BTreeSet::new();
                            credential.chain().links().iter().any(|link| {
                                upstream.insert(link.issuer());
                                link.id() == revocation.certificate()
                                    && upstream.contains(&revocation.revoker())
                            })
                        });
                    store.revoked.add(&revocation, authoritative)?;
                    let chains: Vec<_> = store
                        .chains
                        .values()
                        .flatten()
                        .map(|c| c.chain().clone())
                        .collect();
                    for chain in chains {
                        let _ = store.standing(&chain, Timestamp::now());
                    }
                }
                Change::Roots(roots) => {
                    store.revoked.trust_roots(&roots);
                    store.roots = roots;
                }
                Change::Expire => {
                    let now = Timestamp::now();
                    store.chains.retain(|_, chains| {
                        chains.retain(|c| c.caveats().validity.not_after() > now);
                        !chains.is_empty()
                    });
                }
            }
        }
        // Mutate FIRST, then snapshot: reconnects must meet the new view.
        Ok(self.sweep())
    }
}

#[cfg(test)]
mod tests;
