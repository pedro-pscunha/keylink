//! Signed authority, attenuating delegation, and live admission from one local view.
//!
//! Feed chains with [`Trust::insert`](crate::trust::Trust::insert) or [`present`](crate::trust::present()), then install a [`Guard`](crate::trust::Guard)
//! before dispatch. Feed signed revocations and root changes through
//! [`Revocable::apply`]. Every call checks current standing, even before the
//! connection-closing sweep finishes. Calls already dispatched run to completion.
//!
//! Each store is one node's view. Revocation reaches other nodes only as fast as
//! the application's feeder delivers it. Idle expired connections close on
//! [`Change::Expire`](crate::trust::Change::Expire); expiry is always enforced at admission and per call.
//! Revocation records are permanent and currently never pruned. Host clocks are
//! trusted. Idempotency remains per device key, not per delegated principal.

mod cert;
mod chain;
mod guard;
mod present;
mod sign;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use iroh::PublicKey;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    Admission, Admittance, AdmittedPeer, LiveConnection, ProvenPeer, Revocable, Rpc,
    ServiceBuilder, Subscribers,
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
    fn credential(
        &self,
        peer: PublicKey,
        now: Timestamp,
        permits: impl Fn(&Credential) -> bool,
    ) -> Option<Arc<Credential>> {
        if self.roots.contains(&peer) {
            let credential = Credential::root(peer);
            return permits(&credential).then(|| Arc::new(credential));
        }
        self.chains
            .get(&peer)?
            .iter()
            .find(|c| standing(c.chain(), &self.roots, &self.revoked, now).is_ok() && permits(c))
            .cloned()
    }
}
#[derive(Debug)]
struct Inner {
    store: Mutex<Store>,
    live: Subscribers<LiveConnection>,
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
            live: Subscribers::new(),
        }))
    }
    fn store(&self) -> MutexGuard<'_, Store> {
        self.0.store.lock().unwrap_or_else(PoisonError::into_inner)
    }
    /// Verify and retain one chain at the current time. Refused data is not stored.
    /// Identical certificate-id sequences are a no-op. At capacity, the chain
    /// with the earliest expiry is evicted. Returns the first verification fault.
    pub fn insert(&self, chain: Chain) -> Result<(), ChainError> {
        let credential = Arc::new(structure(&chain)?);
        let mut store = self.store();
        standing(&chain, &store.roots, &store.revoked, Timestamp::now())?;
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
                .min_by_key(|(_, c)| c.caveats().validity.not_after())
            {
                chains.remove(index);
            }
        }
        Ok(())
    }
    /// Verify a chain against this view's roots and revocations at `now`.
    pub fn verify(&self, chain: &Chain, now: Timestamp) -> Result<Credential, ChainError> {
        let credential = structure(chain)?;
        let store = self.store();
        standing(chain, &store.roots, &store.revoked, now)?;
        Ok(credential)
    }
    /// Open a forwarded value against this view's roots and revocations.
    /// Checks signature, chain standing and the purpose's operation caveat before
    /// decoding. Resource caveats are not checked: the application must authorize
    /// the value's resource against the returned credential before using it.
    pub fn open<T: Serialize + DeserializeOwned>(
        &self,
        signed: &Signed<T>,
        purpose: &Purpose,
        now: Timestamp,
    ) -> Result<Opened<T>, SignedError> {
        // User-provided serde runs outside the store lock.
        let (roots, revoked) = {
            let store = self.store();
            (store.roots.clone(), store.revoked.clone())
        };
        signed.open_with(purpose, &roots, &revoked, now)
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
    fn admitting(&self, peer: PublicKey) -> bool {
        self.store()
            .credential(peer, Timestamp::now(), |_| true)
            .is_some()
    }
    fn permitting(
        &self,
        peer: PublicKey,
        now: Timestamp,
        resource: Option<&str>,
        operation: &str,
    ) -> Option<Arc<Credential>> {
        self.store()
            .credential(peer, now, |c| c.permits(resource, operation))
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
        self.0.live.insert(peer.id(), live.clone(), live.watch());
        // Insert BEFORE the re-read: a concurrent change either sees this
        // connection in its snapshot or is visible to this check.
        if !self.admitting(peer.id()) {
            live.close();
            self.0.live.remove(peer.id());
        }
    }
}
impl Revocable for Trust {
    type Change = Change;
    fn apply(&self, change: Change) -> Vec<PublicKey> {
        {
            let mut store = self.store();
            match change {
                Change::Revoke(revocation) => {
                    store
                        .revoked
                        .entry(revocation.certificate())
                        .or_default()
                        .insert(revocation.revoker());
                }
                Change::Roots(roots) => store.roots = roots,
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
        let mut closed = Vec::new();
        for (id, live) in self.0.live.snapshot() {
            if !self.admitting(id) {
                live.close();
                self.0.live.remove(id);
                closed.push(id);
            }
        }
        // TODO: wire the separate Blobs::revoke_peer capability for every
        // affected holder, including inactive ones. `closed` names only live
        // connections; closing RPC alone does not retract prior blob grants.
        closed
    }
}

#[cfg(test)]
mod tests;
