use std::collections::{BTreeMap, BTreeSet};

use iroh::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};

use super::cert::{push_len, CertWire};
use super::sign::{decode_canonical, DecodeError};
use super::{
    Caveats, CertId, Certificate, CertificateError, Delegation, Timestamp, MAX_CERTIFICATE,
    MAX_CHAIN_LINKS,
};

#[derive(Debug, Clone, Default)]
pub(super) struct Revoked {
    pub(super) pending: BTreeMap<CertId, BTreeSet<PublicKey>>,
    accepted: BTreeSet<CertId>,
}
impl Revoked {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(super) fn add(
        &mut self,
        revocation: &super::Revocation,
        authoritative: bool,
    ) -> Result<(), super::ChangeError> {
        let id = revocation.certificate();
        if self.accepted.contains(&id) {
            return Ok(());
        }
        if authoritative {
            self.accepted.insert(id);
            self.pending.remove(&id);
        } else {
            let duplicate = self
                .pending
                .get(&id)
                .is_some_and(|keys| keys.contains(&revocation.revoker()));
            if !duplicate
                && self.pending.values().map(BTreeSet::len).sum::<usize>()
                    >= super::MAX_PENDING_REVOCATIONS
            {
                return Err(super::ChangeError::PendingRevocationsFull {
                    max: super::MAX_PENDING_REVOCATIONS,
                });
            }
            self.pending
                .entry(id)
                .or_default()
                .insert(revocation.revoker());
        }
        Ok(())
    }
    pub(super) fn trust_roots(&mut self, roots: &Roots) {
        self.pending.retain(|id, revokers| {
            if revokers.iter().any(|key| roots.contains(key)) {
                self.accepted.insert(*id);
                false
            } else {
                true
            }
        });
    }
    fn revokes(&mut self, id: CertId, roots: &Roots, upstream: &BTreeSet<PublicKey>) -> bool {
        if self.accepted.contains(&id) {
            return true;
        }
        if self.pending.get(&id).is_some_and(|revokers| {
            revokers
                .iter()
                .any(|key| roots.contains(key) || upstream.contains(key))
        }) {
            self.accepted.insert(id);
            self.pending.remove(&id);
            return true;
        }
        false
    }
}
pub(super) const MAX_CHAIN_BYTES: usize = 1 + MAX_CHAIN_LINKS * (MAX_CERTIFICATE + 74);

/// The non-empty set of keys whose chains are believed. There is no "any root".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Roots(BTreeSet<PublicKey>);
impl Roots {
    /// Refuses an empty set with `ConfigError::EmptyRoots`.
    pub fn only(keys: impl IntoIterator<Item = PublicKey>) -> crate::Result<Self> {
        let keys: BTreeSet<_> = keys.into_iter().collect();
        if keys.is_empty() {
            return Err(crate::ConfigError::EmptyRoots.into());
        }
        Ok(Self(keys))
    }
    /// Whether this key is a trusted root.
    #[must_use]
    pub fn contains(&self, key: &PublicKey) -> bool {
        self.0.contains(key)
    }
}

/// Delegation links in root-first order. Verification decides what they prove.
#[derive(Debug, Clone, Default)]
pub struct Chain(Vec<Certificate>);
impl Chain {
    /// Collect links; structural validation happens at verification or delegation.
    pub fn new(links: Vec<Certificate>) -> Self {
        Self(links)
    }
    /// Root-first links.
    pub fn links(&self) -> &[Certificate] {
        &self.0
    }
    /// The final subject, or `None` for an empty chain.
    pub fn holder(&self) -> Option<PublicKey> {
        self.0.last().map(Certificate::subject)
    }
    /// Issue the next link as the current holder and refuse widening immediately.
    /// Returns `NotTheHolder`, a certificate error, or a structural chain error.
    pub fn delegate(&self, holder: &SecretKey, delegation: Delegation) -> Result<Self, ChainError> {
        if self.holder().is_some_and(|key| key != holder.public()) {
            return Err(ChainError::NotTheHolder);
        }
        let mut links = self.0.clone();
        let at = links.len().min(u8::MAX as usize) as u8;
        links.push(
            Certificate::issue(holder, delegation)
                .map_err(|error| ChainError::Certificate { at, error })?,
        );
        let chain = Self(links);
        structure(&chain)?;
        Ok(chain)
    }
    /// Verify offline, with no revocation knowledge, at an explicit time.
    pub fn verify(&self, roots: &Roots, now: Timestamp) -> Result<Credential, ChainError> {
        let credential = structure(self)?;
        standing(self, roots, &mut Revoked::new(), now)?;
        Ok(credential)
    }
    /// Canonical postcard wire bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_len(&mut bytes, self.0.len());
        for link in &self.0 {
            bytes.extend_from_slice(&link.to_bytes());
        }
        bytes
    }
    pub(super) fn from_wires(wires: Vec<CertWire>) -> Result<Self, ChainError> {
        if wires.len() > MAX_CHAIN_LINKS {
            return Err(ChainError::TooLong {
                links: wires.len() as u32,
                max: MAX_CHAIN_LINKS as u32,
            });
        }
        let links = wires
            .into_iter()
            .enumerate()
            .map(|(at, wire)| {
                Certificate::from_wire(wire).map_err(|error| ChainError::Certificate {
                    at: at as u8,
                    error,
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self(links))
    }
    /// Decode canonical bytes and verify every certificate signature and field.
    /// Does not establish standing under any roots.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ChainError> {
        let wires =
            decode_canonical(bytes, MAX_CHAIN_BYTES).map_err(|e| ChainError::Certificate {
                at: 0,
                error: CertificateError::from(e),
            })?;
        Self::from_wires(wires)
    }
}

/// What a verified chain proves about the principal and the key holding its authority.
#[derive(Debug, Clone)]
pub struct Credential {
    principal: PublicKey,
    holder: PublicKey,
    caveats: Caveats,
    chain: Chain,
}
impl Credential {
    /// The root key the chain starts from.
    pub fn principal(&self) -> PublicKey {
        self.principal
    }
    /// The leaf subject; on a connection, the proven peer.
    pub fn holder(&self) -> PublicKey {
        self.holder
    }
    /// The effective, attenuated caveats.
    pub fn caveats(&self) -> &Caveats {
        &self.caveats
    }
    /// The chain establishing this credential; empty for a direct root.
    pub fn chain(&self) -> &Chain {
        &self.chain
    }
    /// Whether the operation on this resource is permitted. `None` needs `Scope::any`.
    #[must_use]
    pub fn permits(&self, resource: Option<&str>, operation: &str) -> bool {
        resource.map_or_else(
            || self.caveats.resources.is_any(),
            |r| self.caveats.resources.covers(r),
        ) && self.caveats.operations.covers(operation)
    }
    pub(super) fn root(key: PublicKey) -> Self {
        Self {
            principal: key,
            holder: key,
            caveats: Caveats::unlimited(),
            chain: Chain::default(),
        }
    }
}

pub(super) fn structure(chain: &Chain) -> Result<Credential, ChainError> {
    let links = chain.links();
    let first = links.first().ok_or(ChainError::Empty)?;
    if links.len() > MAX_CHAIN_LINKS {
        return Err(ChainError::TooLong {
            links: links.len() as u32,
            max: MAX_CHAIN_LINKS as u32,
        });
    }
    let mut keys = BTreeSet::from([first.issuer()]);
    for (i, link) in links.iter().enumerate() {
        let at = i as u8;
        if i > 0 && link.issuer() != links[i - 1].subject() {
            return Err(ChainError::Broken { at });
        }
        if !keys.insert(link.subject()) {
            return Err(ChainError::RepeatedKey { at });
        }
        if i == 0 {
            continue;
        }
        let parent = &links[i - 1];
        if parent.delegate() == 0 || link.delegate() >= parent.delegate() {
            return Err(ChainError::DepthExceeded { at });
        }
        let child = link.caveats();
        let parent = parent.caveats();
        if !child.resources.subset_of(&parent.resources) {
            return Err(ChainError::Widened {
                at,
                caveat: Widening::Resources,
            });
        }
        if !child.operations.subset_of(&parent.operations) {
            return Err(ChainError::Widened {
                at,
                caveat: Widening::Operations,
            });
        }
        if child.validity.not_before() < parent.validity.not_before()
            || child.validity.not_after() > parent.validity.not_after()
        {
            return Err(ChainError::Widened {
                at,
                caveat: Widening::Validity,
            });
        }
    }
    let leaf = &links[links.len() - 1];
    Ok(Credential {
        principal: first.issuer(),
        holder: leaf.subject(),
        caveats: leaf.caveats().clone(),
        chain: chain.clone(),
    })
}

pub(super) fn standing(
    chain: &Chain,
    roots: &Roots,
    revoked: &mut Revoked,
    now: Timestamp,
) -> Result<(), ChainError> {
    let first = chain.links().first().ok_or(ChainError::Empty)?;
    if !roots.contains(&first.issuer()) {
        return Err(ChainError::UntrustedRoot);
    }
    let mut upstream = BTreeSet::new();
    for (i, link) in chain.links().iter().enumerate() {
        let at = i as u8;
        upstream.insert(link.issuer());
        if now < link.caveats().validity.not_before() {
            return Err(ChainError::NotYetValid { at });
        }
        if now >= link.caveats().validity.not_after() {
            return Err(ChainError::Expired { at });
        }
        if revoked.revokes(link.id(), roots, &upstream) {
            return Err(ChainError::Revoked { at });
        }
    }
    Ok(())
}

/// Which caveat a child widened.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub enum Widening {
    /// Names of resources.
    Resources,
    /// Names of operations.
    Operations,
    /// The validity window.
    Validity,
}

/// Why a delegation chain does not stand. Link indices are zero-based.
#[non_exhaustive]
#[derive(
    Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize, postcard_schema::Schema,
)]
pub enum ChainError {
    /// The total retained-chain budget is full.
    #[error("the retained chain budget of {max} chains is full")]
    StoreFull {
        /// Maximum retained chains.
        max: u32,
    },
    /// The offered chain lost the per-holder retention comparison.
    #[error("the offered chain was not retained")]
    NotRetained,
    /// No links.
    #[error("the chain has no links")]
    Empty,
    /// Chain cap exceeded.
    #[error("{links} links exceeds {max}")]
    TooLong {
        /// Received links.
        links: u32,
        /// Allowed links.
        max: u32,
    },
    /// A certificate did not decode or verify.
    #[error("link {at}: {error}")]
    Certificate {
        /// Link index.
        at: u8,
        /// Certificate fault.
        error: CertificateError,
    },
    /// The first issuer is not trusted.
    #[error("the first issuer is not a trusted root")]
    UntrustedRoot,
    /// The issuer is not the previous subject.
    #[error("link {at} is not issued by the previous subject")]
    Broken {
        /// Link index.
        at: u8,
    },
    /// A key occurs twice.
    #[error("link {at} repeats a key")]
    RepeatedKey {
        /// Link index.
        at: u8,
    },
    /// A child exceeds its parent's depth.
    #[error("link {at} exceeds the delegation depth")]
    DepthExceeded {
        /// Link index.
        at: u8,
    },
    /// A child widens a restriction.
    #[error("link {at} widens its {caveat:?}")]
    Widened {
        /// Link index.
        at: u8,
        /// The widened axis.
        caveat: Widening,
    },
    /// A link's validity has not begun.
    #[error("link {at} is not valid yet")]
    NotYetValid {
        /// Link index.
        at: u8,
    },
    /// A link's validity has ended.
    #[error("link {at} has expired")]
    Expired {
        /// Link index.
        at: u8,
    },
    /// An authorized revoker cut a link.
    #[error("link {at} is revoked")]
    Revoked {
        /// Link index.
        at: u8,
    },
    /// The key is not the final subject.
    #[error("the chain is held by another key")]
    NotTheHolder,
}

impl From<DecodeError> for ChainError {
    fn from(error: DecodeError) -> Self {
        Self::Certificate {
            at: 0,
            error: error.into(),
        }
    }
}
