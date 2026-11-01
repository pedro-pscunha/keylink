#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use iroh::SecretKey;
use serde::{Deserialize, Serialize};

fn grant(subject: iroh::PublicKey, resource: &str, delegate: u8) -> Delegation {
    Delegation {
        subject,
        caveats: Caveats {
            resources: Scope::only([Pattern::under(resource).unwrap()]).unwrap(),
            operations: Scope::any(),
            validity: Validity::new(
                Timestamp::from_unix_secs(0),
                Timestamp::from_unix_secs(u64::MAX),
            )
            .unwrap(),
        },
        delegate,
        serial: 0,
    }
}
fn keys() -> (SecretKey, SecretKey, SecretKey) {
    (
        SecretKey::from_bytes(&[1; 32]),
        SecretKey::from_bytes(&[2; 32]),
        SecretKey::from_bytes(&[3; 32]),
    )
}
fn fixture() -> (SecretKey, SecretKey, Chain, Roots) {
    let (root, user, device) = keys();
    let first = Certificate::issue(&root, grant(user.public(), "repo", 1)).unwrap();
    let chain = Chain::new(vec![first])
        .delegate(&user, grant(device.public(), "repo/a", 0))
        .unwrap();
    let roots = Roots::only([root.public()]).unwrap();
    (root, device, chain, roots)
}
#[test]
fn a_two_link_delegation_is_admitted_offline() {
    let (root, device, chain, roots) = fixture();
    let chain = Chain::from_bytes(&chain.to_bytes()).unwrap();
    let credential = chain.verify(&roots, Timestamp::from_unix_secs(1)).unwrap();
    assert_eq!(credential.principal(), root.public());
    assert_eq!(credential.holder(), device.public());
    assert!(credential.permits(Some("repo/a/x"), "Repo/Read"));
    assert!(!credential.permits(Some("repo/b"), "Repo/Read"));
    assert!(!credential.permits(Some("repo/ab"), "Repo/Read"));
    assert!(!credential.permits(None, "Repo/Read"));
}
// Search `        if !child.resources.subset_of(&parent.resources) {`.
// Made red with scripts/break-once.sh replacing it with `        if false {`:
// delegate returned an Ok widened chain instead of Widened(Resources).
#[test]
fn a_widened_caveat_is_refused() {
    let (root, user, device) = keys();
    let parent = Certificate::issue(&root, grant(user.public(), "repo/a", 1)).unwrap();
    let chain = Chain::new(vec![parent.clone()]);
    let error = ChainError::Widened {
        at: 1,
        caveat: Widening::Resources,
    };
    assert_eq!(
        chain
            .delegate(&user, grant(device.public(), "repo", 0))
            .unwrap_err(),
        error
    );
    let widened = Certificate::issue(&user, grant(device.public(), "repo", 0)).unwrap();
    assert_eq!(
        Chain::new(vec![parent, widened])
            .verify(
                &Roots::only([root.public()]).unwrap(),
                Timestamp::from_unix_secs(1)
            )
            .unwrap_err(),
        error
    );
    assert_eq!(
        chain
            .delegate(&user, grant(device.public(), "repo/a", 1))
            .unwrap_err(),
        ChainError::DepthExceeded { at: 1 }
    );
}
#[test]
fn an_expired_certificate_is_refused() {
    let (root, user, _) = keys();
    let mut delegation = grant(user.public(), "repo", 0);
    delegation.caveats.validity = Validity::new(
        Timestamp::from_unix_secs(10),
        Timestamp::from_unix_secs(1000),
    )
    .unwrap();
    let chain = Chain::new(vec![Certificate::issue(&root, delegation).unwrap()]);
    let roots = Roots::only([root.public()]).unwrap();
    assert!(chain.verify(&roots, Timestamp::from_unix_secs(999)).is_ok());
    assert_eq!(
        chain
            .verify(&roots, Timestamp::from_unix_secs(1000))
            .unwrap_err(),
        ChainError::Expired { at: 0 }
    );
    assert_eq!(
        chain
            .verify(&roots, Timestamp::from_unix_secs(9))
            .unwrap_err(),
        ChainError::NotYetValid { at: 0 }
    );
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Claim {
    text: String,
    serial: u64,
}
#[test]
fn a_signed_claim_verifies_for_a_third_party_and_not_under_another_purpose() {
    let (root, device, chain, roots) = fixture();
    let purpose = Purpose::new("repo/read").unwrap();
    let other = Purpose::new("repo/write").unwrap();
    let claim = Claim {
        text: "forwarded".into(),
        serial: 1,
    };
    let signed = Signed::sign(&device, &purpose, &claim, chain.clone()).unwrap();
    let forwarded = Signed::<Claim>::from_bytes(&signed.to_bytes()).unwrap();
    let opened = forwarded
        .open(&purpose, &roots, Timestamp::from_unix_secs(1))
        .unwrap();
    assert_eq!(opened.value, claim);
    assert_eq!(opened.credential.principal(), root.public());
    assert_eq!(
        forwarded
            .open(&other, &roots, Timestamp::from_unix_secs(1))
            .unwrap_err(),
        SignedError::BadSignature
    );
    let (_, user, _) = keys();
    let mut restricted = grant(device.public(), "repo/a", 0);
    restricted.caveats.operations =
        Scope::only([Pattern::exact("something/else").unwrap()]).unwrap();
    let restricted = Chain::new(vec![chain.links()[0].clone()])
        .delegate(&user, restricted)
        .unwrap();
    let signed = Signed::sign(&device, &purpose, &claim, restricted).unwrap();
    assert_eq!(
        signed
            .open(&purpose, &roots, Timestamp::from_unix_secs(1))
            .unwrap_err(),
        SignedError::NotPermitted
    );
    let trust = Trust::new(roots);
    trust.insert(chain.clone()).unwrap();
    let _closed = trust
        .apply(Change::Revoke(Revocation::issue(
            &root,
            chain.links()[0].id(),
        )))
        .unwrap();
    assert_eq!(
        trust
            .open(&forwarded, &purpose, Timestamp::from_unix_secs(1))
            .unwrap_err(),
        SignedError::Chain(ChainError::Revoked { at: 0 })
    );
}
// Search `    bytes.push(kind as u8);`.
// Made red with scripts/break-once.sh replacing it with `    let _ = kind;`:
// the byte-exact assertion failed, even though signing and opening changed together.
#[test]
fn the_signing_input_is_byte_exact() {
    use super::sign::{signing_input, Kind};
    assert_eq!(
        signing_input(Kind::Certificate, "", b"x"),
        b"keylink-sig-v1\x01\x00x"
    );
    assert_eq!(
        signing_input(Kind::Application, "p", b""),
        b"keylink-sig-v1\x03\x01p"
    );
    assert_ne!(
        signing_input(Kind::Certificate, "", b"x"),
        signing_input(Kind::Revocation, "", b"x")
    );
}
#[test]
fn revocations_are_authorized_at_evaluation_even_when_received_first() {
    let (root, _, chain, roots) = fixture();
    let trust = Trust::new(roots);
    let unrelated = SecretKey::from_bytes(&[9; 32]);
    let _ = trust
        .apply(Change::Revoke(Revocation::issue(
            &unrelated,
            chain.links()[0].id(),
        )))
        .unwrap();
    assert!(trust.verify(&chain, Timestamp::from_unix_secs(1)).is_ok());
    let revocation =
        Revocation::from_bytes(&Revocation::issue(&root, chain.links()[0].id()).to_bytes())
            .unwrap();
    let _ = trust.apply(Change::Revoke(revocation)).unwrap();
    assert_eq!(
        trust
            .verify(&chain, Timestamp::from_unix_secs(1))
            .unwrap_err(),
        ChainError::Revoked { at: 0 }
    );
    assert_eq!(
        trust.insert(chain).unwrap_err(),
        ChainError::Revoked { at: 0 }
    );
}
// Search `        if !self.admitting(peer.id()) {`.
// Made red with scripts/break-once.sh replacing it with `        if false {`:
// the admitted-during-revocation connection stayed in live (1 instead of 0).
#[tokio::test]
async fn a_connection_admitted_during_a_revocation_is_still_closed() {
    let (root, device, chain, roots) = fixture();
    let trust = Trust::new(roots);
    trust.insert(chain.clone()).unwrap();
    let server = crate::Node::bind_local().await.unwrap();
    let client = crate::Node::bind_local_with_secret(device).await.unwrap();
    server.endpoint().set_alpns(vec![b"trust-race".to_vec()]);
    let dial = client
        .endpoint()
        .connect(server.endpoint().addr(), b"trust-race");
    let accept = async { server.endpoint().accept().await.unwrap().await.unwrap() };
    let (dialed, accepted) = tokio::join!(dial, accept);
    let dialed = dialed.unwrap();
    let proven = ProvenPeer::on(&accepted);
    assert_eq!(trust.admits(&proven), Admittance::Admitted);
    let _ = trust
        .apply(Change::Revoke(Revocation::issue(
            &root,
            chain.links()[0].id(),
        )))
        .unwrap();
    trust.admitted(
        &AdmittedPeer::mint(proven),
        LiveConnection::new(accepted, proven),
    );
    assert_eq!(trust.live(), 0);
    let error = dialed.closed().await;
    assert!(
        matches!(error, iroh::endpoint::ConnectionError::ApplicationClosed(frame) if frame.error_code == crate::admission::REFUSED.into())
    );
    assert_eq!(trust.live(), 0);
}

#[test]
// Search `    pub(super) fn trust_roots(&mut self, roots: &Roots) -> Result<(), super::ChangeError> {`.
// Made red with scripts/break-once.sh replacing it with `    pub(super) fn trust_roots(&mut self, roots: &Roots) -> Result<(), super::ChangeError> {
//         self.accepted.clear();`.
// Watched: removing the revoker root resurrected the previously revoked chain.
fn removing_a_root_never_resurrects_an_accepted_revocation() {
    let (root, _, chain, _) = fixture();
    let revoker = SecretKey::from_bytes(&[8; 32]);
    let trust = Trust::new(Roots::only([root.public(), revoker.public()]).unwrap());
    trust.insert(chain.clone()).unwrap();
    let _ = trust
        .apply(Change::Revoke(Revocation::issue(
            &revoker,
            chain.links()[0].id(),
        )))
        .unwrap();
    assert_eq!(
        trust
            .verify(&chain, Timestamp::from_unix_secs(1))
            .unwrap_err(),
        ChainError::Revoked { at: 0 }
    );
    let _ = trust
        .apply(Change::Roots(Roots::only([root.public()]).unwrap()))
        .unwrap();
    assert_eq!(
        trust
            .verify(&chain, Timestamp::from_unix_secs(1))
            .unwrap_err(),
        ChainError::Revoked { at: 0 }
    );
    // Root-authorized revocations are accepted even before their certificate arrives.
    let early = Trust::new(Roots::only([root.public(), revoker.public()]).unwrap());
    let _ = early
        .apply(Change::Revoke(Revocation::issue(
            &revoker,
            chain.links()[0].id(),
        )))
        .unwrap();
    let _ = early
        .apply(Change::Roots(Roots::only([root.public()]).unwrap()))
        .unwrap();
    assert_eq!(
        early.insert(chain).unwrap_err(),
        ChainError::Revoked { at: 0 }
    );
}

#[test]
// Made red with scripts/break-once.sh by replacing retention_order with the expiry-only comparator, without its certificate-ID tie-breaker.
// The multi-line or same-file edit is described rather than quoted.
// Watched: opposite feed orders retained different certificate IDs.
fn equal_expiry_retention_is_independent_of_feed_order() {
    let (root, _, device) = keys();
    let make = || Trust::new(Roots::only([root.public()]).unwrap());
    let a = make();
    let b = make();
    let chains: Vec<_> = (0..(MAX_CHAINS_PER_HOLDER + 4) as u64)
        .map(|serial| {
            let mut delegation = grant(device.public(), "repo", 0);
            delegation.serial = serial;
            Chain::new(vec![Certificate::issue(&root, delegation).unwrap()])
        })
        .collect();
    for chain in &chains {
        let _ = a.insert(chain.clone());
    }
    for chain in chains.iter().rev() {
        let _ = b.insert(chain.clone());
    }
    let ids = |trust: &Trust| {
        trust.store().chains[&device.public()]
            .iter()
            .map(|c| c.chain().links()[0].id())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&a), ids(&b));
    assert_eq!(ids(&a).len(), MAX_CHAINS_PER_HOLDER);
}

fn budget_key(index: usize) -> SecretKey {
    let mut bytes = [9; 32];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    SecretKey::from_bytes(&bytes)
}
#[test]
// Search `            if total >= MAX_RETAINED_CHAINS`.
// Made red with scripts/break-once.sh replacing it with `        if total > MAX_RETAINED_CHAINS`.
// Watched: one additional holder was accepted instead of StoreFull.
fn trust_store_budgets_bound_retained_chains() {
    let (root, _, _, roots) = fixture();
    let trust = Trust::new(roots);
    let chain = |index| {
        Chain::new(vec![Certificate::issue(
            &root,
            grant(budget_key(index).public(), "repo", 0),
        )
        .unwrap()])
    };
    let first = chain(0);
    for index in 0..MAX_RETAINED_CHAINS {
        trust.insert(chain(index)).unwrap();
    }
    trust.insert(first).unwrap(); // an identical retained record costs no place
    assert_eq!(
        trust.insert(chain(MAX_RETAINED_CHAINS)).unwrap_err(),
        ChainError::StoreFull {
            max: MAX_RETAINED_CHAINS as u32
        }
    );
    assert_eq!(
        trust.store().chains.values().map(Vec::len).sum::<usize>(),
        MAX_RETAINED_CHAINS
    );
}
#[test]
// Search `                >= super::MAX_PENDING_REVOCATIONS`.
// Made red with scripts/break-once.sh replacing it with `                    > super::MAX_PENDING_REVOCATIONS`.
// Watched: one additional pending revocation was accepted instead of a typed capacity error.
fn trust_store_budgets_bound_pending_records_and_never_drop_tombstones() {
    let (root, _, chain, roots) = fixture();
    let trust = Trust::new(roots);
    let id = chain.links()[0].id();
    for index in 0..MAX_PENDING_REVOCATIONS {
        trust
            .apply(Change::Revoke(Revocation::issue(&budget_key(index), id)))
            .unwrap();
    }
    trust
        .apply(Change::Revoke(Revocation::issue(&budget_key(0), id)))
        .unwrap();
    assert_eq!(
        trust
            .apply(Change::Revoke(Revocation::issue(
                &budget_key(MAX_PENDING_REVOCATIONS),
                id
            )))
            .unwrap_err(),
        ChangeError::PendingRevocationsFull {
            max: MAX_PENDING_REVOCATIONS
        }
    );
    assert_eq!(
        trust
            .store()
            .revoked
            .pending
            .values()
            .map(std::collections::BTreeSet::len)
            .sum::<usize>(),
        MAX_PENDING_REVOCATIONS
    );
    trust
        .apply(Change::Revoke(Revocation::issue(&root, id)))
        .unwrap();
    assert!(trust.store().revoked.pending.is_empty());
    assert_eq!(
        trust.insert(chain).unwrap_err(),
        ChainError::Revoked { at: 0 }
    );
}

#[test]
// Search `                || self.usage.get(key).copied().unwrap_or(0) >= super::MAX_ACCEPTED_PER_REVOKER`.
// Made red with scripts/break-once.sh replacing it with `                || self.usage.get(key).copied().unwrap_or(0) > super::MAX_ACCEPTED_PER_REVOKER`.
// Watched: one additional tombstone was accepted rather than exhausting the revoker.
fn revoked_leaf_churn_exhausts_its_revoker_without_retaining_the_leaves() {
    let (root, delegate, holder) = keys();
    let parent = Certificate::issue(&root, grant(delegate.public(), "repo", 1)).unwrap();
    let trust = Trust::new(Roots::only([root.public()]).unwrap());
    for serial in 0..=MAX_ACCEPTED_PER_REVOKER as u64 {
        let mut delegation = grant(holder.public(), "repo/a", 0);
        delegation.serial = serial;
        let leaf = Certificate::issue(&delegate, delegation).unwrap();
        trust
            .apply(Change::Revoke(Revocation::issue(&delegate, leaf.id())))
            .unwrap();
        let error = trust
            .insert(Chain::new(vec![parent.clone(), leaf]))
            .unwrap_err();
        if serial < MAX_ACCEPTED_PER_REVOKER as u64 {
            assert_eq!(error, ChainError::Revoked { at: 1 });
        } else {
            assert_eq!(
                error,
                ChainError::RevokerExhausted {
                    revoker: *delegate.public().as_bytes()
                }
            );
        }
    }
    let store = trust.store();
    assert_eq!(store.revoked.revision(), (MAX_ACCEPTED_PER_REVOKER, 1));
    assert_eq!(store.revoked.accounted_keys(), 2);
    assert!(store.chains.is_empty());
    assert!(store.revoked.is_exhausted(&delegate.public()));
    assert!(!store.revoked.is_exhausted(&root.public()));
    drop(store);
    let sibling = Chain::new(vec![Certificate::issue(
        &root,
        grant(budget_key(9000).public(), "repo/b", 0),
    )
    .unwrap()]);
    assert!(trust.verify(&sibling, Timestamp::now()).is_ok());
}

#[test]
// Search `            .chain(path.iter().rev().copied())`.
// Made red with scripts/break-once.sh replacing it with `            .chain(std::iter::empty())`.
// Watched: fresh revoker keys bypassed the common issuer budget.
fn fresh_revoker_keys_cannot_bypass_the_common_issuer_budget() {
    let (root, delegate, holder) = keys();
    let parent = Certificate::issue(&root, grant(delegate.public(), "repo", 2)).unwrap();
    let trust = Trust::new(Roots::only([root.public()]).unwrap());
    for index in 0..MAX_ACCEPTED_PER_REVOKER + 32 {
        let revoker = budget_key(10000 + index);
        let intermediate =
            Certificate::issue(&delegate, grant(revoker.public(), "repo", 1)).unwrap();
        let leaf = Certificate::issue(&revoker, grant(holder.public(), "repo/a", 0)).unwrap();
        trust
            .apply(Change::Revoke(Revocation::issue(&revoker, leaf.id())))
            .unwrap();
        let error = trust
            .insert(Chain::new(vec![parent.clone(), intermediate, leaf]))
            .unwrap_err();
        if index < MAX_ACCEPTED_PER_REVOKER {
            assert_eq!(error, ChainError::Revoked { at: 2 });
        } else {
            assert_eq!(
                error,
                ChainError::RevokerExhausted {
                    revoker: *delegate.public().as_bytes()
                }
            );
        }
    }
    let store = trust.store();
    assert_eq!(store.revoked.revision(), (MAX_ACCEPTED_PER_REVOKER, 1));
    assert_eq!(store.revoked.accounted_keys(), MAX_ACCEPTED_PER_REVOKER + 2);
    assert!(
        store
            .revoked
            .pending
            .values()
            .map(std::collections::BTreeSet::len)
            .sum::<usize>()
            <= MAX_PENDING_REVOCATIONS
    );
    assert!(store.chains.is_empty());
}

#[test]
// Search `            if revoked.is_exhausted(&self.signer) {`.
// Made red with scripts/break-once.sh replacing it with `            if false {`.
// Watched: an exhausted root could still open signed claims.
// Search `        if self.roots.contains(&peer) && !self.revoked.is_exhausted(&peer) {`.
// Made red with scripts/break-once.sh replacing it with `        if self.roots.contains(&peer) {`.
// Watched: an exhausted root was still admitted directly.
fn direct_and_root_promoted_budgets_never_evict_accepted_tombstones() {
    let (root, _, holder) = keys();
    let revoker = budget_key(20000);
    let roots = Roots::only([root.public(), revoker.public()]).unwrap();
    let purpose = Purpose::new("claims/read").unwrap();
    let signed = Signed::sign(&revoker, &purpose, &7u64, Chain::default()).unwrap();
    for promote in [false, true] {
        let trust = Trust::new(if promote {
            Roots::only([root.public()]).unwrap()
        } else {
            roots.clone()
        });
        let mut certificates = Vec::new();
        for serial in 0..=MAX_ACCEPTED_PER_REVOKER as u64 {
            let mut delegation = grant(holder.public(), "repo/a", 0);
            delegation.serial = serial;
            let certificate = Certificate::issue(&root, delegation).unwrap();
            let result = trust.apply(Change::Revoke(Revocation::issue(
                &revoker,
                certificate.id(),
            )));
            if !promote && serial == MAX_ACCEPTED_PER_REVOKER as u64 {
                assert_eq!(
                    result.unwrap_err(),
                    ChangeError::RevokerExhausted {
                        revoker: revoker.public(),
                        max: MAX_ACCEPTED_PER_REVOKER
                    }
                );
            } else {
                result.unwrap();
            }
            certificates.push(certificate);
        }
        if promote {
            assert_eq!(
                trust.apply(Change::Roots(roots.clone())).unwrap_err(),
                ChangeError::RevokerExhausted {
                    revoker: revoker.public(),
                    max: MAX_ACCEPTED_PER_REVOKER
                }
            );
        }
        assert_eq!(
            trust.store().revoked.revision(),
            (MAX_ACCEPTED_PER_REVOKER, 1)
        );
        assert!(!trust.admitting(revoker.public()));
        assert!(trust.admitting(root.public()));
        assert!(matches!(
            trust.open(&signed, &purpose, Timestamp::now()),
            Err(SignedError::Chain(ChainError::RevokerExhausted { .. }))
        ));
        trust
            .apply(Change::Roots(Roots::only([root.public()]).unwrap()))
            .unwrap();
        if promote {
            certificates.sort_by_key(Certificate::id);
        }
        let first = Chain::new(vec![certificates[0].clone()]);
        assert_eq!(
            trust.verify(&first, Timestamp::now()).unwrap_err(),
            ChainError::Revoked { at: 0 }
        );
        let last = Chain::new(vec![certificates[MAX_ACCEPTED_PER_REVOKER].clone()]);
        assert!(trust.verify(&last, Timestamp::now()).is_ok());
    }
}

#[test]
// Search `                        refusal.get_or_insert(super::ChangeError::RevokerExhausted {`.
// Made red with scripts/break-once.sh replacing it with `                        return Err(super::ChangeError::RevokerExhausted { revoker, max: super::MAX_ACCEPTED_PER_REVOKER });
//                         refusal.get_or_insert(super::ChangeError::RevokerExhausted {`.
// Watched: the second root's promotions were deferred instead of establishing the whole new view before sweeping.
fn one_exhausted_root_does_not_defer_other_pending_root_promotions() {
    let (root, _, holder) = keys();
    let a = budget_key(100000);
    let b = budget_key(100001);
    let trust = Trust::new(Roots::only([root.public()]).unwrap());
    let mut certificates: Vec<_> = (0..2 * (MAX_ACCEPTED_PER_REVOKER + 1))
        .map(|serial| {
            let mut delegation = grant(holder.public(), "repo/a", 0);
            delegation.serial = serial as u64;
            Certificate::issue(&root, delegation).unwrap()
        })
        .collect();
    certificates.sort_by_key(Certificate::id);
    for (index, certificate) in certificates.iter().enumerate() {
        let revoker = if index <= MAX_ACCEPTED_PER_REVOKER {
            &a
        } else {
            &b
        };
        trust
            .apply(Change::Revoke(Revocation::issue(revoker, certificate.id())))
            .unwrap();
    }
    assert!(matches!(
        trust.apply(Change::Roots(
            Roots::only([root.public(), a.public(), b.public()]).unwrap()
        )),
        Err(ChangeError::RevokerExhausted { .. })
    ));
    assert_eq!(
        trust.store().revoked.revision(),
        (2 * MAX_ACCEPTED_PER_REVOKER, 2)
    );
    assert!(!trust.admitting(a.public()));
    assert!(!trust.admitting(b.public()));
    assert!(trust.admitting(root.public()));
}

#[test]
// Search `            .map_or(path, |end| &path[..end]);`.
// Made red with scripts/break-once.sh replacing it with `            .map_or(path, |_| path);`.
// Watched: an upstream revocation exhausted a downstream issuer by charging beyond the signer.
fn an_upstream_revocation_does_not_charge_a_downstream_issuers_budget() {
    let (root, delegate, issuer) = keys();
    let holder = budget_key(30000);
    let trust = Trust::new(Roots::only([root.public(), issuer.public()]).unwrap());
    for serial in 1000..1000 + MAX_ACCEPTED_PER_REVOKER as u64 {
        let mut delegation = grant(holder.public(), "repo/b", 0);
        delegation.serial = serial;
        let certificate = Certificate::issue(&root, delegation).unwrap();
        trust
            .apply(Change::Revoke(Revocation::issue(&issuer, certificate.id())))
            .unwrap();
    }
    // The downstream issuer has used its own root budget, but has not attempted
    // another acceptance. Its upstream delegate must not exhaust it by proxy.
    let leaf = Certificate::issue(&issuer, grant(holder.public(), "repo/a", 0)).unwrap();
    trust
        .apply(Change::Revoke(Revocation::issue(&delegate, leaf.id())))
        .unwrap();
    let chain = Chain::new(vec![
        Certificate::issue(&root, grant(delegate.public(), "repo", 2)).unwrap(),
        Certificate::issue(&delegate, grant(issuer.public(), "repo", 1)).unwrap(),
        leaf,
    ]);
    assert_eq!(
        trust.verify(&chain, Timestamp::now()).unwrap_err(),
        ChainError::Revoked { at: 2 }
    );
    assert!(trust.admitting(issuer.public()));
    assert!(!trust.store().revoked.is_exhausted(&issuer.public()));
    assert_eq!(
        trust.store().revoked.revision().0,
        MAX_ACCEPTED_PER_REVOKER + 1
    );
}

// Native sockets and a subprocess watchdog are intentionally not browser work.
#[cfg(not(target_arch = "wasm32"))]
mod lock_order {
    use super::*;
    use crate::{Context, Grant, Node, SeqId, Server, Wire};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    crate::service! {
        service ConcurrentShelf;
        blob Read() -> u32;
    }
    struct ConcurrentShelf(SeqId);
    impl concurrent_shelf::Handler for ConcurrentShelf {
        async fn read(&self, _cx: &Context) -> (u32, Grant) {
            (2, self.0.into())
        }
    }
    const REVERSE_LOCK_ORDER: bool = false;

    // Made red with scripts/break-once.sh by enabling this module's reverse-order
    // injection. Two threads hold the actual trust and blob ledger mutexes, meet
    // at a barrier, then each requests the other's mutex. The parent's Tokio
    // watchdog fails and kills only its own child, so runtime shutdown cannot
    // turn the intentionally deadlocked blocking threads into a stuck gate.
    // Watched: trust/blob lock-order watchdog expired.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn trust_and_blob_revocation_stress_has_a_bounded_deadlock_watchdog() {
        const CHILD: &str = "KEYLINK_TRUST_LOCK_STRESS_CHILD";
        if std::env::var_os(CHILD).is_some() {
            tokio::time::timeout(Duration::from_secs(20), exercise())
                .await
                .expect("trust/blob workload timed out");
            return;
        }
        let log = std::env::temp_dir().join(format!(
            "keylink-trust-lock-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&log)
            .unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .arg("trust_and_blob_revocation_stress_has_a_bounded_deadlock_watchdog")
            .arg("--nocapture")
            .env(CHILD, "1")
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let status = tokio::time::timeout(Duration::from_secs(30), child.wait()).await;
        match status {
            Ok(status) => assert!(
                status.unwrap().success(),
                "stress child failed: {}",
                std::fs::read_to_string(&log).unwrap()
            ),
            Err(_) => {
                child.kill().await.unwrap();
                panic!(
                    "trust/blob lock-order watchdog expired; child log: {}",
                    log.display()
                );
            }
        }
        let output = std::fs::read_to_string(&log).unwrap();
        assert!(
            output.contains("closure fetch attempts="),
            "child did not finish its workload: {output}"
        );
        eprintln!("{output}");
        std::fs::remove_file(log).unwrap();
    }

    async fn exercise() {
        let root = SecretKey::from_bytes(&[81; 32]);
        let other = SecretKey::from_bytes(&[82; 32]);
        let blobs = Blobs::memory();
        let trust = Trust::new(Roots::only([root.public(), other.public()]).unwrap())
            .with_blobs(&blobs)
            .unwrap();
        if REVERSE_LOCK_ORDER {
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let (a, b) = (trust.clone(), trust.clone());
            let (x, y) = (blobs.clone(), blobs.clone());
            let rendezvous = barrier.clone();
            let forward = std::thread::spawn(move || {
                let _trust = a.store();
                rendezvous.wait();
                x.revoke_peer(root.public());
            });
            let reverse = std::thread::spawn(move || {
                let _ledger = y.hold_grant_ledger();
                barrier.wait();
                let _trust = b.store();
            });
            tokio::task::spawn_blocking(move || {
                forward.join().unwrap();
                reverse.join().unwrap();
            })
            .await
            .unwrap();
            unreachable!();
        }
        let node = Node::bind_local().await.unwrap();
        let a = blobs.add_bytes(b"member a".as_slice()).await.unwrap();
        let b = blobs.add_bytes(b"member b".as_slice()).await.unwrap();
        let seq = blobs.add_seq([a, b]).await.unwrap();
        let service = trust
            .guard::<concurrent_shelf::Request, _>(WithinCaveats)
            .resource(|_| Some(std::borrow::Cow::Borrowed("repo/a")))
            .install(|allow| concurrent_shelf::service(ConcurrentShelf(seq), allow))
            .with_blobs(&blobs)
            .build();
        let _running = Server::builder(&node)
            .service(service)
            .service(trust.presentation().build())
            .spawn()
            .unwrap();
        let mut peers = Vec::new();
        let mut ids = Vec::new();
        for index in 0..4u8 {
            let secret = SecretKey::from_bytes(&[90 + index; 32]);
            let peer = Node::bind_local_with_secret(secret.clone()).await.unwrap();
            let keeper = Chain::new(vec![Certificate::issue(
                &root,
                grant(secret.public(), "repo", 0),
            )
            .unwrap()]);
            let extra = Chain::new(vec![Certificate::issue(
                &other,
                grant(secret.public(), "repo", 0),
            )
            .unwrap()]);
            trust.insert(keeper.clone()).unwrap();
            trust.insert(extra).unwrap();
            let client = concurrent_shelf::Client::connect(&peer, node.endpoint().addr())
                .await
                .unwrap();
            let (_, handle) = client.read().await.unwrap();
            handle.fetch_into(&Blobs::memory()).await.unwrap();
            client.close().await.unwrap();
            ids.push(peer.id());
            peers.push((peer, keeper, handle));
        }
        let unused = Certificate::issue(
            &root,
            grant(SecretKey::from_bytes(&[110; 32]).public(), "repo/unused", 0),
        )
        .unwrap();
        let revocation = Revocation::issue(&root, unused.id());
        let counts = Arc::new([
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ]);
        let start = Arc::new(tokio::sync::Barrier::new(6));
        let until = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut tasks = tokio::task::JoinSet::new();
        let (mutating, progress, ready) = (trust.clone(), counts.clone(), start.clone());
        let both = Roots::only([root.public(), other.public()]).unwrap();
        let keeper = Roots::only([root.public()]).unwrap();
        tasks.spawn(async move {
            ready.wait().await;
            while tokio::time::Instant::now() < until {
                mutating.apply(Change::Roots(both.clone())).unwrap();
                mutating.apply(Change::Revoke(revocation.clone())).unwrap();
                mutating.apply(Change::Roots(keeper.clone())).unwrap();
                progress[0].fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });
        let (revoking, progress, ready) = (blobs.clone(), counts.clone(), start.clone());
        tasks.spawn(async move {
            ready.wait().await;
            while tokio::time::Instant::now() < until {
                for id in &ids {
                    revoking.revoke_peer(*id);
                }
                progress[1].fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });
        for (peer, chain, old) in peers {
            let (progress, ready, addr) = (counts.clone(), start.clone(), node.endpoint().addr());
            tasks.spawn(async move {
                ready.wait().await;
                while tokio::time::Instant::now() < until {
                    assert!(matches!(
                        super::super::present(&peer, addr.clone(), &chain)
                            .await
                            .unwrap(),
                        Presented::Held { .. }
                    ));
                    let client = concurrent_shelf::Client::connect(&peer, addr.clone())
                        .await
                        .unwrap();
                    progress[2].fetch_add(1, Ordering::Relaxed);
                    let handle = match client.read().await {
                        Ok((2, handle)) => handle,
                        Err(crate::Error::Remote(crate::RemoteError::GrantsWithdrawn)) => {
                            old.clone()
                        }
                        answer => panic!("unexpected admitted call: {answer:?}"),
                    };
                    // A fresh store forces a real closure request, never a cache hit.
                    // Revocation may refuse it or close a transfer already in flight.
                    let _ = handle.fetch_into(&Blobs::memory()).await;
                    progress[3].fetch_add(1, Ordering::Relaxed);
                    client.close().await.unwrap();
                }
            });
        }
        while let Some(task) = tasks.join_next().await {
            task.unwrap();
        }
        assert!(counts.iter().all(|count| count.load(Ordering::Relaxed) > 0));
        eprintln!(
            "trust changes={}, direct revocations={}, admissions={}, closure fetch attempts={}",
            counts[0].load(Ordering::Relaxed),
            counts[1].load(Ordering::Relaxed),
            counts[2].load(Ordering::Relaxed),
            counts[3].load(Ordering::Relaxed)
        );
    }
}
