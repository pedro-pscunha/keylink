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
    let _closed = trust.apply(Change::Revoke(Revocation::issue(
        &root,
        chain.links()[0].id(),
    )));
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
    let _ = trust.apply(Change::Revoke(Revocation::issue(
        &unrelated,
        chain.links()[0].id(),
    )));
    assert!(trust.verify(&chain, Timestamp::from_unix_secs(1)).is_ok());
    let revocation =
        Revocation::from_bytes(&Revocation::issue(&root, chain.links()[0].id()).to_bytes())
            .unwrap();
    let _ = trust.apply(Change::Revoke(revocation));
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
    let _ = trust.apply(Change::Revoke(Revocation::issue(
        &root,
        chain.links()[0].id(),
    )));
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
