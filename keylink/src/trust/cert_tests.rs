#![allow(clippy::unwrap_used)]
use super::*;

// Search `    if bytes != canonical {` in the canonical decoder.
// Made red with scripts/break-once.sh replacing it with `    if false {`:
// the properly signed overlong serial decoded as an Ok certificate.
#[test]
fn a_non_canonical_certificate_is_refused() {
    let issuer = SecretKey::from_bytes(&[11; 32]);
    let subject = SecretKey::from_bytes(&[12; 32]);
    let certificate = Certificate::issue(
        &issuer,
        Delegation {
            subject: subject.public(),
            serial: 0,
            delegate: 0,
            caveats: Caveats::unlimited(),
        },
    )
    .unwrap();
    let mut body = certificate.body.clone();
    // Version, issuer, subject occupy 65 bytes; serial zero follows them.
    assert_eq!(body[65], 0);
    body.splice(65..66, [0x80, 0]);
    let signature = issuer.sign(&signing_input(Kind::Certificate, "", &body));
    assert_eq!(
        Certificate::from_bytes(&wire_bytes(&body, &signature)).unwrap_err(),
        CertificateError::NonCanonical
    );
    let mut trailing = certificate.to_bytes();
    trailing.push(0);
    assert_eq!(
        Certificate::from_bytes(&trailing).unwrap_err(),
        CertificateError::NonCanonical
    );
}

#[test]
fn decoded_fields_cannot_bypass_the_constructors() {
    let issuer = SecretKey::from_bytes(&[11; 32]);
    let subject = SecretKey::from_bytes(&[12; 32]);
    let fields = CertV1 {
        issuer: issuer.public(),
        subject: subject.public(),
        serial: 0,
        not_before: Timestamp::from_unix_secs(0),
        not_after: Timestamp::from_unix_secs(1),
        delegate: 0,
        resources: RawScope::Only(vec![]),
        operations: RawScope::Any,
    };
    let body = postcard::to_stdvec(&CertBody::V1(fields)).unwrap();
    let signature = issuer.sign(&signing_input(Kind::Certificate, "", &body));
    assert_eq!(
        Certificate::from_bytes(&wire_bytes(&body, &signature)).unwrap_err(),
        CertificateError::EmptyScope
    );
}

#[test]
fn signatures_are_strict_and_received_bytes_are_verified() {
    let issuer = SecretKey::from_bytes(&[11; 32]);
    let subject = SecretKey::from_bytes(&[12; 32]);
    let certificate = Certificate::issue(
        &issuer,
        Delegation {
            subject: subject.public(),
            serial: 0,
            delegate: 0,
            caveats: Caveats::unlimited(),
        },
    )
    .unwrap();
    let mut wire = certificate.to_bytes();
    let last = wire.len() - 1;
    wire[last] ^= 0x80;
    assert_eq!(
        Certificate::from_bytes(&wire).unwrap_err(),
        CertificateError::BadSignature
    );
}
