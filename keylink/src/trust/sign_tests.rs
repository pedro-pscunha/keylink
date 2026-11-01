#![allow(clippy::unwrap_used)]
use super::*;

#[test]
fn a_signed_noncanonical_value_is_not_returned() {
    let key = SecretKey::from_bytes(&[6; 32]);
    let purpose = Purpose::new("claim/number").unwrap();
    let roots = Roots::only([key.public()]).unwrap();
    let body = vec![0x80, 0]; // postcard accepts this overlong encoding of zero.
    let signature = key.sign(&application_input(&purpose, &Chain::default(), &body));
    let signed = Signed::<u64> {
        signer: key.public(),
        chain: Chain::default(),
        body,
        signature,
        value: PhantomData,
    };
    let forwarded = Signed::<u64>::from_bytes(&signed.to_bytes()).unwrap();
    assert_eq!(
        forwarded
            .open(&purpose, &roots, Timestamp::from_unix_secs(1))
            .unwrap_err(),
        SignedError::NonCanonical
    );
    let wrong = Purpose::new("claim/other").unwrap();
    // Even an invalid value never gets decoded under a failed signature.
    assert_eq!(
        forwarded
            .open(&wrong, &roots, Timestamp::from_unix_secs(1))
            .unwrap_err(),
        SignedError::BadSignature
    );
}

#[test]
fn signed_envelopes_and_chains_refuse_noncanonical_outer_lengths() {
    let key = SecretKey::from_bytes(&[6; 32]);
    let purpose = Purpose::new("claim/number").unwrap();
    let signed = Signed::sign(&key, &purpose, &1u64, Chain::default()).unwrap();
    let mut wire = signed.to_bytes();
    assert_eq!(wire[32], 0); // empty chain's length
    wire.splice(32..33, [0x80, 0]);
    assert_eq!(
        Signed::<u64>::from_bytes(&wire).unwrap_err(),
        SignedError::NonCanonical
    );
    assert!(matches!(
        Chain::from_bytes(&[0x80, 0]),
        Err(ChainError::Certificate {
            error: super::super::CertificateError::NonCanonical,
            ..
        })
    ));
    let roots = Roots::only([key.public()]).unwrap();
    assert_eq!(
        signed
            .open(&purpose, &roots, Timestamp::from_unix_secs(1))
            .unwrap()
            .value,
        1
    );
}

#[test]
// Search `    for certificate in chain.links() {`.
// Made red with scripts/break-once.sh replacing it with `    for certificate in chain.links().iter().take(0) {`.
// Watched: a renewed chain opened successfully instead of BadSignature.
fn a_forwarder_cannot_substitute_another_authority_chain() {
    use super::super::{Caveats, Certificate, Delegation};
    let root = SecretKey::from_bytes(&[1; 32]);
    let other_root = SecretKey::from_bytes(&[2; 32]);
    let holder = SecretKey::from_bytes(&[3; 32]);
    let grant = |serial| Delegation {
        subject: holder.public(),
        serial,
        delegate: 0,
        caveats: Caveats::unlimited(),
    };
    let original = Chain::new(vec![Certificate::issue(&root, grant(0)).unwrap()]);
    let renewed = Chain::new(vec![Certificate::issue(&root, grant(1)).unwrap()]);
    let another_principal = Chain::new(vec![Certificate::issue(&other_root, grant(0)).unwrap()]);
    let purpose = Purpose::new("claim/read").unwrap();
    let roots = Roots::only([root.public(), other_root.public(), holder.public()]).unwrap();
    let signed = Signed::sign(&holder, &purpose, &7u64, original).unwrap();
    assert_eq!(
        signed
            .open(&purpose, &roots, Timestamp::from_unix_secs(1))
            .unwrap()
            .value,
        7
    );
    for chain in [renewed, another_principal, Chain::default()] {
        let mut substituted = signed.clone();
        substituted.chain = chain;
        let forwarded = Signed::<u64>::from_bytes(&substituted.to_bytes()).unwrap();
        assert_eq!(
            forwarded
                .open(&purpose, &roots, Timestamp::from_unix_secs(1))
                .unwrap_err(),
            SignedError::BadSignature
        );
    }
}

#[test]
// Made red with scripts/break-once.sh by replacing the purpose alphabetic predicate with `b.is_ascii_lowercase()`.
// The multi-line or same-file edit is described rather than quoted.
// Watched: Inventory/GetItem was refused as BadPurpose.
fn generated_operation_names_are_case_sensitive_purposes() {
    let key = SecretKey::from_bytes(&[6; 32]);
    let purpose = Purpose::new("Inventory/GetItem").unwrap();
    let signed = Signed::sign(&key, &purpose, &1u64, Chain::default()).unwrap();
    let roots = Roots::only([key.public()]).unwrap();
    assert_eq!(
        signed
            .open(&purpose, &roots, Timestamp::from_unix_secs(1))
            .unwrap()
            .value,
        1
    );
    assert_eq!(
        signed
            .open(
                &Purpose::new("inventory/getitem").unwrap(),
                &roots,
                Timestamp::from_unix_secs(1)
            )
            .unwrap_err(),
        SignedError::BadSignature
    );
}
