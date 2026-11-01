#![allow(clippy::unwrap_used)]
use super::*;

#[test]
fn a_signed_noncanonical_value_is_not_returned() {
    let key = SecretKey::from_bytes(&[6; 32]);
    let purpose = Purpose::new("claim/number").unwrap();
    let roots = Roots::only([key.public()]).unwrap();
    let body = vec![0x80, 0]; // postcard accepts this overlong encoding of zero.
    let signature = key.sign(&signing_input(Kind::Application, purpose.0, &body));
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
