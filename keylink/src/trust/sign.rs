use std::marker::PhantomData;

use iroh::{PublicKey, SecretKey, Signature};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::cert::{push_len, CertWire};
use super::chain::{standing, structure, Revoked, MAX_CHAIN_BYTES};
use super::{Chain, ChainError, Credential, Roots, Timestamp, MAX_SIGNED};

const MAGIC: &[u8; 14] = b"keylink-sig-v1";
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Certificate = 1,
    Revocation = 2,
    Application = 3,
}
pub(super) fn signing_input(kind: Kind, purpose: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(MAGIC.len() + 2 + purpose.len() + body.len());
    bytes.extend_from_slice(MAGIC);
    bytes.push(kind as u8);
    bytes.push(purpose.len() as u8);
    bytes.extend_from_slice(purpose.as_bytes());
    bytes.extend_from_slice(body);
    bytes
}

pub(super) enum DecodeError {
    Malformed,
    NonCanonical,
    TooLarge { len: u32, max: u32 },
}
pub(super) fn decode_canonical<T: Serialize + DeserializeOwned>(
    bytes: &[u8],
    max: usize,
) -> Result<T, DecodeError> {
    if bytes.len() > max {
        return Err(DecodeError::TooLarge {
            len: bytes.len().min(u32::MAX as usize) as u32,
            max: max as u32,
        });
    }
    let value = postcard::from_bytes(bytes).map_err(|_| DecodeError::Malformed)?;
    let canonical = postcard::to_stdvec(&value).map_err(|_| DecodeError::Malformed)?;
    if bytes != canonical {
        return Err(DecodeError::NonCanonical);
    }
    Ok(value)
}

/// An application-chosen signed-message purpose and operation name.
/// Names contain 1..=64 bytes of `[A-Za-z0-9._/-]`, without an edge slash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Purpose(&'static str);
impl Purpose {
    /// Refuses an invalid name with `BadPurpose`.
    pub fn new(name: &'static str) -> Result<Self, SignedError> {
        if name.is_empty()
            || name.len() > 64
            || name.starts_with('/')
            || name.ends_with('/')
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphabetic() || b.is_ascii_digit() || b"._/-".contains(&b))
        {
            return Err(SignedError::BadPurpose);
        }
        Ok(Self(name))
    }
}

// Application signatures commit to the selected authority, not just its holder.
// The chain cap makes a single-byte count unambiguous, followed by ordered ids.
fn application_input(purpose: &Purpose, chain: &Chain, body: &[u8]) -> Vec<u8> {
    let mut committed = Vec::with_capacity(1 + chain.links().len() * 32 + body.len());
    committed.push(chain.links().len() as u8);
    for certificate in chain.links() {
        committed.extend_from_slice(certificate.id().as_bytes());
    }
    committed.extend_from_slice(body);
    signing_input(Kind::Application, purpose.0, &committed)
}

#[derive(Serialize, Deserialize)]
struct SignedWire {
    signer: PublicKey,
    chain: Vec<CertWire>,
    body: Vec<u8>,
    signature: Signature,
}

/// A forwardable signed value, bound to an externally asserted purpose.
///
/// `open` checks the signature before decoding the value. Values must round-trip
/// through postcard byte for byte; a nondeterministically ordered `HashMap` may
/// be refused as `NonCanonical`. The value's schema is not in a contract hash.
/// `MAX_SIGNED` bounds encoded bytes, not decoded allocation or work. `T` must
/// not contain collections of elements that encode to zero bytes (for example,
/// an element whose fields are all `#[serde(skip)]`). A tiny encoded length can
/// otherwise construct an enormous collection before canonical checking. Use
/// explicitly bounded application wire types; a valid signature is not a decode budget.
///
/// The signature commits to the ordered certificate IDs of the carried chain.
/// Freshness and replay prevention belong in the application value.
#[derive(Debug, Clone)]
pub struct Signed<T> {
    signer: PublicKey,
    chain: Chain,
    body: Vec<u8>,
    signature: Signature,
    value: PhantomData<fn() -> T>,
}
impl<T> Signed<T> {
    /// The signing key (not yet a verified principal).
    pub fn signer(&self) -> PublicKey {
        self.signer
    }
    /// The carried delegation chain.
    pub fn chain(&self) -> &Chain {
        &self.chain
    }
    /// Canonical envelope bytes; the purpose is deliberately not carried.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.signer.as_bytes().to_vec();
        bytes.extend_from_slice(&self.chain.to_bytes());
        push_len(&mut bytes, self.body.len());
        bytes.extend_from_slice(&self.body);
        bytes.extend_from_slice(&self.signature.to_bytes());
        bytes
    }
}
impl<T: Serialize + DeserializeOwned> Signed<T> {
    /// Sign a value under a purpose. A non-empty chain must be held by the signer.
    /// Refuses an invalid chain, mismatched holder, oversized body, or encode failure.
    pub fn sign(
        signer: &SecretKey,
        purpose: &Purpose,
        value: &T,
        chain: Chain,
    ) -> Result<Self, SignedError> {
        if !chain.links().is_empty() {
            structure(&chain)?;
            if chain.holder() != Some(signer.public()) {
                return Err(SignedError::NotTheHolder);
            }
        }
        let body = postcard::to_stdvec(value).map_err(|_| SignedError::Malformed)?;
        if body.len() > MAX_SIGNED {
            return Err(SignedError::TooLarge {
                len: body.len().min(u32::MAX as usize) as u32,
                max: MAX_SIGNED as u32,
            });
        }
        let signature = signer.sign(&application_input(purpose, &chain, &body));
        Ok(Self {
            signer: signer.public(),
            chain,
            body,
            signature,
            value: PhantomData,
        })
    }
    /// Decode the envelope and its certificates, not the value. The application
    /// signature cannot be checked until a verifier supplies a purpose to `open`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SignedError> {
        let wire: SignedWire = decode_canonical(bytes, MAX_SIGNED + MAX_CHAIN_BYTES + 106)
            .map_err(SignedError::from)?;
        if wire.body.len() > MAX_SIGNED {
            return Err(SignedError::TooLarge {
                len: wire.body.len() as u32,
                max: MAX_SIGNED as u32,
            });
        }
        let chain = Chain::from_wires(wire.chain)?;
        if !chain.links().is_empty() {
            structure(&chain)?;
        }
        Ok(Self {
            signer: wire.signer,
            chain,
            body: wire.body,
            signature: wire.signature,
            value: PhantomData,
        })
    }
    /// Strictly verify the signature, chain standing and purpose's operation
    /// caveat, then decode T. No resource is asserted or checked by this method.
    /// Before using a resource-bearing claim, the application must check its
    /// resource against the returned credential (for example with `permits`).
    /// Offline: this has no revocation knowledge. Use `Trust::open` for that.
    pub fn open(
        &self,
        purpose: &Purpose,
        roots: &Roots,
        now: Timestamp,
    ) -> Result<Opened<T>, SignedError> {
        self.open_with(purpose, roots, &Revoked::new(), now)
    }
    pub(super) fn open_with(
        &self,
        purpose: &Purpose,
        roots: &Roots,
        revoked: &Revoked,
        now: Timestamp,
    ) -> Result<Opened<T>, SignedError> {
        self.signer
            .verify(
                &application_input(purpose, &self.chain, &self.body),
                &self.signature,
            )
            .map_err(|_| SignedError::BadSignature)?;
        let credential = if self.chain.links().is_empty() {
            if !roots.contains(&self.signer) {
                return Err(ChainError::UntrustedRoot.into());
            }
            Credential::root(self.signer)
        } else {
            let credential = structure(&self.chain)?;
            standing(&self.chain, roots, revoked, now)?;
            if credential.holder() != self.signer {
                return Err(SignedError::NotTheHolder);
            }
            credential
        };
        if !credential.caveats().operations.covers(purpose.0) {
            return Err(SignedError::NotPermitted);
        }
        // No T exists until every signature and authority check above passed.
        let value = decode_canonical(&self.body, MAX_SIGNED).map_err(SignedError::from)?;
        Ok(Opened { value, credential })
    }
}
impl<T> Serialize for Signed<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.to_bytes())
    }
}
impl<'de, T: Serialize + DeserializeOwned> Deserialize<'de> for Signed<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}
impl<T> postcard_schema::Schema for Signed<T> {
    const SCHEMA: &'static postcard_schema::schema::NamedType =
        <Vec<u8> as postcard_schema::Schema>::SCHEMA;
}

/// A verified value and the credential it was signed under.
#[derive(Debug, Clone)]
pub struct Opened<T> {
    /// The value, decoded only after verification.
    pub value: T,
    /// The verified principal, holder, and caveats.
    pub credential: Credential,
}

/// Why a signed value could not be opened.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SignedError {
    /// Invalid envelope or application encoding.
    #[error("not a keylink signed message")]
    Malformed,
    /// A decode did not round-trip byte for byte.
    #[error("bytes are not canonical")]
    NonCanonical,
    /// A byte cap was exceeded.
    #[error("{len} bytes exceeds the {max} byte cap")]
    TooLarge {
        /// Received bytes.
        len: u32,
        /// Allowed bytes.
        max: u32,
    },
    /// Strict verification failed under the asserted purpose.
    #[error("the signature does not verify under this purpose")]
    BadSignature,
    /// Invalid purpose name.
    #[error("not a valid purpose name")]
    BadPurpose,
    /// The signer does not hold the chain.
    #[error("the chain is held by another key")]
    NotTheHolder,
    /// The operation caveat does not cover the purpose.
    #[error("the signer's caveats do not cover this purpose")]
    NotPermitted,
    /// The chain does not stand.
    #[error(transparent)]
    Chain(#[from] ChainError),
}
impl From<DecodeError> for SignedError {
    fn from(error: DecodeError) -> Self {
        match error {
            DecodeError::Malformed => Self::Malformed,
            DecodeError::NonCanonical => Self::NonCanonical,
            DecodeError::TooLarge { len, max } => Self::TooLarge { len, max },
        }
    }
}

#[cfg(test)]
#[path = "sign_tests.rs"]
mod tests;
