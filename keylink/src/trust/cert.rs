use std::collections::BTreeSet;
use std::fmt;

use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

use super::sign::{decode_canonical, signing_input, DecodeError, Kind};
use super::{Timestamp, MAX_CERTIFICATE, MAX_CHAIN_LINKS, MAX_PATTERNS, MAX_PATTERN_LEN};

/// The identity of a certificate, derived from its canonical body, not its signature.
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
pub struct CertId([u8; 32]);
impl CertId {
    /// The 32 bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}
impl fmt::Display for CertId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum PatternKind {
    Exact,
    Under,
}

/// A segment-bounded name pattern over resources or `Service/Method` operations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pattern {
    kind: PatternKind,
    name: String,
}
impl Pattern {
    /// Exactly this name. Returns `BadPattern` for an invalid name.
    pub fn exact(name: &str) -> Result<Self, CertificateError> {
        Self::new(PatternKind::Exact, name)
    }
    /// This name and its slash-separated descendants, never an unbounded prefix.
    /// Returns `BadPattern` for an invalid name.
    pub fn under(name: &str) -> Result<Self, CertificateError> {
        Self::new(PatternKind::Under, name)
    }
    fn new(kind: PatternKind, name: &str) -> Result<Self, CertificateError> {
        let p = Self {
            kind,
            name: name.to_owned(),
        };
        p.validate()?;
        Ok(p)
    }
    fn validate(&self) -> Result<(), CertificateError> {
        let fault = if self.name.is_empty() {
            Some(PatternFault::Empty)
        } else if self.name.len() > MAX_PATTERN_LEN {
            Some(PatternFault::TooLong)
        } else if self.name.starts_with('/') || self.name.ends_with('/') {
            Some(PatternFault::EdgeSlash)
        } else if self.name.contains("//") {
            Some(PatternFault::EmptySegment)
        } else {
            None
        };
        fault.map_or(Ok(()), |f| Err(CertificateError::BadPattern(f)))
    }
    fn covers(&self, name: &str) -> bool {
        name == self.name
            || (self.kind == PatternKind::Under
                && name
                    .strip_prefix(&self.name)
                    .is_some_and(|tail| tail.starts_with('/')))
    }
    fn contains(&self, child: &Self) -> bool {
        self.covers(&child.name)
            && (self.kind == PatternKind::Under || child.kind == PatternKind::Exact)
    }
}

/// Why a pattern name was refused.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub enum PatternFault {
    /// No name.
    Empty,
    /// More than `MAX_PATTERN_LEN` bytes.
    TooLong,
    /// Leading or trailing slash.
    EdgeSlash,
    /// Two consecutive slashes.
    EmptySegment,
}

// A Vec on the wire preserves ordering and duplicates so validation can refuse
// them instead of BTreeSet's deserializer silently canonicalizing them.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ScopeKind {
    Any,
    Only(Vec<Pattern>),
}

/// Everything, or a non-empty sorted set of name patterns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope(ScopeKind);
impl Scope {
    /// Everything; this choice must be typed out.
    pub fn any() -> Self {
        Self(ScopeKind::Any)
    }
    /// Only these patterns. Refuses empty sets and more than `MAX_PATTERNS`.
    pub fn only(patterns: impl IntoIterator<Item = Pattern>) -> Result<Self, CertificateError> {
        let patterns: BTreeSet<_> = patterns.into_iter().collect();
        let scope = Self(ScopeKind::Only(patterns.into_iter().collect()));
        scope.validate()?;
        Ok(scope)
    }
    /// Whether this concrete name is allowed.
    #[must_use]
    pub fn covers(&self, name: &str) -> bool {
        match &self.0 {
            ScopeKind::Any => true,
            ScopeKind::Only(p) => p.iter().any(|p| p.covers(name)),
        }
    }
    pub(super) fn is_any(&self) -> bool {
        matches!(self.0, ScopeKind::Any)
    }
    pub(super) fn subset_of(&self, parent: &Self) -> bool {
        match (&self.0, &parent.0) {
            (ScopeKind::Any | ScopeKind::Only(_), ScopeKind::Any) => true,
            (ScopeKind::Any, ScopeKind::Only(_)) => false,
            (ScopeKind::Only(c), ScopeKind::Only(p)) => {
                c.iter().all(|c| p.iter().any(|p| p.contains(c)))
            }
        }
    }
    fn validate(&self) -> Result<(), CertificateError> {
        if let ScopeKind::Only(patterns) = &self.0 {
            if patterns.is_empty() {
                return Err(CertificateError::EmptyScope);
            }
            if patterns.len() > MAX_PATTERNS {
                return Err(CertificateError::TooManyPatterns {
                    len: patterns.len() as u32,
                    max: MAX_PATTERNS as u32,
                });
            }
            for p in patterns {
                p.validate()?;
            }
            if patterns.windows(2).any(|w| w[0] >= w[1]) {
                return Err(CertificateError::NonCanonical);
            }
        }
        Ok(())
    }
}

/// A half-open validity window `[not_before, not_after)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Validity {
    not_before: Timestamp,
    not_after: Timestamp,
}
impl Validity {
    /// Refuses an empty or backwards window with `EmptyValidity`.
    pub fn new(not_before: Timestamp, not_after: Timestamp) -> Result<Self, CertificateError> {
        if not_before >= not_after {
            return Err(CertificateError::EmptyValidity {
                not_before: not_before.unix_secs(),
                not_after: not_after.unix_secs(),
            });
        }
        Ok(Self {
            not_before,
            not_after,
        })
    }
    /// The inclusive start.
    pub fn not_before(&self) -> Timestamp {
        self.not_before
    }
    /// The exclusive end.
    pub fn not_after(&self) -> Timestamp {
        self.not_after
    }
    pub(super) fn unlimited() -> Self {
        Self {
            not_before: Timestamp::from_unix_secs(0),
            not_after: Timestamp::from_unix_secs(u64::MAX),
        }
    }
}

/// Restrictions on resources, operations, and time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caveats {
    /// Names the holder may access.
    pub resources: Scope,
    /// Operations the holder may perform.
    pub operations: Scope,
    /// When the grant counts.
    pub validity: Validity,
}
impl Caveats {
    pub(super) fn unlimited() -> Self {
        Self {
            resources: Scope::any(),
            operations: Scope::any(),
            validity: Validity::unlimited(),
        }
    }
}

/// What an issuer grants to one subject key.
#[derive(Debug, Clone)]
pub struct Delegation {
    /// The key receiving the grant.
    pub subject: PublicKey,
    /// What it may do and when.
    pub caveats: Caveats,
    /// Maximum further links; zero means no delegation.
    pub delegate: u8,
    /// Issuer-chosen serial; a reissued revoked grant needs a new serial or window.
    pub serial: u64,
}

// Wire data must not deserialize directly into validated public types. Keep
// ordering intact until validation, including duplicate and out-of-order names.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawPattern {
    kind: PatternKind,
    name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
enum RawScope {
    Any,
    Only(Vec<RawPattern>),
}
impl RawScope {
    fn from_scope(scope: &Scope) -> Self {
        match &scope.0 {
            ScopeKind::Any => Self::Any,
            ScopeKind::Only(patterns) => Self::Only(
                patterns
                    .iter()
                    .map(|p| RawPattern {
                        kind: p.kind,
                        name: p.name.clone(),
                    })
                    .collect(),
            ),
        }
    }
    fn scope(&self) -> Result<Scope, CertificateError> {
        let scope = match self {
            Self::Any => Scope::any(),
            Self::Only(patterns) => Scope(ScopeKind::Only(
                patterns
                    .iter()
                    .map(|p| Pattern {
                        kind: p.kind,
                        name: p.name.clone(),
                    })
                    .collect(),
            )),
        };
        scope.validate()?;
        Ok(scope)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CertBody {
    V1(CertV1),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CertV1 {
    issuer: PublicKey,
    subject: PublicKey,
    serial: u64,
    not_before: Timestamp,
    not_after: Timestamp,
    delegate: u8,
    resources: RawScope,
    operations: RawScope,
}
#[derive(Serialize, Deserialize)]
pub(super) struct CertWire {
    pub(super) body: Vec<u8>,
    pub(super) signature: Signature,
}

/// One signed link. Construction always checks its signature and fields.
#[derive(Debug, Clone)]
pub struct Certificate {
    body: Vec<u8>,
    signature: Signature,
    fields: CertV1,
    caveats: Caveats,
    id: CertId,
}
impl Certificate {
    /// Sign a delegation. Refuses self-issue, excessive depth, or an oversized body.
    pub fn issue(issuer: &SecretKey, delegation: Delegation) -> Result<Self, CertificateError> {
        let fields = CertV1 {
            issuer: issuer.public(),
            subject: delegation.subject,
            serial: delegation.serial,
            not_before: delegation.caveats.validity.not_before,
            not_after: delegation.caveats.validity.not_after,
            delegate: delegation.delegate,
            resources: RawScope::from_scope(&delegation.caveats.resources),
            operations: RawScope::from_scope(&delegation.caveats.operations),
        };
        let body =
            postcard::to_stdvec(&CertBody::V1(fields)).map_err(|_| CertificateError::Malformed)?;
        let signature = issuer.sign(&signing_input(Kind::Certificate, "", &body));
        Self::from_wire(CertWire { body, signature })
    }
    pub(super) fn from_wire(wire: CertWire) -> Result<Self, CertificateError> {
        let CertBody::V1(fields) =
            decode_canonical(&wire.body, MAX_CERTIFICATE).map_err(CertificateError::from)?;
        if fields.issuer == fields.subject {
            return Err(CertificateError::SelfIssued);
        }
        if fields.delegate as usize >= MAX_CHAIN_LINKS {
            return Err(CertificateError::DelegateTooDeep {
                delegate: fields.delegate,
                max: (MAX_CHAIN_LINKS - 1) as u8,
            });
        }
        let validity = Validity::new(fields.not_before, fields.not_after)?;
        let resources = fields.resources.scope()?;
        let operations = fields.operations.scope()?;
        // Verify the received bytes, never a serialization of `fields`.
        fields
            .issuer
            .verify(
                &signing_input(Kind::Certificate, "", &wire.body),
                &wire.signature,
            )
            .map_err(|_| CertificateError::BadSignature)?;
        let id = CertId(blake3::derive_key(
            "keylink 2026-10-31 certificate id v1",
            &wire.body,
        ));
        let caveats = Caveats {
            resources,
            operations,
            validity,
        };
        Ok(Self {
            body: wire.body,
            signature: wire.signature,
            fields,
            caveats,
            id,
        })
    }
    /// The signing key.
    pub fn issuer(&self) -> PublicKey {
        self.fields.issuer
    }
    /// The receiving key.
    pub fn subject(&self) -> PublicKey {
        self.fields.subject
    }
    /// This link's restrictions.
    pub fn caveats(&self) -> &Caveats {
        &self.caveats
    }
    /// How many links may follow this one.
    pub fn delegate(&self) -> u8 {
        self.fields.delegate
    }
    /// The issuer's serial.
    pub fn serial(&self) -> u64 {
        self.fields.serial
    }
    /// The identity derived from the body.
    pub fn id(&self) -> CertId {
        self.id
    }
    /// Canonical postcard wire bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        wire_bytes(&self.body, &self.signature)
    }
    /// Decode canonical bytes, validate all fields, and strictly verify the signature.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CertificateError> {
        Self::from_wire(
            decode_canonical(bytes, MAX_CERTIFICATE + 74).map_err(CertificateError::from)?,
        )
    }
}

#[derive(Serialize, Deserialize)]
enum RevocationBody {
    V1 {
        revoker: PublicKey,
        certificate: CertId,
    },
}

/// A signed statement that one certificate no longer counts.
#[derive(Debug, Clone)]
pub struct Revocation {
    revoker: PublicKey,
    certificate: CertId,
    body: Vec<u8>,
    signature: Signature,
}
impl Revocation {
    /// Sign a revocation. Its authority is decided against each affected chain.
    pub fn issue(revoker: &SecretKey, certificate: CertId) -> Self {
        // All fields are fixed-width: version + key + id, exactly 65 bytes.
        let mut body = Vec::with_capacity(65);
        body.push(0);
        body.extend_from_slice(revoker.public().as_bytes());
        body.extend_from_slice(certificate.as_bytes());
        let signature = revoker.sign(&signing_input(Kind::Revocation, "", &body));
        Self {
            revoker: revoker.public(),
            certificate,
            body,
            signature,
        }
    }
    /// The signing key.
    pub fn revoker(&self) -> PublicKey {
        self.revoker
    }
    /// The certificate being revoked.
    pub fn certificate(&self) -> CertId {
        self.certificate
    }
    /// Canonical postcard wire bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        wire_bytes(&self.body, &self.signature)
    }
    /// Decode canonical bytes and strictly verify the signature.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CertificateError> {
        let wire: CertWire = decode_canonical(bytes, 130).map_err(CertificateError::from)?;
        let RevocationBody::V1 {
            revoker,
            certificate,
        } = decode_canonical(&wire.body, 65).map_err(CertificateError::from)?;
        revoker
            .verify(
                &signing_input(Kind::Revocation, "", &wire.body),
                &wire.signature,
            )
            .map_err(|_| CertificateError::BadSignature)?;
        Ok(Self {
            revoker,
            certificate,
            body: wire.body,
            signature: wire.signature,
        })
    }
}

pub(super) fn push_len(bytes: &mut Vec<u8>, mut len: usize) {
    while len >= 128 {
        bytes.push((len as u8 & 127) | 128);
        len >>= 7;
    }
    bytes.push(len as u8);
}
fn wire_bytes(body: &[u8], signature: &Signature) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(body.len() + 74);
    push_len(&mut bytes, body.len());
    bytes.extend_from_slice(body);
    bytes.extend_from_slice(&signature.to_bytes());
    bytes
}

/// Why a certificate or revocation was refused at decode or issue.
#[non_exhaustive]
#[derive(
    Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize, postcard_schema::Schema,
)]
pub enum CertificateError {
    /// Unrecognized or invalid wire form.
    #[error("not a keylink certificate")]
    Malformed,
    /// The bytes do not round-trip exactly.
    #[error("bytes are not canonical")]
    NonCanonical,
    /// The byte cap was exceeded.
    #[error("{len} bytes exceeds the {max} byte cap")]
    TooLarge {
        /// Received length.
        len: u32,
        /// Allowed length.
        max: u32,
    },
    /// Strict signature verification failed.
    #[error("the signature does not verify")]
    BadSignature,
    /// Issuer and subject are the same key.
    #[error("issuer and subject are one key")]
    SelfIssued,
    /// A validity window is empty or backwards.
    #[error("empty validity window {not_before}..{not_after}")]
    EmptyValidity {
        /// Start.
        not_before: u64,
        /// End.
        not_after: u64,
    },
    /// Delegation exceeds the chain cap.
    #[error("delegate {delegate} exceeds {max}")]
    DelegateTooDeep {
        /// Requested depth.
        delegate: u8,
        /// Maximum depth.
        max: u8,
    },
    /// No patterns in an `Only` scope.
    #[error("a scope must name at least one pattern")]
    EmptyScope,
    /// Too many patterns.
    #[error("{len} patterns exceeds {max}")]
    TooManyPatterns {
        /// Received count.
        len: u32,
        /// Allowed count.
        max: u32,
    },
    /// A name is invalid.
    #[error("bad pattern: {0:?}")]
    BadPattern(PatternFault),
}
impl From<DecodeError> for CertificateError {
    fn from(error: DecodeError) -> Self {
        match error {
            DecodeError::Malformed => Self::Malformed,
            DecodeError::NonCanonical => Self::NonCanonical,
            DecodeError::TooLarge { len, max } => Self::TooLarge { len, max },
        }
    }
}

#[cfg(test)]
#[path = "cert_tests.rs"]
mod tests;
