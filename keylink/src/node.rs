//! Endpoint lifecycle: identity, binding, and the derived ALPN.

// Both reach exactly one item, `bind_from_env`, so they carry its gate.
#[cfg(feature = "env")]
use std::str::FromStr;

use iroh::{endpoint::presets, Endpoint, EndpointId, SecretKey};
use postcard_rpc::hash::fnv1a64::hash_ty_path;
use postcard_schema::Schema;

#[cfg(feature = "env")]
use crate::ConfigError;
use crate::Result;

/// A bound iroh endpoint. One per process is normal; it multiplexes every
/// protocol you serve or dial.
#[derive(Debug, Clone)]
pub struct Node {
    endpoint: Endpoint,
}

impl Node {
    /// Bind with a fresh random identity. The `EndpointId` changes on every
    /// restart, which is fine for a client and wrong for anything peers allowlist.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Transport`] when the local endpoint cannot bind.
    pub async fn bind() -> Result<Self> {
        Self::build(Endpoint::builder(presets::N0)).await
    }

    /// Bind with a fixed identity, so the id peers dial survives restarts.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Transport`] when the local endpoint cannot bind.
    pub async fn bind_with_secret(secret: SecretKey) -> Result<Self> {
        Self::build(Endpoint::builder(presets::N0).secret_key(secret)).await
    }

    /// Bind using a 64-hex secret from an environment variable, falling back to
    /// a random identity when unset.
    ///
    /// A malformed value is an error rather than a silent fallback: quietly
    /// swapping in a random key would change your `EndpointId` and bounce you
    /// off every peer's allowlist, which is a miserable thing to debug.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MalformedSecretKey`] when the variable is set but does not
    /// parse, plus the errors of [`Self::bind`].
    #[cfg(feature = "env")]
    pub async fn bind_from_env(var: &str) -> Result<Self> {
        let Ok(hex) = std::env::var(var) else {
            return Self::bind().await;
        };
        let secret =
            SecretKey::from_str(&hex).map_err(|source| ConfigError::MalformedSecretKey {
                var: var.to_owned(),
                source,
            })?;
        Self::bind_with_secret(secret).await
    }

    /// Wrap an endpoint you built yourself.
    ///
    /// [`Self::bind`] covers the normal case. Reach for this when the defaults
    /// are wrong — a LAN with no relay, a test with no network, or a transport
    /// configuration keylink does not expose.
    #[must_use]
    pub fn from_endpoint(endpoint: Endpoint) -> Self {
        Self { endpoint }
    }

    async fn build(builder: iroh::endpoint::Builder) -> Result<Self> {
        Ok(Self {
            endpoint: builder.bind().await?,
        })
    }

    /// Wait until the endpoint has a reachable path. Worth awaiting on the
    /// accept side before you print an id for someone to dial.
    pub async fn online(&self) -> &Self {
        self.endpoint.online().await;
        self
    }

    /// This endpoint's public key — the address peers dial.
    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// The underlying iroh endpoint, for anything keylink does not wrap.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

/// The schema hash of `C` under `protocol`.
///
/// The trailing 16 hex digits of [`derive_alpn`] are this value. Exposed so
/// that a caller who needs the number — an actor system comparing a declared
/// hash against a mounted one — takes it from here instead of parsing hex back
/// out of an ALPN.
pub fn schema_hash<C: Schema>(protocol: &str) -> u64 {
    u64::from_le_bytes(hash_ty_path::<C>(protocol))
}

/// Derive an ALPN from a protocol name *and a hash of the schema it carries*.
///
/// postcard is not self-describing: enum variants are encoded by declaration
/// index. Swapping two variants with identical field shapes decodes cleanly as
/// the wrong variant — no error, wrong meaning. Deriving the ALPN from the
/// schema turns that into a refused handshake, because iroh aborts the
/// connection unless both peers present the identical bytestring.
///
/// The hash walks the whole transitive type graph, so a field added three
/// structs deep still moves it. Nobody has to remember to bump a version.
pub fn derive_alpn<C: Schema>(protocol: &str) -> Vec<u8> {
    let hash = schema_hash::<C>(protocol);
    format!("{protocol}/{hash:016x}").into_bytes()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::Envelope;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Schema)]
    struct V1 {
        id: u64,
        name: String,
    }

    /// Same fields, one extra. A real peer would misparse this against `V1`.
    #[derive(Serialize, Deserialize, Schema)]
    struct V2 {
        id: u64,
        name: String,
        scale: u8,
    }

    /// Identical shape to `V1`. Wire-compatible, but the hash is conservative.
    #[derive(Serialize, Deserialize, Schema)]
    struct V1Renamed {
        id: u64,
        label: String,
    }

    #[test]
    fn alpn_is_stable_for_an_unchanged_schema() {
        assert_eq!(derive_alpn::<V1>("svc"), derive_alpn::<V1>("svc"));
    }

    /// The two must not drift: an actor system compares the number while the
    /// handshake compares the string, and they have to mean the same thing.
    #[test]
    fn the_alpn_suffix_is_the_schema_hash() {
        let alpn = derive_alpn::<V1>("crate/Svc");
        let text = std::str::from_utf8(&alpn).expect("alpn is ascii");
        let (_, suffix) = text.rsplit_once('/').expect("has a hash suffix");
        assert_eq!(suffix, format!("{:016x}", schema_hash::<V1>("crate/Svc")));
    }

    #[test]
    fn alpn_shape_is_protocol_slash_16_hex() {
        let alpn = derive_alpn::<V1>("crate/Svc");
        let text = std::str::from_utf8(&alpn).expect("alpn is ascii");
        let (prefix, hash) = text.rsplit_once('/').expect("has a hash suffix");
        assert_eq!(prefix, "crate/Svc");
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn adding_a_field_moves_the_alpn() {
        assert_ne!(derive_alpn::<V1>("svc"), derive_alpn::<V2>("svc"));
    }

    /// Documents a deliberate false positive: renaming a field cannot change the
    /// bytes, because postcard never transmits names. The hash still moves, so
    /// you get a spurious incompatibility. Conservative beats permissive here.
    #[test]
    fn renaming_a_field_also_moves_the_alpn() {
        assert_ne!(derive_alpn::<V1>("svc"), derive_alpn::<V1Renamed>("svc"));
    }

    #[test]
    fn the_protocol_name_is_part_of_the_hash() {
        let a = derive_alpn::<V1>("crate_a/Svc");
        let b = derive_alpn::<V1>("crate_b/Svc");
        assert_ne!(a, b, "two crates must not collide on an identical schema");
    }

    /// keylink's own framing is inside the hashed contract, so a change to the
    /// envelope moves the ALPN exactly like a change to the caller's types.
    /// There is no wire-version constant anyone has to remember to bump.
    #[test]
    fn the_wire_envelope_is_part_of_the_hash() {
        assert_ne!(derive_alpn::<Envelope<V1>>("svc"), derive_alpn::<V1>("svc"));
    }

    /// The fact the whole ALPN scheme exists to defeat. Two enums with the same
    /// field shapes in a different order decode into each other silently,
    /// because postcard encodes a variant by its declaration index.
    #[test]
    fn same_shape_variant_swap_decodes_as_the_wrong_variant() {
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        enum Currency {
            Brl,
            Usd,
        }
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        enum Swapped {
            Usd,
            Brl,
        }

        let bytes = postcard::to_stdvec(&Currency::Brl).unwrap();
        let decoded: Swapped = postcard::from_bytes(&bytes).unwrap();

        // No error. The money just changed currency.
        assert_eq!(decoded, Swapped::Usd);
    }
}
