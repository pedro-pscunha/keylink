//! Component names, derived rather than chosen.

/// The component name for one logical schema id.
///
/// Six BLAKE3 bytes over a versioned domain, the id's length as a big-endian
/// `u64`, and the id's UTF-8 bytes. The length goes in first for the same
/// reason it does in the gateway's replay identity: without it, two ids that
/// concatenate the same way would name one component.
///
/// Six bytes rather than a full hash because a component name is read by
/// people. Twelve hexadecimal characters is 2^48 — a collision needs about 17
/// million schemas in one document before it is worth thinking about, and
/// [`crate::DocumentError::ComponentCollision`] catches it if it happens.
#[must_use]
pub fn component_name(id: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keylink-openapi/component/v1\0");
    hasher.update(&(id.len() as u64).to_be_bytes());
    hasher.update(id.as_bytes());
    let digest = hasher.finalize();
    let short = &digest.as_bytes()[..6];
    let mut out = String::with_capacity(12);
    for byte in short {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
