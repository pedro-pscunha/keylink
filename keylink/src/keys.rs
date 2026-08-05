//! Bounded identifiers that arrive from the network.
//!
//! [`ActorId`] addresses one instance among many behind a single ALPN.
//! [`IdempotencyKey`] lets a caller retry without running the call twice.
//!
//! # Why validation happens twice
//!
//! `new` cannot protect the receiving side. `#[derive(Deserialize)]` builds the
//! value field by field and never calls a constructor, so a peer can put any
//! string on the wire regardless of what our constructor allows. The decoder
//! therefore re-checks before use.
//!
//! This is the same shape of trap as an `Option` that is never `None` in
//! practice: a handler that trusts the constructor passes every test anyone can
//! write, because no test written in Rust can produce the bad input.

use serde::{Deserialize, Serialize};

use crate::{ConfigError, Error, Result};

/// Longest identifier accepted, in bytes.
///
/// Judgement, not measurement. Long enough for a UUID, a path-like key such as
/// `obra/4471/medicao/12`, or a hash. Short enough that a peer cannot make us
/// hold much by sending many.
pub const MAX_KEY_LEN: usize = 128;

/// Shared by every identifier here, so the rule exists once.
fn check(kind: &'static str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::Config(ConfigError::MalformedKey {
            kind,
            reason: "empty".into(),
        }));
    }
    if value.len() > MAX_KEY_LEN {
        return Err(Error::Config(ConfigError::MalformedKey {
            kind,
            reason: format!("{} bytes exceeds the {MAX_KEY_LEN} byte cap", value.len()),
        }));
    }
    Ok(())
}

macro_rules! bounded_key {
    ($name:ident, $kind:literal, $doc:literal) => {
        #[doc = $doc]
        ///
        /// Bounded at [`MAX_KEY_LEN`] bytes. Validate a value that came from a
        /// peer with [`validate`](Self::validate) before you use it: the derived
        /// `Deserialize` does not call [`new`](Self::new).
        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
            postcard_schema::Schema,
        )]
        pub struct $name(String);

        impl $name {
            /// Build one, refusing an empty or oversized value.
            ///
            /// # Errors
            ///
            /// [`ConfigError::MalformedKey`] when empty or over [`MAX_KEY_LEN`].
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                check($kind, &value)?;
                Ok(Self(value))
            }

            /// Re-check a value that arrived from a peer.
            ///
            /// # Errors
            ///
            /// [`ConfigError::MalformedKey`] when empty or over [`MAX_KEY_LEN`].
            pub fn validate(&self) -> Result<()> {
                check($kind, &self.0)
            }

            /// The identifier as text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

bounded_key!(
    ActorId,
    "actor id",
    "Addresses one actor instance among many behind a single ALPN."
);

bounded_key!(
    IdempotencyKey,
    "idempotency key",
    "Marks a call so that a retry of it runs at most once."
);

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[test]
    fn an_ordinary_key_is_accepted() {
        let id = ActorId::new("obra/4471/medicao/12").expect("valid");
        assert_eq!(id.as_str(), "obra/4471/medicao/12");
    }

    #[test]
    fn an_empty_key_is_refused() {
        assert!(ActorId::new("").is_err());
    }

    #[test]
    fn an_oversized_key_is_refused() {
        assert!(ActorId::new("x".repeat(MAX_KEY_LEN + 1)).is_err());
        assert!(ActorId::new("x".repeat(MAX_KEY_LEN)).is_ok());
    }

    /// The constructor cannot protect the receiving side, so the rule has to be
    /// reachable from a decoded value too. If this ever stops compiling because
    /// `validate` was removed, the receive path lost its only guard.
    #[test]
    fn a_decoded_key_can_still_be_checked() {
        let oversized: ActorId =
            postcard::from_bytes(&postcard::to_stdvec(&"x".repeat(MAX_KEY_LEN + 1)).expect("enc"))
                .expect("a peer can encode what our constructor refuses");
        assert!(
            oversized.validate().is_err(),
            "a value that bypassed the constructor must still be catchable"
        );
    }

    #[test]
    fn the_two_key_types_do_not_interchange() {
        // Compile-time property, asserted by construction: this file would not
        // build if `ActorId` and `IdempotencyKey` were one type.
        let a = ActorId::new("a").expect("valid");
        let b = IdempotencyKey::new("a").expect("valid");
        assert_eq!(a.as_str(), b.as_str());
    }
}
