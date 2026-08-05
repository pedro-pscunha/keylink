//! Authorization.
//!
//! iroh proves *who* a peer is during the handshake. It says nothing about
//! whether that peer is welcome. This module is the second half.

use std::collections::BTreeSet;
use std::str::FromStr;

use iroh::EndpointId;

use crate::{ConfigError, Result};

/// Who may talk to a served protocol.
///
/// There is deliberately no `Default`. Admitting everyone is a decision you
/// have to type out, so it can never be what you get by forgetting.
#[derive(Debug, Clone)]
pub enum Allowlist {
    /// Only these proven keys.
    Only(BTreeSet<EndpointId>),
    /// Anyone who can reach us. Sensible for a public service, catastrophic by accident.
    Anyone,
}

impl Allowlist {
    /// Admit exactly the given keys.
    pub fn only<I: IntoIterator<Item = EndpointId>>(ids: I) -> Self {
        Self::Only(ids.into_iter().collect())
    }

    /// Admit any peer that can reach us. Typed out on purpose.
    pub fn anyone() -> Self {
        Self::Anyone
    }

    /// Parse a comma-separated list of 64-hex endpoint ids.
    ///
    /// Errors on a malformed entry rather than skipping it: a typo that silently
    /// shrinks your allowlist is the kind of bug you find during an incident.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MalformedEndpointId`] for an unparseable entry, and
    /// [`ConfigError::EmptyAllowlist`] when nothing at all parsed.
    pub fn parse(raw: &str) -> Result<Self> {
        let mut ids = BTreeSet::new();
        for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let id = EndpointId::from_str(entry).map_err(|source| {
                ConfigError::MalformedEndpointId {
                    entry: entry.to_owned(),
                    source,
                }
            })?;
            ids.insert(id);
        }
        if ids.is_empty() {
            return Err(ConfigError::EmptyAllowlist.into());
        }
        Ok(Self::Only(ids))
    }

    /// Read the allowlist from an environment variable.
    ///
    /// Fails at startup when unset, rather than admitting nobody at runtime and
    /// leaving you to work out why every peer is bouncing. A missing value must
    /// never be the thing that opens *or* silently closes the door.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MissingEnvVar`] when unset, plus the errors of
    /// [`Self::parse`].
    pub fn from_env(var: &str) -> Result<Self> {
        let raw = std::env::var(var).map_err(|_| ConfigError::MissingEnvVar {
            var: var.to_owned(),
        })?;
        Self::parse(&raw)
    }

    /// Whether this proven key is admitted.
    pub fn permits(&self, peer: &EndpointId) -> bool {
        match self {
            Self::Anyone => true,
            Self::Only(ids) => ids.contains(peer),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::Error;

    const A: &str = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0";
    const B: &str = "d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737";

    #[test]
    fn parses_and_permits_only_listed() {
        let list = Allowlist::parse(&format!(" {A} , ")).unwrap();
        assert!(list.permits(&EndpointId::from_str(A).unwrap()));
        assert!(!list.permits(&EndpointId::from_str(B).unwrap()));
    }

    #[test]
    fn rejects_malformed_entry_instead_of_skipping() {
        let err = Allowlist::parse(&format!("{A},notahexid")).unwrap_err();
        assert!(
            matches!(
                err,
                Error::Config(ConfigError::MalformedEndpointId { .. })
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn empty_is_an_error_not_a_deny_all() {
        let err = Allowlist::parse("  ,  ").unwrap_err();
        assert!(
            matches!(err, Error::Config(ConfigError::EmptyAllowlist)),
            "got {err:?}"
        );
    }

    #[test]
    fn unset_env_fails_closed() {
        assert!(Allowlist::from_env("KEYLINK_DEFINITELY_UNSET_VAR").is_err());
    }
}
