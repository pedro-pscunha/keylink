//! Verified principal identity and mandatory authorization.

use std::fmt;
use std::future::Future;

use http::{Extensions, HeaderMap, HeaderValue};

use crate::ConfigError;

const MAX_PRINCIPAL_LEN: usize = 256;

/// Stable, non-secret namespace inserted by deployment authentication middleware.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Principal(Vec<u8>);

impl Principal {
    /// Build a finite, non-empty principal namespace.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::EmptyPrincipal`] or [`ConfigError::PrincipalTooLong`].
    pub fn new(namespace: impl AsRef<[u8]>) -> Result<Self, ConfigError> {
        let namespace = namespace.as_ref();
        if namespace.is_empty() {
            return Err(ConfigError::EmptyPrincipal);
        }
        if namespace.len() > MAX_PRINCIPAL_LEN {
            return Err(ConfigError::PrincipalTooLong {
                max: MAX_PRINCIPAL_LEN,
            });
        }
        Ok(Self(namespace.to_vec()))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Principal([REDACTED])")
    }
}

/// One known RPC call presented to mandatory authorization.
pub struct Call<'a> {
    /// Request extensions, including deployment-owned context.
    pub extensions: &'a Extensions,
    /// Request headers.
    pub headers: &'a HeaderMap,
    /// Verified principal namespace inserted by outer middleware.
    pub principal: &'a Principal,
    /// Generated service name.
    pub service: &'static str,
    /// Validated generated RPC method name.
    pub method: &'static str,
}

/// Mandatory edge authorization refusal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessError {
    /// Authentication is missing or no longer accepted.
    Unauthenticated,
    /// The authenticated principal may not call this method.
    Forbidden,
}

/// Authorizes every matched gateway call before its body is read.
pub trait Access: Clone + Send + Sync + 'static {
    /// Decide whether one verified principal may invoke one known RPC method.
    fn authorize(&self, call: Call<'_>) -> impl Future<Output = Result<(), AccessError>> + Send;
}

/// Validated `WWW-Authenticate` value cached by a gateway.
#[derive(Clone, Debug)]
pub struct Challenge(HeaderValue);

impl Challenge {
    /// Validate and cache an authentication challenge.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidChallenge`] for an empty or non-text value.
    pub fn new(value: HeaderValue) -> Result<Self, ConfigError> {
        if value.is_empty() || value.to_str().is_err() {
            return Err(ConfigError::InvalidChallenge);
        }
        Ok(Self(value))
    }

    pub(crate) fn header(&self) -> &HeaderValue {
        &self.0
    }
}
