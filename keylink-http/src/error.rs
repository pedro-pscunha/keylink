//! Adapter-owned configuration and HTTP failures.

/// Invalid gateway configuration.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// A principal namespace was empty.
    #[error("principal namespace must not be empty")]
    EmptyPrincipal,
    /// A principal namespace exceeded the finite cap.
    #[error("principal namespace exceeds the {max} byte cap")]
    PrincipalTooLong {
        /// Maximum accepted principal namespace length in bytes.
        max: usize,
    },
    /// The request JSON limit was zero.
    #[error("request JSON limit must be positive")]
    ZeroRequestLimit,
    /// The successful response JSON limit was zero.
    #[error("response JSON limit must be positive")]
    ZeroResponseLimit,
    /// The authentication challenge was empty or not a legal header value.
    #[error("authentication challenge must be a non-empty header value")]
    InvalidChallenge,
}
