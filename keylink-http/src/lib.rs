//! Bounded, authenticated HTTP/JSON gateway for generated Keylink services.

#![warn(missing_docs)]

mod access;
mod error;
mod limits;
mod service;

pub use access::{Access, AccessError, Call, Challenge, Principal};
pub use error::ConfigError;
pub use limits::JsonLimits;
