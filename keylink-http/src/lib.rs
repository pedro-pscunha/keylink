//! Bounded, authenticated HTTP/JSON gateway for generated Keylink services.
//!
//! `Gateway` exposes only enabled singleton `rpc` declarations from a generated
//! `External` marker. Applications select the URL prefix with [`axum::Router::nest`].
//! The adapter uses the existing generated Keylink client and does not create a
//! second serving path.
//!
//! Outer authentication middleware must insert a verified [`Principal`] in the
//! request extensions. [`Access`] is mandatory and runs before the adapter reads
//! request JSON. [`Challenge`] supplies the one cached `WWW-Authenticate` value
//! for all `401` responses.
//!
//! ```ignore
//! let gateway = keylink_http::Gateway::<inventory::External, _>::new(
//!     client,
//!     access,
//!     keylink_http::Challenge::new("Bearer realm=keylink".parse()?)?,
//! )
//! .limits(keylink_http::JsonLimits::new(64 * 1024, 64 * 1024)?)
//! .router::<AppState>();
//!
//! let app: axum::Router<AppState> = axum::Router::new()
//!     .nest("/v1/inventory", gateway)
//!     .with_state(app_state);
//! ```
//!
//! A principal cannot be fabricated through `Default`:
//!
//! ```compile_fail
//! let principal: keylink_http::Principal = Default::default();
//! ```
//!
//! Successful request bodies are argument objects. Successful response bodies
//! are only the declared return value. [`JsonLimits`] applies positive inclusive
//! byte ceilings to request JSON and successful response JSON.
//!
//! An external `Idempotency-Key` is hashed with the principal namespace before
//! Keylink receives it. Use one key for one logical call. Keylink replay is
//! process-lifetime replay, not durable exactly-once execution.
//!
//! Dropping the HTTP handler drops its Keylink call future. Backend work that
//! already started can continue. The adapter does not promise rollback.
//!
//! This package does not expose actors, `tell`, streaming methods, feeds, blobs,
//! WebSockets, OpenAPI, or Keylink administration.

#![warn(missing_docs)]

mod access;
mod error;
mod gateway;
mod limits;
mod service;

pub use access::{Access, AccessError, Call, Challenge, Principal};
pub use error::ConfigError;
pub use gateway::Gateway;
pub use limits::JsonLimits;
#[doc(hidden)]
pub use service::{JsonCodec, JsonError};
