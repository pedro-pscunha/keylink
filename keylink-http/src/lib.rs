//! Bounded, authenticated HTTP/JSON gateway for generated Keylink services.
//!
//! [`Gateway`] serves one SINGLETON service; [`ActorGateway`] serves one KEYED
//! actor registry. Both take a generated `External` marker and the generated
//! client, select their URL prefix with [`HttpSurface`], and merge into an
//! application router. Neither creates a second serving path — every call goes
//! through the same generated client an in-process caller uses.
//!
//! # Routes
//!
//! A surface serves ONE address space. The templates below are appended to the
//! prefix given to [`HttpSurface::new`].
//!
//! | Declaration | Singleton | Keyed |
//! |---|---|---|
//! | `rpc` | `POST /rpc/{method}` | `POST /{actor}/rpc/{method}` |
//! | `send` | `POST /send/{method}` | — |
//! | `recv` | `POST /recv/{method}` | `POST /{actor}/recv/{method}` |
//! | `blob` | `POST /blob/{method}` | `POST /{actor}/blob/{method}` |
//! | blob bytes | `GET`/`HEAD` `/blob/{method}/{id}` | `GET`/`HEAD` `/{actor}/blob/{method}/{id}` |
//!
//! `tell` has no route in either space. A keyed surface serves no `send`: a
//! keyed `send` would stream a body to an actor that four separate refusals can
//! reject after the bytes are already on the wire.
//!
//! The streaming families are installed only when [`HttpSurface::with_streaming`]
//! is called. A service whose generated inventory is empty is refused at
//! configuration rather than answering per request.
//!
//! # Content types
//!
//! `rpc` and `blob` lookup take and give `application/json`. `send` takes
//! `application/octet-stream` and gives JSON. `recv` takes JSON and gives
//! `application/octet-stream`. Blob byte routes take no body and give
//! `application/octet-stream`. A request whose `Content-Type` does not match,
//! or which carries two, is refused with `unsupported_media_type`.
//!
//! # Metadata headers
//!
//! A byte body cannot carry a declared value, so one rides a header as
//! base64url with no padding:
//!
//! - `Keylink-Arguments` — REQUEST, on `send`. The declared arguments.
//! - `Keylink-Result` — RESPONSE, on `recv`. The declared return value.
//!
//! Both decode to the same JSON an `rpc` would carry, and both are bounded by
//! [`StreamingLimits::metadata_json_bytes`].
//!
//! # Finite limits
//!
//! [`JsonLimits`] bounds request and response JSON. [`StreamingLimits`] bounds
//! metadata JSON, uploads, downloads and the blob ticket table. Every one is a
//! positive inclusive ceiling with no default — a limit nobody chose is a limit
//! nobody can reason about. Crossing one is `request_too_large`,
//! `request_stream_too_large`, `response_too_large` or
//! `response_stream_too_large`, never a truncated body.
//!
//! The ticket bound is GLOBAL, not per principal. One caller issuing
//! `max_blob_tickets` lookups evicts every other caller's outstanding grants,
//! and each victim's next byte request answers `404`.
//!
//! # Errors
//!
//! Every refusal is `{"error":{"code","message"}}` with a stable code from a
//! fixed inventory and a fixed message that never carries caller input. No
//! refusal repeats an actor id, a method name a caller invented, or upstream
//! text. Every refusal carries `Cache-Control: private, no-store`, including
//! `404` and `410`, which are otherwise heuristically cacheable and depend on
//! WHICH principal asked.
//!
//! # Idempotency
//!
//! An `Idempotency-Key` is accepted on `rpc` only, and is refused with
//! `idempotency_not_supported` on `send`, `recv` and every blob route — a
//! one-shot stream has no reply to replay.
//!
//! The key a caller sends is never the key Keylink sees. It is hashed with a
//! versioned domain separator over length-delimited principal, service, method,
//! addressing mode, actor and the caller's key. So one key names ONE operation
//! for ONE caller on ONE actor: reusing it across methods or actors is two
//! independent calls, and reusing it on the same one replays. Keylink replay is
//! process-lifetime replay, not durable exactly-once execution.
//!
//! # Cancellation
//!
//! Dropping the HTTP handler drops its Keylink call future, and a client that
//! hangs up mid-call drops it too — measured on both address spaces. Backend
//! work that already started can continue. The adapter does not promise
//! rollback.
//!
//! # Blob resume, and where a fault can no longer be reported
//!
//! A blob lookup mints a ticket and answers `Location`. The bytes are a
//! separate resource, and the ticket is bound to the principal, the method and
//! the actor that minted it — another actor's identical `GET` finds nothing.
//!
//! A ticket lives until it is evicted by the global FIFO bound or until the
//! holder refuses, whichever comes first. It is NOT a promise the content is
//! still served: availability is rechecked on every request, and a holder that
//! has gone answers `410`.
//!
//! **A fault after the first byte cannot be reported.** Status and headers are
//! already sent, so the only way to signal a broken download is to break the
//! body. Everything refusable — the length, the range, the conditions, the
//! ticket — is refused before that point.
//!
//! # Actor ids
//!
//! An actor id is read from the RAW path, so `%2F` stays one id rather than
//! becoming a path separator. It is bounded before it is decoded, decoded once,
//! and validated into an `ActorId` before any policy sees it.
//!
//! A refusal never repeats the id. `Location` echoes the caller's own canonical
//! encoded segment, because a relative resource path has to. Access is asked
//! per actor, and a denied caller cannot mount a lazy actor or learn whether
//! one exists — a forbidden actor that IS mounted and one that never was answer
//! byte for byte alike.
//!
//! No route lists, spawns, stops or inspects actors. A registry is a private
//! detail of the serving node.
//!
//! # OpenAPI
//!
//! `keylink-openapi` is a separate opt-in crate. Its schema dependency does not
//! reach this one — `cargo tree -p keylink-http` carries no `schemars` edge.
//!
//! Its bounds are worth knowing before publishing a document. Schemas come from
//! `schemars`, so a DTO with a hand-written `Serialize` that `schemars` cannot
//! see needs an explicit `schemars` override or a manual impl; the generator
//! never emits an unconstrained fallback. Authentication is stated rather than
//! invented: a deployment that authenticates ahead of this gateway is described
//! as exactly that, with no credential scheme guessed. A `u64` exceeding 2^53
//! loses precision in a JavaScript client — the document types it as an
//! integer, and JSON does not carry the distinction. Snapshots are committed
//! and re-blessed deliberately, so a contract change is a diff a reviewer reads.
//!
//! # Deploying behind a proxy
//!
//! - **Buffering.** A proxy that buffers responses breaks streaming: a `recv`
//!   or a blob `GET` arrives all at once or not at all. Disable it per route.
//! - **Timeouts.** A download is bounded by content size, not by time. A proxy
//!   read timeout shorter than a large transfer cuts the body mid-stream, which
//!   is indistinguishable to a client from an upstream fault.
//! - **Caching.** Every refusal is `private, no-store` and every byte response
//!   is `private, no-transform`. A shared cache that ignores them will serve
//!   one principal's `404` to another.
//! - **CORS.** This crate sets no CORS headers and never will. A browser caller
//!   needs `Access-Control-Expose-Headers` for `Keylink-Result`, `Location` and
//!   `Content-Range`, or it cannot read them — that is a deployment decision.
//! - **Browser uploads.** A `send` needs a body stream, which `fetch` supports
//!   only with `duplex: "half"` over HTTP/2. A browser that cannot do it must
//!   buffer the body first.
//!
//! # Authentication
//!
//! Outer middleware inserts a verified [`Principal`] in the request extensions.
//! [`Access`] is mandatory and runs before any request body is read, and before
//! the registry is asked anything. [`Challenge`] supplies the one cached
//! `WWW-Authenticate` value for every `401`.
//!
//! Derive the principal from something you verified. Deriving it from a header
//! the caller controls makes every guarantee above a suggestion.
//!
//! ```ignore
//! let gateway = keylink_http::Gateway::<inventory::External, _>::new(
//!     client,
//!     access,
//!     keylink_http::Challenge::new("Bearer realm=keylink".parse()?)?,
//! )
//! .limits(keylink_http::JsonLimits::new(64 * 1024, 64 * 1024)?)
//! .router::<AppState>(keylink_http::HttpSurface::new(
//!     "/v1/inventory",
//!     keylink_http::Addressing::Singleton,
//! )?)?;
//!
//! let app: axum::Router<AppState> = axum::Router::new()
//!     .merge(gateway)
//!     .with_state(app_state);
//! ```
//!
//! A principal cannot be fabricated through `Default`:
//!
//! ```compile_fail
//! let principal: keylink_http::Principal = Default::default();
//! ```
//!
//! # Not served
//!
//! `tell`, feeds, WebSockets, actor lifecycle, actor discovery, Keylink
//! administration, and CORS.

#![warn(missing_docs)]

#[doc(hidden)]
#[path = "contract.rs"]
pub mod __contract;
mod access;
mod blob;
mod error;
mod gateway;
mod limits;
mod raw_path;
mod service;
mod stream;

pub use __contract::{Addressing, HttpSurface};
pub use access::{Access, AccessError, Call, Challenge, Principal};
pub use error::ConfigError;
pub use gateway::{ActorGateway, Gateway};
pub use limits::{JsonLimits, StreamingLimits};
