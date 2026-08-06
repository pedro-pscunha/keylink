//! # keylink
//!
//! Typed peer-to-peer RPC over [iroh]. You define a service; callers get async
//! methods. Everything underneath — NAT traversal, framing, schema versioning,
//! authorization — is invisible from the call site.
//!
//! ```ignore
//! keylink::service! {
//!     service Inventory;
//!     rpc  GetItem(id: u64) -> Option<Item>;
//!     recv Fetch(id: u64) -> FetchInfo;
//! }
//!
//! let node = keylink::Node::bind().await?;
//! let client = inventory::Client::connect(&node, peer_id).await?;
//! let item = client.get_item(7).await?;         // <- this is the whole API surface
//!
//! let (info, body) = client.fetch(7).await?;    // <- and this is a file arriving
//! body.copy_to(&mut file).await?;
//! ```
//!
//! ## What it handles for you
//!
//! - **Addressing.** Peers are dialed by public key, not IP. No ports, no NAT
//!   configuration, no VPN.
//! - **Framing.** QUIC is a byte stream; messages are length-prefixed so one
//!   connection carries an unbounded conversation.
//! - **Concurrency.** Every call takes its own QUIC stream, so calls run in
//!   parallel and a slow handler blocks nobody. See [`RpcClient`].
//! - **Large payloads.** [`Body`] and [`BodyReader`] stream files and blobs
//!   beside a request, with QUIC flow control as backpressure and peak memory
//!   of one chunk.
//! - **Schema safety.** The ALPN is derived from a hash of your types, so a peer
//!   built against a different definition is refused at the handshake instead of
//!   silently misparsing. See [`derive_alpn`].
//! - **Authorization.** iroh proves *who* a peer is; [`Allowlist`] decides
//!   whether they are welcome, and fails closed.
//! - **Departure.** A peer going away is something you can await, rather than
//!   an error you meet on your next call. See [`Watch`].

#![warn(missing_docs)]

// Three features need an operating system facility a browser does not have, and
// each says so here rather than letting the build fail somewhere unhelpful.
//
// `blocking` and `fs` would fail anyway, dozens of errors deep inside mio or
// tokio's filesystem module, naming crates the reader never chose. `env` is the
// one that would NOT fail: `std::env::var` compiles for
// wasm32-unknown-unknown and always returns `NotPresent`, so
// `Allowlist::from_env` would build, ship, and then refuse every peer at
// runtime. Failing closed is the right default, but a function that can never
// succeed on a target should not compile for it.
#[cfg(all(feature = "blocking", target_family = "wasm", target_os = "unknown"))]
compile_error!(
    "keylink's `blocking` feature needs OS threads, which a browser does not \
     have. Build with --no-default-features."
);
#[cfg(all(feature = "fs", target_family = "wasm", target_os = "unknown"))]
compile_error!(
    "keylink's `fs` feature needs a filesystem, which a browser does not have. \
     Build with --no-default-features, and enable `web` for Blob and File \
     bodies instead."
);
#[cfg(all(feature = "env", target_family = "wasm", target_os = "unknown"))]
compile_error!(
    "keylink's `env` feature needs a process environment, which a browser does \
     not have: `std::env::var` compiles here and always returns nothing. Build \
     with --no-default-features and pass the secret and the allowlist in."
);

// `web` asserts a platform fact rather than checking one: `body.rs` carries an
// `unsafe impl Send` for the browser's stream reader, which holds an
// `Rc<RefCell<_>>` and is `!Send` for a real reason. The assertion is that
// wasm32-unknown-unknown without `atomics` has no second thread to send it to.
//
// Under threaded wasm that stops being true and the `unsafe impl` becomes
// unsound. This is what keeps the assertion enforceable instead of hopeful.
//
// It cannot fire today — threaded wasm needs a nightly `build-std` — so unlike
// the three guards above it is UNTESTED, and that is said here rather than
// discovered later.
#[cfg(all(feature = "web", target_feature = "atomics"))]
compile_error!(
    "keylink's `web` feature assumes a single-threaded browser: it contains an      `unsafe impl Send` that is sound only without shared-memory threading.      Threaded wasm is not supported."
);

/// Gated on the feature AND compiled only where OS threads exist. The Cargo
/// feature alone is not enough: it controls which dependency features are on,
/// not whether this module's `tokio::signal` and `new_multi_thread` calls are
/// compiled. Without this, `--no-default-features` still built the module.
#[cfg(feature = "blocking")]
pub mod blocking;

mod actor;
mod authz;
mod blobs;
mod body;
mod error;
mod idempotency;
mod keys;
mod macros;
mod middleware;
mod node;
mod peer;
mod reply;
#[cfg(test)]
mod representability;
mod rpc;
#[cfg(test)]
mod testing;
mod watch;
mod wire;

pub use actor::{
    actor_schema, guarded, mount, ActorClient, ActorReply, ActorRequest, Eviction, Mail, Mailbox,
    MountError, Mounted, Panic, Sealed, System, SystemBuilder,
};
pub use authz::{AdmittedPeer, Allowlist, ProvenPeer};
pub use blobs::{BlobHandle, BlobId, Blobs, BlobsAdmission, Granting, Retention, MAX_GRANTS};
pub use body::{Body, BodyReader};
pub use error::{ConfigError, Error, Result, Transport};
pub use idempotency::{IdempotencyRefusal, MAX_RECORDED_BYTES, MAX_RECORDS};
pub use keys::{ActorId, IdempotencyKey, MAX_KEY_LEN};
pub use middleware::{Context, Middleware, Next, Rpc, Timeout};
pub use node::{derive_alpn, schema_hash, Node};
pub use peer::{Dial, Peer};
pub use reply::{Outcome, RemoteError, WireResponse};
pub use rpc::{Dispatch, RpcClient, Server, ServerBuilder, Service, ServiceBuilder};
pub use watch::{Departure, Watch};
pub use wire::{Envelope, WireKinds, MAX_HEADER};

/// Re-exported so generated code and callers agree on types without extra
/// imports at the call site.
pub use iroh::{protocol::Router, EndpointAddr, EndpointId, SecretKey};
#[doc(hidden)]
pub use pastey;
