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
//! - **State, when sharing it is the awkward part.** The same `service!` block
//!   generates an `Actor` trait taking `&mut self`, so state lives on a task
//!   rather than behind a `Mutex` — locally through a generated `Handle`, and
//!   over the network with no change to the wire. See [`LocalActor`], and
//!   [`System`] for many keyed instances behind one ALPN.
//! - **Content addressing.** A `blob` method hands back an address; iroh-blobs
//!   moves the bytes, admitted only against a grant keylink recorded when it
//!   returned that address to that peer. See [`Blobs`].
//! - **Extension, at two boundaries and not one.** [`Middleware`] intercepts a
//!   **message on the wire**; [`Observer`] watches a **turn against the state**.
//!   One sentence decides which to reach for, and neither grows toward the
//!   other — an observer never sees a request or a reply payload, and a layer
//!   never sees the state. Durability is the first thing built on the second
//!   one, outside this crate.

#![warn(missing_docs)]
// `deny` and not `forbid`, and the difference is the one `unsafe impl` below.
// `forbid` cannot be lifted at all, so it would make the browser body reader
// unwritable; `deny` can be lifted exactly once, by name, at the site that needs
// it. `#[expect]` rather than `#[allow]` for that site, so the day the impl goes
// away the lift becomes an unfulfilled expectation and is removed with it — an
// `allow` would sit there forever, silently re-opening the door.
//
// This is what turns "keylink contains exactly one `unsafe impl`" from a
// sentence in two doc comments into something the compiler checks.
#![deny(unsafe_code)]

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

// Two places assert a platform fact rather than checking one, and both assert
// the same fact: wasm32-unknown-unknown without `atomics` has no second thread.
//
// `body.rs` carries an `unsafe impl Send` for the browser's stream reader, which
// holds an `Rc<RefCell<_>>` and is `!Send` for a real reason. That one is gated
// on `web`.
//
// `local.rs` is NOT gated on anything: `LocalActor` holds an
// `Arc<n0_future::task::JoinHandle<()>>`, which in a browser wraps an
// `Rc<RefCell<_>>` in a `SendWrapper`. `SendWrapper` is `Send` and `Sync` for
// every `T` and panics if it is dereferenced — or dropped — on another thread,
// so `LocalActor: Send + Sync` is honest only while there is no other thread.
// Under threaded wasm, sending a handle to a Web Worker turns `abort()` into a
// panic and dropping the last clone into a panic inside a destructor.
//
// So the guard is crate-wide rather than feature-gated. It cannot fire today —
// threaded wasm needs a nightly `build-std` — so unlike the three guards above
// it is UNTESTED, and that is said here rather than discovered later.
#[cfg(all(
    target_family = "wasm",
    target_os = "unknown",
    target_feature = "atomics"
))]
compile_error!(
    "keylink assumes a single-threaded browser. `local.rs` holds a thread-affine \
     task handle behind a `Send + Sync` type, and the `web` feature adds an \
     `unsafe impl Send` beside it. Threaded wasm is not supported."
);

/// Gated on the feature AND compiled only where OS threads exist. The Cargo
/// feature alone is not enough: it controls which dependency features are on,
/// not whether this module's `tokio::signal` and `new_multi_thread` calls are
/// compiled. Without this, `--no-default-features` still built the module.
#[cfg(feature = "blocking")]
pub mod blocking;

mod actor;
mod admission;
mod authz;
mod blobs;
mod body;
mod error;
mod idempotency;
mod keys;
mod local;
mod macros;
mod middleware;
mod node;
mod observe;
mod panic;
mod peer;
mod reply;
#[cfg(test)]
mod representability;
mod rpc;
mod subscribers;
#[cfg(test)]
mod testing;
mod watch;
mod wire;

pub use actor::{
    deliver_to_actor, mount, Addressed, Eviction, KeyedService, Mailbox, MountError, Mounted,
    Origin, System, SystemBuilder,
};
pub use admission::{Admission, Admittance, DynamicAdmission, Revocable, Revocation};
pub use authz::{AdmittedPeer, Allowlist, ProvenPeer};
pub use blobs::{BlobHandle, BlobId, Blobs, BlobsAdmission, Granting, Retention, MAX_GRANTS};
pub use body::{Body, BodyReader};
pub use error::{ConfigError, Error, Result, Transport};
pub use idempotency::{
    Idempotency, IdempotencyRefusal, KeyState, MAX_ATTACH, MAX_RECORDED_BYTES, MAX_RECORDS,
    MAX_RUNNING, MAX_RUNNING_PER_PEER, MAX_TOMBSTONES, MAX_WAITING, MAX_WAITING_PER_PEER,
    RECORD_TTL,
};
pub use keys::{ActorId, IdempotencyKey, MAX_KEY_LEN};
pub use local::{deliver_to_local, turn, Hooks, LocalActor, MailboxClosed, Reply, Turn};
pub use middleware::{Caller, Context, Middleware, Next, QueueTimeout, Queueing, Rpc, Timeout};
pub use node::{derive_alpn, Node};
pub use observe::{Ended, FinishFut, Observer, Stop, TurnFut, TurnInfo, TurnLost, TurnOutcome};
pub use panic::Panic;
pub use peer::{Dial, Peer};
pub use reply::{Outcome, RemoteError, WireRequest, WireResponse};
pub use rpc::{Dispatch, RpcClient, Server, ServerBuilder, Service, ServiceBuilder};
pub use subscribers::Subscribers;
pub use watch::{Departure, Watch};
pub use wire::{Envelope, WireKinds, MAX_HEADER};

/// Re-exported so generated code and callers agree on types without extra
/// imports at the call site.
pub use iroh::{protocol::Router, EndpointAddr, EndpointId, SecretKey};
#[doc(hidden)]
pub use pastey;
