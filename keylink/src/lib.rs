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

/// Gated on the feature AND compiled only where OS threads exist. The Cargo
/// feature alone is not enough: it controls which dependency features are on,
/// not whether this module's `tokio::signal` and `new_multi_thread` calls are
/// compiled. Without this, `--no-default-features` still built the module.
#[cfg(feature = "blocking")]
pub mod blocking;

mod actor;
mod authz;
mod body;
mod error;
mod idempotency;
mod keys;
mod macros;
mod middleware;
mod node;
mod reply;
mod rpc;
#[cfg(test)]
mod testing;
mod watch;
mod wire;

pub use actor::{
    actor_schema, guarded, mount, ActorClient, ActorReply, ActorRequest, Eviction, Mail, Mailbox,
    MountError, Mounted, Panic, Sealed, System, SystemBuilder,
};
pub use authz::Allowlist;
pub use body::{Body, BodyReader};
pub use error::{ConfigError, Error, Result, Transport};
pub use idempotency::{IdempotencyRefusal, MAX_RECORDED_BYTES, MAX_RECORDS};
pub use keys::{ActorId, IdempotencyKey, MAX_KEY_LEN};
pub use middleware::{Context, Middleware, Next, Rpc, Timeout};
pub use node::{derive_alpn, schema_hash, Node};
pub use reply::{Outcome, RemoteError, WireResponse};
pub use rpc::{Dispatch, RpcClient, Server, ServerBuilder, Service, ServiceBuilder};
pub use watch::{Departure, Watch};
pub use wire::{Envelope, MAX_HEADER};

/// Re-exported so generated code and callers agree on types without extra
/// imports at the call site.
pub use iroh::{protocol::Router, EndpointAddr, EndpointId, SecretKey};
#[doc(hidden)]
pub use pastey;
