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

#![warn(missing_docs)]

mod authz;
mod body;
mod error;
mod macros;
mod middleware;
mod node;
mod reply;
mod rpc;
#[cfg(test)]
mod testing;
mod wire;

pub use authz::Allowlist;
pub use body::{Body, BodyReader};
pub use error::{ConfigError, Error, Result, Transport};
pub use node::{derive_alpn, Node};
pub use middleware::{Context, Middleware, Next, Rpc, Timeout};
pub use reply::{Outcome, RemoteError};
pub use rpc::{
    serve, Dispatch, RpcClient, RpcService, Server, ServerBuilder, Service, ServiceBuilder,
};
pub use wire::{Envelope, MAX_HEADER};

/// Re-exported so generated code and callers agree on types without extra
/// imports at the call site.
pub use iroh::{protocol::Router, EndpointAddr, EndpointId, SecretKey};
#[doc(hidden)]
pub use pastey;
