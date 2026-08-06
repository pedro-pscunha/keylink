//! Keyed actors: many instances of state, one ALPN, one connection.
//!
//! An actor owns its state as `&mut self` and handles one message at a time, so
//! the obvious code is the correct code: no `Mutex`, no guard to hold across an
//! `.await`, no `clippy::await_holding_lock` to restructure around.
//!
//! # Mounting is a map write
//!
//! Actor identity travels **inside the request**, never in the ALPN. An ALPN is
//! a contract identifier — a hash of the types — not an instance identifier, and
//! `Endpoint::set_alpns` deep-copies the whole TLS server configuration under
//! the endpoint-wide datapath lock while every handshake scans the ALPN list
//! linearly. Announcing one ALPN per actor is therefore O(N) per mount and
//! O(N²) to start N actors, and it costs one connection per actor.
//!
//! So mounting is `HashMap::insert`, one connection carries every actor on a
//! node, and the existing one-stream-per-call design already gives per-message
//! concurrency between different actors. See RFC-02 §10.2 and RFC-04 §14.
//!
//! # Why this is two files
//!
//! That decision produced two answers to two unrelated questions, and they were
//! written as one file. [`registry`] answers **which actor**: a keyed map of
//! tasks, a cap, and an eviction clock. [`remote`] answers **what a message is
//! made of**: an ALPN, an envelope, and a payload nobody may read early.
//!
//! Neither needs the other to be understood, and only one of them needs a
//! network. That is the argument for the seam, and the type parameter `M` is
//! where it runs: the registry is generic over what a mailbox carries, so a QUIC
//! stream and a postcard payload are unreachable from it — not by convention,
//! but because there is no type there to reach them through.
//!
//! The direction is the whole point. `remote.rs` names registry types freely;
//! `registry.rs` names transport types only where the public API forces it, and
//! its own header lists those five places by name so that the day somebody adds
//! a sixth, they have to add it to a list that says it should not exist.

mod registry;
mod remote;

pub use registry::{mount, Eviction, Mailbox, MountError, Mounted, System, SystemBuilder};
pub use remote::{actor_schema, ActorClient, ActorReply, ActorRequest, Mail, Sealed};
