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
//! # Messages are handled in the order they ARRIVE
//!
//! Each call takes its own QUIC stream, and QUIC orders bytes within a stream,
//! not across streams. So two messages sent to one actor in sequence can arrive
//! in either order, and the actor then handles them one at a time, serially, in
//! the order they turned up — which may not be the order they were sent.
//!
//! **A serial mailbox gives you serialization, not sequencing.** Arrival order
//! is the order, the actor stamps sequence as it processes, and a caller who
//! needs send order gets it by awaiting each reply or by carrying a sequence
//! number in its own payload — where it owns the buffering policy and the stall
//! that comes with it.
//!
//! ## A per-actor sequence number was designed and refused
//!
//! The shape considered was a sequence number beside the actor id, with the
//! actor reordering behind a buffer, so that messages are handled in the order
//! they were SENT. Refused, and this one is a judgement rather than an obvious
//! call, so here is the reasoning.
//!
//! A reorder buffer stalls while it waits for a number it has not seen. That is
//! fine when the message is merely late. It is unbounded when the message is
//! never coming — a cancelled call, a dropped stream, a caller that died
//! between two sends — and the buffer cannot tell any of those apart from slow.
//! The only way to bound the stall is a timeout, which means eventually giving
//! up and processing a gap, which means dropping a message that may still
//! arrive.
//!
//! So the choice is not "ordered versus unordered". It is "out-of-order
//! delivery, which the caller can see and correct" against "a stall of
//! unpredictable length, ended by silently dropping someone's message". The
//! second is worse, and it would be keylink's failure rather than the caller's
//! decision.
//!
//! **This argument is independent of what the envelope looks like.** It was
//! first written on the field list of an envelope that no longer exists, which
//! is why it is stated here instead: the reasoning outlives any particular
//! wrapper, and a future author reaching for a reorder buffer must meet it
//! wherever the wrapper happens to live. Said where a user meets it too, on the
//! generated `Actor` trait and on `ActorRef`.
//!
//! ## Two more fields were refused, and both refusals still hold
//!
//! **A protocol version**, because that is what the ALPN is. A version field
//! inside a message hashed into the ALPN would be a second version number that
//! can disagree with the first.
//!
//! **A method name**, because it would duplicate the payload's own variant
//! index and a peer could make the two disagree. The method name IS readable
//! from outside the payload now — [`Rpc::method`](crate::Rpc::method) reports
//! it, which is what per-method authorization on a keyed actor rests on — but
//! it is *derived from* the request rather than carried beside it, so there is
//! still only one place a method name can come from and nothing for a peer to
//! make inconsistent.
//!
//! # Why there are two halves
//!
//! That decision produced two answers to two unrelated questions, and they were
//! written as one file. [`registry`] answers **which actor**: a keyed map of
//! tasks, a cap, and an eviction clock. [`remote`] answers **what a message is
//! made of**: the wrapper that names an actor, where a message came from, and
//! the walk to a mailbox slot.
//!
//! Neither needs the other to be understood, and only one of them needs a
//! network. That is the argument for the seam, and the type parameter `M` is
//! where it runs: the registry is generic over what a mailbox carries, so a QUIC
//! stream and a postcard payload are unreachable from it — not by convention,
//! but because there is no type there to reach them through.
//!
//! The direction is the whole point. `remote.rs` names registry types freely;
//! `registry.rs` names transport types only where the public API forces it. Its
//! own header names the four such places that are in it, and RFC-04 §16.8 is
//! the list of record for all six that block the crate split — because a list
//! kept in two places drifts, and this one did: three intra-doc links to a wire
//! type were added, filed in the RFC, and left out of the header, which went on
//! saying nothing else was there.

mod registry;
mod remote;

pub use registry::{
    mount, ActorStatus, Eviction, Lifecycle, Mailbox, MountError, Mounted, System, SystemBuilder,
};
pub use remote::{deliver_to_actor, Addressed, KeyedService, Origin};
