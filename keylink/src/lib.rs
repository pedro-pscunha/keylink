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
//!   over the network with no change to the wire. **Two kinds of actor, and
//!   serving is not a third:** a [`LocalActor`] behind a generated `Handle`, and
//!   a keyed instance mounted in a [`Registry`] under an [`ActorId`] with its own
//!   ALPN. [`Servable::service`] puts a local one on the wire without making it
//!   either a new kind or a new type.
//! - **Content addressing.** A `blob` method hands back an address; iroh-blobs
//!   moves the bytes, admitted only against a grant keylink recorded when it
//!   returned that address to that peer. See [`Blobs`].
//! - **Extension, at two boundaries and not one.** The [`extend`] page maps the
//!   complete extension surface. [`Middleware`] intercepts a **message on the
//!   wire**; [`Observer`] watches a **turn against the state**.
//!   One sentence decides which to reach for, and neither grows toward the
//!   other — an observer never sees a request or a reply payload, and a layer
//!   never sees the state. Durability is the first thing built on the second
//!   one, outside this crate.
//! - **The compositions every actor system grows**, each with its backpressure
//!   written down rather than inherited from an unbounded queue: fan-out, a
//!   pool, a schedule, a name registry, and a set of actors that end together.
//!   See [`compose`].
//! - **Its own introspection, served over itself.** [`Registry::status`] lists
//!   what is running; [`inspect`] serves that list and a live lifecycle log on
//!   an ALPN and an [`Allowlist`] of their own, and [`Console`] is the peer that
//!   reads them. A subscriber is a parked reply rather than a held handler, so
//!   watching an actor does not stop it.

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

// Four features need an operating system facility a browser does not have, and
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
// The fourth is the durability adapter's, and it is the `persist-file` half
// rather than `persist`. That split is the whole reason the adapter could be a
// module here instead of a third crate: `persist` reaches only the frame and the
// two port traits, so it compiles for the browser and `.mise/tasks/wasm` builds
// it there. `persist-file` turns on `keylink-persist/file`, which turns on
// `tokio/fs`, which a browser does not have.
//
// **This one was watched, and it did NOT fire — say so rather than let a reader
// assume it did.** Unlike the three above, the failure is in a *dependency*, and
// cargo compiles dependencies first: `--target wasm32-unknown-unknown --features
// persist-file` stops at tokio's own `compile_error!`, `Only features
// sync,macros,io-util,rt,time are supported on wasm`, and this line is never
// reached. It stays for two reasons and neither is enforcement. It keeps
// `persist-file` in the list a reader scans to learn which features a browser
// refuses, so the next author does not add it back. And it is the message that
// would be reached if tokio ever accepted the feature, which is the day the
// silent-success hazard `env` documents would apply here too — `std::fs`
// compiles for this target and fails at runtime.
//
// The 48-error mio explosion this used to produce instead is gone for an
// unrelated and better reason: `keylink-persist` stopped inheriting `tokio`'s
// `full`, which its own manifest now explains.
#[cfg(all(
    feature = "persist-file",
    target_family = "wasm",
    target_os = "unknown"
))]
compile_error!(
    "keylink's `persist-file` feature needs a filesystem, which a browser does \
     not have. Build with --no-default-features --features persist, which is the \
     adapter over the port with no file store behind it."
);

// Two places assert a platform fact rather than checking one, and both assert
// the same fact: wasm32-unknown-unknown without `atomics` has no second thread.
//
// `body.rs` carries an `unsafe impl Send` for the browser's stream reader, which
// holds an `Rc<RefCell<_>>` and is `!Send` for a real reason. That one is gated
// on `web`.
//
// `turns.rs` is NOT gated on anything: `LocalActor` holds an
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
    "keylink assumes a single-threaded browser. `turns.rs` holds a thread-affine \
     task handle behind a `Send + Sync` type, and the `web` feature adds an \
     `unsafe impl Send` beside it. Threaded wasm is not supported."
);

// So that keylink can expand its own `service!`.
//
// The macro's generated rustdoc links its way back to this crate by name —
// `[`Servable::service`](keylink::Servable::service)`,
// `[`mount`](keylink::mount)` and a dozen more — because
// every other expansion of it is rendered in somebody else's crate, where
// `keylink::` is the only spelling that resolves. Inside keylink there is no such
// path by default, so `mise run docs` (`-D warnings`, broken intra-doc links
// included) fails on every one of those links the moment the crate expands the
// macro on a non-`#[cfg(test)]` item. `representability.rs` has expanded it for
// waves without hitting this, because rustdoc does not render test-only code.
//
// The alternative was `#[allow(rustdoc::broken_intra_doc_links)]` inside the
// macro, which would have turned the links off in **every** downstream crate to
// fix one crate's problem. This turns them on here instead.
extern crate self as keylink;

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
// The name a keyed call has inside its own handler, and a set to hold what has
// been applied under one. Its own module and not four items in `idempotency.rs`,
// because nothing here is the ledger's: the ledger is what stops running a
// duplicate for as long as the process lives, and this is what a caller's own
// state does about the restart the ledger cannot cross.
mod call;
// A `pub mod` and not six root re-exports. The one thing these five share is an
// argument about backpressure, and a module is where it can be written once
// instead of six times. Its docs live in `compose/mod.rs`: an outer `///` here
// beside the inner `//!` there merges the two and resolves the merged doc's
// intra-doc links in *this* file's scope, which turns every `[`Topic`]` in that
// module doc into a broken link under `mise run docs`.
pub mod compose;
// A private module holding one `service!` invocation and the types around it.
// The generated module — `inspect` — is re-exported at the root below, so a
// caller writes `keylink::inspect::Client` and never `keylink::console::inspect`.
mod console;
mod error;
pub mod extend;
mod idempotency;
mod keys;
mod macros;
mod middleware;
mod node;
mod observe;
mod panic;
mod turns;
// Where an answer waits for the verdict of the turn that produced it. Its own
// module and not four items in `turns.rs`, because the invariant it holds — a
// lot is a local of one turn, so nothing can be parked outside one — is a
// property of these types together and of nothing else in that file.
mod parking;
mod peer;
/// The durability adapter. Gated on the feature AND nothing else: unlike
/// `blocking`, every line in it compiles for the browser, which is what makes
/// `persist` and `persist-file` two switches rather than one.
#[cfg(feature = "persist")]
mod persist;
mod reply;
#[cfg(test)]
mod representability;
mod rpc;
mod subscribers;
mod surfaces;
#[cfg(test)]
mod testing;
mod watch;
mod wire;

pub use actor::{
    deliver_keyed, mount, mount_with_backpressure, serving_keyed, ActorStatus, Addressed, Eviction,
    KeyedService, Lifecycle, LocalError, LocalTellError, Mailbox, MountError, Mountable, Origin,
    Registry, RegistryBuilder,
};
pub use admission::{Admission, Admittance, DynamicAdmission, Revocable, Revocation};
pub use authz::{AdmittedPeer, Allowlist, ProvenPeer};
pub use blobs::{BlobHandle, BlobId, Blobs, BlobsAdmission, Granting, Retention, MAX_GRANTS};
pub use body::{Body, BodyReader};
pub use call::{Applied, CallId, APPLIED_BOUND};
pub use console::{
    Console, EventPage, EventStream, Events, FollowRefused, Inspector, LifecycleEvent, NodeKey,
    NodeStatus, MAX_FOLLOWERS,
};

/// The client and the wiring for keylink's own `Inspect` service.
///
/// # Five documented names, and it was the whole generated module
///
/// `service!` generates around thirty public items into a module — a `Request`,
/// a `Response`, a `Mail`, a `Feed`, a keyed `Registry` and its builder, a
/// `RegistryClient`, six spawn doors and a `blocking` submodule. For a service **a
/// caller writes**, all of that is theirs and belongs in their crate. `Inspect`
/// is keylink's own, so every one of those was keylink's public API —
/// `std::mem::size_of::<keylink::inspect::Mail>()` compiled — which is a
/// compatibility promise about the shape of a mailbox enum nobody outside this
/// crate can construct, consume or name a use for.
///
/// The wave that added this argued for two of them, the `Client` and the
/// `alpn`. These five are that argument applied: what a program needs to **wire
/// an inspector into a server** and what a console needs to **read one**. The
/// `Inspect` service is never keyed, never hand-mounted and never implemented by
/// anybody but [`Inspector`], so none of the rest has a caller.
///
/// A second, `#[doc(hidden)]` group follows it, and it is not a second surface:
/// those are the names the *generated documentation above* cross-references,
/// because `service!` writes its docs for a module whose siblings are all
/// public. Hiding rather than dropping them is the house rule's middle tier —
/// `#[doc(hidden)] pub` when only the macro must name it — and it is what keeps
/// `mise run docs` a gate rather than twelve broken links.
///
/// The hidden re-export list is reviewed directly; name-absence compile-fail guards are not retained.
pub mod inspect {
    pub use crate::console::inspect::{alpn, restarting, setup, Client, Feeds, Handle, Setup};

    /// Named only so that the generated rustdoc above resolves its own
    /// cross-references. Nothing outside this crate has a use for one.
    ///
    /// `IdempotentClient` joined this list after the fact, and how it got here is
    /// the argument for the list existing. It was added to `service!` on one
    /// branch while this narrowing was written on another; each was green alone,
    /// and `mise run docs` went red the first time the two were built together.
    /// A generated doc comment cross-references its own module's siblings, so
    /// **every** name `service!` learns to mention is a name this list must learn
    /// too — which is a maintenance cost worth naming rather than a one-off.
    #[doc(hidden)]
    pub use crate::console::inspect::{
        actor, at_local, serve, service, spawn, Actor, Handler, IdempotentClient, OwnedPermit,
        Permit, RegistryClient, RestartingSetup, StateSetup,
    };
}
pub use authz::{Authorization, Compiled, Rules};
#[cfg(feature = "blocking")]
pub use error::BlockingError;
pub use error::{ConfigError, Error, Failure, Queued, Result, TellError, Transmitted, Transport};
pub use idempotency::{
    Attaching, Idempotency, IdempotencyRefusal, KeyState, Keyable, Limits, Records, MAX_ATTACH,
    MAX_RECORDED_BYTES, MAX_RECORDED_FRAME, MAX_RECORDS, MAX_RUNNING, MAX_RUNNING_PER_PEER,
    MAX_TOMBSTONES, MAX_WAITING, MAX_WAITING_PER_PEER, RECORD_TTL,
};
pub use keys::{ActorId, IdempotencyKey, MAX_KEY_LEN};
pub use middleware::{
    Caller, Context, Middleware, Next, QueueTimeout, QueueTimeoutError, Queueing, Rpc, Timeout,
};
pub use node::{derive_alpn, Node};
pub use observe::{
    EndReason, Ended, FinishFut, Observer, Serving, Stop, TurnFut, TurnInfo, TurnLost, TurnOutcome,
    Withheld,
};
pub use panic::Panic;
pub use peer::{Dial, Peer};
#[cfg(feature = "persist")]
pub use persist::{Opened, Persist};
pub use reply::{Outcome, RemoteError, WireRequest, WireResponse};
pub use rpc::{
    Dispatch, Governed, GovernedSeal, IdempotentClient, RpcClient, Server, ServerBuilder, Service,
    ServiceBuilder,
};
pub use subscribers::Subscribers;
#[doc(hidden)]
pub use surfaces::RegistryDispatch;
pub use surfaces::{BlockingWire, Own, Servable, Status, Wire, WithIdempotency};

/// Extension traits for generated caller surfaces.
pub mod prelude {
    pub use crate::{BlockingWire, Own, Servable, Status, Wire, WithIdempotency};
}
pub use turns::{
    deliver_unkeyed, feed_turn, run_keyed, run_keyed_supervised, serving_unkeyed, turn,
    turn_deferred, Backpressure, Deferred, Fed, Feeds, Hooks, Incarnation, Intensity, LocalActor,
    NotQueued, OwnedPermit, Permit, Replied, Reply, Turn, Unanswered,
};
pub use watch::{Departure, Watch};
pub use wire::{Envelope, WireKinds, MAX_HEADER};

/// Re-exported so generated code and callers agree on types without extra
/// imports at the call site.
pub use iroh::{protocol::Router, EndpointAddr, EndpointId, SecretKey};

/// The trait a `feed` is wired from, re-exported for the same reason.
///
/// A generated `Feeds::new` takes `impl Stream<Item = ..>`, so this trait is in
/// keylink's public API whether or not it is named here — and it is
/// `futures_core::Stream`, the one `tokio_stream`, `futures` and `async_stream` all
/// implement, reached through the runtime shim the rest of the crate uses. Naming it
/// here means a caller wiring a feed needs no direct dependency on any of them.
pub use n0_future::Stream;
#[doc(hidden)]
pub use pastey;
