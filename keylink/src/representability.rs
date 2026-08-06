//! Compile-time proof for the claims that say a state cannot be represented.
//!
//! keylink asserts, in prose, several things of the form "there is no way to do
//! X". The loudest is on [`Sealed`](crate::Sealed): *"There is no way to read
//! these bytes... **Do not** add `bytes()`, `peek()`, `len()`... Any of them
//! reopens the hole."*
//!
//! That is a comment asking a future author to be careful. This module asks the
//! compiler instead. Nothing here runs; the proof is that it compiles.
//!
//! # Two probes, because one cannot see everything
//!
//! **An inherent method outranks a trait method.** So a trait whose default
//! method returns a marker type is a probe: while the type has no inherent
//! method of that name, the call resolves to the trait and the marker checks
//! out. Add `pub fn bytes(&self) -> &[u8]` and the call resolves to the inherent
//! method, whose return type is not the marker, and this module stops compiling.
//!
//! **A trait impl is invisible to name resolution**, so it needs coherence
//! instead. A blanket `impl<T: Deref> Marker for T` beside a concrete
//! `impl Marker for Sealed` overlaps exactly when `Sealed: Deref`, and an
//! overlap is `E0119`. The concrete impl therefore compiles only while the trait
//! is absent.
//!
//! | The claim | The probe |
//! |---|---|
//! | no inherent method of this name | shadowing |
//! | no impl of this trait | coherence |
//!
//! # Why this module is inside the crate rather than in `tests/`
//!
//! The coherence probe only works on a LOCAL type. Written in an integration
//! test, every one of these fails on a clean tree, because rustc will not reason
//! negatively about a foreign type:
//!
//! ```text
//! note: upstream crates may add a new impl of trait `std::ops::Deref`
//!       for type `keylink::Sealed` in future versions
//! ```
//!
//! Which is the correct answer to the question an integration test asks. The
//! question worth asking is whether *this crate* adds the impl, and only this
//! crate can ask it.
//!
//! # Why not `trybuild`
//!
//! `trybuild` is the usual tool for compile-fail assertions. It matches the full
//! rustc stderr, so a compiler release that rewords a diagnostic turns the gate
//! red on a tree nobody touched. These probes carry no error text at all, cost
//! nothing to maintain, and compile as part of the ordinary test build.

#![cfg(test)]
#![allow(dead_code, reason = "the proof is that this module compiles")]

use std::ops::{Deref, DerefMut};

use crate::{
    ActorId, AdmittedPeer, Allowlist, BlobId, Eviction, IdempotencyKey, ProvenPeer, Retention,
    Sealed,
};

/// Returned by every shadowing probe. Distinct from anything a real accessor
/// would return, so an inherent method that shadows one is a type error rather
/// than a silent pass.
struct NoSuchMethod;

// ─────────────────────────────────────────── Sealed: no inherent way to read it

/// Every name here is one an author would reach for when adding "just a quick
/// accessor" to [`Sealed`](crate::Sealed).
///
/// The type exists so that payload bytes cannot be read before their schema hash
/// is checked, and `Sealed::open` is meant to be the only route from bytes to a
/// typed message. Each of these would be a second route.
trait SealedHasNoAccessor {
    fn bytes(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn peek(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn len(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn as_slice(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn as_bytes(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn payload(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn into_inner(self) -> NoSuchMethod
    where
        Self: Sized,
    {
        NoSuchMethod
    }
}

impl SealedHasNoAccessor for Sealed {}

fn sealed_cannot_be_read(sealed: Sealed) {
    let _: NoSuchMethod = sealed.bytes();
    let _: NoSuchMethod = sealed.peek();
    let _: NoSuchMethod = sealed.len();
    let _: NoSuchMethod = sealed.as_slice();
    let _: NoSuchMethod = sealed.as_bytes();
    let _: NoSuchMethod = sealed.payload();
    let _: NoSuchMethod = sealed.into_inner();
}

// ──────────────────────────────────────────── Sealed: no trait way out either

/// The accessor probes above genuinely cannot see a `Deref`. A trait method on
/// `Self` outranks a deref step, so they would keep resolving to the trait and
/// stay green while `sealed.len()` quietly started working through `Vec<u8>`.
///
/// Each of these is a route from a `Sealed` to its bytes that skips
/// `Sealed::open`, which is the one thing the type exists to prevent.
trait SealedDoesNotDeref {}
impl<T: Deref> SealedDoesNotDeref for T {}
impl SealedDoesNotDeref for Sealed {}

trait SealedIsNotBorrowable {}
impl<T: AsRef<[u8]>> SealedIsNotBorrowable for T {}
impl SealedIsNotBorrowable for Sealed {}

trait SealedIsNotUnwrappable {}
impl<T: Into<Vec<u8>>> SealedIsNotUnwrappable for T {}
impl SealedIsNotUnwrappable for Sealed {}

// ───────────────────────────────────────────────── fail-closed has no `Default`

/// [`Allowlist`](crate::Allowlist) has no `Default` on purpose: admitting
/// everyone must be typed out at the call site. [`Eviction`](crate::Eviction)
/// has none for the same shape of reason — an actor system that only grows is a
/// leak, and whether dropping state loses anything is a decision about the
/// caller's data rather than ours.
///
/// [`Retention`](crate::Retention) is the third of the same family: a store
/// that grows until the disk fills is a decision about *your* disk, so it is
/// typed out rather than inherited.
///
/// A hand-written impl trips this, and so does a `#[derive(Default)]`.
trait HasNoDefault {}
impl<T: Default> HasNoDefault for T {}
impl HasNoDefault for Allowlist {}
impl HasNoDefault for Eviction {}
impl HasNoDefault for Retention {}

// ───────────────────────────────── a proven peer cannot be built from a claim

/// [`ProvenPeer`](crate::ProvenPeer) keys the blobs grant ledger, and its
/// whole value is that the identity inside it was proved by a TLS handshake
/// keylink itself accepted. `ProvenPeer::on(&Connection<HandshakeCompleted>)` is
/// meant to be the only route in.
///
/// Every name below is one an author would reach for when adding a second one,
/// and each would take an identity that was *asserted* rather than proved:
///
/// - `proved` is RFC-01 §5.3's own proposal, over the
///   `Option<EndpointId>` iroh-blobs reports. That field is `None` for any
///   transport with no proven identity, and the `None` arm is unreachable
///   through `BlobsProtocol` today — so a mistake in it passes every test
///   anybody can write.
/// - `new`, `from_id` and `trusted` take a bare `EndpointId`, which is what an
///   id parsed out of a request body looks like by the time it reaches here.
/// - `dialling` is the trap in the other direction:
///   `Connecting::remote_id()` is infallible and reports the peer we are
///   dialling, so a constructor over it would compile and prove nothing.
///
/// The shadowing probe fires on an inherent associated function of the same
/// name, whatever it takes as an argument.
#[allow(
    clippy::new_ret_no_self,
    reason = "returning Self is exactly what this probe watches for. `new` is \
              the first name an author reaches for, so dropping it to satisfy \
              the lint would leave the likeliest hole uncovered"
)]
trait ProvenPeerHasOneConstructor {
    fn proved() -> NoSuchMethod {
        NoSuchMethod
    }
    fn new() -> NoSuchMethod {
        NoSuchMethod
    }
    fn from_id() -> NoSuchMethod {
        NoSuchMethod
    }
    fn trusted() -> NoSuchMethod {
        NoSuchMethod
    }
    fn dialling() -> NoSuchMethod {
        NoSuchMethod
    }
}

impl ProvenPeerHasOneConstructor for ProvenPeer {}

fn a_proven_peer_has_one_constructor() {
    let _: NoSuchMethod = ProvenPeer::proved();
    let _: NoSuchMethod = ProvenPeer::new();
    let _: NoSuchMethod = ProvenPeer::from_id();
    let _: NoSuchMethod = ProvenPeer::trusted();
    let _: NoSuchMethod = ProvenPeer::dialling();
}

/// The other half: a `ProvenPeer` must not be constructible by conversion
/// either. A `From<EndpointId>` would be invisible to the probes above, because
/// a trait impl is invisible to name resolution — so it needs coherence.
///
/// `EndpointId` is a foreign type, so the blanket impl is written over
/// `From<T>` in general rather than over that one source. Any `From` impl at
/// all overlaps it, which is the right bar: `ProvenPeer` is built by one
/// constructor, from one argument, and nothing else.
trait ProvenPeerIsNotConvertible {}
impl<T: From<crate::EndpointId>> ProvenPeerIsNotConvertible for T {}
impl ProvenPeerIsNotConvertible for ProvenPeer {}

// ──────────────────────────────── admission has exactly one source of proof

/// [`AdmittedPeer`](crate::AdmittedPeer) is the rung above
/// [`ProvenPeer`](crate::ProvenPeer), and the two answer different questions:
/// one says *who*, the other says *whether an `Allowlist` said yes*. The whole
/// value of the second is that `Allowlist::admit` is the only way to reach it,
/// so a function taking one has proof that a policy ran.
///
/// The blobs mount is why this matters rather than being tidy. It is a second
/// accept path onto a node whose other protocols are allowlisted, and it writes
/// authorization records. If it could build an `AdmittedPeer` from a connection
/// — or from an id, or from a `ProvenPeer` it happened to be holding — it would
/// be a way past every policy on the node with the compiler silent.
///
/// Each name below is a plausible second door: `admit` and `new` taking an id,
/// `proven` or `trust` promoting a `ProvenPeer` without asking anybody.
#[allow(
    clippy::new_ret_no_self,
    reason = "returning Self is exactly what this probe watches for. `new` is \
              the first name an author reaches for, so dropping it to satisfy \
              the lint would leave the likeliest hole uncovered"
)]
trait AdmittedPeerHasOneSource {
    fn new() -> NoSuchMethod {
        NoSuchMethod
    }
    fn admit() -> NoSuchMethod {
        NoSuchMethod
    }
    fn trust() -> NoSuchMethod {
        NoSuchMethod
    }
    fn trusted() -> NoSuchMethod {
        NoSuchMethod
    }
    fn assume() -> NoSuchMethod {
        NoSuchMethod
    }
}

impl AdmittedPeerHasOneSource for AdmittedPeer {}

fn admission_has_one_source() {
    let _: NoSuchMethod = AdmittedPeer::new();
    let _: NoSuchMethod = AdmittedPeer::admit();
    let _: NoSuchMethod = AdmittedPeer::trust();
    let _: NoSuchMethod = AdmittedPeer::trusted();
    let _: NoSuchMethod = AdmittedPeer::assume();
}

/// The conversion half, which the shadowing probes cannot see.
///
/// A `From<ProvenPeer>` would be the quietest possible way to lose this: it
/// reads as a widening, it needs no call site to look suspicious, and
/// `.into()` at one site would promote identity to admission with nothing
/// asked. `From<EndpointId>` is the same hole one rung lower.
trait AdmissionIsNotAWidening {}
impl<T: From<ProvenPeer>> AdmissionIsNotAWidening for T {}
impl AdmissionIsNotAWidening for AdmittedPeer {}

trait AdmissionIsNotBuiltFromAnId {}
impl<T: From<crate::EndpointId>> AdmissionIsNotBuiltFromAnId for T {}
impl AdmissionIsNotBuiltFromAnId for AdmittedPeer {}

// ──────────────────────────────── a content address is not a set of raw bytes

/// [`BlobId`](crate::BlobId) is a capability: holding one is how a peer reaches
/// content. `as_bytes` is the intended read, and it hands out a shared
/// reference.
///
/// Anything handing out `&mut` would let a caller edit an address in place
/// after it was recorded in the grant ledger, which turns one grant into a
/// capability for whatever the caller writes next. The ledger stores a copy, so
/// the mutation would not widen an existing grant — it would silently redirect
/// the id a handler is about to return, which is the same bug one step earlier.
trait BlobIdHasNoMutableAccessor {
    fn as_mut_bytes(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn as_mut(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl BlobIdHasNoMutableAccessor for BlobId {}

fn a_content_address_cannot_be_edited(mut id: BlobId) {
    let _: NoSuchMethod = id.as_mut_bytes();
    let _: NoSuchMethod = id.as_mut();
}

/// A `From<[u8; 32]>` would be a public constructor for a capability, reachable
/// from any caller with 32 bytes to hand. keylink builds a [`BlobId`] from an
/// `iroh_blobs::Hash` — content the store actually holds — and from nothing
/// else.
///
/// This does not stop `derive(Deserialize)` building one from the wire, and it
/// is not meant to: an address a peer sent is a request, and the grant ledger
/// is what answers it. See the type's own documentation.
trait BlobIdIsNotBuiltFromRawBytes {}
impl<T: From<[u8; 32]>> BlobIdIsNotBuiltFromRawBytes for T {}
impl BlobIdIsNotBuiltFromRawBytes for BlobId {}

// ──────────────────────────────── a validated key cannot be edited afterwards

/// [`ActorId`](crate::ActorId) and [`IdempotencyKey`](crate::IdempotencyKey) are
/// newtypes whose whole value is that an empty or oversized value cannot exist.
/// `as_str` is the intended read. Anything handing out `&mut` would let a caller
/// edit the string after the constructor proved something about it, which is the
/// standard way a newtype's guarantee is lost.
trait KeyHasNoMutableAccessor {
    fn as_mut_str(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn as_mut(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn into_inner(self) -> NoSuchMethod
    where
        Self: Sized,
    {
        NoSuchMethod
    }
}

impl KeyHasNoMutableAccessor for ActorId {}
impl KeyHasNoMutableAccessor for IdempotencyKey {}

fn a_validated_key_cannot_be_edited(mut actor: ActorId, mut key: IdempotencyKey) {
    let _: NoSuchMethod = actor.as_mut_str();
    let _: NoSuchMethod = actor.as_mut();
    let _: NoSuchMethod = key.as_mut_str();
    let _: NoSuchMethod = key.as_mut();
    let _: NoSuchMethod = actor.into_inner();
    let _: NoSuchMethod = key.into_inner();
}

trait KeyDoesNotDerefMut {}
impl<T: DerefMut> KeyDoesNotDerefMut for T {}
impl KeyDoesNotDerefMut for ActorId {}
impl KeyDoesNotDerefMut for IdempotencyKey {}

trait KeyIsNotBorrowMut {}
impl<T: AsMut<str>> KeyIsNotBorrowMut for T {}
impl KeyIsNotBorrowMut for ActorId {}
impl KeyIsNotBorrowMut for IdempotencyKey {}
