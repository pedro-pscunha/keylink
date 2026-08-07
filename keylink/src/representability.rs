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
//! Shadowing also reads a **receiver** rather than a name, and the last probe in
//! this file is that shape. Give one type an inherent `&mut self` method beside
//! a trait `&self` method of the same name, returning different markers: a `&T`
//! matches the trait method at its first receiver step, while a `&mut T` matches
//! the inherent one a step earlier and outranks the trait there. The marker that
//! comes back therefore says which borrow the caller was holding, so asserting
//! on it pins a signature that promises `&T`.
//!
//! | The claim | The probe |
//! |---|---|
//! | no inherent method of this name | shadowing, on the name |
//! | no impl of this trait | coherence |
//! | this parameter is `&T` and never `&mut T` | shadowing, on the receiver step |
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
    ActorId, Admission, Admittance, AdmittedPeer, Allowlist, BlobId, DynamicAdmission, Eviction,
    IdempotencyKey, LocalActor, MailboxClosed, Observer, ProvenPeer, Retention, Sealed, TurnFut,
    TurnInfo,
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
/// The adapter inherits the property from the value it holds. A `Default` here
/// would mean "a policy you did not name", and the only policy nobody named is
/// the one that admits everyone.
impl HasNoDefault for DynamicAdmission {}

// ─────────────────────────────────── `Admission` has no permissive blanket impl

/// The claim: nothing implements [`Admission`](crate::Admission) for every
/// type, so a policy is something a caller writes rather than something they
/// get by accident.
///
/// The probe is coherence, not shadowing, because a trait impl is invisible to
/// name resolution. A concrete `impl Admission for Doorstop` overlaps a
/// hypothetical `impl<T> Admission for T` exactly, and an overlap is `E0119`.
/// So this impl compiles only while no blanket impl exists.
///
/// A blanket impl is the shape this crate must never grow: it would make every
/// type a policy, including `()`, and the answer would be whatever the blanket
/// said — which is the fail-open `Allowlist` has no `Default` to prevent.
struct Doorstop;

impl Admission for Doorstop {
    fn admits(&self, _peer: &ProvenPeer) -> Admittance {
        Admittance::Refused
    }
}

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

// ───────────────────────── an observer sees the state, and cannot call into it

/// A state with one method, in the shape `service!` generates: the `Actor`
/// trait a user writes is a set of `async fn method(&mut self, ..)`, and `post`
/// is `durable_ledger`'s own with the parts a probe does not need taken off.
///
/// The method is inherent rather than declared on a trait, and that makes the
/// probe below stronger rather than weaker. An inherent method outranks a trait
/// method at the same receiver step, so a receiver that cannot reach this one
/// cannot reach a `service!` method either.
struct Ledger {
    entries: Vec<u64>,
}

impl Ledger {
    fn post(&mut self) -> ThroughMut {
        self.entries.push(0);
        ThroughMut
    }
}

/// Which borrow a call to `post` went through. Two markers rather than the one
/// [`NoSuchMethod`] the probes above share, because this probe is not asking
/// whether a method exists — both of them do — but which of the two the
/// receiver was able to reach.
struct ThroughShared;

/// The answer only a `&mut Ledger` can get. Nothing in this module should ever
/// produce one; it exists so that reaching the mutable method is a type error
/// with a name on it rather than a silent pass.
struct ThroughMut;

/// The `&self` half of the pair. A `&Ledger` finds this one, and a `&mut Ledger`
/// never gets here because it matches the inherent method a step earlier.
trait LedgerIsReadableThroughAShared {
    fn post(&self) -> ThroughShared {
        ThroughShared
    }
}

impl LedgerIsReadableThroughAShared for Ledger {}

/// The claim, RFC-06 §2.8: [`Observer::after_turn`](crate::Observer) takes `&A`
/// and never `&mut A`, so no observer can call a `service!` method or assign to
/// a field.
///
/// The probe is shadowing rather than coherence, because the claim is about name
/// resolution and there is no impl whose absence would state it — coherence has
/// nothing to overlap here. What it reads is the *receiver step*. A call on a
/// `&Ledger` begins at self type `&Ledger`, where only `&self` methods live, so
/// it finds the trait default and answers `ThroughShared`. A call on a
/// `&mut Ledger` begins one step earlier, at self type `&mut Ledger`, where the
/// inherent `&mut self` method matches — and, being inherent, outranks the trait
/// — so it answers `ThroughMut`. The assertion names the first, so it holds
/// exactly while the parameter is a shared reference.
///
/// The break: change `state: &'a A` to `state: &'a mut A` on
/// `Observer::after_turn` in `observe.rs`. Leave this impl as written and it is
/// a signature mismatch, `E0053`. Update it to match and `state.post()` resolves
/// to the inherent method, so the assertion is `E0308`, expected `ThroughShared`
/// and found `ThroughMut`. There is no third answer, which is the point.
///
/// # What this does not prove
///
/// It does not prove that an observer cannot mutate the state, and no probe here
/// could. `&A` does not stop interior mutability: a state with an `AtomicU64`, a
/// `Mutex` or a `RefCell` field is mutable through `&A`, and an observer holding
/// one will mutate it with the compiler's blessing. What is proved is the
/// narrower thing §2.8 states and nothing beyond it — the `&mut self` route is
/// shut, so a `service!` method cannot be called and a field cannot be assigned.
/// Whether any other route exists is decided by the state's own fields, and
/// those are the caller's choice rather than keylink's.
///
/// It is silent about a *second* method, too. An `Observer` that grew
/// `after_turn_mut(&mut self, state: &mut A)` would be a new port beside this
/// one, and this probe would stay green next to it.
struct Recorder;

impl Observer<Ledger> for Recorder {
    fn name(&self) -> &'static str {
        "recorder"
    }

    fn after_turn<'a>(&'a mut self, state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        let _: ThroughShared = state.post();
        Box::pin(async { Ok(()) })
    }
}

// ──────────────────────────── the same borrow, one rung down: LocalActor::read

/// §2.8 names `Handle::with_state` in the same breath as the observer port, and
/// it is the same claim: the closure takes `&A`, so a concern bolted on from
/// outside cannot quietly become a second way to change the state.
///
/// The probe is the same one read through a bound rather than through a
/// parameter. [`LocalActor::read`] promises `F: FnOnce(&A) -> T`, so the
/// closure's argument is a `&Ledger` and `post` resolves to the trait default.
/// Widen that bound to `FnOnce(&mut A) -> T` in `local.rs` and the argument is a
/// `&mut Ledger`, the inherent method matches a step earlier, `T` infers as
/// `ThroughMut`, and the annotation below says otherwise: `E0308`.
///
/// This proves the crate's own read path and not the macro's. The `with_state`
/// generated in `macros.rs` re-declares `FnOnce(&A) -> T` before forwarding
/// here, so the two bounds can drift apart, and closing that would take a probe
/// inside `mod expansion` beside `no_streaming_on_a_local_handle`.
///
/// The limit above carries over unchanged. A read through `&A` cannot call a
/// `service!` method; it can still touch a `Mutex` field.
async fn a_read_cannot_mutate_either(actor: &LocalActor<Ledger>) {
    let _: Result<ThroughShared, MailboxClosed> = actor.read(|state| state.post()).await;
}
