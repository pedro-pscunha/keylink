//! Compile-time proof for the claims that say a state cannot be represented.
//!
//! keylink asserts, in prose, several things of the form "there is no way to do
//! X". The loudest today is on [`Origin`](crate::Origin): *"**Do not derive
//! `Clone`.** Two messages sharing one progress marker means one of them records
//! a reply it never produced."*
//!
//! That is a comment asking a future author to be careful. This module asks the
//! compiler instead. Nothing here runs; the proof is that it compiles.
//!
//! # Three probes, because one cannot see everything
//!
//! **An inherent method outranks a trait method.** So a trait whose default
//! method returns a marker type is a probe: while the type has no inherent
//! method of that name, the call resolves to the trait and the marker checks
//! out. Add `pub fn as_mut(&mut self) -> &mut [u8]` and the call resolves to the
//! inherent method, whose return type is not the marker, and this module stops
//! compiling.
//!
//! **A trait impl is invisible to name resolution**, so it needs coherence
//! instead. A blanket `impl<T: Clone> Marker for T` beside a concrete
//! `impl Marker for Origin` overlaps exactly when `Origin: Clone`, and an
//! overlap is `E0119`. The concrete impl therefore compiles only while the trait
//! is absent.
//!
//! **Some claims are positive**, and those are written as ordinary code that
//! has to type-check. "A per-service actor system cannot hold another service's
//! actor" is one: the annotated mount below compiles only while the two are the
//! same service. It is deliberately NOT a `compile_fail` doctest — see
//! [`a_system_mounts_only_its_own_services_actors`] for why that tool cannot
//! state this claim at all.
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
//! note: upstream crates may add a new impl of trait `std::clone::Clone`
//!       for type `keylink::Origin` in future versions
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

use std::ops::DerefMut;

use crate::{
    ActorId, Admission, Admittance, AdmittedPeer, Allowlist, BlobId, Caller, DynamicAdmission,
    Ended, Eviction, FinishFut, IdempotencyKey, LocalActor, MailboxClosed, MountError, Observer,
    Origin, ProvenPeer, Retention, TurnFut, TurnInfo,
};

/// Returned by every shadowing probe. Distinct from anything a real accessor
/// would return, so an inherent method that shadows one is a type error rather
/// than a silent pass.
struct NoSuchMethod;

// ─────────────────────────────────── Origin is not Clone, and must never be

/// The claim, and it is the sharpest one in the crate: **[`Origin`] is not
/// `Clone`.**
///
/// `Origin::Peer` owns a [`Context`](crate::Context), whose `progress` field is
/// an `Arc<AtomicU8>` that its own `Clone` shares deliberately — that sharing is
/// what lets a turn running on the actor's task report back to the accept task
/// that dispatched it. `Progress::reached()` is what decides whether an
/// idempotency key is **recorded** or **released**.
///
/// So two mailbox messages carrying clones of one context would share one
/// marker whose bits are `fetch_or` and never cleared: the one that reached
/// `COMPLETED` would make the other read `Reached::Completed` and record a reply
/// it never produced. At-most-once, answered wrongly, silently, on the wire.
///
/// The delivery path works by **ownership** — the context is taken by value and
/// moved into the message — so nothing needs `Clone` and nothing ever should.
/// An earlier draft of RFC-07 §1.6 justified the design with "since `Context` is
/// `Clone`", which is why this probe exists rather than a sentence: the wrong
/// justification was already written down once.
///
/// The break: add `#[derive(Clone)]` to `Origin` in `actor/remote.rs`. This
/// impl then overlaps the blanket one and it is `E0119: conflicting
/// implementations`.
trait OriginIsNotClone {}
impl<T: Clone> OriginIsNotClone for T {}
impl OriginIsNotClone for Origin {}

/// The other half, which coherence cannot see: no inherent method hands out a
/// second `Origin` either. `Clone` is the obvious spelling; these are what an
/// author reaches for when the derive is refused and the need feels real.
///
/// # A third half this file cannot reach, and saying so is the point
///
/// Neither probe touches the hazard as it would actually arrive, because **nobody
/// duplicates the `Origin` — the caller clones the `Context` on the way in.**
/// `deliver_to_actor` is `#[doc(hidden)] pub` and takes a `Context` by value, so
/// two deliveries sharing one `Progress` type-check without naming `Origin`, and a
/// probe can only refuse a shape that does *not* type-check. There is no negative
/// claim to make here and this module must not imply one.
///
/// What holds the two halves it *can* reach: coherence for the derive, and the
/// private field for downstream construction (a `compile_fail,E0624` doctest on
/// `Origin`, since a visibility claim is only false from outside the crate).
/// What holds the third is the call graph — one `Context` per request in
/// `serve_one`, delivered once — pinned as
/// `actor::remote::tests::two_origins_over_one_cloned_context_still_share_one_marker`,
/// which asserts the sharing is real so that the prose above cannot quietly become
/// a guarantee again.
trait OriginHasNoDuplicator {
    fn duplicate(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn copy(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn to_owned(&self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl OriginHasNoDuplicator for Origin {}

fn an_origin_cannot_be_duplicated(origin: Origin) {
    let _: NoSuchMethod = origin.duplicate();
    let _: NoSuchMethod = origin.copy();
    let _: NoSuchMethod = origin.to_owned();
    // And the one thing it IS for: a borrowed caller, for one turn.
    let _: Caller<'_> = origin.caller();
}

// ─────────────────────── a per-service system holds only its own service

// Two methods each and not one, which is about the macro rather than about the
// probe: a `service!` with a single rpc generates an `into_<rpc>` whose
// catch-all arm is `unreachable_patterns`. No downstream service reports it —
// rustc suppresses the lint inside an external macro's expansion, which is the
// same asymmetry `macros.rs` records about `double_must_use` — but keylink
// expanding its own macro is not external to itself, and the lib test build
// denies warnings. `tests/actor_memory.rs` declares exactly one rpc and stays
// green for that reason.
crate::service! {
    service Alpha;
    rpc Ping(n: u64) -> u64;
    rpc Pong(n: u64) -> u64;
}

crate::service! {
    service Beta;
    rpc Ping(n: u64) -> u64;
    rpc Pong(n: u64) -> u64;
}

/// Deliberately identical to [`BetaState`] in every way but the trait it
/// implements. Two services with the same shape are the case that matters: if
/// the separation came from the message *shape* rather than from the service,
/// two services that happen to agree would be interchangeable.
struct AlphaState;

impl alpha::Actor for AlphaState {
    async fn ping(&mut self, _cx: Caller<'_>, n: u64) -> u64 {
        n
    }
    async fn pong(&mut self, _cx: Caller<'_>, n: u64) -> u64 {
        n
    }
}

struct BetaState;

impl beta::Actor for BetaState {
    async fn ping(&mut self, _cx: Caller<'_>, n: u64) -> u64 {
        n
    }
    async fn pong(&mut self, _cx: Caller<'_>, n: u64) -> u64 {
        n
    }
}

/// The keyed and singleton contracts of one service must not collide, or a node
/// could not serve both — [`ConfigError::DuplicateAlpn`](crate::ConfigError) is
/// what a collision would become, at startup, on a tree where nothing looked
/// wrong.
///
/// And `alpha` must not agree with `beta`, which is the property that makes an
/// ALPN a contract identifier at all. Both are runtime assertions rather than
/// probes, because the values exist and there is nothing to be unable to
/// represent.
#[test]
fn a_services_two_alpns_differ_from_each_other_and_from_another_services() {
    assert_ne!(alpha::alpn(), alpha::keyed_alpn());
    assert_ne!(alpha::keyed_alpn(), beta::keyed_alpn());
    assert_ne!(alpha::alpn(), beta::alpn());
    assert!(
        String::from_utf8_lossy(alpha::keyed_alpn()).contains("/Alpha/keyed/"),
        "the keyed ALPN says which of the two it is without anyone hashing anything"
    );
}

/// The claim the whole keyed rewrite rests on: **a per-service `System`
/// physically cannot hold another service's actor**, so the mismatch is
/// unrepresentable rather than refused at run time on the serving node.
///
/// # Why this is positive code and not a `compile_fail` doctest
///
/// Because `compile_fail` cannot state it. **rustdoc ignores the error code**: a
/// doctest annotated `compile_fail,E0308` that in fact fails with `E0425`
/// (a typo in an identifier) or `E0282` (an inference failure) passes green and
/// silently, so the annotation asserts only "something went wrong somewhere".
/// `trybuild` is the one tool that matches stderr, and this module's header
/// explains why it is refused. So the claim is written the only way a compiler
/// can be made to check it: as code that must type-check, whose types are
/// annotated rather than inferred.
///
/// The annotations are the assertion. `let mounted: alpha::Mounted` pins the
/// generated alias, so this stays a statement about `alpha`'s system rather than
/// about whatever inference happened to pick.
///
/// The break, executed by hand rather than described: swap `alpha::spawn` for
/// `beta::spawn` below. Observed —
///
/// ```text
/// error[E0308]: mismatched types
///    |     let mounted: alpha::Mounted = beta::spawn(BetaState);
///    |                  --------------   ^^^^^^^^^^^^^^^^^^^^^^ expected `Mounted<Mail<alpha::Request, alpha::Response>>`,
///    |                                                           found `Mounted<Mail<beta::Request, beta::Response>>`
/// ```
///
/// # What this does NOT prove
///
/// It proves **mount** safety, not ALPN-**pairing** safety.
/// `ServiceBuilder::new(alpn: &'static [u8], ..)` takes the ALPN as an untyped
/// slice, so nothing at the type level stops `alpha`'s dispatch from being
/// registered under `beta`'s ALPN. What prevents that is the generated pairing —
/// `alpha::keyed_service` is the only thing that calls `alpha::keyed_alpn()` —
/// and a hand-written registration could still get it wrong. Saying so is the
/// difference between a probe and a claim about a probe.
fn a_system_mounts_only_its_own_services_actors(
    system: &alpha::System,
    id: ActorId,
) -> Result<(), MountError> {
    let mounted: alpha::Mounted = alpha::spawn(AlphaState);
    system.mount(id, mounted)
}

/// The same property from the builder's side, so a future `System` that lost
/// its parameter would break here too rather than only above.
fn a_system_is_built_at_one_services_mail() -> beta::System {
    beta::System::builder(Eviction::Never)
        .capacity(beta::SystemBuilder::DEFAULT_CAPACITY)
        .build()
}

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
/// # The second method, and the hole that is closed versus the one that is not
///
/// `Observer` grew one: [`finish`](crate::Observer::finish), which sees the state
/// once more before it is dropped. This impl covers it, so the claim now reads
/// "**every** method of this port lends the state through a shared reference"
/// rather than "the one method does" — a `&mut A` on `finish` alone would be
/// `E0053` here, and a `finish` that resolved `post` to the inherent method would
/// be `E0308` on the same annotation.
///
/// That is a **different and weaker** hole than the one this doc used to name. It
/// is still silent about an `after_turn_mut(&mut self, state: &mut A)`: a *new*
/// method added beside these two would be a new port, and this probe would stay
/// green next to it. What changed is that the second method which actually
/// landed is covered instead of unwatched; what did not change is that no probe
/// here can forbid a third.
struct Recorder;

impl Observer<Ledger> for Recorder {
    fn name(&self) -> &'static str {
        "recorder"
    }

    fn after_turn<'a>(&'a mut self, state: &'a Ledger, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        let _: ThroughShared = state.post();
        Box::pin(async { Ok(()) })
    }

    fn finish<'a>(&'a mut self, state: &'a Ledger, _ended: &'a Ended) -> FinishFut<'a> {
        let _: ThroughShared = state.post();
        Box::pin(async {})
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
///
/// # Generic in `M`, and that is itself the claim
///
/// `LocalActor` gained a second parameter — what its mailbox carries — when the
/// mailbox stopped being a boxed closure. This probe is written over **any** `M`
/// rather than over one service's generated enum, and it compiles, which states
/// the property that made the parameter affordable: `read` needs to know nothing
/// about a turn. Bound it to `M: Turn<A>` in `local.rs` and this stops compiling
/// — `Ledger` is hand-written, is not a `service!`, and has no mailbox enum to
/// name.
async fn a_read_cannot_mutate_either<M>(actor: &LocalActor<Ledger, M>) {
    let _: Result<ThroughShared, MailboxClosed> = actor.read(|state| state.post()).await;
}
