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
//!
//! # A claim no probe here can reach, and why its stand-in is a test
//!
//! **A cycle of blocking sends.** An `ask` awaiting an actor whose handler awaits
//! the first one back deadlocks, and nothing in this file can refuse it: a cycle
//! is a property of *which* handle a handler calls at run time, so there is no
//! name to shadow and no impl to overlap. `macros.rs` puts it as "no type can
//! tell one actor's handle from another's" — true of a ring of one service, and
//! a ring of two *different* services has two different `Handle` types that no
//! probe could refuse either.
//!
//! The `unbounded_channel` note further down is the **only** claim in this file
//! with an `rg` stand-in, and it earns one by being a token: its absence is
//! greppable. Every other claim-no-probe-can-reach here stands on prose or on a
//! named test, as §"A third half this file cannot reach" does. A cycle is built
//! out of ordinary sends that are each correct alone, so no pattern matches the
//! defect rather than the mechanism, and a grep can only bound the search:
//! `rg -c 'self\.mail\.send' keylink/src/turns.rs` reports `1` and exits 0,
//! because every local mailbox message funnels through `LocalActor::send` — and
//! that count says nothing about whether two trips through it form a loop, which
//! is the whole of the gap. The keyed path and `Topic`'s fan-out have their own
//! sends and are not behind that funnel.
//!
//! So the stand-in is a test rather than a probe: `keylink/tests/cycles.rs`
//! builds A → B → A and watches it hang under a timeout.

#![cfg(test)]
#![allow(dead_code, reason = "the proof is that this module compiles")]

use std::num::NonZeroUsize;
use std::ops::DerefMut;

use crate::compose::{BroadcastDepth, Lossy, LossySubscription, LossyTopic, Overflow, Topic};
use crate::{
    ActorId, ActorStatus, Admission, Admittance, AdmittedPeer, Allowlist, Backpressure, BlobId,
    Caller, DynamicAdmission, Ended, Eviction, FinishFut, IdempotencyKey, Intensity, Keyable,
    LocalActor, MountError, Observer, Origin, ProvenPeer, Records, Retention, TurnFut, TurnInfo,
    TurnOutcome, Unanswered,
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
/// `deliver_keyed` is `#[doc(hidden)] pub` and takes a `Context` by value, so
/// two deliveries sharing one `Progress` type-check without naming `Origin`, and a
/// probe can only refuse a shape that does *not* type-check. There is no negative
/// claim to make here and this module must not imply one.
///
/// What holds the two halves it *can* reach: coherence for the derive, and the
/// private constructors for downstream construction — a bare `compile_fail`
/// doctest on `Origin`, since a visibility claim is only false from outside the
/// crate. It fails with `E0624`, and **that code is named in prose there and
/// deliberately not on the fence**: rustdoc does not check the code after
/// `compile_fail`, so an annotated one asserts a verification that does not
/// happen. This sentence read `compile_fail,E0624` until Wave 1b's review, which
/// made it the fourth site in this workspace claiming a check rustdoc never
/// performs.
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
// The `tell` is here for the key-scoped probe below, and it has to be on a
// service this module already expands: a probe can only speak about a client
// type that exists.
crate::service! {
    service Alpha;
    rpc Ping(n: u64) -> u64;
    rpc Pong(n: u64) -> u64;
    tell Note(line: String) -> ();
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
    async fn note(&mut self, _cx: Caller<'_>, _line: String) {}
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
/// could not serve both — [`ConfigError::DuplicateAlpn`](crate::ConfigError::DuplicateAlpn) is
/// what a collision would become, at startup, on a tree where nothing looked
/// wrong.
///
/// And `alpha` must not agree with `beta`, which is the property that makes an
/// ALPN a contract identifier at all. Both are runtime assertions rather than
/// probes, because the values exist and there is nothing to be unable to
/// represent.
///
/// Made red by `scripts/break-once.sh keylink/src/macros.rs`, replacing
/// `registry_alpn`'s `derive_alpn::<KeyedContract>(KEYED_PROTOCOL)` with
/// `alpn().to_vec()`. Watched: `assertion `left != right` failed`, both sides
/// `keylink/Alpha/8d5e509da907517a`.
#[test]
fn a_services_two_alpns_differ_from_each_other_and_from_another_services() {
    assert_ne!(alpha::alpn(), alpha::registry_alpn());
    assert_ne!(alpha::registry_alpn(), beta::registry_alpn());
    assert_ne!(alpha::alpn(), beta::alpn());
    assert!(
        String::from_utf8_lossy(alpha::registry_alpn()).contains("/Alpha/keyed/"),
        "the keyed ALPN says which of the two it is without anyone hashing anything"
    );
}

// ────────────────────────────────── a key cannot be attached to a `tell`

/// **The README said this twice and it was false both times.**
///
/// *"It does not accept an idempotency key either, and could not"* — but
/// `with_idempotency` returned `Self`, the generated `tell` is an inherent
/// method on that same type, and so `client.with_idempotency(key).note(line)`
/// compiled. `RpcClient::tell` writes `None` for the key and dropped the
/// attached one on the floor. A reader who took the limit at face value,
/// concluded the compiler would stop them, and wrote it *to be safe* shipped
/// believing they had at-most-once and had nothing.
///
/// **This probe is the whole argument for the invariant that requires it.** Had
/// it been written when the limit was, it would have failed to compile-fail on
/// the day the sentence was published, and the defect would have surfaced at
/// authoring time instead of in review two waves later.
///
/// [`IdempotentClient`](crate::IdempotentClient) is what makes it true now: a
/// distinct type with no `tell`, so the sentence is the type's claim rather than
/// prose asking a future author to be careful.
trait IdempotentClientHasNoTell {
    fn tell(&self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl<Req, Resp> IdempotentClientHasNoTell for crate::IdempotentClient<Req, Resp> {}

/// The generated twin, and it is the one a caller actually meets: nobody writes
/// `RpcClient::tell` by hand, they write `client.with_idempotency(k).note(..)`.
///
/// `note` is `Alpha`'s declared `tell`. The generated method takes one argument,
/// so an inherent `note` appearing on this type shadows the trait's and fails on
/// the argument count even before the return type is compared.
trait AKeyScopedClientHasNoTellMethod {
    fn note(&self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl AKeyScopedClientHasNoTellMethod for alpha::IdempotentClient {}

#[cfg(feature = "blocking")]
impl AKeyScopedClientHasNoTellMethod for alpha::blocking::IdempotentClient {}

fn a_key_scoped_client_has_no_tell(
    raw: crate::IdempotentClient<alpha::Request, alpha::Response>,
    generated: alpha::IdempotentClient,
) {
    let _: NoSuchMethod = raw.tell();
    let _: NoSuchMethod = generated.note();
}

#[cfg(feature = "blocking")]
fn a_blocking_key_scoped_client_has_no_tell(generated: alpha::blocking::IdempotentClient) {
    let _: NoSuchMethod = generated.note();
}

/// The other bracket, and without it the two probes above are satisfied by a
/// misspelling.
///
/// C-4's lesson, applied: a probe body with a typo in the method name passes for
/// a reason unrelated to its claim. So the same `tell` is called here on the
/// plain client, where it must exist with exactly that name, that argument and
/// that return type. Absent there, present here — a pair, because either half
/// alone can be green for the wrong reason.
async fn the_plain_client_does_have_that_tell(plain: alpha::Client) {
    let _: crate::Result<()> = plain.note("a line".to_owned()).await;
}

/// The claim the whole keyed rewrite rests on: **a per-service `Registry`
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
/// The annotations are the assertion. `let mounted: alpha::Mountable` pins the
/// generated alias, so this stays a statement about `alpha`'s system rather than
/// about whatever inference happened to pick.
///
/// The break, executed by hand rather than described: swap `alpha::spawn` for
/// `beta::spawn` below. Observed —
///
/// ```text
/// error[E0308]: mismatched types
///    |     let mounted: alpha::Mountable = beta::spawn(BetaState);
///    |                  --------------   ^^^^^^^^^^^^^^^^^^^^^^ expected `Mountable<Mail<alpha::Request, alpha::Response>>`,
///    |                                                           found `Mountable<Mail<beta::Request, beta::Response>>`
/// ```
///
/// # What this does NOT prove
///
/// It proves **mount** safety, not ALPN-**pairing** safety.
/// `ServiceBuilder::new(alpn: &'static [u8], ..)` takes the ALPN as an untyped
/// slice, so nothing at the type level stops `alpha`'s dispatch from being
/// registered under `beta`'s ALPN. What prevents that is the generated pairing —
/// `registry.service(..)` is the only path that calls `alpha::registry_alpn()` —
/// and a hand-written registration could still get it wrong. Saying so is the
/// difference between a probe and a claim about a probe.
fn a_system_mounts_only_its_own_services_actors(
    system: &alpha::Registry,
    id: ActorId,
) -> Result<(), MountError> {
    let mounted: alpha::Mountable = alpha::spawn(AlphaState);
    system.mount(id, mounted)
}

/// The same property from the builder's side, so a future `Registry` that lost
/// its parameter would break here too rather than only above.
fn a_system_is_built_at_one_services_mail() -> beta::Registry {
    beta::Registry::builder(Eviction::Never)
        .capacity(beta::RegistryBuilder::DEFAULT_CAPACITY)
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
/// [`Intensity`](crate::Intensity) is the fourth. A restart budget nobody chose
/// is either a hot loop hiding a permanent fault or a give-up that discards a
/// recoverable one, and which of the two a wrong number produces is a fact about
/// the caller's actor rather than about keylink.
trait HasNoDefault {}
impl<T: Default> HasNoDefault for T {}
impl HasNoDefault for Allowlist {}
impl HasNoDefault for Eviction {}
impl HasNoDefault for Retention {}
impl HasNoDefault for Intensity {}
/// The fifth and sixth are the fan-out's, and they are the same family read one
/// layer up: [`Overflow`] is the choice between *the publisher waits* and *the
/// slow subscriber is ended*, and both are decisions about the caller's data
/// rather than ours.
///
/// A `#[derive(Default)]` with `#[default]` on one arm is exactly what somebody
/// would add — it is one attribute and it looks harmless — and it would give a
/// fan-out a backpressure policy nobody chose. That is the failure the whole
/// `compose` module is arranged against, so it is checked here rather than
/// promised in its module doc.
///
/// [`Topic`] carries the policy, so it inherits the property: a
/// `Topic::default()` would be a topic whose overflow behaviour is whatever the
/// derive picked.
impl HasNoDefault for Overflow {}

/// The exact public shape of [`Overflow`]: three exhaustive policies, each carrying
/// one finite, non-zero depth, with the bounded-wait policy also carrying patience.
///
/// No arm uses `..`, so adding a field or a variant makes this match fail to
/// compile at the site that owns the exhaustive-policy argument.
///
/// Made red by `scripts/break-once.sh`, replacing
/// `    BlockPublisher(NonZeroUsize),` with
/// `    BlockPublisher(NonZeroUsize, bool),`. The all-feature library test build
/// emitted `E0023` at `every_overflow_policy_carries_one_depth`: the pattern had
/// one field while the changed variant had two.
///
/// The new struct arm was broken separately by adding `ignored: bool` to this
/// exact search literal:
/// `BlockThenDrop { /// The finite, non-zero channel depth for each subscriber. depth: NonZeroUsize, /// How long one full subscriber may hold this publish. patience: Patience, },`.
/// The library build emitted `E0027` here because the exact pattern did not
/// mention it; the two test-only constructors also emitted `E0063`.
fn every_overflow_policy_carries_one_depth(overflow: Overflow) -> NonZeroUsize {
    match overflow {
        Overflow::BlockPublisher(depth) => depth,
        Overflow::BlockThenDrop { depth, patience } => {
            let _: crate::compose::Patience = patience;
            depth
        }
        Overflow::DropSubscriber(depth) => depth,
    }
}

impl<T> HasNoDefault for Topic<T> {}
impl HasNoDefault for BroadcastDepth {}
impl HasNoDefault for Lossy {}
impl<T> HasNoDefault for LossyTopic<T> {}

fn every_lossy_policy_carries_one_depth(policy: Lossy) -> NonZeroUsize {
    match policy {
        Lossy::DropNewest(depth) => depth,
        Lossy::DropOldest(depth) => depth.get(),
    }
}

fn lossy_subscription_is_a_send_static_stream<T>(subscription: LossySubscription<T>)
where
    T: Clone + Send + 'static,
{
    fn assert_stream<T>(_: impl crate::Stream<Item = Lagged<T>> + Send + 'static) {}
    use crate::compose::Lagged;
    assert_stream(subscription);
}
/// The adapter inherits the property from the value it holds. A `Default` here
/// would mean "a policy you did not name", and the only policy nobody named is
/// the one that admits everyone.
impl HasNoDefault for DynamicAdmission {}
/// The eighth and ninth are the ledger's two service policies, and until this
/// line they were the only types in the crate that **claimed** the property in
/// prose and had nothing checking it. [`Records`]'s rustdoc says *"There is
/// deliberately no `Default`"* and [`Keyable`]'s says the same; a
/// `#[derive(Default)]` on either is one attribute, it compiles, and the
/// paragraph goes false with nothing red.
///
/// The values are why. A defaulted [`Records`] would keep a caller's payloads in
/// RAM for ten minutes because a field was left out, and a defaulted [`Keyable`]
/// would pick which methods accept a key from a peer.
///
/// [`Limits`](crate::Limits) is deliberately **not** on this list. It is the one
/// ledger type that does have a `Default`, because nine of its ten bounds refuse
/// **visibly** when they are set wrong, and its rustdoc argues the tenth.
///
/// Said as *visibly* rather than *with the exceeded number in the refusal*, which
/// is what this line claimed while `Limits` held nine bounds. Only the four
/// running and waiting bounds carry the figure; the pool bounds and the ceilings
/// answer `Forgotten` or `Unrecordable`, which name the outcome and not the
/// number. `Limits`'s own rustdoc has always drawn that line, so this summary of
/// it was overstating one clause.
///
/// **This module is `#[cfg(test)]` (`lib.rs:230`), so every probe in it — this one
/// included — reddens under `cargo test` or `cargo check --all-targets` and
/// **never** under a plain `cargo check`.** Worth stating, because a coherence
/// conflict looks like something the compiler enforces everywhere and this one is
/// enforced in one command.
///
/// Reddened by adding `#[derive(Default)]` to `Records` with `#[default]` on
/// `Memory` — both, because the derive alone is `error[E0665]` and stops short of
/// this probe. Watched, via `cargo check -p keylink --all-targets`:
/// `error[E0119]: conflicting implementations of trait `HasNoDefault` for type `idempotency::Records``
/// on the `impl HasNoDefault for Records {}` line below, naming
/// `impl<T: Default> HasNoDefault for T {}` as the `first implementation here`.
///
/// **No line number, deliberately.** The first version of this record cited `:426`,
/// which was the line when the record was two paragraphs shorter — the record's own
/// growth pushed the code down to `:439`, so a reader following the citation landed
/// inside this comment. Same discipline as an anchor literal: a coordinate measured
/// before the prose that displaces it is stale on arrival, and the item's name does
/// not move.
impl HasNoDefault for Records {}
impl HasNoDefault for Keyable {}

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

/// The claim, RFC-06 §2.8: [`Observer::after_turn`] takes `&A`
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
/// Widen that bound to `FnOnce(&mut A) -> T` in `turns.rs` and the argument is a
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
/// about a turn. Bound it to `M: Turn<A>` in `turns.rs` and this stops compiling
/// — `Ledger` is hand-written, is not a `service!`, and has no mailbox enum to
/// name.
async fn a_read_cannot_mutate_either<M>(actor: &LocalActor<Ledger, M>) {
    let _: Result<ThroughShared, Unanswered> = actor.read(|state| state.post()).await;
}

// ─────────────────────── one reply channel, one answer: Wave 2c's `#[defer]`

/// **`Replied` makes "forgot to answer" a compile error, and this is the half of
/// that claim a type system can hold.**
///
/// The obligation to answer is the generated signature — a `#[defer]` method
/// returns a `Replied<T>` and there is no way to build one but by consuming the
/// `Reply` — and the positive doctest on [`Replied`](crate::Replied) is what pins
/// it, because that fence goes red if the signature stops demanding a token.
///
/// What is left is the *at most once* half, and it is exactly `Clone`. Any of the
/// three being `Clone` would make two tokens or two answers reachable from one
/// channel:
///
/// - a cloned [`Reply`](crate::Reply) answers a caller twice, or answers it and
///   parks it, and one of the two answers is silently dropped by the `oneshot`;
/// - a cloned [`Deferred`](crate::Deferred) is two parked answers to one call —
///   and unlike the `Reply` case the second one can arrive turns later, from a
///   different task, with nothing on the stack to blame;
/// - a cloned [`Replied`](crate::Replied) is a forged token: a handler that parked
///   nothing could return one it kept from an earlier turn.
///
/// The break, executed by hand: add `#[derive(Clone)]` to any of the three in
/// `turns.rs`. Its concrete impl then overlaps the blanket one and it is `E0119:
/// conflicting implementations`.
///
/// Coherence and not shadowing, for the reason this module's header gives, and
/// in-crate rather than in `tests/` for the other reason it gives.
trait AtMostOneAnswer {}
impl<T: Clone> AtMostOneAnswer for T {}
impl<T> AtMostOneAnswer for crate::Reply<T> {}
impl<T> AtMostOneAnswer for crate::Deferred<T> {}
impl<T> AtMostOneAnswer for crate::Replied<T> {}

/// **RFC-07 §1.9's open question, answered here: parking a reply past the end of
/// its turn does not make the loop's future non-`Send`.**
///
/// Wave 0 probed `oneshot::Sender<Answered<T>>` held *inside* a turn, which is what
/// its own criterion stated, and said nothing about a channel the state keeps. §1.9
/// flagged it and told Wave 2c to probe its own. This is that probe, and it is
/// positive code: `spawn` requires `A: Send + 'static` and `M: Turn<A>`, and the
/// loop holds `&mut A` across the turn's await and `&A` across the observer chain's.
/// If a parked [`Deferred`](crate::Deferred) demanded `Sync`, this would not compile.
///
/// **The verdict is green, and it is green for a reason worth stating rather than a
/// coincidence.** A `Deferred<T>` is `Send` when `T: Send` and is deliberately
/// **not** `Sync`: its wrapped arm is a `Box<dyn FnOnce(..) + Send>`. A state that
/// parks one is therefore `Send + !Sync` — which is not a problem but the *design*,
/// the exact family `observe.rs` names by hand and the reason `A: Sync` is kept out.
/// The loop never lends the state across an await by shared reference in a way that
/// would need it: an observer's future is the observer's own choice, which is why
/// `after_turn` is a plain `fn`.
///
/// **No stand-in types.** The state below holds a real `Deferred<u64>` beside a real
/// `RefCell<u32>`, so the `!Sync`-ness is doubled and no single member's removal can
/// make the probe vacuous — the house rule a unit-struct `Panic` earned through two
/// revisions of RFC-06.
///
/// The break: bound `Parking: Sync` below, or make `spawn` require `A: Sync`.
struct Parking {
    /// A reply parked past its turn, which is the shape under test.
    parked: Option<crate::Deferred<u64>>,
    /// A second, independent reason to be `!Sync`, so the probe cannot go vacuous
    /// by the first one moving.
    counter: std::cell::RefCell<u32>,
}

/// A mailbox that does nothing, because what is under test is what the STATE holds
/// across the loop's awaits rather than what a message does.
enum Parked {}

impl crate::Turn<Parking> for Parked {
    async fn run(self, _state: &mut Parking, _hooks: &mut crate::Hooks<Parking>) -> Option<Ended> {
        match self {}
    }
}

/// The assertion: this spawns. `spawn` is `A: Send + 'static`, so a `Sync`
/// requirement introduced by the parked channel would land here.
fn a_state_that_parks_a_reply_is_still_an_actor() -> LocalActor<Parking, Parked> {
    LocalActor::spawn(Parking {
        parked: None,
        counter: std::cell::RefCell::new(0),
    })
}

/// The one exhaustive match on [`Ended`] in the workspace, which is what makes
/// "exhaustive by design" a thing the compiler checks rather than a paragraph.
///
/// [`Ended`] is deliberately **not** `#[non_exhaustive]`, and Wave 3 found the usual
/// argument for that hollow: adding `Ended::Feed` produced **zero** `E0004`s, because
/// every match in `keylink/tests` is one arm plus `other => panic!(..)` — an
/// assertion about which ending happened, not a dispatch over all of them — and
/// keylink itself only ever *builds* one. So there was nothing in the tree that a
/// sixth variant would have to be reconciled with, which means the decision had no
/// enforcement behind it either.
///
/// This is the enforcement, in the one form that costs nothing: a match with no
/// wildcard. Wave 9's `Ended::GaveUp` is `E0004` **here**, in keylink, at a site
/// whose doc comment is this one — so whoever adds it reads the reasoning on
/// [`Ended`] before deciding, instead of adding a variant into silence.
///
/// It is **not** a claim that a wildcard is impossible; nothing can be. It is the
/// tripwire that makes the next ending a decision rather than an omission.
fn every_ending_is_named(ended: &Ended) -> &'static str {
    match ended {
        Ended::Stopped => "stopped",
        Ended::Vanished => "vanished",
        Ended::Panicked { .. } => "panicked",
        Ended::Observer { .. } => "observer",
        Ended::Feed { .. } => "feed",
        Ended::GaveUp { .. } => "gave up",
    }
}

/// The same tripwire for [`TurnOutcome`], and this one guards a **durability**
/// decision rather than a report.
///
/// [`Persist::after_turn`](crate::Persist) refuses a panicked turn and stores
/// every other one. Written as `if let TurnOutcome::Panicked { .. }` — which is
/// what it was — a third outcome would have taken the `else` branch and been
/// **written to the disk**, silently, under a rule nobody wrote. That adapter
/// now matches exhaustively, so the day `TurnOutcome::Cancelled` exists it is
/// `E0004` at the line that decides whether the bytes land.
///
/// This function exists beside it because that adapter is behind
/// `#[cfg(feature = "persist")]`: a `--no-default-features` build has no such
/// match, and a commitment that a feature flag can switch off is not one. Here
/// it is unconditional.
fn every_outcome_is_named(outcome: TurnOutcome<'_>) -> &'static str {
    match outcome {
        TurnOutcome::Completed => "completed",
        TurnOutcome::Panicked { .. } => "panicked",
    }
}

/// And the two facts the spawn above rests on, stated so a reader does not have to
/// infer them from a `spawn` compiling.
///
/// `Send` is asserted; `Sync` is deliberately **not**, and must not be — a
/// `Deferred` holding a `Box<dyn FnOnce + Send>` is `!Sync`, and the whole design
/// exists to make that fine.
fn a_deferred_reply_is_send() {
    fn requires_send<T: Send>() {}
    requires_send::<crate::Deferred<u64>>();
    requires_send::<crate::Replied<u64>>();
    requires_send::<crate::Reply<u64>>();
    requires_send::<Parking>();
}

// ───────────────────────────────────── no mailbox can be asked to be unbounded

/// The claim: **an unbounded mailbox is not sayable.** Not absent from the
/// documentation — absent from the language of the API.
///
/// It is the one thing this project rejects outright rather than defers, because
/// it inverts the invariant the rest of the design is built on: an unbounded queue
/// turns backpressure into memory growth, so a latency problem becomes an OOM at a
/// moment nobody chose, in a process that had been reporting healthy.
///
/// Two probes, because the two ways it would arrive cannot see each other.
///
/// **A second constructor**, caught by shadowing on the name. While
/// `RegistryBuilder` has no inherent method of these names each call resolves to the
/// trait default and hands back the marker; add `pub fn unbounded(self) -> Self`
/// and the inherent method outranks the trait, the marker is not what comes back,
/// and this module stops compiling. [`Backpressure`](crate::Backpressure) is
/// shadowed too, because a policy type is where a caller would look for one.
///
/// **A wider depth type**, caught by pinning the signature. The coerced function
/// item below says `NonZeroUsize` and nothing else, so an `Option<usize>` whose
/// `None` meant unbounded — or an `enum Depth { Bounded(..), Unbounded }` — is
/// `E0308` at that line. This is the shape the refusal would actually arrive in:
/// nobody adds a method called `unbounded`, they widen the argument of the one
/// that already exists.
///
/// # The local path took the widening on purpose, and that moved the pin
///
/// [`Backpressure`](crate::Backpressure) **is** the `enum Depth { .. }` the
/// paragraph above names as the hazard, minus the arm that made it one. The local
/// doors now take it, so their pin is no longer *this argument is a
/// `NonZeroUsize`* — it is
/// [`Backpressure::depth`](crate::Backpressure::depth) returning one, totally, for
/// every arm there is. An `Unbounded` variant cannot be added without widening
/// that return to an `Option`, which is `E0308` at the coercion below. So the
/// refusal survived the knob that looked most like it.
///
/// **And a shed is not a leak**, which is the other half of why the widening was
/// affordable: [`Backpressure::Shed`](crate::Backpressure::Shed) bounds the
/// mailbox exactly as `Park` does and differs only in what it tells a sender that
/// arrives past the last slot. The claim this section guards is about *memory
/// growth nobody chose*, and neither arm can produce it.
///
/// # What neither probe reaches, and it is worth naming
///
/// `tokio::sync::mpsc::unbounded_channel` called somewhere inside the crate. That
/// is a claim about a foreign function, and this module's own header explains why
/// no probe can make one: rustc will not reason negatively about another crate's
/// API. The check is `rg 'unbounded_channel' keylink/src keylink-persist/src`,
/// whose only two matches are this sentence and the one above it. The tests use
/// unbounded channels for their own logs, which are not mailboxes and are not on
/// this path.
trait NoUnboundedMailbox {
    fn unbounded(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn unbounded_mailbox(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn without_backpressure(&self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl NoUnboundedMailbox for beta::RegistryBuilder {}
impl NoUnboundedMailbox for crate::Backpressure {}

fn no_mailbox_can_be_asked_to_be_unbounded(
    builder: beta::RegistryBuilder,
) -> beta::RegistryBuilder {
    let _: NoSuchMethod = builder.unbounded();
    let _: NoSuchMethod = builder.unbounded_mailbox();
    let _: NoSuchMethod = builder.without_backpressure();

    let depth: fn(beta::RegistryBuilder, NonZeroUsize) -> beta::RegistryBuilder =
        beta::RegistryBuilder::mailbox_depth;
    depth(builder, NonZeroUsize::MIN)
}

/// The **local** twin, and it is a second probe rather than a second line above
/// because it is a second knob.
///
/// The keyed depth had a probe from the day it existed and the local depth was a
/// private constant with nothing to pin. Once the local mailbox gained doors that
/// take a policy, the same widening is sayable on this path — and the claim above
/// is about *a mailbox*, not about the registry's. A probe that covered one door
/// while the other was free to take an `Option<usize>` would be the shape this
/// module exists to refuse: a guarantee whose wording outruns its guard.
///
/// Both local doors are pinned, because the mailbox is created in exactly two
/// places and a knob that reached only one of them would leave the supervised
/// path — which is the production path — unpinned.
///
/// # Three pins, because the local doors now take a policy rather than a number
///
/// **The doors take `Backpressure` and nothing wider.** Widen either parameter to
/// an `Option<Backpressure>`, or back to a bare `usize`, and the coercions below
/// are `E0308`.
///
/// **`Backpressure::depth` is total and returns a `NonZeroUsize`.** This is the
/// pin that carries the unbounded claim now: an `Unbounded` variant has no depth,
/// so adding one forces that return to widen — `E0308` here — or forces the arm to
/// invent a depth, which is the same lie with more steps.
///
/// **The default parks.** `DEFAULT_LOCAL_BACKPRESSURE` is asserted in a `const`
/// block, so a default that shed would fail to compile rather than quietly turn
/// every existing caller's full mailbox from a wait into a dropped message.
/// The two arms are not equally safe to default to, and only one of them is
/// what every caller before this type existed was already getting.
///
/// The break is the same one line-for-line: change either `Backpressure` below to
/// `usize`, or widen either door's parameter, and this file stops compiling with
/// `E0308`.
///
/// Made red by `scripts/break-once.sh` on this file, replacing
/// `    let unsupervised: fn(Parking, Hooks, (), Backpressure) -> Probe = LocalActor::spawn_deep;`
/// with the same line reading `usize` in place of `Backpressure`. Watched, and the
/// diagnostic is quoted rather than the exit code because for a probe the compile
/// error IS the guard and `── exit=101` alone cannot tell one from a typo:
/// `error[E0308]: mismatched types ... expected fn pointer, found fn item`, with
/// `expected fn pointer `fn(Parking, .., usize) -> ..`` against
/// `found fn item `fn(_, .., Backpressure) -> ..``.
fn no_local_mailbox_can_be_asked_to_be_unbounded() {
    /// The observers half of a spawn's triple. An alias so the two pins below
    /// read as pins rather than as type puzzles — `clippy::type_complexity` is
    /// `deny` here, and the only part of these signatures under test is the
    /// **last** parameter.
    type Hooks = Vec<Box<dyn crate::Observer<Parking>>>;
    /// What a supervised factory hands back, ready-made.
    type Rebuilt = std::future::Ready<Result<(Parking, Hooks, ()), crate::Stop>>;
    /// The actor both doors produce.
    type Probe = LocalActor<Parking, Parked>;

    let unsupervised: fn(Parking, Hooks, (), Backpressure) -> Probe = LocalActor::spawn_deep;
    let _ = unsupervised;

    let supervised: fn(fn() -> Rebuilt, crate::Intensity, Backpressure) -> Probe =
        LocalActor::spawn_supervised_deep;
    let _ = supervised;

    let depth: fn(Backpressure) -> NonZeroUsize = Backpressure::depth;
    let _ = depth;

    let _: NoSuchMethod = Backpressure::Park(NonZeroUsize::MIN).unbounded();
    let _: NoSuchMethod = Backpressure::Park(NonZeroUsize::MIN).unbounded_mailbox();
    let _: NoSuchMethod = Backpressure::Park(NonZeroUsize::MIN).without_backpressure();

    const _: () = assert!(matches!(
        crate::turns::DEFAULT_LOCAL_BACKPRESSURE,
        Backpressure::Park(_)
    ));
}

// ───────────────────────────────── no ledger bound can be asked to be zero

/// The claim: **eight of [`Limits`](crate::Limits)'s ten setters cannot be handed
/// a zero**, so every "what zero would do" sentence in that type's rustdoc
/// describes a value a caller cannot type.
///
/// The natural guard is a `compile_fail` doctest, and it is the wrong tool twice
/// over. Rustdoc does not check the error code, so a fence written for `E0308`
/// stays green when the code fails to compile for a typo instead — a
/// `compile_fail` that never compiled for the reason it names is a guard that
/// cannot be reddened, which is the trap this whole module exists to avoid.
///
/// A coercion has no such slack. Widening one of these annotations to `usize` is
/// `E0308` at this line and nothing suppresses it. That is also the whole break for
/// this probe: change one `NonZeroUsize` below to `usize`, or change one setter's
/// parameter, and the file stops compiling.
///
/// `ttl` and `attach` are absent on purpose and are the stated exceptions: the
/// language offers no `NonZero` for a [`Duration`](std::time::Duration), so zero is
/// representable on both and each rustdoc says what it does rather than claiming it
/// cannot happen. `a_ttl_of_zero_forgets_every_record_at_the_next_claim` and
/// `an_attach_ceiling_of_zero_refuses_the_attacher_at_once` assert what they do
/// instead, because those are the two fields this probe cannot speak for.
///
/// **The exceptions are a list because the type grew one**, and the pairing is not a
/// coincidence: `attach` joined as a `Duration` for `ttl`'s stated reason, so it
/// arrived outside this probe's reach by the same language fact. A third duration
/// would too, which is why the sentence names the property and not the field count.
///
/// **`attach` takes an [`Attaching`](crate::Attaching) now and is still an
/// exception**, because zero lives inside [`Attaching::UpTo`](crate::Attaching::UpTo)
/// rather than being removed from the range. What the enum removed is a second
/// *meaning* on that zero, which is a different property from the one this probe
/// asserts — so a reader meeting the new signature does not gain a bound here.
///
/// Reddened by widening one setter — `Limits::pool_records`'s parameter changed from
/// `NonZeroUsize` to `usize`, which is the real defect rather than an edit to the
/// annotation below. Watched, via `cargo check -p keylink --all-targets`:
/// `error[E0308]: mismatched types` on the `crate::Limits::pool_records;` line in
/// the body below — no line number, for the reason the `HasNoDefault` record above
/// gives: this comment's own growth moves that line, and a citation measured before
/// the prose that displaces it is stale on arrival —
/// `expected fn pointer, found fn item`, with
/// `note: expected fn pointer `fn(idempotency::Limits, NonZero<usize>) -> idempotency::Limits``
/// against
/// `found fn item `fn(idempotency::Limits, usize) -> idempotency::Limits {idempotency::Limits::pool_records}``.
/// No error code is suppressible here, which is the whole reason this is a coercion
/// and not a `compile_fail` fence.
fn no_ledger_bound_can_be_asked_to_be_zero(limits: crate::Limits) -> crate::Limits {
    let pool_records: fn(crate::Limits, NonZeroUsize) -> crate::Limits =
        crate::Limits::pool_records;
    let frame_bytes: fn(crate::Limits, NonZeroUsize) -> crate::Limits = crate::Limits::frame_bytes;
    let pool_bytes: fn(crate::Limits, NonZeroUsize) -> crate::Limits = crate::Limits::pool_bytes;
    let tombstones: fn(crate::Limits, NonZeroUsize) -> crate::Limits = crate::Limits::tombstones;
    let running: fn(crate::Limits, NonZeroUsize) -> crate::Limits = crate::Limits::running;
    let running_per_peer: fn(crate::Limits, NonZeroUsize) -> crate::Limits =
        crate::Limits::running_per_peer;
    let waiting: fn(crate::Limits, NonZeroUsize) -> crate::Limits = crate::Limits::waiting;
    let waiting_per_peer: fn(crate::Limits, NonZeroUsize) -> crate::Limits =
        crate::Limits::waiting_per_peer;

    let one = NonZeroUsize::MIN;
    let limits = pool_records(limits, one);
    let limits = frame_bytes(limits, one);
    let limits = pool_bytes(limits, one);
    let limits = tombstones(limits, one);
    let limits = running(limits, one);
    let limits = running_per_peer(limits, one);
    let limits = waiting(limits, one);
    waiting_per_peer(limits, one)
}

// ────────────────────────────── a status reports only what a registry can read

/// The claim: [`ActorStatus`] has **exactly these four fields**, so a fifth is a
/// decision somebody makes at this line rather than a field that appears.
///
/// A destructuring pattern with no `..`, which is `E0027` the moment a field is
/// added — in keylink, at the doc comment that records which fields were refused
/// and why: an `incarnation` that nothing in the crate counts, and a `queued` that
/// would claim the parked senders [`ActorStatus::queued`] documents itself as
/// unable to see.
///
/// `#[non_exhaustive]` is what stops a *caller* writing this pattern, and it is
/// also why the probe has to live inside the crate: outside, the same pattern is
/// `E0638` on a clean tree, which is the wrong red for the right reason.
fn an_actor_status_claims_only_what_it_measures(status: ActorStatus) -> (bool, u64) {
    let ActorStatus {
        id,
        idle_for,
        busy,
        queued,
    } = status;
    let _ = (id, idle_for);
    (busy, queued)
}

/// The claim: what a console is told about an actor **is** an [`ActorStatus`],
/// not a struct beside it — so the four-field probe above governs the wire as
/// well as the local call.
///
/// A tuple-variant constructor coerced to a function pointer with the payload
/// type written out, which is the shape the `mailbox_depth` probe above uses and
/// for the same reason: it pins a *type*, and a type is what a parallel struct
/// would change. `Response::Actors(SomeConsoleActorRow)` is `E0308` here.
///
/// This is the whole of "`Inspect` exposes no field that `ActorStatus` cannot
/// measure": there is no second field list to keep in step, because there is no
/// second struct. A field claiming the parked-sender count would have to be
/// added to `ActorStatus`, where the destructure above refuses it.
///
/// `crate::console::inspect` and not `crate::inspect`: the root re-export is a
/// curated five names now, and `Response` is deliberately not one of them. An
/// in-crate probe reaches the generated module where it is defined, which is
/// also the honest spelling — this claim is about keylink's own wire and not
/// about anything a caller can name.
fn the_inspect_service_reports_the_status_type_itself() -> crate::console::inspect::Response {
    let actors: fn(Vec<ActorStatus>) -> crate::console::inspect::Response =
        crate::console::inspect::Response::Actors;
    actors(Vec::new())
}
