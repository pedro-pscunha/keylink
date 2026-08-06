//! Compile-time proof for the claims that say a state cannot be represented.
//!
//! keylink asserts, in prose, several things of the form "there is no way to do
//! X". Prose is not a guard. These tests fail to COMPILE if the claim stops
//! being true, which is the only kind of test that can check a claim about what
//! does not exist.
//!
//! # How this works, and why not `trybuild`
//!
//! An inherent method wins name resolution over a trait method. So a trait whose
//! default method returns a *marker* type is a probe: while the type has no
//! inherent method of that name, the call resolves to the trait and the marker
//! type checks out. The day somebody adds `pub fn bytes(&self) -> &[u8]`, the
//! call resolves to the inherent method instead, its return type is `&[u8]`, and
//! this file stops compiling.
//!
//! `trybuild` is the usual tool for compile-fail assertions, and it was
//! considered. It matches the full rustc stderr, so a compiler release that
//! rewords a diagnostic turns the gate red on a tree nobody touched. These
//! probes carry no error text at all, so they cost nothing to maintain and they
//! run inside `cargo test` rather than beside it.
//!
//! What this technique does NOT cover, stated so nobody assumes otherwise: a
//! `Deref` impl. A trait method on `Self` outranks a deref step, so these probes
//! would keep resolving to the trait and stay green. `Sealed` promises no
//! `Deref` and that promise is unproven here.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::{ActorId, Allowlist, Eviction, IdempotencyKey, Sealed};

/// The marker every probe returns. Distinct from anything a real accessor would
/// return, so a shadowing inherent method is a type error rather than a silent
/// pass.
struct NoSuchMethod;

// ────────────────────────────────────────────────────────── Sealed has no read

/// `Sealed` exists so that payload bytes cannot be read before their schema
/// hash is checked. `Sealed::open` is meant to be the only route from bytes to
/// a typed message, and it compares the declared hash against the registry's
/// before it decodes.
///
/// Every name here is one an author would reach for when adding "just a quick
/// accessor". The doc comment on `Sealed` says not to. This makes the compiler
/// say it.
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
    fn into_inner(self) -> NoSuchMethod
    where
        Self: Sized,
    {
        NoSuchMethod
    }
    fn payload(&self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl SealedHasNoAccessor for Sealed {}

/// Not a runtime assertion. The proof is that this function type-checks: each
/// call must resolve to the trait, which is only true while `Sealed` has no
/// inherent method of that name.
#[expect(
    dead_code,
    reason = "the proof is that this compiles, not that it runs"
)]
fn sealed_cannot_be_read(sealed: Sealed) {
    let _: NoSuchMethod = sealed.bytes();
    let _: NoSuchMethod = sealed.peek();
    let _: NoSuchMethod = sealed.len();
    let _: NoSuchMethod = sealed.as_slice();
    let _: NoSuchMethod = sealed.as_bytes();
    let _: NoSuchMethod = sealed.payload();
    let _: NoSuchMethod = sealed.into_inner();
}

// ───────────────────────────────────────────────────── fail-closed has no Default

/// `Allowlist` has no `Default` on purpose: admitting everyone must be typed
/// out at the call site. `Eviction` has none for the same shape of reason — an
/// actor system that only grows is a leak, and whether dropping state loses
/// anything is a decision about the caller's data, not ours.
///
/// If either gains a `Default`, the unqualified `default()` call below becomes
/// ambiguous between two traits and this file stops compiling. That is the
/// point: the failure is loud and it names the type.
trait NoDefault {
    fn default() -> NoSuchMethod {
        NoSuchMethod
    }
}

impl NoDefault for Allowlist {}
impl NoDefault for Eviction {}

#[expect(
    dead_code,
    reason = "the proof is that this compiles, not that it runs"
)]
fn admitting_everyone_must_be_typed_out() {
    let _: NoSuchMethod = Allowlist::default();
    let _: NoSuchMethod = Eviction::default();
}

// ──────────────────────────────────────────── bounded keys expose no inner handle

/// `ActorId` and `IdempotencyKey` are newtypes whose whole value is that an
/// oversized or empty value cannot exist. `as_str` is the intended read. A
/// `&mut` accessor, a `DerefMut`, or an `into_inner` that hands back the
/// `String` would let a caller edit the value after the constructor proved
/// something about it.
trait KeyHasNoMutableHandle {
    fn as_mut_str(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn into_inner(self) -> NoSuchMethod
    where
        Self: Sized,
    {
        NoSuchMethod
    }
    fn as_mut(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl KeyHasNoMutableHandle for ActorId {}
impl KeyHasNoMutableHandle for IdempotencyKey {}

#[expect(
    dead_code,
    reason = "the proof is that this compiles, not that it runs"
)]
fn a_validated_key_cannot_be_edited(mut actor: ActorId, mut key: IdempotencyKey) {
    let _: NoSuchMethod = actor.as_mut_str();
    let _: NoSuchMethod = actor.as_mut();
    let _: NoSuchMethod = key.as_mut_str();
    let _: NoSuchMethod = key.as_mut();
    let _: NoSuchMethod = actor.into_inner();
    let _: NoSuchMethod = key.into_inner();
}

/// One runtime test, so this file is not silently empty to the test runner.
///
/// It also pins the read that IS allowed: removing `as_str` would break callers
/// but nothing above would notice, because these probes only assert absence.
#[test]
fn the_intended_read_still_exists() {
    let id = ActorId::new("obra/4471").expect("valid");
    assert_eq!(id.as_str(), "obra/4471");

    let key = IdempotencyKey::new("k-1").expect("valid");
    assert_eq!(key.as_str(), "k-1");
}
