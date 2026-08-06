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

use crate::{ActorId, Allowlist, Eviction, IdempotencyKey, Sealed};

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
/// A hand-written impl trips this, and so does a `#[derive(Default)]`.
trait HasNoDefault {}
impl<T: Default> HasNoDefault for T {}
impl HasNoDefault for Allowlist {}
impl HasNoDefault for Eviction {}

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
