//! Compile-time proof for the claims in this crate that say a state cannot be
//! represented.
//!
//! [`Versioned`](crate::versioned::Versioned) says, in prose: *"There is no
//! accessor, no `Deref` and no `into_inner`, so a holder of a `Versioned` has
//! no route to the bytes underneath it."* That is a comment asking a future
//! author to be careful. This module asks the compiler instead. Nothing here
//! runs; the proof is that it compiles.
//!
//! # Two probes, because one cannot see everything
//!
//! **An inherent method outranks a trait method.** So a trait whose default
//! method returns a marker type is a probe: while the type has no inherent
//! method of that name, the call resolves to the trait and the marker checks
//! out. Add `pub fn claim(&self) -> &C` and the call resolves to the inherent
//! method, whose return type is not the marker, and this module stops
//! compiling.
//!
//! **A trait impl is invisible to name resolution**, so it needs coherence
//! instead. A blanket `impl<T: Deref> Marker for T` beside a concrete
//! `impl Marker for Versioned<ProbeClaim>` overlaps exactly when
//! `Versioned<ProbeClaim>: Deref`, and an overlap is `E0119`. The concrete impl
//! therefore compiles only while the trait is absent.
//!
//! A third shape appears once below: **two applicable trait methods of one name
//! are an ambiguity**, `E0034`. That is how a probe watches a *foreign-to-it*
//! trait — [`Claim`](crate::Claim) — for a method it must never grow.
//!
//! | The claim | The probe |
//! |---|---|
//! | no inherent method of this name | shadowing, on the name |
//! | no impl of this trait | coherence |
//! | this trait has no method of this name | ambiguity between two traits |
//! | this trait does not require `Sync` | an impl for a `!Sync` type |
//!
//! # Why this module is in `keylink-persist` and not in `keylink`
//!
//! The coherence probe only works on a LOCAL type. Written anywhere else, every
//! one of these fails on a clean tree, because rustc will not reason negatively
//! about a foreign type:
//!
//! ```text
//! note: upstream crates may add a new impl of trait `std::ops::Deref`
//!       for type `keylink_persist::versioned::Versioned<..>` in future versions
//! ```
//!
//! Which is the correct answer to the question a foreign crate asks. The
//! question worth asking is whether *this crate* adds the impl, and only this
//! crate can ask it.
//!
//! # What this module cannot prove, and why
//!
//! **`#[non_exhaustive]` is inert inside its own crate.** The variants
//! [`RestoreError::FromTheFuture`](crate::RestoreError::FromTheFuture) and
//! [`RestoreError::NoMigration`](crate::RestoreError::NoMigration) carry it so
//! that a `Durable` implementation cannot construct them and lie about
//! provenance — but here, in the defining crate, they construct fine, and
//! `decode` constructs both. rustc enforces the attribute across the crate
//! boundary and no in-crate probe can see it. The compiler is the gate; this
//! module is not.
//!
//! **A trait method having no default body is not probeable.** That
//! [`Durable::migrate`](crate::Durable::migrate) is required rather than
//! defaulted is exactly what makes a forgotten hop a compile error, and
//! asserting it would need a compile-*fail* harness. `trybuild` is the usual
//! tool and it matches full rustc stderr, so a compiler release that rewords a
//! diagnostic turns the gate red on a tree nobody touched. The probes here
//! carry no error text at all.
//!
//! **`Claim::store` taking `&mut self` is not probed.** The method returns
//! `impl Future` in the trait, so its signature cannot be named as a function
//! pointer to assert against, and the receiver-step trick needs two methods of
//! one name with one arity. `keylink/src/observe.rs` depends on that receiver;
//! this file does not check it.
//!
//! **That `Versioned<C>` is no more `Sync` than `C` is not probed either**, and
//! the reason is the mirror of the one that puts this module in this crate. The
//! coherence probe would be `impl<T: Sync> Marker for T` beside
//! `impl Marker for Versioned<ProbeClaim>`, and rustc refuses it:
//!
//! ```text
//! note: upstream crates may add a new impl of trait `std::marker::Sync`
//!       for type `std::cell::RefCell<..>` in future versions
//! ```
//!
//! `Versioned<ProbeClaim>` is local, but *why* it is not `Sync` bottoms out in
//! a foreign type, and every route to a `!Sync` field does. So the auto trait
//! is out of reach here in a way that `Deref` is not. What the positive
//! assertion below does cover is the direction that would actually break an
//! actor: `Send + 'static`, for every claim.

#![cfg(test)]
#![allow(dead_code, reason = "the proof is that this module compiles")]

use std::cell::RefCell;
use std::future::{self, Future};
use std::ops::{Deref, DerefMut};

use crate::versioned::Versioned;
use crate::{Claim, Result};

/// Returned by every shadowing probe. Distinct from anything a real accessor
/// would return, so an inherent method that shadows one is a type error rather
/// than a silent pass.
struct NoSuchMethod;

// ─────────────────────────────────────────── a claim that is Send and not Sync

/// A [`Claim`] whose only unusual property is that it is **not `Sync`**.
///
/// The `RefCell` is the probe and not a convenience: it makes this type `Send`
/// and not `Sync`, so `impl Claim for ProbeClaim` compiles only while the trait
/// asks for `Send + 'static` and no more. Widen the supertraits to
/// `Send + Sync + 'static` and this impl is `E0277` — which is the whole point,
/// because `keylink/src/observe.rs` builds its argument for an owned observer
/// on the claim *not* being `Sync`, and a supertrait added here would quietly
/// take that argument away.
///
/// Both methods are written as `-> impl Future` returning
/// [`future::ready`](std::future::ready) rather than as `async fn`. That is
/// deliberate and it is the reason the probe works: an `async fn load(&self)`
/// captures `&Self` in its future, and `&Self: Send` requires `Self: Sync`, so
/// an `async fn` impl would demand the very bound this probe says the trait
/// does not.
///
/// # On the no-stand-in rule
///
/// This does not copy `FileClaim`'s field list — `_lock: std::fs::File`,
/// `snapshot: PathBuf`, `parent: PathBuf`, `limit: u64` — and it should not.
/// That rule exists because a unit-struct stand-in once hid a `Send` fault by
/// being *more* permissive than the type it stood in for. This one is
/// deliberately **less** permissive than any real claim, which is the direction
/// that cannot hide anything: a bound the trait could add would be refused here
/// first.
///
/// # What this does not prove
///
/// It does not prove that a *real* adapter can be non-`Sync`. `FileClaim`
/// writes `load` as an `async fn` and is `Sync` anyway, and any adapter that
/// does the same needs `Self: Sync` for its own future to be `Send`. What is
/// proved is narrower and is the thing observers rely on: the **trait** does
/// not require it, so no generic code may assume it.
struct ProbeClaim {
    stored: RefCell<Option<Vec<u8>>>,
}

impl Claim for ProbeClaim {
    fn load(&self) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send {
        future::ready(Ok(self.stored.borrow().clone()))
    }

    fn store(&mut self, bytes: &[u8]) -> impl Future<Output = Result<()>> + Send {
        *self.stored.get_mut() = Some(bytes.to_vec());
        future::ready(Ok(()))
    }
}

/// A wrapper is a claim with a header, so it must keep the claim's shape.
/// `Send + 'static` for **every** `C: Claim` is what lets one live inside an
/// actor for the actor's whole life; it holds because `Claim: Send + 'static`
/// and a `Versioned` holds nothing else.
///
/// The generic parameter is what makes this an assertion rather than a
/// restatement. It is checked at definition, for all claims at once, so a
/// second field that is not `Send` — an `Rc` cache, a raw pointer — or one
/// that borrows rather than owns is `E0277` here whatever adapter is in use.
///
/// # What this does not prove
///
/// It says nothing about `Sync`, in either direction. An `Arc<Mutex<C>>` field
/// added to make `store` take `&self` would satisfy this line unchanged, and
/// would take away the argument `keylink/src/observe.rs` builds on the claim
/// being exclusive. The module header explains why the `!Sync` half is out of
/// reach of a probe here; this one covers the half that is not.
fn a_wrapper_is_send_and_lives_as_long_as_the_actor<C: Claim>() {
    fn send_and_static<T: Send + 'static>() {}
    send_and_static::<Versioned<C>>();
}

// ───────────────────────────── a wrapper has no route back to the raw claim

/// The claim: once a [`Claim`] is inside a
/// [`Versioned`](crate::versioned::Versioned) there is no way to get it out
/// again, so nothing holding one can write an unframed byte.
///
/// That sentence is the crate's whole answer to "an adapter must stay dumb, and
/// a caller must still not be able to skip the frame". The port is unchanged —
/// `Claim::store` is public and takes raw bytes, as an S3 or vault adapter
/// needs it to. What closes the hole is ownership, and ownership is only worth
/// anything while there is no accessor.
///
/// Every name below is one an author would reach for when adding "just a quick
/// escape hatch": returning the claim, borrowing it, or storing beside it.
///
/// The break: add `pub fn into_claim(self) -> C` to `Versioned`. The call in
/// `a_wrapper_gives_nothing_back` then resolves to the inherent method, whose
/// return type is `ProbeClaim` and not [`NoSuchMethod`], so this module fails
/// with `E0308`.
///
/// # What this does not prove
///
/// It proves nothing about `Versioned::store` itself. A `store` that forgot to
/// call `frame` would satisfy every probe here; the tests in
/// `keylink-persist/tests/versioned.rs` are what watch that, by reading the
/// magic back off the disk.
trait AWrapperGivesNothingBack {
    fn claim(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn claim_mut(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn into_claim(self) -> NoSuchMethod
    where
        Self: Sized,
    {
        NoSuchMethod
    }
    fn inner(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn into_inner(self) -> NoSuchMethod
    where
        Self: Sized,
    {
        NoSuchMethod
    }
    fn get_mut(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn as_mut(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn store_raw(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn store_bytes(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn store_unframed(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl<C: Claim> AWrapperGivesNothingBack for Versioned<C> {}

fn a_wrapper_gives_nothing_back(mut versioned: Versioned<ProbeClaim>) {
    let _: NoSuchMethod = versioned.claim();
    let _: NoSuchMethod = versioned.claim_mut();
    let _: NoSuchMethod = versioned.inner();
    let _: NoSuchMethod = versioned.get_mut();
    let _: NoSuchMethod = versioned.as_mut();
    let _: NoSuchMethod = versioned.store_raw();
    let _: NoSuchMethod = versioned.store_bytes();
    let _: NoSuchMethod = versioned.store_unframed();
    let _: NoSuchMethod = versioned.into_claim();
}

fn a_wrapper_is_not_unwrappable(versioned: Versioned<ProbeClaim>) {
    let _: NoSuchMethod = versioned.into_inner();
}

/// The trait half, which the shadowing probes above cannot see: a trait impl is
/// invisible to name resolution, so it needs coherence.
///
/// A `Deref<Target = C>` is the quietest way to lose this. It needs no call
/// site to look suspicious — `versioned.store(&bytes)` would keep compiling and
/// silently start resolving to `Claim::store`, writing the payload with no
/// header, which is the exact silent corruption the magic exists to catch one
/// step later.
trait AWrapperDoesNotDeref {}
impl<T: Deref> AWrapperDoesNotDeref for T {}
impl AWrapperDoesNotDeref for Versioned<ProbeClaim> {}

trait AWrapperDoesNotDerefMut {}
impl<T: DerefMut> AWrapperDoesNotDerefMut for T {}
impl AWrapperDoesNotDerefMut for Versioned<ProbeClaim> {}

/// The conversion half. `impl<C: Claim> From<Versioned<C>> for C` would be an
/// `.into()` at one call site and the claim would be loose again, with no name
/// on it for the probes above to catch.
trait AWrapperIsNotConvertibleBackToItsClaim {}
impl<T: Into<ProbeClaim>> AWrapperIsNotConvertibleBackToItsClaim for T {}
impl AWrapperIsNotConvertibleBackToItsClaim for Versioned<ProbeClaim> {}

/// And borrowing back out, which is neither a `Deref` nor a conversion.
trait AWrapperDoesNotLendItsClaim {}
impl<T: AsRef<ProbeClaim>> AWrapperDoesNotLendItsClaim for T {}
impl AWrapperDoesNotLendItsClaim for Versioned<ProbeClaim> {}

trait AWrapperDoesNotLendItsClaimMutably {}
impl<T: AsMut<ProbeClaim>> AWrapperDoesNotLendItsClaimMutably for T {}
impl AWrapperDoesNotLendItsClaimMutably for Versioned<ProbeClaim> {}

// ──────────────────────────────────── the port does not know about the frame

/// The claim: [`Claim`] has no method that understands versions, so an adapter
/// author writing one for a bucket or a vault never meets the frame.
///
/// That is the reason the frame lives in a wrapper instead of in the port. If
/// it moved into the port, every adapter would have to be reviewed for whether
/// it framed correctly, and "the adapter can quietly not do it" is the failure
/// this crate's [`Claim`] documentation already names once, about exclusivity.
///
/// The probe is neither shadowing nor coherence but **ambiguity**. `ProbeClaim`
/// implements both `Claim` and the trait below, and both are in scope here. So
/// the moment `Claim` grows a method of one of these names, the call has two
/// applicable candidates and rustc reports `E0034`, *multiple applicable items
/// in scope* — whatever the new method's arguments are.
///
/// # What this does not prove
///
/// Only these names. A `Claim` that grew `fn header(&self)` would sail past.
/// The list is the names an author reaches for, not a closed set.
trait TheClaimPortDoesNotKnowAboutFraming {
    fn store_durable(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn store_versioned(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn store_framed(&mut self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn load_durable(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn load_versioned(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn version(&self) -> NoSuchMethod {
        NoSuchMethod
    }
    fn frame(&self) -> NoSuchMethod {
        NoSuchMethod
    }
}

impl TheClaimPortDoesNotKnowAboutFraming for ProbeClaim {}

fn the_port_stayed_dumb(claim: &mut ProbeClaim) {
    let _: NoSuchMethod = claim.store_durable();
    let _: NoSuchMethod = claim.store_versioned();
    let _: NoSuchMethod = claim.store_framed();
    let _: NoSuchMethod = claim.load_durable();
    let _: NoSuchMethod = claim.load_versioned();
    let _: NoSuchMethod = claim.version();
    let _: NoSuchMethod = claim.frame();
}
