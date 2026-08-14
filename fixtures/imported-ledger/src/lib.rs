//! A service DEFINED here and served from somewhere else.
//!
//! Every other `service!` in this workspace sits in the crate that consumes
//! it, so nothing measured whether the generated code works across a crate
//! boundary. Three things could break there and nowhere else: the macro could
//! emit an item that is private outside its own crate, it could name a type by
//! a path only valid where it was written, and the external seam could be
//! implemented for a marker a consumer cannot reach.
//!
//! This crate exists only to be imported. It has no tests of its own — the
//! measurement is that `keylink-http` can build a gateway over
//! [`ledger::External`] and [`ledger::RegistryClient`] from here.
//!
//! # The declaration callback does not cross a crate boundary
//!
//! `service!` emits one hidden callback per generated module, `pub(crate)`, so
//! it is reachable from the crate that wrote the declaration and from nowhere
//! else. A doctest is a SEPARATE crate that depends on this one, which is
//! exactly the downstream case:
//!
//! ```compile_fail
//! macro_rules! reader { ($($rest:tt)*) => { fn read() {} }; }
//! imported_ledger::ledger::__keylink_declarations!(reader);
//! # fn main() {}
//! ```
//!
//! The callback is a REAL macro, defined right there. Without it the block
//! would fail because `anything` does not exist — which it did in its first
//! version, and a `compile_fail` that fails for a missing name measures
//! nothing.
//!
//! What holds the boundary is NOT the `pub(crate)`, and that is measured:
//! relaxing it to `pub` in `keylink/src/macros.rs` leaves this block still
//! failing and the doctest still green. A `macro_rules!` without
//! `#[macro_export]` is crate-local whatever a `use` says, so the re-export
//! makes the macro nameable as `module::name!` WITHIN its own crate and cannot
//! make it nameable outside one. The `pub(crate)` is the narrower of two
//! boundaries and the redundant one.
//!
//! The control, so the refusal above is about VISIBILITY and not about a
//! mistyped path. Same crate, same module, a public item:
//!
//! ```
//! fn exists<T>() {}
//! exists::<imported_ledger::ledger::External>();
//! ```
//!
//! That refusal is what makes the orphan rule hold for documentation. A
//! documentation trait lives in `keylink-openapi`, `External` lives here, and
//! the only macro that can implement one for the other is invocable only from
//! this crate — so a third crate cannot write the impl even by naming both.
//!
//! The positive half is in `keylink/tests/declaration_seam.rs`, whose own
//! `service!` blocks it reads without difficulty.

// A keyed service whose declarations cover every kind the actor seam serves, so
// a consumer exercises all three of them across the boundary.
keylink::service! {
    service Ledger;

    rpc Count(value: u64) -> u64;
    rpc Whoami() -> String;
    recv Read(chunks: u64) -> String;
    blob Take() -> u64;
}
