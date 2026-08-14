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

// A keyed service whose declarations cover every kind the actor seam serves, so
// a consumer exercises all three of them across the boundary.
keylink::service! {
    service Ledger;

    rpc Count(value: u64) -> u64;
    rpc Whoami() -> String;
    recv Read(chunks: u64) -> String;
    blob Take() -> u64;
}
