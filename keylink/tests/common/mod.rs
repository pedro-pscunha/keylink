//! What every integration test needs before it can say anything: a node.
//!
//! `keylink/src/testing.rs` cannot be reached from here. It is `#[cfg(test)]
//! pub(crate)`, so it exists only while the library's own unit tests compile,
//! and an integration test links against the published crate. The fixture has
//! to live on this side of that boundary.
//!
//! ## `common/mod.rs`, never `common.rs`
//!
//! cargo turns every *file* directly under `tests/` into its own test binary.
//! `tests/common.rs` would therefore become a target that declares no tests and
//! reports `0 passed` on every run. A directory is not a target, so the module
//! form is the one that exists only for the binaries that say `mod common;`.
//!
//! ## The dead-code hazard, and its one remedy
//!
//! `mod common;` compiles this file *into each binary that declares it*, and
//! the dead-code lint runs per binary. A helper that six of seven test binaries
//! call is genuinely unreachable in the seventh, and the gate runs
//! `-D warnings`, so that warning fails the build. `pub` buys nothing: a test
//! binary is a binary, so nothing is reachable from outside it and the lint
//! ignores visibility in a way it would not in a library.
//! `#[expect(dead_code)]` on the item is worse than useless — it raises an
//! unfulfilled-expectation warning in every binary that *does* use the helper.
//!
//! The only remedy is `#![allow(dead_code)]` at the top of this file. It is
//! deliberately not here yet: everything below is used by every consumer, so
//! adding it now would suppress a warning nothing is emitting, and would go on
//! hiding a helper that later goes dead everywhere. Add it in the same commit
//! as the first helper a consumer legitimately does not want.

// Stated here rather than inherited from whichever test file declares the
// module, so a new consumer cannot fail to compile this one.
#![allow(clippy::expect_used)]

use iroh::endpoint::presets;
use iroh::Endpoint;
use keylink::Node;

/// A node on the loopback interface, with no relay and no discovery.
///
/// `presets::Minimal` is what makes the integration suite hermetic: two of
/// these dial each other over real QUIC without touching a network.
pub async fn local_node() -> Node {
    Node::from_endpoint(
        Endpoint::bind(presets::Minimal)
            .await
            .expect("a local endpoint binds"),
    )
}
