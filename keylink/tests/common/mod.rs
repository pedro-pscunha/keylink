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
//! The only remedy is `#![allow(dead_code)]` at the top of this file. That
//! commit has now arrived: [`rendezvous`] and [`NO_PROGRESS`] are wanted by two
//! of the fifteen binaries that declare this module and are genuinely dead in
//! the other thirteen, which is the case the paragraph above was written for.
//! The cost is real and worth naming — a helper that goes dead *everywhere* is
//! no longer reported, and nothing but review will notice.

// Stated here rather than inherited from whichever test file declares the
// module, so a new consumer cannot fail to compile this one.
#![allow(clippy::expect_used)]
#![allow(dead_code)]

use std::time::Duration;

use iroh::endpoint::{presets, IdleTimeout, QuicTransportConfig};
use iroh::Endpoint;
use keylink::Node;
use tokio::sync::{mpsc, watch};

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

/// A node that stops waiting on an unanswered dial after `idle`.
///
/// QUIC's default idle timeout is 30 seconds, so a dial to an endpoint that is
/// simply gone costs the full 30s before it can fail. One test does exactly
/// that, and it was the slowest thing in the whole suite by an order of
/// magnitude — 30.2s against 3.4s for the next binary.
///
/// This shortens the ceiling, never the conclusion. The dial being waited on
/// cannot succeed, so a smaller budget can only make the same failure arrive
/// sooner. iroh does the same thing in its own suite for the same reason.
///
/// Use it only where the wait is a timeout you are not measuring. A test that
/// asserts something *completed* must not borrow this — load would turn a
/// passing run red.
pub async fn local_node_with_idle_timeout(idle: Duration) -> Node {
    let transport = QuicTransportConfig::builder()
        .max_idle_timeout(Some(
            IdleTimeout::try_from(idle).expect("a test-sized idle timeout fits"),
        ))
        .build();

    Node::from_endpoint(
        Endpoint::builder(presets::Minimal)
            .transport_config(transport)
            .bind()
            .await
            .expect("a local endpoint binds"),
    )
}

/// How long a test waits for something that should already have happened.
///
/// A liveness bound, never a measurement. Nothing is asserted to be *fast*; this
/// is what turns a deadlock into a named failure instead of a hung test runner,
/// and a passing path never comes near it.
pub const NO_PROGRESS: Duration = Duration::from_secs(20);

/// Overlap and holding, proven without consulting a clock: the handler's half.
///
/// A handler that enters announces itself and then parks until the test releases
/// it. So a test can assert that a call *reached* the server without waiting for
/// it to finish, and can hold every handler at once to prove they overlap. Load
/// makes the parking longer and the conclusion no weaker, which is exactly what
/// a wall-clock assertion cannot say.
///
/// Shared because it was written twice, verbatim, in one commit — including the
/// `wait_for` comment below, which is the kind of subtlety that survives in one
/// copy and rots in the other.
pub struct Rendezvous {
    entered: mpsc::UnboundedSender<u64>,
    release: watch::Receiver<bool>,
}

/// The test's half: what arrived, and the switch that lets it go.
///
/// Fields rather than methods. The two consumers consume arrivals differently —
/// one waits for a fixed count, one races a `recv` against a call that must not
/// answer yet — and a method covering both would be an interface designed around
/// two callers.
pub struct Arrivals {
    pub entered: mpsc::UnboundedReceiver<u64>,
    pub release: watch::Sender<bool>,
}

/// Both halves of one rendezvous.
///
/// A free function rather than `Rendezvous::new`, because it returns the other
/// half too and a `new` that mostly hands back something else reads as a lie.
pub fn rendezvous() -> (Rendezvous, Arrivals) {
    let (entered, arrived) = mpsc::unbounded_channel();
    let (release, held) = watch::channel(false);
    (
        Rendezvous {
            entered,
            release: held,
        },
        Arrivals {
            entered: arrived,
            release,
        },
    )
}

impl Rendezvous {
    /// Announce this handler, then wait for the test to let it go.
    ///
    /// `marker` is whatever identifies this arrival to the test — a call's
    /// argument, a run counter. A receiver the test has dropped means the test
    /// is already failing, and a handler has nothing useful to do about that.
    pub async fn hold(&self, marker: u64) {
        let _ = self.entered.send(marker);
        let mut release = self.release.clone();
        // `wait_for` re-reads the current value before parking, so a release
        // landing between the announcement and this line is not lost.
        let _ = release.wait_for(|open| *open).await;
    }
}
