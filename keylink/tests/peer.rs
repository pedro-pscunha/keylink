//! The lazily connected, self-healing handle.
//!
//! Two peers over real QUIC on loopback, no relay and no discovery.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use keylink::{Allowlist, Context, Node, Peer, RemoteError};

mod common;
use common::{local_node, local_node_with_idle_timeout};

keylink::service! {
    service Meter;

    rpc Tick() -> u64;
}

/// Counts how many times a client was dialled, so a test can tell "reused" from
/// "reconnected" — which is the whole thing under test and is otherwise
/// invisible.
#[derive(Default)]
struct Ticks(AtomicUsize);

impl meter::Handler for Arc<Ticks> {
    async fn tick(&self, _cx: &Context) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst) as u64
    }
}

async fn serve(node: &Node, state: Arc<Ticks>) -> keylink::Server {
    meter::serve(node, state, Allowlist::anyone()).expect("serves")
}

#[tokio::test]
async fn a_peer_connects_on_first_use_and_reuses_the_connection() {
    let server = local_node().await;
    let caller = local_node().await;
    let state = Arc::new(Ticks::default());
    let _running = serve(&server, state.clone()).await;

    let peer: Peer<meter::Client> = Peer::new(&caller, server.endpoint().addr());

    // Naming a peer dials nothing.
    assert!(
        peer.call(|c| async move { c.tick().await }).await.is_ok(),
        "the first call connects"
    );

    // Three more calls, and every one must land on the SAME connection. If the
    // handle re-dialled each time the counter would still climb, so the proof
    // is that the client handed to the closure is the same allocation.
    let mut seen = Vec::new();
    for _ in 0..3 {
        let ptr = peer
            .call(|c| {
                let id = Arc::as_ptr(&c) as usize;
                async move { Ok(id) }
            })
            .await
            .expect("reuses");
        seen.push(ptr);
    }
    assert_eq!(
        seen.iter().collect::<std::collections::BTreeSet<_>>().len(),
        1,
        "every call after the first must reuse one client, not dial a new one"
    );
    assert_eq!(state.0.load(Ordering::SeqCst), 1, "one real rpc happened");
}

/// The retry is exactly one, and this counts it.
///
/// A dead connection cannot be manufactured from outside — `Client::close`
/// consumes the client and `Peer` hands out an `Arc` — so this drives the
/// semantics directly: a closure that always fails must be invoked **twice**,
/// never once and never three times.
#[tokio::test]
async fn a_failed_call_is_retried_exactly_once_on_a_fresh_connection() {
    let server = local_node().await;
    let caller = local_node().await;
    let _running = serve(&server, Arc::new(Ticks::default())).await;

    let peer: Peer<meter::Client> = Peer::new(&caller, server.endpoint().addr());
    let attempts = AtomicUsize::new(0);

    let outcome: keylink::Result<()> = peer
        .call(|_c| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err(keylink::Error::Closed) }
        })
        .await;

    assert!(outcome.is_err(), "the second failure is returned as-is");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "once on the existing connection, once on a fresh one, and no more"
    );
}

/// A remote/application error proves that the first request reached the peer,
/// so replaying it would be unsafe even though it is represented as `Err`.
#[tokio::test]
async fn a_remote_error_is_not_retried() {
    let server = local_node().await;
    let caller = local_node().await;
    let _running = serve(&server, Arc::new(Ticks::default())).await;

    let peer: Peer<meter::Client> = Peer::new(&caller, server.endpoint().addr());
    let attempts = AtomicUsize::new(0);

    let outcome: keylink::Result<()> = peer
        .call(|_c| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err(keylink::Error::Remote(RemoteError::TimedOut { millis: 25 })) }
        })
        .await;

    assert!(matches!(
        outcome,
        Err(keylink::Error::Remote(RemoteError::TimedOut { millis: 25 }))
    ));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

/// A local body/storage error says nothing about the QUIC connection. It must
/// return as-is without throwing away a client another call can still use.
#[tokio::test]
async fn a_local_io_error_keeps_the_cached_connection() {
    let server = local_node().await;
    let caller = local_node().await;
    let _running = serve(&server, Arc::new(Ticks::default())).await;

    let peer: Peer<meter::Client> = Peer::new(&caller, server.endpoint().addr());
    let before = peer.connect().await.expect("connects");
    let outcome: keylink::Result<()> = peer
        .call(|_client| async {
            Err(keylink::Error::Io(std::io::Error::other(
                "the local body sink failed",
            )))
        })
        .await;
    assert!(matches!(outcome, Err(keylink::Error::Io(_))));

    let after = peer.connect().await.expect("the cached client remains");
    assert!(
        Arc::ptr_eq(&before, &after),
        "local I/O must not invalidate a healthy peer connection"
    );
}

/// A failed retry is still known to be a dead connection. Returning its error
/// must not leave that same client cached for the next independent call.
#[tokio::test]
async fn a_second_connection_failure_is_not_left_cached() {
    let server = local_node().await;
    let caller = local_node().await;
    let _running = serve(&server, Arc::new(Ticks::default())).await;

    let peer: Peer<meter::Client> = Peer::new(&caller, server.endpoint().addr());
    let attempted = std::sync::Mutex::new(Vec::new());
    let outcome: keylink::Result<()> = peer
        .call(|client| {
            attempted.lock().expect("not poisoned").push(client);
            async { Err(keylink::Error::Closed) }
        })
        .await;
    assert!(matches!(outcome, Err(keylink::Error::Closed)));

    let failed_retry = attempted
        .lock()
        .expect("not poisoned")
        .get(1)
        .expect("the connection failure is retried once")
        .clone();
    let next = peer
        .connect()
        .await
        .expect("the next call reconnects directly");
    assert!(
        !Arc::ptr_eq(&failed_retry, &next),
        "a client whose retry also failed must not remain cached"
    );
}

/// And a call that succeeds is never repeated. The obvious other half: a
/// retry-on-success would double every side effect in the system.
#[tokio::test]
async fn a_successful_call_is_not_repeated() {
    let server = local_node().await;
    let caller = local_node().await;
    let state = Arc::new(Ticks::default());
    let _running = serve(&server, state.clone()).await;

    let peer: Peer<meter::Client> = Peer::new(&caller, server.endpoint().addr());
    let attempts = AtomicUsize::new(0);

    peer.call(|c| {
        attempts.fetch_add(1, Ordering::SeqCst);
        async move { c.tick().await }
    })
    .await
    .expect("succeeds");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        state.0.load(Ordering::SeqCst),
        1,
        "one rpc reached the server"
    );
}

/// `disconnect` forgets the connection, and the next call dials a new one.
#[tokio::test]
async fn a_disconnected_peer_dials_again_on_the_next_call() {
    let server = local_node().await;
    let caller = local_node().await;
    let state = Arc::new(Ticks::default());
    let _running = serve(&server, state.clone()).await;

    let peer: Peer<meter::Client> = Peer::new(&caller, server.endpoint().addr());
    // The client is HELD, not turned into an address. Comparing
    // `Arc::as_ptr(..) as usize` across a drop compares allocator addresses,
    // and the allocator reuses one it just freed — so that spelling failed
    // roughly one run in three, correctly reporting a different client as the
    // same one. Keeping the first alive makes reuse impossible by construction.
    let first = peer.call(|c| async move { Ok(c) }).await.expect("connects");

    peer.disconnect();

    let second = peer
        .call(|c| async move { Ok(c) })
        .await
        .expect("dials again");

    assert!(
        !Arc::ptr_eq(&first, &second),
        "a forgotten connection is replaced, not reused"
    );
}

/// The test that exists because "reconnect once" and "reconnect forever" look
/// identical in review.
///
/// The server is gone entirely, so no number of re-dials can help. The handle
/// must give up and return the error rather than retrying until the caller's
/// deadline — a peer that is genuinely absent has to be reported to the code
/// that can react to it.
#[tokio::test]
async fn a_peer_gives_up_rather_than_retrying_forever() {
    // The dial below cannot succeed — the server is closed — so what is being
    // waited out is QUIC's idle timeout, and its 30s default made this one test
    // the slowest thing in the suite by an order of magnitude. A 2s ceiling
    // changes how long the failure takes to arrive, never whether it arrives.
    let caller = local_node_with_idle_timeout(std::time::Duration::from_secs(2)).await;
    let addr = {
        let doomed = local_node().await;
        let addr = doomed.endpoint().addr();
        doomed.endpoint().close().await;
        addr
    };

    let peer: Peer<meter::Client> = Peer::new(&caller, addr);

    let started = std::time::Instant::now();
    let outcome = peer.call(|c| async move { c.tick().await }).await;
    let elapsed = started.elapsed();

    assert!(
        outcome.is_err(),
        "an absent peer must be reported, not retried into a hang"
    );
    // One-sided on purpose: load can only make this slower, so a pass under a
    // generous ceiling still means "bounded" rather than "forever". A retry
    // loop would blow through it by orders of magnitude.
    //
    // 30s and not the old 120s. The caller above now gives up on an idle
    // connection after 2s and the handle re-dials once, so the honest cost is a
    // few seconds and the ceiling is still an order of magnitude above it.
    // It is a ceiling, never a measurement — do not tighten it toward the
    // observed time, because that turns a machine under load into a red gate.
    assert!(
        elapsed < std::time::Duration::from_secs(30),
        "gave up in {elapsed:?}, which is bounded"
    );
}
