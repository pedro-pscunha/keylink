//! The lazily connected, self-healing handle.
//!
//! Two peers over real QUIC on loopback, no relay and no discovery.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use keylink::{Allowlist, Node, Peer};

mod common;
use common::local_node;

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
    async fn tick(&self) -> u64 {
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
            async { Err(keylink::Error::BlockingInAsync) }
        })
        .await;

    assert!(outcome.is_err(), "the second failure is returned as-is");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "once on the existing connection, once on a fresh one, and no more"
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
    let first = peer
        .call(|c| async move { Ok(Arc::as_ptr(&c) as usize) })
        .await
        .expect("connects");

    peer.disconnect();

    let second = peer
        .call(|c| async move { Ok(Arc::as_ptr(&c) as usize) })
        .await
        .expect("dials again");

    assert_ne!(
        first, second,
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
    let caller = local_node().await;
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
    assert!(
        elapsed < std::time::Duration::from_secs(120),
        "gave up in {elapsed:?}, which is bounded"
    );
}
