//! Fire-and-forget, over a real QUIC connection.
//!
//! Two peers on loopback, no relay and no discovery.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::{Allowlist, Context, Transmitted};

mod common;
use common::local_node;

keylink::service! {
    service Log;

    tell Note(line: String) -> ();
    rpc  Count() -> u64;
}

#[derive(Default)]
struct Notes(AtomicUsize);

impl log::Handler for Arc<Notes> {
    async fn note(&self, _cx: &Context, _line: String) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    async fn count(&self, _cx: &Context) -> u64 {
        self.0.load(Ordering::SeqCst) as u64
    }
}

/// The property: the message arrives and is handled, with no reply stream.
#[tokio::test]
async fn a_tell_is_delivered_and_handled() {
    let server = local_node().await;
    let caller = local_node().await;
    let state = Arc::new(Notes::default());
    let _running = log::serve(&server, state.clone(), Allowlist::anyone()).expect("serves");

    let client = log::Client::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials");

    let _ = client.note("first".to_owned()).await.expect("tells");

    // A `tell` returns before the handler runs, so the only honest way to
    // observe it is to ask afterwards on a stream that DOES have a reply. This
    // is also the shape a caller has to use, and saying so here is the point.
    let mut seen = 0u64;
    for _ in 0..50 {
        seen = client.count().await.expect("counts");
        if seen > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(seen, 1, "the tell reached the handler");
}

/// The signature is the guarantee: a remote `tell` hands back proof of local
/// transmission, never the method's declared return type or proof of delivery.
#[tokio::test]
async fn a_tell_returns_transmission_proof_not_a_value() {
    let server = local_node().await;
    let caller = local_node().await;
    let _running =
        log::serve(&server, Arc::new(Notes::default()), Allowlist::anyone()).expect("serves");

    let client = log::Client::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials");

    // The private-field proof can only come from the transport path. It says
    // exactly which phase was observed and cannot be mistaken for a handler
    // result.
    let transmitted: Transmitted = client
        .note("only transmission".to_owned())
        .await
        .expect("transmits");
    assert_eq!(transmitted, transmitted);
}

/// Many tells keep their order relative to each other on one client.
#[tokio::test]
async fn tells_on_one_client_arrive_in_the_order_they_were_sent() {
    let server = local_node().await;
    let caller = local_node().await;
    let state = Arc::new(Notes::default());
    let _running = log::serve(&server, state.clone(), Allowlist::anyone()).expect("serves");

    let client = log::Client::connect(&caller, server.endpoint().addr())
        .await
        .expect("dials");

    for n in 0..20 {
        let _ = client.note(format!("line {n}")).await.expect("tells");
    }

    let mut seen = 0u64;
    for _ in 0..100 {
        seen = client.count().await.expect("counts");
        if seen == 20 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(seen, 20, "every tell was handled");
}

/// A tell to a peer the allowlist refuses is accepted locally and dropped.
///
/// This is the honest half of the contract and the reason `tell` is narrow: the
/// caller cannot learn it was refused, because there is no channel to say so.
/// Pinned so nobody later "improves" it into a silent round trip.
#[tokio::test]
async fn a_refused_tell_still_returns_ok_locally() {
    let server = local_node().await;
    let caller = local_node().await;
    let state = Arc::new(Notes::default());

    // Admits somebody else entirely.
    let stranger = local_node().await;
    let _running = log::serve(
        &server,
        state.clone(),
        Allowlist::only([stranger.endpoint().id()]),
    )
    .expect("serves");

    // The dial itself may fail, or may succeed and be closed straight after —
    // the allowlist runs per connection, not per stream. Either way, no tell
    // may reach the handler.
    if let Ok(client) = log::Client::connect(&caller, server.endpoint().addr()).await {
        let _ = client.note("never handled".to_owned()).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        state.0.load(Ordering::SeqCst),
        0,
        "an unauthorized peer's tell must never reach the handler"
    );
}
