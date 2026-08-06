//! A peer's departure, learned without calling it.
//!
//! Two endpoints on the loopback interface with no relay and no discovery, so
//! the connection is genuine and the tests need no network. What is being
//! tested is the gap this closes: before a watch, the only way to find out a
//! peer had gone was to call it and read the error.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::future::Future;
use std::time::Duration;

use keylink::{Allowlist, Departure, Node, RpcClient};

keylink::service! {
    service Sentinel;

    rpc Ping() -> String;
}

struct Handler;

impl sentinel::Handler for Handler {
    async fn ping(&self) -> String {
        "pong".to_string()
    }
}

/// Long enough that a slow machine never trips it, short enough that a watch
/// which fails to fire is reported as a failure rather than a hang.
const PATIENCE: Duration = Duration::from_secs(10);

async fn within<F: Future>(fut: F) -> F::Output {
    tokio::time::timeout(PATIENCE, fut)
        .await
        .expect("the watch resolves rather than hanging")
}

mod common;
use common::local_node;

type Client = RpcClient<sentinel::Request, sentinel::Response>;

/// A served node and a client dialled into it. Both nodes are held so the
/// endpoints outlive the test body.
struct Fixture {
    client: Client,
    server: keylink::Server,
    _nodes: [Node; 2],
}

async fn start() -> Fixture {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let server =
        sentinel::serve(&server_node, Handler, Allowlist::anyone()).expect("one service, no clash");
    let client = Client::connect(
        &client_node,
        server_node.endpoint().addr(),
        sentinel::alpn(),
    )
    .await
    .expect("the client dials the server");

    Fixture {
        client,
        server,
        _nodes: [server_node, client_node],
    }
}

/// The headline. Every other failure in this crate is the answer to a question
/// you asked; this one arrives on its own, with no call in flight and none
/// made after the watch was created.
#[tokio::test]
async fn a_client_learns_the_server_is_gone_without_calling_it() {
    let fixture = start().await;

    // A real call first, so the connection under test is one that was working.
    let (reply, _body) = fixture
        .client
        .call(sentinel::Request::Ping {}, keylink::Body::empty())
        .await
        .expect("the service answers");
    assert!(matches!(reply, sentinel::Response::Ping(text) if text == "pong"));

    let watch = fixture.client.watch();
    fixture.server.shutdown().await.expect("the server stops");

    assert!(
        matches!(within(watch).await, Departure::Closed),
        "the departure must arrive without a call"
    );
}

/// Closing our own client must never look like the peer leaving. An operator
/// who cannot tell the two apart pages themselves on every deploy.
#[tokio::test]
async fn closing_our_own_client_is_reported_as_local() {
    let fixture = start().await;
    let watch = fixture.client.watch();

    fixture.client.close().await.expect("closes cleanly");

    assert!(
        matches!(within(watch).await, Departure::Local),
        "our own shutdown is not a peer departure"
    );
}

/// A watch outlives the client that made it, and does not hold it open.
///
/// If the watch kept a strong handle the connection would never close, and this
/// would hang instead of resolving — which is the failure mode a diagnostic
/// must not have.
#[tokio::test]
async fn a_watch_outlives_its_client_without_holding_the_connection_open() {
    let fixture = start().await;
    let watch = fixture.client.watch();

    drop(fixture);

    assert!(matches!(within(watch).await, Departure::Local));
}

/// One connection, many independent watchers, no shared subscription to manage.
#[tokio::test]
async fn every_watcher_on_one_client_is_told() {
    const WATCHERS: usize = 8;

    let fixture = start().await;
    let watches: Vec<_> = (0..WATCHERS).map(|_| fixture.client.watch()).collect();

    fixture.server.shutdown().await.expect("the server stops");

    for watch in watches {
        assert!(matches!(within(watch).await, Departure::Closed));
    }
}
