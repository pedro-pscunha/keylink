//! The relay and discovery paths, with both servers running in this process.
//!
//! Every other test in this repo binds `presets::Minimal` — no relay, no
//! discovery, loopback only. That is the right default and it left a hole: the
//! two mechanisms `Node::bind` actually depends on had never executed under
//! `cargo test`. Relay fallback carries a connection when no direct path can be
//! opened, and pkarr-plus-DNS is what turns a bare `EndpointId` into an address.
//! Both are what a first-time user meets, and neither was covered.
//!
//! ## What is covered, stated precisely
//!
//! Not `presets::N0` itself. That preset names n0's *public* relays and *public*
//! DNS, so a test of the constant would need the internet, which is the thing
//! this suite refuses. What runs here is the machinery underneath it — the same
//! relay transport, the same pkarr publisher, the same DNS lookup — pointed at
//! servers on localhost. Everything except the URL.
//!
//! ## Why this is not flaky
//!
//! Nothing here touches a network. `run_relay_server` binds
//! `127.0.0.1:0`, and `DnsPkarrServer` binds its DNS and pkarr listeners the
//! same way. The one genuine race — dialling by id before the announce has been
//! published — is closed by `DnsPkarrServer::on_endpoint`, which waits for the
//! server's own state to record the publish rather than sleeping and hoping.
//!
//! Two details that are easy to get wrong and produce a mysterious hang:
//! the test relay serves a self-signed certificate, so every endpoint here needs
//! `CaTlsConfig::insecure_skip_verify()`; and the helpers live in this file
//! rather than in `tests/common/mod.rs` because they have exactly one consumer,
//! and `common` is compiled into every test binary that declares it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use iroh::endpoint::presets;
use iroh::test_utils::{run_relay_server, DnsPkarrServer};
use iroh::tls::CaTlsConfig;
use iroh::{Endpoint, EndpointAddr, RelayMap, RelayMode};
use keylink::{Allowlist, Node};

/// Long enough that a loaded machine does not fail, short enough that a genuine
/// hang is reported as one rather than running until the harness gives up.
///
/// Every wait here is on an event — a relay connecting, an announce landing —
/// so this is an upper bound on a failure, never a sleep the happy path pays.
const PATIENCE: Duration = Duration::from_secs(30);

keylink::service! {
    service Echo;

    rpc Ping() -> String;
}

struct Server;

impl echo::Handler for Server {
    async fn ping(&self) -> String {
        "pong".to_string()
    }
}

/// An endpoint whose only transport is the test relay.
///
/// `insecure_skip_verify` is not optional: `run_relay_server` generates a
/// self-signed certificate, and an endpoint that validates it cannot reach the
/// relay at all. The failure is a timeout rather than a certificate error, which
/// is why it is called out here.
async fn relay_endpoint(relay: &RelayMap) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Custom(relay.clone()))
        .ca_tls_config(CaTlsConfig::insecure_skip_verify())
        .bind()
        .await
        .expect("an endpoint binds against the test relay")
}

/// The same, plus publishing to and resolving from the test pkarr/DNS server.
///
/// `DnsPkarrServer::preset` also installs `AddrFilter::relay_only()`, so an
/// address learned through discovery keeps only its relay URL. A peer found this
/// way therefore cannot be reached directly, which is what makes the discovery
/// test exercise the relay rather than quietly succeeding over loopback.
async fn discoverable_endpoint(relay: &RelayMap, dns: &DnsPkarrServer) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Custom(relay.clone()))
        .ca_tls_config(CaTlsConfig::insecure_skip_verify())
        .preset(dns.preset())
        .bind()
        .await
        .expect("an endpoint binds against the test relay and dns")
}

/// A call completes when the relay is the only way to reach the peer.
///
/// The address handed to the client carries a relay URL and no IP at all, so
/// there is no direct path to fall back to and no loopback shortcut to take. If
/// the reply arrives, it arrived through the relay.
#[tokio::test]
async fn a_call_lands_when_the_relay_is_the_only_path() {
    let (relay_map, relay_url, _relay) = run_relay_server().await.expect("the relay starts");

    let server_node = Node::from_endpoint(relay_endpoint(&relay_map).await);
    let client_node = Node::from_endpoint(relay_endpoint(&relay_map).await);
    let _running = echo::serve(&server_node, Server, Allowlist::anyone()).expect("serves");

    let through_relay = EndpointAddr::new(server_node.id()).with_relay_url(relay_url);
    assert!(
        through_relay.ip_addrs().next().is_none(),
        "the point of this test is that no direct address is on offer, got {through_relay:?}"
    );

    let client = echo::Client::connect(&client_node, through_relay)
        .await
        .expect("the client reaches the server through the relay");

    assert_eq!(client.ping().await.expect("the call completes"), "pong");
}

/// A bare `EndpointId` is enough to reach a peer, with no address anywhere.
///
/// This is the half `demo connect <id>` depends on in production: the client is
/// told a public key and nothing else, and has to find the rest for itself.
#[tokio::test]
async fn a_bare_endpoint_id_is_dialable_through_discovery() {
    let (relay_map, _relay_url, _relay) = run_relay_server().await.expect("the relay starts");
    let dns = DnsPkarrServer::run()
        .await
        .expect("the dns and pkarr servers start");

    let server_node = Node::from_endpoint(discoverable_endpoint(&relay_map, &dns).await);
    let client_node = Node::from_endpoint(discoverable_endpoint(&relay_map, &dns).await);
    let _running = echo::serve(&server_node, Server, Allowlist::anyone()).expect("serves");

    // Wait for the announce to be recorded rather than sleeping: dialling before
    // the publish lands would fail for a reason that has nothing to do with
    // whether discovery works.
    dns.on_endpoint(&server_node.id(), PATIENCE)
        .await
        .expect("the server publishes itself to pkarr");

    // The id, and nothing else. No `EndpointAddr`, no relay URL, no IP.
    let client = echo::Client::connect(&client_node, server_node.id())
        .await
        .expect("a bare endpoint id resolves to a reachable address");

    assert_eq!(client.ping().await.expect("the call completes"), "pong");
}

/// `Node::online` returns against a live relay.
///
/// `demo serve` awaits this before it prints an id for anyone to dial, and until
/// now nothing had ever executed it. It resolves only when a home relay reports
/// connected, so with no relay configured it waits forever — which is a hang in
/// the one place a reader is told to wait.
#[tokio::test]
async fn online_returns_once_a_relay_is_connected() {
    let (relay_map, _relay_url, _relay) = run_relay_server().await.expect("the relay starts");
    let node = Node::from_endpoint(relay_endpoint(&relay_map).await);

    tokio::time::timeout(PATIENCE, node.online())
        .await
        .expect("online() must return once a relay is reachable, not wait forever");
}
