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
//! Nothing the GATE runs touches a network — the four tests below it. The fifth
//! is `#[ignore]`d and reaches n0's public infrastructure on purpose; it is the
//! reason this heading names the gated four rather than the file.
//!
//! `run_relay_server` binds `127.0.0.1:0`, and `DnsPkarrServer` binds its DNS and
//! pkarr listeners the same way. The one genuine race — dialling by id before the
//! announce has been published — is closed by `DnsPkarrServer::on_endpoint`,
//! which waits for the server's own state to record the publish rather than
//! sleeping and hoping.
//!
//! Loopback is not the only address in play, and that surprised a reviewer's
//! stress run: a failing sample showed the connection selecting the machine's LAN
//! address. Binding `0.0.0.0` gives every interface, so a "direct path" here can
//! be a real NIC.
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
use keylink::{Allowlist, Context, Node};
// `paths_stream()` is a `Stream`, and `next()` comes from the extension trait.
// `n0_future` re-exports `futures_lite`'s, and it is already a `keylink`
// dependency, so this adds nothing to the build.
use n0_future::StreamExt;

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
    async fn ping(&self, _cx: &Context) -> String {
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

/// A connection that starts on the relay finds a direct path and stops paying for it.
///
/// The three tests above all arrange for the relay to be the only option. None of
/// them can see the opposite failure, and that one is silent and expensive: every
/// call still succeeds, through a relay, at relay latency and relay bandwidth,
/// forever. A `Node` that quietly suppressed direct addresses would keep this
/// whole file green.
///
/// **One dial, given the relay and nothing else, and the assertion is about
/// which path ends up SELECTED.** Of the paths a connection holds, exactly one
/// carries traffic. It starts as the relay, and it must become the direct one.
///
/// "A direct path exists" would not do: such a connection carries both kinds at
/// once, so that predicate is satisfied no matter which one is actually used.
/// `is_selected()` is the discriminating fact.
///
/// An earlier draft of this comment appealed to iroh doing "the same thing the
/// same way" in `test_utils::test_transport`. Do not restore that: those helpers
/// live inside a `#[cfg(test)] mod tests` and cannot be reached from here at all,
/// and the nearest-named one, `is_ip_selected_from_ip_and_custom`, opens with
/// `if !has_ip || !has_custom { return true; }` — it returns TRUE when only one
/// kind of path is present, which is precisely the vacuity this test exists to
/// refuse. An appeal to authority is worth less than the predicate below.
///
/// **What this does not prove.** Selection is a property of the connection, not
/// of a stream, so this does not prove any individual RPC travelled the direct
/// path — only that the direct path is the one the connection settled on.
///
/// The observation is `Connection::paths`, which keylink's generated `Client`
/// does not hand back, so the dial goes through `node.endpoint()` with
/// `echo::alpn()` — the same ALPN and the same endpoint configuration the RPC
/// above uses.
#[tokio::test]
async fn a_relayed_connection_upgrades_to_a_direct_path() {
    let (relay_map, relay_url, _relay) = run_relay_server().await.expect("the relay starts");

    let server_node = Node::from_endpoint(relay_endpoint(&relay_map).await);
    let client_node = Node::from_endpoint(relay_endpoint(&relay_map).await);
    let _running = echo::serve(&server_node, Server, Allowlist::anyone()).expect("serves");

    // The relay and nothing else, exactly as in the first test in this file.
    let through_relay = EndpointAddr::new(server_node.id()).with_relay_url(relay_url);
    assert!(
        through_relay.ip_addrs().next().is_none(),
        "the upgrade is only interesting from a standing start, got {through_relay:?}"
    );

    // The ordinary path still works. If this breaks, the assertion below is not
    // the interesting failure.
    let client = echo::Client::connect(&client_node, through_relay.clone())
        .await
        .expect("the client reaches the server");
    assert_eq!(client.ping().await.expect("the call completes"), "pong");

    let conn = client_node
        .endpoint()
        .connect(through_relay, echo::alpn())
        .await
        .expect("the same endpoint dials the same ALPN");

    // `is_selected`, and not merely "a direct path exists somewhere in the list".
    // That distinction is the whole test, and it took FOUR drafts to find:
    //
    //   1. A wait that rebuilt `paths_stream()` each iteration. A stream that
    //      yields immediately never returns `Pending`, so the timer was never
    //      polled and `timeout` could not fire — it HUNG past its own bound.
    //   2. Asserting `any(is_ip)` on a dial that offered IPs passed with the
    //      predicate inverted to `is_relay()` — such a connection carries BOTH
    //      kinds, so either predicate is satisfied and neither asserts anything.
    //   3. Asserting "no direct path yet, then one appears" from a relay-only dial
    //      failed 8 runs in 12, because the direct path is already selected by the
    //      time `connect` returns. **Not a loopback artifact.** This test opens a
    //      SECOND connection to a peer the first one is still connected to, and
    //      iroh opens "any opened paths on all other connections without
    //      holepunching" (`socket/remote_map/remote_state.rs`). A real network
    //      does the same. Dropping the first connection, or dialling from a fresh
    //      endpoint, is what would restore a standing start.
    //   4. A stream-only wait, without the snapshot tie-breaker below.
    //
    // Selection is the property that actually matters and the only one that is
    // stable: of the paths a connection holds, exactly one carries traffic, and
    // it should be the direct one.
    //
    // ── WATCHED, both directions, 2026-08-08 ────────────────────────────────
    //   draft 4, stream only    12 passed HERE, then 3 FAILED in 57 on a loaded
    //                           machine. One dump showed `is_selected: true` on a
    //                           direct path AT ASSERT TIME — the stream had missed
    //                           a transition that already landed.
    //   as written              40 runs under a concurrent `--workspace` load,
    //                           0 failures; a reviewer added 30 more, 0 failures
    //   `is_ip` -> `is_relay`   FAILED at 31.092s WITH the tie-breaker in place —
    //                           all three occurrences inverted, so the new `||`
    //                           arm is not an escape hatch. Re-measured, because
    //                           the earlier 31.088s predates that arm.
    //
    // **Twelve green runs are not evidence of a stable test.** They were mine,
    // and a reviewer found the flake at 57. Draft 3 above failed 8 in 12 and
    // still passed every time it ran inside the full file. Load changes the
    // answer, so re-run both directions AND a burst before touching this wait.

    // Waited for, not slept on: path selection is a negotiation, so a direct path
    // is not always present on the first snapshot. `PATIENCE` is an upper bound on
    // a failure and the happy path returns long before it.
    //
    // **The stream is built ONCE, outside the loop, and that is not style.** An
    // earlier draft called `conn.paths_stream()` on every iteration. When the new
    // stream yields immediately the loop never returns `Pending`, so the runtime
    // never gets to poll the timer, and `timeout` cannot fire — the test hung past
    // its own 30s bound instead of failing. Found by inverting this predicate to
    // `is_relay()` to check the assertion was sensitive at all.
    let mut updates = conn.paths_stream();
    let watched = tokio::time::timeout(PATIENCE, async {
        if conn
            .paths()
            .iter()
            .any(|path| path.is_selected() && path.is_ip())
        {
            return true;
        }
        while let Some(paths) = updates.next().await {
            if paths.iter().any(|path| path.is_selected() && path.is_ip()) {
                return true;
            }
        }
        // **`paths_stream` ends when the CONNECTION closes.** Falling out of this
        // loop is not success, and returning `()` made it look like success: the
        // block completed, `timeout` yielded `Ok`, and a connection that died
        // before selecting anything passed. Inverting the predicate never reached
        // this branch, so neither recorded break could see it.
        false
    })
    .await;

    // The snapshot is a tie-breaker, not a second chance. A transition can land
    // between two polls of the stream and never be yielded to us — measured: 3
    // failures in 57 runs, each at the `PATIENCE` bound, and one panic dump showed
    // `is_selected: true` on a direct path AT ASSERT TIME. The wait had missed an
    // answer that was already true.
    let settled = watched == Ok(true)
        || conn
            .paths()
            .iter()
            .any(|path| path.is_selected() && path.is_ip());

    assert!(
        settled,
        "the selected path never became the direct one, so every call rides the \
         relay: {:?}",
        conn.paths().iter().collect::<Vec<_>>()
    );
}

// ─────────────────────────────────────────────── the public preset, on demand
//
// Everything above points the real machinery at servers on localhost. What none
// of it touches is `presets::N0` itself — the one constant `Node::bind` actually
// names, holding n0's PUBLIC relay URLs and PUBLIC DNS.
//
// That is not a gap anybody can close hermetically: testing the constant means
// reaching n0's infrastructure, and a gate that needs the internet fails on a
// train. So it is `#[ignore]`d and run on demand:
//
//     cargo test --test relay -- --ignored
//
// Run it when you change `Node::bind`, bump iroh, or suspect n0's defaults have
// moved under you. A red here is as likely to mean "n0 is having an outage" as
// "keylink is broken", which is exactly why it is not allowed to gate a push.

/// `Node::bind` — the real preset, the real relays, the real DNS.
///
/// This is the first line of the README and the first thing a user runs, and it
/// is the one path in the crate that no automated check has ever executed.
#[tokio::test]
#[ignore = "reaches n0's public relay and DNS; run with --ignored"]
async fn the_public_preset_reaches_a_peer_by_id_alone() {
    let server_node = Node::bind().await.expect("bind against n0's public preset");
    let client_node = Node::bind().await.expect("bind against n0's public preset");
    let _running = echo::serve(&server_node, Server, Allowlist::anyone()).expect("serves");

    // `bind` publishes to pkarr and resolves through DNS, so the id alone has to
    // be enough — the same claim `demo connect <id>` makes to a stranger.
    tokio::time::timeout(PATIENCE, server_node.online())
        .await
        .expect("a public relay must accept us before anyone can find us");

    let client = tokio::time::timeout(
        PATIENCE,
        echo::Client::connect(&client_node, server_node.id()),
    )
    .await
    .expect("discovery through n0's DNS must not hang")
    .expect("a bare id resolves through the public preset");

    assert_eq!(client.ping().await.expect("the call completes"), "pong");
}
