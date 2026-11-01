//! The accept side speaking first, over two real nodes.
//!
//! RFC-04 §4.4's Design B, and the thing to notice is what is **not** here:
//! no new method kind, no `WireKinds` field, no second meaning for an existing
//! one. The reverse contract is an ordinary `service!` block with its own
//! derived ALPN, served by the side that dialled. Both directions keep their own
//! allowlist and their own schema gate, because both are ordinary services.
//!
//! So a "server push" is two services that happen to point at each other, and
//! the only thing keylink adds is the bookkeeping: which peers are here, and
//! what is the handle for talking back to each of them.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::{Allowlist, Context, EndpointAddr, EndpointId, Node, Server, Subscribers};

mod common;
use common::local_node;

// ───────────────────────────────────────────── the forward contract, dialled

keylink::service! {
    service Desk;

    /// Ask to be told about prices. Answers with how many peers are listening,
    /// so the test can see the registry from the outside.
    rpc Subscribe(back: String) -> u64;
    rpc Unsubscribe() -> u64;
}

// ───────────────────────────────────────── the reverse contract, dialled back

keylink::service! {
    service Ticker;

    /// The push. An `rpc` rather than a `tell`, so the accept side learns
    /// whether the peer took it — a `tell`'s `Ok` means the bytes reached the
    /// transport and nothing more.
    rpc Tick(price: u64) -> u64;
}

/// The accept side. Holds the node so it can dial back, and the registry so it
/// can find the peers to dial back to.
struct Desk {
    node: Node,
    subscribers: Subscribers<ticker::Client>,
}

impl desk::Handler for Desk {
    /// `back` is the peer's own address, and it is a **claim**. `cx.peer()` is
    /// the key iroh proved at the handshake, and it is a **proof**. Reconciling
    /// the two here is the whole of the admission this direction needs: an
    /// address naming somebody else is refused, so a peer cannot make us dial a
    /// third party by asking nicely.
    ///
    /// The address travels in the request because these two nodes run with no
    /// discovery service, which is what makes the suite hermetic. With
    /// discovery, `cx.peer()` alone is the whole address.
    async fn subscribe(&self, cx: &Context, back: String) -> u64 {
        let Some(addr) = parse_addr(&back) else {
            return 0;
        };
        if addr.id != cx.peer() {
            return 0;
        }
        let Ok(client) = ticker::Client::connect(&self.node, addr).await else {
            return 0;
        };
        let watch = client.watch();
        self.subscribers.insert(cx.peer(), client, watch);
        self.subscribers.len() as u64
    }

    async fn unsubscribe(&self, cx: &Context) -> u64 {
        self.subscribers.remove(cx.peer());
        self.subscribers.len() as u64
    }
}

/// The dialling side, which also serves. Records every push it is handed.
#[derive(Clone, Default)]
struct Ticker {
    seen: Arc<AtomicU64>,
    last: Arc<AtomicU64>,
}

impl ticker::Handler for Ticker {
    async fn tick(&self, _cx: &Context, price: u64) -> u64 {
        self.last.store(price, Ordering::SeqCst);
        self.seen.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// An address in the `<id>@<ip:port>` form both sides can parse, which is what
/// stands in for a discovery service here. The demo uses the same shape for its
/// offline mode, and for the same reason.
fn addr_of(node: &Node) -> String {
    let addr = node.endpoint().addr();
    let sockets: Vec<String> = addr.ip_addrs().map(|a| a.to_string()).collect();
    format!("{}@{}", addr.id, sockets.join(","))
}

/// The other half of `addr_of`. Returns `None` rather than erroring, because
/// every caller here treats an unusable address as "do not register" and there
/// is nothing else to do with the distinction.
fn parse_addr(spec: &str) -> Option<EndpointAddr> {
    let (id, sockets) = spec.split_once('@')?;
    let id: EndpointId = id.parse().ok()?;
    sockets
        .split(',')
        .filter(|s| !s.is_empty())
        .try_fold(EndpointAddr::from(id), |addr, socket| {
            Some(addr.with_ip_addr(socket.parse().ok()?))
        })
}

/// Everything a push needs: a desk that accepts, a peer that dials in and
/// serves the reverse contract, and the registry between them.
struct Pushing {
    subscribers: Subscribers<ticker::Client>,
    ticker: Ticker,
    client: desk::Client,
    _servers: [Server; 2],
    _nodes: [Node; 2],
}

async fn pushing(reverse_allows: Allowlist) -> Pushing {
    let desk_node = local_node().await;
    let peer_node = local_node().await;
    let subscribers = Subscribers::new();
    let ticker = Ticker::default();

    let desk_server = desk::serve(
        &desk_node,
        Desk {
            node: desk_node.clone(),
            subscribers: subscribers.clone(),
        },
        Allowlist::anyone(),
    )
    .expect("the desk serves");

    // The dialling side runs an accept loop of its own. That is the whole of
    // Design B: it is a server too, for a different contract.
    let peer_server =
        ticker::serve(&peer_node, ticker.clone(), reverse_allows).expect("the peer serves");

    let client = desk::Client::connect(&peer_node, desk_node.endpoint().addr())
        .await
        .expect("the peer dials the desk");

    Pushing {
        subscribers,
        ticker,
        client,
        _servers: [desk_server, peer_server],
        _nodes: [desk_node, peer_node],
    }
}

// ────────────────────────────────────────────────────────────────── the push

#[tokio::test]
async fn a_server_pushes_to_a_client_that_dialled_in() {
    let up = pushing(Allowlist::anyone()).await;
    let back = addr_of(&up._nodes[1]);

    assert_eq!(
        up.client.subscribe(back).await.expect("subscribes"),
        1,
        "the desk must have dialled back and remembered the peer"
    );

    for (peer, listener) in up.subscribers.snapshot() {
        assert_eq!(
            listener.tick(4471).await.expect("the push lands"),
            1,
            "pushing to {peer}"
        );
    }

    assert_eq!(up.ticker.seen.load(Ordering::SeqCst), 1);
    assert_eq!(up.ticker.last.load(Ordering::SeqCst), 4471);
}

/// The reverse direction is an ordinary service, so it has an ordinary
/// allowlist — and a desk that is not on it cannot push, however many peers
/// dialled in to it.
///
/// This is the property that makes Design B worth the second connection. A
/// reverse channel bolted onto the forward one would inherit the forward
/// direction's admission, so admitting a peer to *ask* would admit it to
/// *tell*, which are not the same decision.
///
/// The refusal lands on the **call**, not on the dial. keylink admits a peer
/// once per connection and closes the connection when it says no, and a QUIC
/// handshake completes before any of that: `connect` is answered by the
/// transport, and the allowlist is answered by keylink one layer up. So a desk
/// that is not welcome gets a handle and then gets nowhere with it, which is
/// exactly what `protocol.rs::a_peer_outside_the_allowlist_is_refused` records
/// for the forward direction.
#[tokio::test]
async fn an_unauthorised_reverse_call_is_refused() {
    let stranger = local_node().await;
    let up = pushing(Allowlist::only([stranger.id()])).await;
    let back = addr_of(&up._nodes[1]);

    assert_eq!(up.client.subscribe(back).await.expect("answers"), 1);

    for (_peer, listener) in up.subscribers.snapshot() {
        let refused = listener
            .tick(4471)
            .await
            .expect_err("the peer's own allowlist does not name this desk");
        assert!(
            matches!(refused, keylink::Error::Transport(_)),
            "got {refused:?}"
        );
    }
    assert_eq!(
        up.ticker.seen.load(Ordering::SeqCst),
        0,
        "the push must not have reached the handler"
    );
}

/// An address is a claim and an identity is a proof, and the registry is keyed
/// on the proof. A peer that names somebody else's address is refused rather
/// than turned into a way of making the desk dial a third party.
#[tokio::test]
async fn a_peer_cannot_register_someone_elses_address() {
    let up = pushing(Allowlist::anyone()).await;
    let elsewhere = local_node().await;

    assert_eq!(
        up.client
            .subscribe(addr_of(&elsewhere))
            .await
            .expect("answers"),
        0,
        "an address whose id is not the proven peer must be refused"
    );
    assert!(up.subscribers.is_empty());
}

/// The registry evicts on [`Departure`], not on a poll, and not on
/// `Connection::closed` — which borrows a strong handle and would keep every
/// connection the desk had ever seen alive. `watch.rs` calls that a leak wearing
/// a diagnostic's clothes; this is the same argument one layer up.
#[tokio::test]
async fn a_subscriber_is_dropped_when_its_connection_ends() {
    let desk_node = local_node().await;
    let subscribers: Subscribers<ticker::Client> = Subscribers::new();
    let _desk = desk::serve(
        &desk_node,
        Desk {
            node: desk_node.clone(),
            subscribers: subscribers.clone(),
        },
        Allowlist::anyone(),
    )
    .expect("the desk serves");

    {
        let peer_node = local_node().await;
        let peer = ticker::serve(&peer_node, Ticker::default(), Allowlist::anyone())
            .expect("the peer serves");
        let client = desk::Client::connect(&peer_node, desk_node.endpoint().addr())
            .await
            .expect("dials");

        assert_eq!(
            client
                .subscribe(addr_of(&peer_node))
                .await
                .expect("subscribes"),
            1
        );
        assert_eq!(subscribers.len(), 1);

        // Shut the peer's accept loop down rather than merely dropping it. A
        // dropped endpoint is a peer that stopped answering, and silence takes
        // the idle timeout to become an event — see `Departure::Lost`. A
        // shutdown is a goodbye, and a goodbye arrives at once.
        peer.shutdown().await.expect("stops");
        client.close().await.expect("closes");
    }

    for _ in 0..2_400 {
        if subscribers.is_empty() {
            return;
        }
        n0_future::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("a subscriber outlived its connection");
}

/// The wire did not move. Wave C adds a second service, not a second kind of
/// message, so the hashed contract of an existing one is untouched — which is
/// what `protocol.rs::the_contract_hash_has_not_moved` pins for the fleet.
#[tokio::test]
async fn a_peer_can_unsubscribe_without_leaving() {
    let up = pushing(Allowlist::anyone()).await;
    let back = addr_of(&up._nodes[1]);

    assert_eq!(up.client.subscribe(back).await.expect("subscribes"), 1);
    assert_eq!(up.client.unsubscribe().await.expect("unsubscribes"), 0);
    assert!(up.subscribers.is_empty());
}

/// A peer that re-subscribes must not be evicted by its own previous
/// connection.
///
/// The registry is keyed by [`EndpointId`], so a second subscribe from one peer
/// **replaces** the first — which drops the first reverse client, closes its
/// connection, and fires the `Watch` that the first subscribe registered. That
/// task then wakes with the peer's id in hand and, if it removes by id alone,
/// deletes the entry that replaced it.
///
/// The result is a peer that believes it is subscribed and a desk that has
/// forgotten it: pushes stop, and nothing reports a fault. In the admission
/// adapter, which instantiates this same registry at `Revocation`, the same race
/// is worse than a lost push — the surviving connection leaves the registry, so
/// a later `replace` can neither see nor close it, and a revoked peer keeps a
/// live connection. That is an authorization bypass reached through a
/// bookkeeping race.
///
/// So each entry is stamped and an eviction task removes only its own stamp.
/// Made red by removing the stamp comparison in `Subscribers::insert`.
#[tokio::test]
async fn a_resubscribing_peer_is_not_evicted_by_its_own_old_connection() {
    let up = pushing(Allowlist::anyone()).await;
    let back = addr_of(&up._nodes[1]);

    assert_eq!(
        up.client.subscribe(back.clone()).await.expect("subscribes"),
        1
    );
    // The second subscribe replaces the first, dropping the first reverse
    // client and closing its connection — which is what arms the race.
    assert_eq!(up.client.subscribe(back).await.expect("re-subscribes"), 1);

    // Long enough for the first connection's `Watch` to fire and its eviction
    // task to run. Not a timing assertion: the test fails if the entry goes,
    // and waiting longer can only make a wrong implementation more wrong.
    n0_future::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        up.subscribers.len(),
        1,
        "the live subscription must survive its predecessor's departure"
    );
    let listener = up
        .subscribers
        .get(up._nodes[1].id())
        .expect("the peer is still registered");
    assert_eq!(
        listener.tick(4471).await.expect("the push still lands"),
        1,
        "and the handle in the registry must be the live one"
    );
}
