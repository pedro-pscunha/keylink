//! Pushing to peers, which is the direction a request cannot travel.
//!
//! ```sh
//! cargo run -p keylink --example noticeboard
//! ```
//!
//! Three nodes on the loopback interface. `Node::bind_local` binds with no relay
//! and no discovery, so this runs offline.
//!
//! For replayable events without a callback service, see `event_feed.rs` and
//! `keylink::events`. This example deliberately keeps no offline history.
//!
//! # A push is two contracts, not one
//!
//! Every other example that binds a node is a caller asking a server —
//! `foundry.rs` binds none, so it is not one of them. A push is the server
//! calling the caller, and keylink has no back channel on a connection for it.
//! So a subscriber **serves** a contract of its own, the board dials it, and the
//! board keeps that client. [`Subscribers`] is where the kept clients live.
//!
//! ```text
//! Board  <- subscribe(address) -  Reader     the ordinary direction
//! Board  -  post(headline) ->     Reader     the push, on a second connection
//! ```
//!
//! # The address is a claim and the peer is a proof
//!
//! A subscriber sends the address to dial back on. That is something it *said*.
//! [`Context::peer`] is the key iroh proved at the handshake. Reconciling the
//! two is the whole of the admission this direction needs: an address naming
//! somebody else is refused, so a peer cannot make the board dial a third party
//! by asking politely.
//!
//! The address travels in the request only because these nodes run without
//! discovery. With discovery, `cx.peer()` is the whole address.
//!
//! # A departed subscriber removes itself
//!
//! [`Subscribers::insert`] takes a [`Watch`](keylink::Watch) beside the client,
//! so a peer that goes away is taken out of the set without anybody sweeping it.
//! Act 3 measures that: a reader is dropped and the board's count falls on its
//! own.

use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::prelude::*;
use keylink::{Allowlist, Context, EndpointAddr, EndpointId, Node, Server, Subscribers};

keylink::service! {
    service Board;

    /// Ask to be posted to, giving the address to dial back on.
    rpc Subscribe(back: String) -> u64;

    /// Stop being posted to.
    rpc Unsubscribe() -> u64;
}

keylink::service! {
    service Reader;

    /// One posted headline.
    ///
    /// An `rpc` and not a `tell`, so the board learns whether the reader took
    /// it. A `tell`'s `Ok` means the bytes reached the local transport and
    /// nothing more, which `firmware_depot.rs` measures.
    rpc Post(headline: String) -> u64;
}

/// The board. Holds its node so it can dial back, and the set it dials.
struct Board {
    node: Node,
    subscribers: Subscribers<reader::Client>,
}

impl board::Handler for Board {
    async fn subscribe(&self, cx: &Context, back: String) -> u64 {
        let Some(addr) = parse_addr(&back) else {
            return 0;
        };
        //  The one check that matters in this direction.
        if addr.id != cx.peer() {
            return 0;
        }
        let Ok(client) = reader::Client::connect(&self.node, addr).await else {
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

/// A reader. Dials the board, and serves the board's push contract back.
#[derive(Clone, Default)]
struct Reader {
    seen: Arc<AtomicU64>,
}

impl reader::Handler for Reader {
    async fn post(&self, _cx: &Context, _headline: String) -> u64 {
        self.seen.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// An address in the `<id>@<ip:port>` form, standing in for a discovery service.
fn addr_of(node: &Node) -> String {
    let addr = node.endpoint().addr();
    let sockets: Vec<String> = addr.ip_addrs().map(|socket| socket.to_string()).collect();
    format!("{}@{}", addr.id, sockets.join(","))
}

/// The other half of [`addr_of`].
fn parse_addr(spec: &str) -> Option<EndpointAddr> {
    let (id, sockets) = spec.split_once('@')?;
    let id: EndpointId = id.parse().ok()?;
    sockets
        .split(',')
        .filter(|socket| !socket.is_empty())
        .try_fold(EndpointAddr::from(id), |addr, socket| {
            Some(addr.with_ip_addr(socket.parse().ok()?))
        })
}

/// Wait for the board's set to reach `want`, by asking rather than by sleeping.
async fn holds(
    subscribers: &Subscribers<reader::Client>,
    want: usize,
) -> Result<(), Box<dyn StdError>> {
    for _ in 0..400 {
        if subscribers.len() == want {
            return Ok(());
        }
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("the board never held {want} subscriber(s)").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let board_node = Node::bind_local().await?;
    let first_node = Node::bind_local().await?;
    let second_node = Node::bind_local().await?;

    // ── 1. The board, and two readers that each serve the push contract.
    let subscribers: Subscribers<reader::Client> = Subscribers::new();
    let _board = Server::builder(&board_node)
        .service(
            board::service(
                Board {
                    node: board_node.clone(),
                    subscribers: subscribers.clone(),
                },
                Allowlist::anyone(),
            )
            .build(),
        )
        .spawn()?;

    let first = Reader::default();
    let second = Reader::default();
    let _first_server = Server::builder(&first_node)
        .service(reader::service(first.clone(), Allowlist::only([board_node.id()])).build())
        .spawn()?;
    let _second_server = Server::builder(&second_node)
        .service(reader::service(second.clone(), Allowlist::only([board_node.id()])).build())
        .spawn()?;

    let asking = board::Client::connect(&first_node, board_node.endpoint().addr()).await?;
    let also = board::Client::connect(&second_node, board_node.endpoint().addr()).await?;
    assert_eq!(asking.subscribe(addr_of(&first_node)).await?, 1);
    assert_eq!(also.subscribe(addr_of(&second_node)).await?, 2);
    println!("the board holds {} subscriber(s)", subscribers.len());

    //  An address naming somebody else is refused, so the board cannot be aimed
    //  at a third party.
    assert_eq!(
        asking.subscribe(addr_of(&second_node)).await?,
        0,
        "a peer must not register an address it did not prove"
    );

    // ── 2. The push itself: one headline, every subscriber, on the connections
    //       the board dialled.
    let mut delivered = 0;
    for (_, reader) in subscribers.snapshot() {
        delivered += u64::from(reader.post("the kiln is lit".to_string()).await.is_ok());
    }
    println!("one headline reached {delivered} reader(s)");
    assert_eq!(delivered, 2);
    assert_eq!(first.seen.load(Ordering::SeqCst), 1);
    assert_eq!(second.seen.load(Ordering::SeqCst), 1);

    // ── 3. A reader that leaves, and a set that notices without being swept.
    //
    //  Dropping the second reader's server and client ends its connection. The
    //  `Watch` handed to `insert` is what turns that into a removal, so nothing
    //  here polls and nothing accumulates a dead client.
    drop(also);
    drop(_second_server);
    holds(&subscribers, 1).await?;
    println!(
        "a reader left and the board holds {} without sweeping",
        subscribers.len()
    );

    // ── 4. And leaving on purpose is the ordinary direction again.
    assert_eq!(asking.unsubscribe().await?, 0);
    println!("the last reader unsubscribed; the board holds nothing");
    assert!(subscribers.is_empty());
    Ok(())
}
