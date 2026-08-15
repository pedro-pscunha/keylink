//! A peer that moved, and a handle that found it anyway.
//!
//! ```sh
//! cargo run -p keylink --example waypoint
//! ```
//!
//! Two nodes on the loopback interface. `Node::bind_local` binds with no relay
//! and no discovery, so this runs offline.
//!
//! # The problem, stated precisely
//!
//! An [`EndpointId`] is an identity and says nothing about where that identity
//! is reachable. Discovery is what turns one into the other, and a node bound
//! with `bind_local` has none — so every other example here hands the client a
//! **full** [`EndpointAddr`], identity plus routes, taken from the node itself.
//!
//! That works, and it pins the handle to the routes it was built with. When the
//! peer rebinds — a restart, a NAT rebinding, a laptop that changed network —
//! its identity is unchanged and its routes are not. A pinned handle then
//! re-dials an address nothing is listening on, forever.
//!
//! # What [`Dial`] moves
//!
//! [`Peer`] re-dials through [`Dial`] on **every** dial: the first one, the one
//! after [`Peer::disconnect`], and the automatic reconnect it performs when a
//! call proves the cached connection died. So a [`Dial`] impl that resolves the
//! address at dial time is consulted again at exactly the moment a peer's routes
//! are most likely to have changed.
//!
//! That is the whole subject. It is **not** "how do I address a peer without
//! discovery" — a full [`EndpointAddr`] answers that and needs no trait.
//!
//! # A caller may write this, and the generated one is not special
//!
//! Every `service!` client implements [`Dial`] already. That impl connects to
//! the address it is handed, which is the right default and is what pins the
//! control handle in act 4. Writing another one is ordinary trait
//! implementation — the trait is public and unsealed, [`LanKiln`] below is
//! twelve lines, and act 3 measures what those twelve lines buy.
//!
//! # The one constraint the signature imposes
//!
//! [`Dial::dial`] returns a future that must be [`Send`], so the returned future
//! may not hold a lock. [`route`] is deliberately **not** `async`: it takes the
//! directory's lock, reads, and drops the guard before the dial below awaits.
//! Locking inside the async block instead makes the future `!Send` and the impl
//! stops compiling.
//!
//! # The control needs its own node, and finding that out was the point
//!
//! Act 4 compares two handles. Built on **one** node, the comparison is void: a
//! successful dial teaches that endpoint where an identity currently answers,
//! and the address a later dial passes is a *hint* merged into what the endpoint
//! already knows. So the pinned handle read 1183 through the route the directory
//! handle had just discovered, and did it one line after the assertion that says
//! it cannot.
//!
//! Two nodes, two address books, and the only shared thing is the kiln. Worth
//! stating in its own right: **an address hint is not a leash**. A handle whose
//! peer another handle already reached is not proof that the address it holds
//! is any good.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use keylink::{Allowlist, Context, Dial, EndpointAddr, EndpointId, Node, Peer, SecretKey};

keylink::service! {
    service Kiln;

    /// What the kiln is doing right now.
    rpc Temperature() -> u32;
}

struct Sensor;

impl kiln::Handler for Sensor {
    async fn temperature(&self, _cx: &Context) -> u32 {
        1183
    }
}

// ── the directory ───────────────────────────────────────────────────────────

/// What a discovery service would otherwise answer: which routes a known
/// identity is currently behind.
///
/// On a private network this is a config file, a DHCP lease table, an mDNS
/// sweep, or a line in etcd — something the operator already runs. A
/// `HashMap` stands in for all of them.
#[derive(Default)]
struct Directory {
    names: HashMap<String, EndpointId>,
    routes: HashMap<EndpointId, Vec<SocketAddr>>,
}

fn directory() -> &'static Mutex<Directory> {
    static DIRECTORY: OnceLock<Mutex<Directory>> = OnceLock::new();
    DIRECTORY.get_or_init(|| Mutex::new(Directory::default()))
}

/// Teach the directory where a name lives now.
fn register(name: &str, id: EndpointId, routes: Vec<SocketAddr>) {
    let mut dir = directory().lock().unwrap_or_else(|e| e.into_inner());
    dir.names.insert(name.to_owned(), id);
    dir.routes.insert(id, routes);
}

/// Name a peer without saying where it is.
///
/// The returned address carries an identity and **no routes**. Handing this to
/// a generated client would fail: there is nothing to dial. It is the
/// [`Dial`] impl below that makes it enough.
fn named(name: &str) -> Option<EndpointAddr> {
    let dir = directory().lock().unwrap_or_else(|e| e.into_inner());
    dir.names.get(name).copied().map(EndpointAddr::from)
}

/// Attach whatever routes the directory holds for this identity, right now.
///
/// Not `async`, and that is the constraint the module docs name: the guard is
/// dropped when this returns, so the dial below awaits holding no lock.
fn route(addr: EndpointAddr) -> EndpointAddr {
    let dir = directory().lock().unwrap_or_else(|e| e.into_inner());
    match dir.routes.get(&addr.id) {
        Some(sockets) => sockets
            .iter()
            .fold(addr, |addr, socket| addr.with_ip_addr(*socket)),
        None => addr,
    }
}

// ── the custom dial ─────────────────────────────────────────────────────────

/// Counts dials, so "reused the connection" and "dialled again" are
/// distinguishable from outside.
static DIALS: AtomicUsize = AtomicUsize::new(0);

/// A `kiln::Client` that resolves its route through the directory.
///
/// `Peer<kiln::Client>` dials the address it was built with. `Peer<LanKiln>`
/// dials the address the directory holds **at the moment of the dial**.
struct LanKiln(kiln::Client);

impl Dial for LanKiln {
    async fn dial(node: &Node, addr: EndpointAddr) -> keylink::Result<Self> {
        let routed = route(addr);
        let nth = DIALS.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "    [dial #{nth}] directory answered {} route(s) for {}",
            routed.ip_addrs().count(),
            short(&routed.id)
        );
        Ok(Self(kiln::Client::connect(node, routed).await?))
    }
}

/// Enough of an identity to see that it did not change.
fn short(id: &EndpointId) -> String {
    id.to_string().chars().take(8).collect()
}

fn sockets(node: &Node) -> Vec<SocketAddr> {
    node.endpoint().addr().ip_addrs().copied().collect()
}

/// One temperature reading, or whatever went wrong, bounded in time.
///
/// A handle that cannot reach its peer does not fail quickly — it retries the
/// route it has until the dial times out. So the control in act 4 is bounded
/// here rather than awaited, and the bound is the measurement.
async fn read(peer: &Peer<LanKiln>, patience: Duration) -> Result<u32, String> {
    match tokio::time::timeout(
        patience,
        peer.call(|c| async move { c.0.temperature().await }),
    )
    .await
    {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!("no answer within {patience:?}")),
    }
}

/// The same, for a handle using the generated `Dial`.
async fn read_pinned(peer: &Peer<kiln::Client>, patience: Duration) -> Result<u32, String> {
    match tokio::time::timeout(
        patience,
        peer.call(|c| async move { c.temperature().await }),
    )
    .await
    {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!("no answer within {patience:?}")),
    }
}

/// The kiln's identity, fixed so that rebinding produces the *same* peer at a
/// *different* address. A fresh keypair would be a different peer, and the
/// example would be demonstrating nothing.
const KILN: [u8; 32] = [77; 32];

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let floor = Node::bind_local().await?;
    //  A second caller, on its own endpoint and therefore its own address book.
    //  Sharing `floor` would let this handle inherit what the other one learns,
    //  which is exactly the mistake the module docs describe.
    let control_floor = Node::bind_local().await?;

    // ── 1. The kiln comes up, and the directory records where it is.
    let kiln_node = Node::bind_local_with_secret(SecretKey::from_bytes(&KILN)).await?;
    let serving = kiln::serve(&kiln_node, Sensor, Allowlist::anyone())?;
    let first_home = sockets(&kiln_node);
    let identity = kiln_node.id();
    register("kiln-3", identity, first_home.clone());
    println!("kiln {} is at {first_home:?}", short(&identity));

    //  The caller names a peer. It never sees an address, and the address it
    //  hands `Peer` carries no routes at all.
    let by_name: Peer<LanKiln> = Peer::new(&floor, named("kiln-3").ok_or("kiln-3 is registered")?);
    assert_eq!(
        by_name.addr().ip_addrs().count(),
        0,
        "the handle holds an identity and no route"
    );

    //  The control handle, built the way every other example builds one: with
    //  the full address, resolved once, now. On its own node, so that what this
    //  handle knows is only what this handle was told.
    let pinned: Peer<kiln::Client> = Peer::new(&control_floor, kiln_node.endpoint().addr());

    println!("\ncall 1 — both handles, kiln at home:");
    assert_eq!(read(&by_name, Duration::from_secs(10)).await?, 1183);
    assert_eq!(read_pinned(&pinned, Duration::from_secs(10)).await?, 1183);
    println!("    both read 1183");

    // ── 2. A second call reuses the connection rather than dialling again.
    //
    //  Worth measuring because act 3's claim is about a *re-dial*, and a
    //  re-dial is only interesting if the ordinary path does not do one.
    let before = DIALS.load(Ordering::SeqCst);
    assert_eq!(read(&by_name, Duration::from_secs(10)).await?, 1183);
    assert_eq!(
        DIALS.load(Ordering::SeqCst),
        before,
        "a live connection must be reused, not re-dialled"
    );
    println!("\ncall 2 — no new dial: the cached connection was reused");

    // ── 3. The kiln moves. Same identity, new routes, nobody rebuilds a handle.
    //
    //  `close` sends the peers a connection-close frame, so both handles learn
    //  promptly rather than waiting out an idle timeout. Then the same secret
    //  binds again and the operating system hands out a different port.
    drop(serving);
    kiln_node.endpoint().close().await;
    drop(kiln_node);

    //  Rebinding can land on the port that was just released, which would make
    //  the rest of this prove nothing. Insist that the address really changed.
    let mut moved = Node::bind_local_with_secret(SecretKey::from_bytes(&KILN)).await?;
    for _ in 0..16 {
        if sockets(&moved) != first_home {
            break;
        }
        moved = Node::bind_local_with_secret(SecretKey::from_bytes(&KILN)).await?;
    }
    let new_home = sockets(&moved);
    assert_ne!(new_home, first_home, "the kiln has to actually move");
    assert_eq!(moved.id(), identity, "and it has to stay the same kiln");
    let _serving = kiln::serve(&moved, Sensor, Allowlist::anyone())?;
    register("kiln-3", identity, new_home.clone());
    println!("\nthe kiln rebound: {first_home:?} -> {new_home:?}");
    println!("identity is unchanged: {}", short(&identity));

    // ── 4. The measurement. Two handles, one moment, one difference.
    //
    //  `by_name` re-dials through the directory and finds the kiln. `pinned`
    //  re-dials the address it was built with, which is now nothing. Neither
    //  handle was touched between act 1 and here, and the kiln is provably up —
    //  the first handle just talked to it.
    println!("\ncall 3 — after the move:");
    let recovered = read(&by_name, Duration::from_secs(10)).await?;
    println!("    by name: {recovered} degrees");
    assert_eq!(recovered, 1183);

    let control = read_pinned(&pinned, Duration::from_secs(5)).await;
    match &control {
        Err(reason) => println!("    pinned: {reason}"),
        Ok(value) => println!("    pinned: UNEXPECTED {value}"),
    }
    assert!(
        control.is_err(),
        "a handle pinned to the old route cannot reach the new one"
    );

    println!(
        "\n{} dial(s) in total; the directory was asked on every one",
        DIALS.load(Ordering::SeqCst)
    );
    Ok(())
}
