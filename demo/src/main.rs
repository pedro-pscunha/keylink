//! An inventory service across two machines.
//!
//! ```text
//! cargo run -p demo -- serve
//! cargo run -p demo -- connect <endpoint-id> [file-to-upload]
//! ```
//!
//! Note what is NOT in this file: no IP address, no port, no framing, no
//! serialization call, no hand-written ALPN string. The domain types and
//! the two `service!` blocks are the entire protocol definition. A file
//! transfer is `Body::from_file(path)` on one side and `copy_to(&mut file)` on
//! the other.

use std::collections::BTreeMap;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{bail, Context as _, Result};
use keylink::{
    Allowlist, BlockingWire, Body, BodyReader, Caller, EndpointAddr, EndpointId, Node, SecretKey,
    Servable, Server, Wire,
};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};

// ─────────────────────────────────────────────── domain types (nested, typed)

/// Two unit variants with identical shapes. postcard encodes them by
/// declaration index, so swapping them decodes `Brl` as `Usd` without an error:
/// every price silently changes currency. The `swap-variants` feature does
/// exactly that, and the schema-derived ALPN turns it into a refused handshake.
#[cfg(not(feature = "swap-variants"))]
#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub enum Currency {
    Brl,
    Usd,
}

#[cfg(feature = "swap-variants")]
#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub enum Currency {
    Usd,
    Brl,
}

#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub struct Money {
    pub cents: u64,
    pub currency: Currency,
}

#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub enum ReserveOutcome {
    Reserved { remaining: u32 },
    Insufficient { requested: u32, available: u32 },
    NoSuchItem,
}

/// What the caller learns about a datasheet before a byte of it arrives.
#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub struct Datasheet {
    pub sku: String,
    pub bytes: u64,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Schema)]
pub enum UploadOutcome {
    Stored { bytes: u64, checksum: u64 },
    Rejected { reason: String },
}

// ─────────────────────────────────────────────────────── the whole protocol

/// Frozen version 2, always built so the pin names its actual derived hash.
mod frozen {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize, Schema)]
    pub struct Item {
        pub sku: String,
        pub name: String,
        pub price: Money,
        pub stock: u32,
        pub tags: Vec<String>,
    }

    keylink::service! {
        service Inventory;
        version 2;

        rpc  Ping() -> String;
        rpc  List() -> Vec<Item>;
        rpc  GetItem(sku: String) -> Option<Item>;
        rpc  Reserve(sku: String, qty: u32) -> ReserveOutcome;

        recv Fetch(sku: String, kib: u32) -> Datasheet;
    }
}
#[cfg(feature = "previous-contract")]
use frozen as current;
#[cfg(not(feature = "previous-contract"))]
mod current {
    use super::*;
    #[derive(Debug, Clone, Serialize, Deserialize, Schema)]
    pub struct Item {
        pub sku: String,
        pub name: String,
        pub price: Money,
        pub stock: u32,
        pub tags: Vec<String>,
        pub aisle: Option<u16>,
    }

    keylink::service! {
        service Inventory;
        version 3;

        rpc  Ping() -> String;
        rpc  List() -> Vec<Item>;
        rpc  GetItem(sku: String) -> Option<Item>;
        rpc  Reserve(sku: String, qty: u32) -> ReserveOutcome;

        recv Fetch(sku: String, kib: u32) -> Datasheet;
        rpc Count() -> u64;
        previous 2 {
            hash crate::frozen::inventory::VERSION.hash();
            rpc Ping() -> String;
            changed rpc List() -> Vec<crate::frozen::Item>;
            changed rpc GetItem(sku: String) -> Option<crate::frozen::Item>;
            rpc Reserve(sku: String, qty: u32) -> ReserveOutcome;
            recv Fetch(sku: String, kib: u32) -> Datasheet;
        }
    }

    impl From<frozen::Item> for Item {
        fn from(old: frozen::Item) -> Self {
            Self {
                sku: old.sku,
                name: old.name,
                price: old.price,
                stock: old.stock,
                tags: old.tags,
                aisle: None,
            }
        }
    }
    impl From<Item> for frozen::Item {
        fn from(new: Item) -> Self {
            Self {
                sku: new.sku,
                name: new.name,
                price: new.price,
                stock: new.stock,
                tags: new.tags,
            }
        }
    }
    impl inventory::version_2::Convert for inventory::version_2::Step {
        fn upgrade_request(
            old: inventory::version_2::Request,
        ) -> std::result::Result<inventory::Request, keylink::Unsupported> {
            use inventory::{version_2::Request as Old, Request as New};
            match old {
                Old::List {} => Ok(New::List {}),
                Old::GetItem { sku } => Ok(New::GetItem { sku }),
                _ => Err(keylink::Unsupported::new(
                    "Inventory",
                    "carried by generated arms",
                )),
            }
        }
        fn downgrade_request(
            new: inventory::Request,
        ) -> std::result::Result<inventory::version_2::Request, keylink::Unsupported> {
            use inventory::{version_2::Request as Old, Request as New};
            match new {
                New::List {} => Ok(Old::List {}),
                New::GetItem { sku } => Ok(Old::GetItem { sku }),
                New::Count {} => Err(keylink::Unsupported::new(
                    "Count",
                    "version 2 has no Count method",
                )),
                _ => Err(keylink::Unsupported::new(
                    "Inventory",
                    "carried by generated arms",
                )),
            }
        }
        fn upgrade_response(
            old: inventory::version_2::Response,
        ) -> std::result::Result<inventory::Response, keylink::Unsupported> {
            use inventory::{version_2::Response as Old, Response as New};
            match old {
                Old::List(items) => Ok(New::List(items.into_iter().map(Into::into).collect())),
                Old::GetItem(item) => Ok(New::GetItem(item.map(Into::into))),
                _ => Err(keylink::Unsupported::new(
                    "Inventory",
                    "carried by generated arms",
                )),
            }
        }
        fn downgrade_response(
            new: inventory::Response,
        ) -> std::result::Result<inventory::version_2::Response, keylink::Unsupported> {
            use inventory::{version_2::Response as Old, Response as New};
            match new {
                New::List(items) => Ok(Old::List(items.into_iter().map(Into::into).collect())),
                New::GetItem(item) => Ok(Old::GetItem(item.map(Into::into))),
                _ => Err(keylink::Unsupported::new(
                    "Inventory",
                    "version 2 has no such reply",
                )),
            }
        }
    }
}
use current::{inventory, Item};

// `Upload` used to be a sixth method on `Inventory`. It cannot be one now that
// the warehouse is an actor: `Handle::service` refuses to compile for a service
// declaring a `send`, because that handler reads the upload INSIDE the serial
// turn (RFC-04 §10.4). A peer that declared a large body and then stopped
// writing would hold the mailbox until iroh's idle timeout, and no layer can
// take it back — a `Timeout` refuses the caller without ending the turn.
//
// So bulk transfer goes beside the actor instead of through it: its own
// service, its own ALPN, its own `&self` handler. Both are registered on one
// node in `serve`, and a stalled upload now costs the peer that stalled it.
keylink::service! {
    service Dock;

    send Upload(name: String) -> UploadOutcome;
}

// ────────────────────────────────────────────────────── content integrity

/// FNV-1a. Not a security hash — a cheap end-to-end check that the bytes that
/// left are the bytes that arrived, computed incrementally so neither side ever
/// holds the payload.
#[derive(Debug, Clone, Copy)]
pub struct Fnv1a(u64);

impl Fnv1a {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    fn new() -> Self {
        Self(Self::OFFSET)
    }

    fn update(&mut self, bytes: &[u8]) {
        self.0 = bytes
            .iter()
            .fold(self.0, |h, b| (h ^ u64::from(*b)).wrapping_mul(Self::PRIME));
    }

    fn finish(self) -> u64 {
        self.0
    }
}

/// A deterministic byte stream of a known length, generated as it is read.
///
/// The point of the streaming path is that a payload never exists in full, so
/// the demo's payload never exists in full either.
struct Pattern {
    offset: u64,
    len: u64,
}

impl Pattern {
    /// The byte at absolute offset `i`. Non-repeating over any 251-byte window,
    /// so a chunking bug shows up in the checksum instead of cancelling out.
    fn byte_at(offset: u64) -> u8 {
        (offset % 251) as u8
    }

    fn checksum(len: u64) -> u64 {
        let mut hash = Fnv1a::new();
        for offset in 0..len {
            hash.update(&[Self::byte_at(offset)]);
        }
        hash.finish()
    }
}

impl AsyncRead for Pattern {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let remaining = self.len - self.offset;
        let take = (buf.remaining() as u64).min(remaining) as usize;
        for index in 0..take {
            buf.put_slice(&[Self::byte_at(self.offset + index as u64)]);
        }
        self.offset += take as u64;
        Poll::Ready(Ok(()))
    }
}

// ────────────────────────────────────────────────────────────── accept side

#[derive(Default)]
struct Warehouse {
    items: BTreeMap<String, Item>,
}

impl Warehouse {
    fn seeded() -> Self {
        let mut items = BTreeMap::new();
        for (sku, name, cents, stock, tags) in [
            (
                "MUD-001",
                "Muda de Ipê Amarelo",
                4500u64,
                12u32,
                vec!["arvore", "nativa"],
            ),
            ("ADU-014", "Adubo Orgânico 20kg", 8900, 3, vec!["insumo"]),
            ("GRA-esm", "Grama Esmeralda m²", 1250, 400, vec!["grama"]),
        ] {
            items.insert(
                sku.to_string(),
                Item {
                    sku: sku.to_string(),
                    name: name.to_string(),
                    price: Money {
                        cents,
                        currency: Currency::Brl,
                    },
                    stock,
                    tags: tags.into_iter().map(String::from).collect(),
                    #[cfg(not(feature = "previous-contract"))]
                    aisle: Some(3),
                },
            );
        }
        Self { items }
    }
}

impl inventory::Actor for Warehouse {
    #[cfg(not(feature = "previous-contract"))]
    async fn count(&mut self, _cx: Caller<'_>) -> u64 {
        self.items.len() as u64
    }

    async fn ping(&mut self, _cx: Caller<'_>) -> String {
        format!("ok from {}", hostname())
    }

    async fn list(&mut self, _cx: Caller<'_>) -> Vec<Item> {
        self.items.values().cloned().collect()
    }

    async fn get_item(&mut self, _cx: Caller<'_>, sku: String) -> Option<Item> {
        self.items.get(&sku).cloned()
    }

    /// All-or-nothing. A partial fill would need a variant carrying how many we
    /// actually got, and a caller that always checks it — easy to get wrong at
    /// the call site, so the stricter contract is the safer default here.
    async fn reserve(&mut self, _cx: Caller<'_>, sku: String, qty: u32) -> ReserveOutcome {
        let Some(item) = self.items.get_mut(&sku) else {
            return ReserveOutcome::NoSuchItem;
        };
        if item.stock < qty {
            return ReserveOutcome::Insufficient {
                requested: qty,
                available: item.stock,
            };
        }
        item.stock -= qty;
        ReserveOutcome::Reserved {
            remaining: item.stock,
        }
    }

    /// Hand back a header and a payload. keylink streams the payload; this
    /// function returns immediately and never sees a byte of it.
    async fn fetch(&mut self, _cx: Caller<'_>, sku: String, kib: u32) -> (Datasheet, Body) {
        let len = u64::from(kib) * 1024;
        let sheet = Datasheet {
            sku,
            bytes: len,
            checksum: Pattern::checksum(len),
        };
        let body = Body::from_reader(Pattern { offset: 0, len }, Some(len));
        (sheet, body)
    }
}

/// The other half of the split, where the uploads land.
///
/// It holds nothing, and that is the argument for where `store` went with it:
/// the function never read or wrote `items`, it opens a file and hashes what
/// arrives. Leaving it on `Warehouse` would have kept the one piece of work
/// that must NOT hold the mailbox on the type whose whole job is to hold it.
struct Dock;

impl Dock {
    /// Land an upload on disk, hashing as it goes. Never holds the payload.
    async fn store(name: &str, mut body: BodyReader) -> Result<(u64, u64)> {
        let path = std::env::temp_dir().join(format!("keylink-upload-{name}"));
        let mut file = tokio::fs::File::create(&path).await?;
        let mut hash = Fnv1a::new();
        let mut bytes = 0u64;
        while let Some(chunk) = body.next_chunk().await? {
            hash.update(&chunk);
            bytes += chunk.len() as u64;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        Ok((bytes, hash.finish()))
    }
}

/// `&self`, not `&mut self`: two uploads run at once, and a peer that stops
/// writing halfway waits alone. `keylink::Context` is spelled out because
/// `std::task::Context` is already in scope for `Pattern`.
impl dock::Handler for Dock {
    /// The declared length lets us refuse an oversized upload before it moves.
    async fn upload(
        &self,
        _cx: &keylink::Context,
        name: String,
        body: BodyReader,
    ) -> UploadOutcome {
        const LIMIT: u64 = 64 * 1024 * 1024;

        if body.declared_len().is_some_and(|len| len > LIMIT) {
            return UploadOutcome::Rejected {
                reason: format!("over the {LIMIT} byte limit"),
            };
        }
        match Dock::store(&name, body).await {
            Ok((bytes, checksum)) => UploadOutcome::Stored { bytes, checksum },
            Err(error) => UploadOutcome::Rejected {
                reason: error.to_string(),
            },
        }
    }
}

// ──────────────────────────────────────────────────────────────────── main

/// Deliberately NOT `#[tokio::main]`.
///
/// `connect-sync` uses the blocking API, which refuses to run inside a runtime
/// — correctly, since blocking there would deadlock. A synchronous `main` is
/// what a program using that API actually looks like.
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let arg = |n: usize| args.get(n).map(String::as_str);

    match arg(1) {
        Some("connect-sync") => {
            let id = arg(2).context("usage: connect-sync <endpoint-id>")?;
            connect_sync(id)
        }
        Some(other) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async move {
                match other {
                    "serve" => serve().await,
                    "id" => print_id().await,
                    "connect" => {
                        let id = arg(2).context("usage: connect <endpoint-id>")?;
                        connect(id, arg(3)).await
                    }
                    _ => bail!("unknown command: {other}"),
                }
            })
        }
        None => {
            bail!("usage: demo id | demo serve | demo connect <id> [file] | demo connect-sync <id>")
        }
    }
}

/// The same conversation, from a program with no runtime of its own.
fn connect_sync(id: &str) -> Result<()> {
    let peer: EndpointId = id.parse().context("bad endpoint id")?;
    let node = keylink::blocking::Node::bind_from_env("KEYLINK_SECRET")?;

    println!("dialing as: {} (blocking)", node.id());
    let client = inventory::blocking::Client::connect(&node, peer)?;

    println!("ping        -> {}", client.ping()?);
    println!("list        -> {} items", client.list()?.len());
    println!(
        "get_item    -> {:?}",
        client.get_item("ADU-014".into())?.map(|i| i.name)
    );
    match client.reserve("MUD-001".into(), 1) {
        Ok(outcome) => println!("reserve 1   -> {outcome:?}"),
        Err(err) => println!("reserve 1   -> REFUSED ({err})"),
    }
    client.close()?;
    Ok(())
}

/// Print the endpoint id for `KEYLINK_SECRET`. This is the address peers dial
/// and the value they put in their allowlist, so it has to be knowable before
/// anything is running.
async fn print_id() -> Result<()> {
    println!("{}", Node::bind_from_env("KEYLINK_SECRET").await?.id());
    Ok(())
}

/// Is this process running without the public internet?
///
/// `presets::N0` publishes to pkarr and resolves ids through n0's DNS, and
/// `Endpoint::online()` waits for a home relay to report connected. Both need
/// the internet, which is right for the showcase and impossible for a test on
/// loopback.
///
/// Set `KEYLINK_OFFLINE=1` and the demo binds `presets::Minimal`, skips
/// `online()`, and accepts a peer written `<id>@<ip:port>`. Unset — the normal
/// path, and the one the README documents — nothing below changes.
fn offline() -> bool {
    std::env::var_os("KEYLINK_OFFLINE").is_some()
}

/// Bind with no discovery, honouring `KEYLINK_SECRET` exactly as the online
/// path does.
///
/// The secret matters MORE here than online: with no discovery a peer is
/// allowlisted by an id somebody knew in advance, and a fresh random identity
/// every run cannot be.
async fn offline_node() -> Result<Node> {
    match std::env::var("KEYLINK_SECRET") {
        Ok(hex) => {
            let secret: SecretKey = hex.trim().parse().context("bad KEYLINK_SECRET")?;
            Ok(Node::bind_local_with_secret(secret).await?)
        }
        Err(_) => Ok(Node::bind_local().await?),
    }
}

/// Parse `<id>` or `<id>@<ip:port>[,<ip:port>]` into something dialable.
///
/// The bare id is the point of this demo, and it stays the default. The second
/// form is the door keylink already documents on `RpcClient::connect` — "an
/// `EndpointAddr` is accepted too, for a LAN or air-gapped network where
/// discovery is not available" — which this file had never used. Loopback is
/// exactly that case: there is no discovery to resolve an id against.
///
/// So the "no IP address appears in your code" claim at the top of this file
/// still holds for every path a reader follows. This is what the documented
/// escape hatch looks like at a call site.
fn dial_target(spec: &str) -> Result<EndpointAddr> {
    let Some((id, addrs)) = spec.split_once('@') else {
        return Ok(EndpointAddr::from(
            spec.parse::<EndpointId>().context("bad endpoint id")?,
        ));
    };
    let id: EndpointId = id.parse().context("bad endpoint id")?;
    // `with_ip_addr` rather than `from_parts`, which wants iroh's `TransportAddr`
    // — a type keylink does not re-export, because a caller has no reason to
    // name it.
    addrs
        .split(',')
        .try_fold(EndpointAddr::from(id), |addr, socket| {
            Ok(addr.with_ip_addr(socket.parse().context("bad socket address")?))
        })
}

async fn serve() -> Result<()> {
    let node = if offline() {
        offline_node().await?
    } else {
        let node = Node::bind_from_env("KEYLINK_SECRET").await?;
        node.online().await;
        node
    };

    // Fails at startup when unset, rather than bouncing every peer at runtime.
    let allow = Allowlist::from_env("KEYLINK_ALLOWED_IDS")?;

    println!(
        "alpn:         {}",
        String::from_utf8_lossy(inventory::alpn())
    );
    println!("upload alpn:  {}", String::from_utf8_lossy(dock::alpn()));
    println!("listening as: {}", node.id());

    // An actor, not a concurrent handler: one task owns the map, messages run
    // one at a time against `&mut self`, and nothing is locked across an await.
    // None of that reaches the wire. `singleton.rs` holds both serving models to
    // one ALPN, and `drift.rs` dials this binary with the ordinary
    // `inventory::Client` to show it end to end.
    let warehouse = inventory::actor(Warehouse::seeded());

    // Two services, two ALPNs, one node — which is why the `warehouse.serve`
    // shortcut cannot be used here: it builds a `Server` around one service and
    // nothing else can join it. The allowlist is the same on both, so a peer
    // that may reserve stock may also upload.
    let server = Server::builder(&node)
        .service(warehouse.service(allow.clone()).build())
        .service(dock::service(Dock, allow).build())
        .spawn()?;

    // AFTER `spawn`, never before. `spawn` is what registers the ALPNs, so a
    // peer that read this line any earlier could dial an endpoint answering
    // nothing and be refused for a reason that has nothing to do with the reason
    // it was dialling. `keylink/tests/drift.rs` reads this line and dials the
    // moment it arrives, so the ordering here is that test's starting gun.
    if offline() {
        // The form the other side can actually dial with no discovery.
        let addr = node.endpoint().addr();
        let sockets: Vec<String> = addr.ip_addrs().map(|a| a.to_string()).collect();
        println!("dial:         {}@{}", node.id(), sockets.join(","));
    }

    tokio::signal::ctrl_c().await?;
    server.shutdown().await?;
    Ok(())
}

async fn connect(id: &str, upload: Option<&str>) -> Result<()> {
    // First, and above everything that can fail. `inventory::alpn()` is a pure
    // function of the type set: it needs neither the peer nor a bound node, so
    // nothing here is waiting on it. `keylink/tests/drift.rs` reads this line to
    // learn what contract a binary derives, and it is the only way to ask a
    // binary that from outside.
    //
    // Under it, that read is a race rather than a question. A transient failure
    // in `dial_target` or in the bind below exits 1 with the line never printed,
    // and the test can only observe that as EOF — no line, no reason, and a
    // discarded stderr. That is what it did, on an otherwise green gate.
    println!("alpn:      {}", String::from_utf8_lossy(inventory::alpn()));

    let peer = dial_target(id)?;
    let node = if offline() {
        offline_node().await?
    } else {
        Node::bind_from_env("KEYLINK_SECRET").await?
    };

    println!("dialing as: {}", node.id());

    let client = inventory::Client::connect(&node, peer.clone()).await?;
    println!(
        "speaking {} ({})",
        client.version().label(),
        client.version()
    );
    println!("local window: {}", inventory::window());

    // ── everything below is ordinary async Rust. No transport in sight. ──
    println!("\nping        -> {}", client.ping().await?);

    let items = client.list().await?;
    println!("list        -> {} items", items.len());
    for item in &items {
        println!("item        -> {item:?}");
        println!(
            "               {:<8} {:<28} {:>8} un  {:?}",
            item.sku, item.name, item.stock, item.price.currency
        );
    }

    println!(
        "\nget_item    -> {:?}",
        client.get_item("ADU-014".into()).await?.map(|i| i.name)
    );
    println!("get_item    -> {:?}", client.get_item("NOPE".into()).await?);

    println!(
        "\nreserve 2   -> {:?}",
        client.reserve("ADU-014".into(), 2).await?
    );
    println!(
        "reserve 99  -> {:?}",
        client.reserve("ADU-014".into(), 99).await?
    );
    println!(
        "reserve ??  -> {:?}",
        client.reserve("NOPE".into(), 1).await?
    );

    concurrent_calls(&client).await?;
    download(&client).await?;
    if let Some(path) = upload {
        // A second service is a second ALPN, so it is a second connection to the
        // same endpoint — dialed here rather than beside `client`, because with
        // no file to send there is nothing to open it for.
        let dock = dock::Client::connect(&node, peer).await?;
        upload_file(&dock, path).await?;
        dock.close().await?;
    }

    #[cfg(not(feature = "previous-contract"))]
    match client.count().await {
        Ok(count) => {
            anyhow::ensure!(count == items.len() as u64, "Count disagreed with List");
            println!("count       -> {count}");
        }
        Err(keylink::Error::UnsupportedRequest { version, source })
            if version.label() == 2 && source.method() == "Count" =>
        {
            println!("count       -> Unsupported ({source})")
        }
        Err(error) => return Err(error.into()),
    }
    client.close().await?;
    println!("contract: OK");
    Ok(())
}

/// Four calls on one connection, issued at once. Each still takes its own QUIC
/// stream — that half is unchanged — but this peer serves the service as an
/// actor, so the four land in one mailbox and run one after another. The number
/// below is the sum of four turns rather than the slowest of four.
///
/// It does not prove that by itself: every method here answers in well under a
/// millisecond, so both serving models print about the same figure. Telling
/// them apart needs a handler held open long enough to see, which is why the
/// claim is measured on both sides elsewhere —
/// `protocol.rs::concurrent_calls_do_not_queue_behind_each_other` for the
/// concurrent half, and
/// `singleton.rs::a_served_actor_serializes_calls_that_arrive_on_separate_streams`
/// for this one.
async fn concurrent_calls(client: &inventory::Client) -> Result<()> {
    let started = n0_future::time::Instant::now();
    let (a, b, c, d) = tokio::join!(
        client.ping(),
        client.list(),
        client.get_item("MUD-001".into()),
        client.get_item("GRA-esm".into()),
    );
    println!(
        "\n4 in flight -> ping={:?} list={} items, {:?} + {:?} in {:?} (4 mailbox turns)",
        a?.is_empty(),
        b?.len(),
        c?.map(|i| i.sku),
        d?.map(|i| i.sku),
        started.elapsed(),
    );
    Ok(())
}

/// Stream a payload down, straight to disk, checking it end to end.
async fn download(client: &inventory::Client) -> Result<()> {
    const KIB: u32 = 8 * 1024;

    let started = n0_future::time::Instant::now();
    let (sheet, mut body) = client.fetch("ADU-014".into(), KIB).await?;
    println!(
        "\nfetch       -> {} declares {} bytes (checksum {:016x})",
        sheet.sku, sheet.bytes, sheet.checksum
    );
    println!(
        "               body declares {:?} bytes",
        body.declared_len()
    );

    let path = std::env::temp_dir().join("keylink-datasheet.bin");
    let mut file = tokio::fs::File::create(&path).await?;
    let mut hash = Fnv1a::new();
    let mut bytes = 0u64;
    while let Some(chunk) = body.next_chunk().await? {
        hash.update(&chunk);
        bytes += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;

    let checksum = hash.finish();
    println!(
        "               received {bytes} bytes in {:?} -> {}",
        started.elapsed(),
        if bytes == sheet.bytes && checksum == sheet.checksum {
            format!(
                "OK, checksum {checksum:016x} matches, saved to {}",
                path.display()
            )
        } else {
            format!("MISMATCH: {checksum:016x} over {bytes} bytes")
        }
    );
    Ok(())
}

/// Stream a real file up, and have the peer confirm what it stored.
///
/// A `dock::Client`, not the `inventory::Client` everything above uses: the
/// upload is a different service now, and the type says so at the call site.
async fn upload_file(client: &dock::Client, path: &str) -> Result<()> {
    let name = std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("upload.bin")
        .to_string();

    let local = tokio::fs::read(path).await?;
    let mut hash = Fnv1a::new();
    hash.update(&local);

    let started = n0_future::time::Instant::now();
    let outcome = client
        .upload(name.clone(), Body::from_file(path).await?)
        .await?;

    println!(
        "\nupload      -> {name} ({} bytes) in {:?}",
        local.len(),
        started.elapsed()
    );
    match outcome {
        UploadOutcome::Stored { bytes, checksum } => println!(
            "               peer stored {bytes} bytes -> {}",
            if bytes == local.len() as u64 && checksum == hash.finish() {
                format!("OK, checksum {checksum:016x} matches")
            } else {
                format!("MISMATCH: {checksum:016x}")
            }
        ),
        UploadOutcome::Rejected { reason } => println!("               rejected: {reason}"),
    }
    Ok(())
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::inventory::Actor;
    use super::*;

    /// ADU-014 is seeded with 3 units.
    const SKU: &str = "ADU-014";

    #[tokio::test]
    async fn reserve_deducts_and_reports_what_is_left() {
        let mut w = Warehouse::seeded();
        match w.reserve(Caller::Local, SKU.into(), 2).await {
            ReserveOutcome::Reserved { remaining } => assert_eq!(remaining, 1),
            other => panic!("expected Reserved, got {other:?}"),
        }
        assert_eq!(
            w.get_item(Caller::Local, SKU.into()).await.unwrap().stock,
            1
        );
    }

    #[tokio::test]
    async fn reserve_is_all_or_nothing_and_leaves_stock_untouched() {
        let mut w = Warehouse::seeded();
        match w.reserve(Caller::Local, SKU.into(), 99).await {
            ReserveOutcome::Insufficient {
                requested,
                available,
            } => {
                assert_eq!((requested, available), (99, 3));
            }
            other => panic!("expected Insufficient, got {other:?}"),
        }
        // The critical half: a refused reservation must not consume anything.
        assert_eq!(
            w.get_item(Caller::Local, SKU.into()).await.unwrap().stock,
            3
        );
    }

    #[tokio::test]
    async fn reserving_exactly_the_stock_succeeds_and_empties_it() {
        let mut w = Warehouse::seeded();
        match w.reserve(Caller::Local, SKU.into(), 3).await {
            ReserveOutcome::Reserved { remaining } => assert_eq!(remaining, 0),
            other => panic!("expected Reserved, got {other:?}"),
        }
        // And the now-empty item still refuses the next request rather than
        // underflowing `u32`, which would wrap to ~4 billion units of stock.
        assert!(matches!(
            w.reserve(Caller::Local, SKU.into(), 1).await,
            ReserveOutcome::Insufficient { available: 0, .. }
        ));
    }

    #[tokio::test]
    async fn unknown_sku_is_distinct_from_insufficient() {
        let mut w = Warehouse::seeded();
        assert!(matches!(
            w.reserve(Caller::Local, "NOPE".into(), 1).await,
            ReserveOutcome::NoSuchItem
        ));
    }

    #[tokio::test]
    async fn fetch_declares_the_length_and_checksum_of_what_it_will_stream() {
        let mut w = Warehouse::seeded();
        let (sheet, body) = w.fetch(Caller::Local, SKU.into(), 4).await;

        assert_eq!(sheet.bytes, 4096);
        assert_eq!(body.declared_len(), Some(4096));
        assert_eq!(sheet.checksum, Pattern::checksum(4096));
    }

    /// The generator and the checksum must agree, or the demo would report a
    /// mismatch on a transfer that was in fact perfect.
    #[tokio::test]
    async fn the_pattern_reader_produces_exactly_what_the_checksum_expects() {
        use tokio::io::AsyncReadExt;

        let len = 5000u64;
        let mut produced = Vec::new();
        Pattern { offset: 0, len }
            .read_to_end(&mut produced)
            .await
            .unwrap();

        assert_eq!(produced.len() as u64, len);
        let mut hash = Fnv1a::new();
        hash.update(&produced);
        assert_eq!(hash.finish(), Pattern::checksum(len));
    }
}
