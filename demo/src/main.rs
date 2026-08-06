//! An inventory service across two machines.
//!
//!   cargo run -p demo -- serve
//!   cargo run -p demo -- connect <endpoint-id> [file-to-upload]
//!
//! Note what is NOT in this file: no IP address, no port, no framing, no
//! serialization call, no ALPN string, no version number. The domain types and
//! the `service!` block are the entire protocol definition. A file transfer is
//! `Body::from_file(path)` on one side and `copy_to(&mut file)` on the other.

use std::collections::BTreeMap;
use std::io;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use anyhow::{bail, Context as _, Result};
use keylink::{Allowlist, Body, BodyReader, EndpointId, Node};
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
pub struct Item {
    pub sku: String,
    pub name: String,
    pub price: Money,
    pub stock: u32,
    pub tags: Vec<String>,
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

keylink::service! {
    service Inventory;

    rpc  Ping() -> String;
    rpc  List() -> Vec<Item>;
    rpc  GetItem(sku: String) -> Option<Item>;
    rpc  Reserve(sku: String, qty: u32) -> ReserveOutcome;

    recv Fetch(sku: String, kib: u32) -> Datasheet;
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
    items: Mutex<BTreeMap<String, Item>>,
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
                },
            );
        }
        Self {
            items: Mutex::new(items),
        }
    }

    /// A panicking handler must not wedge every later request. Recover the guard
    /// instead of propagating poisoning — one bad request should cost one
    /// request, not the process.
    fn items(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Item>> {
        self.items
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

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

impl inventory::Handler for Warehouse {
    async fn ping(&self) -> String {
        format!("ok from {}", hostname())
    }

    async fn list(&self) -> Vec<Item> {
        self.items().values().cloned().collect()
    }

    async fn get_item(&self, sku: String) -> Option<Item> {
        self.items().get(&sku).cloned()
    }

    /// All-or-nothing. A partial fill would need a variant carrying how many we
    /// actually got, and a caller that always checks it — easy to get wrong at
    /// the call site, so the stricter contract is the safer default here.
    async fn reserve(&self, sku: String, qty: u32) -> ReserveOutcome {
        let mut items = self.items();
        let Some(item) = items.get_mut(&sku) else {
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
    async fn fetch(&self, sku: String, kib: u32) -> (Datasheet, Body) {
        let len = u64::from(kib) * 1024;
        let sheet = Datasheet {
            sku,
            bytes: len,
            checksum: Pattern::checksum(len),
        };
        let body = Body::from_reader(Pattern { offset: 0, len }, Some(len));
        (sheet, body)
    }

    /// The declared length lets us refuse an oversized upload before it moves.
    async fn upload(&self, name: String, body: BodyReader) -> UploadOutcome {
        const LIMIT: u64 = 64 * 1024 * 1024;

        if body.declared_len().is_some_and(|len| len > LIMIT) {
            return UploadOutcome::Rejected {
                reason: format!("over the {LIMIT} byte limit"),
            };
        }
        match Warehouse::store(&name, body).await {
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

async fn serve() -> Result<()> {
    let node = Node::bind_from_env("KEYLINK_SECRET").await?;
    node.online().await;

    // Fails at startup when unset, rather than bouncing every peer at runtime.
    let allow = Allowlist::from_env("KEYLINK_ALLOWED_IDS")?;

    println!(
        "alpn:         {}",
        String::from_utf8_lossy(inventory::alpn())
    );
    println!("listening as: {}", node.id());

    let server = inventory::serve(&node, Warehouse::seeded(), allow)?;
    tokio::signal::ctrl_c().await?;
    server.shutdown().await?;
    Ok(())
}

async fn connect(id: &str, upload: Option<&str>) -> Result<()> {
    let peer: EndpointId = id.parse().context("bad endpoint id")?;
    let node = Node::bind_from_env("KEYLINK_SECRET").await?;

    println!("alpn:      {}", String::from_utf8_lossy(inventory::alpn()));
    println!("dialing as: {}", node.id());

    let client = inventory::Client::connect(&node, peer).await?;

    // ── everything below is ordinary async Rust. No transport in sight. ──
    println!("\nping        -> {}", client.ping().await?);

    let items = client.list().await?;
    println!("list        -> {} items", items.len());
    for item in &items {
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
        upload_file(&client, path).await?;
    }

    client.close().await?;
    Ok(())
}

/// Four calls on one connection, in flight at once. Each takes its own QUIC
/// stream, so the slowest one does not decide when the others finish.
async fn concurrent_calls(client: &inventory::Client) -> Result<()> {
    let started = std::time::Instant::now();
    let (a, b, c, d) = tokio::join!(
        client.ping(),
        client.list(),
        client.get_item("MUD-001".into()),
        client.get_item("GRA-esm".into()),
    );
    println!(
        "\n4 concurrent -> ping={:?} list={} items, {:?} + {:?} in {:?}",
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

    let started = std::time::Instant::now();
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
async fn upload_file(client: &inventory::Client, path: &str) -> Result<()> {
    let name = std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("upload.bin")
        .to_string();

    let local = tokio::fs::read(path).await?;
    let mut hash = Fnv1a::new();
    hash.update(&local);

    let started = std::time::Instant::now();
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

    use super::inventory::Handler;
    use super::*;

    /// ADU-014 is seeded with 3 units.
    const SKU: &str = "ADU-014";

    #[tokio::test]
    async fn reserve_deducts_and_reports_what_is_left() {
        let w = Warehouse::seeded();
        match w.reserve(SKU.into(), 2).await {
            ReserveOutcome::Reserved { remaining } => assert_eq!(remaining, 1),
            other => panic!("expected Reserved, got {other:?}"),
        }
        assert_eq!(w.get_item(SKU.into()).await.unwrap().stock, 1);
    }

    #[tokio::test]
    async fn reserve_is_all_or_nothing_and_leaves_stock_untouched() {
        let w = Warehouse::seeded();
        match w.reserve(SKU.into(), 99).await {
            ReserveOutcome::Insufficient {
                requested,
                available,
            } => {
                assert_eq!((requested, available), (99, 3));
            }
            other => panic!("expected Insufficient, got {other:?}"),
        }
        // The critical half: a refused reservation must not consume anything.
        assert_eq!(w.get_item(SKU.into()).await.unwrap().stock, 3);
    }

    #[tokio::test]
    async fn reserving_exactly_the_stock_succeeds_and_empties_it() {
        let w = Warehouse::seeded();
        match w.reserve(SKU.into(), 3).await {
            ReserveOutcome::Reserved { remaining } => assert_eq!(remaining, 0),
            other => panic!("expected Reserved, got {other:?}"),
        }
        // And the now-empty item still refuses the next request rather than
        // underflowing `u32`, which would wrap to ~4 billion units of stock.
        assert!(matches!(
            w.reserve(SKU.into(), 1).await,
            ReserveOutcome::Insufficient { available: 0, .. }
        ));
    }

    #[tokio::test]
    async fn unknown_sku_is_distinct_from_insufficient() {
        let w = Warehouse::seeded();
        assert!(matches!(
            w.reserve("NOPE".into(), 1).await,
            ReserveOutcome::NoSuchItem
        ));
    }

    #[tokio::test]
    async fn fetch_declares_the_length_and_checksum_of_what_it_will_stream() {
        let w = Warehouse::seeded();
        let (sheet, body) = w.fetch(SKU.into(), 4).await;

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
