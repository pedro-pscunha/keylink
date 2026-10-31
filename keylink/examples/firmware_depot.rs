//! Firmware to a fleet: one image, many devices, and an upload coming back.
//!
//! ```sh
//! cargo run -p keylink --example firmware_depot
//! ```
//!
//! Three nodes on the loopback interface: a depot and two devices.
//! `Node::bind_local` binds with no relay and no discovery, so this runs
//! offline. A real program uses [`Node::bind`].
//!
//! # Why this service uses three kinds
//!
//! A firmware image exists as bytes before anybody asks for it, and the same
//! bytes go to every device. That is the `blob` case exactly: content
//! addressing, deduplication and resume. A crash dump is the opposite — it is
//! produced by one device for one depot — so it goes up as a `send`. A reboot
//! order is neither, because nobody waits for it, so it is a `tell`.
//!
//! ```text
//! blob  Image        depot -> device   the same bytes, many times
//! send  Diagnostics  device -> depot   one payload, once
//! tell  Reboot       depot  -> device  nobody waits
//! ```
//!
//! # Two rules this file meets rather than describes
//!
//! A service that declares a `blob` must be given a store with
//! `.with_blobs(&blobs)`, or [`ServerBuilder::spawn`](keylink::ServerBuilder)
//! refuses it at startup rather than on the first call.
//!
//! A service that declares a `send` cannot be served as an actor. A `send`
//! handler reads its upload inside the turn, so a peer that declared a large
//! body and then stopped writing would hold the actor. This one is a concurrent
//! [`Handler`](depot::Handler), which is the shape that has no turn to hold.

use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use keylink::{
    Allowlist, BlobHandle, BlobId, Blobs, Body, BodyReader, Content, Context, Node, Server,
};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};

/// What the depot says about an image, beside the content itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Release {
    /// Which channel answered.
    pub channel: String,
    /// The build this channel points at.
    pub version: u32,
}

/// What the depot says about an upload it accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Receipt {
    /// How many bytes the depot read out of the stream.
    pub bytes: u64,
}

keylink::service! {
    service Depot;

    /// The image for a channel. The reply carries a content address, and the
    /// address is the grant: it authorizes this peer for this content and for
    /// nothing else in the store.
    blob Image(channel: String) -> Release;

    /// A crash dump, streamed up beside the request. Peak memory is one chunk
    /// on both sides, because a body never becomes a `Vec`.
    send Diagnostics(device: String) -> Receipt;

    /// Reboot. Nothing waits for it, so it answers with proof of transmission
    /// rather than with a result.
    tell Reboot(device: String) -> ();

    /// How many reboots the depot has actually carried out, so a caller can
    /// tell transmission from execution.
    rpc Rebooted() -> u64;
}

/// The depot. Concurrent, so every field is shared rather than owned.
struct Depot {
    stable: BlobId,
    beta: BlobId,
    uploaded: Arc<Mutex<Vec<(String, u64)>>>,
    rebooted: Arc<AtomicU64>,
    /// Holds the `reboot` handler until the example releases it, so the
    /// ordering below is a fact rather than a race.
    release: Arc<Semaphore>,
}

impl depot::Handler for Depot {
    async fn image(&self, _cx: &Context, channel: String) -> (Release, Content) {
        let (version, id) = match channel.as_str() {
            "beta" => (91, self.beta),
            _ => (90, self.stable),
        };
        (Release { channel, version }, id.into())
    }

    async fn diagnostics(&self, _cx: &Context, device: String, mut body: BodyReader) -> Receipt {
        let mut bytes = 0;
        while let Ok(Some(chunk)) = body.next_chunk().await {
            bytes += chunk.len() as u64;
        }
        self.uploaded.lock().await.push((device, bytes));
        Receipt { bytes }
    }

    async fn reboot(&self, _cx: &Context, _device: String) {
        // A `tell` handler runs on its own task, and nobody is waiting for it.
        // Holding here is what makes the assertion below provable.
        let _permit = self.release.acquire().await;
        self.rebooted.fetch_add(1, Ordering::SeqCst);
    }

    async fn rebooted(&self, _cx: &Context) -> u64 {
        self.rebooted.load(Ordering::SeqCst)
    }
}

/// Wait for a count to reach `want` by asking, rather than by sleeping.
async fn reaches(client: &depot::Client, want: u64) -> Result<(), Box<dyn StdError>> {
    for _ in 0..400 {
        if client.rebooted().await? == want {
            return Ok(());
        }
        n0_future::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("the count never reached {want}").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. The depot, with a store behind it.
    //
    //  `Blobs::memory` needs no feature and no path. Adding content grants
    //  nothing: a peer reaches an object only once a handler has handed that
    //  peer its address.
    let images = Blobs::memory();
    let stable = images.add_bytes(vec![0xA5; 70_000]).await?;
    let beta = images.add_bytes(vec![0x5A; 12_000]).await?;

    let depot_node = Node::bind_local().await?;
    let uploaded = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Semaphore::new(0));
    let _server = Server::builder(&depot_node)
        .service(
            depot::service(
                Depot {
                    stable,
                    beta,
                    uploaded: Arc::clone(&uploaded),
                    rebooted: Arc::new(AtomicU64::new(0)),
                    release: Arc::clone(&release),
                },
                Allowlist::anyone(),
            )
            .with_blobs(&images)
            .build(),
        )
        .spawn()?;

    let kiln_node = Node::bind_local().await?;
    let press_node = Node::bind_local().await?;
    let kiln = depot::Client::connect(&kiln_node, depot_node.endpoint().addr()).await?;
    let press = depot::Client::connect(&press_node, depot_node.endpoint().addr()).await?;

    // ── 2. Two devices, one image. The address is the same because the bytes
    //       are, and the depot stores one copy however many peers ask.
    //  A `blob` method answers the declared value AND a `BlobHandle`, which is
    //  a content address plus the node that holds it. That pairing is why the
    //  call site below names no second ALPN and dials nothing by hand: the
    //  handle knows both halves and opens the blobs connection itself.
    let (kiln_release, kiln_image): (Release, BlobHandle) =
        kiln.image("stable".to_string()).await?;
    let (press_release, press_image) = press.image("stable".to_string()).await?;
    assert_eq!(kiln_release, press_release);
    assert_eq!(
        kiln_image.id(),
        press_image.id(),
        "the same content has one address"
    );
    println!(
        "both devices were pointed at build {} of {}",
        kiln_release.version, kiln_release.channel
    );

    // ── 3. What a local store buys, measured.
    //
    //  `fetch_into` reports the payload bytes that crossed the wire. The second
    //  call reports zero, because the store already holds content with that
    //  address and a content address is checkable. This is the same mechanism
    //  that resumes an interrupted transfer: whatever is already validated is
    //  never asked for again.
    let device_store = Blobs::memory();
    let first = kiln_image.fetch_into(&device_store).await?;
    let second = kiln_image.fetch_into(&device_store).await?;
    println!("fetch_into transferred {first} bytes, then {second}");
    assert!(first > 0, "the first fetch must move the payload");
    assert_eq!(second, 0, "the second must move nothing");

    //  `copy_to` has no store, so it has neither. It streams to a sink with a
    //  peak of one chunk, and it pays the whole length every time. That is the
    //  right tool for content read once and thrown away, and the wrong one for
    //  a device that will ask again after a power cut.
    let mut sink = Vec::new();
    let copied = kiln_image.copy_to(&mut sink).await?;
    assert_eq!(copied, first, "copy_to always pays full price");
    assert_eq!(sink.len() as u64, copied);

    // ── 4. The upload. One `send`, one body, and a receipt that names what the
    //       depot actually read rather than what the caller claimed.
    let dump = vec![0x7F; 4_096];
    let receipt = kiln
        .diagnostics("kiln-3".to_string(), Body::from_bytes(dump.clone()))
        .await?;
    assert_eq!(receipt.bytes, dump.len() as u64);
    assert_eq!(
        uploaded.lock().await.as_slice(),
        &[("kiln-3".to_string(), 4_096)]
    );
    println!("the depot read {} bytes of diagnostics", receipt.bytes);

    // ── 5. What a `tell` proves, and what it does not.
    //
    //  The handler is held on a semaphore, so it cannot have run. The call
    //  still returns, and it returns `Transmitted` — the bytes reached the
    //  local transport. Nothing more is claimed, and the count says so.
    let _transmitted = kiln.reboot("kiln-3".to_string()).await?;
    assert_eq!(
        kiln.rebooted().await?,
        0,
        "the reply came back before the work: that is what `Transmitted` means"
    );
    println!("the tell returned Transmitted, and the count is still 0");

    //  The same counter reaching 1 is what makes the line above a measurement
    //  rather than a reading of something that is always zero.
    release.add_permits(1);
    reaches(&kiln, 1).await?;
    println!("the reboot has now been carried out");

    Ok(())
}
