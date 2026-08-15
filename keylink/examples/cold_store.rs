//! Content that outlives the process holding it, and a body read off a disk.
//!
//! ```sh
//! cargo run -p keylink --features blobs-fs --example cold_store
//! ```
//!
//! Two nodes on the loopback interface. `Node::bind_local` binds with no relay
//! and no discovery, so this runs offline.
//!
//! # Two features, and neither is on by default
//!
//! [`Blobs::memory`] needs no feature and no path, which is why
//! `firmware_depot.rs` reaches for it first. This example is the other half.
//!
//! | feature | what it adds | default |
//! |---|---|---|
//! | `blobs-fs` | [`Blobs::persistent`], which is iroh-blobs' `fs-store` and pulls redb | no |
//! | `fs` | [`Body::from_file`] | yes |
//!
//! `blobs-fs` is off by default so that a store needing neither a disk nor a
//! feature is the one a reader meets first. This example declares it in
//! `Cargo.toml` under `required-features`, so `cargo` skips the target rather
//! than failing to build it when the feature is off.
//!
//! # An awaited fetch is accepted, not committed
//!
//! iroh-blobs' metadata actor holds one write transaction across a batch and
//! commits when the batch ends. So a fetch that has returned is not yet on the
//! disk, and a process that exits promptly loses it — measured at 2 commits in
//! 10 process pairs when the store is merely dropped.
//!
//! [`Blobs::flush`] is what commits it, and act 5 calls it. To watch the reel
//! survive a process, pin the path so two runs share it:
//!
//! ```sh
//! export KEYLINK_COLD_STORE_ROOT=/tmp/cold-store-demo
//! cargo run -p keylink --features blobs-fs --example cold_store   # moves the reel
//! cargo run -p keylink --features blobs-fs --example cold_store   # moves nothing
//! ```
//!
//! Every assertion below is history-independent. The cross-run number is
//! printed rather than asserted, because a first run and a second run honestly
//! differ.
//!
//! # Why the default path carries this process's id
//!
//! **Two `cold_store` processes on one shared path deadlock, and neither the
//! gate nor either git hook carries a timeout.** `Blobs::persistent` cannot
//! claim a contended redb directory, and the store actor's shutdown then blocks
//! on a channel nothing sends — measured 4 times out of 4, hung past 30
//! seconds, against a control on separate paths that exits in about 200 ms. Two
//! worktrees or two agents running the gate at once would hang a push with no
//! output at all.
//!
//! So the default root ends in [`std::process::id`], which cannot collide, and
//! the environment variable above is how a reader opts into a shared one. That
//! is a sharper answer than `durable_ledger.rs`'s, which prints and returns on
//! `Busy`, because this store never reports `Busy` — it hangs instead.
//!
//! [`Retention::Forever`] is the only policy, and it says what it costs: the
//! store grows until a disk is cleared by hand. There is no sweeper, and naming
//! the variant is how a caller acknowledges that rather than discovering it.

use std::error::Error as StdError;

use keylink::{Allowlist, BlobId, BlobReader, Blobs, Body, Context, Node, Retention, Server};

keylink::service! {
    service Archive;

    /// One reel of film, by content address.
    blob Reel(name: String) -> u64;

    /// The manifest, streamed off the disk rather than read into memory.
    recv Manifest(name: String) -> u64;
}

struct Archive {
    reel: (BlobId, u64),
    manifest: std::path::PathBuf,
}

impl archive::Handler for Archive {
    async fn reel(&self, _cx: &Context, _name: String) -> (u64, BlobId) {
        (self.reel.1, self.reel.0)
    }

    async fn manifest(&self, _cx: &Context, _name: String) -> (u64, Body) {
        //  `from_file` opens and streams. Peak memory is one chunk, whatever the
        //  file weighs, and the declared length comes off the file's own
        //  metadata rather than from a caller's claim.
        match Body::from_file(&self.manifest).await {
            Ok(body) => (body.declared_len().unwrap_or(0), body),
            Err(_) => (0, Body::empty()),
        }
    }
}

const REEL: &[u8] = b"an archive reel, ninety thousand frames of it, abbreviated";

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    //  Unique per process unless a reader pins it, because two processes on one
    //  redb path deadlock rather than refusing. See the header.
    let pinned = std::env::var("KEYLINK_COLD_STORE_ROOT").ok();
    let shared = pinned.is_some();
    //  Sweep what an INTERRUPTED run left. The default root carries a process
    //  id, which is what keeps two runs from deadlocking — and it is also why
    //  nothing would ever match a leftover again: a killed run leaks about
    //  2 MB under a name no later run computes. `Retention::Forever` sweeps
    //  nothing, so this is the sweeper.
    if let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) {
        for stale in entries.filter_map(Result::ok) {
            let name = stale.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("keylink-cold-store-")
                && name != format!("keylink-cold-store-{}", std::process::id())
            {
                let _ = std::fs::remove_dir_all(stale.path());
            }
        }
    }
    let root = pinned.map_or_else(
        || std::env::temp_dir().join(format!("keylink-cold-store-{}", std::process::id())),
        std::path::PathBuf::from,
    );
    //  The SERVING store is rebuilt each run, so the reel is re-added and hashes
    //  to the same address. The READER's store is left alone when the path is
    //  pinned, because whether it still holds that address is the question.
    let vault = root.join("vault");
    let reader_store = root.join("reader");
    let _ = std::fs::remove_dir_all(&vault);
    std::fs::create_dir_all(&root)?;

    let manifest = root.join("manifest.txt");
    std::fs::write(&manifest, b"reel-1\treel-2\treel-3\n")?;

    // ── 1. A store on a disk, and content in it.
    //
    //  `Retention::Forever` is a decision the type makes you type out: this
    //  store grows until somebody clears the path, and nothing sweeps it.
    let archive = Blobs::persistent(&vault, Retention::Forever).await?;
    let id = archive.add_bytes(REEL.to_vec()).await?;

    let archive_node = Node::bind_local().await?;
    let reader_node = Node::bind_local().await?;
    let _server = Server::builder(&archive_node)
        .service(
            archive::service(
                Archive {
                    reel: (id, REEL.len() as u64),
                    manifest: manifest.clone(),
                },
                Allowlist::anyone(),
            )
            .with_blobs(&archive)
            .build(),
        )
        .spawn()?;
    let reader = archive::Client::connect(&reader_node, archive_node.endpoint().addr()).await?;

    // ── 2. The reel, fetched into a store that is also on a disk.
    let (declared, handle) = reader.reel("reel-1".to_string()).await?;
    let store = Blobs::persistent(&reader_store, Retention::Forever).await?;
    let moved = handle.fetch_into(&store).await?;
    println!("this run's first fetch moved {moved} bytes of a {declared}-byte reel");
    if moved == 0 {
        println!("  (zero, so a previous run committed the reel to this path)");
    }
    //  Whole or nothing, which is the one thing true on both paths. A fetch
    //  that moved some other number transferred a partial object, and every
    //  assertion below this line reads the ARCHIVE through `handle` rather than
    //  this store — so without this line a fetch that wrote nothing at all would
    //  pass the whole file.
    assert!(
        moved == declared || moved == 0,
        "a fetch either moves the object or finds it already held, got {moved}"
    );
    assert_eq!(handle.verified_len().await?, declared);

    //  A range, which is what a resumed or partial read is made of.
    //  `total_len` is the whole object and `selected_len` is this window, so a
    //  caller can never mistake one for the other.
    let window: BlobReader = handle.open_range(0..8).await?;
    println!(
        "a range reader sees {} selected of {} total",
        window.selected_len(),
        window.total_len()
    );
    assert_eq!(window.selected_len(), 8);
    assert_eq!(window.total_len(), declared);
    let mut head = Vec::new();
    window.copy_to(&mut head).await?;
    assert_eq!(head.len(), 8);

    // ── 3. Deduplication, measured without depending on history.
    //
    //  Whatever the first fetch moved, the second moves nothing: the store
    //  already holds content with that address, and a content address is
    //  checkable. This is the same mechanism that resumes an interrupted
    //  transfer, and on a disk it survives the process as well as the handle.
    let again = handle.fetch_into(&store).await?;
    println!("the second fetch moved {again} bytes");
    assert_eq!(again, 0, "content already held is never asked for again");

    // ── 4. A body read off a file, which is the other feature.
    //
    //  Nothing here holds the file in memory: the handler streams it and this
    //  caller drains it a chunk at a time.
    let (length, mut body) = reader.manifest("all".to_string()).await?;
    let mut seen = 0u64;
    while let Some(chunk) = body.next_chunk().await? {
        seen += chunk.len() as u64;
    }
    println!("the manifest streamed {seen} bytes, and the handler declared {length}");
    assert_eq!(
        seen, length,
        "a declared length is a promise about the body"
    );
    assert_eq!(seen, std::fs::metadata(&manifest)?.len());

    // ── 5. The commit, without which none of act 3 survives this process.
    //
    //  A store mounted on a service is committed by `Server::shutdown`, through
    //  iroh's router. This one is a fetch target and is mounted on nothing, so
    //  it has no such path and `flush` is the whole of it.
    //
    //  `Retention::Forever` sweeps nothing, so what this commits stays until
    //  somebody clears the path by hand.
    store.flush().await?;
    //  The store is the subject of this file, so something has to read it. A
    //  committed redb file is what `flush` produces and what the next process
    //  opens, and it is the only reading of this store available without
    //  mounting it on a service purely to reach one.
    let committed: u64 = std::fs::read_dir(&reader_store)?
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .map(|meta| meta.len())
        .sum();
    assert!(
        committed > 0,
        "a flushed store holds bytes on the disk, found {committed}"
    );
    println!(
        "the committed store holds {committed} bytes under {}",
        reader_store.display()
    );
    if shared {
        println!("the store is committed; run this again and the first fetch moves 0");
    } else {
        //  Nothing else will clear a `Retention::Forever` store, and this root
        //  is this process's alone, so leaving it would be a leak per run.
        drop(store);
        let _ = std::fs::remove_dir_all(&root);
        println!("the store was committed, then cleared: this root is not shared");
    }
    Ok(())
}
