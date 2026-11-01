//! What the grant ledger costs in heap, measured rather than derived.
//!
//! This target owns a global allocator that counts the bytes the calling thread
//! has allocated and not freed, so it must never join the shared `gate` process.
//! The aggregate inventory and `mise.toml` name it as isolated.
//!
//! The ledger is filled on the test's own thread, after the runtime that made
//! its peers is idle, so the counter sees the ledger and nothing else: the
//! counter is per thread, and the runtime's threads are not this one.
//!
//! Every figure is printed (run with `--nocapture`) and compared with a ceiling
//! written here. The ceilings are what `GRANT_LIMIT`'s documentation relies on:
//! `GRANT_LIMIT` is about 64 MiB at the per-grant cost these runs measure.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::num::NonZeroUsize;

use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use keylink::{AdmittedPeer, Allowlist, BlobId, Blobs, Content, ProvenPeer};

mod common;
use common::local_node;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: every call is delegated unchanged to `System`; the counter is a plain
// thread-local cell with no destructor and no allocation of its own.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the layout is passed through unchanged.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let _ = LIVE.try_with(|live| live.set(live.get() + layout.size() as isize));
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let _ = LIVE.try_with(|live| live.set(live.get() - layout.size() as isize));
        // SAFETY: `pointer` and `layout` came from the allocation above.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Net bytes this thread has allocated.
fn live() -> isize {
    LIVE.with(Cell::get)
}

/// Hands each peer that connects to the test as an admitted peer, which is the
/// only way to get one: `AdmittedPeer` comes out of an allowlist's verdict on a
/// connection that proved its key.
#[derive(Debug, Clone)]
struct Collect(tokio::sync::mpsc::UnboundedSender<AdmittedPeer>);

const ALPN: &[u8] = b"/keylink-test/grant-allocation/1";

impl ProtocolHandler for Collect {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let admitted = Allowlist::anyone()
            .admit(ProvenPeer::on(&connection))
            .expect("anyone admits everyone");
        self.0.send(admitted).ok();
        connection.closed().await;
        Ok(())
    }
}

/// `count` real peers, admitted by a real accept.
fn peers(runtime: &tokio::runtime::Runtime, count: usize) -> Vec<AdmittedPeer> {
    runtime.block_on(async {
        let server = local_node().await;
        let (tx, mut received) = tokio::sync::mpsc::unbounded_channel();
        let _router = Router::builder(server.endpoint().clone())
            .accept(ALPN, Collect(tx))
            .spawn();

        let mut nodes = Vec::new();
        let mut connections = Vec::new();
        let mut admitted = Vec::new();
        for _ in 0..count {
            let node = local_node().await;
            connections.push(
                node.endpoint()
                    .connect(server.endpoint().addr(), ALPN)
                    .await
                    .expect("dials the collector"),
            );
            admitted.push(received.recv().await.expect("the collector admitted it"));
            nodes.push(node);
        }
        admitted
    })
}

/// A fresh store, with a limit that gets out of the way of the measurement.
fn store(runtime: &tokio::runtime::Runtime) -> Blobs {
    let blobs = runtime.block_on(async { Blobs::memory() });
    blobs.set_grant_limit(NonZeroUsize::new(1 << 24).expect("non-zero"));
    blobs
}

/// The `nth` distinct address, built the way a peer's is: off the wire.
fn id(nth: u64) -> BlobId {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&nth.to_le_bytes());
    bytes[8..16].copy_from_slice(&(!nth).to_le_bytes());
    postcard::from_bytes(&bytes).expect("32 raw bytes decode as a BlobId")
}

fn grant(blobs: &Blobs, peers: &[AdmittedPeer], nth: u64) {
    blobs
        .grant(&peers[nth as usize % peers.len()], Content::Blob(id(nth)))
        .expect("within the limit");
}

fn revoke(blobs: &Blobs, peers: &[AdmittedPeer], nth: u64) {
    assert!(blobs.revoke(peers[nth as usize % peers.len()].id(), id(nth)));
}

/// Bytes of heap per grant for `grants` grants held, against `ceiling`.
#[track_caller]
fn report(what: &str, before: isize, grants: u64, ceiling: f64) -> f64 {
    let per = (live() - before) as f64 / grants as f64;
    println!(
        "grant_allocation: {what}: {per:.1} bytes per grant ({grants} grants, ceiling {ceiling})"
    );
    assert!(
        per <= ceiling,
        "{what}: {per:.1} bytes per grant is over the ceiling of {ceiling}"
    );
    per
}

/// The ceilings, in bytes of heap per grant held, over what was measured on
/// aarch64 macOS (568 for a lone grant; 64.2 at 100,000; 100.7 per grant held
/// after churn; 128.5 for 100,000 held after 300,000 trades, which is the most:
/// deleted slots are tombstones, and hashbrown answers a table full of them by
/// doubling it, so a table that trades one grant for another at a steady size can
/// sit at about twice the size it needs).
///
/// The first is for a lone grant, which carries the smallest peers' table and the
/// smallest table of roots. The others are for a ledger that has grown, where a
/// table is between about 1.14 and 2.3 slots per grant of 48 bytes (112 at worst,
/// just after a doubling) and the peers' table is a rounding error; the steady
/// ceiling also covers the tombstone figure. Churn is allowed what the rebuild
/// rule leaves: a table is rebuilt at under half its high-water mark, so it can
/// hold twice what it needs.
///
/// Watched red with `scripts/break-once.sh` by replacing
/// `const CEILING_STEADY: f64 = 160.0;` with `const CEILING_STEADY: f64 = 64.0;`
/// (a ceiling less than half what the tombstone run needs): the 1,000-grant run,
/// at 101.8 bytes, fails with `1,000 grants: 101.8 bytes per grant is over the
/// ceiling of 64`. And by replacing `self.map.shrink_to_fit();` with nothing
/// in `keylink/src/blobs.rs` (row `HK57`): the churn run, which leaves 1,000
/// grants held in tables that once held 12,500 each, fails with `churn: after
/// the first revoke: 1607.1 bytes per grant is over the ceiling of 256`.
const CEILING_ONE: f64 = 1024.0;
const CEILING_STEADY: f64 = 160.0;
const CEILING_CHURN: f64 = 256.0;

/// One test, with the runs as phases of it, for the reason `actor_memory` is: the
/// counter is per thread, but the peers are made once and are costly.
#[test]
fn the_ledger_costs_a_bounded_number_of_bytes_per_grant() {
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    let peers = peers(&runtime, 8);

    // 1. One grant, by one peer: the cost of the smallest ledger.
    {
        let blobs = store(&runtime);
        let before = live();
        grant(&blobs, &peers[..1], 0);
        report("1 grant", before, 1, CEILING_ONE);
    }

    // 2. A thousand over eight peers, then a hundred thousand.
    for (count, name) in [(1_000u64, "1,000 grants"), (100_000, "100,000 grants")] {
        let blobs = store(&runtime);
        let before = live();
        for nth in 0..count {
            grant(&blobs, &peers, nth);
        }
        report(name, before, count, CEILING_STEADY);
    }

    // 3. Churn. A hundred thousand grants, all but a hundredth revoked, then
    //    rounds of granting ninety-nine thousand new ones and revoking them
    //    again: what is retained is judged per grant still held.
    {
        let blobs = store(&runtime);
        let before = live();
        for nth in 0..100_000 {
            grant(&blobs, &peers, nth);
        }
        for nth in (0..100_000).filter(|nth| nth % 100 != 0) {
            revoke(&blobs, &peers, nth);
        }
        report(
            "churn: after the first revoke",
            before,
            1_000,
            CEILING_CHURN,
        );
        for round in 1..=3u64 {
            let base = round * 1_000_000;
            for nth in 0..99_000 {
                grant(&blobs, &peers, base + nth);
            }
            for nth in 0..99_000 {
                revoke(&blobs, &peers, base + nth);
            }
        }
        report("churn: after three rounds", before, 1_000, CEILING_CHURN);
    }

    // 4. Tombstones. A hundred thousand held, and then three hundred thousand
    //    trades of the oldest for a new one: the table stays one size while
    //    every slot in it has been deleted and refilled.
    {
        let blobs = store(&runtime);
        let before = live();
        for nth in 0..100_000 {
            grant(&blobs, &peers, nth);
        }
        for step in 0..300_000u64 {
            grant(&blobs, &peers, 100_000 + step);
            revoke(&blobs, &peers, step);
        }
        report(
            "tombstones: 100,000 held after 300,000 trades",
            before,
            100_000,
            CEILING_STEADY,
        );
    }
}
