//! An actor that survives its own process, served to other machines.
//!
//! ```sh
//! cargo run -p keylink --example durable_ledger
//! cargo run -p keylink --example durable_ledger      # run it again: the count carries over
//! ```
//!
//! Everything here compiles as part of the gate, so it is the shape that
//! actually works rather than the shape a document remembers.

use std::error::Error;
use std::time::Duration;

use keylink::{Allowlist, Node};
use keylink_persist::file::Files;
use keylink_persist::{Claim, Durable, RestoreError, Snapshots, StateId};

keylink::service! {
    service Ledger;

    rpc Post(entry: u64) -> u64;
    rpc Total() -> u64;
}

/// The state. It does not know it is durable — that is the second impl below,
/// and the actor never mentions it.
#[derive(Default)]
struct Ledger {
    entries: Vec<u64>,
}

impl ledger::Actor for Ledger {
    async fn post(&mut self, entry: u64) -> u64 {
        self.entries.push(entry);
        self.entries.len() as u64
    }

    async fn total(&mut self) -> u64 {
        self.entries.iter().sum()
    }
}

impl Durable for Ledger {
    fn snapshot(&self) -> Vec<u8> {
        self.entries.iter().flat_map(|n| n.to_le_bytes()).collect()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        if !bytes.len().is_multiple_of(8) {
            return Err(RestoreError(format!(
                "a ledger is whole u64s, and this is {} bytes",
                bytes.len()
            )));
        }
        Ok(Self {
            entries: bytes
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap_or([0; 8])))
                .collect(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join("keylink-durable-ledger");
    let store = Files::open(&root).await?.limit(1024 * 1024);
    let id = StateId::new("ledgers/sp-01")?;

    // ── 1. Claim it. Exclusive for as long as this value lives, and released
    //       by the operating system if this process dies holding it.
    let mut claim = match store.claim(&id).await {
        Ok(claim) => claim,
        Err(keylink_persist::Error::Busy { id }) => {
            // NOT a transient error to retry in a loop. Another process
            // believes it owns this state, and two owners is what the claim
            // exists to make visible.
            println!("another process already owns {id}");
            return Ok(());
        }
        Err(other) => return Err(other.into()),
    };

    // ── 2. Restore, or start. `None` is the first ever run, not a failure.
    let state = match claim.load().await? {
        Some(bytes) => {
            println!("restored {} bytes from {}", bytes.len(), root.display());
            Ledger::restore(&bytes)?
        }
        None => {
            println!("first run: no snapshot under {}", root.display());
            Ledger::default()
        }
    };

    let ledger = ledger::actor(state);
    println!("carried over: total {}", ledger.total().await?);

    // ── 3. Serve the same actor to other machines. Nothing about persistence
    //       reaches the wire: peers use the ordinary `ledger::Client`.
    let node = Node::bind().await?;
    let _server = ledger.serve(&node, Allowlist::anyone())?;
    println!("serving as {}", node.id());

    // ── 4. Run turns, and commit after each one.
    //
    //  This is the synchronous durability point: the snapshot is taken and
    //  stored before the loop moves on, so a crash after an iteration cannot
    //  lose it. keylink has no hook in the generated turn loop yet, so this
    //  loop is where a caller puts the policy.
    for entry in 1..=3u64 {
        ledger.post(entry).await?;

        // `with_state` takes a turn, so it cannot read a half-applied one.
        let bytes = ledger.with_state(Ledger::snapshot).await?;
        claim.store(&bytes).await?;
    }
    println!("after three posts: total {}", ledger.total().await?);

    // ── 5. What is NOT durable, shown rather than described.
    //
    //  This turn is never snapshotted. A crash here loses it, and the next run
    //  starts from the total printed above. The size of that window is the
    //  caller's choice, and it is the whole trade.
    ledger.post(1_000).await?;
    println!(
        "uncommitted: total {} (a crash here loses 1000)",
        ledger.total().await?
    );

    // Stopping drains what is queued. It does NOT store anything: durability
    // is the caller's, deliberately, because a store on drop would run at a
    // moment nobody chose.
    ledger.stop().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}
