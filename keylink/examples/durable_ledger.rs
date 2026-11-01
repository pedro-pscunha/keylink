//! An actor that survives its own process, served to other machines.
//!
//! ```sh
//! cargo run -p keylink --example durable_ledger
//! cargo run -p keylink --example durable_ledger      # run it again: the count carries over
//! ```
//!
//! Everything here compiles as part of the gate, so it is the shape that
//! actually works rather than the shape a document remembers.
//!
//! The state is journaled: each turn appends the one entry it posted, and the
//! log is folded into a snapshot now and then, so a turn costs the size of its
//! change and not the size of the ledger. `tests/persist.rs` covers the
//! whole-state strategy, `Persist<_, _, Whole>`.
//!
//! # What changed in Wave 5
//!
//! This file used to carry a commit loop, with a comment reading *"keylink has no
//! hook in the generated turn loop yet, so this loop is where a caller puts the
//! policy"*. There is a hook now — [`keylink::Persist`] is an
//! [`Observer`](keylink::Observer) that stores inside the turn — so the loop is
//! gone and the durability policy is one argument at spawn.
//!
//! `Own::with_state` is unchanged and not deprecated. It is a *read*, and a
//! read is still the right tool for taking a consistent look at a state from
//! outside a turn. What Wave 5 subsumed is using it as a **commit** mechanism.

use std::error::Error;
use std::time::Duration;

use keylink::{Allowlist, Caller, Deltas, Node, Opened, Own, Persist, Servable};
use keylink_persist::file::Files;
use keylink_persist::versioned::FrameFault;
use keylink_persist::{Changes, Durable, JournalFault, Journaled, RestoreError, StateId};

keylink::service! {
    service Ledger;

    rpc Post(entry: u64) -> u64;
    rpc Total() -> u64;
}

/// One change to the ledger: an entry was posted. This is what a turn appends.
#[derive(Clone)]
struct Posted(u64);

impl Durable for Posted {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
            RestoreError::Malformed(format!("a post is 8 bytes, and this is {}", bytes.len()))
        })?;
        Ok(Self(u64::from_le_bytes(bytes)))
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// The state. It does not know where it is stored — that is the impls below,
/// and the actor never mentions them. `changes` is the one field that is about
/// durability: the turn's changes wait there for the store.
#[derive(Default)]
struct Ledger {
    entries: Vec<u64>,
    changes: Changes<Posted>,
}

impl Journaled for Ledger {
    type Delta = Posted;

    fn changes(&self) -> &Changes<Posted> {
        &self.changes
    }

    /// The one piece of code that changes the entries. A restore runs it for
    /// every stored change, and so does the handler, so the live path and the
    /// replay path cannot drift apart.
    fn apply(&mut self, delta: Posted) {
        self.entries.push(delta.0);
    }
}

impl ledger::Actor for Ledger {
    async fn post(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        let delta = Posted(entry);
        self.apply(delta.clone());
        self.changes.record(delta);
        self.entries.len() as u64
    }

    async fn total(&mut self, _cx: Caller<'_>) -> u64 {
        self.entries.iter().sum()
    }
}

impl Durable for Ledger {
    /// Bump this whenever `snapshot` starts writing a different shape.
    /// `Versioned` stamps it into the frame and reads it back; nothing can
    /// check that you remembered.
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.entries.iter().flat_map(|n| n.to_le_bytes()).collect()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        if !bytes.len().is_multiple_of(8) {
            return Err(RestoreError::Malformed(format!(
                "a ledger is whole u64s, and this is {} bytes",
                bytes.len()
            )));
        }
        Ok(Self {
            entries: bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|c| u64::from_le_bytes(*c))
                .collect(),
            changes: Changes::default(),
        })
    }

    /// Version 1 is the first, so there is no older version to come from. When
    /// this becomes version 2, this grows a `1 => Some(..)` arm and nothing
    /// else — the loader walks the chain, so a hop is always one step.
    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// The store, and the ledger read out of it.
async fn open_state(
    root: &std::path::Path,
    id: &StateId,
) -> keylink_persist::Result<(Files, Opened<Ledger, Files, Deltas>)> {
    let store = Files::open(root).await?.limit(1024 * 1024);
    let opened = Persist::<Ledger, _, Deltas>::open(&store, id).await?;
    Ok((store, opened))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let root = std::env::temp_dir().join("keylink-durable-ledger-deltas");
    let id = StateId::new("ledgers/sp-01")?;

    // ── 1. Claim the state and read whatever is stored, in one call.
    //
    //  The claim is exclusive from here until `saver` is dropped, and it is
    //  released by the operating system if this process dies holding it. The
    //  restored state comes back with the adapter that will keep writing it,
    //  which is what makes "load before you start" impossible to skip.
    //
    //  A snapshot from a *newer* build stops here with `FromTheFuture` rather
    //  than being read as far as this build understands: half a state is worse
    //  than none, and writing the truncation back would be worse still.
    //
    //  **This directory outlives the process on purpose, so it can outlive the
    //  build that wrote it.** A file layout OLDER than this build's stops here as
    //  `LayoutTooOld`, and for a demo that is a reason to start over and say so.
    //  Nothing else is: a journal recovery refused as damaged is evidence, a layout
    //  from a newer build is somebody else's data, and both stay where they are and
    //  stop the example. A real program would not delete its state at all.
    let (_store, opened) = match open_state(&root, &id).await {
        Ok(opened) => opened,
        Err(keylink_persist::Error::Busy { id }) => {
            // NOT a transient error to retry in a loop. Another process
            // believes it owns this state, and two owners is what the claim
            // exists to make visible.
            println!("another process already owns {id}");
            return Ok(());
        }
        Err(
            keylink_persist::Error::NotAJournal {
                fault: JournalFault::LayoutTooOld { .. },
                ..
            }
            | keylink_persist::Error::Restore(keylink_persist::RestoreError::NotAFrame {
                fault: FrameFault::LayoutTooOld { .. },
                ..
            }),
        ) => {
            println!(
                "{} holds state in an older layout; starting a new one",
                root.display()
            );
            std::fs::remove_dir_all(&root)?;
            open_state(&root, &id).await?
        }
        Err(other) => return Err(other.into()),
    };

    //  A crash in the middle of an append leaves a partly written record. Recovery
    //  cuts it and says so here. Nothing acknowledged is lost: that append never
    //  resolved, so no caller was told it had happened.
    if let Some(torn) = opened.torn_tail() {
        println!(
            "recovery cut {} bytes after record {}: an append that never finished",
            torn.bytes, torn.after
        );
    }

    //  `was_stored` asks the question without taking the answer, and
    //  `start_from` is the only way to reach the observer — so there is no
    //  spelling of these two lines that starts the actor from an empty ledger
    //  while a full one sits on the disk. That used to be a tuple, and
    //  `let (_restored, saver) = ..` compiled clean.
    let first_run = !opened.was_stored();
    let (state, saver) = opened.start_from(Ledger::default)?;
    if first_run {
        println!("first run: no snapshot under {}", root.display());
    } else {
        println!(
            "restored {} entries from {}",
            state.entries.len(),
            root.display()
        );
    }

    // ── 2. Start the actor with the adapter watching its turns.
    //
    //  There is no commit loop below this line, and that is the whole of what
    //  Wave 5 changed. `saver` is an `Observer`, so it runs inside every turn,
    //  after the handler and **before the reply the loop sends** — an answered
    //  `rpc` is a stored `rpc`, and a panicked turn is refused rather than
    //  written.
    //
    //  Neither method here is `#[defer]`, and that no longer needs saying: a
    //  `#[defer]` method's parked answer waits for the same observer chain, so
    //  `Deferred::send` is behind the store too. The one send that is not is one
    //  made from a task you spawned yourself, which is answering with something
    //  no turn produced — see `keylink::Persist`'s module header.
    let ledger = ledger::setup(state).observe(saver).actor();
    println!("carried over: total {}", ledger.total().await?);

    // ── 3. Serve the same actor to other machines. Nothing about persistence
    //       reaches the wire: peers use the ordinary `ledger::Client`, and the
    //       ALPN is a hash of the declared types, which an observer is not part
    //       of.
    let node = Node::bind().await?;
    let _server = ledger.serve(&node, Allowlist::anyone())?;
    println!("serving as {}", node.id());

    // ── 4. Ordinary turns. Every one of them is on the disk before its reply.
    for entry in 1..=3u64 {
        ledger.post(entry).await?;
    }
    println!("after three posts: total {}", ledger.total().await?);

    // ── 5. What this costs, stated rather than hidden.
    //
    //  The first turn of a brand new ledger stored a snapshot (an atomic file
    //  replace, two `fsync`s). Every later turn appended one record to the journal
    //  and paid one `fsync` for it, whatever the size of the ledger: 40 bytes for
    //  an 8-byte entry (the 20-byte frame, the 12-byte record header and the entry).
    //  `keylink/src/persist.rs` has the measured comparison with
    //  a whole-state store, which pays the whole ledger's size every turn. That is
    //  still the price of "a reply means it is on the disk", and the knob that
    //  trades it away is a debounce — which nothing here implements, because a
    //  debounce gives up exactly the property this example is about.
    //
    //  Every so often the journal is folded into a new snapshot, and that one turn
    //  costs the size of the ledger. The default policy folds at a megabyte of
    //  journal, which this example never reaches; `Opened::compaction` sets another.
    //
    //  Nothing in THIS example is uncommitted any more. The window the previous
    //  version demonstrated — a turn the caller's loop had not got around to
    //  storing — does not exist when the store is inside the turn. Two things
    //  the store's placement does not cover, both named rather than implied: a
    //  `Deferred` answered from a task you spawned yourself, which is answering
    //  with something no turn produced, and the idempotency ledger a retrying
    //  peer is matched against (RFC-07 §6.8) — which is memory here, because
    //  this service leaves `Records` at its default. `payment_approval.rs`
    //  serves the other port beside this one and closes that half.
    ledger.stop().await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}
