//! An actor whose state outlives its process.
//!
//! This is the driving side of the hexagon. `keylink-persist` owns the port and
//! the file adapter; keylink owns the actor and the seam
//! (`Handle::with_state`). Neither crate depends on the other outside this test
//! binary, which is what keeps the port from knowing about the framework.
//!
//! What is proved here is the literal requirement: **a restart does not erase
//! the state.** A restart is simulated by dropping every handle to the actor and
//! building a new one from the store, which is all a new process would do.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink::Caller;
use keylink_persist::versioned::Versioned;
use keylink_persist::{file::Files, Durable, RestoreError, Snapshots, StateId};

keylink::service! {
    service Ledger;

    /// Holds the actor for `hold_ms`, so a turn is long enough to queue behind.
    rpc Post(entry: u64, hold_ms: u64) -> u64;
    rpc Entries() -> Vec<u64>;
}

/// The obvious code, with no lock and no persistence concern in sight. The
/// state does not know it is durable — `Durable` is a second impl, below.
#[derive(Default)]
struct Ledger {
    entries: Vec<u64>,
}

impl ledger::Actor for Ledger {
    async fn post(&mut self, _cx: Caller<'_>, entry: u64, hold_ms: u64) -> u64 {
        n0_future::time::sleep(std::time::Duration::from_millis(hold_ms)).await;
        self.entries.push(entry);
        self.entries.len() as u64
    }

    async fn entries(&mut self, _cx: Caller<'_>) -> Vec<u64> {
        self.entries.clone()
    }
}

/// Eight bytes per entry, little-endian. A real state would reach for postcard;
/// the point here is that the encoding is the state's own decision and the port
/// never looks inside.
impl Durable for Ledger {
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
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8)")))
                .collect(),
        })
    }

    /// Version 1 is the first, so there is no older version to come from.
    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// A scratch directory that removes itself.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("keylink-durable-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The whole loop a caller writes today, in one place.
///
/// keylink has no hook in the generated turn loop yet, so the commit is
/// explicit. That is deliberate: a durability policy woven into every turn is a
/// bigger change than a port, and this proves the port first.
async fn run(files: &Files, id: &StateId, entries: &[u64]) -> Vec<u64> {
    // The claim moves into the wrapper and there is no way back out to it, so
    // nothing below this line can write a snapshot without a version stamp.
    let mut durable = Versioned::new(files.claim(id).await.expect("claims"));

    // The first ever run is `None`. No file, no state, no ceremony.
    let state = durable
        .load::<Ledger>()
        .await
        .expect("loads")
        .unwrap_or_default();

    let ledger = ledger::actor(state);
    for entry in entries {
        ledger.post(*entry, 0).await.expect("running");
    }

    // Between turns, so it cannot see a half-applied one.
    let bytes = ledger.with_state(Ledger::snapshot).await.expect("running");
    durable.store::<Ledger>(&bytes).await.expect("stores");

    let seen = ledger.entries().await.expect("running");
    ledger.stop().await;
    seen
}

/// The requirement, end to end: what the first run wrote, the second run reads.
///
/// Made red by removing the `claim.store(..)` call, which is the whole point of
/// the exercise, or by having `run` ignore what `load` returned.
#[tokio::test]
async fn a_state_survives_the_process_that_built_it() {
    let scratch = Scratch::new("survives");
    let files = Files::open(&scratch.0).await.expect("opens");
    let id = StateId::new("ledgers/sp-01").expect("valid");

    assert_eq!(run(&files, &id, &[1, 2, 3]).await, vec![1, 2, 3]);

    // Every handle from the first run is dropped. As far as the store can tell,
    // that is a new process.
    assert_eq!(run(&files, &id, &[4]).await, vec![1, 2, 3, 4]);
    assert_eq!(run(&files, &id, &[]).await, vec![1, 2, 3, 4]);
}

/// Two states on one store do not see each other.
#[tokio::test]
async fn two_actors_keep_their_own_state() {
    let scratch = Scratch::new("two");
    let files = Files::open(&scratch.0).await.expect("opens");
    let sp = StateId::new("ledgers/sp-01").expect("valid");
    let rj = StateId::new("ledgers/rj-02").expect("valid");

    run(&files, &sp, &[1, 2]).await;
    run(&files, &rj, &[9]).await;

    assert_eq!(run(&files, &sp, &[]).await, vec![1, 2]);
    assert_eq!(run(&files, &rj, &[]).await, vec![9]);
}

/// `with_state` takes a turn. So it queues behind a turn already running and
/// observes the state *after* it, never a state that turn has not finished
/// changing.
///
/// Made red by reading the state through a shared reference instead of the
/// mailbox: the snapshot would then be taken while `post` is still sleeping,
/// and come back empty.
#[tokio::test]
async fn a_snapshot_queues_behind_a_turn_that_is_already_running() {
    let ledger = ledger::actor(Ledger::default());

    let slow = {
        let ledger = ledger.clone();
        tokio::spawn(async move { ledger.post(7, 120).await })
    };
    // Long enough for the post to be the turn in progress, short enough that it
    // is still running. Load can only make the post finish later, which makes
    // the assertion harder to pass rather than easier.
    n0_future::time::sleep(std::time::Duration::from_millis(30)).await;

    let bytes = ledger.with_state(Ledger::snapshot).await.expect("running");
    slow.await.expect("no panic").expect("running");

    assert_eq!(
        Ledger::restore(&bytes).expect("restores").entries,
        vec![7],
        "the snapshot must see the finished turn, not the state before it"
    );
}

/// The store refuses a second writer, so two processes cannot both own one
/// actor's state. The claim is held for as long as the actor runs.
#[tokio::test]
async fn a_second_process_cannot_claim_a_running_actor() {
    let scratch = Scratch::new("busy");
    let files = Files::open(&scratch.0).await.expect("opens");
    let id = StateId::new("ledgers/sp-01").expect("valid");

    let _held = files.claim(&id).await.expect("claims");
    let error = files.claim(&id).await.expect_err("already claimed");
    assert!(
        matches!(error, keylink_persist::Error::Busy { .. }),
        "got {error:?}"
    );
}
