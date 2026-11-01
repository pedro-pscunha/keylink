//! `Persist<_, _, Deltas>`: a turn stores what it changed, not the whole state.
//!
//! The subject is one claim in `keylink/src/persist.rs`: that a turn costs the
//! size of its change. Every test below drives the real adapter, a state, and a
//! restart, and the ones that need to count bytes use `Memory`, a second adapter
//! for the port written in this file. `Files` pays an `fsync` per turn, and
//! 10,000 of those at `F_FULLFSYNC` would take about a minute.
//!
//! `tests/persist.rs` covers `Whole`, the failed-store refusal and `Busy`.
//! `keylink-persist/tests/versioned.rs` covers the log's rules from the library
//! side: gaps, migration of a stored delta, a log opened by a state that cannot
//! replay it, and a crash between a snapshot and the cut that follows it.

// The file adapter is this file's subject for the crash tests, and `Persist`
// needs `persist`. The same condition `tests/persist.rs` gates its whole file on.
#![cfg(feature = "persist-file")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keylink::prelude::*;
use keylink::{Caller, Deltas, Ended, Persist, Unanswered, Whole, Withheld};
use keylink_persist::file::Files;
use keylink_persist::versioned::{Compaction, TornTail};
use keylink_persist::{
    Changes, Claim, Durable, Error, Journaled, RestoreError, Seq, Snapshots, StateId, Tail,
};

keylink::service! {
    service Shelf;

    rpc Add(item: u64) -> u64;
    rpc Items() -> Vec<u64>;
    /// Records the item and **then** panics, so a half-applied turn that wrote
    /// its change into the buffer is reachable.
    rpc Bad(item: u64) -> u64;
}

// ─────────────────────────────────────────────────────────────────── the state

/// One change: an item was added. Thirty-two bytes, so the arithmetic in the
/// first test is readable.
#[derive(Clone)]
struct Added([u8; 32]);

impl Added {
    /// An item whose first eight bytes are `n` and whose rest is a fixed pattern.
    fn of(n: u64) -> Self {
        let mut bytes = [7_u8; 32];
        bytes[..8].copy_from_slice(&n.to_le_bytes());
        Self(bytes)
    }

    fn number(&self) -> u64 {
        let mut eight = [0_u8; 8];
        eight.copy_from_slice(&self.0[..8]);
        u64::from_le_bytes(eight)
    }
}

impl Durable for Added {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
            RestoreError::Malformed(format!("an item is 32 bytes, and this is {}", bytes.len()))
        })?;
        Ok(Self(bytes))
    }

    fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// A shelf of 32-byte items, and the changes the running turn made.
#[derive(Default)]
struct Tape {
    items: Vec<[u8; 32]>,
    changes: Changes<Added>,
}

impl Tape {
    /// A shelf that already holds `n` items, as if `n` turns had run.
    fn holding(n: u64) -> Self {
        Self {
            items: (0..n).map(|i| Added::of(i).0).collect(),
            changes: Changes::default(),
        }
    }

    /// What every handler does: change the state the way a restore would, then
    /// say so.
    fn put(&mut self, n: u64) {
        let delta = Added::of(n);
        self.apply(delta.clone());
        self.changes.record(delta);
    }
}

impl Journaled for Tape {
    type Delta = Added;

    fn changes(&self) -> &Changes<Added> {
        &self.changes
    }

    fn apply(&mut self, delta: Added) {
        self.items.push(delta.0);
    }
}

impl Durable for Tape {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.items.concat()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        if !bytes.len().is_multiple_of(32) {
            return Err(RestoreError::Malformed(format!(
                "a shelf is whole 32-byte items, and this is {} bytes",
                bytes.len()
            )));
        }
        Ok(Self {
            items: bytes.as_chunks::<32>().0.to_vec(),
            changes: Changes::default(),
        })
    }

    fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

impl shelf::Actor for Tape {
    async fn add(&mut self, _cx: Caller<'_>, item: u64) -> u64 {
        self.put(item);
        self.items.len() as u64
    }

    async fn items(&mut self, _cx: Caller<'_>) -> Vec<u64> {
        self.items
            .iter()
            .map(|bytes| Added(*bytes).number())
            .collect()
    }

    async fn bad(&mut self, _cx: Caller<'_>, item: u64) -> u64 {
        self.put(item);
        panic!("bad item");
    }
}

// ───────────────────────────────────────────────────────────────────── fixtures

/// A scratch directory that removes itself.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "keylink-journal-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }

    /// The journal beside the snapshot, built the way the adapter's documentation
    /// says the layout is.
    fn journal(&self) -> PathBuf {
        self.0.join("shelves").join("main.journal")
    }

    fn snapshot(&self) -> PathBuf {
        self.0.join("shelves").join("main.snapshot")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn files(name: &str) -> (Scratch, Files) {
    let scratch = Scratch::new(name);
    let files = Files::open(&scratch.0).await.expect("opens");
    (scratch, files)
}

fn id() -> StateId {
    StateId::new("shelves/main").expect("valid")
}

/// Open the shelf the way an application does, and report what recovery cut.
async fn open<S: Snapshots>(
    store: &S,
    when: Compaction,
) -> (Tape, Persist<Tape, S, Deltas>, Option<TornTail>) {
    let opened = Persist::<Tape, _, Deltas>::open(store, &id())
        .await
        .expect("claims and replays")
        .compaction(when);
    let torn = opened.torn_tail();
    let (state, saver) = opened.start_from(Tape::default).expect("adopts");
    (state, saver, torn)
}

/// The shelf's items after a restart, and what recovery cut.
async fn restart<S: Snapshots>(store: &S, when: Compaction) -> (Vec<u64>, Option<TornTail>) {
    let (state, _saver, torn) = open(store, when).await;
    let numbers = state
        .items
        .iter()
        .map(|bytes| Added(*bytes).number())
        .collect();
    (numbers, torn)
}

/// An actor over a shelf that was just opened, and the recovery's torn tail.
async fn start<S: Snapshots>(store: &S, when: Compaction) -> shelf::Handle<Tape> {
    let (state, saver, _torn) = open(store, when).await;
    shelf::setup(state).observe(saver).actor()
}

/// The compaction policy the on-disk tests share: fold when the log reaches the
/// snapshot, and not below 200 bytes, so a few 44-byte records fit between folds.
fn small() -> Compaction {
    Compaction::new(NonZeroU32::MIN, 200)
}

// ──────────────────────────────────────────────────── a second adapter, in memory

/// The port implemented from outside the crate that declares it, holding bytes in
/// a `Vec` and counting the ones it was handed.
///
/// `.mise/tasks/framing` exempts a file that **implements** `Claim`, and this one
/// does. It exists because the claim under test is about bytes per turn, and
/// counting them off a `Files` directory would be counting file sizes after the
/// fact, one `fsync` at a time.
#[derive(Clone, Default)]
struct Memory {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    snapshot: Option<Vec<u8>>,
    log: Vec<(Seq, Vec<u8>)>,
    /// Bytes handed to `store` and `append`.
    written: u64,
    /// How many times `store` ran.
    stores: u64,
    /// The next `append`, and only that one, returns `Err`.
    failing: bool,
    /// The next `append`, and only that one, returns `Err(Indeterminate)`: the
    /// record "may be stored".
    indeterminate: bool,
}

impl Memory {
    fn written(&self) -> u64 {
        self.inner.lock().expect("not poisoned").written
    }

    fn stores(&self) -> u64 {
        self.inner.lock().expect("not poisoned").stores
    }

    fn fail_the_next_append(&self) {
        self.inner.lock().expect("not poisoned").failing = true;
    }

    fn fail_the_next_append_without_being_able_to_undo_it(&self) {
        self.inner.lock().expect("not poisoned").indeterminate = true;
    }
}

impl Snapshots for Memory {
    type Claim = MemoryClaim;

    async fn claim(&self, _id: &StateId) -> keylink_persist::Result<MemoryClaim> {
        Ok(MemoryClaim {
            inner: Arc::clone(&self.inner),
        })
    }
}

/// [`Memory`]'s claim. Exclusivity is deliberately not modelled: the tests that
/// need it use `Files`.
struct MemoryClaim {
    inner: Arc<Mutex<Inner>>,
}

impl Claim for MemoryClaim {
    async fn load(&self) -> keylink_persist::Result<Option<Vec<u8>>> {
        Ok(self.inner.lock().expect("not poisoned").snapshot.clone())
    }

    async fn store(&mut self, bytes: &[u8]) -> keylink_persist::Result<()> {
        let mut inner = self.inner.lock().expect("not poisoned");
        inner.written += bytes.len() as u64;
        inner.stores += 1;
        inner.snapshot = Some(bytes.to_vec());
        Ok(())
    }

    async fn append(&mut self, seq: Seq, record: &[u8]) -> keylink_persist::Result<()> {
        let mut inner = self.inner.lock().expect("not poisoned");
        if std::mem::take(&mut inner.failing) {
            return Err(Error::Io(std::io::Error::other("the log said no, once")));
        }
        if std::mem::take(&mut inner.indeterminate) {
            return Err(Error::Indeterminate {
                append: std::io::Error::other("the log said no, once"),
                rollback: std::io::Error::other("and could not take it back"),
            });
        }
        inner.written += record.len() as u64;
        inner.log.push((seq, record.to_vec()));
        Ok(())
    }

    async fn recover(&mut self, from: Seq) -> keylink_persist::Result<Tail> {
        let inner = self.inner.lock().expect("not poisoned");
        Ok(Tail {
            earlier: inner.log.iter().filter(|(seq, _)| *seq < from).count() as u64,
            records: inner
                .log
                .iter()
                .filter(|(seq, _)| *seq >= from)
                .cloned()
                .collect(),
            torn: None,
        })
    }

    async fn truncate_before(&mut self, seq: Seq) -> keylink_persist::Result<()> {
        self.inner
            .lock()
            .expect("not poisoned")
            .log
            .retain(|(found, _)| *found >= seq);
        Ok(())
    }
}

// ──────────────────────────────────────────────── the limit, and that it is gone

/// **A turn costs the size of its change, however large the state has grown.**
///
/// Ten thousand turns of a shelf that ends at 320,000 bytes. Every turn that did
/// not fold the log wrote exactly what the first one did: a 12-byte record header
/// and the 32-byte delta, 44 bytes, whether the shelf held one item or nine
/// thousand. A whole-state store writes the whole state every turn, which is
/// 320,020 bytes at the end of this run.
///
/// The folds are what keep the log bounded, and they are counted rather than
/// assumed: the total over all turns, folds included, stays under three times
/// what the appends alone wrote. That is the policy's bound (`ratio` one: the
/// bytes written per byte appended stay at a small constant) read off a run.
///
/// **The identity check is the second half.** A restart gives back a state whose
/// encoding is byte for byte the live one's. This is the pattern the
/// [`Changes`] documentation tells an application to copy, and it is what catches
/// a handler that changed the state without recording it.
///
/// Made red by `scripts/break-once.sh`, replacing `Versioned::stage`'s
/// `Staged(Step::Append(record))` with `Staged(Step::Compact(Snapshot::of(state)))` —
/// every turn a fold. Watched: "10000 folds in 10000 turns: the policy folds far
/// too often".
#[tokio::test]
async fn deltas_keep_the_bytes_per_turn_constant_while_the_state_grows() {
    const TURNS: u64 = 10_000;
    const RECORD: u64 = 12 + 32;

    let memory = Memory::default();
    let shelf = start(&memory, Compaction::new(NonZeroU32::MIN, 0)).await;

    let mut folds = 0_u64;
    let mut appends = 0_u64;
    for item in 0..TURNS {
        let (written, stores) = (memory.written(), memory.stores());
        shelf.add(item).await.expect("running");
        let wrote = memory.written() - written;
        if memory.stores() > stores {
            folds += 1;
        } else {
            appends += 1;
            assert_eq!(
                wrote, RECORD,
                "turn {item} did not fold the log and wrote {wrote} bytes, not {RECORD}"
            );
        }
    }

    assert!(folds >= 1, "a log that is never folded is not bounded");
    assert!(
        folds < TURNS / 10,
        "{folds} folds in {TURNS} turns: the policy folds far too often"
    );
    assert_eq!(folds + appends, TURNS);
    assert!(
        memory.written() <= 3 * RECORD * TURNS,
        "{} bytes over {TURNS} turns is more than three times what the appends wrote",
        memory.written()
    );

    // The identity check.
    let live = shelf
        .with_state(|tape: &Tape| tape.snapshot())
        .await
        .expect("running");
    assert_eq!(live.len(), 32 * TURNS as usize, "the shelf kept every item");
    shelf.stop().await.expect("stops");

    let (state, _saver, torn) = open(&memory, small()).await;
    assert_eq!(torn, None);
    assert!(
        state.snapshot() == live,
        "a restart gave back a different state than the one that was running"
    );
}

/// A turn that changed nothing does no I/O at all.
///
/// A whole-state store pays its full price for a read. Here the drain finds
/// nothing and `stage` says so.
///
/// Made red by `scripts/break-once.sh`, replacing `Versioned::stage`'s
/// `if deltas.is_empty() {` with `if false {` — an empty turn encoded and
/// appended as a record of no deltas. Watched: `written` moves by the record
/// header.
#[tokio::test]
async fn a_turn_that_changed_nothing_writes_nothing() {
    let memory = Memory::default();
    let shelf = start(&memory, small()).await;
    shelf.add(1).await.expect("running");
    shelf.add(2).await.expect("running");

    let (written, stores) = (memory.written(), memory.stores());
    assert_eq!(shelf.items().await.expect("running"), vec![1, 2]);
    assert_eq!(
        (memory.written(), memory.stores()),
        (written, stores),
        "a turn that recorded no change must not touch the store"
    );
}

/// A turn whose append failed is refused: the caller is told it was not
/// committed, the actor ends, and the turn is not in the log.
///
/// Made red by `scripts/break-once.sh`, replacing `Versioned::commit`'s
/// `self.claim.append(self.next, &record).await?;` with
/// `let _ = self.claim.append(self.next, &record).await;` — an append that failed
/// and was answered anyway. Watched: the call returns `Ok`.
#[tokio::test]
async fn a_failed_append_refuses_the_turn_and_stores_nothing() {
    let memory = Memory::default();
    let shelf = start(&memory, small()).await;
    shelf
        .add(1)
        .await
        .expect("the first turn stores the snapshot");
    shelf.add(2).await.expect("an append");

    memory.fail_the_next_append();
    assert!(
        shelf.add(3).await.is_err(),
        "the append failed, so the turn was not committed"
    );
    match shelf.ended().await {
        Ended::Observer { name, .. } => assert_eq!(name, "persist"),
        other => panic!("got {other:?}"),
    }

    let (items, _) = restart(&memory, small()).await;
    assert_eq!(items, vec![1, 2], "the refused turn is not in the log");
}

/// Opening a journaled state as a whole one is refused, and says why. The other
/// way to read it is the snapshot alone: the state from before every record, with
/// the records dropped without a word.
#[tokio::test]
async fn a_journaled_state_does_not_open_under_the_whole_strategy() {
    let memory = Memory::default();
    let shelf = start(&memory, small()).await;
    for item in 1..=3 {
        shelf.add(item).await.expect("running");
    }
    shelf.stop().await.expect("stops");

    let error = Persist::<Tape, _, Whole>::open(&memory, &id())
        .await
        .expect_err("two records follow the snapshot");
    assert!(
        matches!(
            error,
            Error::Restore(RestoreError::UnreplayedLog { records: 2, .. })
        ),
        "got {error:?}"
    );
}

/// **An append that failed and could not be undone is not called a clean
/// refusal.** The record may be stored, so a caller told *it was not committed*
/// would retry and apply the work twice. It is told `Indeterminate` instead, by
/// the observer's name, and the actor ends all the same.
///
/// Made red by `scripts/break-once.sh`, replacing `Persist`'s
/// `Err(error @ keylink_persist::Error::Indeterminate { .. }) => {` arm's body
/// `Err(Stop::indeterminate(error))` with `Err(Stop::from(error))`. Watched: the
/// caller is told `Refused`, the sentence "it ran and was not committed".
#[tokio::test]
async fn an_append_that_cannot_be_undone_is_reported_as_indeterminate_to_the_caller() {
    let memory = Memory::default();
    let shelf = start(&memory, small()).await;
    shelf
        .add(1)
        .await
        .expect("the first turn stores the snapshot");
    shelf.add(2).await.expect("an append");

    memory.fail_the_next_append_without_being_able_to_undo_it();
    match shelf.add(3).await {
        Err(Unanswered::Turn(Withheld::Indeterminate { name })) => assert_eq!(name, "persist"),
        other => panic!("a caller must not be told a retry is safe: got {other:?}"),
    }
    match shelf.ended().await {
        Ended::Observer { name, fatal, .. } => assert_eq!((name, fatal), ("persist", false)),
        other => panic!("got {other:?}"),
    }
}

/// A journaled state has one store. A state another store already adopted is
/// refused when the second one tries to start from it, and two stores attached
/// to one actor are told apart at the first turn, loudly, instead of one of them
/// quietly finding the changes already taken.
///
/// Made red by `scripts/break-once.sh`, replacing `Opened::start_from`'s
/// `self.saver.durable.adopt(&state)?;` with `let _ = self.saver.durable.adopt(&state);`.
/// Watched: the second `start_from` succeeds.
#[tokio::test]
async fn a_journaled_state_cannot_have_two_stores() {
    let first = Memory::default();
    let second = Memory::default();

    let (state, saver_first) = Persist::<Tape, _, Deltas>::open(&first, &id())
        .await
        .expect("opens")
        .start_from(Tape::default)
        .expect("adopts");

    // The same state handed to a second store.
    let opened = Persist::<Tape, _, Deltas>::open(&second, &id())
        .await
        .expect("opens");
    let refused = opened.start_from(move || state);
    assert!(
        matches!(refused, Err(Error::ForeignChanges)),
        "got {:?}",
        refused.map(|_| ())
    );
    drop(saver_first);

    // Two stores on one actor: the second one's first turn is refused.
    let (state, saver_a) = Persist::<Tape, _, Deltas>::open(&first, &id())
        .await
        .expect("opens")
        .start_from(Tape::default)
        .expect("adopts");
    let (_unused, saver_b) = Persist::<Tape, _, Deltas>::open(&second, &id())
        .await
        .expect("opens")
        .start_from(Tape::default)
        .expect("adopts its own state");
    let shelf = shelf::setup(state)
        .observe(saver_a)
        .observe(saver_b)
        .actor();
    assert!(
        shelf.add(1).await.is_err(),
        "the second store does not own this state's changes"
    );
}

/// A journaled state that is persisted whole never records, so its buffer cannot
/// grow: nothing was going to drain it.
///
/// Made red by `scripts/break-once.sh`, replacing `Changes::record`'s
/// `if pending.owner != 0 {` with `if true {`. Watched: fifty changes pending.
#[tokio::test]
async fn a_journaled_state_under_whole_does_not_accumulate_changes() {
    let memory = Memory::default();
    let (state, saver) = Persist::<Tape, _, Whole>::open(&memory, &id())
        .await
        .expect("opens")
        .start_from(Tape::default);
    let shelf = shelf::setup(state).observe(saver).actor();
    for item in 0..50 {
        shelf.add(item).await.expect("running");
    }
    let buffer = shelf
        .with_state(|tape: &Tape| format!("{:?}", tape.changes))
        .await
        .expect("running");
    assert_eq!(buffer, "Changes { owned: false, pending: 0 }");
}

// ───────────────────────────────────────────────────────────── the crash points

/// Five posts, then a crash in the middle of the last append: the torn record is
/// cut and its size reported, and nothing acknowledged is lost.
///
/// The first turn stores the snapshot, so the five posts are one snapshot and four
/// records. Five bytes cut from the last record leave three, so recovery keeps
/// them and reports `after: Seq(3)`.
///
/// Made red by `scripts/break-once.sh`, deleting `recover_blocking`'s
/// `file.set_len(scanned.intact as u64)?;`. Watched: the file is not shorter by
/// the bytes reported; and without the first assertion the append that follows
/// lands after the garbage and the next open is `NotAJournal`.
#[tokio::test]
async fn a_torn_tail_is_cut_on_restore_and_reported() {
    let (scratch, files) = files("torn").await;
    let shelf = start(&files, small()).await;
    for item in 1..=5 {
        shelf.add(item).await.expect("running");
    }
    shelf.stop().await.expect("stops");

    let whole = std::fs::metadata(scratch.journal()).expect("reads").len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(scratch.journal())
        .expect("opens")
        .set_len(whole - 5)
        .expect("truncates");

    let (items, torn) = restart(&files, small()).await;
    let torn = torn.expect("a torn tail is reported");
    assert_eq!(items, vec![1, 2, 3, 4], "the fifth turn was never answered");
    assert_eq!(torn.after, Seq::new(3));
    assert_eq!(
        std::fs::metadata(scratch.journal()).expect("reads").len(),
        whole - 5 - torn.bytes,
        "the file is shorter by exactly the bytes reported"
    );

    // And the state carries on from there: a new post, and a clean reopen.
    let shelf = start(&files, small()).await;
    shelf.add(5).await.expect("running");
    shelf.stop().await.expect("stops");
    let (items, torn) = restart(&files, small()).await;
    assert_eq!(items, vec![1, 2, 3, 4, 5]);
    assert_eq!(torn, None, "nothing is left to cut");
}

/// A damaged record with intact data after it is acknowledged data gone bad, and
/// the restore refuses rather than handing back a state with a hole in it.
///
/// Made red by `scripts/break-once.sh`, replacing the `Corrupt` return in
/// `scan` with a `break` — every failed checksum a torn tail. Watched: the open
/// succeeds where it must refuse, reporting `torn: Some(TornTail { bytes: 180,
/// after: Seq(1) })`, so three answered turns are gone behind a report.
#[tokio::test]
async fn a_damaged_record_before_the_end_refuses_the_restore() {
    let (scratch, files) = files("damaged").await;
    let shelf = start(&files, small()).await;
    for item in 1..=5 {
        shelf.add(item).await.expect("running");
    }
    shelf.stop().await.expect("stops");

    // One byte inside the second of the four records. Each frame is 20 bytes of
    // header and a 44-byte record, after the 8-byte file header.
    let mut bytes = std::fs::read(scratch.journal()).expect("reads");
    bytes[8 + 64 + 20 + 12] ^= 0xFF;
    std::fs::write(scratch.journal(), &bytes).expect("writes");

    let error = Persist::<Tape, _, Deltas>::open(&files, &id())
        .await
        .expect_err("the second record is damaged and the log goes on past it");
    assert!(
        matches!(
            error,
            Error::NotAJournal {
                fault: keylink_persist::JournalFault::Corrupt { after },
                ..
            } if after == Seq::new(1)
        ),
        "got {error:?}"
    );
}

/// A log is folded into a snapshot while the state runs, and the state restores
/// across any number of folds. Then the crash that matters: a fold whose snapshot
/// reached the disk and whose cut did not.
///
/// The first half runs 30 turns under a policy that folds every few and checks
/// that folds happened (the snapshot changed and the journal shrank) and that a
/// restart gives back every item once. The second half takes the journal as it
/// stood just before a fold and puts it back after — the file system state "the
/// snapshot was renamed in, the old records were not cut". Recovery must apply
/// each record once, and finish the cut.
///
/// Made red by `scripts/break-once.sh`, replacing `Versioned::open`'s
/// `let from = Seq::new(through.get() + 1);` with `let from = Seq::new(1);` —
/// replaying from the start of the log. Watched: the first restart after a fold
/// fails, because the journal now begins at record 5 and the open expected 1
/// (`Gap`). The crash half is the same defect without the gap: records the
/// snapshot contains are read again, and `records_a_snapshot_already_contains_are_skipped_and_cut`
/// in `keylink-persist/tests/versioned.rs` watches that one on its own.
#[tokio::test]
async fn a_state_restores_across_a_compaction_and_one_a_crash_interrupted() {
    // ── folds happen, and nothing is lost across them
    let (scratch, files) = files("folds").await;
    let shelf = start(&files, small()).await;
    let mut folds = 0;
    let mut shrank = false;
    let mut snapshot = std::fs::read(scratch.snapshot()).unwrap_or_default();
    let mut journal = 0;
    for item in 1..=30 {
        shelf.add(item).await.expect("running");
        let now = std::fs::read(scratch.snapshot()).expect("the first turn stored one");
        if now != snapshot {
            folds += 1;
            snapshot = now;
        }
        let len = std::fs::metadata(scratch.journal()).map_or(0, |meta| meta.len());
        shrank |= len < journal;
        journal = len;
    }
    shelf.stop().await.expect("stops");
    assert!(
        folds >= 3,
        "{folds} snapshots in 30 turns under a 200-byte floor"
    );
    assert!(shrank, "the journal never shrank, so no fold cut it");

    let (items, torn) = restart(&files, small()).await;
    assert_eq!(items, (1..=30).collect::<Vec<_>>(), "every item, once");
    assert_eq!(torn, None);

    // ── the crash between a snapshot and its cut
    let (scratch, files) = self::files("fold-crash").await;
    let shelf = start(&files, small()).await;
    let mut journal_before = Vec::new();
    for item in 1..=40 {
        journal_before = std::fs::read(scratch.journal()).unwrap_or_default();
        let snapshot_before = std::fs::read(scratch.snapshot()).unwrap_or_default();
        shelf.add(item).await.expect("running");
        if item > 1 && std::fs::read(scratch.snapshot()).expect("reads") != snapshot_before {
            break; // this turn folded
        }
    }
    shelf.stop().await.expect("stops");
    let folded_at = restart(&files, small()).await.0.len();
    assert!(
        journal_before.len() > 8,
        "the log held records just before the fold"
    );

    // The snapshot is the new one and the journal is the old one.
    std::fs::write(scratch.journal(), &journal_before).expect("puts the old journal back");
    let (items, torn) = restart(&files, small()).await;
    assert_eq!(
        items,
        (1..=folded_at as u64).collect::<Vec<_>>(),
        "each record applied once: the snapshot already contains them"
    );
    assert_eq!(torn, None);
    assert_eq!(
        std::fs::metadata(scratch.journal()).expect("reads").len(),
        8,
        "the cut the crash interrupted was finished by the open: a header and nothing else"
    );
}

/// A turn that panicked appends nothing and drops what it recorded.
///
/// `Bad` records its change and then panics, so the buffer holds a change that
/// the state half-applied. The journal is byte-identical before and after, the
/// actor ended as `Panicked`, and a restart gives back the turns before it.
///
/// Made red by `scripts/break-once.sh`, replacing `Persist`'s `Panicked` arm for
/// `Deltas` with one that stages and commits the turn. Watched: item 3 comes back
/// from the restart.
#[tokio::test]
async fn a_panicked_turn_appends_nothing() {
    let (scratch, files) = files("panicked").await;
    let shelf = start(&files, small()).await;
    shelf.add(1).await.expect("running");
    shelf.add(2).await.expect("running");
    let journal = std::fs::read(scratch.journal()).expect("one record was appended");

    // Spawned, because the panic is resumed in whoever awaited the call.
    let boom = {
        let shelf = shelf.clone();
        tokio::spawn(async move { shelf.bad(3).await })
    };
    assert!(boom
        .await
        .expect_err("the panic reaches the caller")
        .is_panic());

    // The disk is read before `ended` is awaited, for the reason
    // `tests/persist.rs` gives: an adapter that accepted the turn would not end
    // the actor, and a red test would hang instead of failing.
    assert_eq!(
        std::fs::read(scratch.journal()).expect("reads"),
        journal,
        "the half-applied turn must not be in the journal"
    );
    match shelf.ended().await {
        Ended::Panicked { message } => assert!(message.contains("bad item"), "got {message}"),
        other => panic!("got {other:?}"),
    }

    let (items, _) = restart(&files, small()).await;
    assert_eq!(items, vec![1, 2]);
}

// ──────────────────────────────────────────────────────────────── the measurement

/// What `Whole` and `Deltas` cost a turn, side by side, on this machine.
///
/// **Ignored**, because it measures and asserts nothing about time. Run it with
/// `cargo test -p keylink --test journal -- --ignored --nocapture`.
///
/// Both strategies over `Files`, three runs each, **alternating which goes first**
/// so that neither gets the cold disk. The shelf is pre-filled to 1, 1,000 and
/// 100,000 items, and one warm-up turn runs first (for `Deltas` it is the turn
/// that stores the first snapshot, which is O(state) and not what is measured).
/// Then 200 timed turns. Bytes per turn are read off the files and are a mean over the
/// timed turns for both: the mean snapshot size for `Whole` (every turn rewrites
/// all of it, and the shelf grows by one item a turn), the journal's growth divided
/// by the turns for `Deltas`.
///
/// **What this does not cover**: Linux (the disk here is APFS, where `sync_data`
/// is `F_FULLFSYNC`), a loaded disk, and tail latency. 200 turns at 44 bytes never
/// reach a fold at any of these sizes, so **the spike of a compaction turn is
/// not in these figures by construction**. It is O(state), and it costs four
/// flushes and an in-turn encode.
#[tokio::test]
#[ignore = "measures; run it with --ignored --nocapture"]
async fn the_per_turn_cost_whole_against_deltas() {
    const TURNS: u64 = 200;

    for size in [1_u64, 1_000, 100_000] {
        for run in 0..3 {
            let order = if run % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            };
            for deltas in order {
                let name = format!("ab-{size}-{run}-{deltas}");
                let (scratch, files) = files(&name).await;
                let (micros, bytes) = if deltas {
                    measure_deltas(&scratch, &files, size, TURNS).await
                } else {
                    measure_whole(&scratch, &files, size, TURNS).await
                };
                println!(
                    "items={size:>7} run={run} {:>6}: {micros:>10.1} us/turn {bytes:>10} bytes/turn",
                    if deltas { "Deltas" } else { "Whole" }
                );
            }
        }
    }
}

async fn measure_deltas(scratch: &Scratch, files: &Files, size: u64, turns: u64) -> (f64, u64) {
    let opened = Persist::<Tape, _, Deltas>::open(files, &id())
        .await
        .expect("opens");
    let (state, saver) = opened.start_from(|| Tape::holding(size)).expect("adopts");
    let shelf = shelf::setup(state).observe(saver).actor();
    shelf
        .add(size)
        .await
        .expect("warm-up: stores the first snapshot");
    shelf
        .add(size + 1)
        .await
        .expect("warm-up: the first record");

    let journal = std::fs::metadata(scratch.journal()).expect("reads").len();
    let started = Instant::now();
    for item in 0..turns {
        shelf.add(size + 2 + item).await.expect("running");
    }
    let elapsed = started.elapsed();
    let grown = std::fs::metadata(scratch.journal()).expect("reads").len() - journal;
    shelf.stop().await.expect("stops");
    (elapsed.as_secs_f64() * 1e6 / turns as f64, grown / turns)
}

async fn measure_whole(scratch: &Scratch, files: &Files, size: u64, turns: u64) -> (f64, u64) {
    let opened = Persist::<Tape, _, Whole>::open(files, &id())
        .await
        .expect("opens");
    let (state, saver) = opened.start_from(|| Tape::holding(size));
    let shelf = shelf::setup(state).observe(saver).actor();
    shelf.add(size).await.expect("warm-up");

    let before = std::fs::metadata(scratch.snapshot()).expect("reads").len();
    let started = Instant::now();
    for item in 0..turns {
        shelf.add(size + 1 + item).await.expect("running");
    }
    let elapsed = started.elapsed();
    let after = std::fs::metadata(scratch.snapshot()).expect("reads").len();
    shelf.stop().await.expect("stops");

    // Every timed turn rewrote the whole snapshot, and the shelf grows by one item
    // a turn, so the snapshot's size grows linearly across the run. The bytes
    // written PER TURN are therefore the mean of the sizes after turns 1 to `turns`,
    // which is what this returns. (It used to return `after`, the last turn's size,
    // beside a Deltas figure that was a mean: two different quantities in one column.)
    let mean = before + (after - before) * (turns + 1) / (2 * turns);
    (elapsed.as_secs_f64() * 1e6 / turns as f64, mean)
}
