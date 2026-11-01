//! The frame and the migration chain, from outside the crate.
//!
//! Outside on purpose, and not only for tidiness. Every `Durable` here is under
//! the rule a caller's is — it reports [`RestoreError::Malformed`] and nothing
//! else — so the ledgers below are the shape a caller actually writes.
//!
//! **This file is not the guard for that rule, and an earlier version of this
//! comment claimed it was.** `#[non_exhaustive]` forbids *construction* and
//! nothing else. These tests only ever match the loader's verdicts, always with
//! `..`, which is exactly as legal on an ordinary variant — delete the three
//! attributes and the whole suite stays green. The guard is where the boundary
//! is: the `compile_fail` doctests on each variant in
//! `keylink-persist/src/lib.rs`, because a doctest compiles as its own crate.
//!
//! Those fences carry **no error code**, and an earlier version of this comment
//! said they carried `E0639`. They fail with it — `cannot create non-exhaustive
//! variant using struct expression` — and it is named in prose there, because
//! rustdoc does not check the code after `compile_fail` at all. So each fence
//! asserts one thing: this does not compile. What keeps it from asserting that
//! for the wrong reason — a rename making a variant vanish rather than the
//! attribute holding — is **this file**, which names all three variants from
//! outside the crate. Not the guard for the attribute, and the guard that the
//! attribute's guard is testing the right names.
//!
//! Every snapshot is written and read through the real file adapter, and the
//! two hostile inputs — an unframed file and a frame from the future — are put
//! on the disk with `std::fs::write`, because that is precisely what they are:
//! bytes that were already there when this build arrived.

#![cfg(feature = "file")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use keylink_persist::file::Files;
use keylink_persist::versioned::{FrameFault, Snapshot, Versioned, FRAME_LEN};
use keylink_persist::{Durable, Error, RestoreError, Snapshots, StateId};

// ────────────────────────────────────────────── three versions of one ledger

/// v1: the entries, eight little-endian bytes each.
#[derive(Debug, PartialEq, Eq)]
struct LedgerV1 {
    entries: Vec<u64>,
}

/// v2: a running total in front of the entries.
///
/// The total is what makes the v1 → v2 hop *observable*. A loader that skipped
/// it would hand back a ledger whose first entry had been read as the total,
/// and every assertion below would notice.
#[derive(Debug, PartialEq, Eq)]
struct LedgerV2 {
    total: u64,
    entries: Vec<u64>,
}

/// v3: v2, with the entry count appended.
#[derive(Debug, PartialEq, Eq)]
struct LedgerV3 {
    total: u64,
    entries: Vec<u64>,
    count: u64,
}

/// A ledger with a hole in its chain: it is version 3 and knows the second hop
/// only. Loading v1 bytes into it must stop at the gap rather than jump it.
#[derive(Debug, PartialEq, Eq)]
struct LedgerWithAGap {
    entries: Vec<u64>,
}

// ── the encodings, shared so the hops below are the only interesting part ──

fn entries_to_bytes(entries: &[u64]) -> Vec<u8> {
    entries.iter().flat_map(|n| n.to_le_bytes()).collect()
}

fn entries_from_bytes(bytes: &[u8]) -> Result<Vec<u64>, RestoreError> {
    if !bytes.len().is_multiple_of(8) {
        return Err(RestoreError::Malformed(format!(
            "a ledger is whole u64s, and this is {} bytes",
            bytes.len()
        )));
    }
    Ok(bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| u64::from_le_bytes(*c))
        .collect())
}

/// v1 → v2: read the entries, put their sum in front. The only hop that has to
/// look at the payload rather than append to it.
fn v1_to_v2(bytes: &[u8]) -> Result<Vec<u8>, RestoreError> {
    let entries = entries_from_bytes(bytes)?;
    let total: u64 = entries.iter().sum();
    let mut out = total.to_le_bytes().to_vec();
    out.extend_from_slice(bytes);
    Ok(out)
}

/// v2 → v3: append the entry count. Everything before it is already right.
///
/// The `ok_or_else` is not ceremony. `bytes.get(8..).unwrap_or_default()` reads
/// the same and turns a v2 snapshot too short to hold its own total into a v3
/// with a count of zero — a migration that invents a state rather than
/// reporting one, which is the failure `Malformed` exists for.
fn v2_to_v3(bytes: &[u8]) -> Result<Vec<u8>, RestoreError> {
    let after_total = bytes.get(8..).ok_or_else(|| {
        RestoreError::Malformed(format!(
            "a v2 ledger opens with a total, and this is {} bytes",
            bytes.len()
        ))
    })?;
    let mut out = bytes.to_vec();
    out.extend_from_slice(&(entries_from_bytes(after_total)?.len() as u64).to_le_bytes());
    Ok(out)
}

impl Durable for LedgerV1 {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        entries_to_bytes(&self.entries)
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        Ok(Self {
            entries: entries_from_bytes(bytes)?,
        })
    }

    /// Version 1 is the first, so there is no older version to come from. The
    /// one line of boilerplate the trait refuses to default away, which is the
    /// price of a v3 author not being able to forget theirs.
    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

impl Durable for LedgerV2 {
    const VERSION: u32 = 2;

    fn snapshot(&self) -> Vec<u8> {
        let mut out = self.total.to_le_bytes().to_vec();
        out.extend_from_slice(&entries_to_bytes(&self.entries));
        out
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        let total = bytes
            .get(..8)
            .and_then(|head| head.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or_else(|| {
                RestoreError::Malformed(format!(
                    "a v2 ledger opens with a total, got {} bytes",
                    bytes.len()
                ))
            })?;
        Ok(Self {
            total,
            entries: entries_from_bytes(&bytes[8..])?,
        })
    }

    fn migrate(from: u32, bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(match from {
            1 => Some(v1_to_v2(bytes)?),
            _ => None,
        })
    }
}

impl Durable for LedgerV3 {
    const VERSION: u32 = 3;

    fn snapshot(&self) -> Vec<u8> {
        let mut out = self.total.to_le_bytes().to_vec();
        out.extend_from_slice(&entries_to_bytes(&self.entries));
        out.extend_from_slice(&self.count.to_le_bytes());
        out
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        // total, then entries, then count: at least two u64s.
        if bytes.len() < 16 {
            return Err(RestoreError::Malformed(format!(
                "a v3 ledger is a total, entries and a count, and this is {} bytes",
                bytes.len()
            )));
        }
        let numbers = entries_from_bytes(bytes)?;
        let total = numbers[0];
        let count = numbers[numbers.len() - 1];
        Ok(Self {
            total,
            entries: numbers[1..numbers.len() - 1].to_vec(),
            count,
        })
    }

    fn migrate(from: u32, bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(match from {
            1 => Some(v1_to_v2(bytes)?),
            2 => Some(v2_to_v3(bytes)?),
            _ => None,
        })
    }
}

impl Durable for LedgerWithAGap {
    const VERSION: u32 = 3;

    fn snapshot(&self) -> Vec<u8> {
        entries_to_bytes(&self.entries)
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        Ok(Self {
            entries: entries_from_bytes(bytes)?,
        })
    }

    /// The second hop only. The first is missing, which is the whole point.
    fn migrate(from: u32, bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(match from {
            2 => Some(v2_to_v3(bytes)?),
            _ => None,
        })
    }
}

// ────────────────────────────────────────────────────────────── the scaffold

/// A scratch directory that removes itself.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "keylink-persist-versioned-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }

    /// Where the file adapter keeps `ledgers/sp-01`. Named here so the tests
    /// that plant hostile bytes plant them exactly where a load will look.
    fn snapshot_path(&self) -> std::path::PathBuf {
        self.0.join("ledgers").join("sp-01.snapshot")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn id() -> StateId {
    StateId::new("ledgers/sp-01").expect("a valid id")
}

async fn open(scratch: &Scratch) -> Files {
    Files::open(&scratch.0).await.expect("opens")
}

/// Build the header this build writes, for a version this build may not
/// understand. Deliberately spelled out here rather than reached for through
/// the crate: a test that used the crate's own encoder to build its input would
/// stay green through a layout change that broke every file on disk.
fn framed(version: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = b"klsnap".to_vec();
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Write bytes straight into the adapter's file, the way a previous build left
/// them. The claim is taken and dropped around it so the directory exists.
async fn plant(scratch: &Scratch, files: &Files, bytes: &[u8]) {
    drop(files.claim(&id()).await.expect("claims"));
    std::fs::write(scratch.snapshot_path(), bytes).expect("plants");
}

// ──────────────────────────────────────────────────────────────── the frame

/// The header is on the disk, in the shape the module documents. Made red by
/// having `Versioned::store` pass the payload to the claim unwrapped.
#[tokio::test]
async fn what_is_stored_carries_a_header() {
    let scratch = Scratch::new("header");
    let files = open(&scratch).await;

    let mut state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let ledger = LedgerV1 {
        entries: vec![1, 2, 3],
    };
    state.store(&Snapshot::of(&ledger)).await.expect("stores");

    let on_disk = std::fs::read(scratch.snapshot_path()).expect("reads");
    assert_eq!(&on_disk[..6], b"klsnap", "the magic is the first six bytes");
    assert_eq!(&on_disk[6..8], &1u16.to_le_bytes(), "the layout version");
    assert_eq!(&on_disk[8..12], &1u32.to_le_bytes(), "LedgerV1::VERSION");
    assert_eq!(&on_disk[12..], &ledger.snapshot()[..], "then the payload");
    assert_eq!(on_disk.len(), FRAME_LEN + 24);
}

/// A snapshot survives the claim that wrote it, which is the whole port, now
/// with a version stamp in front.
///
/// Made red by `scripts/break-once.sh`, replacing `FileClaim::store`'s
/// `std::fs::rename(&temp, &snapshot)` with `std::fs::rename(&temp, &temp)` — the
/// bytes written and fsynced into the temporary file, and the atomic step aimed at
/// itself, so nothing is ever published under the snapshot's name. That is the
/// defect the ordering exists to prevent and it succeeds silently. Watched: left
/// `None` against right `Some(LedgerV1 { entries: [4, 5] })` at the second claim.
#[tokio::test]
async fn a_state_survives_the_claim_that_wrote_it() {
    let scratch = Scratch::new("survives");
    let files = open(&scratch).await;

    {
        let mut state = Versioned::new(files.claim(&id()).await.expect("claims"));
        assert_eq!(
            state.load::<LedgerV1>().await.expect("loads"),
            None,
            "nothing stored yet is the first run, not a failure"
        );
        state
            .store(&Snapshot::of(&LedgerV1 {
                entries: vec![4, 5],
            }))
            .await
            .expect("stores");
    }

    // A new claim is a new process, as far as the store can tell.
    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    assert_eq!(
        state.load::<LedgerV1>().await.expect("loads"),
        Some(LedgerV1 {
            entries: vec![4, 5]
        })
    );
}

// ─────────────────────────────────────────────────────────── the three cases

/// One hop. Made red by having the loader call `restore` without walking the
/// chain.
#[tokio::test]
async fn v1_bytes_load_into_a_v2_type_through_one_hop() {
    let scratch = Scratch::new("one-hop");
    let files = open(&scratch).await;

    {
        let mut state = Versioned::new(files.claim(&id()).await.expect("claims"));
        state
            .store(&Snapshot::of(&LedgerV1 {
                entries: vec![10, 20, 30],
            }))
            .await
            .expect("stores");
    }

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    assert_eq!(
        state.load::<LedgerV2>().await.expect("loads"),
        Some(LedgerV2 {
            // The total exists only because the hop computed it. A loader that
            // skipped the hop would read 10 as the total and lose an entry.
            total: 60,
            entries: vec![10, 20, 30],
        })
    );
}

/// Two hops, and the v1 → v2 arm is what produces the total, so a chain that
/// jumped straight to the last hop could not pass this.
///
/// Made red by `scripts/break-once.sh`, replacing `decode`'s
/// `for from in stored..D::VERSION {` with `for from in (D::VERSION - 1)..D::VERSION {`
/// — the jump the sentence above says could not pass. Watched: left
/// `Some(LedgerV3 { total: 7, entries: [8], count: 1 })` against right
/// `Some(LedgerV3 { total: 15, entries: [7, 8], count: 2 })`. The wrong answer is
/// the interesting part: v1 bytes read as v2 bytes are a plausible v2, so the loss
/// is an entry rather than an error.
#[tokio::test]
async fn v1_bytes_load_into_a_v3_type_through_two_hops() {
    let scratch = Scratch::new("two-hops");
    let files = open(&scratch).await;

    {
        let mut state = Versioned::new(files.claim(&id()).await.expect("claims"));
        state
            .store(&Snapshot::of(&LedgerV1 {
                entries: vec![7, 8],
            }))
            .await
            .expect("stores");
    }

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    assert_eq!(
        state.load::<LedgerV3>().await.expect("loads"),
        Some(LedgerV3 {
            total: 15, // from the v1 → v2 arm
            entries: vec![7, 8],
            count: 2, // from the v2 → v3 arm
        }),
        "both arms must run, in order"
    );
}

/// The case people get wrong. A v9 snapshot met by a v1 build is refused, and
/// what is on the disk is exactly what was there before.
///
/// Made red by dropping the `stored > D::VERSION` comparison in `decode`.
#[tokio::test]
async fn a_snapshot_from_the_future_is_refused_and_left_alone() {
    let scratch = Scratch::new("future");
    let files = open(&scratch).await;

    let from_the_future = framed(9, &[0xab; 40]);
    plant(&scratch, &files, &from_the_future).await;

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let error = state
        .load::<LedgerV1>()
        .await
        .expect_err("a v9 snapshot is not a v1 one");

    assert!(
        matches!(
            error,
            Error::Restore(RestoreError::FromTheFuture {
                stored: 9,
                ours: 1,
                ..
            })
        ),
        "got {error:?}"
    );

    assert_eq!(
        std::fs::read(scratch.snapshot_path()).expect("reads"),
        from_the_future,
        "a refused load must not truncate what it could not read"
    );
}

/// A gap is a stop, not a jump. `LedgerWithAGap` is version 3 and knows the
/// second hop only, so v1 bytes stop at the first.
///
/// Made red by having the loader treat a missing hop as "nothing to do".
#[tokio::test]
async fn a_gap_in_the_chain_names_the_missing_hop() {
    let scratch = Scratch::new("gap");
    let files = open(&scratch).await;

    {
        let mut state = Versioned::new(files.claim(&id()).await.expect("claims"));
        state
            .store(&Snapshot::of(&LedgerV1 { entries: vec![1] }))
            .await
            .expect("stores");
    }

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let error = state
        .load::<LedgerWithAGap>()
        .await
        .expect_err("there is no hop from 1");

    assert!(
        matches!(
            error,
            Error::Restore(RestoreError::NoMigration { from: 1, .. })
        ),
        "got {error:?}"
    );
}

// ──────────────────────────────────── the one that matters more than the rest

/// **The silent-corruption case.** A snapshot written before this crate framed
/// anything is 24 raw bytes, and `LedgerV1::restore` accepts any multiple of
/// eight — so a loader without a magic would read the twelve-byte header out of
/// the payload and hand back a ledger built from the remainder, with no error
/// anywhere.
///
/// Made red by deleting the magic comparison in `unframe`.
#[tokio::test]
async fn a_raw_unframed_snapshot_is_refused_rather_than_mis_parsed() {
    let scratch = Scratch::new("unframed");
    let files = open(&scratch).await;

    // Exactly what the pre-framing crate left on disk: the payload, alone.
    let legacy = LedgerV1 {
        entries: vec![11, 22, 33],
    }
    .snapshot();
    assert_eq!(legacy.len(), 24, "a valid payload of a plausible length");
    plant(&scratch, &files, &legacy).await;

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let error = state
        .load::<LedgerV1>()
        .await
        .expect_err("unframed bytes are not a snapshot this build can read");

    assert!(
        matches!(
            error,
            Error::Restore(RestoreError::NotAFrame {
                fault: FrameFault::WrongMagic,
                ..
            })
        ),
        "got {error:?}"
    );
}

/// Made red by `scripts/break-once.sh`, replacing `unframe`'s
/// `fault(FrameFault::TooShort { len: bytes.len() })` with
/// `fault(FrameFault::WrongMagic)` — still refused, refused for a reason that is
/// false about the file. Watched: "got Restore(NotAFrame { fault: WrongMagic })".
/// Which fault a short file gets is the whole of what this pins; the refusal itself
/// is `TooShort`'s only exit and cannot be edited away without the header's
/// `try_into` failing anyway.
#[tokio::test]
async fn a_file_too_short_to_be_a_frame_is_refused() {
    let scratch = Scratch::new("short");
    let files = open(&scratch).await;
    plant(&scratch, &files, b"klsna").await;

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let error = state.load::<LedgerV1>().await.expect_err("five bytes");
    assert!(
        matches!(
            error,
            Error::Restore(RestoreError::NotAFrame {
                fault: FrameFault::TooShort { len: 5 },
                ..
            })
        ),
        "got {error:?}"
    );
}

/// A header layout this build does not write is refused as a frame fault and
/// not as a payload version — the two numbers are separate, and so are the two
/// verdicts.
///
/// Made red by `scripts/break-once.sh`, replacing `unframe`'s
/// `match layout.cmp(&LAYOUT) {` with `match Ordering::Equal {` — the layout check
/// deleted while everything downstream still runs. Watched: this fails at the
/// `expect_err`, "layout 2: Some(LedgerV1 { entries: [0] })". A build that skipped
/// this check does not error; it hands back a ledger it decoded from a header it
/// does not understand.
#[tokio::test]
async fn a_frame_layout_from_the_future_is_refused() {
    let scratch = Scratch::new("layout");
    let files = open(&scratch).await;

    let mut newer = framed(1, &[0; 8]);
    newer[6] = 2;
    plant(&scratch, &files, &newer).await;

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let error = state.load::<LedgerV1>().await.expect_err("layout 2");
    assert!(
        matches!(
            error,
            Error::Restore(RestoreError::NotAFrame {
                fault: FrameFault::LayoutFromTheFuture { stored: 2, ours: 1 },
                ..
            })
        ),
        "got {error:?}"
    );
}

/// The other side of that comparison, and the side a single `!=` got wrong: a
/// layout *older* than this build's is not a layout from the future. Which
/// verdict fires is unobservable while `LAYOUT` is 1 and nothing ever wrote 0 —
/// and it stops being unobservable on the day `LAYOUT` becomes 2, when every
/// file already on disk takes this arm and the wrong name would have refused
/// all of them with a reason that was false about all of them.
///
/// Layout 0 is also what an unframed file reads here. It does not reach this
/// check in practice — the magic is tested first and
/// `a_raw_unframed_snapshot_is_refused_rather_than_mis_parsed` is what proves
/// that — so this is the header's second refusal rather than its first.
///
/// Made red by putting `layout != LAYOUT` back: this then reports
/// `LayoutFromTheFuture` and the match below fails.
#[tokio::test]
async fn a_frame_layout_older_than_this_build_is_refused_as_old() {
    let scratch = Scratch::new("layout-old");
    let files = open(&scratch).await;

    let mut older = framed(1, &[0; 8]);
    older[6] = 0;
    plant(&scratch, &files, &older).await;

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let error = state.load::<LedgerV1>().await.expect_err("layout 0");
    assert!(
        matches!(
            error,
            Error::Restore(RestoreError::NotAFrame {
                fault: FrameFault::LayoutTooOld { stored: 0, ours: 1 },
                ..
            })
        ),
        "got {error:?}"
    );
}

/// What a `Durable` reports about its own bytes reaches the caller unchanged,
/// and is the only variant it can report.
///
/// Made red by `scripts/break-once.sh`, replacing `decode`'s
/// `return D::restore(payload);` with a `map_err` that rewrites whatever the state
/// said as `RestoreError::Malformed("bad snapshot")` — this crate substituting its
/// own summary for the one report that knows the payload's shape. Watched: "got
/// "bad snapshot"". The assertion is `contains`, so it is the state's own words
/// that are pinned, not the enum variant, which is the half a `matches!` would miss.
#[tokio::test]
async fn what_the_state_says_about_its_own_bytes_is_what_the_caller_sees() {
    let scratch = Scratch::new("malformed");
    let files = open(&scratch).await;
    plant(&scratch, &files, &framed(1, b"seven!!")).await;

    let state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let error = state.load::<LedgerV1>().await.expect_err("not whole u64s");
    let Error::Restore(RestoreError::Malformed(said)) = error else {
        panic!("got {error:?}");
    };
    assert!(said.contains("7 bytes"), "got {said:?}");
}

// ──────────────────────────────────────────────── the limit counts the frame

/// The number in [`Error::TooLarge`] is what reached the adapter, which is the
/// payload plus [`FRAME_LEN`]. This test exists because the alternative — a
/// caller told "the snapshot is 128 bytes and the limit is 128" while being
/// refused — is a number nobody can act on.
///
/// Made red by `scripts/break-once.sh`, replacing `FileClaim::store`'s
/// `let offered = bytes.len() as u64;` with `let offered = (bytes.len() - 12) as u64;`
/// — the limit measured against the payload while the disk takes the frame, which is
/// the off-by-a-header this test is named for. Watched: the second store is
/// **accepted**, so it fails at the `expect_err` with "over the limit: ()". A limit
/// that undercounts does not report a wrong number; it lets a write through.
#[tokio::test]
async fn the_limit_counts_the_frame_and_the_error_says_which_number_it_is() {
    let scratch = Scratch::new("limit");
    let files = open(&scratch).await;
    let files = files.limit((FRAME_LEN + 16) as u64);

    let mut state = Versioned::new(files.claim(&id()).await.expect("claims"));

    // Two entries: sixteen bytes of payload, and the frame fills the limit
    // exactly.
    state
        .store(&Snapshot::of(&LedgerV1 {
            entries: vec![1, 2],
        }))
        .await
        .expect("exactly at the limit");

    // One more entry is eight bytes over.
    let error = state
        .store(&Snapshot::of(&LedgerV1 {
            entries: vec![1, 2, 3],
        }))
        .await
        .expect_err("over the limit");

    assert!(
        matches!(
            error,
            Error::TooLarge { offered, limit }
                if offered == (FRAME_LEN + 24) as u64 && limit == (FRAME_LEN + 16) as u64
        ),
        "the refused number must be the framed one, and got {error:?}"
    );

    // And the refusal left the previous snapshot alone.
    assert_eq!(
        state.load::<LedgerV1>().await.expect("loads"),
        Some(LedgerV1 {
            entries: vec![1, 2]
        })
    );
}

/// A limit below [`FRAME_LEN`] leaves no room for a payload, so **every** store
/// through the wrapper is refused, including an empty one. That is documented
/// on [`Files::limit`](keylink_persist::file::Files::limit) and this is what
/// keeps it true.
///
/// It is a store that can never succeed rather than an error at configuration
/// time, because `limit` is a builder that returns `Self`. So the floor is a
/// documented number and a test, and the refusal is at least loud: it names
/// both sides, and `offered` is twelve against a limit below twelve, which is
/// the shape of the mistake rather than a puzzle.
///
/// Made red by having `Files::limit` clamp upward — `self.limit = bytes.max(12)`
/// — which is the tempting fix and a silent fallback: the store would then
/// quietly not have the limit it was configured with.
#[tokio::test]
async fn a_limit_under_the_frame_refuses_even_an_empty_snapshot() {
    let scratch = Scratch::new("floor");
    let files = open(&scratch).await;
    let files = files.limit((FRAME_LEN - 1) as u64);

    let mut state = Versioned::new(files.claim(&id()).await.expect("claims"));
    let error = state
        .store(&Snapshot::of(&LedgerV1 { entries: vec![] }))
        .await
        .expect_err("there is no room for a header, let alone a payload");

    assert!(
        matches!(
            error,
            Error::TooLarge { offered, limit }
                if offered == FRAME_LEN as u64 && limit == (FRAME_LEN - 1) as u64
        ),
        "got {error:?}"
    );
}
