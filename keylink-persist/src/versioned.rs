//! The frame that makes stored bytes say which version wrote them, and the one
//! type in this crate that writes one.
//!
//! # Why a wrapper, and not a pair of free functions
//!
//! [`Claim::store`] takes raw bytes and always will: an adapter for a bucket, a
//! vault or a lease server must not have to understand framing in order to be
//! written. That is the port's whole shape and this module does not touch it.
//!
//! But a *caller* reaching the raw path by accident is the failure this module
//! exists to prevent, and "remember to frame it first" is exactly the kind of
//! guard this crate deletes rather than writes down. So [`Versioned`] **owns**
//! the claim. There is no accessor, no `Deref` and no `into_inner`, so a holder
//! of a `Versioned` has no route to the bytes underneath it. Adapters stay dumb
//! and the framed path is the only one a caller has.
//!
//! [`Versioned::store`] then takes a [`Snapshot<D>`](Snapshot) and not a
//! `&[u8]`, so the version it stamps is the version of the type that produced
//! the bytes rather than a type parameter a caller named beside them.
//! [`Snapshot`] documents the silent corruption the `&[u8]` signature allowed.
//!
//! The `framing` task in `mise.toml` is the belt to those braces: it fails if
//! [`Claim`] is named outside this crate at all, because a trait method is only
//! callable where the trait is in scope.
//!
//! # The byte layout
//!
//! ```text
//! offset  bytes  field     value
//!      0      6  magic     b"klsnap"
//!      6      2  layout    u16 little-endian — this header's own version, 2
//!      8      4  version   u32 little-endian — Durable::VERSION of the payload
//!     12      8  through   u64 little-endian — the last log record this snapshot contains
//!     20      *  payload   Durable::snapshot's bytes, never parsed here
//! ```
//!
//! **`through` is what makes the snapshot and the log one thing.** A journaled
//! store folds its log into a new snapshot, and between the snapshot reaching the
//! disk and the old records being cut away a crash can leave both. `through` says
//! which records the snapshot already contains, so recovery skips them and
//! finishes the cut, instead of applying them a second time. `0` means none, and
//! it is what a whole-state store always writes.
//!
//! **The magic is the load-bearing field**, and it is load-bearing for one
//! specific reason. This crate shipped without a frame, so the bytes on disk
//! *were* the payload. Prepending a header to that world is only safe if the
//! old shape is detectable, and a payload has no discriminator — `restore` on
//! the ledger in `keylink/tests/durable.rs` accepts any multiple of eight bytes,
//! so an unframed file read as `header + payload` would restore **successfully,
//! with garbage**. The magic turns that into [`FrameFault::WrongMagic`]: loud,
//! at the boundary, before a single payload byte is looked at.
//!
//! **`layout` and `version` are deliberately different numbers.** `layout`
//! changes when this table changes; `version` changes when a caller's state
//! changes. Sharing one field would mean that adding a field to somebody's
//! struct and adding a field to this header were the same event, and they are
//! not.
//!
//! **A layout that is not this one gets two different verdicts**, and which one
//! it gets started mattering on the day `LAYOUT` became 2 — which is the entire
//! reason `layout` is a separate field in the first place. A *newer* layout is
//! [`FrameFault::LayoutFromTheFuture`]; an *older* one is
//! [`FrameFault::LayoutTooOld`], and that is the arm a layout migration would
//! be written into. A single `!=` reported both as being from the future. It
//! was unreachable while there was only one layout, and it would have refused
//! every file already on disk with a reason that was false about all of them
//! the moment there were two.
//!
//! `version` is a `u32` because that is what [`Durable::VERSION`] is: four
//! billion revisions of one state is not a second break. `layout` is a `u16`
//! because this table is expected to change approximately never.
//!
//! **Twenty is load-bearing, and not because it is `u32`-aligned.** It is
//! load-bearing because it is *not* a multiple of eight. The magic is the first
//! line against an unframed file and a payload's own length check is the second:
//! a raw `Vec<u64>` file is `8n` bytes, so on the day its first twenty bytes
//! happen to spell a valid header the payload handed on is `8n − 20` bytes,
//! which is `≡ 4 (mod 8)` for every `n` and therefore never a whole number of
//! `u64`s. Widening `layout` to a `u32` for tidiness would take [`FRAME_LEN`] to
//! twenty-four and close that second line while looking like housekeeping, so the
//! property is asserted at compile time below rather than written down here.
//!
//! The magic is ASCII on purpose, so `head -c6` on a snapshot file answers
//! "what is this" without a tool.
//!
//! # Loading: three cases, and the third is the one people get wrong
//!
//! | stored vs [`Durable::VERSION`] | what happens |
//! |---|---|
//! | equal | [`Durable::restore`] on the payload, untouched |
//! | older | [`Durable::migrate`] one hop at a time, then `restore` |
//! | **newer** | [`RestoreError::FromTheFuture`] — refused, never truncated |
//!
//! The third case is where a naive loader reads the fields it knows and drops
//! the rest, which silently discards a field a newer build wrote and then
//! writes the truncation back over the original. There is no partial read here:
//! a snapshot from the future is an error and the stored bytes are not touched.
//!
//! # Opening: the snapshot, the records after it, and one finisher
//!
//! [`Versioned::open`] is the only way to build a [`Versioned`], and it does
//! more than load. It reads the snapshot, asks the claim for every record after
//! the snapshot's `through`, cuts a torn end, finishes a compaction that a crash
//! interrupted, and checks that the records are contiguous. What it hands back is
//! a [`Recovered`] holding the state and the records, and exactly one method
//! takes them: [`Recovered::replayed`] for a [`Journaled`] state, which applies
//! every record in order, or [`Recovered::whole`] for a plain [`Durable`], which
//! **refuses records** because it has nothing to replay them with.
//!
//! That refusal is why the log lives on the same claim as the snapshot. A state
//! last written by a journaled store and opened by a whole-state one would read
//! the snapshot, ignore the log, and lose every record after it, silently.
//!
//! `open` writes in exactly two cases, and neither changes the state the claim
//! holds: it cuts bytes that were never a record, and it finishes a cut that a
//! snapshot already made redundant.
//!
//! # Storing: a snapshot, or the change
//!
//! [`Versioned::store`] writes a whole snapshot and drops the records it
//! contains. For a journaled state, [`Versioned::stage`] drains what a turn
//! recorded and decides between appending it and folding the log into a new
//! snapshot by a [`Compaction`] policy; [`Staged::commit`] does what it decided.
//! The two are split so that `&D` is borrowed only in the synchronous half and
//! never held across an await, and [`Staged`] holds the wrapper exclusively until
//! it is committed: a staged turn cannot be committed late, after a later turn
//! has been, or through another wrapper. A journaled state's changes belong to
//! the one wrapper that [`adopt`](Versioned::adopt)ed them.

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::num::NonZeroU32;

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use crate::{Claim, Durable, Error, Journaled, RestoreError, Result, Seq};

// ────────────────────────────────────────────────────────────────── the frame

/// The first six bytes of every framed snapshot.
const MAGIC: [u8; 6] = *b"klsnap";

/// The version of the header table itself, not of anybody's state.
const LAYOUT: u16 = 2;

/// The size of the header every framed snapshot carries.
///
/// A caller sizing a store's limit needs this, because a limit applies to what
/// reaches the adapter and what reaches the adapter is the payload plus this.
/// See [`Error::TooLarge`].
pub const FRAME_LEN: usize = 20;

/// The header length is not a multiple of eight, and that is a property the
/// module header argues from rather than a coincidence.
///
/// A payload of whole `u64`s is `8n` bytes. If a header ever grew to a multiple
/// of eight, an unframed file whose leading bytes happened to spell a valid
/// header would hand on a payload that was still a whole number of `u64`s, and
/// the last check between a legacy file and a silently wrong state would be
/// gone. Widening `layout` to a `u32` is exactly the tidy-looking change that
/// does it, so it stops here instead of on somebody's disk.
const _: () = assert!(
    !FRAME_LEN.is_multiple_of(8),
    "FRAME_LEN must not be a multiple of 8: see the byte-layout section"
);

/// Which header check refused the stored bytes.
///
/// Reached through [`RestoreError::NotAFrame`], which is the loader's verdict
/// and not something a [`Durable`] implementation can report about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FrameFault {
    /// Fewer bytes than a header, so there is nothing to check.
    #[error("{len} bytes cannot hold a snapshot frame header")]
    TooShort {
        /// How many bytes were stored.
        len: usize,
    },

    /// The bytes do not begin with the six-byte magic the layout above names.
    ///
    /// A snapshot written before this crate framed anything lands here, and so
    /// does a file that was never a snapshot. Both are refused rather than
    /// guessed at, because a payload has no shape a loader could check.
    #[error("the stored bytes do not begin with a snapshot magic")]
    WrongMagic,

    /// The header's own layout is newer than this build writes.
    ///
    /// Reported on `layout > LAYOUT`, which is the comparison this sentence
    /// describes. It was `layout != LAYOUT` until this variant got a sibling,
    /// so an *older* layout was reported as being from the future — false about
    /// every such file, and unfalsifiable only because `LAYOUT` has never left
    /// 1.
    #[error("the frame layout is version {stored}, newer than the {ours} this build writes")]
    LayoutFromTheFuture {
        /// What the header claims.
        stored: u16,
        /// What this build understands.
        ours: u16,
    },

    /// The header's own layout is older than this build writes.
    ///
    /// **There is no layout-migration chain, and this is the arm that would
    /// grow one.** A payload's hops belong to the caller and
    /// [`Durable::migrate`] is where they go; a *layout* hop would belong to
    /// this crate, because this crate is what wrote the header. Until one
    /// exists an older layout is refused rather than guessed at, which is the
    /// same answer this module gives to every other header it cannot account
    /// for.
    ///
    /// Layout `0` is what an unframed file reads here, so this arm is a second
    /// line under the magic rather than a hypothetical. Verified by deleting
    /// the magic comparison: a raw 24-byte legacy ledger then fails here, as
    /// `LayoutTooOld { stored: 0, ours: 1 }`. That redundancy is worth having
    /// and it survived splitting the comparison in two — but it is *this*
    /// variant it lands on. Under the `!=` this replaced it landed on
    /// [`LayoutFromTheFuture`](Self::LayoutFromTheFuture), which called a file
    /// older than anything this build writes a file from the future.
    ///
    /// A legacy file does not reach here in practice.
    /// [`WrongMagic`](Self::WrongMagic) is checked first and is the verdict one
    /// actually gets.
    #[error(
        "the frame layout is version {stored}, older than the {ours} this build writes, and there is no layout migration"
    )]
    LayoutTooOld {
        /// What the header claims.
        stored: u16,
        /// What this build understands.
        ours: u16,
    },
}

/// Wrap `payload` in a header stamping it as version `version`, containing every
/// log record up to and including `through`.
fn frame(version: u32, through: Seq, payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(FRAME_LEN + payload.len());
    framed.extend_from_slice(&MAGIC);
    framed.extend_from_slice(&LAYOUT.to_le_bytes());
    framed.extend_from_slice(&version.to_le_bytes());
    framed.extend_from_slice(&through.get().to_le_bytes());
    framed.extend_from_slice(payload);
    framed
}

/// Check the header and hand back the version it stamps, the last log record the
/// snapshot contains, and the payload it wraps. The payload is not parsed, here
/// or anywhere in this crate.
fn unframe(bytes: &[u8]) -> std::result::Result<(u32, Seq, &[u8]), RestoreError> {
    let fault = |fault| RestoreError::NotAFrame { fault };

    let header: [u8; FRAME_LEN] = bytes
        .get(..FRAME_LEN)
        .and_then(|head| head.try_into().ok())
        .ok_or_else(|| fault(FrameFault::TooShort { len: bytes.len() }))?;

    if header[..MAGIC.len()] != MAGIC {
        return Err(fault(FrameFault::WrongMagic));
    }

    let layout = u16::from_le_bytes([header[6], header[7]]);
    match layout.cmp(&LAYOUT) {
        Ordering::Greater => {
            return Err(fault(FrameFault::LayoutFromTheFuture {
                stored: layout,
                ours: LAYOUT,
            }))
        }
        Ordering::Less => {
            return Err(fault(FrameFault::LayoutTooOld {
                stored: layout,
                ours: LAYOUT,
            }))
        }
        Ordering::Equal => {}
    }

    let version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let mut through = [0_u8; 8];
    through.copy_from_slice(&header[12..20]);
    Ok((
        version,
        Seq::new(u64::from_le_bytes(through)),
        &bytes[FRAME_LEN..],
    ))
}

/// Turn a payload that was stamped `stored` back into a `D`, walking the
/// migration chain.
///
/// Shared by snapshots and by deltas, so that "older is migrated, newer is
/// refused" is one function with no I/O in it and not two that can drift.
fn decode_payload<D: Durable>(stored: u32, payload: &[u8]) -> std::result::Result<D, RestoreError> {
    if stored > D::VERSION {
        return Err(RestoreError::FromTheFuture {
            stored,
            ours: D::VERSION,
        });
    }

    // The common case, and the only one that does not copy the payload.
    if stored == D::VERSION {
        return D::restore(payload);
    }

    // One hop at a time, so a v1 author never learns that v4 exists. A gap is
    // this loader's verdict rather than the state's, which is why `migrate`
    // reports a missing hop as `Ok(None)` and never as an error of its own.
    let mut current = payload.to_vec();
    for from in stored..D::VERSION {
        current = D::migrate(from, &current)?.ok_or(RestoreError::NoMigration { from })?;
    }
    D::restore(&current)
}

/// Turn framed bytes back into a state and the position its snapshot reaches in
/// the log.
fn decode<D: Durable>(bytes: &[u8]) -> std::result::Result<(D, Seq), RestoreError> {
    let (stored, through, payload) = unframe(bytes)?;
    Ok((decode_payload::<D>(stored, payload)?, through))
}

// ──────────────────────────────────────────────────────────────── the records

/// One turn's changes as one log record: the delta version, a count, then each
/// delta's bytes behind its length.
///
/// One record per turn makes a turn all-or-nothing: a torn tail cuts a whole
/// turn and never half of one. The version is in the record, so a log written
/// across an upgrade holds mixed versions and each migrates on its own.
fn encode_record<D: Journaled>(deltas: &[D::Delta]) -> Result<Vec<u8>> {
    let mut record = Vec::new();
    record.extend_from_slice(&<D::Delta as Durable>::VERSION.to_le_bytes());
    record.extend_from_slice(&wire_u32(deltas.len())?.to_le_bytes());
    for delta in deltas {
        let bytes = delta.snapshot();
        record.extend_from_slice(&wire_u32(bytes.len())?.to_le_bytes());
        record.extend_from_slice(&bytes);
    }
    Ok(record)
}

/// A count or a length as the four bytes the record format gives it.
///
/// **Refused, never clamped.** Writing `u32::MAX` beside the full payload would
/// produce a record that is acknowledged and that no parser can read back.
fn wire_u32(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::TooLarge {
        offered: n as u64,
        limit: u64::from(u32::MAX),
    })
}

/// Split a record into its delta version and each delta's bytes.
///
/// `None` when the counts and lengths do not add up to the bytes, or when the
/// record holds no delta: a turn that changed nothing writes no record.
fn parse_record(record: &[u8]) -> Option<(u32, Vec<&[u8]>)> {
    let version = u32::from_le_bytes(record.get(..4)?.try_into().ok()?);
    let count = u32::from_le_bytes(record.get(4..8)?.try_into().ok()?);
    if count == 0 {
        return None;
    }
    let mut deltas = Vec::new();
    let mut at = 8_usize;
    for _ in 0..count {
        let len = u32::from_le_bytes(record.get(at..at.checked_add(4)?)?.try_into().ok()?);
        at += 4;
        let end = at.checked_add(len as usize)?;
        deltas.push(record.get(at..end)?);
        at = end;
    }
    (at == record.len()).then_some((version, deltas))
}

// ──────────────────────────────────────────────────────────────── the payload

/// Bytes a [`Durable`] wrote, carrying which one wrote them.
///
/// # Why this is not a `&[u8]`
///
/// [`Versioned::store`] stamps [`D::VERSION`](Durable::VERSION) into the frame,
/// so it has to know which `D` produced the payload. Raw bytes cannot say, and
/// a `store::<D>(&[u8])` that took them let a caller **assert** a version
/// instead of proving one:
///
/// ```text
/// let v1 = LedgerV1 { entries: vec![10, 20, 30] };   // VERSION = 1
/// state.store::<LedgerV3>(&v1.snapshot()).await?;    // VERSION = 3. Compiled.
/// ```
///
/// Nothing was wrong at either end. The stamp said 3, so the next load took the
/// equal-versions fast path, skipped the migration chain entirely and handed v1
/// bytes to `LedgerV3::restore`, which read the first entry as a total and the
/// last as a count. That is worse than the unbumped-`VERSION` hazard
/// [`Durable::VERSION`] documents as "the failure nobody can type against",
/// because this one *was* typeable against — `&[u8]` had erased the one thing
/// the type parameter needed to know.
///
/// So the only way to make one of these is [`of`](Self::of), which calls
/// [`Durable::snapshot`] itself. `D` is inferred from the value rather than
/// named beside it, and the mismatch above is `E0308` at the call site instead
/// of a stamp on a disk.
///
/// # What it still is not
///
/// It is not a `&D`. By the time a state is committed it lives inside an actor,
/// and the only consistent read of it is through a turn, so the bytes have to
/// be taken *during* one and outlive it —
/// `handle.with_state(Snapshot::of).await?` does exactly that and hands back a
/// value, not a borrow. That is the shape both in-tree consumers use.
///
/// There is deliberately **no probe about reading the bytes back out**, and
/// there is no accessor either. The bytes were never the secret —
/// [`Durable::snapshot`] is public, so anyone holding the state already has
/// them. What `keylink-persist/src/representability.rs` pins is the other
/// direction: no second constructor, and no `From<Vec<u8>>`, because either one
/// hands `D` back to whoever is writing the annotation.
pub struct Snapshot<D: Durable> {
    bytes: Vec<u8>,
    /// `fn() -> D` rather than `D`, so this is `Send`, `Sync` and `'static`
    /// exactly as often as a `Vec<u8>` is, whatever `D` turns out to be. A
    /// snapshot travels back from a turn; a `PhantomData<D>` would make that
    /// depend on properties of `D` that have nothing to do with its bytes.
    durable: PhantomData<fn() -> D>,
}

impl<D: Durable> Snapshot<D> {
    /// Take `state`'s snapshot, remembering what took it.
    ///
    /// Call it **inside a turn** — `handle.with_state(Snapshot::of)` — where
    /// `&D` is exclusive. That is what makes the bytes a consistent point rather
    /// than a torn read, and it is the same rule
    /// [`Durable::snapshot`] states for itself.
    pub fn of(state: &D) -> Self {
        Self {
            bytes: state.snapshot(),
            durable: PhantomData,
        }
    }
}

impl<D: Durable> std::fmt::Debug for Snapshot<D> {
    /// The length and nothing else. A snapshot's bytes are the caller's
    /// encoding and this crate never looks inside one, including here.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("version", &D::VERSION)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

// ──────────────────────────────────────────────────────────────── the policy

/// When a journaled state's log is folded into a new snapshot.
///
/// The log is cut away when it has grown to `ratio` times the snapshot's bytes,
/// and never before it holds `floor` bytes. The ratio bounds the work: with a
/// ratio of one, the bytes written per byte appended stay at most a small
/// constant however large the state grows, because a fold costs the snapshot's
/// size and happens once the log has grown to that size. The floor stops a tiny
/// state from folding on every turn.
///
/// **A fold is a turn with the cost of the whole state**: an in-turn encode and
/// four flushes. That is a spike by construction, and the policy only decides how
/// rare it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Compaction {
    ratio: NonZeroU32,
    floor: u64,
}

impl Compaction {
    /// Fold once the log holds as many bytes as the snapshot, and at least 1 MiB.
    pub const DEFAULT: Self = Self {
        ratio: NonZeroU32::MIN,
        floor: 1 << 20,
    };

    /// Fold once the log holds `ratio` times the snapshot's bytes, and at least
    /// `floor` bytes.
    #[must_use]
    pub const fn new(ratio: NonZeroU32, floor: u64) -> Self {
        Self { ratio, floor }
    }

    /// Whether a log of `log` bytes, about to grow by `record`, has reached the
    /// point of folding into a snapshot of `snapshot` bytes.
    fn due(self, snapshot: u64, log: u64, record: u64) -> bool {
        let threshold = snapshot
            .saturating_mul(u64::from(self.ratio.get()))
            .max(self.floor);
        log.saturating_add(record) >= threshold
    }
}

impl Default for Compaction {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The end of the log that recovery cut, reported rather than dropped.
///
/// The append that was writing those bytes never resolved, so its caller was
/// never told it happened and nothing acknowledged is lost. It is still worth a
/// log line: it is the only trace of a crash during a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TornTail {
    /// How many bytes were cut.
    pub bytes: u64,
    /// The last record that was kept.
    pub after: Seq,
}

// ──────────────────────────────────────────────────────────────── the wrapper

/// A claim that only ever sees framed bytes.
///
/// Owns the claim it was built from. That ownership is the guarantee: there is
/// no accessor, no `Deref` and no `into_inner`, so nothing holding a
/// `Versioned` can write an unframed byte.
/// `keylink-persist/src/representability.rs` holds the compile-time probes for
/// that sentence, and records there why the *conversion* direction rests on the
/// orphan rule instead of on a probe.
///
/// **There is no `new`.** [`open`](Self::open) is the only way to build one, and
/// it reads the log before it returns, so a [`store`](Self::store) can never run
/// before the code knows how far the log reaches. A `store` that did not know
/// would write a snapshot claiming to contain no records, and the next restore
/// would apply them a second time.
///
/// **The claim is a type parameter here and stays one in this example**, which
/// is the whole shape of the thing: the wrapper is this crate's
/// feature-independent core and knows nothing about where the bytes land. The
/// example named `Files` until it was noticed that the only doctest on the
/// crate's core module could not compile without its one optional adapter. The
/// `file` module's own documentation carries the end-to-end version, where the
/// feature gate already is.
///
/// ```
/// # use keylink_persist::{Claim, Durable, RestoreError};
/// # use keylink_persist::versioned::{Snapshot, Versioned};
/// # struct Counter(u64);
/// # impl Durable for Counter {
/// #     const VERSION: u32 = 1;
/// #     fn snapshot(&self) -> Vec<u8> { self.0.to_le_bytes().to_vec() }
/// #     fn restore(_: &[u8]) -> Result<Self, RestoreError> { Ok(Self(0)) }
/// #     fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> { Ok(None) }
/// # }
/// # async fn bump<C: Claim>(claim: C) -> Result<(), Box<dyn std::error::Error>> {
/// let (counter, mut state) = Versioned::open::<Counter>(claim).await?.whole()?;
/// let mut counter = counter.unwrap_or(Counter(0));
///
/// counter.0 += 1;
/// state.store(&Snapshot::of(&counter)).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Versioned<C: Claim> {
    claim: C,
    /// Who this wrapper is, for the [`Changes`](crate::Changes) buffers it owns.
    /// Never zero; every `open` takes a new one.
    session: u64,
    /// The last log record the stored snapshot contains.
    through: Seq,
    /// The position the next append takes, or `None` once every number is used.
    /// Always `through + 1` or later.
    next: Option<Seq>,
    /// The bytes of the stored snapshot, frame included. `None` before the first
    /// ever store.
    snapshot_bytes: Option<u64>,
    /// The bytes of the records appended since the snapshot.
    log_bytes: u64,
    /// A cut that should have followed a snapshot did not happen. The next
    /// snapshot repeats it, and so does `open`.
    stale: bool,
}

/// The next session number. A process-wide counter and not a random value: two
/// wrappers must never share one, and nothing here needs it to be unguessable.
static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

impl<C: Claim> Versioned<C> {
    /// Take a claim, read the snapshot and every record after it, and keep the
    /// claim.
    ///
    /// `Ok` with no state is the first ever run: nothing has been stored, which
    /// is not a failure. The claim is exclusive from here until the returned
    /// [`Recovered`] (or the [`Versioned`] it yields) is dropped.
    ///
    /// **Migration does not write.** An old snapshot, and an old delta in the log,
    /// stay old until the next [`store`](Self::store) replaces them. That is the
    /// safe order: a crash between the two leaves the bytes that were already
    /// there, and the chain simply runs again.
    ///
    /// **Opening writes in exactly two cases, and neither changes the state the
    /// claim holds.** It cuts a torn tail (bytes that were never a record), and it
    /// finishes a [`truncate_before`](Claim::truncate_before) that a crash
    /// interrupted, whose records the stored snapshot already contains.
    ///
    /// # Errors
    ///
    /// - [`Error::Io`] when the store cannot be read.
    /// - [`Error::Restore`] wrapping
    ///   [`RestoreError::NotAFrame`] for bytes that are not a snapshot this
    ///   build can read — an unframed file from before framing existed is this
    ///   case, and it is refused rather than mis-parsed;
    ///   [`RestoreError::FromTheFuture`] for a snapshot a newer build wrote,
    ///   which is refused and never truncated;
    ///   [`RestoreError::NoMigration`] for a gap in the chain; and whatever
    ///   [`Durable::restore`] or [`Durable::migrate`] reported.
    /// - [`RestoreError::LogWithoutSnapshot`] when records exist and the snapshot
    ///   they follow does not, and [`RestoreError::Gap`] when the records skip a
    ///   position.
    /// - [`Error::NotAJournal`] when the stored log is not one this build reads,
    ///   or is damaged before its end.
    /// - [`Error::SeqExhausted`] when the snapshot says it contains the last
    ///   sequence number there is. Decided before the log is touched.
    pub async fn open<D: Durable>(mut claim: C) -> Result<Recovered<C, D>> {
        let (state, through, snapshot_bytes) = match claim.load().await? {
            Some(bytes) => {
                let (state, through) = decode::<D>(&bytes)?;
                (Some(state), through, Some(bytes.len() as u64))
            }
            None => (None, Seq::new(0), None),
        };

        // Before `recover`, which may cut a torn tail: a refusal modifies nothing.
        let from = Seq::new(through.get().checked_add(1).ok_or(Error::SeqExhausted)?);
        let tail = claim.recover(from).await?;
        if tail.earlier > 0 {
            // A snapshot reached the disk and the cut that should have followed
            // it did not. The snapshot is the commit point, so finish the cut.
            claim.truncate_before(from).await?;
        }

        if state.is_none() && !tail.records.is_empty() {
            return Err(RestoreError::LogWithoutSnapshot {
                records: tail.records.len() as u64,
            }
            .into());
        }

        let mut expected = Some(from);
        let mut last = through;
        let mut log_bytes = 0_u64;
        for (found, record) in &tail.records {
            // Past the last number there is nothing a record could be.
            let Some(want) = expected else {
                return Err(RestoreError::BadRecord { seq: *found }.into());
            };
            if *found != want {
                return Err(RestoreError::Gap {
                    expected: want,
                    found: *found,
                }
                .into());
            }
            last = want;
            expected = want.get().checked_add(1).map(Seq::new);
            log_bytes += record.len() as u64;
        }

        let torn = tail.torn.map(|bytes| TornTail { bytes, after: last });
        Ok(Recovered {
            versioned: Self {
                claim,
                session: NEXT_SESSION.fetch_add(1, AtomicOrdering::Relaxed),
                through,
                next: expected,
                snapshot_bytes,
                log_bytes,
                stale: false,
            },
            state,
            records: tail.records,
            torn,
        })
    }

    /// The last position this wrapper has appended or been opened at.
    fn appended_through(&self) -> Seq {
        match self.next {
            // `next` is `through + 1` or later, so it is never zero.
            Some(next) => Seq::new(next.get() - 1),
            None => Seq::new(u64::MAX),
        }
    }

    /// Frame `snapshot` as version [`D::VERSION`](Durable::VERSION), replace what
    /// is stored, and drop the log records it contains.
    ///
    /// **`D` is inferred from `snapshot` and cannot be told.** That is the whole
    /// reason [`Snapshot`] exists rather than a `&[u8]`: the version stamped
    /// here is the version of the type that produced these bytes, proved by the
    /// argument rather than asserted by a turbofish beside it. [`Snapshot`]
    /// documents what the `&[u8]` version of this signature allowed.
    ///
    /// Atomic, because [`Claim::store`] is. A crash during this leaves either
    /// the previous framed snapshot or the new one. **The snapshot reaching the
    /// disk is the commit point**: the cut that follows it is cleanup, and a
    /// failed cut does not fail the store. The turn is on the disk, and an error
    /// would say it was not. The cut is retried at the next snapshot and by
    /// [`open`](Self::open).
    ///
    /// # Errors
    ///
    /// - [`Error::TooLarge`] when the frame **plus**
    ///   the payload is over the adapter's limit. The number it reports is what
    ///   reached the adapter, which is [`FRAME_LEN`] more than the payload.
    /// - [`Error::Io`] when the write fails. The previous
    ///   snapshot is intact.
    pub async fn store<D: Durable>(&mut self, snapshot: &Snapshot<D>) -> Result<()> {
        // Everything appended so far is in this snapshot: it was taken in the
        // turn that appended the last of it.
        let through = self.appended_through();
        let framed = frame(D::VERSION, through, &snapshot.bytes);
        self.claim.store(&framed).await?;

        let records_may_exist = through > self.through || self.stale;
        self.through = through;
        self.snapshot_bytes = Some(framed.len() as u64);
        self.log_bytes = 0;
        if records_may_exist {
            // The first position that is not in the snapshot. When there is none
            // left, the cut keeps the last record: harmless, because a state
            // whose snapshot contains the last number is refused at `open`.
            let cut = self.next.unwrap_or(Seq::new(u64::MAX));
            // Not `?`: see above.
            self.stale = self.claim.truncate_before(cut).await.is_err();
        }
        Ok(())
    }

    /// Make this wrapper the one persistence session that owns `state`'s change
    /// buffer.
    ///
    /// Called once, before the state's first turn (`keylink`'s
    /// `Opened::start_from` does it). Until then the buffer records nothing.
    ///
    /// # Errors
    ///
    /// [`Error::ForeignChanges`] when a different session already owns it: a state
    /// cannot have two stores.
    pub fn adopt<D: Journaled>(&self, state: &D) -> Result<()> {
        if state.changes().adopt_for(self.session) {
            Ok(())
        } else {
            Err(Error::ForeignChanges)
        }
    }

    /// Drain this turn's changes and decide what to write. Synchronous in what it
    /// reads, so the `&D` is never held across an await: the result borrows this
    /// wrapper and nothing else.
    ///
    /// - The first turn ever stores a snapshot, so a record never exists without
    ///   the snapshot it follows.
    /// - A turn that recorded nothing writes nothing. A whole-state store pays its
    ///   full price even for a read.
    /// - Otherwise the turn appends one record, unless the log has reached
    ///   `when`'s threshold, in which case it stores a snapshot instead and the
    ///   changes are dropped because the snapshot contains them.
    ///
    /// **The result holds this wrapper exclusively until it is committed or
    /// dropped.** A turn staged against one wrapper cannot be committed through
    /// another, and a second turn cannot be staged, let alone committed, in
    /// between: both are borrow errors. A snapshot staged before an append can
    /// therefore never be stored after it and cut that append away. Dropping it
    /// without committing loses the drained changes, and it is `#[must_use]`.
    ///
    /// ```compile_fail
    /// # use keylink_persist::versioned::{Compaction, Versioned};
    /// # use keylink_persist::{Claim, Journaled};
    /// # async fn f<C: Claim, D: Journaled>(v: &mut Versioned<C>, state: &D) {
    /// let first = v.stage(state, Compaction::DEFAULT).unwrap();
    /// let second = v.stage(state, Compaction::DEFAULT).unwrap();
    /// second.commit().await.unwrap();
    /// first.commit().await.unwrap();
    /// # }
    /// ```
    ///
    /// The same code with the second turn staged after the first is committed is
    /// what this accepts:
    ///
    /// ```
    /// # use keylink_persist::versioned::{Compaction, Versioned};
    /// # use keylink_persist::{Claim, Journaled};
    /// # async fn f<C: Claim, D: Journaled>(v: &mut Versioned<C>, state: &D) {
    /// v.stage(state, Compaction::DEFAULT).unwrap().commit().await.unwrap();
    /// v.stage(state, Compaction::DEFAULT).unwrap().commit().await.unwrap();
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`Error::ForeignChanges`] when `state`'s changes are not this wrapper's
    ///   (never adopted, or adopted by another). Nothing is drained.
    /// - [`Error::TooLarge`] when a turn recorded more changes, or one change was
    ///   longer, than the record format can count: refused rather than written
    ///   with a different number.
    /// - [`Error::SeqExhausted`] when an append would need a position that does
    ///   not exist.
    pub fn stage<D: Journaled>(&mut self, state: &D, when: Compaction) -> Result<Staged<'_, C, D>> {
        // Ownership and exhaustion are checked before anything is drained, so a
        // stage refused for either loses no change.
        let changes = state.changes();
        if !changes.owned_by(self.session) {
            return Err(Error::ForeignChanges);
        }
        let drained = || changes.take_for(self.session).unwrap_or_default();
        let Some(snapshot_bytes) = self.snapshot_bytes else {
            drop(drained());
            return Ok(Staged {
                versioned: self,
                step: Step::Compact(Snapshot::of(state)),
            });
        };
        if self.next.is_none() {
            return Err(Error::SeqExhausted);
        }
        let deltas = drained();
        if deltas.is_empty() {
            return Ok(Staged {
                versioned: self,
                step: Step::Nothing,
            });
        }
        let record = encode_record::<D>(&deltas)?;
        let step = if when.due(snapshot_bytes, self.log_bytes, record.len() as u64) {
            Step::Compact(Snapshot::of(state))
        } else {
            Step::Append(record)
        };
        Ok(Staged {
            versioned: self,
            step,
        })
    }

    /// Drain this turn's changes and write nothing: the turn is not being
    /// stored.
    ///
    /// For a turn that panicked. Its changes are half of a turn, and they must
    /// not be carried into the next one.
    ///
    /// # Errors
    ///
    /// [`Error::ForeignChanges`] when `state`'s changes are not this wrapper's.
    /// They are left alone: a wrapper cannot discard another store's changes.
    pub fn discard<D: Journaled>(&mut self, state: &D) -> Result<()> {
        state
            .changes()
            .take_for(self.session)
            .map(drop)
            .ok_or(Error::ForeignChanges)
    }
}

/// What one turn will write, holding the wrapper that produced it. Built only by
/// [`Versioned::stage`].
#[must_use = "a staged turn that is never committed is a turn that is not stored"]
pub struct Staged<'v, C: Claim, D: Durable> {
    versioned: &'v mut Versioned<C>,
    step: Step<D>,
}

enum Step<D: Durable> {
    Nothing,
    Append(Vec<u8>),
    Compact(Snapshot<D>),
}

impl<C: Claim, D: Durable> Staged<'_, C, D> {
    /// Write what [`stage`](Versioned::stage) decided.
    ///
    /// # Errors
    ///
    /// As [`Versioned::store`] for a snapshot, and for an append
    /// [`Error::TooLarge`], [`Error::Io`], and [`Error::Indeterminate`]. A failed
    /// append leaves the log as it was, so the turn is refused cleanly, **except
    /// for `Indeterminate`**: the record may be stored, and the caller must not be
    /// told that a retry is safe.
    pub async fn commit(self) -> Result<()> {
        let Self { versioned, step } = self;
        match step {
            Step::Nothing => Ok(()),
            Step::Append(record) => {
                // Checked before any I/O. Counters move only after the append
                // resolved, so a refused append leaves them as they were.
                let seq = versioned.next.ok_or(Error::SeqExhausted)?;
                versioned.claim.append(seq, &record).await?;
                versioned.next = seq.get().checked_add(1).map(Seq::new);
                versioned.log_bytes += record.len() as u64;
                Ok(())
            }
            Step::Compact(snapshot) => versioned.store(&snapshot).await,
        }
    }
}

impl<C: Claim, D: Durable> std::fmt::Debug for Staged<'_, C, D> {
    /// What it will do, and nothing else: the bytes are the state's own.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.step {
            Step::Nothing => "Staged(Nothing)",
            Step::Append(_) => "Staged(Append)",
            Step::Compact(_) => "Staged(Compact)",
        })
    }
}

// ──────────────────────────────────────────────────────────────── the recovery

/// A claim read back, holding the state and the records after it until one
/// finisher takes them.
///
/// The records are not an accessor and have no reader: [`replayed`](Self::replayed)
/// applies them and [`whole`](Self::whole) refuses them. A type that could be
/// asked for its state without being asked about its records is the type that
/// reads a journaled state as a whole one and loses the log.
#[must_use = "this holds the state that was read; dropping it starts from nothing"]
pub struct Recovered<C: Claim, D> {
    versioned: Versioned<C>,
    state: Option<D>,
    records: Vec<(Seq, Vec<u8>)>,
    torn: Option<TornTail>,
}

impl<C: Claim, D: Durable> Recovered<C, D> {
    /// What recovery cut from the end of the log, if anything. Log it.
    #[must_use]
    pub fn torn(&self) -> Option<TornTail> {
        self.torn
    }

    /// The snapshot alone, with the wrapper that will keep writing it.
    ///
    /// **Refuses records**, because this type has nothing to replay them with.
    /// The state is `None` on the first ever run.
    ///
    /// # Errors
    ///
    /// [`RestoreError::UnreplayedLog`] when the log holds records after the
    /// snapshot. The claim is released.
    pub fn whole(self) -> std::result::Result<(Option<D>, Versioned<C>), RestoreError> {
        if !self.records.is_empty() {
            return Err(RestoreError::UnreplayedLog {
                records: self.records.len() as u64,
            });
        }
        Ok((self.state, self.versioned))
    }
}

impl<C: Claim, D: Journaled> Recovered<C, D> {
    /// The snapshot with every later record applied in order.
    ///
    /// The state's change buffer is not yet adopted by anyone while this runs, so
    /// nothing [`Journaled::apply`] records is kept: a replayed change is already
    /// stored.
    ///
    /// # Errors
    ///
    /// [`RestoreError::BadRecord`] for a record whose counts and lengths do not
    /// add up, and [`RestoreError::Delta`] for a delta that did not decode, with
    /// the position of the record it was in.
    pub fn replayed(self) -> std::result::Result<(Option<D>, Versioned<C>), RestoreError> {
        let Self {
            versioned,
            state,
            records,
            ..
        } = self;
        let Some(mut state) = state else {
            return Ok((None, versioned));
        };
        for (seq, record) in &records {
            let (version, deltas) =
                parse_record(record).ok_or(RestoreError::BadRecord { seq: *seq })?;
            for bytes in deltas {
                let delta = decode_payload::<D::Delta>(version, bytes).map_err(|source| {
                    RestoreError::Delta {
                        seq: *seq,
                        source: Box::new(source),
                    }
                })?;
                state.apply(delta);
            }
        }
        Ok((Some(state), versioned))
    }
}

impl<C: Claim, D> std::fmt::Debug for Recovered<C, D> {
    /// Whether there is a state and how many records wait, and nothing else.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recovered")
            .field("state", &self.state.is_some())
            .field("records", &self.records.len())
            .field("torn", &self.torn)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    /// Eight bytes per entry, and nothing else — the shape the in-tree ledgers
    /// use, chosen here because it is the shape that makes an unframed file
    /// dangerous: every multiple of eight is a valid payload.
    struct Ledger(Vec<u64>);

    impl Durable for Ledger {
        const VERSION: u32 = 1;

        fn snapshot(&self) -> Vec<u8> {
            self.0.iter().flat_map(|n| n.to_le_bytes()).collect()
        }

        fn restore(bytes: &[u8]) -> std::result::Result<Self, RestoreError> {
            if !bytes.len().is_multiple_of(8) {
                return Err(RestoreError::Malformed(format!(
                    "a ledger is whole u64s, and this is {} bytes",
                    bytes.len()
                )));
            }
            Ok(Self(
                bytes
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|c| u64::from_le_bytes(*c))
                    .collect(),
            ))
        }

        fn migrate(
            _from: u32,
            _bytes: &[u8],
        ) -> std::result::Result<Option<Vec<u8>>, RestoreError> {
            Ok(None)
        }
    }

    /// Made red by `scripts/break-once.sh`, replacing `frame`'s
    /// `framed.extend_from_slice(&version.to_le_bytes());` with the `u64` widening
    /// `&u64::from(version).to_le_bytes()` — a header that grew by four bytes while
    /// [`FRAME_LEN`] went on saying twenty. Watched at the length assertion, which is
    /// the one that notices a header nobody re-measured.
    #[test]
    fn the_header_is_the_documented_twenty_bytes() {
        let framed = frame(7, Seq::new(3), b"payload");
        assert_eq!(framed.len(), FRAME_LEN + 7);
        assert_eq!(&framed[..6], b"klsnap");
        assert_eq!(&framed[6..8], &2u16.to_le_bytes());
        assert_eq!(&framed[8..12], &7u32.to_le_bytes());
        assert_eq!(&framed[12..20], &3u64.to_le_bytes());
        assert_eq!(&framed[20..], b"payload");
    }

    /// Made red by `scripts/break-once.sh`, replacing `unframe`'s
    /// `Ok((version, &bytes[FRAME_LEN..]))` with `&bytes[FRAME_LEN + 1..]` — one
    /// byte of payload eaten by the header. Watched: left
    /// `(4, [104, 97, 116, 101, 118, 101, 114])` against right
    /// `(4, [119, 104, 97, 116, 101, 118, 101, 114])`. The version still round-trips
    /// under that break, which is why the payload is compared and not just the
    /// stamp.
    #[test]
    fn a_frame_round_trips_through_unframe() {
        let framed = frame(4, Seq::new(9), b"whatever");
        assert_eq!(
            unframe(&framed).expect("a frame"),
            (4, Seq::new(9), &b"whatever"[..])
        );
    }

    /// The silent-corruption case, at the smallest scale it can be shown.
    /// Twenty-four raw bytes are three valid ledger entries, so a loader with
    /// no magic would hand back a `Ledger` built from the header.
    ///
    /// Made red by `scripts/break-once.sh`, replacing `unframe`'s
    /// `if header[..MAGIC.len()] != MAGIC {` with `if false {` — the magic
    /// comparison deleted, which is the state the crate was in before framing.
    /// Watched: the `matches!` fails. What the break produces is worth reading: the
    /// twenty-four raw bytes read as layout `0`, so they are refused as
    /// `LayoutTooOld` instead of `WrongMagic` — a second refusal standing behind the
    /// first, and the reason the *fault* is asserted rather than merely that an
    /// error came back.
    #[test]
    fn raw_payload_bytes_are_not_a_frame() {
        let raw = Ledger(vec![1, 2, 3]).snapshot();
        assert_eq!(raw.len(), 24);
        assert!(matches!(
            unframe(&raw),
            Err(RestoreError::NotAFrame {
                fault: FrameFault::WrongMagic
            })
        ));
    }

    /// "Says so" is the claim, and the number is the saying: made red by
    /// `scripts/break-once.sh` replacing `unframe`'s
    /// `fault(FrameFault::TooShort { len: bytes.len() })` with `{ len: 0 }` — still
    /// refused, refused with a length that is not the file's. Watched: the
    /// `matches!` fails on `len: 5`.
    ///
    /// The refusal itself has no counter-example: it is the `ok_or_else` on a
    /// `[u8; FRAME_LEN]` conversion, so any edit that lets five bytes through stops
    /// compiling. That is a guard whose *reason* is watchable and whose existence is
    /// structural, and it is recorded that way rather than claimed to be more.
    #[test]
    fn a_file_too_short_to_hold_a_header_says_so() {
        assert!(matches!(
            unframe(b"klsna"),
            Err(RestoreError::NotAFrame {
                fault: FrameFault::TooShort { len: 5 }
            })
        ));
    }

    /// Made red by `scripts/break-once.sh`, replacing `unframe`'s
    /// `let layout = u16::from_le_bytes([header[6], header[7]]);` with
    /// `let layout = LAYOUT;` — the layout assumed rather than read, so every file
    /// on disk claims to be this build's. Watched: the `matches!` fails; `unframe`
    /// returns `Ok` for a header stamped `2`. A different literal from the
    /// integration test's, on purpose: that one deletes the comparison, this one
    /// deletes the read, and the two failures are the two ways this check dies.
    #[test]
    fn a_header_layout_this_build_does_not_write_is_refused() {
        let mut framed = frame(1, Seq::new(0), b"12345678");
        framed[6] = 3;
        assert!(matches!(
            unframe(&framed),
            Err(RestoreError::NotAFrame {
                fault: FrameFault::LayoutFromTheFuture { stored: 3, ours: 2 }
            })
        ));
    }

    /// The other side of that comparison, and the one a `!=` got wrong. Layout
    /// 0 is also what an unframed file reads, so this is the check that still
    /// refuses one if the magic ever stops being the first thing tested.
    #[test]
    fn a_header_layout_older_than_this_build_is_refused_as_old() {
        let mut framed = frame(1, Seq::new(0), b"12345678");
        framed[6] = 0;
        assert!(matches!(
            unframe(&framed),
            Err(RestoreError::NotAFrame {
                fault: FrameFault::LayoutTooOld { stored: 0, ours: 2 }
            })
        ));
        // The layout this crate wrote before `through` existed: twelve bytes of
        // header, so its first bytes spell a layout of 1.
        framed[6] = 1;
        assert!(matches!(
            unframe(&framed),
            Err(RestoreError::NotAFrame {
                fault: FrameFault::LayoutTooOld { stored: 1, ours: 2 }
            })
        ));
    }

    /// Made red by `scripts/break-once.sh`, adding `.filter(|_| bytes.len() > FRAME_LEN)`
    /// to `unframe`'s `bytes.get(..FRAME_LEN)` — a frame required to carry at least
    /// one byte, which is the plausible reading of "too short" and the one that
    /// makes an empty state unloadable. Watched: "a frame: NotAFrame { fault:
    /// TooShort { len: 20 } }". The break is isolating rather than broad: every
    /// other test in this module has a payload, so this is the only one it reddens.
    #[test]
    fn an_empty_payload_is_a_payload_and_not_a_fault() {
        let framed = frame(1, Seq::new(0), b"");
        assert_eq!(
            unframe(&framed).expect("a frame"),
            (1, Seq::new(0), &b""[..])
        );
        assert_eq!(
            decode::<Ledger>(&framed).expect("restores").0 .0,
            Vec::new()
        );
    }

    // ───────────────────────────────────────────────────── the log's records

    /// A delta that remembers its bytes, so the record codec can be exercised
    /// without a state around it.
    struct Bytes(Vec<u8>);

    impl Durable for Bytes {
        const VERSION: u32 = 5;
        fn snapshot(&self) -> Vec<u8> {
            self.0.clone()
        }
        fn restore(bytes: &[u8]) -> std::result::Result<Self, RestoreError> {
            Ok(Self(bytes.to_vec()))
        }
        fn migrate(_: u32, _: &[u8]) -> std::result::Result<Option<Vec<u8>>, RestoreError> {
            Ok(None)
        }
    }

    /// A state around [`Bytes`], only so `encode_record` has a `Journaled` to name.
    struct Holder(crate::Changes<Bytes>);

    impl Durable for Holder {
        const VERSION: u32 = 1;
        fn snapshot(&self) -> Vec<u8> {
            Vec::new()
        }
        fn restore(_: &[u8]) -> std::result::Result<Self, RestoreError> {
            Ok(Self(crate::Changes::default()))
        }
        fn migrate(_: u32, _: &[u8]) -> std::result::Result<Option<Vec<u8>>, RestoreError> {
            Ok(None)
        }
    }

    impl Journaled for Holder {
        type Delta = Bytes;
        fn changes(&self) -> &crate::Changes<Bytes> {
            &self.0
        }
        fn apply(&mut self, _: Bytes) {}
    }

    /// One turn is one record, carrying its delta version and every delta behind
    /// its length.
    ///
    /// Made red by `scripts/break-once.sh`, replacing `encode_record`'s
    /// `record.extend_from_slice(&bytes);` with nothing — the length written and the
    /// bytes left out. Watched: the parse gives `None` where it should give the two
    /// deltas.
    #[test]
    fn a_record_round_trips() {
        let record = encode_record::<Holder>(&[Bytes(b"one".to_vec()), Bytes(Vec::new())])
            .expect("a small record encodes");
        assert_eq!(&record[..4], &5u32.to_le_bytes(), "the delta's version");
        assert_eq!(&record[4..8], &2u32.to_le_bytes(), "the count");
        assert_eq!(
            parse_record(&record),
            Some((5, vec![&b"one"[..], &b""[..]]))
        );
    }

    #[test]
    fn a_record_that_does_not_add_up_does_not_parse() {
        let good = encode_record::<Holder>(&[Bytes(b"one".to_vec())]).expect("encodes");
        // Cut anywhere short, or padded: neither is a record.
        for cut in 0..good.len() {
            assert_eq!(parse_record(&good[..cut]), None, "cut at {cut}");
        }
        let mut padded = good.clone();
        padded.push(0);
        assert_eq!(parse_record(&padded), None);
        // And a record of no deltas is not one: a turn that changed nothing
        // writes nothing.
        assert_eq!(
            parse_record(&encode_record::<Holder>(&[]).expect("encodes")),
            None
        );
    }

    /// The policy: fold once the log reaches `ratio` times the snapshot, and never
    /// before the floor.
    ///
    /// Made red by `scripts/break-once.sh`, replacing `Compaction::due`'s
    /// `.max(self.floor)` with `.min(self.floor)`. Watched: with a floor of zero the
    /// threshold collapses to zero and the first assertion fails, "99 of 100
    /// bytes".
    #[test]
    fn the_policy_folds_at_the_ratio_and_not_below_the_floor() {
        let one = NonZeroU32::MIN;
        let two = NonZeroU32::new(2).expect("nonzero");

        let by_size = Compaction::new(one, 0);
        assert!(!by_size.due(100, 50, 49), "99 of 100 bytes");
        assert!(by_size.due(100, 50, 50), "100 of 100 bytes");
        assert!(!Compaction::new(two, 0).due(100, 50, 50));
        assert!(Compaction::new(two, 0).due(100, 100, 100));

        let floored = Compaction::new(one, 1000);
        assert!(
            !floored.due(10, 500, 499),
            "past the ratio, under the floor"
        );
        assert!(floored.due(10, 500, 500));

        // A log that overflows the arithmetic is due, not a panic.
        assert!(Compaction::new(two, 0).due(u64::MAX, u64::MAX, u64::MAX));
    }

    /// A count or a length that does not fit four bytes is refused and not
    /// clamped to `u32::MAX` beside the full payload.
    ///
    /// Made red by `scripts/break-once.sh`, replacing `wire_u32`'s
    /// `u32::try_from(n).map_err(|_| Error::TooLarge {` with
    /// `Ok(u32::try_from(n).unwrap_or(u32::MAX)).map_err(|_: ()| Error::TooLarge {`
    /// — the clamp the review named. Watched: the over-long length gives `Ok`.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn a_length_that_does_not_fit_four_bytes_is_refused() {
        assert_eq!(wire_u32(7).expect("fits"), 7);
        assert_eq!(wire_u32(u32::MAX as usize).expect("fits"), u32::MAX);
        let error = wire_u32(u32::MAX as usize + 1).expect_err("does not fit");
        assert!(
            matches!(error, Error::TooLarge { offered, limit }
                if offered == u64::from(u32::MAX) + 1 && limit == u64::from(u32::MAX)),
            "got {error:?}"
        );
    }
}
