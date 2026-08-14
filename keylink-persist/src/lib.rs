//! State that outlives its process, behind a port with one adapter.
//!
//! # The hexagon
//!
//! The domain need is small and does not mention a filesystem, a bucket or a
//! network: **one state, loaded once at start and replaced atomically after
//! that, written by one process at a time.**
//!
//! That sentence is [`Snapshots`] and [`Claim`]. Everything else is an adapter.
//!
//! ```text
//!            ┌──────────────────────────────┐
//!            │  the actor, holding &mut self │
//!            └───────────────┬──────────────┘
//!                            │  Durable::snapshot / restore / migrate
//!            ┌───────────────▼──────────────┐
//!            │  Versioned    the frame      │   ← the only writer of a header
//!            └───────────────┬──────────────┘
//!            ┌───────────────▼──────────────┐
//!            │  PORT   Snapshots · Claim    │   ← one whole value at a time
//!            ├──────────────────────────────┤
//!            │  PORT   Entries   · Held     │   ← many small ones, separately
//!            └───┬─────────────┬────────────┘
//!                │             │
//!        ┌───────▼──────┐  ┌───▼──────────────┐
//!        │ file adapter │  │ s3, vault, …     │   ← one exists, on purpose
//!        │  (built)     │  │ (not built)      │
//!        └──────────────┘  └──────────────────┘
//! ```
//!
//! # The second need, and why it is a second port
//!
//! **Many small entries, each written and removed on its own.** An idempotency
//! ledger is the case: one record per settled call, arriving one at a time and
//! expiring one at a time.
//!
//! [`Snapshots`] can express that and should not. `Claim::store` replaces the
//! whole value, so recording one reply would rewrite every reply already held —
//! quadratic in the number of records, and the cost is paid on the call path
//! rather than at a checkpoint. [`Entries`] and [`Held`] are the same sentence
//! with the granularity the caller actually has.
//!
//! One adapter carries both, because a store that can hold one value can hold
//! many. They stay two traits because a caller needs exactly one of them and
//! reading the other tells it nothing.
//!
//! # Why the claim is in the port and not in an adapter
//!
//! Every adapter can enforce "one writer", and each does it differently: a file
//! lock here, a conditional write on an ETag in a bucket, a lease epoch against
//! a server. If the port did not say so, an adapter could quietly not do it, and
//! the caller could not tell which kind it had. So exclusivity is the port's
//! promise and [`Claim`] is the proof — **there is no way to store bytes without
//! holding one**, because `store` is a method on the claim rather than on the
//! store.
//!
//! That is the whole type-level content of this crate. It deletes the guard that
//! would otherwise read "remember to take the lock first".
//!
//! # The second one: bytes on disk say which version wrote them
//!
//! [`Claim::store`] takes raw bytes, because an adapter for a bucket or a vault
//! must not have to understand framing. [`Versioned`](versioned::Versioned)
//! **owns** a claim and is the only thing here that writes a header, and there
//! is no route from one back to the claim it holds. So a caller who has a
//! `Versioned` cannot write an unframed byte, and an adapter author never
//! learns that versions exist.
//!
//! What it stores is a [`Snapshot<D>`](versioned::Snapshot) rather than bytes,
//! so the version in the header is the one declared by the type that produced
//! the payload. Naming `D` beside a `&[u8]` instead let a caller stamp a v1
//! snapshot as v3, which the loader then believed.
//!
//! The header carries a magic, which is what makes an unframed file — anything
//! this crate wrote before framing existed — a loud error rather than a payload
//! misread as garbage. [`versioned`] documents the layout byte by byte.
//!
//! # What this does not promise
//!
//! - **Nothing about other machines.** The file adapter survives a restart of
//!   the process. It does not survive the loss of the disk. A bucket adapter
//!   would; that is the reason to write one, and the only reason.
//! - **No history.** One state, one current value. A journal is a different
//!   port with different operations.
//! - **No guess about the payload.** Migration walks
//!   [`Durable::migrate`] one hop at a time and this crate never parses what
//!   comes back. A state whose encoding changed without
//!   [`Durable::VERSION`] changing is undetectable from here, and
//!   [`Durable::VERSION`] says so in as many words.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

use std::fmt;
use std::future::Future;

#[cfg(feature = "file")]
pub mod file;
pub mod versioned;

mod representability;

// ───────────────────────────────────────────────────────────────── the port

/// A place snapshots live.
///
/// The only operation is [`claim`](Self::claim), because reading and writing
/// both require exclusivity and handing out either without it would be the bug
/// this type exists to prevent.
pub trait Snapshots: Send + Sync + 'static {
    /// The exclusive handle this store hands out.
    type Claim: Claim;

    /// Take exclusive write access to one state.
    ///
    /// Held until the claim is dropped. What "exclusive" is enforced against is
    /// the adapter's business: the file adapter excludes every other process on
    /// the machine, and says nothing about other machines.
    ///
    /// # Errors
    ///
    /// [`Error::Busy`] when somebody else holds it. That is not a transient
    /// error to retry in a loop — it means another process believes it owns this
    /// state, and two owners is the situation the claim exists to prevent.
    fn claim(&self, id: &StateId) -> impl Future<Output = Result<Self::Claim>> + Send;
}

/// Exclusive access to one state, for as long as this value lives.
///
/// Dropping it releases the claim. There is deliberately no `release` method:
/// one way to give it up, and it is the one that also runs when the task
/// unwinds.
pub trait Claim: Send + 'static {
    /// The stored bytes, or `None` when this state has never been stored.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the store cannot be read.
    fn load(&self) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;

    /// Replace the stored bytes.
    ///
    /// **Atomic.** A crash during this leaves either the previous bytes or the
    /// new ones, and never a mixture. An adapter that cannot promise that is not
    /// an adapter for this port.
    ///
    /// Takes `&mut self` because a claim is exclusive and the type should say
    /// so at the call site, not only in this sentence.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the write fails. The previous snapshot is intact.
    fn store(&mut self, bytes: &[u8]) -> impl Future<Output = Result<()>> + Send;
}

// ─────────────────────────────────────────────────── the port, entry by entry

/// A place many small entries live, each written and removed on its own.
///
/// The sibling of [`Snapshots`], and the difference is granularity rather than
/// durability: [`Claim::store`] replaces one whole value, and [`Held::put`]
/// writes one entry out of many. A caller with a growing set of independent
/// records — an idempotency ledger, a set of leases — would otherwise rewrite
/// the whole set to add one.
///
/// [`claim`](Self::claim) for the reason [`Snapshots::claim`] is the only
/// operation there: two writers of one set is the situation exclusivity exists
/// to make visible.
pub trait Entries: Send + Sync + 'static {
    /// The exclusive handle this store hands out.
    type Held: Held;

    /// Take exclusive write access to one set of entries.
    ///
    /// Held until the handle is dropped, and enforced against whatever the
    /// adapter can reach — for the file adapter, every other process on the
    /// machine, and nothing beyond it.
    ///
    /// # Errors
    ///
    /// [`Error::Busy`] when somebody else holds it, which is not a wait to
    /// retry. [`Error::Io`] when the store cannot be opened.
    fn claim(&self, id: &StateId) -> impl Future<Output = Result<Self::Held>> + Send;
}

/// Exclusive access to one set of entries, for as long as this value lives.
///
/// Dropping it releases the claim, exactly as [`Claim`] does.
///
/// # What "durable" means on each method
///
/// [`put`](Self::put) and [`remove`](Self::remove) resolve **after** the change
/// is on the storage device. That is the property the whole port exists for: a
/// caller may treat a resolved `put` as a fact that survives losing the process,
/// and may not treat an unresolved one as anything at all.
///
/// A caller that crashes mid-`put` sees, on the next [`load`](Self::load),
/// either the entry or no entry. Never half of one.
pub trait Held: Send + 'static {
    /// Every entry currently stored.
    ///
    /// The recovery path, called once before the caller starts writing. Order
    /// is the order the entries were last written in, which a caller is free to
    /// ignore.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the store cannot be read, and [`Error::NotALog`] when
    /// what is stored is not something this build can read.
    fn load(&self) -> impl Future<Output = Result<Vec<(RecordKey, Vec<u8>)>>> + Send;

    /// Write one entry, replacing any entry already under `key`.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] when the entry is over the adapter's cap, and
    /// [`Error::Io`] when the write fails. Neither disturbs what is stored.
    fn put(&mut self, key: &RecordKey, bytes: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Remove one entry. Removing an entry that is not there is not an error.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the write fails.
    fn remove(&mut self, key: &RecordKey) -> impl Future<Output = Result<()>> + Send;

    /// Reclaim whatever removed and replaced entries still cost.
    ///
    /// **Never required for correctness**, and a caller that never calls it
    /// reads back exactly what a caller that calls it every time reads back.
    /// What it changes is the space a store occupies and how long
    /// [`load`](Self::load) takes, and an adapter that has nothing to reclaim
    /// implements it as a no-op.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the rewrite fails. What was stored is intact.
    fn compact(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// One entry's address within a [`Held`] set.
///
/// Opaque bytes with a length bound and nothing else, which is deliberate: a
/// caller's key is a caller's business, and an adapter that needed it to be a
/// path or an identifier would be asking the caller to escape its own data.
/// [`StateId`] names a *state* and is a path on purpose; this names an entry
/// inside one and is not.
///
/// Validated once, here, so an adapter never meets an empty or unbounded key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordKey(Vec<u8>);

impl RecordKey {
    /// The longest key this accepts.
    ///
    /// A judgement rather than a measurement: long enough for a peer identity
    /// and a caller-chosen string together, short enough that an adapter can
    /// hold the whole key set in memory while it reads.
    pub const MAX_LEN: usize = 256;

    /// Validate a key.
    ///
    /// # Errors
    ///
    /// [`InvalidRecordKey`] naming which rule was broken.
    pub fn new(bytes: impl Into<Vec<u8>>) -> std::result::Result<Self, InvalidRecordKey> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(InvalidRecordKey::Empty);
        }
        if bytes.len() > Self::MAX_LEN {
            return Err(InvalidRecordKey::TooLong {
                len: bytes.len(),
                max: Self::MAX_LEN,
            });
        }
        Ok(Self(bytes))
    }

    /// The key, as written.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Why a key was refused.
///
/// `Copy`, unlike [`InvalidStateId`], because every variant holds numbers: this
/// is reported from inside [`Error::NotALog`], which an adapter builds while
/// reading, and a reason that had to be cloned there would be a reason an
/// adapter is tempted to drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidRecordKey {
    /// A key with nothing in it.
    #[error("a record key cannot be empty")]
    Empty,
    /// Longer than [`RecordKey::MAX_LEN`].
    #[error("a record key is at most {max} bytes, and this one is {len}")]
    TooLong {
        /// What arrived.
        len: usize,
        /// The cap.
        max: usize,
    },
}

/// State that knows how to become bytes, come back, and grow a field without
/// orphaning what is already on disk.
///
/// Deliberately not `Serialize`: the encoding is the state's own decision, and
/// a state that wants postcard, JSON or a hand-rolled format is equally welcome.
/// This crate never looks inside.
pub trait Durable: Sized {
    /// Which revision of this type's encoding [`snapshot`](Self::snapshot)
    /// writes.
    ///
    /// Stamped into the frame by
    /// [`Versioned::store`](versioned::Versioned::store) and read back by
    /// [`Versioned::load`](versioned::Versioned::load), which is what decides
    /// whether [`migrate`](Self::migrate) runs.
    ///
    /// # The failure nobody can type against
    ///
    /// **Change the encoding, bump this.** Nothing here can check that you did.
    /// The frame stamps whatever number this constant holds, and the payload is
    /// opaque to this crate by design — so a `snapshot` that started writing a
    /// new shape under an unchanged `VERSION` writes new bytes wearing an old
    /// label, and the old bytes already on disk wear the same one.
    ///
    /// What happens then depends on the new `restore`, and neither outcome is
    /// good:
    ///
    /// - It rejects the old bytes, and the state fails closed as
    ///   [`RestoreError::Malformed`] — loud, at startup, recoverable. This is
    ///   the likely case and it is the one worth having.
    /// - It *accepts* them, because the two encodings overlap. A ledger of
    ///   little-endian `u64`s that grew a header is exactly this: every old
    ///   file is still a valid new file, and it restores as nonsense. No
    ///   frame can see that, because seeing it would mean parsing the payload.
    ///
    /// The frame closes the version question. It cannot close this one, and
    /// pretending otherwise would be worse than saying so.
    const VERSION: u32;

    /// The bytes to store.
    ///
    /// Call this **inside a turn**, where `&mut self` is exclusive. That is what
    /// makes the result a consistent point rather than a torn read.
    fn snapshot(&self) -> Vec<u8>;

    /// Rebuild from bytes this type wrote at [`VERSION`](Self::VERSION).
    ///
    /// Never called with bytes from another version: the loader has already
    /// refused a newer snapshot and walked an older one forward, so what
    /// arrives here is always this revision's own shape.
    ///
    /// # Errors
    ///
    /// [`RestoreError::Malformed`], which is this trait's only channel. The
    /// other variants are the loader's verdicts about provenance, and an
    /// implementation that could construct them could lie about it.
    fn restore(bytes: &[u8]) -> std::result::Result<Self, RestoreError>;

    /// ONE step: `from` to `from + 1`.
    ///
    /// The loader walks the chain, so a v1 author never learns that v4 exists
    /// and a v4 author writes one arm rather than three. "Migrate from any
    /// older version" is the alternative, and it makes every implementor
    /// re-implement every earlier hop forever.
    ///
    /// Return `Ok(None)` for a hop this type does not have. That is a fact
    /// about the type and not an error: whether a gap is fatal depends on which
    /// version is on disk, which only the loader knows, and it reports
    /// [`RestoreError::NoMigration`].
    ///
    /// **Required, not defaulted.** A defaulted body saves one line for a v1
    /// author and lets a v3 author forget the hop and still compile, which is
    /// the trade this trait refuses.
    ///
    /// ```
    /// # use keylink_persist::{Durable, RestoreError};
    /// # struct Ledger;
    /// # impl Ledger {
    /// #     fn total_of(_: &[u8]) -> Vec<u8> { Vec::new() }
    /// #     fn count_of(_: &[u8]) -> Vec<u8> { Vec::new() }
    /// # }
    /// # impl Durable for Ledger {
    /// # const VERSION: u32 = 3;
    /// # fn snapshot(&self) -> Vec<u8> { Vec::new() }
    /// # fn restore(_: &[u8]) -> Result<Self, RestoreError> { Ok(Self) }
    /// fn migrate(from: u32, bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
    ///     Ok(match from {
    ///         1 => Some(Ledger::total_of(bytes)),
    ///         2 => Some(Ledger::count_of(bytes)),
    ///         _ => None,
    ///     })
    /// }
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`RestoreError::Malformed`] when the bytes are not readable as version
    /// `from`. A hop that simply does not exist is `Ok(None)`, not an error.
    fn migrate(from: u32, bytes: &[u8]) -> std::result::Result<Option<Vec<u8>>, RestoreError>;
}

/// Why a stored snapshot did not become a state.
///
/// Three of these are the **loader's** verdicts about provenance, and their
/// variants are `#[non_exhaustive]` so that only this crate can construct them.
/// A [`Durable::restore`] that could return `FromTheFuture` could claim a
/// version comparison it never made. [`Malformed`](Self::Malformed) stays
/// constructible; it is the implementor's channel and the only one it needs.
///
/// # How that is guarded
///
/// `#[non_exhaustive]` is inert inside its defining crate, so
/// `keylink-persist/src/representability.rs` cannot see it and the tests in
/// `keylink-persist/tests/versioned.rs` cannot either — a test that only ever
/// *matches* a variant, always with `..`, matches an ordinary variant exactly
/// as happily. Construction is the one thing the attribute forbids, so the
/// guard is a `compile_fail` doctest on each of the three variants: a doctest
/// compiles as its own crate, which is where the attribute is in force.
///
/// Those three blocks are the reason this decision is not a comment. Delete an
/// attribute and its block starts compiling, and rustdoc fails a `compile_fail`
/// test that compiled — watched, not assumed.
///
/// Each fails with `E0639: cannot create non-exhaustive variant using struct
/// expression`. **The code is named here and deliberately NOT on the fences.**
/// An earlier revision of this paragraph claimed a fence "annotated with an error
/// code checks only that `E0639` appeared, and error codes are stable where
/// diagnostic wording is not". The second half is true and the first is false:
/// **rustdoc does not check the code at all.** Demonstrated twice while this
/// crate was being reviewed — doctests annotated `compile_fail,E0308` that in
/// fact fail with `E0425` (a typo in the probe body) or `E0282` pass green and
/// silently, and swapping a pinned `E0624` for `E0599` elsewhere in this
/// workspace left its test green too.
///
/// So an annotated code asserts a verification that does not happen, which is
/// the shape of every guard this project has caught being satisfiable without
/// proving its claim. `keylink-persist/src/representability.rs` objects to
/// `trybuild` for matching full rustc stderr, and that objection stands; the
/// answer is not a code on a fence but the narrower claim these three blocks can
/// actually make — this does not compile — plus
/// `keylink-persist/tests/versioned.rs` naming the variants from outside the
/// crate, so a rename cannot pass here by making the name vanish.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RestoreError {
    /// The stored bytes are not a frame this build can read.
    ///
    /// **A snapshot written before this crate framed anything lands here**, and
    /// that is the whole reason the frame carries a magic. Without one, an
    /// unframed file would be read as header-plus-payload and restore
    /// successfully with garbage.
    ///
    /// A [`Durable`] cannot report it, because it did not read the header:
    ///
    /// ```compile_fail
    /// use keylink_persist::versioned::FrameFault;
    /// use keylink_persist::RestoreError;
    ///
    /// let _ = RestoreError::NotAFrame { fault: FrameFault::WrongMagic };
    /// ```
    #[error("the stored bytes are not a snapshot frame this build can read: {fault}")]
    #[non_exhaustive]
    NotAFrame {
        /// Which header check refused it.
        fault: versioned::FrameFault,
    },

    /// The snapshot was written by a newer build than this one.
    ///
    /// Refused, and the stored bytes are left exactly as they were. The
    /// tempting alternative is to read the fields this build knows and drop the
    /// rest, which discards whatever the newer build added and then writes the
    /// truncation back over it.
    ///
    /// A [`Durable`] cannot report it, because it made no version comparison:
    ///
    /// ```compile_fail
    /// use keylink_persist::RestoreError;
    ///
    /// let _ = RestoreError::FromTheFuture { stored: 9, ours: 1 };
    /// ```
    #[error("the snapshot is version {stored} and this build understands {ours}")]
    #[non_exhaustive]
    FromTheFuture {
        /// The version stamped in the frame.
        stored: u32,
        /// [`Durable::VERSION`] of the type being loaded.
        ours: u32,
    },

    /// The migration chain has a gap: [`Durable::migrate`] has no hop from
    /// `from` to `from + 1`, and the loader will not jump over it.
    ///
    /// A [`Durable`] cannot report it, which is precisely why
    /// [`migrate`](Durable::migrate) returns `Ok(None)` for a hop it does not
    /// have: whether a gap is fatal depends on what is on the disk, and only
    /// the loader knows that.
    ///
    /// ```compile_fail
    /// use keylink_persist::RestoreError;
    ///
    /// let _ = RestoreError::NoMigration { from: 1 };
    /// ```
    #[error("no migration from version {from}")]
    #[non_exhaustive]
    NoMigration {
        /// The missing hop's starting version.
        from: u32,
    },

    /// What a [`Durable`] reports about bytes it cannot read.
    #[error("the stored snapshot could not be restored: {0}")]
    Malformed(String),
}

// ──────────────────────────────────────────────────────────────── the name

/// The name of one state.
///
/// Slash-separated segments, so `warehouse/sp-01` reads as a path on a disk and
/// as a key in a bucket. Validated once, here, which is what lets the file
/// adapter build a path from it with no escaping and no guard:
///
/// - one to [`MAX_LEN`](Self::MAX_LEN) bytes,
/// - segments of `a-z`, `0-9`, `.`, `_`, `-`, separated by `/`,
/// - no empty segment, so no `//`, no leading and no trailing slash,
/// - **no segment that starts with `.`**, which is what makes `.` and `..`
///   unrepresentable and therefore makes path traversal unrepresentable.
///
/// The last rule is the load-bearing one. `StateId::new("../../etc/passwd")`
/// does not return a value that an adapter has to be careful with. It returns an
/// error.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateId(String);

impl StateId {
    /// The longest name this accepts.
    ///
    /// A judgement, not a measurement: long enough for a tenant, a kind and an
    /// identifier, short enough that a path built from it survives a filesystem
    /// with a 255-byte component limit.
    pub const MAX_LEN: usize = 128;

    /// Validate a name.
    ///
    /// # Errors
    ///
    /// [`InvalidStateId`] naming which rule was broken.
    pub fn new(name: impl Into<String>) -> std::result::Result<Self, InvalidStateId> {
        let name = name.into();
        if name.is_empty() {
            return Err(InvalidStateId::Empty);
        }
        if name.len() > Self::MAX_LEN {
            return Err(InvalidStateId::TooLong {
                len: name.len(),
                max: Self::MAX_LEN,
            });
        }
        for segment in name.split('/') {
            if segment.is_empty() {
                return Err(InvalidStateId::EmptySegment);
            }
            if segment.starts_with('.') {
                return Err(InvalidStateId::DotSegment {
                    segment: segment.to_owned(),
                });
            }
            if let Some(bad) = segment
                .chars()
                .find(|c| !matches!(c, 'a'..='z' | '0'..='9' | '.' | '_' | '-'))
            {
                return Err(InvalidStateId::BadCharacter { found: bad });
            }
        }
        Ok(Self(name))
    }

    /// The name, as written.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The segments, in order. What an adapter walks to build a path or a key.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('/')
    }
}

impl fmt::Display for StateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a name was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidStateId {
    /// A name with nothing in it.
    #[error("a state id cannot be empty")]
    Empty,
    /// Longer than [`StateId::MAX_LEN`].
    #[error("a state id is at most {max} bytes, and this one is {len}")]
    TooLong {
        /// What arrived.
        len: usize,
        /// The cap.
        max: usize,
    },
    /// A `//`, or a leading or trailing slash.
    #[error("a state id has no empty segments: no leading, trailing or doubled '/'")]
    EmptySegment,
    /// A segment beginning with `.`, which includes `.` and `..`.
    #[error("a segment cannot start with '.', and '{segment}' does")]
    DotSegment {
        /// The offending segment.
        segment: String,
    },
    /// Anything outside `a-z`, `0-9`, `.`, `_`, `-`.
    #[error("a state id holds only a-z, 0-9, '.', '_', '-' and '/', and this has {found:?}")]
    BadCharacter {
        /// The first character that was not allowed.
        found: char,
    },
}

// ─────────────────────────────────────────────────────────────── the errors

/// What this crate reports.
pub type Result<T> = std::result::Result<T, Error>;

/// Which check refused a stored set of entries.
///
/// The sibling of [`versioned::FrameFault`], and it lives here for the reason
/// that one lives beside the frame it describes: the layout an entry store
/// writes is this crate's, not each adapter's, so a second adapter reading a log
/// written by the first is reading a format both were told.
///
/// `#[non_exhaustive]` because an adapter over something other than a file can
/// refuse for reasons a file cannot have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EntryFault {
    /// The stored bytes do not begin with the entry log's magic.
    ///
    /// What a file written by something else, or by a build from before entry
    /// logs existed, looks like. It is a loud error rather than a payload read
    /// as garbage, which is the whole reason a magic is written at all.
    #[error("the stored bytes do not begin with this crate's entry-log magic")]
    WrongMagic,
    /// An entry names an operation this build does not know.
    #[error("an entry names operation {op}, which this build does not know")]
    UnknownOperation {
        /// The byte that was read.
        op: u8,
    },
    /// An entry's key is not one [`RecordKey`] accepts.
    ///
    /// The load path re-validates every key, because bytes read off a disk
    /// never met the constructor.
    #[error("an entry's key was refused: {0}")]
    BadKey(InvalidRecordKey),
}

/// Why a store operation did not happen.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Another holder has the claim.
    ///
    /// Not a transient error. It means a second process believes it owns this
    /// state, which is the situation a claim exists to make visible rather than
    /// to wait out.
    #[error("another process holds the claim on {id}")]
    Busy {
        /// The state that is claimed.
        id: StateId,
    },

    /// More bytes than the adapter will store.
    ///
    /// The field is named `offered` rather than `len` because of what it counts
    /// after framing. A limit applies to what reaches the adapter, and what
    /// reaches the adapter through
    /// [`Versioned::store`](versioned::Versioned::store) is the payload **plus**
    /// [`versioned::FRAME_LEN`] bytes of header. So a caller who handed over
    /// exactly `limit` bytes of snapshot is refused with a number twelve larger
    /// than the one they passed, and the alternative — reporting the payload
    /// length against a limit that includes the header — would print
    /// `120 bytes, limit 128` beside a refusal.
    ///
    /// The honest fix is for the number to say which number it is. Headroom for
    /// a payload is `limit - versioned::FRAME_LEN`.
    #[error("the store was offered {offered} bytes and the limit is {limit}")]
    TooLarge {
        /// What reached the adapter: the payload, plus the frame when there was
        /// one.
        offered: u64,
        /// The cap.
        limit: u64,
    },

    /// Stored bytes that did not become a state.
    #[error(transparent)]
    Restore(#[from] RestoreError),

    /// Stored entries that did not become a set.
    ///
    /// Reported by [`Held::load`], and it is a *refusal to guess*: the bytes
    /// under this id are not something this build can read as entries, so it
    /// says so instead of returning the prefix it understood. Recovering from
    /// half a ledger is worse than recovering from none, because a caller
    /// cannot see which half is missing.
    ///
    /// **A truncated tail is not this**, and the difference is the whole reason
    /// the two are told apart. An entry that was being written when the process
    /// died is incomplete, expected, and dropped in silence — the caller
    /// treated it as unwritten anyway, because the `put` never resolved.
    #[error("the stored entries under {id} could not be read: {fault}")]
    NotALog {
        /// Whose entries they are.
        id: StateId,
        /// Which check refused them.
        fault: EntryFault,
    },

    /// The underlying store failed.
    #[error("the snapshot store failed: {0}")]
    Io(#[from] std::io::Error),
}
