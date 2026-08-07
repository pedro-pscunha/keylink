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
//!            │  PORT   Snapshots · Claim    │   ← this crate's whole surface
//!            └───┬─────────────┬────────────┘
//!                │             │
//!        ┌───────▼──────┐  ┌───▼──────────────┐
//!        │ file adapter │  │ s3, vault, …     │   ← one exists, on purpose
//!        │  (built)     │  │ (not built)      │
//!        └──────────────┘  └──────────────────┘
//! ```
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
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RestoreError {
    /// The stored bytes are not a frame this build can read.
    ///
    /// **A snapshot written before this crate framed anything lands here**, and
    /// that is the whole reason the frame carries a magic. Without one, an
    /// unframed file would be read as header-plus-payload and restore
    /// successfully with garbage.
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

    /// The underlying store failed.
    #[error("the snapshot store failed: {0}")]
    Io(#[from] std::io::Error),
}
