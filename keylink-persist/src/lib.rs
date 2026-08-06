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
//!                            │  Durable::snapshot / restore
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
//! # What this does not promise
//!
//! - **Nothing about other machines.** The file adapter survives a restart of
//!   the process. It does not survive the loss of the disk. A bucket adapter
//!   would; that is the reason to write one, and the only reason.
//! - **No history.** One state, one current value. A journal is a different
//!   port with different operations.
//! - **No migration.** [`Durable::restore`] reports bytes it does not
//!   understand. It does not upgrade them.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

use std::fmt;
use std::future::Future;

#[cfg(feature = "file")]
pub mod file;

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

/// State that knows how to become bytes and come back.
///
/// Deliberately not `Serialize`: the encoding is the state's own decision, and
/// a state that wants postcard, JSON or a hand-rolled format is equally welcome.
/// This crate never looks inside.
pub trait Durable: Sized {
    /// The bytes to store.
    ///
    /// Call this **inside a turn**, where `&mut self` is exclusive. That is what
    /// makes the result a consistent point rather than a torn read.
    fn snapshot(&self) -> Vec<u8>;

    /// Rebuild from bytes this type wrote earlier.
    ///
    /// # Errors
    ///
    /// [`RestoreError`] when the bytes were written by a different version of
    /// this type. There is no migration here, and pretending otherwise would be
    /// worse than saying so.
    fn restore(bytes: &[u8]) -> std::result::Result<Self, RestoreError>;
}

/// What a state reports when it cannot read its own stored bytes.
#[derive(Debug, thiserror::Error)]
#[error("the stored snapshot could not be restored: {0}")]
pub struct RestoreError(pub String);

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

    /// A snapshot larger than the adapter will store.
    #[error("the snapshot is {len} bytes and the limit is {limit}")]
    TooLarge {
        /// What was offered.
        len: u64,
        /// The cap.
        limit: u64,
    },

    /// The underlying store failed.
    #[error("the snapshot store failed: {0}")]
    Io(#[from] std::io::Error),
}
