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
//! The `framing` task in `mise.toml` is the belt to those braces: it fails if
//! [`Claim`] is named outside this crate at all, because a trait method is only
//! callable where the trait is in scope.
//!
//! # The byte layout
//!
//! ```text
//! offset  bytes  field     value
//!      0      6  magic     b"klsnap"
//!      6      2  layout    u16 little-endian — this header's own version, 1
//!      8      4  version   u32 little-endian — Durable::VERSION of the payload
//!     12      *  payload   Durable::snapshot's bytes, never parsed here
//! ```
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
//! `version` is a `u32` because that is what [`Durable::VERSION`] is: four
//! billion revisions of one state is not a second break. `layout` is a `u16`
//! because this table is expected to change approximately never, and the two
//! together keep the header at twelve bytes, which is `u32`-aligned.
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

use crate::{Claim, Durable, RestoreError, Result};

// ────────────────────────────────────────────────────────────────── the frame

/// The first six bytes of every framed snapshot.
const MAGIC: [u8; 6] = *b"klsnap";

/// The version of the header table itself, not of anybody's state.
const LAYOUT: u16 = 1;

/// The size of the header every framed snapshot carries.
///
/// A caller sizing a store's limit needs this, because a limit applies to what
/// reaches the adapter and what reaches the adapter is the payload plus this.
/// See [`Error::TooLarge`](crate::Error::TooLarge).
pub const FRAME_LEN: usize = 12;

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
    #[error("the frame layout is version {stored} and this build writes {ours}")]
    LayoutFromTheFuture {
        /// What the header claims.
        stored: u16,
        /// What this build understands.
        ours: u16,
    },
}

/// Wrap `payload` in a header stamping it as version `version`.
fn frame(version: u32, payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(FRAME_LEN + payload.len());
    framed.extend_from_slice(&MAGIC);
    framed.extend_from_slice(&LAYOUT.to_le_bytes());
    framed.extend_from_slice(&version.to_le_bytes());
    framed.extend_from_slice(payload);
    framed
}

/// Check the header and hand back the version it stamps and the payload it
/// wraps. The payload is not parsed, here or anywhere in this crate.
fn unframe(bytes: &[u8]) -> std::result::Result<(u32, &[u8]), RestoreError> {
    let fault = |fault| RestoreError::NotAFrame { fault };

    let header: [u8; FRAME_LEN] = bytes
        .get(..FRAME_LEN)
        .and_then(|head| head.try_into().ok())
        .ok_or_else(|| fault(FrameFault::TooShort { len: bytes.len() }))?;

    if header[..MAGIC.len()] != MAGIC {
        return Err(fault(FrameFault::WrongMagic));
    }

    let layout = u16::from_le_bytes([header[6], header[7]]);
    if layout != LAYOUT {
        return Err(fault(FrameFault::LayoutFromTheFuture {
            stored: layout,
            ours: LAYOUT,
        }));
    }

    let version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    Ok((version, &bytes[FRAME_LEN..]))
}

/// Turn framed bytes back into a state, walking the migration chain.
///
/// Split out from [`Versioned::load`] so that the decision tree is one
/// function with no I/O in it.
fn decode<D: Durable>(bytes: &[u8]) -> std::result::Result<D, RestoreError> {
    let (stored, payload) = unframe(bytes)?;

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

// ──────────────────────────────────────────────────────────────── the wrapper

/// A claim that only ever sees framed bytes.
///
/// Owns the claim it was built from. That ownership is the guarantee: there is
/// no accessor, no `Deref`, no `From` and no `into_inner`, so nothing holding a
/// `Versioned` can write an unframed byte. `keylink-persist/src/representability.rs`
/// holds the compile-time probes for that sentence.
///
/// ```no_run
/// # use keylink_persist::{Durable, RestoreError, Snapshots, StateId};
/// # use keylink_persist::file::Files;
/// # use keylink_persist::versioned::Versioned;
/// # struct Counter(u64);
/// # impl Durable for Counter {
/// #     const VERSION: u32 = 1;
/// #     fn snapshot(&self) -> Vec<u8> { self.0.to_le_bytes().to_vec() }
/// #     fn restore(_: &[u8]) -> Result<Self, RestoreError> { Ok(Self(0)) }
/// #     fn migrate(_: u32, _: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> { Ok(None) }
/// # }
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let files = Files::open("/var/lib/keylink").await?;
/// let id = StateId::new("counters/hits")?;
///
/// let mut state = Versioned::new(files.claim(&id).await?);
/// let mut counter = state.load::<Counter>().await?.unwrap_or(Counter(0));
///
/// counter.0 += 1;
/// state.store::<Counter>(&counter.snapshot()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Versioned<C: Claim> {
    claim: C,
}

impl<C: Claim> Versioned<C> {
    /// Take a claim, and give nothing back.
    ///
    /// The claim moves in. Exclusivity is unchanged — it is still held until
    /// this value is dropped, and still released by the operating system if the
    /// process ends first.
    pub fn new(claim: C) -> Self {
        Self { claim }
    }

    /// Read the state, migrating it if what is stored is older.
    ///
    /// `Ok(None)` is the first ever run: nothing has been stored, which is not
    /// a failure.
    ///
    /// **Migration does not write.** This takes `&self` and touches nothing on
    /// the store, so an old snapshot stays old until the caller's next
    /// [`store`](Self::store) replaces it. That is the safe order — a crash
    /// between the two leaves the bytes that were already there, and the chain
    /// simply runs again — and it is why a process that loads and never stores
    /// pays for the walk on every start. If that cost matters, store once after
    /// loading; there is deliberately no way to make a read do it silently.
    ///
    /// # Errors
    ///
    /// - [`Error::Io`](crate::Error::Io) when the store cannot be read.
    /// - [`Error::Restore`](crate::Error::Restore) wrapping
    ///   [`RestoreError::NotAFrame`] for bytes that are not a snapshot this
    ///   build can read — an unframed file from before framing existed is this
    ///   case, and it is refused rather than mis-parsed;
    ///   [`RestoreError::FromTheFuture`] for a snapshot a newer build wrote,
    ///   which is refused and never truncated;
    ///   [`RestoreError::NoMigration`] for a gap in the chain; and whatever
    ///   [`Durable::restore`] or [`Durable::migrate`] reported.
    pub async fn load<D: Durable>(&self) -> Result<Option<D>> {
        match self.claim.load().await? {
            Some(bytes) => Ok(Some(decode::<D>(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Frame `snapshot` as version [`D::VERSION`](Durable::VERSION) and replace
    /// what is stored.
    ///
    /// `snapshot` is [`D::snapshot`](Durable::snapshot)'s output. It arrives as
    /// bytes rather than as a `&D` because by the time a state is committed it
    /// usually lives inside an actor, and the only consistent read of it is
    /// through a turn — `handle.with_state(D::snapshot)` hands back bytes, not
    /// a borrow that outlives the turn. Naming `D` at the call site is the
    /// price, and it is not much of one: `D` is what decides the version stamp,
    /// so spelling it out is the honest thing to ask for.
    ///
    /// Atomic, because [`Claim::store`] is. A crash during this leaves either
    /// the previous framed snapshot or the new one.
    ///
    /// # Errors
    ///
    /// - [`Error::TooLarge`](crate::Error::TooLarge) when the frame **plus**
    ///   the payload is over the adapter's limit. The number it reports is what
    ///   reached the adapter, which is [`FRAME_LEN`] more than `snapshot.len()`.
    /// - [`Error::Io`](crate::Error::Io) when the write fails. The previous
    ///   snapshot is intact.
    pub async fn store<D: Durable>(&mut self, snapshot: &[u8]) -> Result<()> {
        self.claim.store(&frame(D::VERSION, snapshot)).await
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
                    .chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8)")))
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

    #[test]
    fn the_header_is_the_documented_twelve_bytes() {
        let framed = frame(7, b"payload");
        assert_eq!(framed.len(), FRAME_LEN + 7);
        assert_eq!(&framed[..6], b"klsnap");
        assert_eq!(&framed[6..8], &1u16.to_le_bytes());
        assert_eq!(&framed[8..12], &7u32.to_le_bytes());
        assert_eq!(&framed[12..], b"payload");
    }

    #[test]
    fn a_frame_round_trips_through_unframe() {
        let framed = frame(4, b"whatever");
        assert_eq!(unframe(&framed).expect("a frame"), (4, &b"whatever"[..]));
    }

    /// The silent-corruption case, at the smallest scale it can be shown.
    /// Twenty-four raw bytes are three valid ledger entries, so a loader with
    /// no magic would hand back a `Ledger` built from the header.
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

    #[test]
    fn a_file_too_short_to_hold_a_header_says_so() {
        assert!(matches!(
            unframe(b"klsna"),
            Err(RestoreError::NotAFrame {
                fault: FrameFault::TooShort { len: 5 }
            })
        ));
    }

    #[test]
    fn a_header_layout_this_build_does_not_write_is_refused() {
        let mut framed = frame(1, b"12345678");
        framed[6] = 2;
        assert!(matches!(
            unframe(&framed),
            Err(RestoreError::NotAFrame {
                fault: FrameFault::LayoutFromTheFuture { stored: 2, ours: 1 }
            })
        ));
    }

    #[test]
    fn an_empty_payload_is_a_payload_and_not_a_fault() {
        let framed = frame(1, b"");
        assert_eq!(unframe(&framed).expect("a frame"), (1, &b""[..]));
        assert_eq!(decode::<Ledger>(&framed).expect("restores").0, Vec::new());
    }
}
