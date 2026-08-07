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
//! **A layout that is not this one gets two different verdicts**, and which one
//! it gets starts mattering on the day `LAYOUT` becomes 2 — which is the entire
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
//! because this table is expected to change approximately never, and the two
//! together keep the header at twelve bytes.
//!
//! **Twelve is load-bearing, and not because it is `u32`-aligned.** It is
//! load-bearing because it is *not* a multiple of eight. The magic is the first
//! line against an unframed file and a payload's own length check is the second:
//! a raw `Vec<u64>` file is `8n` bytes, so on the day its first twelve bytes
//! happen to spell a valid header the payload handed on is `8n − 12` bytes,
//! which is `≡ 4 (mod 8)` for every `n` and therefore never a whole number of
//! `u64`s. Widening `layout` to a `u32` for tidiness would take [`FRAME_LEN`] to
//! sixteen and close that second line while looking like housekeeping, so the
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

use std::cmp::Ordering;
use std::marker::PhantomData;

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
/// let mut state = Versioned::new(claim);
/// let mut counter = state.load::<Counter>().await?.unwrap_or(Counter(0));
///
/// counter.0 += 1;
/// state.store(&Snapshot::of(&counter)).await?;
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
    /// **`D` is inferred from `snapshot` and cannot be told.** That is the whole
    /// reason [`Snapshot`] exists rather than a `&[u8]`: the version stamped
    /// here is the version of the type that produced these bytes, proved by the
    /// argument rather than asserted by a turbofish beside it. [`Snapshot`]
    /// documents what the `&[u8]` version of this signature allowed.
    ///
    /// Atomic, because [`Claim::store`] is. A crash during this leaves either
    /// the previous framed snapshot or the new one.
    ///
    /// # Errors
    ///
    /// - [`Error::TooLarge`](crate::Error::TooLarge) when the frame **plus**
    ///   the payload is over the adapter's limit. The number it reports is what
    ///   reached the adapter, which is [`FRAME_LEN`] more than the payload.
    /// - [`Error::Io`](crate::Error::Io) when the write fails. The previous
    ///   snapshot is intact.
    pub async fn store<D: Durable>(&mut self, snapshot: &Snapshot<D>) -> Result<()> {
        self.claim.store(&frame(D::VERSION, &snapshot.bytes)).await
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

    /// The other side of that comparison, and the one a `!=` got wrong. Layout
    /// 0 is also what an unframed file reads, so this is the check that still
    /// refuses one if the magic ever stops being the first thing tested.
    #[test]
    fn a_header_layout_older_than_this_build_is_refused_as_old() {
        let mut framed = frame(1, b"12345678");
        framed[6] = 0;
        assert!(matches!(
            unframe(&framed),
            Err(RestoreError::NotAFrame {
                fault: FrameFault::LayoutTooOld { stored: 0, ours: 1 }
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
