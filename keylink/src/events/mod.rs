//! A retained event log, followed through an ordinary long-poll `rpc`.
//!
//! Declare `rpc Events(from: keylink::events::Cursor) -> keylink::events::Pull<E>;`
//! in your own `service!`. A concurrent handler awaits [`EventLog::pull`]; an actor
//! declares it `#[defer]` and returns [`EventLog::answer`] so waiting never holds a turn.
//! The event type enters the service's schema hash like any other return type.
//!
//! # Resume and resync
//!
//! [`follow()`] yields each event with a cursor. Save that cursor **after** processing
//! the event. A restart can deliver an event again: delivery across consumer restarts
//! is at-least-once, not exactly-once. A single [`Following`] never repeats a sequence.
//!
//! A refusal is not a skipped page. Take [`PullRefused::head`] **first**, read
//! authoritative state, then follow from that head. Events between the head and
//! the state read can appear twice; your state application must tolerate that.
//! The state read must include everything through that head. A brand-new follower
//! uses [`Cursor::ORIGIN`], which also requires resync if retention removed event 1.
//!
//! # Commit and backpressure
//!
//! [`Stage`] and [`Publish`] publish completed turns before the reply. A panicked
//! turn publishes nothing, as does a turn refused by an earlier observer. Put
//! `Publish` before `Persist` when missing an event is worse than an event for
//! state that was not stored. **Two stores are not atomic:** Publish first can
//! leave a phantom after a crash or a later observer refusal; Persist first can
//! silently lose an event between the two writes. A transactional outbox is not built.
//!
//! A durable `Indeterminate` append may already be stored. It poisons the live
//! log: future appends and pulls are refused as `Poisoned`, and parked pulls wake.
//! Drop every log handle, reopen through recovery, and inspect what survived
//! before deciding whether to submit another event. Never automatically retry
//! the uncertain batch. `head()` remains only the last confirmed cursor.
//!
//! Appends never wait for followers. A slow follower falls behind into retention
//! and is explicitly refused when it asks for removed history. Each caught-up pull
//! holds a QUIC stream for at most [`PULL_WAIT`], capped by [`MAX_PULLERS`]. Many
//! logs on one connection compete with ordinary calls for its stream budget;
//! multiplexing keys in one log is an application decision.
//!
//! # Limits not hidden by this interface
//!
//! The retained window lives in memory; durable storage is for recovery, not
//! reads. The ring holds only checked encoded bytes, not mutable `E` values.
//! Pulls decode independent deliveries after releasing the ring lock. The byte
//! bound excludes metadata and decoded pages; `E` needs neither `Clone` nor `Sync`.
//! A serde decoder that rejects its own encoding is refused as `Unreadable`.
//! Age retention uses wall time, lazily at append and pull. Durable event
//! schema migrations, server-held subscriber cursors and subscriber groups are
//! not built. Undecodable stored events refuse opening rather than being skipped.
//! Reopen with the same `E`: its type is not stored, so overlapping encodings
//! (such as signed and unsigned integers) can silently change meaning.
//! Admission refusals are indistinguishable from outages and retry forever with
//! surfaced backoff. Latency and throughput are unmeasured. The HTTP gateway's
//! JSON long-poll and proxy timeouts have not been verified. Memory logs and
//! followers compile for the browser; no network connection has run in a tab.

use std::fmt;
use std::time::Duration;

use postcard_schema::Schema;
use serde::{Deserialize, Serialize};

mod follow;
mod log;
#[cfg(feature = "persist")]
mod store;

pub use follow::{follow, Delivery, Following, Lapse, Retry};
#[cfg(feature = "persist")]
pub use log::OpenError;
pub use log::{AppendError, EventLog, Publish, Retention, Stage};

/// An event's position. Events start at 1; zero means before the first event.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Schema,
)]
pub struct Seq(u64);

impl Seq {
    /// The sequence number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The last event processed, together with the identity of its log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
pub struct Cursor(Position);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
enum Position {
    Origin,
    After { log: LogId, seq: Seq },
}

impl Cursor {
    /// From the first event. Refused with `Expired` if event 1 was removed.
    pub const ORIGIN: Self = Self(Position::Origin);

    /// The last event processed, or `None` for [`Self::ORIGIN`].
    #[must_use]
    pub const fn seq(&self) -> Option<Seq> {
        match self.0 {
            Position::Origin => None,
            Position::After { seq, .. } => Some(seq),
        }
    }

    const fn after(log: LogId, seq: Seq) -> Self {
        Self(Position::After { log, seq })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Schema)]
struct LogId([u8; 16]);

impl LogId {
    fn mint() -> Self {
        let mut id = [0; 16];
        getrandom::fill(&mut id).unwrap_or_else(|error| {
            panic!("event-log identity randomness is unavailable: {error}")
        });
        Self(id)
    }
}

/// Contiguous events after a cursor, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema)]
#[non_exhaustive]
pub struct Page<E> {
    /// Empty only when a caught-up pull's wait expired.
    pub events: Vec<(Seq, E)>,
    /// The cursor to use for the next pull.
    pub next: Cursor,
    /// The newest event when this page was built.
    pub head: Seq,
}

/// The answer to an application's long-poll `rpc`.
pub type Pull<E> = Result<Page<E>, PullRefused>;

/// An explicit refusal, rather than a silently skipped part of history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Schema, thiserror::Error)]
#[non_exhaustive]
pub enum PullRefused {
    /// Retention removed events this cursor still needs. Resync, then use `head`.
    #[error("events after {asked} are no longer held; the oldest held is {oldest}")]
    Expired {
        /// The last sequence processed by the caller.
        asked: Seq,
        /// The first event still held.
        oldest: Seq,
        /// Where to follow after resyncing.
        head: Cursor,
    },
    /// The cursor names another log, possibly an earlier memory incarnation.
    #[error("the cursor belongs to a different log")]
    Replaced {
        /// Where to follow after resyncing.
        head: Cursor,
    },
    /// The cursor is beyond the current head, possibly after losing a durable tail.
    #[error("cursor {asked} is past this log's head")]
    Ahead {
        /// The sequence asked for.
        asked: Seq,
        /// Where to follow after resyncing.
        head: Cursor,
    },
    /// Retained bytes cannot be decoded as the event type. Fix its serde
    /// round-trip before following again; retrying or skipping is not a repair.
    #[error("the retained event at {seq} cannot be decoded")]
    Unreadable {
        /// The first event that could not be materialized.
        seq: Seq,
    },
    /// An indeterminate durable append made the live log unavailable. The server
    /// must drop its log handles and reopen through recovery; no head is claimed.
    #[error("the event log requires recovery after an indeterminate append")]
    Poisoned,
    /// No log is served under the requested name. Produced by keyed handlers only.
    #[error("no log is served under that name")]
    Unknown,
    /// Too many pulls already wait on this log. Retryable.
    #[error("{parked} pulls are already waiting on this log")]
    Crowded {
        /// How many pulls were parked.
        parked: u64,
    },
}

impl PullRefused {
    /// A resync point for `Expired`, `Replaced` and `Ahead`.
    #[must_use]
    pub const fn head(&self) -> Option<Cursor> {
        match self {
            Self::Expired { head, .. } | Self::Replaced { head } | Self::Ahead { head, .. } => {
                Some(*head)
            }
            Self::Unknown | Self::Crowded { .. } | Self::Unreadable { .. } | Self::Poisoned => None,
        }
    }
}

/// How long a caught-up pull waits before returning an empty page.
pub const PULL_WAIT: Duration = Duration::from_secs(30);
/// Maximum pulls parked on one log at once.
pub const MAX_PULLERS: usize = 1024;
/// Maximum encoded bytes of one event.
pub const MAX_EVENT_BYTES: usize = 256 * 1024;
/// Page budget, counting each encoded event and ten bytes for its sequence.
pub const PAGE_BYTES: usize = 512 * 1024;
const _: () =
    assert!(MAX_EVENT_BYTES + 10 <= PAGE_BYTES && PAGE_BYTES + 4096 <= crate::MAX_HEADER as usize);
