//! Retry safety: a caller-supplied key so a repeated call runs at most once.
//!
//! A call that fails mid-flight leaves the caller unable to say whether it ran.
//! For `Ping` that costs nothing. For `Reserve(sku, qty)` the caller must choose
//! between a double booking and no booking at all, with no way to tell which
//! one it is choosing.
//!
//! An [`IdempotencyKey`] on the call removes the choice. The serving side
//! records the reply under `(peer, key)`, and a later call carrying the same key
//! is answered from that record instead of being run again.
//!
//! # What is guaranteed
//!
//! **The handler is entered at most once per key, on this peer, on this
//! service, for as long as the record survives.** A repeat is answered with the
//! recorded reply, byte for byte.
//!
//! # What is NOT guaranteed
//!
//! - **Durability.** The record is memory. It dies with the process, and a
//!   restarted service will run a retried call again. keylink has no journal and
//!   this is not one. Durable execution is what [Restate] is for.
//! - **Survival.** The ledger is bounded ([`MAX_RECORDS`] entries,
//!   [`MAX_RECORDED_BYTES`] of reply). A record evicted under pressure takes its
//!   guarantee with it.
//! - **A payload.** Bodies are streamed and never held, so a reply that carries
//!   one cannot be recorded. The repeat is refused rather than answered with a
//!   header whose body never arrives.
//! - **Certainty after an interruption.** A call whose handler was entered and
//!   then dropped — a dead connection, a shutdown — leaves
//!   [`IdempotencyRefusal::Interrupted`]. Whether it took effect is genuinely
//!   unknown, and saying so is better than guessing either way.
//!
//! # What this takes from Restate, and what it rejects
//!
//! Taken: the reply is **replayed**, not refused — a caller that lost its answer
//! wants the answer, and refusing hands it nothing it did not already have. The
//! key is scoped to the service and the handler rather than to the whole node.
//! Retention is bounded rather than forever.
//!
//! Rejected: **durability**, and everything downstream of it — journaling,
//! replay of the execution itself, restart from a known point. Also rejected is
//! Restate's *attach*, where a second caller waits on the running invocation.
//! Waiting needs a shared wakeup, and RFC-04 §3.3 already ruled on that class of
//! argument for this codebase: a second call under a live key is refused instead
//! (see [`IdempotencyRefusal::InFlight`]).
//!
//! [Restate]: https://restate.dev

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use crate::{IdempotencyKey, RemoteError};

/// Settled records the ledger holds before it starts evicting.
///
/// Judgement, not measurement. Large enough that an ordinary retry lands inside
/// it, small enough that the ledger is a rounding error beside one connection.
pub const MAX_RECORDS: usize = 1024;

/// Recorded reply bytes the ledger holds before it starts evicting.
///
/// A record is a reply frame, which is capped at [`MAX_HEADER`](crate::MAX_HEADER)
/// — a megabyte each. Counting entries alone would therefore bound the ledger at
/// a gigabyte, so the bytes are counted too, and a reply that cannot fit here on
/// its own is never recorded.
pub const MAX_RECORDED_BYTES: usize = 1 << 20;

/// Why a call carrying an idempotency key was refused without being run.
///
/// Grouped under one [`RemoteError`] variant rather than spread across four,
/// because [`RemoteError`] belongs to every service keylink will ever carry and
/// this vocabulary belongs to one feature.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema, thiserror::Error,
)]
#[non_exhaustive]
pub enum IdempotencyRefusal {
    /// The key was empty, or longer than [`MAX_KEY_LEN`](crate::MAX_KEY_LEN).
    ///
    /// Raised on the receive path, because `derive(Deserialize)` never calls a
    /// constructor and a peer's key has therefore never been checked.
    #[error("the key is malformed: {0}")]
    Malformed(String),

    /// Another call with this key is being handled right now.
    ///
    /// Not an error about the key. It is the answer to "did my call run?" —
    /// yes, it is running, so do not send the work a second time.
    #[error("another call with this key is in flight - ask again when it finishes")]
    InFlight,

    /// A call with this key entered the handler and never produced a reply.
    ///
    /// The connection died, or the service shut down, part way through. Whether
    /// the work took effect is unknown, and no record can make it known. Decide
    /// out of band, then use a different key.
    #[error("a call with this key was interrupted - whether it took effect is unknown")]
    Interrupted,

    /// The recorded call answered with a payload beside its reply.
    ///
    /// A body is streamed and never held, so recording one would contradict the
    /// property that makes a 4 GiB transfer cost a bounded amount of memory. The
    /// header alone would decode into a reply whose body never arrives.
    #[error("the reply to this key carried a payload, which is not recorded")]
    Unrecordable,
}

impl From<IdempotencyRefusal> for RemoteError {
    fn from(refusal: IdempotencyRefusal) -> Self {
        Self::Idempotency(refusal)
    }
}

/// What one key is worth, at the moment it is asked about.
#[derive(Debug)]
enum Slot {
    /// A call is in dispatch under this key now. Never evicted: dropping it
    /// would let a concurrent duplicate into the handler.
    Running,
    /// The reply frame, ready to be written again byte for byte.
    Done(Bytes),
    /// The handler was entered and no reply came out.
    Interrupted,
    /// The handler ran, and its reply is not something this ledger can hold.
    Unrecordable,
}

impl Slot {
    /// What a repeat of this key gets. `None` while the call is still running is
    /// not a case: [`Slot::Running`] answers with a refusal.
    fn replay(&self) -> Claim {
        match self {
            Self::Running => Claim::Refused(IdempotencyRefusal::InFlight.into()),
            Self::Done(frame) => Claim::Replay(frame.clone()),
            Self::Interrupted => Claim::Refused(IdempotencyRefusal::Interrupted.into()),
            Self::Unrecordable => Claim::Refused(IdempotencyRefusal::Unrecordable.into()),
        }
    }
}

/// One peer's records. Peers never share a bucket, and that is the whole
/// security property — see [`Ledger`].
#[derive(Debug, Default)]
struct Bucket {
    slots: HashMap<IdempotencyKey, Slot>,
    /// Settled keys, oldest first, and the eviction order for this peer.
    /// [`Slot::Running`] keys are deliberately absent.
    settled: VecDeque<IdempotencyKey>,
}

#[derive(Debug, Default)]
struct Inner {
    peers: HashMap<EndpointId, Bucket>,
    settled: usize,
    bytes: usize,
}

/// The answer to "has this key been used?".
#[derive(Debug)]
pub(crate) enum Claim {
    /// Nobody has used this key. Run the call, then settle the guard.
    Fresh(Guard),
    /// Write these bytes. The handler is not entered.
    Replay(Bytes),
    /// Refuse. The handler is not entered.
    Refused(RemoteError),
}

/// Every recorded reply for one service.
///
/// # Scope
///
/// One ledger per **service**, and every record inside it is keyed by the
/// **peer** iroh proved at the handshake. Both halves are load bearing.
///
/// Per service, because a record is an encoded reply of *that service's*
/// response type. Sharing one ledger across a node would let a key recorded
/// against one schema be replayed into another, which is the postcard
/// variant-confusion the derived ALPN exists to refuse — reintroduced one layer
/// below the gate that refuses it.
///
/// Per peer, because a key is a string a peer chose. A shared namespace would
/// let one peer read another peer's reply by guessing `order-1`, which is an
/// information leak before it is a collision.
#[derive(Debug)]
pub(crate) struct Ledger {
    inner: Mutex<Inner>,
    max_records: usize,
    max_bytes: usize,
}

impl Ledger {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_limits(MAX_RECORDS, MAX_RECORDED_BYTES)
    }

    fn with_limits(max_records: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            max_records,
            max_bytes,
        })
    }

    /// Take the key, or say what already holds it.
    ///
    /// This is the whole at-most-once property: the slot is claimed *before* the
    /// handler is entered, and the only way back to vacant is eviction.
    pub(crate) fn claim(self: &Arc<Self>, peer: EndpointId, key: IdempotencyKey) -> Claim {
        let mut inner = self.lock();
        let bucket = inner.peers.entry(peer).or_default();
        if let Some(slot) = bucket.slots.get(&key) {
            return slot.replay();
        }
        bucket.slots.insert(key.clone(), Slot::Running);
        Claim::Fresh(Guard {
            ledger: Arc::clone(self),
            peer,
            key,
            settled: false,
        })
    }

    /// Poison recovery rather than propagation, the same call
    /// `demo/src/main.rs` makes about its own state: one bad request should cost
    /// one request. Nothing here can panic while the lock is held, so this arm
    /// is unreachable in practice and must still be typed.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Move a claimed slot to its settled state and make room for it.
    fn settle(&self, peer: EndpointId, key: &IdempotencyKey, slot: Slot) {
        let mut inner = self.lock();
        let Some(bucket) = inner.peers.get_mut(&peer) else {
            return;
        };
        let added = match &slot {
            Slot::Done(frame) => frame.len(),
            _ => 0,
        };
        bucket.slots.insert(key.clone(), slot);
        bucket.settled.push_back(key.clone());
        inner.settled += 1;
        inner.bytes += added;
        self.evict(&mut inner);
    }

    /// Drop settled records until both bounds hold.
    ///
    /// Always from the peer holding the most, so a peer that floods the ledger
    /// evicts its own records first and stops being the largest holder before it
    /// can reach anyone else's. A global oldest-first policy would let one peer
    /// flush every other peer's records, which is a denial of service dressed as
    /// a cache.
    fn evict(&self, inner: &mut Inner) {
        while inner.settled > self.max_records || inner.bytes > self.max_bytes {
            // Ties broken by id so the order is deterministic rather than
            // whatever the hasher happened to seed.
            let Some(peer) = inner
                .peers
                .iter()
                .filter(|(_, bucket)| !bucket.settled.is_empty())
                .max_by_key(|(id, bucket)| (bucket.settled.len(), **id))
                .map(|(id, _)| *id)
            else {
                return;
            };

            let Some(bucket) = inner.peers.get_mut(&peer) else {
                return;
            };
            let Some(key) = bucket.settled.pop_front() else {
                return;
            };
            if let Some(Slot::Done(frame)) = bucket.slots.remove(&key) {
                inner.bytes -= frame.len();
            }
            inner.settled -= 1;
            if bucket.slots.is_empty() {
                inner.peers.remove(&peer);
            }
        }
    }
}

/// A claimed key, held for as long as the call is being handled.
///
/// Settle it with [`Guard::record`]. Dropping it unsettled means the handler was
/// entered and produced nothing, which is [`IdempotencyRefusal::Interrupted`]
/// rather than a vacant key: re-running work whose effect is unknown is the one
/// thing this module exists to prevent.
#[derive(Debug)]
pub(crate) struct Guard {
    ledger: Arc<Ledger>,
    peer: EndpointId,
    key: IdempotencyKey,
    settled: bool,
}

impl Guard {
    /// Record the reply frame this call produced.
    ///
    /// `carries_body` is what makes a reply unrecordable: the frame is the
    /// header, and a header replayed without the payload behind it decodes into
    /// a body that never arrives.
    pub(crate) fn record(mut self, frame: &Bytes, carries_body: bool) {
        self.settled = true;
        let slot = if carries_body || frame.len() > self.ledger.max_bytes {
            Slot::Unrecordable
        } else {
            Slot::Done(frame.clone())
        };
        self.ledger.settle(self.peer, &self.key, slot);
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.settled {
            self.ledger.settle(self.peer, &self.key, Slot::Interrupted);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    /// Two distinct, valid endpoint ids. Reused from `authz.rs`, which needs the
    /// same thing for the same reason.
    const A: &str = "a09aa5f47a6759802ff955f8dc2d2a14a5c99d23be97f864127ff9383455a4f0";
    const B: &str = "d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737";

    fn peer(hex: &str) -> EndpointId {
        use std::str::FromStr;
        EndpointId::from_str(hex).expect("a valid endpoint id")
    }

    fn key(text: &str) -> IdempotencyKey {
        IdempotencyKey::new(text).expect("a valid key")
    }

    fn frame(len: usize) -> Bytes {
        Bytes::from(vec![7u8; len])
    }

    fn refusal(claim: Claim) -> IdempotencyRefusal {
        match claim {
            Claim::Refused(RemoteError::Idempotency(refusal)) => refusal,
            other => panic!("expected an idempotency refusal, got {other:?}"),
        }
    }

    fn recorded(claim: Claim) -> Bytes {
        match claim {
            Claim::Replay(bytes) => bytes,
            other => panic!("expected a replay, got {other:?}"),
        }
    }

    fn guard(claim: Claim) -> Guard {
        match claim {
            Claim::Fresh(guard) => guard,
            other => panic!("expected a fresh claim, got {other:?}"),
        }
    }

    mod claim {
        use super::*;

        #[test]
        fn an_unused_key_is_fresh() {
            let ledger = Ledger::new();
            assert!(matches!(
                ledger.claim(peer(A), key("first")),
                Claim::Fresh(_)
            ));
        }

        #[test]
        fn a_recorded_key_replays_the_same_bytes() {
            let ledger = Ledger::new();
            guard(ledger.claim(peer(A), key("k"))).record(&frame(16), false);

            assert_eq!(recorded(ledger.claim(peer(A), key("k"))), frame(16));
        }

        #[test]
        fn a_different_key_is_fresh_even_after_one_is_recorded() {
            let ledger = Ledger::new();
            guard(ledger.claim(peer(A), key("one"))).record(&frame(16), false);

            assert!(matches!(ledger.claim(peer(A), key("two")), Claim::Fresh(_)));
        }

        /// The property that keeps one peer from reading another's reply by
        /// guessing a key. If this ever goes green with a shared namespace, the
        /// ledger has become an information leak.
        #[test]
        fn two_peers_using_the_same_key_do_not_collide() {
            let ledger = Ledger::new();
            guard(ledger.claim(peer(A), key("shared"))).record(&frame(16), false);

            assert!(
                matches!(ledger.claim(peer(B), key("shared")), Claim::Fresh(_)),
                "peer B must not be answered from peer A's record"
            );
        }

        #[test]
        fn a_second_claim_while_the_first_runs_is_refused_as_in_flight() {
            let ledger = Ledger::new();
            let _running = guard(ledger.claim(peer(A), key("k")));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::InFlight
            );
        }

        /// A handler that was entered and produced nothing leaves uncertainty,
        /// not a vacant key. Re-running it is the double booking this module
        /// exists to prevent.
        #[test]
        fn a_guard_dropped_without_a_reply_leaves_the_key_interrupted() {
            let ledger = Ledger::new();
            drop(guard(ledger.claim(peer(A), key("k"))));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Interrupted
            );
        }

        #[test]
        fn a_reply_that_carries_a_body_is_not_replayed() {
            let ledger = Ledger::new();
            guard(ledger.claim(peer(A), key("k"))).record(&frame(16), true);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Unrecordable
            );
        }

        /// One reply larger than the whole byte budget cannot be evicted into
        /// fitting, so it must never be stored in the first place.
        #[test]
        fn a_reply_larger_than_the_byte_budget_is_not_replayed() {
            let ledger = Ledger::with_limits(MAX_RECORDS, 64);
            guard(ledger.claim(peer(A), key("k"))).record(&frame(65), false);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Unrecordable
            );
        }
    }

    mod eviction {
        use super::*;

        #[test]
        fn the_record_count_stays_inside_its_bound() {
            let ledger = Ledger::with_limits(4, MAX_RECORDED_BYTES);
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert_eq!(ledger.lock().settled, 4);
        }

        #[test]
        fn the_recorded_bytes_stay_inside_their_bound() {
            let ledger = Ledger::with_limits(MAX_RECORDS, 64);
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(16), false);
            }

            assert!(
                ledger.lock().bytes <= 64,
                "held {} bytes over a 64 byte budget",
                ledger.lock().bytes
            );
        }

        #[test]
        fn the_oldest_record_of_the_largest_holder_goes_first() {
            let ledger = Ledger::with_limits(2, MAX_RECORDED_BYTES);
            guard(ledger.claim(peer(A), key("old"))).record(&frame(8), false);
            guard(ledger.claim(peer(A), key("new"))).record(&frame(8), false);
            guard(ledger.claim(peer(A), key("newest"))).record(&frame(8), false);

            assert!(
                matches!(ledger.claim(peer(A), key("old")), Claim::Fresh(_)),
                "the oldest record must be the one that left"
            );
        }

        /// A peer that floods the ledger must evict itself, never a quiet peer.
        /// A global oldest-first policy passes every other test in this module
        /// and fails this one.
        #[test]
        fn a_flooding_peer_cannot_evict_a_quiet_peers_record() {
            let ledger = Ledger::with_limits(4, MAX_RECORDED_BYTES);
            guard(ledger.claim(peer(B), key("quiet"))).record(&frame(8), false);
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert_eq!(
                recorded(ledger.claim(peer(B), key("quiet"))),
                frame(8),
                "the quiet peer's record must survive the flood"
            );
        }

        /// Evicting a running slot would let a concurrent duplicate into the
        /// handler, which is the one thing the ledger must never do. Running
        /// slots are bounded by the transport instead: a call holds its QUIC
        /// stream for as long as it is in the ledger, and
        /// `max_concurrent_bidi_streams` is 100.
        #[test]
        fn a_running_claim_is_never_evicted() {
            let ledger = Ledger::with_limits(1, MAX_RECORDED_BYTES);
            let _running = guard(ledger.claim(peer(A), key("running")));
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert_eq!(
                refusal(ledger.claim(peer(A), key("running"))),
                IdempotencyRefusal::InFlight,
                "the in-flight claim must outlive every settled record"
            );
        }
    }
}
