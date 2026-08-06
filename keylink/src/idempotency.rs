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
//! service, for as long as the record or its tombstone survives.** A repeat is
//! answered with the recorded reply, byte for byte; once the reply has expired
//! or been evicted the repeat is *refused*, never re-run.
//!
//! # Who settles a key, and who does not
//!
//! Only work that was actually entered settles a key. A layer that refuses a
//! call **before** dispatch — an authorization rule, a [`QueueTimeout`] whose
//! budget expired while the message was still queued — leaves the key
//! claimable, because nothing ran and nothing is owed. A layer that refuses
//! **after** dispatch, such as a [`Timeout`](crate::Timeout) that drops the wait
//! for a running actor turn, settles the key as
//! [`Interrupted`](IdempotencyRefusal::Interrupted): whether the work landed is
//! genuinely not known from there.
//!
//! That distinction is not visible in the [`Outcome`](crate::Outcome) a layer
//! returns — `Outcome::Err` is what a panicking actor turn produces too — so it
//! is carried out of band, by the progress marker on
//! [`Context`](crate::Context) that generated dispatch sets. See RFC-05.
//!
//! [`QueueTimeout`]: crate::QueueTimeout
//!
//! # What is NOT guaranteed
//!
//! - **Durability.** The record is memory. It dies with the process, and a
//!   restarted service will run a retried call again. keylink has no journal and
//!   this is not one. Durable execution is what [Restate] is for.
//! - **A reply forever.** Records are bounded three ways — [`MAX_RECORDS`]
//!   entries, [`MAX_RECORDED_BYTES`] of reply, and [`RECORD_TTL`] of age. Past
//!   any of them the reply is dropped and the key becomes
//!   [`IdempotencyRefusal::Forgotten`]: at-most-once still holds, and the answer
//!   does not.
//! - **A tombstone forever.** Tombstones are bounded too, by
//!   [`MAX_TOMBSTONES`]. Past that a key is genuinely vacant again, which
//!   [`KeyState::Fresh`] reports and which is the one state a caller cannot tell
//!   from "never used".
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
//! Retention is bounded rather than forever. And **attach**, where a second
//! caller waits for the running invocation instead of being turned away.
//!
//! Rejected: **durability**, and everything downstream of it — journaling,
//! replay of the execution itself, restart from a known point.
//!
//! ## Attach was rejected once, and the objection was right
//!
//! An earlier version of this module refused attach, citing RFC-04 §3.3's worry
//! about *"whether a dropped `Notified` can swallow a wakeup"*. That worry is
//! real and specific: [`Notify::notify_waiters`] wakes only the waiters
//! registered at the moment it is called, so a waiter that registers after the
//! notification misses it forever — and the failure is load-dependent, which is
//! to say it passes every test.
//!
//! It has a known answer, and this module uses it: [`Notified::enable`]
//! registers the waiter *before* the slot is re-read, so a settle that lands in
//! between is not lost. See RFC-05 §4.
//!
//! [Restate]: https://restate.dev
//! [`Notify::notify_waiters`]: tokio::sync::Notify::notify_waiters
//! [`Notified::enable`]: tokio::sync::futures::Notified::enable

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use iroh::EndpointId;
use n0_future::time::Instant;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::middleware::{Progress, Reached};
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

/// Tombstones the ledger holds before it starts dropping them.
///
/// A tombstone is a key and nothing else, and a key is bounded by
/// [`MAX_KEY_LEN`](crate::MAX_KEY_LEN), so this pool is cheap by construction —
/// eight times [`MAX_RECORDS`] costs on the order of a megabyte. It is separate
/// from the record bound on purpose: a tombstone is what keeps an evicted key
/// from being silently re-executed, so it must outlive the reply it replaced by
/// a wide margin.
pub const MAX_TOMBSTONES: usize = 8192;

/// How long a settled record answers with its reply before it becomes a
/// tombstone.
///
/// The reason a volume bound is not enough on its own: with only a volume bound
/// the at-most-once *window* is a function of unrelated traffic, so an operator
/// cannot say how long a retry stays safe without knowing what every other
/// caller is doing. A duration is answerable.
///
/// On [`n0_future::time::Instant`], not [`std::time::Instant`], which panics on
/// `wasm32-unknown-unknown`.
pub const RECORD_TTL: Duration = Duration::from_secs(600);

/// The longest an attaching call is held waiting for one already in flight.
///
/// A caller names its own budget in [`Idempotency::attaching_for`]; this caps
/// it, because the budget arrives from the network and a peer that asks to be
/// held for a year is asking us to hold a QUIC stream for a year.
pub const MAX_ATTACH: Duration = Duration::from_secs(30);

/// Calls with keys one peer may have in dispatch at once.
///
/// Above QUIC's 100 concurrent bidirectional streams, so a peer using one
/// connection never meets it, and a peer opening connection after connection
/// does. Without it a claimed-but-unsettled slot is invisible to both volume
/// bounds — a running claim is deliberately never evicted — so N connections
/// would admit 100×N unaccounted claims.
pub const MAX_RUNNING_PER_PEER: usize = 128;

/// Calls with keys the whole ledger may have in dispatch at once.
///
/// The per-peer cap alone is not a bound: [`Allowlist::Anyone`](crate::Allowlist)
/// makes the number of peers a property of the network rather than of the
/// configuration.
pub const MAX_RUNNING: usize = 4096;

/// Attaching calls one peer may have parked at once, waiting on a call already
/// in flight.
///
/// A parked attacher holds no claim, so neither running cap can see it or the
/// QUIC stream pair it is holding open: one claimed key plus 40 connections of
/// 100 streams each parks 4000 tasks for [`MAX_ATTACH`] and meets no bound.
/// Same number as [`MAX_RUNNING_PER_PEER`], because that is also the legitimate
/// ceiling — a [`Peer`](crate::Peer) re-issue produces at most one attacher per
/// running claim, and running claims are capped there.
pub const MAX_WAITING_PER_PEER: usize = 128;

/// Attaching calls the whole ledger may have parked at once.
///
/// Two levels for the reason the running pair has two:
/// [`Allowlist::Anyone`](crate::Allowlist) makes the peer count a property of
/// the network. A quarter of [`MAX_RUNNING`] rather than the same number,
/// because a waiter produces nothing — it costs a task and a stream pair, and
/// it may still be refused when its budget runs out — so the node spends the
/// smaller share of its budget on calls it has already decided not to run.
pub const MAX_WAITING: usize = 1024;

/// Why a call carrying an idempotency key was refused without being run.
///
/// Grouped under one [`RemoteError`] variant rather than spread across six,
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
    /// yes, it is running, so do not send the work a second time. To wait for it
    /// instead, ask with [`Idempotency::attaching_for`]; this is also what an
    /// attaching call is told when its budget runs out, because it is still the
    /// truth.
    #[error("another call with this key is in flight - ask again when it finishes")]
    InFlight,

    /// A call with this key entered the handler and never produced a reply.
    ///
    /// The connection died, the service shut down, or a layer above the handler
    /// stopped waiting part way through. Whether the work took effect is
    /// unknown, and no record can make it known. Decide out of band, then use a
    /// different key.
    #[error("a call with this key was interrupted - whether it took effect is unknown")]
    Interrupted,

    /// The recorded call answered with a payload beside its reply.
    ///
    /// A body is streamed and never held, so recording one would contradict the
    /// property that makes a 4 GiB transfer cost a bounded amount of memory. The
    /// header alone would decode into a reply whose body never arrives.
    #[error("the reply to this key carried a payload, which is not recorded")]
    Unrecordable,

    /// This key ran, and its reply is no longer held.
    ///
    /// The record aged past [`RECORD_TTL`] or was evicted under pressure. The
    /// at-most-once promise still holds — that is what the tombstone is for —
    /// and the answer is gone. Distinct from a vacant key on purpose: silently
    /// running the work again is the one thing this module exists to prevent.
    #[error("a call with this key already ran, and its reply is no longer held")]
    Forgotten,

    /// This peer has too many calls with keys in dispatch at once.
    ///
    /// A claimed slot is never evicted, so unbounded claims are unbounded
    /// memory. Bounded by [`MAX_RUNNING_PER_PEER`] and [`MAX_RUNNING`].
    ///
    /// Also what an attaching call is told when [`MAX_WAITING_PER_PEER`] or
    /// [`MAX_WAITING`] is reached. One variant rather than two, because
    /// `postcard_schema` hashes the variant list into the ALPN: a second
    /// spelling of "too many at once" would move the protocol identity and
    /// break every handshake, to say something the caller acts on identically.
    #[error("too many calls with idempotency keys are in flight - the limit is {limit}")]
    TooManyRunning {
        /// The cap that was reached.
        limit: u32,
    },
}

impl From<IdempotencyRefusal> for RemoteError {
    fn from(refusal: IdempotencyRefusal) -> Self {
        Self::Idempotency(refusal)
    }
}

/// What the ledger knows about one key, for one peer.
///
/// Answered by the generated `key_state` query, which is the only way to learn
/// [`Fresh`](Self::Fresh) — every other state can be discovered by retrying, and
/// `Fresh` cannot, because discovering it by retrying means executing.
///
/// Exhaustive on purpose. `#[non_exhaustive]` would be contradictory three ways:
/// this crate reserves that attribute for error enums rather than for ones a
/// caller matches; `postcard_schema` derives from the variant list and ignores
/// the attribute, so a seventh variant moves the ALPN and breaks every handshake
/// anyway — the attribute would advertise source compatibility on a hard wire
/// break; and it forces the `_ =>` arm this crate spent eleven catch-all-free
/// macros deleting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub enum KeyState {
    /// No record. Either never used, or its tombstone was dropped past
    /// [`MAX_TOMBSTONES`]. A call with this key would run.
    Fresh,
    /// A call is in dispatch under this key now.
    Running,
    /// Settled. The reply is held and would be replayed byte for byte.
    Done,
    /// Entered and no reply came out. Whether it took effect is unknown.
    Interrupted,
    /// It ran, and its reply carried a payload, which is never recorded. A
    /// permanent property of the method rather than an eviction.
    Unrecordable,
    /// It ran; the reply is no longer held. At-most-once still holds.
    Forgotten,
}

/// A key, and what to do when it collides with a call already in flight.
///
/// `attach` without a key is meaningless, so it is not representable: the budget
/// lives inside the value that carries the key. This is also the wire type —
/// one type, client side and envelope side, so the two cannot drift.
///
/// ```ignore
/// // refuse on collision, which is what a bare key has always done
/// client.with_idempotency(key.clone()).submit(order).await?;
///
/// // or wait for the call already running under it, and take its answer
/// client
///     .with_idempotency(Idempotency::attaching_for(key, Duration::from_secs(5)))
///     .submit(order)
///     .await?;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, postcard_schema::Schema)]
pub struct Idempotency {
    key: IdempotencyKey,
    /// How long the server may hold this call waiting for one already in flight
    /// under the same key. `None` refuses immediately.
    ///
    /// Millis rather than a `Duration`, matching
    /// [`RemoteError::TimedOut`](crate::RemoteError::TimedOut), so `Duration`'s
    /// `Schema` impl never enters the hashed contract.
    attach_for_millis: Option<u64>,
}

impl Idempotency {
    /// Refuse on collision with a call already in flight.
    #[must_use]
    pub fn new(key: IdempotencyKey) -> Self {
        Self {
            key,
            attach_for_millis: None,
        }
    }

    /// Wait up to `budget` for a call already in flight under this key, then
    /// answer with whatever it settled as.
    ///
    /// Capped server-side by [`MAX_ATTACH`]. A budget that runs out is
    /// [`IdempotencyRefusal::InFlight`] — the same answer a non-attaching call
    /// gets, and still the truth.
    ///
    /// Only a *running* call is waited for. A key that already settled is
    /// answered at once, whether that answer is the reply, `Interrupted`,
    /// `Unrecordable` or `Forgotten`.
    #[must_use]
    pub fn attaching_for(key: IdempotencyKey, budget: Duration) -> Self {
        Self {
            key,
            attach_for_millis: Some(u64::try_from(budget.as_millis()).unwrap_or(u64::MAX)),
        }
    }

    /// The key itself.
    #[must_use]
    pub fn key(&self) -> &IdempotencyKey {
        &self.key
    }

    /// How long the caller asked to be held, already capped at [`MAX_ATTACH`].
    ///
    /// Capped here rather than at the constructor, because the value that
    /// matters is the one that arrived from the network and never met a
    /// constructor.
    #[must_use]
    pub fn attach_budget(&self) -> Option<Duration> {
        self.attach_for_millis
            .map(|millis| Duration::from_millis(millis).min(MAX_ATTACH))
    }
}

/// Keeps every `with_idempotency(key)` call site reading the way it always has.
impl From<IdempotencyKey> for Idempotency {
    fn from(key: IdempotencyKey) -> Self {
        Self::new(key)
    }
}

/// What one key is worth, at the moment it is asked about.
#[derive(Debug)]
enum Slot {
    /// A call is in dispatch under this key now. Never evicted: dropping it
    /// would let a concurrent duplicate into the handler. The handle is what an
    /// attaching call waits on, and it is fired when the slot is replaced.
    Running(Arc<Notify>),
    /// The reply frame, ready to be written again byte for byte.
    Done(Bytes),
    /// The handler was entered and no reply came out.
    Interrupted,
    /// The handler ran, and its reply is not something this ledger can hold.
    Unrecordable,
    /// The handler ran, and the reply has since expired or been evicted.
    Forgotten,
}

impl Slot {
    /// What a repeat of this key gets.
    fn replay(&self) -> Claim {
        match self {
            Self::Running(notify) => Claim::Running(Arc::clone(notify)),
            Self::Done(frame) => Claim::Replay(frame.clone()),
            Self::Interrupted => Claim::Refused(IdempotencyRefusal::Interrupted.into()),
            Self::Unrecordable => Claim::Refused(IdempotencyRefusal::Unrecordable.into()),
            Self::Forgotten => Claim::Refused(IdempotencyRefusal::Forgotten.into()),
        }
    }

    /// What a query about this key gets.
    fn state(&self) -> KeyState {
        match self {
            Self::Running(_) => KeyState::Running,
            Self::Done(_) => KeyState::Done,
            Self::Interrupted => KeyState::Interrupted,
            Self::Unrecordable => KeyState::Unrecordable,
            Self::Forgotten => KeyState::Forgotten,
        }
    }

    /// Recorded bytes this slot is holding, counted once so `Inner::bytes`
    /// cannot drift.
    ///
    /// Exhaustive, with no catch-all, so that a sixth variant is a compile error
    /// here rather than a silent zero. The failure a catch-all buys: a new
    /// variant that holds bytes counts as 0, `inner.bytes` drifts below the
    /// truth, and `evict` stops firing at the cap it exists to enforce. The
    /// ledger grows without bound and every test stays green, because no test
    /// asserts on a variant that does not exist yet.
    fn held_bytes(&self) -> usize {
        match self {
            Self::Done(frame) => frame.len(),
            Self::Running(_) | Self::Interrupted | Self::Unrecordable | Self::Forgotten => 0,
        }
    }
}

/// One slot and when it reached its current state.
#[derive(Debug)]
struct Entry {
    slot: Slot,
    /// When this entry settled — or, for [`Slot::Running`], when it was claimed.
    /// Only settled entries are aged out; a running claim has no deadline,
    /// because the transport is what bounds it.
    at: Instant,
}

/// One peer's records. Peers never share a bucket, and that is the whole
/// security property — see [`Ledger`].
#[derive(Debug, Default)]
struct Bucket {
    slots: HashMap<IdempotencyKey, Entry>,
    /// Keys holding a replayable reply, oldest first. The eviction order for
    /// this peer's records, and the expiry order too, since both are by age.
    held: VecDeque<IdempotencyKey>,
    /// Keys that ran and hold no reply: interrupted, unrecordable, forgotten.
    /// Oldest first, and bounded separately, because a tombstone is cheap and
    /// has to outlive the record it replaced.
    spent: VecDeque<IdempotencyKey>,
    /// [`Slot::Running`] keys, which appear in neither queue.
    running: usize,
}

#[derive(Debug, Default)]
struct Inner {
    peers: HashMap<EndpointId, Bucket>,
    /// Attaching calls parked right now, per peer. Beside `peers` rather than
    /// inside a [`Bucket`], because a waiter outlives the records it is waiting
    /// on: the settle it is woken by can leave the bucket empty, and an empty
    /// bucket is removed.
    waiters: HashMap<EndpointId, usize>,
    held: usize,
    bytes: usize,
    spent: usize,
    running: usize,
    waiting: usize,
}

/// Every bound the ledger enforces, in one value so a test can move them.
#[derive(Debug, Clone, Copy)]
struct Limits {
    records: usize,
    bytes: usize,
    tombstones: usize,
    running: usize,
    running_per_peer: usize,
    waiting: usize,
    waiting_per_peer: usize,
    ttl: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            records: MAX_RECORDS,
            bytes: MAX_RECORDED_BYTES,
            tombstones: MAX_TOMBSTONES,
            running: MAX_RUNNING,
            running_per_peer: MAX_RUNNING_PER_PEER,
            waiting: MAX_WAITING,
            waiting_per_peer: MAX_WAITING_PER_PEER,
            ttl: RECORD_TTL,
        }
    }
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
    /// A call is in dispatch under this key. A caller that asked to attach waits
    /// on this handle and claims again; one that did not is refused
    /// [`IdempotencyRefusal::InFlight`].
    Running(Arc<Notify>),
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
/// information leak before it is a collision. It is also what makes `attach`
/// safe for free: one peer can never wait on another peer's call.
#[derive(Debug)]
pub(crate) struct Ledger {
    inner: Mutex<Inner>,
    limits: Limits,
}

impl Ledger {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_limits(Limits::default())
    }

    fn with_limits(limits: Limits) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            limits,
        })
    }

    /// Take the key, or say what already holds it.
    ///
    /// This is the whole at-most-once property: the slot is claimed *before* the
    /// handler is entered, and the only way back to vacant is for the guard to
    /// be released — which happens exactly when nothing ran.
    pub(crate) fn claim(self: &Arc<Self>, peer: EndpointId, key: IdempotencyKey) -> Claim {
        let now = Instant::now();
        let mut guard = self.lock();
        // Dereferenced once, on purpose: field-splitting borrows work through a
        // `&mut Inner` and do NOT work through a `MutexGuard`, because deref
        // borrows the whole guard. Every function here that touches a bucket and
        // a counter in the same breath needs this line.
        let inner = &mut *guard;
        self.expire(inner, peer, now);

        let total = inner.running;
        // `get`, never `entry`: an `or_default()` above these checks inserts a
        // bucket for a peer the global cap is about to turn away, and nothing
        // removes it, because `release` and `evict` prune only a bucket they
        // took the last slot out of. With `Allowlist::Anyone` the number of
        // distinct peers is a property of the network, so that is unbounded
        // memory in the one module whose whole purpose is bounds.
        if let Some(bucket) = inner.peers.get(&peer) {
            if let Some(entry) = bucket.slots.get(&key) {
                return entry.slot.replay();
            }
            if bucket.running >= self.limits.running_per_peer {
                return refused_over(self.limits.running_per_peer);
            }
        }
        if total >= self.limits.running {
            return refused_over(self.limits.running);
        }
        {
            let bucket = inner.peers.entry(peer).or_default();
            bucket.slots.insert(
                key.clone(),
                Entry {
                    slot: Slot::Running(Arc::new(Notify::new())),
                    at: now,
                },
            );
            bucket.running += 1;
        }
        inner.running += 1;

        Claim::Fresh(Guard {
            ledger: Arc::clone(self),
            peer,
            key,
            settled: false,
            progress: None,
        })
    }

    /// Take a place to wait in, or refuse the attach before it parks.
    ///
    /// The other half of the running caps, and it has to be a separate count:
    /// an attaching call holds **no** claim, so `Bucket::running` and
    /// `Inner::running` see neither the task nor the QUIC stream pair it holds
    /// open for [`MAX_ATTACH`]. Counted in the ledger rather than per
    /// connection, because a peer's waiters are spread across however many
    /// connections it opened and a per-connection value cannot see the others.
    ///
    /// # Errors
    ///
    /// [`IdempotencyRefusal::TooManyRunning`] once [`MAX_WAITING_PER_PEER`] or
    /// [`MAX_WAITING`] is reached.
    pub(crate) fn wait(self: &Arc<Self>, peer: EndpointId) -> Result<Waiting, RemoteError> {
        let mut guard = self.lock();
        let inner = &mut *guard;
        if inner.waiters.get(&peer).copied().unwrap_or(0) >= self.limits.waiting_per_peer {
            return Err(over(self.limits.waiting_per_peer));
        }
        if inner.waiting >= self.limits.waiting {
            return Err(over(self.limits.waiting));
        }
        *inner.waiters.entry(peer).or_default() += 1;
        inner.waiting += 1;
        Ok(Waiting {
            ledger: Arc::clone(self),
            peer,
        })
    }

    /// What this peer's key is worth, without touching it.
    ///
    /// Peer-scoped by construction: the id is the one iroh proved on the
    /// connection the question arrived on, so a peer cannot name another's.
    pub(crate) fn key_state(&self, peer: EndpointId, key: &IdempotencyKey) -> KeyState {
        let now = Instant::now();
        let mut guard = self.lock();
        let inner = &mut *guard;
        self.expire(inner, peer, now);
        inner
            .peers
            .get(&peer)
            .and_then(|bucket| bucket.slots.get(key))
            .map_or(KeyState::Fresh, |entry| entry.slot.state())
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
        let mut guard = self.lock();
        let inner = &mut *guard;
        let added = slot.held_bytes();
        let holds_reply = matches!(slot, Slot::Done(_));

        let Some(bucket) = inner.peers.get_mut(&peer) else {
            return;
        };
        let Some(previous) = bucket.slots.insert(
            key.clone(),
            Entry {
                slot,
                at: Instant::now(),
            },
        ) else {
            // Nothing was claimed, so nothing is being settled. Unreachable
            // through `Guard`, which only exists for a slot this ledger made.
            bucket.slots.remove(key);
            return;
        };

        // Waiters are woken while the new slot is already in place, so a waiter
        // that re-claims sees the settled value rather than racing the write.
        let was_running = matches!(previous.slot, Slot::Running(_));
        if let Slot::Running(notify) = previous.slot {
            notify.notify_waiters();
            bucket.running = bucket.running.saturating_sub(1);
        }
        if holds_reply {
            bucket.held.push_back(key.clone());
        } else {
            bucket.spent.push_back(key.clone());
        }

        if was_running {
            inner.running = inner.running.saturating_sub(1);
        }
        if holds_reply {
            inner.held += 1;
            inner.bytes += added;
        } else {
            inner.spent += 1;
        }
        self.evict(inner);
    }

    /// Give a claimed key back, because nothing ran under it.
    ///
    /// The counterpart of [`Guard::record`], and the reason a layer that refuses
    /// before dispatch costs the caller nothing: the key is vacant again, so the
    /// caller's retry is a first attempt rather than a permanent refusal.
    fn release(&self, peer: EndpointId, key: &IdempotencyKey) {
        let mut guard = self.lock();
        let inner = &mut *guard;
        let Some(bucket) = inner.peers.get_mut(&peer) else {
            return;
        };
        let Some(entry) = bucket.slots.remove(key) else {
            return;
        };
        // Only a running claim is releasable; a settled one is somebody's
        // answer. Restoring it keeps the counters honest either way.
        let Slot::Running(notify) = entry.slot else {
            bucket.slots.insert(key.clone(), entry);
            return;
        };
        notify.notify_waiters();
        bucket.running = bucket.running.saturating_sub(1);
        let empty = bucket.slots.is_empty();
        inner.running = inner.running.saturating_sub(1);
        if empty {
            inner.peers.remove(&peer);
        }
    }

    /// Age one peer's settled records out, oldest first.
    ///
    /// Both queues are in insertion order, so age is monotone along them and
    /// this stops at the first entry still inside its budget. Scoped to the
    /// peer being served rather than sweeping every bucket, so a claim costs
    /// what that peer's own history costs and nothing else.
    ///
    /// A record past its TTL becomes a tombstone rather than vanishing. The
    /// reply is gone; the promise is not.
    fn expire(&self, inner: &mut Inner, peer: EndpointId, now: Instant) {
        let ttl = self.limits.ttl;
        let Some(bucket) = inner.peers.get_mut(&peer) else {
            return;
        };
        while let Some(key) = bucket.held.front().cloned() {
            let Some(entry) = bucket.slots.get_mut(&key) else {
                bucket.held.pop_front();
                inner.held = inner.held.saturating_sub(1);
                continue;
            };
            if now.saturating_duration_since(entry.at) < ttl {
                break;
            }
            let freed = entry.slot.held_bytes();
            entry.slot = Slot::Forgotten;
            bucket.held.pop_front();
            bucket.spent.push_back(key);
            inner.bytes = inner.bytes.saturating_sub(freed);
            inner.held = inner.held.saturating_sub(1);
            inner.spent += 1;
        }
    }

    /// Bring both pools back inside their bounds.
    ///
    /// Always from the peer holding the most, so a peer that floods the ledger
    /// evicts its own records first and stops being the largest holder before it
    /// can reach anyone else's. A global oldest-first policy would let one peer
    /// flush every other peer's records, which is a denial of service dressed as
    /// a cache.
    ///
    /// Records are **downgraded** rather than dropped: the reply goes and the
    /// key stays, as a tombstone, so a retry of an evicted key is refused
    /// [`IdempotencyRefusal::Forgotten`] instead of silently running again. That
    /// is also why the two pools are counted separately — a tombstone that still
    /// counted as a record would make this loop unable to reach its own bound.
    fn evict(&self, inner: &mut Inner) {
        while inner.held > self.limits.records || inner.bytes > self.limits.bytes {
            let Some(peer) = largest(inner, |bucket| bucket.held.len()) else {
                return;
            };
            let Some(bucket) = inner.peers.get_mut(&peer) else {
                return;
            };
            let Some(key) = bucket.held.pop_front() else {
                return;
            };
            let freed = match bucket.slots.get_mut(&key) {
                Some(entry) => {
                    let freed = entry.slot.held_bytes();
                    entry.slot = Slot::Forgotten;
                    bucket.spent.push_back(key);
                    freed
                }
                None => 0,
            };
            inner.bytes = inner.bytes.saturating_sub(freed);
            inner.held = inner.held.saturating_sub(1);
            inner.spent += 1;
        }

        while inner.spent > self.limits.tombstones {
            let Some(peer) = largest(inner, |bucket| bucket.spent.len()) else {
                return;
            };
            let Some(bucket) = inner.peers.get_mut(&peer) else {
                return;
            };
            let Some(key) = bucket.spent.pop_front() else {
                return;
            };
            bucket.slots.remove(&key);
            let empty = bucket.slots.is_empty();
            inner.spent = inner.spent.saturating_sub(1);
            if empty {
                inner.peers.remove(&peer);
            }
        }
    }
}

/// The peer holding the most of whatever `size` counts, ties broken by id so
/// the order is deterministic rather than whatever the hasher happened to seed.
fn largest(inner: &Inner, size: impl Fn(&Bucket) -> usize) -> Option<EndpointId> {
    inner
        .peers
        .iter()
        .filter(|(_, bucket)| size(bucket) > 0)
        .max_by_key(|(id, bucket)| (size(bucket), **id))
        .map(|(id, _)| *id)
}

/// One refusal, built in one place, so every cap reports the same way.
fn over(limit: usize) -> RemoteError {
    IdempotencyRefusal::TooManyRunning {
        limit: u32::try_from(limit).unwrap_or(u32::MAX),
    }
    .into()
}

/// The same refusal, on the path that answers a claim rather than a wait.
fn refused_over(limit: usize) -> Claim {
    Claim::Refused(over(limit))
}

/// A claimed key, held for as long as the call is being handled.
///
/// Settle it with [`Guard::record`]. Dropping it unsettled asks the call's
/// progress marker what happened, which is the whole of the defect RFC-05 §2
/// records: a refusal that never reached the handler used to be recorded as the
/// call's settled reply and replayed forever, while a call that really was
/// interrupted has to stay unknown.
///
/// Deciding inside `Drop` rather than at the one call site is deliberate. The
/// serving task can be **aborted** — the connection ended, the server shut
/// down — and then there is nobody left to settle the key by hand. `Drop` is
/// the only code that still runs, so it is where the decision has to live.
#[derive(Debug)]
pub(crate) struct Guard {
    ledger: Arc<Ledger>,
    peer: EndpointId,
    key: IdempotencyKey,
    settled: bool,
    /// How far the call got. `None` for a guard nothing is watching, which
    /// settles `Interrupted` on drop — the conservative answer, and the one the
    /// ledger gave before anything watched.
    progress: Option<Progress>,
}

impl Guard {
    /// Watch how far this call gets, so an abort settles honestly.
    #[must_use]
    pub(crate) fn watching(mut self, progress: Progress) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Record the reply frame this call produced.
    ///
    /// `carries_body` is what makes a reply unrecordable: the frame is the
    /// header, and a header replayed without the payload behind it decodes into
    /// a body that never arrives.
    pub(crate) fn record(mut self, frame: &Bytes, carries_body: bool) {
        self.settled = true;
        let slot = if carries_body || frame.len() > self.ledger.limits.bytes {
            Slot::Unrecordable
        } else {
            Slot::Done(frame.clone())
        };
        self.ledger.settle(self.peer, &self.key, slot);
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        // Nothing ran, so nothing is owed and the key goes back. Anything else
        // — entered and unfinished, or finished with nobody left to record it —
        // is genuinely unknown.
        if self.progress.as_ref().map(Progress::reached) == Some(Reached::NotDispatched) {
            self.ledger.release(self.peer, &self.key);
        } else {
            self.ledger.settle(self.peer, &self.key, Slot::Interrupted);
        }
    }
}

/// One attaching call's place in the queue, held for as long as it waits.
///
/// RAII for the reason [`Guard`] is: the ordinary way a waiter stops waiting is
/// that its task is **aborted** — the caller's connection ended — and a
/// decrement written on the return paths of `claim_key` does not run then.
/// `Drop` does. Getting this wrong leaks a place per abandoned attach until the
/// cap is reached with nobody waiting, and every attach after that is refused
/// for as long as the process lives.
#[derive(Debug)]
pub(crate) struct Waiting {
    ledger: Arc<Ledger>,
    peer: EndpointId,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        let mut guard = self.ledger.lock();
        let inner = &mut *guard;
        inner.waiting = inner.waiting.saturating_sub(1);
        let Some(count) = inner.waiters.get_mut(&self.peer) else {
            return;
        };
        *count = count.saturating_sub(1);
        // The entry goes at zero, for the reason `claim` creates no bucket on a
        // refusing path: a map keyed by peer is a map an open network chooses
        // the size of.
        if *count == 0 {
            inner.waiters.remove(&self.peer);
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

    fn records(max: usize) -> Limits {
        Limits {
            records: max,
            ..Limits::default()
        }
    }

    fn refusal(claim: Claim) -> IdempotencyRefusal {
        match claim {
            Claim::Refused(RemoteError::Idempotency(refusal)) => refusal,
            Claim::Running(_) => IdempotencyRefusal::InFlight,
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

    fn waited(place: Result<Waiting, RemoteError>) -> IdempotencyRefusal {
        match place {
            Err(RemoteError::Idempotency(refusal)) => refusal,
            Err(other) => panic!("expected an idempotency refusal, got {other:?}"),
            Ok(_) => panic!("expected a refusal, got a place to wait in"),
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

        /// Byte for byte, not value for value. The property is asserted in three
        /// doc comments and, until this test, by nothing executable — and it is
        /// the one an over-the-wire test cannot check, because postcard is
        /// canonical: a decode-and-re-encode produces the same bytes, so the
        /// wire cannot tell a replayed frame from a rebuilt one.
        ///
        /// It is why the ledger holds an encoded frame rather than a typed
        /// reply, and why a design that made the ledger a `Middleware` was
        /// rejected: a layer sees `(Outcome<Resp>, Body)`, so it would have to
        /// decode and re-encode, and this test is what would stop being true.
        #[test]
        fn a_replayed_reply_is_byte_identical() {
            let ledger = Ledger::new();
            // Deliberately not a run of one repeated byte: a frame that is
            // uniform cannot show a re-encoding that reordered anything.
            let original = Bytes::from((0u8..=255).cycle().take(777).collect::<Vec<u8>>());
            guard(ledger.claim(peer(A), key("k"))).record(&original, false);

            let replayed = recorded(ledger.claim(peer(A), key("k")));
            assert_eq!(
                replayed, original,
                "the recorded frame must be written back verbatim"
            );
            assert_eq!(replayed.len(), 777);
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

        /// The other half of the same rule, and the one that was missing: a key
        /// nothing ever entered goes back to vacant, so the caller's retry is a
        /// first attempt rather than a permanent refusal.
        #[test]
        fn a_guard_dropped_with_nothing_dispatched_leaves_the_key_claimable() {
            let ledger = Ledger::new();
            // A fresh marker says `NotDispatched`, which is what a layer that
            // refused without calling through leaves behind.
            drop(guard(ledger.claim(peer(A), key("k"))).watching(Progress::new()));

            assert!(
                matches!(ledger.claim(peer(A), key("k")), Claim::Fresh(_)),
                "a key nothing ran under must be claimable again"
            );
        }

        /// The other side of the same marker: entered, and no reply. Settling
        /// this as vacant would let the work run twice.
        #[test]
        fn a_guard_dropped_after_dispatch_leaves_the_key_interrupted() {
            let ledger = Ledger::new();
            let progress = Progress::new();
            progress.dispatched();
            drop(guard(ledger.claim(peer(A), key("k"))).watching(progress));

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
            let ledger = Ledger::with_limits(Limits {
                bytes: 64,
                ..Limits::default()
            });
            guard(ledger.claim(peer(A), key("k"))).record(&frame(65), false);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Unrecordable
            );
        }
    }

    mod bounds {
        use super::*;

        #[test]
        fn the_record_count_stays_inside_its_bound() {
            let ledger = Ledger::with_limits(records(4));
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert_eq!(ledger.lock().held, 4);
        }

        #[test]
        fn the_recorded_bytes_stay_inside_their_bound() {
            let ledger = Ledger::with_limits(Limits {
                bytes: 64,
                ..Limits::default()
            });
            for n in 0..64 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(16), false);
            }

            assert!(
                ledger.lock().bytes <= 64,
                "held {} bytes over a 64 byte budget",
                ledger.lock().bytes
            );
        }

        /// A peer that floods the ledger must evict itself, never a quiet peer.
        /// A global oldest-first policy passes every other test in this module
        /// and fails this one.
        #[test]
        fn a_flooding_peer_cannot_evict_a_quiet_peers_record() {
            let ledger = Ledger::with_limits(records(4));
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
        /// handler, which is the one thing the ledger must never do.
        #[test]
        fn a_running_claim_is_never_evicted() {
            let ledger = Ledger::with_limits(records(1));
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

        /// The defect this replaced: an evicted key used to be vacant, so the
        /// work ran a second time and nobody could tell.
        #[test]
        fn an_evicted_key_is_refused_not_rerun() {
            let ledger = Ledger::with_limits(records(2));
            guard(ledger.claim(peer(A), key("old"))).record(&frame(8), false);
            guard(ledger.claim(peer(A), key("new"))).record(&frame(8), false);
            guard(ledger.claim(peer(A), key("newest"))).record(&frame(8), false);

            assert_eq!(
                refusal(ledger.claim(peer(A), key("old"))),
                IdempotencyRefusal::Forgotten,
                "the oldest record must leave a tombstone, not a vacancy"
            );
        }

        /// Tombstones are cheap, not free. Past their own bound the key really
        /// is vacant again, and `KeyState::Fresh` is what says so.
        #[test]
        fn a_tombstone_is_dropped_once_its_own_bound_is_reached() {
            let ledger = Ledger::with_limits(Limits {
                records: 1,
                tombstones: 2,
                ..Limits::default()
            });
            for n in 0..8 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }

            assert!(
                matches!(ledger.claim(peer(A), key("k0")), Claim::Fresh(_)),
                "the oldest tombstone must be the one that left"
            );
            assert!(ledger.lock().spent <= 2);
        }

        #[test]
        fn running_claims_are_bounded_per_peer() {
            let ledger = Ledger::with_limits(Limits {
                running_per_peer: 2,
                ..Limits::default()
            });
            let _one = guard(ledger.claim(peer(A), key("a")));
            let _two = guard(ledger.claim(peer(A), key("b")));

            assert_eq!(
                refusal(ledger.claim(peer(A), key("c"))),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
            assert!(
                matches!(ledger.claim(peer(B), key("c")), Claim::Fresh(_)),
                "one peer's flood must not close the door on another"
            );
        }

        #[test]
        fn running_claims_are_bounded_in_total() {
            let ledger = Ledger::with_limits(Limits {
                running: 2,
                ..Limits::default()
            });
            let _one = guard(ledger.claim(peer(A), key("a")));
            let _two = guard(ledger.claim(peer(B), key("b")));

            assert_eq!(
                refusal(ledger.claim(peer(B), key("c"))),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
        }

        /// The bound the running caps cannot supply: an attaching call holds no
        /// claim, so one claimed key and enough connections parks unboundedly
        /// many tasks and stream pairs and meets neither cap.
        #[test]
        fn waiting_attachers_are_bounded_per_peer() {
            let ledger = Ledger::with_limits(Limits {
                waiting_per_peer: 2,
                ..Limits::default()
            });
            let _one = ledger.wait(peer(A)).expect("the first place is free");
            let _two = ledger.wait(peer(A)).expect("and so is the second");

            assert_eq!(
                waited(ledger.wait(peer(A))),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
            assert!(
                ledger.wait(peer(B)).is_ok(),
                "one peer's attachers must not close the door on another"
            );
        }

        #[test]
        fn waiting_attachers_are_bounded_in_total() {
            let ledger = Ledger::with_limits(Limits {
                waiting: 2,
                ..Limits::default()
            });
            let _one = ledger.wait(peer(A)).expect("free");
            let _two = ledger.wait(peer(B)).expect("free");

            assert_eq!(
                waited(ledger.wait(peer(B))),
                IdempotencyRefusal::TooManyRunning { limit: 2 }
            );
        }

        /// The RAII half, and the one that decides whether the cap is a bound or
        /// a fuse. A waiter usually stops waiting because its task was aborted;
        /// if the place did not come back, the cap would be reached once and
        /// then refuse every attach for as long as the process lives, with
        /// nobody waiting.
        #[test]
        fn a_dropped_waiter_gives_its_place_back() {
            let ledger = Ledger::with_limits(Limits {
                waiting_per_peer: 1,
                ..Limits::default()
            });
            let first = ledger.wait(peer(A)).expect("free");
            assert_eq!(
                waited(ledger.wait(peer(A))),
                IdempotencyRefusal::TooManyRunning { limit: 1 }
            );

            drop(first);

            assert!(
                ledger.wait(peer(A)).is_ok(),
                "the place must come back when the waiter does not"
            );
        }
    }

    mod ttl {
        use super::*;

        /// `tokio::time::pause` rather than a sleep, because a wall-clock
        /// assertion is a flake waiting for a loaded CI box. `n0_future::time`
        /// re-exports tokio's clock on native, so pausing tokio pauses this.
        #[tokio::test(start_paused = true)]
        async fn a_record_expires_after_its_ttl() {
            let ledger = Ledger::with_limits(Limits {
                ttl: Duration::from_secs(60),
                ..Limits::default()
            });
            guard(ledger.claim(peer(A), key("k"))).record(&frame(8), false);
            assert_eq!(recorded(ledger.claim(peer(A), key("k"))), frame(8));

            tokio::time::advance(Duration::from_secs(61)).await;

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::Forgotten,
                "an expired record must leave a tombstone, not its reply"
            );
            assert_eq!(ledger.lock().bytes, 0, "the reply's bytes must be freed");
        }

        #[tokio::test(start_paused = true)]
        async fn a_running_claim_does_not_expire() {
            let ledger = Ledger::with_limits(Limits {
                ttl: Duration::from_secs(60),
                ..Limits::default()
            });
            let _running = guard(ledger.claim(peer(A), key("k")));

            tokio::time::advance(Duration::from_secs(600)).await;

            assert_eq!(
                refusal(ledger.claim(peer(A), key("k"))),
                IdempotencyRefusal::InFlight,
                "a claim with no reply yet has no deadline but the transport's"
            );
        }
    }

    mod state {
        use super::*;

        #[test]
        fn key_state_reports_every_state() {
            let ledger = Ledger::with_limits(records(1));
            assert_eq!(ledger.key_state(peer(A), &key("never")), KeyState::Fresh);

            let running = guard(ledger.claim(peer(A), key("running")));
            assert_eq!(
                ledger.key_state(peer(A), &key("running")),
                KeyState::Running
            );
            drop(running);
            assert_eq!(
                ledger.key_state(peer(A), &key("running")),
                KeyState::Interrupted
            );

            guard(ledger.claim(peer(A), key("body"))).record(&frame(8), true);
            assert_eq!(
                ledger.key_state(peer(A), &key("body")),
                KeyState::Unrecordable
            );

            guard(ledger.claim(peer(A), key("kept"))).record(&frame(8), false);
            assert_eq!(ledger.key_state(peer(A), &key("kept")), KeyState::Done);

            // One record fits, so the second recording evicts the first.
            guard(ledger.claim(peer(A), key("later"))).record(&frame(8), false);
            assert_eq!(ledger.key_state(peer(A), &key("kept")), KeyState::Forgotten);
        }

        #[test]
        fn key_state_is_peer_scoped() {
            let ledger = Ledger::new();
            guard(ledger.claim(peer(A), key("mine"))).record(&frame(8), false);

            assert_eq!(ledger.key_state(peer(A), &key("mine")), KeyState::Done);
            assert_eq!(
                ledger.key_state(peer(B), &key("mine")),
                KeyState::Fresh,
                "one peer must not learn that another used a key"
            );
        }
    }

    /// The counters and the maps must agree, on every path.
    ///
    /// `Inner` tracks `held`, `bytes`, `spent` and `running` beside per-bucket
    /// queues, and every one of `claim`, `settle`, `release`, `expire` and
    /// `evict` moves both halves. A counter that drifts below the truth makes
    /// `evict` stop firing at the bound it exists to enforce, and the ledger
    /// grows without limit while every other test stays green — which is
    /// exactly the failure the exhaustive `match` in `held_bytes` was written to
    /// prevent, one level up. Nothing checked the arithmetic itself until this.
    mod consistency {
        use super::*;

        /// Recompute every counter from the maps and compare. The maps are the
        /// truth; the counters are a cache of them.
        ///
        /// The waiter count is the one thing here that is *not* recomputable
        /// from the maps: a place to wait in is held by a `Waiting` living on
        /// some other task's stack, and `Inner` records how many there are
        /// rather than which. So this checks the two counts against each other
        /// and against the pruning rule, and
        /// `the_waiting_count_follows_the_live_guards` is what checks them
        /// against the guards themselves.
        fn assert_agrees(ledger: &Ledger, whereabouts: &str) {
            let inner = ledger.lock();
            let mut held = 0;
            let mut bytes = 0;
            let mut spent = 0;
            let mut running = 0;
            let mut waiting = 0;
            for count in inner.waiters.values() {
                assert_ne!(
                    *count, 0,
                    "{whereabouts}: a peer with no waiters must be removed from the map, \
                     not left in it at zero"
                );
                waiting += *count;
            }
            assert_eq!(inner.waiting, waiting, "{whereabouts}: waiting");
            for bucket in inner.peers.values() {
                // Fix 1's guard. A bucket is created by the claim that inserts
                // the first slot and removed by whatever takes the last one
                // out, so an empty one is a peer nothing removes — unbounded,
                // because `Allowlist::Anyone` makes the peer count a property
                // of the network.
                assert!(
                    !bucket.slots.is_empty(),
                    "{whereabouts}: an empty bucket must be removed, not left behind"
                );
                assert_eq!(
                    bucket.held.len() + bucket.spent.len() + bucket.running,
                    bucket.slots.len(),
                    "{whereabouts}: a bucket's queues plus its running claims must \
                     account for every slot it holds"
                );
                held += bucket.held.len();
                spent += bucket.spent.len();
                running += bucket.running;
                for entry in bucket.slots.values() {
                    bytes += entry.slot.held_bytes();
                }
            }
            assert_eq!(inner.held, held, "{whereabouts}: held");
            assert_eq!(inner.bytes, bytes, "{whereabouts}: bytes");
            assert_eq!(inner.spent, spent, "{whereabouts}: spent");
            assert_eq!(inner.running, running, "{whereabouts}: running");
        }

        /// Every settled path, mixed, against tight bounds so eviction and the
        /// tombstone pool both fire many times rather than never.
        #[test]
        fn the_counters_agree_with_the_maps_on_every_path() {
            let ledger = Ledger::with_limits(Limits {
                records: 3,
                bytes: 64,
                tombstones: 4,
                ..Limits::default()
            });
            assert_agrees(&ledger, "empty");

            // Held across the whole loop, so every probe below runs with a live
            // waiter for a peer whose bucket comes and goes underneath it. That
            // is the reason the waiter counts live beside `peers` rather than
            // inside a `Bucket`: the bucket is removed when its last slot goes,
            // and the waiter is still waiting.
            let _waiter = ledger.wait(peer(A)).expect("a free place");

            for n in 0..40u8 {
                let k = key(&format!("k{n}"));
                let p = if n % 3 == 0 { peer(B) } else { peer(A) };
                match n % 5 {
                    // recorded, sometimes too large to hold
                    0 => guard(ledger.claim(p, k)).record(&frame(usize::from(n) + 1), false),
                    // unrecordable
                    1 => guard(ledger.claim(p, k)).record(&frame(8), true),
                    // interrupted, by dropping an unwatched guard
                    2 => drop(guard(ledger.claim(p, k))),
                    // released, because nothing was dispatched
                    3 => drop(guard(ledger.claim(p, k)).watching(Progress::new())),
                    // claimed and left running for a while
                    _ => {
                        let running = guard(ledger.claim(p, k.clone()));
                        assert_agrees(&ledger, "while a claim is running");
                        running.record(&frame(4), false);
                    }
                }
                assert_agrees(&ledger, &format!("after n={n}"));
            }

            // And a re-claim of an evicted key, which is the path that both
            // removes a tombstone and adds a record in one call.
            for n in 0..20u8 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(2), false);
                assert_agrees(&ledger, &format!("after re-claiming k{n}"));
            }
        }

        /// The TTL path moves entries between the two pools without going
        /// through `settle`, so it gets its own check.
        #[tokio::test(start_paused = true)]
        async fn the_counters_agree_after_records_expire() {
            let ledger = Ledger::with_limits(Limits {
                ttl: Duration::from_secs(60),
                ..Limits::default()
            });
            for n in 0..10u8 {
                guard(ledger.claim(peer(A), key(&format!("k{n}")))).record(&frame(8), false);
            }
            assert_agrees(&ledger, "before the ttl");

            tokio::time::advance(Duration::from_secs(61)).await;
            // `expire` is lazy, so a claim is what drives it.
            let _ = ledger.claim(peer(A), key("wake"));
            assert_agrees(&ledger, "after the ttl");
            assert_eq!(ledger.lock().bytes, 0, "expired records free their bytes");
        }

        /// Fix 1's own case, and the only path that reached it: the GLOBAL cap
        /// refuses a peer the ledger has never seen. Taking the bucket with
        /// `entry(peer).or_default()` before the check meant the refusal itself
        /// introduced the peer, and nothing ever removed it — `release` and
        /// `evict` prune only a bucket they took the last slot out of, and this
        /// one never had one.
        #[test]
        fn a_claim_the_global_cap_refuses_leaves_no_bucket_behind() {
            let ledger = Ledger::with_limits(Limits {
                running: 1,
                ..Limits::default()
            });
            let _running = guard(ledger.claim(peer(A), key("a")));

            assert_eq!(
                refusal(ledger.claim(peer(B), key("b"))),
                IdempotencyRefusal::TooManyRunning { limit: 1 }
            );
            // The probe is the assertion. A second, narrower one here would
            // never be the one that fires, which makes it decoration.
            assert_agrees(&ledger, "after a globally refused claim");
        }

        /// The waiter counts are what `assert_agrees` cannot recompute — a
        /// place to wait in is a value on the waiting task's stack, and `Inner`
        /// records how many exist rather than which — so they are checked
        /// against the guards themselves here.
        #[test]
        fn the_waiting_count_follows_the_live_guards() {
            let ledger = Ledger::new();
            let mut places = Vec::new();
            for _ in 0..4 {
                places.push(ledger.wait(peer(A)).expect("free"));
                places.push(ledger.wait(peer(B)).expect("free"));
                assert_agrees(&ledger, "while places are being taken");
            }
            {
                let inner = ledger.lock();
                assert_eq!(inner.waiting, 8);
                assert_eq!(inner.waiters.get(&peer(A)), Some(&4));
                assert_eq!(inner.waiters.get(&peer(B)), Some(&4));
            }

            while let Some(place) = places.pop() {
                let live = places.len();
                drop(place);
                assert_eq!(
                    ledger.lock().waiting,
                    live,
                    "the count must be one per guard still alive"
                );
                assert_agrees(&ledger, "while places are being given back");
            }

            assert!(
                ledger.lock().waiters.is_empty(),
                "the last waiter to leave must take its map entry with it"
            );
        }
    }

    mod attach {
        use super::*;

        /// The wakeup, end to end: a second caller waits on the running slot and
        /// is answered from the first caller's record.
        #[tokio::test]
        async fn a_waiter_is_woken_when_the_slot_settles() {
            let ledger = Ledger::new();
            let running = guard(ledger.claim(peer(A), key("k")));

            let Claim::Running(notify) = ledger.claim(peer(A), key("k")) else {
                panic!("expected a running slot");
            };

            // Registered BEFORE the settle, which is the hazard RFC-04 §3.3
            // named: `notify_waiters` wakes only the waiters already there.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            running.record(&frame(16), false);
            notified.await;

            assert_eq!(recorded(ledger.claim(peer(A), key("k"))), frame(16));
        }

        /// The half that made §3.3 right to worry: a waiter that registers after
        /// the settle would sleep forever, so `enable()` is not decoration.
        #[tokio::test]
        async fn a_waiter_that_registers_late_still_sees_the_settled_slot() {
            let ledger = Ledger::new();
            let running = guard(ledger.claim(peer(A), key("k")));
            let Claim::Running(notify) = ledger.claim(peer(A), key("k")) else {
                panic!("expected a running slot");
            };

            running.record(&frame(16), false);

            // No `enable()` before the settle, so this wakeup is genuinely lost.
            // The re-claim under the lock is what makes that survivable, and it
            // is why the accept path re-reads the slot before it awaits.
            assert_eq!(recorded(ledger.claim(peer(A), key("k"))), frame(16));
            drop(notify);
        }

        #[test]
        fn a_budget_is_capped_at_max_attach() {
            let idem = Idempotency::attaching_for(key("k"), Duration::from_secs(3600));
            assert_eq!(idem.attach_budget(), Some(MAX_ATTACH));
        }

        #[test]
        fn a_bare_key_does_not_attach() {
            let idem: Idempotency = key("k").into();
            assert_eq!(idem.attach_budget(), None);
            assert_eq!(idem.key(), &key("k"));
        }
    }
}
