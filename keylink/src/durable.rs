//! The idempotency ledger, on a disk.
//!
//! [`Ledger`](crate::idempotency::Ledger) holds every settled reply in memory,
//! which is exactly as long as the process lives. This module is the other
//! option: the same records, written through [`keylink_persist`]'s entry port,
//! so that a key answered before a restart is still answered after one.
//!
//! # What is stored, and what is not
//!
//! One entry per claimed key, keyed on the pair the ledger keys on — the peer
//! iroh proved, and the key that peer chose. Its value is one byte of state and,
//! for a settled call that produced a recordable reply, that reply's frame.
//!
//! **The in-flight half is not stored and cannot be.** A claimed key belongs to
//! a caller holding a live QUIC stream, and no disk carries one of those across
//! a restart. So a key that was claimed when the process died recovers as
//! [`KeyState::Interrupted`](crate::KeyState) — the state that already means
//! *whether it took effect is unknown* — rather than as vacant, which would run
//! the work twice, or as settled, which would invent a reply.
//!
//! # Two writes per keyed call
//!
//! One at the claim, before the handler is entered, and one at the settle,
//! before the reply is written to the stream. Both are the same property said
//! twice: whatever the caller is about to be told, the disk was told first.
//!
//! That is two `fsync`s a memory ledger does not pay, which is why
//! [`Records::Durable`](crate::Records) is opt-in and
//! [`Records::Memory`](crate::Records) is still the default.
//!
//! **And they are taken under one lock per service, so keyed calls do not
//! overlap.** Two `fsync`s is the latency of one call on an idle service; it is
//! also the whole service's keyed throughput, because the store is one lane. At
//! the 7.6 ms figure `keylink/examples/durable_ledger.rs` measures for a
//! comparable store, a durable service serves keyed calls in the low hundreds
//! per second whatever its connection count. An operator sizing a desk needs
//! that sentence and not only the per-call one.
//!
//! # Three ways a key leaves memory, and the disk is told about all of them
//!
//! A settle writes its own record. Everything else — a released claim, a reply
//! downgraded to a tombstone, a tombstone evicted — happens under the ledger's
//! lock, where nothing can await. Those keys are queued on `Inner::pending` and
//! reconciled by the next claim, which is the next code that can.
//!
//! Reconciling reads what **memory** now says and makes the disk agree, rather
//! than replaying the operation that queued it. That is what makes the queue
//! safe against a key being re-claimed while its removal is still pending: the
//! new claim is a `Slot::Running`, and a running slot is left alone.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use iroh::EndpointId;
use keylink_persist::{Entries, EntryKey, Held, StateId};
use tokio::sync::Mutex;

use crate::idempotency::Slot;
use crate::IdempotencyKey;

/// A peer id, in bytes, which is what a record key begins with.
const PEER_LEN: usize = 32;

/// A claimed key whose call never settled. Recovers as `Interrupted`.
const TAG_CLAIMED: u8 = 0;
/// A settled key holding a replayable reply.
const TAG_DONE: u8 = 1;
/// Entered, and no reply came out.
const TAG_INTERRUPTED: u8 = 2;
/// It ran, and this ledger never holds its reply.
const TAG_UNRECORDABLE: u8 = 3;
/// It ran, and the reply is no longer held.
const TAG_FORGOTTEN: u8 = 4;

/// A record key is a peer id and an idempotency key, and both are bounded, so
/// one always fits. Asserted rather than handled, because a failure here would
/// be a key that cannot be stored and therefore a call that cannot be made
/// durable — and the constants are known now.
const _: () = assert!(
    PEER_LEN + crate::MAX_KEY_LEN <= EntryKey::MAX_LEN,
    "a peer id plus a bounded idempotency key must fit in one record key"
);

/// How much dead weight a log carries before it is rewritten.
///
/// The log is rewritten once the entries written since the last rewrite exceed
/// the live count plus this, which bounds the file at roughly twice what it
/// holds and makes the rewrite amortized: each one is paid for by at least as
/// many writes as it copies.
///
/// That holds only while the live count the ledger reports is the store's live
/// count too, which is what `reconcile` is for. A key dropped from memory
/// without the disk being told would make this bound a fiction, and the file
/// would grow while the threshold stayed small.
///
/// The floor exists so that a ledger holding almost nothing does not rewrite
/// itself on every second call.
const COMPACTION_SLACK: u64 = 64;

// ────────────────────────────────────────────────────────────────── the errors

/// Why a durable record could not be read or written.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RecordError {
    /// The store itself refused.
    #[error(transparent)]
    Store(#[from] keylink_persist::Error),

    /// A stored record is not one this build can read.
    ///
    /// Loud, and at startup. The alternative is to skip the record and carry
    /// on, which recovers a ledger missing an unknown subset of its keys — and
    /// a key that is missing reads `Fresh`, so the work runs again. A ledger
    /// that cannot be trusted is worse than one that refuses to open.
    #[error("a stored idempotency record is not readable: {0}")]
    Unreadable(#[from] RecordFault),
}

/// Which check refused a stored record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RecordFault {
    /// Shorter than the peer id every key starts with.
    #[error("a record key is {len} bytes, and a peer id alone is {PEER_LEN}")]
    KeyTooShort {
        /// What was stored.
        len: usize,
    },
    /// The bytes after the peer id are not text, and an idempotency key is.
    #[error("a record key is not text after its peer id")]
    KeyNotText,
    /// The bytes after the peer id are text this build would not accept as a
    /// key. Re-validated on the way in, because a decoded value met no
    /// constructor.
    #[error("a record key is text this build does not accept as an idempotency key")]
    KeyRefused,
    /// The first [`PEER_LEN`] bytes of a record key are not a peer id.
    ///
    /// Its own variant because [`KeyRefused`](Self::KeyRefused) named three
    /// causes and could only have happened for two of them. This one is about
    /// the bytes BEFORE the key, and it is the only one of the three that a
    /// stored record can actually reach: the write-path spelling was covered by
    /// the `const _` above, which proves a peer id plus a bounded idempotency
    /// key always fits.
    #[error("the first {PEER_LEN} bytes of a record key are not a peer id")]
    PeerNotAnId,
    /// A record with no state byte at all.
    #[error("a record holds no state")]
    Empty,
    /// A state byte this build does not know, which is a record written by a
    /// newer one.
    #[error("a record names state {tag}, which this build does not know")]
    UnknownState {
        /// The byte that was read.
        tag: u8,
    },
}

// ─────────────────────────────────────────────────────────────────── the codec

/// The address one key is stored under.
///
/// **The `Result` cannot be `Err`**, and it is still a `Result`: the `const _`
/// above proves a peer id plus a bounded idempotency key fits inside
/// [`EntryKey::MAX_LEN`], so [`EntryKey::new`] cannot refuse one of these. The
/// total spelling would be an `expect`, which this crate denies, so the type
/// carries an outcome nothing produces rather than the crate carrying an
/// `#[allow]`.
///
/// [`RecordFault::KeyRefused`] is what it would report, and that is a lie about
/// which check refused — this is the WRITE path, and `RecordFault`'s own type
/// doc says it names which check refused a **stored** record. Left alone
/// because no caller can observe it.
fn encode_key(peer: EndpointId, key: &IdempotencyKey) -> Result<EntryKey, RecordFault> {
    let key = key.as_str().as_bytes();
    let mut bytes = Vec::with_capacity(PEER_LEN + key.len());
    bytes.extend_from_slice(peer.as_bytes());
    bytes.extend_from_slice(key);
    EntryKey::new(bytes).map_err(|_| RecordFault::KeyRefused)
}

/// The pair a stored address names.
fn decode_key(stored: &EntryKey) -> Result<(EndpointId, IdempotencyKey), RecordFault> {
    let bytes = stored.as_bytes();
    if bytes.len() <= PEER_LEN {
        return Err(RecordFault::KeyTooShort { len: bytes.len() });
    }
    let mut id = [0_u8; PEER_LEN];
    id.copy_from_slice(&bytes[..PEER_LEN]);
    let peer = EndpointId::from_bytes(&id).map_err(|_| RecordFault::PeerNotAnId)?;
    let text = std::str::from_utf8(&bytes[PEER_LEN..]).map_err(|_| RecordFault::KeyNotText)?;
    let key = IdempotencyKey::new(text).map_err(|_| RecordFault::KeyRefused)?;
    Ok((peer, key))
}

/// What one slot is worth on a disk.
///
/// [`Slot::Running`] stores as [`TAG_CLAIMED`], which is the whole of how an
/// interrupted call is recovered: the claim is written before the handler runs,
/// so a process that dies mid-call leaves exactly this.
pub(crate) fn encode_slot(slot: &Slot) -> Vec<u8> {
    match slot {
        Slot::Running(_) => vec![TAG_CLAIMED],
        Slot::Done(frame) => {
            let mut bytes = Vec::with_capacity(1 + frame.len());
            bytes.push(TAG_DONE);
            bytes.extend_from_slice(frame);
            bytes
        }
        Slot::Interrupted => vec![TAG_INTERRUPTED],
        Slot::Unrecordable => vec![TAG_UNRECORDABLE],
        Slot::Forgotten => vec![TAG_FORGOTTEN],
    }
}

/// What a stored record becomes when the process comes back.
fn decode_slot(stored: &[u8]) -> Result<Slot, RecordFault> {
    let Some((tag, rest)) = stored.split_first() else {
        return Err(RecordFault::Empty);
    };
    match *tag {
        // The claim outlived the call that took it, which is the definition of
        // this state and not a guess about it.
        TAG_CLAIMED | TAG_INTERRUPTED => Ok(Slot::Interrupted),
        TAG_DONE => Ok(Slot::Done(Bytes::copy_from_slice(rest))),
        TAG_UNRECORDABLE => Ok(Slot::Unrecordable),
        TAG_FORGOTTEN => Ok(Slot::Forgotten),
        tag => Err(RecordFault::UnknownState { tag }),
    }
}

// ──────────────────────────────────────────────────────────────── the erasure

/// One boxed operation on an erased store.
type Writing<'a> = Pin<Box<dyn Future<Output = keylink_persist::Result<()>> + Send + 'a>>;

/// [`Held`] with its type forgotten.
///
/// The port returns `impl Future`, which is what makes it cheap for an adapter
/// to write and what makes it impossible to hold as a `dyn`. Erasing it here
/// rather than making [`Records`](crate::Records) generic is deliberate:
/// `Records` is public and reaches [`ServiceBuilder`](crate::ServiceBuilder),
/// so a type parameter on it would spread through every service that never
/// wanted one. The boxed future costs one allocation per durable write, beside
/// an `fsync`.
trait ErasedStore: Send {
    fn put<'a>(&'a mut self, key: &'a EntryKey, bytes: &'a [u8]) -> Writing<'a>;
    fn remove<'a>(&'a mut self, key: &'a EntryKey) -> Writing<'a>;
    fn compact(&mut self) -> Writing<'_>;
}

impl<H: Held> ErasedStore for H {
    fn put<'a>(&'a mut self, key: &'a EntryKey, bytes: &'a [u8]) -> Writing<'a> {
        Box::pin(Held::put(self, key, bytes))
    }

    fn remove<'a>(&'a mut self, key: &'a EntryKey) -> Writing<'a> {
        Box::pin(Held::remove(self, key))
    }

    fn compact(&mut self) -> Writing<'_> {
        Box::pin(Held::compact(self))
    }
}

// ──────────────────────────────────────────────────────────────── the records

/// An idempotency ledger's records, held on a durable store.
///
/// Build one with [`open`](Self::open) and hand it to a service as
/// [`Records::Durable`](crate::Records).
///
/// **One of these per service.** Cloning it shares the store, which is what an
/// `Arc` is for and is also the one way to misuse it: two services sharing a
/// record set would replay one service's reply into the other's response type,
/// which is the confusion a per-service ledger exists to prevent. The clone
/// exists because [`Records`](crate::Records) is `Clone`, not because sharing
/// is a use.
#[derive(Clone)]
pub struct DurableRecords {
    store: Arc<Store>,
}

/// The claimed store, and what it held when this process opened it.
struct Store {
    id: StateId,
    held: Mutex<Box<dyn ErasedStore>>,
    /// What was on the disk at startup, taken once by the ledger built from it.
    ///
    /// A `std::sync::Mutex` and not the async one beside it: this is taken from
    /// `Ledger::new`, which is not a future, and holding it spans no await.
    recovered: std::sync::Mutex<Vec<(EndpointId, IdempotencyKey, Slot)>>,
    /// Entries written since the log was last rewritten. Read against the live
    /// count, which only the ledger knows.
    since_compaction: AtomicU64,
}

impl fmt::Debug for DurableRecords {
    /// Written out because an erased store is not `Debug` and should not have to
    /// be: an adapter author implements two traits, and neither is this one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableRecords")
            .field("id", &self.store.id)
            .finish_non_exhaustive()
    }
}

impl DurableRecords {
    /// Claim `id` on `store` and read back whatever is there.
    ///
    /// The claim is held for as long as this value lives, which is what stops
    /// two processes writing one ledger. Recovery happens here, once, so that
    /// building the service stays synchronous and every stored record has been
    /// read and validated before a single call is served.
    ///
    /// # Errors
    ///
    /// [`RecordError::Store`] when the store refuses the claim or the read —
    /// including [`Busy`](keylink_persist::Error::Busy), which means another
    /// process believes it owns this ledger. [`RecordError::Unreadable`] when a
    /// stored record is not one this build can read.
    pub async fn open<E: Entries>(store: &E, id: &StateId) -> Result<Self, RecordError> {
        let held = store.claim(id).await?;
        let stored = held.load().await?;

        let mut recovered = Vec::with_capacity(stored.len());
        for (key, value) in stored {
            let (peer, key) = decode_key(&key)?;
            recovered.push((peer, key, decode_slot(&value)?));
        }

        Ok(Self {
            store: Arc::new(Store {
                id: id.clone(),
                held: Mutex::new(Box::new(held)),
                recovered: std::sync::Mutex::new(recovered),
                since_compaction: AtomicU64::new(0),
            }),
        })
    }

    /// What was on the disk, handed over once.
    ///
    /// The ledger built from these records takes them; a second reader gets
    /// nothing, which is the honest answer — there is one ledger per store, and
    /// this value is how the first one is told what it already knows.
    pub(crate) fn take_recovered(&self) -> Vec<(EndpointId, IdempotencyKey, Slot)> {
        let mut held = self
            .store
            .recovered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut held)
    }

    /// Record that a key has been taken, before its call is entered.
    ///
    /// Its own method rather than [`write`](Self::write) with a
    /// [`Slot::Running`], because building one of those means building the
    /// [`Notify`](tokio::sync::Notify) a waiter parks on — an allocation and a
    /// synchronisation primitive, to name a byte.
    pub(crate) async fn claimed(
        &self,
        peer: EndpointId,
        key: &IdempotencyKey,
    ) -> Result<(), RecordError> {
        let address = encode_key(peer, key)?;
        self.store
            .held
            .lock()
            .await
            .put(&address, &[TAG_CLAIMED])
            .await?;
        self.store.since_compaction.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Write one key's state, and resolve only once it is on the disk.
    pub(crate) async fn write(
        &self,
        peer: EndpointId,
        key: &IdempotencyKey,
        slot: &Slot,
    ) -> Result<(), RecordError> {
        let address = encode_key(peer, key)?;
        let value = encode_slot(slot);
        self.store.held.lock().await.put(&address, &value).await?;
        self.store.since_compaction.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Make the disk agree with memory about a batch of keys, and return the
    /// ones it could not.
    ///
    /// `current` is asked, key by key, what memory says **now**. It is called
    /// while this store's lock is held and takes the ledger's own lock, which is
    /// the ordering that makes the whole thing safe: every disk write for a key
    /// goes through this lock, so a claim that beats the check to memory cannot
    /// also beat this erase to the disk. It arrives after, and its record wins.
    ///
    /// Reversing that — checking memory first and taking the lock second — is
    /// the race this shape exists to remove: a key evicted past the tombstone
    /// pool is immediately claimable again, so a removal in flight could delete
    /// a live claim and leave a handler running with nothing on the disk.
    pub(crate) async fn reconcile<F>(
        &self,
        mut batch: Vec<(EndpointId, IdempotencyKey)>,
        mut current: F,
    ) -> Vec<(EndpointId, IdempotencyKey)>
    where
        F: FnMut(EndpointId, &IdempotencyKey) -> Reconcile,
    {
        let mut held = self.store.held.lock().await;
        for index in 0..batch.len() {
            let (peer, key) = &batch[index];
            let (peer, key) = (*peer, key);
            let Ok(address) = encode_key(peer, key) else {
                // A key this build cannot address is a key it never wrote.
                continue;
            };
            let wrote = match current(peer, key) {
                Reconcile::Untouched => continue,
                Reconcile::Erase => held.remove(&address).await,
                Reconcile::Write(value) => held.put(&address, &value).await,
            };
            if wrote.is_err() {
                // A store that refuses one write refuses the next. Hand the
                // rest back rather than burning the batch against it.
                //
                // `split_off` and not `[index..].to_vec()`: the remainder is
                // moved out of a `Vec` that is about to drop, so the clone that
                // spelling paid for bought nothing. The loop is indexed for the
                // same reason — an iterator would still be borrowing `batch`.
                return batch.split_off(index);
            }
            self.store.since_compaction.fetch_add(1, Ordering::Relaxed);
        }
        Vec::new()
    }

    /// Rewrite the store if it is carrying more dead weight than `live` entries
    /// justify.
    ///
    /// The ledger is the only thing that knows `live`, and the store is the only
    /// thing that knows how much has been written. Neither can make this
    /// decision alone, so the number crosses here. `live` is the ledger's count
    /// and the store's, which the reconcile above is what keeps equal.
    ///
    /// **Reports nothing, failure included.** A rewrite is never required for
    /// correctness, so there is no outcome for a caller to act on. What a
    /// failure must not do is retry on the next call and every call after it,
    /// which is what leaving the counter alone would produce — a full disk is
    /// exactly when compaction is wanted and exactly when it fails, and each
    /// attempt reads and encodes the whole log before failing. So the counter is
    /// reset either way, and a failed rewrite waits another `live` entries like
    /// any other.
    pub(crate) async fn compact_if_worthwhile(&self, live: usize) {
        let written = self.store.since_compaction.load(Ordering::Relaxed);
        let Ok(live) = u64::try_from(live) else {
            return;
        };
        if written <= live.saturating_add(COMPACTION_SLACK) {
            return;
        }
        let _ = self.store.held.lock().await.compact().await;
        // `fetch_sub` and not `store(0)`: the lock is released at the end of the
        // statement above, so a put that lands between there and here has
        // already counted itself, and a store would throw that count away.
        self.store
            .since_compaction
            .fetch_sub(written, Ordering::Relaxed);
    }
}

/// What the disk should be told about one key whose memory state moved.
pub(crate) enum Reconcile {
    /// Memory holds nothing under it. Take it off the disk.
    Erase,
    /// Memory holds a settled slot. Write it.
    Write(Vec<u8>),
    /// A claim owns the key now, so the write that counts is its own.
    Untouched,
}
