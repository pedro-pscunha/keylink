use std::collections::VecDeque;
use std::marker::PhantomData;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::watch;

use super::{
    Cursor, LogId, Page, Position, Pull, PullRefused, Seq, MAX_EVENT_BYTES, MAX_PULLERS,
    PAGE_BYTES, PULL_WAIT,
};
use crate::{Observer, Replied, Reply, TurnFut, TurnInfo, TurnOutcome};

#[cfg(test)]
#[path = "log_tests.rs"]
mod tests;

/// How much history a log keeps. There is no default retention policy.
///
/// The newest event is always kept, even when it exceeds the byte or age bound.
/// Encoded memory is therefore bounded by `bytes` plus one event. Durable batches
/// can retain extra disk bytes until the last event of that batch is evicted.
#[derive(Debug, Clone, Copy)]
pub struct Retention {
    events: NonZeroUsize,
    bytes: NonZeroU64,
    age: Option<Duration>,
}

impl Retention {
    /// Keep at most `events` events and `bytes` encoded bytes, oldest removed first.
    #[must_use]
    pub const fn new(events: NonZeroUsize, bytes: NonZeroU64) -> Self {
        Self {
            events,
            bytes,
            age: None,
        }
    }

    /// Also remove events older than `age`, using the wall clock.
    ///
    /// After a clock regression, cut through the last expired non-newest event,
    /// including preceding future-dated events, to retain a contiguous suffix.
    #[must_use]
    pub const fn max_age(mut self, age: Duration) -> Self {
        self.age = Some(age);
        self
    }
}

/// A sequenced, retained event log. Clones share one log and one exclusive writer.
pub struct EventLog<E> {
    inner: Arc<Inner>,
    event: PhantomData<fn() -> E>,
}

impl<E> Clone for EventLog<E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            event: PhantomData,
        }
    }
}

struct Inner {
    id: LogId,
    retention: Retention,
    ring: Mutex<Ring>,
    writer: tokio::sync::Mutex<Writer>,
    // Some is the last published head; None is poisoned until recovery.
    changed: watch::Sender<Option<u64>>,
    #[cfg(feature = "persist")]
    durable: bool,
}

struct Writer {
    #[cfg(feature = "persist")]
    store: Option<Box<dyn super::store::Records>>,
    #[cfg(feature = "persist")]
    truncated_before: u64,
}

struct Ring {
    events: VecDeque<Held>,
    bytes: u64,
    head: Seq,
    parked: usize,
}

struct Held {
    seq: Seq,
    at_ms: u64,
    bytes: Bytes,
}

impl Ring {
    fn trim(&mut self, retention: Retention, now_ms: u64) -> usize {
        // Wall-clock timestamps need not increase with sequence. Keep a
        // contiguous suffix by cutting through the LAST expired non-newest event.
        let expired_through = retention.age.and_then(|age| {
            self.events
                .iter()
                .rev()
                .skip(1)
                .find(|event| u128::from(now_ms.saturating_sub(event.at_ms)) > age.as_millis())
                .map(|event| event.seq)
        });
        let mut removed = 0;
        while self.events.len() > 1 {
            let over_age = expired_through
                .is_some_and(|last| self.events.front().is_some_and(|event| event.seq <= last));
            if self.events.len() <= retention.events.get()
                && self.bytes <= retention.bytes.get()
                && !over_age
            {
                break;
            }
            if let Some(event) = self.events.pop_front() {
                self.bytes -= event.bytes.len() as u64;
                removed += 1;
            }
        }
        removed
    }

    fn oldest(&self) -> Seq {
        self.events.front().map_or(Seq(1), |event| event.seq)
    }
}

impl Inner {
    fn ring(&self) -> MutexGuard<'_, Ring> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<E> EventLog<E>
where
    E: Serialize + DeserializeOwned + Send + 'static,
{
    /// Create a memory log. Recreating it gives old cursors a `Replaced` refusal.
    ///
    /// # Panics
    ///
    /// If OS/browser randomness is unavailable. A clock-based fallback cannot
    /// distinguish independently created log incarnations and is never used.
    #[must_use]
    pub fn memory(retention: Retention) -> Self {
        Self {
            inner: Arc::new(Self::build(LogId::mint(), retention)),
            event: PhantomData,
        }
    }

    fn build(id: LogId, retention: Retention) -> Inner {
        let (changed, _) = watch::channel(Some(0));
        Inner {
            id,
            retention,
            ring: Mutex::new(Ring {
                events: VecDeque::new(),
                bytes: 0,
                head: Seq(0),
                parked: 0,
            }),
            writer: tokio::sync::Mutex::new(Writer {
                #[cfg(feature = "persist")]
                store: None,
                #[cfg(feature = "persist")]
                truncated_before: 1,
            }),
            changed,
            #[cfg(feature = "persist")]
            durable: false,
        }
    }

    /// Open an exclusive durable log through `Snapshots::claim`.
    ///
    /// The snapshot slot holds a framed identity, written only at creation. One
    /// checksummed log record holds one whole append batch, keyed by its **last**
    /// event sequence. Recovery never exposes a prefix of a torn turn's batch.
    /// A torn record is cut by the adapter and reported as [`OpenError::Torn`];
    /// retrying open then reads the intact prefix. A saved cursor past that
    /// prefix gets `Ahead`, never silently skips it.
    ///
    /// # Errors
    ///
    /// [`OpenError`] for a busy or failed store, incompatible identity or record,
    /// a gap, an oversized stored event, or a cut torn tail. Adapter recovery
    /// budgets are surfaced as `OpenError::Store(Error::TooLarge)`, without
    /// guessing at or skipping stored records. Event schema migration
    /// is not built. Reopen with the same `E`: overlapping encodings can silently
    /// change meaning. Use a fresh id for an incompatible event schema.
    #[cfg(feature = "persist")]
    pub async fn durable<S: keylink_persist::Snapshots>(
        snapshots: &S,
        id: &keylink_persist::StateId,
        retention: Retention,
    ) -> Result<Self, OpenError> {
        let (identity, store, records) = super::store::open(snapshots, id).await?;
        let mut inner = Self::build(identity, retention);
        let mut previous = None;
        for (last, record) in records {
            let batch: super::store::Batch = super::store::decode(Seq(last), &record)?;
            let count = batch.events.len() as u64;
            let first = last
                .checked_sub(count.saturating_sub(1))
                .filter(|first| *first > 0 && count > 0)
                .ok_or(OpenError::Invalid { seq: Seq(last) })?;
            if previous.is_some_and(|previous: u64| previous.checked_add(1) != Some(first)) {
                return Err(OpenError::Invalid { seq: Seq(first) });
            }
            for (offset, bytes) in batch.events.into_iter().enumerate() {
                let seq = Seq(first + offset as u64);
                if bytes.len() > MAX_EVENT_BYTES {
                    return Err(OpenError::Invalid { seq });
                }
                let _: E = super::store::decode(seq, &bytes)?;
                let mut ring = inner.ring();
                ring.bytes += bytes.len() as u64;
                ring.events.push_back(Held {
                    seq,
                    at_ms: batch.at_ms,
                    bytes: bytes.into(),
                });
                ring.head = seq;
                ring.trim(retention, now_ms());
            }
            previous = Some(last);
        }
        let head = inner.ring().head;
        inner.changed.send_replace(Some(head.0));
        inner.durable = true;
        inner.writer.get_mut().store = Some(store);
        Ok(Self {
            inner: Arc::new(inner),
            event: PhantomData,
        })
    }

    /// Append one event, durable before resolving for a durable log.
    ///
    /// Never waits for a follower. Dropping this future after the durable write
    /// starts does not cancel the write or leave the in-memory head behind disk.
    ///
    /// # Errors
    ///
    /// [`AppendError`] for encoding, size, exhausted sequences or storage failure.
    /// A store's `Indeterminate` error means the batch may be on disk: this log
    /// then refuses appends and pulls with `Poisoned` until all handles are dropped
    /// and it is reopened through recovery. Never automatically retry that batch.
    pub async fn append(&self, event: E) -> Result<Seq, AppendError> {
        if self.inner.changed.borrow().is_none() {
            return Err(AppendError::Poisoned);
        }
        self.append_batch(vec![encode(event)?]).await
    }

    async fn append_batch(&self, batch: Vec<Bytes>) -> Result<Seq, AppendError> {
        #[cfg(feature = "persist")]
        if self.inner.durable {
            let log = self.clone();
            // A detached writer owns the Claim until disk AND memory agree. In
            // particular, cancelling the caller cannot strand FileClaim's handle.
            let task = n0_future::task::spawn(async move { log.commit(batch).await });
            return task.await.map_err(|_| AppendError::Interrupted)?;
        }
        self.commit(batch).await
    }

    async fn commit(&self, batch: Vec<Bytes>) -> Result<Seq, AppendError> {
        let mut writer = self.inner.writer.lock().await;
        let before = {
            let ring = self.inner.ring();
            if self.inner.changed.borrow().is_none() {
                return Err(AppendError::Poisoned);
            }
            ring.head.0
        };
        let last = before
            .checked_add(batch.len() as u64)
            .ok_or(AppendError::Exhausted)?;
        let at_ms = now_ms();
        #[cfg(feature = "persist")]
        if let Some(store) = &mut writer.store {
            let bytes = super::store::encode_batch(at_ms, batch.iter().map(Bytes::as_ref))
                .map_err(AppendError::Encode)?;
            if let Err(error) = store.append(last, &bytes).await {
                if matches!(&error, keylink_persist::Error::Indeterminate { .. }) {
                    // The record may exist on disk. Stop advertising the ring
                    // and wake parked readers; only a fresh recovery can decide.
                    let _ring = self.inner.ring();
                    self.inner.changed.send_replace(None);
                }
                return Err(AppendError::Store(error));
            }
        }
        let (oldest, retained) = {
            let mut ring = self.inner.ring();
            for (offset, bytes) in batch.into_iter().enumerate() {
                let seq = Seq(before + offset as u64 + 1);
                ring.bytes += bytes.len() as u64;
                ring.events.push_back(Held { seq, at_ms, bytes });
                // Bound the backing allocation too, not just the final length.
                ring.trim(self.inner.retention, at_ms);
            }
            ring.head = Seq(last);
            (ring.oldest(), ring.events.len())
        };
        self.inner.changed.send_replace(Some(last));
        #[cfg(feature = "persist")]
        {
            // Count by position, including age evictions performed by pulls.
            if oldest.0.saturating_sub(writer.truncated_before) >= retained.max(64) as u64 {
                if let Some(store) = &mut writer.store {
                    // A failed cut cannot be returned as a failed append: the
                    // event already exists and retrying it would duplicate it.
                    if store.truncate_before(oldest.0).await.is_ok() {
                        writer.truncated_before = oldest.0;
                    }
                }
            }
        }
        #[cfg(not(feature = "persist"))]
        let _ = (&mut writer, oldest, retained);
        Ok(Seq(last))
    }

    /// Pull events after `from`, waiting up to [`PULL_WAIT`] when caught up.
    ///
    /// Use in a concurrent handler. Inside an actor use [`Self::answer`] instead:
    /// awaiting a long poll inside the turn would stop every other actor method.
    pub async fn pull(&self, from: Cursor) -> Pull<E> {
        // Subscribe BEFORE examining the ring so an intervening append cannot
        // leave this pull waiting on a head it already missed.
        let mut changed = self.inner.changed.subscribe();
        let after = {
            let mut ring = self.inner.ring();
            ring.trim(self.inner.retention, now_ms());
            let after = self.position(&ring, from)?;
            if after < ring.head.0 {
                let page = self.page(&ring, after);
                drop(ring);
                return Self::materialize(page);
            }
            if ring.parked >= MAX_PULLERS {
                return Err(PullRefused::Crowded {
                    parked: ring.parked as u64,
                });
            }
            ring.parked += 1;
            after
        };
        let _park = ParkGuard(&self.inner);
        let _ = n0_future::time::timeout(
            PULL_WAIT,
            changed.wait_for(|head| head.is_none_or(|head| head > after)),
        )
        .await;
        let mut ring = self.inner.ring();
        ring.trim(self.inner.retention, now_ms());
        self.position(&ring, from)?;
        let page = self.page(&ring, after);
        drop(ring);
        Self::materialize(page)
    }

    fn position(&self, ring: &Ring, from: Cursor) -> Result<u64, PullRefused> {
        if self.inner.changed.borrow().is_none() {
            return Err(PullRefused::Poisoned);
        }
        let head = Cursor::after(self.inner.id, ring.head);
        let after = match from.0 {
            Position::Origin => 0,
            Position::After { log, seq } => {
                if log != self.inner.id {
                    return Err(PullRefused::Replaced { head });
                }
                seq.0
            }
        };
        if after > ring.head.0 {
            return Err(PullRefused::Ahead {
                asked: Seq(after),
                head,
            });
        }
        let oldest = ring.oldest();
        if after < oldest.0.saturating_sub(1) {
            return Err(PullRefused::Expired {
                asked: Seq(after),
                oldest,
                head,
            });
        }
        Ok(after)
    }

    fn page(&self, ring: &Ring, after: u64) -> Page<Bytes> {
        let mut bytes = 0;
        let events: Vec<_> = ring
            .events
            .iter()
            .filter(|event| event.seq.0 > after)
            .take_while(|event| {
                bytes += event.bytes.len() + 10;
                bytes <= PAGE_BYTES
            })
            .map(|event| (event.seq, event.bytes.clone()))
            .collect();
        let next = events.last().map_or(Seq(after), |(seq, _)| *seq);
        Page {
            events,
            next: Cursor::after(self.inner.id, next),
            head: ring.head,
        }
    }

    fn materialize(page: Page<Bytes>) -> Pull<E> {
        let events = page
            .events
            .into_iter()
            .map(|(seq, bytes)| {
                let (event, remaining) = postcard::take_from_bytes::<E>(&bytes)
                    .map_err(|_| PullRefused::Unreadable { seq })?;
                if !remaining.is_empty() {
                    return Err(PullRefused::Unreadable { seq });
                }
                Ok((seq, event))
            })
            .collect::<Result<_, _>>()?;
        Ok(Page {
            events,
            next: page.next,
            head: page.head,
        })
    }

    /// Park an actor's deferred reply, end its turn, and pull on a separate task.
    pub fn answer(&self, reply: Reply<Pull<E>>, from: Cursor) -> Replied<Pull<E>> {
        let (reply, token) = reply.later();
        let log = self.clone();
        n0_future::task::spawn(async move { reply.send(log.pull(from).await) });
        token
    }

    /// This log's identity and last confirmed event sequence.
    ///
    /// After an indeterminate append this remains the last confirmed cursor,
    /// not a claim about the disk's head. Pulls are refused until recovery.
    #[must_use]
    pub fn head(&self) -> Cursor {
        Cursor::after(self.inner.id, self.inner.ring().head)
    }

    /// One staging pair per actor incarnation. Keep the stage in the actor and
    /// pass the publisher to `.observe(publish)`.
    ///
    /// Do not share one stage between actors or push outside its actor's turns.
    /// A publisher refused by an earlier observer never runs; the actor ends and
    /// its pair must be discarded. A restarting factory creates a fresh pair.
    #[must_use]
    pub fn stage(&self) -> (Stage<E>, Publish<E>) {
        let stage = Stage {
            batch: Arc::new(Mutex::new(Vec::new())),
            event: PhantomData,
        };
        let publish = Publish {
            log: self.clone(),
            batch: stage.batch.clone(),
        };
        (stage, publish)
    }
}

struct ParkGuard<'a>(&'a Inner);
impl Drop for ParkGuard<'_> {
    fn drop(&mut self) {
        self.0.ring().parked -= 1;
    }
}

fn now_ms() -> u64 {
    n0_future::time::SystemTime::now()
        .duration_since(n0_future::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn encode<E: Serialize>(event: E) -> Result<Bytes, AppendError> {
    let bytes = postcard::to_allocvec(&event).map_err(AppendError::Encode)?;
    if bytes.len() > MAX_EVENT_BYTES {
        return Err(AppendError::TooLarge {
            len: bytes.len(),
            max: MAX_EVENT_BYTES,
        });
    }
    Ok(bytes.into())
}

/// Events staged by one actor's current turn. A stage is intentionally not cloneable.
pub struct Stage<E> {
    batch: Arc<Mutex<Vec<Bytes>>>,
    event: PhantomData<fn() -> E>,
}

impl<E: Serialize> Stage<E> {
    /// Encode and stage an event. Sequence numbers are assigned only at commit,
    /// so a turn cannot put its own event's sequence in its reply.
    ///
    /// # Errors
    ///
    /// [`AppendError::TooLarge`] or [`AppendError::Encode`], before staging anything.
    pub fn push(&self, event: E) -> Result<(), AppendError> {
        let event = encode(event)?;
        self.batch
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
        Ok(())
    }
}

/// An observer that publishes a completed turn's batch before its reply.
///
/// A panicked turn clears the batch. A cleanly failed durable append publishes
/// none of the batch and ends the actor with a refusal named `Publish`. An
/// indeterminate append may already exist on disk: it poisons the log and ends
/// the actor instead of inviting a retry. The batch is one record, not several
/// appends, so recovery cannot expose only part of a turn.
/// See the module's warning about ordering this observer against `Persist`.
pub struct Publish<E> {
    log: EventLog<E>,
    batch: Arc<Mutex<Vec<Bytes>>>,
}

impl<A: Send + 'static, E> Observer<A> for Publish<E>
where
    E: Serialize + DeserializeOwned + Send + 'static,
{
    fn name(&self) -> &'static str {
        "Publish"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a A, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let batch = std::mem::take(&mut *self.batch.lock().unwrap_or_else(PoisonError::into_inner));
        let completed = matches!(turn.outcome, TurnOutcome::Completed);
        Box::pin(async move {
            if completed && !batch.is_empty() {
                self.log
                    .append_batch(batch)
                    .await
                    .map_err(crate::Stop::fatal)?;
            }
            Ok(())
        })
    }
}

/// Why an event or turn batch was not confirmed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AppendError {
    /// An encoded event exceeded the per-event cap.
    #[error("an event of {len} bytes is over the {max} byte limit")]
    TooLarge {
        /// Encoded event length.
        len: usize,
        /// Per-event cap.
        max: usize,
    },
    /// The event or batch could not be encoded.
    #[error(transparent)]
    Encode(postcard::Error),
    /// There are no sequence numbers left.
    #[error("the event sequence is exhausted")]
    Exhausted,
    /// The log is unavailable after an indeterminate write. Drop every handle
    /// and reopen through recovery instead of retrying an append that may exist.
    #[error("the event log is poisoned and requires recovery")]
    Poisoned,
    /// The store refused a batch. `Indeterminate` means the record may exist:
    /// the live log is then poisoned, not safe to retry. Other errors append none.
    #[cfg(feature = "persist")]
    #[error(transparent)]
    Store(keylink_persist::Error),
    /// The detached durable writer stopped unexpectedly. Reopen before writing again.
    #[cfg(feature = "persist")]
    #[error("the event writer stopped unexpectedly")]
    Interrupted,
}

/// Why a durable log could not be opened. Stored events are never silently skipped.
#[cfg(feature = "persist")]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OpenError {
    /// The claim could not be taken or read. Includes `TooLarge` when the
    /// journal exceeds the adapter's recovery budget; nothing is skipped.
    #[error(transparent)]
    Store(#[from] keylink_persist::Error),
    /// A record or event did not decode.
    #[error("the record at {seq} did not decode: {error}")]
    Decode {
        /// The record or event's sequence.
        seq: Seq,
        /// The decoder's refusal.
        error: postcard::Error,
    },
    /// A batch was empty, noncontiguous or contained an oversized event.
    #[error("the event log is invalid at {seq}")]
    Invalid {
        /// The first invalid position.
        seq: Seq,
    },
    /// The identity slot does not hold this module's identity frame.
    #[error("the snapshot slot is not an event-log identity")]
    Identity,
    /// Recovery cut a torn last record. The next open reads the intact prefix.
    #[error(
        "recovery cut {bytes} torn bytes from the event log; reopen to accept the intact prefix"
    )]
    Torn {
        /// How many bytes the adapter cut.
        bytes: u64,
    },
}
