use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use n0_future::Stream;

use super::{Cursor, LogId, Page, Position, Pull, PullRefused, Seq};
use crate::{Dial, Error, Peer, RemoteError};

/// Follow through an application's pull method, reconnecting with surfaced backoff.
///
/// The closure receives a shared client and the last delivered cursor. Several
/// followers can share one `Arc<Peer<C>>` and therefore one connection. Save a
/// delivery's resume cursor only after processing the event. Transport outages,
/// remote capacity, timeouts, restarting actors and full mailboxes back off and
/// retry; other failures terminate the stream.
///
/// ```ignore
/// let mut events = keylink::events::follow(peer, saved, |client, from| async move {
///     client.events(from).await
/// });
/// while let Some(item) = events.next().await {
///     match item {
///         Ok(delivery) => { apply(delivery.event); save(delivery.resume); }
///         Err(keylink::events::Lapse::Retrying { .. }) => {}
///         Err(terminal) => break,
///     }
/// }
/// ```
pub fn follow<C, E, F, Fut>(peer: Arc<Peer<C>>, from: Cursor, pull: F) -> Following<E>
where
    C: Dial + Send + Sync + 'static,
    E: Send + 'static,
    F: FnMut(Arc<C>, Cursor) -> Fut + Send + 'static,
    Fut: Future<Output = crate::Result<Pull<E>>> + Send + 'static,
{
    let cursor = Arc::new(Mutex::new(from));
    let state = State {
        peer,
        pull,
        cursor: cursor.clone(),
        buffer: None,
        attempt: 0,
        wait: None,
        ended: false,
    };
    let stream = n0_future::stream::unfold(state, |mut state| async move {
        if state.ended {
            return None;
        }
        loop {
            if let Some(buffer) = &mut state.buffer {
                if let Some((seq, event)) = buffer.events.pop_front() {
                    let resume = Cursor::after(buffer.log, seq);
                    *state.cursor.lock().unwrap_or_else(PoisonError::into_inner) = resume;
                    return Some((Ok(Delivery { seq, event, resume }), state));
                }
            }
            if let Some(after) = state.wait.take() {
                n0_future::time::sleep(after).await;
            }
            let from = *state.cursor.lock().unwrap_or_else(PoisonError::into_inner);
            let result = state.peer.call(|client| (state.pull)(client, from)).await;
            let cause = match result {
                Ok(Ok(page)) => {
                    let log = match validate(from, &page) {
                        Ok(log) => log,
                        Err(lapse) => {
                            state.ended = true;
                            return Some((Err(lapse), state));
                        }
                    };
                    state.attempt = 0;
                    if page.events.is_empty() {
                        *state.cursor.lock().unwrap_or_else(PoisonError::into_inner) = page.next;
                    }
                    state.buffer = Some(Buffered {
                        log,
                        events: page.events.into(),
                    });
                    continue;
                }
                Ok(Err(PullRefused::Crowded { parked })) => Retry::Crowded { parked },
                Ok(Err(refused)) => {
                    state.ended = true;
                    return Some((Err(Lapse::Refused(refused)), state));
                }
                Err(error) if retryable(&error) => Retry::Unreachable(error),
                Err(error) => {
                    state.ended = true;
                    return Some((Err(Lapse::Failed(error)), state));
                }
            };
            let after = backoff(state.peer.node().id().as_bytes(), state.attempt);
            state.attempt = state.attempt.saturating_add(1);
            state.wait = Some(after);
            return Some((Err(Lapse::Retrying { cause, after }), state));
        }
    });
    Following {
        stream: Box::pin(stream),
        cursor,
    }
}

struct State<C, E, F> {
    peer: Arc<Peer<C>>,
    pull: F,
    cursor: Arc<Mutex<Cursor>>,
    buffer: Option<Buffered<E>>,
    attempt: u32,
    wait: Option<Duration>,
    ended: bool,
}

struct Buffered<E> {
    log: LogId,
    events: VecDeque<(Seq, E)>,
}

/// A pull-driven stream. It ends after yielding one terminal [`Lapse`].
pub struct Following<E> {
    stream: Pin<Box<dyn Stream<Item = Result<Delivery<E>, Lapse>> + Send>>,
    cursor: Arc<Mutex<Cursor>>,
}

impl<E> Following<E> {
    /// The last yielded event's cursor, never the end of an undelivered page.
    #[must_use]
    pub fn cursor(&self) -> Cursor {
        *self.cursor.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<E> Stream for Following<E> {
    type Item = Result<Delivery<E>, Lapse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

/// One event and the cursor to save after processing it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Delivery<E> {
    /// The deduplication key within this log.
    pub seq: Seq,
    /// The application's event.
    pub event: E,
    /// The resume point immediately after this event.
    pub resume: Cursor,
}

/// A surfaced outage or a terminal reason to stop following.
#[derive(Debug)]
#[non_exhaustive]
pub enum Lapse {
    /// Retryable. The next poll waits before attempting the same cursor again.
    Retrying {
        /// Why this attempt failed.
        cause: Retry,
        /// The deterministic, per-node jittered delay.
        after: Duration,
    },
    /// Terminal. Resync before following from the refusal's head.
    Refused(PullRefused),
    /// Terminal. Retrying cannot repair this application or protocol failure.
    Failed(crate::Error),
    /// Terminal. A page's events or metadata did not continue the cursor.
    Broken {
        /// The sequence expected next.
        expected: Seq,
        /// The sequence received, or the malformed page's next sequence.
        got: Seq,
    },
}

/// Why a follower will retry without moving its cursor.
#[derive(Debug)]
#[non_exhaustive]
pub enum Retry {
    /// Transport loss, clean close, remote timeout or capacity, actor restart,
    /// or mailbox saturation. All retry with backoff, not immediate replay.
    Unreachable(crate::Error),
    /// The log's parked-pull limit was reached.
    Crowded {
        /// How many pulls already waited.
        parked: u64,
    },
}

fn retryable(error: &Error) -> bool {
    matches!(
        error,
        Error::Transport(_)
            | Error::Closed
            | Error::Remote(
                RemoteError::TimedOut { .. }
                    | RemoteError::AtCapacity { .. }
                    | RemoteError::MailboxFull
                    | RemoteError::Withheld(crate::Withheld::Restarting { .. })
            )
    )
}

fn backoff(node: &[u8; 32], attempt: u32) -> Duration {
    let cap = 250_u64.saturating_mul(1_u64 << attempt.min(7)).min(30_000);
    let mut hash = blake3::Hasher::new();
    hash.update(node);
    hash.update(&attempt.to_le_bytes());
    let digest = hash.finalize();
    let fraction = u64::from(u16::from_le_bytes([
        digest.as_bytes()[0],
        digest.as_bytes()[1],
    ]));
    Duration::from_micros(cap * 1000 / 2 + cap * 1000 * fraction / (2 * 65_536))
}

fn broken(from: Cursor, next: Cursor) -> Lapse {
    Lapse::Broken {
        expected: Seq(from.seq().map_or(0, Seq::get).saturating_add(1)),
        got: next.seq().unwrap_or(Seq(0)),
    }
}

fn validate<E>(from: Cursor, page: &Page<E>) -> Result<LogId, Lapse> {
    let Position::After { log, seq: next } = page.next.0 else {
        return Err(broken(from, page.next));
    };
    if let Position::After { log: previous, .. } = from.0 {
        if previous != log {
            return Err(broken(from, page.next));
        }
    }
    let mut after = from.seq().map_or(0, Seq::get);
    for (seq, _) in &page.events {
        if after.checked_add(1) != Some(seq.0) {
            return Err(Lapse::Broken {
                expected: Seq(after.saturating_add(1)),
                got: *seq,
            });
        }
        after = seq.0;
    }
    if next.0 != after || page.head.0 < after || (page.events.is_empty() && page.head.0 != after) {
        return Err(broken(from, page.next));
    }
    Ok(log)
}

#[cfg(test)]
#[path = "follow_tests.rs"]
mod tests;
