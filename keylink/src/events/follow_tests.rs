#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use n0_future::{future::poll_once, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};

fn page(values: &[u64]) -> Page<u64> {
    let last = values.last().copied().unwrap_or(0);
    Page {
        events: values.iter().map(|n| (Seq(*n), *n)).collect(),
        next: Cursor::after(LogId([1; 16]), Seq(last)),
        head: Seq(last),
    }
}

#[test]
fn a_page_must_continue_both_the_sequence_and_the_log_identity() {
    assert!(validate(Cursor::ORIGIN, &page(&[1, 2])).is_ok());
    assert!(matches!(
        validate(Cursor::ORIGIN, &page(&[2, 3])),
        Err(Lapse::Broken {
            expected: Seq(1),
            got: Seq(2)
        })
    ));
    assert!(validate(Cursor::ORIGIN, &page(&[1, 3])).is_err());
    assert!(validate(Cursor::after(LogId([2; 16]), Seq(0)), &page(&[1])).is_err());
    let mut bad = page(&[1, 2]);
    bad.next = Cursor::after(LogId([1; 16]), Seq(3));
    assert!(validate(Cursor::ORIGIN, &bad).is_err());
    let mut bad = page(&[1, 2]);
    bad.head = Seq(1);
    assert!(validate(Cursor::ORIGIN, &bad).is_err());
    let mut bad = page(&[]);
    bad.head = Seq(1);
    assert!(validate(Cursor::ORIGIN, &bad).is_err());
}

#[test]
fn jitter_is_deterministic_per_node_and_stays_inside_the_capped_window() {
    for attempt in 0..100 {
        let cap = Duration::from_millis(250_u64.saturating_mul(1 << attempt.min(7)).min(30_000));
        let first = backoff(&[1; 32], attempt);
        assert_eq!(first, backoff(&[1; 32], attempt));
        assert!(first >= cap / 2 && first < cap);
    }
    assert_ne!(backoff(&[1; 32], 0), backoff(&[2; 32], 0));
    assert!(retryable(&Error::Closed));
    assert!(retryable(&Error::Remote(RemoteError::TimedOut {
        millis: 1
    })));
    assert!(retryable(&Error::Remote(RemoteError::AtCapacity {
        capacity: 1
    })));
    assert!(!retryable(&Error::Remote(RemoteError::Withheld(
        crate::Withheld::Gone
    ))));
    assert!(!retryable(&Error::Remote(RemoteError::Unauthorized(
        "Events".into()
    ))));
}

struct Client;
impl Dial for Client {
    async fn dial(_node: &crate::Node, _addr: crate::EndpointAddr) -> crate::Result<Self> {
        Ok(Self)
    }
}

#[tokio::test(start_paused = true)]
async fn retries_wait_without_advancing_and_buffered_events_advance_only_when_yielded() {
    let node = crate::Node::bind_local().await.unwrap();
    let peer = Arc::new(Peer::<Client>::new(&node, node.id()));
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let mut following = follow(peer, Cursor::ORIGIN, move |_client, from| {
        let call = counted.fetch_add(1, Ordering::SeqCst);
        async move {
            assert_eq!(
                from,
                Cursor::ORIGIN,
                "no pull may advance before yielding events"
            );
            match call {
                0 => Err(Error::Remote(RemoteError::TimedOut { millis: 1 })),
                1 => Ok(Err(PullRefused::Crowded { parked: 1024 })),
                _ => Ok(Ok(page(&[1, 2, 3]))),
            }
        }
    });
    let first = following.next().await.unwrap();
    let Err(Lapse::Retrying {
        after: first_wait, ..
    }) = first
    else {
        panic!("expected retry")
    };
    assert_eq!(following.cursor(), Cursor::ORIGIN);
    let mut pending = Box::pin(following.next());
    assert!(poll_once(pending.as_mut()).await.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(first_wait).await;
    let second = pending.await.unwrap();
    let Err(Lapse::Retrying {
        after: second_wait, ..
    }) = second
    else {
        panic!("expected crowded retry")
    };
    assert!(second_wait >= Duration::from_millis(250));
    let event = following.next().await.unwrap().unwrap();
    assert_eq!(event.seq, Seq(1));
    assert_eq!(following.cursor(), event.resume);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let event = following.next().await.unwrap().unwrap();
    assert_eq!(event.seq, Seq(2));
    assert_eq!(following.cursor(), event.resume);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "drain the page before pulling again"
    );
}

// Finding 3 was watched red for both callers below: each temporary condition
// yielded terminal Failed instead of the required Retrying lapse.
async fn temporary_actor_condition_retries(error: Error) {
    let node = crate::Node::bind_local().await.unwrap();
    let peer = Arc::new(Peer::<Client>::new(&node, node.id()));
    let saved = Cursor::after(LogId([1; 16]), Seq(1));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let mut transient = Some(error);
    let mut events = follow(peer, saved, move |_client, from| {
        observed.lock().unwrap().push(from);
        let error = transient.take();
        async move {
            match error {
                Some(error) => Err(error),
                None => Ok(Ok(page(&[2, 3]))),
            }
        }
    });
    let first = events.next().await.unwrap();
    let Err(Lapse::Retrying { after, .. }) = first else {
        panic!("a temporary actor condition must back off and retry: {first:?}")
    };
    assert_eq!(after, backoff(node.id().as_bytes(), 0));
    assert_eq!(events.cursor(), saved);
    let mut pending = Box::pin(events.next());
    assert!(poll_once(pending.as_mut()).await.is_none());
    assert_eq!(*calls.lock().unwrap(), [saved]);
    tokio::time::advance(after).await;
    let event = pending.await.unwrap().unwrap();
    assert_eq!((event.seq, event.event), (Seq(2), 2));
    assert_eq!(events.cursor(), event.resume);
    assert_eq!(*calls.lock().unwrap(), [saved, saved]);
    assert_eq!(events.next().await.unwrap().unwrap().seq, Seq(3));
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn restarting_retries_without_advancing_the_cursor() {
    temporary_actor_condition_retries(Error::Remote(RemoteError::Withheld(
        crate::Withheld::Restarting {
            reason: crate::EndReason::Panicked {
                message: "the next incarnation is starting".into(),
            },
        },
    )))
    .await;
}

#[tokio::test(start_paused = true)]
async fn mailbox_full_retries_without_advancing_the_cursor() {
    temporary_actor_condition_retries(Error::Remote(RemoteError::MailboxFull)).await;
}

#[tokio::test]
async fn a_broken_page_and_a_refusal_each_end_the_stream_once() {
    let node = crate::Node::bind_local().await.unwrap();
    let peer = Arc::new(Peer::<Client>::new(&node, node.id()));
    let mut events = follow(peer.clone(), Cursor::ORIGIN, |_client, _from| async {
        Ok(Ok(page(&[2])))
    });
    assert!(matches!(
        events.next().await,
        Some(Err(Lapse::Broken { .. }))
    ));
    assert!(events.next().await.is_none());
    let mut events = follow(peer, Cursor::ORIGIN, |_client, _from| async {
        Ok::<Pull<u64>, _>(Err(PullRefused::Unknown))
    });
    assert!(matches!(
        events.next().await,
        Some(Err(Lapse::Refused(PullRefused::Unknown)))
    ));
    assert!(events.next().await.is_none());
}
