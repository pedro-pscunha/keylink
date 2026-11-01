#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use n0_future::future::poll_once;

fn retention(events: usize, bytes: u64) -> Retention {
    Retention::new(
        NonZeroUsize::new(events).unwrap(),
        NonZeroU64::new(bytes).unwrap(),
    )
}

#[tokio::test(start_paused = true)]
async fn a_caught_up_pull_times_out_and_releases_its_slot() {
    let log = EventLog::<u64>::memory(retention(100, 1 << 20));
    let before = tokio::time::Instant::now();
    let page = log.pull(Cursor::ORIGIN).await.unwrap();
    assert!(page.events.is_empty());
    assert_eq!(page.next, log.head());
    assert_eq!(tokio::time::Instant::now() - before, PULL_WAIT);
    assert_eq!(log.inner.ring().parked, 0);
}

#[tokio::test(start_paused = true)]
async fn parked_pulls_are_capped_and_cancellation_releases_a_slot() {
    let log = EventLog::<u64>::memory(retention(100, 1 << 20));
    let mut pulls = Vec::new();
    for _ in 0..MAX_PULLERS {
        let mut pull = Box::pin(log.pull(Cursor::ORIGIN));
        assert!(poll_once(pull.as_mut()).await.is_none());
        pulls.push(pull);
    }
    assert_eq!(
        log.pull(Cursor::ORIGIN).await,
        Err(PullRefused::Crowded {
            parked: MAX_PULLERS as u64
        })
    );
    pulls.pop();
    assert_eq!(log.inner.ring().parked, MAX_PULLERS - 1);
    let mut next = Box::pin(log.pull(Cursor::ORIGIN));
    assert!(poll_once(next.as_mut()).await.is_none());
    drop(next);
    drop(pulls);
    assert_eq!(log.inner.ring().parked, 0);
}

#[tokio::test]
async fn an_append_wakes_every_parked_pull_without_a_lost_notification() {
    let log = EventLog::memory(retention(100, 1 << 20));
    let mut a = Box::pin(log.pull(Cursor::ORIGIN));
    let mut b = Box::pin(log.pull(Cursor::ORIGIN));
    assert!(poll_once(a.as_mut()).await.is_none());
    assert!(poll_once(b.as_mut()).await.is_none());
    log.append(7_u64).await.unwrap();
    assert_eq!(a.await.unwrap().events, [(Seq(1), 7)]);
    assert_eq!(b.await.unwrap().events, [(Seq(1), 7)]);
}

#[tokio::test]
async fn pages_fit_the_wire_and_an_oversized_event_changes_nothing() {
    let log = EventLog::memory(retention(100, 4 << 20));
    for _ in 0..3 {
        log.append(vec![0_u8; MAX_EVENT_BYTES - 10]).await.unwrap();
    }
    let page = log.pull(Cursor::ORIGIN).await.unwrap();
    assert_eq!(
        page.events.len(),
        1,
        "two near-cap events exceed the page budget including seqs"
    );
    assert!(postcard::to_allocvec(&page).unwrap().len() < crate::MAX_HEADER as usize);
    let before = log.head();
    assert!(matches!(
        log.append(vec![0; MAX_EVENT_BYTES + 1]).await,
        Err(AppendError::TooLarge { .. })
    ));
    assert_eq!(log.head(), before);
    let next = log.pull(page.next).await.unwrap();
    assert_eq!(next.events[0].0, Seq(2));
}

#[tokio::test]
async fn age_and_byte_retention_keep_the_newest_event_and_expire_old_cursors() {
    let policy = retention(100, 1).max_age(Duration::from_millis(10));
    let log = EventLog::memory(policy);
    log.append(1000_u64).await.unwrap();
    log.append(2000).await.unwrap();
    assert_eq!(log.inner.ring().events.len(), 1);
    assert!(matches!(
        log.pull(Cursor::ORIGIN).await,
        Err(PullRefused::Expired { .. })
    ));
    let log = EventLog::memory(retention(100, 1 << 20).max_age(Duration::from_millis(10)));
    log.append(1_u64).await.unwrap();
    log.append(2).await.unwrap();
    let mut ring = log.inner.ring();
    for event in &mut ring.events {
        event.at_ms = 100;
    }
    assert_eq!(ring.trim(log.inner.retention, 111), 1);
    assert_eq!(ring.events.front().unwrap().seq, Seq(2));
}

#[tokio::test]
async fn a_cursor_past_the_head_is_explicitly_refused() {
    let log = EventLog::<u64>::memory(retention(100, 1 << 20));
    let ahead = Cursor::after(log.inner.id, Seq(9));
    assert_eq!(
        log.pull(ahead).await,
        Err(PullRefused::Ahead {
            asked: Seq(9),
            head: log.head()
        })
    );
}

#[tokio::test]
async fn a_panicked_staging_batch_is_cleared_before_the_next_turn() {
    let log = EventLog::memory(retention(100, 1 << 20));
    let (stage, mut publish) = log.stage();
    stage.push(1_u64).unwrap();
    publish
        .after_turn(
            &(),
            TurnInfo::for_test(
                TurnOutcome::Panicked { message: "failed" },
                "Post",
                Duration::ZERO,
            ),
        )
        .await
        .unwrap();
    assert_eq!(log.head().seq(), Some(Seq(0)));
    stage.push(2).unwrap();
    publish
        .after_turn(
            &(),
            TurnInfo::for_test(TurnOutcome::Completed, "Post", Duration::ZERO),
        )
        .await
        .unwrap();
    assert_eq!(
        log.pull(Cursor::ORIGIN).await.unwrap().events,
        [(Seq(1), 2)]
    );
}

#[cfg(feature = "persist-file")]
mod durable {
    use super::*;
    use keylink_persist::{file::Files, StateId};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Scratch(std::path::PathBuf);
    impl Scratch {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            Self(std::env::temp_dir().join(format!(
                "keylink-event-batch-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn batch(
        log: &EventLog<u64>,
        values: impl IntoIterator<Item = u64>,
    ) -> Result<(), crate::Stop> {
        let (stage, mut publish) = log.stage();
        for value in values {
            stage.push(value).unwrap();
        }
        publish
            .after_turn(
                &(),
                TurnInfo::for_test(TurnOutcome::Completed, "Batch", Duration::ZERO),
            )
            .await
    }

    #[test]
    fn stored_decoding_refuses_trailing_bytes_instead_of_silently_dropping_them() {
        assert!(matches!(
            crate::events::store::decode::<u64>(Seq(1), &[1, 2]),
            Err(OpenError::Invalid { seq: Seq(1) })
        ));
    }

    #[tokio::test]
    async fn a_durable_turn_is_one_record_and_a_torn_batch_recovers_none_of_its_events() {
        let scratch = Scratch::new();
        let files = Files::open(&scratch.0).await.unwrap();
        let id = StateId::new("events").unwrap();
        let log = EventLog::<u64>::durable(&files, &id, retention(100, 1 << 20))
            .await
            .unwrap();
        batch(&log, [1]).await.unwrap();
        let saved = log.head();
        batch(&log, [2, 3, 4]).await.unwrap();
        let later = log.head();
        drop(log);
        let journal = scratch.0.join("events.journal");
        let metadata = std::fs::metadata(&journal).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(journal)
            .unwrap()
            .set_len(metadata.len() - 1)
            .unwrap();
        assert!(matches!(
            EventLog::<u64>::durable(&files, &id, retention(100, 1 << 20)).await,
            Err(OpenError::Torn { .. })
        ));
        let log = EventLog::<u64>::durable(&files, &id, retention(100, 1 << 20))
            .await
            .unwrap();
        assert_eq!(log.head(), saved);
        assert!(matches!(
            log.pull(later).await,
            Err(PullRefused::Ahead { .. })
        ));
        assert_eq!(
            log.pull(Cursor::ORIGIN).await.unwrap().events,
            [(Seq(1), 1)]
        );
        assert_eq!(log.append(5).await.unwrap(), Seq(2));
    }

    #[tokio::test]
    async fn truncation_keeps_a_record_whose_batch_straddles_retention() {
        let scratch = Scratch::new();
        let files = Files::open(&scratch.0).await.unwrap();
        let id = StateId::new("events").unwrap();
        let log = EventLog::durable(&files, &id, retention(3, 1 << 20))
            .await
            .unwrap();
        batch(&log, 1..=70).await.unwrap();
        let head = log.head();
        drop(log);
        let log = EventLog::<u64>::durable(&files, &id, retention(3, 1 << 20))
            .await
            .unwrap();
        assert_eq!(log.head(), head);
        assert_eq!(
            log.inner
                .ring()
                .events
                .iter()
                .map(|held| held.seq)
                .collect::<Vec<_>>(),
            [Seq(68), Seq(69), Seq(70)]
        );
        log.append(71).await.unwrap();
        assert!(matches!(
            EventLog::<u64>::durable(&files, &id, retention(3, 1 << 20)).await,
            Err(OpenError::Store(keylink_persist::Error::Busy { .. }))
        ));
    }

    #[tokio::test]
    async fn cancelling_the_append_waiter_does_not_cancel_the_durable_writer() {
        struct HeldWrite {
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
            written: Arc<Mutex<Vec<u64>>>,
        }
        impl crate::events::store::Records for HeldWrite {
            fn append<'a>(
                &'a mut self,
                seq: u64,
                _record: &'a [u8],
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = keylink_persist::Result<()>> + Send + 'a>,
            > {
                Box::pin(async move {
                    if seq == 1 {
                        self.entered.notify_one();
                        self.release.notified().await;
                    }
                    self.written.lock().unwrap().push(seq);
                    Ok(())
                })
            }
            fn truncate_before(
                &mut self,
                _seq: u64,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = keylink_persist::Result<()>> + Send + '_>,
            > {
                Box::pin(async { Ok(()) })
            }
        }
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let written = Arc::new(Mutex::new(Vec::new()));
        let mut log = EventLog::<u64>::memory(retention(100, 1 << 20));
        let inner = Arc::get_mut(&mut log.inner).unwrap();
        inner.durable = true;
        inner.writer.get_mut().store = Some(Box::new(HeldWrite {
            entered: entered.clone(),
            release: release.clone(),
            written: written.clone(),
        }));
        let writer = log.clone();
        let caller = tokio::spawn(async move { writer.append(1).await });
        entered.notified().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        release.notify_one();
        // The next writer waits for disk AND ring to agree before assigning seq 2.
        assert_eq!(log.append(2).await.unwrap(), Seq(2));
        assert_eq!(*written.lock().unwrap(), [1, 2]);
        assert_eq!(
            log.pull(Cursor::ORIGIN).await.unwrap().events,
            [(Seq(1), 1), (Seq(2), 2)]
        );
    }

    #[tokio::test]
    async fn a_failed_batch_write_publishes_none_and_reuses_no_partial_sequences() {
        let scratch = Scratch::new();
        let files = Files::open(&scratch.0).await.unwrap().limit(64);
        let id = StateId::new("events").unwrap();
        let log = EventLog::<u64>::durable(&files, &id, retention(100, 1 << 20))
            .await
            .unwrap();
        let before = log.head();
        assert!(batch(&log, 1..=100).await.is_err());
        assert_eq!(log.head(), before);
        assert_eq!(log.append(1).await.unwrap(), Seq(1));
        drop(log);
        let log = EventLog::<u64>::durable(&files, &id, retention(100, 1 << 20))
            .await
            .unwrap();
        assert_eq!(
            log.pull(Cursor::ORIGIN).await.unwrap().events,
            [(Seq(1), 1)]
        );
    }
}
