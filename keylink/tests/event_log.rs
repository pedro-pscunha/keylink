//! Real long-poll RPCs, recovery, retention and actor publication.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use keylink::events::{
    follow, Cursor, Delivery, EventLog, Following, Lapse, Pull, PullRefused, Retention, Stage,
};
use keylink::prelude::*;
use keylink::{
    Admission, Admittance, AdmittedPeer, Allowlist, Context, Peer, ProvenPeer, Revocation, Server,
};
use n0_future::StreamExt;
use tokio::sync::mpsc;

mod common;
use common::{local_node, NO_PROGRESS};

keylink::service! {
    service History;
    rpc Events(from: Cursor) -> Pull<u64>;
}

struct History {
    log: EventLog<u64>,
    entered: mpsc::UnboundedSender<()>,
}

impl history::Handler for History {
    async fn events(&self, _cx: &Context, from: Cursor) -> Pull<u64> {
        let _ = self.entered.send(());
        self.log.pull(from).await
    }
}

#[derive(Default)]
struct Gate {
    handshakes: AtomicUsize,
    live: Mutex<Option<Revocation>>,
}

impl Gate {
    fn close(&self) {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .expect("an admitted connection")
            .close();
    }
}

impl Admission for Gate {
    fn admits(&self, _peer: &ProvenPeer) -> Admittance {
        self.handshakes.fetch_add(1, Ordering::SeqCst);
        Admittance::Admitted
    }

    fn admitted(&self, _peer: &AdmittedPeer, live: Revocation) {
        *self.live.lock().unwrap_or_else(PoisonError::into_inner) = Some(live);
    }
}

fn retention(events: usize) -> Retention {
    Retention::new(
        NonZeroUsize::new(events).unwrap(),
        NonZeroU64::new(64 << 20).unwrap(),
    )
}

async fn serving(
    log: EventLog<u64>,
) -> (
    keylink::Node,
    Server,
    Arc<Peer<history::Client>>,
    Arc<Gate>,
    mpsc::UnboundedReceiver<()>,
) {
    let server_node = local_node().await;
    let client_node = local_node().await;
    let gate = Arc::new(Gate::default());
    let (entered, arrivals) = mpsc::unbounded_channel();
    let server = Server::builder(&server_node)
        .service(
            history::service(History { log, entered }, Allowlist::anyone())
                .admission(gate.clone())
                .build(),
        )
        .spawn()
        .unwrap();
    let peer = Arc::new(Peer::new(&client_node, server_node.endpoint().addr()));
    (server_node, server, peer, gate, arrivals)
}

fn following(peer: Arc<Peer<history::Client>>, from: Cursor) -> Following<u64> {
    follow(peer, from, |client, from| async move {
        client.events(from).await
    })
}

async fn delivery(events: &mut Following<u64>) -> Delivery<u64> {
    tokio::time::timeout(NO_PROGRESS, async {
        loop {
            match events.next().await.expect("the stream is live") {
                Ok(event) => return event,
                Err(Lapse::Retrying { .. }) => {}
                Err(error) => panic!("unexpected terminal lapse: {error:?}"),
            }
        }
    })
    .await
    .expect("the follower makes progress")
}

#[tokio::test]
async fn a_follower_resumes_across_a_dropped_connection_with_no_gap_and_no_duplicate() {
    let log = EventLog::memory(retention(100));
    for value in 1..=10 {
        log.append(value).await.unwrap();
    }
    let (_node, _server, peer, gate, mut arrivals) = serving(log.clone()).await;
    let mut events = following(peer.clone(), Cursor::ORIGIN);
    let mut seen = Vec::new();
    for _ in 1..=10 {
        let event = delivery(&mut events).await;
        seen.push((event.seq.get(), event.event));
    }
    // The next pull actually enters before the serving side kills its connection.
    arrivals.recv().await.unwrap();
    let mut pending = Box::pin(events.next());
    tokio::select! {
        result = &mut pending => panic!("caught-up pull answered early: {result:?}"),
        entered = arrivals.recv() => { entered.unwrap(); }
    }
    let departed = peer.connect().await.unwrap().watch();
    gate.close();
    departed.await;
    for value in 11..=20 {
        log.append(value).await.unwrap();
    }
    drop(pending);
    let mut saved = None;
    for _ in 11..=20 {
        let event = delivery(&mut events).await;
        if event.event == 15 {
            saved = Some(event.resume);
        }
        seen.push((event.seq.get(), event.event));
    }
    assert_eq!(seen, (1..=20).map(|n| (n, n)).collect::<Vec<_>>());
    assert!(gate.handshakes.load(Ordering::SeqCst) >= 2);
    // Drop an undrained page at 15, not merely a stream that already yielded 20.
    drop(events);
    let mut events = following(peer.clone(), Cursor::ORIGIN);
    for expected in 1..=15 {
        let event = delivery(&mut events).await;
        assert_eq!((event.seq.get(), event.event), (expected, expected));
        assert_eq!(events.cursor(), event.resume);
        if expected == 15 {
            assert_eq!(Some(event.resume), saved);
        }
    }
    drop(events);
    let mut events = following(peer, saved.unwrap());
    for expected in 16..=20 {
        let event = delivery(&mut events).await;
        assert_eq!((event.seq.get(), event.event), (expected, expected));
    }
}

#[cfg(feature = "persist-file")]
#[tokio::test]
async fn a_follower_offline_while_1000_events_were_appended_receives_all_1000() {
    let scratch = Scratch::new("offline");
    let files = keylink_persist::file::Files::open(&scratch.0)
        .await
        .unwrap();
    let id = keylink_persist::StateId::new("events").unwrap();
    let log = EventLog::durable(&files, &id, retention(2000))
        .await
        .unwrap();
    log.append(1).await.unwrap();
    let (node, server, peer, _gate, _arrivals) = serving(log.clone()).await;
    let mut events = following(peer.clone(), Cursor::ORIGIN);
    let saved = delivery(&mut events).await.resume;
    drop(events);
    for value in 2..=1001 {
        log.append(value).await.unwrap();
    }
    let before = log.head();
    drop(peer);
    server.shutdown().await.unwrap();
    node.endpoint().close().await;
    drop(log);
    let log = EventLog::durable(&files, &id, retention(2000))
        .await
        .unwrap();
    assert_eq!(log.head(), before, "identity and head survive reopen");
    let (_node, _server, peer, _gate, _arrivals) = serving(log).await;
    let mut events = following(peer, saved);
    for expected in 2..=1001 {
        let event = delivery(&mut events).await;
        assert_eq!((event.seq.get(), event.event), (expected, expected));
    }
}

#[tokio::test]
async fn a_cursor_older_than_retention_is_told_to_resync() {
    let log = EventLog::memory(retention(10));
    log.append(1).await.unwrap();
    let saved = log.head();
    for value in 2..=31 {
        log.append(value).await.unwrap();
    }
    let (_node, _server, peer, _gate, _arrivals) = serving(log.clone()).await;
    let mut events = following(peer, saved);
    match events.next().await.unwrap() {
        Err(Lapse::Refused(PullRefused::Expired {
            asked,
            oldest,
            head,
        })) => {
            assert_eq!(asked.get(), 1);
            assert_eq!(oldest.get(), 22);
            assert_eq!(head, log.head());
        }
        other => panic!("expected an explicit resync, got {other:?}"),
    }
    assert!(events.next().await.is_none());
    assert!(matches!(
        log.pull(Cursor::ORIGIN).await,
        Err(PullRefused::Expired { .. })
    ));
    let replacement = EventLog::<u64>::memory(retention(10));
    assert!(matches!(
        replacement.pull(saved).await,
        Err(PullRefused::Replaced { .. })
    ));
}

/// Watched break: replace `let mut writer = self.inner.writer.lock().await;`
/// in events/log.rs with `if self.inner.ring().parked > 0 { std::future::pending::<()>().await; } let mut writer = self.inner.writer.lock().await;`.
/// The producer's NO_PROGRESS watchdog fails with `a stalled follower cannot hold the producer`.
#[tokio::test]
async fn a_follower_that_stops_reading_does_not_stall_the_producer() {
    let log = EventLog::memory(retention(100));
    log.append(1).await.unwrap();
    let (_node, _server, peer, _gate, mut arrivals) = serving(log.clone()).await;
    let mut events = following(peer, Cursor::ORIGIN);
    assert_eq!(delivery(&mut events).await.event, 1);
    arrivals.recv().await.unwrap();
    let mut pending = Box::pin(events.next());
    tokio::select! {
        result = &mut pending => panic!("caught-up pull answered early: {result:?}"),
        entered = arrivals.recv() => { entered.unwrap(); }
    }
    tokio::time::timeout(NO_PROGRESS, async {
        for value in 2..=10_001 {
            log.append(value).await.unwrap();
        }
    })
    .await
    .expect("a stalled follower cannot hold the producer");
    assert_eq!(log.head().seq().unwrap().get(), 10_001);
    // An already answered page may still be in flight. It must be contiguous,
    // and then retention must refuse the next pull instead of skipping ahead.
    let mut last = 1;
    let first = pending.await.unwrap();
    let mut next = Some(first);
    loop {
        match next
            .take()
            .unwrap_or_else(|| panic!("an item was expected"))
        {
            Ok(event) => {
                assert_eq!(event.seq.get(), last + 1);
                last += 1;
            }
            Err(Lapse::Refused(PullRefused::Expired { .. })) => break,
            other => panic!("unexpected item: {other:?}"),
        }
        next = events.next().await;
    }
    assert!(events.next().await.is_none());
}

keylink::service! {
    service Publisher;
    rpc Post(value: u64) -> ();
    #[defer]
    rpc Events(from: Cursor) -> Pull<u64>;
}

struct Publisher {
    stage: Stage<u64>,
    log: EventLog<u64>,
}
impl publisher::Actor for Publisher {
    async fn post(&mut self, _cx: keylink::Caller<'_>, value: u64) {
        self.stage.push(value).unwrap();
        assert_ne!(value, 2, "this turn must not publish");
    }

    async fn events(
        &mut self,
        _cx: keylink::Caller<'_>,
        reply: keylink::Reply<Pull<u64>>,
        from: Cursor,
    ) -> keylink::Replied<Pull<u64>> {
        self.log.answer(reply, from)
    }
}

#[tokio::test]
async fn a_turn_that_panics_publishes_nothing() {
    let log = EventLog::memory(retention(100));
    let (stage, publish) = log.stage();
    let actor = publisher::setup(Publisher {
        stage,
        log: log.clone(),
    })
    .observe(publish)
    .actor();
    let server_node = local_node().await;
    let client_node = local_node().await;
    let _server = Server::builder(&server_node)
        .service(actor.service(Allowlist::anyone()).build())
        .spawn()
        .unwrap();
    let client = publisher::Client::connect(&client_node, server_node.endpoint().addr())
        .await
        .unwrap();
    client.post(1).await.unwrap();
    let failure = client.post(2).await.unwrap_err();
    assert!(matches!(
        failure,
        keylink::Error::Remote(keylink::RemoteError::Withheld(
            keylink::Withheld::Panicked { .. }
        ))
    ));
    client.post(3).await.unwrap();
    let page = client.events(Cursor::ORIGIN).await.unwrap().unwrap();
    assert_eq!(
        page.events
            .iter()
            .map(|(seq, event)| (seq.get(), *event))
            .collect::<Vec<_>>(),
        [(1, 1), (2, 3)]
    );
    // answer releases a caught-up actor turn: Post still runs and wakes Events.
    let pending = client.events(page.next);
    tokio::pin!(pending);
    let post = client.post(4);
    let (page, posted) = tokio::join!(pending, post);
    posted.unwrap();
    assert_eq!(page.unwrap().unwrap().events[0].1, 4);
}

#[cfg(feature = "persist-file")]
struct Scratch(std::path::PathBuf);
#[cfg(feature = "persist-file")]
impl Scratch {
    fn new(name: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self(std::env::temp_dir().join(format!(
            "keylink-events-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
#[cfg(feature = "persist-file")]
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
