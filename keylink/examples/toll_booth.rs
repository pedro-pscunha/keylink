//! Layers around a call, and the two budgets that answer in different words and
//! mean opposite things.
//!
//! ```sh
//! cargo run -p keylink --example toll_booth
//! ```
//!
//! Six nodes on the loopback interface. `Node::bind_local` binds with no relay
//! and no discovery, so this runs offline.
//!
//! # A layer is written once for every service you will ever declare
//!
//! [`Middleware`] is generic over the request type, and every generated
//! `Request` implements [`Rpc`]. So a layer that names methods asks
//! [`Rpc::method`] rather than matching a service's own enum, and `Tally` below
//! compiles against both services here without knowing either.
//!
//! # A deadline and `QueueTimeout` are not two spellings of one bound
//!
//! They answer in different words, [`RemoteError::DeadlinePassed`] and
//! [`RemoteError::TimedOut`], because they say opposite things about the work.
//! What differs is what the call left behind.
//!
//! | bound | says | the key it carried |
//! |---|---|---|
//! | [`deadline`](keylink::ServiceBuilder::deadline) | the service stopped waiting, and the work was not cancelled | stays running and settles as what the call really returned, so a retry is handed that answer |
//! | [`QueueTimeout`] | a message never started, and nothing ran | stays claimable, so the retry is a first attempt |
//!
//! **Act 2 measures the top row and act 3 the bottom one.** An unkeyed call on a
//! `Handler` has nothing to settle, so its future is dropped where it stands. A
//! keyed one is awaited to the end, so the retry gets the real answer and the
//! work runs once. On an *actor* a deadline cancels no turn either way: the turn
//! runs to completion whatever the caller does.
//!
//! # A limit is a decision, and `Forgotten` is what it produces
//!
//! Act 4 sets [`Limits`] to one record. The second settled key evicts the first,
//! and the first then answers
//! [`Forgotten`](keylink::IdempotencyRefusal::Forgotten) — the refusal that
//! means *it ran, and the reply is gone*. At-most-once still holds. What is lost
//! is the answer.

use std::error::Error as StdError;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use keylink::prelude::*;
use keylink::{
    Allowlist, Body, BodyReader, Caller, Context, Error, IdempotencyKey, IdempotencyRefusal,
    KeyState, Limits, Middleware, Next, Node, Outcome, QueueTimeout, QueueTimeoutError,
    RemoteError, Rpc, Server,
};
use tokio::sync::mpsc;

keylink::service! {
    service Booth;

    /// Sleeps for `millis` inside the handler, then counts itself finished.
    rpc Stall(millis: u64) -> u64;

    /// How many stalls ran all the way to the end.
    rpc Finished() -> u32;

    /// Never reached, because the layer below refuses it.
    rpc Ledger() -> u32;
}

keylink::service! {
    service Yard;

    /// Holds the actor's one turn for `millis`.
    rpc Park(millis: u64) -> u64;

    /// How many parks completed, so a caller can tell a refusal from a run.
    rpc Parked() -> u32;
}

#[derive(Default)]
struct Booth {
    finished: AtomicU32,
}

impl booth::Handler for Booth {
    async fn stall(&self, _cx: &Context, millis: u64) -> u64 {
        n0_future::time::sleep(Duration::from_millis(millis)).await;
        self.finished.fetch_add(1, Ordering::SeqCst);
        millis
    }

    async fn finished(&self, _cx: &Context) -> u32 {
        self.finished.load(Ordering::SeqCst)
    }

    async fn ledger(&self, _cx: &Context) -> u32 {
        unreachable!("the layer refuses this method before the handler is reached")
    }
}

struct Yard {
    parked: u32,
    /// Fires from inside the turn, before the sleep.
    ///
    /// This is what replaces a sleep in `main`. The signal is sent by the turn
    /// itself, so receiving it proves the mailbox is occupied — and act 3's
    /// whole subject is a message that queued behind an occupied one.
    started: mpsc::UnboundedSender<()>,
}

impl yard::Actor for Yard {
    async fn park(&mut self, _cx: Caller<'_>, millis: u64) -> u64 {
        let _ = self.started.send(());
        n0_future::time::sleep(Duration::from_millis(millis)).await;
        self.parked += 1;
        millis
    }

    async fn parked(&mut self, _cx: Caller<'_>) -> u32 {
        self.parked
    }
}

// ──────────────────────────────────────────────────────────────────── the layer

/// Counts every call by method name, and refuses one method outright.
///
/// Generic over `Req: Rpc`, so it is installed on two unrelated services below
/// with no per-service code. That genericity is the reason a metrics or audit
/// layer is written once rather than per service.
///
/// # It does not touch the progress marker
///
/// [`Context`] carries the marker the idempotency ledger reads to decide whether
/// the work was entered. A layer that marks it and then refuses records the
/// refusal as the call's answer and replays it to every retry. Refusing without
/// marking, as below, leaves the key claimable.
///
/// # `Clone` and not `Arc`, because that is what the builder takes
///
/// [`ServiceBuilder::layer`](keylink::ServiceBuilder) takes the layer by value,
/// so a layer shared between two services shares its state itself. One `Arc`
/// inside beats an `Arc` around, which would need a second impl to satisfy the
/// bound.
#[derive(Clone)]
struct Tally {
    seen: Arc<Mutex<Vec<&'static str>>>,
    refuse: &'static str,
}

impl Tally {
    fn new(refuse: &'static str) -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            refuse,
        }
    }

    fn counted(&self, method: &'static str) -> usize {
        self.seen
            .lock()
            .map(|seen| seen.iter().filter(|name| **name == method).count())
            .unwrap_or_default()
    }
}

impl<Req: Rpc + Send + 'static, Resp: Send + 'static> Middleware<Req, Resp> for Tally {
    fn handle(
        &self,
        cx: Context,
        req: Req,
        body: BodyReader,
        next: Next<Req, Resp>,
    ) -> Pin<Box<dyn Future<Output = (Outcome<Resp>, Body)> + Send>> {
        let method = req.method();
        if let Ok(mut seen) = self.seen.lock() {
            seen.push(method);
        }
        if method == self.refuse {
            // Refusing here means `next` is never called, so the handler is
            // never reached. `unreachable!` in `Booth::ledger` is what asserts
            // that rather than assuming it.
            return Box::pin(async move {
                (
                    Outcome::Err(RemoteError::Unauthorized(method.to_owned())),
                    Body::empty(),
                )
            });
        }
        Box::pin(async move { next(cx, req, body).await })
    }
}

/// One non-zero bound, without an `unwrap` this workspace denies.
fn one() -> Result<NonZeroUsize, Box<dyn StdError>> {
    NonZeroUsize::new(1).ok_or_else(|| "one is not zero".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    // ── 1. A layer that observes, and a layer that refuses.
    //
    //  Both on a `Handler`, where dropping the future of an unkeyed call genuinely
    //  cancels the work.
    let booth_node = Node::bind_local().await?;
    let caller_node = Node::bind_local().await?;
    let tally = Tally::new("Ledger");
    let _booth = Server::builder(&booth_node)
        .service(
            booth::service(Booth::default(), Allowlist::anyone())
                .layer(tally.clone())
                .deadline(Duration::from_millis(150))
                .build(),
        )
        .spawn()?;
    let caller = booth::Client::connect(&caller_node, booth_node.endpoint().addr()).await?;

    let refused = caller
        .ledger()
        .await
        .err()
        .ok_or("the layer must refuse this method")?;
    println!("the layer refused a method before the handler: {refused}");
    assert!(
        matches!(refused, Error::Remote(RemoteError::Unauthorized(_))),
        "got {refused:?}"
    );

    // ── 2. A deadline on a handler: an unkeyed call is cancelled, a keyed one is not.
    //
    //  The handler sleeps 600 ms and the budget is 150 ms, so the caller is told
    //  at 150 ms and the future is dropped where it stands: the counter it would
    //  have incremented never moves. That is the measurement; the refusal alone
    //  would not show it.
    let timed_out = caller
        .stall(600)
        .await
        .err()
        .ok_or("a handler over its budget must be refused")?;
    assert!(
        matches!(
            timed_out,
            Error::Remote(RemoteError::DeadlinePassed { millis: 150 })
        ),
        "got {timed_out:?}"
    );
    n0_future::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        caller.finished().await?,
        0,
        "the dropped future must not have finished"
    );
    println!("the handler was cancelled at its budget and never finished: {timed_out}");
    assert_eq!(tally.counted("Stall"), 1, "the layer saw the call it timed");

    //  The same overrun under a key. The caller is told at 150 ms all the same,
    //  but the call is not dropped: the service goes on waiting for the handler,
    //  and the key settles as what it really returned. The retry attaches to the
    //  call still running and is handed that answer, and the handler ran once.
    //  The retry sends the same request, as a retry must: a key is bound to the
    //  bytes of the call it first met, and a different argument under it is
    //  refused as a mismatch.
    let key = IdempotencyKey::new("stall-1")?;
    let passed = caller
        .with_idempotency(key.clone())
        .stall(300)
        .await
        .err()
        .ok_or("the deadline passes while the keyed handler runs")?;
    assert!(
        matches!(
            passed,
            Error::Remote(RemoteError::DeadlinePassed { millis: 150 })
        ),
        "got {passed:?}"
    );
    let answer = caller
        .with_idempotency(keylink::Idempotency::attaching_for(
            key,
            Duration::from_secs(10),
        ))
        .stall(300)
        .await?;
    assert_eq!(
        answer, 300,
        "the retry is handed the first call's real answer"
    );
    assert_eq!(
        caller.finished().await?,
        1,
        "and the handler ran to the end exactly once"
    );
    println!(
        "a keyed call past its deadline finished anyway, and its retry got the answer: {answer}"
    );

    // ── 3. `QueueTimeout` on an actor, and the key it leaves behind.
    //
    //  One turn is held for 600 ms. A second message queues behind it with a
    //  150 ms budget and is refused BEFORE it starts — so nothing ran, nothing
    //  is owed, and the key it carried is claimable again.
    //  `try_after` and not `after`, because a budget is not always representable:
    //  the deadline is `Instant::now() + budget`, and a budget past the clock's
    //  range has no deadline to compute. It is [`QueueTimeoutError`] at
    //  construction rather than a panic in a dispatch task months later, or —
    //  worse — a silently clamped budget that refuses everything or nothing.
    let unrepresentable = QueueTimeout::try_after(Duration::MAX)
        .err()
        .ok_or("a budget past the clock's range has no deadline")?;
    assert!(matches!(unrepresentable, QueueTimeoutError));
    println!("a budget the clock cannot represent is refused up front");

    let yard_node = Node::bind_local().await?;
    let desk_node = Node::bind_local().await?;
    let (started_tx, mut started) = mpsc::unbounded_channel();
    let yard = yard::actor(Yard {
        parked: 0,
        started: started_tx,
    });
    let _serving = Server::builder(&yard_node)
        .service(
            yard.service(Allowlist::anyone())
                .layer(tally.clone())
                .layer(QueueTimeout::try_after(Duration::from_millis(150))?)
                .build(),
        )
        .spawn()?;
    let desk = yard::Client::connect(&desk_node, yard_node.endpoint().addr()).await?;

    let holding = {
        let desk = desk.clone();
        tokio::spawn(async move { desk.park(600).await })
    };
    //  Wait for the holding turn to have STARTED, which the turn itself reports.
    //  A sleep here would be a guess, and a `parked()` probe would be worse than
    //  a guess: `parked` is an `rpc`, so it is a turn, and a turn that queued
    //  behind `park(600)` answers 1 after 600 ms rather than 0.
    started
        .recv()
        .await
        .ok_or("the holding turn never started")?;

    let queued = IdempotencyKey::new("slot-118")?;
    let never_started = desk
        .with_idempotency(queued.clone())
        .park(10)
        .await
        .err()
        .ok_or("a message still queued at its budget must be refused")?;
    assert!(
        matches!(never_started, Error::Remote(RemoteError::TimedOut { .. })),
        "got {never_started:?}"
    );
    println!("a queued message was refused before it started: {never_started}");

    //  The key is the difference. Nothing ran, so the ledger holds nothing, and
    //  the retry below is a first attempt rather than a permanent refusal.
    assert_eq!(
        desk.key_state(queued.clone()).await?,
        KeyState::Fresh,
        "a message that never started must not settle its key"
    );
    holding.await??;
    let retried = desk.with_idempotency(queued).park(10).await?;
    assert_eq!(retried, 10);
    println!("the same key ran on the retry, because nothing had happened under it");
    assert_eq!(desk.parked().await?, 2, "both parks that started finished");

    // ── 4. A limit, and the refusal it produces.
    //
    //  One record in the pool. Settling a second key evicts the first, and the
    //  first then answers `Forgotten`: it ran, at-most-once still holds, and
    //  what is gone is the reply. A caller reads the result out of its own
    //  domain rather than asking again.
    let small_node = Node::bind_local().await?;
    let clerk_node = Node::bind_local().await?;
    let (unwatched, _idle) = mpsc::unbounded_channel();
    let small = yard::actor(Yard {
        parked: 0,
        started: unwatched,
    });
    let _small = Server::builder(&small_node)
        .service(
            small
                .service(Allowlist::anyone())
                .limits(Limits::default().pool_records(one()?))
                .build(),
        )
        .spawn()?;
    let clerk = yard::Client::connect(&clerk_node, small_node.endpoint().addr()).await?;

    let first = IdempotencyKey::new("bay-1")?;
    let second = IdempotencyKey::new("bay-2")?;
    clerk.with_idempotency(first.clone()).park(0).await?;
    assert_eq!(clerk.key_state(first.clone()).await?, KeyState::Done);
    clerk.with_idempotency(second).park(0).await?;

    let evicted = clerk.key_state(first.clone()).await?;
    println!("with one record slot, the first key now reads {evicted:?}");
    let gone = clerk
        .with_idempotency(first)
        .park(0)
        .await
        .err()
        .ok_or("an evicted key must be refused, not re-run")?;
    assert!(
        matches!(
            gone,
            Error::Remote(RemoteError::Idempotency(IdempotencyRefusal::Forgotten))
        ),
        "got {gone:?}"
    );
    assert_eq!(
        clerk.parked().await?,
        2,
        "the evicted key must not have run a second time"
    );
    println!("and its retry was refused rather than re-run: {gone}");

    yard.stop().await?;
    small.stop().await?;
    Ok(())
}
