//! Supervision: an outer loop on the actor's own task.
//!
//! Wave 9, phases 1 and 2. The structural claim is that a supervisor is **not** a
//! second task spawning children: `spawn_supervised` wraps the loop that already
//! exists, and the mailbox is owned by the outer loop and lent to the inner one.
//! Four properties fall out of that with no mechanism, and three of them are
//! asserted here — `abort()` destroys the actor rather than one incarnation, a
//! restart cannot resolve `ended()`, and a `Handle` reaches the next incarnation.
//! The fourth, `is_running()` being true during a restart, is what makes the
//! `Handle` test above work at all.
//!
//! # Why this could not be built before Wave 5
//!
//! **A restart is worth exactly the distance between the state at fault and the
//! state the next incarnation starts from, and that distance is set by the commit
//! point.** With no durable observer a supervisor restarts from a factory into
//! empty state, which is a panic guard plus data loss. So the two tests that
//! carry the word *committed* drive the real `Persist` adapter against the real
//! file store, and the measured price of that commit point is 7.6 ms per turn.
//!
//! # No wall-clock assertion anywhere
//!
//! Every timing claim runs under `#[tokio::test(start_paused = true)]`, so
//! `tokio::time::Instant` moves only when a timer the code under test set is the
//! earliest thing the runtime has to do. The two tests that use real time —
//! the two durable ones and `abort` — assert nothing about it.
//!
//! `tokio::time::advance` plus `yield_now` appears nowhere on purpose: it leaves
//! the test task runnable, so the runtime never parks, the timer wheel is never
//! turned, and a spawned task's `sleep` never fires. Every wait here is a real
//! park — an `await` on the actor's own channel, or a `timeout` — which is what
//! makes the paused clock auto-advance to the supervisor's backoff.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use keylink::{
    Allowlist, Caller, Deferred, Ended, Intensity, Observer, Persist, RemoteError, Replied, Reply,
    Server, Stop, Stream, TurnFut, TurnInfo, TurnOutcome,
};
use keylink_persist::file::Files;
use keylink_persist::{Durable, RestoreError, StateId};

mod common;
use common::local_node;

/// A liveness bound, never a measurement. Under a paused clock this costs no
/// real time at all: the runtime advances to it only when nothing else can run,
/// which is exactly the hang it exists to name.
const NO_PROGRESS: Duration = Duration::from_secs(60);

// ─────────────────────────────────────────────────────────────────── services

keylink::service! {
    service Ledger;

    rpc Post(entry: u64) -> u64;
    rpc Entries() -> Vec<u64>;
    /// Applies the entry and **then** panics, so a half-applied turn is
    /// reachable and "rolled back to the last commit" is a claim with something
    /// behind it.
    ///
    /// A `tell` and not an `rpc`, because a local `rpc` resumes the handler's
    /// panic in the caller — which would be this test — and a `tell` drops its
    /// reply receiver before sending, so the panic is caught and dropped. The
    /// actor still sees it, which is all these tests need.
    tell Bad(entry: u64) -> ();
    /// The same fault, reachable from a **peer**.
    ///
    /// A second declaration and not a second caller of the first, because a
    /// keyed `tell` has no client method at all — `__keylink_actor_client_method`
    /// refuses one, since four keyed failures are reported on a reply a `tell`
    /// does not have. A peer's `rpc` gets the fault as `Failed("panicked: ..")`
    /// rather than as an unwind, which is exactly what this test wants.
    rpc Ruin(entry: u64) -> u64;
}

keylink::service! {
    service Ticker;

    rpc Beats() -> u64;
    /// The input a supervisor cannot re-open by itself. A feed that ends stops
    /// the actor by design, so a factory handing out a fresh stream per
    /// incarnation is the only thing that can re-attempt one.
    feed Beat(beat: u64);
}

keylink::service! {
    service Desk;

    /// Parked in the state and never answered, which is the one message a
    /// restart genuinely loses.
    #[defer]
    rpc Place(order: u64) -> u64;
    /// How many orders this incarnation has parked, so a test can wait for a
    /// park to have **happened** rather than assume it raced ahead of the fault.
    rpc Parked() -> u64;
    /// Panics. Over the wire that is `Failed("panicked: ..")`, and it is here so
    /// that the two prefixes can be shown side by side.
    rpc Boom() -> u64;
}

#[derive(Default)]
struct Ledger {
    entries: Vec<u64>,
}

impl ledger::Actor for Ledger {
    async fn post(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        self.entries.push(entry);
        self.entries.len() as u64
    }

    async fn entries(&mut self, _cx: Caller<'_>) -> Vec<u64> {
        self.entries.clone()
    }

    async fn bad(&mut self, _cx: Caller<'_>, entry: u64) {
        self.entries.push(entry);
        panic!("bad entry");
    }

    async fn ruin(&mut self, _cx: Caller<'_>, entry: u64) -> u64 {
        self.entries.push(entry);
        panic!("bad entry");
    }
}

impl Durable for Ledger {
    const VERSION: u32 = 1;

    fn snapshot(&self) -> Vec<u8> {
        self.entries.iter().flat_map(|n| n.to_le_bytes()).collect()
    }

    fn restore(bytes: &[u8]) -> Result<Self, RestoreError> {
        if !bytes.len().is_multiple_of(8) {
            return Err(RestoreError::Malformed(format!(
                "a ledger is whole u64s, and this is {} bytes",
                bytes.len()
            )));
        }
        Ok(Self {
            entries: bytes
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8)")))
                .collect(),
        })
    }

    fn migrate(_from: u32, _bytes: &[u8]) -> Result<Option<Vec<u8>>, RestoreError> {
        Ok(None)
    }
}

/// Records which beat each incarnation saw, so "the feed was re-subscribed" is a
/// list rather than a count.
struct Ticker {
    seen: Arc<Mutex<Vec<u64>>>,
}

impl ticker::Actor for Ticker {
    async fn beats(&mut self, _cx: Caller<'_>) -> u64 {
        self.seen.lock().expect("not poisoned").len() as u64
    }

    async fn on_beat(&mut self, beat: u64) {
        self.seen.lock().expect("not poisoned").push(beat);
    }
}

/// Parks every order and answers none of them. The park is the point: a
/// `Deferred` lives in the state, so it dies when the incarnation does.
#[derive(Default)]
struct Desk {
    parked: Vec<Deferred<u64>>,
}

impl desk::Actor for Desk {
    async fn place(&mut self, _cx: Caller<'_>, reply: Reply<u64>, _order: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        self.parked.push(deferred);
        replied
    }

    async fn parked(&mut self, _cx: Caller<'_>) -> u64 {
        self.parked.len() as u64
    }

    async fn boom(&mut self, _cx: Caller<'_>) -> u64 {
        panic!("the desk fell over");
    }
}

// ─────────────────────────────────────────────────────────────────── observers

/// Refuses the first turn it ever sees, across **every** incarnation.
///
/// The counter is shared rather than held, which is what makes exactly one
/// restart happen: an observer that refused every turn would refuse the next
/// incarnation's too, and the actor would storm instead of recovering.
struct RefuseOnce {
    seen: Arc<AtomicU32>,
    fatal: bool,
}

impl<A: Send + 'static> Observer<A> for RefuseOnce {
    fn name(&self) -> &'static str {
        "refuse-once"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a A, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        let first = self.seen.fetch_add(1, Ordering::SeqCst) == 0;
        let fatal = self.fatal;
        Box::pin(async move {
            match (first, fatal) {
                (false, _) => Ok(()),
                (true, false) => Err(Stop::new("the first turn is refused")),
                (true, true) => Err(Stop::fatal("this build cannot read that snapshot")),
            }
        })
    }
}

/// Refuses a turn that panicked, exactly as `Persist` does, and accepts every
/// other one.
///
/// A `service!` method that panics does **not** end its actor by itself — the
/// loop catches it and the next message runs — so an observer is what turns a
/// fault into an ending, and therefore into a restart.
struct RefuseFaults;

impl<A: Send + 'static> Observer<A> for RefuseFaults {
    fn name(&self) -> &'static str {
        "refuse-faults"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a A, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let faulted = matches!(turn.outcome, TurnOutcome::Panicked { .. });
        Box::pin(async move {
            if faulted {
                Err(Stop::new("a panicked turn is not committed"))
            } else {
                Ok(())
            }
        })
    }
}

/// Writes `built n` when it is made and `dropped n` when it goes, so that "the
/// old one is dropped before the new one is built" is an order rather than a
/// count.
struct Tracked {
    at: u32,
    log: Arc<Mutex<Vec<String>>>,
}

impl Tracked {
    fn new(at: u32, log: &Arc<Mutex<Vec<String>>>) -> Self {
        log.lock()
            .expect("not poisoned")
            .push(format!("built {at}"));
        Self {
            at,
            log: log.clone(),
        }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.log
            .lock()
            .expect("not poisoned")
            .push(format!("dropped {}", self.at));
    }
}

impl<A: Send + 'static> Observer<A> for Tracked {
    fn name(&self) -> &'static str {
        "tracked"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a A, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        Box::pin(async { Ok(()) })
    }
}

// ─────────────────────────────────────────────────────────────────── fixtures

/// A scratch directory that removes itself.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("keylink-supervision-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn store(name: &str) -> (Scratch, Files) {
    let scratch = Scratch::new(name);
    let files = Files::open(&scratch.0).await.expect("opens");
    (scratch, files)
}

fn limit(max: u32, within: Duration) -> Intensity {
    Intensity::new(
        NonZeroU32::new(max).expect("a budget of at least one"),
        within,
    )
}

/// How many times a factory has been called.
type Calls = Arc<AtomicU32>;

fn calls() -> Calls {
    Arc::new(AtomicU32::new(0))
}

fn seen(calls: &Calls) -> u32 {
    calls.load(Ordering::SeqCst)
}

/// The ending, bounded.
///
/// **Every `ended()` in this file goes through here, and that is a lesson rather
/// than a habit.** A break that stops an actor ending — a budget that never
/// refuses, an ending the supervisor treats as retryable — makes an unbounded
/// `ended().await` *hang*, and a hang is not a failure a test harness can read:
/// the red-test run for two of these guards sat for minutes reporting nothing.
/// Bounded, the same break is a named failure in one line.
async fn ended(waiting: impl std::future::Future<Output = Ended>) -> Ended {
    tokio::time::timeout(NO_PROGRESS, waiting)
        .await
        .expect("the actor must end rather than run for ever")
}

// ────────────────────────────────────────────── phase 1 — the loop

/// The criterion the whole wave rests on: a restarted actor comes back holding
/// what it had committed, and not what the faulting turn had done to it.
///
/// `bad(9)` pushes the entry and *then* panics, so the in-memory state is
/// half-applied at the moment `RefuseFaults` — the shape `Persist` itself has —
/// refuses the turn. The next incarnation opens the same claim and reads the
/// snapshot, which stops at the last turn that was committed.
///
/// Real time and a real file store, and nothing here asserts a duration.
#[tokio::test]
async fn an_actor_restarts_from_its_last_committed_state() {
    let (_scratch, files) = store("committed").await;
    let id = StateId::new("ledgers/sp-01").expect("valid");
    let built = calls();

    let ledger = {
        let built = built.clone();
        ledger::supervised(
            move || {
                let files = files.clone();
                let id = id.clone();
                let built = built.clone();
                async move {
                    built.fetch_add(1, Ordering::SeqCst);
                    // The previous incarnation's claim is already released:
                    // `run_loop` drops the observers before this is called.
                    let (restored, saver) = Persist::<Ledger, _>::open(&files, &id).await?;
                    let observers: Vec<Box<dyn Observer<Ledger>>> =
                        vec![Box::new(saver), Box::new(RefuseFaults)];
                    Ok((restored.unwrap_or_default(), observers, ()))
                }
            },
            limit(2, Duration::from_millis(50)),
        )
    };

    assert_eq!(ledger.post(1).await.expect("the first turn"), 1);
    assert_eq!(ledger.post(2).await.expect("the second turn"), 2);

    // Applied, then faulted. The observer refuses it, the actor ends, and the
    // supervisor builds the next one.
    ledger.bad(9).await.expect("queued");

    assert_eq!(
        ledger
            .entries()
            .await
            .expect("the next incarnation answers"),
        vec![1, 2],
        "a restart starts from the last COMMITTED state, not from the faulted one"
    );
    assert_eq!(seen(&built), 2, "one factory call per incarnation");
}

/// A budget that is spent is spent: the actor ends rather than restarting for
/// ever.
///
/// The storm is a feed that ends the moment it is polled, which needs no caller
/// and no panic — `Ended::Feed` is a restart-worthy ending exactly like a fault.
#[tokio::test(start_paused = true)]
async fn a_restart_storm_ends_the_actor() {
    let built = calls();
    let ticker = {
        let built = built.clone();
        ticker::supervised(
            move || {
                let built = built.clone();
                async move {
                    built.fetch_add(1, Ordering::SeqCst);
                    Ok((
                        Ticker {
                            seen: Arc::new(Mutex::new(Vec::new())),
                        },
                        Vec::new(),
                        ticker::Feeds::new(n0_future::stream::empty()),
                    ))
                }
            },
            limit(3, Duration::from_secs(4)),
        )
    };

    match ended(ticker.ended()).await {
        Ended::GaveUp { after, error } => {
            assert_eq!(after, 3, "the budget is three restarts, and all three ran");
            assert!(
                error.to_string().contains("Beat"),
                "the last incarnation's reason is carried out: got {error}"
            );
        }
        other => panic!("a storm must end the actor, got {other:?}"),
    }
    assert_eq!(seen(&built), 4, "one first incarnation and three restarts");
}

/// An actor that gave up still answers `stop()`.
///
/// **The timeout is the assertion.** `stop` waits on the ending channel, and an
/// actor that already ended has already filled it — so this returns at once, and
/// a `stop` that waited for something that will never happen would hang rather
/// than fail. Under a paused clock the bound costs no real time.
#[tokio::test(start_paused = true)]
async fn a_supervised_actor_that_gave_up_answers_stop() {
    let ticker = ticker::supervised(
        move || async move {
            Ok((
                Ticker {
                    seen: Arc::new(Mutex::new(Vec::new())),
                },
                Vec::new(),
                ticker::Feeds::new(n0_future::stream::empty()),
            ))
        },
        limit(2, Duration::from_secs(4)),
    );

    assert!(
        matches!(ended(ticker.ended()).await, Ended::GaveUp { .. }),
        "the storm has to have ended it before this test says anything"
    );

    tokio::time::timeout(NO_PROGRESS, ticker.stop())
        .await
        .expect("an actor that gave up must answer stop() rather than hang");
    // Idempotent, and still true the second time.
    tokio::time::timeout(NO_PROGRESS, ticker.stop())
        .await
        .expect("stop() is documented idempotent, and giving up does not change that");
}

/// A restart is not an ending, and it is structural rather than filtered: the
/// incarnations run inside one task, and only that task's exit publishes an
/// `Ended`.
///
/// The first incarnation's feed ends at once; every later one is `pending`, so
/// exactly one restart happens and the actor then stays up. **The timeout is the
/// assertion** — `ended()` resolving is the failure.
#[tokio::test(start_paused = true)]
async fn a_restart_is_not_an_ending() {
    // A channel and not a counter, because a counter can only be *polled* and
    // the first version of this test polled it too early: the first
    // incarnation's mailbox arm can win the race against its own dying feed, so
    // an answered `beats()` is no evidence that a restart has happened. The
    // factory says when it ran.
    let (announce, mut built) = tokio::sync::mpsc::unbounded_channel();
    let ticker = {
        let built = calls();
        ticker::supervised(
            move || {
                let built = built.clone();
                let announce = announce.clone();
                async move {
                    let at = built.fetch_add(1, Ordering::SeqCst);
                    let _ = announce.send(at);
                    let beats: std::pin::Pin<Box<dyn Stream<Item = u64> + Send>> = match at {
                        0 => Box::pin(n0_future::stream::empty()),
                        _ => Box::pin(n0_future::stream::pending()),
                    };
                    Ok((
                        Ticker {
                            seen: Arc::new(Mutex::new(Vec::new())),
                        },
                        Vec::new(),
                        ticker::Feeds::new(beats),
                    ))
                }
            },
            limit(3, Duration::from_secs(4)),
        )
    };

    // Evidence the restart happened, reported by the thing that did it.
    assert_eq!(built.recv().await, Some(0), "the first incarnation");
    assert_eq!(built.recv().await, Some(1), "and the restart");
    // And the second one is alive, which is what makes the wait below a wait
    // rather than a race with a dead actor.
    assert_eq!(ticker.beats().await.expect("the second incarnation"), 0);

    let ending = tokio::time::timeout(NO_PROGRESS, ticker.ended()).await;
    assert!(
        ending.is_err(),
        "ended() must not resolve across a restart: got {ending:?}"
    );
    assert!(
        ticker.is_running(),
        "and the actor is running, which is true"
    );
}

/// Spending the budget costs its whole window, because the backoff is
/// `within / max` and there is no knob.
///
/// That is what stops a supervised actor turning a permanent fault into a hot
/// loop, and it is the reason `Intensity`'s two numbers are one decision rather
/// than two. Four restarts at two seconds each is eight seconds of **virtual**
/// time; the clock moves only because the supervisor's own `sleep` is the
/// earliest thing the runtime has to do.
#[tokio::test(start_paused = true)]
async fn the_backoff_spreads_the_budget_across_its_window() {
    let within = Duration::from_secs(8);
    let started = tokio::time::Instant::now();

    let ticker = ticker::supervised(
        move || async move {
            Ok((
                Ticker {
                    seen: Arc::new(Mutex::new(Vec::new())),
                },
                Vec::new(),
                ticker::Feeds::new(n0_future::stream::empty()),
            ))
        },
        limit(4, within),
    );

    assert!(matches!(
        ended(ticker.ended()).await,
        Ended::GaveUp { after: 4, .. }
    ));

    let spent = started.elapsed();
    assert!(
        spent >= within,
        "exhausting the budget must cost its window, not microseconds: {spent:?}"
    );
    assert!(
        spent < within * 2,
        "and it must not cost more than one window: {spent:?}"
    );
}

/// `Stop::fatal` skips the budget: trying again cannot help, so nothing is spent
/// proving it.
///
/// `after: 0` and one factory call is the whole claim — the actor is built once,
/// refused once, and never rebuilt.
#[tokio::test(start_paused = true)]
async fn a_fatal_stop_skips_the_budget() {
    let built = calls();
    let ledger = {
        let built = built.clone();
        ledger::supervised(
            move || {
                let built = built.clone();
                async move {
                    built.fetch_add(1, Ordering::SeqCst);
                    let observers: Vec<Box<dyn Observer<Ledger>>> = vec![Box::new(RefuseOnce {
                        seen: Arc::new(AtomicU32::new(0)),
                        fatal: true,
                    })];
                    Ok((Ledger::default(), observers, ()))
                }
            },
            limit(5, Duration::from_secs(10)),
        )
    };

    assert!(
        ledger.post(1).await.is_err(),
        "a refused turn owes its caller a refusal"
    );

    match ended(ledger.ended()).await {
        Ended::GaveUp { after, error } => {
            assert_eq!(after, 0, "a fatal stop spends nothing");
            assert!(
                error.to_string().contains("cannot read that snapshot"),
                "the observer's own reason is carried out: got {error}"
            );
        }
        other => panic!("a fatal stop must end the actor, got {other:?}"),
    }
    assert_eq!(seen(&built), 1, "built once, and never rebuilt");
}

/// Every incarnation gets its own observers, and the old ones are dropped
/// **before** the new ones are built.
///
/// The ordering is not decoration: a durable observer owns an exclusive claim,
/// so an overlap here would be the next incarnation asking for a claim the last
/// one still holds — and the file adapter would refuse it, turning every restart
/// of every durable actor into a give-up.
#[tokio::test(start_paused = true)]
async fn an_observer_is_rebuilt_for_each_incarnation() {
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let built = calls();

    let ticker = {
        let log = log.clone();
        let built = built.clone();
        ticker::supervised(
            move || {
                let log = log.clone();
                let built = built.clone();
                async move {
                    let at = built.fetch_add(1, Ordering::SeqCst);
                    let observers: Vec<Box<dyn Observer<Ticker>>> =
                        vec![Box::new(Tracked::new(at, &log))];
                    Ok((
                        Ticker {
                            seen: Arc::new(Mutex::new(Vec::new())),
                        },
                        observers,
                        ticker::Feeds::new(n0_future::stream::empty()),
                    ))
                }
            },
            limit(2, Duration::from_secs(4)),
        )
    };

    assert!(matches!(
        ended(ticker.ended()).await,
        Ended::GaveUp { after: 2, .. }
    ));

    assert_eq!(
        *log.lock().expect("not poisoned"),
        vec![
            "built 0",
            "dropped 0",
            "built 1",
            "dropped 1",
            "built 2",
            "dropped 2"
        ],
        "each incarnation's observers are gone before the next one's are made"
    );
}

/// A feed is re-subscribed on each incarnation, and a feed that ended the actor
/// is re-attempted.
///
/// Each incarnation's stream yields one distinct beat and then ends, so the list
/// of beats says both things at once: three different values means three
/// different streams, and a fourth incarnation existing at all means a dead feed
/// was tried again rather than written off.
#[tokio::test(start_paused = true)]
async fn a_feed_is_resubscribed_on_each_incarnation() {
    let beats: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let built = calls();

    let ticker = {
        let beats = beats.clone();
        let built = built.clone();
        ticker::supervised(
            move || {
                let beats = beats.clone();
                let built = built.clone();
                async move {
                    let at = built.fetch_add(1, Ordering::SeqCst);
                    Ok((
                        Ticker { seen: beats },
                        Vec::new(),
                        // One item, then the end: the feed both feeds the actor
                        // and kills it, which is the shape a re-subscription has
                        // to survive.
                        ticker::Feeds::new(n0_future::stream::iter([u64::from(at)])),
                    ))
                }
            },
            limit(2, Duration::from_secs(4)),
        )
    };

    match ended(ticker.ended()).await {
        Ended::GaveUp { after, error } => {
            assert_eq!(after, 2);
            assert!(error.to_string().contains("Beat"), "got {error}");
        }
        other => panic!("got {other:?}"),
    }

    assert_eq!(
        *beats.lock().expect("not poisoned"),
        vec![0, 1, 2],
        "each incarnation subscribed a stream of its own and saw its own item"
    );
}

/// `abort()` destroys the actor, not one incarnation.
///
/// It is `task.abort()` and nothing else, and the task is the supervisor's — so
/// there is no mechanism here to get wrong, and the assertion is that no
/// mechanism was added.
#[tokio::test]
async fn abort_destroys_a_supervised_actor_rather_than_one_incarnation() {
    let built = calls();
    let ledger = {
        let built = built.clone();
        ledger::supervised(
            move || {
                let built = built.clone();
                async move {
                    built.fetch_add(1, Ordering::SeqCst);
                    Ok((Ledger::default(), Vec::new(), ()))
                }
            },
            limit(3, Duration::from_secs(10)),
        )
    };

    assert_eq!(ledger.post(1).await.expect("the first turn"), 1);
    ledger.abort();

    assert!(
        matches!(ended(ledger.ended()).await, Ended::Vanished),
        "an aborted supervisor is a cancelled task, which is `Vanished`"
    );
    assert_eq!(
        seen(&built),
        1,
        "abort destroys the actor: nothing rebuilds it"
    );
}

// ──────────────────────── phase 2 — the generated door, served, and keyed

/// The same `Handle` reaches the next incarnation.
///
/// This is the property that makes OTP's `one_for_all` unnecessary here: a
/// crashed Erlang process leaves its peers holding a dead pid, and there is no
/// stale reference to repair when identity does not die with the incarnation.
/// The outer loop keeps the `mpsc::Sender`, so the handle held here goes on
/// working across a restart with nothing re-registered.
#[tokio::test(start_paused = true)]
async fn a_handle_survives_its_actors_restart() {
    let refusals = Arc::new(AtomicU32::new(0));
    let ledger = {
        let refusals = refusals.clone();
        ledger::supervised(
            move || {
                let refusals = refusals.clone();
                async move {
                    let observers: Vec<Box<dyn Observer<Ledger>>> = vec![Box::new(RefuseOnce {
                        seen: refusals,
                        fatal: false,
                    })];
                    Ok((Ledger::default(), observers, ()))
                }
            },
            limit(3, Duration::from_secs(10)),
        )
    };

    assert!(
        ledger.post(1).await.is_err(),
        "the first turn is refused, which ends the incarnation"
    );

    // No re-registration, no second handle, no `push_front`: the same value.
    assert_eq!(
        ledger.post(5).await.expect("the next incarnation answers"),
        1,
        "the second incarnation starts from a fresh state and answers this handle"
    );
    assert!(ledger.is_running());
}

/// A remote caller can tell *restarting* from *gone*, and neither answer is a new
/// `RemoteError` variant.
///
/// The one message a restart genuinely loses is a `Deferred` parked in the state,
/// because the state is dropped when the incarnation ends. Before supervision
/// that was always `NoSuchActor` — *there is nothing here, stop asking* — which
/// is a lie about an actor that is being rebuilt and about to answer again. So
/// the served path reads the supervisor's decision, which is published **before**
/// the state is dropped, and renders `Failed("restarting: ..")`.
///
/// Two real nodes, one QUIC connection, and three answers on it:
///
/// 1. `boom` faults → `Failed("panicked: ..")`, the actor survives the message;
/// 2. the parked `place` → `Failed("restarting: ..")`, the incarnation did not;
/// 3. once the budget is out → `NoSuchActor`, which is *gone* and is final.
#[tokio::test]
async fn a_remote_caller_can_tell_restarting_from_gone() {
    let server_node = local_node().await;
    let client_node = local_node().await;

    // One restart, so the second fault is terminal and "gone" is reachable
    // inside one test without a storm.
    let desk = desk::supervised(
        move || async move {
            let observers: Vec<Box<dyn Observer<Desk>>> = vec![Box::new(RefuseFaults)];
            Ok((Desk::default(), observers, ()))
        },
        limit(1, Duration::from_millis(50)),
    );
    let _server = Server::builder(&server_node)
        .service(desk.service(Allowlist::anyone()).build())
        .spawn()
        .expect("one service, one ALPN");

    let peer: keylink::Peer<desk::Client> =
        keylink::Peer::new(&client_node, server_node.endpoint().addr());
    let client = peer.connect().await.expect("the dial");

    for (round, expected) in [(1u8, "restarting: "), (2, "gone")] {
        // Parked by the handler and never answered. Held across the fault below.
        let placing = {
            let client = client.clone();
            tokio::spawn(async move { client.place(round.into()).await })
        };
        // **Asked, never assumed.** The two calls race on one connection, and a
        // fault that overtook the park would end the incarnation the park was
        // meant to be lost with — leaving the placed order sitting in the *next*
        // one for ever. Observed as a hang before this loop existed.
        for _ in 0..1_000 {
            if client.parked().await.expect("the actor answers") == 1 {
                break;
            }
            n0_future::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            client.parked().await.expect("the actor answers"),
            1,
            "round {round}: the order has to be parked before the fault"
        );

        let faulted = client.boom().await.expect_err("boom panics");
        assert!(
            matches!(&faulted, keylink::Error::Remote(RemoteError::Failed(why))
                     if why.starts_with("panicked: ")),
            "a fault is rendered with its own prefix and is not confusable with a \
             restart: got {faulted:?}"
        );

        let placed = placing
            .await
            .expect("the caller task is not itself panicking")
            .expect_err("a parked reply whose incarnation ended is not answered");
        match (&placed, expected) {
            (keylink::Error::Remote(RemoteError::Failed(why)), "restarting: ") => assert!(
                why.starts_with("restarting: "),
                "round {round}: a lost reply on a restarting actor must say so: got {why}"
            ),
            (keylink::Error::Remote(RemoteError::NoSuchActor { .. }), "gone") => {}
            _ => panic!("round {round}: expected {expected}, got {placed:?}"),
        }
    }

    // And it stays gone, through the other door: the mailbox is closed, so the
    // send itself fails.
    let after = client.place(3).await.expect_err("the actor gave up");
    assert!(
        matches!(
            after,
            keylink::Error::Remote(RemoteError::NoSuchActor { .. })
        ),
        "an actor that gave up is gone for good: got {after:?}"
    );
    assert!(
        matches!(ended(desk.ended()).await, Ended::GaveUp { after: 1, .. }),
        "one restart was budgeted, and the second fault spent nothing left"
    );
}

/// Supervision reaches a **keyed** actor, because Wave 3 unified the loops.
///
/// The registry is untouched: `mount` takes a closure over a mailbox, and the
/// supervisor is that closure's whole body — so `System::mount`, eviction and
/// `shutdown` see one task exactly as they do for an unsupervised keyed actor.
///
/// This is also the keyed door a durable actor could not use lazily before:
/// `SystemBuilder::spawn_with`'s factory is a plain `Fn` and cannot `await`, and
/// `Persist::open` is I/O. This factory awaits, once per incarnation.
#[tokio::test]
async fn a_keyed_actor_restarts_from_its_last_committed_state() {
    let (_scratch, files) = store("keyed").await;
    let id = StateId::new("ledgers/sp-02").expect("valid");
    let actor = keylink::ActorId::new("ledgers/sp-02").expect("valid");
    let built = calls();

    let system = ledger::System::builder(keylink::Eviction::Never)
        .capacity(4)
        .build();
    system
        .mount(actor.clone(), {
            let built = built.clone();
            ledger::spawn_supervised(
                move || {
                    let files = files.clone();
                    let id = id.clone();
                    let built = built.clone();
                    async move {
                        built.fetch_add(1, Ordering::SeqCst);
                        let (restored, saver) = Persist::<Ledger, _>::open(&files, &id).await?;
                        let observers: Vec<Box<dyn Observer<Ledger>>> =
                            vec![Box::new(saver), Box::new(RefuseFaults)];
                        Ok((
                            restored.unwrap_or_default(),
                            observers,
                            ledger::Feeds::new(),
                        ))
                    }
                },
                limit(2, Duration::from_millis(50)),
            )
        })
        .expect("mounts");

    let server_node = local_node().await;
    let client_node = local_node().await;
    let _server = Server::builder(&server_node)
        .service(ledger::keyed_service(&system, Allowlist::anyone()).build())
        .spawn()
        .expect("one service, one ALPN");
    let client = ledger::KeyedClient::connect(&client_node, server_node.endpoint().addr())
        .await
        .expect("the client dials the actor system");

    ledger::at(&client, actor.clone())
        .post(1)
        .await
        .expect("the keyed actor answers");
    ledger::at(&client, actor.clone())
        .post(2)
        .await
        .expect("the keyed actor answers");

    // Applied, then faulted, then refused: the incarnation ends and the
    // supervisor re-opens the claim the last one has just released.
    let faulted = ledger::at(&client, actor.clone())
        .ruin(9)
        .await
        .expect_err("the turn panicked");
    assert!(
        matches!(&faulted, keylink::Error::Remote(RemoteError::Failed(why))
                 if why.starts_with("panicked: ")),
        "got {faulted:?}"
    );

    assert_eq!(
        ledger::at(&client, actor.clone())
            .entries()
            .await
            .expect("the next incarnation answers"),
        vec![1, 2],
        "a keyed restart starts from the last committed state too"
    );
    assert_eq!(seen(&built), 2, "one factory call per incarnation");
}
