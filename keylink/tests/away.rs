//! `Deferred::away`: slow work that leaves a turn and comes back as one.
//!
//! Limit 8 of the design was that a turn cannot be bounded and a slow step inside
//! it holds the actor. `#[defer]` parked the caller but had no way back: a
//! `Deferred` handed to a spawned task answers at once, outside any turn. Three
//! claims are pinned here, one test each:
//!
//! | the claim | the test |
//! |---|---|
//! | the actor serves other messages while the work is away, and the work then comes back as a turn | `a_slow_off_turn_step_does_not_hold_the_actor` |
//! | the actor ending cancels the work and tells the parked caller the truth | `stopping_while_work_is_away_answers_the_parked_caller` |
//! | a launching turn that is refused never starts its work | `work_launched_by_a_refused_turn_never_starts` |
//! | work cancelled by a restart answers its caller with that restart, never with the next incarnation | `work_cancelled_by_a_restart_answers_with_the_restart_and_not_the_next_incarnation` |
//! | an aborted actor cancels the work and tells its caller the actor is gone; it does not wait for the work | `an_aborted_actor_tells_its_away_caller_it_is_gone` |
//! | a `back` that sends before it returns its future is still gated by its turn's commit | `a_back_that_sends_eagerly_is_still_gated_by_its_turns_commit` |
//! | a `back` that launches before it returns its future launches nothing for a refused turn | `a_back_that_relaunches_eagerly_launches_nothing_for_a_refused_turn` |
//! | a `back` that panics before it returns its future costs the message and not the actor | `a_back_that_panics_before_returning_a_future_costs_the_message_and_not_the_actor` |
//!
//! Not one assertion reads a clock. The first test runs on a paused clock, so the
//! hour of "slow" work costs nothing, and the other two bound a wait only to turn a
//! hang into a failure.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use keylink::prelude::*;
use keylink::{
    Back, Caller, Deferred, Observer, Replied, Reply, Stop, TurnFut, TurnInfo, TurnOutcome,
    Unanswered, Withheld,
};
use tokio::sync::{oneshot, Notify};

keylink::service! {
    service Desk;

    /// Parks the caller, sends the work away, and is answered by `Back`.
    #[defer]
    rpc Land(n: u64) -> u64;

    /// How many `Land` turns have run. An ordinary `rpc`, so a test can wait for a
    /// launch by asking rather than by sleeping.
    rpc Launched() -> u64;

    /// How many `back` turns have run, and the sum of what they carried.
    rpc Applied() -> u64;

    /// Does nothing. A test's observer refuses it, which ends an incarnation.
    rpc Retire() -> u64;
}

/// What the work does. One enum and not three services, so the three tests share a
/// handler and differ only in the future it builds.
enum Work {
    /// Sleeps, then returns `n`. The clock is paused, so an hour is free.
    Sleep(Duration),
    /// Waits for a gate the test holds, with a sentinel that says when it dies.
    Gate {
        gate: Arc<Notify>,
        log: Log,
        dropped: Mutex<Option<oneshot::Sender<()>>>,
    },
    /// Sets a flag the moment it runs.
    Flag(Arc<AtomicBool>),
    /// Blocks the worker thread that polls it, after saying it is inside the poll,
    /// until the test releases it, and then waits for ever. Cancelling it has to
    /// wait for the poll to return, which is the slow destruction a restart must
    /// not outrun. It holds a sentinel, so the log says when its future died.
    Blocks {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        log: Log,
    },
}

/// An ordered record of what happened to the work, shared with the test.
type Log = Arc<Mutex<Vec<&'static str>>>;

/// Writes `"dropped"` and fires its signal when the work's future is dropped, which
/// is how a test sees a cancellation it cannot await.
struct Sentinel {
    log: Log,
    signal: Option<oneshot::Sender<()>>,
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        self.log.lock().unwrap().push("dropped");
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(());
        }
    }
}

impl Work {
    /// The future for one `Land(n)`. Built in the turn and `'static`, so it holds
    /// nothing of the state.
    fn run(&self, n: u64) -> Pin<Box<dyn Future<Output = u64> + Send>> {
        match self {
            Self::Sleep(how_long) => {
                let how_long = *how_long;
                Box::pin(async move {
                    tokio::time::sleep(how_long).await;
                    n
                })
            }
            Self::Gate { gate, log, dropped } => {
                let gate = Arc::clone(gate);
                let sentinel = Sentinel {
                    log: Arc::clone(log),
                    signal: dropped.lock().unwrap().take(),
                };
                Box::pin(async move {
                    let _held = sentinel;
                    gate.notified().await;
                    n
                })
            }
            Self::Flag(set) => {
                let set = Arc::clone(set);
                Box::pin(async move {
                    set.store(true, Ordering::SeqCst);
                    n
                })
            }
            Self::Blocks {
                entered,
                release,
                log,
            } => {
                let entered = entered.lock().unwrap().take();
                let release = release.lock().unwrap().take();
                let sentinel = Sentinel {
                    log: Arc::clone(log),
                    signal: None,
                };
                Box::pin(async move {
                    let _held = sentinel;
                    if let Some(entered) = entered {
                        let _ = entered.send(());
                    }
                    if let Some(release) = release {
                        // Blocks this worker inside the poll, on purpose.
                        let _ = release.recv();
                    }
                    std::future::pending::<u64>().await
                })
            }
        }
    }
}

struct Desk {
    work: Work,
    launched: u64,
    applied: u64,
}

impl Desk {
    fn new(work: Work) -> Self {
        Self {
            work,
            launched: 0,
            applied: 0,
        }
    }
}

impl desk::Actor for Desk {
    async fn land(&mut self, _cx: Caller<'_>, reply: Reply<u64>, n: u64) -> Replied<u64> {
        self.launched += 1;
        let work = self.work.run(n);
        let (deferred, replied) = reply.later();
        deferred.away(self, work);
        replied
    }

    async fn launched(&mut self, _cx: Caller<'_>) -> u64 {
        self.launched
    }

    async fn applied(&mut self, _cx: Caller<'_>) -> u64 {
        self.applied
    }

    async fn retire(&mut self, _cx: Caller<'_>) -> u64 {
        0
    }
}

impl Back<u64, u64> for Desk {
    async fn back(&mut self, out: u64, reply: Deferred<u64>) {
        self.applied += out;
        reply.send(out);
    }
}

/// Wait for `want` launches, by asking the actor rather than sleeping. A
/// `Launched()` call takes a mailbox permit like anything else, so it answers only
/// once the `Land` turn has *finished*, which is the fact being waited for.
async fn launched(desk: &desk::Handle<Desk>, want: u64) {
    for _ in 0..500 {
        if desk.launched().await.expect("the actor is running") == want {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{want} launches never happened");
}

// ───────────────────────────────────────────── the actor serves while work is away

/// Records the name every turn is observed under.
struct Naming(Arc<Mutex<Vec<&'static str>>>);

impl Observer<Desk> for Naming {
    fn name(&self) -> &'static str {
        "naming"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Desk, turn: TurnInfo<'a>) -> TurnFut<'a> {
        self.0.lock().unwrap().push(turn.method);
        Box::pin(async { Ok(()) })
    }
}

/// An hour of work does not hold the actor, and the work comes back as a turn.
///
/// The ordering is the proof, and it needs no clock: with the work an hour long on
/// a paused clock, `Launched()` and `Applied()` are answered while it is still
/// away, so two turns ran in between, and `Applied()` reads `0` because no `back`
/// turn has run. Only then does the runtime idle, the clock advances, and the
/// caller gets `1` with `Applied()` at `1`: the answer came from a later turn.
///
/// The final wait is bounded at twice the work, a ceiling derived from the
/// constant it bounds. It exists so that a `back` turn that never comes fails the
/// test instead of hanging it.
///
/// Made red three times, by `scripts/break-once.sh`.
///
/// The work run inline: `keylink/tests/away.rs`, this file, so the search edit is
/// described and not quoted. In `Desk::land`, the eight-space-indented statement
/// that sends the work away is replaced by `{ let out = work.await; self.applied +=
/// out; deferred.send(out); }`. `Launched()` then waits behind the turn, the clock
/// advances, and the first assertion fails with `no back turn has run while the
/// work is away`, `left: 1, right: 0`.
///
/// The loop's third input removed: `keylink/src/turns.rs`, in `next`, the line
/// `while let Poll::Ready(Some(done)) = set.poll_join_next(cx) {` (indented
/// twenty-four spaces) replaced by `while let Poll::Ready(Some(done)) =
/// Poll::<Option<Result<(&'static str, Comeback<A>), n0_future::task::JoinError>>>::Pending {`.
/// The work finishes and nothing runs it, so the bounded wait fails with `a back
/// turn never came: Elapsed(())`.
///
/// The back turn left unnamed: `keylink/src/turns.rs`, in `Deferred::away`,
/// `let method = reply.method;` (indented eight spaces) replaced by `let method =
/// "";`. The observer then sees the back turn under no name, and the last
/// assertion fails with `the launching turn and the back turn are both named for
/// the call: ["Launched", "Land", "Launched", "Applied", "", "Applied"]`.
#[tokio::test(start_paused = true)]
async fn a_slow_off_turn_step_does_not_hold_the_actor() {
    const SLOW: Duration = Duration::from_secs(3600);
    let names = Arc::new(Mutex::new(Vec::new()));
    let desk = desk::setup(Desk::new(Work::Sleep(SLOW)))
        .observe(Naming(Arc::clone(&names)))
        .actor();

    let landing = {
        let desk = desk.clone();
        tokio::spawn(async move { desk.land(1).await })
    };
    launched(&desk, 1).await;

    assert_eq!(
        desk.applied().await.expect("the actor is running"),
        0,
        "no back turn has run while the work is away"
    );
    assert!(!landing.is_finished(), "the caller is still waiting");

    let landed = n0_future::time::timeout(SLOW * 2, landing)
        .await
        .expect("a back turn never came")
        .expect("the caller is not itself panicking");
    assert_eq!(landed.expect("the actor is running"), 1);
    assert_eq!(desk.applied().await.expect("the actor is running"), 1);

    // The back turn is a turn like any other, observed under the method of the
    // call it answers: `Land` ran twice, once to launch and once to come back.
    let names = names.lock().unwrap();
    assert_eq!(
        names.iter().filter(|name| **name == "Land").count(),
        2,
        "the launching turn and the back turn are both named for the call: {names:?}"
    );
    assert!(!names.contains(&""), "no turn goes unnamed: {names:?}");
}

// ─────────────────────────────────────────────── the actor ending cancels the work

/// Stopping the actor cancels the work, and the caller parked behind it is told
/// the actor is gone.
///
/// The work holds a sentinel that writes `"dropped"` when its future dies, and the
/// test holds the only gate it waits on. A fallback releases the gate after ten
/// seconds of paused time and writes `"released"`: if the work is not cancelled it
/// finishes on its own and the log reads `["released", "dropped"]`. Cancelled, the
/// log is `["dropped"]` and the fallback never gets its turn. That is a lower
/// bound: it can only prove the work died before the fallback, and says nothing
/// about how soon.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `run_loop`'s
/// `set.shutdown().await;` (indented eight spaces; registered as `OT2`) and the
/// replacement `set.detach_all();`, which lets the work outlive its actor. The
/// fallback fires and the log reads `left: ["released", "dropped"]`, `right:
/// ["dropped"]`.
#[tokio::test(start_paused = true)]
async fn stopping_while_work_is_away_answers_the_parked_caller() {
    let log = Log::default();
    let gate = Arc::new(Notify::new());
    let (dropped, died) = oneshot::channel();
    let desk = desk::actor(Desk::new(Work::Gate {
        gate: Arc::clone(&gate),
        log: Arc::clone(&log),
        dropped: Mutex::new(Some(dropped)),
    }));

    let landing = {
        let desk = desk.clone();
        tokio::spawn(async move { desk.land(1).await })
    };
    launched(&desk, 1).await;

    {
        let (gate, log) = (Arc::clone(&gate), Arc::clone(&log));
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            log.lock().unwrap().push("released");
            gate.notify_one();
        });
    }

    desk.stop()
        .await
        .expect("a stop from outside the actor's own turn is never refused");

    let landed = n0_future::time::timeout(Duration::from_secs(30), landing)
        .await
        .expect("a parked caller must not outlive its actor as a hang")
        .expect("the caller is not itself panicking");
    assert!(
        matches!(landed, Err(Unanswered::Turn(Withheld::Gone))),
        "the caller is told the actor is gone: got {landed:?}"
    );

    n0_future::time::timeout(Duration::from_secs(30), died)
        .await
        .expect("the work was never cancelled")
        .expect("the sentinel signals once");
    assert_eq!(*log.lock().unwrap(), ["dropped"]);
}

// ───────────────────────────────────────────── a refused turn never starts its work

/// Refuses every turn it is shown, after yielding once: a launch that was started
/// before the verdict would get to run in that yield.
struct Refusing;

impl Observer<Desk> for Refusing {
    fn name(&self) -> &'static str {
        "refusing"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Desk, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            Err(Stop::new("refusing refused"))
        })
    }
}

/// An observer that refuses the launching turn stops the work from starting, and
/// the caller is told it was refused, not that the actor is gone.
///
/// Two assertions and each guards a different mistake. The flag proves the work
/// did not run; the answer proves the reply was not carried into a launch that was
/// then thrown away, which would read `Gone`.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `Deferred::away`'s
/// `Verdict::Committed => crate::away::push::<A>(Box::pin(async move {` (indented
/// twelve spaces) and the replacement `_ => crate::away::push::<A>(Box::pin(async
/// move {`, which launches the work on every verdict. The refused caller is then
/// told the actor is gone, and the first assertion fails with `the caller is told
/// the observer refused: got Err(Turn(Gone))`.
///
/// **The flag assertion is the weaker half, and measured as such.** With the first
/// assertion neutered the same break passes: on a current-thread runtime the loop
/// ends the actor and drops the set before the launched task is first polled, so
/// the work never runs either way. The flag still guards a launch that is started
/// before the verdict, which the observer's yield would let run.
#[tokio::test]
async fn work_launched_by_a_refused_turn_never_starts() {
    let ran = Arc::new(AtomicBool::new(false));
    let desk = desk::setup(Desk::new(Work::Flag(Arc::clone(&ran))))
        .observe(Refusing)
        .actor();

    let landed = desk.land(1).await;
    assert!(
        matches!(
            &landed,
            Err(Unanswered::Turn(Withheld::Refused { name })) if name == "refusing"
        ),
        "the caller is told the observer refused: got {landed:?}"
    );

    n0_future::time::timeout(Duration::from_secs(5), desk.ended())
        .await
        .expect("the actor ends when an observer refuses");
    assert!(!ran.load(Ordering::SeqCst), "the work never started");
}

// ───────────────────────────────────────────────── work cancelled by a restart

/// Refuses a turn named `Retire`, without declaring the refusal fatal, which ends
/// the incarnation and lets its supervisor start another.
struct RetiresTheIncarnation;

impl Observer<Desk> for RetiresTheIncarnation {
    fn name(&self) -> &'static str {
        "retiring"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Desk, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let retire = turn.method == "Retire";
        Box::pin(async move {
            if retire {
                Err(Stop::new("this incarnation is retired"))
            } else {
                Ok(())
            }
        })
    }
}

/// A caller parked behind away work is told what became of **its** incarnation,
/// even when the cancelled work is slow to die, and the next incarnation is not
/// built until that work is gone.
///
/// Cancelling a task asks for it and does not wait: the future, and the parked
/// reply inside it, are destroyed only when the cancellation has run, and a task
/// that is in the middle of a poll finishes the poll first. Here the work blocks
/// its worker inside the poll until the test releases it, so the reply outlives the
/// incarnation by as long as the test likes. A restart publishes the standing
/// `Running` for the new incarnation; a reply that died after that read `Running`
/// and told its caller `Dropped`, *the actor is serving and this reply was thrown
/// away*, about a reply the old incarnation's end had taken. The loop now waits for
/// the cancelled work to be gone before the incarnation is over, so the reply dies
/// while the standing is still `Restarting`.
///
/// Every step is a gate and none is a duration. The work says it is inside its poll
/// and waits for a release. The factory of the next incarnation logs that it was
/// called and waits for a gate before it returns, so the standing stays
/// `Restarting` while the old caller's answer is read, and the read cannot lose a
/// race with a new `Running`. The ordering is then read from the log: the work
/// must be destroyed **before** the next incarnation is built.
///
/// **One window is a bound and not an assertion, and it can only hide a
/// regression, never invent one.** Whether the end waits for the work cannot be
/// seen without letting the supervisor try to go on: a correct end blocks until
/// the release, so the test holds the work for `HOLD` and gives the supervisor
/// that long to reach the next factory. A correct build never does, and pays
/// `HOLD` for it. A build that does not wait reaches it within the supervisor's
/// ten-millisecond backoff, and logs it before the release. If a loaded machine
/// starves the supervisor for longer than `HOLD`, that regression goes unseen on
/// that run; correct teardown is never reported as broken.
///
/// Four workers, because on one the blocked poll would hold up the supervisor
/// too and there would be no window to close.
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `run_loop`'s
/// `set.shutdown().await;` (indented eight spaces; registered as `OT6`) and the
/// replacement `drop(set);`, which cancels without waiting. Watched: `the work is
/// destroyed before the next incarnation is built`, `left: ["next incarnation",
/// "dropped"]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_cancelled_by_a_restart_answers_with_the_restart_and_not_the_next_incarnation() {
    const HOLD: Duration = Duration::from_millis(250);

    let log = Log::default();
    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let (built_tx, built) = oneshot::channel();
    let next_gate = Arc::new(Notify::new());

    let first = Arc::new(Mutex::new(Some((entered_tx, release_rx))));
    let built_tx = Arc::new(Mutex::new(Some(built_tx)));
    let desk = desk::restarting(
        {
            let (log, next_gate) = (Arc::clone(&log), Arc::clone(&next_gate));
            move || {
                let (log, next_gate) = (Arc::clone(&log), Arc::clone(&next_gate));
                let first = first.lock().unwrap().take();
                let built_tx = Arc::clone(&built_tx);
                async move {
                    let observers: Vec<Box<dyn Observer<Desk>>> =
                        vec![Box::new(RetiresTheIncarnation)];
                    let work = match first {
                        Some((entered, release)) => Work::Blocks {
                            entered: Mutex::new(Some(entered)),
                            release: Mutex::new(Some(release)),
                            log,
                        },
                        None => {
                            log.lock().unwrap().push("next incarnation");
                            if let Some(built) = built_tx.lock().unwrap().take() {
                                let _ = built.send(());
                            }
                            next_gate.notified().await;
                            Work::Flag(Arc::new(AtomicBool::new(false)))
                        }
                    };
                    Ok((Desk::new(work), observers, ()))
                }
            }
        },
        keylink::Intensity::new(
            std::num::NonZeroU32::new(2).expect("a budget of at least one"),
            Duration::from_millis(20),
        )
        .expect("a recovery span whose backoff is not zero"),
    )
    .actor();

    let landing = {
        let desk = desk.clone();
        tokio::spawn(async move { desk.land(1).await })
    };
    entered.await.expect("the work is inside its poll");

    // The work is now blocking a worker mid-poll. Ending the incarnation cancels it.
    let refused = desk.retire().await;
    assert!(
        matches!(&refused, Err(Unanswered::Turn(Withheld::Refused { .. }))),
        "the observer ended the incarnation: got {refused:?}"
    );

    // Hold the work while the supervisor gets every chance to go on without it.
    let _ = n0_future::time::timeout(HOLD, built).await;
    release.send(()).expect("the work is still waiting for it");

    // The old caller's answer is read while the next factory is still gated, so the
    // standing cannot have moved on to `Running`.
    let landed = n0_future::time::timeout(Duration::from_secs(30), landing)
        .await
        .expect("a parked caller must not outlive its incarnation as a hang")
        .expect("the caller is not itself panicking");
    assert!(
        matches!(&landed, Err(Unanswered::Turn(Withheld::Restarting { .. }))),
        "the caller is told its own incarnation restarted: got {landed:?}"
    );

    next_gate.notify_one();
    assert_eq!(
        desk.applied().await.expect("the next incarnation serves"),
        0,
        "and the restart did happen"
    );
    assert_eq!(
        *log.lock().unwrap(),
        ["dropped", "next incarnation"],
        "the work is destroyed before the next incarnation is built"
    );
}

// ──────────────────────────────────────────────── an aborted actor and its away work

/// Aborting the actor cancels the work and tells the parked caller the actor is
/// gone. That is all `abort` promises about away work, and the test pins no more.
///
/// `abort` destroys the actor now. Unlike `stop`, a refusal or a restart it does
/// not wait for the cancelled work to be gone, so the actor's end can be
/// published while a cancelled future is still being destroyed, and this test says
/// nothing about that order. What survives is the answer: the loop's floor settles
/// `Gone` as the aborted task is destroyed, before the state and everything parked
/// in it, so the reply that dies with the away task reads `Gone` and not `Running`
/// (which would tell a caller the actor is serving and dropped its answer).
///
/// Made red by `scripts/break-once.sh keylink/src/turns.rs` on `Floor`'s `Drop`,
/// `self.fate.settle(Standing::Gone);` (indented twelve spaces; registered as
/// `AB1`) and the replacement `self.fate.settle(Standing::Running);`. Watched: `the
/// caller is told the actor is gone: got Err(Turn(Dropped))`.
#[tokio::test(start_paused = true)]
async fn an_aborted_actor_tells_its_away_caller_it_is_gone() {
    let log = Log::default();
    let (dropped, died) = oneshot::channel();
    let desk = desk::actor(Desk::new(Work::Gate {
        gate: Arc::new(Notify::new()),
        log: Arc::clone(&log),
        dropped: Mutex::new(Some(dropped)),
    }));

    let landing = {
        let desk = desk.clone();
        tokio::spawn(async move { desk.land(1).await })
    };
    launched(&desk, 1).await;

    desk.abort();

    let landed = n0_future::time::timeout(Duration::from_secs(30), landing)
        .await
        .expect("a parked caller must not outlive its aborted actor as a hang")
        .expect("the caller is not itself panicking");
    assert!(
        matches!(landed, Err(Unanswered::Turn(Withheld::Gone))),
        "the caller is told the actor is gone: got {landed:?}"
    );

    n0_future::time::timeout(Duration::from_secs(30), died)
        .await
        .expect("the work was never cancelled")
        .expect("the sentinel signals once");
    assert_eq!(*log.lock().unwrap(), ["dropped"]);
}

// ───────────────────────────────────── a `back` that does work before it returns a future

keylink::service! {
    service Eager;

    /// Comes back to a `back` that sends from the body of a plain `fn`.
    #[defer]
    rpc Settle(n: u64) -> u64;

    /// Comes back to a `back` that sends the work away again from a plain `fn`.
    #[defer]
    rpc Relaunch(n: u64) -> u64;

    /// Comes back to a `back` that panics before it returns anything.
    #[defer]
    rpc Explode(n: u64) -> u64;

    /// What the `back` turns added, so a test can ask whether the actor is serving.
    rpc Applied() -> u64;
}

/// The three outputs the away work produces, one per `back` below. Distinct types
/// because a state has one `Back` impl per output type, and one per behaviour is
/// the point.
struct Settled(u64);
struct Relaunched(u64);
struct Exploded(u64);

#[derive(Default)]
struct Eager {
    applied: u64,
}

impl eager::Actor for Eager {
    async fn settle(&mut self, _cx: Caller<'_>, reply: Reply<u64>, n: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        deferred.away(self, async move { Settled(n) });
        replied
    }

    async fn relaunch(&mut self, _cx: Caller<'_>, reply: Reply<u64>, n: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        deferred.away(self, async move { Relaunched(n) });
        replied
    }

    async fn explode(&mut self, _cx: Caller<'_>, reply: Reply<u64>, n: u64) -> Replied<u64> {
        let (deferred, replied) = reply.later();
        deferred.away(self, async move { Exploded(n) });
        replied
    }

    async fn applied(&mut self, _cx: Caller<'_>) -> u64 {
        self.applied
    }
}

/// A plain `fn`, which the trait permits: it changes the state and sends **before**
/// it returns its future. An `async fn` could not.
impl Back<Settled, u64> for Eager {
    fn back(&mut self, out: Settled, reply: Deferred<u64>) -> impl Future<Output = ()> + Send {
        self.applied += out.0;
        reply.send(out.0);
        async {}
    }
}

/// A plain `fn` that sends the work away again before it returns its future.
impl Back<Relaunched, u64> for Eager {
    fn back(&mut self, out: Relaunched, reply: Deferred<u64>) -> impl Future<Output = ()> + Send {
        self.applied += out.0;
        reply.away(self, async move { Settled(out.0) });
        async {}
    }
}

/// A plain `fn` that panics before there is a future to return.
impl Back<Exploded, u64> for Eager {
    #[allow(unreachable_code)]
    fn back(&mut self, out: Exploded, _reply: Deferred<u64>) -> impl Future<Output = ()> + Send {
        panic!(
            "a back that fell over before it returned a future: {}",
            out.0
        );
        async {}
    }
}

/// Refuses a turn that leaves `applied` above zero, which is exactly the `back`
/// turn: the launching turn changes nothing and is accepted.
struct RefusesWhatApplied;

impl Observer<Eager> for RefusesWhatApplied {
    fn name(&self) -> &'static str {
        "unapplied"
    }

    fn after_turn<'a>(&'a mut self, state: &'a Eager, _turn: TurnInfo<'a>) -> TurnFut<'a> {
        let applied = state.applied;
        Box::pin(async move {
            if applied > 0 {
                Err(Stop::new("a back turn applied something"))
            } else {
                Ok(())
            }
        })
    }
}

/// Records every turn's method and whether it panicked.
struct Recording(Arc<Mutex<Vec<(&'static str, bool)>>>);

impl Observer<Eager> for Recording {
    fn name(&self) -> &'static str {
        "recording"
    }

    fn after_turn<'a>(&'a mut self, _state: &'a Eager, turn: TurnInfo<'a>) -> TurnFut<'a> {
        let panicked = matches!(turn.outcome, TurnOutcome::Panicked { .. });
        self.0.lock().unwrap().push((turn.method, panicked));
        Box::pin(async { Ok(()) })
    }
}

/// A `back` that answers from the body of a plain `fn` is still answered by its
/// turn's commit.
///
/// The turn's observer refuses it, so the caller must read the refusal. When the
/// loop called `back` before it built the lot that parks a turn's answers, the
/// send crossed at once and the caller held `1` for a turn that was then refused:
/// a durable actor's caller would hold a success the store never accepted.
///
/// Made red by `scripts/break-once.sh keylink/src/away.rs` on `comeback`'s
/// `Box::new(move |state: &mut A| Box::pin(async move { state.back(out, reply).await }))`
/// (the whole line, indented four spaces; registered as `OT5`) and the replacement
/// `Box::new(move |state: &mut A| Box::pin(state.back(out, reply)))`, which is the
/// call made before the lot exists. Watched: `the caller is told the observer
/// refused: got Ok(1)`.
#[tokio::test]
async fn a_back_that_sends_eagerly_is_still_gated_by_its_turns_commit() {
    let eager = eager::setup(Eager::default())
        .observe(RefusesWhatApplied)
        .actor();

    let landed = eager.settle(1).await;
    assert!(
        matches!(
            &landed,
            Err(Unanswered::Turn(Withheld::Refused { name })) if name == "unapplied"
        ),
        "the caller is told the observer refused: got {landed:?}"
    );
}

/// A `back` that sends its work away again from the body of a plain `fn` launches
/// nothing for a turn that is then refused.
///
/// Launched before the verdict, the work took the caller's reply with it, the
/// refused turn ended the actor, and the caller was told the actor was gone. Held
/// until the verdict, the caller is told the observer refused.
///
/// Made red by the same edit as the test above. Watched: `the caller is told the
/// observer refused: got Err(Turn(Gone))`.
#[tokio::test]
async fn a_back_that_relaunches_eagerly_launches_nothing_for_a_refused_turn() {
    let eager = eager::setup(Eager::default())
        .observe(RefusesWhatApplied)
        .actor();

    let landed = eager.relaunch(1).await;
    assert!(
        matches!(
            &landed,
            Err(Unanswered::Turn(Withheld::Refused { name })) if name == "unapplied"
        ),
        "the caller is told the observer refused: got {landed:?}"
    );
}

/// A `back` that panics before it returns a future costs the message and not the
/// actor, and its observers read the turn as panicked.
///
/// Called outside the panic guard, the panic unwound the loop itself: the actor
/// task died, every handle read `Gone`, and no observer saw a turn.
///
/// Made red by the same edit as the first test. Watched: `the caller's reply died
/// in the unwind: got Err(Turn(Gone))`, after the panic itself unwound the loop.
#[tokio::test]
async fn a_back_that_panics_before_returning_a_future_costs_the_message_and_not_the_actor() {
    let turns = Arc::new(Mutex::new(Vec::new()));
    let eager = eager::setup(Eager::default())
        .observe(Recording(Arc::clone(&turns)))
        .actor();

    let lost = eager.explode(1).await;
    assert!(
        matches!(&lost, Err(Unanswered::Turn(Withheld::Dropped))),
        "the caller's reply died in the unwind: got {lost:?}"
    );
    assert_eq!(
        eager.applied().await.expect("the actor survived"),
        0,
        "and the actor is serving the next message"
    );
    assert!(
        turns.lock().unwrap().contains(&("Explode", true)),
        "an observer read the back turn as panicked: {:?}",
        turns.lock().unwrap()
    );
}
