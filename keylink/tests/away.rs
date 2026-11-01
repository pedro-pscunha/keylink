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
    Back, Caller, Deferred, Observer, Replied, Reply, Stop, TurnFut, TurnInfo, Unanswered, Withheld,
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
/// Made red twice, by `scripts/break-once.sh`.
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
#[tokio::test(start_paused = true)]
async fn a_slow_off_turn_step_does_not_hold_the_actor() {
    const SLOW: Duration = Duration::from_secs(3600);
    let desk = desk::actor(Desk::new(Work::Sleep(SLOW)));

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
/// `drop(away);` (indented four spaces) and the replacement `if let Some(mut set) =
/// away { set.detach_all(); }`, which lets the work outlive its actor. The fallback
/// fires and the log reads `left: ["released", "dropped"]`, `right: ["dropped"]`.
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
